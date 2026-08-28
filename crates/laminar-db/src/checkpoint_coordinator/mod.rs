//! Checkpoint capture and participant persistence.

#![allow(clippy::disallowed_types)] // checkpoint control path

mod artifacts;
mod capture;
mod commit_index;
#[cfg(feature = "cluster")]
mod follower_completion;
#[cfg(feature = "cluster")]
mod follower_prepare;
#[cfg(feature = "cluster")]
mod handoff;
mod recovery;
mod retention;
mod sink_commit;
pub(crate) mod sink_epoch_admission;
mod sink_protocol;
#[cfg(feature = "cluster")]
mod subscription_output;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use laminar_connectors::checkpoint::SourceCheckpoint;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::CheckpointAttemptRelation;
use laminar_core::checkpoint::{
    channel_progress_frontiers_by_source, checkpoint_artifact_identity_sha256,
    checkpoint_descriptor_sha256, checkpoint_manifest_bytes, checkpoint_sha256,
    classify_channel_progress, ByteRange, ChannelProgress, CheckpointAttempt, CheckpointManifest,
    CheckpointScope, CheckpointStore, CheckpointWatermark, CommittedCheckpointIndex,
    CommittedCheckpointRef, CommittedParticipantRef, ConnectorCheckpoint, LeaderProof,
    PipelineIdentity, PreparedSinkDescriptor, ReferencedStateChunk, StateChunkId, StateFrame,
    StateFrameKey, COMMITTED_CHECKPOINT_INDEX_VERSION, PREPARED_SINK_DESCRIPTOR_VERSION,
};
use laminar_core::checkpoint_decision::{
    CheckpointArtifactInventory, CheckpointArtifactInventoryUpdateResult,
};
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::{BarrierAnnouncement, Phase, QuorumOutcome};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::error::DbError;
use retention::{run_gc_worker, GcRequest};

const MAX_RETENTION_IO_CONCURRENCY: usize = 8;
const REFERENCED_CHUNK_REBASE_THRESHOLD: usize = 64;
#[cfg(feature = "cluster")]
const FOLLOWER_DECISION_POLL: Duration = Duration::from_millis(250);
#[cfg(feature = "cluster")]
async fn publish_terminal_hint_until<F>(deadline: tokio::time::Instant, hint: F)
where
    F: std::future::Future<Output = Result<(), String>>,
{
    // Terminal hints accelerate observation but do not own the already-immutable durable verdict.
    let _ = tokio::time::timeout_at(deadline, hint).await;
}

#[cfg(all(test, feature = "cluster"))]
#[tokio::test]
async fn terminal_checkpoint_hint_cannot_outlive_its_cleanup_deadline() {
    tokio::time::timeout(
        Duration::from_secs(1),
        publish_terminal_hint_until(
            tokio::time::Instant::now(),
            std::future::pending::<Result<(), String>>(),
        ),
    )
    .await
    .expect("an unresponsive terminal hint must be released at its private cleanup deadline");
}

#[cfg(all(debug_assertions, feature = "cluster"))]
async fn checkpoint_kill_gate(
    role: &'static str,
    attempt: CheckpointAttempt,
    predecessor: Option<(u64, u64)>,
) {
    static GATE_FILE: std::sync::OnceLock<Option<std::path::PathBuf>> = std::sync::OnceLock::new();
    let Some(gate_file) = GATE_FILE
        .get_or_init(|| std::env::var_os("LAMINAR_CHECKPOINT_KILL_GATE_FILE").map(Into::into))
        .as_ref()
    else {
        return;
    };
    if std::fs::read_to_string(gate_file)
        .ok()
        .is_none_or(|requested| requested.trim() != role)
    {
        return;
    }
    let Some((predecessor_id, predecessor_epoch)) = predecessor else {
        return;
    };

    let ready_file = gate_file.with_extension("ready");
    let evidence = format!(
        "{role} {} {} {predecessor_id} {predecessor_epoch}",
        attempt.checkpoint_id, attempt.epoch
    );
    if std::fs::write(&ready_file, evidence).is_err() {
        return;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while gate_file.is_file() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = std::fs::remove_file(ready_file);
}

#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub checkpoint_timeout: Duration,
    pub(crate) cleanup_timeout: Duration,
    pub(crate) quorum_timeout: Duration,
    pub max_node_data_bytes: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_timeout: Duration::from_secs(120),
            cleanup_timeout: Duration::from_secs(30),
            quorum_timeout: Duration::from_secs(3),
            max_node_data_bytes:
                laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CheckpointRequest {
    pub flags: u64,
    pub handoff_replay_pending: bool,
    /// Capture-time proof that this cut can be restored under a different vnode assignment.
    pub reassignment_portable: bool,
    pub assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    pub state_frames: Vec<CapturedStateFrame>,
    pub(crate) managed_vnode_operators: Vec<ManagedVnodeOperator>,
    pub source_names: Vec<String>,
    pub channel_progress: Vec<ChannelProgress>,
    pub source_offset_overrides: HashMap<String, ConnectorCheckpoint>,
    #[cfg(feature = "cluster")]
    pub(crate) subscription_output:
        Option<Arc<crate::subscription::cluster::PreparedNodeSubscriptionOutput>>,
}

#[derive(Debug, Clone)]
pub struct CapturedStateFrame {
    pub key: StateFrameKey,
    pub state: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagedVnodePlacement {
    GlobalSingleton,
    VnodeKeyed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedVnodeOperator {
    pub(crate) operator_id: String,
    pub(crate) placement: ManagedVnodePlacement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum CheckpointPhase {
    Idle,
    PreCommitting,
    Persisting,
    Deciding,
}

impl std::fmt::Display for CheckpointPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => formatter.write_str("Idle"),
            Self::PreCommitting => formatter.write_str("PreCommitting"),
            Self::Persisting => formatter.write_str("Persisting"),
            Self::Deciding => formatter.write_str("Deciding"),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointFailureDisposition {
    Retryable,
    RequiresRecovery,
}

/// Determines who publishes a prepared successor sink epoch as writable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkEpochPublication {
    /// Startup and direct coordinator APIs have no callback-owned transition guard.
    Immediate,
    /// A spawned pipeline tail publishes only after its terminal result is known successful.
    DeferredToTail,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointResult {
    pub success: bool,
    pub checkpoint_id: u64,
    pub epoch: u64,
    pub duration: Duration,
    pub error: Option<String>,
    pub failure_disposition: Option<CheckpointFailureDisposition>,
}

impl CheckpointResult {
    #[must_use]
    pub fn continuation_error(&self) -> Option<&str> {
        self.success.then_some(self.error.as_deref()).flatten()
    }

    #[must_use]
    pub fn requires_recovery(&self) -> bool {
        !self.success
            && self.failure_disposition == Some(CheckpointFailureDisposition::RequiresRecovery)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkEpochReservation {
    Opening(CheckpointAttempt),
    Ready(CheckpointAttempt),
    InDoubt(CheckpointAttempt),
}

impl SinkEpochReservation {
    const fn attempt(self) -> CheckpointAttempt {
        match self {
            Self::Opening(attempt) | Self::Ready(attempt) | Self::InDoubt(attempt) => attempt,
        }
    }
}

#[derive(Debug)]
pub(crate) struct EpochAllocator {
    next_id_floor: std::sync::atomic::AtomicU64,
    observed_id_floor: std::sync::atomic::AtomicU64,
    allocation_lock: tokio::sync::Mutex<()>,
    sink_epoch_reservation: parking_lot::Mutex<Option<SinkEpochReservation>>,
    decision_store:
        std::sync::OnceLock<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
}

fn checked_successor_epoch(epoch: u64, context: &str) -> Result<u64, DbError> {
    epoch.checked_add(1).ok_or_else(|| {
        DbError::Checkpoint(format!(
            "checkpoint epoch space exhausted at {epoch} while {context}"
        ))
    })
}

fn require_canonical_attempt(
    attempt: CheckpointAttempt,
    context: &str,
) -> Result<CheckpointAttempt, DbError> {
    if attempt.is_canonical() {
        Ok(attempt)
    } else {
        Err(DbError::Checkpoint(format!(
            "{context} requires one nonzero canonical checkpoint ID; received epoch {} and checkpoint ID {}",
            attempt.epoch, attempt.checkpoint_id
        )))
    }
}

impl EpochAllocator {
    fn new() -> Self {
        Self {
            next_id_floor: std::sync::atomic::AtomicU64::new(1),
            observed_id_floor: std::sync::atomic::AtomicU64::new(0),
            allocation_lock: tokio::sync::Mutex::new(()),
            sink_epoch_reservation: parking_lot::Mutex::new(None),
            decision_store: std::sync::OnceLock::new(),
        }
    }

    fn bind_decision_store(
        &self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        if let Some(bound) = self.decision_store.get() {
            return if Arc::ptr_eq(bound, &store) {
                Ok(())
            } else {
                Err(DbError::Checkpoint(
                    "checkpoint allocator decision store is already bound".into(),
                ))
            };
        }
        self.decision_store.set(store).map_err(|_| {
            DbError::Checkpoint("checkpoint allocator decision store is already bound".into())
        })
    }

    async fn allocate_fresh_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        use std::sync::atomic::Ordering;

        let store = self.decision_store.get().ok_or_else(|| {
            DbError::Checkpoint("checkpoint ID allocation requires a decision store".into())
        })?;
        loop {
            let minimum = self.next_id_floor.load(Ordering::Acquire).max(1);
            let checkpoint_id =
                tokio::time::timeout_at(deadline, store.allocate_checkpoint_id_at_least(minimum))
                    .await
                    .map_err(|_| DbError::Checkpoint("checkpoint ID allocation timed out".into()))?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("checkpoint ID allocation failed: {error}"))
                    })?;
            let successor = checked_successor_epoch(checkpoint_id, "advancing allocation")?;
            let mut floor = self.next_id_floor.load(Ordering::Acquire);
            loop {
                if checkpoint_id < floor {
                    break;
                }
                match self.next_id_floor.compare_exchange_weak(
                    floor,
                    successor.max(floor),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(CheckpointAttempt::canonical(checkpoint_id)),
                    Err(observed) => floor = observed,
                }
            }
            tokio::task::yield_now().await;
        }
    }

    pub(crate) async fn allocate_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint allocator lock timed out".into()))?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} must be consumed before allocating another checkpoint",
                reservation.attempt().epoch
            )));
        }
        self.allocate_fresh_until(deadline).await
    }

    async fn reserve_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("sink epoch allocator lock timed out".into()))?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} is already reserved",
                reservation.attempt().epoch
            )));
        }
        let attempt = self.allocate_fresh_until(deadline).await?;
        *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::Opening(attempt));
        Ok(attempt)
    }

    fn mark_sink_epoch_ready(&self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Opening(current)) if current == attempt => {
                *reservation = Some(SinkEpochReservation::Ready(attempt));
                Ok(())
            }
            current => Err(DbError::Checkpoint(format!(
                "sink epoch reservation mismatch for {attempt:?}: {current:?}"
            ))),
        }
    }

    fn mark_sink_epoch_in_doubt(&self, attempt: CheckpointAttempt) {
        *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::InDoubt(attempt));
    }

    pub(crate) async fn consume_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint allocator lock timed out".into()))?;
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Ready(attempt)) => {
                reservation.take();
                Ok(attempt)
            }
            Some(SinkEpochReservation::Opening(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} is still opening",
                attempt.epoch
            ))),
            Some(SinkEpochReservation::InDoubt(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} requires recovery",
                attempt.epoch
            ))),
            None => Err(DbError::Checkpoint(
                "checkpoint-committable sinks have no open epoch".into(),
            )),
        }
    }

    fn clear_sink_epoch(&self, attempt: CheckpointAttempt) {
        let mut reservation = self.sink_epoch_reservation.lock();
        if reservation.is_some_and(|current| current.attempt() == attempt) {
            reservation.take();
        }
    }

    pub(crate) fn peek_epoch(&self) -> u64 {
        use std::sync::atomic::Ordering;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            reservation.attempt().epoch
        } else {
            self.next_id_floor.load(Ordering::Acquire)
        }
    }

    pub(crate) fn advance_epoch_to(&self, epoch: u64) {
        use std::sync::atomic::Ordering;
        self.next_id_floor.fetch_max(epoch, Ordering::AcqRel);
        self.observed_id_floor.fetch_max(epoch, Ordering::AcqRel);
    }
}

#[cfg(feature = "cluster")]
pub(crate) type QuorumPeer = laminar_core::cluster::discovery::NodeId;

#[derive(Debug, Clone)]
pub(crate) enum QuorumStage {
    RunInline,
    #[cfg(feature = "cluster")]
    Captured {
        cluster_watermark: laminar_core::checkpoint::CheckpointWatermark,
        participants: Vec<QuorumPeer>,
        leader_proof: LeaderProof,
    },
}

/// Follower-local durability state after immutable capture ownership has been acknowledged.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FollowerPrepareOutcome {
    /// Manifest persistence completed with an acknowledgement.
    Prepared,
    /// Manifest Create may be visible even though its acknowledgement was lost. The captured
    /// phase-one state must remain intact until an authoritative Commit or Abort is observed.
    InDoubt,
}

#[cfg(feature = "cluster")]
pub(crate) struct PrepareQuorum<'a> {
    attempt: CheckpointAttempt,
    local_watermark: laminar_core::checkpoint::CheckpointWatermark,
    assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_proof: &'a LeaderProof,
    flags: u64,
}

#[cfg(feature = "cluster")]
impl<'a> PrepareQuorum<'a> {
    pub(crate) const fn new(
        attempt: CheckpointAttempt,
        local_watermark: laminar_core::checkpoint::CheckpointWatermark,
        assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
        leader_proof: &'a LeaderProof,
        flags: u64,
    ) -> Self {
        Self {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            flags,
        }
    }
}

pub(crate) struct RegisteredSink {
    name: String,
    handle: crate::sink_task::SinkTaskHandle,
}

struct PackedCheckpoint {
    manifest: CheckpointManifest,
    node_data: Vec<Bytes>,
}

#[cfg(test)]
mod artifact_tests;

#[cfg(test)]
mod external_sink_tests;

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use laminar_core::checkpoint::ObjectStoreCheckpointStore;
    #[cfg(feature = "cluster")]
    use laminar_core::checkpoint_decision::CheckpointDecisionStore;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn outcome_recording_keeps_internal_and_prometheus_counts_in_lockstep() {
        let store = ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "metrics");
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
        coordinator.set_metrics(Arc::clone(&metrics));

        coordinator.record_checkpoint_outcome(
            true,
            CheckpointAttempt::canonical(1),
            Duration::from_millis(10),
            Some(37),
        );
        coordinator.record_checkpoint_outcome(
            false,
            CheckpointAttempt::canonical(2),
            Duration::from_millis(20),
            None,
        );

        let stats = coordinator.stats();
        assert_eq!((stats.completed, stats.failed), (1, 1));
        assert_eq!(stats.last_duration, Some(Duration::from_millis(20)));
        assert_eq!(metrics.checkpoints_completed.get(), 1);
        assert_eq!(metrics.checkpoints_failed.get(), 1);
        assert_eq!(metrics.checkpoint_duration.get_sample_count(), 2);
        assert_eq!(metrics.checkpoint_epoch.get(), 2);
        assert_eq!(metrics.checkpoint_size_bytes.get(), 37);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_installs_leader_certified_sink_epoch_without_allocating() {
        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let decisions = Arc::new(CheckpointDecisionStore::new(objects));
        let leader = EpochAllocator::new();
        leader.bind_decision_store(Arc::clone(&decisions)).unwrap();
        let follower = EpochAllocator::new();
        follower.bind_decision_store(decisions).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

        let leader_attempt = leader.reserve_sink_epoch_until(deadline).await.unwrap();
        follower
            .reserve_certified_sink_epoch_until(leader_attempt, deadline)
            .await
            .unwrap();
        follower.mark_sink_epoch_ready(leader_attempt).unwrap();
        leader.mark_sink_epoch_ready(leader_attempt).unwrap();

        assert_eq!(
            follower.consume_sink_epoch_until(deadline).await.unwrap(),
            leader_attempt
        );
        assert_eq!(
            leader.consume_sink_epoch_until(deadline).await.unwrap(),
            leader_attempt
        );
        assert_eq!(
            leader.reserve_sink_epoch_until(deadline).await.unwrap(),
            CheckpointAttempt::canonical(2),
            "the follower must not consume a second global checkpoint ID"
        );
    }
}

#[cfg(test)]
mod sparse_capture_tests;

#[cfg(all(test, feature = "cluster"))]
mod subscription_output_tests;

#[cfg(all(test, feature = "cluster"))]
mod handoff_tests;

pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    store: Arc<dyn CheckpointStore>,
    allocator: Arc<EpochAllocator>,
    phase: CheckpointPhase,
    sinks: Vec<RegisteredSink>,
    assignment_version: u64,
    assignment_scoped_sources: HashSet<String>,
    owned_vnodes: Vec<u32>,
    pipeline_identity: Option<PipelineIdentity>,
    deployment_id: Option<String>,
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    active_sink_witness: Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>,
    prepared: HashMap<CheckpointAttempt, (Arc<CheckpointManifest>, Bytes)>,
    last_committed_manifest: Option<Arc<CheckpointManifest>>,
    last_committed_ref: Option<CommittedCheckpointRef>,
    last_committed_source_watermarks: Option<(CommittedCheckpointRef, BTreeMap<String, i64>)>,
    failure_requires_recovery: bool,
    local_watermark: laminar_core::checkpoint::CheckpointWatermark,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    #[cfg(feature = "cluster")]
    recovery_graph_payload_limit: usize,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    last_checkpoint_duration: Option<Duration>,
    duration_histogram: DurationHistogram,
    total_bytes_written: u64,
    prom: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    gc_requests: tokio::sync::watch::Sender<Option<GcRequest>>,
    gc_task: tokio::task::JoinHandle<()>,
}

impl CheckpointCoordinator {
    pub fn new(config: CheckpointConfig, store: Box<dyn CheckpointStore>) -> Result<Self, DbError> {
        laminar_core::checkpoint::checkpoint_store::validate_max_checkpoint_node_data_bytes(
            config.max_node_data_bytes,
        )
        .map_err(|error| DbError::Config(format!("checkpoint.max_node_data_bytes: {error}")))?;
        if store.max_node_data_bytes() != config.max_node_data_bytes {
            return Err(DbError::Config(format!(
                "checkpoint store node-data limit {} does not match checkpoint.max_node_data_bytes {}",
                store.max_node_data_bytes(),
                config.max_node_data_bytes
            )));
        }
        if store.participant_id() == 0 {
            return Err(DbError::Config(
                "checkpoint participant ID must be nonzero".into(),
            ));
        }
        let vnode_count = store.key_group_count().get();
        #[cfg(feature = "cluster")]
        let recovery_graph_payload_limit =
            usize::try_from(config.max_node_data_bytes).unwrap_or(usize::MAX);
        let store: Arc<dyn CheckpointStore> = Arc::from(store);
        let (gc_requests, gc_receiver) = tokio::sync::watch::channel(None);
        let gc_task = tokio::spawn(run_gc_worker(Arc::clone(&store), gc_receiver));
        Ok(Self {
            allocator: Arc::new(EpochAllocator::new()),
            config,
            store,
            phase: CheckpointPhase::Idle,
            sinks: Vec::new(),
            assignment_version: 0,
            assignment_scoped_sources: HashSet::new(),
            owned_vnodes: (0..u32::from(vnode_count)).collect(),
            pipeline_identity: None,
            deployment_id: None,
            decision_store: None,
            active_sink_witness: None,
            prepared: HashMap::new(),
            last_committed_manifest: None,
            last_committed_ref: None,
            last_committed_source_watermarks: None,
            failure_requires_recovery: false,
            local_watermark: laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            #[cfg(feature = "cluster")]
            recovery_graph_payload_limit,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            total_bytes_written: 0,
            prom: None,
            gc_requests,
            gc_task,
        })
    }

    pub fn set_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        self.allocator.bind_decision_store(Arc::clone(&store))?;
        self.decision_store = Some(store);
        Ok(())
    }

    pub async fn bind_durable_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        let deployment_id = store
            .load_or_create_deployment_id()
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("load checkpoint deployment identity: {error}"))
            })?;
        self.set_decision_store(store)?;
        self.bind_deployment_id(deployment_id)
    }

    pub(crate) fn bind_pipeline_identity(
        &mut self,
        identity: PipelineIdentity,
    ) -> Result<(), DbError> {
        match self.pipeline_identity.as_ref() {
            None => {
                self.pipeline_identity = Some(identity);
                Ok(())
            }
            Some(current) if current == &identity => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "pipeline identity cannot change while checkpointing is active".into(),
            )),
        }
    }

    pub(crate) fn bound_pipeline_identity(&self) -> Result<PipelineIdentity, DbError> {
        self.pipeline_identity.clone().ok_or_else(|| {
            DbError::Checkpoint("checkpoint coordinator has no pipeline identity".into())
        })
    }

    pub(crate) fn bind_deployment_id(&mut self, deployment_id: String) -> Result<(), DbError> {
        let canonical = uuid::Uuid::parse_str(&deployment_id)
            .is_ok_and(|id| !id.is_nil() && id.to_string() == deployment_id);
        if !canonical {
            return Err(DbError::Checkpoint(
                "checkpoint deployment identity must be a canonical non-nil UUID".into(),
            ));
        }
        match self.deployment_id.as_ref() {
            None => {
                self.deployment_id = Some(deployment_id);
                Ok(())
            }
            Some(current) if current == &deployment_id => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "deployment identity cannot change while checkpointing is active".into(),
            )),
        }
    }

    fn expected_pipeline_identity(&self) -> Result<PipelineIdentity, DbError> {
        self.bound_pipeline_identity()
    }

    fn expected_deployment_id(&self) -> Result<&str, DbError> {
        self.deployment_id.as_deref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint deployment identity is not bound".into())
        })
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn bound_deployment_id(&self) -> Result<&str, DbError> {
        self.expected_deployment_id()
    }

    #[cfg(feature = "cluster")]
    pub fn set_cluster_controller(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        self.cluster_controller = Some(controller);
    }

    #[cfg(feature = "cluster")]
    #[must_use]
    pub(crate) fn participant_id(&self) -> u64 {
        self.store.participant_id()
    }

    pub fn set_assignment_version(&mut self, version: u64) {
        self.assignment_version = version;
    }

    pub(crate) fn set_assignment_scoped_sources(
        &mut self,
        sources: impl IntoIterator<Item = String>,
    ) {
        self.assignment_scoped_sources = sources.into_iter().collect();
    }

    pub fn set_vnode_set(&mut self, mut vnodes: Vec<u32>) {
        vnodes.sort_unstable();
        vnodes.dedup();
        self.owned_vnodes = vnodes;
    }

    pub fn set_local_watermark(
        &mut self,
        watermark: laminar_core::checkpoint::CheckpointWatermark,
    ) {
        self.local_watermark = watermark;
    }

    pub fn set_metrics(&mut self, prom: Arc<crate::engine_metrics::EngineMetrics>) {
        self.prom = Some(prom);
    }

    fn emit_checkpoint_metrics(
        &self,
        success: bool,
        attempt: CheckpointAttempt,
        duration: Duration,
        checkpoint_bytes: Option<u64>,
    ) {
        let Some(metrics) = self.prom.as_ref() else {
            return;
        };
        if success {
            metrics.checkpoints_completed.inc();
            if let Some(checkpoint_bytes) = checkpoint_bytes {
                metrics
                    .checkpoint_size_bytes
                    .set(i64::try_from(checkpoint_bytes).unwrap_or(i64::MAX));
            }
        } else {
            metrics.checkpoints_failed.inc();
            warn!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                "checkpoint failure metric recorded"
            );
        }
        metrics
            .checkpoint_epoch
            .set(i64::try_from(attempt.epoch).unwrap_or(i64::MAX));
        metrics.checkpoint_duration.observe(duration.as_secs_f64());
    }

    fn record_checkpoint_outcome(
        &mut self,
        success: bool,
        attempt: CheckpointAttempt,
        duration: Duration,
        checkpoint_bytes: Option<u64>,
    ) {
        if success {
            self.checkpoints_completed = self.checkpoints_completed.saturating_add(1);
        } else {
            self.checkpoints_failed = self.checkpoints_failed.saturating_add(1);
        }
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(success, attempt, duration, checkpoint_bytes);
    }

    #[must_use]
    pub fn phase(&self) -> CheckpointPhase {
        self.phase
    }

    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.allocator.peek_epoch()
    }

    #[must_use]
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    #[must_use]
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    #[must_use]
    pub fn store(&self) -> &dyn CheckpointStore {
        self.store.as_ref()
    }

    async fn allocate_attempt_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        if self.has_checkpoint_committable_sinks() {
            self.allocator.consume_sink_epoch_until(deadline).await
        } else {
            self.allocator.allocate_until(deadline).await
        }
    }

    async fn record_outcome_until(
        &self,
        attempt: CheckpointAttempt,
        verdict: laminar_core::checkpoint_decision::CheckpointVerdict,
        committed_checkpoint: Option<CommittedCheckpointRef>,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<laminar_core::checkpoint_decision::CheckpointOutcome, DbError> {
        use laminar_core::checkpoint_decision::RecordOutcomeResult;

        #[cfg(feature = "cluster")]
        let result = if let Some(controller) = self.cluster_controller.as_ref() {
            let proof = leader_proof
                .as_ref()
                .ok_or_else(|| DbError::Checkpoint("cluster outcome has no leader proof".into()))?;
            let fence = assignment_fence.clone().ok_or_else(|| {
                DbError::Checkpoint("cluster outcome has no assignment fence".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            tokio::time::timeout_at(
                deadline,
                authority.record_cluster_outcome(
                    proof,
                    attempt.epoch,
                    attempt.checkpoint_id,
                    fence,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("cluster outcome create: {error}")))?
        } else {
            let store = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("checkpoint outcome requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                store.record_outcome(
                    attempt.epoch,
                    attempt.checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("checkpoint outcome create: {error}")))?
        };
        #[cfg(not(feature = "cluster"))]
        let result = {
            let store = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("checkpoint outcome requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                store.record_outcome(
                    attempt.epoch,
                    attempt.checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("checkpoint outcome create: {error}")))?
        };

        let winner = match result {
            RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => {
                outcome
            }
            RecordOutcomeResult::Conflict { winner } => winner,
        };
        if winner.epoch != attempt.epoch
            || winner.checkpoint_id != attempt.checkpoint_id
            || winner.verdict != verdict
            || winner.committed_checkpoint != committed_checkpoint
            || winner.assignment_fence != assignment_fence
            || winner.leader_proof != leader_proof
            || winner.deployment_id != self.expected_deployment_id()?
        {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} lost its immutable outcome race",
                attempt.checkpoint_id
            )));
        }
        Ok(winner)
    }

    async fn abort_attempt_until(
        &mut self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.phase = CheckpointPhase::Deciding;
        #[cfg(feature = "cluster")]
        let cluster_scope = self.cluster_controller.is_some();
        #[cfg(not(feature = "cluster"))]
        let cluster_scope = false;
        self.record_outcome_until(
            attempt,
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
            None,
            assignment_fence,
            leader_proof,
            deadline,
        )
        .await?;
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let rollback = self
            .rollback_sinks_until(attempt.epoch, cleanup_deadline)
            .await;
        let witness_cleanup = if rollback.is_ok() {
            self.clear_sink_witness_until(cleanup_deadline).await
        } else {
            Ok(())
        };
        let artifact_cleanup = if cluster_scope {
            self.failure_requires_recovery = true;
            Ok(())
        } else {
            self.cleanup_local_checkpoint_artifacts_until(attempt, cleanup_deadline)
                .await
        };
        rollback?;
        witness_cleanup?;
        artifact_cleanup?;
        self.allocator.advance_epoch_to(checked_successor_epoch(
            attempt.epoch,
            "closing an aborted checkpoint",
        )?);
        self.phase = CheckpointPhase::Idle;
        Ok(())
    }

    fn failed_result(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        error: String,
        disposition: CheckpointFailureDisposition,
    ) -> CheckpointResult {
        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        self.record_checkpoint_outcome(false, attempt, duration, None);
        if disposition == CheckpointFailureDisposition::RequiresRecovery {
            self.failure_requires_recovery = true;
        }
        CheckpointResult {
            success: false,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration,
            error: Some(error),
            failure_disposition: Some(disposition),
        }
    }

    async fn fail_before_commit(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        error: DbError,
        flags: u64,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        _attempt_deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> CheckpointResult {
        #[cfg(not(feature = "cluster"))]
        let _ = flags;
        // Once an exact attempt has been reserved, failure settlement owns a private cleanup
        // budget. The attempt deadline fences new capture/durable work, but must not cancel the
        // durable Abort and rollback that make the attempt terminal.
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let mut message = error.to_string();
        if let Err(seal) = self
            .seal_sink_epoch_until(attempt.epoch, cleanup_deadline)
            .await
        {
            self.failure_requires_recovery = true;
            message = format!("{message}; {seal}");
        }
        match self
            .abort_attempt_until(
                attempt,
                assignment_fence.clone(),
                leader_proof.clone(),
                cleanup_deadline,
            )
            .await
        {
            Ok(()) => {
                // Durable Abort is already terminal. Bound its best-effort cluster hint under a
                // fresh, explicit cleanup window rather than reviving the expired attempt deadline
                // or allowing notification I/O to hang.
                #[cfg(feature = "cluster")]
                if let Some(controller) = self.cluster_controller.as_ref() {
                    let notification_deadline =
                        tokio::time::Instant::now() + self.config.cleanup_timeout;
                    publish_terminal_hint_until(
                        notification_deadline,
                        controller.announce_barrier(&BarrierAnnouncement {
                            epoch: attempt.epoch,
                            checkpoint_id: attempt.checkpoint_id,
                            assignment_fence,
                            leader_proof,
                            phase: Phase::Abort,
                            flags,
                        }),
                    )
                    .await;
                }
                // A slow best-effort hint must not consume the local successor epoch's required
                // continuation budget.
                let continuation_deadline =
                    tokio::time::Instant::now() + self.config.cleanup_timeout;
                let requires_recovery = self.failure_requires_recovery;
                let successor = if !requires_recovery && self.has_checkpoint_committable_sinks() {
                    self.begin_sink_epoch_until(continuation_deadline, sink_epoch_publication)
                        .await
                        .err()
                } else {
                    None
                };
                let (error, disposition) = match (requires_recovery, successor) {
                    (true, _) => (message, CheckpointFailureDisposition::RequiresRecovery),
                    (false, Some(successor)) => (
                        format!("{message}; successor sink epoch failed: {successor}"),
                        CheckpointFailureDisposition::RequiresRecovery,
                    ),
                    (false, None) => (message, CheckpointFailureDisposition::Retryable),
                };
                self.failed_result(attempt, started, error, disposition)
            }
            Err(abort) => self.failed_result(
                attempt,
                started,
                format!("{message}; durable Abort or rollback failed: {abort}"),
                CheckpointFailureDisposition::RequiresRecovery,
            ),
        }
    }

    async fn run_checkpoint_attempt(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
        deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<CheckpointResult, DbError> {
        require_canonical_attempt(attempt, "checkpoint admission")?;
        let (flags, assignment_fence, terminal_handoff) = (
            request.flags,
            request.assignment_fence.clone(),
            sink_epoch_admission::is_terminal_handoff(
                request.flags,
                request.handoff_replay_pending,
            ),
        );
        #[cfg(feature = "cluster")]
        let validation_proof = match &quorum {
            QuorumStage::Captured { leader_proof, .. } => Some(leader_proof.clone()),
            QuorumStage::RunInline => self
                .cluster_controller
                .as_ref()
                .and_then(|controller| controller.capture_leader_proof()),
        };
        #[cfg(not(feature = "cluster"))]
        let validation_proof = None;
        if self.failure_requires_recovery {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    DbError::Checkpoint(
                        "a prior checkpoint has unresolved durable or sink state".into(),
                    ),
                    flags,
                    assignment_fence,
                    validation_proof,
                    deadline,
                    sink_epoch_publication,
                )
                .await);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    DbError::Checkpoint("checkpoint deadline expired before durable work".into()),
                    flags,
                    assignment_fence,
                    validation_proof,
                    deadline,
                    sink_epoch_publication,
                )
                .await);
        }
        if let Err(error) = self.validate_request(&request) {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    error,
                    flags,
                    assignment_fence,
                    validation_proof,
                    deadline,
                    sink_epoch_publication,
                )
                .await);
        }

        let assignment_fence = request.assignment_fence.clone();
        #[cfg(feature = "cluster")]
        let (scope, leader_proof, quorum_watermark) =
            if let Some(controller) = self.cluster_controller.clone() {
                let fence = assignment_fence.as_ref().ok_or_else(|| {
                    DbError::Checkpoint("cluster checkpoint has no assignment fence".into())
                })?;
                let (proof, participants, cluster_watermark) = match quorum {
                    QuorumStage::RunInline => {
                        return Ok(self
                            .fail_before_commit(
                                attempt,
                                started,
                                DbError::Checkpoint(
                                    "cluster checkpoint reached durable execution without a \
                                 precomputed certified quorum"
                                        .into(),
                                ),
                                flags,
                                assignment_fence,
                                validation_proof,
                                deadline,
                                sink_epoch_publication,
                            )
                            .await);
                    }
                    QuorumStage::Captured {
                        cluster_watermark,
                        participants,
                        leader_proof,
                    } => (leader_proof, participants, cluster_watermark),
                };
                if let Err(error) =
                    self.validate_captured_quorum(&controller, fence, participants, &proof)
                {
                    return Ok(self
                        .fail_before_commit(
                            attempt,
                            started,
                            error,
                            flags,
                            assignment_fence,
                            Some(proof),
                            deadline,
                            sink_epoch_publication,
                        )
                        .await);
                }
                (
                    CheckpointScope::Cluster,
                    Some(proof),
                    Some(cluster_watermark),
                )
            } else {
                let _ = quorum;
                (CheckpointScope::Local, None, None)
            };
        #[cfg(not(feature = "cluster"))]
        let (scope, leader_proof, quorum_watermark) = {
            let _ = quorum;
            (CheckpointScope::Local, None, None)
        };

        self.phase = CheckpointPhase::PreCommitting;
        let descriptors = match self.pre_commit_sinks_until(attempt.epoch, deadline).await {
            Ok(descriptors) => descriptors,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let packed = match self
            .pack_checkpoint(attempt, request, descriptors, deadline)
            .await
        {
            Ok(packed) => packed,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let local_manifest_bytes = match self.persist_checkpoint_until(&packed, deadline).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };

        let manifests = match self
            .await_prepared_participant_manifests(
                attempt,
                assignment_fence.as_ref(),
                (packed.manifest.clone(), local_manifest_bytes),
                deadline,
            )
            .await
        {
            Ok(manifests) => manifests,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let predecessor = match self
            .authoritative_committed_predecessor_until(scope, deadline)
            .await
        {
            Ok(predecessor) => predecessor,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let predecessor_source_watermarks = match self
            .predecessor_source_watermarks_until(predecessor.as_ref(), deadline)
            .await
        {
            Ok(source_watermarks) => source_watermarks,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        let index = match self
            .build_validated_committed_index_until(
                attempt,
                scope,
                assignment_fence.clone(),
                predecessor.clone(),
                &predecessor_source_watermarks,
                &manifests,
                quorum_watermark,
                deadline,
            )
            .await
        {
            Ok(index) => index,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };
        #[cfg(feature = "cluster")]
        let subscription_commit_stats = (scope == CheckpointScope::Cluster)
            .then(|| subscription_output::subscription_commit_stats(&manifests));
        #[cfg(all(debug_assertions, feature = "cluster"))]
        checkpoint_kill_gate(
            "leader",
            attempt,
            predecessor
                .as_ref()
                .map(|reference| (reference.checkpoint_id, reference.epoch)),
        )
        .await;
        let reference = match self.create_committed_index_until(&index, deadline).await {
            Ok(reference) => reference,
            Err(error) => {
                return Ok(self
                    .fail_before_commit(
                        attempt,
                        started,
                        error,
                        flags,
                        assignment_fence,
                        leader_proof,
                        deadline,
                        sink_epoch_publication,
                    )
                    .await);
            }
        };

        self.phase = CheckpointPhase::Deciding;
        #[cfg(feature = "cluster")]
        let commit_visibility_started = Instant::now();
        let outcome = self
            .record_outcome_until(
                attempt,
                laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
                Some(reference.clone()),
                assignment_fence.clone(),
                leader_proof.clone(),
                deadline,
            )
            .await;
        if let Err(error) = outcome {
            return Ok(self.failed_result(
                attempt,
                started,
                format!("commit outcome is in-doubt: {error}"),
                CheckpointFailureDisposition::RequiresRecovery,
            ));
        }
        #[cfg(feature = "cluster")]
        self.record_subscription_commit(
            subscription_commit_stats,
            commit_visibility_started.elapsed(),
            attempt,
        );

        let predecessor_checkpoint_id = index
            .predecessor
            .as_ref()
            .map_or(0, |reference| reference.checkpoint_id);
        self.last_committed_ref = Some(reference.clone());
        self.last_committed_source_watermarks = Some((reference, index.source_watermarks.clone()));
        self.last_committed_manifest = manifests
            .iter()
            .find(|(manifest, _)| manifest.participant_id == self.store.participant_id())
            .map(|(manifest, _)| Arc::new(manifest.clone()));
        self.prepared.remove(&attempt);
        self.allocator.advance_epoch_to(checked_successor_epoch(
            attempt.epoch,
            "closing a committed checkpoint",
        )?);
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            controller
                .publish_committed_checkpoint_progress(
                    &index.channel_progress,
                    &index.source_watermarks,
                )
                .map_err(DbError::Checkpoint)?;
            // The durable Commit is already immutable. Its cluster hint is best-effort and must
            // not delay sink continuation or the terminal caller reply without bound.
            let notification_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
            publish_terminal_hint_until(
                notification_deadline,
                controller.announce_barrier(&BarrierAnnouncement {
                    epoch: attempt.epoch,
                    checkpoint_id: attempt.checkpoint_id,
                    assignment_fence: assignment_fence.clone(),
                    leader_proof: leader_proof.clone(),
                    phase: Phase::Commit,
                    flags,
                }),
            )
            .await;
        }

        let continuation_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let fencing_token = leader_proof.as_ref().map_or(1, |proof| proof.fencing_token);
        let participant_manifests = manifests
            .iter()
            .map(|(manifest, _)| manifest)
            .collect::<Vec<_>>();
        let continuation = self
            .commit_external_sinks_until(
                attempt,
                &participant_manifests,
                fencing_token,
                predecessor_checkpoint_id,
                continuation_deadline,
            )
            .await;
        let continuation = self
            .continue_committed_sink_epoch_until(
                continuation,
                &index,
                leader_proof.as_ref(),
                terminal_handoff,
                continuation_deadline,
                sink_epoch_publication,
            )
            .await;

        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        let checkpoint_bytes = self
            .last_committed_manifest
            .as_ref()
            .map(|manifest| manifest.node_data.object_length);
        self.record_checkpoint_outcome(true, attempt, duration, checkpoint_bytes);
        let continuation_error = continuation.err().map(|error| {
            self.failure_requires_recovery = true;
            format!(
                "checkpoint {} committed, but sink continuation requires recovery: {error}",
                attempt.checkpoint_id
            )
        });
        Ok(CheckpointResult {
            success: true,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration,
            error: continuation_error,
            failure_disposition: None,
        })
    }

    pub async fn checkpoint(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        #[cfg(feature = "cluster")]
        let local = self.cluster_controller.is_none();
        #[cfg(not(feature = "cluster"))]
        let local = true;
        if !local {
            return Err(DbError::Checkpoint(
                "cluster checkpoints require reserved pipeline admission and certified Prepare"
                    .into(),
            ));
        }
        let attempt = self.allocate_attempt_until(deadline).await?;
        if let Err(error) = self
            .begin_checkpoint_artifacts_until(attempt, None, None, deadline)
            .await
        {
            return Ok(self
                .fail_before_commit(
                    attempt,
                    started,
                    error,
                    request.flags,
                    request.assignment_fence.clone(),
                    None,
                    deadline,
                    SinkEpochPublication::Immediate,
                )
                .await);
        }
        self.run_checkpoint_attempt(
            request,
            attempt,
            QuorumStage::RunInline,
            started,
            deadline,
            SinkEpochPublication::Immediate,
        )
        .await
    }

    pub async fn checkpoint_with_offsets(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        self.checkpoint(request).await
    }

    pub(crate) async fn checkpoint_preallocated_started(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointResult, DbError> {
        self.run_checkpoint_attempt(
            request,
            attempt,
            quorum,
            started,
            deadline,
            SinkEpochPublication::DeferredToTail,
        )
        .await
    }

    pub(crate) async fn abandon_epoch_until(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        error: String,
        flags: u64,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<CheckpointResult, DbError> {
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "checkpoint abandonment",
        )?;
        let started = Instant::now();
        Ok(self
            .fail_before_commit(
                attempt,
                started,
                DbError::Checkpoint(error),
                flags,
                assignment_fence,
                leader_proof,
                deadline,
                sink_epoch_publication,
            )
            .await)
    }

    #[cfg(feature = "cluster")]
    fn validate_cluster_watermark_candidate(
        controller: &laminar_core::cluster::control::ClusterController,
        observed: laminar_core::checkpoint::CheckpointWatermark,
    ) -> Result<laminar_core::checkpoint::CheckpointWatermark, String> {
        observed
            .validate()
            .map_err(|error| format!("invalid checkpoint watermark: {error}"))?;
        match (controller.cluster_min_watermark(), observed) {
            (Some(current), laminar_core::checkpoint::CheckpointWatermark::Active(watermark))
                if watermark < current =>
            {
                Err(format!(
                    "cluster watermark {watermark} regresses committed frontier {current}"
                ))
            }
            (Some(current), laminar_core::checkpoint::CheckpointWatermark::Uninitialized) => Err(
                format!("uninitialized watermark cannot replace committed frontier {current}"),
            ),
            _ => Ok(observed),
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn run_prepare_quorum(
        controller: &Arc<laminar_core::cluster::control::ClusterController>,
        quorum_timeout: Duration,
        request: PrepareQuorum<'_>,
    ) -> Result<
        (
            laminar_core::checkpoint::CheckpointWatermark,
            Vec<laminar_core::cluster::discovery::NodeId>,
            bool,
        ),
        String,
    > {
        let PrepareQuorum {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            flags,
        } = request;
        if !controller.proof_is_live(leader_proof) {
            return Err("leader proof is stale before checkpoint Prepare".into());
        }
        let announcement = BarrierAnnouncement {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_fence: Some(assignment_fence.clone()),
            leader_proof: Some(leader_proof.clone()),
            phase: Phase::Prepare,
            flags,
        };
        let mut followers = assignment_fence
            .participants
            .iter()
            .map(|participant| laminar_core::cluster::discovery::NodeId(participant.node_id))
            .filter(|participant| *participant != controller.instance_id())
            .collect::<Vec<_>>();
        followers.sort_unstable_by_key(|participant| participant.0);

        let outcome = controller
            .wait_for_quorum(&announcement, &followers, quorum_timeout)
            .await;
        if !controller.proof_is_live(leader_proof) {
            return Err("leader proof expired during checkpoint Prepare".into());
        }
        match outcome {
            QuorumOutcome::Reached {
                follower_watermark,
                ref acks,
                handoff_replay_pending,
            } => {
                controller.note_responsive(acks);
                let watermark = if followers.is_empty() {
                    local_watermark
                } else {
                    local_watermark.cluster_min(follower_watermark)
                };
                Ok((
                    Self::validate_cluster_watermark_candidate(controller, watermark)?,
                    followers,
                    handoff_replay_pending,
                ))
            }
            QuorumOutcome::TimedOut { missing, .. } => {
                controller.note_unresponsive(&missing);
                Err(format!(
                    "checkpoint Prepare timed out waiting for {} participants",
                    missing.len()
                ))
            }
            QuorumOutcome::Failed { failures } => Err(format!(
                "checkpoint Prepare failed on {} participants: {}",
                failures.len(),
                failures
                    .first()
                    .map_or("unknown", |(_, message)| message.as_str())
            )),
        }
    }

    #[cfg(feature = "cluster")]
    async fn certify_follower_assignment_until(
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
        context: &'static str,
    ) -> Result<(), DbError> {
        let certified = tokio::time::timeout_at(
            deadline,
            controller.checkpoint_assignment_fence_for_leader(fence.assignment_version, proof),
        )
        .await
        .map_err(|_| DbError::Checkpoint(format!("{context} authority validation timed out")))?;
        if certified.as_ref() != Some(fence) {
            return Err(DbError::Checkpoint(format!(
                "{context} authority is no longer current"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn validate_follower_prepare_context(
        controller: &laminar_core::cluster::control::ClusterController,
        request: &CheckpointRequest,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            laminar_core::checkpoint::CheckpointAssignmentFence,
            LeaderProof,
        ),
        DbError,
    > {
        use laminar_core::cluster::control::Phase;

        require_canonical_attempt(
            CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id),
            "follower Prepare",
        )?;
        if announcement.phase != Phase::Prepare {
            return Err(DbError::Checkpoint(
                "follower checkpoint did not originate from Prepare".into(),
            ));
        }
        let fence = request.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("follower checkpoint has no assignment fence".into())
        })?;
        let proof = announcement
            .leader_proof
            .as_ref()
            .ok_or_else(|| DbError::Checkpoint("follower checkpoint has no leader proof".into()))?;
        if announcement.assignment_fence.as_ref() != Some(fence)
            || announcement.flags != request.flags
            || !fence.contains(controller.instance_id().0)
            || fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id)
        {
            return Err(DbError::Checkpoint(
                "follower Prepare does not match the certified assignment".into(),
            ));
        }
        Self::certify_follower_assignment_until(
            controller,
            fence,
            proof,
            deadline,
            "follower Prepare",
        )
        .await?;
        Ok((fence.clone(), proof.clone()))
    }

    /// Legacy direct follower entry point.
    ///
    /// This API owns no callback-supervised immutable capture tail, so it deliberately publishes
    /// `Captured` only after local phase-one packing/persistence has returned. The streaming
    /// runtime uses its early-capture path instead and acknowledges immediately after transferring
    /// the sealed capture into the supervised follower tail.
    #[cfg(feature = "cluster")]
    pub async fn follower_checkpoint(
        &mut self,
        request: CheckpointRequest,
        announcement: laminar_core::cluster::control::BarrierAnnouncement,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::cluster::control::{BarrierAck, BarrierAckDisposition};

        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        let controller = self.cluster_controller.clone().ok_or_else(|| {
            DbError::Checkpoint("follower checkpoint has no cluster controller".into())
        })?;
        let (fence, proof) =
            Self::validate_follower_prepare_context(&controller, &request, &announcement, deadline)
                .await?;
        let handoff_replay_pending = request.handoff_replay_pending;
        let terminal_handoff =
            sink_epoch_admission::is_terminal_handoff(request.flags, handoff_replay_pending);
        let prepare_outcome = self
            .follower_prepare_acked_until(
                request,
                proof,
                announcement.epoch,
                announcement.checkpoint_id,
                deadline,
            )
            .await?;
        let captured_ack_error = match tokio::time::timeout_at(
            deadline,
            controller.ack_barrier(&BarrierAck {
                epoch: announcement.epoch,
                checkpoint_id: announcement.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags: announcement.flags,
                disposition: if handoff_replay_pending {
                    BarrierAckDisposition::CapturedWithReplay
                } else {
                    BarrierAckDisposition::Captured
                },
                error: None,
                watermark: self.local_watermark,
            }),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(format!("follower captured ack failed: {error}")),
            Err(_) => Some("follower captured ack timed out".to_string()),
        };
        if let Some(error) = captured_ack_error.as_deref() {
            // The local prepared image and any phase-one sink state cannot be discarded merely
            // because the best-effort Captured notification was ambiguous.  The leader may have
            // observed it and committed, so continue through exact terminal settlement.
            tracing::warn!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                %error,
                "follower Captured acknowledgement was not confirmed; awaiting authority"
            );
        }
        let required_settlement_deadline = deadline
            .checked_add(self.config.cleanup_timeout)
            .ok_or_else(|| DbError::Checkpoint("follower settlement deadline overflowed".into()))?;
        let decision_timeout = decision_timeout.max(
            required_settlement_deadline.saturating_duration_since(tokio::time::Instant::now()),
        );
        if prepare_outcome == FollowerPrepareOutcome::InDoubt {
            tracing::debug!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                "preserving in-doubt follower preparation through terminal observation"
            );
        }
        let committed = match Self::await_follower_decision(
            &controller,
            announcement.epoch,
            announcement.checkpoint_id,
            &fence,
            decision_timeout,
        )
        .await
        {
            Ok(committed) => committed,
            Err(settlement) => {
                self.failure_requires_recovery = true;
                let message = captured_ack_error.map_or_else(
                    || settlement.to_string(),
                    |ack| format!("{ack}; terminal settlement failed: {settlement}"),
                );
                return Err(DbError::Checkpoint(message));
            }
        };
        let result = self
            .follower_finish(
                announcement.epoch,
                announcement.checkpoint_id,
                committed,
                started,
                terminal_handoff,
            )
            .await;
        if result.is_err() {
            self.failure_requires_recovery = true;
        }
        result
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn await_follower_decision(
        controller: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::checkpoint_decision::CheckpointVerdict;
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower decision",
        )?;
        if !assignment_fence.is_canonical()
            || !assignment_fence.contains(controller.instance_id().0)
        {
            return Err(DbError::Checkpoint(
                "follower decision has an invalid assignment fence".into(),
            ));
        }
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
        })?;
        let deadline = tokio::time::Instant::now() + decision_timeout;
        loop {
            let settlement =
                tokio::time::timeout_at(deadline, authority.cluster_attempt_settlement(attempt))
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "follower decision timed out for checkpoint {checkpoint_id}"
                        ))
                    })?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("follower decision read failed: {error}"))
                    })?;
            if let Some(settlement) = settlement {
                let settled = CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id);
                match settled.relation_to(attempt) {
                    CheckpointAttemptRelation::Exact
                        if settlement.verdict == CheckpointVerdict::Abort =>
                    {
                        return Ok(false);
                    }
                    CheckpointAttemptRelation::Exact => {
                        let exact = tokio::time::timeout_at(
                            deadline,
                            authority.cluster_outcome_with_committed_checkpoint(epoch),
                        )
                        .await
                        .map_err(|_| {
                            DbError::Checkpoint("follower committed-index read timed out".into())
                        })?
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "follower committed-index read failed: {error}"
                            ))
                        })?
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "Commit outcome has no committed checkpoint index".into(),
                            )
                        })?;
                        let (outcome, index) = exact;
                        let index = index.ok_or_else(|| {
                            DbError::Checkpoint(
                                "Commit outcome has no committed checkpoint body".into(),
                            )
                        })?;
                        if outcome != settlement
                            || outcome.assignment_fence.as_ref() != Some(assignment_fence)
                            || index.epoch != epoch
                            || index.checkpoint_id != checkpoint_id
                            || index.assignment_fence.as_ref() != Some(assignment_fence)
                            || !index.participants.iter().any(|participant| {
                                participant.participant_id == controller.instance_id().0
                            })
                        {
                            return Err(DbError::Checkpoint(
                                "follower Commit does not match its prepared participant cut"
                                    .into(),
                            ));
                        }
                        index.validate().map_err(DbError::Checkpoint)?;
                        let source_watermarks = index
                            .effective_source_watermarks()
                            .map_err(DbError::Checkpoint)?;
                        controller
                            .publish_committed_checkpoint_progress(
                                &index.channel_progress,
                                &source_watermarks,
                            )
                            .map_err(DbError::Checkpoint)?;
                        return Ok(true);
                    }
                    CheckpointAttemptRelation::Newer => return Ok(false),
                    CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                        return Err(DbError::Checkpoint(
                            "follower observed an incompatible terminal checkpoint".into(),
                        ));
                    }
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(DbError::Checkpoint(format!(
                    "follower decision timed out for checkpoint {checkpoint_id}"
                )));
            }
            tokio::time::sleep(FOLLOWER_DECISION_POLL.min(remaining)).await;
        }
    }
}

impl std::fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointCoordinator")
            .field("phase", &self.phase)
            .field("participant", &self.store.participant_id())
            .field("sinks", &self.sinks.len())
            .field("completed", &self.checkpoints_completed)
            .field("failed", &self.checkpoints_failed)
            .finish_non_exhaustive()
    }
}

impl Drop for CheckpointCoordinator {
    fn drop(&mut self) {
        self.gc_task.abort();
    }
}

struct DurationHistogram {
    samples: Box<[u64; Self::CAPACITY]>,
    cursor: usize,
    count: usize,
}

impl DurationHistogram {
    const CAPACITY: usize = 100;

    fn new() -> Self {
        Self {
            samples: Box::new([0; Self::CAPACITY]),
            cursor: 0,
            count: 0,
        }
    }

    fn record(&mut self, duration: Duration) {
        self.samples[self.cursor] = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.cursor = (self.cursor + 1) % Self::CAPACITY;
        self.count = self.count.saturating_add(1).min(Self::CAPACITY);
    }

    fn percentiles(&self) -> (u64, u64, u64) {
        if self.count == 0 {
            return (0, 0, 0);
        }
        let mut values = self.samples[..self.count].to_vec();
        values.sort_unstable();
        let at = |numerator: usize| {
            let index = (self.count.saturating_sub(1) * numerator).div_ceil(100);
            values[index]
        };
        (at(50), at(95), at(99))
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckpointStats {
    pub completed: u64,
    pub failed: u64,
    pub last_duration: Option<Duration>,
    pub duration_p50_ms: u64,
    pub duration_p95_ms: u64,
    pub duration_p99_ms: u64,
    pub total_bytes_written: u64,
    pub current_phase: CheckpointPhase,
    pub current_epoch: u64,
}

impl CheckpointCoordinator {
    #[cfg(feature = "cluster")]
    pub(crate) fn last_committed_ref(&self) -> Option<&CommittedCheckpointRef> {
        self.last_committed_ref.as_ref()
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn set_last_committed_ref_for_test(&mut self, reference: CommittedCheckpointRef) {
        self.last_committed_source_watermarks = None;
        self.last_committed_ref = Some(reference);
    }

    pub(crate) fn last_committed_manifest(&self) -> Option<&CheckpointManifest> {
        self.last_committed_manifest.as_deref()
    }

    pub(crate) fn committed_manifest_needs_vnode_rebase(&self, attempt: CheckpointAttempt) -> bool {
        self.last_committed_manifest
            .as_ref()
            .is_some_and(|manifest| {
                manifest.checkpoint_id == attempt.checkpoint_id
                    && manifest.epoch == attempt.epoch
                    && manifest.referenced_chunks.len() >= REFERENCED_CHUNK_REBASE_THRESHOLD
            })
    }

    #[must_use]
    pub fn stats(&self) -> CheckpointStats {
        let (p50, p95, p99) = self.duration_histogram.percentiles();
        CheckpointStats {
            completed: self.checkpoints_completed,
            failed: self.checkpoints_failed,
            last_duration: self.last_checkpoint_duration,
            duration_p50_ms: p50 / 1_000,
            duration_p95_ms: p95 / 1_000,
            duration_p99_ms: p99 / 1_000,
            total_bytes_written: self.total_bytes_written,
            current_phase: self.phase,
            current_epoch: self.allocator.peek_epoch(),
        }
    }
}

#[must_use]
pub(crate) fn source_to_connector_checkpoint(cp: &SourceCheckpoint) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: cp.durable_offsets(),
        metadata: cp.metadata().clone(),
        input_channels: cp.input_channels().map(<[Vec<u8>]>::to_vec),
        source_assignment_version: cp.assignment_version(),
    }
}

#[must_use]
pub(crate) fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source = SourceCheckpoint::with_offsets(cp.offsets.clone());
    for (key, value) in &cp.metadata {
        source.set_metadata(key.clone(), value.clone());
    }
    if let Some(channels) = &cp.input_channels {
        source
            .set_input_channels(channels.clone())
            .expect("validated connector checkpoint input-channel inventory");
    }
    if let Some(version) = cp.source_assignment_version {
        source.bind_assignment_version(version);
    }
    source
}
