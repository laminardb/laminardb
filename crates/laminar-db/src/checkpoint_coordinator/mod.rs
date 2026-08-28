//! Checkpoint capture and participant persistence.

#![allow(clippy::disallowed_types)] // checkpoint control path

mod allocation;
mod artifacts;
mod attempt;
mod attempt_failure;
mod capture;
mod commit_index;
#[cfg(feature = "cluster")]
mod follower_completion;
#[cfg(feature = "cluster")]
mod follower_prepare;
#[cfg(feature = "cluster")]
mod follower_protocol;
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
use std::time::Duration;

use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use laminar_connectors::checkpoint::SourceCheckpoint;
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
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::error::DbError;
pub(crate) use allocation::EpochAllocator;
#[cfg(feature = "cluster")]
use allocation::SinkEpochReservation;
use allocation::{checked_successor_epoch, require_canonical_attempt};
use retention::{run_gc_worker, GcRequest};

const MAX_RETENTION_IO_CONCURRENCY: usize = 8;
const REFERENCED_CHUNK_REBASE_THRESHOLD: usize = 64;
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
