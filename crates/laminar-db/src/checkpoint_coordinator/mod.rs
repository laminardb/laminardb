//! Checkpoint capture and participant persistence.

#![allow(clippy::disallowed_types)] // checkpoint control path

#[cfg(feature = "cluster")]
mod follower_completion;
#[cfg(feature = "cluster")]
mod follower_prepare;
#[cfg(feature = "cluster")]
mod handoff;
mod recovery;
mod retention;
pub(crate) mod sink_epoch_admission;
#[cfg(feature = "cluster")]
mod subscription_output;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{stream::FuturesOrdered, stream::FuturesUnordered, StreamExt};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitPayload, MAX_COORDINATED_COMMIT_BATCH_BYTES,
    MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
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

const MAX_SINK_PHASE_ONE_CONCURRENCY: usize = 8;
const MAX_EXTERNAL_SINK_COMMIT_CONCURRENCY: usize = 8;
const MAX_RETENTION_IO_CONCURRENCY: usize = 8;
const REFERENCED_CHUNK_REBASE_THRESHOLD: usize = 64;
#[cfg(feature = "cluster")]
const FOLLOWER_DECISION_POLL: Duration = Duration::from_millis(250);
const PARTICIPANT_MANIFEST_POLL_INITIAL: Duration = Duration::from_millis(10);
const PARTICIPANT_MANIFEST_POLL_MAX: Duration = Duration::from_millis(250);

async fn await_participant_manifest_until<F, Fut>(
    participant_id: u64,
    attempt: CheckpointAttempt,
    deadline: tokio::time::Instant,
    mut load: F,
) -> Result<CheckpointManifest, DbError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<CheckpointManifest>, DbError>>,
{
    let mut backoff = PARTICIPANT_MANIFEST_POLL_INITIAL;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(format!(
                "participant {participant_id} checkpoint {} epoch {} manifest readiness timed out",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        match tokio::time::timeout_at(deadline, load()).await {
            Ok(Ok(Some(manifest))) => {
                if manifest.participant_id != participant_id
                    || manifest.checkpoint_id != attempt.checkpoint_id
                    || manifest.epoch != attempt.epoch
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {participant_id} published an invalid manifest readiness marker for checkpoint {} epoch {}",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
                return Ok(manifest);
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(DbError::Checkpoint(format!(
                    "participant {participant_id} checkpoint {} epoch {} manifest read timed out",
                    attempt.checkpoint_id, attempt.epoch
                )));
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(DbError::Checkpoint(format!(
                "participant {participant_id} checkpoint {} epoch {} manifest readiness timed out",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        tokio::time::sleep_until((now + backoff).min(deadline)).await;
        backoff = backoff.saturating_mul(2).min(PARTICIPANT_MANIFEST_POLL_MAX);
    }
}

#[cfg(test)]
mod participant_manifest_readiness_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn missing_manifest_is_retried_until_it_becomes_ready() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut manifest = CheckpointManifest::new(7, 7);
        manifest.bind_participant(2);
        let loaded = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            tokio::time::Instant::now() + Duration::from_secs(1),
            {
                let attempts = Arc::clone(&attempts);
                let manifest = manifest.clone();
                move || {
                    let attempts = Arc::clone(&attempts);
                    let manifest = manifest.clone();
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                            Ok(None)
                        } else {
                            Ok(Some(manifest))
                        }
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(loaded.checkpoint_id, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn wrong_manifest_identity_fails_without_polling() {
        for (manifest, field) in [
            (CheckpointManifest::new(7, 7), "participant"),
            (
                {
                    let mut manifest = CheckpointManifest::new(8, 7);
                    manifest.bind_participant(2);
                    manifest
                },
                "checkpoint",
            ),
            (
                {
                    let mut manifest = CheckpointManifest::new(7, 8);
                    manifest.bind_participant(2);
                    manifest
                },
                "epoch",
            ),
        ] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let started = tokio::time::Instant::now();
            let error = await_participant_manifest_until(
                2,
                CheckpointAttempt::canonical(7),
                started + Duration::from_secs(1),
                {
                    let attempts = Arc::clone(&attempts);
                    move || {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        let manifest = manifest.clone();
                        async move { Ok(Some(manifest)) }
                    }
                },
            )
            .await
            .unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("invalid manifest readiness marker"),
                "wrong {field} returned {error}"
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
            assert_eq!(tokio::time::Instant::now(), started);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_manifest_read_fails_without_polling() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let started = tokio::time::Instant::now();
        let error = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            started + Duration::from_secs(1),
            {
                let attempts = Arc::clone(&attempts);
                move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err(DbError::Checkpoint(
                            "invalid participant manifest readiness marker".into(),
                        ))
                    }
                }
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("invalid participant manifest"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(tokio::time::Instant::now(), started);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_missing_and_blocked_reads_share_the_exact_deadline() {
        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_millis(100);
        let missing = await_participant_manifest_until(
            2,
            CheckpointAttempt::canonical(7),
            deadline,
            || async { Ok(None) },
        );
        let blocked =
            await_participant_manifest_until(3, CheckpointAttempt::canonical(7), deadline, || {
                std::future::pending::<Result<Option<CheckpointManifest>, DbError>>()
            });

        let (missing, blocked) = tokio::join!(missing, blocked);

        assert!(missing
            .unwrap_err()
            .to_string()
            .contains("readiness timed out"));
        assert!(blocked.unwrap_err().to_string().contains("read timed out"));
        assert_eq!(tokio::time::Instant::now(), deadline);
    }
}

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

    fn checkpoint_artifact_inventory(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    ) -> Result<CheckpointArtifactInventory, DbError> {
        if assignment_fence.is_none()
            && self.store.participant_id() != laminar_core::state::LOCAL_NODE_ID.0
        {
            return Err(DbError::Checkpoint(
                "local checkpoint artifacts require the singleton local participant".into(),
            ));
        }
        let inventory = CheckpointArtifactInventory {
            deployment_id: self.expected_deployment_id()?.to_owned(),
            pipeline_identity: self.expected_pipeline_identity()?,
            attempt: require_canonical_attempt(attempt, "checkpoint artifact admission")?,
            assignment_fence,
        };
        inventory.validate().map_err(DbError::Checkpoint)?;
        Ok(inventory)
    }

    fn validate_checkpoint_artifact_inventory(
        &self,
        inventory: &CheckpointArtifactInventory,
    ) -> Result<(), DbError> {
        inventory.validate().map_err(DbError::Checkpoint)?;
        if inventory.deployment_id != self.expected_deployment_id()?
            || inventory.pipeline_identity != self.expected_pipeline_identity()?
        {
            return Err(DbError::Checkpoint(
                "checkpoint artifact inventory does not belong to this pipeline deployment".into(),
            ));
        }
        Ok(())
    }

    async fn authoritative_committed_predecessor_until(
        &self,
        scope: CheckpointScope,
        deadline: tokio::time::Instant,
    ) -> Result<Option<CommittedCheckpointRef>, DbError> {
        #[cfg(feature = "cluster")]
        if scope == CheckpointScope::Cluster {
            let controller = self.cluster_controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint("cluster checkpoint has no cluster controller".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let outcome =
                tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                    .await
                    .map_err(|_| DbError::Checkpoint("cluster predecessor read timed out".into()))?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("cluster predecessor read failed: {error}"))
                    })?;
            return outcome
                .map(|outcome| {
                    outcome.committed_checkpoint.ok_or_else(|| {
                        DbError::Checkpoint(
                            "cluster predecessor Commit has no checkpoint reference".into(),
                        )
                    })
                })
                .transpose();
        }

        if scope != CheckpointScope::Local {
            return Err(DbError::Checkpoint(
                "cluster checkpointing requires the cluster feature".into(),
            ));
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("local predecessor read requires a decision store".into())
        })?;
        let head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("local predecessor read timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("local predecessor read failed: {error}"))
            })?;
        head.and_then(|head| head.latest_commit)
            .map(|outcome| {
                outcome.committed_checkpoint.ok_or_else(|| {
                    DbError::Checkpoint(
                        "local predecessor Commit has no checkpoint reference".into(),
                    )
                })
            })
            .transpose()
    }

    async fn predecessor_source_watermarks_until(
        &self,
        predecessor: Option<&CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<BTreeMap<String, i64>, DbError> {
        let Some(predecessor) = predecessor else {
            return Ok(BTreeMap::new());
        };
        if let Some((cached_reference, source_watermarks)) =
            self.last_committed_source_watermarks.as_ref()
        {
            if cached_reference == predecessor {
                return Ok(source_watermarks.clone());
            }
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "committed predecessor source-watermark read requires a decision store".into(),
            )
        })?;
        let committed =
            tokio::time::timeout_at(deadline, store.load_committed_checkpoint(predecessor))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "committed predecessor source-watermark read timed out".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "committed predecessor source-watermark read failed: {error}"
                    ))
                })?;
        committed
            .effective_source_watermarks()
            .map_err(DbError::Checkpoint)
    }

    pub(crate) async fn begin_checkpoint_artifacts_until(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<&LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let inventory = self.checkpoint_artifact_inventory(attempt, assignment_fence)?;

        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            let proof = leader_proof.ok_or_else(|| {
                DbError::Checkpoint("cluster artifact admission has no leader proof".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            let admitted = tokio::time::timeout_at(
                deadline,
                authority.begin_cluster_checkpoint_artifacts(proof, inventory.clone()),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster artifact admission timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("cluster artifact admission failed: {error}"))
            })?;
            if admitted != inventory {
                return Err(DbError::Checkpoint(
                    "cluster artifact admission returned a different inventory".into(),
                ));
            }
            return Ok(());
        }

        if inventory.assignment_fence.is_some() || leader_proof.is_some() {
            return Err(DbError::Checkpoint(
                "local artifact admission cannot carry cluster authority".into(),
            ));
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("checkpoint artifact admission requires a decision store".into())
        })?;
        let result = tokio::time::timeout_at(
            deadline,
            store.begin_checkpoint_artifact_inventory(inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("checkpoint artifact admission timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("checkpoint artifact admission failed: {error}"))
        })?;
        match result {
            CheckpointArtifactInventoryUpdateResult::Applied
            | CheckpointArtifactInventoryUpdateResult::Unchanged => Ok(()),
            CheckpointArtifactInventoryUpdateResult::Conflict { current } => {
                Err(DbError::Checkpoint(format!(
                    "checkpoint artifact admission conflicts with {current:?}"
                )))
            }
        }
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

    pub(crate) fn register_sink(
        &mut self,
        name: impl Into<String>,
        handle: crate::sink_task::SinkTaskHandle,
    ) {
        self.sinks.push(RegisteredSink {
            name: name.into(),
            handle,
        });
    }

    pub(crate) fn committable_sink_names(&self) -> Result<Vec<String>, DbError> {
        let mut names = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
            .map(|sink| sink.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint(
                "checkpoint-committable sink names must be unique".into(),
            ));
        }
        Ok(names)
    }

    fn sorted_sink_names(&self) -> Result<Vec<String>, DbError> {
        let mut names = self
            .sinks
            .iter()
            .map(|sink| sink.name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint("sink names must be unique".into()));
        }
        Ok(names)
    }

    fn has_checkpoint_committable_sinks(&self) -> bool {
        self.sinks
            .iter()
            .any(|sink| sink.handle.checkpoint_committable())
    }

    pub(crate) fn clear_sinks(&mut self) -> Result<(), DbError> {
        if self.active_sink_witness.is_some()
            || self.allocator.sink_epoch_reservation.lock().is_some()
        {
            return Err(DbError::Checkpoint(
                "cannot clear sinks while a sink epoch remains open".into(),
            ));
        }
        self.sinks.clear();
        Ok(())
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

    async fn create_sink_witness_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>, DbError> {
        let names = self.committable_sink_names()?;
        if names.is_empty() {
            return Ok(None);
        }
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Ok(None);
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("committable sinks require a decision store".into())
        })?;
        tokio::time::timeout_at(
            deadline,
            store.create_sink_open_witness(
                self.expected_pipeline_identity()?,
                self.store.participant_id(),
                attempt,
                names,
            ),
        )
        .await
        .map_err(|_| DbError::Checkpoint("sink-open witness create timed out".into()))?
        .map(Some)
        .map_err(|error| DbError::Checkpoint(format!("sink-open witness create: {error}")))
    }

    async fn clear_sink_witness_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let Some(witness) = self.active_sink_witness.clone() else {
            return Ok(());
        };
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("sink-open witness cleanup requires a decision store".into())
        })?;
        tokio::time::timeout_at(deadline, store.clear_sink_open_witness(&witness))
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness cleanup timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("sink-open witness cleanup failed: {error}"))
            })?;
        self.active_sink_witness = None;
        Ok(())
    }

    async fn begin_sink_epoch_until(
        &mut self,
        deadline: tokio::time::Instant,
        publication: SinkEpochPublication,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        let attempt = self.reserve_sink_epoch_for_runtime_until(deadline).await?;
        let witness = match self.create_sink_witness_until(attempt, deadline).await {
            Ok(witness) => witness,
            Err(error) => {
                self.allocator.clear_sink_epoch(attempt);
                return Err(error);
            }
        };
        self.active_sink_witness = witness;

        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move {
                        (
                            name,
                            handle.begin_epoch_until(attempt.epoch, deadline).await,
                        )
                    }
                }),
        )
        .await;
        let failures = results
            .into_iter()
            .filter_map(|(name, result)| result.err().map(|error| format!("{name}: {error}")))
            .collect::<Vec<_>>();
        if failures.is_empty() {
            let admissions = self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    (
                        sink.name.as_str(),
                        sink.handle.begun_epoch_admission(attempt.epoch),
                    )
                })
                .collect::<Vec<_>>();
            let expected = admissions.first().and_then(|(_, admission)| *admission);
            let invalid = admissions
                .iter()
                .filter_map(|(name, admission)| {
                    (*admission != expected || admission.is_none()).then_some(*name)
                })
                .collect::<Vec<_>>();
            if !invalid.is_empty() {
                for sink in self
                    .sinks
                    .iter()
                    .filter(|sink| sink.handle.checkpoint_committable())
                {
                    sink.handle.fail_epoch_gate();
                }
                self.allocator.mark_sink_epoch_in_doubt(attempt);
                self.failure_requires_recovery = true;
                return Err(DbError::Checkpoint(format!(
                    "sink epoch {} begin acknowledgement did not leave every gate Begun: {}",
                    attempt.epoch,
                    invalid.join(", ")
                )));
            }
            let admission = expected.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "sink epoch {} has no committable gate admission",
                    attempt.epoch
                ))
            })?;
            if let Err(error) = self.allocator.mark_sink_epoch_ready(attempt) {
                for sink in self
                    .sinks
                    .iter()
                    .filter(|sink| sink.handle.checkpoint_committable())
                {
                    sink.handle.fail_epoch_gate();
                }
                self.failure_requires_recovery = true;
                return Err(error);
            }
            if publication == SinkEpochPublication::Immediate {
                let mut publication_error = None;
                for sink in self
                    .sinks
                    .iter()
                    .filter(|sink| sink.handle.checkpoint_committable())
                {
                    if let Err(error) = sink.handle.publish_open_epoch(admission) {
                        publication_error = Some(DbError::Checkpoint(format!(
                            "sink '{}' epoch {} publication failed: {error}",
                            sink.name, attempt.epoch
                        )));
                        break;
                    }
                }
                if let Some(error) = publication_error {
                    for sink in self
                        .sinks
                        .iter()
                        .filter(|sink| sink.handle.checkpoint_committable())
                    {
                        sink.handle.fail_epoch_gate();
                    }
                    self.failure_requires_recovery = true;
                    return Err(error);
                }
            }
            return Ok(());
        }

        for sink in self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
        {
            sink.handle.fail_epoch_gate();
        }

        if let Err(rollback) = self.rollback_sinks_until(attempt.epoch, deadline).await {
            self.allocator.mark_sink_epoch_in_doubt(attempt);
            self.failure_requires_recovery = true;
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} failed to open ({}) and rollback failed ({rollback})",
                attempt.epoch,
                failures.join("; ")
            )));
        }
        self.clear_sink_witness_until(deadline).await?;
        self.allocator.clear_sink_epoch(attempt);
        Err(DbError::Checkpoint(format!(
            "sink epoch {} failed to open: {}",
            attempt.epoch,
            failures.join("; ")
        )))
    }

    pub async fn begin_initial_epoch(&mut self) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if !self.initial_sink_epoch_required()? {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        self.begin_sink_epoch_until(deadline, SinkEpochPublication::Immediate)
            .await
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

    async fn rollback_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move { (name, handle.rollback_epoch_until(epoch, deadline).await) }
                }),
        )
        .await;
        let failures = results
            .into_iter()
            .filter_map(|(name, result)| result.err().map(|error| format!("{name}: {error}")))
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            for sink in self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
            {
                sink.handle.fail_epoch_gate();
            }
            Err(DbError::Checkpoint(format!(
                "sink rollback failed: {}",
                failures.join("; ")
            )))
        }
    }

    async fn seal_sink_epoch_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let results = futures::future::join_all(
            self.sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
                .map(|sink| {
                    let name = sink.name.clone();
                    let handle = sink.handle.clone();
                    async move {
                        (
                            name,
                            handle.seal_epoch_for_protocol_until(epoch, deadline).await,
                        )
                    }
                }),
        )
        .await;
        let mut admission = None;
        let mut failures = Vec::new();
        for (name, result) in results {
            match result {
                Ok(Some(current)) if admission.is_none_or(|expected| expected == current) => {
                    admission = Some(current);
                }
                Ok(Some(current)) => failures.push(format!(
                    "{name}: mismatched sink transition admission {current:?}"
                )),
                Ok(None) => {}
                Err(error) => failures.push(format!("{name}: {error}")),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            for sink in self
                .sinks
                .iter()
                .filter(|sink| sink.handle.checkpoint_committable())
            {
                sink.handle.fail_epoch_gate();
            }
            Err(DbError::Checkpoint(format!(
                "sink epoch {epoch} seal failed: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) async fn reconcile_sink_open_witness(&mut self) -> Result<(), DbError> {
        self.reconcile_sink_open_witness_until(
            tokio::time::Instant::now() + self.config.cleanup_timeout,
        )
        .await
    }

    pub(crate) async fn reconcile_sink_open_witness_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Ok(());
        }
        let Some(store) = self.decision_store.as_ref() else {
            return Ok(());
        };
        let Some(witness) = tokio::time::timeout_at(deadline, store.sink_open_witness())
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open witness read: {error}")))?
        else {
            return Ok(());
        };
        if witness.pipeline_identity != self.expected_pipeline_identity()?
            || witness.deployment_id != self.expected_deployment_id()?
            || witness.participant_id != self.store.participant_id()
            || witness.committable_sinks != self.committable_sink_names()?
        {
            return Err(DbError::Checkpoint(
                "sink-open witness does not match the running checkpoint namespace".into(),
            ));
        }
        let head = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("sink-open outcome read timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open outcome read: {error}")))?;
        match head.and_then(|head| head.latest_terminal) {
            Some(outcome) if outcome.epoch > witness.attempt.epoch => {
                return Err(DbError::Checkpoint(
                    "sink-open witness remained open past a newer terminal outcome".into(),
                ));
            }
            Some(outcome) if outcome.epoch == witness.attempt.epoch => {
                if outcome.checkpoint_id != witness.attempt.checkpoint_id {
                    return Err(DbError::Checkpoint(
                        "sink-open witness conflicts with its terminal outcome".into(),
                    ));
                }
                if !outcome.is_commit() {
                    self.rollback_sinks_until(witness.attempt.epoch, deadline)
                        .await?;
                }
            }
            Some(_) | None => {
                self.rollback_sinks_until(witness.attempt.epoch, deadline)
                    .await?;
            }
        }
        tokio::time::timeout_at(deadline, store.clear_sink_open_witness(&witness))
            .await
            .map_err(|_| DbError::Checkpoint("sink-open witness cleanup timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("sink-open witness cleanup: {error}")))?;
        self.allocator.advance_epoch_to(checked_successor_epoch(
            witness.attempt.epoch,
            "reconciling sink-open ownership",
        )?);
        self.failure_requires_recovery = false;
        Ok(())
    }

    async fn pre_commit_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<BTreeMap<String, Option<Vec<u8>>>, DbError> {
        // Phase one is a group boundary: no connector may enter PreCommit while a peer can still
        // admit an epoch write. Each handle repeats this seal idempotently at command admission.
        self.seal_sink_epoch_until(epoch, deadline).await?;
        let mut pending = self.sinks.iter();
        let mut active = FuturesUnordered::new();
        for sink in pending.by_ref().take(MAX_SINK_PHASE_ONE_CONCURRENCY) {
            active.push(Self::sink_phase_one(sink, epoch, deadline));
        }
        let mut descriptors = BTreeMap::new();
        let mut descriptor_bytes = 0usize;
        let mut first_error = None;
        while let Some(result) = active.next().await {
            match result {
                Ok(Some((name, payload))) if first_error.is_none() => {
                    descriptor_bytes = descriptor_bytes
                        .checked_add(payload.as_ref().map_or(0, Vec::len))
                        .ok_or_else(|| {
                            DbError::Checkpoint("sink descriptor byte count overflow".into())
                        })?;
                    if descriptor_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
                        first_error = Some(DbError::Checkpoint(format!(
                            "sink descriptors exceed {MAX_COORDINATED_COMMIT_BATCH_BYTES} bytes"
                        )));
                    } else {
                        descriptors.insert(name, payload);
                        if let Some(sink) = pending.next() {
                            active.push(Self::sink_phase_one(sink, epoch, deadline));
                        }
                    }
                }
                Ok(None) if first_error.is_none() => {
                    if let Some(sink) = pending.next() {
                        active.push(Self::sink_phase_one(sink, epoch, deadline));
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(descriptors), Err)
    }

    async fn sink_phase_one(
        sink: &RegisteredSink,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<Option<(String, Option<Vec<u8>>)>, DbError> {
        if sink.handle.checkpoint_committable() {
            let payload = sink
                .handle
                .pre_commit_until(epoch, deadline)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!("sink '{}' pre-commit failed: {error}", sink.name))
                })?;
            if payload
                .as_ref()
                .is_some_and(|payload| payload.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES)
            {
                return Err(DbError::Checkpoint(format!(
                    "sink '{}' descriptor exceeds {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} bytes",
                    sink.name
                )));
            }
            Ok(Some((sink.name.clone(), payload)))
        } else {
            sink.handle.flush_until(deadline).await.map_err(|error| {
                DbError::Checkpoint(format!("sink '{}' flush failed: {error}", sink.name))
            })?;
            Ok(None)
        }
    }

    fn validate_request(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        let unsupported_flags = request.flags & !laminar_core::checkpoint::flags::HANDOFF;
        if unsupported_flags != 0 {
            return Err(DbError::Checkpoint(format!(
                "checkpoint request carries unsupported flags {unsupported_flags:#x}"
            )));
        }
        if request.handoff_replay_pending
            && request.flags & laminar_core::checkpoint::flags::HANDOFF == 0
        {
            return Err(DbError::Checkpoint(
                "aligned replay may only qualify an assignment handoff checkpoint".into(),
            ));
        }
        if request.handoff_replay_pending && request.reassignment_portable {
            return Err(DbError::Checkpoint(
                "a checkpoint with aligned replay pending cannot claim vnode reassignment portability"
                    .into(),
            ));
        }
        #[cfg(feature = "cluster")]
        if request.flags & laminar_core::checkpoint::flags::HANDOFF != 0
            && self.cluster_controller.is_none()
        {
            return Err(DbError::Checkpoint(
                "assignment handoff checkpoint requires a cluster runtime".into(),
            ));
        }
        #[cfg(not(feature = "cluster"))]
        if request.flags != 0 {
            return Err(DbError::Checkpoint(
                "assignment handoff checkpoint requires cluster support".into(),
            ));
        }

        #[cfg(feature = "cluster")]
        let vnode_count = u32::from(self.store.key_group_count().get());
        #[cfg(feature = "cluster")]
        match (
            self.cluster_controller.as_ref(),
            request.assignment_fence.as_ref(),
        ) {
            (None, None) if !request.reassignment_portable => {}
            (None, None) => {
                return Err(DbError::Checkpoint(
                    "local checkpoint cannot claim vnode reassignment portability".into(),
                ));
            }
            (None, Some(_)) => {
                return Err(DbError::Checkpoint(
                    "local checkpoint received an assignment fence".into(),
                ));
            }
            (Some(_), None) => {
                return Err(DbError::Checkpoint(
                    "cluster checkpoint is missing its assignment fence".into(),
                ));
            }
            (Some(controller), Some(fence)) => {
                if !request.reassignment_portable {
                    return Err(DbError::Checkpoint(
                        "cluster checkpoint requires a vnode-reassignment-portable capture".into(),
                    ));
                }
                if !fence.is_canonical()
                    || !fence.contains(self.store.participant_id())
                    || fence.vnode_count != vnode_count
                    || fence.assignment_version != self.assignment_version
                    || controller
                        .checkpoint_assignment_fence(fence.assignment_version)
                        .as_ref()
                        != Some(fence)
                {
                    return Err(DbError::Checkpoint(
                        "checkpoint assignment fence is stale or incompatible".into(),
                    ));
                }
            }
        }
        #[cfg(not(feature = "cluster"))]
        if request.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(
                "local checkpoint received an assignment fence".into(),
            ));
        }
        #[cfg(not(feature = "cluster"))]
        if request.reassignment_portable {
            return Err(DbError::Checkpoint(
                "local checkpoint cannot claim vnode reassignment portability".into(),
            ));
        }

        for source in &self.assignment_scoped_sources {
            let checkpoint = request.source_offset_overrides.get(source).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "assignment-scoped source '{source}' has no captured offset"
                ))
            })?;
            let expected = request
                .assignment_fence
                .as_ref()
                .map_or(self.assignment_version, |fence| fence.assignment_version);
            if checkpoint
                .source_assignment_version
                .map(std::num::NonZeroU64::get)
                != Some(expected)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' offset is not bound to assignment {expected}"
                )));
            }
        }
        if let Some((source, version)) = request
            .source_offset_overrides
            .iter()
            .filter(|(source, _)| !self.assignment_scoped_sources.contains(*source))
            .find_map(|(source, checkpoint)| {
                checkpoint
                    .source_assignment_version
                    .map(|version| (source, version))
            })
        {
            return Err(DbError::Checkpoint(format!(
                "non-assignment source '{source}' carries assignment version {version}"
            )));
        }
        Ok(())
    }

    fn prior_chunk_metadata(
        prior: &CheckpointManifest,
        chunk: StateChunkId,
    ) -> Option<(u64, String)> {
        if prior.node_data.chunk == chunk {
            return Some((
                prior.node_data.object_length,
                prior.node_data.sha256.clone(),
            ));
        }
        prior
            .referenced_chunks
            .binary_search_by_key(&chunk, |reference| reference.chunk)
            .ok()
            .map(|index| &prior.referenced_chunks[index])
            .map(|reference| (reference.object_length, reference.sha256.clone()))
    }

    fn managed_vnode_is_required(
        &self,
        operators: &[ManagedVnodeOperator],
        operator_id: &str,
        vnode: u16,
    ) -> bool {
        let Ok(index) =
            operators.binary_search_by(|operator| operator.operator_id.as_str().cmp(operator_id))
        else {
            return false;
        };
        let vnode = u32::from(vnode);
        if self.owned_vnodes.binary_search(&vnode).is_err() {
            return false;
        }
        match operators[index].placement {
            ManagedVnodePlacement::GlobalSingleton => vnode == 0,
            ManagedVnodePlacement::VnodeKeyed => true,
        }
    }

    fn complete_sparse_vnode_captures(
        &self,
        request: &mut CheckpointRequest,
    ) -> Result<(), DbError> {
        request
            .managed_vnode_operators
            .sort_unstable_by(|left, right| left.operator_id.cmp(&right.operator_id));
        if request
            .managed_vnode_operators
            .iter()
            .any(|operator| operator.operator_id.is_empty())
            || request
                .managed_vnode_operators
                .windows(2)
                .any(|pair| pair[0].operator_id == pair[1].operator_id)
        {
            return Err(DbError::Checkpoint(
                "managed vnode operator inventory must have non-empty, unique identifiers".into(),
            ));
        }

        for capture in &request.state_frames {
            if let StateFrameKey::Vnode { operator_id, vnode } = &capture.key {
                if !self.managed_vnode_is_required(
                    &request.managed_vnode_operators,
                    operator_id,
                    *vnode,
                ) {
                    return Err(DbError::Checkpoint(format!(
                        "captured vnode frame {:?} is outside the current managed-state inventory or ownership roster",
                        capture.key
                    )));
                }
            }
        }

        let expected_vnodes =
            request
                .managed_vnode_operators
                .iter()
                .try_fold(0usize, |total, operator| {
                    let count = match operator.placement {
                        ManagedVnodePlacement::GlobalSingleton => {
                            usize::from(self.owned_vnodes.first() == Some(&0))
                        }
                        ManagedVnodePlacement::VnodeKeyed => self.owned_vnodes.len(),
                    };
                    total.checked_add(count).ok_or_else(|| {
                        DbError::Checkpoint("managed vnode frame count overflowed usize".into())
                    })
                })?;
        let current_vnodes = request
            .state_frames
            .iter()
            .filter(|capture| matches!(capture.key, StateFrameKey::Vnode { .. }))
            .count();
        if current_vnodes < expected_vnodes {
            if let Some(prior) = self.last_committed_manifest.as_ref() {
                let current_whole_frames = request.state_frames.len() - current_vnodes;
                let merged_capacity = current_whole_frames
                    .checked_add(expected_vnodes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "managed vnode checkpoint frame count overflowed usize".into(),
                        )
                    })?;
                let mut merged = Vec::new();
                merged.try_reserve_exact(merged_capacity).map_err(|error| {
                    DbError::Checkpoint(format!(
                        "managed vnode checkpoint roster reservation failed: {error}"
                    ))
                })?;

                let current = std::mem::take(&mut request.state_frames);
                let mut current = current.into_iter().peekable();
                let mut inherited = prior
                    .state_frames
                    .iter()
                    .filter(|frame| {
                        let StateFrameKey::Vnode { operator_id, vnode } = &frame.key else {
                            return false;
                        };
                        self.managed_vnode_is_required(
                            &request.managed_vnode_operators,
                            operator_id,
                            *vnode,
                        )
                    })
                    .peekable();

                loop {
                    match (current.peek(), inherited.peek()) {
                        (Some(current_frame), Some(inherited_frame)) => {
                            match current_frame.key.cmp(&inherited_frame.key) {
                                std::cmp::Ordering::Less => {
                                    merged.push(current.next().expect("peeked current frame"));
                                }
                                std::cmp::Ordering::Equal => {
                                    merged.push(current.next().expect("peeked current frame"));
                                    inherited.next();
                                }
                                std::cmp::Ordering::Greater => {
                                    let frame = inherited.next().expect("peeked inherited frame");
                                    merged.push(CapturedStateFrame {
                                        key: frame.key.clone(),
                                        state: None,
                                    });
                                }
                            }
                        }
                        (Some(_), None) => {
                            merged.extend(current);
                            break;
                        }
                        (None, Some(_)) => {
                            merged.extend(inherited.map(|frame| CapturedStateFrame {
                                key: frame.key.clone(),
                                state: None,
                            }));
                            break;
                        }
                        (None, None) => break,
                    }
                }
                request.state_frames = merged;
            }
        }
        self.validate_capture_roster(&request.state_frames)?;

        let actual_vnodes = request
            .state_frames
            .iter()
            .filter(|capture| matches!(capture.key, StateFrameKey::Vnode { .. }))
            .count();
        if actual_vnodes != expected_vnodes {
            return Err(DbError::Checkpoint(format!(
                "managed vnode checkpoint is incomplete: captured {actual_vnodes} logical frames, expected {expected_vnodes}"
            )));
        }
        Ok(())
    }

    fn validate_capture_roster(&self, captures: &[CapturedStateFrame]) -> Result<(), DbError> {
        if captures.windows(2).any(|pair| pair[0].key >= pair[1].key) {
            return Err(DbError::Checkpoint(
                "captured state frames must be strictly ordered and unique".into(),
            ));
        }
        if captures.iter().any(|capture| {
            matches!(capture.key, StateFrameKey::OperatorWhole { .. }) && capture.state.is_none()
        }) {
            return Err(DbError::Checkpoint(
                "whole-operator state must carry its current payload".into(),
            ));
        }
        Ok(())
    }

    fn canonicalize_checkpoint_request(
        &self,
        request: &mut CheckpointRequest,
    ) -> Result<(), DbError> {
        self.validate_request(request)?;
        request
            .state_frames
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        self.validate_capture_roster(&request.state_frames)?;
        self.complete_sparse_vnode_captures(request)?;
        for channel in &mut request.channel_progress {
            channel.participant_id = self.store.participant_id();
        }
        request.channel_progress.sort_unstable_by(|left, right| {
            (
                left.participant_id,
                left.source_name.as_str(),
                left.input_channel.as_slice(),
            )
                .cmp(&(
                    right.participant_id,
                    right.source_name.as_str(),
                    right.input_channel.as_slice(),
                ))
        });
        if request.channel_progress.windows(2).any(|pair| {
            pair[0].participant_id == pair[1].participant_id
                && pair[0].source_name == pair[1].source_name
                && pair[0].input_channel == pair[1].input_channel
        }) {
            return Err(DbError::Checkpoint(
                "channel progress contains duplicate channel identities".into(),
            ));
        }
        Ok(())
    }

    async fn pack_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        mut request: CheckpointRequest,
        sink_payloads: BTreeMap<String, Option<Vec<u8>>>,
        deadline: tokio::time::Instant,
    ) -> Result<PackedCheckpoint, DbError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(
                "checkpoint packing exceeded its end-to-end deadline".into(),
            ));
        }
        self.canonicalize_checkpoint_request(&mut request)?;
        #[cfg(feature = "cluster")]
        let subscription_output = self
            .prepare_subscription_output_until(
                attempt,
                request.assignment_fence.as_ref(),
                request.subscription_output.take(),
                deadline,
            )
            .await?;
        let expected_sinks = self.committable_sink_names()?;
        if !sink_payloads
            .keys()
            .map(String::as_str)
            .eq(expected_sinks.iter().map(String::as_str))
        {
            return Err(DbError::Checkpoint(
                "phase one did not produce exactly one descriptor per committable sink".into(),
            ));
        }

        let current_chunk = StateChunkId {
            participant_id: self.store.participant_id(),
            checkpoint_id: attempt.checkpoint_id,
        };
        let mut node_data = Vec::new();
        let mut object_length = 0;
        let mut frames = Vec::new();
        let mut current_frame_chunks = Vec::new();
        let mut referenced = BTreeMap::<StateChunkId, (u64, String, u32)>::new();

        for CapturedStateFrame { key, state } in request.state_frames {
            if let Some(bytes) = state {
                let length = u64::try_from(bytes.len()).map_err(|_| {
                    DbError::Checkpoint(format!("state frame {key:?} length exceeds u64"))
                })?;
                if length == 0 {
                    return Err(DbError::Checkpoint(format!(
                        "state frame {key:?} has an empty payload"
                    )));
                }
                let range = ByteRange {
                    offset: object_length,
                    length,
                };
                object_length = range.end().ok_or_else(|| {
                    DbError::Checkpoint("checkpoint node-data length overflow".into())
                })?;
                node_data.push(bytes);
                let node_data_index = node_data.len() - 1;
                let frame_index = frames.len();
                frames.push(StateFrame {
                    key,
                    chunk: current_chunk,
                    range,
                    sha256: String::new(),
                });
                current_frame_chunks.push((frame_index, node_data_index));
            } else {
                let prior = self.last_committed_manifest.as_ref().ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "unchanged state frame {key:?} has no committed predecessor"
                    ))
                })?;
                let frame_index = prior
                    .state_frames
                    .binary_search_by(|frame| frame.key.cmp(&key))
                    .ok()
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "unchanged state frame {key:?} is absent from its committed predecessor"
                        ))
                    })?;
                let prior_frame = &prior.state_frames[frame_index];
                let (length, digest) = Self::prior_chunk_metadata(prior, prior_frame.chunk)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "predecessor frame {:?} references untracked object {:?}",
                            prior_frame.key, prior_frame.chunk
                        ))
                    })?;
                let entry =
                    referenced
                        .entry(prior_frame.chunk)
                        .or_insert((length, digest.clone(), 0));
                if entry.0 != length || entry.1 != digest {
                    return Err(DbError::Checkpoint(format!(
                        "conflicting metadata for referenced object {:?}",
                        prior_frame.chunk
                    )));
                }
                entry.2 = entry
                    .2
                    .checked_add(1)
                    .ok_or_else(|| DbError::Checkpoint("referenced frame count overflow".into()))?;
                frames.push(StateFrame {
                    key,
                    chunk: prior_frame.chunk,
                    range: prior_frame.range,
                    sha256: prior_frame.sha256.clone(),
                });
            }
        }

        let mut prepared_sinks = Vec::with_capacity(sink_payloads.len());
        let mut prepared_sink_chunks = Vec::new();
        for (sink_name, payload) in &sink_payloads {
            let (range, digest) = match payload {
                None => (None, checkpoint_descriptor_sha256(None)),
                Some(payload) => {
                    let length = u64::try_from(payload.len()).map_err(|_| {
                        DbError::Checkpoint(format!(
                            "sink '{sink_name}' descriptor length exceeds u64"
                        ))
                    })?;
                    let range = ByteRange {
                        offset: object_length,
                        length,
                    };
                    object_length = range.end().ok_or_else(|| {
                        DbError::Checkpoint("checkpoint node-data length overflow".into())
                    })?;
                    node_data.push(Bytes::copy_from_slice(payload));
                    prepared_sink_chunks.push((prepared_sinks.len(), node_data.len() - 1));
                    (Some(range), String::new())
                }
            };
            prepared_sinks.push(PreparedSinkDescriptor {
                sink_name: sink_name.clone(),
                format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
                payload: range,
                sha256: digest,
            });
        }

        if object_length > self.config.max_node_data_bytes {
            return Err(DbError::Checkpoint(format!(
                "checkpoint node data is {object_length} bytes; limit is {}",
                self.config.max_node_data_bytes
            )));
        }
        let digest_chunks = node_data.clone();
        let digest_task = tokio::task::spawn_blocking(move || {
            let mut object_digest = Sha256::new();
            for bytes in &digest_chunks {
                object_digest.update(bytes);
            }
            let frame_digests = current_frame_chunks
                .into_iter()
                .map(|(frame, chunk)| (frame, checkpoint_sha256(&digest_chunks[chunk])))
                .collect::<Vec<_>>();
            let sink_digests = prepared_sink_chunks
                .into_iter()
                .map(|(sink, chunk)| {
                    (
                        sink,
                        checkpoint_descriptor_sha256(Some(&digest_chunks[chunk])),
                    )
                })
                .collect::<Vec<_>>();
            (
                format!("{:x}", object_digest.finalize()),
                frame_digests,
                sink_digests,
            )
        });
        let (object_sha256, frame_digests, sink_digests) =
            tokio::time::timeout_at(deadline, digest_task)
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "checkpoint digest task exceeded its end-to-end deadline".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("checkpoint digest task failed: {error}"))
                })?;
        for (frame, digest) in frame_digests {
            frames[frame].sha256 = digest;
        }
        for (sink, digest) in sink_digests {
            prepared_sinks[sink].sha256 = digest;
        }

        let mut manifest = CheckpointManifest::new_with_key_group_count(
            attempt.checkpoint_id,
            attempt.epoch,
            self.store.key_group_count(),
        );
        manifest.bind_participant(self.store.participant_id());
        manifest.pipeline_identity = self.expected_pipeline_identity()?;
        self.expected_deployment_id()?
            .clone_into(&mut manifest.deployment_id);
        manifest.assignment_fence = request.assignment_fence;
        manifest.reassignment_portable = request.reassignment_portable;
        manifest.owned_vnodes = self
            .owned_vnodes
            .iter()
            .map(|vnode| {
                u16::try_from(*vnode).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "owned vnode {vnode} exceeds the configured vnode ID space"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        manifest.source_offsets = request.source_offset_overrides;
        manifest.source_names = request.source_names;
        manifest.sink_names = self.sorted_sink_names()?;
        manifest.channel_progress = request.channel_progress;
        manifest.checkpoint_watermark = classify_channel_progress(&manifest.channel_progress)
            .map_err(DbError::Checkpoint)?
            .active_value();
        manifest.node_data.object_length = object_length;
        manifest.node_data.sha256 = object_sha256;
        manifest.state_frames = frames;
        manifest.prepared_sinks = prepared_sinks;
        manifest.referenced_chunks = referenced
            .into_iter()
            .map(|(chunk, (object_length, sha256, count))| {
                Ok(ReferencedStateChunk {
                    chunk,
                    object_length,
                    sha256,
                    ref_count: NonZeroU32::new(count).ok_or_else(|| {
                        DbError::Checkpoint("referenced object has zero frame references".into())
                    })?,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        #[cfg(feature = "cluster")]
        {
            manifest.subscription_output = subscription_output;
        }
        let errors = manifest.validate(self.store.key_group_count());
        if !errors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "checkpoint manifest validation: {}",
                errors
                    .into_iter()
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        Ok(PackedCheckpoint {
            manifest,
            node_data,
        })
    }

    async fn persist_checkpoint_until(
        &mut self,
        packed: &PackedCheckpoint,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes, DbError> {
        self.phase = CheckpointPhase::Persisting;
        if tokio::time::Instant::now() >= deadline {
            self.retain_ambiguous_prepared(packed)?;
            return Err(DbError::Checkpoint(
                "checkpoint persistence timed out".into(),
            ));
        }
        let persisted = tokio::time::timeout_at(
            deadline,
            self.store
                .save_checkpoint(&packed.manifest, &packed.node_data),
        )
        .await;
        let manifest_bytes = match persisted {
            Err(_) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::Checkpoint(
                    "checkpoint persistence timed out".into(),
                ));
            }
            Ok(Err(error)) => {
                self.retain_ambiguous_prepared(packed)?;
                return Err(DbError::from(error));
            }
            Ok(Ok(bytes)) => bytes,
        };
        self.total_bytes_written = self
            .total_bytes_written
            .saturating_add(packed.manifest.node_data.object_length);
        self.prepared.insert(
            CheckpointAttempt::canonical(packed.manifest.checkpoint_id),
            (Arc::new(packed.manifest.clone()), manifest_bytes.clone()),
        );
        Ok(manifest_bytes)
    }

    fn retain_ambiguous_prepared(&mut self, packed: &PackedCheckpoint) -> Result<(), DbError> {
        let manifest_bytes = Bytes::from(checkpoint_manifest_bytes(&packed.manifest).map_err(
            |error| DbError::Checkpoint(format!("encode checkpoint manifest: {error}")),
        )?);
        self.prepared
            .entry(CheckpointAttempt::canonical(packed.manifest.checkpoint_id))
            .or_insert_with(|| (Arc::new(packed.manifest.clone()), manifest_bytes));
        Ok(())
    }

    async fn seal_checkpoint_artifacts_until(
        &mut self,
        inventory: &CheckpointArtifactInventory,
        predecessor: Option<CommittedCheckpointRef>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.validate_checkpoint_artifact_inventory(inventory)?;
        let participant_ids = inventory.assignment_fence.as_ref().map_or_else(
            || vec![laminar_core::state::LOCAL_NODE_ID.0],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        let mut manifest_seals = futures::stream::iter(participant_ids.iter().copied())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let chunk = StateChunkId {
                    participant_id,
                    checkpoint_id: inventory.attempt.checkpoint_id,
                };
                let artifact_identity =
                    checkpoint_artifact_identity_sha256(inventory, chunk).map_err(DbError::from);
                async move {
                    let artifact_identity = artifact_identity?;
                    let manifest = tokio::time::timeout_at(deadline, async {
                        store.seal_aborted_manifest(chunk, &artifact_identity).await
                    })
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} artifact manifest seal timed out"
                        ))
                    })?
                    .map_err(DbError::from)?;
                    Ok::<_, DbError>((participant_id, manifest))
                }
            })
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY);
        let mut loaded = BTreeMap::new();
        while let Some(result) = manifest_seals.next().await {
            let (participant_id, manifest) = result?;
            if let Some((manifest, encoded)) = manifest {
                if manifest.deployment_id != inventory.deployment_id
                    || manifest.pipeline_identity != inventory.pipeline_identity
                    || manifest.epoch != inventory.attempt.epoch
                    || manifest.checkpoint_id != inventory.attempt.checkpoint_id
                    || manifest.participant_id != participant_id
                    || manifest.vnode_count != self.store.key_group_count().get()
                    || manifest.node_data.chunk
                        != (StateChunkId {
                            participant_id,
                            checkpoint_id: inventory.attempt.checkpoint_id,
                        })
                    || manifest.assignment_fence != inventory.assignment_fence
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {participant_id} manifest does not match the active artifact inventory"
                    )));
                }
                loaded.insert(participant_id, (manifest, encoded));
            }
        }

        if loaded.len() == participant_ids.len() {
            let manifests = loaded.into_values().collect::<Vec<_>>();
            let scope = if inventory.assignment_fence.is_some() {
                CheckpointScope::Cluster
            } else {
                CheckpointScope::Local
            };
            let predecessor_source_watermarks = self
                .predecessor_source_watermarks_until(predecessor.as_ref(), deadline)
                .await?;
            let candidate = self.build_committed_index(
                inventory.attempt,
                scope,
                inventory.assignment_fence.clone(),
                predecessor,
                &predecessor_source_watermarks,
                &manifests,
                None,
            )?;
            let decisions = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("artifact sealing requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                decisions.seal_aborted_committed_checkpoint_candidate(&candidate),
            )
            .await
            .map_err(|_| DbError::Checkpoint("candidate checkpoint index seal timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("candidate checkpoint index seal failed: {error}"))
            })?;
        }

        let mut data_seals = futures::stream::iter(participant_ids.iter().copied())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let chunk = StateChunkId {
                    participant_id,
                    checkpoint_id: inventory.attempt.checkpoint_id,
                };
                let artifact_identity =
                    checkpoint_artifact_identity_sha256(inventory, chunk).map_err(DbError::from);
                async move {
                    let artifact_identity = artifact_identity?;
                    tokio::time::timeout_at(deadline, async {
                        store
                            .seal_aborted_node_data(chunk, &artifact_identity)
                            .await
                    })
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "participant {participant_id} node-data seal timed out"
                        ))
                    })?
                    .map_err(DbError::from)
                }
            })
            .buffer_unordered(MAX_RETENTION_IO_CONCURRENCY);
        while let Some(result) = data_seals.next().await {
            result?;
        }
        self.prepared.remove(&inventory.attempt);
        Ok(())
    }

    async fn cleanup_local_checkpoint_artifacts_until(
        &mut self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let store = Arc::clone(self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("local artifact cleanup requires a decision store".into())
        })?);
        let Some(head) = tokio::time::timeout_at(deadline, store.checkpoint_decision_head())
            .await
            .map_err(|_| DbError::Checkpoint("local artifact cleanup read timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("local artifact cleanup read failed: {error}"))
            })?
        else {
            return Ok(());
        };
        let Some(inventory) = head.active_artifacts else {
            return Ok(());
        };
        if inventory.attempt != attempt || inventory.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} does not match the active local artifact inventory",
                attempt.checkpoint_id
            )));
        }
        if !head.latest_terminal.as_ref().is_some_and(|outcome| {
            !outcome.is_commit()
                && outcome.epoch == attempt.epoch
                && outcome.checkpoint_id == attempt.checkpoint_id
        }) {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} artifacts cannot be cleaned without its durable Abort",
                attempt.checkpoint_id
            )));
        }
        let predecessor = head
            .latest_commit
            .and_then(|outcome| outcome.committed_checkpoint);
        self.seal_checkpoint_artifacts_until(&inventory, predecessor, deadline)
            .await?;
        let result = tokio::time::timeout_at(
            deadline,
            store.complete_checkpoint_artifact_cleanup(&inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("local artifact inventory cleanup timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("local artifact inventory cleanup failed: {error}"))
        })?;
        match result {
            CheckpointArtifactInventoryUpdateResult::Applied
            | CheckpointArtifactInventoryUpdateResult::Unchanged => Ok(()),
            CheckpointArtifactInventoryUpdateResult::Conflict { current } => {
                Err(DbError::Checkpoint(format!(
                    "local artifact inventory cleanup conflicted with {current:?}"
                )))
            }
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn settle_cluster_checkpoint_artifacts_until(
        &mut self,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster artifact cleanup has no cluster controller".into())
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
        })?;
        let Some(inventory) =
            tokio::time::timeout_at(deadline, authority.cluster_checkpoint_artifacts())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("cluster artifact inventory read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("cluster artifact inventory read failed: {error}"))
                })?
        else {
            return Ok(false);
        };
        self.validate_checkpoint_artifact_inventory(&inventory)?;
        let assignment_fence = inventory.assignment_fence.clone().ok_or_else(|| {
            DbError::Checkpoint("cluster artifact inventory has no assignment fence".into())
        })?;
        let settlement = tokio::time::timeout_at(
            deadline,
            authority.cluster_attempt_settlement(inventory.attempt),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster artifact settlement read timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("cluster artifact settlement read failed: {error}"))
        })?;
        match settlement {
            None => {
                self.record_outcome_until(
                    inventory.attempt,
                    laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                    None,
                    Some(assignment_fence),
                    Some(proof.clone()),
                    deadline,
                )
                .await?;
            }
            Some(outcome)
                if outcome.epoch == inventory.attempt.epoch
                    && outcome.checkpoint_id == inventory.attempt.checkpoint_id
                    && !outcome.is_commit()
                    && outcome.deployment_id == inventory.deployment_id
                    && outcome.scope == CheckpointScope::Cluster
                    && outcome.assignment_fence.as_ref() == inventory.assignment_fence.as_ref() => {
            }
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint {} has incompatible terminal authority while artifacts remain",
                    inventory.attempt.checkpoint_id
                )));
            }
        }
        let latest_commit =
            tokio::time::timeout_at(deadline, authority.highest_cluster_committed_outcome())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("cluster committed predecessor read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "cluster committed predecessor read failed: {error}"
                    ))
                })?;
        if latest_commit
            .as_ref()
            .is_some_and(|outcome| outcome.epoch >= inventory.attempt.epoch)
        {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} artifact cleanup does not follow the committed head",
                inventory.attempt.checkpoint_id
            )));
        }
        let predecessor = latest_commit.and_then(|outcome| outcome.committed_checkpoint);
        self.seal_checkpoint_artifacts_until(&inventory, predecessor, deadline)
            .await?;
        tokio::time::timeout_at(
            deadline,
            authority.finish_cluster_checkpoint_artifact_cleanup(proof, &inventory),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster artifact inventory cleanup timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "cluster artifact inventory cleanup failed: {error}"
            ))
        })?;
        Ok(true)
    }

    async fn await_prepared_participant_manifests(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        local: (CheckpointManifest, Bytes),
        deadline: tokio::time::Instant,
    ) -> Result<Vec<(CheckpointManifest, Bytes)>, DbError> {
        let participant_ids = assignment_fence.map_or_else(
            || vec![self.store.participant_id()],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        let expected_assignment_fence = assignment_fence.cloned();
        let expected_deployment_id = self.expected_deployment_id()?.to_owned();
        let expected_pipeline_identity = self.expected_pipeline_identity()?;
        if local.0.participant_id != self.store.participant_id()
            || !participant_ids.contains(&local.0.participant_id)
            || local.0.checkpoint_id != attempt.checkpoint_id
            || local.0.epoch != attempt.epoch
            || local.0.assignment_fence != expected_assignment_fence
            || local.0.deployment_id != expected_deployment_id
            || local.0.pipeline_identity != expected_pipeline_identity
        {
            return Err(DbError::Checkpoint(format!(
                "local participant published an invalid manifest readiness marker for checkpoint {} epoch {}",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        let mut loaded = BTreeMap::from([(local.0.participant_id, local)]);
        let mut reads = participant_ids
            .into_iter()
            .filter(|participant_id| *participant_id != self.store.participant_id())
            .map(|participant_id| {
                let store = Arc::clone(&self.store);
                let expected_assignment_fence = expected_assignment_fence.clone();
                let expected_deployment_id = expected_deployment_id.clone();
                let expected_pipeline_identity = expected_pipeline_identity.clone();
                async move {
                    let manifest = await_participant_manifest_until(
                        participant_id,
                        attempt,
                        deadline,
                        || {
                            let store = Arc::clone(&store);
                            async move {
                                store
                                    .load_manifest_for_participant(
                                        participant_id,
                                        attempt.checkpoint_id,
                                    )
                                    .await
                                    .map_err(DbError::from)
                            }
                        },
                    )
                    .await?;
                    if manifest.epoch != attempt.epoch
                        || manifest.assignment_fence != expected_assignment_fence
                        || manifest.deployment_id != expected_deployment_id
                        || manifest.pipeline_identity != expected_pipeline_identity
                    {
                        return Err(DbError::Checkpoint(format!(
                            "participant {participant_id} published an invalid manifest readiness marker for checkpoint {} epoch {}",
                            attempt.checkpoint_id, attempt.epoch
                        )));
                    }
                    let encoded =
                        Bytes::from(checkpoint_manifest_bytes(&manifest).map_err(|error| {
                            DbError::Checkpoint(format!(
                                "encode participant {participant_id} manifest: {error}"
                            ))
                        })?);
                    Ok::<_, DbError>((participant_id, manifest, encoded))
                }
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(result) = reads.next().await {
            let (participant_id, manifest, encoded) = result?;
            loaded.insert(participant_id, (manifest, encoded));
        }
        Ok(loaded.into_values().collect())
    }

    fn merge_source_checkpoint(
        source: &str,
        destination: &mut ConnectorCheckpoint,
        incoming: &ConnectorCheckpoint,
    ) -> Result<(), DbError> {
        match (
            destination.source_assignment_version,
            incoming.source_assignment_version,
        ) {
            (None, None) => {}
            (Some(left), Some(right)) if left == right => {}
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' participant offsets disagree on assignment version"
                )));
            }
        }
        for (key, value) in &incoming.offsets {
            if destination
                .offsets
                .insert(key.clone(), value.clone())
                .is_some_and(|previous| previous != *value)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' has conflicting offset '{key}'"
                )));
            }
        }
        for (key, value) in &incoming.metadata {
            if destination
                .metadata
                .insert(key.clone(), value.clone())
                .is_some_and(|previous| previous != *value)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' has conflicting metadata '{key}'"
                )));
            }
        }
        match (&mut destination.input_channels, &incoming.input_channels) {
            (None, None) => {}
            (Some(destination), Some(incoming)) => {
                if incoming
                    .iter()
                    .any(|channel| destination.binary_search(channel).is_ok())
                {
                    return Err(DbError::Checkpoint(format!(
                        "source '{source}' input channel is owned by multiple participants"
                    )));
                }
                destination.extend(incoming.iter().cloned());
                destination.sort_unstable();
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' participant checkpoints disagree on whether input channels are declared"
                )));
            }
        }
        Ok(())
    }

    fn build_committed_index(
        &self,
        attempt: CheckpointAttempt,
        scope: CheckpointScope,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        predecessor: Option<CommittedCheckpointRef>,
        predecessor_source_watermarks: &BTreeMap<String, i64>,
        manifests: &[(CheckpointManifest, Bytes)],
        quorum_watermark: Option<CheckpointWatermark>,
    ) -> Result<CommittedCheckpointIndex, DbError> {
        let mut participants = Vec::with_capacity(manifests.len());
        let mut source_offsets = BTreeMap::<String, ConnectorCheckpoint>::new();
        let mut channels = BTreeMap::<(u64, String, Vec<u8>), ChannelProgress>::new();
        for (manifest, encoded) in manifests {
            participants.push(
                CommittedParticipantRef::from_manifest(manifest, encoded)
                    .map_err(DbError::Checkpoint)?,
            );
            for (source, checkpoint) in &manifest.source_offsets {
                match source_offsets.entry(source.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(checkpoint.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        Self::merge_source_checkpoint(source, entry.get_mut(), checkpoint)?;
                    }
                }
            }
            for channel in &manifest.channel_progress {
                if channels
                    .insert(
                        (
                            channel.participant_id,
                            channel.source_name.clone(),
                            channel.input_channel.clone(),
                        ),
                        channel.clone(),
                    )
                    .is_some()
                {
                    return Err(DbError::Checkpoint(format!(
                        "participant {} source '{}' input channel appears more than once",
                        channel.participant_id, channel.source_name
                    )));
                }
            }
        }
        participants.sort_unstable_by_key(|participant| participant.participant_id);
        let channel_progress = channels.into_values().collect::<Vec<_>>();
        let classification =
            classify_channel_progress(&channel_progress).map_err(DbError::Checkpoint)?;
        if let Some(quorum_watermark) = quorum_watermark {
            if quorum_watermark != classification {
                return Err(DbError::Checkpoint(format!(
                    "checkpoint quorum watermark {quorum_watermark:?} does not match merged channel progress {classification:?}"
                )));
            }
        }
        let checkpoint_watermark = classification.active_value();
        let source_names = manifests
            .first()
            .map_or_else(Vec::new, |(manifest, _)| manifest.source_names.clone());
        let mut source_watermarks = predecessor_source_watermarks
            .iter()
            .filter(|(source, _)| source_names.binary_search(source).is_ok())
            .map(|(source, watermark)| (source.clone(), *watermark))
            .collect::<BTreeMap<_, _>>();
        for (source, frontier) in
            channel_progress_frontiers_by_source(&channel_progress).map_err(DbError::Checkpoint)?
        {
            let Some(frontier) = frontier else {
                continue;
            };
            if source_watermarks
                .get(source)
                .is_some_and(|predecessor| *predecessor > frontier)
            {
                return Err(DbError::Checkpoint(format!(
                    "source '{source}' decision watermark regressed below its committed predecessor"
                )));
            }
            source_watermarks.insert(source.to_owned(), frontier);
        }
        let reassignment_portable = manifests
            .first()
            .is_some_and(|(manifest, _)| manifest.reassignment_portable);
        let index = CommittedCheckpointIndex {
            version: COMMITTED_CHECKPOINT_INDEX_VERSION,
            deployment_id: self.expected_deployment_id()?.to_owned(),
            pipeline_identity: self.expected_pipeline_identity()?,
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            scope,
            vnode_count: self.store.key_group_count().get(),
            assignment_fence,
            reassignment_portable,
            predecessor,
            participants,
            source_names,
            source_offsets,
            channel_progress,
            source_watermarks,
            checkpoint_watermark,
        };
        let manifest_views = manifests
            .iter()
            .map(|(manifest, encoded)| (manifest, encoded.as_ref()))
            .collect::<Vec<_>>();
        index
            .validate_participant_manifests(&manifest_views)
            .map_err(DbError::Checkpoint)?;
        Ok(index)
    }

    // COMPAT: cluster builds await shared-store continuity validation; keep one caller shape.
    #[cfg_attr(
        not(feature = "cluster"),
        allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)
    )]
    async fn build_validated_committed_index_until(
        &self,
        attempt: CheckpointAttempt,
        scope: CheckpointScope,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        predecessor: Option<CommittedCheckpointRef>,
        predecessor_source_watermarks: &BTreeMap<String, i64>,
        manifests: &[(CheckpointManifest, Bytes)],
        quorum_watermark: Option<CheckpointWatermark>,
        deadline: tokio::time::Instant,
    ) -> Result<CommittedCheckpointIndex, DbError> {
        let index = self.build_committed_index(
            attempt,
            scope,
            assignment_fence.clone(),
            predecessor.clone(),
            predecessor_source_watermarks,
            manifests,
            quorum_watermark,
        )?;
        #[cfg(feature = "cluster")]
        let subscription_validation = self
            .validate_subscription_continuity_until(
                attempt,
                assignment_fence.as_ref(),
                predecessor.as_ref(),
                manifests,
                deadline,
            )
            .await;
        #[cfg(feature = "cluster")]
        if let Err(error) = &subscription_validation {
            self.record_cluster_subscription_error(error);
        }
        #[cfg(feature = "cluster")]
        subscription_validation?;
        #[cfg(not(feature = "cluster"))]
        let _ = deadline;
        Ok(index)
    }

    #[cfg(feature = "cluster")]
    fn validate_captured_quorum(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        participants: Vec<QuorumPeer>,
        proof: &LeaderProof,
    ) -> Result<(), DbError> {
        let mut expected = fence
            .participant_ids()
            .into_iter()
            .filter(|participant| *participant != self.store.participant_id())
            .collect::<Vec<_>>();
        let mut actual = participants
            .into_iter()
            .map(|participant| participant.0)
            .collect::<Vec<_>>();
        expected.sort_unstable();
        actual.sort_unstable();
        if actual != expected || !controller.proof_is_live(proof) {
            return Err(DbError::Checkpoint(
                "checkpoint quorum does not match its assignment or leader proof".into(),
            ));
        }
        Ok(())
    }

    async fn commit_external_sinks_until(
        &self,
        attempt: CheckpointAttempt,
        manifests: &[&CheckpointManifest],
        fencing_token: u64,
        predecessor_checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        if fencing_token == 0 {
            return Err(DbError::Checkpoint(
                "external checkpoint publication requires a nonzero fencing token".into(),
            ));
        }
        let identity = self.expected_pipeline_identity()?;
        let deployment_id = self.expected_deployment_id()?.to_owned();
        let mut pending = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable());
        let mut active = FuturesOrdered::new();
        for sink in pending.by_ref().take(MAX_EXTERNAL_SINK_COMMIT_CONCURRENCY) {
            active.push_back(self.commit_external_sink_until(
                sink,
                attempt,
                manifests,
                fencing_token,
                predecessor_checkpoint_id,
                &identity,
                &deployment_id,
                deadline,
            ));
        }
        let mut first_error = None;
        while let Some(result) = active.next().await {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
            if let Some(sink) = pending.next() {
                active.push_back(self.commit_external_sink_until(
                    sink,
                    attempt,
                    manifests,
                    fencing_token,
                    predecessor_checkpoint_id,
                    &identity,
                    &deployment_id,
                    deadline,
                ));
            }
        }

        // RECOVERY: durable Commit cannot be rolled back. Attempt and drain every sink even after
        // one failure; ordered completion preserves the registration-order first error.
        first_error.map_or(Ok(()), Err)
    }

    #[allow(clippy::too_many_arguments)] // Protocol authority stays explicit at the I/O boundary.
    async fn commit_external_sink_until(
        &self,
        sink: &RegisteredSink,
        attempt: CheckpointAttempt,
        manifests: &[&CheckpointManifest],
        fencing_token: u64,
        predecessor_checkpoint_id: u64,
        identity: &PipelineIdentity,
        deployment_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let namespace = CoordinatedCommitNamespace::try_new(
            identity.clone(),
            deployment_id.to_owned(),
            sink.name.clone(),
        )
        .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        let cursor =
            tokio::time::timeout_at(deadline, sink.handle.committed_cursor(namespace.clone()))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "sink '{}' committed-cursor read timed out",
                        sink.name
                    ))
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "sink '{}' committed-cursor read failed: {error}",
                        sink.name
                    ))
                })?;
        if let Some(cursor) = cursor {
            if cursor.checkpoint_id == attempt.checkpoint_id {
                if cursor.fencing_token != fencing_token {
                    return Err(DbError::Checkpoint(format!(
                        "sink '{}' checkpoint {} was committed under fencing token {}, expected {fencing_token}",
                        sink.name, attempt.checkpoint_id, cursor.fencing_token
                    )));
                }
                return Ok(());
            }
        }
        let expected_predecessor = if predecessor_checkpoint_id == 0 {
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            }
        } else {
            let cursor = cursor.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "sink '{}' has no external cursor for committed predecessor {predecessor_checkpoint_id}",
                    sink.name
                ))
            })?;
            if cursor.checkpoint_id != predecessor_checkpoint_id {
                return Err(DbError::Checkpoint(format!(
                    "sink '{}' external cursor {} trails committed predecessor {predecessor_checkpoint_id}; recovery must publish the missing cut first",
                    sink.name, cursor.checkpoint_id
                )));
            }
            cursor
        };

        let mut entries = Vec::with_capacity(manifests.len());
        for manifest in manifests {
            let descriptor = manifest
                .prepared_sinks
                .binary_search_by(|descriptor| descriptor.sink_name.cmp(&sink.name))
                .ok()
                .map(|index| &manifest.prepared_sinks[index])
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "participant {} has no prepared descriptor for sink '{}'",
                        manifest.participant_id, sink.name
                    ))
                })?;
            let payload = tokio::time::timeout_at(
                deadline,
                self.store
                    .load_prepared_sink_descriptor(manifest, descriptor),
            )
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "sink '{}' descriptor read timed out for participant {}",
                    sink.name, manifest.participant_id
                ))
            })?
            .map_err(DbError::from)?
            .map(|bytes| bytes.to_vec());
            entries.push(CoordinatedCommitPayload {
                attempt,
                participant_id: manifest.participant_id,
                payload,
            });
        }
        entries.sort_unstable_by_key(|entry| entry.participant_id);
        let batch = CoordinatedCommitBatch {
            namespace,
            expected_predecessor,
            fencing_token,
            target: attempt,
            entries,
        };
        batch.validate_shape().map_err(|error| {
            DbError::Checkpoint(format!("sink '{}' commit batch: {error}", sink.name))
        })?;
        tokio::time::timeout_at(deadline, sink.handle.commit_aggregated(batch))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!("sink '{}' external commit timed out", sink.name))
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "sink '{}' external commit failed: {error}",
                    sink.name
                ))
            })
    }

    async fn create_committed_index_until(
        &self,
        index: &CommittedCheckpointIndex,
        deadline: tokio::time::Instant,
    ) -> Result<CommittedCheckpointRef, DbError> {
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("committed checkpoint publication requires a decision store".into())
        })?;
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(
                "committed checkpoint index create timed out".into(),
            ));
        }
        tokio::time::timeout_at(deadline, store.create_committed_checkpoint(index))
            .await
            .map_err(|_| DbError::Checkpoint("committed checkpoint index create timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("committed checkpoint index create failed: {error}"))
            })
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
