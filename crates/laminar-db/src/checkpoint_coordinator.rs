//! Checkpoint coordinator — Ring 2 control-plane orchestrator.
//!
//! Checkpoint manifest is the source of truth; external source progress commits are advisory.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{stream::FuturesUnordered, StreamExt};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::CoordinatedCommitNamespace;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::{
    canonical_json_sha256, ClusterRecoveryCapsule, CommittedSourceHandoff,
    PreparedCheckpointWitness, RecoveryCapsuleRef, MAX_PREPARED_CHECKPOINT_WITNESSES,
};
use laminar_core::checkpoint::{CheckpointWatermark, LeaderProof};
#[cfg(feature = "cluster")]
use laminar_core::state::CheckpointSealInventory;
use laminar_core::state::{CheckpointAttempt, StateBackend, StateBackendError};
use laminar_core::storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, PipelineIdentity,
};
use laminar_core::storage::checkpoint_store::{
    CheckpointStore, CheckpointStoreError, MAX_CHECKPOINT_INVENTORY_ENTRIES,
};
use tracing::{debug, error, info, warn};

#[cfg(all(feature = "cluster", test))]
use crate::cluster_recovery_capsule::PARTICIPANT_READY_PREFIX;
#[cfg(feature = "cluster")]
use crate::cluster_recovery_capsule::{
    assemble_capsule, checked_participant_ready_total, manifest_digests, ParticipantReady,
    MAX_PARTICIPANT_READY_BYTES, MAX_PARTICIPANT_READY_READ_CONCURRENCY, PARTICIPANT_READY_VERSION,
};
#[cfg(feature = "cluster")]
pub(crate) use crate::cluster_recovery_capsule::{
    participant_from_ready_key, participant_ready_key,
};
use crate::error::DbError;

const MAX_SINK_PHASE_ONE_CONCURRENCY: usize = 8;
const MAX_VNODE_PARTIAL_WRITE_CONCURRENCY: usize = 32;

// Actor commands and remote writes may outlive their acknowledgment future, so drain work that
// was already admitted after the first error instead of canceling it.
async fn try_collect_bounded_draining<F, T, E>(futures: Vec<F>, limit: usize) -> Result<Vec<T>, E>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    assert!(limit > 0, "bounded concurrency must be nonzero");
    let mut pending = futures.into_iter();
    let mut active = FuturesUnordered::new();
    for future in pending.by_ref().take(limit) {
        active.push(future);
    }

    let mut values = Vec::new();
    let mut first_error = None;
    while let Some(result) = active.next().await {
        match result {
            Ok(value) if first_error.is_none() => {
                values.push(value);
                if let Some(future) = pending.next() {
                    active.push(future);
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

    match first_error {
        Some(error) => Err(error),
        None => Ok(values),
    }
}

#[cfg_attr(not(feature = "cluster"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum StagedSlice {
    Bytes(bytes::Bytes),
    // Changed-group bytes chained to this vnode's previous partial.
    Delta(bytes::Bytes),
}

pub(crate) type StagedVnodeStates = HashMap<u32, HashMap<String, StagedSlice>>;

/// Records one operator slice from a self-contained root upload for reference comparison.
#[derive(Debug, Clone)]
pub(crate) enum UploadedSlice {
    Bytes(bytes::Bytes),
}

impl UploadedSlice {
    /// Returns true if `staged` proves the slice unchanged since this upload.
    ///
    fn matches(&self, staged: &StagedSlice) -> bool {
        match staged {
            StagedSlice::Bytes(bytes) => match self {
                UploadedSlice::Bytes(previous) => previous == bytes,
            },
            // A delta never matches a prior full — it rides the delta-chain path, not the reference path.
            StagedSlice::Delta(_) => false,
        }
    }
}

enum VnodeUploadUpdate {
    Retain,
    Replace(HashMap<String, UploadedSlice>),
    Remove,
}

struct PendingDecisionWrite {
    epoch: u64,
    checkpoint_id: u64,
    handle: tokio::task::JoinHandle<
        Result<laminar_core::checkpoint_decision::RecordOutcomeResult, String>,
    >,
}

enum PendingDecisionWait {
    Completed {
        epoch: u64,
        checkpoint_id: u64,
        outcome: Result<
            Result<Box<laminar_core::checkpoint_decision::RecordOutcomeResult>, String>,
            tokio::task::JoinError,
        >,
    },
    TimedOut {
        epoch: u64,
        checkpoint_id: u64,
    },
}

struct PendingSinkWitnessCreate {
    attempt: CheckpointAttempt,
    handle: tokio::task::JoinHandle<
        Result<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness, String>,
    >,
}

enum PendingSinkWitnessClearState {
    Running(tokio::task::JoinHandle<Result<(), String>>),
    NeedsRetry,
    Verified,
}

struct PendingSinkWitnessClear {
    witness: laminar_core::checkpoint_decision::CheckpointSinkOpenWitness,
    state: PendingSinkWitnessClearState,
}

struct PreparedVnodePartial {
    vnode: u32,
    payload: bytes::Bytes,
    upload_update: VnodeUploadUpdate,
    is_reference: bool,
    delta_depth: u32,
}

#[cfg(feature = "cluster")]
struct RestorableGateWatches {
    assignment: Option<
        tokio::sync::watch::Receiver<
            Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        >,
    >,
    membership:
        Option<tokio::sync::watch::Receiver<Vec<laminar_core::cluster::discovery::NodeInfo>>>,
}

/// Checkpoint configuration.
const STATE_INLINE_THRESHOLD: usize = 1_048_576;
const RESTORABLE_GATE_POLL_INITIAL: Duration = Duration::from_millis(5);
const RESTORABLE_GATE_POLL_MAX: Duration = Duration::from_millis(100);
const COORDINATED_COMMITTER_POLL: Duration = Duration::from_secs(1);
#[cfg(feature = "cluster")]
const FOLLOWER_DECISION_POLL: Duration = Duration::from_millis(250);

/// A follower's relationship to the immutable outcome for one exact certified attempt.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowerOutcomeMatch {
    Pending,
    Commit { frontier: Option<i64> },
    Abort,
}

#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Number of predecessor checkpoints retained alongside the current recovery cut.
    /// Predecessors keep reference/delta partial chains resolvable.
    pub max_retained: usize,
    /// Hard end-to-end deadline for one durable checkpoint attempt. Phase-specific connector
    /// and storage limits are clamped by this single semantic deadline.
    pub checkpoint_timeout: Duration,
    /// Internal recovery/rollback budget. This is deliberately not an end-user checkpoint
    /// tuning dimension: cleanup happens only after the attempt deadline or another failure.
    pub(crate) cleanup_timeout: Duration,
    /// Private cluster-control health limit; the absolute checkpoint deadline remains authoritative.
    pub(crate) quorum_timeout: Duration,
    /// Hard byte budget shared by capture, durable storage, validation, and recovery.
    pub max_staged_bytes: u64,
    /// Runtime-owned safety cap on sealed-but-not-externally-committed epochs rather than
    /// another public checkpoint tuning dimension.
    pub(crate) max_uncommitted_epochs: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            max_retained: 3,
            checkpoint_timeout: Duration::from_secs(120),
            cleanup_timeout: Duration::from_secs(30),
            quorum_timeout: Duration::from_secs(3),
            max_staged_bytes:
                laminar_core::storage::checkpoint_store::DEFAULT_MAX_CHECKPOINT_STATE_BYTES,
            max_uncommitted_epochs: 16,
        }
    }
}

/// Lock-free view used by checkpoint admission, source backpressure, and graceful shutdown. It
/// exposes only the semantic pending-count bound; cursor polling/backoff stays private to the
/// designated committer.
#[derive(Clone)]
pub(crate) struct CoordinatedCommitAdmission {
    pending: Arc<std::sync::atomic::AtomicU64>,
    known: Arc<std::sync::atomic::AtomicBool>,
    progress: Arc<tokio::sync::Notify>,
    wake_committer: Arc<tokio::sync::Notify>,
    cap: u64,
}

impl CoordinatedCommitAdmission {
    #[cfg(test)]
    pub(crate) fn for_test(
        pending: Arc<std::sync::atomic::AtomicU64>,
        known: Arc<std::sync::atomic::AtomicBool>,
        cap: u64,
    ) -> Self {
        Self {
            pending,
            known,
            progress: Arc::new(tokio::sync::Notify::new()),
            wake_committer: Arc::new(tokio::sync::Notify::new()),
            cap,
        }
    }

    #[must_use]
    pub(crate) fn can_admit(&self) -> bool {
        self.known.load(std::sync::atomic::Ordering::Acquire)
            && self.pending.load(std::sync::atomic::Ordering::Acquire) < self.cap
    }

    #[must_use]
    pub(crate) fn state(&self) -> (bool, u64, u64) {
        (
            self.known.load(std::sync::atomic::Ordering::Acquire),
            self.pending.load(std::sync::atomic::Ordering::Acquire),
            self.cap,
        )
    }

    pub(crate) fn progress_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.progress)
    }

    /// Request an immediate designated-committer pass. Used by graceful shutdown
    /// after every already-captured checkpoint tail has settled.
    pub(crate) fn wake_committer(&self) {
        self.wake_committer.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn committer_wakeup_for_test(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.wake_committer)
    }
}

/// Parameters for a checkpoint operation.
#[derive(Debug, Clone, Default)]
pub struct CheckpointRequest {
    /// Exact clustered assignment cut captured at admission. `None` is valid only when no cluster
    /// controller is installed (embedded and local single-node runtimes).
    pub assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    /// Serialized operator states. `Bytes` avoids a copy at each pipeline stage.
    pub operator_states: HashMap<String, bytes::Bytes>,
    /// Current watermark timestamp.
    pub watermark: Option<i64>,
    /// Path for table store checkpoint data.
    pub table_store_checkpoint_path: Option<String>,
    /// Additional table offset overrides.
    pub extra_table_offsets: HashMap<String, ConnectorCheckpoint>,
    /// Per-source watermark timestamps.
    pub source_watermarks: HashMap<String, i64>,
    /// Source offset overrides for recovery.
    pub source_offset_overrides: HashMap<String, ConnectorCheckpoint>,
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
struct SinkEpochOpenFailure {
    error: DbError,
    rollback_complete: bool,
}

impl SinkEpochOpenFailure {
    #[cfg(test)]
    const fn requires_pipeline_recovery(&self) -> bool {
        !self.rollback_complete
    }
}

impl std::fmt::Display for SinkEpochOpenFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

/// Serializes allocation from the single durable checkpoint order.
///
/// Runtime-generated attempts use the durable checkpoint ID as their epoch. A checkpoint-
/// committable sink reserves that identity before opening its transaction and checkpoint
/// admission consumes the exact same reservation.
#[derive(Debug)]
pub(crate) struct EpochAllocator {
    next_id_floor: std::sync::atomic::AtomicU64,
    /// Highest floor observed from recovery or another cluster participant. Kept separate from
    /// the allocator's own successor so an exact `attempt + 1` observation fences a pre-opened
    /// sink epoch, including when it races between durable allocation and in-memory publication.
    observed_id_floor: std::sync::atomic::AtomicU64,
    allocation_lock: tokio::sync::Mutex<()>,
    sink_epoch_reservation: parking_lot::Mutex<Option<SinkEpochReservation>>,
    allocation_timeout: Duration,
    decision_store:
        std::sync::OnceLock<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
}

fn checked_successor_epoch(epoch: u64, context: &str) -> Result<u64, DbError> {
    epoch.checked_add(1).ok_or_else(|| {
        DbError::Checkpoint(format!(
            "[LDB-6050] checkpoint epoch space exhausted at {epoch} while {context}"
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
            "[LDB-6050] {context} requires one nonzero canonical checkpoint ID; received epoch {} and checkpoint ID {}",
            attempt.epoch, attempt.checkpoint_id
        )))
    }
}

impl EpochAllocator {
    fn new(next_id_floor: u64, allocation_timeout: Duration) -> Self {
        Self {
            next_id_floor: std::sync::atomic::AtomicU64::new(next_id_floor),
            observed_id_floor: std::sync::atomic::AtomicU64::new(0),
            allocation_lock: tokio::sync::Mutex::new(()),
            sink_epoch_reservation: parking_lot::Mutex::new(None),
            allocation_timeout,
            decision_store: std::sync::OnceLock::new(),
        }
    }

    /// Bind the durable ID reservation store. Rebinding the same handle is idempotent.
    fn bind_decision_store(
        &self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        if let Some(bound) = self.decision_store.get() {
            return if Arc::ptr_eq(bound, &store) {
                Ok(())
            } else {
                Err(DbError::Checkpoint(
                    "[LDB-6050] checkpoint allocator decision store is already bound".into(),
                ))
            };
        }

        match self.decision_store.set(store) {
            Ok(()) => Ok(()),
            Err(store) => {
                // Another binder may have won between `get` and `set`.
                if self
                    .decision_store
                    .get()
                    .is_some_and(|bound| Arc::ptr_eq(bound, &store))
                {
                    Ok(())
                } else {
                    Err(DbError::Checkpoint(
                        "[LDB-6050] checkpoint allocator decision store is already bound".into(),
                    ))
                }
            }
        }
    }

    /// Durably reserve the next canonical checkpoint attempt.
    #[cfg(test)]
    pub(crate) async fn allocate(&self) -> Result<CheckpointAttempt, DbError> {
        self.allocate_until(tokio::time::Instant::now() + self.allocation_timeout)
            .await
    }

    /// Durably reserve an attempt without refreshing the caller's admission deadline between
    /// waiting for the allocator lock and writing the durable ID reservation.
    pub(crate) async fn allocate_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let timeout = self.allocation_timeout;
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] checkpoint ID allocator lock exhausted its {timeout:?} admission deadline"
                ))
            })?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            let attempt = reservation.attempt();
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] pre-opened sink epoch {} must be consumed before allocating another checkpoint attempt",
                attempt.epoch
            )));
        }
        self.allocate_fresh_until(deadline).await
    }

    async fn allocate_fresh_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        use std::sync::atomic::Ordering;

        let timeout = self.allocation_timeout;
        let store = self.decision_store.get().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] checkpoint ID allocation requires a durable decision store".into(),
            )
        })?;
        loop {
            let minimum = self.next_id_floor.load(Ordering::Acquire).max(1);
            let checkpoint_id = tokio::time::timeout_at(
                deadline,
                store.allocate_checkpoint_id_at_least(minimum),
            )
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] durable checkpoint ID reservation exhausted its {timeout:?} admission deadline"
                ))
            })?
            .map_err(|e| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] durable checkpoint ID reservation failed: {e}"
                ))
            })?;
            let successor = checked_successor_epoch(
                checkpoint_id,
                "advancing the durable checkpoint ID allocator",
            )?;

            // Recovery and follower observation may raise the floor without taking the async
            // allocator lock. Accept this reservation only if its CAS linearizes before that
            // advance; otherwise burn it and retry at the observed floor.
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

    /// Reserve the attempt a checkpoint-committable sink is about to open.
    async fn reserve_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let timeout = self.allocation_timeout;
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] checkpoint ID allocator lock exhausted its {timeout:?} sink-epoch deadline"
                ))
            })?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            let attempt = reservation.attempt();
            let state = match reservation {
                SinkEpochReservation::Opening(_) => "opening",
                SinkEpochReservation::Ready(_) => "ready",
                SinkEpochReservation::InDoubt(_) => "in-doubt and requires recovery",
            };
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] sink epoch {} is already reserved ({state})",
                attempt.epoch
            )));
        }
        loop {
            let attempt = self.allocate_fresh_until(deadline).await?;
            *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::Opening(attempt));
            if !self.sink_reservation_is_stale(attempt) {
                return Ok(attempt);
            }
            self.burn_sink_epoch_reservation(attempt)?;
            tokio::task::yield_now().await;
        }
    }

    fn mark_sink_epoch_ready(&self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Opening(opening)) if opening == attempt => {
                *reservation = Some(SinkEpochReservation::Ready(attempt));
                Ok(())
            }
            Some(current) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] sink epoch reservation mismatch: expected {attempt:?}, found {current:?}"
            ))),
            None => Err(DbError::Checkpoint(format!(
                "[LDB-6050] sink epoch {attempt:?} lost its durable reservation before activation"
            ))),
        }
    }

    fn mark_sink_epoch_in_doubt(&self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Opening(opening)) if opening == attempt => {
                *reservation = Some(SinkEpochReservation::InDoubt(attempt));
                Ok(())
            }
            Some(SinkEpochReservation::InDoubt(current)) if current == attempt => Ok(()),
            Some(current) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] cannot poison sink epoch {attempt:?}; current reservation is {current:?}"
            ))),
            None => Err(DbError::Checkpoint(format!(
                "[LDB-6050] cannot poison sink epoch {attempt:?}; its reservation is missing"
            ))),
        }
    }

    fn burn_sink_epoch_reservation(&self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(current) if current.attempt() == attempt => {
                reservation.take();
                Ok(())
            }
            Some(current) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] cannot burn sink epoch {attempt:?}; current reservation is {current:?}"
            ))),
            None => Ok(()),
        }
    }

    pub(crate) async fn consume_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let timeout = self.allocation_timeout;
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] checkpoint ID allocator lock exhausted its {timeout:?} admission deadline"
                ))
            })?;
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Ready(attempt))
                if !self.sink_reservation_is_stale(attempt) =>
            {
                reservation.take();
                Ok(attempt)
            }
            Some(SinkEpochReservation::Ready(attempt)) => {
                *reservation = Some(SinkEpochReservation::InDoubt(attempt));
                Err(DbError::Checkpoint(format!(
                    "[LDB-6050] sink epoch {} fell below an advanced checkpoint floor and requires recovery",
                    attempt.epoch
                )))
            }
            Some(SinkEpochReservation::Opening(attempt)) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] sink epoch {} is not ready for checkpoint admission",
                attempt.epoch
            ))),
            Some(SinkEpochReservation::InDoubt(attempt)) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] sink epoch {} is in-doubt and requires recovery before checkpoint admission",
                attempt.epoch
            ))),
            None => Err(DbError::Checkpoint(
                "[LDB-6050] checkpoint-committable sinks have no pre-opened durable attempt".into(),
            )),
        }
    }

    fn sink_epoch_reservation(&self) -> Option<SinkEpochReservation> {
        *self.sink_epoch_reservation.lock()
    }

    fn sink_reservation_is_stale(&self, attempt: CheckpointAttempt) -> bool {
        use std::sync::atomic::Ordering;

        self.observed_id_floor.load(Ordering::Acquire) > attempt.checkpoint_id
    }

    /// Active pre-opened sink epoch, or the floor the next successful allocation will claim.
    pub(crate) fn peek_epoch(&self) -> u64 {
        use std::sync::atomic::Ordering;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            reservation.attempt().epoch
        } else {
            self.next_id_floor.load(Ordering::Acquire)
        }
    }

    /// Monotonically advance the local epoch after recovery or observing a cluster attempt.
    pub(crate) fn advance_epoch_to(&self, epoch: u64) {
        use std::sync::atomic::Ordering;
        // Publish the allocation fence first. An allocator that races this update must either
        // observe the new floor or lose its CAS before the observation watermark becomes visible.
        self.next_id_floor.fetch_max(epoch, Ordering::AcqRel);
        self.observed_id_floor.fetch_max(epoch, Ordering::AcqRel);
        let mut reservation = self.sink_epoch_reservation.lock();
        if let Some(current) = *reservation {
            if matches!(
                current,
                SinkEpochReservation::Opening(_) | SinkEpochReservation::Ready(_)
            ) && self.sink_reservation_is_stale(current.attempt())
            {
                *reservation = Some(SinkEpochReservation::InDoubt(current.attempt()));
            }
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn advance_past(&self, epoch: u64, context: &str) -> Result<(), DbError> {
        self.advance_epoch_to(checked_successor_epoch(epoch, context)?);
        Ok(())
    }
}

/// Capture-quorum participant id. Aliased so non-cluster builds still type-check.
#[cfg(feature = "cluster")]
pub(crate) type QuorumPeer = laminar_core::cluster::discovery::NodeId;
#[cfg(not(feature = "cluster"))]
pub(crate) type QuorumPeer = u64;

/// Whether the capture quorum still needs to run, or a pipelined tail already ran it.
#[derive(Debug, Clone)]
pub(crate) enum QuorumStage {
    /// Run the quorum + `Aligned` announce inline (forced/timer paths).
    RunInline,
    /// Quorum already reached before the coordinator lock.
    #[cfg_attr(not(feature = "cluster"), allow(dead_code))]
    Done {
        /// Exact cluster watermark status folded from the capture acks.
        cluster_watermark: CheckpointWatermark,
        /// Followers that acked the capture quorum.
        participants: Vec<QuorumPeer>,
        /// Exact durable leader term that ran the reversible capture protocol.
        #[cfg(feature = "cluster")]
        leader_proof: LeaderProof,
    },
}

/// Immutable inputs for one clustered capture-quorum attempt.
#[cfg(feature = "cluster")]
pub(crate) struct PrepareQuorum<'a> {
    attempt: CheckpointAttempt,
    local_watermark: CheckpointWatermark,
    assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_proof: &'a LeaderProof,
    announce_prepare: bool,
}

#[cfg(feature = "cluster")]
impl<'a> PrepareQuorum<'a> {
    pub(crate) const fn new(
        attempt: CheckpointAttempt,
        local_watermark: CheckpointWatermark,
        assignment_fence: &'a laminar_core::checkpoint::CheckpointAssignmentFence,
        leader_proof: &'a LeaderProof,
        announce_prepare: bool,
    ) -> Self {
        Self {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            announce_prepare,
        }
    }
}

/// One validated durable source cut selected for an assignment acquisition.
#[cfg(feature = "cluster")]
#[derive(Debug)]
pub(crate) struct AcquiredClusterHandoff {
    pub(crate) outcome: laminar_core::checkpoint_decision::CheckpointOutcome,
    pub(crate) sources: Arc<CommittedSourceHandoff>,
}

/// Immutable handles needed to read one cluster recovery cut without holding the checkpoint
/// coordinator mutex across decision-store or object-store I/O.
#[cfg(feature = "cluster")]
#[derive(Clone)]
pub(crate) struct ClusterHandoffReader {
    backend: Arc<dyn StateBackend>,
    authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    decision_store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    pipeline_identity: PipelineIdentity,
    deployment_id: Option<String>,
}

#[cfg(feature = "cluster")]
impl ClusterHandoffReader {
    pub(crate) async fn highest_commit_outcome(
        &self,
    ) -> Result<Option<laminar_core::checkpoint_decision::CheckpointOutcome>, DbError> {
        self.authority
            .highest_cluster_committed_outcome()
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "checkpoint decision inventory read failed: {error}"
                ))
            })
    }

    pub(crate) async fn acquired_source_handoff(
        &self,
    ) -> Result<Option<AcquiredClusterHandoff>, DbError> {
        let Some(outcome) = self.highest_commit_outcome().await? else {
            return Ok(None);
        };
        let capsule_reference = outcome.recovery_capsule.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6041] cluster Commit for epoch {} checkpoint {} has no recovery capsule",
                outcome.epoch, outcome.checkpoint_id
            ))
        })?;
        let capsule = self
            .decision_store
            .load_recovery_capsule(capsule_reference)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] recovery capsule read failed for checkpoint {} epoch {}: {error}",
                    outcome.checkpoint_id, outcome.epoch
                ))
            })?;
        let attempt = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
        let inventory = self
            .backend
            .checkpoint_seal_inventory(attempt)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "source-offset handoff seal read failed for checkpoint {} epoch {}: {error}",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] decided checkpoint {} epoch {} has no exact state seal",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?;
        let deployment_id = self.deployment_id.as_deref().ok_or_else(|| {
            DbError::Checkpoint(
                "coordinated commit requires a durable deployment identity before startup".into(),
            )
        })?;
        CheckpointCoordinator::validate_cluster_recovery_capsule(
            &outcome,
            &inventory,
            &capsule,
            deployment_id,
            &self.pipeline_identity,
        )?;
        let sources = Arc::new(CommittedSourceHandoff::try_from(&capsule).map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] recovery capsule source handoff is invalid: {error}"
            ))
        })?);
        info!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            sources = sources.source_count(),
            "decision-bound source handoff staged for acquire"
        );
        Ok(Some(AcquiredClusterHandoff { outcome, sources }))
    }

    pub(crate) fn same_namespace(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.backend, &other.backend)
            && Arc::ptr_eq(&self.authority, &other.authority)
            && Arc::ptr_eq(&self.decision_store, &other.decision_store)
            && self.pipeline_identity == other.pipeline_identity
            && self.deployment_id == other.deployment_id
    }
}

/// Phase of the checkpoint lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum CheckpointPhase {
    /// No checkpoint in progress.
    Idle,
    /// Collecting operator and source snapshots.
    Snapshotting,
    /// Sinks running phase-1 pre-commit.
    PreCommitting,
    /// Writing the manifest.
    Persisting,
    /// Resolving and writing per-vnode state.
    PersistingVnodes,
    /// Waiting for the exact state inventory to seal.
    Sealing,
    /// Publishing the exact durable decision. Once entered, rollback is unsafe because a timed
    /// out write may already be visible to recovery.
    Deciding,
}

impl std::fmt::Display for CheckpointPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Snapshotting => write!(f, "Snapshotting"),
            Self::PreCommitting => write!(f, "PreCommitting"),
            Self::Persisting => write!(f, "Persisting"),
            Self::PersistingVnodes => write!(f, "PersistingVnodes"),
            Self::Sealing => write!(f, "Sealing"),
            Self::Deciding => write!(f, "Deciding"),
        }
    }
}

/// Debug-only handshake used by the real-process soak to stop a selected role inside an active
/// checkpoint before sending `SIGKILL`. The arm file contains `leader` or `follower`; the runtime
/// publishes a sibling `.ready` file and holds the checkpoint until the harness kills the process
/// or removes the arm. Release builds contain no hook.
#[cfg(all(debug_assertions, feature = "cluster"))]
async fn checkpoint_kill_gate(role: &'static str) {
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

    let ready_file = gate_file.with_extension("ready");
    if std::fs::write(&ready_file, role).is_err() {
        return;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while gate_file.is_file() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = std::fs::remove_file(ready_file);
}

/// Required runtime response when a checkpoint attempt fails.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointFailureDisposition {
    /// The attempt is known not to have reached its durable decision and may be retried.
    Retryable,
    /// A durable decision write was issued and its outcome is not safely known. The pipeline must
    /// stop and reconcile recovery state before accepting more input, regardless of its requested
    /// delivery guarantee.
    RequiresRecovery,
}

/// Result of a checkpoint attempt.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointResult {
    /// Whether this checkpoint reached its durable commit point and its source cut may be
    /// acknowledged.
    ///
    /// A successful checkpoint can still carry [`Self::error`] when opening the successor sink
    /// epoch failed. In that case the committed cut remains valid, but the pipeline must stop
    /// before accepting more writes.
    pub success: bool,
    /// Checkpoint ID assigned to this attempt.
    pub checkpoint_id: u64,
    /// Epoch number.
    pub epoch: u64,
    /// Wall time for the full checkpoint cycle.
    pub duration: Duration,
    /// Failure reason, or a terminal continuation error after a durable commit.
    ///
    /// `success == false` means the source cut must not be acknowledged. `success == true` with
    /// an error means the source cut must be acknowledged and the pipeline must then fault before
    /// any subsequent write.
    pub error: Option<String>,
    /// Runtime action required for a failed attempt. Successful attempts always use `None`.
    pub failure_disposition: Option<CheckpointFailureDisposition>,
}

impl CheckpointResult {
    /// Return the terminal error that prevents this pipeline from continuing after a durable
    /// checkpoint commit.
    #[must_use]
    pub fn continuation_error(&self) -> Option<&str> {
        if self.success {
            self.error.as_deref()
        } else {
            None
        }
    }

    /// Whether processing must stop until the in-doubt durable decision is reconciled.
    #[must_use]
    pub fn requires_recovery(&self) -> bool {
        !self.success
            && self.failure_disposition == Some(CheckpointFailureDisposition::RequiresRecovery)
    }
}

/// Registered sink for checkpoint coordination.
pub(crate) struct RegisteredSink {
    pub name: String,
    pub handle: crate::sink_task::SinkTaskHandle,
}

#[derive(Clone)]
struct RetentionRequest {
    horizon: u64,
    trigger_epoch: u64,
    #[cfg(feature = "cluster")]
    preflight_state_backend: Option<Arc<dyn StateBackend>>,
    state_backend: Option<Arc<dyn StateBackend>>,
    state_ancestry_slack: u64,
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    #[cfg(feature = "cluster")]
    checkpoint_authority: Option<Arc<laminar_core::cluster::control::LeaderLeaseStore>>,
    #[cfg(feature = "cluster")]
    leader_proof: Option<LeaderProof>,
    advance_decision_floor: bool,
}

const fn state_artifact_horizon(manifest_horizon: u64, ancestry_slack: u64) -> u64 {
    manifest_horizon.saturating_sub(ancestry_slack)
}

#[cfg(feature = "cluster")]
pub(crate) fn bounded_state_ancestry_slack(
    max_retained: usize,
    delta_chain_bound: Option<u32>,
) -> u64 {
    let reference_slack = u64::try_from(max_retained.saturating_sub(1)).unwrap_or(u64::MAX);
    reference_slack.saturating_add(delta_chain_bound.map_or(0, u64::from))
}

fn accept_retention_floor(
    floor: Result<Result<u64, String>, tokio::time::error::Elapsed>,
    requested: u64,
    trigger_epoch: u64,
    advance_decision_floor: bool,
    operation_timeout: Duration,
) -> Option<u64> {
    match floor {
        Ok(Ok(effective)) if effective >= requested => Some(requested),
        Ok(Ok(effective)) if !advance_decision_floor && effective > 0 => {
            warn!(
                trigger_epoch,
                horizon = requested,
                effective,
                "[LDB-6026] follower decision GC floor trails the requested horizon; pruning only the proven-safe prefix"
            );
            Some(effective)
        }
        Ok(Ok(effective)) => {
            warn!(
                trigger_epoch,
                horizon = requested,
                effective,
                "[LDB-6026] decision GC floor did not reach the requested horizon; skipping artifact prune"
            );
            None
        }
        Ok(Err(error)) => {
            warn!(
                trigger_epoch,
                horizon = requested,
                %error,
                "[LDB-6026] decision prune failed; skipping artifact prune"
            );
            None
        }
        Err(_) => {
            warn!(
                trigger_epoch,
                horizon = requested,
                ?operation_timeout,
                "[LDB-6026] decision prune timed out; skipping artifact prune"
            );
            None
        }
    }
}

#[cfg(feature = "cluster")]
async fn preflight_cluster_retention_cut(
    store: &dyn CheckpointStore,
    state_backend: &dyn StateBackend,
    decision_store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
) -> Result<(), DbError> {
    let reference = outcome.recovery_capsule.as_ref().ok_or_else(|| {
        DbError::Checkpoint(format!(
            "[LDB-6041] cluster Commit epoch {} checkpoint {} has no recovery capsule",
            outcome.epoch, outcome.checkpoint_id
        ))
    })?;
    let capsule = decision_store
        .load_recovery_capsule(reference)
        .await
        .map_err(|error| DbError::Checkpoint(format!("[LDB-6041] {error}")))?;
    crate::recovery_manager::RecoveryManager::new(store)
        .with_pipeline_identity(&capsule.pipeline_identity)
        .with_deployment_id(&capsule.deployment_id)
        .with_outcome_scope(laminar_core::checkpoint_decision::CheckpointScope::Cluster)
        .preflight_cluster_committed_metadata(outcome, &capsule)
        .await?;
    CheckpointCoordinator::validate_cluster_cut_metadata(
        state_backend,
        outcome,
        &capsule,
        &capsule.deployment_id,
        &capsule.pipeline_identity,
    )
    .await
}

async fn authorize_retention_horizon(
    requested: u64,
    trigger_epoch: u64,
    store: Arc<dyn CheckpointStore>,
    state_backend: Option<Arc<dyn StateBackend>>,
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    #[cfg(feature = "cluster")] checkpoint_authority: Option<
        Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    >,
    #[cfg(feature = "cluster")] leader_proof: Option<LeaderProof>,
    advance_decision_floor: bool,
    operation_timeout: Duration,
) -> Option<u64> {
    #[cfg(not(feature = "cluster"))]
    let _ = (&store, &state_backend);
    #[cfg(feature = "cluster")]
    if let Some(authority) = checkpoint_authority {
        let floor = if advance_decision_floor {
            let Some(capsule_store) = decision_store.clone() else {
                warn!(
                    trigger_epoch,
                    horizon = requested,
                    "[LDB-6050] cluster retention has no recovery capsule store; skipping artifact prune"
                );
                return None;
            };
            let Some(preflight_backend) = state_backend.clone() else {
                warn!(
                    trigger_epoch,
                    horizon = requested,
                    "[LDB-6050] cluster retention has no sealed state backend; skipping artifact prune"
                );
                return None;
            };
            let Some(proof) = leader_proof else {
                warn!(
                    trigger_epoch,
                    horizon = requested,
                    "[LDB-6026] cluster retention has no captured leader proof; skipping artifact prune"
                );
                return None;
            };
            let validate_artifacts = move |outcome| {
                let store = Arc::clone(&store);
                let state_backend = Arc::clone(&preflight_backend);
                let decision_store = Arc::clone(&capsule_store);
                async move {
                    preflight_cluster_retention_cut(
                        store.as_ref(),
                        state_backend.as_ref(),
                        decision_store.as_ref(),
                        &outcome,
                    )
                    .await
                    .map_err(|error| error.to_string())
                }
            };
            tokio::time::timeout(operation_timeout, async {
                authority
                    .prune_cluster_outcomes_before(&proof, requested, validate_artifacts)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
        } else {
            tokio::time::timeout(operation_timeout, async {
                authority
                    .audited_cluster_outcome_retention_boundary()
                    .await
                    .map(|boundary| boundary.artifact_before_epoch)
                    .map_err(|error| error.to_string())
            })
            .await
        };
        return accept_retention_floor(
            floor,
            requested,
            trigger_epoch,
            advance_decision_floor,
            operation_timeout,
        );
    }
    let Some(decision_store) = decision_store else {
        if !advance_decision_floor {
            warn!(
                trigger_epoch,
                horizon = requested,
                "[LDB-6026] follower retention cannot verify the shared decision GC floor; skipping artifact prune"
            );
            return None;
        }
        return Some(requested);
    };
    let floor = if advance_decision_floor {
        tokio::time::timeout(operation_timeout, async {
            decision_store
                .prune_outcomes_before(requested)
                .await
                .map_err(|error| error.to_string())
        })
        .await
    } else {
        tokio::time::timeout(operation_timeout, async {
            decision_store
                .outcome_retention_boundary()
                .await
                .map(|boundary| boundary.before_epoch)
                .map_err(|error| error.to_string())
        })
        .await
    };
    accept_retention_floor(
        floor,
        requested,
        trigger_epoch,
        advance_decision_floor,
        operation_timeout,
    )
}

#[cfg(feature = "cluster")]
async fn run_capsule_gc_step(
    authority: &laminar_core::cluster::control::LeaderLeaseStore,
    operation_timeout: Duration,
) -> bool {
    match tokio::time::timeout(
        operation_timeout,
        authority.maintain_cluster_recovery_capsules(),
    )
    .await
    {
        Ok(Ok(step)) => {
            if step.examined > 0 || step.deleted > 0 || step.quarantined > 0 {
                debug!(
                    examined = step.examined,
                    deleted = step.deleted,
                    quarantined = step.quarantined,
                    pending = step.pending,
                    "cluster recovery capsule maintenance step completed"
                );
            }
            step.pending
        }
        Ok(Err(error)) => {
            warn!(%error, "cluster recovery capsule maintenance failed; retrying while idle");
            true
        }
        Err(_) => {
            warn!(
                ?operation_timeout,
                "cluster recovery capsule maintenance timed out; retrying while idle"
            );
            true
        }
    }
}

async fn run_retention_maintenance(
    store: Arc<dyn CheckpointStore>,
    mut requests: tokio::sync::watch::Receiver<Option<RetentionRequest>>,
    operation_timeout: Duration,
) {
    #[cfg(feature = "cluster")]
    let mut pending_capsule_gc: Option<Arc<laminar_core::cluster::control::LeaderLeaseStore>> =
        None;
    #[cfg(feature = "cluster")]
    let mut idle_gc = tokio::time::interval(
        operation_timeout
            .min(Duration::from_secs(30))
            .max(Duration::from_secs(1)),
    );
    #[cfg(feature = "cluster")]
    idle_gc.tick().await;

    loop {
        #[cfg(feature = "cluster")]
        let changed = tokio::select! {
            changed = requests.changed() => changed,
            _ = idle_gc.tick(), if pending_capsule_gc.is_some() => {
                let authority = Arc::clone(pending_capsule_gc
                    .as_ref()
                    .expect("capsule GC interval is guarded above"));
                if !run_capsule_gc_step(authority.as_ref(), operation_timeout).await {
                    pending_capsule_gc = None;
                }
                continue;
            }
        };
        #[cfg(not(feature = "cluster"))]
        let changed = requests.changed().await;
        if changed.is_err() {
            break;
        }
        // `watch` is intentional: checkpoints can advance while remote deletion is
        // slow, and only the newest safe horizon matters. There is never a queue or
        // task per checkpoint.
        let Some(request) = requests.borrow_and_update().clone() else {
            continue;
        };
        let RetentionRequest {
            horizon,
            trigger_epoch,
            #[cfg(feature = "cluster")]
            preflight_state_backend,
            state_backend,
            state_ancestry_slack,
            decision_store,
            #[cfg(feature = "cluster")]
            checkpoint_authority,
            #[cfg(feature = "cluster")]
            leader_proof,
            advance_decision_floor,
        } = request;
        #[cfg(feature = "cluster")]
        let cleanup_authority = advance_decision_floor
            .then(|| checkpoint_authority.clone())
            .flatten();
        #[cfg(feature = "cluster")]
        if let Some(authority) = cleanup_authority.as_ref() {
            pending_capsule_gc = Some(Arc::clone(authority));
        }

        // Publish/verify the authoritative tombstone before deleting any manifest/state artifact.
        // If the floor cannot reach a safe prefix, an old decision could remain or reappear after
        // its exact recovery data is gone, so the destructive phases are skipped.
        let Some(artifact_horizon) = Box::pin(authorize_retention_horizon(
            horizon,
            trigger_epoch,
            Arc::clone(&store),
            #[cfg(feature = "cluster")]
            preflight_state_backend,
            #[cfg(not(feature = "cluster"))]
            state_backend.clone(),
            decision_store,
            #[cfg(feature = "cluster")]
            checkpoint_authority,
            #[cfg(feature = "cluster")]
            leader_proof,
            advance_decision_floor,
            operation_timeout,
        ))
        .await
        else {
            continue;
        };

        match tokio::time::timeout(operation_timeout, store.prune_before(artifact_horizon)).await {
            Ok(Ok(removed)) => {
                debug!(
                    trigger_epoch,
                    horizon = artifact_horizon,
                    removed,
                    "checkpoint manifests pruned"
                );
            }
            Ok(Err(error)) => warn!(
                trigger_epoch,
                horizon = artifact_horizon,
                %error,
                "[LDB-6026] checkpoint manifest prune failed"
            ),
            Err(_) => warn!(
                trigger_epoch,
                horizon = artifact_horizon,
                ?operation_timeout,
                "[LDB-6026] checkpoint manifest prune timed out"
            ),
        }

        if let Some(state_backend) = state_backend {
            // A retained manifest can still reference an older FULL partial. Keep that ancestry
            // without extending the decision/manifest fallback window itself.
            let state_horizon = state_artifact_horizon(artifact_horizon, state_ancestry_slack);
            if state_horizon > 0 {
                match tokio::time::timeout(
                    operation_timeout,
                    state_backend.prune_before(state_horizon),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => warn!(
                        trigger_epoch,
                        horizon = state_horizon,
                        manifest_horizon = artifact_horizon,
                        %error,
                        "[LDB-6026] state backend prune failed"
                    ),
                    Err(_) => warn!(
                        trigger_epoch,
                        horizon = state_horizon,
                        manifest_horizon = artifact_horizon,
                        ?operation_timeout,
                        "[LDB-6026] state backend prune timed out"
                    ),
                }
            }
        }

        #[cfg(feature = "cluster")]
        if let Some(authority) = cleanup_authority {
            if run_capsule_gc_step(authority.as_ref(), operation_timeout).await {
                pending_capsule_gc = Some(authority);
            } else {
                pending_capsule_gc = None;
            }
        }
    }
}

/// Orchestrates the checkpoint lifecycle across sources, sinks, and operator state.
pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    store: Arc<dyn CheckpointStore>,
    sinks: Vec<RegisteredSink>,
    // Shared with the pipeline callback so barrier admission can claim ids without the mutex.
    allocator: Arc<EpochAllocator>,
    phase: CheckpointPhase,
    // Set before a decision write is issued. After this point a timeout/error is ambiguous and
    // cleanup must leave prepared artifacts intact for recovery instead of rolling sinks back.
    decision_write_started: bool,
    // Owns an issued decision create until it reaches a terminal client-side state. A timeout
    // faults the pipeline but must not detach the write: coordinated recovery may acknowledge a
    // node as stopped only after this handle settles, otherwise it could choose a cut and then
    // observe this process publish a newer decision behind that cut.
    pending_decision_write: Option<PendingDecisionWrite>,
    // Owns the create-before-begin write across caller cancellation. Until it settles, teardown
    // retains the deployment fence and no higher external sink epoch may open.
    pending_sink_witness_create: tokio::sync::Mutex<Option<PendingSinkWitnessCreate>>,
    // Owns the close tombstone write across timeout, caller cancellation, and lost
    // acknowledgements. Exact in-memory ownership is released only after this task is quiescent.
    pending_sink_witness_clear: tokio::sync::Mutex<Option<PendingSinkWitnessClear>>,
    // Live witness retained through pre-commit and the terminal decision. Recovery reconstructs
    // this from the durable inventory after a process crash.
    active_sink_witness:
        parking_lot::Mutex<Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>>,
    // Set before failed-epoch rollback/successor setup starts and cleared only after both finish.
    // Cancellation must leave this latched: `phase == Idle` is not proof that connector cleanup
    // completed, because failure accounting intentionally precedes the bounded cleanup awaits.
    failure_recovery_required: bool,
    // A participant-readiness PUT is the follower's irrevocable prepare boundary. Its error can be
    // an acknowledgement loss after the descriptor landed, so only a durable terminal outcome may
    // authorize rollback once this exact attempt has started the write.
    #[cfg(feature = "cluster")]
    participant_ready_write: Option<CheckpointAttempt>,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    last_checkpoint_duration: Option<Duration>,
    duration_histogram: DurationHistogram,
    prom: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    total_bytes_written: u64,
    // Consulted between manifest persist and sink commit for per-vnode durability.
    state_backend: Option<Arc<dyn StateBackend>>,
    // Stamped into every `write_partial` for the split-brain fence; zero = fence disabled.
    assignment_version: u64,
    // Sources for which the runtime successfully installed cluster vnode ownership. Their
    // connector cursor must be captured under the same assignment as operator and shuffle state.
    assignment_scoped_sources: HashSet<String>,
    // Exact admission certificate for the attempt currently owning this serialized coordinator.
    // Terminal announcements retain it even when membership changes during failure cleanup.
    active_assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    // Exact authority captured before the reversible cluster Prepare. Local runtimes keep None.
    active_leader_proof: Option<LeaderProof>,
    // Written before sink commits so recovery can distinguish a committed epoch from a crash.
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    // Bound once from the canonical topology before recovery; stamped into every manifest.
    pipeline_identity: Option<PipelineIdentity>,
    // Create-once identity from the durable decision namespace. Unlike topology identity this
    // rotates on an explicit checkpoint-store reset and fences surviving external cursors.
    deployment_id: Option<String>,
    // Highest epoch this process recorded a commit marker for; pins the prune horizon so a
    // coordinated rewind always finds its target's artifacts intact.
    highest_decided: u64,
    // Folded by the leader with follower states to compute the safe cluster frontier.
    local_watermark: CheckpointWatermark,
    // Leader-side cluster watermark candidate, made recovery-safe only by a Commit outcome.
    #[cfg(feature = "cluster")]
    cluster_watermark: CheckpointWatermark,
    // Vnodes this coordinator owns; drives per-vnode marker writes.
    vnode_set: Vec<u32>,
    // In cluster mode: the full registry. Single-instance mirrors `vnode_set`.
    gate_vnode_set: Vec<u32>,
    // First epoch admitted after the latest vnode rotation. Epochs below this captured
    // under the previous assignment can never seal their gate — fail them fast.
    rotation_epoch_floor: u64,
    // Per-vnode operator-state slices for the in-flight checkpoint.
    // Empty in single-instance mode (the partial is a durability marker only).
    #[allow(clippy::disallowed_types)] // matches the graph snapshot shape
    pending_vnode_states: StagedVnodeStates,
    // Per-sink commit descriptors from `pre_commit`, persisted in `write_vnode_partials`.
    // Only coordinated sinks contribute; empty otherwise.
    #[allow(clippy::disallowed_types)]
    pending_sink_descriptors: std::collections::HashMap<String, Option<Vec<u8>>>,
    // Lowest uncommitted epoch; prune must not cross it. Advanced by the
    // committer task (leader only); starts at 0.
    coordinated_commit_floor: Arc<std::sync::atomic::AtomicU64>,
    // Exact number of sealed+decided checkpoints not yet committed by every external sink.
    // `known` stays false until the external cursors and durable seal inventory are reconciled.
    coordinated_commit_lag: Arc<std::sync::atomic::AtomicU64>,
    coordinated_commit_lag_known: Arc<std::sync::atomic::AtomicBool>,
    // Wakes a checkpoint tail paused at the external-commit hard bound.
    coordinated_commit_progress: Arc<tokio::sync::Notify>,
    // Wakes the designated committer as soon as a sealed checkpoint has a durable decision.
    // `Notify` coalesces bursts safely because each pass drains every ready checkpoint.
    coordinated_commit_notify: Arc<tokio::sync::Notify>,
    // Self-contained root bases for reference partials. Delta preparation always removes an old
    // entry, and reference preparation retains the original root rather than chaining references.
    // Bytes are refcounted.
    last_vnode_uploads: std::collections::HashMap<
        u32,
        (
            CheckpointAttempt,
            std::collections::HashMap<String, UploadedSlice>,
        ),
    >,
    // Exact previous partial per vnode — the parent link a delta/reference partial chains to.
    #[allow(clippy::disallowed_types)]
    last_partial_attempt: std::collections::HashMap<u32, CheckpointAttempt>,
    #[allow(clippy::disallowed_types)]
    last_partial_delta_depth: std::collections::HashMap<u32, u32>,
    // Candidates above advance after all writes land. Reuse is allowed only after the exact
    // state seal proves that the candidate is durable for this vnode.
    last_sealed_partial_attempt: std::collections::HashMap<u32, CheckpointAttempt>,
    last_sealed_delta_depth: std::collections::HashMap<u32, u32>,
    last_sealed_upload_attempt: std::collections::HashMap<u32, CheckpointAttempt>,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    // A single watch slot coalesces checkpoint retention requests at a monotonically advancing
    // recovery/external-commit-safe horizon. The JoinSet owns exactly one worker and aborts it on
    // coordinator drop, so remote GC can neither enter source-ack latency nor detach.
    retention_requests: tokio::sync::watch::Sender<Option<RetentionRequest>>,
    // Bounded suffix of outcome-certified committed cuts. Checkpoint IDs may contain arbitrary
    // burned gaps, so retention counts entries here instead of subtracting from the numeric ID.
    recent_committed_checkpoints: VecDeque<u64>,
    recent_committed_capacity: usize,
    retention_requested_horizon: u64,
    // Additional state-only history required by retained reference/delta partials. Decisions and
    // manifests retain exactly `max_retained`; only their transitive state ancestors get slack.
    state_ancestry_slack: u64,
    // Private runtime-derived cap, enforced again here so a faulty capture producer cannot make
    // state ancestry exceed the GC slack. `None` disables delta partial admission.
    delta_chain_bound: Option<u32>,
    // Followers prune only their participant-local manifest namespace. Keep that horizon
    // independent from shared state/decision GC: after a role change, a follower-local horizon
    // must never let a newly promoted leader advance shared retention past its commit floor.
    #[cfg(feature = "cluster")]
    local_manifest_retention_requested_horizon: u64,
    maintenance_tasks: tokio::task::JoinSet<()>,
    // Invalidated on `register_sink`; rebuilt on the next checkpoint.
    cached_sorted_sink_names: Option<Vec<String>>,
}

/// Load the highest readable manifest.
///
/// Deterministically corrupt manifests are skipped so recovery can select an older valid cut.
/// Operational failures are never treated as corruption: doing so could start a writer while its
/// durable history is merely unavailable. Checkpoint ID continuity is owned independently by
/// durable reservations in the decision store.
async fn load_highest(
    store: &dyn CheckpointStore,
) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
    let ids = store.list_ids().await?;
    for id in ids.iter().rev() {
        match store.load_by_id(*id).await {
            Ok(Some(manifest)) if manifest.checkpoint_id == *id => return Ok(Some(manifest)),
            Ok(Some(m)) => {
                warn!(
                    storage_id = *id,
                    manifest_id = m.checkpoint_id,
                    "[LDB-6041] checkpoint id binding mismatch; trying an older checkpoint",
                );
            }
            Ok(None) => {
                warn!(
                    storage_id = *id,
                    "[LDB-6041] listed checkpoint disappeared; trying an older checkpoint",
                );
            }
            Err(CheckpointStoreError::Serde(e)) => {
                warn!(
                    storage_id = *id,
                    error = %e,
                    "[LDB-6041] corrupt checkpoint manifest; trying an older checkpoint",
                );
            }
            Err(CheckpointStoreError::Invalid(e)) => {
                warn!(
                    storage_id = *id,
                    error = %e,
                    "[LDB-6041] incompatible checkpoint manifest; trying an older checkpoint",
                );
            }
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}

impl CheckpointCoordinator {
    /// Create a coordinator seeded from the highest stored checkpoint.
    ///
    /// # Errors
    /// Returns a store read failure rather than silently starting at epoch 1 and clobbering
    /// on-disk state.
    pub async fn new(
        config: CheckpointConfig,
        store: Box<dyn CheckpointStore>,
    ) -> Result<Self, DbError> {
        laminar_core::storage::checkpoint_store::validate_max_checkpoint_state_bytes(
            config.max_staged_bytes,
        )
        .map_err(|error| DbError::Config(format!("checkpoint.max_staged_bytes: {error}")))?;
        let store_state_limit = store.max_state_data_bytes();
        if store_state_limit != config.max_staged_bytes {
            return Err(DbError::Config(format!(
                "checkpoint store state limit {store_state_limit} does not match checkpoint.max_staged_bytes {}",
                config.max_staged_bytes
            )));
        }
        let recent_committed_capacity = config.max_retained.checked_add(1).ok_or_else(|| {
            DbError::Config("checkpoint.max_retained is too large to count the current cut".into())
        })?;
        if recent_committed_capacity > MAX_CHECKPOINT_INVENTORY_ENTRIES {
            return Err(DbError::Config(format!(
                "checkpoint.max_retained must be less than {MAX_CHECKPOINT_INVENTORY_ENTRIES}"
            )));
        }
        let store: Arc<dyn CheckpointStore> = Arc::from(store);
        let highest = load_highest(store.as_ref()).await.map_err(|e| {
            DbError::Checkpoint(format!(
                "[LDB-6028] failed to list checkpoints at coordinator \
                 construction: {e} — refusing to start at epoch 1 and \
                 clobber existing on-disk state"
            ))
        })?;
        let epoch = match highest.as_ref() {
            Some(manifest) => {
                checked_successor_epoch(manifest.epoch, "seeding the checkpoint coordinator")?
            }
            None => 1,
        };
        let allocation_timeout = config.checkpoint_timeout;
        let retention_timeout = config.checkpoint_timeout;
        let (retention_requests, retention_receiver) = tokio::sync::watch::channel(None);
        let mut maintenance_tasks = tokio::task::JoinSet::new();
        maintenance_tasks.spawn(run_retention_maintenance(
            Arc::clone(&store),
            retention_receiver,
            retention_timeout,
        ));

        Ok(Self {
            config,
            store,
            sinks: Vec::new(),
            allocator: Arc::new(EpochAllocator::new(epoch, allocation_timeout)),
            phase: CheckpointPhase::Idle,
            decision_write_started: false,
            pending_decision_write: None,
            pending_sink_witness_create: tokio::sync::Mutex::new(None),
            pending_sink_witness_clear: tokio::sync::Mutex::new(None),
            active_sink_witness: parking_lot::Mutex::new(None),
            failure_recovery_required: false,
            #[cfg(feature = "cluster")]
            participant_ready_write: None,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            prom: None,
            total_bytes_written: 0,
            state_backend: None,
            assignment_version: 0,
            assignment_scoped_sources: HashSet::new(),
            active_assignment_fence: None,
            active_leader_proof: None,
            decision_store: None,
            pipeline_identity: None,
            deployment_id: None,
            highest_decided: 0,
            local_watermark: CheckpointWatermark::Uninitialized,
            #[cfg(feature = "cluster")]
            cluster_watermark: CheckpointWatermark::Uninitialized,
            vnode_set: Vec::new(),
            gate_vnode_set: Vec::new(),
            rotation_epoch_floor: 0,
            pending_vnode_states: std::collections::HashMap::new(),
            pending_sink_descriptors: std::collections::HashMap::new(),
            coordinated_commit_floor: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            coordinated_commit_lag: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            coordinated_commit_lag_known: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            coordinated_commit_progress: Arc::new(tokio::sync::Notify::new()),
            coordinated_commit_notify: Arc::new(tokio::sync::Notify::new()),
            last_vnode_uploads: std::collections::HashMap::new(),
            last_partial_attempt: std::collections::HashMap::new(),
            last_partial_delta_depth: std::collections::HashMap::new(),
            last_sealed_partial_attempt: std::collections::HashMap::new(),
            last_sealed_delta_depth: std::collections::HashMap::new(),
            last_sealed_upload_attempt: std::collections::HashMap::new(),
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            retention_requests,
            recent_committed_checkpoints: VecDeque::new(),
            recent_committed_capacity,
            retention_requested_horizon: 0,
            state_ancestry_slack: 0,
            delta_chain_bound: None,
            #[cfg(feature = "cluster")]
            local_manifest_retention_requested_horizon: 0,
            maintenance_tasks,
            cached_sorted_sink_names: None,
        })
    }

    async fn wait_pending_decision_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Option<PendingDecisionWait> {
        let (epoch, checkpoint_id, outcome) = {
            let pending = self.pending_decision_write.as_mut()?;
            let epoch = pending.epoch;
            let checkpoint_id = pending.checkpoint_id;
            let outcome = tokio::time::timeout_at(deadline, &mut pending.handle).await;
            (epoch, checkpoint_id, outcome)
        };
        match outcome {
            Ok(outcome) => {
                // The task is terminal, so removing its completed handle cannot detach I/O.
                self.pending_decision_write.take();
                Some(PendingDecisionWait::Completed {
                    epoch,
                    checkpoint_id,
                    outcome: outcome.map(|result| result.map(Box::new)),
                })
            }
            Err(_) => Some(PendingDecisionWait::TimedOut {
                epoch,
                checkpoint_id,
            }),
        }
    }

    fn outcome_matches_active_authority(
        &self,
        outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
        attempt: CheckpointAttempt,
    ) -> bool {
        outcome.epoch == attempt.epoch
            && outcome.checkpoint_id == attempt.checkpoint_id
            && self.deployment_id.as_ref() == Some(&outcome.deployment_id)
            && outcome.scope == self.active_outcome_scope()
            && outcome.assignment_fence == self.active_assignment_fence
            && outcome.leader_proof == self.active_leader_proof
    }

    async fn record_terminal_outcome_until(
        &mut self,
        attempt: CheckpointAttempt,
        verdict: laminar_core::checkpoint_decision::CheckpointVerdict,
        recovery_capsule: Option<laminar_core::checkpoint::RecoveryCapsuleRef>,
        deadline: tokio::time::Instant,
    ) -> Result<laminar_core::checkpoint_decision::RecordOutcomeResult, String> {
        if let Some(pending) = self.pending_decision_write.as_ref() {
            return Err(format!(
                "[LDB-6038] checkpoint {} epoch {} cannot publish an outcome while checkpoint {} epoch {} still owns an outcome create",
                attempt.checkpoint_id, attempt.epoch, pending.checkpoint_id, pending.epoch
            ));
        }
        let handle;
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            let proof = self.active_leader_proof.clone().ok_or_else(|| {
                "[LDB-6054] cluster outcome create has no captured leader proof".to_owned()
            })?;
            let assignment_fence = self.active_assignment_fence.clone().ok_or_else(|| {
                "[LDB-6054] cluster outcome create has no assignment certificate".to_owned()
            })?;
            if !controller.proof_is_live(&proof) {
                return Err(
                    "[LDB-6054] captured leader proof is not live at terminal outcome create"
                        .into(),
                );
            }
            let authority = controller
                .checkpoint_authority()
                .map_err(|error| format!("[LDB-6050] {error}"))?;
            handle = tokio::spawn(async move {
                authority
                    .record_cluster_outcome(
                        &proof,
                        attempt.epoch,
                        attempt.checkpoint_id,
                        assignment_fence,
                        verdict,
                        recovery_capsule,
                    )
                    .await
                    .map_err(|error| error.to_string())
            });
        } else {
            let store = self
                .decision_store
                .clone()
                .ok_or_else(|| "[LDB-6050] checkpoint outcome store is not bound".to_owned())?;
            handle = tokio::spawn(async move {
                store
                    .record_outcome(
                        attempt.epoch,
                        attempt.checkpoint_id,
                        laminar_core::checkpoint_decision::CheckpointScope::Local,
                        None,
                        None,
                        verdict,
                        recovery_capsule,
                    )
                    .await
                    .map_err(|error| error.to_string())
            });
        }
        #[cfg(not(feature = "cluster"))]
        {
            let store = self
                .decision_store
                .clone()
                .ok_or_else(|| "[LDB-6050] checkpoint outcome store is not bound".to_owned())?;
            handle = tokio::spawn(async move {
                store
                    .record_outcome(
                        attempt.epoch,
                        attempt.checkpoint_id,
                        laminar_core::checkpoint_decision::CheckpointScope::Local,
                        None,
                        None,
                        verdict,
                        recovery_capsule,
                    )
                    .await
                    .map_err(|error| error.to_string())
            });
        }
        self.decision_write_started = true;
        self.pending_decision_write = Some(PendingDecisionWrite {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            handle,
        });
        match self.wait_pending_decision_until(deadline).await {
            Some(PendingDecisionWait::Completed {
                outcome: Ok(Ok(result)),
                ..
            }) => Ok(*result),
            Some(PendingDecisionWait::Completed {
                outcome: Ok(Err(error)),
                ..
            }) => Err(format!("durable checkpoint outcome create failed: {error}")),
            Some(PendingDecisionWait::Completed {
                outcome: Err(error),
                ..
            }) => Err(format!("checkpoint outcome task failed: {error}")),
            Some(PendingDecisionWait::TimedOut { .. }) => Err(
                "durable checkpoint outcome did not settle before the checkpoint deadline"
                    .to_owned(),
            ),
            None => Err("checkpoint outcome task ownership disappeared".into()),
        }
    }

    /// Wait for an ambiguous decision create to stop writing before releasing lifecycle fences.
    ///
    /// A terminal I/O or task error is still an ambiguous durable outcome, but it is quiescent.
    /// Startup audits the valid Prepared witness and its create-once terminal key before choosing
    /// a recovery frontier. A timeout retains the owned task and requires teardown to be retried.
    pub(crate) async fn quiesce_pending_decision_write_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.quiesce_pending_sink_witness_create_until(deadline)
            .await?;
        self.quiesce_pending_sink_witness_clear_until(deadline)
            .await?;
        match self.wait_pending_decision_until(deadline).await {
            None => Ok(()),
            Some(PendingDecisionWait::Completed {
                epoch,
                checkpoint_id,
                outcome: Ok(Ok(result)),
            }) => {
                let outcome = match *result {
                    laminar_core::checkpoint_decision::RecordOutcomeResult::Created(outcome)
                    | laminar_core::checkpoint_decision::RecordOutcomeResult::Unchanged(outcome) => {
                        outcome
                    }
                    laminar_core::checkpoint_decision::RecordOutcomeResult::Conflict { winner } => {
                        winner
                    }
                };
                if outcome.is_commit() {
                    self.highest_decided = self.highest_decided.max(epoch);
                }
                debug!(
                    epoch,
                    checkpoint_id, "ambiguous decision write settled successfully during teardown"
                );
                Ok(())
            }
            Some(PendingDecisionWait::Completed {
                epoch,
                checkpoint_id,
                outcome: Ok(Err(error)),
            }) => {
                warn!(
                    epoch,
                    checkpoint_id,
                    %error,
                    "[LDB-6038] decision write reached a terminal I/O error during teardown; recovery will audit its Prepared witness and terminal key"
                );
                Ok(())
            }
            Some(PendingDecisionWait::Completed {
                epoch,
                checkpoint_id,
                outcome: Err(error),
            }) => {
                warn!(
                    epoch,
                    checkpoint_id,
                    %error,
                    "[LDB-6038] decision task terminated during teardown; recovery will audit its Prepared witness and terminal key"
                );
                Ok(())
            }
            Some(PendingDecisionWait::TimedOut {
                epoch,
                checkpoint_id,
            }) => Err(DbError::Checkpoint(format!(
                "[LDB-6038] checkpoint {checkpoint_id} epoch {epoch} still has an in-flight durable decision write; teardown remains fenced and must be retried"
            ))),
        }
    }

    fn record_committed_checkpoint(&mut self, checkpoint_id: u64) -> Result<Option<u64>, DbError> {
        if checkpoint_id == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6041] committed checkpoint ID must be nonzero".into(),
            ));
        }
        match self.recent_committed_checkpoints.back().copied() {
            Some(latest) if checkpoint_id < latest => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] committed checkpoint {checkpoint_id} regresses retained cut {latest}"
                )));
            }
            Some(latest) if checkpoint_id == latest => {}
            _ => self.recent_committed_checkpoints.push_back(checkpoint_id),
        }
        while self.recent_committed_checkpoints.len() > self.recent_committed_capacity {
            self.recent_committed_checkpoints.pop_front();
        }
        if self.recent_committed_checkpoints.len() < self.recent_committed_capacity {
            Ok(None)
        } else {
            Ok(self.recent_committed_checkpoints.front().copied())
        }
    }

    fn schedule_retention_prune(
        &mut self,
        backend: Option<Arc<dyn StateBackend>>,
        decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
        horizon: u64,
        epoch: u64,
    ) {
        while let Some(result) = self.maintenance_tasks.try_join_next() {
            match result {
                Ok(()) => warn!("checkpoint retention worker stopped unexpectedly"),
                Err(error) => warn!(%error, "checkpoint retention worker terminated unexpectedly"),
            }
        }

        self.retention_requested_horizon = self.retention_requested_horizon.max(horizon);
        #[cfg(feature = "cluster")]
        let (decision_store, checkpoint_authority, leader_proof) = if let Some(controller) =
            self.cluster_controller.as_ref()
        {
            let authority = match controller.checkpoint_authority() {
                Ok(authority) => authority,
                Err(error) => {
                    warn!(
                        epoch,
                        horizon = self.retention_requested_horizon,
                        %error,
                        "[LDB-6026] cluster checkpoint authority is unavailable; skipping artifact prune"
                    );
                    return;
                }
            };
            let Some(proof) = self.active_leader_proof.clone() else {
                warn!(
                        epoch,
                        horizon = self.retention_requested_horizon,
                        "[LDB-6026] cluster retention has no captured leader proof; skipping artifact prune"
                    );
                return;
            };
            (decision_store, Some(authority), Some(proof))
        } else {
            (decision_store, None, None)
        };
        let request = RetentionRequest {
            horizon: self.retention_requested_horizon,
            trigger_epoch: epoch,
            #[cfg(feature = "cluster")]
            preflight_state_backend: backend.clone(),
            state_backend: backend,
            state_ancestry_slack: self.state_ancestry_slack,
            decision_store,
            #[cfg(feature = "cluster")]
            checkpoint_authority,
            #[cfg(feature = "cluster")]
            leader_proof,
            advance_decision_floor: true,
        };
        if self.retention_requests.send(Some(request)).is_err() {
            warn!(
                epoch,
                horizon = self.retention_requested_horizon,
                "[LDB-6026] checkpoint retention worker is unavailable"
            );
        }
    }

    /// Schedule participant-local manifest retention without advancing the shared GC horizon.
    ///
    /// A coordinator can be promoted after serving as a follower. Reusing
    /// `retention_requested_horizon` here would allow the old follower horizon to overtake a
    /// lagging coordinated-sink commit floor when that coordinator later becomes leader.
    #[cfg(feature = "cluster")]
    fn schedule_local_manifest_retention_prune(&mut self, horizon: u64, epoch: u64) {
        while let Some(result) = self.maintenance_tasks.try_join_next() {
            match result {
                Ok(()) => warn!("checkpoint retention worker stopped unexpectedly"),
                Err(error) => warn!(%error, "checkpoint retention worker terminated unexpectedly"),
            }
        }

        self.local_manifest_retention_requested_horizon =
            self.local_manifest_retention_requested_horizon.max(horizon);
        let Some(controller) = self.cluster_controller.as_ref() else {
            warn!(
                epoch,
                horizon = self.local_manifest_retention_requested_horizon,
                "[LDB-6026] follower retention has no cluster controller; skipping manifest prune"
            );
            return;
        };
        let checkpoint_authority = match controller.checkpoint_authority() {
            Ok(authority) => Some(authority),
            Err(error) => {
                warn!(
                    epoch,
                    horizon = self.local_manifest_retention_requested_horizon,
                    %error,
                    "[LDB-6026] follower retention cannot read the cluster checkpoint authority; skipping manifest prune"
                );
                return;
            }
        };
        let request = RetentionRequest {
            horizon: self.local_manifest_retention_requested_horizon,
            trigger_epoch: epoch,
            preflight_state_backend: None,
            state_backend: None,
            state_ancestry_slack: 0,
            decision_store: None,
            checkpoint_authority,
            leader_proof: None,
            advance_decision_floor: false,
        };
        if self.retention_requests.send(Some(request)).is_err() {
            warn!(
                epoch,
                horizon = self.local_manifest_retention_requested_horizon,
                "[LDB-6026] checkpoint retention worker is unavailable"
            );
        }
    }

    /// Activate cluster-mode 2PC. Without this the coordinator runs single-instance semantics.
    #[cfg(feature = "cluster")]
    pub fn set_cluster_controller(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        self.cluster_controller = Some(controller);
    }

    /// Wire a state backend to enable per-vnode markers and the durability gate.
    ///
    /// # Errors
    /// Returns [`DbError::Config`] when the backend's key-group capacity is invalid or does not
    /// match the checkpoint store topology.
    pub fn set_state_backend(&mut self, backend: Arc<dyn StateBackend>) -> Result<(), DbError> {
        let capacity = backend.key_group_capacity();
        let key_groups = laminar_core::state::KeyGroupCount::try_from(capacity).map_err(|_| {
            DbError::Config(format!(
                "state backend key-group capacity must be between 1 and {}, got {capacity}",
                laminar_core::state::MAX_KEY_GROUP_COUNT
            ))
        })?;
        let store_key_groups = self.store.key_group_count();
        if key_groups != store_key_groups {
            return Err(DbError::Config(format!(
                "state backend key-group capacity {key_groups} does not match checkpoint store key-group count {store_key_groups}"
            )));
        }
        self.state_backend = Some(backend);
        Ok(())
    }

    /// Install the private runtime-derived ancestry policy for cluster-shared vnode state.
    ///
    /// Reference age is fixed by manifest retention. Delta depth is admitted only when this
    /// method supplies a bound, and the coordinator independently enforces it before persistence.
    #[cfg(feature = "cluster")]
    pub(crate) fn configure_state_ancestry(&mut self, delta_chain_bound: Option<u32>) {
        self.delta_chain_bound = delta_chain_bound;
        self.state_ancestry_slack =
            bounded_state_ancestry_slack(self.config.max_retained, delta_chain_bound);
    }

    /// Wire the durable commit-marker store.
    pub fn set_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        self.allocator.bind_decision_store(Arc::clone(&store))?;
        self.decision_store = Some(store);
        Ok(())
    }

    /// Wire the durable commit-marker store and bind its create-once deployment identity.
    ///
    /// Direct coordinator users must call this before recovery or checkpointing. Loading the
    /// identity from the store keeps the deployment fence tied to durable provenance rather than
    /// accepting a caller-supplied value.
    pub async fn bind_durable_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        let deployment_id = store
            .load_or_create_deployment_id()
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "load/create durable deployment identity before checkpoint startup: {error}"
                ))
            })?;
        if self
            .deployment_id
            .as_ref()
            .is_some_and(|existing| existing != &deployment_id)
        {
            return Err(DbError::Checkpoint(
                "[LDB-6043] deployment identity cannot change while checkpoint state is active"
                    .into(),
            ));
        }
        self.set_decision_store(store)?;
        self.bind_deployment_id(deployment_id)
    }

    /// Bind the canonical pipeline identity before recovery or checkpointing.
    ///
    /// Rebinding to the same value is idempotent; changing it in-place would require a topology
    /// migration barrier and is rejected.
    pub(crate) fn bind_pipeline_identity(
        &mut self,
        identity: PipelineIdentity,
    ) -> Result<(), DbError> {
        match self.pipeline_identity.as_ref() {
            None => {
                self.pipeline_identity = Some(identity);
                Ok(())
            }
            Some(existing) if existing == &identity => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "[LDB-6043] pipeline identity cannot change while checkpoint state is active"
                    .into(),
            )),
        }
    }

    /// Bind the create-once durable deployment incarnation before any coordinated sink opens.
    pub(crate) fn bind_deployment_id(&mut self, deployment_id: String) -> Result<(), DbError> {
        match self.deployment_id.as_ref() {
            None => {
                self.deployment_id = Some(deployment_id);
                Ok(())
            }
            Some(existing) if existing == &deployment_id => Ok(()),
            Some(_) => Err(DbError::Checkpoint(
                "[LDB-6043] deployment identity cannot change while checkpoint state is active"
                    .into(),
            )),
        }
    }

    fn expected_pipeline_identity(&self) -> PipelineIdentity {
        self.pipeline_identity
            .clone()
            .unwrap_or_else(PipelineIdentity::empty)
    }

    fn expected_deployment_id(&self) -> Result<&str, DbError> {
        self.deployment_id.as_deref().ok_or_else(|| {
            DbError::Checkpoint(
                "coordinated commit requires a durable deployment identity before startup".into(),
            )
        })
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

    fn validate_sink_open_witness(
        &self,
        witness: &laminar_core::checkpoint_decision::CheckpointSinkOpenWitness,
    ) -> Result<(), DbError> {
        let expected_sinks = self.committable_sink_names()?;
        self.validate_sink_open_witness_for_sinks(witness, &expected_sinks)
    }

    fn validate_sink_open_witness_for_sinks(
        &self,
        witness: &laminar_core::checkpoint_decision::CheckpointSinkOpenWitness,
        expected_sinks: &[String],
    ) -> Result<(), DbError> {
        if !witness.attempt.is_canonical() {
            return Err(DbError::Checkpoint(
                "[LDB-6050] sink-open witness has a non-canonical checkpoint identity".into(),
            ));
        }
        if witness.deployment_id != self.expected_deployment_id()?
            || witness.pipeline_identity != self.expected_pipeline_identity()
            || witness.participant_id != self.store.participant_id()
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] sink-open witness for checkpoint {} does not match the active deployment, pipeline, and participant",
                witness.attempt.checkpoint_id
            )));
        }
        if witness.committable_sinks != expected_sinks {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] sink-open witness for checkpoint {} names {:?}, but the active committable sinks are {:?}",
                witness.attempt.checkpoint_id, witness.committable_sinks, expected_sinks
            )));
        }
        Ok(())
    }

    /// Audit durable sink ownership against the configured topology before any connector is
    /// opened or asked to reconcile an epoch. Settlement re-reads the witness after recovery.
    pub(crate) async fn audit_sink_open_witness_topology(
        &self,
        mut expected_sinks: Vec<String>,
    ) -> Result<(), DbError> {
        expected_sinks.sort_unstable();
        if expected_sinks.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DbError::Checkpoint(
                "checkpoint-committable sink names must be unique".into(),
            ));
        }
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] sink-open audit requires a durable decision store".into(),
            )
        })?;
        let witness = tokio::time::timeout(self.config.cleanup_timeout, store.sink_open_witness())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6050] sink-open witness inventory timed out during topology audit".into(),
                )
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] sink-open witness inventory failed during topology audit: {error}"
                ))
            })?;
        let Some(witness) = witness else {
            return Ok(());
        };
        self.validate_sink_open_witness_for_sinks(&witness, &expected_sinks)?;
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Err(DbError::Checkpoint(
                "[LDB-0013] cluster exactly-once sink recovery is unavailable until connector epoch operations are leader-term fenced"
                    .into(),
            ));
        }
        Ok(())
    }

    async fn create_sink_open_witness_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness, DbError> {
        if self.active_sink_witness.lock().is_some() {
            return Err(DbError::Checkpoint(
                "[LDB-6050] a prior durable sink-open witness is still active".into(),
            ));
        }
        let store = self.decision_store.clone().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] sink epoch open requires a durable decision store".into(),
            )
        })?;
        let pipeline_identity = self.expected_pipeline_identity();
        let participant_id = self.store.participant_id();
        let committable_sinks = self.committable_sink_names()?;
        let mut pending =
            tokio::time::timeout_at(deadline, self.pending_sink_witness_create.lock())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "[LDB-6050] sink-open witness ownership deadline expired".into(),
                    )
                })?;
        if let Some(existing) = pending.as_ref() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] checkpoint {} still owns an in-flight sink-open witness create",
                existing.attempt.checkpoint_id
            )));
        }
        let handle = tokio::spawn(async move {
            store
                .create_sink_open_witness(
                    pipeline_identity,
                    participant_id,
                    attempt,
                    committable_sinks,
                )
                .await
                .map_err(|error| error.to_string())
        });
        *pending = Some(PendingSinkWitnessCreate { attempt, handle });
        let outcome = tokio::time::timeout_at(
            deadline,
            &mut pending.as_mut().expect("pending create installed").handle,
        )
        .await;
        match outcome {
            Ok(Ok(Ok(witness))) => {
                pending.take();
                drop(pending);
                self.validate_sink_open_witness(&witness)?;
                *self.active_sink_witness.lock() = Some(witness.clone());
                Ok(witness)
            }
            Ok(Ok(Err(error))) => {
                pending.take();
                Err(DbError::Checkpoint(format!(
                    "[LDB-6050] durable sink-open witness create failed: {error}"
                )))
            }
            Ok(Err(error)) => {
                pending.take();
                Err(DbError::Checkpoint(format!(
                    "[LDB-6050] sink-open witness task failed: {error}"
                )))
            }
            Err(_) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] durable sink-open witness for checkpoint {} did not settle before its deadline",
                attempt.checkpoint_id
            ))),
        }
    }

    async fn quiesce_pending_sink_witness_create_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut pending =
            tokio::time::timeout_at(deadline, self.pending_sink_witness_create.lock())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "[LDB-6050] teardown could not acquire sink-open witness ownership".into(),
                    )
                })?;
        let Some(write) = pending.as_mut() else {
            return Ok(());
        };
        let attempt = write.attempt;
        match tokio::time::timeout_at(deadline, &mut write.handle).await {
            Ok(Ok(Ok(witness))) => {
                pending.take();
                drop(pending);
                self.validate_sink_open_witness(&witness)?;
                *self.active_sink_witness.lock() = Some(witness);
                Ok(())
            }
            Ok(Ok(Err(error))) => {
                pending.take();
                warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    %error,
                    "[LDB-6050] sink-open witness create ended with an I/O error; recovery will audit the durable inventory"
                );
                Ok(())
            }
            Ok(Err(error)) => {
                pending.take();
                warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    %error,
                    "[LDB-6050] sink-open witness task terminated; recovery will audit the durable inventory"
                );
                Ok(())
            }
            Err(_) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] checkpoint {} still has an in-flight sink-open witness create; teardown remains fenced",
                attempt.checkpoint_id
            ))),
        }
    }

    fn spawn_sink_open_witness_clear(
        &self,
        witness: laminar_core::checkpoint_decision::CheckpointSinkOpenWitness,
    ) -> Result<tokio::task::JoinHandle<Result<(), String>>, DbError> {
        let store = self.decision_store.clone().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] sink-open witness cleanup requires a durable decision store".into(),
            )
        })?;
        Ok(tokio::spawn(async move {
            store
                .clear_sink_open_witness(&witness)
                .await
                .map_err(|error| error.to_string())
        }))
    }

    fn require_active_sink_open_witness(
        &self,
        witness: &laminar_core::checkpoint_decision::CheckpointSinkOpenWitness,
    ) -> Result<(), DbError> {
        match self.active_sink_witness.lock().as_ref() {
            Some(active) if active == witness => Ok(()),
            Some(active) => Err(DbError::Checkpoint(format!(
                "[LDB-6050] sink-open witness cleanup for checkpoint {} conflicts with the exact active witness for checkpoint {}",
                witness.attempt.checkpoint_id, active.attempt.checkpoint_id
            ))),
            None => Err(DbError::Checkpoint(format!(
                "[LDB-6050] sink-open witness cleanup for checkpoint {} has no exact in-memory owner",
                witness.attempt.checkpoint_id
            ))),
        }
    }

    fn finalize_sink_open_witness_clear(
        &self,
        witness: &laminar_core::checkpoint_decision::CheckpointSinkOpenWitness,
    ) -> Result<(), DbError> {
        let successor = checked_successor_epoch(
            witness.attempt.epoch,
            "advancing after sink-open witness cleanup",
        )?;
        let mut active = self.active_sink_witness.lock();
        match active.as_ref() {
            Some(current) if current == witness => {}
            Some(current) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6050] durable sink-open cleanup for checkpoint {} conflicts with the exact active witness for checkpoint {}",
                    witness.attempt.checkpoint_id, current.attempt.checkpoint_id
                )));
            }
            None => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6050] durable sink-open cleanup for checkpoint {} lost its in-memory owner",
                    witness.attempt.checkpoint_id
                )));
            }
        }
        self.allocator
            .burn_sink_epoch_reservation(witness.attempt)?;
        self.allocator.advance_epoch_to(successor);
        active.take();
        Ok(())
    }

    async fn settle_sink_open_witness_clear_until(
        &self,
        requested: Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if let Some(witness) = requested.as_ref() {
            self.validate_sink_open_witness(witness)?;
        }
        let mut pending = tokio::time::timeout_at(deadline, self.pending_sink_witness_clear.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6050] sink-open witness cleanup ownership deadline expired".into(),
                )
            })?;

        if let Some(requested) = requested {
            match pending.as_ref() {
                Some(existing) if existing.witness != requested => {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6050] checkpoint {} cannot clear the sink-open witness while checkpoint {} owns an unresolved cleanup",
                        requested.attempt.checkpoint_id,
                        existing.witness.attempt.checkpoint_id
                    )));
                }
                Some(_) => {}
                None => {
                    self.require_active_sink_open_witness(&requested)?;
                    *pending = Some(PendingSinkWitnessClear {
                        witness: requested,
                        state: PendingSinkWitnessClearState::NeedsRetry,
                    });
                }
            }
        }

        let Some(write) = pending.as_mut() else {
            return Ok(());
        };
        self.validate_sink_open_witness(&write.witness)?;
        checked_successor_epoch(
            write.witness.attempt.epoch,
            "preparing sink-open witness cleanup",
        )?;
        if matches!(&write.state, PendingSinkWitnessClearState::NeedsRetry) {
            write.state = PendingSinkWitnessClearState::Running(
                self.spawn_sink_open_witness_clear(write.witness.clone())?,
            );
        }

        let outcome = match &mut write.state {
            PendingSinkWitnessClearState::Running(handle) => {
                Some(tokio::time::timeout_at(deadline, handle).await)
            }
            PendingSinkWitnessClearState::Verified => None,
            PendingSinkWitnessClearState::NeedsRetry => unreachable!("clear retry was started"),
        };
        if let Some(outcome) = outcome {
            match outcome {
                Ok(Ok(Ok(()))) => {
                    write.state = PendingSinkWitnessClearState::Verified;
                }
                Ok(Ok(Err(error))) => {
                    write.state = PendingSinkWitnessClearState::NeedsRetry;
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6050] sink-open witness cleanup failed for checkpoint {}: {error}",
                        write.witness.attempt.checkpoint_id
                    )));
                }
                Ok(Err(error)) => {
                    write.state = PendingSinkWitnessClearState::NeedsRetry;
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6050] sink-open witness cleanup task failed for checkpoint {}: {error}",
                        write.witness.attempt.checkpoint_id
                    )));
                }
                Err(_) => {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6050] sink-open witness cleanup timed out for checkpoint {}",
                        write.witness.attempt.checkpoint_id
                    )));
                }
            }
        }

        let witness = write.witness.clone();
        self.finalize_sink_open_witness_clear(&witness)?;
        pending.take();
        Ok(())
    }

    async fn clear_sink_open_witness_until(
        &self,
        witness: &laminar_core::checkpoint_decision::CheckpointSinkOpenWitness,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.settle_sink_open_witness_clear_until(Some(witness.clone()), deadline)
            .await
    }

    async fn quiesce_pending_sink_witness_clear_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.settle_sink_open_witness_clear_until(None, deadline)
            .await
    }

    /// Participant bound to the checkpoint-store namespace.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub(crate) fn participant_id(&self) -> u64 {
        self.store.participant_id()
    }

    #[cfg(feature = "cluster")]
    fn prepared_witness_from_local_manifest(
        &self,
        storage_id: u64,
        manifest: CheckpointManifest,
    ) -> Result<Option<PreparedCheckpointWitness>, DbError> {
        if manifest.checkpoint_id != storage_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] storage checkpoint {storage_id} contains manifest checkpoint {}",
                manifest.checkpoint_id
            )));
        }
        if manifest.durable_phase
            != laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase::Prepared
        {
            return Ok(None);
        }
        self.store
            .ensure_manifest_participant(&manifest)
            .map_err(DbError::from)?;
        let validation_errors = manifest.validate(self.store.key_group_count());
        if !validation_errors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] prepared checkpoint {storage_id} is invalid: {}",
                validation_errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        let expected_deployment_id = self.expected_deployment_id()?;
        if manifest.deployment_id != expected_deployment_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] prepared checkpoint {storage_id} belongs to deployment {}, runtime deployment is {expected_deployment_id}",
                manifest.deployment_id
            )));
        }
        let expected_pipeline_identity = self.expected_pipeline_identity();
        if manifest.pipeline_identity != expected_pipeline_identity {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] prepared checkpoint {storage_id} belongs to pipeline identity {}, runtime identity is {}",
                manifest.pipeline_identity.sha256, expected_pipeline_identity.sha256
            )));
        }

        PreparedCheckpointWitness::new(
            CheckpointAttempt::new(manifest.epoch, manifest.checkpoint_id),
            manifest.participant_id,
            manifest.deployment_id,
            manifest.pipeline_identity,
        )
        .map(Some)
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] prepared checkpoint {storage_id} cannot form canonical recovery evidence: {error}"
            ))
        })
    }

    /// Return every unresolved participant-local prepare that still needs a cluster outcome.
    ///
    /// The inventory is bounded recovery evidence. It never creates an outcome and fails closed
    /// on corrupt, foreign, or non-monotonic local manifest history.
    #[cfg(feature = "cluster")]
    pub(crate) async fn prepared_checkpoint_witnesses(
        &self,
    ) -> Result<Vec<PreparedCheckpointWitness>, DbError> {
        let participant_id = self.store.participant_id();
        let ids = self.store.list_ids().await.map_err(DbError::from)?;
        if ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DbError::Checkpoint(
                "[LDB-6041] participant-local checkpoint IDs are not strictly ascending".into(),
            ));
        }

        let (outcomes, boundary) = self.cluster_outcome_inventory().await?;
        let highest_terminal = outcomes.last().or(boundary.terminal_anchor.as_ref());
        if let Some(terminal) = highest_terminal {
            self.validate_cluster_outcome_provenance(terminal)?;
        }

        let mut witnesses = Vec::new();
        for storage_id in ids {
            let manifest = self
                .store
                .load_by_id(storage_id)
                .await
                .map_err(DbError::from)?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6041] listed participant-local checkpoint {storage_id} disappeared during prepared inventory"
                    ))
                })?;
            let Some(witness) = self.prepared_witness_from_local_manifest(storage_id, manifest)?
            else {
                continue;
            };

            let attempt = witness.attempt;
            if let Some(outcome) = outcomes
                .iter()
                .find(|outcome| outcome.epoch == attempt.epoch)
            {
                self.validate_prepared_outcome(outcome, attempt)?;
                continue;
            }
            if highest_terminal.is_some_and(|terminal| {
                terminal.epoch > attempt.epoch && terminal.checkpoint_id > attempt.checkpoint_id
            }) {
                continue;
            }

            witnesses.push(witness);
            if witnesses.len() > MAX_PREPARED_CHECKPOINT_WITNESSES {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6040] participant {participant_id} has more than {MAX_PREPARED_CHECKPOINT_WITNESSES} unresolved prepared checkpoints"
                )));
            }
        }
        witnesses.sort_unstable_by_key(|witness| {
            (
                witness.attempt.epoch,
                witness.attempt.checkpoint_id,
                witness.participant_id,
            )
        });
        self.validate_prepared_checkpoint_witnesses(&witnesses)?;
        Ok(witnesses)
    }

    /// Validate peer prepared evidence before any successor outcome is written.
    #[cfg(feature = "cluster")]
    pub(crate) fn validate_prepared_checkpoint_witnesses(
        &self,
        witnesses: &[PreparedCheckpointWitness],
    ) -> Result<(), DbError> {
        if witnesses.len() > MAX_PREPARED_CHECKPOINT_WITNESSES {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6040] prepared checkpoint inventory has {} witnesses; maximum is {MAX_PREPARED_CHECKPOINT_WITNESSES}",
                witnesses.len()
            )));
        }
        let expected_deployment_id = self.expected_deployment_id()?;
        let expected_pipeline_identity = self.expected_pipeline_identity();
        let mut attempts = Vec::with_capacity(witnesses.len());
        for witness in witnesses {
            if !witness.is_canonical() {
                return Err(DbError::Checkpoint(
                    "[LDB-6041] prepared checkpoint inventory contains a non-canonical witness"
                        .into(),
                ));
            }
            if witness.deployment_id != expected_deployment_id
                || witness.pipeline_identity != expected_pipeline_identity
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6043] prepared checkpoint witness for participant {} does not match the active deployment and pipeline",
                    witness.participant_id
                )));
            }
            attempts.push(witness.attempt);
        }
        attempts.sort_unstable_by_key(|attempt| (attempt.epoch, attempt.checkpoint_id));
        attempts.dedup();
        if attempts.windows(2).any(|pair| {
            pair[0].epoch >= pair[1].epoch || pair[0].checkpoint_id >= pair[1].checkpoint_id
        }) {
            return Err(DbError::Checkpoint(
                "[LDB-6041] prepared checkpoint witnesses are not monotonically compatible".into(),
            ));
        }
        Ok(())
    }

    /// Set the assignment generation forwarded to `write_partial` for the split-brain fence.
    pub fn set_assignment_version(&mut self, version: u64) {
        self.assignment_version = version;
    }

    pub(crate) fn set_assignment_scoped_sources(
        &mut self,
        sources: impl IntoIterator<Item = String>,
    ) {
        self.assignment_scoped_sources = sources.into_iter().collect();
    }

    /// Set the explicit local event-time state included in the checkpoint cut.
    pub fn set_local_watermark(&mut self, watermark: CheckpointWatermark) {
        self.local_watermark = watermark;
    }

    /// Stage per-vnode operator-state slices for the next checkpoint.
    ///
    /// Call once per checkpoint (even with an empty map) so prior epoch slices never leak.
    pub(crate) fn set_pending_vnode_states(&mut self, states: StagedVnodeStates) {
        self.pending_vnode_states = states;
    }

    /// Set the owned vnodes. Also the default gate set until `set_gate_vnode_set` is called.
    pub fn set_vnode_set(&mut self, vnodes: Vec<u32>) {
        if self.gate_vnode_set.is_empty() {
            self.gate_vnode_set.clone_from(&vnodes);
        }
        self.rotation_epoch_floor = self.allocator.peek_epoch();
        // Drop bases for shed vnodes; the new owner builds its own from a full upload.
        self.last_vnode_uploads.retain(|v, _| vnodes.contains(v));
        // Drop parent links for shed vnodes so a newly-acquired vnode has no stale parent — it must
        // re-base FULL before any delta chains to it.
        self.last_partial_attempt.retain(|v, _| vnodes.contains(v));
        self.last_partial_delta_depth
            .retain(|v, _| vnodes.contains(v));
        self.last_sealed_partial_attempt
            .retain(|v, _| vnodes.contains(v));
        self.last_sealed_delta_depth
            .retain(|v, _| vnodes.contains(v));
        self.last_sealed_upload_attempt
            .retain(|v, _| vnodes.contains(v));
        self.vnode_set = vnodes;
    }

    /// Set the vnodes the durability gate checks (the full registry in cluster mode).
    pub fn set_gate_vnode_set(&mut self, vnodes: Vec<u32>) {
        self.gate_vnode_set = vnodes;
    }

    /// Register a sink for checkpoint coordination.
    pub(crate) fn register_sink(
        &mut self,
        name: impl Into<String>,
        handle: crate::sink_task::SinkTaskHandle,
    ) {
        let name = name.into();
        if handle.checkpoint_committable() {
            // A newly attached exact external namespace must seed its cursor before another
            // checkpoint can be admitted. This is reset again on every leadership loss.
            self.coordinated_commit_lag_known
                .store(false, std::sync::atomic::Ordering::Release);
        }
        self.sinks.push(RegisteredSink { name, handle });
        self.cached_sorted_sink_names = None;
    }

    /// Drop every registered sink handle after durable sink-open ownership is settled.
    pub(crate) fn clear_sinks(&mut self) -> Result<(), DbError> {
        let pending_witness = self
            .pending_sink_witness_create
            .try_lock()
            .map_or(true, |pending| pending.is_some());
        let pending_witness_clear = self
            .pending_sink_witness_clear
            .try_lock()
            .map_or(true, |pending| pending.is_some());
        if pending_witness
            || pending_witness_clear
            || self.active_sink_witness.lock().is_some()
            || self.allocator.sink_epoch_reservation().is_some()
        {
            return Err(DbError::Checkpoint(
                "[LDB-6050] cannot clear sink handles while durable sink-open ownership remains unresolved"
                    .into(),
            ));
        }
        self.sinks.clear();
        self.cached_sorted_sink_names = None;
        self.coordinated_commit_lag
            .store(0, std::sync::atomic::Ordering::Release);
        self.coordinated_commit_lag_known
            .store(true, std::sync::atomic::Ordering::Release);
        self.coordinated_commit_progress.notify_one();
        Ok(())
    }

    /// Build the decoupled committer for coordinated-commit sinks, or `None`
    /// when there are none (or no state backend to read descriptors from).
    pub(crate) fn coordinated_committer(
        &self,
    ) -> Result<Option<crate::coordinated_committer::CoordinatedCommitter>, DbError> {
        let Some(backend) = self.state_backend.clone() else {
            return Ok(None);
        };
        let sinks: Vec<(String, crate::sink_task::SinkTaskHandle)> = self
            .sinks
            .iter()
            .filter(|s| s.handle.checkpoint_committable())
            .map(|s| (s.name.clone(), s.handle.clone()))
            .collect();
        if sinks.is_empty() {
            return Ok(None);
        }
        let deployment_id = self.expected_deployment_id()?.to_owned();
        let committer = crate::coordinated_committer::CoordinatedCommitter::new(
            backend,
            sinks,
            self.expected_pipeline_identity(),
            deployment_id,
            Arc::clone(&self.coordinated_commit_floor),
        )
        .with_metrics(self.prom.clone())
        .with_max_uncommitted_epochs(self.config.max_uncommitted_epochs)
        .with_lag_state(
            Arc::clone(&self.coordinated_commit_lag),
            Arc::clone(&self.coordinated_commit_lag_known),
            Arc::clone(&self.coordinated_commit_progress),
        )
        .with_storage_timeout(self.config.checkpoint_timeout)
        .with_decision_store(self.decision_store.clone());
        #[cfg(feature = "cluster")]
        let committer = committer.with_cluster_controller(self.cluster_controller.clone());
        Ok(Some(committer))
    }

    /// Poll interval for the decoupled committer task.
    pub(crate) const fn committer_poll_interval() -> Duration {
        COORDINATED_COMMITTER_POLL
    }

    /// Event-driven wakeup for the designated committer. The periodic poll remains a
    /// recovery/lost-wakeup safety net rather than the normal commit trigger.
    pub(crate) fn committer_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.coordinated_commit_notify)
    }

    pub(crate) fn coordinated_commit_admission(&self) -> Option<CoordinatedCommitAdmission> {
        self.sinks
            .iter()
            .any(|sink| sink.handle.checkpoint_committable())
            .then(|| CoordinatedCommitAdmission {
                pending: Arc::clone(&self.coordinated_commit_lag),
                known: Arc::clone(&self.coordinated_commit_lag_known),
                progress: Arc::clone(&self.coordinated_commit_progress),
                wake_committer: Arc::clone(&self.coordinated_commit_notify),
                cap: self.config.max_uncommitted_epochs,
            })
    }

    fn is_designated_commit_leader(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            self.cluster_controller
                .as_ref()
                .is_none_or(|controller| controller.is_leader())
        }
        #[cfg(not(feature = "cluster"))]
        {
            true
        }
    }

    /// Capture the exact durable leadership term at checkpoint entry.
    #[cfg(feature = "cluster")]
    fn capture_checkpoint_leadership(&self) -> Result<Option<LeaderProof>, String> {
        let Some(controller) = self.cluster_controller.as_ref() else {
            return Ok(None);
        };
        controller.capture_leader_proof().map(Some).ok_or_else(|| {
            "[LDB-6054] checkpoint rejected because no exact durable leader proof is live"
                .to_owned()
        })
    }

    /// Re-check that the exact durable leader term captured at entry remains live.
    #[cfg(feature = "cluster")]
    fn ensure_checkpoint_leadership(
        &self,
        captured: Option<&LeaderProof>,
        boundary: &str,
    ) -> Result<(), String> {
        let Some(controller) = self.cluster_controller.as_ref() else {
            return Ok(());
        };
        let Some(captured) = captured else {
            return Err(format!(
                "[LDB-6054] checkpoint leadership context disappeared before {boundary}"
            ));
        };
        if controller.proof_is_live(captured) {
            Ok(())
        } else {
            Err(format!(
                "[LDB-6054] exact leader proof is no longer live before {boundary}"
            ))
        }
    }

    /// Persist this participant's final prepare attestation.
    ///
    /// The payload also carries the exact source-offset handoff. Requiring its key in `_SEAL`
    /// proves both manifest completion and a complete vnode-owner offset inventory, including
    /// owners whose connectors have empty offsets.
    #[cfg(feature = "cluster")]
    async fn persist_participant_ready_until(
        &mut self,
        attempt: CheckpointAttempt,
        manifest: &CheckpointManifest,
        deadline: tokio::time::Instant,
        follower_prepare: bool,
    ) -> Result<(), DbError> {
        let backend = Arc::clone(self.state_backend.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] cluster checkpoint requires a state backend for participant \
                 readiness attestation"
                    .into(),
            )
        })?);
        let assignment_fence = self
            .active_assignment_fence
            .as_ref()
            .filter(|fence| {
                fence.is_canonical() && fence.assignment_version == self.assignment_version
            })
            .cloned()
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] cluster participant readiness requires the exact active assignment certificate"
                        .into(),
                )
            })?;
        let mut owned_vnodes = self.vnode_set.clone();
        owned_vnodes.sort_unstable();
        owned_vnodes.dedup();
        let source_offsets = manifest
            .source_offsets
            .iter()
            .map(|(source, checkpoint)| {
                (
                    source.clone(),
                    checkpoint
                        .offsets
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                )
            })
            .collect();
        let source_metadata = manifest
            .source_offsets
            .iter()
            .map(|(source, checkpoint)| {
                (
                    source.clone(),
                    checkpoint
                        .metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                )
            })
            .collect();
        let source_assignment_versions = manifest
            .source_offsets
            .iter()
            .filter_map(|(source, checkpoint)| {
                checkpoint
                    .source_assignment_version
                    .map(|version| (source.clone(), version))
            })
            .collect();
        let source_watermarks = manifest
            .source_watermarks
            .iter()
            .map(|(source, watermark)| (source.clone(), *watermark))
            .collect();
        let (manifest_sha256, portable_state_sha256) = manifest_digests(manifest)?;
        let ready = ParticipantReady {
            version: PARTICIPANT_READY_VERSION,
            attempt,
            participant_id: self.self_node_id(),
            assignment_fence: assignment_fence.clone(),
            deployment_id: manifest.deployment_id.clone(),
            pipeline_identity: manifest.pipeline_identity.clone(),
            owned_vnodes,
            source_offsets,
            source_metadata,
            source_assignment_versions,
            source_watermarks,
            local_watermark: self.local_watermark,
            manifest_sha256,
            portable_state_sha256,
        };
        if ready.participant_id != manifest.participant_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] participant readiness writer {} does not match manifest participant {}",
                ready.participant_id, manifest.participant_id
            )));
        }
        let bytes = laminar_core::checkpoint::canonical_json_bytes(&ready)
            .map(bytes::Bytes::from)
            .map_err(|error| {
                DbError::Checkpoint(format!("participant readiness encode: {error}"))
            })?;
        checked_participant_ready_total(0, bytes.len())?;
        if follower_prepare {
            self.participant_ready_write = Some(attempt);
        }
        tokio::time::timeout_at(
            deadline,
            self.write_commit_descriptor(
                backend.as_ref(),
                attempt,
                &participant_ready_key(ready.participant_id),
                bytes,
            ),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6046] participant readiness write for epoch {} checkpoint {} exceeded the \
                 checkpoint deadline",
                attempt.epoch, attempt.checkpoint_id
            ))
        })?
        .map_err(|error| {
            DbError::Checkpoint(format!("participant readiness write failed: {error}"))
        })?;
        if follower_prepare && self.participant_ready_write == Some(attempt) {
            self.participant_ready_write = None;
        }
        Ok(())
    }

    async fn write_commit_descriptor(
        &self,
        backend: &dyn StateBackend,
        attempt: CheckpointAttempt,
        key: &str,
        bytes: bytes::Bytes,
    ) -> Result<(), StateBackendError> {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            let fence = self.active_assignment_fence.as_ref().ok_or_else(|| {
                StateBackendError::Conflict {
                    resource: format!(
                        "state-v2/epoch={}/checkpoint={}/commit/{key}",
                        attempt.epoch, attempt.checkpoint_id
                    ),
                    message: "cluster descriptor write has no active assignment certificate".into(),
                }
            })?;
            let leader_proof =
                self.active_leader_proof
                    .as_ref()
                    .ok_or_else(|| StateBackendError::Conflict {
                        resource: format!(
                            "state-v2/epoch={}/checkpoint={}/commit/{key}",
                            attempt.epoch, attempt.checkpoint_id
                        ),
                        message: "cluster descriptor write has no active leader proof".into(),
                    })?;
            return backend
                .write_certified_commit_descriptor(
                    attempt,
                    key,
                    fence,
                    self.self_node_id(),
                    leader_proof,
                    bytes,
                )
                .await;
        }
        backend.write_commit_descriptor(attempt, key, bytes).await
    }

    /// The source-instance-namespaced offset map for the highest durable decision, unioned from
    /// that exact attempt's participant readiness attestations. A seal is only a prepared state
    /// cut; it is published before the irrevocable decision and therefore cannot independently
    /// advance input recovery. Binding both source handoff and vnode rehydration to the decision
    /// replays an abandoned `[decision, prepared-seal)` interval instead of skipping it.
    #[cfg(all(feature = "cluster", test))]
    pub(crate) async fn acquired_source_handoff(
        &self,
    ) -> Result<Option<AcquiredClusterHandoff>, DbError> {
        let Some(reader) = self.cluster_handoff_reader()? else {
            return Ok(None);
        };
        reader.acquired_source_handoff().await
    }

    /// Snapshot immutable recovery handles while the coordinator is at an epoch boundary. Remote
    /// reads through the returned value do not hold the coordinator mutex or delay checkpoint
    /// admission; assignment publication revalidates the namespace and durable decision.
    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_handoff_reader(&self) -> Result<Option<ClusterHandoffReader>, DbError> {
        let Some(controller) = self.cluster_controller.as_ref() else {
            return Ok(None);
        };
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6050] cluster recovery requires the exact checkpoint authority: {error}"
            ))
        })?;
        let backend = self.state_backend.clone().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] cluster source-offset handoff requires a state backend".into(),
            )
        })?;
        let decision_store = self.decision_store.clone().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] cluster recovery requires the durable decision store".into(),
            )
        })?;
        Ok(Some(ClusterHandoffReader {
            backend,
            authority,
            decision_store,
            pipeline_identity: self.expected_pipeline_identity(),
            deployment_id: self.deployment_id.clone(),
        }))
    }

    #[cfg(feature = "cluster")]
    async fn read_participant_ready(
        backend: &dyn StateBackend,
        attempt: CheckpointAttempt,
        sealed: &laminar_core::state::SealedCommitDescriptor,
    ) -> Result<bytes::Bytes, DbError> {
        let key = sealed.key.as_str();
        backend
            .read_sealed_commit_descriptor_bounded(attempt, sealed, MAX_PARTICIPANT_READY_BYTES)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "participant readiness read failed for '{key}': {error}"
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] sealed participant readiness marker '{key}' is missing"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    async fn read_readiness_inventory(
        backend: &dyn StateBackend,
        attempt: CheckpointAttempt,
        inventory: &CheckpointSealInventory,
    ) -> Result<Vec<(String, ParticipantReady)>, DbError> {
        if inventory.attempt != attempt {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] requested readiness attempt {attempt:?} does not match sealed inventory attempt {:?}",
                inventory.attempt
            )));
        }
        let ready_keys: Vec<String> = inventory
            .required_descriptors
            .iter()
            .filter(|key| participant_from_ready_key(key).is_some())
            .cloned()
            .collect();
        if ready_keys.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] checkpoint {} epoch {} seal has no participant readiness inventory",
                attempt.checkpoint_id, attempt.epoch
            )));
        }

        let reads = futures::stream::iter(ready_keys.into_iter().map(|key| async move {
            let key_participant = participant_from_ready_key(&key).ok_or_else(|| {
                DbError::Checkpoint(format!("invalid participant readiness key '{key}'"))
            })?;
            let descriptor = inventory.sealed_descriptor(&key).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] participant readiness marker '{key}' has no sealed provenance"
                ))
            })?;
            if descriptor
                .writer
                .as_ref()
                .map(|writer| writer.participant.node_id)
                != Some(key_participant)
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] participant readiness marker '{key}' was not written by participant {key_participant}"
                )));
            }
            let bytes = Self::read_participant_ready(backend, attempt, descriptor).await?;
            Ok::<_, DbError>((key, key_participant, bytes))
        }))
        .buffer_unordered(MAX_PARTICIPANT_READY_READ_CONCURRENCY);
        tokio::pin!(reads);
        let mut retained_bytes = 0;
        let mut records = Vec::new();
        while let Some(result) = reads.next().await {
            let (key, key_participant, bytes) = result?;
            retained_bytes = checked_participant_ready_total(retained_bytes, bytes.len())?;
            let marker = serde_json::from_slice::<ParticipantReady>(&bytes).map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] participant readiness marker '{key}' is corrupt: {error}"
                ))
            })?;
            if marker.participant_id != key_participant {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] participant readiness marker '{key}' names participant {}",
                    marker.participant_id
                )));
            }
            records.push((key, marker));
        }
        Ok(records)
    }

    #[cfg(feature = "cluster")]
    async fn create_cluster_recovery_capsule_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<Option<RecoveryCapsuleRef>, String> {
        if self.cluster_controller.is_none() {
            return Ok(None);
        }
        let backend = self.state_backend.as_ref().ok_or_else(|| {
            "[LDB-6050] cluster recovery capsule requires a state backend".to_owned()
        })?;
        let inventory = tokio::time::timeout_at(
            deadline,
            backend.checkpoint_seal_inventory(attempt),
        )
        .await
        .map_err(|_| {
            format!(
                "[LDB-6046] seal read for recovery capsule exceeded the checkpoint deadline for epoch {} checkpoint {}",
                attempt.epoch, attempt.checkpoint_id
            )
        })?
        .map_err(|error| format!("[LDB-6041] recovery capsule seal read failed: {error}"))?
        .ok_or_else(|| {
            format!(
                "[LDB-6041] checkpoint {} epoch {} has no exact state seal",
                attempt.checkpoint_id, attempt.epoch
            )
        })?;
        if inventory
            .descriptor_leader_proof()
            .map_err(|error| format!("[LDB-6041] invalid descriptor provenance: {error}"))?
            != self.active_leader_proof.as_ref()
        {
            return Err(format!(
                "[LDB-6041] checkpoint {} epoch {} descriptors do not bind the active leader term",
                attempt.checkpoint_id, attempt.epoch
            ));
        }
        let readiness = tokio::time::timeout_at(
            deadline,
            Self::read_readiness_inventory(backend.as_ref(), attempt, &inventory),
        )
        .await
        .map_err(|_| {
            format!(
                "[LDB-6046] readiness inventory for recovery capsule exceeded the checkpoint deadline for epoch {} checkpoint {}",
                attempt.epoch, attempt.checkpoint_id
            )
        })?
        .map_err(|error| error.to_string())?;
        let recovery_watermark_frontier = match self.cluster_watermark {
            CheckpointWatermark::Active(watermark) => Some(watermark),
            CheckpointWatermark::Idle => self
                .cluster_controller
                .as_ref()
                .and_then(|controller| controller.cluster_min_watermark()),
            CheckpointWatermark::Uninitialized => None,
        };
        let capsule = assemble_capsule(
            &inventory,
            readiness,
            self.expected_deployment_id()
                .map_err(|error| error.to_string())?,
            &self.expected_pipeline_identity(),
            self.cluster_watermark,
            recovery_watermark_frontier,
        )
        .map_err(|error| error.to_string())?;
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            "[LDB-6050] cluster recovery capsule requires the outcome store".to_owned()
        })?;
        let reference = tokio::time::timeout_at(
            deadline,
            decision_store.create_recovery_capsule(&capsule),
        )
        .await
        .map_err(|_| {
            format!(
                "[LDB-6046] recovery capsule persistence exceeded the checkpoint deadline for epoch {} checkpoint {}",
                attempt.epoch, attempt.checkpoint_id
            )
        })?
        .map_err(|error| format!("[LDB-6041] recovery capsule persistence failed: {error}"))?;
        Ok(Some(reference))
    }

    /// Begin the initial epoch on all exactly-once sinks.
    ///
    /// Must be called once after all sinks are registered and before any writes. Subsequent
    /// epochs are started automatically after each successful checkpoint commit.
    ///
    /// # Errors
    /// Returns the first sink error.
    pub async fn begin_initial_epoch(&self) -> Result<(), DbError> {
        let timeout = self.config.cleanup_timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        self.open_next_sink_epoch_until(deadline)
            .await
            .map(|_| ())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "initial sink epoch failed within its {timeout:?} cleanup deadline: {error}"
                ))
            })
    }

    /// Shared id allocator — the pipeline callback clones this to allocate without the mutex.
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    fn has_checkpoint_committable_sinks(&self) -> bool {
        self.sinks
            .iter()
            .any(|sink| sink.handle.checkpoint_committable())
    }

    async fn open_next_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<CheckpointAttempt>, DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(None);
        }

        let attempt = self.allocator.reserve_sink_epoch_until(deadline).await?;
        let witness = match self.create_sink_open_witness_until(attempt, deadline).await {
            Ok(witness) => witness,
            Err(error) => {
                self.allocator.mark_sink_epoch_in_doubt(attempt)?;
                return Err(error);
            }
        };
        if let Err(failure) = self
            .begin_epoch_for_sinks_until(attempt.epoch, deadline, deadline)
            .await
        {
            // A failed or timed-out rollback leaves the external transaction in-doubt. Keep the
            // reservation as a poison fence so this process cannot open a higher epoch over it.
            if failure.rollback_complete {
                if let Err(clear_error) =
                    self.clear_sink_open_witness_until(&witness, deadline).await
                {
                    self.allocator.mark_sink_epoch_in_doubt(attempt)?;
                    return Err(DbError::Checkpoint(format!(
                        "{}; connector rollback completed but durable sink-open cleanup failed: {clear_error}",
                        failure.error
                    )));
                }
            } else {
                self.allocator.mark_sink_epoch_in_doubt(attempt)?;
            }
            return Err(failure.error);
        }
        if let Err(error) = self.allocator.mark_sink_epoch_ready(attempt) {
            let rollback_error = self
                .rollback_sinks_until(attempt.epoch, deadline)
                .await
                .err();
            let cleanup_error = if rollback_error.is_none() {
                self.clear_sink_open_witness_until(&witness, deadline)
                    .await
                    .err()
            } else {
                None
            };
            if rollback_error.is_some() || cleanup_error.is_some() {
                self.allocator.mark_sink_epoch_in_doubt(attempt)?;
            }
            let rollback_detail = match (rollback_error, cleanup_error) {
                (None, None) => "completed".to_owned(),
                (Some(rollback), _) => {
                    format!("failed, leaving epoch state in-doubt: {rollback}")
                }
                (None, Some(cleanup)) => format!(
                    "completed, but durable witness cleanup failed and left epoch state in-doubt: {cleanup}"
                ),
            };
            return Err(DbError::Checkpoint(format!(
                "{error}; cleanup for opened sink epoch {} {rollback_detail}",
                attempt.epoch
            )));
        }
        Ok(Some(attempt))
    }

    /// Begin an epoch on all exactly-once sinks, rolling back already-started sinks on failure.
    async fn begin_epoch_for_sinks_until(
        &self,
        epoch: u64,
        begin_deadline: tokio::time::Instant,
        rollback_deadline: tokio::time::Instant,
    ) -> Result<(), SinkEpochOpenFailure> {
        let begins = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
            .map(|sink| {
                let name = sink.name.clone();
                let handle = sink.handle.clone();
                async move { (name, handle.begin_epoch_until(epoch, begin_deadline).await) }
            });
        let results = futures::future::join_all(begins).await;
        let mut begin_errors = Vec::new();
        for (name, result) in results {
            match result {
                Ok(()) => debug!(sink = %name, epoch, "began epoch"),
                Err(error) => begin_errors.push(format!("{name}: {error}")),
            }
        }
        if begin_errors.is_empty() {
            return Ok(());
        }

        // Every begin was invoked concurrently and is a remote mutation. Even an error may be an
        // acknowledgement loss after the transaction opened, so roll back every committable sink
        // concurrently under the caller-owned cleanup bound. Post-Commit continuation passes its
        // original absolute deadline here so a failed successor cannot extend that budget.
        let rollback_error = self
            .rollback_sinks_until(epoch, rollback_deadline)
            .await
            .err();
        let rollback_complete = rollback_error.is_none();
        let rollback_detail = rollback_error.map_or_else(String::new, |error| {
            format!(
                "; rollback failed for sink(s) that may have started, leaving epoch state in-doubt: {error}"
            )
        });
        Err(SinkEpochOpenFailure {
            error: DbError::Checkpoint(format!(
                "sink(s) failed to begin epoch {epoch}: {}{rollback_detail}",
                begin_errors.join("; ")
            )),
            rollback_complete,
        })
    }

    /// Wire Prometheus engine metrics.
    pub fn set_metrics(&mut self, prom: Arc<crate::engine_metrics::EngineMetrics>) {
        self.prom = Some(prom);
    }

    fn emit_checkpoint_metrics(
        &self,
        success: bool,
        checkpoint_id: u64,
        epoch: u64,
        duration: Duration,
    ) {
        if let Some(ref m) = self.prom {
            if success {
                m.checkpoints_completed.inc();
            } else {
                m.checkpoints_failed.inc();
                warn!(checkpoint_id, epoch, "checkpoint failure metric recorded");
            }
            #[allow(clippy::cast_possible_wrap)]
            m.checkpoint_epoch.set(epoch as i64);
            m.checkpoint_duration.observe(duration.as_secs_f64());
        }
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

    /// Run a full checkpoint cycle.
    ///
    /// Barrier propagation and operator snapshots (steps 1-2) are handled by the caller and
    /// passed in via `CheckpointRequest`.
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if any phase fails.
    pub async fn checkpoint(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        let attempt = self.allocate_attempt_until(deadline).await?;
        self.run_checkpoint_attempt(request, attempt, QuorumStage::RunInline, started)
            .await
    }

    #[cfg(feature = "cluster")]
    fn install_attempt_authority(&mut self, quorum: &QuorumStage) -> Result<(), String> {
        self.active_leader_proof = None;
        self.active_leader_proof = match quorum {
            QuorumStage::RunInline => self.capture_checkpoint_leadership()?,
            QuorumStage::Done { leader_proof, .. } => Some(leader_proof.clone()),
        };
        Ok(())
    }

    #[cfg(not(feature = "cluster"))]
    fn install_attempt_authority(&mut self, quorum: &QuorumStage) {
        self.active_leader_proof = None;
        let _ = quorum;
    }

    /// Apply one absolute deadline to the whole durable attempt. A decision write is treated as
    /// irrevocable from the instant it is issued because a timed-out create may still be visible.
    async fn run_checkpoint_attempt(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
    ) -> Result<CheckpointResult, DbError> {
        require_canonical_attempt(attempt, "checkpoint admission")?;
        if self.failure_recovery_required {
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id: attempt.checkpoint_id,
                epoch: attempt.epoch,
                duration: started.elapsed(),
                error: Some(
                    "[LDB-6048] a prior failed checkpoint has unresolved sink state; recovery is required before another attempt"
                        .into(),
                ),
                failure_disposition: Some(CheckpointFailureDisposition::RequiresRecovery),
            });
        }
        if let Some(pending) = self.pending_decision_write.as_ref() {
            let error = format!(
                "[LDB-6038] cannot admit checkpoint {} epoch {} while checkpoint {} epoch {} still owns an ambiguous decision write",
                attempt.checkpoint_id, attempt.epoch, pending.checkpoint_id, pending.epoch
            );
            return Ok(self.fail_after_irrevocable_work(
                attempt.checkpoint_id,
                attempt.epoch,
                started,
                error,
            ));
        }
        self.decision_write_started = false;
        self.active_assignment_fence
            .clone_from(&request.assignment_fence);
        #[cfg(feature = "cluster")]
        if let Err(error) = self.install_attempt_authority(&quorum) {
            return Ok(self.fail_after_irrevocable_work(
                attempt.checkpoint_id,
                attempt.epoch,
                started,
                error,
            ));
        }
        #[cfg(not(feature = "cluster"))]
        self.install_attempt_authority(&quorum);
        if let Err(error) = self.validate_attempt_request(&request) {
            return Ok(self
                .fail_epoch(
                    attempt.checkpoint_id,
                    attempt.epoch,
                    started,
                    error.to_string(),
                )
                .await);
        }
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(self
                .fail_epoch(
                    attempt.checkpoint_id,
                    attempt.epoch,
                    started,
                    format!(
                        "checkpoint attempt exceeded its {:?} end-to-end deadline during allocation",
                        self.config.checkpoint_timeout
                    ),
                )
                .await);
        }

        let commit_is_durable = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut work = Box::pin(self.checkpoint_inner(
            request,
            attempt,
            quorum,
            started,
            deadline,
            Arc::clone(&commit_is_durable),
        ));
        let result = tokio::select! {
            result = &mut work => Some(result),
            () = tokio::time::sleep_until(deadline) => {
                if commit_is_durable.load(std::sync::atomic::Ordering::Acquire) {
                    // Commit is irrevocable. Finish the separately bounded continuation instead
                    // of cancelling it at the reversible-attempt deadline.
                    Some(work.as_mut().await)
                } else {
                    None
                }
            }
        };
        let Some(result) = result else {
            // Release the mutable coordinator borrow before classifying the timeout.
            drop(work);
            let error = format!(
                "checkpoint {} epoch {} exceeded its {:?} end-to-end deadline during {}",
                attempt.checkpoint_id, attempt.epoch, self.config.checkpoint_timeout, self.phase
            );
            return Ok(self.resolve_attempt_timeout(attempt, started, error).await);
        };
        result
    }

    async fn resolve_attempt_timeout(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        error: String,
    ) -> CheckpointResult {
        if self.decision_write_started {
            return self.fail_after_irrevocable_work(
                attempt.checkpoint_id,
                attempt.epoch,
                started,
                error,
            );
        }
        // `Idle` only means the durable tail was not polled. A committable sink attempt has
        // already consumed its open epoch before entering this method and must still publish
        // Abort, roll back, and rotate its witness. Non-committable attempts have no mutation to
        // compensate before the first poll.
        if self.phase == CheckpointPhase::Idle
            && !self.has_checkpoint_committable_sinks()
            && !self.failure_recovery_required
        {
            self.pending_vnode_states.clear();
            self.pending_sink_descriptors.clear();
            return CheckpointResult {
                success: false,
                checkpoint_id: attempt.checkpoint_id,
                epoch: attempt.epoch,
                duration: started.elapsed(),
                error: Some(error),
                failure_disposition: Some(CheckpointFailureDisposition::Retryable),
            };
        }
        let error = if self.failure_recovery_required {
            format!("{error}; failed-epoch sink state remains in-doubt")
        } else {
            error
        };
        self.fail_epoch(attempt.checkpoint_id, attempt.epoch, started, error)
            .await
    }

    fn validate_attempt_request(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        self.validate_assignment_fence(request)?;
        #[cfg(not(feature = "cluster"))]
        Self::validate_assignment_fence(request)?;
        self.validate_source_assignment_cuts(request)
    }

    #[cfg(feature = "cluster")]
    fn validate_assignment_fence(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        match (
            self.cluster_controller.as_ref(),
            request.assignment_fence.as_ref(),
        ) {
            (None, None) => Ok(()),
            (None, Some(_)) => Err(DbError::Checkpoint(
                "[LDB-6055] local checkpoint received a cluster assignment certificate".into(),
            )),
            (Some(_), None) => Err(DbError::Checkpoint(
                "[LDB-6055] clustered checkpoint is missing its assignment certificate".into(),
            )),
            (Some(controller), Some(fence)) => {
                let canonical = fence.is_canonical() && fence.contains(controller.instance_id().0);
                if !canonical {
                    return Err(DbError::Checkpoint(
                        "[LDB-6055] clustered checkpoint has a non-canonical assignment certificate"
                            .into(),
                    ));
                }
                if fence.assignment_version != self.assignment_version {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6055] checkpoint captured assignment version {}, coordinator requires {}",
                        fence.assignment_version, self.assignment_version
                    )));
                }
                if controller
                    .checkpoint_assignment_fence(fence.assignment_version)
                    .as_ref()
                    != Some(fence)
                {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6055] checkpoint assignment {} participant certificate is stale",
                        fence.assignment_version
                    )));
                }
                Ok(())
            }
        }
    }

    #[cfg(not(feature = "cluster"))]
    fn validate_assignment_fence(request: &CheckpointRequest) -> Result<(), DbError> {
        if request.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(
                "[LDB-6055] local checkpoint received a cluster assignment certificate".into(),
            ));
        }
        Ok(())
    }

    fn validate_source_assignment_cuts(&self, request: &CheckpointRequest) -> Result<(), DbError> {
        if self.assignment_version == 0 {
            if !self.assignment_scoped_sources.is_empty() {
                return Err(DbError::Checkpoint(
                    "[LDB-6055] assignment-scoped sources are configured without an authoritative assignment version"
                        .into(),
                ));
            }
            if let Some((source, version)) =
                request
                    .source_offset_overrides
                    .iter()
                    .find_map(|(source, checkpoint)| {
                        checkpoint
                            .source_assignment_version
                            .map(|version| (source, version))
                    })
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6055] local source '{source}' checkpoint unexpectedly carries cluster assignment version {version}"
                )));
            }
            return Ok(());
        }
        let required_assignment = request
            .assignment_fence
            .as_ref()
            .map_or(self.assignment_version, |fence| fence.assignment_version);
        for source in &self.assignment_scoped_sources {
            let checkpoint = request.source_offset_overrides.get(source).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6055] cluster-assigned source '{source}' has no checkpoint at the admitted cut"
                ))
            })?;
            let captured = checkpoint.source_assignment_version.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6055] cluster-assigned source '{source}' checkpoint is missing its assignment version"
                ))
            })?;
            if captured.get() != required_assignment {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6055] source '{source}' captured assignment version {captured}, coordinator requires {required_assignment}"
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
                "[LDB-6055] non-assigned source '{source}' checkpoint unexpectedly carries assignment version {version}"
            )));
        }
        Ok(())
    }

    async fn pre_commit_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<std::collections::HashMap<String, Option<Vec<u8>>>, DbError> {
        let start = std::time::Instant::now();
        let result = self.pre_commit_sinks_inner(epoch, deadline).await;

        if let Some(ref m) = self.prom {
            m.sink_precommit_duration
                .observe(start.elapsed().as_secs_f64());
        }

        result
    }

    /// Flushes every sink so a checkpoint never records
    /// offsets past rows still buffered in an at-least-once sink (CP-5); exactly-once sinks
    /// additionally prepare their transaction, and coordinated-commit sinks return a descriptor.
    /// (`commit`/`rollback` remain exactly-once-only — an ALO sink is durable after its flush.)
    async fn pre_commit_sinks_inner(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<std::collections::HashMap<String, Option<Vec<u8>>>, DbError> {
        let futures: Vec<_> = self
            .sinks
            .iter()
            .map(|sink| {
                let handle = sink.handle.clone();
                let name = sink.name.clone();
                let checkpoint_committable = sink.handle.checkpoint_committable();
                async move {
                    if checkpoint_committable {
                        // 2PC phase 1: flush + prepare; coordinated sinks return a descriptor.
                        match handle.pre_commit_until(epoch, deadline).await {
                            Ok(descriptor) => {
                                debug!(sink = %name, epoch, "sink pre-committed");
                                // Every coordinated sink emits a prepared marker, even when it has
                                // no files for this cut. Empty markers are required to prove each
                                // quorum participant reached phase 1 and to advance external cursors.
                                Ok(Some((name, descriptor)))
                            }
                            Err(e) => Err(DbError::Checkpoint(format!(
                                "sink '{name}' pre-commit failed: {e}"
                            ))),
                        }
                    } else {
                        // At-least-once sinks flush buffered rows before the manifest seals offsets;
                        // they do not enter the transactional pre-commit path.
                        match handle.flush_until(deadline).await {
                            Ok(()) => {
                                debug!(sink = %name, epoch, "at-least-once sink flushed");
                                Ok(None)
                            }
                            Err(e) => Err(DbError::Checkpoint(format!(
                                "sink '{name}' flush failed: {e}"
                            ))),
                        }
                    }
                }
            })
            .collect();
        let collected =
            try_collect_bounded_draining(futures, MAX_SINK_PHASE_ONE_CONCURRENCY).await?;
        Ok(collected.into_iter().flatten().collect())
    }

    /// Save a manifest (and optional sidecar) to the store, bounded by the attempt timeout.
    ///
    /// Sidecar is written before the manifest: a failed sidecar write never leaves a
    /// manifest referencing missing state.
    async fn save_manifest_until(
        &self,
        manifest: Arc<CheckpointManifest>,
        state_data: Option<Vec<bytes::Bytes>>,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointManifest, DbError> {
        let fut = self.store.save_with_state(&manifest, state_data.as_deref());
        match tokio::time::timeout_at(deadline, fut).await {
            Ok(Ok(persisted)) => Ok(persisted),
            Ok(Err(e)) => Err(DbError::from(e)),
            Err(_elapsed) => Err(DbError::Checkpoint(format!(
                "[LDB-6011] manifest persist exhausted the checkpoint's {:?} end-to-end \
                 deadline — filesystem may be degraded",
                self.config.checkpoint_timeout
            ))),
        }
    }

    async fn finalize_manifest_until(
        &self,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointManifest, DbError> {
        tokio::time::timeout_at(deadline, self.store.finalize(checkpoint_id))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "manifest finalization for checkpoint {checkpoint_id} exhausted the \
                     checkpoint's {:?} end-to-end deadline",
                    self.config.checkpoint_timeout
                ))
            })?
            .map_err(DbError::from)
    }

    async fn finalize_manifest(&self, checkpoint_id: u64) -> Result<CheckpointManifest, DbError> {
        self.finalize_manifest_until(
            checkpoint_id,
            tokio::time::Instant::now() + self.config.checkpoint_timeout,
        )
        .await
    }

    /// This coordinator's node id for namespacing commit descriptors (0 without the cluster feature).
    fn self_node_id(&self) -> u64 {
        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.as_ref() {
            return cc.instance_id().0;
        }
        0
    }

    fn active_outcome_scope(&self) -> laminar_core::checkpoint_decision::CheckpointScope {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return laminar_core::checkpoint_decision::CheckpointScope::Cluster;
        }
        laminar_core::checkpoint_decision::CheckpointScope::Local
    }

    fn coordinated_namespaces(&self) -> Result<Vec<CoordinatedCommitNamespace>, DbError> {
        let identity = self.expected_pipeline_identity();
        let deployment_id = self.expected_deployment_id()?;
        let mut namespaces: Vec<_> = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.checkpoint_committable())
            .map(|sink| {
                CoordinatedCommitNamespace::try_new(
                    identity.clone(),
                    deployment_id,
                    sink.name.clone(),
                )
                .map_err(|error| DbError::Checkpoint(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        namespaces.sort_unstable_by(|left, right| left.sink_id.cmp(&right.sink_id));
        Ok(namespaces)
    }

    /// Every quorum participant must persist one marker for every coordinated sink, including
    /// an empty marker, plus one final readiness attestation in cluster mode. The exact key set
    /// is bound into `_SEAL`.
    fn required_descriptor_keys(
        &self,
        participants: &[QuorumPeer],
    ) -> Result<Vec<String>, DbError> {
        let participant_ids = self.checkpoint_participant_ids(participants);

        let namespaces = self.coordinated_namespaces()?;
        let mut keys = Vec::with_capacity(namespaces.len() * participant_ids.len());
        for namespace in &namespaces {
            for &participant_id in &participant_ids {
                keys.push(crate::coordinated_committer::descriptor_key(
                    namespace,
                    participant_id,
                ));
            }
        }
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            keys.extend(participant_ids.iter().copied().map(participant_ready_key));
        }
        keys.sort_unstable();
        Ok(keys)
    }

    fn checkpoint_participant_ids(&self, participants: &[QuorumPeer]) -> Vec<u64> {
        let mut participant_ids = vec![self.self_node_id()];
        #[cfg(feature = "cluster")]
        participant_ids.extend(participants.iter().map(|participant| participant.0));
        #[cfg(not(feature = "cluster"))]
        participant_ids.extend(participants.iter().copied());
        participant_ids.sort_unstable();
        participant_ids.dedup();
        participant_ids
    }

    /// Persist this participant's prepared marker for every coordinated sink.
    async fn take_and_persist_descriptors(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<(), DbError> {
        let descriptors = std::mem::take(&mut self.pending_sink_descriptors);
        let namespaces = self.coordinated_namespaces()?;
        if namespaces.is_empty() {
            if !descriptors.is_empty() {
                return Err(DbError::Checkpoint(
                    "prepared descriptors exist but no coordinated sink is registered".into(),
                ));
            }
            return Ok(());
        }
        if descriptors.len() != namespaces.len()
            || namespaces
                .iter()
                .any(|namespace| !descriptors.contains_key(&namespace.sink_id))
        {
            return Err(DbError::Checkpoint(
                "phase-1 did not produce exactly one prepared marker per coordinated sink".into(),
            ));
        }
        let Some(ref backend) = self.state_backend else {
            return Err(DbError::Checkpoint(
                "coordinated-commit sinks require a state backend for prepared markers".into(),
            ));
        };
        self.persist_sink_descriptors(backend, attempt, &namespaces, &descriptors)
            .await
    }

    async fn persist_sink_descriptors(
        &self,
        backend: &Arc<dyn StateBackend>,
        attempt: CheckpointAttempt,
        namespaces: &[CoordinatedCommitNamespace],
        descriptors: &std::collections::HashMap<String, Option<Vec<u8>>>,
    ) -> Result<(), DbError> {
        let participant_id = self.self_node_id();
        for namespace in namespaces {
            let payload = descriptors.get(&namespace.sink_id).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "missing prepared marker for sink '{}'",
                    namespace.sink_id
                ))
            })?;
            let marker = crate::coordinated_committer::encode_prepared_marker(
                namespace,
                attempt,
                participant_id,
                payload.as_deref(),
            )?;
            self.write_commit_descriptor(
                backend.as_ref(),
                attempt,
                &crate::coordinated_committer::descriptor_key(namespace, participant_id),
                bytes::Bytes::from(marker),
            )
            .await
            .map_err(|e| {
                DbError::Checkpoint(format!(
                    "[LDB-6024] commit descriptor write failed \
                         (sink={}, participant={participant_id}, epoch={}, checkpoint={}): {e}",
                    namespace.sink_id, attempt.epoch, attempt.checkpoint_id
                ))
            })?;
        }
        Ok(())
    }

    fn validated_delta_parent(
        &self,
        vnode: u32,
        epoch: u64,
    ) -> Result<(CheckpointAttempt, u32), DbError> {
        let parent = self
            .last_partial_attempt
            .get(&vnode)
            .copied()
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6025] delta partial for vnode {vnode} has no parent epoch \
                     (epoch={epoch}); a just-acquired vnode must re-base FULL first"
                ))
            })?;
        if self.last_sealed_partial_attempt.get(&vnode) != Some(&parent) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6025] delta partial for vnode {vnode} would reference unsealed attempt \
                 {parent:?} from epoch {epoch}; the next capture must re-base FULL"
            )));
        }
        if parent.epoch.checked_add(1) != Some(epoch) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6025] delta partial for vnode {vnode} at epoch {epoch} would cross a \
                 numeric epoch gap from sealed parent epoch {}; the next capture must re-base FULL",
                parent.epoch
            )));
        }
        let parent_depth = self
            .last_sealed_delta_depth
            .get(&vnode)
            .copied()
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6025] sealed delta parent metadata is missing for vnode {vnode} at \
                     epoch {epoch}; the next capture must re-base FULL"
                ))
            })?;
        let Some(bound) = self.delta_chain_bound else {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6025] delta partial admission is disabled for vnode {vnode} at epoch \
                 {epoch}; the capture must be FULL"
            )));
        };
        let depth = parent_depth.checked_add(1).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6025] delta depth overflow for vnode {vnode} at epoch {epoch}; the next \
                 capture must re-base FULL"
            ))
        })?;
        if depth > bound {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6025] delta partial for vnode {vnode} at epoch {epoch} would exceed the \
                 runtime-derived chain bound {bound}; the next capture must re-base FULL"
            )));
        }
        Ok((parent, depth))
    }

    fn validate_staged_delta_parents(&self, epoch: u64) -> Result<(), DbError> {
        for (&vnode, operators) in &self.pending_vnode_states {
            if operators
                .values()
                .any(|slice| matches!(slice, StagedSlice::Delta(_)))
            {
                self.validated_delta_parent(vnode, epoch)?;
            }
        }
        Ok(())
    }

    fn mark_vnode_partials_sealed(&mut self, attempt: CheckpointAttempt) {
        for &vnode in &self.vnode_set {
            if self.last_partial_attempt.get(&vnode) == Some(&attempt) {
                self.last_sealed_partial_attempt.insert(vnode, attempt);
                if let Some(depth) = self.last_partial_delta_depth.get(&vnode).copied() {
                    self.last_sealed_delta_depth.insert(vnode, depth);
                }
            }
            if self
                .last_vnode_uploads
                .get(&vnode)
                .is_some_and(|(base, _)| *base == attempt)
            {
                self.last_sealed_upload_attempt.insert(vnode, attempt);
            }
        }
    }

    /// Write each owned vnode's `partial.bin` to seal the durability gate.
    ///
    /// Unchanged vnodes emit a reference partial; changed vnodes do a full upload. References
    /// are forced back to full before their base ages out of the prune window. All writes run
    /// concurrently. Candidate bases advance only after every write lands and cannot be reused
    /// until the exact state seal records them as durable.
    async fn write_vnode_partials(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        tokio::time::timeout_at(
            deadline,
            self.write_vnode_partials_inner(epoch, checkpoint_id),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6024] vnode/descriptor persistence exhausted the checkpoint deadline \
                 (epoch={epoch}, checkpoint={checkpoint_id})"
            ))
        })?
    }

    async fn write_vnode_partials_inner(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<(), DbError> {
        let attempt = CheckpointAttempt::new(epoch, checkpoint_id);
        // Persist commit descriptors before partials so the partial gate
        // transitively implies their durability across all nodes.
        self.take_and_persist_descriptors(attempt).await?;
        let Some(ref backend) = self.state_backend else {
            return Ok(());
        };
        if self.vnode_set.is_empty() {
            return Ok(());
        }
        // Zero version = single-instance path; the fence is a no-op.
        let caller_version = self.assignment_version;
        let certified_writer = self
            .active_assignment_fence
            .as_ref()
            .map(|fence| (fence.clone(), self.self_node_id()));
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() && certified_writer.is_none() {
            return Err(DbError::Checkpoint(
                "[LDB-6050] cluster vnode partial write has no active assignment certificate"
                    .into(),
            ));
        }
        if certified_writer.as_ref().is_some_and(|(fence, node_id)| {
            fence.assignment_version != caller_version
                || !fence.is_canonical()
                || fence.participant_incarnation(*node_id).is_none()
        }) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] vnode partial writer {} is not certified by active assignment {}",
                self.self_node_id(),
                caller_version
            )));
        }
        let max_ref_age = (self.config.max_retained as u64).max(1);

        // Prepare each vnode as a delta, reusable reference, or full snapshot.
        let mut prepared = Vec::with_capacity(self.vnode_set.len());
        let mut remaining_bytes = self.config.max_staged_bytes;
        for &v in &self.vnode_set {
            let partial = self.prepare_vnode_partial(v, epoch, max_ref_age)?;
            let payload_bytes = u64::try_from(partial.payload.len())
                .map_err(|_| DbError::Checkpoint("vnode partial size does not fit u64".into()))?;
            remaining_bytes = remaining_bytes.checked_sub(payload_bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6014] vnode checkpoint payloads exceed the staged-state cap {} bytes",
                    self.config.max_staged_bytes
                ))
            })?;
            prepared.push(partial);
        }

        let writes: Vec<_> = prepared
            .iter()
            .map(|partial| {
                let backend = Arc::clone(backend);
                let v = partial.vnode;
                let payload = partial.payload.clone();
                let certified_writer = certified_writer.clone();
                async move {
                    let write = match certified_writer {
                        Some((fence, node_id)) => {
                            backend
                                .write_certified_partial(attempt, v, &fence, node_id, payload)
                                .await
                        }
                        None => {
                            backend
                                .write_partial(attempt, v, caller_version, payload)
                                .await
                        }
                    };
                    write.map_err(|e| {
                        DbError::Checkpoint(format!(
                            "[LDB-6024] vnode partial write failed (vnode={v}, epoch={epoch}): {e}"
                        ))
                    })
                }
            })
            .collect();
        try_collect_bounded_draining(writes, MAX_VNODE_PARTIAL_WRITE_CONCURRENCY).await?;

        // Record the parent link only after every write lands, so a partially failed epoch is not
        // chained from on the next attempt.
        let mut reference_count = 0_u64;
        for partial in prepared {
            self.last_partial_attempt.insert(partial.vnode, attempt);
            self.last_partial_delta_depth
                .insert(partial.vnode, partial.delta_depth);
            match partial.upload_update {
                VnodeUploadUpdate::Retain => {}
                VnodeUploadUpdate::Replace(ops) => {
                    self.last_vnode_uploads
                        .insert(partial.vnode, (attempt, ops));
                }
                VnodeUploadUpdate::Remove => {
                    self.last_vnode_uploads.remove(&partial.vnode);
                }
            }
            if partial.is_reference {
                reference_count += 1;
            }
        }
        if reference_count > 0 {
            if let Some(ref m) = self.prom {
                m.checkpoint_unchanged_vnodes.inc_by(reference_count);
            }
        }
        Ok(())
    }

    fn prepare_vnode_partial(
        &self,
        vnode: u32,
        epoch: u64,
        max_ref_age: u64,
    ) -> Result<PreparedVnodePartial, DbError> {
        let ops = self.pending_vnode_states.get(&vnode);
        let has_delta =
            ops.is_some_and(|ops| ops.values().any(|s| matches!(s, StagedSlice::Delta(_))));
        let (partial, upload_update, is_reference, delta_depth) = if has_delta {
            let (parent, delta_depth) = self.validated_delta_parent(vnode, epoch)?;
            let (partial, update) = Self::prepare_delta_vnode_partial(parent, ops);
            (partial, update, false, delta_depth)
        } else {
            let (partial, update, is_reference) =
                self.prepare_snapshot_vnode_partial(vnode, epoch, max_ref_age, ops)?;
            (partial, update, is_reference, 0)
        };

        Ok(PreparedVnodePartial {
            vnode,
            payload: bytes::Bytes::from(partial.encode()?),
            upload_update,
            is_reference,
            delta_depth,
        })
    }

    fn prepare_delta_vnode_partial(
        parent: CheckpointAttempt,
        ops: Option<&HashMap<String, StagedSlice>>,
    ) -> (crate::vnode_partial::VnodePartial, VnodeUploadUpdate) {
        let mut operators = Vec::new();
        let mut deltas = Vec::new();
        if let Some(ops) = ops {
            for (name, slice) in ops {
                match slice {
                    StagedSlice::Delta(changed) => {
                        deltas.push((name.clone(), changed.to_vec()));
                    }
                    StagedSlice::Bytes(bytes) => {
                        operators.push((name.clone(), bytes.to_vec()));
                    }
                }
            }
        }
        (
            crate::vnode_partial::VnodePartial {
                operators,
                base: Some(parent),
                deltas,
            },
            // A reference to this mixed partial would inherit its delta ancestry. Repeated
            // delta/reference alternation can multiply numeric depth beyond both independent
            // bounds, so the next snapshot path must establish a new FULL reference base.
            VnodeUploadUpdate::Remove,
        )
    }

    fn prepare_snapshot_vnode_partial(
        &self,
        vnode: u32,
        epoch: u64,
        max_ref_age: u64,
        ops: Option<&HashMap<String, StagedSlice>>,
    ) -> Result<(crate::vnode_partial::VnodePartial, VnodeUploadUpdate, bool), DbError> {
        let reusable_base = ops.filter(|ops| !ops.is_empty()).and_then(|ops| {
            self.last_vnode_uploads
                .get(&vnode)
                .filter(|(base, last)| {
                    self.last_sealed_upload_attempt.get(&vnode) == Some(base)
                        && epoch
                            .checked_sub(base.epoch)
                            .is_some_and(|age| age > 0 && age < max_ref_age)
                        && last.len() == ops.len()
                        && ops
                            .iter()
                            .all(|(name, slice)| last.get(name).is_some_and(|p| p.matches(slice)))
                })
                .map(|(base, _)| *base)
        });
        if let Some(base) = reusable_base {
            return Ok((
                crate::vnode_partial::VnodePartial {
                    operators: Vec::new(),
                    base: Some(base),
                    deltas: Vec::new(),
                },
                VnodeUploadUpdate::Retain,
                true,
            ));
        }

        let mut operators = Vec::new();
        let mut recorded = HashMap::new();
        if let Some(ops) = ops {
            for (name, slice) in ops {
                match slice {
                    StagedSlice::Bytes(bytes) => {
                        operators.push((name.clone(), bytes.to_vec()));
                        recorded.insert(name.clone(), UploadedSlice::Bytes(bytes.clone()));
                    }
                    StagedSlice::Delta(_) => {
                        return Err(DbError::Checkpoint(format!(
                            "delta slice for operator '{name}' vnode {vnode} reached snapshot resolution"
                        )));
                    }
                }
            }
        }
        let update = if recorded.is_empty() {
            VnodeUploadUpdate::Remove
        } else {
            VnodeUploadUpdate::Replace(recorded)
        };
        Ok((
            crate::vnode_partial::VnodePartial {
                operators,
                base: None,
                deltas: Vec::new(),
            },
            update,
            false,
        ))
    }

    fn validate_restorable_gate_certificate(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some()
            && assignment_fence.is_none_or(|fence| {
                fence.assignment_version != self.assignment_version || !fence.is_canonical()
            })
        {
            return Err(format!(
                "checkpoint assignment certificate is missing or invalid while epoch {} \
                 checkpoint {} is becoming restorable",
                attempt.epoch, attempt.checkpoint_id
            ));
        }
        #[cfg(not(feature = "cluster"))]
        let _ = assignment_fence;
        if attempt.epoch < self.rotation_epoch_floor {
            return Err(format!(
                "vnode assignment rotated after epoch {} captured \
                 (rotation floor {}); epoch cannot seal",
                attempt.epoch, self.rotation_epoch_floor
            ));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn validate_restorable_gate_cut(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        if let Some(controller) = self.cluster_controller.as_ref() {
            if controller
                .checkpoint_assignment_fence(self.assignment_version)
                .as_ref()
                != assignment_fence
            {
                return Err(format!(
                    "checkpoint assignment fence changed while epoch {} checkpoint {} was \
                     becoming restorable (assignment version {})",
                    attempt.epoch, attempt.checkpoint_id, self.assignment_version
                ));
            }
        }
        Ok(())
    }

    async fn seal_restorable_gate_once(
        &self,
        backend: &dyn StateBackend,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        required_descriptors: &[String],
        deadline: tokio::time::Instant,
        #[cfg(feature = "cluster")] watches: &mut RestorableGateWatches,
    ) -> Result<Result<bool, laminar_core::state::StateBackendError>, String> {
        let seal = backend.seal_checkpoint(
            attempt,
            assignment_fence,
            &self.gate_vnode_set,
            required_descriptors,
        );
        tokio::pin!(seal);
        #[cfg(feature = "cluster")]
        let seal_result = if let (Some(controller), Some(fence_rx), Some(members_rx)) = (
            self.cluster_controller.as_ref(),
            watches.assignment.as_mut(),
            watches.membership.as_mut(),
        ) {
            loop {
                tokio::select! {
                    result = tokio::time::timeout_at(deadline, &mut seal) => break result,
                    changed = fence_rx.changed() => {
                        if changed.is_err() {
                            return Err("checkpoint assignment fence watch closed while the durability gate was waiting".into());
                        }
                        self.validate_restorable_gate_cut(attempt, assignment_fence)?;
                    }
                    changed = members_rx.changed() => {
                        if changed.is_err() {
                            return Err("cluster membership watch closed while the durability gate was waiting".into());
                        }
                        if controller
                            .checkpoint_assignment_fence(self.assignment_version)
                            .as_ref()
                            != assignment_fence
                        {
                            return Err(format!(
                                "checkpoint assignment fence changed while epoch {} checkpoint {} was becoming restorable (assignment version {})",
                                attempt.epoch, attempt.checkpoint_id, self.assignment_version
                            ));
                        }
                    }
                }
            }
        } else {
            tokio::time::timeout_at(deadline, &mut seal).await
        };
        #[cfg(not(feature = "cluster"))]
        let seal_result = tokio::time::timeout_at(deadline, &mut seal).await;
        seal_result.map_err(|_| {
            "state durability gate exhausted the end-to-end checkpoint deadline while sealing exact attempt"
                .into()
        })
    }

    #[cfg(feature = "cluster")]
    fn restorable_gate_participant_failure(&self, participants: &[QuorumPeer]) -> Option<String> {
        if let Some(controller) = self.cluster_controller.as_ref() {
            if let Some(reason) =
                Self::unhealthy_participant(&controller.members_watch().borrow(), participants)
            {
                return Some(format!("durability gate fail-fast: {reason}"));
            }
            if let Some(participant) = participants
                .iter()
                .find(|participant| controller.is_unresponsive(**participant))
            {
                return Some(format!(
                    "durability gate fail-fast: follower {} missed a capture quorum",
                    participant.0
                ));
            }
        }
        None
    }

    #[cfg(not(feature = "cluster"))]
    const fn restorable_gate_participant_failure(_participants: &[QuorumPeer]) -> Option<String> {
        None
    }

    /// Poll until every vnode in `gate_vnode_set` has its partial for the exact attempt, or the
    /// gate timeout expires. Transient I/O errors retry; immutable conflicts abort.
    async fn await_restorable_gate(
        &self,
        attempt: CheckpointAttempt,
        participants: &[QuorumPeer],
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        use laminar_core::state::StateBackendError;
        // Back off exponentially from the configured initial to the cap. Clamp to a
        // 1ms floor (cap >= initial) so a 0ms config can't spin the gate under the mutex.
        let initial_poll = RESTORABLE_GATE_POLL_INITIAL;
        let max_poll = RESTORABLE_GATE_POLL_MAX.max(initial_poll);

        let Some(ref backend) = self.state_backend else {
            return Ok(());
        };
        // Every quorum participant/sink marker joins the same exact-attempt
        // `_SEAL`; the seal body binds this canonical inventory permanently.
        let required_descriptors = self
            .required_descriptor_keys(participants)
            .map_err(|error| error.to_string())?;
        if self.gate_vnode_set.is_empty() && required_descriptors.is_empty() {
            return Ok(());
        }

        let mut interval = initial_poll;
        let mut last_state = String::from("not all vnodes persisted");
        #[cfg(feature = "cluster")]
        let mut watches = RestorableGateWatches {
            assignment: self
                .cluster_controller
                .as_ref()
                .map(|controller| controller.checkpoint_assignment_watch()),
            membership: self
                .cluster_controller
                .as_ref()
                .map(|controller| controller.members_watch()),
        };
        self.validate_restorable_gate_certificate(attempt, assignment_fence)?;
        loop {
            #[cfg(feature = "cluster")]
            self.validate_restorable_gate_cut(attempt, assignment_fence)?;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "state durability gate exhausted the end-to-end checkpoint deadline: \
                     {last_state}"
                ));
            }
            let seal_result = self
                .seal_restorable_gate_once(
                    backend.as_ref(),
                    attempt,
                    assignment_fence,
                    &required_descriptors,
                    deadline,
                    #[cfg(feature = "cluster")]
                    &mut watches,
                )
                .await?;
            match seal_result {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(e @ StateBackendError::Conflict { .. }) => {
                    return Err(format!("state durability gate: {e}"));
                }
                Err(e) => {
                    debug!(epoch = attempt.epoch, checkpoint_id = attempt.checkpoint_id, error = %e, "durability gate poll error; retrying");
                    last_state = e.to_string();
                }
            }
            // Fail fast when a capture participant dies; doomed pipelined epochs each burn the
            // full timeout otherwise.
            #[cfg(feature = "cluster")]
            let participant_failure = self.restorable_gate_participant_failure(participants);
            #[cfg(not(feature = "cluster"))]
            let participant_failure = Self::restorable_gate_participant_failure(participants);
            if let Some(reason) = participant_failure {
                return Err(reason);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "state durability gate exhausted the end-to-end checkpoint deadline: \
                     {last_state}"
                ));
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(max_poll);
        }
    }

    /// Abandon a failed epoch only after its immutable Abort outcome is durable.
    async fn fail_epoch(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        started: Instant,
        reason: String,
    ) -> CheckpointResult {
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let attempt = CheckpointAttempt::new(epoch, checkpoint_id);
        let abort = self
            .record_terminal_outcome_until(
                attempt,
                laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                None,
                cleanup_deadline,
            )
            .await;
        let abort_outcome = match abort {
            Ok(
                laminar_core::checkpoint_decision::RecordOutcomeResult::Created(outcome)
                | laminar_core::checkpoint_decision::RecordOutcomeResult::Unchanged(outcome),
            ) if self.outcome_matches_active_authority(&outcome, attempt)
                && matches!(
                    outcome.verdict,
                    laminar_core::checkpoint_decision::CheckpointVerdict::Abort
                ) =>
            {
                outcome
            }
            Ok(laminar_core::checkpoint_decision::RecordOutcomeResult::Conflict { winner }) => {
                return self.fail_after_irrevocable_work(
                    checkpoint_id,
                    epoch,
                    started,
                    format!(
                        "checkpoint failure could not select Abort because epoch {epoch} already has {:?} for checkpoint {}",
                        winner.verdict, winner.checkpoint_id
                    ),
                );
            }
            Ok(_) => {
                return self.fail_after_irrevocable_work(
                    checkpoint_id,
                    epoch,
                    started,
                    "durable Abort outcome did not match the exact checkpoint authority".into(),
                );
            }
            Err(error) => {
                return self.fail_after_irrevocable_work(
                    checkpoint_id,
                    epoch,
                    started,
                    format!("checkpoint failure remains unresolved: {error}"),
                );
            }
        };
        self.failure_recovery_required = true;
        let mut result = self.record_failed_epoch(checkpoint_id, epoch, started, reason);
        // The terminal RPC is a wake-up hint only and is never sent ahead of durable authority.
        #[cfg(feature = "cluster")]
        if tokio::time::timeout_at(
            cleanup_deadline,
            self.announce_if_leader(
                epoch,
                checkpoint_id,
                laminar_core::cluster::control::Phase::Abort,
                abort_outcome.assignment_fence.as_ref(),
                abort_outcome.leader_proof.as_ref(),
            ),
        )
        .await
        .is_err()
        {
            error!(
                checkpoint_id,
                epoch,
                timeout = ?self.config.cleanup_timeout,
                "[LDB-6031] checkpoint Abort announcement exhausted the cleanup deadline",
            );
        }
        #[cfg(not(feature = "cluster"))]
        let _ = abort_outcome;
        match self
            .cleanup_failed_epoch_until(
                CheckpointAttempt::new(epoch, checkpoint_id),
                cleanup_deadline,
            )
            .await
        {
            Ok(()) => self.failure_recovery_required = false,
            Err(cleanup_error) => {
                error!(
                    checkpoint_id, epoch, error = %cleanup_error,
                    "[LDB-6004] checkpoint failure cleanup did not complete",
                );
                if let Some(error) = result.error.as_mut() {
                    *error = format!("{error}; cleanup incomplete: {cleanup_error}");
                }
                result.failure_disposition = Some(CheckpointFailureDisposition::RequiresRecovery);
            }
        }
        let mut poisoned_sinks = self
            .sinks
            .iter()
            .filter(|sink| sink.handle.epoch_requires_recovery())
            .map(|sink| sink.name.clone())
            .collect::<Vec<_>>();
        if !poisoned_sinks.is_empty() {
            poisoned_sinks.sort_unstable();
            self.failure_recovery_required = true;
            if let Some(error) = result.error.as_mut() {
                *error = format!(
                    "{error}; sink actor state requires replay recovery: {}",
                    poisoned_sinks.join(", ")
                );
            }
            result.failure_disposition = Some(CheckpointFailureDisposition::RequiresRecovery);
        }
        result
    }

    fn record_failed_epoch(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        started: Instant,
        error: String,
    ) -> CheckpointResult {
        self.checkpoints_failed += 1;
        self.phase = CheckpointPhase::Idle;
        self.decision_write_started = false;
        let duration = started.elapsed();
        self.emit_checkpoint_metrics(false, checkpoint_id, epoch, duration);
        // Once the attempt is terminal, staged data must not survive cancellation of the bounded
        // connector cleanup below.
        self.pending_vnode_states.clear();
        self.pending_sink_descriptors.clear();
        CheckpointResult {
            success: false,
            checkpoint_id,
            epoch,
            duration,
            error: Some(error),
            failure_disposition: Some(CheckpointFailureDisposition::Retryable),
        }
    }

    async fn cleanup_failed_epoch_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.rollback_sinks_until(attempt.epoch, deadline).await?;
        self.clear_owned_sink_witness_for_attempt_until(attempt, deadline)
            .await?;
        self.begin_next_epoch_until(deadline).await
    }

    async fn clear_owned_sink_witness_for_attempt_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        let witness = self.active_sink_witness.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6050] checkpoint {} has no owned durable sink-open witness",
                attempt.checkpoint_id
            ))
        })?;
        if witness.attempt != attempt {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] checkpoint {attempt:?} conflicts with active sink-open witness {:?}",
                witness.attempt
            )));
        }
        self.clear_sink_open_witness_until(&witness, deadline).await
    }

    /// Fail after the durable decision write has started.
    ///
    /// At this point rollback is not generally safe and a durable decision may already exist.
    /// Leave the prepared/finalized artifacts for recovery, return a terminal failure so the
    /// pipeline cannot acknowledge source offsets, and do not open a successor sink epoch.
    fn fail_after_irrevocable_work(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        started: Instant,
        error: String,
    ) -> CheckpointResult {
        self.checkpoints_failed += 1;
        self.phase = CheckpointPhase::Idle;
        self.decision_write_started = false;
        let duration = started.elapsed();
        self.emit_checkpoint_metrics(false, checkpoint_id, epoch, duration);
        self.pending_vnode_states.clear();
        self.pending_sink_descriptors.clear();
        CheckpointResult {
            success: false,
            checkpoint_id,
            epoch,
            duration,
            error: Some(error),
            failure_disposition: Some(CheckpointFailureDisposition::RequiresRecovery),
        }
    }

    async fn begin_next_epoch_until(&self, deadline: tokio::time::Instant) -> Result<(), DbError> {
        self.open_next_sink_epoch_until(deadline)
            .await
            .map(|_| ())
            .map_err(|error| {
                DbError::Checkpoint(format!("failed to begin successor sink epoch: {error}"))
            })
    }

    #[cfg(feature = "cluster")]
    async fn cluster_outcome_inventory(
        &self,
    ) -> Result<
        (
            Vec<laminar_core::checkpoint_decision::CheckpointOutcome>,
            laminar_core::cluster::control::ClusterOutcomeRetentionBoundary,
        ),
        DbError,
    > {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] cluster outcome inventory requires the cluster controller".into(),
            )
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6050] cluster outcome inventory requires the exact checkpoint authority: {error}"
            ))
        })?;
        tokio::time::timeout(self.config.checkpoint_timeout, async {
            let inventory = authority
                .cluster_outcome_inventory()
                .await
                .map_err(|error| DbError::Checkpoint(format!("[LDB-6040] {error}")))?;
            Ok((inventory.outcomes, inventory.retention_boundary))
        })
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6040] cluster outcome inventory timed out after {:?}",
                self.config.checkpoint_timeout
            ))
        })?
    }

    fn validate_cluster_outcome_provenance(
        &self,
        outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
    ) -> Result<(), DbError> {
        if outcome.deployment_id != self.expected_deployment_id()?
            || outcome.scope != self.active_outcome_scope()
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] cluster outcome for checkpoint {} epoch {} does not match the active outcome provenance",
                outcome.checkpoint_id, outcome.epoch
            )));
        }
        Ok(())
    }

    fn validate_prepared_outcome(
        &self,
        outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
        attempt: CheckpointAttempt,
    ) -> Result<(), DbError> {
        self.validate_cluster_outcome_provenance(outcome)?;
        if outcome.epoch != attempt.epoch || outcome.checkpoint_id != attempt.checkpoint_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] prepared checkpoint {} epoch {} does not match its durable outcome",
                attempt.checkpoint_id, attempt.epoch
            )));
        }
        Ok(())
    }

    async fn prepared_outcome(
        &self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<Option<laminar_core::checkpoint_decision::CheckpointOutcome>, DbError> {
        let attempt = CheckpointAttempt::new(epoch, checkpoint_id);
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            let (outcomes, boundary) = self.cluster_outcome_inventory().await?;
            if let Some(outcome) = outcomes.iter().find(|outcome| outcome.epoch == epoch) {
                self.validate_prepared_outcome(outcome, attempt)?;
                return Ok(Some(outcome.clone()));
            }

            let highest_terminal = outcomes.last().or(boundary.terminal_anchor.as_ref());
            if let Some(terminal) = highest_terminal {
                self.validate_cluster_outcome_provenance(terminal)?;
            }
            let strictly_dominated = highest_terminal.is_some_and(|terminal| {
                terminal.epoch > epoch && terminal.checkpoint_id > checkpoint_id
            });
            if strictly_dominated {
                warn!(
                    epoch,
                    checkpoint_id,
                    retention_floor = boundary.terminal_before_epoch,
                    highest_terminal_epoch = highest_terminal.map(|outcome| outcome.epoch),
                    highest_terminal_checkpoint_id =
                        highest_terminal.map(|outcome| outcome.checkpoint_id),
                    "prepared checkpoint is irreversibly dominated by cluster outcome continuity"
                );
                return Ok(None);
            }

            return Err(DbError::Checkpoint(format!(
                "[LDB-6040] prepared checkpoint {checkpoint_id} epoch {epoch} has no immutable terminal outcome; leaving it prepared"
            )));
        }

        let outcome = tokio::time::timeout(
            self.config.checkpoint_timeout,
            self.decision_store
                .as_ref()
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "[LDB-6050] prepared-checkpoint reconciliation requires the durable decision store"
                            .into(),
                    )
                })?
                .outcome(epoch),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6040] outcome read for prepared epoch {epoch} timed out after {:?}",
                self.config.checkpoint_timeout
            ))
        })?
        .map_err(|error| DbError::Checkpoint(format!("[LDB-6040] {error}")))?
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6040] prepared checkpoint {checkpoint_id} epoch {epoch} has no immutable terminal outcome; leaving it prepared"
            ))
        })?;
        self.validate_prepared_outcome(&outcome, attempt)?;
        Ok(Some(outcome))
    }

    async fn finish_prepared_reconciliation(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        outcome: laminar_core::checkpoint_decision::CheckpointOutcome,
    ) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        let is_leader = self
            .cluster_controller
            .as_ref()
            .is_some_and(|controller| controller.is_leader());

        match outcome.verdict {
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit => {
                #[cfg(feature = "cluster")]
                if self.cluster_controller.is_some() {
                    let fence = outcome.assignment_fence.as_ref().ok_or_else(|| {
                        DbError::Checkpoint(
                            "[LDB-6041] cluster Commit outcome has no assignment fence".into(),
                        )
                    })?;
                    if !fence.contains(self.store.participant_id()) {
                        // This local prepare landed after the leader sealed a smaller exact cut.
                        // It has no authority to publish and does not depend on the committed cut's
                        // recovery artifacts being locally readable in order to roll back.
                        self.rollback_sinks(epoch).await?;
                        return Ok(());
                    }
                    let backend = self.state_backend.as_ref().ok_or_else(|| {
                        DbError::Checkpoint(
                            "[LDB-6050] prepared cluster reconciliation requires the sealed state backend"
                                .into(),
                        )
                    })?;
                    let attempt = CheckpointAttempt::new(epoch, checkpoint_id);
                    let inventory = tokio::time::timeout(
                        self.config.checkpoint_timeout,
                        backend.checkpoint_seal_inventory(attempt),
                    )
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "[LDB-6040] exact seal read for prepared checkpoint {checkpoint_id} epoch {epoch} timed out"
                        ))
                    })?
                    .map_err(|error| DbError::Checkpoint(format!("[LDB-6041] {error}")))?
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "[LDB-6041] committed checkpoint {checkpoint_id} epoch {epoch} has no exact state seal"
                        ))
                    })?;
                    self.load_cluster_recovery_capsule(&outcome, &inventory)
                        .await?;
                }
                self.finalize_manifest(checkpoint_id).await?;
            }
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort => {
                self.rollback_sinks(epoch).await?;
            }
        }

        #[cfg(feature = "cluster")]
        if is_leader {
            let phase = if outcome.is_commit() {
                laminar_core::cluster::control::Phase::Commit
            } else {
                laminar_core::cluster::control::Phase::Abort
            };
            self.announce_if_leader(
                epoch,
                checkpoint_id,
                phase,
                outcome.assignment_fence.as_ref(),
                outcome.leader_proof.as_ref(),
            )
            .await;
        }
        Ok(())
    }

    /// Reconcile the highest prepared manifest on startup.
    ///
    /// The exact decision is the sole commit authority. External sink publication is re-driven
    /// by the coordinated committer from sealed participant descriptors; the checkpoint
    /// coordinator finalizes an exact Commit, rolls back an exact Abort, or rolls back a prepare
    /// that immutable outcome continuity proves can no longer receive a decision.
    pub async fn reconcile_prepared_on_init(&self) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            // `load_highest` deliberately skips corrupt recovery cuts so a valid older finalized
            // cut remains usable. Cluster Prepared evidence is different: silently skipping one
            // could abandon an external sink transaction. Audit the complete participant
            // inventory before selecting a local reconciliation candidate.
            self.prepared_checkpoint_witnesses().await?;
        }
        let Some(last) = load_highest(self.store.as_ref())
            .await
            .map_err(DbError::from)?
        else {
            return Ok(());
        };
        let expected_identity = self.expected_pipeline_identity();
        if last.pipeline_identity != expected_identity {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] prepared checkpoint {} belongs to pipeline identity {}, runtime \
                 identity is {}; explicit checkpoint reset or savepoint migration is required",
                last.checkpoint_id, last.pipeline_identity.sha256, expected_identity.sha256
            )));
        }
        let expected_deployment_id = self.expected_deployment_id()?;
        if last.deployment_id != expected_deployment_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6043] prepared checkpoint {} belongs to deployment {}, runtime deployment is {}; explicit checkpoint reset is required",
                last.checkpoint_id, last.deployment_id, expected_deployment_id
            )));
        }
        let expected_participant_id = self.store.participant_id();
        if last.participant_id != expected_participant_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] prepared checkpoint {} claims participant {}, checkpoint namespace belongs to participant {}",
                last.checkpoint_id, last.participant_id, expected_participant_id
            )));
        }
        let validation_errors = last.validate(self.store.key_group_count());
        if !validation_errors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] prepared checkpoint {} is invalid: {}",
                last.checkpoint_id,
                validation_errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        if last.durable_phase
            == laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase::Finalized
        {
            return Ok(());
        }

        let epoch = last.epoch;
        let checkpoint_id = last.checkpoint_id;

        let Some(outcome) = self.prepared_outcome(epoch, checkpoint_id).await? else {
            return self.rollback_sinks(epoch).await;
        };
        self.finish_prepared_reconciliation(epoch, checkpoint_id, outcome)
            .await
    }

    /// Settle any durable external sink epoch left open by a prior process before a successor can
    /// begin. This runs after checkpoint recovery and Prepared-manifest reconciliation, when an
    /// exact terminal decision (if any) is authoritative.
    pub(crate) async fn reconcile_sink_open_witness(&mut self) -> Result<(), DbError> {
        let deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        self.reconcile_sink_open_witness_until(deadline).await
    }

    pub(crate) async fn reconcile_sink_open_witness_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        self.quiesce_pending_sink_witness_create_until(deadline)
            .await?;
        self.quiesce_pending_sink_witness_clear_until(deadline)
            .await?;
        let store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] sink-open reconciliation requires a durable decision store".into(),
            )
        })?;
        let witness = tokio::time::timeout_at(deadline, store.sink_open_witness())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6050] sink-open witness inventory timed out during recovery".into(),
                )
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] sink-open witness inventory failed during recovery: {error}"
                ))
            })?;
        let Some(witness) = witness else {
            if self.active_sink_witness.lock().is_some() {
                return Err(DbError::Checkpoint(
                    "[LDB-6050] in-memory sink ownership exists without its durable witness".into(),
                ));
            }
            if let Some(reservation) = self.allocator.sink_epoch_reservation() {
                match reservation {
                    SinkEpochReservation::Opening(attempt) => {
                        // Connector begin cannot run until witness creation returns successfully.
                        // Confirmed durable absence therefore makes this pre-begin reservation safe
                        // to burn.
                        self.allocator.burn_sink_epoch_reservation(attempt)?;
                    }
                    SinkEpochReservation::Ready(attempt)
                    | SinkEpochReservation::InDoubt(attempt) => {
                        return Err(DbError::Checkpoint(format!(
                            "[LDB-6050] sink epoch {} is active without its durable witness",
                            attempt.checkpoint_id
                        )));
                    }
                }
            }
            return Ok(());
        };
        self.validate_sink_open_witness(&witness)?;
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return Err(DbError::Checkpoint(
                "[LDB-0013] cluster exactly-once sink recovery is unavailable until connector epoch operations are leader-term fenced"
                    .into(),
            ));
        }
        *self.active_sink_witness.lock() = Some(witness.clone());
        let attempt = witness.attempt;
        let outcome = tokio::time::timeout_at(deadline, store.outcome(attempt.epoch))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] terminal outcome lookup timed out for sink-open checkpoint {}",
                    attempt.checkpoint_id
                ))
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] terminal outcome lookup failed for sink-open checkpoint {}: {error}",
                    attempt.checkpoint_id
                ))
            })?;

        let rollback_required = if let Some(outcome) = outcome {
            if outcome.epoch != attempt.epoch
                || outcome.checkpoint_id != attempt.checkpoint_id
                || outcome.deployment_id != witness.deployment_id
                || outcome.scope != laminar_core::checkpoint_decision::CheckpointScope::Local
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6043] terminal outcome does not match sink-open checkpoint {}",
                    attempt.checkpoint_id
                )));
            }
            !outcome.is_commit()
        } else {
            let floor = tokio::time::timeout_at(deadline, store.outcome_gc_floor_horizon())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "[LDB-6050] outcome-retention lookup timed out during sink recovery".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6050] outcome-retention lookup failed during sink recovery: {error}"
                    ))
                })?;
            if attempt.epoch < floor {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6040] sink-open checkpoint {} is below retained outcome history at {floor}; refusing to guess rollback",
                    attempt.checkpoint_id
                )));
            }
            let highest = tokio::time::timeout_at(deadline, store.highest_terminal_outcome())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "[LDB-6050] terminal outcome inventory timed out during sink recovery"
                            .into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                    "[LDB-6050] terminal outcome inventory failed during sink recovery: {error}"
                ))
                })?;
            if highest.is_some_and(|terminal| terminal.epoch > attempt.epoch) {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] sink-open checkpoint {} was bypassed by a newer terminal outcome",
                    attempt.checkpoint_id
                )));
            }
            let result = self
                .record_terminal_outcome_until(
                    attempt,
                    laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
                    None,
                    deadline,
                )
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6050] could not durably resolve sink-open checkpoint {} before rollback: {error}",
                        attempt.checkpoint_id
                    ))
                })?;
            let winner = match result {
                laminar_core::checkpoint_decision::RecordOutcomeResult::Created(outcome)
                | laminar_core::checkpoint_decision::RecordOutcomeResult::Unchanged(outcome) => {
                    outcome
                }
                laminar_core::checkpoint_decision::RecordOutcomeResult::Conflict { winner } => {
                    winner
                }
            };
            if !self.outcome_matches_active_authority(&winner, attempt) {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6043] durable winner does not match sink-open checkpoint {}",
                    attempt.checkpoint_id
                )));
            }
            !winner.is_commit()
        };
        // Any decision issued above now has an exact, validated immutable winner.
        self.decision_write_started = false;
        if rollback_required {
            self.rollback_sinks_until(attempt.epoch, deadline).await?;
        }
        self.clear_sink_open_witness_until(&witness, deadline)
            .await?;
        Ok(())
    }

    /// No-op when not the leader. Errors are logged; worst case is a longer follower timeout.
    #[cfg(feature = "cluster")]
    async fn announce_if_leader(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        phase: laminar_core::cluster::control::Phase,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        leader_proof: Option<&LeaderProof>,
    ) {
        let Some(cc) = self.cluster_controller.as_ref() else {
            return;
        };
        if !cc.is_leader() {
            return;
        }
        let Some(assignment_fence) = assignment_fence else {
            error!(
                epoch,
                checkpoint_id,
                ?phase,
                "refusing to publish an uncertified cluster barrier announcement"
            );
            return;
        };
        if matches!(
            phase,
            laminar_core::cluster::control::Phase::Prepare
                | laminar_core::cluster::control::Phase::Aligned
        ) {
            let Some(proof) = leader_proof else {
                error!(
                    epoch,
                    checkpoint_id,
                    ?phase,
                    "refusing to publish a reversible barrier without an exact leader proof"
                );
                return;
            };
            if !cc.proof_is_live(proof) {
                error!(
                    epoch,
                    checkpoint_id,
                    ?phase,
                    "refusing to publish a reversible barrier from a stale leader term"
                );
                return;
            }
        }
        let ann = laminar_core::cluster::control::BarrierAnnouncement {
            epoch,
            checkpoint_id,
            assignment_fence: Some(assignment_fence.clone()),
            leader_proof: leader_proof.cloned(),
            phase,
            flags: 0,
        };
        if let Err(e) = cc.announce_barrier(&ann).await {
            warn!(
                epoch,
                checkpoint_id,
                ?phase,
                error = %e,
                "[LDB-6031] barrier announcement failed",
            );
        }
    }

    /// Announce PREPARE and wait for follower acks.
    ///
    /// On quorum, returns the capture-time follower set and retains the exact cluster watermark
    /// status for the recovery capsule. The caller records a durable Abort before publishing its
    /// terminal wake-up hint on failure.
    #[cfg(feature = "cluster")]
    async fn await_prepare_quorum(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: Option<&laminar_core::cluster::control::CheckpointAssignmentFence>,
        leader_proof: Option<&LeaderProof>,
    ) -> Result<Vec<laminar_core::cluster::discovery::NodeId>, String> {
        let Some(cc) = self.cluster_controller.clone() else {
            return Ok(Vec::new());
        };
        if !cc.is_leader() {
            return Ok(Vec::new());
        }
        let assignment_fence = assignment_fence.ok_or_else(|| {
            "[LDB-6055] clustered checkpoint is missing its assignment certificate".to_string()
        })?;
        let leader_proof = leader_proof.ok_or_else(|| {
            "[LDB-6054] clustered checkpoint is missing its exact leader proof".to_string()
        })?;
        match Self::run_prepare_quorum(
            &cc,
            self.config.quorum_timeout,
            PrepareQuorum::new(
                CheckpointAttempt::new(epoch, checkpoint_id),
                self.local_watermark,
                assignment_fence,
                leader_proof,
                true,
            ),
        )
        .await
        {
            Ok((merged, participants)) => {
                self.cluster_watermark = merged;
                Ok(participants)
            }
            Err(msg) => Err(msg),
        }
    }

    /// Returns a failure reason unless every participant remains Active or Draining. A draining
    /// owner remains responsible for the clean handoff checkpoint; every other transition
    /// invalidates the captured quorum immediately.
    #[cfg(feature = "cluster")]
    fn unhealthy_participant(
        members: &[laminar_core::cluster::discovery::NodeInfo],
        participants: &[QuorumPeer],
    ) -> Option<String> {
        use laminar_core::cluster::discovery::NodeState;
        for &id in participants {
            match members.iter().find(|m| m.id.0 == id.0) {
                Some(node) if !matches!(node.state, NodeState::Active | NodeState::Draining) => {
                    return Some(format!(
                        "Follower {} transitioned to unhealthy state {:?}",
                        id.0, node.state
                    ));
                }
                Some(_) => {}
                None => {
                    return Some(format!("Follower {} missing from cluster membership", id.0));
                }
            }
        }
        None
    }

    #[cfg(feature = "cluster")]
    fn validate_cluster_watermark_candidate(
        cc: &laminar_core::cluster::control::ClusterController,
        observed: CheckpointWatermark,
    ) -> Result<CheckpointWatermark, String> {
        observed
            .validate()
            .map_err(|error| format!("[LDB-6041] invalid checkpoint watermark: {error}"))?;
        match (cc.cluster_min_watermark(), observed) {
            (Some(current), CheckpointWatermark::Active(watermark)) if watermark < current => {
                return Err(format!(
                    "[LDB-6041] active cluster watermark {watermark} regresses the certified frontier {current}; source reactivation or handoff is unsafe"
                ));
            }
            (Some(current), CheckpointWatermark::Uninitialized) => {
                return Err(format!(
                    "[LDB-6041] uninitialized cluster watermark cannot replace certified frontier {current}"
                ));
            }
            _ => {}
        }
        Ok(observed)
    }

    #[cfg(feature = "cluster")]
    fn finish_prepare_quorum(
        cc: &laminar_core::cluster::control::ClusterController,
        followers: Vec<laminar_core::cluster::discovery::NodeId>,
        local_watermark: CheckpointWatermark,
        outcome: Result<laminar_core::cluster::control::QuorumOutcome, String>,
    ) -> Result<
        (
            CheckpointWatermark,
            Vec<laminar_core::cluster::discovery::NodeId>,
        ),
        String,
    > {
        use laminar_core::cluster::control::QuorumOutcome;

        match outcome {
            Ok(QuorumOutcome::Reached {
                follower_watermark,
                ref acks,
            }) => {
                cc.note_responsive(acks);
                let observed = local_watermark.cluster_min(follower_watermark);
                Ok((
                    Self::validate_cluster_watermark_candidate(cc, observed)?,
                    followers,
                ))
            }
            Ok(QuorumOutcome::TimedOut { missing, .. }) => {
                cc.note_unresponsive(&missing);
                Err(format!(
                    "quorum timeout: {} follower(s) did not ack",
                    missing.len()
                ))
            }
            Ok(QuorumOutcome::Failed { failures }) => {
                let first = failures
                    .first()
                    .map_or("unknown", |(_, message)| message.as_str());
                Err(format!(
                    "follower snapshot failed on {} peer(s): {first}",
                    failures.len()
                ))
            }
            Err(message) => Err(format!("fail-fast: {message}")),
        }
    }

    /// Run the capture-quorum stage outside the coordinator mutex so pipelined tails can
    /// reach `Aligned` while an earlier epoch's durable tail holds the lock.
    ///
    /// Announces `Prepare`, waits for live-follower acks, returns the merged cluster-min
    /// watermark. Caller announces `Aligned` on success or `Abort` on failure.
    #[cfg(feature = "cluster")]
    pub(crate) async fn run_prepare_quorum(
        cc: &Arc<laminar_core::cluster::control::ClusterController>,
        quorum_timeout: Duration,
        request: PrepareQuorum<'_>,
    ) -> Result<
        (
            CheckpointWatermark,
            Vec<laminar_core::cluster::discovery::NodeId>,
        ),
        String,
    > {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

        let PrepareQuorum {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            announce_prepare,
        } = request;
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);

        let prepare = BarrierAnnouncement {
            epoch,
            checkpoint_id,
            assignment_fence: Some(assignment_fence.clone()),
            leader_proof: Some(leader_proof.clone()),
            phase: Phase::Prepare,
            flags: 0,
        };
        if !cc.proof_is_live(leader_proof) {
            return Err("[LDB-6054] exact leader proof was stale before Prepare".into());
        }
        if announce_prepare {
            cc.announce_prepare_barrier(&prepare, quorum_timeout)
                .await
                .map_err(|error| {
                    format!("[LDB-6031] checkpoint Prepare publication failed: {error}")
                })?;
        }

        let mut followers: Vec<laminar_core::cluster::discovery::NodeId> = assignment_fence
            .participants
            .iter()
            .map(|participant| laminar_core::cluster::discovery::NodeId(participant.node_id))
            .collect();
        followers.retain(|id| *id != cc.instance_id());
        if followers.is_empty() {
            // Even an empty remote roster must close the exact Prepare quorum in the barrier
            // state machine before Aligned is admissible.
            let outcome = cc
                .wait_for_quorum(&prepare, &followers, quorum_timeout)
                .await;
            if !matches!(
                outcome,
                laminar_core::cluster::control::QuorumOutcome::Reached { ref acks, .. }
                    if acks.is_empty()
            ) {
                return Err(format!(
                    "leader-only Prepare failed to close its exact quorum: {outcome:?}"
                ));
            }
            if !cc.proof_is_live(leader_proof) {
                return Err(
                    "[LDB-6054] exact leader proof expired during leader-only Prepare".into(),
                );
            }
            return Ok((
                Self::validate_cluster_watermark_candidate(cc, local_watermark)?,
                Vec::new(),
            ));
        }

        let mut members_rx = cc.members_watch();
        let mut leader_grant_rx = cc
            .leader_grant_watch()
            .ok_or_else(|| "[LDB-6054] durable leader-grant watch is not installed".to_owned())?;
        let mut candidacy_rx = cc.leader_candidacy_watch();

        let quorum_fut = cc.wait_for_quorum(&prepare, &followers, quorum_timeout);
        let membership_fut = async {
            loop {
                if let Some(reason) = Self::unhealthy_participant(&members_rx.borrow(), &followers)
                {
                    return reason;
                }
                if members_rx.changed().await.is_err() {
                    // Watch closed (shutting down): park this arm so the quorum deadline decides.
                    futures::future::pending::<()>().await;
                }
            }
        };
        let leadership_fut = async {
            loop {
                if !cc.proof_is_live(leader_proof) {
                    return "[LDB-6054] exact leader proof expired while capture quorum was pending"
                        .to_owned();
                }
                tokio::select! {
                    changed = leader_grant_rx.changed() => {
                        if changed.is_err() {
                            return "[LDB-6054] durable leader-grant watch closed while capture quorum was pending".to_owned();
                        }
                    }
                    changed = candidacy_rx.changed() => {
                        if changed.is_err() {
                            return "[LDB-6054] leader candidacy watch closed while capture quorum was pending".to_owned();
                        }
                    }
                }
            }
        };

        let outcome = tokio::select! {
            o = quorum_fut => Ok(o),
            e = membership_fut => Err(e),
            e = leadership_fut => Err(e),
        };

        if !cc.proof_is_live(leader_proof) {
            return Err(
                "[LDB-6054] exact leader proof expired at capture quorum completion".into(),
            );
        }

        Self::finish_prepare_quorum(cc, followers, local_watermark, outcome)
    }

    /// Pack operator states into the manifest; large states go to a sidecar rather than
    /// base64 JSON. The returned chunks are handed to `save_with_state` without a full copy.
    fn pack_operator_states(
        manifest: &mut CheckpointManifest,
        operator_states: &HashMap<String, bytes::Bytes>,
        threshold: usize,
    ) -> Option<Vec<bytes::Bytes>> {
        let mut sidecar_chunks: Vec<bytes::Bytes> = Vec::new();
        let mut offset: u64 = 0;
        let mut states: Vec<_> = operator_states.iter().collect();
        states.sort_unstable_by_key(|(name, _)| *name);
        for (name, data) in states {
            let (op_ckpt, maybe_blob) =
                laminar_core::storage::checkpoint_manifest::OperatorCheckpoint::from_bytes_shared(
                    data.clone(),
                    threshold,
                    offset,
                );
            if let Some(blob) = maybe_blob {
                offset += blob.len() as u64;
                sidecar_chunks.push(blob);
            }
            manifest.operator_states.insert(name.clone(), op_ckpt);
        }

        if sidecar_chunks.is_empty() {
            None
        } else {
            Some(sidecar_chunks)
        }
    }

    /// Roll back all exactly-once sinks in parallel, bounded by the internal cleanup budget.
    async fn rollback_sinks(&self, epoch: u64) -> Result<(), DbError> {
        let deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        self.rollback_sinks_until(epoch, deadline).await
    }

    async fn rollback_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let futures = self
            .sinks
            .iter()
            .filter(|s| s.handle.checkpoint_committable())
            .map(|sink| {
                let handle = sink.handle.clone();
                let name = sink.name.clone();
                async move {
                    let result = handle.rollback_epoch_until(epoch, deadline).await;
                    (name, result)
                }
            });
        let results = futures::future::join_all(futures).await;

        let mut errors = Vec::new();
        for (name, result) in results {
            if let Err(e) = result {
                error!(sink = %name, epoch, error = %e, "[LDB-6016] sink rollback failed");
                errors.push(format!("sink '{name}': {e}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(DbError::Checkpoint(format!(
                "rollback failed: {}",
                errors.join("; ")
            )))
        }
    }

    /// Sorted sink names for the manifest; cached and rebuilt only when the sink list changes.
    fn sorted_sink_names(&mut self) -> Vec<String> {
        if self.cached_sorted_sink_names.is_none() {
            let mut names: Vec<String> = self.sinks.iter().map(|s| s.name.clone()).collect();
            names.sort();
            self.cached_sorted_sink_names = Some(names);
        }
        self.cached_sorted_sink_names.as_ref().unwrap().clone()
    }

    /// Current checkpoint phase.
    #[must_use]
    pub fn phase(&self) -> CheckpointPhase {
        self.phase
    }

    /// Active pre-opened sink epoch, or the next allocation floor.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.allocator.peek_epoch()
    }

    /// Checkpoint configuration.
    #[must_use]
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Checkpoint performance statistics.
    #[must_use]
    pub fn stats(&self) -> CheckpointStats {
        let (p50, p95, p99) = self.duration_histogram.percentiles();
        // Histogram is in microseconds; stats fields are milliseconds.
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

    /// The underlying checkpoint store.
    #[must_use]
    pub fn store(&self) -> &dyn CheckpointStore {
        &*self.store
    }

    /// Run a full checkpoint using pre-captured source offsets.
    ///
    /// Non-empty `source_offset_overrides` bypass the live snapshot call — required for
    /// barrier-aligned checkpoints where source positions must match operator state exactly.
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if any phase fails.
    pub async fn checkpoint_with_offsets(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        let attempt = self.allocate_attempt_until(deadline).await?;
        self.run_checkpoint_attempt(request, attempt, QuorumStage::RunInline, started)
            .await
    }

    /// Pipelined-barrier entry point whose exact attempt was pre-allocated and whose one deadline
    /// started at admission/capture rather than after the durable tail acquired the mutex.
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if any phase fails.
    pub(crate) async fn checkpoint_preallocated_started(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        started: Instant,
    ) -> Result<CheckpointResult, DbError> {
        self.run_checkpoint_attempt(request, attempt, quorum, started)
            .await
    }

    /// Abandon a pre-allocated attempt that failed before the coordinator tail completed.
    ///
    /// The coordinator creates the immutable Abort outcome before publishing its terminal hint,
    /// rolling back sinks, or beginning the next local epoch.
    ///
    /// # Errors
    ///
    /// Returns an error when rollback or successor-epoch setup cannot complete by `deadline`.
    pub(crate) async fn abandon_epoch_until(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        error: String,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointResult, DbError> {
        require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "checkpoint abandonment",
        )?;
        self.active_assignment_fence = assignment_fence;
        self.active_leader_proof = leader_proof;
        let started = Instant::now();
        let result = tokio::time::timeout_at(
            deadline,
            self.fail_epoch(checkpoint_id, epoch, started, error),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "checkpoint {checkpoint_id} epoch {epoch} abandonment exceeded its cleanup deadline"
            ))
        })?;
        if result.failure_disposition == Some(CheckpointFailureDisposition::RequiresRecovery) {
            return Err(DbError::Checkpoint(result.error.clone().unwrap_or_else(
                || "checkpoint abandonment requires recovery".into(),
            )));
        }
        Ok(result)
    }

    /// Follower checkpoint: ack the capture, run the durable prepare, then wait for the
    /// leader's commit/abort. Returns `Ok(true)` = committed, `Ok(false)` = aborted.
    ///
    /// The ack means "aligned + captured"; the leader verifies prepare completion through
    /// the restorable gate (the final readiness attestation proves the full prepare finished).
    ///
    /// # Errors
    /// Propagates sink pre-commit, manifest save, or marker-write failures.
    #[cfg(feature = "cluster")]
    async fn validate_follower_prepare_context(
        cc: &laminar_core::cluster::control::ClusterController,
        request: &CheckpointRequest,
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
    ) -> Result<
        (
            laminar_core::cluster::control::CheckpointAssignmentFence,
            LeaderProof,
        ),
        DbError,
    > {
        use laminar_core::cluster::control::Phase;

        require_canonical_attempt(
            CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id),
            "follower Prepare admission",
        )?;
        if announcement.phase != Phase::Prepare {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6055] follower checkpoint {} epoch {} did not originate from Prepare",
                announcement.checkpoint_id, announcement.epoch
            )));
        }
        let leader_proof = announcement.leader_proof.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6055] follower Prepare has no durable leader authority proof".into(),
            )
        })?;
        let fence = request.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6055] follower request has no assignment certificate".into())
        })?;
        let locally_valid = fence.is_canonical()
            && leader_proof.is_canonical()
            && announcement.assignment_fence.as_ref() == Some(fence)
            && fence.contains(cc.instance_id().0)
            && fence.participant_incarnation(leader_proof.owner.node_id)
                == Some(leader_proof.owner.boot_id);
        if !locally_valid
            || cc
                .checkpoint_assignment_fence_for_leader(fence.assignment_version, leader_proof)
                .await
                .as_ref()
                != Some(fence)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6055] follower checkpoint {} epoch {} does not match the current certified \
                 Prepare assignment",
                announcement.checkpoint_id, announcement.epoch
            )));
        }
        Ok((fence.clone(), leader_proof.clone()))
    }

    #[cfg(feature = "cluster")]
    pub async fn follower_checkpoint(
        &mut self,
        request: CheckpointRequest,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::cluster::control::BarrierAck;

        let Some(cc) = self.cluster_controller.clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6033] follower_checkpoint called without cluster controller".into(),
            ));
        };
        let (assignment_fence, _) =
            Self::validate_follower_prepare_context(&cc, &request, &ann).await?;
        let (epoch, checkpoint_id) = (ann.epoch, ann.checkpoint_id);
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;

        // State is captured; ack so the leader can release the pipeline.
        match tokio::time::timeout_at(
            deadline,
            cc.ack_barrier(&BarrierAck {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                assignment_digest: Some(assignment_fence.digest()),
                ok: true,
                error: None,
                watermark: self.local_watermark,
            }),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(
                epoch,
                checkpoint_id,
                %error,
                "capture barrier ack was not delivered; immutable outcome remains authoritative"
            ),
            Err(_) => warn!(
                epoch,
                checkpoint_id,
                "capture barrier ack exceeded the checkpoint deadline; immutable outcome remains authoritative"
            ),
        }

        let decision_timeout =
            decision_timeout.min(deadline.saturating_duration_since(tokio::time::Instant::now()));
        tokio::time::timeout_at(
            deadline,
            self.follower_checkpoint_acked(request, ann, decision_timeout, deadline),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6046] follower checkpoint {} epoch {} exceeded its {:?} end-to-end deadline",
                checkpoint_id, epoch, self.config.checkpoint_timeout
            ))
        })?
    }

    /// `follower_checkpoint` minus the capture ack: prepare, await the decision, commit/rollback.
    ///
    /// Pipelined tails call the three stages separately so the decision wait doesn't hold the
    /// mutex while the next epoch's uploads queue.
    ///
    /// # Errors
    /// Propagates sink pre-commit, manifest save, or marker-write failures.
    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_checkpoint_acked(
        &mut self,
        request: CheckpointRequest,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        decision_timeout: Duration,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        let Some(cc) = self.cluster_controller.clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6033] follower_checkpoint called without cluster controller".into(),
            ));
        };
        let (assignment_fence, leader_proof) =
            Self::validate_follower_prepare_context(&cc, &request, &ann).await?;
        let (epoch, checkpoint_id) = (ann.epoch, ann.checkpoint_id);
        self.follower_prepare_acked_until(request, leader_proof, epoch, checkpoint_id, deadline)
            .await?;
        let committed = Self::await_follower_decision(
            &cc,
            epoch,
            checkpoint_id,
            &assignment_fence,
            decision_timeout,
        )
        .await?;
        self.follower_finish(epoch, checkpoint_id, committed).await
    }

    /// Follower stage 1: durable prepare (pre-commit + manifest + partial uploads).
    ///
    /// On failure a best-effort `ok = false` ack overwrites the capture ack.
    ///
    /// # Errors
    /// Propagates sink pre-commit, manifest save, or marker-write failures.
    /// Sink commands inherit the capture-time attempt deadline.
    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_prepare_acked_until(
        &mut self,
        request: CheckpointRequest,
        leader_proof: LeaderProof,
        epoch: u64,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        use laminar_core::cluster::control::BarrierAck;
        require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower prepare",
        )?;
        if let Some(in_doubt) = self.participant_ready_write {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6048] follower cannot prepare epoch {epoch}, checkpoint {checkpoint_id} while participant readiness for epoch {}, checkpoint {} remains in-doubt",
                in_doubt.epoch, in_doubt.checkpoint_id
            )));
        }
        let assignment_fence = request.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6055] follower prepare has no assignment certificate".into())
        })?;
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6055] follower prepare has no cluster controller".into())
        })?;
        if controller
            .checkpoint_assignment_fence_for_leader(
                assignment_fence.assignment_version,
                &leader_proof,
            )
            .await
            .as_ref()
            != Some(assignment_fence)
        {
            return Err(DbError::Checkpoint(
                "[LDB-6055] follower prepare authority no longer matches its assignment certificate"
                    .into(),
            ));
        }
        self.active_leader_proof = Some(leader_proof);
        let assignment_digest = request
            .assignment_fence
            .as_ref()
            .map(laminar_core::cluster::control::CheckpointAssignmentFence::digest);

        // Monotonic: a late-finishing depth>1 tail must not walk ids back past a successor's.
        self.allocator.advance_epoch_to(epoch);

        if let Err(e) = self
            .follower_prepare(request, epoch, checkpoint_id, deadline)
            .await
        {
            let ack_error = if let Some(cc) = self.cluster_controller.clone() {
                cc.ack_barrier(&BarrierAck {
                    epoch,
                    checkpoint_id,
                    assignment_digest,
                    ok: false,
                    error: Some(e.to_string()),
                    watermark: self.local_watermark,
                })
                .await
                .err()
            } else {
                None
            };
            if self.participant_ready_write == Some(CheckpointAttempt::new(epoch, checkpoint_id)) {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6048] follower prepare failed after issuing its final readiness write for epoch {epoch}, checkpoint {checkpoint_id}: {e}; the write may be durable, so rollback and successor setup are forbidden until recovery observes the immutable outcome{}",
                    ack_error
                        .as_ref()
                        .map(|error| format!("; negative barrier ack failed: {error}"))
                        .unwrap_or_default()
                )));
            }
            if let Err(rollback_error) = self.rollback_sinks(epoch).await {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6048] follower prepare failed for epoch {epoch}, checkpoint {checkpoint_id}: {e}; sink rollback also failed and the prepared state is in-doubt: {rollback_error}{}",
                    ack_error
                        .as_ref()
                        .map(|error| format!("; negative barrier ack failed: {error}"))
                        .unwrap_or_default()
                )));
            }
            self.pending_sink_descriptors.clear();
            self.allocator.advance_past(
                epoch,
                "advancing past a failed follower prepare before successor setup",
            )?;
            let successor_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
            self.begin_next_epoch_until(successor_deadline)
                .await
                .map_err(|begin_error| {
                    DbError::Checkpoint(format!(
                        "[LDB-6048] follower prepare failed for epoch {epoch}, checkpoint {checkpoint_id}: {e}; rollback succeeded but successor epoch setup failed: {begin_error}"
                    ))
                })?;
            self.phase = CheckpointPhase::Idle;
            return Err(DbError::Checkpoint(format!(
                "[LDB-6048] follower prepare failed for epoch {epoch}, checkpoint {checkpoint_id}: {e}{}",
                ack_error
                    .as_ref()
                    .map(|error| format!("; negative barrier ack failed: {error}"))
                    .unwrap_or_default()
            )));
        }
        Ok(())
    }

    /// Follower stage 2: wait for the immutable outcome without holding the coordinator mutex.
    ///
    /// Commit/Abort announcements are only wake-up notifications. The create-once outcome is the
    /// sole authority for either verdict. Timeouts, a missing outcome, and read failures leave the
    /// participant in-doubt; a prepared participant never guesses.
    ///
    /// # Errors
    /// Returns an error when the outcome store is unavailable, contains a conflicting outcome,
    /// cannot be read, or remains unresolved at timeout.
    #[cfg(feature = "cluster")]
    pub(crate) async fn await_follower_decision(
        cc: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower decision wait",
        )?;
        let deadline = Instant::now() + decision_timeout;
        let participant_id = cc.instance_id().0;
        let authority = cc.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6045] cluster follower has no exact checkpoint authority: {error}"
            ))
        })?;
        if !assignment_fence.is_canonical() || !assignment_fence.contains(cc.instance_id().0) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6045] follower checkpoint {checkpoint_id} epoch {epoch} has an invalid \
                 assignment certificate"
            )));
        }
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Self::follower_decision_timeout(epoch, checkpoint_id));
            }

            // Poll the create-once outcome independently of best-effort control notification.
            // This also covers a leader that recorded the outcome and crashed before publishing
            // its terminal hint.
            match Self::read_follower_outcome(
                authority.as_ref(),
                participant_id,
                epoch,
                checkpoint_id,
                assignment_fence,
                deadline,
            )
            .await?
            {
                FollowerOutcomeMatch::Commit { frontier } => {
                    Self::install_follower_watermark(cc, frontier);
                    return Ok(true);
                }
                FollowerOutcomeMatch::Abort => return Ok(false),
                FollowerOutcomeMatch::Pending => {}
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // The bounded read above was the final authoritative observation.
                return Err(Self::follower_decision_timeout(epoch, checkpoint_id));
            }

            // Keep control observation and durable polling live together. Near the deadline use
            // half of the remaining budget so the control wait can never consume the time needed
            // for one final bounded marker read.
            let poll_wait = Self::follower_control_poll_wait(remaining);
            if poll_wait.is_zero() {
                tokio::task::yield_now().await;
                continue;
            }
            let hint_wait_started = Instant::now();
            let Some(_announcement) =
                Self::wait_for_follower_terminal_hint(cc, epoch, poll_wait).await?
            else {
                continue;
            };

            // A matching-epoch terminal hint only wakes an immediate durable read. Its verdict,
            // checkpoint ID, assignment, and leader proof do not authorize state transition: a
            // successor may legitimately abort under a new proof and assignment, and a stale or
            // forged opposite hint must be harmless.
            match Self::read_follower_outcome(
                authority.as_ref(),
                participant_id,
                epoch,
                checkpoint_id,
                assignment_fence,
                deadline,
            )
            .await?
            {
                FollowerOutcomeMatch::Commit { frontier } => {
                    Self::install_follower_watermark(cc, frontier);
                    return Ok(true);
                }
                FollowerOutcomeMatch::Abort => return Ok(false),
                FollowerOutcomeMatch::Pending => {
                    // `wait_for_barrier` may return a cached terminal hint immediately. Preserve
                    // the polling interval so an unresolved hint cannot hot-spin object storage.
                    let pace = poll_wait.saturating_sub(hint_wait_started.elapsed());
                    if !pace.is_zero() {
                        tokio::time::sleep(
                            pace.min(deadline.saturating_duration_since(Instant::now())),
                        )
                        .await;
                    }
                }
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn follower_decision_timeout(epoch: u64, checkpoint_id: u64) -> DbError {
        DbError::Checkpoint(format!(
            "[LDB-6046] follower decision timed out for epoch {epoch}, checkpoint \
             {checkpoint_id}; participant remains prepared"
        ))
    }

    #[cfg(feature = "cluster")]
    fn follower_control_poll_wait(remaining: Duration) -> Duration {
        if remaining > FOLLOWER_DECISION_POLL {
            FOLLOWER_DECISION_POLL
        } else {
            remaining / 2
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_follower_terminal_hint(
        cc: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
        poll_wait: Duration,
    ) -> Result<Option<laminar_core::cluster::control::BarrierAnnouncement>, DbError> {
        use laminar_core::cluster::control::Phase;

        match tokio::time::timeout(
            poll_wait,
            cc.wait_for_barrier(
                |announcement| {
                    announcement.epoch == epoch
                        && matches!(announcement.phase, Phase::Commit | Phase::Abort)
                },
                poll_wait,
            ),
        )
        .await
        {
            Ok(Ok(announcement)) => Ok(announcement),
            Ok(Err(error)) => Err(DbError::Checkpoint(format!(
                "[LDB-6046] follower control observation failed for epoch {epoch}; participant \
                 remains prepared: {error}"
            ))),
            Err(_) => Ok(None),
        }
    }

    #[cfg(feature = "cluster")]
    async fn read_follower_outcome(
        authority: &laminar_core::cluster::control::LeaderLeaseStore,
        participant_id: u64,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
        deadline: Instant,
    ) -> Result<FollowerOutcomeMatch, DbError> {
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower outcome read",
        )?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Self::follower_decision_timeout(epoch, checkpoint_id));
        }
        let settlement =
            tokio::time::timeout(remaining, authority.cluster_attempt_settlement(attempt))
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                "[LDB-6046] durable settlement read timed out for epoch {epoch}, checkpoint \
                     {checkpoint_id}; participant remains prepared"
            ))
                })?
                .map_err(|e| {
                    DbError::Checkpoint(format!(
                "[LDB-6045] failed to read durable settlement for epoch {epoch}; participant \
                     remains prepared: {e}"
            ))
                })?;
        let Some(settlement) = settlement else {
            return Ok(FollowerOutcomeMatch::Pending);
        };
        let settled = CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id);
        match settled.relation_to(attempt) {
            laminar_core::state::CheckpointAttemptRelation::Exact if settlement.is_commit() => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(Self::follower_decision_timeout(epoch, checkpoint_id));
                }
                let exact = tokio::time::timeout(
                    remaining,
                    authority.cluster_outcome_with_recovery_capsule(epoch),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "[LDB-6046] durable Commit capsule read timed out for epoch {epoch}, \
                         checkpoint {checkpoint_id}; participant remains prepared"
                    ))
                })?
                .map_err(|e| {
                    DbError::Checkpoint(format!(
                        "[LDB-6045] failed to read durable Commit capsule for epoch {epoch}; \
                         participant remains prepared: {e}"
                    ))
                })?;
                let Some((outcome, capsule)) = exact else {
                    return Ok(FollowerOutcomeMatch::Pending);
                };
                if outcome != settlement {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6045] exact durable Commit changed while follower epoch {epoch}, \
                         checkpoint {checkpoint_id} was validating its recovery capsule"
                    )));
                }
                Self::match_follower_outcome(
                    Some(&outcome),
                    capsule.as_ref(),
                    participant_id,
                    epoch,
                    checkpoint_id,
                    assignment_fence,
                )
            }
            laminar_core::state::CheckpointAttemptRelation::Exact => Self::match_follower_outcome(
                Some(&settlement),
                None,
                participant_id,
                epoch,
                checkpoint_id,
                assignment_fence,
            ),
            laminar_core::state::CheckpointAttemptRelation::Newer => {
                if settlement.scope != laminar_core::checkpoint_decision::CheckpointScope::Cluster {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6045] follower epoch {epoch}, checkpoint {checkpoint_id} observed \
                         non-cluster durable settlement epoch {}, checkpoint {}",
                        settlement.epoch, settlement.checkpoint_id
                    )));
                }
                Ok(FollowerOutcomeMatch::Abort)
            }
            laminar_core::state::CheckpointAttemptRelation::Older
            | laminar_core::state::CheckpointAttemptRelation::Conflict => {
                Err(DbError::Checkpoint(format!(
                    "[LDB-6045] durable settlement epoch {}, checkpoint {} does not close \
                     follower epoch {epoch}, checkpoint {checkpoint_id}",
                    settlement.epoch, settlement.checkpoint_id
                )))
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn match_follower_outcome(
        outcome: Option<&laminar_core::checkpoint_decision::CheckpointOutcome>,
        capsule: Option<&laminar_core::checkpoint::ClusterRecoveryCapsule>,
        participant_id: u64,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
    ) -> Result<FollowerOutcomeMatch, DbError> {
        use laminar_core::checkpoint_decision::{CheckpointScope, CheckpointVerdict};

        let Some(outcome) = outcome else {
            return Ok(FollowerOutcomeMatch::Pending);
        };
        if outcome.epoch != epoch || outcome.checkpoint_id != checkpoint_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6045] epoch {epoch} is durably resolved for checkpoint {}, not prepared \
                 checkpoint {checkpoint_id}",
                outcome.checkpoint_id
            )));
        }
        if outcome.scope != CheckpointScope::Cluster {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6045] cluster participant {participant_id} observed a local-scope durable \
                 outcome for epoch {epoch}, checkpoint {checkpoint_id}"
            )));
        }
        match &outcome.verdict {
            CheckpointVerdict::Commit => {
                let Some(outcome_fence) = outcome.assignment_fence.as_ref() else {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6045] cluster Commit outcome for epoch {epoch}, checkpoint \
                         {checkpoint_id} has no assignment certificate"
                    )));
                };
                if outcome_fence != assignment_fence {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6045] durable Commit outcome for epoch {epoch}, checkpoint \
                         {checkpoint_id} does not match prepared assignment {:?}",
                        assignment_fence.digest()
                    )));
                }
                if !outcome_fence.contains(participant_id) || outcome.recovery_capsule.is_none() {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6045] durable Commit outcome for epoch {epoch}, checkpoint \
                         {checkpoint_id} excludes follower {participant_id} or has no recovery capsule"
                    )));
                }
                let capsule = capsule.ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6045] durable Commit outcome for epoch {epoch}, checkpoint \
                         {checkpoint_id} has no validated recovery capsule"
                    ))
                })?;
                Ok(FollowerOutcomeMatch::Commit {
                    frontier: capsule.recovery_watermark_frontier,
                })
            }
            CheckpointVerdict::Abort => {
                // The store validates deployment identity and the successor's canonical
                // proof/fence. They intentionally need not equal the Prepare authority: a newly
                // elected leader may be the process that makes the epoch's immutable Abort win.
                Ok(FollowerOutcomeMatch::Abort)
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn install_follower_watermark(
        controller: &laminar_core::cluster::control::ClusterController,
        frontier: Option<i64>,
    ) {
        match (controller.cluster_min_watermark(), frontier) {
            (Some(current), Some(committed)) if current >= committed => {}
            (Some(_) | None, None) => {}
            (_, Some(committed)) => controller.publish_cluster_min_watermark(committed),
        }
    }

    /// Follower stage 3: act on the decision. Returns `true` on a clean commit.
    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_finish(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
    ) -> Result<bool, DbError> {
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower terminal application",
        )?;
        let clean = if committed {
            // A valid Commit can only be published after the leader created the exact state seal.
            // Record that durable fact before this follower admits a successor delta/reference.
            self.mark_vnode_partials_sealed(attempt);
            // Followers never publish external sink state. The exact decision makes their
            // prepared state recoverable; finalization merely publishes the local recovery cut.
            self.finalize_manifest(checkpoint_id).await.map_err(|e| {
                DbError::Checkpoint(format!(
                    "[LDB-6048] follower could not finalize decided epoch {epoch}, checkpoint \
                     {checkpoint_id}: {e}"
                ))
            })?;
            let horizon = self.record_committed_checkpoint(checkpoint_id)?;
            self.checkpoints_completed += 1;
            self.allocator
                .advance_past(epoch, "finalizing a follower commit")?;
            // The shared backend and decision namespace are leader-owned, but each follower owns
            // a participant-specific manifest namespace and must bound that local inventory too.
            if let Some(horizon) = horizon.filter(|horizon| *horizon > 0) {
                self.schedule_local_manifest_retention_prune(horizon, epoch);
            }
            true
        } else {
            self.rollback_sinks(epoch).await.map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6048] follower could not roll back aborted epoch {epoch}, checkpoint {checkpoint_id}; prepared sink state remains in-doubt: {error}"
                ))
            })?;
            self.pending_sink_descriptors.clear();
            self.checkpoints_failed += 1;
            self.allocator
                .advance_past(epoch, "finalizing a follower abort")?;
            false
        };
        let successor_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        self.begin_next_epoch_until(successor_deadline)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6048] follower could not open the successor after terminal epoch {epoch}, checkpoint {checkpoint_id}: {error}"
                ))
            })?;
        if self.participant_ready_write == Some(attempt) {
            self.participant_ready_write = None;
        }
        self.phase = CheckpointPhase::Idle;
        Ok(clean)
    }

    /// Pre-commit + save manifest + write vnode markers.
    #[cfg(feature = "cluster")]
    async fn follower_prepare(
        &mut self,
        request: CheckpointRequest,
        epoch: u64,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower durable prepare",
        )?;
        // Source cuts are captured before the deferred follower tail acquires the
        // coordinator mutex. An assignment adoption may win that interval, so
        // revalidate against the coordinator's now-current generation before any
        // sink pre-commit, manifest persistence, or participant-readiness write.
        #[cfg(feature = "cluster")]
        self.validate_assignment_fence(&request)?;
        #[cfg(not(feature = "cluster"))]
        Self::validate_assignment_fence(&request)?;
        self.active_assignment_fence
            .clone_from(&request.assignment_fence);
        self.validate_source_assignment_cuts(&request)?;
        // Reject unsafe ancestry before sink phase 1, manifest persistence, descriptors, or vnode
        // writes. The caller will force a FULL capture for the successor attempt.
        self.validate_staged_delta_parents(epoch)?;

        let CheckpointRequest {
            assignment_fence: _,
            operator_states,
            watermark,
            table_store_checkpoint_path,
            extra_table_offsets,
            source_watermarks,
            source_offset_overrides,
        } = request;

        self.phase = CheckpointPhase::PreCommitting;
        #[cfg(all(debug_assertions, feature = "cluster"))]
        checkpoint_kill_gate("follower").await;
        match self.pre_commit_sinks_until(epoch, deadline).await {
            Ok(descriptors) => self.pending_sink_descriptors = descriptors,
            Err(e) => {
                self.pending_vnode_states.clear();
                return Err(e);
            }
        }

        let mut manifest = CheckpointManifest::new_with_key_group_count(
            checkpoint_id,
            epoch,
            self.store.key_group_count(),
        );
        manifest.participant_id = self.store.participant_id();
        manifest.source_offsets = source_offset_overrides;
        manifest.table_offsets = extra_table_offsets;
        manifest.watermark = watermark;
        manifest.source_watermarks = source_watermarks;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.source_names = {
            let mut names: Vec<String> = manifest.source_offsets.keys().cloned().collect();
            names.sort();
            names
        };
        manifest.sink_names = self.sorted_sink_names();
        manifest.pipeline_identity = self.expected_pipeline_identity();
        manifest.deployment_id = self.expected_deployment_id()?.to_owned();
        let state_data =
            Self::pack_operator_states(&mut manifest, &operator_states, STATE_INLINE_THRESHOLD);

        self.phase = CheckpointPhase::Persisting;
        let manifest = match self
            .save_manifest_until(Arc::new(manifest), state_data, deadline)
            .await
        {
            Ok(manifest) => manifest,
            Err(error) => {
                self.pending_vnode_states.clear();
                return Err(error);
            }
        };
        self.phase = CheckpointPhase::PersistingVnodes;
        if let Err(e) = self
            .write_vnode_partials(epoch, checkpoint_id, deadline)
            .await
        {
            self.pending_vnode_states.clear();
            return Err(e);
        }
        if let Err(error) = self
            .persist_participant_ready_until(
                CheckpointAttempt::new(epoch, checkpoint_id),
                &manifest,
                deadline,
                true,
            )
            .await
        {
            self.pending_vnode_states.clear();
            return Err(error);
        }
        self.pending_vnode_states.clear();
        Ok(())
    }

    /// Shared checkpoint implementation for all entry points.
    #[allow(clippy::too_many_lines)]
    async fn checkpoint_inner(
        &mut self,
        request: CheckpointRequest,
        attempt: CheckpointAttempt,
        quorum: QuorumStage,
        start: Instant,
        attempt_deadline: tokio::time::Instant,
        commit_is_durable: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<CheckpointResult, DbError> {
        #[cfg(feature = "cluster")]
        let assignment_fence = request.assignment_fence.clone();
        #[cfg(not(feature = "cluster"))]
        let assignment_fence: Option<
            laminar_core::cluster::control::CheckpointAssignmentFence,
        > = None;
        let CheckpointRequest {
            assignment_fence: _,
            operator_states,
            watermark,
            table_store_checkpoint_path,
            extra_table_offsets,
            source_watermarks,
            source_offset_overrides,
        } = request;
        // Flink-style: ids are allocated up front; a failed epoch is abandoned, never retried.
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);

        // A cluster epoch high-watermark can jump after leadership churn. Never publish a
        // prepared manifest or state marker for a delta whose sealed parent is non-consecutive.
        if let Err(error) = self.validate_staged_delta_parents(epoch) {
            return Ok(self
                .fail_epoch(checkpoint_id, epoch, start, error.to_string())
                .await);
        }

        #[cfg(feature = "cluster")]
        let checkpoint_leadership = self.active_leader_proof.clone();
        #[cfg(feature = "cluster")]
        if let Err(error) = self.ensure_checkpoint_leadership(
            checkpoint_leadership.as_ref(),
            "checkpoint coordinator admission",
        ) {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        debug!(checkpoint_id, epoch, "starting checkpoint durable tail");
        self.phase = CheckpointPhase::Snapshotting;
        #[cfg(all(debug_assertions, feature = "cluster"))]
        checkpoint_kill_gate("leader").await;
        let source_offsets = source_offset_overrides;
        let table_offsets = extra_table_offsets;

        // Level 1: collect capture acks, announce `Aligned` (pipeline resume gate).
        // Pipelined tails run this pre-mutex and pass `Done`.
        #[cfg(feature = "cluster")]
        #[allow(unused_assignments)] // both match arms assign; init keeps non-cluster shape
        let mut quorum_participants: Vec<QuorumPeer> = Vec::new();
        #[cfg(feature = "cluster")]
        match quorum {
            QuorumStage::RunInline => {
                match self
                    .await_prepare_quorum(
                        epoch,
                        checkpoint_id,
                        assignment_fence.as_ref(),
                        checkpoint_leadership.as_ref(),
                    )
                    .await
                {
                    Ok(p) => quorum_participants = p,
                    Err(quorum_failure) => {
                        error!(checkpoint_id, epoch, error = %quorum_failure, "[LDB-6032] quorum miss");
                        return Ok(self
                            .fail_epoch(checkpoint_id, epoch, start, quorum_failure)
                            .await);
                    }
                }
                self.announce_if_leader(
                    epoch,
                    checkpoint_id,
                    laminar_core::cluster::control::Phase::Aligned,
                    assignment_fence.as_ref(),
                    checkpoint_leadership.as_ref(),
                )
                .await;
            }
            QuorumStage::Done {
                cluster_watermark,
                participants,
                leader_proof: _,
            } => {
                self.cluster_watermark = cluster_watermark;
                quorum_participants = participants;
            }
        }
        #[cfg(not(feature = "cluster"))]
        let _ = quorum;

        // Phase 1 creates prepared sink transactions. Once leadership loss is observed, this
        // attempt performs no more connector or durable mutations; recovery owns cleanup.
        #[cfg(feature = "cluster")]
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership.as_ref(), "sink phase 1")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        self.phase = CheckpointPhase::PreCommitting;
        match self.pre_commit_sinks_until(epoch, attempt_deadline).await {
            Ok(descriptors) => self.pending_sink_descriptors = descriptors,
            Err(e) => {
                error!(checkpoint_id, epoch, error = %e, "pre-commit failed");
                return Ok(self
                    .fail_epoch(
                        checkpoint_id,
                        epoch,
                        start,
                        format!("pre-commit failed: {e}"),
                    )
                    .await);
            }
        }

        // Phase 1 itself is asynchronous. Do not persist a prepared manifest for this leader if
        // the lease lapsed while connectors were flushing/preparing.
        #[cfg(feature = "cluster")]
        if let Err(error) = self
            .ensure_checkpoint_leadership(checkpoint_leadership.as_ref(), "manifest persistence")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let mut manifest = CheckpointManifest::new_with_key_group_count(
            checkpoint_id,
            epoch,
            self.store.key_group_count(),
        );
        manifest.participant_id = self.store.participant_id();
        manifest.source_offsets = source_offsets;
        manifest.table_offsets = table_offsets;
        manifest.watermark = watermark;
        // When empty, recovery falls back to manifest.watermark; do not fabricate per-source
        // values from the global watermark as that loses granularity.
        manifest.source_watermarks = source_watermarks;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.source_names = {
            let mut names: Vec<String> = manifest.source_offsets.keys().cloned().collect();
            names.sort();
            names
        };
        manifest.sink_names = self.sorted_sink_names();
        manifest.pipeline_identity = self.expected_pipeline_identity();
        manifest.deployment_id = self.expected_deployment_id()?.to_owned();

        let checkpoint_bytes = operator_states.values().fold(0u64, |total, state| {
            total.saturating_add(state.len() as u64)
        });
        let cap = self.config.max_staged_bytes;
        if checkpoint_bytes > cap {
            let message = format!(
                "[LDB-6014] checkpoint state size {checkpoint_bytes} bytes exceeds the shared \
                 staged-state cap {cap} bytes"
            );
            error!(checkpoint_id, epoch, checkpoint_bytes, cap, %message);
            return Ok(self.fail_epoch(checkpoint_id, epoch, start, message).await);
        }
        if checkpoint_bytes > cap.saturating_mul(4) / 5 {
            warn!(
                checkpoint_id,
                epoch,
                checkpoint_bytes,
                cap,
                "checkpoint state approaching staged-state cap (>80%)"
            );
        }

        let state_data =
            Self::pack_operator_states(&mut manifest, &operator_states, STATE_INLINE_THRESHOLD);
        let sidecar_bytes = state_data
            .as_ref()
            .map_or(0, |chunks| chunks.iter().map(bytes::Bytes::len).sum());
        if sidecar_bytes > 0 {
            debug!(
                checkpoint_id,
                sidecar_bytes, "writing operator state sidecar"
            );
        }

        self.phase = CheckpointPhase::Persisting;
        let persisted_manifest = match self
            .save_manifest_until(Arc::new(manifest), state_data, attempt_deadline)
            .await
        {
            Ok(persisted) => Arc::new(persisted),
            Err(e) => {
                error!(checkpoint_id, epoch, error = %e, "[LDB-6008] manifest persist failed");
                return Ok(self
                    .fail_epoch(
                        checkpoint_id,
                        epoch,
                        start,
                        format!("manifest persist failed: {e}"),
                    )
                    .await);
            }
        };

        if let Err(e) = self
            .write_vnode_partials(epoch, checkpoint_id, attempt_deadline)
            .await
        {
            error!(checkpoint_id, epoch, error = %e, "[LDB-6025] vnode partial write failed");
            return Ok(self
                .fail_epoch(
                    checkpoint_id,
                    epoch,
                    start,
                    format!("vnode partial write failed: {e}"),
                )
                .await);
        }

        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            if let Err(e) = self
                .persist_participant_ready_until(
                    CheckpointAttempt::new(epoch, checkpoint_id),
                    &persisted_manifest,
                    attempt_deadline,
                    false,
                )
                .await
            {
                error!(checkpoint_id, epoch, error = %e, "participant readiness write failed");
                return Ok(self
                    .fail_epoch(
                        checkpoint_id,
                        epoch,
                        start,
                        format!("participant readiness write failed: {e}"),
                    )
                    .await);
            }
        }
        #[cfg(not(feature = "cluster"))]
        drop(persisted_manifest);

        // Level 2 ("restorable"): all vnodes persisted before sinks commit.
        // Polls because followers upload asynchronously after their capture ack.
        #[cfg(not(feature = "cluster"))]
        let quorum_participants: Vec<QuorumPeer> = Vec::new();
        self.phase = CheckpointPhase::Sealing;
        let gate_start = Instant::now();
        let gate_result = self
            .await_restorable_gate(
                CheckpointAttempt::new(epoch, checkpoint_id),
                &quorum_participants,
                assignment_fence.as_ref(),
                attempt_deadline,
            )
            .await;
        if let Some(ref m) = self.prom {
            m.checkpoint_restorable_gate_wait
                .observe(gate_start.elapsed().as_secs_f64());
        }
        if let Err(gate_err) = gate_result {
            warn!(
                checkpoint_id,
                epoch,
                vnodes = self.gate_vnode_set.len(),
                error = %gate_err,
                "[LDB-6020] state durability gate failed — rolling back sinks",
            );
            return Ok(self.fail_epoch(checkpoint_id, epoch, start, gate_err).await);
        }
        // Only the exact immutable seal, never a completed callback or a successful upload, makes
        // this attempt eligible as a future delta/reference parent.
        self.mark_vnode_partials_sealed(CheckpointAttempt::new(epoch, checkpoint_id));

        // The durable decision is the sole commit point. External sinks publish later from the
        // exact sealed descriptor inventory; no connector phase-2 mutation occurs inline here.
        #[cfg(feature = "cluster")]
        if let Err(error) = self
            .ensure_checkpoint_leadership(checkpoint_leadership.as_ref(), "durable commit decision")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let is_decision_leader = self.is_designated_commit_leader();
        #[cfg(feature = "cluster")]
        let recovery_capsule = if is_decision_leader {
            match self
                .create_cluster_recovery_capsule_until(attempt, attempt_deadline)
                .await
            {
                Ok(reference) => reference,
                Err(error) => {
                    error!(checkpoint_id, epoch, %error, "recovery capsule creation failed");
                    return Ok(self.fail_epoch(checkpoint_id, epoch, start, error).await);
                }
            }
        } else {
            None
        };
        #[cfg(not(feature = "cluster"))]
        let recovery_capsule = None;
        if is_decision_leader {
            self.phase = CheckpointPhase::Deciding;
            let verdict = laminar_core::checkpoint_decision::CheckpointVerdict::Commit;
            let outcome_result = self
                .record_terminal_outcome_until(
                    attempt,
                    verdict,
                    recovery_capsule.clone(),
                    attempt_deadline,
                )
                .await;
            match outcome_result {
                Ok(
                    laminar_core::checkpoint_decision::RecordOutcomeResult::Created(outcome)
                    | laminar_core::checkpoint_decision::RecordOutcomeResult::Unchanged(outcome),
                ) if self.outcome_matches_active_authority(&outcome, attempt)
                    && outcome.verdict
                        == laminar_core::checkpoint_decision::CheckpointVerdict::Commit
                    && outcome.recovery_capsule == recovery_capsule => {}
                Ok(laminar_core::checkpoint_decision::RecordOutcomeResult::Conflict { winner })
                    if self.outcome_matches_active_authority(&winner, attempt)
                        && matches!(
                            winner.verdict,
                            laminar_core::checkpoint_decision::CheckpointVerdict::Abort
                        ) =>
                {
                    return Ok(self
                        .fail_epoch(
                            checkpoint_id,
                            epoch,
                            start,
                            "durable Abort outcome won before commit publication".into(),
                        )
                        .await);
                }
                Ok(laminar_core::checkpoint_decision::RecordOutcomeResult::Conflict { winner }) => {
                    return Ok(self.fail_after_irrevocable_work(
                        checkpoint_id,
                        epoch,
                        start,
                        format!(
                            "[LDB-6038] stale checkpoint task lost terminal outcome to checkpoint {} {:?}",
                            winner.checkpoint_id, winner.verdict
                        ),
                    ));
                }
                Ok(_) => {
                    return Ok(self.fail_after_irrevocable_work(
                        checkpoint_id,
                        epoch,
                        start,
                        "[LDB-6038] durable Commit outcome did not match the exact checkpoint authority"
                            .into(),
                    ));
                }
                Err(error) => {
                    error!(checkpoint_id, epoch, %error, "[LDB-6038] commit outcome write failed ambiguously");
                    return Ok(self.fail_after_irrevocable_work(
                        checkpoint_id,
                        epoch,
                        start,
                        format!("commit outcome write failed ambiguously: {error}"),
                    ));
                }
            }
            // Keep the write latch set until the exact immutable Commit has been validated. The
            // outer deadline can now distinguish reversible work from the committed continuation.
            commit_is_durable.store(true, std::sync::atomic::Ordering::Release);
            self.decision_write_started = false;
            #[cfg(feature = "cluster")]
            if let (Some(controller), CheckpointWatermark::Active(watermark)) =
                (self.cluster_controller.as_ref(), self.cluster_watermark)
            {
                // Aligned is reversible. Advance the replay/filter frontier only after the exact
                // Commit outcome is immutable, so an aborted tail cannot make recovery skip rows.
                controller.publish_cluster_min_watermark(watermark);
            }
            self.highest_decided = self.highest_decided.max(epoch);
        }

        let post_commit_deadline = if commit_is_durable.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::Instant::now() + self.config.cleanup_timeout
        } else {
            attempt_deadline
        };

        // The authority append fenced the outcome against the exact captured term. Re-check the
        // live term before local finalization so a demoted task cannot acknowledge sources.
        #[cfg(feature = "cluster")]
        if let Err(error) = self
            .ensure_checkpoint_leadership(checkpoint_leadership.as_ref(), "manifest finalization")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let manifest_finalized = match self
            .finalize_manifest_until(checkpoint_id, post_commit_deadline)
            .await
        {
            Ok(_) => true,
            Err(e) => {
                // The exact decision is irrevocable and the Prepared manifest already contains
                // the source/state cut. Recovery finalizes this exact pair; live rollback is
                // forbidden. Do not advance retention until that repair is durable.
                error!(
                    checkpoint_id,
                    epoch,
                    error = %e,
                    "[LDB-6047] commit decided but manifest finalization failed; recovery will repair"
                );
                false
            }
        };

        // Finalization can outlive a lease. A finalized artifact is safe for the next leader to
        // recover, but the stale task must still return failure so source offsets are not acked.
        #[cfg(feature = "cluster")]
        if let Err(error) = self
            .ensure_checkpoint_leadership(checkpoint_leadership.as_ref(), "checkpoint completion")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        #[cfg(feature = "cluster")]
        if tokio::time::timeout_at(
            post_commit_deadline,
            self.announce_if_leader(
                epoch,
                checkpoint_id,
                laminar_core::cluster::control::Phase::Commit,
                assignment_fence.as_ref(),
                checkpoint_leadership.as_ref(),
            ),
        )
        .await
        .is_err()
        {
            warn!(
                checkpoint_id,
                epoch,
                timeout = ?self.config.cleanup_timeout,
                "[LDB-6031] checkpoint Commit announcement exhausted the post-commit deadline",
            );
        }

        // `announce_if_leader` intentionally degrades to a no-op after demotion. Re-check here so
        // that observation cannot be followed by success accounting or source acknowledgement.
        #[cfg(feature = "cluster")]
        if let Err(error) = self.ensure_checkpoint_leadership(
            checkpoint_leadership.as_ref(),
            "post-decision maintenance",
        ) {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let retention_horizon = if manifest_finalized {
            match self.record_committed_checkpoint(checkpoint_id) {
                Ok(Some(horizon)) => horizon,
                Ok(None) => 0,
                Err(error) => {
                    return Ok(self.fail_after_irrevocable_work(
                        checkpoint_id,
                        epoch,
                        start,
                        error.to_string(),
                    ));
                }
            }
        } else {
            0
        };

        if let Some(ref m) = self.prom {
            #[allow(clippy::cast_possible_wrap)]
            m.checkpoint_size_bytes.set(checkpoint_bytes as i64);
        }

        // Prune old partials/markers outside the retention window. Leader-gated:
        // the state backend is shared in cluster mode, so the leader (which
        // advances the committer floor) owns GC; a follower's floor stays 0.
        if self.is_designated_commit_leader() {
            let mut horizon = retention_horizon;
            // Never prune descriptors the designated committer hasn't committed
            // yet — hold the horizon at the commit floor for coordinated sinks.
            if self.sinks.iter().any(|s| s.handle.checkpoint_committable()) {
                horizon = horizon.min(
                    self.coordinated_commit_floor
                        .load(std::sync::atomic::Ordering::Acquire),
                );
            }
            // Pin at the decided cut: a coordinated rewind targets the highest commit
            // marker and must find that epoch's partials and readiness descriptors intact even
            // when sink commits stall behind the retention window.
            if self.decision_store.is_some() {
                horizon = horizon.min(self.highest_decided);
            }
            if horizon > 0 {
                self.schedule_retention_prune(
                    self.state_backend.clone(),
                    self.decision_store.clone(),
                    horizon,
                    epoch,
                );
            }
        }

        #[cfg(feature = "cluster")]
        if let Err(error) = self
            .ensure_checkpoint_leadership(checkpoint_leadership.as_ref(), "successor sink epoch")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let continuation = async {
            self.clear_owned_sink_witness_for_attempt_until(attempt, post_commit_deadline)
                .await?;
            self.begin_next_epoch_until(post_commit_deadline).await
        }
        .await;
        let begin_epoch_error = match continuation {
            Ok(()) => None,
            Err(e) => {
                error!(
                    error = %e,
                    "[LDB-6015] failed to settle the committed sink epoch and begin its successor — pipeline must stop before further writes"
                );
                Some(format!(
                    "checkpoint {checkpoint_id} epoch {epoch} committed, but sink continuation \
                     failed before the successor opened: {e}"
                ))
            }
        };

        // `begin_epoch_for_sinks` is asynchronous; lease expiry while it runs must still turn the
        // completion into a failure before the caller can publish/ack the source cut.
        #[cfg(feature = "cluster")]
        if let Err(error) = self
            .ensure_checkpoint_leadership(checkpoint_leadership.as_ref(), "successful completion")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        self.phase = CheckpointPhase::Idle;
        self.decision_write_started = false;
        self.checkpoints_completed += 1;
        self.total_bytes_written += checkpoint_bytes;
        let duration = start.elapsed();
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(true, checkpoint_id, epoch, duration);

        // The state seal and exact durable decision are now both visible. Wake the one
        // designated external committer immediately; it re-validates both before committing.
        // Followers never drive external commits and therefore must not create wakeup traffic.
        if is_decision_leader
            && self
                .sinks
                .iter()
                .any(|sink| sink.handle.checkpoint_committable())
        {
            if self
                .coordinated_commit_lag_known
                .load(std::sync::atomic::Ordering::Acquire)
            {
                self.coordinated_commit_lag
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
            self.coordinated_commit_notify.notify_one();
        }

        info!(
            checkpoint_id,
            epoch,
            duration_ms = duration.as_millis(),
            "checkpoint completed"
        );

        // A `begin_epoch` failure for the next epoch does not retroactively fail this one. The
        // successful result carries a continuation error so downstream first acknowledges this
        // durable source cut and then terminally fences the pipeline before another write.
        self.pending_vnode_states.clear();
        Ok(CheckpointResult {
            success: true,
            checkpoint_id,
            epoch,
            duration,
            error: begin_epoch_error,
            failure_disposition: None,
        })
    }

    /// Recover from the latest stored checkpoint.
    ///
    /// Returns `Ok(None)` for a fresh start (no checkpoint found).
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if the store read fails.
    #[cfg(feature = "cluster")]
    fn validate_cluster_recovery_capsule(
        outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
        inventory: &CheckpointSealInventory,
        capsule: &ClusterRecoveryCapsule,
        expected_deployment: &str,
        expected_identity: &PipelineIdentity,
    ) -> Result<(), DbError> {
        if outcome.verdict != laminar_core::checkpoint_decision::CheckpointVerdict::Commit {
            return Err(DbError::Checkpoint(
                "[LDB-6041] cluster recovery cut is not a Commit outcome".into(),
            ));
        }
        capsule
            .validate()
            .map_err(|error| DbError::Checkpoint(format!("[LDB-6041] {error}")))?;
        let expected_attempt = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
        if inventory.attempt != expected_attempt || capsule.attempt != expected_attempt {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] decided attempt {expected_attempt:?} does not match its seal and recovery capsule"
            )));
        }
        if outcome.scope != laminar_core::checkpoint_decision::CheckpointScope::Cluster {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] cluster recovery observed a {:?} durable outcome",
                outcome.scope
            )));
        }
        let assignment_fence = outcome.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6041] cluster decision is missing its assignment certificate".into(),
            )
        })?;
        if inventory.assignment_fence.as_ref() != Some(assignment_fence) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] decided epoch {} checkpoint {} assignment certificate does not match \
                 the sealed certificate",
                outcome.epoch, outcome.checkpoint_id,
            )));
        }
        if inventory.descriptor_leader_proof().map_err(|error| {
            DbError::Checkpoint(format!("[LDB-6041] invalid descriptor provenance: {error}"))
        })? != outcome.leader_proof.as_ref()
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] decided epoch {} checkpoint {} leader proof does not match the sealed descriptors",
                outcome.epoch, outcome.checkpoint_id
            )));
        }
        if capsule.assignment_fence != *assignment_fence
            || capsule.deployment_id != outcome.deployment_id
            || capsule.deployment_id != expected_deployment
            || capsule.pipeline_identity != *expected_identity
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] recovery capsule for epoch {} checkpoint {} does not match its durable authority or runtime namespace",
                outcome.epoch, outcome.checkpoint_id
            )));
        }
        let seal_inventory_sha256 = canonical_json_sha256(inventory).map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] canonical seal inventory encode failed: {error}"
            ))
        })?;
        if capsule.seal_inventory_sha256 != seal_inventory_sha256 {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] recovery capsule for epoch {} checkpoint {} does not name the exact state seal",
                outcome.epoch, outcome.checkpoint_id
            )));
        }
        let sealed_participants: std::collections::BTreeSet<u64> = inventory
            .required_descriptors
            .iter()
            .filter_map(|key| participant_from_ready_key(key))
            .collect();
        let decided_participants: std::collections::BTreeSet<u64> =
            assignment_fence.participant_ids().into_iter().collect();
        let capsule_participants: std::collections::BTreeSet<u64> = capsule
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect();
        if sealed_participants != decided_participants
            || capsule_participants != decided_participants
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] decided participants {decided_participants:?} do not match the sealed readiness inventory and recovery capsule"
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn validate_cluster_cut_metadata(
        backend: &dyn StateBackend,
        outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
        capsule: &ClusterRecoveryCapsule,
        expected_deployment: &str,
        expected_identity: &PipelineIdentity,
    ) -> Result<(), DbError> {
        let attempt = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
        let inventory = backend
            .checkpoint_seal_inventory(attempt)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("checkpoint seal inventory read failed: {error}"))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] decided epoch {} checkpoint {} has no exact state seal",
                    outcome.epoch, outcome.checkpoint_id
                ))
            })?;
        Self::validate_cluster_recovery_capsule(
            outcome,
            &inventory,
            capsule,
            expected_deployment,
            expected_identity,
        )?;
        backend
            .verify_checkpoint_artifact_metadata(&inventory)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] sealed state artifact metadata verification failed: {error}"
                ))
            })?;
        let readiness = Self::read_readiness_inventory(backend, attempt, &inventory).await?;
        let reproduced = assemble_capsule(
            &inventory,
            readiness,
            expected_deployment,
            expected_identity,
            capsule.cluster_watermark,
            capsule.recovery_watermark_frontier,
        )?;
        if reproduced != *capsule {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] participant readiness inventory no longer reproduces the committed recovery capsule for epoch {} checkpoint {}",
                outcome.epoch, outcome.checkpoint_id
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn load_cluster_recovery_capsule(
        &self,
        outcome: &laminar_core::checkpoint_decision::CheckpointOutcome,
        inventory: &CheckpointSealInventory,
    ) -> Result<ClusterRecoveryCapsule, DbError> {
        let reference = outcome.recovery_capsule.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6041] cluster Commit for epoch {} checkpoint {} has no recovery capsule",
                outcome.epoch, outcome.checkpoint_id
            ))
        })?;
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6050] cluster recovery requires the outcome store".into())
        })?;
        let capsule = tokio::time::timeout(
            self.config.checkpoint_timeout,
            decision_store.load_recovery_capsule(reference),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6040] recovery capsule read for epoch {} checkpoint {} timed out",
                outcome.epoch, outcome.checkpoint_id
            ))
        })?
        .map_err(|error| DbError::Checkpoint(format!("[LDB-6041] {error}")))?;
        Self::validate_cluster_recovery_capsule(
            outcome,
            inventory,
            &capsule,
            self.expected_deployment_id()?,
            &self.expected_pipeline_identity(),
        )?;
        Ok(capsule)
    }

    #[cfg(feature = "cluster")]
    async fn validate_recovered_cluster_cut(
        &self,
        recovered: &mut crate::recovery_manager::RecoveredState,
    ) -> Result<(), DbError> {
        if self.active_outcome_scope()
            != laminar_core::checkpoint_decision::CheckpointScope::Cluster
        {
            return Ok(());
        }
        let outcome = recovered.outcome().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6041] cluster recovery selected epoch {} without an immutable checkpoint outcome",
                recovered.epoch()
            ))
        })?;
        let backend = self.state_backend.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] cluster decision recovery requires the sealed state backend".into(),
            )
        })?;
        let capsule = recovered.cluster_capsule().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "[LDB-6041] cluster recovery selected epoch {} checkpoint {} without its recovery capsule",
                outcome.epoch, outcome.checkpoint_id
            ))
        })?;
        Self::validate_cluster_cut_metadata(
            backend.as_ref(),
            outcome,
            capsule,
            self.expected_deployment_id()?,
            &self.expected_pipeline_identity(),
        )
        .await
    }

    #[cfg(feature = "cluster")]
    fn install_recovered_cluster_watermark(
        &mut self,
        recovered: &crate::recovery_manager::RecoveredState,
    ) -> Result<(), DbError> {
        let Some(capsule) = recovered.cluster_capsule() else {
            return Ok(());
        };
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] clustered recovery image has no cluster controller".into(),
            )
        })?;
        capsule.validate().map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] recovered cluster capsule is invalid: {error}"
            ))
        })?;
        let recovered_status = capsule.cluster_watermark;
        let recovered_frontier = capsule.recovery_watermark_frontier;
        match (controller.cluster_min_watermark(), recovered_frontier) {
            (Some(current), Some(recovered)) if current > recovered => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] controller watermark {current} is ahead of committed recovery frontier {recovered}"
                )));
            }
            (Some(current), None) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] controller watermark {current} is ahead of a committed {recovered_status:?} recovery frontier without a numeric value"
                )));
            }
            (_, Some(recovered)) => {
                controller.publish_cluster_min_watermark(recovered);
            }
            (None, None) => {}
        }
        self.cluster_watermark = recovered_status;
        self.local_watermark = recovered_status;
        Ok(())
    }

    pub async fn recover(
        &mut self,
    ) -> Result<Option<crate::recovery_manager::RecoveredState>, DbError> {
        use crate::recovery_manager::RecoveryManager;

        let identity = self.expected_pipeline_identity();
        let deployment_id = self.expected_deployment_id()?;
        let mgr = RecoveryManager::new(&*self.store)
            .with_pipeline_identity(&identity)
            .with_deployment_id(deployment_id)
            .with_outcome_scope(self.active_outcome_scope());
        let decision_store = self.decision_store.as_deref().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6050] recovery requires the outcome store".into())
        })?;
        #[cfg(feature = "cluster")]
        let mut result = if let Some(controller) = self.cluster_controller.as_ref() {
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] cluster recovery requires the exact checkpoint authority: {error}"
                ))
            })?;
            mgr.recover_cluster(authority.as_ref(), decision_store)
                .await?
        } else {
            mgr.recover(Some(decision_store)).await?
        };
        #[cfg(not(feature = "cluster"))]
        let mut result = mgr.recover(Some(decision_store)).await?;

        if let Some(ref mut recovered) = result {
            #[cfg(feature = "cluster")]
            {
                self.validate_recovered_cluster_cut(recovered).await?;
                self.install_recovered_cluster_watermark(recovered)?;
            }
            self.record_committed_checkpoint(recovered.manifest.checkpoint_id)?;
            let successor =
                checked_successor_epoch(recovered.epoch(), "advancing after checkpoint recovery")?;
            self.allocator.advance_epoch_to(successor);
            info!(
                epoch = self.allocator.peek_epoch(),
                "coordinator epoch set after recovery"
            );
        }
        if let Some(outcome) = self.highest_terminal_outcome().await? {
            let successor = checked_successor_epoch(
                outcome.epoch,
                "advancing after the highest terminal checkpoint outcome",
            )?;
            self.allocator.advance_epoch_to(successor);
        }

        Ok(result)
    }

    /// Recover to a coordinated cluster target epoch instead of the local latest.
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if the store read fails.
    pub async fn recover_to_epoch(
        &mut self,
        target_epoch: u64,
    ) -> Result<Option<crate::recovery_manager::RecoveredState>, DbError> {
        use crate::recovery_manager::RecoveryManager;

        let identity = self.expected_pipeline_identity();
        let deployment_id = self.expected_deployment_id()?;
        let mgr = RecoveryManager::new(&*self.store)
            .with_pipeline_identity(&identity)
            .with_deployment_id(deployment_id)
            .with_outcome_scope(self.active_outcome_scope());
        let decision_store = self.decision_store.as_deref().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6050] recovery requires the outcome store".into())
        })?;
        #[cfg(feature = "cluster")]
        let mut result = if let Some(controller) = self.cluster_controller.as_ref() {
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] cluster recovery requires the exact checkpoint authority: {error}"
                ))
            })?;
            mgr.recover_cluster_to_epoch(target_epoch, authority.as_ref(), decision_store)
                .await?
        } else {
            mgr.recover_to_epoch(target_epoch, Some(decision_store))
                .await?
        };
        #[cfg(not(feature = "cluster"))]
        let mut result = mgr
            .recover_to_epoch(target_epoch, Some(decision_store))
            .await?;

        if let Some(ref mut recovered) = result {
            #[cfg(feature = "cluster")]
            {
                self.validate_recovered_cluster_cut(recovered).await?;
                self.install_recovered_cluster_watermark(recovered)?;
            }
            self.record_committed_checkpoint(recovered.manifest.checkpoint_id)?;
            let successor = checked_successor_epoch(
                recovered.epoch(),
                "advancing after coordinated checkpoint recovery",
            )?;
            self.allocator.advance_epoch_to(successor);
            info!(
                epoch = self.allocator.peek_epoch(),
                "coordinator epoch set after coordinated recovery"
            );
        }
        if let Some(outcome) = self.highest_terminal_outcome().await? {
            let successor = checked_successor_epoch(
                outcome.epoch,
                "advancing after the highest terminal checkpoint outcome",
            )?;
            self.allocator.advance_epoch_to(successor);
        }

        Ok(result)
    }

    async fn highest_terminal_outcome(
        &self,
    ) -> Result<Option<laminar_core::checkpoint_decision::CheckpointOutcome>, DbError> {
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller.as_ref() {
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] cluster recovery requires the exact checkpoint authority: {error}"
                ))
            })?;
            return authority
                .highest_cluster_terminal_outcome()
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!("[LDB-6040] terminal outcome read failed: {error}"))
                });
        }
        self.decision_store
            .as_ref()
            .ok_or_else(|| {
                DbError::Checkpoint("[LDB-6050] recovery requires the outcome store".into())
            })?
            .highest_terminal_outcome()
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("[LDB-6040] terminal outcome read failed: {error}"))
            })
    }

    /// Load the latest manifest from the store.
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` on store errors.
    pub async fn load_latest_manifest(&self) -> Result<Option<CheckpointManifest>, DbError> {
        self.store.load_latest().await.map_err(DbError::from)
    }
}

impl std::fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointCoordinator")
            .field("allocator", &self.allocator)
            .field("phase", &self.phase)
            .field("sinks", &self.sinks.len())
            .field("completed", &self.checkpoints_completed)
            .field("failed", &self.checkpoints_failed)
            .finish_non_exhaustive()
    }
}

/// Fixed-size ring buffer for duration percentile tracking (microseconds, p50/p95/p99).
struct DurationHistogram {
    samples: Box<[u64; Self::CAPACITY]>,
    cursor: usize,
    count: u64,
}

impl DurationHistogram {
    const CAPACITY: usize = 100;

    /// Empty histogram.
    #[must_use]
    fn new() -> Self {
        Self {
            samples: Box::new([0; Self::CAPACITY]),
            cursor: 0,
            count: 0,
        }
    }

    /// Record a duration sample.
    fn record(&mut self, duration: Duration) {
        #[allow(clippy::cast_possible_truncation)]
        let us = duration.as_micros() as u64;
        self.samples[self.cursor] = us;
        self.cursor = (self.cursor + 1) % Self::CAPACITY;
        self.count += 1;
    }

    /// Number of recorded samples, up to `CAPACITY`.
    #[must_use]
    fn len(&self) -> usize {
        if self.count >= Self::CAPACITY as u64 {
            Self::CAPACITY
        } else {
            #[allow(clippy::cast_possible_truncation)] // count < 100, always fits usize
            {
                self.count as usize
            }
        }
    }

    /// Compute percentile `p` (0.0–1.0) over recorded samples. Returns 0 if empty.
    #[cfg(test)]
    #[must_use]
    fn percentile(&self, p: f64) -> u64 {
        let n = self.len();
        if n == 0 {
            return 0;
        }
        let mut sorted: Vec<u64> = self.samples[..n].to_vec();
        sorted.sort_unstable();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let idx = ((p * (n as f64 - 1.0)).ceil() as usize).min(n - 1);
        sorted[idx]
    }

    /// Returns `(p50, p95, p99)` in microseconds, sorting once.
    #[must_use]
    fn percentiles(&self) -> (u64, u64, u64) {
        let n = self.len();
        if n == 0 {
            return (0, 0, 0);
        }
        let mut sorted: Vec<u64> = self.samples[..n].to_vec();
        sorted.sort_unstable();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let at = |p: f64| -> u64 {
            let idx = ((p * (n as f64 - 1.0)).ceil() as usize).min(n - 1);
            sorted[idx]
        };
        (at(0.50), at(0.95), at(0.99))
    }
}

/// Checkpoint performance statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckpointStats {
    /// Successful checkpoint count.
    pub completed: u64,
    /// Failed checkpoint count.
    pub failed: u64,
    /// Duration of the most recent checkpoint.
    pub last_duration: Option<Duration>,
    /// p50 in milliseconds.
    pub duration_p50_ms: u64,
    /// p95 in milliseconds.
    pub duration_p95_ms: u64,
    /// p99 in milliseconds.
    pub duration_p99_ms: u64,
    /// Cumulative sidecar bytes written.
    pub total_bytes_written: u64,
    /// Current phase.
    pub current_phase: CheckpointPhase,
    /// Current epoch.
    pub current_epoch: u64,
}

/// Convert a `SourceCheckpoint` to a `ConnectorCheckpoint`.
#[must_use]
pub(crate) fn source_to_connector_checkpoint(cp: &SourceCheckpoint) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: cp.durable_offsets(),
        metadata: cp.metadata().clone(),
        source_assignment_version: cp.assignment_version(),
    }
}

/// Convert a `ConnectorCheckpoint` back to a `SourceCheckpoint`.
#[must_use]
pub(crate) fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source_cp = SourceCheckpoint::with_offsets(cp.offsets.clone());
    for (k, v) in &cp.metadata {
        source_cp.set_metadata(k.clone(), v.clone());
    }
    if let Some(version) = cp.source_assignment_version {
        source_cp.bind_assignment_version(version);
    }
    source_cp
}

#[cfg(test)]
mod tests;
