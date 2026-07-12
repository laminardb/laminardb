//! Checkpoint coordinator — Ring 2 control-plane orchestrator.
//!
//! Checkpoint manifest is the source of truth for source offsets; broker commits are advisory.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(feature = "cluster")]
use futures::{StreamExt, TryStreamExt};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::CoordinatedCommitNamespace;
use laminar_core::state::{CheckpointAttempt, StateBackend};
use laminar_core::storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, PipelineIdentity,
};
use laminar_core::storage::checkpoint_store::{CheckpointStore, CheckpointStoreError};
use tracing::{debug, error, info, warn};

use crate::error::DbError;

/// Which operator's blob codec a cold-group partial uses, dispatching the tier merge.
#[cfg(feature = "state-tier")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateCodec {
    Agg,
    Join,
}

/// Merge tier-fetched cold-group blobs into one cold-only partial, per operator codec.
#[cfg(feature = "state-tier")]
fn merge_cold_groups(codec: StateCodec, parts: &[bytes::Bytes]) -> Result<Vec<u8>, DbError> {
    match codec {
        StateCodec::Agg => crate::aggregate_state::merge_serialized_agg_cps(parts),
        StateCodec::Join => crate::operator::incremental_join::merge_serialized_join_frames(parts),
    }
}

#[cfg_attr(not(feature = "cluster"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum StagedSlice {
    Bytes(bytes::Bytes),
    // No bytes; a reference partial, or fetched from the tier on a forced full re-upload.
    Cold,
    // Changed-group columnar bytes + tombstone IPC, chained to this vnode's previous partial.
    Delta {
        changed: bytes::Bytes,
        tombstones: bytes::Bytes,
    },
    // Demoted groups by tier key, fetched into a cold-only partial; recovery merges additively.
    #[cfg(feature = "state-tier")]
    ColdGroups {
        group_keys: Vec<Vec<u8>>,
        codec: StateCodec,
    },
    // Re-base of a vnode holding demoted groups: resident FULL bytes merged with tier-fetched
    // groups into one self-contained base. `resident` may be empty for a fully-cold vnode.
    #[cfg(feature = "state-tier")]
    FullWithColdGroups {
        resident: bytes::Bytes,
        group_keys: Vec<Vec<u8>>,
        codec: StateCodec,
    },
}

pub(crate) type StagedVnodeStates = HashMap<u32, HashMap<String, StagedSlice>>;

/// Records the last full upload per operator slice: bytes for reference-partial comparison, or
/// `Cold` after demotion (bytes live only in the tier).
#[cfg_attr(not(feature = "state-tier"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum UploadedSlice {
    Bytes(bytes::Bytes),
    Cold,
}

impl UploadedSlice {
    /// Returns true if `staged` proves the slice unchanged since this upload.
    ///
    /// `Cold` staged means unchanged; fresh bytes against a `Cold` record re-upload full (the
    /// cold bytes are unavailable to compare).
    fn matches(&self, staged: &StagedSlice) -> bool {
        match (staged, self) {
            (StagedSlice::Cold, _) => true,
            (StagedSlice::Bytes(b), UploadedSlice::Bytes(prev)) => b == prev,
            // A delta never matches a prior full — it rides the delta-chain path, not the reference path.
            (StagedSlice::Bytes(_), UploadedSlice::Cold) | (StagedSlice::Delta { .. }, _) => false,
            // A cold-groups / full-with-cold slice re-fetches+merges from the tier — always full upload.
            #[cfg(feature = "state-tier")]
            (StagedSlice::ColdGroups { .. } | StagedSlice::FullWithColdGroups { .. }, _) => false,
        }
    }
}

enum VnodeUploadUpdate {
    Retain,
    Replace(HashMap<String, UploadedSlice>),
    Remove,
}

struct PreparedVnodePartial {
    vnode: u32,
    payload: bytes::Bytes,
    upload_update: VnodeUploadUpdate,
    is_reference: bool,
}

/// Checkpoint configuration.
const STATE_INLINE_THRESHOLD: usize = 1_048_576;
const RESTORABLE_GATE_POLL_INITIAL: Duration = Duration::from_millis(5);
const RESTORABLE_GATE_POLL_MAX: Duration = Duration::from_millis(100);
const COORDINATED_COMMITTER_POLL: Duration = Duration::from_secs(1);
#[cfg(feature = "cluster")]
const FOLLOWER_DECISION_POLL: Duration = Duration::from_millis(250);

/// A follower's relationship to the durable decision for one exact attempt.
///
/// `Excluded` is a terminal decision for the local participant: the cluster committed the cut,
/// but this participant was not part of it and must discard any late prepared state.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowerDecisionMatch {
    Pending,
    Included,
    Excluded,
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
    /// Max pipelined epochs between `Aligned` and restorable. Exactly-once pipelines cap at 1.
    pub max_in_flight_epochs: u64,
    /// Cap on in-flight captured-state bytes. At the cap, barrier admission pauses.
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
            max_in_flight_epochs: 4,
            max_staged_bytes: 512 * 1024 * 1024,
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

/// Serializes durable checkpoint-id reservation with the corresponding local epoch claim.
///
/// Checkpoint IDs come exclusively from the durable decision store. Epochs are process-local
/// ordering labels and advance only after a durable ID reservation succeeds. Failed checkpoint
/// attempts are abandoned, so both values may have gaps across crashes.
#[derive(Debug)]
pub(crate) struct EpochAllocator {
    epoch: std::sync::atomic::AtomicU64,
    allocation_lock: tokio::sync::Mutex<()>,
    allocation_timeout: Duration,
    decision_store:
        std::sync::OnceLock<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
}

impl EpochAllocator {
    fn new(epoch: u64, allocation_timeout: Duration) -> Self {
        Self {
            epoch: std::sync::atomic::AtomicU64::new(epoch),
            allocation_lock: tokio::sync::Mutex::new(()),
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

    /// Durably reserve a checkpoint ID, then claim the matching local epoch.
    ///
    /// The lock keeps concurrent admissions in epoch order. An ID reservation error returns
    /// without advancing the epoch; once the reservation lands, a later failure may leave a gap.
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
        use std::sync::atomic::Ordering;
        let timeout = self.allocation_timeout;
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6050] checkpoint ID allocator lock exhausted its {timeout:?} admission deadline"
                ))
            })?;
        let store = self.decision_store.get().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] checkpoint ID allocation requires a durable decision store".into(),
            )
        })?;
        let checkpoint_id = tokio::time::timeout_at(deadline, store.allocate_checkpoint_id())
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
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(CheckpointAttempt::new(epoch, checkpoint_id))
    }

    /// The epoch the next successful allocation will claim.
    pub(crate) fn peek_epoch(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.epoch.load(Ordering::Acquire)
    }

    /// Monotonically advance the local epoch after recovery or observing a cluster attempt.
    pub(crate) fn advance_epoch_to(&self, epoch: u64) {
        use std::sync::atomic::Ordering;
        self.epoch.fetch_max(epoch, Ordering::AcqRel);
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
        /// Cluster-min watermark from the capture acks.
        min_watermark_ms: Option<i64>,
        /// Followers that acked the capture quorum.
        participants: Vec<QuorumPeer>,
    },
}

#[cfg(feature = "cluster")]
const PARTICIPANT_READY_VERSION: u16 = 1;
#[cfg(feature = "cluster")]
pub(crate) const PARTICIPANT_READY_PREFIX: &str = "participant-ready/v1/participant=";
#[cfg(feature = "cluster")]
const PARTICIPANT_READY_READ_CONCURRENCY: usize = 8;

/// Durable proof that one capture participant completed its entire local prepare.
///
/// The marker is written after the manifest, source offsets, sink descriptors, and vnode
/// partials. The leader requires every capture participant's key in the immutable state seal
/// before recording the global decision.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct ParticipantReady {
    version: u16,
    attempt: CheckpointAttempt,
    participant_id: u64,
    assignment_version: u64,
    deployment_id: String,
    pipeline_identity: PipelineIdentity,
    owned_vnodes: Vec<u32>,
    source_offsets: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
}

#[cfg(feature = "cluster")]
pub(crate) fn participant_ready_key(participant_id: u64) -> String {
    format!("{PARTICIPANT_READY_PREFIX}{participant_id}")
}

#[cfg(feature = "cluster")]
pub(crate) fn participant_from_ready_key(key: &str) -> Option<u64> {
    let participant_id = key.strip_prefix(PARTICIPANT_READY_PREFIX)?.parse().ok()?;
    (participant_ready_key(participant_id) == key).then_some(participant_id)
}

/// Leadership observation captured for one cluster checkpoint attempt.
///
/// A lease token lets us detect that the attempt crossed terms, but it is deliberately not called
/// a durable fence: the current decision and sink APIs cannot atomically validate it.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy)]
struct CheckpointLeadership {
    lease_token: Option<u64>,
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
    state_backend: Option<Arc<dyn StateBackend>>,
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
}

async fn run_retention_maintenance(
    store: Arc<dyn CheckpointStore>,
    mut requests: tokio::sync::watch::Receiver<Option<RetentionRequest>>,
    operation_timeout: Duration,
) {
    while requests.changed().await.is_ok() {
        // `watch` is intentional: checkpoints can advance while remote deletion is
        // slow, and only the newest safe horizon matters. There is never a queue or
        // task per checkpoint.
        let Some(request) = requests.borrow_and_update().clone() else {
            continue;
        };
        let RetentionRequest {
            horizon,
            trigger_epoch,
            state_backend,
            decision_store,
        } = request;

        match tokio::time::timeout(operation_timeout, store.prune_before(horizon)).await {
            Ok(Ok(removed)) => {
                debug!(
                    trigger_epoch,
                    horizon, removed, "checkpoint manifests pruned"
                );
            }
            Ok(Err(error)) => warn!(
                trigger_epoch,
                horizon,
                %error,
                "[LDB-6026] checkpoint manifest prune failed"
            ),
            Err(_) => warn!(
                trigger_epoch,
                horizon,
                ?operation_timeout,
                "[LDB-6026] checkpoint manifest prune timed out"
            ),
        }

        if let Some(state_backend) = state_backend {
            match tokio::time::timeout(operation_timeout, state_backend.prune_before(horizon)).await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(
                    trigger_epoch,
                    horizon,
                    %error,
                    "[LDB-6026] state backend prune failed"
                ),
                Err(_) => warn!(
                    trigger_epoch,
                    horizon,
                    ?operation_timeout,
                    "[LDB-6026] state backend prune timed out"
                ),
            }
        }

        if let Some(decision_store) = decision_store {
            match tokio::time::timeout(operation_timeout, decision_store.prune_before(horizon))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(trigger_epoch, horizon, %error, "[LDB-6026] decision prune failed");
                }
                Err(_) => warn!(
                    trigger_epoch,
                    horizon,
                    ?operation_timeout,
                    "[LDB-6026] decision prune timed out"
                ),
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
    // Folded by the leader with follower watermarks to compute the cluster-wide min.
    local_watermark_ms: Option<i64>,
    // Leader-side cluster-wide min watermark, fanned out in the Commit announcement.
    #[cfg(feature = "cluster")]
    cluster_min_watermark: Option<i64>,
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
    // Bases for reference partials. Bytes are refcounted; demoted slices hold a cold marker.
    #[allow(clippy::disallowed_types)]
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
    // Channel to fetch demoted slice bytes back from the tier on a forced full re-upload.
    #[cfg(feature = "state-tier")]
    state_tier: Option<crate::state_tier::TierTx>,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    // A single watch slot coalesces checkpoint retention requests at a monotonically advancing
    // recovery/external-commit-safe horizon. The JoinSet owns exactly one worker and aborts it on
    // coordinator drop, so remote GC can neither enter source-ack latency nor detach.
    retention_requests: tokio::sync::watch::Sender<Option<RetentionRequest>>,
    retention_requested_horizon: u64,
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
            Ok(Some(m)) if m.checkpoint_id == *id => return Ok(Some(m)),
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
        let store: Arc<dyn CheckpointStore> = Arc::from(store);
        let highest = load_highest(store.as_ref()).await.map_err(|e| {
            DbError::Checkpoint(format!(
                "[LDB-6028] failed to list checkpoints at coordinator \
                 construction: {e} — refusing to start at epoch 1 and \
                 clobber existing on-disk state"
            ))
        })?;
        let epoch = highest.as_ref().map_or(1, |m| m.epoch.saturating_add(1));
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
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            prom: None,
            total_bytes_written: 0,
            state_backend: None,
            assignment_version: 0,
            decision_store: None,
            pipeline_identity: None,
            deployment_id: None,
            highest_decided: 0,
            local_watermark_ms: None,
            #[cfg(feature = "cluster")]
            cluster_min_watermark: None,
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
            #[cfg(feature = "state-tier")]
            state_tier: None,
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            retention_requests,
            retention_requested_horizon: 0,
            #[cfg(feature = "cluster")]
            local_manifest_retention_requested_horizon: 0,
            maintenance_tasks,
            cached_sorted_sink_names: None,
        })
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
        let request = RetentionRequest {
            horizon: self.retention_requested_horizon,
            trigger_epoch: epoch,
            state_backend: backend,
            decision_store,
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
        let request = RetentionRequest {
            horizon: self.local_manifest_retention_requested_horizon,
            trigger_epoch: epoch,
            state_backend: None,
            decision_store: None,
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
    pub fn set_state_backend(&mut self, backend: Arc<dyn StateBackend>) {
        self.state_backend = Some(backend);
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

    /// Participant bound to the checkpoint-store namespace.
    #[must_use]
    #[cfg(feature = "cluster")]
    pub(crate) fn participant_id(&self) -> u64 {
        self.store.participant_id()
    }

    /// Set the assignment generation forwarded to `write_partial` for the split-brain fence.
    pub fn set_assignment_version(&mut self, version: u64) {
        self.assignment_version = version;
    }

    /// Set the local watermark reported in `BarrierAck` so the leader can fold it into the
    /// cluster-wide minimum. `None` disables this instance's contribution.
    pub fn set_local_watermark_ms(&mut self, watermark: Option<i64>) {
        self.local_watermark_ms = watermark;
    }

    /// Stage per-vnode operator-state slices for the next checkpoint.
    ///
    /// Call once per checkpoint (even with an empty map) so prior epoch slices never leak.
    #[allow(clippy::disallowed_types)]
    pub(crate) fn set_pending_vnode_states(&mut self, states: StagedVnodeStates) {
        self.pending_vnode_states = states;
    }

    /// Wire the cold-tier channel for fetching demoted slice bytes on forced full re-uploads.
    #[cfg(feature = "state-tier")]
    pub(crate) fn set_state_tier(&mut self, tier: crate::state_tier::TierTx) {
        self.state_tier = Some(tier);
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
        self.vnode_set = vnodes;
    }

    /// Set the vnodes the durability gate checks (the full registry in cluster mode).
    pub fn set_gate_vnode_set(&mut self, vnodes: Vec<u32>) {
        self.gate_vnode_set = vnodes;
    }

    /// Fetch a demoted slice from the cold tier for a forced full re-upload.
    #[cfg(feature = "state-tier")]
    async fn fetch_cold_slice(&self, operator: &str, vnode: u32) -> Result<bytes::Bytes, DbError> {
        let Some(ref tier) = self.state_tier else {
            return Err(DbError::Checkpoint(format!(
                "cold slice staged but no state tier is wired \
                 (operator={operator}, vnode={vnode})"
            )));
        };
        let (reply, rx) = tokio::sync::oneshot::channel();
        let operation = async {
            tier.send(crate::state_tier::TierRequest::Fetch {
                operator: Arc::from(operator),
                vnode,
                reply,
            })
            .await
            .map_err(|_| DbError::Checkpoint("state tier worker is gone".to_string()))?;
            rx.await.map_err(|_| {
                DbError::Checkpoint("state tier worker dropped the reply".to_string())
            })?
        };
        let fetched = tokio::time::timeout(self.config.checkpoint_timeout, operation)
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "state tier fetch timed out (operator={operator}, vnode={vnode})"
                ))
            })??;
        match fetched {
            Some(bytes) => Ok(bytes),
            None => Err(DbError::Checkpoint(format!(
                "demoted slice missing from the state tier \
                 (operator={operator}, vnode={vnode}) — failing the epoch \
                 rather than dropping it from recovery truth"
            ))),
        }
    }

    /// Without `state-tier`, a `Cold` slice cannot be staged; reaching this is a logic error.
    #[cfg(not(feature = "state-tier"))]
    #[allow(clippy::unused_async)]
    async fn fetch_cold_slice(&self, operator: &str, vnode: u32) -> Result<bytes::Bytes, DbError> {
        Err(DbError::Checkpoint(format!(
            "cold slice staged without state-tier support \
             (operator={operator}, vnode={vnode})"
        )))
    }

    /// Fetch one demoted GROUP's bytes from the cold tier. A miss fails the epoch — silently
    /// dropping a demoted group would break recovery.
    #[cfg(feature = "state-tier")]
    async fn fetch_cold_group(
        &self,
        operator: &str,
        vnode: u32,
        group: &[u8],
    ) -> Result<bytes::Bytes, DbError> {
        let Some(ref tier) = self.state_tier else {
            return Err(DbError::Checkpoint(format!(
                "cold group staged but no state tier is wired (operator={operator}, vnode={vnode})"
            )));
        };
        let (reply, rx) = tokio::sync::oneshot::channel();
        let operation = async {
            tier.send(crate::state_tier::TierRequest::FetchGroup {
                operator: Arc::from(operator),
                vnode,
                group: group.to_vec(),
                reply,
            })
            .await
            .map_err(|_| DbError::Checkpoint("state tier worker is gone".to_string()))?;
            rx.await.map_err(|_| {
                DbError::Checkpoint("state tier worker dropped the reply".to_string())
            })?
        };
        let fetched = tokio::time::timeout(self.config.checkpoint_timeout, operation)
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "state tier group fetch timed out (operator={operator}, vnode={vnode})"
                ))
            })??;
        match fetched {
            Some(bytes) => Ok(bytes),
            None => Err(DbError::Checkpoint(format!(
                "demoted group missing from the state tier (operator={operator}, vnode={vnode}) — \
                 failing the epoch rather than dropping it from recovery truth"
            ))),
        }
    }

    /// Resolve a `ColdGroups` slice into one cold-only checkpoint: fetch each demoted group and
    /// merge over disjoint keys. Recovery applies it additively over the resident groups.
    #[cfg(feature = "state-tier")]
    async fn resolve_cold_groups(
        &self,
        operator: &str,
        vnode: u32,
        group_keys: &[Vec<u8>],
        codec: StateCodec,
    ) -> Result<bytes::Bytes, DbError> {
        let mut parts: Vec<bytes::Bytes> = Vec::with_capacity(group_keys.len());
        for gk in group_keys {
            parts.push(self.fetch_cold_group(operator, vnode, gk).await?);
        }
        Ok(bytes::Bytes::from(merge_cold_groups(codec, &parts)?))
    }

    /// Resolve a `FullWithColdGroups` re-base into one self-contained base: fold the resident FULL
    /// bytes with the tier-fetched demoted groups over disjoint keys. `resident` may be empty.
    #[cfg(feature = "state-tier")]
    async fn resolve_full_with_cold_groups(
        &self,
        operator: &str,
        vnode: u32,
        resident: &bytes::Bytes,
        group_keys: &[Vec<u8>],
        codec: StateCodec,
    ) -> Result<bytes::Bytes, DbError> {
        let mut parts: Vec<bytes::Bytes> = Vec::with_capacity(group_keys.len() + 1);
        parts.push(resident.clone());
        for gk in group_keys {
            parts.push(self.fetch_cold_group(operator, vnode, gk).await?);
        }
        Ok(bytes::Bytes::from(merge_cold_groups(codec, &parts)?))
    }

    /// Vnodes with memory-resident slices, as `(vnode, bytes)`, largest first.
    #[cfg(feature = "state-tier")]
    pub(crate) fn demotion_candidates(&self) -> Vec<(u32, usize)> {
        let mut out: Vec<(u32, usize)> = self
            .last_vnode_uploads
            .iter()
            .filter(|(_, (_, slices))| {
                slices
                    .values()
                    .any(|s| matches!(s, UploadedSlice::Bytes(_)))
            })
            .map(|(v, (_, slices))| {
                let total = slices
                    .values()
                    .map(|s| match s {
                        UploadedSlice::Bytes(b) => b.len(),
                        UploadedSlice::Cold => 0,
                    })
                    .sum();
                (*v, total)
            })
            .collect();
        out.sort_by_key(|&(_, total)| std::cmp::Reverse(total));
        out
    }

    /// The last durable upload bytes for `vnode`, to hand to the tier on demotion.
    #[cfg(feature = "state-tier")]
    pub(crate) fn slices_for_demotion(&self, vnode: u32) -> Vec<(String, bytes::Bytes)> {
        self.last_vnode_uploads
            .get(&vnode)
            .map(|(_, slices)| {
                slices
                    .iter()
                    .filter_map(|(n, s)| match s {
                        UploadedSlice::Bytes(b) => Some((n.clone(), b.clone())),
                        UploadedSlice::Cold => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Release the in-memory pin for a confirmed-demoted slice. Call after the tier write
    /// lands and the operator drops the groups; reference partials then key off the cold marker.
    #[cfg(feature = "state-tier")]
    pub(crate) fn mark_slice_demoted(&mut self, vnode: u32, operator: &str) {
        if let Some((_, slices)) = self.last_vnode_uploads.get_mut(&vnode) {
            if let Some(s) = slices.get_mut(operator) {
                *s = UploadedSlice::Cold;
            }
        }
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

    /// Drop every registered sink handle after an aborted pipeline startup.
    pub(crate) fn clear_sinks(&mut self) {
        self.sinks.clear();
        self.cached_sorted_sink_names = None;
        self.coordinated_commit_lag
            .store(0, std::sync::atomic::Ordering::Release);
        self.coordinated_commit_lag_known
            .store(true, std::sync::atomic::Ordering::Release);
        self.coordinated_commit_progress.notify_one();
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

    #[cfg_attr(not(feature = "cluster"), allow(clippy::unused_self))]
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

    /// Capture the leadership term at checkpoint entry. Gossip-only clusters have no term token;
    /// they still re-check the elected leader at every irreversible boundary.
    #[cfg(feature = "cluster")]
    fn capture_checkpoint_leadership(&self) -> Result<Option<CheckpointLeadership>, String> {
        let Some(controller) = self.cluster_controller.as_ref() else {
            return Ok(None);
        };
        if !controller.is_leader() {
            return Err(
                "[LDB-6054] checkpoint rejected because this node is not the current leader".into(),
            );
        }
        let lease_token = if controller.has_leader_lease_fencing() {
            Some(controller.leader_fencing_token().ok_or_else(|| {
                "[LDB-6054] checkpoint rejected because the durable leader lease is not held"
                    .to_owned()
            })?)
        } else {
            None
        };
        Ok(Some(CheckpointLeadership { lease_token }))
    }

    /// Re-check both leader ownership and, when available, the exact lease term captured at entry.
    #[cfg(feature = "cluster")]
    fn ensure_checkpoint_leadership(
        &self,
        captured: Option<CheckpointLeadership>,
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
        if !controller.is_leader() {
            return Err(format!(
                "[LDB-6054] checkpoint leadership lost before {boundary}"
            ));
        }

        match (captured.lease_token, controller.leader_fencing_token()) {
            (Some(expected), Some(current)) if expected == current => Ok(()),
            (Some(expected), Some(current)) => Err(format!(
                "[LDB-6054] checkpoint crossed leader-lease terms before {boundary}: \
                 expected token {expected}, current token {current}"
            )),
            (Some(expected), None) => Err(format!(
                "[LDB-6054] checkpoint lost leader-lease token {expected} before {boundary}"
            )),
            (None, Some(current)) => Err(format!(
                "[LDB-6054] durable leader fencing appeared during checkpoint before {boundary} \
                 (token {current}); retry in one stable term"
            )),
            (None, None) => Ok(()),
        }
    }

    /// Persist this participant's final prepare attestation.
    ///
    /// The payload also carries the exact source-offset handoff. Requiring its key in `_SEAL`
    /// proves both manifest completion and a complete participant offset inventory, including
    /// participants with zero vnodes or empty offsets.
    #[cfg(feature = "cluster")]
    async fn persist_participant_ready_until(
        &self,
        attempt: CheckpointAttempt,
        manifest: &CheckpointManifest,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let backend = self.state_backend.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] cluster checkpoint requires a state backend for participant \
                 readiness attestation"
                    .into(),
            )
        })?;
        if self.assignment_version == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] cluster participant readiness requires a non-zero assignment version"
                    .into(),
            ));
        }
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
        let ready = ParticipantReady {
            version: PARTICIPANT_READY_VERSION,
            attempt,
            participant_id: self.self_node_id(),
            assignment_version: self.assignment_version,
            deployment_id: manifest.deployment_id.clone(),
            pipeline_identity: manifest.pipeline_identity.clone(),
            owned_vnodes,
            source_offsets,
        };
        if ready.participant_id != manifest.participant_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] participant readiness writer {} does not match manifest participant {}",
                ready.participant_id, manifest.participant_id
            )));
        }
        let bytes = serde_json::to_vec(&ready)
            .map(bytes::Bytes::from)
            .map_err(|error| {
                DbError::Checkpoint(format!("participant readiness encode: {error}"))
            })?;
        tokio::time::timeout_at(
            deadline,
            backend.write_commit_descriptor(
                attempt,
                &participant_ready_key(ready.participant_id),
                self.assignment_version,
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
        })
    }

    /// The source-instance-namespaced offset map for the latest sealed checkpoint, unioned from
    /// every participant's readiness attestation (opaque connector key/values). An acquiring
    /// source resumes its newly-owned partitions from
    /// the previous owner's sealed position. Empty backend / no sealed attempt yields an empty map;
    /// a read FAILURE is propagated so the caller defers the rotation rather than re-emitting.
    ///
    /// The cut is `latest_sealed_checkpoint` (the exact state attempt), NOT `CheckpointDecisionStore::
    /// highest_committed` (the 2PC decision) — deliberately. `RecoveryManager::rehydrate` picks the
    /// adopt path's state cut from the same seal, and the two must agree: resuming offsets at the
    /// decision cut while rehydrating state at the seal double-counts `[decision, seal)`. Moving
    /// both together is `0fb07a90`, reverted in `6ab5e04e`; re-landing it is soak-gated on the
    /// cluster kill-9 matrix. A coordinated rewind truncates the timeline to the decision cut
    /// (`coordinated_recovery.rs`), so the seal cannot outlive a decision across a recovery.
    #[cfg(feature = "cluster")]
    pub(crate) async fn acquired_source_handoff(
        &self,
    ) -> Result<Option<(CheckpointAttempt, HashMap<String, HashMap<String, String>>)>, DbError>
    {
        if self.cluster_controller.is_none() {
            return Ok(None);
        }
        let Some(ref backend) = self.state_backend else {
            return Ok(None);
        };
        let attempt = match backend.latest_sealed_checkpoint().await {
            Ok(Some(attempt)) => attempt,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(DbError::Checkpoint(format!(
                    "source-offset handoff: latest_sealed_checkpoint failed: {e}"
                )));
            }
        };
        let offsets = self.source_offsets_at(attempt).await?;
        // Return the exact attempt with its offsets so assignment adoption can
        // pin vnode state to this same cut even if a newer checkpoint seals
        // while object-store rehydration is in progress.
        info!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            sources = offsets.len(),
            "source-offset handoff staged for acquire"
        );
        Ok(Some((attempt, offsets)))
    }

    #[cfg(feature = "cluster")]
    async fn read_participant_ready(
        backend: &dyn StateBackend,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<ParticipantReady, DbError> {
        let bytes = backend
            .read_commit_descriptor(attempt, key)
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
            })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6041] participant readiness marker '{key}' is corrupt: {error}"
            ))
        })
    }

    /// Every sealed participant's exact source-offset handoff, unioned from readiness markers.
    /// Recovery passes the attempt it restored (not latest), so an acquired partition cannot skip
    /// past its state cut.
    #[cfg(feature = "cluster")]
    pub(crate) async fn source_offsets_at(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<HashMap<String, HashMap<String, String>>, DbError> {
        // `cluster` is a compile-time capability, not the active runtime mode. Embedded and
        // standalone runtimes compiled with that feature recover directly from their local
        // manifest and intentionally have no participant-readiness inventory.
        if self.cluster_controller.is_none() {
            return Ok(HashMap::new());
        }
        let backend = self.state_backend.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] source-offset recovery requires the sealed cluster state backend"
                    .into(),
            )
        })?;
        let inventory = backend
            .checkpoint_seal_inventory(attempt)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("checkpoint seal inventory read failed: {error}"))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] checkpoint {} epoch {} has no exact state seal",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })?;
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

        let expected_deployment = self.expected_deployment_id()?;
        let expected_identity = self.expected_pipeline_identity();
        let readiness = futures::stream::iter(ready_keys.into_iter().map(|key| async move {
            let key_participant = participant_from_ready_key(&key).ok_or_else(|| {
                DbError::Checkpoint(format!("invalid participant readiness key '{key}'"))
            })?;
            let ready = Self::read_participant_ready(backend.as_ref(), attempt, &key).await?;
            Ok::<_, DbError>((key, key_participant, ready))
        }))
        .buffer_unordered(PARTICIPANT_READY_READ_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

        let mut participants = std::collections::BTreeSet::new();
        let mut owned_vnodes = std::collections::BTreeSet::new();
        let mut merged = HashMap::new();
        for (key, key_participant, ready) in readiness {
            let mut canonical_vnodes = ready.owned_vnodes.clone();
            canonical_vnodes.sort_unstable();
            canonical_vnodes.dedup();
            if ready.version != PARTICIPANT_READY_VERSION
                || ready.attempt != attempt
                || ready.participant_id != key_participant
                || ready.assignment_version != inventory.assignment_version
                || ready.deployment_id != expected_deployment
                || ready.pipeline_identity != expected_identity
                || canonical_vnodes != ready.owned_vnodes
                || !participants.insert(ready.participant_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6041] participant readiness marker '{key}' does not match its sealed cut"
                )));
            }
            for vnode in ready.owned_vnodes {
                if !owned_vnodes.insert(vnode) {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6041] vnode {vnode} is claimed by multiple checkpoint participants"
                    )));
                }
            }
            for (source, offsets) in ready.source_offsets {
                let target = merged.entry(source).or_insert_with(HashMap::new);
                for (key, value) in offsets {
                    if let Some(existing) = target.insert(key.clone(), value.clone()) {
                        if existing != value {
                            return Err(DbError::Checkpoint(format!(
                                "[LDB-6033] conflicting handoff offset for source key '{key}' at checkpoint {}",
                                attempt.checkpoint_id
                            )));
                        }
                    }
                }
            }
        }
        let sealed_vnodes: std::collections::BTreeSet<u32> =
            inventory.required_vnodes.iter().copied().collect();
        if owned_vnodes != sealed_vnodes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] participant readiness vnode inventory {owned_vnodes:?} does not match seal {sealed_vnodes:?}"
            )));
        }
        Ok(merged)
    }

    /// Begin the initial epoch on all exactly-once sinks.
    ///
    /// Must be called once after all sinks are registered and before any writes. Subsequent
    /// epochs are started automatically after each successful checkpoint commit.
    ///
    /// # Errors
    /// Returns the first sink error.
    pub async fn begin_initial_epoch(&self) -> Result<(), DbError> {
        self.begin_epoch_for_sinks_bounded(self.allocator.peek_epoch())
            .await
    }

    /// Shared id allocator — the pipeline callback clones this to allocate without the mutex.
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    /// Begin an epoch on all exactly-once sinks, rolling back already-started sinks on failure.
    async fn begin_epoch_for_sinks_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut started: Vec<&RegisteredSink> = Vec::new();
        for sink in &self.sinks {
            if sink.handle.checkpoint_committable() {
                match sink.handle.begin_epoch_until(epoch, deadline).await {
                    Ok(()) => {
                        started.push(sink);
                        debug!(sink = %sink.name, epoch, "began epoch");
                    }
                    Err(e) => {
                        for s in &started {
                            if let Err(re) = s.handle.rollback_epoch_until(epoch, deadline).await {
                                error!(sink = %s.name, epoch, error = %re,
                                    "[LDB-6016] sink rollback failed during begin_epoch recovery");
                            }
                        }
                        return Err(DbError::Checkpoint(format!(
                            "sink '{}' failed to begin epoch {epoch}: {e}",
                            sink.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    async fn begin_epoch_for_sinks_bounded(&self, epoch: u64) -> Result<(), DbError> {
        let timeout = self.config.cleanup_timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        self.begin_epoch_for_sinks_until(epoch, deadline)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "sink begin_epoch {epoch} failed within its {timeout:?} cleanup deadline: \
                     {error}"
                ))
            })
    }

    /// Wire Prometheus engine metrics.
    pub fn set_metrics(&mut self, prom: Arc<crate::engine_metrics::EngineMetrics>) {
        self.prom = Some(prom);
    }

    fn emit_checkpoint_metrics(&self, success: bool, epoch: u64, duration: Duration) {
        if let Some(ref m) = self.prom {
            if success {
                m.checkpoints_completed.inc();
            } else {
                m.checkpoints_failed.inc();
            }
            #[allow(clippy::cast_possible_wrap)]
            m.checkpoint_epoch.set(epoch as i64);
            m.checkpoint_duration.observe(duration.as_secs_f64());
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
        let attempt = self.allocator.allocate_until(deadline).await?;
        self.run_checkpoint_attempt(request, attempt, QuorumStage::RunInline, started)
            .await
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
        self.decision_write_started = false;
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

        let Ok(result) = tokio::time::timeout_at(
            deadline,
            self.checkpoint_inner(request, attempt, quorum, started, deadline),
        )
        .await
        else {
            let error = format!(
                "checkpoint {} epoch {} exceeded its {:?} end-to-end deadline",
                attempt.checkpoint_id, attempt.epoch, self.config.checkpoint_timeout
            );
            if self.decision_write_started {
                return Ok(self.fail_after_irrevocable_work(
                    attempt.checkpoint_id,
                    attempt.epoch,
                    started,
                    error,
                ));
            }
            // `fail_epoch` records failure before its bounded cleanup awaits. If the total
            // deadline expired during that cleanup, do not count or enqueue cleanup twice.
            if self.phase == CheckpointPhase::Idle {
                self.pending_vnode_states.clear();
                self.pending_sink_descriptors.clear();
                return Ok(CheckpointResult {
                    success: false,
                    checkpoint_id: attempt.checkpoint_id,
                    epoch: attempt.epoch,
                    duration: started.elapsed(),
                    error: Some(error),
                });
            }
            return Ok(self
                .fail_epoch(attempt.checkpoint_id, attempt.epoch, started, error)
                .await);
        };
        result
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
        let futures = self.sinks.iter().map(|sink| {
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
                    // At-least-once: a plain buffer flush, NOT pre_commit — ALO sinks never got
                    // begin_epoch, and some (Postgres) reject a pre_commit for an epoch they didn't
                    // open. This lands buffered rows before the manifest seals offsets (CP-5).
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
        });
        let collected = futures::future::try_join_all(futures).await?;
        Ok(collected.into_iter().flatten().collect())
    }

    /// Save a manifest (and optional sidecar) to the store, bounded by the attempt timeout.
    ///
    /// Sidecar is written before the manifest: a failed sidecar write never leaves a
    /// manifest referencing missing state.
    async fn save_manifest(
        &self,
        manifest: Arc<CheckpointManifest>,
        state_data: Option<Vec<bytes::Bytes>>,
    ) -> Result<CheckpointManifest, DbError> {
        let timeout_dur = self.config.checkpoint_timeout;
        let fut = self.store.save_with_state(&manifest, state_data.as_deref());
        match tokio::time::timeout(timeout_dur, fut).await {
            Ok(Ok(persisted)) => Ok(persisted),
            Ok(Err(e)) => Err(DbError::from(e)),
            Err(_elapsed) => Err(DbError::Checkpoint(format!(
                "[LDB-6011] manifest persist timed out after {}s — \
                 filesystem may be degraded",
                timeout_dur.as_secs()
            ))),
        }
    }

    async fn finalize_manifest(&self, checkpoint_id: u64) -> Result<CheckpointManifest, DbError> {
        let timeout = self.config.checkpoint_timeout;
        tokio::time::timeout(timeout, self.store.finalize(checkpoint_id))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "manifest finalization for checkpoint {checkpoint_id} timed out after \
                     {timeout:?}"
                ))
            })?
            .map_err(DbError::from)
    }

    /// This coordinator's node id for namespacing commit descriptors (0 without the cluster feature).
    #[cfg_attr(not(feature = "cluster"), allow(clippy::unused_self))]
    fn self_node_id(&self) -> u64 {
        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.as_ref() {
            return cc.instance_id().0;
        }
        0
    }

    #[cfg_attr(not(feature = "cluster"), allow(clippy::unused_self))]
    fn active_decision_scope(&self) -> laminar_core::checkpoint_decision::CommitDecisionScope {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return laminar_core::checkpoint_decision::CommitDecisionScope::Cluster;
        }
        laminar_core::checkpoint_decision::CommitDecisionScope::Local
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
            backend
                .write_commit_descriptor(
                    attempt,
                    &crate::coordinated_committer::descriptor_key(namespace, participant_id),
                    self.assignment_version,
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

    /// Write each owned vnode's `partial.bin` to seal the durability gate.
    ///
    /// Unchanged vnodes emit a reference partial; changed vnodes do a full upload. References
    /// are forced back to full before their base ages out of the prune window. All writes run
    /// concurrently. Bases are recorded only after every write in an epoch lands, so a partially
    /// failed epoch re-uploads full on the next attempt.
    async fn write_vnode_partials(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<(), DbError> {
        let timeout = self.config.checkpoint_timeout;
        tokio::time::timeout(
            timeout,
            self.write_vnode_partials_inner(epoch, checkpoint_id),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6024] vnode/descriptor persistence timed out after {timeout:?} \
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
        let max_ref_age = (self.config.max_retained as u64).max(1);

        // Classify each vnode as reference or full. A staged `Cold` slice counts as unchanged;
        // a forced full upload fetches the cold bytes back (a fetch miss fails the epoch).
        let mut prepared = Vec::with_capacity(self.vnode_set.len());
        for &v in &self.vnode_set {
            prepared.push(self.prepare_vnode_partial(v, epoch, max_ref_age).await?);
        }

        let writes = prepared.iter().map(|partial| {
            let backend = Arc::clone(backend);
            let v = partial.vnode;
            let payload = partial.payload.clone();
            async move {
                backend
                    .write_partial(attempt, v, caller_version, payload)
                    .await
                    .map_err(|e| {
                        DbError::Checkpoint(format!(
                            "[LDB-6024] vnode partial write failed (vnode={v}, epoch={epoch}): {e}"
                        ))
                    })
            }
        });
        futures::future::try_join_all(writes).await?;

        // Record the parent link only after every write lands, so a partially failed epoch is not
        // chained from on the next attempt.
        let mut reference_count = 0_u64;
        for partial in prepared {
            self.last_partial_attempt.insert(partial.vnode, attempt);
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

    async fn prepare_vnode_partial(
        &self,
        vnode: u32,
        epoch: u64,
        max_ref_age: u64,
    ) -> Result<PreparedVnodePartial, DbError> {
        let ops = self.pending_vnode_states.get(&vnode);
        let has_delta =
            ops.is_some_and(|ops| ops.values().any(|s| matches!(s, StagedSlice::Delta { .. })));
        let (partial, upload_update, is_reference) = if has_delta {
            // A delta partial chains to the exact previous attempt this vnode was written.
            let Some(parent) = self.last_partial_attempt.get(&vnode).copied() else {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6025] delta partial for vnode {vnode} has no parent epoch \
                     (epoch={epoch}); a just-acquired vnode must re-base FULL first"
                )));
            };
            let (partial, update) = self.prepare_delta_vnode_partial(vnode, parent, ops).await?;
            (partial, update, false)
        } else {
            self.prepare_snapshot_vnode_partial(vnode, epoch, max_ref_age, ops)
                .await?
        };

        Ok(PreparedVnodePartial {
            vnode,
            payload: bytes::Bytes::from(partial.encode()?),
            upload_update,
            is_reference,
        })
    }

    async fn prepare_delta_vnode_partial(
        &self,
        vnode: u32,
        parent: CheckpointAttempt,
        ops: Option<&HashMap<String, StagedSlice>>,
    ) -> Result<(crate::vnode_partial::VnodePartial, VnodeUploadUpdate), DbError> {
        // Re-based (full) operators ride beside deltas and reset their own bases. Recovery resolves
        // each operator's chain independently.
        let mut operators = Vec::new();
        let mut deltas = Vec::new();
        let mut recorded = HashMap::new();
        if let Some(ops) = ops {
            for (name, slice) in ops {
                match slice {
                    StagedSlice::Delta {
                        changed,
                        tombstones,
                    } => deltas.push((
                        name.clone(),
                        crate::vnode_partial::OpDelta {
                            changed: changed.to_vec(),
                            tombstones_ipc: tombstones.to_vec(),
                        },
                    )),
                    full_slice => {
                        let (bytes, uploaded) = self
                            .resolve_full_vnode_slice(name, vnode, full_slice)
                            .await?;
                        operators.push((name.clone(), bytes.to_vec()));
                        recorded.insert(name.clone(), uploaded);
                    }
                }
            }
        }
        let update = if recorded.is_empty() {
            VnodeUploadUpdate::Retain
        } else {
            VnodeUploadUpdate::Replace(recorded)
        };
        Ok((
            crate::vnode_partial::VnodePartial {
                operators,
                base: Some(parent),
                deltas,
            },
            update,
        ))
    }

    async fn prepare_snapshot_vnode_partial(
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
                    epoch.saturating_sub(base.epoch) < max_ref_age
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
                // Cold slices/groups contribute bytes but stay pinned in the tier. A cold-groups
                // partial is cold-only.
                let (bytes, uploaded) = self.resolve_full_vnode_slice(name, vnode, slice).await?;
                operators.push((name.clone(), bytes.to_vec()));
                recorded.insert(name.clone(), uploaded);
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

    async fn resolve_full_vnode_slice(
        &self,
        operator: &str,
        vnode: u32,
        slice: &StagedSlice,
    ) -> Result<(bytes::Bytes, UploadedSlice), DbError> {
        match slice {
            StagedSlice::Bytes(bytes) => Ok((bytes.clone(), UploadedSlice::Bytes(bytes.clone()))),
            StagedSlice::Cold => Ok((
                self.fetch_cold_slice(operator, vnode).await?,
                UploadedSlice::Cold,
            )),
            #[cfg(feature = "state-tier")]
            StagedSlice::FullWithColdGroups {
                resident,
                group_keys,
                codec,
            } => Ok((
                self.resolve_full_with_cold_groups(operator, vnode, resident, group_keys, *codec)
                    .await?,
                UploadedSlice::Cold,
            )),
            #[cfg(feature = "state-tier")]
            StagedSlice::ColdGroups { group_keys, codec } => Ok((
                self.resolve_cold_groups(operator, vnode, group_keys, *codec)
                    .await?,
                UploadedSlice::Cold,
            )),
            StagedSlice::Delta { .. } => unreachable!("delta routed before full-slice resolution"),
        }
    }

    /// Poll until every vnode in `gate_vnode_set` has its partial for the exact attempt, or the
    /// gate timeout expires. Transient I/O errors retry; immutable conflicts abort.
    async fn await_restorable_gate(
        &self,
        attempt: CheckpointAttempt,
        participants: &[QuorumPeer],
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
        loop {
            if attempt.epoch < self.rotation_epoch_floor {
                return Err(format!(
                    "vnode assignment rotated after epoch {} captured \
                     (rotation floor {}); epoch cannot seal",
                    attempt.epoch, self.rotation_epoch_floor
                ));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "state durability gate exhausted the end-to-end checkpoint deadline: \
                     {last_state}"
                ));
            }
            let seal_result = tokio::time::timeout(
                remaining,
                backend.seal_checkpoint(
                    attempt,
                    self.assignment_version,
                    &self.gate_vnode_set,
                    &required_descriptors,
                ),
            )
            .await;
            match seal_result {
                Err(_) => {
                    return Err(
                        "state durability gate exhausted the end-to-end checkpoint deadline while sealing exact attempt"
                            .into(),
                    );
                }
                Ok(Ok(true)) => {
                    #[cfg(feature = "cluster")]
                    if self.cluster_controller.is_some() {
                        // The object-store seal proves the required marker keys exist. Decode the
                        // payloads before publishing a decision so corrupt or cross-attempt
                        // attestations fail closed instead of becoming the recovery frontier.
                        self.source_offsets_at(attempt)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    return Ok(());
                }
                Ok(Ok(false)) => {}
                Ok(Err(e @ StateBackendError::Conflict { .. })) => {
                    return Err(format!("state durability gate: {e}"));
                }
                Ok(Err(e)) => {
                    debug!(epoch = attempt.epoch, checkpoint_id = attempt.checkpoint_id, error = %e, "durability gate poll error; retrying");
                    last_state = e.to_string();
                }
            }
            // Fail fast when a capture participant dies; doomed pipelined epochs each burn the
            // full timeout otherwise.
            #[cfg(feature = "cluster")]
            if let Some(cc) = self.cluster_controller.as_ref() {
                if let Some(reason) =
                    Self::unhealthy_participant(&cc.members_watch().borrow(), participants)
                {
                    return Err(format!("durability gate fail-fast: {reason}"));
                }
                if let Some(p) = participants
                    .iter()
                    .find(|p| cc.is_recently_unresponsive(**p))
                {
                    return Err(format!(
                        "durability gate fail-fast: follower {} missed a capture quorum",
                        p.0
                    ));
                }
            }
            #[cfg(not(feature = "cluster"))]
            let _ = participants;
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

    /// Abandon a failed epoch: announce `Abort`, roll back sinks, and open the next epoch.
    async fn fail_epoch(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        started: Instant,
        reason: String,
    ) -> CheckpointResult {
        let mut result = self.record_failed_epoch(checkpoint_id, epoch, started, reason);
        // Abort publication, rollback and successor setup share one cleanup deadline. In
        // particular, a slow control-plane write must not refresh the connector cleanup budget.
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        #[cfg(feature = "cluster")]
        if tokio::time::timeout_at(
            cleanup_deadline,
            self.announce_if_leader(
                epoch,
                checkpoint_id,
                laminar_core::cluster::control::Phase::Abort,
                None,
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
        if let Err(cleanup_error) = self
            .cleanup_failed_epoch_until(epoch, cleanup_deadline)
            .await
        {
            error!(
                checkpoint_id, epoch, error = %cleanup_error,
                "[LDB-6004] checkpoint failure cleanup did not complete",
            );
            if let Some(error) = result.error.as_mut() {
                *error = format!("{error}; cleanup incomplete: {cleanup_error}");
            }
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
        self.emit_checkpoint_metrics(false, epoch, duration);
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
        }
    }

    async fn cleanup_failed_epoch_until(
        &self,
        epoch: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let rollback = self.rollback_sinks_until(epoch, deadline).await;
        let successor = self.begin_next_epoch_until(deadline).await;
        match (rollback, successor) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(rollback_error), Err(successor_error)) => Err(DbError::Checkpoint(format!(
                "sink rollback failed: {rollback_error}; successor epoch failed: {successor_error}"
            ))),
        }
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
        self.emit_checkpoint_metrics(false, epoch, duration);
        self.pending_vnode_states.clear();
        self.pending_sink_descriptors.clear();
        CheckpointResult {
            success: false,
            checkpoint_id,
            epoch,
            duration,
            error: Some(error),
        }
    }

    /// Begin the next epoch's sink transactions, bounded by the internal cleanup budget.
    ///
    /// The failing sink may be wedged; an unbounded await would hang the coordinator.
    #[cfg(feature = "cluster")]
    async fn begin_next_epoch_bounded(&self) {
        let deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        if let Err(error) = self.begin_next_epoch_until(deadline).await {
            error!(
                error = %error,
                "[LDB-6015] failed to begin next epoch after abandoning a failed one"
            );
        }
    }

    async fn begin_next_epoch_until(&self, deadline: tokio::time::Instant) -> Result<(), DbError> {
        let next_epoch = self.allocator.peek_epoch();
        self.begin_epoch_for_sinks_until(next_epoch, deadline)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "failed to begin successor epoch {next_epoch}: {error}"
                ))
            })
    }

    async fn prepared_decision_status(
        &self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<(bool, bool), DbError> {
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] prepared-checkpoint reconciliation requires the durable decision store"
                    .into(),
            )
        })?;
        let decision = tokio::time::timeout(
            self.config.checkpoint_timeout,
            decision_store.decision(epoch),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "[LDB-6040] decision read for prepared epoch {epoch} timed out after {:?}",
                self.config.checkpoint_timeout
            ))
        })?
        .map_err(|error| DbError::Checkpoint(format!("[LDB-6040] {error}")))?;

        match decision {
            Some(decision) if decision.checkpoint_id == checkpoint_id => {
                #[cfg(feature = "cluster")]
                if let Some(controller) = self.cluster_controller.as_ref() {
                    let local = matches!(
                        Self::match_follower_decision(
                            Some(&decision),
                            controller.instance_id().0,
                            epoch,
                            checkpoint_id,
                        )?,
                        FollowerDecisionMatch::Included
                    );
                    return Ok((true, local));
                }
                Ok((true, true))
            }
            Some(decision) => Err(DbError::Checkpoint(format!(
                "[LDB-6041] epoch {epoch} decision binds checkpoint {}, but highest prepared checkpoint is {checkpoint_id}",
                decision.checkpoint_id
            ))),
            None => Ok((false, false)),
        }
    }

    /// Reconcile the highest prepared manifest on startup.
    ///
    /// The exact decision is the sole commit authority. External sink publication is re-driven
    /// by the coordinated committer from sealed participant descriptors; the checkpoint
    /// coordinator only finalizes a decided manifest or force-rolls back an undecided prepare.
    pub async fn reconcile_prepared_on_init(&self) -> Result<(), DbError> {
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
        if last.durable_phase
            == laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase::Finalized
        {
            return Ok(());
        }

        let epoch = last.epoch;
        let checkpoint_id = last.checkpoint_id;

        let (globally_committed, locally_committed) =
            self.prepared_decision_status(epoch, checkpoint_id).await?;

        #[cfg(feature = "cluster")]
        let is_leader = self
            .cluster_controller
            .as_ref()
            .is_some_and(|cc| cc.is_leader());

        if locally_committed {
            info!(
                epoch,
                checkpoint_id, "finalizing exactly decided prepared checkpoint"
            );
            self.finalize_manifest(checkpoint_id).await?;
        } else {
            if globally_committed {
                warn!(
                    epoch,
                    checkpoint_id,
                    participant_id = self.store.participant_id(),
                    "[LDB-6035] prepared participant is excluded from the exact decision — \
                     force-rolling back local state"
                );
            } else {
                warn!(
                    epoch,
                    checkpoint_id,
                    "[LDB-6035] prepared epoch has no exact decision — force-rolling back"
                );
            }
            self.rollback_sinks(epoch).await?;
        }

        #[cfg(feature = "cluster")]
        if is_leader {
            let phase = if globally_committed {
                laminar_core::cluster::control::Phase::Commit
            } else {
                laminar_core::cluster::control::Phase::Abort
            };
            self.announce_if_leader(epoch, checkpoint_id, phase, None)
                .await;
        }

        Ok(())
    }

    /// No-op when not the leader. Errors are logged; worst case is a longer follower timeout.
    #[cfg(feature = "cluster")]
    async fn announce_if_leader(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        phase: laminar_core::cluster::control::Phase,
        min_watermark_ms: Option<i64>,
    ) {
        let Some(cc) = self.cluster_controller.as_ref() else {
            return;
        };
        if !cc.is_leader() {
            return;
        }
        let ann = laminar_core::cluster::control::BarrierAnnouncement {
            epoch,
            checkpoint_id,
            phase,
            flags: 0,
            min_watermark_ms,
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
    /// On quorum, returns the capture-time follower set and writes the cluster-wide min into
    /// `cluster_min_watermark` for the Commit announcement. On failure, announces Abort.
    #[cfg(feature = "cluster")]
    async fn await_prepare_quorum(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<Vec<laminar_core::cluster::discovery::NodeId>, String> {
        use laminar_core::cluster::control::Phase;
        let Some(cc) = self.cluster_controller.clone() else {
            return Ok(Vec::new());
        };
        if !cc.is_leader() {
            return Ok(Vec::new());
        }
        match Self::run_prepare_quorum(
            &cc,
            self.config.quorum_timeout,
            epoch,
            checkpoint_id,
            self.local_watermark_ms,
        )
        .await
        {
            Ok((merged, participants)) => {
                self.cluster_min_watermark = merged;
                Ok(participants)
            }
            Err(msg) => {
                self.announce_if_leader(epoch, checkpoint_id, Phase::Abort, None)
                    .await;
                Err(msg)
            }
        }
    }

    /// Returns a failure reason if any participant is suspected, draining, left, or missing.
    #[cfg(feature = "cluster")]
    fn unhealthy_participant(
        members: &[laminar_core::cluster::discovery::NodeInfo],
        participants: &[QuorumPeer],
    ) -> Option<String> {
        use laminar_core::cluster::discovery::NodeState;
        for &id in participants {
            match members.iter().find(|m| m.id.0 == id.0) {
                Some(node)
                    if matches!(
                        node.state,
                        NodeState::Suspected | NodeState::Left | NodeState::Draining
                    ) =>
                {
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

    /// Run the capture-quorum stage outside the coordinator mutex so pipelined tails can
    /// reach `Aligned` while an earlier epoch's durable tail holds the lock.
    ///
    /// Announces `Prepare`, waits for live-follower acks, returns the merged cluster-min
    /// watermark. Caller announces `Aligned` on success or `Abort` on failure.
    #[cfg(feature = "cluster")]
    pub(crate) async fn run_prepare_quorum(
        cc: &laminar_core::cluster::control::ClusterController,
        quorum_timeout: Duration,
        epoch: u64,
        checkpoint_id: u64,
        local_watermark_ms: Option<i64>,
    ) -> Result<(Option<i64>, Vec<laminar_core::cluster::discovery::NodeId>), String> {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, QuorumOutcome};

        if let Err(e) = cc
            .announce_barrier(&BarrierAnnouncement {
                epoch,
                checkpoint_id,
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            })
            .await
        {
            warn!(epoch, checkpoint_id, error = %e, "[LDB-6031] prepare announcement failed");
        }

        let mut followers = cc.live_instances();
        followers.retain(|id| *id != cc.instance_id());
        if followers.is_empty() {
            // Leader-only cluster; min is the leader's local watermark.
            if let Some(wm) = local_watermark_ms {
                cc.publish_cluster_min_watermark(wm);
            }
            return Ok((local_watermark_ms, Vec::new()));
        }

        let mut members_rx = cc.members_watch();

        let quorum_fut = cc.wait_for_quorum(epoch, &followers, quorum_timeout);
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

        let outcome = tokio::select! {
            o = quorum_fut => Ok(o),
            e = membership_fut => Err(e),
        };

        match outcome {
            Ok(QuorumOutcome::Reached {
                min_follower_watermark_ms,
                ref acks,
            }) => {
                cc.note_responsive(acks);
                let merged = match (local_watermark_ms, min_follower_watermark_ms) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };
                if let Some(wm) = merged {
                    cc.publish_cluster_min_watermark(wm);
                }
                Ok((merged, followers))
            }
            Ok(QuorumOutcome::TimedOut { missing, .. }) => {
                // Gossip can lag a hard kill; record the leader's faster signal so gate
                // fail-fasts kick in before each captured epoch burns its full timeout.
                cc.note_unresponsive(&missing);
                Err(format!(
                    "quorum timeout: {} follower(s) did not ack",
                    missing.len()
                ))
            }
            Ok(QuorumOutcome::Failed { failures }) => {
                let first = failures.first().map_or("unknown", |(_, msg)| msg.as_str());
                Err(format!(
                    "follower snapshot failed on {} peer(s): {first}",
                    failures.len()
                ))
            }
            Err(err_msg) => Err(format!("fail-fast: {err_msg}")),
        }
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
        for (name, data) in operator_states {
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

    /// Next epoch to be allocated.
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
        let attempt = self.allocator.allocate_until(deadline).await?;
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
    /// Every runtime mode rolls back sinks and begins the next local epoch. The caller publishes
    /// the cluster `Abort` before waiting for this coordinator's mutex, then supplies that same
    /// absolute cleanup deadline here.
    ///
    /// # Errors
    ///
    /// Returns an error when rollback or successor-epoch setup cannot complete by `deadline`.
    pub(crate) async fn abandon_epoch_until(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        error: String,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointResult, DbError> {
        let result = self.record_failed_epoch(checkpoint_id, epoch, Instant::now(), error);
        self.cleanup_failed_epoch_until(epoch, deadline).await?;
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
        let (epoch, checkpoint_id) = (ann.epoch, ann.checkpoint_id);
        let deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;

        // State is captured; ack so the leader can release the pipeline.
        let _ = tokio::time::timeout_at(
            deadline,
            cc.ack_barrier(&BarrierAck {
                epoch: ann.epoch,
                ok: true,
                error: None,
                local_watermark_ms: self.local_watermark_ms,
            }),
        )
        .await; // best effort; leader's quorum tolerates missed acks

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
        let (epoch, checkpoint_id) = (ann.epoch, ann.checkpoint_id);
        self.follower_prepare_acked_until(request, epoch, checkpoint_id, deadline)
            .await?;
        let committed = Self::await_follower_decision(
            &cc,
            self.decision_store.as_deref(),
            epoch,
            checkpoint_id,
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
        epoch: u64,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        use laminar_core::cluster::control::BarrierAck;

        // Monotonic: a late-finishing depth>1 tail must not walk ids back past a successor's.
        self.allocator.advance_epoch_to(epoch);

        if let Err(e) = self
            .follower_prepare(request, epoch, checkpoint_id, deadline)
            .await
        {
            if let Some(cc) = self.cluster_controller.clone() {
                cc.ack_barrier(&BarrierAck {
                    epoch,
                    ok: false,
                    error: Some(e.to_string()),
                    local_watermark_ms: self.local_watermark_ms,
                })
                .await
                .ok();
            }
            self.rollback_sinks(epoch).await.ok();
            self.phase = CheckpointPhase::Idle;
            // Open the next epoch so post-failure writes stay transactional (mirrors fail_epoch).
            self.begin_next_epoch_bounded().await;
            return Err(e);
        }
        Ok(())
    }

    /// Follower stage 2: wait for the leader's decision without holding the coordinator mutex.
    ///
    /// The durable decision is authoritative per participant: an included participant commits,
    /// while an excluded participant rolls back its late prepare. An explicit Abort is
    /// authoritative only while no decision exists. Timeouts and read failures leave the
    /// participant in-doubt and return an error; a prepared participant never guesses.
    ///
    /// # Errors
    /// Returns an error when the decision store is unavailable, contains a conflicting decision,
    /// a Commit announcement has no matching marker, or the decision remains in-doubt at timeout.
    #[cfg(feature = "cluster")]
    pub(crate) async fn await_follower_decision(
        cc: &laminar_core::cluster::control::ClusterController,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        epoch: u64,
        checkpoint_id: u64,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        let deadline = Instant::now() + decision_timeout;
        let participant_id = cc.instance_id().0;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Self::follower_decision_timeout(epoch, checkpoint_id));
            }

            // The durable marker is the commit authority, so poll it independently of the
            // best-effort control announcement. This also covers a leader that recorded the
            // decision and crashed before publishing Commit.
            match Self::has_matching_follower_decision(
                decision_store,
                participant_id,
                epoch,
                checkpoint_id,
                deadline,
            )
            .await?
            {
                FollowerDecisionMatch::Included => return Ok(true),
                FollowerDecisionMatch::Excluded => return Ok(false),
                FollowerDecisionMatch::Pending => {}
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
            let Some(announcement) =
                Self::wait_for_follower_announcement(cc, epoch, poll_wait).await
            else {
                continue;
            };
            if let Some(committed) = Self::resolve_follower_announcement(
                &announcement,
                decision_store,
                participant_id,
                epoch,
                checkpoint_id,
                deadline,
            )
            .await?
            {
                return Ok(committed);
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
    async fn wait_for_follower_announcement(
        cc: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
        poll_wait: Duration,
    ) -> Option<laminar_core::cluster::control::BarrierAnnouncement> {
        use laminar_core::cluster::control::Phase;

        tokio::time::timeout(
            poll_wait,
            cc.wait_for_barrier(
                |announcement| {
                    announcement.epoch > epoch
                        || (announcement.epoch == epoch
                            && matches!(announcement.phase, Phase::Commit | Phase::Abort))
                },
                poll_wait,
            ),
        )
        .await
        .ok()
        .flatten()
    }

    #[cfg(feature = "cluster")]
    async fn resolve_follower_announcement(
        announcement: &laminar_core::cluster::control::BarrierAnnouncement,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        participant_id: u64,
        epoch: u64,
        checkpoint_id: u64,
        deadline: Instant,
    ) -> Result<Option<bool>, DbError> {
        use laminar_core::cluster::control::Phase;

        if announcement.epoch == epoch && announcement.phase == Phase::Commit {
            return match Self::has_matching_follower_decision(
                decision_store,
                participant_id,
                epoch,
                checkpoint_id,
                deadline,
            )
            .await?
            {
                FollowerDecisionMatch::Included => Ok(Some(true)),
                FollowerDecisionMatch::Excluded => Ok(Some(false)),
                FollowerDecisionMatch::Pending => Err(DbError::Checkpoint(format!(
                    "[LDB-6045] Commit announcement for epoch {epoch}, checkpoint \
                     {checkpoint_id} has no matching durable decision"
                ))),
            };
        }
        if announcement.epoch == epoch && announcement.phase == Phase::Abort {
            return match Self::has_matching_follower_decision(
                decision_store,
                participant_id,
                epoch,
                checkpoint_id,
                deadline,
            )
            .await?
            {
                FollowerDecisionMatch::Included => {
                    warn!(
                        epoch,
                        checkpoint_id,
                        "Abort announcement conflicts with durable commit — commit wins",
                    );
                    Ok(Some(true))
                }
                FollowerDecisionMatch::Excluded | FollowerDecisionMatch::Pending => Ok(Some(false)),
            };
        }
        match Self::has_matching_follower_decision(
            decision_store,
            participant_id,
            epoch,
            checkpoint_id,
            deadline,
        )
        .await?
        {
            FollowerDecisionMatch::Included => {
                info!(
                    epoch,
                    checkpoint_id,
                    observed_epoch = announcement.epoch,
                    "newer epoch observed with commit marker present — committing",
                );
                return Ok(Some(true));
            }
            FollowerDecisionMatch::Excluded => {
                info!(
                    epoch,
                    checkpoint_id,
                    participant_id,
                    observed_epoch = announcement.epoch,
                    "newer epoch observed after participant exclusion — rolling back local prepare",
                );
                return Ok(Some(false));
            }
            FollowerDecisionMatch::Pending => {}
        }
        // A pipelined newer Prepare is not an abort decision for this epoch. Pace the durable
        // re-check so a cached gossip announcement cannot busy-spin.
        tokio::time::sleep(
            FOLLOWER_DECISION_POLL.min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
        Ok(None)
    }

    #[cfg(feature = "cluster")]
    async fn has_matching_follower_decision(
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        participant_id: u64,
        epoch: u64,
        checkpoint_id: u64,
        deadline: Instant,
    ) -> Result<FollowerDecisionMatch, DbError> {
        let store = decision_store.ok_or_else(|| {
            DbError::Checkpoint("[LDB-6045] cluster follower has no durable decision store".into())
        })?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6046] follower decision deadline expired before the durable read for epoch \
                 {epoch}, checkpoint {checkpoint_id}"
            )));
        }
        let decision = tokio::time::timeout(remaining, store.decision(epoch))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "[LDB-6046] durable decision read timed out for epoch {epoch}, checkpoint \
                     {checkpoint_id}"
                ))
            })?
            .map_err(|e| {
                DbError::Checkpoint(format!(
                    "[LDB-6045] failed to read durable decision for epoch {epoch}: {e}"
                ))
            })?;
        Self::match_follower_decision(decision.as_ref(), participant_id, epoch, checkpoint_id)
    }

    #[cfg(feature = "cluster")]
    fn match_follower_decision(
        decision: Option<&laminar_core::checkpoint_decision::CommitDecision>,
        participant_id: u64,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<FollowerDecisionMatch, DbError> {
        let Some(decision) = decision else {
            return Ok(FollowerDecisionMatch::Pending);
        };
        if decision.checkpoint_id != checkpoint_id {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6045] epoch {epoch} is durably bound to checkpoint {}, not participant \
                 checkpoint {checkpoint_id}",
                decision.checkpoint_id
            )));
        }
        if decision.scope != laminar_core::checkpoint_decision::CommitDecisionScope::Cluster {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6045] cluster participant {participant_id} observed a local-scope durable \
                 decision for epoch {epoch}, checkpoint {checkpoint_id}"
            )));
        }
        if decision.participants.binary_search(&participant_id).is_ok() {
            Ok(FollowerDecisionMatch::Included)
        } else {
            Ok(FollowerDecisionMatch::Excluded)
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
        let clean = if committed {
            // Followers never publish external sink state. The exact decision makes their
            // prepared state recoverable; finalization merely publishes the local recovery cut.
            self.finalize_manifest(checkpoint_id).await.map_err(|e| {
                DbError::Checkpoint(format!(
                    "[LDB-6048] follower could not finalize decided epoch {epoch}, checkpoint \
                     {checkpoint_id}: {e}"
                ))
            })?;
            self.checkpoints_completed += 1;
            self.allocator.advance_epoch_to(epoch.saturating_add(1));
            self.phase = CheckpointPhase::Idle;
            // The shared backend and decision namespace are leader-owned, but each follower owns
            // a participant-specific manifest namespace and must bound that local inventory too.
            let horizon = epoch.saturating_sub(self.config.max_retained as u64);
            if horizon > 0 {
                self.schedule_local_manifest_retention_prune(horizon, epoch);
            }
            true
        } else {
            self.rollback_sinks(epoch).await.ok();
            self.checkpoints_failed += 1;
            self.phase = CheckpointPhase::Idle;
            false
        };
        // Both paths close the sinks' open transaction; open the next epoch (mirrors fail_epoch).
        self.begin_next_epoch_bounded().await;
        Ok(clean)
    }

    /// Commit-marker store handle for the lock-free decision wait in pipelined follower tails.
    #[cfg(feature = "cluster")]
    pub(crate) fn decision_store_handle(
        &self,
    ) -> Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>> {
        self.decision_store.clone()
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
        let CheckpointRequest {
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

        let mut manifest = CheckpointManifest::new_with_vnode_count(
            checkpoint_id,
            epoch,
            self.store.vnode_count(),
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
        let manifest = match self.save_manifest(Arc::new(manifest), state_data).await {
            Ok(manifest) => manifest,
            Err(error) => {
                self.pending_vnode_states.clear();
                return Err(error);
            }
        };
        if let Err(e) = self.write_vnode_partials(epoch, checkpoint_id).await {
            self.pending_vnode_states.clear();
            return Err(e);
        }
        if let Err(error) = self
            .persist_participant_ready_until(
                CheckpointAttempt::new(epoch, checkpoint_id),
                &manifest,
                deadline,
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
    ) -> Result<CheckpointResult, DbError> {
        let CheckpointRequest {
            operator_states,
            watermark,
            table_store_checkpoint_path,
            extra_table_offsets,
            source_watermarks,
            source_offset_overrides,
        } = request;
        // Flink-style: ids are allocated up front; a failed epoch is abandoned, never retried.
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);

        #[cfg(feature = "cluster")]
        let checkpoint_leadership = match self.capture_checkpoint_leadership() {
            Ok(captured) => captured,
            Err(error) => {
                return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
            }
        };

        info!(checkpoint_id, epoch, "starting checkpoint");

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
                match self.await_prepare_quorum(epoch, checkpoint_id).await {
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
                    self.cluster_min_watermark,
                )
                .await;
            }
            QuorumStage::Done {
                min_watermark_ms,
                participants,
            } => {
                self.cluster_min_watermark = min_watermark_ms;
                quorum_participants = participants;
            }
        }
        #[cfg(not(feature = "cluster"))]
        let _ = quorum;

        // Phase 1 creates prepared sink transactions. Once leadership loss is observed, this
        // attempt performs no more connector or durable mutations; recovery owns cleanup.
        #[cfg(feature = "cluster")]
        if let Err(error) = self.ensure_checkpoint_leadership(checkpoint_leadership, "sink phase 1")
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
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership, "manifest persistence")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let mut manifest = CheckpointManifest::new_with_vnode_count(
            checkpoint_id,
            epoch,
            self.store.vnode_count(),
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
        let persisted_manifest = match self.save_manifest(Arc::new(manifest), state_data).await {
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

        if let Err(e) = self.write_vnode_partials(epoch, checkpoint_id).await {
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
        let gate_start = Instant::now();
        let gate_result = self
            .await_restorable_gate(
                CheckpointAttempt::new(epoch, checkpoint_id),
                &quorum_participants,
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

        // The durable decision is the sole commit point. External sinks publish later from the
        // exact sealed descriptor inventory; no connector phase-2 mutation occurs inline here.
        #[cfg(feature = "cluster")]
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership, "durable commit decision")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let is_decision_leader = self.is_designated_commit_leader();
        if is_decision_leader {
            let Some(ds) = self.decision_store.as_ref() else {
                return Ok(self
                    .fail_epoch(
                        checkpoint_id,
                        epoch,
                        start,
                        "[LDB-6050] checkpoint decision store is not bound".into(),
                    )
                    .await);
            };
            self.phase = CheckpointPhase::Deciding;
            // Set this before issuing I/O. A timeout/error cannot prove the create was absent.
            self.decision_write_started = true;
            let timeout = self.config.checkpoint_timeout;
            #[cfg(feature = "cluster")]
            let decision_participants = self.checkpoint_participant_ids(&quorum_participants);
            let decision_write = async {
                #[cfg(feature = "cluster")]
                if self.cluster_controller.is_some() {
                    return ds
                        .record_committed_for_participants(
                            epoch,
                            checkpoint_id,
                            &decision_participants,
                            self.self_node_id(),
                            self.assignment_version,
                        )
                        .await;
                }
                ds.record_committed(epoch, checkpoint_id).await
            };
            match tokio::time::timeout(timeout, decision_write).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    error!(checkpoint_id, epoch, %error, "[LDB-6038] commit decision write failed ambiguously");
                    return Ok(self.fail_after_irrevocable_work(
                        checkpoint_id,
                        epoch,
                        start,
                        format!("commit decision write failed ambiguously: {error}"),
                    ));
                }
                Err(_) => {
                    let error = format!("commit decision write timed out after {timeout:?}");
                    error!(checkpoint_id, epoch, %error, "[LDB-6038] commit decision write timed out ambiguously");
                    return Ok(self.fail_after_irrevocable_work(
                        checkpoint_id,
                        epoch,
                        start,
                        error,
                    ));
                }
            }
            self.highest_decided = self.highest_decided.max(epoch);
        }

        // The decision-store create and the leader lease are separate operations. This check
        // cannot fence the write (cluster EO is therefore not admitted), but it guarantees a
        // process that observes loss after the decision does not finalize or acknowledge sources.
        #[cfg(feature = "cluster")]
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership, "manifest finalization")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        if let Err(e) = self.finalize_manifest(checkpoint_id).await {
            // The exact decision is irrevocable and the Prepared manifest already contains the
            // source/state cut. Recovery finalizes this exact pair; live rollback is forbidden.
            error!(
                checkpoint_id,
                epoch,
                error = %e,
                "[LDB-6047] commit decided but manifest finalization failed; recovery will repair"
            );
        }

        // Finalization can outlive a lease. A finalized artifact is safe for the next leader to
        // recover, but the stale task must still return failure so source offsets are not acked.
        #[cfg(feature = "cluster")]
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership, "checkpoint completion")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        #[cfg(feature = "cluster")]
        self.announce_if_leader(
            epoch,
            checkpoint_id,
            laminar_core::cluster::control::Phase::Commit,
            self.cluster_min_watermark,
        )
        .await;

        // `announce_if_leader` intentionally degrades to a no-op after demotion. Re-check here so
        // that observation cannot be followed by success accounting or source acknowledgement.
        #[cfg(feature = "cluster")]
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership, "post-decision maintenance")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        // Completion latency ends at the durable decision/finalization/announcement boundary;
        // retention cleanup and successor-epoch setup are maintenance, not checkpoint latency.
        let committed_duration = start.elapsed();

        if let Some(ref m) = self.prom {
            #[allow(clippy::cast_possible_wrap)]
            m.checkpoint_size_bytes.set(checkpoint_bytes as i64);
        }

        // Prune old partials/markers outside the retention window. Leader-gated:
        // the state backend is shared in cluster mode, so the leader (which
        // advances the committer floor) owns GC; a follower's floor stays 0.
        if self.is_designated_commit_leader() {
            let mut horizon = epoch.saturating_sub(self.config.max_retained as u64);
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
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership, "successor sink epoch")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        let next_epoch = self.allocator.peek_epoch();
        let begin_epoch_error = match self
            .begin_epoch_for_sinks_until(next_epoch, attempt_deadline)
            .await
        {
            Ok(()) => None,
            Err(e) => {
                error!(
                    next_epoch,
                    error = %e,
                    "[LDB-6015] failed to begin next epoch — pipeline must stop before further writes"
                );
                Some(format!(
                    "checkpoint {checkpoint_id} epoch {epoch} committed, but successor sink \
                     epoch {next_epoch} failed to begin: {e}"
                ))
            }
        };

        // `begin_epoch_for_sinks` is asynchronous; lease expiry while it runs must still turn the
        // completion into a failure before the caller can publish/ack the source cut.
        #[cfg(feature = "cluster")]
        if let Err(error) =
            self.ensure_checkpoint_leadership(checkpoint_leadership, "successful completion")
        {
            return Ok(self.fail_after_irrevocable_work(checkpoint_id, epoch, start, error));
        }

        self.phase = CheckpointPhase::Idle;
        self.decision_write_started = false;
        self.checkpoints_completed += 1;
        self.total_bytes_written += checkpoint_bytes;
        let duration = committed_duration;
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(true, epoch, duration);

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
        })
    }

    /// Recover from the latest stored checkpoint.
    ///
    /// Returns `Ok(None)` for a fresh start (no checkpoint found).
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if the store read fails.
    #[cfg(feature = "cluster")]
    fn validate_manifest_source_handoff(
        manifest: &CheckpointManifest,
        handoff: &HashMap<String, HashMap<String, String>>,
    ) -> Result<(), DbError> {
        for (source, checkpoint) in &manifest.source_offsets {
            let sealed = handoff.get(source).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] participant manifest source '{source}' is absent from the sealed source handoff"
                ))
            })?;
            for (key, value) in &checkpoint.offsets {
                if sealed.get(key) != Some(value) {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6041] participant manifest source '{source}' offset '{key}' does not match the sealed source handoff"
                    )));
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn validate_recovered_cluster_cut(
        &self,
        recovered: &mut crate::recovery_manager::RecoveredState,
    ) -> Result<(), DbError> {
        if self.active_decision_scope()
            != laminar_core::checkpoint_decision::CommitDecisionScope::Cluster
        {
            return Ok(());
        }
        let Some(decision) = recovered.decision() else {
            return Ok(());
        };
        if decision.scope != laminar_core::checkpoint_decision::CommitDecisionScope::Cluster {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] cluster recovery observed a {:?} durable decision",
                decision.scope
            )));
        }
        let backend = self.state_backend.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6050] cluster decision recovery requires the sealed state backend".into(),
            )
        })?;
        let attempt = CheckpointAttempt::new(decision.epoch, decision.checkpoint_id);
        let inventory = backend
            .checkpoint_seal_inventory(attempt)
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!("checkpoint seal inventory read failed: {error}"))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6041] decided epoch {} checkpoint {} has no exact state seal",
                    decision.epoch, decision.checkpoint_id
                ))
            })?;
        if inventory.assignment_version != decision.assignment_version {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] decided epoch {} checkpoint {} assignment version {} does not match \
                 sealed version {}",
                decision.epoch,
                decision.checkpoint_id,
                decision.assignment_version,
                inventory.assignment_version
            )));
        }
        let sealed_participants: std::collections::BTreeSet<u64> = inventory
            .required_descriptors
            .iter()
            .filter_map(|key| participant_from_ready_key(key))
            .collect();
        let decided_participants: std::collections::BTreeSet<u64> =
            decision.participants.iter().copied().collect();
        if sealed_participants != decided_participants {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6041] decided participants {decided_participants:?} do not match sealed readiness participants {sealed_participants:?}"
            )));
        }
        // Decode and validate every readiness payload even for pipelines without replayable
        // sources; the marker is also the manifest-completion attestation.
        let handoff = self.source_offsets_at(attempt).await?;
        Self::validate_manifest_source_handoff(&recovered.manifest, &handoff)?;
        recovered.set_cluster_source_handoff(handoff);
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
            .with_decision_scope(self.active_decision_scope());
        let mut result = mgr.recover(self.decision_store.as_deref()).await?;

        if let Some(ref mut recovered) = result {
            #[cfg(feature = "cluster")]
            self.validate_recovered_cluster_cut(recovered).await?;
            self.allocator.advance_epoch_to(recovered.epoch() + 1);
            info!(
                epoch = self.allocator.peek_epoch(),
                "coordinator epoch set after recovery"
            );
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
            .with_decision_scope(self.active_decision_scope());
        let mut result = mgr
            .recover_to_epoch(target_epoch, self.decision_store.as_deref())
            .await?;

        if let Some(ref mut recovered) = result {
            #[cfg(feature = "cluster")]
            self.validate_recovered_cluster_cut(recovered).await?;
            self.allocator.advance_epoch_to(recovered.epoch() + 1);
            info!(
                epoch = self.allocator.peek_epoch(),
                "coordinator epoch set after coordinated recovery"
            );
        }

        Ok(result)
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
    }
}

/// Convert a `ConnectorCheckpoint` back to a `SourceCheckpoint`.
#[must_use]
pub(crate) fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source_cp = SourceCheckpoint::with_offsets(cp.offsets.clone());
    for (k, v) in &cp.metadata {
        source_cp.set_metadata(k.clone(), v.clone());
    }
    source_cp
}

#[cfg(test)]
mod tests;
