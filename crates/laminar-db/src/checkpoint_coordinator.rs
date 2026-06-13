//! Unified checkpoint coordinator.
//!
//! Single orchestrator for checkpoint lifecycle. Lives in Ring 2 (control plane).
//!
//! The checkpoint manifest is the source of truth for source offsets.
//! Kafka broker commits are advisory (for monitoring tools). On recovery,
//! offsets restore from manifest, not consumer group state.
//!
//! ## Checkpoint Cycle
//!
//! 1. Barrier injection — `CheckpointBarrierInjector.trigger()`
//! 2. Operator snapshot — `OperatorGraph.snapshot_state()` → operator states
//! 3. Source snapshot — `source.checkpoint()` for each source
//! 4. Sink pre-commit — `sink.pre_commit(epoch)` for each exactly-once sink
//! 5. Manifest persist — `store.save(&manifest)` (atomic write)
//! 6. Sink commit — `sink.commit_epoch(epoch)` for each exactly-once sink
//! 7. On ANY failure at 6 — `sink.rollback_epoch()` on remaining sinks
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::SourceConnector;
use laminar_core::state::StateBackend;
use laminar_core::storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, SinkCommitStatus,
};
use laminar_core::storage::checkpoint_store::CheckpointStore;
use tracing::{debug, error, info, warn};

use crate::error::DbError;

/// One operator's staged slice for one vnode of the next checkpoint.
///
/// Both variants are only constructed on the per-vnode capture path
/// (`cluster`); without it the coordinator still carries the type but never
/// builds one.
#[cfg_attr(not(feature = "cluster"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum StagedSlice {
    /// Freshly serialized memory-resident state.
    Bytes(bytes::Bytes),
    /// Demoted to the cold tier and untouched since the upload recorded in
    /// `last_vnode_uploads`: no bytes staged. The coordinator emits a
    /// reference partial, or fetches the slice back from the tier when a
    /// full re-upload is forced.
    Cold,
}

/// Per-vnode staged slices, `vnode → operator → slice`.
pub(crate) type StagedVnodeStates = HashMap<u32, HashMap<String, StagedSlice>>;

/// What `last_vnode_uploads` records per operator slice of the last full
/// upload: the exact bytes (the reference-partial comparison base, and the
/// byte source for coordinator-driven demotion), or a cold marker once the
/// slice was demoted — the bytes then live only in the tier, releasing the
/// in-memory pin.
/// `Cold` is only recorded when a demotion lands (`state-tier`); the plain
/// `cluster` build records bytes but never a cold marker.
#[cfg_attr(not(feature = "state-tier"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum UploadedSlice {
    Bytes(bytes::Bytes),
    Cold,
}

impl UploadedSlice {
    /// Whether `staged` proves the slice unchanged since this upload.
    ///
    /// `Cold` staged ⇒ unchanged by the demotion contract (a demoted slice
    /// is byte-identical to its recorded upload, and any row for it
    /// promotes the slice back to memory before processing). Fresh bytes
    /// against a cold record are conservatively "changed" — the cold bytes
    /// aren't here to compare, so the slice re-uploads full once.
    fn matches(&self, staged: &StagedSlice) -> bool {
        match (staged, self) {
            (StagedSlice::Cold, _) => true,
            (StagedSlice::Bytes(b), UploadedSlice::Bytes(prev)) => b == prev,
            (StagedSlice::Bytes(_), UploadedSlice::Cold) => false,
        }
    }
}

/// Unified checkpoint configuration.
///
/// Timeouts prevent a stuck sink or hung filesystem from stalling the
/// runtime. `state_inline_threshold` decides per-operator whether state
/// inlines as base64 in the JSON manifest or lands in a sidecar file.
/// `max_checkpoint_bytes` caps total sidecar size; an oversized
/// checkpoint is rejected with `[LDB-6014]`.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Interval between checkpoints. `None` = manual only.
    pub interval: Option<Duration>,
    /// Number of completed checkpoints retained on disk/in object store.
    pub max_retained: usize,
    /// Upper bound on barrier-alignment wait at fan-in operators.
    pub alignment_timeout: Duration,
    /// Upper bound on sink pre-commit (phase 1).
    pub pre_commit_timeout: Duration,
    /// Upper bound on manifest persist.
    pub persist_timeout: Duration,
    /// Upper bound on sink commit (phase 2).
    pub commit_timeout: Duration,
    /// Upper bound on sink rollback.
    pub rollback_timeout: Duration,
    /// States larger than this go to a sidecar rather than base64 JSON.
    pub state_inline_threshold: usize,
    /// Upper bound on operator state serialization.
    pub serialization_timeout: Duration,
    /// Cap on total sidecar bytes; `None` = no limit. 80% warn threshold.
    pub max_checkpoint_bytes: Option<usize>,
    /// Quorum wait timeout for checkpoint coordination.
    pub quorum_timeout: Duration,
    /// Max wait for every participating vnode's partial to land before
    /// the epoch is declared restorable. Followers ack at *capture* and
    /// upload asynchronously after acking, so the
    /// leader's durability gate polls for partial presence instead of
    /// checking once. Expiry aborts the epoch.
    pub restorable_gate_timeout: Duration,
    /// Maximum epochs admitted between `Aligned` and restorable — the
    /// upload backlog. Exactly-once pipelines are
    /// capped at 1: a single-open-transaction sink (e.g. a Kafka
    /// transactional producer) cannot overlap epochs.
    pub max_in_flight_epochs: u64,
    /// Cap on captured-state bytes held in memory by in-flight epochs
    /// awaiting upload. At the cap, barrier admission pauses — cadence
    /// degrades to upload speed instead of exhausting memory.
    pub max_staged_bytes: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Some(Duration::from_secs(60)),
            max_retained: 3,
            alignment_timeout: Duration::from_secs(30),
            pre_commit_timeout: Duration::from_secs(30),
            persist_timeout: Duration::from_secs(120),
            commit_timeout: Duration::from_secs(60),
            rollback_timeout: Duration::from_secs(30),
            serialization_timeout: Duration::from_secs(120),
            state_inline_threshold: 1_048_576,
            max_checkpoint_bytes: None,
            quorum_timeout: Duration::from_secs(3),
            // Last-resort bound only: membership/unresponsive/rotation
            // fail-fasts catch dead participants in seconds. Long values
            // serialize recovery (pipelined doomed epochs each burn it).
            restorable_gate_timeout: Duration::from_secs(10),
            max_in_flight_epochs: 4,
            max_staged_bytes: 512 * 1024 * 1024,
        }
    }
}

/// Parameters for a checkpoint operation.
#[derive(Debug, Clone, Default)]
pub struct CheckpointRequest {
    /// Serialized operator states. `Bytes` (not `Vec<u8>`) so producers
    /// (rkyv output, MV IPC bytes) can hand off the buffer without an
    /// extra copy at each stage of the checkpoint pipeline.
    pub operator_states: HashMap<String, bytes::Bytes>,
    /// Current watermark timestamp.
    pub watermark: Option<i64>,
    /// Path for table store checkpoint data.
    pub table_store_checkpoint_path: Option<String>,
    /// Additional table offset overrides.
    pub extra_table_offsets: HashMap<String, ConnectorCheckpoint>,
    /// Per-source watermark timestamps.
    pub source_watermarks: HashMap<String, i64>,
    /// Pipeline topology hash for change detection.
    pub pipeline_hash: Option<u64>,
    /// Source offset overrides for recovery.
    pub source_offset_overrides: HashMap<String, ConnectorCheckpoint>,
}

/// Lock-free epoch/checkpoint-id allocator.
///
/// Ids are handed out at barrier admission — possibly while an earlier
/// epoch's durable tail still holds the coordinator mutex — so they
/// live outside the coordinator's exclusive state. Failed epochs are
/// abandoned, never re-allocated.
#[derive(Debug)]
pub(crate) struct EpochAllocator {
    epoch: std::sync::atomic::AtomicU64,
    next_checkpoint_id: std::sync::atomic::AtomicU64,
}

impl EpochAllocator {
    fn new(epoch: u64, next_checkpoint_id: u64) -> Self {
        Self {
            epoch: std::sync::atomic::AtomicU64::new(epoch),
            next_checkpoint_id: std::sync::atomic::AtomicU64::new(next_checkpoint_id),
        }
    }

    /// Claim the next `(epoch, checkpoint_id)` pair.
    ///
    /// The two counters advance independently (not as one atomic unit):
    /// every allocation site runs on the pipeline task or under the
    /// coordinator mutex, so concurrent `allocate` calls cannot occur
    /// today. A new call site off those paths would need this upgraded
    /// to a single CAS over a packed pair.
    pub(crate) fn allocate(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.epoch.fetch_add(1, Ordering::AcqRel),
            self.next_checkpoint_id.fetch_add(1, Ordering::AcqRel),
        )
    }

    /// The pair the next [`allocate`](Self::allocate) would return.
    pub(crate) fn peek(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.epoch.load(Ordering::Acquire),
            self.next_checkpoint_id.load(Ordering::Acquire),
        )
    }

    /// Monotonic advance — ids never walk backwards. An aborted epoch
    /// leaves artifacts behind (Pending manifest, uploaded partials);
    /// recovery restoring an OLDER committed epoch must not pull ids
    /// back down onto them and re-allocate the aborted epoch.
    fn advance_to(&self, epoch: u64, next_checkpoint_id: u64) {
        use std::sync::atomic::Ordering;
        self.epoch.fetch_max(epoch, Ordering::AcqRel);
        self.next_checkpoint_id
            .fetch_max(next_checkpoint_id, Ordering::AcqRel);
    }
}

/// Capture-quorum participant id. Aliased so non-cluster builds (where
/// `laminar_core::cluster` is compiled out and participant sets are
/// always empty) still type-check the shared plumbing.
#[cfg(feature = "cluster")]
pub(crate) type QuorumPeer = laminar_core::cluster::discovery::NodeId;
#[cfg(not(feature = "cluster"))]
pub(crate) type QuorumPeer = u64;

/// Whether `checkpoint_inner` still needs to run the cluster capture
/// quorum, or a pipelined tail already ran it before taking the
/// coordinator mutex.
#[derive(Debug, Clone)]
pub(crate) enum QuorumStage {
    /// Run the quorum + `Aligned` announce inline (forced/timer paths).
    RunInline,
    /// Already reached before the coordinator lock; carries the merged
    /// cluster-min watermark for the `Commit` announcement plus the
    /// capture-time follower set, so the durability gate can fail fast
    /// if one of them dies instead of burning its full timeout.
    #[cfg_attr(not(feature = "cluster"), allow(dead_code))]
    Done {
        /// Merged cluster-min watermark from the capture acks.
        min_watermark_ms: Option<i64>,
        /// Followers that acked the capture quorum.
        participants: Vec<QuorumPeer>,
    },
}

/// Phase of the checkpoint lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum CheckpointPhase {
    /// No checkpoint in progress.
    Idle,
    /// Operators snapshotted, collecting source positions.
    Snapshotting,
    /// Sinks pre-committing (phase 1).
    PreCommitting,
    /// Manifest being persisted.
    Persisting,
    /// Sinks committing (phase 2).
    Committing,
}

impl std::fmt::Display for CheckpointPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::Snapshotting => write!(f, "Snapshotting"),
            Self::PreCommitting => write!(f, "PreCommitting"),
            Self::Persisting => write!(f, "Persisting"),
            Self::Committing => write!(f, "Committing"),
        }
    }
}

/// Result of a checkpoint attempt.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CheckpointResult {
    /// Whether the checkpoint succeeded.
    pub success: bool,
    /// Checkpoint ID (if created).
    pub checkpoint_id: u64,
    /// Epoch number.
    pub epoch: u64,
    /// Duration of the checkpoint operation.
    pub duration: Duration,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Registered source for checkpoint coordination.
pub(crate) struct RegisteredSource {
    /// Source name.
    pub name: String,
    /// Source connector handle.
    pub connector: Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
    /// Whether this source supports replay from a checkpointed position.
    ///
    /// Sources that do not support replay (e.g., WebSocket) degrade
    /// exactly-once semantics to at-most-once.
    pub supports_replay: bool,
}

/// Registered sink for checkpoint coordination.
pub(crate) struct RegisteredSink {
    /// Sink name.
    pub name: String,
    /// Sink task handle (channel-based, no mutex contention).
    pub handle: crate::sink_task::SinkTaskHandle,
    /// Whether this sink supports exactly-once / two-phase commit.
    pub exactly_once: bool,
}

/// Unified checkpoint coordinator.
///
/// Orchestrates the full checkpoint lifecycle across sources, sinks,
/// and operator state, persisting everything in a single
/// [`CheckpointManifest`].
pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    store: Arc<dyn CheckpointStore>,
    sinks: Vec<RegisteredSink>,
    /// Shared with the pipeline callback, which allocates ids at
    /// barrier admission without taking the coordinator mutex.
    allocator: Arc<EpochAllocator>,
    phase: CheckpointPhase,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    last_checkpoint_duration: Option<Duration>,
    duration_histogram: DurationHistogram,
    prom: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    total_bytes_written: u64,
    /// Consulted between manifest persist and sink commit to verify
    /// per-vnode durability.
    state_backend: Option<Arc<dyn StateBackend>>,
    /// Stamped into every `write_partial` for the split-brain fence.
    /// Zero = fence disabled.
    assignment_version: u64,
    /// Durable commit marker, written before sinks are told to commit
    /// so recovery can tell the 2PC verdict apart from a mid-flight
    /// crash. `None` falls back to "rollback on recovery".
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    /// Reported in each `BarrierAck`; the leader folds these with its
    /// own watermark to compute the cluster-wide min.
    local_watermark_ms: Option<i64>,
    /// Cluster-wide min watermark as of the last committed epoch
    /// (leader-side; fanned out in the Commit announcement).
    #[cfg(feature = "cluster")]
    cluster_min_watermark: Option<i64>,
    /// Vnodes this coordinator owns; drives per-vnode marker writes.
    vnode_set: Vec<u32>,
    /// Vnodes the leader's durability gate checks. In cluster mode the
    /// full registry; single-instance mirrors `vnode_set`.
    gate_vnode_set: Vec<u32>,
    /// First epoch admitted at-or-after the latest vnode rotation. An
    /// in-flight epoch BELOW this captured under the previous
    /// assignment: vnodes that changed hands mid-epoch were captured by
    /// nobody, so its durability gate can never seal — fail it fast
    /// instead of burning the gate timeout (with pipelining, several
    /// such epochs would burn serially).
    rotation_epoch_floor: u64,
    /// Per-vnode operator-state slices for the in-flight checkpoint,
    /// `vnode → (operator_name → bytes)`. Set fresh before each checkpoint
    /// by the pipeline callback from `OperatorGraph::snapshot_state_by_vnode`;
    /// folded into each owned vnode's `partial.bin` by
    /// [`write_vnode_partials`](Self::write_vnode_partials). Empty in
    /// single-instance mode (the partial is then just a durability marker).
    #[allow(clippy::disallowed_types)] // matches the graph snapshot shape
    pending_vnode_states: StagedVnodeStates,
    /// Per-vnode `(epoch, slices)` of the last *full* partial uploaded —
    /// the bases for unchanged-vnode reference partials. Bytes are
    /// refcounted, so this holds one serialized
    /// snapshot's worth of memory, not a deep copy per epoch; demoted
    /// slices hold a cold marker instead (their bytes live in the tier).
    #[allow(clippy::disallowed_types)]
    last_vnode_uploads:
        std::collections::HashMap<u32, (u64, std::collections::HashMap<String, UploadedSlice>)>,
    /// Cold-tier request channel; lets a forced full re-upload of a
    /// demoted slice fetch its bytes back from the tier.
    #[cfg(feature = "state-tier")]
    state_tier: Option<crate::state_tier::TierTx>,
    /// `Some` in cluster mode, `None` in single-instance / embedded.
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Cached sorted sink names; invalidated on `register_sink`.
    cached_sorted_sink_names: Option<Vec<String>>,
}

/// Highest-loadable manifest — tolerates a torn `latest.txt` pointer.
async fn load_highest(
    store: &dyn CheckpointStore,
) -> Result<Option<CheckpointManifest>, laminar_core::storage::checkpoint_store::CheckpointStoreError>
{
    let ids = store.list_ids().await?;
    for id in ids.iter().rev() {
        if let Ok(Some(m)) = store.load_by_id(*id).await {
            return Ok(Some(m));
        }
    }
    Ok(None)
}

impl CheckpointCoordinator {
    /// Creates a new checkpoint coordinator, seeded from the highest
    /// loadable stored checkpoint.
    ///
    /// # Errors
    /// Surfaces a store read failure rather than silently starting at
    /// `(1, 1)` and clobbering on-disk state.
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
        let (next_id, epoch) = match highest.as_ref() {
            Some(m) => (m.checkpoint_id + 1, m.epoch + 1),
            None => (1, 1),
        };

        Ok(Self {
            config,
            store,
            sinks: Vec::new(),
            allocator: Arc::new(EpochAllocator::new(epoch, next_id)),
            phase: CheckpointPhase::Idle,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            prom: None,
            total_bytes_written: 0,
            state_backend: None,
            assignment_version: 0,
            decision_store: None,
            local_watermark_ms: None,
            #[cfg(feature = "cluster")]
            cluster_min_watermark: None,
            vnode_set: Vec::new(),
            gate_vnode_set: Vec::new(),
            rotation_epoch_floor: 0,
            pending_vnode_states: std::collections::HashMap::new(),
            last_vnode_uploads: std::collections::HashMap::new(),
            #[cfg(feature = "state-tier")]
            state_tier: None,
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            cached_sorted_sink_names: None,
        })
    }

    /// Activates cluster-mode 2PC. Without this the coordinator runs
    /// single-instance semantics.
    #[cfg(feature = "cluster")]
    pub fn set_cluster_controller(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        self.cluster_controller = Some(controller);
    }

    /// Wired with a non-empty `vnode_set` to enable per-vnode markers
    /// and the `epoch_complete` durability gate.
    pub fn set_state_backend(&mut self, backend: Arc<dyn StateBackend>) {
        self.state_backend = Some(backend);
    }

    /// Install the durable commit-marker store.
    pub fn set_decision_store(
        &mut self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) {
        self.decision_store = Some(store);
    }

    /// Record the assignment generation this coordinator is writing
    /// with. Forwarded to `backend.write_partial` so the split-brain
    /// fence can reject stale writers. Host sets this whenever a fresh
    /// `AssignmentSnapshot` rotates in.
    pub fn set_assignment_version(&mut self, version: u64) {
        self.assignment_version = version;
    }

    /// Record this instance's current local watermark, reported in
    /// every subsequent `BarrierAck` so the leader can compute the
    /// cluster-wide minimum. `None` disables the per-follower
    /// contribution — leader falls back to its own watermark (and
    /// the other followers').
    pub fn set_local_watermark_ms(&mut self, watermark: Option<i64>) {
        self.local_watermark_ms = watermark;
    }

    /// Stage the per-vnode operator-state slices for the next checkpoint.
    ///
    /// Each owned vnode's slice is folded into its `partial.bin` by
    /// `write_vnode_partials` so a node that
    /// later acquires the vnode can rehydrate exactly that vnode's state.
    /// Call once per checkpoint (even with an empty map) so a prior epoch's
    /// slices never leak forward.
    #[allow(clippy::disallowed_types)]
    pub(crate) fn set_pending_vnode_states(&mut self, states: StagedVnodeStates) {
        self.pending_vnode_states = states;
    }

    /// Wire the cold-tier request channel (forced full re-uploads of
    /// demoted slices fetch their bytes through it).
    #[cfg(feature = "state-tier")]
    #[allow(dead_code)] // wired once the demotion trigger lands with promotion
    pub(crate) fn set_state_tier(&mut self, tier: crate::state_tier::TierTx) {
        self.state_tier = Some(tier);
    }

    /// Vnodes this instance owns; drives marker writes. Also the
    /// default gate set until [`Self::set_gate_vnode_set`] is called.
    pub fn set_vnode_set(&mut self, vnodes: Vec<u32>) {
        if self.gate_vnode_set.is_empty() {
            self.gate_vnode_set.clone_from(&vnodes);
        }
        self.rotation_epoch_floor = self.allocator.peek().0;
        // Drop reference bases for vnodes shed in a rebalance — they
        // hold refcounts on serialized state this node no longer owns
        // (and the new owner builds its own bases from a full upload).
        self.last_vnode_uploads.retain(|v, _| vnodes.contains(v));
        self.vnode_set = vnodes;
    }

    /// Set the vnodes the leader's durability gate checks (the full
    /// registry in cluster mode). Defaults to `vnode_set` when unset.
    pub fn set_gate_vnode_set(&mut self, vnodes: Vec<u32>) {
        self.gate_vnode_set = vnodes;
    }

    /// Fetch a demoted slice's bytes back from the cold tier for a forced
    /// full re-upload.
    #[cfg(feature = "state-tier")]
    async fn fetch_cold_slice(&self, operator: &str, vnode: u32) -> Result<bytes::Bytes, DbError> {
        let Some(ref tier) = self.state_tier else {
            return Err(DbError::Checkpoint(format!(
                "cold slice staged but no state tier is wired \
                 (operator={operator}, vnode={vnode})"
            )));
        };
        let (reply, rx) = tokio::sync::oneshot::channel();
        tier.send(crate::state_tier::TierRequest::Fetch {
            operator: Arc::from(operator),
            vnode,
            reply,
        })
        .await
        .map_err(|_| DbError::Checkpoint("state tier worker is gone".to_string()))?;
        match rx
            .await
            .map_err(|_| DbError::Checkpoint("state tier worker dropped the reply".to_string()))??
        {
            Some(bytes) => Ok(bytes),
            None => Err(DbError::Checkpoint(format!(
                "demoted slice missing from the state tier \
                 (operator={operator}, vnode={vnode}) — failing the epoch \
                 rather than dropping it from recovery truth"
            ))),
        }
    }

    /// `state-tier` off: a `Cold` slice can never be staged, so reaching
    /// this is a logic error reported as a failed epoch.
    #[cfg(not(feature = "state-tier"))]
    #[allow(clippy::unused_async)]
    async fn fetch_cold_slice(&self, operator: &str, vnode: u32) -> Result<bytes::Bytes, DbError> {
        Err(DbError::Checkpoint(format!(
            "cold slice staged without state-tier support \
             (operator={operator}, vnode={vnode})"
        )))
    }

    /// Vnodes eligible for demotion: every recorded slice still
    /// memory-resident (`Bytes`, not already `Cold`). Returns
    /// `(vnode, total_bytes)`, largest first — demoting big idle slices
    /// first frees the most memory. The caller only invokes this with no
    /// checkpoint in flight, so every recorded upload is already durable.
    #[cfg(feature = "state-tier")]
    pub(crate) fn demotion_candidates(&self) -> Vec<(u32, usize)> {
        let mut out: Vec<(u32, usize)> = self
            .last_vnode_uploads
            .iter()
            .filter(|(_, (_, slices))| {
                !slices.is_empty()
                    && slices
                        .values()
                        .all(|s| matches!(s, UploadedSlice::Bytes(_)))
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

    /// The recorded upload bytes for `vnode`, to hand to the tier. These
    /// are exactly the bytes of the slice's last durable full upload, so a
    /// demotion writes truth-identical state by construction.
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

    /// Release one operator's byte pin for a demoted `(operator, vnode)`
    /// slice — call only after the tier write is confirmed and the operator
    /// dropped the groups. The reference-partial flow then keys off the cold
    /// marker. Per operator (a vnode may carry several) so a refusal by one
    /// doesn't strand the others' records.
    #[cfg(feature = "state-tier")]
    pub(crate) fn mark_slice_demoted(&mut self, vnode: u32, operator: &str) {
        if let Some((_, slices)) = self.last_vnode_uploads.get_mut(&vnode) {
            if let Some(s) = slices.get_mut(operator) {
                *s = UploadedSlice::Cold;
            }
        }
    }

    /// Registers a sink connector for checkpoint coordination.
    pub(crate) fn register_sink(
        &mut self,
        name: impl Into<String>,
        handle: crate::sink_task::SinkTaskHandle,
        exactly_once: bool,
    ) {
        self.sinks.push(RegisteredSink {
            name: name.into(),
            handle,
            exactly_once,
        });
        // Invalidate the sorted-name cache; the next checkpoint will
        // rebuild it.
        self.cached_sorted_sink_names = None;
    }

    /// Begins the initial epoch on all exactly-once sinks.
    ///
    /// Must be called once after all sinks are registered and before any
    /// writes occur. This starts the first Kafka transaction for exactly-once
    /// sinks. Subsequent epochs are started automatically after each
    /// successful checkpoint commit.
    ///
    /// # Errors
    ///
    /// Returns the first error from any sink that fails to begin the epoch.
    pub async fn begin_initial_epoch(&self) -> Result<(), DbError> {
        self.begin_epoch_for_sinks(self.allocator.peek().0).await
    }

    /// Shared id allocator, cloned by the pipeline callback so barrier
    /// admission can claim ids without the coordinator mutex.
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    /// Begins an epoch on all exactly-once sinks. If any sink fails,
    /// rolls back sinks that already started the epoch.
    async fn begin_epoch_for_sinks(&self, epoch: u64) -> Result<(), DbError> {
        let mut started: Vec<&RegisteredSink> = Vec::new();
        for sink in &self.sinks {
            if sink.exactly_once {
                match sink.handle.begin_epoch(epoch).await {
                    Ok(()) => {
                        started.push(sink);
                        debug!(sink = %sink.name, epoch, "began epoch");
                    }
                    Err(e) => {
                        // Roll back sinks that already started.
                        for s in &started {
                            if let Err(re) = s.handle.rollback_epoch(epoch).await {
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

    /// Inject prometheus engine metrics.
    pub fn set_metrics(&mut self, prom: Arc<crate::engine_metrics::EngineMetrics>) {
        self.prom = Some(prom);
    }

    /// Emits checkpoint metrics to prometheus.
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

    /// Performs a full checkpoint cycle (steps 3-7).
    ///
    /// Steps 1-2 (barrier propagation + operator snapshots) are handled
    /// externally by the DAG executor and passed in via [`CheckpointRequest`].
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if any phase fails.
    pub async fn checkpoint(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        self.checkpoint_inner(request, None, QuorumStage::RunInline)
            .await
    }

    /// Pre-commits all exactly-once sinks (phase 1) with a timeout.
    ///
    /// A stuck sink will not block checkpointing indefinitely. The timeout
    /// is configured via [`CheckpointConfig::pre_commit_timeout`].
    async fn pre_commit_sinks(&self, epoch: u64) -> Result<(), DbError> {
        let timeout_dur = self.config.pre_commit_timeout;
        let start = std::time::Instant::now();

        let result =
            match tokio::time::timeout(timeout_dur, self.pre_commit_sinks_inner(epoch)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(DbError::Checkpoint(format!(
                    "pre-commit timed out after {}s",
                    timeout_dur.as_secs()
                ))),
            };

        // Record pre-commit duration regardless of success/failure.
        if let Some(ref m) = self.prom {
            m.sink_precommit_duration
                .observe(start.elapsed().as_secs_f64());
        }

        result
    }

    /// Inner pre-commit loop (no timeout).
    ///
    /// Only sinks with `exactly_once = true` participate in two-phase commit.
    /// At-most-once sinks are skipped — they receive no `pre_commit`/`commit`
    /// calls and provide no transactional guarantees.
    ///
    /// Fires every sink's `pre_commit` concurrently via `try_join_all` —
    /// matching the rollback path's shape. The serial version was
    /// `sum(per-sink pre-commit latency)`; with 4 Delta sinks at ~200ms
    /// S3 write each, that was 800ms serial. Concurrent = 200ms.
    async fn pre_commit_sinks_inner(&self, epoch: u64) -> Result<(), DbError> {
        let futures = self.sinks.iter().filter(|s| s.exactly_once).map(|sink| {
            let handle = sink.handle.clone();
            let name = sink.name.clone();
            async move {
                let result = handle.pre_commit(epoch).await;
                match result {
                    Ok(()) => {
                        debug!(sink = %name, epoch, "sink pre-committed");
                        Ok(())
                    }
                    Err(e) => Err(DbError::Checkpoint(format!(
                        "sink '{name}' pre-commit failed: {e}"
                    ))),
                }
            }
        });
        futures::future::try_join_all(futures).await.map(|_| ())
    }

    /// Commit each exactly-once sink in its own task, bounded by
    /// `commit_timeout`. Per-sink isolation avoids a slow sink blanket-
    /// failing the whole batch; cancellation lives inside the spawned
    /// task so an outer drop doesn't leave the sink-task with a dropped
    /// oneshot ack.
    async fn commit_sinks_tracked(&self, epoch: u64) -> HashMap<String, SinkCommitStatus> {
        let timeout_dur = self.config.commit_timeout;
        let start = std::time::Instant::now();

        let tasks: Vec<_> = self
            .sinks
            .iter()
            .filter(|s| s.exactly_once)
            .map(|sink| {
                let handle = sink.handle.clone();
                let name = sink.name.clone();
                let task = tokio::spawn(async move {
                    tokio::time::timeout(timeout_dur, handle.commit_epoch(epoch)).await
                });
                (name, task)
            })
            .collect();

        let mut statuses = HashMap::new();
        for (name, task) in tasks {
            let status = match task.await {
                Ok(Ok(Ok(()))) => SinkCommitStatus::Committed,
                Ok(Ok(Err(e))) => {
                    error!(sink = %name, epoch, error = %e, "sink commit failed");
                    SinkCommitStatus::Failed(format!("sink '{name}' commit failed: {e}"))
                }
                Ok(Err(_)) => {
                    error!(
                        sink = %name, epoch,
                        timeout_secs = timeout_dur.as_secs(),
                        "[LDB-6012] sink commit timed out",
                    );
                    SinkCommitStatus::Failed(format!(
                        "sink '{name}' commit timed out after {}s",
                        timeout_dur.as_secs()
                    ))
                }
                Err(join_err) => {
                    error!(sink = %name, epoch, error = %join_err, "sink commit task panicked");
                    SinkCommitStatus::Failed(format!("sink '{name}' commit panicked: {join_err}"))
                }
            };
            statuses.insert(name, status);
        }

        if let Some(ref m) = self.prom {
            m.sink_commit_duration
                .observe(start.elapsed().as_secs_f64());
        }
        statuses
    }

    /// Saves a manifest to the checkpoint store.
    ///
    /// Uses [`CheckpointStore::save_with_state`] to write optional sidecar
    /// data **before** the manifest, ensuring atomicity: if the sidecar write
    /// fails, the manifest is never persisted.
    ///
    /// Takes `Arc<CheckpointManifest>` so the caller can retain its own copy
    /// without a deep clone. Bounded by [`CheckpointConfig::persist_timeout`]
    /// to prevent a hung filesystem from stalling the runtime indefinitely.
    async fn save_manifest(
        &self,
        manifest: Arc<CheckpointManifest>,
        state_data: Option<Vec<bytes::Bytes>>,
    ) -> Result<(), DbError> {
        let timeout_dur = self.config.persist_timeout;
        let fut = self.store.save_with_state(&manifest, state_data.as_deref());
        match tokio::time::timeout(timeout_dur, fut).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(DbError::from(e)),
            Err(_elapsed) => Err(DbError::Checkpoint(format!(
                "[LDB-6011] manifest persist timed out after {}s — \
                 filesystem may be degraded",
                timeout_dur.as_secs()
            ))),
        }
    }

    /// Writes each owned vnode's `partial.bin` so the leader's
    /// `epoch_complete` gate returns true and sinks can commit.
    ///
    /// The payload is a [`VnodePartial`](crate::vnode_partial::VnodePartial)
    /// carrying the operator-state slices staged via
    /// [`set_pending_vnode_states`](Self::set_pending_vnode_states) for that
    /// vnode. A vnode with no staged state writes an empty `VnodePartial`,
    /// which still seals the durability gate (presence is all the gate checks).
    ///
    /// A vnode whose slices are byte-identical to its last full upload
    /// writes a tiny *reference* partial instead — upload cost scales
    /// with changed vnodes. References are forced back to full before
    /// their base leaves the `max_retained` prune window; bases record
    /// only after every write lands, so a partially-failed epoch
    /// re-uploads full.
    ///
    /// Fires every vnode write concurrently via `try_join_all`: the serial
    /// version was O(`vnode_count` × per-write latency) — trivial CPU but each
    /// a scheduling hop (and tens-to-hundreds of ms on a remote object store).
    async fn write_vnode_partials(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<(), DbError> {
        let Some(ref backend) = self.state_backend else {
            return Ok(());
        };
        if self.vnode_set.is_empty() {
            return Ok(());
        }
        // Stamp every write with the current assignment generation. Zero
        // means the host hasn't wired a version (single-instance path) and
        // the fence is a no-op.
        let caller_version = self.assignment_version;
        let max_ref_age = (self.config.max_retained as u64).max(1);

        // Step 1: classify each vnode as reference or full and encode its
        // payload. A staged `Cold` slice counts as unchanged (the demotion
        // contract guarantees byte identity with the recorded upload); when
        // a full upload is forced anyway — another operator's slice changed,
        // or the reference base is aging out — the cold bytes are fetched
        // back from the tier. A fetch failure fails the epoch: writing a
        // partial without the demoted slice would silently drop it from the
        // recovery truth.
        let mut full_uploads: Vec<(u32, std::collections::HashMap<String, UploadedSlice>)> =
            Vec::new();
        let mut emptied: Vec<u32> = Vec::new();
        let mut reference_count: u64 = 0;
        let mut encoded: Vec<(u32, bytes::Bytes)> = Vec::with_capacity(self.vnode_set.len());
        for &v in &self.vnode_set {
            let ops = self.pending_vnode_states.get(&v);
            let base = ops.filter(|ops| !ops.is_empty()).and_then(|ops| {
                self.last_vnode_uploads
                    .get(&v)
                    .filter(|(base, last)| {
                        epoch.saturating_sub(*base) < max_ref_age
                            && last.len() == ops.len()
                            && ops
                                .iter()
                                .all(|(n, s)| last.get(n).is_some_and(|prev| prev.matches(s)))
                    })
                    .map(|(base, _)| *base)
            });
            let partial = if let Some(base_epoch) = base {
                reference_count += 1;
                crate::vnode_partial::VnodePartial {
                    checkpoint_id,
                    operators: Vec::new(),
                    base_epoch: Some(base_epoch),
                }
            } else {
                let mut resolved: Vec<(String, Vec<u8>)> = Vec::new();
                let mut recorded: std::collections::HashMap<String, UploadedSlice> =
                    std::collections::HashMap::new();
                if let Some(ops) = ops {
                    for (name, slice) in ops {
                        let bytes = match slice {
                            StagedSlice::Bytes(b) => b.clone(),
                            StagedSlice::Cold => self.fetch_cold_slice(name, v).await?,
                        };
                        resolved.push((name.clone(), bytes.to_vec()));
                        // Record cold slices as cold: their bytes go into
                        // this upload but stay pinned only in the tier.
                        recorded.insert(
                            name.clone(),
                            match slice {
                                StagedSlice::Bytes(b) => UploadedSlice::Bytes(b.clone()),
                                StagedSlice::Cold => UploadedSlice::Cold,
                            },
                        );
                    }
                }
                if recorded.is_empty() {
                    emptied.push(v);
                } else {
                    full_uploads.push((v, recorded));
                }
                crate::vnode_partial::VnodePartial {
                    checkpoint_id,
                    operators: resolved,
                    base_epoch: None,
                }
            };
            encoded.push((v, bytes::Bytes::from(partial.encode()?)));
        }

        // Step 2 (async): upload concurrently.
        let writes = encoded.into_iter().map(|(v, payload)| {
            let backend = Arc::clone(backend);
            async move {
                backend
                    .write_partial(v, epoch, caller_version, payload)
                    .await
                    .map_err(|e| {
                        DbError::Checkpoint(format!(
                            "[LDB-6024] vnode partial write failed (vnode={v}, epoch={epoch}): {e}"
                        ))
                    })
            }
        });
        futures::future::try_join_all(writes).await?;

        // Step 3 (sync): record the new bases / clear emptied vnodes.
        for (v, ops) in full_uploads {
            self.last_vnode_uploads.insert(v, (epoch, ops));
        }
        for v in emptied {
            self.last_vnode_uploads.remove(&v);
        }
        if reference_count > 0 {
            if let Some(ref m) = self.prom {
                m.checkpoint_unchanged_vnodes.inc_by(reference_count);
            }
        }
        Ok(())
    }

    /// Poll the state backend's durability gate until every vnode in
    /// `gate_vnode_set` has its partial for `epoch` persisted (sealing
    /// the epoch's `_COMMIT` marker), or `restorable_gate_timeout`
    /// expires. No-op without a backend or with an empty gate set.
    ///
    /// Each node writes its partials *after* sink pre-commit + manifest
    /// save, so full presence proves every node finished its durable
    /// prepare. Transient backend errors retry until the deadline; a
    /// split-brain commit marker aborts immediately.
    async fn await_restorable_gate(
        &self,
        epoch: u64,
        participants: &[QuorumPeer],
    ) -> Result<(), String> {
        use laminar_core::state::StateBackendError;

        // Each poll LISTs the epoch prefix on the object store, so back
        // off exponentially: fast for the common local/in-process case,
        // ~1 LIST/s steady-state per slow epoch on S3 (gates also
        // serialize on the coordinator mutex, so at most one loop polls
        // at a time regardless of pipelining depth). Push-driven
        // follower upload-completion acks are the protocol-level
        // replacement for this poll (tracked in the plan).
        const INITIAL_POLL: Duration = Duration::from_millis(100);
        const MAX_POLL: Duration = Duration::from_secs(1);

        let Some(ref backend) = self.state_backend else {
            return Ok(());
        };
        if self.gate_vnode_set.is_empty() {
            return Ok(());
        }

        let deadline = Instant::now() + self.config.restorable_gate_timeout;
        let mut interval = INITIAL_POLL;
        let mut last_state = String::from("not all vnodes persisted");
        loop {
            if epoch < self.rotation_epoch_floor {
                return Err(format!(
                    "vnode assignment rotated after epoch {epoch} captured \
                     (rotation floor {}); epoch cannot seal",
                    self.rotation_epoch_floor
                ));
            }
            match backend.epoch_complete(epoch, &self.gate_vnode_set).await {
                Ok(true) => return Ok(()),
                // Keep the default/previous message; a lingering
                // transient-error string is more informative than
                // resetting it.
                Ok(false) => {}
                Err(e @ StateBackendError::SplitBrainCommit { .. }) => {
                    return Err(format!("state durability gate: {e}"));
                }
                Err(e) => {
                    // Transient I/O — keep polling until the deadline.
                    debug!(epoch, error = %e, "durability gate poll error; retrying");
                    last_state = e.to_string();
                }
            }
            // Fail fast when a capture participant dies: its uploads
            // will never arrive, so waiting out the timeout only
            // stalls the pipeline — and with pipelining, queued doomed
            // epochs would each burn the full timeout serially.
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
            if Instant::now() >= deadline {
                return Err(format!(
                    "state durability gate timed out after {:?}: {last_state}",
                    self.config.restorable_gate_timeout
                ));
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(MAX_POLL);
        }
    }

    /// Common abandon path for a failed checkpoint attempt: announce
    /// `Abort` so prepared followers release immediately, roll the
    /// epoch's sink transactions back, and begin the next epoch's
    /// transactions so writes arriving after the failure are not
    /// orphaned in a rolled-back transaction. Ids were allocated at the
    /// start of [`checkpoint_inner`](Self::checkpoint_inner), so the
    /// failed epoch is abandoned, never retried.
    async fn fail_epoch(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        started: Instant,
        error: String,
    ) -> CheckpointResult {
        #[cfg(feature = "cluster")]
        self.announce_if_leader(
            epoch,
            checkpoint_id,
            laminar_core::cluster::control::Phase::Abort,
            None,
        )
        .await;
        self.checkpoints_failed += 1;
        self.phase = CheckpointPhase::Idle;
        let duration = started.elapsed();
        self.emit_checkpoint_metrics(false, epoch, duration);
        if let Err(e) = self.rollback_sinks(epoch).await {
            error!(
                checkpoint_id, epoch, error = %e,
                "[LDB-6004] sink rollback failed after checkpoint failure",
            );
        }
        self.begin_next_epoch_bounded().await;
        self.pending_vnode_states.clear();
        CheckpointResult {
            success: false,
            checkpoint_id,
            epoch,
            duration,
            error: Some(error),
        }
    }

    /// Begin the (already-allocated) next epoch's sink transactions
    /// after a failed or partially-committed checkpoint, bounded by
    /// `rollback_timeout` — the sink that just failed may be wedged,
    /// and an unbounded `begin_epoch` await would hang the coordinator
    /// on it.
    async fn begin_next_epoch_bounded(&self) {
        let next_epoch = self.allocator.peek().0;
        match tokio::time::timeout(
            self.config.rollback_timeout,
            self.begin_epoch_for_sinks(next_epoch),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(
                next_epoch, error = %e,
                "[LDB-6015] failed to begin next epoch after abandoning a \
                 failed one — writes will be non-transactional",
            ),
            Err(_) => error!(
                next_epoch,
                timeout_secs = self.config.rollback_timeout.as_secs(),
                "[LDB-6015] begin next epoch timed out after a failed \
                 checkpoint — writes will be non-transactional",
            ),
        }
    }

    /// On startup, reconcile any Pending sinks in the last manifest
    /// against the durable commit marker. Marker present → drive
    /// local commit (idempotent); marker absent → rollback. Runs on
    /// every node; in cluster mode the leader additionally
    /// re-announces the decision.
    pub async fn reconcile_prepared_on_init(&self) {
        let last = match load_highest(self.store.as_ref()).await {
            Ok(Some(m)) => m,
            Ok(None) => return,
            Err(e) => {
                error!(error = %e, "[LDB-6041] reconcile skipped: could not load checkpoints");
                return;
            }
        };
        let has_pending = last
            .sink_commit_statuses
            .values()
            .any(|s| matches!(s, SinkCommitStatus::Pending));
        if !has_pending {
            return;
        }

        let epoch = last.epoch;
        let checkpoint_id = last.checkpoint_id;

        let committed = match self.decision_store.as_ref() {
            Some(ds) => ds.is_committed(epoch).await.unwrap_or_else(|e| {
                warn!(
                    epoch, checkpoint_id, error = %e,
                    "[LDB-6040] decision store read failed — defaulting to Abort",
                );
                false
            }),
            None => false,
        };

        #[cfg(feature = "cluster")]
        let is_leader = self
            .cluster_controller
            .as_ref()
            .is_some_and(|cc| cc.is_leader());

        if committed {
            info!(
                epoch,
                checkpoint_id, "recovering Pending epoch as Committed"
            );
            let statuses = self.commit_sinks_tracked(epoch).await;
            if let Err(e) = self
                .persist_recovered_statuses(checkpoint_id, statuses)
                .await
            {
                warn!(epoch, checkpoint_id, error = %e, "post-recovery manifest update failed");
            }
            #[cfg(feature = "cluster")]
            if is_leader {
                self.announce_if_leader(
                    epoch,
                    checkpoint_id,
                    laminar_core::cluster::control::Phase::Commit,
                    None,
                )
                .await;
            }
        } else {
            warn!(
                epoch,
                checkpoint_id, "[LDB-6035] Pending epoch with no commit marker — rolling back",
            );
            if let Err(e) = self.rollback_sinks(epoch).await {
                error!(epoch, checkpoint_id, error = %e, "sink rollback failed during recovery");
            }
            #[cfg(feature = "cluster")]
            if is_leader {
                self.announce_if_leader(
                    epoch,
                    checkpoint_id,
                    laminar_core::cluster::control::Phase::Abort,
                    None,
                )
                .await;
            }
        }

        // Brief pause so the announcement gossips before the next checkpoint tick.
        #[cfg(feature = "cluster")]
        if is_leader {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Overwrite the Pending sink statuses on the last manifest with
    /// the outcomes produced during recovery.
    async fn persist_recovered_statuses(
        &self,
        checkpoint_id: u64,
        statuses: HashMap<String, SinkCommitStatus>,
    ) -> Result<(), DbError> {
        if statuses.is_empty() {
            return Ok(());
        }
        match self.store.load_by_id(checkpoint_id).await {
            Ok(Some(mut m)) => {
                m.sink_commit_statuses = statuses;
                self.update_manifest_only(Arc::new(m)).await
            }
            Ok(None) => Ok(()),
            Err(e) => Err(DbError::from(e)),
        }
    }

    /// No-op when not the leader. Errors are logged — worst case is a
    /// longer follower timeout, not a correctness issue.
    ///
    /// `min_watermark_ms` is typically `None` on `Prepare`/`Abort` and
    /// `Some(cluster_min)` on `Commit` (computed from follower acks +
    /// local watermark). Downstream operators read the published
    /// value from [`ClusterController`] so event-time decisions stay
    /// consistent across the cluster.
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

    /// Announce PREPARE and block for follower acks. On quorum (or
    /// no-op — not leader / no controller) returns the capture-time
    /// follower set for the durability gate fail-fast and writes the
    /// cluster-wide minimum watermark into `self.cluster_min_watermark`
    /// so the subsequent `Commit` announcement can fan it out. On
    /// failure, announces `Abort` and returns the failure message.
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

    /// Shared health predicate for the capture quorum and the
    /// durability gate: a participant that is suspected, draining,
    /// left, or vanished can no longer contribute to this epoch, so
    /// waiting out the full timeout only stalls the pipeline.
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

    /// The capture-quorum stage, callable without the coordinator mutex
    /// so pipelined barrier tails can reach `Aligned` while an earlier
    /// epoch's durable tail still holds it. Announces `Prepare`, waits
    /// for every live follower's capture ack (failing fast on unhealthy
    /// membership), and returns the merged cluster-min watermark. The
    /// caller announces `Aligned` on success / `Abort` on failure and
    /// publishes the watermark via the announcement.
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
            // Leader-only cluster — cluster-wide min is just the
            // leader's local watermark (if any).
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
                    // Membership watch closed (host shutting down): this
                    // arm can no longer fail fast, so park it and let the
                    // sibling `select!` arm — the deadline-bounded quorum
                    // wait — decide the outcome. Not a leak: the select
                    // always completes via that arm.
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
                // Demonstrably alive — clear any quorum-miss suspicion.
                cc.note_responsive(acks);
                // Fold follower min with the leader's own watermark.
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
                // Gossip failure detection can lag a hard kill by tens
                // of seconds; record the leader's own faster signal so
                // the durability gates of already-captured epochs fail
                // fast instead of each burning their full timeout.
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

    /// Overwrites an existing manifest with updated fields (e.g., sink commit
    /// statuses after Step 6). Uses [`CheckpointStore::update_manifest`] which
    /// does NOT use conditional PUT, so the overwrite always succeeds.
    async fn update_manifest_only(&self, manifest: Arc<CheckpointManifest>) -> Result<(), DbError> {
        let timeout_dur = self.config.persist_timeout;
        let fut = self.store.update_manifest(&manifest);
        match tokio::time::timeout(timeout_dur, fut).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(DbError::from(e)),
            Err(_elapsed) => Err(DbError::Checkpoint(format!(
                "manifest update timed out after {}s",
                timeout_dur.as_secs()
            ))),
        }
    }

    /// Returns initial sink commit statuses (all `Pending`) for the manifest.
    fn initial_sink_commit_statuses(&self) -> HashMap<String, SinkCommitStatus> {
        self.sinks
            .iter()
            .filter(|s| s.exactly_once)
            .map(|s| (s.name.clone(), SinkCommitStatus::Pending))
            .collect()
    }

    /// Packs operator states into a manifest with optional sidecar chunks.
    ///
    /// States larger than `threshold` are stored in a sidecar blob rather
    /// than base64-inlined in the JSON manifest. The returned `Vec<Bytes>`
    /// is handed to `save_with_state` as a chain — the object-store path
    /// builds a multi-chunk `PutPayload` (no contiguous buffer), the FS
    /// path writes chunks sequentially. Either way no full-state copy
    /// happens here.
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

    /// Rolls back all exactly-once sinks in parallel, bounded by
    /// [`CheckpointConfig::rollback_timeout`].
    async fn rollback_sinks(&self, epoch: u64) -> Result<(), DbError> {
        let timeout_dur = self.config.rollback_timeout;
        match tokio::time::timeout(timeout_dur, self.rollback_sinks_inner(epoch)).await {
            Ok(result) => result,
            Err(_elapsed) => {
                error!(
                    epoch,
                    timeout_secs = timeout_dur.as_secs(),
                    "[LDB-6016] sink rollback timed out"
                );
                Err(DbError::Checkpoint(format!(
                    "rollback timed out after {}s",
                    timeout_dur.as_secs()
                )))
            }
        }
    }

    async fn rollback_sinks_inner(&self, epoch: u64) -> Result<(), DbError> {
        let futures = self.sinks.iter().filter(|s| s.exactly_once).map(|sink| {
            let handle = sink.handle.clone();
            let name = sink.name.clone();
            async move {
                // Live abandon, not a hard rollback — a healthy sink
                // keeps its pending output (see `SinkCommand::RollbackEpoch`).
                // Restart-time rollback (recovery manager) stays hard.
                let result = handle.abandon_epoch(epoch).await;
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

    /// Per-sink epoch map for the manifest: every exactly-once sink is
    /// committing `epoch` (passed in — `self.epoch` already points at
    /// the next epoch once a checkpoint is underway).
    fn collect_sink_epochs(&self, epoch: u64) -> HashMap<String, u64> {
        let mut epochs = HashMap::with_capacity(self.sinks.len());
        for sink in &self.sinks {
            if sink.exactly_once {
                epochs.insert(sink.name.clone(), epoch);
            }
        }
        epochs
    }

    /// Returns sorted sink names for topology tracking in the manifest.
    ///
    /// Computed once per topology change (via `register_sink`) and
    /// cached; subsequent checkpoints clone the cached Vec rather than
    /// re-sorting the sinks list.
    fn sorted_sink_names(&mut self) -> Vec<String> {
        if self.cached_sorted_sink_names.is_none() {
            let mut names: Vec<String> = self.sinks.iter().map(|s| s.name.clone()).collect();
            names.sort();
            self.cached_sorted_sink_names = Some(names);
        }
        // Invariant: set above.
        self.cached_sorted_sink_names.as_ref().unwrap().clone()
    }

    /// Returns the current phase.
    #[must_use]
    pub fn phase(&self) -> CheckpointPhase {
        self.phase
    }

    /// Returns the next epoch to be allocated.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.allocator.peek().0
    }

    /// Returns the next checkpoint ID to be allocated.
    #[must_use]
    pub fn next_checkpoint_id(&self) -> u64 {
        self.allocator.peek().1
    }

    /// Returns the checkpoint config.
    #[must_use]
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Returns checkpoint statistics.
    #[must_use]
    pub fn stats(&self) -> CheckpointStats {
        let (p50, p95, p99) = self.duration_histogram.percentiles();
        // Histogram stores microseconds; stats fields are milliseconds.
        CheckpointStats {
            completed: self.checkpoints_completed,
            failed: self.checkpoints_failed,
            last_duration: self.last_checkpoint_duration,
            duration_p50_ms: p50 / 1_000,
            duration_p95_ms: p95 / 1_000,
            duration_p99_ms: p99 / 1_000,
            total_bytes_written: self.total_bytes_written,
            current_phase: self.phase,
            current_epoch: self.allocator.peek().0,
        }
    }

    /// Returns a reference to the underlying store.
    #[must_use]
    pub fn store(&self) -> &dyn CheckpointStore {
        &*self.store
    }

    /// Performs a full checkpoint with pre-captured source offsets.
    ///
    /// When [`CheckpointRequest::source_offset_overrides`] is non-empty,
    /// those sources skip the live `snapshot_sources()` call and use the
    /// provided offsets instead. This is essential for barrier-aligned
    /// checkpoints where source positions must match the operator state
    /// at the barrier point.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if any phase fails.
    pub async fn checkpoint_with_offsets(
        &mut self,
        request: CheckpointRequest,
    ) -> Result<CheckpointResult, DbError> {
        self.checkpoint_inner(request, None, QuorumStage::RunInline)
            .await
    }

    /// Pipelined-barrier entry point: ids were allocated at admission
    /// and the capture quorum already ran before the coordinator mutex
    /// was taken (see `QuorumStage::Done`).
    ///
    /// # Errors
    /// Returns `DbError::Checkpoint` if any phase fails.
    pub(crate) async fn checkpoint_preallocated(
        &mut self,
        request: CheckpointRequest,
        epoch: u64,
        checkpoint_id: u64,
        quorum: QuorumStage,
    ) -> Result<CheckpointResult, DbError> {
        self.checkpoint_inner(request, Some((epoch, checkpoint_id)), quorum)
            .await
    }

    /// Abandon a pre-allocated epoch whose pre-mutex stage (alignment
    /// or capture quorum) failed: announce `Abort`, roll back, and
    /// begin the next epoch's sink transactions.
    #[cfg(feature = "cluster")]
    pub(crate) async fn abandon_epoch(
        &mut self,
        checkpoint_id: u64,
        epoch: u64,
        error: String,
    ) -> CheckpointResult {
        self.fail_epoch(checkpoint_id, epoch, Instant::now(), error)
            .await
    }

    /// Follower half of a cluster checkpoint: ack the capture, run the
    /// durable prepare (pre-commit + manifest + partial uploads), then
    /// wait for the leader's commit/abort.
    /// `Ok(true)` = committed, `Ok(false)` = aborted/timed out.
    ///
    /// The ack precedes the durable prepare — it means "aligned +
    /// captured". The leader verifies prepare completion through the
    /// restorable gate instead (partials are written last, so their
    /// presence implies the whole prepare finished).
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

        // Capture ack — state is already consistently captured by the
        // caller; the leader needs nothing more to release pipelines.
        cc.ack_barrier(&BarrierAck {
            epoch: ann.epoch,
            ok: true,
            error: None,
            // Leader folds this into the cluster-wide min announced on
            // Aligned/Commit. `None` is non-blocking.
            local_watermark_ms: self.local_watermark_ms,
        })
        .await
        .ok(); // best effort; leader's quorum wait tolerates missed acks

        self.follower_checkpoint_acked(request, ann, decision_timeout)
            .await
    }

    /// [`follower_checkpoint`](Self::follower_checkpoint) minus the
    /// capture ack: prepare, await the decision, then commit/rollback.
    /// Pipelined tails call the three stages separately so the
    /// decision wait does not hold the coordinator mutex (the next
    /// epoch's uploads would queue behind it for up to
    /// `decision_timeout`).
    ///
    /// # Errors
    /// Propagates sink pre-commit, manifest save, or marker-write failures.
    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_checkpoint_acked(
        &mut self,
        request: CheckpointRequest,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        let Some(cc) = self.cluster_controller.clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6033] follower_checkpoint called without cluster controller".into(),
            ));
        };
        let (epoch, checkpoint_id) = (ann.epoch, ann.checkpoint_id);
        self.follower_prepare_acked(request, epoch, checkpoint_id)
            .await?;
        let committed = Self::await_follower_decision(
            &cc,
            self.decision_store.as_deref(),
            epoch,
            checkpoint_id,
            decision_timeout,
        )
        .await;
        Ok(self.follower_finish(epoch, checkpoint_id, committed).await)
    }

    /// Stage 1 of the follower tail: durable prepare (sink pre-commit +
    /// manifest + partial uploads), after the capture ack. On failure,
    /// a best-effort `ok = false` ack overwrites the capture ack and
    /// the epoch rolls back.
    ///
    /// # Errors
    /// Propagates sink pre-commit, manifest save, or marker-write failures.
    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_prepare_acked(
        &mut self,
        request: CheckpointRequest,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<(), DbError> {
        use laminar_core::cluster::control::BarrierAck;

        // Track the leader's ids so a later leader-mode call resumes
        // correctly. Monotonic: a depth>1 tail finishing late must not
        // walk the allocator backwards past a successor's ids.
        self.allocator
            .advance_to(epoch, checkpoint_id.saturating_add(1));

        if let Err(e) = self.follower_prepare(request, epoch, checkpoint_id).await {
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
            // The rollback aborted the sinks' open transaction; open
            // the next epoch's so post-failure writes stay
            // transactional (mirrors the leader's `fail_epoch`).
            self.begin_next_epoch_bounded().await;
            return Err(e);
        }
        Ok(())
    }

    /// Stage 2 of the follower tail: wait for the leader's decision —
    /// `&self`-free so the wait never holds the coordinator mutex.
    /// Only Commit/Abort (or epoch advancement) end the wait; a newer
    /// epoch's announcement can supersede this epoch's Commit under
    /// latest-wins observation, so the durable marker is the
    /// tie-breaker. Returns the verdict.
    #[cfg(feature = "cluster")]
    pub(crate) async fn await_follower_decision(
        cc: &laminar_core::cluster::control::ClusterController,
        decision_store: Option<&laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        epoch: u64,
        checkpoint_id: u64,
        decision_timeout: Duration,
    ) -> bool {
        use laminar_core::cluster::control::Phase;

        let is_marked = || async {
            match decision_store {
                Some(ds) => ds.is_committed(epoch).await.unwrap_or_else(|e| {
                    warn!(
                        epoch, checkpoint_id, error = %e,
                        "[LDB-6045] decision store read failed — defaulting to Abort",
                    );
                    false
                }),
                None => false,
            }
        };

        let deadline = Instant::now() + decision_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let decision = cc
                .wait_for_barrier(
                    |a| {
                        a.epoch > epoch
                            || (a.epoch == epoch && matches!(a.phase, Phase::Commit | Phase::Abort))
                    },
                    remaining,
                )
                .await;
            match decision {
                Some(a) if a.epoch == epoch => return a.phase == Phase::Commit,
                Some(a) => {
                    // Newer epoch supersedes the decision announcement.
                    if is_marked().await {
                        info!(
                            epoch,
                            checkpoint_id,
                            observed_epoch = a.epoch,
                            "newer epoch observed with commit marker present — committing",
                        );
                        return true;
                    }
                    // No marker yet — keep waiting. Each pass through
                    // this (rare) state costs an object-store HEAD, so
                    // pace the re-check well below the decision timeout.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                None => {
                    // Deadline: one last durable-marker check.
                    if is_marked().await {
                        warn!(
                            epoch,
                            checkpoint_id,
                            "[LDB-6046] follower timeout but marker present — committing",
                        );
                        return true;
                    }
                    warn!(
                        epoch,
                        checkpoint_id, "[LDB-6034] follower decision timeout; rolling back",
                    );
                    return false;
                }
            }
        }
    }

    /// Stage 3 of the follower tail: act on the decision. Returns
    /// `true` on a clean commit.
    #[cfg(feature = "cluster")]
    pub(crate) async fn follower_finish(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
    ) -> bool {
        let clean = if committed {
            self.drive_follower_commit(epoch, checkpoint_id).await
        } else {
            self.rollback_sinks(epoch).await.ok();
            self.checkpoints_failed += 1;
            self.phase = CheckpointPhase::Idle;
            false
        };
        // Commit and rollback both close the sinks' open transaction;
        // open the next epoch's so subsequent writes are transactional
        // (the leader's mirror lives in `checkpoint_inner`/`fail_epoch`).
        self.begin_next_epoch_bounded().await;
        clean
    }

    /// Durable commit-marker store handle, for the lock-free decision
    /// wait in pipelined follower tails.
    #[cfg(feature = "cluster")]
    pub(crate) fn decision_store_handle(
        &self,
    ) -> Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>> {
        self.decision_store.clone()
    }

    /// Commit this follower's sinks for `epoch` and update its
    /// manifest's Pending entries. Returns `true` on clean commit.
    #[cfg(feature = "cluster")]
    async fn drive_follower_commit(&mut self, epoch: u64, checkpoint_id: u64) -> bool {
        let statuses = self.commit_sinks_tracked(epoch).await;
        let has_failures = statuses
            .values()
            .any(|s| matches!(s, SinkCommitStatus::Failed(_)));
        if has_failures {
            error!(
                epoch,
                checkpoint_id, "follower sink commit partially failed — rolling back",
            );
            self.rollback_sinks(epoch).await.ok();
            self.checkpoints_failed += 1;
            self.phase = CheckpointPhase::Idle;
            return false;
        }
        // Overwrite the follower's own manifest so the Pending sink
        // entries stamped during prepare get replaced with the
        // Committed statuses we just produced.
        if let Err(e) = self
            .persist_recovered_statuses(checkpoint_id, statuses)
            .await
        {
            warn!(
                checkpoint_id,
                epoch,
                error = %e,
                "follower post-commit manifest update failed",
            );
        }
        self.checkpoints_completed += 1;
        let (_, next_id) = self.allocator.peek();
        self.allocator.advance_to(epoch.saturating_add(1), next_id);
        self.phase = CheckpointPhase::Idle;
        true
    }

    /// Pre-commit + save manifest + write vnode markers.
    #[cfg(feature = "cluster")]
    async fn follower_prepare(
        &mut self,
        request: CheckpointRequest,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<(), DbError> {
        let CheckpointRequest {
            operator_states,
            watermark,
            table_store_checkpoint_path,
            extra_table_offsets,
            source_watermarks,
            pipeline_hash,
            source_offset_overrides,
        } = request;

        self.phase = CheckpointPhase::PreCommitting;
        if let Err(e) = self.pre_commit_sinks(epoch).await {
            self.pending_vnode_states.clear();
            return Err(e);
        }

        let mut manifest = CheckpointManifest::new(checkpoint_id, epoch);
        manifest.source_offsets = source_offset_overrides;
        manifest.table_offsets = extra_table_offsets;
        manifest.sink_epochs = self.collect_sink_epochs(epoch);
        manifest.sink_commit_statuses = self.initial_sink_commit_statuses();
        manifest.watermark = watermark;
        manifest.source_watermarks = source_watermarks;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.source_names = {
            let mut names: Vec<String> = manifest.source_offsets.keys().cloned().collect();
            names.sort();
            names
        };
        manifest.sink_names = self.sorted_sink_names();
        manifest.pipeline_hash = pipeline_hash;
        let state_data = Self::pack_operator_states(
            &mut manifest,
            &operator_states,
            self.config.state_inline_threshold,
        );

        self.phase = CheckpointPhase::Persisting;
        if let Err(e) = self.save_manifest(Arc::new(manifest), state_data).await {
            self.pending_vnode_states.clear();
            return Err(e);
        }
        if let Err(e) = self.write_vnode_partials(epoch, checkpoint_id).await {
            self.pending_vnode_states.clear();
            return Err(e);
        }
        self.pending_vnode_states.clear();
        Ok(())
    }

    /// Shared checkpoint implementation for all checkpoint entry points.
    ///
    /// When [`CheckpointRequest::source_offset_overrides`] is non-empty,
    /// those sources use the provided offsets instead of calling
    /// `snapshot_sources()`. This ensures barrier-aligned and pre-captured
    /// offsets are used atomically.
    #[allow(clippy::too_many_lines)]
    async fn checkpoint_inner(
        &mut self,
        request: CheckpointRequest,
        ids: Option<(u64, u64)>,
        quorum: QuorumStage,
    ) -> Result<CheckpointResult, DbError> {
        let CheckpointRequest {
            operator_states,
            watermark,
            table_store_checkpoint_path,
            extra_table_offsets,
            source_watermarks,
            pipeline_hash,
            source_offset_overrides,
        } = request;
        let start = Instant::now();
        // Ids are allocated up front (Flink-style): a failed attempt is
        // abandoned — never retried under the same ids — so a future
        // epoch's identity never depends on this one's outcome, which
        // is what allows epochs to overlap. Pipelined barrier paths
        // allocate at admission and pass the ids in; forced/timer
        // paths allocate here.
        let (epoch, checkpoint_id) = ids.unwrap_or_else(|| self.allocator.allocate());

        info!(checkpoint_id, epoch, "starting checkpoint");

        // Source offsets are provided by the caller (pre-captured at barrier
        // alignment or pre-spawn). Table offsets come from extra_table_offsets.
        self.phase = CheckpointPhase::Snapshotting;
        let source_offsets = source_offset_overrides;
        let table_offsets = extra_table_offsets;

        // Two-level completion, level 1: collect capture acks from
        // every live follower, then announce `Aligned` (the pipeline
        // resume gate); the restorable gate below verifies their
        // durable prepares. Pipelined barrier tails run this stage
        // pre-mutex and pass `Done` here.
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

        self.phase = CheckpointPhase::PreCommitting;
        if let Err(e) = self.pre_commit_sinks(epoch).await {
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

        let mut manifest = CheckpointManifest::new(checkpoint_id, epoch);
        manifest.source_offsets = source_offsets;
        manifest.table_offsets = table_offsets;
        manifest.sink_epochs = self.collect_sink_epochs(epoch);
        // Mark all exactly-once sinks as Pending before commit phase.
        manifest.sink_commit_statuses = self.initial_sink_commit_statuses();
        manifest.watermark = watermark;
        // Use caller-provided per-source watermarks if available. When empty,
        // leave source_watermarks empty — recovery falls back to the global
        // manifest.watermark. Do NOT fabricate per-source values from the
        // global watermark, as that loses granularity on recovery.
        manifest.source_watermarks = source_watermarks;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.source_names = {
            let mut names: Vec<String> = manifest.source_offsets.keys().cloned().collect();
            names.sort();
            names
        };
        manifest.sink_names = self.sorted_sink_names();
        manifest.pipeline_hash = pipeline_hash;

        let state_data = Self::pack_operator_states(
            &mut manifest,
            &operator_states,
            self.config.state_inline_threshold,
        );
        let sidecar_bytes = state_data
            .as_ref()
            .map_or(0, |chunks| chunks.iter().map(bytes::Bytes::len).sum());
        if sidecar_bytes > 0 {
            debug!(
                checkpoint_id,
                sidecar_bytes, "writing operator state sidecar"
            );
        }

        if let Some(cap) = self.config.max_checkpoint_bytes {
            if sidecar_bytes > cap {
                let msg = format!(
                    "[LDB-6014] checkpoint size {sidecar_bytes} bytes exceeds \
                     cap {cap} bytes — checkpoint rejected"
                );
                error!(checkpoint_id, epoch, sidecar_bytes, cap, "{msg}");
                // `fail_epoch` also rolls the pre-committed sinks back —
                // previously this path returned without a rollback,
                // leaving the epoch's transactions open.
                return Ok(self.fail_epoch(checkpoint_id, epoch, start, msg).await);
            }
            let warn_threshold = cap * 4 / 5; // 80%
            if sidecar_bytes > warn_threshold {
                warn!(
                    checkpoint_id,
                    epoch, sidecar_bytes, cap, "checkpoint size approaching cap (>80%)"
                );
            }
        }
        let checkpoint_bytes = sidecar_bytes as u64;

        self.phase = CheckpointPhase::Persisting;
        // Arc-wrap so the save task gets a cheap refcount bump instead of
        // a deep clone. After `save_manifest.await` the task drops its
        // Arc and we're the sole owner; `Arc::make_mut` below gets us a
        // free mutable reference for the post-commit sink-status update.
        let mut manifest = Arc::new(manifest);
        if let Err(e) = self.save_manifest(Arc::clone(&manifest), state_data).await {
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

        // Publish each owned vnode's partial (operator-state slice + commit
        // marker in one blob) so the durability gate below has something to
        // check and a future owner can rehydrate the vnode's state.
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

        // Durability gate (level 2, "restorable"): confirm every
        // participating vnode has its partial persisted before sinks
        // commit. `gate_vnode_set` is the FULL registry in cluster mode —
        // the leader verifies markers from every follower's
        // shared-storage writes, not just its own. Single-instance
        // defaults to `vnode_set` (the two match). Followers upload
        // asynchronously after their capture ack, so this polls until
        // `restorable_gate_timeout` rather than checking once.
        #[cfg(not(feature = "cluster"))]
        let quorum_participants: Vec<QuorumPeer> = Vec::new();
        let gate_start = Instant::now();
        let gate_result = self
            .await_restorable_gate(epoch, &quorum_participants)
            .await;
        // Observed on failure too — a burned gate timeout is the signal
        // that decides when the push-driven completion-ack follow-up is
        // worth building.
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

        // Record the commit marker before issuing sink commits — this
        // is the durable record of the commit decision that recovery
        // reads to distinguish "committed mid-flight" from "never
        // committed". Cluster mode gates on leadership.
        let is_decision_leader = {
            #[cfg(feature = "cluster")]
            {
                self.cluster_controller
                    .as_ref()
                    .is_none_or(|cc| cc.is_leader())
            }
            #[cfg(not(feature = "cluster"))]
            {
                true
            }
        };
        if is_decision_leader {
            if let Some(ds) = self.decision_store.as_ref() {
                if let Err(e) = ds.record_committed(epoch).await {
                    error!(
                        checkpoint_id, epoch, error = %e,
                        "[LDB-6038] cannot record commit marker — aborting epoch",
                    );
                    return Ok(self
                        .fail_epoch(checkpoint_id, epoch, start, format!("commit marker: {e}"))
                        .await);
                }
            }
        }

        #[cfg(feature = "cluster")]
        // Fan out the cluster-wide min watermark computed during
        // `await_prepare_quorum`. Followers consume this from
        // `observe_barrier` and update their consumer-side view.
        self.announce_if_leader(
            epoch,
            checkpoint_id,
            laminar_core::cluster::control::Phase::Commit,
            self.cluster_min_watermark,
        )
        .await;

        self.phase = CheckpointPhase::Committing;
        let sink_statuses = self.commit_sinks_tracked(epoch).await;
        let has_failures = sink_statuses
            .values()
            .any(|s| matches!(s, SinkCommitStatus::Failed(_)));

        if !sink_statuses.is_empty() {
            // `Arc::make_mut`: COW. Refcount is 1 here (spawn_blocking
            // task in save_manifest has already returned and dropped its
            // clone), so this is a zero-copy mutable borrow.
            Arc::make_mut(&mut manifest).sink_commit_statuses = sink_statuses;
            if let Err(e) = self.update_manifest_only(Arc::clone(&manifest)).await {
                warn!(
                    checkpoint_id,
                    epoch,
                    error = %e,
                    "post-commit manifest update failed"
                );
            }
        }

        if has_failures {
            // The commit decision is already durable (marker + Commit
            // announcement), so the epoch is NOT rolled back: the failed
            // sinks' statuses are recorded in the manifest and re-driven
            // by `reconcile_prepared_on_init` on restart. Begin the next
            // epoch so subsequent writes stay transactional.
            self.checkpoints_failed += 1;
            error!(
                checkpoint_id,
                epoch,
                "sink commit partially failed after the commit decision — \
                 statuses recorded for recovery-time re-drive"
            );
            self.phase = CheckpointPhase::Idle;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            self.begin_next_epoch_bounded().await;
            self.pending_vnode_states.clear();
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some("partial sink commit failure".into()),
            });
        }

        self.phase = CheckpointPhase::Idle;
        self.checkpoints_completed += 1;
        self.total_bytes_written += checkpoint_bytes;
        let duration = start.elapsed();
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(true, epoch, duration);

        // Emit checkpoint size metrics.
        if let Some(ref m) = self.prom {
            #[allow(clippy::cast_possible_wrap)]
            m.checkpoint_size_bytes.set(checkpoint_bytes as i64);
        }

        // Garbage-collect state-backend partials / commit markers for
        // epochs no longer needed for recovery. Without this the
        // in-process backend grows per-checkpoint forever and the
        // object-store backend leaks `epoch=N/…` objects indefinitely.
        // `max_retained` is in terms of checkpoints which map 1:1 to
        // epochs here, so prune everything older than
        // `epoch - max_retained`.
        if let Some(ref backend) = self.state_backend {
            let horizon = epoch.saturating_sub(self.config.max_retained as u64);
            if horizon > 0 {
                if let Err(e) = backend.prune_before(horizon).await {
                    warn!(
                        epoch,
                        horizon,
                        error = %e,
                        "[LDB-6026] state backend prune failed; old partials will linger"
                    );
                }
                if let Some(ref ds) = self.decision_store {
                    if let Err(e) = ds.prune_before(horizon).await {
                        warn!(epoch, horizon, error = %e, "decision prune failed");
                    }
                }
            }
        }

        let next_epoch = self.allocator.peek().0;
        let begin_epoch_error = match self.begin_epoch_for_sinks(next_epoch).await {
            Ok(()) => None,
            Err(e) => {
                error!(
                    next_epoch,
                    error = %e,
                    "[LDB-6015] failed to begin next epoch — writes will be non-transactional"
                );
                Some(e.to_string())
            }
        };

        info!(
            checkpoint_id,
            epoch,
            duration_ms = duration.as_millis(),
            "checkpoint completed"
        );

        // The checkpoint itself succeeded (state persisted, sinks committed).
        // begin_epoch failure for the *next* epoch is reported as a warning
        // but does not retroactively fail the completed checkpoint.
        self.pending_vnode_states.clear();
        Ok(CheckpointResult {
            success: true,
            checkpoint_id,
            epoch,
            duration,
            error: begin_epoch_error,
        })
    }

    /// Attempts recovery from the latest checkpoint.
    ///
    /// Creates a [`RecoveryManager`](crate::recovery_manager::RecoveryManager)
    /// using the coordinator's store and delegates recovery to it.
    /// On success, advances `self.epoch` past the recovered epoch so the
    /// next checkpoint gets a fresh epoch number.
    ///
    /// Returns `Ok(None)` for a fresh start (no checkpoint found).
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store itself fails.
    pub async fn recover(
        &mut self,
    ) -> Result<Option<crate::recovery_manager::RecoveredState>, DbError> {
        use crate::recovery_manager::RecoveryManager;

        let mgr = RecoveryManager::new(&*self.store);
        // Sources and table sources are restored by the pipeline lifecycle
        // (via SourceRegistration.restore_checkpoint), not by the coordinator.
        // Pass empty slices — the coordinator only manages sink recovery here.
        let result = mgr.recover(&[], &self.sinks, &[]).await?;

        if let Some(ref recovered) = result {
            // Monotonic: the recovered (committed) epoch may be older
            // than a Pending manifest this node seeded its ids from.
            self.allocator
                .advance_to(recovered.epoch() + 1, recovered.manifest.checkpoint_id + 1);
            let (epoch, checkpoint_id) = self.allocator.peek();
            info!(epoch, checkpoint_id, "coordinator epoch set after recovery");
        }

        Ok(result)
    }

    /// Loads the latest manifest from the store.
    ///
    /// # Errors
    ///
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

/// Fixed-size ring buffer for duration percentile tracking.
///
/// Stores the last `CAPACITY` durations in **microseconds** and computes
/// p50/p95/p99 via sorted extraction. No heap allocation after construction.
#[derive(Clone)]
pub struct DurationHistogram {
    /// Ring buffer of durations in microseconds.
    samples: Box<[u64; Self::CAPACITY]>,
    /// Write cursor (wraps at `CAPACITY`).
    cursor: usize,
    /// Total samples written (may exceed `CAPACITY`).
    count: u64,
}

impl Default for DurationHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl DurationHistogram {
    const CAPACITY: usize = 100;

    /// Creates an empty histogram.
    #[must_use]
    pub fn new() -> Self {
        Self {
            samples: Box::new([0; Self::CAPACITY]),
            cursor: 0,
            count: 0,
        }
    }

    /// Records a duration sample (stored in microseconds).
    pub fn record(&mut self, duration: Duration) {
        #[allow(clippy::cast_possible_truncation)]
        let us = duration.as_micros() as u64;
        self.samples[self.cursor] = us;
        self.cursor = (self.cursor + 1) % Self::CAPACITY;
        self.count += 1;
    }

    /// Returns `true` if no samples have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the number of recorded samples (up to `CAPACITY`).
    #[must_use]
    pub fn len(&self) -> usize {
        if self.count >= Self::CAPACITY as u64 {
            Self::CAPACITY
        } else {
            // SAFETY: count < CAPACITY (100), which always fits in usize.
            #[allow(clippy::cast_possible_truncation)]
            {
                self.count as usize
            }
        }
    }

    /// Computes a percentile (0.0–1.0) from recorded samples.
    ///
    /// Returns 0 if no samples have been recorded.
    #[must_use]
    pub fn percentile(&self, p: f64) -> u64 {
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

    /// Returns (p50, p95, p99) in microseconds. Sorts once.
    #[must_use]
    pub fn percentiles(&self) -> (u64, u64, u64) {
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

impl std::fmt::Debug for DurationHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (p50, p95, p99) = self.percentiles();
        f.debug_struct("DurationHistogram")
            .field("samples_len", &self.samples.len())
            .field("cursor", &self.cursor)
            .field("count", &self.count)
            .field("p50_us", &p50)
            .field("p95_us", &p95)
            .field("p99_us", &p99)
            .finish()
    }
}

/// Checkpoint performance statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckpointStats {
    /// Total completed checkpoints.
    pub completed: u64,
    /// Total failed checkpoints.
    pub failed: u64,
    /// Duration of the last checkpoint.
    pub last_duration: Option<Duration>,
    /// p50 checkpoint duration in milliseconds.
    pub duration_p50_ms: u64,
    /// p95 checkpoint duration in milliseconds.
    pub duration_p95_ms: u64,
    /// p99 checkpoint duration in milliseconds.
    pub duration_p99_ms: u64,
    /// Total bytes written across all checkpoints.
    pub total_bytes_written: u64,
    /// Current checkpoint phase.
    pub current_phase: CheckpointPhase,
    /// Current epoch number.
    pub current_epoch: u64,
}

/// Converts a `SourceCheckpoint` to a `ConnectorCheckpoint`.
#[must_use]
pub fn source_to_connector_checkpoint(cp: &SourceCheckpoint) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: cp.offsets().clone(),
        epoch: cp.epoch(),
        metadata: cp.metadata().clone(),
    }
}

/// Converts a `ConnectorCheckpoint` back to a `SourceCheckpoint`.
#[must_use]
pub fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source_cp = SourceCheckpoint::with_offsets(cp.epoch, cp.offsets.clone());
    for (k, v) in &cp.metadata {
        source_cp.set_metadata(k.clone(), v.clone());
    }
    source_cp
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

    async fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
        let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
        CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap()
    }

    /// Coordinator whose restorable gate gives up quickly — for tests
    /// that exercise a gate *miss* (the default 30s poll would stall
    /// the suite).
    async fn make_coordinator_with_fast_gate(dir: &std::path::Path) -> CheckpointCoordinator {
        let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
        let config = CheckpointConfig {
            restorable_gate_timeout: Duration::from_millis(250),
            ..CheckpointConfig::default()
        };
        CheckpointCoordinator::new(config, store).await.unwrap()
    }

    #[tokio::test]
    async fn test_coordinator_new() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path()).await;

        assert_eq!(coord.epoch(), 1);
        assert_eq!(coord.next_checkpoint_id(), 1);
        assert_eq!(coord.phase(), CheckpointPhase::Idle);
    }

    #[tokio::test]
    async fn test_coordinator_resumes_from_stored_checkpoint() {
        let dir = tempfile::tempdir().unwrap();

        // Save a checkpoint manually
        let store = FileSystemCheckpointStore::new(dir.path(), 3);
        let m = CheckpointManifest::new(5, 10);
        store.save(&m).await.unwrap();

        // Coordinator should resume from epoch 11, checkpoint_id 6
        let coord = make_coordinator(dir.path()).await;
        assert_eq!(coord.epoch(), 11);
        assert_eq!(coord.next_checkpoint_id(), 6);
    }

    #[test]
    fn test_checkpoint_phase_display() {
        assert_eq!(CheckpointPhase::Idle.to_string(), "Idle");
        assert_eq!(CheckpointPhase::Snapshotting.to_string(), "Snapshotting");
        assert_eq!(CheckpointPhase::PreCommitting.to_string(), "PreCommitting");
        assert_eq!(CheckpointPhase::Persisting.to_string(), "Persisting");
        assert_eq!(CheckpointPhase::Committing.to_string(), "Committing");
    }

    #[test]
    fn test_source_to_connector_checkpoint() {
        let mut cp = SourceCheckpoint::new(5);
        cp.set_offset("partition-0", "1234");
        cp.set_metadata("topic", "events");

        let cc = source_to_connector_checkpoint(&cp);
        assert_eq!(cc.epoch, 5);
        assert_eq!(cc.offsets.get("partition-0"), Some(&"1234".into()));
        assert_eq!(cc.metadata.get("topic"), Some(&"events".into()));
    }

    #[test]
    fn test_connector_to_source_checkpoint() {
        let cc = ConnectorCheckpoint {
            offsets: HashMap::from([("lsn".into(), "0/ABCD".into())]),
            epoch: 3,
            metadata: HashMap::from([("type".into(), "postgres".into())]),
        };

        let cp = connector_to_source_checkpoint(&cc);
        assert_eq!(cp.epoch(), 3);
        assert_eq!(cp.get_offset("lsn"), Some("0/ABCD"));
        assert_eq!(cp.get_metadata("type"), Some("postgres"));
    }

    #[tokio::test]
    async fn test_stats_initial() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path()).await;
        let stats = coord.stats();

        assert_eq!(stats.completed, 0);
        assert_eq!(stats.failed, 0);
        assert!(stats.last_duration.is_none());
        assert_eq!(stats.duration_p50_ms, 0);
        assert_eq!(stats.duration_p95_ms, 0);
        assert_eq!(stats.duration_p99_ms, 0);
        assert_eq!(stats.current_phase, CheckpointPhase::Idle);
    }

    #[tokio::test]
    async fn test_checkpoint_no_sources_no_sinks() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let result = coord
            .checkpoint(CheckpointRequest {
                watermark: Some(1000),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(result.checkpoint_id, 1);
        assert_eq!(result.epoch, 1);

        // Verify manifest was persisted
        let loaded = coord.store().load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
        assert_eq!(loaded.watermark, Some(1000));

        // Second checkpoint should increment
        let result2 = coord
            .checkpoint(CheckpointRequest {
                watermark: Some(2000),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result2.success);
        assert_eq!(result2.checkpoint_id, 2);
        assert_eq!(result2.epoch, 2);

        let stats = coord.stats();
        assert_eq!(stats.completed, 2);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn test_checkpoint_with_operator_states() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let mut ops = HashMap::new();
        ops.insert(
            "window-agg".into(),
            bytes::Bytes::from_static(b"state-data"),
        );
        ops.insert("filter".into(), bytes::Bytes::from_static(b"filter-state"));

        let result = coord
            .checkpoint(CheckpointRequest {
                operator_states: ops,
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success);

        let loaded = coord.store().load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.operator_states.len(), 2);

        let window_op = loaded.operator_states.get("window-agg").unwrap();
        assert_eq!(window_op.decode_inline().unwrap(), b"state-data");
    }

    #[tokio::test]
    async fn test_checkpoint_with_table_store_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let result = coord
            .checkpoint(CheckpointRequest {
                table_store_checkpoint_path: Some("/tmp/table_store_cp".into()),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success);

        let loaded = coord.store().load_latest().await.unwrap().unwrap();
        assert_eq!(
            loaded.table_store_checkpoint_path.as_deref(),
            Some("/tmp/table_store_cp")
        );
    }

    #[tokio::test]
    async fn test_load_latest_manifest_empty() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path()).await;
        assert!(coord.load_latest_manifest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_coordinator_debug() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path()).await;
        let debug = format!("{coord:?}");
        assert!(debug.contains("CheckpointCoordinator"));
        assert!(debug.contains("epoch: 1"));
    }

    #[tokio::test]
    async fn test_checkpoint_emits_metrics_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let registry = prometheus::Registry::new();
        let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
        coord.set_metrics(Arc::clone(&prom));

        let result = coord
            .checkpoint(CheckpointRequest {
                watermark: Some(1000),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(prom.checkpoints_completed.get(), 1);
        assert_eq!(prom.checkpoints_failed.get(), 0);
        assert_eq!(prom.checkpoint_epoch.get(), 1);

        // Second checkpoint
        let result2 = coord
            .checkpoint(CheckpointRequest {
                watermark: Some(2000),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result2.success);
        assert_eq!(prom.checkpoints_completed.get(), 2);
        assert_eq!(prom.checkpoint_epoch.get(), 2);
    }

    #[tokio::test]
    async fn test_checkpoint_without_metrics() {
        // Verify checkpoint works fine without metrics set
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();

        assert!(result.success);
        // No panics — metrics emission is a no-op
    }

    #[test]
    fn test_histogram_empty() {
        let h = DurationHistogram::new();
        assert_eq!(h.len(), 0);
        assert_eq!(h.percentile(0.50), 0);
        assert_eq!(h.percentile(0.99), 0);
        let (p50, p95, p99) = h.percentiles();
        assert_eq!((p50, p95, p99), (0, 0, 0));
    }

    #[test]
    fn test_histogram_single_sample() {
        let mut h = DurationHistogram::new();
        h.record(Duration::from_millis(42));
        assert_eq!(h.len(), 1);
        // 42ms = 42_000μs
        assert_eq!(h.percentile(0.50), 42_000);
        assert_eq!(h.percentile(0.99), 42_000);
    }

    #[test]
    fn test_histogram_sub_millisecond() {
        let mut h = DurationHistogram::new();
        // 500μs — previously truncated to 0 with as_millis()
        h.record(Duration::from_micros(500));
        assert_eq!(h.percentile(0.50), 500);
        assert_eq!(h.percentile(0.99), 500);
    }

    #[test]
    fn test_histogram_percentiles() {
        let mut h = DurationHistogram::new();
        // Record 1..=100ms in order → 1000..=100_000 μs.
        for i in 1..=100 {
            h.record(Duration::from_millis(i));
        }
        assert_eq!(h.len(), 100);

        let p50 = h.percentile(0.50);
        let p95 = h.percentile(0.95);
        let p99 = h.percentile(0.99);

        // Values in μs: 1000..=100_000
        //   p50 ≈ 50_000, p95 ≈ 95_000, p99 ≈ 99_000
        assert!((49_000..=51_000).contains(&p50), "p50={p50}");
        assert!((94_000..=96_000).contains(&p95), "p95={p95}");
        assert!((98_000..=100_000).contains(&p99), "p99={p99}");
    }

    #[test]
    fn test_histogram_wraps_ring_buffer() {
        let mut h = DurationHistogram::new();
        // Write 150 samples — first 50 are overwritten.
        for i in 1..=150 {
            h.record(Duration::from_millis(i));
        }
        assert_eq!(h.len(), 100);
        assert_eq!(h.count, 150);

        // Only samples 51..=150 remain in the buffer (51_000..=150_000 μs).
        let p50 = h.percentile(0.50);
        assert!((99_000..=101_000).contains(&p50), "p50={p50}");
    }

    #[tokio::test]
    async fn test_sidecar_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig {
            state_inline_threshold: 100, // 100 bytes threshold
            ..CheckpointConfig::default()
        };
        let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

        // Small state stays inline, large state goes to sidecar
        let mut ops = HashMap::new();
        ops.insert("small".into(), bytes::Bytes::from(vec![0xAAu8; 50]));
        ops.insert("large".into(), bytes::Bytes::from(vec![0xBBu8; 200]));

        let result = coord
            .checkpoint(CheckpointRequest {
                operator_states: ops,
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();
        assert!(result.success);

        // Verify manifest
        let loaded = coord.store().load_latest().await.unwrap().unwrap();
        let small_op = loaded.operator_states.get("small").unwrap();
        assert!(!small_op.external, "small state should be inline");
        assert_eq!(small_op.decode_inline().unwrap(), vec![0xAAu8; 50]);

        let large_op = loaded.operator_states.get("large").unwrap();
        assert!(large_op.external, "large state should be external");
        assert_eq!(large_op.external_length, 200);

        // Verify sidecar file exists and has correct data
        let state_data = coord.store().load_state_data(1).await.unwrap().unwrap();
        assert_eq!(state_data.len(), 200);
        assert!(state_data.iter().all(|&b| b == 0xBB));
    }

    #[tokio::test]
    async fn test_all_inline_no_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig::default(); // 1MB threshold
        let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

        let mut ops = HashMap::new();
        ops.insert("op1".into(), bytes::Bytes::from_static(b"small-state"));

        let result = coord
            .checkpoint(CheckpointRequest {
                operator_states: ops,
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();
        assert!(result.success);

        // No sidecar file
        assert!(coord.store().load_state_data(1).await.unwrap().is_none());
    }

    // Durability gate tests.

    #[tokio::test]
    async fn durability_gate_skipped_when_vnode_set_empty() {
        // With no state backend installed AND empty vnode set, the commit
        // path behaves as before. Regression guard: the durability gate
        // must not change single-instance semantics.
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;
        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success, "baseline checkpoint must succeed");
    }

    #[tokio::test]
    async fn bridge_writes_markers_and_gate_passes() {
        use laminar_core::state::InProcessBackend;
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;
        let backend = Arc::new(InProcessBackend::new(4));
        coord.set_state_backend(backend.clone());
        coord.set_vnode_set(vec![0, 1, 2, 3]);

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success, "bridge writes markers → gate passes");
        // Every owned vnode has a marker for the completed epoch.
        for v in 0..4 {
            assert!(
                backend.read_partial(v, 1).await.unwrap().is_some(),
                "bridge should have written marker for vnode {v}",
            );
        }
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn reconcile_announces_abort_when_no_decision_store() {
        // Fallback path: if no decision store is wired (e.g. legacy
        // deployments), absence of a marker == Abort. This is the
        // pre-decision-store behavior preserved for compatibility.
        use laminar_core::cluster::control::{
            BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let mut orphan = CheckpointManifest::new(42, 7);
        orphan
            .sink_commit_statuses
            .insert("kafka_out".into(), SinkCommitStatus::Pending);
        store.save_with_state(&orphan, None).await.unwrap();

        let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let (_tx, rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
        let mut coord = coord;
        coord.set_cluster_controller(controller);

        coord.reconcile_prepared_on_init().await;

        let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
        let ann: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
        assert_eq!(ann.phase, Phase::Abort);
        assert_eq!(ann.epoch, 7);
        assert_eq!(ann.checkpoint_id, 42);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn reconcile_announces_commit_when_marker_present() {
        use laminar_core::cluster::control::{
            BarrierAnnouncement, CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv,
            Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
        use object_store::local::LocalFileSystem;
        use tokio::sync::watch;

        let ckpt_dir = tempfile::tempdir().unwrap();
        let decision_dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path(), 3));
        let mut orphan = CheckpointManifest::new(42, 7);
        orphan
            .sink_commit_statuses
            .insert("kafka_out".into(), SinkCommitStatus::Pending);
        store.save_with_state(&orphan, None).await.unwrap();

        let decision_os: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
        let decision_store = Arc::new(CheckpointDecisionStore::new(decision_os));
        decision_store.record_committed(7).await.unwrap();

        let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let (_tx, rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
        let mut coord = coord;
        coord.set_cluster_controller(controller);
        coord.set_decision_store(decision_store);

        coord.reconcile_prepared_on_init().await;

        let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
        let ann: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
        assert_eq!(ann.phase, Phase::Commit);
        assert_eq!(ann.epoch, 7);
        assert_eq!(ann.checkpoint_id, 42);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn reconcile_announces_abort_when_marker_missing() {
        // Decision store is wired but has no marker for this epoch —
        // the "leader crashed before commit point" case.
        use laminar_core::cluster::control::{
            BarrierAnnouncement, CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv,
            Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
        use object_store::local::LocalFileSystem;
        use tokio::sync::watch;

        let ckpt_dir = tempfile::tempdir().unwrap();
        let decision_dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path(), 3));
        let mut orphan = CheckpointManifest::new(11, 3);
        orphan
            .sink_commit_statuses
            .insert("out".into(), SinkCommitStatus::Pending);
        store.save_with_state(&orphan, None).await.unwrap();

        let decision_os: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
        let decision_store = Arc::new(CheckpointDecisionStore::new(decision_os));

        let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let (_tx, rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
        let mut coord = coord;
        coord.set_cluster_controller(controller);
        coord.set_decision_store(decision_store);

        coord.reconcile_prepared_on_init().await;

        let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
        let ann: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
        assert_eq!(ann.phase, Phase::Abort);
        assert_eq!(ann.epoch, 3);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn reconcile_silent_when_manifest_clean() {
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, InMemoryKv, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let mut clean = CheckpointManifest::new(5, 3);
        clean
            .sink_commit_statuses
            .insert("out".into(), SinkCommitStatus::Committed);
        store.save_with_state(&clean, None).await.unwrap();

        let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let (_tx, rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
        let mut coord = coord;
        coord.set_cluster_controller(controller);

        coord.reconcile_prepared_on_init().await;

        // No announcement emitted.
        assert!(kv.read_from(self_id, ANNOUNCEMENT_KEY).await.is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_checkpoint_commits_on_leader_commit() {
        use laminar_core::cluster::control::{
            BarrierAck, BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase,
            ACK_KEY, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let leader_id = NodeId(1);
        let follower_id = NodeId(7);

        // Follower's KV sees both its own writes and a seeded view of the
        // leader's announcements. `members_rx` includes the leader so
        // `current_leader()` picks the lowest id (the leader, not self).
        let kv = Arc::new(InMemoryKv::new(follower_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let leader_info = NodeInfo {
            id: leader_id,
            name: "leader".into(),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let (_tx, rx) = watch::channel(vec![leader_info]);
        let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
        coord.set_cluster_controller(controller);

        // Leader has already announced PREPARE and COMMIT (simulates
        // a fast-gossip scenario; follower sees both on its first poll).
        let prepare_json = serde_json::to_string(&BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap();
        let commit_json = serde_json::to_string(&BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Commit,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap();
        // Overwrite the prepare with commit — observe_barrier reads the
        // latest value. Real gossip shows both in order; for the unit
        // test, landing on Commit is enough for the decision loop.
        kv.seed(leader_id, ANNOUNCEMENT_KEY, prepare_json);
        kv.seed(leader_id, ANNOUNCEMENT_KEY, commit_json);

        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let committed = coord
            .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(2))
            .await
            .unwrap();
        assert!(committed, "follower should commit on leader's Commit");

        // Follower's ack landed in its own KV.
        let ack_raw = kv.read_from(follower_id, ACK_KEY).await.unwrap();
        let ack: BarrierAck = serde_json::from_str(&ack_raw).unwrap();
        assert_eq!(ack.epoch, 1);
        assert!(ack.ok, "prepare succeeded, ack should be ok");

        // Follower's manifest is on disk at the leader's epoch.
        let stored = coord.store().load_latest().await.unwrap().unwrap();
        assert_eq!(stored.epoch, 1);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_checkpoint_rolls_back_on_leader_abort() {
        use laminar_core::cluster::control::{
            BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let leader_id = NodeId(1);
        let follower_id = NodeId(9);
        let kv = Arc::new(InMemoryKv::new(follower_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let leader_info = NodeInfo {
            id: leader_id,
            name: "leader".into(),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let (_tx, rx) = watch::channel(vec![leader_info]);
        let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
        coord.set_cluster_controller(controller);

        let abort_json = serde_json::to_string(&BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Abort,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap();
        kv.seed(leader_id, ANNOUNCEMENT_KEY, abort_json);

        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let committed = coord
            .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(2))
            .await
            .unwrap();
        assert!(!committed, "follower should roll back on leader's Abort");
    }

    /// KV that records every announcement written, preserving order —
    /// the single-slot `InMemoryKv` only keeps the latest.
    #[cfg(feature = "cluster")]
    struct RecordingKv {
        inner: laminar_core::cluster::control::InMemoryKv,
        announcements: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    #[cfg(feature = "cluster")]
    #[async_trait::async_trait]
    impl laminar_core::cluster::control::ClusterKv for RecordingKv {
        async fn write(&self, key: &str, value: String) {
            if key == laminar_core::cluster::control::ANNOUNCEMENT_KEY {
                self.announcements.lock().push(value.clone());
            }
            self.inner.write(key, value).await;
        }
        async fn read_from(
            &self,
            who: laminar_core::cluster::discovery::NodeId,
            key: &str,
        ) -> Option<String> {
            self.inner.read_from(who, key).await
        }
        async fn scan(&self, key: &str) -> Vec<(laminar_core::cluster::discovery::NodeId, String)> {
            self.inner.scan(key).await
        }
        async fn scan_prefix(
            &self,
            prefix: &str,
        ) -> Vec<(laminar_core::cluster::discovery::NodeId, String, String)> {
            self.inner.scan_prefix(prefix).await
        }
    }

    /// Two-level completion: the leader must announce `Aligned`
    /// (the pipeline resume gate) after the capture quorum and *before*
    /// the durable tail's `Commit`.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn leader_announces_aligned_between_prepare_and_commit() {
        use laminar_core::cluster::control::{
            BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase,
        };
        use laminar_core::cluster::discovery::NodeId;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let self_id = NodeId(1);
        let announcements = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let kv: Arc<dyn ClusterKv> = Arc::new(RecordingKv {
            inner: InMemoryKv::new(self_id),
            announcements: Arc::clone(&announcements),
        });
        let (_tx, rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));
        coord.set_cluster_controller(controller);

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success);

        let phases: Vec<Phase> = announcements
            .lock()
            .iter()
            .map(|json| {
                serde_json::from_str::<BarrierAnnouncement>(json)
                    .unwrap()
                    .phase
            })
            .collect();
        assert_eq!(
            phases,
            vec![Phase::Prepare, Phase::Aligned, Phase::Commit],
            "two-level completion must announce Aligned between Prepare and Commit",
        );
    }

    /// The follower acks at capture (before its durable
    /// prepare). If the prepare then fails, a best-effort `ok = false`
    /// ack overwrites the capture ack so a still-polling leader can
    /// fail the quorum fast instead of waiting for its gate timeout.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_prepare_failure_overwrites_capture_ack() {
        use laminar_core::cluster::control::{
            BarrierAck, BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase,
            ACK_KEY,
        };
        use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
        use laminar_core::state::InProcessBackend;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let leader_id = NodeId(1);
        let follower_id = NodeId(7);
        let kv = Arc::new(InMemoryKv::new(follower_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let leader_info = NodeInfo {
            id: leader_id,
            name: "leader".into(),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        let (_tx, rx) = watch::channel(vec![leader_info]);
        let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
        coord.set_cluster_controller(controller);
        // Backend sized for 2 vnodes but the follower claims vnode 99 —
        // `write_vnode_partials` (the last prepare step) fails.
        coord.set_state_backend(Arc::new(InProcessBackend::new(2)));
        coord.set_vnode_set(vec![99]);

        let ann = BarrierAnnouncement {
            epoch: 1,
            checkpoint_id: 1,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        };
        let result = coord
            .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(1))
            .await;
        assert!(result.is_err(), "prepare failure must surface as an error");

        let ack_raw = kv.read_from(follower_id, ACK_KEY).await.unwrap();
        let ack: BarrierAck = serde_json::from_str(&ack_raw).unwrap();
        assert_eq!(ack.epoch, 1);
        assert!(!ack.ok, "the failure ack must overwrite the capture ack");
        assert!(
            ack.error.unwrap().contains("vnode partial write failed"),
            "failure ack should carry the prepare error",
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn leader_publishes_cluster_min_watermark_to_controller() {
        // On a solo cluster, `await_prepare_quorum` computes the
        // cluster-wide min as "leader's local watermark" (no followers
        // to fold). This must be mirrored into the controller atomic so
        // the leader's own operators consume the same value that
        // followers pick up via `observe_barrier(Commit)` — otherwise
        // the leader would drive event-time decisions off a watermark
        // that none of its peers have acked yet.
        use laminar_core::cluster::control::{
            CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv,
        };
        use laminar_core::cluster::discovery::NodeId;
        use object_store::local::LocalFileSystem;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let decision_dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let decision_os: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
        coord.set_decision_store(Arc::new(CheckpointDecisionStore::new(decision_os)));

        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let (_tx, rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
        coord.set_cluster_controller(Arc::clone(&controller));

        // Pre-condition: controller atomic is at its "unset" sentinel.
        assert_eq!(controller.cluster_min_watermark(), None);

        // Seed a local watermark on the coordinator and drive a full
        // checkpoint. Solo cluster → leader's local value *is* the
        // cluster-wide min.
        coord.set_local_watermark_ms(Some(12_345));
        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success, "solo-cluster checkpoint should succeed");

        assert_eq!(
            controller.cluster_min_watermark(),
            Some(12_345),
            "leader must mirror the cluster-wide min into its controller",
        );

        // A subsequent checkpoint with a lower local watermark must
        // NOT regress the published value — event-time progress is
        // monotonic (same invariant the follower path already enforces).
        coord.set_local_watermark_ms(Some(42));
        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success);
        assert_eq!(
            controller.cluster_min_watermark(),
            Some(12_345),
            "stale local watermark must not lower the published cluster min",
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn leader_announces_prepare_and_commit_on_solo_cluster() {
        use laminar_core::cluster::control::{
            CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv, Phase,
            ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use object_store::local::LocalFileSystem;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let decision_dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let decision_os: Arc<dyn object_store::ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
        coord.set_decision_store(Arc::new(CheckpointDecisionStore::new(decision_os)));

        let self_id = NodeId(1);
        let kv = Arc::new(InMemoryKv::new(self_id));
        let kv_trait: Arc<dyn ClusterKv> = kv.clone();
        let (_tx, rx) = watch::channel(Vec::new()); // solo — no peers
        let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
        coord.set_cluster_controller(controller);

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success, "solo-cluster checkpoint should succeed");

        // The last announce on the leader's KV is COMMIT (PREPARE was
        // overwritten in the same slot).
        let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
        let ann: laminar_core::cluster::control::BarrierAnnouncement =
            serde_json::from_str(&raw).unwrap();
        assert_eq!(ann.phase, Phase::Commit);
        assert_eq!(ann.epoch, result.epoch);
    }

    #[tokio::test]
    async fn gate_checks_full_registry_not_just_owned() {
        // Leader owns vnodes {0, 1}. Cluster has 4 vnodes total; a
        // follower (simulated by pre-populating half the backend) owns
        // {2, 3}. If the follower's markers are missing, the leader's
        // gate must fail even though the leader wrote its own.
        use laminar_core::state::InProcessBackend;
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator_with_fast_gate(dir.path()).await;
        let backend = Arc::new(InProcessBackend::new(4));
        coord.set_state_backend(backend.clone());
        coord.set_vnode_set(vec![0, 1]); // leader's owned subset
        coord.set_gate_vnode_set(vec![0, 1, 2, 3]); // full cluster registry

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(
            !result.success,
            "gate must fail when follower markers are missing",
        );
        let err = result.error.expect("failure produces an error message");
        assert!(
            err.contains("not all vnodes persisted"),
            "expected full-registry gate miss, got: {err}",
        );
    }

    /// Followers ack at capture and upload partials
    /// asynchronously, so the leader's restorable gate must *wait* for
    /// late partials rather than failing on the first check.
    #[tokio::test]
    async fn restorable_gate_waits_for_async_follower_uploads() {
        use bytes::Bytes;
        use laminar_core::state::InProcessBackend;
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;
        let backend = Arc::new(InProcessBackend::new(4));
        // Leader's own partials are present; the "follower's" vnodes
        // {2, 3} land only after a delay, simulating its background
        // upload completing while the leader polls.
        backend
            .write_partial(0, 1, 0, Bytes::from_static(b"leader"))
            .await
            .unwrap();
        backend
            .write_partial(1, 1, 0, Bytes::from_static(b"leader"))
            .await
            .unwrap();
        let late = Arc::clone(&backend);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            for v in [2u32, 3] {
                late.write_partial(v, 1, 0, Bytes::from_static(b"follower"))
                    .await
                    .unwrap();
            }
        });
        coord.set_state_backend(backend);
        coord.set_vnode_set(vec![0, 1]);
        coord.set_gate_vnode_set(vec![0, 1, 2, 3]);

        let start = std::time::Instant::now();
        coord
            .await_restorable_gate(1, &[])
            .await
            .expect("gate must seal once the late partials land");
        assert!(
            start.elapsed() >= Duration::from_millis(250),
            "gate returned before the late partials could have landed",
        );
    }

    #[tokio::test]
    async fn gate_passes_when_all_registry_markers_present() {
        // Same topology as the previous test, but now the follower's
        // markers are pre-populated — the gate sees a complete set
        // across the full registry and the checkpoint succeeds.
        use bytes::Bytes;
        use laminar_core::state::InProcessBackend;
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;
        let backend = Arc::new(InProcessBackend::new(4));
        // Simulate the follower's prior write on vnodes {2, 3} for the
        // epoch the leader is about to use (fresh store starts at 1).
        backend
            .write_partial(2, 1, 0, Bytes::from_static(b"follower"))
            .await
            .unwrap();
        backend
            .write_partial(3, 1, 0, Bytes::from_static(b"follower"))
            .await
            .unwrap();
        coord.set_state_backend(backend);
        coord.set_vnode_set(vec![0, 1]);
        coord.set_gate_vnode_set(vec![0, 1, 2, 3]);

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success, "gate should pass: every vnode has a marker");
    }

    #[tokio::test]
    async fn marker_write_failure_aborts_checkpoint() {
        use laminar_core::state::InProcessBackend;
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;
        // Backend is sized for 2 vnodes, but we claim to own vnode 99 →
        // bridge fails its write, checkpoint aborts cleanly.
        coord.set_state_backend(Arc::new(InProcessBackend::new(2)));
        coord.set_vnode_set(vec![0, 99]);

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(
            !result.success,
            "out-of-range vnode must fail the checkpoint"
        );
        let err = result.error.expect("failure produces an error message");
        assert!(err.contains("vnode partial write failed"), "got: {err}");
    }

    /// A vnode whose slices didn't change uploads a
    /// reference to its last full partial instead of the state bytes,
    /// and is forced back to full before the base ages out of the
    /// prune retention window.
    #[tokio::test]
    async fn unchanged_vnode_state_becomes_reference_partial() {
        use laminar_core::state::InProcessBackend;

        let dir = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            max_retained: 2, // reference age cap = 2 epochs
            ..CheckpointConfig::default()
        };
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
        let backend = Arc::new(InProcessBackend::new(2));
        coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
        coord.set_vnode_set(vec![0]);

        let slices = || {
            let mut ops = std::collections::HashMap::new();
            ops.insert(
                "agg".to_string(),
                StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v1")),
            );
            std::collections::HashMap::from([(0u32, ops)])
        };

        // Epoch 1: full upload.
        coord.set_pending_vnode_states(slices());
        let r1 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r1.success);
        let p1 = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(0, r1.epoch).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(p1.base_epoch, None, "first upload must be full");
        assert!(!p1.operators.is_empty());

        // Epoch 2: identical slices → reference to epoch 1.
        coord.set_pending_vnode_states(slices());
        let r2 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r2.success);
        let p2 = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(0, r2.epoch).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(
            p2.base_epoch,
            Some(r1.epoch),
            "unchanged slice must reference its base"
        );
        assert!(p2.operators.is_empty());

        // Epoch 3: still identical, but the base would hit the age cap —
        // forced back to a full upload (new base).
        coord.set_pending_vnode_states(slices());
        let r3 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r3.success);
        let p3 = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(0, r3.epoch).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(
            p3.base_epoch, None,
            "reference age cap must force a full re-upload",
        );

        // Changed slices always upload full.
        let mut changed = std::collections::HashMap::new();
        let mut ops = std::collections::HashMap::new();
        ops.insert(
            "agg".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v2")),
        );
        changed.insert(0u32, ops);
        coord.set_pending_vnode_states(changed);
        let r4 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r4.success);
        let p4 = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(0, r4.epoch).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(p4.base_epoch, None);
        assert!(!p4.operators.is_empty());
    }

    /// A demoted (cold-staged) slice keeps emitting references while its
    /// base is fresh, and a forced full re-upload (base hitting the age
    /// cap) fetches the bytes back from the tier instead of dropping the
    /// slice from recovery truth.
    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn cold_slice_references_then_tier_fetch_on_forced_full() {
        use laminar_core::state::InProcessBackend;

        let tier_dir = tempfile::tempdir().unwrap();
        let tier = Arc::new(
            crate::state_tier::StateTierStore::open(tier_dir.path().join("tier"), None).unwrap(),
        );
        tier.put("agg", 0, b"state-v1").unwrap();
        let tier_tx = crate::state_tier::spawn_worker(&tokio::runtime::Handle::current(), tier, 16);

        let dir = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            max_retained: 2, // reference age cap = 2 epochs
            ..CheckpointConfig::default()
        };
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
        let backend = Arc::new(InProcessBackend::new(2));
        coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
        coord.set_vnode_set(vec![0]);
        coord.set_state_tier(tier_tx);

        // Epoch 1: full upload while the slice is still memory-resident.
        let mut ops = std::collections::HashMap::new();
        ops.insert(
            "agg".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v1")),
        );
        coord.set_pending_vnode_states(std::collections::HashMap::from([(0u32, ops)]));
        let r1 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r1.success);

        // Demote: release the in-memory pin; subsequent captures stage Cold.
        assert_eq!(coord.demotion_candidates(), vec![(0, b"state-v1".len())]);
        let slices = coord.slices_for_demotion(0);
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].1.as_ref(), b"state-v1");
        coord.mark_slice_demoted(0, "agg");
        assert!(coord.demotion_candidates().is_empty());

        // Epoch 2: Cold staged → reference to epoch 1, no bytes needed.
        let cold = || {
            let mut ops = std::collections::HashMap::new();
            ops.insert("agg".to_string(), StagedSlice::Cold);
            std::collections::HashMap::from([(0u32, ops)])
        };
        coord.set_pending_vnode_states(cold());
        let r2 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r2.success);
        let p2 = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(0, r2.epoch).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(p2.base_epoch, Some(r1.epoch), "cold slice must reference");

        // Epoch 3: still cold, base ages out → full re-upload from the tier.
        coord.set_pending_vnode_states(cold());
        let r3 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(
            r3.success,
            "forced full must fetch from the tier: {:?}",
            r3.error
        );
        let p3 = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(0, r3.epoch).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(p3.base_epoch, None);
        assert_eq!(p3.operators.len(), 1);
        assert_eq!(p3.operators[0].0, "agg");
        assert_eq!(
            p3.operators[0].1, b"state-v1",
            "the re-uploaded slice must be the tier's bytes"
        );
    }

    /// A forced full re-upload whose cold slice is missing from the tier
    /// must fail the epoch — writing the partial without it would silently
    /// drop the slice from recovery truth.
    #[cfg(feature = "state-tier")]
    #[tokio::test]
    async fn cold_slice_missing_from_tier_fails_epoch() {
        use laminar_core::state::InProcessBackend;

        let tier_dir = tempfile::tempdir().unwrap();
        let tier = Arc::new(
            crate::state_tier::StateTierStore::open(tier_dir.path().join("tier"), None).unwrap(),
        );
        // Deliberately empty tier.
        let tier_tx = crate::state_tier::spawn_worker(&tokio::runtime::Handle::current(), tier, 16);

        let dir = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            max_retained: 2,
            ..CheckpointConfig::default()
        };
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
        let backend = Arc::new(InProcessBackend::new(2));
        coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
        coord.set_vnode_set(vec![0]);
        coord.set_state_tier(tier_tx);

        let mut ops = std::collections::HashMap::new();
        ops.insert(
            "agg".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v1")),
        );
        coord.set_pending_vnode_states(std::collections::HashMap::from([(0u32, ops)]));
        let r1 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r1.success);
        coord.mark_slice_demoted(0, "agg");

        let cold = || {
            let mut ops = std::collections::HashMap::new();
            ops.insert("agg".to_string(), StagedSlice::Cold);
            std::collections::HashMap::from([(0u32, ops)])
        };
        // Epoch 2 references; epoch 3 forces full → tier miss → failure.
        coord.set_pending_vnode_states(cold());
        assert!(
            coord
                .checkpoint(CheckpointRequest::default())
                .await
                .unwrap()
                .success
        );
        coord.set_pending_vnode_states(cold());
        let r3 = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(
            !r3.success,
            "a tier miss must fail the epoch, not drop state"
        );
        assert!(
            r3.error.unwrap().contains("missing from the state tier"),
            "failure must name the missing slice"
        );
    }

    /// `InProcessBackend` wrapper with a per-write delay (forces epoch
    /// overlap) and injected failures keyed by `(epoch, vnode)`.
    struct FaultBackend {
        inner: laminar_core::state::InProcessBackend,
        fail: parking_lot::Mutex<std::collections::HashSet<(u64, u32)>>,
        write_delay: Duration,
    }

    #[async_trait::async_trait]
    impl StateBackend for FaultBackend {
        async fn write_partial(
            &self,
            vnode: u32,
            epoch: u64,
            assignment_version: u64,
            bytes: bytes::Bytes,
        ) -> Result<(), laminar_core::state::StateBackendError> {
            tokio::time::sleep(self.write_delay).await;
            if self.fail.lock().contains(&(epoch, vnode)) {
                return Err(laminar_core::state::StateBackendError::Io(
                    "injected write failure".into(),
                ));
            }
            self.inner
                .write_partial(vnode, epoch, assignment_version, bytes)
                .await
        }

        async fn read_partial(
            &self,
            vnode: u32,
            epoch: u64,
        ) -> Result<Option<bytes::Bytes>, laminar_core::state::StateBackendError> {
            self.inner.read_partial(vnode, epoch).await
        }

        async fn epoch_complete(
            &self,
            epoch: u64,
            vnodes: &[u32],
        ) -> Result<bool, laminar_core::state::StateBackendError> {
            self.inner.epoch_complete(epoch, vnodes).await
        }

        async fn prune_before(
            &self,
            before: u64,
        ) -> Result<(), laminar_core::state::StateBackendError> {
            self.inner.prune_before(before).await
        }

        async fn latest_committed_epoch(
            &self,
        ) -> Result<Option<u64>, laminar_core::state::StateBackendError> {
            self.inner.latest_committed_epoch().await
        }

        fn set_authoritative_version(&self, version: u64) {
            self.inner.set_authoritative_version(version);
        }

        fn authoritative_version(&self) -> u64 {
            self.inner.authoritative_version()
        }
    }

    /// Fault injection at pipeline depth > 1. Four
    /// epochs are admitted (ids allocated, tails spawned) while the
    /// first is still uploading; the third epoch's upload partially
    /// fails — one vnode's write lands, the other's is injected to
    /// fail. Must hold:
    /// - tails complete in admission order (FIFO coordinator mutex);
    /// - the failed epoch is abandoned without disturbing successors;
    /// - the recovery point is the last successful epoch;
    /// - the partial that *landed* for the failed epoch never becomes
    ///   a reference base (a successor with identical state must
    ///   re-upload full, or reference an older *successful* epoch).
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // four-epoch fault sequence reads better unsplit
    async fn overlapping_epoch_failure_is_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        let backend = Arc::new(FaultBackend {
            inner: laminar_core::state::InProcessBackend::new(2),
            fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
            write_delay: Duration::from_millis(100),
        });
        coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
        coord.set_vnode_set(vec![0, 1]);

        let allocator = coord.epoch_allocator();
        let coordinator = Arc::new(tokio::sync::Mutex::new(Some(coord)));
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<CheckpointResult>();

        // Admit an epoch exactly as the pipeline callback does: claim
        // ids lock-free, spawn the tail; the FIFO mutex serializes the
        // durable work.
        let admit = |tag: &'static [u8]| {
            let (epoch, checkpoint_id) = allocator.allocate();
            let coordinator = Arc::clone(&coordinator);
            let done = done_tx.clone();
            let states = std::collections::HashMap::from([
                (
                    0u32,
                    std::collections::HashMap::from([(
                        "agg".to_string(),
                        StagedSlice::Bytes(bytes::Bytes::from_static(tag)),
                    )]),
                ),
                (
                    1u32,
                    std::collections::HashMap::from([(
                        "agg".to_string(),
                        StagedSlice::Bytes(bytes::Bytes::from_static(tag)),
                    )]),
                ),
            ]);
            tokio::spawn(async move {
                let mut guard = coordinator.lock().await;
                let coord = guard.as_mut().unwrap();
                coord.set_pending_vnode_states(states);
                let result = coord
                    .checkpoint_preallocated(
                        CheckpointRequest::default(),
                        epoch,
                        checkpoint_id,
                        QuorumStage::RunInline,
                    )
                    .await
                    .unwrap();
                done.send(result).unwrap();
            });
            epoch
        };

        // All four admitted while epoch A's tail is still uploading
        // (each write sleeps 100ms; admissions are microseconds apart,
        // paced just enough that lock-queue order is admission order).
        let a = admit(b"v1");
        tokio::time::sleep(Duration::from_millis(10)).await;
        let b = admit(b"v1"); // unchanged → reference to A
        tokio::time::sleep(Duration::from_millis(10)).await;
        let (c_epoch, _) = allocator.peek();
        backend.fail.lock().insert((c_epoch, 1)); // vnode 0 lands, vnode 1 fails
        let c = admit(b"v2"); // changed → full attempt, partially fails
        tokio::time::sleep(Duration::from_millis(10)).await;
        let d = admit(b"v2"); // same state as the failed epoch

        let mut results = Vec::new();
        for _ in 0..4 {
            results.push(done_rx.recv().await.unwrap());
        }

        assert_eq!(
            results.iter().map(|r| r.epoch).collect::<Vec<_>>(),
            vec![a, b, c, d],
            "tails must complete in admission order",
        );
        assert_eq!(
            results.iter().map(|r| r.success).collect::<Vec<_>>(),
            vec![true, true, false, true],
            "the failed epoch must not disturb its successors",
        );
        assert!(results[2]
            .error
            .as_deref()
            .is_some_and(|e| e.contains("vnode partial write failed")));

        // Recovery point: the failed epoch was never sealed.
        assert_eq!(
            backend.latest_committed_epoch().await.unwrap(),
            Some(d),
            "the last successful epoch is the recovery point",
        );

        // B was unchanged from A → reference. D matches the FAILED
        // epoch's state, and C's vnode-0 write landed before the
        // injected failure — D must not reference it (bases are
        // recorded only after every write in an epoch lands).
        let p_b = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(0, b).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(p_b.base_epoch, Some(a));
        for vnode in [0u32, 1] {
            let p_d = crate::vnode_partial::VnodePartial::decode(
                &backend.read_partial(vnode, d).await.unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(
                p_d.base_epoch, None,
                "vnode {vnode}: a successor of a failed epoch must re-upload full, \
                 never reference the failed epoch's stray partial",
            );
            assert_eq!(p_d.operators[0].1, b"v2");
        }
        assert_eq!(c, d - 1, "abandoned epoch's id is burned, not reused");
    }

    /// A follower persists its manifest before learning the leader
    /// aborted, so an aborted epoch's Pending manifest can be the
    /// highest on disk at restart. Construction seeds ids from it
    /// (high is safe); recovery then restores from the older committed
    /// epoch and must NOT walk the ids back down — that would
    /// re-allocate the aborted epoch over its stale artifacts.
    #[tokio::test]
    async fn recovery_never_walks_ids_back_onto_aborted_epochs() {
        use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;

        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 5);
        // Committed epoch 3.
        let mut committed = CheckpointManifest::new(3, 3);
        committed
            .sink_commit_statuses
            .insert("out".into(), SinkCommitStatus::Committed);
        store.save(&committed).await.unwrap();
        // Aborted epoch 5: persisted by a follower before the leader's
        // Abort, never committed.
        let mut aborted = CheckpointManifest::new(5, 5);
        aborted
            .sink_commit_statuses
            .insert("out".into(), SinkCommitStatus::Pending);
        store.save(&aborted).await.unwrap();

        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store))
            .await
            .unwrap();
        assert_eq!(coord.epoch(), 6, "seeds from the highest loadable manifest");

        let recovered = coord.recover().await.unwrap().expect("recovers");
        assert_eq!(recovered.epoch(), 3, "restores from the committed epoch");
        assert_eq!(
            coord.epoch(),
            6,
            "ids must stay above the aborted epoch, never re-allocating it",
        );
    }

    #[test]
    fn epoch_allocator_allocates_monotonic_pairs() {
        let a = EpochAllocator::new(5, 9);
        assert_eq!(a.peek(), (5, 9));
        assert_eq!(a.allocate(), (5, 9));
        assert_eq!(a.allocate(), (6, 10));
        assert_eq!(a.peek(), (7, 11));
        a.advance_to(20, 30);
        assert_eq!(a.allocate(), (20, 30));
        // Monotonic: never walks backwards.
        a.advance_to(5, 5);
        assert_eq!(a.peek(), (21, 31));
    }

    /// Ids are allocated at the start of an attempt: a failed epoch is
    /// abandoned (Flink-style), never retried under the same ids.
    #[tokio::test]
    async fn failed_epoch_is_abandoned_not_retried() {
        let dir = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            max_checkpoint_bytes: Some(16),
            ..CheckpointConfig::default()
        };
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

        // Oversized state → size-cap rejection.
        let mut ops = HashMap::new();
        ops.insert("big".to_string(), bytes::Bytes::from(vec![0u8; 2_000_000]));
        let failed = coord
            .checkpoint(CheckpointRequest {
                operator_states: ops,
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();
        assert!(!failed.success);

        let ok = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(ok.success);
        assert_eq!(
            ok.epoch,
            failed.epoch + 1,
            "the failed epoch must be abandoned, not reused",
        );
        assert_eq!(ok.checkpoint_id, failed.checkpoint_id + 1);
    }

    #[tokio::test]
    async fn test_stats_include_percentiles_after_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        // Run 3 checkpoints.
        for _ in 0..3 {
            let result = coord
                .checkpoint(CheckpointRequest::default())
                .await
                .unwrap();
            assert!(result.success);
        }

        let stats = coord.stats();
        assert_eq!(stats.completed, 3);
        // After 3 fast checkpoints, percentiles should be > 0
        // (they're real durations, not zero).
        assert!(stats.last_duration.is_some());
    }

    /// Sink whose `pre_commit` always fails; counts `rollback_epoch` calls.
    struct FailingPreCommitSink {
        rollback_count: Arc<std::sync::atomic::AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SinkConnector for FailingPreCommitSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &arrow::array::RecordBatch,
        ) -> Result<
            laminar_connectors::connector::WriteResult,
            laminar_connectors::error::ConnectorError,
        > {
            Ok(laminar_connectors::connector::WriteResult::new(0, 0))
        }

        async fn pre_commit(
            &mut self,
            epoch: u64,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            Err(laminar_connectors::error::ConnectorError::TransactionError(
                format!("synthetic pre_commit failure at epoch {epoch}"),
            ))
        }

        async fn rollback_epoch(
            &mut self,
            _epoch: u64,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            self.rollback_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
            laminar_connectors::connector::SinkConnectorCapabilities::new(Duration::from_secs(5))
                .with_exactly_once()
                .with_two_phase_commit()
                .with_preserves_pending_on_abandon()
        }
    }

    /// A pre-commit failure abandons the epoch but must NOT hard-roll
    /// back a connector that preserves pending output (see
    /// `SinkCommand::RollbackEpoch`): the pending rows ride into the
    /// next epoch's commit. Connectors without the capability ARE
    /// rolled back.
    #[tokio::test]
    async fn pre_commit_failure_abandons_without_connector_rollback() {
        use arrow::datatypes::{DataType, Field, Schema};

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

        let rollback_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let sink = FailingPreCommitSink {
            rollback_count: Arc::clone(&rollback_count),
            schema,
        };
        let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
            crate::sink_task::SinkEvent,
        >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
        let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "failing-sink".into(),
            sink_id: Arc::from("failing-sink"),
            connector: Box::new(sink),
            exactly_once: true,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_secs(5),
            event_tx,
        });
        coord.register_sink("failing-sink", handle, true);

        coord.begin_initial_epoch().await.unwrap();

        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();

        assert!(!result.success);
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|e| e.contains("pre-commit failed")),
            "error should mention pre-commit: got {:?}",
            result.error
        );
        assert_eq!(
            rollback_count.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a healthy sink must keep its pending output on a live abandon"
        );
    }

    /// Writes fail (poisoning the epoch); `rollback_epoch` hangs
    /// forever. The poisoned epoch is what makes the live abandon take
    /// the forced connector-rollback path that can hang.
    struct StuckRollbackSink {
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SinkConnector for StuckRollbackSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &arrow::array::RecordBatch,
        ) -> Result<
            laminar_connectors::connector::WriteResult,
            laminar_connectors::error::ConnectorError,
        > {
            Err(laminar_connectors::error::ConnectorError::WriteError(
                "synthetic write failure".into(),
            ))
        }

        async fn pre_commit(
            &mut self,
            _epoch: u64,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            Err(laminar_connectors::error::ConnectorError::TransactionError(
                "synthetic pre_commit failure".into(),
            ))
        }

        async fn rollback_epoch(
            &mut self,
            _epoch: u64,
        ) -> Result<(), laminar_connectors::error::ConnectorError> {
            // Hang until the test runtime drops us.
            std::future::pending::<()>().await;
            Ok(())
        }

        async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
            laminar_connectors::connector::SinkConnectorCapabilities::new(Duration::from_secs(5))
                .with_exactly_once()
                .with_two_phase_commit()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_rollback_sinks_bounded_by_timeout() {
        use arrow::datatypes::{DataType, Field, Schema};

        let dir = tempfile::tempdir().unwrap();
        let config = CheckpointConfig {
            rollback_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let store = Box::new(
            laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(dir.path(), 3),
        );
        let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let sink = StuckRollbackSink { schema };
        let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
            crate::sink_task::SinkEvent,
        >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
        let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
            name: "stuck-sink".into(),
            sink_id: Arc::from("stuck-sink"),
            connector: Box::new(sink),
            exactly_once: true,
            channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
            flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
            write_timeout: Duration::from_secs(5),
            event_tx,
        });
        coord.register_sink("stuck-sink", handle.clone(), true);
        coord.begin_initial_epoch().await.unwrap();

        // Poison the epoch with a failing write — only a poisoned sink
        // takes the forced connector-rollback path that can hang.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        handle.write_batch(batch).await.unwrap();
        handle.sync().await.unwrap();

        // Poisoned pre_commit fails → rollback_sinks fires → connector
        // rollback hangs → 100ms rollback_timeout fires → coordinator
        // returns instead of wedging.
        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();

        assert!(!result.success);
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|e| e.contains("pre-commit failed")),
            "checkpoint result should reflect pre-commit failure: got {:?}",
            result.error
        );
    }
}
