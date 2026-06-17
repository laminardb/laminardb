//! Checkpoint coordinator — Ring 2 control-plane orchestrator.
//!
//! Checkpoint manifest is the source of truth for source offsets; broker commits are advisory.
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
#[cfg_attr(not(feature = "cluster"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum StagedSlice {
    Bytes(bytes::Bytes),
    // No bytes; the coordinator emits a reference partial or fetches from the tier on a forced
    // full re-upload.
    Cold,
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
    /// `Cold` staged means unchanged (demotion contract: byte-identical to the recorded upload).
    /// Fresh bytes against a `Cold` record are conservatively "changed" — the cold bytes are
    /// unavailable for comparison, so the slice re-uploads full.
    fn matches(&self, staged: &StagedSlice) -> bool {
        match (staged, self) {
            (StagedSlice::Cold, _) => true,
            (StagedSlice::Bytes(b), UploadedSlice::Bytes(prev)) => b == prev,
            (StagedSlice::Bytes(_), UploadedSlice::Cold) => false,
        }
    }
}

/// Checkpoint configuration.
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
    /// Max wait for every vnode's partial to land before the epoch is declared restorable.
    ///
    /// Followers upload asynchronously after the capture ack, so the durability gate polls
    /// rather than checking once. Expiry aborts the epoch.
    pub restorable_gate_timeout: Duration,
    /// Max pipelined epochs between `Aligned` and restorable. Exactly-once pipelines cap at 1.
    pub max_in_flight_epochs: u64,
    /// Cap on in-flight captured-state bytes. At the cap, barrier admission pauses.
    pub max_staged_bytes: u64,
    /// Sealed-but-uncommitted epochs (designated-committer lag) past which a loud
    /// warning fires — the committer is falling behind and descriptors/data are
    /// accumulating in object storage (prune is held to avoid losing them).
    pub max_uncommitted_epochs: u64,
    /// When true, exceeding `max_uncommitted_epochs` fails the checkpoint
    /// (back-pressuring the pipeline) instead of only warning — bounds storage
    /// growth at the cost of halting progress until the committer catches up.
    pub uncommitted_epochs_backpressure: bool,
    /// How often the decoupled committer task polls for newly sealed epochs.
    pub coordinated_committer_poll: Duration,
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
            // Last-resort bound: fail-fasts catch dead participants in seconds.
            restorable_gate_timeout: Duration::from_secs(10),
            max_in_flight_epochs: 4,
            max_staged_bytes: 512 * 1024 * 1024,
            max_uncommitted_epochs: 1024,
            uncommitted_epochs_backpressure: false,
            coordinated_committer_poll: Duration::from_secs(1),
        }
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
    /// Pipeline topology hash for change detection.
    pub pipeline_hash: Option<u64>,
    /// Source offset overrides for recovery.
    pub source_offset_overrides: HashMap<String, ConnectorCheckpoint>,
}

/// Lock-free epoch/checkpoint-id allocator.
///
/// Ids are allocated at barrier admission, outside the coordinator mutex, so pipelined tails
/// can overlap. Failed epochs are abandoned, never re-allocated.
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
    /// The two counters advance independently; all callers today run on the pipeline task or
    /// under the coordinator mutex, so concurrent calls do not occur.
    pub(crate) fn allocate(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.epoch.fetch_add(1, Ordering::AcqRel),
            self.next_checkpoint_id.fetch_add(1, Ordering::AcqRel),
        )
    }

    /// The pair the next `allocate` would return, without consuming it.
    pub(crate) fn peek(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.epoch.load(Ordering::Acquire),
            self.next_checkpoint_id.load(Ordering::Acquire),
        )
    }

    /// Monotonically advance. An aborted epoch leaves artifacts (Pending manifest, partials);
    /// recovery from an older committed epoch must not re-allocate those ids.
    fn advance_to(&self, epoch: u64, next_checkpoint_id: u64) {
        use std::sync::atomic::Ordering;
        self.epoch.fetch_max(epoch, Ordering::AcqRel);
        self.next_checkpoint_id
            .fetch_max(next_checkpoint_id, Ordering::AcqRel);
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
    /// Sinks running phase-2 commit.
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
    /// Checkpoint ID assigned to this attempt.
    pub checkpoint_id: u64,
    /// Epoch number.
    pub epoch: u64,
    /// Wall time for the full checkpoint cycle.
    pub duration: Duration,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Registered source for checkpoint coordination.
pub(crate) struct RegisteredSource {
    pub name: String,
    pub connector: Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
    /// Sources without replay (e.g. WebSocket) degrade to at-most-once.
    pub supports_replay: bool,
}

/// Registered sink for checkpoint coordination.
pub(crate) struct RegisteredSink {
    pub name: String,
    pub handle: crate::sink_task::SinkTaskHandle,
    pub exactly_once: bool,
    pub coordinated_commit: bool,
}

/// Orchestrates the checkpoint lifecycle across sources, sinks, and operator state.
pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    store: Arc<dyn CheckpointStore>,
    sinks: Vec<RegisteredSink>,
    // Shared with the pipeline callback so barrier admission can claim ids without the mutex.
    allocator: Arc<EpochAllocator>,
    phase: CheckpointPhase,
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
    // Per-sink commit descriptors from `pre_commit` for the in-flight checkpoint,
    // persisted to the state backend in `write_vnode_partials`. Only coordinated
    // sinks contribute; empty otherwise.
    #[allow(clippy::disallowed_types)]
    pending_sink_descriptors: std::collections::HashMap<String, Vec<u8>>,
    // Descriptor keys actually written this epoch — the gate requires exactly
    // these (not all coordinated sinks), so an idle sink doesn't stall the seal.
    epoch_descriptor_keys: Vec<String>,
    // Lowest epoch the designated committer has NOT yet externally committed;
    // prune must not delete descriptors at or above it. Shared with the
    // committer task, which advances it. Starts at 0 (prune nothing until the
    // committer confirms) when coordinated sinks exist.
    coordinated_commit_floor: Arc<std::sync::atomic::AtomicU64>,
    // Bases for reference partials. Bytes are refcounted; demoted slices hold a cold marker.
    #[allow(clippy::disallowed_types)]
    last_vnode_uploads:
        std::collections::HashMap<u32, (u64, std::collections::HashMap<String, UploadedSlice>)>,
    // Channel to fetch demoted slice bytes back from the tier on a forced full re-upload.
    #[cfg(feature = "state-tier")]
    state_tier: Option<crate::state_tier::TierTx>,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    // Invalidated on `register_sink`; rebuilt on the next checkpoint.
    cached_sorted_sink_names: Option<Vec<String>>,
}

/// Load the highest-id manifest, tolerating a torn `latest.txt` pointer.
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
            pending_sink_descriptors: std::collections::HashMap::new(),
            epoch_descriptor_keys: Vec::new(),
            coordinated_commit_floor: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            last_vnode_uploads: std::collections::HashMap::new(),
            #[cfg(feature = "state-tier")]
            state_tier: None,
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            cached_sorted_sink_names: None,
        })
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
    ) {
        self.decision_store = Some(store);
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
    #[allow(dead_code)] // wired once the demotion trigger lands with promotion
    pub(crate) fn set_state_tier(&mut self, tier: crate::state_tier::TierTx) {
        self.state_tier = Some(tier);
    }

    /// Set the owned vnodes. Also the default gate set until `set_gate_vnode_set` is called.
    pub fn set_vnode_set(&mut self, vnodes: Vec<u32>) {
        if self.gate_vnode_set.is_empty() {
            self.gate_vnode_set.clone_from(&vnodes);
        }
        self.rotation_epoch_floor = self.allocator.peek().0;
        // Drop bases for shed vnodes; the new owner builds its own from a full upload.
        self.last_vnode_uploads.retain(|v, _| vnodes.contains(v));
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

    /// Without `state-tier`, a `Cold` slice cannot be staged; reaching this is a logic error.
    #[cfg(not(feature = "state-tier"))]
    #[allow(clippy::unused_async)]
    async fn fetch_cold_slice(&self, operator: &str, vnode: u32) -> Result<bytes::Bytes, DbError> {
        Err(DbError::Checkpoint(format!(
            "cold slice staged without state-tier support \
             (operator={operator}, vnode={vnode})"
        )))
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
        exactly_once: bool,
        coordinated_commit: bool,
    ) {
        self.sinks.push(RegisteredSink {
            name: name.into(),
            handle,
            exactly_once,
            coordinated_commit,
        });
        self.cached_sorted_sink_names = None;
    }

    /// Build the decoupled committer for coordinated-commit sinks, or `None`
    /// when there are none (or no state backend to read descriptors from).
    pub(crate) fn coordinated_committer(
        &self,
    ) -> Option<crate::coordinated_committer::CoordinatedCommitter> {
        let backend = self.state_backend.clone()?;
        let sinks: Vec<(String, crate::sink_task::SinkTaskHandle)> = self
            .sinks
            .iter()
            .filter(|s| s.coordinated_commit)
            .map(|s| (s.name.clone(), s.handle.clone()))
            .collect();
        if sinks.is_empty() {
            return None;
        }
        let committer = crate::coordinated_committer::CoordinatedCommitter::new(
            backend,
            sinks,
            Arc::clone(&self.coordinated_commit_floor),
        )
        .with_metrics(self.prom.clone())
        .with_max_uncommitted_epochs(self.config.max_uncommitted_epochs);
        #[cfg(feature = "cluster")]
        let committer = committer.with_cluster_controller(self.cluster_controller.clone());
        Some(committer)
    }

    /// Poll interval for the decoupled committer task.
    pub(crate) fn committer_poll_interval(&self) -> Duration {
        self.config.coordinated_committer_poll
    }

    /// Begin the initial epoch on all exactly-once sinks.
    ///
    /// Must be called once after all sinks are registered and before any writes. Subsequent
    /// epochs are started automatically after each successful checkpoint commit.
    ///
    /// # Errors
    /// Returns the first sink error.
    pub async fn begin_initial_epoch(&self) -> Result<(), DbError> {
        self.begin_epoch_for_sinks(self.allocator.peek().0).await
    }

    /// Shared id allocator — the pipeline callback clones this to allocate without the mutex.
    pub(crate) fn epoch_allocator(&self) -> Arc<EpochAllocator> {
        Arc::clone(&self.allocator)
    }

    /// Begin an epoch on all exactly-once sinks, rolling back already-started sinks on failure.
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
        self.checkpoint_inner(request, None, QuorumStage::RunInline)
            .await
    }

    /// Pre-commit all exactly-once sinks (phase 1), bounded by `pre_commit_timeout`.
    /// Returns the per-sink commit descriptors from coordinated-commit sinks.
    async fn pre_commit_sinks(
        &self,
        epoch: u64,
    ) -> Result<std::collections::HashMap<String, Vec<u8>>, DbError> {
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

        if let Some(ref m) = self.prom {
            m.sink_precommit_duration
                .observe(start.elapsed().as_secs_f64());
        }

        result
    }

    /// Inner pre-commit loop (no timeout). Fires all exactly-once sinks concurrently
    /// and collects commit descriptors from coordinated-commit sinks.
    async fn pre_commit_sinks_inner(
        &self,
        epoch: u64,
    ) -> Result<std::collections::HashMap<String, Vec<u8>>, DbError> {
        let futures = self.sinks.iter().filter(|s| s.exactly_once).map(|sink| {
            let handle = sink.handle.clone();
            let name = sink.name.clone();
            let coordinated = sink.coordinated_commit;
            async move {
                match handle.pre_commit(epoch).await {
                    Ok(descriptor) => {
                        debug!(sink = %name, epoch, "sink pre-committed");
                        // Only coordinated sinks contribute a descriptor for the committer.
                        Ok(descriptor.filter(|_| coordinated).map(|d| (name, d)))
                    }
                    Err(e) => Err(DbError::Checkpoint(format!(
                        "sink '{name}' pre-commit failed: {e}"
                    ))),
                }
            }
        });
        let collected = futures::future::try_join_all(futures).await?;
        Ok(collected.into_iter().flatten().collect())
    }

    /// Commit each exactly-once sink in its own task, bounded by `commit_timeout`.
    ///
    /// Per-sink tasks isolate failures; cancellation is internal so an outer drop
    /// never leaves a dangling oneshot ack on the sink side.
    async fn commit_sinks_tracked(&self, epoch: u64) -> HashMap<String, SinkCommitStatus> {
        let timeout_dur = self.config.commit_timeout;
        let start = std::time::Instant::now();

        let tasks: Vec<_> = self
            .sinks
            .iter()
            // Coordinated sinks commit via the decoupled committer, not here.
            .filter(|s| s.exactly_once && !s.coordinated_commit)
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

    /// Save a manifest (and optional sidecar) to the store, bounded by `persist_timeout`.
    ///
    /// Sidecar is written before the manifest: a failed sidecar write never leaves a
    /// manifest referencing missing state.
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

    /// This coordinator's node id, used to namespace commit descriptors.
    /// Always 0 without the cluster feature (single-instance leader-of-one).
    #[cfg_attr(not(feature = "cluster"), allow(clippy::unused_self))]
    fn self_node_id(&self) -> u64 {
        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.as_ref() {
            return cc.instance_id().0;
        }
        0
    }

    /// Commit-descriptor keys this node produced this epoch — the gate requires
    /// exactly these. Other nodes' descriptors are covered transitively: each
    /// node writes its descriptors before its vnode partials, so the partial
    /// gate implies them.
    fn self_descriptor_keys(&self) -> Vec<String> {
        self.epoch_descriptor_keys.clone()
    }

    /// Take the staged commit descriptors and persist them to the state backend.
    /// Fails the epoch if a coordinated sink produced descriptors but no backend
    /// is configured (rather than silently dropping committables).
    async fn take_and_persist_descriptors(&mut self, epoch: u64) -> Result<(), DbError> {
        let descriptors = std::mem::take(&mut self.pending_sink_descriptors);
        // The gate requires exactly the descriptors produced this epoch — an
        // idle sink contributes none and must not stall the seal.
        let node = self.self_node_id();
        self.epoch_descriptor_keys = descriptors
            .keys()
            .map(|sink| crate::coordinated_committer::descriptor_key(node, sink))
            .collect();
        if descriptors.is_empty() {
            return Ok(());
        }
        let Some(ref backend) = self.state_backend else {
            return Err(DbError::Checkpoint(
                "coordinated-commit sink produced descriptors but no state \
                 backend is configured to persist them"
                    .into(),
            ));
        };
        self.persist_sink_descriptors(backend, epoch, &descriptors)
            .await
    }

    /// Persist this epoch's coordinated-commit descriptors to the state backend.
    async fn persist_sink_descriptors(
        &self,
        backend: &Arc<dyn StateBackend>,
        epoch: u64,
        descriptors: &std::collections::HashMap<String, Vec<u8>>,
    ) -> Result<(), DbError> {
        let node = self.self_node_id();
        for (sink, descriptor) in descriptors {
            backend
                .write_commit_descriptor(
                    epoch,
                    &crate::coordinated_committer::descriptor_key(node, sink),
                    self.assignment_version,
                    bytes::Bytes::from(descriptor.clone()),
                )
                .await
                .map_err(|e| {
                    DbError::Checkpoint(format!(
                        "[LDB-6024] commit descriptor write failed \
                         (sink={sink}, epoch={epoch}): {e}"
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
        // Persist commit descriptors before partials so the partial gate
        // transitively implies their durability across all nodes.
        self.take_and_persist_descriptors(epoch).await?;
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
        // on a forced full upload the cold bytes are fetched back from the tier.
        // A fetch failure fails the epoch: silently dropping a demoted slice breaks recovery.
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
                        // Cold slices contribute bytes to this upload but stay pinned in the tier.
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

    /// Poll until every vnode in `gate_vnode_set` has its partial for `epoch`, or the
    /// gate timeout expires. Transient errors retry; a split-brain commit marker aborts.
    async fn await_restorable_gate(
        &self,
        epoch: u64,
        participants: &[QuorumPeer],
    ) -> Result<(), String> {
        use laminar_core::state::StateBackendError;

        // Each poll LISTs the epoch prefix; back off exponentially. Gates serialize on the
        // coordinator mutex so at most one loop runs at a time regardless of pipeline depth.
        const INITIAL_POLL: Duration = Duration::from_millis(100);
        const MAX_POLL: Duration = Duration::from_secs(1);

        let Some(ref backend) = self.state_backend else {
            return Ok(());
        };
        // Commit descriptors join the same `_COMMIT` seal as vnode partials.
        let required_descriptors = self.self_descriptor_keys();
        if self.gate_vnode_set.is_empty() && required_descriptors.is_empty() {
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
            match backend
                .epoch_complete(epoch, &self.gate_vnode_set, &required_descriptors)
                .await
            {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(e @ StateBackendError::SplitBrainCommit { .. }) => {
                    return Err(format!("state durability gate: {e}"));
                }
                Err(e) => {
                    debug!(epoch, error = %e, "durability gate poll error; retrying");
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

    /// Abandon a failed epoch: announce `Abort`, roll back sinks, and open the next epoch.
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
        self.pending_sink_descriptors.clear();
        self.epoch_descriptor_keys.clear();
        CheckpointResult {
            success: false,
            checkpoint_id,
            epoch,
            duration,
            error: Some(error),
        }
    }

    /// Begin the next epoch's sink transactions, bounded by `rollback_timeout`.
    ///
    /// The failing sink may be wedged; an unbounded await would hang the coordinator.
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

    /// On startup, reconcile any Pending sinks in the last manifest.
    ///
    /// Commit marker present → drive local commit; marker absent → rollback. In cluster mode
    /// the leader re-announces the decision.
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

        // Let the announcement gossip before the next tick.
        #[cfg(feature = "cluster")]
        if is_leader {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Overwrite Pending sink statuses in the manifest with recovery outcomes.
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

    /// Overwrite an existing manifest (e.g. sink commit statuses after phase 2).
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

    fn initial_sink_commit_statuses(&self) -> HashMap<String, SinkCommitStatus> {
        self.sinks
            .iter()
            // Coordinated sinks commit asynchronously via the designated committer
            // (guarded by catalog idempotency), so they are never manifest-tracked
            // — otherwise their Pending status would make recovery skip every
            // checkpoint on nodes that don't run the per-sink commit.
            .filter(|s| s.exactly_once && !s.coordinated_commit)
            .map(|s| (s.name.clone(), SinkCommitStatus::Pending))
            .collect()
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

    /// Roll back all exactly-once sinks in parallel, bounded by `rollback_timeout`.
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
                // Live abandon: a healthy sink keeps pending output; hard rollback
                // is reserved for the recovery manager on restart.
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

    fn collect_sink_epochs(&self, epoch: u64) -> HashMap<String, u64> {
        let mut epochs = HashMap::with_capacity(self.sinks.len());
        for sink in &self.sinks {
            if sink.exactly_once {
                epochs.insert(sink.name.clone(), epoch);
            }
        }
        epochs
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
        self.allocator.peek().0
    }

    /// Next checkpoint ID to be allocated.
    #[must_use]
    pub fn next_checkpoint_id(&self) -> u64 {
        self.allocator.peek().1
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
            current_epoch: self.allocator.peek().0,
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
        self.checkpoint_inner(request, None, QuorumStage::RunInline)
            .await
    }

    /// Checkpoint entry point for pipelined barriers where ids were pre-allocated and the
    /// capture quorum already ran (`QuorumStage::Done`).
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

    /// Abandon a pre-allocated epoch that failed before the mutex: announce `Abort`, roll
    /// back sinks, and begin the next epoch.
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

    /// Follower checkpoint: ack the capture, run the durable prepare, then wait for the
    /// leader's commit/abort. Returns `Ok(true)` = committed, `Ok(false)` = aborted.
    ///
    /// The ack means "aligned + captured"; the leader verifies prepare completion through
    /// the restorable gate (partials written last imply the full prepare finished).
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

        // State is captured; ack so the leader can release the pipeline.
        cc.ack_barrier(&BarrierAck {
            epoch: ann.epoch,
            ok: true,
            error: None,
            local_watermark_ms: self.local_watermark_ms,
        })
        .await
        .ok(); // best effort; leader's quorum tolerates missed acks

        self.follower_checkpoint_acked(request, ann, decision_timeout)
            .await
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

    /// Follower stage 1: durable prepare (pre-commit + manifest + partial uploads).
    ///
    /// On failure a best-effort `ok = false` ack overwrites the capture ack.
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

        // Monotonic: a late-finishing depth>1 tail must not walk ids back past a successor's.
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
            // Open the next epoch so post-failure writes stay transactional (mirrors fail_epoch).
            self.begin_next_epoch_bounded().await;
            return Err(e);
        }
        Ok(())
    }

    /// Follower stage 2: wait for the leader's decision without holding the coordinator mutex.
    ///
    /// Only Commit/Abort (or a newer epoch) end the wait. A superseded announcement defers
    /// to the durable marker. Returns the commit verdict.
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
                    if is_marked().await {
                        info!(
                            epoch,
                            checkpoint_id,
                            observed_epoch = a.epoch,
                            "newer epoch observed with commit marker present — committing",
                        );
                        return true;
                    }
                    // No marker yet; each check costs an object-store HEAD so pace the re-check.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                None => {
                    // Deadline: one final marker check.
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

    /// Follower stage 3: act on the decision. Returns `true` on a clean commit.
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
        // Both paths close the sinks' open transaction; open the next epoch (mirrors fail_epoch).
        self.begin_next_epoch_bounded().await;
        clean
    }

    /// Commit-marker store handle for the lock-free decision wait in pipelined follower tails.
    #[cfg(feature = "cluster")]
    pub(crate) fn decision_store_handle(
        &self,
    ) -> Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>> {
        self.decision_store.clone()
    }

    /// Commit this follower's sinks for `epoch` and overwrite the Pending manifest entries.
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
        match self.pre_commit_sinks(epoch).await {
            Ok(descriptors) => self.pending_sink_descriptors = descriptors,
            Err(e) => {
                self.pending_vnode_states.clear();
                return Err(e);
            }
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

    /// Shared checkpoint implementation for all entry points.
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
        // Flink-style: ids allocated up front; a failed epoch is abandoned, never retried.
        // Pipelined barrier paths allocate at admission; forced/timer paths allocate here.
        let (epoch, checkpoint_id) = ids.unwrap_or_else(|| self.allocator.allocate());

        info!(checkpoint_id, epoch, "starting checkpoint");

        self.phase = CheckpointPhase::Snapshotting;
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

        // Hard backpressure: refuse a new checkpoint while the designated
        // committer is too far behind, so descriptors/data can't grow unbounded.
        if self.config.uncommitted_epochs_backpressure
            && self.sinks.iter().any(|s| s.coordinated_commit)
        {
            let committed = self
                .coordinated_commit_floor
                .load(std::sync::atomic::Ordering::Acquire)
                .saturating_sub(1);
            let uncommitted = epoch.saturating_sub(committed);
            if uncommitted > self.config.max_uncommitted_epochs {
                return Ok(self
                    .fail_epoch(
                        checkpoint_id,
                        epoch,
                        start,
                        format!(
                            "[LDB-6031] coordinated committer {uncommitted} epochs behind \
                             (cap {}) — back-pressuring checkpoints",
                            self.config.max_uncommitted_epochs
                        ),
                    )
                    .await);
            }
        }

        self.phase = CheckpointPhase::PreCommitting;
        match self.pre_commit_sinks(epoch).await {
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

        let mut manifest = CheckpointManifest::new(checkpoint_id, epoch);
        manifest.source_offsets = source_offsets;
        manifest.table_offsets = table_offsets;
        manifest.sink_epochs = self.collect_sink_epochs(epoch);
        manifest.sink_commit_statuses = self.initial_sink_commit_statuses();
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
        // Arc so `save_manifest` gets a refcount bump without a deep clone.
        // After the await we're the sole owner; `Arc::make_mut` below is zero-copy.
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

        // Level 2 ("restorable"): all vnodes persisted before sinks commit.
        // Polls because followers upload asynchronously after their capture ack.
        #[cfg(not(feature = "cluster"))]
        let quorum_participants: Vec<QuorumPeer> = Vec::new();
        let gate_start = Instant::now();
        let gate_result = self
            .await_restorable_gate(epoch, &quorum_participants)
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

        // Write the commit marker before sink commits; recovery uses this to distinguish a
        // committed epoch from a crash mid-flight. Leader-gated in cluster mode.
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
            // Refcount is 1 here; `Arc::make_mut` is a zero-copy borrow.
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
            // The commit decision is durable; don't roll back. Failed statuses are re-driven
            // by `reconcile_prepared_on_init` on restart.
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

        if let Some(ref m) = self.prom {
            #[allow(clippy::cast_possible_wrap)]
            m.checkpoint_size_bytes.set(checkpoint_bytes as i64);
        }

        // Prune old partials/markers outside the retention window.
        if let Some(ref backend) = self.state_backend {
            let mut horizon = epoch.saturating_sub(self.config.max_retained as u64);
            // Never prune descriptors the designated committer hasn't committed
            // yet — hold the horizon at the commit floor for coordinated sinks.
            if self.sinks.iter().any(|s| s.coordinated_commit) {
                horizon = horizon.min(
                    self.coordinated_commit_floor
                        .load(std::sync::atomic::Ordering::Acquire),
                );
            }
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

        // A `begin_epoch` failure for the next epoch does not retroactively fail this one.
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
    pub async fn recover(
        &mut self,
    ) -> Result<Option<crate::recovery_manager::RecoveredState>, DbError> {
        use crate::recovery_manager::RecoveryManager;

        let mgr = RecoveryManager::new(&*self.store);
        // Sources are restored by the pipeline lifecycle; pass empty slices here.
        let result = mgr.recover(&[], &self.sinks, &[]).await?;

        if let Some(ref recovered) = result {
            // Monotonic: the committed epoch may predate a Pending manifest that seeded ids.
            self.allocator
                .advance_to(recovered.epoch() + 1, recovered.manifest.checkpoint_id + 1);
            let (epoch, checkpoint_id) = self.allocator.peek();
            info!(epoch, checkpoint_id, "coordinator epoch set after recovery");
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
#[derive(Clone)]
pub struct DurationHistogram {
    samples: Box<[u64; Self::CAPACITY]>,
    cursor: usize,
    count: u64,
}

impl Default for DurationHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl DurationHistogram {
    const CAPACITY: usize = 100;

    /// Empty histogram.
    #[must_use]
    pub fn new() -> Self {
        Self {
            samples: Box::new([0; Self::CAPACITY]),
            cursor: 0,
            count: 0,
        }
    }

    /// Record a duration sample.
    pub fn record(&mut self, duration: Duration) {
        #[allow(clippy::cast_possible_truncation)]
        let us = duration.as_micros() as u64;
        self.samples[self.cursor] = us;
        self.cursor = (self.cursor + 1) % Self::CAPACITY;
        self.count += 1;
    }

    /// True if no samples have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Number of recorded samples, up to `CAPACITY`.
    #[must_use]
    pub fn len(&self) -> usize {
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

    /// Returns `(p50, p95, p99)` in microseconds, sorting once.
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
pub fn source_to_connector_checkpoint(cp: &SourceCheckpoint) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: cp.offsets().clone(),
        epoch: cp.epoch(),
        metadata: cp.metadata().clone(),
    }
}

/// Convert a `ConnectorCheckpoint` back to a `SourceCheckpoint`.
#[must_use]
pub fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source_cp = SourceCheckpoint::with_offsets(cp.epoch, cp.offsets.clone());
    for (k, v) in &cp.metadata {
        source_cp.set_metadata(k.clone(), v.clone());
    }
    source_cp
}

#[cfg(test)]
mod tests;
