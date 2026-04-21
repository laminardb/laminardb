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
use laminar_storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, SinkCommitStatus,
};
use laminar_storage::checkpoint_store::CheckpointStore;
use tracing::{debug, error, info, warn};

use crate::error::DbError;

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
#[derive(Debug, serde::Serialize)]
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
    next_checkpoint_id: u64,
    epoch: u64,
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
    /// Stamped into every `write_partial` for the Phase 1.4 fence.
    /// Zero = fence disabled.
    assignment_version: u64,
    /// Durable cluster-wide 2PC decision store. Written by the leader
    /// after the prepare quorum but before the `Commit` announcement,
    /// so a new leader elected mid-2PC can recover the cluster vote
    /// from shared storage instead of guessing. `None` disables the
    /// durable-decision path (single-instance).
    #[cfg(feature = "cluster-unstable")]
    decision_store: Option<Arc<laminar_core::cluster::control::CheckpointDecisionStore>>,
    /// Reported in each `BarrierAck`; the leader folds these with its
    /// own watermark to compute the cluster-wide min.
    local_watermark_ms: Option<i64>,
    /// Cluster-wide min watermark as of the last committed epoch
    /// (leader-side; fanned out in the Commit announcement).
    #[cfg(feature = "cluster-unstable")]
    cluster_min_watermark: Option<i64>,
    /// Vnodes this coordinator owns; drives per-vnode marker writes.
    vnode_set: Vec<u32>,
    /// Vnodes the leader's durability gate checks. In cluster mode the
    /// full registry; single-instance mirrors `vnode_set`.
    gate_vnode_set: Vec<u32>,
    /// `Some` in cluster mode, `None` in single-instance / embedded.
    #[cfg(feature = "cluster-unstable")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Cached sorted sink names; invalidated on `register_sink`.
    cached_sorted_sink_names: Option<Vec<String>>,
}

impl CheckpointCoordinator {
    /// Creates a new checkpoint coordinator, seeded from the latest
    /// stored checkpoint.
    ///
    /// # Errors
    /// Returns [`DbError::Checkpoint`] if `store.load_latest()` fails.
    /// A store read error is surfaced rather than silently starting at
    /// `(1, 1)` and clobbering existing on-disk state. `Ok(None)` is
    /// the fresh-start path and is not an error.
    pub async fn new(
        config: CheckpointConfig,
        store: Box<dyn CheckpointStore>,
    ) -> Result<Self, DbError> {
        let store: Arc<dyn CheckpointStore> = Arc::from(store);
        let (next_id, epoch) = match store.load_latest().await {
            Ok(Some(m)) => (m.checkpoint_id + 1, m.epoch + 1),
            Ok(None) => (1, 1),
            Err(e) => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6028] failed to load latest checkpoint at coordinator \
                     construction: {e} — refusing to start at epoch 1 and \
                     clobber existing on-disk state"
                )));
            }
        };

        Ok(Self {
            config,
            store,
            sinks: Vec::new(),
            next_checkpoint_id: next_id,
            epoch,
            phase: CheckpointPhase::Idle,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            prom: None,
            total_bytes_written: 0,
            state_backend: None,
            assignment_version: 0,
            #[cfg(feature = "cluster-unstable")]
            decision_store: None,
            local_watermark_ms: None,
            #[cfg(feature = "cluster-unstable")]
            cluster_min_watermark: None,
            vnode_set: Vec::new(),
            gate_vnode_set: Vec::new(),
            #[cfg(feature = "cluster-unstable")]
            cluster_controller: None,
            cached_sorted_sink_names: None,
        })
    }

    /// Activates cluster-mode 2PC. Without this the coordinator runs
    /// single-instance semantics.
    #[cfg(feature = "cluster-unstable")]
    pub fn set_cluster_controller(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        self.cluster_controller = Some(controller);
    }

    #[cfg(feature = "cluster-unstable")]
    #[allow(dead_code)] // kept for future role-aware call sites
    pub(crate) fn cluster_controller(
        &self,
    ) -> Option<&Arc<laminar_core::cluster::control::ClusterController>> {
        self.cluster_controller.as_ref()
    }

    /// Wired with a non-empty `vnode_set` to enable per-vnode markers
    /// and the `epoch_complete` durability gate.
    pub fn set_state_backend(&mut self, backend: Arc<dyn StateBackend>) {
        self.state_backend = Some(backend);
    }

    /// Plug in the shared 2PC decision store. In cluster mode the
    /// leader writes `Committed` here after the prepare quorum but
    /// before announcing `Commit`, so a new leader elected mid-2PC
    /// can recover the cluster's verdict from shared storage. On
    /// startup, every node consults this store for any Pending epoch
    /// in its local manifest (see [`Self::reconcile_prepared_on_init`]).
    #[cfg(feature = "cluster-unstable")]
    pub fn set_decision_store(
        &mut self,
        store: Arc<laminar_core::cluster::control::CheckpointDecisionStore>,
    ) {
        self.decision_store = Some(store);
    }

    /// Record the assignment generation this coordinator is writing
    /// with. Forwarded to `backend.write_partial` so the Phase 1.4
    /// split-brain fence can reject stale writers. Host sets this
    /// whenever a fresh `AssignmentSnapshot` rotates in.
    pub fn set_assignment_version(&mut self, version: u64) {
        self.assignment_version = version;
    }

    /// Record this instance's current local watermark, reported in
    /// every subsequent `BarrierAck` so the leader can compute the
    /// cluster-wide minimum (Phase 1.3). `None` disables the
    /// per-follower contribution — leader falls back to its own
    /// watermark (and the other followers').
    pub fn set_local_watermark_ms(&mut self, watermark: Option<i64>) {
        self.local_watermark_ms = watermark;
    }

    /// Vnodes this instance owns; drives marker writes. Also the
    /// default gate set until [`Self::set_gate_vnode_set`] is called.
    pub fn set_vnode_set(&mut self, vnodes: Vec<u32>) {
        if self.gate_vnode_set.is_empty() {
            self.gate_vnode_set.clone_from(&vnodes);
        }
        self.vnode_set = vnodes;
    }

    /// Set the vnodes the leader's durability gate checks (the full
    /// registry in cluster mode). Defaults to `vnode_set` when unset.
    pub fn set_gate_vnode_set(&mut self, vnodes: Vec<u32>) {
        self.gate_vnode_set = vnodes;
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
        self.begin_epoch_for_sinks(self.epoch).await
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
        self.checkpoint_inner(request).await
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

    /// Commits all exactly-once sinks with per-sink status tracking.
    ///
    /// Returns a map of sink name → commit status. Sinks that committed
    /// successfully are `Committed`; failures are `Failed(message)`.
    /// All sinks are attempted even if some fail.
    ///
    /// Bounded by [`CheckpointConfig::commit_timeout`] to prevent a stuck
    /// sink from blocking checkpoint completion indefinitely.
    async fn commit_sinks_tracked(&self, epoch: u64) -> HashMap<String, SinkCommitStatus> {
        let timeout_dur = self.config.commit_timeout;
        let start = std::time::Instant::now();

        let statuses = match tokio::time::timeout(timeout_dur, self.commit_sinks_inner(epoch)).await
        {
            Ok(statuses) => statuses,
            Err(_elapsed) => {
                error!(
                    epoch,
                    timeout_secs = timeout_dur.as_secs(),
                    "[LDB-6012] sink commit timed out — marking all pending sinks as failed"
                );
                self.sinks
                    .iter()
                    .filter(|s| s.exactly_once)
                    .map(|s| {
                        (
                            s.name.clone(),
                            SinkCommitStatus::Failed(format!(
                                "sink '{}' commit timed out after {}s",
                                s.name,
                                timeout_dur.as_secs()
                            )),
                        )
                    })
                    .collect()
            }
        };

        // Record commit duration regardless of success/failure.
        if let Some(ref m) = self.prom {
            m.sink_commit_duration
                .observe(start.elapsed().as_secs_f64());
        }

        statuses
    }

    /// Inner commit loop (no timeout).
    ///
    /// Fires every sink's `commit_epoch` concurrently via `join_all`
    /// (not `try_join_all` — one sink failing must not short-circuit
    /// the others, because each sink's status is tracked independently
    /// and callers inspect the map). Matches the rollback path's
    /// pattern at `rollback_sinks_inner`.
    async fn commit_sinks_inner(&self, epoch: u64) -> HashMap<String, SinkCommitStatus> {
        let futures = self.sinks.iter().filter(|s| s.exactly_once).map(|sink| {
            let handle = sink.handle.clone();
            let name = sink.name.clone();
            async move {
                let status = match handle.commit_epoch(epoch).await {
                    Ok(()) => {
                        debug!(sink = %name, epoch, "sink committed");
                        SinkCommitStatus::Committed
                    }
                    Err(e) => {
                        let msg = format!("sink '{name}' commit failed: {e}");
                        error!(sink = %name, epoch, error = %e, "sink commit failed");
                        SinkCommitStatus::Failed(msg)
                    }
                };
                (name, status)
            }
        });
        let results = futures::future::join_all(futures).await;
        results.into_iter().collect()
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

    /// Writes a per-vnode durability marker so the leader's
    /// `epoch_complete` gate returns true and sinks can commit.
    ///
    /// Fires every vnode marker concurrently via `try_join_all`.
    /// The serial version was O(`vnode_count` × per-write latency) —
    /// ~256 awaits on a typical cluster, trivial CPU but each a
    /// scheduling hop (and on remote object-store backends each
    /// write is tens to hundreds of ms). Concurrent dispatch
    /// collapses that to one round-trip's worth of wall time.
    async fn write_vnode_markers(&self, epoch: u64, checkpoint_id: u64) -> Result<(), DbError> {
        let Some(ref backend) = self.state_backend else {
            return Ok(());
        };
        if self.vnode_set.is_empty() {
            return Ok(());
        }
        let payload = bytes::Bytes::from(format!("ckpt:{checkpoint_id}").into_bytes());
        // Phase 1.4: stamp every marker with the current assignment
        // generation. Zero means the host hasn't wired a version (e.g.
        // pre-Phase-1.2 single-instance path) and the fence is a no-op.
        let caller_version = self.assignment_version;
        let writes = self.vnode_set.iter().map(|&v| {
            let backend = Arc::clone(backend);
            let payload = payload.clone();
            async move {
                backend
                    .write_partial(v, epoch, caller_version, payload)
                    .await
                    .map_err(|e| {
                        DbError::Checkpoint(format!(
                            "[LDB-6024] vnode marker write failed (vnode={v}, epoch={epoch}): {e}"
                        ))
                    })
            }
        });
        futures::future::try_join_all(writes).await.map(|_| ())
    }

    /// At startup: if the last persisted manifest on this node has
    /// any `Pending` sink, consult the shared 2PC decision store for
    /// the authoritative verdict and drive local sinks to match.
    /// Called on both leaders and followers, so a crashed node coming
    /// back can heal its own half-committed state without waiting for
    /// leader coordination.
    ///
    /// Protocol:
    /// - `Decision::Committed` → drive local sinks commit (idempotent —
    ///   Kafka `commit_transaction` on an already-committed txid is a
    ///   fast-path no-op), rewrite the manifest's Pending entries with
    ///   the commit statuses we produced. If this node is currently
    ///   leader, also re-announce `Commit` so any still-Prepared
    ///   followers finish committing.
    /// - `Decision::Aborted` or no decision recorded → roll back local
    ///   sinks and (if leader) announce `Abort`.
    ///
    /// Absence of a decision marker is the "no quorum ever achieved"
    /// case — a leader that crashed before the commit-point write
    /// leaves followers in Prepared state, and the safe default is
    /// to release them. The decision store is the pivot: with it the
    /// protocol is always consistent.
    #[cfg(feature = "cluster-unstable")]
    pub async fn reconcile_prepared_on_init(&self) {
        use laminar_core::cluster::control::{Decision, Phase};

        let Ok(Some(last)) = self.store.load_latest().await else {
            return;
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

        let decision = match self.decision_store.as_ref() {
            Some(ds) => match ds.load(epoch).await {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        epoch,
                        checkpoint_id,
                        error = %e,
                        "[LDB-6040] failed to read decision store during recovery — \
                         defaulting to Abort",
                    );
                    None
                }
            },
            None => None,
        };

        let is_leader = self
            .cluster_controller
            .as_ref()
            .is_some_and(|cc| cc.is_leader());

        match decision {
            Some(Decision::Committed) => {
                info!(
                    epoch,
                    checkpoint_id,
                    "[LDB-6041] decision=Committed — driving local sinks to commit on startup",
                );
                let statuses = self.commit_sinks_tracked(epoch).await;
                if let Err(e) = self.persist_recovered_statuses(checkpoint_id, statuses).await {
                    warn!(
                        epoch,
                        checkpoint_id,
                        error = %e,
                        "[LDB-6042] post-recovery manifest update failed",
                    );
                }
                if is_leader {
                    self.announce_if_leader(epoch, checkpoint_id, Phase::Commit, None)
                        .await;
                }
            }
            Some(Decision::Aborted) | None => {
                if decision.is_some() {
                    info!(
                        epoch,
                        checkpoint_id,
                        "[LDB-6043] decision=Aborted — rolling back local sinks on startup",
                    );
                } else {
                    warn!(
                        epoch,
                        checkpoint_id,
                        "[LDB-6035] orphaned prepared epoch and no commit decision — \
                         rolling back local sinks",
                    );
                }
                if let Err(e) = self.rollback_sinks(epoch).await {
                    error!(
                        epoch,
                        checkpoint_id,
                        error = %e,
                        "[LDB-6044] sink rollback failed during recovery",
                    );
                }
                if is_leader {
                    self.announce_if_leader(epoch, checkpoint_id, Phase::Abort, None)
                        .await;
                }
            }
        }

        // Small pause lets the leader's announcement gossip out
        // before the next tick triggers a fresh Prepare. No-op on
        // followers.
        if is_leader {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Overwrite the Pending sink statuses on the last persisted
    /// manifest with the outcomes we produced during recovery. Called
    /// by [`Self::reconcile_prepared_on_init`] after it drives local
    /// sinks to the decision-store verdict.
    #[cfg(feature = "cluster-unstable")]
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
    /// consistent across the cluster. See Phase 1.3.
    #[cfg(feature = "cluster-unstable")]
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

    /// Announce PREPARE and block for follower acks. Returns `None` on
    /// quorum or no-op (not leader); `Some(msg)` with the failure.
    /// When quorum is reached, the `Ok` path writes the cluster-wide
    /// minimum watermark (leader's local + min of follower acks) into
    /// `self.cluster_min_watermark` so the subsequent `Commit`
    /// announcement can fan it out (Phase 1.3).
    #[cfg(feature = "cluster-unstable")]
    async fn await_prepare_quorum(&mut self, epoch: u64, checkpoint_id: u64) -> Option<String> {
        use laminar_core::cluster::control::{Phase, QuorumOutcome};
        let cc = self.cluster_controller.as_ref()?;
        if !cc.is_leader() {
            return None;
        }
        self.announce_if_leader(epoch, checkpoint_id, Phase::Prepare, None)
            .await;

        let mut followers = cc.live_instances();
        followers.retain(|id| *id != cc.instance_id());
        if followers.is_empty() {
            // Leader-only cluster — cluster-wide min is just the
            // leader's local watermark (if any).
            self.cluster_min_watermark = self.local_watermark_ms;
            if let Some(wm) = self.local_watermark_ms {
                cc.publish_cluster_min_watermark(wm);
            }
            return None;
        }

        let outcome = cc
            .wait_for_quorum(epoch, &followers, Duration::from_secs(30))
            .await;
        match outcome {
            QuorumOutcome::Reached {
                min_follower_watermark_ms,
                ..
            } => {
                // Fold follower min with the leader's own watermark.
                // Either may be `None` (unreported) — treated as
                // "non-blocking" (ignored from the min computation).
                let merged = match (self.local_watermark_ms, min_follower_watermark_ms) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };
                self.cluster_min_watermark = merged;
                // Leader-side mirror: publish to the controller atomic
                // so this instance's operators see the same value the
                // followers will pick up via `observe_barrier(Commit)`.
                // Without this, the leader's event-time decisions lag a
                // full checkpoint behind its own announcements.
                if let Some(wm) = merged {
                    cc.publish_cluster_min_watermark(wm);
                }
                None
            }
            QuorumOutcome::TimedOut { missing, .. } => {
                self.announce_if_leader(epoch, checkpoint_id, Phase::Abort, None)
                    .await;
                Some(format!(
                    "quorum timeout: {} follower(s) did not ack",
                    missing.len()
                ))
            }
            QuorumOutcome::Failed { failures } => {
                self.announce_if_leader(epoch, checkpoint_id, Phase::Abort, None)
                    .await;
                let first = failures.first().map_or("unknown", |(_, msg)| msg.as_str());
                Some(format!(
                    "follower snapshot failed on {} peer(s): {first}",
                    failures.len()
                ))
            }
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
                laminar_storage::checkpoint_manifest::OperatorCheckpoint::from_bytes_shared(
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
                let result = handle.rollback_epoch(epoch).await;
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

    /// Collects the last committed epoch from each sink.
    fn collect_sink_epochs(&self) -> HashMap<String, u64> {
        let mut epochs = HashMap::with_capacity(self.sinks.len());
        for sink in &self.sinks {
            // The epoch being committed is the current one
            if sink.exactly_once {
                epochs.insert(sink.name.clone(), self.epoch);
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

    /// Returns the current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the next checkpoint ID.
    #[must_use]
    pub fn next_checkpoint_id(&self) -> u64 {
        self.next_checkpoint_id
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
            current_epoch: self.epoch,
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
        self.checkpoint_inner(request).await
    }

    /// Follower half of a cluster checkpoint: pre-commit + save +
    /// markers + ack, then wait for the leader's commit/abort.
    /// `Ok(true)` = committed, `Ok(false)` = aborted/timed out.
    ///
    /// # Errors
    /// Propagates sink pre-commit, manifest save, or marker-write failures.
    #[cfg(feature = "cluster-unstable")]
    pub async fn follower_checkpoint(
        &mut self,
        request: CheckpointRequest,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::cluster::control::{BarrierAck, Phase};

        let Some(cc) = self.cluster_controller.clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6033] follower_checkpoint called without cluster controller".into(),
            ));
        };

        let epoch = ann.epoch;
        let checkpoint_id = ann.checkpoint_id;
        // Align our local counters with the leader's so any subsequent
        // leader-mode call on this coordinator resumes correctly.
        self.epoch = epoch;
        self.next_checkpoint_id = checkpoint_id.saturating_add(1);

        // Phase 1: local prepare.
        let prepare_result = self.follower_prepare(request, epoch, checkpoint_id).await;
        let prepare_err = prepare_result.as_ref().err().map(ToString::to_string);
        cc.ack_barrier(&BarrierAck {
            epoch,
            ok: prepare_err.is_none(),
            error: prepare_err.clone(),
            // Phase 1.3: stamp this follower's current watermark so the
            // leader can fold it into the cluster-wide min before
            // announcing Commit. `None` means "no watermark reported"
            // — the leader treats the follower as non-blocking (its
            // watermark is effectively +infinity).
            local_watermark_ms: self.local_watermark_ms,
        })
        .await
        .ok(); // best effort; leader's quorum wait tolerates missed acks
        if let Err(e) = prepare_result {
            self.rollback_sinks(epoch).await.ok();
            return Err(e);
        }

        // Phase 2: wait for the leader's decision. Poll the same KV the
        // leader writes to; `observe_barrier` already filters on current
        // leader and returns the freshest announcement.
        let deadline = Instant::now() + decision_timeout;
        loop {
            match cc.observe_barrier().await.ok().flatten() {
                Some(a) if a.epoch == epoch && a.phase == Phase::Commit => {
                    return Ok(self.drive_follower_commit(epoch, checkpoint_id).await);
                }
                Some(a) if a.epoch == epoch && a.phase == Phase::Abort => {
                    self.rollback_sinks(epoch).await.ok();
                    self.checkpoints_failed += 1;
                    return Ok(false);
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                // Announcement never arrived. Before rolling back —
                // which used to be unconditional and could violate the
                // cluster's 2PC if the leader already recorded a
                // `Committed` decision and then crashed — consult the
                // durable decision store. Recorded=Committed means the
                // leader got past the commit point; this follower must
                // drive the commit itself (idempotent). Aborted or
                // absent → safe rollback.
                let verdict = match self.decision_store.as_ref() {
                    Some(ds) => ds.load(epoch).await.unwrap_or_else(|e| {
                        warn!(
                            epoch,
                            checkpoint_id,
                            error = %e,
                            "[LDB-6045] decision store read failed on follower timeout \
                             — defaulting to Abort",
                        );
                        None
                    }),
                    None => None,
                };
                match verdict {
                    Some(laminar_core::cluster::control::Decision::Committed) => {
                        warn!(
                            epoch,
                            checkpoint_id,
                            "[LDB-6046] follower decision timeout but decision=Committed \
                             — driving local commit to match cluster vote",
                        );
                        return Ok(self.drive_follower_commit(epoch, checkpoint_id).await);
                    }
                    Some(laminar_core::cluster::control::Decision::Aborted) | None => {
                        warn!(
                            epoch,
                            checkpoint_id,
                            "[LDB-6034] follower decision timeout; rolling back local prepare",
                        );
                        self.rollback_sinks(epoch).await.ok();
                        self.checkpoints_failed += 1;
                        return Ok(false);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Commit this follower's sinks for `epoch`, update the local
    /// manifest's Pending entries, and return `true` on clean commit.
    /// Extracted so the happy-path (observed `Phase::Commit`) and the
    /// decision-store recovery path (timeout + durable vote says
    /// Committed) share the exact same semantics.
    #[cfg(feature = "cluster-unstable")]
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
            return false;
        }
        // Overwrite the follower's own manifest so the Pending sink
        // entries stamped during prepare get replaced with the
        // Committed statuses we just produced. Recovery inspects
        // these; leaving them Pending makes the follower look as if
        // sinks were still in flight.
        if let Err(e) = self.persist_recovered_statuses(checkpoint_id, statuses).await {
            warn!(
                checkpoint_id,
                epoch,
                error = %e,
                "follower post-commit manifest update failed",
            );
        }
        self.checkpoints_completed += 1;
        true
    }

    /// Pre-commit + save manifest + write vnode markers.
    #[cfg(feature = "cluster-unstable")]
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
        self.pre_commit_sinks(epoch).await?;

        let mut manifest = CheckpointManifest::new(checkpoint_id, epoch);
        manifest.source_offsets = source_offset_overrides;
        manifest.table_offsets = extra_table_offsets;
        manifest.sink_epochs = self.collect_sink_epochs();
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
        self.save_manifest(Arc::new(manifest), state_data).await?;
        self.write_vnode_markers(epoch, checkpoint_id).await?;
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
        let checkpoint_id = self.next_checkpoint_id;
        let epoch = self.epoch;

        info!(checkpoint_id, epoch, "starting checkpoint");

        // Source offsets are provided by the caller (pre-captured at barrier
        // alignment or pre-spawn). Table offsets come from extra_table_offsets.
        self.phase = CheckpointPhase::Snapshotting;
        let source_offsets = source_offset_overrides;
        let table_offsets = extra_table_offsets;

        self.phase = CheckpointPhase::PreCommitting;
        if let Err(e) = self.pre_commit_sinks(epoch).await {
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            // Roll back unconditionally — `rollback_epoch` is idempotent.
            // Without this, a poisoned epoch leaves Kafka transactions
            // open until the broker-side transaction.timeout.ms fires.
            if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                error!(
                    checkpoint_id,
                    epoch,
                    error = %rollback_err,
                    "[LDB-6004] sink rollback failed after pre-commit failure"
                );
            }
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            error!(checkpoint_id, epoch, error = %e, "pre-commit failed");
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some(format!("pre-commit failed: {e}")),
            });
        }

        let mut manifest = CheckpointManifest::new(checkpoint_id, epoch);
        manifest.source_offsets = source_offsets;
        manifest.table_offsets = table_offsets;
        manifest.sink_epochs = self.collect_sink_epochs();
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
                self.phase = CheckpointPhase::Idle;
                self.checkpoints_failed += 1;
                let duration = start.elapsed();
                self.emit_checkpoint_metrics(false, epoch, duration);
                let msg = format!(
                    "[LDB-6014] checkpoint size {sidecar_bytes} bytes exceeds \
                     cap {cap} bytes — checkpoint rejected"
                );
                error!(checkpoint_id, epoch, sidecar_bytes, cap, "{msg}");
                return Ok(CheckpointResult {
                    success: false,
                    checkpoint_id,
                    epoch,
                    duration,
                    error: Some(msg),
                });
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
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                error!(
                    checkpoint_id,
                    epoch,
                    error = %rollback_err,
                    "[LDB-6004] sink rollback failed after manifest persist failure — \
                     sinks may be in an inconsistent state"
                );
            }
            error!(checkpoint_id, epoch, error = %e, "[LDB-6008] manifest persist failed");
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some(format!("manifest persist failed: {e}")),
            });
        }

        // Bridge: publish per-vnode durability markers so the gate below
        // has something to check. Replace with real per-operator partial
        // writes when operators adopt the state backend directly.
        if let Err(e) = self.write_vnode_markers(epoch, checkpoint_id).await {
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                error!(
                    checkpoint_id,
                    epoch,
                    error = %rollback_err,
                    "[LDB-6025] sink rollback failed after marker write failure",
                );
            }
            error!(checkpoint_id, epoch, error = %e, "vnode marker write failed");
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some(format!("vnode marker write failed: {e}")),
            });
        }

        // Cluster 2PC phase 1: announce PREPARE and wait for followers
        // to snapshot + ack.
        #[cfg(feature = "cluster-unstable")]
        {
            if let Some(quorum_failure) = self.await_prepare_quorum(epoch, checkpoint_id).await {
                self.phase = CheckpointPhase::Idle;
                self.checkpoints_failed += 1;
                let duration = start.elapsed();
                self.emit_checkpoint_metrics(false, epoch, duration);
                if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                    error!(
                        checkpoint_id,
                        epoch,
                        error = %rollback_err,
                        "[LDB-6032] sink rollback failed after quorum miss",
                    );
                }
                return Ok(CheckpointResult {
                    success: false,
                    checkpoint_id,
                    epoch,
                    duration,
                    error: Some(quorum_failure),
                });
            }
        }

        // Durability gate: confirm every participating vnode has its
        // partial persisted before sinks commit. `gate_vnode_set` is the
        // FULL registry in cluster mode — the leader verifies markers
        // from every follower's shared-storage writes, not just its own.
        // Single-instance defaults to `vnode_set` (the two match).
        if let Some(ref backend) = self.state_backend {
            if !self.gate_vnode_set.is_empty() {
                match backend.epoch_complete(epoch, &self.gate_vnode_set).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(
                            checkpoint_id,
                            epoch,
                            vnodes = self.gate_vnode_set.len(),
                            "[LDB-6020] state durability gate returned false — \
                             rolling back sinks",
                        );
                        #[cfg(feature = "cluster-unstable")]
                        self.announce_if_leader(
                            epoch,
                            checkpoint_id,
                            laminar_core::cluster::control::Phase::Abort,
                            None,
                        )
                        .await;
                        self.checkpoints_failed += 1;
                        self.phase = CheckpointPhase::Idle;
                        let duration = start.elapsed();
                        self.emit_checkpoint_metrics(false, epoch, duration);
                        if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                            error!(
                                checkpoint_id,
                                epoch,
                                error = %rollback_err,
                                "[LDB-6021] sink rollback failed after durability gate miss",
                            );
                        }
                        return Ok(CheckpointResult {
                            success: false,
                            checkpoint_id,
                            epoch,
                            duration,
                            error: Some("state durability gate: not all vnodes persisted".into()),
                        });
                    }
                    Err(e) => {
                        warn!(
                            checkpoint_id,
                            epoch,
                            error = %e,
                            "[LDB-6022] state backend error during durability gate — \
                             treating as gate miss, rolling back sinks",
                        );
                        #[cfg(feature = "cluster-unstable")]
                        self.announce_if_leader(
                            epoch,
                            checkpoint_id,
                            laminar_core::cluster::control::Phase::Abort,
                            None,
                        )
                        .await;
                        self.checkpoints_failed += 1;
                        self.phase = CheckpointPhase::Idle;
                        let duration = start.elapsed();
                        self.emit_checkpoint_metrics(false, epoch, duration);
                        if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                            error!(
                                checkpoint_id,
                                epoch,
                                error = %rollback_err,
                                "[LDB-6023] sink rollback failed after durability gate error",
                            );
                        }
                        return Ok(CheckpointResult {
                            success: false,
                            checkpoint_id,
                            epoch,
                            duration,
                            error: Some(format!("state durability gate: {e}")),
                        });
                    }
                }
            }
        }

        // Cluster 2PC commit point. Durable `Decision::Committed`
        // must land on shared storage BEFORE the `Commit`
        // announcement goes out, so a new leader elected between
        // here and any follower's local commit can recover the
        // cluster vote from object storage instead of defaulting to
        // Abort. If the CAS reveals a prior `Aborted` verdict (e.g.
        // a racing leader aborted first on its own path), we honor
        // it and fall through to the abort handling below.
        #[cfg(feature = "cluster-unstable")]
        if self.cluster_controller.as_ref().is_some_and(|cc| cc.is_leader()) {
            if let Some(ref ds) = self.decision_store {
                use laminar_core::cluster::control::{Decision, RecordOutcome};
                match ds.record(epoch, Decision::Committed).await {
                    Ok(
                        RecordOutcome::Recorded
                        | RecordOutcome::AlreadyRecorded(Decision::Committed),
                    ) => {}
                    Ok(RecordOutcome::AlreadyRecorded(Decision::Aborted)) => {
                        warn!(
                            checkpoint_id,
                            epoch,
                            "[LDB-6036] decision store already records Aborted for \
                             this epoch — honoring the persisted verdict and \
                             rolling back",
                        );
                        self.announce_if_leader(
                            epoch,
                            checkpoint_id,
                            laminar_core::cluster::control::Phase::Abort,
                            None,
                        )
                        .await;
                        self.checkpoints_failed += 1;
                        self.phase = CheckpointPhase::Idle;
                        let duration = start.elapsed();
                        self.emit_checkpoint_metrics(false, epoch, duration);
                        if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                            error!(
                                checkpoint_id,
                                epoch,
                                error = %rollback_err,
                                "[LDB-6037] sink rollback failed after decision=Aborted",
                            );
                        }
                        return Ok(CheckpointResult {
                            success: false,
                            checkpoint_id,
                            epoch,
                            duration,
                            error: Some(
                                "decision store already records Aborted for this epoch"
                                    .into(),
                            ),
                        });
                    }
                    Err(e) => {
                        // Can't achieve durability on the decision —
                        // safer to abort than to commit and potentially
                        // leave followers in disagreement on recovery.
                        error!(
                            checkpoint_id,
                            epoch,
                            error = %e,
                            "[LDB-6038] failed to record commit decision — aborting epoch",
                        );
                        self.announce_if_leader(
                            epoch,
                            checkpoint_id,
                            laminar_core::cluster::control::Phase::Abort,
                            None,
                        )
                        .await;
                        self.checkpoints_failed += 1;
                        self.phase = CheckpointPhase::Idle;
                        let duration = start.elapsed();
                        self.emit_checkpoint_metrics(false, epoch, duration);
                        if let Err(rollback_err) = self.rollback_sinks(epoch).await {
                            error!(
                                checkpoint_id,
                                epoch,
                                error = %rollback_err,
                                "[LDB-6039] sink rollback failed after decision write error",
                            );
                        }
                        return Ok(CheckpointResult {
                            success: false,
                            checkpoint_id,
                            epoch,
                            duration,
                            error: Some(format!("decision record failed: {e}")),
                        });
                    }
                }
            }
        }

        #[cfg(feature = "cluster-unstable")]
        // Phase 1.3: fan out the cluster-wide min watermark computed
        // during `await_prepare_quorum`. Followers consume this from
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
            self.checkpoints_failed += 1;
            error!(
                checkpoint_id,
                epoch, "sink commit partially failed — epoch NOT advanced, will retry"
            );
            self.phase = CheckpointPhase::Idle;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some("partial sink commit failure".into()),
            });
        }

        self.phase = CheckpointPhase::Idle;
        self.next_checkpoint_id += 1;
        self.epoch += 1;
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
            }
        }

        let next_epoch = self.epoch;
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
            // Advance epoch past the recovered one
            self.epoch = recovered.epoch() + 1;
            self.next_checkpoint_id = recovered.manifest.checkpoint_id + 1;
            info!(
                epoch = self.epoch,
                checkpoint_id = self.next_checkpoint_id,
                "coordinator epoch set after recovery"
            );
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
            .field("epoch", &self.epoch)
            .field("next_checkpoint_id", &self.next_checkpoint_id)
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
    use laminar_storage::checkpoint_store::FileSystemCheckpointStore;

    async fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
        let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
        CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap()
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
                table_store_checkpoint_path: Some("/tmp/rocksdb_cp".into()),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success);

        let loaded = coord.store().load_latest().await.unwrap().unwrap();
        assert_eq!(
            loaded.table_store_checkpoint_path.as_deref(),
            Some("/tmp/rocksdb_cp")
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
        // path behaves as before. Regression guard: Phase B must not change
        // single-instance semantics.
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

    #[cfg(feature = "cluster-unstable")]
    #[tokio::test]
    async fn reconcile_announces_abort_when_no_decision_store() {
        // Fallback path: if no decision store is wired (e.g. legacy
        // deployments), absence of a marker == Abort. This is the
        // pre-decision-store behavior preserved for compatibility.
        use laminar_core::cluster::control::{
            BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_storage::checkpoint_manifest::SinkCommitStatus;
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

    #[cfg(feature = "cluster-unstable")]
    #[tokio::test]
    async fn reconcile_announces_commit_when_decision_committed() {
        // Durable decision=Committed means a prior leader got past the
        // quorum and recorded the cluster vote. Even if the current
        // local manifest still shows Pending (because the old leader
        // crashed before updating), this node must drive local sinks
        // to commit and (if leader) re-announce Commit for stragglers.
        use laminar_core::cluster::control::{
            BarrierAnnouncement, CheckpointDecisionStore, ClusterController, ClusterKv, Decision,
            InMemoryKv, Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_storage::checkpoint_manifest::SinkCommitStatus;
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

        let decision_os: Arc<dyn object_store::ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap(),
        );
        let decision_store = Arc::new(CheckpointDecisionStore::new(decision_os));
        decision_store
            .record(7, Decision::Committed)
            .await
            .unwrap();

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
        assert_eq!(
            ann.phase, Phase::Commit,
            "decision=Committed must lead to Commit re-announcement",
        );
        assert_eq!(ann.epoch, 7);
        assert_eq!(ann.checkpoint_id, 42);
    }

    #[cfg(feature = "cluster-unstable")]
    #[tokio::test]
    async fn reconcile_announces_abort_when_decision_aborted() {
        use laminar_core::cluster::control::{
            BarrierAnnouncement, CheckpointDecisionStore, ClusterController, ClusterKv, Decision,
            InMemoryKv, Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_storage::checkpoint_manifest::SinkCommitStatus;
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

        let decision_os: Arc<dyn object_store::ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap(),
        );
        let decision_store = Arc::new(CheckpointDecisionStore::new(decision_os));
        decision_store.record(3, Decision::Aborted).await.unwrap();

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

    #[cfg(feature = "cluster-unstable")]
    #[tokio::test]
    async fn reconcile_silent_when_manifest_clean() {
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, InMemoryKv, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use laminar_storage::checkpoint_manifest::SinkCommitStatus;
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

    #[cfg(feature = "cluster-unstable")]
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

    #[cfg(feature = "cluster-unstable")]
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

    #[cfg(feature = "cluster-unstable")]
    #[tokio::test]
    async fn leader_publishes_cluster_min_watermark_to_controller() {
        // Phase 1.3b: on a solo cluster, `await_prepare_quorum` computes
        // the cluster-wide min as "leader's local watermark" (no followers
        // to fold). This must be mirrored into the controller atomic so
        // the leader's own operators consume the same value that
        // followers pick up via `observe_barrier(Commit)` — otherwise
        // the leader would drive event-time decisions off a watermark
        // that none of its peers have acked yet.
        use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::NodeId;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

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

    #[cfg(feature = "cluster-unstable")]
    #[tokio::test]
    async fn leader_announces_prepare_and_commit_on_solo_cluster() {
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
        };
        use laminar_core::cluster::discovery::NodeId;
        use tokio::sync::watch;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path()).await;

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
        let mut coord = make_coordinator(dir.path()).await;
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
        assert!(err.contains("vnode marker write failed"), "got: {err}");
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
        }
    }

    #[tokio::test]
    async fn test_pre_commit_failure_triggers_rollback() {
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
            1,
            "rollback_epoch should have been called once"
        );
    }

    /// `pre_commit` fails; `rollback_epoch` hangs forever.
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
            Ok(laminar_connectors::connector::WriteResult::new(0, 0))
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
            laminar_storage::checkpoint_store::FileSystemCheckpointStore::new(dir.path(), 3),
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
        coord.register_sink("stuck-sink", handle, true);
        coord.begin_initial_epoch().await.unwrap();

        // pre_commit fails → rollback_sinks fires → hangs → 100ms
        // rollback_timeout fires → coordinator returns.
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
