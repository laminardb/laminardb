//! Production `PipelineCallback` bridging coordinator to sinks, checkpoints, and watermarks.
#![allow(clippy::disallowed_types)] // cold path

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{filter_late_rows, SourceWatermarkState};
use crate::error::DbError;

/// Resolution state of a sink WHERE filter.
#[derive(Clone)]
pub(crate) enum SinkFilter {
    Pending,
    Compiled(Arc<dyn PhysicalExpr>),
    Rejected,
}

/// Per-cycle dispatch chosen from `SinkFilter`. `Rejected` is fail-closed.
enum SinkFilterDispatch {
    Compiled(Arc<dyn PhysicalExpr>),
    Rejected,
    None,
}

/// Implements [`PipelineCallback`](crate::pipeline::PipelineCallback) to bridge
/// the event-driven pipeline coordinator to the rest of the database (stream
/// executor, sinks, watermarks, checkpoints, table sources).
///
/// `ConnectorPipelineCallback` runs on a dedicated single-threaded tokio runtime
/// (laminar-compute thread). Serialization and checkpoint I/O run inline because:
/// 1. `tokio::spawn` on `current_thread` has no parallelism benefit
/// 2. `spawn_blocking` on `current_thread` has no dedicated blocking pool
/// 3. The compute thread is dedicated — blocking is acceptable
// Throttled (~once/10s) WARN so silent watermark drops are diagnosable.
fn warn_late_drops(source: &str, column: &str, watermark_ms: i64, dropped: usize) {
    use std::sync::atomic::{AtomicI64, Ordering};
    static LAST_WARN_MS: AtomicI64 = AtomicI64::new(0);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    if now_ms - LAST_WARN_MS.load(Ordering::Relaxed) < 10_000 {
        return;
    }
    LAST_WARN_MS.store(now_ms, Ordering::Relaxed);
    tracing::warn!(
        source,
        time_column = column,
        watermark_ms,
        dropped,
        "dropping rows older than the event-time watermark; a future-dated \
         timestamp can advance the watermark and starve the stream"
    );
}

/// Bounded subscription-routing channel depth; drop-on-full caps memory when a
/// subscriber peer stalls.
#[cfg(feature = "cluster")]
const SUB_ROUTE_CAPACITY: usize = 1024;

/// Per-peer send timeout in the routing consumer, so one slow peer can't
/// head-of-line block delivery to the others.
#[cfg(feature = "cluster")]
const SUB_ROUTE_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Throttled (~once/10s) WARN when subscription routing drops a batch, tagged
/// with `cause` so a full queue isn't confused with a slow peer.
#[cfg(feature = "cluster")]
fn warn_subscription_route_drop(cause: &str) {
    use std::sync::atomic::{AtomicI64, Ordering};
    static LAST_WARN_MS: AtomicI64 = AtomicI64::new(0);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    if now_ms - LAST_WARN_MS.load(Ordering::Relaxed) < 10_000 {
        return;
    }
    LAST_WARN_MS.store(now_ms, Ordering::Relaxed);
    tracing::warn!(cause, "dropping remote subscription batch");
}

/// Releases an in-flight epoch's admission slot and staged-bytes
/// budget on drop, so a panicking tail can't leave them leaked and
/// permanently stall checkpoint admission.
struct EpochInFlightGuard {
    in_flight: Arc<std::sync::atomic::AtomicU64>,
    staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
}

impl EpochInFlightGuard {
    /// Claim one admission slot and `bytes` of the staged budget.
    fn claim(
        in_flight: &Arc<std::sync::atomic::AtomicU64>,
        staged_bytes: &Arc<std::sync::atomic::AtomicU64>,
        bytes: u64,
    ) -> Self {
        in_flight.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        staged_bytes.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        Self {
            in_flight: Arc::clone(in_flight),
            staged_bytes: Arc::clone(staged_bytes),
            bytes,
        }
    }
}

impl Drop for EpochInFlightGuard {
    fn drop(&mut self) {
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.staged_bytes
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Captured-state bytes a pending [`CheckpointRequest`]
/// (plus per-vnode slices) holds in memory while its epoch is between
/// capture and upload — the unit the `max_staged_bytes` admission cap
/// counts.
#[allow(clippy::disallowed_types)]
fn staged_request_bytes(
    request: &crate::checkpoint_coordinator::CheckpointRequest,
    vnode_states: &std::collections::HashMap<u32, std::collections::HashMap<String, bytes::Bytes>>,
) -> u64 {
    let ops: usize = request
        .operator_states
        .values()
        .map(bytes::Bytes::len)
        .sum();
    let vnodes: usize = vnode_states
        .values()
        .flat_map(|m| m.values())
        .map(bytes::Bytes::len)
        .sum();
    (ops + vnodes) as u64
}

/// Follower durable-tail bookkeeping shared between the pipeline task
/// and the spawned tail task (ADR-003 two-level completion). Epochs
/// start at 1, so `0` encodes "none".
#[cfg(feature = "cluster")]
#[derive(Debug, Default)]
pub(crate) struct FollowerTailState {
    /// Epoch whose durable tail (prepare + decision wait + 2PC commit)
    /// is currently running in a background task.
    in_flight_epoch: std::sync::atomic::AtomicU64,
    /// Highest epoch this follower has successfully committed; dedups
    /// the leader's idempotent `Prepare` re-announcements. Advanced
    /// only on commit so a failed-then-retried epoch is reprocessed,
    /// not deduped.
    committed_epoch: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "cluster")]
impl FollowerTailState {
    fn in_flight(&self) -> Option<u64> {
        match self
            .in_flight_epoch
            .load(std::sync::atomic::Ordering::Acquire)
        {
            0 => None,
            e => Some(e),
        }
    }

    fn committed(&self) -> Option<u64> {
        match self
            .committed_epoch
            .load(std::sync::atomic::Ordering::Acquire)
        {
            0 => None,
            e => Some(e),
        }
    }

    fn begin(&self, epoch: u64) {
        self.in_flight_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
    }

    /// Record the tail's outcome. The in-flight slot is cleared only if
    /// it still belongs to `epoch` so a late-finishing stale tail can't
    /// clobber a newer epoch's bookkeeping.
    fn finish(&self, epoch: u64, committed: bool) {
        if committed {
            self.committed_epoch
                .fetch_max(epoch, std::sync::atomic::Ordering::AcqRel);
        }
        let _ = self.in_flight_epoch.compare_exchange(
            epoch,
            0,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
    }
}

pub(crate) struct ConnectorPipelineCallback {
    pub(crate) graph: crate::operator_graph::OperatorGraph,
    pub(crate) stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)>,
    #[allow(clippy::type_complexity)]
    pub(crate) sinks: Vec<(
        String,
        crate::sink_task::SinkTaskHandle,
        Option<String>,
        String, // input stream name (FROM clause target)
        bool,   // changelog-capable sink
    )>,
    pub(crate) watermark_states: FxHashMap<String, SourceWatermarkState>,
    pub(crate) source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>>,
    pub(crate) source_ids: FxHashMap<String, usize>,
    /// Pre-interned source names keyed by source id; avoids `Arc::from` per cycle.
    pub(crate) source_name_arcs: FxHashMap<usize, Arc<str>>,
    pub(crate) source_wms_buf: FxHashMap<Arc<str>, i64>,
    pub(crate) tracker: Option<laminar_core::time::WatermarkTracker>,
    pub(crate) prom: Arc<crate::engine_metrics::EngineMetrics>,
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    #[allow(clippy::type_complexity)]
    pub(crate) table_sources: Vec<(
        String,
        Box<dyn laminar_connectors::reference::ReferenceTableSource>,
        laminar_connectors::reference::RefreshMode,
    )>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) mv_store: Arc<parking_lot::RwLock<crate::mv_store::MvStore>>,
    /// Mirrors `MvStore::has_any`; read per cycle to skip the write lock.
    pub(crate) mv_store_has_any: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    pub(crate) filter_ctx: SessionContext,
    pub(crate) compiled_sink_filters: Vec<SinkFilter>,
    pub(crate) pending_sink_filter_compiles: usize,
    pub(crate) last_checkpoint: std::time::Instant,
    /// `None` = no automatic checkpointing (manual only via coordinator).
    pub(crate) checkpoint_interval: Option<std::time::Duration>,
    pub(crate) pipeline_hash: Option<u64>,
    pub(crate) delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee,
    /// Maximum time to wait for operator state serialization.
    pub(crate) serialization_timeout: Duration,
    /// Drained once per cycle by `write_to_sinks` to update sink metrics.
    pub(crate) sink_event_rx: laminar_core::streaming::AsyncConsumer<crate::sink_task::SinkEvent>,
    /// Set when a sink write times out in this cycle. Suppresses the next
    /// checkpoint to preserve the replay window.
    pub(crate) sink_timed_out: bool,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    /// Cluster control facade. Present only when this instance was
    /// built with a controller. Used to gate the periodic checkpoint
    /// trigger on `is_leader()` and to run `follower_checkpoint` on
    /// non-leaders.
    #[cfg(feature = "cluster")]
    pub(crate) cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Shared follower durable-tail bookkeeping: the epoch whose tail is
    /// in flight and the highest committed epoch (dedup, advanced only
    /// on commit — see [`Self::follower_should_skip`]). Epochs awaiting
    /// local source barriers are tracked separately by
    /// [`Self::pending_follower_checkpoint`].
    #[cfg(feature = "cluster")]
    pub(crate) follower_tail: Arc<FollowerTailState>,
    /// Local source barrier injectors to trigger follower-side checkpoints.
    #[cfg(feature = "cluster")]
    pub(crate) barrier_injectors: Vec<(
        Arc<str>,
        laminar_core::checkpoint::CheckpointBarrierInjector,
    )>,
    /// Pending follower checkpoint announcement from gossip, waiting for local sources to align.
    #[cfg(feature = "cluster")]
    pub(crate) pending_follower_checkpoint:
        Option<laminar_core::cluster::control::BarrierAnnouncement>,
    /// Inbound channel for `db.checkpoint()` requests. The db sends a
    /// oneshot sender here; on the next cycle the callback captures
    /// operator state (via `checkpoint_with_barrier`-style path) and
    /// replies on that sender. Without this, `db.checkpoint()` reaches
    /// the coordinator with an empty `CheckpointRequest` and the
    /// leader's manifest is useless for restart recovery.
    pub(crate) force_ckpt_rx: Option<crate::db::ForceCheckpointRx>,
    pub(crate) subscription_registry: Arc<crate::subscription::SubscriptionRegistry>,
    /// Stream/MV name → subscribing node ids, written by the gossip poller and
    /// read each cycle to route output to remote subscribers.
    #[cfg(feature = "cluster")]
    pub(crate) active_subs:
        Arc<parking_lot::RwLock<std::collections::HashMap<String, std::collections::HashSet<u64>>>>,
    /// Ordered single-consumer channel shipping SUBSCRIBE output to remote
    /// subscribers; one consumer preserves order a per-cycle spawn would not.
    #[cfg(feature = "cluster")]
    pub(crate) sub_route:
        std::sync::OnceLock<tokio::sync::mpsc::Sender<(String, RecordBatch, Vec<u64>)>>,
    pub(crate) static_stream_names: rustc_hash::FxHashSet<Arc<str>>,
    pub(crate) checkpoint_complete_tx: crossfire::MAsyncTx<
        crossfire::mpsc::Array<(u64, rustc_hash::FxHashMap<String, SourceCheckpoint>)>,
    >,
    /// Count of epochs between admission and restorable (tails still
    /// running). The streaming coordinator compares it against
    /// `max_in_flight_epochs` to admit the next barrier.
    pub(crate) checkpoint_in_flight: Arc<std::sync::atomic::AtomicU64>,
    /// Captured-state bytes held by in-flight epochs; admission pauses
    /// at `max_staged_bytes`.
    pub(crate) staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Shared id allocator (cloned from the coordinator) so barrier
    /// admission claims ids without the coordinator mutex, which an
    /// earlier epoch's durable tail may hold.
    pub(crate) epoch_allocator: Option<Arc<crate::checkpoint_coordinator::EpochAllocator>>,
    /// Copied from `CheckpointConfig` for the pre-mutex quorum stage.
    #[cfg(feature = "cluster")]
    pub(crate) quorum_timeout: Duration,
}

impl ConnectorPipelineCallback {
    /// `BackpressureFail` raises `shutdown`; coordinator exits via its drain path.
    fn map_graph_error(err: &crate::error::DbError, shutdown: &tokio::sync::Notify) -> String {
        if let crate::error::DbError::BackpressureFail(msg) = err {
            tracing::error!(reason = %msg, "backpressure_policy=Fail tripped; halting pipeline");
            shutdown.notify_one();
        }
        format!("{err}")
    }

    /// Cap each source's tracker-reported watermark by the cluster-wide
    /// minimum, when one has been published. A `None` `cluster_wm`
    /// leaves the map untouched — blindly capping to MIN would freeze
    /// every downstream event-time decision and hang the pipeline.
    #[cfg(feature = "cluster")]
    fn cap_source_watermarks_by_cluster_min(
        source_wms: &mut FxHashMap<Arc<str>, i64>,
        cluster_wm: Option<i64>,
    ) {
        let Some(cluster_wm) = cluster_wm else { return };
        for wm in source_wms.values_mut() {
            if cluster_wm < *wm {
                *wm = cluster_wm;
            }
        }
    }

    /// Ship this cycle's output to remote subscribers under the `__sub::<stream>`
    /// shuffle stage (disjoint from operator stages). No-op without remote interest.
    #[cfg(feature = "cluster")]
    fn route_to_remote_subscribers(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        use laminar_core::shuffle::ShuffleMessage;

        let Some(cfg) = self.graph.cluster_shuffle_config() else {
            return;
        };
        let local_id = cfg.self_id.0;

        let mut to_send: Vec<(String, RecordBatch, Vec<u64>)> = Vec::new();
        {
            let active = self.active_subs.read();
            if active.is_empty() {
                return;
            }
            for (stream_name, batches) in results {
                let Some(nodes) = active.get(stream_name.as_ref()) else {
                    continue;
                };
                let remote: Vec<u64> = nodes.iter().copied().filter(|&id| id != local_id).collect();
                if remote.is_empty() {
                    continue;
                }
                let stage = crate::subscription::remote_stage(stream_name);
                for batch in batches {
                    if batch.num_rows() > 0 {
                        to_send.push((stage.clone(), batch.clone(), remote.clone()));
                    }
                }
            }
        }
        if to_send.is_empty() {
            return;
        }

        // One long-lived consumer drains in order so emission order survives to
        // the peer; a per-cycle spawn would let later batches overtake earlier.
        let tx = self.sub_route.get_or_init(|| {
            let (tx, mut rx) =
                tokio::sync::mpsc::channel::<(String, RecordBatch, Vec<u64>)>(SUB_ROUTE_CAPACITY);
            let sender = Arc::clone(&cfg.sender);
            let prom = Arc::clone(&self.prom);
            tokio::spawn(async move {
                while let Some((stage, batch, targets)) = rx.recv().await {
                    let msg = ShuffleMessage::VnodeData(stage, 0, batch);
                    for node in targets {
                        // Bound each send so one slow peer can't head-of-line block
                        // the others; a failed/timed-out send drops that batch.
                        match tokio::time::timeout(
                            SUB_ROUTE_SEND_TIMEOUT,
                            sender.send_to(node, &msg),
                        )
                        .await
                        {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                prom.remote_subscription_batches_dropped.inc();
                                tracing::warn!(node, error = %e, "subscription batch send failed");
                            }
                            Err(_) => {
                                prom.remote_subscription_batches_dropped.inc();
                                warn_subscription_route_drop("peer send timed out");
                            }
                        }
                    }
                }
            });
            tx
        });
        // Bounded, best-effort: if a stalled peer backs the queue up, drop the
        // batch (subscriptions are at-most-once under backpressure) and warn.
        for item in to_send {
            if tx.try_send(item).is_err() {
                self.prom.remote_subscription_batches_dropped.inc();
                warn_subscription_route_drop("routing queue full");
            }
        }
    }

    /// Follower-side dispatch: poll the leader's announcement, and
    /// if a fresh PREPARE is visible, capture local state and run
    /// `CheckpointCoordinator::follower_checkpoint`. Returns `true`
    /// when a checkpoint was driven this tick.
    /// Driven by `db.checkpoint()` via the `force_ckpt_rx` channel.
    /// Captures operator state (plus table/watermark metadata) and
    /// commits the manifest through the coordinator, returning the
    /// full `CheckpointResult` for the caller. Mirrors
    /// `checkpoint_with_barrier` but without the barrier-alignment
    /// gate — the caller has chosen to take a consistent snapshot
    /// of whatever state is currently in the accumulators.
    async fn force_capture_and_checkpoint(
        &mut self,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, DbError> {
        self.sync_sinks_and_drain_events().await;

        // Ids allocated at alignment must be threaded through to the
        // checkpoint, otherwise the inner allocation would burn a second
        // pair and checkpoint a different epoch than was announced.
        #[cfg(feature = "cluster")]
        let leader_ids = self.align_shuffle_for_leader().await?;
        #[cfg(not(feature = "cluster"))]
        let leader_ids: Option<(u64, u64)> = None;

        let operator_states = self
            .capture_and_serialize_operator_state()
            .await
            .map_err(DbError::Checkpoint)?;
        let request = self.build_checkpoint_request(operator_states, &FxHashMap::default());

        let vnode_states = self.capture_vnode_states();
        let mut guard = self.coordinator.lock().await;
        let coord = guard.as_mut().ok_or_else(|| {
            DbError::Checkpoint(
                "coordinator not initialized when force_capture_and_checkpoint ran".into(),
            )
        })?;
        coord.set_pending_vnode_states(vnode_states);
        // Seed leader-side local watermark so `await_prepare_quorum`
        // can fold it into the cluster-wide min.
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        coord.set_local_watermark_ms(if wm == i64::MIN { None } else { Some(wm) });
        let result = match leader_ids {
            Some((epoch, checkpoint_id)) => {
                coord
                    .checkpoint_preallocated(
                        request,
                        epoch,
                        checkpoint_id,
                        crate::checkpoint_coordinator::QuorumStage::RunInline,
                    )
                    .await?
            }
            None => coord.checkpoint_with_offsets(request).await?,
        };
        if result.success {
            self.last_checkpoint = std::time::Instant::now();
        }
        Ok(result)
    }

    /// Skip a follower `Prepare` for `ann_epoch` only if it is already
    /// committed (`last_committed`), already awaiting local source
    /// barriers (`pending`), or its durable tail is already running
    /// (`tail_in_flight`). Comparisons are monotonic — leaders abandon
    /// failed epochs, so gaps in the announced sequence are normal.
    #[cfg(feature = "cluster")]
    fn follower_should_skip(
        last_committed: Option<u64>,
        pending: Option<u64>,
        tail_in_flight: Option<u64>,
        ann_epoch: u64,
    ) -> bool {
        last_committed.is_some_and(|e| e >= ann_epoch)
            || pending.is_some_and(|e| e >= ann_epoch)
            || tail_in_flight.is_some_and(|e| e >= ann_epoch)
    }

    /// Hand the follower's durable tail — sink pre-commit, manifest
    /// save, vnode-partial uploads, the leader-decision wait, and the
    /// 2PC commit/rollback — to a background task so the pipeline can
    /// resume on `Aligned` instead of stalling until `Commit`
    /// (ADR-003 two-level completion). The capture ack is sent before
    /// queuing on the coordinator mutex — an earlier epoch's tail may
    /// hold it for the duration of its uploads, and the leader's quorum
    /// must not wait on those. On commit, completion flows through
    /// `checkpoint_complete_tx` so the streaming coordinator fans
    /// `EpochCommitted` out to sources and publishes the wire barrier —
    /// the same path the leader's background persist uses.
    #[cfg(feature = "cluster")]
    fn spawn_follower_tail(
        &mut self,
        request: crate::checkpoint_coordinator::CheckpointRequest,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        fan_out: FxHashMap<String, SourceCheckpoint>,
    ) {
        let epoch = ann.epoch;
        let vnode_states = self.capture_vnode_states();
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        self.follower_tail.begin(epoch);

        let coordinator = Arc::clone(&self.coordinator);
        let tail = Arc::clone(&self.follower_tail);
        let complete_tx = self.checkpoint_complete_tx.clone();
        let cc = self.cluster_controller.clone();
        tokio::spawn(async move {
            let wm_opt = if wm == i64::MIN { None } else { Some(wm) };
            if let Some(ref cc) = cc {
                cc.ack_barrier(&laminar_core::cluster::control::BarrierAck {
                    epoch,
                    ok: true,
                    error: None,
                    local_watermark_ms: wm_opt,
                })
                .await
                .ok();
            }
            let committed = {
                let mut guard = coordinator.lock().await;
                match guard.as_mut() {
                    Some(coord) => {
                        coord.set_pending_vnode_states(vnode_states);
                        coord.set_local_watermark_ms(wm_opt);
                        match coord
                            .follower_checkpoint_acked(
                                request,
                                ann,
                                std::time::Duration::from_secs(30),
                            )
                            .await
                        {
                            Ok(true) => {
                                tracing::info!(epoch, "follower checkpoint committed");
                                true
                            }
                            Ok(false) => {
                                tracing::warn!(
                                    epoch,
                                    "follower checkpoint aborted (leader signalled Abort)"
                                );
                                false
                            }
                            Err(e) => {
                                tracing::warn!(epoch, error = %e, "follower checkpoint errored");
                                false
                            }
                        }
                    }
                    None => false,
                }
            };
            // Record only on commit so a failed-then-retried epoch is
            // reprocessed, not deduped.
            tail.finish(epoch, committed);
            if committed {
                let _ = complete_tx.send((epoch, fan_out)).await;
            }
        });
    }

    /// Block the pipeline until the leader announces `Aligned` for
    /// `epoch` — the re-keyed shuffle-alignment resume gate (ADR-003).
    /// `Aligned` is announced only after the *full-membership* capture
    /// quorum, preserving the invariant the no-post-barrier-buffering
    /// shuffle drain relies on: no peer ships epoch-N+1 rows while
    /// another node is still folding pre-barrier rows into its epoch-N
    /// snapshot. `Commit`, `Abort`, or any newer epoch's announcement
    /// also releases the gate (observation is latest-wins, so a quick
    /// successor can overwrite `Aligned`).
    ///
    /// No-op without a cross-node shuffle — there are no in-flight
    /// peer rows to protect. Bounded: on timeout the pipeline resumes
    /// anyway (the epoch aborts independently via the leader's gate).
    ///
    /// Associated fn (no `&self`) so callers' futures stay `Send`
    /// without requiring `ConnectorPipelineCallback: Sync`.
    #[cfg(feature = "cluster")]
    async fn wait_for_aligned_resume(
        has_cluster_shuffle: bool,
        controller: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
    ) {
        use laminar_core::cluster::control::Phase;

        const RESUME_GATE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
        const POLL: std::time::Duration = std::time::Duration::from_millis(10);

        if !has_cluster_shuffle {
            return;
        }
        let start = std::time::Instant::now();
        loop {
            match controller.observe_barrier().await.ok().flatten() {
                Some(a) if a.epoch > epoch => return,
                Some(a)
                    if a.epoch == epoch
                        && matches!(a.phase, Phase::Aligned | Phase::Commit | Phase::Abort) =>
                {
                    return;
                }
                _ => {}
            }
            if start.elapsed() >= RESUME_GATE_TIMEOUT {
                tracing::warn!(
                    epoch,
                    "aligned resume gate timed out — resuming pipeline \
                     (epoch will abort via the leader's restorable gate)"
                );
                return;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    #[cfg(feature = "cluster")]
    async fn maybe_follower_checkpoint(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64> {
        use laminar_core::cluster::control::Phase;

        let ann = match controller.observe_barrier().await {
            Ok(Some(a)) if a.phase == Phase::Prepare => a,
            _ => return None,
        };
        let pending = self.pending_follower_checkpoint.as_ref().map(|p| p.epoch);
        if Self::follower_should_skip(
            self.follower_tail.committed(),
            pending,
            self.follower_tail.in_flight(),
            ann.epoch,
        ) {
            return None;
        }

        // Inject barriers into local sources so they flow through this node's
        // streaming tasks and emit the shuffle barriers required for alignment.
        for (name, injector) in &self.barrier_injectors {
            tracing::debug!(
                source = %name,
                checkpoint_id = ann.checkpoint_id,
                "follower injecting checkpoint barrier"
            );
            injector.trigger(ann.checkpoint_id, ann.flags);
        }

        if !self.barrier_injectors.is_empty() {
            tracing::info!(
                checkpoint_id = ann.checkpoint_id,
                epoch = ann.epoch,
                "follower deferring checkpoint alignment until source barriers flow through"
            );
            self.pending_follower_checkpoint = Some(ann);
            return None;
        }

        // Drain + align the cross-node shuffle on the leader's checkpoint id so
        // the snapshot includes peers' pre-checkpoint rows.
        {
            let live: Vec<u64> = controller.live_instances().iter().map(|n| n.0).collect();
            let wm = self
                .pipeline_watermark
                .load(std::sync::atomic::Ordering::Acquire);
            if let Err(e) = self
                .graph
                .align_shuffle_barriers(ann.checkpoint_id, wm, &live, Some(&*controller))
                .await
            {
                tracing::warn!(error = %e, "follower shuffle alignment failed — skipping");
                return None;
            }
        }

        let request =
            self.build_checkpoint_request(std::collections::HashMap::new(), &source_offsets);

        // Durable tail off the pipeline task; resume once every node has
        // captured (Aligned). Completion is reported asynchronously via
        // `checkpoint_complete_tx`, so there is no epoch to return here.
        let stall_start = std::time::Instant::now();
        let epoch = ann.epoch;
        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        self.spawn_follower_tail(request, ann, source_offsets);
        Self::wait_for_aligned_resume(has_shuffle, &controller, epoch).await;
        self.prom
            .checkpoint_pipeline_stall_duration
            .observe(stall_start.elapsed().as_secs_f64());
        self.last_checkpoint = std::time::Instant::now();
        None
    }

    #[cfg(feature = "cluster")]
    async fn run_follower_checkpoint_deferred(
        &mut self,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    ) -> crate::pipeline::BarrierOutcome {
        use crate::pipeline::BarrierOutcome;

        // Owned clone: `controller` outlives the `&mut self` calls below
        // (state capture, tail spawn) without borrowing `self`.
        let Some(controller) = self.cluster_controller.clone() else {
            return BarrierOutcome::Failed;
        };

        // Drain + align the cross-node shuffle on the leader's checkpoint id so
        // the snapshot includes peers' pre-checkpoint rows.
        {
            let live: Vec<u64> = controller.live_instances().iter().map(|n| n.0).collect();
            let wm = self
                .pipeline_watermark
                .load(std::sync::atomic::Ordering::Acquire);
            if let Err(e) = self
                .graph
                .align_shuffle_barriers(ann.checkpoint_id, wm, &live, Some(&*controller))
                .await
            {
                tracing::warn!(error = %e, "follower shuffle alignment failed — skipping");
                return BarrierOutcome::Failed;
            }
        }

        let operator_states = match self.capture_and_serialize_operator_state().await {
            Ok(states) => states,
            Err(e) => {
                tracing::warn!(error = %e, "follower deferred checkpoint: operator state capture failed");
                return BarrierOutcome::Failed;
            }
        };
        let request = self.build_checkpoint_request(operator_states, &source_checkpoints);

        // Durable tail (pre-commit + manifest + uploads + decision wait
        // + 2PC) off the pipeline task; resume processing once every
        // node has captured (Aligned) instead of stalling until Commit.
        // Commit completion flows through `checkpoint_complete_tx`
        // (EpochCommitted fan-out + wire barrier), so the outcome here
        // is Async, not Committed.
        let stall_start = std::time::Instant::now();
        let epoch = ann.epoch;
        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        self.spawn_follower_tail(request, ann, source_checkpoints);
        Self::wait_for_aligned_resume(has_shuffle, &controller, epoch).await;
        self.prom
            .checkpoint_pipeline_stall_duration
            .observe(stall_start.elapsed().as_secs_f64());
        self.last_checkpoint = std::time::Instant::now();
        BarrierOutcome::Async
    }

    /// Leader-side barrier admission: allocate this barrier's ids
    /// (lock-free — an earlier epoch's durable tail may hold the
    /// coordinator mutex), announce `Prepare` so followers begin
    /// aligning, then drain + align the cross-node shuffle so the
    /// snapshot includes peers' pre-barrier rows. Returns the allocated
    /// `(epoch, checkpoint_id)`; `None` when this node isn't the
    /// cluster leader or no coordinator is wired. On alignment failure
    /// the allocated epoch is abandoned (Abort + rollback + next epoch
    /// begun).
    #[cfg(feature = "cluster")]
    async fn align_shuffle_for_leader(&mut self) -> Result<Option<(u64, u64)>, DbError> {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

        let (Some(cc), Some(allocator)) = (
            self.cluster_controller.clone(),
            self.epoch_allocator.clone(),
        ) else {
            return Ok(None);
        };
        if !cc.is_leader() {
            return Ok(None);
        }
        let (epoch, checkpoint_id) = allocator.allocate();
        if let Err(e) = cc
            .announce_barrier(&BarrierAnnouncement {
                epoch,
                checkpoint_id,
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
                seq: 0,
            })
            .await
        {
            tracing::warn!(epoch, checkpoint_id, error = %e, "prepare announcement failed");
        }

        let live: Vec<u64> = cc.live_instances().iter().map(|n| n.0).collect();
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        if let Err(e) = self
            .graph
            .align_shuffle_barriers(checkpoint_id, wm, &live, Some(&*cc))
            .await
        {
            if let Some(coord) = self.coordinator.lock().await.as_mut() {
                coord
                    .abandon_epoch(checkpoint_id, epoch, format!("shuffle alignment: {e}"))
                    .await;
            }
            return Err(e);
        }
        Ok(Some((epoch, checkpoint_id)))
    }

    /// Assemble a [`CheckpointRequest`](crate::checkpoint_coordinator::CheckpointRequest)
    /// from the given operator states and barrier-captured source
    /// offsets, folding in table-source offsets and per-source
    /// watermarks. Shared by every checkpoint entry point (leader
    /// barrier, follower, timer, forced).
    fn build_checkpoint_request(
        &self,
        operator_states: std::collections::HashMap<String, bytes::Bytes>,
        source_checkpoints: &FxHashMap<String, SourceCheckpoint>,
    ) -> crate::checkpoint_coordinator::CheckpointRequest {
        use crate::checkpoint_coordinator::source_to_connector_checkpoint;

        let mut extra_tables = HashMap::with_capacity(self.table_sources.len());
        for (name, source, _) in &self.table_sources {
            extra_tables.insert(
                name.clone(),
                source_to_connector_checkpoint(&source.checkpoint()),
            );
        }
        let mut per_source_watermarks = HashMap::with_capacity(self.watermark_states.len());
        for (name, wm_state) in &self.watermark_states {
            let wm = wm_state.generator.current_watermark();
            if wm > i64::MIN {
                per_source_watermarks.insert(name.clone(), wm);
            }
        }
        let source_offset_overrides = source_checkpoints
            .iter()
            .map(|(name, cp)| (name.clone(), source_to_connector_checkpoint(cp)))
            .collect();

        crate::checkpoint_coordinator::CheckpointRequest {
            operator_states,
            watermark: None,
            table_store_checkpoint_path: None,
            extra_table_offsets: extra_tables,
            source_watermarks: per_source_watermarks,
            pipeline_hash: self.pipeline_hash,
            source_offset_overrides,
        }
    }

    async fn capture_and_serialize_operator_state(
        &mut self,
    ) -> Result<std::collections::HashMap<String, bytes::Bytes>, String> {
        let mut operator_states = HashMap::with_capacity(2);
        let cp = match self.graph.snapshot_state() {
            Ok(Some(cp)) => cp,
            Ok(None) => return Ok(operator_states),
            Err(e) => return Err(format!("snapshot failed: {e}")),
        };
        // Offload CPU-bound serialization to blocking thread pool.
        let timeout = self.serialization_timeout;
        let bytes = tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || {
                crate::operator_graph::OperatorGraph::serialize_checkpoint(&cp)
            }),
        )
        .await
        .map_err(|_| format!("[LDB-6017] operator state serialization timed out ({timeout:?})"))?
        .map_err(|e| format!("serialize join error: {e}"))?
        .map_err(|e| format!("serialize error: {e}"))?;
        operator_states.insert("operator_graph".to_string(), bytes::Bytes::from(bytes));

        // Serialize MV result store — each MV gets its own entry keyed "mv:{name}".
        let mv_states = self
            .mv_store
            .read()
            .checkpoint_states()
            .map_err(|e| format!("MV checkpoint failed: {e}"))?;
        operator_states.extend(mv_states);

        Ok(operator_states)
    }

    /// Per-vnode operator-state slices for the in-flight checkpoint
    /// (`vnode → operator_name → bytes`), staged on the coordinator so each
    /// owned vnode's `partial.bin` carries real, rehydratable state. Empty
    /// outside cluster mode. Best-effort: a snapshot error logs and yields an
    /// empty map — the partial then carries no operator state for that epoch,
    /// exactly as the legacy marker did, so the checkpoint still succeeds.
    #[allow(clippy::unused_self, clippy::disallowed_types)] // matches the coordinator/graph map shape
    fn capture_vnode_states(
        &mut self,
    ) -> std::collections::HashMap<u32, std::collections::HashMap<String, bytes::Bytes>> {
        #[cfg(feature = "cluster")]
        {
            match self.graph.snapshot_state_by_vnode() {
                Ok(map) => map,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "per-vnode state snapshot failed — partials carry no state this epoch"
                    );
                    std::collections::HashMap::new()
                }
            }
        }
        #[cfg(not(feature = "cluster"))]
        {
            std::collections::HashMap::new()
        }
    }

    /// Wait for every sink to finish processing previously-enqueued
    /// `WriteBatch` commands, then drain any `SinkEvent`s they produced.
    /// Callers rely on `sink_timed_out` being current after this returns.
    async fn sync_sinks_and_drain_events(&mut self) {
        let sync_futures = self.sinks.iter().map(|(name, handle, _, _, _)| {
            let name = name.clone();
            let handle = handle.clone();
            async move {
                if let Err(e) = handle.sync().await {
                    tracing::warn!(sink = %name, error = %e, "sink sync barrier failed");
                }
            }
        });
        futures::future::join_all(sync_futures).await;
        self.drain_sink_events();
    }

    /// Non-blocking drain of `sink_event_rx` into Prometheus counters and
    /// `sink_timed_out`.
    fn drain_sink_events(&mut self) {
        while let Ok(event) = self.sink_event_rx.try_recv() {
            tracing::debug!(?event, "sink event");
            match &event {
                crate::sink_task::SinkEvent::WriteError { .. } => {
                    self.prom.sink_write_failures.inc();
                }
                crate::sink_task::SinkEvent::WriteTimeout { .. } => {
                    self.sink_timed_out = true;
                    self.prom.sink_write_timeouts.inc();
                }
                crate::sink_task::SinkEvent::ChannelClosed { .. } => {
                    self.prom.sink_task_channel_closed.inc();
                }
            }
        }
    }

    async fn compile_pending_sink_filters(
        &mut self,
        results: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
    ) {
        if self.pending_sink_filter_compiles == 0 {
            return;
        }

        while self.compiled_sink_filters.len() < self.sinks.len() {
            self.compiled_sink_filters.push(SinkFilter::Pending);
        }

        for (i, (sink_name, _, filter_sql, sink_input, _)) in self.sinks.iter().enumerate() {
            if filter_sql.is_none() || !matches!(self.compiled_sink_filters[i], SinkFilter::Pending)
            {
                continue;
            }
            let Some(batches) = results.get(sink_input.as_str()) else {
                continue;
            };
            let Some(batch) = batches.first() else {
                continue;
            };
            let schema = batch.schema();
            let sql = filter_sql.as_deref().unwrap();
            match crate::filter_compile::compile(&self.filter_ctx, sql, &schema).await {
                Ok(compiled) => {
                    self.compiled_sink_filters[i] = SinkFilter::Compiled(compiled);
                }
                Err(e) => {
                    tracing::error!(
                        sink = %sink_name,
                        filter = %sql,
                        error = %e,
                        "[LDB-1100] sink filter did not compile; fail-closed: \
                         ALL rows from this stream will be dropped for this sink. \
                         Track via sink_filter_rejected_rows_total."
                    );
                    self.compiled_sink_filters[i] = SinkFilter::Rejected;
                }
            }
            self.pending_sink_filter_compiles = self.pending_sink_filter_compiles.saturating_sub(1);
        }
    }
}

#[allow(clippy::too_many_lines)]
impl crate::pipeline::PipelineCallback for ConnectorPipelineCallback {
    async fn execute_cycle(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, String> {
        self.source_wms_buf.clear();
        if let Some(ref tracker) = self.tracker {
            for (&sid, name_arc) in &self.source_name_arcs {
                if let Some(wm) = tracker.source_watermark(sid) {
                    self.source_wms_buf.insert(Arc::clone(name_arc), wm);
                }
            }
        }

        #[cfg(feature = "cluster")]
        {
            let cluster_wm = self
                .cluster_controller
                .as_ref()
                .and_then(|cc| cc.cluster_min_watermark());
            Self::cap_source_watermarks_by_cluster_min(&mut self.source_wms_buf, cluster_wm);
        }

        let swm_ref = if self.source_wms_buf.is_empty() {
            None
        } else {
            Some(&self.source_wms_buf)
        };
        self.graph
            .execute_cycle(source_batches, watermark, swm_ref)
            .await
            .map_err(|e| Self::map_graph_error(&e, &self.shutdown_signal))
    }

    fn push_to_streams(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        // Cluster mode: ship this cycle's output to any remote nodes holding a
        // SUBSCRIBE interest before publishing to local subscribers below.
        #[cfg(feature = "cluster")]
        self.route_to_remote_subscribers(results);

        for (stream_name, src) in &self.stream_sources {
            if let Some(batches) = results.get(stream_name.as_str()) {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        #[allow(clippy::cast_possible_truncation)]
                        let row_count = batch.num_rows() as u64;
                        self.prom.events_emitted.inc_by(row_count);
                        if src.push_arrow(batch.clone()).is_err() {
                            #[allow(clippy::cast_possible_truncation)]
                            let dropped = batch.num_rows() as u64;
                            self.prom.events_dropped.inc_by(dropped);
                        }
                        self.subscription_registry
                            .send_batch(stream_name, batch.clone());
                    }
                }
            }
        }

        // Ephemeral/dynamic streams (e.g. console live queries) have no
        // downstream sink, so they aren't in `stream_sources` and the loop
        // above never forwards them — push their batches to any subscribers.
        // MVs are skipped here; they reach subscribers via their own path.
        let mv_has_any = self
            .mv_store_has_any
            .load(std::sync::atomic::Ordering::Acquire);
        let mv_read = if mv_has_any {
            Some(self.mv_store.read())
        } else {
            None
        };
        for (stream_name, batches) in results {
            let is_static = self.static_stream_names.contains(stream_name);
            let has_mv = mv_read
                .as_ref()
                .is_some_and(|r| r.has_mv(stream_name.as_ref()));
            if is_static || has_mv {
                continue;
            }
            for batch in batches {
                if batch.num_rows() > 0 {
                    self.subscription_registry
                        .send_batch(stream_name, batch.clone());
                }
            }
        }
    }

    fn update_mv_stores(&self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        if results.is_empty() {
            return;
        }
        if !self
            .mv_store_has_any
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        let mut store = self.mv_store.write();
        let mut updates = 0u64;
        for (stream_name, batches) in results {
            if store.has_mv(stream_name) {
                for batch in batches {
                    if batch.num_rows() > 0 {
                        store.update(stream_name, batch);
                        updates += 1;
                        self.subscription_registry
                            .send_batch(stream_name, batch.clone());
                    }
                }
            }
        }
        if updates > 0 {
            self.prom.mv_updates.inc_by(updates);
            #[allow(clippy::cast_possible_truncation)]
            let bytes = store.total_bytes() as u64;
            #[allow(clippy::cast_possible_wrap)]
            self.prom.mv_bytes_stored.set(bytes as i64);
        }
    }

    async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        self.compile_pending_sink_filters(results).await;

        // Share per-stream batches across sinks so fan-out doesn't reclone the Vec.
        let mut shared_inputs: FxHashMap<&str, Arc<[RecordBatch]>> = FxHashMap::default();

        let sink_futures: Vec<_> = self
            .sinks
            .iter()
            .enumerate()
            .filter_map(
                |(sink_idx, (sink_name, handle, _filter_sql, sink_input, changelog_capable))| {
                    // Route by FROM clause: only send matching results.
                    let batches = results.get(sink_input.as_str())?;
                    if batches.is_empty() {
                        return None;
                    }
                    let shared = shared_inputs
                        .entry(sink_input.as_str())
                        .or_insert_with(|| Arc::<[RecordBatch]>::from(batches.as_slice()))
                        .clone();
                    let sink_name = sink_name.clone();
                    let handle = handle.clone();
                    // Rejected → drop (fail-closed). Pending → None (no filter SQL set).
                    let filter_state = match self.compiled_sink_filters.get(sink_idx).cloned() {
                        Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
                        Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
                        Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
                    };
                    let changelog_capable = *changelog_capable;
                    let prom = Arc::clone(&self.prom);
                    Some(async move {
                        for batch in shared.iter() {
                            let filtered: Cow<RecordBatch> = match &filter_state {
                                SinkFilterDispatch::Compiled(phys) => {
                                    match crate::filter_compile::apply(batch, phys.as_ref()) {
                                        Ok(Some(fb)) => Cow::Owned(fb),
                                        Ok(None) => continue,
                                        Err(e) => {
                                            tracing::warn!(
                                                sink = %sink_name,
                                                error = %e,
                                                "Compiled sink filter error"
                                            );
                                            continue;
                                        }
                                    }
                                }
                                SinkFilterDispatch::Rejected => {
                                    #[allow(clippy::cast_possible_truncation)]
                                    let dropped = batch.num_rows() as u64;
                                    prom.sink_filter_rejected_rows
                                        .with_label_values(&[sink_name.as_str()])
                                        .inc_by(dropped);
                                    continue;
                                }
                                SinkFilterDispatch::None => Cow::Borrowed(batch),
                            };

                            let prepared = crate::changelog_filter::prepare_for_sink(
                                &filtered,
                                changelog_capable,
                            );
                            if prepared.num_rows() == 0 {
                                continue;
                            }
                            if let Err(e) = handle.write_batch(prepared.into_owned()).await {
                                tracing::warn!(
                                    sink = %sink_name,
                                    error = %e,
                                    "Sink command channel closed"
                                );
                                break;
                            }
                        }
                    })
                },
            )
            .collect();
        futures::future::join_all(sink_futures).await;

        // Opportunistic; the strict "all prior writes settled" barrier
        // runs in the checkpoint paths, not on every cycle.
        self.drain_sink_events();
    }

    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch) {
        if let Some(wm_state) = self.watermark_states.get_mut(source_name) {
            // Check external watermarks from Source::watermark() calls.
            if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                let external_wm = entry.source.current_watermark();
                if let Some(wm) = wm_state.generator.advance_watermark(external_wm) {
                    self.prom
                        .source_watermark_ms
                        .with_label_values(&[source_name])
                        .set(wm.timestamp());
                    if let Some(ref mut trk) = self.tracker {
                        if let Some(sid) = self.source_ids.get(source_name) {
                            if let Some(global_wm) = trk.update_source(*sid, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global_wm.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }

            // Extract watermark from batch data.
            if let Ok(max_ts) = wm_state.extractor.extract(batch) {
                if let Some(wm) = wm_state.generator.on_event(max_ts) {
                    if let Some(entry) = self.source_entries_for_wm.get(source_name) {
                        entry.source.watermark(wm.timestamp());
                    }
                    self.prom
                        .source_watermark_ms
                        .with_label_values(&[source_name])
                        .set(wm.timestamp());
                    if let Some(ref mut trk) = self.tracker {
                        if let Some(sid) = self.source_ids.get(source_name) {
                            if let Some(global_wm) = trk.update_source(*sid, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global_wm.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        }

        // Update ingestion counters.
        #[allow(clippy::cast_possible_truncation)]
        let row_count = batch.num_rows() as u64;
        self.prom.events_ingested.inc_by(row_count);
        self.prom.batches.inc();
    }

    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
        if let Some(wm_state) = self.watermark_states.get(source_name) {
            // A processing-time watermark is wall-clock, not derived from the event
            // column — every real (past) timestamp is "older than" it, so filtering
            // would drop all rows. Windows still close on the wall-clock watermark;
            // window-level late handling applies downstream.
            if wm_state.generator.is_processing_time() {
                return Some(batch.clone());
            }
            let current_wm = wm_state.generator.current_watermark();
            if current_wm > i64::MIN {
                let before = batch.num_rows();
                // Null timestamps are data-quality, not lateness — count separately.
                let null_ts = batch
                    .column_by_name(&wm_state.column)
                    .map_or(0, |c| c.null_count());
                match filter_late_rows(batch, &wm_state.column, current_wm) {
                    Ok(out) => {
                        let after = out.as_ref().map_or(0, arrow_array::RecordBatch::num_rows);
                        let dropped = before.saturating_sub(after);
                        let late = dropped.saturating_sub(null_ts);
                        if null_ts > 0 {
                            self.prom
                                .events_null_timestamp
                                .inc_by(u64::try_from(null_ts).unwrap_or(u64::MAX));
                        }
                        if late > 0 {
                            self.prom
                                .events_dropped
                                .inc_by(u64::try_from(late).unwrap_or(u64::MAX));
                            warn_late_drops(source_name, &wm_state.column, current_wm, late);
                        }
                        return out;
                    }
                    Err(e) => {
                        // Schema drift, not lateness: fail-safe drop + error.
                        tracing::error!(
                            source = source_name,
                            column = %wm_state.column,
                            error = %e,
                            "filter_late_rows: dropping batch (schema drift)"
                        );
                        return None;
                    }
                }
            }
        }
        // No watermark configured → pass through all rows.
        Some(batch.clone())
    }

    fn current_watermark(&self) -> i64 {
        self.pipeline_watermark
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn is_leader(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            if let Some(ref cc) = self.cluster_controller {
                return cc.is_leader();
            }
        }
        true
    }

    fn tick_idle_watermark(&mut self) {
        let Some(ref mut trk) = self.tracker else {
            return;
        };
        // Drive periodic generators (ProcessingTime) and pull external
        // `Source::watermark()` so a quiet source still advances the frontier.
        for (name, state) in &mut self.watermark_states {
            if let Some(wm) = state.generator.on_periodic() {
                if let Some(&id) = self.source_ids.get(name) {
                    if let Some(global) = trk.update_source(id, wm.timestamp()) {
                        self.pipeline_watermark
                            .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            if let Some(entry) = self.source_entries_for_wm.get(name) {
                let external = entry.source.current_watermark();
                if external > i64::MIN {
                    if let Some(wm) = state.generator.advance_watermark(external) {
                        if let Some(&id) = self.source_ids.get(name) {
                            if let Some(global) = trk.update_source(id, wm.timestamp()) {
                                self.pipeline_watermark.store(
                                    global.timestamp(),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        }
        // `update_combined` is monotone, so a re-activating source with an
        // older watermark can't regress the combined value.
        if let Some(global) = trk.check_idle_sources() {
            self.pipeline_watermark
                .store(global.timestamp(), std::sync::atomic::Ordering::Relaxed);
            tracing::info!(
                watermark_ms = global.timestamp(),
                "pipeline watermark advanced via idle-source detection"
            );
        }
        for (name, &id) in &self.source_ids {
            self.prom
                .source_idle
                .with_label_values(&[name.as_str()])
                .set(i64::from(trk.is_idle(id)));
        }
    }

    /// Timer-based checkpoint (at-least-once semantics).
    ///
    /// Source offsets are captured BEFORE operator state. On recovery:
    /// consumer replays from offset → operator may re-process some
    /// records. This is correct for at-least-once but NOT exactly-once.
    /// For exactly-once, use barrier-aligned checkpoints instead.
    async fn maybe_checkpoint(
        &mut self,
        force: bool,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64> {
        // Drain any pending `db.checkpoint()` requests first. Each
        // request gets a capture of operator state (the same path the
        // periodic barrier-aligned checkpoint uses), committed through
        // the coordinator, and the full `CheckpointResult` returned on
        // the oneshot. Without this, `db.checkpoint()` reaches the
        // coordinator with an empty `CheckpointRequest` and the
        // manifest has no operator state — restart loses every
        // `IncrementalAggState` accumulator on the invoking node.
        let mut force_reqs: Vec<
            crossfire::oneshot::TxOneshot<
                Result<crate::checkpoint_coordinator::CheckpointResult, DbError>,
            >,
        > = Vec::new();
        if let Some(rx) = self.force_ckpt_rx.as_ref() {
            while let Ok(reply) = rx.try_recv() {
                force_reqs.push(reply);
            }
        }
        for reply_tx in force_reqs {
            let result = self.force_capture_and_checkpoint().await;
            // Ignore receiver-dropped: caller gave up on `db.checkpoint()`.
            reply_tx.send(result);
        }

        // Cluster mode: only the leader fires periodic checkpoints.
        // Followers mirror the leader's epoch via `follower_checkpoint`
        // on observed PREPARE announcements. Forced checkpoints
        // (shutdown) run on whatever instance is shutting down.
        #[cfg(feature = "cluster")]
        if !force {
            if let Some(cc) = self.cluster_controller.clone() {
                if cc.is_leader() {
                    // Leader in cluster mode: do not trigger timer-based checkpoints here.
                    // Periodic checkpoints are driven by barrier injection from the streaming coordinator.
                    return None;
                }
                return self.maybe_follower_checkpoint(cc, source_offsets).await;
            }
        }

        // Under exactly-once, only barrier-aligned checkpoints are consistent.
        // Timer-based checkpoints are skipped (barrier path handles exactly-once).
        if !force
            && self.delivery_guarantee
                == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            tracing::debug!("skipping timer checkpoint under exactly-once (use barriers)");
            return None;
        }

        if self.prom.cycles.get() == 0 {
            return None;
        }

        self.sync_sinks_and_drain_events().await;

        // After a sink timeout, skip one checkpoint cycle so that source
        // offsets don't advance past the dropped batch. The NEXT cycle will
        // clear this flag and checkpoint normally. This preserves at-least-once:
        // on recovery, the source replays from the pre-drop checkpoint.
        if !force && self.sink_timed_out {
            self.sink_timed_out = false;
            tracing::info!("skipping checkpoint after sink timeout to preserve replay window");
            return None;
        }

        if !force {
            let Some(interval) = self.checkpoint_interval else {
                return None; // no auto-checkpointing configured
            };
            if self.last_checkpoint.elapsed() < interval {
                return None;
            }
        }

        // Capture stream executor aggregate state and serialize on blocking thread.
        // Abort the checkpoint if serialization fails — persisting source
        // offsets without matching operator state would cause data loss on
        // recovery (offsets advance past unreplayable state).
        let operator_states = match self.capture_and_serialize_operator_state().await {
            Ok(states) => states,
            Err(e) => {
                tracing::warn!(error = %e, "Stream executor checkpoint failed — skipping checkpoint");
                return None;
            }
        };
        let request = self.build_checkpoint_request(operator_states, &source_offsets);

        let vnode_states = self.capture_vnode_states();
        let mut committed = None;
        if force {
            // Blocking checkpoint at shutdown.
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                coord.set_pending_vnode_states(vnode_states);
                match coord.checkpoint_with_offsets(request).await {
                    Ok(result) if result.success => {
                        tracing::info!(epoch = result.epoch, "Final pipeline checkpoint saved");
                        committed = Some(result.epoch);
                    }
                    Ok(result) => {
                        tracing::warn!(
                            epoch = result.epoch,
                            error = ?result.error,
                            "Final checkpoint failed"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Final checkpoint error");
                    }
                }
            }
        } else {
            // Persist in the background; the in-flight slot + staged
            // bytes are released by the guard when the tail finishes.
            let in_flight = EpochInFlightGuard::claim(
                &self.checkpoint_in_flight,
                &self.staged_bytes,
                staged_request_bytes(&request, &vnode_states),
            );
            let coordinator_clone = Arc::clone(&self.coordinator);
            let tx = self.checkpoint_complete_tx.clone();
            tokio::spawn(async move {
                let _in_flight = in_flight; // released on drop, even on panic
                let mut guard = coordinator_clone.lock().await;
                if let Some(ref mut coord) = *guard {
                    coord.set_pending_vnode_states(vnode_states);
                    match coord.checkpoint_with_offsets(request).await {
                        Ok(result) if result.success => {
                            let _ = tx
                                .send((result.epoch, rustc_hash::FxHashMap::default()))
                                .await;
                        }
                        Ok(result) => tracing::warn!(
                            epoch = result.epoch,
                            error = ?result.error,
                            "Pipeline checkpoint failed"
                        ),
                        Err(e) => tracing::warn!(error = %e, "Checkpoint error"),
                    }
                }
            });
            committed = None;
        }

        self.last_checkpoint = std::time::Instant::now();
        committed
    }

    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        checkpoint_id: u64,
    ) -> crate::pipeline::BarrierOutcome {
        use crate::pipeline::{BarrierOutcome, SkipReason};

        #[cfg(not(feature = "cluster"))]
        let _ = checkpoint_id;
        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            if !cc.is_leader() {
                if let Some(ann) = self.pending_follower_checkpoint.take() {
                    // Leaders abandon failed epochs, so a slow barrier
                    // round can complete after its announcement was
                    // superseded — the captured offsets must never be
                    // attributed to the newer epoch's announcement.
                    if ann.checkpoint_id != checkpoint_id {
                        tracing::warn!(
                            round_checkpoint_id = checkpoint_id,
                            pending_checkpoint_id = ann.checkpoint_id,
                            "stale follower barrier round — its epoch was abandoned; \
                             re-queueing the newer announcement"
                        );
                        self.pending_follower_checkpoint = Some(ann);
                        return BarrierOutcome::Failed;
                    }
                    return self
                        .run_follower_checkpoint_deferred(ann, source_checkpoints)
                        .await;
                }
                tracing::warn!("follower received checkpoint_with_barrier but pending_follower_checkpoint is None");
                return BarrierOutcome::Failed;
            }
        }

        if self.prom.cycles.get() == 0 {
            return BarrierOutcome::Skipped(SkipReason::NoCyclesSinceLastCheckpoint);
        }

        self.sync_sinks_and_drain_events().await;

        // Clear after one suppression — the timer-based path that also
        // clears this flag is unreachable when barrier checkpointing is active.
        if self.sink_timed_out {
            self.sink_timed_out = false;
            return BarrierOutcome::Skipped(SkipReason::PreservingReplayWindowAfterSinkTimeout);
        }

        // Everything from here until the Aligned resume gate releases is
        // pipeline stall — the metric Phase 1 of ADR-003 targets.
        let stall_start = std::time::Instant::now();

        // Drain + align the cross-node shuffle before capture so peers'
        // pre-barrier rows enter the snapshot (announces Prepare early).
        // The returned ids key this barrier's tail and resume gate.
        #[cfg(feature = "cluster")]
        let leader_ids = match self.align_shuffle_for_leader().await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, "shuffle barrier alignment failed");
                return BarrierOutcome::Failed;
            }
        };
        #[cfg(not(feature = "cluster"))]
        let leader_ids: Option<(u64, u64)> = None;

        // Capture stream executor aggregate state — now consistent because
        // all pre-barrier data has been executed. Serialize on blocking thread.
        // Abort if serialization fails — persisting source offsets without
        // matching operator state would cause data loss on recovery.
        let operator_states = match self.capture_and_serialize_operator_state().await {
            Ok(states) => states,
            Err(e) => {
                tracing::warn!(error = %e, "Stream executor barrier checkpoint failed");
                return BarrierOutcome::Failed;
            }
        };

        // The barrier-captured source offsets are consistent with the
        // operator state at the barrier point and must not be re-queried
        // from the live connectors.
        let vnode_states = self.capture_vnode_states();
        let request = self.build_checkpoint_request(operator_states, &source_checkpoints);

        // Durable tail in the background, two stages: the capture
        // quorum + `Aligned` run *before* the coordinator mutex (an
        // earlier epoch's tail may hold it for its uploads — Aligned
        // must never queue behind those), then the durable remainder
        // under the FIFO mutex, which keeps epochs restorable in order.
        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.staged_bytes,
            staged_request_bytes(&request, &vnode_states),
        );
        let coordinator_clone = Arc::clone(&self.coordinator);
        let tx = self.checkpoint_complete_tx.clone();
        let fan_out = source_checkpoints.clone();
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        #[cfg(feature = "cluster")]
        let cc_for_tail = self.cluster_controller.clone();
        #[cfg(feature = "cluster")]
        let quorum_timeout = self.quorum_timeout;
        tokio::spawn(async move {
            use crate::checkpoint_coordinator::{CheckpointCoordinator, QuorumStage};

            let _in_flight = in_flight; // released on drop, even on panic
            let wm_opt = if wm == i64::MIN { None } else { Some(wm) };

            #[allow(unused_mut)]
            let mut quorum = QuorumStage::RunInline;
            #[cfg(feature = "cluster")]
            if let (Some(cc), Some((epoch, checkpoint_id))) = (cc_for_tail.as_ref(), leader_ids) {
                use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
                match CheckpointCoordinator::run_prepare_quorum(
                    cc,
                    quorum_timeout,
                    epoch,
                    checkpoint_id,
                    wm_opt,
                )
                .await
                {
                    Ok(min_watermark_ms) => {
                        if let Err(e) = cc
                            .announce_barrier(&BarrierAnnouncement {
                                epoch,
                                checkpoint_id,
                                phase: Phase::Aligned,
                                flags: 0,
                                min_watermark_ms,
                                seq: 0,
                            })
                            .await
                        {
                            tracing::warn!(
                                epoch, error = %e,
                                "[LDB-6031] aligned announcement failed; peers resume on Commit"
                            );
                        }
                        quorum = QuorumStage::Done(min_watermark_ms);
                    }
                    Err(msg) => {
                        tracing::error!(checkpoint_id, epoch, error = %msg, "[LDB-6032] quorum miss");
                        let mut guard = coordinator_clone.lock().await;
                        if let Some(ref mut coord) = *guard {
                            coord.abandon_epoch(checkpoint_id, epoch, msg).await;
                        }
                        return;
                    }
                }
            }

            let mut guard = coordinator_clone.lock().await;
            if let Some(ref mut coord) = *guard {
                coord.set_pending_vnode_states(vnode_states);
                coord.set_local_watermark_ms(wm_opt);
                let result = match leader_ids {
                    Some((epoch, checkpoint_id)) => {
                        coord
                            .checkpoint_preallocated(request, epoch, checkpoint_id, quorum)
                            .await
                    }
                    None => coord.checkpoint_with_offsets(request).await,
                };
                match result {
                    Ok(result) if result.success => {
                        let _ = tx.send((result.epoch, fan_out)).await;
                    }
                    Ok(result) => tracing::warn!(
                        epoch = result.epoch,
                        error = ?result.error,
                        "Barrier-aligned checkpoint failed"
                    ),
                    Err(e) => tracing::warn!(error = %e, "Barrier-aligned checkpoint error"),
                }
            }
        });

        // Leader resume gate (ADR-003): hold the pipeline until the tail
        // above announces `Aligned` (full-membership capture quorum) —
        // resuming earlier could ship epoch-N+1 shuffle rows to a peer
        // still folding pre-barrier rows into its epoch-N snapshot.
        // Abort (quorum miss) also releases; the gate is bounded.
        #[cfg(feature = "cluster")]
        if let (Some((epoch, _)), Some(cc)) = (leader_ids, self.cluster_controller.clone()) {
            let has_shuffle = self.graph.cluster_shuffle_config().is_some();
            Self::wait_for_aligned_resume(has_shuffle, &cc, epoch).await;
        }
        self.prom
            .checkpoint_pipeline_stall_duration
            .observe(stall_start.elapsed().as_secs_f64());

        self.last_checkpoint = std::time::Instant::now();
        BarrierOutcome::Async
    }

    fn record_cycle(&self, events_ingested: u64, _batches: u64, elapsed_ns: u64) {
        let _ = events_ingested; // already recorded in extract_watermark
        self.prom.cycles.inc();
        #[allow(clippy::cast_precision_loss)]
        self.prom
            .cycle_duration
            .observe(elapsed_ns as f64 / 1_000_000_000.0);
    }

    fn apply_control(&mut self, msg: crate::pipeline::ControlMsg) {
        match msg {
            crate::pipeline::ControlMsg::AddStream {
                name,
                sql,
                emit_clause,
                window_config,
                order_config,
                join_config,
            } => {
                self.graph.add_query(
                    name.clone(),
                    sql,
                    emit_clause,
                    window_config,
                    order_config,
                    None,
                    join_config,
                );
                tracing::info!(stream = %name, "Stream added via control channel");
            }
            crate::pipeline::ControlMsg::DropStream { name } => {
                self.graph.remove_query(&name);
                tracing::info!(stream = %name, "Stream removed via control channel");
            }
            crate::pipeline::ControlMsg::AddSourceSchema { name, schema } => {
                self.graph.register_source_schema(name, schema);
            }
        }
    }

    fn is_backpressured(&self) -> bool {
        let bp = self.graph.input_buf_pressure() > 0.8;
        if bp {
            self.prom.cycles_backpressured.inc();
        }
        bp
    }

    fn publish_barrier(&self, epoch: u64, checkpoint_id: u64) {
        self.subscription_registry
            .broadcast_barrier(epoch, checkpoint_id);
    }

    fn has_deferred_input(&self) -> bool {
        // In cluster mode, always claim pending input so the coordinator
        // runs `execute_cycle` each idle tick — otherwise a follower with
        // no local source events never drains the shuffle receiver and
        // remote rows sit stranded. Non-cluster path unchanged.
        #[cfg(feature = "cluster")]
        {
            if self.cluster_controller.is_some() {
                return true;
            }
        }
        self.graph.has_pending_input()
    }

    async fn poll_tables(&mut self) {
        use laminar_connectors::reference::RefreshMode;

        for (name, source, mode) in &mut self.table_sources {
            if matches!(mode, RefreshMode::SnapshotOnly | RefreshMode::Manual) {
                continue;
            }
            match source.poll_changes().await {
                Ok(Some(batch)) => {
                    // Single registry lookup — dispatch by variant.
                    let entry = self.lookup_registry.get_entry(name);
                    if let Some(
                        laminar_sql::datafusion::lookup_join_exec::RegisteredLookup::Partial(
                            partial,
                        ),
                    ) = &entry
                    {
                        update_partial_cache_from_batch(partial, &batch);
                        let mut ts = self.table_store.write();
                        if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                            #[allow(clippy::cast_possible_truncation)]
                            let dropped = batch.num_rows() as u64;
                            self.prom.events_dropped.inc_by(dropped);
                            tracing::error!(
                                table=%name, error=%e, rows_dropped=dropped,
                                "[LDB-5030] Table upsert failed (partial); \
                                 {dropped} rows dropped"
                            );
                        }
                    } else if let Some(
                        laminar_sql::datafusion::lookup_join_exec::RegisteredLookup::Versioned(
                            versioned,
                        ),
                    ) = &entry
                    {
                        // Versioned path: append new CDC rows, preserving
                        // all versions for temporal point-in-time lookups.
                        let combined = if versioned.batch.num_rows() == 0
                            || versioned.batch.schema().fields().is_empty()
                        {
                            batch.clone()
                        } else {
                            match arrow::compute::concat_batches(
                                &versioned.batch.schema(),
                                [&versioned.batch, &batch],
                            ) {
                                Ok(b) => b,
                                Err(e) => {
                                    tracing::warn!(
                                        table=%name, error=%e,
                                        "Versioned table concat error (schema mismatch?); \
                                         keeping existing state"
                                    );
                                    // Preserve existing versioned history rather than
                                    // discarding it. The CDC batch is silently dropped.
                                    continue;
                                }
                            }
                        };
                        // Build versioned index from the combined batch.
                        let key_indices: Vec<usize> = versioned
                            .key_columns
                            .iter()
                            .filter_map(|k| combined.schema().index_of(k).ok())
                            .collect();
                        let Ok(version_col_idx) =
                            combined.schema().index_of(&versioned.version_column)
                        else {
                            tracing::warn!(
                                table=%name,
                                version_col=%versioned.version_column,
                                "Version column not found; skipping index rebuild"
                            );
                            continue;
                        };
                        let index =
                            match laminar_sql::datafusion::lookup_join_exec::VersionedIndex::build(
                                &combined,
                                &key_indices,
                                version_col_idx,
                                versioned.max_versions_per_key,
                            ) {
                                Ok(idx) => Arc::new(idx),
                                Err(e) => {
                                    tracing::warn!(
                                        table=%name, error=%e,
                                        "Versioned index build error"
                                    );
                                    continue;
                                }
                            };
                        self.lookup_registry.register_versioned(
                            name,
                            laminar_sql::datafusion::VersionedLookupState {
                                batch: combined,
                                index,
                                key_columns: versioned.key_columns.clone(),
                                version_column: versioned.version_column.clone(),
                                stream_time_column: versioned.stream_time_column.clone(),
                                max_versions_per_key: versioned.max_versions_per_key,
                            },
                        );
                        let mut ts = self.table_store.write();
                        if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                            #[allow(clippy::cast_possible_truncation)]
                            let dropped = batch.num_rows() as u64;
                            self.prom.events_dropped.inc_by(dropped);
                            tracing::error!(
                                table=%name, error=%e, rows_dropped=dropped,
                                "[LDB-5030] Table upsert failed (versioned); \
                                 {dropped} rows dropped"
                            );
                        }
                    } else {
                        let maybe_batch = {
                            let mut ts = self.table_store.write();
                            if let Err(e) = ts.upsert_and_rebuild(name, &batch) {
                                #[allow(clippy::cast_possible_truncation)]
                                let dropped = batch.num_rows() as u64;
                                self.prom.events_dropped.inc_by(dropped);
                                tracing::error!(
                                    table=%name, error=%e, rows_dropped=dropped,
                                    "[LDB-5030] Table upsert failed; \
                                     {dropped} rows dropped"
                                );
                                None
                            } else if ts.is_persistent(name) {
                                None
                            } else {
                                ts.to_record_batch(name)
                            }
                        };
                        // Update lookup registry for join operators.
                        // DataFusion table registration is NOT needed here — all
                        // tables use ReferenceTableProvider which reads live data.
                        if let Some(rb) = maybe_batch {
                            self.lookup_registry.register(
                                name,
                                laminar_sql::datafusion::LookupSnapshot { batch: rb },
                            );
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(table=%name, error=%e, "Table poll error");
                }
            }
        }
    }

    fn next_checkpoint_id(&self) -> Option<u64> {
        // Lock-free: an in-flight epoch's tail may hold the coordinator
        // mutex for the duration of its uploads.
        self.epoch_allocator.as_ref().map(|a| a.peek().1)
    }

    fn set_barrier_injectors(
        &mut self,
        injectors: Vec<(
            Arc<str>,
            laminar_core::checkpoint::CheckpointBarrierInjector,
        )>,
    ) {
        #[cfg(feature = "cluster")]
        {
            self.barrier_injectors = injectors;
        }
        #[cfg(not(feature = "cluster"))]
        {
            let _ = injectors;
        }
    }
}

/// Update a partial lookup cache from a CDC batch by inserting or deleting
/// each row keyed by the primary key column(s).
///
/// CDC delete detection: if the batch has a column named `__op`, `__operation`,
/// or `op` with values `"d"`, `"D"`, `"delete"`, or `"DELETE"`, the row is
/// removed from the cache instead of upserted.
fn update_partial_cache_from_batch(
    partial: &laminar_sql::datafusion::PartialLookupState,
    batch: &RecordBatch,
) {
    use arrow_array::{Array, StringArray};

    if partial.key_columns.is_empty() {
        return;
    }

    let key_cols: Vec<_> = partial
        .key_columns
        .iter()
        .filter_map(|name| {
            batch
                .schema()
                .index_of(name)
                .ok()
                .map(|idx| batch.column(idx).clone())
        })
        .collect();
    if key_cols.len() != partial.key_columns.len() {
        return;
    }

    let Ok(converter) = arrow::row::RowConverter::new(partial.key_sort_fields.clone()) else {
        return;
    };
    let Ok(rows) = converter.convert_columns(&key_cols) else {
        return;
    };

    // Detect CDC operation column for delete handling.
    let op_col_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| matches!(f.name().as_str(), "__op" | "__operation" | "op"));
    let op_array = op_col_idx.and_then(|idx| {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| (idx, a))
    });

    let num_rows = batch.num_rows();
    for row in 0..num_rows {
        let key = rows.row(row);

        let is_delete = op_array.is_some_and(|(_, arr)| {
            !arr.is_null(row) && matches!(arr.value(row), "d" | "D" | "delete" | "DELETE")
        });

        if is_delete {
            partial.lookup_cache.invalidate(key.as_ref());
        } else {
            let row_batch = batch.slice(row, 1);
            partial.lookup_cache.insert(key.as_ref(), row_batch);
        }
    }
}

/// Encode an Arrow schema as a hex-encoded IPC flatbuffer for `ConnectorConfig`.
pub(crate) fn encode_arrow_schema(schema: &arrow_schema::Schema) -> String {
    laminar_connectors::config::encode_arrow_schema_ipc(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DbError;

    #[tokio::test]
    async fn test_backpressure_fail_notifies_shutdown() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::BackpressureFail("downstream of 'q'".into());
        let msg = ConnectorPipelineCallback::map_graph_error(&err, &notify);
        assert!(msg.contains("Backpressure fail"), "unexpected: {msg}");

        tokio::time::timeout(Duration::from_millis(50), notify.notified())
            .await
            .expect("shutdown should have been notified");
    }

    #[tokio::test]
    async fn test_non_backpressure_error_does_not_notify() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::Pipeline("unrelated".into());
        let _ = ConnectorPipelineCallback::map_graph_error(&err, &notify);

        let got = tokio::time::timeout(Duration::from_millis(50), notify.notified()).await;
        assert!(got.is_err(), "non-Fail errors must not trigger shutdown");
    }

    /// Rejected must drop, not passthrough.
    #[test]
    fn rejected_filter_dispatches_to_drop_not_passthrough() {
        let filters = [SinkFilter::Rejected];
        let dispatch = match filters.first().cloned() {
            Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
            Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
            Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
        };
        assert!(
            matches!(dispatch, SinkFilterDispatch::Rejected),
            "Rejected filter must map to Rejected dispatch (drop), not None (passthrough)"
        );
    }

    /// Pending / absent → no filter (compilation runs before the dispatch loop).
    #[test]
    fn pending_and_absent_filters_dispatch_to_passthrough() {
        for filter in [Some(SinkFilter::Pending), None] {
            let dispatch = match filter.clone() {
                Some(SinkFilter::Compiled(phys)) => SinkFilterDispatch::Compiled(phys),
                Some(SinkFilter::Rejected) => SinkFilterDispatch::Rejected,
                Some(SinkFilter::Pending) | None => SinkFilterDispatch::None,
            };
            assert!(matches!(dispatch, SinkFilterDispatch::None));
        }
    }

    /// Consumer-side cap is a no-op when the cluster has not yet
    /// published a minimum watermark — otherwise every event-time
    /// decision would freeze behind the `i64::MIN` sentinel.
    #[cfg(feature = "cluster")]
    #[test]
    fn cap_source_watermarks_none_cluster_wm_leaves_map_untouched() {
        let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
        wms.insert(Arc::from("a"), 1_000);
        wms.insert(Arc::from("b"), 500);

        ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, None);

        assert_eq!(wms.get(&Arc::<str>::from("a")).copied(), Some(1_000));
        assert_eq!(wms.get(&Arc::<str>::from("b")).copied(), Some(500));
    }

    /// When a cluster-wide minimum is published, sources that have
    /// advanced past it get pulled back to it; sources at or below
    /// the cap are left alone (cap must not push watermarks forward).
    #[cfg(feature = "cluster")]
    #[test]
    fn cap_source_watermarks_lowers_only_sources_above_cluster_min() {
        let mut wms: FxHashMap<Arc<str>, i64> = FxHashMap::default();
        wms.insert(Arc::from("ahead"), 2_000);
        wms.insert(Arc::from("at"), 1_500);
        wms.insert(Arc::from("behind"), 800);

        ConnectorPipelineCallback::cap_source_watermarks_by_cluster_min(&mut wms, Some(1_500));

        assert_eq!(
            wms.get(&Arc::<str>::from("ahead")).copied(),
            Some(1_500),
            "source above cluster min must be capped down",
        );
        assert_eq!(
            wms.get(&Arc::<str>::from("at")).copied(),
            Some(1_500),
            "source at cluster min unchanged",
        );
        assert_eq!(
            wms.get(&Arc::<str>::from("behind")).copied(),
            Some(800),
            "source below cluster min must NOT be advanced by the cap",
        );
    }

    /// A committed (or older) epoch re-announced by the leader is deduped.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_skips_already_committed_or_older_epoch() {
        assert!(ConnectorPipelineCallback::follower_should_skip(
            Some(5),
            None,
            None,
            5
        ));
        assert!(ConnectorPipelineCallback::follower_should_skip(
            Some(5),
            None,
            None,
            3
        ));
    }

    /// A deferred epoch awaiting local barriers (tracked via the pending
    /// arg) is deduped so its injectors aren't re-triggered each cycle.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_skips_in_flight_deferred_epoch() {
        assert!(ConnectorPipelineCallback::follower_should_skip(
            Some(4),
            Some(5),
            None,
            5
        ));
    }

    /// An epoch whose durable tail (prepare + decision wait) is running
    /// in the background is deduped — the pipeline has already resumed,
    /// so the same Prepare announcement stays visible (latest-wins
    /// observation) and must not re-trigger capture.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_skips_epoch_with_tail_in_flight() {
        assert!(ConnectorPipelineCallback::follower_should_skip(
            Some(4),
            None,
            Some(5),
            5
        ));
    }

    /// Regression: a failed checkpoint doesn't advance the committed epoch, and
    /// the leader retries the same epoch, so the retry must be reprocessed —
    /// not deduped. The old code recorded the epoch before commit and wedged.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_reprocesses_retry_of_failed_epoch() {
        // Epoch 5 failed: committed is still 4, nothing in flight, leader retries 5.
        assert!(!ConnectorPipelineCallback::follower_should_skip(
            Some(4),
            None,
            None,
            5
        ));
        // Same on a fresh follower whose first epoch failed.
        assert!(!ConnectorPipelineCallback::follower_should_skip(
            None, None, None, 5
        ));
    }

    /// A higher epoch is always processed.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_processes_newer_epoch() {
        assert!(!ConnectorPipelineCallback::follower_should_skip(
            Some(5),
            None,
            None,
            6
        ));
        assert!(!ConnectorPipelineCallback::follower_should_skip(
            Some(5),
            Some(5),
            Some(5),
            6
        ));
    }

    /// Build a follower-side controller whose `current_leader()` is a
    /// seeded peer, for resume-gate tests. The caller holds the
    /// returned membership sender alive for the test's duration.
    #[cfg(feature = "cluster")]
    fn gate_controller() -> (
        Arc<laminar_core::cluster::control::InMemoryKv>,
        laminar_core::cluster::control::ClusterController,
        laminar_core::cluster::discovery::NodeId,
        tokio::sync::watch::Sender<Vec<laminar_core::cluster::discovery::NodeInfo>>,
    ) {
        use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};

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
        let (tx, rx) = tokio::sync::watch::channel(vec![leader_info]);
        (
            kv,
            ClusterController::new(follower_id, kv_trait, None, rx),
            leader_id,
            tx,
        )
    }

    /// The resume gate releases on the leader's `Aligned` announcement.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_releases_on_aligned() {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

        let (kv, controller, leader_id, _members_tx) = gate_controller();
        let aligned = serde_json::to_string(&BarrierAnnouncement {
            epoch: 3,
            checkpoint_id: 3,
            phase: Phase::Aligned,
            flags: 0,
            min_watermark_ms: Some(42),
            seq: 0,
        })
        .unwrap();
        kv.seed(leader_id, ANNOUNCEMENT_KEY, aligned);

        tokio::time::timeout(
            Duration::from_secs(2),
            ConnectorPipelineCallback::wait_for_aligned_resume(true, &controller, 3),
        )
        .await
        .expect("gate must release on Aligned");
        // The Aligned announcement also publishes the cluster-min
        // watermark, so a resuming pipeline sees fresh event-time
        // progress before the upload-gated Commit.
        assert_eq!(controller.cluster_min_watermark(), Some(42));
    }

    /// A newer epoch's announcement supersedes the awaited one
    /// (latest-wins observation can overwrite Aligned/Commit).
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_releases_on_newer_epoch() {
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

        let (kv, controller, leader_id, _members_tx) = gate_controller();
        let newer = serde_json::to_string(&BarrierAnnouncement {
            epoch: 4,
            checkpoint_id: 4,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
            seq: 0,
        })
        .unwrap();
        kv.seed(leader_id, ANNOUNCEMENT_KEY, newer);

        tokio::time::timeout(
            Duration::from_secs(2),
            ConnectorPipelineCallback::wait_for_aligned_resume(true, &controller, 3),
        )
        .await
        .expect("gate must release when a newer epoch is announced");
    }

    /// Without a cross-node shuffle there is no in-flight-row invariant
    /// to protect — the gate is a no-op even with no announcement.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_resume_gate_skips_without_shuffle() {
        let (_kv, controller, _leader_id, _members_tx) = gate_controller();
        tokio::time::timeout(
            Duration::from_millis(100),
            ConnectorPipelineCallback::wait_for_aligned_resume(false, &controller, 3),
        )
        .await
        .expect("gate must be a no-op without a cluster shuffle");
    }

    /// Tail bookkeeping: `finish` clears only its own epoch's in-flight
    /// slot and advances `committed` only on commit.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_tail_state_lifecycle() {
        let tail = FollowerTailState::default();
        assert_eq!(tail.in_flight(), None);
        assert_eq!(tail.committed(), None);

        tail.begin(5);
        assert_eq!(tail.in_flight(), Some(5));

        // Aborted tail: in-flight cleared, committed not advanced.
        tail.finish(5, false);
        assert_eq!(tail.in_flight(), None);
        assert_eq!(tail.committed(), None);

        // Committed tail.
        tail.begin(5);
        tail.finish(5, true);
        assert_eq!(tail.in_flight(), None);
        assert_eq!(tail.committed(), Some(5));

        // A stale tail finishing late must not clobber a newer epoch's
        // in-flight slot, and committed stays monotonic.
        tail.begin(7);
        tail.finish(5, true);
        assert_eq!(tail.in_flight(), Some(7), "stale finish must not clear");
        assert_eq!(tail.committed(), Some(5));
        tail.finish(7, true);
        assert_eq!(tail.committed(), Some(7));
    }
}
