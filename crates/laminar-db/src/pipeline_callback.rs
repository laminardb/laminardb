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

/// Bridges the coordinator to the rest of the database (sinks, watermarks, checkpoints).
// Throttled WARN so silent watermark drops are diagnosable.
fn warn_late_drops(source: &str, column: &str, watermark_ms: i64, dropped: usize) {
    static THROTTLE: crate::log_throttle::LogThrottle =
        crate::log_throttle::LogThrottle::every(Duration::from_secs(10));
    if !THROTTLE.allow() {
        return;
    }
    tracing::warn!(
        source,
        time_column = column,
        watermark_ms,
        dropped,
        "dropping rows older than the event-time watermark; a future-dated \
         timestamp can advance the watermark and starve the stream"
    );
}

/// Subscription routing channel depth; drop-on-full caps memory when a subscriber stalls.
#[cfg(feature = "cluster")]
const SUB_ROUTE_CAPACITY: usize = 1024;

/// Per-peer send timeout so one slow peer can't head-of-line block others.
#[cfg(feature = "cluster")]
const SUB_ROUTE_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

#[cfg(feature = "cluster")]
fn warn_subscription_route_drop(cause: &str) {
    static THROTTLE: crate::log_throttle::LogThrottle =
        crate::log_throttle::LogThrottle::every(Duration::from_secs(10));
    if THROTTLE.allow() {
        tracing::warn!(cause, "dropping remote subscription batch");
    }
}

/// RAII guard that releases an epoch's admission slot and staged-byte budget on drop.
struct EpochInFlightGuard {
    in_flight: Arc<std::sync::atomic::AtomicU64>,
    staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
}

impl EpochInFlightGuard {
    /// Claim one admission slot and `bytes` of staged budget.
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

/// State for the leader's spawned durable tail.
struct LeaderTail {
    in_flight: EpochInFlightGuard,
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    complete_tx: crossfire::MAsyncTx<
        crossfire::mpsc::Array<(u64, rustc_hash::FxHashMap<String, SourceCheckpoint>)>,
    >,
    request: crate::checkpoint_coordinator::CheckpointRequest,
    #[allow(clippy::disallowed_types)]
    vnode_states: crate::checkpoint_coordinator::StagedVnodeStates,
    fan_out: FxHashMap<String, SourceCheckpoint>,
    local_watermark_ms: Option<i64>,
    leader_ids: Option<(u64, u64)>,
    #[cfg(feature = "cluster")]
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    #[cfg(feature = "cluster")]
    quorum_timeout: Duration,
}

/// Bytes held in memory by a pending checkpoint (operator states + per-vnode slices).
#[allow(clippy::disallowed_types)]
fn staged_request_bytes(
    request: &crate::checkpoint_coordinator::CheckpointRequest,
    vnode_states: &crate::checkpoint_coordinator::StagedVnodeStates,
) -> u64 {
    let ops: usize = request
        .operator_states
        .values()
        .map(bytes::Bytes::len)
        .sum();
    let vnodes: usize = vnode_states
        .values()
        .flat_map(|m| m.values())
        .map(|s| match s {
            crate::checkpoint_coordinator::StagedSlice::Bytes(b) => b.len(),
            // Cold slices are on disk; they hold no RAM.
            crate::checkpoint_coordinator::StagedSlice::Cold => 0,
            crate::checkpoint_coordinator::StagedSlice::Delta {
                changed,
                tombstones,
            } => changed.len() + tombstones.len(),
        })
        .sum();
    (ops + vnodes) as u64
}

/// Follower durable-tail bookkeeping. Epoch `0` encodes "none" (epochs start at 1).
#[cfg(feature = "cluster")]
#[derive(Debug, Default)]
pub(crate) struct FollowerTailState {
    /// Epoch whose durable tail is currently running.
    in_flight_epoch: std::sync::atomic::AtomicU64,
    /// Highest committed epoch; advanced only on commit so a failed epoch is retried, not deduped.
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

    /// Record the tail's outcome; clears the in-flight slot only if it still belongs to `epoch`.
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

/// `true` when every live node has reported a committed-assignment version and all
/// agree. A node missing from `reported` hasn't republished since (re)joining, so it
/// is treated as not-yet-converged; disagreement means a rebalance is still
/// propagating (the leader has bumped, a follower lags). The leader's committed
/// version is the max, so all-equal ⇒ every follower has caught up.
#[cfg(feature = "cluster")]
pub(crate) fn assignment_versions_converged(
    live: &[u64],
    reported: &rustc_hash::FxHashMap<u64, u64>,
) -> bool {
    let mut seen: Option<u64> = None;
    for id in live {
        let Some(&v) = reported.get(id) else {
            return false;
        };
        match seen {
            None => seen = Some(v),
            Some(s) if s != v => return false,
            _ => {}
        }
    }
    true
}

#[allow(clippy::struct_excessive_bools)] // config/state flags, not a state machine
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
    /// Mirrors `MvStore::has_any` so the per-cycle check skips the write lock.
    pub(crate) mv_store_has_any: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    pub(crate) filter_ctx: SessionContext,
    pub(crate) compiled_sink_filters: Vec<SinkFilter>,
    pub(crate) pending_sink_filter_compiles: usize,
    pub(crate) last_checkpoint: std::time::Instant,
    pub(crate) checkpoint_interval: Option<std::time::Duration>,
    pub(crate) pipeline_hash: Option<u64>,
    pub(crate) delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee,
    /// Fault (rather than drop) on a fatal cycle error even at-least-once, so coordinated
    /// recovery can drive a global restart and an EO sink's 2PC keeps output exactly-once.
    pub(crate) coordinated_recovery: bool,
    pub(crate) serialization_timeout: Duration,
    pub(crate) sink_event_rx: laminar_core::streaming::AsyncConsumer<crate::sink_task::SinkEvent>,
    /// Set when a sink write times out; suppresses the next checkpoint to preserve the replay window.
    pub(crate) sink_timed_out: bool,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    #[cfg(feature = "cluster")]
    pub(crate) cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Cached convergence verdict for the periodic-checkpoint gate, published by the
    /// snapshot watcher. `None` in single-node mode (gate defaults open).
    #[cfg(feature = "cluster")]
    pub(crate) converged_rx: Option<tokio::sync::watch::Receiver<bool>>,
    /// In-flight epoch + highest committed epoch for follower tail dedup.
    #[cfg(feature = "cluster")]
    pub(crate) follower_tail: Arc<FollowerTailState>,
    #[cfg(feature = "cluster")]
    pub(crate) barrier_injectors: Vec<(
        Arc<str>,
        laminar_core::checkpoint::CheckpointBarrierInjector,
    )>,
    #[cfg(feature = "cluster")]
    pub(crate) pending_follower_checkpoint:
        Option<laminar_core::cluster::control::BarrierAnnouncement>,
    /// Receives `db.checkpoint()` requests; the callback captures state and replies on the oneshot.
    pub(crate) force_ckpt_rx: Option<crate::db::ForceCheckpointRx>,
    pub(crate) subscription_registry: Arc<crate::subscription::SubscriptionRegistry>,
    /// Stream → subscribing node ids; written by the gossip poller, read each cycle.
    #[cfg(feature = "cluster")]
    pub(crate) active_subs:
        Arc<parking_lot::RwLock<std::collections::HashMap<String, std::collections::HashSet<u64>>>>,
    /// Single-consumer SUBSCRIBE routing channel; one consumer preserves emission order.
    #[cfg(feature = "cluster")]
    pub(crate) sub_route:
        std::sync::OnceLock<tokio::sync::mpsc::Sender<(String, RecordBatch, Vec<u64>)>>,
    pub(crate) static_stream_names: rustc_hash::FxHashSet<Arc<str>>,
    pub(crate) checkpoint_complete_tx: crossfire::MAsyncTx<
        crossfire::mpsc::Array<(u64, rustc_hash::FxHashMap<String, SourceCheckpoint>)>,
    >,
    /// In-flight epoch count; the coordinator gates new barriers against `max_in_flight_epochs`.
    pub(crate) checkpoint_in_flight: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Lock-free id allocator shared with the coordinator so barrier admission doesn't
    /// queue behind an earlier epoch's durable tail holding the coordinator mutex.
    pub(crate) epoch_allocator: Option<Arc<crate::checkpoint_coordinator::EpochAllocator>>,
    #[cfg(feature = "cluster")]
    pub(crate) quorum_timeout: Duration,
    /// When true, durable tails run inline so a transactional producer never sees
    /// post-barrier rows while its epoch-N transaction is still open.
    pub(crate) exactly_once_sinks: bool,
    pub(crate) state_memory_budget_bytes: Option<usize>,
    pub(crate) state_budget_probe_at: std::time::Instant,
    pub(crate) state_budget_exceeded: bool,
    /// Cold-tier send channel; `None` = no tier configured.
    #[cfg(feature = "state-tier")]
    pub(crate) state_tier: Option<crate::state_tier::TierTx>,
}

/// Minimum interval between budget probes; each probe walks all operator estimates.
const STATE_BUDGET_PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Demotion begins at 4/5 of the budget — below the 100% backpressure point.
#[cfg(feature = "state-tier")]
const STATE_DEMOTE_WATERMARK_NUM: usize = 4;
#[cfg(feature = "state-tier")]
const STATE_DEMOTE_WATERMARK_DEN: usize = 5;
/// Demotion target per pass: 13/20 of the budget (hysteresis against thrash).
#[cfg(feature = "state-tier")]
const STATE_DEMOTE_TARGET_NUM: usize = 13;
#[cfg(feature = "state-tier")]
const STATE_DEMOTE_TARGET_DEN: usize = 20;
/// Max vnodes demoted per maintenance pass.
#[cfg(feature = "state-tier")]
const STATE_DEMOTE_MAX_PER_PASS: usize = 32;

/// Send a slice to the cold tier; returns `false` if the worker is gone.
#[cfg(feature = "state-tier")]
async fn tier_demote(
    tier: &crate::state_tier::TierTx,
    operator: &str,
    vnode: u32,
    bytes: bytes::Bytes,
) -> bool {
    let (reply, rx) = tokio::sync::oneshot::channel();
    let req = crate::state_tier::TierRequest::Demote {
        operator: Arc::from(operator),
        vnode,
        bytes,
        reply,
    };
    if tier.send(req).await.is_err() {
        return false;
    }
    matches!(rx.await, Ok(Ok(())))
}

/// Roll back a tier write when the operator refused to drop the slice (dirty since capture).
#[cfg(feature = "state-tier")]
async fn tier_drop(tier: &crate::state_tier::TierTx, operator: &str, vnode: u32) {
    let (reply, rx) = tokio::sync::oneshot::channel();
    let req = crate::state_tier::TierRequest::Drop {
        operator: Arc::from(operator),
        vnode,
        reply,
    };
    if tier.send(req).await.is_ok() {
        let _ = rx.await;
    }
}

/// Demote idle vnode slices to the cold tier until memory falls below `target_bytes`.
///
/// Only candidates clean since their last capture are eligible, so dirty vnodes
/// are skipped before any tier I/O. Must be called with no checkpoint in flight.
#[cfg(feature = "state-tier")]
pub(crate) async fn run_demotion_pass(
    graph: &mut crate::operator_graph::OperatorGraph,
    coordinator: &Arc<
        tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>,
    >,
    tier: &crate::state_tier::TierTx,
    total_bytes: usize,
    target_bytes: usize,
) -> u64 {
    // Build the plan under the lock; release before any tier I/O.
    let plan: Vec<(u32, Vec<(String, bytes::Bytes)>)> = {
        let guard = coordinator.lock().await;
        let Some(coord) = guard.as_ref() else {
            return 0;
        };
        let mut plan = Vec::new();
        let mut freed = 0usize;
        for (vnode, _) in coord.demotion_candidates() {
            if plan.len() >= STATE_DEMOTE_MAX_PER_PASS
                || total_bytes.saturating_sub(freed) < target_bytes
            {
                break;
            }
            // Pre-filter to operators that can demote now; avoids a write-then-rollback.
            let eligible: Vec<(String, bytes::Bytes)> = coord
                .slices_for_demotion(vnode)
                .into_iter()
                .filter(|(op, _)| graph.can_demote(op, vnode))
                .collect();
            if eligible.is_empty() {
                continue;
            }
            let eligible_bytes: usize = eligible.iter().map(|(_, b)| b.len()).sum();
            freed = freed.saturating_add(eligible_bytes);
            plan.push((vnode, eligible));
        }
        plan
    };

    let mut demoted = 0u64;
    for (vnode, slices) in plan {
        for (op, bytes) in &slices {
            if !tier_demote(tier, op, vnode, bytes.clone()).await {
                continue;
            }
            if graph.demote_vnode(op, vnode) {
                let mut guard = coordinator.lock().await;
                if let Some(coord) = guard.as_mut() {
                    coord.mark_slice_demoted(vnode, op);
                }
                demoted += 1;
            } else {
                tier_drop(tier, op, vnode).await;
            }
        }
    }
    demoted
}

impl ConnectorPipelineCallback {
    /// Classify a graph error; `BackpressureFail` also signals shutdown.
    fn map_graph_error(
        err: &crate::error::DbError,
        shutdown: &tokio::sync::Notify,
    ) -> crate::pipeline::CycleError {
        use crate::pipeline::CycleError;
        if let crate::error::DbError::BackpressureFail(msg) = err {
            tracing::error!(reason = %msg, "backpressure_policy=Fail tripped; halting pipeline");
            shutdown.notify_one();
            return CycleError::Halt(format!("{err}"));
        }
        CycleError::Fatal(format!("{err}"))
    }

    /// Cap each source watermark by the cluster-wide min, if one has been published.
    ///
    /// `None` leaves the map untouched — capping to `i64::MIN` would freeze the pipeline.
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

    /// Route this cycle's output to remote SUBSCRIBE peers. No-op without remote interest.
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

        // One long-lived consumer preserves emission order to the peer.
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
                                tracing::warn!(node, error = %e, "subscription send failed");
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
        // At-most-once under backpressure: drop if the routing queue is full.
        for item in to_send {
            if tx.try_send(item).is_err() {
                self.prom.remote_subscription_batches_dropped.inc();
                warn_subscription_route_drop("routing queue full");
            }
        }
    }

    /// Capture operator state and commit a checkpoint for a `db.checkpoint()` request.
    ///
    /// `source_offsets` are the coordinator's current committed offsets; a forced
    /// checkpoint has no source barrier, so without them the manifest advances the
    /// recovery point with no offsets and a recovered source replays from the start.
    async fn force_capture_and_checkpoint(
        &mut self,
        source_offsets: &FxHashMap<String, SourceCheckpoint>,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, DbError> {
        self.sync_sinks_and_drain_events().await;

        // Thread the pre-allocated ids through; a second allocation would checkpoint
        // a different epoch than the one announced.
        #[cfg(feature = "cluster")]
        let leader_ids = self.align_shuffle_for_leader().await?;
        #[cfg(not(feature = "cluster"))]
        let leader_ids: Option<(u64, u64)> = None;

        let operator_states = self
            .capture_and_serialize_operator_state()
            .await
            .map_err(DbError::Checkpoint)?;
        let request = self.build_checkpoint_request(operator_states, source_offsets);

        let vnode_states = self.capture_vnode_states();
        let mut guard = self.coordinator.lock().await;
        let coord = guard.as_mut().ok_or_else(|| {
            DbError::Checkpoint(
                "coordinator not initialized when force_capture_and_checkpoint ran".into(),
            )
        })?;
        coord.set_pending_vnode_states(vnode_states);
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

    /// `true` when `ann_epoch` is already committed, pending, or in flight.
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

    /// Leader durable tail: quorum + `Aligned` pre-mutex, then durable writes under the FIFO mutex.
    async fn run_leader_tail(tail: LeaderTail) {
        use crate::checkpoint_coordinator::QuorumStage;

        let _in_flight = tail.in_flight; // released on drop

        #[allow(unused_mut)]
        let mut quorum = QuorumStage::RunInline;
        #[cfg(feature = "cluster")]
        if let (Some(cc), Some((epoch, checkpoint_id))) =
            (tail.controller.as_ref(), tail.leader_ids)
        {
            use crate::checkpoint_coordinator::CheckpointCoordinator;
            use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
            match CheckpointCoordinator::run_prepare_quorum(
                cc,
                tail.quorum_timeout,
                epoch,
                checkpoint_id,
                tail.local_watermark_ms,
            )
            .await
            {
                Ok((min_watermark_ms, participants)) => {
                    if let Err(e) = cc
                        .announce_barrier(&BarrierAnnouncement {
                            epoch,
                            checkpoint_id,
                            phase: Phase::Aligned,
                            flags: 0,
                            min_watermark_ms,
                        })
                        .await
                    {
                        tracing::warn!(
                            epoch, error = %e,
                            "[LDB-6031] aligned announcement failed; peers resume on Commit"
                        );
                    }
                    quorum = QuorumStage::Done {
                        min_watermark_ms,
                        participants,
                    };
                }
                Err(msg) => {
                    tracing::error!(checkpoint_id, epoch, error = %msg, "[LDB-6032] quorum miss");
                    let mut guard = tail.coordinator.lock().await;
                    if let Some(ref mut coord) = *guard {
                        coord.abandon_epoch(checkpoint_id, epoch, msg).await;
                    }
                    return;
                }
            }
        }

        let mut guard = tail.coordinator.lock().await;
        if let Some(ref mut coord) = *guard {
            coord.set_pending_vnode_states(tail.vnode_states);
            coord.set_local_watermark_ms(tail.local_watermark_ms);
            let result = match tail.leader_ids {
                Some((epoch, checkpoint_id)) => {
                    coord
                        .checkpoint_preallocated(tail.request, epoch, checkpoint_id, quorum)
                        .await
                }
                None => coord.checkpoint_with_offsets(tail.request).await,
            };
            match result {
                Ok(result) if result.success => {
                    let _ = tail.complete_tx.send((result.epoch, tail.fan_out)).await;
                }
                Ok(result) => tracing::warn!(
                    epoch = result.epoch,
                    error = ?result.error,
                    "Barrier-aligned checkpoint failed"
                ),
                Err(e) => tracing::warn!(error = %e, "Barrier-aligned checkpoint error"),
            }
        }
    }

    /// Build the follower's durable tail future (ack → prepare → decision wait → 2PC).
    ///
    /// Spawned for at-least-once (resumes on `Aligned`) or awaited inline for exactly-once.
    #[cfg(feature = "cluster")]
    fn follower_tail_future(
        &mut self,
        request: crate::checkpoint_coordinator::CheckpointRequest,
        epoch: u64,
        checkpoint_id: u64,
        fan_out: FxHashMap<String, SourceCheckpoint>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let vnode_states = self.capture_vnode_states();
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        self.follower_tail.begin(epoch);

        // Charge followers too; otherwise their capture-to-upload memory is unaccounted.
        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.staged_bytes,
            staged_request_bytes(&request, &vnode_states),
        );
        let coordinator = Arc::clone(&self.coordinator);
        let tail = Arc::clone(&self.follower_tail);
        let complete_tx = self.checkpoint_complete_tx.clone();
        let cc = self.cluster_controller.clone();
        async move {
            use crate::checkpoint_coordinator::CheckpointCoordinator;

            let _in_flight = in_flight; // released on drop

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

            let decision_store = {
                let mut guard = coordinator.lock().await;
                match guard.as_mut() {
                    Some(coord) => {
                        coord.set_pending_vnode_states(vnode_states);
                        coord.set_local_watermark_ms(wm_opt);
                        match coord
                            .follower_prepare_acked(request, epoch, checkpoint_id)
                            .await
                        {
                            Ok(()) => Some(coord.decision_store_handle()),
                            Err(e) => {
                                tracing::warn!(epoch, error = %e, "follower prepare errored");
                                None
                            }
                        }
                    }
                    None => None,
                }
            };

            // Await the decision outside the mutex so the next epoch's prepare can proceed.
            let committed = match (decision_store, cc.as_ref()) {
                (Some(decision_store), Some(cc)) => {
                    let verdict = CheckpointCoordinator::await_follower_decision(
                        cc,
                        decision_store.as_deref(),
                        epoch,
                        checkpoint_id,
                        std::time::Duration::from_secs(10),
                    )
                    .await;
                    let mut guard = coordinator.lock().await;
                    match guard.as_mut() {
                        Some(coord) => {
                            let committed =
                                coord.follower_finish(epoch, checkpoint_id, verdict).await;
                            if committed {
                                tracing::info!(epoch, "follower checkpoint committed");
                            } else {
                                tracing::warn!(epoch, "follower checkpoint aborted");
                            }
                            committed
                        }
                        None => false,
                    }
                }
                _ => false,
            };

            tail.finish(epoch, committed);
            if committed {
                let _ = complete_tx.send((epoch, fan_out)).await;
            }
        }
    }

    /// Hold the pipeline until the leader announces `Aligned` (or `Commit`/`Abort`/newer epoch).
    ///
    /// Prevents epoch-N+1 shuffle rows from reaching a peer still snapshotting epoch-N.
    /// No-op without a cross-node shuffle; bounded — on timeout the epoch aborts via the leader.
    #[cfg(feature = "cluster")]
    async fn wait_for_aligned_resume(
        has_cluster_shuffle: bool,
        controller: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
    ) {
        use laminar_core::cluster::control::Phase;

        const RESUME_GATE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

        if !has_cluster_shuffle {
            return;
        }
        let released = controller
            .wait_for_barrier(
                |a| {
                    a.epoch > epoch
                        || (a.epoch == epoch
                            && matches!(a.phase, Phase::Aligned | Phase::Commit | Phase::Abort))
                },
                RESUME_GATE_TIMEOUT,
            )
            .await;
        if released.is_none() {
            tracing::warn!(
                epoch,
                "aligned resume gate timed out — resuming pipeline \
                 (epoch will abort via the leader's restorable gate)"
            );
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

        let stall_start = std::time::Instant::now();
        let epoch = ann.epoch;
        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let tail = self.follower_tail_future(request, ann.epoch, ann.checkpoint_id, source_offsets);
        if self.exactly_once_sinks {
            tail.await;
        } else {
            tokio::spawn(tail);
            Self::wait_for_aligned_resume(has_shuffle, &controller, epoch).await;
        }
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

        let stall_start = std::time::Instant::now();
        let epoch = ann.epoch;
        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let tail =
            self.follower_tail_future(request, ann.epoch, ann.checkpoint_id, source_checkpoints);
        if self.exactly_once_sinks {
            tail.await;
        } else {
            tokio::spawn(tail);
            Self::wait_for_aligned_resume(has_shuffle, &controller, epoch).await;
        }
        self.prom
            .checkpoint_pipeline_stall_duration
            .observe(stall_start.elapsed().as_secs_f64());
        self.last_checkpoint = std::time::Instant::now();
        BarrierOutcome::Async
    }

    /// Allocate barrier ids, announce `Prepare`, and align the cross-node shuffle.
    ///
    /// Returns `None` when not the leader or no coordinator is wired. On alignment
    /// failure the epoch is abandoned.
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
        // A reclaiming leader recovers to its last committed epoch, which can lag the
        // cluster's in-flight epoch; allocating below it would re-announce ids caught-up
        // followers skip, deadlocking alignment. Start above the cluster-wide max.
        if let Some((max_epoch, max_cid)) = cc.max_announced_epoch().await {
            allocator.advance_to(max_epoch.saturating_add(1), max_cid.saturating_add(1));
        }
        let (epoch, checkpoint_id) = allocator.allocate();
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

    /// Build a `CheckpointRequest` from operator states and source offsets.
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

        let mv_states = self
            .mv_store
            .read()
            .checkpoint_states()
            .map_err(|e| format!("MV checkpoint failed: {e}"))?;
        operator_states.extend(mv_states);

        Ok(operator_states)
    }

    /// Capture per-vnode operator state for the in-flight checkpoint.
    ///
    /// Empty outside cluster mode. On error, logs and returns an empty map so the
    /// checkpoint still succeeds (the partial carries no operator state for this epoch).
    #[allow(clippy::unused_self, clippy::disallowed_types)] // matches the coordinator/graph map shape
    fn capture_vnode_states(&mut self) -> crate::checkpoint_coordinator::StagedVnodeStates {
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

    /// Sync all sinks and drain their events; `sink_timed_out` is current after this returns.
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
    ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, crate::pipeline::CycleError> {
        // Test-only one-shot fault injector for the recovery soak (inert in release / when
        // unset): the first cycle after `LAMINAR_FAULT_INJECT_AFTER_MS` faults once.
        #[cfg(debug_assertions)]
        {
            use std::sync::atomic::{AtomicBool, Ordering};
            use std::sync::OnceLock;
            use std::time::{Duration, Instant};
            static AFTER_MS: OnceLock<Option<u64>> = OnceLock::new();
            static START: OnceLock<Instant> = OnceLock::new();
            static FIRED: AtomicBool = AtomicBool::new(false);
            if let Some(after_ms) = *AFTER_MS.get_or_init(|| {
                std::env::var("LAMINAR_FAULT_INJECT_AFTER_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
            }) {
                let start = START.get_or_init(Instant::now);
                if !FIRED.load(Ordering::Relaxed)
                    && start.elapsed() >= Duration::from_millis(after_ms)
                    && FIRED
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                        .is_ok()
                {
                    return Err(crate::pipeline::CycleError::Fatal(
                        "injected fault for coordinated-recovery soak \
                         (LAMINAR_FAULT_INJECT_AFTER_MS)"
                            .into(),
                    ));
                }
            }
        }
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

        // Ephemeral streams (console live queries) aren't in `stream_sources`; push them here.
        // MVs reach subscribers via their own path below.
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

    async fn close_sinks(&mut self) {
        // Concurrently — a stalled sink shouldn't add its 15s timeout to every other one
        // (recovery downtime would be N×15s with serial closes).
        futures::future::join_all(self.sinks.iter().map(|(_, h, _, _, _)| h.close())).await;
    }

    async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
        self.compile_pending_sink_filters(results).await;

        // Shared Arc per stream so multiple sinks don't each clone the Vec.
        let mut shared_inputs: FxHashMap<&str, Arc<[RecordBatch]>> = FxHashMap::default();

        let sink_futures: Vec<_> = self
            .sinks
            .iter()
            .enumerate()
            .filter_map(
                |(sink_idx, (sink_name, handle, _filter_sql, sink_input, changelog_capable))| {
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

        // Opportunistic; the strict barrier runs in the checkpoint path.
        self.drain_sink_events();
    }

    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch) {
        if let Some(wm_state) = self.watermark_states.get_mut(source_name) {
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

        #[allow(clippy::cast_possible_truncation)]
        let row_count = batch.num_rows() as u64;
        self.prom.events_ingested.inc_by(row_count);
        self.prom.batches.inc();
    }

    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
        if let Some(wm_state) = self.watermark_states.get(source_name) {
            // Processing-time watermarks are wall-clock; filtering would drop every real row.
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
                        // Schema drift, not lateness.
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

    fn is_recovering(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            if let Some(ref cc) = self.cluster_controller {
                return cc.is_recovering();
            }
        }
        false
    }

    fn fault_on_cycle_error(&self) -> bool {
        use laminar_connectors::connector::DeliveryGuarantee;
        self.delivery_guarantee == DeliveryGuarantee::ExactlyOnce || self.coordinated_recovery
    }

    #[cfg(feature = "cluster")]
    async fn assignment_ready_for_checkpoint(&mut self) -> bool {
        // Local borrow of the verdict the snapshot watcher computes off the hot path
        // (see `rebalance::spawn_snapshot_watcher`); no gossip scan on the gate.
        self.converged_rx.as_ref().is_none_or(|rx| *rx.borrow())
    }

    fn tick_idle_watermark(&mut self) {
        let Some(ref mut trk) = self.tracker else {
            return;
        };
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

    /// Timer-based checkpoint (at-least-once). For exactly-once use barrier-aligned checkpoints.
    async fn maybe_checkpoint(
        &mut self,
        force: bool,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64> {
        // Drain `db.checkpoint()` requests; each gets a full operator-state capture.
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
            let result = self.force_capture_and_checkpoint(&source_offsets).await;
            // Ignore receiver-dropped: caller gave up on `db.checkpoint()`.
            reply_tx.send(result);
        }

        // Only the leader fires periodic checkpoints; followers pick up PREPARE via gossip.
        #[cfg(feature = "cluster")]
        if !force {
            if let Some(cc) = self.cluster_controller.clone() {
                if cc.is_leader() {
                    // Periodic checkpoints are injected by the coordinator, not here.
                    return None;
                }
                return self.maybe_follower_checkpoint(cc, source_offsets).await;
            }
        }

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

        // Skip after a sink timeout so offsets don't advance past the dropped batch.
        if !force && self.sink_timed_out {
            self.sink_timed_out = false;
            tracing::info!("skipping checkpoint after sink timeout to preserve replay window");
            return None;
        }

        if !force {
            let interval = self.checkpoint_interval?;
            if self.last_checkpoint.elapsed() < interval {
                return None;
            }
        }

        // Abort if serialization fails — advancing offsets without matching operator state
        // would lose data on recovery.
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
            let in_flight = EpochInFlightGuard::claim(
                &self.checkpoint_in_flight,
                &self.staged_bytes,
                staged_request_bytes(&request, &vnode_states),
            );
            let coordinator_clone = Arc::clone(&self.coordinator);
            let tx = self.checkpoint_complete_tx.clone();
            tokio::spawn(async move {
                let _in_flight = in_flight; // released on drop
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
                    // A slow round can finish after its epoch was abandoned; never attribute
                    // captured offsets to a newer announcement.
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

        // Clear after one suppression; the timer path is unreachable under barrier checkpointing.
        if self.sink_timed_out {
            self.sink_timed_out = false;
            return BarrierOutcome::Skipped(SkipReason::PreservingReplayWindowAfterSinkTimeout);
        }

        let stall_start = std::time::Instant::now();

        // Align the shuffle before capture so peers' pre-barrier rows enter the snapshot.
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

        let operator_states = match self.capture_and_serialize_operator_state().await {
            Ok(states) => states,
            Err(e) => {
                tracing::warn!(error = %e, "Stream executor barrier checkpoint failed");
                return BarrierOutcome::Failed;
            }
        };

        let vnode_states = self.capture_vnode_states();
        let request = self.build_checkpoint_request(operator_states, &source_checkpoints);

        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.staged_bytes,
            staged_request_bytes(&request, &vnode_states),
        );
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        let tail = LeaderTail {
            in_flight,
            coordinator: Arc::clone(&self.coordinator),
            complete_tx: self.checkpoint_complete_tx.clone(),
            request,
            vnode_states,
            fan_out: source_checkpoints.clone(),
            local_watermark_ms: if wm == i64::MIN { None } else { Some(wm) },
            leader_ids,
            #[cfg(feature = "cluster")]
            controller: self.cluster_controller.clone(),
            #[cfg(feature = "cluster")]
            quorum_timeout: self.quorum_timeout,
        };
        if self.exactly_once_sinks {
            Self::run_leader_tail(tail).await;
        } else {
            tokio::spawn(Self::run_leader_tail(tail));

            #[cfg(feature = "cluster")]
            if let (Some((epoch, _)), Some(cc)) = (leader_ids, self.cluster_controller.clone()) {
                let has_shuffle = self.graph.cluster_shuffle_config().is_some();
                Self::wait_for_aligned_resume(has_shuffle, &cc, epoch).await;
            }
        }
        self.prom
            .checkpoint_pipeline_stall_duration
            .observe(stall_start.elapsed().as_secs_f64());

        self.last_checkpoint = std::time::Instant::now();
        BarrierOutcome::Async
    }

    fn record_cycle(&self, events_ingested: u64, _batches: u64, elapsed_ns: u64) {
        let _ = events_ingested; // counted in extract_watermark
        self.prom.cycles.inc();
        #[allow(clippy::cast_precision_loss)]
        self.prom
            .cycle_duration
            .observe(elapsed_ns as f64 / 1_000_000_000.0);
    }

    fn note_cycle_error(&self) {
        self.prom.pipeline_cycle_errors_total.inc();
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

    fn state_over_budget(&mut self) -> bool {
        let Some(budget) = self.state_memory_budget_bytes else {
            return false;
        };
        // Probe is throttled; cached verdict is served between probes.
        if self.state_budget_probe_at.elapsed() >= STATE_BUDGET_PROBE_INTERVAL {
            self.state_budget_probe_at = std::time::Instant::now();
            #[allow(clippy::cast_possible_wrap)]
            self.prom.state_memory_budget_bytes.set(budget as i64);
            let mut total = 0usize;
            for (name, bytes) in self.graph.state_bytes_per_operator() {
                #[allow(clippy::cast_possible_wrap)]
                self.prom
                    .operator_state_bytes
                    .with_label_values(&[name.as_ref()])
                    .set(bytes as i64);
                total = total.saturating_add(bytes);
            }
            #[allow(clippy::cast_possible_wrap)]
            self.prom.state_bytes.set(total as i64);
            let exceeded = total >= budget;
            if exceeded != self.state_budget_exceeded {
                if exceeded {
                    tracing::warn!(
                        state_bytes = total,
                        budget_bytes = budget,
                        "operator state over memory budget — pausing source intake"
                    );
                } else {
                    tracing::info!(
                        state_bytes = total,
                        budget_bytes = budget,
                        "operator state back under memory budget — resuming source intake"
                    );
                }
            }
            self.state_budget_exceeded = exceeded;
            self.prom.state_over_budget.set(i64::from(exceeded));
        }
        if self.state_budget_exceeded {
            self.prom.state_budget_paused_cycles.inc();
        }
        self.state_budget_exceeded
    }

    fn publish_barrier(&self, epoch: u64, checkpoint_id: u64) {
        self.subscription_registry
            .broadcast_barrier(epoch, checkpoint_id);
    }

    #[cfg(feature = "state-tier")]
    async fn maybe_demote_state(&mut self) {
        use std::sync::atomic::Ordering;
        let Some(tier) = self.state_tier.clone() else {
            return;
        };
        let Some(budget) = self.state_memory_budget_bytes else {
            return;
        };
        // Require no checkpoint in flight: a clean vnode's resident state then equals
        // the durable bytes recorded by the coordinator.
        if self.checkpoint_in_flight.load(Ordering::Acquire) != 0 {
            return;
        }
        let total: usize = self.graph.state_bytes_per_operator().map(|(_, b)| b).sum();
        let watermark = budget / STATE_DEMOTE_WATERMARK_DEN * STATE_DEMOTE_WATERMARK_NUM;
        if total < watermark {
            return;
        }
        let target = budget / STATE_DEMOTE_TARGET_DEN * STATE_DEMOTE_TARGET_NUM;
        let coordinator = Arc::clone(&self.coordinator);
        let demoted = run_demotion_pass(&mut self.graph, &coordinator, &tier, total, target).await;
        if demoted > 0 {
            tracing::debug!(demoted, "demoted idle vnode slices to the cold tier");
        }
    }

    fn has_deferred_input(&self) -> bool {
        // In cluster mode, always return true so the coordinator runs `execute_cycle` each idle
        // tick; without this a follower with no local sources never drains the shuffle receiver.
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
                                    continue;
                                }
                            }
                        };
                        let Some(state) = rebuild_versioned_state(versioned, combined) else {
                            tracing::warn!(
                                table=%name, version_col=%versioned.version_column,
                                "versioned index rebuild skipped (version column missing or build failed)"
                            );
                            continue;
                        };
                        self.lookup_registry.register_versioned(name, state);
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
                        // `ReferenceTableProvider` reads live data; no DataFusion re-registration needed.
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
        // Lock-free: an in-flight tail may hold the coordinator mutex.
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

/// Upsert or delete each row in a CDC batch from the partial lookup cache.
///
/// Deletes are detected via an `__op`/`__operation`/`op` column with value `d`/`D`/`delete`/`DELETE`.
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

/// Rebuild a versioned lookup state over `batch`, reusing the prior state's key
/// and version columns. `None` if the version column is absent or the index
/// fails to build.
pub(crate) fn rebuild_versioned_state(
    prior: &laminar_sql::datafusion::VersionedLookupState,
    batch: RecordBatch,
) -> Option<laminar_sql::datafusion::VersionedLookupState> {
    let key_indices: Vec<usize> = prior
        .key_columns
        .iter()
        .filter_map(|k| batch.schema().index_of(k).ok())
        .collect();
    let version_col_idx = batch.schema().index_of(&prior.version_column).ok()?;
    let index = laminar_sql::datafusion::lookup_join_exec::VersionedIndex::build(
        &batch,
        &key_indices,
        version_col_idx,
        prior.max_versions_per_key,
    )
    .ok()?;
    Some(laminar_sql::datafusion::VersionedLookupState {
        batch,
        index: Arc::new(index),
        key_columns: prior.key_columns.clone(),
        version_column: prior.version_column.clone(),
        stream_time_column: prior.stream_time_column.clone(),
        max_versions_per_key: prior.max_versions_per_key,
    })
}

/// Encode an Arrow schema as a hex-encoded IPC flatbuffer.
pub(crate) fn encode_arrow_schema(schema: &arrow_schema::Schema) -> String {
    laminar_connectors::config::encode_arrow_schema_ipc(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DbError;

    #[tokio::test]
    async fn test_backpressure_fail_notifies_shutdown() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::BackpressureFail("downstream of 'q'".into());
        let mapped = ConnectorPipelineCallback::map_graph_error(&err, &notify);
        assert!(
            matches!(&mapped, CycleError::Halt(m) if m.contains("Backpressure fail")),
            "unexpected: {mapped:?}"
        );

        tokio::time::timeout(Duration::from_millis(50), notify.notified())
            .await
            .expect("shutdown should have been notified");
    }

    #[tokio::test]
    async fn test_non_backpressure_error_does_not_notify() {
        use crate::pipeline::CycleError;
        let notify = Arc::new(tokio::sync::Notify::new());
        let err = DbError::Pipeline("unrelated".into());
        let mapped = ConnectorPipelineCallback::map_graph_error(&err, &notify);
        assert!(
            matches!(mapped, CycleError::Fatal(_)),
            "non-Fail errors must classify as Fatal"
        );

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

    /// `follower_should_skip(committed, pending, tail_in_flight, announced)`:
    /// skip when ANY of the three trackers already covers the announced
    /// epoch. Committed advances only on commit, so a failed epoch's
    /// retry (committed still behind) is reprocessed, not deduped — the
    /// old code recorded the epoch before commit and wedged.
    #[cfg(feature = "cluster")]
    #[test]
    fn follower_should_skip_dedup_matrix() {
        let skip = ConnectorPipelineCallback::follower_should_skip;
        // Already committed (or older re-announcement).
        assert!(skip(Some(5), None, None, 5));
        assert!(skip(Some(5), None, None, 3));
        // Deferred, awaiting local barriers.
        assert!(skip(Some(4), Some(5), None, 5));
        // Durable tail running in the background (pipeline resumed; the
        // Prepare stays visible under latest-wins observation).
        assert!(skip(Some(4), None, Some(5), 5));
        // Failed-epoch retry: committed didn't advance — reprocess.
        assert!(!skip(Some(4), None, None, 5));
        assert!(!skip(None, None, None, 5));
        // A higher epoch is always processed.
        assert!(!skip(Some(5), None, None, 6));
        assert!(!skip(Some(5), Some(5), Some(5), 6));
    }

    /// The leader's checkpoint-convergence gate: ready only when every live node
    /// has reported the same committed-assignment version. A respawned node that
    /// lags (or hasn't republished yet) holds the gate closed.
    #[cfg(feature = "cluster")]
    #[test]
    fn assignment_versions_converged_matrix() {
        let map = |pairs: &[(u64, u64)]| -> rustc_hash::FxHashMap<u64, u64> {
            pairs.iter().copied().collect()
        };
        // All live nodes on the same version → converged.
        assert!(assignment_versions_converged(
            &[1, 2, 3],
            &map(&[(1, 5), (2, 5), (3, 5)])
        ));
        // A follower lagging behind the leader's newer version → not converged.
        assert!(!assignment_versions_converged(
            &[1, 2, 3],
            &map(&[(1, 6), (2, 6), (3, 5)])
        ));
        // A live node with no reported version yet (just rejoined) → not converged.
        assert!(!assignment_versions_converged(
            &[1, 2, 3],
            &map(&[(1, 5), (2, 5)])
        ));
        // Stale entries for dead nodes don't matter — only live ids are checked.
        assert!(assignment_versions_converged(
            &[1, 2],
            &map(&[(1, 7), (2, 7), (9, 3)])
        ));
        // Single live node is trivially converged.
        assert!(assignment_versions_converged(&[1], &map(&[(1, 4)])));
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

#[cfg(all(test, feature = "state-tier"))]
mod demotion_tests {
    use super::run_demotion_pass;
    use crate::operator_graph::OperatorGraph;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{NodeId, StateBackend, VnodeRegistry};
    use laminar_sql::parser::EmitClause;
    use rustc_hash::FxHashMap;
    use std::sync::Arc;

    const VNODES: u32 = 8;

    fn events_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    fn events_batch(keys: &[&str], vals: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            events_schema(),
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    async fn single_node_shuffle() -> crate::operator::sql_query::ClusterShuffleConfig {
        let registry = Arc::new(VnodeRegistry::new(VNODES));
        let assignment: Arc<[NodeId]> = (0..VNODES).map(|_| NodeId(1)).collect::<Vec<_>>().into();
        registry.set_assignment(assignment);
        let sender = laminar_core::shuffle::ShuffleSender::new(1);
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        crate::operator::sql_query::ClusterShuffleConfig {
            registry,
            sender: Arc::new(sender),
            receiver,
            self_id: NodeId(1),
        }
    }

    async fn build_agg_graph(tier: crate::state_tier::TierTx) -> OperatorGraph {
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.set_cluster_shuffle(single_node_shuffle().await);
        graph.set_state_tier(tier);
        graph.set_runtime_handle(tokio::runtime::Handle::current());
        graph.register_source_schema("events".to_string(), events_schema());
        graph.add_query(
            "out".to_string(),
            "SELECT key, SUM(val) AS total FROM events GROUP BY key".to_string(),
            Some(EmitClause::Changes),
            None,
            None,
            None,
            None,
        );
        graph.take_build_errors().unwrap();
        graph
    }

    fn graph_state_bytes(graph: &OperatorGraph) -> usize {
        graph.state_bytes_per_operator().map(|(_, b)| b).sum()
    }

    /// End-to-end trigger: an agg with resident vnode state, checkpointed so
    /// the coordinator holds durable upload bytes, is demoted by the pass —
    /// the slices land in the tier, drop from operator memory, and the
    /// coordinator marks them cold (no longer candidates).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn demotion_pass_sheds_idle_slices() {
        use crate::checkpoint_coordinator::{
            CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
        };
        use laminar_core::state::InProcessBackend;
        use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            crate::state_tier::StateTierStore::open(tmp.path().join("tier"), None).unwrap(),
        );
        let tier_tx = crate::state_tier::spawn_worker(
            &tokio::runtime::Handle::current(),
            Arc::clone(&store),
            64,
        );

        let mut graph = build_agg_graph(tier_tx.clone()).await;
        let mut src: FxHashMap<Arc<str>, Vec<RecordBatch>> = FxHashMap::default();
        src.insert(
            Arc::from("events"),
            vec![events_batch(&["a", "b", "c", "d"], &[1, 2, 3, 4])],
        );
        graph.execute_cycle(&src, i64::MAX, None).await.unwrap();
        let before = graph_state_bytes(&graph);
        assert!(before > 0, "operator should hold group state");

        // Capture per-vnode state and commit it through a coordinator so the
        // durable upload bytes are recorded.
        let states = graph.snapshot_state_by_vnode().unwrap();
        assert!(!states.is_empty(), "agg state should partition by vnode");

        let ckpt_dir = tempfile::tempdir().unwrap();
        let store_box = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path(), 3));
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store_box)
            .await
            .unwrap();
        coord.set_state_backend(Arc::new(InProcessBackend::new(VNODES)) as Arc<dyn StateBackend>);
        coord.set_vnode_set((0..VNODES).collect());
        coord.set_pending_vnode_states(states);
        let r = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(r.success, "checkpoint must commit: {:?}", r.error);
        assert!(
            !coord.demotion_candidates().is_empty(),
            "committed slices should be demotion candidates"
        );
        let coordinator = Arc::new(tokio::sync::Mutex::new(Some(coord)));

        // Drain target 0 → demote every candidate.
        let demoted = run_demotion_pass(&mut graph, &coordinator, &tier_tx, before, 0).await;
        assert!(demoted > 0, "the pass should demote at least one slice");

        assert_eq!(
            graph_state_bytes(&graph),
            0,
            "demoted slices leave operator memory"
        );
        assert!(
            store.logical_slices() > 0,
            "demoted slices are written to the tier"
        );
        let guard = coordinator.lock().await;
        assert!(
            guard.as_ref().unwrap().demotion_candidates().is_empty(),
            "demoted slices are marked cold and no longer candidates"
        );
    }
}
