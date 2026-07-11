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
use laminar_connectors::connector::SinkContract;
use laminar_core::state::CheckpointAttempt;
use laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint;
use laminar_core::streaming;
use rustc_hash::FxHashMap;

use crate::db::{filter_late_rows, SourceWatermarkState};
use crate::error::DbError;
use crate::pipeline::CheckpointCompletion;

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

/// Terminal failure reporting is cleanup, not part of the durable attempt. A failure discovered
/// at the attempt deadline must still release its manual caller and exact-attempt bookkeeping.
const CHECKPOINT_FAILURE_REPORT_TIMEOUT: Duration = Duration::from_secs(1);

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
    _in_flight: EpochInFlightGuard,
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    complete_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    request: crate::checkpoint_coordinator::CheckpointRequest,
    #[allow(clippy::disallowed_types)]
    vnode_states: crate::checkpoint_coordinator::StagedVnodeStates,
    fan_out: FxHashMap<String, SourceCheckpoint>,
    local_watermark_ms: Option<i64>,
    attempt: CheckpointAttempt,
    attempt_started: std::time::Instant,
    checkpoint_timeout: Duration,
    checkpoint_cleanup_timeout: Duration,
    fault_on_failure: bool,
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    #[cfg(feature = "cluster")]
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    #[cfg(feature = "cluster")]
    quorum_timeout: Duration,
    #[cfg(feature = "cluster")]
    delta_rebase_needed: Arc<std::sync::atomic::AtomicBool>,
}

/// Captured follower state and the runtime handles that own its decision-led durable tail.
#[cfg(feature = "cluster")]
struct FollowerDurableTail {
    _in_flight: EpochInFlightGuard,
    coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    state: Arc<FollowerTailState>,
    complete_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    delta_rebase_needed: Arc<std::sync::atomic::AtomicBool>,
    checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    request: crate::checkpoint_coordinator::CheckpointRequest,
    vnode_states: crate::checkpoint_coordinator::StagedVnodeStates,
    fan_out: FxHashMap<String, SourceCheckpoint>,
    local_watermark_ms: Option<i64>,
    attempt: CheckpointAttempt,
    attempt_started: std::time::Instant,
    checkpoint_timeout: Duration,
}

fn set_checkpoint_fault(slot: &parking_lot::Mutex<Option<String>>, reason: impl Into<String>) {
    let mut fault = slot.lock();
    if fault.is_none() {
        *fault = Some(reason.into());
    }
}

async fn deliver_checkpoint_completion(
    tx: &crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    completion: CheckpointCompletion,
    deadline: tokio::time::Instant,
) -> bool {
    matches!(
        tokio::time::timeout_at(deadline, tx.send(completion)).await,
        Ok(Ok(()))
    )
}

async fn deliver_checkpoint_failure(
    tx: &crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    attempt: CheckpointAttempt,
    error: impl Into<String>,
    checkpoint_fault: &parking_lot::Mutex<Option<String>>,
) {
    let error = error.into();
    let deadline = tokio::time::Instant::now() + CHECKPOINT_FAILURE_REPORT_TIMEOUT;
    if !deliver_checkpoint_completion(
        tx,
        CheckpointCompletion::failed(attempt, error.clone()),
        deadline,
    )
    .await
    {
        set_checkpoint_fault(
            checkpoint_fault,
            format!(
                "checkpoint {} epoch {} failed ({error}), but its terminal failure could not be \
                 reported within {:?}",
                attempt.checkpoint_id, attempt.epoch, CHECKPOINT_FAILURE_REPORT_TIMEOUT
            ),
        );
    }
}

#[cfg(feature = "cluster")]
async fn announce_predecision_abort_until(
    controller: Option<&laminar_core::cluster::control::ClusterController>,
    attempt: CheckpointAttempt,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

    let Some(controller) = controller else {
        return Ok(());
    };
    if !controller.is_leader() {
        return Err(format!(
            "leadership was lost before checkpoint {} epoch {} Abort publication",
            attempt.checkpoint_id, attempt.epoch
        ));
    }
    let announcement = BarrierAnnouncement {
        epoch: attempt.epoch,
        checkpoint_id: attempt.checkpoint_id,
        phase: Phase::Abort,
        flags: 0,
        min_watermark_ms: None,
    };
    tokio::time::timeout_at(deadline, controller.announce_barrier(&announcement))
        .await
        .map_err(|_| {
            format!(
                "checkpoint {} epoch {} Abort publication exceeded its cleanup deadline",
                attempt.checkpoint_id, attempt.epoch
            )
        })?
        .map_err(|error| {
            format!(
                "checkpoint {} epoch {} Abort publication failed: {error}",
                attempt.checkpoint_id, attempt.epoch
            )
        })
}

async fn cleanup_reserved_attempt_until(
    coordinator: &tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>,
    attempt: CheckpointAttempt,
    reason: String,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    tokio::time::timeout_at(deadline, async {
        let mut guard = coordinator.lock().await;
        let coordinator = guard.as_mut().ok_or_else(|| {
            format!(
                "checkpoint {} epoch {} has no initialized coordinator for cleanup",
                attempt.checkpoint_id, attempt.epoch
            )
        })?;
        coordinator
            .abandon_epoch_until(attempt.checkpoint_id, attempt.epoch, reason, deadline)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|_| {
        format!(
            "checkpoint {} epoch {} cleanup exceeded its end-to-end deadline",
            attempt.checkpoint_id, attempt.epoch
        )
    })?
}

/// Materialize immutable source snapshots at the durability boundary.
///
/// `SourceCheckpoint` clones share persistent offset trees, so moving this conversion into a
/// blocking worker keeps an `O(N)` file inventory traversal off Tokio workers and the pipeline
/// callback. The resulting map owns the exact bytes persisted in the checkpoint manifest.
fn materialize_source_checkpoint_map(
    checkpoints: FxHashMap<String, SourceCheckpoint>,
) -> HashMap<String, ConnectorCheckpoint> {
    checkpoints
        .into_iter()
        .map(|(name, checkpoint)| {
            (
                name,
                crate::checkpoint_coordinator::source_to_connector_checkpoint(&checkpoint),
            )
        })
        .collect()
}

/// Run source-offset materialization without refreshing the attempt's absolute deadline.
async fn materialize_source_checkpoints_until(
    checkpoints: FxHashMap<String, SourceCheckpoint>,
    attempt: CheckpointAttempt,
    deadline: tokio::time::Instant,
) -> Result<HashMap<String, ConnectorCheckpoint>, String> {
    if tokio::time::Instant::now() >= deadline {
        return Err(format!(
            "checkpoint {} epoch {} exhausted its end-to-end deadline before source-offset \
             materialization",
            attempt.checkpoint_id, attempt.epoch
        ));
    }

    let materialization =
        tokio::task::spawn_blocking(move || materialize_source_checkpoint_map(checkpoints));
    match tokio::time::timeout_at(deadline, materialization).await {
        Ok(Ok(offsets)) => Ok(offsets),
        Ok(Err(error)) => Err(format!(
            "checkpoint {} epoch {} source-offset materialization worker failed: {error}",
            attempt.checkpoint_id, attempt.epoch
        )),
        Err(_) => Err(format!(
            "checkpoint {} epoch {} source-offset materialization exceeded its end-to-end \
             deadline",
            attempt.checkpoint_id, attempt.epoch
        )),
    }
}

/// Fail a leader tail before the coordinator has taken ownership of the attempt.
///
/// Terminal reporting, cluster Abort publication, and exact-attempt abandonment run
/// concurrently. Cleanup therefore starts immediately even when the completion channel or
/// cluster control plane is slow, while every cleanup operation remains bounded by one private
/// runtime-owned deadline.
async fn fail_reserved_leader_attempt(
    tail: &LeaderTail,
    terminal_error: String,
    cleanup_reason: String,
) {
    let attempt = tail.attempt;
    if tail.fault_on_failure {
        set_checkpoint_fault(&tail.checkpoint_fault, terminal_error.clone());
    }
    #[cfg(feature = "cluster")]
    tail.delta_rebase_needed
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let cleanup_deadline = tokio::time::Instant::now() + tail.checkpoint_cleanup_timeout;
    let report = deliver_checkpoint_failure(
        &tail.complete_tx,
        attempt,
        terminal_error,
        &tail.checkpoint_fault,
    );
    let cleanup = cleanup_reserved_attempt_until(
        tail.coordinator.as_ref(),
        attempt,
        cleanup_reason,
        cleanup_deadline,
    );

    #[cfg(feature = "cluster")]
    let ((), abort_result, cleanup_result) = tokio::join!(
        report,
        announce_predecision_abort_until(tail.controller.as_deref(), attempt, cleanup_deadline,),
        cleanup,
    );
    #[cfg(not(feature = "cluster"))]
    let ((), cleanup_result) = tokio::join!(report, cleanup);

    let mut cleanup_errors = Vec::new();
    #[cfg(feature = "cluster")]
    if let Err(error) = abort_result {
        cleanup_errors.push(error);
    }
    if let Err(error) = cleanup_result {
        cleanup_errors.push(error);
    }
    if !cleanup_errors.is_empty() {
        let cleanup_fault = format!(
            "checkpoint {} epoch {} pre-execution cleanup incomplete: {}",
            attempt.checkpoint_id,
            attempt.epoch,
            cleanup_errors.join("; ")
        );
        tracing::error!(%cleanup_fault, "checkpoint cleanup faulted the pipeline");
        set_checkpoint_fault(&tail.checkpoint_fault, cleanup_fault);
    }
}

#[allow(clippy::disallowed_types)]
fn combine_operator_checkpoint_states<I>(
    graph_state: Option<bytes::Bytes>,
    mv_states: I,
) -> std::collections::HashMap<String, bytes::Bytes>
where
    I: IntoIterator<Item = (String, bytes::Bytes)>,
{
    let mut states = std::collections::HashMap::with_capacity(2);
    if let Some(bytes) = graph_state {
        states.insert("operator_graph".to_string(), bytes);
    }
    states.extend(mv_states);
    states
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
            // Cold-only — the demoted groups are on disk; no RAM.
            #[cfg(feature = "state-tier")]
            crate::checkpoint_coordinator::StagedSlice::ColdGroups { .. } => 0,
            // Only the resident base is held in RAM; the demoted groups stream from disk.
            #[cfg(feature = "state-tier")]
            crate::checkpoint_coordinator::StagedSlice::FullWithColdGroups { resident, .. } => {
                resident.len()
            }
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
        SinkContract,
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
    pub(crate) delivery_guarantee: laminar_connectors::connector::DeliveryGuarantee,
    pub(crate) serialization_timeout: Duration,
    /// One semantic deadline spanning sink fence, capture, quorum, and durable publication.
    pub(crate) checkpoint_timeout: Duration,
    /// Runtime-owned budget for Abort publication, coordinator acquisition and sink cleanup.
    pub(crate) checkpoint_cleanup_timeout: Duration,
    pub(crate) sink_event_rx: laminar_core::streaming::AsyncConsumer<crate::sink_task::SinkEvent>,
    /// Set when a best-effort sink write is dropped; suppresses checkpoint admission while the
    /// handle's sticky poison prevents later durable publication.
    pub(crate) sink_timed_out: bool,
    /// Set when an exactly-once sink fails (poisoned epoch); the coordinator polls it via
    /// `take_sink_fault` and faults for recovery so the dropped rows are replayed (CP-4).
    pub(crate) sink_fault: Option<String>,
    /// Fault raised by a spawned checkpoint tail. This is separate from `sink_fault` because
    /// durable decision waits run outside the callback and must never turn uncertainty into Abort.
    pub(crate) checkpoint_fault: Arc<parking_lot::Mutex<Option<String>>>,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    #[cfg(feature = "cluster")]
    pub(crate) cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Frames a peer shuffled to us that never arrived (CL-2). Read before every seal.
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_lost: Option<Arc<std::sync::atomic::AtomicU64>>,
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_lost_seen: u64,
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
    pub(crate) checkpoint_complete_tx:
        crossfire::MAsyncTx<crossfire::mpsc::Array<CheckpointCompletion>>,
    /// Every asynchronous ALO checkpoint tail. `JoinSet` provides structured cancellation and
    /// prevents shutdown from racing detached state/sink work.
    pub(crate) checkpoint_tail_tasks: tokio::task::JoinSet<()>,
    /// In-flight epoch count; the coordinator gates new barriers against `max_in_flight_epochs`.
    pub(crate) checkpoint_in_flight: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) staged_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Set by a checkpoint tail on epoch failure; the next capture consumes it to force a FULL
    /// re-base. Delta checkpoints run with `max_in_flight_epochs == 1` so the flag is seen in time.
    #[cfg(feature = "cluster")]
    pub(crate) delta_rebase_needed: Arc<std::sync::atomic::AtomicBool>,
    /// Lock-free id allocator shared with the coordinator so barrier admission doesn't
    /// queue behind an earlier epoch's durable tail holding the coordinator mutex.
    pub(crate) epoch_allocator: Option<Arc<crate::checkpoint_coordinator::EpochAllocator>>,
    #[cfg(feature = "cluster")]
    pub(crate) quorum_timeout: Duration,
    /// When true, durable tails run inline so post-barrier rows cannot enter an epoch-N open
    /// transaction or staged descriptor.
    pub(crate) checkpoint_committable_sinks: bool,
    pub(crate) state_memory_budget_bytes: Option<usize>,
    pub(crate) state_budget_probe_at: std::time::Instant,
    pub(crate) state_budget_exceeded: bool,
    /// Cold-tier send channel; `None` = no tier configured.
    #[cfg(feature = "state-tier")]
    pub(crate) state_tier: Option<crate::state_tier::TierTx>,
    /// Demote at group granularity rather than whole vnodes.
    #[cfg(feature = "state-tier")]
    pub(crate) state_tier_group_demotion: bool,
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
    fn reap_checkpoint_tail_tasks(&mut self) {
        while let Some(result) = self.checkpoint_tail_tasks.try_join_next() {
            if let Err(error) = result {
                set_checkpoint_fault(
                    &self.checkpoint_fault,
                    format!("checkpoint durable tail terminated unexpectedly: {error}"),
                );
            }
        }
    }

    fn spawn_checkpoint_tail(
        &mut self,
        tail: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        self.reap_checkpoint_tail_tasks();
        self.checkpoint_tail_tasks.spawn(tail);
    }
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
        let attempt = tail.attempt;
        let remaining = tail
            .checkpoint_timeout
            .saturating_sub(tail.attempt_started.elapsed());
        let deadline = tokio::time::Instant::now() + remaining;
        if remaining.is_zero() {
            let error = format!(
                "checkpoint {} epoch {} exhausted its {:?} end-to-end deadline before the durable tail",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            );
            fail_reserved_leader_attempt(&tail, error.clone(), error).await;
            return;
        }

        #[cfg(feature = "cluster")]
        let Some(quorum) = Self::prepare_leader_quorum(&tail, deadline).await
        else {
            return;
        };
        #[cfg(not(feature = "cluster"))]
        let quorum = crate::checkpoint_coordinator::QuorumStage::RunInline;

        Self::execute_leader_tail(tail, quorum, deadline).await;
    }

    #[cfg(feature = "cluster")]
    async fn prepare_leader_quorum(
        tail: &LeaderTail,
        deadline: tokio::time::Instant,
    ) -> Option<crate::checkpoint_coordinator::QuorumStage> {
        use crate::checkpoint_coordinator::{CheckpointCoordinator, QuorumStage};
        use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

        let Some(controller) = tail.controller.as_ref() else {
            return Some(QuorumStage::RunInline);
        };
        let attempt = tail.attempt;
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);
        let quorum_timeout = tail.quorum_timeout.min(
            tail.checkpoint_timeout
                .saturating_sub(tail.attempt_started.elapsed()),
        );
        let quorum_result = tokio::time::timeout_at(
            deadline,
            CheckpointCoordinator::run_prepare_quorum(
                controller,
                quorum_timeout,
                epoch,
                checkpoint_id,
                tail.local_watermark_ms,
            ),
        )
        .await
        .map_err(|_| {
            format!(
                "capture quorum exhausted the {:?} end-to-end checkpoint deadline",
                tail.checkpoint_timeout
            )
        })
        .and_then(|result| result);

        match quorum_result {
            Ok((min_watermark_ms, participants)) => {
                if let Ok(Err(error)) = tokio::time::timeout_at(
                    deadline,
                    controller.announce_barrier(&BarrierAnnouncement {
                        epoch,
                        checkpoint_id,
                        phase: Phase::Aligned,
                        flags: 0,
                        min_watermark_ms,
                    }),
                )
                .await
                {
                    tracing::warn!(
                        epoch, %error,
                        "[LDB-6031] aligned announcement failed; peers resume on Commit"
                    );
                }
                Some(QuorumStage::Done {
                    min_watermark_ms,
                    participants,
                })
            }
            Err(message) => {
                Self::handle_leader_quorum_failure(tail, message).await;
                None
            }
        }
    }

    #[cfg(feature = "cluster")]
    async fn handle_leader_quorum_failure(tail: &LeaderTail, message: String) {
        let attempt = tail.attempt;
        let (epoch, checkpoint_id) = (attempt.epoch, attempt.checkpoint_id);
        tracing::error!(checkpoint_id, epoch, error = %message, "[LDB-6032] quorum miss");
        let terminal_error =
            format!("checkpoint {checkpoint_id} epoch {epoch} quorum failed: {message}");
        fail_reserved_leader_attempt(tail, terminal_error, message).await;
    }

    async fn execute_leader_tail(
        mut tail: LeaderTail,
        quorum: crate::checkpoint_coordinator::QuorumStage,
        deadline: tokio::time::Instant,
    ) {
        let attempt = tail.attempt;
        let source_offsets =
            match materialize_source_checkpoints_until(tail.fan_out.clone(), attempt, deadline)
                .await
            {
                Ok(offsets) => offsets,
                Err(error) => {
                    fail_reserved_leader_attempt(&tail, error.clone(), error).await;
                    return;
                }
            };
        tail.request.source_offset_overrides = source_offsets;

        let remaining = tail
            .checkpoint_timeout
            .saturating_sub(tail.attempt_started.elapsed());
        let Ok(mut guard) = tokio::time::timeout(remaining, tail.coordinator.lock()).await else {
            let error = format!(
                "checkpoint {} epoch {} exceeded its {:?} end-to-end deadline waiting for the coordinator",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            );
            fail_reserved_leader_attempt(&tail, error.clone(), error).await;
            return;
        };

        let Some(coordinator) = guard.as_mut() else {
            drop(guard);
            let error = format!(
                "checkpoint {} epoch {} coordinator disappeared before the durable tail",
                attempt.checkpoint_id, attempt.epoch
            );
            fail_reserved_leader_attempt(&tail, error.clone(), error).await;
            return;
        };
        coordinator.set_pending_vnode_states(std::mem::take(&mut tail.vnode_states));
        coordinator.set_local_watermark_ms(tail.local_watermark_ms);
        let result = coordinator
            .checkpoint_preallocated_started(
                std::mem::take(&mut tail.request),
                attempt,
                quorum,
                tail.attempt_started,
            )
            .await;
        // Completion delivery may wait on a bounded channel; it must not hold
        // the FIFO checkpoint coordinator lock while doing so.
        drop(guard);
        Self::handle_leader_result(&tail, result, deadline).await;
    }

    async fn handle_leader_result(
        tail: &LeaderTail,
        result: Result<crate::checkpoint_coordinator::CheckpointResult, DbError>,
        deadline: tokio::time::Instant,
    ) {
        let attempt = tail.attempt;
        match result {
            Ok(result) if result.success => {
                Self::complete_successful_leader_tail(tail, result, deadline).await;
            }
            Ok(result) => {
                #[cfg(feature = "cluster")]
                tail.delta_rebase_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tracing::warn!(
                    epoch = result.epoch,
                    error = ?result.error,
                    "Barrier-aligned checkpoint failed"
                );
                let terminal_error = format!(
                    "checkpoint {} epoch {} failed: {}",
                    result.checkpoint_id,
                    result.epoch,
                    result
                        .error
                        .as_deref()
                        .unwrap_or("unknown checkpoint failure")
                );
                if tail.fault_on_failure {
                    set_checkpoint_fault(&tail.checkpoint_fault, terminal_error.clone());
                }
                deliver_checkpoint_failure(
                    &tail.complete_tx,
                    attempt,
                    terminal_error,
                    &tail.checkpoint_fault,
                )
                .await;
            }
            Err(error) => {
                #[cfg(feature = "cluster")]
                tail.delta_rebase_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tracing::warn!(%error, "Barrier-aligned checkpoint error");
                let terminal_error = error.to_string();
                if tail.fault_on_failure {
                    set_checkpoint_fault(&tail.checkpoint_fault, terminal_error.clone());
                }
                deliver_checkpoint_failure(
                    &tail.complete_tx,
                    attempt,
                    terminal_error,
                    &tail.checkpoint_fault,
                )
                .await;
            }
        }
    }

    async fn complete_successful_leader_tail(
        tail: &LeaderTail,
        result: crate::checkpoint_coordinator::CheckpointResult,
        deadline: tokio::time::Instant,
    ) {
        let continuation_error = result.continuation_error().map(str::to_owned);
        match CheckpointCompletion::validated(tail.attempt, result, tail.fan_out.clone()) {
            Ok(completion) => {
                if !deliver_checkpoint_completion(&tail.complete_tx, completion, deadline).await {
                    set_checkpoint_fault(
                        &tail.checkpoint_fault,
                        format!(
                            "checkpoint {} epoch {} committed but its completion missed the \
                             end-to-end deadline",
                            tail.attempt.checkpoint_id, tail.attempt.epoch
                        ),
                    );
                    return;
                }
                // Completion is enqueued first so the durable source cut is acknowledged before a
                // successor-epoch continuation fault fences further writes.
                if let Some(error) = continuation_error {
                    set_checkpoint_fault(&tail.checkpoint_fault, error);
                }
            }
            Err(reason) => {
                tracing::error!(
                    error = %reason,
                    "[LDB-6048] refusing mismatched checkpoint completion"
                );
                set_checkpoint_fault(&tail.checkpoint_fault, reason.clone());
                deliver_checkpoint_failure(
                    &tail.complete_tx,
                    tail.attempt,
                    reason,
                    &tail.checkpoint_fault,
                )
                .await;
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
        attempt_started: std::time::Instant,
    ) -> Result<impl std::future::Future<Output = ()> + Send + 'static, String> {
        let vnode_states = self.capture_vnode_states()?;
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        let local_watermark_ms = if wm == i64::MIN { None } else { Some(wm) };
        self.follower_tail.begin(epoch);

        // Charge followers too; otherwise their capture-to-upload memory is unaccounted.
        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.staged_bytes,
            staged_request_bytes(&request, &vnode_states),
        );
        let tail = FollowerDurableTail {
            _in_flight: in_flight,
            coordinator: Arc::clone(&self.coordinator),
            state: Arc::clone(&self.follower_tail),
            complete_tx: self.checkpoint_complete_tx.clone(),
            controller: self.cluster_controller.clone(),
            delta_rebase_needed: Arc::clone(&self.delta_rebase_needed),
            checkpoint_fault: Arc::clone(&self.checkpoint_fault),
            request,
            vnode_states,
            fan_out,
            local_watermark_ms,
            attempt: CheckpointAttempt::new(epoch, checkpoint_id),
            attempt_started,
            checkpoint_timeout: self.checkpoint_timeout,
        };
        Ok(Self::run_follower_tail(tail))
    }

    #[cfg(feature = "cluster")]
    async fn run_follower_tail(mut tail: FollowerDurableTail) {
        let remaining = tail
            .checkpoint_timeout
            .saturating_sub(tail.attempt_started.elapsed());
        let deadline = tokio::time::Instant::now() + remaining;

        // The immutable source snapshot is already captured, so this acknowledgement may release
        // `Aligned`. The leader's exact-attempt restorable gate still requires this participant's
        // partial before a decision and therefore fences the materialization/prepare tail below.
        Self::acknowledge_follower_capture(&tail, deadline).await;
        let source_offsets = match materialize_source_checkpoints_until(
            tail.fan_out.clone(),
            tail.attempt,
            deadline,
        )
        .await
        {
            Ok(offsets) => offsets,
            Err(error) => {
                Self::reject_follower_capture(
                    tail.controller.as_deref(),
                    tail.checkpoint_fault.as_ref(),
                    tail.attempt,
                    error,
                    deadline,
                )
                .await;
                tail.state.finish(tail.attempt.epoch, false);
                tail.delta_rebase_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return;
            }
        };
        tail.request.source_offset_overrides = source_offsets;

        let prepared = Self::prepare_follower_tail_until(&mut tail, deadline).await;
        // Decision observation deliberately happens outside the coordinator mutex so a successor
        // epoch can prepare without queuing behind this attempt's control-plane wait.
        let outcome = Self::apply_follower_decision_until(&tail, prepared, deadline).await;
        Self::complete_follower_tail(tail, outcome, deadline).await;
    }

    #[cfg(feature = "cluster")]
    async fn acknowledge_follower_capture(
        tail: &FollowerDurableTail,
        deadline: tokio::time::Instant,
    ) {
        let Some(controller) = tail.controller.as_ref() else {
            return;
        };
        let _ = tokio::time::timeout_at(
            deadline,
            controller.ack_barrier(&laminar_core::cluster::control::BarrierAck {
                epoch: tail.attempt.epoch,
                ok: true,
                error: None,
                local_watermark_ms: tail.local_watermark_ms,
            }),
        )
        .await;
    }

    #[cfg(feature = "cluster")]
    async fn prepare_follower_tail_until(
        tail: &mut FollowerDurableTail,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>, DbError>
    {
        let request = std::mem::take(&mut tail.request);
        let vnode_states = std::mem::take(&mut tail.vnode_states);
        let attempt = tail.attempt;
        let result = tokio::time::timeout_at(deadline, async {
            let mut guard = tail.coordinator.lock().await;
            let coordinator = guard.as_mut().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6045] checkpoint coordinator disappeared before follower prepare".into(),
                )
            })?;
            coordinator.set_pending_vnode_states(vnode_states);
            coordinator.set_local_watermark_ms(tail.local_watermark_ms);
            coordinator
                .follower_prepare_acked_until(
                    request,
                    attempt.epoch,
                    attempt.checkpoint_id,
                    deadline,
                )
                .await?;
            Ok::<_, DbError>(coordinator.decision_store_handle())
        })
        .await;

        result.unwrap_or_else(|_| {
            Err(DbError::Checkpoint(format!(
                "[LDB-6046] follower checkpoint {} epoch {} exceeded its {:?} end-to-end \
                 deadline during prepare",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            )))
        })
    }

    #[cfg(feature = "cluster")]
    async fn apply_follower_decision_until(
        tail: &FollowerDurableTail,
        prepared: Result<
            Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
            DbError,
        >,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        use crate::checkpoint_coordinator::CheckpointCoordinator;

        let decision_store = prepared?.ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6045] follower durable tail lost its decision dependencies".into(),
            )
        })?;
        let controller = tail.controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6045] follower durable tail lost its decision dependencies".into(),
            )
        })?;
        let attempt = tail.attempt;
        let decision_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        let verdict = CheckpointCoordinator::await_follower_decision(
            controller,
            Some(decision_store.as_ref()),
            attempt.epoch,
            attempt.checkpoint_id,
            decision_timeout,
        )
        .await?;

        tokio::time::timeout_at(deadline, async {
            let mut guard = tail.coordinator.lock().await;
            let coordinator = guard.as_mut().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6045] checkpoint coordinator disappeared while a follower decision \
                     was pending"
                        .into(),
                )
            })?;
            coordinator
                .follower_finish(attempt.epoch, attempt.checkpoint_id, verdict)
                .await
        })
        .await
        .unwrap_or_else(|_| {
            Err(DbError::Checkpoint(format!(
                "[LDB-6046] follower checkpoint {} epoch {} exceeded its {:?} end-to-end \
                 deadline while applying the decision",
                attempt.checkpoint_id, attempt.epoch, tail.checkpoint_timeout
            )))
        })
    }

    #[cfg(feature = "cluster")]
    async fn complete_follower_tail(
        tail: FollowerDurableTail,
        outcome: Result<bool, DbError>,
        deadline: tokio::time::Instant,
    ) {
        let attempt = tail.attempt;
        let committed = match outcome {
            Ok(true) => {
                tracing::info!(epoch = attempt.epoch, "follower checkpoint committed");
                true
            }
            Ok(false) => {
                tracing::warn!(
                    epoch = attempt.epoch,
                    "follower checkpoint aborted by leader"
                );
                false
            }
            Err(error) => {
                tracing::error!(
                    epoch = attempt.epoch,
                    checkpoint_id = attempt.checkpoint_id,
                    %error,
                    "follower checkpoint is in-doubt; faulting pipeline",
                );
                set_checkpoint_fault(&tail.checkpoint_fault, error.to_string());
                false
            }
        };

        tail.state.finish(attempt.epoch, committed);
        if committed {
            let completion = CheckpointCompletion::new(attempt, tail.fan_out);
            if !deliver_checkpoint_completion(&tail.complete_tx, completion, deadline).await {
                set_checkpoint_fault(
                    &tail.checkpoint_fault,
                    format!(
                        "follower checkpoint {} epoch {} committed but its completion could not \
                         be delivered before the end-to-end deadline",
                        attempt.checkpoint_id, attempt.epoch
                    ),
                );
            }
        } else {
            // Follower capture is destructive; every uncommitted outcome must re-base FULL next.
            tail.delta_rebase_needed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Reject a follower capture or pre-prepare step under the attempt deadline.
    ///
    /// When capture readiness was already acknowledged, the negative acknowledgement retracts it.
    /// If quorum raced ahead, the exact-attempt restorable gate still refuses to seal without this
    /// participant's partial.
    #[cfg(feature = "cluster")]
    async fn reject_follower_capture(
        controller: Option<&laminar_core::cluster::control::ClusterController>,
        checkpoint_fault: &parking_lot::Mutex<Option<String>>,
        attempt: CheckpointAttempt,
        error: String,
        deadline: tokio::time::Instant,
    ) {
        tracing::warn!(
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            error = %error,
            "follower checkpoint capture failed; rejecting attempt"
        );
        let Some(controller) = controller else {
            set_checkpoint_fault(
                checkpoint_fault,
                format!(
                    "checkpoint {} epoch {} capture failed without a cluster controller to \
                     publish its negative acknowledgement",
                    attempt.checkpoint_id, attempt.epoch
                ),
            );
            return;
        };
        let rejection = laminar_core::cluster::control::BarrierAck {
            epoch: attempt.epoch,
            ok: false,
            error: Some(error),
            local_watermark_ms: None,
        };
        let acknowledgement = controller.ack_barrier(&rejection);
        match tokio::time::timeout_at(deadline, acknowledgement).await {
            Ok(Ok(())) => {}
            Ok(Err(ack_error)) => set_checkpoint_fault(
                checkpoint_fault,
                format!(
                    "checkpoint {} epoch {} capture failed and its negative acknowledgement \
                     could not be published: {ack_error}",
                    attempt.checkpoint_id, attempt.epoch
                ),
            ),
            Err(_) => set_checkpoint_fault(
                checkpoint_fault,
                format!(
                    "checkpoint {} epoch {} capture failed and its negative acknowledgement \
                     missed the end-to-end attempt deadline",
                    attempt.checkpoint_id, attempt.epoch
                ),
            ),
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
        quorum_timeout: std::time::Duration,
    ) {
        use laminar_core::cluster::control::Phase;

        // The gate must outlast the leader's quorum wait: a slow-but-successful alignment that lands
        // `Aligned` AFTER the follower resumes would let it fold epoch-N+1 shuffle rows into a peer
        // still capturing epoch-N. Derive the gate from quorum_timeout (default 3s → 10s) so a
        // user-raised quorum_timeout can never invert the gate > quorum relation (CL-6).
        let resume_gate_timeout = std::time::Duration::from_secs(10)
            .max(quorum_timeout + std::time::Duration::from_secs(5));

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
                resume_gate_timeout,
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

    /// Observe and inject a leader `Prepare`, returning only attempts ready for immediate capture.
    #[cfg(feature = "cluster")]
    async fn admit_follower_prepare(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
    ) -> Option<laminar_core::cluster::control::BarrierAnnouncement> {
        use laminar_core::cluster::control::Phase;

        let announcement = match controller.observe_barrier().await {
            Ok(Some(announcement)) if announcement.phase == Phase::Prepare => announcement,
            _ => return None,
        };
        let pending_epoch = self
            .pending_follower_checkpoint
            .as_ref()
            .map(|pending| pending.epoch);
        if Self::follower_should_skip(
            self.follower_tail.committed(),
            pending_epoch,
            self.follower_tail.in_flight(),
            announcement.epoch,
        ) {
            return None;
        }

        if self
            .barrier_injectors
            .iter()
            .any(|(_, injector)| !injector.can_trigger())
        {
            tracing::debug!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                "follower barrier injection deferred while a prior command is pending"
            );
            return None;
        }

        let barrier = laminar_core::checkpoint::CheckpointBarrier {
            checkpoint_id: announcement.checkpoint_id,
            epoch: announcement.epoch,
            flags: announcement.flags,
        };
        for (name, injector) in &self.barrier_injectors {
            tracing::debug!(
                source = %name,
                checkpoint_id = announcement.checkpoint_id,
                "follower injecting checkpoint barrier"
            );
            if !injector.trigger(barrier) {
                set_checkpoint_fault(
                    &self.checkpoint_fault,
                    format!(
                        "follower rejected source barrier for checkpoint {} epoch {}",
                        announcement.checkpoint_id, announcement.epoch
                    ),
                );
                return None;
            }
        }

        if self.barrier_injectors.is_empty() {
            return Some(announcement);
        }
        tracing::info!(
            checkpoint_id = announcement.checkpoint_id,
            epoch = announcement.epoch,
            "follower deferring checkpoint alignment until source barriers flow through"
        );
        self.pending_follower_checkpoint = Some(announcement);
        None
    }

    /// Align the follower's shuffle contribution under the attempt's absolute deadline.
    #[cfg(feature = "cluster")]
    async fn align_follower_shuffle_until(
        &mut self,
        controller: &laminar_core::cluster::control::ClusterController,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let live: Vec<u64> = controller
            .live_instances()
            .iter()
            .map(|node| node.0)
            .collect();
        let watermark = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        tokio::time::timeout_at(
            deadline,
            self.graph
                .align_shuffle_barriers(checkpoint_id, watermark, &live, Some(controller)),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "follower shuffle alignment exhausted the {:?} end-to-end checkpoint deadline",
                self.checkpoint_timeout
            ))
        })?
    }

    #[cfg(feature = "cluster")]
    async fn maybe_follower_checkpoint(
        &mut self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64> {
        let ann = self.admit_follower_prepare(&controller).await?;

        // The histogram timer records on drop, including every early-return failure below.
        // Alignment and capture stop the pipeline just as surely as the durable tail does.
        let attempt_started = std::time::Instant::now();
        let attempt_deadline = tokio::time::Instant::now() + self.checkpoint_timeout;
        let _stall_timer = self.prom.checkpoint_pipeline_stall_duration.start_timer();

        if let Err(error) = self
            .align_follower_shuffle_until(&controller, ann.checkpoint_id, attempt_deadline)
            .await
        {
            tracing::warn!(%error, "follower shuffle alignment failed — skipping");
            return None;
        }

        // Alignment above is where a peer's barrier reveals trailing loss; capturing now would
        // hand the leader a gapped snapshot to seal. Leave the flag for `take_sink_fault`.
        self.check_shuffle_loss();
        if self.sink_fault.is_some() {
            tracing::warn!("follower: shuffle loss before capture; failing the epoch for replay");
            return None;
        }

        let request = self.build_checkpoint_request(std::collections::HashMap::new());

        let epoch = ann.epoch;
        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let tail = match self.follower_tail_future(
            request,
            ann.epoch,
            ann.checkpoint_id,
            source_offsets,
            attempt_started,
        ) {
            Ok(tail) => tail,
            Err(error) => {
                Self::reject_follower_capture(
                    Some(controller.as_ref()),
                    self.checkpoint_fault.as_ref(),
                    CheckpointAttempt::new(ann.epoch, ann.checkpoint_id),
                    error,
                    attempt_deadline,
                )
                .await;
                return None;
            }
        };
        if self.checkpoint_committable_sinks {
            tail.await;
        } else {
            self.spawn_checkpoint_tail(tail);
            let _ = tokio::time::timeout_at(
                attempt_deadline,
                Self::wait_for_aligned_resume(has_shuffle, &controller, epoch, self.quorum_timeout),
            )
            .await;
        }
        None
    }

    #[cfg(feature = "cluster")]
    async fn run_follower_checkpoint_deferred(
        &mut self,
        ann: laminar_core::cluster::control::BarrierAnnouncement,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt_started: std::time::Instant,
    ) -> crate::pipeline::BarrierOutcome {
        use crate::pipeline::BarrierOutcome;

        let Some(controller) = self.cluster_controller.clone() else {
            return BarrierOutcome::Failed;
        };

        // Record the complete pipeline pause. The timer observes on drop so alignment or
        // serialization failures are visible instead of disappearing from latency telemetry.
        let attempt_deadline =
            tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;
        let _stall_timer = self.prom.checkpoint_pipeline_stall_duration.start_timer();

        // Drain + align the cross-node shuffle on the leader's checkpoint id so
        // the snapshot includes peers' pre-checkpoint rows.
        if let Err(error) = self
            .align_follower_shuffle_until(&controller, ann.checkpoint_id, attempt_deadline)
            .await
        {
            tracing::warn!(%error, "follower shuffle alignment failed — skipping");
            return BarrierOutcome::Failed;
        }

        // Same pre-seal fence as the leader: the follower's captured state is what the leader's
        // durability gate seals, so a gap here is sealed cluster-wide (CL-2).
        self.check_shuffle_loss();
        if self.sink_fault.is_some() {
            tracing::warn!("follower: shuffle loss before capture; failing the epoch for replay");
            return BarrierOutcome::Failed;
        }

        let operator_states = match tokio::time::timeout_at(
            attempt_deadline,
            self.capture_and_serialize_operator_state(),
        )
        .await
        {
            Ok(Ok(states)) => states,
            Ok(Err(error)) => {
                tracing::warn!(%error, "follower deferred checkpoint: operator state capture failed");
                return BarrierOutcome::Failed;
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?self.checkpoint_timeout,
                    "follower state capture exhausted the checkpoint deadline"
                );
                return BarrierOutcome::Failed;
            }
        };
        let request = self.build_checkpoint_request(operator_states);

        let epoch = ann.epoch;
        let has_shuffle = self.graph.cluster_shuffle_config().is_some();
        let tail = match self.follower_tail_future(
            request,
            ann.epoch,
            ann.checkpoint_id,
            source_checkpoints,
            attempt_started,
        ) {
            Ok(tail) => tail,
            Err(error) => {
                Self::reject_follower_capture(
                    Some(controller.as_ref()),
                    self.checkpoint_fault.as_ref(),
                    CheckpointAttempt::new(ann.epoch, ann.checkpoint_id),
                    error,
                    attempt_deadline,
                )
                .await;
                return BarrierOutcome::Failed;
            }
        };
        if self.checkpoint_committable_sinks {
            tail.await;
        } else {
            self.spawn_checkpoint_tail(tail);
            let _ = tokio::time::timeout_at(
                attempt_deadline,
                Self::wait_for_aligned_resume(has_shuffle, &controller, epoch, self.quorum_timeout),
            )
            .await;
        }
        BarrierOutcome::Async
    }

    /// Reserve one durable attempt before any source or shuffle barrier is admitted.
    async fn reserve_attempt(
        &mut self,
        attempt_started: std::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let allocator = self.epoch_allocator.clone().ok_or_else(|| {
            DbError::Checkpoint("checkpoint attempt allocator is not initialized".into())
        })?;
        let deadline = tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;

        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            if !cc.is_leader() {
                return Err(DbError::Checkpoint(
                    "only the cluster leader may reserve checkpoint attempts".into(),
                ));
            }
            // A reclaiming leader can lag an in-flight announced epoch. Checkpoint IDs come from
            // the shared durable allocator; only the execution epoch needs a local high-watermark.
            let max_announced = tokio::time::timeout_at(deadline, cc.max_announced_epoch())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "checkpoint admission exhausted its {:?} end-to-end deadline while \
                         reading the cluster epoch high-watermark",
                        self.checkpoint_timeout
                    ))
                })?;
            if let Some((max_epoch, _)) = max_announced {
                allocator.advance_epoch_to(max_epoch.saturating_add(1));
            }
        }

        let attempt = allocator.allocate_until(deadline).await?;

        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            use laminar_core::cluster::control::{BarrierAnnouncement, Phase};
            let announcement = BarrierAnnouncement {
                epoch: attempt.epoch,
                checkpoint_id: attempt.checkpoint_id,
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            };
            match tokio::time::timeout_at(deadline, cc.announce_barrier(&announcement)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.abandon_reserved_attempt(
                        attempt,
                        format!("prepare announcement failed: {error}"),
                    )
                    .await;
                    return Err(DbError::Checkpoint(format!(
                        "prepare announcement failed for checkpoint {} epoch {}: {error}",
                        attempt.checkpoint_id, attempt.epoch
                    )));
                }
                Err(_) => {
                    // The publication may have landed even though its acknowledgement timed out.
                    // Abandon under a fresh cleanup budget so followers cannot remain prepared
                    // and the local sink epoch cannot remain open.
                    self.abandon_reserved_attempt(
                        attempt,
                        "prepare announcement acknowledgement exceeded the admission deadline"
                            .into(),
                    )
                    .await;
                    return Err(DbError::Checkpoint(format!(
                        "prepare announcement for checkpoint {} epoch {} exhausted its {:?} \
                         end-to-end admission deadline",
                        attempt.checkpoint_id, attempt.epoch, self.checkpoint_timeout
                    )));
                }
            }
        }

        Ok(attempt)
    }

    async fn abandon_reserved_attempt(&mut self, attempt: CheckpointAttempt, reason: String) {
        let deadline = tokio::time::Instant::now() + self.checkpoint_cleanup_timeout;
        let mut cleanup_errors = Vec::new();
        // Publish Abort before contending on the coordinator lock. A durable Prepare may already
        // be visible even when its acknowledgement failed, and followers must not wait behind a
        // busy local coordinator to learn that this exact attempt is terminal.
        #[cfg(feature = "cluster")]
        if let Err(error) =
            announce_predecision_abort_until(self.cluster_controller.as_deref(), attempt, deadline)
                .await
        {
            cleanup_errors.push(error);
        }
        if let Err(error) =
            cleanup_reserved_attempt_until(self.coordinator.as_ref(), attempt, reason, deadline)
                .await
        {
            cleanup_errors.push(error);
        }
        if !cleanup_errors.is_empty() {
            let error = format!(
                "checkpoint {} epoch {} abandonment incomplete: {}",
                attempt.checkpoint_id,
                attempt.epoch,
                cleanup_errors.join("; ")
            );
            tracing::error!(%error, "checkpoint cleanup faulted the pipeline");
            set_checkpoint_fault(&self.checkpoint_fault, error);
        }
    }

    /// Align the cross-node shuffle for an already announced exact attempt.
    #[cfg(feature = "cluster")]
    async fn align_shuffle_for_leader(
        &mut self,
        attempt: CheckpointAttempt,
    ) -> Result<(), DbError> {
        // `state-tier` enables the cluster compile feature, but embedded and single-node
        // runtimes deliberately have no controller. They have no cross-node shuffle to align.
        let Some(cc) = self.cluster_controller.clone() else {
            return Ok(());
        };
        if !cc.is_leader() {
            return Err(DbError::Checkpoint(
                "only the cluster leader may align checkpoint shuffles".into(),
            ));
        }
        let live: Vec<u64> = cc.live_instances().iter().map(|n| n.0).collect();
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        self.graph
            .align_shuffle_barriers(attempt.checkpoint_id, wm, &live, Some(&*cc))
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "shuffle alignment failed for checkpoint {} epoch {}: {error}",
                    attempt.checkpoint_id, attempt.epoch
                ))
            })
    }

    /// Build the callback-owned portion of a `CheckpointRequest`.
    ///
    /// Source offset overrides remain empty here. The immutable source snapshot is already owned
    /// by the durable tail, which materializes it on a blocking worker after the pipeline resumes.
    fn build_checkpoint_request(
        &self,
        operator_states: std::collections::HashMap<String, bytes::Bytes>,
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
        crate::checkpoint_coordinator::CheckpointRequest {
            operator_states,
            watermark: None,
            table_store_checkpoint_path: None,
            extra_table_offsets: extra_tables,
            source_watermarks: per_source_watermarks,
            source_offset_overrides: HashMap::new(),
        }
    }

    async fn capture_and_serialize_operator_state(
        &mut self,
    ) -> Result<std::collections::HashMap<String, bytes::Bytes>, String> {
        let graph_state = match self.graph.snapshot_state() {
            Ok(Some(cp)) => {
                let timeout = self.serialization_timeout;
                let bytes = tokio::time::timeout(
                    timeout,
                    tokio::task::spawn_blocking(move || {
                        crate::operator_graph::OperatorGraph::serialize_checkpoint(&cp)
                    }),
                )
                .await
                .map_err(|_| {
                    format!("[LDB-6017] operator state serialization timed out ({timeout:?})")
                })?
                .map_err(|e| format!("serialize join error: {e}"))?
                .map_err(|e| format!("serialize error: {e}"))?;
                Some(bytes::Bytes::from(bytes))
            }
            Ok(None) => None,
            Err(e) => return Err(format!("snapshot failed: {e}")),
        };

        let mv_states = self
            .mv_store
            .read()
            .checkpoint_states()
            .map_err(|e| format!("MV checkpoint failed: {e}"))?;
        Ok(combine_operator_checkpoint_states(graph_state, mv_states))
    }

    /// Capture per-vnode operator state for the in-flight checkpoint.
    ///
    /// Empty outside cluster mode. Cluster capture failures are fatal to the attempt: an empty
    /// map is a valid snapshot for a stateless graph and must not also encode capture failure.
    #[allow(clippy::unused_self, clippy::disallowed_types)] // matches the coordinator/graph map shape
    #[cfg_attr(
        not(feature = "cluster"),
        allow(clippy::unnecessary_wraps) // one cross-feature API; cluster capture is fallible
    )]
    fn capture_vnode_states(
        &mut self,
    ) -> Result<crate::checkpoint_coordinator::StagedVnodeStates, String> {
        #[cfg(feature = "cluster")]
        {
            // A prior epoch failed post-capture; re-base FULL so no delta chain outruns the parent.
            if self
                .delta_rebase_needed
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.graph.force_full_rebase();
            }
            self.graph.snapshot_state_by_vnode().map_err(|e| {
                self.delta_rebase_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                format!("per-vnode state snapshot failed: {e}")
            })
        }
        #[cfg(not(feature = "cluster"))]
        {
            Ok(std::collections::HashMap::new())
        }
    }

    /// Sync all sinks and drain their events; `sink_timed_out` is current after this returns.
    /// A failed fence aborts this checkpoint: sealing offsets while queued writes are unknown
    /// would violate both ALO and EO delivery.
    async fn sync_sinks_and_drain_events(
        &mut self,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let sync_futures = self.sinks.iter().map(|(name, handle, _, _, _)| {
            let name = name.clone();
            let handle = handle.clone();
            async move { (name, handle.sync_until(attempt_deadline).await) }
        });
        let results = futures::future::join_all(sync_futures).await;
        self.drain_sink_events();
        let failures: Vec<String> = results
            .into_iter()
            .filter_map(|(name, result)| {
                result
                    .err()
                    .map(|error| format!("sink '{name}' sync barrier failed: {error}"))
            })
            .collect();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// Cluster pipelines always fault (rather than drop) on fatal errors: coordinated
    /// recovery replays them, and a swallowed fault would desync the cross-node cut.
    #[cfg_attr(not(feature = "cluster"), allow(clippy::unused_self))]
    fn in_cluster(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            self.cluster_controller.is_some()
        }
        #[cfg(not(feature = "cluster"))]
        {
            false
        }
    }

    /// A shuffle frame a peer sent never arrived, so this node's state is missing records that
    /// only exist upstream. Sealing here would commit the gap permanently — the rewind target
    /// would sit at or above the corrupt epoch. Fault instead: the round rewinds to the last
    /// committed cut and the replay regenerates them (CL-2).
    #[cfg(feature = "cluster")]
    fn check_shuffle_loss(&mut self) {
        let Some(ref counter) = self.shuffle_lost else {
            return;
        };
        let lost = counter.load(std::sync::atomic::Ordering::Acquire);
        if lost > self.shuffle_lost_seen {
            let missing = lost - self.shuffle_lost_seen;
            self.shuffle_lost_seen = lost;
            self.prom.shuffle_frames_lost_total.inc_by(missing);
            self.sink_fault.get_or_insert_with(|| {
                format!(
                    "{missing} shuffle frame(s) lost in transit; replaying from the last checkpoint"
                )
            });
        }
    }

    #[cfg(not(feature = "cluster"))]
    #[allow(clippy::unused_self)]
    fn check_shuffle_loss(&mut self) {}

    fn record_dropped_sink_write(&mut self, reason: String) {
        let requires_replay = self.checkpoint_committable_sinks
            || self.delivery_guarantee
                != laminar_connectors::connector::DeliveryGuarantee::BestEffort
            || self.in_cluster();
        if requires_replay {
            self.sink_fault.get_or_insert(reason);
        } else {
            self.sink_timed_out = true;
        }
    }

    fn drain_sink_events(&mut self) {
        self.check_shuffle_loss();
        while let Ok(event) = self.sink_event_rx.try_recv() {
            tracing::debug!(?event, "sink event");
            let reason = match &event {
                crate::sink_task::SinkEvent::FlushError {
                    sink_id,
                    epoch,
                    operation,
                    error,
                } => {
                    self.prom.sink_write_failures.inc();
                    format!("sink '{sink_id}' {operation} failed at epoch {epoch}: {error}")
                }
                crate::sink_task::SinkEvent::WriteError {
                    sink_id,
                    epoch,
                    rows,
                    error,
                } => {
                    self.prom.sink_write_failures.inc();
                    format!(
                        "sink '{sink_id}' write error for {rows} rows at epoch {epoch}: {error}"
                    )
                }
                crate::sink_task::SinkEvent::WriteTimeout {
                    sink_id,
                    epoch,
                    rows,
                    timeout,
                } => {
                    self.prom.sink_write_timeouts.inc();
                    format!(
                        "sink '{sink_id}' write timeout for {rows} rows at epoch {epoch} \
                         after {timeout:?}"
                    )
                }
                crate::sink_task::SinkEvent::WriteEnqueueTimeout {
                    sink_id,
                    rows,
                    timeout,
                } => {
                    self.prom.sink_write_timeouts.inc();
                    format!(
                        "sink '{sink_id}' write enqueue timeout for {rows} rows after {timeout:?}"
                    )
                }
                crate::sink_task::SinkEvent::ChannelClosed { sink_id } => {
                    self.prom.sink_task_channel_closed.inc();
                    format!("sink '{sink_id}' task channel closed")
                }
            };
            // Any replay-guaranteed mode must fault so recovery replays the dropped rows.
            // Best-effort mode may continue, but the handle's sticky poison still prevents a
            // later checkpoint from claiming the dropped batch.
            self.record_dropped_sink_write(reason);
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
    ) -> Result<crate::pipeline::CycleOutcome, crate::pipeline::CycleError> {
        // Test-only one-shot fault injector for the recovery soak (inert in release / when
        // unset). The harness creates a per-process trigger file only after the cluster has
        // reached steady state and it has observed the node's actual leader/follower role.
        #[cfg(all(debug_assertions, feature = "cluster"))]
        {
            use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
            use std::sync::OnceLock;
            use std::time::Instant;
            static TRIGGER_FILE: OnceLock<Option<std::path::PathBuf>> = OnceLock::new();
            static POLL_START: OnceLock<Instant> = OnceLock::new();
            static NEXT_POLL_MS: AtomicU64 = AtomicU64::new(0);
            static FIRED: AtomicBool = AtomicBool::new(false);
            if let Some(path) = TRIGGER_FILE
                .get_or_init(|| {
                    std::env::var_os("LAMINAR_FAULT_INJECT_TRIGGER_FILE").map(Into::into)
                })
                .as_ref()
            {
                let now_ms =
                    u64::try_from(POLL_START.get_or_init(Instant::now).elapsed().as_millis())
                        .unwrap_or(u64::MAX);
                let next_poll = NEXT_POLL_MS.load(Ordering::Relaxed);
                if now_ms >= next_poll
                    && NEXT_POLL_MS
                        .compare_exchange(
                            next_poll,
                            now_ms.saturating_add(25),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                {
                    let requested_role = std::fs::read_to_string(path).ok();
                    let role_matches = requested_role.as_deref().is_some_and(|role| {
                        self.cluster_controller.as_ref().is_some_and(|controller| {
                            match role.trim() {
                                "leader" => controller.is_leader(),
                                "follower" => {
                                    !controller.is_leader() && controller.current_leader().is_some()
                                }
                                _ => false,
                            }
                        })
                    });
                    if role_matches
                        && !FIRED.load(Ordering::Relaxed)
                        && FIRED
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                            .is_ok()
                    {
                        if std::fs::remove_file(path).is_ok() {
                            return Err(crate::pipeline::CycleError::Fatal(
                                "injected fault for coordinated-recovery soak \
                                 (LAMINAR_FAULT_INJECT_TRIGGER_FILE)"
                                    .into(),
                            ));
                        }
                        FIRED.store(false, Ordering::Release);
                    }
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
        let results = self
            .graph
            .execute_cycle(source_batches, watermark, swm_ref)
            .await
            .map_err(|e| Self::map_graph_error(&e, &self.shutdown_signal))?;
        let (any_failed, failed_sources) = self.graph.take_cycle_failures();
        Ok(crate::pipeline::CycleOutcome {
            results,
            any_failed,
            failed_sources,
        })
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
        // Snapshot broadcast is deferred past the write lock (rematerialize is O(rows) and would
        // otherwise block SELECT readers on the store-wide lock).
        let mut changelog_broadcasts: Vec<Arc<str>> = Vec::new();
        for (stream_name, batches) in results {
            if !store.has_mv(stream_name) {
                continue;
            }
            // A changelog MV (batches carry `__weight`) must not put the raw Z-set changelog on the
            // SUBSCRIBE wire — subscribers get plain rows. Apply to the store, then broadcast the
            // consolidated snapshot instead (only when someone is listening). A full-emit MV keeps
            // forwarding its per-cycle batch verbatim (that batch already IS its full state).
            let changelog = batches.iter().any(|b| {
                b.num_rows() > 0
                    && b.schema()
                        .index_of(laminar_core::changelog::WEIGHT_COLUMN)
                        .is_ok()
            });
            // Apply the whole cycle's output in one call: an Aggregate-mode MV replaces its
            // result set per cycle, so a per-batch update would keep only the last chunk of a
            // multi-batch (>8192-row) output (EX-1).
            #[allow(clippy::cast_possible_truncation)]
            let row_batches = batches.iter().filter(|b| b.num_rows() > 0).count() as u64;
            if row_batches > 0 {
                store.update_cycle(stream_name, batches);
                updates += row_batches;
                if !changelog {
                    for batch in batches {
                        if batch.num_rows() > 0 {
                            self.subscription_registry
                                .send_batch(stream_name, batch.clone());
                        }
                    }
                }
            }
            if changelog && self.subscription_registry.subscriber_count(stream_name) > 0 {
                changelog_broadcasts.push(Arc::clone(stream_name));
            }
        }
        if updates > 0 {
            self.prom.mv_updates.inc_by(updates);
            #[allow(clippy::cast_possible_truncation)]
            let bytes = store.total_bytes() as u64;
            #[allow(clippy::cast_possible_wrap)]
            self.prom.mv_bytes_stored.set(bytes as i64);
        }
        drop(store);

        if !changelog_broadcasts.is_empty() {
            let store = self.mv_store.read();
            for stream_name in changelog_broadcasts {
                match store.to_record_batch(&stream_name) {
                    Ok(Some(snap)) => self.subscription_registry.send_batch(&stream_name, snap),
                    Ok(None) => {}
                    // On the pipeline task with no caller to fail; log rather than silently skip.
                    Err(e) => tracing::error!(
                        mv = %stream_name, error = %e,
                        "MV snapshot for subscribers failed"
                    ),
                }
            }
        }
    }

    async fn close_sinks(&mut self) -> Result<(), String> {
        let mut failures = Vec::new();

        // Close is itself an ordered command: each actor processes every previously queued write
        // before acknowledging connector flush/close. Its bounded enqueue, acknowledgement, and
        // join replace the old unbounded pre-close Sync round trip.
        let close_results =
            futures::future::join_all(self.sinks.iter().map(|(name, handle, _, _, _)| {
                let name = name.clone();
                let handle = handle.clone();
                async move { (name, handle.close().await) }
            }))
            .await;
        for (name, result) in close_results {
            if let Err(error) = result {
                failures.push(format!("sink '{name}' shutdown close failed: {error}"));
            }
        }
        self.drain_sink_events();
        if let Some(reason) = self.sink_fault.take() {
            failures.push(reason);
        } else if self.sink_timed_out {
            failures.push("one or more best-effort sink writes failed before shutdown".to_string());
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
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
                |(sink_idx, (sink_name, handle, _filter_sql, sink_input, contract))| {
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
                    let accepts_full_changelog = contract.accepts_full_changelog();
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
                                accepts_full_changelog,
                            );
                            if prepared.num_rows() == 0 {
                                continue;
                            }
                            if let Err(e) = handle.write_batch(prepared.into_owned()).await {
                                tracing::warn!(
                                    sink = %sink_name,
                                    error = %e,
                                    "Sink write could not be enqueued"
                                );
                                return Some(format!(
                                    "sink '{sink_name}' write enqueue failed: {e}"
                                ));
                            }
                        }
                        None
                    })
                },
            )
            .collect();
        let direct_failures = futures::future::join_all(sink_futures).await;
        for reason in direct_failures.into_iter().flatten() {
            // Do not depend on the bounded event channel for correctness. In particular, a full
            // event channel must not turn an enqueue timeout into a checkpointable lost write.
            self.record_dropped_sink_write(reason);
        }

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
        self.delivery_guarantee != DeliveryGuarantee::BestEffort || self.in_cluster()
    }

    fn take_sink_fault(&mut self) -> Option<String> {
        self.reap_checkpoint_tail_tasks();
        self.sink_fault
            .take()
            .or_else(|| self.checkpoint_fault.lock().take())
    }

    async fn settle_checkpoint_tail_tasks(&mut self, abort: bool) -> Result<(), String> {
        if abort {
            self.checkpoint_tail_tasks.abort_all();
        }
        let mut failures = Vec::new();
        while let Some(result) = self.checkpoint_tail_tasks.join_next().await {
            match result {
                Ok(()) => {}
                Err(error) if abort && error.is_cancelled() => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "checkpoint durable tail task failure: {}",
                failures.join("; ")
            ))
        }
    }

    fn record_checkpoint_failure(&mut self, checkpoint_id: u64, reason: &str) {
        if self.delivery_guarantee == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            set_checkpoint_fault(
                &self.checkpoint_fault,
                format!("checkpoint {checkpoint_id}: {reason}"),
            );
        }
    }

    fn record_checkpoint_admission_failure(&mut self, reason: &str) {
        if self.delivery_guarantee == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            set_checkpoint_fault(
                &self.checkpoint_fault,
                format!("checkpoint admission: {reason}"),
            );
        }
    }

    async fn reserve_checkpoint_attempt(
        &mut self,
        attempt_started: std::time::Instant,
    ) -> Result<CheckpointAttempt, String> {
        self.reserve_attempt(attempt_started)
            .await
            .map_err(|error| error.to_string())
    }

    async fn abandon_checkpoint_attempt(&mut self, attempt: CheckpointAttempt, reason: &str) {
        self.abandon_reserved_attempt(attempt, reason.to_owned())
            .await;
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

    /// Service follower announcements observed from the cluster leader.
    /// All local checkpoint admission is owned exclusively by `StreamingCoordinator`.
    async fn service_checkpoint_control(
        &mut self,
        source_offsets: FxHashMap<String, SourceCheckpoint>,
    ) -> Option<u64> {
        // Followers respond only to a leader-published PREPARE. Leaders and local runtimes return
        // control to the streaming coordinator, which is the sole admission owner.
        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            return if cc.is_leader() {
                None
            } else {
                self.maybe_follower_checkpoint(cc, source_offsets).await
            };
        }

        let _ = source_offsets;
        None
    }

    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        attempt: CheckpointAttempt,
        attempt_started: std::time::Instant,
    ) -> crate::pipeline::BarrierOutcome {
        use crate::pipeline::{BarrierOutcome, SkipReason};

        #[cfg(feature = "cluster")]
        if let Some(cc) = self.cluster_controller.clone() {
            if !cc.is_leader() {
                if let Some(ann) = self.pending_follower_checkpoint.take() {
                    // A slow round can finish after its epoch was abandoned; never attribute
                    // captured offsets to a newer announcement.
                    if ann.checkpoint_id != attempt.checkpoint_id || ann.epoch != attempt.epoch {
                        tracing::warn!(
                            round_checkpoint_id = attempt.checkpoint_id,
                            round_epoch = attempt.epoch,
                            pending_checkpoint_id = ann.checkpoint_id,
                            pending_epoch = ann.epoch,
                            "stale follower barrier round — its epoch was abandoned; \
                             re-queueing the newer announcement"
                        );
                        self.pending_follower_checkpoint = Some(ann);
                        return BarrierOutcome::Failed;
                    }
                    return self
                        .run_follower_checkpoint_deferred(ann, source_checkpoints, attempt_started)
                        .await;
                }
                tracing::warn!(
                    "follower received checkpoint_with_barrier but pending_follower_checkpoint is None"
                );
                return BarrierOutcome::Failed;
            }
        }

        if self.prom.cycles.get() == 0 {
            return BarrierOutcome::Skipped(SkipReason::NoCyclesSinceLastCheckpoint);
        }

        let attempt_deadline =
            tokio::time::Instant::from_std(attempt_started) + self.checkpoint_timeout;
        // Sink fencing, shuffle alignment, and state capture all pause the pipeline. A
        // drop-observing timer also records early failures rather than hiding their latency.
        let _stall_timer = self.prom.checkpoint_pipeline_stall_duration.start_timer();

        match tokio::time::timeout_at(
            attempt_deadline,
            self.sync_sinks_and_drain_events(attempt_deadline),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(%error, "checkpoint sink write fence failed");
                return BarrierOutcome::Failed;
            }
            Err(_) => {
                tracing::error!(
                    timeout = ?self.checkpoint_timeout,
                    "checkpoint sink write fence exhausted the end-to-end attempt deadline"
                );
                return BarrierOutcome::Failed;
            }
        }

        // A pending exactly-once sink fault means the coordinator is about to fault for recovery;
        // don't seal this epoch past the dropped rows (CP-4). Leave the flag for `take_sink_fault`.
        if self.sink_fault.is_some() {
            return BarrierOutcome::Failed;
        }

        // Clear after one suppression; the timer path is unreachable under barrier checkpointing.
        if self.sink_timed_out {
            self.sink_timed_out = false;
            return BarrierOutcome::Skipped(SkipReason::PreservingReplayWindowAfterSinkTimeout);
        }

        // Align the shuffle before capture so peers' pre-barrier rows enter the snapshot.
        #[cfg(feature = "cluster")]
        match tokio::time::timeout_at(attempt_deadline, self.align_shuffle_for_leader(attempt))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "shuffle barrier alignment failed");
                return BarrierOutcome::Failed;
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?self.checkpoint_timeout,
                    "shuffle alignment exhausted the end-to-end checkpoint deadline"
                );
                return BarrierOutcome::Failed;
            }
        }

        // Trailing loss — a peer's last frames of the epoch, which no data-gap check can see —
        // surfaces only when that peer's barrier lands, i.e. inside the alignment above, after the
        // fence at the top of this function. Re-check before capture: a snapshot taken now would
        // seal the gap, and the rewind target would then sit at or above the corrupt epoch (CL-2).
        #[cfg(feature = "cluster")]
        {
            self.check_shuffle_loss();
            if self.sink_fault.is_some() {
                return BarrierOutcome::Failed;
            }
        }

        let operator_states = match tokio::time::timeout_at(
            attempt_deadline,
            self.capture_and_serialize_operator_state(),
        )
        .await
        {
            Ok(Ok(states)) => states,
            Ok(Err(error)) => {
                tracing::warn!(%error, "Stream executor barrier checkpoint failed");
                return BarrierOutcome::Failed;
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?self.checkpoint_timeout,
                    "state capture exhausted the end-to-end checkpoint deadline"
                );
                return BarrierOutcome::Failed;
            }
        };

        let vnode_states = match self.capture_vnode_states() {
            Ok(states) => states,
            Err(error) => {
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    error = %error,
                    "barrier checkpoint vnode capture failed"
                );
                return BarrierOutcome::Failed;
            }
        };
        let request = self.build_checkpoint_request(operator_states);

        let in_flight = EpochInFlightGuard::claim(
            &self.checkpoint_in_flight,
            &self.staged_bytes,
            staged_request_bytes(&request, &vnode_states),
        );
        let wm = self
            .pipeline_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        let tail = LeaderTail {
            _in_flight: in_flight,
            coordinator: Arc::clone(&self.coordinator),
            complete_tx: self.checkpoint_complete_tx.clone(),
            request,
            vnode_states,
            fan_out: source_checkpoints.clone(),
            local_watermark_ms: if wm == i64::MIN { None } else { Some(wm) },
            attempt,
            attempt_started,
            checkpoint_timeout: self.checkpoint_timeout,
            checkpoint_cleanup_timeout: self.checkpoint_cleanup_timeout,
            fault_on_failure: self.delivery_guarantee
                == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce,
            checkpoint_fault: Arc::clone(&self.checkpoint_fault),
            #[cfg(feature = "cluster")]
            controller: self.cluster_controller.clone(),
            #[cfg(feature = "cluster")]
            quorum_timeout: self.quorum_timeout,
            #[cfg(feature = "cluster")]
            delta_rebase_needed: Arc::clone(&self.delta_rebase_needed),
        };
        if self.checkpoint_committable_sinks {
            Self::run_leader_tail(tail).await;
        } else {
            self.spawn_checkpoint_tail(Self::run_leader_tail(tail));

            #[cfg(feature = "cluster")]
            if let Some(cc) = self.cluster_controller.clone() {
                let has_shuffle = self.graph.cluster_shuffle_config().is_some();
                let _ = tokio::time::timeout_at(
                    attempt_deadline,
                    Self::wait_for_aligned_resume(
                        has_shuffle,
                        &cc,
                        attempt.epoch,
                        self.quorum_timeout,
                    ),
                )
                .await;
            }
        }
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
                incremental,
            } => {
                self.graph.add_query(
                    name.clone(),
                    sql,
                    emit_clause,
                    window_config,
                    order_config,
                    None,
                    join_config,
                    incremental,
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
        // No checkpoint in flight: a clean vnode then matches the durable bytes and a drop can't
        // race the staged cold-group fetch. Before the budget gate so a budgetless tier still releases.
        if self.checkpoint_in_flight.load(Ordering::Acquire) != 0 {
            return;
        }
        self.graph.release_tier_drops();
        let Some(budget) = self.state_memory_budget_bytes else {
            return;
        };
        let total: usize = self.graph.state_bytes_per_operator().map(|(_, b)| b).sum();
        let watermark = budget / STATE_DEMOTE_WATERMARK_DEN * STATE_DEMOTE_WATERMARK_NUM;
        if total < watermark {
            return;
        }
        let target = budget / STATE_DEMOTE_TARGET_DEN * STATE_DEMOTE_TARGET_NUM;
        if self.state_tier_group_demotion {
            // Shed individual idle groups (skew-proof). `tier` is held by each operator's
            // promotion channel, so the pass needs only the free budget.
            let to_free = total.saturating_sub(target);
            let demoted = self.graph.demote_cold_groups(to_free).await;
            if demoted > 0 {
                tracing::debug!(demoted, "demoted idle groups to the cold tier");
            }
            return;
        }
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
        // Single-node: keep cycling while any operator has promotion work pending (a batch deferred
        // until its cold-group fetch resolves) so it drains even when the source goes quiet.
        #[cfg(feature = "state-tier")]
        if self.graph.has_pending_promotion() {
            return true;
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

    #[test]
    fn source_checkpoint_map_materializes_offsets_and_metadata() {
        use laminar_connectors::checkpoint::PersistentOffset;

        let mut inventory = PersistentOffset::new("[", ",", "]");
        inventory.push_fragment(r#""first.parquet""#);
        inventory.push_fragment(r#""second.parquet""#);
        let mut files = SourceCheckpoint::new();
        files.set_offset("row", "17");
        files.set_persistent_offset("manifest", inventory);
        files.set_metadata("connector", "file");
        files.set_metadata("schema_sha256", "abc123");

        let mut kafka = SourceCheckpoint::new();
        kafka.set_offset("orders:0", "42");
        let mut snapshots = FxHashMap::default();
        snapshots.insert("files".to_string(), files.clone());
        snapshots.insert("orders".to_string(), kafka);

        let materialized = materialize_source_checkpoint_map(snapshots);
        let files_durable = materialized.get("files").expect("files checkpoint");
        assert_eq!(
            files_durable.offsets.get("manifest").map(String::as_str),
            Some(r#"["first.parquet","second.parquet"]"#)
        );
        assert_eq!(
            files_durable.offsets.get("row").map(String::as_str),
            Some("17")
        );
        assert_eq!(
            files_durable
                .metadata
                .get("schema_sha256")
                .map(String::as_str),
            Some("abc123")
        );
        assert_eq!(
            materialized
                .get("orders")
                .and_then(|checkpoint| checkpoint.offsets.get("orders:0"))
                .map(String::as_str),
            Some("42")
        );
    }

    #[tokio::test]
    async fn source_checkpoint_materialization_rejects_expired_deadline() {
        let attempt = CheckpointAttempt::new(7, 11);
        let result = materialize_source_checkpoints_until(
            FxHashMap::default(),
            attempt,
            tokio::time::Instant::now(),
        )
        .await;
        let Err(error) = result else {
            panic!("an expired absolute deadline must reject materialization");
        };
        assert!(error.contains("checkpoint 11 epoch 7"));
        assert!(error.contains("before source-offset materialization"));
    }

    #[test]
    fn mv_state_is_retained_when_graph_has_no_snapshot() {
        let mv_bytes = bytes::Bytes::from_static(b"materialized-view-state");
        let states = combine_operator_checkpoint_states(
            None,
            [("mv:test_view".to_string(), mv_bytes.clone())],
        );

        assert_eq!(states.get("mv:test_view"), Some(&mv_bytes));
        assert!(!states.contains_key("operator_graph"));
    }

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

    #[tokio::test]
    async fn reserved_attempt_cleanup_deadline_includes_coordinator_lock() {
        let coordinator = tokio::sync::Mutex::new(None);
        let lock = coordinator.lock().await;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
        let started = std::time::Instant::now();

        let error = cleanup_reserved_attempt_until(
            &coordinator,
            CheckpointAttempt::new(7, 11),
            "injected admission failure".into(),
            deadline,
        )
        .await
        .unwrap_err();

        assert!(error.contains("cleanup exceeded its end-to-end deadline"));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "coordinator lock contention must not refresh or bypass the cleanup deadline"
        );
        drop(lock);
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
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                3,
                std::time::Duration::from_secs(3),
            ),
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
            ConnectorPipelineCallback::wait_for_aligned_resume(
                true,
                &controller,
                3,
                std::time::Duration::from_secs(3),
            ),
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
            ConnectorPipelineCallback::wait_for_aligned_resume(
                false,
                &controller,
                3,
                std::time::Duration::from_secs(3),
            ),
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
            false,
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
        let store_box = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path()));
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
