//! Single-task pipeline coordinator on the dedicated `laminar-compute` thread.
//!
//! ```text
//! Source task (main runtime) ──MAsyncTx──► StreamingCoordinator
//!                                               │  execute_cycle / write_to_sinks
//!                                               ▼  Sinks
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use crossfire::{mpsc, AsyncRx, MAsyncTx};
use futures::FutureExt;
use laminar_connectors::checkpoint::SourceCheckpoint;
#[cfg(test)]
use laminar_connectors::connector::SourceBatch;
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart,
};
#[cfg(test)]
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
use laminar_core::state::CheckpointAttempt;
use rustc_hash::{FxHashMap, FxHashSet};

use super::callback::{
    BarrierOutcome, CheckpointCompletion, CycleError, PipelineCallback, SourceRegistration,
};
use super::config::PipelineConfig;
use crate::error::DbError;

type SourceMsgRx = AsyncRx<mpsc::Array<SourceMsg>>;
type ControlMsgRx = AsyncRx<mpsc::Array<super::ControlMsg>>;
type ForceCheckpointReply = crate::db::ForceCheckpointReply;

/// Message from a source task to the coordinator; carries the [`SourceCheckpoint`]
/// captured at production time so no offset is checkpointed for unprocessed data.
enum SourceMsg {
    Batch {
        source_idx: usize,
        batch: RecordBatch,
        /// Committed to `committed_offsets` only after a successful `execute_cycle`.
        checkpoint: SourceCheckpoint,
    },
    Barrier {
        source_idx: usize,
        barrier: CheckpointBarrier,
        checkpoint: SourceCheckpoint,
    },
    Fault {
        source: Arc<str>,
        error: String,
    },
}

/// Handle to a running source I/O task.
struct SourceHandle {
    name: Arc<str>,
    shutdown: Arc<tokio::sync::Notify>,
    join: tokio::task::JoinHandle<()>,
    barrier_injector: CheckpointBarrierInjector,
    /// Notifies the source of a committed `(epoch, checkpoint)` so it can ack upstream.
    /// The checkpoint is what was written to the manifest (may lag). Empty only when the epoch
    /// captured no state for this source; an empty one is a no-op for upstream advancement.
    epoch_committed_tx: tokio::sync::watch::Sender<Option<(u64, SourceCheckpoint)>>,
}

/// Why [`StreamingCoordinator::run`] returned.
#[derive(Debug)]
pub enum ExitReason {
    /// Shutdown signaled, or all source senders dropped — a clean stop.
    Shutdown,
    /// Fatal runtime error under a replay guarantee; the caller recovers from the last checkpoint.
    Fault(String),
}

/// Single-task pipeline coordinator — no core threads.
pub struct StreamingCoordinator {
    config: PipelineConfig,
    rx: SourceMsgRx,
    source_handles: Vec<SourceHandle>,
    source_names: Vec<Arc<str>>,
    shutdown: Arc<tokio::sync::Notify>,
    pending_barrier: PendingBarrier,
    last_checkpoint: Instant,
    checkpoint_request_flags: Vec<Arc<AtomicBool>>,
    source_batches_buf: FxHashMap<Arc<str>, Vec<RecordBatch>>,
    /// Batches from a source after its barrier this drain cycle; they belong to the
    /// next epoch and are deferred to the next cycle.
    post_barrier_buf: Vec<SourceMsg>,
    pending_watermark_batches: Vec<(Arc<str>, RecordBatch)>,
    /// Sources that delivered a barrier this drain cycle; subsequent batches from
    /// them go to `post_barrier_buf`.
    barrier_seen: FxHashSet<usize>,
    /// Per-source offset merged from `pending_offsets` after a successful `execute_cycle`.
    committed_offsets: Vec<Option<SourceCheckpoint>>,
    /// Offsets staged by `process_msg`; merged on success, discarded on failure.
    pending_offsets: Vec<Option<SourceCheckpoint>>,
    control_rx: ControlMsgRx,
    checkpoint_complete_rx:
        Option<crossfire::AsyncRx<crossfire::mpsc::Array<CheckpointCompletion>>>,
    /// Public checkpoint requests waiting for the next newly-admitted exact attempt.
    force_ckpt_rx: Option<crate::db::ForceCheckpointRx>,
    manual_waiting: Vec<ForceCheckpointReply>,
    /// Requests attached at admission. Later requests remain in `manual_waiting`.
    manual_active: Option<ManualCheckpointAttempt>,
    /// Epochs between admission and durable (tails still running); shared with callback.
    checkpoint_in_flight: Arc<AtomicU64>,
    /// Admission cap on `checkpoint_in_flight`. Exactly-once pipelines use 1.
    max_in_flight_epochs: u64,
    /// Captured-state bytes held by in-flight epochs; shared with callback.
    staged_bytes: Arc<AtomicU64>,
    max_staged_bytes: u64,
    /// Shared exact external-commit bound, checked before ID reservation/barrier injection.
    coordinated_commit_admission: Option<crate::checkpoint_coordinator::CoordinatedCommitAdmission>,
}

/// Public checkpoint callers attached to one exact attempt at admission time.
struct ManualCheckpointAttempt {
    attempt: CheckpointAttempt,
    replies: Vec<ForceCheckpointReply>,
}

struct CheckpointAdmission {
    manual: bool,
}

/// Tracks in-flight checkpoint barrier alignment.
struct PendingBarrier {
    attempt: Option<CheckpointAttempt>,
    sources_total: usize,
    sources_aligned: FxHashSet<usize>,
    source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    started_at: Instant,
    active: bool,
}

impl PendingBarrier {
    fn new() -> Self {
        Self {
            attempt: None,
            sources_total: 0,
            sources_aligned: FxHashSet::default(),
            source_checkpoints: FxHashMap::default(),
            started_at: Instant::now(),
            active: false,
        }
    }

    fn reset(&mut self, attempt: CheckpointAttempt, sources_total: usize) {
        self.attempt = Some(attempt);
        self.sources_total = sources_total;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.started_at = Instant::now();
        self.active = true;
    }

    /// Clear alignment state and return the exact active attempt, if one existed.
    fn take_active_attempt(&mut self) -> Option<CheckpointAttempt> {
        if !self.active {
            return None;
        }
        self.active = false;
        self.sources_total = 0;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.attempt.take()
    }

    fn clear(&mut self) {
        self.active = false;
        self.attempt = None;
        self.sources_total = 0;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
    }
}

/// Fallback timeout for idle wake.
const IDLE_TIMEOUT: Duration = Duration::from_millis(100);

/// Cap on a source task's post-shutdown flush so a hot source can't stall shutdown.
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(2);

/// Cap on awaiting a source task at shutdown before aborting it.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Shutdown-only poll cadence. It closes the atomic/channel race when a tail drops its in-flight
/// guard without producing another wakeup (for example an aborted cluster follower tail).
const SHUTDOWN_COMPLETION_TICK: Duration = Duration::from_millis(10);

/// Grace period for already-captured asynchronous checkpoint tails. On expiry their tracked
/// tasks are cancelled before sources or sinks are torn down; exact attempt namespaces leave any
/// ambiguous remote write safe for recovery.
const SHUTDOWN_CHECKPOINT_TAIL_TIMEOUT: Duration = Duration::from_secs(8);

/// Graceful stop gives already-sealed checkpoints one bounded opportunity to
/// reach coordinated external sinks. Timeout leaves durable markers for replay.
const COORDINATED_COMMIT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// Throttled WARN while barrier admission is paused at the staged-state cap; this
/// runs every coordinator tick, so an unthrottled warn would spam under a backlog.
fn warn_staged_cap_throttled(staged_bytes: u64, cap: u64) {
    static THROTTLE: crate::log_throttle::LogThrottle =
        crate::log_throttle::LogThrottle::every(Duration::from_secs(10));
    if THROTTLE.allow() {
        tracing::warn!(
            staged_bytes,
            cap,
            "checkpoint admission paused: staged-state cap reached"
        );
    }
}

fn warn_external_commit_cap_throttled(known: bool, pending: u64, cap: u64) {
    static THROTTLE: crate::log_throttle::LogThrottle =
        crate::log_throttle::LogThrottle::every(Duration::from_secs(10));
    if THROTTLE.allow() {
        tracing::warn!(
            lag_known = known,
            pending_external_checkpoints = pending,
            cap,
            "checkpoint admission paused at coordinated external-commit bound"
        );
    }
}

/// Apply the newest durable commit notification while no source poll borrows
/// the connector. Non-best-effort pipelines fault if upstream acknowledgement
/// fails because silently continuing can exhaust broker retention/headroom.
async fn acknowledge_latest_source_commit(
    connector: &mut dyn SourceConnector,
    epoch_committed_rx: &mut tokio::sync::watch::Receiver<Option<(u64, SourceCheckpoint)>>,
    delivery_guarantee: DeliveryGuarantee,
    src_name: &str,
    task_tx: &MAsyncTx<mpsc::Array<SourceMsg>>,
) -> bool {
    let Some((epoch, checkpoint)) = epoch_committed_rx.borrow_and_update().clone() else {
        return true;
    };
    if let Err(error) = connector.notify_epoch_committed(epoch, &checkpoint).await {
        if delivery_guarantee == DeliveryGuarantee::BestEffort {
            tracing::warn!(
                source = src_name,
                %error,
                epoch,
                "notify_epoch_committed failed",
            );
            return true;
        }
        let _ = task_tx
            .send(SourceMsg::Fault {
                source: Arc::from(src_name),
                error: format!("commit notification failed at epoch {epoch}: {error}"),
            })
            .await;
        return false;
    }
    true
}

/// Backoff between completed polls while still servicing durable commit
/// notifications immediately. This never races a live `poll_batch` future.
async fn wait_source_idle(
    connector: &mut dyn SourceConnector,
    epoch_committed_rx: &mut tokio::sync::watch::Receiver<Option<(u64, SourceCheckpoint)>>,
    delivery_guarantee: DeliveryGuarantee,
    src_name: &str,
    task_tx: &MAsyncTx<mpsc::Array<SourceMsg>>,
    shutdown: &tokio::sync::Notify,
    poll_interval: Duration,
) -> bool {
    tokio::select! {
        biased;
        () = shutdown.notified() => false,
        changed = epoch_committed_rx.changed() => match changed {
            Ok(()) => acknowledge_latest_source_commit(
                connector,
                epoch_committed_rx,
                delivery_guarantee,
                src_name,
                task_tx,
            ).await,
            Err(_) => false,
        },
        () = tokio::time::sleep(poll_interval) => true,
    }
}

impl StreamingCoordinator {
    async fn close_startup_source(
        source: &mut SourceRegistration,
        cleanup_deadline: tokio::time::Instant,
    ) {
        match tokio::time::timeout_at(cleanup_deadline, source.connector.close()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(
                    source = %source.name,
                    %error,
                    "source close failed while rolling back pipeline startup"
                );
            }
            Err(_) => {
                tracing::warn!(
                    source = %source.name,
                    "source close exceeded the shared pipeline-startup cleanup deadline"
                );
            }
        }
    }

    async fn close_prepared_sources(
        sources: &mut Vec<SourceRegistration>,
        cleanup_deadline: tokio::time::Instant,
    ) {
        while let Some(mut source) = sources.pop() {
            Self::close_startup_source(&mut source, cleanup_deadline).await;
        }
    }

    /// Notify every source of a committed epoch so they can ack to their broker.
    fn broadcast_epoch_committed(
        &self,
        epoch: u64,
        per_source: &FxHashMap<String, SourceCheckpoint>,
    ) {
        for handle in &self.source_handles {
            let cp = per_source
                .get(handle.name.as_ref())
                .cloned()
                .unwrap_or_else(SourceCheckpoint::new);
            let _ = handle.epoch_committed_tx.send(Some((epoch, cp)));
        }
    }

    /// Build the coordinator, atomically start each source connector, and spawn source tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery guarantee constraints are violated or a source fails to start
    /// at its requested initial/recovered position.
    #[allow(clippy::too_many_lines)]
    pub async fn new(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
        source_gate: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Self, DbError> {
        if matches!(
            config.delivery_guarantee,
            DeliveryGuarantee::AtLeastOnce | DeliveryGuarantee::ExactlyOnce
        ) {
            for src in &sources {
                if !src.contract.supports_replay() {
                    return Err(DbError::Config(format!(
                        "[LDB-5031] {} requires source '{}' to support replay",
                        config.delivery_guarantee, src.name
                    )));
                }
            }
            if config.checkpoint_interval.is_none() {
                return Err(DbError::Config(format!(
                    "[LDB-5032] {} requires checkpointing to be enabled",
                    config.delivery_guarantee
                )));
            }
        }

        // A source that releases an upstream resource only on durable commit (e.g. a PostgreSQL
        // replication slot, whose WAL advances via notify_epoch_committed) needs checkpointing —
        // otherwise that resource grows without bound (CN-1). Reject the combination up front.
        if config.checkpoint_interval.is_none() {
            for src in &sources {
                if src.contract.requires_checkpointing() {
                    return Err(DbError::Config(format!(
                        "[LDB-5034] source '{}' requires checkpointing to be enabled: its upstream \
                         resource (e.g. a replication slot) is only released at a durable checkpoint",
                        src.name
                    )));
                }
            }
        }

        if config.channel_capacity == 0 {
            return Err(DbError::Config(
                "[LDB-0010] channel_capacity must be > 0".into(),
            ));
        }

        let source_count = sources.len();
        let mut prepared_sources = Vec::with_capacity(source_count);
        let mut checkpoint_request_flags = Vec::new();
        let mut committed_offsets = Vec::with_capacity(source_count);
        let source_start_timeout = config.checkpoint_timeout;
        let source_start_deadline = tokio::time::Instant::now() + source_start_timeout;

        // Do not spawn a polling task until every source has atomically installed its startup
        // position. Otherwise a later startup failure detaches the earlier tasks and they keep
        // polling without an owner capable of shutting them down.
        for mut src in sources {
            if let Some(flag) = src.connector.checkpoint_requested() {
                checkpoint_request_flags.push(flag);
            }

            let src_name = src.name.clone();
            // Seed with the durable resume position so a pre-data shutdown still checkpoints it.
            // Capture it before moving the complete request into `start`; no connector lifecycle
            // operation is allowed between configuration and cursor installation.
            let committed_offset = match &src.position {
                SourcePosition::Initial => None,
                SourcePosition::Resume { checkpoint, .. } => Some(checkpoint.clone()),
            };
            let start_position = src.position.clone();
            let start = SourceStart {
                config: src.config.clone(),
                position: start_position.clone(),
                delivery: config.delivery_guarantee,
            };
            let start_error =
                match tokio::time::timeout_at(source_start_deadline, src.connector.start(start))
                    .await
                {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error.to_string()),
                    Err(_) => Some(format!(
                        "exceeded the shared {source_start_timeout:?} source-start stage deadline"
                    )),
                };
            if let Some(error) = start_error {
                // Include the current connector: a cancelled/failed start may already have acquired
                // external resources. All rollback closes share one fresh absolute deadline.
                prepared_sources.push(src);
                let cleanup_deadline =
                    tokio::time::Instant::now() + PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT;
                Self::close_prepared_sources(&mut prepared_sources, cleanup_deadline).await;
                return match start_position {
                    SourcePosition::Initial => Err(DbError::Config(format!(
                        "source '{src_name}' start failed at initial position: {error}"
                    ))),
                    SourcePosition::Resume { attempt, .. } => Err(DbError::Checkpoint(format!(
                        "[LDB-6003] source '{src_name}' start failed while resuming exact \
                             checkpoint epoch={} id={}: {error}",
                        attempt.epoch, attempt.checkpoint_id
                    ))),
                };
            }

            committed_offsets.push(committed_offset);
            prepared_sources.push(src);
        }

        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(config.channel_capacity);
        let mut source_handles = Vec::with_capacity(source_count);
        let mut source_names = Vec::with_capacity(source_count);

        for (idx, src) in prepared_sources.into_iter().enumerate() {
            let task_shutdown = Arc::new(tokio::sync::Notify::new());
            let task_shutdown_clone = Arc::clone(&task_shutdown);
            let task_tx = tx.clone();
            let task_gate = Arc::clone(&source_gate);
            let max_poll = config.max_poll_records;
            let poll_interval = config.fallback_poll_interval;
            let delivery_guarantee = config.delivery_guarantee;
            let src_name = src.name.clone();
            let mut connector = src.connector;

            let barrier_injector = CheckpointBarrierInjector::new();
            let barrier_handle = barrier_injector.handle();

            let (epoch_committed_tx, mut epoch_committed_rx) =
                tokio::sync::watch::channel::<Option<(u64, SourceCheckpoint)>>(None);

            let join = tokio::spawn(async move {
                // Ack a fresh commit before polling more — keeps
                // max_ack_pending headroom for the broker.
                loop {
                    match epoch_committed_rx.has_changed() {
                        Ok(true) => {
                            if !acknowledge_latest_source_commit(
                                connector.as_mut(),
                                &mut epoch_committed_rx,
                                delivery_guarantee,
                                &src_name,
                                &task_tx,
                            )
                            .await
                            {
                                break;
                            }
                            continue;
                        }
                        Ok(false) => {}
                        Err(_) => break,
                    }

                    // Source-intake gate: held closed during a coordinated round until the
                    // restore quorum, so a rewound source doesn't re-shuffle its replay into a
                    // peer whose receiver hasn't rebound (the frames would be dropped). The
                    // compute loop keeps draining the shuffle receiver on idle cycles meanwhile.
                    if task_gate.load(std::sync::atomic::Ordering::Acquire) {
                        // Barriers must still flow: the round waits for the rebalance rotation,
                        // and the rotation's pre-rotation checkpoint aligns on a barrier from
                        // every source. Starving them here deadlocks the round against itself.
                        if let Some(barrier) = barrier_handle.poll() {
                            let cp = connector.checkpoint();
                            let msg = SourceMsg::Barrier {
                                source_idx: idx,
                                barrier,
                                checkpoint: cp,
                            };
                            if task_tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                        if !wait_source_idle(
                            connector.as_mut(),
                            &mut epoch_committed_rx,
                            delivery_guarantee,
                            &src_name,
                            &task_tx,
                            &task_shutdown_clone,
                            poll_interval,
                        )
                        .await
                        {
                            break;
                        }
                        continue;
                    }
                    let poll_result = tokio::select! {
                        biased;
                        () = task_shutdown_clone.notified() => break,
                        result = connector.poll_batch(max_poll) => result,
                    };

                    match poll_result {
                        Ok(Some(batch)) => {
                            let cp = connector.checkpoint();
                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch: batch.records,
                                checkpoint: cp,
                            };
                            if task_tx.send(msg).await.is_err() {
                                break; // Coordinator dropped
                            }
                        }
                        Ok(None) => {
                            if !wait_source_idle(
                                connector.as_mut(),
                                &mut epoch_committed_rx,
                                delivery_guarantee,
                                &src_name,
                                &task_tx,
                                &task_shutdown_clone,
                                poll_interval,
                            )
                            .await
                            {
                                break;
                            }
                        }
                        Err(e) if !e.is_transient() => {
                            tracing::error!(source = %src_name, error = %e, "terminal poll error");
                            if delivery_guarantee != DeliveryGuarantee::BestEffort {
                                let _ = task_tx
                                    .send(SourceMsg::Fault {
                                        source: Arc::from(src_name.as_str()),
                                        error: e.to_string(),
                                    })
                                    .await;
                            }
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(source = %src_name, error = %e, "poll error (retrying)");
                            if !wait_source_idle(
                                connector.as_mut(),
                                &mut epoch_committed_rx,
                                delivery_guarantee,
                                &src_name,
                                &task_tx,
                                &task_shutdown_clone,
                                poll_interval,
                            )
                            .await
                            {
                                break;
                            }
                        }
                    }

                    if let Some(barrier) = barrier_handle.poll() {
                        let cp = connector.checkpoint();
                        let msg = SourceMsg::Barrier {
                            source_idx: idx,
                            barrier,
                            checkpoint: cp,
                        };
                        if task_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                }

                // Bounded best-effort flush before close(): the `while` deadline bounds an
                // always-ready poll (timeout() polls the future first). Unflushed rows resume
                // from the committed offset.
                let deadline = Instant::now() + SHUTDOWN_DRAIN_BUDGET;
                while Instant::now() < deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    match tokio::time::timeout(remaining, connector.poll_batch(max_poll)).await {
                        Ok(Ok(Some(batch))) => {
                            let cp = connector.checkpoint();
                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch: batch.records,
                                checkpoint: cp,
                            };
                            if task_tx.try_send(msg).is_err() {
                                break;
                            }
                        }
                        _ => break,
                    }
                }

                // Drain EpochCommitted broadcasts before close so a durable tail settled during
                // shutdown is acknowledged to the broker.
                while let Ok(()) = epoch_committed_rx.changed().await {
                    let snapshot = epoch_committed_rx.borrow_and_update().clone();
                    if let Some((e, cp)) = snapshot {
                        if let Err(err) = connector.notify_epoch_committed(e, &cp).await {
                            tracing::warn!(
                                source = %src_name,
                                error = %err,
                                epoch = e,
                                "notify_epoch_committed failed",
                            );
                        }
                    }
                }

                if let Err(e) = connector.close().await {
                    tracing::warn!(source = %src_name, error = %e, "source close error");
                }
            });

            let arc_name: Arc<str> = Arc::from(src.name.as_str());
            source_handles.push(SourceHandle {
                name: Arc::clone(&arc_name),
                shutdown: task_shutdown,
                join,
                barrier_injector,
                epoch_committed_tx,
            });
            source_names.push(arc_name);
        }

        Ok(Self {
            config,
            rx,
            source_handles,
            source_names,
            shutdown,
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags,
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            pending_offsets: vec![None; committed_offsets.len()],
            committed_offsets,
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        })
    }

    /// Wire in the callback's admission counters so the coordinator gates new barriers.
    pub(crate) fn with_checkpoint_admission(
        mut self,
        in_flight: Arc<AtomicU64>,
        max_in_flight_epochs: u64,
        staged_bytes: Arc<AtomicU64>,
        max_staged_bytes: u64,
    ) -> Self {
        self.checkpoint_in_flight = in_flight;
        self.max_in_flight_epochs = max_in_flight_epochs.max(1);
        self.staged_bytes = staged_bytes;
        self.max_staged_bytes = max_staged_bytes;
        self
    }

    pub(crate) fn with_coordinated_commit_admission(
        mut self,
        admission: Option<crate::checkpoint_coordinator::CoordinatedCommitAdmission>,
    ) -> Self {
        self.coordinated_commit_admission = admission;
        self
    }

    pub(crate) fn with_checkpoint_complete_rx(
        mut self,
        rx: crossfire::AsyncRx<crossfire::mpsc::Array<CheckpointCompletion>>,
    ) -> Self {
        self.checkpoint_complete_rx = Some(rx);
        self
    }

    pub(crate) fn with_force_checkpoint_rx(mut self, rx: crate::db::ForceCheckpointRx) -> Self {
        self.force_ckpt_rx = Some(rx);
        self
    }

    fn drain_manual_requests(&mut self) {
        let Some(rx) = self.force_ckpt_rx.as_ref() else {
            return;
        };
        while let Ok(reply) = rx.try_recv() {
            self.manual_waiting.push(reply);
        }
    }

    fn activate_manual_attempt(&mut self, attempt: CheckpointAttempt) {
        if self.manual_waiting.is_empty() {
            return;
        }
        debug_assert!(self.manual_active.is_none());
        self.manual_active = Some(ManualCheckpointAttempt {
            attempt,
            replies: std::mem::take(&mut self.manual_waiting),
        });
    }

    fn fail_waiting_manual(&mut self, error: impl Into<String>) {
        let error = error.into();
        for reply in self.manual_waiting.drain(..) {
            reply.send(Err(DbError::Checkpoint(error.clone())));
        }
    }

    fn finish_manual_success(
        &mut self,
        attempt: CheckpointAttempt,
        result: &crate::checkpoint_coordinator::CheckpointResult,
    ) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
        if completed != attempt || !result.success {
            let reason = format!(
                "manual checkpoint terminal result mismatch: admitted epoch={} id={}, \
                 completed epoch={} id={} success={}",
                attempt.epoch,
                attempt.checkpoint_id,
                completed.epoch,
                completed.checkpoint_id,
                result.success,
            );
            for reply in active.replies {
                reply.send(Err(DbError::Checkpoint(reason.clone())));
            }
            return;
        }
        for reply in active.replies {
            reply.send(Ok(result.clone()));
        }
    }

    fn fail_manual_attempt(&mut self, attempt: CheckpointAttempt, error: impl Into<String>) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        let error = error.into();
        for reply in active.replies {
            reply.send(Err(DbError::Checkpoint(error.clone())));
        }
    }

    fn fail_all_manual(&mut self, error: &str) {
        self.fail_waiting_manual(error);
        if let Some(active) = self.manual_active.take() {
            for reply in active.replies {
                reply.send(Err(DbError::Checkpoint(error.to_owned())));
            }
        }
    }

    async fn cancel_pending_barrier_for_stop(
        &mut self,
        callback: &mut impl PipelineCallback,
        reason: &str,
    ) {
        let was_active = self.pending_barrier.active;
        let attempt = self.pending_barrier.take_active_attempt();
        self.barrier_seen.clear();

        match attempt {
            Some(attempt) => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason,
                    "abandoning checkpoint interrupted before source alignment"
                );
                if callback.is_leader() {
                    callback.abandon_checkpoint_attempt(attempt, reason).await;
                }
                self.fail_manual_attempt(
                    attempt,
                    format!("manual checkpoint was interrupted: {reason}"),
                );
            }
            None if was_active => {
                callback.record_checkpoint_admission_failure(
                    "active source barrier had no exact reserved attempt during shutdown",
                );
            }
            None => {}
        }
    }

    /// Settle every captured durable tail before source or sink lifecycle teardown.
    ///
    /// The counter is claimed synchronously before a tail is spawned. A tail sends its terminal
    /// completion before dropping the claim, so waiting for zero and then draining the channel
    /// preserves exact source acknowledgements, public barriers, and manual replies. The tick
    /// handles tails that legitimately terminate without a completion (cluster followers) and
    /// avoids relying on a channel event after the atomic reaches zero.
    async fn settle_checkpoint_tails(
        &mut self,
        callback: &mut impl PipelineCallback,
    ) -> Option<String> {
        let mut continuation_fault = None;
        let deadline = Instant::now() + SHUTDOWN_CHECKPOINT_TAIL_TIMEOUT;
        let mut tails_aborted = false;
        loop {
            self.drain_manual_requests();
            self.fail_waiting_manual("pipeline is stopping; no new checkpoint can be admitted");

            while let Some(completion) = self
                .checkpoint_complete_rx
                .as_ref()
                .and_then(|rx| rx.try_recv().ok())
            {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    continuation_fault.get_or_insert(error);
                }
            }

            if self.checkpoint_in_flight.load(Ordering::Acquire) == 0 {
                break;
            }

            if Instant::now() >= deadline {
                let pending = self.checkpoint_in_flight.load(Ordering::Acquire);
                let reason = format!(
                    "checkpoint durable-tail shutdown drain timed out after \
                     {SHUTDOWN_CHECKPOINT_TAIL_TIMEOUT:?} with {pending} attempt(s) still in \
                     flight; cancelling tails for recovery"
                );
                continuation_fault.get_or_insert(reason);
                if let Err(error) = callback.settle_checkpoint_tail_tasks(true).await {
                    continuation_fault.get_or_insert(error);
                }
                tails_aborted = true;
                break;
            }

            let completion = if let Some(rx) = self.checkpoint_complete_rx.as_mut() {
                let tick = SHUTDOWN_COMPLETION_TICK
                    .min(deadline.saturating_duration_since(Instant::now()));
                match tokio::time::timeout(tick, rx.recv()).await {
                    Ok(Ok(completion)) => Some(completion),
                    Ok(Err(_)) => {
                        tokio::time::sleep(SHUTDOWN_COMPLETION_TICK).await;
                        None
                    }
                    Err(_) => None,
                }
            } else {
                tokio::time::sleep(SHUTDOWN_COMPLETION_TICK).await;
                None
            };
            if let Some(completion) = completion {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    continuation_fault.get_or_insert(error);
                }
            }
        }

        if !tails_aborted {
            if let Err(error) = callback.settle_checkpoint_tail_tasks(false).await {
                continuation_fault.get_or_insert(error);
            }
        }

        // A sender enqueues its completion before dropping the in-flight guard. Once the counter
        // reaches zero, drain the enqueue that may have raced with our last atomic load.
        while let Some(completion) = self
            .checkpoint_complete_rx
            .as_ref()
            .and_then(|rx| rx.try_recv().ok())
        {
            if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                continuation_fault.get_or_insert(error);
            }
        }
        self.drain_manual_requests();
        self.fail_all_manual("pipeline stopped before the checkpoint reached a terminal result");
        continuation_fault
    }

    fn handle_checkpoint_completion(
        &mut self,
        completion: CheckpointCompletion,
        callback: &mut impl PipelineCallback,
    ) -> Option<String> {
        let attempt = completion.attempt();
        match completion {
            CheckpointCompletion::Committed {
                result,
                source_checkpoints,
                ..
            } => {
                let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
                if !result.success || completed != attempt {
                    let reason = format!(
                        "checkpoint terminal identity mismatch: admitted epoch={} id={}, \
                         completed epoch={} id={} success={}",
                        attempt.epoch,
                        attempt.checkpoint_id,
                        completed.epoch,
                        completed.checkpoint_id,
                        result.success,
                    );
                    self.fail_manual_attempt(attempt, &reason);
                    callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                } else {
                    let continuation_error = result.continuation_error().map(str::to_owned);
                    // Ordering is semantic: N reached its durable point, so source and public
                    // acknowledgements for N must be published even when N+1 cannot be opened.
                    self.broadcast_epoch_committed(attempt.epoch, &source_checkpoints);
                    callback.publish_barrier(attempt.epoch, attempt.checkpoint_id);
                    self.finish_manual_success(attempt, &result);
                    return continuation_error;
                }
            }
            CheckpointCompletion::Failed { error, .. } => {
                self.fail_manual_attempt(attempt, &error);
                callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
            }
        }
        None
    }

    /// Run the coordinator loop until shutdown or a fatal cycle fault.
    ///
    /// Cycle priority: (1) shutdown, (2) drain + SQL, (3) barrier alignment,
    /// (4) periodic checkpoint, (5) table polling, (6) barrier timeout.
    #[allow(clippy::too_many_lines)]
    pub async fn run<C: PipelineCallback>(mut self, mut callback: C) -> ExitReason {
        /// Maximum messages to drain per cycle before yielding for maintenance work.
        const MAX_DRAIN_PER_CYCLE: usize = 10_000;

        let injectors = self
            .source_handles
            .iter()
            .map(|h| (h.name.clone(), h.barrier_injector.clone()))
            .collect();
        callback.set_barrier_injectors(injectors);

        let batch_window = self.config.batch_window;
        let coordinated_commit_progress = self
            .coordinated_commit_admission
            .as_ref()
            .map(crate::checkpoint_coordinator::CoordinatedCommitAdmission::progress_notify);
        let mut barriers_buf: Vec<(usize, CheckpointBarrier, SourceCheckpoint)> = Vec::new();
        // Set by a fatal replay-guaranteed error; gates the open-epoch shutdown drain.
        let mut fault: Option<String> = None;

        loop {
            // At the coordinated external hard bound, stop consuming source data entirely.
            // The bounded source channel then propagates backpressure to connector polling. A
            // one-batch-per-idle-tick trickle would still let the open epoch grow without bound
            // while an external catalog is unavailable.
            let external_commit_paused = self
                .coordinated_commit_admission
                .as_ref()
                .is_some_and(|admission| !admission.can_admit());
            // Wait for data, shutdown, or idle timeout.
            let msg = tokio::select! {
                biased;
                () = self.shutdown.notified() => break,
                // A background persist finished (in-flight guard ensures epoch order).
                Some(completion) = async {
                    if let Some(ref mut rx) = self.checkpoint_complete_rx {
                        rx.recv().await.ok()
                    } else {
                        futures::future::pending::<Option<CheckpointCompletion>>().await
                    }
                } => {
                    if let Some(error) = self.handle_checkpoint_completion(completion, &mut callback) {
                        fault = Some(error);
                        break;
                    }
                    continue;
                }
                Some(reply) = async {
                    if let Some(ref mut rx) = self.force_ckpt_rx {
                        rx.recv().await.ok()
                    } else {
                        futures::future::pending::<Option<ForceCheckpointReply>>().await
                    }
                } => {
                    self.manual_waiting.push(reply);
                    None
                },
                () = async {
                    if let Some(notify) = coordinated_commit_progress.as_ref() {
                        notify.notified().await;
                    } else {
                        futures::future::pending::<()>().await;
                    }
                } => None,
                msg = self.rx.recv(), if !external_commit_paused => {
                    match msg {
                        Ok(m) => {
                            if !batch_window.is_zero() {
                                tokio::time::sleep(batch_window).await;
                            }
                            Some(m)
                        }
                        Err(_) => break, // All senders dropped
                    }
                }
                () = tokio::time::sleep(IDLE_TIMEOUT) => None,
            };

            // A progress wake is edge-triggered; recompute the gate on the next loop before
            // touching deferred/open-epoch data. Completion branches above already `continue`.
            if external_commit_paused && msg.is_none() {
                continue;
            }

            self.source_batches_buf.clear();
            self.reset_barrier_seen_for_cycle();
            self.discard_pending_offsets();
            barriers_buf.clear();
            let mut cycle_events: u64 = 0;
            let cycle_start = Instant::now();

            // Drain post-barrier messages deferred from the previous cycle.
            let deferred = std::mem::take(&mut self.post_barrier_buf);
            for deferred_msg in deferred {
                self.process_msg(
                    deferred_msg,
                    &mut callback,
                    &mut barriers_buf,
                    &mut cycle_events,
                );
            }

            let had_data = msg.is_some();
            if let Some(first_msg) = msg {
                let first_msg = match first_msg {
                    SourceMsg::Fault { source, error } => {
                        fault = Some(format!("source '{source}' fault: {error}"));
                        break;
                    }
                    message => message,
                };
                self.process_msg(
                    first_msg,
                    &mut callback,
                    &mut barriers_buf,
                    &mut cycle_events,
                );
            }

            // Coalesce additional buffered messages; stop at count, time budget, or backpressure.
            let mut drain_count = 0;
            let drain_budget_ns = self.config.drain_budget_ns;
            // When over budget, skip the coalescing drain entirely. `is_backpressured()` bumps a
            // counter so it's only called on active wakeups, not idle timeouts.
            let state_paused = callback.state_over_budget();
            let backpressured = state_paused || (had_data && callback.is_backpressured());
            if backpressured {
                tracing::debug!("operator graph backpressured — skipping drain");
            }
            #[allow(clippy::cast_possible_truncation)]
            while !backpressured
                && drain_count < MAX_DRAIN_PER_CYCLE
                && (cycle_start.elapsed().as_nanos() as u64) < drain_budget_ns
            {
                match self.rx.try_recv() {
                    Ok(SourceMsg::Fault { source, error }) => {
                        fault = Some(format!("source '{source}' fault: {error}"));
                        break;
                    }
                    Ok(msg) => {
                        self.process_msg(msg, &mut callback, &mut barriers_buf, &mut cycle_events);
                        drain_count += 1;
                    }
                    Err(_) => break,
                }
            }
            if fault.is_some() {
                self.discard_pending_offsets();
                break;
            }

            for (name, batch) in self.pending_watermark_batches.drain(..) {
                callback.extract_watermark(&name, &batch);
            }

            // Skip idle-watermark ticking while intake is budget-paused: a paused source
            // isn't actually idle and demoting it would advance the watermark past its
            // queued rows, dropping them as late on resume.
            if !state_paused {
                callback.tick_idle_watermark();
            }

            // Run on idle wakeups too when operators have deferred input; otherwise
            // deferred data stalls once the source goes quiet.
            if !self.source_batches_buf.is_empty() || callback.has_deferred_input() {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(out) => {
                        // Exactly-once / coordinated recovery rewinds the whole pipeline, so
                        // don't partial-commit siblings — recover instead.
                        if out.any_failed && callback.fault_on_cycle_error() {
                            self.discard_pending_offsets();
                            tracing::error!(
                                "[LDB-3021] failure domain faulted; faulting for recovery"
                            );
                            fault = Some("isolated domain fault (exactly-once)".to_string());
                            break;
                        }
                        // Best effort: healthy domains commit; the faulted domain is dropped.
                        self.commit_pending_offsets_except(&out.failed_sources);
                        callback.update_mv_stores(&out.results);
                        callback.push_to_streams(&out.results);
                        callback.write_to_sinks(&out.results).await;
                        if out.any_failed {
                            callback.note_cycle_error();
                            tracing::warn!(
                                "[LDB-3020] failure domain dropped (best-effort: continuing)"
                            );
                        }
                    }
                    Err(e) => {
                        self.discard_pending_offsets();
                        match e {
                            // Shutdown already signaled; restarting would just re-trip it.
                            CycleError::Halt(msg) => {
                                tracing::warn!(reason = %msg, "[LDB-3022] cycle halted");
                            }
                            // Continuing would drop the drained rows (EO gap), so fault for
                            // recovery under exactly-once or coordinated recovery.
                            CycleError::Fatal(msg) if callback.fault_on_cycle_error() => {
                                tracing::error!(
                                    error = %msg,
                                    "[LDB-3021] fatal SQL cycle error; faulting for recovery"
                                );
                                fault = Some(msg);
                                break;
                            }
                            // Best effort: drop the bad cycle and continue.
                            CycleError::Fatal(msg) => {
                                callback.note_cycle_error();
                                tracing::warn!(
                                    error = %msg,
                                    "[LDB-3020] SQL cycle error (best-effort: continuing)"
                                );
                            }
                        }
                    }
                }
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                callback.record_cycle(cycle_events, 0, elapsed_ns);

                if elapsed_ns >= self.config.cycle_budget_ns {
                    tracing::debug!(
                        elapsed_ms = elapsed_ns / 1_000_000,
                        budget_ms = self.config.cycle_budget_ns / 1_000_000,
                        "cycle budget exceeded — proceeding to maintenance"
                    );
                }
            }

            #[allow(clippy::cast_possible_truncation)]
            let cycle_elapsed_ns = cycle_start.elapsed().as_nanos() as u64;

            let bg_start = Instant::now();
            let bg_budget = self.config.background_budget_ns;

            // Barriers are cheap (O(num_sources) lookups) and must not be skipped.
            for (source_idx, barrier, cp) in &barriers_buf {
                self.handle_barrier(*source_idx, barrier, cp, &mut callback)
                    .await;
            }

            // A stop notification may arrive while SQL or barrier work is running. Observe its
            // stored permit before admission so this cycle cannot originate a fresh attempt on
            // its way out.
            if self.shutdown.notified().now_or_never().is_some() {
                break;
            }

            // Never reserve or inject another attempt after an already-observed terminal fault.
            if let Some(reason) = callback.take_sink_fault() {
                self.discard_pending_offsets();
                tracing::error!(
                    reason = %reason,
                    "[LDB-3024] sink or checkpoint failure; faulting for recovery"
                );
                fault = Some(reason);
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            if (bg_start.elapsed().as_nanos() as u64) < bg_budget {
                self.maybe_checkpoint(&mut callback).await;
            }

            // An exactly-once sink write failure poisons the epoch and aborts its transaction but
            // never faulted the pipeline, so the next checkpoint would seal offsets past the
            // dropped rows. Escalate to a fault so recovery replays them (CP-4). Barriers and the
            // periodic checkpoint above drain sink events, so the flag is current here.
            if let Some(reason) = callback.take_sink_fault() {
                self.discard_pending_offsets();
                tracing::error!(
                    reason = %reason,
                    "[LDB-3024] sink or checkpoint failure; faulting for recovery"
                );
                fault = Some(reason);
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            if (bg_start.elapsed().as_nanos() as u64) < bg_budget {
                callback.maybe_demote_state().await;
            }

            // Table polling is the lowest-priority background work.
            #[allow(clippy::cast_possible_truncation)]
            let bg_elapsed = bg_start.elapsed().as_nanos() as u64;
            if cycle_elapsed_ns < self.config.cycle_budget_ns && bg_elapsed < bg_budget {
                callback.poll_tables().await;
            } else {
                tracing::debug!("skipping poll_tables (budget exhausted)");
            }

            // DDL after checkpoint so newly added queries don't appear in the same snapshot.
            while let Ok(msg) = self.control_rx.try_recv() {
                callback.apply_control(msg);
            }

            if self.pending_barrier.active
                && self.pending_barrier.started_at.elapsed() > self.config.checkpoint_timeout
            {
                let attempt = self.pending_barrier.take_active_attempt();
                if let Some(attempt) = attempt {
                    tracing::warn!(
                        checkpoint_id = attempt.checkpoint_id,
                        epoch = attempt.epoch,
                        "Barrier alignment timeout — abandoning checkpoint"
                    );
                    if callback.is_leader() {
                        callback
                            .abandon_checkpoint_attempt(attempt, "source barrier alignment timeout")
                            .await;
                    }
                    callback.record_checkpoint_failure(
                        attempt.checkpoint_id,
                        "source barrier alignment timeout",
                    );
                    self.fail_manual_attempt(
                        attempt,
                        "manual checkpoint source barrier alignment timed out",
                    );
                } else {
                    callback.record_checkpoint_admission_failure(
                        "barrier alignment was active without a reserved attempt",
                    );
                }
            }
        }

        // Stop is an admission fence. Cancel alignment before waiting for captured tails: an
        // unaligned attempt has no tail and therefore cannot make the in-flight counter progress.
        self.drain_manual_requests();
        self.fail_waiting_manual("pipeline is stopping; no new checkpoint can be admitted");
        let interrupted_reason = if fault.is_some() {
            "pipeline fault interrupted source barrier alignment"
        } else {
            "pipeline shutdown interrupted source barrier alignment"
        };
        self.cancel_pending_barrier_for_stop(&mut callback, interrupted_reason)
            .await;

        // Captured tails own durable state and may still need to publish source acknowledgements.
        // Settling them while sources and sinks remain open prevents close from racing commit.
        if let Some(error) = self.settle_checkpoint_tails(&mut callback).await {
            fault.get_or_insert(error);
        }
        if let Some(reason) = callback.take_sink_fault() {
            fault.get_or_insert(reason);
        }

        for handle in &self.source_handles {
            handle.shutdown.notify_one();
        }

        self.source_batches_buf.clear();
        self.pending_watermark_batches.clear();
        self.barrier_seen.clear();
        self.discard_pending_offsets();
        let mut drain_events = 0_u64;

        // Deferred batches are open-epoch data. Once their barrier attempt is cancelled, process
        // them normally; a shutdown barrier has no power to defer anything further.
        for msg in std::mem::take(&mut self.post_barrier_buf) {
            if fault.is_none() {
                if let Some(reason) =
                    self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                {
                    fault = Some(reason);
                    self.source_batches_buf.clear();
                    self.pending_watermark_batches.clear();
                    self.discard_pending_offsets();
                }
            }
        }

        // Closing the watch senders releases source tasks after they consume any exact commit
        // broadcast settled above. Keep draining their data channel while they finish so a task
        // blocked on a full channel cannot deadlock shutdown.
        let mut stopping_sources = Vec::with_capacity(self.source_handles.len());
        for handle in std::mem::take(&mut self.source_handles) {
            let SourceHandle {
                name,
                join,
                epoch_committed_tx,
                ..
            } = handle;
            drop(epoch_committed_tx);
            stopping_sources.push((name, join));
        }

        let source_deadline = Instant::now() + SHUTDOWN_JOIN_TIMEOUT;
        while stopping_sources.iter().any(|(_, join)| !join.is_finished())
            && Instant::now() < source_deadline
        {
            while let Ok(msg) = self.rx.try_recv() {
                if fault.is_none() {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                    {
                        fault = Some(reason);
                        self.source_batches_buf.clear();
                        self.pending_watermark_batches.clear();
                        self.discard_pending_offsets();
                    }
                }
            }

            if stopping_sources.iter().all(|(_, join)| join.is_finished()) {
                break;
            }

            let tick = SHUTDOWN_COMPLETION_TICK
                .min(source_deadline.saturating_duration_since(Instant::now()));
            match tokio::time::timeout(tick, self.rx.recv()).await {
                Ok(Ok(msg)) if fault.is_none() => {
                    if let Some(reason) =
                        self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                    {
                        fault = Some(reason);
                        self.source_batches_buf.clear();
                        self.pending_watermark_batches.clear();
                        self.discard_pending_offsets();
                    }
                }
                Ok(Ok(_) | Err(_)) | Err(_) => {}
            }
        }

        for (name, join) in stopping_sources {
            if !join.is_finished() {
                tracing::warn!(
                    source = %name,
                    "source task did not exit within shutdown budget; aborting"
                );
                join.abort();
            }
            match join.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => tracing::warn!(source = %name, error = ?e, "source task panicked"),
            }
        }

        // Capture messages enqueued immediately before the last source task exited.
        while let Ok(msg) = self.rx.try_recv() {
            if fault.is_none() {
                if let Some(reason) =
                    self.process_shutdown_msg(msg, &mut callback, &mut drain_events)
                {
                    fault = Some(reason);
                    self.source_batches_buf.clear();
                    self.pending_watermark_batches.clear();
                    self.discard_pending_offsets();
                }
            }
        }

        if fault.is_none() {
            for (name, batch) in self.pending_watermark_batches.drain(..) {
                callback.extract_watermark(&name, &batch);
            }
            callback.tick_idle_watermark();
            if !self.source_batches_buf.is_empty() || callback.has_deferred_input() {
                let cycle_start = Instant::now();
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(out) if out.any_failed && callback.fault_on_cycle_error() => {
                        self.discard_pending_offsets();
                        fault = Some(
                            "isolated domain fault during shutdown drain under replay guarantee"
                                .to_string(),
                        );
                    }
                    Ok(out) => {
                        self.commit_pending_offsets_except(&out.failed_sources);
                        callback.update_mv_stores(&out.results);
                        callback.push_to_streams(&out.results);
                        callback.write_to_sinks(&out.results).await;
                        if out.any_failed {
                            callback.note_cycle_error();
                        }
                    }
                    Err(CycleError::Halt(reason)) => {
                        self.discard_pending_offsets();
                        tracing::warn!(%reason, "[LDB-3022] cycle halted during shutdown drain");
                    }
                    Err(CycleError::Fatal(reason)) if callback.fault_on_cycle_error() => {
                        self.discard_pending_offsets();
                        fault = Some(format!(
                            "fatal SQL cycle error during shutdown drain: {reason}"
                        ));
                    }
                    Err(CycleError::Fatal(reason)) => {
                        self.discard_pending_offsets();
                        callback.note_cycle_error();
                        tracing::warn!(%reason, "[LDB-3020] SQL cycle error during shutdown drain");
                    }
                }
                #[allow(clippy::cast_possible_truncation)]
                callback.record_cycle(drain_events, 0, cycle_start.elapsed().as_nanos() as u64);
            }
        }

        // Captured tails are settled and no more checkpoints can be admitted. Keep sink actors
        // open while the designated committer publishes every already-sealed exact cut. Open-
        // epoch rows above are intentionally excluded and replay after restart.
        if fault.is_none() {
            if let Err(error) = self.drain_coordinated_commits().await {
                if callback.fault_on_cycle_error() {
                    fault = Some(error);
                } else {
                    callback.note_cycle_error();
                    tracing::warn!(%error, "coordinated commit drain failed during shutdown");
                }
            }
        }

        // No final snapshot is synthesized: open-epoch rows deliberately replay from the last
        // committed cut. Sink close must confirm queued writes and abort any uncommitted
        // transactional epoch. A replay guarantee turns every close failure into a recovery fault;
        // best-effort reports it but may still stop normally.
        if let Err(close_error) = callback.close_sinks().await {
            if callback.fault_on_cycle_error() {
                match fault.as_mut() {
                    Some(existing) => {
                        existing.push_str("; sink shutdown also failed: ");
                        existing.push_str(&close_error);
                    }
                    None => fault = Some(format!("sink shutdown failed: {close_error}")),
                }
            } else {
                callback.note_cycle_error();
                tracing::warn!(
                    error = %close_error,
                    "sink shutdown failed under best-effort delivery"
                );
            }
        }

        fault.map_or(ExitReason::Shutdown, ExitReason::Fault)
    }

    async fn drain_coordinated_commits(&mut self) -> Result<(), String> {
        let Some(admission) = self.coordinated_commit_admission.as_ref() else {
            return Ok(());
        };
        let deadline = Instant::now() + COORDINATED_COMMIT_SHUTDOWN_TIMEOUT;
        loop {
            let (known, pending, _) = admission.state();
            if known && pending == 0 {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "coordinated external commit drain timed out after \
                     {COORDINATED_COMMIT_SHUTDOWN_TIMEOUT:?} (lag_known={known}, \
                     pending={pending}); durable markers remain for recovery"
                ));
            }
            let progress = admission.progress_notify();
            let notified = progress.notified();
            tokio::pin!(notified);
            admission.wake_committer();
            if tokio::time::timeout(remaining, &mut notified)
                .await
                .is_err()
            {
                let (known, pending, _) = admission.state();
                return Err(format!(
                    "coordinated external commit drain timed out after \
                     {COORDINATED_COMMIT_SHUTDOWN_TIMEOUT:?} (lag_known={known}, \
                     pending={pending}); durable markers remain for recovery"
                ));
            }
        }
    }

    fn stage_batch(
        &mut self,
        source_idx: usize,
        batch: RecordBatch,
        checkpoint: SourceCheckpoint,
        callback: &mut impl PipelineCallback,
        cycle_events: &mut u64,
    ) {
        if source_idx < self.pending_offsets.len() {
            self.pending_offsets[source_idx] = Some(checkpoint);
        }

        if let Some(name) = self.source_names.get(source_idx) {
            #[allow(clippy::cast_possible_truncation)]
            {
                *cycle_events += batch.num_rows() as u64;
            }
            // Filter against the pre-drain watermark. Extraction is deferred until after all
            // batches are filtered so one batch cannot make the next batch appear late.
            if let Some(filtered) = callback.filter_late_rows(name, &batch) {
                self.source_batches_buf
                    .entry(Arc::clone(name))
                    .or_default()
                    .push(filtered);
            }
            self.pending_watermark_batches
                .push((Arc::clone(name), batch));
        }
    }

    /// Process one source message. Post-barrier batches are diverted to `post_barrier_buf`.
    fn process_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut impl PipelineCallback,
        barriers: &mut Vec<(usize, CheckpointBarrier, SourceCheckpoint)>,
        cycle_events: &mut u64,
    ) {
        match msg {
            SourceMsg::Batch {
                source_idx,
                batch,
                checkpoint,
            } => {
                if self.barrier_seen.contains(&source_idx) {
                    self.post_barrier_buf.push(SourceMsg::Batch {
                        source_idx,
                        batch,
                        checkpoint,
                    });
                    return;
                }
                self.stage_batch(source_idx, batch, checkpoint, callback, cycle_events);
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
                checkpoint,
            } => {
                tracing::debug!(
                    source_idx,
                    checkpoint_id = barrier.checkpoint_id,
                    "coordinator received source barrier"
                );
                self.barrier_seen.insert(source_idx);
                barriers.push((source_idx, barrier, checkpoint));
            }
            SourceMsg::Fault { .. } => {
                // Faults are intercepted before normal message processing.
            }
        }
    }

    /// Process a message after checkpoint admission has closed.
    ///
    /// No shutdown checkpoint exists, so every remaining batch belongs to an uncommitted open
    /// epoch. Barriers are control records for attempts that have already been cancelled and are
    /// ignored; they must never defer a later batch back into `post_barrier_buf`.
    fn process_shutdown_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut impl PipelineCallback,
        cycle_events: &mut u64,
    ) -> Option<String> {
        match msg {
            SourceMsg::Batch {
                source_idx,
                batch,
                checkpoint,
            } => {
                self.stage_batch(source_idx, batch, checkpoint, callback, cycle_events);
                None
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
                ..
            } => {
                tracing::debug!(
                    source_idx,
                    checkpoint_id = barrier.checkpoint_id,
                    epoch = barrier.epoch,
                    "ignoring checkpoint barrier during shutdown drain"
                );
                None
            }
            SourceMsg::Fault { source, error } => {
                let reason = format!("source '{source}' fault during shutdown drain: {error}");
                if callback.fault_on_cycle_error() {
                    Some(reason)
                } else {
                    callback.note_cycle_error();
                    tracing::warn!(reason, "source fault dropped during best-effort shutdown");
                    None
                }
            }
        }
    }

    /// Per-source committed offsets keyed by source name, reflecting the last successful cycle.
    /// Follower control uses this stable cut rather than advancing without source positions.
    fn current_source_offsets(&self) -> FxHashMap<String, SourceCheckpoint> {
        self.committed_offsets
            .iter()
            .enumerate()
            .filter_map(|(idx, cp)| {
                cp.as_ref().and_then(|c| {
                    self.source_names
                        .get(idx)
                        .map(|name| (name.to_string(), c.clone()))
                })
            })
            .collect()
    }

    /// Merge staged offsets into `committed_offsets` after a successful cycle.
    fn commit_pending_offsets(&mut self) {
        for (i, pending) in self.pending_offsets.iter_mut().enumerate() {
            if let Some(cp) = pending.take() {
                self.committed_offsets[i] = Some(cp);
            }
        }
    }

    /// Commit staged offsets for sources whose failure domain succeeded, discarding those
    /// named in `failed` so the faulted domain replays them on its next cycle (or recovery).
    fn commit_pending_offsets_except(&mut self, failed: &FxHashSet<Arc<str>>) {
        if failed.is_empty() {
            self.commit_pending_offsets();
            return;
        }
        for (i, pending) in self.pending_offsets.iter_mut().enumerate() {
            let in_failed_domain = self
                .source_names
                .get(i)
                .is_some_and(|name| failed.contains(name));
            if in_failed_domain {
                *pending = None;
            } else if let Some(cp) = pending.take() {
                self.committed_offsets[i] = Some(cp);
            }
        }
    }

    /// Discard staged offsets when `execute_cycle` fails.
    fn discard_pending_offsets(&mut self) {
        for slot in &mut self.pending_offsets {
            *slot = None;
        }
    }

    /// Reset per-cycle barrier tracking at cycle start. While a multi-source barrier is
    /// still aligning, re-arm deferral for sources that already passed it: `barrier_seen`
    /// is per-cycle, so without this a post-barrier (epoch N+1) batch re-drained from
    /// `post_barrier_buf` folds into epoch-N state while the manifest records that source's
    /// offset at-barrier → duplicates on recovery (CP-1).
    fn reset_barrier_seen_for_cycle(&mut self) {
        self.barrier_seen.clear();
        if self.pending_barrier.active {
            self.barrier_seen
                .extend(self.pending_barrier.sources_aligned.iter().copied());
        }
    }

    /// Handle a barrier from a source.
    async fn handle_barrier(
        &mut self,
        source_idx: usize,
        barrier: &CheckpointBarrier,
        barrier_checkpoint: &SourceCheckpoint,
        callback: &mut impl PipelineCallback,
    ) {
        let barrier_attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
        if !self.pending_barrier.active && !callback.is_leader() {
            self.pending_barrier
                .reset(barrier_attempt, self.source_handles.len());
        }

        if !self.pending_barrier.active || self.pending_barrier.attempt != Some(barrier_attempt) {
            return;
        }

        if let Some(name) = self.source_names.get(source_idx) {
            self.pending_barrier
                .source_checkpoints
                .insert(name.to_string(), barrier_checkpoint.clone());
        }

        self.pending_barrier.sources_aligned.insert(source_idx);

        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            let checkpoints = std::mem::take(&mut self.pending_barrier.source_checkpoints);
            // Clone for fan-out so each source gets the exact checkpoint that was persisted.
            let fan_out = checkpoints.clone();
            let attempt = barrier_attempt;
            let attempt_started = self.pending_barrier.started_at;
            self.pending_barrier.clear();
            let outcome = callback
                .checkpoint_with_barrier(checkpoints, attempt, attempt_started)
                .await;
            match outcome {
                BarrierOutcome::Committed(epoch) => {
                    if epoch == attempt.epoch {
                        self.broadcast_epoch_committed(epoch, &fan_out);
                        // Wire barrier = durable attempt.
                        callback.publish_barrier(epoch, attempt.checkpoint_id);
                        self.finish_manual_success(
                            attempt,
                            &crate::checkpoint_coordinator::CheckpointResult {
                                success: true,
                                checkpoint_id: attempt.checkpoint_id,
                                epoch: attempt.epoch,
                                duration: self.pending_barrier.started_at.elapsed(),
                                error: None,
                            },
                        );
                    } else {
                        let reason = format!(
                            "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                            attempt.epoch
                        );
                        if callback.is_leader() {
                            callback.abandon_checkpoint_attempt(attempt, &reason).await;
                        }
                        callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                        self.fail_manual_attempt(attempt, reason);
                    }
                }
                BarrierOutcome::Async => {
                    self.last_checkpoint = Instant::now();
                }
                BarrierOutcome::Skipped(reason) => {
                    tracing::debug!(
                        checkpoint_id = attempt.checkpoint_id,
                        epoch = attempt.epoch,
                        reason = %reason,
                        "barrier checkpoint skipped"
                    );
                    if callback.is_leader() {
                        callback
                            .abandon_checkpoint_attempt(attempt, &reason.to_string())
                            .await;
                    }
                    self.fail_manual_attempt(
                        attempt,
                        format!("manual checkpoint skipped: {reason}"),
                    );
                }
                BarrierOutcome::Failed => {
                    if callback.is_leader() {
                        callback
                            .abandon_checkpoint_attempt(
                                attempt,
                                "barrier-aligned checkpoint failed before durable tail",
                            )
                            .await;
                    }
                    callback.record_checkpoint_failure(
                        attempt.checkpoint_id,
                        "barrier-aligned checkpoint failed",
                    );
                    self.fail_manual_attempt(
                        attempt,
                        "manual barrier-aligned checkpoint failed before the durable tail",
                    );
                    tracing::warn!(
                        checkpoint_id = attempt.checkpoint_id,
                        epoch = attempt.epoch,
                        "barrier checkpoint failed"
                    );
                }
            }
            self.last_checkpoint = Instant::now();
        }
    }

    fn checkpoint_capacity_available(&self) -> bool {
        if self.pending_barrier.active
            || self.checkpoint_in_flight.load(Ordering::Acquire) >= self.max_in_flight_epochs
        {
            return false;
        }
        let staged_bytes = self.staged_bytes.load(Ordering::Acquire);
        if staged_bytes >= self.max_staged_bytes {
            warn_staged_cap_throttled(staged_bytes, self.max_staged_bytes);
            return false;
        }
        true
    }

    async fn checkpoint_admission(
        &mut self,
        callback: &mut impl PipelineCallback,
    ) -> Option<CheckpointAdmission> {
        // Requests arriving after a manual attempt was admitted belong to a later cut. Never let
        // an intervening periodic attempt consume them or attach them to the active attempt.
        if !self.manual_waiting.is_empty() && self.manual_active.is_some() {
            return None;
        }
        let manual = !self.manual_waiting.is_empty();
        if manual && !callback.is_leader() {
            self.fail_waiting_manual("only the cluster leader may admit a manual checkpoint");
            return None;
        }
        if manual && callback.is_recovering() {
            self.fail_waiting_manual(
                "manual checkpoint rejected while coordinated recovery is in progress",
            );
            return None;
        }

        let interval = callback.is_leader()
            && self
                .config
                .checkpoint_interval
                .is_some_and(|value| self.last_checkpoint.elapsed() >= value);
        // Hold only periodic work while assignment/recovery converges. The due interval remains
        // latched through `last_checkpoint`, so it fires immediately after convergence.
        if !manual
            && interval
            && (callback.is_recovering() || !callback.assignment_ready_for_checkpoint().await)
        {
            return None;
        }

        // Connector requests bypass assignment convergence, but never coordinated recovery.
        let connector = callback.is_leader()
            && !callback.is_recovering()
            && self
                .checkpoint_request_flags
                .iter()
                .any(|flag| flag.load(Ordering::Acquire));
        if !manual && !interval && !connector {
            return None;
        }
        if let Some(admission) = &self.coordinated_commit_admission {
            if !admission.can_admit() {
                let (known, pending, cap) = admission.state();
                warn_external_commit_cap_throttled(known, pending, cap);
                return None;
            }
        }
        // Clear connector-owned requests only after every admission gate passes.
        if connector {
            for flag in &self.checkpoint_request_flags {
                flag.store(false, Ordering::Release);
            }
        }
        Some(CheckpointAdmission { manual })
    }

    async fn admit_source_less_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
    ) {
        let attempt_started = Instant::now();
        let attempt = match callback.reserve_checkpoint_attempt(attempt_started).await {
            Ok(attempt) => attempt,
            Err(error) => {
                tracing::error!(
                    error = %error,
                    "durable source-less checkpoint attempt reservation failed"
                );
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        if admission.manual {
            self.activate_manual_attempt(attempt);
        }
        match callback
            .checkpoint_with_barrier(FxHashMap::default(), attempt, attempt_started)
            .await
        {
            BarrierOutcome::Committed(epoch) if epoch == attempt.epoch => {
                self.broadcast_epoch_committed(epoch, &FxHashMap::default());
                callback.publish_barrier(epoch, attempt.checkpoint_id);
                self.finish_manual_success(
                    attempt,
                    &crate::checkpoint_coordinator::CheckpointResult {
                        success: true,
                        checkpoint_id: attempt.checkpoint_id,
                        epoch: attempt.epoch,
                        duration: Duration::ZERO,
                        error: None,
                    },
                );
            }
            BarrierOutcome::Committed(epoch) => {
                let reason = format!(
                    "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                    attempt.epoch
                );
                if callback.is_leader() {
                    callback.abandon_checkpoint_attempt(attempt, &reason).await;
                }
                callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                self.fail_manual_attempt(attempt, reason);
            }
            BarrierOutcome::Async => {}
            BarrierOutcome::Skipped(reason) => {
                tracing::debug!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason = %reason,
                    "source-less checkpoint skipped"
                );
                if callback.is_leader() {
                    callback
                        .abandon_checkpoint_attempt(attempt, &reason.to_string())
                        .await;
                }
                self.fail_manual_attempt(attempt, format!("manual checkpoint skipped: {reason}"));
            }
            BarrierOutcome::Failed => {
                if callback.is_leader() {
                    callback
                        .abandon_checkpoint_attempt(
                            attempt,
                            "source-less checkpoint failed before durable tail",
                        )
                        .await;
                }
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    "source-less checkpoint failed",
                );
                self.fail_manual_attempt(
                    attempt,
                    "manual source-less checkpoint failed before the durable tail",
                );
                tracing::warn!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "source-less checkpoint failed"
                );
            }
        }
        self.last_checkpoint = Instant::now();
    }

    async fn admit_source_barrier_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
    ) {
        if self
            .source_handles
            .iter()
            .any(|handle| !handle.barrier_injector.can_trigger())
        {
            tracing::debug!(
                "checkpoint admission deferred: a source barrier injector is still busy"
            );
            return;
        }

        let attempt_started = Instant::now();
        let attempt = match callback.reserve_checkpoint_attempt(attempt_started).await {
            Ok(attempt) => attempt,
            Err(error) => {
                tracing::error!(error = %error, "durable checkpoint attempt reservation failed");
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        if admission.manual {
            self.activate_manual_attempt(attempt);
        }
        self.pending_barrier
            .reset(attempt, self.source_handles.len());
        // Attempt time includes reservation, alignment, capture, quorum, and publication.
        self.pending_barrier.started_at = attempt_started;
        let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);

        for handle in &self.source_handles {
            if !handle.barrier_injector.trigger(barrier) {
                self.pending_barrier.clear();
                if callback.is_leader() {
                    callback
                        .abandon_checkpoint_attempt(
                            attempt,
                            "source barrier injection was rejected after preflight",
                        )
                        .await;
                }
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    "source barrier injection was rejected after preflight",
                );
                self.fail_manual_attempt(
                    attempt,
                    "manual checkpoint source barrier injection was rejected after preflight",
                );
                return;
            }
        }
    }

    /// Trigger a periodic, connector-requested, or manual checkpoint when admission permits.
    async fn maybe_checkpoint(&mut self, callback: &mut impl PipelineCallback) {
        self.drain_manual_requests();
        if !self.checkpoint_capacity_available() {
            return;
        }

        // Followers observe leader checkpoint control even when no local trigger is due.
        if let Some(epoch) = callback
            .service_checkpoint_control(self.current_source_offsets())
            .await
        {
            self.broadcast_epoch_committed(epoch, &FxHashMap::default());
        }
        let Some(admission) = self.checkpoint_admission(callback).await else {
            return;
        };
        if self.source_handles.is_empty() {
            self.admit_source_less_checkpoint(callback, &admission)
                .await;
        } else {
            self.admit_source_barrier_checkpoint(callback, &admission)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::callback::CycleOutcome;
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parking_lot::Mutex;
    use std::sync::Arc;

    /// Minimal mock callback for testing the coordinator loop.
    struct MockCallback {
        cycle_count: u32,
        attempt_to_reserve: CheckpointAttempt,
        reserve_error: Option<String>,
        reserve_calls: u64,
        control_checkpoint_calls: u64,
        control_checkpoint_call_audit: Arc<AtomicU64>,
        barrier_captures: Vec<(CheckpointAttempt, usize)>,
        leader: bool,
        abandoned_attempts: Arc<Mutex<Vec<(CheckpointAttempt, String)>>>,
        checkpoint_failures: Vec<(u64, String)>,
        barrier_outcome: Option<BarrierOutcome>,
        results: Vec<FxHashMap<Arc<str>, Vec<RecordBatch>>>,
        watermark: i64,
        /// Fail on this 1-based cycle number.
        fatal_at_cycle: Option<u32>,
        cycle_errors: Arc<AtomicU64>,
        /// Whether a fatal cycle error should fault (exactly-once) vs drop-and-continue.
        fault_on_error: bool,
        /// Returned once by `take_sink_fault` to simulate an exactly-once sink failure.
        sink_fault: Option<String>,
        /// Exact downstream checkpoint identities published by async completions.
        published_barriers: Arc<Mutex<Vec<(u64, u64)>>>,
        written_rows: Arc<AtomicU64>,
        published_barriers_observed_at_close: Arc<AtomicU64>,
        close_error: Option<String>,
    }

    impl MockCallback {
        fn new() -> Self {
            Self {
                cycle_count: 0,
                attempt_to_reserve: CheckpointAttempt::new(1, 1),
                reserve_error: None,
                reserve_calls: 0,
                control_checkpoint_calls: 0,
                control_checkpoint_call_audit: Arc::new(AtomicU64::new(0)),
                barrier_captures: Vec::new(),
                leader: true,
                abandoned_attempts: Arc::new(Mutex::new(Vec::new())),
                checkpoint_failures: Vec::new(),
                barrier_outcome: None,
                results: Vec::new(),
                watermark: 0,
                fatal_at_cycle: None,
                cycle_errors: Arc::new(AtomicU64::new(0)),
                fault_on_error: false,
                sink_fault: None,
                published_barriers: Arc::new(Mutex::new(Vec::new())),
                written_rows: Arc::new(AtomicU64::new(0)),
                published_barriers_observed_at_close: Arc::new(AtomicU64::new(0)),
                close_error: None,
            }
        }
    }

    impl PipelineCallback for MockCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<CycleOutcome, CycleError> {
            self.cycle_count += 1;
            if self.fatal_at_cycle == Some(self.cycle_count) {
                return Err(CycleError::Fatal(format!(
                    "injected fatal at cycle {}",
                    self.cycle_count
                )));
            }
            // Pass through source batches as results.
            let results: FxHashMap<Arc<str>, Vec<RecordBatch>> = source_batches
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            self.results.push(results.clone());
            Ok(CycleOutcome::clean(results))
        }

        fn note_cycle_error(&self) {
            self.cycle_errors.fetch_add(1, Ordering::SeqCst);
        }

        fn fault_on_cycle_error(&self) -> bool {
            self.fault_on_error
        }

        fn take_sink_fault(&mut self) -> Option<String> {
            self.sink_fault.take()
        }

        fn is_leader(&self) -> bool {
            self.leader
        }

        async fn reserve_checkpoint_attempt(
            &mut self,
            _attempt_started: Instant,
        ) -> Result<CheckpointAttempt, String> {
            self.reserve_calls += 1;
            match self.reserve_error.take() {
                Some(error) => Err(error),
                None => Ok(self.attempt_to_reserve),
            }
        }

        async fn abandon_checkpoint_attempt(&mut self, attempt: CheckpointAttempt, reason: &str) {
            self.abandoned_attempts
                .lock()
                .push((attempt, reason.to_owned()));
        }

        fn record_checkpoint_failure(&mut self, checkpoint_id: u64, reason: &str) {
            self.checkpoint_failures
                .push((checkpoint_id, reason.to_owned()));
        }

        fn push_to_streams(&self, _results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {}
        async fn write_to_sinks(&mut self, results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
            let rows = results
                .values()
                .flat_map(|batches| batches.iter())
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            self.written_rows
                .fetch_add(u64::try_from(rows).unwrap(), Ordering::SeqCst);
        }

        fn extract_watermark(&mut self, _source_name: &str, batch: &RecordBatch) {
            // Use row count as a simple watermark proxy.
            #[allow(clippy::cast_possible_wrap)]
            {
                self.watermark += batch.num_rows() as i64;
            }
        }

        fn filter_late_rows(&self, _source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
            Some(batch.clone())
        }

        fn current_watermark(&self) -> i64 {
            self.watermark
        }

        fn publish_barrier(&self, epoch: u64, checkpoint_id: u64) {
            self.published_barriers.lock().push((epoch, checkpoint_id));
        }

        async fn service_checkpoint_control(
            &mut self,
            _source_offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> Option<u64> {
            self.control_checkpoint_calls += 1;
            self.control_checkpoint_call_audit
                .fetch_add(1, Ordering::SeqCst);
            None
        }

        async fn checkpoint_with_barrier(
            &mut self,
            source_checkpoints: FxHashMap<String, SourceCheckpoint>,
            attempt: CheckpointAttempt,
            _attempt_started: Instant,
        ) -> BarrierOutcome {
            self.barrier_captures
                .push((attempt, source_checkpoints.len()));
            self.barrier_outcome
                .take()
                .unwrap_or(BarrierOutcome::Committed(attempt.epoch))
        }

        fn record_cycle(&self, _events: u64, _batches: u64, _elapsed_ns: u64) {}
        async fn poll_tables(&mut self) {}
        fn apply_control(&mut self, _msg: crate::pipeline::ControlMsg) {}

        async fn close_sinks(&mut self) -> Result<(), String> {
            let published = self.published_barriers.lock().len();
            self.published_barriers_observed_at_close
                .store(u64::try_from(published).unwrap(), Ordering::SeqCst);
            match self.close_error.take() {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
    }

    /// Build a source-less coordinator over a direct channel (bypasses source spawning).
    fn test_coordinator(
        rx: SourceMsgRx,
        control_rx: ControlMsgRx,
        shutdown: Arc<tokio::sync::Notify>,
        delivery_guarantee: DeliveryGuarantee,
        checkpoint_interval: Option<Duration>,
    ) -> StreamingCoordinator {
        StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval,
                delivery_guarantee,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown,
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        }
    }

    fn int_batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    fn checkpoint_at(position: u64) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("test_position", position.to_string());
        checkpoint
    }

    fn successful_checkpoint_result(
        attempt: CheckpointAttempt,
    ) -> crate::checkpoint_coordinator::CheckpointResult {
        crate::checkpoint_coordinator::CheckpointResult {
            success: true,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration: Duration::from_millis(1),
            error: None,
        }
    }

    #[tokio::test]
    async fn shutdown_drain_wakes_committer_and_waits_for_zero_exact_lag() {
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(1)),
        );
        let pending = Arc::new(AtomicU64::new(1));
        let known = Arc::new(AtomicBool::new(true));
        let admission = crate::checkpoint_coordinator::CoordinatedCommitAdmission::for_test(
            Arc::clone(&pending),
            known,
            4,
        );
        let wake = admission.committer_wakeup_for_test();
        let progress = admission.progress_notify();
        coordinator.coordinated_commit_admission = Some(admission);

        let worker = tokio::spawn(async move {
            wake.notified().await;
            pending.store(0, Ordering::Release);
            progress.notify_one();
        });
        tokio::time::timeout(
            Duration::from_millis(250),
            coordinator.drain_coordinated_commits(),
        )
        .await
        .expect("shutdown drain should be event-driven")
        .expect("zero exact lag should complete the drain");
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn external_commit_hard_bound_backpressures_source_consumption() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            None,
        );
        let pending = Arc::new(AtomicU64::new(1));
        let known = Arc::new(AtomicBool::new(true));
        let admission = crate::checkpoint_coordinator::CoordinatedCommitAdmission::for_test(
            Arc::clone(&pending),
            known,
            1,
        );
        let progress = admission.progress_notify();
        coordinator.coordinated_commit_admission = Some(admission);

        let callback = MockCallback::new();
        let written_rows = Arc::clone(&callback.written_rows);
        let join = tokio::spawn(async move { coordinator.run(callback).await });
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(7),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            written_rows.load(Ordering::Acquire),
            0,
            "source data must remain queued while the external hard bound is closed"
        );

        pending.store(0, Ordering::Release);
        progress.notify_one();
        tokio::time::timeout(Duration::from_millis(500), async {
            while written_rows.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source consumption should resume on exact external progress");

        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), join)
            .await
            .expect("coordinator must shut down")
            .unwrap();
        assert!(matches!(exit, ExitReason::Shutdown));
    }

    fn checkpoint_source_handle(
        name: &str,
    ) -> (SourceHandle, laminar_core::checkpoint::BarrierPollHandle) {
        let barrier_injector = CheckpointBarrierInjector::new();
        let barrier_handle = barrier_injector.handle();
        let (epoch_committed_tx, _epoch_committed_rx) = tokio::sync::watch::channel(None);
        (
            SourceHandle {
                name: Arc::from(name),
                shutdown: Arc::new(tokio::sync::Notify::new()),
                join: tokio::spawn(async {}),
                barrier_injector,
                epoch_committed_tx,
            },
            barrier_handle,
        )
    }

    fn admission_coordinator(source_handles: Vec<SourceHandle>) -> StreamingCoordinator {
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let source_names = source_handles
            .iter()
            .map(|handle| Arc::clone(&handle.name))
            .collect::<Vec<_>>();
        let source_count = source_handles.len();
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::new(tokio::sync::Notify::new()),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::ZERO),
        );
        coordinator.source_handles = source_handles;
        coordinator.source_names = source_names;
        coordinator.committed_offsets = vec![None; source_count];
        coordinator.pending_offsets = vec![None; source_count];
        coordinator
    }

    #[tokio::test]
    async fn sourced_pipeline_without_output_streams_has_one_periodic_barrier_path() {
        // `MockCallback` has no output-stream registrations. Admission must depend on the
        // coordinator's input handles, never on production callback `stream_sources`.
        let (source, poll) = checkpoint_source_handle("input-only");
        let mut coordinator = admission_coordinator(vec![source]);
        let mut callback = MockCallback::new();
        let reserved = CheckpointAttempt::new(101, 10_001);
        callback.attempt_to_reserve = reserved;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.control_checkpoint_calls, 1);
        assert_eq!(callback.reserve_calls, 1);
        assert!(callback.barrier_captures.is_empty());
        assert_eq!(poll.poll(), Some(CheckpointBarrier::new(101, 10_001)));
    }

    #[tokio::test]
    async fn coordinated_external_bound_defers_before_attempt_reservation() {
        let (source, poll) = checkpoint_source_handle("input-only");
        let mut coordinator = admission_coordinator(vec![source]);
        let pending = Arc::new(AtomicU64::new(0));
        let known = Arc::new(AtomicBool::new(false));
        coordinator.coordinated_commit_admission = Some(
            crate::checkpoint_coordinator::CoordinatedCommitAdmission::for_test(
                Arc::clone(&pending),
                Arc::clone(&known),
                2,
            ),
        );
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = CheckpointAttempt::new(101, 10_001);

        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 0, "unknown cursor state must gate");
        assert_eq!(poll.poll(), None);

        known.store(true, Ordering::Release);
        pending.store(2, Ordering::Release);
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 0, "the exact cap must gate");
        assert_eq!(poll.poll(), None);

        pending.store(1, Ordering::Release);
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(callback.reserve_calls, 1);
        assert_eq!(poll.poll(), Some(CheckpointBarrier::new(101, 10_001)));
    }

    #[tokio::test]
    async fn source_less_local_periodic_checkpoint_uses_exact_attempt_capture() {
        let mut coordinator = admission_coordinator(Vec::new());
        let mut callback = MockCallback::new();
        let reserved = CheckpointAttempt::new(102, 10_002);
        callback.attempt_to_reserve = reserved;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.control_checkpoint_calls, 1);
        assert_eq!(callback.reserve_calls, 1);
        assert_eq!(callback.barrier_captures, vec![(reserved, 0)]);
        assert!(callback.abandoned_attempts.lock().is_empty());
        assert_eq!(
            *callback.published_barriers.lock(),
            vec![(reserved.epoch, reserved.checkpoint_id)]
        );
    }

    #[tokio::test]
    async fn source_less_cluster_follower_never_originates_checkpoint_attempt() {
        let mut coordinator = admission_coordinator(Vec::new());
        let mut callback = MockCallback::new();
        callback.leader = false;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.control_checkpoint_calls, 1);
        assert_eq!(callback.reserve_calls, 0);
        assert!(callback.barrier_captures.is_empty());
    }

    #[tokio::test]
    async fn busy_source_injector_preflight_does_not_reserve_an_attempt() {
        let (busy_source, busy_poll) = checkpoint_source_handle("busy");
        let (idle_source, idle_poll) = checkpoint_source_handle("idle");
        let already_pending = CheckpointBarrier::new(71, 7);
        assert!(busy_source.barrier_injector.trigger(already_pending));

        let mut coordinator = admission_coordinator(vec![busy_source, idle_source]);
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = CheckpointAttempt::new(42, 9_001);

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(
            callback.reserve_calls, 0,
            "preflight must not burn a durable checkpoint ID while any injector is busy"
        );
        assert!(!coordinator.pending_barrier.active);
        assert_eq!(busy_poll.poll(), Some(already_pending));
        assert_eq!(idle_poll.poll(), None);
    }

    #[tokio::test]
    async fn admitted_checkpoint_injects_the_exact_durably_reserved_attempt() {
        let (source_0, poll_0) = checkpoint_source_handle("source-0");
        let (source_1, poll_1) = checkpoint_source_handle("source-1");
        let mut coordinator = admission_coordinator(vec![source_0, source_1]);
        let mut callback = MockCallback::new();
        let reserved = CheckpointAttempt::new(37, u64::from(u32::MAX) + 8_192);
        callback.attempt_to_reserve = reserved;

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.reserve_calls, 1);
        assert_eq!(coordinator.pending_barrier.attempt, Some(reserved));
        assert!(coordinator.pending_barrier.active);
        for injected in [poll_0.poll(), poll_1.poll()] {
            let injected = injected.expect("every preflighted source must receive the barrier");
            assert_eq!(injected.epoch, reserved.epoch);
            assert_eq!(injected.checkpoint_id, reserved.checkpoint_id);
        }
    }

    #[tokio::test]
    async fn manual_requests_coalesce_onto_one_new_exact_source_barrier() {
        let (source, poll) = checkpoint_source_handle("manual-source");
        let mut coordinator = admission_coordinator(vec![source]);
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(8);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);

        let (first_sender, first_completion) = crossfire::oneshot::oneshot();
        let (second_sender, second_completion) = crossfire::oneshot::oneshot();
        force_tx.send(first_sender).await.unwrap();
        force_tx.send(second_sender).await.unwrap();

        let attempt = CheckpointAttempt::new(80, 8_080);
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = attempt;
        callback.barrier_outcome = Some(BarrierOutcome::Async);

        coordinator.maybe_checkpoint(&mut callback).await;

        assert_eq!(callback.reserve_calls, 1);
        let active = coordinator
            .manual_active
            .as_ref()
            .expect("manual callers must attach at admission");
        assert_eq!(active.attempt, attempt);
        assert_eq!(active.replies.len(), 2);
        assert!(coordinator.manual_waiting.is_empty());

        let barrier = poll
            .poll()
            .expect("manual attempt must inject a source barrier");
        assert_eq!(barrier, CheckpointBarrier::new(8_080, 80));
        coordinator
            .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
            .await;
        assert!(coordinator.manual_active.is_some());

        let result = successful_checkpoint_result(attempt);
        let completion =
            CheckpointCompletion::validated(attempt, result.clone(), FxHashMap::default()).unwrap();
        assert!(coordinator
            .handle_checkpoint_completion(completion, &mut callback)
            .is_none());

        for reply in [first_completion, second_completion] {
            let completed = reply.await.unwrap().unwrap();
            assert_eq!(completed.epoch, attempt.epoch);
            assert_eq!(completed.checkpoint_id, attempt.checkpoint_id);
        }
        assert!(coordinator.manual_active.is_none());
    }

    #[tokio::test]
    async fn manual_reservation_failure_replies_instead_of_hanging() {
        let mut coordinator = admission_coordinator(Vec::new());
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(2);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);
        let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
        force_tx.send(reply_tx).await.unwrap();

        let mut callback = MockCallback::new();
        callback.reserve_error = Some("decision store unavailable".into());
        coordinator.maybe_checkpoint(&mut callback).await;

        let error = reply_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("decision store unavailable"));
        assert!(coordinator.manual_waiting.is_empty());
        assert!(coordinator.manual_active.is_none());
    }

    #[tokio::test]
    async fn manual_request_after_admission_waits_for_the_next_attempt() {
        let (source, poll) = checkpoint_source_handle("manual-source");
        let mut coordinator = admission_coordinator(vec![source]);
        coordinator.config.checkpoint_interval = None;
        let (force_tx, force_rx) = mpsc::bounded_async::<ForceCheckpointReply>(8);
        coordinator = coordinator.with_force_checkpoint_rx(force_rx);

        let first = CheckpointAttempt::new(81, 8_081);
        let second = CheckpointAttempt::new(82, 8_099);
        let (first_tx, first_rx) = crossfire::oneshot::oneshot();
        force_tx.send(first_tx).await.unwrap();
        let mut callback = MockCallback::new();
        callback.attempt_to_reserve = first;
        callback.barrier_outcome = Some(BarrierOutcome::Async);
        coordinator.maybe_checkpoint(&mut callback).await;
        let first_barrier = poll.poll().unwrap();

        let (second_tx, second_rx) = crossfire::oneshot::oneshot();
        force_tx.send(second_tx).await.unwrap();
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(coordinator.manual_waiting.len(), 1);
        assert_eq!(coordinator.manual_active.as_ref().unwrap().attempt, first);

        coordinator
            .handle_barrier(
                0,
                &first_barrier,
                &checkpoint_at(first.epoch),
                &mut callback,
            )
            .await;
        assert!(coordinator
            .handle_checkpoint_completion(
                CheckpointCompletion::validated(
                    first,
                    successful_checkpoint_result(first),
                    FxHashMap::default(),
                )
                .unwrap(),
                &mut callback,
            )
            .is_none());
        assert_eq!(
            first_rx.await.unwrap().unwrap().checkpoint_id,
            first.checkpoint_id
        );
        assert_eq!(coordinator.manual_waiting.len(), 1);

        callback.attempt_to_reserve = second;
        coordinator.maybe_checkpoint(&mut callback).await;
        assert_eq!(coordinator.manual_active.as_ref().unwrap().attempt, second);
        let second_barrier = poll.poll().unwrap();
        assert_eq!(second_barrier, CheckpointBarrier::new(8_099, 82));
        callback.barrier_outcome = Some(BarrierOutcome::Async);
        coordinator
            .handle_barrier(
                0,
                &second_barrier,
                &checkpoint_at(second.epoch),
                &mut callback,
            )
            .await;

        assert!(coordinator
            .handle_checkpoint_completion(
                CheckpointCompletion::failed(second, "injected durable-tail failure"),
                &mut callback,
            )
            .is_none());
        let error = second_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("injected durable-tail failure"));
    }

    #[tokio::test]
    async fn skipped_or_failed_aligned_checkpoint_abandons_the_exact_attempt() {
        use super::super::callback::SkipReason;

        let outcomes = [
            (
                BarrierOutcome::Skipped(SkipReason::NoCyclesSinceLastCheckpoint),
                "no_cycles_since_last_checkpoint",
                false,
            ),
            (
                BarrierOutcome::Failed,
                "barrier-aligned checkpoint failed before durable tail",
                true,
            ),
        ];

        for (outcome, expected_reason, records_failure) in outcomes {
            let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
            let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
            let mut coordinator = test_coordinator(
                rx,
                control_rx,
                Arc::new(tokio::sync::Notify::new()),
                DeliveryGuarantee::ExactlyOnce,
                None,
            );
            let attempt = CheckpointAttempt::new(53, 90_053);
            coordinator.pending_barrier.reset(attempt, 1);
            let mut callback = MockCallback::new();
            callback.barrier_outcome = Some(outcome);
            let barrier = CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch);

            coordinator
                .handle_barrier(0, &barrier, &checkpoint_at(attempt.epoch), &mut callback)
                .await;

            let abandoned = callback.abandoned_attempts.lock();
            assert_eq!(abandoned.len(), 1);
            assert_eq!(abandoned[0].0, attempt);
            assert_eq!(abandoned[0].1, expected_reason);
            assert_eq!(
                callback.checkpoint_failures.len(),
                usize::from(records_failure)
            );
            if records_failure {
                assert_eq!(callback.checkpoint_failures[0].0, attempt.checkpoint_id);
            }
        }
    }

    #[tokio::test]
    async fn alignment_timeout_abandons_the_exact_reserved_attempt() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            None,
        );
        coordinator.config.checkpoint_timeout = Duration::ZERO;
        let attempt = CheckpointAttempt::new(61, 600_061);
        coordinator.pending_barrier.reset(attempt, 2);
        coordinator.pending_barrier.sources_aligned.insert(0);

        let callback = MockCallback::new();
        let abandoned_attempts = Arc::clone(&callback.abandoned_attempts);
        let observed_abandoned_attempts = Arc::clone(&abandoned_attempts);
        let stop = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let observed = !observed_abandoned_attempts.lock().is_empty();
                    if observed {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("alignment timeout was not observed");
            shutdown.notify_one();
        });

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("coordinator must stop after the timeout is observed");
        stop.await.expect("timeout observer must not panic");

        assert!(matches!(exit, ExitReason::Shutdown));
        let abandoned = abandoned_attempts.lock();
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].0, attempt);
        assert_eq!(abandoned[0].1, "source barrier alignment timeout");
    }

    #[derive(Default)]
    struct StartupSourceState {
        open: AtomicBool,
        open_calls: AtomicU64,
        restore_calls: AtomicU64,
        close_calls: AtomicU64,
        poll_calls: AtomicU64,
    }

    struct StartupSource {
        state: Arc<StartupSourceState>,
        schema: Arc<Schema>,
        start_delay: Duration,
        fail_open: bool,
        fail_restore: bool,
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SourceConnector for StartupSource {
        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            self.state.open_calls.fetch_add(1, Ordering::SeqCst);
            // Model a connector that acquired resources inside the atomic startup operation
            // before discovering that startup failed. The coordinator must still close it.
            self.state.open.store(true, Ordering::SeqCst);
            if !self.start_delay.is_zero() {
                tokio::time::sleep(self.start_delay).await;
            }
            if self.fail_open {
                return Err(ConnectorError::ConnectionFailed(
                    "injected open failure".into(),
                ));
            }

            if matches!(request.position, SourcePosition::Resume { .. }) {
                self.state.restore_calls.fetch_add(1, Ordering::SeqCst);
                if self.fail_restore {
                    return Err(ConnectorError::Internal(
                        "injected resume-position failure".into(),
                    ));
                }
            }

            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            self.state.poll_calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.state.close_calls.fetch_add(1, Ordering::SeqCst);
            self.state.open.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum RuntimeSourceFailure {
        TerminalPoll,
        CommitNotification,
    }

    #[derive(Default)]
    struct RuntimeSourceState {
        polls: AtomicU64,
        commit_notifications: AtomicU64,
        closes: AtomicU64,
    }

    struct RuntimeFailureSource {
        state: Arc<RuntimeSourceState>,
        failure: RuntimeSourceFailure,
    }

    #[derive(Default)]
    struct CancellationSafePollState {
        poll_calls: AtomicU64,
        cancelled_polls: AtomicU64,
        commit_notification_calls: AtomicU64,
        first_poll_started: tokio::sync::Notify,
        release_first_poll: tokio::sync::Notify,
    }

    struct PollCancellationGuard {
        state: Arc<CancellationSafePollState>,
        completed: bool,
    }

    impl Drop for PollCancellationGuard {
        fn drop(&mut self) {
            if !self.completed {
                self.state.cancelled_polls.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    struct CancellationSafePollSource {
        state: Arc<CancellationSafePollState>,
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SourceConnector for CancellationSafePollSource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            let call = self.state.poll_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let mut guard = PollCancellationGuard {
                    state: Arc::clone(&self.state),
                    completed: false,
                };
                self.state.first_poll_started.notify_one();
                self.state.release_first_poll.notified().await;
                guard.completed = true;
            }
            Ok(None)
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::empty())
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn notify_epoch_committed(
            &mut self,
            _epoch: u64,
            _checkpoint: &SourceCheckpoint,
        ) -> Result<(), ConnectorError> {
            self.state
                .commit_notification_calls
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl laminar_connectors::connector::SourceConnector for RuntimeFailureSource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            self.state.polls.fetch_add(1, Ordering::SeqCst);
            match self.failure {
                RuntimeSourceFailure::TerminalPoll => Err(ConnectorError::Internal(
                    "injected terminal poll failure".into(),
                )),
                RuntimeSourceFailure::CommitNotification => Ok(None),
            }
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::new(Schema::empty())
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn notify_epoch_committed(
            &mut self,
            _epoch: u64,
            _checkpoint: &SourceCheckpoint,
        ) -> Result<(), ConnectorError> {
            self.state
                .commit_notifications
                .fetch_add(1, Ordering::SeqCst);
            match self.failure {
                RuntimeSourceFailure::CommitNotification => Err(ConnectorError::Internal(
                    "injected commit notification failure".into(),
                )),
                RuntimeSourceFailure::TerminalPoll => Ok(()),
            }
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.state.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn runtime_failure_coordinator(
        delivery_guarantee: DeliveryGuarantee,
        failure: RuntimeSourceFailure,
        state: Arc<RuntimeSourceState>,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> StreamingCoordinator {
        let source = SourceRegistration {
            name: "runtime-failure-source".into(),
            connector: Box::new(RuntimeFailureSource { state, failure }),
            config: laminar_connectors::config::ConnectorConfig::new("runtime-failure-test"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let config = PipelineConfig {
            delivery_guarantee,
            checkpoint_interval: Some(Duration::from_secs(60)),
            fallback_poll_interval: Duration::from_millis(1),
            ..PipelineConfig::default()
        };

        StreamingCoordinator::new(
            vec![source],
            config,
            shutdown,
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("runtime failure source must start")
    }

    async fn shut_down_after_observed(counter: &AtomicU64, shutdown: &tokio::sync::Notify) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while counter.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("injected source failure was not observed");
        shutdown.notify_one();
    }

    fn startup_source(
        name: &str,
        state: Arc<StartupSourceState>,
        fail_open: bool,
        fail_restore: bool,
        position: SourcePosition,
    ) -> SourceRegistration {
        startup_source_with_delay(
            name,
            state,
            fail_open,
            fail_restore,
            Duration::ZERO,
            position,
        )
    }

    fn startup_source_with_delay(
        name: &str,
        state: Arc<StartupSourceState>,
        fail_open: bool,
        fail_restore: bool,
        start_delay: Duration,
        position: SourcePosition,
    ) -> SourceRegistration {
        SourceRegistration {
            name: name.into(),
            connector: Box::new(StartupSource {
                state,
                schema: Arc::new(Schema::empty()),
                start_delay,
                fail_open,
                fail_restore,
            }),
            config: laminar_connectors::config::ConnectorConfig::new("startup-test"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            position,
        }
    }

    async fn startup_result(
        sources: Vec<SourceRegistration>,
    ) -> Result<StreamingCoordinator, DbError> {
        startup_result_with_config(sources, PipelineConfig::default()).await
    }

    async fn startup_result_with_config(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
    ) -> Result<StreamingCoordinator, DbError> {
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        StreamingCoordinator::new(
            sources,
            config,
            Arc::new(tokio::sync::Notify::new()),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn source_start_stage_uses_one_deadline_and_rolls_back_current_and_prior() {
        let prior = Arc::new(StartupSourceState::default());
        let current = Arc::new(StartupSourceState::default());
        let config = PipelineConfig {
            checkpoint_timeout: Duration::from_secs(10),
            ..PipelineConfig::default()
        };
        let result = startup_result_with_config(
            vec![
                startup_source_with_delay(
                    "prior",
                    Arc::clone(&prior),
                    false,
                    false,
                    Duration::from_secs(6),
                    SourcePosition::Initial,
                ),
                startup_source_with_delay(
                    "current",
                    Arc::clone(&current),
                    false,
                    false,
                    Duration::from_secs(6),
                    SourcePosition::Initial,
                ),
            ],
            config,
        )
        .await;

        let Err(error) = result else {
            panic!("the second source must consume the remaining shared startup budget");
        };
        assert!(
            matches!(error, DbError::Config(ref message)
                if message.contains("source 'current' start failed at initial position")
                    && message.contains("shared 10s source-start stage deadline")),
            "unexpected error: {error}"
        );
        assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(current.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(current.close_calls.load(Ordering::SeqCst), 1);
        assert!(!prior.open.load(Ordering::SeqCst));
        assert!(!current.open.load(Ordering::SeqCst));
        assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
        assert_eq!(current.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn source_resume_failure_is_fatal_and_closes_all_started_sources() {
        let prior = Arc::new(StartupSourceState::default());
        let failing = Arc::new(StartupSourceState::default());
        let result = startup_result(vec![
            startup_source(
                "prior",
                Arc::clone(&prior),
                false,
                false,
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::new(7, 7),
                    checkpoint: checkpoint_at(7),
                },
            ),
            startup_source(
                "failing",
                Arc::clone(&failing),
                false,
                true,
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::new(7, 7),
                    checkpoint: checkpoint_at(7),
                },
            ),
        ])
        .await;

        let Err(err) = result else {
            panic!("source resume-position failure must abort startup");
        };
        assert!(
            matches!(err, DbError::Checkpoint(ref msg) if msg.contains("source 'failing' start failed while resuming exact checkpoint epoch=7 id=7")),
            "unexpected error: {err}"
        );
        assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.restore_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.restore_calls.load(Ordering::SeqCst), 1);
        assert!(!prior.open.load(Ordering::SeqCst));
        assert!(!failing.open.load(Ordering::SeqCst));
        assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
        assert_eq!(failing.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn source_initial_start_failure_closes_prior_and_partially_started_source() {
        let prior = Arc::new(StartupSourceState::default());
        let failing = Arc::new(StartupSourceState::default());
        let result = startup_result(vec![
            startup_source(
                "prior",
                Arc::clone(&prior),
                false,
                false,
                SourcePosition::Initial,
            ),
            startup_source(
                "failing",
                Arc::clone(&failing),
                true,
                false,
                SourcePosition::Initial,
            ),
        ])
        .await;

        let Err(err) = result else {
            panic!("source initial-start failure must abort startup");
        };
        assert!(
            matches!(err, DbError::Config(ref msg) if msg.contains("source 'failing' start failed at initial position")),
            "unexpected error: {err}"
        );
        assert_eq!(prior.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.restore_calls.load(Ordering::SeqCst), 0);
        assert_eq!(failing.restore_calls.load(Ordering::SeqCst), 0);
        assert!(!prior.open.load(Ordering::SeqCst));
        assert!(!failing.open.load(Ordering::SeqCst));
        assert_eq!(prior.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(failing.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(prior.poll_calls.load(Ordering::SeqCst), 0);
        assert_eq!(failing.poll_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn terminal_source_poll_failure_faults_replay_guaranteed_modes() {
        for guarantee in [
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ] {
            let state = Arc::new(RuntimeSourceState::default());
            let shutdown = Arc::new(tokio::sync::Notify::new());
            let coordinator = runtime_failure_coordinator(
                guarantee,
                RuntimeSourceFailure::TerminalPoll,
                Arc::clone(&state),
                shutdown,
            )
            .await;

            let exit =
                tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                    .await
                    .expect("terminal source poll failure must stop the pipeline");

            assert!(
                matches!(exit, ExitReason::Fault(ref error) if error.contains("terminal poll failure")),
                "{guarantee} must fault for recovery after a terminal poll failure, got {exit:?}"
            );
            assert!(state.polls.load(Ordering::SeqCst) > 0);
            assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn source_commit_notification_failure_faults_replay_guaranteed_modes() {
        for guarantee in [
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ] {
            let state = Arc::new(RuntimeSourceState::default());
            let shutdown = Arc::new(tokio::sync::Notify::new());
            let coordinator = runtime_failure_coordinator(
                guarantee,
                RuntimeSourceFailure::CommitNotification,
                Arc::clone(&state),
                shutdown,
            )
            .await;
            coordinator.broadcast_epoch_committed(11, &FxHashMap::default());

            let exit =
                tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                    .await
                    .expect("source commit-notification failure must stop the pipeline");

            assert!(
                matches!(exit, ExitReason::Fault(ref error) if error.contains("commit notification failed at epoch 11")),
                "{guarantee} must fault for recovery after commit notification fails, got {exit:?}"
            );
            assert_eq!(state.commit_notifications.load(Ordering::SeqCst), 1);
            assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn epoch_commit_waits_for_in_flight_poll_without_cancelling_it() {
        let state = Arc::new(CancellationSafePollState::default());
        let source = SourceRegistration {
            name: "cancellation-safe-source".into(),
            connector: Box::new(CancellationSafePollSource {
                state: Arc::clone(&state),
            }),
            config: laminar_connectors::config::ConnectorConfig::new("cancellation-safe-test"),
            contract: laminar_connectors::connector::SourceContract::new(
                laminar_connectors::connector::SourceConsistency::Replayable,
                laminar_connectors::connector::SourceTopology::Singleton,
            ),
            position: SourcePosition::Initial,
        };
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let config = PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_interval: Some(Duration::from_secs(60)),
            fallback_poll_interval: Duration::from_millis(1),
            ..PipelineConfig::default()
        };
        let coordinator = StreamingCoordinator::new(
            vec![source],
            config,
            Arc::clone(&shutdown),
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), state.first_poll_started.notified())
            .await
            .expect("source never entered its first poll");
        coordinator.broadcast_epoch_committed(17, &FxHashMap::default());
        tokio::task::yield_now().await;
        assert_eq!(state.cancelled_polls.load(Ordering::SeqCst), 0);
        assert_eq!(
            state.commit_notification_calls.load(Ordering::SeqCst),
            0,
            "commit notification must wait for the connector borrow to return"
        );

        state.release_first_poll.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while state.commit_notification_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("commit notification was not applied after poll completion");
        assert_eq!(state.cancelled_polls.load(Ordering::SeqCst), 0);

        shutdown.notify_one();
        let exit =
            tokio::time::timeout(Duration::from_secs(5), coordinator.run(MockCallback::new()))
                .await
                .expect("coordinator must stop after shutdown");
        assert!(matches!(exit, ExitReason::Shutdown));
    }

    #[tokio::test]
    async fn best_effort_terminal_source_failures_do_not_claim_recovery() {
        for failure in [
            RuntimeSourceFailure::TerminalPoll,
            RuntimeSourceFailure::CommitNotification,
        ] {
            let state = Arc::new(RuntimeSourceState::default());
            let shutdown = Arc::new(tokio::sync::Notify::new());
            let coordinator = runtime_failure_coordinator(
                DeliveryGuarantee::BestEffort,
                failure,
                Arc::clone(&state),
                Arc::clone(&shutdown),
            )
            .await;
            let observed_counter = match failure {
                RuntimeSourceFailure::TerminalPoll => &state.polls,
                RuntimeSourceFailure::CommitNotification => {
                    coordinator.broadcast_epoch_committed(11, &FxHashMap::default());
                    &state.commit_notifications
                }
            };

            let run = coordinator.run(MockCallback::new());
            let stop = shut_down_after_observed(observed_counter, &shutdown);
            let (exit, ()) =
                tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(run, stop) })
                    .await
                    .expect("best-effort pipeline must stop cleanly after shutdown");

            assert!(
                matches!(exit, ExitReason::Shutdown),
                "best-effort must not report a recoverable fault, got {exit:?}"
            );
            assert_eq!(state.closes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn fatal_cycle_error_faults_exactly_once() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(60)),
        );

        let mut callback = MockCallback::new();
        callback.fatal_at_cycle = Some(1);
        callback.fault_on_error = true; // exactly-once: a fatal cycle error must fault

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("run() must return after a fatal cycle error");

        assert!(
            matches!(exit, ExitReason::Fault(_)),
            "exactly-once fatal cycle error must fault, got {exit:?}"
        );
        drop(tx);
    }

    #[tokio::test]
    async fn fatal_cycle_error_continues_at_least_once() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );

        let mut callback = MockCallback::new();
        callback.fatal_at_cycle = Some(1);
        let errors = Arc::clone(&callback.cycle_errors);

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_clone.notify_one();
        });

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("run() must return on shutdown");

        assert!(
            matches!(exit, ExitReason::Shutdown),
            "at-least-once must not fault on a cycle error, got {exit:?}"
        );
        assert_eq!(
            errors.load(Ordering::SeqCst),
            1,
            "at-least-once must drop-and-continue and count the error"
        );
        drop(tx);
    }

    /// CP-1: while a multi-source barrier is mid-alignment, a source that already passed the
    /// barrier must keep deferring its post-barrier (epoch N+1) batches across cycles — else
    /// they fold into epoch-N state while the manifest records that source's offset at-barrier,
    /// duplicating them on recovery.
    #[tokio::test]
    async fn aligned_source_post_barrier_batch_defers_across_cycles() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(60)),
        );
        coordinator.source_names = vec![Arc::from("src0"), Arc::from("src1")];
        coordinator.committed_offsets = vec![None, None];
        coordinator.pending_offsets = vec![None, None];

        let mut callback = MockCallback::new();

        // Source 0 passed its barrier for the in-flight checkpoint; source 1 has not, so
        // alignment spans cycles and source 0's post-barrier batch is queued for replay.
        coordinator
            .pending_barrier
            .reset(CheckpointAttempt::new(7, 7), 2);
        coordinator.pending_barrier.sources_aligned.insert(0);
        coordinator.post_barrier_buf.push(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(99),
            checkpoint: checkpoint_at(8),
        });

        // Next cycle start + post_barrier_buf replay.
        coordinator.source_batches_buf.clear();
        coordinator.reset_barrier_seen_for_cycle();
        let deferred = std::mem::take(&mut coordinator.post_barrier_buf);
        let mut barriers = Vec::new();
        let mut events = 0u64;
        for msg in deferred {
            coordinator.process_msg(msg, &mut callback, &mut barriers, &mut events);
        }

        assert!(
            coordinator.source_batches_buf.is_empty(),
            "aligned source's post-barrier batch must not fold into the pending epoch"
        );
        assert_eq!(
            coordinator.post_barrier_buf.len(),
            1,
            "aligned source's post-barrier batch must stay deferred"
        );
        assert!(
            coordinator.pending_offsets[0].is_none(),
            "deferred batch must not stage its offset"
        );

        // A batch from the not-yet-aligned source 1 folds normally.
        coordinator.process_msg(
            SourceMsg::Batch {
                source_idx: 1,
                batch: int_batch(5),
                checkpoint: checkpoint_at(8),
            },
            &mut callback,
            &mut barriers,
            &mut events,
        );
        assert_eq!(
            coordinator.source_batches_buf.get("src1").map(Vec::len),
            Some(1),
            "not-yet-aligned source's batch must fold into the pending epoch"
        );
    }

    /// CP-4: an exactly-once sink failure poisons the epoch and aborts its transaction; the
    /// coordinator must fault for recovery (via `take_sink_fault`) rather than continue and seal
    /// offsets past the dropped rows on the next checkpoint.
    #[tokio::test]
    async fn exactly_once_sink_fault_faults_pipeline() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            Some(Duration::from_secs(60)),
        );

        let mut callback = MockCallback::new();
        callback.sink_fault = Some("sink 's' write error at epoch 1".to_string());

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(5), coordinator.run(callback))
            .await
            .expect("run() must return after a sink fault");

        assert!(
            matches!(exit, ExitReason::Fault(_)),
            "an exactly-once sink fault must fault the pipeline, got {exit:?}"
        );
        drop(tx);
    }

    /// Test that the coordinator processes messages via direct mpsc channel.
    #[tokio::test]
    async fn test_coordinator_direct_channel() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);

        // Create coordinator directly (bypassing source spawning).
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let callback = MockCallback::new();

        // Send a batch.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        // Signal shutdown after a brief delay.
        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_clone.notify_one();
        });

        // Run coordinator — it should process the batch and exit on shutdown.
        coordinator.run(callback).await;

        // The callback was consumed by run(), so we can't inspect it directly.
        // But the test proves: no panics, no deadlocks, clean shutdown.
    }

    /// A wedged source task must not stall shutdown: `run()` still returns.
    #[tokio::test(start_paused = true)]
    async fn test_shutdown_aborts_wedged_source_task() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        // No timer: under paused time only the join timeout can unblock run().
        let wedged = tokio::spawn(std::future::pending::<()>());
        let (epoch_tx, _epoch_rx) = tokio::sync::watch::channel(None);
        let handle = SourceHandle {
            name: Arc::from("wedged"),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            join: wedged,
            barrier_injector: CheckpointBarrierInjector::new(),
            epoch_committed_tx: epoch_tx,
        };

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_handles: vec![handle],
            source_names: vec![Arc::from("wedged")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        shutdown.notify_one();

        // Outer bound gives a timer so a regression fails cleanly, not hangs.
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            coordinator.run(MockCallback::new()),
        )
        .await;
        assert!(
            result.is_ok(),
            "coordinator.run() must return after shutdown even with a wedged source task"
        );
    }

    #[test]
    fn completion_rejects_result_for_a_different_attempt() {
        let admitted = CheckpointAttempt::new(7, 42);
        let error = CheckpointCompletion::validated(
            admitted,
            crate::checkpoint_coordinator::CheckpointResult {
                success: true,
                checkpoint_id: 43,
                epoch: admitted.epoch,
                duration: Duration::ZERO,
                error: None,
            },
            FxHashMap::default(),
        )
        .expect_err("a different durable checkpoint ID must be rejected");
        assert!(error.contains("identity mismatch"));
        assert!(error.contains("id=42"));
        assert!(error.contains("id=43"));
    }

    /// A burned durable ID makes checkpoint ID diverge from epoch. The async completion path
    /// must preserve that exact identity rather than reconstructing `checkpoint_id = epoch`.
    #[tokio::test]
    async fn async_completion_publishes_exact_burned_gap_id() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);

        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::BestEffort,
            None,
        )
        .with_checkpoint_complete_rx(completion_rx);
        let callback = MockCallback::new();
        let published = Arc::clone(&callback.published_barriers);
        let join = tokio::spawn(async move { coordinator.run(callback).await });

        let attempt = CheckpointAttempt::new(7, 42);
        completion_tx
            .send(CheckpointCompletion::new(attempt, FxHashMap::default()))
            .await
            .expect("completion receiver must be live");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let has_published = !published.lock().is_empty();
                if has_published {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("async completion was not published");

        shutdown.notify_one();
        drop(source_tx);
        drop(completion_tx);
        let _ = join.await.expect("coordinator task panicked");

        assert_eq!(
            published.lock().as_slice(),
            &[(attempt.epoch, attempt.checkpoint_id)]
        );
    }

    #[tokio::test]
    async fn committed_cut_with_successor_failure_acks_then_faults_before_next_write() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(4);
        let (source, _barrier_poll) = checkpoint_source_handle("test_source");
        let committed_rx = source.epoch_committed_tx.subscribe();

        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::ExactlyOnce,
            None,
        )
        .with_checkpoint_complete_rx(completion_rx);
        coordinator.source_handles = vec![source];

        let callback = MockCallback::new();
        let published = Arc::clone(&callback.published_barriers);
        let published_at_close = Arc::clone(&callback.published_barriers_observed_at_close);
        let written_rows = Arc::clone(&callback.written_rows);
        let attempt = CheckpointAttempt::new(11, 8_111);
        let mut result = successful_checkpoint_result(attempt);
        result.error = Some(
            "checkpoint 8111 epoch 11 committed, but successor sink epoch 12 failed to begin"
                .into(),
        );
        let mut source_checkpoints = FxHashMap::default();
        let mut source_checkpoint = checkpoint_at(attempt.epoch);
        source_checkpoint.set_offset("partition-0", "committed-11");
        source_checkpoints.insert("test_source".to_string(), source_checkpoint);

        // Make both branches ready before run starts. The completion branch is biased ahead of
        // source intake and must publish checkpoint N, then terminally fence the queued N+1 row.
        source_tx
            .send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(12),
                checkpoint: checkpoint_at(attempt.epoch + 1),
            })
            .await
            .unwrap();
        completion_tx
            .send(
                CheckpointCompletion::validated(attempt, result, source_checkpoints)
                    .expect("completion identity must match"),
            )
            .await
            .unwrap();

        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("successor-open failure must terminate the pipeline");

        assert!(
            matches!(exit, ExitReason::Fault(ref error) if error.contains("successor sink epoch 12 failed to begin")),
            "pipeline must report the successor-open fault, got {exit:?}"
        );
        assert_eq!(
            published.lock().as_slice(),
            &[(attempt.epoch, attempt.checkpoint_id)],
            "the durable checkpoint must be published before faulting"
        );
        assert_eq!(
            published_at_close.load(Ordering::Acquire),
            1,
            "checkpoint acknowledgement must precede lifecycle teardown"
        );
        let committed = committed_rx
            .borrow()
            .clone()
            .expect("source must receive the durable checkpoint acknowledgement");
        assert_eq!(committed.0, attempt.epoch);
        assert_eq!(committed.1.get_offset("partition-0"), Some("committed-11"));
        assert_eq!(
            written_rows.load(Ordering::Acquire),
            0,
            "no successor-epoch row may reach a sink after begin_epoch failed"
        );
        drop(source_tx);
        drop(completion_tx);
    }

    /// Shutdown drains the open epoch but must not synthesize an unaligned final checkpoint.
    #[tokio::test]
    async fn shutdown_does_not_synthesize_final_checkpoint() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: Some(Duration::from_secs(60)),
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let callback = MockCallback::new();
        let control_calls = Arc::clone(&callback.control_checkpoint_call_audit);
        let written_rows = Arc::clone(&callback.written_rows);

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
            checkpoint: checkpoint_at(1),
        })
        .await
        .unwrap();

        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("shutdown drain must terminate");

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(written_rows.load(Ordering::SeqCst), 1);
        assert_eq!(
            control_calls.load(Ordering::SeqCst),
            0,
            "shutdown must not invoke checkpoint control or originate a final attempt"
        );
    }

    #[tokio::test]
    async fn shutdown_abandons_exact_pending_barrier_and_fails_manual_caller() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let mut coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            Some(Duration::from_secs(60)),
        );
        let attempt = CheckpointAttempt::new(31, 9_031);
        coordinator.pending_barrier.reset(attempt, 1);
        let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
        coordinator.manual_active = Some(ManualCheckpointAttempt {
            attempt,
            replies: vec![reply_tx],
        });

        let callback = MockCallback::new();
        let abandoned = Arc::clone(&callback.abandoned_attempts);
        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("pending alignment cancellation must not stall shutdown");

        assert!(matches!(exit, ExitReason::Shutdown));
        {
            let audit = abandoned.lock();
            assert_eq!(audit.len(), 1);
            assert_eq!(audit[0].0, attempt);
            assert!(audit[0].1.contains("shutdown interrupted"));
        }
        let error = reply_rx.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("shutdown interrupted"));
    }

    #[tokio::test]
    async fn shutdown_drain_ignores_barrier_and_processes_following_batch() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );

        let attempt = CheckpointAttempt::new(41, 9_041);
        source_tx
            .send(SourceMsg::Barrier {
                source_idx: 0,
                barrier: CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
                checkpoint: checkpoint_at(attempt.epoch),
            })
            .await
            .unwrap();
        source_tx
            .send(SourceMsg::Batch {
                source_idx: 0,
                batch: int_batch(7),
                checkpoint: checkpoint_at(attempt.epoch + 1),
            })
            .await
            .unwrap();

        let callback = MockCallback::new();
        let written_rows = Arc::clone(&callback.written_rows);
        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("a shutdown barrier must not requeue the following batch forever");

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(written_rows.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_settles_async_tail_before_closing_sinks() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(8);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
        let (completion_tx, completion_rx) = mpsc::bounded_async::<CheckpointCompletion>(8);
        let in_flight = Arc::new(AtomicU64::new(1));
        let staged_bytes = Arc::new(AtomicU64::new(0));
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        )
        .with_checkpoint_admission(Arc::clone(&in_flight), 1, staged_bytes, u64::MAX)
        .with_checkpoint_complete_rx(completion_rx);

        let callback = MockCallback::new();
        let published = Arc::clone(&callback.published_barriers);
        let published_at_close = Arc::clone(&callback.published_barriers_observed_at_close);
        let attempt = CheckpointAttempt::new(51, 9_051);
        let tail_in_flight = Arc::clone(&in_flight);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            completion_tx
                .send(CheckpointCompletion::new(attempt, FxHashMap::default()))
                .await
                .unwrap();
            tail_in_flight.fetch_sub(1, Ordering::AcqRel);
        });

        shutdown.notify_one();
        let exit = tokio::time::timeout(Duration::from_secs(1), coordinator.run(callback))
            .await
            .expect("shutdown must wait for the captured durable tail");

        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(
            published.lock().as_slice(),
            &[(attempt.epoch, attempt.checkpoint_id)]
        );
        assert_eq!(
            published_at_close.load(Ordering::SeqCst),
            1,
            "sink close raced the terminal completion"
        );
    }

    #[tokio::test]
    async fn replay_guarantee_faults_when_sink_shutdown_is_not_acknowledged() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::AtLeastOnce,
            None,
        );
        let mut callback = MockCallback::new();
        callback.fault_on_error = true;
        callback.close_error = Some("flush acknowledgement failed".to_string());

        shutdown.notify_one();
        let exit = coordinator.run(callback).await;
        let ExitReason::Fault(reason) = exit else {
            panic!("replay guarantee accepted an unacknowledged sink close");
        };
        assert!(reason.contains("flush acknowledgement failed"));
    }

    #[tokio::test]
    async fn best_effort_reports_sink_shutdown_failure_without_recovery_fault() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_source_tx, rx) = mpsc::bounded_async::<SourceMsg>(4);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(4);
        let coordinator = test_coordinator(
            rx,
            control_rx,
            Arc::clone(&shutdown),
            DeliveryGuarantee::BestEffort,
            None,
        );
        let mut callback = MockCallback::new();
        callback.close_error = Some("close acknowledgement failed".to_string());
        let errors = Arc::clone(&callback.cycle_errors);

        shutdown.notify_one();
        let exit = coordinator.run(callback).await;
        assert!(matches!(exit, ExitReason::Shutdown));
        assert_eq!(errors.load(Ordering::SeqCst), 1);
    }

    /// Test that post-barrier batches are excluded from the current cycle's
    /// `source_batches_buf` and deferred to the next cycle.
    #[tokio::test]
    #[allow(clippy::too_many_lines, clippy::similar_names)]
    async fn test_barrier_excludes_post_barrier_data() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));

        let (_control_tx2, control_rx2) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        let mut coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx: mpsc::bounded_async::<SourceMsg>(64).1, // dummy, not used
            source_handles: Vec::new(),
            source_names: vec![Arc::from("s0"), Arc::from("s1")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None, None],
            pending_offsets: vec![None, None],
            control_rx: control_rx2,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let mut callback = MockCallback::new();
        let mut barriers = Vec::new();
        let mut cycle_events: u64 = 0;

        // Source 0: batch(ts=1), barrier, batch(ts=2)
        let batch_1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let batch_2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![2]))],
        )
        .unwrap();
        let barrier = CheckpointBarrier::new(1, 1);

        coordinator.process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: batch_1,
                checkpoint: checkpoint_at(10),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Barrier {
                source_idx: 0,
                barrier,
                checkpoint: checkpoint_at(10),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: batch_2,
                checkpoint: checkpoint_at(20),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );

        // Source 1: batch(ts=1), barrier
        let batch_s1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        coordinator.process_msg(
            SourceMsg::Batch {
                source_idx: 1,
                batch: batch_s1,
                checkpoint: checkpoint_at(5),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Barrier {
                source_idx: 1,
                barrier,
                checkpoint: checkpoint_at(5),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );

        // Verify: source_batches_buf should have ts=1 from both sources,
        // but NOT ts=2 from source 0 (that's post-barrier).
        let s0_batches = coordinator.source_batches_buf.get("s0").unwrap();
        assert_eq!(
            s0_batches.len(),
            1,
            "s0 should have exactly 1 pre-barrier batch"
        );
        let s0_col = s0_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(s0_col.value(0), 1, "s0 batch should contain ts=1");

        let s1_batches = coordinator.source_batches_buf.get("s1").unwrap();
        assert_eq!(s1_batches.len(), 1, "s1 should have exactly 1 batch");

        // Post-barrier buf should contain the ts=2 batch.
        assert_eq!(
            coordinator.post_barrier_buf.len(),
            1,
            "post_barrier_buf should have 1 deferred batch"
        );

        // pending_offsets: pre-barrier only (post-barrier deferred, not staged).
        assert_eq!(
            coordinator.pending_offsets[0]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("10"),
            "s0 pending offset should be the pre-barrier batch"
        );
        assert_eq!(
            coordinator.pending_offsets[1]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("5"),
            "s1 pending offset should be epoch 5"
        );
        // committed_offsets must still be None — no execute_cycle has run.
        assert!(
            coordinator.committed_offsets[0].is_none(),
            "s0 committed offset should be None before execute_cycle"
        );
        assert!(
            coordinator.committed_offsets[1].is_none(),
            "s1 committed offset should be None before execute_cycle"
        );

        // Simulate successful cycle → commit.
        coordinator.commit_pending_offsets();
        assert_eq!(
            coordinator.committed_offsets[0]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("10"),
            "s0 committed after cycle"
        );
        assert_eq!(
            coordinator.committed_offsets[1]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("5"),
            "s1 committed after cycle"
        );

        // Barriers should have both sources.
        assert_eq!(barriers.len(), 2, "should have barriers from both sources");
    }

    // A faulted domain's source offset is held back while a healthy sibling source commits.
    #[tokio::test]
    async fn test_commit_pending_offsets_except_holds_failed_source() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        let mut coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx: mpsc::bounded_async::<SourceMsg>(64).1,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("s0"), Arc::from("s1")],
            shutdown,
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None, None],
            pending_offsets: vec![Some(checkpoint_at(10)), Some(checkpoint_at(20))],
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let mut failed: FxHashSet<Arc<str>> = FxHashSet::default();
        failed.insert(Arc::from("s0"));
        coordinator.commit_pending_offsets_except(&failed);

        assert!(
            coordinator.committed_offsets[0].is_none(),
            "faulted s0 must not commit"
        );
        assert!(
            coordinator.pending_offsets[0].is_none(),
            "faulted s0 staged offset is discarded for replay"
        );
        assert_eq!(
            coordinator.committed_offsets[1]
                .as_ref()
                .unwrap()
                .get_offset("test_position"),
            Some("20"),
            "healthy s1 commits and advances"
        );
    }

    struct BackpressuredCallback {
        inner: MockCallback,
        cycle_count: Arc<std::sync::atomic::AtomicU32>,
        events_per_cycle: Arc<Mutex<Vec<u64>>>,
    }

    impl BackpressuredCallback {
        fn new(
            cycle_count: Arc<std::sync::atomic::AtomicU32>,
            events_per_cycle: Arc<Mutex<Vec<u64>>>,
        ) -> Self {
            Self {
                inner: MockCallback::new(),
                cycle_count,
                events_per_cycle,
            }
        }
    }

    impl PipelineCallback for BackpressuredCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            watermark: i64,
        ) -> Result<CycleOutcome, CycleError> {
            self.cycle_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let total: u64 = source_batches
                .values()
                .flat_map(|bs| bs.iter())
                .map(|b| b.num_rows() as u64)
                .sum();
            self.events_per_cycle.lock().push(total);
            self.inner.execute_cycle(source_batches, watermark).await
        }

        fn push_to_streams(&self, r: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
            self.inner.push_to_streams(r);
        }
        async fn write_to_sinks(&mut self, r: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
            self.inner.write_to_sinks(r).await;
        }
        fn extract_watermark(&mut self, s: &str, b: &RecordBatch) {
            self.inner.extract_watermark(s, b);
        }
        fn filter_late_rows(&self, s: &str, b: &RecordBatch) -> Option<RecordBatch> {
            self.inner.filter_late_rows(s, b)
        }
        fn current_watermark(&self) -> i64 {
            self.inner.current_watermark()
        }
        async fn service_checkpoint_control(
            &mut self,
            offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> Option<u64> {
            self.inner.service_checkpoint_control(offsets).await
        }
        async fn checkpoint_with_barrier(
            &mut self,
            cp: FxHashMap<String, SourceCheckpoint>,
            attempt: CheckpointAttempt,
            attempt_started: Instant,
        ) -> BarrierOutcome {
            self.inner
                .checkpoint_with_barrier(cp, attempt, attempt_started)
                .await
        }
        async fn reserve_checkpoint_attempt(
            &mut self,
            attempt_started: Instant,
        ) -> Result<CheckpointAttempt, String> {
            self.inner.reserve_checkpoint_attempt(attempt_started).await
        }
        async fn abandon_checkpoint_attempt(&mut self, attempt: CheckpointAttempt, reason: &str) {
            self.inner.abandon_checkpoint_attempt(attempt, reason).await;
        }
        fn record_cycle(&self, e: u64, b: u64, ns: u64) {
            self.inner.record_cycle(e, b, ns);
        }
        async fn poll_tables(&mut self) {
            self.inner.poll_tables().await;
        }
        fn apply_control(&mut self, msg: crate::pipeline::ControlMsg) {
            self.inner.apply_control(msg);
        }

        fn is_backpressured(&self) -> bool {
            true // Always backpressured — drain loop should never fire.
        }
    }

    /// With `is_backpressured() == true`, the coordinator processes only
    /// the first wakeup message per cycle (no drain coalescing). With 5
    /// messages pre-loaded and `batch_window=0`, each cycle should see
    /// exactly 1 event, spread across multiple cycles.
    #[tokio::test]
    async fn test_drain_skip_under_backpressure() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("src")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

        // Pre-load 5 batches (1 row each).
        for i in 0..5 {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![i]))],
            )
            .unwrap();
            tx.send(SourceMsg::Batch {
                source_idx: 0,
                batch,
                checkpoint: checkpoint_at(u64::try_from(i).unwrap()),
            })
            .await
            .unwrap();
        }

        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            shutdown_clone.notify_one();
        });

        let cycle_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let events_per_cycle = Arc::new(Mutex::new(Vec::new()));
        let callback =
            BackpressuredCallback::new(Arc::clone(&cycle_count), Arc::clone(&events_per_cycle));
        coordinator.run(callback).await;

        let cycles = cycle_count.load(std::sync::atomic::Ordering::SeqCst);
        let epc = events_per_cycle.lock();
        let total: u64 = epc.iter().sum();

        // All 5 events must be processed (no data loss).
        assert_eq!(total, 5, "all events must be processed, got {total}");
        // Under backpressure each cycle gets only the wakeup message (1
        // event), so we need at least 5 cycles for 5 messages. Without
        // backpressure, cycle 1 would drain all 5 in one shot.
        assert!(
            cycles >= 5,
            "expected >=5 cycles (1 event each), got {cycles} cycles with events/cycle: {epc:?}"
        );
        // Each cycle sees at most 1 event (the wakeup message; drain skipped).
        for (i, &events) in epc.iter().enumerate() {
            assert!(
                events <= 1,
                "cycle {i} saw {events} events, expected <=1 under backpressure"
            );
        }
    }

    struct StateBudgetCallback {
        inner: MockCallback,
        events_per_cycle: Arc<Mutex<Vec<u64>>>,
        idle_ticks: Arc<std::sync::atomic::AtomicU32>,
    }

    impl PipelineCallback for StateBudgetCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            watermark: i64,
        ) -> Result<CycleOutcome, CycleError> {
            let total: u64 = source_batches
                .values()
                .flat_map(|bs| bs.iter())
                .map(|b| b.num_rows() as u64)
                .sum();
            self.events_per_cycle.lock().push(total);
            self.inner.execute_cycle(source_batches, watermark).await
        }

        fn push_to_streams(&self, r: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
            self.inner.push_to_streams(r);
        }
        async fn write_to_sinks(&mut self, r: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {
            self.inner.write_to_sinks(r).await;
        }
        fn extract_watermark(&mut self, s: &str, b: &RecordBatch) {
            self.inner.extract_watermark(s, b);
        }
        fn filter_late_rows(&self, s: &str, b: &RecordBatch) -> Option<RecordBatch> {
            self.inner.filter_late_rows(s, b)
        }
        fn current_watermark(&self) -> i64 {
            self.inner.current_watermark()
        }
        async fn service_checkpoint_control(
            &mut self,
            offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> Option<u64> {
            self.inner.service_checkpoint_control(offsets).await
        }
        async fn checkpoint_with_barrier(
            &mut self,
            cp: FxHashMap<String, SourceCheckpoint>,
            attempt: CheckpointAttempt,
            attempt_started: Instant,
        ) -> BarrierOutcome {
            self.inner
                .checkpoint_with_barrier(cp, attempt, attempt_started)
                .await
        }
        async fn reserve_checkpoint_attempt(
            &mut self,
            attempt_started: Instant,
        ) -> Result<CheckpointAttempt, String> {
            self.inner.reserve_checkpoint_attempt(attempt_started).await
        }
        async fn abandon_checkpoint_attempt(&mut self, attempt: CheckpointAttempt, reason: &str) {
            self.inner.abandon_checkpoint_attempt(attempt, reason).await;
        }
        fn record_cycle(&self, e: u64, b: u64, ns: u64) {
            self.inner.record_cycle(e, b, ns);
        }
        async fn poll_tables(&mut self) {
            self.inner.poll_tables().await;
        }
        fn apply_control(&mut self, msg: crate::pipeline::ControlMsg) {
            self.inner.apply_control(msg);
        }

        fn state_over_budget(&mut self) -> bool {
            true // Permanently over budget — drain must skip, idle tick must not run.
        }

        fn tick_idle_watermark(&mut self) {
            self.idle_ticks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// With `state_over_budget() == true`, the coordinator throttles intake
    /// exactly like buffer backpressure (one wakeup message per cycle, no
    /// drain coalescing, no data loss) and never ticks the idle-watermark
    /// demotion — a budget-paused source must not be treated as idle.
    #[tokio::test]
    async fn test_state_budget_pause_throttles_intake_and_holds_idle_tick() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(64);
        let (_control_tx, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                checkpoint_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
                shared_source_isolation: false,
                max_replay_buffer_bytes: 256 * 1024 * 1024,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("src")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            pending_watermark_batches: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
            checkpoint_complete_rx: None,
            force_ckpt_rx: None,
            manual_waiting: Vec::new(),
            manual_active: None,
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
            coordinated_commit_admission: None,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        for i in 0..5 {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![i]))],
            )
            .unwrap();
            tx.send(SourceMsg::Batch {
                source_idx: 0,
                batch,
                checkpoint: checkpoint_at(u64::try_from(i).unwrap()),
            })
            .await
            .unwrap();
        }

        let events_per_cycle = Arc::new(Mutex::new(Vec::new()));
        let idle_ticks = Arc::new(std::sync::atomic::AtomicU32::new(0));

        // The shutdown drain legitimately ticks (everything queued was
        // drained and watermark-extracted first), so the budget-pause
        // assertion is on the count snapshotted just before shutdown.
        let ticks_before_shutdown = Arc::new(std::sync::atomic::AtomicU32::new(u32::MAX));
        let shutdown_clone = Arc::clone(&shutdown);
        let idle_ticks_clone = Arc::clone(&idle_ticks);
        let ticks_before_clone = Arc::clone(&ticks_before_shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            ticks_before_clone.store(
                idle_ticks_clone.load(std::sync::atomic::Ordering::SeqCst),
                std::sync::atomic::Ordering::SeqCst,
            );
            shutdown_clone.notify_one();
        });

        let callback = StateBudgetCallback {
            inner: MockCallback::new(),
            events_per_cycle: Arc::clone(&events_per_cycle),
            idle_ticks: Arc::clone(&idle_ticks),
        };
        coordinator.run(callback).await;

        let epc = events_per_cycle.lock();
        let total: u64 = epc.iter().sum();
        assert_eq!(total, 5, "all events must be processed, got {total}");
        for (i, &events) in epc.iter().enumerate() {
            assert!(
                events <= 1,
                "cycle {i} saw {events} events, expected <=1 while over budget"
            );
        }
        assert_eq!(
            ticks_before_shutdown.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "idle-watermark tick must not run while intake is budget-paused"
        );
    }
}
