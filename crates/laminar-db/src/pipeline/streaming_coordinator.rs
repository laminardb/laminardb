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
use crossfire::{mpsc, AsyncRx};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::{DeliveryGuarantee, SourceBatch};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
use rustc_hash::{FxHashMap, FxHashSet};

use super::callback::{BarrierOutcome, CycleError, PipelineCallback, SourceRegistration};
use super::config::PipelineConfig;
use crate::error::DbError;

type SourceMsgRx = AsyncRx<mpsc::Array<SourceMsg>>;
type ControlMsgRx = AsyncRx<mpsc::Array<super::ControlMsg>>;

/// Message sent from a source task to the coordinator.
///
/// Each variant carries the [`SourceCheckpoint`] captured at production time,
/// so the coordinator never checkpoints an offset for data it hasn't processed.
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
}

/// Handle to a running source I/O task.
struct SourceHandle {
    name: Arc<str>,
    shutdown: Arc<tokio::sync::Notify>,
    join: tokio::task::JoinHandle<()>,
    barrier_injector: CheckpointBarrierInjector,
    /// Notifies the source of a committed `(epoch, checkpoint)` so it can ack to
    /// its external system. The checkpoint is what was actually written to the manifest,
    /// which may lag `self.offsets`. Empty for timer-driven commits.
    epoch_committed_tx: tokio::sync::watch::Sender<Option<(u64, SourceCheckpoint)>>,
}

/// `(epoch, per-source fan-out)` sent back when a background checkpoint completes.
type CheckpointCompletion = (u64, rustc_hash::FxHashMap<String, SourceCheckpoint>);

/// Why [`StreamingCoordinator::run`] returned.
#[derive(Debug)]
pub enum ExitReason {
    /// Shutdown signaled, or all source senders dropped — a clean stop.
    Shutdown,
    /// Fatal exactly-once cycle error; the caller recovers from the last checkpoint.
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
    next_checkpoint_id: u64,
    last_checkpoint: Instant,
    checkpoint_request_flags: Vec<Arc<AtomicBool>>,
    source_batches_buf: FxHashMap<Arc<str>, Vec<RecordBatch>>,
    /// Batches received from a source after it sent a barrier in the same drain cycle.
    /// They belong to the next epoch and are deferred to the next cycle.
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
    /// Epochs between admission and durable (tails still running); shared with callback.
    checkpoint_in_flight: Arc<AtomicU64>,
    /// Admission cap on `checkpoint_in_flight`. Exactly-once pipelines use 1.
    max_in_flight_epochs: u64,
    /// Captured-state bytes held by in-flight epochs; shared with callback.
    staged_bytes: Arc<AtomicU64>,
    max_staged_bytes: u64,
}

/// Tracks in-flight checkpoint barrier alignment.
struct PendingBarrier {
    checkpoint_id: u64,
    sources_total: usize,
    sources_aligned: FxHashSet<usize>,
    source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    started_at: Instant,
    active: bool,
}

impl PendingBarrier {
    fn new() -> Self {
        Self {
            checkpoint_id: 0,
            sources_total: 0,
            sources_aligned: FxHashSet::default(),
            source_checkpoints: FxHashMap::default(),
            started_at: Instant::now(),
            active: false,
        }
    }

    fn reset(&mut self, checkpoint_id: u64, sources_total: usize) {
        self.checkpoint_id = checkpoint_id;
        self.sources_total = sources_total;
        self.sources_aligned.clear();
        self.source_checkpoints.clear();
        self.started_at = Instant::now();
        self.active = true;
    }
}

/// Fallback timeout for idle wake.
const IDLE_TIMEOUT: Duration = Duration::from_millis(100);

/// Cap on a source task's post-shutdown flush so a hot source can't stall shutdown.
const SHUTDOWN_DRAIN_BUDGET: Duration = Duration::from_secs(2);

/// Cap on awaiting a source task at shutdown before aborting it.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Throttled (~once/10s) WARN while barrier admission is paused at the
/// staged-state cap — this check runs every coordinator tick, so an
/// unthrottled warn would spam under a sustained upload backlog.
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

/// What woke a source task's select loop.
enum SourceWake {
    Shutdown,
    EpochCommitted(u64, SourceCheckpoint),
    Polled(Result<Option<SourceBatch>, ConnectorError>),
}

impl StreamingCoordinator {
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
                .unwrap_or_else(|| SourceCheckpoint::new(epoch));
            let _ = handle.epoch_committed_tx.send(Some((epoch, cp)));
        }
    }

    /// Build the coordinator, open each source connector, and spawn source tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery guarantee constraints are violated or a source fails to open.
    #[allow(clippy::too_many_lines)]
    pub async fn new(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: ControlMsgRx,
    ) -> Result<Self, DbError> {
        // Validate delivery guarantee constraints.
        if config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            for src in &sources {
                if !src.supports_replay {
                    return Err(DbError::Config(format!(
                        "[LDB-5031] exactly-once requires source '{}' to support replay",
                        src.name
                    )));
                }
            }
            if config.checkpoint_interval.is_none() {
                return Err(DbError::Config(
                    "[LDB-5032] exactly-once requires checkpointing to be enabled".into(),
                ));
            }
        }

        if config.channel_capacity == 0 {
            return Err(DbError::Config(
                "[LDB-0010] channel_capacity must be > 0".into(),
            ));
        }

        let (tx, rx) = mpsc::bounded_async::<SourceMsg>(config.channel_capacity);

        let mut source_handles = Vec::with_capacity(sources.len());
        let mut source_names = Vec::with_capacity(sources.len());
        let mut checkpoint_request_flags = Vec::new();
        let mut committed_offsets = Vec::with_capacity(sources.len());

        for (idx, src) in sources.into_iter().enumerate() {
            if let Some(flag) = src.connector.checkpoint_requested() {
                checkpoint_request_flags.push(flag);
            }

            let task_shutdown = Arc::new(tokio::sync::Notify::new());
            let task_shutdown_clone = Arc::clone(&task_shutdown);
            let task_tx = tx.clone();
            let max_poll = config.max_poll_records;
            let poll_interval = config.fallback_poll_interval;
            let src_name = src.name.clone();
            let restore = src.restore_checkpoint.clone();
            let mut connector = src.connector;
            let connector_config = src.config;

            connector
                .open(&connector_config)
                .await
                .map_err(|e| DbError::Config(format!("source '{src_name}' open failed: {e}")))?;

            if let Some(ref cp) = restore {
                if let Err(e) = connector.restore(cp).await {
                    tracing::warn!(
                        source = %src_name, error = %e,
                        "source restore failed, starting from beginning"
                    );
                }
            }

            // Seed with the restore checkpoint so a pre-data shutdown still checkpoints it.
            committed_offsets.push(src.restore_checkpoint);

            let barrier_injector = CheckpointBarrierInjector::new();
            let barrier_handle = barrier_injector.handle();

            let (epoch_committed_tx, mut epoch_committed_rx) =
                tokio::sync::watch::channel::<Option<(u64, SourceCheckpoint)>>(None);

            let join = tokio::spawn(async move {
                let mut epoch: u64 = 0;

                // Ack a fresh commit before polling more — keeps
                // max_ack_pending headroom for the broker.
                loop {
                    let wake = tokio::select! {
                        biased;
                        () = task_shutdown_clone.notified() => SourceWake::Shutdown,
                        r = epoch_committed_rx.changed() => match r {
                            Ok(()) => {
                                let snapshot = epoch_committed_rx.borrow_and_update().clone();
                                match snapshot {
                                    Some((e, cp)) => SourceWake::EpochCommitted(e, cp),
                                    None => continue,
                                }
                            },
                            Err(_) => SourceWake::Shutdown,
                        },
                        r = connector.poll_batch(max_poll) => SourceWake::Polled(r),
                    };

                    let poll_result = match wake {
                        SourceWake::Shutdown => break,
                        SourceWake::EpochCommitted(e, cp) => {
                            if let Err(err) = connector.notify_epoch_committed(e, &cp).await {
                                tracing::warn!(
                                    source = %src_name,
                                    error = %err,
                                    epoch = e,
                                    "notify_epoch_committed failed",
                                );
                            }
                            continue;
                        }
                        SourceWake::Polled(r) => r,
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
                            tokio::select! {
                                biased;
                                () = task_shutdown_clone.notified() => break,
                                () = tokio::time::sleep(poll_interval) => {}
                            }
                        }
                        Err(e) if !e.is_transient() => {
                            tracing::error!(source = %src_name, error = %e, "terminal poll error");
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(source = %src_name, error = %e, "poll error (retrying)");
                            tokio::select! {
                                biased;
                                () = task_shutdown_clone.notified() => break,
                                () = tokio::time::sleep(poll_interval) => {}
                            }
                        }
                    }

                    if let Some(barrier) = barrier_handle.poll(epoch) {
                        epoch += 1;
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

                // Bounded best-effort flush before close(). The `while` deadline (not the
                // inner timeout) bounds an always-ready poll — timeout() polls the future
                // first. Non-blocking send; unflushed rows resume from the committed offset.
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

                // Drain EpochCommitted broadcasts before close so the final checkpoint
                // epoch is acked to the broker. Loop until the sender is dropped.
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
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
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

    pub(crate) fn with_checkpoint_complete_rx(
        mut self,
        rx: crossfire::AsyncRx<crossfire::mpsc::Array<CheckpointCompletion>>,
    ) -> Self {
        self.checkpoint_complete_rx = Some(rx);
        self
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
        let mut barriers_buf: Vec<(usize, CheckpointBarrier, SourceCheckpoint)> = Vec::new();
        // Set by a fatal exactly-once error; gates the final checkpoint (committing the
        // open epoch would seal offsets past the lost rows → recovery dups/gaps them).
        let mut fault: Option<String> = None;

        loop {
            // Step: Wait for data, shutdown, or idle timeout.
            let msg = tokio::select! {
                biased;
                () = self.shutdown.notified() => break,
                // A background persist finished (in-flight guard ensures epoch order).
                Some((epoch, fan_out)) = async {
                    if let Some(ref mut rx) = self.checkpoint_complete_rx {
                        rx.recv().await.ok()
                    } else {
                        futures::future::pending::<Option<CheckpointCompletion>>().await
                    }
                } => {
                    self.broadcast_epoch_committed(epoch, &fan_out);
                    callback.publish_barrier(epoch, epoch);
                    continue;
                }
                msg = self.rx.recv() => {
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

            self.source_batches_buf.clear();
            self.barrier_seen.clear();
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
                    Ok(msg) => {
                        self.process_msg(msg, &mut callback, &mut barriers_buf, &mut cycle_events);
                        drain_count += 1;
                    }
                    Err(_) => break,
                }
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
                    Ok(results) => {
                        self.commit_pending_offsets();
                        callback.update_mv_stores(&results);
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => {
                        self.discard_pending_offsets();
                        match e {
                            // Shutdown already signaled; restarting would just re-trip it.
                            CycleError::Halt(msg) => {
                                tracing::warn!(reason = %msg, "[LDB-3022] cycle halted");
                            }
                            // Exactly-once: continuing would drop the drained rows (EO gap).
                            CycleError::Fatal(msg)
                                if self.config.delivery_guarantee
                                    == DeliveryGuarantee::ExactlyOnce =>
                            {
                                tracing::error!(
                                    error = %msg,
                                    "[LDB-3021] fatal SQL cycle error; faulting for recovery"
                                );
                                fault = Some(msg);
                                break;
                            }
                            // At-least-once: drop the bad cycle and continue.
                            CycleError::Fatal(msg) => {
                                callback.note_cycle_error();
                                tracing::warn!(
                                    error = %msg,
                                    "[LDB-3020] SQL cycle error (at-least-once: continuing)"
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

            #[allow(clippy::cast_possible_truncation)]
            if (bg_start.elapsed().as_nanos() as u64) < bg_budget {
                self.maybe_checkpoint(&mut callback).await;
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
                && self.pending_barrier.started_at.elapsed() > self.config.barrier_alignment_timeout
            {
                tracing::warn!(
                    checkpoint_id = self.pending_barrier.checkpoint_id,
                    "Barrier alignment timeout — cancelling checkpoint"
                );
                self.pending_barrier.active = false;
            }
        }

        for handle in &self.source_handles {
            handle.shutdown.notify_one();
        }

        // Skip the drain + final checkpoint on a fault (see `fault` above). Sources are
        // still notified above and joined below, so old tasks stop before a restart.
        if fault.is_none() {
            // Drain before joining: source tasks blocked on a full channel can't see
            // the shutdown signal until slots free. Joining first deadlocks.
            self.source_batches_buf.clear();
            self.barrier_seen.clear();
            self.discard_pending_offsets();
            let mut drain_barriers: Vec<(usize, CheckpointBarrier, SourceCheckpoint)> = Vec::new();
            let mut drain_events: u64 = 0;

            loop {
                let deferred = std::mem::take(&mut self.post_barrier_buf);
                let mut got_any = !deferred.is_empty();
                for msg in deferred {
                    self.process_msg(msg, &mut callback, &mut drain_barriers, &mut drain_events);
                }
                while let Ok(msg) = self.rx.try_recv() {
                    got_any = true;
                    self.process_msg(msg, &mut callback, &mut drain_barriers, &mut drain_events);
                }
                if !got_any {
                    break;
                }
            }

            for (name, batch) in self.pending_watermark_batches.drain(..) {
                callback.extract_watermark(&name, &batch);
            }
            callback.tick_idle_watermark();
            if !self.source_batches_buf.is_empty() || callback.has_deferred_input() {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(results) => {
                        self.commit_pending_offsets();
                        callback.update_mv_stores(&results);
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => {
                        self.discard_pending_offsets();
                        tracing::warn!(error = %e, "[LDB-3020] SQL cycle error during shutdown drain");
                    }
                }
            }

            // Second drain: messages sent between the first drain and source task exit.
            self.source_batches_buf.clear();
            self.barrier_seen.clear();
            self.discard_pending_offsets();
            drain_barriers.clear();
            while let Ok(msg) = self.rx.try_recv() {
                self.process_msg(msg, &mut callback, &mut drain_barriers, &mut drain_events);
            }
            for (name, batch) in self.pending_watermark_batches.drain(..) {
                callback.extract_watermark(&name, &batch);
            }
            callback.tick_idle_watermark();
            if !self.source_batches_buf.is_empty() || callback.has_deferred_input() {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(results) => {
                        self.commit_pending_offsets();
                        callback.update_mv_stores(&results);
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => {
                        self.discard_pending_offsets();
                        tracing::warn!(error = %e, "[LDB-3020] SQL cycle error during final drain");
                    }
                }
            }

            // Run the final checkpoint before dropping source senders so the EpochCommitted
            // ack reaches the broker (Kafka group offsets, etc.).
            let checkpoint_enabled = self.config.checkpoint_interval.is_some();
            if checkpoint_enabled {
                let source_offsets = self.current_source_offsets();
                if let Some(epoch) = callback
                    .maybe_checkpoint(true, source_offsets.clone())
                    .await
                {
                    tracing::info!(epoch, "final checkpoint completed before shutdown");
                    self.broadcast_epoch_committed(epoch, &source_offsets);
                }
            }
        }

        // Drop senders before join: source tasks wait on `epoch_committed_rx.changed()`
        // and exit only when the sender is dropped.
        for handle in std::mem::take(&mut self.source_handles) {
            let SourceHandle {
                name,
                mut join,
                epoch_committed_tx,
                ..
            } = handle;
            drop(epoch_committed_tx);
            // Abort a source task that didn't exit in time so run() can return.
            match tokio::time::timeout(SHUTDOWN_JOIN_TIMEOUT, &mut join).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(source = %name, error = ?e, "source task panicked"),
                Err(_) => {
                    tracing::warn!(
                        source = %name,
                        "source task did not exit within shutdown budget; aborting"
                    );
                    join.abort();
                }
            }
        }

        fault.map_or(ExitReason::Shutdown, ExitReason::Fault)
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

                if source_idx < self.pending_offsets.len() {
                    self.pending_offsets[source_idx] = Some(checkpoint);
                }

                if let Some(name) = self.source_names.get(source_idx) {
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        *cycle_events += batch.num_rows() as u64;
                    }
                    // Filter against the pre-drain watermark. Extraction is deferred
                    // until after all batches are filtered so one batch's watermark
                    // advance can't cause the next batch's rows to be dropped as late.
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
        }
    }

    /// Current per-source committed offsets, keyed by source name. These reflect the
    /// last successfully processed cycle, so they match the operator/sink state a
    /// forced (non-barrier) checkpoint captures — without them such a checkpoint would
    /// advance the recovery point with no source offset and replay from the start.
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

    /// Discard staged offsets when `execute_cycle` fails.
    fn discard_pending_offsets(&mut self) {
        for slot in &mut self.pending_offsets {
            *slot = None;
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
        if !self.pending_barrier.active && !callback.is_leader() {
            self.pending_barrier
                .reset(barrier.checkpoint_id, self.source_handles.len());
        }

        if !self.pending_barrier.active
            || barrier.checkpoint_id != self.pending_barrier.checkpoint_id
        {
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
            let checkpoint_id = self.pending_barrier.checkpoint_id;
            match callback
                .checkpoint_with_barrier(checkpoints, checkpoint_id)
                .await
            {
                BarrierOutcome::Committed(epoch) => {
                    self.broadcast_epoch_committed(epoch, &fan_out);
                    // Wire barrier = durable epoch.
                    callback.publish_barrier(epoch, checkpoint_id);
                }
                BarrierOutcome::Async => {
                    self.pending_barrier.active = false;
                    self.last_checkpoint = Instant::now();
                }
                BarrierOutcome::Skipped(reason) => {
                    tracing::debug!(checkpoint_id, reason = %reason, "barrier checkpoint skipped");
                }
                BarrierOutcome::Failed => {
                    tracing::warn!(
                        checkpoint_id,
                        "barrier checkpoint failed, will retry on next interval"
                    );
                }
            }
            self.pending_barrier.active = false;
            self.last_checkpoint = Instant::now();
        }
    }

    /// Trigger a periodic checkpoint if the interval has elapsed and admission permits.
    async fn maybe_checkpoint(&mut self, callback: &mut impl PipelineCallback) {
        if self.pending_barrier.active {
            return;
        }
        if self.checkpoint_in_flight.load(Ordering::Acquire) >= self.max_in_flight_epochs {
            return;
        }
        if self.staged_bytes.load(Ordering::Acquire) >= self.max_staged_bytes {
            warn_staged_cap_throttled(
                self.staged_bytes.load(Ordering::Acquire),
                self.max_staged_bytes,
            );
            return;
        }

        // Give the callback a chance to run follower polling. On non-leaders this routes
        // to `maybe_follower_checkpoint` so gossip PREPARE announcements are picked up
        // even with no data-path events. The offsets also feed any drained
        // `db.checkpoint()` request (e.g. the rebalance pre-rotation drain) so a forced
        // checkpoint records source offsets instead of advancing the recovery point blind.
        let offsets = self.current_source_offsets();
        if let Some(epoch) = callback.maybe_checkpoint(false, offsets).await {
            self.broadcast_epoch_committed(epoch, &FxHashMap::default());
        }

        let interval_due = callback.is_leader()
            && self
                .config
                .checkpoint_interval
                .is_some_and(|interval| self.last_checkpoint.elapsed() >= interval);

        // Hold the interval while a rebalance is converging: a checkpoint started before
        // a respawned node has adopted the assignment align-waits on its not-yet-flowing
        // shuffle barrier and times out. The verdict is a local borrow, so don't bump
        // `last_checkpoint` — the first post-convergence checkpoint fires immediately.
        if interval_due && !callback.assignment_ready_for_checkpoint().await {
            return;
        }

        // Explicit connector checkpoint requests are honored regardless of convergence.
        let should_checkpoint = interval_due
            || (callback.is_leader()
                && self
                    .checkpoint_request_flags
                    .iter()
                    .any(|f| f.swap(false, Ordering::AcqRel)));

        if !should_checkpoint {
            return;
        }

        if self.source_handles.is_empty() {
            let offsets = FxHashMap::default();
            if let Some(epoch) = callback.maybe_checkpoint(false, offsets).await {
                self.last_checkpoint = Instant::now();
                self.broadcast_epoch_committed(epoch, &FxHashMap::default());
            }
            return;
        }

        let checkpoint_id = if let Some(external_id) = callback.next_checkpoint_id() {
            self.next_checkpoint_id = external_id + 1;
            external_id
        } else {
            let id = self.next_checkpoint_id;
            self.next_checkpoint_id += 1;
            id
        };
        self.pending_barrier
            .reset(checkpoint_id, self.source_handles.len());

        for handle in &self.source_handles {
            handle.barrier_injector.trigger(checkpoint_id, 0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Minimal mock callback for testing the coordinator loop.
    struct MockCallback {
        cycle_count: u32,
        results: Vec<FxHashMap<Arc<str>, Vec<RecordBatch>>>,
        watermark: i64,
        /// Optional shared flag set when `maybe_checkpoint(force=true)` fires.
        force_checkpoint_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
        /// Fail on this 1-based cycle number.
        fatal_at_cycle: Option<u32>,
        cycle_errors: Arc<AtomicU64>,
    }

    impl MockCallback {
        fn new() -> Self {
            Self {
                cycle_count: 0,
                results: Vec::new(),
                watermark: 0,
                force_checkpoint_flag: None,
                fatal_at_cycle: None,
                cycle_errors: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl PipelineCallback for MockCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, CycleError> {
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
            Ok(results)
        }

        fn note_cycle_error(&self) {
            self.cycle_errors.fetch_add(1, Ordering::SeqCst);
        }

        fn push_to_streams(&self, _results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {}
        async fn write_to_sinks(&mut self, _results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {}

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

        async fn maybe_checkpoint(
            &mut self,
            force: bool,
            _source_offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> Option<u64> {
            if force {
                if let Some(ref flag) = self.force_checkpoint_flag {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                Some(1)
            } else {
                None
            }
        }

        async fn checkpoint_with_barrier(
            &mut self,
            _source_checkpoints: FxHashMap<String, SourceCheckpoint>,
            _checkpoint_id: u64,
        ) -> BarrierOutcome {
            BarrierOutcome::Committed(1)
        }

        fn record_cycle(&self, _events: u64, _batches: u64, _elapsed_ns: u64) {}
        async fn poll_tables(&mut self) {}
        fn apply_control(&mut self, _msg: crate::pipeline::ControlMsg) {}
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
                barrier_alignment_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown,
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
        }
    }

    fn int_batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    #[tokio::test]
    async fn fatal_cycle_error_faults_and_skips_final_checkpoint_exactly_once() {
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

        let force_flag = Arc::new(AtomicBool::new(false));
        let mut callback = MockCallback::new();
        callback.force_checkpoint_flag = Some(Arc::clone(&force_flag));
        callback.fatal_at_cycle = Some(1);

        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch: int_batch(1),
            checkpoint: SourceCheckpoint::new(1),
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
        assert!(
            !force_flag.load(Ordering::SeqCst),
            "the final checkpoint must be skipped on the fault path"
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
            checkpoint: SourceCheckpoint::new(1),
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
                barrier_alignment_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
        };

        let callback = MockCallback::new();

        // Send a batch.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
            checkpoint: SourceCheckpoint::new(1),
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
                barrier_alignment_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            },
            rx,
            source_handles: vec![handle],
            source_names: vec![Arc::from("wedged")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
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

    /// Test that a final checkpoint is triggered on shutdown when checkpointing
    /// is enabled.
    #[tokio::test]
    async fn test_final_checkpoint_on_shutdown() {
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
                barrier_alignment_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("test_source")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
        };

        let force_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut callback = MockCallback::new();
        callback.force_checkpoint_flag = Some(Arc::clone(&force_flag));

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
            checkpoint: SourceCheckpoint::new(1),
        })
        .await
        .unwrap();

        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            shutdown_clone.notify_one();
        });

        coordinator.run(callback).await;

        assert!(
            force_flag.load(std::sync::atomic::Ordering::SeqCst),
            "final checkpoint with force=true should have been called"
        );
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
                barrier_alignment_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            },
            rx: mpsc::bounded_async::<SourceMsg>(64).1, // dummy, not used
            source_handles: Vec::new(),
            source_names: vec![Arc::from("s0"), Arc::from("s1")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
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
        let barrier = CheckpointBarrier::new(1, 0);

        coordinator.process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: batch_1,
                checkpoint: SourceCheckpoint::new(10),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Barrier {
                source_idx: 0,
                barrier,
                checkpoint: SourceCheckpoint::new(10),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: batch_2,
                checkpoint: SourceCheckpoint::new(20),
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
                checkpoint: SourceCheckpoint::new(5),
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Barrier {
                source_idx: 1,
                barrier,
                checkpoint: SourceCheckpoint::new(5),
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
            coordinator.pending_offsets[0].as_ref().unwrap().epoch(),
            10,
            "s0 pending offset should be the pre-barrier batch"
        );
        assert_eq!(
            coordinator.pending_offsets[1].as_ref().unwrap().epoch(),
            5,
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
            coordinator.committed_offsets[0].as_ref().unwrap().epoch(),
            10,
            "s0 committed after cycle"
        );
        assert_eq!(
            coordinator.committed_offsets[1].as_ref().unwrap().epoch(),
            5,
            "s1 committed after cycle"
        );

        // Barriers should have both sources.
        assert_eq!(barriers.len(), 2, "should have barriers from both sources");
    }

    #[allow(clippy::disallowed_types)] // test-only: std::sync::Mutex is fine here
    struct BackpressuredCallback {
        inner: MockCallback,
        cycle_count: Arc<std::sync::atomic::AtomicU32>,
        events_per_cycle: Arc<std::sync::Mutex<Vec<u64>>>,
    }

    impl BackpressuredCallback {
        #[allow(clippy::disallowed_types)]
        fn new(
            cycle_count: Arc<std::sync::atomic::AtomicU32>,
            events_per_cycle: Arc<std::sync::Mutex<Vec<u64>>>,
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
        ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, CycleError> {
            self.cycle_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let total: u64 = source_batches
                .values()
                .flat_map(|bs| bs.iter())
                .map(|b| b.num_rows() as u64)
                .sum();
            self.events_per_cycle.lock().unwrap().push(total);
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
        async fn maybe_checkpoint(
            &mut self,
            force: bool,
            offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> Option<u64> {
            self.inner.maybe_checkpoint(force, offsets).await
        }
        async fn checkpoint_with_barrier(
            &mut self,
            cp: FxHashMap<String, SourceCheckpoint>,
            checkpoint_id: u64,
        ) -> BarrierOutcome {
            self.inner.checkpoint_with_barrier(cp, checkpoint_id).await
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
                barrier_alignment_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("src")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
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
                checkpoint: SourceCheckpoint::new(u64::try_from(i).unwrap()),
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
        #[allow(clippy::disallowed_types)]
        let events_per_cycle = Arc::new(std::sync::Mutex::new(Vec::new()));
        let callback =
            BackpressuredCallback::new(Arc::clone(&cycle_count), Arc::clone(&events_per_cycle));
        coordinator.run(callback).await;

        let cycles = cycle_count.load(std::sync::atomic::Ordering::SeqCst);
        let epc = events_per_cycle.lock().unwrap();
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

    #[allow(clippy::disallowed_types)] // test-only: std::sync::Mutex is fine here
    struct StateBudgetCallback {
        inner: MockCallback,
        events_per_cycle: Arc<std::sync::Mutex<Vec<u64>>>,
        idle_ticks: Arc<std::sync::atomic::AtomicU32>,
    }

    impl PipelineCallback for StateBudgetCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            watermark: i64,
        ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, CycleError> {
            let total: u64 = source_batches
                .values()
                .flat_map(|bs| bs.iter())
                .map(|b| b.num_rows() as u64)
                .sum();
            self.events_per_cycle.lock().unwrap().push(total);
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
        async fn maybe_checkpoint(
            &mut self,
            force: bool,
            offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> Option<u64> {
            self.inner.maybe_checkpoint(force, offsets).await
        }
        async fn checkpoint_with_barrier(
            &mut self,
            cp: FxHashMap<String, SourceCheckpoint>,
            checkpoint_id: u64,
        ) -> BarrierOutcome {
            self.inner.checkpoint_with_barrier(cp, checkpoint_id).await
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
                barrier_alignment_timeout: Duration::from_secs(30),
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                max_input_buf_batches: 256,
                max_input_buf_bytes: None,
                backpressure_policy: crate::config::BackpressurePolicy::Backpressure,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("src")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
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
            checkpoint_in_flight: Arc::new(AtomicU64::new(0)),
            max_in_flight_epochs: 1,
            staged_bytes: Arc::new(AtomicU64::new(0)),
            max_staged_bytes: u64::MAX,
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
                checkpoint: SourceCheckpoint::new(u64::try_from(i).unwrap()),
            })
            .await
            .unwrap();
        }

        #[allow(clippy::disallowed_types)]
        let events_per_cycle = Arc::new(std::sync::Mutex::new(Vec::new()));
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

        let epc = events_per_cycle.lock().unwrap();
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
