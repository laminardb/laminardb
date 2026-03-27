//! Simplified pipeline coordinator.
//!
//! Sources push directly to the coordinator via `tokio::sync::mpsc`.
//! The coordinator runs on a dedicated single-threaded tokio runtime
//! (`laminar-compute` thread), isolating CPU-bound event processing
//! from IO tasks (Kafka poll, S3 checkpoint writes, HTTP) on the main
//! work-stealing runtime. SQL execution is delegated to [`PipelineCallback`].
//!
//! # Architecture
//!
//! ```text
//! Source task (main tokio runtime)
//!   │  connector.poll_batch().await
//!   │
//!   └──── mpsc::Sender ────► StreamingCoordinator (dedicated compute thread)
//!                                 │  callback.execute_cycle()
//!                                 │  callback.write_to_sinks()
//!                                 ▼
//!                               Sinks
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::alloc::{PriorityClass, PriorityGuard};
use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::mpsc;

use super::callback::{PipelineCallback, SourceRegistration};
use super::config::PipelineConfig;
use crate::error::DbError;

/// Message sent from a source task to the coordinator.
///
/// Each variant carries the [`SourceCheckpoint`] captured at the point the
/// message was produced.  Co-locating the offset with the data guarantees
/// the coordinator never checkpoints an offset for data it has not yet
/// processed (eliminates the offset-before-batch race).
enum SourceMsg {
    /// A batch of records from a source.
    Batch {
        source_idx: usize,
        batch: RecordBatch,
        /// Offset snapshot captured *after* `poll_batch` drained the reader
        /// channel.  Committed to `committed_offsets` only when the batch
        /// is actually processed (not when deferred to `post_barrier_buf`).
        checkpoint: SourceCheckpoint,
    },
    /// Checkpoint barrier injected at the source.
    Barrier {
        source_idx: usize,
        barrier: CheckpointBarrier,
        /// Offset snapshot captured at barrier injection (consistent cut).
        checkpoint: SourceCheckpoint,
    },
}

/// Handle to a running source I/O task.
struct SourceHandle {
    name: Arc<str>,
    shutdown: Arc<tokio::sync::Notify>,
    join: tokio::task::JoinHandle<()>,
    /// Injector for Chandy-Lamport checkpoint barriers.
    barrier_injector: CheckpointBarrierInjector,
}

/// Simplified pipeline coordinator — single tokio task, no core threads.
///
/// Replaces `TpcPipelineCoordinator` + `TpcRuntime` + `Reactor` with a
/// direct source → coordinator → sink pipeline.
pub struct StreamingCoordinator {
    config: PipelineConfig,
    /// Receives all source messages (batches + barriers).
    rx: mpsc::Receiver<SourceMsg>,
    /// Handles to source tasks (for shutdown + checkpoint queries).
    source_handles: Vec<SourceHandle>,
    /// Source name cache indexed by `source_idx`.
    source_names: Vec<Arc<str>>,
    /// Shutdown signal.
    shutdown: Arc<tokio::sync::Notify>,
    /// Pending barrier alignment.
    pending_barrier: PendingBarrier,
    /// Next checkpoint ID for barrier injection.
    next_checkpoint_id: u64,
    /// Last checkpoint time.
    last_checkpoint: Instant,
    /// Source-initiated checkpoint request flags.
    checkpoint_request_flags: Vec<Arc<AtomicBool>>,
    /// Pre-allocated source batches buffer (cleared per cycle).
    source_batches_buf: FxHashMap<Arc<str>, Vec<RecordBatch>>,
    /// Batches received after a barrier from the same source in the same
    /// drain cycle. These belong to the NEXT checkpoint epoch and must not
    /// be included in the current checkpoint state. Bounded in practice by
    /// `channel_capacity` (max messages available per drain cycle).
    post_barrier_buf: Vec<SourceMsg>,
    /// Source indices that have delivered a barrier during the current drain
    /// cycle. Any subsequent batch from these sources goes to
    /// `post_barrier_buf`.
    barrier_seen: FxHashSet<usize>,
    /// Latest durably-processed source offset per source index.  Merged
    /// from `pending_offsets` only after a successful `execute_cycle`.
    committed_offsets: Vec<Option<SourceCheckpoint>>,
    /// Offsets staged by `process_msg`.  Merged into `committed_offsets`
    /// after a successful `execute_cycle`, discarded on failure.
    pending_offsets: Vec<Option<SourceCheckpoint>>,
    /// Control channel for live DDL (add/drop stream) from `LaminarDB`.
    control_rx: mpsc::Receiver<super::ControlMsg>,
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

/// Barrier alignment timeout.
const BARRIER_TIMEOUT: Duration = Duration::from_secs(60);

impl StreamingCoordinator {
    /// Create a new streaming coordinator.
    ///
    /// Spawns a tokio task for each source that polls the connector and
    /// sends batches/barriers to the coordinator via mpsc.
    ///
    /// # Errors
    ///
    /// Returns an error if delivery guarantee constraints are violated
    /// or if any source connector fails to open.
    #[allow(clippy::too_many_lines)]
    pub async fn new(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
        control_rx: mpsc::Receiver<super::ControlMsg>,
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

        // Channel for all source messages.
        let (tx, rx) = mpsc::channel(config.channel_capacity);

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

            // Open connector eagerly so startup fails fast on bad config.
            connector
                .open(&connector_config)
                .await
                .map_err(|e| DbError::Config(format!("source '{src_name}' open failed: {e}")))?;

            // Restore checkpoint if available.
            if let Some(ref cp) = restore {
                if let Err(e) = connector.restore(cp).await {
                    tracing::warn!(
                        source = %src_name, error = %e,
                        "source restore failed, starting from beginning"
                    );
                }
            }

            // Seed committed_offsets with the restore checkpoint so a
            // shutdown before any data still checkpoints the restore position.
            committed_offsets.push(src.restore_checkpoint);

            // Barrier injection: coordinator triggers → source polls → sends SourceMsg::Barrier
            let barrier_injector = CheckpointBarrierInjector::new();
            let barrier_handle = barrier_injector.handle();

            let join = tokio::spawn(async move {
                let mut epoch: u64 = 0;

                // Poll loop — tokio::select! ensures shutdown cancels a
                // long-running poll_batch without waiting for it to return.
                loop {
                    let poll_result = tokio::select! {
                        biased;
                        () = task_shutdown_clone.notified() => break,
                        result = connector.poll_batch(max_poll) => result,
                    };

                    match poll_result {
                        Ok(Some(batch)) => {
                            // Offset travels with the batch — no separate
                            // watch channel.  The coordinator commits the
                            // offset only after processing the batch.
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
                            // No data — sleep briefly (cancellable).
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

                    // Poll for pending checkpoint barrier.
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

                // Drain remaining data from the connector's internal
                // buffer before closing — close() drops the buffer and
                // anything not drained here is lost.
                while let Ok(Some(batch)) = connector.poll_batch(max_poll).await {
                    let cp = connector.checkpoint();
                    let msg = SourceMsg::Batch {
                        source_idx: idx,
                        batch: batch.records,
                        checkpoint: cp,
                    };
                    if task_tx.send(msg).await.is_err() {
                        break;
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
            barrier_seen: FxHashSet::default(),
            pending_offsets: vec![None; committed_offsets.len()],
            committed_offsets,
            control_rx,
        })
    }

    /// Run the coordinator loop.
    ///
    /// Receives batches from sources via mpsc, executes SQL cycles via
    /// the callback, and handles checkpoint barriers. Returns when
    /// shutdown is signaled.
    ///
    /// Cycle priority ordering:
    /// 1. Shutdown signal (biased select — checked first)
    /// 2. Event drain + SQL execution (up to `MAX_DRAIN_PER_CYCLE` messages)
    /// 3. Barrier alignment (after SQL so checkpoint state includes processed data)
    /// 4. Periodic checkpoint (if interval elapsed)
    /// 5. Table source polling (idle maintenance)
    /// 6. Barrier timeout check
    #[allow(clippy::too_many_lines)]
    pub async fn run<C: PipelineCallback>(mut self, mut callback: C) {
        /// Maximum messages to drain per cycle before yielding for maintenance work.
        const MAX_DRAIN_PER_CYCLE: usize = 10_000;

        let batch_window = self.config.batch_window;
        let mut barriers_buf: Vec<(usize, CheckpointBarrier, SourceCheckpoint)> = Vec::new();

        loop {
            // Step: Wait for data, shutdown, or idle timeout.
            let msg = tokio::select! {
                biased;
                () = self.shutdown.notified() => break,
                msg = self.rx.recv() => {
                    match msg {
                        Some(m) => {
                            // If batch_window > 0, coalesce: sleep briefly
                            // to let more data accumulate.
                            if !batch_window.is_zero() {
                                tokio::time::sleep(batch_window).await;
                            }
                            Some(m)
                        }
                        None => break, // All senders dropped
                    }
                }
                () = tokio::time::sleep(IDLE_TIMEOUT) => None,
            };

            // Step: Drain messages and coalesce batches.
            let event_priority = PriorityGuard::enter(PriorityClass::EventProcessing);
            self.source_batches_buf.clear();
            self.barrier_seen.clear();
            self.discard_pending_offsets();
            barriers_buf.clear();
            let mut cycle_events: u64 = 0;
            let cycle_start = Instant::now();

            // First, drain any post-barrier messages deferred from the
            // previous cycle — these are pre-next-barrier data that should
            // be processed in this cycle.
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

            // Drain any additional buffered messages (batch coalescing).
            // Terminates on count limit, time budget, or backpressure.
            // Only check backpressure on active wakeups to avoid bumping
            // the counter on idle timeouts.
            let mut drain_count = 0;
            let drain_budget_ns = self.config.drain_budget_ns;
            let backpressured = had_data && callback.is_backpressured();
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

            // Step: Execute SQL cycle. Also runs on idle wakeups when
            // operators have deferred input from a prior budget-exceeded
            // cycle — otherwise that data is stuck forever once the source
            // goes idle.
            if !self.source_batches_buf.is_empty() || callback.has_deferred_input() {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(results) => {
                        self.commit_pending_offsets();
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => {
                        self.discard_pending_offsets();
                        tracing::warn!(error = %e, "[LDB-3020] SQL cycle error");
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

            // Hoist elapsed_ns so the background phase can use it.
            #[allow(clippy::cast_possible_truncation)]
            let cycle_elapsed_ns = cycle_start.elapsed().as_nanos() as u64;

            drop(event_priority);
            let _bg_priority = PriorityGuard::enter(PriorityClass::BackgroundIo);
            let bg_start = Instant::now();
            let bg_budget = self.config.background_budget_ns;

            // Step: Handle barriers — always process (cheap, O(num_sources)
            // hash lookups, must not be deferred for correctness).
            for (source_idx, barrier, cp) in &barriers_buf {
                self.handle_barrier(*source_idx, barrier, cp, &mut callback)
                    .await;
            }

            // Step: Periodic checkpoint — skip if background budget already
            // exhausted (checkpoint is expensive I/O).
            #[allow(clippy::cast_possible_truncation)]
            if (bg_start.elapsed().as_nanos() as u64) < bg_budget {
                self.maybe_checkpoint(&mut callback).await;
            }

            // Step: Poll table sources — skip if cycle OR background budget
            // exceeded (lowest priority background work).
            #[allow(clippy::cast_possible_truncation)]
            let bg_elapsed = bg_start.elapsed().as_nanos() as u64;
            if cycle_elapsed_ns < self.config.cycle_budget_ns && bg_elapsed < bg_budget {
                callback.poll_tables().await;
            } else {
                tracing::debug!("skipping poll_tables (budget exhausted)");
            }

            // Step: Drain control messages (add/drop stream DDL).
            // Processed AFTER checkpoint so newly added queries don't have
            // inconsistent state in the checkpoint.
            while let Ok(msg) = self.control_rx.try_recv() {
                callback.apply_control(msg);
            }

            // Step: Barrier timeout check.
            if self.pending_barrier.active
                && self.pending_barrier.started_at.elapsed() > BARRIER_TIMEOUT
            {
                tracing::warn!(
                    checkpoint_id = self.pending_barrier.checkpoint_id,
                    "Barrier alignment timeout — cancelling checkpoint"
                );
                self.pending_barrier.active = false;
            }
        }

        // ── Shutdown ──
        // Signal source tasks to stop.
        for handle in &self.source_handles {
            handle.shutdown.notify_one();
        }

        // Drain rx BEFORE joining source tasks. Source tasks may be
        // blocked on task_tx.send() (channel full because the pipeline
        // is slower than the source). Draining first frees channel
        // slots so those source tasks can unblock, see the shutdown
        // signal, and exit. Joining before draining deadlocks.
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

        if !self.source_batches_buf.is_empty() || callback.has_deferred_input() {
            let wm = callback.current_watermark();
            match callback.execute_cycle(&self.source_batches_buf, wm).await {
                Ok(results) => {
                    self.commit_pending_offsets();
                    callback.push_to_streams(&results);
                    callback.write_to_sinks(&results).await;
                }
                Err(e) => {
                    self.discard_pending_offsets();
                    tracing::warn!(error = %e, "[LDB-3020] SQL cycle error during shutdown drain");
                }
            }
        }

        // Join source tasks (unblocked by the drain above).
        for handle in std::mem::take(&mut self.source_handles) {
            if let Err(e) = handle.join.await {
                tracing::warn!(source = %handle.name, error = ?e, "source task panicked");
            }
        }

        // Final drain: pick up any messages source tasks sent between
        // the first drain and their exit.
        self.source_batches_buf.clear();
        self.barrier_seen.clear();
        self.discard_pending_offsets();
        drain_barriers.clear();
        while let Ok(msg) = self.rx.try_recv() {
            self.process_msg(msg, &mut callback, &mut drain_barriers, &mut drain_events);
        }
        if !self.source_batches_buf.is_empty() || callback.has_deferred_input() {
            let wm = callback.current_watermark();
            match callback.execute_cycle(&self.source_batches_buf, wm).await {
                Ok(results) => {
                    self.commit_pending_offsets();
                    callback.push_to_streams(&results);
                    callback.write_to_sinks(&results).await;
                }
                Err(e) => {
                    self.discard_pending_offsets();
                    tracing::warn!(error = %e, "[LDB-3020] SQL cycle error during final drain");
                }
            }
        }

        // Final checkpoint uses committed_offsets — only reflects data
        // that successfully passed through execute_cycle.
        let checkpoint_enabled = self.config.checkpoint_interval.is_some();
        if checkpoint_enabled {
            let source_offsets: FxHashMap<String, SourceCheckpoint> = self
                .committed_offsets
                .iter()
                .enumerate()
                .filter_map(|(idx, cp)| {
                    cp.as_ref().and_then(|c| {
                        self.source_names
                            .get(idx)
                            .map(|name| (name.to_string(), c.clone()))
                    })
                })
                .collect();
            if callback.maybe_checkpoint(true, source_offsets).await {
                tracing::info!("final checkpoint completed before shutdown");
            }
        }
    }

    /// Process a single source message.
    ///
    /// When a barrier is seen from a source, subsequent batches from that
    /// source are diverted to `post_barrier_buf` to ensure they are not
    /// included in the current checkpoint state.
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
                // If this source already delivered a barrier in this drain
                // cycle, this batch is post-barrier data — defer it.
                if self.barrier_seen.contains(&source_idx) {
                    self.post_barrier_buf.push(SourceMsg::Batch {
                        source_idx,
                        batch,
                        checkpoint,
                    });
                    return;
                }

                // Stage offset (committed after successful execute_cycle).
                if source_idx < self.pending_offsets.len() {
                    self.pending_offsets[source_idx] = Some(checkpoint);
                }

                if let Some(name) = self.source_names.get(source_idx) {
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        *cycle_events += batch.num_rows() as u64;
                    }
                    // Filter BEFORE advancing the watermark — otherwise the
                    // batch's own max timestamp advances the watermark and
                    // then rows below that max are dropped from the same batch.
                    if let Some(filtered) = callback.filter_late_rows(name, &batch) {
                        self.source_batches_buf
                            .entry(Arc::clone(name))
                            .or_default()
                            .push(filtered);
                    }
                    callback.extract_watermark(name, &batch);
                }
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
                checkpoint,
            } => {
                self.barrier_seen.insert(source_idx);
                barriers.push((source_idx, barrier, checkpoint));
            }
        }
    }

    /// Merge staged offsets into `committed_offsets`.  Called after a
    /// successful `execute_cycle`.
    fn commit_pending_offsets(&mut self) {
        for (i, pending) in self.pending_offsets.iter_mut().enumerate() {
            if let Some(cp) = pending.take() {
                self.committed_offsets[i] = Some(cp);
            }
        }
    }

    /// Discard staged offsets.  Called when `execute_cycle` fails.
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
        if !self.pending_barrier.active
            || barrier.checkpoint_id != self.pending_barrier.checkpoint_id
        {
            return;
        }

        // Use the checkpoint that traveled atomically with the barrier
        // message — no watch-channel race.
        if let Some(name) = self.source_names.get(source_idx) {
            self.pending_barrier
                .source_checkpoints
                .insert(name.to_string(), barrier_checkpoint.clone());
        }

        self.pending_barrier.sources_aligned.insert(source_idx);

        // Check if all sources aligned.
        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            let checkpoints = std::mem::take(&mut self.pending_barrier.source_checkpoints);
            let ok = callback.checkpoint_with_barrier(checkpoints).await;
            if ok {
                self.pending_barrier.active = false;
                self.last_checkpoint = Instant::now();
            } else {
                tracing::warn!(
                    checkpoint_id = self.pending_barrier.checkpoint_id,
                    "barrier checkpoint failed, will retry on next interval"
                );
                // Keep active=true so next periodic trigger retries.
                // Source checkpoints were consumed; sources will re-checkpoint
                // on the next barrier injection.
                self.pending_barrier.active = false;
            }
        }
    }

    /// Check if a periodic checkpoint should be triggered.
    ///
    /// When barriers are supported (sources present), injects barriers for
    /// Chandy-Lamport consistent snapshots. When no sources are present,
    /// falls back to timer-based offset-only checkpoints.
    async fn maybe_checkpoint(&mut self, callback: &mut impl PipelineCallback) {
        if self.pending_barrier.active {
            return; // Already tracking a barrier.
        }

        let should_checkpoint = self
            .config
            .checkpoint_interval
            .is_some_and(|interval| self.last_checkpoint.elapsed() >= interval)
            || self
                .checkpoint_request_flags
                .iter()
                .any(|f| f.swap(false, Ordering::AcqRel));

        if !should_checkpoint {
            return;
        }

        if self.source_handles.is_empty() {
            // No sources — timer-based checkpoint only.
            let offsets = FxHashMap::default();
            if callback.maybe_checkpoint(false, offsets).await {
                self.last_checkpoint = Instant::now();
            }
            return;
        }

        // Inject barriers into all source tasks for Chandy-Lamport alignment.
        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;
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
    }

    impl MockCallback {
        fn new() -> Self {
            Self {
                cycle_count: 0,
                results: Vec::new(),
                watermark: 0,
                force_checkpoint_flag: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl PipelineCallback for MockCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, String> {
            self.cycle_count += 1;
            // Pass through source batches as results.
            let results: FxHashMap<Arc<str>, Vec<RecordBatch>> = source_batches
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            self.results.push(results.clone());
            Ok(results)
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
        ) -> bool {
            if force {
                if let Some(ref flag) = self.force_checkpoint_flag {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
            force
        }

        async fn checkpoint_with_barrier(
            &mut self,
            _source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        ) -> bool {
            true
        }

        fn record_cycle(&self, _events: u64, _batches: u64, _elapsed_ns: u64) {}
        async fn poll_tables(&mut self) {}
        fn apply_control(&mut self, _msg: crate::pipeline::ControlMsg) {}
    }

    /// Test that the coordinator processes messages via direct mpsc channel.
    #[tokio::test]
    async fn test_coordinator_direct_channel() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::channel(64);

        // Create coordinator directly (bypassing source spawning).
        let (_control_tx, control_rx) = mpsc::channel(64);
        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                barrier_alignment_timeout: BARRIER_TIMEOUT,
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                sink_write_timeout: Duration::from_secs(30),
                max_input_buf_batches: 256,
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
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
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

    /// Test that a final checkpoint is triggered on shutdown when checkpointing
    /// is enabled.
    #[tokio::test]
    async fn test_final_checkpoint_on_shutdown() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::channel(64);
        let (_control_tx, control_rx) = mpsc::channel(64);

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: Some(Duration::from_secs(60)),
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                barrier_alignment_timeout: BARRIER_TIMEOUT,
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                sink_write_timeout: Duration::from_secs(30),
                max_input_buf_batches: 256,
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
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
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

        let (_control_tx2, control_rx2) = mpsc::channel(64);
        let mut coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                barrier_alignment_timeout: BARRIER_TIMEOUT,
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                sink_write_timeout: Duration::from_secs(30),
                max_input_buf_batches: 256,
            },
            rx: mpsc::channel(64).1, // dummy, not used
            source_handles: Vec::new(),
            source_names: vec![Arc::from("s0"), Arc::from("s1")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None, None],
            pending_offsets: vec![None, None],
            control_rx: control_rx2,
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

    // ── Backpressure drain-skip test ─────────────────────────────

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

    #[async_trait::async_trait]
    impl PipelineCallback for BackpressuredCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            watermark: i64,
        ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, String> {
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
        ) -> bool {
            self.inner.maybe_checkpoint(force, offsets).await
        }
        async fn checkpoint_with_barrier(
            &mut self,
            cp: FxHashMap<String, SourceCheckpoint>,
        ) -> bool {
            self.inner.checkpoint_with_barrier(cp).await
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
        let (tx, rx) = mpsc::channel(64);
        let (_control_tx, control_rx) = mpsc::channel(64);

        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                barrier_alignment_timeout: BARRIER_TIMEOUT,
                cycle_budget_ns: 10_000_000,
                drain_budget_ns: 1_000_000,
                query_budget_ns: 8_000_000,
                background_budget_ns: 5_000_000,
                sink_write_timeout: Duration::from_secs(30),
                max_input_buf_batches: 256,
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
            barrier_seen: FxHashSet::default(),
            committed_offsets: vec![None],
            pending_offsets: vec![None],
            control_rx,
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
}
