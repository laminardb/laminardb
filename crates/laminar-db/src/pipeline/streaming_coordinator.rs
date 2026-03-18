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
use laminar_core::checkpoint::{
    CheckpointBarrier, CheckpointBarrierInjector, TrackerEvent, UnalignedBarrierTracker,
};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::mpsc;

use super::callback::{PipelineCallback, SourceRegistration};
use super::config::PipelineConfig;
use crate::error::DbError;

/// Message sent from a source task to the coordinator.
enum SourceMsg {
    /// A batch of records from a source.
    Batch {
        source_idx: usize,
        batch: RecordBatch,
    },
    /// Checkpoint barrier injected at the source.
    Barrier {
        source_idx: usize,
        barrier: CheckpointBarrier,
    },
}

/// Handle to a running source I/O task.
struct SourceHandle {
    name: Arc<str>,
    shutdown: Arc<tokio::sync::Notify>,
    join: tokio::task::JoinHandle<()>,
    checkpoint_rx: tokio::sync::watch::Receiver<SourceCheckpoint>,
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
    /// be included in the current checkpoint state.
    post_barrier_buf: Vec<SourceMsg>,
    /// Source indices that have delivered a barrier during the current drain
    /// cycle. Any subsequent batch from these sources goes to
    /// `post_barrier_buf`.
    barrier_seen: FxHashSet<usize>,
    /// Control channel for live DDL (add/drop stream) from `LaminarDB`.
    control_rx: mpsc::Receiver<super::ControlMsg>,
    /// Unaligned checkpoint barrier tracker (when configured).
    unaligned_tracker: Option<UnalignedBarrierTracker>,
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

        // Channel for all source messages.
        let (tx, rx) = mpsc::channel(8192);

        let mut source_handles = Vec::with_capacity(sources.len());
        let mut source_names = Vec::with_capacity(sources.len());
        let mut checkpoint_request_flags = Vec::new();

        for (idx, src) in sources.into_iter().enumerate() {
            if let Some(flag) = src.connector.checkpoint_requested() {
                checkpoint_request_flags.push(flag);
            }

            // Initialize watch with the restored checkpoint (not zero) so
            // maybe_checkpoint() sees correct offsets from the start.
            let initial_cp = src
                .restore_checkpoint
                .clone()
                .unwrap_or_else(|| SourceCheckpoint::new(0));
            let (cp_tx, cp_rx) = tokio::sync::watch::channel(initial_cp);
            let task_shutdown = Arc::new(tokio::sync::Notify::new());
            let task_shutdown_clone = Arc::clone(&task_shutdown);
            let task_tx = tx.clone();
            let max_poll = config.max_poll_records;
            let poll_interval = config.fallback_poll_interval;
            let src_name = src.name.clone();
            let restore = src.restore_checkpoint;
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

            // Publish initial offset after open/restore.
            let _ = cp_tx.send(connector.checkpoint());

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
                            // Publish offset BEFORE sending the batch so
                            // maybe_checkpoint() never sees an offset ahead
                            // of what the coordinator has consumed.
                            let _ = cp_tx.send(connector.checkpoint());

                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch: batch.records,
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
                        Err(e) => {
                            tracing::warn!(source = %src_name, error = %e, "poll error");
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
                        // Capture offset at barrier point (consistent cut).
                        let _ = cp_tx.send(connector.checkpoint());
                        let msg = SourceMsg::Barrier {
                            source_idx: idx,
                            barrier,
                        };
                        if task_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                }

                // Close connector.
                if let Err(e) = connector.close().await {
                    tracing::warn!(source = %src_name, error = %e, "source close error");
                }
            });

            let arc_name: Arc<str> = Arc::from(src.name.as_str());
            source_handles.push(SourceHandle {
                name: Arc::clone(&arc_name),
                shutdown: task_shutdown,
                join,
                checkpoint_rx: cp_rx,
                barrier_injector,
            });
            source_names.push(arc_name);
        }

        let unaligned_tracker = config
            .unaligned_checkpoint
            .as_ref()
            .filter(|c| c.enabled)
            .map(|c| UnalignedBarrierTracker::new(c.clone()));

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
            control_rx,
            unaligned_tracker,
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
        let mut barriers_buf: Vec<(usize, CheckpointBarrier)> = Vec::new();

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

            if let Some(first_msg) = msg {
                self.process_msg(
                    first_msg,
                    &mut callback,
                    &mut barriers_buf,
                    &mut cycle_events,
                );
            }

            // Drain any additional buffered messages (batch coalescing).
            // Terminates on count limit OR time budget, whichever comes first.
            let mut drain_count = 0;
            let drain_budget_ns = self.config.drain_budget_ns;
            #[allow(clippy::cast_possible_truncation)]
            while drain_count < MAX_DRAIN_PER_CYCLE
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

            // Step: Execute SQL cycle (before committing barriers so that
            // checkpoint state includes the processed batches).
            if !self.source_batches_buf.is_empty() {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(results) => {
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => {
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
            for (source_idx, barrier) in &barriers_buf {
                self.handle_barrier(*source_idx, barrier, &mut callback)
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

            // Step: Check unaligned checkpoint timeout (may switch to unaligned mode).
            self.check_unaligned_timeout(&mut callback).await;

            // Step: Barrier timeout check (hard cancel).
            if self.pending_barrier.active
                && self.pending_barrier.started_at.elapsed() > BARRIER_TIMEOUT
            {
                tracing::warn!(
                    checkpoint_id = self.pending_barrier.checkpoint_id,
                    "Barrier alignment timeout — cancelling checkpoint"
                );
                self.pending_barrier.active = false;
                if let Some(ref mut tracker) = self.unaligned_tracker {
                    tracker.cancel();
                }
            }
        }

        // Shutdown: signal all source tasks to stop producing, then await
        // completion so checkpoint_rx stops advancing before we read it.
        for handle in &self.source_handles {
            handle.shutdown.notify_one();
        }

        // Await each source task, then read its final (now-stable) checkpoint.
        let checkpoint_enabled = self.config.checkpoint_interval.is_some();
        let mut source_offsets = FxHashMap::default();
        for (idx, handle) in std::mem::take(&mut self.source_handles)
            .into_iter()
            .enumerate()
        {
            if let Err(e) = handle.join.await {
                tracing::warn!(source = %handle.name, error = ?e, "source task panicked");
            }
            if checkpoint_enabled {
                if let Some(name) = self.source_names.get(idx) {
                    source_offsets.insert(name.to_string(), handle.checkpoint_rx.borrow().clone());
                }
            }
        }

        if checkpoint_enabled && callback.maybe_checkpoint(true, source_offsets).await {
            tracing::info!("final checkpoint completed before shutdown");
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
        barriers: &mut Vec<(usize, CheckpointBarrier)>,
        cycle_events: &mut u64,
    ) {
        match msg {
            SourceMsg::Batch { source_idx, batch } => {
                // If this source already delivered a barrier in this drain
                // cycle, this batch is post-barrier data — defer it.
                if self.barrier_seen.contains(&source_idx) {
                    self.post_barrier_buf
                        .push(SourceMsg::Batch { source_idx, batch });
                    return;
                }

                if let Some(name) = self.source_names.get(source_idx) {
                    callback.extract_watermark(name, &batch);
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        *cycle_events += batch.num_rows() as u64;
                    }
                    if let Some(filtered) = callback.filter_late_rows(name, &batch) {
                        self.source_batches_buf
                            .entry(Arc::clone(name))
                            .or_default()
                            .push(filtered);
                    }
                }
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
            } => {
                self.barrier_seen.insert(source_idx);
                barriers.push((source_idx, barrier));
            }
        }
    }

    /// Handle a barrier from a source.
    async fn handle_barrier(
        &mut self,
        source_idx: usize,
        barrier: &CheckpointBarrier,
        callback: &mut impl PipelineCallback,
    ) {
        if !self.pending_barrier.active
            || barrier.checkpoint_id != self.pending_barrier.checkpoint_id
        {
            return;
        }

        // Record this source's checkpoint.
        if let Some(name) = self.source_names.get(source_idx) {
            let cp = self.source_handles[source_idx]
                .checkpoint_rx
                .borrow()
                .clone();
            self.pending_barrier
                .source_checkpoints
                .insert(name.to_string(), cp);
        }

        self.pending_barrier.sources_aligned.insert(source_idx);

        // Delegate to unaligned tracker if present.
        if let Some(ref mut tracker) = self.unaligned_tracker {
            let events = tracker.barrier_received(source_idx);
            for event in events {
                match event {
                    TrackerEvent::Aligned => {
                        self.complete_barrier_checkpoint(callback).await;
                        return;
                    }
                    TrackerEvent::LateBarrier(idx) => {
                        tracing::debug!(
                            checkpoint_id = self.pending_barrier.checkpoint_id,
                            source_idx = idx,
                            "late barrier arrived (unaligned mode)"
                        );
                    }
                    TrackerEvent::UnalignedComplete => {
                        tracing::debug!(
                            checkpoint_id = self.pending_barrier.checkpoint_id,
                            "all late barriers arrived after unaligned switch"
                        );
                        self.complete_barrier_checkpoint(callback).await;
                        return;
                    }
                    _ => {}
                }
            }
            return;
        }

        // Default aligned-only path.
        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            self.complete_barrier_checkpoint(callback).await;
        }
    }

    /// Complete a barrier checkpoint (all sources aligned).
    async fn complete_barrier_checkpoint(&mut self, callback: &mut impl PipelineCallback) {
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
            self.pending_barrier.active = false;
        }
    }

    /// Check unaligned checkpoint timeout and trigger fallback if needed.
    async fn check_unaligned_timeout(&mut self, callback: &mut impl PipelineCallback) {
        let missing_sources =
            self.unaligned_tracker
                .as_mut()
                .and_then(|tracker| match tracker.check_timeout() {
                    Some(TrackerEvent::SwitchedToUnaligned { missing_sources }) => {
                        tracker.cancel();
                        Some(missing_sources)
                    }
                    _ => None,
                });

        if let Some(missing_sources) = missing_sources {
            tracing::warn!(
                checkpoint_id = self.pending_barrier.checkpoint_id,
                missing = ?missing_sources,
                "barrier alignment timed out, switching to unaligned checkpoint"
            );
            self.complete_barrier_checkpoint(callback).await;
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
        let num_sources = self.source_handles.len();
        self.pending_barrier.reset(checkpoint_id, num_sources);

        // Begin tracking with the unaligned tracker (if configured).
        let mut barrier_flags: u64 = 0;
        if let Some(ref mut tracker) = self.unaligned_tracker {
            let events = tracker.begin(checkpoint_id, num_sources);
            for event in &events {
                if matches!(event, TrackerEvent::SwitchedToUnaligned { .. }) {
                    // force_unaligned — set the flag on the barrier.
                    barrier_flags |= laminar_core::checkpoint::flags::UNALIGNED;
                }
            }
        }

        for handle in &self.source_handles {
            handle
                .barrier_injector
                .trigger(checkpoint_id, barrier_flags);
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
        barrier_checkpoint_count: u32,
        /// Optional shared flag set when `maybe_checkpoint(force=true)` fires.
        force_checkpoint_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    impl MockCallback {
        fn new() -> Self {
            Self {
                cycle_count: 0,
                results: Vec::new(),
                watermark: 0,
                barrier_checkpoint_count: 0,
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
            self.barrier_checkpoint_count += 1;
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
                unaligned_checkpoint: None,
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
            control_rx,
            unaligned_tracker: None,
        };

        let callback = MockCallback::new();

        // Send a batch.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        tx.send(SourceMsg::Batch {
            source_idx: 0,
            batch,
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
                unaligned_checkpoint: None,
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
            control_rx,
            unaligned_tracker: None,
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
                unaligned_checkpoint: None,
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
            control_rx: control_rx2,
            unaligned_tracker: None,
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
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Barrier {
                source_idx: 0,
                barrier,
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Batch {
                source_idx: 0,
                batch: batch_2,
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
            },
            &mut callback,
            &mut barriers,
            &mut cycle_events,
        );
        coordinator.process_msg(
            SourceMsg::Barrier {
                source_idx: 1,
                barrier,
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

        // Barriers should have both sources.
        assert_eq!(barriers.len(), 2, "should have barriers from both sources");
    }

    #[tokio::test]
    async fn test_unaligned_timeout_commits_checkpoint() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (_control_tx, control_rx) = mpsc::channel(64);
        let mut coordinator = StreamingCoordinator {
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
                unaligned_checkpoint: Some(laminar_core::checkpoint::UnalignedCheckpointConfig {
                    enabled: true,
                    alignment_timeout_threshold: Duration::ZERO,
                    max_inflight_buffer_bytes: 1024,
                    force_unaligned: false,
                }),
            },
            rx: mpsc::channel(64).1,
            source_handles: Vec::new(),
            source_names: vec![Arc::from("s0"), Arc::from("s1")],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 2,
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
            post_barrier_buf: Vec::new(),
            barrier_seen: FxHashSet::default(),
            control_rx,
            unaligned_tracker: Some(UnalignedBarrierTracker::new(
                laminar_core::checkpoint::UnalignedCheckpointConfig {
                    enabled: true,
                    alignment_timeout_threshold: Duration::ZERO,
                    max_inflight_buffer_bytes: 1024,
                    force_unaligned: false,
                },
            )),
        };

        coordinator.pending_barrier.reset(1, 2);
        coordinator
            .pending_barrier
            .source_checkpoints
            .insert("s0".to_string(), SourceCheckpoint::new(10));
        coordinator.pending_barrier.sources_aligned.insert(0);
        coordinator.unaligned_tracker.as_mut().unwrap().begin(1, 2);
        coordinator
            .unaligned_tracker
            .as_mut()
            .unwrap()
            .barrier_received(0);

        let mut callback = MockCallback::new();
        coordinator.check_unaligned_timeout(&mut callback).await;

        assert_eq!(callback.barrier_checkpoint_count, 1);
        assert!(!coordinator.pending_barrier.active);
    }
}
