//! Simplified pipeline coordinator that replaces TPC core threads.
//!
//! Sources push directly to the coordinator via `tokio::sync::mpsc`,
//! eliminating the SPSC inbox → Ring 0 core thread → SPSC outbox relay
//! that the TPC architecture imposed. The coordinator runs as a single
//! tokio task and delegates SQL execution to [`PipelineCallback`].
//!
//! # Architecture
//!
//! ```text
//! Source task (tokio::spawn)
//!   │  connector.poll_batch().await
//!   │
//!   └──── mpsc::Sender ────► StreamingCoordinator (tokio task)
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
use laminar_core::checkpoint::CheckpointBarrier;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::mpsc;

use super::callback::{PipelineCallback, SourceRegistration};
use super::config::PipelineConfig;
use crate::error::DbError;

/// Message sent from a source task to the coordinator.
#[allow(dead_code)] // Barrier variant used when barrier injection is wired
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
    name: String,
    shutdown: Arc<AtomicBool>,
    join: tokio::task::JoinHandle<()>,
    checkpoint_rx: tokio::sync::watch::Receiver<SourceCheckpoint>,
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
    source_names: Vec<String>,
    /// Shutdown signal.
    shutdown: Arc<tokio::sync::Notify>,
    /// Pending barrier alignment.
    pending_barrier: PendingBarrier,
    /// Next checkpoint ID (used when barrier injection is wired).
    #[allow(dead_code)]
    next_checkpoint_id: u64,
    /// Last checkpoint time.
    last_checkpoint: Instant,
    /// Source-initiated checkpoint request flags.
    checkpoint_request_flags: Vec<Arc<AtomicBool>>,
    /// Pre-allocated source batches buffer (cleared per cycle).
    source_batches_buf: FxHashMap<String, Vec<RecordBatch>>,
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

    #[allow(dead_code)] // Used when barrier injection is wired
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
    /// Returns an error if delivery guarantee constraints are violated.
    pub fn new(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<tokio::sync::Notify>,
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

            let (cp_tx, cp_rx) = tokio::sync::watch::channel(SourceCheckpoint::new(0));
            let task_shutdown = Arc::new(AtomicBool::new(false));
            let task_shutdown_clone = Arc::clone(&task_shutdown);
            let task_tx = tx.clone();
            let max_poll = config.max_poll_records;
            let poll_interval = config.fallback_poll_interval;
            let src_name = src.name.clone();
            let restore = src.restore_checkpoint;
            let mut connector = src.connector;
            let connector_config = src.config;

            let join = tokio::spawn(async move {
                // Open the connector.
                if let Err(e) = connector.open(&connector_config).await {
                    tracing::error!(source = %src_name, error = %e, "source open failed");
                    return;
                }

                // Restore checkpoint if available.
                if let Some(ref cp) = restore {
                    if let Err(e) = connector.restore(cp).await {
                        tracing::warn!(
                            source = %src_name, error = %e,
                            "source restore failed, starting from beginning"
                        );
                    }
                }

                // Poll loop.
                loop {
                    if task_shutdown_clone.load(Ordering::Acquire) {
                        break;
                    }

                    match connector.poll_batch(max_poll).await {
                        Ok(Some(batch)) => {
                            let msg = SourceMsg::Batch {
                                source_idx: idx,
                                batch: batch.records,
                            };
                            if task_tx.send(msg).await.is_err() {
                                break; // Coordinator dropped
                            }
                        }
                        Ok(None) => {
                            // No data — sleep briefly.
                            tokio::time::sleep(poll_interval).await;
                        }
                        Err(e) => {
                            tracing::warn!(source = %src_name, error = %e, "poll error");
                            tokio::time::sleep(poll_interval).await;
                        }
                    }

                    // Publish checkpoint offset.
                    let _ = cp_tx.send(connector.checkpoint());
                }

                // Close connector.
                if let Err(e) = connector.close().await {
                    tracing::warn!(source = %src_name, error = %e, "source close error");
                }
            });

            source_handles.push(SourceHandle {
                name: src.name.clone(),
                shutdown: task_shutdown,
                join,
                checkpoint_rx: cp_rx,
            });
            source_names.push(src.name);
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
        })
    }

    /// Run the coordinator loop.
    ///
    /// Receives batches from sources via mpsc, executes SQL cycles via
    /// the callback, and handles checkpoint barriers. Returns when
    /// shutdown is signaled.
    #[allow(clippy::too_many_lines)]
    pub async fn run(mut self, mut callback: Box<dyn PipelineCallback>) {
        let batch_window = self.config.batch_window;

        loop {
            // Phase 1: Wait for data, shutdown, or idle timeout.
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

            // Phase 2: Process the message and drain any buffered messages.
            self.source_batches_buf.clear();
            let mut barriers = Vec::new();
            let mut cycle_events: u64 = 0;
            let cycle_start = Instant::now();

            if let Some(first_msg) = msg {
                self.process_msg(first_msg, &mut *callback, &mut barriers, &mut cycle_events);
            }

            // Drain any additional buffered messages (batch coalescing).
            while let Ok(msg) = self.rx.try_recv() {
                self.process_msg(msg, &mut *callback, &mut barriers, &mut cycle_events);
            }

            // Phase 3: Handle barriers.
            for (source_idx, barrier) in &barriers {
                self.handle_barrier(*source_idx, barrier, &mut *callback)
                    .await;
            }

            // Phase 4: Execute SQL cycle.
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
            }

            // Phase 5: Periodic checkpoint.
            self.maybe_checkpoint(&mut *callback).await;

            // Phase 6: Poll table sources.
            callback.poll_tables().await;

            // Phase 7: Barrier timeout check.
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

        // Shutdown: signal all source tasks and wait.
        for handle in &self.source_handles {
            handle.shutdown.store(true, Ordering::Release);
        }
        for handle in self.source_handles {
            if let Err(e) = handle.join.await {
                tracing::warn!(source = %handle.name, error = ?e, "source task panicked");
            }
        }
    }

    /// Process a single source message.
    fn process_msg(
        &mut self,
        msg: SourceMsg,
        callback: &mut dyn PipelineCallback,
        barriers: &mut Vec<(usize, CheckpointBarrier)>,
        cycle_events: &mut u64,
    ) {
        match msg {
            SourceMsg::Batch { source_idx, batch } => {
                if let Some(name) = self.source_names.get(source_idx) {
                    callback.extract_watermark(name, &batch);
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        *cycle_events += batch.num_rows() as u64;
                    }
                    if let Some(filtered) = callback.filter_late_rows(name, &batch) {
                        self.source_batches_buf
                            .entry(name.clone())
                            .or_default()
                            .push(filtered);
                    }
                }
            }
            SourceMsg::Barrier {
                source_idx,
                barrier,
            } => {
                barriers.push((source_idx, barrier));
            }
        }
    }

    /// Handle a barrier from a source.
    async fn handle_barrier(
        &mut self,
        source_idx: usize,
        barrier: &CheckpointBarrier,
        callback: &mut dyn PipelineCallback,
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
                .insert(name.clone(), cp);
        }

        self.pending_barrier.sources_aligned.insert(source_idx);

        // Check if all sources aligned.
        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            let checkpoints = std::mem::take(&mut self.pending_barrier.source_checkpoints);
            callback.checkpoint_with_barrier(checkpoints).await;
            self.pending_barrier.active = false;
            self.last_checkpoint = Instant::now();
        }
    }

    /// Check if a periodic checkpoint should be triggered.
    async fn maybe_checkpoint(&mut self, callback: &mut dyn PipelineCallback) {
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

        // Collect current source offsets.
        let mut offsets = FxHashMap::default();
        for handle in &self.source_handles {
            offsets.insert(handle.name.clone(), handle.checkpoint_rx.borrow().clone());
        }

        if callback.maybe_checkpoint(false, offsets).await {
            self.last_checkpoint = Instant::now();
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
        results: Vec<FxHashMap<String, Vec<RecordBatch>>>,
        watermark: i64,
    }

    impl MockCallback {
        fn new() -> Self {
            Self {
                cycle_count: 0,
                results: Vec::new(),
                watermark: 0,
            }
        }
    }

    #[async_trait::async_trait]
    impl PipelineCallback for MockCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<String, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<FxHashMap<String, Vec<RecordBatch>>, String> {
            self.cycle_count += 1;
            // Pass through source batches as results.
            let results: FxHashMap<String, Vec<RecordBatch>> = source_batches
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            self.results.push(results.clone());
            Ok(results)
        }

        fn push_to_streams(&self, _results: &FxHashMap<String, Vec<RecordBatch>>) {}
        async fn write_to_sinks(&mut self, _results: &FxHashMap<String, Vec<RecordBatch>>) {}

        fn extract_watermark(&mut self, _source_name: &str, batch: &RecordBatch) {
            // Use row count as a simple watermark proxy.
            self.watermark += batch.num_rows() as i64;
        }

        fn filter_late_rows(&self, _source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
            Some(batch.clone())
        }

        fn current_watermark(&self) -> i64 {
            self.watermark
        }

        async fn maybe_checkpoint(
            &mut self,
            _force: bool,
            _source_offsets: FxHashMap<String, SourceCheckpoint>,
        ) -> bool {
            false
        }

        async fn checkpoint_with_barrier(
            &mut self,
            _source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        ) -> bool {
            true
        }

        fn record_cycle(&self, _events: u64, _batches: u64, _elapsed_ns: u64) {}
        async fn poll_tables(&mut self) {}
    }

    /// Test that the coordinator processes messages via direct mpsc channel.
    #[tokio::test]
    async fn test_coordinator_direct_channel() {
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let (tx, rx) = mpsc::channel(64);

        // Create coordinator directly (bypassing source spawning).
        let coordinator = StreamingCoordinator {
            config: PipelineConfig {
                batch_window: Duration::ZERO,
                max_poll_records: 1000,
                channel_capacity: 64,
                fallback_poll_interval: Duration::from_millis(10),
                checkpoint_interval: None,
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                barrier_alignment_timeout: BARRIER_TIMEOUT,
            },
            rx,
            source_handles: Vec::new(),
            source_names: vec!["test_source".to_string()],
            shutdown: Arc::clone(&shutdown),
            pending_barrier: PendingBarrier::new(),
            next_checkpoint_id: 1,
            last_checkpoint: Instant::now(),
            checkpoint_request_flags: Vec::new(),
            source_batches_buf: FxHashMap::default(),
        };

        let callback = Box::new(MockCallback::new());

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
}
