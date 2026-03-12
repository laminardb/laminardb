//! TPC-mode pipeline coordinator.
//!
//! Runs as a tokio task. Drains core outboxes from [`TpcRuntime`], runs SQL
//! cycles via [`PipelineCallback`], and handles checkpoint barriers.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_core::checkpoint::CheckpointBarrier;
use laminar_core::operator::Output;
use laminar_core::tpc::TaggedOutput;
use laminar_core::tpc::TpcConfig;
use rustc_hash::{FxHashMap, FxHashSet};

use super::callback::{PipelineCallback, SourceRegistration};
use super::config::PipelineConfig;
use super::tpc_runtime::TpcRuntime;
use crate::error::DbError;

/// Tracks in-flight checkpoint barrier alignment.
struct PendingBarrier {
    checkpoint_id: u64,
    sources_total: usize,
    sources_aligned: FxHashSet<usize>,
    source_checkpoints: FxHashMap<String, SourceCheckpoint>,
    started_at: Instant,
    /// Whether this barrier is currently active (tracking alignment).
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

/// Fallback timeout for idle periods. Ensures `maybe_inject_checkpoint`
/// is called even when no data flows.
const IDLE_FALLBACK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

/// Thread-per-core pipeline coordinator.
///
/// Drains core outboxes (SPSC queues) from CPU-pinned core threads,
/// runs SQL cycles via [`PipelineCallback`], and handles checkpoint barriers.
pub struct TpcPipelineCoordinator {
    config: PipelineConfig,
    runtime: TpcRuntime,
    shutdown: Arc<tokio::sync::Notify>,
    /// Lock-free flag set by core threads when outputs are available.
    has_new_data: Arc<std::sync::atomic::AtomicBool>,
    /// Signaled on the false→true transition of `has_new_data`.
    data_notify: Arc<tokio::sync::Notify>,
    /// Pre-built source names indexed by `source_idx` (avoids clone per event).
    source_name_cache: Vec<String>,
    /// Pre-allocated drain buffer (reused each cycle, zero alloc).
    drain_buffer: Vec<TaggedOutput>,
    /// Pre-allocated source batches buffer (cleared per cycle, not dropped).
    source_batches_buf: FxHashMap<String, Vec<RecordBatch>>,
    /// Pre-allocated barriers buffer (cleared per cycle, not dropped).
    barriers_buf: Vec<(usize, CheckpointBarrier)>,
    /// Pre-allocated barrier alignment tracking (reused across checkpoints).
    pending_barrier: PendingBarrier,
    /// Counter for late events (arrived after watermark).
    late_events: u64,
    /// Next checkpoint ID.
    next_checkpoint_id: u64,
    /// Last checkpoint time.
    last_checkpoint: Instant,
    /// Consecutive SQL cycle errors (reset on success).
    consecutive_sql_errors: u32,
    /// Source-initiated checkpoint request flags (e.g., Kafka rebalance).
    /// Polled each cycle; when any flag is set, a forced checkpoint is triggered.
    checkpoint_request_flags: Vec<Arc<std::sync::atomic::AtomicBool>>,
}

impl TpcPipelineCoordinator {
    /// Create a new TPC pipeline coordinator.
    ///
    /// # Errors
    ///
    /// Returns an error if the TPC runtime cannot be initialized.
    pub fn new(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        tpc_config: &TpcConfig,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> Result<Self, DbError> {
        let mut runtime =
            TpcRuntime::new(tpc_config).map_err(|e| DbError::Config(e.to_string()))?;

        // Capture checkpoint_requested flags before connectors are moved.
        let mut checkpoint_request_flags = Vec::new();
        for src in &sources {
            if let Some(flag) = src.connector.checkpoint_requested() {
                checkpoint_request_flags.push(flag);
            }
        }

        for (idx, src) in sources.into_iter().enumerate() {
            runtime
                .attach_source(
                    idx,
                    src.name,
                    src.connector,
                    src.config,
                    &config,
                    src.restore_checkpoint,
                )
                .map_err(|e| DbError::Config(format!("failed to spawn source thread: {e}")))?;
        }

        let source_name_cache: Vec<String> =
            runtime.source_names().iter().map(String::clone).collect();
        let (has_new_data, data_notify) = runtime.output_signal();

        Ok(Self {
            config,
            runtime,
            shutdown,
            has_new_data,
            data_notify,
            source_name_cache,
            drain_buffer: Vec::with_capacity(4096),
            source_batches_buf: FxHashMap::default(),
            barriers_buf: Vec::new(),
            pending_barrier: PendingBarrier::new(),
            late_events: 0,
            next_checkpoint_id: 1,
            last_checkpoint: Instant::now(),
            consecutive_sql_errors: 0,
            checkpoint_request_flags,
        })
    }

    /// Run the coordinator loop.
    ///
    /// Drains core outboxes, converts tagged outputs to source batches,
    /// executes SQL cycles via the callback, and handles checkpoints.
    #[allow(clippy::too_many_lines)]
    pub async fn run(mut self, mut callback: Box<dyn PipelineCallback>) {
        let batch_window = self.config.batch_window;
        let barrier_timeout = self.config.barrier_alignment_timeout;

        loop {
            // Phase 1: Wait for data, shutdown, or fallback timeout.
            // Core threads call notify_one() after pushing to the outbox,
            // waking the coordinator immediately instead of polling on a timer.
            tokio::select! {
                biased;
                () = self.shutdown.notified() => break,
                () = self.data_notify.notified() => {
                    // Data available. If batch_window > 0, coalesce: sleep
                    // briefly to let more data accumulate before executing
                    // the SQL cycle. This amortizes DataFusion overhead for
                    // high-throughput sources (e.g., Kafka at 500K events/sec).
                    if !batch_window.is_zero() {
                        tokio::time::sleep(batch_window).await;
                    }
                }
                // Fallback: wake periodically for checkpoint injection
                // even when no data flows.
                () = tokio::time::sleep(IDLE_FALLBACK_TIMEOUT) => {}
            }

            // Clear the flag so the next core output triggers a new wake.
            self.has_new_data
                .store(false, std::sync::atomic::Ordering::Release);

            // Phase 2: Drain all core outboxes
            self.drain_buffer.clear();
            self.runtime.poll_all_outputs(&mut self.drain_buffer);
            if self.drain_buffer.is_empty() {
                self.maybe_inject_checkpoint(&mut *callback).await;
                callback.poll_tables().await;
                // Check barrier timeout even when idle — otherwise an
                // in-flight barrier from a stalled source never expires.
                if self.pending_barrier.active
                    && self.pending_barrier.started_at.elapsed() > barrier_timeout
                {
                    tracing::warn!(
                        checkpoint_id = self.pending_barrier.checkpoint_id,
                        aligned = self.pending_barrier.sources_aligned.len(),
                        total = self.pending_barrier.sources_total,
                        "Barrier alignment timeout — cancelling checkpoint"
                    );
                    self.pending_barrier.active = false;
                }
                continue;
            }

            // Phase 3: Convert TaggedOutput → FxHashMap<String, Vec<RecordBatch>>
            // Collect barriers separately to avoid borrow conflicts.
            // Reuse pre-allocated buffers (cleared, not dropped).
            self.source_batches_buf.clear();
            self.barriers_buf.clear();

            let mut cycle_events: u64 = 0;
            let mut cycle_batches: u64 = 0;
            let cycle_start = Instant::now();

            for tagged in self.drain_buffer.drain(..) {
                match tagged.output {
                    Output::Event(event) => {
                        if let Some(name) = self.source_name_cache.get(tagged.source_idx) {
                            callback.extract_watermark(name, &event.data);
                            cycle_events += event.data.num_rows() as u64;
                            if let Some(filtered) = callback.filter_late_rows(name, &event.data) {
                                if let Some(vec) = self.source_batches_buf.get_mut(name.as_str()) {
                                    vec.push(filtered);
                                } else {
                                    self.source_batches_buf.insert(name.clone(), vec![filtered]);
                                }
                                cycle_batches += 1;
                            }
                        }
                    }
                    Output::Barrier(barrier) => {
                        self.barriers_buf.push((tagged.source_idx, barrier));
                    }
                    Output::Watermark(_ts) => {
                        // Watermark progression is already tracked via
                        // extract_watermark() on each Event batch
                    }
                    Output::CheckpointComplete(data) => {
                        tracing::debug!(
                            checkpoint_id = data.checkpoint_id,
                            operators = data.operator_states.len(),
                            "core checkpoint complete (operator states not yet persisted — \
                             cores currently run no operators)"
                        );
                    }
                    Output::LateEvent(_event) => {
                        self.late_events += 1;
                        tracing::trace!(total_late = self.late_events, "late event past watermark");
                    }
                    Output::SideOutput(_) | Output::Changelog(_) => {
                        tracing::warn!(
                            source_idx = tagged.source_idx,
                            "SideOutput/Changelog leaked past DAG boundary — dropped"
                        );
                    }
                }
            }

            // Process barriers after drain is complete.
            // Swap with empty to avoid borrow conflict with self.handle_barrier.
            let mut barriers = std::mem::take(&mut self.barriers_buf);
            for (source_idx, barrier) in barriers.drain(..) {
                self.handle_barrier(source_idx, &barrier, &mut *callback)
                    .await;
            }
            // Restore capacity for next cycle.
            self.barriers_buf = barriers;

            // Phase 4: SQL cycle
            if !self.source_batches_buf.is_empty() {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&self.source_batches_buf, wm).await {
                    Ok(results) => {
                        self.consecutive_sql_errors = 0;
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => {
                        self.consecutive_sql_errors += 1;
                        tracing::warn!(
                            error = %e,
                            consecutive = self.consecutive_sql_errors,
                            "[LDB-3020] SQL cycle error"
                        );
                        if self.consecutive_sql_errors >= 100 {
                            tracing::error!(
                                "[LDB-3021] {} consecutive SQL errors — shutting down pipeline",
                                self.consecutive_sql_errors
                            );
                            break;
                        }
                    }
                }
                #[allow(clippy::cast_possible_truncation)]
                // Cycle duration will never exceed u64::MAX nanoseconds (~584 years)
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                callback.record_cycle(cycle_events, cycle_batches, elapsed_ns);
            }

            // Phase 5: Periodic checkpoint injection
            self.maybe_inject_checkpoint(&mut *callback).await;

            // Phase 5b: Poll reference table CDC updates
            callback.poll_tables().await;

            // Phase 6: Barrier timeout check
            if self.pending_barrier.active
                && self.pending_barrier.started_at.elapsed() > barrier_timeout
            {
                tracing::warn!(
                    checkpoint_id = self.pending_barrier.checkpoint_id,
                    aligned = self.pending_barrier.sources_aligned.len(),
                    total = self.pending_barrier.sources_total,
                    "Barrier alignment timeout — cancelling checkpoint"
                );
                self.pending_barrier.active = false;
            }
        }

        // Shutdown: drain remaining outputs
        self.drain_buffer.clear();
        self.runtime.poll_all_outputs(&mut self.drain_buffer);

        // Close sources
        let connectors = self.runtime.shutdown();
        for (_name, mut connector) in connectors {
            if let Err(e) = connector.close().await {
                tracing::warn!(error = %e, "Error closing connector on shutdown");
            }
        }
    }

    /// Handle a barrier from a source. Track alignment and trigger checkpoint
    /// when all sources have aligned.
    async fn handle_barrier(
        &mut self,
        source_idx: usize,
        barrier: &CheckpointBarrier,
        callback: &mut dyn PipelineCallback,
    ) {
        if !self.pending_barrier.active {
            self.pending_barrier
                .reset(barrier.checkpoint_id, self.runtime.num_sources());
        }

        if self.pending_barrier.checkpoint_id != barrier.checkpoint_id {
            tracing::debug!(
                expected = self.pending_barrier.checkpoint_id,
                actual = barrier.checkpoint_id,
                source_idx,
                "ignoring barrier with mismatched checkpoint ID"
            );
            return;
        }

        self.pending_barrier.sources_aligned.insert(source_idx);

        // Capture this source's checkpoint
        if let Some(name) = self.source_name_cache.get(source_idx) {
            let cp = self.runtime.source_checkpoint(source_idx);
            self.pending_barrier
                .source_checkpoints
                .insert(name.clone(), cp);
        }

        // Check if all sources are aligned
        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            // drain() preserves the map's allocated capacity for reuse.
            let source_checkpoints: FxHashMap<String, SourceCheckpoint> =
                self.pending_barrier.source_checkpoints.drain().collect();
            self.pending_barrier.active = false;

            let success = callback.checkpoint_with_barrier(source_checkpoints).await;
            if !success {
                tracing::warn!("Checkpoint with barrier failed");
            }
        }
    }

    /// Inject checkpoint barriers if the interval has elapsed or a source
    /// has requested an immediate checkpoint (e.g., Kafka partition revocation).
    async fn maybe_inject_checkpoint(&mut self, callback: &mut dyn PipelineCallback) {
        if self.pending_barrier.active {
            return; // Already waiting for alignment
        }

        // Check source-initiated checkpoint requests (e.g., Kafka rebalance).
        let source_requested = self.checkpoint_request_flags.iter().any(|flag| {
            flag.compare_exchange(
                true,
                false,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        });

        let Some(interval) = self.config.checkpoint_interval else {
            if source_requested {
                // Manual-only mode but a source needs a checkpoint. Force one.
                let _ = callback.maybe_checkpoint(true).await;
            }
            return;
        };

        if !source_requested && self.last_checkpoint.elapsed() < interval {
            return;
        }

        self.last_checkpoint = Instant::now();
        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;

        // Inject barriers into all sources
        for idx in 0..self.runtime.num_sources() {
            self.runtime
                .injector(idx)
                .trigger(checkpoint_id, laminar_core::checkpoint::flags::NONE);
        }

        // Also try a non-barrier checkpoint via callback
        let _ = callback.maybe_checkpoint(false).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_barrier_creation_and_reset() {
        let mut pending = PendingBarrier::new();
        assert!(!pending.active);

        pending.reset(1, 3);
        assert!(pending.active);
        assert_eq!(pending.checkpoint_id, 1);
        assert_eq!(pending.sources_total, 3);
        assert!(pending.sources_aligned.is_empty());

        pending.sources_aligned.insert(0);
        pending.reset(2, 5);
        assert_eq!(pending.checkpoint_id, 2);
        assert!(pending.sources_aligned.is_empty());
    }
}
