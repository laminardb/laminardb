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
}

/// Thread-per-core pipeline coordinator.
///
/// Drains core outboxes (SPSC queues) from CPU-pinned core threads,
/// runs SQL cycles via [`PipelineCallback`], and handles checkpoint barriers.
pub struct TpcPipelineCoordinator {
    config: PipelineConfig,
    runtime: TpcRuntime,
    shutdown: Arc<tokio::sync::Notify>,
    /// Pre-built source names indexed by `source_idx` (avoids clone per event).
    source_name_cache: Vec<String>,
    /// Pre-allocated drain buffer (reused each cycle, zero alloc).
    drain_buffer: Vec<TaggedOutput>,
    /// Pre-allocated source batches buffer (cleared per cycle, not dropped).
    source_batches_buf: FxHashMap<String, Vec<RecordBatch>>,
    /// Pre-allocated barriers buffer (cleared per cycle, not dropped).
    barriers_buf: Vec<(usize, CheckpointBarrier)>,
    /// Pending barrier alignment tracking.
    pending_barrier: Option<PendingBarrier>,
    /// Counter for late events (arrived after watermark).
    late_events: u64,
    /// Next checkpoint ID.
    next_checkpoint_id: u64,
    /// Last checkpoint time.
    last_checkpoint: Instant,
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
        for (idx, src) in sources.into_iter().enumerate() {
            runtime
                .attach_source(idx, src.name, src.connector, src.config, &config)
                .map_err(|e| DbError::Config(format!("failed to spawn source thread: {e}")))?;
        }

        let source_name_cache: Vec<String> =
            runtime.source_names().iter().map(String::clone).collect();

        Ok(Self {
            config,
            runtime,
            shutdown,
            source_name_cache,
            drain_buffer: Vec::with_capacity(4096),
            source_batches_buf: FxHashMap::default(),
            barriers_buf: Vec::new(),
            pending_barrier: None,
            late_events: 0,
            next_checkpoint_id: 1,
            last_checkpoint: Instant::now(),
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
            // Phase 1: Wait (yield to tokio, check shutdown)
            tokio::select! {
                biased;
                () = self.shutdown.notified() => break,
                () = tokio::time::sleep(batch_window) => {}
            }

            // Phase 2: Drain all core outboxes
            self.drain_buffer.clear();
            self.runtime.poll_all_outputs(&mut self.drain_buffer);
            if self.drain_buffer.is_empty() {
                self.maybe_inject_checkpoint(&mut *callback).await;
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
                                self.source_batches_buf
                                    .entry(name.clone())
                                    .or_default()
                                    .push(filtered);
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
                            "core checkpoint complete"
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
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => tracing::warn!(error = %e, "SQL cycle error"),
                }
                #[allow(clippy::cast_possible_truncation)]
                // Cycle duration will never exceed u64::MAX nanoseconds (~584 years)
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                callback.record_cycle(cycle_events, cycle_batches, elapsed_ns);
            }

            // Phase 5: Periodic checkpoint injection
            self.maybe_inject_checkpoint(&mut *callback).await;

            // Phase 6: Barrier timeout check
            if let Some(ref pending) = self.pending_barrier {
                if pending.started_at.elapsed() > barrier_timeout {
                    tracing::warn!(
                        checkpoint_id = pending.checkpoint_id,
                        aligned = pending.sources_aligned.len(),
                        total = pending.sources_total,
                        "Barrier alignment timeout — cancelling checkpoint"
                    );
                    self.pending_barrier = None;
                }
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
        let pending = self.pending_barrier.get_or_insert_with(|| PendingBarrier {
            checkpoint_id: barrier.checkpoint_id,
            sources_total: self.runtime.num_sources(),
            sources_aligned: FxHashSet::default(),
            source_checkpoints: FxHashMap::default(),
            started_at: Instant::now(),
        });

        if pending.checkpoint_id != barrier.checkpoint_id {
            tracing::debug!(
                expected = pending.checkpoint_id,
                actual = barrier.checkpoint_id,
                source_idx,
                "ignoring barrier with mismatched checkpoint ID"
            );
            return;
        }

        pending.sources_aligned.insert(source_idx);

        // Capture this source's checkpoint
        if let Some(name) = self.source_name_cache.get(source_idx) {
            let cp = self.runtime.source_checkpoint(source_idx);
            pending.source_checkpoints.insert(name.clone(), cp);
        }

        // Check if all sources are aligned
        if pending.sources_aligned.len() >= pending.sources_total {
            // Move checkpoints out — pending_barrier is about to be cleared
            let source_checkpoints = std::mem::take(&mut pending.source_checkpoints);
            self.pending_barrier = None;

            let success = callback.checkpoint_with_barrier(source_checkpoints).await;
            if !success {
                tracing::warn!("Checkpoint with barrier failed");
            }
        }
    }

    /// Inject checkpoint barriers if the interval has elapsed.
    async fn maybe_inject_checkpoint(&mut self, callback: &mut dyn PipelineCallback) {
        let Some(interval) = self.config.checkpoint_interval else {
            return;
        };

        if self.pending_barrier.is_some() {
            return; // Already waiting for alignment
        }

        if self.last_checkpoint.elapsed() < interval {
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
    fn test_pending_barrier_creation() {
        let pending = PendingBarrier {
            checkpoint_id: 1,
            sources_total: 3,
            sources_aligned: FxHashSet::default(),
            source_checkpoints: FxHashMap::default(),
            started_at: Instant::now(),
        };
        assert_eq!(pending.sources_total, 3);
        assert!(pending.sources_aligned.is_empty());
    }
}
