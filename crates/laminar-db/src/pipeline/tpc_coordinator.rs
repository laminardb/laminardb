//! TPC-mode pipeline coordinator.
//!
//! Runs as a tokio task. Drains core outboxes from [`TpcRuntime`], runs SQL
//! cycles via [`PipelineCallback`], and handles checkpoint barriers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_core::checkpoint::CheckpointBarrier;
use laminar_core::operator::Output;
use laminar_core::tpc::TaggedOutput;
use laminar_core::tpc::TpcConfig;

use super::callback::{PipelineCallback, SourceRegistration};
use super::config::PipelineConfig;
use super::tpc_runtime::TpcRuntime;
use crate::error::DbError;

/// Tracks in-flight checkpoint barrier alignment.
struct PendingBarrier {
    checkpoint_id: u64,
    sources_total: usize,
    sources_aligned: std::collections::HashSet<usize>,
    source_checkpoints: HashMap<String, SourceCheckpoint>,
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
    /// Pre-allocated drain buffer (reused each cycle, zero alloc).
    drain_buffer: Vec<TaggedOutput>,
    /// Pending barrier alignment tracking.
    pending_barrier: Option<PendingBarrier>,
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
        tpc_config: TpcConfig,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> Result<Self, DbError> {
        let mut runtime =
            TpcRuntime::new(tpc_config).map_err(|e| DbError::Config(e.to_string()))?;
        for (idx, src) in sources.into_iter().enumerate() {
            runtime
                .attach_source(idx, src.name, src.connector, src.config, &config)
                .map_err(|e| DbError::Config(format!("failed to spawn source thread: {e}")))?;
        }

        Ok(Self {
            config,
            runtime,
            shutdown,
            drain_buffer: Vec::with_capacity(4096),
            pending_barrier: None,
            next_checkpoint_id: 1,
            last_checkpoint: Instant::now(),
        })
    }

    /// Run the coordinator loop.
    ///
    /// Drains core outboxes, converts tagged outputs to source batches,
    /// executes SQL cycles via the callback, and handles checkpoints.
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

            // Phase 3: Convert TaggedOutput → HashMap<String, Vec<RecordBatch>>
            // Collect barriers separately to avoid borrow conflicts.
            let mut source_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
            let mut barriers: Vec<(usize, CheckpointBarrier)> = Vec::new();

            let outputs: Vec<TaggedOutput> = self.drain_buffer.drain(..).collect();
            for tagged in outputs {
                match tagged.output {
                    Output::Event(event) => {
                        if tagged.source_idx < self.runtime.source_names().len() {
                            let name = self.runtime.source_names()[tagged.source_idx].clone();
                            callback.extract_watermark(&name, &event.data);
                            if let Some(filtered) = callback.filter_late_rows(&name, &event.data) {
                                source_batches.entry(name).or_default().push(filtered);
                            }
                        }
                    }
                    Output::Barrier(barrier) => {
                        barriers.push((tagged.source_idx, barrier));
                    }
                    _ => {
                        // Watermark, CheckpointComplete, LateEvent, etc.
                    }
                }
            }

            // Process barriers after drain is complete
            for (source_idx, barrier) in barriers {
                self.handle_barrier(source_idx, &barrier, &mut *callback)
                    .await;
            }

            // Phase 4: SQL cycle
            if !source_batches.is_empty() {
                let wm = callback.current_watermark();
                match callback.execute_cycle(&source_batches, wm).await {
                    Ok(results) => {
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => tracing::warn!(error = %e, "SQL cycle error"),
                }
                callback.record_cycle(0, 0, 0);
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
            sources_aligned: std::collections::HashSet::new(),
            source_checkpoints: HashMap::new(),
            started_at: Instant::now(),
        });

        // Only track if this barrier matches the pending checkpoint
        if pending.checkpoint_id != barrier.checkpoint_id {
            return;
        }

        pending.sources_aligned.insert(source_idx);

        // Capture this source's checkpoint
        if source_idx < self.runtime.source_names().len() {
            let name = self.runtime.source_names()[source_idx].clone();
            let cp = self.runtime.source_checkpoint(source_idx);
            pending.source_checkpoints.insert(name, cp);
        }

        // Check if all sources are aligned
        if pending.sources_aligned.len() >= pending.sources_total {
            let source_checkpoints = pending.source_checkpoints.clone();
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
            sources_aligned: std::collections::HashSet::new(),
            source_checkpoints: HashMap::new(),
            started_at: Instant::now(),
        };
        assert_eq!(pending.sources_total, 3);
        assert!(pending.sources_aligned.is_empty());
    }
}
