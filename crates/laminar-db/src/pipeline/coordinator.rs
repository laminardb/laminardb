//! Event-driven pipeline coordinator.
//!
//! Replaces the monolithic polling loop with a fan-out/fan-in architecture:
//! - **Fan-out**: Each source runs in its own tokio task with exclusive ownership.
//! - **Fan-in**: All source tasks send [`SourceEvent`]s to a single `mpsc` channel.
//! - **Coordinator**: `select!`s on the merged channel, processes batches, runs
//!   SQL execution cycles, routes results to sinks, and manages checkpoints.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use tokio::sync::{mpsc, Notify};

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SourceConnector;

use super::config::PipelineConfig;
use super::source_event::SourceEvent;
use super::source_task::{spawn_source_task, SourceTaskHandle};

/// A registered source with its name and config.
pub struct SourceRegistration {
    /// Source name.
    pub name: String,
    /// The connector (owned).
    pub connector: Box<dyn SourceConnector>,
    /// Connector config (for open).
    pub config: ConnectorConfig,
}

/// Callback trait for the coordinator to interact with the rest of the DB.
///
/// This decouples the pipeline module from db.rs internals.
#[async_trait::async_trait]
pub trait PipelineCallback: Send + 'static {
    /// Called with accumulated source batches to execute a SQL cycle.
    async fn execute_cycle(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        watermark: i64,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, String>;

    /// Called with results to push to stream subscriptions.
    fn push_to_streams(&self, results: &HashMap<String, Vec<RecordBatch>>);

    /// Called with results to write to sinks.
    async fn write_to_sinks(&mut self, results: &HashMap<String, Vec<RecordBatch>>);

    /// Extract watermark from a batch for a given source.
    fn extract_watermark(&mut self, source_name: &str, batch: &RecordBatch);

    /// Filter late rows from a batch. Returns the filtered batch (or None if all late).
    fn filter_late_rows(&self, source_name: &str, batch: &RecordBatch) -> Option<RecordBatch>;

    /// Get the current pipeline watermark.
    fn current_watermark(&self) -> i64;

    /// Perform a periodic checkpoint. Returns true if checkpoint was triggered.
    async fn maybe_checkpoint(&mut self, force: bool) -> bool;

    /// Record cycle metrics.
    fn record_cycle(&self, events_ingested: u64, batches: u64, elapsed_ns: u64);

    /// Poll table sources for incremental CDC changes.
    async fn poll_tables(&mut self);
}

/// The event-driven pipeline coordinator.
///
/// Owns the fan-in channel receiver and all source task handles.
/// Runs as a single tokio task, processing events as they arrive.
pub struct PipelineCoordinator {
    config: PipelineConfig,
    /// Per-source task handles (indexed by source idx).
    handles: Vec<SourceTaskHandle>,
    /// Source names (indexed by source idx).
    source_names: Vec<String>,
    /// Fan-in channel receiver.
    rx: mpsc::Receiver<SourceEvent>,
    /// Shutdown signal.
    shutdown: Arc<Notify>,
}

impl PipelineCoordinator {
    /// Creates a new coordinator and spawns per-source tasks.
    ///
    /// Each source connector is moved into its own task — no `Arc<Mutex>` wrapping.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Connector` if any source fails to open.
    pub async fn new(
        sources: Vec<SourceRegistration>,
        config: PipelineConfig,
        shutdown: Arc<Notify>,
    ) -> Result<Self, crate::error::DbError> {
        let (tx, rx) = mpsc::channel(config.channel_capacity * sources.len().max(1));

        let mut handles = Vec::with_capacity(sources.len());
        let mut source_names = Vec::with_capacity(sources.len());

        for reg in &sources {
            source_names.push(reg.name.clone());
        }

        for (idx, mut reg) in sources.into_iter().enumerate() {
            reg.connector.open(&reg.config).await.map_err(|e| {
                crate::error::DbError::Connector(format!(
                    "Failed to open source '{}': {e}",
                    reg.name
                ))
            })?;

            let handle =
                spawn_source_task(idx, reg.name.clone(), reg.connector, tx.clone(), &config);
            handles.push(handle);
        }

        // Drop our copy of tx so the channel closes when all source tasks finish.
        drop(tx);

        Ok(Self {
            config,
            handles,
            source_names,
            rx,
            shutdown,
        })
    }

    /// Returns a checkpoint snapshot for a source by index (lock-free read).
    #[must_use]
    pub fn source_checkpoint(&self, idx: usize) -> Option<SourceCheckpoint> {
        self.handles
            .get(idx)
            .map(|h| h.checkpoint_rx.borrow().clone())
    }

    /// Returns all source checkpoints as a name→checkpoint map.
    #[must_use]
    pub fn all_source_checkpoints(&self) -> HashMap<String, SourceCheckpoint> {
        self.source_names
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                let cp = self.handles[idx].checkpoint_rx.borrow().clone();
                (name.clone(), cp)
            })
            .collect()
    }

    /// Returns source names (indexed by position).
    #[must_use]
    pub fn source_names(&self) -> &[String] {
        &self.source_names
    }

    /// Runs the coordinator event loop.
    ///
    /// Uses a two-phase micro-batch approach:
    ///
    /// 1. **Wait** — Block on `rx.recv()` until the first event arrives (zero CPU).
    /// 2. **Accumulate** — Sleep for [`PipelineConfig::batch_window`] to let more
    ///    events queue in the channel. The coordinator does **not** drain during
    ///    this phase, so the bounded channel provides natural back-pressure to
    ///    source tasks (they block on `tx.send().await` when the channel fills).
    /// 3. **Drain** — Non-blocking `try_recv()` to collect all queued events.
    /// 4. **Execute** — Run one SQL cycle over the accumulated batch.
    ///
    /// This bounds the number of SQL cycles to at most `1 / batch_window` per
    /// second regardless of the inbound message rate.
    #[allow(clippy::too_many_lines)]
    pub async fn run(mut self, mut callback: Box<dyn PipelineCallback>) {
        tracing::info!(
            sources = self.handles.len(),
            batch_window_ms = %self.config.batch_window.as_millis(),
            "Pipeline coordinator started (event-driven)"
        );

        let mut cycle_count: u64 = 0;
        let mut total_batches: u64 = 0;
        let mut total_records: u64 = 0;

        let checkpoint_interval = self.config.checkpoint_interval;
        let batch_window = self.config.batch_window;

        // Reuse across cycles to avoid per-cycle HashMap allocation.
        let mut source_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();

        loop {
            for v in source_batches.values_mut() {
                v.clear();
            }
            let mut got_data = false;
            let mut shutting_down = false;

            // ── Phase 1: Wait for the first event (blocks, zero CPU). ──
            tokio::select! {
                biased;

                () = self.shutdown.notified() => {
                    tracing::info!("Pipeline coordinator shutdown signal received");
                    break;
                }

                event = self.rx.recv() => {
                    if let Some(evt) = event {
                        self.handle_event(
                            evt,
                            &mut source_batches,
                            &mut total_batches,
                            &mut total_records,
                            &mut got_data,
                            &mut *callback,
                        );
                    } else {
                        tracing::info!("All source tasks completed");
                        break;
                    }
                }
            }

            // ── Phase 2: Batch accumulation window. ──────────────────
            // Sleep to let source tasks fill the channel. During this
            // window the coordinator does NOT drain, so the bounded
            // channel back-pressures source tasks naturally.
            if !batch_window.is_zero() {
                tokio::select! {
                    biased;

                    () = self.shutdown.notified() => {
                        shutting_down = true;
                    }

                    () = tokio::time::sleep(batch_window) => {}
                }
            }

            // ── Phase 3: Drain all queued events. ────────────────────
            while let Ok(evt) = self.rx.try_recv() {
                self.handle_event(
                    evt,
                    &mut source_batches,
                    &mut total_batches,
                    &mut total_records,
                    &mut got_data,
                    &mut *callback,
                );
            }

            // ── Phase 4: Execute SQL cycle. ──────────────────────────
            if got_data {
                let cycle_start = Instant::now();
                let wm = callback.current_watermark();

                match callback.execute_cycle(&source_batches, wm).await {
                    Ok(results) => {
                        callback.push_to_streams(&results);
                        callback.write_to_sinks(&results).await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Stream execution cycle error");
                    }
                }

                callback.poll_tables().await;

                cycle_count += 1;
                #[allow(clippy::cast_possible_truncation)]
                let elapsed_ns = cycle_start.elapsed().as_nanos() as u64;
                #[allow(clippy::cast_possible_truncation)]
                let batch_count: u64 = source_batches.values().map(|v| v.len() as u64).sum();
                #[allow(clippy::cast_possible_truncation)]
                let record_count: u64 = source_batches
                    .values()
                    .flat_map(|v| v.iter())
                    .map(|b| b.num_rows() as u64)
                    .sum();
                callback.record_cycle(record_count, batch_count, elapsed_ns);

                if cycle_count.is_multiple_of(50) {
                    tracing::debug!(
                        cycles = cycle_count,
                        batches = total_batches,
                        records = total_records,
                        "Pipeline processing"
                    );
                }
            }

            // Periodic checkpoint.
            if checkpoint_interval.is_some() {
                callback.maybe_checkpoint(false).await;
            }

            if shutting_down {
                tracing::info!("Pipeline coordinator shutdown signal received");
                break;
            }
        }

        tracing::info!(
            cycles = cycle_count,
            batches = total_batches,
            records = total_records,
            "Pipeline coordinator stopping"
        );

        // Final checkpoint.
        callback.maybe_checkpoint(true).await;

        // Shut down all source tasks and reclaim connectors for cleanup.
        let mut connectors = Vec::new();
        for (idx, handle) in self.handles.into_iter().enumerate() {
            handle.shutdown.notify_one();
            match handle.join.await {
                Ok(connector) => connectors.push((idx, connector)),
                Err(e) => {
                    tracing::warn!(source_idx = idx, error = %e, "Source task join error");
                }
            }
        }

        for (idx, mut connector) in connectors {
            let name = &self.source_names[idx];
            if let Err(e) = connector.close().await {
                tracing::warn!(source = %name, error = %e, "Source close error");
            }
        }

        tracing::info!("Pipeline coordinator stopped");
    }

    /// Handles a single source event, accumulating batches.
    fn handle_event(
        &self,
        event: SourceEvent,
        source_batches: &mut HashMap<String, Vec<RecordBatch>>,
        total_batches: &mut u64,
        total_records: &mut u64,
        got_data: &mut bool,
        callback: &mut dyn PipelineCallback,
    ) {
        match event {
            SourceEvent::Batch { idx, batch } => {
                let name = &self.source_names[idx];
                *total_batches += 1;
                *total_records += batch.num_rows() as u64;

                callback.extract_watermark(name, &batch);
                let filtered = callback.filter_late_rows(name, &batch);
                if let Some(fb) = filtered {
                    source_batches.entry(name.clone()).or_default().push(fb);
                    *got_data = true;
                }
            }
            SourceEvent::Error { idx, message } => {
                tracing::warn!(
                    source = %self.source_names[idx],
                    error = %message,
                    "Source task error"
                );
            }
            SourceEvent::Exhausted { idx } => {
                tracing::info!(
                    source = %self.source_names[idx],
                    "Source exhausted"
                );
            }
        }
    }
}
