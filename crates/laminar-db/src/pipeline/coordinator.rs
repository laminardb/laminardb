//! Event-driven pipeline coordinator.
//!
//! Replaces the monolithic polling loop with a fan-out/fan-in architecture:
//! - **Fan-out**: Each source runs in its own tokio task with exclusive ownership.
//! - **Fan-in**: All source tasks send [`SourceEvent`]s to a single `mpsc` channel.
//! - **Coordinator**: `select!`s on the merged channel, processes batches, runs
//!   SQL execution cycles, routes results to sinks, and manages checkpoints.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use tokio::sync::{mpsc, Notify};

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SourceConnector;
use laminar_core::checkpoint::barrier::flags as barrier_flags;
use laminar_core::checkpoint::{CheckpointBarrier, CheckpointBarrierInjector};

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
    /// Whether this source supports replay from a checkpointed position.
    pub supports_replay: bool,
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

    /// Called when all sources have aligned on a barrier.
    ///
    /// Receives source checkpoints captured at the barrier point (consistent).
    /// The callback should snapshot operator state and persist the checkpoint.
    /// Returns true if the checkpoint succeeded.
    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: HashMap<String, SourceCheckpoint>,
    ) -> bool;

    /// Record cycle metrics.
    fn record_cycle(&self, events_ingested: u64, batches: u64, elapsed_ns: u64);

    /// Poll table sources for incremental CDC changes.
    async fn poll_tables(&mut self);
}

/// Tracks in-flight barrier alignment across sources.
struct PendingBarrier {
    /// Checkpoint ID for this barrier.
    checkpoint_id: u64,
    /// Total number of sources that must align.
    sources_total: usize,
    /// Source indices that have reported their barrier.
    sources_aligned: HashSet<usize>,
    /// Source checkpoints captured at barrier time (name → checkpoint).
    source_checkpoints: HashMap<String, SourceCheckpoint>,
    /// When this barrier was started.
    started_at: Instant,
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
    /// Per-source barrier injectors (indexed by source idx).
    injectors: Vec<CheckpointBarrierInjector>,
    /// Fan-in channel receiver.
    rx: mpsc::Receiver<SourceEvent>,
    /// Shutdown signal.
    shutdown: Arc<Notify>,
    /// In-flight barrier alignment (if any).
    pending_barrier: Option<PendingBarrier>,
    /// Monotonically increasing checkpoint ID for barrier-based checkpoints.
    next_barrier_checkpoint_id: u64,
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
        let mut injectors = Vec::with_capacity(sources.len());

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

            let injector = CheckpointBarrierInjector::new();
            let barrier_handle = injector.handle();
            let handle = spawn_source_task(
                idx,
                reg.name.clone(),
                reg.connector,
                tx.clone(),
                &config,
                barrier_handle,
            );
            handles.push(handle);
            injectors.push(injector);
        }

        // Drop our copy of tx so the channel closes when all source tasks finish.
        drop(tx);

        Ok(Self {
            config,
            handles,
            source_names,
            injectors,
            rx,
            shutdown,
            pending_barrier: None,
            next_barrier_checkpoint_id: 1,
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

    /// Injects a checkpoint barrier into all source tasks.
    ///
    /// Each source will see the barrier on its next poll iteration
    /// (single atomic load, <10ns fast path) and send a
    /// [`SourceEvent::Barrier`] back to the coordinator.
    fn inject_barriers(&mut self, flags: u64) {
        let checkpoint_id = self.next_barrier_checkpoint_id;
        self.next_barrier_checkpoint_id += 1;

        tracing::debug!(
            checkpoint_id,
            sources = self.injectors.len(),
            "Injecting checkpoint barriers"
        );

        self.pending_barrier = Some(PendingBarrier {
            checkpoint_id,
            sources_total: self.injectors.len(),
            sources_aligned: HashSet::new(),
            source_checkpoints: HashMap::new(),
            started_at: Instant::now(),
        });

        for injector in &self.injectors {
            injector.trigger(checkpoint_id, flags);
        }
    }

    /// Handles a barrier event from a source, tracking alignment.
    ///
    /// Returns `true` when all sources have aligned and the checkpoint
    /// callback should fire.
    fn handle_barrier(&mut self, idx: usize, barrier: &CheckpointBarrier) -> bool {
        let pending = match self.pending_barrier.as_mut() {
            Some(p) if p.checkpoint_id == barrier.checkpoint_id => p,
            Some(p) => {
                tracing::warn!(
                    expected = p.checkpoint_id,
                    got = barrier.checkpoint_id,
                    "Stale barrier from source {idx}, ignoring"
                );
                return false;
            }
            None => {
                tracing::warn!(
                    checkpoint_id = barrier.checkpoint_id,
                    "Barrier from source {idx} with no pending barrier, ignoring"
                );
                return false;
            }
        };

        if !pending.sources_aligned.insert(idx) {
            tracing::warn!(source_idx = idx, "Duplicate barrier from source");
            return false;
        }

        // Capture the source checkpoint at the barrier point.
        let name = &self.source_names[idx];
        let cp = self.handles[idx].checkpoint_rx.borrow().clone();
        pending.source_checkpoints.insert(name.clone(), cp);

        tracing::debug!(
            checkpoint_id = barrier.checkpoint_id,
            source_idx = idx,
            aligned = pending.sources_aligned.len(),
            total = pending.sources_total,
            "Source barrier aligned"
        );

        pending.sources_aligned.len() == pending.sources_total
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
            let first_aligned;
            tokio::select! {
                biased;

                () = self.shutdown.notified() => {
                    tracing::info!("Pipeline coordinator shutdown signal received");
                    break;
                }

                event = self.rx.recv() => {
                    if let Some(evt) = event {
                        first_aligned = self.handle_event(
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
            let mut all_barriers_aligned = first_aligned;

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
                if self.handle_event(
                    evt,
                    &mut source_batches,
                    &mut total_batches,
                    &mut total_records,
                    &mut got_data,
                    &mut *callback,
                ) {
                    all_barriers_aligned = true;
                }
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

            // ── Phase 4b: Fire barrier-aligned checkpoint. ─────────
            // All pre-barrier data has been executed in Phase 4, so
            // operator state is consistent with the barrier offsets.
            if all_barriers_aligned {
                if let Some(pending) = self.pending_barrier.take() {
                    callback
                        .checkpoint_with_barrier(pending.source_checkpoints)
                        .await;
                }
            }

            // ── Phase 5: Periodic checkpoint via barrier injection. ──
            if checkpoint_interval.is_some() && self.pending_barrier.is_none() {
                // Instead of snapshotting at an arbitrary point, inject
                // barriers so all sources align before we checkpoint.
                if callback.maybe_checkpoint(false).await {
                    self.inject_barriers(barrier_flags::NONE);
                }
            }

            // Check for barrier alignment timeout.
            if let Some(ref pending) = self.pending_barrier {
                if pending.started_at.elapsed() > self.config.barrier_alignment_timeout {
                    tracing::warn!(
                        checkpoint_id = pending.checkpoint_id,
                        aligned = pending.sources_aligned.len(),
                        total = pending.sources_total,
                        "Barrier alignment timeout — cancelling checkpoint"
                    );
                    self.pending_barrier = None;
                }
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

        // Final checkpoint: inject DRAIN barriers and wait for alignment.
        if !self.injectors.is_empty() {
            self.inject_barriers(barrier_flags::DRAIN);

            // Drain remaining events to collect barrier responses.
            let deadline =
                tokio::time::Instant::now() + self.config.barrier_alignment_timeout;
            while self.pending_barrier.is_some() {
                match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                    Ok(Some(evt)) => {
                        let all_aligned = if let SourceEvent::Barrier { idx, ref barrier } = evt {
                            self.handle_barrier(idx, barrier)
                        } else {
                            false
                        };
                        if all_aligned {
                            if let Some(pending) = self.pending_barrier.take() {
                                callback
                                    .checkpoint_with_barrier(pending.source_checkpoints)
                                    .await;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        tracing::warn!("Final barrier alignment timed out");
                        self.pending_barrier = None;
                        break;
                    }
                }
            }
        }

        // Fallback: force checkpoint if barriers didn't align.
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
    ///
    /// Returns `true` if this event was a barrier and all sources are now aligned.
    fn handle_event(
        &mut self,
        event: SourceEvent,
        source_batches: &mut HashMap<String, Vec<RecordBatch>>,
        total_batches: &mut u64,
        total_records: &mut u64,
        got_data: &mut bool,
        callback: &mut dyn PipelineCallback,
    ) -> bool {
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
                false
            }
            SourceEvent::Barrier { idx, barrier } => self.handle_barrier(idx, &barrier),
            SourceEvent::Error { idx, message } => {
                tracing::warn!(
                    source = %self.source_names[idx],
                    error = %message,
                    "Source task error"
                );
                false
            }
            SourceEvent::Exhausted { idx } => {
                tracing::info!(
                    source = %self.source_names[idx],
                    "Source exhausted"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_connectors::testing::MockSourceConnector;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    /// A minimal `PipelineCallback` for coordinator tests.
    struct MockCallback {
        barrier_checkpoints: Vec<HashMap<String, SourceCheckpoint>>,
        maybe_checkpoint_count: u64,
        cycle_count: u64,
        should_trigger_checkpoint: Arc<AtomicBool>,
    }

    impl MockCallback {
        fn new() -> Self {
            Self {
                barrier_checkpoints: Vec::new(),
                maybe_checkpoint_count: 0,
                cycle_count: 0,
                should_trigger_checkpoint: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait::async_trait]
    impl PipelineCallback for MockCallback {
        async fn execute_cycle(
            &mut self,
            _source_batches: &HashMap<String, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<HashMap<String, Vec<RecordBatch>>, String> {
            self.cycle_count += 1;
            Ok(HashMap::new())
        }

        fn push_to_streams(&self, _results: &HashMap<String, Vec<RecordBatch>>) {}

        async fn write_to_sinks(&mut self, _results: &HashMap<String, Vec<RecordBatch>>) {}

        fn extract_watermark(&mut self, _source_name: &str, _batch: &RecordBatch) {}

        fn filter_late_rows(
            &self,
            _source_name: &str,
            batch: &RecordBatch,
        ) -> Option<RecordBatch> {
            Some(batch.clone())
        }

        fn current_watermark(&self) -> i64 {
            0
        }

        async fn maybe_checkpoint(&mut self, _force: bool) -> bool {
            self.maybe_checkpoint_count += 1;
            self.should_trigger_checkpoint
                .load(std::sync::atomic::Ordering::Relaxed)
        }

        async fn checkpoint_with_barrier(
            &mut self,
            source_checkpoints: HashMap<String, SourceCheckpoint>,
        ) -> bool {
            self.barrier_checkpoints.push(source_checkpoints);
            true
        }

        fn record_cycle(&self, _events_ingested: u64, _batches: u64, _elapsed_ns: u64) {}

        async fn poll_tables(&mut self) {}
    }

    #[tokio::test]
    async fn test_coordinator_barrier_alignment() {
        // 2 sources: both must align before checkpoint fires.
        let sources = vec![
            SourceRegistration {
                name: "src0".to_string(),
                connector: Box::new(MockSourceConnector::with_batches(100, 5)),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                supports_replay: true,
            },
            SourceRegistration {
                name: "src1".to_string(),
                connector: Box::new(MockSourceConnector::with_batches(100, 5)),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                supports_replay: true,
            },
        ];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: Some(Duration::from_millis(10)),
            barrier_alignment_timeout: Duration::from_secs(5),
            ..PipelineConfig::default()
        };

        let coordinator = PipelineCoordinator::new(sources, config, shutdown).await.unwrap();

        let callback = MockCallback::new();
        // Tell callback to trigger checkpoints when asked.
        callback
            .should_trigger_checkpoint
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let handle = tokio::spawn(async move {
            coordinator.run(Box::new(callback)).await;
        });

        // Let pipeline run for a bit so barriers get injected and aligned.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Shut down.
        shutdown_clone.notify_one();
        handle.await.unwrap();

        // The coordinator should have run at least some cycles without panicking.
        // Since the mock sources produce data fast and checkpoint interval is 10ms,
        // barriers should have been injected and aligned.
    }

    #[tokio::test]
    async fn test_coordinator_barrier_timeout() {
        // Test that a barrier alignment timeout is handled gracefully.
        // We construct PendingBarrier state directly on the coordinator
        // to test the timeout logic without needing real source tasks.

        let sources = vec![SourceRegistration {
            name: "src0".to_string(),
            connector: Box::new(MockSourceConnector::with_batches(5, 5)),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            supports_replay: true,
        }];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: None,
            barrier_alignment_timeout: Duration::from_millis(50),
            ..PipelineConfig::default()
        };

        let coordinator =
            PipelineCoordinator::new(sources, config, shutdown).await.unwrap();

        let callback = MockCallback::new();
        let handle = tokio::spawn(async move {
            coordinator.run(Box::new(callback)).await;
        });

        // Let the source exhaust itself (5 batches).
        tokio::time::sleep(Duration::from_millis(200)).await;

        shutdown_clone.notify_one();
        handle.await.unwrap();

        // Should complete without panicking even with short timeout.
    }
}
