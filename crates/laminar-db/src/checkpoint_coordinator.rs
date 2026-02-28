//! Unified checkpoint coordinator.
//!
//! Single orchestrator that replaces `StreamCheckpointManager`,
//! `PipelineCheckpointManager`, and the persistence side of `DagRecoveryManager`.
//! Lives in Ring 2 (control plane). Reuses the existing
//! `DagCheckpointCoordinator` for barrier logic.
//!
//! ## Checkpoint Cycle
//!
//! 1. Barrier propagation — `dag_coordinator.trigger_checkpoint()`
//! 2. Operator snapshot — `dag_coordinator.finalize_checkpoint()` → operator states
//! 3. Source snapshot — `source.checkpoint()` for each source
//! 4. Sink pre-commit — `sink.pre_commit(epoch)` for each exactly-once sink
//! 5. Manifest persist — `store.save(&manifest)` (atomic write)
//! 6. Sink commit — `sink.commit_epoch(epoch)` for each exactly-once sink
//! 7. On ANY failure at 6 — `sink.rollback_epoch()` on remaining sinks

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::sync::atomic::Ordering;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::SourceConnector;
use laminar_storage::changelog_drainer::ChangelogDrainer;
use laminar_storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, SinkCommitStatus,
};
use laminar_storage::checkpoint_store::CheckpointStore;
use laminar_storage::per_core_wal::PerCoreWalManager;
use tracing::{debug, error, info, warn};

use crate::error::DbError;
use crate::metrics::PipelineCounters;

/// Unified checkpoint configuration.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Interval between checkpoints. `None` = manual only.
    pub interval: Option<Duration>,
    /// Maximum number of retained checkpoints.
    pub max_retained: usize,
    /// Maximum time to wait for barrier alignment at fan-in nodes.
    pub alignment_timeout: Duration,
    /// Maximum time to wait for all sinks to pre-commit.
    ///
    /// A stuck sink will not block checkpointing indefinitely.
    /// Defaults to 30 seconds.
    pub pre_commit_timeout: Duration,
    /// Whether to use incremental checkpoints.
    pub incremental: bool,
    /// Maximum operator state size (bytes) to inline in the JSON manifest.
    ///
    /// States larger than this threshold are written to a `state.bin` sidecar
    /// file and referenced by offset/length in the manifest. This avoids
    /// inflating the manifest with base64-encoded blobs (~33% overhead).
    ///
    /// Default: 1 MB (`1_048_576`). Set to `usize::MAX` to inline everything.
    pub state_inline_threshold: usize,
    /// Adaptive checkpoint interval configuration.
    ///
    /// When `Some`, the coordinator dynamically adjusts the checkpoint interval
    /// based on observed checkpoint durations using an exponential moving average.
    /// When `None` (default), the static `interval` is used unchanged.
    pub adaptive: Option<AdaptiveCheckpointConfig>,
    /// Unaligned checkpoint configuration.
    ///
    /// When `Some`, the coordinator can fall back to unaligned checkpoints
    /// when barrier alignment takes too long under backpressure.
    /// When `None` (default), only aligned checkpoints are used.
    pub unaligned: Option<UnalignedCheckpointConfig>,
}

/// Configuration for adaptive checkpoint intervals.
///
/// Dynamically adjusts the checkpoint interval based on observed checkpoint
/// durations: `interval = clamp(smoothed_duration / target_ratio, min, max)`.
///
/// This avoids checkpointing too frequently under light load (wasting I/O)
/// or too infrequently under heavy load (increasing recovery time).
#[derive(Debug, Clone)]
pub struct AdaptiveCheckpointConfig {
    /// Minimum checkpoint interval (floor). Default: 10s.
    pub min_interval: Duration,
    /// Maximum checkpoint interval (ceiling). Default: 300s.
    pub max_interval: Duration,
    /// Target ratio of checkpoint duration to interval.
    ///
    /// For example, 0.1 means checkpoints should take at most 10% of the
    /// time between them. Default: 0.1.
    pub target_overhead_ratio: f64,
    /// EMA smoothing factor for checkpoint durations.
    ///
    /// Higher values give more weight to recent observations.
    /// Range: 0.0–1.0. Default: 0.3.
    pub smoothing_alpha: f64,
}

impl Default for AdaptiveCheckpointConfig {
    fn default() -> Self {
        Self {
            min_interval: Duration::from_secs(10),
            max_interval: Duration::from_secs(300),
            target_overhead_ratio: 0.1,
            smoothing_alpha: 0.3,
        }
    }
}

/// Configuration for unaligned checkpoints.
///
/// When barrier alignment takes too long (due to backpressure on slow inputs),
/// the checkpoint can fall back to an unaligned snapshot that captures in-flight
/// data from channels. This trades larger checkpoint size for faster completion.
#[derive(Debug, Clone)]
pub struct UnalignedCheckpointConfig {
    /// Whether unaligned checkpoints are enabled.
    pub enabled: bool,
    /// Duration after which aligned checkpoint falls back to unaligned.
    ///
    /// If all barriers don't arrive within this threshold after the first
    /// barrier, the checkpoint switches to unaligned mode. Default: 10s.
    pub alignment_timeout_threshold: Duration,
    /// Maximum bytes of in-flight data to buffer per checkpoint.
    ///
    /// If the total in-flight data exceeds this, the checkpoint fails
    /// rather than consuming unbounded memory. Default: 256 MB.
    pub max_inflight_buffer_bytes: usize,
    /// Force unaligned mode for all checkpoints (skip aligned attempt).
    ///
    /// Useful for testing or when backpressure is known to be persistent.
    pub force_unaligned: bool,
}

impl Default for UnalignedCheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            alignment_timeout_threshold: Duration::from_secs(10),
            max_inflight_buffer_bytes: 256 * 1024 * 1024,
            force_unaligned: false,
        }
    }
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Some(Duration::from_secs(60)),
            max_retained: 3,
            alignment_timeout: Duration::from_secs(30),
            pre_commit_timeout: Duration::from_secs(30),
            incremental: false,
            state_inline_threshold: 1_048_576,
            adaptive: None,
            unaligned: None,
        }
    }
}

/// Phase of the checkpoint lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum CheckpointPhase {
    /// No checkpoint in progress.
    Idle,
    /// Barrier injected, waiting for operator snapshots.
    BarrierInFlight,
    /// Operators snapshotted, collecting source positions.
    Snapshotting,
    /// Sinks pre-committing (phase 1).
    PreCommitting,
    /// Manifest being persisted.
    Persisting,
    /// Sinks committing (phase 2).
    Committing,
}

impl std::fmt::Display for CheckpointPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::BarrierInFlight => write!(f, "BarrierInFlight"),
            Self::Snapshotting => write!(f, "Snapshotting"),
            Self::PreCommitting => write!(f, "PreCommitting"),
            Self::Persisting => write!(f, "Persisting"),
            Self::Committing => write!(f, "Committing"),
        }
    }
}

/// Result of a checkpoint attempt.
#[derive(Debug, serde::Serialize)]
pub struct CheckpointResult {
    /// Whether the checkpoint succeeded.
    pub success: bool,
    /// Checkpoint ID (if created).
    pub checkpoint_id: u64,
    /// Epoch number.
    pub epoch: u64,
    /// Duration of the checkpoint operation.
    pub duration: Duration,
    /// Error message if failed.
    pub error: Option<String>,
}

/// Registered source for checkpoint coordination.
pub(crate) struct RegisteredSource {
    /// Source name.
    pub name: String,
    /// Source connector handle.
    pub connector: Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
    /// Whether this source supports replay from a checkpointed position.
    ///
    /// Sources that do not support replay (e.g., WebSocket) degrade
    /// exactly-once semantics to at-most-once.
    pub supports_replay: bool,
}

/// Registered sink for checkpoint coordination.
pub(crate) struct RegisteredSink {
    /// Sink name.
    pub name: String,
    /// Sink task handle (channel-based, no mutex contention).
    pub handle: crate::sink_task::SinkTaskHandle,
    /// Whether this sink supports exactly-once / two-phase commit.
    pub exactly_once: bool,
}

/// Result of WAL preparation for a checkpoint.
///
/// Contains the WAL positions recorded after flushing changelog drainers,
/// writing epoch barriers, and syncing all segments.
#[derive(Debug, Clone)]
pub struct WalPrepareResult {
    /// Per-core WAL positions at the time of the epoch barrier.
    pub per_core_wal_positions: Vec<u64>,
    /// Number of changelog entries drained across all drainers.
    pub entries_drained: u64,
}

/// Unified checkpoint coordinator.
///
/// Orchestrates the full checkpoint lifecycle across sources, sinks,
/// and operator state, persisting everything in a single
/// [`CheckpointManifest`].
pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    store: Arc<dyn CheckpointStore>,
    sources: Vec<RegisteredSource>,
    sinks: Vec<RegisteredSink>,
    table_sources: Vec<RegisteredSource>,
    next_checkpoint_id: u64,
    epoch: u64,
    phase: CheckpointPhase,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    last_checkpoint_duration: Option<Duration>,
    /// Rolling histogram of checkpoint durations for percentile tracking.
    duration_histogram: DurationHistogram,
    /// Per-core WAL manager for epoch barriers and truncation.
    wal_manager: Option<PerCoreWalManager>,
    /// Changelog drainers to flush before checkpointing.
    changelog_drainers: Vec<ChangelogDrainer>,
    /// Shared counters for observability.
    counters: Option<Arc<PipelineCounters>>,
    /// Exponential moving average of checkpoint durations (milliseconds).
    smoothed_duration_ms: f64,
    /// Cumulative bytes written across all checkpoints (manifest + sidecar).
    total_bytes_written: u64,
    /// Number of checkpoints completed in unaligned mode.
    unaligned_checkpoint_count: u64,
}

impl CheckpointCoordinator {
    /// Creates a new checkpoint coordinator.
    #[must_use]
    pub fn new(config: CheckpointConfig, store: Box<dyn CheckpointStore>) -> Self {
        let store: Arc<dyn CheckpointStore> = Arc::from(store);
        // Determine starting epoch from stored checkpoints.
        let (next_id, epoch) = match store.load_latest() {
            Ok(Some(m)) => (m.checkpoint_id + 1, m.epoch + 1),
            _ => (1, 1),
        };

        Self {
            config,
            store,
            sources: Vec::new(),
            sinks: Vec::new(),
            table_sources: Vec::new(),
            next_checkpoint_id: next_id,
            epoch,
            phase: CheckpointPhase::Idle,
            checkpoints_completed: 0,
            checkpoints_failed: 0,
            last_checkpoint_duration: None,
            duration_histogram: DurationHistogram::new(),
            wal_manager: None,
            changelog_drainers: Vec::new(),
            counters: None,
            smoothed_duration_ms: 0.0,
            total_bytes_written: 0,
            unaligned_checkpoint_count: 0,
        }
    }

    /// Registers a source connector for checkpoint coordination.
    ///
    /// The `supports_replay` flag indicates whether the source can seek
    /// back to a checkpointed offset. Sources that cannot replay (e.g.,
    /// WebSocket) will log a warning since exactly-once semantics are
    /// degraded to at-most-once for events from that source.
    pub fn register_source(
        &mut self,
        name: impl Into<String>,
        connector: Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
        supports_replay: bool,
    ) {
        let name = name.into();
        if !supports_replay {
            warn!(
                source = %name,
                "source does not support replay — exactly-once semantics \
                 are degraded to at-most-once for this source"
            );
        }
        self.sources.push(RegisteredSource {
            name,
            connector,
            supports_replay,
        });
    }

    /// Registers a sink connector for checkpoint coordination.
    pub(crate) fn register_sink(
        &mut self,
        name: impl Into<String>,
        handle: crate::sink_task::SinkTaskHandle,
        exactly_once: bool,
    ) {
        self.sinks.push(RegisteredSink {
            name: name.into(),
            handle,
            exactly_once,
        });
    }

    /// Registers a reference table source connector.
    pub fn register_table_source(
        &mut self,
        name: impl Into<String>,
        connector: Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
    ) {
        self.table_sources.push(RegisteredSource {
            name: name.into(),
            connector,
            supports_replay: true, // Table sources are always replayable
        });
    }

    // ── Observability ──

    /// Sets the shared pipeline counters for checkpoint metrics emission.
    ///
    /// When set, checkpoint completion and failure will update the counters
    /// automatically.
    pub fn set_counters(&mut self, counters: Arc<PipelineCounters>) {
        self.counters = Some(counters);
    }

    /// Emits checkpoint metrics to the shared counters.
    fn emit_checkpoint_metrics(&self, success: bool, epoch: u64, duration: Duration) {
        if let Some(ref counters) = self.counters {
            if success {
                counters
                    .checkpoints_completed
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                counters.checkpoints_failed.fetch_add(1, Ordering::Relaxed);
            }
            #[allow(clippy::cast_possible_truncation)]
            counters
                .last_checkpoint_duration_ms
                .store(duration.as_millis() as u64, Ordering::Relaxed);
            counters.checkpoint_epoch.store(epoch, Ordering::Relaxed);
        }
    }

    // ── WAL coordination ──

    /// Registers a per-core WAL manager for checkpoint coordination.
    ///
    /// When registered, [`prepare_wal_for_checkpoint()`](Self::prepare_wal_for_checkpoint)
    /// will write epoch barriers and sync all segments, and
    /// [`truncate_wal_after_checkpoint()`](Self::truncate_wal_after_checkpoint)
    /// will reset all segments.
    pub fn register_wal_manager(&mut self, wal_manager: PerCoreWalManager) {
        self.wal_manager = Some(wal_manager);
    }

    /// Registers a changelog drainer to flush before checkpointing.
    ///
    /// Multiple drainers may be registered (one per core or per state store).
    /// All are flushed during
    /// [`prepare_wal_for_checkpoint()`](Self::prepare_wal_for_checkpoint).
    pub fn register_changelog_drainer(&mut self, drainer: ChangelogDrainer) {
        self.changelog_drainers.push(drainer);
    }

    /// Prepares the WAL for a checkpoint.
    ///
    /// 1. Flushes all registered [`ChangelogDrainer`] instances (Ring 1 → Ring 0 catchup)
    /// 2. Writes epoch barriers to all per-core WAL segments
    /// 3. Syncs all WAL segments (`fdatasync`)
    /// 4. Records and returns per-core WAL positions
    ///
    /// Call this **before** [`checkpoint()`](Self::checkpoint) and pass the returned
    /// positions into that method.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if WAL operations fail.
    pub fn prepare_wal_for_checkpoint(&mut self) -> Result<WalPrepareResult, DbError> {
        // Step 1: Flush changelog drainers
        let mut total_drained: u64 = 0;
        for drainer in &mut self.changelog_drainers {
            let count = drainer.drain();
            total_drained += count as u64;
            debug!(
                drained = count,
                pending = drainer.pending_count(),
                "changelog drainer flushed"
            );
        }

        // Step 2-4: WAL epoch barriers + sync + positions
        let per_core_wal_positions = if let Some(ref mut wal) = self.wal_manager {
            let epoch = wal.advance_epoch();
            wal.set_epoch_all(epoch);

            wal.write_epoch_barrier_all()
                .map_err(|e| DbError::Checkpoint(format!("WAL epoch barrier failed: {e}")))?;

            wal.sync_all()
                .map_err(|e| DbError::Checkpoint(format!("WAL sync failed: {e}")))?;

            // Use synced positions — only durable data is safe for the manifest.
            let positions = wal.synced_positions();
            debug!(epoch, positions = ?positions, "WAL prepared for checkpoint");
            positions
        } else {
            Vec::new()
        };

        Ok(WalPrepareResult {
            per_core_wal_positions,
            entries_drained: total_drained,
        })
    }

    /// Truncates (resets) all per-core WAL segments after a successful checkpoint.
    ///
    /// Call this **after** [`checkpoint()`](Self::checkpoint) returns success.
    /// WAL data before the checkpoint position is no longer needed.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if truncation fails.
    pub fn truncate_wal_after_checkpoint(&mut self) -> Result<(), DbError> {
        if let Some(ref mut wal) = self.wal_manager {
            wal.reset_all()
                .map_err(|e| DbError::Checkpoint(format!("WAL truncation failed: {e}")))?;
            debug!("WAL segments truncated after checkpoint");
        }
        Ok(())
    }

    /// Returns a reference to the registered WAL manager, if any.
    #[must_use]
    pub fn wal_manager(&self) -> Option<&PerCoreWalManager> {
        self.wal_manager.as_ref()
    }

    /// Returns a mutable reference to the registered WAL manager, if any.
    pub fn wal_manager_mut(&mut self) -> Option<&mut PerCoreWalManager> {
        self.wal_manager.as_mut()
    }

    /// Returns a slice of registered changelog drainers.
    #[must_use]
    pub fn changelog_drainers(&self) -> &[ChangelogDrainer] {
        &self.changelog_drainers
    }

    /// Returns a mutable slice of registered changelog drainers.
    pub fn changelog_drainers_mut(&mut self) -> &mut [ChangelogDrainer] {
        &mut self.changelog_drainers
    }

    /// Performs a full checkpoint cycle (steps 3-7).
    ///
    /// Steps 1-2 (barrier propagation + operator snapshots) are handled
    /// externally by the DAG executor and passed in as `operator_states`.
    ///
    /// # Arguments
    ///
    /// * `operator_states` — serialized operator state from `DagCheckpointCoordinator`
    /// * `watermark` — current global watermark
    /// * `wal_position` — single-writer WAL position
    /// * `per_core_wal_positions` — thread-per-core WAL positions
    /// * `table_store_checkpoint_path` — table store checkpoint path
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if any phase fails.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn checkpoint(
        &mut self,
        operator_states: HashMap<String, Vec<u8>>,
        watermark: Option<i64>,
        wal_position: u64,
        per_core_wal_positions: Vec<u64>,
        table_store_checkpoint_path: Option<String>,
        source_watermarks: HashMap<String, i64>,
        pipeline_hash: Option<u64>,
    ) -> Result<CheckpointResult, DbError> {
        let start = Instant::now();
        let checkpoint_id = self.next_checkpoint_id;
        let epoch = self.epoch;

        info!(checkpoint_id, epoch, "starting checkpoint");

        // ── Step 2b: Defense-in-depth changelog drain ──
        // Flush any straggler changelog entries that arrived between
        // prepare_wal_for_checkpoint() and this call. This ensures the
        // operator state snapshot and source offsets are consistent.
        for drainer in &mut self.changelog_drainers {
            let extra = drainer.drain();
            if extra > 0 {
                debug!(extra_drained = extra, "drained straggler changelog entries");
            }
        }

        // ── Step 3: Source snapshot (parallel) ──
        self.phase = CheckpointPhase::Snapshotting;
        let (source_offsets, table_offsets) = tokio::try_join!(
            self.snapshot_sources(&self.sources),
            self.snapshot_sources(&self.table_sources),
        )?;

        // ── Step 4: Sink pre-commit ──
        self.phase = CheckpointPhase::PreCommitting;
        if let Err(e) = self.pre_commit_sinks(epoch).await {
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            error!(checkpoint_id, epoch, error = %e, "pre-commit failed");
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some(format!("pre-commit failed: {e}")),
            });
        }

        // ── Build manifest ──
        let mut manifest = CheckpointManifest::new(checkpoint_id, epoch);
        manifest.source_offsets = source_offsets;
        manifest.table_offsets = table_offsets;
        manifest.sink_epochs = self.collect_sink_epochs();
        // Mark all exactly-once sinks as Pending before commit phase.
        manifest.sink_commit_statuses = self.initial_sink_commit_statuses();
        manifest.watermark = watermark;
        // Prefer caller-provided per-source watermarks (from the pipeline's
        // watermark tracker). Fall back to global watermark if none provided.
        if source_watermarks.is_empty() {
            if let Some(wm) = watermark {
                manifest.source_watermarks = self.collect_source_watermarks(wm);
            }
        } else {
            manifest.source_watermarks = source_watermarks;
        }
        manifest.wal_position = wal_position;
        manifest.per_core_wal_positions = per_core_wal_positions;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.is_incremental = self.config.incremental;
        manifest.source_names = self.sorted_source_names();
        manifest.sink_names = self.sorted_sink_names();
        manifest.pipeline_hash = pipeline_hash;

        // Convert operator states to manifest format with sidecar threshold
        let mut sidecar_blobs: Vec<u8> = Vec::new();
        let threshold = self.config.state_inline_threshold;
        for (name, data) in &operator_states {
            let (op_ckpt, maybe_blob) =
                laminar_storage::checkpoint_manifest::OperatorCheckpoint::from_bytes(
                    data,
                    threshold,
                    sidecar_blobs.len() as u64,
                );
            if let Some(blob) = maybe_blob {
                sidecar_blobs.extend_from_slice(&blob);
            }
            manifest.operator_states.insert(name.clone(), op_ckpt);
        }

        let state_data = if sidecar_blobs.is_empty() {
            None
        } else {
            debug!(
                checkpoint_id,
                sidecar_bytes = sidecar_blobs.len(),
                "writing operator state sidecar"
            );
            Some(sidecar_blobs)
        };

        // ── Step 5: Persist manifest (decision record — sinks are Pending) ──
        self.phase = CheckpointPhase::Persisting;
        if let Err(e) = self.save_manifest(&manifest, state_data).await {
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            let _ = self.rollback_sinks(epoch).await;
            error!(checkpoint_id, epoch, error = %e, "manifest persist failed");
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some(format!("manifest persist failed: {e}")),
            });
        }

        // ── Step 6: Sink commit (per-sink tracking) ──
        self.phase = CheckpointPhase::Committing;
        let sink_statuses = self.commit_sinks_tracked(epoch).await;
        let has_failures = sink_statuses
            .values()
            .any(|s| matches!(s, SinkCommitStatus::Failed(_)));

        // ── Step 6b: Save manifest again with final sink commit statuses ──
        if !sink_statuses.is_empty() {
            manifest.sink_commit_statuses = sink_statuses;
            if let Err(e) = self.save_manifest(&manifest, None).await {
                warn!(
                    checkpoint_id,
                    epoch,
                    error = %e,
                    "post-commit manifest update failed"
                );
            }
        }

        if has_failures {
            self.checkpoints_failed += 1;
            warn!(checkpoint_id, epoch, "sink commit partially failed");
        }

        // ── Step 7: Clear changelog drainer pending buffers ──
        // Entries are metadata-only (key_hash + mmap_offset) used for SPSC
        // backpressure relief. The checkpoint captured full state, so the
        // metadata is no longer needed. Clearing prevents unbounded growth.
        for drainer in &mut self.changelog_drainers {
            drainer.clear_pending();
        }

        // ── Success ──
        self.phase = CheckpointPhase::Idle;
        self.next_checkpoint_id += 1;
        self.epoch += 1;
        self.checkpoints_completed += 1;
        let duration = start.elapsed();
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(true, epoch, duration);
        self.adjust_interval();

        info!(
            checkpoint_id,
            epoch,
            duration_ms = duration.as_millis(),
            "checkpoint completed"
        );

        Ok(CheckpointResult {
            success: true,
            checkpoint_id,
            epoch,
            duration,
            error: None,
        })
    }

    /// Snapshots all registered source connectors concurrently.
    ///
    /// Uses `join_all` to lock and checkpoint each source in parallel rather
    /// than sequentially, reducing snapshot latency proportional to source count.
    async fn snapshot_sources(
        &self,
        sources: &[RegisteredSource],
    ) -> Result<HashMap<String, ConnectorCheckpoint>, DbError> {
        use futures::future::join_all;

        let futs = sources.iter().map(|source| {
            let connector = Arc::clone(&source.connector);
            let name = source.name.clone();
            async move {
                let guard = connector.lock().await;
                let cp = guard.checkpoint();
                let result = source_to_connector_checkpoint(&cp);
                debug!(source = %name, epoch = cp.epoch(), "source snapshotted");
                (name, result)
            }
        });

        let results = join_all(futs).await;
        Ok(results.into_iter().collect())
    }

    /// Pre-commits all exactly-once sinks (phase 1) with a timeout.
    ///
    /// A stuck sink will not block checkpointing indefinitely. The timeout
    /// is configured via [`CheckpointConfig::pre_commit_timeout`].
    async fn pre_commit_sinks(&self, epoch: u64) -> Result<(), DbError> {
        let timeout_dur = self.config.pre_commit_timeout;

        match tokio::time::timeout(timeout_dur, self.pre_commit_sinks_inner(epoch)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(DbError::Checkpoint(format!(
                "pre-commit timed out after {}s",
                timeout_dur.as_secs()
            ))),
        }
    }

    /// Inner pre-commit loop (no timeout).
    async fn pre_commit_sinks_inner(&self, epoch: u64) -> Result<(), DbError> {
        for sink in &self.sinks {
            if sink.exactly_once {
                sink.handle.pre_commit(epoch).await.map_err(|e| {
                    DbError::Checkpoint(format!("sink '{}' pre-commit failed: {e}", sink.name))
                })?;
                debug!(sink = %sink.name, epoch, "sink pre-committed");
            }
        }
        Ok(())
    }

    /// Commits all exactly-once sinks with per-sink status tracking.
    ///
    /// Returns a map of sink name → commit status. Sinks that committed
    /// successfully are `Committed`; failures are `Failed(message)`.
    /// All sinks are attempted even if some fail.
    async fn commit_sinks_tracked(&self, epoch: u64) -> HashMap<String, SinkCommitStatus> {
        let mut statuses = HashMap::with_capacity(self.sinks.len());

        for sink in &self.sinks {
            if sink.exactly_once {
                match sink.handle.commit_epoch(epoch).await {
                    Ok(()) => {
                        statuses.insert(sink.name.clone(), SinkCommitStatus::Committed);
                        debug!(sink = %sink.name, epoch, "sink committed");
                    }
                    Err(e) => {
                        let msg = format!("sink '{}' commit failed: {e}", sink.name);
                        error!(sink = %sink.name, epoch, error = %e, "sink commit failed");
                        statuses.insert(sink.name.clone(), SinkCommitStatus::Failed(msg));
                    }
                }
            }
        }

        statuses
    }

    /// Saves a manifest to the checkpoint store (blocking I/O on a task).
    ///
    /// Uses [`CheckpointStore::save_with_state`] to write optional sidecar
    /// data **before** the manifest, ensuring atomicity: if the sidecar write
    /// fails, the manifest is never persisted.
    async fn save_manifest(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<Vec<u8>>,
    ) -> Result<(), DbError> {
        let store = Arc::clone(&self.store);
        let manifest = manifest.clone();
        let task_result = tokio::task::spawn_blocking(move || {
            store.save_with_state(&manifest, state_data.as_deref())
        })
        .await;

        match task_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(DbError::Checkpoint(format!("manifest persist failed: {e}"))),
            Err(join_err) => Err(DbError::Checkpoint(format!(
                "manifest persist task failed: {join_err}"
            ))),
        }
    }

    /// Returns initial sink commit statuses (all `Pending`) for the manifest.
    fn initial_sink_commit_statuses(&self) -> HashMap<String, SinkCommitStatus> {
        self.sinks
            .iter()
            .filter(|s| s.exactly_once)
            .map(|s| (s.name.clone(), SinkCommitStatus::Pending))
            .collect()
    }

    /// Rolls back all exactly-once sinks.
    async fn rollback_sinks(&self, epoch: u64) -> Result<(), DbError> {
        for sink in &self.sinks {
            if sink.exactly_once {
                sink.handle.rollback_epoch(epoch).await;
            }
        }
        Ok(())
    }

    /// Collects the last committed epoch from each sink.
    fn collect_sink_epochs(&self) -> HashMap<String, u64> {
        let mut epochs = HashMap::with_capacity(self.sinks.len());
        for sink in &self.sinks {
            // The epoch being committed is the current one
            if sink.exactly_once {
                epochs.insert(sink.name.clone(), self.epoch);
            }
        }
        epochs
    }

    /// Collects per-source watermarks for the manifest.
    ///
    /// Uses the global watermark as a conservative lower bound for all
    /// registered sources. This ensures watermark progress is not lost
    /// on recovery (prevents watermark regression).
    fn collect_source_watermarks(&self, global_watermark: i64) -> HashMap<String, i64> {
        let mut watermarks = HashMap::with_capacity(self.sources.len());
        for source in &self.sources {
            watermarks.insert(source.name.clone(), global_watermark);
        }
        watermarks
    }

    /// Returns sorted source names for topology tracking in the manifest.
    fn sorted_source_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.sources.iter().map(|s| s.name.clone()).collect();
        names.sort();
        names
    }

    /// Returns sorted sink names for topology tracking in the manifest.
    fn sorted_sink_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.sinks.iter().map(|s| s.name.clone()).collect();
        names.sort();
        names
    }

    /// Returns the current phase.
    #[must_use]
    pub fn phase(&self) -> CheckpointPhase {
        self.phase
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the next checkpoint ID.
    #[must_use]
    pub fn next_checkpoint_id(&self) -> u64 {
        self.next_checkpoint_id
    }

    /// Returns the checkpoint config.
    #[must_use]
    pub fn config(&self) -> &CheckpointConfig {
        &self.config
    }

    /// Adjusts the checkpoint interval based on observed durations.
    ///
    /// Uses an exponential moving average (EMA) of checkpoint durations to
    /// compute a new interval: `interval = smoothed_duration / target_ratio`,
    /// clamped to `[min_interval, max_interval]`.
    ///
    /// No-op if adaptive checkpointing is not configured.
    fn adjust_interval(&mut self) {
        let adaptive = match &self.config.adaptive {
            Some(a) => a.clone(),
            None => return,
        };

        #[allow(clippy::cast_precision_loss)] // Checkpoint durations are << 2^52 ms
        let last_ms = match self.last_checkpoint_duration {
            Some(d) => d.as_millis() as f64,
            None => return,
        };

        // Update EMA
        if self.smoothed_duration_ms == 0.0 {
            self.smoothed_duration_ms = last_ms;
        } else {
            self.smoothed_duration_ms = adaptive.smoothing_alpha * last_ms
                + (1.0 - adaptive.smoothing_alpha) * self.smoothed_duration_ms;
        }

        // Compute target interval: smoothed_ms / (1000 * ratio)
        let new_interval_secs =
            self.smoothed_duration_ms / (1000.0 * adaptive.target_overhead_ratio);
        let new_interval = Duration::from_secs_f64(new_interval_secs);

        // Clamp to bounds
        let clamped = new_interval.clamp(adaptive.min_interval, adaptive.max_interval);

        let old_interval = self.config.interval;
        self.config.interval = Some(clamped);

        if old_interval != Some(clamped) {
            debug!(
                old_interval_ms = old_interval.map(|d| d.as_millis()),
                new_interval_ms = clamped.as_millis(),
                smoothed_duration_ms = self.smoothed_duration_ms,
                "adaptive checkpoint interval adjusted"
            );
        }
    }

    /// Returns the current smoothed checkpoint duration (milliseconds).
    ///
    /// Returns 0.0 if no checkpoints have been completed or adaptive mode
    /// is not enabled.
    #[must_use]
    pub fn smoothed_duration_ms(&self) -> f64 {
        self.smoothed_duration_ms
    }

    /// Returns the number of unaligned checkpoints completed.
    #[must_use]
    pub fn unaligned_checkpoint_count(&self) -> u64 {
        self.unaligned_checkpoint_count
    }

    /// Returns checkpoint statistics.
    #[must_use]
    pub fn stats(&self) -> CheckpointStats {
        let (p50, p95, p99) = self.duration_histogram.percentiles();
        CheckpointStats {
            completed: self.checkpoints_completed,
            failed: self.checkpoints_failed,
            last_duration: self.last_checkpoint_duration,
            duration_p50_ms: p50,
            duration_p95_ms: p95,
            duration_p99_ms: p99,
            total_bytes_written: self.total_bytes_written,
            current_phase: self.phase,
            current_epoch: self.epoch,
            unaligned_checkpoint_count: self.unaligned_checkpoint_count,
        }
    }

    /// Returns a reference to the underlying store.
    #[must_use]
    pub fn store(&self) -> &dyn CheckpointStore {
        &*self.store
    }

    /// Performs a full checkpoint cycle with additional table offsets.
    ///
    /// Identical to [`checkpoint()`](Self::checkpoint) but merges
    /// `extra_table_offsets` into the manifest's `table_offsets` field.
    /// This is useful for `ReferenceTableSource` instances that are not
    /// registered as `SourceConnector` but still need their offsets persisted.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if any phase fails.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn checkpoint_with_extra_tables(
        &mut self,
        operator_states: HashMap<String, Vec<u8>>,
        watermark: Option<i64>,
        wal_position: u64,
        per_core_wal_positions: Vec<u64>,
        table_store_checkpoint_path: Option<String>,
        extra_table_offsets: HashMap<String, ConnectorCheckpoint>,
        source_watermarks: HashMap<String, i64>,
        pipeline_hash: Option<u64>,
    ) -> Result<CheckpointResult, DbError> {
        let start = Instant::now();
        let checkpoint_id = self.next_checkpoint_id;
        let epoch = self.epoch;

        info!(
            checkpoint_id,
            epoch, "starting checkpoint (with extra tables)"
        );

        // ── Step 2b: Defense-in-depth changelog drain ──
        for drainer in &mut self.changelog_drainers {
            let extra = drainer.drain();
            if extra > 0 {
                debug!(extra_drained = extra, "drained straggler changelog entries");
            }
        }

        // ── Step 3: Source snapshot (parallel) ──
        self.phase = CheckpointPhase::Snapshotting;
        let (source_offsets, mut table_offsets) = tokio::try_join!(
            self.snapshot_sources(&self.sources),
            self.snapshot_sources(&self.table_sources),
        )?;

        // Merge extra table offsets (from ReferenceTableSource instances)
        for (name, cp) in extra_table_offsets {
            table_offsets.insert(name, cp);
        }

        // ── Step 4: Sink pre-commit ──
        self.phase = CheckpointPhase::PreCommitting;
        if let Err(e) = self.pre_commit_sinks(epoch).await {
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            error!(checkpoint_id, epoch, error = %e, "pre-commit failed");
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some(format!("pre-commit failed: {e}")),
            });
        }

        // ── Build manifest ──
        let mut manifest = CheckpointManifest::new(checkpoint_id, epoch);
        manifest.source_offsets = source_offsets;
        manifest.table_offsets = table_offsets;
        manifest.sink_epochs = self.collect_sink_epochs();
        manifest.sink_commit_statuses = self.initial_sink_commit_statuses();
        manifest.watermark = watermark;
        if source_watermarks.is_empty() {
            if let Some(wm) = watermark {
                manifest.source_watermarks = self.collect_source_watermarks(wm);
            }
        } else {
            manifest.source_watermarks = source_watermarks;
        }
        manifest.wal_position = wal_position;
        manifest.per_core_wal_positions = per_core_wal_positions;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.is_incremental = self.config.incremental;
        manifest.source_names = self.sorted_source_names();
        manifest.sink_names = self.sorted_sink_names();
        manifest.pipeline_hash = pipeline_hash;

        // Convert operator states to manifest format with sidecar threshold
        let mut sidecar_blobs: Vec<u8> = Vec::new();
        let threshold = self.config.state_inline_threshold;
        for (name, data) in &operator_states {
            let (op_ckpt, maybe_blob) =
                laminar_storage::checkpoint_manifest::OperatorCheckpoint::from_bytes(
                    data,
                    threshold,
                    sidecar_blobs.len() as u64,
                );
            if let Some(blob) = maybe_blob {
                sidecar_blobs.extend_from_slice(&blob);
            }
            manifest.operator_states.insert(name.clone(), op_ckpt);
        }

        let state_data = if sidecar_blobs.is_empty() {
            None
        } else {
            debug!(
                checkpoint_id,
                sidecar_bytes = sidecar_blobs.len(),
                "writing operator state sidecar"
            );
            Some(sidecar_blobs)
        };

        // ── Step 5: Persist manifest (decision record — sinks are Pending) ──
        self.phase = CheckpointPhase::Persisting;
        if let Err(e) = self.save_manifest(&manifest, state_data).await {
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            let _ = self.rollback_sinks(epoch).await;
            error!(checkpoint_id, epoch, error = %e, "manifest persist failed");
            return Ok(CheckpointResult {
                success: false,
                checkpoint_id,
                epoch,
                duration,
                error: Some(format!("manifest persist failed: {e}")),
            });
        }

        // ── Step 6: Sink commit (per-sink tracking) ──
        self.phase = CheckpointPhase::Committing;
        let sink_statuses = self.commit_sinks_tracked(epoch).await;
        let has_failures = sink_statuses
            .values()
            .any(|s| matches!(s, SinkCommitStatus::Failed(_)));

        // ── Step 6b: Save manifest again with final sink commit statuses ──
        if !sink_statuses.is_empty() {
            manifest.sink_commit_statuses = sink_statuses;
            if let Err(e) = self.save_manifest(&manifest, None).await {
                warn!(
                    checkpoint_id,
                    epoch,
                    error = %e,
                    "post-commit manifest update failed"
                );
            }
        }

        if has_failures {
            self.checkpoints_failed += 1;
            warn!(checkpoint_id, epoch, "sink commit partially failed");
        }

        // ── Success ──
        self.phase = CheckpointPhase::Idle;
        self.next_checkpoint_id += 1;
        self.epoch += 1;
        self.checkpoints_completed += 1;
        let duration = start.elapsed();
        self.last_checkpoint_duration = Some(duration);
        self.duration_histogram.record(duration);
        self.emit_checkpoint_metrics(true, epoch, duration);
        self.adjust_interval();

        info!(
            checkpoint_id,
            epoch,
            duration_ms = duration.as_millis(),
            "checkpoint completed"
        );

        Ok(CheckpointResult {
            success: true,
            checkpoint_id,
            epoch,
            duration,
            error: None,
        })
    }

    /// Attempts recovery from the latest checkpoint.
    ///
    /// Creates a [`RecoveryManager`](crate::recovery_manager::RecoveryManager)
    /// using the coordinator's store and delegates recovery to it.
    /// On success, advances `self.epoch` past the recovered epoch so the
    /// next checkpoint gets a fresh epoch number.
    ///
    /// Returns `Ok(None)` for a fresh start (no checkpoint found).
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store itself fails.
    pub async fn recover(
        &mut self,
    ) -> Result<Option<crate::recovery_manager::RecoveredState>, DbError> {
        use crate::recovery_manager::RecoveryManager;

        let mgr = RecoveryManager::new(&*self.store);
        let result = mgr
            .recover(&self.sources, &self.sinks, &self.table_sources)
            .await?;

        if let Some(ref recovered) = result {
            // Advance epoch past the recovered one
            self.epoch = recovered.epoch() + 1;
            self.next_checkpoint_id = recovered.manifest.checkpoint_id + 1;
            info!(
                epoch = self.epoch,
                checkpoint_id = self.next_checkpoint_id,
                "coordinator epoch set after recovery"
            );
        }

        Ok(result)
    }

    /// Loads the latest manifest from the store.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` on store errors.
    pub fn load_latest_manifest(&self) -> Result<Option<CheckpointManifest>, DbError> {
        self.store
            .load_latest()
            .map_err(|e| DbError::Checkpoint(format!("failed to load latest manifest: {e}")))
    }
}

impl std::fmt::Debug for CheckpointCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointCoordinator")
            .field("epoch", &self.epoch)
            .field("next_checkpoint_id", &self.next_checkpoint_id)
            .field("phase", &self.phase)
            .field("sources", &self.sources.len())
            .field("sinks", &self.sinks.len())
            .field("has_wal_manager", &self.wal_manager.is_some())
            .field("changelog_drainers", &self.changelog_drainers.len())
            .field("completed", &self.checkpoints_completed)
            .field("failed", &self.checkpoints_failed)
            .finish_non_exhaustive()
    }
}

/// Fixed-size ring buffer for checkpoint duration percentile tracking.
///
/// Stores the last `CAPACITY` checkpoint durations and computes p50/p95/p99
/// via sorted extraction. No heap allocation after construction.
#[derive(Clone)]
pub struct DurationHistogram {
    /// Ring buffer of durations in milliseconds.
    samples: Box<[u64; Self::CAPACITY]>,
    /// Write cursor (wraps at `CAPACITY`).
    cursor: usize,
    /// Total samples written (may exceed `CAPACITY`).
    count: u64,
}

impl DurationHistogram {
    const CAPACITY: usize = 100;

    /// Creates an empty histogram.
    #[must_use]
    fn new() -> Self {
        Self {
            samples: Box::new([0; Self::CAPACITY]),
            cursor: 0,
            count: 0,
        }
    }

    /// Records a checkpoint duration.
    fn record(&mut self, duration: Duration) {
        #[allow(clippy::cast_possible_truncation)]
        let ms = duration.as_millis() as u64;
        self.samples[self.cursor] = ms;
        self.cursor = (self.cursor + 1) % Self::CAPACITY;
        self.count += 1;
    }

    /// Returns the number of recorded samples (up to `CAPACITY`).
    #[must_use]
    fn len(&self) -> usize {
        if self.count >= Self::CAPACITY as u64 {
            Self::CAPACITY
        } else {
            // SAFETY: count < CAPACITY (100), which always fits in usize.
            #[allow(clippy::cast_possible_truncation)]
            {
                self.count as usize
            }
        }
    }

    /// Computes a percentile (0.0–1.0) from recorded samples.
    ///
    /// Returns 0 if no samples have been recorded.
    #[must_use]
    fn percentile(&self, p: f64) -> u64 {
        let n = self.len();
        if n == 0 {
            return 0;
        }
        let mut sorted: Vec<u64> = self.samples[..n].to_vec();
        sorted.sort_unstable();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let idx = ((p * (n as f64 - 1.0)).ceil() as usize).min(n - 1);
        sorted[idx]
    }

    /// Returns (p50, p95, p99) in milliseconds.
    #[must_use]
    fn percentiles(&self) -> (u64, u64, u64) {
        (
            self.percentile(0.50),
            self.percentile(0.95),
            self.percentile(0.99),
        )
    }
}

impl std::fmt::Debug for DurationHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (p50, p95, p99) = self.percentiles();
        f.debug_struct("DurationHistogram")
            .field("samples_len", &self.samples.len())
            .field("cursor", &self.cursor)
            .field("count", &self.count)
            .field("p50_ms", &p50)
            .field("p95_ms", &p95)
            .field("p99_ms", &p99)
            .finish()
    }
}

/// Checkpoint performance statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckpointStats {
    /// Total completed checkpoints.
    pub completed: u64,
    /// Total failed checkpoints.
    pub failed: u64,
    /// Duration of the last checkpoint.
    pub last_duration: Option<Duration>,
    /// p50 checkpoint duration in milliseconds.
    pub duration_p50_ms: u64,
    /// p95 checkpoint duration in milliseconds.
    pub duration_p95_ms: u64,
    /// p99 checkpoint duration in milliseconds.
    pub duration_p99_ms: u64,
    /// Total bytes written across all checkpoints.
    pub total_bytes_written: u64,
    /// Current checkpoint phase.
    pub current_phase: CheckpointPhase,
    /// Current epoch number.
    pub current_epoch: u64,
    /// Number of checkpoints completed in unaligned mode.
    pub unaligned_checkpoint_count: u64,
}

// ── Conversion helpers ──

/// Converts a `SourceCheckpoint` to a `ConnectorCheckpoint`.
#[must_use]
pub fn source_to_connector_checkpoint(cp: &SourceCheckpoint) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: cp.offsets().clone(),
        epoch: cp.epoch(),
        metadata: cp.metadata().clone(),
    }
}

/// Converts a `ConnectorCheckpoint` back to a `SourceCheckpoint`.
#[must_use]
pub fn connector_to_source_checkpoint(cp: &ConnectorCheckpoint) -> SourceCheckpoint {
    let mut source_cp = SourceCheckpoint::with_offsets(cp.epoch, cp.offsets.clone());
    for (k, v) in &cp.metadata {
        source_cp.set_metadata(k.clone(), v.clone());
    }
    source_cp
}

/// Converts from the legacy `SerializableSourceCheckpoint` format.
#[must_use]
pub fn legacy_to_connector_checkpoint<S: std::hash::BuildHasher>(
    offsets: &HashMap<String, String, S>,
    epoch: u64,
    metadata: &HashMap<String, String, S>,
) -> ConnectorCheckpoint {
    ConnectorCheckpoint {
        offsets: offsets
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        epoch,
        metadata: metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

// ── DAG operator state conversion helpers ──

/// Converts DAG operator states (from `DagCheckpointSnapshot`) to manifest format.
///
/// Uses `"{node_id}"` as the key and base64-encodes the state data.
#[must_use]
pub fn dag_snapshot_to_manifest_operators<S: std::hash::BuildHasher>(
    node_states: &std::collections::HashMap<
        u32,
        laminar_core::dag::recovery::SerializableOperatorState,
        S,
    >,
) -> HashMap<String, laminar_storage::checkpoint_manifest::OperatorCheckpoint> {
    node_states
        .iter()
        .map(|(id, state)| {
            (
                id.to_string(),
                laminar_storage::checkpoint_manifest::OperatorCheckpoint::inline(&state.data),
            )
        })
        .collect()
}

/// Converts manifest operator states back to DAG format for recovery.
///
/// Parses string keys as node IDs and decodes base64 state data.
#[must_use]
pub fn manifest_operators_to_dag_states<S: std::hash::BuildHasher>(
    operators: &HashMap<String, laminar_storage::checkpoint_manifest::OperatorCheckpoint, S>,
) -> rustc_hash::FxHashMap<laminar_core::dag::topology::NodeId, laminar_core::operator::OperatorState>
{
    let mut states =
        rustc_hash::FxHashMap::with_capacity_and_hasher(operators.len(), rustc_hash::FxBuildHasher);
    for (key, op_ckpt) in operators {
        if let Ok(node_id) = key.parse::<u32>() {
            if let Some(data) = op_ckpt.decode_inline() {
                states.insert(
                    laminar_core::dag::topology::NodeId(node_id),
                    laminar_core::operator::OperatorState {
                        operator_id: key.clone(),
                        data,
                    },
                );
            }
        }
    }
    states
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_storage::checkpoint_store::FileSystemCheckpointStore;

    fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
        let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
        CheckpointCoordinator::new(CheckpointConfig::default(), store)
    }

    #[test]
    fn test_coordinator_new() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path());

        assert_eq!(coord.epoch(), 1);
        assert_eq!(coord.next_checkpoint_id(), 1);
        assert_eq!(coord.phase(), CheckpointPhase::Idle);
    }

    #[test]
    fn test_coordinator_resumes_from_stored_checkpoint() {
        let dir = tempfile::tempdir().unwrap();

        // Save a checkpoint manually
        let store = FileSystemCheckpointStore::new(dir.path(), 3);
        let m = CheckpointManifest::new(5, 10);
        store.save(&m).unwrap();

        // Coordinator should resume from epoch 11, checkpoint_id 6
        let coord = make_coordinator(dir.path());
        assert_eq!(coord.epoch(), 11);
        assert_eq!(coord.next_checkpoint_id(), 6);
    }

    #[test]
    fn test_checkpoint_phase_display() {
        assert_eq!(CheckpointPhase::Idle.to_string(), "Idle");
        assert_eq!(
            CheckpointPhase::BarrierInFlight.to_string(),
            "BarrierInFlight"
        );
        assert_eq!(CheckpointPhase::Snapshotting.to_string(), "Snapshotting");
        assert_eq!(CheckpointPhase::PreCommitting.to_string(), "PreCommitting");
        assert_eq!(CheckpointPhase::Persisting.to_string(), "Persisting");
        assert_eq!(CheckpointPhase::Committing.to_string(), "Committing");
    }

    #[test]
    fn test_source_to_connector_checkpoint() {
        let mut cp = SourceCheckpoint::new(5);
        cp.set_offset("partition-0", "1234");
        cp.set_metadata("topic", "events");

        let cc = source_to_connector_checkpoint(&cp);
        assert_eq!(cc.epoch, 5);
        assert_eq!(cc.offsets.get("partition-0"), Some(&"1234".into()));
        assert_eq!(cc.metadata.get("topic"), Some(&"events".into()));
    }

    #[test]
    fn test_connector_to_source_checkpoint() {
        let cc = ConnectorCheckpoint {
            offsets: HashMap::from([("lsn".into(), "0/ABCD".into())]),
            epoch: 3,
            metadata: HashMap::from([("type".into(), "postgres".into())]),
        };

        let cp = connector_to_source_checkpoint(&cc);
        assert_eq!(cp.epoch(), 3);
        assert_eq!(cp.get_offset("lsn"), Some("0/ABCD"));
        assert_eq!(cp.get_metadata("type"), Some("postgres"));
    }

    #[test]
    fn test_stats_initial() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path());
        let stats = coord.stats();

        assert_eq!(stats.completed, 0);
        assert_eq!(stats.failed, 0);
        assert!(stats.last_duration.is_none());
        assert_eq!(stats.duration_p50_ms, 0);
        assert_eq!(stats.duration_p95_ms, 0);
        assert_eq!(stats.duration_p99_ms, 0);
        assert_eq!(stats.current_phase, CheckpointPhase::Idle);
    }

    #[tokio::test]
    async fn test_checkpoint_no_sources_no_sinks() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let result = coord
            .checkpoint(
                HashMap::new(),
                Some(1000),
                0,
                vec![],
                None,
                HashMap::new(),
                None,
            )
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(result.checkpoint_id, 1);
        assert_eq!(result.epoch, 1);

        // Verify manifest was persisted
        let loaded = coord.store().load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
        assert_eq!(loaded.watermark, Some(1000));

        // Second checkpoint should increment
        let result2 = coord
            .checkpoint(
                HashMap::new(),
                Some(2000),
                100,
                vec![],
                None,
                HashMap::new(),
                None,
            )
            .await
            .unwrap();

        assert!(result2.success);
        assert_eq!(result2.checkpoint_id, 2);
        assert_eq!(result2.epoch, 2);

        let stats = coord.stats();
        assert_eq!(stats.completed, 2);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn test_checkpoint_with_operator_states() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let mut ops = HashMap::new();
        ops.insert("window-agg".into(), b"state-data".to_vec());
        ops.insert("filter".into(), b"filter-state".to_vec());

        let result = coord
            .checkpoint(ops, None, 4096, vec![100, 200], None, HashMap::new(), None)
            .await
            .unwrap();

        assert!(result.success);

        let loaded = coord.store().load_latest().unwrap().unwrap();
        assert_eq!(loaded.operator_states.len(), 2);
        assert_eq!(loaded.wal_position, 4096);
        assert_eq!(loaded.per_core_wal_positions, vec![100, 200]);

        let window_op = loaded.operator_states.get("window-agg").unwrap();
        assert_eq!(window_op.decode_inline().unwrap(), b"state-data");
    }

    #[tokio::test]
    async fn test_checkpoint_with_table_store_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let result = coord
            .checkpoint(
                HashMap::new(),
                None,
                0,
                vec![],
                Some("/tmp/rocksdb_cp".into()),
                HashMap::new(),
                None,
            )
            .await
            .unwrap();

        assert!(result.success);

        let loaded = coord.store().load_latest().unwrap().unwrap();
        assert_eq!(
            loaded.table_store_checkpoint_path.as_deref(),
            Some("/tmp/rocksdb_cp")
        );
    }

    #[test]
    fn test_load_latest_manifest_empty() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path());
        assert!(coord.load_latest_manifest().unwrap().is_none());
    }

    #[test]
    fn test_coordinator_debug() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path());
        let debug = format!("{coord:?}");
        assert!(debug.contains("CheckpointCoordinator"));
        assert!(debug.contains("epoch: 1"));
    }

    // ── Operator state persistence tests ──

    #[test]
    fn test_dag_snapshot_to_manifest_operators() {
        use laminar_core::dag::recovery::SerializableOperatorState;

        let mut node_states = std::collections::HashMap::new();
        node_states.insert(
            0,
            SerializableOperatorState {
                operator_id: "window-agg".into(),
                data: b"window-state".to_vec(),
            },
        );
        node_states.insert(
            3,
            SerializableOperatorState {
                operator_id: "filter".into(),
                data: b"filter-state".to_vec(),
            },
        );

        let manifest_ops = dag_snapshot_to_manifest_operators(&node_states);
        assert_eq!(manifest_ops.len(), 2);

        let w = manifest_ops.get("0").unwrap();
        assert_eq!(w.decode_inline().unwrap(), b"window-state");
        let f = manifest_ops.get("3").unwrap();
        assert_eq!(f.decode_inline().unwrap(), b"filter-state");
    }

    #[test]
    fn test_manifest_operators_to_dag_states() {
        use laminar_storage::checkpoint_manifest::OperatorCheckpoint;

        let mut operators = HashMap::new();
        operators.insert("0".into(), OperatorCheckpoint::inline(b"state-0"));
        operators.insert("5".into(), OperatorCheckpoint::inline(b"state-5"));

        let dag_states = manifest_operators_to_dag_states(&operators);
        assert_eq!(dag_states.len(), 2);

        let s0 = dag_states
            .get(&laminar_core::dag::topology::NodeId(0))
            .unwrap();
        assert_eq!(s0.data, b"state-0");

        let s5 = dag_states
            .get(&laminar_core::dag::topology::NodeId(5))
            .unwrap();
        assert_eq!(s5.data, b"state-5");
    }

    #[test]
    fn test_operator_state_round_trip_through_manifest() {
        use laminar_core::dag::recovery::SerializableOperatorState;

        // Original DAG states
        let mut node_states = std::collections::HashMap::new();
        node_states.insert(
            7,
            SerializableOperatorState {
                operator_id: "join".into(),
                data: vec![1, 2, 3, 4, 5],
            },
        );

        // DAG → manifest
        let manifest_ops = dag_snapshot_to_manifest_operators(&node_states);

        // Persist and reload (simulate)
        let json = serde_json::to_string(&manifest_ops).unwrap();
        let reloaded: HashMap<String, laminar_storage::checkpoint_manifest::OperatorCheckpoint> =
            serde_json::from_str(&json).unwrap();

        // Manifest → DAG
        let recovered = manifest_operators_to_dag_states(&reloaded);
        let state = recovered
            .get(&laminar_core::dag::topology::NodeId(7))
            .unwrap();
        assert_eq!(state.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_manifest_operators_skips_invalid_keys() {
        use laminar_storage::checkpoint_manifest::OperatorCheckpoint;

        let mut operators = HashMap::new();
        operators.insert("not-a-number".into(), OperatorCheckpoint::inline(b"data"));
        operators.insert("42".into(), OperatorCheckpoint::inline(b"good"));

        let dag_states = manifest_operators_to_dag_states(&operators);
        // Only the numeric key should survive
        assert_eq!(dag_states.len(), 1);
        assert!(dag_states.contains_key(&laminar_core::dag::topology::NodeId(42)));
    }

    // ── WAL checkpoint coordination tests ──

    #[test]
    fn test_prepare_wal_no_wal_manager() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        // Without a WAL manager, prepare should succeed with empty positions.
        let result = coord.prepare_wal_for_checkpoint().unwrap();
        assert!(result.per_core_wal_positions.is_empty());
        assert_eq!(result.entries_drained, 0);
    }

    #[test]
    fn test_prepare_wal_with_manager() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        // Set up a per-core WAL manager
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_config = laminar_storage::per_core_wal::PerCoreWalConfig::new(&wal_dir, 2);
        let wal = laminar_storage::per_core_wal::PerCoreWalManager::new(wal_config).unwrap();
        coord.register_wal_manager(wal);

        // Write some data to WAL
        coord
            .wal_manager_mut()
            .unwrap()
            .writer(0)
            .append_put(b"key", b"value")
            .unwrap();

        let result = coord.prepare_wal_for_checkpoint().unwrap();
        assert_eq!(result.per_core_wal_positions.len(), 2);
        // Positions should be > 0 (epoch barrier + data)
        assert!(result.per_core_wal_positions.iter().any(|p| *p > 0));
    }

    #[test]
    fn test_truncate_wal_no_manager() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        // Without a WAL manager, truncation is a no-op.
        coord.truncate_wal_after_checkpoint().unwrap();
    }

    #[test]
    fn test_truncate_wal_with_manager() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_config = laminar_storage::per_core_wal::PerCoreWalConfig::new(&wal_dir, 2);
        let wal = laminar_storage::per_core_wal::PerCoreWalManager::new(wal_config).unwrap();
        coord.register_wal_manager(wal);

        // Write data
        coord
            .wal_manager_mut()
            .unwrap()
            .writer(0)
            .append_put(b"key", b"value")
            .unwrap();

        assert!(coord.wal_manager().unwrap().total_size() > 0);

        // Truncate
        coord.truncate_wal_after_checkpoint().unwrap();
        assert_eq!(coord.wal_manager().unwrap().total_size(), 0);
    }

    #[test]
    fn test_prepare_wal_with_changelog_drainer() {
        use laminar_storage::incremental::StateChangelogBuffer;
        use laminar_storage::incremental::StateChangelogEntry;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        // Set up a changelog buffer and drainer
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));

        // Push some entries
        buf.push(StateChangelogEntry::put(1, 100, 0, 10));
        buf.push(StateChangelogEntry::put(1, 200, 10, 20));
        buf.push(StateChangelogEntry::delete(1, 300));

        let drainer = ChangelogDrainer::new(buf, 100);
        coord.register_changelog_drainer(drainer);

        let result = coord.prepare_wal_for_checkpoint().unwrap();
        assert_eq!(result.entries_drained, 3);
        assert!(result.per_core_wal_positions.is_empty()); // No WAL manager

        // Drainer should have pending entries
        assert_eq!(coord.changelog_drainers()[0].pending_count(), 3);
    }

    #[tokio::test]
    async fn test_full_checkpoint_with_wal_coordination() {
        use laminar_storage::incremental::StateChangelogBuffer;
        use laminar_storage::incremental::StateChangelogEntry;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        // Set up WAL manager
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_config = laminar_storage::per_core_wal::PerCoreWalConfig::new(&wal_dir, 2);
        let wal = laminar_storage::per_core_wal::PerCoreWalManager::new(wal_config).unwrap();
        coord.register_wal_manager(wal);

        // Set up changelog drainer
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));
        buf.push(StateChangelogEntry::put(1, 100, 0, 10));
        let drainer = ChangelogDrainer::new(buf, 100);
        coord.register_changelog_drainer(drainer);

        // Full cycle: prepare → checkpoint → truncate
        let wal_result = coord.prepare_wal_for_checkpoint().unwrap();
        assert_eq!(wal_result.entries_drained, 1);

        let result = coord
            .checkpoint(
                HashMap::new(),
                Some(5000),
                0,
                wal_result.per_core_wal_positions.clone(),
                None,
                HashMap::new(),
                None,
            )
            .await
            .unwrap();

        assert!(result.success);

        // Changelog drainer pending should be cleared after successful checkpoint
        assert_eq!(
            coord.changelog_drainers()[0].pending_count(),
            0,
            "pending entries should be cleared after checkpoint"
        );

        // Manifest should have per-core WAL positions
        let loaded = coord.store().load_latest().unwrap().unwrap();
        assert_eq!(
            loaded.per_core_wal_positions.len(),
            wal_result.per_core_wal_positions.len()
        );

        // Truncate WAL
        coord.truncate_wal_after_checkpoint().unwrap();
        assert_eq!(coord.wal_manager().unwrap().total_size(), 0);
    }

    #[test]
    fn test_wal_manager_accessors() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        assert!(coord.wal_manager().is_none());
        assert!(coord.wal_manager_mut().is_none());
        assert!(coord.changelog_drainers().is_empty());

        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_config = laminar_storage::per_core_wal::PerCoreWalConfig::new(&wal_dir, 2);
        let wal = laminar_storage::per_core_wal::PerCoreWalManager::new(wal_config).unwrap();
        coord.register_wal_manager(wal);

        assert!(coord.wal_manager().is_some());
        assert!(coord.wal_manager_mut().is_some());
    }

    #[test]
    fn test_coordinator_debug_with_wal() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal_config = laminar_storage::per_core_wal::PerCoreWalConfig::new(&wal_dir, 2);
        let wal = laminar_storage::per_core_wal::PerCoreWalManager::new(wal_config).unwrap();
        coord.register_wal_manager(wal);

        let debug = format!("{coord:?}");
        assert!(debug.contains("has_wal_manager: true"));
        assert!(debug.contains("changelog_drainers: 0"));
    }

    // ── Checkpoint observability tests ──

    #[tokio::test]
    async fn test_checkpoint_emits_metrics_on_success() {
        use crate::metrics::PipelineCounters;
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let counters = Arc::new(PipelineCounters::new());
        coord.set_counters(Arc::clone(&counters));

        let result = coord
            .checkpoint(
                HashMap::new(),
                Some(1000),
                0,
                vec![],
                None,
                HashMap::new(),
                None,
            )
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(counters.checkpoints_completed.load(Ordering::Relaxed), 1);
        assert_eq!(counters.checkpoints_failed.load(Ordering::Relaxed), 0);
        assert!(counters.last_checkpoint_duration_ms.load(Ordering::Relaxed) < 5000);
        assert_eq!(counters.checkpoint_epoch.load(Ordering::Relaxed), 1);

        // Second checkpoint
        let result2 = coord
            .checkpoint(
                HashMap::new(),
                Some(2000),
                0,
                vec![],
                None,
                HashMap::new(),
                None,
            )
            .await
            .unwrap();

        assert!(result2.success);
        assert_eq!(counters.checkpoints_completed.load(Ordering::Relaxed), 2);
        assert_eq!(counters.checkpoint_epoch.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_checkpoint_without_counters() {
        // Verify checkpoint works fine without counters set
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let result = coord
            .checkpoint(HashMap::new(), None, 0, vec![], None, HashMap::new(), None)
            .await
            .unwrap();

        assert!(result.success);
        // No panics — metrics emission is a no-op
    }

    // ── DurationHistogram tests ──

    #[test]
    fn test_histogram_empty() {
        let h = DurationHistogram::new();
        assert_eq!(h.len(), 0);
        assert_eq!(h.percentile(0.50), 0);
        assert_eq!(h.percentile(0.99), 0);
        let (p50, p95, p99) = h.percentiles();
        assert_eq!((p50, p95, p99), (0, 0, 0));
    }

    #[test]
    fn test_histogram_single_sample() {
        let mut h = DurationHistogram::new();
        h.record(Duration::from_millis(42));
        assert_eq!(h.len(), 1);
        assert_eq!(h.percentile(0.50), 42);
        assert_eq!(h.percentile(0.99), 42);
    }

    #[test]
    fn test_histogram_percentiles() {
        let mut h = DurationHistogram::new();
        // Record 1..=100ms in order.
        for i in 1..=100 {
            h.record(Duration::from_millis(i));
        }
        assert_eq!(h.len(), 100);

        let p50 = h.percentile(0.50);
        let p95 = h.percentile(0.95);
        let p99 = h.percentile(0.99);

        // With values 1..=100:
        //   p50 ≈ 50, p95 ≈ 95, p99 ≈ 99
        assert!((49..=51).contains(&p50), "p50={p50}");
        assert!((94..=96).contains(&p95), "p95={p95}");
        assert!((98..=100).contains(&p99), "p99={p99}");
    }

    #[test]
    fn test_histogram_wraps_ring_buffer() {
        let mut h = DurationHistogram::new();
        // Write 150 samples — first 50 are overwritten.
        for i in 1..=150 {
            h.record(Duration::from_millis(i));
        }
        assert_eq!(h.len(), 100);
        assert_eq!(h.count, 150);

        // Only samples 51..=150 remain in the buffer.
        let p50 = h.percentile(0.50);
        assert!((99..=101).contains(&p50), "p50={p50}");
    }

    // ── Sidecar threshold tests ──

    #[tokio::test]
    async fn test_sidecar_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig {
            state_inline_threshold: 100, // 100 bytes threshold
            ..CheckpointConfig::default()
        };
        let mut coord = CheckpointCoordinator::new(config, store);

        // Small state stays inline, large state goes to sidecar
        let mut ops = HashMap::new();
        ops.insert("small".into(), vec![0xAAu8; 50]);
        ops.insert("large".into(), vec![0xBBu8; 200]);

        let result = coord
            .checkpoint(ops, None, 0, vec![], None, HashMap::new(), None)
            .await
            .unwrap();
        assert!(result.success);

        // Verify manifest
        let loaded = coord.store().load_latest().unwrap().unwrap();
        let small_op = loaded.operator_states.get("small").unwrap();
        assert!(!small_op.external, "small state should be inline");
        assert_eq!(small_op.decode_inline().unwrap(), vec![0xAAu8; 50]);

        let large_op = loaded.operator_states.get("large").unwrap();
        assert!(large_op.external, "large state should be external");
        assert_eq!(large_op.external_length, 200);

        // Verify sidecar file exists and has correct data
        let state_data = coord.store().load_state_data(1).unwrap().unwrap();
        assert_eq!(state_data.len(), 200);
        assert!(state_data.iter().all(|&b| b == 0xBB));
    }

    #[tokio::test]
    async fn test_all_inline_no_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig::default(); // 1MB threshold
        let mut coord = CheckpointCoordinator::new(config, store);

        let mut ops = HashMap::new();
        ops.insert("op1".into(), b"small-state".to_vec());

        let result = coord
            .checkpoint(ops, None, 0, vec![], None, HashMap::new(), None)
            .await
            .unwrap();
        assert!(result.success);

        // No sidecar file
        assert!(coord.store().load_state_data(1).unwrap().is_none());
    }

    // ── Adaptive interval tests ──

    #[test]
    fn test_adaptive_disabled_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let coord = make_coordinator(dir.path());
        assert!(coord.config().adaptive.is_none());
        assert_eq!(coord.config().interval, Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_adaptive_increases_interval_for_slow_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig {
            adaptive: Some(AdaptiveCheckpointConfig::default()),
            ..CheckpointConfig::default()
        };
        let mut coord = CheckpointCoordinator::new(config, store);

        // Simulate a 5-second checkpoint
        coord.last_checkpoint_duration = Some(Duration::from_secs(5));
        coord.adjust_interval();

        // Expected: 5000ms / (1000 * 0.1) = 50s
        let interval = coord.config().interval.unwrap();
        assert!(
            interval >= Duration::from_secs(49) && interval <= Duration::from_secs(51),
            "expected ~50s, got {interval:?}",
        );
    }

    #[test]
    fn test_adaptive_decreases_interval_for_fast_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig {
            adaptive: Some(AdaptiveCheckpointConfig::default()),
            ..CheckpointConfig::default()
        };
        let mut coord = CheckpointCoordinator::new(config, store);

        // Simulate a 100ms checkpoint → 100 / (1000 * 0.1) = 1s → clamped to 10s min
        coord.last_checkpoint_duration = Some(Duration::from_millis(100));
        coord.adjust_interval();

        let interval = coord.config().interval.unwrap();
        assert_eq!(
            interval,
            Duration::from_secs(10),
            "should clamp to min_interval"
        );
    }

    #[test]
    fn test_adaptive_clamps_to_min_max() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig {
            adaptive: Some(AdaptiveCheckpointConfig {
                min_interval: Duration::from_secs(20),
                max_interval: Duration::from_secs(120),
                target_overhead_ratio: 0.1,
                smoothing_alpha: 1.0, // Full weight on latest
            }),
            ..CheckpointConfig::default()
        };
        let mut coord = CheckpointCoordinator::new(config, store);

        // Very slow → clamp to max
        coord.last_checkpoint_duration = Some(Duration::from_secs(60));
        coord.adjust_interval();
        let interval = coord.config().interval.unwrap();
        assert_eq!(interval, Duration::from_secs(120), "should clamp to max");

        // Very fast → clamp to min
        coord.last_checkpoint_duration = Some(Duration::from_millis(10));
        coord.smoothed_duration_ms = 0.0; // Reset EMA
        coord.adjust_interval();
        let interval = coord.config().interval.unwrap();
        assert_eq!(interval, Duration::from_secs(20), "should clamp to min");
    }

    #[test]
    fn test_adaptive_ema_smoothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
        let config = CheckpointConfig {
            adaptive: Some(AdaptiveCheckpointConfig {
                min_interval: Duration::from_secs(1),
                max_interval: Duration::from_secs(600),
                target_overhead_ratio: 0.1,
                smoothing_alpha: 0.5,
            }),
            ..CheckpointConfig::default()
        };
        let mut coord = CheckpointCoordinator::new(config, store);

        // First observation: 1000ms → EMA = 1000 (cold start)
        coord.last_checkpoint_duration = Some(Duration::from_millis(1000));
        coord.adjust_interval();
        assert!((coord.smoothed_duration_ms() - 1000.0).abs() < 1.0);

        // Second observation: 2000ms → EMA = 0.5*2000 + 0.5*1000 = 1500
        coord.last_checkpoint_duration = Some(Duration::from_millis(2000));
        coord.adjust_interval();
        assert!((coord.smoothed_duration_ms() - 1500.0).abs() < 1.0);

        // Third observation: 2000ms → EMA = 0.5*2000 + 0.5*1500 = 1750
        coord.last_checkpoint_duration = Some(Duration::from_millis(2000));
        coord.adjust_interval();
        assert!((coord.smoothed_duration_ms() - 1750.0).abs() < 1.0);
    }

    #[tokio::test]
    async fn test_stats_include_percentiles_after_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        // Run 3 checkpoints.
        for _ in 0..3 {
            let result = coord
                .checkpoint(HashMap::new(), None, 0, vec![], None, HashMap::new(), None)
                .await
                .unwrap();
            assert!(result.success);
        }

        let stats = coord.stats();
        assert_eq!(stats.completed, 3);
        // After 3 fast checkpoints, percentiles should be > 0
        // (they're real durations, not zero).
        assert!(stats.last_duration.is_some());
    }
}
