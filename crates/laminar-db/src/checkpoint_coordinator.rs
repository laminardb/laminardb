//! Unified checkpoint coordinator (F-CKP-003).
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
use laminar_connectors::connector::{SinkConnector, SourceConnector};
use laminar_storage::changelog_drainer::ChangelogDrainer;
use laminar_storage::checkpoint_manifest::{CheckpointManifest, ConnectorCheckpoint};
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
    /// Whether to use incremental checkpoints (future use with `RocksDB`).
    pub incremental: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Some(Duration::from_secs(60)),
            max_retained: 3,
            alignment_timeout: Duration::from_secs(30),
            incremental: false,
        }
    }
}

/// Phase of the checkpoint lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug)]
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
}

/// Registered sink for checkpoint coordination.
pub(crate) struct RegisteredSink {
    /// Sink name.
    pub name: String,
    /// Sink connector handle.
    pub connector: Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>,
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
    store: Box<dyn CheckpointStore>,
    sources: Vec<RegisteredSource>,
    sinks: Vec<RegisteredSink>,
    table_sources: Vec<RegisteredSource>,
    next_checkpoint_id: u64,
    epoch: u64,
    phase: CheckpointPhase,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    last_checkpoint_duration: Option<Duration>,
    /// Per-core WAL manager for epoch barriers and truncation (F-CKP-006).
    wal_manager: Option<PerCoreWalManager>,
    /// Changelog drainers to flush before checkpointing (F-CKP-006).
    changelog_drainers: Vec<ChangelogDrainer>,
    /// Shared counters for observability (F-CKP-009).
    counters: Option<Arc<PipelineCounters>>,
}

impl CheckpointCoordinator {
    /// Creates a new checkpoint coordinator.
    #[must_use]
    pub fn new(config: CheckpointConfig, store: Box<dyn CheckpointStore>) -> Self {
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
            wal_manager: None,
            changelog_drainers: Vec::new(),
            counters: None,
        }
    }

    /// Registers a source connector for checkpoint coordination.
    pub fn register_source(
        &mut self,
        name: impl Into<String>,
        connector: Arc<tokio::sync::Mutex<Box<dyn SourceConnector>>>,
    ) {
        self.sources.push(RegisteredSource {
            name: name.into(),
            connector,
        });
    }

    /// Registers a sink connector for checkpoint coordination.
    pub fn register_sink(
        &mut self,
        name: impl Into<String>,
        connector: Arc<tokio::sync::Mutex<Box<dyn SinkConnector>>>,
        exactly_once: bool,
    ) {
        self.sinks.push(RegisteredSink {
            name: name.into(),
            connector,
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
        });
    }

    // ── F-CKP-009: Observability ──

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

    // ── F-CKP-006: WAL coordination ──

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

            let positions = wal.positions();
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
    /// * `table_store_checkpoint_path` — `RocksDB` table store checkpoint path
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if any phase fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn checkpoint(
        &mut self,
        operator_states: HashMap<String, Vec<u8>>,
        watermark: Option<i64>,
        wal_position: u64,
        per_core_wal_positions: Vec<u64>,
        table_store_checkpoint_path: Option<String>,
    ) -> Result<CheckpointResult, DbError> {
        let start = Instant::now();
        let checkpoint_id = self.next_checkpoint_id;
        let epoch = self.epoch;

        info!(checkpoint_id, epoch, "starting checkpoint");

        // ── Step 3: Source snapshot ──
        self.phase = CheckpointPhase::Snapshotting;
        let source_offsets = self.snapshot_sources(&self.sources).await?;
        let table_offsets = self.snapshot_sources(&self.table_sources).await?;

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
        manifest.watermark = watermark;
        manifest.wal_position = wal_position;
        manifest.per_core_wal_positions = per_core_wal_positions;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.is_incremental = self.config.incremental;

        // Convert operator states to manifest format
        for (name, data) in &operator_states {
            manifest.operator_states.insert(
                name.clone(),
                laminar_storage::checkpoint_manifest::OperatorCheckpoint::inline(data),
            );
        }

        // ── Step 5: Persist manifest ──
        self.phase = CheckpointPhase::Persisting;
        if let Err(e) = self.store.save(&manifest) {
            self.phase = CheckpointPhase::Idle;
            self.checkpoints_failed += 1;
            let duration = start.elapsed();
            self.emit_checkpoint_metrics(false, epoch, duration);
            // Rollback sinks
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

        // ── Step 6: Sink commit ──
        self.phase = CheckpointPhase::Committing;
        if let Err(e) = self.commit_sinks(epoch).await {
            // Manifest is already persisted, but some sinks failed to commit.
            // The recovery manager will handle rollback on restart.
            self.checkpoints_failed += 1;
            warn!(checkpoint_id, epoch, error = %e, "sink commit partially failed");
        }

        // ── Success ──
        self.phase = CheckpointPhase::Idle;
        self.next_checkpoint_id += 1;
        self.epoch += 1;
        self.checkpoints_completed += 1;
        let duration = start.elapsed();
        self.last_checkpoint_duration = Some(duration);
        self.emit_checkpoint_metrics(true, epoch, duration);

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

    /// Snapshots all registered source connectors.
    async fn snapshot_sources(
        &self,
        sources: &[RegisteredSource],
    ) -> Result<HashMap<String, ConnectorCheckpoint>, DbError> {
        let mut offsets = HashMap::new();

        for source in sources {
            let connector = source.connector.lock().await;
            let cp = connector.checkpoint();
            offsets.insert(source.name.clone(), source_to_connector_checkpoint(&cp));
            debug!(source = %source.name, epoch = cp.epoch(), "source snapshotted");
        }

        Ok(offsets)
    }

    /// Pre-commits all exactly-once sinks (phase 1).
    async fn pre_commit_sinks(&self, epoch: u64) -> Result<(), DbError> {
        for sink in &self.sinks {
            if sink.exactly_once {
                let mut connector = sink.connector.lock().await;
                connector.pre_commit(epoch).await.map_err(|e| {
                    DbError::Checkpoint(format!("sink '{}' pre-commit failed: {e}", sink.name))
                })?;
                debug!(sink = %sink.name, epoch, "sink pre-committed");
            }
        }
        Ok(())
    }

    /// Commits all exactly-once sinks (phase 2).
    async fn commit_sinks(&self, epoch: u64) -> Result<(), DbError> {
        let mut first_error: Option<DbError> = None;

        for sink in &self.sinks {
            if sink.exactly_once {
                let mut connector = sink.connector.lock().await;
                if let Err(e) = connector.commit_epoch(epoch).await {
                    let err =
                        DbError::Checkpoint(format!("sink '{}' commit failed: {e}", sink.name));
                    error!(sink = %sink.name, epoch, error = %e, "sink commit failed");
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                } else {
                    debug!(sink = %sink.name, epoch, "sink committed");
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Rolls back all exactly-once sinks.
    async fn rollback_sinks(&self, epoch: u64) -> Result<(), DbError> {
        for sink in &self.sinks {
            if sink.exactly_once {
                let mut connector = sink.connector.lock().await;
                if let Err(e) = connector.rollback_epoch(epoch).await {
                    warn!(sink = %sink.name, epoch, error = %e, "sink rollback failed");
                }
            }
        }
        Ok(())
    }

    /// Collects the last committed epoch from each sink.
    fn collect_sink_epochs(&self) -> HashMap<String, u64> {
        let mut epochs = HashMap::new();
        for sink in &self.sinks {
            // The epoch being committed is the current one
            if sink.exactly_once {
                epochs.insert(sink.name.clone(), self.epoch);
            }
        }
        epochs
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

    /// Returns checkpoint statistics.
    #[must_use]
    pub fn stats(&self) -> CheckpointStats {
        CheckpointStats {
            completed: self.checkpoints_completed,
            failed: self.checkpoints_failed,
            last_duration: self.last_checkpoint_duration,
            current_phase: self.phase,
            current_epoch: self.epoch,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn checkpoint_with_extra_tables(
        &mut self,
        operator_states: HashMap<String, Vec<u8>>,
        watermark: Option<i64>,
        wal_position: u64,
        per_core_wal_positions: Vec<u64>,
        table_store_checkpoint_path: Option<String>,
        extra_table_offsets: HashMap<String, ConnectorCheckpoint>,
    ) -> Result<CheckpointResult, DbError> {
        let start = Instant::now();
        let checkpoint_id = self.next_checkpoint_id;
        let epoch = self.epoch;

        info!(
            checkpoint_id,
            epoch, "starting checkpoint (with extra tables)"
        );

        // ── Step 3: Source snapshot ──
        self.phase = CheckpointPhase::Snapshotting;
        let source_offsets = self.snapshot_sources(&self.sources).await?;
        let mut table_offsets = self.snapshot_sources(&self.table_sources).await?;

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
        manifest.watermark = watermark;
        manifest.wal_position = wal_position;
        manifest.per_core_wal_positions = per_core_wal_positions;
        manifest.table_store_checkpoint_path = table_store_checkpoint_path;
        manifest.is_incremental = self.config.incremental;

        for (name, data) in &operator_states {
            manifest.operator_states.insert(
                name.clone(),
                laminar_storage::checkpoint_manifest::OperatorCheckpoint::inline(data),
            );
        }

        // ── Step 5: Persist manifest ──
        self.phase = CheckpointPhase::Persisting;
        if let Err(e) = self.store.save(&manifest) {
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

        // ── Step 6: Sink commit ──
        self.phase = CheckpointPhase::Committing;
        if let Err(e) = self.commit_sinks(epoch).await {
            self.checkpoints_failed += 1;
            warn!(checkpoint_id, epoch, error = %e, "sink commit partially failed");
        }

        // ── Success ──
        self.phase = CheckpointPhase::Idle;
        self.next_checkpoint_id += 1;
        self.epoch += 1;
        self.checkpoints_completed += 1;
        let duration = start.elapsed();
        self.last_checkpoint_duration = Some(duration);
        self.emit_checkpoint_metrics(true, epoch, duration);

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

/// Checkpoint performance statistics.
#[derive(Debug, Clone)]
pub struct CheckpointStats {
    /// Total completed checkpoints.
    pub completed: u64,
    /// Total failed checkpoints.
    pub failed: u64,
    /// Duration of the last checkpoint.
    pub last_duration: Option<Duration>,
    /// Current checkpoint phase.
    pub current_phase: CheckpointPhase,
    /// Current epoch number.
    pub current_epoch: u64,
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
) -> fxhash::FxHashMap<laminar_core::dag::topology::NodeId, laminar_core::operator::OperatorState> {
    let mut states = fxhash::FxHashMap::default();
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
        assert_eq!(stats.current_phase, CheckpointPhase::Idle);
    }

    #[tokio::test]
    async fn test_checkpoint_no_sources_no_sinks() {
        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let result = coord
            .checkpoint(HashMap::new(), Some(1000), 0, vec![], None)
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
            .checkpoint(HashMap::new(), Some(2000), 100, vec![], None)
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
            .checkpoint(ops, None, 4096, vec![100, 200], None)
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

    // ── F-CKP-004: operator state persistence tests ──

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

    // ── F-CKP-006: WAL checkpoint coordination tests ──

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
            )
            .await
            .unwrap();

        assert!(result.success);

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

    // ── F-CKP-009: Checkpoint observability tests ──

    #[tokio::test]
    async fn test_checkpoint_emits_metrics_on_success() {
        use crate::metrics::PipelineCounters;
        use std::sync::atomic::Ordering;

        let dir = tempfile::tempdir().unwrap();
        let mut coord = make_coordinator(dir.path());

        let counters = Arc::new(PipelineCounters::new());
        coord.set_counters(Arc::clone(&counters));

        let result = coord
            .checkpoint(HashMap::new(), Some(1000), 0, vec![], None)
            .await
            .unwrap();

        assert!(result.success);
        assert_eq!(counters.checkpoints_completed.load(Ordering::Relaxed), 1);
        assert_eq!(counters.checkpoints_failed.load(Ordering::Relaxed), 0);
        assert!(counters.last_checkpoint_duration_ms.load(Ordering::Relaxed) < 5000);
        assert_eq!(counters.checkpoint_epoch.load(Ordering::Relaxed), 1);

        // Second checkpoint
        let result2 = coord
            .checkpoint(HashMap::new(), Some(2000), 0, vec![], None)
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
            .checkpoint(HashMap::new(), None, 0, vec![], None)
            .await
            .unwrap();

        assert!(result.success);
        // No panics — metrics emission is a no-op
    }
}
