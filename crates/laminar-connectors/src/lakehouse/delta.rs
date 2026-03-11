//! Delta Lake sink connector implementation.
//!
//! [`DeltaLakeSink`] implements [`SinkConnector`], writing Arrow `RecordBatch`
//! data to Delta Lake tables with ACID transactions and at-least-once delivery
//! (exactly-once opt-in via `delivery.guarantee = 'exactly-once'`).
//!
//! # Write Strategies
//!
//! - **Append mode**: Arrow-to-Parquet zero-copy writes for immutable streams
//! - **Overwrite mode**: Replace partition contents for recomputation
//! - **Upsert mode**: CDC MERGE via Z-set changelog integration
//!
//! Exactly-once semantics use epoch-to-Delta-version mapping: each `LaminarDB`
//! epoch maps to exactly one Delta Lake transaction via `txn` application
//! transaction metadata in the Delta log.
//!
//! # Ring Architecture
//!
//! - **Ring 0**: No sink code. Data arrives via SPSC channel (~5ns push).
//! - **Ring 1**: Batch buffering, Parquet writes, Delta log commits.
//! - **Ring 2**: Schema management, configuration, health checks.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, RecordBatch};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::{debug, info, warn};

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use deltalake::protocol::SaveMode;

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::delta_config::{DeliveryGuarantee, DeltaLakeSinkConfig, DeltaWriteMode};
use super::delta_metrics::DeltaLakeSinkMetrics;

/// Delta Lake sink connector.
///
/// Writes Arrow `RecordBatch` to Delta Lake tables with ACID transactions,
/// at-least-once delivery (exactly-once opt-in), partitioning, and
/// background compaction.
///
/// # Lifecycle
///
/// ```text
/// new() -> open() -> [begin_epoch() -> write_batch()* -> commit_epoch()] -> close()
///                          |                                    |
///                          +--- rollback_epoch() (on failure) --+
/// ```
///
/// # Exactly-Once Semantics
///
/// Each `LaminarDB` epoch maps to exactly one Delta Lake transaction (version).
/// On recovery, the sink checks `_delta_log/` for the last committed epoch
/// via `txn` (application transaction) metadata. If an epoch was already
/// committed, it is skipped (idempotent commit).
///
/// # 2PC Protocol
///
/// `pre_commit()` coalesces the buffer but does NOT write to Delta.
/// `commit_epoch()` performs the actual Delta write+commit atomically.
/// `rollback_epoch()` discards the staged buffer without side effects.
/// This ensures that a rolled-back epoch never leaves data in Delta.
pub struct DeltaLakeSink {
    /// Sink configuration.
    config: DeltaLakeSinkConfig,
    /// Arrow schema for input batches (set on first write or from existing table).
    schema: Option<SchemaRef>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Current epoch being written.
    current_epoch: u64,
    /// Last successfully committed epoch.
    last_committed_epoch: u64,
    /// `RecordBatch` buffer for the current epoch.
    buffer: Vec<RecordBatch>,
    /// Total rows buffered in current epoch.
    buffered_rows: usize,
    /// Total bytes buffered (estimated) in current epoch.
    buffered_bytes: u64,
    /// Number of Parquet files pending commit in current epoch.
    pending_files: usize,
    /// Current Delta Lake table version.
    delta_version: u64,
    /// Time when the current buffer started accumulating.
    buffer_start_time: Option<Instant>,
    /// Sink metrics.
    metrics: DeltaLakeSinkMetrics,
    /// Delta Lake table handle (present when `delta-lake` feature is enabled).
    #[cfg(feature = "delta-lake")]
    table: Option<DeltaTable>,
    /// Whether the current epoch was skipped (already committed).
    epoch_skipped: bool,
    /// Staged batches ready for commit (populated by `pre_commit()`, consumed
    /// by `commit_epoch()`). This separation ensures `rollback_epoch()` can
    /// discard prepared data without leaving orphan files in Delta.
    staged_batches: Vec<RecordBatch>,
    /// Rows staged for commit (mirrors `staged_batches`).
    staged_rows: usize,
    /// Estimated bytes staged for commit.
    staged_bytes: u64,
    /// Cancellation token for the background compaction task.
    #[cfg(feature = "delta-lake")]
    compaction_cancel: Option<tokio_util::sync::CancellationToken>,
    /// Handle for the background compaction task.
    #[cfg(feature = "delta-lake")]
    compaction_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DeltaLakeSink {
    /// Creates a new Delta Lake sink with the given configuration.
    #[must_use]
    pub fn new(config: DeltaLakeSinkConfig) -> Self {
        Self {
            config,
            schema: None,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            buffer: Vec::with_capacity(16),
            buffered_rows: 0,
            buffered_bytes: 0,
            pending_files: 0,
            delta_version: 0,
            buffer_start_time: None,
            metrics: DeltaLakeSinkMetrics::new(),
            epoch_skipped: false,
            staged_batches: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
            #[cfg(feature = "delta-lake")]
            table: None,
            #[cfg(feature = "delta-lake")]
            compaction_cancel: None,
            #[cfg(feature = "delta-lake")]
            compaction_handle: None,
        }
    }

    /// Creates a new Delta Lake sink with an explicit schema.
    #[must_use]
    pub fn with_schema(config: DeltaLakeSinkConfig, schema: SchemaRef) -> Self {
        let mut sink = Self::new(config);
        sink.schema = Some(schema);
        sink
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Returns the last committed epoch.
    #[must_use]
    pub fn last_committed_epoch(&self) -> u64 {
        self.last_committed_epoch
    }

    /// Returns the number of buffered rows pending flush.
    #[must_use]
    pub fn buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    /// Returns the estimated buffered bytes.
    #[must_use]
    pub fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    /// Returns the current Delta Lake table version.
    #[must_use]
    pub fn delta_version(&self) -> u64 {
        self.delta_version
    }

    /// Returns a reference to the sink metrics.
    #[must_use]
    pub fn sink_metrics(&self) -> &DeltaLakeSinkMetrics {
        &self.metrics
    }

    /// Returns the sink configuration.
    #[must_use]
    pub fn config(&self) -> &DeltaLakeSinkConfig {
        &self.config
    }

    /// Checks if a buffer flush is needed based on size or time thresholds.
    #[must_use]
    pub fn should_flush(&self) -> bool {
        if self.buffered_rows >= self.config.max_buffer_records {
            return true;
        }
        if self.buffered_bytes >= self.config.target_file_size as u64 {
            return true;
        }
        if let Some(start) = self.buffer_start_time {
            if start.elapsed() >= self.config.max_buffer_duration {
                return true;
            }
        }
        false
    }

    /// Returns the target table schema, stripping metadata columns (`_op`, etc.)
    /// in upsert mode so the Delta table isn't created with changelog columns.
    /// CDC metadata columns stripped from the target Delta table schema.
    const CDC_METADATA_COLUMNS: &'static [&'static str] = &["_op", "_ts_ms"];

    fn target_schema(batch_schema: &SchemaRef, write_mode: DeltaWriteMode) -> SchemaRef {
        if write_mode == DeltaWriteMode::Upsert {
            let fields: Vec<_> = batch_schema
                .fields()
                .iter()
                .filter(|f| !Self::CDC_METADATA_COLUMNS.contains(&f.name().as_str()))
                .cloned()
                .collect();
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            batch_schema.clone()
        }
    }

    /// Estimates the byte size of a `RecordBatch`.
    #[must_use]
    pub fn estimate_batch_size(batch: &RecordBatch) -> u64 {
        batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size() as u64)
            .sum()
    }

    /// Writes all staged data to Delta Lake as a single atomic transaction.
    ///
    /// Called from `commit_epoch()` only — after the checkpoint manifest is
    /// persisted. This ensures `rollback_epoch()` can discard staged data
    /// without leaving orphaned files in Delta.
    #[cfg(feature = "delta-lake")]
    async fn flush_staged_to_delta(&mut self) -> Result<WriteResult, ConnectorError> {
        if self.staged_batches.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }

        let total_rows = self.staged_rows;
        let estimated_bytes = self.staged_bytes;

        // Take the table and staged batches for the write operation.
        let mut table = self
            .table
            .take()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "table initialized".into(),
                actual: "table not initialized".into(),
            })?;

        let batches = std::mem::take(&mut self.staged_batches);
        self.staged_rows = 0;
        self.staged_bytes = 0;

        if self.config.write_mode == DeltaWriteMode::Upsert {
            // ── Upsert/Merge path ──
            // Single atomic MERGE handles inserts, updates, and deletes
            // via conditional clauses on the _op column.
            let combined = arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
                .map_err(|e| ConnectorError::Internal(format!("failed to concat batches: {e}")))?;

            let (t, result) = super::delta_io::merge_changelog(
                table,
                combined,
                &self.config.merge_key_columns,
                &self.config.writer_id,
                self.current_epoch,
                self.config.schema_evolution,
            )
            .await?;
            table = t;
            self.metrics.record_merge();
            if result.rows_deleted > 0 {
                self.metrics.record_deletes(result.rows_deleted as u64);
            }
        } else {
            // ── Append/Overwrite path ──
            let save_mode = match self.config.write_mode {
                DeltaWriteMode::Append => SaveMode::Append,
                DeltaWriteMode::Overwrite => SaveMode::Overwrite,
                DeltaWriteMode::Upsert => {
                    return Err(ConnectorError::ConfigurationError(
                        "upsert mode should be handled by the merge branch above".into(),
                    ));
                }
            };

            let partition_cols = if self.config.partition_columns.is_empty() {
                None
            } else {
                Some(self.config.partition_columns.as_slice())
            };

            let (t, _version) = super::delta_io::write_batches(
                table,
                batches,
                &self.config.writer_id,
                self.current_epoch,
                save_mode,
                partition_cols,
                self.config.schema_evolution,
            )
            .await?;
            table = t;
        }

        // Restore table and update state.
        // Note: Delta Lake uses i64 for version, but our version is u64.
        // Versions are always non-negative, so this is safe.
        #[allow(clippy::cast_sign_loss)]
        {
            self.delta_version = table.version().unwrap_or(0) as u64;
        }
        self.table = Some(table);
        self.pending_files = 0;

        self.metrics
            .record_flush(total_rows as u64, estimated_bytes);
        self.metrics.record_commit(self.delta_version);

        debug!(
            rows = total_rows,
            bytes = estimated_bytes,
            delta_version = self.delta_version,
            "Delta Lake: committed staged data to Delta"
        );

        Ok(WriteResult::new(total_rows, estimated_bytes))
    }

    /// Commits pending files as a Delta Lake transaction (updates metrics).
    /// Only used when the delta-lake feature is NOT enabled (local simulation).
    #[cfg(not(feature = "delta-lake"))]
    fn commit_local(&mut self, epoch: u64) {
        self.delta_version += 1;
        self.pending_files = 0;

        // Record flush metrics for staged data.
        self.metrics
            .record_flush(self.staged_rows as u64, self.staged_bytes);
        self.metrics.record_commit(self.delta_version);

        debug!(
            epoch,
            delta_version = self.delta_version,
            "Delta Lake: committed transaction"
        );
    }

    /// Splits a changelog `RecordBatch` into insert and delete batches.
    ///
    /// Uses the `_op` metadata column:
    /// - `"I"` (insert), `"U"` (update-after), `"r"` (snapshot read) -> insert
    /// - `"D"` (delete) -> delete
    ///
    /// The returned batches exclude metadata columns (those starting with `_`).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the `_op` column is
    /// missing or not a string type.
    pub fn split_changelog_batch(
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, RecordBatch), ConnectorError> {
        let op_idx = batch.schema().index_of("_op").map_err(|_| {
            ConnectorError::ConfigurationError(
                "upsert mode requires '_op' column in input schema".into(),
            )
        })?;

        let op_array = batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("'_op' column must be String (Utf8) type".into())
            })?;

        // Build boolean masks (compact bit-buffers, no per-element heap allocation).
        let len = op_array.len();
        let mut insert_mask = Vec::with_capacity(len);
        let mut delete_mask = Vec::with_capacity(len);

        for i in 0..len {
            if op_array.is_null(i) {
                insert_mask.push(false);
                delete_mask.push(false);
                continue;
            }
            match op_array.value(i) {
                "I" | "U" | "r" => {
                    insert_mask.push(true);
                    delete_mask.push(false);
                }
                "D" => {
                    insert_mask.push(false);
                    delete_mask.push(true);
                }
                _ => {
                    insert_mask.push(false);
                    delete_mask.push(false);
                }
            }
        }

        // Compute user-column projection indices once (strip metadata columns).
        let user_col_indices: Vec<usize> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.name().starts_with('_'))
            .map(|(i, _)| i)
            .collect();

        let insert_batch = filter_and_project(batch, &insert_mask, &user_col_indices)?;
        let delete_batch = filter_and_project(batch, &delete_mask, &user_col_indices)?;

        Ok((insert_batch, delete_batch))
    }
}

/// Background compaction loop that periodically runs OPTIMIZE and VACUUM.
///
/// Opens its own `DeltaTable` handle (no shared state with the sink).
#[cfg(feature = "delta-lake")]
async fn compaction_loop(
    table_path: String,
    storage_options: Arc<std::collections::HashMap<String, String>>,
    config: super::delta_config::CompactionConfig,
    vacuum_retention: std::time::Duration,
    cancel: tokio_util::sync::CancellationToken,
) {
    use super::delta_io;

    info!(
        table_path = %table_path,
        check_interval_secs = config.check_interval.as_secs(),
        "compaction background task started"
    );

    let mut interval = tokio::time::interval(config.check_interval);
    // Skip the first immediate tick.
    interval.tick().await;

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!("compaction background task cancelled");
                return;
            }
            _ = interval.tick() => {
                // Open a fresh table handle for compaction (no shared state).
                // Clone the HashMap only here (once per tick, not avoidable
                // since open_or_create_table takes owned HashMap).
                let table = match delta_io::open_or_create_table(
                    &table_path,
                    (*storage_options).clone(),
                    None,
                )
                .await
                {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(error = %e, "compaction: failed to open table, will retry");
                        continue;
                    }
                };

                // Skip compaction if not enough files.
                match table.snapshot() {
                    Ok(snapshot) => {
                        let file_count = snapshot.log_data().num_files();
                        if file_count < config.min_files_for_compaction {
                            debug!(
                                file_count,
                                min = config.min_files_for_compaction,
                                "compaction: skipping, not enough files"
                            );
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "compaction: snapshot failed, skipping tick");
                        continue;
                    }
                }

                // Run OPTIMIZE.
                let target_size = config.target_file_size as u64;
                match delta_io::run_compaction(table, target_size, &config.z_order_columns).await {
                    Ok((table, result)) => {
                        debug!(
                            files_added = result.files_added,
                            files_removed = result.files_removed,
                            "compaction: OPTIMIZE complete"
                        );

                        // Run VACUUM after compaction.
                        match delta_io::run_vacuum(table, vacuum_retention).await {
                            Ok((_table, files_deleted)) => {
                                debug!(files_deleted, "compaction: VACUUM complete");
                            }
                            Err(e) => {
                                warn!(error = %e, "compaction: VACUUM failed");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "compaction: OPTIMIZE failed");
                    }
                }
            }
        }
    }
}

#[async_trait]
impl SinkConnector for DeltaLakeSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
        if !config.properties().is_empty() {
            self.config = DeltaLakeSinkConfig::from_config(config)?;
        }

        info!(
            table_path = %self.config.table_path,
            mode = %self.config.write_mode,
            guarantee = %self.config.delivery_guarantee,
            "opening Delta Lake sink connector"
        );

        // When delta-lake feature is enabled, open/create the actual table.
        #[cfg(feature = "delta-lake")]
        {
            use super::delta_io;

            // Merge catalog options (Unity/Glue) into storage options.
            let (resolved_path, merged_options) = delta_io::resolve_catalog_options(
                &self.config.catalog_type,
                self.config.catalog_database.as_deref(),
                self.config.catalog_name.as_deref(),
                self.config.catalog_schema.as_deref(),
                &self.config.table_path,
                &self.config.storage_options,
                &self.config.catalog_properties,
            )
            .await?;

            let table = delta_io::open_or_create_table(
                &resolved_path,
                merged_options.clone(),
                self.schema.as_ref(),
            )
            .await?;

            // Read schema from existing table if we don't have one.
            if self.schema.is_none() {
                if let Ok(schema) = delta_io::get_table_schema(&table) {
                    self.schema = Some(schema);
                }
            }

            // Resolve last committed epoch for exactly-once recovery.
            if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                self.last_committed_epoch =
                    delta_io::get_last_committed_epoch(&table, &self.config.writer_id).await;
                if self.last_committed_epoch > 0 {
                    info!(
                        writer_id = %self.config.writer_id,
                        last_committed_epoch = self.last_committed_epoch,
                        "recovered last committed epoch from Delta Lake txn metadata"
                    );
                }
            }

            // Store the Delta version.
            // Note: Delta Lake uses i64 for version, but our version is u64.
            // Versions are always non-negative, so this is safe.
            #[allow(clippy::cast_sign_loss)]
            {
                self.delta_version = table.version().unwrap_or(0) as u64;
            }
            self.table = Some(table);

            // Spawn background compaction task if enabled.
            if self.config.compaction.enabled {
                let cancel = tokio_util::sync::CancellationToken::new();
                let handle = tokio::spawn(compaction_loop(
                    resolved_path.clone(),
                    Arc::new(merged_options),
                    self.config.compaction.clone(),
                    self.config.vacuum_retention,
                    cancel.clone(),
                ));
                self.compaction_cancel = Some(cancel);
                self.compaction_handle = Some(handle);
            }
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ConfigurationError(
                "Delta Lake sink requires the 'delta-lake' feature to be enabled. \
                 Build with: cargo build --features delta-lake"
                    .into(),
            ));
        }

        #[cfg(feature = "delta-lake")]
        {
            self.state = ConnectorState::Running;
            info!("Delta Lake sink connector opened successfully");
            Ok(())
        }
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        if self.epoch_skipped {
            return Ok(WriteResult::new(0, 0));
        }

        // Handle schema on first write. In upsert mode, strip metadata columns
        // (_op, _ts_ms) so the Delta table isn't created with changelog columns.
        if self.schema.is_none() {
            self.schema = Some(Self::target_schema(&batch.schema(), self.config.write_mode));
        }

        let num_rows = batch.num_rows();
        let estimated_bytes = Self::estimate_batch_size(batch);

        // Buffer the batch.
        if self.buffer_start_time.is_none() {
            self.buffer_start_time = Some(Instant::now());
        }
        self.buffer.push(batch.clone());
        self.buffered_rows += num_rows;
        self.buffered_bytes += estimated_bytes;

        // Data stays in buffer until pre_commit(). No mid-epoch Delta writes.

        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // For exactly-once, skip epochs already committed.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            warn!(
                epoch,
                last_committed = self.last_committed_epoch,
                "Delta Lake: skipping already-committed epoch"
            );
            self.epoch_skipped = true;
            return Ok(());
        }

        self.epoch_skipped = false;
        self.current_epoch = epoch;
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_files = 0;
        self.buffer_start_time = None;

        debug!(epoch, "Delta Lake: began epoch");
        Ok(())
    }

    async fn pre_commit(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        // Stage buffered data for commit. The actual Delta write happens in
        // commit_epoch() — this ensures rollback_epoch() can discard the data
        // without leaving orphan files in Delta.
        if !self.buffer.is_empty() {
            self.staged_batches = std::mem::take(&mut self.buffer);
            self.staged_rows = self.buffered_rows;
            self.staged_bytes = self.buffered_bytes;
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.buffer_start_time = None;
        }

        #[cfg(not(feature = "delta-lake"))]
        {
            self.pending_files += 1;
        }

        debug!(epoch, "Delta Lake: pre-committed (batches staged)");
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        // Write staged data to Delta as a single atomic transaction.
        #[cfg(feature = "delta-lake")]
        {
            if !self.staged_batches.is_empty() {
                self.flush_staged_to_delta().await?;
            }
        }
        #[cfg(not(feature = "delta-lake"))]
        {
            if self.pending_files > 0 || !self.staged_batches.is_empty() {
                self.commit_local(epoch);
                self.staged_batches.clear();
                self.staged_rows = 0;
                self.staged_bytes = 0;
            }
        }

        self.last_committed_epoch = epoch;

        info!(
            epoch,
            delta_version = self.delta_version,
            "Delta Lake: committed epoch"
        );

        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard both buffered and staged data. Because the actual Delta
        // write only happens in commit_epoch(), no orphan files are created.
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.pending_files = 0;
        self.buffer_start_time = None;
        self.staged_batches.clear();
        self.staged_rows = 0;
        self.staged_bytes = 0;

        self.epoch_skipped = false;
        self.metrics.record_rollback();
        warn!(epoch, "Delta Lake: rolled back epoch");
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => HealthStatus::Healthy,
            ConnectorState::Created | ConnectorState::Initializing => HealthStatus::Unknown,
            ConnectorState::Paused => HealthStatus::Degraded("connector paused".into()),
            ConnectorState::Recovering => HealthStatus::Degraded("recovering".into()),
            ConnectorState::Closed => HealthStatus::Unhealthy("closed".into()),
            ConnectorState::Failed => HealthStatus::Unhealthy("failed".into()),
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = SinkConnectorCapabilities::default().with_idempotent();

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            caps = caps.with_exactly_once().with_two_phase_commit();
        }
        if self.config.write_mode == DeltaWriteMode::Upsert {
            caps = caps.with_upsert().with_changelog();
        }
        if self.config.schema_evolution {
            caps = caps.with_schema_evolution();
        }
        if !self.config.partition_columns.is_empty() {
            caps = caps.with_partitioned();
        }

        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        // Coalesce buffered batches to reduce memory fragmentation.
        // Actual Delta write is deferred to commit_epoch().
        if self.buffer.len() > 1 {
            let schema = self.buffer[0].schema();
            let combined = arrow_select::concat::concat_batches(&schema, &self.buffer)
                .map_err(|e| ConnectorError::Internal(format!("concat failed: {e}")))?;
            self.buffer.clear();
            self.buffer.push(combined);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Delta Lake sink connector");

        // Commit any remaining buffered data before closing.
        if !self.buffer.is_empty() {
            self.pre_commit(self.current_epoch).await?;
            self.commit_epoch(self.current_epoch).await?;
        }

        // Cancel and join the background compaction task.
        #[cfg(feature = "delta-lake")]
        {
            if let Some(cancel) = self.compaction_cancel.take() {
                cancel.cancel();
            }
            if let Some(handle) = self.compaction_handle.take() {
                // Wait up to 5 seconds for the compaction task to finish.
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
            }
        }

        // Drop the table handle when closing.
        #[cfg(feature = "delta-lake")]
        {
            self.table = None;
        }

        self.state = ConnectorState::Closed;

        info!(
            table_path = %self.config.table_path,
            delta_version = self.delta_version,
            "Delta Lake sink connector closed"
        );

        Ok(())
    }
}

impl std::fmt::Debug for DeltaLakeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaLakeSink")
            .field("state", &self.state)
            .field("table_path", &self.config.table_path)
            .field("mode", &self.config.write_mode)
            .field("guarantee", &self.config.delivery_guarantee)
            .field("current_epoch", &self.current_epoch)
            .field("last_committed_epoch", &self.last_committed_epoch)
            .field("buffered_rows", &self.buffered_rows)
            .field("delta_version", &self.delta_version)
            .field("epoch_skipped", &self.epoch_skipped)
            .finish_non_exhaustive()
    }
}

// ── Helper functions ────────────────────────────────────────────────

/// Filters a `RecordBatch` using a boolean mask and projects to the given column indices.
///
/// Uses Arrow's SIMD-optimized `filter` kernel instead of index-gather (`take`).
fn filter_and_project(
    batch: &RecordBatch,
    mask: &[bool],
    col_indices: &[usize],
) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::BooleanArray;
    use arrow_select::filter::filter_record_batch;

    let bool_array = BooleanArray::from(mask.to_vec());

    // Filter the full batch first (SIMD-optimized), then project columns.
    let filtered = filter_record_batch(batch, &bool_array)
        .map_err(|e| ConnectorError::Internal(format!("arrow filter failed: {e}")))?;

    filtered
        .project(col_indices)
        .map_err(|e| ConnectorError::Internal(format!("batch projection failed: {e}")))
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn test_config() -> DeltaLakeSinkConfig {
        DeltaLakeSinkConfig::new("/tmp/delta_test")
    }

    fn upsert_config() -> DeltaLakeSinkConfig {
        let mut cfg = test_config();
        cfg.write_mode = DeltaWriteMode::Upsert;
        cfg.merge_key_columns = vec!["id".to_string()];
        cfg
    }

    fn test_batch(n: usize) -> RecordBatch {
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<&str> = (0..n).map(|_| "test").collect();
        let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    // ── Constructor tests ──

    #[test]
    fn test_new_defaults() {
        let sink = DeltaLakeSink::new(test_config());
        assert_eq!(sink.state(), ConnectorState::Created);
        assert_eq!(sink.current_epoch(), 0);
        assert_eq!(sink.last_committed_epoch(), 0);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_bytes(), 0);
        assert_eq!(sink.delta_version(), 0);
        assert!(sink.schema.is_none());
    }

    #[test]
    fn test_with_schema() {
        let schema = test_schema();
        let sink = DeltaLakeSink::with_schema(test_config(), schema.clone());
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn test_schema_empty_when_none() {
        let sink = DeltaLakeSink::new(test_config());
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 0);
    }

    // ── Batch size estimation ──

    #[test]
    fn test_estimate_batch_size() {
        let batch = test_batch(100);
        let size = DeltaLakeSink::estimate_batch_size(&batch);
        assert!(size > 0);
    }

    #[test]
    fn test_estimate_batch_size_empty() {
        let batch = RecordBatch::new_empty(test_schema());
        let size = DeltaLakeSink::estimate_batch_size(&batch);
        // Arrow arrays have baseline buffer allocation even with 0 rows,
        // so size may be small but not necessarily zero.
        assert!(size < 1024);
    }

    // ── Should flush tests ──

    #[test]
    fn test_should_flush_by_rows() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = DeltaLakeSink::new(config);
        sink.buffered_rows = 99;
        assert!(!sink.should_flush());
        sink.buffered_rows = 100;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_should_flush_by_bytes() {
        let mut config = test_config();
        config.target_file_size = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.buffered_bytes = 999;
        assert!(!sink.should_flush());
        sink.buffered_bytes = 1000;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_should_flush_empty() {
        let sink = DeltaLakeSink::new(test_config());
        assert!(!sink.should_flush());
    }

    // ── Batch buffering tests ──

    #[tokio::test]
    async fn test_write_batch_buffering() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await.unwrap();

        // Should buffer, not flush (10 < 100)
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 10);
        assert!(sink.buffered_bytes() > 0);
    }

    #[tokio::test]
    async fn test_write_batch_buffers_without_commit() {
        let mut config = test_config();
        config.max_buffer_records = 10;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        // Write batches that exceed the threshold — no mid-epoch commit.
        let batch = test_batch(6);
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();

        assert_eq!(sink.buffered_rows(), 12);
        assert_eq!(sink.buffer.len(), 2);
        assert_eq!(sink.pending_files, 0);
    }

    #[tokio::test]
    async fn test_write_batch_empty() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        let batch = test_batch(0);
        let result = sink.write_batch(&batch).await.unwrap();
        assert_eq!(result.records_written, 0);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn test_write_batch_not_running() {
        let mut sink = DeltaLakeSink::new(test_config());
        // state is Created, not Running

        let batch = test_batch(10);
        let result = sink.write_batch(&batch).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_batch_sets_schema() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;
        assert!(sink.schema.is_none());

        let batch = test_batch(5);
        sink.write_batch(&batch).await.unwrap();
        assert!(sink.schema.is_some());
        assert_eq!(sink.schema.unwrap().fields().len(), 3);
    }

    #[tokio::test]
    async fn test_multiple_write_batches_accumulate() {
        let mut config = test_config();
        config.max_buffer_records = 100;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();

        assert_eq!(sink.buffered_rows(), 30);
    }

    // ── Epoch lifecycle tests ──
    // Note: These tests bypass open() and test business logic only.
    // With the delta-lake feature, commit_epoch() does real I/O which fails without a table.
    // See delta_io.rs for integration tests with real I/O.

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_epoch_lifecycle() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        // Begin epoch
        sink.begin_epoch(1).await.unwrap();
        assert_eq!(sink.current_epoch(), 1);

        // Write some data
        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();

        // Two-phase commit: pre_commit flushes buffer, commit_epoch commits
        sink.pre_commit(1).await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
        assert_eq!(sink.delta_version(), 1);

        // Metrics should show 1 commit
        let m = sink.metrics();
        let commits = m.custom.iter().find(|(k, _)| k == "delta.commits");
        assert_eq!(commits.unwrap().1, 1.0);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_epoch_skip_already_committed() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        // Commit epoch 1
        sink.begin_epoch(1).await.unwrap();
        let batch = test_batch(5);
        sink.write_batch(&batch).await.unwrap();
        sink.pre_commit(1).await.unwrap();
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);

        // Try to begin epoch 1 again (should skip)
        sink.begin_epoch(1).await.unwrap();
        // Should not have cleared the state for a new epoch
        // (skipped because already committed)

        // Commit epoch 1 again (should be no-op due to idempotency)
        sink.pre_commit(1).await.unwrap();
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
        assert_eq!(sink.delta_version(), 1); // No new version
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_epoch_at_least_once_no_skip() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        let batch = test_batch(5);
        sink.write_batch(&batch).await.unwrap();
        sink.pre_commit(1).await.unwrap();
        sink.commit_epoch(1).await.unwrap();

        // Begin epoch 1 again (at-least-once doesn't skip)
        sink.begin_epoch(1).await.unwrap();
        assert_eq!(sink.current_epoch(), 1);
        assert_eq!(sink.buffered_rows(), 0); // Buffer was cleared
    }

    #[tokio::test]
    async fn test_rollback_clears_buffer() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(50);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 50);

        sink.rollback_epoch(0).await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.buffered_bytes(), 0);
        assert_eq!(sink.pending_files, 0);
    }

    /// D001: Rollback after pre_commit must discard staged data.
    /// pre_commit stages batches; rollback discards them without writing to Delta.
    #[tokio::test]
    async fn test_rollback_after_pre_commit_discards_staged() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        let batch = test_batch(50);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 50);

        // pre_commit stages the buffer
        sink.pre_commit(1).await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.staged_rows, 50);
        assert!(!sink.staged_batches.is_empty());

        // rollback discards both buffer and staged data
        sink.rollback_epoch(1).await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.staged_rows, 0);
        assert_eq!(sink.staged_bytes, 0);
        assert!(sink.staged_batches.is_empty());
        assert_eq!(sink.delta_version(), 0); // no Delta write occurred
    }

    #[tokio::test]
    async fn test_commit_empty_epoch() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        // No writes
        sink.commit_epoch(1).await.unwrap();
        assert_eq!(sink.last_committed_epoch(), 1);
        assert_eq!(sink.delta_version(), 0); // No version bump (no files)
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_sequential_epochs() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        for epoch in 1..=5 {
            sink.begin_epoch(epoch).await.unwrap();
            let batch = test_batch(10);
            sink.write_batch(&batch).await.unwrap();
            sink.pre_commit(epoch).await.unwrap();
            sink.commit_epoch(epoch).await.unwrap();
        }

        assert_eq!(sink.last_committed_epoch(), 5);
        assert_eq!(sink.delta_version(), 5);
    }

    // ── Flush tests ──
    // Note: These tests bypass open() and test business logic only.

    #[tokio::test]
    async fn test_flush_coalesces_buffer() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffer.len(), 2);

        // flush() coalesces batches but does not write to Delta.
        sink.flush().await.unwrap();
        assert_eq!(sink.buffer.len(), 1);
        assert_eq!(sink.buffered_rows(), 20);
    }

    // ── Open and close tests ──
    // Note: These tests use fake paths that don't exist.
    // With the delta-lake feature, open() tries to actually access the path.
    // See delta_io.rs for integration tests with real I/O.

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_open_requires_feature() {
        let mut sink = DeltaLakeSink::new(test_config());

        let connector_config = ConnectorConfig::new("delta-lake");
        let result = sink.open(&connector_config).await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("delta-lake"), "error: {err}");
    }

    #[tokio::test]
    async fn test_close() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.close().await.unwrap();
        assert_eq!(sink.state(), ConnectorState::Closed);
    }

    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_close_flushes_remaining() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        let batch = test_batch(30);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 30);

        sink.close().await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);

        let m = sink.metrics();
        assert_eq!(m.records_total, 30);
    }

    // ── Health check tests ──

    #[test]
    fn test_health_check_created() {
        let sink = DeltaLakeSink::new(test_config());
        assert_eq!(sink.health_check(), HealthStatus::Unknown);
    }

    #[test]
    fn test_health_check_running() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;
        assert_eq!(sink.health_check(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_closed() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Closed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_failed() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Failed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_health_check_paused() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Paused;
        assert!(matches!(sink.health_check(), HealthStatus::Degraded(_)));
    }

    // ── Capabilities tests ──

    #[test]
    fn test_capabilities_append_exactly_once() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let sink = DeltaLakeSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.exactly_once);
        assert!(caps.idempotent);
        assert!(!caps.upsert);
        assert!(!caps.changelog);
        assert!(!caps.schema_evolution);
        assert!(!caps.partitioned);
    }

    #[test]
    fn test_capabilities_upsert() {
        let sink = DeltaLakeSink::new(upsert_config());
        let caps = sink.capabilities();
        assert!(caps.upsert);
        assert!(caps.changelog);
        assert!(caps.idempotent);
    }

    #[test]
    fn test_capabilities_schema_evolution() {
        let mut config = test_config();
        config.schema_evolution = true;
        let sink = DeltaLakeSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.schema_evolution);
    }

    #[test]
    fn test_capabilities_partitioned() {
        let mut config = test_config();
        config.partition_columns = vec!["trade_date".to_string()];
        let sink = DeltaLakeSink::new(config);
        let caps = sink.capabilities();
        assert!(caps.partitioned);
    }

    #[test]
    fn test_capabilities_at_least_once() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::AtLeastOnce;
        let sink = DeltaLakeSink::new(config);
        let caps = sink.capabilities();
        assert!(!caps.exactly_once);
        assert!(caps.idempotent);
    }

    // ── Metrics tests ──

    #[test]
    fn test_metrics_initial() {
        let sink = DeltaLakeSink::new(test_config());
        let m = sink.metrics();
        assert_eq!(m.records_total, 0);
        assert_eq!(m.bytes_total, 0);
        assert_eq!(m.errors_total, 0);
    }

    // When delta-lake feature is enabled, this triggers auto-flush which
    // needs a real table. See delta_io::tests for integration coverage.
    #[cfg(not(feature = "delta-lake"))]
    #[tokio::test]
    async fn test_metrics_after_commit() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        let batch = test_batch(10);
        sink.write_batch(&batch).await.unwrap();
        assert_eq!(sink.buffered_rows(), 10);

        // Metrics are recorded on commit_epoch, not on pre_commit.
        sink.pre_commit(0).await.unwrap();
        sink.commit_epoch(0).await.unwrap();
        let m = sink.metrics();
        assert_eq!(m.records_total, 10);
        assert!(m.bytes_total > 0);
    }

    // ── Changelog splitting tests ──

    fn changelog_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_op", DataType::Utf8, false),
            Field::new("_ts_ms", DataType::Int64, false),
        ]))
    }

    fn changelog_batch() -> RecordBatch {
        RecordBatch::try_new(
            changelog_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(StringArray::from(vec!["I", "U", "D", "I", "D"])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_split_changelog_batch() {
        let batch = changelog_batch();
        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();

        // Inserts: rows 0 (I), 1 (U), 3 (I) = 3 rows
        assert_eq!(inserts.num_rows(), 3);
        // Deletes: rows 2 (D), 4 (D) = 2 rows
        assert_eq!(deletes.num_rows(), 2);

        // Metadata columns should be stripped
        assert_eq!(inserts.num_columns(), 2); // id, name only
        assert_eq!(deletes.num_columns(), 2);

        // Verify insert values
        let insert_ids = inserts
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(insert_ids.value(0), 1);
        assert_eq!(insert_ids.value(1), 2);
        assert_eq!(insert_ids.value(2), 4);

        // Verify delete values
        let delete_ids = deletes
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(delete_ids.value(0), 3);
        assert_eq!(delete_ids.value(1), 5);
    }

    #[test]
    fn test_split_changelog_all_inserts() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["I", "I"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 2);
        assert_eq!(deletes.num_rows(), 0);
    }

    #[test]
    fn test_split_changelog_all_deletes() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(StringArray::from(vec!["D", "D"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 0);
        assert_eq!(deletes.num_rows(), 2);
    }

    #[test]
    fn test_split_changelog_missing_op_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        let result = DeltaLakeSink::split_changelog_batch(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_split_changelog_snapshot_read() {
        let schema = changelog_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["r"])), // snapshot read
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .unwrap();

        let (inserts, deletes) = DeltaLakeSink::split_changelog_batch(&batch).unwrap();
        assert_eq!(inserts.num_rows(), 1);
        assert_eq!(deletes.num_rows(), 0);
    }

    // ── Debug output test ──

    #[test]
    fn test_debug_output() {
        let sink = DeltaLakeSink::new(test_config());
        let debug = format!("{sink:?}");
        assert!(debug.contains("DeltaLakeSink"));
        assert!(debug.contains("/tmp/delta_test"));
    }
}
