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

use super::delta_config::{DeltaLakeSinkConfig, DeltaWriteMode};
use super::delta_metrics::DeltaLakeSinkMetrics;
use crate::connector::DeliveryGuarantee;

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
    /// Current Delta Lake table version.
    delta_version: u64,
    /// Time when the current buffer started accumulating.
    buffer_start_time: Option<Instant>,
    /// Sink metrics (shared with compaction loop for cross-task reporting).
    metrics: Arc<DeltaLakeSinkMetrics>,
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
    /// Resolved table path after catalog lookup (may differ from `config.table_path`
    /// when using Unity/Glue catalogs). Used by `reopen_table()` so retries
    /// target the same resolved path that `open()` connected to.
    #[cfg(feature = "delta-lake")]
    resolved_table_path: String,
    /// Resolved storage options after catalog lookup.
    #[cfg(feature = "delta-lake")]
    resolved_storage_options: std::collections::HashMap<String, String>,
    /// Cancellation token for the background compaction task.
    #[cfg(feature = "delta-lake")]
    compaction_cancel: Option<tokio_util::sync::CancellationToken>,
    /// Handle for the background compaction task.
    #[cfg(feature = "delta-lake")]
    compaction_handle: Option<tokio::task::JoinHandle<()>>,
    /// When true, Delta table init is deferred until the first `write_batch()`
    /// provides a schema. This happens when Unity Catalog auto-create is
    /// configured but the pipeline schema is not yet available at `open()` time.
    #[cfg(feature = "delta-lake")]
    needs_deferred_delta_init: bool,
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
            delta_version: 0,
            buffer_start_time: None,
            metrics: Arc::new(DeltaLakeSinkMetrics::new()),
            epoch_skipped: false,
            staged_batches: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
            #[cfg(feature = "delta-lake")]
            table: None,
            #[cfg(feature = "delta-lake")]
            resolved_table_path: String::new(),
            #[cfg(feature = "delta-lake")]
            resolved_storage_options: std::collections::HashMap::new(),
            #[cfg(feature = "delta-lake")]
            compaction_cancel: None,
            #[cfg(feature = "delta-lake")]
            compaction_handle: None,
            #[cfg(feature = "delta-lake")]
            needs_deferred_delta_init: false,
        }
    }

    /// Creates a new Delta Lake sink with an explicit schema.
    #[must_use]
    pub fn with_schema(config: DeltaLakeSinkConfig, schema: SchemaRef) -> Self {
        let mut sink = Self::new(config);
        sink.schema = Some(schema);
        sink
    }

    /// Initializes the Delta table: auto-creates in Unity Catalog if needed,
    /// resolves the catalog path, opens or creates the Delta table, and spawns
    /// the compaction loop. Called from `open()` or deferred to the first
    /// `write_batch()` when the schema is not yet available at open time.
    #[cfg(feature = "delta-lake")]
    async fn init_delta_table(&mut self) -> Result<(), ConnectorError> {
        use super::delta_io;

        // For uc:// tables, pre-create in Unity Catalog if needed.
        // Must run before resolve_catalog_options which calls GET on the table.
        #[cfg(feature = "delta-lake-unity")]
        ensure_uc_table_exists(&self.config, self.schema.as_ref()).await?;

        // Resolve catalog path: for Unity this calls GET to get the
        // storage_location, bypassing delta-rs credential vending.
        let (resolved_path, merged_options) = delta_io::resolve_catalog_options(
            &self.config.catalog_type,
            self.config.catalog_database.as_deref(),
            self.config.catalog_name.as_deref(),
            self.config.catalog_schema.as_deref(),
            &self.config.table_path,
            &self.config.storage_options,
        )
        .await?;

        // Inject default connection timeouts if not explicitly set.
        // Azure load balancers close idle connections after ~4 minutes.
        // Without these, a stale connection causes writes to hang forever.
        let mut merged_options = merged_options;
        merged_options
            .entry("timeout".to_string())
            .or_insert_with(|| "120s".to_string());
        merged_options
            .entry("connect_timeout".to_string())
            .or_insert_with(|| "30s".to_string());
        merged_options
            .entry("pool_idle_timeout".to_string())
            .or_insert_with(|| "60s".to_string());

        // Persist resolved values for reopen_table() on conflict retry.
        self.resolved_table_path.clone_from(&resolved_path);
        self.resolved_storage_options.clone_from(&merged_options);

        let init_timeout = self
            .config
            .write_timeout
            .max(std::time::Duration::from_secs(120));
        let table = tokio::time::timeout(
            init_timeout,
            delta_io::open_or_create_table(
                &resolved_path,
                merged_options.clone(),
                self.schema.as_ref(),
            ),
        )
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "Delta table init timed out after {}s",
                init_timeout.as_secs()
            ))
        })??;

        // Read schema from existing table if we don't have one.
        if self.schema.is_none() {
            if let Ok(schema) = delta_io::get_table_schema(&table) {
                self.schema = Some(schema);
            }
        }

        // Crash recovery: scan for orphan files and resolve last committed epoch.
        // This replaces the old txn-only recovery with a full orphan scan (F031B).
        match super::delta_recovery::recover_delta_table(&table, &self.config.writer_id).await {
            Ok(recovery) => {
                if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                    self.last_committed_epoch = recovery.last_committed_epoch;
                }
                if recovery.had_incomplete_transactions {
                    warn!(
                        orphans_detected = recovery.orphans_detected,
                        orphans_deleted = recovery.orphans_deleted,
                        "recovered from incomplete transactions"
                    );
                }
            }
            Err(e) => {
                // Recovery scan failure is non-fatal: fall back to txn-only recovery.
                warn!(error = %e, "crash recovery scan failed, falling back to txn metadata");
                if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                    self.last_committed_epoch =
                        delta_io::get_last_committed_epoch(&table, &self.config.writer_id).await;
                }
            }
        }
        if self.last_committed_epoch > 0 {
            info!(
                writer_id = %self.config.writer_id,
                last_committed_epoch = self.last_committed_epoch,
                "recovered last committed epoch from Delta Lake"
            );
        }

        // Store the Delta version.
        #[allow(clippy::cast_sign_loss)]
        {
            self.delta_version = table.version().unwrap_or(0) as u64;
        }
        self.table = Some(table);

        // Spawn background compaction task if enabled (F031C).
        if self.config.compaction.enabled {
            let cancel = tokio_util::sync::CancellationToken::new();
            let handle = tokio::spawn(compaction_loop(
                resolved_path.clone(),
                Arc::new(merged_options),
                self.config.compaction.clone(),
                self.config.vacuum_retention,
                Arc::clone(&self.metrics),
                cancel.clone(),
            ));
            self.compaction_cancel = Some(cancel);
            self.compaction_handle = Some(handle);
        }

        Ok(())
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

    /// Returns `true` if the error is a Delta Lake optimistic concurrency
    /// conflict (retryable). Matches specific delta-rs conflict indicators
    /// only — not generic "transaction" mentions.
    #[cfg(feature = "delta-lake")]
    fn is_conflict_error(err: &ConnectorError) -> bool {
        let msg = err.to_string().to_lowercase();
        msg.contains("conflicting commit")
            || msg.contains("version already exists")
            || msg.contains("concurrent")
            || (msg.contains("conflict") && !msg.contains("log") && !msg.contains("corrupt"))
    }

    /// Re-opens the Delta Lake table after a conflict error destroys the handle.
    /// Uses the resolved path/options from `open()`, not `config.*`, so that
    /// catalog-resolved paths (Unity/Glue) are used correctly.
    #[cfg(feature = "delta-lake")]
    async fn reopen_table(&mut self) -> Result<(), ConnectorError> {
        use super::delta_io;

        let table = delta_io::open_or_create_table(
            &self.resolved_table_path,
            self.resolved_storage_options.clone(),
            self.schema.as_ref(),
        )
        .await?;

        #[allow(clippy::cast_sign_loss)]
        {
            self.delta_version = table.version().unwrap_or(0) as u64;
        }
        self.table = Some(table);
        Ok(())
    }

    /// Attempts a single Delta write/merge and returns the updated table on success.
    #[cfg(feature = "delta-lake")]
    async fn attempt_delta_write(
        &mut self,
        table: DeltaTable,
    ) -> Result<DeltaTable, ConnectorError> {
        // Clone batches for the write — staged_batches is only cleared on
        // success. RecordBatch::clone is Arc-bump only (~16-48ns per batch).
        let batches: Vec<RecordBatch> = self.staged_batches.clone();

        if self.config.write_mode == DeltaWriteMode::Upsert {
            // ── Upsert/Merge path ──
            let combined =
                match arrow_select::concat::concat_batches(&batches[0].schema(), &batches) {
                    Ok(c) => c,
                    Err(e) => {
                        // Concat is a local op — restore table and propagate.
                        self.table = Some(table);
                        return Err(ConnectorError::Internal(format!(
                            "failed to concat batches: {e}"
                        )));
                    }
                };

            super::delta_io::merge_changelog(
                table,
                combined,
                &self.config.merge_key_columns,
                &self.config.writer_id,
                self.current_epoch,
                self.config.schema_evolution,
            )
            .await
            .map(|(t, result)| {
                self.metrics.record_merge();
                if result.rows_deleted > 0 {
                    self.metrics.record_deletes(result.rows_deleted as u64);
                }
                t
            })
        } else {
            // ── Append/Overwrite path ──
            let save_mode = match self.config.write_mode {
                DeltaWriteMode::Append => SaveMode::Append,
                DeltaWriteMode::Overwrite => SaveMode::Overwrite,
                DeltaWriteMode::Upsert => unreachable!("handled by the upsert branch above"),
            };

            let partition_cols = if self.config.partition_columns.is_empty() {
                None
            } else {
                Some(self.config.partition_columns.as_slice())
            };

            // Create a Delta Lake checkpoint every N commits for read performance.
            let should_checkpoint = self.config.checkpoint_interval > 0
                && self.delta_version > 0
                && (self.delta_version + 1).is_multiple_of(self.config.checkpoint_interval);

            super::delta_io::write_batches(
                table,
                batches,
                &self.config.writer_id,
                self.current_epoch,
                save_mode,
                partition_cols,
                self.config.schema_evolution,
                Some(self.config.target_file_size),
                should_checkpoint,
            )
            .await
            .map(|(t, _version)| t)
        }
    }

    /// Writes all staged data to Delta Lake as a single atomic transaction.
    ///
    /// Retries on optimistic concurrency conflicts with exponential backoff.
    /// On non-conflict errors or exhausted retries, propagates the error.
    #[cfg(feature = "delta-lake")]
    async fn flush_staged_to_delta(&mut self) -> Result<WriteResult, ConnectorError> {
        if self.staged_batches.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }

        let total_rows = self.staged_rows;
        let estimated_bytes = self.staged_bytes;

        // Retry loop with exponential backoff for optimistic concurrency conflicts.
        let backoff_ms = [100, 500, 2000];
        let max_attempts = (self.config.max_commit_retries as usize).saturating_add(1);
        let mut last_error: Option<ConnectorError> = None;

        for attempt in 0..max_attempts {
            // delta-rs write/merge APIs consume DeltaTable by value.
            // If self.table is None (from a previous failed attempt), re-open it.
            if self.table.is_none() {
                let reopen_timeout = self.config.write_timeout;
                tokio::time::timeout(reopen_timeout, self.reopen_table())
                    .await
                    .map_err(|_| {
                        ConnectorError::ConnectionFailed(format!(
                            "Delta table reopen timed out after {}s",
                            reopen_timeout.as_secs()
                        ))
                    })??;
            }

            let table = self
                .table
                .take()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "table initialized".into(),
                    actual: "table not initialized".into(),
                })?;

            // Timeout the write to prevent hanging on stale connections.
            // Azure LB drops idle connections after ~4 min; without this,
            // a dead connection blocks the sink task forever.
            let write_timeout = self.config.write_timeout;
            let write_result =
                tokio::time::timeout(write_timeout, self.attempt_delta_write(table)).await;

            // Convert timeout to a write error. The table handle was consumed
            // by attempt_delta_write; the retry loop will reopen via reopen_table().
            let write_result = match write_result {
                Ok(inner) => inner,
                Err(_elapsed) => Err(ConnectorError::WriteError(format!(
                    "Delta write timed out after {}s",
                    write_timeout.as_secs()
                ))),
            };

            match write_result {
                Ok(table) => {
                    // ── Success: commit state ──
                    #[allow(clippy::cast_sign_loss)]
                    {
                        self.delta_version = table.version().unwrap_or(0) as u64;
                    }
                    self.table = Some(table);

                    // Clear staged state only after confirmed success.
                    self.staged_batches.clear();
                    self.staged_rows = 0;
                    self.staged_bytes = 0;

                    self.metrics
                        .record_flush(total_rows as u64, estimated_bytes);
                    self.metrics.record_commit(self.delta_version);

                    debug!(
                        rows = total_rows,
                        bytes = estimated_bytes,
                        delta_version = self.delta_version,
                        attempt = attempt + 1,
                        "Delta Lake: committed staged data to Delta"
                    );

                    return Ok(WriteResult::new(total_rows, estimated_bytes));
                }
                Err(e) => {
                    if Self::is_conflict_error(&e) && attempt + 1 < max_attempts {
                        let delay_ms = backoff_ms.get(attempt).copied().unwrap_or(2000);
                        warn!(
                            attempt = attempt + 1,
                            max_attempts,
                            delay_ms,
                            error = %e,
                            "Delta Lake: conflict error, retrying after backoff"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        last_error = Some(e);
                        // self.table is already None; loop will re-open.
                        continue;
                    }
                    // Non-conflict error or exhausted retries — propagate.
                    return Err(e);
                }
            }
        }

        // Should not reach here, but if it does, return the last error.
        Err(last_error.unwrap_or_else(|| {
            ConnectorError::Internal("flush_staged_to_delta: no attempts made".into())
        }))
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

        // Build boolean masks for insert vs delete rows.
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

/// Background compaction loop that periodically runs OPTIMIZE and VACUUM (F031C).
///
/// Opens its own `DeltaTable` handle (no shared state with the sink).
/// Reports compaction and vacuum metrics via the shared `DeltaLakeSinkMetrics`.
#[cfg(feature = "delta-lake")]
async fn compaction_loop(
    table_path: String,
    storage_options: Arc<std::collections::HashMap<String, String>>,
    config: super::delta_config::CompactionConfig,
    vacuum_retention: std::time::Duration,
    metrics: Arc<DeltaLakeSinkMetrics>,
    cancel: tokio_util::sync::CancellationToken,
) {
    use super::delta_io;

    info!(
        table_path = %table_path,
        check_interval_secs = config.check_interval.as_secs(),
        min_files = config.min_files_for_compaction,
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
                        info!(
                            file_count,
                            min = config.min_files_for_compaction,
                            "compaction: threshold exceeded, running OPTIMIZE"
                        );
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
                        info!(
                            files_added = result.files_added,
                            files_removed = result.files_removed,
                            partitions_optimized = result.partitions_optimized,
                            "compaction: OPTIMIZE complete"
                        );
                        metrics.record_compaction(result.files_added, result.files_removed);

                        // Run VACUUM after compaction to clean up old files.
                        match delta_io::run_vacuum(table, vacuum_retention).await {
                            Ok((_table, files_deleted)) => {
                                info!(files_deleted, "compaction: VACUUM complete");
                                metrics.record_vacuum(files_deleted as u64);
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

/// Pre-creates a Unity Catalog external Delta table via the REST API if
/// `catalog.storage.location` is configured and a schema is available.
/// Idempotent: treats "already exists" (HTTP 409 / `ALREADY_EXISTS`) as success.
#[cfg(all(feature = "delta-lake", feature = "delta-lake-unity"))]
async fn ensure_uc_table_exists(
    config: &DeltaLakeSinkConfig,
    schema: Option<&SchemaRef>,
) -> Result<(), ConnectorError> {
    let super::delta_config::DeltaCatalogType::Unity {
        ref workspace_url,
        ref access_token,
    } = config.catalog_type
    else {
        return Ok(());
    };

    let Some(ref storage_location) = config.catalog_storage_location else {
        return Ok(());
    };

    let Some(arrow_schema) = schema else {
        warn!(
            "catalog.storage.location is set but no schema available — \
             skipping Unity Catalog auto-create"
        );
        return Ok(());
    };

    let catalog = config.catalog_name.as_deref().unwrap_or_default();
    let schema_name = config.catalog_schema.as_deref().unwrap_or_default();
    let table_name = config
        .table_path
        .strip_prefix("uc://")
        .and_then(|s| s.rsplit('.').next())
        .unwrap_or(&config.table_path);

    let columns = super::unity_catalog::arrow_to_uc_columns(arrow_schema);
    super::unity_catalog::create_uc_table(
        workspace_url,
        access_token,
        catalog,
        schema_name,
        table_name,
        storage_location,
        &columns,
    )
    .await
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
        // If Unity Catalog auto-create is configured but no schema is available
        // yet, defer initialization to the first write_batch() call.
        #[cfg(feature = "delta-lake")]
        {
            let should_defer = matches!(
                self.config.catalog_type,
                super::delta_config::DeltaCatalogType::Unity { .. }
            ) && self.config.catalog_storage_location.is_some()
                && self.schema.is_none();

            if should_defer {
                info!(
                    "Unity Catalog auto-create configured but pipeline schema not yet \
                     available — deferring Delta table init to first begin_epoch"
                );
                self.needs_deferred_delta_init = true;
                // Stay in Initializing until init completes in begin_epoch().
                self.state = ConnectorState::Initializing;
                return Ok(());
            }

            self.init_delta_table().await?;
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
        // Accept both Running and Initializing (deferred init in progress).
        if self.state != ConnectorState::Running && self.state != ConnectorState::Initializing {
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

        // Fallback for deferred init: if begin_epoch() couldn't complete
        // init (schema was still None), complete it now that the first
        // batch provides a schema.
        #[cfg(feature = "delta-lake")]
        if self.needs_deferred_delta_init {
            info!("schema now available from first batch — completing deferred Delta table init");
            match self.init_delta_table().await {
                Ok(()) => {
                    self.needs_deferred_delta_init = false;
                    self.state = ConnectorState::Running;
                    info!("Delta Lake sink connector opened successfully (deferred)");
                }
                Err(e) => {
                    self.state = ConnectorState::Failed;
                    return Err(e);
                }
            }
        }

        // F031D: Validate schema evolution before accepting the batch.
        // Check that the incoming batch schema is compatible with the table schema.
        // Additive changes (new nullable columns) are allowed when schema_evolution
        // is enabled; breaking changes (type changes, removals) always error.
        if let Some(ref table_schema) = self.schema {
            let batch_target = Self::target_schema(&batch.schema(), self.config.write_mode);
            if table_schema.fields() != batch_target.fields() {
                super::delta_schema_evolution::check_schema_compatibility(
                    table_schema,
                    &batch_target,
                    self.config.schema_evolution,
                )?;
                // If we get here, the change is additive and evolution is enabled.
                // Update our schema to the merged superset so subsequent batches
                // are validated against the new schema.
                if self.config.schema_evolution {
                    self.schema = Some(batch_target);
                }
            }
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

        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce && self.should_flush() {
            if !self.staged_batches.is_empty() {
                self.flush_staged_to_delta().await?;
            }
            self.staged_batches = std::mem::take(&mut self.buffer);
            self.staged_rows = self.buffered_rows;
            self.staged_bytes = self.buffered_bytes;
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.buffer_start_time = None;
            self.flush_staged_to_delta().await?;
        }

        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Complete deferred Delta table init on the first epoch.
        // This must run BEFORE the epoch skip check so that
        // last_committed_epoch is resolved from the Delta log.
        // Without this, exactly-once recovery would miss already-committed
        // epochs and produce duplicates.
        #[cfg(feature = "delta-lake")]
        if self.needs_deferred_delta_init {
            // Schema may not be available yet on the very first epoch.
            // If so, buffer the epoch — the write_batch will provide it.
            // But if the pipeline provided a schema via with_schema() or a
            // previous epoch's write_batch set it, complete init now.
            if self.schema.is_some() {
                info!("schema available — completing deferred Delta table init");
                match self.init_delta_table().await {
                    Ok(()) => {
                        self.needs_deferred_delta_init = false;
                        self.state = ConnectorState::Running;
                        info!("Delta Lake sink connector opened successfully (deferred)");
                    }
                    Err(e) => {
                        self.state = ConnectorState::Failed;
                        return Err(e);
                    }
                }
            }
        }

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
        // For at-least-once delivery, flush() is the only commit trigger
        // because the checkpoint coordinator skips begin_epoch/pre_commit/
        // commit_epoch for non-exactly-once sinks. Write directly to Delta.
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            // Retry any orphaned staged data from a prior failed flush
            // before moving new data in, to prevent silent data loss.
            // flush_staged_to_delta handles table == None via reopen_table().
            if !self.staged_batches.is_empty() {
                self.flush_staged_to_delta().await?;
            }

            // Stage new buffered data and flush to Delta.
            if !self.buffer.is_empty() {
                self.staged_batches = std::mem::take(&mut self.buffer);
                self.staged_rows = self.buffered_rows;
                self.staged_bytes = self.buffered_bytes;
                self.buffered_rows = 0;
                self.buffered_bytes = 0;
                self.buffer_start_time = None;

                self.flush_staged_to_delta().await?;
            }
            return Ok(());
        }

        if self.buffer.is_empty() {
            return Ok(());
        }

        // For exactly-once, just coalesce in memory — the 2PC path
        // (pre_commit + commit_epoch) handles the actual Delta write.
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

        // Commit any remaining data before closing.
        // For at-least-once, use flush() which handles both orphaned
        // staged data and buffered data. For exactly-once, use 2PC.
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            self.flush().await?;
        } else if !self.buffer.is_empty() {
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
    use super::super::delta_config::DeltaCatalogType;
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

    #[cfg(feature = "delta-lake")]
    #[test]
    fn test_deferred_init_flag_default_false() {
        let sink = DeltaLakeSink::new(test_config());
        assert!(!sink.needs_deferred_delta_init);
    }

    fn unity_config() -> DeltaLakeSinkConfig {
        let mut config = test_config();
        config.catalog_type = DeltaCatalogType::Unity {
            workspace_url: "https://test.azuredatabricks.net".to_string(),
            access_token: "dapi123".to_string(),
        };
        config.catalog_name = Some("main".to_string());
        config.catalog_schema = Some("default".to_string());
        config.catalog_storage_location = Some("abfss://c@acct.dfs.core.windows.net/t".to_string());
        config
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn test_open_defers_init_for_unity_no_schema() {
        use crate::config::ConnectorConfig;

        let config = unity_config();
        let mut sink = DeltaLakeSink::new(config);

        // open() with empty ConnectorConfig (simulates factory path)
        let connector_config = ConnectorConfig::new("delta-lake");
        // open() will re-parse but table.path is "/tmp/delta_test" (local),
        // so it won't actually reach UC REST. However from_config requires
        // table.path, so we use the sink's existing config by passing empty.
        // The sink skips re-parse when properties are empty.
        let result = sink.open(&connector_config).await;
        assert!(result.is_ok());

        // Should be in Initializing state with deferred flag set.
        assert!(sink.needs_deferred_delta_init);
        assert_eq!(sink.state(), ConnectorState::Initializing);
        assert!(sink.schema.is_none());
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn test_deferred_init_transitions_to_failed_on_error() {
        // When deferred init fails, the sink must transition to Failed
        // to prevent an unbounded retry storm.
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Initializing;
        sink.needs_deferred_delta_init = true;
        sink.schema = Some(test_schema());

        // begin_epoch will try init_delta_table() which will fail
        // (no real Delta table at /tmp/delta_test). The sink should
        // transition to Failed.
        let result = sink.begin_epoch(1).await;
        assert!(result.is_err());
        assert_eq!(sink.state(), ConnectorState::Failed);
        // Flag may still be set, but Failed state prevents further usage.
    }

    #[cfg(feature = "delta-lake")]
    #[tokio::test]
    async fn test_write_batch_accepts_initializing_state() {
        // During deferred init, write_batch must accept Initializing state
        // so the first batch can provide the schema.
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Initializing;
        sink.needs_deferred_delta_init = true;

        let batch = test_batch(5);
        // write_batch sets schema, then tries init_delta_table which fails.
        // Sink transitions to Failed.
        let result = sink.write_batch(&batch).await;
        assert!(result.is_err());
        assert_eq!(sink.state(), ConnectorState::Failed);
        // Schema was set before init was attempted.
        assert!(sink.schema.is_some());
    }

    #[test]
    fn test_no_deferred_init_without_catalog_storage_location() {
        // Unity catalog without catalog.storage.location should NOT defer.
        let mut config = unity_config();
        config.catalog_storage_location = None;
        let sink = DeltaLakeSink::new(config);

        let should_defer = matches!(sink.config.catalog_type, DeltaCatalogType::Unity { .. })
            && sink.config.catalog_storage_location.is_some()
            && sink.schema.is_none();
        assert!(!should_defer);
    }

    #[test]
    fn test_no_deferred_init_with_schema() {
        // Unity catalog with schema already set should NOT defer.
        let config = unity_config();
        let sink = DeltaLakeSink::with_schema(config, test_schema());

        let should_defer = matches!(sink.config.catalog_type, DeltaCatalogType::Unity { .. })
            && sink.config.catalog_storage_location.is_some()
            && sink.schema.is_none();
        assert!(!should_defer);
    }

    #[test]
    fn test_health_check_initializing_during_deferred() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Initializing;
        // During deferred init, health should NOT report Healthy.
        assert!(matches!(sink.health_check(), HealthStatus::Unknown));
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
    // Note: Epoch lifecycle with real I/O is tested in delta_io.rs integration tests.

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
    }

    /// D001: Rollback after `pre_commit` must discard staged data.
    /// `pre_commit` stages batches; rollback discards them without writing to Delta.
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

    /// Staged data is preserved across `pre_commit` → failed commit → rollback.
    /// This verifies that `pre_commit` does not destroy staged state, so a
    /// subsequent rollback can discard it cleanly.
    #[tokio::test]
    async fn test_staged_data_preserved_until_commit_or_rollback() {
        let mut config = test_config();
        config.max_buffer_records = 1000;
        let mut sink = DeltaLakeSink::new(config);
        sink.state = ConnectorState::Running;

        sink.begin_epoch(1).await.unwrap();
        sink.write_batch(&test_batch(25)).await.unwrap();
        sink.write_batch(&test_batch(25)).await.unwrap();

        // pre_commit moves buffer → staged
        sink.pre_commit(1).await.unwrap();
        assert_eq!(sink.staged_rows, 50);
        assert_eq!(sink.staged_batches.len(), 2);
        assert_eq!(sink.buffered_rows(), 0);

        // Simulate: commit_epoch would write to Delta, but without the
        // feature we exercise the local path. Verify staged state is
        // consumed only on success.
        // (Without delta-lake feature, commit_epoch calls commit_local
        // which succeeds.)

        // Instead, test rollback: staged data should be discarded.
        sink.rollback_epoch(1).await.unwrap();
        assert!(sink.staged_batches.is_empty());
        assert_eq!(sink.staged_rows, 0);
        assert_eq!(sink.staged_bytes, 0);
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

    // ── Flush tests ──
    // Note: These tests bypass open() and test business logic only.

    #[tokio::test]
    async fn test_flush_coalesces_buffer() {
        let mut config = test_config();
        config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        config.writer_id = "test-writer".to_string();
        let mut sink = DeltaLakeSink::new(config);
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

    #[tokio::test]
    async fn test_close() {
        let mut sink = DeltaLakeSink::new(test_config());
        sink.state = ConnectorState::Running;

        sink.close().await.unwrap();
        assert_eq!(sink.state(), ConnectorState::Closed);
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
