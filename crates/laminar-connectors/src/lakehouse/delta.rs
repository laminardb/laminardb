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
use std::time::{Duration, Instant};

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

use super::delta_config::{DeltaLakeSinkConfig, DeltaWriteMode};
use super::delta_metrics::DeltaLakeSinkMetrics;
use crate::connector::DeliveryGuarantee;

/// Counts `(upserts, deletes)` in a collapsed changelog batch's `_op` column.
/// A row is a delete iff `_op == "D"`; everything else (including a missing or
/// null op) counts as an upsert. Used only for collapse observability.
#[cfg(feature = "delta-lake")]
fn count_collapsed_ops(batch: &RecordBatch) -> (u64, u64) {
    let Ok(idx) = batch.schema().index_of("_op") else {
        return (0, 0);
    };
    let Some(ops) = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
    else {
        return (0, 0);
    };
    let deletes = (0..ops.len())
        .filter(|&i| !ops.is_null(i) && ops.value(i) == "D")
        .count() as u64;
    let upserts = ops.len() as u64 - deletes;
    (upserts, deletes)
}

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
    /// Background reopen kicked off after a checkpoint-boundary drop, so
    /// the next flush doesn't pay the table-load cost on the commit path.
    #[cfg(feature = "delta-lake")]
    pending_reopen: Option<tokio::task::JoinHandle<Result<DeltaTable, ConnectorError>>>,
    /// Pre-built Parquet writer properties for hot-path writes. Built once
    /// in `init_delta_table()` from `config.parquet`; cloning this is far
    /// cheaper than rebuilding (string parsing, bloom-filter column setup)
    /// from scratch on every commit.
    #[cfg(feature = "delta-lake")]
    cached_writer_properties: Option<deltalake::parquet::file::properties::WriterProperties>,
    /// Shared `DataFusion` session for upsert/merge operations. Creating a
    /// fresh `SessionContext` per merge allocated a runtime env, memory
    /// pool, and object-store registry each commit; reusing one flattens
    /// allocator churn under steady-state upsert load.
    #[cfg(feature = "delta-lake")]
    merge_session: Option<datafusion::prelude::SessionContext>,
}

impl DeltaLakeSink {
    /// Creates a new Delta Lake sink with the given configuration.
    #[must_use]
    pub fn new(config: DeltaLakeSinkConfig, registry: Option<&prometheus::Registry>) -> Self {
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
            metrics: DeltaLakeSinkMetrics::new(registry),
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
            #[cfg(feature = "delta-lake")]
            pending_reopen: None,
            #[cfg(feature = "delta-lake")]
            cached_writer_properties: None,
            #[cfg(feature = "delta-lake")]
            merge_session: None,
        }
    }

    /// Creates a new Delta Lake sink with an explicit schema.
    ///
    /// In upsert mode the changelog metadata columns (`_op`, `_ts_ms`,
    /// `__weight`) are stripped, matching the deferred first-write path, so the
    /// target table holds only user data regardless of how the schema arrives.
    #[must_use]
    pub fn with_schema(config: DeltaLakeSinkConfig, schema: SchemaRef) -> Self {
        let write_mode = config.write_mode;
        let mut sink = Self::new(config, None);
        sink.schema = Some(if write_mode == DeltaWriteMode::Upsert {
            Self::target_schema(&schema, write_mode)
        } else {
            schema
        });
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
        let (resolved_path, mut merged_options) = delta_io::resolve_catalog_options(
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

        // Resolve last committed epoch for exactly-once recovery. In coordinated
        // mode nothing is ever committed under writer_id (only the designated
        // committer commits, under COORDINATED_COMMITTER_ID), so we must read the
        // committer's txn id or recovery always sees 0 and re-stages sealed epochs
        // into orphan Parquet.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            let recovery_id = if self.is_coordinated() {
                COORDINATED_COMMITTER_ID
            } else {
                self.config.writer_id.as_str()
            };
            self.last_committed_epoch =
                delta_io::get_last_committed_epoch(&table, recovery_id).await;
            if self.last_committed_epoch > 0 {
                info!(
                    recovery_id,
                    last_committed_epoch = self.last_committed_epoch,
                    "recovered last committed epoch from Delta Lake txn metadata"
                );
            }
        }

        // Store the Delta version.
        #[allow(clippy::cast_sign_loss)]
        {
            self.delta_version = table.version().unwrap_or(0) as u64;
        }
        self.table = Some(table);

        // Pre-build caches used on every commit. Rebuilding WriterProperties
        // from config strings per commit was pure churn (HashMap<ColumnPath>
        // cloned for bloom filters, string lowercase allocs); a cached value
        // clones cheaply. Similarly, a shared SessionContext avoids
        // allocating a new RuntimeEnv + MemoryPool + ObjectStoreRegistry
        // per merge — a significant source of allocator fragmentation on
        // long-running upsert streams.
        self.cached_writer_properties = self.config.parquet.to_writer_properties().ok();
        if self.config.write_mode == DeltaWriteMode::Upsert {
            self.merge_session = Some(datafusion::prelude::SessionContext::new());
        }

        // Spawn background compaction task if enabled. Pre-build the
        // compaction writer properties once; the loop clones per tick
        // instead of re-parsing config strings.
        if self.config.compaction.enabled {
            let cancel = tokio_util::sync::CancellationToken::new();
            let compaction_props = self.config.parquet.compaction_writer_properties().ok();
            let handle = tokio::spawn(compaction_loop(
                resolved_path.clone(),
                Arc::new(merged_options),
                self.config.compaction.clone(),
                self.config.vacuum_retention,
                compaction_props,
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

    /// Changelog metadata columns stripped from the target Delta table schema
    /// in upsert mode, so the table holds only user data — not the CDC `_op`/
    /// `_ts_ms` envelope or the Z-set `__weight` column. `collapse_changelog`
    /// strips the same columns from the MERGE source, keeping the two in sync.
    const CHANGELOG_METADATA_COLUMNS: &'static [&'static str] =
        &["_op", "_ts_ms", laminar_core::changelog::WEIGHT_COLUMN];

    fn target_schema(batch_schema: &SchemaRef, write_mode: DeltaWriteMode) -> SchemaRef {
        if write_mode == DeltaWriteMode::Upsert {
            let fields: Vec<_> = batch_schema
                .fields()
                .iter()
                .filter(|f| !Self::CHANGELOG_METADATA_COLUMNS.contains(&f.name().as_str()))
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

    /// Spawn a background reopen of the Delta table so the next flush
    /// doesn't pay the load cost on its hot path. An already-in-flight
    /// reopen is aborted — the fresher one supersedes it.
    #[cfg(feature = "delta-lake")]
    fn schedule_background_reopen(&mut self) {
        if let Some(prev) = self.pending_reopen.take() {
            prev.abort();
        }
        let path = self.resolved_table_path.clone();
        let opts = self.resolved_storage_options.clone();
        let schema = self.schema.clone();
        self.pending_reopen = Some(tokio::spawn(async move {
            super::delta_io::open_or_create_table(&path, opts, schema.as_ref()).await
        }));
    }

    /// Install a previously-scheduled background reopen. Returns `false`
    /// on miss — no pending reopen, task failure, or timeout — so callers
    /// fall through to the synchronous `reopen_table()` path.
    #[cfg(feature = "delta-lake")]
    async fn try_install_pending_reopen(&mut self, timeout: std::time::Duration) -> bool {
        let Some(mut pending) = self.pending_reopen.take() else {
            return false;
        };
        let table = match tokio::time::timeout(timeout, &mut pending).await {
            Ok(Ok(Ok(t))) => t,
            Ok(Ok(Err(e))) => {
                warn!(error = %e, "Delta background reopen failed");
                return false;
            }
            Ok(Err(e)) => {
                warn!(error = %e, "Delta background reopen task ended unexpectedly");
                return false;
            }
            Err(_) => {
                warn!(
                    timeout_secs = timeout.as_secs(),
                    "Delta background reopen timed out"
                );
                pending.abort();
                return false;
            }
        };
        #[allow(clippy::cast_sign_loss)]
        {
            self.delta_version = table.version().unwrap_or(0) as u64;
        }
        self.table = Some(table);
        true
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
            // flush_staged_to_delta pre-concats for upsert so retries don't
            // pay a full O(rows × cols) copy each attempt. Handle len > 1
            // defensively in case a future caller skips the pre-concat.
            let combined = if batches.len() == 1 {
                batches.into_iter().next().expect("len == 1 checked")
            } else {
                match arrow_select::concat::concat_batches(&batches[0].schema(), &batches) {
                    Ok(c) => c,
                    Err(e) => {
                        // Concat is a local op — restore table and propagate.
                        self.table = Some(table);
                        return Err(ConnectorError::Internal(format!(
                            "failed to concat batches: {e}"
                        )));
                    }
                }
            };

            // Cached in init_delta_table — cloning is a small HashMap copy
            // plus a handful of Arc bumps, far cheaper than rebuilding from
            // config strings each attempt.
            let writer_props = self.cached_writer_properties.clone();
            let merge_session = self
                .merge_session
                .as_ref()
                .expect("merge_session built in init_delta_table for Upsert mode");

            super::delta_io::merge_changelog(
                table,
                combined,
                &self.config.merge_key_columns,
                &self.config.writer_id,
                self.current_epoch,
                self.config.schema_evolution,
                writer_props,
                merge_session,
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
                self.cached_writer_properties.clone(),
            )
            .await
            .map(|(t, _version)| t)
        }
    }

    /// Writes all staged data to Delta Lake as a single atomic transaction.
    ///
    /// Append + exactly-once decomposes into distributed write + one commit.
    fn is_coordinated(&self) -> bool {
        self.config.write_mode == DeltaWriteMode::Append
            && self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
    }

    /// Write staged batches to Parquet (no commit) and serialize the resulting
    /// `Add` actions as the designated committer's descriptor.
    #[cfg(feature = "delta-lake")]
    async fn write_staged_to_descriptor(&self) -> Result<Vec<u8>, ConnectorError> {
        use deltalake::writer::{DeltaWriter, RecordBatchWriter};

        let table = self
            .table
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "delta table not loaded".into(),
            })?;
        let mut writer = RecordBatchWriter::for_table(table)
            .map_err(|e| ConnectorError::WriteError(format!("delta writer: {e}")))?;
        // Mirror the non-coordinated path: honor configured Parquet properties
        // (compression, row-group size, bloom filters) instead of the writer's
        // hard-coded Snappy default.
        if let Some(props) = self.cached_writer_properties.clone() {
            writer = writer.with_writer_properties(props);
        }
        for batch in &self.staged_batches {
            writer
                .write(batch.clone())
                .await
                .map_err(|e| ConnectorError::WriteError(format!("delta write: {e}")))?;
        }
        let adds = writer
            .flush()
            .await
            .map_err(|e| ConnectorError::WriteError(format!("delta flush: {e}")))?;
        super::delta_io::encode_commit_descriptor(adds)
    }

    /// Retries on optimistic concurrency conflicts with exponential backoff.
    /// On non-conflict errors or exhausted retries, propagates the error.
    #[cfg(feature = "delta-lake")]
    #[allow(clippy::too_many_lines)]
    async fn flush_staged_to_delta(&mut self) -> Result<WriteResult, ConnectorError> {
        if self.staged_batches.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }

        // For upsert, concat the whole epoch and collapse the changelog to one
        // row per merge key BEFORE the MERGE. This (a) makes the MERGE
        // cardinality-safe — delta-rs rejects multiple source rows matching one
        // target row, which every aggregate retract+insert would otherwise
        // trigger — and (b) strips the Z-set `__weight` column the MERGE does
        // not understand. Pre-concatenating also means conflict retries don't
        // redo the O(rows × cols) copy each attempt. Append/overwrite passes
        // the Vec straight to delta-rs, which handles multi-batch internally.
        if self.config.write_mode == DeltaWriteMode::Upsert {
            let combined = if self.staged_batches.len() == 1 {
                self.staged_batches[0].clone()
            } else {
                let schema = self.staged_batches[0].schema();
                arrow_select::concat::concat_batches(&schema, &self.staged_batches).map_err(
                    |e| ConnectorError::Internal(format!("failed to concat staged batches: {e}")),
                )?
            };
            let rows_in = combined.num_rows() as u64;
            let collapse_start = Instant::now();
            let collapsed =
                crate::changelog::collapse_changelog(&combined, &self.config.merge_key_columns)?;
            let (upserts_out, deletes_out) = count_collapsed_ops(&collapsed);
            self.metrics.observe_collapse(
                rows_in,
                upserts_out,
                deletes_out,
                collapse_start.elapsed().as_secs_f64(),
            );
            self.staged_batches.clear();
            self.staged_batches.push(collapsed);
        }

        let total_rows = self.staged_rows;
        let estimated_bytes = self.staged_bytes;
        let flush_start = Instant::now();

        // Retry loop with exponential backoff for optimistic concurrency conflicts.
        let backoff_ms = [100u64, 500, 2000];
        let max_attempts = (self.config.max_commit_retries as usize).saturating_add(1);
        let mut last_error: Option<ConnectorError> = None;

        for attempt in 0..max_attempts {
            // delta-rs consumes DeltaTable by value. If self.table is None
            // (prior failure or checkpoint-boundary drop) reinstall it —
            // prefer a scheduled background reopen, fall back to sync.
            if self.table.is_none() {
                let reopen_timeout = self.config.write_timeout;
                if !self.try_install_pending_reopen(reopen_timeout).await {
                    tokio::time::timeout(reopen_timeout, self.reopen_table())
                        .await
                        .map_err(|_| {
                            ConnectorError::ConnectionFailed(format!(
                                "Delta table reopen timed out after {}s",
                                reopen_timeout.as_secs()
                            ))
                        })??;
                }
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
                    #[allow(clippy::cast_sign_loss)]
                    {
                        self.delta_version = table.version().unwrap_or(0) as u64;
                    }

                    // delta-rs' in-memory Snapshot grows per commit and is not
                    // compacted in place; drop on checkpoint boundaries so the
                    // next flush re-opens from the checkpoint file. Pre-open
                    // in the background so the next commit doesn't block.
                    let crossed_checkpoint = self.config.checkpoint_interval > 0
                        && self.delta_version > 0
                        && self
                            .delta_version
                            .is_multiple_of(self.config.checkpoint_interval);
                    if crossed_checkpoint {
                        self.table = None;
                        self.schedule_background_reopen();
                    } else {
                        self.table = Some(table);
                    }

                    self.staged_batches.clear();
                    self.staged_rows = 0;
                    self.staged_bytes = 0;

                    self.metrics
                        .record_flush(total_rows as u64, estimated_bytes);
                    self.metrics.record_commit(self.delta_version);
                    self.metrics
                        .observe_flush_duration(flush_start.elapsed().as_secs_f64());

                    debug!(
                        rows = total_rows,
                        bytes = estimated_bytes,
                        delta_version = self.delta_version,
                        attempt = attempt + 1,
                        reopened = crossed_checkpoint,
                        "Delta Lake: committed staged data to Delta"
                    );

                    return Ok(WriteResult::new(total_rows, estimated_bytes));
                }
                Err(e) => {
                    if Self::is_conflict_error(&e) && attempt + 1 < max_attempts {
                        self.metrics.record_conflict();
                        self.metrics.record_retry();
                        // ±25% jitter breaks up lockstep retries from
                        // concurrent writers colliding on the same version.
                        let base = backoff_ms.get(attempt).copied().unwrap_or(2000);
                        let delay_ms = jittered_backoff_ms(base);
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
                    self.metrics
                        .observe_flush_duration(flush_start.elapsed().as_secs_f64());
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

        let insert_batch = filter_and_project(batch, insert_mask, &user_col_indices)?;
        let delete_batch = filter_and_project(batch, delete_mask, &user_col_indices)?;

        Ok((insert_batch, delete_batch))
    }
}

/// Background compaction loop: OPTIMIZE + VACUUM with adaptive intervals.
///
/// Opens its own `DeltaTable` handle (no shared state with the sink).
#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_lines)]
async fn compaction_loop(
    table_path: String,
    storage_options: Arc<std::collections::HashMap<String, String>>,
    config: super::delta_config::CompactionConfig,
    vacuum_retention: std::time::Duration,
    compaction_props: Option<deltalake::parquet::file::properties::WriterProperties>,
    cancel: tokio_util::sync::CancellationToken,
) {
    use super::delta_io;

    /// Minimum adaptive compaction interval (floor).
    const MIN_COMPACTION_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

    let base_interval = config.check_interval;
    let mut current_interval = base_interval;
    let mut consecutive_skips: u32 = 0;

    info!(
        table_path = %table_path,
        check_interval_secs = base_interval.as_secs(),
        "compaction background task started (adaptive interval)"
    );

    // Initial delay before the first check.
    tokio::select! {
        () = cancel.cancelled() => {
            info!("compaction background task cancelled");
            return;
        }
        () = tokio::time::sleep(current_interval) => {}
    }

    loop {
        // Open a fresh table handle for compaction (no shared state).
        let table =
            match delta_io::open_or_create_table(&table_path, (*storage_options).clone(), None)
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    warn!(error = %e, "compaction: failed to open table, will retry");
                    tokio::select! {
                        () = cancel.cancelled() => {
                            info!("compaction background task cancelled");
                            return;
                        }
                        () = tokio::time::sleep(current_interval) => {}
                    }
                    continue;
                }
            };

        // Check if compaction is needed.
        let should_compact = match table.snapshot() {
            Ok(snapshot) => {
                let file_count = snapshot.log_data().num_files();
                if file_count < config.min_files_for_compaction {
                    debug!(
                        file_count,
                        min = config.min_files_for_compaction,
                        "compaction: skipping, not enough files"
                    );
                    false
                } else {
                    true
                }
            }
            Err(e) => {
                warn!(error = %e, "compaction: snapshot failed, skipping tick");
                false
            }
        };

        if should_compact {
            consecutive_skips = 0;
            // Speed up: halve the interval (floor at MIN_COMPACTION_INTERVAL).
            current_interval = (current_interval / 2).max(MIN_COMPACTION_INTERVAL);

            let target_size = config.target_file_size as u64;
            // Clone the pre-built properties (cheap) instead of re-parsing
            // from config strings each tick.
            match delta_io::run_compaction(
                table,
                target_size,
                &config.z_order_columns,
                compaction_props.clone(),
            )
            .await
            {
                Ok((table, result)) => {
                    debug!(
                        files_added = result.files_added,
                        files_removed = result.files_removed,
                        interval_secs = current_interval.as_secs(),
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
        } else {
            consecutive_skips = consecutive_skips.saturating_add(1);
            // Slow down: double the interval after 2 consecutive idle ticks.
            if consecutive_skips >= 2 {
                current_interval = (current_interval * 2).min(base_interval);
            }
        }

        // Adaptive sleep: wait current_interval or cancel.
        tokio::select! {
            () = cancel.cancelled() => {
                info!("compaction background task cancelled");
                return;
            }
            () = tokio::time::sleep(current_interval) => {}
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
                self.state = ConnectorState::Initializing;
                return Ok(());
            }

            self.init_delta_table().await?;

            // If table still has no version after init (new table, no schema yet),
            // defer full creation to the first write_batch() when schema is available.
            if self.table.as_ref().is_some_and(|t| t.version().is_none()) && self.schema.is_none() {
                self.needs_deferred_delta_init = true;
                self.state = ConnectorState::Initializing;
                return Ok(());
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

        let num_rows = batch.num_rows();
        let estimated_bytes = Self::estimate_batch_size(batch);

        // Hard cap on combined buffered + staged data. Applies to both
        // delivery guarantees:
        // - Exactly-once cannot flush opportunistically from write_batch
        //   (a mid-epoch flush would leak rows on rollback), so pending
        //   accumulates until the next checkpoint.
        // - At-least-once normally auto-flushes via `should_flush()`, but
        //   if flushes fail repeatedly, `staged_batches` holds data while
        //   `buffer` keeps growing — without this cap the sink can OOM.
        //
        // A single incoming batch may itself exceed the cap; we cannot
        // split or reject it (the rows would be lost), so we widen the
        // cap to `num_rows`/`estimated_bytes` when a batch alone is
        // larger. This still rejects the *next* batch if that one-shot
        // admission left us bloated.
        let pending_rows = self.buffered_rows + self.staged_rows + num_rows;
        let pending_bytes = self.buffered_bytes + self.staged_bytes + estimated_bytes;
        let row_cap = self
            .config
            .max_buffer_records
            .saturating_mul(4)
            .max(num_rows);
        let byte_cap = (self.config.target_file_size as u64)
            .saturating_mul(4)
            .max(estimated_bytes);
        if pending_rows > row_cap || pending_bytes > byte_cap {
            return Err(ConnectorError::WriteError(format!(
                "delta sink buffer full ({pending_rows} rows, \
                 {pending_bytes} bytes pending; cap {row_cap} rows, \
                 {byte_cap} bytes) — backpressure until next flush/commit"
            )));
        }

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

    async fn pre_commit(&mut self, epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(None);
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

        // Coordinated (append) mode: write Parquet now and hand the Add actions
        // to the designated committer; the catalog commit happens in
        // commit_aggregated. Upsert/at-least-once keep the commit_epoch path.
        #[cfg(feature = "delta-lake")]
        if self.is_coordinated() {
            if self.staged_batches.is_empty() {
                return Ok(None);
            }
            // Same timeout the commit path uses: a stale object-store connection
            // must not hang the sink task forever (Azure LB drops idle conns).
            let write_timeout = self.config.write_timeout;
            let descriptor = tokio::time::timeout(write_timeout, self.write_staged_to_descriptor())
                .await
                .map_err(|_| {
                    ConnectorError::WriteError(format!(
                        "Delta write timed out after {}s",
                        write_timeout.as_secs()
                    ))
                })??;
            self.staged_batches.clear();
            self.staged_rows = 0;
            self.staged_bytes = 0;
            return Ok(Some(descriptor));
        }

        debug!(epoch, "Delta Lake: pre-committed (batches staged)");
        Ok(None)
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Skip if already committed (exactly-once idempotency).
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && epoch <= self.last_committed_epoch
        {
            return Ok(());
        }

        // Write staged data to Delta as a single atomic transaction. Coordinated
        // (append) sinks commit via the designated committer (commit_aggregated),
        // not here.
        #[cfg(feature = "delta-lake")]
        {
            if !self.is_coordinated() && !self.staged_batches.is_empty() {
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

    fn capabilities(&self) -> SinkConnectorCapabilities {
        // Delta commits can run long under compaction or contention.
        let mut caps = SinkConnectorCapabilities::new(Duration::from_secs(180)).with_idempotent();

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            caps = caps.with_exactly_once().with_two_phase_commit();
        }
        // Append is the only mode that decomposes into distributed write +
        // single commit; upsert (MERGE) keeps the per-writer path.
        if self.is_coordinated() {
            caps = caps.with_coordinated_commit();
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
        // Coordinated sinks are committed by the designated committer, not on
        // close; any un-checkpointed buffer is dropped and reprocessed on restart.
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            self.flush().await?;
        } else if !self.is_coordinated() && !self.buffer.is_empty() {
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

        #[cfg(feature = "delta-lake")]
        if let Some(pending) = self.pending_reopen.take() {
            pending.abort();
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

    #[cfg(feature = "delta-lake")]
    fn as_coordinated_committer(&self) -> Option<&dyn crate::connector::CoordinatedCommitter> {
        self.is_coordinated()
            .then_some(self as &dyn crate::connector::CoordinatedCommitter)
    }
}

/// Single app-transaction id the designated committer uses for idempotency; it
/// is scoped to the table's own log, so a constant is sufficient.
#[cfg(feature = "delta-lake")]
const COORDINATED_COMMITTER_ID: &str = "laminardb.coordinated";

#[cfg(feature = "delta-lake")]
#[async_trait]
impl crate::connector::CoordinatedCommitter for DeltaLakeSink {
    async fn commit_aggregated(
        &self,
        epoch: u64,
        descriptors: Vec<Vec<u8>>,
    ) -> Result<(), ConnectorError> {
        let table = super::delta_io::open_or_create_table(
            &self.resolved_table_path,
            self.resolved_storage_options.clone(),
            None,
        )
        .await?;
        // Idempotent: skip if this epoch is already sealed for the committer.
        if super::delta_io::get_last_committed_epoch(&table, COORDINATED_COMMITTER_ID).await
            >= epoch
        {
            return Ok(());
        }
        let adds = super::delta_io::decode_commit_descriptors(&descriptors)?;
        if adds.is_empty() {
            return Ok(());
        }
        super::delta_io::commit_adds_coordinated(&table, adds, COORDINATED_COMMITTER_ID, epoch)
            .await?;
        info!(
            epoch,
            writers = descriptors.len(),
            "delta coordinated commit"
        );
        Ok(())
    }

    async fn committed_through(&self) -> Result<Option<u64>, ConnectorError> {
        let table = super::delta_io::open_or_create_table(
            &self.resolved_table_path,
            self.resolved_storage_options.clone(),
            None,
        )
        .await?;
        let epoch =
            super::delta_io::get_last_committed_epoch(&table, COORDINATED_COMMITTER_ID).await;
        Ok((epoch > 0).then_some(epoch))
    }
}

/// Safety-net: if the sink is dropped mid-lifecycle (panic, config error,
/// shutdown without calling `close()`), cancel any background work so we
/// don't leak a compaction loop or a stray table-reopen task.
#[cfg(feature = "delta-lake")]
impl Drop for DeltaLakeSink {
    fn drop(&mut self) {
        if let Some(cancel) = self.compaction_cancel.take() {
            cancel.cancel();
        }
        if let Some(handle) = self.compaction_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.pending_reopen.take() {
            handle.abort();
        }
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

/// Applies ±25% jitter to a backoff delay in milliseconds.
///
/// Without jitter, multiple writers colliding on the same Delta version
/// retry in lockstep and keep colliding — a thundering herd. The 0.75–1.25
/// range spreads retries across a 500ms window for the default 2s max.
#[cfg(feature = "delta-lake")]
fn jittered_backoff_ms(base_ms: u64) -> u64 {
    use rand::RngExt as _;
    let factor: f64 = rand::rng().random_range(0.75_f64..=1.25_f64);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let jittered = (base_ms as f64 * factor) as u64;
    jittered.max(1)
}

/// Filters a `RecordBatch` using a boolean mask and projects to the given column indices.
///
/// Takes `mask` by value to hand it straight to `BooleanArray::from` without
/// an intermediate `Vec<bool>` copy. Projects before filtering so the SIMD
/// kernel only walks user columns, not the dropped metadata columns.
fn filter_and_project(
    batch: &RecordBatch,
    mask: Vec<bool>,
    col_indices: &[usize],
) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::BooleanArray;
    use arrow_select::filter::filter_record_batch;

    let bool_array = BooleanArray::from(mask);

    let projected = batch
        .project(col_indices)
        .map_err(|e| ConnectorError::Internal(format!("batch projection failed: {e}")))?;

    filter_record_batch(&projected, &bool_array)
        .map_err(|e| ConnectorError::Internal(format!("arrow filter failed: {e}")))
}

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::float_cmp)]
mod tests;
