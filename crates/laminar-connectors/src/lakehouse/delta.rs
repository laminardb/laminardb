//! Delta Lake sink connector implementation.
//!
//! [`DeltaLakeSink`] implements [`SinkConnector`], writing Arrow `RecordBatch`
//! data to Delta Lake tables with ACID transactions and at-least-once delivery
//! (exactly-once is selected once at the pipeline/runtime boundary).
//!
//! # Write Strategies
//!
//! - **Append mode**: Arrow-to-Parquet zero-copy writes for immutable streams
//! - **Overwrite mode**: Replace partition contents for recomputation
//! - **Upsert mode**: CDC MERGE via Z-set changelog integration
//!
//! Exactly-once append writes prepare immutable Parquet descriptors during the
//! checkpoint and publish them with one namespaced, designated Delta commit.
//! At-least-once append, overwrite, and upsert writes commit through `flush()`.
//!
//! # Ring Architecture
//!
//! - **Ring 0**: No sink code. Data arrives via SPSC channel (~5ns push).
//! - **Ring 1**: Batch buffering, Parquet writes, Delta log commits.
//! - **Ring 2**: Schema management, configuration, health checks.

#[cfg(feature = "delta-lake")]
use std::future::Future;
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
#[cfg(feature = "delta-lake")]
use crate::connector::{ConnectorTaskGuard, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;

use super::delta_config::{DeltaLakeSinkConfig, DeltaWriteMode};
use super::delta_metrics::DeltaLakeSinkMetrics;
use crate::connector::DeliveryGuarantee;

#[cfg(feature = "delta-lake")]
async fn run_tracked_delta_task<F>(guard: ConnectorTaskGuard, task: F) -> F::Output
where
    F: Future,
{
    let _guard = guard;
    task.await
}

#[cfg(feature = "delta-lake")]
fn classify_delta_attempt_error(error: super::delta_io::DeltaWriteAttemptError) -> ConnectorError {
    use super::delta_io::DeltaWriteAttemptError;

    if error.is_definite_optimistic_conflict() {
        return ConnectorError::WriteError(format!(
            "Delta optimistic commit collision did not publish: {error}"
        ));
    }

    match error {
        DeltaWriteAttemptError::Local(error) => error,
        DeltaWriteAttemptError::Delta(error) => {
            // A storage failure may make progress in a fresh generation, but
            // it still cannot prove whether the catalog accepted the commit.
            // Structural/protocol errors are terminal and must not be turned
            // into retries merely because their message says "conflict".
            let retryable = super::delta_io::delta_error_has_retryable_transport(&error);
            ConnectorError::outcome_unknown(
                format!(
                    "Delta write was dispatched but its catalog commit outcome is not known: {error}"
                ),
                retryable,
            )
        }
    }
}

#[cfg(feature = "delta-lake")]
struct DeltaWriteTaskSuccess {
    table: DeltaTable,
    merge_result: Option<super::delta_io::MergeResult>,
}

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

#[cfg(feature = "delta-lake")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedDeltaPublication {
    external_key: String,
    target: crate::connector::CoordinatedCommitCursor,
    exact_batch_fingerprint: [u8; 32],
}

#[cfg(feature = "delta-lake")]
impl UnresolvedDeltaPublication {
    fn reconciled_by(&self, observed: Option<crate::connector::CoordinatedCommitCursor>) -> bool {
        observed == Some(self.target)
    }
}

#[cfg(feature = "delta-lake")]
async fn publish_coordinated_delta_batch(
    table_path: String,
    storage_options: std::collections::HashMap<String, String>,
    unresolved: Arc<parking_lot::Mutex<Option<UnresolvedDeltaPublication>>>,
    pending: UnresolvedDeltaPublication,
    batch: crate::connector::CoordinatedCommitBatch,
    deadline: tokio::time::Instant,
    publication_budget: Duration,
) -> Result<(), ConnectorError> {
    super::delta_io::validate_coordinated_storage_preflight(&table_path, &storage_options)?;
    let storage_options = super::delta_io::bound_coordinated_storage_options(storage_options);
    let result = async {
        let table = tokio::time::timeout_at(
            deadline,
            super::delta_io::open_or_create_table(&table_path, storage_options, None),
        )
        .await
        .map_err(|_| {
            ConnectorError::TransactionError(
                "Delta coordinated table open exceeded the publication deadline".into(),
            )
        })??;
        let descriptor_count =
            super::delta_io::commit_batch_coordinated(&table, &batch, deadline).await?;
        info!(
            epoch = batch.target.epoch,
            checkpoint_id = batch.target.checkpoint_id,
            descriptors = descriptor_count,
            "delta coordinated commit"
        );
        Ok(())
    }
    .await;

    if result.is_ok() && tokio::time::Instant::now() < deadline {
        let mut unresolved = unresolved.lock();
        if unresolved.as_ref() == Some(&pending) {
            *unresolved = None;
        }
        return Ok(());
    }
    if result.is_ok() {
        return Err(ConnectorError::outcome_unknown(
            format!(
                "Delta coordinated publication exceeded its {publication_budget:?} remaining \
                 budget; reconcile the exact cursor before replay"
            ),
            true,
        ));
    }
    result
}

/// Delta Lake sink connector.
///
/// Writes Arrow `RecordBatch` to Delta Lake tables with ACID transactions,
/// at-least-once delivery (exactly-once opt-in), partitioning, and
/// externally managed table maintenance.
///
/// # Exactly-Once Semantics
///
/// `pre_commit()` materializes immutable Parquet files and returns their Delta
/// `Add` actions. The runtime durably seals the checkpoint before one
/// designated committer calls `commit_aggregated()` with every participant's
/// descriptor. `rollback_epoch()` discards in-memory state; unreferenced files
/// are reclaimed later by retention-safe vacuum.
pub struct DeltaLakeSink {
    /// Sole admission authority for Delta tasks that may outlive a cancelled caller.
    #[cfg(feature = "delta-lake")]
    task_owner: ConnectorTaskOwner,
    /// Stable terminal observer retained by the runtime after this sink is retired.
    task_tracker: ConnectorTaskTracker,
    /// Sink configuration.
    config: DeltaLakeSinkConfig,
    /// Arrow schema for input batches (set on first write or from existing table).
    schema: Option<SchemaRef>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Current epoch being written.
    current_epoch: u64,
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
    /// Staged batches ready for either coordinated descriptor creation or an
    /// at-least-once flush. This separation lets `rollback_epoch()` discard
    /// prepared in-memory data without publishing it to the Delta log.
    staged_batches: Vec<RecordBatch>,
    /// Rows staged for commit (mirrors `staged_batches`).
    staged_rows: usize,
    /// Estimated bytes staged for commit.
    staged_bytes: u64,
    /// Uncommitted `Add` actions for immutable Parquet files materialized in
    /// the current coordinated epoch.
    #[cfg(feature = "delta-lake")]
    coordinated_adds: Vec<deltalake::kernel::Add>,
    /// Canonical table incarnation and write-metadata fingerprint captured
    /// from the exact immutable snapshot used to create `coordinated_adds`.
    #[cfg(feature = "delta-lake")]
    coordinated_binding: Option<super::commit_descriptor::DeltaTableBinding>,
    /// Exact encoded descriptor size for `coordinated_adds`, or zero when the
    /// vector is empty.
    #[cfg(feature = "delta-lake")]
    coordinated_descriptor_bytes: usize,
    /// Exact publication that must be retried or observed at its target before
    /// this instance stages a later cut. Absence cannot resolve a timed-out
    /// remote catalog mutation because the server may still complete it.
    #[cfg(feature = "delta-lake")]
    coordinated_unresolved_publication: Arc<parking_lot::Mutex<Option<UnresolvedDeltaPublication>>>,
    /// Resolved table path after catalog lookup (may differ from `config.table_path`
    /// when using Unity/Glue catalogs). Used by `reopen_table()` so retries
    /// target the same resolved path that `open()` connected to.
    #[cfg(feature = "delta-lake")]
    resolved_table_path: String,
    /// Resolved storage options after catalog lookup.
    #[cfg(feature = "delta-lake")]
    resolved_storage_options: std::collections::HashMap<String, String>,
    /// When true, Delta table init is deferred until the first `write_batch()`
    /// provides a schema. This happens when Unity Catalog auto-create is
    /// configured but the pipeline schema is not yet available at `open()` time.
    #[cfg(feature = "delta-lake")]
    needs_deferred_delta_init: bool,
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
    /// A dispatched Delta operation lost its result, so this generation cannot
    /// safely admit another write even after its detached task terminates.
    #[cfg(feature = "delta-lake")]
    unresolved_delta_write: bool,
    /// Test hook that makes coordinated descriptor preparation remain pending.
    /// This is compiled out of production builds.
    #[cfg(all(test, feature = "delta-lake"))]
    stall_descriptor_write: bool,
}

impl DeltaLakeSink {
    /// Creates a new Delta Lake sink with the given configuration.
    #[must_use]
    pub fn new(config: DeltaLakeSinkConfig, registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        #[cfg(not(feature = "delta-lake"))]
        let _ = task_owner;
        Self {
            #[cfg(feature = "delta-lake")]
            task_owner,
            task_tracker,
            config,
            schema: None,
            state: ConnectorState::Created,
            current_epoch: 0,
            buffer: Vec::with_capacity(16),
            buffered_rows: 0,
            buffered_bytes: 0,
            delta_version: 0,
            buffer_start_time: None,
            metrics: DeltaLakeSinkMetrics::new(registry),
            staged_batches: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
            #[cfg(feature = "delta-lake")]
            coordinated_adds: Vec::new(),
            #[cfg(feature = "delta-lake")]
            coordinated_binding: None,
            #[cfg(feature = "delta-lake")]
            coordinated_descriptor_bytes: 0,
            #[cfg(feature = "delta-lake")]
            coordinated_unresolved_publication: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "delta-lake")]
            table: None,
            #[cfg(feature = "delta-lake")]
            resolved_table_path: String::new(),
            #[cfg(feature = "delta-lake")]
            resolved_storage_options: std::collections::HashMap::new(),
            #[cfg(feature = "delta-lake")]
            needs_deferred_delta_init: false,
            #[cfg(feature = "delta-lake")]
            cached_writer_properties: None,
            #[cfg(feature = "delta-lake")]
            merge_session: None,
            #[cfg(feature = "delta-lake")]
            unresolved_delta_write: false,
            #[cfg(all(test, feature = "delta-lake"))]
            stall_descriptor_write: false,
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
    /// resolves the catalog path, and opens or creates the Delta table. Called
    /// from `open()` or deferred to the first
    /// `write_batch()` when the schema is not yet available at open time.
    #[cfg(feature = "delta-lake")]
    async fn init_delta_table(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        use super::delta_io;

        // For uc:// tables, pre-create in Unity Catalog if needed.
        // Must run before resolve_catalog_options which calls GET on the table.
        #[cfg(feature = "delta-lake-unity")]
        tokio::time::timeout_at(
            deadline,
            ensure_uc_table_exists(&self.config, self.schema.as_ref()),
        )
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(
                "Delta Unity table initialization exceeded the write deadline".into(),
            )
        })??;

        // Resolve catalog path: for Unity this calls GET to get the
        // storage_location, bypassing delta-rs credential vending.
        let (resolved_path, mut merged_options) = tokio::time::timeout_at(
            deadline,
            delta_io::resolve_catalog_options(
                &self.config.catalog_type,
                self.config.catalog_database.as_deref(),
                self.config.catalog_name.as_deref(),
                self.config.catalog_schema.as_deref(),
                &self.config.table_path,
                &self.config.storage_options,
            ),
        )
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(
                "Delta catalog resolution exceeded the write deadline".into(),
            )
        })??;

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            delta_io::validate_coordinated_storage_preflight(&resolved_path, &merged_options)?;
        }

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

        let table = tokio::time::timeout_at(
            deadline,
            delta_io::open_or_create_table(
                &resolved_path,
                merged_options.clone(),
                self.schema.as_ref(),
            ),
        )
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "Delta table initialization exceeded the {:?} write deadline",
                self.config.write_timeout
            ))
        })??;

        // Read schema from existing table if we don't have one.
        if self.schema.is_none() {
            if let Ok(schema) = delta_io::get_table_schema(&table) {
                self.schema = Some(schema);
            }
        }

        self.delta_version = table
            .version()
            .and_then(|version| u64::try_from(version).ok())
            .unwrap_or(0);
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

    #[cfg(feature = "delta-lake")]
    fn operation_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.config.write_timeout
    }

    #[cfg(feature = "delta-lake")]
    fn ensure_write_generation_usable(&self) -> Result<(), ConnectorError> {
        if self.unresolved_delta_write {
            return Err(ConnectorError::InvalidState {
                expected: "a fresh Delta sink generation after reconciliation".into(),
                actual: "a prior dispatched Delta operation lost its result".into(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "delta-lake")]
    fn spawn_tracked_delta_task<F>(
        &self,
        task: F,
    ) -> Result<tokio::task::JoinHandle<F::Output>, ConnectorError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let guard = self.task_owner.track().ok_or(ConnectorError::Closed)?;
        Ok(tokio::spawn(run_tracked_delta_task(guard, task)))
    }

    /// Re-opens the Delta Lake table after a failed write consumes the handle.
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

        self.delta_version = table
            .version()
            .and_then(|version| u64::try_from(version).ok())
            .unwrap_or(0);
        self.table = Some(table);
        Ok(())
    }

    /// Attempts a single Delta write/merge and returns the updated table on success.
    #[cfg(feature = "delta-lake")]
    async fn attempt_delta_write(
        table: DeltaTable,
        batches: Vec<RecordBatch>,
        write_mode: DeltaWriteMode,
        merge_key_columns: Vec<String>,
        partition_columns: Vec<String>,
        schema_evolution: bool,
        target_file_size: usize,
        writer_properties: Option<deltalake::parquet::file::properties::WriterProperties>,
        merge_session: Option<datafusion::prelude::SessionContext>,
    ) -> Result<DeltaWriteTaskSuccess, super::delta_io::DeltaWriteAttemptError> {
        if write_mode == DeltaWriteMode::Upsert {
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
                        return Err(ConnectorError::Internal(format!(
                            "failed to concat batches: {e}"
                        ))
                        .into());
                    }
                }
            };

            let merge_session = merge_session
                .as_ref()
                .expect("merge_session built in init_delta_table for Upsert mode");

            super::delta_io::merge_changelog(
                table,
                combined,
                &merge_key_columns,
                schema_evolution,
                writer_properties,
                merge_session,
            )
            .await
            .map(|(table, merge_result)| DeltaWriteTaskSuccess {
                table,
                merge_result: Some(merge_result),
            })
        } else {
            // ── Append/Overwrite path ──
            let save_mode = match write_mode {
                DeltaWriteMode::Append => SaveMode::Append,
                DeltaWriteMode::Overwrite => SaveMode::Overwrite,
                DeltaWriteMode::Upsert => unreachable!("handled by the upsert branch above"),
            };

            let partition_cols = if partition_columns.is_empty() {
                None
            } else {
                Some(partition_columns.as_slice())
            };

            super::delta_io::write_batches(
                table,
                batches,
                save_mode,
                partition_cols,
                schema_evolution,
                Some(target_file_size),
                writer_properties,
            )
            .await
            .map(|(table, _version)| DeltaWriteTaskSuccess {
                table,
                merge_result: None,
            })
        }
    }

    /// Writes all staged data to Delta Lake as a single atomic transaction.
    ///
    /// Append + exactly-once decomposes into distributed write + one commit.
    fn is_coordinated(&self) -> bool {
        self.config.write_mode == DeltaWriteMode::Append
            && self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
    }

    #[cfg(feature = "delta-lake")]
    fn ensure_coordinated_reconciled(&self) -> Result<(), ConnectorError> {
        if self.is_coordinated() && self.coordinated_unresolved_publication.lock().is_some() {
            return Err(ConnectorError::TransactionError(
                "Delta coordinated publication outcome is unresolved; reconcile or retry the exact cut before processing later work"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Write staged batches to uniquely named Parquet files without making
    /// them visible in the Delta log.
    #[cfg(feature = "delta-lake")]
    async fn materialize_staged_adds(
        table: DeltaTable,
        batches: Vec<RecordBatch>,
        writer_properties: Option<deltalake::parquet::file::properties::WriterProperties>,
        #[cfg(test)] stall: bool,
    ) -> Result<Vec<deltalake::kernel::Add>, ConnectorError> {
        use deltalake::writer::{DeltaWriter, RecordBatchWriter};

        #[cfg(test)]
        if stall {
            std::future::pending::<()>().await;
        }

        let mut writer = RecordBatchWriter::for_table(&table)
            .map_err(|e| ConnectorError::WriteError(format!("delta writer: {e}")))?;
        // Mirror the non-coordinated path: honor configured Parquet properties
        // (compression, row-group size, bloom filters) instead of the writer's
        // hard-coded Snappy default.
        if let Some(props) = writer_properties {
            writer = writer.with_writer_properties(props);
        }
        for batch in batches {
            writer
                .write(batch)
                .await
                .map_err(|e| ConnectorError::WriteError(format!("delta write: {e}")))?;
        }
        let adds = writer
            .flush()
            .await
            .map_err(|e| ConnectorError::WriteError(format!("delta flush: {e}")))?;
        Ok(adds)
    }

    /// Materialize the current exact-mode staging buffer and retain only its
    /// bounded Delta metadata. The Parquet objects remain invisible until
    /// `commit_aggregated` publishes these Adds with the checkpoint cursor.
    #[cfg(feature = "delta-lake")]
    async fn materialize_coordinated_staged(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        if self.staged_batches.is_empty() {
            return Ok(());
        }
        self.ensure_write_generation_usable()?;

        let binding =
            super::delta_io::coordinated_table_binding(self.table.as_ref().ok_or_else(|| {
                ConnectorError::InvalidState {
                    expected: "open".into(),
                    actual: "delta table not loaded".into(),
                }
            })?)?;
        if self
            .coordinated_binding
            .as_ref()
            .is_some_and(|existing| existing != &binding)
        {
            return Err(ConnectorError::TransactionError(
                "Delta table binding changed while materializing one coordinated checkpoint cut"
                    .into(),
            ));
        }

        let table = self
            .table
            .clone()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "delta table not loaded".into(),
            })?;
        let batches = self.staged_batches.clone();
        let writer_properties = self.cached_writer_properties.clone();
        #[cfg(test)]
        let stall = self.stall_descriptor_write;
        let task = self.spawn_tracked_delta_task(Self::materialize_staged_adds(
            table,
            batches,
            writer_properties,
            #[cfg(test)]
            stall,
        ))?;
        let adds = match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(result)) => result?,
            Ok(Err(error)) => {
                self.unresolved_delta_write = true;
                return Err(ConnectorError::outcome_unknown(
                    format!("Delta descriptor writer task terminated unexpectedly: {error}"),
                    true,
                ));
            }
            Err(_) => {
                self.unresolved_delta_write = true;
                return Err(ConnectorError::outcome_unknown(
                    format!(
                        "Delta descriptor materialization timed out at its {:?} end-to-end deadline",
                        self.config.write_timeout
                    ),
                    true,
                ));
            }
        };
        if adds.is_empty() {
            self.staged_batches.clear();
            self.staged_rows = 0;
            self.staged_bytes = 0;
            return Ok(());
        }

        let projected_add_count = self
            .coordinated_adds
            .len()
            .checked_add(adds.len())
            .ok_or_else(|| {
                ConnectorError::WriteError("Delta coordinated Add count overflow".into())
            })?;
        if projected_add_count > super::delta_io::MAX_COORDINATED_ADD_ACTIONS {
            return Err(ConnectorError::WriteError(format!(
                "Delta coordinated checkpoint would exceed the fixed {} Add-action limit",
                super::delta_io::MAX_COORDINATED_ADD_ACTIONS
            )));
        }

        // Account only for the new Add array. Once the binding is encoded, appending another
        // non-empty chunk replaces no envelope fields and inserts one comma between array items.
        let chunk_add_array_bytes = super::delta_io::encoded_add_array_len(&adds)?;
        let projected_descriptor_bytes = if self.coordinated_adds.is_empty() {
            super::delta_io::encode_commit_descriptor(&binding, &adds)?.len()
        } else {
            self.coordinated_descriptor_bytes
                .checked_add(chunk_add_array_bytes)
                .and_then(|bytes| bytes.checked_sub(2))
                .and_then(|bytes| bytes.checked_add(1))
                .ok_or_else(|| {
                    ConnectorError::WriteError("Delta coordinated descriptor size overflow".into())
                })?
        };
        if projected_descriptor_bytes > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
            return Err(ConnectorError::WriteError(format!(
                "Delta coordinated descriptor would exceed the fixed {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} byte control-plane \
                 limit; the checkpoint cut produced too many files or partition values",
            )));
        }

        self.coordinated_binding = Some(binding);
        self.coordinated_adds.extend(adds);
        self.coordinated_descriptor_bytes = projected_descriptor_bytes;
        self.staged_batches.clear();
        self.staged_rows = 0;
        self.staged_bytes = 0;
        Ok(())
    }

    /// Flush any prior retry buffer first, then move the current in-memory
    /// exact-mode buffer into invisible Parquet staging.
    #[cfg(feature = "delta-lake")]
    async fn stage_coordinated_buffer(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.materialize_coordinated_staged(deadline).await?;
        if self.buffer.is_empty() {
            return Ok(());
        }

        self.staged_batches = std::mem::take(&mut self.buffer);
        self.staged_rows = self.buffered_rows;
        self.staged_bytes = self.buffered_bytes;
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;
        self.materialize_coordinated_staged(deadline).await
    }

    /// Writes staged data under one end-to-end deadline. delta-rs owns the
    /// optimistic commit retry loop; this layer must not amplify that budget.
    #[cfg(feature = "delta-lake")]
    #[allow(clippy::too_many_lines)]
    async fn flush_staged_to_delta(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<WriteResult, ConnectorError> {
        if self.staged_batches.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }
        self.ensure_write_generation_usable()?;
        let write_timeout = self.config.write_timeout;

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

        // A failed/expired delta-rs write consumes the table handle. A later
        // flush reopens it, but reopening and the new commit share this one
        // operation deadline.
        if self.table.is_none() {
            tokio::time::timeout_at(deadline, self.reopen_table())
                .await
                .map_err(|_| {
                    ConnectorError::ConnectionFailed(format!(
                        "Delta table reopen exceeded the {write_timeout:?} write deadline"
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

        let write_task = self.spawn_tracked_delta_task(Self::attempt_delta_write(
            table,
            self.staged_batches.clone(),
            self.config.write_mode,
            self.config.merge_key_columns.clone(),
            self.config.partition_columns.clone(),
            self.config.schema_evolution,
            self.config.target_file_size,
            self.cached_writer_properties.clone(),
            self.merge_session.clone(),
        ))?;
        let write = match tokio::time::timeout_at(deadline, write_task).await {
            Ok(Ok(Ok(write))) => write,
            Ok(Ok(Err(error))) => {
                self.metrics
                    .observe_flush_duration(flush_start.elapsed().as_secs_f64());
                let error = classify_delta_attempt_error(error);
                if error.is_outcome_unknown() {
                    self.unresolved_delta_write = true;
                }
                return Err(error);
            }
            Ok(Err(error)) => {
                self.unresolved_delta_write = true;
                self.metrics
                    .observe_flush_duration(flush_start.elapsed().as_secs_f64());
                return Err(ConnectorError::outcome_unknown(
                    format!("Delta writer task terminated unexpectedly: {error}"),
                    true,
                ));
            }
            Err(_) => {
                self.unresolved_delta_write = true;
                self.metrics
                    .observe_flush_duration(flush_start.elapsed().as_secs_f64());
                return Err(ConnectorError::outcome_unknown(
                    format!("Delta write exceeded its {write_timeout:?} end-to-end deadline"),
                    true,
                ));
            }
        };
        let table = write.table;
        if let Some(result) = write.merge_result {
            self.metrics.record_merge();
            if result.rows_deleted > 0 {
                self.metrics.record_deletes(result.rows_deleted as u64);
            }
        }

        self.delta_version = table
            .version()
            .and_then(|version| u64::try_from(version).ok())
            .unwrap_or(0);
        self.table = Some(table);
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
            "Delta Lake: committed staged data to Delta"
        );

        Ok(WriteResult::new(total_rows, estimated_bytes))
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
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let cfg = if config.properties().is_empty() {
            self.config.clone()
        } else {
            DeltaLakeSinkConfig::from_config(config)?
        };
        #[cfg(feature = "delta-lake")]
        if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            super::delta_io::validate_coordinated_storage_preflight(
                &cfg.table_path,
                &cfg.storage_options,
            )?;
        }
        if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && cfg.write_mode != DeltaWriteMode::Append
        {
            return Err(ConnectorError::ConfigurationError(
                "Delta exactly-once requires coordinated append mode".into(),
            ));
        }
        let consistency = if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && cfg.write_mode == DeltaWriteMode::Append
        {
            // Append mode emits durable data-file descriptors for one
            // designated catalog commit. Certification still requires an
            // exact external namespace/cursor, enforced by runtime admission.
            SinkConsistency::CheckpointCommittable
        } else {
            SinkConsistency::DurableAtLeastOnce
        };
        let target = cfg.table_path.to_ascii_lowercase();
        let shared_target = [
            "s3://", "s3a://", "gs://", "gcs://", "az://", "abfs://", "uc://",
        ]
        .iter()
        .any(|scheme| target.starts_with(scheme));
        let topology = if cfg.write_mode == DeltaWriteMode::Append && shared_target {
            SinkTopology::MultiWriter
        } else {
            // Node-local files plus overwrite/MERGE lack fenced distributed ownership.
            SinkTopology::Singleton
        };
        let input_mode = if cfg.write_mode == DeltaWriteMode::Upsert {
            SinkInputMode::FullChangelog
        } else {
            SinkInputMode::AppendOnly
        };
        Ok(SinkContract::new(consistency, topology, input_mode))
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
        if !config.properties().is_empty() {
            self.config = DeltaLakeSinkConfig::from_config(config)?;
        }
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            super::delta_io::validate_coordinated_storage_preflight(
                &self.config.table_path,
                &self.config.storage_options,
            )?;
        }
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && !self.is_coordinated()
        {
            self.state = ConnectorState::Failed;
            return Err(ConnectorError::ConfigurationError(
                "Delta exactly-once requires coordinated append mode".into(),
            ));
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

            let deadline = self.operation_deadline();
            self.init_delta_table(deadline).await?;

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

        #[cfg(feature = "delta-lake")]
        {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
        }
        #[cfg(feature = "delta-lake")]
        let deadline = self.operation_deadline();

        if batch.num_rows() == 0 {
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
            match self.init_delta_table(deadline).await {
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

        // At-least-once retries retain failed staged data, so cap the combined
        // in-memory backlog. Coordinated append instead drains full buffers to
        // invisible Parquet files below and bounds only their retained Adds.
        if !self.is_coordinated() {
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
        }

        // Buffer the batch.
        if self.buffer_start_time.is_none() {
            self.buffer_start_time = Some(Instant::now());
        }
        self.buffer.push(batch.clone());
        self.buffered_rows += num_rows;
        self.buffered_bytes += estimated_bytes;

        #[cfg(feature = "delta-lake")]
        if self.is_coordinated() && self.should_flush() {
            self.stage_coordinated_buffer(deadline).await?;
        } else if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce
            && self.should_flush()
        {
            if !self.staged_batches.is_empty() {
                self.flush_staged_to_delta(deadline).await?;
            }
            self.staged_batches = std::mem::take(&mut self.buffer);
            self.staged_rows = self.buffered_rows;
            self.staged_bytes = self.buffered_bytes;
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.buffer_start_time = None;
            self.flush_staged_to_delta(deadline).await?;
        }

        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        #[cfg(feature = "delta-lake")]
        {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
        }
        #[cfg(feature = "delta-lake")]
        let deadline = self.operation_deadline();

        // Complete deferred Delta table init on the first epoch.
        #[cfg(feature = "delta-lake")]
        if self.needs_deferred_delta_init {
            // Schema may not be available yet on the very first epoch.
            // If so, buffer the epoch — the write_batch will provide it.
            // But if the pipeline provided a schema via with_schema() or a
            // previous epoch's write_batch set it, complete init now.
            if self.schema.is_some() {
                info!("schema available — completing deferred Delta table init");
                match self.init_delta_table(deadline).await {
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

        #[cfg(feature = "delta-lake")]
        if self.is_coordinated()
            && (!self.buffer.is_empty()
                || !self.staged_batches.is_empty()
                || !self.coordinated_adds.is_empty())
        {
            return Err(ConnectorError::InvalidState {
                expected: "the previous coordinated epoch to be prepared or rolled back".into(),
                actual: "unresolved Delta staging data remains".into(),
            });
        }

        self.current_epoch = epoch;
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;

        debug!(epoch, "Delta Lake: began epoch");
        Ok(())
    }

    async fn pre_commit(&mut self, epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        // Coordinated append may already have materialized several invisible
        // Parquet chunks during writes. Flush only the remainder, then return
        // one bounded descriptor for the entire checkpoint cut.
        #[cfg(feature = "delta-lake")]
        if self.is_coordinated() {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
            let deadline = self.operation_deadline();
            self.stage_coordinated_buffer(deadline).await?;
            if self.coordinated_adds.is_empty() {
                if self.coordinated_binding.is_some() {
                    return Err(ConnectorError::Internal(
                        "empty Delta coordinated cut retained a table binding".into(),
                    ));
                }
                return Ok(None);
            }

            let binding = self.coordinated_binding.as_ref().ok_or_else(|| {
                ConnectorError::Internal(
                    "non-empty Delta coordinated cut has no table binding".into(),
                )
            })?;
            let descriptor =
                super::delta_io::encode_commit_descriptor(binding, &self.coordinated_adds)?;
            if descriptor.len() != self.coordinated_descriptor_bytes
                || descriptor.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES
            {
                return Err(ConnectorError::Internal(format!(
                    "Delta coordinated descriptor accounting mismatch: tracked {}, encoded {}",
                    self.coordinated_descriptor_bytes,
                    descriptor.len()
                )));
            }
            self.coordinated_adds.clear();
            self.coordinated_binding = None;
            self.coordinated_descriptor_bytes = 0;
            return Ok(Some(descriptor));
        }

        // Non-coordinated callers retain the existing in-memory phase-one
        // behavior; the checkpoint runtime only invokes this path for sinks
        // whose contract admits it.
        if !self.buffer.is_empty() {
            self.staged_batches = std::mem::take(&mut self.buffer);
            self.staged_rows = self.buffered_rows;
            self.staged_bytes = self.buffered_bytes;
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.buffer_start_time = None;
        }

        debug!(epoch, "Delta Lake: pre-committed (batches staged)");
        Ok(None)
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        // Discard buffered/staged data. A coordinated attempt may already have
        // materialized an unreferenced immutable Parquet file; it must be reclaimed only by a
        // retention-safe vacuum after ambiguous catalog commits are impossible, never here.
        self.buffer.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.buffer_start_time = None;
        self.staged_batches.clear();
        self.staged_rows = 0;
        self.staged_bytes = 0;
        #[cfg(feature = "delta-lake")]
        {
            self.coordinated_adds.clear();
            self.coordinated_binding = None;
            self.coordinated_descriptor_bytes = 0;
        }

        self.metrics.record_rollback();
        warn!(epoch, "Delta Lake: rolled back epoch");
        Ok(())
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.config.write_timeout
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "delta-lake")]
        {
            self.ensure_coordinated_reconciled()?;
            self.ensure_write_generation_usable()?;
        }
        #[cfg(feature = "delta-lake")]
        let deadline = self.operation_deadline();

        // For at-least-once delivery, flush() is the commit trigger. Write
        // directly to Delta without entering the coordinated protocol.
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee != DeliveryGuarantee::ExactlyOnce {
            // Retry any orphaned staged data from a prior failed flush
            // before moving new data in, to prevent silent data loss.
            // flush_staged_to_delta handles table == None via reopen_table().
            if !self.staged_batches.is_empty() {
                self.flush_staged_to_delta(deadline).await?;
            }

            // Stage new buffered data and flush to Delta.
            if !self.buffer.is_empty() {
                self.staged_batches = std::mem::take(&mut self.buffer);
                self.staged_rows = self.buffered_rows;
                self.staged_bytes = self.buffered_bytes;
                self.buffered_rows = 0;
                self.buffered_bytes = 0;
                self.buffer_start_time = None;

                self.flush_staged_to_delta(deadline).await?;
            }
            return Ok(());
        }

        #[cfg(feature = "delta-lake")]
        return self.stage_coordinated_buffer(deadline).await;

        #[cfg(not(feature = "delta-lake"))]
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Delta Lake sink connector");

        // Only at-least-once may land buffered data during close. Exactly-once output without a
        // matching durable checkpoint must be discarded and replayed after recovery.
        #[cfg(feature = "delta-lake")]
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            || self.unresolved_delta_write
        {
            self.buffer.clear();
            self.staged_batches.clear();
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            self.staged_rows = 0;
            self.staged_bytes = 0;
            self.coordinated_adds.clear();
            self.coordinated_binding = None;
            self.coordinated_descriptor_bytes = 0;
        } else {
            self.flush().await?;
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

#[cfg(feature = "delta-lake")]
#[async_trait]
impl crate::connector::CoordinatedCommitter for DeltaLakeSink {
    async fn commit_aggregated(
        &self,
        batch: crate::connector::CoordinatedCommitBatch,
        context: crate::connector::CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        let deadline = context.deadline();
        let publication_budget = context.remaining();
        if publication_budget.is_zero() {
            return Err(ConnectorError::TransactionError(
                "Delta coordinated publication deadline elapsed before I/O".into(),
            ));
        }

        batch.validate_shape().map_err(|error| {
            ConnectorError::TransactionError(format!(
                "Delta coordinated batch validation failed: {error}"
            ))
        })?;
        let external_key = batch.namespace.external_key();
        let exact_batch_fingerprint = batch.exact_fingerprint();
        let target_cursor = crate::connector::CoordinatedCommitCursor {
            checkpoint_id: batch.target.checkpoint_id,
            fencing_token: batch.fencing_token,
        };
        let pending = UnresolvedDeltaPublication {
            external_key,
            target: target_cursor,
            exact_batch_fingerprint,
        };
        {
            let mut unresolved = self.coordinated_unresolved_publication.lock();
            if unresolved
                .as_ref()
                .is_some_and(|unresolved| unresolved != &pending)
            {
                return Err(ConnectorError::TransactionError(
                    "Delta has a different unresolved coordinated publication; only that exact cut may be retried"
                        .into(),
                ));
            }
            *unresolved = Some(pending.clone());
        }

        let operation = publish_coordinated_delta_batch(
            self.resolved_table_path.clone(),
            self.resolved_storage_options.clone(),
            Arc::clone(&self.coordinated_unresolved_publication),
            pending.clone(),
            batch,
            deadline,
            publication_budget,
        );
        // Tokio task-local state is intentionally propagated only for the
        // deterministic delayed-catalog test hook.
        #[cfg(test)]
        let operation = {
            let delay = super::delta_io::DELAY_COORDINATED_CATALOG_COMMIT
                .try_with(Clone::clone)
                .ok();
            async move {
                if let Some(delay) = delay {
                    super::delta_io::DELAY_COORDINATED_CATALOG_COMMIT
                        .scope(delay, operation)
                        .await
                } else {
                    operation.await
                }
            }
        };
        let task = match self.spawn_tracked_delta_task(operation) {
            Ok(task) => task,
            Err(error) => {
                let mut unresolved = self.coordinated_unresolved_publication.lock();
                if unresolved.as_ref() == Some(&pending) {
                    *unresolved = None;
                }
                return Err(error);
            }
        };

        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(ConnectorError::outcome_unknown(
                format!("Delta coordinated publication task terminated unexpectedly: {error}"),
                true,
            )),
            Err(_) => Err(ConnectorError::outcome_unknown(
                format!(
                    "Delta coordinated publication exceeded its {publication_budget:?} remaining \
                     budget; reconcile the exact cursor before replay"
                ),
                true,
            )),
        }
    }

    async fn committed_cursor(
        &self,
        namespace: &crate::connector::CoordinatedCommitNamespace,
    ) -> Result<Option<crate::connector::CoordinatedCommitCursor>, ConnectorError> {
        let table = super::delta_io::open_or_create_table(
            &self.resolved_table_path,
            self.resolved_storage_options.clone(),
            None,
        )
        .await?;
        let external_key = namespace.external_key();
        let observed = super::delta_io::get_coordinated_cursor(&table, &external_key).await?;
        let mut unresolved = self.coordinated_unresolved_publication.lock();
        if unresolved.as_ref().is_some_and(|pending| {
            pending.external_key == external_key && pending.reconciled_by(observed)
        }) {
            *unresolved = None;
        }
        Ok(observed)
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
            .field("buffered_rows", &self.buffered_rows)
            .field("delta_version", &self.delta_version)
            .finish_non_exhaustive()
    }
}

// ── Helper functions ────────────────────────────────────────────────

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
