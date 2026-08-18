//! Stateful table initialization, staging, materialization, and write operations.

#[cfg(feature = "delta-lake")]
use super::{
    classify_delta_attempt_error, count_collapsed_ops, debug, run_tracked_delta_task, DataType,
    DeltaTable, DeltaWriteTaskSuccess, Future, Instant, SaveMode, WriteResult,
    MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
#[cfg(all(feature = "delta-lake", feature = "delta-lake-unity"))]
use super::ensure_uc_table_exists;
use super::{
    filter_and_project, Arc, Array, ConnectorError, ConnectorState, ConnectorTaskOwner,
    DeliveryGuarantee, DeltaLakeSink, DeltaLakeSinkConfig, DeltaLakeSinkMetrics, DeltaWriteMode,
    RecordBatch, SchemaRef,
};

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
    pub(super) async fn init_delta_table(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        use super::super::delta_io;

        if let Some(schema) = self.schema.as_ref() {
            Self::validate_weight_column(schema, "pipeline")?;
        }

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

        if table.version().is_some() {
            let table_schema = delta_io::get_table_schema(&table)?;
            if let Some(pipeline_schema) = self.schema.as_ref() {
                Self::validate_existing_table_schema(
                    pipeline_schema,
                    &table_schema,
                    self.config.schema_evolution,
                    self.is_coordinated(),
                )?;
            } else {
                Self::validate_weight_column(&table_schema, "Delta table")?;
                self.schema = Some(table_schema);
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

    pub(super) fn target_schema(batch_schema: &SchemaRef, write_mode: DeltaWriteMode) -> SchemaRef {
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

    #[cfg(feature = "delta-lake")]
    fn validate_weight_column(schema: &SchemaRef, owner: &str) -> Result<(), ConnectorError> {
        let Ok(field) = schema.field_with_name(laminar_core::changelog::WEIGHT_COLUMN) else {
            return Ok(());
        };
        if field.data_type() != &DataType::Int64 || field.is_nullable() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "{owner} '{}' column must be non-null Int64",
                laminar_core::changelog::WEIGHT_COLUMN
            )));
        }
        Ok(())
    }

    #[cfg(feature = "delta-lake")]
    fn validate_existing_table_schema(
        pipeline: &SchemaRef,
        table: &SchemaRef,
        schema_evolution: bool,
        exact_schema_required: bool,
    ) -> Result<(), ConnectorError> {
        Self::validate_weight_column(pipeline, "pipeline")?;
        Self::validate_weight_column(table, "Delta table")?;

        let weight = laminar_core::changelog::WEIGHT_COLUMN;
        if pipeline.field_with_name(weight).is_ok() != table.field_with_name(weight).is_ok() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "pipeline and existing Delta table must both contain '{weight}' for a weighted changelog"
            )));
        }

        if exact_schema_required {
            if pipeline.as_ref() != table.as_ref() {
                return Err(ConnectorError::SchemaMismatch(
                    "pipeline schema must exactly match the existing Delta table for coordinated append"
                        .into(),
                ));
            }
            return Ok(());
        }

        let allow_evolution = schema_evolution;
        for input_field in pipeline.fields() {
            let Ok(table_field) = table.field_with_name(input_field.name()) else {
                if allow_evolution {
                    continue;
                }
                return Err(ConnectorError::SchemaMismatch(format!(
                    "pipeline column '{}' is missing from the existing Delta table",
                    input_field.name()
                )));
            };
            if !arrow_cast::can_cast_types(input_field.data_type(), table_field.data_type()) {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "pipeline column '{}' cannot be written as Delta type {}",
                    input_field.name(),
                    table_field.data_type()
                )));
            }
            if input_field.is_nullable() && !table_field.is_nullable() {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "nullable pipeline column '{}' cannot target a non-null Delta column",
                    input_field.name()
                )));
            }
        }

        for table_field in table.fields() {
            if pipeline.field_with_name(table_field.name()).is_err()
                && (!allow_evolution || !table_field.is_nullable())
            {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "existing Delta column '{}' is missing from the pipeline schema",
                    table_field.name()
                )));
            }
        }
        Ok(())
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
    pub(super) fn operation_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.config.write_timeout
    }

    #[cfg(feature = "delta-lake")]
    pub(super) fn ensure_write_generation_usable(&self) -> Result<(), ConnectorError> {
        if self.unresolved_delta_write {
            return Err(ConnectorError::InvalidState {
                expected: "a fresh Delta sink generation after reconciliation".into(),
                actual: "a prior dispatched Delta operation lost its result".into(),
            });
        }
        Ok(())
    }

    #[cfg(feature = "delta-lake")]
    pub(super) fn spawn_tracked_delta_task<F>(
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
        use super::super::delta_io;

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
    ) -> Result<DeltaWriteTaskSuccess, super::super::delta_io::DeltaWriteAttemptError> {
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

            super::super::delta_io::merge_changelog(
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

            super::super::delta_io::write_batches(
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
    pub(super) fn is_coordinated(&self) -> bool {
        self.config.write_mode == DeltaWriteMode::Append
            && self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
    }

    #[cfg(feature = "delta-lake")]
    pub(super) fn ensure_coordinated_reconciled(&self) -> Result<(), ConnectorError> {
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
            super::super::delta_io::coordinated_table_binding(self.table.as_ref().ok_or_else(
                || ConnectorError::InvalidState {
                    expected: "open".into(),
                    actual: "delta table not loaded".into(),
                },
            )?)?;
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
        if projected_add_count > super::super::delta_io::MAX_COORDINATED_ADD_ACTIONS {
            return Err(ConnectorError::WriteError(format!(
                "Delta coordinated checkpoint would exceed the fixed {} Add-action limit",
                super::super::delta_io::MAX_COORDINATED_ADD_ACTIONS
            )));
        }

        // Account only for the new Add array. Once the binding is encoded, appending another
        // non-empty chunk replaces no envelope fields and inserts one comma between array items.
        let chunk_add_array_bytes = super::super::delta_io::encoded_add_array_len(&adds)?;
        let projected_descriptor_bytes = if self.coordinated_adds.is_empty() {
            super::super::delta_io::encode_commit_descriptor(&binding, &adds)?.len()
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
    pub(super) async fn stage_coordinated_buffer(
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

    #[cfg(feature = "delta-lake")]
    fn collapse_staged_upsert_changelog(&mut self) -> Result<(), ConnectorError> {
        if self.config.write_mode != DeltaWriteMode::Upsert {
            return Ok(());
        }

        // Collapse the epoch before MERGE so each key appears once and retries reuse the result.
        let combined = if self.staged_batches.len() == 1 {
            self.staged_batches[0].clone()
        } else {
            let schema = self.staged_batches[0].schema();
            arrow_select::concat::concat_batches(&schema, &self.staged_batches).map_err(
                |error| {
                    ConnectorError::Internal(format!("failed to concat staged batches: {error}"))
                },
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
        Ok(())
    }

    /// Writes staged data under one end-to-end deadline. delta-rs owns the
    /// optimistic commit retry loop; this layer must not amplify that budget.
    #[cfg(feature = "delta-lake")]
    pub(super) async fn flush_staged_to_delta(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<WriteResult, ConnectorError> {
        if self.staged_batches.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }
        self.ensure_write_generation_usable()?;
        let write_timeout = self.config.write_timeout;
        self.collapse_staged_upsert_changelog()?;

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
