//! Apache Iceberg sink connector implementation.
//!
//! [`IcebergSink`] implements [`SinkConnector`], writing Arrow `RecordBatch`
//! data to Iceberg tables with exactly-once semantics tied to checkpoint
//! epochs via Iceberg's atomic transaction commits.
//!
//! `pre_commit()` moves the buffer into `staged_batches`.
//! `commit_epoch()` writes Parquet data files and commits via Iceberg
//! `Transaction::fast_append()`. `rollback_epoch()` discards staged data
//! without side effects.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "iceberg")]
use tracing::info;
use tracing::{debug, warn};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::iceberg_config::IcebergSinkConfig;

/// Apache Iceberg sink connector.
///
/// Buffers `RecordBatch` data during each checkpoint epoch and commits to
/// an Iceberg table atomically when the epoch commits. Each epoch produces
/// at most one Iceberg transaction.
pub struct IcebergSink {
    /// Sink configuration — reparsed from `ConnectorConfig` in `open()`.
    config: IcebergSinkConfig,
    /// Arrow schema for input batches.
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
    /// Staged batches ready for commit (populated by `pre_commit()`).
    staged_batches: Vec<RecordBatch>,
    /// Rows staged for commit.
    staged_rows: usize,
    /// Whether the current epoch was skipped (already committed).
    epoch_skipped: bool,
    /// Cached catalog connection (initialized in `open()`).
    #[cfg(feature = "iceberg")]
    catalog: Option<std::sync::Arc<dyn iceberg::Catalog>>,
    /// Cached table handle (updated after each commit).
    #[cfg(feature = "iceberg")]
    table: Option<iceberg::table::Table>,
    /// Arrow schema derived from the Iceberg table schema (carries
    /// `PARQUET:field_id` metadata required by the Iceberg Parquet writer).
    #[cfg(feature = "iceberg")]
    iceberg_arrow_schema: Option<SchemaRef>,
}

impl IcebergSink {
    /// Creates a new Iceberg sink with the given configuration.
    #[must_use]
    pub fn new(config: IcebergSinkConfig, _registry: Option<&prometheus::Registry>) -> Self {
        Self {
            config,
            schema: None,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            buffer: Vec::new(),
            buffered_rows: 0,
            staged_batches: Vec::new(),
            staged_rows: 0,
            epoch_skipped: false,
            #[cfg(feature = "iceberg")]
            catalog: None,
            #[cfg(feature = "iceberg")]
            table: None,
            #[cfg(feature = "iceberg")]
            iceberg_arrow_schema: None,
        }
    }

    fn clear_buffer(&mut self) {
        self.buffer.clear();
        self.buffered_rows = 0;
    }

    fn clear_staged(&mut self) {
        self.staged_batches.clear();
        self.staged_rows = 0;
    }

    /// Reprojects a pipeline `RecordBatch` onto the Iceberg-derived Arrow
    /// schema so that every field carries `PARQUET:field_id` metadata.
    ///
    /// Fast path: when the batch schema fields match the Iceberg schema
    /// field-for-field (same names, types, count), just swap the schema
    /// wrapper — columns are already in the right order.
    ///
    /// Slow path: match columns by name, cast where types differ (safe
    /// widening validated in `open()`), fill nullable extras with nulls.
    #[cfg(feature = "iceberg")]
    fn align_batch_to_iceberg_schema(
        &self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, ConnectorError> {
        let target_schema =
            self.iceberg_arrow_schema
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "open".into(),
                    actual: "iceberg arrow schema not initialized".into(),
                })?;

        // Fast path: field names, types, and count match — only metadata differs.
        // Avoids per-column name lookup and Vec construction.
        let batch_schema = batch.schema();
        if batch_schema.fields().len() == target_schema.fields().len()
            && batch_schema
                .fields()
                .iter()
                .zip(target_schema.fields().iter())
                .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
        {
            return RecordBatch::try_new(target_schema.clone(), batch.columns().to_vec()).map_err(
                |e| ConnectorError::WriteError(format!("align batch to iceberg schema: {e}")),
            );
        }

        // Slow path: column reordering, type casting, or null-filling needed.
        let mut columns = Vec::with_capacity(target_schema.fields().len());

        for field in target_schema.fields() {
            if let Ok(col_idx) = batch_schema.index_of(field.name()) {
                let col = batch.column(col_idx);
                if col.data_type() == field.data_type() {
                    columns.push(col.clone());
                } else {
                    columns.push(arrow_cast::cast(col, field.data_type()).map_err(|e| {
                        ConnectorError::WriteError(format!(
                            "cast field '{}' from {} to {}: {e}",
                            field.name(),
                            col.data_type(),
                            field.data_type(),
                        ))
                    })?);
                }
            } else if field.is_nullable() {
                // Nullable Iceberg column not in pipeline — fill with nulls.
                columns.push(arrow_array::new_null_array(
                    field.data_type(),
                    batch.num_rows(),
                ));
            } else {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "Iceberg column '{}' is NOT NULL but missing from pipeline",
                    field.name(),
                )));
            }
        }

        // Detect batch columns that would be silently dropped — every field
        // in the source batch must map to a field in the target schema.
        for field in batch_schema.fields() {
            if target_schema.field_with_name(field.name()).is_err() {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "pipeline column '{}' has no matching field in Iceberg table schema \
                     (schema evolved since open?)",
                    field.name(),
                )));
            }
        }

        RecordBatch::try_new(target_schema.clone(), columns)
            .map_err(|e| ConnectorError::WriteError(format!("align batch to iceberg schema: {e}")))
    }

    /// Parses compression config string to parquet Compression.
    #[cfg(feature = "iceberg")]
    fn parquet_compression(name: &str) -> parquet::basic::Compression {
        match name.to_lowercase().as_str() {
            "snappy" => parquet::basic::Compression::SNAPPY,
            "none" | "uncompressed" => parquet::basic::Compression::UNCOMPRESSED,
            "lz4" => parquet::basic::Compression::LZ4,
            // Default to zstd(3) for anything else including "zstd".
            _ => parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap_or_default(),
            ),
        }
    }

    /// Checks that every pipeline field still exists in the refreshed
    /// Iceberg Arrow schema. Returns `SchemaMismatch` on drift.
    #[cfg(feature = "iceberg")]
    fn validate_schema_not_drifted(&self) -> Result<(), ConnectorError> {
        if let (Some(pipeline_schema), Some(target_schema)) =
            (&self.schema, &self.iceberg_arrow_schema)
        {
            for field in pipeline_schema.fields() {
                if target_schema.field_with_name(field.name()).is_err() {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "pipeline field '{}' no longer exists in Iceberg table schema \
                         (concurrent schema evolution?)",
                        field.name(),
                    )));
                }
            }
        }
        Ok(())
    }

    /// Writes staged batches to Iceberg as data files and commits.
    #[cfg(feature = "iceberg")]
    async fn commit_to_iceberg(&mut self) -> Result<(), ConnectorError> {
        use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};

        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "catalog not initialized".into(),
            })?;
        let table = self
            .table
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "table not loaded".into(),
            })?;

        let file_io = table.file_io().clone();
        let location = table.metadata().location().to_string();
        let schema = table.current_schema_ref();

        self.validate_schema_not_drifted()?;

        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(Self::parquet_compression(&self.config.compression))
            .build();
        let writer_builder = ParquetWriterBuilder::new(props, schema);

        let mut all_data_files = Vec::new();

        for (idx, batch) in self.staged_batches.iter().enumerate() {
            if batch.num_rows() == 0 {
                continue;
            }

            // Reproject the batch onto the Iceberg-derived Arrow schema so
            // that every field carries PARQUET:field_id metadata. Without
            // this the writer cannot correlate Arrow fields with Iceberg
            // field IDs and fails with "Field id N not found in struct array".
            let aligned = self.align_batch_to_iceberg_schema(batch)?;

            let file_path = format!(
                "{location}/data/{}-{}-{idx}.parquet",
                self.config.writer_id, self.current_epoch,
            );

            let output_file = file_io
                .new_output(&file_path)
                .map_err(|e| ConnectorError::WriteError(format!("create output: {e}")))?;

            let mut writer = writer_builder
                .clone()
                .build(output_file)
                .await
                .map_err(|e| ConnectorError::WriteError(format!("build parquet writer: {e}")))?;

            writer
                .write(&aligned)
                .await
                .map_err(|e| ConnectorError::WriteError(format!("parquet write: {e}")))?;

            let data_file_builders = writer
                .close()
                .await
                .map_err(|e| ConnectorError::WriteError(format!("close parquet writer: {e}")))?;

            for dfb in data_file_builders {
                let data_file = dfb
                    .build()
                    .map_err(|e| ConnectorError::WriteError(format!("data file build: {e}")))?;
                all_data_files.push(data_file);
            }
        }

        if all_data_files.is_empty() {
            debug!(epoch = self.current_epoch, "no data files to commit");
            return Ok(());
        }

        let updated_table = super::iceberg_io::commit_data_files(
            table,
            catalog.as_ref(),
            all_data_files,
            Some((&self.config.writer_id, self.current_epoch)),
        )
        .await?;

        // The Iceberg commit is durable at this point. Update table handle
        // unconditionally so the next commit sees the new snapshot.
        self.table = Some(updated_table);

        // Best-effort schema refresh — keep the cached Arrow schema in sync
        // with the table for the next epoch. A failure here must NOT prevent
        // epoch advancement: if commit_to_iceberg() returned Err after the
        // durable commit, commit_epoch() would skip last_committed_epoch
        // update, and the next retry would duplicate the append.
        let table = self.table.as_ref().expect("just set above");
        let new_iceberg_schema = table.current_schema_ref();
        match iceberg::arrow::schema_to_arrow_schema(&new_iceberg_schema) {
            Ok(arrow_schema) => {
                self.iceberg_arrow_schema = Some(std::sync::Arc::new(arrow_schema));
            }
            Err(e) => {
                // Invalidate the cache so the next align_batch_to_iceberg_schema()
                // fails with InvalidState rather than silently writing with stale
                // field IDs that no longer match table.current_schema_ref().
                self.iceberg_arrow_schema = None;
                warn!(
                    epoch = self.current_epoch,
                    error = %e,
                    "failed to refresh Iceberg Arrow schema; cache invalidated"
                );
            }
        }

        info!(
            epoch = self.current_epoch,
            rows = self.staged_rows,
            "iceberg commit succeeded"
        );

        Ok(())
    }
}

#[async_trait]
impl SinkConnector for IcebergSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        // Re-parse config from the runtime ConnectorConfig (not factory defaults).
        if !config.properties().is_empty() {
            self.config = IcebergSinkConfig::from_config(config)?;
        }

        #[cfg(feature = "iceberg")]
        {
            let catalog = super::iceberg_io::build_catalog(&self.config.catalog).await?;
            let ns = &self.config.catalog.namespace;
            let tbl = &self.config.catalog.table_name;

            if self.config.auto_create {
                if let Some(schema) = config.arrow_schema() {
                    super::iceberg_io::ensure_table_exists(catalog.as_ref(), ns, tbl, &schema)
                        .await?;
                }
            }

            let table = super::iceberg_io::load_table(catalog.as_ref(), ns, tbl).await?;

            // Always derive the canonical schema from the Iceberg table.
            let iceberg_schema = table.current_schema_ref();
            let table_schema = std::sync::Arc::new(
                iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(|e| {
                    ConnectorError::SchemaMismatch(format!("iceberg→arrow schema: {e}"))
                })?,
            );

            // Store the Iceberg-derived Arrow schema (with PARQUET:field_id
            // metadata) for use during Parquet writes.
            self.iceberg_arrow_schema = Some(table_schema.clone());

            if self.schema.is_none() {
                self.schema = Some(table_schema.clone());
            }

            // Recover last committed epoch from table properties.
            if let Some(epoch) =
                super::iceberg_io::get_last_committed_epoch(&table, &self.config.writer_id)
            {
                self.last_committed_epoch = epoch;
                info!(writer_id = %self.config.writer_id, epoch, "recovered last committed epoch");
            }

            // Validate pipeline schema against table schema, then use the
            // pipeline schema as self.schema (it's what write_batch receives).
            if let Some(pipeline_schema) = config.arrow_schema() {
                super::iceberg_config::validate_sink_schema(&pipeline_schema, &table_schema)?;
                self.schema = Some(pipeline_schema);
            }

            self.catalog = Some(catalog);
            self.table = Some(table);
            self.state = ConnectorState::Running;

            info!(table = tbl, namespace = ns, "iceberg sink connected");
            return Ok(());
        }

        #[cfg(not(feature = "iceberg"))]
        {
            self.state = ConnectorState::Failed;
            Err(ConnectorError::ConfigurationError(
                "Apache Iceberg requires the 'iceberg' feature".into(),
            ))
        }
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if batch.num_rows() == 0 || self.epoch_skipped {
            return Ok(WriteResult::new(0, 0));
        }

        if self.schema.is_none() {
            self.schema = Some(batch.schema());
        }

        let rows = batch.num_rows();
        self.buffer.push(batch.clone());
        self.buffered_rows += rows;

        Ok(WriteResult::new(rows, 0))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| std::sync::Arc::new(arrow_schema::Schema::empty()))
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch = epoch;
        self.epoch_skipped = false;
        self.clear_buffer();
        self.clear_staged();

        if epoch > 0 && epoch <= self.last_committed_epoch {
            debug!(
                epoch,
                last = self.last_committed_epoch,
                "epoch already committed, skipping"
            );
            self.epoch_skipped = true;
        }

        Ok(())
    }

    async fn pre_commit(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        if self.epoch_skipped {
            return Ok(());
        }

        std::mem::swap(&mut self.staged_batches, &mut self.buffer);
        self.staged_rows = self.buffered_rows;
        self.clear_buffer();

        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.epoch_skipped || self.staged_rows == 0 {
            self.clear_staged();
            return Ok(());
        }

        #[cfg(feature = "iceberg")]
        {
            self.commit_to_iceberg().await?;
        }

        self.last_committed_epoch = epoch;
        self.clear_staged();
        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        warn!(epoch, "iceberg rollback: discarding staged data");
        self.clear_buffer();
        self.clear_staged();
        self.epoch_skipped = false;
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => HealthStatus::Healthy,
            ConnectorState::Failed => HealthStatus::Unhealthy("sink failed".into()),
            _ => HealthStatus::Unknown,
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics::default()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        // Iceberg catalog writes can be slow under contention.
        SinkConnectorCapabilities::new(Duration::from_secs(300))
            .with_exactly_once()
            .with_two_phase_commit()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "iceberg")]
        {
            self.catalog = None;
            self.table = None;
            self.iceberg_arrow_schema = None;
        }
        self.state = ConnectorState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn test_batch(n: usize) -> RecordBatch {
        let ids: Vec<i64> = (0..n as i64).collect();
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
    }

    fn test_config() -> IcebergSinkConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://test/wh");
        config.set("namespace", "test");
        config.set("table.name", "events");
        IcebergSinkConfig::from_config(&config).unwrap()
    }

    #[test]
    fn test_new_sink() {
        let sink = IcebergSink::new(test_config(), None);
        assert!(sink.schema.is_none());
        assert_eq!(sink.current_epoch, 0);
        assert_eq!(sink.buffered_rows, 0);
    }

    #[tokio::test]
    async fn test_write_buffers_batches() {
        let mut sink = IcebergSink::new(test_config(), None);
        sink.begin_epoch(1).await.unwrap();

        let result = sink.write_batch(&test_batch(100)).await.unwrap();
        assert_eq!(result.records_written, 100);
        assert_eq!(sink.buffered_rows, 100);
        assert_eq!(sink.buffer.len(), 1);

        let result = sink.write_batch(&test_batch(50)).await.unwrap();
        assert_eq!(result.records_written, 50);
        assert_eq!(sink.buffered_rows, 150);
        assert_eq!(sink.buffer.len(), 2);
    }

    #[tokio::test]
    async fn test_pre_commit_stages_buffer() {
        let mut sink = IcebergSink::new(test_config(), None);
        sink.begin_epoch(1).await.unwrap();
        sink.write_batch(&test_batch(100)).await.unwrap();

        sink.pre_commit(1).await.unwrap();
        assert_eq!(sink.staged_rows, 100);
        assert_eq!(sink.staged_batches.len(), 1);
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.buffered_rows, 0);
    }

    #[tokio::test]
    async fn test_rollback_clears_staged() {
        let mut sink = IcebergSink::new(test_config(), None);
        sink.begin_epoch(1).await.unwrap();
        sink.write_batch(&test_batch(100)).await.unwrap();
        sink.pre_commit(1).await.unwrap();

        sink.rollback_epoch(1).await.unwrap();
        assert!(sink.staged_batches.is_empty());
        assert_eq!(sink.staged_rows, 0);
        assert!(sink.buffer.is_empty());
    }

    #[tokio::test]
    async fn test_epoch_skip_when_already_committed() {
        let mut sink = IcebergSink::new(test_config(), None);
        sink.last_committed_epoch = 5;

        sink.begin_epoch(3).await.unwrap();
        assert!(sink.epoch_skipped);

        let result = sink.write_batch(&test_batch(100)).await.unwrap();
        assert_eq!(result.records_written, 0);
    }

    #[tokio::test]
    async fn test_empty_epoch_commit() {
        let mut sink = IcebergSink::new(test_config(), None);
        sink.begin_epoch(1).await.unwrap();
        sink.pre_commit(1).await.unwrap();
        sink.commit_epoch(1).await.unwrap();
    }

    #[test]
    fn test_capabilities() {
        let sink = IcebergSink::new(test_config(), None);
        let caps = sink.capabilities();
        assert!(caps.exactly_once);
        assert!(caps.two_phase_commit);
        assert!(!caps.partitioned);
        assert!(!caps.upsert);
    }
}
