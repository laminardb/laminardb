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
}

impl IcebergSink {
    /// Creates a new Iceberg sink with the given configuration.
    #[must_use]
    pub fn new(config: IcebergSinkConfig) -> Self {
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

        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(Self::parquet_compression(&self.config.compression))
            .build();
        let writer_builder = ParquetWriterBuilder::new(props, schema);

        let mut all_data_files = Vec::new();

        for (idx, batch) in self.staged_batches.iter().enumerate() {
            if batch.num_rows() == 0 {
                continue;
            }

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
                .write(batch)
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

        // Cache the updated table so the next commit sees the new snapshot.
        self.table = Some(updated_table);

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
                    self.schema = Some(schema);
                }
            }

            let table = super::iceberg_io::load_table(catalog.as_ref(), ns, tbl).await?;

            if self.schema.is_none() {
                let iceberg_schema = table.current_schema_ref();
                let arrow_schema = iceberg::arrow::schema_to_arrow_schema(&iceberg_schema)
                    .map_err(|e| {
                        ConnectorError::SchemaMismatch(format!("iceberg→arrow schema: {e}"))
                    })?;
                self.schema = Some(std::sync::Arc::new(arrow_schema));
            }

            // Recover last committed epoch from table properties.
            if let Some(epoch) =
                super::iceberg_io::get_last_committed_epoch(&table, &self.config.writer_id)
            {
                self.last_committed_epoch = epoch;
                info!(writer_id = %self.config.writer_id, epoch, "recovered last committed epoch");
            }

            // Validate pipeline schema against table schema if available.
            if let Some(pipeline_schema) = config.arrow_schema() {
                super::iceberg_config::validate_sink_schema(
                    &pipeline_schema,
                    self.schema.as_ref().unwrap(),
                )?;
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
        SinkConnectorCapabilities::default()
            .with_exactly_once()
            .with_two_phase_commit()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "iceberg")]
        {
            self.catalog = None;
            self.table = None;
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
        let sink = IcebergSink::new(test_config());
        assert!(sink.schema.is_none());
        assert_eq!(sink.current_epoch, 0);
        assert_eq!(sink.buffered_rows, 0);
    }

    #[tokio::test]
    async fn test_write_buffers_batches() {
        let mut sink = IcebergSink::new(test_config());
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
        let mut sink = IcebergSink::new(test_config());
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
        let mut sink = IcebergSink::new(test_config());
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
        let mut sink = IcebergSink::new(test_config());
        sink.last_committed_epoch = 5;

        sink.begin_epoch(3).await.unwrap();
        assert!(sink.epoch_skipped);

        let result = sink.write_batch(&test_batch(100)).await.unwrap();
        assert_eq!(result.records_written, 0);
    }

    #[tokio::test]
    async fn test_empty_epoch_commit() {
        let mut sink = IcebergSink::new(test_config());
        sink.begin_epoch(1).await.unwrap();
        sink.pre_commit(1).await.unwrap();
        sink.commit_epoch(1).await.unwrap();
    }

    #[test]
    fn test_capabilities() {
        let sink = IcebergSink::new(test_config());
        let caps = sink.capabilities();
        assert!(caps.exactly_once);
        assert!(caps.two_phase_commit);
        assert!(!caps.partitioned);
        assert!(!caps.upsert);
    }
}
