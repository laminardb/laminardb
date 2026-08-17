//! Apache Iceberg append sink connector.

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "iceberg")]
use tracing::info;

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;

use super::iceberg_config::IcebergSinkConfig;

/// Apache Iceberg sink connector.
///
/// Buffers `RecordBatch` data and publishes each flush with an Iceberg
/// `fast_append` transaction.
pub struct IcebergSink {
    config: IcebergSinkConfig,
    schema: Option<SchemaRef>,
    state: ConnectorState,
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    staged_batches: Vec<RecordBatch>,
    #[cfg(feature = "iceberg")]
    catalog: Option<std::sync::Arc<dyn iceberg::Catalog>>,
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
            buffer: Vec::new(),
            buffered_rows: 0,
            staged_batches: Vec::new(),
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

    #[cfg(feature = "iceberg")]
    fn clear_staged(&mut self) {
        self.staged_batches.clear();
    }

    /// Reproject a batch onto the Iceberg-derived Arrow schema so every field
    /// carries the `PARQUET:field_id` metadata the Iceberg writer requires.
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

    /// Write staged batches to Parquet data files (no catalog commit).
    #[cfg(feature = "iceberg")]
    async fn write_staged_data_files(
        &self,
    ) -> Result<Vec<iceberg::spec::DataFile>, ConnectorError> {
        use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};

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

        // Stream every staged batch into ONE Parquet file (one S3 upload) per
        // epoch. The source hands the sink many small batches (e.g. 1k-row Kafka
        // polls); a file-per-batch loop would pay a full S3 multipart lifecycle
        // for each, serializing the pipeline behind that I/O.
        let mut writer = None;
        for batch in &self.staged_batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Reproject onto the Iceberg-derived Arrow schema so every field
            // carries PARQUET:field_id metadata; without it the writer can't
            // map Arrow fields to Iceberg field IDs ("Field id N not found").
            let aligned = self.align_batch_to_iceberg_schema(batch)?;

            // Open lazily on the first non-empty batch so an all-empty epoch
            // leaves no zero-row file behind.
            if writer.is_none() {
                let file_path = format!("{location}/data/ldb-{}.parquet", uuid::Uuid::new_v4());
                let output_file = file_io
                    .new_output(&file_path)
                    .map_err(|e| ConnectorError::WriteError(format!("create output: {e}")))?;
                writer = Some(
                    writer_builder
                        .clone()
                        .build(output_file)
                        .await
                        .map_err(|e| {
                            ConnectorError::WriteError(format!("build parquet writer: {e}"))
                        })?,
                );
            }

            writer
                .as_mut()
                .expect("writer opened above")
                .write(&aligned)
                .await
                .map_err(|e| ConnectorError::WriteError(format!("parquet write: {e}")))?;
        }

        let mut all_data_files = Vec::new();
        if let Some(writer) = writer {
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

        Ok(all_data_files)
    }
}

#[async_trait]
impl SinkConnector for IcebergSink {
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let config = IcebergSinkConfig::from_config(config)?;
        let warehouse = config.catalog.warehouse.to_ascii_lowercase();
        let shared_warehouse = ["s3://", "s3a://"]
            .iter()
            .any(|scheme| warehouse.starts_with(scheme))
            || config
                .catalog
                .storage_type
                .as_deref()
                .is_some_and(|storage| matches!(storage, "s3" | "s3a"));
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            if shared_warehouse {
                SinkTopology::MultiWriter
            } else {
                SinkTopology::Singleton
            },
            SinkInputMode::AppendOnly,
        ))
    }

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
        if batch.num_rows() == 0 {
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

    fn suggested_write_timeout(&self) -> Duration {
        // Iceberg catalog writes can be slow under contention.
        Duration::from_secs(300)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if self.staged_batches.is_empty() {
            if self.buffer.is_empty() {
                return Ok(());
            }
            std::mem::swap(&mut self.staged_batches, &mut self.buffer);
            self.clear_buffer();
        }

        #[cfg(feature = "iceberg")]
        {
            let data_files = self.write_staged_data_files().await?;
            if data_files.is_empty() {
                self.clear_staged();
                return Ok(());
            }
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
            let updated =
                super::iceberg_io::commit_data_files_append(table, catalog.as_ref(), data_files)
                    .await?;
            self.table = Some(updated);
            self.clear_staged();
            return Ok(());
        }

        #[cfg(not(feature = "iceberg"))]
        Err(ConnectorError::ConfigurationError(
            "Apache Iceberg requires the 'iceberg' feature".into(),
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        while !self.staged_batches.is_empty() || !self.buffer.is_empty() {
            self.flush().await?;
        }
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
mod tests;
