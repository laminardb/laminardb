//! Parquet format encoder implementing [`FormatEncoder`].
//!
//! Encodes Arrow `RecordBatch`es into Parquet file bytes using
//! `ArrowWriter<Vec<u8>>` with configurable compression and row-group sizing.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatEncoder;

/// Configuration for the Parquet encoder.
#[derive(Debug, Clone)]
pub struct ParquetEncoderConfig {
    /// Compression codec (default: Snappy).
    pub compression: Compression,

    /// Parquet writer version (1 or 2, default: 2).
    pub writer_version: i32,

    /// Maximum rows per row group (default: `1_000_000`).
    pub max_row_group_size: usize,

    /// Whether to write column statistics (default: true).
    pub write_statistics: bool,
}

impl Default for ParquetEncoderConfig {
    fn default() -> Self {
        Self {
            compression: Compression::SNAPPY,
            writer_version: 2,
            max_row_group_size: 1_000_000,
            write_statistics: true,
        }
    }
}

impl ParquetEncoderConfig {
    /// Sets the compression codec.
    #[must_use]
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the writer version.
    #[must_use]
    pub fn with_writer_version(mut self, version: i32) -> Self {
        self.writer_version = version;
        self
    }

    /// Sets the maximum rows per row group.
    #[must_use]
    pub fn with_max_row_group_size(mut self, size: usize) -> Self {
        self.max_row_group_size = size;
        self
    }

    /// Enables or disables column statistics.
    #[must_use]
    pub fn with_statistics(mut self, enabled: bool) -> Self {
        self.write_statistics = enabled;
        self
    }
}

/// Encodes Arrow `RecordBatch`es into Parquet file bytes.
///
/// Each call to `encode_batch` produces a single Parquet file (as `Vec<u8>`)
/// containing the entire batch. The file includes footer metadata and is
/// self-contained.
#[derive(Debug)]
pub struct ParquetEncoder {
    schema: SchemaRef,
    config: ParquetEncoderConfig,
}

impl ParquetEncoder {
    /// Creates a new Parquet encoder for the given schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self::with_config(schema, ParquetEncoderConfig::default())
    }

    /// Creates a new Parquet encoder with custom configuration.
    #[must_use]
    pub fn with_config(schema: SchemaRef, config: ParquetEncoderConfig) -> Self {
        Self { schema, config }
    }
}

impl FormatEncoder for ParquetEncoder {
    fn input_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn encode_batch(&self, batch: &RecordBatch) -> SchemaResult<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let mut props_builder = WriterProperties::builder()
            .set_compression(self.config.compression)
            .set_max_row_group_size(self.config.max_row_group_size);

        if !self.config.write_statistics {
            props_builder = props_builder
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None);
        }

        let props = props_builder.build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, self.schema.clone(), Some(props))
            .map_err(|e| SchemaError::DecodeError(format!("Parquet writer init: {e}")))?;

        writer
            .write(batch)
            .map_err(|e| SchemaError::DecodeError(format!("Parquet write error: {e}")))?;

        writer
            .close()
            .map_err(|e| SchemaError::DecodeError(format!("Parquet close error: {e}")))?;

        // Single Parquet file containing the full batch.
        Ok(vec![buf])
    }

    fn format_name(&self) -> &'static str {
        "parquet"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::{GzipLevel, ZstdLevel};
    use std::sync::Arc;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_encode_empty_batch() {
        let schema = make_schema();
        let batch = RecordBatch::new_empty(schema.clone());
        let encoder = ParquetEncoder::new(schema);
        let result = encoder.encode_batch(&batch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_encode_roundtrip() {
        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let encoder = ParquetEncoder::new(schema);
        let encoded = encoder.encode_batch(&batch).unwrap();
        assert_eq!(encoded.len(), 1);

        // Decode back to verify roundtrip.
        let bytes = bytes::Bytes::from(encoded.into_iter().next().unwrap());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();

        let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_encode_with_compression() {
        let schema = make_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();

        let config = ParquetEncoderConfig::default()
            .with_compression(Compression::GZIP(GzipLevel::default()));
        let encoder = ParquetEncoder::with_config(schema, config);
        let encoded = encoder.encode_batch(&batch).unwrap();
        assert_eq!(encoded.len(), 1);
        assert!(!encoded[0].is_empty());
    }

    #[test]
    fn test_format_name() {
        let schema = make_schema();
        let encoder = ParquetEncoder::new(schema);
        assert_eq!(encoder.format_name(), "parquet");
    }

    #[test]
    fn test_config_builder() {
        let config = ParquetEncoderConfig::default()
            .with_compression(Compression::ZSTD(ZstdLevel::default()))
            .with_writer_version(1)
            .with_max_row_group_size(500)
            .with_statistics(false);

        assert!(matches!(config.compression, Compression::ZSTD(_)));
        assert_eq!(config.writer_version, 1);
        assert_eq!(config.max_row_group_size, 500);
        assert!(!config.write_statistics);
    }
}
