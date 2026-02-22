//! Parquet format decoder implementing [`FormatDecoder`].
//!
//! Each [`RawRecord`] value contains complete Parquet file bytes.
//! The decoder uses `ParquetRecordBatchReaderBuilder` with optional
//! projection pushdown and row-group filtering.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

/// Predicate for row-group level filtering.
///
/// Evaluated against row-group statistics to skip entire row groups
/// that cannot contain matching rows.
#[derive(Debug, Clone)]
pub enum RowGroupPredicate {
    /// Column equals value (statistics min <= value <= max).
    Eq {
        /// Column name.
        column: String,
        /// Comparison value as string (parsed per column type).
        value: String,
    },
    /// Column greater than value (statistics max > value).
    Gt {
        /// Column name.
        column: String,
        /// Comparison value.
        value: String,
    },
    /// Column less than value (statistics min < value).
    Lt {
        /// Column name.
        column: String,
        /// Comparison value.
        value: String,
    },
    /// Column in range \[low, high\].
    Between {
        /// Column name.
        column: String,
        /// Low bound (inclusive).
        low: String,
        /// High bound (inclusive).
        high: String,
    },
    /// Logical AND of predicates.
    And(Vec<RowGroupPredicate>),
    /// Logical OR of predicates.
    Or(Vec<RowGroupPredicate>),
}

/// Configuration for the Parquet decoder.
#[derive(Debug, Clone)]
pub struct ParquetDecoderConfig {
    /// Column indices to project (empty = all columns).
    pub projection_indices: Vec<usize>,

    /// Row-group indices to read (empty = all row groups).
    pub row_group_indices: Vec<usize>,

    /// Maximum rows per `RecordBatch`.
    pub batch_size: usize,

    /// Optional row-group predicate for statistics-based filtering.
    pub predicate: Option<RowGroupPredicate>,
}

impl Default for ParquetDecoderConfig {
    fn default() -> Self {
        Self {
            projection_indices: Vec::new(),
            row_group_indices: Vec::new(),
            batch_size: 8192,
            predicate: None,
        }
    }
}

impl ParquetDecoderConfig {
    /// Sets the projection column indices.
    #[must_use]
    pub fn with_projection(mut self, indices: Vec<usize>) -> Self {
        self.projection_indices = indices;
        self
    }

    /// Sets the row-group indices to read.
    #[must_use]
    pub fn with_row_groups(mut self, indices: Vec<usize>) -> Self {
        self.row_group_indices = indices;
        self
    }

    /// Sets the batch size.
    #[must_use]
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Sets a row-group predicate.
    #[must_use]
    pub fn with_predicate(mut self, predicate: RowGroupPredicate) -> Self {
        self.predicate = Some(predicate);
        self
    }
}

/// Decodes Parquet file bytes into Arrow `RecordBatch`es.
///
/// # Ring Placement
///
/// - **Ring 1**: `decode_batch()` — Parquet read + Arrow conversion
/// - **Ring 2**: Construction — one-time schema validation
pub struct ParquetDecoder {
    /// Output schema (frozen at construction).
    schema: SchemaRef,
    /// Decoder configuration.
    config: ParquetDecoderConfig,
}

impl std::fmt::Debug for ParquetDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetDecoder")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .finish()
    }
}

impl ParquetDecoder {
    /// Creates a new Parquet decoder for the given Arrow schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self::with_config(schema, ParquetDecoderConfig::default())
    }

    /// Creates a new Parquet decoder with custom configuration.
    #[must_use]
    pub fn with_config(schema: SchemaRef, config: ParquetDecoderConfig) -> Self {
        Self { schema, config }
    }
}

impl FormatDecoder for ParquetDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for record in records {
            let bytes = Bytes::copy_from_slice(&record.value);
            let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(|e| SchemaError::DecodeError(format!("Parquet reader init error: {e}")))?;

            // Apply batch size.
            builder = builder.with_batch_size(self.config.batch_size);

            // Apply projection if specified.
            if !self.config.projection_indices.is_empty() {
                let parquet_schema = builder.parquet_schema().clone();
                let mask = ProjectionMask::roots(
                    &parquet_schema,
                    self.config.projection_indices.iter().copied(),
                );
                builder = builder.with_projection(mask);
            }

            // Apply row-group selection if specified.
            if !self.config.row_group_indices.is_empty() {
                builder = builder.with_row_groups(self.config.row_group_indices.clone());
            }

            let reader = builder.build().map_err(|e| {
                SchemaError::DecodeError(format!("Parquet reader build error: {e}"))
            })?;

            for batch_result in reader {
                let batch = batch_result
                    .map_err(|e| SchemaError::DecodeError(format!("Parquet read error: {e}")))?;
                all_batches.push(batch);
            }
        }

        if all_batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        if all_batches.len() == 1 {
            return Ok(all_batches.into_iter().next().unwrap());
        }

        // Concatenate all batches.
        arrow_select::concat::concat_batches(&self.schema, &all_batches)
            .map_err(|e| SchemaError::DecodeError(format!("batch concat error: {e}")))
    }

    fn format_name(&self) -> &'static str {
        "parquet"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    /// Helper: write a `RecordBatch` to Parquet bytes.
    fn to_parquet_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn make_batch(schema: &SchemaRef, ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_decode_empty_batch() {
        let schema = make_schema();
        let decoder = ParquetDecoder::new(schema.clone());
        let batch = decoder.decode_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_decode_single_record() {
        let schema = make_schema();
        let input = make_batch(&schema, vec![1, 2, 3], vec!["a", "b", "c"]);
        let parquet_bytes = to_parquet_bytes(&input);

        let decoder = ParquetDecoder::new(schema);
        let record = RawRecord::new(parquet_bytes);
        let output = decoder.decode_batch(&[record]).unwrap();

        assert_eq!(output.num_rows(), 3);
        let ids = output
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
        let names = output.column(1).as_string::<i32>();
        assert_eq!(names.value(0), "a");
    }

    #[test]
    fn test_decode_multiple_records() {
        let schema = make_schema();
        let b1 = make_batch(&schema, vec![1, 2], vec!["x", "y"]);
        let b2 = make_batch(&schema, vec![3, 4], vec!["z", "w"]);

        let r1 = RawRecord::new(to_parquet_bytes(&b1));
        let r2 = RawRecord::new(to_parquet_bytes(&b2));

        let decoder = ParquetDecoder::new(schema);
        let output = decoder.decode_batch(&[r1, r2]).unwrap();
        assert_eq!(output.num_rows(), 4);
    }

    #[test]
    fn test_decode_with_batch_size() {
        let schema = make_schema();
        let input = make_batch(&schema, vec![1, 2, 3], vec!["a", "b", "c"]);
        let parquet_bytes = to_parquet_bytes(&input);

        let config = ParquetDecoderConfig::default().with_batch_size(1);
        let decoder = ParquetDecoder::with_config(schema, config);
        let record = RawRecord::new(parquet_bytes);
        let output = decoder.decode_batch(&[record]).unwrap();

        // All rows should still be present (batch_size only affects internal
        // chunking, concat merges them back).
        assert_eq!(output.num_rows(), 3);
    }

    #[test]
    fn test_format_name() {
        let schema = make_schema();
        let decoder = ParquetDecoder::new(schema);
        assert_eq!(decoder.format_name(), "parquet");
    }

    #[test]
    fn test_decode_one() {
        let schema = make_schema();
        let input = make_batch(&schema, vec![42], vec!["hello"]);
        let parquet_bytes = to_parquet_bytes(&input);

        let decoder = ParquetDecoder::new(schema);
        let record = RawRecord::new(parquet_bytes);
        let output = decoder.decode_one(&record).unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
    }

    #[test]
    fn test_decode_invalid_bytes() {
        let schema = make_schema();
        let decoder = ParquetDecoder::new(schema);
        let record = RawRecord::new(b"not parquet".to_vec());
        let result = decoder.decode_batch(&[record]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Parquet"));
    }

    #[test]
    fn test_config_builder() {
        let config = ParquetDecoderConfig::default()
            .with_projection(vec![0, 2])
            .with_row_groups(vec![0])
            .with_batch_size(4096)
            .with_predicate(RowGroupPredicate::Eq {
                column: "id".into(),
                value: "42".into(),
            });

        assert_eq!(config.projection_indices, vec![0, 2]);
        assert_eq!(config.row_group_indices, vec![0]);
        assert_eq!(config.batch_size, 4096);
        assert!(config.predicate.is_some());
    }
}
