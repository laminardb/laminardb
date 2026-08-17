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
mod tests;
