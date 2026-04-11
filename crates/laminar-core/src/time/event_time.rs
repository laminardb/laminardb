//! Event-time extraction from Arrow `RecordBatch` columns.
//!
//! Columns must be `Timestamp(_)` at any precision; non-millisecond
//! precisions are cast to `Timestamp(Millisecond)` via the Arrow kernel.

use std::sync::Arc;

use arrow::array::{Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;

use super::cast::cast_to_millis_array;

/// Column identifier for timestamp field.
#[derive(Debug, Clone)]
pub enum TimestampField {
    /// Column name (resolved on first use, cached).
    Name(String),
    /// Column index (most efficient).
    Index(usize),
}

/// Multi-row extraction strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtractionMode {
    /// Extract from first row — O(1), default.
    #[default]
    First,
    /// Extract from last row — O(1).
    Last,
    /// Extract maximum timestamp — O(n).
    Max,
    /// Extract minimum timestamp — O(n).
    Min,
}

/// Errors that can occur during event time extraction.
#[derive(Debug, thiserror::Error)]
pub enum EventTimeError {
    /// Column not found in schema.
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// Column index out of bounds.
    #[error("Column index {index} out of bounds (batch has {num_columns} columns)")]
    IndexOutOfBounds {
        /// Requested index.
        index: usize,
        /// Number of columns in the batch.
        num_columns: usize,
    },

    /// Column is not a `Timestamp(_)` type.
    #[error("event-time column must be Timestamp(_), found {found}")]
    IncompatibleType {
        /// Actual type found.
        found: String,
    },

    /// Null timestamp encountered at a non-null-tolerant position.
    #[error("Null timestamp at row {row}")]
    NullTimestamp {
        /// Row index with the null value.
        row: usize,
    },

    /// Empty batch provided.
    #[error("Cannot extract timestamp from empty batch")]
    EmptyBatch,

    /// Arrow `cast` kernel failed.
    #[error("Arrow cast to Timestamp(Millisecond) failed: {0}")]
    CastFailed(String),
}

/// Extracts event timestamps from Arrow `RecordBatch` columns.
#[derive(Debug)]
pub struct EventTimeExtractor {
    field: TimestampField,
    mode: ExtractionMode,
    cached_index: Option<usize>,
}

impl EventTimeExtractor {
    /// Creates an extractor that looks up a column by name. The column
    /// index is cached after the first extraction.
    #[must_use]
    pub fn from_column(name: &str) -> Self {
        Self {
            field: TimestampField::Name(name.to_string()),
            mode: ExtractionMode::default(),
            cached_index: None,
        }
    }

    /// Creates an extractor that uses a column by index. Skips the name
    /// lookup entirely.
    #[must_use]
    pub fn from_index(index: usize) -> Self {
        Self {
            field: TimestampField::Index(index),
            mode: ExtractionMode::default(),
            cached_index: Some(index),
        }
    }

    /// Sets the extraction mode for multi-row batches.
    #[must_use]
    pub fn with_mode(mut self, mode: ExtractionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Gets the configured extraction mode.
    #[must_use]
    pub fn mode(&self) -> ExtractionMode {
        self.mode
    }

    /// Validates that the schema contains a `Timestamp(_)` column at the
    /// configured position.
    ///
    /// # Errors
    ///
    /// Returns an error if the column is missing or not a `Timestamp(_)`.
    pub fn validate_schema(&self, schema: &Schema) -> Result<(), EventTimeError> {
        let (_, data_type) = self.resolve_column(schema)?;
        if !matches!(data_type, DataType::Timestamp(_, _)) {
            return Err(EventTimeError::IncompatibleType {
                found: format!("{data_type:?}"),
            });
        }
        Ok(())
    }

    /// Extracts the event timestamp from a batch, in epoch milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch is empty, the column is missing,
    /// the column is not a `Timestamp(_)`, the value at the selected row
    /// is null, or Arrow's cast kernel fails.
    pub fn extract(&mut self, batch: &RecordBatch) -> Result<i64, EventTimeError> {
        if batch.num_rows() == 0 {
            return Err(EventTimeError::EmptyBatch);
        }
        let index = self.get_column_index(batch.schema().as_ref())?;
        let column = batch.column(index);
        self.extract_from_column(column)
    }

    fn get_column_index(&mut self, schema: &Schema) -> Result<usize, EventTimeError> {
        if let Some(idx) = self.cached_index {
            if idx < schema.fields().len() {
                return Ok(idx);
            }
        }
        let (index, _) = self.resolve_column(schema)?;
        self.cached_index = Some(index);
        Ok(index)
    }

    fn resolve_column<'a>(
        &self,
        schema: &'a Schema,
    ) -> Result<(usize, &'a DataType), EventTimeError> {
        match &self.field {
            TimestampField::Name(name) => {
                let index = schema
                    .index_of(name)
                    .map_err(|_| EventTimeError::ColumnNotFound(name.clone()))?;
                Ok((index, schema.field(index).data_type()))
            }
            TimestampField::Index(index) => {
                if *index >= schema.fields().len() {
                    return Err(EventTimeError::IndexOutOfBounds {
                        index: *index,
                        num_columns: schema.fields().len(),
                    });
                }
                Ok((*index, schema.field(*index).data_type()))
            }
        }
    }

    fn extract_from_column(&self, column: &Arc<dyn Array>) -> Result<i64, EventTimeError> {
        let ms = cast_to_millis_array(column.as_ref()).map_err(|e| {
            if matches!(column.data_type(), DataType::Timestamp(_, _)) {
                EventTimeError::CastFailed(e.0)
            } else {
                EventTimeError::IncompatibleType { found: e.0 }
            }
        })?;
        match self.mode {
            ExtractionMode::First => read_indexed(&ms, 0),
            ExtractionMode::Last => read_indexed(&ms, ms.len() - 1),
            ExtractionMode::Max => fold_non_null(&ms, i64::MIN, i64::max),
            ExtractionMode::Min => fold_non_null(&ms, i64::MAX, i64::min),
        }
    }
}

fn read_indexed(arr: &TimestampMillisecondArray, idx: usize) -> Result<i64, EventTimeError> {
    if arr.is_null(idx) {
        Err(EventTimeError::NullTimestamp { row: idx })
    } else {
        Ok(arr.value(idx))
    }
}

fn fold_non_null<F>(arr: &TimestampMillisecondArray, init: i64, f: F) -> Result<i64, EventTimeError>
where
    F: Fn(i64, i64) -> i64,
{
    let mut out = init;
    let mut found = false;
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            found = true;
            out = f(out, arr.value(i));
        }
    }
    if found {
        Ok(out)
    } else {
        Err(EventTimeError::NullTimestamp { row: 0 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Int64Builder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
        TimestampNanosecondBuilder, TimestampSecondBuilder,
    };
    use arrow::datatypes::{Field, TimeUnit};
    use std::sync::Arc;

    fn make_ms_batch(values: &[Option<i64>]) -> RecordBatch {
        let mut b = TimestampMillisecondBuilder::new();
        for v in values {
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(b.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn make_ns_batch(values: &[Option<i64>]) -> RecordBatch {
        let mut b = TimestampNanosecondBuilder::new();
        for v in values {
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(b.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn make_us_batch(values: &[Option<i64>]) -> RecordBatch {
        let mut b = TimestampMicrosecondBuilder::new();
        for v in values {
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(b.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn make_s_batch(values: &[Option<i64>]) -> RecordBatch {
        let mut b = TimestampSecondBuilder::new();
        for v in values {
            match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(b.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn test_extract_millis() {
        let batch = make_ms_batch(&[Some(1_705_312_200_000)]);
        let mut extractor = EventTimeExtractor::from_column("ts");
        assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
    }

    #[test]
    fn test_extract_nanos_is_rescaled_to_millis() {
        let batch = make_ns_batch(&[Some(1_705_312_200_000_000_000)]);
        let mut extractor = EventTimeExtractor::from_column("ts");
        assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
    }

    #[test]
    fn test_extract_micros_is_rescaled_to_millis() {
        let batch = make_us_batch(&[Some(1_705_312_200_000_000)]);
        let mut extractor = EventTimeExtractor::from_column("ts");
        assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
    }

    #[test]
    fn test_extract_seconds_is_rescaled_to_millis() {
        let batch = make_s_batch(&[Some(1_705_312_200)]);
        let mut extractor = EventTimeExtractor::from_column("ts");
        assert_eq!(extractor.extract(&batch).unwrap(), 1_705_312_200_000);
    }

    #[test]
    fn test_mode_first() {
        let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::First);
        assert_eq!(extractor.extract(&batch).unwrap(), 100);
    }

    #[test]
    fn test_mode_last() {
        let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Last);
        assert_eq!(extractor.extract(&batch).unwrap(), 150);
    }

    #[test]
    fn test_mode_max() {
        let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max);
        assert_eq!(extractor.extract(&batch).unwrap(), 200);
    }

    #[test]
    fn test_mode_min() {
        let batch = make_ms_batch(&[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Min);
        assert_eq!(extractor.extract(&batch).unwrap(), 100);
    }

    #[test]
    fn test_max_skips_nulls() {
        let batch = make_ms_batch(&[Some(100), None, Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::Max);
        assert_eq!(extractor.extract(&batch).unwrap(), 200);
    }

    #[test]
    fn test_column_not_found() {
        let batch = make_ms_batch(&[Some(100)]);
        let mut extractor = EventTimeExtractor::from_column("missing");
        assert!(matches!(
            extractor.extract(&batch),
            Err(EventTimeError::ColumnNotFound(_))
        ));
    }

    #[test]
    fn test_non_timestamp_column_is_rejected() {
        let mut b = Int64Builder::new();
        b.append_value(100);
        let array: ArrayRef = Arc::new(b.finish());
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let mut extractor = EventTimeExtractor::from_column("ts");
        assert!(matches!(
            extractor.extract(&batch),
            Err(EventTimeError::IncompatibleType { .. })
        ));
    }

    #[test]
    fn test_empty_batch() {
        let batch = make_ms_batch(&[]);
        let mut extractor = EventTimeExtractor::from_column("ts");
        assert!(matches!(
            extractor.extract(&batch),
            Err(EventTimeError::EmptyBatch)
        ));
    }

    #[test]
    fn test_null_first_row() {
        let batch = make_ms_batch(&[None, Some(100)]);
        let mut extractor = EventTimeExtractor::from_column("ts").with_mode(ExtractionMode::First);
        assert!(matches!(
            extractor.extract(&batch),
            Err(EventTimeError::NullTimestamp { row: 0 })
        ));
    }

    #[test]
    fn test_column_index_caching() {
        let batch = make_ms_batch(&[Some(100)]);
        let mut extractor = EventTimeExtractor::from_column("ts");

        assert!(extractor.cached_index.is_none());
        let _ = extractor.extract(&batch).unwrap();
        assert_eq!(extractor.cached_index, Some(0));
        assert_eq!(extractor.extract(&batch).unwrap(), 100);
    }

    #[test]
    fn test_from_index_skips_name_lookup() {
        let batch = make_ms_batch(&[Some(100)]);
        let mut extractor = EventTimeExtractor::from_index(0);
        assert_eq!(extractor.cached_index, Some(0));
        assert_eq!(extractor.extract(&batch).unwrap(), 100);
    }

    #[test]
    fn test_validate_schema_ok() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);
        let extractor = EventTimeExtractor::from_column("ts");
        assert!(extractor.validate_schema(&schema).is_ok());
    }

    #[test]
    fn test_validate_schema_rejects_non_timestamp() {
        let schema = Schema::new(vec![Field::new("ts", DataType::Int64, true)]);
        let extractor = EventTimeExtractor::from_column("ts");
        assert!(matches!(
            extractor.validate_schema(&schema),
            Err(EventTimeError::IncompatibleType { .. })
        ));
    }
}
