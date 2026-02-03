//! Event Time Extraction
//!
//! This module provides `EventTimeExtractor` for extracting timestamps from Arrow `RecordBatch`
//! columns. It supports multiple timestamp formats and extraction modes for correct event-time
//! processing in streaming windows.
//!
//! # Example
//!
//! ```ignore
//! use laminar_core::time::{EventTimeExtractor, TimestampFormat};
//!
//! let mut extractor = EventTimeExtractor::from_column("event_time", TimestampFormat::UnixMillis);
//! let timestamp = extractor.extract(&batch)?;
//! ```

use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_cast::parse::string_to_datetime;
use chrono::Utc;

/// Timestamp format variants for extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampFormat {
    /// Unix timestamp in milliseconds (i64)
    UnixMillis,
    /// Unix timestamp in seconds (i64) - converted to millis
    UnixSeconds,
    /// Unix timestamp in microseconds (i64) - converted to millis
    UnixMicros,
    /// Unix timestamp in nanoseconds (i64) - converted to millis
    UnixNanos,
    /// ISO 8601 string format (e.g., "2024-01-15T10:30:00Z")
    Iso8601,
    /// Auto-detect from Arrow Timestamp type
    ArrowNative,
}

impl fmt::Display for TimestampFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimestampFormat::UnixMillis => write!(f, "UnixMillis"),
            TimestampFormat::UnixSeconds => write!(f, "UnixSeconds"),
            TimestampFormat::UnixMicros => write!(f, "UnixMicros"),
            TimestampFormat::UnixNanos => write!(f, "UnixNanos"),
            TimestampFormat::Iso8601 => write!(f, "ISO8601"),
            TimestampFormat::ArrowNative => write!(f, "ArrowNative"),
        }
    }
}

/// Column identifier for timestamp field.
#[derive(Debug, Clone)]
pub enum TimestampField {
    /// Column name (cached after first lookup)
    Name(String),
    /// Column index (most efficient)
    Index(usize),
}

/// Multi-row extraction strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtractionMode {
    /// Extract from first row - O(1), default
    #[default]
    First,
    /// Extract from last row - O(1)
    Last,
    /// Extract maximum timestamp - O(n)
    Max,
    /// Extract minimum timestamp - O(n)
    Min,
}

/// Errors that can occur during event time extraction.
#[derive(Debug, thiserror::Error)]
pub enum EventTimeError {
    /// Column not found in schema
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// Column index out of bounds
    #[error("Column index {index} out of bounds (batch has {num_columns} columns)")]
    IndexOutOfBounds {
        /// Requested index
        index: usize,
        /// Number of columns in batch
        num_columns: usize,
    },

    /// Incompatible column type for format
    #[error("Incompatible type for format {format}: expected {expected}, found {found}")]
    IncompatibleType {
        /// Requested format
        format: TimestampFormat,
        /// Expected type
        expected: String,
        /// Actual type found
        found: String,
    },

    /// Failed to parse timestamp value
    #[error("Failed to parse timestamp '{value}': {reason}")]
    ParseError {
        /// The value that failed to parse
        value: String,
        /// Reason for failure
        reason: String,
    },

    /// Null timestamp encountered
    #[error("Null timestamp at row {row}")]
    NullTimestamp {
        /// Row index with null value
        row: usize,
    },

    /// Empty batch provided
    #[error("Cannot extract timestamp from empty batch")]
    EmptyBatch,
}

/// Extracts event timestamps from Arrow `RecordBatch` columns.
///
/// Supports multiple timestamp formats and extraction modes for multi-row batches.
/// Uses internal caching for column index lookup to optimize repeated extractions.
#[derive(Debug)]
pub struct EventTimeExtractor {
    field: TimestampField,
    format: TimestampFormat,
    mode: ExtractionMode,
    cached_index: Option<usize>,
}

impl EventTimeExtractor {
    /// Creates an extractor that looks up a column by name.
    ///
    /// The column index is cached after the first extraction for efficiency.
    #[must_use]
    pub fn from_column(name: &str, format: TimestampFormat) -> Self {
        Self {
            field: TimestampField::Name(name.to_string()),
            format,
            mode: ExtractionMode::default(),
            cached_index: None,
        }
    }

    /// Creates an extractor that uses a column by index.
    ///
    /// This is the most efficient option as no lookup is required.
    #[must_use]
    pub fn from_index(index: usize, format: TimestampFormat) -> Self {
        Self {
            field: TimestampField::Index(index),
            format,
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

    /// Gets the configured format.
    #[must_use]
    pub fn format(&self) -> TimestampFormat {
        self.format
    }

    /// Gets the configured mode.
    #[must_use]
    pub fn mode(&self) -> ExtractionMode {
        self.mode
    }

    /// Validates that the schema contains a compatible timestamp column.
    ///
    /// # Errors
    ///
    /// Returns an error if the column is not found or has an incompatible type.
    pub fn validate_schema(&self, schema: &Schema) -> Result<(), EventTimeError> {
        let (index, data_type) = self.resolve_column(schema)?;
        self.validate_type(data_type, index)?;
        Ok(())
    }

    /// Extracts the event timestamp from a batch.
    ///
    /// Returns the timestamp in milliseconds since Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The batch is empty
    /// - The column is not found
    /// - The column type is incompatible with the format
    /// - The timestamp value is null
    /// - The timestamp cannot be parsed
    pub fn extract(&mut self, batch: &RecordBatch) -> Result<i64, EventTimeError> {
        if batch.num_rows() == 0 {
            return Err(EventTimeError::EmptyBatch);
        }

        let index = self.get_column_index(batch.schema().as_ref())?;
        let column = batch.column(index);

        self.extract_from_column(column)
    }

    /// Resolves the column index, using cache if available.
    fn get_column_index(&mut self, schema: &Schema) -> Result<usize, EventTimeError> {
        if let Some(idx) = self.cached_index {
            // Validate the cached index is still valid
            if idx < schema.fields().len() {
                return Ok(idx);
            }
        }

        let (index, _) = self.resolve_column(schema)?;
        self.cached_index = Some(index);
        Ok(index)
    }

    /// Resolves column name/index to actual index and data type.
    fn resolve_column<'a>(
        &self,
        schema: &'a Schema,
    ) -> Result<(usize, &'a DataType), EventTimeError> {
        match &self.field {
            TimestampField::Name(name) => {
                let index = schema
                    .index_of(name)
                    .map_err(|_| EventTimeError::ColumnNotFound(name.clone()))?;
                let data_type = schema.field(index).data_type();
                Ok((index, data_type))
            }
            TimestampField::Index(index) => {
                if *index >= schema.fields().len() {
                    return Err(EventTimeError::IndexOutOfBounds {
                        index: *index,
                        num_columns: schema.fields().len(),
                    });
                }
                let data_type = schema.field(*index).data_type();
                Ok((*index, data_type))
            }
        }
    }

    /// Validates that the data type is compatible with the format.
    fn validate_type(&self, data_type: &DataType, _index: usize) -> Result<(), EventTimeError> {
        match self.format {
            TimestampFormat::UnixMillis
            | TimestampFormat::UnixSeconds
            | TimestampFormat::UnixMicros
            | TimestampFormat::UnixNanos => {
                if !matches!(data_type, DataType::Int64) {
                    return Err(EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Int64".to_string(),
                        found: format!("{data_type:?}"),
                    });
                }
            }
            TimestampFormat::Iso8601 => {
                if !matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                    return Err(EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Utf8 or LargeUtf8".to_string(),
                        found: format!("{data_type:?}"),
                    });
                }
            }
            TimestampFormat::ArrowNative => {
                if !matches!(data_type, DataType::Timestamp(_, _)) {
                    return Err(EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Timestamp".to_string(),
                        found: format!("{data_type:?}"),
                    });
                }
            }
        }
        Ok(())
    }

    /// Extracts timestamp from a column array.
    fn extract_from_column(&self, column: &Arc<dyn Array>) -> Result<i64, EventTimeError> {
        match self.format {
            TimestampFormat::UnixMillis => {
                let array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Int64".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_i64(array, |v| v)
            }
            TimestampFormat::UnixSeconds => {
                let array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Int64".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_i64(array, |v| v.saturating_mul(1000))
            }
            TimestampFormat::UnixMicros => {
                let array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Int64".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_i64(array, |v| v / 1000)
            }
            TimestampFormat::UnixNanos => {
                let array = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Int64".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_i64(array, |v| v / 1_000_000)
            }
            TimestampFormat::Iso8601 => {
                let array = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "Utf8".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_iso8601(array)
            }
            TimestampFormat::ArrowNative => self.extract_arrow_timestamp(column),
        }
    }

    /// Extracts from Int64 array with conversion function.
    fn extract_i64<F>(&self, array: &Int64Array, convert: F) -> Result<i64, EventTimeError>
    where
        F: Fn(i64) -> i64,
    {
        match self.mode {
            ExtractionMode::First => {
                if array.is_null(0) {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                } else {
                    Ok(convert(array.value(0)))
                }
            }
            ExtractionMode::Last => {
                let last = array.len() - 1;
                if array.is_null(last) {
                    Err(EventTimeError::NullTimestamp { row: last })
                } else {
                    Ok(convert(array.value(last)))
                }
            }
            ExtractionMode::Max => {
                let mut max_val = i64::MIN;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = convert(array.value(i));
                        if v > max_val {
                            max_val = v;
                        }
                    }
                }
                if found {
                    Ok(max_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
            ExtractionMode::Min => {
                let mut min_val = i64::MAX;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = convert(array.value(i));
                        if v < min_val {
                            min_val = v;
                        }
                    }
                }
                if found {
                    Ok(min_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
        }
    }

    /// Extracts from ISO 8601 string array.
    fn extract_iso8601(&self, array: &StringArray) -> Result<i64, EventTimeError> {
        let parse_value = |idx: usize| -> Result<i64, EventTimeError> {
            if array.is_null(idx) {
                return Err(EventTimeError::NullTimestamp { row: idx });
            }
            let s = array.value(idx);
            let dt = string_to_datetime(&Utc, s).map_err(|e| EventTimeError::ParseError {
                value: s.to_string(),
                reason: e.to_string(),
            })?;
            Ok(dt.timestamp_millis())
        };

        match self.mode {
            ExtractionMode::First => parse_value(0),
            ExtractionMode::Last => parse_value(array.len() - 1),
            ExtractionMode::Max => {
                let mut max_val = i64::MIN;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        let v = parse_value(i)?;
                        found = true;
                        if v > max_val {
                            max_val = v;
                        }
                    }
                }
                if found {
                    Ok(max_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
            ExtractionMode::Min => {
                let mut min_val = i64::MAX;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        let v = parse_value(i)?;
                        found = true;
                        if v < min_val {
                            min_val = v;
                        }
                    }
                }
                if found {
                    Ok(min_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
        }
    }

    /// Extracts from Arrow native Timestamp array.
    fn extract_arrow_timestamp(&self, column: &Arc<dyn Array>) -> Result<i64, EventTimeError> {
        match column.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "TimestampSecond".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_ts_seconds(array)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "TimestampMillisecond".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_ts_millis(array)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "TimestampMicrosecond".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_ts_micros(array)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let array = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| EventTimeError::IncompatibleType {
                        format: self.format,
                        expected: "TimestampNanosecond".to_string(),
                        found: format!("{:?}", column.data_type()),
                    })?;
                self.extract_ts_nanos(array)
            }
            _ => Err(EventTimeError::IncompatibleType {
                format: self.format,
                expected: "Timestamp".to_string(),
                found: format!("{:?}", column.data_type()),
            }),
        }
    }

    /// Extracts from `TimestampSecondArray`.
    fn extract_ts_seconds(&self, array: &TimestampSecondArray) -> Result<i64, EventTimeError> {
        match self.mode {
            ExtractionMode::First => {
                if array.is_null(0) {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                } else {
                    Ok(array.value(0).saturating_mul(1000))
                }
            }
            ExtractionMode::Last => {
                let last = array.len() - 1;
                if array.is_null(last) {
                    Err(EventTimeError::NullTimestamp { row: last })
                } else {
                    Ok(array.value(last).saturating_mul(1000))
                }
            }
            ExtractionMode::Max => {
                let mut max_val = i64::MIN;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i).saturating_mul(1000);
                        if v > max_val {
                            max_val = v;
                        }
                    }
                }
                if found {
                    Ok(max_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
            ExtractionMode::Min => {
                let mut min_val = i64::MAX;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i).saturating_mul(1000);
                        if v < min_val {
                            min_val = v;
                        }
                    }
                }
                if found {
                    Ok(min_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
        }
    }

    /// Extracts from `TimestampMillisecondArray`.
    fn extract_ts_millis(&self, array: &TimestampMillisecondArray) -> Result<i64, EventTimeError> {
        match self.mode {
            ExtractionMode::First => {
                if array.is_null(0) {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                } else {
                    Ok(array.value(0))
                }
            }
            ExtractionMode::Last => {
                let last = array.len() - 1;
                if array.is_null(last) {
                    Err(EventTimeError::NullTimestamp { row: last })
                } else {
                    Ok(array.value(last))
                }
            }
            ExtractionMode::Max => {
                let mut max_val = i64::MIN;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i);
                        if v > max_val {
                            max_val = v;
                        }
                    }
                }
                if found {
                    Ok(max_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
            ExtractionMode::Min => {
                let mut min_val = i64::MAX;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i);
                        if v < min_val {
                            min_val = v;
                        }
                    }
                }
                if found {
                    Ok(min_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
        }
    }

    /// Extracts from `TimestampMicrosecondArray`.
    fn extract_ts_micros(&self, array: &TimestampMicrosecondArray) -> Result<i64, EventTimeError> {
        match self.mode {
            ExtractionMode::First => {
                if array.is_null(0) {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                } else {
                    Ok(array.value(0) / 1000)
                }
            }
            ExtractionMode::Last => {
                let last = array.len() - 1;
                if array.is_null(last) {
                    Err(EventTimeError::NullTimestamp { row: last })
                } else {
                    Ok(array.value(last) / 1000)
                }
            }
            ExtractionMode::Max => {
                let mut max_val = i64::MIN;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i) / 1000;
                        if v > max_val {
                            max_val = v;
                        }
                    }
                }
                if found {
                    Ok(max_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
            ExtractionMode::Min => {
                let mut min_val = i64::MAX;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i) / 1000;
                        if v < min_val {
                            min_val = v;
                        }
                    }
                }
                if found {
                    Ok(min_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
        }
    }

    /// Extracts from `TimestampNanosecondArray`.
    fn extract_ts_nanos(&self, array: &TimestampNanosecondArray) -> Result<i64, EventTimeError> {
        match self.mode {
            ExtractionMode::First => {
                if array.is_null(0) {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                } else {
                    Ok(array.value(0) / 1_000_000)
                }
            }
            ExtractionMode::Last => {
                let last = array.len() - 1;
                if array.is_null(last) {
                    Err(EventTimeError::NullTimestamp { row: last })
                } else {
                    Ok(array.value(last) / 1_000_000)
                }
            }
            ExtractionMode::Max => {
                let mut max_val = i64::MIN;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i) / 1_000_000;
                        if v > max_val {
                            max_val = v;
                        }
                    }
                }
                if found {
                    Ok(max_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
            ExtractionMode::Min => {
                let mut min_val = i64::MAX;
                let mut found = false;
                for i in 0..array.len() {
                    if !array.is_null(i) {
                        found = true;
                        let v = array.value(i) / 1_000_000;
                        if v < min_val {
                            min_val = v;
                        }
                    }
                }
                if found {
                    Ok(min_val)
                } else {
                    Err(EventTimeError::NullTimestamp { row: 0 })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn make_int64_batch(name: &str, values: &[Option<i64>]) -> RecordBatch {
        let mut builder = Int64Builder::new();
        for v in values {
            match v {
                Some(val) => builder.append_value(*val),
                None => builder.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn make_string_batch(name: &str, values: &[Option<&str>]) -> RecordBatch {
        let mut builder = StringBuilder::new();
        for v in values {
            match v {
                Some(val) => builder.append_value(*val),
                None => builder.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn make_timestamp_millis_batch(name: &str, values: &[Option<i64>]) -> RecordBatch {
        use arrow::array::TimestampMillisecondBuilder;
        let mut builder = TimestampMillisecondBuilder::new();
        for v in values {
            match v {
                Some(val) => builder.append_value(*val),
                None => builder.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn test_unix_millis_extraction() {
        let batch = make_int64_batch("event_time", &[Some(1_705_312_200_000)]);
        let mut extractor =
            EventTimeExtractor::from_column("event_time", TimestampFormat::UnixMillis);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 1_705_312_200_000);
    }

    #[test]
    fn test_unix_seconds_extraction() {
        let batch = make_int64_batch("event_time", &[Some(1_705_312_200)]);
        let mut extractor =
            EventTimeExtractor::from_column("event_time", TimestampFormat::UnixSeconds);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 1_705_312_200_000);
    }

    #[test]
    fn test_unix_micros_extraction() {
        let batch = make_int64_batch("event_time", &[Some(1_705_312_200_000_000)]);
        let mut extractor =
            EventTimeExtractor::from_column("event_time", TimestampFormat::UnixMicros);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 1_705_312_200_000);
    }

    #[test]
    fn test_unix_nanos_extraction() {
        let batch = make_int64_batch("event_time", &[Some(1_705_312_200_000_000_000)]);
        let mut extractor =
            EventTimeExtractor::from_column("event_time", TimestampFormat::UnixNanos);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 1_705_312_200_000);
    }

    #[test]
    fn test_iso8601_extraction() {
        let timestamp_str = "2024-01-15T10:30:00Z";
        let batch = make_string_batch("event_time", &[Some(timestamp_str)]);
        let mut extractor = EventTimeExtractor::from_column("event_time", TimestampFormat::Iso8601);
        let ts = extractor.extract(&batch).unwrap();

        // Compute expected value using the same parsing method for consistency
        let expected = string_to_datetime(&Utc, timestamp_str)
            .unwrap()
            .timestamp_millis();
        assert_eq!(ts, expected);
    }

    #[test]
    fn test_arrow_native_millis_extraction() {
        let batch = make_timestamp_millis_batch("event_time", &[Some(1_705_312_200_000)]);
        let mut extractor =
            EventTimeExtractor::from_column("event_time", TimestampFormat::ArrowNative);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 1_705_312_200_000);
    }

    #[test]
    fn test_extraction_mode_first() {
        let batch = make_int64_batch("ts", &[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::First);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 100);
    }

    #[test]
    fn test_extraction_mode_last() {
        let batch = make_int64_batch("ts", &[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Last);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 150);
    }

    #[test]
    fn test_extraction_mode_max() {
        let batch = make_int64_batch("ts", &[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Max);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 200);
    }

    #[test]
    fn test_extraction_mode_min() {
        let batch = make_int64_batch("ts", &[Some(100), Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Min);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 100);
    }

    #[test]
    fn test_max_with_nulls() {
        let batch = make_int64_batch("ts", &[Some(100), None, Some(200), Some(150)]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Max);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 200);
    }

    #[test]
    fn test_min_with_nulls() {
        let batch = make_int64_batch("ts", &[Some(100), None, Some(200), Some(50)]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Min);
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 50);
    }

    #[test]
    fn test_column_not_found() {
        let batch = make_int64_batch("other_column", &[Some(100)]);
        let mut extractor =
            EventTimeExtractor::from_column("event_time", TimestampFormat::UnixMillis);
        let result = extractor.extract(&batch);
        assert!(matches!(result, Err(EventTimeError::ColumnNotFound(_))));
    }

    #[test]
    fn test_index_out_of_bounds() {
        let batch = make_int64_batch("ts", &[Some(100)]);
        let mut extractor = EventTimeExtractor::from_index(5, TimestampFormat::UnixMillis);
        let result = extractor.extract(&batch);
        assert!(matches!(
            result,
            Err(EventTimeError::IndexOutOfBounds { .. })
        ));
    }

    #[test]
    fn test_incompatible_type() {
        let batch = make_string_batch("ts", &[Some("not a number")]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis);
        let result = extractor.extract(&batch);
        assert!(matches!(
            result,
            Err(EventTimeError::IncompatibleType { .. })
        ));
    }

    #[test]
    fn test_null_timestamp_first() {
        let batch = make_int64_batch("ts", &[None, Some(100)]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::First);
        let result = extractor.extract(&batch);
        assert!(matches!(
            result,
            Err(EventTimeError::NullTimestamp { row: 0 })
        ));
    }

    #[test]
    fn test_null_timestamp_last() {
        let batch = make_int64_batch("ts", &[Some(100), None]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Last);
        let result = extractor.extract(&batch);
        assert!(matches!(
            result,
            Err(EventTimeError::NullTimestamp { row: 1 })
        ));
    }

    #[test]
    fn test_empty_batch() {
        let batch = make_int64_batch("ts", &[]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis);
        let result = extractor.extract(&batch);
        assert!(matches!(result, Err(EventTimeError::EmptyBatch)));
    }

    #[test]
    fn test_all_nulls_max() {
        let batch = make_int64_batch("ts", &[None, None, None]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Max);
        let result = extractor.extract(&batch);
        assert!(matches!(result, Err(EventTimeError::NullTimestamp { .. })));
    }

    #[test]
    fn test_parse_error_iso8601() {
        let batch = make_string_batch("ts", &[Some("not-a-timestamp")]);
        let mut extractor = EventTimeExtractor::from_column("ts", TimestampFormat::Iso8601);
        let result = extractor.extract(&batch);
        assert!(matches!(result, Err(EventTimeError::ParseError { .. })));
    }

    #[test]
    fn test_column_index_caching() {
        let batch = make_int64_batch("event_time", &[Some(100)]);
        let mut extractor =
            EventTimeExtractor::from_column("event_time", TimestampFormat::UnixMillis);

        // First extraction should cache the index
        assert!(extractor.cached_index.is_none());
        let _ = extractor.extract(&batch).unwrap();
        assert_eq!(extractor.cached_index, Some(0));

        // Second extraction should use cached index
        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 100);
    }

    #[test]
    fn test_index_extractor_no_lookup() {
        let batch = make_int64_batch("ts", &[Some(100)]);
        let mut extractor = EventTimeExtractor::from_index(0, TimestampFormat::UnixMillis);

        // Index extractor should have cached index from start
        assert_eq!(extractor.cached_index, Some(0));

        let ts = extractor.extract(&batch).unwrap();
        assert_eq!(ts, 100);
    }

    #[test]
    fn test_validate_schema_success() {
        let schema = Schema::new(vec![Field::new("ts", DataType::Int64, true)]);
        let extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis);
        assert!(extractor.validate_schema(&schema).is_ok());
    }

    #[test]
    fn test_validate_schema_column_not_found() {
        let schema = Schema::new(vec![Field::new("other", DataType::Int64, true)]);
        let extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis);
        let result = extractor.validate_schema(&schema);
        assert!(matches!(result, Err(EventTimeError::ColumnNotFound(_))));
    }

    #[test]
    fn test_validate_schema_incompatible_type() {
        let schema = Schema::new(vec![Field::new("ts", DataType::Utf8, true)]);
        let extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis);
        let result = extractor.validate_schema(&schema);
        assert!(matches!(
            result,
            Err(EventTimeError::IncompatibleType { .. })
        ));
    }

    #[test]
    fn test_format_accessor() {
        let extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMicros);
        assert_eq!(extractor.format(), TimestampFormat::UnixMicros);
    }

    #[test]
    fn test_mode_accessor() {
        let extractor = EventTimeExtractor::from_column("ts", TimestampFormat::UnixMillis)
            .with_mode(ExtractionMode::Max);
        assert_eq!(extractor.mode(), ExtractionMode::Max);
    }
}
