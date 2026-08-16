#![deny(clippy::disallowed_types)]

//! Shared key-column utilities for stateful streaming joins.

use std::hash::Hash;

use rustc_hash::FxHasher;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use laminar_core::time::cast_to_millis_array;

use crate::error::DbError;

/// Typed key column for streaming joins.
pub(crate) enum KeyColumn<'a> {
    Utf8(&'a StringArray),
    Int64(&'a Int64Array),
}

impl KeyColumn<'_> {
    pub fn is_null(&self, i: usize) -> bool {
        match self {
            KeyColumn::Utf8(a) => a.is_null(i),
            KeyColumn::Int64(a) => a.is_null(i),
        }
    }

    pub fn hash_into(&self, i: usize, hasher: &mut FxHasher) {
        match self {
            KeyColumn::Utf8(a) => a.value(i).hash(hasher),
            KeyColumn::Int64(a) => a.value(i).hash(hasher),
        }
    }

    /// Returns false if either key is null (SQL three-valued logic).
    pub fn keys_equal(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        if self.is_null(i) || other.is_null(j) {
            return false;
        }
        self.eq_at(i, other, j)
    }

    pub fn eq_at(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        match (self, other) {
            (KeyColumn::Utf8(a), KeyColumn::Utf8(b)) => a.value(i) == b.value(j),
            (KeyColumn::Int64(a), KeyColumn::Int64(b)) => a.value(i) == b.value(j),
            _ => false,
        }
    }
}

pub(crate) fn extract_key_column<'a>(
    batch: &'a RecordBatch,
    col_name: &str,
) -> Result<KeyColumn<'a>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Column '{col_name}' not found")))?;
    let array = batch.column(col_idx);
    match array.data_type() {
        DataType::Utf8 => Ok(KeyColumn::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Utf8")))?,
        )),
        DataType::Int64 => Ok(KeyColumn::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Int64")))?,
        )),
        other => Err(DbError::Pipeline(format!(
            "Unsupported key column type for '{col_name}': {other}"
        ))),
    }
}

/// Extracts a timestamp column as `Vec<i64>` (epoch millis).
///
/// Accepts any Arrow `Timestamp(_)`; the cast kernel rescales to milliseconds.
pub(crate) fn extract_column_as_timestamps(
    batch: &RecordBatch,
    col_name: &str,
) -> Result<Vec<i64>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Timestamp column '{col_name}' not found")))?;
    let array = batch.column(col_idx);

    if !matches!(array.data_type(), DataType::Timestamp(_, _)) {
        return Err(DbError::Pipeline(format!(
            "event-time column '{col_name}' must be Timestamp(_), found {}",
            array.data_type()
        )));
    }
    if array.null_count() != 0 {
        return Err(DbError::Pipeline(format!(
            "event-time column '{col_name}' contains {} null value(s)",
            array.null_count()
        )));
    }

    cast_to_millis_array(array.as_ref())
        .map(|a| a.values().to_vec())
        .map_err(|e| DbError::Pipeline(format!("column '{col_name}': {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Float64Array, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    /// Regression: interval-join operators hit this path, and until the
    /// `Timestamp(_)` migration it only accepted `Timestamp(Millisecond)`.
    /// `_laminar_received_at` is `Timestamp(Nanosecond)` in the OTLP source;
    /// the cast kernel rescales it to millis.
    #[test]
    fn extract_timestamps_handles_nanos() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        // 1s, 2s in ns.
        let arr = TimestampNanosecondArray::from(vec![1_000_000_000, 2_000_000_000]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let result = extract_column_as_timestamps(&batch, "ts").unwrap();
        assert_eq!(result, vec![1_000, 2_000]);

        let nullable_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let nullable = TimestampNanosecondArray::from(vec![Some(1_000_000_000), None]);
        let nullable_batch =
            RecordBatch::try_new(nullable_schema, vec![Arc::new(nullable)]).unwrap();
        let error = extract_column_as_timestamps(&nullable_batch, "ts").unwrap_err();
        assert!(error.to_string().contains("contains 1 null value"));
    }

    #[test]
    fn extract_timestamps_handles_micros() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let arr = TimestampMicrosecondArray::from(vec![1_500_000]); // 1.5s in µs
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        let result = extract_column_as_timestamps(&batch, "ts").unwrap();
        assert_eq!(result, vec![1_500]);
    }

    #[test]
    fn extract_timestamps_rejects_numeric_columns() {
        for array in [
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "ts",
                array.data_type().clone(),
                false,
            )]));
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let error = extract_column_as_timestamps(&batch, "ts").unwrap_err();
            assert!(
                error.to_string().contains("must be Timestamp(_)"),
                "{error}"
            );
        }
    }
}
