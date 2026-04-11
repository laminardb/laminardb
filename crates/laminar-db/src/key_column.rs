#![deny(clippy::disallowed_types)]

//! Shared key-column utilities for streaming joins (ASOF, interval,
//! temporal probe).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use laminar_core::time::cast_to_millis_array;

use crate::error::DbError;

/// Borrowed reference to a typed key column. Avoids per-row allocation.
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

    pub fn hash_at(&self, i: usize) -> Option<u64> {
        if self.is_null(i) {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        self.hash_into(i, &mut hasher);
        Some(hasher.finish())
    }

    pub fn hash_into(&self, i: usize, hasher: &mut DefaultHasher) {
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
/// Accepts any Arrow `Timestamp(_)` (the cast kernel rescales). Also
/// accepts legacy `Int64` (treated as epoch millis) and `Float64`
/// (truncated).
pub(crate) fn extract_column_as_timestamps(
    batch: &RecordBatch,
    col_name: &str,
) -> Result<Vec<i64>, DbError> {
    let col_idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| DbError::Pipeline(format!("Timestamp column '{col_name}' not found")))?;
    let array = batch.column(col_idx);

    match array.data_type() {
        DataType::Timestamp(_, _) => cast_to_millis_array(array.as_ref())
            .map(|a| a.values().to_vec())
            .map_err(|e| DbError::Pipeline(format!("column '{col_name}': {e}"))),
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 downcast");
            Ok(a.values().to_vec())
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 downcast");
            #[allow(clippy::cast_possible_truncation)]
            Ok(a.values().iter().map(|v| *v as i64).collect())
        }
        other => Err(DbError::Pipeline(format!(
            "Unsupported timestamp column type for '{col_name}': {other}"
        ))),
    }
}

/// Gather rows by optional index, producing nulls for `None` entries.
pub(crate) fn take_with_nulls(
    array: &dyn Array,
    indices: &[Option<usize>],
    num_rows: usize,
) -> Result<ArrayRef, DbError> {
    if array.is_empty() {
        return Ok(arrow::array::new_null_array(array.data_type(), num_rows));
    }

    #[allow(clippy::cast_possible_truncation)]
    let index_array = arrow::array::UInt32Array::from(
        indices
            .iter()
            .map(|opt| opt.map(|i| i as u32))
            .collect::<Vec<Option<u32>>>(),
    );

    arrow::compute::take(array, &index_array, None)
        .map_err(|e| DbError::Pipeline(format!("take_with_nulls: {e}")))
}

/// Multi-column key extractor for composite join keys.
pub(crate) struct CompositeKey<'a> {
    columns: Vec<KeyColumn<'a>>,
}

impl<'a> CompositeKey<'a> {
    pub fn extract(batch: &'a RecordBatch, col_names: &[String]) -> Result<Self, DbError> {
        if col_names.is_empty() {
            return Err(DbError::Pipeline(
                "Composite key requires at least one column".to_string(),
            ));
        }
        let mut columns = Vec::with_capacity(col_names.len());
        for name in col_names {
            columns.push(extract_key_column(batch, name)?);
        }
        Ok(Self { columns })
    }

    pub fn hash_at(&self, i: usize) -> Option<u64> {
        if self.columns.iter().any(|c| c.is_null(i)) {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        for col in &self.columns {
            col.hash_into(i, &mut hasher);
        }
        Some(hasher.finish())
    }

    pub fn keys_equal(&self, i: usize, other: &CompositeKey<'_>, j: usize) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }
        for (a, b) in self.columns.iter().zip(other.columns.iter()) {
            if a.is_null(i) || b.is_null(j) || !a.eq_at(i, b, j) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{TimestampMicrosecondArray, TimestampNanosecondArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    /// Regression: interval-join operators hit this path, and until the
    /// `Timestamp(_)` migration it only accepted `Timestamp(Millisecond)`.
    /// The OTel example's `_laminar_received_at` is `Timestamp(Nanosecond)`;
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
}
