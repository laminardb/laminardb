#![deny(clippy::disallowed_types)]

//! Shared key-column helpers for hash-based joins.
//!
//! [`KeyColumn`] avoids per-row `String` allocations by borrowing typed
//! Arrow arrays directly. Used by ASOF, interval, and temporal probe joins.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;

use crate::error::DbError;

/// A borrowed reference to a key column, avoiding per-row String allocations.
pub(crate) enum KeyColumn<'a> {
    /// UTF-8 string key.
    Utf8(&'a StringArray),
    /// 64-bit integer key.
    Int64(&'a Int64Array),
}

impl KeyColumn<'_> {
    /// Returns true if the key at row `i` is null.
    pub fn is_null(&self, i: usize) -> bool {
        match self {
            KeyColumn::Utf8(a) => a.is_null(i),
            KeyColumn::Int64(a) => a.is_null(i),
        }
    }

    /// Computes a hash for the key at row `i`. Returns `None` for null keys.
    pub fn hash_at(&self, i: usize) -> Option<u64> {
        if self.is_null(i) {
            return None;
        }
        let mut hasher = DefaultHasher::new();
        self.hash_into(i, &mut hasher);
        Some(hasher.finish())
    }

    /// Hashes the key at row `i` into an existing hasher (for composite keys).
    pub fn hash_into(&self, i: usize, hasher: &mut DefaultHasher) {
        match self {
            KeyColumn::Utf8(a) => a.value(i).hash(hasher),
            KeyColumn::Int64(a) => a.value(i).hash(hasher),
        }
    }

    /// Returns true if keys at the given indices are equal.
    /// Returns false if either key is null (SQL three-valued logic).
    pub fn keys_equal(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        if self.is_null(i) || other.is_null(j) {
            return false;
        }
        self.eq_at(i, other, j)
    }

    /// Raw equality check without null handling (caller must check nulls).
    pub fn eq_at(&self, i: usize, other: &KeyColumn<'_>, j: usize) -> bool {
        match (self, other) {
            (KeyColumn::Utf8(a), KeyColumn::Utf8(b)) => a.value(i) == b.value(j),
            (KeyColumn::Int64(a), KeyColumn::Int64(b)) => a.value(i) == b.value(j),
            _ => false,
        }
    }
}

/// Extracts a key column from a `RecordBatch` without per-row allocation.
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
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Utf8")))?;
            Ok(KeyColumn::Utf8(a))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DbError::Pipeline(format!("Column '{col_name}' is not Int64")))?;
            Ok(KeyColumn::Int64(a))
        }
        other => Err(DbError::Pipeline(format!(
            "Unsupported key column type: {other}"
        ))),
    }
}

/// Multi-column key extractor for composite join keys.
pub(crate) struct CompositeKey<'a> {
    columns: Vec<KeyColumn<'a>>,
}

impl<'a> CompositeKey<'a> {
    /// Extract composite key columns from a batch.
    pub fn extract(batch: &'a RecordBatch, col_names: &[String]) -> Result<Self, DbError> {
        let mut columns = Vec::with_capacity(col_names.len());
        for name in col_names {
            columns.push(extract_key_column(batch, name)?);
        }
        Ok(Self { columns })
    }

    /// Computes a hash for the composite key at row `i`. Returns `None` if any column is null.
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

    /// Returns true if composite keys at the given indices are equal.
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
