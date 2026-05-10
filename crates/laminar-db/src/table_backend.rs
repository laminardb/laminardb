//! Backend storage for reference tables.
//!
//! Provides an in-memory backend for storing reference/dimension table rows.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::DbError;

/// In-memory backend storage for a single reference table.
pub(crate) struct TableBackend {
    rows: HashMap<String, RecordBatch>,
}

impl TableBackend {
    /// Create a new in-memory backend.
    pub fn in_memory() -> Self {
        Self {
            rows: HashMap::new(),
        }
    }

    /// Get a row by primary key. Test-only — production reads go through
    /// `to_record_batch`.
    #[cfg(test)]
    pub fn get(&self, key: &str) -> Option<RecordBatch> {
        self.rows.get(key).cloned()
    }

    /// Insert or update a row.
    ///
    /// Returns `true` if the key already existed (update), `false` if new.
    pub fn put(&mut self, key: &str, batch: RecordBatch) -> bool {
        self.rows.insert(key.to_string(), batch).is_some()
    }

    /// Remove a row by primary key. Returns `true` if the key existed.
    /// Test-only — production never deletes individual rows.
    #[cfg(test)]
    pub fn remove(&mut self, key: &str) -> bool {
        self.rows.remove(key).is_some()
    }

    /// Collect all keys.
    pub fn keys(&self) -> Vec<String> {
        self.rows.keys().cloned().collect()
    }

    /// Concatenate all rows into a single `RecordBatch`.
    pub fn to_record_batch(&self, schema: &SchemaRef) -> Result<RecordBatch, DbError> {
        if self.rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        let batches: Vec<&RecordBatch> = self.rows.values().collect();
        arrow::compute::concat_batches(schema, batches.iter().copied())
            .map_err(|e| DbError::Storage(format!("concat batches: {e}")))
    }

    /// Whether this backend is persistent. Always `false` for the in-memory backend.
    pub const fn is_persistent(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(id: i32, name: &str, price: f64) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
                Arc::new(Float64Array::from(vec![price])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_in_memory_crud() {
        let mut backend = TableBackend::in_memory();
        assert!(!backend.is_persistent());
        assert!(backend.keys().is_empty());

        let existed = backend.put("1", make_batch(1, "A", 1.0));
        assert!(!existed);

        let row = backend.get("1").unwrap();
        assert_eq!(row.num_rows(), 1);

        let existed = backend.put("1", make_batch(1, "B", 2.0));
        assert!(existed);

        assert!(backend.remove("1"));
        assert!(backend.get("1").is_none());
        assert!(!backend.remove("1"));
    }

    #[test]
    fn test_in_memory_to_record_batch() {
        let mut backend = TableBackend::in_memory();
        let schema = test_schema();

        let batch = backend.to_record_batch(&schema).unwrap();
        assert_eq!(batch.num_rows(), 0);

        backend.put("1", make_batch(1, "A", 1.0));
        backend.put("2", make_batch(2, "B", 2.0));
        let batch = backend.to_record_batch(&schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
