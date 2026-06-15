//! Backend storage for reference/dimension table rows.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::DbError;

pub(crate) enum TableBackend {
    InMemory { rows: HashMap<String, RecordBatch> },
}

// `get`/`remove` are used by table_store read paths not reachable in minimal builds.
#[allow(dead_code, clippy::unnecessary_wraps)]
impl TableBackend {
    pub fn in_memory() -> Self {
        Self::InMemory {
            rows: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<RecordBatch>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.get(key).cloned()),
        }
    }

    /// Insert or update a row; returns `true` if the key existed.
    pub fn put(&mut self, key: &str, batch: RecordBatch) -> Result<bool, DbError> {
        match self {
            Self::InMemory { rows } => {
                let existed = rows.insert(key.to_string(), batch).is_some();
                Ok(existed)
            }
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<bool, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.remove(key).is_some()),
        }
    }

    pub fn keys(&self) -> Result<Vec<String>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.keys().cloned().collect()),
        }
    }

    pub fn to_record_batch(&self, schema: &SchemaRef) -> Result<Option<RecordBatch>, DbError> {
        match self {
            Self::InMemory { rows } => {
                if rows.is_empty() {
                    return Ok(Some(RecordBatch::new_empty(schema.clone())));
                }
                let batches: Vec<&RecordBatch> = rows.values().collect();
                arrow::compute::concat_batches(schema, batches.iter().copied())
                    .map(Some)
                    .map_err(|e| DbError::Storage(format!("concat batches: {e}")))
            }
        }
    }

    #[allow(clippy::unused_self)]
    pub fn is_persistent(&self) -> bool {
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
    fn in_memory_backend_round_trips_the_used_api() {
        let mut backend = TableBackend::in_memory();
        assert!(!backend.is_persistent());

        // Insert (new) then update (existing) report the right prior state.
        assert!(!backend.put("1", make_batch(1, "A", 1.0)).unwrap());
        assert!(backend.put("1", make_batch(1, "B", 2.0)).unwrap());

        assert_eq!(backend.get("1").unwrap().unwrap().num_rows(), 1);
        assert_eq!(backend.keys().unwrap(), vec!["1".to_string()]);

        // Empty schema-only batch when present; row when populated.
        let schema = test_schema();
        backend.put("2", make_batch(2, "C", 3.0)).unwrap();
        assert_eq!(
            backend
                .to_record_batch(&schema)
                .unwrap()
                .unwrap()
                .num_rows(),
            2
        );

        assert!(backend.remove("1").unwrap());
        assert!(!backend.remove("1").unwrap());
    }
}
