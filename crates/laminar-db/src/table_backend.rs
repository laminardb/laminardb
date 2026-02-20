//! Backend storage for reference tables.
//!
//! Provides an in-memory backend for storing reference/dimension table rows.

use std::collections::HashMap;
use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::DbError;

// ── Arrow IPC helpers ──

/// Serialize a single `RecordBatch` to Arrow IPC bytes.
#[allow(dead_code)]
pub(crate) fn serialize_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref())
            .map_err(|e| DbError::Storage(format!("IPC writer init: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| DbError::Storage(format!("IPC write: {e}")))?;
        writer
            .finish()
            .map_err(|e| DbError::Storage(format!("IPC finish: {e}")))?;
    }
    Ok(buf)
}

/// Deserialize a `RecordBatch` from Arrow IPC bytes.
#[allow(dead_code)]
pub(crate) fn deserialize_record_batch(data: &[u8]) -> Result<RecordBatch, DbError> {
    let cursor = Cursor::new(data);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| DbError::Storage(format!("IPC reader init: {e}")))?;
    reader
        .next()
        .ok_or_else(|| DbError::Storage("IPC: no record batch in data".to_string()))?
        .map_err(|e| DbError::Storage(format!("IPC read: {e}")))
}

// ── TableBackend enum ──

/// Backend storage for a single reference table.
#[allow(dead_code)]
pub(crate) enum TableBackend {
    /// In-memory storage (default behavior).
    InMemory {
        /// Rows keyed by stringified primary key.
        rows: HashMap<String, RecordBatch>,
    },
}

#[allow(dead_code, clippy::unnecessary_wraps)]
impl TableBackend {
    /// Create a new in-memory backend.
    pub fn in_memory() -> Self {
        Self::InMemory {
            rows: HashMap::new(),
        }
    }

    /// Get a row by primary key.
    pub fn get(&self, key: &str) -> Result<Option<RecordBatch>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.get(key).cloned()),
        }
    }

    /// Insert or update a row.
    ///
    /// Returns `true` if the key already existed (update), `false` if new.
    pub fn put(&mut self, key: &str, batch: RecordBatch) -> Result<bool, DbError> {
        match self {
            Self::InMemory { rows } => {
                let existed = rows.insert(key.to_string(), batch).is_some();
                Ok(existed)
            }
        }
    }

    /// Remove a row by primary key. Returns `true` if the key existed.
    pub fn remove(&mut self, key: &str) -> Result<bool, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.remove(key).is_some()),
        }
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &str) -> Result<bool, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.contains_key(key)),
        }
    }

    /// Collect all keys.
    pub fn keys(&self) -> Result<Vec<String>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.keys().cloned().collect()),
        }
    }

    /// Row count.
    pub fn len(&self) -> Result<usize, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.len()),
        }
    }

    /// Drain all rows from the backend and return them.
    pub fn drain(&mut self) -> Result<Vec<(String, RecordBatch)>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.drain().collect()),
        }
    }

    /// Concatenate all rows into a single `RecordBatch`.
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

    /// Whether this backend is persistent.
    #[allow(clippy::unused_self)]
    pub fn is_persistent(&self) -> bool {
        false
    }

    /// Whether this backend is empty.
    pub fn is_empty(&self) -> Result<bool, DbError> {
        self.len().map(|n| n == 0)
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

    // ── IPC round-trip tests ──

    #[test]
    fn test_ipc_round_trip_basic() {
        let batch = make_batch(1, "Widget", 9.99);
        let data = serialize_record_batch(&batch).unwrap();
        let restored = deserialize_record_batch(&data).unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(restored.schema(), batch.schema());
        let ids = restored
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
    }

    #[test]
    fn test_ipc_round_trip_multiple_types() {
        let batch = make_batch(42, "Gadget", 123.456);
        let data = serialize_record_batch(&batch).unwrap();
        let restored = deserialize_record_batch(&data).unwrap();

        let names = restored
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Gadget");

        let prices = restored
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((prices.value(0) - 123.456).abs() < f64::EPSILON);
    }

    #[test]
    fn test_ipc_round_trip_empty_batch() {
        let batch = RecordBatch::new_empty(test_schema());
        let data = serialize_record_batch(&batch).unwrap();
        let restored = deserialize_record_batch(&data).unwrap();
        assert_eq!(restored.num_rows(), 0);
        assert_eq!(restored.schema(), test_schema());
    }

    // ── InMemory backend tests ──

    #[test]
    fn test_in_memory_crud() {
        let mut backend = TableBackend::in_memory();
        assert!(!backend.is_persistent());
        assert!(backend.is_empty().unwrap());

        // Put
        let existed = backend.put("1", make_batch(1, "A", 1.0)).unwrap();
        assert!(!existed);
        assert_eq!(backend.len().unwrap(), 1);

        // Get
        let row = backend.get("1").unwrap().unwrap();
        assert_eq!(row.num_rows(), 1);

        // Contains
        assert!(backend.contains_key("1").unwrap());
        assert!(!backend.contains_key("2").unwrap());

        // Update
        let existed = backend.put("1", make_batch(1, "B", 2.0)).unwrap();
        assert!(existed);
        assert_eq!(backend.len().unwrap(), 1);

        // Remove
        let existed = backend.remove("1").unwrap();
        assert!(existed);
        assert!(backend.is_empty().unwrap());

        // Remove missing
        let existed = backend.remove("1").unwrap();
        assert!(!existed);
    }

    #[test]
    fn test_in_memory_keys_and_drain() {
        let mut backend = TableBackend::in_memory();
        backend.put("a", make_batch(1, "A", 1.0)).unwrap();
        backend.put("b", make_batch(2, "B", 2.0)).unwrap();

        let mut keys = backend.keys().unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a", "b"]);

        let items = backend.drain().unwrap();
        assert_eq!(items.len(), 2);
        assert!(backend.is_empty().unwrap());
    }

    #[test]
    fn test_in_memory_to_record_batch() {
        let mut backend = TableBackend::in_memory();
        let schema = test_schema();

        // Empty
        let batch = backend.to_record_batch(&schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 0);

        // With data
        backend.put("1", make_batch(1, "A", 1.0)).unwrap();
        backend.put("2", make_batch(2, "B", 2.0)).unwrap();
        let batch = backend.to_record_batch(&schema).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
