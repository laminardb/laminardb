//! Materialized view result storage.
//!
//! Stores the latest results for each materialized view so they can be
//! queried via `SELECT * FROM mv_name`.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, VecDeque};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

use crate::error::DbError;

/// Default maximum batches retained in append mode.
const DEFAULT_APPEND_MAX_BATCHES: usize = 1000;

/// Default byte limit per MV in append mode (256 MB).
const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;

/// How a materialized view accumulates results.
#[derive(Debug, Clone)]
pub(crate) enum MvStorageMode {
    /// GROUP BY queries: replace entire result set per cycle.
    Aggregate,
    /// Non-aggregate queries: append with bounded retention.
    Append { max_batches: usize },
}

impl MvStorageMode {
    pub fn append_default() -> Self {
        Self::Append {
            max_batches: DEFAULT_APPEND_MAX_BATCHES,
        }
    }
}

/// Per-MV result store.
pub(crate) struct MvEntry {
    schema: SchemaRef,
    mode: MvStorageMode,
    batches: VecDeque<RecordBatch>,
    approx_bytes: usize,
}

impl MvEntry {
    fn new(schema: SchemaRef, mode: MvStorageMode) -> Self {
        Self {
            schema,
            mode,
            batches: VecDeque::new(),
            approx_bytes: 0,
        }
    }

    fn update(&mut self, batch: &RecordBatch) {
        match &self.mode {
            MvStorageMode::Aggregate => {
                self.batches.clear();
                self.approx_bytes = batch.get_array_memory_size();
                self.batches.push_back(batch.clone());
            }
            MvStorageMode::Append { max_batches } => {
                self.approx_bytes += batch.get_array_memory_size();
                self.batches.push_back(batch.clone());
                while self.batches.len() > *max_batches || self.approx_bytes > DEFAULT_MAX_BYTES {
                    if let Some(evicted) = self.batches.pop_front() {
                        self.approx_bytes = self
                            .approx_bytes
                            .saturating_sub(evicted.get_array_memory_size());
                    } else {
                        break;
                    }
                }
            }
        }
    }

    fn to_record_batch(&self) -> RecordBatch {
        if self.batches.is_empty() {
            return RecordBatch::new_empty(self.schema.clone());
        }
        let refs: Vec<&RecordBatch> = self.batches.iter().collect();
        arrow::compute::concat_batches(&self.schema, refs.iter().copied())
            .unwrap_or_else(|_| RecordBatch::new_empty(self.schema.clone()))
    }
}

/// Store for all materialized view results.
///
/// Shared between the compute thread (writes) and query threads (reads)
/// via `Arc<parking_lot::RwLock<MvStore>>`.
pub(crate) struct MvStore {
    entries: HashMap<String, MvEntry>,
}

impl MvStore {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn create_mv(&mut self, name: &str, schema: SchemaRef, mode: MvStorageMode) {
        self.entries
            .insert(name.to_string(), MvEntry::new(schema, mode));
    }

    pub fn drop_mv(&mut self, name: &str) -> bool {
        self.entries.remove(name).is_some()
    }

    pub fn has_mv(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    pub fn update(&mut self, name: &str, batch: &RecordBatch) {
        if let Some(entry) = self.entries.get_mut(name) {
            entry.update(batch);
        }
    }

    pub fn to_record_batch(&self, name: &str) -> Option<RecordBatch> {
        self.entries.get(name).map(MvEntry::to_record_batch)
    }

    pub fn total_bytes(&self) -> usize {
        self.entries.values().map(|e| e.approx_bytes).sum()
    }

    /// Returns per-MV IPC-serialized bytes for checkpoint.
    /// Each entry is keyed `"mv:{name}"` for use in the `operator_states` map.
    pub fn checkpoint_states(&self) -> Result<HashMap<String, Vec<u8>>, DbError> {
        let mut out = HashMap::new();
        for (name, entry) in &self.entries {
            if entry.batches.is_empty() {
                continue;
            }
            let bytes = batches_to_ipc(&entry.schema, &entry.batches)?;
            out.insert(format!("mv:{name}"), bytes);
        }
        Ok(out)
    }

    /// Restore a single MV from checkpoint IPC bytes.
    pub fn restore_from_ipc(&mut self, name: &str, bytes: &[u8]) -> Result<(), DbError> {
        let Some(entry) = self.entries.get_mut(name) else {
            return Ok(()); // MV no longer registered, skip
        };
        let batches = ipc_to_batches(bytes)
            .map_err(|e| DbError::Storage(format!("MV restore '{name}': {e}")))?;
        // Validate schema compatibility — reject stale checkpoints from
        // before a schema change rather than panicking in concat_batches.
        if let Some(first) = batches.first() {
            if first.schema() != entry.schema {
                return Err(DbError::Storage(format!(
                    "MV '{name}' schema mismatch on restore (checkpoint has {}, current has {})",
                    first.schema().fields().len(),
                    entry.schema.fields().len(),
                )));
            }
        }
        entry.batches.clear();
        entry.approx_bytes = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        entry.batches.extend(batches);
        Ok(())
    }
}

/// Prefix for MV entries in the `operator_states` checkpoint map.
pub(crate) const CHECKPOINT_KEY_PREFIX: &str = "mv:";

fn batches_to_ipc(schema: &SchemaRef, batches: &VecDeque<RecordBatch>) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| DbError::Storage(format!("IPC write: {e}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| DbError::Storage(format!("IPC write: {e}")))?;
    }
    writer
        .finish()
        .map_err(|e| DbError::Storage(format!("IPC finish: {e}")))?;
    Ok(buf)
}

fn ipc_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    reader.into_iter().collect()
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
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i32], names: &[&str], values: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn create_and_drop() {
        let mut store = MvStore::new();
        store.create_mv("mv1", test_schema(), MvStorageMode::Aggregate);
        assert!(store.has_mv("mv1"));
        assert!(store.drop_mv("mv1"));
        assert!(!store.has_mv("mv1"));
        assert!(!store.drop_mv("mv1"));
    }

    #[test]
    fn aggregate_replaces_on_each_update() {
        let mut store = MvStore::new();
        store.create_mv("agg", test_schema(), MvStorageMode::Aggregate);

        store.update("agg", &make_batch(&[1], &["a"], &[1.0]));
        assert_eq!(store.to_record_batch("agg").unwrap().num_rows(), 1);

        store.update("agg", &make_batch(&[2, 3], &["b", "c"], &[2.0, 3.0]));
        assert_eq!(store.to_record_batch("agg").unwrap().num_rows(), 2);
    }

    #[test]
    fn append_evicts_oldest() {
        let mut store = MvStore::new();
        store.create_mv(
            "app",
            test_schema(),
            MvStorageMode::Append { max_batches: 3 },
        );

        for i in 0..4 {
            store.update("app", &make_batch(&[i], &["x"], &[f64::from(i)]));
        }

        let result = store.to_record_batch("app").unwrap();
        assert_eq!(result.num_rows(), 3);

        // Batch 0 evicted, should start at 1
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
    }

    #[test]
    fn empty_mv_returns_empty_batch() {
        let mut store = MvStore::new();
        store.create_mv("empty", test_schema(), MvStorageMode::Aggregate);
        let result = store.to_record_batch("empty").unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.schema(), test_schema());
    }

    #[test]
    fn nonexistent_returns_none() {
        let store = MvStore::new();
        assert!(store.to_record_batch("nope").is_none());
    }

    #[test]
    fn checkpoint_round_trip() {
        let mut store = MvStore::new();
        store.create_mv("agg", test_schema(), MvStorageMode::Aggregate);
        store.update("agg", &make_batch(&[1, 2], &["a", "b"], &[1.0, 2.0]));

        let states = store.checkpoint_states().unwrap();
        assert_eq!(states.len(), 1);
        assert!(states.contains_key("mv:agg"));

        // Simulate recovery into a fresh store
        let mut store2 = MvStore::new();
        store2.create_mv("agg", test_schema(), MvStorageMode::Aggregate);
        for (key, bytes) in &states {
            let name = key.strip_prefix(CHECKPOINT_KEY_PREFIX).unwrap();
            store2.restore_from_ipc(name, bytes).unwrap();
        }
        assert_eq!(store2.to_record_batch("agg").unwrap().num_rows(), 2);
    }

    #[test]
    fn update_nonexistent_is_noop() {
        let mut store = MvStore::new();
        store.update("nope", &make_batch(&[1], &["a"], &[1.0]));
        assert!(!store.has_mv("nope"));
    }

    #[test]
    fn create_replaces_existing() {
        let mut store = MvStore::new();
        store.create_mv("mv1", test_schema(), MvStorageMode::Aggregate);
        store.update("mv1", &make_batch(&[1], &["a"], &[1.0]));
        assert_eq!(store.to_record_batch("mv1").unwrap().num_rows(), 1);

        store.create_mv("mv1", test_schema(), MvStorageMode::append_default());
        assert_eq!(store.to_record_batch("mv1").unwrap().num_rows(), 0);
    }

    #[test]
    fn restore_rejects_schema_mismatch() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]));

        // Serialize a batch with schema_b
        let batch_b = RecordBatch::try_new(
            schema_b.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let mut deque = VecDeque::new();
        deque.push_back(batch_b);
        let ipc_bytes = batches_to_ipc(&schema_b, &deque).unwrap();

        // Try to restore into an MV with schema_a
        let mut store = MvStore::new();
        store.create_mv("mv1", schema_a, MvStorageMode::Aggregate);
        let err = store.restore_from_ipc("mv1", &ipc_bytes);
        assert!(err.is_err(), "should reject mismatched schema");
    }
}
