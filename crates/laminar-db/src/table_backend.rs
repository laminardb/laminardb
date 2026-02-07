//! Backend storage for reference tables.
//!
//! Provides an in-memory backend (default) and an optional RocksDB-backed
//! persistent backend (feature-gated behind `rocksdb`).

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
        let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, batch.schema_ref())
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
    let mut reader = arrow_ipc::reader::FileReader::try_new(cursor, None)
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

    /// RocksDB-backed persistent storage.
    #[cfg(feature = "rocksdb")]
    Persistent {
        /// Shared `RocksDB` instance (one per database), behind a mutex for CF management.
        db: std::sync::Arc<parking_lot::Mutex<rocksdb::DB>>,
        /// Column family name for this table.
        cf_name: String,
        /// Arrow schema for deserialization.
        schema: SchemaRef,
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

    /// Create a new persistent backend.
    #[cfg(feature = "rocksdb")]
    pub fn persistent(
        db: std::sync::Arc<parking_lot::Mutex<rocksdb::DB>>,
        cf_name: String,
        schema: SchemaRef,
    ) -> Self {
        Self::Persistent {
            db,
            cf_name,
            schema,
        }
    }

    /// Get a row by primary key.
    pub fn get(&self, key: &str) -> Result<Option<RecordBatch>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.get(key).cloned()),
            #[cfg(feature = "rocksdb")]
            Self::Persistent { db, cf_name, .. } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                match db.get_cf(&cf, key.as_bytes()) {
                    Ok(Some(data)) => Ok(Some(deserialize_record_batch(&data)?)),
                    Ok(None) => Ok(None),
                    Err(e) => Err(DbError::Storage(format!("RocksDB get: {e}"))),
                }
            }
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
            #[cfg(feature = "rocksdb")]
            Self::Persistent { db, cf_name, .. } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                let existed = db
                    .get_cf(&cf, key.as_bytes())
                    .map_err(|e| DbError::Storage(format!("RocksDB get: {e}")))?
                    .is_some();
                let data = serialize_record_batch(&batch)?;
                db.put_cf(&cf, key.as_bytes(), &data)
                    .map_err(|e| DbError::Storage(format!("RocksDB put: {e}")))?;
                Ok(existed)
            }
        }
    }

    /// Remove a row by primary key. Returns `true` if the key existed.
    pub fn remove(&mut self, key: &str) -> Result<bool, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.remove(key).is_some()),
            #[cfg(feature = "rocksdb")]
            Self::Persistent { db, cf_name, .. } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                let existed = db
                    .get_cf(&cf, key.as_bytes())
                    .map_err(|e| DbError::Storage(format!("RocksDB get: {e}")))?
                    .is_some();
                if existed {
                    db.delete_cf(&cf, key.as_bytes())
                        .map_err(|e| DbError::Storage(format!("RocksDB delete: {e}")))?;
                }
                Ok(existed)
            }
        }
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &str) -> Result<bool, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.contains_key(key)),
            #[cfg(feature = "rocksdb")]
            Self::Persistent { db, cf_name, .. } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                db.get_cf(&cf, key.as_bytes())
                    .map(|v| v.is_some())
                    .map_err(|e| DbError::Storage(format!("RocksDB get: {e}")))
            }
        }
    }

    /// Collect all keys.
    pub fn keys(&self) -> Result<Vec<String>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.keys().cloned().collect()),
            #[cfg(feature = "rocksdb")]
            Self::Persistent { db, cf_name, .. } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                let mut keys = Vec::new();
                for item in iter {
                    let (k, _) =
                        item.map_err(|e| DbError::Storage(format!("RocksDB iter: {e}")))?;
                    if let Ok(s) = String::from_utf8(k.to_vec()) {
                        keys.push(s);
                    }
                }
                Ok(keys)
            }
        }
    }

    /// Row count.
    pub fn len(&self) -> Result<usize, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.len()),
            #[cfg(feature = "rocksdb")]
            Self::Persistent { db, cf_name, .. } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                let mut count = 0usize;
                let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                for item in iter {
                    item.map_err(|e| DbError::Storage(format!("RocksDB iter: {e}")))?;
                    count += 1;
                }
                Ok(count)
            }
        }
    }

    /// Drain all rows from an in-memory backend and return them.
    /// For persistent backends, iterates all rows and deletes them.
    pub fn drain(&mut self) -> Result<Vec<(String, RecordBatch)>, DbError> {
        match self {
            Self::InMemory { rows } => Ok(rows.drain().collect()),
            #[cfg(feature = "rocksdb")]
            Self::Persistent { db, cf_name, .. } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                let mut items = Vec::new();
                let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                for kv in iter {
                    let (k, v) = kv.map_err(|e| DbError::Storage(format!("RocksDB iter: {e}")))?;
                    let key = String::from_utf8(k.to_vec())
                        .map_err(|e| DbError::Storage(format!("invalid UTF-8 key: {e}")))?;
                    let batch = deserialize_record_batch(&v)?;
                    items.push((key, batch));
                }
                // Delete all
                for (key, _) in &items {
                    db.delete_cf(&cf, key.as_bytes())
                        .map_err(|e| DbError::Storage(format!("RocksDB delete: {e}")))?;
                }
                Ok(items)
            }
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
            #[cfg(feature = "rocksdb")]
            Self::Persistent {
                db,
                cf_name,
                schema: s,
                ..
            } => {
                let db = db.lock();
                let cf = db.cf_handle(cf_name).ok_or_else(|| {
                    DbError::Storage(format!("column family '{cf_name}' not found"))
                })?;
                let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                let mut batches = Vec::new();
                for item in iter {
                    let (_, v) =
                        item.map_err(|e| DbError::Storage(format!("RocksDB iter: {e}")))?;
                    let batch = deserialize_record_batch(&v)?;
                    batches.push(batch);
                }
                if batches.is_empty() {
                    return Ok(Some(RecordBatch::new_empty(s.clone())));
                }
                let refs: Vec<&RecordBatch> = batches.iter().collect();
                arrow::compute::concat_batches(s, refs)
                    .map(Some)
                    .map_err(|e| DbError::Storage(format!("concat batches: {e}")))
            }
        }
    }

    /// Whether this backend is persistent.
    pub fn is_persistent(&self) -> bool {
        match self {
            Self::InMemory { .. } => false,
            #[cfg(feature = "rocksdb")]
            Self::Persistent { .. } => true,
        }
    }

    /// Whether this backend is empty.
    pub fn is_empty(&self) -> Result<bool, DbError> {
        self.len().map(|n| n == 0)
    }
}

// ── RocksDB configuration ──

/// Configure `RocksDB` options optimized for point lookups on reference tables.
#[cfg(feature = "rocksdb")]
#[allow(dead_code)]
pub(crate) fn table_store_rocksdb_options() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_max_open_files(256);
    opts.set_keep_log_file_num(3);

    // Optimize for point lookups
    opts.set_advise_random_on_open(true);

    // Direct I/O on Linux only (O_DIRECT not available on Windows)
    #[cfg(target_os = "linux")]
    opts.set_use_direct_reads(true);

    // Block-based table with bloom filter
    let cache = rocksdb::Cache::new_lru_cache(128 * 1024 * 1024); // 128 MB
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_block_cache(&cache);
    block_opts.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&block_opts);

    // LZ4 compression
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

    opts
}

/// Open or create a `RocksDB` database with the given column family names.
#[cfg(feature = "rocksdb")]
#[allow(dead_code)]
pub(crate) fn open_rocksdb_for_tables(
    path: &std::path::Path,
    cf_names: &[&str],
) -> Result<rocksdb::DB, DbError> {
    let opts = table_store_rocksdb_options();

    // Build CF descriptors: one per existing table + default
    let mut cfs: Vec<rocksdb::ColumnFamilyDescriptor> = Vec::new();
    for name in cf_names {
        let cf_opts = rocksdb::Options::default();
        cfs.push(rocksdb::ColumnFamilyDescriptor::new(*name, cf_opts));
    }

    // Always include the "default" CF
    if !cf_names.contains(&"default") {
        cfs.push(rocksdb::ColumnFamilyDescriptor::new(
            "default",
            rocksdb::Options::default(),
        ));
    }

    rocksdb::DB::open_cf_descriptors(&opts, path, cfs)
        .map_err(|e| DbError::Storage(format!("RocksDB open: {e}")))
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

    // ── RocksDB backend tests ──

    #[cfg(feature = "rocksdb")]
    mod rocksdb_tests {
        use super::*;

        fn open_test_db() -> (tempfile::TempDir, Arc<parking_lot::Mutex<rocksdb::DB>>) {
            let dir = tempfile::tempdir().unwrap();
            let db = open_rocksdb_for_tables(dir.path(), &[]).unwrap();
            (dir, Arc::new(parking_lot::Mutex::new(db)))
        }

        fn create_cf(db: &parking_lot::Mutex<rocksdb::DB>, name: &str) {
            let opts = rocksdb::Options::default();
            db.lock().create_cf(name, &opts).unwrap();
        }

        #[test]
        fn test_persistent_crud() {
            let (_dir, db) = open_test_db();
            create_cf(&db, "test_table");

            let mut backend =
                TableBackend::persistent(db.clone(), "test_table".to_string(), test_schema());
            assert!(backend.is_persistent());
            assert!(backend.is_empty().unwrap());

            // Put
            let existed = backend.put("1", make_batch(1, "A", 1.0)).unwrap();
            assert!(!existed);
            assert_eq!(backend.len().unwrap(), 1);

            // Get
            let row = backend.get("1").unwrap().unwrap();
            assert_eq!(row.num_rows(), 1);
            let names = row
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "A");

            // Contains
            assert!(backend.contains_key("1").unwrap());
            assert!(!backend.contains_key("2").unwrap());

            // Update
            let existed = backend.put("1", make_batch(1, "B", 2.0)).unwrap();
            assert!(existed);
            let row = backend.get("1").unwrap().unwrap();
            let names = row
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "B");

            // Remove
            let existed = backend.remove("1").unwrap();
            assert!(existed);
            assert!(backend.is_empty().unwrap());
        }

        #[test]
        fn test_persistent_keys_and_drain() {
            let (_dir, db) = open_test_db();
            create_cf(&db, "t");

            let mut backend = TableBackend::persistent(db.clone(), "t".to_string(), test_schema());
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
        fn test_persistent_to_record_batch() {
            let (_dir, db) = open_test_db();
            create_cf(&db, "t");

            let mut backend = TableBackend::persistent(db.clone(), "t".to_string(), test_schema());
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

        #[test]
        fn test_rocksdb_options() {
            // Just verify it doesn't panic
            let _ = table_store_rocksdb_options();
        }
    }
}
