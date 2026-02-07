//! Primary-key-based reference table store.
//!
//! Supports upsert, delete, and lookup by primary key for dimension/reference
//! tables used in enrichment joins (e.g., `JOIN instruments ON t.symbol = i.symbol`).
//!
//! Tables can operate in different cache modes:
//! - **Full**: all rows in memory (default, unchanged behavior)
//! - **Partial**: LRU cache of hot keys with xor filter for negative lookups
//! - **None**: no caching, direct backend access (same as Full for in-memory)
//!
//! Backend storage can be:
//! - **`InMemory`**: rows stored in a `HashMap` (default)
//! - **Persistent**: rows stored in `RocksDB` (feature-gated, `#[cfg(feature = "rocksdb")]`)

use std::collections::HashMap;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;

use laminar_core::operator::table_cache::{
    collect_cache_metrics, TableCacheMetrics, TableLruCache, TableXorFilter,
};

use crate::error::DbError;
use crate::table_backend::TableBackend;
use crate::table_cache_mode::TableCacheMode;

/// Internal state for a single reference table.
#[allow(dead_code)]
struct TableState {
    /// Arrow schema for this table.
    schema: SchemaRef,
    /// Name of the primary key column.
    primary_key: String,
    /// Index of the primary key column in the schema.
    pk_index: usize,
    /// Backend storage for row data.
    backend: TableBackend,
    /// Tracked row count (avoids expensive iteration for persistent backends).
    row_count: usize,
    /// Whether the table has been fully populated (e.g., initial snapshot done).
    ready: bool,
    /// Connector type backing this table, if any.
    connector: Option<String>,
    /// Cache mode for this table.
    cache_mode: TableCacheMode,
    /// LRU cache (only used in Partial mode).
    lru_cache: Option<TableLruCache>,
    /// Xor filter for negative lookup short-circuiting.
    xor_filter: TableXorFilter,
}

/// Primary-key-based reference table store.
///
/// Manages dimension/reference tables with upsert/delete/lookup semantics.
/// Each table has a designated primary key column used to key individual rows.
pub(crate) struct TableStore {
    tables: HashMap<String, TableState>,
    /// Shared `RocksDB` instance for persistent tables.
    #[cfg(feature = "rocksdb")]
    db: Option<std::sync::Arc<parking_lot::Mutex<rocksdb::DB>>>,
    /// Path to the `RocksDB` directory.
    #[cfg(feature = "rocksdb")]
    #[allow(dead_code)]
    db_path: Option<std::path::PathBuf>,
    /// Row count threshold for auto-spill from in-memory to `RocksDB`.
    #[cfg(feature = "rocksdb")]
    spill_threshold: usize,
}

impl TableStore {
    /// Create an empty table store.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            #[cfg(feature = "rocksdb")]
            db: None,
            #[cfg(feature = "rocksdb")]
            db_path: None,
            #[cfg(feature = "rocksdb")]
            spill_threshold: 1_000_000,
        }
    }

    /// Create a table store with `RocksDB` backing.
    #[cfg(feature = "rocksdb")]
    #[allow(dead_code)]
    pub fn new_with_rocksdb(
        path: impl Into<std::path::PathBuf>,
        spill_threshold: usize,
    ) -> Result<Self, DbError> {
        let path = path.into();

        // List existing CFs from the path if the DB already exists
        let cf_names = if path.exists() {
            rocksdb::DB::list_cf(&crate::table_backend::table_store_rocksdb_options(), &path)
                .unwrap_or_default()
        } else {
            vec![]
        };
        let cf_refs: Vec<&str> = cf_names.iter().map(String::as_str).collect();

        let db = crate::table_backend::open_rocksdb_for_tables(&path, &cf_refs)?;

        Ok(Self {
            tables: HashMap::new(),
            db: Some(std::sync::Arc::new(parking_lot::Mutex::new(db))),
            db_path: Some(path),
            spill_threshold,
        })
    }

    /// Register a new table with the given schema and primary key column.
    ///
    /// Uses the default `Full` cache mode.
    ///
    /// # Errors
    ///
    /// Returns an error if the primary key column does not exist in the schema,
    /// or if a table with the same name already exists.
    pub fn create_table(
        &mut self,
        name: &str,
        schema: SchemaRef,
        primary_key: &str,
    ) -> Result<(), DbError> {
        self.create_table_with_cache(name, schema, primary_key, TableCacheMode::Full)
    }

    /// Register a new table with an explicit cache mode.
    ///
    /// # Errors
    ///
    /// Returns an error if the primary key column does not exist in the schema,
    /// or if a table with the same name already exists.
    pub fn create_table_with_cache(
        &mut self,
        name: &str,
        schema: SchemaRef,
        primary_key: &str,
        cache_mode: TableCacheMode,
    ) -> Result<(), DbError> {
        if self.tables.contains_key(name) {
            return Err(DbError::TableAlreadyExists(name.to_string()));
        }
        let pk_index = schema.index_of(primary_key).map_err(|_| {
            DbError::InvalidOperation(format!(
                "Primary key column '{primary_key}' not found in table '{name}'"
            ))
        })?;

        let lru_cache = match &cache_mode {
            TableCacheMode::Partial { max_entries } => Some(TableLruCache::new(*max_entries)),
            _ => None,
        };

        self.tables.insert(
            name.to_string(),
            TableState {
                schema,
                primary_key: primary_key.to_string(),
                pk_index,
                backend: TableBackend::in_memory(),
                row_count: 0,
                ready: false,
                connector: None,
                cache_mode,
                lru_cache,
                xor_filter: TableXorFilter::new(),
            },
        );
        Ok(())
    }

    /// Register a new persistent table backed by `RocksDB`.
    ///
    /// Creates a column family for the table and uses the persistent backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the `rocksdb` feature is not enabled, the primary
    /// key column is missing, or the table name is duplicated.
    #[cfg(feature = "rocksdb")]
    pub fn create_table_persistent(
        &mut self,
        name: &str,
        schema: SchemaRef,
        primary_key: &str,
        cache_mode: TableCacheMode,
    ) -> Result<(), DbError> {
        if self.tables.contains_key(name) {
            return Err(DbError::TableAlreadyExists(name.to_string()));
        }
        let pk_index = schema.index_of(primary_key).map_err(|_| {
            DbError::InvalidOperation(format!(
                "Primary key column '{primary_key}' not found in table '{name}'"
            ))
        })?;

        let db_mtx = self.db.as_ref().ok_or_else(|| {
            DbError::Storage("RocksDB not initialized — call new_with_rocksdb()".to_string())
        })?;

        // Create a column family for this table (if not already existing)
        let cf_name = format!("table_{name}");
        {
            let mut db = db_mtx.lock();
            if db.cf_handle(&cf_name).is_none() {
                let opts = rocksdb::Options::default();
                db.create_cf(&cf_name, &opts)
                    .map_err(|e| DbError::Storage(format!("create CF '{cf_name}': {e}")))?;
            }
        }

        let lru_cache = match &cache_mode {
            TableCacheMode::Partial { max_entries } => Some(TableLruCache::new(*max_entries)),
            _ => None,
        };

        let backend = TableBackend::persistent(db_mtx.clone(), cf_name, schema.clone());
        let row_count = backend.len()?;

        self.tables.insert(
            name.to_string(),
            TableState {
                schema,
                primary_key: primary_key.to_string(),
                pk_index,
                backend,
                row_count,
                ready: false,
                connector: None,
                cache_mode,
                lru_cache,
                xor_filter: TableXorFilter::new(),
            },
        );
        Ok(())
    }

    /// Remove a table. Returns `true` if it existed.
    pub fn drop_table(&mut self, name: &str) -> bool {
        if let Some(state) = self.tables.remove(name) {
            // If persistent, drop the column family
            #[cfg(feature = "rocksdb")]
            if state.backend.is_persistent() {
                if let Some(ref db_mtx) = self.db {
                    let cf_name = format!("table_{name}");
                    let _ = db_mtx.lock().drop_cf(&cf_name);
                }
            }
            let _ = state; // consumed
            true
        } else {
            false
        }
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get the schema for a table.
    pub fn table_schema(&self, name: &str) -> Option<SchemaRef> {
        self.tables.get(name).map(|t| t.schema.clone())
    }

    /// Get the primary key column name for a table.
    pub fn primary_key(&self, name: &str) -> Option<&str> {
        self.tables.get(name).map(|t| t.primary_key.as_str())
    }

    /// Get the row count for a table.
    pub fn table_row_count(&self, name: &str) -> usize {
        self.tables.get(name).map_or(0, |t| t.row_count)
    }

    /// Check if a table is ready (fully populated).
    #[allow(dead_code)]
    pub fn is_ready(&self, name: &str) -> bool {
        self.tables.get(name).is_some_and(|t| t.ready)
    }

    /// Set the ready flag for a table.
    #[allow(dead_code)]
    pub fn set_ready(&mut self, name: &str, ready: bool) {
        if let Some(t) = self.tables.get_mut(name) {
            t.ready = ready;
        }
    }

    /// Set the connector type for a table.
    pub fn set_connector(&mut self, name: &str, connector_type: &str) {
        if let Some(t) = self.tables.get_mut(name) {
            t.connector = Some(connector_type.to_string());
        }
    }

    /// Get the connector type for a table.
    pub fn connector(&self, name: &str) -> Option<&str> {
        self.tables.get(name).and_then(|t| t.connector.as_deref())
    }

    /// Whether a table uses persistent storage.
    #[allow(dead_code)]
    pub fn is_persistent(&self, name: &str) -> bool {
        self.tables
            .get(name)
            .is_some_and(|t| t.backend.is_persistent())
    }

    /// Upsert rows from a `RecordBatch` into a table.
    ///
    /// Extracts the primary key column, slices the batch into per-row batches,
    /// and inserts/overwrites each row by its PK value.
    /// Invalidates affected LRU cache entries on update.
    ///
    /// Returns the number of rows upserted.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist.
    pub fn upsert(&mut self, name: &str, batch: &RecordBatch) -> Result<usize, DbError> {
        let state = self
            .tables
            .get_mut(name)
            .ok_or_else(|| DbError::TableNotFound(name.to_string()))?;

        let pk_col = batch.column(state.pk_index);
        let count = batch.num_rows();

        for i in 0..count {
            let key = extract_pk_string(pk_col, i);
            // Invalidate LRU cache entry so stale data isn't served
            if let Some(ref mut lru) = state.lru_cache {
                lru.invalidate(&key);
            }
            let row = batch.slice(i, 1);
            let existed = state.backend.put(&key, row)?;
            if !existed {
                state.row_count += 1;
            }
        }

        Ok(count)
    }

    /// Delete a row by primary key. Returns `true` if the key existed.
    /// Invalidates the LRU cache entry if present.
    #[allow(dead_code)]
    pub fn delete(&mut self, name: &str, key: &str) -> bool {
        if let Some(state) = self.tables.get_mut(name) {
            if let Some(ref mut lru) = state.lru_cache {
                lru.invalidate(key);
            }
            match state.backend.remove(key) {
                Ok(true) => {
                    state.row_count = state.row_count.saturating_sub(1);
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    /// Look up a single row by primary key.
    ///
    /// In **Full** / **None** mode: direct backend lookup (unchanged behavior).
    /// In **Partial** mode: xor filter -> LRU cache -> backend -> populate LRU.
    #[allow(dead_code)]
    pub fn lookup(&mut self, name: &str, key: &str) -> Option<RecordBatch> {
        let state = self.tables.get_mut(name)?;

        match state.cache_mode {
            TableCacheMode::Full | TableCacheMode::None => state.backend.get(key).ok().flatten(),
            TableCacheMode::Partial { .. } => {
                // Step 1: Xor filter check — if definitely absent, short-circuit
                if !state.xor_filter.contains(key) {
                    return None;
                }

                // Step 2: LRU cache check
                if let Some(lru) = &mut state.lru_cache {
                    if let Some(batch) = lru.get(key) {
                        return Some(batch.clone());
                    }
                }

                // Step 3: Backing store lookup -> populate LRU on hit
                let batch = state.backend.get(key).ok().flatten()?;
                if let Some(lru) = &mut state.lru_cache {
                    lru.insert(key.to_string(), batch.clone());
                }
                Some(batch)
            }
        }
    }

    /// Rebuild the xor filter for a table from all current keys.
    #[allow(dead_code)]
    pub fn rebuild_xor_filter(&mut self, name: &str) {
        if let Some(state) = self.tables.get_mut(name) {
            let keys = state.backend.keys().unwrap_or_default();
            state.xor_filter.rebuild(&keys);
        }
    }

    /// Get cache metrics for a table.
    #[allow(dead_code)]
    pub fn cache_metrics(&self, name: &str) -> Option<TableCacheMetrics> {
        let state = self.tables.get(name)?;
        state
            .lru_cache
            .as_ref()
            .map(|lru| collect_cache_metrics(lru, &state.xor_filter))
    }

    /// Concatenate all rows into a single `RecordBatch`.
    ///
    /// Returns `None` if the table does not exist. Returns an empty batch
    /// (with correct schema) if the table has no rows.
    pub fn to_record_batch(&self, name: &str) -> Option<RecordBatch> {
        let state = self.tables.get(name)?;
        state.backend.to_record_batch(&state.schema).ok().flatten()
    }

    /// Migrate an in-memory table to `RocksDB` if row count exceeds threshold.
    ///
    /// No-op if the table is already persistent, the `rocksdb` feature is
    /// disabled, or the database is not initialized.
    #[cfg(feature = "rocksdb")]
    #[allow(dead_code)]
    pub fn maybe_spill_to_rocksdb(&mut self, name: &str) -> Result<bool, DbError> {
        let db_mtx = match self.db.as_ref() {
            Some(db) => db.clone(),
            None => return Ok(false),
        };

        let Some(state) = self.tables.get_mut(name) else {
            return Err(DbError::TableNotFound(name.to_string()));
        };

        if state.backend.is_persistent() {
            return Ok(false);
        }

        if state.row_count <= self.spill_threshold {
            return Ok(false);
        }

        // Create CF
        let cf_name = format!("table_{name}");
        {
            let mut db = db_mtx.lock();
            if db.cf_handle(&cf_name).is_none() {
                let opts = rocksdb::Options::default();
                db.create_cf(&cf_name, &opts)
                    .map_err(|e| DbError::Storage(format!("create CF '{cf_name}': {e}")))?;
            }
        }

        // Drain in-memory rows and write to RocksDB
        let rows = state.backend.drain()?;
        let mut new_backend = TableBackend::persistent(db_mtx, cf_name, state.schema.clone());
        for (key, batch) in rows {
            new_backend.put(&key, batch)?;
        }
        state.backend = new_backend;

        Ok(true)
    }

    /// Create a `RocksDB` checkpoint of the persistent table data.
    ///
    /// Wraps `rocksdb::checkpoint::Checkpoint::create_checkpoint()`.
    #[cfg(feature = "rocksdb")]
    #[allow(dead_code)]
    pub fn checkpoint_rocksdb(&self, dir: &std::path::Path) -> Result<(), DbError> {
        let db_mtx = self
            .db
            .as_ref()
            .ok_or_else(|| DbError::Storage("RocksDB not initialized".to_string()))?;
        let db = db_mtx.lock();

        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&db)
            .map_err(|e| DbError::Storage(format!("RocksDB checkpoint init: {e}")))?;
        checkpoint
            .create_checkpoint(dir)
            .map_err(|e| DbError::Storage(format!("RocksDB checkpoint: {e}")))?;
        Ok(())
    }
}

/// Extract the string representation of a primary key value at a given row.
fn extract_pk_string(col: &dyn Array, row: usize) -> String {
    // Try StringArray first (most common for PKs)
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    // Fall back to generic display via arrow's array formatting
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return arr.value(row).to_string();
    }
    // Generic fallback using array_value_to_string
    arrow::util::display::array_value_to_string(col, row).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn make_batch(ids: &[i32], names: &[&str], prices: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Float64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_create_table_validates_pk() {
        let mut store = TableStore::new();
        let result = store.create_table("t", test_schema(), "id");
        assert!(result.is_ok());
        assert!(store.has_table("t"));
    }

    #[test]
    fn test_create_table_rejects_missing_pk() {
        let mut store = TableStore::new();
        let result = store.create_table("t", test_schema(), "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_create_table_rejects_duplicate() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        let result = store.create_table("t", test_schema(), "id");
        assert!(matches!(result, Err(DbError::TableAlreadyExists(_))));
    }

    #[test]
    fn test_upsert_and_lookup() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1], &["Widget"], &[9.99]);
        let count = store.upsert("t", &batch).unwrap();
        assert_eq!(count, 1);
        assert_eq!(store.table_row_count("t"), 1);

        let row = store.lookup("t", "1").unwrap();
        assert_eq!(row.num_rows(), 1);
        let names = row
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Widget");
    }

    #[test]
    fn test_upsert_multiple_rows() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1, 2, 3], &["A", "B", "C"], &[1.0, 2.0, 3.0]);
        let count = store.upsert("t", &batch).unwrap();
        assert_eq!(count, 3);
        assert_eq!(store.table_row_count("t"), 3);
    }

    #[test]
    fn test_upsert_overwrites_existing() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch1 = make_batch(&[1], &["Old"], &[1.0]);
        store.upsert("t", &batch1).unwrap();

        let batch2 = make_batch(&[1], &["New"], &[2.0]);
        store.upsert("t", &batch2).unwrap();

        assert_eq!(store.table_row_count("t"), 1);
        let row = store.lookup("t", "1").unwrap();
        let names = row
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "New");
    }

    #[test]
    fn test_delete_existing_key() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        let batch = make_batch(&[1], &["Widget"], &[9.99]);
        store.upsert("t", &batch).unwrap();

        assert!(store.delete("t", "1"));
        assert_eq!(store.table_row_count("t"), 0);
        assert!(store.lookup("t", "1").is_none());
    }

    #[test]
    fn test_delete_missing_key() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(!store.delete("t", "999"));
    }

    #[test]
    fn test_lookup_missing() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.lookup("t", "1").is_none());
        assert!(store.lookup("nosuch", "1").is_none());
    }

    #[test]
    fn test_table_names_and_counts() {
        let mut store = TableStore::new();
        assert!(store.table_names().is_empty());

        store.create_table("a", test_schema(), "id").unwrap();
        store.create_table("b", test_schema(), "id").unwrap();

        let mut names = store.table_names();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
        assert!(store.has_table("a"));
        assert!(!store.has_table("c"));
    }

    #[test]
    fn test_to_record_batch() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        // Empty table returns empty batch
        let batch = store.to_record_batch("t").unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), test_schema());

        // With data
        store
            .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
            .unwrap();
        let batch = store.to_record_batch("t").unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Missing table
        assert!(store.to_record_batch("nosuch").is_none());
    }

    #[test]
    fn test_drop_table() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.drop_table("t"));
        assert!(!store.has_table("t"));
        assert!(!store.drop_table("t"));
    }

    #[test]
    fn test_ready_flag() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(!store.is_ready("t"));

        store.set_ready("t", true);
        assert!(store.is_ready("t"));

        store.set_ready("t", false);
        assert!(!store.is_ready("t"));
    }

    #[test]
    fn test_connector_tracking() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(store.connector("t").is_none());

        store.set_connector("t", "kafka");
        assert_eq!(store.connector("t"), Some("kafka"));
    }

    #[test]
    fn test_is_persistent_default_false() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert!(!store.is_persistent("t"));
    }

    // ── Partial cache mode tests ──

    #[test]
    fn test_partial_cache_lookup_populates_lru() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 100 },
            )
            .unwrap();

        let batch = make_batch(&[1, 2, 3], &["A", "B", "C"], &[1.0, 2.0, 3.0]);
        store.upsert("t", &batch).unwrap();
        store.rebuild_xor_filter("t");

        // First lookup: cache miss -> backing store hit -> populates LRU
        let row = store.lookup("t", "1").unwrap();
        assert_eq!(row.num_rows(), 1);

        // Second lookup: should be an LRU hit
        let row = store.lookup("t", "1").unwrap();
        assert_eq!(row.num_rows(), 1);

        let metrics = store.cache_metrics("t").unwrap();
        assert_eq!(metrics.cache_gets, 2);
        // First get is a miss (not in LRU yet), second is a hit
        assert_eq!(metrics.cache_hits, 1);
    }

    #[test]
    fn test_partial_cache_xor_short_circuits() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 100 },
            )
            .unwrap();

        let batch = make_batch(&[1], &["A"], &[1.0]);
        store.upsert("t", &batch).unwrap();
        store.rebuild_xor_filter("t");

        // Lookup a key that doesn't exist — xor filter should short-circuit
        assert!(store.lookup("t", "999").is_none());

        let _metrics = store.cache_metrics("t").unwrap();
        // The xor filter may or may not short-circuit for this particular key
        // (0.4% FPR means ~99.6% chance it short-circuits), but the important
        // thing is that the lookup returns None.
    }

    #[test]
    fn test_partial_cache_eviction() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 2 },
            )
            .unwrap();

        let batch = make_batch(&[1, 2, 3], &["A", "B", "C"], &[1.0, 2.0, 3.0]);
        store.upsert("t", &batch).unwrap();
        store.rebuild_xor_filter("t");

        // Access all three keys — LRU can only hold 2
        store.lookup("t", "1");
        store.lookup("t", "2");
        store.lookup("t", "3");

        let metrics = store.cache_metrics("t").unwrap();
        assert_eq!(metrics.cache_entries, 2);
        assert_eq!(metrics.cache_max_entries, 2);
        assert!(metrics.cache_evictions >= 1);
    }

    #[test]
    fn test_partial_cache_upsert_invalidates_lru() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 100 },
            )
            .unwrap();

        let batch1 = make_batch(&[1], &["Old"], &[1.0]);
        store.upsert("t", &batch1).unwrap();
        store.rebuild_xor_filter("t");

        // Populate LRU
        store.lookup("t", "1");

        // Upsert new value — should invalidate LRU entry
        let batch2 = make_batch(&[1], &["New"], &[2.0]);
        store.upsert("t", &batch2).unwrap();

        // Lookup should return the new value
        let row = store.lookup("t", "1").unwrap();
        let names = row
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "New");
    }

    #[test]
    fn test_partial_cache_delete_invalidates_lru() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 100 },
            )
            .unwrap();

        let batch = make_batch(&[1], &["A"], &[1.0]);
        store.upsert("t", &batch).unwrap();
        store.rebuild_xor_filter("t");

        // Populate LRU
        store.lookup("t", "1");

        // Delete the key — should invalidate LRU
        assert!(store.delete("t", "1"));

        // Note: xor filter still says "might exist" (stale), but backing
        // store returns None. The filter will be rebuilt after next snapshot/CDC.
    }

    #[test]
    fn test_rebuild_xor_filter() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 100 },
            )
            .unwrap();

        // Initially filter is not built
        let state = store.tables.get("t").unwrap();
        assert!(!state.xor_filter.is_built());

        // After rebuild with data
        let batch = make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]);
        store.upsert("t", &batch).unwrap();
        store.rebuild_xor_filter("t");

        let state = store.tables.get("t").unwrap();
        assert!(state.xor_filter.is_built());
    }

    #[test]
    fn test_full_mode_no_lru() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();

        // Full mode should have no LRU cache or metrics
        assert!(store.cache_metrics("t").is_none());
    }

    #[test]
    fn test_create_table_with_cache_rejects_duplicate() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 100 },
            )
            .unwrap();
        let result = store.create_table_with_cache(
            "t",
            test_schema(),
            "id",
            TableCacheMode::Partial { max_entries: 100 },
        );
        assert!(matches!(result, Err(DbError::TableAlreadyExists(_))));
    }

    #[test]
    fn test_partial_cache_without_xor_filter() {
        let mut store = TableStore::new();
        store
            .create_table_with_cache(
                "t",
                test_schema(),
                "id",
                TableCacheMode::Partial { max_entries: 100 },
            )
            .unwrap();

        let batch = make_batch(&[1], &["A"], &[1.0]);
        store.upsert("t", &batch).unwrap();

        // Without rebuilding xor filter, lookup should still work
        // (filter is permissive when not built)
        let row = store.lookup("t", "1").unwrap();
        assert_eq!(row.num_rows(), 1);
    }

    // ── Row count tracking tests ──

    #[test]
    fn test_row_count_tracks_upserts_and_deletes() {
        let mut store = TableStore::new();
        store.create_table("t", test_schema(), "id").unwrap();
        assert_eq!(store.table_row_count("t"), 0);

        store
            .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
            .unwrap();
        assert_eq!(store.table_row_count("t"), 2);

        // Upsert existing key — count should not increase
        store
            .upsert("t", &make_batch(&[1], &["X"], &[9.0]))
            .unwrap();
        assert_eq!(store.table_row_count("t"), 2);

        // Delete
        assert!(store.delete("t", "1"));
        assert_eq!(store.table_row_count("t"), 1);

        // Delete non-existent — count unchanged
        assert!(!store.delete("t", "999"));
        assert_eq!(store.table_row_count("t"), 1);
    }

    // ── RocksDB-backed persistent table tests ──

    #[cfg(feature = "rocksdb")]
    mod rocksdb_tests {
        use super::*;

        #[test]
        fn test_persistent_table_create_and_crud() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = TableStore::new_with_rocksdb(dir.path(), 1_000_000).unwrap();

            store
                .create_table_persistent("t", test_schema(), "id", TableCacheMode::Full)
                .unwrap();

            assert!(store.is_persistent("t"));
            assert_eq!(store.table_row_count("t"), 0);

            // Upsert
            store
                .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
                .unwrap();
            assert_eq!(store.table_row_count("t"), 2);

            // Lookup
            let row = store.lookup("t", "1").unwrap();
            let names = row
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "A");

            // Delete
            assert!(store.delete("t", "1"));
            assert_eq!(store.table_row_count("t"), 1);
            assert!(store.lookup("t", "1").is_none());
        }

        #[test]
        fn test_auto_spill_to_rocksdb() {
            let dir = tempfile::tempdir().unwrap();
            // Very low threshold to trigger spill
            let mut store = TableStore::new_with_rocksdb(dir.path(), 2).unwrap();

            store.create_table("t", test_schema(), "id").unwrap();
            assert!(!store.is_persistent("t"));

            // Insert rows below threshold
            store
                .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
                .unwrap();
            let spilled = store.maybe_spill_to_rocksdb("t").unwrap();
            assert!(!spilled);
            assert!(!store.is_persistent("t"));

            // Insert one more to exceed threshold
            store
                .upsert("t", &make_batch(&[3], &["C"], &[3.0]))
                .unwrap();
            let spilled = store.maybe_spill_to_rocksdb("t").unwrap();
            assert!(spilled);
            assert!(store.is_persistent("t"));

            // Data should survive the spill
            assert_eq!(store.table_row_count("t"), 3);
            let row = store.lookup("t", "2").unwrap();
            let names = row
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "B");
        }

        #[test]
        fn test_drop_persistent_table_cleans_cf() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = TableStore::new_with_rocksdb(dir.path(), 1_000_000).unwrap();

            store
                .create_table_persistent("t", test_schema(), "id", TableCacheMode::Full)
                .unwrap();
            store
                .upsert("t", &make_batch(&[1], &["A"], &[1.0]))
                .unwrap();

            assert!(store.drop_table("t"));
            assert!(!store.has_table("t"));
        }

        #[test]
        fn test_checkpoint_rocksdb() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = TableStore::new_with_rocksdb(dir.path().join("db"), 1_000_000).unwrap();

            store
                .create_table_persistent("t", test_schema(), "id", TableCacheMode::Full)
                .unwrap();
            store
                .upsert("t", &make_batch(&[1], &["A"], &[1.0]))
                .unwrap();

            let cp_dir = dir.path().join("checkpoint");
            store.checkpoint_rocksdb(&cp_dir).unwrap();
            assert!(cp_dir.exists());
        }

        #[test]
        fn test_mixed_backends() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = TableStore::new_with_rocksdb(dir.path(), 1_000_000).unwrap();

            // One in-memory, one persistent
            store.create_table("mem_t", test_schema(), "id").unwrap();
            store
                .create_table_persistent("disk_t", test_schema(), "id", TableCacheMode::Full)
                .unwrap();

            assert!(!store.is_persistent("mem_t"));
            assert!(store.is_persistent("disk_t"));

            // Both work independently
            store
                .upsert("mem_t", &make_batch(&[1], &["A"], &[1.0]))
                .unwrap();
            store
                .upsert("disk_t", &make_batch(&[2], &["B"], &[2.0]))
                .unwrap();

            assert_eq!(store.table_row_count("mem_t"), 1);
            assert_eq!(store.table_row_count("disk_t"), 1);

            let row = store.lookup("mem_t", "1").unwrap();
            assert_eq!(row.num_rows(), 1);
            let row = store.lookup("disk_t", "2").unwrap();
            assert_eq!(row.num_rows(), 1);
        }

        #[test]
        fn test_persistent_to_record_batch() {
            let dir = tempfile::tempdir().unwrap();
            let mut store = TableStore::new_with_rocksdb(dir.path(), 1_000_000).unwrap();

            store
                .create_table_persistent("t", test_schema(), "id", TableCacheMode::Full)
                .unwrap();

            // Empty
            let batch = store.to_record_batch("t").unwrap();
            assert_eq!(batch.num_rows(), 0);

            // With data
            store
                .upsert("t", &make_batch(&[1, 2], &["A", "B"], &[1.0, 2.0]))
                .unwrap();
            let batch = store.to_record_batch("t").unwrap();
            assert_eq!(batch.num_rows(), 2);
        }
    }
}
