//! Primary-key-based reference table store for dimension/enrichment tables.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;

use crate::error::DbError;
use crate::table_backend::TableBackend;
use crate::table_cache_mode::TableCacheMode;

struct TableState {
    schema: SchemaRef,
    primary_key: String,
    pk_index: usize,
    backend: TableBackend,
    row_count: usize,
    ready: bool,
    connector: Option<String>,
    #[cfg_attr(not(test), allow(dead_code))]
    cache_mode: TableCacheMode,
}

pub(crate) struct TableStore {
    tables: HashMap<String, TableState>,
}

impl TableStore {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a new table with the given schema and primary key column.
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
            },
        );
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn table_schema(&self, name: &str) -> Option<SchemaRef> {
        self.tables.get(name).map(|t| t.schema.clone())
    }

    pub fn primary_key(&self, name: &str) -> Option<&str> {
        self.tables.get(name).map(|t| t.primary_key.as_str())
    }

    pub fn table_row_count(&self, name: &str) -> usize {
        self.tables.get(name).map_or(0, |t| t.row_count)
    }

    #[cfg(test)]
    pub fn is_ready(&self, name: &str) -> bool {
        self.tables.get(name).is_some_and(|t| t.ready)
    }

    pub fn set_ready(&mut self, name: &str, ready: bool) {
        if let Some(t) = self.tables.get_mut(name) {
            t.ready = ready;
        }
    }

    pub fn set_connector(&mut self, name: &str, connector_type: &str) {
        if let Some(t) = self.tables.get_mut(name) {
            t.connector = Some(connector_type.to_string());
        }
    }

    pub fn connector(&self, name: &str) -> Option<&str> {
        self.tables.get(name).and_then(|t| t.connector.as_deref())
    }

    pub fn is_persistent(&self, name: &str) -> bool {
        self.tables
            .get(name)
            .is_some_and(|t| t.backend.is_persistent())
    }

    /// Upsert rows from a `RecordBatch`, keyed by the primary key column.
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
            let row = batch.slice(i, 1);
            let existed = state.backend.put(&key, row)?;
            if !existed {
                state.row_count += 1;
            }
        }

        Ok(count)
    }

    /// Delete a row by primary key. Returns `true` if the key existed.
    #[cfg(test)]
    pub fn delete(&mut self, name: &str, key: &str) -> bool {
        if let Some(state) = self.tables.get_mut(name) {
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
    #[cfg(test)]
    pub fn lookup(&mut self, name: &str, key: &str) -> Option<RecordBatch> {
        let state = self.tables.get_mut(name)?;
        state.backend.get(key).ok().flatten()
    }

    /// Upsert a batch; returns the row count.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist.
    pub fn upsert_and_rebuild(
        &mut self,
        name: &str,
        batch: &RecordBatch,
    ) -> Result<usize, DbError> {
        self.upsert(name, batch)
    }

    // No-op since removing hand-rolled caches.
    #[allow(clippy::unused_self)]
    pub fn rebuild_xor_filter(&mut self, _name: &str) {}

    #[cfg(test)]
    pub fn cache_mode(&self, name: &str) -> Option<&TableCacheMode> {
        self.tables.get(name).map(|t| &t.cache_mode)
    }

    pub fn to_record_batch(&self, name: &str) -> Option<RecordBatch> {
        let state = self.tables.get(name)?;
        state.backend.to_record_batch(&state.schema).ok().flatten()
    }
}

fn extract_pk_string(col: &dyn Array, row: usize) -> String {
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        return arr.value(row).to_string();
    }
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
}
