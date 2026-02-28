//! Index lifecycle manager.
//!
//! Manages multiple secondary indexes, handling creation, deletion,
//! batch updates, and lookups across source/column pairs.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use redb::Database;
use rustc_hash::FxHashMap;

use super::encoding::encode_comparable;
use super::redb_index::{IndexError, SecondaryIndex};
use crate::lookup::ScalarValue;

/// Manages secondary indexes for lookup tables.
///
/// Each index is identified by a `(source_id, column_name)` pair and backed
/// by a redb table within a shared database file.
pub struct IndexManager {
    db: Arc<Database>,
    indexes: FxHashMap<IndexKey, IndexEntry>,
}

/// Composite key for index lookup: `(source_id, column_name)`.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct IndexKey {
    source_id: String,
    column_name: String,
}

/// Metadata about a registered index.
struct IndexEntry {
    index: SecondaryIndex,
    #[allow(dead_code)]
    data_type: DataType,
}

impl IndexManager {
    /// Creates a new index manager with a redb database at the given path.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` if the database cannot be created.
    pub fn new(path: PathBuf) -> Result<Self, IndexError> {
        let db = Database::create(path).map_err(IndexError::Redb)?;
        Ok(Self {
            db: Arc::new(db),
            indexes: FxHashMap::default(),
        })
    }

    /// Creates a new index manager from an existing redb database.
    #[must_use]
    pub fn from_database(db: Arc<Database>) -> Self {
        Self {
            db,
            indexes: FxHashMap::default(),
        }
    }

    /// Creates a secondary index for a column.
    ///
    /// The redb table name is derived as `{source_id}__{column_name}`.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` if the index table cannot be created.
    pub fn create_index(
        &mut self,
        source_id: &str,
        column_name: &str,
        data_type: DataType,
    ) -> Result<(), IndexError> {
        let key = IndexKey {
            source_id: source_id.to_string(),
            column_name: column_name.to_string(),
        };
        let table_name = format!("{source_id}__{column_name}");
        let index = SecondaryIndex::new(Arc::clone(&self.db), table_name)?;

        self.indexes.insert(key, IndexEntry { index, data_type });
        Ok(())
    }

    /// Drops a secondary index.
    pub fn drop_index(&mut self, source_id: &str, column_name: &str) {
        let key = IndexKey {
            source_id: source_id.to_string(),
            column_name: column_name.to_string(),
        };
        self.indexes.remove(&key);
    }

    /// Batch-updates all indexes for a source using a `RecordBatch`.
    ///
    /// For each registered index on the given source, finds the matching column
    /// in the batch and calls `update_batch`.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` on storage failure.
    pub fn update(
        &self,
        source_id: &str,
        batch: &RecordBatch,
        start_offset: u64,
    ) -> Result<(), IndexError> {
        let schema = batch.schema();
        for (key, entry) in &self.indexes {
            if key.source_id != source_id {
                continue;
            }
            if let Some((col_idx, _)) = schema.column_with_name(&key.column_name) {
                entry.index.update_batch(batch, col_idx, start_offset)?;
            }
        }
        Ok(())
    }

    /// Point lookup on an index: returns matching row offsets.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` on storage failure or if the index doesn't exist.
    pub fn lookup(
        &self,
        source_id: &str,
        column_name: &str,
        value: &ScalarValue,
    ) -> Result<Vec<u64>, IndexError> {
        let key = IndexKey {
            source_id: source_id.to_string(),
            column_name: column_name.to_string(),
        };
        let entry = self
            .indexes
            .get(&key)
            .ok_or_else(|| IndexError::NotFound(format!("{source_id}.{column_name}")))?;
        let encoded_key = encode_comparable(value);
        entry.index.lookup(&encoded_key)
    }

    /// Checks whether an index exists for the given source/column.
    #[must_use]
    pub fn has_index(&self, source_id: &str, column_name: &str) -> bool {
        let key = IndexKey {
            source_id: source_id.to_string(),
            column_name: column_name.to_string(),
        };
        self.indexes.contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};

    fn temp_manager() -> IndexManager {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");
        std::mem::forget(dir);
        IndexManager::new(path).unwrap()
    }

    #[test]
    fn test_create_and_drop_lifecycle() {
        let mut mgr = temp_manager();

        mgr.create_index("orders", "customer_id", DataType::Int64)
            .unwrap();
        assert!(mgr.has_index("orders", "customer_id"));

        mgr.drop_index("orders", "customer_id");
        assert!(!mgr.has_index("orders", "customer_id"));
    }

    #[test]
    fn test_batch_update_and_lookup() {
        let mut mgr = temp_manager();
        mgr.create_index("orders", "amount", DataType::Int64)
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )]));
        let col = Int64Array::from(vec![100, 200, 100, 300]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

        mgr.update("orders", &batch, 0).unwrap();

        let hits = mgr
            .lookup("orders", "amount", &ScalarValue::Int64(100))
            .unwrap();
        assert_eq!(hits, vec![0, 2]);

        let hits = mgr
            .lookup("orders", "amount", &ScalarValue::Int64(300))
            .unwrap();
        assert_eq!(hits, vec![3]);
    }

    #[test]
    fn test_update_ignores_unindexed_sources() {
        let mut mgr = temp_manager();
        mgr.create_index("orders", "amount", DataType::Int64)
            .unwrap();

        // A batch for a different source should be a no-op
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            false,
        )]));
        let col = Int64Array::from(vec![999]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();

        mgr.update("payments", &batch, 0).unwrap(); // no-op

        // Original index should be empty
        let hits = mgr
            .lookup("orders", "amount", &ScalarValue::Int64(999))
            .unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn test_lookup_missing_index() {
        let mgr = temp_manager();
        let result = mgr.lookup("missing", "col", &ScalarValue::Int64(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_indexes_same_source() {
        let mut mgr = temp_manager();
        mgr.create_index("orders", "customer_id", DataType::Int64)
            .unwrap();
        mgr.create_index("orders", "product_id", DataType::Int64)
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("product_id", DataType::Int64, false),
        ]));
        let c1 = Int64Array::from(vec![10, 20, 10]);
        let c2 = Int64Array::from(vec![100, 100, 200]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(c1), Arc::new(c2)]).unwrap();

        mgr.update("orders", &batch, 0).unwrap();

        let hits = mgr
            .lookup("orders", "customer_id", &ScalarValue::Int64(10))
            .unwrap();
        assert_eq!(hits, vec![0, 2]);

        let hits = mgr
            .lookup("orders", "product_id", &ScalarValue::Int64(100))
            .unwrap();
        assert_eq!(hits, vec![0, 1]);
    }
}
