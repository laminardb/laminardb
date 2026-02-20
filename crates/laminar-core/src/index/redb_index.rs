//! Single secondary index backed by redb.
//!
//! Each `SecondaryIndex` maps encoded key bytes to a set of row offsets.
//! The key encoding uses [`super::encoding`] for memcomparable ordering.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::encoding::encode_comparable;
use crate::lookup::ScalarValue;

/// Result type for range scans: `(key_bytes, row_offsets)` pairs.
pub type RangeResult = Vec<(Vec<u8>, Vec<u64>)>;

/// Error type for index operations.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    /// redb storage error.
    #[error("redb error: {0}")]
    Redb(#[from] redb::DatabaseError),

    /// redb table error.
    #[error("redb table error: {0}")]
    Table(#[from] redb::TableError),

    /// redb storage error.
    #[error("redb storage error: {0}")]
    Storage(#[from] redb::StorageError),

    /// redb commit error.
    #[error("redb commit error: {0}")]
    Commit(#[from] redb::CommitError),

    /// redb transaction error.
    #[error("redb transaction error: {0}")]
    Transaction(Box<redb::TransactionError>),

    /// Index not found.
    #[error("index not found: {0}")]
    NotFound(String),
}

/// A secondary index for a single column, backed by redb.
///
/// Keys are memcomparable-encoded column values. Values are
/// little-endian encoded lists of `u64` row offsets.
pub struct SecondaryIndex {
    db: Arc<Database>,
    table_name: String,
}

impl SecondaryIndex {
    /// Creates a new secondary index, eagerly creating the redb table.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` if the table cannot be created.
    pub fn new(db: Arc<Database>, table_name: String) -> Result<Self, IndexError> {
        // Eagerly create the table so that lookup/range work even before any inserts.
        {
            let table_def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&table_name);
            let txn = db.begin_write().map_err(box_txn_error)?;
            let _ = txn.open_table(table_def)?;
            txn.commit()?;
        }
        Ok(Self { db, table_name })
    }

    /// Inserts a single key -> row offset mapping.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` on storage failure.
    pub fn insert(&self, key: &[u8], row_offset: u64) -> Result<(), IndexError> {
        let table_name = self.table_name.clone();
        let table_def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&table_name);
        let txn = self.db.begin_write().map_err(box_txn_error)?;
        {
            let mut table = txn.open_table(table_def)?;
            let mut offsets = read_offsets_from(&table, key)?;
            if !offsets.contains(&row_offset) {
                offsets.push(row_offset);
            }
            table.insert(key, encode_offsets(&offsets).as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Removes a single key -> row offset mapping.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` on storage failure.
    pub fn remove(&self, key: &[u8], row_offset: u64) -> Result<(), IndexError> {
        let table_name = self.table_name.clone();
        let table_def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&table_name);
        let txn = self.db.begin_write().map_err(box_txn_error)?;
        {
            let mut table = txn.open_table(table_def)?;
            let mut offsets = read_offsets_from(&table, key)?;
            offsets.retain(|&o| o != row_offset);
            if offsets.is_empty() {
                table.remove(key)?;
            } else {
                table.insert(key, encode_offsets(&offsets).as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Looks up all row offsets for a given key.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` on storage failure.
    pub fn lookup(&self, key: &[u8]) -> Result<Vec<u64>, IndexError> {
        let table_name = self.table_name.clone();
        let table_def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&table_name);
        let txn = self.db.begin_read().map_err(box_txn_error)?;
        let table = txn.open_table(table_def)?;
        read_offsets_from(&table, key)
    }

    /// Range scan returning `(key_bytes, offsets)` pairs for keys in `[start, end)`.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` on storage failure.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<RangeResult, IndexError> {
        let table_name = self.table_name.clone();
        let table_def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&table_name);
        let txn = self.db.begin_read().map_err(box_txn_error)?;
        let table = txn.open_table(table_def)?;
        let mut results = Vec::new();

        let range = table.range(start..end)?;
        for entry in range {
            let entry = entry?;
            let k = entry.0.value().to_vec();
            let offsets = decode_offsets(entry.1.value());
            results.push((k, offsets));
        }

        Ok(results)
    }

    /// Bulk-updates the index from a `RecordBatch` column.
    ///
    /// Inserts index entries for each non-null value in the specified column,
    /// mapping to sequential row offsets starting from `start_offset`.
    ///
    /// # Errors
    ///
    /// Returns `IndexError` on storage failure.
    pub fn update_batch(
        &self,
        batch: &arrow::array::RecordBatch,
        column_idx: usize,
        start_offset: u64,
    ) -> Result<(), IndexError> {
        let col = batch.column(column_idx);
        let data_type = col.data_type().clone();
        let table_name = self.table_name.clone();
        let table_def: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new(&table_name);
        let txn = self.db.begin_write().map_err(box_txn_error)?;
        {
            let mut table = txn.open_table(table_def)?;
            for row in 0..col.len() {
                if col.is_null(row) {
                    continue;
                }
                let sv = extract_scalar(col.as_ref(), &data_type, row);
                let key = encode_comparable(&sv);
                let offset = start_offset + row as u64;

                let mut offsets = read_offsets_from(&table, &key)?;
                if !offsets.contains(&offset) {
                    offsets.push(offset);
                }
                table.insert(key.as_slice(), encode_offsets(&offsets).as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }
}

/// Extracts a `ScalarValue` from an Arrow array at the given row index.
fn extract_scalar(array: &dyn Array, data_type: &DataType, row: usize) -> ScalarValue {
    match data_type {
        DataType::Boolean => ScalarValue::Bool(array.as_boolean().value(row)),
        DataType::Int8 => ScalarValue::Int64(i64::from(
            array
                .as_primitive::<arrow::datatypes::Int8Type>()
                .value(row),
        )),
        DataType::Int16 => ScalarValue::Int64(i64::from(
            array
                .as_primitive::<arrow::datatypes::Int16Type>()
                .value(row),
        )),
        DataType::Int32 => ScalarValue::Int64(i64::from(
            array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row),
        )),
        DataType::Int64 => ScalarValue::Int64(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::UInt8 => ScalarValue::Int64(i64::from(
            array
                .as_primitive::<arrow::datatypes::UInt8Type>()
                .value(row),
        )),
        DataType::UInt16 => ScalarValue::Int64(i64::from(
            array
                .as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row),
        )),
        DataType::UInt32 => ScalarValue::Int64(i64::from(
            array
                .as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row),
        )),
        DataType::UInt64 => {
            // May truncate for very large u64, but lookup ScalarValue uses i64
            let v = array
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row);
            ScalarValue::Int64(v.cast_signed())
        }
        DataType::Float32 => ScalarValue::Float64(f64::from(
            array
                .as_primitive::<arrow::datatypes::Float32Type>()
                .value(row),
        )),
        DataType::Float64 => ScalarValue::Float64(
            array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row),
        ),
        DataType::Utf8 => ScalarValue::Utf8(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => ScalarValue::Utf8(array.as_string::<i64>().value(row).to_string()),
        DataType::Binary => ScalarValue::Binary(array.as_binary::<i32>().value(row).to_vec()),
        DataType::LargeBinary => ScalarValue::Binary(array.as_binary::<i64>().value(row).to_vec()),
        _ => ScalarValue::Null,
    }
}

fn read_offsets_from<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    key: &[u8],
) -> Result<Vec<u64>, IndexError> {
    match table.get(key)? {
        Some(guard) => Ok(decode_offsets(guard.value())),
        None => Ok(Vec::new()),
    }
}

fn box_txn_error(e: redb::TransactionError) -> IndexError {
    IndexError::Transaction(Box::new(e))
}

/// Encodes a list of u64 offsets as little-endian bytes.
fn encode_offsets(offsets: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(offsets.len() * 8);
    for &o in offsets {
        out.extend_from_slice(&o.to_le_bytes());
    }
    out
}

/// Decodes little-endian bytes back into u64 offsets.
fn decode_offsets(data: &[u8]) -> Vec<u64> {
    data.chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("8-byte chunk")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> Arc<Database> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");
        // Leak the tempdir so it persists for the test
        std::mem::forget(dir);
        Arc::new(Database::create(path).unwrap())
    }

    #[test]
    fn test_insert_and_lookup() {
        let db = temp_db();
        let idx = SecondaryIndex::new(db, "test_idx".to_string()).unwrap();

        idx.insert(b"key1", 10).unwrap();
        idx.insert(b"key1", 20).unwrap();
        idx.insert(b"key2", 30).unwrap();

        let offsets = idx.lookup(b"key1").unwrap();
        assert_eq!(offsets, vec![10, 20]);

        let offsets = idx.lookup(b"key2").unwrap();
        assert_eq!(offsets, vec![30]);

        let offsets = idx.lookup(b"key3").unwrap();
        assert!(offsets.is_empty());
    }

    #[test]
    fn test_remove() {
        let db = temp_db();
        let idx = SecondaryIndex::new(db, "test_idx".to_string()).unwrap();

        idx.insert(b"key1", 10).unwrap();
        idx.insert(b"key1", 20).unwrap();

        idx.remove(b"key1", 10).unwrap();
        let offsets = idx.lookup(b"key1").unwrap();
        assert_eq!(offsets, vec![20]);

        // Remove last offset -> key is cleaned up
        idx.remove(b"key1", 20).unwrap();
        let offsets = idx.lookup(b"key1").unwrap();
        assert!(offsets.is_empty());
    }

    #[test]
    fn test_range_scan() {
        let db = temp_db();
        let idx = SecondaryIndex::new(db, "test_idx".to_string()).unwrap();

        idx.insert(b"a", 1).unwrap();
        idx.insert(b"b", 2).unwrap();
        idx.insert(b"c", 3).unwrap();
        idx.insert(b"d", 4).unwrap();

        let results = idx.range(b"b", b"d").unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"b");
        assert_eq!(results[0].1, vec![2]);
        assert_eq!(results[1].0, b"c");
        assert_eq!(results[1].1, vec![3]);
    }

    #[test]
    fn test_batch_update() {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc as StdArc;

        let db = temp_db();
        let idx = SecondaryIndex::new(db, "batch_idx".to_string()).unwrap();

        let schema = StdArc::new(Schema::new(vec![Field::new("val", DataType::Int64, true)]));
        let col = Int64Array::from(vec![Some(100), None, Some(200), Some(100)]);
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(col)]).unwrap();

        idx.update_batch(&batch, 0, 0).unwrap();

        // key for 100 should have offsets [0, 3]
        let key_100 = encode_comparable(&ScalarValue::Int64(100));
        let offsets = idx.lookup(&key_100).unwrap();
        assert_eq!(offsets, vec![0, 3]);

        // key for 200 should have offset [2]
        let key_200 = encode_comparable(&ScalarValue::Int64(200));
        let offsets = idx.lookup(&key_200).unwrap();
        assert_eq!(offsets, vec![2]);
    }

    #[test]
    fn test_duplicate_insert_is_idempotent() {
        let db = temp_db();
        let idx = SecondaryIndex::new(db, "test_idx".to_string()).unwrap();

        idx.insert(b"key1", 10).unwrap();
        idx.insert(b"key1", 10).unwrap();

        let offsets = idx.lookup(b"key1").unwrap();
        assert_eq!(offsets, vec![10]);
    }
}
