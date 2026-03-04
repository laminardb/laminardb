//! Key-based row routing for per-core state partitioning.
//!
//! Splits a `RecordBatch` into per-core sub-batches based on a hash of
//! the specified key columns, ensuring that rows with the same key always
//! route to the same core.

use arrow::compute::take;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{Array, RecordBatch, UInt32Array};
use std::hash::{Hash, Hasher};

use super::router::{KeySpec, RouterError};

/// Routes `RecordBatch` rows to cores based on key columns.
///
/// Uses `FxHasher` for fast, deterministic hashing. Same key always maps
/// to the same core, guaranteeing state locality.
pub struct PartitionedRouter {
    key_spec: KeySpec,
    num_cores: usize,
}

impl PartitionedRouter {
    /// Create a new router.
    ///
    /// # Panics
    ///
    /// Panics if `num_cores` is zero.
    #[must_use]
    pub fn new(key_spec: KeySpec, num_cores: usize) -> Self {
        assert!(num_cores > 0, "num_cores must be > 0");
        Self {
            key_spec,
            num_cores,
        }
    }

    /// Split a batch into per-core sub-batches.
    ///
    /// Returns a vec of `(core_id, RecordBatch)` — one entry per core that
    /// has rows. Cores with zero rows are omitted.
    ///
    /// # Errors
    ///
    /// Returns an error if key columns are not found or have unsupported types.
    pub fn route_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<Vec<(usize, RecordBatch)>, RouterError> {
        if batch.num_rows() == 0 {
            return Err(RouterError::EmptyBatch);
        }

        match &self.key_spec {
            KeySpec::RoundRobin => {
                // Round-robin: not partitioned, return as-is to core 0
                Ok(vec![(0, batch.clone())])
            }
            KeySpec::Columns(names) => {
                let indices: Vec<usize> = names
                    .iter()
                    .map(|name| {
                        batch
                            .schema()
                            .index_of(name)
                            .map_err(|_| RouterError::ColumnNotFoundByName)
                    })
                    .collect::<Result<_, _>>()?;
                self.route_by_indices(batch, &indices)
            }
            KeySpec::ColumnIndices(indices) => {
                for &idx in indices {
                    if idx >= batch.num_columns() {
                        return Err(RouterError::ColumnIndexOutOfRange);
                    }
                }
                self.route_by_indices(batch, indices)
            }
            KeySpec::AllColumns => {
                let indices: Vec<usize> = (0..batch.num_columns()).collect();
                self.route_by_indices(batch, &indices)
            }
        }
    }

    /// Route rows by hashing the values at the given column indices.
    fn route_by_indices(
        &self,
        batch: &RecordBatch,
        indices: &[usize],
    ) -> Result<Vec<(usize, RecordBatch)>, RouterError> {
        let num_rows = batch.num_rows();
        let mut core_rows: Vec<Vec<u32>> = vec![Vec::new(); self.num_cores];

        for row in 0..num_rows {
            let mut hasher = rustc_hash::FxHasher::default();
            for &col_idx in indices {
                hash_array_value(batch.column(col_idx).as_ref(), row, &mut hasher)?;
            }
            #[allow(clippy::cast_possible_truncation)]
            let core_id = (hasher.finish() as usize) % self.num_cores;
            #[allow(clippy::cast_possible_truncation)]
            core_rows[core_id].push(row as u32);
        }

        let mut result = Vec::new();
        for (core_id, rows) in core_rows.into_iter().enumerate() {
            if !rows.is_empty() {
                let take_indices = UInt32Array::from(rows);
                let columns: Vec<_> = batch
                    .columns()
                    .iter()
                    .map(|col| take(col.as_ref(), &take_indices, None))
                    .collect::<Result<_, _>>()
                    .map_err(|_| RouterError::UnsupportedDataType)?;
                let sub_batch = RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|_| RouterError::UnsupportedDataType)?;
                result.push((core_id, sub_batch));
            }
        }
        Ok(result)
    }
}

/// Hash a single value from an Arrow array at the given row index.
fn hash_array_value(
    array: &dyn Array,
    row: usize,
    hasher: &mut impl Hasher,
) -> Result<(), RouterError> {
    if row >= array.len() {
        return Err(RouterError::RowIndexOutOfRange);
    }

    if array.is_null(row) {
        0u8.hash(hasher);
        return Ok(());
    }

    // Try numeric types (most common for keys)
    if let Some(a) = array.as_primitive_opt::<Int64Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<Int32Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<UInt64Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<UInt32Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<Int16Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<Int8Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<UInt16Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<UInt8Type>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<Float64Type>() {
        a.value(row).to_bits().hash(hasher);
    } else if let Some(a) = array.as_primitive_opt::<Float32Type>() {
        a.value(row).to_bits().hash(hasher);
    } else if let Some(a) = array.as_string_opt::<i32>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_string_opt::<i64>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_binary_opt::<i32>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_binary_opt::<i64>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = array.as_boolean_opt() {
        a.value(row).hash(hasher);
    } else {
        return Err(RouterError::UnsupportedDataType);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(keys: Vec<i64>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("key", DataType::Int64, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(keys))]).unwrap()
    }

    fn make_string_batch(keys: Vec<&str>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(keys))]).unwrap()
    }

    #[test]
    fn test_deterministic_routing() {
        let router = PartitionedRouter::new(KeySpec::ColumnIndices(vec![0]), 4);
        let batch = make_batch(vec![1, 2, 3, 4, 1, 2, 3, 4]);

        let result = router.route_batch(&batch).unwrap();

        // Same key should always map to the same core
        // Verify by routing the same batch again
        let result2 = router.route_batch(&batch).unwrap();
        assert_eq!(result.len(), result2.len());
        for (a, b) in result.iter().zip(result2.iter()) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1.num_rows(), b.1.num_rows());
        }
    }

    #[test]
    fn test_same_key_same_core() {
        let router = PartitionedRouter::new(KeySpec::ColumnIndices(vec![0]), 4);
        let batch = make_batch(vec![42, 42, 42]);

        let result = router.route_batch(&batch).unwrap();
        assert_eq!(result.len(), 1, "All same keys should go to one core");
        assert_eq!(result[0].1.num_rows(), 3);
    }

    #[test]
    fn test_round_robin_passthrough() {
        let router = PartitionedRouter::new(KeySpec::RoundRobin, 4);
        let batch = make_batch(vec![1, 2, 3]);

        let result = router.route_batch(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 0);
        assert_eq!(result[0].1.num_rows(), 3);
    }

    #[test]
    fn test_column_names() {
        let router = PartitionedRouter::new(KeySpec::Columns(vec!["key".to_string()]), 2);
        let batch = make_batch(vec![1, 2, 3, 4]);

        let result = router.route_batch(&batch).unwrap();
        let total_rows: usize = result.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_column_not_found() {
        let router = PartitionedRouter::new(KeySpec::Columns(vec!["missing".to_string()]), 2);
        let batch = make_batch(vec![1, 2]);

        let result = router.route_batch(&batch);
        assert!(matches!(result, Err(RouterError::ColumnNotFoundByName)));
    }

    #[test]
    fn test_column_index_out_of_range() {
        let router = PartitionedRouter::new(KeySpec::ColumnIndices(vec![5]), 2);
        let batch = make_batch(vec![1, 2]);

        let result = router.route_batch(&batch);
        assert!(matches!(result, Err(RouterError::ColumnIndexOutOfRange)));
    }

    #[test]
    fn test_empty_batch() {
        let router = PartitionedRouter::new(KeySpec::ColumnIndices(vec![0]), 2);
        let batch = make_batch(vec![]);

        let result = router.route_batch(&batch);
        assert!(matches!(result, Err(RouterError::EmptyBatch)));
    }

    #[test]
    fn test_string_keys() {
        let router = PartitionedRouter::new(KeySpec::Columns(vec!["name".to_string()]), 4);
        let batch = make_string_batch(vec!["alice", "bob", "alice", "charlie"]);

        let result = router.route_batch(&batch).unwrap();
        let total_rows: usize = result.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 4);

        // "alice" appears twice — both should be in the same sub-batch
        for (_, sub) in &result {
            let col = sub.column(0);
            let arr = col.as_string::<i32>();
            let values: Vec<&str> = (0..arr.len()).map(|i| arr.value(i)).collect();
            if values.contains(&"alice") {
                assert_eq!(
                    values.iter().filter(|&&v| v == "alice").count(),
                    2,
                    "Both 'alice' rows should be in the same sub-batch"
                );
            }
        }
    }

    #[test]
    fn test_all_columns() {
        let router = PartitionedRouter::new(KeySpec::AllColumns, 2);
        let batch = make_batch(vec![1, 2, 3, 4]);

        let result = router.route_batch(&batch).unwrap();
        let total_rows: usize = result.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_preserves_schema() {
        let router = PartitionedRouter::new(KeySpec::ColumnIndices(vec![0]), 2);
        let batch = make_batch(vec![1, 2, 3]);

        let result = router.route_batch(&batch).unwrap();
        for (_, sub) in &result {
            assert_eq!(sub.schema(), batch.schema());
        }
    }
}
