//! Feature-gated complete MV operations for Criterion.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::{MvStorageMode, MvStore};
use crate::DbError;

/// Storage modes exercised by the non-default benchmark feature.
#[derive(Clone, Copy, Debug)]
pub enum MaterializedViewBenchmarkMode {
    /// Replace the entire snapshot each cycle.
    Aggregate,
    /// Retain the last eight batches.
    Append,
    /// Replace rows by their first-column key.
    Upsert,
    /// Consolidate signed full-row multiplicities.
    Multiset,
}

/// Fixture owning two independent local MVs.
pub struct MaterializedViewBenchmark(MvStore);

impl MaterializedViewBenchmark {
    /// Create two empty views with the same schema and storage mode.
    ///
    /// # Errors
    /// Returns MV schema or key-converter errors.
    pub fn new(schema: &SchemaRef, mode: MaterializedViewBenchmarkMode) -> Result<Self, DbError> {
        let mode = match mode {
            MaterializedViewBenchmarkMode::Aggregate => MvStorageMode::Aggregate,
            MaterializedViewBenchmarkMode::Append => MvStorageMode::Append { max_batches: 8 },
            MaterializedViewBenchmarkMode::Upsert => MvStorageMode::Upsert { key_cols: vec![0] },
            MaterializedViewBenchmarkMode::Multiset => MvStorageMode::Multiset,
        };
        let mut store = MvStore::new();
        for name in ["first", "second"] {
            store.create_mv(name, Arc::clone(schema), mode.clone())?;
        }
        Ok(Self(store))
    }

    /// Apply a cycle to both views.
    ///
    /// # Errors
    /// Returns state-admission or changelog-validation errors.
    pub fn update(&mut self, batches: &[RecordBatch]) -> Result<(), DbError> {
        self.0
            .update_views([("first", batches), ("second", batches)])
    }

    /// Materialize one view as a query/subscriber snapshot.
    ///
    /// # Errors
    /// Returns Arrow conversion or snapshot-admission errors.
    pub fn snapshot(&self) -> Result<Option<RecordBatch>, DbError> {
        self.0.to_record_batch("first")
    }

    /// Capture the old cut, update both views, then encode the checkpoint.
    ///
    /// # Errors
    /// Returns checkpoint or update errors.
    pub fn checkpoint_during_update(&mut self, batches: &[RecordBatch]) -> Result<usize, DbError> {
        let capture = self.0.capture_checkpoint(u64::MAX)?;
        self.update(batches)?;
        Ok(capture.encode(u64::MAX)?.into_parts().0.len())
    }
}
