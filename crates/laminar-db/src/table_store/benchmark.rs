//! Feature-gated access to complete reference-table operations for Criterion.

use arrow::array::RecordBatch;

use super::TableStore;
use crate::DbError;

/// Reference-table fixture used only by the non-default benchmark feature.
pub struct ReferenceTableBenchmark(TableStore);

impl ReferenceTableBenchmark {
    /// Create and populate a table whose first column is its primary key.
    ///
    /// # Errors
    /// Returns table validation or insertion errors.
    pub fn new(batch: &RecordBatch) -> Result<Self, DbError> {
        let mut store = TableStore::new();
        store.create_table("dimensions", batch.schema(), batch.schema().field(0).name())?;
        store.upsert("dimensions", batch)?;
        Ok(Self(store))
    }

    /// Upsert an entire batch and report the resulting live row count.
    ///
    /// # Errors
    /// Returns table insertion errors.
    pub fn upsert(&mut self, batch: &RecordBatch) -> Result<usize, DbError> {
        self.0.upsert("dimensions", batch)?;
        Ok(self.0.table_row_count("dimensions"))
    }

    /// Materialize the current table, as used by its query provider.
    ///
    /// # Errors
    /// Returns Arrow materialization errors.
    pub fn scan(&self) -> Result<Option<RecordBatch>, DbError> {
        self.0.to_record_batch("dimensions")
    }

    /// Prepare and atomically install a complete replacement snapshot.
    ///
    /// # Errors
    /// Returns snapshot validation or installation errors.
    pub fn refresh(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        let snapshot = self
            .0
            .prepare_snapshot("dimensions", std::slice::from_ref(batch))?;
        self.0.install_prepared_snapshots(vec![snapshot])
    }

    /// Capture a checkpoint, update the table, then encode the pinned old cut.
    ///
    /// # Errors
    /// Returns checkpoint capture/encoding or update errors.
    pub fn checkpoint_during_update(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<bytes::Bytes, DbError> {
        let capture = self
            .0
            .capture_checkpoint(u64::MAX)?
            .ok_or_else(|| DbError::Checkpoint("benchmark table inventory is empty".into()))?;
        self.0.upsert("dimensions", batch)?;
        Ok(capture.encode(u64::MAX)?.0)
    }
}
