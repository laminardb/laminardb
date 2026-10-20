//! Feature-gated access to managed aggregate updates for Criterion.

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use laminar_core::state::DEFAULT_KEY_GROUP_COUNT;

use super::IncrementalAggState;
use crate::DbError;

/// Managed aggregate fixture used only by the non-default benchmark feature.
pub struct AggregateBenchmark(IncrementalAggState);

impl AggregateBenchmark {
    /// Plan `sql` against tables registered in `ctx` with the default key-group topology.
    ///
    /// # Errors
    /// Returns planning errors, or an unsupported error for a query without an aggregate or
    /// without a compiled pre-aggregation.
    pub async fn new(ctx: &SessionContext, sql: &str) -> Result<Self, DbError> {
        let state = IncrementalAggState::try_from_sql(ctx, sql, false, DEFAULT_KEY_GROUP_COUNT)
            .await?
            .ok_or_else(|| DbError::Unsupported(format!("not an aggregate query: {sql}")))?;
        if state.compiled_projection().is_none() {
            return Err(DbError::Unsupported(format!(
                "aggregate has no compiled pre-aggregation: {sql}"
            )));
        }
        Ok(Self(state))
    }

    /// Project one raw input batch and fold it into the per-vnode accumulators.
    ///
    /// # Errors
    /// Returns projection or accumulator errors.
    pub fn update(&mut self, batch: &RecordBatch) -> Result<(), DbError> {
        let pre_aggregated = self.pre_aggregate(batch)?;
        self.apply(&pre_aggregated)
    }

    /// Project one raw input batch through the compiled pre-aggregation.
    ///
    /// # Errors
    /// Returns projection errors.
    pub fn pre_aggregate(&self, batch: &RecordBatch) -> Result<RecordBatch, DbError> {
        self.0
            .compiled_projection()
            .ok_or_else(|| DbError::Pipeline("compiled pre-aggregation missing".into()))?
            .evaluate(batch)
    }

    /// Fold a projected batch into the accumulators, as the global owner does with rows a peer
    /// shuffled to it.
    ///
    /// # Errors
    /// Returns accumulator errors.
    pub fn apply(&mut self, pre_aggregated: &RecordBatch) -> Result<(), DbError> {
        self.0
            .process_batch_for_vnode(pre_aggregated, i64::MIN, None)
    }

    /// Fold projected batches into one partial row each, as a node that does not own the global
    /// vnode does before sending.
    ///
    /// # Errors
    /// Returns accumulator errors.
    pub fn fold_partial(&self, pre_aggregated: &[RecordBatch]) -> Result<RecordBatch, DbError> {
        self.0.global_partial_batch(pre_aggregated)
    }

    /// Merge a peer's partial rows, as the global owner does.
    ///
    /// # Errors
    /// Returns accumulator errors.
    pub fn merge_partial(&mut self, partial: &RecordBatch) -> Result<(), DbError> {
        self.0.merge_global_partials(partial, i64::MIN)
    }

    /// Emit the running state, as one cycle's default emission does.
    ///
    /// # Errors
    /// Returns emission errors.
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        self.0.emit()
    }
}
