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
        let projection = self
            .0
            .compiled_projection()
            .ok_or_else(|| DbError::Pipeline("compiled pre-aggregation missing".into()))?;
        let pre_aggregated = projection.evaluate(batch)?;
        self.0
            .process_batch_for_vnode(&pre_aggregated, i64::MIN, None)
    }

    /// Emit the running state, as one cycle's default emission does.
    ///
    /// # Errors
    /// Returns emission errors.
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        self.0.emit()
    }
}
