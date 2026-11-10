//! Feature-gated access to managed aggregate updates for Criterion.

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use laminar_core::state::DEFAULT_KEY_GROUP_COUNT;

use super::keyed_partial::KeyedPartialLayout;
use super::{AggStateCheckpoint, IncrementalAggState};
use crate::DbError;

/// Every vnode's encoded state, as a checkpoint captures it.
#[derive(Clone)]
pub struct AggregateBenchmarkCheckpoint(Vec<(u32, AggStateCheckpoint)>);

/// Managed aggregate fixture used only by the non-default benchmark feature.
pub struct AggregateBenchmark {
    state: IncrementalAggState,
    /// Present for a keyed aggregate.
    keyed_partial: Option<KeyedPartialLayout>,
}

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
        let Some(projection) = state.compiled_projection() else {
            return Err(DbError::Unsupported(format!(
                "aggregate has no compiled pre-aggregation: {sql}"
            )));
        };
        let keyed_partial = (state.num_group_cols() != 0)
            .then(|| state.keyed_partial_layout(&projection.output_schema))
            .transpose()?;
        Ok(Self {
            state,
            keyed_partial,
        })
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
        self.state
            .compiled_projection()
            .ok_or_else(|| DbError::Pipeline("compiled pre-aggregation missing".into()))?
            .evaluate(batch)
    }

    /// Fold a projected batch into the accumulators, as the global owner does with its own rows.
    ///
    /// # Errors
    /// Returns accumulator errors.
    pub fn apply(&mut self, pre_aggregated: &RecordBatch) -> Result<(), DbError> {
        self.state
            .process_batch_for_vnode(pre_aggregated, i64::MIN, None)
    }

    /// Fold projected batches into one partial row each, as a node that does not own the global
    /// vnode does before sending.
    ///
    /// # Errors
    /// Returns accumulator errors.
    pub fn fold_partial(&self, pre_aggregated: &[RecordBatch]) -> Result<RecordBatch, DbError> {
        self.state.global_partial_batch(pre_aggregated)
    }

    /// Merge a peer's partial rows, as the global owner does.
    ///
    /// # Errors
    /// Returns accumulator errors.
    pub fn merge_partial(&mut self, partial: &RecordBatch) -> Result<(), DbError> {
        self.state.merge_global_partials(partial, i64::MIN)
    }

    /// Fold a projected batch into one partial row per group key, as a node would before sending
    /// a keyed aggregate's routed rows to their owner.
    ///
    /// # Errors
    /// Returns accumulator errors, or an unsupported error for a global aggregate.
    pub fn fold_keyed_partial(
        &mut self,
        pre_aggregated: &RecordBatch,
    ) -> Result<RecordBatch, DbError> {
        let layout = self.keyed_partial.as_ref().ok_or_else(no_keyed_partial)?;
        self.state.keyed_partial_batch(layout, pre_aggregated)
    }

    /// Merge a peer's keyed partial rows, as the keys' owner would.
    ///
    /// # Errors
    /// Returns accumulator errors, or an unsupported error for a global aggregate.
    pub fn merge_keyed_partial(&mut self, partial: &RecordBatch) -> Result<(), DbError> {
        let layout = self.keyed_partial.as_ref().ok_or_else(no_keyed_partial)?;
        self.state
            .merge_keyed_partials(layout, partial, i64::MIN, None)
    }

    /// Capture and encode every vnode's state, as a checkpoint does.
    ///
    /// # Errors
    /// Returns capture or encoding errors.
    pub fn checkpoint(&mut self) -> Result<AggregateBenchmarkCheckpoint, DbError> {
        let vnode_count = u32::from(DEFAULT_KEY_GROUP_COUNT.get());
        let vnodes = (0..vnode_count).collect::<Vec<_>>();
        self.state.force_full_vnode_capture();
        self.state
            .capture_checkpoint_vnodes(&vnodes, vnode_count, u64::MAX)?
            .into_iter()
            .map(|(vnode, capture)| capture.encode(usize::MAX).map(|state| (vnode, state)))
            .collect::<Result<_, _>>()
            .map(AggregateBenchmarkCheckpoint)
    }

    /// Restore a capture into this aggregate, as recovery does into a fresh one.
    ///
    /// # Errors
    /// Returns decoding or restore errors.
    pub fn restore(&mut self, checkpoint: AggregateBenchmarkCheckpoint) -> Result<(), DbError> {
        let vnode_count = u32::from(DEFAULT_KEY_GROUP_COUNT.get());
        for (vnode, state) in checkpoint.0 {
            self.state.restore_vnode(vnode, vnode_count, state)?;
        }
        Ok(())
    }

    /// Emit the running state, as one cycle's default emission does.
    ///
    /// # Errors
    /// Returns emission errors.
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        self.state.emit()
    }
}

fn no_keyed_partial() -> DbError {
    DbError::Unsupported("a global aggregate has no keyed partial".into())
}
