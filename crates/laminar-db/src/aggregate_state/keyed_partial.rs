//! Keyed partial aggregation, measured only (ADR-015 A0, ADR-016 C1): a routed batch folded into
//! one row per group key before it is sent, and those rows merged on the key's owner through the
//! owner's grouping, preflight and apply.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, UInt32Array};
use arrow::compute;
use arrow::datatypes::{Int64Type, Schema, SchemaRef};
use datafusion_common::ScalarValue;

use super::grouping::BatchGroups;
use super::{AggregateInput, ConcreteAggregateState, IncrementalAggState};
use crate::error::DbError;

/// The columns of a keyed partial batch: the group columns, `__input_weight`, then each
/// aggregate's state columns.
pub(crate) struct KeyedPartialLayout {
    schema: SchemaRef,
    /// State columns per aggregate.
    arities: Vec<usize>,
}

impl KeyedPartialLayout {
    fn weight_column(&self) -> usize {
        self.schema.fields().len() - 1 - self.arities.iter().sum::<usize>()
    }

    /// Merge the deltas of `rows`, in order, into a group's accumulators.
    fn merge_rows(
        &self,
        accs: &mut [ConcreteAggregateState],
        batch: &RecordBatch,
        rows: &[u32],
    ) -> Result<(), DbError> {
        if accs.len() != self.arities.len() {
            return Err(partial_shape_error("aggregate count differs"));
        }
        for &row in rows {
            let mut column = self.weight_column() + 1;
            for (acc, &arity) in accs.iter_mut().zip(&self.arities) {
                let delta = (column..column + arity)
                    .map(|index| ScalarValue::try_from_array(batch.column(index), row as usize))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        DbError::Pipeline(format!("keyed aggregate partial scalar: {error}"))
                    })?;
                acc.merge_checkpoint_state(&delta)?;
                column += arity;
            }
        }
        Ok(())
    }
}

impl IncrementalAggState {
    /// The partial layout for pre-aggregate batches of `pre_aggregate`'s schema.
    pub(crate) fn keyed_partial_layout(
        &self,
        pre_aggregate: &Schema,
    ) -> Result<KeyedPartialLayout, DbError> {
        let group_fields = pre_aggregate
            .fields()
            .get(..self.num_group_cols)
            .filter(|fields| !fields.is_empty())
            .ok_or_else(|| partial_shape_error("pre-aggregate schema lacks the group columns"))?;
        let state = self.global_partial_schema();
        let fields = group_fields
            .iter()
            .chain(state.fields().iter())
            .cloned()
            .collect::<Vec<_>>();
        let arities = self
            .aggregate_templates
            .iter()
            .map(|template| template.checkpoint_state().len())
            .collect();
        Ok(KeyedPartialLayout {
            schema: Arc::new(Schema::new(fields)),
            arities,
        })
    }

    /// Fold a routed pre-aggregate batch into one row per distinct group key, in first-appearance
    /// order.
    pub(crate) fn keyed_partial_batch(
        &mut self,
        layout: &KeyedPartialLayout,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, DbError> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&layout.schema)));
        }
        let keys = self.encode_group_keys(batch)?;
        let input_weights = Self::validated_input_weights(batch, self.weight_col_idx)?;
        let inputs = AggregateInput::resolve_all(
            batch,
            &self.agg_specs,
            &self.aggregate_templates,
            input_weights,
        )?;
        let groups = BatchGroups::try_new(&keys, input_weights, self.previous_batch_groups)?;
        self.previous_batch_groups = groups.len();
        let mut first_rows = Vec::with_capacity(groups.len());
        let mut group_weights = Vec::with_capacity(groups.len());
        let mut states: Vec<Vec<ScalarValue>> =
            std::iter::repeat_with(|| Vec::with_capacity(groups.len()))
                .take(layout.arities.iter().sum())
                .collect();
        for (_, rows, weight) in groups.iter() {
            first_rows.push(rows[0]);
            group_weights.push(weight);
            let mut column = 0;
            for (template, input) in self.aggregate_templates.iter().zip(&inputs) {
                for scalar in template.batch_delta(input, rows)? {
                    states
                        .get_mut(column)
                        .ok_or_else(|| partial_shape_error("delta has extra state"))?
                        .push(scalar);
                    column += 1;
                }
            }
        }
        let first_rows = UInt32Array::from(first_rows);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(layout.schema.fields().len());
        for column in &batch.columns()[..self.num_group_cols] {
            columns.push(
                compute::take(column, &first_rows, None)
                    .map_err(|error| DbError::Pipeline(format!("partial group take: {error}")))?,
            );
        }
        columns.push(Arc::new(Int64Array::from(group_weights)));
        for state in states {
            columns.push(ScalarValue::iter_to_array(state).map_err(|error| {
                DbError::Pipeline(format!("keyed aggregate partial column: {error}"))
            })?);
        }
        RecordBatch::try_new(Arc::clone(&layout.schema), columns)
            .map_err(|error| DbError::Pipeline(format!("keyed aggregate partial batch: {error}")))
    }

    /// Merge partial rows folded by [`Self::keyed_partial_batch`].
    pub(crate) fn merge_keyed_partials(
        &mut self,
        layout: &KeyedPartialLayout,
        batch: &RecordBatch,
        watermark_ms: i64,
        uniform_vnode: Option<u32>,
    ) -> Result<(), DbError> {
        if let Some(vnode) = uniform_vnode {
            let vnode_count = self.routing_vnode_count().get();
            if vnode >= vnode_count {
                return Err(DbError::Pipeline(format!(
                    "aggregate routed vnode {vnode} is outside key-group count {vnode_count}"
                )));
            }
        }
        if batch.num_columns() != layout.schema.fields().len() {
            return Err(partial_shape_error(
                "column count differs from the partial layout",
            ));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let weights = batch
            .column(layout.weight_column())
            .as_primitive_opt::<Int64Type>()
            .filter(|weights| weights.null_count() == 0)
            .ok_or_else(|| partial_shape_error("input weight must be non-null Int64"))?;
        let keys = self.encode_group_keys(batch)?;
        let groups = BatchGroups::try_new(&keys, Some(weights), self.previous_batch_groups)?;
        self.previous_batch_groups = groups.len();
        self.apply_keyed_groups(
            &groups,
            |accs, rows| layout.merge_rows(accs, batch, rows),
            watermark_ms,
            uniform_vnode,
        )
    }
}

fn partial_shape_error(reason: &str) -> DbError {
    DbError::Pipeline(format!("keyed aggregate partial is malformed: {reason}"))
}

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    const SQL: &str = "SELECT k, COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a, MIN(v) AS lo, \
                       MAX(p) AS hi FROM events GROUP BY k";

    fn events(keys: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("p", DataType::Float64, false),
        ]));
        let values = (0_i32..)
            .take(keys.len())
            .map(|i| i * 3 - 7)
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from_iter_values(
                    values.iter().map(|&v| i64::from(v)),
                )),
                Arc::new(Float64Array::from_iter_values(
                    values.iter().map(|&v| f64::from(v) * 0.5),
                )),
            ],
        )
        .unwrap()
    }

    async fn state() -> IncrementalAggState {
        let ctx = laminar_sql::create_session_context();
        let batch = events(&[0]);
        let table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        IncrementalAggState::try_from_sql(
            &ctx,
            SQL,
            false,
            laminar_core::state::DEFAULT_KEY_GROUP_COUNT,
        )
        .await
        .unwrap()
        .expect("aggregate state")
    }

    fn pre_aggregate(state: &IncrementalAggState, keys: &[i64]) -> RecordBatch {
        state
            .compiled_projection()
            .expect("compiled pre-aggregation")
            .evaluate(&events(keys))
            .unwrap()
    }

    fn emitted(state: &mut IncrementalAggState) -> RecordBatch {
        let batches = state.emit().unwrap();
        let batch = compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let order = compute::sort_to_indices(batch.column(0), None, None).unwrap();
        compute::take_record_batch(&batch, &order).unwrap()
    }

    #[tokio::test]
    async fn folded_partials_merge_to_the_state_the_rows_apply() {
        let mut applied = state().await;
        let mut merged = state().await;
        for keys in [&[1, 2, 1, 3, 1, 2][..], &[2, 4, 4, 1]] {
            let rows = pre_aggregate(&applied, keys);
            let layout = merged.keyed_partial_layout(&rows.schema()).unwrap();
            let partial = merged.keyed_partial_batch(&layout, &rows).unwrap();
            let mut distinct = keys.to_vec();
            distinct.sort_unstable();
            distinct.dedup();
            assert_eq!(partial.num_rows(), distinct.len());
            applied.process_batch_for_vnode(&rows, 0, None).unwrap();
            merged
                .merge_keyed_partials(&layout, &partial, 0, None)
                .unwrap();
        }
        assert_eq!(emitted(&mut merged), emitted(&mut applied));
    }

    #[tokio::test]
    async fn each_grouped_batch_sizes_the_next_from_its_distinct_keys() {
        let mut state = state().await;
        let keys: Vec<i64> = (0..4096).map(|row| row % 3001).collect();
        let rows = pre_aggregate(&state, &keys);
        state.process_batch_for_vnode(&rows, 0, None).unwrap();
        assert_eq!(state.previous_batch_groups, 3001);
        let layout = state.keyed_partial_layout(&rows.schema()).unwrap();
        let partial = state
            .keyed_partial_batch(&layout, &pre_aggregate(&state, &[5, 6, 5]))
            .unwrap();
        assert_eq!(state.previous_batch_groups, 2);
        state.process_batch_for_vnode(&rows, 0, None).unwrap();
        state
            .merge_keyed_partials(&layout, &partial, 0, None)
            .unwrap();
        assert_eq!(state.previous_batch_groups, 2);
    }

    #[tokio::test]
    async fn a_partial_over_the_group_limit_changes_no_group() {
        let mut merged = state().await;
        merged.set_max_groups_for_test(2);
        let first = pre_aggregate(&merged, &[1, 2, 1]);
        let layout = merged.keyed_partial_layout(&first.schema()).unwrap();
        let partial = merged.keyed_partial_batch(&layout, &first).unwrap();
        merged
            .merge_keyed_partials(&layout, &partial, 0, None)
            .unwrap();
        let before = emitted(&mut merged);

        let second = pre_aggregate(&merged, &[1, 3]);
        let partial = merged.keyed_partial_batch(&layout, &second).unwrap();
        merged
            .merge_keyed_partials(&layout, &partial, 0, None)
            .unwrap_err();
        assert_eq!(emitted(&mut merged), before);
    }
}
