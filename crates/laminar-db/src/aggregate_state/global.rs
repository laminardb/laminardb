//! The global aggregate's single group, updated from raw pre-aggregate rows on its owner or, in
//! cluster mode, from partial rows other nodes fold from theirs.

#[cfg(feature = "cluster")]
use std::sync::Arc;

#[cfg(feature = "cluster")]
use arrow::array::{Array, ArrayRef, AsArray, Int64Array};
#[cfg(feature = "cluster")]
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "cluster")]
use datafusion_common::ScalarValue;

use super::IncrementalAggState;
use super::{
    global_aggregate_key, AggFuncSpec, AggregateInput, ConcreteAggregateState, GroupEntry,
};
use crate::error::DbError;

/// What one update applies to the global group.
enum GlobalInput<'a> {
    Rows(&'a RecordBatch),
    /// One partial row: each aggregate's delta in `checkpoint_state` form.
    #[cfg(feature = "cluster")]
    Partial(&'a [Vec<ScalarValue>]),
}

impl IncrementalAggState {
    pub(super) fn process_batch_no_groups(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> Result<(), DbError> {
        let input_weight_delta = Self::batch_input_weight(batch, self.weight_col_idx)?;
        self.update_global_group(input_weight_delta, watermark_ms, &GlobalInput::Rows(batch))
    }

    /// Schema of a partial batch: the net input weight, then each aggregate's state columns.
    #[cfg(feature = "cluster")]
    pub(crate) fn global_partial_schema(&self) -> Result<SchemaRef, DbError> {
        let mut fields = vec![Field::new("__input_weight", DataType::Int64, false)];
        for (index, template) in self.aggregate_templates.iter().enumerate() {
            for (column, scalar) in template.checkpoint_state()?.iter().enumerate() {
                fields.push(Field::new(
                    format!("__partial_{index}_{column}"),
                    scalar.data_type(),
                    true,
                ));
            }
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Fold each pre-aggregate batch into one partial row. One row per batch makes merging it on
    /// the owner run the same checks, in the same order, as applying that batch there.
    #[cfg(feature = "cluster")]
    pub(crate) fn global_partial_batch(
        &self,
        batches: &[RecordBatch],
    ) -> Result<RecordBatch, DbError> {
        let schema = self.global_partial_schema()?;
        let mut weights = Vec::with_capacity(batches.len());
        let mut states = vec![Vec::with_capacity(batches.len()); schema.fields().len() - 1];
        for batch in batches {
            weights.push(Self::batch_input_weight(batch, self.weight_col_idx)?);
            let input_weights = Self::validated_input_weights(batch, self.weight_col_idx)?;
            let inputs = AggregateInput::resolve_all(batch, &self.agg_specs, input_weights)?;
            let rows = all_row_indices(batch);
            let mut column = 0;
            for (template, input) in self.aggregate_templates.iter().zip(&inputs) {
                for scalar in template.batch_delta(input, &rows)? {
                    states
                        .get_mut(column)
                        .ok_or_else(|| partial_shape_error("delta has extra state"))?
                        .push(scalar);
                    column += 1;
                }
            }
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        columns.push(Arc::new(Int64Array::from(weights)));
        for state in states {
            columns.push(ScalarValue::iter_to_array(state).map_err(|error| {
                DbError::Pipeline(format!("global aggregate partial column: {error}"))
            })?);
        }
        RecordBatch::try_new(schema, columns)
            .map_err(|error| DbError::Pipeline(format!("global aggregate partial batch: {error}")))
    }

    /// Merge partial rows folded by [`Self::global_partial_batch`], one row at a time.
    #[cfg(feature = "cluster")]
    pub(crate) fn merge_global_partials(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> Result<(), DbError> {
        let arities = self
            .aggregate_templates
            .iter()
            .map(|template| template.checkpoint_state().map(|state| state.len()))
            .collect::<Result<Vec<_>, _>>()?;
        if batch.num_columns() != 1 + arities.iter().sum::<usize>() {
            return Err(partial_shape_error(
                "column count differs from the aggregate state",
            ));
        }
        let weights = batch
            .column(0)
            .as_primitive_opt::<Int64Type>()
            .filter(|weights| weights.null_count() == 0)
            .ok_or_else(|| partial_shape_error("input weight must be non-null Int64"))?;
        for row in 0..batch.num_rows() {
            let mut column = 1;
            let mut partial = Vec::with_capacity(arities.len());
            for arity in &arities {
                let state = (column..column + arity)
                    .map(|index| ScalarValue::try_from_array(batch.column(index), row))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        DbError::Pipeline(format!("global aggregate partial scalar: {error}"))
                    })?;
                partial.push(state);
                column += arity;
            }
            self.update_global_group(
                weights.value(row),
                watermark_ms,
                &GlobalInput::Partial(&partial),
            )?;
        }
        Ok(())
    }

    fn update_global_group(
        &mut self,
        input_weight_delta: i64,
        watermark_ms: i64,
        input: &GlobalInput<'_>,
    ) -> Result<(), DbError> {
        let empty_key = global_aggregate_key();
        let (current_input_weight, is_new) = self
            .vnode_states
            .get(0)
            .and_then(|state| state.groups.get(&empty_key))
            .map_or((0, true), |entry| (entry.input_weight, false));
        let input_weight = Self::next_group_input_weight(current_input_weight, input_weight_delta)?;
        if is_new {
            let mut entry = GroupEntry::new(self.fresh_concrete_states(), 0, watermark_ms);
            apply_global_input(&mut entry.accs, &self.agg_specs, self.weight_col_idx, input)?;
            entry.input_weight = input_weight;
            entry.last_updated_ms = watermark_ms;
            entry.refresh_accumulator_usage();

            let vnode_state = self.vnode_states.get_or_insert(0);
            vnode_state.insert_new_group(empty_key.clone(), entry);
            if self.emit_changelog {
                vnode_state.insert_emit_dirty_key(empty_key);
            }
            self.vnode_states.increment_resident_groups();
            self.mark_checkpoint_vnode_dirty(0);
            return Ok(());
        }

        let vnode_state = self
            .vnode_states
            .get_mut(0)
            .expect("global aggregate vnode must remain resident");
        let (update_result, previous_usage, current_usage) = {
            let entry = vnode_state
                .groups
                .get_mut(&empty_key)
                .expect("global aggregate group must remain resident");
            let update_result =
                apply_global_input(&mut entry.accs, &self.agg_specs, self.weight_col_idx, input);
            if update_result.is_ok() {
                entry.input_weight = input_weight;
                entry.last_updated_ms = watermark_ms;
            }
            let (previous, current) = entry.refresh_accumulator_usage();
            (update_result, previous, current)
        };
        vnode_state.reconcile_accumulator_usage(previous_usage, current_usage);
        if update_result.is_ok() && self.emit_changelog {
            vnode_state.insert_emit_dirty_key(empty_key);
        }
        update_result?;
        self.mark_checkpoint_vnode_dirty(0);
        Ok(())
    }

    fn batch_input_weight(
        batch: &RecordBatch,
        weight_col_idx: Option<usize>,
    ) -> Result<i64, DbError> {
        match Self::validated_input_weights(batch, weight_col_idx)? {
            Some(weights) => weights.values().iter().try_fold(0_i64, |total, weight| {
                total
                    .checked_add(*weight)
                    .ok_or_else(|| DbError::Pipeline("aggregate input weight overflow".into()))
            }),
            None => i64::try_from(batch.num_rows())
                .map_err(|_| DbError::Pipeline("aggregate row count exceeds Int64".into())),
        }
    }
}

fn apply_global_input(
    accs: &mut [ConcreteAggregateState],
    agg_specs: &[AggFuncSpec],
    weight_col_idx: Option<usize>,
    input: &GlobalInput<'_>,
) -> Result<(), DbError> {
    match input {
        GlobalInput::Rows(batch) => IncrementalAggState::update_concrete_accumulators(
            accs,
            &AggregateInput::resolve_all(
                batch,
                agg_specs,
                IncrementalAggState::validated_input_weights(batch, weight_col_idx)?,
            )?,
            &all_row_indices(batch),
        ),
        #[cfg(feature = "cluster")]
        GlobalInput::Partial(partial) => {
            if partial.len() != accs.len() {
                return Err(partial_shape_error("aggregate count differs"));
            }
            accs.iter_mut()
                .zip(partial.iter())
                .try_for_each(|(acc, delta)| acc.merge_checkpoint_state(delta))
        }
    }
}

fn all_row_indices(batch: &RecordBatch) -> Vec<u32> {
    #[allow(clippy::cast_possible_truncation)]
    (0..batch.num_rows() as u32).collect()
}

#[cfg(feature = "cluster")]
fn partial_shape_error(reason: &str) -> DbError {
    DbError::Pipeline(format!("global aggregate partial is malformed: {reason}"))
}
