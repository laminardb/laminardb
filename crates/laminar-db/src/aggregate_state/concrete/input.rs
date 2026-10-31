//! One batch's aggregate inputs, checked once and then read in place for each group's rows, so
//! a group update copies no column.

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, UInt32Array};
use arrow::compute;
use arrow::record_batch::RecordBatch;

use super::aggregate_error;
use crate::aggregate_state::AggFuncSpec;
use crate::error::DbError;

/// One aggregate's value column, FILTER mask, and input weights, borrowed from one batch.
pub(crate) struct AggregateInput<'a> {
    pub(super) value: &'a ArrayRef,
    filter: Option<&'a BooleanArray>,
    weights: Option<&'a Int64Array>,
}

impl<'a> AggregateInput<'a> {
    pub(crate) const fn new(
        value: &'a ArrayRef,
        filter: Option<&'a BooleanArray>,
        weights: Option<&'a Int64Array>,
    ) -> Self {
        Self {
            value,
            filter,
            weights,
        }
    }

    /// Resolve each aggregate's columns in `batch`. `weights` must already be validated as
    /// non-NULL `Int64` of the batch's length.
    pub(crate) fn resolve_all(
        batch: &'a RecordBatch,
        agg_specs: &[AggFuncSpec],
        weights: Option<&'a Int64Array>,
    ) -> Result<Vec<Self>, DbError> {
        let column = |index: usize, role: &str| {
            batch.columns().get(index).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "aggregate {role} column {index} is outside a {}-column batch",
                    batch.num_columns()
                ))
            })
        };
        agg_specs
            .iter()
            .map(|spec| {
                let [value_index] = spec.input_col_indices[..] else {
                    return Err(DbError::Pipeline(format!(
                        "managed aggregate '{}' expected one input column, got {}",
                        spec.output_name,
                        spec.input_col_indices.len()
                    )));
                };
                let filter = spec
                    .filter_col_index
                    .map(|index| {
                        let filter = column(index, "filter")?;
                        filter
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                DbError::Pipeline(format!(
                                    "aggregate FILTER must be Boolean, got {:?}",
                                    filter.data_type()
                                ))
                            })
                    })
                    .transpose()?;
                Ok(Self::new(column(value_index, "input")?, filter, weights))
            })
            .collect()
    }

    /// The rows of `rows` that FILTER admits, in order. A NULL filter value excludes its row.
    pub(super) fn selected<'r>(&'r self, rows: &'r [u32]) -> impl Iterator<Item = usize> + 'r {
        rows.iter().map(|&row| row as usize).filter(|&row| {
            self.filter
                .is_none_or(|filter| filter.is_valid(row) && filter.value(row))
        })
    }

    pub(super) fn weights(&self) -> Result<&'a Int64Array, DbError> {
        self.weights
            .ok_or_else(|| DbError::Pipeline("weighted aggregate received no input weights".into()))
    }

    /// The admitted rows' values copied into one array, for states that update from arrays.
    pub(super) fn take_selected(&self, rows: &[u32]) -> Result<ArrayRef, DbError> {
        #[allow(clippy::cast_possible_truncation)]
        let indices = UInt32Array::from_iter_values(self.selected(rows).map(|row| row as u32));
        compute::take(self.value, &indices, None)
            .map_err(|error| aggregate_error("aggregate input take", error))
    }
}
