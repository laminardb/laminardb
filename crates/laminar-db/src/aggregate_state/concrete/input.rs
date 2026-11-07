//! One batch's aggregate inputs, checked once and then read in place for each group's rows, so
//! a group update copies no column.

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, UInt32Array};
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::functions_aggregate::min_max::{Max, Min};

use super::aggregate_error;
use crate::aggregate_state::AggFuncSpec;
use crate::error::DbError;

/// One aggregate's value column, FILTER mask, and input weights, borrowed from one batch.
pub(crate) struct AggregateInput<'a> {
    pub(super) value: &'a ArrayRef,
    filter: Option<&'a BooleanArray>,
    weights: Option<&'a Int64Array>,
    /// The value column's row encoding, for a MIN or MAX over weighted input. Held by value: an
    /// interior-mutable cache here would make every per-row loop reload the other fields.
    rows: Option<Rows>,
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
            rows: None,
        }
    }

    /// Encodes the value column into rows that order as the values do.
    pub(crate) fn with_row_encoding(mut self) -> Result<Self, DbError> {
        let converter = RowConverter::new(vec![SortField::new(self.value.data_type().clone())])
            .map_err(|error| aggregate_error("aggregate input row encoding", error))?;
        let rows = converter
            .convert_columns(std::slice::from_ref(self.value))
            .map_err(|error| aggregate_error("aggregate input row encoding", error))?;
        self.rows = Some(rows);
        Ok(self)
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
                let input = Self::new(column(value_index, "input")?, filter, weights);
                let extremum = spec.udf.inner().as_any();
                if weights.is_some() && (extremum.is::<Min>() || extremum.is::<Max>()) {
                    input.with_row_encoding()
                } else {
                    Ok(input)
                }
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

    pub(super) fn row_encoding(&self) -> Result<&Rows, DbError> {
        self.rows
            .as_ref()
            .ok_or_else(|| DbError::Pipeline("MIN/MAX input has no row encoding".into()))
    }

    /// The admitted rows' values copied into one array, for states that update from arrays.
    pub(super) fn take_selected(&self, rows: &[u32]) -> Result<ArrayRef, DbError> {
        #[allow(clippy::cast_possible_truncation)]
        let indices = UInt32Array::from_iter_values(self.selected(rows).map(|row| row as u32));
        compute::take(self.value, &indices, None)
            .map_err(|error| aggregate_error("aggregate input take", error))
    }
}
