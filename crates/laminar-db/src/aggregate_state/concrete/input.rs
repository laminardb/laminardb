//! One batch's aggregate inputs, checked once and then read in place for each group's rows, so
//! a group update copies no column and checks no type.

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array, Int64Array, UInt32Array,
    UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::functions_aggregate::min_max::{Max, Min};

use super::{aggregate_error, ConcreteAggregateState};
use crate::aggregate_state::AggFuncSpec;
use crate::error::DbError;

/// The value column downcast once per batch, for the types with typed kernels.
#[derive(Clone, Copy)]
pub(super) enum TypedValues<'a> {
    Int64(&'a Int64Array),
    UInt64(&'a UInt64Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array),
    Other,
}

impl<'a> TypedValues<'a> {
    fn of(value: &'a ArrayRef) -> Self {
        match value.data_type() {
            DataType::Int64 => value.as_primitive_opt().map(Self::Int64),
            DataType::UInt64 => value.as_primitive_opt().map(Self::UInt64),
            DataType::Float64 => value.as_primitive_opt().map(Self::Float64),
            DataType::Decimal128(_, _) => value.as_primitive_opt().map(Self::Decimal128),
            _ => None,
        }
        .unwrap_or(Self::Other)
    }
}

/// One aggregate's value column, FILTER mask, and input weights, borrowed from one batch.
pub(crate) struct AggregateInput<'a> {
    pub(super) value: &'a ArrayRef,
    pub(super) values: TypedValues<'a>,
    pub(super) nulls: Option<&'a NullBuffer>,
    filter: Option<&'a BooleanArray>,
    weights: Option<&'a Int64Array>,
    /// The value column's row encoding, for a MIN or MAX over weighted input. Held by value: an
    /// interior-mutable cache here would make every per-row loop reload the other fields.
    rows: Option<Rows>,
}

impl<'a> AggregateInput<'a> {
    pub(super) fn new(
        value: &'a ArrayRef,
        filter: Option<&'a BooleanArray>,
        weights: Option<&'a Int64Array>,
    ) -> Self {
        Self {
            value,
            values: TypedValues::of(value),
            nulls: value.nulls(),
            filter,
            weights,
            rows: None,
        }
    }

    /// Encodes the value column into rows that order as the values do.
    pub(super) fn with_row_encoding(mut self) -> Result<Self, DbError> {
        let converter = RowConverter::new(vec![SortField::new(self.value.data_type().clone())])
            .map_err(|error| aggregate_error("aggregate input row encoding", error))?;
        let rows = converter
            .convert_columns(std::slice::from_ref(self.value))
            .map_err(|error| aggregate_error("aggregate input row encoding", error))?;
        self.rows = Some(rows);
        Ok(self)
    }

    /// Resolve each aggregate's columns in `batch`, checked against `templates`: one state per
    /// aggregate, its template or any state built from it. Every state of an aggregate takes
    /// the same input type, so updates read the columns without checking them again.
    /// `weights` must already be validated as non-NULL `Int64` of the batch's length.
    pub(crate) fn resolve_all(
        batch: &'a RecordBatch,
        agg_specs: &[AggFuncSpec],
        templates: &[ConcreteAggregateState],
        weights: Option<&'a Int64Array>,
    ) -> Result<Vec<Self>, DbError> {
        if templates.len() != agg_specs.len() {
            return Err(DbError::Pipeline(format!(
                "aggregate template arity mismatch: {} templates vs {} aggregates",
                templates.len(),
                agg_specs.len()
            )));
        }
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
            .zip(templates)
            .map(|(spec, template)| {
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
                template.check_input(&input)?;
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
