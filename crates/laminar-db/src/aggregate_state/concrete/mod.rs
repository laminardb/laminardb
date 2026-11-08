use arrow::datatypes::{DataType, Decimal128Type};
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::min_max::{Max, Min};
use datafusion::functions_aggregate::sum::Sum;
use datafusion_common::ScalarValue;
use datafusion_functions_aggregate_common::utils::DecimalAverager;

use super::AggFuncSpec;
use crate::error::DbError;

mod input;
mod min_max;
mod retractable;
mod sum;

pub(crate) use input::AggregateInput;
use min_max::{Extremum, ExtremumState};
use retractable::RetractableExtremumState;
#[cfg(feature = "cluster")]
use sum::sum_rows;
pub(crate) use sum::SumState;
use sum::{add_rows_sum, merge_sum_scalar, require_array_type, weighted_f64_delta};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConcreteInputMode {
    AppendOnly,
    Weighted,
}

#[derive(Clone, Debug)]
pub(crate) enum ConcreteAggregateState {
    Count(CountState),
    Sum(SumState),
    Avg(AvgState),
    Min(ExtremumState),
    Max(ExtremumState),
    /// MIN over a changelog.
    RetractableMin(RetractableExtremumState),
    /// MAX over a changelog.
    RetractableMax(RetractableExtremumState),
}

impl ConcreteAggregateState {
    pub(crate) fn try_new(spec: &AggFuncSpec, mode: ConcreteInputMode) -> Result<Self, DbError> {
        if spec.input_types.len() != 1 {
            return Err(unsupported(spec, "exactly one aggregate input is required"));
        }

        let implementation = spec.udf.inner().as_any();
        if implementation.is::<Count>() {
            if spec.return_type != DataType::Int64 {
                return Err(unsupported(spec, "COUNT must return Int64"));
            }
            return Ok(Self::Count(CountState {
                count: 0,
                count_star: spec.is_count_star,
                weighted: mode == ConcreteInputMode::Weighted,
            }));
        }
        if implementation.is::<Sum>() {
            return Ok(Self::Sum(SumState::try_new(
                &spec.input_types[0],
                &spec.return_type,
                mode,
            )?));
        }
        if implementation.is::<Avg>() {
            return Ok(Self::Avg(AvgState::try_new(
                &spec.input_types[0],
                &spec.return_type,
                mode,
            )?));
        }
        let (input_type, output_type) = (&spec.input_types[0], &spec.return_type);
        if implementation.is::<Min>() {
            return match mode {
                ConcreteInputMode::AppendOnly => {
                    ExtremumState::try_new(input_type, output_type).map(Self::Min)
                }
                ConcreteInputMode::Weighted => {
                    RetractableExtremumState::try_new(input_type, output_type)
                        .map(Self::RetractableMin)
                }
            };
        }
        if implementation.is::<Max>() {
            return match mode {
                ConcreteInputMode::AppendOnly => {
                    ExtremumState::try_new(input_type, output_type).map(Self::Max)
                }
                ConcreteInputMode::Weighted => {
                    RetractableExtremumState::try_new(input_type, output_type)
                        .map(Self::RetractableMax)
                }
            };
        }

        Err(unsupported(
            spec,
            "only built-in COUNT, SUM, AVG, MIN, and MAX are supported",
        ))
    }

    /// Rejects an input column of a type this aggregate's states do not take.
    fn check_input(&self, input: &AggregateInput<'_>) -> Result<(), DbError> {
        match self {
            Self::Count(_) => Ok(()),
            Self::Sum(state) => state.check_input(input),
            Self::Avg(state) => state.check_input(input),
            Self::Min(state) => state.check_input(Extremum::Min, input),
            Self::Max(state) => state.check_input(Extremum::Max, input),
            Self::RetractableMin(state) => state.check_input(Extremum::Min, input),
            Self::RetractableMax(state) => state.check_input(Extremum::Max, input),
        }
    }

    /// Fold the admitted values of `rows`, in row order, into the state. `input` must have
    /// passed this aggregate's `check_input`.
    pub(crate) fn update_rows(
        &mut self,
        input: &AggregateInput<'_>,
        rows: &[u32],
    ) -> Result<(), DbError> {
        match self {
            Self::Count(state) => state.update_rows(input, rows),
            Self::Sum(state) => state.update_rows(input, rows),
            Self::Avg(state) => state.update_rows(input, rows),
            Self::Min(state) => state.update_rows(Extremum::Min, input, rows),
            Self::Max(state) => state.update_rows(Extremum::Max, input, rows),
            Self::RetractableMin(state) => state.update_rows(Extremum::Min, input, rows),
            Self::RetractableMax(state) => state.update_rows(Extremum::Max, input, rows),
        }
    }

    /// The contribution of `rows` in `checkpoint_state` form, independent of accumulated values.
    /// `merge_checkpoint_state` applies it with the same checks `update_rows` makes, so a delta
    /// may be negative where the state it merges into may not. `input` must have passed
    /// `check_input`.
    // PERF: SUM and AVG deltas repeat `update_rows` arithmetic; routing their updates through
    // `Vec<ScalarValue>` deltas cost the keyed update benchmark about 4%.
    #[cfg(feature = "cluster")]
    pub(crate) fn batch_delta(
        &self,
        input: &AggregateInput<'_>,
        rows: &[u32],
    ) -> Result<Vec<ScalarValue>, DbError> {
        match self {
            Self::Count(state) => Ok(vec![ScalarValue::Int64(Some(
                state.rows_delta(input, rows)?,
            ))]),
            Self::Sum(state) => state.batch_delta(input, rows),
            Self::Avg(state) => state.batch_delta(input, rows),
            Self::Min(state) => state.batch_delta(Extremum::Min, input, rows),
            Self::Max(state) => state.batch_delta(Extremum::Max, input, rows),
            Self::RetractableMin(_) | Self::RetractableMax(_) => Err(DbError::Pipeline(
                "MIN/MAX over a changelog has no partial form; its rows go to the owner".into(),
            )),
        }
    }

    pub(crate) fn evaluate(&mut self) -> Result<ScalarValue, DbError> {
        match self {
            Self::Count(state) => Ok(ScalarValue::Int64(Some(state.count))),
            Self::Sum(state) => state.evaluate(),
            Self::Avg(state) => state.evaluate(),
            Self::Min(state) | Self::Max(state) => Ok(state.evaluate()),
            Self::RetractableMin(state) => state.evaluate(Extremum::Min),
            Self::RetractableMax(state) => state.evaluate(Extremum::Max),
        }
    }

    pub(crate) fn checkpoint_state(&self) -> Vec<ScalarValue> {
        match self {
            Self::Count(state) => vec![ScalarValue::Int64(Some(state.count))],
            Self::Sum(state) => state.checkpoint_state(),
            Self::Avg(state) => state.checkpoint_state(),
            Self::Min(state) | Self::Max(state) => state.checkpoint_state(),
            Self::RetractableMin(state) | Self::RetractableMax(state) => state.checkpoint_state(),
        }
    }

    pub(crate) fn merge_checkpoint_state(
        &mut self,
        checkpoint: &[ScalarValue],
    ) -> Result<(), DbError> {
        match self {
            Self::Count(state) => state.merge_checkpoint_state(checkpoint),
            Self::Sum(state) => state.merge_checkpoint_state(checkpoint),
            Self::Avg(state) => state.merge_checkpoint_state(checkpoint),
            Self::Min(state) => state.merge_checkpoint_state(Extremum::Min, checkpoint),
            Self::Max(state) => state.merge_checkpoint_state(Extremum::Max, checkpoint),
            Self::RetractableMin(state) => state.merge_checkpoint_state(Extremum::Min, checkpoint),
            Self::RetractableMax(state) => state.merge_checkpoint_state(Extremum::Max, checkpoint),
        }
    }

    pub(crate) fn nested_retained_bytes(&self) -> usize {
        match self {
            Self::Min(state) | Self::Max(state) => state.nested_retained_bytes(),
            Self::RetractableMin(state) | Self::RetractableMax(state) => {
                state.nested_retained_bytes()
            }
            Self::Count(_) | Self::Sum(_) | Self::Avg(_) => 0,
        }
    }
}

fn unsupported(spec: &AggFuncSpec, reason: &str) -> DbError {
    DbError::Unsupported(format!(
        "[{}] managed aggregate '{}' is unsupported: {reason}",
        laminar_core::error_codes::SQL_UNSUPPORTED,
        spec.udf.name(),
    ))
}

fn aggregate_error(context: &str, error: impl std::fmt::Display) -> DbError {
    DbError::Pipeline(format!("{context}: {error}"))
}

#[derive(Clone, Debug)]
pub(crate) struct CountState {
    count: i64,
    count_star: bool,
    weighted: bool,
}

impl CountState {
    fn update_rows(&mut self, input: &AggregateInput<'_>, rows: &[u32]) -> Result<(), DbError> {
        let delta = self.rows_delta(input, rows)?;
        let next = self
            .count
            .checked_add(delta)
            .ok_or_else(|| DbError::Pipeline("COUNT state overflow".into()))?;
        if next < 0 {
            return Err(DbError::Pipeline(
                "COUNT state became negative after retraction".into(),
            ));
        }
        self.count = next;
        Ok(())
    }

    /// Admitted rows, each counted by its weight in weighted mode.
    fn rows_delta(&self, input: &AggregateInput<'_>, rows: &[u32]) -> Result<i64, DbError> {
        let weights = if self.weighted {
            Some(input.weights()?)
        } else {
            None
        };
        let nulls = input.nulls;
        input
            .selected(rows)
            .filter(|&row| self.count_star || nulls.is_none_or(|nulls| nulls.is_valid(row)))
            .try_fold(0_i64, |total, row| {
                total.checked_add(weights.map_or(1, |weights| weights.value(row)))
            })
            .ok_or_else(|| DbError::Pipeline("COUNT weight overflow".into()))
    }

    fn merge_checkpoint_state(&mut self, checkpoint: &[ScalarValue]) -> Result<(), DbError> {
        let [ScalarValue::Int64(Some(delta))] = checkpoint else {
            return Err(DbError::Pipeline(
                "COUNT checkpoint state must be [Int64]".into(),
            ));
        };
        let next = self
            .count
            .checked_add(*delta)
            .ok_or_else(|| DbError::Pipeline("COUNT checkpoint overflow".into()))?;
        if next < 0 {
            return Err(DbError::Pipeline(
                "COUNT checkpoint state is negative".into(),
            ));
        }
        self.count = next;
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum AvgMode {
    AppendOnly { count: u64 },
    Weighted { non_null_weight: i64 },
}

#[derive(Clone, Debug)]
pub(crate) struct AvgState {
    input_type: DataType,
    output_type: DataType,
    sum: ScalarValue,
    mode: AvgMode,
}

impl AvgState {
    fn try_new(
        input_type: &DataType,
        output_type: &DataType,
        mode: ConcreteInputMode,
    ) -> Result<Self, DbError> {
        let (sum, mode) = match mode {
            ConcreteInputMode::AppendOnly => {
                validate_append_avg_types(input_type, output_type)?;
                let sum = match input_type {
                    DataType::Decimal128(_, scale) => ScalarValue::Decimal128(None, 38, *scale),
                    _ => ScalarValue::try_from(input_type)
                        .map_err(|error| aggregate_error("AVG sum state type", error))?,
                };
                (sum, AvgMode::AppendOnly { count: 0 })
            }
            ConcreteInputMode::Weighted => {
                if input_type != &DataType::Float64 || output_type != &DataType::Float64 {
                    return Err(DbError::Unsupported(format!(
                        "[{}] weighted AVG supports Float64 only, got {input_type:?} -> {output_type:?}",
                        laminar_core::error_codes::SQL_UNSUPPORTED
                    )));
                }
                (
                    ScalarValue::Float64(Some(0.0)),
                    AvgMode::Weighted { non_null_weight: 0 },
                )
            }
        };
        Ok(Self {
            input_type: input_type.clone(),
            output_type: output_type.clone(),
            sum,
            mode,
        })
    }

    fn check_input(&self, input: &AggregateInput<'_>) -> Result<(), DbError> {
        let name = match self.mode {
            AvgMode::AppendOnly { .. } => "AVG",
            AvgMode::Weighted { .. } => "weighted AVG",
        };
        require_array_type(input.value, &self.input_type, name)
    }

    fn update_rows(&mut self, input: &AggregateInput<'_>, rows: &[u32]) -> Result<(), DbError> {
        match &mut self.mode {
            AvgMode::AppendOnly { count } => {
                let next_count = count
                    .checked_add(non_null_rows(input, rows)?)
                    .ok_or_else(|| DbError::Pipeline("AVG count overflow".into()))?;
                add_rows_sum(&mut self.sum, input, rows)?;
                *count = next_count;
                Ok(())
            }
            AvgMode::Weighted { non_null_weight } => {
                let ScalarValue::Float64(Some(sum)) = &mut self.sum else {
                    return Err(DbError::Pipeline(
                        "weighted AVG has an invalid concrete state shape".into(),
                    ));
                };
                let (delta_sum, delta_weight) = weighted_f64_delta(input, rows, "weighted AVG")?;
                let next_weight = non_null_weight
                    .checked_add(delta_weight)
                    .ok_or_else(|| DbError::Pipeline("weighted AVG weight overflow".into()))?;
                let next_sum = *sum + delta_sum;
                validate_weighted_avg_state(next_sum, next_weight)?;
                *sum = next_sum;
                *non_null_weight = next_weight;
                Ok(())
            }
        }
    }

    fn evaluate(&self) -> Result<ScalarValue, DbError> {
        match self.mode {
            AvgMode::AppendOnly { count } => append_average(&self.sum, count, &self.output_type),
            AvgMode::Weighted { non_null_weight } => {
                let sum = match &self.sum {
                    ScalarValue::Float64(Some(sum)) => *sum,
                    _ => {
                        return Err(DbError::Pipeline(
                            "weighted AVG has an invalid concrete state shape".into(),
                        ))
                    }
                };
                if non_null_weight == 0 {
                    return Ok(ScalarValue::Float64(None));
                }
                #[allow(clippy::cast_precision_loss)]
                Ok(ScalarValue::Float64(Some(sum / non_null_weight as f64)))
            }
        }
    }

    fn checkpoint_state(&self) -> Vec<ScalarValue> {
        match self.mode {
            AvgMode::AppendOnly { count } => {
                vec![ScalarValue::UInt64(Some(count)), self.sum.clone()]
            }
            AvgMode::Weighted { non_null_weight } => {
                vec![self.sum.clone(), ScalarValue::Int64(Some(non_null_weight))]
            }
        }
    }

    fn merge_checkpoint_state(&mut self, checkpoint: &[ScalarValue]) -> Result<(), DbError> {
        match &mut self.mode {
            AvgMode::AppendOnly { count } => {
                let [ScalarValue::UInt64(Some(delta_count)), delta_sum] = checkpoint else {
                    return Err(DbError::Pipeline(
                        "append-only AVG checkpoint state must be [UInt64, sum]".into(),
                    ));
                };
                if (*delta_count == 0) != delta_sum.is_null() {
                    return Err(DbError::Pipeline(
                        "append-only AVG checkpoint count/sum invariant failed".into(),
                    ));
                }
                let next_count = count
                    .checked_add(*delta_count)
                    .ok_or_else(|| DbError::Pipeline("AVG checkpoint count overflow".into()))?;
                let mut next_sum = self.sum.clone();
                merge_sum_scalar(&mut next_sum, delta_sum)?;
                self.sum = next_sum;
                *count = next_count;
                Ok(())
            }
            AvgMode::Weighted { non_null_weight } => {
                let [ScalarValue::Float64(Some(delta_sum)), ScalarValue::Int64(Some(delta_weight))] =
                    checkpoint
                else {
                    return Err(DbError::Pipeline(
                        "weighted AVG checkpoint state must be [Float64, Int64]".into(),
                    ));
                };
                let ScalarValue::Float64(Some(sum)) = self.sum else {
                    return Err(DbError::Pipeline(
                        "weighted AVG has an invalid concrete state shape".into(),
                    ));
                };
                let next_weight = non_null_weight.checked_add(*delta_weight).ok_or_else(|| {
                    DbError::Pipeline("weighted AVG checkpoint weight overflow".into())
                })?;
                let next_sum = sum + delta_sum;
                validate_weighted_avg_state(next_sum, next_weight)?;
                self.sum = ScalarValue::Float64(Some(next_sum));
                *non_null_weight = next_weight;
                Ok(())
            }
        }
    }
}

#[cfg(feature = "cluster")]
impl AvgState {
    fn batch_delta(
        &self,
        input: &AggregateInput<'_>,
        rows: &[u32],
    ) -> Result<Vec<ScalarValue>, DbError> {
        match self.mode {
            AvgMode::AppendOnly { .. } => Ok(vec![
                ScalarValue::UInt64(Some(non_null_rows(input, rows)?)),
                sum_rows(input, rows, &self.sum.data_type())?,
            ]),
            AvgMode::Weighted { .. } => {
                let (sum, weight) = weighted_f64_delta(input, rows, "weighted AVG")?;
                Ok(vec![
                    ScalarValue::Float64(Some(sum)),
                    ScalarValue::Int64(Some(weight)),
                ])
            }
        }
    }
}

fn non_null_rows(input: &AggregateInput<'_>, rows: &[u32]) -> Result<u64, DbError> {
    let nulls = input.nulls;
    let count = input
        .selected(rows)
        .filter(|&row| nulls.is_none_or(|nulls| nulls.is_valid(row)))
        .count();
    u64::try_from(count).map_err(|_| DbError::Pipeline("AVG batch count exceeds UInt64".into()))
}

fn validate_weighted_avg_state(sum: f64, non_null_weight: i64) -> Result<(), DbError> {
    if !sum.is_finite() {
        return Err(DbError::Pipeline(
            "weighted AVG state must be finite".into(),
        ));
    }
    if non_null_weight < 0 {
        return Err(DbError::Pipeline(
            "weighted AVG non-null weight became negative".into(),
        ));
    }
    if non_null_weight == 0 && sum != 0.0 {
        return Err(DbError::Pipeline(
            "weighted AVG has a nonzero sum with zero non-null weight".into(),
        ));
    }
    Ok(())
}

fn validate_append_avg_types(input: &DataType, output: &DataType) -> Result<(), DbError> {
    let supported = matches!(
        (input, output),
        (DataType::Float64, DataType::Float64)
            | (DataType::Decimal128(_, _), DataType::Decimal128(_, _))
    );
    if !supported {
        return Err(DbError::Unsupported(format!(
            "[{}] managed AVG does not support {input:?} -> {output:?}",
            laminar_core::error_codes::SQL_UNSUPPORTED
        )));
    }
    Ok(())
}

fn append_average(
    sum: &ScalarValue,
    count: u64,
    output_type: &DataType,
) -> Result<ScalarValue, DbError> {
    if count == 0 {
        if !sum.is_null() {
            return Err(DbError::Pipeline(
                "append-only AVG has non-NULL sum with zero count".into(),
            ));
        }
        return ScalarValue::try_from(output_type)
            .map_err(|error| aggregate_error("AVG null result", error));
    }

    macro_rules! decimal_avg {
        ($value:expr, $arrow_type:ty, $sum_scale:expr, $precision:expr, $scale:expr, $count:expr) => {{
            let averager = DecimalAverager::<$arrow_type>::try_new($sum_scale, $precision, $scale)
                .map_err(|error| aggregate_error("AVG decimal setup", error))?;
            averager
                .avg($value, $count)
                .map_err(|error| aggregate_error("AVG decimal evaluation", error))?
        }};
    }

    match (sum, output_type) {
        (ScalarValue::Float64(Some(value)), DataType::Float64) =>
        {
            #[allow(clippy::cast_precision_loss)]
            Ok(ScalarValue::Float64(Some(*value / count as f64)))
        }
        (
            ScalarValue::Decimal128(Some(value), _, sum_scale),
            DataType::Decimal128(precision, scale),
        ) => Ok(ScalarValue::Decimal128(
            Some(decimal_avg!(
                *value,
                Decimal128Type,
                *sum_scale,
                *precision,
                *scale,
                i128::from(count)
            )),
            *precision,
            *scale,
        )),
        _ => Err(DbError::Pipeline(format!(
            "AVG state {:?} cannot produce {output_type:?}",
            sum.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests;
