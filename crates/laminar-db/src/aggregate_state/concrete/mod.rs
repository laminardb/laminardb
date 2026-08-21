use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Decimal128Type, Float64Type};
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::min_max::{Max, MaxAccumulator, Min, MinAccumulator};
use datafusion::functions_aggregate::sum::Sum;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate_common::utils::DecimalAverager;

use super::AggFuncSpec;
use crate::error::DbError;

mod sum;

pub(crate) use sum::SumState;
use sum::{merge_sum_scalar, require_array_type, sum_array, weighted_f64_delta};

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
    Min(MinAccumulator),
    Max(MaxAccumulator),
}

impl ConcreteAggregateState {
    pub(crate) fn try_new(spec: &AggFuncSpec, mode: ConcreteInputMode) -> Result<Self, DbError> {
        if spec.input_types.len() != 1 {
            return Err(unsupported(spec, "exactly one aggregate input is required"));
        }

        let implementation = spec.udf.inner();
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
        if implementation.is::<Min>() {
            require_append_only(spec, mode)?;
            return MinAccumulator::try_new(&spec.return_type)
                .map(Self::Min)
                .map_err(|error| aggregate_error("MIN state creation", error));
        }
        if implementation.is::<Max>() {
            require_append_only(spec, mode)?;
            return MaxAccumulator::try_new(&spec.return_type)
                .map(Self::Max)
                .map_err(|error| aggregate_error("MAX state creation", error));
        }

        Err(unsupported(
            spec,
            "only built-in COUNT, SUM, AVG, MIN, and MAX are supported",
        ))
    }

    pub(crate) fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DbError> {
        match self {
            Self::Count(state) => state.update_batch(values),
            Self::Sum(state) => state.update_batch(values),
            Self::Avg(state) => state.update_batch(values),
            Self::Min(state) => {
                let value = append_value(values)?;
                state
                    .update_batch(std::slice::from_ref(value))
                    .map_err(|error| aggregate_error("MIN update", error))
            }
            Self::Max(state) => {
                let value = append_value(values)?;
                state
                    .update_batch(std::slice::from_ref(value))
                    .map_err(|error| aggregate_error("MAX update", error))
            }
        }
    }

    pub(crate) fn evaluate(&mut self) -> Result<ScalarValue, DbError> {
        match self {
            Self::Count(state) => Ok(ScalarValue::Int64(Some(state.count))),
            Self::Sum(state) => state.evaluate(),
            Self::Avg(state) => state.evaluate(),
            Self::Min(state) => state
                .evaluate()
                .map_err(|error| aggregate_error("MIN evaluation", error)),
            Self::Max(state) => state
                .evaluate()
                .map_err(|error| aggregate_error("MAX evaluation", error)),
        }
    }

    pub(crate) fn checkpoint_state(&self) -> Result<Vec<ScalarValue>, DbError> {
        match self {
            Self::Count(state) => Ok(vec![ScalarValue::Int64(Some(state.count))]),
            Self::Sum(state) => Ok(state.checkpoint_state()),
            Self::Avg(state) => Ok(state.checkpoint_state()),
            Self::Min(state) => state
                .clone()
                .state()
                .map_err(|error| aggregate_error("MIN checkpoint state", error)),
            Self::Max(state) => state
                .clone()
                .state()
                .map_err(|error| aggregate_error("MAX checkpoint state", error)),
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
            Self::Min(state) => merge_extremum_checkpoint(state, checkpoint, "MIN"),
            Self::Max(state) => merge_extremum_checkpoint(state, checkpoint, "MAX"),
        }
    }

    pub(crate) fn nested_retained_bytes(&self) -> usize {
        match self {
            Self::Min(state) => state.size().saturating_sub(std::mem::size_of_val(state)),
            Self::Max(state) => state.size().saturating_sub(std::mem::size_of_val(state)),
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

fn require_append_only(spec: &AggFuncSpec, mode: ConcreteInputMode) -> Result<(), DbError> {
    if mode == ConcreteInputMode::Weighted {
        return Err(unsupported(spec, "MIN/MAX require append-only input"));
    }
    Ok(())
}

fn aggregate_error(context: &str, error: impl std::fmt::Display) -> DbError {
    DbError::Pipeline(format!("{context}: {error}"))
}

fn append_value(values: &[ArrayRef]) -> Result<&ArrayRef, DbError> {
    let [value] = values else {
        return Err(DbError::Pipeline(format!(
            "append-only aggregate expected one input array, got {}",
            values.len()
        )));
    };
    Ok(value)
}

fn weighted_values(values: &[ArrayRef]) -> Result<(&ArrayRef, &arrow::array::Int64Array), DbError> {
    let [value, weight] = values else {
        return Err(DbError::Pipeline(format!(
            "weighted aggregate expected value and weight arrays, got {} inputs",
            values.len()
        )));
    };
    if value.len() != weight.len() {
        return Err(DbError::Pipeline(format!(
            "weighted aggregate value/weight length mismatch: {} vs {}",
            value.len(),
            weight.len()
        )));
    }
    let weights = weight
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| {
            DbError::Pipeline(format!(
                "weighted aggregate requires Int64 weights, got {:?}",
                weight.data_type()
            ))
        })?;
    if weights.null_count() != 0 {
        return Err(DbError::Pipeline(
            "weighted aggregate does not accept NULL weights".into(),
        ));
    }
    Ok((value, weights))
}

#[derive(Clone, Debug)]
pub(crate) struct CountState {
    count: i64,
    count_star: bool,
    weighted: bool,
}

impl CountState {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DbError> {
        let delta = if self.weighted {
            let (value, weights) = weighted_values(values)?;
            weights
                .values()
                .iter()
                .enumerate()
                .filter(|(row, _)| self.count_star || !value.is_null(*row))
                .try_fold(0_i64, |total, (_, weight)| total.checked_add(*weight))
                .ok_or_else(|| DbError::Pipeline("COUNT weight overflow".into()))?
        } else {
            let value = append_value(values)?;
            let rows = if self.count_star {
                value.len()
            } else {
                value.len().checked_sub(value.null_count()).ok_or_else(|| {
                    DbError::Pipeline("COUNT null count exceeds array length".into())
                })?
            };
            i64::try_from(rows)
                .map_err(|_| DbError::Pipeline("COUNT batch row count exceeds Int64".into()))?
        };
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

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DbError> {
        match &mut self.mode {
            AvgMode::AppendOnly { count } => {
                let value = append_value(values)?;
                require_array_type(value, &self.input_type, "AVG")?;
                let non_null = value.len().checked_sub(value.null_count()).ok_or_else(|| {
                    DbError::Pipeline("AVG null count exceeds array length".into())
                })?;
                let delta_count = u64::try_from(non_null)
                    .map_err(|_| DbError::Pipeline("AVG batch count exceeds UInt64".into()))?;
                let next_count = count
                    .checked_add(delta_count)
                    .ok_or_else(|| DbError::Pipeline("AVG count overflow".into()))?;
                let delta = sum_array(value, &self.sum.data_type())?;
                merge_sum_scalar(&mut self.sum, &delta)?;
                *count = next_count;
                Ok(())
            }
            AvgMode::Weighted { non_null_weight } => {
                let (value, weights) = weighted_values(values)?;
                require_array_type(value, &self.input_type, "weighted AVG")?;
                let ScalarValue::Float64(Some(sum)) = &mut self.sum else {
                    return Err(DbError::Pipeline(
                        "weighted AVG has an invalid concrete state shape".into(),
                    ));
                };
                let values = value.as_primitive::<Float64Type>();
                let (delta_sum, delta_weight) =
                    weighted_f64_delta(values, weights, "weighted AVG")?;
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

fn merge_extremum_checkpoint<T: Accumulator>(
    state: &mut T,
    checkpoint: &[ScalarValue],
    name: &str,
) -> Result<(), DbError> {
    let [value] = checkpoint else {
        return Err(DbError::Pipeline(format!(
            "{name} checkpoint state must contain one scalar"
        )));
    };
    let array = value
        .to_array()
        .map_err(|error| aggregate_error(&format!("{name} checkpoint scalar"), error))?;
    state
        .merge_batch(&[array])
        .map_err(|error| aggregate_error(&format!("{name} checkpoint merge"), error))
}

#[cfg(test)]
mod tests;
