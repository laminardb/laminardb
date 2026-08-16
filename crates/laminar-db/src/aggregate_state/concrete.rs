use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray};
use arrow::compute;
use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Float64Type, Int64Type, UInt64Type};
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::min_max::{Max, MaxAccumulator, Min, MinAccumulator};
use datafusion::functions_aggregate::sum::Sum;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate_common::utils::DecimalAverager;

use super::AggFuncSpec;
use crate::error::DbError;

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
enum SumMode {
    AppendOnly,
    Weighted { non_null_weight: i64 },
}

#[derive(Clone, Debug)]
pub(crate) struct SumState {
    input_type: DataType,
    output_type: DataType,
    sum: ScalarValue,
    mode: SumMode,
}

impl SumState {
    fn try_new(
        input_type: &DataType,
        output_type: &DataType,
        mode: ConcreteInputMode,
    ) -> Result<Self, DbError> {
        let sum = match mode {
            ConcreteInputMode::AppendOnly => {
                validate_sum_types(input_type, output_type)?;
                ScalarValue::try_from(output_type)
                    .map_err(|error| aggregate_error("SUM state type", error))?
            }
            ConcreteInputMode::Weighted => {
                validate_sum_types(input_type, output_type)?;
                weighted_sum_zero(output_type)?
            }
        };
        Ok(Self {
            input_type: input_type.clone(),
            output_type: output_type.clone(),
            sum,
            mode: match mode {
                ConcreteInputMode::AppendOnly => SumMode::AppendOnly,
                ConcreteInputMode::Weighted => SumMode::Weighted { non_null_weight: 0 },
            },
        })
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DbError> {
        match &mut self.mode {
            SumMode::AppendOnly => {
                let value = append_value(values)?;
                require_array_type(value, &self.input_type, "SUM")?;
                let delta = sum_array(value, &self.output_type)?;
                merge_sum_scalar(&mut self.sum, &delta)
            }
            SumMode::Weighted { non_null_weight } => {
                let (value, weights) = weighted_values(values)?;
                require_array_type(value, &self.input_type, "weighted SUM")?;
                update_weighted_sum(
                    &mut self.sum,
                    non_null_weight,
                    value,
                    weights,
                    &self.output_type,
                )
            }
        }
    }

    fn evaluate(&self) -> Result<ScalarValue, DbError> {
        match self.mode {
            SumMode::AppendOnly => Ok(self.sum.clone()),
            SumMode::Weighted { non_null_weight } => {
                if non_null_weight == 0 {
                    ScalarValue::try_from(&self.output_type)
                        .map_err(|error| aggregate_error("SUM null result", error))
                } else {
                    weighted_sum_result(&self.sum, &self.output_type)
                }
            }
        }
    }

    fn checkpoint_state(&self) -> Vec<ScalarValue> {
        match self.mode {
            SumMode::AppendOnly => vec![self.sum.clone()],
            SumMode::Weighted { non_null_weight } => {
                vec![self.sum.clone(), ScalarValue::Int64(Some(non_null_weight))]
            }
        }
    }

    fn merge_checkpoint_state(&mut self, checkpoint: &[ScalarValue]) -> Result<(), DbError> {
        match &mut self.mode {
            SumMode::AppendOnly => {
                let [sum] = checkpoint else {
                    return Err(DbError::Pipeline(
                        "append-only SUM checkpoint state must contain one scalar".into(),
                    ));
                };
                merge_sum_scalar(&mut self.sum, sum)
            }
            SumMode::Weighted { non_null_weight } => {
                let [sum, ScalarValue::Int64(Some(weight))] = checkpoint else {
                    return Err(DbError::Pipeline(
                        "weighted SUM checkpoint state must be [sum, Int64]".into(),
                    ));
                };
                if sum.is_null() {
                    return Err(DbError::Pipeline(
                        "weighted SUM checkpoint sum must be non-NULL".into(),
                    ));
                }
                let next_weight = non_null_weight.checked_add(*weight).ok_or_else(|| {
                    DbError::Pipeline("weighted SUM checkpoint weight overflow".into())
                })?;
                let mut next_sum = self.sum.clone();
                merge_weighted_sum_scalar(&mut next_sum, sum)?;
                validate_weighted_sum_state(&next_sum, next_weight)?;
                weighted_sum_result(&next_sum, &self.output_type)?;
                self.sum = next_sum;
                *non_null_weight = next_weight;
                Ok(())
            }
        }
    }
}

fn validate_sum_types(input: &DataType, output: &DataType) -> Result<(), DbError> {
    let supported = match (input, output) {
        (DataType::Int64, DataType::Int64)
        | (DataType::UInt64, DataType::UInt64)
        | (DataType::Float64, DataType::Float64) => true,
        (DataType::Decimal128(_, input_scale), DataType::Decimal128(_, output_scale)) => {
            input_scale == output_scale
        }
        _ => false,
    };
    if !supported {
        return Err(DbError::Unsupported(format!(
            "[{}] managed SUM does not support {input:?} -> {output:?}",
            laminar_core::error_codes::SQL_UNSUPPORTED
        )));
    }
    Ok(())
}

fn weighted_sum_zero(output: &DataType) -> Result<ScalarValue, DbError> {
    match output {
        DataType::Int64 | DataType::UInt64 | DataType::Decimal128(_, _) => {
            Ok(ScalarValue::Decimal128(Some(0), 38, 0))
        }
        DataType::Float64 => Ok(ScalarValue::Float64(Some(0.0))),
        _ => Err(DbError::Pipeline(format!(
            "weighted SUM does not support output type {output:?}"
        ))),
    }
}

fn require_array_type(array: &ArrayRef, expected: &DataType, name: &str) -> Result<(), DbError> {
    if array.data_type() != expected {
        return Err(DbError::Pipeline(format!(
            "{name} expected input {expected:?}, got {:?}",
            array.data_type()
        )));
    }
    Ok(())
}

fn sum_array(array: &ArrayRef, output_type: &DataType) -> Result<ScalarValue, DbError> {
    macro_rules! primitive_sum {
        ($arrow_type:ty, $scalar:ident) => {
            ScalarValue::$scalar(
                compute::sum_checked(array.as_primitive::<$arrow_type>())
                    .map_err(|error| aggregate_error("SUM batch overflow", error))?,
            )
        };
        ($arrow_type:ty, $scalar:ident, $precision:expr, $scale:expr) => {
            ScalarValue::$scalar(
                compute::sum_checked(array.as_primitive::<$arrow_type>())
                    .map_err(|error| aggregate_error("SUM batch overflow", error))?,
                $precision,
                $scale,
            )
        };
    }

    let sum = match (array.data_type(), output_type) {
        (DataType::Int64, DataType::Int64) => primitive_sum!(Int64Type, Int64),
        (DataType::UInt64, DataType::UInt64) => primitive_sum!(UInt64Type, UInt64),
        (DataType::Float64, DataType::Float64) => primitive_sum!(Float64Type, Float64),
        (DataType::Decimal128(_, _), DataType::Decimal128(precision, scale)) => {
            primitive_sum!(Decimal128Type, Decimal128, *precision, *scale)
        }
        _ => {
            return Err(DbError::Pipeline(format!(
                "SUM cannot combine input {:?} with state {output_type:?}",
                array.data_type()
            )))
        }
    };
    if let ScalarValue::Decimal128(Some(value), precision, scale) = &sum {
        Decimal128Type::validate_decimal_precision(*value, *precision, *scale)
            .map_err(|error| aggregate_error("SUM batch precision", error))?;
    }
    Ok(sum)
}

fn merge_sum_scalar(total: &mut ScalarValue, delta: &ScalarValue) -> Result<(), DbError> {
    if total.data_type() != delta.data_type() {
        return Err(sum_state_type_error(total, delta));
    }
    if let ScalarValue::Decimal128(Some(value), precision, scale) = delta {
        Decimal128Type::validate_decimal_precision(*value, *precision, *scale)
            .map_err(|error| aggregate_error("SUM checkpoint precision", error))?;
    }
    if delta.is_null() {
        return Ok(());
    }
    if total.is_null() {
        *total = delta.clone();
        return Ok(());
    }

    match (total, delta) {
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => {
            let next = left
                .unwrap()
                .add_checked(right.unwrap())
                .map_err(|error| aggregate_error("SUM state overflow", error))?;
            *left = Some(next);
        }
        (ScalarValue::UInt64(left), ScalarValue::UInt64(right)) => {
            let next = left
                .unwrap()
                .add_checked(right.unwrap())
                .map_err(|error| aggregate_error("SUM state overflow", error))?;
            *left = Some(next);
        }
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => {
            let next = left
                .unwrap()
                .add_checked(right.unwrap())
                .map_err(|error| aggregate_error("SUM state overflow", error))?;
            *left = Some(next);
        }
        (
            ScalarValue::Decimal128(left, left_precision, left_scale),
            ScalarValue::Decimal128(right, right_precision, right_scale),
        ) if left_precision == right_precision && left_scale == right_scale => {
            let next = left
                .unwrap()
                .add_checked(right.unwrap())
                .map_err(|error| aggregate_error("SUM state overflow", error))?;
            Decimal128Type::validate_decimal_precision(next, *left_precision, *left_scale)
                .map_err(|error| aggregate_error("SUM state precision", error))?;
            *left = Some(next);
        }
        (left, right) => return Err(sum_state_type_error(left, right)),
    }
    Ok(())
}

fn merge_weighted_sum_scalar(total: &mut ScalarValue, delta: &ScalarValue) -> Result<(), DbError> {
    match (total, delta) {
        (
            ScalarValue::Decimal128(Some(total), 38, 0),
            ScalarValue::Decimal128(Some(delta), 38, 0),
        ) => {
            let next = total.checked_add(*delta).ok_or_else(|| {
                DbError::Pipeline("weighted SUM checkpoint state overflow".into())
            })?;
            Decimal128Type::validate_decimal_precision(next, 38, 0)
                .map_err(|error| aggregate_error("weighted SUM checkpoint precision", error))?;
            *total = next;
            Ok(())
        }
        (ScalarValue::Float64(Some(total)), ScalarValue::Float64(Some(delta))) => {
            *total += delta;
            Ok(())
        }
        (left, right) => Err(sum_state_type_error(left, right)),
    }
}

fn validate_weighted_sum_state(sum: &ScalarValue, non_null_weight: i64) -> Result<(), DbError> {
    if non_null_weight < 0 {
        return Err(DbError::Pipeline(
            "weighted SUM non-null weight became negative".into(),
        ));
    }
    let is_zero = match sum {
        ScalarValue::Decimal128(Some(value), 38, 0) => *value == 0,
        ScalarValue::Float64(Some(value)) if value.is_finite() => *value == 0.0,
        ScalarValue::Float64(Some(_)) => {
            return Err(DbError::Pipeline(
                "weighted SUM state must be finite".into(),
            ));
        }
        _ => {
            return Err(DbError::Pipeline(
                "weighted SUM has an invalid concrete state shape".into(),
            ));
        }
    };
    if non_null_weight == 0 && !is_zero {
        return Err(DbError::Pipeline(
            "weighted SUM has a nonzero sum with zero non-null weight".into(),
        ));
    }
    Ok(())
}

fn sum_state_type_error(left: &ScalarValue, right: &ScalarValue) -> DbError {
    DbError::Pipeline(format!(
        "aggregate sum state type mismatch: {:?} vs {:?}",
        left.data_type(),
        right.data_type()
    ))
}

fn update_weighted_sum(
    sum: &mut ScalarValue,
    non_null_weight: &mut i64,
    values: &ArrayRef,
    weights: &arrow::array::Int64Array,
    output_type: &DataType,
) -> Result<(), DbError> {
    match sum {
        ScalarValue::Decimal128(Some(total), 38, 0) => {
            let (delta_sum, delta_weight) = match values.data_type() {
                DataType::Int64 => {
                    let values = values.as_primitive::<Int64Type>();
                    exact_weighted_delta(values, weights, |row| i128::from(values.value(row)))?
                }
                DataType::UInt64 => {
                    let values = values.as_primitive::<UInt64Type>();
                    exact_weighted_delta(values, weights, |row| i128::from(values.value(row)))?
                }
                DataType::Decimal128(_, _) => {
                    let values = values.as_primitive::<Decimal128Type>();
                    exact_weighted_delta(values, weights, |row| values.value(row))?
                }
                other => {
                    return Err(DbError::Pipeline(format!(
                        "weighted exact SUM does not support input type {other:?}"
                    )))
                }
            };
            let next_sum = total
                .checked_add(delta_sum)
                .ok_or_else(|| DbError::Pipeline("weighted SUM state overflow".into()))?;
            Decimal128Type::validate_decimal_precision(next_sum, 38, 0)
                .map_err(|error| aggregate_error("weighted SUM state precision", error))?;
            let next_weight = non_null_weight
                .checked_add(delta_weight)
                .ok_or_else(|| DbError::Pipeline("weighted SUM weight overflow".into()))?;
            validate_weighted_sum_state(
                &ScalarValue::Decimal128(Some(next_sum), 38, 0),
                next_weight,
            )?;
            weighted_sum_result(&ScalarValue::Decimal128(Some(next_sum), 38, 0), output_type)?;
            *total = next_sum;
            *non_null_weight = next_weight;
            Ok(())
        }
        ScalarValue::Float64(Some(total)) if output_type == &DataType::Float64 => {
            let values = values
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "weighted SUM(Float64) received {:?}",
                        values.data_type()
                    ))
                })?;
            let (delta_sum, delta_weight) = weighted_f64_delta(values, weights, "weighted SUM")?;
            let next_weight = non_null_weight
                .checked_add(delta_weight)
                .ok_or_else(|| DbError::Pipeline("weighted SUM weight overflow".into()))?;
            let next_sum = *total + delta_sum;
            validate_weighted_sum_state(&ScalarValue::Float64(Some(next_sum)), next_weight)?;
            weighted_sum_result(&ScalarValue::Float64(Some(next_sum)), output_type)?;
            *total = next_sum;
            *non_null_weight = next_weight;
            Ok(())
        }
        _ => Err(DbError::Pipeline(
            "weighted SUM has an invalid concrete state shape".into(),
        )),
    }
}

fn exact_weighted_delta<A: Array>(
    values: &A,
    weights: &arrow::array::Int64Array,
    value_at: impl Fn(usize) -> i128,
) -> Result<(i128, i64), DbError> {
    let mut sum = 0_i128;
    let mut non_null_weight = 0_i64;
    for row in 0..values.len() {
        if values.is_null(row) {
            continue;
        }
        let weight = weights.value(row);
        let weighted = value_at(row)
            .checked_mul(i128::from(weight))
            .ok_or_else(|| DbError::Pipeline("weighted SUM multiplication overflow".into()))?;
        sum = sum
            .checked_add(weighted)
            .ok_or_else(|| DbError::Pipeline("weighted SUM batch overflow".into()))?;
        non_null_weight = non_null_weight
            .checked_add(weight)
            .ok_or_else(|| DbError::Pipeline("weighted SUM weight overflow".into()))?;
    }
    Ok((sum, non_null_weight))
}

fn weighted_f64_delta(
    values: &arrow::array::Float64Array,
    weights: &arrow::array::Int64Array,
    name: &str,
) -> Result<(f64, i64), DbError> {
    let mut sum = 0.0;
    let mut non_null_weight = 0_i64;
    for row in 0..values.len() {
        if values.is_null(row) {
            continue;
        }
        let value = values.value(row);
        if !value.is_finite() {
            return Err(DbError::Pipeline(format!("{name} input must be finite")));
        }
        let weight = weights.value(row);
        #[allow(clippy::cast_precision_loss)]
        let weighted = value * weight as f64;
        if !weighted.is_finite() {
            return Err(DbError::Pipeline(format!(
                "{name} multiplication must be finite"
            )));
        }
        sum += weighted;
        if !sum.is_finite() {
            return Err(DbError::Pipeline(format!(
                "{name} batch result must be finite"
            )));
        }
        non_null_weight = non_null_weight
            .checked_add(weight)
            .ok_or_else(|| DbError::Pipeline(format!("{name} weight overflow")))?;
    }
    Ok((sum, non_null_weight))
}

fn weighted_sum_result(sum: &ScalarValue, output: &DataType) -> Result<ScalarValue, DbError> {
    match (sum, output) {
        (ScalarValue::Decimal128(Some(value), 38, 0), DataType::Int64) => i64::try_from(*value)
            .map(|value| ScalarValue::Int64(Some(value)))
            .map_err(|_| DbError::Pipeline("weighted SUM result exceeds Int64".into())),
        (ScalarValue::Decimal128(Some(value), 38, 0), DataType::UInt64) => u64::try_from(*value)
            .map(|value| ScalarValue::UInt64(Some(value)))
            .map_err(|_| DbError::Pipeline("weighted SUM result exceeds UInt64".into())),
        (ScalarValue::Decimal128(Some(value), 38, 0), DataType::Decimal128(precision, scale)) => {
            Decimal128Type::validate_decimal_precision(*value, *precision, *scale)
                .map_err(|error| aggregate_error("weighted SUM decimal overflow", error))?;
            Ok(ScalarValue::Decimal128(Some(*value), *precision, *scale))
        }
        (ScalarValue::Float64(value), DataType::Float64) => Ok(ScalarValue::Float64(*value)),
        _ => Err(DbError::Pipeline(format!(
            "weighted SUM state cannot produce {output:?}"
        ))),
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
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Decimal128Array, Float64Array, Int64Array, StringArray};
    use datafusion_expr::{AggregateUDF, AggregateUDFImpl};

    fn spec<T: AggregateUDFImpl + 'static>(
        implementation: T,
        input_type: DataType,
        return_type: DataType,
        count_star: bool,
    ) -> AggFuncSpec {
        AggFuncSpec {
            udf: Arc::new(AggregateUDF::new_from_impl(implementation)),
            input_types: vec![input_type],
            input_col_indices: vec![0],
            output_name: "value".into(),
            return_type,
            is_count_star: count_star,
            filter_col_index: None,
        }
    }

    #[test]
    fn cloned_append_sum_is_an_exact_cut() {
        let spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
        let mut live =
            ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
        live.update_batch(&[Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .unwrap();
        let captured = live.clone();

        live.update_batch(&[Arc::new(Int64Array::from(vec![4]))])
            .unwrap();

        assert_eq!(
            captured.checkpoint_state().unwrap(),
            vec![ScalarValue::Int64(Some(6))]
        );
        assert_eq!(live.evaluate().unwrap(), ScalarValue::Int64(Some(10)));

        let mut restored =
            ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
        restored
            .merge_checkpoint_state(&captured.checkpoint_state().unwrap())
            .unwrap();
        assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(6)));
    }

    #[test]
    fn decimal_avg_widens_running_sum_and_preserves_scale() {
        let spec = spec(
            Avg::new(),
            DataType::Decimal128(10, 2),
            DataType::Decimal128(14, 6),
            false,
        );
        let mut state =
            ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
        let values = Decimal128Array::from(vec![Some(9_999_999_999), Some(9_999_999_999), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        state.update_batch(&[Arc::new(values)]).unwrap();

        let checkpoint = state.checkpoint_state().unwrap();
        assert_eq!(
            checkpoint,
            vec![
                ScalarValue::UInt64(Some(2)),
                ScalarValue::Decimal128(Some(19_999_999_998), 38, 2),
            ]
        );
        assert_eq!(
            state.evaluate().unwrap(),
            ScalarValue::Decimal128(Some(99_999_999_990_000), 14, 6)
        );

        let mut restored =
            ConcreteAggregateState::try_new(&spec, ConcreteInputMode::AppendOnly).unwrap();
        restored.merge_checkpoint_state(&checkpoint).unwrap();
        assert_eq!(restored.evaluate().unwrap(), state.evaluate().unwrap());
    }

    #[test]
    fn weighted_arithmetic_uses_existing_checkpoint_shapes() {
        let sum_spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
        let mut sum =
            ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::Weighted).unwrap();
        sum.update_batch(&[
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ])
        .unwrap();
        sum.update_batch(&[
            Arc::new(Int64Array::from(vec![10, 30])),
            Arc::new(Int64Array::from(vec![-1, 1])),
        ])
        .unwrap();
        assert_eq!(
            sum.checkpoint_state().unwrap(),
            vec![
                ScalarValue::Decimal128(Some(50), 38, 0),
                ScalarValue::Int64(Some(2)),
            ]
        );
        assert_eq!(sum.evaluate().unwrap(), ScalarValue::Int64(Some(50)));

        let avg_spec = spec(Avg::new(), DataType::Float64, DataType::Float64, false);
        let mut avg =
            ConcreteAggregateState::try_new(&avg_spec, ConcreteInputMode::Weighted).unwrap();
        avg.update_batch(&[
            Arc::new(Float64Array::from(vec![10.0, 20.0])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ])
        .unwrap();
        avg.update_batch(&[
            Arc::new(Float64Array::from(vec![10.0, 30.0])),
            Arc::new(Int64Array::from(vec![-1, 1])),
        ])
        .unwrap();
        assert_eq!(
            avg.checkpoint_state().unwrap(),
            vec![
                ScalarValue::Float64(Some(50.0)),
                ScalarValue::Int64(Some(2)),
            ]
        );
        assert_eq!(avg.evaluate().unwrap(), ScalarValue::Float64(Some(25.0)));
    }

    #[test]
    fn invalid_arithmetic_is_rejected_without_mutating_state() {
        let sum_spec = spec(Sum::new(), DataType::Int64, DataType::Int64, false);
        let mut append_sum =
            ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::AppendOnly).unwrap();
        assert!(append_sum
            .update_batch(&[Arc::new(Int64Array::from(vec![i64::MAX, 1]))])
            .is_err());
        assert_eq!(append_sum.evaluate().unwrap(), ScalarValue::Int64(None));

        let mut weighted_sum =
            ConcreteAggregateState::try_new(&sum_spec, ConcreteInputMode::Weighted).unwrap();
        assert!(weighted_sum
            .update_batch(&[
                Arc::new(Int64Array::from(vec![i64::MAX, 1])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ])
            .is_err());
        assert_eq!(weighted_sum.evaluate().unwrap(), ScalarValue::Int64(None));
        assert!(weighted_sum
            .merge_checkpoint_state(&[
                ScalarValue::Decimal128(Some(i128::from(i64::MAX) + 1), 38, 0),
                ScalarValue::Int64(Some(1)),
            ])
            .is_err());
        assert_eq!(weighted_sum.evaluate().unwrap(), ScalarValue::Int64(None));

        let count_spec = spec(Count::new(), DataType::Int64, DataType::Int64, true);
        let mut weighted_count =
            ConcreteAggregateState::try_new(&count_spec, ConcreteInputMode::Weighted).unwrap();
        assert!(weighted_count
            .update_batch(&[
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(Int64Array::from(vec![-1])),
            ])
            .is_err());
        assert_eq!(
            weighted_count.evaluate().unwrap(),
            ScalarValue::Int64(Some(0))
        );

        let avg_spec = spec(Avg::new(), DataType::Float64, DataType::Float64, false);
        let mut weighted_avg =
            ConcreteAggregateState::try_new(&avg_spec, ConcreteInputMode::Weighted).unwrap();
        assert!(weighted_avg
            .update_batch(&[
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
                Arc::new(Int64Array::from(vec![1])),
            ])
            .is_err());
        assert_eq!(weighted_avg.evaluate().unwrap(), ScalarValue::Float64(None));
    }

    #[test]
    fn extrema_are_append_only_and_use_datafusion_ordering() {
        let min_spec = spec(Min::new(), DataType::Utf8, DataType::Utf8, false);
        let mut min =
            ConcreteAggregateState::try_new(&min_spec, ConcreteInputMode::AppendOnly).unwrap();
        min.update_batch(&[Arc::new(StringArray::from(vec![
            Some("pear"),
            None,
            Some("apple"),
        ]))])
        .unwrap();
        assert_eq!(
            min.checkpoint_state().unwrap(),
            vec![ScalarValue::Utf8(Some("apple".into()))]
        );
        assert!(min.nested_retained_bytes() >= "apple".len());

        let error =
            ConcreteAggregateState::try_new(&min_spec, ConcreteInputMode::Weighted).unwrap_err();
        assert!(error.to_string().contains("append-only"));
    }
}
