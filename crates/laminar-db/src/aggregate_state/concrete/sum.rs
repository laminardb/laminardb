use arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray};
use arrow::compute;
use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Float64Type, Int64Type, UInt64Type};
use datafusion_common::ScalarValue;

use super::{aggregate_error, append_value, weighted_values, ConcreteInputMode};
use crate::error::DbError;

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
    pub(super) fn try_new(
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

    pub(super) fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DbError> {
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

    pub(super) fn evaluate(&self) -> Result<ScalarValue, DbError> {
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

    pub(super) fn checkpoint_state(&self) -> Vec<ScalarValue> {
        match self.mode {
            SumMode::AppendOnly => vec![self.sum.clone()],
            SumMode::Weighted { non_null_weight } => {
                vec![self.sum.clone(), ScalarValue::Int64(Some(non_null_weight))]
            }
        }
    }

    pub(super) fn merge_checkpoint_state(
        &mut self,
        checkpoint: &[ScalarValue],
    ) -> Result<(), DbError> {
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

pub(super) fn require_array_type(
    array: &ArrayRef,
    expected: &DataType,
    name: &str,
) -> Result<(), DbError> {
    if array.data_type() != expected {
        return Err(DbError::Pipeline(format!(
            "{name} expected input {expected:?}, got {:?}",
            array.data_type()
        )));
    }
    Ok(())
}

pub(super) fn sum_array(array: &ArrayRef, output_type: &DataType) -> Result<ScalarValue, DbError> {
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

pub(super) fn merge_sum_scalar(
    total: &mut ScalarValue,
    delta: &ScalarValue,
) -> Result<(), DbError> {
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

pub(super) fn weighted_f64_delta(
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
