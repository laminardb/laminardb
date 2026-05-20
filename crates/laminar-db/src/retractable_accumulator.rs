//! Weight-aware accumulators for Z-set changelog aggregation.
//!
//! When aggregating over a changelog stream (one with a `__weight` column),
//! standard `DataFusion` accumulators produce incorrect results because they
//! are purely additive — they have no concept of retraction.
//!
//! This module provides [`datafusion_expr::Accumulator`] implementations that
//! receive the weight as the **last element** of `update_batch` inputs and
//! handle retraction internally.
//!
//! # Supported aggregates
//!
//! | Function | Strategy | State | Retract |
//! |----------|----------|-------|---------|
//! | SUM | `SUM(value * weight)` | `(sum, count)` | O(1) |
//! | COUNT(\*) | `SUM(weight)` | `count` | O(1) |
//! | COUNT(col) | `SUM(weight)` where col IS NOT NULL | `count` | O(1) |
//! | AVG | `SUM(value * weight) / SUM(weight)` | `(sum, wt_sum)` | O(1) |
//! | MIN | Counted multiset, result = first key | `BTreeMap` | O(log n) |
//! | MAX | Counted multiset, result = last key | `BTreeMap` | O(log n) |

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;

use crate::error::DbError;

/// Extract `i64` weights from the last array in the inputs slice.
/// Returns an error rather than panicking if the contract is violated
/// (empty input, wrong dtype) — the upstream is internal but a corrupt
/// checkpoint or hand-rolled CDC source must not crash the engine task.
fn weight_array(values: &[ArrayRef]) -> datafusion_common::Result<&arrow::array::Int64Array> {
    let last = values.last().ok_or_else(|| {
        datafusion_common::DataFusionError::Internal(
            "retractable accumulator: input is missing the __weight column".into(),
        )
    })?;
    last.as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "retractable accumulator: __weight must be Int64, got {:?}",
                last.data_type()
            ))
        })
}

/// Cast a numeric array element at `row` to `f64`.
fn to_f64(arr: &ArrayRef, row: usize) -> Option<f64> {
    if arr.is_null(row) {
        return None;
    }
    match arr.data_type() {
        DataType::Int8 => Some(f64::from(
            arr.as_primitive::<arrow::datatypes::Int8Type>().value(row),
        )),
        DataType::Int16 => Some(f64::from(
            arr.as_primitive::<arrow::datatypes::Int16Type>().value(row),
        )),
        DataType::Int32 => Some(f64::from(
            arr.as_primitive::<arrow::datatypes::Int32Type>().value(row),
        )),
        DataType::Int64 =>
        {
            #[allow(clippy::cast_precision_loss)]
            Some(arr.as_primitive::<arrow::datatypes::Int64Type>().value(row) as f64)
        }
        DataType::UInt8 => Some(f64::from(
            arr.as_primitive::<arrow::datatypes::UInt8Type>().value(row),
        )),
        DataType::UInt16 => Some(f64::from(
            arr.as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row),
        )),
        DataType::UInt32 => Some(f64::from(
            arr.as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row),
        )),
        DataType::UInt64 =>
        {
            #[allow(clippy::cast_precision_loss)]
            Some(
                arr.as_primitive::<arrow::datatypes::UInt64Type>()
                    .value(row) as f64,
            )
        }
        DataType::Float32 => Some(f64::from(
            arr.as_primitive::<arrow::datatypes::Float32Type>()
                .value(row),
        )),
        DataType::Float64 => Some(
            arr.as_primitive::<arrow::datatypes::Float64Type>()
                .value(row),
        ),
        _ => None,
    }
}

/// Convert an `f64` value back to the target `ScalarValue` type.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
fn f64_to_scalar(v: f64, dt: &DataType) -> ScalarValue {
    match dt {
        DataType::Int8 => ScalarValue::Int8(Some(v as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(v as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(v as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(v as i64)),
        DataType::UInt8 => ScalarValue::UInt8(Some(v as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(v as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(v as u32)),
        DataType::UInt64 => ScalarValue::UInt64(Some(v as u64)),
        DataType::Float32 => ScalarValue::Float32(Some(v as f32)),
        _ => ScalarValue::Float64(Some(v)),
    }
}

/// Create a retractable accumulator for the given aggregate function name.
///
/// Returns `Ok(accumulator)` for supported functions, or `Err` for unsupported.
/// The `return_type` is the expected output data type (used for casting on
/// evaluate).
pub(crate) fn create_retractable(
    func_name: &str,
    return_type: &DataType,
    is_count_star: bool,
) -> Result<Box<dyn Accumulator>, DbError> {
    match func_name {
        "sum" => Ok(Box::new(RetractableSumAccum::new(return_type.clone()))),
        "count" => {
            if is_count_star {
                Ok(Box::new(RetractableCountStarAccum::default()))
            } else {
                Ok(Box::new(RetractableCountAccum::default()))
            }
        }
        "avg" => Ok(Box::new(RetractableAvgAccum::default())),
        "min" => Ok(Box::new(RetractableExtremumAccum::new(
            return_type.clone(),
            Extremum::Min,
        ))),
        "max" => Ok(Box::new(RetractableExtremumAccum::new(
            return_type.clone(),
            Extremum::Max,
        ))),
        other => Err(DbError::Pipeline(format!(
            "Cannot compute {other}() over a changelog stream. \
             Supported: SUM, COUNT, AVG, MIN, MAX.",
        ))),
    }
}

// ═══════════════════════════════════════════════════════════════════
// SUM
// ═══════════════════════════════════════════════════════════════════

/// Read an integer-typed value as `i128` (covers Int8/16/32/64,
/// UInt8/16/32/64, and Decimal128). Returns `None` for nulls or
/// non-integer types.
fn read_i128(arr: &ArrayRef, row: usize) -> Option<i128> {
    if arr.is_null(row) {
        return None;
    }
    match arr.data_type() {
        DataType::Int8 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::Int8Type>().value(row),
        )),
        DataType::Int16 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::Int16Type>().value(row),
        )),
        DataType::Int32 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::Int32Type>().value(row),
        )),
        DataType::Int64 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::Int64Type>().value(row),
        )),
        DataType::UInt8 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::UInt8Type>().value(row),
        )),
        DataType::UInt16 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row),
        )),
        DataType::UInt32 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row),
        )),
        DataType::UInt64 => Some(i128::from(
            arr.as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row),
        )),
        DataType::Decimal128(_, _) => Some(
            arr.as_primitive::<arrow::datatypes::Decimal128Type>()
                .value(row),
        ),
        _ => None,
    }
}

/// Cast an `i128` sum back to a `ScalarValue` of the target type.
/// Saturates on out-of-range integer targets so a pathological sum can
/// never panic; Decimal128 keeps full precision.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn i128_to_scalar(v: i128, dt: &DataType) -> ScalarValue {
    let clamp = |lo: i128, hi: i128| v.clamp(lo, hi);
    match dt {
        DataType::Int8 => {
            ScalarValue::Int8(Some(clamp(i128::from(i8::MIN), i128::from(i8::MAX)) as i8))
        }
        DataType::Int16 => ScalarValue::Int16(Some(
            clamp(i128::from(i16::MIN), i128::from(i16::MAX)) as i16,
        )),
        DataType::Int32 => ScalarValue::Int32(Some(
            clamp(i128::from(i32::MIN), i128::from(i32::MAX)) as i32,
        )),
        DataType::Int64 => ScalarValue::Int64(Some(
            clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
        )),
        DataType::UInt8 => ScalarValue::UInt8(Some(clamp(0, i128::from(u8::MAX)) as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(clamp(0, i128::from(u16::MAX)) as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(clamp(0, i128::from(u32::MAX)) as u32)),
        DataType::UInt64 => ScalarValue::UInt64(Some(clamp(0, i128::from(u64::MAX)) as u64)),
        DataType::Decimal128(p, s) => ScalarValue::Decimal128(Some(v), *p, *s),
        // Float / unknown: fall back to f64 via i128 → f64 (precision loss
        // only at very large magnitudes, which couldn't have been an
        // integer-typed sum to begin with).
        #[allow(clippy::cast_precision_loss)]
        _ => ScalarValue::Float64(Some(v as f64)),
    }
}

fn is_integer_or_decimal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal128(_, _)
    )
}

/// Retractable SUM: `SUM(value * weight)`.
///
/// Integer / `Decimal128` types accumulate in `i128` so a SUM beyond
/// `2^53` no longer silently loses precision — `f64` is reserved for
/// genuinely floating-point targets where the precision tradeoff is
/// inherent to the type.
#[derive(Debug)]
struct RetractableSumAccum {
    return_type: DataType,
    is_int: bool,
    sum_i128: i128,
    sum_f64: f64,
    count: i64,
}

impl RetractableSumAccum {
    fn new(return_type: DataType) -> Self {
        let is_int = is_integer_or_decimal(&return_type);
        Self {
            return_type,
            is_int,
            sum_i128: 0,
            sum_f64: 0.0,
            count: 0,
        }
    }
}

impl Accumulator for RetractableSumAccum {
    /// Inputs: `[value_col, weight_col]`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let weights = weight_array(values)?;
        let value_arr = &values[0];
        for row in 0..value_arr.len() {
            let w = weights.value(row);
            if self.is_int {
                if let Some(v) = read_i128(value_arr, row) {
                    let prod = v.saturating_mul(i128::from(w));
                    self.sum_i128 = self.sum_i128.saturating_add(prod);
                    self.count = self.count.saturating_add(w);
                }
            } else if let Some(v) = to_f64(value_arr, row) {
                #[allow(clippy::cast_precision_loss)]
                {
                    self.sum_f64 += v * w as f64;
                }
                self.count = self.count.saturating_add(w);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        if self.count == 0 {
            return ScalarValue::try_from(&self.return_type);
        }
        if self.is_int {
            Ok(i128_to_scalar(self.sum_i128, &self.return_type))
        } else {
            Ok(f64_to_scalar(self.sum_f64, &self.return_type))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let sum = if self.is_int {
            ScalarValue::Decimal128(Some(self.sum_i128), 38, 0)
        } else {
            ScalarValue::Float64(Some(self.sum_f64))
        };
        Ok(vec![sum, ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        let counts = states[1].as_primitive::<arrow::datatypes::Int64Type>();
        if self.is_int {
            let sums = states[0].as_primitive::<arrow::datatypes::Decimal128Type>();
            for row in 0..sums.len() {
                if !sums.is_null(row) {
                    self.sum_i128 = self.sum_i128.saturating_add(sums.value(row));
                }
                if !counts.is_null(row) {
                    self.count = self.count.saturating_add(counts.value(row));
                }
            }
        } else {
            let sums = states[0].as_primitive::<arrow::datatypes::Float64Type>();
            for row in 0..sums.len() {
                if !sums.is_null(row) {
                    self.sum_f64 += sums.value(row);
                }
                if !counts.is_null(row) {
                    self.count = self.count.saturating_add(counts.value(row));
                }
            }
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
// COUNT(*)
// ═══════════════════════════════════════════════════════════════════

/// Retractable COUNT(\*): `SUM(weight)`.
///
/// The dummy boolean input column is ignored. Only the weight column matters.
#[derive(Debug, Default)]
struct RetractableCountStarAccum {
    count: i64,
}

impl Accumulator for RetractableCountStarAccum {
    /// Inputs: `[dummy_bool, weight_col]`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let weights = weight_array(values)?;
        for row in 0..weights.len() {
            self.count += weights.value(row);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        let counts = states[0].as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..counts.len() {
            if !counts.is_null(row) {
                self.count += counts.value(row);
            }
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
// COUNT(col)
// ═══════════════════════════════════════════════════════════════════

/// Retractable COUNT(col): `SUM(weight)` where col IS NOT NULL.
#[derive(Debug, Default)]
struct RetractableCountAccum {
    count: i64,
}

impl Accumulator for RetractableCountAccum {
    /// Inputs: `[value_col, weight_col]`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let weights = weight_array(values)?;
        let value_arr = &values[0];
        for row in 0..value_arr.len() {
            if !value_arr.is_null(row) {
                self.count += weights.value(row);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        let counts = states[0].as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..counts.len() {
            if !counts.is_null(row) {
                self.count += counts.value(row);
            }
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
// AVG
// ═══════════════════════════════════════════════════════════════════

/// Retractable AVG: `SUM(value * weight) / SUM(weight)`.
#[derive(Debug, Default)]
struct RetractableAvgAccum {
    weighted_sum: f64,
    weight_sum: i64,
}

impl Accumulator for RetractableAvgAccum {
    /// Inputs: `[value_col, weight_col]`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let weights = weight_array(values)?;
        let value_arr = &values[0];
        for row in 0..value_arr.len() {
            if let Some(v) = to_f64(value_arr, row) {
                let w = weights.value(row);
                #[allow(clippy::cast_precision_loss)]
                {
                    self.weighted_sum += v * w as f64;
                }
                self.weight_sum += w;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        if self.weight_sum == 0 {
            return Ok(ScalarValue::Float64(None));
        }
        #[allow(clippy::cast_precision_loss)]
        let avg = self.weighted_sum / self.weight_sum as f64;
        Ok(ScalarValue::Float64(Some(avg)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.weighted_sum)),
            ScalarValue::Int64(Some(self.weight_sum)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        let sums = states[0].as_primitive::<arrow::datatypes::Float64Type>();
        let wts = states[1].as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..sums.len() {
            if !sums.is_null(row) {
                self.weighted_sum += sums.value(row);
            }
            if !wts.is_null(row) {
                self.weight_sum += wts.value(row);
            }
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
// MIN / MAX (shared implementation)
// ═══════════════════════════════════════════════════════════════════

/// Whether the extremum accumulator tracks the minimum or maximum.
#[derive(Debug, Clone, Copy)]
enum Extremum {
    Min,
    Max,
}

/// Retractable MIN/MAX using a counted multiset keyed by an Arrow
/// row-encoded value. `arrow::row::RowConverter` produces a stable
/// lexicographically-sortable byte representation for every relevant
/// scalar type (integers, decimals, dates, timestamps, strings,
/// binaries, booleans, floats), so the same operator works correctly
/// for any of them — no more silent NULLs from `to_f64` on Decimal /
/// Date / Utf8 columns.
///
/// On insert (weight > 0): increment count for the value's row key.
/// On retract (weight < 0): decrement; remove the entry at zero.
/// Result: first key (MIN) or last key (MAX), re-materialised via the
/// same row converter back to a `ScalarValue` of the return type.
#[derive(Debug)]
struct RetractableExtremumAccum {
    counts: BTreeMap<arrow::row::OwnedRow, i64>,
    row_converter: arrow::row::RowConverter,
    return_type: DataType,
    direction: Extremum,
}

impl RetractableExtremumAccum {
    fn new(return_type: DataType, direction: Extremum) -> Self {
        let row_converter =
            arrow::row::RowConverter::new(vec![arrow::row::SortField::new(return_type.clone())])
                .expect("RowConverter supports the MIN/MAX value type");
        Self {
            counts: BTreeMap::new(),
            row_converter,
            return_type,
            direction,
        }
    }
}

impl Accumulator for RetractableExtremumAccum {
    /// Inputs: `[value_col, weight_col]`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        use std::collections::btree_map::Entry;
        let weights = weight_array(values)?;
        let value_arr = &values[0];
        let rows = self
            .row_converter
            .convert_columns(&[Arc::clone(value_arr)])
            .map_err(|e| {
                datafusion_common::DataFusionError::Internal(format!("MIN/MAX row encode: {e}"))
            })?;
        for row in 0..value_arr.len() {
            if value_arr.is_null(row) {
                continue;
            }
            let key = rows.row(row).owned();
            let w = weights.value(row);
            if w == 0 {
                continue;
            }
            match self.counts.entry(key) {
                Entry::Occupied(mut o) => {
                    *o.get_mut() += w;
                    if *o.get() == 0 {
                        o.remove();
                    }
                }
                Entry::Vacant(v) => {
                    v.insert(w);
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        let key = match self.direction {
            Extremum::Min => self.counts.first_key_value().map(|(k, _)| k),
            Extremum::Max => self.counts.last_key_value().map(|(k, _)| k),
        };
        let Some(key) = key else {
            return ScalarValue::try_from(&self.return_type);
        };
        let arrays = self
            .row_converter
            .convert_rows(std::iter::once(key.row()))
            .map_err(|e| {
                datafusion_common::DataFusionError::Internal(format!("MIN/MAX row decode: {e}"))
            })?;
        ScalarValue::try_from_array(&arrays[0], 0)
    }

    fn size(&self) -> usize {
        // Each entry: key bytes + 8B count + ~64B BTreeMap node overhead.
        let key_bytes: usize = self.counts.keys().map(|k| k.as_ref().len()).sum();
        std::mem::size_of::<Self>() + key_bytes + self.counts.len() * 72
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        // Materialise the keys back into an Arrow array of the value
        // type, then expose key+count as List columns.
        let key_arrays = self
            .row_converter
            .convert_rows(self.counts.keys().map(arrow::row::OwnedRow::row))
            .map_err(|e| {
                datafusion_common::DataFusionError::Internal(format!("MIN/MAX state encode: {e}"))
            })?;
        let key_arr = Arc::clone(&key_arrays[0]);
        let key_scalars: Vec<ScalarValue> = (0..key_arr.len())
            .map(|i| ScalarValue::try_from_array(&key_arr, i))
            .collect::<datafusion_common::Result<_>>()?;
        let val_scalars: Vec<ScalarValue> = self
            .counts
            .values()
            .map(|c| ScalarValue::Int64(Some(*c)))
            .collect();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list(&key_scalars, &self.return_type, true)),
            ScalarValue::List(ScalarValue::new_list(&val_scalars, &DataType::Int64, true)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        use std::collections::btree_map::Entry;
        let keys_list = states[0].as_list::<i32>();
        let vals_list = states[1].as_list::<i32>();
        for row in 0..keys_list.len() {
            let keys_arr = keys_list.value(row);
            let vals_arr = vals_list.value(row);
            let v_arr = vals_arr.as_primitive::<arrow::datatypes::Int64Type>();
            let row_keys = self
                .row_converter
                .convert_columns(&[Arc::clone(&keys_arr)])
                .map_err(|e| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "MIN/MAX merge encode: {e}"
                    ))
                })?;
            for i in 0..keys_arr.len() {
                let cnt = v_arr.value(i);
                if cnt == 0 {
                    continue;
                }
                let key = row_keys.row(i).owned();
                match self.counts.entry(key) {
                    Entry::Occupied(mut o) => {
                        *o.get_mut() += cnt;
                        if *o.get() == 0 {
                            o.remove();
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(cnt);
                    }
                }
            }
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array};

    fn i64_arr(vals: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(vals.to_vec()))
    }

    fn f64_arr(vals: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(vals.to_vec()))
    }

    fn bool_arr(vals: &[bool]) -> ArrayRef {
        Arc::new(BooleanArray::from(vals.to_vec()))
    }

    fn nullable_i64_arr(vals: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(vals.to_vec()))
    }

    #[test]
    fn sum_basic_insert_retract() {
        let mut acc = RetractableSumAccum::new(DataType::Int64);
        // Insert: 10 (+1), 20 (+1)
        acc.update_batch(&[i64_arr(&[10, 20]), i64_arr(&[1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(30)));

        // Retract 10 (-1), insert 30 (+1)
        acc.update_batch(&[i64_arr(&[10, 30]), i64_arr(&[-1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(50)));
    }

    #[test]
    fn sum_checkpoint_roundtrip() {
        let mut acc = RetractableSumAccum::new(DataType::Int64);
        acc.update_batch(&[i64_arr(&[10, 20]), i64_arr(&[1, 1])])
            .unwrap();

        let state = acc.state().unwrap();
        let mut restored = RetractableSumAccum::new(DataType::Int64);
        let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
        restored.merge_batch(&arrays).unwrap();
        assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(30)));
    }

    /// Int64 SUM of values whose total exceeds 2^53 must be exact —
    /// the pre-`i128` `f64`-backed implementation silently lost precision.
    #[test]
    fn sum_int64_above_f64_precision_limit() {
        use arrow::array::Int64Array;
        let mut acc = RetractableSumAccum::new(DataType::Int64);
        // Two values that sum to 2^54 + 1 — exactly representable in i64
        // but not in f64 (the `+1` would be lost to rounding).
        let vals = Int64Array::from(vec![1_i64 << 53, (1_i64 << 53) + 1]);
        let weights = Int64Array::from(vec![1_i64, 1]);
        acc.update_batch(&[Arc::new(vals), Arc::new(weights)])
            .unwrap();
        let got = acc.evaluate().unwrap();
        let expected = (1_i64 << 54).wrapping_add(1);
        assert_eq!(got, ScalarValue::Int64(Some(expected)));
    }

    #[test]
    fn count_star_basic() {
        let mut acc = RetractableCountStarAccum::default();
        // 3 inserts
        acc.update_batch(&[bool_arr(&[true, true, true]), i64_arr(&[1, 1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(3)));

        // Retract 1
        acc.update_batch(&[bool_arr(&[true]), i64_arr(&[-1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn count_col_skips_nulls() {
        let mut acc = RetractableCountAccum::default();
        // 3 rows: value present, NULL, value present
        acc.update_batch(&[
            nullable_i64_arr(&[Some(10), None, Some(30)]),
            i64_arr(&[1, 1, 1]),
        ])
        .unwrap();
        // Only 2 non-null rows counted
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn count_col_retract() {
        let mut acc = RetractableCountAccum::default();
        acc.update_batch(&[i64_arr(&[10, 20, 30]), i64_arr(&[1, 1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(3)));

        acc.update_batch(&[i64_arr(&[10]), i64_arr(&[-1])]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn avg_basic() {
        let mut acc = RetractableAvgAccum::default();
        // Insert 10, 20, 30
        acc.update_batch(&[i64_arr(&[10, 20, 30]), i64_arr(&[1, 1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(20.0)));
    }

    #[test]
    fn avg_retract() {
        let mut acc = RetractableAvgAccum::default();
        // Insert 10, 20, 30 -> avg = 20
        acc.update_batch(&[i64_arr(&[10, 20, 30]), i64_arr(&[1, 1, 1])])
            .unwrap();
        // Retract 10 -> {20, 30} -> avg = 25
        acc.update_batch(&[i64_arr(&[10]), i64_arr(&[-1])]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(Some(25.0)));
    }

    #[test]
    fn min_retract_current_min() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Min);
        acc.update_batch(&[i64_arr(&[10, 20, 30]), i64_arr(&[1, 1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(10)));

        acc.update_batch(&[i64_arr(&[10]), i64_arr(&[-1])]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(20)));
    }

    #[test]
    fn min_retract_all() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Min);
        acc.update_batch(&[i64_arr(&[10, 20]), i64_arr(&[1, 1])])
            .unwrap();
        acc.update_batch(&[i64_arr(&[10, 20]), i64_arr(&[-1, -1])])
            .unwrap();
        assert!(acc.evaluate().unwrap().is_null());
    }

    #[test]
    fn min_duplicate_values() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Min);
        acc.update_batch(&[i64_arr(&[10, 10, 20]), i64_arr(&[1, 1, 1])])
            .unwrap();
        acc.update_batch(&[i64_arr(&[10]), i64_arr(&[-1])]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(10)));

        acc.update_batch(&[i64_arr(&[10]), i64_arr(&[-1])]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(20)));
    }

    #[test]
    fn min_checkpoint_roundtrip() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Min);
        acc.update_batch(&[i64_arr(&[30, 10, 20]), i64_arr(&[1, 1, 1])])
            .unwrap();

        let state = acc.state().unwrap();
        let mut restored = RetractableExtremumAccum::new(DataType::Int64, Extremum::Min);
        let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
        restored.merge_batch(&arrays).unwrap();
        assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(10)));
    }

    #[test]
    fn max_retract_current_max() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Max);
        acc.update_batch(&[i64_arr(&[10, 20, 30]), i64_arr(&[1, 1, 1])])
            .unwrap();
        acc.update_batch(&[i64_arr(&[30]), i64_arr(&[-1])]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(20)));
    }

    /// SUM/AVG/MIN/MAX all return NULL before any insert lands.
    #[test]
    fn empty_evaluates_to_null() {
        assert!(RetractableSumAccum::new(DataType::Int64)
            .evaluate()
            .unwrap()
            .is_null());
        assert!(RetractableAvgAccum::default().evaluate().unwrap().is_null());
        assert!(
            RetractableExtremumAccum::new(DataType::Int64, Extremum::Min)
                .evaluate()
                .unwrap()
                .is_null()
        );
        assert!(
            RetractableExtremumAccum::new(DataType::Int64, Extremum::Max)
                .evaluate()
                .unwrap()
                .is_null()
        );
    }

    #[test]
    fn min_max_float64() {
        let mut min_acc = RetractableExtremumAccum::new(DataType::Float64, Extremum::Min);
        let mut max_acc = RetractableExtremumAccum::new(DataType::Float64, Extremum::Max);

        let vals = f64_arr(&[3.25, 1.41, 2.72]);
        let wts = i64_arr(&[1, 1, 1]);

        min_acc.update_batch(&[vals.clone(), wts.clone()]).unwrap();
        max_acc.update_batch(&[vals, wts]).unwrap();

        assert_eq!(
            min_acc.evaluate().unwrap(),
            ScalarValue::Float64(Some(1.41))
        );
        assert_eq!(
            max_acc.evaluate().unwrap(),
            ScalarValue::Float64(Some(3.25))
        );
    }

    /// MIN/MAX over a `Utf8` column. The old `f64`-only implementation
    /// silently emitted NULL (because `to_f64` returns `None` for
    /// strings); the row-converter implementation handles it natively.
    #[test]
    fn min_max_utf8() {
        use arrow::array::StringArray;
        let mut min_acc = RetractableExtremumAccum::new(DataType::Utf8, Extremum::Min);
        let mut max_acc = RetractableExtremumAccum::new(DataType::Utf8, Extremum::Max);
        let vals: ArrayRef = Arc::new(StringArray::from(vec!["banana", "apple", "cherry"]));
        min_acc
            .update_batch(&[Arc::clone(&vals), i64_arr(&[1, 1, 1])])
            .unwrap();
        max_acc.update_batch(&[vals, i64_arr(&[1, 1, 1])]).unwrap();
        assert_eq!(
            min_acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("apple".into()))
        );
        assert_eq!(
            max_acc.evaluate().unwrap(),
            ScalarValue::Utf8(Some("cherry".into()))
        );
    }

    #[test]
    fn factory_supported_functions() {
        assert!(create_retractable("sum", &DataType::Int64, false).is_ok());
        assert!(create_retractable("count", &DataType::Int64, true).is_ok());
        assert!(create_retractable("count", &DataType::Int64, false).is_ok());
        assert!(create_retractable("avg", &DataType::Float64, false).is_ok());
        assert!(create_retractable("min", &DataType::Int64, false).is_ok());
        assert!(create_retractable("max", &DataType::Int64, false).is_ok());
    }

    #[test]
    fn factory_unsupported_function() {
        assert!(create_retractable("stddev", &DataType::Float64, false).is_err());
    }
}
