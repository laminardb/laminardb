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

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;

use crate::error::DbError;

// ── Helpers ─────────────────────────────────────────────────────────

/// Extract `i64` weights from the last array in the inputs slice.
fn weight_array(values: &[ArrayRef]) -> &arrow::array::Int64Array {
    values
        .last()
        .expect("retractable accumulator requires weight as last input")
        .as_primitive::<arrow::datatypes::Int64Type>()
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

/// Encode an `f64` as `i64` for deterministic `BTreeMap` ordering.
///
/// Uses IEEE 754 total-order encoding: negative floats get all bits flipped,
/// positive floats get the sign bit set. A final XOR shifts from u64 ordering
/// to i64 ordering so `BTreeMap<i64, _>` iteration matches numeric order.
#[allow(clippy::cast_possible_wrap)]
fn f64_to_sortable_i64(v: f64) -> i64 {
    let bits = v.to_bits();
    let sortable = if (bits >> 63) != 0 {
        !bits // negative: flip all bits (reverses order)
    } else {
        bits | (1_u64 << 63) // positive: set sign bit (places after negatives)
    };
    // Shift from u64 ordering to i64 ordering.
    (sortable ^ (1_u64 << 63)) as i64
}

/// Decode the sortable `i64` back to `f64`.
fn sortable_i64_to_f64(encoded: i64) -> f64 {
    #[allow(clippy::cast_sign_loss)]
    let sortable = (encoded as u64) ^ (1_u64 << 63);
    let bits = if (sortable >> 63) != 0 {
        sortable & !(1_u64 << 63) // was positive: clear the sign bit we set
    } else {
        !sortable // was negative: undo the flip
    };
    f64::from_bits(bits)
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

// ── Factory ─────────────────────────────────────────────────────────

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

/// Retractable SUM: `SUM(value * weight)`.
#[derive(Debug)]
struct RetractableSumAccum {
    sum: f64,
    count: i64,
    return_type: DataType,
}

impl RetractableSumAccum {
    fn new(return_type: DataType) -> Self {
        Self {
            sum: 0.0,
            count: 0,
            return_type,
        }
    }
}

impl Accumulator for RetractableSumAccum {
    /// Inputs: `[value_col, weight_col]`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let weights = weight_array(values);
        let value_arr = &values[0];
        for row in 0..value_arr.len() {
            if let Some(v) = to_f64(value_arr, row) {
                let w = weights.value(row);
                #[allow(clippy::cast_precision_loss)]
                {
                    self.sum += v * w as f64;
                }
                self.count += w;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        if self.count == 0 {
            return ScalarValue::try_from(&self.return_type);
        }
        Ok(f64_to_scalar(self.sum, &self.return_type))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.sum)),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        let sums = states[0].as_primitive::<arrow::datatypes::Float64Type>();
        let counts = states[1].as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..sums.len() {
            if !sums.is_null(row) {
                self.sum += sums.value(row);
            }
            if !counts.is_null(row) {
                self.count += counts.value(row);
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
        let weights = weight_array(values);
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
        let weights = weight_array(values);
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
        let weights = weight_array(values);
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

/// Retractable MIN/MAX using a counted multiset (`BTreeMap<sortable_bits, count>`).
///
/// On insert (weight > 0): increment count for value.
/// On retract (weight < 0): decrement count; remove entry when zero.
/// Result: first key (MIN) or last key (MAX).
#[derive(Debug)]
struct RetractableExtremumAccum {
    /// `value_bits` (sortable encoding) -> net count
    counts: BTreeMap<i64, i64>,
    return_type: DataType,
    direction: Extremum,
}

impl RetractableExtremumAccum {
    fn new(return_type: DataType, direction: Extremum) -> Self {
        Self {
            counts: BTreeMap::new(),
            return_type,
            direction,
        }
    }
}

impl Accumulator for RetractableExtremumAccum {
    /// Inputs: `[value_col, weight_col]`.
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let weights = weight_array(values);
        let value_arr = &values[0];
        for row in 0..value_arr.len() {
            if let Some(v) = to_f64(value_arr, row) {
                let bits = f64_to_sortable_i64(v);
                let w = weights.value(row);
                let entry = self.counts.entry(bits).or_insert(0);
                *entry += w;
                if *entry == 0 {
                    self.counts.remove(&bits);
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        let pair = match self.direction {
            Extremum::Min => self.counts.first_key_value(),
            Extremum::Max => self.counts.last_key_value(),
        };
        match pair {
            Some((&bits, _)) => {
                let v = sortable_i64_to_f64(bits);
                Ok(f64_to_scalar(v, &self.return_type))
            }
            None => ScalarValue::try_from(&self.return_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.counts.len() * (std::mem::size_of::<i64>() * 2 + 64)
        // BTreeMap node overhead
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let keys: Vec<ScalarValue> = self
            .counts
            .keys()
            .map(|k| ScalarValue::Int64(Some(*k)))
            .collect();
        let vals: Vec<ScalarValue> = self
            .counts
            .values()
            .map(|c| ScalarValue::Int64(Some(*c)))
            .collect();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list(&keys, &DataType::Int64, true)),
            ScalarValue::List(ScalarValue::new_list(&vals, &DataType::Int64, true)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        let keys_list = states[0].as_list::<i32>();
        let vals_list = states[1].as_list::<i32>();
        for row in 0..keys_list.len() {
            let keys = keys_list.value(row);
            let vals = vals_list.value(row);
            let k_arr = keys.as_primitive::<arrow::datatypes::Int64Type>();
            let v_arr = vals.as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..k_arr.len() {
                let bits = k_arr.value(i);
                let cnt = v_arr.value(i);
                let entry = self.counts.entry(bits).or_insert(0);
                *entry += cnt;
                if *entry == 0 {
                    self.counts.remove(&bits);
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

    // ── SUM ─────────────────────────────────────────────────────

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

    #[test]
    fn sum_empty_returns_null() {
        let mut acc = RetractableSumAccum::new(DataType::Int64);
        let result = acc.evaluate().unwrap();
        assert!(result.is_null());
    }

    // ── COUNT(*) ────────────────────────────────────────────────

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
    fn count_star_checkpoint() {
        let mut acc = RetractableCountStarAccum::default();
        acc.update_batch(&[bool_arr(&[true, true]), i64_arr(&[1, 1])])
            .unwrap();
        let state = acc.state().unwrap();
        let mut restored = RetractableCountStarAccum::default();
        let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
        restored.merge_batch(&arrays).unwrap();
        assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    }

    // ── COUNT(col) ──────────────────────────────────────────────

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

    // ── AVG ─────────────────────────────────────────────────────

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
    fn avg_empty_returns_null() {
        let mut acc = RetractableAvgAccum::default();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(None));
    }

    #[test]
    fn avg_checkpoint() {
        let mut acc = RetractableAvgAccum::default();
        acc.update_batch(&[i64_arr(&[10, 20, 30]), i64_arr(&[1, 1, 1])])
            .unwrap();
        let state = acc.state().unwrap();
        let mut restored = RetractableAvgAccum::default();
        let arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array().unwrap()).collect();
        restored.merge_batch(&arrays).unwrap();
        assert_eq!(
            restored.evaluate().unwrap(),
            ScalarValue::Float64(Some(20.0))
        );
    }

    // ── MIN ─────────────────────────────────────────────────────

    #[test]
    fn min_basic() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Min);
        acc.update_batch(&[i64_arr(&[30, 10, 20]), i64_arr(&[1, 1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(10)));
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
    fn min_empty_returns_null() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Min);
        assert!(acc.evaluate().unwrap().is_null());
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

    // ── MAX ─────────────────────────────────────────────────────

    #[test]
    fn max_basic() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Max);
        acc.update_batch(&[i64_arr(&[10, 30, 20]), i64_arr(&[1, 1, 1])])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(30)));
    }

    #[test]
    fn max_retract_current_max() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Max);
        acc.update_batch(&[i64_arr(&[10, 20, 30]), i64_arr(&[1, 1, 1])])
            .unwrap();
        acc.update_batch(&[i64_arr(&[30]), i64_arr(&[-1])]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(20)));
    }

    #[test]
    fn max_empty_returns_null() {
        let mut acc = RetractableExtremumAccum::new(DataType::Int64, Extremum::Max);
        assert!(acc.evaluate().unwrap().is_null());
    }

    // ── Float support ───────────────────────────────────────────

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

    // ── Sortable encoding ───────────────────────────────────────

    #[test]
    fn sortable_encoding_roundtrip() {
        let values = [-100.0, -1.0, -0.0, 0.0, 1.0, 100.0, f64::INFINITY];
        for v in values {
            let bits = f64_to_sortable_i64(v);
            let back = sortable_i64_to_f64(bits);
            assert_eq!(v.to_bits(), back.to_bits(), "roundtrip failed for {v}");
        }
    }

    #[test]
    fn sortable_encoding_preserves_order() {
        let values = [-100.0, -1.0, 0.0, 1.0, 100.0];
        let encoded: Vec<i64> = values.iter().map(|v| f64_to_sortable_i64(*v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "order not preserved: {} ({}) >= {} ({})",
                values[i],
                encoded[i],
                values[i + 1],
                encoded[i + 1]
            );
        }
    }

    // ── Mixed aggregates (factory) ──────────────────────────────

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
