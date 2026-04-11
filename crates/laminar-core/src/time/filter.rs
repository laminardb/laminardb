//! Timestamp-column row filtering — shared by late-row filtering,
//! EOWC closed-window filtering, and the `DataFusion` watermark
//! pushdown. The millisecond threshold is rescaled to the column's
//! unit so the SIMD compare stays exact.

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp;
use arrow::datatypes::{DataType, TimeUnit};

/// Direction of timestamp threshold comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThresholdOp {
    /// Keep rows where `ts >= threshold` (late-row filtering).
    GreaterEq,
    /// Keep rows where `ts < threshold` (closed-window filtering).
    Less,
}

/// Filter a `RecordBatch` by comparing a `Timestamp(_)` column against
/// a millisecond threshold.
///
/// Returns `None` when the filtered result is empty, and returns the
/// batch unchanged when `column` is missing or isn't a `Timestamp(_)`
/// type — both are best-effort opt-outs so callers can fall through
/// rather than propagate a hard error from a filter step.
#[must_use]
pub fn filter_batch_by_timestamp(
    batch: &RecordBatch,
    column: &str,
    threshold_ms: i64,
    op: ThresholdOp,
) -> Option<RecordBatch> {
    let Ok(idx) = batch.schema().index_of(column) else {
        return Some(batch.clone());
    };
    let col = batch.column(idx);

    // Arrow SIMD-accelerated comparison. Null values propagate as null
    // in the boolean mask, and `filter_record_batch` treats null mask
    // entries as false (row excluded).
    macro_rules! cmp_scalar {
        ($arr:expr, $scalar:expr) => {
            match op {
                ThresholdOp::GreaterEq => cmp::gt_eq($arr, &$scalar).ok()?,
                ThresholdOp::Less => cmp::lt($arr, &$scalar).ok()?,
            }
        };
    }

    let mask: BooleanArray = match col.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()?;
            cmp_scalar!(
                arr,
                arrow::array::TimestampMillisecondArray::new_scalar(threshold_ms)
            )
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()?;
            // Round up: flooring would admit late rows for GreaterEq
            // and drop in-window rows for Less when the threshold isn't
            // second-aligned.
            let thr_secs = div_ceil_i64(threshold_ms, 1000);
            cmp_scalar!(
                arr,
                arrow::array::TimestampSecondArray::new_scalar(thr_secs)
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?;
            let thr_micros = threshold_ms.saturating_mul(1000);
            cmp_scalar!(
                arr,
                arrow::array::TimestampMicrosecondArray::new_scalar(thr_micros)
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()?;
            let thr_nanos = threshold_ms.saturating_mul(1_000_000);
            cmp_scalar!(
                arr,
                arrow::array::TimestampNanosecondArray::new_scalar(thr_nanos)
            )
        }
        // Non-Timestamp columns pass through — caller should have used
        // a `Timestamp(_)` event-time column.
        _ => return Some(batch.clone()),
    };

    let filtered = filter_record_batch(batch, &mask).ok()?;
    if filtered.num_rows() == 0 {
        None
    } else {
        Some(filtered)
    }
}

/// Ceiling division — `i64::div_ceil` is still behind `int_roundings`.
/// `b` must be positive; `a` may have any sign.
fn div_ceil_i64(a: i64, b: i64) -> i64 {
    let q = a.div_euclid(b);
    if a.rem_euclid(b) > 0 {
        q + 1
    } else {
        q
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_millis_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from(timestamps))],
        )
        .unwrap()
    }

    fn make_nanos_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from(timestamps))],
        )
        .unwrap()
    }

    fn make_seconds_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampSecondArray::from(timestamps))],
        )
        .unwrap()
    }

    #[test]
    fn test_greater_eq_filters_late_rows() {
        let batch = make_millis_batch(vec![100, 200, 300, 400]);
        let result = filter_batch_by_timestamp(&batch, "ts", 250, ThresholdOp::GreaterEq).unwrap();
        assert_eq!(result.num_rows(), 2);
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 300);
        assert_eq!(ts.value(1), 400);
    }

    #[test]
    fn test_less_filters_closed_window_rows() {
        let batch = make_millis_batch(vec![100, 200, 300, 400]);
        let result = filter_batch_by_timestamp(&batch, "ts", 250, ThresholdOp::Less).unwrap();
        assert_eq!(result.num_rows(), 2);
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 100);
        assert_eq!(ts.value(1), 200);
    }

    #[test]
    fn test_missing_column_passes_through() {
        let batch = make_millis_batch(vec![100, 200]);
        let result = filter_batch_by_timestamp(&batch, "nonexistent", 150, ThresholdOp::GreaterEq);
        assert_eq!(result.unwrap().num_rows(), 2);
    }

    #[test]
    fn test_nanosecond_rescaled_to_watermark() {
        // Watermark 250 ms → threshold 250_000_000 ns.
        let batch = make_nanos_batch(vec![100_000_000, 200_000_000, 300_000_000, 400_000_000]);
        let result = filter_batch_by_timestamp(&batch, "ts", 250, ThresholdOp::GreaterEq).unwrap();
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(ts.value(0), 300_000_000);
    }

    /// `threshold_ms = 1500` with a seconds-precision column: a row at
    /// `ts = 1` is 1000 ms, which is late (< 1500), so it must drop for
    /// GreaterEq and stay for Less. Flooring the threshold to 1 would
    /// break both directions.
    #[test]
    fn test_seconds_precision_rounds_up() {
        let batch = make_seconds_batch(vec![1, 2]);

        let kept = filter_batch_by_timestamp(&batch, "ts", 1500, ThresholdOp::GreaterEq).unwrap();
        let ts = kept
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!(kept.num_rows(), 1);
        assert_eq!(ts.value(0), 2);

        let kept = filter_batch_by_timestamp(&batch, "ts", 1500, ThresholdOp::Less).unwrap();
        let ts = kept
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!(kept.num_rows(), 1);
        assert_eq!(ts.value(0), 1);
    }

    #[test]
    fn test_div_ceil_i64_signs() {
        assert_eq!(div_ceil_i64(2000, 1000), 2);
        assert_eq!(div_ceil_i64(1500, 1000), 2);
        assert_eq!(div_ceil_i64(0, 1000), 0);
        assert_eq!(div_ceil_i64(-1000, 1000), -1);
        assert_eq!(div_ceil_i64(-1500, 1000), -1);
    }
}
