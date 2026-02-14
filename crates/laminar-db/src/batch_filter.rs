//! Shared timestamp-based batch filtering utility.
//!
//! Provides a generic `filter_batch_by_timestamp` function used by both
//! late-row filtering (keep rows >= watermark) and EOWC closed-window
//! filtering (keep rows < boundary).

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use laminar_core::time::TimestampFormat;

/// Direction of timestamp threshold comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThresholdOp {
    /// Keep rows where ts >= threshold (late-row filtering: keep on-time rows).
    GreaterEq,
    /// Keep rows where ts < threshold (EOWC: keep rows in closed windows).
    Less,
}

/// Filter a `RecordBatch` by comparing a timestamp column against a threshold.
///
/// Handles Int64 (millis/seconds/micros/nanos), Arrow Timestamp (all `TimeUnit`s),
/// and Iso8601 (pass-through). Returns `None` if the filtered result is empty.
#[allow(clippy::too_many_lines)]
pub(crate) fn filter_batch_by_timestamp(
    batch: &RecordBatch,
    column: &str,
    threshold_ms: i64,
    format: TimestampFormat,
    op: ThresholdOp,
) -> Option<RecordBatch> {
    use arrow::array::{Array, BooleanArray, Int64Array};
    use arrow::compute::filter_record_batch;
    use arrow::datatypes::TimeUnit;

    let Ok(idx) = batch.schema().index_of(column) else {
        return Some(batch.clone());
    };

    let col = batch.column(idx);
    let num_rows = batch.num_rows();

    let cmp: fn(i64, i64) -> bool = match op {
        ThresholdOp::GreaterEq => |val, thr| val >= thr,
        ThresholdOp::Less => |val, thr| val < thr,
    };

    // Helper closure to build the boolean mask for an Int64 column
    let i64_mask = |arr: &Int64Array, threshold: i64| -> BooleanArray {
        (0..num_rows)
            .map(|i| {
                if arr.is_null(i) {
                    Some(false)
                } else {
                    Some(cmp(arr.value(i), threshold))
                }
            })
            .collect()
    };

    let mask: BooleanArray = match format {
        TimestampFormat::UnixMillis => {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                i64_mask(arr, threshold_ms)
            } else {
                return Some(batch.clone());
            }
        }
        TimestampFormat::ArrowNative => match col.data_type() {
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()?;
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            Some(false)
                        } else {
                            Some(cmp(arr.value(i), threshold_ms))
                        }
                    })
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()?;
                let thr_secs = threshold_ms / 1000;
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            Some(false)
                        } else {
                            Some(cmp(arr.value(i), thr_secs))
                        }
                    })
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()?;
                let thr_micros = threshold_ms.saturating_mul(1000);
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            Some(false)
                        } else {
                            Some(cmp(arr.value(i), thr_micros))
                        }
                    })
                    .collect()
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()?;
                let thr_nanos = threshold_ms.saturating_mul(1_000_000);
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            Some(false)
                        } else {
                            Some(cmp(arr.value(i), thr_nanos))
                        }
                    })
                    .collect()
            }
            _ => return Some(batch.clone()),
        },
        TimestampFormat::UnixSeconds => {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                i64_mask(arr, threshold_ms / 1000)
            } else {
                return Some(batch.clone());
            }
        }
        TimestampFormat::UnixMicros => {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                i64_mask(arr, threshold_ms.saturating_mul(1000))
            } else {
                return Some(batch.clone());
            }
        }
        TimestampFormat::UnixNanos => {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                i64_mask(arr, threshold_ms.saturating_mul(1_000_000))
            } else {
                return Some(batch.clone());
            }
        }
        // Iso8601 would require parsing each row; skip filtering for string timestamps
        TimestampFormat::Iso8601 => {
            return Some(batch.clone());
        }
    };

    let filtered = filter_record_batch(batch, &mask).ok()?;
    if filtered.num_rows() == 0 {
        None
    } else {
        Some(filtered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn make_batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let len = timestamps.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Int64Array::from(
                    (0..i64::try_from(len).expect("len fits i64")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_greater_eq_filters_late_rows() {
        let batch = make_batch(vec![100, 200, 300, 400]);
        let result = filter_batch_by_timestamp(
            &batch,
            "ts",
            250,
            TimestampFormat::UnixMillis,
            ThresholdOp::GreaterEq,
        );
        let result = result.unwrap();
        assert_eq!(result.num_rows(), 2);
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 300);
        assert_eq!(ts.value(1), 400);
    }

    #[test]
    fn test_less_filters_closed_window_rows() {
        let batch = make_batch(vec![100, 200, 300, 400]);
        let result = filter_batch_by_timestamp(
            &batch,
            "ts",
            250,
            TimestampFormat::UnixMillis,
            ThresholdOp::Less,
        );
        let result = result.unwrap();
        assert_eq!(result.num_rows(), 2);
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.value(0), 100);
        assert_eq!(ts.value(1), 200);
    }

    #[test]
    fn test_missing_column_passes_through() {
        let batch = make_batch(vec![100, 200]);
        let result = filter_batch_by_timestamp(
            &batch,
            "nonexistent",
            150,
            TimestampFormat::UnixMillis,
            ThresholdOp::GreaterEq,
        );
        assert_eq!(result.unwrap().num_rows(), 2);
    }

    #[test]
    fn test_all_filtered_returns_none() {
        let batch = make_batch(vec![100, 200]);
        let result = filter_batch_by_timestamp(
            &batch,
            "ts",
            300,
            TimestampFormat::UnixMillis,
            ThresholdOp::GreaterEq,
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_less_all_filtered_returns_none() {
        let batch = make_batch(vec![300, 400]);
        let result = filter_batch_by_timestamp(
            &batch,
            "ts",
            200,
            TimestampFormat::UnixMillis,
            ThresholdOp::Less,
        );
        assert!(result.is_none());
    }
}
