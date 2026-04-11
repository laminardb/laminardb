//! Timestamp-column row filtering — shared by late-row filtering,
//! EOWC closed-window filtering, and the `DataFusion` watermark pushdown.

use arrow::array::{RecordBatch, TimestampMillisecondArray};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp;
use arrow::datatypes::DataType;

use super::cast::cast_to_millis_array;

/// Direction of timestamp threshold comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThresholdOp {
    /// Keep rows where `ts >= threshold` (late-row filtering).
    GreaterEq,
    /// Keep rows where `ts < threshold` (closed-window filtering).
    Less,
}

/// Reasons `filter_batch_by_timestamp` cannot filter a batch.
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    /// Named column does not exist in the batch's schema.
    #[error("timestamp column '{0}' not found in batch schema")]
    ColumnNotFound(String),
    /// Column exists but isn't a `Timestamp(_)` type (schema drift).
    #[error("column '{column}' is not a Timestamp type, found {found}")]
    IncompatibleType {
        /// Column name.
        column: String,
        /// Actual Arrow data type.
        found: String,
    },
}

/// Filter a `RecordBatch` by comparing a `Timestamp(_)` column against
/// a millisecond threshold. Returns `Ok(None)` when no rows survive.
///
/// # Errors
///
/// Errors if `column` is missing or isn't a `Timestamp(_)` — both are
/// schema drift and should surface loudly rather than leak rows.
pub fn filter_batch_by_timestamp(
    batch: &RecordBatch,
    column: &str,
    threshold_ms: i64,
    op: ThresholdOp,
) -> Result<Option<RecordBatch>, FilterError> {
    let idx = batch
        .schema()
        .index_of(column)
        .map_err(|_| FilterError::ColumnNotFound(column.to_string()))?;
    let col = batch.column(idx);

    if !matches!(col.data_type(), DataType::Timestamp(_, _)) {
        return Err(FilterError::IncompatibleType {
            column: column.to_string(),
            found: format!("{:?}", col.data_type()),
        });
    }

    // cast_to_millis_array is zero-copy for already-millis columns and
    // uses Arrow's SIMD cast kernel for other precisions. The mask compare
    // below then runs on one representation no matter the source unit.
    let ms = cast_to_millis_array(col.as_ref()).map_err(|e| FilterError::IncompatibleType {
        column: column.to_string(),
        found: e.to_string(),
    })?;
    let threshold = TimestampMillisecondArray::new_scalar(threshold_ms);
    let mask = match op {
        ThresholdOp::GreaterEq => cmp::gt_eq(&ms, &threshold),
        ThresholdOp::Less => cmp::lt(&ms, &threshold),
    }
    .map_err(|e| FilterError::IncompatibleType {
        column: column.to_string(),
        found: e.to_string(),
    })?;

    let filtered =
        filter_record_batch(batch, &mask).map_err(|e| FilterError::IncompatibleType {
            column: column.to_string(),
            found: e.to_string(),
        })?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
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
        let result = filter_batch_by_timestamp(&batch, "ts", 250, ThresholdOp::GreaterEq)
            .unwrap()
            .unwrap();
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
        let result = filter_batch_by_timestamp(&batch, "ts", 250, ThresholdOp::Less)
            .unwrap()
            .unwrap();
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
    fn test_missing_column_errors() {
        let batch = make_millis_batch(vec![100, 200]);
        let err = filter_batch_by_timestamp(&batch, "nonexistent", 150, ThresholdOp::GreaterEq)
            .expect_err("missing column should error");
        assert!(matches!(err, FilterError::ColumnNotFound(_)));
    }

    #[test]
    fn test_non_timestamp_column_errors() {
        use arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let err = filter_batch_by_timestamp(&batch, "ts", 150, ThresholdOp::GreaterEq)
            .expect_err("non-timestamp column should error");
        assert!(matches!(err, FilterError::IncompatibleType { .. }));
    }

    #[test]
    fn test_nanosecond_rescaled_to_watermark() {
        // Watermark 250 ms → threshold 250_000_000 ns.
        let batch = make_nanos_batch(vec![100_000_000, 200_000_000, 300_000_000, 400_000_000]);
        let result = filter_batch_by_timestamp(&batch, "ts", 250, ThresholdOp::GreaterEq)
            .unwrap()
            .unwrap();
        let ts = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(ts.value(0), 300_000_000);
    }

    /// Seconds-precision column + sub-second threshold. `ts = 1` (1000 ms)
    /// is late vs. `threshold_ms = 1500`, so `GreaterEq` drops it and
    /// `Less` keeps it.
    #[test]
    fn test_seconds_precision_sub_second_threshold() {
        let batch = make_seconds_batch(vec![1, 2]);

        let kept = filter_batch_by_timestamp(&batch, "ts", 1500, ThresholdOp::GreaterEq)
            .unwrap()
            .unwrap();
        let ts = kept
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!(kept.num_rows(), 1);
        assert_eq!(ts.value(0), 2);

        let kept = filter_batch_by_timestamp(&batch, "ts", 1500, ThresholdOp::Less)
            .unwrap()
            .unwrap();
        let ts = kept
            .column(0)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!(kept.num_rows(), 1);
        assert_eq!(ts.value(0), 1);
    }
}
