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
mod tests;
