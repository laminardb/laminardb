#![deny(clippy::disallowed_types)]

//! Filters CDC `RecordBatch`es to separate positive (I, U+, U) and negative (D, U-)
//! events. Non-CDC batches (no `_op` column) pass through unchanged.

use arrow::array::{BooleanArray, RecordBatch, StringArray};
use arrow::datatypes::DataType;

use crate::error::DbError;

/// Extracts negative events (Delete, Update-Before) from a `RecordBatch`.
/// Returns `None` if no `_op` column or no negative events.
pub(crate) fn extract_negative_events(batch: &RecordBatch) -> Result<Option<RecordBatch>, DbError> {
    let Ok(op_idx) = batch.schema().index_of("_op") else {
        return Ok(None);
    };
    if !matches!(batch.schema().field(op_idx).data_type(), DataType::Utf8) {
        return Ok(None);
    }
    let op_col = batch
        .column(op_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DbError::Pipeline("_op column is not Utf8".into()))?;
    let mask: BooleanArray = op_col
        .iter()
        .map(|v| Some(v.is_some_and(|s| s == "D" || s == "U-")))
        .collect();
    let filtered = arrow::compute::filter_record_batch(batch, &mask)
        .map_err(|e| DbError::Pipeline(format!("changelog negative filter: {e}")))?;
    if filtered.num_rows() == 0 {
        Ok(None)
    } else {
        Ok(Some(filtered))
    }
}

/// Filters a `RecordBatch` to keep only positive events (Insert, Update-After).
/// If no `_op` column exists, returns the batch unchanged (append-only source).
pub(crate) fn filter_positive_events(batch: &RecordBatch) -> Result<RecordBatch, DbError> {
    let Ok(op_idx) = batch.schema().index_of("_op") else {
        return Ok(batch.clone());
    };
    if !matches!(batch.schema().field(op_idx).data_type(), DataType::Utf8) {
        return Ok(batch.clone());
    }
    let op_col = batch
        .column(op_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DbError::Pipeline("_op column is not Utf8".into()))?;
    let mask: BooleanArray = op_col
        .iter()
        .map(|v| Some(v.is_some_and(|s| s == "I" || s == "U+" || s == "U")))
        .collect();
    arrow::compute::filter_record_batch(batch, &mask)
        .map_err(|e| DbError::Pipeline(format!("changelog filter: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn cdc_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("_op", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
                Arc::new(StringArray::from(vec!["I", "D", "U+", "U-"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_filter_positive_keeps_inserts_and_updates() {
        let result = filter_positive_events(&cdc_batch()).unwrap();
        assert_eq!(result.num_rows(), 2); // I and U+
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1); // I
        assert_eq!(ids.value(1), 3); // U+
    }

    #[test]
    fn test_no_op_column_passthrough() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let result = filter_positive_events(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);
    }
}
