//! Upsert-envelope changelog projection and fail-before-enqueue key validation.

use arrow_array::StringArray;

use super::{KafkaSink, KeyBuffer};
use crate::changelog::collapse_changelog;
use crate::error::ConnectorError;

pub(super) fn project_upsert_values(
    collapsed: &arrow_array::RecordBatch,
) -> Result<(arrow_array::RecordBatch, StringArray), ConnectorError> {
    let operation_index = collapsed
        .schema()
        .index_of("_op")
        .map_err(|_| ConnectorError::Internal("collapsed changelog missing _op".into()))?;
    let operations = collapsed
        .column(operation_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ConnectorError::Internal("_op column is not Utf8".into()))?
        .clone();
    let value_columns = (0..collapsed.num_columns())
        .filter(|index| *index != operation_index)
        .collect::<Vec<_>>();
    let values = collapsed
        .project(&value_columns)
        .map_err(|error| ConnectorError::Internal(format!("project value columns: {error}")))?;
    Ok((values, operations))
}

pub(super) fn validate_upsert_keys(keys: &KeyBuffer, rows: usize) -> Result<(), ConnectorError> {
    if keys.len() != rows {
        return Err(ConnectorError::Internal(format!(
            "upsert envelope: {} keys for {rows} rows",
            keys.len()
        )));
    }
    if let Some(row) = (0..rows).find(|row| keys.key(*row).is_none()) {
        return Err(ConnectorError::WriteError(format!(
            "upsert envelope: row {row} has a NULL merge key; a compacted topic needs a key \
             (use key.format = 'json' to encode nullable key columns)"
        )));
    }
    Ok(())
}

impl KafkaSink {
    pub(super) fn collapse_upsert_changelog(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<arrow_array::RecordBatch, ConnectorError> {
        let key_columns = self.config.key_columns();
        if key_columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "envelope = 'upsert' requires 'key.column'".into(),
            ));
        }
        collapse_changelog(batch, &key_columns)
    }
}
