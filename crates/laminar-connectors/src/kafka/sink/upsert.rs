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

pub(super) fn validate_upsert_keys(
    keys: Option<&KeyBuffer>,
    rows: usize,
) -> Result<(), ConnectorError> {
    let Some(keys) = keys else {
        return Ok(());
    };
    for row in 0..rows {
        if keys.key(row).is_empty() {
            return Err(ConnectorError::WriteError(format!(
                "upsert envelope: row {row} has an empty/NULL merge key"
            )));
        }
    }
    Ok(())
}

impl KafkaSink {
    pub(super) fn collapse_upsert_changelog(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<arrow_array::RecordBatch, ConnectorError> {
        let key_column = self.config.key_column.as_ref().ok_or_else(|| {
            ConnectorError::ConfigurationError("envelope = 'upsert' requires 'key.column'".into())
        })?;
        collapse_changelog(batch, std::slice::from_ref(key_column))
    }
}
