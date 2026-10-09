//! Write mode configuration for the `MongoDB` sink connector.
//!
//! Defines [`WriteMode`] which determines how incoming `RecordBatch` rows
//! are translated into `MongoDB` write operations.

use crate::error::ConnectorError;

/// Write operation mode for the `MongoDB` sink.
///
/// Determines how incoming `RecordBatch` rows are translated into
/// `MongoDB` write operations.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum WriteMode {
    /// Append-only inserts using `insertOne` / `insertMany`.
    #[default]
    Insert,

    /// Upsert by caller-supplied key fields. Uses `replaceOne` with
    /// `upsert: true`, keyed by the specified fields.
    Upsert {
        /// Fields used to match existing documents for upsert.
        key_fields: Vec<String>,
    },

    /// Applies `MongoDB` CDC history records (`output.mode=history`) in order, mirroring the
    /// source collection with its exact BSON types:
    ///
    /// - `insert`, `replace`, `snapshot` → `replaceOne(..., upsert: true)` by document key
    /// - `update` → `replaceOne` with the post-image when present, otherwise `updateOne`
    ///   from `updateDescription`
    /// - `delete` → `deleteOne` by document key
    /// - collection metadata events → no write
    /// - `drop`, `rename`, `dropDatabase`, `invalidate`, unknown → rejected; a fixed destination
    ///   never receives destructive DDL
    CdcReplay,
}

/// Validates that a write mode is compatible with time series collections.
///
/// Time series collections only accept `Insert`. Any other mode returns
/// an error.
///
/// # Errors
///
/// Returns `ConnectorError::ConfigurationError` if the mode is not `Insert`.
pub fn validate_timeseries_write_mode(mode: &WriteMode) -> Result<(), ConnectorError> {
    if matches!(mode, WriteMode::Insert) {
        Ok(())
    } else {
        Err(ConnectorError::ConfigurationError(format!(
            "time series collections only support Insert write mode, got: {mode:?}"
        )))
    }
}

#[cfg(test)]
mod tests;
