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

    /// Full document replacement. Fails if the document is absent
    /// unless `upsert_on_missing` is `true`.
    Replace {
        /// If `true`, insert the document when no match is found.
        upsert_on_missing: bool,
    },

    /// Routes operations based on the incoming event's `operationType`.
    ///
    /// Only valid for `LaminarEvent<MongoDbChangeEvent>` (CDC fan-out
    /// replication). Maps operations as follows:
    ///
    /// - `insert` → `insertOne`
    /// - `update` → `updateOne` using `$set`/`$unset` from `updateDescription`
    /// - `replace` → `replaceOne` with `upsert: true`
    /// - `delete` → `deleteOne` using `documentKey._id`
    /// - `drop`/`rename`/`invalidate` → lifecycle events, no write issued
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
mod tests {
    use super::*;

    #[test]
    fn test_write_mode_default() {
        assert!(matches!(WriteMode::default(), WriteMode::Insert));
    }

    #[test]
    fn test_validate_timeseries_insert_ok() {
        validate_timeseries_write_mode(&WriteMode::Insert).unwrap();
    }

    #[test]
    fn test_validate_timeseries_upsert_fails() {
        let mode = WriteMode::Upsert {
            key_fields: vec!["id".to_string()],
        };
        let err = validate_timeseries_write_mode(&mode).unwrap_err();
        assert!(err.to_string().contains("time series"));
    }

    #[test]
    fn test_validate_timeseries_cdc_replay_fails() {
        let err = validate_timeseries_write_mode(&WriteMode::CdcReplay).unwrap_err();
        assert!(err.to_string().contains("time series"));
    }
}
