//! Connector SDK error types.
//!
//! Provides a unified error hierarchy for all connector operations:
//! - `ConnectorError`: Top-level error for source/sink connector operations
//! - `SerdeError`: Serialization/deserialization errors

use thiserror::Error;

/// Errors that can occur during connector operations.
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// Failed to connect to the external system.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Authentication or authorization error.
    #[error("authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Invalid connector configuration.
    #[error("configuration error: {0}")]
    ConfigurationError(String),

    /// Required configuration key is missing.
    #[error("missing required config: {0}")]
    MissingConfig(String),

    /// Error reading data from a source.
    #[error("read error: {0}")]
    ReadError(String),

    /// Error writing data to a sink.
    #[error("write error: {0}")]
    WriteError(String),

    /// Serialization or deserialization error.
    #[error("serde error: {0}")]
    Serde(#[from] SerdeError),

    /// Checkpoint or offset commit error.
    #[error("checkpoint error: {0}")]
    CheckpointError(String),

    /// Transaction error (begin/commit/rollback).
    #[error("transaction error: {0}")]
    TransactionError(String),

    /// The connector is not in the expected state.
    #[error("invalid state: expected {expected}, got {actual}")]
    InvalidState {
        /// The expected state.
        expected: String,
        /// The actual state.
        actual: String,
    },

    /// Schema mismatch between expected and actual data.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Operation timed out.
    #[error("timeout after {0}ms")]
    Timeout(u64),

    /// The connector has been closed.
    #[error("connector closed")]
    Closed,

    /// An internal error that doesn't fit other categories.
    #[error("internal error: {0}")]
    Internal(String),

    /// An I/O error from the underlying system.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors that occur during record serialization or deserialization.
#[derive(Debug, Error)]
pub enum SerdeError {
    /// JSON parsing or encoding error.
    #[error("JSON error: {0}")]
    Json(String),

    /// CSV parsing or encoding error.
    #[error("CSV error: {0}")]
    Csv(String),

    /// The data format is not supported.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    /// A required field is missing from the input.
    #[error("missing field: {0}")]
    MissingField(String),

    /// A field value could not be converted to the target Arrow type.
    #[error("type conversion error: field '{field}', expected {expected}: {message}")]
    TypeConversion {
        /// The field name.
        field: String,
        /// The expected Arrow data type.
        expected: String,
        /// Details about the conversion failure.
        message: String,
    },

    /// The input data is malformed.
    #[error("malformed input: {0}")]
    MalformedInput(String),

    /// Schema ID not found in registry.
    #[error("schema not found: schema ID {schema_id}")]
    SchemaNotFound {
        /// The schema ID that was not found.
        schema_id: i32,
    },

    /// Confluent wire format magic byte mismatch.
    #[error("invalid Confluent header: expected 0x{expected:02x}, got 0x{got:02x}")]
    InvalidConfluentHeader {
        /// Expected magic byte (0x00).
        expected: u8,
        /// Actual byte found.
        got: u8,
    },

    /// Schema incompatible with existing version in the registry.
    #[error("schema incompatible: subject '{subject}': {message}")]
    SchemaIncompatible {
        /// The Schema Registry subject name.
        subject: String,
        /// Incompatibility details.
        message: String,
    },

    /// Avro decode failure for a specific column.
    #[error("Avro decode error: column '{column}' (avro type '{avro_type}'): {message}")]
    AvroDecodeError {
        /// The column that failed to decode.
        column: String,
        /// The Avro type being decoded.
        avro_type: String,
        /// The decode failure details.
        message: String,
    },

    /// Record count mismatch after serialization.
    #[error("record count mismatch: expected {expected}, got {got}")]
    RecordCountMismatch {
        /// Expected number of records.
        expected: usize,
        /// Actual number of records produced.
        got: usize,
    },
}

impl From<serde_json::Error> for SerdeError {
    fn from(e: serde_json::Error) -> Self {
        SerdeError::Json(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_error_display() {
        let err = ConnectorError::ConnectionFailed("host unreachable".into());
        assert_eq!(err.to_string(), "connection failed: host unreachable");
    }

    #[test]
    fn test_serde_error_from_json() {
        let json_err: Result<serde_json::Value, _> = serde_json::from_str("{bad json");
        let serde_err: SerdeError = json_err.unwrap_err().into();
        assert!(matches!(serde_err, SerdeError::Json(_)));
    }

    #[test]
    fn test_serde_error_into_connector_error() {
        let serde_err = SerdeError::MissingField("timestamp".into());
        let conn_err: ConnectorError = serde_err.into();
        assert!(matches!(conn_err, ConnectorError::Serde(_)));
        assert!(conn_err.to_string().contains("timestamp"));
    }

    #[test]
    fn test_invalid_state_error() {
        let err = ConnectorError::InvalidState {
            expected: "Running".into(),
            actual: "Closed".into(),
        };
        assert!(err.to_string().contains("Running"));
        assert!(err.to_string().contains("Closed"));
    }

    #[test]
    fn test_schema_not_found_error() {
        let err = SerdeError::SchemaNotFound { schema_id: 42 };
        assert!(err.to_string().contains("42"));
        assert!(err.to_string().contains("schema not found"));
    }

    #[test]
    fn test_invalid_confluent_header_error() {
        let err = SerdeError::InvalidConfluentHeader {
            expected: 0x00,
            got: 0xFF,
        };
        let msg = err.to_string();
        assert!(msg.contains("0x00"));
        assert!(msg.contains("0xff"));
    }

    #[test]
    fn test_schema_incompatible_error() {
        let err = SerdeError::SchemaIncompatible {
            subject: "orders-value".into(),
            message: "READER_FIELD_MISSING_DEFAULT_VALUE".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("orders-value"));
        assert!(msg.contains("READER_FIELD_MISSING_DEFAULT_VALUE"));
    }

    #[test]
    fn test_avro_decode_error() {
        let err = SerdeError::AvroDecodeError {
            column: "price".into(),
            avro_type: "double".into(),
            message: "unexpected null".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("price"));
        assert!(msg.contains("double"));
        assert!(msg.contains("unexpected null"));
    }

    #[test]
    fn test_record_count_mismatch_error() {
        let err = SerdeError::RecordCountMismatch {
            expected: 5,
            got: 3,
        };
        let msg = err.to_string();
        assert!(msg.contains('5'));
        assert!(msg.contains('3'));
    }
}
