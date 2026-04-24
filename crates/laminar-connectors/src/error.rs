use thiserror::Error;

/// Errors that can occur during connector operations.
///
/// Callers that need to distinguish "retry may work" from "propagate"
/// should use [`ConnectorError::is_transient`] rather than matching
/// variants directly — the variant set has changed in the past.
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// Failed to connect to the external system (network error, DNS
    /// failure, TLS negotiation failure, auth rejection).
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Invalid, missing, or contradictory connector configuration.
    #[error("configuration error: {0}")]
    ConfigurationError(String),

    /// Error reading data from a source.
    #[error("read error: {0}")]
    ReadError(String),

    /// Error writing data to a sink.
    #[error("write error: {0}")]
    WriteError(String),

    /// Serialization or deserialization error.
    #[error("serde error: {0}")]
    Serde(#[from] SerdeError),

    /// Transaction error (begin/commit/rollback).
    ///
    /// Kept separate from [`Self::WriteError`] because transactional
    /// failures are classified as **non-transient** by default; a write
    /// error is transient. Per-connector retry policy can override, but
    /// the default must not loop forever on bad transactional state.
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

impl From<laminar_core::lookup::source::LookupError> for ConnectorError {
    fn from(err: laminar_core::lookup::source::LookupError) -> Self {
        use laminar_core::lookup::source::LookupError;
        match err {
            LookupError::Connection(m) => Self::ConnectionFailed(m),
            LookupError::Query(m) => Self::ReadError(m),
            LookupError::Timeout(d) =>
            {
                #[allow(clippy::cast_possible_truncation)]
                Self::Timeout(d.as_millis() as u64)
            }
            LookupError::NotAvailable(m) => Self::InvalidState {
                expected: "lookup source available".into(),
                actual: m,
            },
            LookupError::Internal(m) => Self::Internal(m),
        }
    }
}

impl ConnectorError {
    /// Construct a "missing required config" error. Thin helper around
    /// [`Self::ConfigurationError`] so every "missing required config:
    /// {key}" message is shaped the same way.
    #[must_use]
    pub fn missing_config(key: impl Into<String>) -> Self {
        Self::ConfigurationError(format!("missing required config: {}", key.into()))
    }

    /// Returns `true` if this error is likely transient and the operation
    /// may succeed on retry (e.g., network timeout, throttled request).
    /// Returns `false` for configuration, schema, and state errors that
    /// will not resolve without user intervention.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::ReadError(_)
            | Self::WriteError(_)
            | Self::Timeout(_)
            | Self::Io(_)
            | Self::ConnectionFailed(_) => true,

            Self::ConfigurationError(_)
            | Self::SchemaMismatch(_)
            | Self::InvalidState { .. }
            | Self::TransactionError(_)
            | Self::Serde(_)
            | Self::Closed
            | Self::Internal(_) => false,
        }
    }
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
