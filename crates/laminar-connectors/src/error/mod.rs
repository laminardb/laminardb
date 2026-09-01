use thiserror::Error;

/// Errors that can occur during connector operations.
///
/// Callers that need to distinguish "retry may work" from "propagate"
/// should use [`ConnectorError::is_transient`] rather than matching
/// variants directly — the variant set has changed in the past.
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// A connector factory was registered more than once in the same registry category.
    #[error("{kind} connector factory '{name}' is already registered")]
    FactoryAlreadyRegistered {
        /// Registry category (`source`, `sink`, `table source`, or `lookup source`).
        kind: &'static str,
        /// Duplicate connector type name.
        name: String,
    },

    /// Connector factory registration was attempted after construction completed.
    #[error("connector registry is frozen; cannot register {kind} factory '{name}'")]
    RegistryFrozen {
        /// Registry category (`source`, `sink`, `table source`, or `lookup source`).
        kind: &'static str,
        /// Connector type name that was rejected.
        name: String,
    },

    /// Failed to connect to the external system (network error, DNS
    /// failure, TLS negotiation failure, auth rejection).
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// Invalid, missing, or contradictory connector configuration.
    #[error("configuration error: {0}")]
    ConfigurationError(String),

    /// A requested connector capability is intentionally unavailable.
    #[error("feature unsupported: {0}")]
    FeatureUnsupported(String),

    /// Error reading data from a source.
    #[error("read error: {0}")]
    ReadError(String),

    /// Error writing data to a sink.
    #[error("write error: {0}")]
    WriteError(String),

    /// A dispatched operation failed without proving whether its external
    /// side effects were applied.
    ///
    /// Recovery may replay the operation, but the connector generation that
    /// observed this error must not process later work because its external
    /// position is no longer known.
    #[error("operation outcome unknown: {message}")]
    OutcomeUnknown {
        /// Failure detail.
        message: String,
        /// Whether a fresh connector generation may make progress after reconciliation.
        retryable: bool,
    },

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
    /// Construct an error for a dispatched operation whose external outcome is unknown.
    #[must_use]
    pub fn outcome_unknown(message: impl Into<String>, retryable: bool) -> Self {
        Self::OutcomeUnknown {
            message: message.into(),
            retryable,
        }
    }

    /// Construct a "missing required config" error. Thin helper around
    /// [`Self::ConfigurationError`] so every "missing required config:
    /// {key}" message is shaped the same way.
    #[must_use]
    pub fn missing_config(key: impl Into<String>) -> Self {
        Self::ConfigurationError(format!("missing required config: {}", key.into()))
    }

    /// Returns `true` if this error is likely transient and recovery or a
    /// retry may make progress (e.g., network timeout, throttled request).
    /// An outcome-unknown error still requires connector retirement before
    /// replay; inspect [`Self::is_outcome_unknown`] when that distinction
    /// matters.
    /// Returns `false` for configuration, schema, and state errors that
    /// will not resolve without user intervention.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::OutcomeUnknown { retryable, .. } => *retryable,
            Self::ReadError(_)
            | Self::WriteError(_)
            | Self::Timeout(_)
            | Self::Io(_)
            | Self::ConnectionFailed(_) => true,

            Self::ConfigurationError(_)
            | Self::FeatureUnsupported(_)
            | Self::FactoryAlreadyRegistered { .. }
            | Self::RegistryFrozen { .. }
            | Self::SchemaMismatch(_)
            | Self::InvalidState { .. }
            | Self::TransactionError(_)
            | Self::Serde(_)
            | Self::Closed
            | Self::Internal(_) => false,
        }
    }

    /// Returns `true` when an external side effect may have completed even
    /// though the operation returned an error.
    ///
    /// This is stronger than [`Self::is_transient`]: callers may recover and
    /// replay, but must retire the connector generation before doing so.
    #[must_use]
    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
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
mod tests;
