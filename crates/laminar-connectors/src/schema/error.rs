//! Schema error types.
//!
//! Provides [`SchemaError`] for schema inference, resolution, and evolution
//! operations, plus a convenience [`SchemaResult`] alias.

use thiserror::Error;

use crate::error::ConnectorError;

/// Result alias for schema operations.
pub type SchemaResult<T> = Result<T, SchemaError>;

/// Errors that can occur during schema operations.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// Schema inference failed (e.g., not enough samples, conflicting types).
    #[error("inference failed: {0}")]
    InferenceFailed(String),

    /// Two schemas are incompatible and cannot be merged.
    #[error("incompatible schemas: {0}")]
    Incompatible(String),

    /// Error communicating with a schema registry.
    #[error("registry error: {0}")]
    RegistryError(String),

    /// Error decoding raw data into Arrow records.
    #[error("decode error: {0}")]
    DecodeError(String),

    /// A proposed schema evolution was rejected by compatibility rules.
    #[error("evolution rejected: {0}")]
    EvolutionRejected(String),

    /// A required configuration key is missing.
    #[error("missing config: {0}")]
    MissingConfig(String),

    /// A configuration value is invalid.
    #[error("invalid config key '{key}': {message}")]
    InvalidConfig {
        /// The configuration key.
        key: String,
        /// What was wrong with the value.
        message: String,
    },

    /// An Arrow error propagated from schema operations.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Catch-all for wrapped external errors.
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl From<ConnectorError> for SchemaError {
    fn from(err: ConnectorError) -> Self {
        match err {
            ConnectorError::MissingConfig(msg) => SchemaError::MissingConfig(msg),
            ConnectorError::ConfigurationError(msg) => SchemaError::InvalidConfig {
                key: String::new(),
                message: msg,
            },
            ConnectorError::SchemaMismatch(msg) => SchemaError::Incompatible(msg),
            other => SchemaError::Other(Box::new(other)),
        }
    }
}

impl From<SchemaError> for ConnectorError {
    fn from(err: SchemaError) -> Self {
        match err {
            SchemaError::MissingConfig(msg) => ConnectorError::MissingConfig(msg),
            SchemaError::Incompatible(msg) => ConnectorError::SchemaMismatch(msg),
            SchemaError::DecodeError(msg) => ConnectorError::ReadError(msg),
            other => ConnectorError::Internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_error_display() {
        let err = SchemaError::InferenceFailed("too few samples".into());
        assert_eq!(err.to_string(), "inference failed: too few samples");
    }

    #[test]
    fn test_schema_error_invalid_config() {
        let err = SchemaError::InvalidConfig {
            key: "format".into(),
            message: "unknown format 'xml'".into(),
        };
        assert!(err.to_string().contains("format"));
        assert!(err.to_string().contains("unknown format"));
    }

    #[test]
    fn test_connector_to_schema_error() {
        let ce = ConnectorError::MissingConfig("topic".into());
        let se: SchemaError = ce.into();
        assert!(matches!(se, SchemaError::MissingConfig(ref m) if m == "topic"));
    }

    #[test]
    fn test_schema_to_connector_error() {
        let se = SchemaError::Incompatible("field type mismatch".into());
        let ce: ConnectorError = se.into();
        assert!(matches!(ce, ConnectorError::SchemaMismatch(_)));
    }

    #[test]
    fn test_schema_error_from_arrow() {
        let arrow_err = arrow_schema::ArrowError::SchemaError("bad schema".into());
        let se: SchemaError = arrow_err.into();
        assert!(matches!(se, SchemaError::Arrow(_)));
        assert!(se.to_string().contains("bad schema"));
    }

    #[test]
    fn test_other_connector_error_wraps() {
        let ce = ConnectorError::ConnectionFailed("host down".into());
        let se: SchemaError = ce.into();
        assert!(matches!(se, SchemaError::Other(_)));
        assert!(se.to_string().contains("host down"));
    }
}
