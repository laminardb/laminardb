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

    /// Duplicate wildcard `*` in the column list.
    #[error("duplicate wildcard: only one `*` is allowed in the column list")]
    DuplicateWildcard,

    /// Wildcard `*` used without a connector that provides schema resolution.
    #[error(
        "wildcard without resolution: `*` requires a connector with a schema provider or registry"
    )]
    WildcardWithoutResolution,

    /// A wildcard-prefixed column name collides with a declared column.
    #[error("wildcard prefix collision: prefixed column '{0}' collides with a declared column")]
    WildcardPrefixCollision(String),

    /// Wildcard expanded to zero new columns (all source columns were
    /// already declared).
    #[error("wildcard expanded to zero new columns: all source columns are already declared")]
    WildcardNoNewFields,

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
            // `ConnectorError::MissingConfig` folded into
            // `ConfigurationError` — both land in `InvalidConfig` now.
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
            SchemaError::MissingConfig(msg) => ConnectorError::missing_config(msg),
            SchemaError::InvalidConfig { key, message } => {
                ConnectorError::ConfigurationError(format!("invalid config key '{key}': {message}"))
            }
            SchemaError::Incompatible(msg) => ConnectorError::SchemaMismatch(msg),
            SchemaError::DecodeError(msg) => ConnectorError::ReadError(msg),
            other => ConnectorError::Internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests;
