//! API error types with numeric codes for FFI interoperability.

use thiserror::Error;

/// Error codes for FFI interop.
///
/// Ranges:
/// - 100-199: Connection errors
/// - 200-299: Schema errors
/// - 300-399: Ingestion errors
/// - 400-499: Query errors
/// - 500-599: Subscription errors
/// - 900-999: Internal errors
pub mod codes {
    // Connection
    /// Connection failed.
    pub const CONNECTION_FAILED: i32 = 100;
    /// Connection is closed.
    pub const CONNECTION_CLOSED: i32 = 101;
    /// Connection is still in use.
    pub const CONNECTION_IN_USE: i32 = 102;

    // Schema
    /// Table/source not found.
    pub const TABLE_NOT_FOUND: i32 = 200;
    /// Table/source already exists.
    pub const TABLE_EXISTS: i32 = 201;
    /// Schema mismatch.
    pub const SCHEMA_MISMATCH: i32 = 202;
    /// Invalid schema definition.
    pub const INVALID_SCHEMA: i32 = 203;

    // Ingestion
    /// Ingestion failed.
    pub const INGESTION_FAILED: i32 = 300;
    /// Writer is closed.
    pub const WRITER_CLOSED: i32 = 301;
    /// Batch schema doesn't match source schema.
    pub const BATCH_SCHEMA_MISMATCH: i32 = 302;

    // Query
    /// Query failed.
    pub const QUERY_FAILED: i32 = 400;
    /// SQL parse error.
    pub const SQL_PARSE_ERROR: i32 = 401;
    /// Query was cancelled.
    pub const QUERY_CANCELLED: i32 = 402;

    // Subscription
    /// Subscription failed.
    pub const SUBSCRIPTION_FAILED: i32 = 500;
    /// Subscription is closed.
    pub const SUBSCRIPTION_CLOSED: i32 = 501;
    /// Subscription timed out.
    pub const SUBSCRIPTION_TIMEOUT: i32 = 502;

    // Internal
    /// Internal error.
    pub const INTERNAL_ERROR: i32 = 900;
    /// Database is shut down.
    pub const SHUTDOWN: i32 = 901;
}

/// API error with numeric code for FFI.
///
/// Each error variant includes a numeric code suitable for FFI and a
/// human-readable message.
#[derive(Debug, Clone, Error)]
pub enum ApiError {
    /// Connection-related error.
    #[error("Connection error ({code}): {message}")]
    Connection {
        /// Numeric error code.
        code: i32,
        /// Error message.
        message: String,
    },

    /// Schema-related error (table not found, already exists, etc.).
    #[error("Schema error ({code}): {message}")]
    Schema {
        /// Numeric error code.
        code: i32,
        /// Error message.
        message: String,
    },

    /// Data ingestion error.
    #[error("Ingestion error ({code}): {message}")]
    Ingestion {
        /// Numeric error code.
        code: i32,
        /// Error message.
        message: String,
    },

    /// Query execution error.
    #[error("Query error ({code}): {message}")]
    Query {
        /// Numeric error code.
        code: i32,
        /// Error message.
        message: String,
    },

    /// Subscription error.
    #[error("Subscription error ({code}): {message}")]
    Subscription {
        /// Numeric error code.
        code: i32,
        /// Error message.
        message: String,
    },

    /// Internal error.
    #[error("Internal error ({code}): {message}")]
    Internal {
        /// Numeric error code.
        code: i32,
        /// Error message.
        message: String,
    },
}

impl ApiError {
    /// Get the numeric error code.
    #[must_use]
    pub fn code(&self) -> i32 {
        match self {
            Self::Connection { code, .. }
            | Self::Schema { code, .. }
            | Self::Ingestion { code, .. }
            | Self::Query { code, .. }
            | Self::Subscription { code, .. }
            | Self::Internal { code, .. } => *code,
        }
    }

    /// Get the error message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::Connection { message, .. }
            | Self::Schema { message, .. }
            | Self::Ingestion { message, .. }
            | Self::Query { message, .. }
            | Self::Subscription { message, .. }
            | Self::Internal { message, .. } => message,
        }
    }

    // ---- Constructor helpers ----

    /// Create a connection error with default code.
    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection {
            code: codes::CONNECTION_FAILED,
            message: message.into(),
        }
    }

    /// Create a "table not found" error.
    #[must_use]
    pub fn table_not_found(table: &str) -> Self {
        Self::Schema {
            code: codes::TABLE_NOT_FOUND,
            message: format!("Table not found: {table}"),
        }
    }

    /// Create a "table already exists" error.
    #[must_use]
    pub fn table_exists(table: &str) -> Self {
        Self::Schema {
            code: codes::TABLE_EXISTS,
            message: format!("Table already exists: {table}"),
        }
    }

    /// Create a schema mismatch error.
    pub fn schema_mismatch(message: impl Into<String>) -> Self {
        Self::Schema {
            code: codes::SCHEMA_MISMATCH,
            message: message.into(),
        }
    }

    /// Create an ingestion error with default code.
    pub fn ingestion(message: impl Into<String>) -> Self {
        Self::Ingestion {
            code: codes::INGESTION_FAILED,
            message: message.into(),
        }
    }

    /// Create a query error with default code.
    pub fn query(message: impl Into<String>) -> Self {
        Self::Query {
            code: codes::QUERY_FAILED,
            message: message.into(),
        }
    }

    /// Create a SQL parse error.
    pub fn sql_parse(message: impl Into<String>) -> Self {
        Self::Query {
            code: codes::SQL_PARSE_ERROR,
            message: message.into(),
        }
    }

    /// Create a subscription error with default code.
    pub fn subscription(message: impl Into<String>) -> Self {
        Self::Subscription {
            code: codes::SUBSCRIPTION_FAILED,
            message: message.into(),
        }
    }

    /// Create a "subscription closed" error.
    #[must_use]
    pub fn subscription_closed() -> Self {
        Self::Subscription {
            code: codes::SUBSCRIPTION_CLOSED,
            message: "Subscription closed".into(),
        }
    }

    /// Create a "subscription timeout" error.
    #[must_use]
    pub fn subscription_timeout() -> Self {
        Self::Subscription {
            code: codes::SUBSCRIPTION_TIMEOUT,
            message: "Subscription timeout".into(),
        }
    }

    /// Create an internal error with default code.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            code: codes::INTERNAL_ERROR,
            message: message.into(),
        }
    }

    /// Create a "database shutdown" error.
    #[must_use]
    pub fn shutdown() -> Self {
        Self::Internal {
            code: codes::SHUTDOWN,
            message: "Database is shut down".into(),
        }
    }
}

impl From<crate::DbError> for ApiError {
    fn from(e: crate::DbError) -> Self {
        use crate::DbError;
        match e {
            DbError::SourceNotFound(name)
            | DbError::TableNotFound(name)
            | DbError::StreamNotFound(name)
            | DbError::SinkNotFound(name)
            | DbError::QueryNotFound(name) => Self::table_not_found(&name),

            DbError::SourceAlreadyExists(name)
            | DbError::TableAlreadyExists(name)
            | DbError::StreamAlreadyExists(name)
            | DbError::SinkAlreadyExists(name) => Self::table_exists(&name),

            DbError::SchemaMismatch(msg) => Self::schema_mismatch(msg),
            DbError::InsertError(msg) => Self::ingestion(msg),
            DbError::Sql(e) => Self::sql_parse(e.to_string()),
            DbError::SqlParse(e) => Self::sql_parse(e.to_string()),
            DbError::Streaming(e) => Self::ingestion(e.to_string()),
            DbError::Shutdown => Self::shutdown(),
            DbError::InvalidOperation(msg) => Self::Query {
                code: codes::QUERY_FAILED,
                message: msg,
            },

            // DataFusion errors are query errors, not internal errors
            DbError::DataFusion(e) => {
                let translated = laminar_sql::error::translate_datafusion_error(&e.to_string());
                Self::query(translated.to_string())
            }

            DbError::Engine(e) => Self::internal(format!("Engine error: {e}")),
            DbError::Connector(msg) => Self::internal(format!("Connector error: {msg}")),
            DbError::Pipeline(msg) => Self::internal(format!("Pipeline error: {msg}")),
            DbError::QueryPipeline {
                context,
                translated,
            } => Self::query(format!("Stream '{context}': {translated}")),
            DbError::MaterializedView(msg) => {
                Self::query(format!("Materialized view error: {msg}"))
            }

            other => Self::internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        let err = ApiError::table_not_found("missing");
        assert_eq!(err.code(), codes::TABLE_NOT_FOUND);
        assert!(err.message().contains("missing"));

        let err = ApiError::shutdown();
        assert_eq!(err.code(), codes::SHUTDOWN);
    }

    #[test]
    fn test_error_display() {
        let err = ApiError::connection("failed to connect");
        let s = err.to_string();
        assert!(s.contains("100"));
        assert!(s.contains("failed to connect"));
    }

    #[test]
    fn test_error_conversion() {
        let db_err = crate::DbError::SourceNotFound("foo".into());
        let api_err: ApiError = db_err.into();
        assert_eq!(api_err.code(), codes::TABLE_NOT_FOUND);
        assert!(api_err.message().contains("foo"));
    }

    #[test]
    fn test_datafusion_error_becomes_query_not_internal() {
        let datafusion_err =
            datafusion_common::DataFusionError::Plan("No field named 'foo'".to_string());
        let db_error = crate::DbError::DataFusion(datafusion_err);
        let api_err: ApiError = db_error.into();
        // Should be a Query error, not Internal
        assert_eq!(api_err.code(), codes::QUERY_FAILED);
        assert!(
            api_err.message().contains("foo"),
            "message was: {}",
            api_err.message()
        );
        // Should not contain raw "DataFusion" prefix
        assert!(
            !api_err.message().contains("DataFusion"),
            "message was: {}",
            api_err.message()
        );
    }

    #[test]
    fn test_query_pipeline_error_becomes_query_not_internal() {
        let db_err = crate::DbError::QueryPipeline {
            context: "my_stream".to_string(),
            translated: "[LDB-1100] Column 'foo' not found in query".to_string(),
        };
        let api_err: ApiError = db_err.into();
        // Should be a Query error (400), not Internal (900)
        assert_eq!(api_err.code(), codes::QUERY_FAILED);
        assert!(
            api_err.message().contains("my_stream"),
            "message should include stream name: {}",
            api_err.message()
        );
        assert!(
            api_err.message().contains("LDB-1100"),
            "message should include error code: {}",
            api_err.message()
        );
    }

    #[test]
    fn test_materialized_view_error_becomes_query() {
        let db_err = crate::DbError::MaterializedView("view failed".into());
        let api_err: ApiError = db_err.into();
        assert_eq!(api_err.code(), codes::QUERY_FAILED);
        assert!(api_err.message().contains("view failed"));
    }
}
