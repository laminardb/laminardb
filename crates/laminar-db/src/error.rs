//! Error types for the `LaminarDB` facade.

/// Errors from database operations.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// SQL parse error
    Sql(#[from] laminar_sql::Error),

    /// Core engine error
    Engine(#[from] laminar_core::Error),

    /// Streaming API error
    Streaming(#[from] laminar_core::streaming::StreamingError),

    /// `DataFusion` error (translated to user-friendly messages on display)
    DataFusion(#[from] datafusion_common::DataFusionError),

    /// Source not found
    SourceNotFound(String),

    /// Sink not found
    SinkNotFound(String),

    /// Query not found
    QueryNotFound(String),

    /// Source already exists
    SourceAlreadyExists(String),

    /// Sink already exists
    SinkAlreadyExists(String),

    /// Stream not found
    StreamNotFound(String),

    /// Stream already exists
    StreamAlreadyExists(String),

    /// Table not found
    TableNotFound(String),

    /// Table already exists
    TableAlreadyExists(String),

    /// Insert error
    InsertError(String),

    /// Schema mismatch between Rust type and SQL definition
    SchemaMismatch(String),

    /// Invalid SQL statement for the operation
    InvalidOperation(String),

    /// SQL parse error (from streaming parser)
    SqlParse(#[from] laminar_sql::parser::ParseError),

    /// Database is shut down
    Shutdown,

    /// Checkpoint error
    Checkpoint(String),

    /// Unresolved config variable
    UnresolvedConfigVar(String),

    /// Connector error
    Connector(String),

    /// Pipeline error (start/shutdown lifecycle)
    Pipeline(String),

    /// Query pipeline error — wraps a `DataFusion` error with stream context.
    /// Unlike `Pipeline`, this variant is translated to user-friendly messages.
    QueryPipeline {
        /// The stream or query name where the error occurred.
        context: String,
        /// The translated error message (already processed through
        /// `translate_datafusion_error`).
        translated: String,
    },

    /// Materialized view error
    MaterializedView(String),

    /// Storage backend error.
    Storage(String),

    /// Configuration / profile validation error
    Config(String),
}

impl DbError {
    /// Create a `QueryPipeline` error from a `DataFusion` error with stream context.
    ///
    /// The `DataFusion` error is translated to a user-friendly message with
    /// structured error codes. The raw `DataFusion` internals are never exposed.
    pub fn query_pipeline(
        context: impl Into<String>,
        df_error: &datafusion_common::DataFusionError,
    ) -> Self {
        let translated =
            laminar_sql::error::translate_datafusion_error(&df_error.to_string());
        Self::QueryPipeline {
            context: context.into(),
            translated: translated.to_string(),
        }
    }

    /// Create a `QueryPipeline` error from a `DataFusion` error with stream
    /// context and available column names for typo suggestions.
    pub fn query_pipeline_with_columns(
        context: impl Into<String>,
        df_error: &datafusion_common::DataFusionError,
        available_columns: &[&str],
    ) -> Self {
        let translated =
            laminar_sql::error::translate_datafusion_error_with_context(
                &df_error.to_string(),
                Some(available_columns),
            );
        Self::QueryPipeline {
            context: context.into(),
            translated: translated.to_string(),
        }
    }

    /// Create a `QueryPipeline` error from an Arrow error with stream context.
    pub fn query_pipeline_arrow(
        context: impl Into<String>,
        arrow_error: &arrow::error::ArrowError,
    ) -> Self {
        let translated =
            laminar_sql::error::translate_datafusion_error(&arrow_error.to_string());
        Self::QueryPipeline {
            context: context.into(),
            translated: translated.to_string(),
        }
    }
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(e) => write!(f, "SQL error: {e}"),
            Self::Engine(e) => write!(f, "Engine error: {e}"),
            Self::Streaming(e) => write!(f, "Streaming error: {e}"),
            Self::DataFusion(e) => {
                let translated =
                    laminar_sql::error::translate_datafusion_error(
                        &e.to_string(),
                    );
                write!(f, "{translated}")
            }
            Self::SourceNotFound(name) => {
                write!(f, "Source '{name}' not found")
            }
            Self::SinkNotFound(name) => write!(f, "Sink '{name}' not found"),
            Self::QueryNotFound(name) => {
                write!(f, "Query '{name}' not found")
            }
            Self::SourceAlreadyExists(name) => {
                write!(f, "Source '{name}' already exists")
            }
            Self::SinkAlreadyExists(name) => {
                write!(f, "Sink '{name}' already exists")
            }
            Self::StreamNotFound(name) => {
                write!(f, "Stream '{name}' not found")
            }
            Self::StreamAlreadyExists(name) => {
                write!(f, "Stream '{name}' already exists")
            }
            Self::TableNotFound(name) => {
                write!(f, "Table '{name}' not found")
            }
            Self::TableAlreadyExists(name) => {
                write!(f, "Table '{name}' already exists")
            }
            Self::InsertError(msg) => write!(f, "Insert error: {msg}"),
            Self::SchemaMismatch(msg) => {
                write!(f, "Schema mismatch: {msg}")
            }
            Self::InvalidOperation(msg) => {
                write!(f, "Invalid operation: {msg}")
            }
            Self::SqlParse(e) => write!(f, "SQL parse error: {e}"),
            Self::Shutdown => write!(f, "Database is shut down"),
            Self::Checkpoint(msg) => write!(f, "Checkpoint error: {msg}"),
            Self::UnresolvedConfigVar(msg) => {
                write!(f, "Unresolved config variable: {msg}")
            }
            Self::Connector(msg) => write!(f, "Connector error: {msg}"),
            Self::Pipeline(msg) => write!(f, "Pipeline error: {msg}"),
            Self::QueryPipeline {
                context,
                translated,
            } => write!(f, "Stream '{context}': {translated}"),
            Self::MaterializedView(msg) => {
                write!(f, "Materialized view error: {msg}")
            }
            Self::Storage(msg) => write!(f, "Storage error: {msg}"),
            Self::Config(msg) => write!(f, "Config error: {msg}"),
        }
    }
}
