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

    /// Materialized view error
    MaterializedView(String),

    /// Storage backend error.
    Storage(String),

    /// Configuration / profile validation error
    Config(String),
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
            Self::MaterializedView(msg) => {
                write!(f, "Materialized view error: {msg}")
            }
            Self::Storage(msg) => write!(f, "Storage error: {msg}"),
            Self::Config(msg) => write!(f, "Config error: {msg}"),
        }
    }
}
