//! SQL parser with streaming extensions

mod parser_simple;
mod statements;
mod window_rewriter;

pub use parser_simple::StreamingParser;
pub use statements::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, SinkFrom,
    StreamingStatement, WatermarkDef, WindowFunction,
};
pub use window_rewriter::WindowRewriter;

/// Parses SQL with streaming extensions
///
/// # Errors
///
/// Returns `ParseError` if the SQL syntax is invalid
pub fn parse_streaming_sql(sql: &str) -> Result<Vec<StreamingStatement>, ParseError> {
    StreamingParser::parse_sql(sql).map_err(ParseError::SqlParseError)
}

/// SQL parsing errors
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// Standard SQL parse error
    #[error("SQL parse error: {0}")]
    SqlParseError(#[from] sqlparser::parser::ParserError),

    /// Streaming extension parse error
    #[error("Streaming SQL error: {0}")]
    StreamingError(String),

    /// Window function error
    #[error("Window function error: {0}")]
    WindowError(String),
}