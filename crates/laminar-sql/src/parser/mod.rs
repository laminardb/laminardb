//! SQL parser with streaming extensions

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Streaming-specific SQL statements
#[derive(Debug, Clone)]
pub enum StreamingStatement {
    /// Standard SQL statement
    Standard(Statement),
    /// CREATE STREAM statement
    CreateStream {
        /// Stream name
        name: String,
        /// Source configuration
        source: String,
    },
    /// CREATE CONTINUOUS QUERY
    CreateContinuousQuery {
        /// Query name
        name: String,
        /// SQL query
        query: Statement,
    },
}

/// Parses SQL with streaming extensions
pub fn parse_streaming_sql(sql: &str) -> Result<Vec<StreamingStatement>, ParseError> {
    // TODO: Implement streaming SQL parser
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    Ok(statements
        .into_iter()
        .map(StreamingStatement::Standard)
        .collect())
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
}