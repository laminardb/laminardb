//! SQL parser with streaming extensions.
//!
//! Routes streaming DDL (CREATE SOURCE/SINK/CONTINUOUS QUERY) to custom
//! parsers that use sqlparser primitives. Routes standard SQL to sqlparser
//! with `GenericDialect`.

pub mod aggregation_parser;
mod continuous_query_parser;
pub(crate) mod dialect;
mod emit_parser;
pub mod join_parser;
mod late_data_parser;
pub mod order_analyzer;
mod sink_parser;
mod source_parser;
mod statements;
mod tokenizer;
mod window_rewriter;

pub use statements::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, EmitStrategy, LateDataClause,
    SinkFrom, StreamingStatement, WatermarkDef, WindowFunction,
};
pub use window_rewriter::WindowRewriter;

use dialect::LaminarDialect;
use tokenizer::{detect_streaming_ddl, StreamingDdlKind};

/// Parses SQL with streaming extensions.
///
/// Routes streaming DDL to custom parsers that use sqlparser's `Parser` API
/// for structured parsing. Standard SQL is delegated to sqlparser directly.
///
/// # Errors
///
/// Returns `ParseError` if the SQL syntax is invalid.
pub fn parse_streaming_sql(sql: &str) -> Result<Vec<StreamingStatement>, ParseError> {
    StreamingParser::parse_sql(sql).map_err(ParseError::SqlParseError)
}

/// Parser for streaming SQL extensions.
///
/// Provides static methods for parsing streaming SQL statements.
/// Uses sqlparser's `Parser` API internally for structured parsing
/// of identifiers, data types, expressions, and queries.
pub struct StreamingParser;

impl StreamingParser {
    /// Parse a SQL string with streaming extensions.
    ///
    /// Tokenizes the input to detect statement type, then routes to the
    /// appropriate parser:
    /// - CREATE SOURCE → `source_parser`
    /// - CREATE SINK → `sink_parser`
    /// - CREATE CONTINUOUS QUERY → `continuous_query_parser`
    /// - Everything else → `sqlparser::parser::Parser`
    ///
    /// # Errors
    ///
    /// Returns `ParserError` if the SQL syntax is invalid.
    pub fn parse_sql(
        sql: &str,
    ) -> Result<Vec<StreamingStatement>, sqlparser::parser::ParserError> {
        let sql_trimmed = sql.trim();
        if sql_trimmed.is_empty() {
            return Err(sqlparser::parser::ParserError::ParserError(
                "Empty SQL statement".to_string(),
            ));
        }

        let dialect = LaminarDialect::default();

        // Tokenize to detect statement type (with location for better errors)
        let tokens = sqlparser::tokenizer::Tokenizer::new(&dialect, sql_trimmed)
            .tokenize_with_location()
            .map_err(|e| {
                sqlparser::parser::ParserError::ParserError(format!(
                    "Tokenization error: {e}"
                ))
            })?;

        // Route based on token-level detection
        match detect_streaming_ddl(&tokens) {
            StreamingDdlKind::CreateSource { .. } => {
                let mut parser = sqlparser::parser::Parser::new(&dialect)
                    .with_tokens_with_locations(tokens);
                let source = source_parser::parse_create_source(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![StreamingStatement::CreateSource(Box::new(source))])
            }
            StreamingDdlKind::CreateSink { .. } => {
                let mut parser = sqlparser::parser::Parser::new(&dialect)
                    .with_tokens_with_locations(tokens);
                let sink = sink_parser::parse_create_sink(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![StreamingStatement::CreateSink(Box::new(sink))])
            }
            StreamingDdlKind::CreateContinuousQuery { .. } => {
                let mut parser = sqlparser::parser::Parser::new(&dialect)
                    .with_tokens_with_locations(tokens);
                let stmt = continuous_query_parser::parse_continuous_query(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::None => {
                // Standard SQL - delegate to sqlparser
                let statements =
                    sqlparser::parser::Parser::parse_sql(&dialect, sql_trimmed)?;
                Ok(statements
                    .into_iter()
                    .map(|s| StreamingStatement::Standard(Box::new(s)))
                    .collect())
            }
        }
    }

    /// Check if an expression contains a window function.
    #[must_use]
    pub fn has_window_function(expr: &sqlparser::ast::Expr) -> bool {
        match expr {
            sqlparser::ast::Expr::Function(func) => {
                if let Some(name) = func.name.0.last() {
                    let func_name = name.to_string().to_uppercase();
                    matches!(func_name.as_str(), "TUMBLE" | "HOP" | "SESSION")
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Parse EMIT clause from SQL string.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::StreamingError` if the EMIT clause syntax is invalid.
    pub fn parse_emit_clause(sql: &str) -> Result<Option<EmitClause>, ParseError> {
        emit_parser::parse_emit_clause_from_sql(sql)
    }

    /// Parse late data handling clause from SQL string.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::StreamingError` if the clause syntax is invalid.
    pub fn parse_late_data_clause(sql: &str) -> Result<Option<LateDataClause>, ParseError> {
        late_data_parser::parse_late_data_clause_from_sql(sql)
    }
}

/// Convert `ParseError` to `ParserError` for backward compatibility.
fn parse_error_to_parser_error(e: ParseError) -> sqlparser::parser::ParserError {
    match e {
        ParseError::SqlParseError(pe) => pe,
        ParseError::StreamingError(msg) => {
            sqlparser::parser::ParserError::ParserError(msg)
        }
        ParseError::WindowError(msg) => {
            sqlparser::parser::ParserError::ParserError(format!("Window error: {msg}"))
        }
        ParseError::ValidationError(msg) => {
            sqlparser::parser::ParserError::ParserError(format!("Validation error: {msg}"))
        }
    }
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

    /// Validation error (e.g., invalid option values)
    #[error("Validation error: {0}")]
    ValidationError(String),
}
