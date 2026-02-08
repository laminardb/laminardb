//! SQL parser with streaming extensions.
//!
//! Routes streaming DDL (CREATE SOURCE/SINK/CONTINUOUS QUERY) to custom
//! parsers that use sqlparser primitives. Routes standard SQL to sqlparser
//! with `GenericDialect`.

pub mod aggregation_parser;
pub mod analytic_parser;
mod continuous_query_parser;
pub(crate) mod dialect;
mod emit_parser;
/// INTERVAL arithmetic rewriter for BIGINT timestamp columns
pub mod interval_rewriter;
pub mod join_parser;
mod late_data_parser;
pub mod order_analyzer;
mod sink_parser;
mod source_parser;
mod statements;
mod tokenizer;
mod window_rewriter;

pub use statements::{
    CreateSinkStatement, CreateSourceStatement, EmitClause, EmitStrategy, FormatSpec,
    LateDataClause, ShowCommand, SinkFrom, StreamingStatement, WatermarkDef, WindowFunction,
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
    #[allow(clippy::too_many_lines)]
    pub fn parse_sql(sql: &str) -> Result<Vec<StreamingStatement>, sqlparser::parser::ParserError> {
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
                sqlparser::parser::ParserError::ParserError(format!("Tokenization error: {e}"))
            })?;

        // Route based on token-level detection
        match detect_streaming_ddl(&tokens) {
            StreamingDdlKind::CreateSource { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let source = source_parser::parse_create_source(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![StreamingStatement::CreateSource(Box::new(source))])
            }
            StreamingDdlKind::CreateSink { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let sink = sink_parser::parse_create_sink(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![StreamingStatement::CreateSink(Box::new(sink))])
            }
            StreamingDdlKind::CreateContinuousQuery { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = continuous_query_parser::parse_continuous_query(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::DropSource { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = parse_drop_source(&mut parser).map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::DropSink { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = parse_drop_sink(&mut parser).map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::DropMaterializedView { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = parse_drop_materialized_view(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::ShowSources => {
                Ok(vec![StreamingStatement::Show(ShowCommand::Sources)])
            }
            StreamingDdlKind::ShowSinks => Ok(vec![StreamingStatement::Show(ShowCommand::Sinks)]),
            StreamingDdlKind::ShowQueries => {
                Ok(vec![StreamingStatement::Show(ShowCommand::Queries)])
            }
            StreamingDdlKind::ShowMaterializedViews => Ok(vec![StreamingStatement::Show(
                ShowCommand::MaterializedViews,
            )]),
            StreamingDdlKind::DescribeSource => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = parse_describe(&mut parser).map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::ExplainStreaming => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt =
                    parse_explain(&mut parser, sql_trimmed).map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::CreateMaterializedView { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = parse_create_materialized_view(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::CreateStream { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = parse_create_stream(&mut parser, sql_trimmed)
                    .map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::DropStream { .. } => {
                let mut parser =
                    sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
                let stmt = parse_drop_stream(&mut parser).map_err(parse_error_to_parser_error)?;
                Ok(vec![stmt])
            }
            StreamingDdlKind::ShowStreams => {
                Ok(vec![StreamingStatement::Show(ShowCommand::Streams)])
            }
            StreamingDdlKind::ShowTables => Ok(vec![StreamingStatement::Show(ShowCommand::Tables)]),
            StreamingDdlKind::None => {
                // Standard SQL - check for INSERT INTO and convert
                let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql_trimmed)?;
                Ok(statements
                    .into_iter()
                    .map(convert_standard_statement)
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
        ParseError::StreamingError(msg) => sqlparser::parser::ParserError::ParserError(msg),
        ParseError::WindowError(msg) => {
            sqlparser::parser::ParserError::ParserError(format!("Window error: {msg}"))
        }
        ParseError::ValidationError(msg) => {
            sqlparser::parser::ParserError::ParserError(format!("Validation error: {msg}"))
        }
    }
}

/// Convert a standard sqlparser statement to a `StreamingStatement`.
///
/// Detects INSERT INTO statements and converts them to the streaming
/// `InsertInto` variant. All other statements are wrapped as `Standard`.
fn convert_standard_statement(stmt: sqlparser::ast::Statement) -> StreamingStatement {
    if let sqlparser::ast::Statement::Insert(insert) = &stmt {
        // Extract table name from TableObject
        if let sqlparser::ast::TableObject::TableName(ref name) = insert.table {
            let table_name = name.clone();
            let columns = insert.columns.clone();

            // Try to extract VALUES rows from source query
            if let Some(ref source) = insert.source {
                if let sqlparser::ast::SetExpr::Values(ref values) = *source.body {
                    let rows: Vec<Vec<sqlparser::ast::Expr>> = values.rows.clone();
                    return StreamingStatement::InsertInto {
                        table_name,
                        columns,
                        values: rows,
                    };
                }
            }
        }
    }
    StreamingStatement::Standard(Box::new(stmt))
}

/// Parse a DROP SOURCE statement.
///
/// Syntax: `DROP SOURCE [IF EXISTS] name`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_drop_source(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::DROP)
        .map_err(ParseError::SqlParseError)?;
    tokenizer::expect_custom_keyword(parser, "SOURCE")?;
    let if_exists = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::IF,
        sqlparser::keywords::Keyword::EXISTS,
    ]);
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;
    Ok(StreamingStatement::DropSource { name, if_exists })
}

/// Parse a DROP SINK statement.
///
/// Syntax: `DROP SINK [IF EXISTS] name`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_drop_sink(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::DROP)
        .map_err(ParseError::SqlParseError)?;
    tokenizer::expect_custom_keyword(parser, "SINK")?;
    let if_exists = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::IF,
        sqlparser::keywords::Keyword::EXISTS,
    ]);
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;
    Ok(StreamingStatement::DropSink { name, if_exists })
}

/// Parse a DROP MATERIALIZED VIEW statement.
///
/// Syntax: `DROP MATERIALIZED VIEW [IF EXISTS] name [CASCADE]`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_drop_materialized_view(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::DROP)
        .map_err(ParseError::SqlParseError)?;
    parser
        .expect_keyword(sqlparser::keywords::Keyword::MATERIALIZED)
        .map_err(ParseError::SqlParseError)?;
    parser
        .expect_keyword(sqlparser::keywords::Keyword::VIEW)
        .map_err(ParseError::SqlParseError)?;
    let if_exists = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::IF,
        sqlparser::keywords::Keyword::EXISTS,
    ]);
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;
    let cascade = parser.parse_keyword(sqlparser::keywords::Keyword::CASCADE);
    Ok(StreamingStatement::DropMaterializedView {
        name,
        if_exists,
        cascade,
    })
}

/// Parse a CREATE STREAM statement.
///
/// Syntax: `CREATE [OR REPLACE] STREAM [IF NOT EXISTS] name AS <select_query> [EMIT <strategy>]`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_create_stream(
    parser: &mut sqlparser::parser::Parser,
    _original_sql: &str,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;

    let or_replace = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::OR,
        sqlparser::keywords::Keyword::REPLACE,
    ]);

    tokenizer::expect_custom_keyword(parser, "STREAM")?;

    let if_not_exists = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::IF,
        sqlparser::keywords::Keyword::NOT,
        sqlparser::keywords::Keyword::EXISTS,
    ]);

    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    parser
        .expect_keyword(sqlparser::keywords::Keyword::AS)
        .map_err(ParseError::SqlParseError)?;

    // Collect remaining tokens and split at EMIT boundary
    let remaining = collect_remaining_tokens(parser);
    let (query_tokens, emit_tokens) = split_at_emit(&remaining);

    let stream_dialect = LaminarDialect::default();

    let query = if query_tokens.is_empty() {
        return Err(ParseError::StreamingError(
            "Expected SELECT query after AS".to_string(),
        ));
    } else {
        let mut query_parser = sqlparser::parser::Parser::new(&stream_dialect)
            .with_tokens_with_locations(query_tokens);
        query_parser
            .parse_query()
            .map_err(ParseError::SqlParseError)?
    };

    let query_stmt =
        StreamingStatement::Standard(Box::new(sqlparser::ast::Statement::Query(query)));

    let emit_clause = if emit_tokens.is_empty() {
        None
    } else {
        let mut emit_parser =
            sqlparser::parser::Parser::new(&stream_dialect).with_tokens_with_locations(emit_tokens);
        emit_parser::parse_emit_clause(&mut emit_parser)?
    };

    Ok(StreamingStatement::CreateStream {
        name,
        query: Box::new(query_stmt),
        emit_clause,
        or_replace,
        if_not_exists,
    })
}

/// Parse a DROP STREAM statement.
///
/// Syntax: `DROP STREAM [IF EXISTS] name`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_drop_stream(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::DROP)
        .map_err(ParseError::SqlParseError)?;
    tokenizer::expect_custom_keyword(parser, "STREAM")?;
    let if_exists = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::IF,
        sqlparser::keywords::Keyword::EXISTS,
    ]);
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;
    Ok(StreamingStatement::DropStream { name, if_exists })
}

/// Parse a DESCRIBE statement.
///
/// Syntax: `DESCRIBE [EXTENDED] name`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_describe(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    // Consume DESCRIBE or DESC
    let token = parser.next_token();
    match &token.token {
        sqlparser::tokenizer::Token::Word(w)
            if w.keyword == sqlparser::keywords::Keyword::DESCRIBE
                || w.keyword == sqlparser::keywords::Keyword::DESC => {}
        _ => {
            return Err(ParseError::StreamingError(
                "Expected DESCRIBE or DESC".to_string(),
            ));
        }
    }
    let extended = tokenizer::try_parse_custom_keyword(parser, "EXTENDED");
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;
    Ok(StreamingStatement::Describe { name, extended })
}

/// Parse an EXPLAIN statement wrapping a streaming query.
///
/// Syntax: `EXPLAIN <streaming_statement>`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_explain(
    parser: &mut sqlparser::parser::Parser,
    original_sql: &str,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::EXPLAIN)
        .map_err(ParseError::SqlParseError)?;

    // Find the position after EXPLAIN in the original SQL
    let explain_prefix_upper = original_sql.to_uppercase();
    let inner_start = explain_prefix_upper
        .find("EXPLAIN")
        .map_or(0, |pos| pos + "EXPLAIN".len());
    let inner_sql = original_sql[inner_start..].trim();

    // Parse the inner statement recursively
    let inner_stmts = StreamingParser::parse_sql(inner_sql)?;
    let inner = inner_stmts.into_iter().next().ok_or_else(|| {
        sqlparser::parser::ParserError::ParserError("Expected statement after EXPLAIN".to_string())
    })?;
    Ok(StreamingStatement::Explain {
        statement: Box::new(inner),
    })
}

/// Parse a CREATE MATERIALIZED VIEW statement.
///
/// Syntax:
/// ```sql
/// CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] name
/// AS <select_query>
/// [EMIT <strategy>]
/// ```
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_create_materialized_view(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;

    let or_replace = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::OR,
        sqlparser::keywords::Keyword::REPLACE,
    ]);

    parser
        .expect_keyword(sqlparser::keywords::Keyword::MATERIALIZED)
        .map_err(ParseError::SqlParseError)?;
    parser
        .expect_keyword(sqlparser::keywords::Keyword::VIEW)
        .map_err(ParseError::SqlParseError)?;

    let if_not_exists = parser.parse_keywords(&[
        sqlparser::keywords::Keyword::IF,
        sqlparser::keywords::Keyword::NOT,
        sqlparser::keywords::Keyword::EXISTS,
    ]);

    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    parser
        .expect_keyword(sqlparser::keywords::Keyword::AS)
        .map_err(ParseError::SqlParseError)?;

    // Collect remaining tokens and split at EMIT boundary (same strategy as continuous query)
    let remaining = collect_remaining_tokens(parser);
    let (query_tokens, emit_tokens) = split_at_emit(&remaining);

    let mv_dialect = LaminarDialect::default();

    let query = if query_tokens.is_empty() {
        return Err(ParseError::StreamingError(
            "Expected SELECT query after AS".to_string(),
        ));
    } else {
        let mut query_parser =
            sqlparser::parser::Parser::new(&mv_dialect).with_tokens_with_locations(query_tokens);
        query_parser
            .parse_query()
            .map_err(ParseError::SqlParseError)?
    };

    let query_stmt =
        StreamingStatement::Standard(Box::new(sqlparser::ast::Statement::Query(query)));

    let emit_clause = if emit_tokens.is_empty() {
        None
    } else {
        let mut emit_parser =
            sqlparser::parser::Parser::new(&mv_dialect).with_tokens_with_locations(emit_tokens);
        emit_parser::parse_emit_clause(&mut emit_parser)?
    };

    Ok(StreamingStatement::CreateMaterializedView {
        name,
        query: Box::new(query_stmt),
        emit_clause,
        or_replace,
        if_not_exists,
    })
}

/// Collect all remaining tokens from the parser into a Vec.
fn collect_remaining_tokens(
    parser: &mut sqlparser::parser::Parser,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    let mut tokens = Vec::new();
    loop {
        let token = parser.next_token();
        if token.token == sqlparser::tokenizer::Token::EOF {
            tokens.push(token);
            break;
        }
        tokens.push(token);
    }
    tokens
}

/// Split tokens at the first standalone EMIT keyword (not inside parentheses).
///
/// Returns (query_tokens, emit_tokens) where emit_tokens starts with EMIT
/// (or is empty if no EMIT found).
fn split_at_emit(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
) -> (
    Vec<sqlparser::tokenizer::TokenWithSpan>,
    Vec<sqlparser::tokenizer::TokenWithSpan>,
) {
    let mut depth: i32 = 0;
    for (i, token) in tokens.iter().enumerate() {
        match &token.token {
            sqlparser::tokenizer::Token::LParen => depth += 1,
            sqlparser::tokenizer::Token::RParen => {
                depth -= 1;
            }
            sqlparser::tokenizer::Token::Word(w)
                if depth == 0 && w.value.eq_ignore_ascii_case("EMIT") =>
            {
                let mut query_tokens = tokens[..i].to_vec();
                query_tokens.push(sqlparser::tokenizer::TokenWithSpan {
                    token: sqlparser::tokenizer::Token::EOF,
                    span: sqlparser::tokenizer::Span::empty(),
                });
                let emit_tokens = tokens[i..].to_vec();
                return (query_tokens, emit_tokens);
            }
            _ => {}
        }
    }
    (tokens.to_vec(), vec![])
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to parse SQL and return the first statement.
    fn parse_one(sql: &str) -> StreamingStatement {
        let stmts = StreamingParser::parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1, "Expected exactly 1 statement");
        stmts.into_iter().next().unwrap()
    }

    #[test]
    fn test_parse_drop_source() {
        let stmt = parse_one("DROP SOURCE events");
        match stmt {
            StreamingStatement::DropSource { name, if_exists } => {
                assert_eq!(name.to_string(), "events");
                assert!(!if_exists);
            }
            _ => panic!("Expected DropSource, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_source_if_exists() {
        let stmt = parse_one("DROP SOURCE IF EXISTS events");
        match stmt {
            StreamingStatement::DropSource { name, if_exists } => {
                assert_eq!(name.to_string(), "events");
                assert!(if_exists);
            }
            _ => panic!("Expected DropSource, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_sink() {
        let stmt = parse_one("DROP SINK output");
        match stmt {
            StreamingStatement::DropSink { name, if_exists } => {
                assert_eq!(name.to_string(), "output");
                assert!(!if_exists);
            }
            _ => panic!("Expected DropSink, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_sink_if_exists() {
        let stmt = parse_one("DROP SINK IF EXISTS output");
        match stmt {
            StreamingStatement::DropSink { name, if_exists } => {
                assert_eq!(name.to_string(), "output");
                assert!(if_exists);
            }
            _ => panic!("Expected DropSink, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_materialized_view() {
        let stmt = parse_one("DROP MATERIALIZED VIEW live_stats");
        match stmt {
            StreamingStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert!(!if_exists);
                assert!(!cascade);
            }
            _ => panic!("Expected DropMaterializedView, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_materialized_view_if_exists_cascade() {
        let stmt = parse_one("DROP MATERIALIZED VIEW IF EXISTS live_stats CASCADE");
        match stmt {
            StreamingStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert!(if_exists);
                assert!(cascade);
            }
            _ => panic!("Expected DropMaterializedView, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_show_sources() {
        let stmt = parse_one("SHOW SOURCES");
        assert!(matches!(
            stmt,
            StreamingStatement::Show(ShowCommand::Sources)
        ));
    }

    #[test]
    fn test_parse_show_sinks() {
        let stmt = parse_one("SHOW SINKS");
        assert!(matches!(stmt, StreamingStatement::Show(ShowCommand::Sinks)));
    }

    #[test]
    fn test_parse_show_queries() {
        let stmt = parse_one("SHOW QUERIES");
        assert!(matches!(
            stmt,
            StreamingStatement::Show(ShowCommand::Queries)
        ));
    }

    #[test]
    fn test_parse_show_materialized_views() {
        let stmt = parse_one("SHOW MATERIALIZED VIEWS");
        assert!(matches!(
            stmt,
            StreamingStatement::Show(ShowCommand::MaterializedViews)
        ));
    }

    #[test]
    fn test_parse_describe() {
        let stmt = parse_one("DESCRIBE events");
        match stmt {
            StreamingStatement::Describe { name, extended } => {
                assert_eq!(name.to_string(), "events");
                assert!(!extended);
            }
            _ => panic!("Expected Describe, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_describe_extended() {
        let stmt = parse_one("DESCRIBE EXTENDED my_schema.events");
        match stmt {
            StreamingStatement::Describe { name, extended } => {
                assert_eq!(name.to_string(), "my_schema.events");
                assert!(extended);
            }
            _ => panic!("Expected Describe, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_explain_select() {
        let stmt = parse_one("EXPLAIN SELECT * FROM events");
        match stmt {
            StreamingStatement::Explain { statement } => {
                assert!(matches!(*statement, StreamingStatement::Standard(_)));
            }
            _ => panic!("Expected Explain, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_explain_create_source() {
        let stmt = parse_one("EXPLAIN CREATE SOURCE events (id BIGINT)");
        match stmt {
            StreamingStatement::Explain { statement } => {
                assert!(matches!(*statement, StreamingStatement::CreateSource(_)));
            }
            _ => panic!("Expected Explain wrapping CreateSource, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view() {
        let stmt = parse_one("CREATE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events");
        match stmt {
            StreamingStatement::CreateMaterializedView {
                name,
                emit_clause,
                or_replace,
                if_not_exists,
                ..
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert!(emit_clause.is_none());
                assert!(!or_replace);
                assert!(!if_not_exists);
            }
            _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view_with_emit() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events EMIT ON WINDOW CLOSE",
        );
        match stmt {
            StreamingStatement::CreateMaterializedView {
                name, emit_clause, ..
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
            }
            _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_create_or_replace_materialized_view() {
        let stmt = parse_one(
            "CREATE OR REPLACE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events",
        );
        match stmt {
            StreamingStatement::CreateMaterializedView {
                name,
                or_replace,
                if_not_exists,
                ..
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert!(or_replace);
                assert!(!if_not_exists);
            }
            _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_create_materialized_view_if_not_exists() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS live_stats AS SELECT COUNT(*) FROM events",
        );
        match stmt {
            StreamingStatement::CreateMaterializedView {
                name,
                or_replace,
                if_not_exists,
                ..
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert!(!or_replace);
                assert!(if_not_exists);
            }
            _ => panic!("Expected CreateMaterializedView, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_insert_into() {
        let stmt = parse_one("INSERT INTO events (id, name) VALUES (1, 'test')");
        match stmt {
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => {
                assert_eq!(table_name.to_string(), "events");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].to_string(), "id");
                assert_eq!(columns[1].to_string(), "name");
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 2);
            }
            _ => panic!("Expected InsertInto, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_insert_into_multiple_rows() {
        let stmt = parse_one("INSERT INTO events VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        match stmt {
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => {
                assert_eq!(table_name.to_string(), "events");
                assert!(columns.is_empty());
                assert_eq!(values.len(), 3);
            }
            _ => panic!("Expected InsertInto, got {stmt:?}"),
        }
    }

    // ── CREATE STREAM tests (F-SQL-003) ─────────────────────────────

    #[test]
    fn test_parse_create_stream() {
        let stmt = parse_one(
            "CREATE STREAM session_activity AS SELECT session_id, COUNT(*) as cnt FROM clicks GROUP BY session_id",
        );
        match stmt {
            StreamingStatement::CreateStream {
                name,
                or_replace,
                if_not_exists,
                emit_clause,
                ..
            } => {
                assert_eq!(name.to_string(), "session_activity");
                assert!(!or_replace);
                assert!(!if_not_exists);
                assert!(emit_clause.is_none());
            }
            _ => panic!("Expected CreateStream, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_create_or_replace_stream() {
        let stmt = parse_one("CREATE OR REPLACE STREAM metrics AS SELECT AVG(value) FROM events");
        match stmt {
            StreamingStatement::CreateStream { or_replace, .. } => {
                assert!(or_replace);
            }
            _ => panic!("Expected CreateStream, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_create_stream_if_not_exists() {
        let stmt = parse_one("CREATE STREAM IF NOT EXISTS counts AS SELECT COUNT(*) FROM events");
        match stmt {
            StreamingStatement::CreateStream { if_not_exists, .. } => {
                assert!(if_not_exists);
            }
            _ => panic!("Expected CreateStream, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_create_stream_with_emit() {
        let stmt =
            parse_one("CREATE STREAM windowed AS SELECT COUNT(*) FROM events EMIT ON WINDOW CLOSE");
        match stmt {
            StreamingStatement::CreateStream { emit_clause, .. } => {
                assert_eq!(emit_clause, Some(EmitClause::OnWindowClose));
            }
            _ => panic!("Expected CreateStream, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_stream() {
        let stmt = parse_one("DROP STREAM my_stream");
        match stmt {
            StreamingStatement::DropStream { name, if_exists } => {
                assert_eq!(name.to_string(), "my_stream");
                assert!(!if_exists);
            }
            _ => panic!("Expected DropStream, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_drop_stream_if_exists() {
        let stmt = parse_one("DROP STREAM IF EXISTS my_stream");
        match stmt {
            StreamingStatement::DropStream { name, if_exists } => {
                assert_eq!(name.to_string(), "my_stream");
                assert!(if_exists);
            }
            _ => panic!("Expected DropStream, got {stmt:?}"),
        }
    }

    #[test]
    fn test_parse_show_streams() {
        let stmt = parse_one("SHOW STREAMS");
        assert!(matches!(
            stmt,
            StreamingStatement::Show(ShowCommand::Streams)
        ));
    }
}
