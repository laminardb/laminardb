//! SQL parser with streaming extensions.
//!
//! Routes streaming DDL (CREATE SOURCE/SINK/CONTINUOUS QUERY) to custom
//! parsers that use sqlparser primitives. Routes standard SQL to sqlparser
//! with `GenericDialect`.

pub mod aggregation_parser;
pub mod analytic_parser;
mod continuous_query_parser;
mod declare_parser;
pub(crate) mod dialect;
mod emit_parser;
pub mod join_parser;
mod late_data_parser;
/// Parser for CREATE/DROP LOOKUP TABLE DDL statements
pub mod lookup_table;
pub mod order_analyzer;
mod sink_parser;
mod source_parser;
mod statements;
mod subscribe_parser;
mod tokenizer;
mod window_rewriter;

pub use lookup_table::CreateLookupTableStatement;
pub use statements::{
    AlterSourceOperation, CreateSinkStatement, CreateSourceStatement, EmitClause, EmitStrategy,
    FormatSpec, LateDataClause, ShowCommand, SinkFrom, StreamingStatement, SubscribeStatement,
    WatermarkDef, WindowFunction,
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
    pub fn parse_sql(sql: &str) -> Result<Vec<StreamingStatement>, sqlparser::parser::ParserError> {
        let sql_trimmed = sql.trim();
        if sql_trimmed.is_empty() {
            return Err(sqlparser::parser::ParserError::ParserError(
                "Empty SQL statement".to_string(),
            ));
        }

        let dialect = LaminarDialect::default();

        let tokens = sqlparser::tokenizer::Tokenizer::new(&dialect, sql_trimmed)
            .tokenize_with_location()
            .map_err(|e| {
                sqlparser::parser::ParserError::ParserError(format!("Tokenization error: {e}"))
            })?;
        let kind = detect_streaming_ddl(&tokens);
        if kind == StreamingDdlKind::None {
            return parse_standard_or_temporal_sql(&dialect, sql_trimmed, &tokens);
        }

        let mut parser =
            sqlparser::parser::Parser::new(&dialect).with_tokens_with_locations(tokens);
        let statement = match kind {
            StreamingDdlKind::CreateSource { .. } => StreamingStatement::CreateSource(Box::new(
                source_parser::parse_create_source(&mut parser)
                    .map_err(parse_error_to_parser_error)?,
            )),
            StreamingDdlKind::CreateSink { .. } => StreamingStatement::CreateSink(Box::new(
                sink_parser::parse_create_sink(&mut parser).map_err(parse_error_to_parser_error)?,
            )),
            StreamingDdlKind::CreateContinuousQuery { .. } => {
                continuous_query_parser::parse_continuous_query(&mut parser)
                    .map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::DropSource { .. } => {
                parse_drop_source(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::DropSink { .. } => {
                parse_drop_sink(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::DropMaterializedView { .. } => {
                parse_drop_materialized_view(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::ShowSources => StreamingStatement::Show(ShowCommand::Sources),
            StreamingDdlKind::ShowSinks => StreamingStatement::Show(ShowCommand::Sinks),
            StreamingDdlKind::ShowQueries => StreamingStatement::Show(ShowCommand::Queries),
            StreamingDdlKind::ShowMaterializedViews => {
                StreamingStatement::Show(ShowCommand::MaterializedViews)
            }
            StreamingDdlKind::DescribeSource => {
                parse_describe(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::ExplainStreaming => {
                parse_explain(&mut parser, sql_trimmed).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::CreateMaterializedView { .. } => {
                parse_create_materialized_view(&mut parser, sql_trimmed)
                    .map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::CreateStream { .. } => parse_create_stream(&mut parser, sql_trimmed)
                .map_err(parse_error_to_parser_error)?,
            StreamingDdlKind::DropStream { .. } => {
                parse_drop_stream(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::ShowStreams => StreamingStatement::Show(ShowCommand::Streams),
            StreamingDdlKind::ShowTables => StreamingStatement::Show(ShowCommand::Tables),
            StreamingDdlKind::CreateLookupTable { .. } => {
                StreamingStatement::CreateLookupTable(Box::new(
                    lookup_table::parse_create_lookup_table(&mut parser)
                        .map_err(parse_error_to_parser_error)?,
                ))
            }
            StreamingDdlKind::DropLookupTable { .. } => {
                let (name, if_exists) = lookup_table::parse_drop_lookup_table(&mut parser)
                    .map_err(parse_error_to_parser_error)?;
                StreamingStatement::DropLookupTable { name, if_exists }
            }
            StreamingDdlKind::AlterSource => {
                parse_alter_source(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::ShowCheckpointStatus => {
                StreamingStatement::Show(ShowCommand::CheckpointStatus)
            }
            StreamingDdlKind::ShowCreateSource => {
                parse_show_create_source(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::ShowCreateSink => {
                parse_show_create_sink(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::Checkpoint => StreamingStatement::Checkpoint,
            StreamingDdlKind::Subscribe => StreamingStatement::Subscribe(Box::new(
                subscribe_parser::parse_subscribe(&mut parser)
                    .map_err(parse_error_to_parser_error)?,
            )),
            StreamingDdlKind::DeclareCursor => declare_parser::parse_declare_cursor(&mut parser)
                .map_err(parse_error_to_parser_error)?,
            StreamingDdlKind::RestoreCheckpoint => {
                parse_restore_checkpoint(&mut parser).map_err(parse_error_to_parser_error)?
            }
            StreamingDdlKind::None => unreachable!("handled before parser construction"),
        };
        Ok(vec![statement])
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

fn parse_standard_or_temporal_sql(
    dialect: &LaminarDialect,
    sql: &str,
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
) -> Result<Vec<StreamingStatement>, sqlparser::parser::ParserError> {
    if let Some(parsed) =
        join_parser::parse_temporal_probe_query(tokens).map_err(parse_error_to_parser_error)?
    {
        return Ok(vec![StreamingStatement::TemporalProbeQuery {
            statement: Box::new(parsed.statement),
            analysis: Box::new(parsed.analysis),
        }]);
    }

    let statements = sqlparser::parser::Parser::parse_sql(dialect, sql)?;
    Ok(statements
        .into_iter()
        .map(convert_standard_statement)
        .collect())
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

/// Parse a RESTORE FROM CHECKPOINT statement.
///
/// Syntax: `RESTORE FROM CHECKPOINT <id>`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_restore_checkpoint(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    // Consume RESTORE
    tokenizer::expect_custom_keyword(parser, "RESTORE")?;
    // Consume FROM
    if !parser.parse_keyword(sqlparser::keywords::Keyword::FROM) {
        return Err(ParseError::StreamingError(
            "Expected FROM after RESTORE".to_string(),
        ));
    }
    // Consume CHECKPOINT
    tokenizer::expect_custom_keyword(parser, "CHECKPOINT")?;
    // Parse checkpoint ID (numeric literal)
    let token = parser.next_token();
    match &token.token {
        sqlparser::tokenizer::Token::Number(n, _) => {
            let id: u64 = n
                .parse()
                .map_err(|_| ParseError::StreamingError(format!("Invalid checkpoint ID: {n}")))?;
            Ok(StreamingStatement::RestoreCheckpoint { checkpoint_id: id })
        }
        other => Err(ParseError::StreamingError(format!(
            "Expected checkpoint ID (number), found {other}"
        ))),
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
            let columns: Option<Vec<_>> = insert
                .columns
                .iter()
                .map(|column| match column.0.as_slice() {
                    [sqlparser::ast::ObjectNamePart::Identifier(ident)] => Some(ident.clone()),
                    _ => None,
                })
                .collect();

            // Try to extract VALUES rows from source query
            if let Some(ref source) = insert.source {
                if let (Some(columns), sqlparser::ast::SetExpr::Values(values)) =
                    (columns, source.body.as_ref())
                {
                    let rows = values.rows.iter().map(|row| row.content.clone()).collect();
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
/// Syntax: `DROP SOURCE [IF EXISTS] name [CASCADE]`
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
    let cascade = parser.parse_keyword(sqlparser::keywords::Keyword::CASCADE);
    Ok(StreamingStatement::DropSource {
        name,
        if_exists,
        cascade,
    })
}

/// Parse a DROP SINK statement.
///
/// Syntax: `DROP SINK [IF EXISTS] name [CASCADE]`
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
    let cascade = parser.parse_keyword(sqlparser::keywords::Keyword::CASCADE);
    Ok(StreamingStatement::DropSink {
        name,
        if_exists,
        cascade,
    })
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
    original_sql: &str,
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

    // Collect remaining tokens, then peel off the optional trailing
    // `WITH (...)` before splitting query / EMIT.
    let remaining = collect_remaining_tokens(parser);
    let (head_tokens, with_tokens) = split_off_trailing_with(&remaining);
    let (query_tokens, emit_tokens) = split_at_emit(&head_tokens);
    let raw_query_sql = query_body_sql(
        original_sql,
        &query_tokens,
        &emit_tokens,
        with_tokens.as_deref(),
    );

    let stream_dialect = LaminarDialect::default();

    let (query_stmt, normalized_temporal_sql) = if query_tokens.is_empty() {
        return Err(ParseError::StreamingError(
            "Expected SELECT query after AS".to_string(),
        ));
    } else if let Some(parsed) = join_parser::parse_temporal_probe_query(&query_tokens)? {
        (
            StreamingStatement::TemporalProbeQuery {
                statement: Box::new(parsed.statement),
                analysis: Box::new(parsed.analysis),
            },
            Some(parsed.normalized_sql),
        )
    } else {
        let mut query_parser = sqlparser::parser::Parser::new(&stream_dialect)
            .with_tokens_with_locations(query_tokens);
        let query = query_parser
            .parse_query()
            .map_err(ParseError::SqlParseError)?;
        (
            StreamingStatement::Standard(Box::new(sqlparser::ast::Statement::Query(query))),
            None,
        )
    };
    let query_sql = normalized_temporal_sql.unwrap_or(raw_query_sql);

    let emit_clause = if emit_tokens.is_empty() {
        None
    } else {
        let mut emit_parser =
            sqlparser::parser::Parser::new(&stream_dialect).with_tokens_with_locations(emit_tokens);
        emit_parser::parse_emit_clause(&mut emit_parser)?
    };

    let retention_bytes = match with_tokens {
        None => None,
        Some(tokens) => {
            let mut with_parser =
                sqlparser::parser::Parser::new(&stream_dialect).with_tokens_with_locations(tokens);
            let opts = tokenizer::parse_with_options(&mut with_parser)?;
            extract_retention_bytes(&opts)?
        }
    };

    Ok(StreamingStatement::CreateStream {
        name,
        query: Box::new(query_stmt),
        emit_clause,
        or_replace,
        if_not_exists,
        query_sql,
        retention_bytes,
    })
}

/// Split off a trailing `WITH (` at depth 0. CTE-style `WITH ident AS (...)`
/// is ignored because it's followed by an identifier, not `(`.
fn split_off_trailing_with(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
) -> (
    Vec<sqlparser::tokenizer::TokenWithSpan>,
    Option<Vec<sqlparser::tokenizer::TokenWithSpan>>,
) {
    use sqlparser::tokenizer::Token;
    let mut depth: i32 = 0;
    let mut last_with_idx: Option<usize> = None;
    for (i, t) in tokens.iter().enumerate() {
        match &t.token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(w) if depth == 0 && w.value.eq_ignore_ascii_case("WITH") => {
                if matches!(tokens.get(i + 1).map(|t| &t.token), Some(Token::LParen)) {
                    last_with_idx = Some(i);
                }
            }
            _ => {}
        }
    }
    match last_with_idx {
        None => (tokens.to_vec(), None),
        Some(i) => {
            let mut head = tokens[..i].to_vec();
            head.push(sqlparser::tokenizer::TokenWithSpan {
                token: Token::EOF,
                span: sqlparser::tokenizer::Span::empty(),
            });
            let tail = tokens[i..].to_vec();
            (head, Some(tail))
        }
    }
}

/// Unknown keys are rejected so typos surface immediately.
fn extract_retention_bytes(
    opts: &std::collections::HashMap<String, String>,
) -> Result<Option<u64>, ParseError> {
    for key in opts.keys() {
        if !key.eq_ignore_ascii_case("retain_history") {
            return Err(ParseError::StreamingError(format!(
                "unknown CREATE STREAM option '{key}' (expected RETAIN_HISTORY)"
            )));
        }
    }
    match opts
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("retain_history"))
    {
        None => Ok(None),
        Some((_, v)) => {
            let bytes = lookup_table::ByteSize::parse(v)?;
            Ok(Some(bytes.as_bytes()))
        }
    }
}

/// Slice `original_sql` from the first query token to whichever trailing
/// clause comes next (`EMIT` or `WITH`), or end of input if neither is
/// present. Preserves custom streaming syntax that sqlparser's AST would
/// drop. Falls back to joining token text if spans are empty.
pub(super) fn query_body_sql(
    original_sql: &str,
    query_tokens: &[sqlparser::tokenizer::TokenWithSpan],
    emit_tokens: &[sqlparser::tokenizer::TokenWithSpan],
    with_tokens: Option<&[sqlparser::tokenizer::TokenWithSpan]>,
) -> String {
    use sqlparser::tokenizer::Token;

    let from_spans = || -> Option<String> {
        let first = query_tokens
            .iter()
            .find(|t| !matches!(t.token, Token::EOF))?;
        let start = location_to_byte_offset(original_sql, first.span.start)?;
        let trailer_starts = [emit_tokens.first(), with_tokens.and_then(|t| t.first())]
            .into_iter()
            .flatten()
            .filter_map(|t| location_to_byte_offset(original_sql, t.span.start));
        let end = trailer_starts.min().unwrap_or(original_sql.len());
        let slice = original_sql.get(start..end)?;
        Some(
            slice
                .trim_end_matches(|c: char| c.is_whitespace() || c == ';')
                .to_string(),
        )
    };

    from_spans().unwrap_or_else(|| {
        query_tokens
            .iter()
            .take_while(|t| !matches!(t.token, Token::EOF))
            .map(|t| t.token.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    })
}

/// Parse a DROP STREAM statement.
///
/// Syntax: `DROP STREAM [IF EXISTS] name [CASCADE]`
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
    let cascade = parser.parse_keyword(sqlparser::keywords::Keyword::CASCADE);
    Ok(StreamingStatement::DropStream {
        name,
        if_exists,
        cascade,
    })
}

/// Parse a DESCRIBE statement.
///
/// Syntax: `DESCRIBE [EXTENDED] name`
/// Parse an ALTER SOURCE statement.
///
/// Syntax:
/// - `ALTER SOURCE name ADD COLUMN col_name data_type`
/// - `ALTER SOURCE name SET ('key' = 'value', ...)`
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
fn parse_alter_source(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    parser
        .expect_keyword(sqlparser::keywords::Keyword::ALTER)
        .map_err(ParseError::SqlParseError)?;
    tokenizer::expect_custom_keyword(parser, "SOURCE")?;
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    // Determine operation: ADD COLUMN or SET
    if parser.parse_keywords(&[
        sqlparser::keywords::Keyword::ADD,
        sqlparser::keywords::Keyword::COLUMN,
    ]) {
        // ALTER SOURCE name ADD COLUMN col_name data_type
        let col_name = parser
            .parse_identifier()
            .map_err(ParseError::SqlParseError)?;
        let data_type = parser
            .parse_data_type()
            .map_err(ParseError::SqlParseError)?;
        let column_def = sqlparser::ast::ColumnDef {
            name: col_name,
            data_type,
            options: vec![],
        };
        Ok(StreamingStatement::AlterSource {
            name,
            operation: statements::AlterSourceOperation::AddColumn { column_def },
        })
    } else if parser.parse_keyword(sqlparser::keywords::Keyword::SET) {
        // ALTER SOURCE name SET ('key' = 'value', ...)
        parser
            .expect_token(&sqlparser::tokenizer::Token::LParen)
            .map_err(ParseError::SqlParseError)?;
        #[allow(clippy::disallowed_types)] // cold path: SQL parsing
        let mut properties = std::collections::HashMap::new();
        loop {
            let key = parser
                .parse_literal_string()
                .map_err(ParseError::SqlParseError)?;
            parser
                .expect_token(&sqlparser::tokenizer::Token::Eq)
                .map_err(ParseError::SqlParseError)?;
            let value = parser
                .parse_literal_string()
                .map_err(ParseError::SqlParseError)?;
            properties.insert(key, value);
            if !parser.consume_token(&sqlparser::tokenizer::Token::Comma) {
                break;
            }
        }
        parser
            .expect_token(&sqlparser::tokenizer::Token::RParen)
            .map_err(ParseError::SqlParseError)?;
        Ok(StreamingStatement::AlterSource {
            name,
            operation: statements::AlterSourceOperation::SetProperties { properties },
        })
    } else {
        Err(ParseError::StreamingError(
            "Expected ADD COLUMN or SET after ALTER SOURCE <name>".to_string(),
        ))
    }
}

/// Parse a DESCRIBE statement.
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

/// Parse `SHOW CREATE SOURCE <name>`.
fn parse_show_create_source(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    // Consume SHOW CREATE SOURCE
    parser
        .expect_keyword(sqlparser::keywords::Keyword::SHOW)
        .map_err(ParseError::SqlParseError)?;
    parser
        .expect_keyword(sqlparser::keywords::Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;
    tokenizer::expect_custom_keyword(parser, "SOURCE")?;
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;
    Ok(StreamingStatement::Show(ShowCommand::CreateSource { name }))
}

/// Parse `SHOW CREATE SINK <name>`.
fn parse_show_create_sink(
    parser: &mut sqlparser::parser::Parser,
) -> Result<StreamingStatement, ParseError> {
    // Consume SHOW CREATE SINK
    parser
        .expect_keyword(sqlparser::keywords::Keyword::SHOW)
        .map_err(ParseError::SqlParseError)?;
    parser
        .expect_keyword(sqlparser::keywords::Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;
    tokenizer::expect_custom_keyword(parser, "SINK")?;
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;
    Ok(StreamingStatement::Show(ShowCommand::CreateSink { name }))
}

/// Parse an EXPLAIN [ANALYZE] statement wrapping a streaming query.
///
/// Syntax: `EXPLAIN [ANALYZE] <streaming_statement>`
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

    // Check for optional ANALYZE keyword
    let analyze = tokenizer::try_parse_custom_keyword(parser, "ANALYZE");

    // Find the position after EXPLAIN [ANALYZE] in the original SQL
    let explain_prefix_upper = original_sql.to_uppercase();
    let skip_keyword = if analyze { "ANALYZE" } else { "EXPLAIN" };
    let inner_start = if analyze {
        explain_prefix_upper
            .find("ANALYZE")
            .map_or(0, |pos| pos + "ANALYZE".len())
    } else {
        explain_prefix_upper
            .find("EXPLAIN")
            .map_or(0, |pos| pos + "EXPLAIN".len())
    };
    let inner_sql = original_sql[inner_start..].trim();
    let _ = skip_keyword; // suppress unused warning

    // Parse the inner statement recursively
    let inner_stmts = StreamingParser::parse_sql(inner_sql)?;
    let inner = inner_stmts.into_iter().next().ok_or_else(|| {
        sqlparser::parser::ParserError::ParserError("Expected statement after EXPLAIN".to_string())
    })?;
    Ok(StreamingStatement::Explain {
        statement: Box::new(inner),
        analyze,
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
    original_sql: &str,
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
    let raw_query_sql = query_body_sql(original_sql, &query_tokens, &emit_tokens, None);

    let mv_dialect = LaminarDialect::default();

    let (query_stmt, normalized_temporal_sql) = if query_tokens.is_empty() {
        return Err(ParseError::StreamingError(
            "Expected SELECT query after AS".to_string(),
        ));
    } else if let Some(parsed) = join_parser::parse_temporal_probe_query(&query_tokens)? {
        (
            StreamingStatement::TemporalProbeQuery {
                statement: Box::new(parsed.statement),
                analysis: Box::new(parsed.analysis),
            },
            Some(parsed.normalized_sql),
        )
    } else {
        let mut query_parser =
            sqlparser::parser::Parser::new(&mv_dialect).with_tokens_with_locations(query_tokens);
        let query = query_parser
            .parse_query()
            .map_err(ParseError::SqlParseError)?;
        (
            StreamingStatement::Standard(Box::new(sqlparser::ast::Statement::Query(query))),
            None,
        )
    };
    let query_sql = normalized_temporal_sql.unwrap_or(raw_query_sql);

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
        query_sql,
    })
}

/// Byte offset in `sql` for a sqlparser `Location` (1-indexed line/column).
fn location_to_byte_offset(sql: &str, loc: sqlparser::tokenizer::Location) -> Option<usize> {
    if loc.line == 0 {
        return None;
    }
    let (mut line, mut col) = (1u64, 1u64);
    for (idx, ch) in sql.char_indices() {
        if line == loc.line && col == loc.column {
            return Some(idx);
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line == loc.line && col == loc.column).then_some(sql.len())
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
mod tests;
