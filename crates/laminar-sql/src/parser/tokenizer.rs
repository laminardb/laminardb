//! Token-level helpers for streaming SQL keyword detection and consumption.
//!
//! Provides helpers to detect streaming DDL types from a token stream and
//! to consume custom keywords that are not in sqlparser's keyword enum.

use std::collections::HashMap;

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};

use super::ParseError;

/// The kind of streaming DDL statement detected from the token stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingDdlKind {
    /// CREATE [OR REPLACE] SOURCE
    CreateSource {
        /// Whether OR REPLACE was specified
        or_replace: bool,
    },
    /// CREATE [OR REPLACE] SINK
    CreateSink {
        /// Whether OR REPLACE was specified
        or_replace: bool,
    },
    /// CREATE [OR REPLACE] CONTINUOUS QUERY
    CreateContinuousQuery {
        /// Whether OR REPLACE was specified
        or_replace: bool,
    },
    /// DROP SOURCE [IF EXISTS]
    DropSource {
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },
    /// DROP SINK [IF EXISTS]
    DropSink {
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },
    /// DROP MATERIALIZED VIEW [IF EXISTS]
    DropMaterializedView {
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },
    /// SHOW SOURCES
    ShowSources,
    /// SHOW SINKS
    ShowSinks,
    /// SHOW QUERIES
    ShowQueries,
    /// SHOW MATERIALIZED VIEWS
    ShowMaterializedViews,
    /// DESCRIBE <object>
    DescribeSource,
    /// EXPLAIN <streaming query>
    ExplainStreaming,
    /// CREATE [OR REPLACE] MATERIALIZED VIEW
    CreateMaterializedView {
        /// Whether OR REPLACE was specified
        or_replace: bool,
    },
    /// CREATE [OR REPLACE] STREAM
    CreateStream {
        /// Whether OR REPLACE was specified
        or_replace: bool,
    },
    /// DROP STREAM [IF EXISTS]
    DropStream {
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },
    /// SHOW STREAMS
    ShowStreams,
    /// SHOW TABLES
    ShowTables,
    /// CREATE [OR REPLACE] LOOKUP TABLE
    CreateLookupTable {
        /// Whether OR REPLACE was specified
        or_replace: bool,
    },
    /// DROP LOOKUP TABLE [IF EXISTS]
    DropLookupTable {
        /// Whether IF EXISTS was specified
        if_exists: bool,
    },
    /// ALTER SOURCE
    AlterSource,
    /// Not a streaming DDL statement
    None,
}

/// Detect which streaming DDL type (if any) the token stream represents.
///
/// Examines the first few significant tokens to determine the statement type.
/// Recognizes CREATE SOURCE/SINK/CONTINUOUS QUERY/MATERIALIZED VIEW,
/// DROP SOURCE/SINK/MATERIALIZED VIEW, SHOW SOURCES/SINKS/QUERIES/MATERIALIZED VIEWS,
/// DESCRIBE, and EXPLAIN statements.
/// Whitespace tokens are skipped during detection.
pub fn detect_streaming_ddl(tokens: &[TokenWithSpan]) -> StreamingDdlKind {
    let significant: Vec<&TokenWithSpan> = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();

    if significant.len() < 2 {
        return StreamingDdlKind::None;
    }

    // Dispatch based on the first token
    match &significant[0].token {
        Token::Word(Word {
            keyword: Keyword::CREATE,
            ..
        }) => detect_create_ddl(&significant),
        Token::Word(Word {
            keyword: Keyword::DROP,
            ..
        }) => detect_drop_ddl(&significant),
        Token::Word(w) if is_word_ci(w, "SHOW") => detect_show_ddl(&significant),
        Token::Word(
            Word {
                keyword: Keyword::DESCRIBE,
                ..
            }
            | Word {
                keyword: Keyword::DESC,
                ..
            },
        ) => StreamingDdlKind::DescribeSource,
        Token::Word(Word {
            keyword: Keyword::EXPLAIN,
            ..
        }) => detect_explain_ddl(&significant),
        Token::Word(Word {
            keyword: Keyword::ALTER,
            ..
        }) => detect_alter_ddl(&significant),
        _ => StreamingDdlKind::None,
    }
}

/// Detect CREATE-based DDL statements.
fn detect_create_ddl(significant: &[&TokenWithSpan]) -> StreamingDdlKind {
    if significant.len() < 2 {
        return StreamingDdlKind::None;
    }

    match &significant[1].token {
        Token::Word(w) if is_word_ci(w, "SOURCE") => {
            StreamingDdlKind::CreateSource { or_replace: false }
        }
        Token::Word(w) if is_word_ci(w, "SINK") => {
            StreamingDdlKind::CreateSink { or_replace: false }
        }
        Token::Word(w) if is_word_ci(w, "CONTINUOUS") => {
            StreamingDdlKind::CreateContinuousQuery { or_replace: false }
        }
        Token::Word(w) if is_word_ci(w, "STREAM") => {
            StreamingDdlKind::CreateStream { or_replace: false }
        }
        Token::Word(w) if is_word_ci(w, "LOOKUP") => {
            // CREATE LOOKUP TABLE
            if significant.len() >= 3 {
                if let Token::Word(Word {
                    keyword: Keyword::TABLE,
                    ..
                }) = &significant[2].token
                {
                    return StreamingDdlKind::CreateLookupTable { or_replace: false };
                }
            }
            StreamingDdlKind::None
        }
        Token::Word(Word {
            keyword: Keyword::MATERIALIZED,
            ..
        }) => {
            // CREATE MATERIALIZED VIEW
            if significant.len() >= 3 {
                if let Token::Word(Word {
                    keyword: Keyword::VIEW,
                    ..
                }) = &significant[2].token
                {
                    return StreamingDdlKind::CreateMaterializedView { or_replace: false };
                }
            }
            StreamingDdlKind::None
        }
        Token::Word(Word {
            keyword: Keyword::OR,
            ..
        }) => {
            // Check for CREATE OR REPLACE <keyword>
            if significant.len() >= 4 {
                let is_replace = matches!(
                    &significant[2].token,
                    Token::Word(Word {
                        keyword: Keyword::REPLACE,
                        ..
                    })
                );
                if is_replace {
                    return classify_after_or_replace(&significant[3].token, significant);
                }
            }
            StreamingDdlKind::None
        }
        _ => StreamingDdlKind::None,
    }
}

/// Detect DROP-based DDL statements.
fn detect_drop_ddl(significant: &[&TokenWithSpan]) -> StreamingDdlKind {
    if significant.len() < 2 {
        return StreamingDdlKind::None;
    }

    match &significant[1].token {
        Token::Word(w) if is_word_ci(w, "SOURCE") => {
            let if_exists = has_if_exists(significant, 2);
            StreamingDdlKind::DropSource { if_exists }
        }
        Token::Word(w) if is_word_ci(w, "SINK") => {
            let if_exists = has_if_exists(significant, 2);
            StreamingDdlKind::DropSink { if_exists }
        }
        Token::Word(w) if is_word_ci(w, "STREAM") => {
            let if_exists = has_if_exists(significant, 2);
            StreamingDdlKind::DropStream { if_exists }
        }
        Token::Word(w) if is_word_ci(w, "LOOKUP") => {
            // DROP LOOKUP TABLE
            if significant.len() >= 3 {
                if let Token::Word(Word {
                    keyword: Keyword::TABLE,
                    ..
                }) = &significant[2].token
                {
                    let if_exists = has_if_exists(significant, 3);
                    return StreamingDdlKind::DropLookupTable { if_exists };
                }
            }
            StreamingDdlKind::None
        }
        Token::Word(Word {
            keyword: Keyword::MATERIALIZED,
            ..
        }) => {
            // DROP MATERIALIZED VIEW
            if significant.len() >= 3 {
                if let Token::Word(Word {
                    keyword: Keyword::VIEW,
                    ..
                }) = &significant[2].token
                {
                    let if_exists = has_if_exists(significant, 3);
                    return StreamingDdlKind::DropMaterializedView { if_exists };
                }
            }
            StreamingDdlKind::None
        }
        _ => StreamingDdlKind::None,
    }
}

/// Detect ALTER-based DDL statements.
fn detect_alter_ddl(significant: &[&TokenWithSpan]) -> StreamingDdlKind {
    if significant.len() < 2 {
        return StreamingDdlKind::None;
    }
    match &significant[1].token {
        Token::Word(w) if is_word_ci(w, "SOURCE") => StreamingDdlKind::AlterSource,
        _ => StreamingDdlKind::None,
    }
}

/// Detect SHOW-based DDL statements.
fn detect_show_ddl(significant: &[&TokenWithSpan]) -> StreamingDdlKind {
    if significant.len() < 2 {
        return StreamingDdlKind::None;
    }

    match &significant[1].token {
        Token::Word(w) if is_word_ci(w, "SOURCES") => StreamingDdlKind::ShowSources,
        Token::Word(w) if is_word_ci(w, "SINKS") => StreamingDdlKind::ShowSinks,
        Token::Word(w) if is_word_ci(w, "QUERIES") => StreamingDdlKind::ShowQueries,
        Token::Word(w) if is_word_ci(w, "STREAMS") => StreamingDdlKind::ShowStreams,
        Token::Word(w) if is_word_ci(w, "TABLES") => StreamingDdlKind::ShowTables,
        Token::Word(Word {
            keyword: Keyword::MATERIALIZED,
            ..
        }) => {
            // SHOW MATERIALIZED VIEWS
            if significant.len() >= 3 {
                if let Token::Word(w) = &significant[2].token {
                    if is_word_ci(w, "VIEWS") {
                        return StreamingDdlKind::ShowMaterializedViews;
                    }
                }
            }
            StreamingDdlKind::None
        }
        _ => StreamingDdlKind::None,
    }
}

/// Detect EXPLAIN statements that wrap streaming queries.
fn detect_explain_ddl(significant: &[&TokenWithSpan]) -> StreamingDdlKind {
    if significant.len() < 2 {
        return StreamingDdlKind::None;
    }

    // EXPLAIN followed by SELECT or CREATE (streaming DDL)
    match &significant[1].token {
        Token::Word(
            Word {
                keyword: Keyword::SELECT,
                ..
            }
            | Word {
                keyword: Keyword::CREATE,
                ..
            },
        ) => StreamingDdlKind::ExplainStreaming,
        _ => StreamingDdlKind::None,
    }
}

/// Check if IF EXISTS appears at the given offset in the significant tokens.
fn has_if_exists(significant: &[&TokenWithSpan], offset: usize) -> bool {
    if significant.len() > offset + 1 {
        let is_if = matches!(
            &significant[offset].token,
            Token::Word(Word {
                keyword: Keyword::IF,
                ..
            })
        );
        let is_exists = matches!(
            &significant[offset + 1].token,
            Token::Word(Word {
                keyword: Keyword::EXISTS,
                ..
            })
        );
        is_if && is_exists
    } else {
        false
    }
}

/// Classify the token after CREATE OR REPLACE.
///
/// Also handles `CREATE OR REPLACE MATERIALIZED VIEW` by checking the
/// next token in the significant tokens array.
fn classify_after_or_replace(token: &Token, significant: &[&TokenWithSpan]) -> StreamingDdlKind {
    match token {
        Token::Word(w) if is_word_ci(w, "SOURCE") => {
            StreamingDdlKind::CreateSource { or_replace: true }
        }
        Token::Word(w) if is_word_ci(w, "SINK") => {
            StreamingDdlKind::CreateSink { or_replace: true }
        }
        Token::Word(w) if is_word_ci(w, "CONTINUOUS") => {
            StreamingDdlKind::CreateContinuousQuery { or_replace: true }
        }
        Token::Word(w) if is_word_ci(w, "STREAM") => {
            StreamingDdlKind::CreateStream { or_replace: true }
        }
        Token::Word(w) if is_word_ci(w, "LOOKUP") => {
            // CREATE OR REPLACE LOOKUP TABLE
            // significant[0]=CREATE [1]=OR [2]=REPLACE [3]=LOOKUP [4]=TABLE
            if significant.len() >= 5 {
                if let Token::Word(Word {
                    keyword: Keyword::TABLE,
                    ..
                }) = &significant[4].token
                {
                    return StreamingDdlKind::CreateLookupTable { or_replace: true };
                }
            }
            StreamingDdlKind::None
        }
        Token::Word(Word {
            keyword: Keyword::MATERIALIZED,
            ..
        }) => {
            // CREATE OR REPLACE MATERIALIZED VIEW
            // significant[0]=CREATE [1]=OR [2]=REPLACE [3]=MATERIALIZED [4]=VIEW
            if significant.len() >= 5 {
                if let Token::Word(Word {
                    keyword: Keyword::VIEW,
                    ..
                }) = &significant[4].token
                {
                    return StreamingDdlKind::CreateMaterializedView { or_replace: true };
                }
            }
            StreamingDdlKind::None
        }
        _ => StreamingDdlKind::None,
    }
}

/// Check if a Word matches a keyword string (case-insensitive).
fn is_word_ci(word: &Word, keyword: &str) -> bool {
    word.value.eq_ignore_ascii_case(keyword)
}

/// Try to consume a custom keyword that may not be in sqlparser's keyword enum.
///
/// Returns `true` if the next token is a word matching `keyword` (case-insensitive)
/// and the token was consumed. Returns `false` otherwise (no token consumed).
pub fn try_parse_custom_keyword(parser: &mut Parser, keyword: &str) -> bool {
    let token = parser.peek_token();
    if let Token::Word(w) = &token.token {
        if w.value.eq_ignore_ascii_case(keyword) {
            parser.next_token();
            return true;
        }
    }
    false
}

/// Consume a custom keyword, returning an error if not found.
///
/// # Errors
///
/// Returns `ParseError::StreamingError` if the next token is not a word
/// matching `keyword`.
pub fn expect_custom_keyword(parser: &mut Parser, keyword: &str) -> Result<(), ParseError> {
    if try_parse_custom_keyword(parser, keyword) {
        Ok(())
    } else {
        let actual = parser.peek_token();
        Err(ParseError::StreamingError(format!(
            "Expected {keyword}, found {actual}"
        )))
    }
}

/// Parse WITH ('key' = 'value', ...) options.
///
/// Returns an empty map if no WITH clause is present.
/// Handles single-quoted, double-quoted, and unquoted keys and values.
///
/// # Errors
///
/// Returns `ParseError` if the WITH clause syntax is invalid.
pub fn parse_with_options(parser: &mut Parser) -> Result<HashMap<String, String>, ParseError> {
    let mut options = HashMap::new();

    if !parser.parse_keyword(Keyword::WITH) {
        return Ok(options);
    }

    parser
        .expect_token(&Token::LParen)
        .map_err(ParseError::SqlParseError)?;

    loop {
        // Check for closing paren (empty options or trailing comma)
        if parser.consume_token(&Token::RParen) {
            break;
        }

        // Parse key
        let key = parse_option_string(parser)?;

        // Check for unquoted dotted key (e.g. bind.address instead of 'bind.address')
        if parser.peek_token().token == Token::Period {
            return Err(ParseError::StreamingError(format!(
                "Config keys containing dots must be quoted, e.g. '{key}.…' — \
                 found unquoted dot after '{key}'"
            )));
        }

        // Expect '='
        parser
            .expect_token(&Token::Eq)
            .map_err(ParseError::SqlParseError)?;

        // Parse value
        let value = parse_option_string(parser)?;

        options.insert(key, value);

        // Comma or closing paren
        if !parser.consume_token(&Token::Comma) {
            parser
                .expect_token(&Token::RParen)
                .map_err(ParseError::SqlParseError)?;
            break;
        }
    }

    Ok(options)
}

/// Parse a string value for WITH options (key or value).
///
/// Accepts single-quoted strings, double-quoted strings, unquoted identifiers,
/// and numbers.
fn parse_option_string(parser: &mut Parser) -> Result<String, ParseError> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Number(n, _) => Ok(n),
        other => Err(ParseError::StreamingError(format!(
            "Expected string or identifier in WITH options, found {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::tokenizer::Tokenizer;

    fn tokenize(sql: &str) -> Vec<TokenWithSpan> {
        let dialect = GenericDialect {};
        Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .unwrap()
    }

    #[test]
    fn test_detect_create_source() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("CREATE SOURCE events (id INT)")),
            StreamingDdlKind::CreateSource { or_replace: false }
        );
    }

    #[test]
    fn test_detect_create_or_replace_source() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("CREATE OR REPLACE SOURCE events (id INT)")),
            StreamingDdlKind::CreateSource { or_replace: true }
        );
    }

    #[test]
    fn test_detect_create_sink() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("CREATE SINK output FROM events")),
            StreamingDdlKind::CreateSink { or_replace: false }
        );
    }

    #[test]
    fn test_detect_create_continuous_query() {
        assert_eq!(
            detect_streaming_ddl(&tokenize(
                "CREATE CONTINUOUS QUERY q AS SELECT * FROM events"
            )),
            StreamingDdlKind::CreateContinuousQuery { or_replace: false }
        );
    }

    #[test]
    fn test_detect_standard_sql() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("SELECT * FROM events")),
            StreamingDdlKind::None
        );
    }

    #[test]
    fn test_detect_create_table_is_not_streaming() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("CREATE TABLE events (id INT)")),
            StreamingDdlKind::None
        );
    }

    #[test]
    fn test_detect_case_insensitive() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("create source events (id int)")),
            StreamingDdlKind::CreateSource { or_replace: false }
        );
    }

    #[test]
    fn test_custom_keyword_helpers() {
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect)
            .try_with_sql("WATERMARK FOR ts")
            .unwrap();

        assert!(try_parse_custom_keyword(&mut parser, "WATERMARK"));
        // WATERMARK consumed, next should be FOR
        assert!(parser.parse_keyword(Keyword::FOR));
    }

    #[test]
    fn test_expect_custom_keyword_error() {
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect)
            .try_with_sql("SELECT * FROM t")
            .unwrap();

        let result = expect_custom_keyword(&mut parser, "WATERMARK");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_with_options_basic() {
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect)
            .try_with_sql("WITH ('connector' = 'kafka', 'topic' = 'events')")
            .unwrap();

        let options = parse_with_options(&mut parser).unwrap();
        assert_eq!(options.get("connector"), Some(&"kafka".to_string()));
        assert_eq!(options.get("topic"), Some(&"events".to_string()));
    }

    #[test]
    fn test_parse_with_options_empty() {
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql("SELECT 1").unwrap();

        let options = parse_with_options(&mut parser).unwrap();
        assert!(options.is_empty());
    }

    #[test]
    fn test_detect_drop_source() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("DROP SOURCE events")),
            StreamingDdlKind::DropSource { if_exists: false }
        );
    }

    #[test]
    fn test_detect_drop_source_if_exists() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("DROP SOURCE IF EXISTS events")),
            StreamingDdlKind::DropSource { if_exists: true }
        );
    }

    #[test]
    fn test_detect_drop_sink() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("DROP SINK output")),
            StreamingDdlKind::DropSink { if_exists: false }
        );
    }

    #[test]
    fn test_detect_drop_sink_if_exists() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("DROP SINK IF EXISTS output")),
            StreamingDdlKind::DropSink { if_exists: true }
        );
    }

    #[test]
    fn test_detect_drop_materialized_view() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("DROP MATERIALIZED VIEW live_stats")),
            StreamingDdlKind::DropMaterializedView { if_exists: false }
        );
    }

    #[test]
    fn test_detect_drop_materialized_view_if_exists() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("DROP MATERIALIZED VIEW IF EXISTS live_stats")),
            StreamingDdlKind::DropMaterializedView { if_exists: true }
        );
    }

    #[test]
    fn test_detect_show_sources() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("SHOW SOURCES")),
            StreamingDdlKind::ShowSources
        );
    }

    #[test]
    fn test_detect_show_sinks() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("SHOW SINKS")),
            StreamingDdlKind::ShowSinks
        );
    }

    #[test]
    fn test_detect_show_queries() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("SHOW QUERIES")),
            StreamingDdlKind::ShowQueries
        );
    }

    #[test]
    fn test_detect_show_materialized_views() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("SHOW MATERIALIZED VIEWS")),
            StreamingDdlKind::ShowMaterializedViews
        );
    }

    #[test]
    fn test_detect_show_tables() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("SHOW TABLES")),
            StreamingDdlKind::ShowTables
        );
    }

    #[test]
    fn test_detect_describe() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("DESCRIBE events")),
            StreamingDdlKind::DescribeSource
        );
    }

    #[test]
    fn test_detect_explain_select() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("EXPLAIN SELECT * FROM events")),
            StreamingDdlKind::ExplainStreaming
        );
    }

    #[test]
    fn test_detect_create_materialized_view() {
        assert_eq!(
            detect_streaming_ddl(&tokenize(
                "CREATE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events"
            )),
            StreamingDdlKind::CreateMaterializedView { or_replace: false }
        );
    }

    #[test]
    fn test_detect_create_or_replace_materialized_view() {
        assert_eq!(
            detect_streaming_ddl(&tokenize(
                "CREATE OR REPLACE MATERIALIZED VIEW live_stats AS SELECT COUNT(*) FROM events"
            )),
            StreamingDdlKind::CreateMaterializedView { or_replace: true }
        );
    }

    #[test]
    fn test_detect_show_case_insensitive() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("show sources")),
            StreamingDdlKind::ShowSources
        );
    }

    #[test]
    fn test_detect_drop_source_case_insensitive() {
        assert_eq!(
            detect_streaming_ddl(&tokenize("drop source events")),
            StreamingDdlKind::DropSource { if_exists: false }
        );
    }
}
