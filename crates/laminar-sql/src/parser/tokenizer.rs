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
    /// Not a streaming DDL statement
    None,
}

/// Detect which streaming DDL type (if any) the token stream represents.
///
/// Examines the first few significant tokens to determine if this is
/// a CREATE SOURCE, CREATE SINK, or CREATE CONTINUOUS QUERY statement.
/// Whitespace tokens are skipped during detection.
pub fn detect_streaming_ddl(tokens: &[TokenWithSpan]) -> StreamingDdlKind {
    let significant: Vec<&TokenWithSpan> = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();

    if significant.len() < 2 {
        return StreamingDdlKind::None;
    }

    // First token must be CREATE
    let is_create = matches!(
        &significant[0].token,
        Token::Word(Word { keyword: Keyword::CREATE, .. })
    );
    if !is_create {
        return StreamingDdlKind::None;
    }

    // Check second token for SOURCE/SINK/CONTINUOUS or OR (for OR REPLACE)
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
        Token::Word(Word {
            keyword: Keyword::OR,
            ..
        }) => {
            // Check for CREATE OR REPLACE <keyword>
            if significant.len() >= 4 {
                let is_replace = matches!(
                    &significant[2].token,
                    Token::Word(Word { keyword: Keyword::REPLACE, .. })
                );
                if is_replace {
                    return classify_after_or_replace(&significant[3].token);
                }
            }
            StreamingDdlKind::None
        }
        _ => StreamingDdlKind::None,
    }
}

/// Classify the token after CREATE OR REPLACE.
fn classify_after_or_replace(token: &Token) -> StreamingDdlKind {
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
        let mut parser = Parser::new(&dialect)
            .try_with_sql("SELECT 1")
            .unwrap();

        let options = parse_with_options(&mut parser).unwrap();
        assert!(options.is_empty());
    }
}
