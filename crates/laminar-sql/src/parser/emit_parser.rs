//! EMIT clause parser using sqlparser tokens.
//!
//! Replaces string-containment matching with token-based keyword detection.
//!
//! Supported syntax:
//! - `EMIT AFTER WATERMARK` / `EMIT ON WATERMARK`
//! - `EMIT ON WINDOW CLOSE`
//! - `EMIT ON UPDATE`
//! - `EMIT EVERY INTERVAL 'N' UNIT` / `EMIT PERIODICALLY INTERVAL 'N' UNIT`
//! - `EMIT CHANGES` (F011B)
//! - `EMIT FINAL` (F011B)

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::statements::EmitClause;
use super::tokenizer::try_parse_custom_keyword;
use super::ParseError;

/// Parse an EMIT clause from the parser's current position.
///
/// Returns `Ok(None)` if no EMIT keyword is found at the current position.
/// Uses token matching rather than string containment for robust detection.
///
/// # Errors
///
/// Returns `ParseError::StreamingError` if an EMIT keyword is found but the
/// following syntax is unrecognized.
pub fn parse_emit_clause(parser: &mut Parser) -> Result<Option<EmitClause>, ParseError> {
    // Check for EMIT keyword
    if !try_parse_custom_keyword(parser, "EMIT") {
        return Ok(None);
    }

    // Determine which variant follows
    let token = parser.peek_token();
    match &token.token {
        // EMIT AFTER WATERMARK
        Token::Word(w) if w.keyword == Keyword::AFTER => {
            parser.next_token(); // consume AFTER
            expect_watermark_keyword(parser)?;
            Ok(Some(EmitClause::AfterWatermark))
        }

        // EMIT ON ... (WATERMARK | WINDOW CLOSE | UPDATE)
        Token::Word(w) if w.keyword == Keyword::ON => {
            parser.next_token(); // consume ON
            parse_emit_on(parser)
        }

        // EMIT CHANGES (F011B)
        Token::Word(w) if w.value.eq_ignore_ascii_case("CHANGES") => {
            parser.next_token(); // consume CHANGES
            Ok(Some(EmitClause::Changes))
        }

        // EMIT FINAL (F011B)
        Token::Word(w) if w.value.eq_ignore_ascii_case("FINAL") => {
            parser.next_token(); // consume FINAL
            Ok(Some(EmitClause::Final))
        }

        // EMIT EVERY INTERVAL ... | EMIT PERIODICALLY INTERVAL ...
        Token::Word(w)
            if w.value.eq_ignore_ascii_case("EVERY")
                || w.value.eq_ignore_ascii_case("PERIODICALLY") =>
        {
            parser.next_token(); // consume EVERY/PERIODICALLY
            let interval = parser.parse_expr().map_err(ParseError::SqlParseError)?;
            Ok(Some(EmitClause::Periodically {
                interval: Box::new(interval),
            }))
        }

        _ => Err(ParseError::StreamingError(format!(
            "Unknown EMIT clause syntax after EMIT: {token}"
        ))),
    }
}

/// Parse the clause after EMIT ON (WATERMARK, WINDOW CLOSE, or UPDATE).
fn parse_emit_on(parser: &mut Parser) -> Result<Option<EmitClause>, ParseError> {
    let token = parser.peek_token();
    match &token.token {
        // EMIT ON WATERMARK
        Token::Word(w) if w.value.eq_ignore_ascii_case("WATERMARK") => {
            parser.next_token();
            Ok(Some(EmitClause::AfterWatermark))
        }

        // EMIT ON WINDOW CLOSE
        Token::Word(w) if w.keyword == Keyword::WINDOW || w.value.eq_ignore_ascii_case("WINDOW") => {
            parser.next_token(); // consume WINDOW
            expect_close_keyword(parser)?;
            Ok(Some(EmitClause::OnWindowClose))
        }

        // EMIT ON UPDATE
        Token::Word(w) if w.keyword == Keyword::UPDATE => {
            parser.next_token();
            Ok(Some(EmitClause::OnUpdate))
        }

        _ => Err(ParseError::StreamingError(format!(
            "Expected WATERMARK, WINDOW CLOSE, or UPDATE after EMIT ON, found {token}"
        ))),
    }
}

/// Expect the WATERMARK keyword (not in sqlparser's keyword enum).
fn expect_watermark_keyword(parser: &mut Parser) -> Result<(), ParseError> {
    if !try_parse_custom_keyword(parser, "WATERMARK") {
        let actual = parser.peek_token();
        return Err(ParseError::StreamingError(format!(
            "Expected WATERMARK, found {actual}"
        )));
    }
    Ok(())
}

/// Expect the CLOSE keyword after WINDOW.
fn expect_close_keyword(parser: &mut Parser) -> Result<(), ParseError> {
    if !parser.parse_keyword(Keyword::CLOSE) && !try_parse_custom_keyword(parser, "CLOSE") {
        let actual = parser.peek_token();
        return Err(ParseError::StreamingError(format!(
            "Expected CLOSE after WINDOW, found {actual}"
        )));
    }
    Ok(())
}

/// Parse an EMIT clause from a raw SQL string.
///
/// This is a convenience wrapper that tokenizes the SQL and scans for
/// an EMIT keyword, then parses the clause from that position.
/// Used for backward compatibility with existing callers that pass full SQL strings.
///
/// # Errors
///
/// Returns `ParseError::StreamingError` if the EMIT clause syntax is invalid.
pub fn parse_emit_clause_from_sql(sql: &str) -> Result<Option<EmitClause>, ParseError> {
    let sql_upper = sql.to_uppercase();

    // Quick check: if no EMIT keyword, skip tokenization
    let Some(emit_pos) = sql_upper.find("EMIT ") else {
        return Ok(None);
    };

    // Parse from the EMIT position onward
    let emit_sql = &sql[emit_pos..];
    let dialect = super::dialect::LaminarDialect::default();
    let mut parser = Parser::new(&dialect)
        .try_with_sql(emit_sql)
        .map_err(ParseError::SqlParseError)?;

    parse_emit_clause(&mut parser)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::LaminarDialect;

    fn parse_from_sql(sql: &str) -> Option<EmitClause> {
        parse_emit_clause_from_sql(sql).unwrap()
    }

    #[test]
    fn test_emit_after_watermark() {
        let emit = parse_from_sql("SELECT * EMIT AFTER WATERMARK");
        assert!(matches!(emit, Some(EmitClause::AfterWatermark)));
    }

    #[test]
    fn test_emit_on_watermark() {
        let emit = parse_from_sql("SELECT * EMIT ON WATERMARK");
        assert!(matches!(emit, Some(EmitClause::AfterWatermark)));
    }

    #[test]
    fn test_emit_on_window_close() {
        let emit = parse_from_sql("SELECT * EMIT ON WINDOW CLOSE");
        assert!(matches!(emit, Some(EmitClause::OnWindowClose)));
    }

    #[test]
    fn test_emit_on_update() {
        let emit = parse_from_sql("SELECT * EMIT ON UPDATE");
        assert!(matches!(emit, Some(EmitClause::OnUpdate)));
    }

    #[test]
    fn test_emit_every() {
        let emit = parse_from_sql("SELECT * EMIT EVERY INTERVAL '10' SECOND");
        assert!(matches!(emit, Some(EmitClause::Periodically { .. })));
    }

    #[test]
    fn test_emit_periodically() {
        let emit = parse_from_sql("SELECT * EMIT PERIODICALLY INTERVAL '5' SECOND");
        assert!(matches!(emit, Some(EmitClause::Periodically { .. })));
    }

    #[test]
    fn test_emit_changes() {
        let emit = parse_from_sql("SELECT * EMIT CHANGES");
        assert!(matches!(emit, Some(EmitClause::Changes)));
    }

    #[test]
    fn test_emit_final() {
        let emit = parse_from_sql("SELECT * EMIT FINAL");
        assert!(matches!(emit, Some(EmitClause::Final)));
    }

    #[test]
    fn test_no_emit_clause() {
        let emit = parse_from_sql("SELECT * FROM events");
        assert!(emit.is_none());
    }

    #[test]
    fn test_on_window_close_distinct_from_watermark() {
        let emit1 = parse_from_sql("SELECT * EMIT ON WINDOW CLOSE").unwrap();
        let emit2 = parse_from_sql("SELECT * EMIT AFTER WATERMARK").unwrap();
        assert_ne!(
            std::mem::discriminant(&emit1),
            std::mem::discriminant(&emit2),
            "OnWindowClose and AfterWatermark should be distinct"
        );
    }

    #[test]
    fn test_emit_from_parser_directly() {
        let dialect = LaminarDialect::default();
        let mut parser = Parser::new(&dialect)
            .try_with_sql("EMIT ON WINDOW CLOSE")
            .unwrap();
        let emit = parse_emit_clause(&mut parser).unwrap();
        assert!(matches!(emit, Some(EmitClause::OnWindowClose)));
    }
}
