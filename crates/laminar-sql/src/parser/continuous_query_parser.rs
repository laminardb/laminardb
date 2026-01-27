//! CREATE CONTINUOUS QUERY parser using sqlparser primitives.
//!
//! Handles the challenge of EMIT not being a sqlparser keyword by
//! splitting the SQL at the EMIT boundary before parsing.
//!
//! Supported syntax:
//! ```sql
//! CREATE [OR REPLACE] CONTINUOUS QUERY name
//! AS <select_query>
//! [EMIT <strategy>]
//! ```

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithSpan};

use super::emit_parser::parse_emit_clause;
use super::statements::StreamingStatement;
use super::tokenizer::expect_custom_keyword;
use super::ParseError;

/// Parse a CREATE CONTINUOUS QUERY statement from a sqlparser `Parser`.
///
/// Since EMIT is not a sqlparser keyword, `parse_query()` would consume
/// it as a table alias. To handle this, we:
/// 1. Parse the preamble (CREATE CONTINUOUS QUERY name AS) with the parser
/// 2. Collect remaining tokens and split at the EMIT boundary
/// 3. Parse the query portion with a fresh parser
/// 4. Parse the EMIT clause with the emit parser
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
pub fn parse_continuous_query(parser: &mut Parser) -> Result<StreamingStatement, ParseError> {
    // CREATE
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;

    // OR REPLACE (optional)
    let _or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);

    // CONTINUOUS
    expect_custom_keyword(parser, "CONTINUOUS")?;

    // QUERY
    expect_custom_keyword(parser, "QUERY")?;

    // Query name
    let name = parser.parse_object_name(false).unwrap_or_else(|_| {
        ObjectName(vec![ObjectNamePart::Identifier(Ident::new("query"))])
    });

    // AS
    parser
        .expect_keyword(Keyword::AS)
        .map_err(ParseError::SqlParseError)?;

    // Collect remaining tokens and split at EMIT boundary
    let remaining = collect_remaining_tokens(parser);
    let (query_tokens, emit_tokens) = split_at_emit(&remaining);

    // Parse the query portion
    let dialect = super::dialect::LaminarDialect::default();

    let query = if query_tokens.is_empty() {
        return Err(ParseError::StreamingError(
            "Expected SELECT query after AS".to_string(),
        ));
    } else {
        let mut query_parser =
            Parser::new(&dialect).with_tokens_with_locations(query_tokens);
        query_parser
            .parse_query()
            .map_err(ParseError::SqlParseError)?
    };

    let query_stmt = StreamingStatement::Standard(Box::new(
        sqlparser::ast::Statement::Query(query),
    ));

    // Parse optional EMIT clause from the remaining tokens
    let emit_clause = if emit_tokens.is_empty() {
        None
    } else {
        let mut emit_parser =
            Parser::new(&dialect).with_tokens_with_locations(emit_tokens);
        parse_emit_clause(&mut emit_parser)?
    };

    Ok(StreamingStatement::CreateContinuousQuery {
        name,
        query: Box::new(query_stmt),
        emit_clause,
    })
}

/// Collect all remaining tokens from the parser into a Vec.
fn collect_remaining_tokens(parser: &mut Parser) -> Vec<TokenWithSpan> {
    let mut tokens = Vec::new();
    loop {
        let token = parser.next_token();
        if token.token == Token::EOF {
            // Put EOF back and add it to our collection
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
fn split_at_emit(tokens: &[TokenWithSpan]) -> (Vec<TokenWithSpan>, Vec<TokenWithSpan>) {
    let mut depth: i32 = 0;
    for (i, token) in tokens.iter().enumerate() {
        match &token.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
            }
            Token::Word(w) if depth == 0 && w.value.eq_ignore_ascii_case("EMIT") => {
                // Split here: everything before EMIT is the query,
                // everything from EMIT onward is the emit clause
                let mut query_tokens = tokens[..i].to_vec();
                // Ensure query tokens end with EOF for parser
                query_tokens.push(TokenWithSpan {
                    token: Token::EOF,
                    span: sqlparser::tokenizer::Span::empty(),
                });
                let emit_tokens = tokens[i..].to_vec();
                return (query_tokens, emit_tokens);
            }
            _ => {}
        }
    }
    // No EMIT found - all tokens are query tokens
    (tokens.to_vec(), vec![])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::LaminarDialect;
    use crate::parser::statements::EmitClause;

    fn parse(sql: &str) -> StreamingStatement {
        let dialect = LaminarDialect::default();
        let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
        parse_continuous_query(&mut parser).unwrap()
    }

    #[test]
    fn test_basic_continuous_query() {
        let stmt = parse("CREATE CONTINUOUS QUERY live_stats AS SELECT COUNT(*) FROM events");
        match stmt {
            StreamingStatement::CreateContinuousQuery {
                name,
                emit_clause,
                ..
            } => {
                assert_eq!(name.to_string(), "live_stats");
                assert!(emit_clause.is_none());
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_emit_after_watermark() {
        let stmt = parse(
            "CREATE CONTINUOUS QUERY live_stats AS SELECT COUNT(*) FROM events EMIT AFTER WATERMARK",
        );
        match stmt {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::AfterWatermark)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_emit_on_update() {
        let stmt = parse(
            "CREATE CONTINUOUS QUERY live_stats AS SELECT COUNT(*) FROM events EMIT ON UPDATE",
        );
        match stmt {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::OnUpdate)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_emit_every() {
        let stmt = parse(
            "CREATE CONTINUOUS QUERY dashboard AS SELECT SUM(amount) FROM sales EMIT EVERY INTERVAL '30' SECOND",
        );
        match stmt {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::Periodically { .. })));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_emit_changes() {
        let stmt = parse(
            "CREATE CONTINUOUS QUERY cdc_pipeline AS SELECT * FROM orders EMIT CHANGES",
        );
        match stmt {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::Changes)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_emit_final() {
        let stmt = parse(
            "CREATE CONTINUOUS QUERY bi_report AS SELECT SUM(amount) FROM sales EMIT FINAL",
        );
        match stmt {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::Final)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_complex_query() {
        let stmt = parse(
            "CREATE CONTINUOUS QUERY enriched AS SELECT o.id, p.status FROM orders o JOIN payments p ON o.id = p.order_id EMIT ON UPDATE",
        );
        match stmt {
            StreamingStatement::CreateContinuousQuery {
                name, emit_clause, ..
            } => {
                assert_eq!(name.to_string(), "enriched");
                assert!(matches!(emit_clause, Some(EmitClause::OnUpdate)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }

    #[test]
    fn test_continuous_query_with_window() {
        let stmt = parse(
            "CREATE CONTINUOUS QUERY windowed AS SELECT COUNT(*) FROM events GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE) EMIT ON WINDOW CLOSE",
        );
        match stmt {
            StreamingStatement::CreateContinuousQuery { emit_clause, .. } => {
                assert!(matches!(emit_clause, Some(EmitClause::OnWindowClose)));
            }
            _ => panic!("Expected CreateContinuousQuery"),
        }
    }
}
