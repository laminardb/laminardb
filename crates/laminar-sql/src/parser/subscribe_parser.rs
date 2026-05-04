//! `SUBSCRIBE <name> [WHERE <fragment>] [WITH ('k' = 'v', ...)]` parser.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::statements::SubscribeStatement;
use super::tokenizer::{expect_custom_keyword, parse_with_options};
use super::ParseError;

pub fn parse_subscribe(parser: &mut Parser) -> Result<SubscribeStatement, ParseError> {
    expect_custom_keyword(parser, "SUBSCRIBE")?;

    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    // Validate via sqlparser then round-trip; filter_compile re-parses anyway.
    let filter_sql = if parser.parse_keyword(Keyword::WHERE) {
        let expr = parser.parse_expr().map_err(ParseError::SqlParseError)?;
        Some(expr.to_string())
    } else {
        None
    };

    let options = parse_with_options(parser)?;

    match parser.peek_token().token {
        Token::EOF | Token::SemiColon => {}
        ref other => {
            return Err(ParseError::StreamingError(format!(
                "Unexpected tokens after SUBSCRIBE target: {other}"
            )));
        }
    }

    Ok(SubscribeStatement {
        name,
        filter_sql,
        options,
    })
}

#[cfg(test)]
mod tests {
    use super::super::dialect::LaminarDialect;
    use super::*;

    fn parse(sql: &str) -> Result<SubscribeStatement, ParseError> {
        let dialect = LaminarDialect::default();
        let mut parser = Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(ParseError::SqlParseError)?;
        parse_subscribe(&mut parser)
    }

    #[test]
    fn ident_only() {
        let stmt = parse("SUBSCRIBE foo").expect("parse");
        assert_eq!(stmt.name.to_string(), "foo");
        assert!(stmt.options.is_empty());
    }

    #[test]
    fn with_options() {
        let stmt = parse("SUBSCRIBE foo WITH ('snapshot' = 'true')").expect("parse");
        assert_eq!(stmt.name.to_string(), "foo");
        assert_eq!(
            stmt.options.get("snapshot").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn trailing_semicolon_ok() {
        parse("SUBSCRIBE foo;").expect("parse");
    }

    #[test]
    fn schema_qualified() {
        let stmt = parse("SUBSCRIBE my_schema.foo").expect("parse");
        assert_eq!(stmt.name.to_string(), "my_schema.foo");
    }

    #[test]
    fn missing_target() {
        assert!(parse("SUBSCRIBE").is_err());
    }

    #[test]
    fn rejects_trailing_garbage() {
        assert!(parse("SUBSCRIBE foo bar").is_err());
    }

    #[test]
    fn rejects_empty_input() {
        assert!(parse("").is_err());
    }

    #[test]
    fn parses_where_clause() {
        let stmt = parse("SUBSCRIBE foo WHERE c > 10").expect("parse");
        assert_eq!(stmt.name.to_string(), "foo");
        assert!(stmt.filter_sql.as_deref().is_some_and(|s| s.contains('>')));
    }

    #[test]
    fn where_then_with() {
        let stmt = parse("SUBSCRIBE foo WHERE a = 1 WITH ('snapshot' = 'true')").expect("parse");
        assert!(stmt.filter_sql.is_some());
        assert_eq!(
            stmt.options.get("snapshot").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn rejects_with_before_where() {
        assert!(parse("SUBSCRIBE foo WITH ('snapshot' = 'true') WHERE c > 1").is_err());
    }
}
