//! `DECLARE <name> [NO SCROLL] CURSOR [WITHOUT HOLD] FOR SUBSCRIBE …` parser.
//!
//! Cursors over SUBSCRIBE are forward-only and ephemeral. SCROLL/BINARY/
//! WITH HOLD/INSENSITIVE/ASENSITIVE are rejected at parse time so the
//! pgwire dispatcher can rely on a single supported shape.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use super::statements::StreamingStatement;
use super::subscribe_parser::parse_subscribe;
use super::tokenizer::{expect_custom_keyword, try_parse_custom_keyword};
use super::ParseError;

pub fn parse_declare_cursor(parser: &mut Parser) -> Result<StreamingStatement, ParseError> {
    expect_custom_keyword(parser, "DECLARE")?;

    let name = parser
        .parse_identifier()
        .map_err(ParseError::SqlParseError)?;

    // BINARY / INSENSITIVE / ASENSITIVE — rejected up-front. SCROLL is rejected
    // here too; only the `NO SCROLL` shape is allowed.
    if try_parse_custom_keyword(parser, "BINARY") {
        return Err(ParseError::StreamingError(
            "BINARY cursors are not supported on pgwire SimpleQuery (text format only)".into(),
        ));
    }
    if try_parse_custom_keyword(parser, "INSENSITIVE")
        || try_parse_custom_keyword(parser, "ASENSITIVE")
    {
        return Err(ParseError::StreamingError(
            "INSENSITIVE/ASENSITIVE cursors are not supported".into(),
        ));
    }

    let no_scroll = if parser.parse_keyword(Keyword::NO) {
        if !try_parse_custom_keyword(parser, "SCROLL") {
            return Err(ParseError::StreamingError(
                "expected SCROLL after NO".into(),
            ));
        }
        true
    } else if try_parse_custom_keyword(parser, "SCROLL") {
        return Err(ParseError::StreamingError(
            "SCROLL cursors are not supported (SUBSCRIBE is forward-only); use NO SCROLL".into(),
        ));
    } else {
        false
    };

    expect_custom_keyword(parser, "CURSOR")?;

    // WITHOUT HOLD is the default; WITH HOLD is rejected (no transaction model).
    if parser.parse_keyword(Keyword::WITH) {
        if !try_parse_custom_keyword(parser, "HOLD") {
            return Err(ParseError::StreamingError(
                "expected HOLD after WITH (cursor declaration)".into(),
            ));
        }
        return Err(ParseError::StreamingError(
            "WITH HOLD cursors are not supported".into(),
        ));
    }
    if try_parse_custom_keyword(parser, "WITHOUT") && !try_parse_custom_keyword(parser, "HOLD") {
        return Err(ParseError::StreamingError(
            "expected HOLD after WITHOUT (cursor declaration)".into(),
        ));
    }

    if !parser.parse_keyword(Keyword::FOR) {
        return Err(ParseError::StreamingError(
            "expected FOR <SUBSCRIBE …> after CURSOR".into(),
        ));
    }

    let subscribe = parse_subscribe(parser)?;

    Ok(StreamingStatement::DeclareCursorForSubscribe {
        name,
        no_scroll,
        subscribe: Box::new(subscribe),
    })
}

#[cfg(test)]
mod tests {
    use super::super::dialect::LaminarDialect;
    use super::super::statements::StreamingStatement;
    use super::*;

    fn parse(sql: &str) -> Result<StreamingStatement, ParseError> {
        let dialect = LaminarDialect::default();
        let mut parser = Parser::new(&dialect)
            .try_with_sql(sql)
            .map_err(ParseError::SqlParseError)?;
        parse_declare_cursor(&mut parser)
    }

    #[test]
    fn plain_cursor_for_subscribe() {
        let stmt = parse("DECLARE c CURSOR FOR SUBSCRIBE prices").expect("parse");
        let StreamingStatement::DeclareCursorForSubscribe {
            name,
            no_scroll,
            subscribe,
        } = stmt
        else {
            panic!("wrong variant");
        };
        assert_eq!(name.value, "c");
        assert!(!no_scroll);
        assert_eq!(subscribe.name.to_string(), "prices");
    }

    #[test]
    fn no_scroll_recognised() {
        let stmt = parse("DECLARE c NO SCROLL CURSOR FOR SUBSCRIBE prices").expect("parse");
        let StreamingStatement::DeclareCursorForSubscribe { no_scroll, .. } = stmt else {
            panic!("wrong variant");
        };
        assert!(no_scroll);
    }

    #[test]
    fn cursor_with_subscribe_filter_and_epoch() {
        let stmt =
            parse("DECLARE c CURSOR FOR SUBSCRIBE prices AS OF EPOCH 7 WHERE symbol = 'AAPL'")
                .expect("parse");
        let StreamingStatement::DeclareCursorForSubscribe { subscribe, .. } = stmt else {
            panic!("wrong variant");
        };
        assert_eq!(subscribe.as_of_epoch, Some(7));
        assert!(subscribe.filter_sql.is_some());
    }

    #[test]
    fn rejects_scroll() {
        let err = parse("DECLARE c SCROLL CURSOR FOR SUBSCRIBE prices").unwrap_err();
        assert!(matches!(err, ParseError::StreamingError(ref m) if m.contains("SCROLL")));
    }

    #[test]
    fn rejects_binary() {
        let err = parse("DECLARE c BINARY CURSOR FOR SUBSCRIBE prices").unwrap_err();
        assert!(matches!(err, ParseError::StreamingError(ref m) if m.contains("BINARY")));
    }

    #[test]
    fn rejects_with_hold() {
        let err = parse("DECLARE c CURSOR WITH HOLD FOR SUBSCRIBE prices").unwrap_err();
        assert!(matches!(err, ParseError::StreamingError(ref m) if m.contains("WITH HOLD")));
    }

    #[test]
    fn rejects_insensitive() {
        let err = parse("DECLARE c INSENSITIVE CURSOR FOR SUBSCRIBE prices").unwrap_err();
        assert!(matches!(err, ParseError::StreamingError(_)));
    }

    #[test]
    fn without_hold_accepted() {
        parse("DECLARE c CURSOR WITHOUT HOLD FOR SUBSCRIBE prices").expect("parse");
    }
}
