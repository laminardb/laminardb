//! CREATE SINK parser using sqlparser primitives.
//!
//! Supported syntax:
//! ```sql
//! CREATE [OR REPLACE] SINK [IF NOT EXISTS] name
//! FROM table_name | (SELECT ...)
//! [WITH ('key' = 'value', ...)];
//! ```

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::statements::{CreateSinkStatement, SinkFrom, StreamingStatement};
use super::tokenizer::{expect_custom_keyword, parse_with_options};
use super::ParseError;

/// Parse a CREATE SINK statement from a sqlparser `Parser`.
///
/// The parser should be positioned at the start of the SQL (at the CREATE token).
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
pub fn parse_create_sink(parser: &mut Parser) -> Result<CreateSinkStatement, ParseError> {
    // CREATE
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;

    // OR REPLACE (optional)
    let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);

    // SINK
    expect_custom_keyword(parser, "SINK")?;

    // IF NOT EXISTS (optional)
    let if_not_exists =
        parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    // Object name
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    // FROM
    parser
        .expect_keyword(Keyword::FROM)
        .map_err(ParseError::SqlParseError)?;

    // Table reference or subquery
    let from = if parser.consume_token(&Token::LParen) {
        // Subquery: parse as SQL query
        let query = parser
            .parse_query()
            .map_err(ParseError::SqlParseError)?;
        parser
            .expect_token(&Token::RParen)
            .map_err(ParseError::SqlParseError)?;
        SinkFrom::Query(Box::new(StreamingStatement::Standard(Box::new(
            sqlparser::ast::Statement::Query(query),
        ))))
    } else {
        // Table reference
        let table = parser
            .parse_object_name(false)
            .map_err(ParseError::SqlParseError)?;
        SinkFrom::Table(table)
    };

    // WITH options (optional)
    let with_options = parse_with_options(parser)?;

    Ok(CreateSinkStatement {
        name,
        from,
        with_options,
        or_replace,
        if_not_exists,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::LaminarDialect;

    fn parse(sql: &str) -> CreateSinkStatement {
        let dialect = LaminarDialect::default();
        let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
        parse_create_sink(&mut parser).unwrap()
    }

    #[test]
    fn test_create_sink_from_table() {
        let sink = parse("CREATE SINK output_sink FROM processed_orders");
        assert_eq!(sink.name.to_string(), "output_sink");
        match &sink.from {
            SinkFrom::Table(table) => {
                assert_eq!(table.to_string(), "processed_orders");
            }
            _ => panic!("Expected SinkFrom::Table"),
        }
    }

    #[test]
    fn test_create_sink_from_query() {
        let sink = parse("CREATE SINK alerts FROM (SELECT * FROM events WHERE severity > 5)");
        assert_eq!(sink.name.to_string(), "alerts");
        assert!(matches!(sink.from, SinkFrom::Query(_)));
    }

    #[test]
    fn test_create_sink_with_options() {
        let sink = parse(
            "CREATE SINK kafka_sink FROM orders WITH (
                'connector' = 'kafka',
                'topic' = 'processed_orders',
                'format' = 'avro'
            )",
        );
        assert_eq!(sink.name.to_string(), "kafka_sink");
        assert_eq!(sink.with_options.len(), 3);
        assert_eq!(
            sink.with_options.get("connector"),
            Some(&"kafka".to_string())
        );
        assert_eq!(
            sink.with_options.get("topic"),
            Some(&"processed_orders".to_string())
        );
    }

    #[test]
    fn test_create_sink_if_not_exists() {
        let sink = parse("CREATE SINK IF NOT EXISTS my_sink FROM events");
        assert!(sink.if_not_exists);
        assert!(!sink.or_replace);
    }

    #[test]
    fn test_create_sink_or_replace() {
        let sink = parse("CREATE OR REPLACE SINK my_sink FROM events");
        assert!(sink.or_replace);
        assert!(!sink.if_not_exists);
    }
}
