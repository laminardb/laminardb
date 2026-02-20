//! CREATE SINK parser using sqlparser primitives.
//!
//! Supported syntax:
//! ```sql
//! CREATE [OR REPLACE] SINK [IF NOT EXISTS] name
//! FROM table_name | (SELECT ...)
//! [WITH ('key' = 'value', ...)];
//! ```

use std::collections::HashMap;

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::statements::{CreateSinkStatement, FormatSpec, SinkFrom, StreamingStatement};
use super::tokenizer::{expect_custom_keyword, parse_with_options, try_parse_custom_keyword};
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
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

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
        let query = parser.parse_query().map_err(ParseError::SqlParseError)?;
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

    // Optional WHERE filter
    let filter = if parser.parse_keyword(Keyword::WHERE) {
        Some(parser.parse_expr().map_err(ParseError::SqlParseError)?)
    } else {
        None
    };

    // Optional INTO <connector> (...) clause
    let (connector_type, connector_options) = parse_into_connector(parser)?;

    // Optional FORMAT <type> [WITH (key = 'value', ...)]
    let (format, output_options) = parse_sink_format(parser)?;

    // WITH options (optional) — only if we haven't consumed format WITH already
    let with_options = if format.is_none() {
        parse_with_options(parser)?
    } else {
        HashMap::new()
    };

    Ok(CreateSinkStatement {
        name,
        from,
        with_options,
        or_replace,
        if_not_exists,
        filter,
        connector_type,
        connector_options,
        format,
        output_options,
    })
}

/// Parse optional `INTO <connector_type> (key = 'value', ...)` clause.
fn parse_into_connector(
    parser: &mut Parser,
) -> Result<(Option<String>, HashMap<String, String>), ParseError> {
    if !try_parse_custom_keyword(parser, "INTO") {
        return Ok((None, HashMap::new()));
    }

    // Connector type name (e.g., KAFKA, POSTGRES)
    let token = parser.next_token();
    let connector_type = match &token.token {
        Token::Word(w) => w.value.to_uppercase(),
        other => {
            return Err(ParseError::StreamingError(format!(
                "Expected connector type after INTO, found {other}"
            )));
        }
    };

    // Optional parenthesized options
    let options = if parser.consume_token(&Token::LParen) {
        let mut opts = HashMap::new();
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            let key = parse_sink_option_string(parser)?;
            parser
                .expect_token(&Token::Eq)
                .map_err(ParseError::SqlParseError)?;
            let value = parse_sink_option_string(parser)?;
            opts.insert(key, value);
            if !parser.consume_token(&Token::Comma) {
                parser
                    .expect_token(&Token::RParen)
                    .map_err(ParseError::SqlParseError)?;
                break;
            }
        }
        opts
    } else {
        HashMap::new()
    };

    Ok((Some(connector_type), options))
}

/// Parse optional `FORMAT <type> WITH (key = 'value', ...)` clause for sinks.
///
/// Returns `(format_spec, output_options)`.
fn parse_sink_format(
    parser: &mut Parser,
) -> Result<(Option<FormatSpec>, HashMap<String, String>), ParseError> {
    if !try_parse_custom_keyword(parser, "FORMAT") {
        return Ok((None, HashMap::new()));
    }

    // Format type name
    let token = parser.next_token();
    let format_type = match &token.token {
        Token::Word(w) => w.value.to_uppercase(),
        other => {
            return Err(ParseError::StreamingError(format!(
                "Expected format type after FORMAT, found {other}"
            )));
        }
    };

    // WITH (key = 'value', ...) — output options like key specification
    let output_options = parse_with_options(parser)?;

    Ok((
        Some(FormatSpec {
            format_type,
            options: HashMap::new(),
        }),
        output_options,
    ))
}

/// Parse a single option key or value string.
fn parse_sink_option_string(parser: &mut Parser) -> Result<String, ParseError> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Number(n, _) => Ok(n),
        other => Err(ParseError::StreamingError(format!(
            "Expected string or identifier in options, found {other}"
        ))),
    }
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
            SinkFrom::Query(_) => panic!("Expected SinkFrom::Table"),
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

    // ── INTO connector tests ────────────────────────────

    #[test]
    fn test_sink_into_kafka() {
        let sink = parse(
            "CREATE SINK alerts FROM suspicious_sessions
            INTO KAFKA (
                'bootstrap.servers' = 'localhost:9092',
                'topic' = 'alerts.fraud'
            )",
        );
        assert_eq!(sink.name.to_string(), "alerts");
        assert_eq!(sink.connector_type, Some("KAFKA".to_string()));
        assert_eq!(sink.connector_options.len(), 2);
        assert_eq!(
            sink.connector_options.get("topic"),
            Some(&"alerts.fraud".to_string())
        );
    }

    #[test]
    fn test_sink_with_where_filter() {
        let sink = parse(
            "CREATE SINK fraud_alerts FROM sessions
            WHERE severity > 5",
        );
        assert!(sink.filter.is_some());
    }

    #[test]
    fn test_sink_full_syntax() {
        let sink = parse(
            "CREATE SINK fraud_alerts
            FROM suspicious_sessions
            WHERE high_velocity_flag = 1
            INTO KAFKA (
                'bootstrap.servers' = 'localhost:9092',
                'topic' = 'alerts.fraud'
            ) FORMAT JSON WITH (key = 'session_id')",
        );
        assert_eq!(sink.name.to_string(), "fraud_alerts");
        assert!(sink.filter.is_some());
        assert_eq!(sink.connector_type, Some("KAFKA".to_string()));
        assert!(sink.format.is_some());
        assert_eq!(sink.format.as_ref().unwrap().format_type, "JSON");
        assert_eq!(sink.output_options.len(), 1);
        assert_eq!(
            sink.output_options.get("key"),
            Some(&"session_id".to_string())
        );
    }

    #[test]
    fn test_sink_format_json_no_connector() {
        let sink = parse("CREATE SINK output FROM events FORMAT JSON WITH (key = 'id')");
        assert!(sink.connector_type.is_none());
        assert!(sink.format.is_some());
        assert_eq!(sink.format.as_ref().unwrap().format_type, "JSON");
    }

    #[test]
    fn test_sink_backward_compat() {
        let sink = parse("CREATE SINK output FROM events");
        assert!(sink.filter.is_none());
        assert!(sink.connector_type.is_none());
        assert!(sink.format.is_none());
        assert!(sink.output_options.is_empty());
    }

    #[test]
    fn test_sink_into_kafka_with_format() {
        let sink = parse(
            "CREATE SINK output FROM events
            INTO KAFKA (
                'topic' = 'processed'
            ) FORMAT AVRO",
        );
        assert_eq!(sink.connector_type, Some("KAFKA".to_string()));
        assert_eq!(sink.format.as_ref().unwrap().format_type, "AVRO");
    }
}
