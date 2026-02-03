//! CREATE SOURCE parser using sqlparser primitives.
//!
//! Replaces manual string parsing with sqlparser's `Parser` API for
//! object names, column definitions, data types, and expressions.
//!
//! Supported syntax:
//! ```sql
//! CREATE [OR REPLACE] SOURCE [IF NOT EXISTS] name (
//!     column1 TYPE [NOT NULL] [DEFAULT expr],
//!     column2 TYPE,
//!     WATERMARK FOR time_col AS time_col - INTERVAL 'n' UNIT
//! ) [WITH ('key' = 'value', ...)];
//! ```

use std::collections::HashMap;

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::statements::{CreateSourceStatement, FormatSpec, WatermarkDef};
use super::tokenizer::{expect_custom_keyword, parse_with_options, try_parse_custom_keyword};
use super::ParseError;

/// Parse a CREATE SOURCE statement from a sqlparser `Parser`.
///
/// The parser should be positioned at the start of the SQL (at the CREATE token).
/// Uses sqlparser's built-in methods for parsing identifiers, data types,
/// column definitions, and expressions.
///
/// # Errors
///
/// Returns `ParseError` if the statement syntax is invalid.
pub fn parse_create_source(parser: &mut Parser) -> Result<CreateSourceStatement, ParseError> {
    // CREATE
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(ParseError::SqlParseError)?;

    // OR REPLACE (optional)
    let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);

    // SOURCE
    expect_custom_keyword(parser, "SOURCE")?;

    // IF NOT EXISTS (optional)
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

    // Object name (handles quoted identifiers, schema-qualified names)
    let name = parser
        .parse_object_name(false)
        .map_err(ParseError::SqlParseError)?;

    // Check for FROM <connector> (...) syntax (connector-first ordering)
    let (mut connector_type, mut connector_options) = parse_from_connector(parser)?;

    // Check for FORMAT <type> syntax (after connector, before columns)
    let mut format = parse_format_clause(parser)?;

    // SCHEMA (...) or (...) for column definitions with optional WATERMARK
    // If we have a connector, columns come after FORMAT/SCHEMA; otherwise right after name
    let has_schema_keyword = try_parse_custom_keyword(parser, "SCHEMA");
    let (columns, watermark) = if has_schema_keyword || connector_type.is_none() {
        parse_source_body(parser)?
    } else {
        // If connector_type is set but no SCHEMA keyword and no paren, allow empty columns
        if let Token::LParen = parser.peek_token().token {
            parse_source_body(parser)?
        } else {
            (vec![], None)
        }
    };

    // Check for FROM <connector> (...) syntax AFTER columns (columns-first ordering).
    // Supports: CREATE SOURCE name (columns) FROM KAFKA (options)
    if connector_type.is_none() {
        let (ct, co) = parse_from_connector(parser)?;
        if ct.is_some() {
            connector_type = ct;
            connector_options = co;
        }
        if format.is_none() {
            format = parse_format_clause(parser)?;
        }
    }

    // WITH options (optional) — contains watermark config like event_time, watermark_delay
    let with_options = parse_with_options(parser)?;

    Ok(CreateSourceStatement {
        name,
        columns,
        watermark,
        with_options,
        or_replace,
        if_not_exists,
        connector_type,
        connector_options,
        format,
    })
}

/// Parse the column list and optional WATERMARK clause inside parentheses.
///
/// Uses `parser.parse_column_def()` for each column, which supports all
/// SQL data types (including parameterized types like `DECIMAL(10,2)`,
/// `VARCHAR(255)`, `ARRAY<INT>`, etc.) and column constraints (`NOT NULL`,
/// `DEFAULT`, `PRIMARY KEY`, etc.).
fn parse_source_body(
    parser: &mut Parser,
) -> Result<(Vec<sqlparser::ast::ColumnDef>, Option<WatermarkDef>), ParseError> {
    // If no opening paren, no columns defined
    if !parser.consume_token(&Token::LParen) {
        return Ok((vec![], None));
    }

    let mut columns = Vec::new();
    let mut watermark = None;

    loop {
        // Check for closing paren (empty list)
        if parser.consume_token(&Token::RParen) {
            break;
        }

        // Peek to check for WATERMARK keyword
        if try_parse_custom_keyword(parser, "WATERMARK") {
            watermark = Some(parse_watermark_def(parser)?);
        } else {
            // Parse as regular column definition using sqlparser
            let col = parser
                .parse_column_def()
                .map_err(ParseError::SqlParseError)?;
            columns.push(col);
        }

        // Expect comma or closing paren
        if !parser.consume_token(&Token::Comma) {
            parser
                .expect_token(&Token::RParen)
                .map_err(ParseError::SqlParseError)?;
            break;
        }
    }

    Ok((columns, watermark))
}

/// Parse WATERMARK FOR column AS expression.
///
/// Assumes the WATERMARK keyword has already been consumed.
/// Uses sqlparser's `parse_identifier()` for the column name and
/// `parse_expr()` for the watermark expression.
fn parse_watermark_def(parser: &mut Parser) -> Result<WatermarkDef, ParseError> {
    // FOR
    parser
        .expect_keyword(Keyword::FOR)
        .map_err(ParseError::SqlParseError)?;

    // Column name
    let column = parser
        .parse_identifier()
        .map_err(ParseError::SqlParseError)?;

    // AS
    parser
        .expect_keyword(Keyword::AS)
        .map_err(ParseError::SqlParseError)?;

    // Expression (e.g., ts - INTERVAL '5' SECOND)
    // parse_expr will consume tokens until it hits a comma, closing paren, or EOF
    let expression = parser.parse_expr().map_err(ParseError::SqlParseError)?;

    Ok(WatermarkDef { column, expression })
}

/// Parse optional `FROM <connector_type> (key = 'value', ...)` clause.
///
/// Returns `(Some(connector_type), options)` if present, or `(None, empty_map)`.
fn parse_from_connector(
    parser: &mut Parser,
) -> Result<(Option<String>, HashMap<String, String>), ParseError> {
    if !parser.parse_keyword(Keyword::FROM) {
        return Ok((None, HashMap::new()));
    }

    // Connector type name (e.g., KAFKA, POSTGRES, FILE)
    let token = parser.next_token();
    let connector_type = match &token.token {
        Token::Word(w) => w.value.to_uppercase(),
        other => {
            return Err(ParseError::StreamingError(format!(
                "Expected connector type after FROM, found {other}"
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
            let key = parse_connector_option_string(parser)?;
            parser
                .expect_token(&Token::Eq)
                .map_err(ParseError::SqlParseError)?;
            let value = parse_connector_option_string(parser)?;
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

/// Parse optional `FORMAT <type> [WITH (key = 'value', ...)]` clause.
fn parse_format_clause(parser: &mut Parser) -> Result<Option<FormatSpec>, ParseError> {
    if !try_parse_custom_keyword(parser, "FORMAT") {
        return Ok(None);
    }

    // Format type name (e.g., JSON, AVRO, PROTOBUF)
    let token = parser.next_token();
    let format_type = match &token.token {
        Token::Word(w) => w.value.to_uppercase(),
        other => {
            return Err(ParseError::StreamingError(format!(
                "Expected format type after FORMAT, found {other}"
            )));
        }
    };

    // Optional WITH (key = 'value', ...) for format-specific options
    let options = parse_with_options(parser)?;

    Ok(Some(FormatSpec {
        format_type,
        options,
    }))
}

/// Parse a single option key or value string in connector options.
fn parse_connector_option_string(parser: &mut Parser) -> Result<String, ParseError> {
    let token = parser.next_token();
    match token.token {
        Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => Ok(s),
        Token::Word(w) => Ok(w.value),
        Token::Number(n, _) => Ok(n),
        other => Err(ParseError::StreamingError(format!(
            "Expected string or identifier in connector options, found {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::dialect::LaminarDialect;
    use sqlparser::ast::{DataType, Expr};

    fn parse(sql: &str) -> CreateSourceStatement {
        let dialect = LaminarDialect::default();
        let mut parser = Parser::new(&dialect).try_with_sql(sql).unwrap();
        parse_create_source(&mut parser).unwrap()
    }

    #[test]
    fn test_basic_create_source() {
        let source = parse("CREATE SOURCE events (id BIGINT, name VARCHAR)");
        assert_eq!(source.name.to_string(), "events");
        assert_eq!(source.columns.len(), 2);
        assert_eq!(source.columns[0].name.to_string(), "id");
        assert_eq!(source.columns[1].name.to_string(), "name");
        assert!(!source.or_replace);
        assert!(!source.if_not_exists);
        assert!(source.watermark.is_none());
        assert!(source.with_options.is_empty());
    }

    #[test]
    fn test_create_source_with_watermark() {
        let source = parse(
            "CREATE SOURCE orders (
                order_id BIGINT,
                amount DECIMAL(10,2),
                order_time TIMESTAMP,
                WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
            )",
        );
        assert_eq!(source.name.to_string(), "orders");
        assert_eq!(source.columns.len(), 3);
        assert!(source.watermark.is_some());
        let wm = source.watermark.as_ref().unwrap();
        assert_eq!(wm.column.to_string(), "order_time");
        assert!(matches!(wm.expression, Expr::BinaryOp { .. }));
    }

    #[test]
    fn test_create_source_with_options() {
        let source = parse(
            "CREATE SOURCE kafka_events (
                id BIGINT,
                data TEXT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'events',
                'bootstrap.servers' = 'localhost:9092'
            )",
        );
        assert_eq!(source.name.to_string(), "kafka_events");
        assert_eq!(source.columns.len(), 2);
        assert_eq!(source.with_options.len(), 3);
        assert_eq!(
            source.with_options.get("connector"),
            Some(&"kafka".to_string())
        );
        assert_eq!(
            source.with_options.get("topic"),
            Some(&"events".to_string())
        );
        assert_eq!(
            source.with_options.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
    }

    #[test]
    fn test_create_source_full() {
        let source = parse(
            "CREATE SOURCE IF NOT EXISTS orders (
                order_id BIGINT,
                customer_id BIGINT,
                amount DECIMAL(10,2),
                order_time TIMESTAMP,
                WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'orders',
                'format' = 'json'
            )",
        );
        assert_eq!(source.name.to_string(), "orders");
        assert_eq!(source.columns.len(), 4);
        assert!(source.watermark.is_some());
        assert_eq!(source.with_options.len(), 3);
        assert!(source.if_not_exists);
        assert!(!source.or_replace);
    }

    #[test]
    fn test_create_source_or_replace() {
        let source = parse("CREATE OR REPLACE SOURCE events (id BIGINT)");
        assert!(source.or_replace);
        assert!(!source.if_not_exists);
    }

    #[test]
    fn test_data_type_parsing() {
        let source = parse(
            "CREATE SOURCE typed_source (
                col_bigint BIGINT,
                col_int INT,
                col_smallint SMALLINT,
                col_bool BOOLEAN,
                col_float FLOAT,
                col_double DOUBLE,
                col_text TEXT,
                col_varchar VARCHAR(255),
                col_timestamp TIMESTAMP,
                col_date DATE,
                col_decimal DECIMAL(10,2),
                col_json JSON
            )",
        );
        assert_eq!(source.columns.len(), 12);
        assert_eq!(source.columns[0].name.to_string(), "col_bigint");
        assert_eq!(source.columns[11].name.to_string(), "col_json");
    }

    #[test]
    fn test_schema_qualified_source_name() {
        let source = parse("CREATE SOURCE my_schema.events (id BIGINT)");
        assert_eq!(source.name.to_string(), "my_schema.events");
    }

    #[test]
    fn test_watermark_expression_parsing() {
        let source = parse(
            "CREATE SOURCE events (
                id BIGINT,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '10' MINUTE
            )",
        );
        assert!(source.watermark.is_some());
        let wm = source.watermark.as_ref().unwrap();
        assert_eq!(wm.column.to_string(), "ts");
        assert!(matches!(wm.expression, Expr::BinaryOp { .. }));
    }

    #[test]
    fn test_no_columns() {
        let dialect = LaminarDialect::default();
        let mut parser = Parser::new(&dialect)
            .try_with_sql("CREATE SOURCE events")
            .unwrap();
        let source = parse_create_source(&mut parser).unwrap();
        assert_eq!(source.columns.len(), 0);
        assert!(source.watermark.is_none());
    }

    #[test]
    fn test_tinyint_and_real_types() {
        let source = parse("CREATE SOURCE s (a TINYINT, b REAL)");
        assert_eq!(source.columns.len(), 2);
        assert!(matches!(source.columns[0].data_type, DataType::TinyInt(_)));
        assert!(matches!(source.columns[1].data_type, DataType::Real));
    }

    // ── FROM connector tests (F-SQL-001) ────────────────────────────

    #[test]
    fn test_from_kafka_connector() {
        let source = parse(
            "CREATE SOURCE clickstream FROM KAFKA (
                'bootstrap.servers' = 'localhost:9092',
                'topic' = 'ecommerce.clicks',
                'group.id' = 'laminar-demo'
            ) SCHEMA (
                event_id VARCHAR,
                user_id VARCHAR,
                ts BIGINT
            )",
        );
        assert_eq!(source.name.to_string(), "clickstream");
        assert_eq!(source.connector_type, Some("KAFKA".to_string()));
        assert_eq!(source.connector_options.len(), 3);
        assert_eq!(
            source.connector_options.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(
            source.connector_options.get("topic"),
            Some(&"ecommerce.clicks".to_string())
        );
        assert_eq!(source.columns.len(), 3);
    }

    #[test]
    fn test_from_kafka_format_json() {
        let source = parse(
            "CREATE SOURCE events FROM KAFKA (
                'topic' = 'events'
            ) FORMAT JSON SCHEMA (
                id BIGINT,
                data TEXT
            )",
        );
        assert_eq!(source.connector_type, Some("KAFKA".to_string()));
        assert!(source.format.is_some());
        assert_eq!(source.format.as_ref().unwrap().format_type, "JSON");
        assert_eq!(source.columns.len(), 2);
    }

    #[test]
    fn test_from_kafka_format_avro_with_options() {
        let source = parse(
            "CREATE SOURCE events FROM KAFKA (
                'topic' = 'events'
            ) FORMAT AVRO WITH (
                'schema.registry.url' = 'http://localhost:8081'
            ) SCHEMA (
                id BIGINT
            )",
        );
        assert_eq!(source.format.as_ref().unwrap().format_type, "AVRO");
        assert_eq!(source.format.as_ref().unwrap().options.len(), 1);
    }

    #[test]
    fn test_from_kafka_with_watermark() {
        let source = parse(
            "CREATE SOURCE orders FROM KAFKA (
                'topic' = 'orders'
            ) FORMAT JSON SCHEMA (
                order_id BIGINT,
                amount DOUBLE,
                ts TIMESTAMP,
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            ) WITH (
                'event_time' = 'ts',
                'watermark_delay' = '5 seconds'
            )",
        );
        assert_eq!(source.connector_type, Some("KAFKA".to_string()));
        assert!(source.watermark.is_some());
        assert_eq!(source.columns.len(), 3);
        assert_eq!(source.with_options.len(), 2);
    }

    #[test]
    fn test_from_postgres_connector() {
        let source = parse(
            "CREATE SOURCE users FROM POSTGRES (
                'host' = 'localhost',
                'port' = '5432',
                'database' = 'mydb'
            ) SCHEMA (
                user_id VARCHAR,
                email VARCHAR
            )",
        );
        assert_eq!(source.connector_type, Some("POSTGRES".to_string()));
        assert_eq!(source.connector_options.len(), 3);
    }

    #[test]
    fn test_backward_compat_no_connector() {
        let source = parse("CREATE SOURCE events (id BIGINT, name VARCHAR)");
        assert!(source.connector_type.is_none());
        assert!(source.connector_options.is_empty());
        assert!(source.format.is_none());
        assert_eq!(source.columns.len(), 2);
    }

    #[test]
    fn test_from_kafka_no_schema() {
        let source = parse(
            "CREATE SOURCE raw_events FROM KAFKA (
                'topic' = 'raw'
            )",
        );
        assert_eq!(source.connector_type, Some("KAFKA".to_string()));
        assert_eq!(source.columns.len(), 0);
    }

    #[test]
    fn test_columns_first_from_kafka() {
        // Columns-first ordering: CREATE SOURCE name (cols) FROM KAFKA (opts)
        let source = parse(
            "CREATE SOURCE market_ticks (
                symbol VARCHAR NOT NULL,
                price DOUBLE NOT NULL,
                ts BIGINT NOT NULL
            ) FROM KAFKA (
                brokers = 'localhost:19092',
                topic = 'market-ticks',
                group_id = 'laminar-demo',
                format = 'json',
                offset_reset = 'earliest'
            )",
        );
        assert_eq!(source.name.to_string(), "market_ticks");
        assert_eq!(source.connector_type, Some("KAFKA".to_string()));
        assert_eq!(source.columns.len(), 3);
        assert_eq!(
            source.connector_options.get("brokers"),
            Some(&"localhost:19092".to_string())
        );
        assert_eq!(
            source.connector_options.get("topic"),
            Some(&"market-ticks".to_string())
        );
        assert_eq!(
            source.connector_options.get("group_id"),
            Some(&"laminar-demo".to_string())
        );
        assert_eq!(source.connector_options.len(), 5);
    }

    #[test]
    fn test_columns_first_from_kafka_with_format() {
        let source = parse(
            "CREATE SOURCE events (
                id BIGINT,
                data VARCHAR
            ) FROM KAFKA (
                topic = 'events'
            ) FORMAT JSON",
        );
        assert_eq!(source.connector_type, Some("KAFKA".to_string()));
        assert_eq!(source.columns.len(), 2);
        assert!(source.format.is_some());
        assert_eq!(source.format.as_ref().unwrap().format_type, "JSON");
    }
}
