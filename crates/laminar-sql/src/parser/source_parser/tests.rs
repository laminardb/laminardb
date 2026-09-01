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
    assert!(matches!(wm.expression, Some(Expr::BinaryOp { .. })));
}

#[test]
fn test_create_source_with_runtime_options() {
    let source = parse(
        "CREATE SOURCE kafka_events (
                id BIGINT,
                data TEXT
            ) WITH (
                'buffer_size' = '4096'
            )",
    );
    assert_eq!(source.name.to_string(), "kafka_events");
    assert_eq!(source.columns.len(), 2);
    assert_eq!(source.with_options.len(), 1);
    assert_eq!(
        source.with_options.get("buffer_size"),
        Some(&"4096".to_string())
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
            ) FROM KAFKA (topic = 'orders') FORMAT JSON",
    );
    assert_eq!(source.name.to_string(), "orders");
    assert_eq!(source.columns.len(), 4);
    assert!(source.watermark.is_some());
    assert!(source.with_options.is_empty());
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
    assert!(matches!(wm.expression, Some(Expr::BinaryOp { .. })));
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

// ── FROM connector tests ────────────────────────────

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
fn quoted_hyphenated_connector_preserves_provider_id() {
    let source = parse(
        r#"CREATE SOURCE changes FROM "postgres-cdc" (
                host = 'localhost',
                database = 'app'
            ) SCHEMA (id BIGINT)"#,
    );

    assert_eq!(source.connector_type.as_deref(), Some("POSTGRES-CDC"));
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
                'buffer_size' = '4096'
            )",
    );
    assert_eq!(source.connector_type, Some("KAFKA".to_string()));
    assert!(source.watermark.is_some());
    assert_eq!(source.columns.len(), 3);
    assert_eq!(source.with_options.len(), 1);
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
fn test_in_memory_source_without_connector() {
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
fn test_create_source_watermark_no_expression() {
    let source = parse(
        "CREATE SOURCE events (
                id BIGINT,
                ts TIMESTAMP,
                WATERMARK FOR ts
            )",
    );
    assert!(source.watermark.is_some());
    let wm = source.watermark.as_ref().unwrap();
    assert_eq!(wm.column.to_string(), "ts");
    assert!(wm.expression.is_none());
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
                'bootstrap.servers' = 'localhost:19092',
                topic = 'market-ticks',
                'group.id' = 'laminar-demo',
                'auto.offset.reset' = 'earliest'
            ) FORMAT JSON",
    );
    assert_eq!(source.name.to_string(), "market_ticks");
    assert_eq!(source.connector_type, Some("KAFKA".to_string()));
    assert_eq!(source.columns.len(), 3);
    assert_eq!(
        source.connector_options.get("bootstrap.servers"),
        Some(&"localhost:19092".to_string())
    );
    assert_eq!(
        source.connector_options.get("topic"),
        Some(&"market-ticks".to_string())
    );
    assert_eq!(
        source.connector_options.get("group.id"),
        Some(&"laminar-demo".to_string())
    );
    assert_eq!(source.connector_options.len(), 4);
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

#[test]
fn format_requires_connector() {
    let dialect = LaminarDialect::default();
    let mut parser = Parser::new(&dialect)
        .try_with_sql("CREATE SOURCE events (id BIGINT) FORMAT JSON")
        .unwrap();
    let error = parse_create_source(&mut parser).unwrap_err().to_string();
    assert!(
        error.contains("requires an explicit FROM connector"),
        "{error}"
    );
}

#[test]
fn connector_local_format_is_rejected() {
    let dialect = LaminarDialect::default();
    let mut parser = Parser::new(&dialect)
        .try_with_sql("CREATE SOURCE events (id BIGINT) FROM KAFKA (format = 'json')")
        .unwrap();
    let error = parse_create_source(&mut parser).unwrap_err().to_string();
    assert!(
        error.contains("declare the format with the FORMAT clause"),
        "{error}"
    );
}

#[test]
fn trailing_with_accepts_only_buffer_size() {
    for option in [
        "'connector' = 'kafka'",
        "'FORMAT' = 'json'",
        "'format.schema.registry.url' = 'http://registry'",
        "'topic' = 'other-events'",
        "'buffer-size' = '4096'",
        "'buffersize' = '4096'",
        "'backpressure' = 'block'",
        "'wait_strategy' = 'park'",
        "'waitstrategy' = 'park'",
        "'track_stats' = 'true'",
        "'trackstats' = 'true'",
        "'stats' = 'true'",
    ] {
        for prefix in [
            "CREATE SOURCE events (id BIGINT)",
            "CREATE SOURCE events FROM KAFKA (topic = 'events') SCHEMA (id BIGINT)",
        ] {
            let sql = format!("{prefix} WITH ({option})");
            let dialect = LaminarDialect::default();
            let mut parser = Parser::new(&dialect).try_with_sql(&sql).unwrap();
            let error = parse_create_source(&mut parser).unwrap_err().to_string();
            assert!(error.contains("supports only 'buffer_size'"), "{error}");
        }
    }
}

#[test]
fn wildcard_schema_merging_is_rejected() {
    let dialect = LaminarDialect::default();
    let mut parser = Parser::new(&dialect)
        .try_with_sql("CREATE SOURCE events (id BIGINT, *)")
        .unwrap();
    let error = parse_create_source(&mut parser).unwrap_err().to_string();
    assert!(error.contains("omit the column list"), "{error}");
}

// ── Dotted option key tests ────────────────────────────

#[test]
fn test_dotted_option_keys_unquoted() {
    let source = parse(
        "CREATE SOURCE trades (
                s VARCHAR, p DOUBLE
            ) FROM WEBSOCKET (
                url = 'wss://example.com/ws',
                json.path = 'data',
                json.explode = 'price,qty'
            ) FORMAT JSON",
    );
    assert_eq!(source.connector_type, Some("WEBSOCKET".to_string()));
    assert_eq!(
        source.connector_options.get("json.path"),
        Some(&"data".to_string())
    );
    assert_eq!(
        source.connector_options.get("json.explode"),
        Some(&"price,qty".to_string())
    );
}

#[test]
fn test_deep_dotted_option_keys() {
    let source = parse(
        "CREATE SOURCE events FROM WEBSOCKET (
                url = 'wss://example.com',
                json.column.stream_name = 'stream',
                json.column.ts = 'meta.timestamp'
            ) SCHEMA (
                stream_name VARCHAR,
                ts BIGINT
            )",
    );
    assert_eq!(
        source.connector_options.get("json.column.stream_name"),
        Some(&"stream".to_string())
    );
    assert_eq!(
        source.connector_options.get("json.column.ts"),
        Some(&"meta.timestamp".to_string())
    );
}
