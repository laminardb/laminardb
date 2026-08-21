use super::*;
use crate::parser::StreamingParser;
use crate::parser::StreamingStatement;

/// Helper to parse SQL and return the first statement.
fn parse_one(sql: &str) -> StreamingStatement {
    let stmts = StreamingParser::parse_sql(sql).unwrap();
    assert_eq!(stmts.len(), 1, "Expected exactly 1 statement");
    stmts.into_iter().next().unwrap()
}

#[test]
fn test_parse_basic_create_lookup_table() {
    let stmt = parse_one(
        "CREATE LOOKUP TABLE instruments (
                symbol VARCHAR NOT NULL,
                name VARCHAR,
                PRIMARY KEY (symbol)
            ) WITH (
                'connector' = 'catalog-source',
                'endpoint' = 'https://catalog.example'
            )",
    );
    match stmt {
        StreamingStatement::CreateLookupTable(lt) => {
            assert_eq!(lt.name.to_string(), "instruments");
            assert_eq!(lt.columns.len(), 2);
            assert_eq!(lt.primary_key, vec!["symbol"]);
            assert!(!lt.or_replace);
            assert!(!lt.if_not_exists);
            assert_eq!(
                lt.with_options.get("connector"),
                Some(&"catalog-source".to_string())
            );
        }
        _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
    }
}

#[test]
fn test_parse_or_replace_and_if_not_exists() {
    let stmt = parse_one(
        "CREATE OR REPLACE LOOKUP TABLE IF NOT EXISTS dims (
                id INT,
                PRIMARY KEY (id)
            ) WITH (
                'connector' = 'static'
            )",
    );
    match stmt {
        StreamingStatement::CreateLookupTable(lt) => {
            assert!(lt.or_replace);
            assert!(lt.if_not_exists);
        }
        _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
    }
}

#[test]
fn test_parse_with_primary_key() {
    let stmt = parse_one(
        "CREATE LOOKUP TABLE t (
                a INT,
                b VARCHAR,
                c FLOAT,
                PRIMARY KEY (a, b)
            ) WITH ('connector' = 'static')",
    );
    match stmt {
        StreamingStatement::CreateLookupTable(lt) => {
            assert_eq!(lt.primary_key, vec!["a", "b"]);
            assert_eq!(lt.columns.len(), 3);
        }
        _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
    }
}

#[test]
fn test_parse_with_clause_properties() {
    let stmt = parse_one(
        "CREATE LOOKUP TABLE t (
                id INT,
                PRIMARY KEY (id)
            ) WITH (
                'connector' = 'mock-direct',
                'connection' = 'endpoint=localhost',
                'strategy' = 'on-demand',
                'cache.memory' = '512mb',
                'pushdown' = 'auto'
            )",
    );
    match stmt {
        StreamingStatement::CreateLookupTable(lt) => {
            let props = validate_properties(&lt.with_options).unwrap();
            assert_eq!(
                props.connector,
                LookupConnector::External("mock-direct".into())
            );
            assert_eq!(
                lt.with_options.get("connection").map(String::as_str),
                Some("endpoint=localhost")
            );
            assert_eq!(props.strategy, LookupStrategy::OnDemand);
            assert_eq!(props.cache_memory, Some(ByteSize(512 * 1024 * 1024)));
            assert_eq!(props.pushdown_mode, PushdownMode::Auto);
        }
        _ => panic!("Expected CreateLookupTable, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_lookup_table() {
    let stmt = parse_one("DROP LOOKUP TABLE instruments");
    match stmt {
        StreamingStatement::DropLookupTable { name, if_exists } => {
            assert_eq!(name.to_string(), "instruments");
            assert!(!if_exists);
        }
        _ => panic!("Expected DropLookupTable, got {stmt:?}"),
    }
}

#[test]
fn test_parse_drop_lookup_table_if_exists() {
    let stmt = parse_one("DROP LOOKUP TABLE IF EXISTS instruments");
    match stmt {
        StreamingStatement::DropLookupTable { name, if_exists } => {
            assert_eq!(name.to_string(), "instruments");
            assert!(if_exists);
        }
        _ => panic!("Expected DropLookupTable, got {stmt:?}"),
    }
}

#[test]
fn test_byte_size_parsing() {
    assert_eq!(
        ByteSize::parse("512mb").unwrap(),
        ByteSize(512 * 1024 * 1024)
    );
    assert_eq!(
        ByteSize::parse("1gb").unwrap(),
        ByteSize(1024 * 1024 * 1024)
    );
    assert_eq!(ByteSize::parse("10kb").unwrap(), ByteSize(10 * 1024));
    assert_eq!(ByteSize::parse("100b").unwrap(), ByteSize(100));
    assert_eq!(ByteSize::parse("1024").unwrap(), ByteSize(1024));
    assert_eq!(
        ByteSize::parse("2tb").unwrap(),
        ByteSize(2 * 1024 * 1024 * 1024 * 1024)
    );
}

#[test]
fn lookup_connector_parsing_is_provider_neutral() {
    assert_eq!(
        LookupConnector::parse(" STATIC ").unwrap(),
        LookupConnector::Static
    );
    assert_eq!(
        LookupConnector::parse("CuStOm-SrC").unwrap(),
        LookupConnector::External("custom-src".to_string())
    );
    assert_eq!(
        LookupConnector::parse("memory").unwrap(),
        LookupConnector::External("memory".to_string())
    );
    assert_eq!(
        LookupConnector::parse("alpha_lookup").unwrap(),
        LookupConnector::External("alpha_lookup".to_string())
    );
    assert!(LookupConnector::parse("  ").is_err());
}

#[test]
fn test_error_missing_columns() {
    let result =
        StreamingParser::parse_sql("CREATE LOOKUP TABLE t () WITH ('connector' = 'static')");
    assert!(result.is_err());
}

#[test]
fn test_error_missing_with_clause() {
    let result = StreamingParser::parse_sql("CREATE LOOKUP TABLE t (id INT, PRIMARY KEY (id))");
    assert!(result.is_err());
}

#[test]
fn test_error_invalid_property() {
    let mut options = HashMap::new();
    options.insert("connector".to_string(), "catalog-source".to_string());
    options.insert("strategy".to_string(), "invalid-strategy".to_string());
    let result = validate_properties(&options);
    assert!(result.is_err());
}

#[test]
fn lookup_strategy_and_cache_surface_is_strict() {
    assert_eq!(
        LookupStrategy::parse("replicated").unwrap(),
        LookupStrategy::Replicated
    );
    assert_eq!(
        LookupStrategy::parse("on-demand").unwrap(),
        LookupStrategy::OnDemand
    );
    for unsupported in [
        "partitioned",
        "sharded",
        "full",
        "poll",
        "snapshot",
        "cdc",
        "on_demand",
        "lazy",
        "manual",
    ] {
        assert!(LookupStrategy::parse(unsupported).is_err(), "{unsupported}");
    }

    let mut replicated_cache = HashMap::from([
        ("connector".to_string(), "catalog-source".to_string()),
        ("strategy".to_string(), "replicated".to_string()),
        ("cache.memory".to_string(), "1mb".to_string()),
    ]);
    assert!(validate_properties(&replicated_cache).is_err());
    replicated_cache.insert("strategy".into(), "on-demand".into());
    assert!(validate_properties(&replicated_cache).is_ok());
    replicated_cache.insert("cache.disk".into(), "1gb".into());
    assert!(validate_properties(&replicated_cache).is_err());
}
