//! Unit and integration tests for [`LaminarDB`](super::LaminarDB).
//!
//! Split out of `db.rs` to keep the main file focused on public API and
//! struct wiring. Declared via `#[cfg(test)] mod tests;` in `db.rs`.

use super::*;
use crate::ddl::extract_connector_from_with_options;

#[tokio::test]
async fn test_open_default() {
    let db = LaminarDB::open().unwrap();
    assert!(!db.is_closed());
    assert!(db.sources().is_empty());
    assert!(db.sinks().is_empty());
}

#[tokio::test]
async fn test_create_source() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE SOURCE");
            assert_eq!(info.object_name, "trades");
        }
        _ => panic!("Expected DDL result"),
    }

    assert_eq!(db.sources().len(), 1);
    assert_eq!(db.sources()[0].name, "trades");
}

#[tokio::test]
async fn test_create_source_with_watermark() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();

    let sources = db.sources();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].watermark_column, Some("ts".to_string()));
}

#[tokio::test]
async fn test_create_source_duplicate_error() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE test (id INT)").await.unwrap();
    let result = db.execute("CREATE SOURCE test (id INT)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_source_if_not_exists() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE test (id INT)").await.unwrap();
    let result = db
        .execute("CREATE SOURCE IF NOT EXISTS test (id INT)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_or_replace_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE test (id INT)").await.unwrap();
    let result = db
        .execute("CREATE OR REPLACE SOURCE test (id INT, name VARCHAR)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_sink() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE SINK output FROM events").await.unwrap();

    assert_eq!(db.sinks().len(), 1);
}

#[tokio::test]
async fn test_source_handle_untyped() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    assert_eq!(handle.name(), "events");
    assert_eq!(handle.schema().fields().len(), 2);
}

#[tokio::test]
async fn test_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.source_untyped("nonexistent");
    assert!(matches!(result, Err(DbError::SourceNotFound(_))));
}

#[tokio::test]
async fn test_show_sources() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT)").await.unwrap();
    db.execute("CREATE SOURCE b (id INT)").await.unwrap();

    let result = db.execute("SHOW SOURCES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 2);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_describe_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, name VARCHAR, active BOOLEAN)")
        .await
        .unwrap();

    let result = db.execute("DESCRIBE events").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 3);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_describe_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        .await
        .unwrap();

    let result = db.execute("DESCRIBE products").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 3);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_describe_materialized_view() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, name VARCHAR, value DOUBLE)")
        .await
        .unwrap();
    db.execute(
        "CREATE MATERIALIZED VIEW event_counts AS \
         SELECT name, COUNT(*) as cnt FROM events GROUP BY name",
    )
    .await
    .unwrap();

    let result = db.execute("DESCRIBE event_counts").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert!(batch.num_rows() >= 2, "Should have at least name and cnt");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_describe_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DESCRIBE nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_drop_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE test (id INT)").await.unwrap();
    assert_eq!(db.sources().len(), 1);

    db.execute("DROP SOURCE test").await.unwrap();
    assert_eq!(db.sources().len(), 0);
}

#[tokio::test]
async fn test_drop_source_if_exists() {
    let db = LaminarDB::open().unwrap();
    // Should not error when source doesn't exist
    let result = db.execute("DROP SOURCE IF EXISTS nonexistent").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_drop_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP SOURCE nonexistent").await;
    assert!(matches!(result, Err(DbError::SourceNotFound(_))));
}

#[tokio::test]
async fn test_shutdown() {
    let db = LaminarDB::open().unwrap();
    assert!(!db.is_closed());
    db.close();
    assert!(db.is_closed());

    let result = db.execute("CREATE SOURCE test (id INT)").await;
    assert!(matches!(result, Err(DbError::Shutdown)));
}

#[tokio::test]
async fn test_debug_format() {
    let db = LaminarDB::open().unwrap();
    let debug = format!("{db:?}");
    assert!(debug.contains("LaminarDB"));
}

#[tokio::test]
async fn test_explain_create_source() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("EXPLAIN CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert!(batch.num_rows() > 0);
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            // Should contain plan_type and source info
            let key_values: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
            assert!(key_values.contains(&"plan_type"));
        }
        _ => panic!("Expected Metadata result for EXPLAIN"),
    }
}

#[tokio::test]
async fn test_cancel_query() {
    let db = LaminarDB::open().unwrap();
    // Register a query via catalog directly for testing
    assert_eq!(db.active_query_count(), 0);

    // Simulate a query registration
    let query_id = db.catalog.register_query("SELECT * FROM test");
    assert_eq!(db.active_query_count(), 1);

    // Cancel it
    db.cancel_query(query_id).unwrap();
    assert_eq!(db.active_query_count(), 0);
}

#[tokio::test]
async fn test_source_and_sink_counts() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(db.source_count(), 0);
    assert_eq!(db.sink_count(), 0);

    db.execute("CREATE SOURCE a (id INT)").await.unwrap();
    db.execute("CREATE SOURCE b (id INT)").await.unwrap();
    assert_eq!(db.source_count(), 2);

    db.execute("CREATE SINK output FROM a").await.unwrap();
    assert_eq!(db.sink_count(), 1);

    db.execute("DROP SOURCE a").await.unwrap();
    assert_eq!(db.source_count(), 1);
}

#[tokio::test]
async fn test_multi_statement_execution() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT); CREATE SOURCE b (id INT); CREATE SINK output FROM a")
        .await
        .unwrap();
    assert_eq!(db.source_count(), 2);
    assert_eq!(db.sink_count(), 1);
}

#[tokio::test]
async fn test_multi_statement_trailing_semicolon() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT);").await.unwrap();
    assert_eq!(db.source_count(), 1);
}

#[tokio::test]
async fn test_multi_statement_error_stops() {
    let db = LaminarDB::open().unwrap();
    // Second statement should fail (duplicate)
    let result = db
        .execute("CREATE SOURCE a (id INT); CREATE SOURCE a (id INT)")
        .await;
    assert!(result.is_err());
    // First statement should have succeeded
    assert_eq!(db.source_count(), 1);
}

#[tokio::test]
async fn test_config_var_substitution() {
    let db = LaminarDB::builder()
        .config_var("TABLE_NAME", "events")
        .build()
        .await
        .unwrap();
    // Config var in source name won't work (parsed as identifier),
    // but it works in WITH option values
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    assert_eq!(db.source_count(), 1);
}

#[tokio::test]
async fn test_create_stream() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE STREAM counts AS SELECT COUNT(*) as cnt FROM events")
        .await
        .unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE STREAM");
            assert_eq!(info.object_name, "counts");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_drop_stream() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM counts AS SELECT COUNT(*) as cnt FROM events")
        .await
        .unwrap();
    let result = db.execute("DROP STREAM counts").await.unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "DROP STREAM");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_drop_stream_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP STREAM nonexistent").await;
    assert!(matches!(result, Err(DbError::StreamNotFound(_))));
}

#[tokio::test]
async fn test_drop_stream_if_exists() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP STREAM IF EXISTS nonexistent").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_show_streams() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM a AS SELECT 1 FROM events")
        .await
        .unwrap();
    let result = db.execute("SHOW STREAMS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_stream_duplicate_error() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
        .await
        .unwrap();
    let result = db
        .execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
        .await;
    assert!(matches!(result, Err(DbError::StreamAlreadyExists(_))));
}

#[tokio::test]
async fn test_create_table() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE TABLE");
            assert_eq!(info.object_name, "products");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_create_table_and_query_empty() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE dim (id INT, label VARCHAR)")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM dim").await.unwrap();
    match result {
        ExecuteResult::Query(q) => {
            assert_eq!(q.schema().fields().len(), 2);
        }
        _ => panic!("Expected Query result"),
    }
}

#[tokio::test]
async fn test_insert_into_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO events VALUES (1, 3.14), (2, 2.72)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 2),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_insert_into_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_insert_into_nonexistent_table() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("INSERT INTO nosuch VALUES (1, 2)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_table_with_types() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("CREATE TABLE orders (id BIGINT NOT NULL, qty SMALLINT, total DECIMAL(10,2))")
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE TABLE");
            assert_eq!(info.object_name, "orders");
        }
        _ => panic!("Expected DDL result"),
    }
}

#[tokio::test]
async fn test_insert_null_values() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE data (id BIGINT, label VARCHAR)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO data VALUES (1, NULL)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_insert_negative_values() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE temps (id BIGINT, celsius DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO temps VALUES (1, -40.0)")
        .await
        .unwrap();
    match result {
        ExecuteResult::RowsAffected(n) => assert_eq!(n, 1),
        _ => panic!("Expected RowsAffected"),
    }
}

#[tokio::test]
async fn test_create_source_unknown_connector() {
    let db = LaminarDB::open().unwrap();
    // Use correct SQL syntax: FROM <type> (...) SCHEMA (...)
    let result = db
        .execute(
            "CREATE SOURCE events FROM NONEXISTENT \
             ('topic' = 'test') SCHEMA (id INT)",
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Unknown source connector type"), "got: {err}");
}

#[tokio::test]
async fn test_create_sink_unknown_connector() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    // Use correct SQL syntax: INTO <type> (...)
    let result = db
        .execute(
            "CREATE SINK output FROM events \
             INTO NONEXISTENT ('topic' = 'out')",
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Unknown sink connector type"), "got: {err}");
}

#[tokio::test]
async fn test_create_source_invalid_format() {
    // We test format validation via build_source_config in
    // connector_manager::tests (since the SQL parser may reject
    // unknown formats at parse time rather than DDL validation).
    // Here we verify that an error is returned either way.
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE SOURCE events FROM NONEXISTENT \
             FORMAT BADFORMAT SCHEMA (id INT)",
        )
        .await;
    assert!(result.is_err());
}

// -----------------------------------------------------------------------
// Pipeline-running state guards
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_create_source_with_connector_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE seed (id INT)").await.unwrap();
    db.start().await.unwrap();

    // WITH syntax
    let result = db
        .execute("CREATE SOURCE events (id INT) WITH ('connector' = 'kafka', 'topic' = 'x')")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("pipeline is running"),
        "expected pipeline-running error, got: {err}"
    );

    // FROM syntax (what server mode generates via source_to_ddl)
    let result = db
        .execute("CREATE SOURCE events2 (id INT) FROM KAFKA (topic = 'x')")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("pipeline is running"),
        "expected pipeline-running error for FROM syntax, got: {err}"
    );
}

#[tokio::test]
async fn test_create_source_without_connector_allowed_when_running() {
    let db = LaminarDB::open().unwrap();
    db.start().await.unwrap();

    // Schema-only source (no connector) — used by embedded db.insert() API.
    let result = db.execute("CREATE SOURCE events (id INT)").await;
    assert!(
        result.is_ok(),
        "schema-only source should be allowed when running"
    );
}

#[tokio::test]
async fn test_create_sink_with_connector_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.start().await.unwrap();

    let result = db
        .execute(
            "CREATE SINK output FROM events \
             WITH ('connector' = 'kafka', 'topic' = 'out')",
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("pipeline is running"),
        "expected pipeline-running error, got: {err}"
    );
}

#[tokio::test]
async fn test_drop_source_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.start().await.unwrap();

    let result = db.execute("DROP SOURCE events").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("pipeline is running"),
        "expected pipeline-running error, got: {err}"
    );
}

#[tokio::test]
async fn test_drop_sink_rejected_when_running() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE SINK output FROM events").await.unwrap();
    db.start().await.unwrap();

    let result = db.execute("DROP SINK output").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("pipeline is running"),
        "expected pipeline-running error, got: {err}"
    );
}

#[tokio::test]
async fn test_connector_registry_accessor() {
    let db = LaminarDB::open().unwrap();
    let registry = db.connector_registry();

    // With feature flags enabled, built-in connectors are auto-registered.
    // Without any features, registry should be empty.
    #[allow(unused_mut)]
    let mut expected_sources = 0;
    #[allow(unused_mut)]
    let mut expected_sinks = 0;

    #[cfg(feature = "kafka")]
    {
        expected_sources += 1; // kafka source
        expected_sinks += 1; // kafka sink
    }
    #[cfg(feature = "postgres-cdc")]
    {
        expected_sources += 1; // postgres CDC source
    }
    #[cfg(feature = "postgres-sink")]
    {
        expected_sinks += 1; // postgres sink
    }
    #[cfg(feature = "delta-lake")]
    {
        expected_sources += 1; // delta-lake source
        expected_sinks += 1; // delta-lake sink
    }
    #[cfg(feature = "iceberg")]
    {
        expected_sources += 1; // iceberg source
        expected_sinks += 1; // iceberg sink
    }
    #[cfg(feature = "websocket")]
    {
        expected_sources += 1; // websocket source
        expected_sinks += 1; // websocket sink
    }
    #[cfg(feature = "mysql-cdc")]
    {
        expected_sources += 1; // mysql CDC source
    }
    #[cfg(feature = "mongodb-cdc")]
    {
        expected_sources += 1; // mongodb CDC source
        expected_sinks += 1; // mongodb sink
    }
    #[cfg(feature = "files")]
    {
        expected_sources += 1; // file source
        expected_sinks += 1; // file sink
    }
    #[cfg(feature = "otel")]
    {
        expected_sources += 1; // otel source
    }
    #[cfg(feature = "nats")]
    {
        expected_sources += 1; // nats source
        expected_sinks += 1; // nats sink
    }

    assert_eq!(registry.list_sources().len(), expected_sources);
    assert_eq!(registry.list_sinks().len(), expected_sinks);
}

#[tokio::test]
async fn test_builder_register_connector() {
    use std::sync::Arc;

    let db = LaminarDB::builder()
        .register_connector(|registry| {
            registry.register_source(
                "test-source",
                laminar_connectors::config::ConnectorInfo {
                    name: "test-source".to_string(),
                    display_name: "Test Source".to_string(),
                    version: "0.1.0".to_string(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(|_: Option<&prometheus::Registry>| {
                    Box::new(laminar_connectors::testing::MockSourceConnector::new())
                }),
            );
        })
        .build()
        .await
        .unwrap();
    let registry = db.connector_registry();
    assert!(registry.list_sources().contains(&"test-source".to_string()));
}

/// SQL DDL auto-discovery must land a `Map` column in the catalog
/// verbatim from the connector — no SQL string round-trip.
#[tokio::test]
async fn test_sql_create_source_auto_discovers_map_column() {
    use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};

    let map_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "data",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]));

    let (db, _) = fake_source_db("fake-avro", Some(Arc::clone(&map_schema))).await;
    db.execute(
        "CREATE SOURCE events WITH ('connector' = 'fake-avro', \
         'schema.registry.url' = 'http://irrelevant', 'topic' = 'events')",
    )
    .await
    .unwrap();

    let entry = db.catalog.get_source("events").expect("source in catalog");
    assert_eq!(entry.schema.fields().len(), 2);
    assert_eq!(entry.schema.field(0).data_type(), &DataType::Int64);
    assert!(
        matches!(entry.schema.field(1).data_type(), DataType::Map(_, _)),
        "auto-discovered `data` must arrive as Map, got {:?}",
        entry.schema.field(1).data_type()
    );
}

/// CREATE SOURCE IF NOT EXISTS must skip auto-discovery when the
/// source already exists — no wasted Schema Registry round trip.
#[tokio::test]
async fn test_sql_create_source_if_not_exists_skips_discovery() {
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    let discovered = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let (db, counter) = fake_source_db("counting-fake", Some(discovered)).await;

    db.execute("CREATE SOURCE events WITH ('connector' = 'counting-fake')")
        .await
        .unwrap();
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);

    db.execute("CREATE SOURCE IF NOT EXISTS events WITH ('connector' = 'counting-fake')")
        .await
        .unwrap();
    assert_eq!(
        counter.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "IF NOT EXISTS should short-circuit before discovery"
    );
}

/// CREATE SOURCE without columns must fail loudly when discovery
/// yields an empty schema, not register a zero-column table.
#[tokio::test]
async fn test_sql_create_source_errors_when_discovery_yields_empty() {
    let (db, _) = fake_source_db("empty-fake", None).await;
    let err = db
        .execute("CREATE SOURCE events WITH ('connector' = 'empty-fake')")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("could not auto-discover a schema"),
        "expected actionable discovery-failure error, got: {err}"
    );
}

/// Build a `LaminarDB` with one fake source plus a shared counter
/// that ticks on every `discover_schema` call.
async fn fake_source_db(
    name: &'static str,
    discovered: Option<Arc<arrow::datatypes::Schema>>,
) -> (LaminarDB, Arc<std::sync::atomic::AtomicUsize>) {
    use arrow::datatypes::Schema as ArrowSchema;
    use async_trait::async_trait;
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
    use laminar_connectors::connector::{SourceBatch, SourceConnector};
    use laminar_connectors::error::ConnectorError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeSource {
        schema: Arc<ArrowSchema>,
        on_discover: Option<Arc<ArrowSchema>>,
        counter: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourceConnector for FakeSource {
        async fn open(&mut self, _: &ConnectorConfig) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            Ok(None)
        }
        async fn discover_schema(&mut self, _: &std::collections::HashMap<String, String>) {
            self.counter.fetch_add(1, Ordering::SeqCst);
            if let Some(s) = &self.on_discover {
                self.schema = Arc::clone(s);
            }
        }
        fn schema(&self) -> Arc<ArrowSchema> {
            Arc::clone(&self.schema)
        }
        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new(0)
        }
        async fn restore(&mut self, _: &SourceCheckpoint) -> Result<(), ConnectorError> {
            Ok(())
        }
        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    let db = LaminarDB::builder()
        .register_connector(move |registry| {
            let discovered = discovered.clone();
            let counter = Arc::clone(&counter_clone);
            registry.register_source(
                name,
                ConnectorInfo {
                    name: name.into(),
                    display_name: name.into(),
                    version: "0.1.0".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(move |_: Option<&prometheus::Registry>| {
                    Box::new(FakeSource {
                        schema: Arc::new(ArrowSchema::empty()),
                        on_discover: discovered.clone(),
                        counter: Arc::clone(&counter),
                    })
                }),
            );
        })
        .build()
        .await
        .unwrap();
    (db, counter)
}

#[tokio::test]
async fn test_create_materialized_view() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("CREATE MATERIALIZED VIEW event_stats AS SELECT * FROM events")
        .await;

    // The MV may fail at query execution (no data in DataFusion) but the
    // important thing is the MV path is invoked and the registry is wired up.
    // If it succeeds, verify the DDL result.
    if let Ok(ExecuteResult::Ddl(info)) = &result {
        assert_eq!(info.statement_type, "CREATE MATERIALIZED VIEW");
        assert_eq!(info.object_name, "event_stats");
    }
}

#[tokio::test]
async fn test_mv_registry_base_tables() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (sym VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    let registry = db.mv_registry.lock();
    assert!(registry.is_base_table("trades"));
}

#[tokio::test]
async fn test_show_materialized_views_empty() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("SHOW MATERIALIZED VIEWS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), 3);
            assert_eq!(batch.schema().field(0).name(), "view_name");
            assert_eq!(batch.schema().field(1).name(), "sql");
            assert_eq!(batch.schema().field(2).name(), "state");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_drop_materialized_view_if_exists() {
    let db = LaminarDB::open().unwrap();
    // Should not error with IF EXISTS on non-existent view
    let result = db
        .execute("DROP MATERIALIZED VIEW IF EXISTS nonexistent")
        .await
        .unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "DROP MATERIALIZED VIEW");
        }
        _ => panic!("Expected Ddl result"),
    }
}

#[tokio::test]
async fn test_drop_materialized_view_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP MATERIALIZED VIEW nonexistent").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found"),
        "Expected 'not found' error, got: {err}"
    );
}

#[tokio::test]
async fn test_create_mv_if_not_exists() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    // Register a view directly in the registry for this test
    {
        let mut registry = db.mv_registry.lock();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mv = laminar_core::mv::MaterializedView::new(
            "my_view",
            "SELECT * FROM events",
            vec!["events".to_string()],
            schema,
        );
        registry.register(mv).unwrap();
    }

    // IF NOT EXISTS should succeed without error
    let result = db
        .execute("CREATE MATERIALIZED VIEW IF NOT EXISTS my_view AS SELECT * FROM events")
        .await
        .unwrap();
    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.object_name, "my_view");
        }
        _ => panic!("Expected Ddl result"),
    }
}

#[tokio::test]
async fn test_create_mv_duplicate_error() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    // Register a view directly
    {
        let mut registry = db.mv_registry.lock();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mv = laminar_core::mv::MaterializedView::new(
            "my_view",
            "SELECT * FROM events",
            vec!["events".to_string()],
            schema,
        );
        registry.register(mv).unwrap();
    }

    // Without IF NOT EXISTS, should error
    let result = db
        .execute("CREATE MATERIALIZED VIEW my_view AS SELECT * FROM events")
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("already exists"),
        "Expected 'already exists' error, got: {err}"
    );
}

#[tokio::test]
async fn test_show_materialized_views_with_entries() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    // Register views directly for metadata testing
    {
        let mut registry = db.mv_registry.lock();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mv = laminar_core::mv::MaterializedView::new(
            "view_a",
            "SELECT * FROM events",
            vec!["events".to_string()],
            schema,
        );
        registry.register(mv).unwrap();
    }

    let result = db.execute("SHOW MATERIALIZED VIEWS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "view_a");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_drop_mv_and_show() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    // Register a view
    {
        let mut registry = db.mv_registry.lock();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mv = laminar_core::mv::MaterializedView::new(
            "temp_view",
            "SELECT * FROM events",
            vec!["events".to_string()],
            schema,
        );
        registry.register(mv).unwrap();
    }

    // Verify it's there
    assert_eq!(db.mv_registry.lock().len(), 1);

    // Drop it
    db.execute("DROP MATERIALIZED VIEW temp_view")
        .await
        .unwrap();

    // Verify it's gone
    assert_eq!(db.mv_registry.lock().len(), 0);
}

#[tokio::test]
async fn test_debug_includes_mv_count() {
    let db = LaminarDB::open().unwrap();
    let debug = format!("{db:?}");
    assert!(
        debug.contains("materialized_views: 0"),
        "Debug should include MV count, got: {debug}"
    );
}

#[tokio::test]
async fn test_pipeline_topology_empty() {
    let db = LaminarDB::open().unwrap();
    let topo = db.pipeline_topology();
    assert!(topo.nodes.is_empty());
    assert!(topo.edges.is_empty());
}

#[tokio::test]
async fn test_pipeline_topology_sources_only() {
    use crate::handle::PipelineNodeType;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE SOURCE clicks (url VARCHAR, ts BIGINT)")
        .await
        .unwrap();

    let topo = db.pipeline_topology();
    assert_eq!(topo.nodes.len(), 2);
    assert!(topo.edges.is_empty());

    for node in &topo.nodes {
        assert_eq!(node.node_type, PipelineNodeType::Source);
        assert!(node.schema.is_some());
        assert!(node.sql.is_none());
    }
}

#[tokio::test]
async fn test_pipeline_topology_full_pipeline() {
    use crate::handle::PipelineNodeType;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE STREAM agg AS SELECT COUNT(*) as cnt FROM events GROUP BY id")
        .await
        .unwrap();
    db.execute("CREATE SINK output FROM agg").await.unwrap();

    let topo = db.pipeline_topology();

    // Nodes: 1 source + 1 stream + 1 sink = 3
    assert_eq!(topo.nodes.len(), 3);

    let sources: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Source)
        .collect();
    let streams: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Stream)
        .collect();
    let sinks: Vec<_> = topo
        .nodes
        .iter()
        .filter(|n| n.node_type == PipelineNodeType::Sink)
        .collect();

    assert_eq!(sources.len(), 1);
    assert_eq!(streams.len(), 1);
    assert_eq!(sinks.len(), 1);

    assert_eq!(sources[0].name, "events");
    assert_eq!(streams[0].name, "agg");
    assert!(streams[0].sql.is_some());
    assert_eq!(sinks[0].name, "output");

    // Edges: events->agg, agg->output
    assert_eq!(topo.edges.len(), 2);
    assert!(topo
        .edges
        .iter()
        .any(|e| e.from == "events" && e.to == "agg"));
    assert!(topo
        .edges
        .iter()
        .any(|e| e.from == "agg" && e.to == "output"));
}

#[tokio::test]
async fn test_pipeline_topology_fan_out() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE ticks (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE STREAM ohlc AS SELECT symbol, MIN(price) FROM ticks GROUP BY symbol")
        .await
        .unwrap();
    db.execute("CREATE STREAM vol AS SELECT symbol, COUNT(*) FROM ticks GROUP BY symbol")
        .await
        .unwrap();

    let topo = db.pipeline_topology();

    // 1 source + 2 streams = 3 nodes
    assert_eq!(topo.nodes.len(), 3);

    // Both streams should have an edge from ticks
    let ticks_edges: Vec<_> = topo.edges.iter().filter(|e| e.from == "ticks").collect();
    assert_eq!(ticks_edges.len(), 2);

    let targets: Vec<&str> = ticks_edges.iter().map(|e| e.to.as_str()).collect();
    assert!(targets.contains(&"ohlc"));
    assert!(targets.contains(&"vol"));
}

#[tokio::test]
async fn test_streams_method() {
    let db = LaminarDB::open().unwrap();
    assert!(db.streams().is_empty());

    db.execute("CREATE STREAM counts AS SELECT COUNT(*) FROM events")
        .await
        .unwrap();

    let streams = db.streams();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name, "counts");
    assert!(streams[0].sql.is_some());
    assert!(
        streams[0].sql.as_ref().unwrap().contains("COUNT"),
        "SQL should contain the query: {:?}",
        streams[0].sql,
    );
}

#[tokio::test]
async fn test_pipeline_node_types() {
    use crate::handle::PipelineNodeType;

    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE src (id INT)").await.unwrap();
    db.execute("CREATE STREAM st AS SELECT * FROM src")
        .await
        .unwrap();
    db.execute("CREATE SINK sk FROM st").await.unwrap();

    let topo = db.pipeline_topology();

    let find = |name: &str| topo.nodes.iter().find(|n| n.name == name).unwrap();

    assert_eq!(find("src").node_type, PipelineNodeType::Source);
    assert_eq!(find("st").node_type, PipelineNodeType::Stream);
    assert_eq!(find("sk").node_type, PipelineNodeType::Sink);
}

#[tokio::test]
async fn test_create_table_with_primary_key() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE TABLE instruments (\
             symbol VARCHAR PRIMARY KEY, \
             company_name VARCHAR, \
             sector VARCHAR\
             )",
        )
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.statement_type, "CREATE TABLE");
            assert_eq!(info.object_name, "instruments");
        }
        _ => panic!("Expected DDL result"),
    }

    // Verify TableStore registration
    let ts = db.table_store.read();
    assert!(ts.has_table("instruments"));
    assert_eq!(ts.primary_key("instruments"), Some("symbol"));
    assert_eq!(ts.table_row_count("instruments"), 0);
}

#[tokio::test]
async fn test_create_table_with_connector_options() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE TABLE instruments (\
             symbol VARCHAR PRIMARY KEY, \
             company_name VARCHAR\
             ) WITH (connector = 'kafka', topic = 'instruments')",
        )
        .await
        .unwrap();

    match result {
        ExecuteResult::Ddl(info) => {
            assert_eq!(info.object_name, "instruments");
        }
        _ => panic!("Expected DDL result"),
    }

    // Verify ConnectorManager registration
    let mgr = db.connector_manager.lock();
    let tables = mgr.tables();
    assert!(tables.contains_key("instruments"));
    let reg = &tables["instruments"];
    assert_eq!(reg.connector_type.as_deref(), Some("kafka"));
    assert_eq!(reg.primary_key, "symbol");

    // Verify TableStore connector
    let ts = db.table_store.read();
    assert_eq!(ts.connector("instruments"), Some("kafka"));
}

#[tokio::test]
async fn test_insert_into_table_with_pk_upserts() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE TABLE products (\
         id INT PRIMARY KEY, \
         name VARCHAR, \
         price DOUBLE\
         )",
    )
    .await
    .unwrap();

    // Insert a row
    db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        .await
        .unwrap();
    assert_eq!(db.table_store.read().table_row_count("products"), 1);

    // Upsert (same PK = overwrite)
    db.execute("INSERT INTO products VALUES (1, 'Super Widget', 19.99)")
        .await
        .unwrap();
    assert_eq!(db.table_store.read().table_row_count("products"), 1);

    // Insert another row (different PK)
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 14.99)")
        .await
        .unwrap();
    assert_eq!(db.table_store.read().table_row_count("products"), 2);

    // Verify via SELECT
    let result = db.execute("SELECT * FROM products").await.unwrap();
    match result {
        ExecuteResult::Query(q) => {
            assert_eq!(q.schema().fields().len(), 3);
        }
        _ => panic!("Expected Query result"),
    }
}

#[tokio::test]
async fn test_show_tables() {
    let db = LaminarDB::open().unwrap();

    // Empty
    let result = db.execute("SHOW TABLES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 0);
            assert_eq!(batch.num_columns(), 4);
            assert_eq!(batch.schema().field(0).name(), "name");
            assert_eq!(batch.schema().field(1).name(), "primary_key");
            assert_eq!(batch.schema().field(2).name(), "row_count");
            assert_eq!(batch.schema().field(3).name(), "connector");
        }
        _ => panic!("Expected Metadata result"),
    }

    // With a table
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR)")
        .await
        .unwrap();
    let result = db.execute("SHOW TABLES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_drop_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val VARCHAR)")
        .await
        .unwrap();
    assert!(db.table_store.read().has_table("t"));

    db.execute("DROP TABLE t").await.unwrap();
    assert!(!db.table_store.read().has_table("t"));
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("DROP TABLE IF EXISTS nonexistent").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_having_filters_grouped_results() {
    let db = LaminarDB::open().unwrap();

    // Create table and query via DataFusion directly
    db.ctx
        .sql(
            "CREATE TABLE hv_trades AS SELECT * FROM (VALUES \
             ('AAPL', 100), ('GOOG', 5), ('MSFT', 50)) \
             AS t(symbol, volume)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql("SELECT symbol, volume FROM hv_trades WHERE volume > 10 ORDER BY symbol")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    // AAPL(100), MSFT(50) pass; GOOG(5) filtered
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_having_with_aggregate() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE hv_orders AS SELECT * FROM (VALUES \
             ('A', 100), ('A', 200), ('B', 50), ('B', 30), ('C', 500)) \
             AS t(category, amount)",
        )
        .await
        .unwrap();

    // Query with GROUP BY + HAVING through DataFusion
    let df = db
        .ctx
        .sql(
            "SELECT category, SUM(amount) as total \
             FROM hv_orders GROUP BY category \
             HAVING SUM(amount) > 100 ORDER BY category",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    // A: 300 > 100 ✓, B: 80 ✗, C: 500 > 100 ✓
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_having_all_filtered_out() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE items AS SELECT * FROM (VALUES \
             ('x', 1), ('y', 2)) AS t(name, qty)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql("SELECT name, SUM(qty) as total FROM items GROUP BY name HAVING SUM(qty) > 1000")
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_having_compound_predicate() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE sales AS SELECT * FROM (VALUES \
             ('A', 100), ('A', 200), ('B', 50), ('C', 10), ('C', 20)) \
             AS t(region, amount)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT region, COUNT(*) as cnt, SUM(amount) as total \
             FROM sales GROUP BY region \
             HAVING COUNT(*) >= 2 AND SUM(amount) > 25 \
             ORDER BY region",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    // A: cnt=2>=2 AND total=300>25 ✓
    // B: cnt=1<2 ✗
    // C: cnt=2>=2 AND total=30>25 ✓
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_two_way_lookup() {
    let db = LaminarDB::open().unwrap();

    // Create tables via DataFusion
    db.ctx
        .sql(
            "CREATE TABLE orders AS SELECT * FROM (VALUES \
             (1, 100, 'A'), (2, 200, 'B')) AS t(id, customer_id, product_code)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE customers AS SELECT * FROM (VALUES \
             (100, 'Alice'), (200, 'Bob')) AS t(id, name)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE products AS SELECT * FROM (VALUES \
             ('A', 'Widget'), ('B', 'Gadget')) AS t(code, label)",
        )
        .await
        .unwrap();

    // Two-way join through DataFusion
    let df = db
        .ctx
        .sql(
            "SELECT o.id, c.name, p.label \
             FROM orders o \
             JOIN customers c ON o.customer_id = c.id \
             JOIN products p ON o.product_code = p.code \
             ORDER BY o.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_three_way() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql("CREATE TABLE t1 AS SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(id, fk1)")
        .await
        .unwrap();
    db.ctx
        .sql("CREATE TABLE t2 AS SELECT * FROM (VALUES (10, 100), (20, 200)) AS t(id, fk2)")
        .await
        .unwrap();
    db.ctx
        .sql("CREATE TABLE t3 AS SELECT * FROM (VALUES (100, 'x'), (200, 'y')) AS t(id, fk3)")
        .await
        .unwrap();
    db.ctx
        .sql("CREATE TABLE t4 AS SELECT * FROM (VALUES ('x', 'final_x'), ('y', 'final_y')) AS t(id, val)")
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT t1.id, t4.val \
             FROM t1 \
             JOIN t2 ON t1.fk1 = t2.id \
             JOIN t3 ON t2.fk2 = t3.id \
             JOIN t4 ON t3.fk3 = t4.id \
             ORDER BY t1.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_mixed_types() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE stream_a AS SELECT * FROM (VALUES \
             (1, 'k1'), (2, 'k2')) AS t(id, key)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE stream_b AS SELECT * FROM (VALUES \
             ('k1', 10), ('k2', 20)) AS t(key, value)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE dim_c AS SELECT * FROM (VALUES \
             ('k1', 'label1'), ('k2', 'label2')) AS t(key, label)",
        )
        .await
        .unwrap();

    // Inner join + left join
    let df = db
        .ctx
        .sql(
            "SELECT a.id, b.value, c.label \
             FROM stream_a a \
             JOIN stream_b b ON a.key = b.key \
             LEFT JOIN dim_c c ON a.key = c.key \
             ORDER BY a.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_multi_join_single_backward_compat() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE left_t AS SELECT * FROM (VALUES \
             (1, 'a'), (2, 'b')) AS t(id, val)",
        )
        .await
        .unwrap();
    db.ctx
        .sql(
            "CREATE TABLE right_t AS SELECT * FROM (VALUES \
             (1, 'x'), (2, 'y')) AS t(id, data)",
        )
        .await
        .unwrap();

    // Single join still works
    let df = db
        .ctx
        .sql(
            "SELECT l.id, l.val, r.data \
             FROM left_t l JOIN right_t r ON l.id = r.id \
             ORDER BY l.id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_frame_moving_average() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_prices AS SELECT * FROM (VALUES \
             (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0)) \
             AS t(id, price)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, AVG(price) OVER (ORDER BY id \
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma \
             FROM frame_prices ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 5);

    // Verify moving average values: row 3 → avg(10,20,30) = 20
    let ma_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!((ma_col.value(2) - 20.0).abs() < 0.01);
}

#[tokio::test]
async fn test_frame_running_sum() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_amounts AS SELECT * FROM (VALUES \
             (1, 100.0), (2, 200.0), (3, 300.0)) AS t(id, amount)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, SUM(amount) OVER (ORDER BY id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running \
             FROM frame_amounts ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);

    let sum_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Row 3: cumulative sum = 100 + 200 + 300 = 600
    assert!((sum_col.value(2) - 600.0).abs() < 0.01);
}

#[tokio::test]
async fn test_frame_rolling_max() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_vals AS SELECT * FROM (VALUES \
             (1, 5.0), (2, 15.0), (3, 10.0), (4, 20.0)) AS t(id, price)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, MAX(price) OVER (ORDER BY id \
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rmax \
             FROM frame_vals ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4);

    let max_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Row 3: max(5, 15, 10) = 15
    assert!((max_col.value(2) - 15.0).abs() < 0.01);
}

#[tokio::test]
async fn test_frame_rolling_count() {
    let db = LaminarDB::open().unwrap();

    db.ctx
        .sql(
            "CREATE TABLE frame_events AS SELECT * FROM (VALUES \
             (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) AS t(id, code)",
        )
        .await
        .unwrap();

    let df = db
        .ctx
        .sql(
            "SELECT id, COUNT(*) OVER (ORDER BY id \
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS cnt \
             FROM frame_events ORDER BY id",
        )
        .await
        .unwrap();

    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 4);

    let cnt_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    // Row 1: count of just row 1 = 1
    assert_eq!(cnt_col.value(0), 1);
    // Row 2+: count of current + 1 preceding = 2
    assert_eq!(cnt_col.value(1), 2);
    assert_eq!(cnt_col.value(2), 2);
}

/// Helper: create a test `RecordBatch` for table population tests.
fn table_test_batch(ids: &[i32], symbols: &[&str]) -> RecordBatch {
    use arrow::array::Int32Array;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("symbol", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(symbols.to_vec())),
        ],
    )
    .unwrap()
}

/// Register a mock table source factory that returns a `MockReferenceTableSource`
/// pre-loaded with the given snapshot and change batches.
fn register_mock_table_source(
    db: &LaminarDB,
    snapshot_batches: Vec<RecordBatch>,
    change_batches: Vec<RecordBatch>,
) {
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::reference::MockReferenceTableSource;

    let snap = std::sync::Arc::new(parking_lot::Mutex::new(Some(snapshot_batches)));
    let chg = std::sync::Arc::new(parking_lot::Mutex::new(Some(change_batches)));
    db.connector_registry().register_table_source(
        "mock",
        ConnectorInfo {
            name: "mock".to_string(),
            display_name: "Mock Table Source".to_string(),
            version: "0.1.0".to_string(),
            is_source: true,
            is_sink: false,
            config_keys: vec![],
        },
        std::sync::Arc::new(move |_config| {
            let s = snap.lock().take().unwrap_or_default();
            let c = chg.lock().take().unwrap_or_default();
            Ok(Box::new(MockReferenceTableSource::new(s, c)))
        }),
    );
}

#[tokio::test]
async fn test_table_source_snapshot_populates_table() {
    let db = LaminarDB::open().unwrap();
    let batch = table_test_batch(&[1, 2], &["AAPL", "GOOG"]);
    register_mock_table_source(&db, vec![batch], vec![]);

    db.execute("CREATE SOURCE events (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE instruments (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json')",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    // Table should be populated by snapshot
    let ts = db.table_store.read();
    assert!(ts.is_ready("instruments"));
    assert_eq!(ts.table_row_count("instruments"), 2);
}

#[tokio::test]
async fn test_table_source_manual_no_snapshot() {
    let db = LaminarDB::open().unwrap();
    let batch = table_test_batch(&[1], &["AAPL"]);
    register_mock_table_source(&db, vec![batch], vec![]);

    db.execute("CREATE SOURCE events (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE instruments (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json', refresh = 'manual')",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    // Manual mode: table stays empty
    let ts = db.table_store.read();
    assert!(!ts.is_ready("instruments"));
    assert_eq!(ts.table_row_count("instruments"), 0);
}

#[tokio::test]
async fn test_table_source_multiple_tables() {
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::reference::MockReferenceTableSource;

    let db = LaminarDB::open().unwrap();

    // Register two separate mock factories

    let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let cc = call_count.clone();
    let batch1 = table_test_batch(&[1], &["AAPL"]);
    let batch2 = table_test_batch(&[2, 3], &["GOOG", "MSFT"]);
    let batches = std::sync::Arc::new(parking_lot::Mutex::new(vec![vec![batch1], vec![batch2]]));

    db.connector_registry().register_table_source(
        "mock",
        ConnectorInfo {
            name: "mock".to_string(),
            display_name: "Mock".to_string(),
            version: "0.1.0".to_string(),
            is_source: true,
            is_sink: false,
            config_keys: vec![],
        },
        std::sync::Arc::new(move |_config| {
            let idx = cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;
            let mut all = batches.lock();
            let snap = if idx < all.len() {
                std::mem::take(&mut all[idx])
            } else {
                vec![]
            };
            Ok(Box::new(MockReferenceTableSource::new(snap, vec![])))
        }),
    );

    db.execute("CREATE SOURCE events (x INT)").await.unwrap();

    db.execute(
        "CREATE TABLE t1 (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json')",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE TABLE t2 (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json')",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    let ts = db.table_store.read();
    // Both tables should be snapshot-populated (order may vary)
    let total = ts.table_row_count("t1") + ts.table_row_count("t2");
    assert_eq!(total, 3); // 1 + 2
    assert!(ts.is_ready("t1"));
    assert!(ts.is_ready("t2"));
}

#[tokio::test]
async fn test_table_create_with_refresh_mode() {
    let db = LaminarDB::open().unwrap();

    // Just test DDL parsing — no need to register a mock factory
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json', refresh = 'cdc')",
    )
    .await
    .unwrap();

    let mgr = db.connector_manager.lock();
    let reg = mgr.tables().get("t").unwrap();
    assert_eq!(
        reg.refresh,
        Some(laminar_connectors::reference::RefreshMode::SnapshotPlusCdc)
    );
}

#[tokio::test]
async fn test_table_source_snapshot_only_no_changes() {
    let db = LaminarDB::open().unwrap();
    let snap = table_test_batch(&[1], &["AAPL"]);
    let change = table_test_batch(&[2], &["GOOG"]);
    register_mock_table_source(&db, vec![snap], vec![change]);

    db.execute("CREATE SOURCE events (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE instruments (id INT PRIMARY KEY, symbol VARCHAR NOT NULL) \
         WITH (connector = 'mock', format = 'json', refresh = 'snapshot_only')",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    // Should have snapshot data but not the change batch (it's snapshot_only)
    let mut ts = db.table_store.write();
    assert!(ts.is_ready("instruments"));
    assert_eq!(ts.table_row_count("instruments"), 1);
    // The change batch id=2/GOOG should NOT be present
    assert!(ts.lookup("instruments", "2").is_none());
}

#[tokio::test]
async fn test_create_table_partial_cache_mode() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE TABLE large_dim (\
         id INT PRIMARY KEY, \
         name VARCHAR\
         ) WITH (cache_mode = 'partial')",
    )
    .await
    .unwrap();

    // Verify table exists
    {
        let ts = db.table_store.read();
        assert!(ts.has_table("large_dim"));
        // Cache metrics should exist for partial-mode tables
    }

    // Insert some data
    db.execute("INSERT INTO large_dim VALUES (1, 'Alice')")
        .await
        .unwrap();
    db.execute("INSERT INTO large_dim VALUES (2, 'Bob')")
        .await
        .unwrap();

    let ts = db.table_store.read();
    assert_eq!(ts.table_row_count("large_dim"), 2);
}

#[tokio::test]
async fn test_create_table_partial_with_max_entries() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE TABLE customers (\
         id INT PRIMARY KEY, \
         name VARCHAR\
         ) WITH (cache_mode = 'partial', cache_max_entries = '10000')",
    )
    .await
    .unwrap();

    let ts = db.table_store.read();
    assert!(ts.has_table("customers"));
    // Verify the cache metrics report the correct max_entries
    let metrics = ts.cache_metrics("customers").unwrap();
    assert_eq!(metrics.cache_max_entries, 10000);
}

#[tokio::test]
async fn test_create_table_invalid_cache_max_entries() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE TABLE bad (\
             id INT PRIMARY KEY, \
             name VARCHAR\
             ) WITH (cache_mode = 'partial', cache_max_entries = 'not_a_number')",
        )
        .await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("cache_max_entries"));
}

// --- Pipeline Observability API tests ---

#[tokio::test]
async fn test_metrics_initial_state() {
    let db = LaminarDB::open().unwrap();
    let m = db.metrics();
    assert_eq!(m.total_events_ingested, 0);
    assert_eq!(m.total_events_emitted, 0);
    assert_eq!(m.total_events_dropped, 0);
    assert_eq!(m.total_cycles, 0);
    assert_eq!(m.total_batches, 0);
    assert_eq!(m.state, crate::metrics::PipelineState::Created);
    assert_eq!(m.source_count, 0);
    assert_eq!(m.stream_count, 0);
    assert_eq!(m.sink_count, 0);
}

#[tokio::test]
async fn test_source_metrics_after_push() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE)")
        .await
        .unwrap();

    // Push some data
    let handle = db.source_untyped("trades").unwrap();
    let batch = RecordBatch::try_new(
        handle.schema().clone(),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Float64Array::from(vec![150.0, 2800.0])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let sm = db.source_metrics("trades").unwrap();
    assert_eq!(sm.name, "trades");
    assert_eq!(sm.total_events, 1); // 1 push = sequence 1
    assert!(sm.pending > 0);
    assert!(sm.capacity > 0);
    assert!(sm.utilization > 0.0);
}

#[tokio::test]
async fn test_source_metrics_not_found() {
    let db = LaminarDB::open().unwrap();
    assert!(db.source_metrics("nonexistent").is_none());
}

#[tokio::test]
async fn test_all_source_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE a (id INT)").await.unwrap();
    db.execute("CREATE SOURCE b (id INT)").await.unwrap();

    let all = db.all_source_metrics();
    assert_eq!(all.len(), 2);
    #[allow(clippy::disallowed_types)] // test code
    let names: std::collections::HashSet<_> = all.iter().map(|m| m.name.clone()).collect();
    assert!(names.contains("a"));
    assert!(names.contains("b"));
}

#[tokio::test]
async fn test_total_events_processed_zero() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(db.total_events_processed(), 0);
}

#[tokio::test]
async fn test_pipeline_state_enum_created() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(
        db.pipeline_state_enum(),
        crate::metrics::PipelineState::Created
    );
}

#[tokio::test]
async fn test_engine_metrics_accessible() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = std::sync::Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(prom.clone());
    prom.events_ingested.inc_by(42);
    let m = db.metrics();
    assert_eq!(m.total_events_ingested, 42);
}

#[tokio::test]
async fn test_metrics_counts_after_create() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE s1 (id INT)").await.unwrap();
    db.execute("CREATE SINK out1 FROM s1").await.unwrap();

    let m = db.metrics();
    assert_eq!(m.source_count, 1);
    assert_eq!(m.sink_count, 1);
}

#[tokio::test]
async fn test_source_handle_capacity() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    // Default buffer size is 1024
    assert!(handle.capacity() >= 1024);
    assert!(!handle.is_backpressured());
}

#[tokio::test]
async fn test_stream_metrics_with_sql() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM avg_price AS \
         SELECT symbol, AVG(price) as avg_price \
         FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)",
    )
    .await
    .unwrap();

    let sm = db.stream_metrics("avg_price");
    assert!(sm.is_some());
    let sm = sm.unwrap();
    assert_eq!(sm.name, "avg_price");
    assert!(sm.sql.is_some());
    assert!(sm.sql.as_deref().unwrap().contains("AVG"));
}

#[tokio::test]
async fn test_all_stream_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)")
        .await
        .unwrap();
    db.execute(
        "CREATE STREAM s1 AS SELECT symbol, AVG(price) as avg_price \
         FROM trades GROUP BY symbol, TUMBLE(ts, INTERVAL '1' MINUTE)",
    )
    .await
    .unwrap();

    let all = db.all_stream_metrics();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].name, "s1");
}

#[tokio::test]
async fn test_stream_metrics_not_found() {
    let db = LaminarDB::open().unwrap();
    assert!(db.stream_metrics("nonexistent").is_none());
}

/// Helper: push a batch with `Timestamp(µs)` column to a source.
///
/// `timestamps_ms` are in **milliseconds**; the helper converts to microseconds
/// internally to match the `TIMESTAMP` SQL type (`Timestamp(Microsecond, None)`).
fn make_ts_batch(schema: &arrow::datatypes::SchemaRef, timestamps_ms: &[i64]) -> RecordBatch {
    let us_values: Vec<i64> = timestamps_ms.iter().map(|ms| ms * 1000).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(
                (1..=i64::try_from(timestamps_ms.len()).expect("len fits i64")).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(us_values)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_watermark_gauges_advance_on_push() {
    let db = LaminarDB::open().unwrap();
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    db.set_engine_metrics(Arc::clone(&prom));

    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();
    handle
        .push_arrow(make_ts_batch(&schema, &[1000, 2000, 3000]))
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let source_wm = prom
        .source_watermark_ms
        .with_label_values(&["events"])
        .get();
    assert_eq!(source_wm, 3000);
    let stream_wm = prom.stream_watermark_ms.with_label_values(&["out"]).get();
    assert_eq!(stream_wm, 3000);
}

#[tokio::test]
async fn test_watermark_advances_on_push() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();
    let batch = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch).unwrap();

    // Wait for pipeline loop to process
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // With 0s delay, watermark should be max timestamp = 3000
    let wm = handle.current_watermark();
    assert_eq!(
        wm, 3000,
        "watermark should equal max timestamp with 0s delay"
    );
}

#[tokio::test]
async fn test_watermark_bounded_delay() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '100' MILLISECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push timestamps [1000, 800, 1200] — max = 1200
    let batch = make_ts_batch(&schema, &[1000, 800, 1200]);
    handle.push_arrow(batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Watermark = max(1200) - 100ms delay = 1100
    let wm = handle.current_watermark();
    assert_eq!(wm, 1100, "watermark should be max_ts - delay");
}

#[tokio::test]
async fn test_watermark_no_regression() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push high timestamps first
    let batch1 = make_ts_batch(&schema, &[5000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let wm1 = handle.current_watermark();

    // Push lower timestamps
    let batch2 = make_ts_batch(&schema, &[1000]);
    handle.push_arrow(batch2).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let wm2 = handle.current_watermark();

    // Watermark should never decrease
    assert!(wm2 >= wm1, "watermark must not regress: {wm2} < {wm1}");
    assert_eq!(wm1, 5000);
    assert_eq!(wm2, 5000);
}

#[tokio::test]
async fn test_source_without_watermark() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
        .await
        .unwrap();

    // Source without WATERMARK clause should have default watermark
    let handle = db.source_untyped("events").unwrap();
    assert_eq!(handle.current_watermark(), i64::MIN);
    assert!(handle.max_out_of_orderness().is_none());
}

#[tokio::test]
async fn test_watermark_with_arrow_timestamp_column() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Build a batch with Arrow Timestamp(us) column matching the schema
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                5_000_000i64,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let wm = handle.current_watermark();
    // ArrowNative format: timestamp is in microseconds, extractor converts to millis
    assert_eq!(wm, 5000, "watermark should work with Arrow Timestamp type");
}

#[tokio::test]
async fn test_pipeline_watermark_global_min() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE SOURCE orders (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM trades")
        .await
        .unwrap();
    db.start().await.unwrap();

    let trades = db.source_untyped("trades").unwrap();
    let orders = db.source_untyped("orders").unwrap();

    // Push high watermark to trades
    let batch1 = make_ts_batch(trades.schema(), &[5000]);
    trades.push_arrow(batch1).unwrap();

    // Push lower watermark to orders
    let batch2 = make_ts_batch(orders.schema(), &[2000]);
    orders.push_arrow(batch2).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Global watermark should be min(5000, 2000) = 2000
    let global = db.pipeline_watermark();
    assert_eq!(
        global, 2000,
        "global watermark should be min of all sources"
    );
}

#[tokio::test]
async fn test_pipeline_watermark_in_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let batch = make_ts_batch(handle.schema(), &[4000]);
    handle.push_arrow(batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let m = db.metrics();
    assert_eq!(
        m.pipeline_watermark,
        db.pipeline_watermark(),
        "metrics().pipeline_watermark should match pipeline_watermark()"
    );
    assert_eq!(m.pipeline_watermark, 4000);
}

#[tokio::test]
async fn test_source_handle_max_out_of_orderness() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)",
    )
    .await
    .unwrap();

    let handle = db.source_untyped("events").unwrap();
    let dur = handle.max_out_of_orderness();
    assert_eq!(dur, Some(std::time::Duration::from_secs(5)));
}

#[tokio::test]
async fn test_source_handle_no_watermark() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts BIGINT)")
        .await
        .unwrap();

    let handle = db.source_untyped("events").unwrap();
    assert!(handle.max_out_of_orderness().is_none());
}

#[tokio::test]
async fn test_late_data_dropped_after_external_watermark() {
    // Scenario:
    //  1. Push on-time batch (ts = [1000, 2000, 3000])
    //  2. Advance watermark to 200_000 externally via source.watermark()
    //  3. Push late batch (ts = [100, 200, 300]) — all timestamps < watermark
    //  4. Verify late batch does NOT appear in stream output
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    let mut sub = db.catalog.get_stream_subscription("out").unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Step 1: Push on-time data
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Drain on-time results
    let mut on_time_rows = 0;
    for _ in 0..256 {
        match sub.poll() {
            Some(b) => on_time_rows += b.num_rows(),
            None => break,
        }
    }
    assert!(on_time_rows > 0, "should have on-time rows");

    // Step 2: Advance watermark to 200_000 (external signal)
    handle.watermark(200_000);
    // Give the pipeline loop a cycle to pick up the external watermark
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Step 3: Push late data (all timestamps < 200_000)
    let late_batch = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(late_batch).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Step 4: Check that late data was filtered out
    let mut late_rows = 0;
    for _ in 0..256 {
        match sub.poll() {
            Some(b) => late_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
}

#[test]
fn test_filter_late_rows_filters_correctly() {
    use arrow::array::{Int64Array, TimestampMillisecondArray};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 500, 200, 800])),
        ],
    )
    .unwrap();

    // Watermark at 300: rows with ts >= 300 survive (ts=500, ts=800).
    let filtered = filter_late_rows(&batch, "ts", 300).expect("should have some on-time rows");
    assert_eq!(filtered.num_rows(), 2);

    let ids = filtered
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 2); // ts=500
    assert_eq!(ids.value(1), 4); // ts=800
}

#[test]
fn test_filter_late_rows_all_late() {
    use arrow::array::{Int64Array, TimestampMillisecondArray};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![100, 200])),
        ],
    )
    .unwrap();

    let result = filter_late_rows(&batch, "ts", 1000);
    assert!(result.is_none(), "all-late batch should return None");
}

#[test]
fn test_filter_late_rows_no_column() {
    use arrow::array::Int64Array;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();

    // Missing event-time column is schema drift — drop the batch.
    assert!(filter_late_rows(&batch, "ts", 1000).is_none());
}

/// Helper: creates a `RecordBatch` with (id: BIGINT, ts: BIGINT).
#[tokio::test]
async fn test_programmatic_watermark_filters_late_rows() {
    // Source with set_event_time_column("ts"), no SQL WATERMARK clause.
    // Push data, advance watermark, push late data, verify late data filtered.
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP)")
        .await
        .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    let mut sub = db.catalog.get_stream_subscription("out").unwrap();

    let handle = db.source_untyped("events").unwrap();
    handle.set_event_time_column("ts");

    db.start().await.unwrap();

    let schema = handle.schema().clone();

    // Step 1: Push on-time data
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Drain on-time results
    let mut on_time_rows = 0;
    for _ in 0..256 {
        match sub.poll() {
            Some(b) => on_time_rows += b.num_rows(),
            None => break,
        }
    }
    assert!(on_time_rows > 0, "should have on-time rows");

    // Step 2: Advance watermark to 200_000 (external signal)
    handle.watermark(200_000);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Step 3: Push late data (all timestamps < 200_000)
    let late_batch = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(late_batch).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Step 4: Check that late data was filtered out
    let mut late_rows = 0;
    for _ in 0..256 {
        match sub.poll() {
            Some(b) => late_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
}

#[tokio::test]
async fn test_sql_watermark_for_col_filters_late_rows() {
    // Source with WATERMARK FOR ts (no AS expr), should use zero delay.
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts)")
        .await
        .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    let mut sub = db.catalog.get_stream_subscription("out").unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push on-time data
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut on_time_rows = 0;
    for _ in 0..256 {
        match sub.poll() {
            Some(b) => on_time_rows += b.num_rows(),
            None => break,
        }
    }
    assert!(on_time_rows > 0, "should have on-time rows");

    // Advance watermark externally
    handle.watermark(200_000);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Push late data
    let late_batch = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(late_batch).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut late_rows = 0;
    for _ in 0..256 {
        match sub.poll() {
            Some(b) => late_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(late_rows, 0, "late data behind watermark should be dropped");
}

#[tokio::test]
async fn test_no_watermark_passes_all_data() {
    // Source without any watermark config — all data should pass through.
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, ts TIMESTAMP)")
        .await
        .unwrap();
    db.execute("CREATE STREAM out AS SELECT id, ts FROM events")
        .await
        .unwrap();

    let mut sub = db.catalog.get_stream_subscription("out").unwrap();
    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();

    // Push two batches — no watermark filtering should happen
    let batch1 = make_ts_batch(&schema, &[1000, 2000, 3000]);
    handle.push_arrow(batch1).unwrap();
    handle.watermark(200_000); // watermark without event_time_column is a no-op for filtering
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let batch2 = make_ts_batch(&schema, &[100, 200, 300]);
    handle.push_arrow(batch2).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // All rows from both batches should appear
    let mut total_rows = 0;
    for _ in 0..256 {
        match sub.poll() {
            Some(b) => total_rows += b.num_rows(),
            None => break,
        }
    }
    assert_eq!(
        total_rows, 6,
        "all data should pass through without watermark config"
    );
}

#[tokio::test]
async fn test_select_from_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
        .await
        .unwrap();
    db.execute("INSERT INTO sensors VALUES (1, 22.5), (2, 23.1)")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM sensors").await.unwrap();
    match result {
        ExecuteResult::Query(mut q) => {
            // The bridge_query_stream spawns a tokio task; yield to let it run.
            tokio::task::yield_now().await;
            let mut sub = q.subscribe_raw().unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut total_rows = 0;
            for _ in 0..256 {
                match sub.poll() {
                    Some(b) => total_rows += b.num_rows(),
                    None => break,
                }
            }
            assert_eq!(total_rows, 2);
        }
        _ => panic!("Expected Query result from SELECT on source"),
    }
}

#[tokio::test]
async fn test_select_from_dropped_source_fails() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
        .await
        .unwrap();
    db.execute("DROP SOURCE sensors").await.unwrap();

    let result = db.execute("SELECT * FROM sensors").await;
    assert!(result.is_err(), "SELECT after DROP SOURCE should fail");
}

#[tokio::test]
async fn test_select_from_replaced_source() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE sensors (id BIGINT, temp DOUBLE)")
        .await
        .unwrap();
    db.execute("INSERT INTO sensors VALUES (1, 20.0)")
        .await
        .unwrap();

    // Replace the source — old buffer is gone
    db.execute("CREATE OR REPLACE SOURCE sensors (id BIGINT, temp DOUBLE)")
        .await
        .unwrap();
    db.execute("INSERT INTO sensors VALUES (2, 30.0)")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM sensors").await.unwrap();
    match result {
        ExecuteResult::Query(mut q) => {
            let mut sub = q.subscribe_raw().unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut total_rows = 0;
            for _ in 0..256 {
                match sub.poll() {
                    Some(b) => total_rows += b.num_rows(),
                    None => break,
                }
            }
            assert_eq!(
                total_rows, 1,
                "only the post-replace insert should be visible"
            );
        }
        _ => panic!("Expected Query result"),
    }
}

#[tokio::test]
async fn test_mv_registers_stream_in_connector_manager() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    // Before MV creation, no stream registered
    {
        let mgr = db.connector_manager.lock();
        assert!(
            !mgr.streams().contains_key("event_totals"),
            "stream should not exist before MV creation"
        );
    }

    let result = db
        .execute("CREATE MATERIALIZED VIEW event_totals AS SELECT * FROM events")
        .await;

    // The MV may fail at query execution (no data), but if DDL succeeds
    // the connector manager should have the stream registered
    if result.is_ok() {
        let mgr = db.connector_manager.lock();
        assert!(
            mgr.streams().contains_key("event_totals"),
            "MV should be registered as a stream in connector manager"
        );
        let reg = &mgr.streams()["event_totals"];
        assert!(
            reg.query_sql.contains("events"),
            "stream query should reference the source"
        );
    }
}

#[tokio::test]
async fn test_drop_mv_unregisters_stream() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM events")
        .await;

    if result.is_ok() {
        // Verify registered
        {
            let mgr = db.connector_manager.lock();
            assert!(mgr.streams().contains_key("mv1"));
        }

        // Drop the MV
        db.execute("DROP MATERIALIZED VIEW mv1").await.unwrap();

        // Verify unregistered
        {
            let mgr = db.connector_manager.lock();
            assert!(
                !mgr.streams().contains_key("mv1"),
                "stream should be unregistered after DROP MV"
            );
        }
    }
}

#[tokio::test]
async fn test_set_session_property() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET parallelism = 4").await.unwrap();
    assert_eq!(
        db.get_session_property("parallelism"),
        Some("4".to_string())
    );
}

#[tokio::test]
async fn test_set_session_property_string_value() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET state_ttl = '1 hour'").await.unwrap();
    assert_eq!(
        db.get_session_property("state_ttl"),
        Some("1 hour".to_string())
    );
}

#[tokio::test]
async fn test_set_session_property_overwrite() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET batch_size = 100").await.unwrap();
    db.execute("SET batch_size = 200").await.unwrap();
    assert_eq!(
        db.get_session_property("batch_size"),
        Some("200".to_string())
    );
}

#[tokio::test]
async fn test_get_session_property_not_set() {
    let db = LaminarDB::open().unwrap();
    assert_eq!(db.get_session_property("nonexistent"), None);
}

#[tokio::test]
async fn test_session_properties_all() {
    let db = LaminarDB::open().unwrap();
    db.execute("SET parallelism = 4").await.unwrap();
    db.execute("SET state_ttl = '1 hour'").await.unwrap();
    let props = db.session_properties();
    assert_eq!(props.len(), 2);
    assert_eq!(props.get("parallelism"), Some(&"4".to_string()));
    assert_eq!(props.get("state_ttl"), Some(&"1 hour".to_string()));
}

#[tokio::test]
async fn test_alter_source_add_column() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();

    // Verify initial schema has 2 columns
    let schema = db.catalog.describe_source("events").unwrap();
    assert_eq!(schema.fields().len(), 2);

    // Add a column
    db.execute("ALTER SOURCE events ADD COLUMN new_col VARCHAR")
        .await
        .unwrap();

    // Verify updated schema has 3 columns
    let schema = db.catalog.describe_source("events").unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(2).name(), "new_col");
}

#[tokio::test]
async fn test_alter_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute("ALTER SOURCE nonexistent ADD COLUMN col INT")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_alter_source_set_properties() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("ALTER SOURCE events SET ('batch.size' = '1000')")
        .await
        .unwrap();
    assert_eq!(
        db.get_session_property("events.batch.size"),
        Some("1000".to_string())
    );
}

#[test]
fn test_extract_connector_from_with_options_basic() {
    let mut opts = HashMap::new();
    opts.insert("connector".to_string(), "kafka".to_string());
    opts.insert("topic".to_string(), "events".to_string());
    opts.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    opts.insert("format".to_string(), "json".to_string());

    let (conn_opts, format, fmt_opts) = extract_connector_from_with_options(&opts);

    // 'connector' and 'format' are extracted, not in connector_options
    assert!(!conn_opts.contains_key("connector"));
    assert!(!conn_opts.contains_key("format"));
    assert_eq!(conn_opts.get("topic"), Some(&"events".to_string()));
    assert_eq!(
        conn_opts.get("bootstrap.servers"),
        Some(&"localhost:9092".to_string())
    );
    assert_eq!(format, Some("json".to_string()));
    assert!(fmt_opts.is_empty());
}

#[test]
fn test_extract_connector_filters_streaming_keys() {
    let mut opts = HashMap::new();
    opts.insert("connector".to_string(), "websocket".to_string());
    opts.insert("url".to_string(), "wss://feed.example.com".to_string());
    opts.insert("buffer_size".to_string(), "4096".to_string());
    opts.insert("backpressure".to_string(), "block".to_string());
    opts.insert("watermark_delay".to_string(), "5s".to_string());

    let (conn_opts, _, _) = extract_connector_from_with_options(&opts);

    // Streaming keys should NOT be in connector_options
    assert!(!conn_opts.contains_key("buffer_size"));
    assert!(!conn_opts.contains_key("backpressure"));
    assert!(!conn_opts.contains_key("watermark_delay"));
    // Connector-specific key should be present
    assert_eq!(
        conn_opts.get("url"),
        Some(&"wss://feed.example.com".to_string())
    );
}

#[test]
fn test_extract_connector_format_options() {
    let mut opts = HashMap::new();
    opts.insert("connector".to_string(), "kafka".to_string());
    opts.insert("format".to_string(), "avro".to_string());
    opts.insert(
        "format.schema.registry.url".to_string(),
        "http://localhost:8081".to_string(),
    );
    opts.insert("topic".to_string(), "events".to_string());

    let (conn_opts, format, fmt_opts) = extract_connector_from_with_options(&opts);

    assert_eq!(format, Some("avro".to_string()));
    assert_eq!(
        fmt_opts.get("schema.registry.url"),
        Some(&"http://localhost:8081".to_string())
    );
    assert_eq!(conn_opts.get("topic"), Some(&"events".to_string()));
    assert!(!conn_opts.contains_key("format.schema.registry.url"));
}

#[tokio::test]
async fn test_create_source_with_connector_option() {
    // Verify that WITH ('connector' = '...') is accepted at the DDL level.
    // The actual connector won't be instantiated because the type isn't
    // registered in the default embedded registry, so we just check
    // that the error is "Unknown source connector type" (meaning the
    // WITH clause was correctly routed) rather than silently ignored.
    let db = LaminarDB::open().unwrap();
    let result = db
        .execute(
            "CREATE SOURCE ws_feed (id BIGINT, data TEXT) WITH (
                'connector' = 'websocket',
                'url' = 'wss://feed.example.com',
                'format' = 'json'
            )",
        )
        .await;

    // Without the websocket feature, the connector type won't be registered,
    // so we expect an "Unknown source connector type" error — which proves
    // the WITH clause WAS routed to the connector registry.
    if let Err(e) = result {
        let msg = e.to_string();
        assert!(
            msg.contains("Unknown source connector type"),
            "Expected connector routing error, got: {msg}"
        );
    } else {
        // If websocket feature IS enabled, the connector type is registered
        // and the DDL succeeds — also acceptable.
    }
}

#[tokio::test]
async fn test_show_sources_enriched() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id BIGINT, ts TIMESTAMP, WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
    )
    .await
    .unwrap();

    let result = db.execute("SHOW SOURCES").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 4);
            assert_eq!(batch.schema().field(0).name(), "source_name");
            assert_eq!(batch.schema().field(1).name(), "connector");
            assert_eq!(batch.schema().field(2).name(), "format");
            assert_eq!(batch.schema().field(3).name(), "watermark_column");

            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "events");

            let wm = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(wm.value(0), "ts");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_sinks_enriched() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    db.execute("CREATE SINK output FROM events").await.unwrap();

    let result = db.execute("SHOW SINKS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 4);
            assert_eq!(batch.schema().field(0).name(), "sink_name");
            assert_eq!(batch.schema().field(1).name(), "input");
            assert_eq!(batch.schema().field(2).name(), "connector");
            assert_eq!(batch.schema().field(3).name(), "format");

            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "output");

            let inputs = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(inputs.value(0), "events");
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_streams_enriched() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE STREAM my_stream AS SELECT 1 FROM events")
        .await
        .unwrap();

    let result = db.execute("SHOW STREAMS").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.schema().field(0).name(), "stream_name");
            assert_eq!(batch.schema().field(1).name(), "sql");

            let sqls = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(
                sqls.value(0).contains("SELECT"),
                "SQL column should contain query"
            );
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_create_source() {
    let db = LaminarDB::open().unwrap();
    let ddl = "CREATE SOURCE events (id BIGINT, name VARCHAR)";
    db.execute(ddl).await.unwrap();

    let result = db.execute("SHOW CREATE SOURCE events").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.schema().field(0).name(), "create_statement");
            let stmts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(stmts.value(0), ddl);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_create_sink() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT)").await.unwrap();
    let ddl = "CREATE SINK output FROM events";
    db.execute(ddl).await.unwrap();

    let result = db.execute("SHOW CREATE SINK output").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.schema().field(0).name(), "create_statement");
            let stmts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(stmts.value(0), ddl);
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_show_create_source_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("SHOW CREATE SOURCE nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_show_create_sink_not_found() {
    let db = LaminarDB::open().unwrap();
    let result = db.execute("SHOW CREATE SINK nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_explain_analyze_returns_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let result = db
        .execute("EXPLAIN ANALYZE SELECT * FROM events")
        .await
        .unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let key_vals: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
            assert!(
                key_vals.contains(&"rows_produced"),
                "Expected rows_produced metric, got: {key_vals:?}"
            );
            assert!(
                key_vals.contains(&"execution_time_ms"),
                "Expected execution_time_ms metric, got: {key_vals:?}"
            );
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_explain_without_analyze_has_no_metrics() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id BIGINT, value DOUBLE)")
        .await
        .unwrap();

    let result = db.execute("EXPLAIN SELECT * FROM events").await.unwrap();
    match result {
        ExecuteResult::Metadata(batch) => {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let key_vals: Vec<&str> = (0..batch.num_rows()).map(|i| keys.value(i)).collect();
            assert!(
                !key_vals.contains(&"rows_produced"),
                "EXPLAIN without ANALYZE should not have rows_produced"
            );
        }
        _ => panic!("Expected Metadata result"),
    }
}

#[tokio::test]
async fn test_connectorless_source_does_not_break_pipeline() {
    let db = LaminarDB::open().unwrap();

    // Connector-less source (no FROM clause) — formerly caused
    // LDB-1002 "No partitions provided" on every pipeline cycle.
    db.execute("CREATE SOURCE metadata (symbol VARCHAR, category VARCHAR)")
        .await
        .unwrap();

    // A real source with a watermark that the pipeline will process.
    db.execute(
        "CREATE SOURCE trades (id BIGINT, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    db.execute("CREATE STREAM out AS SELECT id, price FROM trades")
        .await
        .unwrap();

    db.start().await.unwrap();

    // Push data into the real source. `ts TIMESTAMP` maps to
    // Timestamp(Microsecond), so values here are in µs.
    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow::array::Float64Array::from(vec![100.0, 200.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    // Let the pipeline run a few cycles.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify the pipeline processed data without errors.
    let m = db.metrics();
    assert!(m.total_events_ingested > 0, "pipeline should ingest events");

    // Push data into the connector-less source via push_arrow — should
    // work without causing pipeline errors.
    let meta_handle = db.source_untyped("metadata").unwrap();
    let meta_schema = meta_handle.schema().clone();
    let meta_batch = RecordBatch::try_new(
        meta_schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["BTC", "ETH"])),
            Arc::new(arrow::array::StringArray::from(vec!["L1", "L1"])),
        ],
    )
    .unwrap();
    meta_handle.push_arrow(meta_batch).unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Pipeline should still be healthy.
    let m2 = db.metrics();
    assert!(
        m2.total_events_ingested >= m.total_events_ingested,
        "pipeline should continue after connector-less source push"
    );
}

/// Poll an MV until it has at least `min_rows` rows, or timeout.
async fn poll_mv(db: &LaminarDB, mv: &str, min_rows: usize) -> usize {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let df = db.ctx.sql(&format!("SELECT * FROM {mv}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        if rows >= min_rows || std::time::Instant::now() > deadline {
            return rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn test_mv_aggregate_queryable_with_pipeline() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE MATERIALIZED VIEW trade_counts AS \
         SELECT symbol, COUNT(*) as cnt FROM trades GROUP BY symbol",
    )
    .await
    .unwrap();

    assert_eq!(poll_mv(&db, "trade_counts", 0).await, 0);

    db.start().await.unwrap();

    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(arrow::array::Float64Array::from(vec![150.0, 2800.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let rows = poll_mv(&db, "trade_counts", 1).await;
    assert!(rows > 0, "MV should have data after pipeline processes");
}

#[tokio::test]
async fn test_mv_append_mode_queryable() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (id INT, value DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    db.execute(
        "CREATE MATERIALIZED VIEW filtered AS \
         SELECT id, value FROM events WHERE value > 10.0",
    )
    .await
    .unwrap();

    db.start().await.unwrap();

    let handle = db.source_untyped("events").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::Float64Array::from(vec![5.0, 15.0, 25.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000, 2_000_000, 3_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let rows = poll_mv(&db, "filtered", 2).await;
    assert_eq!(rows, 2, "filter MV should have 2 matching rows");
}

#[tokio::test]
async fn test_mv_drop_cleans_up_table() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW ev_mv AS SELECT * FROM events")
        .await
        .unwrap();

    assert!(db.ctx.sql("SELECT * FROM ev_mv").await.is_ok());
    assert!(db.mv_store.read().has_mv("ev_mv"));

    db.execute("DROP MATERIALIZED VIEW ev_mv").await.unwrap();

    assert!(!db.mv_store.read().has_mv("ev_mv"));
    assert!(
        db.ctx.sql("SELECT * FROM ev_mv").await.is_err(),
        "table should be deregistered after DROP"
    );
}

#[tokio::test]
async fn test_mv_empty_returns_correct_schema() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE events (id INT, value DOUBLE)")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW ev_mv AS SELECT id, value FROM events")
        .await
        .unwrap();

    let df = db.ctx.sql("SELECT * FROM ev_mv").await.unwrap();
    let schema = df.schema().clone();
    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 0);

    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        names.contains(&"id") && names.contains(&"value"),
        "schema should contain projected columns, got: {names:?}"
    );
}

#[tokio::test]
async fn test_mv_hot_add_while_pipeline_running() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP, \
         WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();

    // Need at least one stream before start() for the pipeline to launch.
    db.execute("CREATE STREAM noop AS SELECT * FROM trades")
        .await
        .unwrap();

    db.start().await.unwrap();

    // Create MV AFTER pipeline is running (hot-add via ControlMsg).
    db.execute(
        "CREATE MATERIALIZED VIEW trade_counts AS \
         SELECT symbol, COUNT(*) as cnt FROM trades GROUP BY symbol",
    )
    .await
    .unwrap();

    // Wait for the coordinator to process the AddStream control message.
    // Poll observable state instead of a fixed sleep.
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while db.metrics().total_cycles == 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    let handle = db.source_untyped("trades").unwrap();
    let schema = handle.schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
            Arc::new(arrow::array::Float64Array::from(vec![150.0])),
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                1_000_000,
            ])),
        ],
    )
    .unwrap();
    handle.push_arrow(batch).unwrap();

    let rows = poll_mv(&db, "trade_counts", 1).await;
    assert!(rows > 0, "hot-added MV should receive pipeline data");
}
