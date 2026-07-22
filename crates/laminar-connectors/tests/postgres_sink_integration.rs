//! Integration tests for the PostgreSQL sink connector.
//!
//! These tests require Docker. They spin up a real PostgreSQL container via
//! `testcontainers` and verify that data written through [`PostgresSink`]
//! is observable with `SELECT` queries.
//!
//! Run with: `cargo test -p laminar-connectors --features postgres-sink --test postgres_sink_integration`

#![cfg(feature = "postgres-sink")]
#![cfg(not(target_os = "windows"))]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SinkConnector;
use laminar_connectors::postgres::{
    register_postgres_sink, PostgresSink, PostgresSinkConfig, WriteMode,
};
use laminar_connectors::registry::ConnectorRegistry;

// ── Helpers ─────────────────────────────────────────────────────────

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn make_batch(ids: &[i64], names: &[&str], values: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .expect("test batch")
}

fn sink_config(host: &str, port: u16, mode: WriteMode) -> PostgresSinkConfig {
    let mut cfg = PostgresSinkConfig::new(host, "postgres", "test_events");
    cfg.username = "postgres".into();
    cfg.password = "postgres".into();
    cfg.port = port;
    cfg.write_mode = mode;
    cfg.auto_create_table = true;
    cfg.ssl_mode = laminar_connectors::postgres::SslMode::Disable;
    if mode == WriteMode::Upsert {
        cfg.primary_key_columns = vec!["id".to_string()];
    }
    cfg
}

async fn connect(host: &str, port: u16) -> tokio_postgres::Client {
    let conn_str =
        format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("direct pg connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("pg connection error: {e}");
        }
    });
    client
}

/// Starts a Postgres container and returns (host, port).
async fn start_pg() -> (testcontainers::ContainerAsync<Postgres>, String, u16) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.expect("get host").to_string();
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    (container, host, port)
}

#[tokio::test]
async fn test_factory_schema_drives_ddl_and_duplicate_upsert_is_last_row_wins() {
    let (_container, host, port) = start_pg().await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("sequence", DataType::Int64, false),
        Field::new("enabled", DataType::Boolean, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let mut config = ConnectorConfig::new("postgres-sink");
    config.set("hostname", host.as_str());
    config.set("port", port.to_string());
    config.set("database", "postgres");
    config.set("username", "postgres");
    config.set("password", "postgres");
    config.set("table.name", "factory_events");
    config.set("write.mode", "upsert");
    config.set("primary.key", "tenant, sequence");
    config.set("auto.create.table", "true");
    config.set("ssl.mode", "disable");
    config.set(
        "_arrow_schema",
        laminar_connectors::config::encode_arrow_schema_ipc(schema.as_ref()),
    );

    let registry = ConnectorRegistry::new();
    register_postgres_sink(&registry).unwrap();
    let mut sink = registry.create_sink(&config, None).unwrap();
    assert_eq!(sink.schema(), schema);
    sink.open(&config).await.expect("factory sink open");

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["acme", "beta", "acme"])),
            Arc::new(Int64Array::from(vec![7, 8, 7])),
            Arc::new(BooleanArray::from(vec![false, true, true])),
            Arc::new(StringArray::from(vec!["old", "other", "new"])),
        ],
    )
    .unwrap();
    sink.write_batch(&batch).await.expect("upsert batch");
    sink.flush().await.expect("upsert flush");

    let pg = connect(&host, port).await;
    let rows = pg
        .query(
            "SELECT tenant, sequence, enabled, payload \
             FROM public.factory_events ORDER BY tenant",
            &[],
        )
        .await
        .expect("select factory table");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "acme");
    assert_eq!(rows[0].get::<_, i64>(1), 7);
    assert!(rows[0].get::<_, bool>(2));
    assert_eq!(rows[0].get::<_, &str>(3), "new");
    assert_eq!(rows[1].get::<_, &str>(0), "beta");

    sink.close().await.expect("close");
}

#[tokio::test]
async fn test_quoted_identifiers_uint64_and_negative_timestamp_upsert() {
    let (_container, host, port) = start_pg().await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("select", DataType::UInt64, false),
        Field::new(
            "EventTime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("_private", DataType::Utf8, false),
    ]));
    let mut config = PostgresSinkConfig::new(&host, "postgres", "Order\"Events");
    config.username = "postgres".into();
    config.password = "postgres".into();
    config.port = port;
    config.ssl_mode = laminar_connectors::postgres::SslMode::Disable;
    config.write_mode = WriteMode::Upsert;
    config.primary_key_columns = vec!["select".into()];
    config.auto_create_table = true;

    let mut sink = PostgresSink::new(schema.clone(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open quoted sink");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![u64::try_from(i64::MAX).unwrap()])),
            Arc::new(TimestampMillisecondArray::from(vec![-1])),
            Arc::new(StringArray::from(vec!["kept"])),
        ],
    )
    .unwrap();
    sink.write_batch(&batch).await.expect("write quoted row");
    sink.flush().await.expect("flush quoted row");

    let pg = connect(&host, port).await;
    let row = pg
        .query_one(
            "SELECT \"select\", \"EventTime\", \"_private\" \
             FROM \"public\".\"Order\"\"Events\"",
            &[],
        )
        .await
        .expect("select quoted row");
    assert_eq!(row.get::<_, i64>(0), i64::MAX);
    let timestamp = row.get::<_, chrono::NaiveDateTime>(1).and_utc();
    assert_eq!(timestamp.timestamp(), -1);
    assert_eq!(timestamp.timestamp_subsec_nanos(), 999_000_000);
    assert_eq!(row.get::<_, &str>(2), "kept");

    sink.close().await.expect("close quoted sink");

    // COPY must use the same BIGINT contract rather than pgpq's native UInt64-to-NUMERIC mapping.
    let copy_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let mut copy_config = PostgresSinkConfig::new(&host, "postgres", "CopyUInt64");
    copy_config.username = "postgres".into();
    copy_config.password = "postgres".into();
    copy_config.port = port;
    copy_config.ssl_mode = laminar_connectors::postgres::SslMode::Disable;
    copy_config.auto_create_table = true;
    let mut copy_sink = PostgresSink::new(copy_schema.clone(), copy_config, None);
    copy_sink
        .open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open UInt64 COPY sink");
    let copy_batch = RecordBatch::try_new(
        copy_schema,
        vec![Arc::new(UInt64Array::from(vec![
            u64::try_from(i64::MAX).unwrap()
        ]))],
    )
    .unwrap();
    copy_sink
        .write_batch(&copy_batch)
        .await
        .expect("write UInt64 COPY row");
    copy_sink.flush().await.expect("flush UInt64 COPY row");
    let copied = pg
        .query_one("SELECT \"value\" FROM \"public\".\"CopyUInt64\"", &[])
        .await
        .unwrap();
    assert_eq!(copied.get::<_, i64>(0), i64::MAX);
    copy_sink.close().await.expect("close UInt64 COPY sink");
}

// ── Append (COPY BINARY) tests ──────────────────────────────────────

#[tokio::test]
async fn test_append_flush_writes_data() {
    let (_container, host, port) = start_pg().await;

    let config = sink_config(&host, port, WriteMode::Append);
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    let batch = make_batch(&[1, 2, 3], &["alice", "bob", "carol"], &[1.0, 2.0, 3.0]);
    sink.write_batch(&batch).await.expect("write");
    sink.flush().await.expect("flush");

    // Verify data in PG.
    let pg = connect(&host, port).await;
    let rows = pg
        .query(
            "SELECT id, name, value FROM public.test_events ORDER BY id",
            &[],
        )
        .await
        .expect("select");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "alice");
    assert_eq!(rows[1].get::<_, i64>(0), 2);
    assert_eq!(rows[2].get::<_, i64>(0), 3);
    assert!((rows[2].get::<_, f64>(2) - 3.0).abs() < f64::EPSILON);

    sink.close().await.expect("close");
}

#[tokio::test]
async fn test_append_multiple_flushes() {
    let (_container, host, port) = start_pg().await;

    let config = sink_config(&host, port, WriteMode::Append);
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    // Two separate flushes.
    sink.write_batch(&make_batch(&[1], &["a"], &[1.0]))
        .await
        .expect("write1");
    sink.flush().await.expect("flush1");

    sink.write_batch(&make_batch(&[2], &["b"], &[2.0]))
        .await
        .expect("write2");
    sink.flush().await.expect("flush2");

    let pg = connect(&host, port).await;
    let count: i64 = pg
        .query_one("SELECT COUNT(*) FROM public.test_events", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 2);

    sink.close().await.expect("close");
}

// ── Upsert (UNNEST) tests ──────────────────────────────────────────

#[tokio::test]
async fn test_upsert_insert_and_update() {
    let (_container, host, port) = start_pg().await;

    let config = sink_config(&host, port, WriteMode::Upsert);
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    // Insert.
    sink.write_batch(&make_batch(&[1, 2], &["alice", "bob"], &[10.0, 20.0]))
        .await
        .expect("write");
    sink.flush().await.expect("flush");

    // Update id=1.
    sink.write_batch(&make_batch(&[1], &["alice_updated"], &[99.0]))
        .await
        .expect("write update");
    sink.flush().await.expect("flush update");

    let pg = connect(&host, port).await;
    let rows = pg
        .query(
            "SELECT name, value FROM public.test_events WHERE id = 1",
            &[],
        )
        .await
        .expect("select");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "alice_updated");
    assert!((rows[0].get::<_, f64>(1) - 99.0).abs() < f64::EPSILON);

    // id=2 should be unchanged.
    let row2 = pg
        .query_one("SELECT name FROM public.test_events WHERE id = 2", &[])
        .await
        .expect("select id=2");
    assert_eq!(row2.get::<_, &str>(0), "bob");

    sink.close().await.expect("close");
}

// ── Auto-create table test ──────────────────────────────────────────

#[tokio::test]
async fn test_auto_create_table() {
    let (_container, host, port) = start_pg().await;

    // Verify table doesn't exist before open.
    let pg = connect(&host, port).await;
    let exists: bool = pg
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_events')",
            &[],
        )
        .await
        .expect("check")
        .get(0);
    assert!(!exists, "table should not exist yet");

    let config = sink_config(&host, port, WriteMode::Upsert);
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    // Table should now exist.
    let exists_after: bool = pg
        .query_one(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_events')",
            &[],
        )
        .await
        .expect("check after")
        .get(0);
    assert!(exists_after, "table should exist after open");

    sink.close().await.expect("close");
}

#[tokio::test]
async fn test_statement_timeout_is_applied_to_pool_connections() {
    let (_container, host, port) = start_pg().await;
    let mut config = sink_config(&host, port, WriteMode::Append);
    config.statement_timeout = Duration::from_secs(1);
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    let pg = connect(&host, port).await;
    pg.batch_execute(
        "CREATE FUNCTION slow_sink_insert() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN PERFORM pg_sleep(10); RETURN NEW; END $$; \
         CREATE TRIGGER slow_sink_insert BEFORE INSERT ON public.test_events \
         FOR EACH ROW EXECUTE FUNCTION slow_sink_insert()",
    )
    .await
    .expect("create slow trigger");

    sink.write_batch(&make_batch(&[1], &["slow"], &[1.0]))
        .await
        .expect("buffer slow row");
    let started = Instant::now();
    let error = sink
        .flush()
        .await
        .expect_err("server-side statement timeout must cancel COPY");
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "statement timeout was not applied: {error}"
    );
    sink.close().await.expect("close after timeout");
}

// ── Changelog (upsert + delete) test ────────────────────────────────

#[tokio::test]
async fn test_changelog_upsert_and_delete() {
    let (_container, host, port) = start_pg().await;

    let changelog_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("_op", DataType::Utf8, false),
    ]));

    let mut config = sink_config(&host, port, WriteMode::Upsert);
    config.changelog_mode = true;
    let mut sink = PostgresSink::new(changelog_schema.clone(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    // Insert three rows, delete one.
    let batch = RecordBatch::try_new(
        changelog_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 2])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol", "bob"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 2.0])),
            Arc::new(StringArray::from(vec!["I", "I", "I", "D"])),
        ],
    )
    .expect("changelog batch");

    sink.write_batch(&batch).await.expect("write");
    sink.flush().await.expect("flush");

    let pg = connect(&host, port).await;
    let rows = pg
        .query("SELECT id, name FROM public.test_events ORDER BY id", &[])
        .await
        .expect("select");

    // id=2 was inserted then deleted → should not exist.
    // id=1 and id=3 remain.
    assert_eq!(rows.len(), 2, "expected 2 rows after delete");
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[1].get::<_, i64>(0), 3);

    sink.close().await.expect("close");
}

// ── Z-set changelog collapse (incremental MV → upsert) ──────────────

fn changelog_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("__weight", DataType::Int64, false),
    ]))
}

fn make_changelog(ids: &[i64], names: &[&str], values: &[f64], weights: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        changelog_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(weights.to_vec())),
        ],
    )
    .expect("changelog batch")
}

/// A Z-set (`__weight`) changelog from an incremental MV is collapsed per primary key before the
/// UNNEST upsert: retract+insert nets to an update, and a key with only net-negative weight becomes
/// a DELETE. Without the collapse the raw changelog has no `_op` and fails the split.
#[tokio::test]
async fn test_upsert_collapses_zset_changelog() {
    let (_container, host, port) = start_pg().await;

    let mut config = sink_config(&host, port, WriteMode::Upsert);
    config.changelog_mode = true;
    let mut sink = PostgresSink::new(changelog_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    // Epoch 1: three groups appear.
    sink.write_batch(&make_changelog(
        &[1, 2, 3],
        &["alice", "bob", "carol"],
        &[10.0, 20.0, 30.0],
        &[1, 1, 1],
    ))
    .await
    .expect("write epoch 1");
    sink.flush().await.expect("flush epoch 1");

    // Epoch 2: id=1 updates (retract 10, insert 15); id=3 is removed (retract only).
    sink.write_batch(&make_changelog(
        &[1, 1, 3],
        &["alice", "alice", "carol"],
        &[10.0, 15.0, 30.0],
        &[-1, 1, -1],
    ))
    .await
    .expect("write epoch 2");
    sink.flush().await.expect("flush epoch 2");

    let pg = connect(&host, port).await;
    let rows = pg
        .query("SELECT id, value FROM public.test_events ORDER BY id", &[])
        .await
        .expect("select");
    assert_eq!(
        rows.len(),
        2,
        "id=3 deleted, id=1 updated in place, id=2 kept"
    );
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert!(
        (rows[0].get::<_, f64>(1) - 15.0).abs() < f64::EPSILON,
        "id=1 collapsed to its latest value 15"
    );
    assert_eq!(rows[1].get::<_, i64>(0), 2);
    assert!((rows[1].get::<_, f64>(1) - 20.0).abs() < f64::EPSILON);

    sink.close().await.expect("close");
}
