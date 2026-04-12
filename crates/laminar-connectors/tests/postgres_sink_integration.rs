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

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SinkConnector;
use laminar_connectors::postgres::{
    DeliveryGuarantee, PostgresSink, PostgresSinkConfig, WriteMode,
};

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
    cfg.batch_size = 1000;
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

// ── Auto-flush on batch_size ────────────────────────────────────────

#[tokio::test]
async fn test_auto_flush_on_batch_size() {
    let (_container, host, port) = start_pg().await;

    let mut config = sink_config(&host, port, WriteMode::Append);
    config.batch_size = 5; // Flush after 5 rows.
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    // Write 6 rows — should trigger auto-flush at >=5.
    let batch = make_batch(
        &[1, 2, 3, 4, 5, 6],
        &["a", "b", "c", "d", "e", "f"],
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    );
    let result = sink.write_batch(&batch).await.expect("write");

    // Auto-flush happened: records_written > 0.
    assert!(result.records_written > 0, "expected auto-flush");

    let pg = connect(&host, port).await;
    let count: i64 = pg
        .query_one("SELECT COUNT(*) FROM public.test_events", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 6);

    sink.close().await.expect("close");
}

// ── Exactly-once tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_exactly_once_commit() {
    let (_container, host, port) = start_pg().await;

    let mut config = sink_config(&host, port, WriteMode::Upsert);
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    sink.begin_epoch(1).await.expect("begin");
    sink.write_batch(&make_batch(&[10, 20], &["x", "y"], &[1.1, 2.2]))
        .await
        .expect("write");
    sink.pre_commit(1).await.expect("pre_commit");
    sink.commit_epoch(1).await.expect("commit");

    // Data should be visible.
    let pg = connect(&host, port).await;
    let count: i64 = pg
        .query_one("SELECT COUNT(*) FROM public.test_events", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 2);

    // Epoch marker should be written.
    let epoch_row = pg
        .query_one("SELECT epoch FROM _laminardb_sink_offsets LIMIT 1", &[])
        .await
        .expect("epoch select");
    assert_eq!(epoch_row.get::<_, i64>(0), 1);

    sink.close().await.expect("close");
}

#[tokio::test]
async fn test_exactly_once_rollback_discards() {
    let (_container, host, port) = start_pg().await;

    let mut config = sink_config(&host, port, WriteMode::Upsert);
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    let mut sink = PostgresSink::new(test_schema(), config, None);
    sink.open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect("open");

    sink.begin_epoch(1).await.expect("begin");
    sink.write_batch(&make_batch(&[100], &["rollback_me"], &[0.0]))
        .await
        .expect("write");
    // Don't pre_commit — just rollback.
    sink.rollback_epoch(1).await.expect("rollback");

    // Table should be empty (data never flushed).
    let pg = connect(&host, port).await;
    let count: i64 = pg
        .query_one("SELECT COUNT(*) FROM public.test_events", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 0);

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

// ── Epoch recovery (idempotent replay) test ─────────────────────────

#[tokio::test]
async fn test_epoch_recovery_skips_replay() {
    let (_container, host, port) = start_pg().await;

    let mut config = sink_config(&host, port, WriteMode::Upsert);
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    config.sink_id = "recovery-test-sink".into();

    // First sink: commit epoch 5.
    {
        let mut sink = PostgresSink::new(test_schema(), config.clone(), None);
        sink.open(&ConnectorConfig::new("postgres-sink"))
            .await
            .expect("open1");
        sink.begin_epoch(5).await.expect("begin");
        sink.write_batch(&make_batch(&[1], &["original"], &[1.0]))
            .await
            .expect("write");
        sink.pre_commit(5).await.expect("pre_commit");
        sink.commit_epoch(5).await.expect("commit");
        sink.close().await.expect("close1");
    }

    // Second sink: simulate recovery replay of epoch 5.
    {
        let mut sink = PostgresSink::new(test_schema(), config.clone(), None);
        sink.open(&ConnectorConfig::new("postgres-sink"))
            .await
            .expect("open2");

        // Last committed epoch should be recovered.
        assert_eq!(sink.last_committed_epoch(), 5);

        // Replaying epoch 5 should be a no-op (already committed).
        sink.begin_epoch(5).await.expect("begin replay");
        sink.write_batch(&make_batch(&[1], &["SHOULD_NOT_OVERWRITE"], &[999.0]))
            .await
            .expect("write replay");
        sink.pre_commit(5).await.expect("pre_commit replay");
        sink.commit_epoch(5).await.expect("commit replay");
        sink.close().await.expect("close2");
    }

    // Verify original data is unchanged (replay was skipped).
    let pg = connect(&host, port).await;
    let row = pg
        .query_one(
            "SELECT name, value FROM public.test_events WHERE id = 1",
            &[],
        )
        .await
        .expect("select");
    assert_eq!(row.get::<_, &str>(0), "original");
    assert!((row.get::<_, f64>(1) - 1.0).abs() < f64::EPSILON);
}
