//! Integration tests for the NATS source and sink connectors.
//!
//! These talk to a real `nats-server` on `localhost:4222`. Stand one up
//! with the sibling compose file, then run:
//!
//! ```text
//! cd crates/laminar-connectors/tests
//! docker compose -f docker-compose.nats.yml up -d
//! cargo test --test nats_integration --features nats -- --ignored
//! ```
//!
//! Every test is `#[ignore]`d so the regular `cargo test` run is unaffected.
//!
//! Tests are serial (`#[tokio::test(flavor = "multi_thread")]` with
//! distinct stream/consumer/subject names per test) so a single running
//! NATS server is enough.

#![cfg(feature = "nats")]
#![allow(clippy::disallowed_types)] // test code: std::HashMap is fine

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_nats::jetstream::{self, stream::Config as StreamConfig};

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{SinkConnector, SourceConnector};
use laminar_connectors::nats::{NatsSink, NatsSource};

const NATS_URL: &str = "nats://127.0.0.1:4222";

fn payload_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// JSON-serializable test batch.
fn test_batch(ids: &[i64]) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
    RecordBatch::try_new(
        payload_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                names.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Reset a stream between tests: delete if present, then recreate with
/// the given config.
async fn reset_stream(ctx: &jetstream::Context, cfg: StreamConfig) {
    let name = cfg.name.clone();
    let _ = ctx.delete_stream(&name).await;
    ctx.create_stream(cfg).await.expect("create_stream");
}

fn source_props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

async fn connect() -> jetstream::Context {
    let client = async_nats::connect(NATS_URL)
        .await
        .expect("connect to local NATS");
    jetstream::new(client)
}

// ── roundtrip ──

/// Publish through the sink, consume through the source, verify the
/// payload comes back unchanged.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker compose -f docker-compose.nats.yml up"]
async fn roundtrip_jetstream() {
    let ctx = connect().await;
    reset_stream(
        &ctx,
        StreamConfig {
            name: "RT".into(),
            subjects: vec!["rt.events".into()],
            ..Default::default()
        },
    )
    .await;

    // Sink: publish 3 rows.
    let mut sink = NatsSink::new(payload_schema(), None);
    sink.open(&ConnectorConfig::with_properties(
        "nats",
        source_props(&[
            ("servers", NATS_URL),
            ("subject", "rt.events"),
            ("stream", "RT"),
            ("format", "json"),
        ]),
    ))
    .await
    .expect("sink open");
    let batch = test_batch(&[1, 2, 3]);
    let written = sink.write_batch(&batch).await.expect("write_batch");
    assert_eq!(written.records_written, 3);
    sink.pre_commit(1).await.expect("pre_commit drains acks");
    sink.close().await.expect("sink close");

    // Source: read them back.
    let mut source = NatsSource::new(payload_schema(), None);
    source
        .open(&ConnectorConfig::with_properties(
            "nats",
            source_props(&[
                ("servers", NATS_URL),
                ("stream", "RT"),
                ("consumer", "rt-consumer"),
                ("subject", "rt.events"),
                ("format", "json"),
                ("fetch.max.wait.ms", "250"),
            ]),
        ))
        .await
        .expect("source open");

    let received = drain_rows(&mut source, 3, Duration::from_secs(5)).await;
    source.close().await.expect("source close");

    assert_eq!(
        received,
        vec![
            (1, "row-1".to_string()),
            (2, "row-2".to_string()),
            (3, "row-3".to_string()),
        ]
    );
}

/// Core NATS pub/sub round-trip — non-durable, no stream, no ack.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker compose -f docker-compose.nats.yml up"]
async fn roundtrip_core() {
    let mut source = NatsSource::new(payload_schema(), None);
    source
        .open(&ConnectorConfig::with_properties(
            "nats",
            source_props(&[
                ("servers", NATS_URL),
                ("mode", "core"),
                ("subject", "core.events"),
                ("format", "json"),
            ]),
        ))
        .await
        .expect("source open");

    // Subscriber needs to be ready before publish — give it a brief moment.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sink = NatsSink::new(payload_schema(), None);
    sink.open(&ConnectorConfig::with_properties(
        "nats",
        source_props(&[
            ("servers", NATS_URL),
            ("mode", "core"),
            ("subject", "core.events"),
            ("format", "json"),
        ]),
    ))
    .await
    .expect("sink open");
    sink.write_batch(&test_batch(&[10, 11]))
        .await
        .expect("publish");
    sink.close().await.expect("sink close");

    let received = drain_rows(&mut source, 2, Duration::from_secs(3)).await;
    source.close().await.expect("source close");
    assert_eq!(
        received,
        vec![(10, "row-10".to_string()), (11, "row-11".to_string())]
    );
}

// ── exactly-once ──

/// Publish the same row twice with an identical `Nats-Msg-Id`. The
/// second publish lands inside the stream's duplicate_window and is
/// silently dropped by the server — so the source sees it once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker compose -f docker-compose.nats.yml up"]
async fn exactly_once_dedup_drops_duplicate() {
    let ctx = connect().await;
    reset_stream(
        &ctx,
        StreamConfig {
            name: "EO".into(),
            subjects: vec!["eo.events".into()],
            duplicate_window: Duration::from_secs(300),
            ..Default::default()
        },
    )
    .await;

    // Batch with a dedup column. We'll publish it twice with the same
    // event_id; server-side dedup should drop the second.
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["evt-42"])),
            Arc::new(Int64Array::from(vec![7])),
        ],
    )
    .unwrap();

    let mut sink = NatsSink::new(schema, None);
    sink.open(&ConnectorConfig::with_properties(
        "nats",
        source_props(&[
            ("servers", NATS_URL),
            ("subject", "eo.events"),
            ("stream", "EO"),
            ("delivery.guarantee", "exactly_once"),
            ("dedup.id.column", "event_id"),
            ("format", "json"),
        ]),
    ))
    .await
    .expect("sink open");

    sink.write_batch(&batch).await.expect("first publish");
    sink.pre_commit(1).await.expect("first drain");
    sink.write_batch(&batch).await.expect("second publish");
    sink.pre_commit(2).await.expect("second drain");
    sink.close().await.expect("close");

    // Server side: stream should contain exactly one message.
    let mut stream = ctx.get_stream("EO").await.expect("get_stream");
    let info = stream.info().await.expect("info");
    assert_eq!(
        info.state.messages, 1,
        "expected dedup to drop the second publish"
    );
}

/// A stream with duplicate_window below the sink's minimum makes
/// exactly-once unsafe. `open()` must refuse with LDB-5056 rather than
/// silently publishing without dedup coverage.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker compose -f docker-compose.nats.yml up"]
async fn exactly_once_rejects_short_duplicate_window() {
    let ctx = connect().await;
    reset_stream(
        &ctx,
        StreamConfig {
            name: "SHORT".into(),
            subjects: vec!["short.events".into()],
            duplicate_window: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await;

    let mut sink = NatsSink::new(payload_schema(), None);
    let err = sink
        .open(&ConnectorConfig::with_properties(
            "nats",
            source_props(&[
                ("servers", NATS_URL),
                ("subject", "short.events"),
                ("stream", "SHORT"),
                ("delivery.guarantee", "exactly_once"),
                ("dedup.id.column", "id"),
                ("format", "json"),
                ("min.duplicate.window.ms", "60000"),
            ]),
        ))
        .await
        .expect_err("should fail startup");
    assert!(
        err.to_string().contains("LDB-5056"),
        "expected LDB-5056, got: {err}"
    );
}

// ── helpers ──

async fn drain_rows(
    source: &mut NatsSource,
    expected: usize,
    timeout: Duration,
) -> Vec<(i64, String)> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut rows: Vec<(i64, String)> = Vec::new();
    while rows.len() < expected && tokio::time::Instant::now() < deadline {
        match source.poll_batch(128).await {
            Ok(Some(batch)) => rows.extend(extract_rows(&batch.records)),
            Ok(None) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("poll_batch: {e}"),
        }
    }
    assert_eq!(
        rows.len(),
        expected,
        "expected {expected} rows within {timeout:?}, got {}: {rows:?}",
        rows.len()
    );
    rows
}

fn extract_rows(batch: &RecordBatch) -> Vec<(i64, String)> {
    let ids = batch
        .column_by_name("id")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .expect("id column");
    let names = batch
        .column_by_name("name")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .expect("name column");
    (0..batch.num_rows())
        .map(|i| (ids.value(i), names.value(i).to_string()))
        .collect()
}
