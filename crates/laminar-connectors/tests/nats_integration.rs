//! Integration tests against a real `nats-server` — see the sibling
//! `docker-compose.nats.yml`. Plain server on :4222, `secure` profile
//! on :4223. Run with:
//!
//! ```text
//! docker compose -f crates/laminar-connectors/tests/docker-compose.nats.yml \
//!   --profile secure up -d
//! cargo test --test nats_integration --features nats -- --ignored
//! ```

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
use laminar_connectors::health::HealthStatus;
use laminar_connectors::nats::{NatsSink, NatsSource};

const NATS_URL: &str = "nats://127.0.0.1:4222";

fn payload_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

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

/// Publishing the same `Nats-Msg-Id` twice inside `duplicate_window`:
/// server drops the repeat, stream holds one message.
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

    let prom = prometheus::Registry::new();
    let mut sink = NatsSink::new(schema, Some(&prom));
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

    // Client-side counter via the public `SinkConnector::metrics`.
    let cm = sink.metrics();
    let dedup = cm
        .custom
        .iter()
        .find_map(|(k, v)| (k == "nats.dedup").then_some(*v as u64))
        .expect("nats.dedup custom metric");
    assert_eq!(dedup, 1, "expected nats.dedup = 1, got {dedup}");
}

/// `open()` refuses a stream with `duplicate_window` below the
/// configured minimum (LDB-5056).
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

// ── health escalation ──

/// Delete the stream behind a running source; health flips to Unhealthy.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker compose -f docker-compose.nats.yml up"]
async fn source_flips_unhealthy_after_stream_deleted() {
    let ctx = connect().await;
    reset_stream(
        &ctx,
        StreamConfig {
            name: "DROP".into(),
            subjects: vec!["drop.events".into()],
            ..Default::default()
        },
    )
    .await;

    let mut source = NatsSource::new(payload_schema(), None);
    source
        .open(&ConnectorConfig::with_properties(
            "nats",
            source_props(&[
                ("servers", NATS_URL),
                ("stream", "DROP"),
                ("consumer", "drop-consumer"),
                ("subject", "drop.events"),
                ("format", "json"),
                ("fetch.max.wait.ms", "100"),
                ("fetch.error.threshold", "2"),
            ]),
        ))
        .await
        .expect("source open");

    // Let the reader establish a few successful fetches first.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        matches!(
            source.health_check(),
            HealthStatus::Healthy | HealthStatus::Degraded(_)
        ),
        "expected healthy before stream delete, got {:?}",
        source.health_check()
    );

    // Pull the stream out from under the running source.
    ctx.delete_stream("DROP").await.expect("delete_stream");

    // Wait for consecutive errors to accumulate past the threshold.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut final_status = source.health_check();
    while tokio::time::Instant::now() < deadline {
        final_status = source.health_check();
        if matches!(final_status, HealthStatus::Unhealthy(_)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    source.close().await.expect("source close");
    assert!(
        matches!(final_status, HealthStatus::Unhealthy(_)),
        "expected Unhealthy after stream delete, got {final_status:?}"
    );
}

// ── chaos ──

fn restart_nats() {
    let compose_file = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/docker-compose.nats.yml");
    let status = std::process::Command::new("docker")
        .args(["compose", "-f", compose_file, "restart", "nats"])
        .status()
        .expect("docker compose restart nats");
    assert!(status.success(), "docker compose restart exited non-zero");
}

fn dedup_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn dedup_batch(ids: std::ops::Range<i64>) -> RecordBatch {
    let evt_ids: Vec<String> = ids.clone().map(|i| format!("evt-{i}")).collect();
    let values: Vec<i64> = ids.collect();
    RecordBatch::try_new(
        dedup_schema(),
        vec![
            Arc::new(StringArray::from(
                evt_ids.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Exactly-once sink survives a broker restart. Publish 50 rows →
/// `docker compose restart nats` → publish 0..100 (first 50 collide
/// with the pre-restart dedup ids). Final stream must hold exactly
/// 100 unique messages.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker compose available on host; chaos test restarts the broker"]
async fn exactly_once_survives_broker_restart() {
    let ctx = connect().await;
    reset_stream(
        &ctx,
        StreamConfig {
            name: "RESTART".into(),
            subjects: vec!["restart.events".into()],
            duplicate_window: Duration::from_secs(300),
            ..Default::default()
        },
    )
    .await;

    let schema = dedup_schema();
    let props = source_props(&[
        ("servers", NATS_URL),
        ("subject", "restart.events"),
        ("stream", "RESTART"),
        ("delivery.guarantee", "exactly_once"),
        ("dedup.id.column", "event_id"),
        ("format", "json"),
        ("ack.timeout.ms", "10000"),
    ]);

    let mut sink = NatsSink::new(schema.clone(), None);
    sink.open(&ConnectorConfig::with_properties("nats", props.clone()))
        .await
        .expect("sink open");

    // First epoch: publish rows 0..50.
    sink.write_batch(&dedup_batch(0..50))
        .await
        .expect("first batch publish");
    sink.pre_commit(1).await.expect("first epoch drain");

    // JetStream persists dedup state across restart.
    restart_nats();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Client connection is gone — reopen.
    let _ = sink.close().await;
    let mut sink = NatsSink::new(schema, None);
    sink.open(&ConnectorConfig::with_properties("nats", props))
        .await
        .expect("sink reopen after restart");

    // Re-publish 0..50 (server dedups) + publish 50..100.
    sink.write_batch(&dedup_batch(0..100))
        .await
        .expect("second batch publish");
    sink.pre_commit(2).await.expect("second epoch drain");
    sink.close().await.expect("sink close");

    let mut stream = ctx.get_stream("RESTART").await.expect("get_stream");
    let info = stream.info().await.expect("info");
    assert_eq!(
        info.state.messages, 100,
        "expected 100 unique messages after dedup; got {}",
        info.state.messages
    );
}

/// A source that never commits the epoch must not ack. After
/// `ack_wait`, a fresh consumer on the same durable replays;
/// committing that one drains the queue for good.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker compose -f docker-compose.nats.yml up"]
async fn source_redelivers_when_epoch_not_committed() {
    let ctx = connect().await;
    reset_stream(
        &ctx,
        StreamConfig {
            name: "ACKTEST".into(),
            subjects: vec!["acktest.events".into()],
            ..Default::default()
        },
    )
    .await;

    for i in 0..3i64 {
        let payload = format!(r#"{{"id": {i}, "name": "row-{i}"}}"#);
        ctx.publish("acktest.events".to_string(), payload.into())
            .await
            .expect("publish")
            .await
            .expect("ack");
    }

    let props = source_props(&[
        ("servers", NATS_URL),
        ("stream", "ACKTEST"),
        ("consumer", "acktest-consumer"),
        ("subject", "acktest.events"),
        ("format", "json"),
        ("ack.wait.ms", "2000"),
        ("fetch.max.wait.ms", "250"),
    ]);

    let mut source = NatsSource::new(payload_schema(), None);
    source
        .open(&ConnectorConfig::with_properties("nats", props.clone()))
        .await
        .expect("source 1 open");
    let first = drain_rows(&mut source, 3, Duration::from_secs(5)).await;
    source.close().await.expect("source 1 close");

    // Wait past ack_wait so the server unparks the unacked set.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut source = NatsSource::new(payload_schema(), None);
    source
        .open(&ConnectorConfig::with_properties("nats", props.clone()))
        .await
        .expect("source 2 open");
    let second = drain_rows(&mut source, 3, Duration::from_secs(5)).await;
    assert_eq!(
        first, second,
        "expected the same messages redelivered after no-commit close"
    );
    source
        .notify_epoch_committed(1)
        .await
        .expect("epoch commit acks");
    source.close().await.expect("source 2 close");

    let mut source = NatsSource::new(payload_schema(), None);
    source
        .open(&ConnectorConfig::with_properties("nats", props))
        .await
        .expect("source 3 open");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut saw_redelivery = false;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(batch)) = source.poll_batch(10).await {
            if batch.records.num_rows() > 0 {
                saw_redelivery = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    source.close().await.expect("source 3 close");
    assert!(
        !saw_redelivery,
        "expected no messages after source 2 committed — got a redelivery"
    );
}

// ── auth ──

const SECURE_NATS_URL: &str = "nats://127.0.0.1:4223";

/// Core-mode round-trip against a user/password-protected server.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs `docker compose -f docker-compose.nats.yml --profile secure up`"]
async fn user_pass_roundtrip_core() {
    let mut source = NatsSource::new(payload_schema(), None);
    source
        .open(&ConnectorConfig::with_properties(
            "nats",
            source_props(&[
                ("servers", SECURE_NATS_URL),
                ("mode", "core"),
                ("subject", "auth.events"),
                ("format", "json"),
                ("auth.mode", "user_pass"),
                ("user", "alice"),
                ("password", "wonderland"),
            ]),
        ))
        .await
        .expect("source open with user_pass");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sink = NatsSink::new(payload_schema(), None);
    sink.open(&ConnectorConfig::with_properties(
        "nats",
        source_props(&[
            ("servers", SECURE_NATS_URL),
            ("mode", "core"),
            ("subject", "auth.events"),
            ("format", "json"),
            ("auth.mode", "user_pass"),
            ("user", "alice"),
            ("password", "wonderland"),
        ]),
    ))
    .await
    .expect("sink open with user_pass");
    sink.write_batch(&test_batch(&[100, 101]))
        .await
        .expect("publish");
    sink.close().await.expect("sink close");

    let received = drain_rows(&mut source, 2, Duration::from_secs(3)).await;
    source.close().await.expect("source close");
    assert_eq!(
        received,
        vec![(100, "row-100".to_string()), (101, "row-101".to_string())]
    );
}

/// Connecting to an auth-required server without credentials must fail.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs `docker compose -f docker-compose.nats.yml --profile secure up`"]
async fn user_pass_missing_credentials_rejected_by_server() {
    let mut sink = NatsSink::new(payload_schema(), None);
    let result = sink
        .open(&ConnectorConfig::with_properties(
            "nats",
            source_props(&[
                ("servers", SECURE_NATS_URL),
                ("mode", "core"),
                ("subject", "auth.events"),
                ("format", "json"),
                // no auth.mode
            ]),
        ))
        .await;
    assert!(
        result.is_err(),
        "expected unauthenticated open to fail, got Ok"
    );
}
