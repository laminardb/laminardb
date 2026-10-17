//! RabbitMQ source and sink against a real broker (`tests/docker/compose.yml` service
//! `rabbitmq`, pinned `rabbitmq:4.3.6-management`).
//!
//! ```text
//! docker compose -f tests/docker/compose.yml up -d --wait rabbitmq
//! LAMINAR_TEST_RABBITMQ_CONTAINER=laminardb-rabbitmq \
//!   cargo test -p laminar-connectors --features rabbitmq --test rabbitmq_integration
//! ```
//!
//! Tests skip when the broker is unreachable unless `LAMINAR_REQUIRE_RABBITMQ=1`. Tests that
//! change broker policy or force-close connections run `rabbitmqctl` in the container named by
//! `LAMINAR_TEST_RABBITMQ_CONTAINER` and skip without it.

#![cfg(feature = "rabbitmq")]

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures_util::StreamExt;
use lapin::options::{
    BasicConsumeOptions, BasicGetOptions, BasicPublishOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::rabbitmq::metadata::MetadataColumns;
use laminar_connectors::rabbitmq::{RabbitMqSink, RabbitMqSource};

const DEFAULT_URL: &str = "amqp://guest:guest@127.0.0.1:5672/%2f";
static NEXT: AtomicU64 = AtomicU64::new(0);

fn unique(name: &str) -> String {
    format!(
        "laminar.it.{name}.{}.{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn broker() -> Option<(String, Connection)> {
    let url = std::env::var("LAMINAR_TEST_RABBITMQ_URL").unwrap_or_else(|_| DEFAULT_URL.into());
    let attempt = tokio::time::timeout(
        Duration::from_secs(3),
        Connection::connect(&url, ConnectionProperties::default()),
    )
    .await;
    match attempt {
        Ok(Ok(connection)) => Some((url, connection)),
        _ if std::env::var("LAMINAR_REQUIRE_RABBITMQ").is_ok_and(|v| v == "1") => {
            panic!("RabbitMQ is required but {url} is unreachable")
        }
        _ => {
            eprintln!("skipping: RabbitMQ not reachable");
            None
        }
    }
}

/// `rabbitmqctl` inside the broker container, when the harness names one.
fn rabbitmqctl(args: &[&str]) -> Option<String> {
    let container = std::env::var("LAMINAR_TEST_RABBITMQ_CONTAINER").ok()?;
    let output = Command::new("docker")
        .args(["exec", &container, "rabbitmqctl"])
        .args(args)
        .output()
        .expect("docker exec rabbitmqctl");
    assert!(
        output.status.success(),
        "rabbitmqctl {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

async fn declare_queue(channel: &Channel, queue: &str, arguments: FieldTable) {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .expect("declare test queue");
}

async fn ready_count(channel: &Channel, queue: &str) -> u32 {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare")
        .message_count()
}

async fn wait_ready(channel: &Channel, queue: &str, expected: u32) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let count = ready_count(channel, queue).await;
        if count == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "queue {queue} holds {count} ready messages, expected {expected}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn publish(channel: &Channel, queue: &str, payloads: &[Vec<u8>]) {
    for payload in payloads {
        let confirm = channel
            .basic_publish(
                "".into(),
                queue.into(),
                BasicPublishOptions::default(),
                payload,
                BasicProperties::default()
                    .with_delivery_mode(2)
                    .with_message_id(format!("m-{}", payload.len()).into()),
            )
            .await
            .expect("publish");
        drop(confirm);
    }
}

fn json(ids: std::ops::Range<i64>) -> Vec<Vec<u8>> {
    ids.map(|id| format!(r#"{{"id":{id}}}"#).into_bytes())
        .collect()
}

fn ids_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn connector_config(url: &str, pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("rabbitmq");
    config.set("url", url);
    for (key, value) in pairs {
        config.set(*key, *value);
    }
    config
}

async fn started_source(
    url: &str,
    queue: &str,
    payload: SchemaRef,
    pairs: &[(&str, &str)],
) -> Result<RabbitMqSource, laminar_connectors::error::ConnectorError> {
    let mut all = vec![("queue", queue), ("format", "json")];
    all.extend_from_slice(pairs);
    let config = connector_config(url, &all);
    let output = MetadataColumns::parse(config.get("metadata.columns"))
        .unwrap()
        .output_schema(&payload);
    let mut source = RabbitMqSource::new(output, None);
    source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await?;
    Ok(source)
}

/// Poll until `rows` rows arrived or `wait` passes; returns every batch.
async fn poll_rows(source: &mut RabbitMqSource, rows: usize, wait: Duration) -> Vec<RecordBatch> {
    let deadline = tokio::time::Instant::now() + wait;
    let mut batches = Vec::new();
    let mut seen = 0;
    while seen < rows && tokio::time::Instant::now() < deadline {
        match source.poll_batch(1_000).await.expect("poll") {
            Some(batch) => {
                seen += batch.num_rows();
                batches.push(batch.records);
            }
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    batches
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn source_acknowledges_only_committed_deliveries() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("commit");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    publish(&channel, &queue, &json(0..10)).await;

    let mut source = started_source(&url, &queue, ids_schema(), &[])
        .await
        .unwrap();
    assert_eq!(
        ids(&poll_rows(&mut source, 10, Duration::from_secs(10)).await),
        (0..10).collect::<Vec<_>>()
    );
    let first_cut = source.checkpoint();
    publish(&channel, &queue, &json(10..15)).await;
    assert_eq!(
        ids(&poll_rows(&mut source, 5, Duration::from_secs(10)).await).len(),
        5
    );
    // Nothing is released before a durable checkpoint names it.
    source.notify_epoch_committed(1, &first_cut).await.unwrap();
    source.notify_epoch_committed(1, &first_cut).await.unwrap();
    source
        .notify_epoch_committed(2, &SourceCheckpoint::new())
        .await
        .unwrap();
    source.close().await.unwrap();
    wait_ready(&channel, &queue, 5).await;

    let mut replay = started_source(
        &url,
        &queue,
        ids_schema(),
        &[("metadata.columns", "redelivered")],
    )
    .await
    .unwrap();
    let batches = poll_rows(&mut replay, 5, Duration::from_secs(10)).await;
    assert_eq!(ids(&batches), (10..15).collect::<Vec<_>>());
    assert!(batches[0]
        .column(1)
        .as_boolean()
        .iter()
        .all(|flag| flag == Some(true)));
    // A cursor from the closed generation must not acknowledge the new channel's deliveries.
    replay.notify_epoch_committed(3, &first_cut).await.unwrap();
    let cut = replay.checkpoint();
    replay.notify_epoch_committed(4, &cut).await.unwrap();
    replay.close().await.unwrap();
    wait_ready(&channel, &queue, 0).await;
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn prefetch_bounds_unacknowledged_deliveries() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("prefetch");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    publish(&channel, &queue, &json(0..10)).await;

    let mut source = started_source(&url, &queue, ids_schema(), &[("prefetch.count", "4")])
        .await
        .unwrap();
    assert_eq!(
        ids(&poll_rows(&mut source, 10, Duration::from_millis(800)).await).len(),
        4
    );
    let cut = source.checkpoint();
    source.notify_epoch_committed(1, &cut).await.unwrap();
    assert_eq!(
        ids(&poll_rows(&mut source, 4, Duration::from_secs(5)).await),
        vec![4, 5, 6, 7]
    );
    source.close().await.unwrap();
    wait_ready(&channel, &queue, 6).await;
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn undecodable_message_fails_closed_and_is_redelivered() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("poison");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    publish(
        &channel,
        &queue,
        &[br#"{"id":1}"#.to_vec(), b"not json".to_vec()],
    )
    .await;

    let mut source = started_source(&url, &queue, ids_schema(), &[])
        .await
        .unwrap();
    let error = loop {
        match source.poll_batch(10).await {
            Err(error) => break error,
            Ok(Some(_)) => panic!("a batch containing an undecodable message was handed off"),
            Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    };
    assert!(
        !error.is_transient(),
        "decode failure must end the generation: {error}"
    );
    assert!(
        source.poll_batch(10).await.is_err(),
        "a failed generation stays failed"
    );
    source.close().await.unwrap();
    wait_ready(&channel, &queue, 2).await;
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_messages_follow_the_operator_dead_letter_policy() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("dlx.src");
    let dead = unique("dlx.dead");
    let exchange = unique("dlx.ex");
    channel
        .exchange_declare(
            exchange.as_str().into(),
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    declare_queue(&channel, &dead, FieldTable::default()).await;
    channel
        .queue_bind(
            dead.as_str().into(),
            exchange.as_str().into(),
            "".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    // Dead-lettering belongs to the operator: a policy when the harness can set one, otherwise
    // the queue's own declaration argument. The connector sends neither.
    let policy = unique("dlx-policy");
    let by_policy = rabbitmqctl(&[
        "set_policy",
        &policy,
        &format!("^{}$", regex_escape(&queue)),
        &format!(r#"{{"dead-letter-exchange":"{exchange}"}}"#),
        "--apply-to",
        "queues",
    ])
    .is_some();
    let mut arguments = FieldTable::default();
    if !by_policy {
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(exchange.as_str().into()),
        );
    }
    declare_queue(&channel, &queue, arguments).await;
    publish(
        &channel,
        &queue,
        &[
            br#"{"id":1}"#.to_vec(),
            b"\xff\xfe".to_vec(),
            br#"{"id":3}"#.to_vec(),
        ],
    )
    .await;

    let mut source = started_source(&url, &queue, ids_schema(), &[("on.decode.error", "reject")])
        .await
        .unwrap();
    assert_eq!(
        ids(&poll_rows(&mut source, 2, Duration::from_secs(10)).await),
        vec![1, 3]
    );
    wait_ready(&channel, &dead, 1).await;
    let dead_letter = channel
        .basic_get(dead.as_str().into(), BasicGetOptions { no_ack: true })
        .await
        .unwrap()
        .expect("dead-lettered message");
    assert_eq!(dead_letter.delivery.data, b"\xff\xfe");
    let cut = source.checkpoint();
    source.notify_epoch_committed(1, &cut).await.unwrap();
    source.close().await.unwrap();
    wait_ready(&channel, &queue, 0).await;
    if by_policy {
        rabbitmqctl(&["clear_policy", &policy]);
    }
    for name in [&queue, &dead] {
        channel
            .queue_delete(name.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
}

fn regex_escape(name: &str) -> String {
    name.replace('.', "\\.")
}

#[tokio::test(flavor = "multi_thread")]
async fn mutable_policy_changes_apply_without_redeploying_the_connector() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    if std::env::var("LAMINAR_TEST_RABBITMQ_CONTAINER").is_err() {
        eprintln!("skipping: policy test needs LAMINAR_TEST_RABBITMQ_CONTAINER");
        return;
    }
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("policy");
    let policy = unique("limit");
    // Opt-in declaration with only the immutable queue type: the operator's policy stays
    // authoritative for mutable limits.
    let mut source = started_source(
        &url,
        &queue,
        ids_schema(),
        &[
            ("queue.declare", "true"),
            ("queue.type", "quorum"),
            ("prefetch.count", "1"),
        ],
    )
    .await
    .unwrap();
    rabbitmqctl(&[
        "set_policy",
        &policy,
        &format!("^{}$", regex_escape(&queue)),
        r#"{"max-length":3,"overflow":"drop-head"}"#,
        "--apply-to",
        "queues",
    ]);
    let applied = rabbitmqctl(&["list_queues", "name", "policy", "--formatter", "json"]).unwrap();
    assert!(applied.contains(&policy), "policy not applied: {applied}");
    publish(&channel, &queue, &json(0..10)).await;
    // One delivery is unacknowledged by the consumer; drop-head keeps the newest three ready.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ready = ready_count(&channel, &queue).await;
    assert!(
        ready <= 3,
        "policy max-length was not enforced while consuming: {ready} ready"
    );
    let batches = poll_rows(&mut source, 1, Duration::from_secs(5)).await;
    assert_eq!(ids(&batches).len(), 1);
    // Tighten the policy in place; the running connector is not reconfigured.
    rabbitmqctl(&[
        "set_policy",
        &policy,
        &format!("^{}$", regex_escape(&queue)),
        r#"{"max-length":1,"overflow":"drop-head"}"#,
        "--apply-to",
        "queues",
    ]);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(ready_count(&channel, &queue).await <= 1);
    // Restarting with the same declaration is idempotent and does not fight the policy.
    source.close().await.unwrap();
    let restarted = started_source(
        &url,
        &queue,
        ids_schema(),
        &[("queue.declare", "true"), ("queue.type", "quorum")],
    )
    .await;
    assert!(
        restarted.is_ok(),
        "redeclaring under a policy failed: {:?}",
        restarted.err()
    );
    restarted.unwrap().close().await.unwrap();
    rabbitmqctl(&["clear_policy", &policy]);
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn incompatible_declaration_is_rejected_without_changing_the_queue() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("incompatible");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    publish(&channel, &queue, &json(0..2)).await;
    let error = started_source(
        &url,
        &queue,
        ids_schema(),
        &[("queue.declare", "true"), ("queue.type", "quorum")],
    )
    .await
    .err()
    .expect("a classic queue redeclared as quorum must be rejected");
    assert!(
        error.to_string().contains("different declaration"),
        "{error}"
    );
    let missing = started_source(&url, &unique("missing"), ids_schema(), &[])
        .await
        .err()
        .unwrap();
    assert!(missing.to_string().contains("does not exist"), "{missing}");
    wait_ready(&channel, &queue, 2).await;
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_queues_are_rejected_before_handoff() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("stream");
    let mut arguments = FieldTable::default();
    arguments.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("stream".into()),
    );
    declare_queue(&channel, &queue, arguments).await;
    let mut source = started_source(&url, &queue, ids_schema(), &[])
        .await
        .unwrap();
    // A stream consumer without x-stream-offset attaches at `next`: publish after attaching.
    tokio::time::sleep(Duration::from_millis(200)).await;
    publish(&channel, &queue, &json(0..1)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let error = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no stream delivery arrived"
        );
        match source.poll_batch(10).await {
            Err(error) => break error,
            Ok(Some(_)) => panic!("a stream-queue delivery was handed off"),
            Ok(None) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    assert!(error.to_string().contains("stream queue"), "{error}");
    source.close().await.unwrap();
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_connection_ends_the_generation_and_requeues() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    if std::env::var("LAMINAR_TEST_RABBITMQ_CONTAINER").is_err() {
        eprintln!("skipping: connection-loss test needs LAMINAR_TEST_RABBITMQ_CONTAINER");
        return;
    }
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("lost");
    let name = unique("lost-conn");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    publish(&channel, &queue, &json(0..3)).await;
    let mut source = started_source(&url, &queue, ids_schema(), &[("connection.name", &name)])
        .await
        .unwrap();
    assert_eq!(
        ids(&poll_rows(&mut source, 3, Duration::from_secs(10)).await).len(),
        3
    );
    let cut = source.checkpoint();
    let connections = rabbitmqctl(&["list_connections", "pid", "client_properties"]).unwrap();
    let pid = connections
        .lines()
        .find(|line| line.contains(&name))
        .and_then(|line| line.split_whitespace().next())
        .expect("connector connection listed")
        .to_string();
    rabbitmqctl(&["close_connection", &pid, "test forced close"]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let error = loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "connection loss was not observed"
        );
        match source.poll_batch(10).await {
            Err(error) => break error,
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    assert!(!error.is_transient(), "{error}");
    // The committed cursor names the dead channel: nothing is acknowledged and the broker
    // requeues the deliveries for the restarted generation.
    source.notify_epoch_committed(1, &cut).await.unwrap();
    assert!(source.poll_batch(1).await.is_err());
    wait_ready(&channel, &queue, 3).await;
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

fn sink_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Binary, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, true),
        Field::new("prio", DataType::Int64, true),
        Field::new("tenant", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from(vec![
                &[0xff_u8, 0x00, 0x80][..],
                &b"low"[..],
                &b"high"[..],
            ])),
            Arc::new(StringArray::from(vec!["route", "route", "route"])),
            Arc::new(StringArray::from(vec![Some("m-1"), None, Some("m-3")])),
            Arc::new(Int64Array::from(vec![Some(5), Some(1), Some(9)])),
            Arc::new(Int64Array::from(vec![Some(42), None, Some(7)])),
        ],
    )
    .unwrap()
}

async fn opened_sink(url: &str, pairs: &[(&str, &str)]) -> RabbitMqSink {
    let mut sink = RabbitMqSink::new(sink_batch().schema(), None);
    sink.open(&connector_config(url, pairs))
        .await
        .expect("open sink");
    sink
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_publishes_confirmed_bytes_with_native_properties_and_priority() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let exchange = unique("sink.ex");
    let queue = unique("sink.q");
    channel
        .exchange_declare(
            exchange.as_str().into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut arguments = FieldTable::default();
    arguments.insert("x-max-priority".into(), AMQPValue::ShortShortUInt(10));
    declare_queue(&channel, &queue, arguments).await;
    channel
        .queue_bind(
            queue.as_str().into(),
            exchange.as_str().into(),
            "route".into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut sink = opened_sink(
        &url,
        &[
            ("exchange", &exchange),
            ("format", "raw"),
            ("routing.key.column", "key"),
            ("message.id.column", "id"),
            ("priority.column", "prio"),
            ("header.columns", "tenant"),
            ("app.id", "laminardb-it"),
        ],
    )
    .await;
    let written = sink.write_batch(&sink_batch()).await.unwrap();
    assert_eq!(written.records_written, 3);
    sink.flush().await.unwrap();
    sink.close().await.unwrap();
    wait_ready(&channel, &queue, 3).await;

    let mut consumer = channel
        .basic_consume(
            queue.as_str().into(),
            "".into(),
            BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut received = Vec::new();
    for _ in 0..3 {
        let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        received.push(delivery);
    }
    // Priority queue order: 9, 5, 1.
    let bodies: Vec<&[u8]> = received.iter().map(|d| d.data.as_slice()).collect();
    assert_eq!(
        bodies,
        vec![&b"high"[..], &[0xff, 0x00, 0x80][..], &b"low"[..]]
    );
    let first = &received[1].properties;
    assert_eq!(*first.delivery_mode(), Some(2));
    assert_eq!(*first.priority(), Some(5));
    assert_eq!(
        first.message_id().as_ref().map(ShortString::as_str),
        Some("m-1")
    );
    assert_eq!(
        first.app_id().as_ref().map(ShortString::as_str),
        Some("laminardb-it")
    );
    assert_eq!(
        first.content_type().as_ref().map(ShortString::as_str),
        Some("application/octet-stream")
    );
    assert_eq!(
        first
            .headers()
            .as_ref()
            .unwrap()
            .inner()
            .get(&ShortString::from("tenant")),
        Some(&AMQPValue::LongLongInt(42))
    );
    assert!(received[2].properties.message_id().is_none());
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_mandatory_return_fails_instead_of_dropping() {
    let Some((url, _admin)) = broker().await else {
        return;
    };
    let pairs = [
        ("exchange", "amq.direct"),
        ("format", "raw"),
        ("routing.key", "laminar.nobody.bound"),
    ];
    let batch = sink_batch().project(&[0]).unwrap();
    let mut sink_one = RabbitMqSink::new(batch.schema(), None);
    sink_one
        .open(&connector_config(&url, &pairs))
        .await
        .unwrap();
    sink_one.write_batch(&batch).await.unwrap();
    let error = sink_one.flush().await.unwrap_err();
    assert!(error.to_string().contains("unroutable"), "{error}");
    assert!(
        sink_one.write_batch(&batch).await.is_err(),
        "a failed sink generation stays failed"
    );
    // Without mandatory, the broker confirms and discards: route assurance needs mandatory.
    let mut discarding = RabbitMqSink::new(batch.schema(), None);
    let mut lenient = connector_config(&url, &pairs);
    lenient.set("mandatory", "false");
    discarding.open(&lenient).await.unwrap();
    discarding.write_batch(&batch).await.unwrap();
    discarding.flush().await.unwrap();
    discarding.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_rejects_a_missing_exchange_at_open() {
    let Some((url, _admin)) = broker().await else {
        return;
    };
    let missing = unique("missing.ex");
    let mut sink = RabbitMqSink::new(sink_batch().project(&[0]).unwrap().schema(), None);
    let error = sink
        .open(&connector_config(
            &url,
            &[
                ("exchange", &missing),
                ("routing.key", "x"),
                ("format", "raw"),
            ],
        ))
        .await
        .expect_err("missing exchange");
    assert!(error.to_string().contains("does not exist"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_pending_confirms_pipeline_large_batches() {
    let Some((url, admin)) = broker().await else {
        return;
    };
    let channel = admin.create_channel().await.unwrap();
    let queue = unique("pipelined");
    declare_queue(&channel, &queue, FieldTable::default()).await;
    let rows = 5_000;
    let payloads: Vec<String> = (0..rows).map(|i| format!("{i:08}")).collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(
            payloads.iter().map(String::as_str).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();
    let mut sink = RabbitMqSink::new(schema, None);
    sink.open(&connector_config(
        &url,
        &[
            ("routing.key", &queue),
            ("format", "raw"),
            ("max.pending", "64"),
            ("max.pending.bytes", "4096"),
        ],
    ))
    .await
    .unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    sink.close().await.unwrap();
    wait_ready(&channel, &queue, rows).await;
    let first = channel
        .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: true })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.delivery.data, b"00000000");
    assert!(first
        .delivery
        .properties
        .content_type()
        .as_ref()
        .is_some_and(|t| t.as_str().starts_with("text/plain")));
    channel
        .queue_delete(queue.as_str().into(), QueueDeleteOptions::default())
        .await
        .unwrap();
}

/// A RabbitMQ container with a TLS listener that requires a client certificate, using a CA,
/// server certificate (`localhost` only), and client certificate generated for this test.
struct TlsBroker {
    container: String,
    directory: std::path::PathBuf,
}

impl Drop for TlsBroker {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn distinguished(common_name: &str) -> rcgen::DistinguishedName {
    let mut name = rcgen::DistinguishedName::new();
    name.push(rcgen::DnType::CommonName, common_name);
    name
}

async fn tls_broker() -> Option<TlsBroker> {
    if std::env::var("LAMINAR_TEST_RABBITMQ_TLS").is_err() {
        eprintln!(
            "skipping: TLS test needs LAMINAR_TEST_RABBITMQ_TLS=1 (starts its own container)"
        );
        return None;
    }
    let directory = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(unique("tls"));
    std::fs::create_dir_all(&directory).unwrap();
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    // Erlang's verifier insists on a CA allowed to sign certificates.
    ca_params.distinguished_name = distinguished("LaminarDB Test CA");
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let leaf = |names: Vec<String>, usage: rcgen::ExtendedKeyUsagePurpose| {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(names.clone()).unwrap();
        // rcgen's default subject equals the CA's, which a verifier reads as self-signed.
        params.distinguished_name = distinguished(&names[0]);
        params.extended_key_usages = vec![usage];
        let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    };
    let (server_cert, server_key) = leaf(
        vec!["localhost".into()],
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
    );
    let (client_cert, client_key) = leaf(
        vec!["laminar-client".into()],
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    );
    for (name, contents) in [
        ("ca.pem", ca.pem()),
        ("server.pem", server_cert),
        ("server.key", server_key),
        ("client.pem", client_cert),
        ("client.key", client_key),
        (
            "rabbitmq.conf",
            "listeners.ssl.default = 5671\nssl_options.cacertfile = /certs/ca.pem\n\
             ssl_options.certfile = /certs/server.pem\nssl_options.keyfile = /certs/server.key\n\
             ssl_options.verify = verify_peer\nssl_options.fail_if_no_peer_cert = true\n\
             loopback_users = none\n"
                .to_string(),
        ),
    ] {
        std::fs::write(directory.join(name), contents).unwrap();
    }
    let container = unique("laminardb-rabbitmq-tls").replace('.', "-");
    let started = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &container,
            "-p",
            "127.0.0.1:15671:5671",
        ])
        .arg("-v")
        .arg(format!("{}:/certs:ro", directory.display()))
        .arg("-v")
        .arg(format!(
            "{}:/etc/rabbitmq/conf.d/20-tls.conf:ro",
            directory.join("rabbitmq.conf").display()
        ))
        .arg("rabbitmq:4.3.6-management")
        .output()
        .expect("docker run");
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    let broker = TlsBroker {
        container,
        directory,
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let ready = Command::new("docker")
            // As root, the probe would create the Erlang cookie before the broker can.
            .args([
                "exec",
                "-u",
                "rabbitmq",
                &broker.container,
                "rabbitmq-diagnostics",
                "-q",
                "check_port_listener",
                "5671",
            ])
            .output()
            .is_ok_and(|output| output.status.success());
        if ready {
            return Some(broker);
        }
        if tokio::time::Instant::now() >= deadline {
            let logs = Command::new("docker")
                .args(["logs", "--tail", "40", &broker.container])
                .output()
                .unwrap();
            panic!(
                "TLS broker did not start:\n{}{}",
                String::from_utf8_lossy(&logs.stdout),
                String::from_utf8_lossy(&logs.stderr)
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_verifies_the_broker_and_presents_a_client_certificate() {
    let Some(broker) = tls_broker().await else {
        return;
    };
    let path = |name: &str| broker.directory.join(name).display().to_string();
    let attempt = |url: &str, ca: bool, client: bool| {
        let mut pairs = vec![("url".to_string(), url.to_string())];
        if ca {
            pairs.push(("tls.ca.location".into(), path("ca.pem")));
        }
        if client {
            pairs.push(("tls.cert.location".into(), path("client.pem")));
            pairs.push(("tls.key.location".into(), path("client.key")));
        }
        pairs.push(("exchange".into(), "amq.direct".into()));
        pairs.push(("routing.key".into(), "nobody".into()));
        pairs.push(("format".into(), "raw".into()));
        pairs.push(("mandatory".into(), "false".into()));
        let mut config = ConnectorConfig::new("rabbitmq");
        for (key, value) in pairs {
            config.set(key, value);
        }
        async move {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )]));
            let mut sink = RabbitMqSink::new(schema, None);
            let opened = sink.open(&config).await;
            if opened.is_ok() {
                sink.close().await.unwrap();
            }
            opened
        }
    };
    let good = "amqps://guest:guest@localhost:15671/%2f";
    if let Err(error) = attempt(good, true, true).await {
        let logs = Command::new("docker")
            .args(["logs", "--tail", "15", &broker.container])
            .output()
            .unwrap();
        panic!(
            "trusted CA, matching host, client certificate: {error}\n{}",
            String::from_utf8_lossy(&logs.stdout)
        );
    }
    let untrusted = attempt(good, false, true)
        .await
        .expect_err("server CA not trusted");
    assert!(
        !untrusted.to_string().contains("guest:guest"),
        "{untrusted}"
    );
    attempt(good, true, false)
        .await
        .expect_err("the broker requires a client certificate");
    attempt("amqps://guest:guest@127.0.0.1:15671/%2f", true, true)
        .await
        .expect_err("the certificate names localhost, not 127.0.0.1");
}
