//! Integration tests for Kafka source connector.
//!
//! Requires Docker. Uses Redpanda as a lightweight Kafka-compatible broker.
//! Validates produce → consume roundtrip, checkpoint/restore, and poison
//! pill isolation.
//!
//! Run with: `cargo test --test kafka_integration --features kafka`

#![cfg(feature = "kafka")]
#![cfg(not(target_os = "windows"))]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::time::sleep;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::SourceConnector;
use laminar_connectors::kafka::{KafkaSource, KafkaSourceConfig, TopicSubscription};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Starts a Redpanda container and returns the broker address.
async fn start_redpanda() -> (testcontainers::ContainerAsync<GenericImage>, String) {
    // Redpanda listens on 9092 inside the container. We omit
    // --advertise-kafka-addr so Redpanda advertises its listen address.
    // rdkafka caches the bootstrap broker from the initial connection,
    // so metadata-redirect to the internal address is not an issue for
    // short-lived integration tests.
    let container = GenericImage::new("redpandadata/redpanda", "v24.3.1")
        .with_exposed_port(9092.into())
        .with_cmd([
            "redpanda",
            "start",
            "--smp",
            "1",
            "--memory",
            "256M",
            "--overprovisioned",
            "--kafka-addr",
            "PLAINTEXT://0.0.0.0:9092",
            "--node-id",
            "0",
        ])
        .with_wait_for(testcontainers::core::WaitFor::message_on_stderr(
            "Successfully started Redpanda",
        ))
        .start()
        .await
        .expect("failed to start Redpanda container");

    let port = container.get_host_port_ipv4(9092).await.expect("get port");
    let brokers = format!("127.0.0.1:{port}");

    // Brief wait for broker to accept connections.
    sleep(Duration::from_secs(2)).await;

    (container, brokers)
}

/// Produces N JSON messages to the given topic.
async fn produce_messages(brokers: &str, topic: &str, count: usize) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creation");

    for i in 0..count {
        let payload = format!(r#"{{"id": {i}, "name": "item-{i}"}}"#);
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key(&i.to_string()),
                Duration::from_secs(5),
            )
            .await
            .expect("send failed");
    }
}

/// Polls the source until `expected` records are collected or timeout.
async fn poll_all(
    source: &mut KafkaSource,
    expected: usize,
    timeout: Duration,
) -> Vec<arrow_array::RecordBatch> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut batches = Vec::new();
    let mut total = 0;

    while total < expected && tokio::time::Instant::now() < deadline {
        match source.poll_batch(1000).await {
            Ok(Some(batch)) => {
                total += batch.records.num_rows();
                batches.push(batch.records);
            }
            Ok(None) => sleep(Duration::from_millis(50)).await,
            Err(e) => panic!("poll_batch failed: {e}"),
        }
    }
    assert_eq!(total, expected, "expected {expected} records, got {total}");
    batches
}

#[tokio::test]
async fn test_produce_consume_roundtrip() {
    let (_container, brokers) = start_redpanda().await;
    let topic = "test-roundtrip";
    let n = 50;

    produce_messages(&brokers, topic, n).await;

    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = brokers.clone();
    cfg.group_id = "test-roundtrip-group".into();
    cfg.subscription = TopicSubscription::Topics(vec![topic.into()]);

    let mut source = KafkaSource::new(test_schema(), cfg);
    let connector_cfg = ConnectorConfig::new("kafka");
    source.open(&connector_cfg).await.unwrap();

    let batches = poll_all(&mut source, n, Duration::from_secs(30)).await;

    // Verify content.
    let mut seen_ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let id = ids.value(i);
            seen_ids.push(id);
            assert_eq!(names.value(i), format!("item-{id}"));
        }
    }
    seen_ids.sort_unstable();
    let expected: Vec<i64> = (0..n as i64).collect();
    assert_eq!(seen_ids, expected);

    source.close().await.unwrap();
}

#[tokio::test]
async fn test_checkpoint_restore() {
    let (_container, brokers) = start_redpanda().await;
    let topic = "test-checkpoint";
    let n = 20;

    produce_messages(&brokers, topic, n).await;

    // Phase 1: consume all, checkpoint.
    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = brokers.clone();
    cfg.group_id = "test-checkpoint-group".into();
    cfg.subscription = TopicSubscription::Topics(vec![topic.into()]);

    let mut source = KafkaSource::new(test_schema(), cfg.clone());
    let connector_cfg = ConnectorConfig::new("kafka");
    source.open(&connector_cfg).await.unwrap();

    poll_all(&mut source, n, Duration::from_secs(30)).await;
    let checkpoint = source.checkpoint();
    source.close().await.unwrap();

    // Produce more messages after checkpoint.
    let extra = 10;
    produce_messages(&brokers, topic, extra).await;

    // Phase 2: restore from checkpoint, should only get the extra messages.
    let mut source2 = KafkaSource::new(test_schema(), cfg);
    source2.open(&connector_cfg).await.unwrap();
    source2.restore(&checkpoint).await.unwrap();

    let batches = poll_all(&mut source2, extra, Duration::from_secs(30)).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, extra);

    source2.close().await.unwrap();
}

#[tokio::test]
async fn test_poison_pill_isolation() {
    let (_container, brokers) = start_redpanda().await;
    let topic = "test-poison";

    // Produce a mix of valid JSON and garbage.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creation");

    let payloads = vec![
        r#"{"id": 1, "name": "good-1"}"#,
        "NOT VALID JSON {{{",
        r#"{"id": 3, "name": "good-3"}"#,
    ];
    for (i, payload) in payloads.iter().enumerate() {
        producer
            .send(
                FutureRecord::to(topic)
                    .payload(*payload)
                    .key(&i.to_string()),
                Duration::from_secs(5),
            )
            .await
            .expect("send failed");
    }

    let mut cfg = KafkaSourceConfig::default();
    cfg.bootstrap_servers = brokers;
    cfg.group_id = "test-poison-group".into();
    cfg.subscription = TopicSubscription::Topics(vec![topic.into()]);
    cfg.max_deser_error_rate = 0.5; // tolerate up to 50% errors

    let mut source = KafkaSource::new(test_schema(), cfg);
    let connector_cfg = ConnectorConfig::new("kafka");
    source.open(&connector_cfg).await.unwrap();

    // Should get the 2 good records; the poison pill is skipped.
    let batches = poll_all(&mut source, 2, Duration::from_secs(30)).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);

    // Verify the good records came through.
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            ids.push(col.value(i));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 3]);

    source.close().await.unwrap();
}
