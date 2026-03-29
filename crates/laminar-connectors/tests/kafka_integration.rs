//! Integration tests for Kafka source connector.
//!
//! Requires Docker. Uses a single Redpanda container with a fixed host port.
//! All test scenarios run sequentially within one `#[tokio::test]` to avoid
//! port conflicts from parallel container starts.
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
use testcontainers::core::IntoContainerPort;
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

const REDPANDA_HOST_PORT: u16 = 19092;

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

fn make_config(brokers: &str, group_id: &str, topic: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        bootstrap_servers: brokers.to_string(),
        group_id: group_id.into(),
        subscription: TopicSubscription::Topics(vec![topic.into()]),
        ..KafkaSourceConfig::default()
    }
}

/// Single test entry point — starts one Redpanda container and runs all
/// scenarios sequentially to avoid fixed-port conflicts.
#[tokio::test]
async fn kafka_source_integration() {
    let _container = GenericImage::new("redpandadata/redpanda", "v24.3.1")
        .with_exposed_port(9092.into())
        .with_wait_for(testcontainers::core::WaitFor::message_on_stderr(
            "Successfully started Redpanda",
        ))
        .with_mapped_port(REDPANDA_HOST_PORT, 9092.tcp())
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
            "--advertise-kafka-addr",
            "PLAINTEXT://127.0.0.1:19092",
            "--node-id",
            "0",
        ])
        .start()
        .await
        .expect("failed to start Redpanda container");

    let brokers = format!("127.0.0.1:{REDPANDA_HOST_PORT}");
    sleep(Duration::from_secs(2)).await;

    roundtrip(&brokers).await;
    checkpoint_restore(&brokers).await;
    poison_pill(&brokers).await;
}

async fn roundtrip(brokers: &str) {
    let topic = "test-roundtrip";
    let n = 50;

    produce_messages(brokers, topic, n).await;

    let cfg = make_config(brokers, "test-roundtrip-group", topic);
    let mut source = KafkaSource::new(test_schema(), cfg);
    let connector_cfg = ConnectorConfig::new("kafka");
    source.open(&connector_cfg).await.unwrap();

    let batches = poll_all(&mut source, n, Duration::from_secs(30)).await;

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

async fn checkpoint_restore(brokers: &str) {
    let topic = "test-checkpoint";
    let n = 20;

    produce_messages(brokers, topic, n).await;

    let cfg = make_config(brokers, "test-checkpoint-group", topic);
    let connector_cfg = ConnectorConfig::new("kafka");

    let mut source = KafkaSource::new(test_schema(), cfg.clone());
    source.open(&connector_cfg).await.unwrap();

    poll_all(&mut source, n, Duration::from_secs(30)).await;
    let checkpoint = source.checkpoint();
    source.close().await.unwrap();

    let extra = 10;
    produce_messages(brokers, topic, extra).await;

    let mut source2 = KafkaSource::new(test_schema(), cfg);
    source2.open(&connector_cfg).await.unwrap();
    source2.restore(&checkpoint).await.unwrap();

    let batches = poll_all(&mut source2, extra, Duration::from_secs(30)).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, extra);

    source2.close().await.unwrap();
}

async fn poison_pill(brokers: &str) {
    let topic = "test-poison";

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creation");

    let payloads = [
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

    let cfg = KafkaSourceConfig {
        max_deser_error_rate: 0.5,
        ..make_config(brokers, "test-poison-group", topic)
    };

    let mut source = KafkaSource::new(test_schema(), cfg);
    let connector_cfg = ConnectorConfig::new("kafka");
    source.open(&connector_cfg).await.unwrap();

    let batches = poll_all(&mut source, 2, Duration::from_secs(30)).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);

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
