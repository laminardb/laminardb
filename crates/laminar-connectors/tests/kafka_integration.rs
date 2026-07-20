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
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use tokio::time::sleep;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::kafka::{KafkaSource, KafkaSourceConfig, TopicSubscription};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

const REDPANDA_HOST_PORT: u16 = 19092;

fn initial_source_start(config: &ConnectorConfig) -> SourceStart {
    SourceStart::new(
        config.clone(),
        SourcePosition::Initial,
        DeliveryGuarantee::BestEffort,
    )
    .unwrap()
}

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

async fn wait_for_group_offset(
    brokers: &str,
    group_id: &str,
    topic: &str,
    partition: i32,
    expected: i64,
) {
    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .create()
        .expect("group offset observer creation");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut requested = TopicPartitionList::new();
        requested.add_partition(topic, partition);
        let committed = observer
            .committed_offsets(requested, Duration::from_secs(1))
            .expect("committed offset query");
        if committed
            .find_partition(topic, partition)
            .is_some_and(|entry| entry.offset() == Offset::Offset(expected))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Kafka did not persist advisory group offset {topic}-{partition}@{expected}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn expand_topic_and_wait(brokers: &str, topic: &str, partition_count: usize) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Kafka admin creation");
    let change = NewPartitions::new(topic, partition_count);
    let results = admin
        .create_partitions([&change], &AdminOptions::new())
        .await
        .expect("partition expansion request");
    assert!(
        results.into_iter().all(|result| result.is_ok()),
        "Kafka rejected partition expansion"
    );

    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "partition-inventory-observer")
        .create()
        .expect("partition inventory observer creation");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let observed = observer
            .fetch_metadata(Some(topic), Duration::from_secs(1))
            .expect("partition inventory metadata query")
            .topics()
            .first()
            .map_or(0, |metadata| metadata.partitions().len());
        if observed == partition_count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Kafka metadata did not expose {partition_count} partitions for {topic}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_kafka(brokers: &str) {
    let observer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "startup-readiness-observer")
        .set("socket.timeout.ms", "1000")
        .create()
        .expect("Kafka readiness observer creation");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        if observer
            .fetch_metadata(None, Duration::from_secs(1))
            .is_ok_and(|metadata| !metadata.brokers().is_empty())
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Kafka metadata protocol was not ready within 30 seconds"
        );
        sleep(Duration::from_millis(100)).await;
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
    let _container = GenericImage::new("docker.redpanda.com/redpandadata/redpanda", "v26.1.13")
        .with_exposed_port(9092.into())
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
    wait_for_kafka(&brokers).await;

    roundtrip(&brokers).await;
    checkpoint_restore(&brokers).await;
    poison_pill(&brokers).await;
}

async fn roundtrip(brokers: &str) {
    let topic = "test-roundtrip";
    let n = 50;

    produce_messages(brokers, topic, n).await;

    let cfg = make_config(brokers, "test-roundtrip-group", topic);
    let mut source = KafkaSource::new(test_schema(), cfg, None);
    let connector_cfg = ConnectorConfig::new("kafka");
    source
        .start(initial_source_start(&connector_cfg))
        .await
        .unwrap();

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

    let mut cfg = make_config(brokers, "test-checkpoint-group", topic);
    cfg.startup_mode = laminar_connectors::kafka::StartupMode::Earliest;
    let connector_cfg = ConnectorConfig::new("kafka");

    let mut source = KafkaSource::new(test_schema(), cfg.clone(), None);
    source
        .start(
            SourceStart::new(
                connector_cfg.clone(),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // A checkpoint taken before the first accepted record still seals the complete partition
    // inventory. Restarting from it must begin at the configured stable baseline, not a moving
    // broker group cursor.
    let before_first_record = source.checkpoint();
    source.close().await.unwrap();
    let mut source = KafkaSource::new(test_schema(), cfg.clone(), None);
    source
        .start(
            SourceStart::new(
                connector_cfg.clone(),
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: before_first_record,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    poll_all(&mut source, n, Duration::from_secs(30)).await;
    let checkpoint = source.checkpoint();
    // The streaming coordinator drives this advisory monitoring cursor after a barrier. Engine
    // recovery below remains bound to its explicit checkpoint and never trusts the group offset.
    source.notify_epoch_committed(1, &checkpoint).await.unwrap();
    wait_for_group_offset(brokers, "test-checkpoint-group", topic, 0, n as i64).await;
    source.close().await.unwrap();

    let extra = 10;
    produce_messages(brokers, topic, extra).await;

    let mut source2 = KafkaSource::new(test_schema(), cfg.clone(), None);
    source2
        .start(
            SourceStart::new(
                connector_cfg,
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: checkpoint.clone(),
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // Coordinated recovery may complete a barrier while source intake remains
    // gated. The advisory progress commit must enqueue before the resumed
    // connector's first poll; the subsequent poll services its broker callback.
    source2
        .notify_epoch_committed(2, &checkpoint)
        .await
        .unwrap();

    let batches = poll_all(&mut source2, extra, Duration::from_secs(30)).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, extra);

    source2.close().await.unwrap();

    // A new partition has no position in the decided checkpoint. Guaranteed recovery rejects the
    // topology change instead of assigning that partition at a newer broker end and skipping it.
    expand_topic_and_wait(brokers, topic, 2).await;
    let mut changed = KafkaSource::new(test_schema(), cfg, None);
    let error = changed
        .start(
            SourceStart::new(
                ConnectorConfig::new("kafka"),
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("partition expansion must fail a guaranteed resume closed");
    assert!(error.to_string().contains("partition inventory changed"));
    changed.close().await.unwrap();
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

    let mut source = KafkaSource::new(test_schema(), cfg, None);
    let connector_cfg = ConnectorConfig::new("kafka");
    source
        .start(initial_source_start(&connector_cfg))
        .await
        .unwrap();

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
