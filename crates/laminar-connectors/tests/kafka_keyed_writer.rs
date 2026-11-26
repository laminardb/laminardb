//! A Kafka upsert topic written by key from several cluster processes, against a real broker: once
//! the leader fences a departed process's writer, nothing that process still sends can land after
//! its successor's write of the same key.
//!
//! It skips when the default local address is absent, but a configured `LAMINAR_KAFKA_BROKERS`
//! must be reachable and support transactions. Run with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19092 \
//!     cargo test -p laminar-connectors --features kafka --test kafka_keyed_writer

#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{DestinationWriter, SinkConnector};
use laminar_connectors::kafka::{register_kafka_sink, KafkaSink, KafkaSinkConfig};
use laminar_connectors::registry::ConnectorRegistry;

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";
const SINK: &str = "totals";
const TIMEOUT: Duration = Duration::from_secs(10);

fn broker() -> Option<String> {
    let configured = std::env::var("LAMINAR_KAFKA_BROKERS").ok();
    let brokers = configured.clone().unwrap_or_else(|| DEFAULT_BROKERS.into());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("probe client");
    match consumer.fetch_metadata(None, TIMEOUT) {
        Ok(metadata) if !metadata.brokers().is_empty() => Some(brokers),
        _ if configured.is_none() => None,
        _ => panic!("configured Kafka broker '{brokers}' is unreachable"),
    }
}

/// A one-partition upsert topic, so every key's records share one log order.
struct Topic {
    brokers: String,
    name: String,
}

impl Topic {
    async fn create() -> Option<Self> {
        let brokers = broker()?;
        let topic = Self {
            name: format!("keyed-writer-{}", uuid::Uuid::new_v4()),
            brokers,
        };
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &topic.brokers)
            .create()
            .expect("admin client");
        let new_topic = NewTopic::new(&topic.name, 1, TopicReplication::Fixed(1));
        for result in admin
            .create_topics([&new_topic], &AdminOptions::new())
            .await
            .expect("create topic")
        {
            result.expect("create topic");
        }
        topic.wait_for_leader();
        Some(topic)
    }

    /// A new topic's partition has no leader for a moment, and a sink opened then fails.
    fn wait_for_leader(&self) {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .expect("metadata client");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let led = consumer
                .fetch_metadata(Some(&self.name), TIMEOUT)
                .ok()
                .and_then(|metadata| {
                    metadata
                        .topics()
                        .iter()
                        .find(|topic| topic.name() == self.name && topic.error().is_none())
                        .map(|topic| topic.partitions().iter().all(|p| p.leader() >= 0))
                })
                .unwrap_or(false);
            if led {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{} has no leader",
                self.name
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn config(&self) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("kafka");
        for (key, value) in [
            ("bootstrap.servers", self.brokers.as_str()),
            ("topic", self.name.as_str()),
            ("format", "json"),
            ("key.column", "id"),
            ("key.format", "text"),
            ("partition.count", "1"),
            ("envelope", "upsert"),
            ("delivery.guarantee", "at_least_once"),
            ("transaction.timeout.ms", "10000"),
        ] {
            config.set(key, value);
        }
        config
    }

    /// `writer`'s sink, opened as a cluster process does.
    async fn sink(&self, writer: DestinationWriter) -> KafkaSink {
        let config = self.config();
        let parsed = KafkaSinkConfig::from_config(&config).expect("sink config");
        let mut sink = KafkaSink::new(schema(), parsed, None);
        sink.share_destination_by_key(writer);
        sink.open(&config).await.expect("open keyed writer");
        sink
    }

    /// What the leader does before a recovery that removes `departed` is recorded.
    async fn fence(&self, departed: &[DestinationWriter]) {
        let registry = ConnectorRegistry::new();
        register_kafka_sink(&registry).expect("register the Kafka sink");
        let fence = registry
            .sink_writer_fence("kafka")
            .expect("the Kafka sink registers a writer fence");
        fence(
            self.config(),
            SINK.into(),
            departed.iter().map(|writer| writer.process).collect(),
            tokio::time::Instant::now() + TIMEOUT,
        )
        .await
        .expect("fence departed writers");
    }

    /// The departed process's own producer, with a transaction it opened before its lease ended.
    fn departed_producer(&self, writer: &DestinationWriter) -> BaseProducer {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set(
                "transactional.id",
                format!(
                    "laminardb-writer-{}-{:016x}-{}",
                    writer.sink_id, writer.process.node_id, writer.process.boot_incarnation
                ),
            )
            .set("transaction.timeout.ms", "10000")
            .create()
            .expect("departed producer");
        producer.init_transactions(TIMEOUT).expect("init");
        producer.begin_transaction().expect("begin");
        producer
    }

    /// Values of key `id` in log order, read with `isolation`; `None` is a tombstone.
    fn values(&self, id: i64, isolation: &str) -> Vec<Option<String>> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", format!("keyed-writer-{}", uuid::Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("isolation.level", isolation)
            .create()
            .expect("verifier");
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(&self.name, 0, Offset::Beginning)
            .unwrap();
        consumer.assign(&assignment).unwrap();
        let key = id.to_string();
        let mut values = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "verifier never reached the end"
            );
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(message)) if message.key() == Some(key.as_bytes()) => values.push(
                    message
                        .payload()
                        .map(|value| String::from_utf8_lossy(value).into_owned()),
                ),
                Some(Ok(_)) | None => {}
                Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => return values,
                Some(Err(error)) => panic!("verifier: {error}"),
            }
        }
    }

    fn latest(&self, id: i64, isolation: &str) -> Option<String> {
        self.values(id, isolation).pop().flatten()
    }
}

fn writer(node_id: u64) -> DestinationWriter {
    DestinationWriter {
        sink_id: SINK.into(),
        process: laminar_core::checkpoint::CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::new_v4(),
        },
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("_op", DataType::Utf8, false),
    ]))
}

fn upsert(id: i64, value: &str) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![value])),
            Arc::new(StringArray::from(vec!["I"])),
        ],
    )
    .unwrap()
}

fn json(id: i64, value: &str) -> String {
    format!(r#"{{"id":{id},"value":"{value}"}}"#)
}

/// librdkafka delivers what the departed process queued before its lease ended, then commits.
fn deliver_late(producer: &BaseProducer, topic: &str) -> Result<(), String> {
    producer
        .send(
            BaseRecord::to(topic)
                .partition(0)
                .key("1")
                .payload(&json(1, "stale")),
        )
        .map_err(|(error, _)| error.to_string())?;
    producer.flush(TIMEOUT).map_err(|error| error.to_string())?;
    producer
        .commit_transaction(TIMEOUT)
        .map_err(|error| error.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_writer_cannot_land_a_record_after_its_successor() {
    let Some(topic) = Topic::create().await else {
        return;
    };
    let departed = writer(2);
    let late = topic.departed_producer(&departed);

    topic.fence(std::slice::from_ref(&departed)).await;
    let mut successor = topic.sink(writer(1)).await;
    successor.write_batch(&upsert(1, "new")).await.unwrap();

    assert!(deliver_late(&late, &topic.name).is_err());
    assert_eq!(topic.latest(1, "read_committed"), Some(json(1, "new")));
    assert_eq!(topic.latest(1, "read_uncommitted"), Some(json(1, "new")));
}

#[tokio::test(flavor = "multi_thread")]
async fn without_the_fence_a_departed_writer_overwrites_its_successor() {
    let Some(topic) = Topic::create().await else {
        return;
    };
    let late = topic.departed_producer(&writer(2));

    let mut successor = topic.sink(writer(1)).await;
    successor.write_batch(&upsert(1, "new")).await.unwrap();

    deliver_late(&late, &topic.name).unwrap();
    assert_eq!(topic.latest(1, "read_committed"), Some(json(1, "stale")));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_racing_the_fence_lands_before_the_successor_or_not_at_all() {
    let Some(topic) = Topic::create().await else {
        return;
    };
    let departed = writer(2);
    let late = topic.departed_producer(&departed);
    late.send(
        BaseRecord::to(&topic.name)
            .partition(0)
            .key("1")
            .payload(&json(1, "stale")),
    )
    .map_err(|(error, _)| error)
    .unwrap();
    late.flush(TIMEOUT).unwrap();

    let commit = std::thread::spawn(move || late.commit_transaction(TIMEOUT).is_ok());
    topic.fence(std::slice::from_ref(&departed)).await;
    let mut successor = topic.sink(writer(1)).await;
    successor.write_batch(&upsert(1, "new")).await.unwrap();

    let committed = commit.join().unwrap();
    let values = topic.values(1, "read_committed");
    let landed_first = vec![Some(json(1, "stale")), Some(json(1, "new"))];
    // A commit whose response was lost can still land, so only a successful one pins the log.
    if committed {
        assert_eq!(values, landed_first);
    } else {
        assert!(
            values == [Some(json(1, "new"))] || values == landed_first,
            "{values:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_sink_refuses_to_write() {
    let Some(topic) = Topic::create().await else {
        return;
    };
    let departed = writer(2);
    let mut stale = topic.sink(departed.clone()).await;
    stale.write_batch(&upsert(1, "old")).await.unwrap();

    topic.fence(&[departed]).await;

    assert!(stale.write_batch(&upsert(1, "stale")).await.is_err());
    assert_eq!(topic.latest(1, "read_committed"), Some(json(1, "old")));
}
