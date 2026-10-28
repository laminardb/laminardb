//! A Kafka source's recency probe: the end offset of each partition when a read arrives, and
//! whether a checkpoint delivered everything before it, including past transaction markers and
//! aborted records.
//!
//! Needs a real broker. It skips when the default local address is absent, but a configured
//! `LAMINAR_KAFKA_BROKERS` must be reachable. Run with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19092 \
//!     cargo test -p laminar-connectors --features kafka,testing --test kafka_recency -- --nocapture

#![cfg(all(feature = "kafka", feature = "testing"))]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{RecencyTarget, SourceConnector, SourceRecency};
use laminar_connectors::kafka::KafkaSource;

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";

fn broker() -> Result<Option<String>, String> {
    let configured = std::env::var("LAMINAR_KAFKA_BROKERS").ok();
    let brokers = configured.clone().unwrap_or_else(|| DEFAULT_BROKERS.into());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "group.id",
            format!("recency-probe-{}", uuid::Uuid::new_v4()),
        )
        .create()
        .map_err(|error| format!("invalid Kafka broker configuration '{brokers}': {error}"))?;
    match consumer.fetch_metadata(None, Duration::from_secs(1)) {
        Ok(metadata) if !metadata.brokers().is_empty() => Ok(Some(brokers)),
        Ok(_) | Err(_) if configured.is_some() => Err(format!(
            "configured Kafka broker '{brokers}' is unreachable"
        )),
        Ok(_) | Err(_) => Ok(None),
    }
}

async fn create_topic(brokers: &str, partitions: i32) -> String {
    let topic = format!("recency-{}", uuid::Uuid::new_v4());
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new = NewTopic::new(&topic, partitions, TopicReplication::Fixed(1));
    for result in admin
        .create_topics([&new], &AdminOptions::new())
        .await
        .expect("create_topics request")
    {
        result.expect("create topic");
    }
    topic
}

fn producer(brokers: &str, transactional: bool) -> BaseProducer {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    if transactional {
        config.set(
            "transactional.id",
            format!("recency-{}", uuid::Uuid::new_v4()),
        );
    }
    let producer: BaseProducer = config.create().expect("producer");
    if transactional {
        producer
            .init_transactions(Duration::from_secs(10))
            .expect("init transactions");
    }
    producer
}

fn send(producer: &BaseProducer, topic: &str, partition: i32, count: usize) {
    for index in 0..count {
        let payload = format!("{{\"id\":{index}}}");
        producer
            .send(
                BaseRecord::<(), String>::to(topic)
                    .partition(partition)
                    .payload(&payload),
            )
            .expect("enqueue record");
    }
    producer.flush(Duration::from_secs(10)).expect("flush");
}

fn probe(brokers: &str, topic: &str) -> Arc<dyn SourceRecency> {
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", brokers);
    config.set("group.id", format!("recency-{}", uuid::Uuid::new_v4()));
    config.set("topic", topic);
    config.set("format", "json");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    KafkaSource::new(schema, Default::default(), None)
        .recency(&config)
        .expect("valid Kafka configuration")
        .expect("an explicit topic list supports recency")
}

/// A checkpoint that delivered through `delivered` on each listed partition.
fn checkpoint(topic: &str, delivered: &[(i32, i64)]) -> SourceCheckpoint {
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_metadata("connector", "kafka");
    checkpoint.set_metadata("checkpoint.version", "2");
    for (partition, offset) in delivered {
        checkpoint.set_offset(format!("{topic}:{partition}"), offset.to_string());
    }
    checkpoint
}

/// The offsets of the records a `read_committed` reader sees; brokers place transaction control
/// batches differently, so tests take record offsets from the broker rather than assume them.
fn committed_offsets(brokers: &str, topic: &str) -> Vec<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("recency-read-{}", uuid::Uuid::new_v4()))
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .create()
        .expect("reader");
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let mut offsets = Vec::new();
    loop {
        match consumer.poll(Duration::from_secs(10)).expect("poll") {
            Ok(message) => offsets.push(message.offset()),
            Err(KafkaError::PartitionEOF(_)) => return offsets,
            Err(error) => panic!("read {topic}: {error}"),
        }
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}

async fn end(probe: &Arc<dyn SourceRecency>) -> Box<dyn RecencyTarget> {
    probe.upstream_end(deadline()).await.expect("upstream end")
}

async fn covered(target: &dyn RecencyTarget, checkpoint: &SourceCheckpoint) -> bool {
    target
        .covered_by(checkpoint, deadline())
        .await
        .expect("coverage decided")
}

#[tokio::test]
async fn a_checkpoint_covers_the_end_once_it_delivered_every_record() {
    let Some(brokers) = broker().unwrap() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = create_topic(&brokers, 3).await;
    let plain = producer(&brokers, false);
    send(&plain, &topic, 0, 5);
    send(&plain, &topic, 1, 3);
    let probe = probe(&brokers, &topic);
    let target = end(&probe).await;

    assert!(covered(target.as_ref(), &checkpoint(&topic, &[(0, 4), (1, 2)])).await);
    assert!(!covered(target.as_ref(), &checkpoint(&topic, &[(0, 3), (1, 2)])).await);
    assert!(!covered(target.as_ref(), &checkpoint(&topic, &[(0, 4)])).await);
    // Records written after the read arrived are not required.
    send(&plain, &topic, 2, 4);
    assert!(covered(target.as_ref(), &checkpoint(&topic, &[(0, 4), (1, 2)])).await);
    assert!(
        !covered(
            end(&probe).await.as_ref(),
            &checkpoint(&topic, &[(0, 4), (1, 2)])
        )
        .await
    );
    assert!(
        target
            .covered_by(&SourceCheckpoint::new(), deadline())
            .await
            .is_err(),
        "a checkpoint of another connector is refused"
    );
}

#[tokio::test]
async fn markers_and_aborted_records_after_the_last_delivered_record_are_covered() {
    let Some(brokers) = broker().unwrap() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = create_topic(&brokers, 1).await;
    let transactional = producer(&brokers, true);
    transactional.begin_transaction().unwrap();
    send(&transactional, &topic, 0, 3);
    transactional
        .commit_transaction(Duration::from_secs(10))
        .unwrap();
    let records = committed_offsets(&brokers, &topic);
    assert_eq!(records.len(), 3);
    let (last, before_last) = (records[2], records[1]);
    // The commit marker follows the last record.
    let probe = probe(&brokers, &topic);
    assert!(
        covered(
            end(&probe).await.as_ref(),
            &checkpoint(&topic, &[(0, last)])
        )
        .await
    );
    assert!(
        !covered(
            end(&probe).await.as_ref(),
            &checkpoint(&topic, &[(0, before_last)])
        )
        .await
    );

    transactional.begin_transaction().unwrap();
    send(&transactional, &topic, 0, 2);
    transactional
        .abort_transaction(Duration::from_secs(10))
        .unwrap();
    // A read_committed reader never sees the aborted records or their marker.
    assert_eq!(committed_offsets(&brokers, &topic), records);
    assert!(
        covered(
            end(&probe).await.as_ref(),
            &checkpoint(&topic, &[(0, last)])
        )
        .await
    );
}

#[tokio::test]
async fn an_open_transaction_is_not_part_of_the_end() {
    let Some(brokers) = broker().unwrap() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topic = create_topic(&brokers, 1).await;
    send(&producer(&brokers, false), &topic, 0, 2);
    let open = producer(&brokers, true);
    open.begin_transaction().unwrap();
    send(&open, &topic, 0, 3);
    let probe = probe(&brokers, &topic);
    let target = end(&probe).await;
    assert!(
        covered(target.as_ref(), &checkpoint(&topic, &[(0, 1)])).await,
        "read_committed sees through the last stable offset only: {target:?}"
    );
    open.commit_transaction(Duration::from_secs(10)).unwrap();
    assert!(!covered(end(&probe).await.as_ref(), &checkpoint(&topic, &[(0, 1)])).await);
}

#[tokio::test]
async fn a_topic_pattern_cannot_report_recency() {
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", DEFAULT_BROKERS);
    config.set("group.id", "recency-pattern");
    config.set("topic.pattern", "events-.*");
    config.set("format", "json");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let recency = KafkaSource::new(schema, Default::default(), None)
        .recency(&config)
        .expect("valid Kafka configuration");
    assert!(recency.is_none());
}
