//! Kafka exactly-once: every node stages its rows in the checkpoint, and the leader publishes the
//! rows of every node for one checkpoint, with a marker in a compacted commit topic, in one
//! transaction. The oracle reads the output topic from the beginning with `read_committed`, so any
//! row a transaction ever committed twice is counted twice.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use super::{setting, Destination, Observed, Oracle};
use crate::kill_rejoin::EVENTS_INGESTED;
use crate::{kafka_create_compacted_topic, kafka_create_topic, JoinDelivery};

/// Pinned in the sink, so every node routes a row over the same partitions.
const OUTPUT_PARTITIONS: i32 = 4;

pub(super) struct KafkaExactlyOnce {
    brokers: String,
}

impl KafkaExactlyOnce {
    pub(super) fn from_environment() -> Self {
        Self {
            brokers: setting("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS", None),
        }
    }
}

fn commit_topic(topic: &str) -> String {
    format!("{topic}-commits")
}

impl Destination for KafkaExactlyOnce {
    fn connector(&self) -> &'static str {
        "kafka"
    }

    fn delivery(&self) -> JoinDelivery {
        JoinDelivery::ExactlyOnce
    }

    /// Exactly-once rows are staged on each node and produced by the leader, so no sink counter
    /// grows per node; each node reads its share of the source and stages what it reads.
    fn written_metric(&self) -> &'static str {
        EVENTS_INGESTED
    }

    fn format(&self) -> Option<&'static str> {
        Some("json")
    }

    fn name(&self, run_id: &str) -> String {
        format!("soak-sink-out-{run_id}")
    }

    fn sink_properties(&self, topic: &str) -> String {
        format!(
            r#""bootstrap.servers" = "{brokers}"
topic = "{topic}"
"commit.topic" = "{commits}"
"partition.count" = "{OUTPUT_PARTITIONS}"
"#,
            brokers = self.brokers,
            commits = commit_topic(topic),
        )
    }

    fn create(&self, topic: &str) {
        kafka_create_topic(&self.brokers, topic, OUTPUT_PARTITIONS);
        kafka_create_compacted_topic(&self.brokers, &commit_topic(topic), 1);
    }

    /// Topics are named per run and stay on the broker, which other runs share.
    fn delete(&self, _topic: &str) {}

    fn spawn_oracle(&self, topic: &str) -> Oracle {
        let (brokers, topic) = (self.brokers.clone(), topic.to_owned());
        Oracle::spawn(move |observed, stop| consume(&brokers, &topic, &observed, &stop))
    }
}

fn consume(brokers: &str, topic: &str, observed: &Observed, stop: &AtomicBool) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("soak-oracle-{topic}"))
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .expect("output oracle consumer");
    let mut assignment = TopicPartitionList::new();
    for partition in 0..OUTPUT_PARTITIONS {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .expect("assign an output partition");
    }
    consumer
        .assign(&assignment)
        .expect("assign the output partitions");
    while !stop.load(Ordering::Acquire) {
        match consumer.poll(Duration::from_millis(250)) {
            Some(Ok(message)) => {
                observed.add(message.payload().expect("an output row has a value"));
            }
            Some(Err(error)) => eprintln!("soak: Kafka output oracle poll: {error}"),
            None => {}
        }
    }
}
