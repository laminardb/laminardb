//! Kafka-to-Kafka exactly-once through the engine: a cross-partition aggregate and a projection
//! publish each committed checkpoint once to `read_committed` consumers, output of an uncommitted
//! epoch never reaches the broker, and a restart replays it from the last checkpoint once.
//!
//! It skips when the default local address is absent, but a configured `LAMINAR_KAFKA_BROKERS`
//! must be reachable. Run with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19092 \
//!     cargo test -p laminar-db --features kafka --test kafka_exactly_once_e2e

#![cfg(feature = "kafka")]

use std::collections::BTreeMap;
use std::time::Duration;

use laminar_db::{DeliveryGuarantee, LaminarConfig, LaminarDB};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";
const PARTITIONS: i32 = 3;
const GROUPS: i64 = 4;

fn broker() -> Option<String> {
    let configured = std::env::var("LAMINAR_KAFKA_BROKERS").ok();
    let brokers = configured.clone().unwrap_or_else(|| DEFAULT_BROKERS.into());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("probe consumer");
    match consumer.fetch_metadata(None, Duration::from_secs(10)) {
        Ok(metadata) if !metadata.brokers().is_empty() => Some(brokers),
        _ if configured.is_some() => panic!("configured Kafka broker '{brokers}' is unreachable"),
        _ => None,
    }
}

struct Topics {
    brokers: String,
    input: String,
    copied: String,
    totals: String,
    commits: String,
}

impl Topics {
    async fn create(brokers: &str) -> Self {
        let suffix = uuid::Uuid::new_v4();
        let topics = Self {
            brokers: brokers.into(),
            input: format!("eo-e2e-in-{suffix}"),
            copied: format!("eo-e2e-copied-{suffix}"),
            totals: format!("eo-e2e-totals-{suffix}"),
            commits: format!("eo-e2e-commits-{suffix}"),
        };
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("admin");
        let partitioned = [&topics.input, &topics.copied, &topics.totals]
            .map(|name| NewTopic::new(name, PARTITIONS, TopicReplication::Fixed(1)));
        let commits = NewTopic::new(&topics.commits, 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact");
        let all = partitioned.iter().chain(std::iter::once(&commits));
        for result in admin
            .create_topics(all, &AdminOptions::new())
            .await
            .expect("create")
        {
            result.expect("create topic");
        }
        topics
    }

    /// Produce rows `ids`, keyed by id so each aggregation group spans every input partition.
    fn produce(&self, ids: std::ops::Range<i64>) {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("enable.idempotence", "true")
            .create()
            .expect("producer");
        for id in ids {
            let key = id.to_string();
            let value = format!(r#"{{"id":{id},"k":{},"v":{id}}}"#, id % GROUPS);
            producer
                .send(BaseRecord::to(&self.input).key(&key).payload(&value))
                .map_err(|(error, _)| error)
                .expect("enqueue");
        }
        producer.flush(Duration::from_secs(10)).expect("flush");
    }

    /// Every record of `topic` read with `isolation`, as `(key, value)` in partition order.
    fn read(&self, topic: &str, isolation: &str) -> Vec<(String, Option<String>)> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set(
                "group.id",
                format!("eo-e2e-verify-{}", uuid::Uuid::new_v4()),
            )
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("isolation.level", isolation)
            .create()
            .expect("verifier");
        let mut assignment = TopicPartitionList::new();
        for partition in 0..PARTITIONS {
            assignment
                .add_partition_offset(topic, partition, Offset::Beginning)
                .unwrap();
        }
        consumer.assign(&assignment).unwrap();
        let mut records = Vec::new();
        let mut ended = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while ended < PARTITIONS {
            assert!(std::time::Instant::now() < deadline, "verifier timed out");
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(message)) => records.push((
                    String::from_utf8_lossy(message.key().unwrap_or_default()).into_owned(),
                    message
                        .payload()
                        .map(|value| String::from_utf8_lossy(value).into_owned()),
                )),
                Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => ended += 1,
                Some(Err(error)) => panic!("verifier: {error}"),
                None => {}
            }
        }
        records
    }

    async fn open(&self, storage: &std::path::Path) -> std::sync::Arc<LaminarDB> {
        let db = LaminarDB::open_with_config(LaminarConfig {
            storage_dir: Some(storage.to_path_buf()),
            checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            }),
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
            ..LaminarConfig::default()
        })
        .expect("open");
        let brokers = &self.brokers;
        for statement in [
            format!(
                "CREATE SOURCE input (id BIGINT NOT NULL, k BIGINT NOT NULL, v BIGINT NOT NULL) \
                 FROM KAFKA ('bootstrap.servers' = '{brokers}', 'topic' = '{}', \
                 'group.id' = 'eo-e2e', 'startup.mode' = 'earliest') FORMAT JSON",
                self.input
            ),
            "CREATE STREAM copied AS SELECT id, k, v FROM input".to_string(),
            "CREATE MATERIALIZED VIEW totals AS \
             SELECT k, SUM(v) AS total, COUNT(*) AS n FROM input GROUP BY k"
                .to_string(),
            format!(
                "CREATE SINK copied_out FROM copied INTO KAFKA (\
                 'bootstrap.servers' = '{brokers}', 'topic' = '{}', 'key.column' = 'id', \
                 'key.format' = 'text', 'commit.topic' = '{}') FORMAT JSON",
                self.copied, self.commits
            ),
            format!(
                "CREATE SINK totals_out FROM totals INTO KAFKA (\
                 'bootstrap.servers' = '{brokers}', 'topic' = '{}', 'key.column' = 'k', \
                 'key.format' = 'text', 'envelope' = 'upsert', 'partition.count' = '{PARTITIONS}', \
                 'commit.topic' = '{}') FORMAT JSON",
                self.totals, self.commits
            ),
        ] {
            db.execute(&statement).await.unwrap_or_else(|error| {
                panic!("{statement}: {error}");
            });
        }
        db.start().await.expect("start");
        db
    }
}

async fn wait_for_rows(db: &LaminarDB, stream: &str, rows: u64) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while db
            .stream_metrics(stream)
            .is_none_or(|metrics| metrics.total_events < rows)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{stream} never emitted {rows} rows"));
}

async fn checkpoint(db: &LaminarDB) {
    let result = db.checkpoint().await.expect("checkpoint");
    assert!(result.success, "checkpoint failed: {:?}", result.error);
}

/// Each copied id exactly once.
fn assert_copied_once(records: &[(String, Option<String>)], ids: std::ops::Range<i64>) {
    let mut counts = BTreeMap::<i64, usize>::new();
    for (key, _) in records {
        *counts.entry(key.parse().expect("numeric key")).or_default() += 1;
    }
    let expected = ids.map(|id| (id, 1)).collect::<BTreeMap<_, _>>();
    assert_eq!(counts, expected);
}

/// The newest committed total per group equals the exact sum over `ids`.
fn assert_totals(records: &[(String, Option<String>)], ids: std::ops::Range<i64>) {
    let mut latest = BTreeMap::<i64, (i64, i64)>::new();
    for (key, value) in records {
        let value: serde_json::Value =
            serde_json::from_str(value.as_deref().expect("totals are never deleted")).unwrap();
        latest.insert(
            key.parse().unwrap(),
            (
                value["total"].as_i64().unwrap(),
                value["n"].as_i64().unwrap(),
            ),
        );
    }
    let mut expected = BTreeMap::<i64, (i64, i64)>::new();
    for id in ids {
        let entry = expected.entry(id % GROUPS).or_default();
        entry.0 += id;
        entry.1 += 1;
    }
    assert_eq!(latest, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_partition_aggregate_is_exactly_once_across_a_crash_between_checkpoints() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let Some(brokers) = broker() else {
        eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
        return;
    };
    let topics = Topics::create(&brokers).await;
    let storage = tempfile::tempdir().expect("tempdir");

    topics.produce(0..60);
    let db = topics.open(storage.path()).await;
    wait_for_rows(&db, "copied", 60).await;
    checkpoint(&db).await;
    assert_copied_once(&topics.read(&topics.copied, "read_committed"), 0..60);
    assert_totals(&topics.read(&topics.totals, "read_committed"), 0..60);

    // Rows processed after the last checkpoint stay in the uncommitted epoch: nothing reaches the
    // broker, committed or not, before that epoch commits.
    topics.produce(60..120);
    wait_for_rows(&db, "copied", 120).await;
    assert_eq!(topics.read(&topics.copied, "read_uncommitted").len(), 60);
    db.shutdown().await.expect("shutdown without a checkpoint");

    let restarted = topics.open(storage.path()).await;
    wait_for_rows(&restarted, "copied", 60).await;
    checkpoint(&restarted).await;
    let committed = topics.read(&topics.copied, "read_committed");
    assert_copied_once(&committed, 0..120);
    assert_eq!(
        topics.read(&topics.copied, "read_uncommitted").len(),
        committed.len()
    );
    assert_totals(&topics.read(&topics.totals, "read_committed"), 0..120);
    restarted.shutdown().await.expect("shutdown");
}
