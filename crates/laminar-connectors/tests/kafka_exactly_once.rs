//! Exactly-once Kafka sink protocol against a real broker: staged epochs are published only by
//! the fenced designated committer, atomically with their commit marker, and every recovery path
//! publishes a committed checkpoint once to `read_committed` consumers.
//!
//! It skips when the default local address is absent, but a configured `LAMINAR_KAFKA_BROKERS`
//! must be reachable. Run with:
//!   LAMINAR_KAFKA_BROKERS=127.0.0.1:19092 \
//!     cargo test -p laminar-connectors --features kafka --test kafka_exactly_once

#![cfg(feature = "kafka")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
use laminar_core::checkpoint::CheckpointAttempt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitContext, CoordinatedCommitCursor,
    CoordinatedCommitNamespace, CoordinatedCommitPayload, SinkConnector,
};
use laminar_connectors::kafka::{KafkaSink, KafkaSinkConfig};

const DEFAULT_BROKERS: &str = "127.0.0.1:19092";

fn broker() -> Result<Option<String>, String> {
    let configured = std::env::var("LAMINAR_KAFKA_BROKERS").ok();
    let brokers = configured.clone().unwrap_or_else(|| DEFAULT_BROKERS.into());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("eo-probe-{}", uuid::Uuid::new_v4()))
        .create()
        .map_err(|error| format!("invalid Kafka broker configuration '{brokers}': {error}"))?;
    match consumer.fetch_metadata(None, Duration::from_secs(10)) {
        Ok(metadata) if !metadata.brokers().is_empty() => Ok(Some(brokers)),
        Ok(_) | Err(_) if configured.is_none() => Ok(None),
        Ok(_) => Err(format!(
            "configured Kafka broker '{brokers}' returned no brokers"
        )),
        Err(error) => Err(format!(
            "configured Kafka broker '{brokers}' is unreachable: {error}"
        )),
    }
}

/// Topics for one scenario: a 3-partition output topic and a compacted commit topic.
struct Topics {
    brokers: String,
    output: String,
    commits: String,
}

impl Topics {
    async fn create(brokers: &str) -> Self {
        let suffix = uuid::Uuid::new_v4();
        let topics = Self {
            brokers: brokers.into(),
            output: format!("eo-out-{suffix}"),
            commits: format!("eo-commits-{suffix}"),
        };
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .expect("admin client");
        let output = NewTopic::new(&topics.output, 3, TopicReplication::Fixed(1));
        let commits = NewTopic::new(&topics.commits, 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact");
        for result in admin
            .create_topics([&output, &commits], &AdminOptions::new())
            .await
            .expect("create topics")
        {
            result.expect("create topic");
        }
        wait_for_leaders(brokers, &[(&topics.output, 3), (&topics.commits, 1)]);
        topics
    }

    fn config(&self, envelope: &str) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("kafka");
        for (key, value) in [
            ("bootstrap.servers", self.brokers.as_str()),
            ("topic", self.output.as_str()),
            ("format", "json"),
            ("key.column", "id"),
            ("key.format", "text"),
            ("partition.count", "3"),
            ("envelope", envelope),
            ("delivery.guarantee", "exactly_once"),
            ("commit.topic", self.commits.as_str()),
            ("transaction.timeout.ms", "10000"),
        ] {
            config.set(key, value);
        }
        config
    }

    async fn sink(&self) -> KafkaSink {
        self.sink_with("append").await
    }

    async fn sink_with(&self, envelope: &str) -> KafkaSink {
        let schema = if envelope == "upsert" {
            upsert_schema()
        } else {
            schema()
        };
        let mut sink = KafkaSink::new(schema, KafkaSinkConfig::default(), None);
        sink.open(&self.config(envelope))
            .await
            .expect("open exactly-once sink");
        sink
    }

    /// Every record in the output topic as `(id, value)`, read with `isolation`.
    fn read(&self, isolation: &str) -> Vec<(Option<String>, Option<String>)> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", format!("eo-verify-{}", uuid::Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .set("isolation.level", isolation)
            .create()
            .expect("verifier");
        let mut assignment = TopicPartitionList::new();
        for partition in 0..3 {
            assignment
                .add_partition_offset(&self.output, partition, Offset::Beginning)
                .unwrap();
        }
        consumer.assign(&assignment).unwrap();
        let mut records = Vec::new();
        let mut ended = 0;
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while ended < 3 {
            assert!(
                std::time::Instant::now() < deadline,
                "verifier never reached the end"
            );
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(message)) => records.push((
                    message
                        .key()
                        .map(|key| String::from_utf8_lossy(key).into_owned()),
                    message
                        .payload()
                        .map(|value| String::from_utf8_lossy(value).into_owned()),
                )),
                Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => ended += 1,
                Some(Err(error)) => panic!("verifier: {error}"),
                None => {}
            }
        }
        records.sort();
        records
    }
}

/// Wait until every partition of `topics` has a leader; new topics take a moment to propagate.
fn wait_for_leaders(brokers: &str, topics: &[(&str, usize)]) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("metadata client");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for (topic, partitions) in topics {
        loop {
            let ready = consumer
                .fetch_metadata(Some(topic), Duration::from_secs(5))
                .ok()
                .and_then(|metadata| {
                    metadata
                        .topics()
                        .iter()
                        .find(|candidate| candidate.name() == *topic)
                        .filter(|candidate| candidate.error().is_none())
                        .map(|candidate| {
                            candidate.partitions().len() == *partitions
                                && candidate
                                    .partitions()
                                    .iter()
                                    .all(|partition| partition.leader() >= 0)
                        })
                })
                .unwrap_or(false);
            if ready {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{topic} has no leaders"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn upsert_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("_op", DataType::Utf8, false),
    ]))
}

fn rows(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter().map(|id| format!("v{id}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// A namespace in a fresh deployment, so concurrently running tests never share a writer.
fn namespace(sink_id: &str) -> CoordinatedCommitNamespace {
    CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        uuid::Uuid::new_v4().to_string(),
        sink_id,
    )
    .unwrap()
}

/// Stage `ids` as epoch `checkpoint` and return the phase-one descriptor.
async fn stage(sink: &mut KafkaSink, checkpoint: u64, ids: &[i64]) -> Option<Vec<u8>> {
    sink.begin_epoch(checkpoint).await.unwrap();
    if !ids.is_empty() {
        sink.write_batch(&rows(ids)).await.unwrap();
    }
    sink.pre_commit(checkpoint).await.unwrap()
}

fn batch(
    namespace: &CoordinatedCommitNamespace,
    predecessor: Option<(u64, u64)>,
    fencing_token: u64,
    entries: Vec<(u64, u64, Option<Vec<u8>>)>,
) -> CoordinatedCommitBatch {
    let target = entries
        .iter()
        .map(|(checkpoint, _, _)| *checkpoint)
        .max()
        .unwrap();
    let (checkpoint_id, token) = predecessor.unwrap_or((0, 0));
    CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id,
            fencing_token: token,
        },
        fencing_token,
        target: CheckpointAttempt::canonical(target),
        entries: entries
            .into_iter()
            .map(
                |(checkpoint, participant_id, payload)| CoordinatedCommitPayload {
                    attempt: CheckpointAttempt::canonical(checkpoint),
                    participant_id,
                    payload,
                },
            )
            .collect(),
    }
}

fn context() -> CoordinatedCommitContext {
    CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(30))
}

fn ids(records: &[(Option<String>, Option<String>)]) -> Vec<String> {
    let mut ids = records
        .iter()
        .filter_map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn cursor(checkpoint_id: u64, fencing_token: u64) -> Option<CoordinatedCommitCursor> {
    Some(CoordinatedCommitCursor {
        checkpoint_id,
        fencing_token,
    })
}

macro_rules! brokers_or_skip {
    () => {
        match broker().unwrap() {
            Some(brokers) => brokers,
            None => {
                eprintln!("skipping: no Kafka broker at {DEFAULT_BROKERS}");
                return;
            }
        }
    };
}

#[tokio::test]
async fn staged_output_is_invisible_until_one_idempotent_commit() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("orders");
    let mut sink = topics.sink().await;
    let descriptor = stage(&mut sink, 1, &[1, 2, 3]).await;
    assert!(
        topics.read("read_uncommitted").is_empty(),
        "nothing is produced before commit"
    );

    let committer = sink.as_coordinated_committer().unwrap();
    let commit = batch(&namespace, None, 1, vec![(1, 1, descriptor)]);
    committer
        .commit_aggregated(commit.clone(), context())
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["1", "2", "3"]);
    assert_eq!(
        committer.committed_cursor(&namespace).await.unwrap(),
        cursor(1, 1)
    );

    // A retried commit of the same cut is a no-op.
    committer
        .commit_aggregated(commit, context())
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["1", "2", "3"]);
    sink.close().await.unwrap();
}

#[tokio::test]
async fn a_successor_publishes_a_committed_cut_the_predecessor_never_published() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("orders");
    let mut crashed = topics.sink().await;
    let descriptor = stage(&mut crashed, 1, &[10, 11]).await;
    drop(crashed);

    let successor = topics.sink().await;
    let committer = successor.as_coordinated_committer().unwrap();
    assert_eq!(committer.committed_cursor(&namespace).await.unwrap(), None);
    committer
        .commit_aggregated(
            batch(&namespace, None, 1, vec![(1, 1, descriptor)]),
            context(),
        )
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["10", "11"]);
}

#[tokio::test]
async fn a_successor_fences_an_open_transaction_and_publishes_once() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("orders");
    let mut sink = topics.sink().await;
    let descriptor = stage(&mut sink, 1, &[20, 21]).await;

    // A predecessor that crashed mid-transaction: same transactional.id, records produced, no
    // commit.
    let dangling: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "transactional.id",
            format!("laminardb-{}", namespace.external_key()),
        )
        .create()
        .unwrap();
    dangling.init_transactions(Duration::from_secs(10)).unwrap();
    dangling.begin_transaction().unwrap();
    for id in ["dangling-a", "dangling-b"] {
        dangling
            .send(
                BaseRecord::to(&topics.output)
                    .partition(0)
                    .key(id)
                    .payload("x"),
            )
            .map_err(|(error, _)| error)
            .unwrap();
    }
    dangling.flush(Duration::from_secs(10)).unwrap();

    let committer = sink.as_coordinated_committer().unwrap();
    committer
        .commit_aggregated(
            batch(&namespace, None, 1, vec![(1, 1, descriptor)]),
            context(),
        )
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["20", "21"]);
    let all = ids(&topics.read("read_uncommitted"));
    assert!(
        all.contains(&"dangling-a".to_string()),
        "the fenced records exist but are aborted"
    );
    assert!(dangling
        .commit_transaction(Duration::from_secs(10))
        .is_err());
}

#[tokio::test]
async fn a_commit_whose_response_was_lost_is_reconciled_by_the_marker() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("orders");
    let mut first = topics.sink().await;
    let descriptor = stage(&mut first, 1, &[30, 31, 32]).await;
    let commit = batch(&namespace, None, 1, vec![(1, 1, descriptor)]);
    first
        .as_coordinated_committer()
        .unwrap()
        .commit_aggregated(commit.clone(), context())
        .await
        .unwrap();
    drop(first);

    // Recovery re-drives the same committed cut without knowing whether it was published.
    let recovered = topics.sink().await;
    let committer = recovered.as_coordinated_committer().unwrap();
    assert_eq!(
        committer.committed_cursor(&namespace).await.unwrap(),
        cursor(1, 1)
    );
    committer
        .commit_aggregated(commit, context())
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["30", "31", "32"]);
}

/// Drive one commit the way the engine does: read the cursor, then commit.
async fn engine_commit(
    sink: &KafkaSink,
    namespace: &CoordinatedCommitNamespace,
    commit: CoordinatedCommitBatch,
) -> Result<(), laminar_connectors::error::ConnectorError> {
    let committer = sink.as_coordinated_committer().unwrap();
    committer.committed_cursor(namespace).await?;
    committer.commit_aggregated(commit, context()).await
}

#[tokio::test]
async fn an_obsolete_committer_cannot_publish_or_fence_a_newer_authority() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("orders");
    let mut obsolete = topics.sink().await;
    let mut successor = topics.sink().await;

    // Reading the cursor never fences: two readers coexist.
    for sink in [&obsolete, &successor] {
        let cursor = sink.as_coordinated_committer().unwrap();
        assert_eq!(cursor.committed_cursor(&namespace).await.unwrap(), None);
    }
    let first = stage(&mut obsolete, 1, &[40]).await;
    engine_commit(
        &obsolete,
        &namespace,
        batch(&namespace, None, 1, vec![(1, 1, first)]),
    )
    .await
    .unwrap();

    // A new leader (token 2) takes over and commits checkpoint 2, fencing the old writer.
    let second = stage(&mut successor, 2, &[41]).await;
    engine_commit(
        &successor,
        &namespace,
        batch(&namespace, Some((1, 1)), 2, vec![(2, 1, second)]),
    )
    .await
    .unwrap();

    // The old leader still believes it owns checkpoint 2. Its fenced writer cannot publish, and
    // once its cursor read shows the newer token it refuses to fence the successor again.
    let stale = stage(&mut obsolete, 2, &[99]).await;
    let stale_commit = batch(&namespace, Some((1, 1)), 1, vec![(2, 1, stale)]);
    let fenced = obsolete
        .as_coordinated_committer()
        .unwrap()
        .commit_aggregated(stale_commit.clone(), context())
        .await
        .unwrap_err();
    assert!(fenced.is_outcome_unknown(), "{fenced}");
    let refused = engine_commit(&obsolete, &namespace, stale_commit)
        .await
        .unwrap_err();
    assert!(refused.to_string().contains("fencing token"), "{refused}");

    // The successor was never interrupted: its live writer commits the next cut directly.
    let third = stage(&mut successor, 3, &[42]).await;
    successor
        .as_coordinated_committer()
        .unwrap()
        .commit_aggregated(
            batch(&namespace, Some((2, 2)), 2, vec![(3, 1, third)]),
            context(),
        )
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["40", "41", "42"]);
    assert_eq!(
        successor
            .as_coordinated_committer()
            .unwrap()
            .committed_cursor(&namespace)
            .await
            .unwrap(),
        cursor(3, 2)
    );
}

#[tokio::test]
async fn re_driving_an_older_cut_after_a_newer_commit_is_a_no_op() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("orders");
    let mut sink = topics.sink().await;
    let first = stage(&mut sink, 1, &[70]).await;
    let first_commit = batch(&namespace, None, 1, vec![(1, 1, first)]);
    engine_commit(&sink, &namespace, first_commit.clone())
        .await
        .unwrap();
    let second = stage(&mut sink, 2, &[71]).await;
    engine_commit(
        &sink,
        &namespace,
        batch(&namespace, Some((1, 1)), 1, vec![(2, 1, second)]),
    )
    .await
    .unwrap();

    // Recovery may re-drive checkpoint 1 after checkpoint 2 committed; the marker must not move
    // backwards and nothing may be published again.
    let recovered = topics.sink().await;
    engine_commit(&recovered, &namespace, first_commit)
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["70", "71"]);
    assert_eq!(
        recovered
            .as_coordinated_committer()
            .unwrap()
            .committed_cursor(&namespace)
            .await
            .unwrap(),
        cursor(2, 1)
    );
}

#[tokio::test]
async fn participants_and_overlapping_attempts_publish_in_order_exactly_once() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("orders");
    let mut left = topics.sink().await;
    let mut right = topics.sink().await;
    let left_one = stage(&mut left, 1, &[50]).await;
    let right_one = stage(&mut right, 1, &[51]).await;
    left.as_coordinated_committer()
        .unwrap()
        .commit_aggregated(
            batch(
                &namespace,
                None,
                1,
                vec![(1, 1, left_one.clone()), (1, 2, right_one.clone())],
            ),
            context(),
        )
        .await
        .unwrap();

    // A recovery batch spanning the already-published attempt 1 and a new attempt 2 publishes
    // only attempt 2, and an empty participant advances nothing but the cursor.
    let left_two = stage(&mut left, 2, &[52]).await;
    let committer = left.as_coordinated_committer().unwrap();
    committer
        .commit_aggregated(
            batch(
                &namespace,
                None,
                1,
                vec![
                    (1, 1, left_one),
                    (1, 2, right_one),
                    (2, 1, left_two),
                    (2, 2, None),
                ],
            ),
            context(),
        )
        .await
        .unwrap();
    assert_eq!(ids(&topics.read("read_committed")), ["50", "51", "52"]);
    assert_eq!(
        committer.committed_cursor(&namespace).await.unwrap(),
        cursor(2, 1)
    );
}

#[tokio::test]
async fn upsert_tombstones_are_published_in_the_checkpoint_transaction() {
    let brokers = brokers_or_skip!();
    let topics = Topics::create(&brokers).await;
    let namespace = namespace("table");
    let mut sink = topics.sink_with("upsert").await;
    sink.begin_epoch(1).await.unwrap();
    let changes = RecordBatch::try_new(
        upsert_schema(),
        vec![
            Arc::new(Int64Array::from(vec![60, 61])),
            Arc::new(StringArray::from(vec![Some("live"), None])),
            Arc::new(StringArray::from(vec!["U", "D"])),
        ],
    )
    .unwrap();
    sink.write_batch(&changes).await.unwrap();
    let descriptor = sink.pre_commit(1).await.unwrap();
    sink.as_coordinated_committer()
        .unwrap()
        .commit_aggregated(
            batch(&namespace, None, 1, vec![(1, 1, descriptor)]),
            context(),
        )
        .await
        .unwrap();
    let records = topics
        .read("read_committed")
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert!(records[&Some("60".to_string())].is_some());
    assert_eq!(
        records[&Some("61".to_string())],
        None,
        "a delete is a tombstone"
    );
}
