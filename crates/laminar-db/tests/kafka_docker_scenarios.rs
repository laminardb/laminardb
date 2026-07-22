//! Kafka checkpoint scenarios against `tests/docker/compose.yml`'s
//! Redpanda. Skips when the broker is unreachable unless release validation requires it.

#![cfg(feature = "kafka")]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
    time::{Duration, SystemTime},
};

use laminar_db::{DeliveryGuarantee, LaminarConfig, LaminarDB};
use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    Message, Offset, TopicPartitionList,
};

#[path = "common/kafka.rs"]
mod common;
use common::{
    consume_keyed, create_topic, delete_topic, json_i64, kafka_brokers, produce_json_seq,
    wait_for_broker, wait_for_broker_unavailable,
};

const REQUIRE_REDPANDA_ENV: &str = "LAMINAR_REQUIRE_REDPANDA";
const STOPPED_WRITER_STABILITY: Duration = Duration::from_millis(500);
static NEXT_UNIQUE_ID: AtomicU64 = AtomicU64::new(0);
static RUN_NONCE: OnceLock<String> = OnceLock::new();

fn unique(name: &str) -> String {
    let nonce = RUN_NONCE.get_or_init(|| {
        let started_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        format!("{started_at:032x}{:08x}", std::process::id())
    });
    format!(
        "{name}_{nonce}_{}",
        NEXT_UNIQUE_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn redpanda_required() -> bool {
    std::env::var(REQUIRE_REDPANDA_ENV)
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn kafka_test_brokers() -> Option<&'static str> {
    match kafka_brokers() {
        Some(brokers) => Some(brokers),
        None if redpanda_required() => {
            panic!("Redpanda is required by {REQUIRE_REDPANDA_ENV} but is not reachable")
        }
        None => {
            eprintln!("skipping: Redpanda not reachable");
            None
        }
    }
}

fn at_least_once_config(storage: &Path) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(storage.to_path_buf()),
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        }),
        delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
        ..LaminarConfig::default()
    }
}

fn compose_checked(args: &[&str]) {
    let compose_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests")
        .join("docker")
        .join("compose.yml");
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg(&compose_path)
        .args(args)
        .status()
        .unwrap_or_else(|error| panic!("failed to run docker compose {args:?}: {error}"));
    assert!(
        status.success(),
        "docker compose {args:?} failed with status {status}"
    );
}

async fn produce_json_range(brokers: &str, topic: &str, range: std::ops::Range<usize>) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer");
    for id in range {
        let payload = format!(r#"{{"id": {id}, "value": {}}}"#, id * 10);
        let key = id.to_string();
        producer
            .send(
                FutureRecord::to(topic).payload(&payload).key(&key),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PartitionWatermarkCut {
    low: i64,
    high_exclusive: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct KafkaHighWatermarkCut {
    partitions: BTreeMap<i32, PartitionWatermarkCut>,
}

fn capture_high_watermark_cut(brokers: &str, topic: &str) -> KafkaHighWatermarkCut {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("socket.timeout.ms", "5000")
        .create()
        .expect("Kafka watermark consumer");
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("fetch metadata for {topic}: {error}"));
    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
        .unwrap_or_else(|| panic!("Kafka metadata omitted topic {topic}"));
    assert_eq!(
        topic_metadata.error(),
        None,
        "Kafka metadata reported an error for {topic}"
    );
    assert!(
        !topic_metadata.partitions().is_empty(),
        "Kafka topic {topic} has no partitions"
    );

    let partitions = topic_metadata
        .partitions()
        .iter()
        .map(|partition| {
            assert_eq!(
                partition.error(),
                None,
                "Kafka metadata reported an error for {topic}/{}",
                partition.id()
            );
            let (low, high_exclusive) = consumer
                .fetch_watermarks(topic, partition.id(), Duration::from_secs(5))
                .unwrap_or_else(|error| {
                    panic!("fetch watermarks for {topic}/{}: {error}", partition.id())
                });
            assert!(
                low >= 0 && high_exclusive >= low,
                "invalid Kafka watermarks for {topic}/{}: {low}..{high_exclusive}",
                partition.id()
            );
            (
                partition.id(),
                PartitionWatermarkCut {
                    low,
                    high_exclusive,
                },
            )
        })
        .collect();
    KafkaHighWatermarkCut { partitions }
}

async fn capture_stopped_writer_cut(brokers: &str, topic: &str) -> KafkaHighWatermarkCut {
    let cut = capture_high_watermark_cut(brokers, topic);
    tokio::time::sleep(STOPPED_WRITER_STABILITY).await;
    let later = capture_high_watermark_cut(brokers, topic);
    assert_eq!(
        later, cut,
        "Kafka high watermarks for {topic} changed after its writer stopped"
    );
    cut
}

fn update_consumer_positions(
    consumer: &StreamConsumer,
    topic: &str,
    next_offsets: &mut BTreeMap<i32, i64>,
) {
    let positions = consumer
        .position()
        .unwrap_or_else(|error| panic!("read consumer positions for {topic}: {error}"));
    for position in positions.elements_for_topic(topic) {
        if let Offset::Offset(offset) = position.offset() {
            let next = next_offsets
                .get_mut(&position.partition())
                .unwrap_or_else(|| {
                    panic!(
                        "consumer position contains unexpected partition {topic}/{}",
                        position.partition()
                    )
                });
            *next = (*next).max(offset);
        }
    }
}

async fn consume_through_cut(
    brokers: &str,
    topic: &str,
    group: &str,
    cut: &KafkaHighWatermarkCut,
    deadline: Duration,
) -> Vec<String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .expect("cut consumer");
    let mut assignment = TopicPartitionList::with_capacity(cut.partitions.len());
    let mut next_offsets = BTreeMap::new();
    for (&partition, watermark) in &cut.partitions {
        assignment
            .add_partition_offset(topic, partition, Offset::Offset(watermark.low))
            .unwrap_or_else(|error| panic!("assign {topic}/{partition}: {error}"));
        next_offsets.insert(partition, watermark.low);
    }
    consumer
        .assign(&assignment)
        .unwrap_or_else(|error| panic!("assign Kafka cut for {topic}: {error}"));

    let covered = |positions: &BTreeMap<i32, i64>| {
        cut.partitions.iter().all(|(partition, watermark)| {
            positions
                .get(partition)
                .is_some_and(|offset| *offset >= watermark.high_exclusive)
        })
    };
    let deadline = tokio::time::Instant::now() + deadline;
    let mut seen_offsets = BTreeSet::new();
    let mut payloads = Vec::new();
    while !covered(&next_offsets) && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
            Ok(Ok(message)) => {
                assert_eq!(message.topic(), topic, "cut consumer read another topic");
                let partition = message.partition();
                let watermark = cut.partitions.get(&partition).unwrap_or_else(|| {
                    panic!("cut consumer read unexpected partition {topic}/{partition}")
                });
                let offset = message.offset();
                if offset >= watermark.low && offset < watermark.high_exclusive {
                    assert!(
                        seen_offsets.insert((partition, offset)),
                        "cut consumer read {topic}/{partition}@{offset} more than once"
                    );
                    payloads.push(
                        message
                            .payload_view::<str>()
                            .and_then(Result::ok)
                            .expect("Kafka output must be UTF-8 JSON")
                            .to_owned(),
                    );
                }
                update_consumer_positions(&consumer, topic, &mut next_offsets);
            }
            Ok(Err(error)) => panic!("consume Kafka cut for {topic}: {error}"),
            Err(_) => update_consumer_positions(&consumer, topic, &mut next_offsets),
        }
    }

    assert!(
        covered(&next_offsets),
        "did not consume through Kafka cut for {topic} before {deadline:?}; cut={cut:?}, positions={next_offsets:?}"
    );
    payloads
}

async fn wait_for_required_ids(
    brokers: &str,
    topic: &str,
    group: &str,
    required_ids: std::ops::Range<i64>,
    deadline: Duration,
) {
    let required: BTreeSet<_> = required_ids.collect();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");

    let deadline = tokio::time::Instant::now() + deadline;
    let mut observed = BTreeSet::new();
    while !required.is_subset(&observed) && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(250), consumer.recv()).await {
            Ok(Ok(message)) => {
                let payload = message
                    .payload_view::<str>()
                    .and_then(Result::ok)
                    .expect("Kafka output must be UTF-8 JSON")
                    .to_owned();
                observed.insert(json_i64(&payload, "id"));
            }
            Ok(Err(error)) => eprintln!("consumer error: {error}"),
            Err(_) => {}
        }
    }

    let missing: Vec<_> = required.difference(&observed).copied().collect();
    assert!(
        missing.is_empty(),
        "missing required IDs from {topic}: {missing:?}; observed {observed:?}"
    );
}

fn validated_id_counts(
    payloads: &[String],
    expected_ids: std::ops::Range<i64>,
) -> BTreeMap<i64, usize> {
    let expected: BTreeSet<_> = expected_ids.collect();
    let mut counts = BTreeMap::new();
    for payload in payloads {
        let id = json_i64(payload, "id");
        assert!(expected.contains(&id), "unexpected ID {id}: {payload}");
        assert_eq!(
            json_i64(payload, "value"),
            id * 10,
            "incorrect value for ID {id}: {payload}"
        );
        *counts.entry(id).or_insert(0) += 1;
    }
    assert_eq!(
        counts.keys().copied().collect::<BTreeSet<_>>(),
        expected,
        "captured Kafka cut has missing or unexpected IDs"
    );
    counts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_1_kafka_roundtrip() {
    let Some(brokers) = kafka_test_brokers() else {
        return;
    };
    let in_topic = unique("s1_in");
    let out_topic = unique("s1_out");
    create_topic(brokers, &in_topic, 1).await;
    create_topic(brokers, &out_topic, 1).await;

    // 50 input records.
    let n = 50;
    produce_json_seq(brokers, &in_topic, n).await;

    let db = LaminarDB::open().expect("open db");
    let ddl_src = format!(
        "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
             'connector' = 'kafka', \
             'bootstrap.servers' = '{brokers}', \
             'topic' = '{in_topic}', \
             'group.id' = 'laminar_s1', \
             'format' = 'json', \
             'auto.offset.reset' = 'earliest')"
    );
    db.execute(&ddl_src).await.expect("create source");
    db.execute("CREATE STREAM projected AS SELECT id, value FROM input")
        .await
        .expect("create stream");
    let ddl_sink = format!(
        "CREATE SINK out FROM projected WITH (\
             'connector' = 'kafka', \
             'bootstrap.servers' = '{brokers}', \
             'topic' = '{out_topic}', \
             'format' = 'json')"
    );
    db.execute(&ddl_sink).await.expect("create sink");
    db.start().await.expect("start");

    wait_for_required_ids(
        brokers,
        &out_topic,
        &unique("s1_verify"),
        0..n as i64,
        Duration::from_secs(30),
    )
    .await;
    db.shutdown().await.expect("shutdown");
    let cut = capture_stopped_writer_cut(brokers, &out_topic).await;
    let results = consume_through_cut(
        brokers,
        &out_topic,
        &unique("s1_cut"),
        &cut,
        Duration::from_secs(30),
    )
    .await;
    let counts = validated_id_counts(&results, 0..n as i64);
    assert!(
        counts.values().all(|count| *count == 1),
        "roundtrip output must contain each exact ID/value record once at the captured cut: {counts:?}"
    );
    delete_topic(brokers, &in_topic).await;
    delete_topic(brokers, &out_topic).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // Reconnect smoke test. Killing the shared broker disturbs concurrent Docker tests.
async fn scenario_2_broker_outage_between_batches_reconnect_smoke() {
    let Some(brokers) = kafka_test_brokers() else {
        return;
    };
    let in_topic = unique("s2_in");
    let out_topic = unique("s2_out");
    create_topic(brokers, &in_topic, 1).await;
    create_topic(brokers, &out_topic, 1).await;

    // Produce in two halves so we can kill the broker between them.
    let half = 20;
    produce_json_range(brokers, &in_topic, 0..half).await;

    let storage = tempfile::tempdir().expect("tempdir");
    let db = LaminarDB::open_with_config(at_least_once_config(storage.path())).expect("open db");
    let ddl_src = format!(
        "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
             'connector' = 'kafka', \
             'bootstrap.servers' = '{brokers}', \
             'topic' = '{in_topic}', \
             'group.id' = 'laminar_s2', \
             'format' = 'json', \
             'startup.mode' = 'earliest')"
    );
    db.execute(&ddl_src).await.expect("create source");
    db.execute("CREATE STREAM projected AS SELECT id, value FROM input")
        .await
        .expect("create stream");
    let ddl_sink = format!(
        "CREATE SINK out FROM projected WITH (\
             'connector' = 'kafka', \
             'bootstrap.servers' = '{brokers}', \
             'topic' = '{out_topic}', \
             'format' = 'json')"
    );
    db.execute(&ddl_sink).await.expect("create sink");
    db.start().await.expect("start");

    // Prove the first input batch is visible before interrupting the broker.
    wait_for_required_ids(
        brokers,
        &out_topic,
        &unique("s2_ready"),
        0..half as i64,
        Duration::from_secs(20),
    )
    .await;
    compose_checked(&["kill", "redpanda"]);
    let broker_became_unavailable = wait_for_broker_unavailable(Duration::from_secs(10)).await;
    compose_checked(&["start", "redpanda"]);
    assert!(
        broker_became_unavailable,
        "broker still served Kafka metadata after docker compose kill",
    );
    assert!(
        wait_for_broker(Duration::from_secs(30)).await,
        "broker did not come back online",
    );

    // This is a reconnect smoke test: the later batch must become visible after recovery.
    let total = half * 2;
    produce_json_range(brokers, &in_topic, half..total).await;
    wait_for_required_ids(
        brokers,
        &out_topic,
        &unique("s2_verify"),
        0..total as i64,
        Duration::from_secs(60),
    )
    .await;
    db.shutdown().await.expect("shutdown after broker recovery");
    let cut = capture_stopped_writer_cut(brokers, &out_topic).await;
    let results = consume_through_cut(
        brokers,
        &out_topic,
        &unique("s2_cut"),
        &cut,
        Duration::from_secs(30),
    )
    .await;

    // At-least-once permits duplicates; the captured broker cut must contain both batches.
    validated_id_counts(&results, 0..total as i64);
    delete_topic(brokers, &in_topic).await;
    delete_topic(brokers, &out_topic).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_3_at_least_once_has_no_loss_after_db_restart() {
    let Some(brokers) = kafka_test_brokers() else {
        return;
    };
    let in_topic = unique("s3_in");
    let out_topic = unique("s3_out");
    create_topic(brokers, &in_topic, 1).await;
    create_topic(brokers, &out_topic, 1).await;

    let storage = tempfile::tempdir().expect("tempdir");

    let n = 30;
    let first_batch = n / 2;
    produce_json_seq(brokers, &in_topic, first_batch).await;

    // Stop every writer before taking the baseline used to detect replay on restart.
    let (first_cut, first_counts) = {
        let config = at_least_once_config(storage.path());
        let db = LaminarDB::open_with_config(config).expect("open");
        let ddl = format!(
            "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
                 'connector' = 'kafka', \
                 'bootstrap.servers' = '{brokers}', \
                 'topic' = '{in_topic}', \
                 'group.id' = 'laminar_s3', \
                 'format' = 'json', \
                 'startup.mode' = 'earliest')"
        );
        db.execute(&ddl).await.expect("src");
        db.execute("CREATE STREAM out_stream AS SELECT id, value FROM input")
            .await
            .expect("stream");
        let ddl_sink = format!(
            "CREATE SINK sink_a FROM out_stream WITH (\
                 'connector' = 'kafka', \
                 'bootstrap.servers' = '{brokers}', \
                 'topic' = '{out_topic}', \
                 'format' = 'json')"
        );
        db.execute(&ddl_sink).await.expect("sink");
        db.start().await.expect("start");

        wait_for_required_ids(
            brokers,
            &out_topic,
            &unique("s3_ready"),
            0..first_batch as i64,
            Duration::from_secs(20),
        )
        .await;
        let checkpoint = db.checkpoint().await.expect("checkpoint");
        assert!(
            checkpoint.success && checkpoint.error.is_none(),
            "checkpoint did not commit cleanly: {checkpoint:?}"
        );
        db.shutdown().await.expect("shutdown");

        let first_cut = capture_stopped_writer_cut(brokers, &out_topic).await;
        let first_results = consume_through_cut(
            brokers,
            &out_topic,
            &unique("s3_baseline"),
            &first_cut,
            Duration::from_secs(20),
        )
        .await;
        (
            first_cut,
            validated_id_counts(&first_results, 0..first_batch as i64),
        )
    };

    produce_json_range(brokers, &in_topic, first_batch..n).await;

    {
        let config = at_least_once_config(storage.path());
        let db = LaminarDB::open_with_config(config).expect("reopen");
        let ddl = format!(
            "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
                 'connector' = 'kafka', \
                 'bootstrap.servers' = '{brokers}', \
                 'topic' = '{in_topic}', \
                 'group.id' = 'laminar_s3', \
                 'format' = 'json', \
                 'startup.mode' = 'earliest')"
        );
        db.execute(&ddl).await.expect("src");
        db.execute("CREATE STREAM out_stream AS SELECT id, value FROM input")
            .await
            .expect("stream");
        let ddl_sink = format!(
            "CREATE SINK sink_a FROM out_stream WITH (\
                 'connector' = 'kafka', \
                 'bootstrap.servers' = '{brokers}', \
                 'topic' = '{out_topic}', \
                 'format' = 'json')"
        );
        db.execute(&ddl_sink).await.expect("sink");
        db.start().await.expect("restart");
        wait_for_required_ids(
            brokers,
            &out_topic,
            &unique("s3_verify"),
            first_batch as i64..n as i64,
            Duration::from_secs(20),
        )
        .await;
        db.shutdown().await.expect("shutdown after restart");
    }

    let final_cut = capture_stopped_writer_cut(brokers, &out_topic).await;
    assert_eq!(
        final_cut.partitions.keys().collect::<Vec<_>>(),
        first_cut.partitions.keys().collect::<Vec<_>>(),
        "Kafka output partition set changed across restart"
    );
    for (partition, first) in &first_cut.partitions {
        let final_watermark = &final_cut.partitions[partition];
        assert_eq!(
            final_watermark.low, first.low,
            "Kafka output low watermark changed for partition {partition}"
        );
        assert!(
            final_watermark.high_exclusive >= first.high_exclusive,
            "Kafka output high watermark regressed for partition {partition}: {} -> {}",
            first.high_exclusive,
            final_watermark.high_exclusive
        );
    }
    let final_results = consume_through_cut(
        brokers,
        &out_topic,
        &unique("s3_final"),
        &final_cut,
        Duration::from_secs(20),
    )
    .await;
    let final_counts = validated_id_counts(&final_results, 0..n as i64);
    for id in 0..first_batch as i64 {
        assert_eq!(
            final_counts.get(&id),
            first_counts.get(&id),
            "pre-checkpoint ID {id} was emitted again after a clean restore"
        );
    }

    delete_topic(brokers, &in_topic).await;
    delete_topic(brokers, &out_topic).await;
}

fn kv_batch(ks: &[i64], vs: &[i64]) -> arrow::array::RecordBatch {
    use std::sync::Arc;
    arrow::array::RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(arrow::array::Int64Array::from(ks.to_vec())) as _,
        ),
        (
            "v",
            Arc::new(arrow::array::Int64Array::from(vs.to_vec())) as _,
        ),
    ])
    .unwrap()
}

/// Broker for this test: honor `LAMINAR_TEST_KAFKA_BROKERS` (a functional broker on this host,
/// e.g. the 29092 cluster) before the shared 19092 metadata probe.
fn upsert_test_brokers() -> Option<String> {
    if let Ok(b) = std::env::var("LAMINAR_TEST_KAFKA_BROKERS") {
        if !b.is_empty() {
            return Some(b);
        }
    }
    kafka_test_brokers().map(String::from)
}

/// Shared ENVELOPE UPSERT scenario: build an incremental agg MV, optionally project it through
/// `extra_stream_ddl`, sink `sink_from` to a topic, push k1=10/k2=20 then a k1 update, and assert
/// the latest-per-key totals collapse to 15/20 (not a lossy positive-only stream).
async fn run_upsert_scenario(
    brokers: &str,
    tag: &str,
    extra_stream_ddl: Option<&str>,
    sink_from: &str,
) {
    let out_topic = unique(&format!("{tag}_out"));
    create_topic(brokers, &out_topic, 1).await;

    let dir = tempfile::tempdir().unwrap();
    let cfg = laminar_db::LaminarConfig {
        storage_dir: Some(dir.path().to_path_buf()),
        incremental_emit: true,
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        }),
        ..Default::default()
    };
    let db = LaminarDB::open_with_config(cfg).expect("open db");
    db.execute("CREATE SOURCE events (k BIGINT, v BIGINT)")
        .await
        .expect("source");
    db.execute("CREATE MATERIALIZED VIEW agg AS SELECT k, SUM(v) AS total FROM events GROUP BY k")
        .await
        .expect("mv");
    if let Some(ddl) = extra_stream_ddl {
        db.execute(ddl).await.expect("extra stream ddl");
    }
    let ddl_sink = format!(
        "CREATE SINK out FROM {sink_from} WITH (\
             'connector' = 'kafka', \
             'bootstrap.servers' = '{brokers}', \
             'topic' = '{out_topic}', \
             'format' = 'json', \
             'key.column' = 'k', \
             'envelope' = 'upsert')"
    );
    db.execute(&ddl_sink).await.expect("upsert sink");
    db.start().await.expect("start");

    let src = db.source_untyped("events").expect("source handle");
    src.push_arrow(kv_batch(&[1, 2], &[10, 20])).unwrap(); // k1=10, k2=20
    tokio::time::sleep(Duration::from_millis(500)).await;
    src.push_arrow(kv_batch(&[1], &[5])).unwrap(); // k1: 10 -> 15 (retract+insert)
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Read every message and fold to the latest value per key (offset order = arrival order).
    let msgs = consume_keyed(
        brokers,
        &out_topic,
        &unique(&format!("{tag}_verify")),
        100,
        Duration::from_secs(10),
    )
    .await;
    db.shutdown().await.expect("shutdown");
    delete_topic(brokers, &out_topic).await;

    let mut latest: std::collections::BTreeMap<String, Option<String>> =
        std::collections::BTreeMap::new();
    for (k, v) in msgs {
        if let Some(k) = k {
            latest.insert(k, v);
        }
    }
    let v1 = latest
        .get("1")
        .expect("key 1 present")
        .clone()
        .expect("k1 not tombstoned");
    let v2 = latest
        .get("2")
        .expect("key 2 present")
        .clone()
        .expect("k2 not tombstoned");
    // Exact value: the k1 update collapses to 15, not 25 (positive-only) or a 150 substring match.
    assert_eq!(json_i64(&v1, "total"), 15, "k1 latest total, got {v1}");
    assert_eq!(json_i64(&v2, "total"), 20, "k2 latest total, got {v2}");
}

/// An incremental aggregate MV feeding a Kafka `ENVELOPE UPSERT` sink: latest-per-key equals the
/// current aggregate, and an update (retract+insert) collapses to a single latest value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_incremental_agg_kafka_upsert() {
    let Some(brokers) = upsert_test_brokers() else {
        return;
    };
    run_upsert_scenario(brokers.as_str(), "p1b_upsert", None, "agg").await;
}

/// A CREATE STREAM projecting the incremental MV forwards its netted changelog to the upsert sink;
/// latest-per-key still equals the current aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_stream_over_incremental_mv_to_kafka_upsert() {
    let Some(brokers) = upsert_test_brokers() else {
        return;
    };
    run_upsert_scenario(
        brokers.as_str(),
        "p4_stream_upsert",
        Some("CREATE STREAM s AS SELECT k, total FROM agg"),
        "s",
    )
    .await;
}
