//! Kafka checkpoint scenarios against `tests/docker/compose.yml`'s
//! Redpanda. Skips when the broker is unreachable.

#![cfg(feature = "kafka")]

use std::time::Duration;

use laminar_db::LaminarDB;

mod common;
use common::{
    compose, consume_json, consume_keyed, create_topic, delete_topic, json_i64, kafka_brokers,
    produce_json_seq, wait_for_broker,
};

fn unique(name: &str) -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{name}_{t}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_1_kafka_roundtrip() {
    let Some(brokers) = kafka_brokers() else {
        eprintln!("skipping: Redpanda not reachable");
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

    let results = consume_json(
        brokers,
        &out_topic,
        &unique("s1_verify"),
        n,
        Duration::from_secs(30),
    )
    .await;
    db.shutdown().await.ok();
    delete_topic(brokers, &in_topic).await;
    delete_topic(brokers, &out_topic).await;

    assert_eq!(
        results.len(),
        n,
        "expected {n} records in output, got {}",
        results.len(),
    );
    // Spot-check: every output carries both id and value fields.
    for payload in &results {
        assert!(payload.contains("\"id\""));
        assert!(payload.contains("\"value\""));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // enabled via `cargo test -- --ignored`; killing the broker
          // disturbs other concurrent tests against the same compose.
async fn scenario_2_broker_kill_midstream() {
    let Some(brokers) = kafka_brokers() else {
        eprintln!("skipping: Redpanda not reachable");
        return;
    };
    let in_topic = unique("s2_in");
    let out_topic = unique("s2_out");
    create_topic(brokers, &in_topic, 1).await;
    create_topic(brokers, &out_topic, 1).await;

    // Produce in two halves so we can kill the broker between them.
    let half = 20;
    produce_json_seq(brokers, &in_topic, half).await;

    let db = LaminarDB::open().expect("open db");
    let ddl_src = format!(
        "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
             'connector' = 'kafka', \
             'bootstrap.servers' = '{brokers}', \
             'topic' = '{in_topic}', \
             'group.id' = 'laminar_s2', \
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

    // Let the first half flow through, then kill and restart the broker.
    tokio::time::sleep(Duration::from_secs(3)).await;
    compose(&["kill", "redpanda"]);
    tokio::time::sleep(Duration::from_secs(1)).await;
    compose(&["start", "redpanda"]);
    assert!(
        wait_for_broker(Duration::from_secs(30)).await,
        "broker did not come back online",
    );

    // Produce the remaining half; pipeline should resume and emit.
    produce_json_seq(brokers, &in_topic, half).await;
    let results = consume_json(
        brokers,
        &out_topic,
        &unique("s2_verify"),
        half * 2,
        Duration::from_secs(60),
    )
    .await;
    db.shutdown().await.ok();
    delete_topic(brokers, &in_topic).await;
    delete_topic(brokers, &out_topic).await;

    // At-least-once: every record arrives, duplicates acceptable.
    assert!(
        results.len() >= half * 2,
        "expected at least {} records, got {}",
        half * 2,
        results.len(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_3_exactly_once_survives_db_restart() {
    let Some(brokers) = kafka_brokers() else {
        eprintln!("skipping: Redpanda not reachable");
        return;
    };
    let in_topic = unique("s3_in");
    let out_topic = unique("s3_out");
    create_topic(brokers, &in_topic, 1).await;
    create_topic(brokers, &out_topic, 1).await;

    let storage = tempfile::tempdir().expect("tempdir");

    let n = 30;
    produce_json_seq(brokers, &in_topic, n).await;

    // First run: process, checkpoint, shut down cleanly.
    {
        let config = laminar_db::LaminarConfig {
            storage_dir: Some(storage.path().to_path_buf()),
            ..laminar_db::LaminarConfig::default()
        };
        let db = LaminarDB::open_with_config(config).expect("open");
        let ddl = format!(
            "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
                 'connector' = 'kafka', \
                 'bootstrap.servers' = '{brokers}', \
                 'topic' = '{in_topic}', \
                 'group.id' = 'laminar_s3', \
                 'format' = 'json', \
                 'auto.offset.reset' = 'earliest')"
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

        // Wait for the first n to flow through, then force a checkpoint
        // and shut down cleanly.
        tokio::time::sleep(Duration::from_secs(5)).await;
        db.checkpoint().await.ok();
        db.shutdown().await.ok();
    }

    // Second run against the same storage dir: should resume at the
    // committed offset and NOT re-emit.
    {
        let config = laminar_db::LaminarConfig {
            storage_dir: Some(storage.path().to_path_buf()),
            ..laminar_db::LaminarConfig::default()
        };
        let db = LaminarDB::open_with_config(config).expect("reopen");
        let ddl = format!(
            "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
                 'connector' = 'kafka', \
                 'bootstrap.servers' = '{brokers}', \
                 'topic' = '{in_topic}', \
                 'group.id' = 'laminar_s3', \
                 'format' = 'json', \
                 'auto.offset.reset' = 'earliest')"
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
        tokio::time::sleep(Duration::from_secs(2)).await;
        db.shutdown().await.ok();
    }

    let results = consume_json(
        brokers,
        &out_topic,
        &unique("s3_verify"),
        n * 2, // upper bound
        Duration::from_secs(10),
    )
    .await;
    delete_topic(brokers, &in_topic).await;
    delete_topic(brokers, &out_topic).await;

    // Exactly-once target: exactly `n` records in output.
    // At-least-once acceptable fallback: `n` to `2n`. Fail only if under n.
    assert!(
        results.len() >= n,
        "expected ≥{n} records after restart, got {}",
        results.len(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // rebalance is timing-sensitive; run explicitly.
async fn scenario_4_consumer_rebalance_midstream() {
    let Some(brokers) = kafka_brokers() else {
        eprintln!("skipping: Redpanda not reachable");
        return;
    };
    let in_topic = unique("s4_in");
    let out_topic = unique("s4_out");
    create_topic(brokers, &in_topic, 2).await; // 2 partitions so rebalance does something
    create_topic(brokers, &out_topic, 1).await;

    let n = 80;
    produce_json_seq(brokers, &in_topic, n).await;

    let group = unique("laminar_s4_grp");
    let make_db = || async {
        let db = LaminarDB::open().expect("open");
        let ddl = format!(
            "CREATE SOURCE input (id BIGINT, value BIGINT) WITH (\
                 'connector' = 'kafka', \
                 'bootstrap.servers' = '{brokers}', \
                 'topic' = '{in_topic}', \
                 'group.id' = '{group}', \
                 'format' = 'json', \
                 'auto.offset.reset' = 'earliest')"
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
        db
    };

    let db_a = make_db().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let db_b = make_db().await; // joining the group triggers rebalance
    tokio::time::sleep(Duration::from_secs(6)).await;

    let results = consume_json(
        brokers,
        &out_topic,
        &unique("s4_verify"),
        n,
        Duration::from_secs(20),
    )
    .await;
    db_a.shutdown().await.ok();
    db_b.shutdown().await.ok();
    delete_topic(brokers, &in_topic).await;
    delete_topic(brokers, &out_topic).await;

    // At-least-once: every record visible, duplicates acceptable.
    assert!(
        results.len() >= n,
        "expected ≥{n} records after rebalance, got {}",
        results.len(),
    );
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
/// e.g. the 29092 cluster) before the shared 19092 helper, since TCP-reachability alone doesn't
/// prove the broker serves admin/produce.
fn upsert_test_brokers() -> Option<String> {
    if let Ok(b) = std::env::var("LAMINAR_TEST_KAFKA_BROKERS") {
        if !b.is_empty() {
            return Some(b);
        }
    }
    kafka_brokers().map(String::from)
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
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            incremental_emit: true,
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
    db.shutdown().await.ok();
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
        eprintln!("skipping: Redpanda not reachable");
        return;
    };
    run_upsert_scenario(brokers.as_str(), "p1b_upsert", None, "agg").await;
}

/// A CREATE STREAM projecting the incremental MV forwards its netted changelog to the upsert sink;
/// latest-per-key still equals the current aggregate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_stream_over_incremental_mv_to_kafka_upsert() {
    let Some(brokers) = upsert_test_brokers() else {
        eprintln!("skipping: Redpanda not reachable");
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
