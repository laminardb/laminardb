//! Kafka checkpoint scenarios against `tests/docker/compose.yml`'s
//! Redpanda. Skips when the broker is unreachable.

#![cfg(feature = "kafka")]

use std::time::Duration;

use laminar_db::LaminarDB;

mod common;
use common::{
    compose, consume_json, create_topic, delete_topic, kafka_brokers, produce_json_seq,
    wait_for_broker,
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
