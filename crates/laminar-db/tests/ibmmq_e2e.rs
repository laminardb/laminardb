//! IBM MQ through the public SQL path: `CREATE SOURCE ... FROM IBMMQ`, a stream, and
//! `CREATE SINK ... INTO IBMMQ`, with checkpointed at-least-once delivery.
//!
//! Needs the `tests/docker/compose.yml` service `ibmmq` and an installed IBM MQ client named by
//! `LAMINAR_TEST_IBMMQ_LIBRARY`; skips without them unless `LAMINAR_REQUIRE_IBMMQ=1`.

#![cfg(feature = "ibmmq")]

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::{BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::ibmmq::{IbmMqSink, IbmMqSource};
use laminar_db::LaminarDB;

const PASSWORD: &str = "laminar-test-app";
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Mq {
    library: String,
    container: String,
}

fn fixture() -> Option<Mq> {
    match (
        std::env::var("LAMINAR_TEST_IBMMQ_LIBRARY"),
        std::env::var("LAMINAR_TEST_IBMMQ_CONTAINER"),
    ) {
        (Ok(library), Ok(container)) => Some(Mq { library, container }),
        _ if std::env::var("LAMINAR_REQUIRE_IBMMQ").is_ok_and(|v| v == "1") => {
            panic!("IBM MQ is required: set LAMINAR_TEST_IBMMQ_LIBRARY and LAMINAR_TEST_IBMMQ_CONTAINER")
        }
        _ => {
            eprintln!("skipping: IBM MQ client library or container not configured");
            None
        }
    }
}

impl Mq {
    fn mqsc(&self, commands: &str) -> String {
        let mut child = Command::new("docker")
            .args(["exec", "-i", &self.container, "runmqsc", "QM1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("runmqsc");
        {
            use std::io::Write;
            child
                .stdin
                .take()
                .unwrap()
                .write_all(commands.as_bytes())
                .unwrap();
        }
        String::from_utf8_lossy(&child.wait_with_output().unwrap().stdout).into_owned()
    }

    fn queue(&self, name: &str) -> String {
        let queue = format!(
            "DEV.LAMINAR.E2E.{name}.{}.{}",
            std::process::id() % 100_000,
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        assert!(self
            .mqsc(&format!("DEFINE QLOCAL('{queue}') REPLACE\n"))
            .contains("AMQ8006I"));
        queue
    }

    fn delete(&self, queues: &[&str]) {
        for queue in queues {
            self.mqsc(&format!("DELETE QLOCAL('{queue}') PURGE\n"));
        }
    }

    fn depth(&self, queue: &str) -> u32 {
        let output = self.mqsc(&format!("DISPLAY QLOCAL('{queue}') CURDEPTH\n"));
        output
            .split("CURDEPTH(")
            .nth(1)
            .and_then(|rest| rest.split(')').next())
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    /// Connector options shared by every DDL statement of these tests.
    fn options(&self) -> String {
        format!(
            "'client.library' = '{}', 'queue.manager' = 'QM1', 'channel' = 'DEV.APP.SVRCONN', \
             'connection.name' = '127.0.0.1(1414)', 'username' = 'app', \
             'password' = '${{E2E_IBMMQ_PASSWORD}}'",
            self.library
        )
    }

    fn connector(&self, queue: &str, format: &str) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("ibmmq");
        for (key, value) in [
            ("client.library", self.library.as_str()),
            ("queue.manager", "QM1"),
            ("channel", "DEV.APP.SVRCONN"),
            ("connection.name", "127.0.0.1(1414)"),
            ("username", "app"),
            ("password", PASSWORD),
            ("queue", queue),
            ("format", format),
        ] {
            config.set(key, value);
        }
        config
    }

    async fn put(&self, queue: &str, schema: SchemaRef, batch: RecordBatch, format: &str) {
        let mut sink = IbmMqSink::new(schema, None);
        sink.open(&self.connector(queue, format)).await.unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.close().await.unwrap();
    }

    /// Drain `queue` with an independent consumer until `predicate` holds, committing as it goes.
    async fn drain_until(
        &self,
        queue: &str,
        payload: SchemaRef,
        format: &str,
        predicate: impl Fn(&[RecordBatch]) -> bool,
    ) -> Vec<RecordBatch> {
        let mut config = self.connector(queue, format);
        config.set("wait.interval.ms", "50");
        let mut source = IbmMqSource::new(payload, None);
        source
            .start(
                SourceStart::new(
                    config,
                    SourcePosition::Initial,
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let mut batches = Vec::new();
        while !predicate(&batches) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out draining {queue}"
            );
            if let Some(batch) = source.poll_batch(1_000).await.unwrap() {
                batches.push(batch.records);
                let cut = source.checkpoint();
                source.notify_epoch_committed(1, &cut).await.unwrap();
            } else {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        source.close().await.unwrap();
        batches
    }
}

async fn open(storage: &Path) -> Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .config_var("E2E_IBMMQ_PASSWORD", PASSWORD)
        .build()
        .await
        .expect("open database")
}

fn events(ids: std::ops::Range<i64>) -> RecordBatch {
    let values: Vec<String> = ids
        .map(|id| format!(r#"{{"id":{id},"label":"e{id}"}}"#))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(values))],
    )
    .unwrap()
}

fn json_ids(batches: &[RecordBatch]) -> BTreeMap<i64, usize> {
    let mut counts = BTreeMap::new();
    for batch in batches {
        for text in batch.column(0).as_string::<i32>().iter().flatten() {
            let value: serde_json::Value = serde_json::from_str(text).unwrap();
            assert_eq!(value["doubled"], value["id"].as_i64().unwrap() * 2);
            *counts.entry(value["id"].as_i64().unwrap()).or_insert(0) += 1;
        }
    }
    counts
}

async fn json_pipeline(db: &LaminarDB, mq: &Mq, input: &str, output: &str) {
    let options = mq.options();
    db.execute(&format!(
        "CREATE SOURCE events (id BIGINT, label VARCHAR) FROM IBMMQ ({options}, \
         'queue' = '{input}', 'wait.interval.ms' = '50') FORMAT JSON"
    ))
    .await
    .expect("create source");
    db.execute("CREATE STREAM doubled AS SELECT id, id * 2 AS doubled, label FROM events")
        .await
        .expect("create stream");
    db.execute(&format!(
        "CREATE SINK out FROM doubled INTO IBMMQ ({options}, 'queue' = '{output}') FORMAT JSON"
    ))
    .await
    .expect("create sink");
}

/// The output is read back as raw UTF-8 text so the oracle parses every JSON message itself.
async fn read_json(
    mq: &Mq,
    queue: &str,
    predicate: impl Fn(&BTreeMap<i64, usize>) -> bool,
) -> BTreeMap<i64, usize> {
    let raw = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batches = mq
        .drain_until(queue, raw, "raw", |batches| predicate(&json_ids(batches)))
        .await;
    json_ids(&batches)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn json_roundtrip_commits_input_after_checkpoint() {
    let Some(mq) = fixture() else { return };
    let (input, output) = (mq.queue("JSON.IN"), mq.queue("JSON.OUT"));
    let batch = events(0..200);
    mq.put(&input, batch.schema(), batch, "raw").await;
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    json_pipeline(&db, &mq, &input, &output).await;
    db.start().await.expect("start");
    let ids = read_json(&mq, &output, |ids| ids.len() >= 200).await;
    assert_eq!(ids, (0..200).map(|id| (id, 1)).collect());
    assert!(db.checkpoint().await.expect("checkpoint").success);
    db.shutdown().await.expect("shutdown");
    assert_eq!(mq.depth(&input), 0, "committed input is gone");
    mq.delete(&[&input, &output]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncheckpointed_input_is_backed_out_and_replayed_without_loss() {
    let Some(mq) = fixture() else { return };
    let (input, output) = (mq.queue("RESTART.IN"), mq.queue("RESTART.OUT"));
    let storage = tempfile::tempdir().unwrap();
    let batch = events(0..100);
    mq.put(&input, batch.schema(), batch, "raw").await;
    {
        let db = open(storage.path()).await;
        json_pipeline(&db, &mq, &input, &output).await;
        db.start().await.expect("start");
        read_json(&mq, &output, |ids| ids.len() >= 100).await;
        assert!(db.checkpoint().await.expect("checkpoint").success);
        let batch = events(100..150);
        mq.put(&input, batch.schema(), batch, "raw").await;
        read_json(&mq, &output, |ids| ids.len() >= 50).await;
        db.shutdown().await.expect("shutdown");
    }
    assert_eq!(
        mq.depth(&input),
        50,
        "uncommitted gets must be backed out, not committed by MQDISC"
    );
    let batch = events(150..200);
    mq.put(&input, batch.schema(), batch, "raw").await;
    {
        let db = open(storage.path()).await;
        json_pipeline(&db, &mq, &input, &output).await;
        db.start().await.expect("restart");
        let ids = read_json(&mq, &output, |ids| {
            (100..200).all(|id| ids.contains_key(&id))
        })
        .await;
        assert!(ids.keys().all(|id| (100..200).contains(id)), "{ids:?}");
        assert!(db.checkpoint().await.expect("checkpoint").success);
        db.shutdown().await.expect("shutdown");
    }
    assert_eq!(mq.depth(&input), 0);
    mq.delete(&[&input, &output]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_binary_payloads_round_trip_byte_for_byte() {
    let Some(mq) = fixture() else { return };
    let (input, output) = (mq.queue("BIN.IN"), mq.queue("BIN.OUT"));
    let payloads: Vec<Vec<u8>> = (0..50_u8)
        .map(|i| vec![0xff, 0x00, i, 0xc3, 0x28, 0x80])
        .collect();
    let binary = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(
        binary.clone(),
        vec![Arc::new(BinaryArray::from(
            payloads.iter().map(Vec::as_slice).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();
    mq.put(&input, binary.clone(), batch, "raw").await;
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    let options = mq.options();
    db.execute(&format!(
        "CREATE SOURCE blobs (value BYTEA) FROM IBMMQ ({options}, 'queue' = '{input}') FORMAT RAW"
    ))
    .await
    .expect("create source");
    db.execute("CREATE STREAM passthrough AS SELECT value FROM blobs")
        .await
        .unwrap();
    db.execute(&format!(
        "CREATE SINK copied FROM passthrough INTO IBMMQ ({options}, 'queue' = '{output}') FORMAT RAW"
    ))
    .await
    .expect("create sink");
    db.start().await.expect("start");
    let batches = mq
        .drain_until(&output, binary, "raw", |batches| {
            batches.iter().map(RecordBatch::num_rows).sum::<usize>() >= 50
        })
        .await;
    let mut received: Vec<Vec<u8>> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_binary::<i32>()
                .iter()
                .flatten()
                .map(<[u8]>::to_vec)
                .collect::<Vec<_>>()
        })
        .collect();
    received.sort();
    let mut expected = payloads;
    expected.sort();
    assert_eq!(received, expected);
    assert!(db.checkpoint().await.expect("checkpoint").success);
    db.shutdown().await.expect("shutdown");
    assert_eq!(mq.depth(&input), 0);
    mq.delete(&[&input, &output]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_columns_survive_a_restart_with_the_same_storage() {
    let Some(mq) = fixture() else { return };
    let (input, output) = (mq.queue("META.IN"), mq.queue("META.OUT"));
    let storage = tempfile::tempdir().unwrap();
    let options = mq.options();
    for (round, ids) in [(0, 0..20_i64), (1, 20..40)] {
        let db = open(storage.path()).await;
        for statement in [
            format!(
                "CREATE SOURCE tagged (id BIGINT) FROM IBMMQ ({options}, 'queue' = '{input}', \
                 'wait.interval.ms' = '50', 'metadata.columns' = 'backout_count,format') FORMAT JSON"
            ),
            "CREATE STREAM routed AS SELECT id, _format AS mq_format, _backout_count AS backouts \
             FROM tagged"
                .to_string(),
            format!(
                "CREATE SINK routed_out FROM routed INTO IBMMQ ({options}, 'queue' = '{output}') \
                 FORMAT JSON"
            ),
        ] {
            db.execute(&statement)
                .await
                .unwrap_or_else(|error| panic!("round {round}: {statement}: {error}"));
        }
        db.start()
            .await
            .unwrap_or_else(|error| panic!("round {round} start: {error}"));
        let batch = events(ids.clone());
        mq.put(&input, batch.schema(), batch, "raw").await;
        let raw = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batches = mq
            .drain_until(&output, raw, "raw", |batches| {
                batches.iter().map(RecordBatch::num_rows).sum::<usize>() >= 20
            })
            .await;
        for text in batches.iter().flat_map(|batch| {
            batch
                .column(0)
                .as_string::<i32>()
                .iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
        }) {
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["mq_format"], "MQSTR", "{value}");
            assert!(ids.contains(&value["id"].as_i64().unwrap()), "{value}");
        }
        assert!(db.checkpoint().await.expect("checkpoint").success);
        db.shutdown().await.expect("shutdown");
    }
    assert_eq!(mq.depth(&input), 0);
    mq.delete(&[&input, &output]);
}
