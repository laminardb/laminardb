//! Cross-engine Delta qualification against Spark 4.2 / delta-spark 4.4.
//!
//! Spark writes a change-data-feed table with deletion vectors through INSERT, UPDATE, DELETE,
//! MERGE, a key change, and OPTIMIZE; the LaminarDB source must reproduce Spark's final table
//! from both the change feed and a snapshot. LaminarDB then mirrors that change feed into an
//! upsert table and writes a partitioned append table, and Spark must read both back exactly.
//!
//! Opt-in: `docker compose -f tests/docker/delta-spark-compose.yml up -d`, then
//! `LAMINAR_DELTA_SPARK_ORACLE=1 cargo test -p laminar-connectors --features delta-lake
//! --test delta_spark_interop -- --nocapture`. Without the variable the test reports a skip.

#![cfg(feature = "delta-lake")]
#![allow(clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::lakehouse::{
    DeltaLakeSink, DeltaLakeSinkConfig, DeltaSource, DeltaSourceConfig, DeltaWriteMode,
};

const ORACLE_ENV: &str = "LAMINAR_DELTA_SPARK_ORACLE";

type Row = (i64, String, i64);

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Run one oracle command in the compose service and return its JSON result.
fn oracle(command: &str, table: &str) -> serde_json::Value {
    let compose = workspace().join("tests/docker/delta-spark-compose.yml");
    let output = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg(&compose)
        .args(["exec", "-T", "delta-spark", "python3", "/scripts/oracle.py"])
        .args([command, &format!("/lake/{table}")])
        .output()
        .expect("docker compose is installed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "oracle {command} failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_str(stdout.lines().last().unwrap_or_default()).expect("oracle JSON")
}

fn spark_rows(result: &serde_json::Value) -> Vec<Row> {
    let mut rows: Vec<Row> = result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row[0].as_i64().unwrap(),
                row[1].as_str().unwrap().to_string(),
                row[2].as_i64().unwrap(),
            )
        })
        .collect();
    rows.sort();
    rows
}

fn location(table: &str) -> String {
    workspace()
        .join("target/delta-spark-interop")
        .join(table)
        .to_string_lossy()
        .into_owned()
}

async fn start_source(table: &str, options: &[(&str, &str)]) -> DeltaSource {
    let mut config = ConnectorConfig::new("delta-lake");
    config.set("table.path", location(table));
    config.set("poll.interval.ms", "1");
    for (key, value) in options {
        config.set(*key, *value);
    }
    let mut source = DeltaSource::new(DeltaSourceConfig::default(), None);
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
    source
}

/// Every batch the source emits until it reports no more data.
async fn drain(source: &mut DeltaSource) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = source.poll_batch(97).await.unwrap() {
        batches.push(batch.records);
    }
    batches
}

/// Net rows of a changelog: each `(id, label, qty)` with its summed `__weight`, which must be 1.
fn net(batches: &[RecordBatch]) -> Vec<Row> {
    let mut net = BTreeMap::<Row, i64>::new();
    for batch in batches {
        let column = |name: &str| batch.column_by_name(name).unwrap();
        let ids = column("id").as_any().downcast_ref::<Int64Array>().unwrap();
        let labels = column("label")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let qty = column("qty").as_any().downcast_ref::<Int64Array>().unwrap();
        let weights = column("__weight")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            *net.entry((
                ids.value(row),
                labels.value(row).to_string(),
                qty.value(row),
            ))
            .or_default() += weights.value(row);
        }
    }
    net.into_iter()
        .filter(|(_, weight)| {
            assert!((0..=1).contains(weight), "net weight {weight} is not a set");
            *weight == 1
        })
        .map(|(row, _)| row)
        .collect()
}

async fn write_through(
    sink_table: &str,
    mode: DeltaWriteMode,
    partitioned: bool,
    batches: &[RecordBatch],
) {
    std::fs::create_dir_all(location(sink_table)).unwrap();
    let mut config = DeltaLakeSinkConfig::new(&location(sink_table));
    config.auto_create = true;
    config.write_mode = mode;
    if mode == DeltaWriteMode::Upsert {
        config.merge_key_columns = vec!["id".into()];
    }
    if partitioned {
        config.partition_columns = vec!["label".into()];
    }
    let mut sink = DeltaLakeSink::with_schema(config, batches[0].schema());
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .unwrap();
    for batch in batches {
        sink.write_batch(batch).await.unwrap();
        sink.flush().await.unwrap();
    }
    sink.close().await.unwrap();
}

fn rows_only(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, false),
    ]));
    batches
        .iter()
        .map(|batch| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::clone(batch.column_by_name("id").unwrap()),
                    Arc::clone(batch.column_by_name("label").unwrap()),
                    Arc::clone(batch.column_by_name("qty").unwrap()),
                ],
            )
            .unwrap()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spark_change_feed_and_laminar_tables_agree_across_engines() {
    if std::env::var(ORACLE_ENV).as_deref() != Ok("1") {
        eprintln!("skipping: set {ORACLE_ENV}=1 with tests/docker/delta-spark-compose.yml up");
        return;
    }
    let run = format!(
        "run_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let spark_table = format!("{run}_spark");
    let written = oracle("write_changes", &spark_table);
    let expected = spark_rows(&written);
    eprintln!(
        "spark wrote {} rows at version {}, protocol {}",
        expected.len(),
        written["version"],
        written["protocol"]
    );

    // The full change feed from the first data commit nets to Spark's table.
    let mut changes = start_source(&spark_table, &[("starting.version", "1")]).await;
    let feed = drain(&mut changes).await;
    assert_eq!(net(&feed), expected, "change feed");

    // A snapshot of the latest version, through the deletion vectors, equals it too.
    let mut snapshot = start_source(&spark_table, &[("read.mode", "snapshot")]).await;
    let bootstrap = drain(&mut snapshot).await;
    assert_eq!(net(&bootstrap), expected, "snapshot");

    // The change feed mirrored by an upsert sink reads back in Spark as Spark's own table.
    let mirror = format!("{run}_mirror");
    write_through(&mirror, DeltaWriteMode::Upsert, false, &feed).await;
    assert_eq!(
        spark_rows(&oracle("read", &mirror)),
        expected,
        "upsert mirror"
    );

    // A partitioned append table written from the snapshot reads back in Spark.
    let appended = format!("{run}_append");
    write_through(
        &appended,
        DeltaWriteMode::Append,
        true,
        &rows_only(&bootstrap),
    )
    .await;
    assert_eq!(
        spark_rows(&oracle("read", &appended)),
        expected,
        "partitioned append"
    );

    // Column mapping: snapshots read through the logical names; the kernel change feed
    // rejects mapped tables, and the source says so before reading.
    let mapped = format!("{run}_mapped");
    let expected = spark_rows(&oracle("write_column_mapped", &mapped));
    let mut snapshot = start_source(&mapped, &[("read.mode", "snapshot")]).await;
    assert_eq!(
        net(&drain(&mut snapshot).await),
        expected,
        "column-mapped snapshot"
    );
    let mut config = ConnectorConfig::new("delta-lake");
    config.set("table.path", location(&mapped));
    let error = DeltaSource::new(DeltaSourceConfig::default(), None)
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("column-mapped"), "{error}");
}
