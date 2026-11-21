//! A string column produced by a SQL cast (`CAST(x AS VARCHAR)` types as `Utf8View`) reaches
//! the exactly-once lakehouse sinks, which store it in their own string type.
#![cfg(any(feature = "delta-lake", feature = "iceberg"))]
#![allow(clippy::disallowed_types)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{DataType, Int64Type};
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_db::LaminarDB;

const ROWS: i64 = 200;
const CONVERGE: Duration = Duration::from_secs(60);

async fn open(storage: &Path) -> Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(300),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
        .config_var("E2E_MINIO_SECRET", "minioadmin")
        .build()
        .await
        .expect("open database")
}

/// Runs `seq` and its text cast into `sink`, checkpointing until `read` sees every row.
async fn write_labelled_rows<F, Fut>(sink: &str, mut read: F) -> Vec<(i64, String)>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Vec<(i64, String)>>,
{
    let storage = tempfile::tempdir().unwrap();
    let db = open(storage.path()).await;
    for statement in [
        format!(
            "CREATE SOURCE gen FROM GENERATOR ('splits' = '2', 'max.rows' = '{ROWS}', \
             'rows.per.second' = '1000')"
        ),
        "CREATE STREAM labelled AS SELECT seq, CAST(seq AS VARCHAR) AS label FROM gen".into(),
        format!("CREATE SINK lake FROM labelled INTO {sink}"),
    ] {
        db.execute(&statement)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    db.start().await.expect("start");
    let deadline = tokio::time::Instant::now() + CONVERGE;
    let rows = loop {
        let _ = db.checkpoint().await;
        let rows = read().await;
        if rows.len() >= usize::try_from(ROWS).unwrap() || tokio::time::Instant::now() >= deadline {
            break rows;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    db.shutdown().await.expect("shutdown");
    rows
}

fn labelled(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let seq = batch
            .column_by_name("seq")
            .unwrap()
            .as_primitive::<Int64Type>();
        let label =
            arrow::compute::cast(batch.column_by_name("label").unwrap(), &DataType::Utf8).unwrap();
        let label = label.as_string::<i32>();
        rows.extend((0..batch.num_rows()).map(|row| (seq.value(row), label.value(row).to_owned())));
    }
    rows.sort();
    rows
}

fn every_row() -> Vec<(i64, String)> {
    (0..ROWS).map(|seq| (seq, seq.to_string())).collect()
}

#[cfg(feature = "delta-lake")]
async fn delta_rows(table: &str) -> Vec<(i64, String)> {
    if !Path::new(table).join("_delta_log").exists() {
        return Vec::new();
    }
    let ctx = datafusion::prelude::SessionContext::new();
    laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
        &ctx,
        "t",
        table,
        std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    let batches = ctx
        .sql("SELECT seq, label FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    labelled(&batches)
}

#[cfg(feature = "delta-lake")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cast_string_column_reaches_the_delta_sink() {
    let directory = tempfile::tempdir().unwrap();
    let table = directory
        .path()
        .join("table")
        .to_string_lossy()
        .replace('\\', "/");
    let sink = format!(
        "\"delta-lake\" ('table.path' = '{table}', 'write.mode' = 'append', 'auto.create' = 'true')"
    );
    let rows = write_labelled_rows(&sink, || delta_rows(&table)).await;
    assert_eq!(rows, every_row());
}

#[cfg(feature = "iceberg")]
fn iceberg_options(table: &str, secret: &str) -> Vec<(&'static str, String)> {
    vec![
        ("catalog.uri", "http://localhost:8181".into()),
        ("warehouse", "s3://warehouse/wh".into()),
        ("storage.type", "s3".into()),
        ("namespace", "laminar_lakehouse_sink".into()),
        ("table.name", table.into()),
        ("auto.create", "true".into()),
        ("storage.endpoint", "http://localhost:9000".into()),
        ("storage.region", "us-east-1".into()),
        ("storage.path_style", "true".into()),
        ("storage.property.s3.access-key-id", "minioadmin".into()),
        ("storage.property.s3.secret-access-key", secret.into()),
    ]
}

#[cfg(feature = "iceberg")]
async fn iceberg_rows(table: &str) -> Vec<(i64, String)> {
    use futures_util::TryStreamExt;
    use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;
    use laminar_connectors::lakehouse::iceberg_io;

    let mut config = laminar_connectors::config::ConnectorConfig::new("iceberg");
    for (key, value) in iceberg_options(table, "minioadmin") {
        config.set(key, value);
    }
    let config = IcebergSinkConfig::from_config(&config).unwrap();
    let catalog = iceberg_io::build_catalog(&config.catalog, &config.storage)
        .await
        .unwrap();
    let Ok(table) = iceberg_io::load_table(
        catalog.as_ref(),
        &config.catalog.namespace,
        &config.catalog.table_name,
    )
    .await
    else {
        return Vec::new();
    };
    let batches: Vec<RecordBatch> = table
        .scan()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    labelled(&batches)
}

#[cfg(feature = "iceberg")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cast_string_column_reaches_the_iceberg_sink() {
    let catalog = std::net::SocketAddr::from(([127, 0, 0, 1], 8181));
    if std::net::TcpStream::connect_timeout(&catalog, Duration::from_secs(2)).is_err() {
        assert!(
            std::env::var("LAMINAR_REQUIRE_ICEBERG").is_err(),
            "the Iceberg fixture (tests/docker/iceberg-compose.yml) is required"
        );
        eprintln!("skipping: Iceberg REST catalog is not reachable on {catalog}");
        return;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("labelled_{nanos:x}");
    let options = iceberg_options(&table, "${E2E_MINIO_SECRET}")
        .iter()
        .map(|(key, value)| format!("'{key}' = '{value}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let rows =
        write_labelled_rows(&format!("\"iceberg\" ({options})"), || iceberg_rows(&table)).await;
    assert_eq!(rows, every_row());
}
