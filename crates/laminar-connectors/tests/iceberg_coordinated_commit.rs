//! Iceberg designated-committer integration test (REST catalog + MinIO).
//!
//!   docker compose -f tests/docker/iceberg-compose.yml up -d --wait
//!   cargo test -p laminar-connectors --test iceberg_coordinated_commit \
//!     --features iceberg -- --ignored
//!   docker compose -f tests/docker/iceberg-compose.yml down -v
//!
//! Validates that N parallel writers' Parquet files commit as ONE snapshot via
//! the designated committer, and that re-committing the same epoch is a no-op.
#![cfg(feature = "iceberg")]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::{CoordinatedCommitter, SinkConnector};
use laminar_connectors::lakehouse::iceberg::IcebergSink;
use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;
use laminar_connectors::lakehouse::iceberg_io;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn config(table: &str, writer_id: &str) -> ConnectorConfig {
    let mut c = ConnectorConfig::new("iceberg");
    c.set("catalog.uri", "http://localhost:8181");
    c.set("warehouse", "s3://warehouse/wh");
    c.set("storage.type", "s3");
    c.set("namespace", "laminar_test");
    c.set("table.name", table);
    c.set("auto.create", "true");
    c.set("writer.id", writer_id);
    c.set("catalog.property.s3.endpoint", "http://localhost:9000");
    c.set("catalog.property.s3.access-key-id", "minioadmin");
    c.set("catalog.property.s3.secret-access-key", "minioadmin");
    c.set("catalog.property.s3.region", "us-east-1");
    c.set("catalog.property.s3.path-style-access", "true");
    c.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
    c
}

async fn open_sink(table: &str, writer_id: &str) -> IcebergSink {
    let connector_config = config(table, writer_id);
    let sink_config = IcebergSinkConfig::from_config(&connector_config).unwrap();
    let mut sink = IcebergSink::new(sink_config, None);
    sink.open(&connector_config)
        .await
        .expect("open iceberg sink");
    sink
}

/// `(snapshot_count, total_rows)` for the table.
async fn inspect(table: &str) -> (usize, usize) {
    let cfg = IcebergSinkConfig::from_config(&config(table, "inspector")).unwrap();
    let catalog = iceberg_io::build_catalog(&cfg.catalog).await.unwrap();
    let t = iceberg_io::load_table(
        catalog.as_ref(),
        &cfg.catalog.namespace,
        &cfg.catalog.table_name,
    )
    .await
    .unwrap();
    let snapshots = t.metadata().snapshots().count();
    let rows: usize = iceberg_io::scan_table(&t, None, &[])
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    (snapshots, rows)
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn designated_committer_one_snapshot_from_many_writers() {
    if std::net::TcpStream::connect("127.0.0.1:8181").is_err() {
        eprintln!("skipping: Iceberg REST not reachable on 127.0.0.1:8181");
        return;
    }
    let table = format!("events_{}", uuid::Uuid::new_v4().simple());

    // Three writers, each writing distinct rows, hand descriptors to the committer.
    let mut descriptors = Vec::new();
    for w in 0..3i64 {
        let mut writer = open_sink(&table, &format!("w{w}")).await;
        writer.begin_epoch(1).await.unwrap();
        let ids: Vec<i64> = (w * 10..w * 10 + 10).collect();
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
        writer.write_batch(&batch).await.unwrap();
        let descriptor = writer
            .pre_commit(1)
            .await
            .unwrap()
            .expect("coordinated sink yields a descriptor");
        descriptors.push(descriptor);
        writer.close().await.unwrap();
    }

    // One designated commit for all writers' files.
    let mut committer = open_sink(&table, "committer").await;
    committer
        .commit_aggregated(1, descriptors.clone())
        .await
        .unwrap();

    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 1, "all writers must land in a single snapshot");
    assert_eq!(rows, 30, "every writer's rows must be committed");

    // Re-commit the same epoch — idempotent, no new snapshot.
    committer.commit_aggregated(1, descriptors).await.unwrap();
    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 1, "re-commit of a sealed epoch must be a no-op");
    assert_eq!(rows, 30);

    committer.close().await.unwrap();
}
