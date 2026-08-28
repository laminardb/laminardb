#![cfg(feature = "iceberg-core")]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::lakehouse::iceberg::IcebergSink;
use laminar_connectors::lakehouse::iceberg_config::{IcebergSinkConfig, IcebergSourceConfig};
use laminar_connectors::lakehouse::iceberg_io;
use laminar_connectors::lakehouse::IcebergSource;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn config(table: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://warehouse/wh");
    config.set("storage.type", "s3");
    config.set("namespace", "laminar_test");
    config.set("table.name", table);
    config.set("auto.create", "true");
    config.set("catalog.property.s3.endpoint", "http://localhost:9000");
    config.set("catalog.property.s3.access-key-id", "minioadmin");
    config.set("catalog.property.s3.secret-access-key", "minioadmin");
    config.set("catalog.property.s3.region", "us-east-1");
    config.set("catalog.property.s3.path-style-access", "true");
    config.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
    config
}

async fn open_sink(table: &str) -> IcebergSink {
    let connector_config = config(table);
    let mut sink = IcebergSink::new(
        IcebergSinkConfig::from_config(&connector_config).unwrap(),
        None,
    );
    sink.open(&connector_config).await.unwrap();
    sink
}

async fn append_ids(table: &str, ids: Vec<i64>) {
    let mut sink = open_sink(table).await;
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    sink.close().await.unwrap();
}

fn source_config(table: &str) -> ConnectorConfig {
    let mut config = config(table);
    config.set("read.mode", "append");
    config.set("read.bootstrap", "initial");
    config.set("poll.interval", "1ms");
    config
}

async fn start_source(table: &str, position: SourcePosition) -> IcebergSource {
    let config = source_config(table);
    let mut source = IcebergSource::new(IcebergSourceConfig::from_config(&config).unwrap(), None);
    source
        .start(SourceStart::new(config, position, DeliveryGuarantee::AtLeastOnce).unwrap())
        .await
        .unwrap();
    source
}

async fn drain_ids(source: &mut IcebergSource) -> Vec<i64> {
    let mut ids = Vec::new();
    while let Some(batch) = source.poll_batch(4).await.unwrap() {
        let values = batch
            .records
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.extend(values.values());
    }
    ids
}

async fn inspect(table: &str) -> (usize, usize) {
    let config = IcebergSinkConfig::from_config(&config(table)).unwrap();
    let catalog = iceberg_io::build_catalog(&config.catalog, &config.storage)
        .await
        .unwrap();
    let table = iceberg_io::load_table(
        catalog.as_ref(),
        &config.catalog.namespace,
        &config.catalog.table_name,
    )
    .await
    .unwrap();
    let snapshots = table.metadata().snapshots().count();
    let rows = iceberg_io::scan_table(&table, None, &[])
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    (snapshots, rows)
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn ordinary_appends_are_visible_from_multiple_writers() {
    if std::net::TcpStream::connect("127.0.0.1:8181").is_err() {
        return;
    }
    let table = format!("events_{}", uuid::Uuid::new_v4().simple());

    for writer in 0..3i64 {
        append_ids(&table, (writer * 10..writer * 10 + 10).collect()).await;
    }

    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 3);
    assert_eq!(rows, 30);
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn append_source_bootstrap_and_resume_do_not_duplicate_snapshots() {
    if std::net::TcpStream::connect("127.0.0.1:8181").is_err() {
        return;
    }
    let table = format!("source_{}", uuid::Uuid::new_v4().simple());
    append_ids(&table, (0..6).collect()).await;

    let mut initial = start_source(&table, SourcePosition::Initial).await;
    assert_eq!(drain_ids(&mut initial).await, (0..6).collect::<Vec<_>>());
    let bootstrap_cursor = initial.checkpoint();
    initial.close().await.unwrap();

    append_ids(&table, (10..14).collect()).await;
    append_ids(&table, (20..24).collect()).await;
    let mut resumed = start_source(
        &table,
        SourcePosition::Resume {
            attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(2),
            checkpoint: bootstrap_cursor,
        },
    )
    .await;
    assert_eq!(
        drain_ids(&mut resumed).await,
        vec![10, 11, 12, 13, 20, 21, 22, 23]
    );
    let resumed_cursor = resumed.checkpoint();
    resumed.close().await.unwrap();

    let mut replay = start_source(
        &table,
        SourcePosition::Resume {
            attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(3),
            checkpoint: resumed_cursor,
        },
    )
    .await;
    assert!(drain_ids(&mut replay).await.is_empty());
    replay.close().await.unwrap();
}
