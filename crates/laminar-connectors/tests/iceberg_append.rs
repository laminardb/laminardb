#![cfg(feature = "iceberg")]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::SinkConnector;
use laminar_connectors::lakehouse::iceberg::IcebergSink;
use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;
use laminar_connectors::lakehouse::iceberg_io;

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

async fn inspect(table: &str) -> (usize, usize) {
    let config = IcebergSinkConfig::from_config(&config(table)).unwrap();
    let catalog = iceberg_io::build_catalog(&config.catalog).await.unwrap();
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
        let mut sink = open_sink(&table).await;
        let ids: Vec<i64> = (writer * 10..writer * 10 + 10).collect();
        let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
        sink.write_batch(&batch).await.unwrap();
        sink.flush().await.unwrap();
        sink.close().await.unwrap();
    }

    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 3);
    assert_eq!(rows, 30);
}
