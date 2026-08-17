use super::*;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn test_batch(n: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..n as i64).collect();
    RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
}

fn test_connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://test/wh");
    config.set("namespace", "test");
    config.set("table.name", "events");
    config
}

fn test_config() -> IcebergSinkConfig {
    IcebergSinkConfig::from_config(&test_connector_config()).unwrap()
}

#[test]
fn test_new_sink() {
    let sink = IcebergSink::new(test_config(), None);
    assert!(sink.schema.is_none());
    assert_eq!(sink.buffered_rows, 0);
}

#[tokio::test]
async fn test_write_buffers_batches() {
    let mut sink = IcebergSink::new(test_config(), None);

    let result = sink.write_batch(&test_batch(100)).await.unwrap();
    assert_eq!(result.records_written, 100);
    assert_eq!(sink.buffered_rows, 100);
    assert_eq!(sink.buffer.len(), 1);

    let result = sink.write_batch(&test_batch(50)).await.unwrap();
    assert_eq!(result.records_written, 50);
    assert_eq!(sink.buffered_rows, 150);
    assert_eq!(sink.buffer.len(), 2);
}

#[test]
fn test_contract() {
    let sink = IcebergSink::new(test_config(), None);
    let contract = sink.contract(&test_connector_config()).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert!(sink.as_coordinated_committer().is_none());
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(300));
}

#[test]
fn test_contract_uses_singleton_for_wired_local_storage() {
    let sink = IcebergSink::new(test_config(), None);
    let mut config = test_connector_config();
    config.set("warehouse", "file:///tmp/iceberg");
    config.set("storage.type", "fs");

    let contract = sink.contract(&config).unwrap();
    assert_eq!(contract.topology, SinkTopology::Singleton);
}

#[test]
fn test_contract_uses_multi_writer_for_named_s3_warehouse() {
    let sink = IcebergSink::new(test_config(), None);
    let mut config = test_connector_config();
    config.set("warehouse", "production");
    config.set("storage.type", "s3");

    let contract = sink.contract(&config).unwrap();
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
}
