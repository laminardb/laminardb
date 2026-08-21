use super::*;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

fn test_source_config() -> IcebergSourceConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://test/wh");
    config.set("namespace", "test");
    config.set("table.name", "dim_customers");
    IcebergSourceConfig::from_config(&config).unwrap()
}

fn declared_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

#[test]
fn construction_carries_declared_non_null_key_schema() {
    let source = IcebergReferenceTableSource::new(test_source_config(), declared_schema()).unwrap();
    assert_eq!(source.config.select_columns, vec!["id", "name"]);
    assert_eq!(source.declared_schema.field(0).name(), "id");
    assert!(!source.declared_schema.field(0).is_nullable());
}

#[test]
fn conflicting_explicit_projection_is_rejected() {
    let mut config = test_source_config();
    config.select_columns = vec!["name".into()];
    assert!(IcebergReferenceTableSource::new(config, declared_schema()).is_err());
}

#[tokio::test]
async fn exhaustion_and_close_are_stable_without_external_io() {
    let mut source =
        IcebergReferenceTableSource::new(test_source_config(), declared_schema()).unwrap();
    source.phase = Phase::Draining;
    assert!(source.poll_snapshot().await.unwrap().is_none());
    assert!(source.poll_snapshot().await.unwrap().is_none());
    source.close().await.unwrap();
    source.close().await.unwrap();
    assert!(source.poll_snapshot().await.is_err());
}
