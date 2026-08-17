use super::*;

#[test]
fn test_catalog_type_parse() {
    assert_eq!(
        "rest".parse::<IcebergCatalogType>().unwrap(),
        IcebergCatalogType::Rest
    );
    assert!("unknown".parse::<IcebergCatalogType>().is_err());
}

#[test]
fn test_sink_config_from_config() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("compression", "snappy");

    let cfg = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.catalog.catalog_uri, "http://localhost:8181");
    assert_eq!(cfg.catalog.warehouse, "s3://bucket/wh");
    assert_eq!(cfg.catalog.namespace, "prod");
    assert_eq!(cfg.catalog.table_name, "events");
    assert_eq!(cfg.compression, "snappy");
}

#[test]
fn test_source_config_from_config() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "dim_customers");
    config.set("poll.interval.ms", "30000");
    config.set("snapshot.id", "42");

    let cfg = IcebergSourceConfig::from_config(&config).unwrap();
    assert_eq!(cfg.poll_interval, Duration::from_secs(30));
    assert_eq!(cfg.snapshot_id, Some(42));
}

#[test]
fn test_missing_required_field() {
    let config = ConnectorConfig::new("iceberg");
    assert!(IcebergSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_defaults() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");

    let cfg = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.compression, "zstd");
    assert!(!cfg.auto_create);
}

#[test]
fn test_unwired_storage_backends_are_rejected() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "demo");
    config.set("storage.type", "gcs");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    assert!(IcebergSinkConfig::from_config(&config).is_err());

    config.set("storage.type", "s3");
    config.set("warehouse", "abfs://container/warehouse");
    assert!(IcebergSinkConfig::from_config(&config).is_err());
}

// ── Schema validation tests ──

use arrow_schema::{DataType, Field, Schema};

fn schema(fields: Vec<(&str, DataType)>) -> Schema {
    Schema::new(
        fields
            .into_iter()
            .map(|(n, t)| Field::new(n, t, true))
            .collect::<Vec<_>>(),
    )
}

#[test]
fn test_validate_matching_schemas() {
    let s = schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
    assert!(validate_sink_schema(&s, &s).is_ok());
}

#[test]
fn test_validate_missing_field() {
    let pipeline = schema(vec![("id", DataType::Int64), ("extra", DataType::Utf8)]);
    let table = schema(vec![("id", DataType::Int64)]);
    let err = validate_sink_schema(&pipeline, &table).unwrap_err();
    assert!(err.to_string().contains("extra"));
}

#[test]
fn test_validate_type_mismatch() {
    let pipeline = schema(vec![("id", DataType::Int64)]);
    let table = schema(vec![("id", DataType::Utf8)]);
    let err = validate_sink_schema(&pipeline, &table).unwrap_err();
    assert!(err.to_string().contains("incompatible"));
}

#[test]
fn test_validate_extra_table_columns_ok() {
    let pipeline = schema(vec![("id", DataType::Int64)]);
    let table = schema(vec![("id", DataType::Int64), ("extra", DataType::Utf8)]);
    assert!(validate_sink_schema(&pipeline, &table).is_ok());
}

#[test]
fn test_validate_safe_widening() {
    let pipeline = schema(vec![("n", DataType::Int32), ("f", DataType::Float32)]);
    let table = schema(vec![("n", DataType::Int64), ("f", DataType::Float64)]);
    assert!(validate_sink_schema(&pipeline, &table).is_ok());
}
