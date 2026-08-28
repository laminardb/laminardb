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
fn typed_storage_backends_parse_without_silent_fallback() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "demo");
    config.set("storage.type", "gcs");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    let parsed = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(parsed.storage.storage_type, Some(IcebergStorageType::Gcs));

    config.set("storage.type", "hdfs");
    assert!(IcebergSinkConfig::from_config(&config).is_err());
}

#[test]
fn legacy_and_typed_option_aliases_match() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("snapshot.id", "42");
    config.set("select.columns", "id, payload");
    config.set("target.file.size", "4096");
    config.set("catalog.property.s3.path-style-access", "true");

    let source = IcebergSourceConfig::from_config(&config).unwrap();
    let sink = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(source.snapshot_id, Some(42));
    assert_eq!(source.select_columns, ["id", "payload"]);
    assert_eq!(sink.target_file_size_bytes, 4096);
    assert!(sink.storage.path_style);
}

#[test]
fn debug_redacts_catalog_and_storage_values() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "https://user:secret@catalog.test");
    config.set("warehouse", "s3://bucket/wh?token=secret-token");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("catalog.property.token", "secret-token");
    config.set("storage.endpoint", "https://secret-endpoint.test");
    config.set("storage.property.aws_secret_access_key", "secret-key");

    let parsed = IcebergSinkConfig::from_config(&config).unwrap();
    let debug = format!("{parsed:?}");
    for secret in [
        "user:secret",
        "secret-token",
        "secret-endpoint",
        "secret-key",
    ] {
        assert!(!debug.contains(secret), "Debug leaked {secret}");
    }
}

#[test]
fn invalid_mode_combinations_are_rejected() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("read.mode", "snapshot");
    config.set("read.bootstrap", "none");
    assert!(IcebergSourceConfig::from_config(&config).is_err());
}

#[test]
fn catalog_auth_is_inferred_without_permitting_mixed_credentials() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "https://catalog.test");
    config.set("catalog.warehouse", "s3://bucket/warehouse");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("catalog.property.token", "resolved-secret");
    assert_eq!(
        IcebergCatalogConfig::from_config(&config)
            .unwrap()
            .auth_type,
        IcebergCatalogAuthType::Bearer
    );

    config.set("catalog.auth.type", "oauth2");
    assert!(IcebergCatalogConfig::from_config(&config).is_err());
    config.set("catalog.property.credential", "client:resolved-secret");
    assert!(IcebergCatalogConfig::from_config(&config).is_err());
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
