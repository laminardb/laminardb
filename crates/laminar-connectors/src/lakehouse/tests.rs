use super::*;

#[test]
fn test_register_delta_lake_sink() {
    let registry = ConnectorRegistry::new();
    register_delta_lake_sink(&registry).unwrap();

    let info = registry.sink_info("delta-lake");
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "delta-lake");
    assert!(info.is_sink);
    assert!(!info.is_source);
    assert!(!info.config_keys.is_empty());
}

#[test]
fn test_config_keys_required() {
    let keys = delta_lake_config_keys();
    let required: Vec<&str> = keys
        .iter()
        .filter(|k| k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(required.contains(&"table.path"));
    assert_eq!(required.len(), 1);
}

#[test]
fn test_config_keys_include_cloud_storage() {
    let keys = delta_lake_config_keys();
    let key_names: Vec<&str> = keys.iter().map(|k| k.key.as_str()).collect();
    assert!(key_names.contains(&"storage.aws_access_key_id"));
    assert!(key_names.contains(&"storage.aws_secret_access_key"));
    assert!(key_names.contains(&"storage.aws_region"));
    assert!(key_names.contains(&"storage.azure_storage_account_name"));
    assert!(key_names.contains(&"storage.azure_storage_account_key"));
    assert!(key_names.contains(&"storage.google_service_account_path"));
}

#[test]
fn test_config_keys_optional_present() {
    let keys = delta_lake_config_keys();
    let optional: Vec<&str> = keys
        .iter()
        .filter(|k| !k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(optional.contains(&"partition.columns"));
    assert!(optional.contains(&"target.file.size"));
    assert!(optional.contains(&"write.mode"));
    assert!(!optional.contains(&"delivery.guarantee"));
    assert!(optional.contains(&"merge.key.columns"));
    assert!(optional.contains(&"schema.evolution"));
    assert!(!optional.contains(&"checkpoint.interval"));
    assert!(!optional.contains(&"max.commit.retries"));
    assert!(!optional.iter().any(|key| key.starts_with("compaction.")));
    assert!(!optional.contains(&"vacuum.retention.hours"));
    assert!(!optional.contains(&"writer.id"));
    // Catalog keys
    assert!(optional.contains(&"catalog.type"));
    assert!(optional.contains(&"catalog.database"));
    assert!(optional.contains(&"catalog.name"));
    assert!(optional.contains(&"catalog.schema"));
    assert!(optional.contains(&"catalog.workspace_url"));
    assert!(optional.contains(&"catalog.access_token"));
    assert!(optional.contains(&"catalog.storage.location"));
}

#[test]
fn test_factory_creates_sink() {
    let registry = ConnectorRegistry::new();
    register_delta_lake_sink(&registry).unwrap();

    let mut config = crate::config::ConnectorConfig::new("delta-lake");
    config.set("table.path", "/tmp/laminardb-factory-test");
    let sink = registry.create_sink(&config, None);
    assert!(sink.is_ok());
}

// ── Delta Lake source registration tests ──

#[test]
fn test_register_delta_lake_source() {
    let registry = ConnectorRegistry::new();
    register_delta_lake_source(&registry).unwrap();

    let info = registry.source_info("delta-lake");
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "delta-lake");
    assert!(info.is_source);
    assert!(!info.is_sink);
    assert!(!info.config_keys.is_empty());
}

#[test]
fn test_source_config_keys() {
    let keys = delta_lake_source_config_keys();
    let required: Vec<&str> = keys
        .iter()
        .filter(|k| k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(required.contains(&"table.path"));
    assert_eq!(required.len(), 1);

    let optional: Vec<&str> = keys
        .iter()
        .filter(|k| !k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(optional.contains(&"starting.version"));
    assert!(optional.contains(&"poll.interval.ms"));
    // Catalog keys
    assert!(optional.contains(&"catalog.type"));
    assert!(optional.contains(&"catalog.database"));
}

#[test]
fn test_factory_creates_source() {
    let registry = ConnectorRegistry::new();
    register_delta_lake_source(&registry).unwrap();

    let config = crate::config::ConnectorConfig::new("delta-lake");
    let source = registry.create_source(&config, None);
    assert!(source.is_ok());
}

#[test]
fn test_register_lakehouse_sinks() {
    let registry = ConnectorRegistry::new();
    register_lakehouse_sinks(&registry).unwrap();

    assert!(registry.sink_info("delta-lake").is_some());
    assert!(registry.sink_info("iceberg").is_some());
}

// ── Iceberg registration tests ──

#[test]
fn test_register_iceberg_sink() {
    let registry = ConnectorRegistry::new();
    register_iceberg_sink(&registry).unwrap();

    let info = registry.sink_info("iceberg");
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "iceberg");
    assert!(info.is_sink);
    assert!(!info.is_source);
    assert!(!info.config_keys.is_empty());
}

#[test]
fn test_register_iceberg_source() {
    let registry = ConnectorRegistry::new();
    register_iceberg_source(&registry).unwrap();

    let info = registry.source_info("iceberg");
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "iceberg");
    assert!(info.is_source);
    assert!(!info.is_sink);
}

#[test]
fn test_iceberg_sink_config_keys() {
    let keys = iceberg_sink_config_keys();
    let required: Vec<&str> = keys
        .iter()
        .filter(|k| k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(required.contains(&"catalog.uri"));
    assert!(required.contains(&"warehouse"));
    assert!(required.contains(&"namespace"));
    assert!(required.contains(&"table.name"));
    assert_eq!(required.len(), 4);
}

#[test]
fn test_iceberg_source_config_keys() {
    let keys = iceberg_source_config_keys();
    let required: Vec<&str> = keys
        .iter()
        .filter(|k| k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(required.contains(&"catalog.uri"));
    assert!(required.contains(&"warehouse"));
    assert!(required.contains(&"namespace"));
    assert!(required.contains(&"table.name"));
    assert_eq!(required.len(), 4);
}

#[test]
fn test_factory_creates_iceberg_sink() {
    let registry = ConnectorRegistry::new();
    register_iceberg_sink(&registry).unwrap();

    let mut config = crate::config::ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/warehouse");
    config.set("namespace", "default");
    config.set("table.name", "events");
    let sink = registry.create_sink(&config, None);
    assert!(sink.is_ok());
}

#[test]
fn test_factory_creates_iceberg_source() {
    let registry = ConnectorRegistry::new();
    register_iceberg_source(&registry).unwrap();

    let config = crate::config::ConnectorConfig::new("iceberg");
    let source = registry.create_source(&config, None);
    assert!(source.is_ok());
}
