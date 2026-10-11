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
    assert!(required.contains(&"namespace"));
    assert!(required.contains(&"table.name"));
    assert_eq!(required.len(), 3);

    let optional: Vec<&str> = keys
        .iter()
        .filter(|key| !key.required)
        .map(|key| key.key.as_str())
        .collect();
    for expected in [
        "warehouse",
        "catalog.warehouse",
        "write.mode",
        "target.file.size.bytes",
        "parquet.row.group.size.bytes",
        "max.buffer.bytes",
        "max.open.partitions",
        "max.descriptor.bytes",
        "write.distribution.mode",
        "schema.evolution.mode",
        "storage.encryption",
    ] {
        assert!(optional.contains(&expected), "missing {expected}");
    }
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
    assert!(required.contains(&"namespace"));
    assert!(required.contains(&"table.name"));
    assert_eq!(required.len(), 3);

    let optional: Vec<&str> = keys
        .iter()
        .filter(|key| !key.required)
        .map(|key| key.key.as_str())
        .collect();
    for expected in [
        "warehouse",
        "catalog.warehouse",
        "read.mode",
        "read.bootstrap",
        "start.snapshot.id",
        "table.ref",
        "poll.interval",
        "projection",
        "filter",
        "read.max.manifest.list.bytes",
        "read.max.manifest.bytes",
        "read.max.manifests.per.snapshot",
    ] {
        assert!(optional.contains(&expected), "missing {expected}");
    }
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

/// An on-demand lookup cannot renew vended credentials, so it refuses them at open.
#[cfg(feature = "delta-lake-unity")]
#[tokio::test]
async fn delta_lookup_refuses_vended_unity_credentials() {
    use crate::registry::LookupSourceFactory as _;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables/main.lake.dims"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "dims", "catalog_name": "main", "schema_name": "lake",
            "table_type": "EXTERNAL", "data_source_format": "DELTA", "columns": [],
            "storage_location": "s3://bucket/dims", "properties": {},
            "created_at": 0, "updated_at": 0, "table_id": "uc-dims"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "aws_temp_credentials": {"access_key_id": "A", "secret_access_key": "S"},
            "expiration_time": 4_102_444_800_000_i64, "url": "s3://bucket/dims"
        })))
        .mount(&server)
        .await;
    let mut config = crate::config::ConnectorConfig::new("delta-lake");
    for (key, value) in [
        ("table.path", "uc://main.lake.dims"),
        ("catalog.type", "unity"),
        ("catalog.workspace_url", server.uri().as_str()),
        ("catalog.access_token", "dapi-test"),
        ("catalog.name", "main"),
        ("catalog.schema", "lake"),
        ("_primary_key_columns", "id"),
    ] {
        config.set(key, value);
    }
    let error = DeltaLookupFactory
        .build(config, None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("cannot renew"), "{error}");
}
