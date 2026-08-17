use super::*;
use arrow_schema::{DataType, Field, Schema};

fn sink_factory_config() -> crate::config::ConnectorConfig {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let mut config = crate::config::ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://localhost:27017");
    config.set("database", "db");
    config.set("collection", "out");
    config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(&schema),
    );
    config
}

#[test]
fn test_register_mongodb_cdc_source() {
    let registry = ConnectorRegistry::new();
    register_mongodb_cdc_source(&registry).unwrap();

    let info = registry.source_info("mongodb-cdc");
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "mongodb-cdc");
    assert!(info.is_source);
    assert!(!info.is_sink);
    assert!(!info.config_keys.is_empty());
}

#[test]
fn test_register_mongodb_sink() {
    let registry = ConnectorRegistry::new();
    register_mongodb_sink(&registry).unwrap();

    let info = registry.sink_info("mongodb-sink");
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.name, "mongodb-sink");
    assert!(info.is_sink);
    assert!(!info.is_source);
    assert!(!info.config_keys.is_empty());
}

#[test]
fn test_cdc_config_keys() {
    let keys = mongodb_cdc_config_keys();
    let required: Vec<&str> = keys
        .iter()
        .filter(|k| k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(required.contains(&"connection.uri"));
    assert!(required.contains(&"database"));
    assert!(required.contains(&"collection"));
    let byte_budget = keys
        .iter()
        .find(|key| key.key == "max.buffered.bytes")
        .expect("MongoDB CDC byte budget must be discoverable");
    assert_eq!(
        byte_budget
            .default
            .as_deref()
            .and_then(|value| value.parse::<usize>().ok()),
        Some(config::DEFAULT_MAX_BUFFERED_BYTES)
    );
    let pipeline = keys
        .iter()
        .find(|key| key.key == "pipeline")
        .expect("MongoDB CDC pipeline must be discoverable");
    assert_eq!(pipeline.default.as_deref(), Some("[]"));
    for removed in [
        "batch.size",
        "max.buffered.events",
        "max.await.time.ms",
        "resume.token.store",
        "split.large.events",
        "max.poll.records",
    ] {
        assert!(keys.iter().all(|key| key.key != removed));
    }
}

#[test]
fn lookup_config_keys_are_minimal() {
    let keys = mongodb_lookup_config_keys();
    assert_eq!(keys.len(), 3);
    assert!(keys.iter().all(|key| key.required));
    assert!(keys.iter().all(|key| !matches!(
        key.key.as_str(),
        "full.document.mode" | "max.buffered.bytes"
    )));
}

#[test]
fn test_sink_config_keys() {
    let keys = mongodb_sink_config_keys();
    let required: Vec<&str> = keys
        .iter()
        .filter(|k| k.required)
        .map(|k| k.key.as_str())
        .collect();
    assert!(required.contains(&"connection.uri"));
    assert!(required.contains(&"database"));
    assert!(required.contains(&"collection"));
    for removed in [
        "batch.size",
        "ordered",
        "write.mode.upsert_on_missing",
        "write_concern.journal",
        "write_concern.timeout_ms",
    ] {
        assert!(keys.iter().all(|key| key.key != removed));
    }
    let write_timeout = keys
        .iter()
        .find(|key| key.key == "sink.write.timeout.ms")
        .expect("write timeout must be discoverable");
    assert_eq!(write_timeout.default.as_deref(), Some("30000"));
}

#[test]
fn test_factory_creates_source() {
    let registry = ConnectorRegistry::new();
    register_mongodb_cdc_source(&registry).unwrap();

    let config = crate::config::ConnectorConfig::new("mongodb-cdc");
    let source = registry.create_source(&config, None);
    assert!(source.is_ok());
}

#[test]
fn test_factory_creates_sink() {
    let registry = ConnectorRegistry::new();
    register_mongodb_sink(&registry).unwrap();

    let sink = registry.create_sink(&sink_factory_config(), None).unwrap();
    assert_eq!(sink.schema().field(0).name(), "id");
}

#[test]
fn sink_factory_rejects_missing_and_malformed_schema() {
    let registry = ConnectorRegistry::new();
    register_mongodb_sink(&registry).unwrap();

    let mut missing = sink_factory_config();
    let mut properties = missing.properties().clone();
    properties.remove("_arrow_schema");
    missing = crate::config::ConnectorConfig::with_properties("mongodb-sink", properties);
    let missing_error = registry
        .create_sink(&missing, None)
        .err()
        .expect("missing schema must fail")
        .to_string();
    assert!(missing_error.contains("_arrow_schema"), "{missing_error}");

    let mut malformed = sink_factory_config();
    malformed.set("_arrow_schema", "not-arrow-ipc");
    let malformed_error = registry
        .create_sink(&malformed, None)
        .err()
        .expect("malformed schema must fail")
        .to_string();
    assert!(
        malformed_error.contains("invalid") && malformed_error.contains("_arrow_schema"),
        "{malformed_error}"
    );
}
