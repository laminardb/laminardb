#![cfg(any(feature = "postgres-cdc", feature = "mongodb-cdc"))]

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::registry::ConnectorRegistry;

#[cfg(feature = "postgres-cdc")]
#[test]
fn postgres_cdc_admission_rejects_unexecuted_options_and_reference_use() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::postgres::register_postgres_cdc_source(&registry).unwrap();
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "localhost");
    config.set("database", "app");
    config.set("slot.name", "laminar_app");
    config.set("publication", "laminar_app");
    config.set("ssl.mode", "disable");

    let source = registry.create_source(&config, None).unwrap();
    let error = source.contract(&config).unwrap_err();
    assert!(error.to_string().contains("raw JSON change envelope"));

    let mut removed = config.clone();
    removed.set("snapshot.mode", "initial");
    let error = source.contract(&removed).unwrap_err();
    assert!(error.to_string().contains("snapshot.mode"));

    let error = registry
        .create_table_source(&config, std::sync::Arc::new(arrow_schema::Schema::empty()))
        .err()
        .expect("CDC polling cannot determine snapshot completion");
    assert!(error.to_string().contains("snapshot-capable table source"));
}

#[cfg(feature = "mongodb-cdc")]
#[test]
fn mongodb_cdc_admission_uses_runtime_options_and_rejects_removed_ones() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::mongodb::register_mongodb_cdc_source(&registry).unwrap();
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://localhost:27017");
    config.set("database", "app");
    config.set("collection", "events");
    config.set("max.buffered.bytes", "33554432");

    let source = registry.create_source(&config, None).unwrap();
    let error = source.contract(&config).unwrap_err();
    assert!(error.to_string().contains("raw JSON change envelope"));

    let mut removed = config;
    removed.set("max.buffered.events", "4096");
    let error = source.contract(&removed).unwrap_err();
    assert!(error.to_string().contains("max.buffered.bytes"));
}
