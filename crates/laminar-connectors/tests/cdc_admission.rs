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
fn mongodb_cdc_admission_declares_history_and_document_contracts() {
    use laminar_connectors::connector::{
        SourceConsistency, SourceInputMode, SourceRowPositionCapability, SourceTopology,
    };

    let registry = ConnectorRegistry::new();
    laminar_connectors::mongodb::register_mongodb_cdc_source(&registry).unwrap();
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://localhost:27017");
    config.set("database", "app");
    config.set("collection", "events");
    config.set("max.buffered.bytes", "33554432");

    let source = registry.create_source(&config, None).unwrap();
    let history = source.contract(&config).unwrap();
    assert_eq!(history.input_mode, SourceInputMode::AppendOnly);
    assert_eq!(history.consistency, SourceConsistency::Replayable);
    assert_eq!(history.topology, SourceTopology::Singleton);
    assert!(!history.is_exact_delivery_certified());

    let mut document = config.clone();
    document.set("output.mode", "document");
    document.set("full.document.mode", "required");
    let keyed = source.contract(&document).unwrap();
    assert_eq!(keyed.input_mode, SourceInputMode::KeyedUpsert);
    assert_eq!(
        keyed.row_positions,
        SourceRowPositionCapability::OrderedDeterministic
    );

    let mut snapshot = document.clone();
    snapshot.set("snapshot.mode", "initial");
    assert_eq!(
        source.contract(&snapshot).unwrap().consistency,
        SourceConsistency::CommitCoupled
    );

    let mut delta_document = document;
    delta_document.set("full.document.mode", "delta");
    let error = source.contract(&delta_document).unwrap_err();
    assert!(error.to_string().contains("full.document.mode=required"));

    let mut removed = config;
    removed.set("max.buffered.events", "4096");
    let error = source.contract(&removed).unwrap_err();
    assert!(error.to_string().contains("max.buffered.bytes"));
}
