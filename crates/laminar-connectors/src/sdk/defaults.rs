//! Default connector registration.
//!
//! Provides convenience functions to register all built-in connectors with
//! a registry.

use std::sync::Arc;

use crate::config::ConnectorInfo;
use crate::registry::ConnectorRegistry;
use crate::testing::{MockSinkConnector, MockSourceConnector};

/// Registers all built-in connectors with the registry.
///
/// This registers connectors that are available in the current build based
/// on enabled features:
///
/// - Mock (always available, for testing)
/// - Kafka (with `kafka` feature)
/// - `PostgreSQL` CDC (with `postgres-cdc` feature)
/// - `PostgreSQL` Sink (with `postgres-sink` feature)
/// - `MySQL` CDC (with `mysql-cdc` feature)
/// - Delta Lake (always available, business logic only)
/// - Iceberg (always available, business logic only)
pub fn register_all_connectors(registry: &ConnectorRegistry) {
    // Always register mock connectors
    register_mock_connectors(registry);

    // Register Delta Lake sink (business logic only, no external dependency)
    register_delta_lake(registry);

    // Register Iceberg sink (business logic only, no external dependency)
    register_iceberg(registry);

    // Feature-gated connectors
    #[cfg(feature = "kafka")]
    register_kafka(registry);

    #[cfg(feature = "postgres-cdc")]
    register_postgres_cdc(registry);

    #[cfg(feature = "postgres-sink")]
    register_postgres_sink(registry);

    #[cfg(feature = "mysql-cdc")]
    register_mysql_cdc(registry);
}

/// Registers mock connectors for testing.
pub fn register_mock_connectors(registry: &ConnectorRegistry) {
    registry.register_source(
        "mock",
        ConnectorInfo {
            name: "mock".to_string(),
            display_name: "Mock Source".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            is_source: true,
            is_sink: false,
            config_keys: vec![],
        },
        Arc::new(|| Box::new(MockSourceConnector::new())),
    );

    registry.register_sink(
        "mock",
        ConnectorInfo {
            name: "mock".to_string(),
            display_name: "Mock Sink".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            is_source: false,
            is_sink: true,
            config_keys: vec![],
        },
        Arc::new(|| Box::new(MockSinkConnector::new())),
    );
}

fn register_delta_lake(registry: &ConnectorRegistry) {
    use crate::lakehouse::register_delta_lake_sink;
    register_delta_lake_sink(registry);
}

fn register_iceberg(registry: &ConnectorRegistry) {
    use crate::lakehouse::register_iceberg_sink;
    register_iceberg_sink(registry);
}

#[cfg(feature = "kafka")]
fn register_kafka(registry: &ConnectorRegistry) {
    use crate::kafka::{register_kafka_sink, register_kafka_source};
    register_kafka_source(registry);
    register_kafka_sink(registry);
}

#[cfg(feature = "postgres-cdc")]
fn register_postgres_cdc(registry: &ConnectorRegistry) {
    crate::cdc::postgres::register_postgres_cdc(registry);
}

#[cfg(feature = "postgres-sink")]
fn register_postgres_sink(registry: &ConnectorRegistry) {
    use crate::postgres::register_postgres_sink;
    register_postgres_sink(registry);
}

#[cfg(feature = "mysql-cdc")]
fn register_mysql_cdc(registry: &ConnectorRegistry) {
    use crate::cdc::mysql::register_mysql_cdc_source;
    register_mysql_cdc_source(registry);
}

/// Returns a registry with all default connectors pre-registered.
#[must_use]
pub fn default_registry() -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    register_all_connectors(&registry);
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_all_connectors() {
        let registry = ConnectorRegistry::new();
        register_all_connectors(&registry);

        // Mock should always be available
        assert!(registry.source_info("mock").is_some());
        assert!(registry.sink_info("mock").is_some());

        // Delta Lake and Iceberg sinks should be available
        assert!(registry.sink_info("delta-lake").is_some());
        assert!(registry.sink_info("iceberg").is_some());
    }

    #[test]
    fn test_default_registry() {
        let registry = default_registry();

        assert!(!registry.list_sources().is_empty());
        assert!(!registry.list_sinks().is_empty());
    }

    #[test]
    fn test_mock_connectors_registered() {
        let registry = ConnectorRegistry::new();
        register_mock_connectors(&registry);

        let source_info = registry.source_info("mock").unwrap();
        assert_eq!(source_info.name, "mock");
        assert!(source_info.is_source);

        let sink_info = registry.sink_info("mock").unwrap();
        assert_eq!(sink_info.name, "mock");
        assert!(sink_info.is_sink);
    }

    #[test]
    fn test_can_create_from_default_registry() {
        let registry = default_registry();
        let config = crate::config::ConnectorConfig::new("mock");

        let source = registry.create_source(&config);
        assert!(source.is_ok());

        let sink = registry.create_sink(&config);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_delta_lake_registered() {
        let registry = default_registry();
        let info = registry.sink_info("delta-lake");
        assert!(info.is_some());

        let info = info.unwrap();
        assert!(info.is_sink);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_iceberg_registered() {
        let registry = default_registry();
        let info = registry.sink_info("iceberg");
        assert!(info.is_some());

        let info = info.unwrap();
        assert!(info.is_sink);
        assert!(!info.config_keys.is_empty());
    }
}
