//! Connector registry with factory pattern.
//!
//! The `ConnectorRegistry` maintains a catalog of available connector
//! implementations and provides factory methods to instantiate them.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::config::{ConnectorConfig, ConnectorInfo};
use crate::connector::{SinkConnector, SourceConnector};
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;
use crate::serde::{self, Format, RecordDeserializer, RecordSerializer};

/// Factory function type for creating source connectors.
pub type SourceFactory = Arc<dyn Fn() -> Box<dyn SourceConnector> + Send + Sync>;

/// Factory function type for creating sink connectors.
pub type SinkFactory = Arc<dyn Fn() -> Box<dyn SinkConnector> + Send + Sync>;

/// Factory function type for creating reference table sources.
pub type TableSourceFactory = Arc<
    dyn Fn(&ConnectorConfig) -> Result<Box<dyn ReferenceTableSource>, ConnectorError> + Send + Sync,
>;

/// Registry of available connector implementations.
///
/// Connectors register themselves with a factory function that creates
/// new instances. The runtime uses the registry to instantiate connectors
/// based on the `connector` property in the configuration.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_connectors::registry::ConnectorRegistry;
///
/// let mut registry = ConnectorRegistry::new();
/// registry.register_source("kafka", info, Arc::new(|| Box::new(KafkaSource::new())));
///
/// let connector = registry.create_source("kafka", &config)?;
/// ```
#[derive(Clone)]
pub struct ConnectorRegistry {
    sources: Arc<RwLock<HashMap<String, (ConnectorInfo, SourceFactory)>>>,
    sinks: Arc<RwLock<HashMap<String, (ConnectorInfo, SinkFactory)>>>,
    table_sources: Arc<RwLock<HashMap<String, (ConnectorInfo, TableSourceFactory)>>>,
}

impl ConnectorRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: Arc::new(RwLock::new(HashMap::new())),
            sinks: Arc::new(RwLock::new(HashMap::new())),
            table_sources: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registers a source connector factory.
    pub fn register_source(
        &self,
        name: impl Into<String>,
        info: ConnectorInfo,
        factory: SourceFactory,
    ) {
        self.sources.write().insert(name.into(), (info, factory));
    }

    /// Registers a sink connector factory.
    pub fn register_sink(
        &self,
        name: impl Into<String>,
        info: ConnectorInfo,
        factory: SinkFactory,
    ) {
        self.sinks.write().insert(name.into(), (info, factory));
    }

    /// Creates a new source connector instance.
    ///
    /// The connector type is determined by `config.connector_type()`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the connector type
    /// is not registered.
    pub fn create_source(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SourceConnector>, ConnectorError> {
        let sources = self.sources.read();
        let (_, factory) = sources.get(config.connector_type()).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "unknown source connector type: '{}'",
                config.connector_type()
            ))
        })?;
        Ok(factory())
    }

    /// Creates a new sink connector instance.
    ///
    /// The connector type is determined by `config.connector_type()`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the connector type
    /// is not registered.
    pub fn create_sink(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let sinks = self.sinks.read();
        let (_, factory) = sinks.get(config.connector_type()).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "unknown sink connector type: '{}'",
                config.connector_type()
            ))
        })?;
        Ok(factory())
    }

    /// Registers a reference table source factory.
    pub fn register_table_source(
        &self,
        name: impl Into<String>,
        info: ConnectorInfo,
        factory: TableSourceFactory,
    ) {
        self.table_sources
            .write()
            .insert(name.into(), (info, factory));
    }

    /// Creates a new reference table source instance.
    ///
    /// The connector type is determined by `config.connector_type()`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the connector type
    /// is not registered as a table source.
    pub fn create_table_source(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Box<dyn ReferenceTableSource>, ConnectorError> {
        let table_sources = self.table_sources.read();
        let (_, factory) = table_sources.get(config.connector_type()).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "unknown table source connector type: '{}'",
                config.connector_type()
            ))
        })?;
        factory(config)
    }

    /// Lists all registered table source connector names.
    #[must_use]
    pub fn list_table_sources(&self) -> Vec<String> {
        self.table_sources.read().keys().cloned().collect()
    }

    /// Returns information about a registered source connector.
    #[must_use]
    pub fn source_info(&self, name: &str) -> Option<ConnectorInfo> {
        self.sources.read().get(name).map(|(info, _)| info.clone())
    }

    /// Returns information about a registered sink connector.
    #[must_use]
    pub fn sink_info(&self, name: &str) -> Option<ConnectorInfo> {
        self.sinks.read().get(name).map(|(info, _)| info.clone())
    }

    /// Lists all registered source connector names.
    #[must_use]
    pub fn list_sources(&self) -> Vec<String> {
        self.sources.read().keys().cloned().collect()
    }

    /// Lists all registered sink connector names.
    #[must_use]
    pub fn list_sinks(&self) -> Vec<String> {
        self.sinks.read().keys().cloned().collect()
    }

    /// Creates a deserializer for the given format string.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::Serde` if the format is not supported.
    pub fn create_deserializer(
        &self,
        format: &str,
    ) -> Result<Box<dyn RecordDeserializer>, ConnectorError> {
        let fmt = Format::parse(format).map_err(ConnectorError::Serde)?;
        serde::create_deserializer(fmt).map_err(ConnectorError::Serde)
    }

    /// Creates a serializer for the given format string.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::Serde` if the format is not supported.
    pub fn create_serializer(
        &self,
        format: &str,
    ) -> Result<Box<dyn RecordSerializer>, ConnectorError> {
        let fmt = Format::parse(format).map_err(ConnectorError::Serde)?;
        serde::create_serializer(fmt).map_err(ConnectorError::Serde)
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorRegistry")
            .field("sources", &self.list_sources())
            .field("sinks", &self.list_sinks())
            .field("table_sources", &self.list_table_sources())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;

    fn mock_info(name: &str, is_source: bool, is_sink: bool) -> ConnectorInfo {
        ConnectorInfo {
            name: name.to_string(),
            display_name: name.to_string(),
            version: "0.1.0".to_string(),
            is_source,
            is_sink,
            config_keys: vec![],
        }
    }

    #[test]
    fn test_register_and_create_source() {
        let registry = ConnectorRegistry::new();
        registry.register_source(
            "mock",
            mock_info("mock", true, false),
            Arc::new(|| Box::new(MockSourceConnector::new())),
        );

        let config = ConnectorConfig::new("mock");
        let connector = registry.create_source(&config);
        assert!(connector.is_ok());
    }

    #[test]
    fn test_register_and_create_sink() {
        let registry = ConnectorRegistry::new();
        registry.register_sink(
            "mock",
            mock_info("mock", false, true),
            Arc::new(|| Box::new(MockSinkConnector::new())),
        );

        let config = ConnectorConfig::new("mock");
        let connector = registry.create_sink(&config);
        assert!(connector.is_ok());
    }

    #[test]
    fn test_create_unknown_connector() {
        let registry = ConnectorRegistry::new();
        let config = ConnectorConfig::new("nonexistent");

        assert!(registry.create_source(&config).is_err());
        assert!(registry.create_sink(&config).is_err());
    }

    #[test]
    fn test_list_connectors() {
        let registry = ConnectorRegistry::new();
        registry.register_source(
            "kafka",
            mock_info("kafka", true, false),
            Arc::new(|| Box::new(MockSourceConnector::new())),
        );
        registry.register_sink(
            "delta",
            mock_info("delta", false, true),
            Arc::new(|| Box::new(MockSinkConnector::new())),
        );

        let sources = registry.list_sources();
        assert_eq!(sources.len(), 1);
        assert!(sources.contains(&"kafka".to_string()));

        let sinks = registry.list_sinks();
        assert_eq!(sinks.len(), 1);
        assert!(sinks.contains(&"delta".to_string()));
    }

    #[test]
    fn test_connector_info() {
        let registry = ConnectorRegistry::new();
        registry.register_source(
            "kafka",
            mock_info("kafka", true, false),
            Arc::new(|| Box::new(MockSourceConnector::new())),
        );

        let info = registry.source_info("kafka");
        assert!(info.is_some());
        assert_eq!(info.unwrap().name, "kafka");

        assert!(registry.source_info("nonexistent").is_none());
    }

    #[test]
    fn test_format_registry() {
        let registry = ConnectorRegistry::new();

        assert!(registry.create_deserializer("json").is_ok());
        assert!(registry.create_serializer("csv").is_ok());
        assert!(registry.create_deserializer("unknown").is_err());
    }

    // ── Table source factory tests ──

    #[test]
    fn test_register_and_create_table_source() {
        use crate::reference::MockReferenceTableSource;

        let registry = ConnectorRegistry::new();
        registry.register_table_source(
            "mock",
            mock_info("mock", true, false),
            Arc::new(|_config| Ok(Box::new(MockReferenceTableSource::empty()))),
        );

        let config = ConnectorConfig::new("mock");
        let source = registry.create_table_source(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_create_unknown_table_source() {
        let registry = ConnectorRegistry::new();
        let config = ConnectorConfig::new("nonexistent");
        let result = registry.create_table_source(&config);
        match result {
            Err(e) => assert!(e.to_string().contains("unknown table source"), "got: {e}"),
            Ok(_) => panic!("Expected error for unknown table source"),
        }
    }

    #[test]
    fn test_list_table_sources() {
        use crate::reference::MockReferenceTableSource;

        let registry = ConnectorRegistry::new();
        assert!(registry.list_table_sources().is_empty());

        registry.register_table_source(
            "mock-table",
            mock_info("mock-table", true, false),
            Arc::new(|_config| Ok(Box::new(MockReferenceTableSource::empty()))),
        );

        let names = registry.list_table_sources();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"mock-table".to_string()));
    }
}
