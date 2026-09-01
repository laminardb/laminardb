//! Registry of connector factories, keyed by connector type string.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use parking_lot::RwLock;

use crate::config::{ConnectorConfig, ConnectorInfo};
use crate::connector::{SinkConnector, SourceConnector};
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;
use crate::serde::{self, Format, RecordDeserializer, RecordSerializer};

/// Factory function type for creating source connectors.
///
/// The optional shared Prometheus registry allows connectors to register
/// metrics and retain stable registry identity when one is available.
/// Construction failures, including metrics registration errors, are returned
/// to the caller.
pub type SourceFactory = Arc<
    dyn Fn(Option<&Arc<prometheus::Registry>>) -> Result<Box<dyn SourceConnector>, ConnectorError>
        + Send
        + Sync,
>;

/// Factory function type for creating sink connectors.
///
/// The connector config is available during construction so factories can
/// select a concrete implementation and reject invalid mode-specific options
/// before `open()` performs external I/O.
///
/// The optional shared Prometheus registry allows connectors to register
/// metrics and retain stable registry identity when one is available.
pub type SinkFactory = Arc<
    dyn Fn(
            &ConnectorConfig,
            Option<&Arc<prometheus::Registry>>,
        ) -> Result<Box<dyn SinkConnector>, ConnectorError>
        + Send
        + Sync,
>;

/// Factory for finite reference-table snapshots constrained by the declared schema.
pub type TableSourceFactory = Arc<
    dyn Fn(&ConnectorConfig, SchemaRef) -> Result<Box<dyn ReferenceTableSource>, ConnectorError>
        + Send
        + Sync,
>;

/// Factory for constructing a lookup source (async, for on-demand mode).
#[async_trait]
pub trait LookupSourceFactory: Send + Sync {
    /// Build a lookup source instance from the given config.
    ///
    /// `declared_schema` is the table's declared Arrow schema, when known.
    /// Schema-bearing sources may derive their own; schemaless sources use it
    /// to project records into typed columns.
    async fn build(
        &self,
        config: ConnectorConfig,
        declared_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, ConnectorError>;
}

type LookupSourceRegistration = (ConnectorInfo, Arc<dyn LookupSourceFactory>);

/// Registry of available connector implementations. Connectors register
/// a factory per type string; the runtime resolves the connector named by
/// `FROM` in `CREATE SOURCE` or `INTO` in `CREATE SINK`.
#[derive(Clone)]
pub struct ConnectorRegistry {
    sources: Arc<RwLock<HashMap<String, (ConnectorInfo, SourceFactory)>>>,
    sinks: Arc<RwLock<HashMap<String, (ConnectorInfo, SinkFactory)>>>,
    table_sources: Arc<RwLock<HashMap<String, (ConnectorInfo, TableSourceFactory)>>>,
    lookup_sources: Arc<RwLock<HashMap<String, LookupSourceRegistration>>>,
    /// A registration holds a read guard through insertion; `freeze` takes the write guard.
    /// This makes the freeze boundary linearizable with concurrent registration attempts.
    frozen: Arc<RwLock<bool>>,
}

impl ConnectorRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sources: Arc::new(RwLock::new(HashMap::new())),
            sinks: Arc::new(RwLock::new(HashMap::new())),
            table_sources: Arc::new(RwLock::new(HashMap::new())),
            lookup_sources: Arc::new(RwLock::new(HashMap::new())),
            frozen: Arc::new(RwLock::new(false)),
        }
    }

    /// Permanently closes this registry to factory mutation.
    ///
    /// The transition is idempotent. Once this call returns, every registration that began
    /// before it is visible and every later registration is rejected.
    pub fn freeze(&self) {
        *self.frozen.write() = true;
    }

    /// Returns whether factory registration has been permanently disabled.
    #[must_use]
    pub fn is_frozen(&self) -> bool {
        *self.frozen.read()
    }

    /// Registers a source connector factory.
    ///
    /// # Errors
    ///
    /// Returns an error if the source name is already registered or the registry is frozen.
    pub fn register_source(
        &self,
        name: impl Into<String>,
        info: ConnectorInfo,
        factory: SourceFactory,
    ) -> Result<(), ConnectorError> {
        let name = name.into();
        let frozen = self.frozen.read();
        if *frozen {
            return Err(ConnectorError::RegistryFrozen {
                kind: "source",
                name,
            });
        }
        let mut sources = self.sources.write();
        if sources.contains_key(&name) {
            return Err(ConnectorError::FactoryAlreadyRegistered {
                kind: "source",
                name,
            });
        }
        sources.insert(name, (info, factory));
        drop(frozen);
        Ok(())
    }

    /// Registers a sink connector factory.
    ///
    /// # Errors
    ///
    /// Returns an error if the sink name is already registered or the registry is frozen.
    pub fn register_sink(
        &self,
        name: impl Into<String>,
        info: ConnectorInfo,
        factory: SinkFactory,
    ) -> Result<(), ConnectorError> {
        let name = name.into();
        let frozen = self.frozen.read();
        if *frozen {
            return Err(ConnectorError::RegistryFrozen { kind: "sink", name });
        }
        let mut sinks = self.sinks.write();
        if sinks.contains_key(&name) {
            return Err(ConnectorError::FactoryAlreadyRegistered { kind: "sink", name });
        }
        sinks.insert(name, (info, factory));
        drop(frozen);
        Ok(())
    }

    /// Run a connector's `discover_schema` against the given properties.
    ///
    /// Three outcomes:
    /// - `Ok(Some(schema))` — discovery succeeded and produced fields.
    /// - `Ok(None)` — connector type is unknown or does not support discovery.
    /// - `Err(_)` — discovery was attempted and failed with a specific
    ///   cause; callers should surface the message verbatim.
    ///
    /// # Errors
    ///
    /// Returns the source factory's construction error or the underlying
    /// [`ConnectorError`] from [`SourceConnector::discover_schema`] when
    /// discovery fails (bad config, unreachable network endpoint, timeout,
    /// etc.).
    pub async fn default_source_schema(
        &self,
        connector_type: &str,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<Option<SchemaRef>, ConnectorError> {
        let factory = {
            let sources = self.sources.read();
            let Some((_, factory)) = sources.get(connector_type) else {
                return Ok(None);
            };
            factory.clone()
        };

        let mut instance = factory(None)?;
        instance.discover_schema(properties).await?;
        let schema = instance.schema();
        Ok((!schema.fields().is_empty()).then_some(schema))
    }

    /// Creates a new source connector instance.
    ///
    /// The factory creates a default-configured connector. The caller forwards
    /// the resolved `FROM` and `FORMAT` configuration in the startup request.
    ///
    /// If a `prometheus::Registry` is provided, the connector will register
    /// its metrics on it so they appear in the scrape output.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if not registered, or the
    /// source factory's construction error.
    pub fn create_source(
        &self,
        config: &ConnectorConfig,
        registry: Option<&Arc<prometheus::Registry>>,
    ) -> Result<Box<dyn SourceConnector>, ConnectorError> {
        let sources = self.sources.read();
        let (_, factory) = sources.get(config.connector_type()).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "unknown source connector type: '{}'",
                config.connector_type()
            ))
        })?;
        factory(registry)
    }

    /// Derive a registered source connector's semantic recovery identity.
    ///
    /// This constructs an unopened source instance and invokes its pure,
    /// configuration-only identity hook. `None` requests the runtime's
    /// conservative sanitized-property fallback.
    ///
    /// # Errors
    ///
    /// Returns an error when the source type is not registered or the connector
    /// rejects the supplied semantic configuration.
    pub fn source_recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<BTreeMap<String, String>>, ConnectorError> {
        self.create_source(config, None)?
            .recovery_identity_options(config)
    }

    /// Creates a new sink connector instance.
    ///
    /// The connector type is determined by `config.connector_type()`.
    ///
    /// If a `prometheus::Registry` is provided, the connector will register
    /// its metrics on it so they appear in the scrape output.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the connector type is
    /// not registered, or the selected sink configuration is invalid.
    pub fn create_sink(
        &self,
        config: &ConnectorConfig,
        registry: Option<&Arc<prometheus::Registry>>,
    ) -> Result<Box<dyn SinkConnector>, ConnectorError> {
        let sinks = self.sinks.read();
        let (_, factory) = sinks.get(config.connector_type()).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "unknown sink connector type: '{}'",
                config.connector_type()
            ))
        })?;
        factory(config, registry)
    }

    /// Registers a reference table source factory.
    ///
    /// # Errors
    ///
    /// Returns an error if the table-source name is already registered or the registry is frozen.
    pub fn register_table_source(
        &self,
        name: impl Into<String>,
        info: ConnectorInfo,
        factory: TableSourceFactory,
    ) -> Result<(), ConnectorError> {
        let name = name.into();
        let frozen = self.frozen.read();
        if *frozen {
            return Err(ConnectorError::RegistryFrozen {
                kind: "table source",
                name,
            });
        }
        let mut table_sources = self.table_sources.write();
        if table_sources.contains_key(&name) {
            return Err(ConnectorError::FactoryAlreadyRegistered {
                kind: "table source",
                name,
            });
        }
        table_sources.insert(name, (info, factory));
        drop(frozen);
        Ok(())
    }

    /// Creates a new reference table source instance.
    ///
    /// The connector type is determined by `config.connector_type()`. The declared schema is the
    /// authoritative field order, Arrow type, and nullability boundary for every returned batch.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the connector type
    /// is not registered as a table source.
    pub fn create_table_source(
        &self,
        config: &ConnectorConfig,
        declared_schema: SchemaRef,
    ) -> Result<Box<dyn ReferenceTableSource>, ConnectorError> {
        let table_sources = self.table_sources.read();
        let (_, factory) = table_sources.get(config.connector_type()).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "connector type '{}' is not registered as a snapshot-capable table source",
                config.connector_type()
            ))
        })?;
        factory(config, declared_schema)
    }

    /// Lists all registered table source connector names.
    #[must_use]
    pub fn list_table_sources(&self) -> Vec<String> {
        let mut names: Vec<_> = self.table_sources.read().keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// Returns whether a snapshot-capable table source is registered.
    #[must_use]
    pub fn has_table_source(&self, name: &str) -> bool {
        self.table_sources.read().contains_key(name)
    }

    /// Registers a lookup source factory for on-demand/partial cache mode.
    ///
    /// # Errors
    ///
    /// Returns an error if the lookup name is already registered or the registry is frozen.
    pub fn register_lookup_source(
        &self,
        name: impl Into<String>,
        info: ConnectorInfo,
        factory: Arc<dyn LookupSourceFactory>,
    ) -> Result<(), ConnectorError> {
        let name = name.into();
        let frozen = self.frozen.read();
        if *frozen {
            return Err(ConnectorError::RegistryFrozen {
                kind: "lookup source",
                name,
            });
        }
        let mut lookup_sources = self.lookup_sources.write();
        if lookup_sources.contains_key(&name) {
            return Err(ConnectorError::FactoryAlreadyRegistered {
                kind: "lookup source",
                name,
            });
        }
        lookup_sources.insert(name, (info, factory));
        drop(frozen);
        Ok(())
    }

    /// Returns whether an on-demand lookup source is registered.
    #[must_use]
    pub fn has_lookup_source(&self, name: &str) -> bool {
        self.lookup_sources.read().contains_key(name)
    }

    /// Creates a lookup source for on-demand cache-miss fallback.
    ///
    /// Returns `None` if no lookup source factory is registered for
    /// the given connector type.
    pub async fn create_lookup_source(
        &self,
        config: ConnectorConfig,
        declared_schema: Option<arrow_schema::SchemaRef>,
    ) -> Option<Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, ConnectorError>>
    {
        let factory = {
            let lookup_sources = self.lookup_sources.read();
            Arc::clone(&lookup_sources.get(config.connector_type())?.1)
        };
        Some(factory.build(config, declared_schema).await)
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
        let mut names: Vec<_> = self.sources.read().keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// Lists all registered sink connector names.
    #[must_use]
    pub fn list_sinks(&self) -> Vec<String> {
        let mut names: Vec<_> = self.sinks.read().keys().cloned().collect();
        names.sort_unstable();
        names
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
    use crate::reference::MockReferenceTableSource;
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

    fn declared_schema() -> SchemaRef {
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]))
    }

    #[test]
    fn test_register_and_create_source() {
        let registry = ConnectorRegistry::new();
        registry
            .register_source(
                "mock",
                mock_info("mock", true, false),
                Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                    Ok(Box::new(MockSourceConnector::new()))
                }),
            )
            .unwrap();

        let config = ConnectorConfig::new("mock");
        let connector = registry.create_source(&config, None);
        assert!(connector.is_ok());
        assert_eq!(
            registry.source_recovery_identity_options(&config).unwrap(),
            None
        );
    }

    #[test]
    fn test_register_and_create_sink() {
        let registry = ConnectorRegistry::new();
        registry
            .register_sink(
                "mock",
                mock_info("mock", false, true),
                Arc::new(|_config, _registry| Ok(Box::new(MockSinkConnector::new()))),
            )
            .unwrap();

        let config = ConnectorConfig::new("mock");
        let connector = registry.create_sink(&config, None);
        assert!(connector.is_ok());
    }

    #[test]
    fn factories_receive_shared_registry_identity() {
        let connectors = ConnectorRegistry::new();
        let metrics = Arc::new(prometheus::Registry::new());

        let source_metrics = Arc::clone(&metrics);
        connectors
            .register_source(
                "identity-source",
                mock_info("identity-source", true, false),
                Arc::new(move |registry: Option<&Arc<prometheus::Registry>>| {
                    assert!(
                        registry.is_some_and(|registry| { Arc::ptr_eq(registry, &source_metrics) })
                    );
                    Ok(Box::new(MockSourceConnector::new()))
                }),
            )
            .unwrap();

        let sink_metrics = Arc::clone(&metrics);
        connectors
            .register_sink(
                "identity-sink",
                mock_info("identity-sink", false, true),
                Arc::new(
                    move |_config, registry: Option<&Arc<prometheus::Registry>>| {
                        assert!(registry
                            .is_some_and(|registry| { Arc::ptr_eq(registry, &sink_metrics) }));
                        Ok(Box::new(MockSinkConnector::new()))
                    },
                ),
            )
            .unwrap();

        connectors
            .create_source(&ConnectorConfig::new("identity-source"), Some(&metrics))
            .expect("source factory must receive the shared registry Arc");
        connectors
            .create_sink(&ConnectorConfig::new("identity-sink"), Some(&metrics))
            .expect("sink factory must receive the shared registry Arc");
    }

    struct RejectLookupFactory;

    #[async_trait]
    impl LookupSourceFactory for RejectLookupFactory {
        async fn build(
            &self,
            _config: ConnectorConfig,
            _declared_schema: Option<SchemaRef>,
        ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, ConnectorError>
        {
            Err(ConnectorError::ConfigurationError(
                "test lookup factory has no backing source".into(),
            ))
        }
    }

    #[test]
    fn sink_factory_receives_config_and_propagates_validation_errors() {
        let registry = ConnectorRegistry::new();
        registry
            .register_sink(
                "validated",
                mock_info("validated", false, true),
                Arc::new(|config, _registry| {
                    if config.get("enabled") == Some("true") {
                        Ok(Box::new(MockSinkConnector::new()))
                    } else {
                        Err(ConnectorError::ConfigurationError(
                            "validated sink requires enabled = true".into(),
                        ))
                    }
                }),
            )
            .unwrap();

        let mut config = ConnectorConfig::new("validated");
        let error = match registry.create_sink(&config, None) {
            Ok(_) => panic!("expected factory validation error"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("enabled = true"));

        config.set("enabled", "true");
        assert!(registry.create_sink(&config, None).is_ok());
    }

    #[test]
    fn test_create_unknown_connector() {
        let registry = ConnectorRegistry::new();
        let config = ConnectorConfig::new("nonexistent");

        assert!(registry.create_source(&config, None).is_err());
        assert!(registry.source_recovery_identity_options(&config).is_err());
        assert!(registry.create_sink(&config, None).is_err());
    }

    #[test]
    fn test_list_connectors() {
        let registry = ConnectorRegistry::new();
        registry
            .register_source(
                "kafka",
                mock_info("kafka", true, false),
                Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                    Ok(Box::new(MockSourceConnector::new()))
                }),
            )
            .unwrap();
        registry
            .register_sink(
                "delta",
                mock_info("delta", false, true),
                Arc::new(|_config, _registry| Ok(Box::new(MockSinkConnector::new()))),
            )
            .unwrap();

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
        registry
            .register_source(
                "kafka",
                mock_info("kafka", true, false),
                Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                    Ok(Box::new(MockSourceConnector::new()))
                }),
            )
            .unwrap();

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

    #[tokio::test]
    async fn default_source_schema_some_when_discovered() {
        let registry = ConnectorRegistry::new();
        registry
            .register_source(
                "mock",
                mock_info("mock", true, false),
                Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                    Ok(Box::new(MockSourceConnector::new()))
                }),
            )
            .unwrap();
        let schema = registry
            .default_source_schema("mock", &std::collections::HashMap::new())
            .await
            .expect("discovery must not fail");
        assert!(schema.is_some_and(|s| !s.fields().is_empty()));
    }

    #[tokio::test]
    async fn default_source_schema_none_for_unknown_connector() {
        let registry = ConnectorRegistry::new();
        assert!(registry
            .default_source_schema("nope", &std::collections::HashMap::new())
            .await
            .expect("unknown connector is Ok(None), not Err")
            .is_none());
    }

    #[tokio::test]
    async fn source_factory_errors_propagate_from_creation_and_discovery() {
        let registry = ConnectorRegistry::new();
        registry
            .register_source(
                "failing",
                mock_info("failing", true, false),
                Arc::new(
                    |_: Option<&Arc<prometheus::Registry>>| -> Result<
                        Box<dyn SourceConnector>,
                        ConnectorError,
                    > {
                        Err(ConnectorError::Internal("source construction failed".into()))
                    },
                ),
            )
            .unwrap();

        let config = ConnectorConfig::new("failing");
        let Err(create_error) = registry.create_source(&config, None) else {
            panic!("source construction must fail");
        };
        assert!(create_error
            .to_string()
            .contains("source construction failed"));

        let discovery_error = registry
            .default_source_schema("failing", &std::collections::HashMap::new())
            .await
            .expect_err("schema discovery must propagate source construction failure");
        assert!(discovery_error
            .to_string()
            .contains("source construction failed"));
    }

    // ── Table source factory tests ──

    #[test]
    fn test_register_and_create_table_source() {
        use crate::reference::MockReferenceTableSource;

        let registry = ConnectorRegistry::new();
        let observed_schema = Arc::new(parking_lot::Mutex::new(None));
        let factory_schema = Arc::clone(&observed_schema);
        registry
            .register_table_source(
                "mock",
                mock_info("mock", true, false),
                Arc::new(move |_config, declared_schema| {
                    *factory_schema.lock() = Some(declared_schema);
                    Ok(Box::new(MockReferenceTableSource::empty()))
                }),
            )
            .unwrap();

        let config = ConnectorConfig::new("mock");
        let declared_schema = declared_schema();
        let source = registry.create_table_source(&config, Arc::clone(&declared_schema));
        assert!(source.is_ok());
        assert_eq!(observed_schema.lock().as_ref(), Some(&declared_schema));
        assert!(registry.has_table_source("mock"));
        assert!(!registry.has_table_source("missing"));
    }

    #[test]
    fn test_create_unknown_table_source() {
        let registry = ConnectorRegistry::new();
        let config = ConnectorConfig::new("nonexistent");
        let result = registry.create_table_source(&config, declared_schema());
        match result {
            Err(e) => assert!(
                e.to_string().contains("snapshot-capable table source"),
                "got: {e}"
            ),
            Ok(_) => panic!("Expected error for unknown table source"),
        }
    }

    #[test]
    fn test_list_table_sources() {
        let registry = ConnectorRegistry::new();
        assert!(registry.list_table_sources().is_empty());

        registry
            .register_table_source(
                "mock-table",
                mock_info("mock-table", true, false),
                Arc::new(|_config, _declared_schema| {
                    Ok(Box::new(MockReferenceTableSource::empty()))
                }),
            )
            .unwrap();

        let names = registry.list_table_sources();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"mock-table".to_string()));
    }

    #[test]
    fn duplicate_registration_is_rejected_in_every_category() {
        let registry = ConnectorRegistry::new();
        let source = || {
            Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                Ok(Box::new(MockSourceConnector::new()) as Box<dyn SourceConnector>)
            }) as SourceFactory
        };
        let sink = || {
            Arc::new(
                |_config: &ConnectorConfig, _registry: Option<&Arc<prometheus::Registry>>| {
                    Ok(Box::new(MockSinkConnector::new()) as Box<dyn SinkConnector>)
                },
            ) as SinkFactory
        };
        let table = || {
            Arc::new(|_config: &ConnectorConfig, _declared_schema: SchemaRef| {
                Ok(Box::new(MockReferenceTableSource::empty()) as Box<dyn ReferenceTableSource>)
            }) as TableSourceFactory
        };

        registry
            .register_source("same", mock_info("same", true, false), source())
            .unwrap();
        assert!(matches!(
            registry.register_source("same", mock_info("same", true, false), source()),
            Err(ConnectorError::FactoryAlreadyRegistered { kind: "source", .. })
        ));

        registry
            .register_sink("same", mock_info("same", false, true), sink())
            .unwrap();
        assert!(matches!(
            registry.register_sink("same", mock_info("same", false, true), sink()),
            Err(ConnectorError::FactoryAlreadyRegistered { kind: "sink", .. })
        ));

        registry
            .register_table_source("same", mock_info("same", true, false), table())
            .unwrap();
        assert!(matches!(
            registry.register_table_source("same", mock_info("same", true, false), table()),
            Err(ConnectorError::FactoryAlreadyRegistered {
                kind: "table source",
                ..
            })
        ));

        registry
            .register_lookup_source(
                "same",
                mock_info("same", true, false),
                Arc::new(RejectLookupFactory),
            )
            .unwrap();
        assert!(matches!(
            registry.register_lookup_source(
                "same",
                mock_info("same", true, false),
                Arc::new(RejectLookupFactory)
            ),
            Err(ConnectorError::FactoryAlreadyRegistered {
                kind: "lookup source",
                ..
            })
        ));
    }

    #[test]
    fn freeze_rejects_every_registration_category() {
        let registry = ConnectorRegistry::new();
        registry.freeze();
        registry.freeze();
        assert!(registry.is_frozen());

        assert!(matches!(
            registry.register_source(
                "late-source",
                mock_info("late-source", true, false),
                Arc::new(|_: Option<&Arc<prometheus::Registry>>| {
                    Ok(Box::new(MockSourceConnector::new()))
                })
            ),
            Err(ConnectorError::RegistryFrozen { kind: "source", .. })
        ));
        assert!(matches!(
            registry.register_sink(
                "late-sink",
                mock_info("late-sink", false, true),
                Arc::new(|_config, _registry| Ok(Box::new(MockSinkConnector::new())))
            ),
            Err(ConnectorError::RegistryFrozen { kind: "sink", .. })
        ));
        assert!(matches!(
            registry.register_table_source(
                "late-table",
                mock_info("late-table", true, false),
                Arc::new(|_config, _declared_schema| {
                    Ok(Box::new(MockReferenceTableSource::empty()))
                })
            ),
            Err(ConnectorError::RegistryFrozen {
                kind: "table source",
                ..
            })
        ));
        assert!(matches!(
            registry.register_lookup_source(
                "late-lookup",
                mock_info("late-lookup", true, false),
                Arc::new(RejectLookupFactory)
            ),
            Err(ConnectorError::RegistryFrozen {
                kind: "lookup source",
                ..
            })
        ));
    }
}
