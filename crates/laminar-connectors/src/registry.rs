//! Registry of connector factories, keyed by connector type string.

use std::collections::HashMap;
use std::sync::Arc;

use ::serde::Serialize;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use crate::config::{ConnectorConfig, ConnectorInfo};
use crate::connector::{SinkConnector, SourceConnector};
use crate::error::ConnectorError;
use crate::reference::ReferenceTableSource;
use crate::serde::{self, Format, RecordDeserializer, RecordSerializer};

/// Factory function type for creating source connectors.
///
/// The optional `&prometheus::Registry` allows connectors to register
/// their metrics on the shared Prometheus registry when one is available.
pub type SourceFactory =
    Arc<dyn Fn(Option<&prometheus::Registry>) -> Box<dyn SourceConnector> + Send + Sync>;

/// Factory function type for creating sink connectors.
///
/// The connector config is available during construction so factories can
/// select a concrete implementation and reject invalid mode-specific options
/// before `open()` performs external I/O.
///
/// The optional `&prometheus::Registry` allows connectors to register
/// their metrics on the shared Prometheus registry when one is available.
pub type SinkFactory = Arc<
    dyn Fn(
            &ConnectorConfig,
            Option<&prometheus::Registry>,
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
///
/// Previously this was a hand-rolled `Arc<dyn Fn(...) -> Pin<Box<Future>>>`
/// type alias that nobody could read. A trait with an `async` method
/// says the same thing without forcing the caller to spell out the
/// `Pin<Box<...>>`.
#[async_trait]
pub trait LookupSourceFactory: Send + Sync {
    /// Build a lookup source instance from the given config.
    ///
    /// `declared_schema` is the table's declared Arrow schema (from the
    /// `CREATE LOOKUP TABLE` columns), when known. Schema-bearing sources
    /// (Delta/Iceberg/Postgres) derive their own and ignore it; schemaless
    /// sources (`MongoDB`) need it to project documents into typed columns.
    async fn build(
        &self,
        config: ConnectorConfig,
        declared_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, ConnectorError>;
}

type LookupSourceRegistration = (ConnectorInfo, Arc<dyn LookupSourceFactory>);

/// Stable connector-configuration field included in a frozen registry descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectorConfigKeyDescriptor {
    /// Property name.
    pub key: String,
    /// Whether construction requires the property when no default is present.
    pub required: bool,
    /// Declared default value.
    pub default: Option<String>,
}

/// Stable description of one registered connector factory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectorFactoryDescriptor {
    /// Registry category.
    pub kind: &'static str,
    /// Name used for runtime lookup.
    pub registered_name: String,
    /// Connector implementation name declared by its metadata.
    pub implementation_name: String,
    /// Connector implementation version declared by its metadata.
    pub implementation_version: String,
    /// Whether the implementation declares source capability.
    pub is_source: bool,
    /// Whether the implementation declares sink capability.
    pub is_sink: bool,
    /// Accepted configuration schema, sorted by property name.
    pub config_keys: Vec<ConnectorConfigKeyDescriptor>,
}

/// Canonical, factory-pointer-free description of a frozen connector registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FrozenConnectorRegistryDescriptor {
    /// Descriptor wire/schema version.
    pub version: u32,
    /// Registered factories sorted by category and lookup name.
    pub factories: Vec<ConnectorFactoryDescriptor>,
}

/// Registry of available connector implementations. Connectors register
/// a factory per type string; the runtime looks up by the `connector`
/// property in `CREATE SOURCE/SINK` DDL.
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

    /// Returns a deterministic description of this registry after it has been frozen.
    ///
    /// Factory pointers and display text are intentionally excluded. The descriptor captures
    /// only lookup/category identity and construction-relevant connector metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectorError::InvalidState`] while registration is still open.
    pub fn frozen_descriptor(&self) -> Result<FrozenConnectorRegistryDescriptor, ConnectorError> {
        let frozen = self.frozen.read();
        if !*frozen {
            return Err(ConnectorError::InvalidState {
                expected: "frozen connector registry".into(),
                actual: "registration open".into(),
            });
        }

        let mut factories = Vec::new();
        factories.extend(
            self.sources
                .read()
                .iter()
                .map(|(name, (info, _))| connector_descriptor("source", name, info)),
        );
        factories.extend(
            self.sinks
                .read()
                .iter()
                .map(|(name, (info, _))| connector_descriptor("sink", name, info)),
        );
        factories.extend(
            self.table_sources
                .read()
                .iter()
                .map(|(name, (info, _))| connector_descriptor("table source", name, info)),
        );
        factories.extend(
            self.lookup_sources
                .read()
                .iter()
                .map(|(name, (info, _))| connector_descriptor("lookup source", name, info)),
        );
        factories.sort_unstable_by(|left, right| {
            (left.kind, left.registered_name.as_str())
                .cmp(&(right.kind, right.registered_name.as_str()))
        });
        drop(frozen);
        Ok(FrozenConnectorRegistryDescriptor {
            version: 1,
            factories,
        })
    }

    /// Returns the lowercase SHA-256 digest of the canonical frozen descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry is not frozen or descriptor serialization fails.
    pub fn frozen_fingerprint(&self) -> Result<String, ConnectorError> {
        let descriptor = self.frozen_descriptor()?;
        let bytes = serde_json::to_vec(&descriptor).map_err(|error| {
            ConnectorError::Internal(format!(
                "failed to serialize frozen connector registry descriptor: {error}"
            ))
        })?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
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
    /// - `Ok(None)` — connector type is unknown OR the connector chose
    ///   not to discover (e.g. non-Avro Kafka format, missing SR url).
    /// - `Err(_)` — discovery was attempted and failed with a specific
    ///   cause; callers should surface the message verbatim.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`ConnectorError`] from
    /// [`SourceConnector::discover_schema`] when discovery fails (bad
    /// config, unreachable network endpoint, timeout, etc.).
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

        let mut instance = factory(None);
        instance.discover_schema(properties).await?;
        let schema = instance.schema();
        Ok((!schema.fields().is_empty()).then_some(schema))
    }

    /// Creates a new source connector instance.
    ///
    /// The factory creates a default-configured connector. The caller must
    /// subsequently call `open(config)` to forward WITH clause properties.
    ///
    /// If a `prometheus::Registry` is provided, the connector will register
    /// its metrics on it so they appear in the scrape output.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if not registered.
    pub fn create_source(
        &self,
        config: &ConnectorConfig,
        registry: Option<&prometheus::Registry>,
    ) -> Result<Box<dyn SourceConnector>, ConnectorError> {
        let sources = self.sources.read();
        let (_, factory) = sources.get(config.connector_type()).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "unknown source connector type: '{}'",
                config.connector_type()
            ))
        })?;
        Ok(factory(registry))
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
        registry: Option<&prometheus::Registry>,
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
                Arc::new(|_: Option<&prometheus::Registry>| Box::new(MockSourceConnector::new())),
            )
            .unwrap();

        let config = ConnectorConfig::new("mock");
        let connector = registry.create_source(&config, None);
        assert!(connector.is_ok());
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
        assert!(registry.create_sink(&config, None).is_err());
    }

    #[test]
    fn test_list_connectors() {
        let registry = ConnectorRegistry::new();
        registry
            .register_source(
                "kafka",
                mock_info("kafka", true, false),
                Arc::new(|_: Option<&prometheus::Registry>| Box::new(MockSourceConnector::new())),
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
                Arc::new(|_: Option<&prometheus::Registry>| Box::new(MockSourceConnector::new())),
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
                Arc::new(|_: Option<&prometheus::Registry>| Box::new(MockSourceConnector::new())),
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
            Arc::new(|_: Option<&prometheus::Registry>| {
                Box::new(MockSourceConnector::new()) as Box<dyn SourceConnector>
            }) as SourceFactory
        };
        let sink = || {
            Arc::new(
                |_config: &ConnectorConfig, _registry: Option<&prometheus::Registry>| {
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
                Arc::new(|_: Option<&prometheus::Registry>| {
                    Box::new(MockSourceConnector::new())
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

    #[test]
    fn frozen_fingerprint_is_independent_of_registration_and_config_key_order() {
        use crate::config::ConfigKeySpec;

        fn info(keys: Vec<ConfigKeySpec>) -> ConnectorInfo {
            ConnectorInfo {
                name: "implementation".into(),
                display_name: "display text is not deployment identity".into(),
                version: "7.2.1".into(),
                is_source: true,
                is_sink: false,
                config_keys: keys,
            }
        }
        let keys = || {
            vec![
                ConfigKeySpec::optional("z", "z field", "default"),
                ConfigKeySpec::required("a", "a field"),
            ]
        };
        let source = || {
            Arc::new(|_: Option<&prometheus::Registry>| {
                Box::new(MockSourceConnector::new()) as Box<dyn SourceConnector>
            }) as SourceFactory
        };

        let first = ConnectorRegistry::new();
        first.register_source("b", info(keys()), source()).unwrap();
        first
            .register_source("a", info(keys().into_iter().rev().collect()), source())
            .unwrap();
        assert!(first.frozen_descriptor().is_err());
        first.freeze();

        let second = ConnectorRegistry::new();
        second.register_source("a", info(keys()), source()).unwrap();
        second
            .register_source("b", info(keys().into_iter().rev().collect()), source())
            .unwrap();
        second.freeze();

        assert_eq!(
            first.frozen_descriptor().unwrap(),
            second.frozen_descriptor().unwrap()
        );
        assert_eq!(
            first.frozen_fingerprint().unwrap(),
            second.frozen_fingerprint().unwrap()
        );
    }

    #[test]
    fn frozen_descriptor_redacts_secret_defaults_without_losing_endpoint_identity() {
        use crate::config::ConfigKeySpec;

        let registry = ConnectorRegistry::new();
        let mut info = mock_info("secure", true, false);
        info.config_keys = vec![
            ConfigKeySpec::optional("password", "credential", "literal-password"),
            ConfigKeySpec::optional(
                "endpoint",
                "service URI",
                "https://user:pass@api.example/v1?region=eu&sig=signed-secret",
            ),
            ConfigKeySpec::optional("batch.size", "batch size", "128"),
        ];
        registry
            .register_source(
                "secure",
                info,
                Arc::new(|_: Option<&prometheus::Registry>| Box::new(MockSourceConnector::new())),
            )
            .unwrap();
        registry.freeze();

        let encoded = serde_json::to_string(&registry.frozen_descriptor().unwrap()).unwrap();
        assert!(!encoded.contains("literal-password"));
        assert!(!encoded.contains("signed-secret"));
        assert!(!encoded.contains("user:pass"));
        assert!(encoded.contains("api.example"));
        assert!(encoded.contains("region=eu"));
        assert!(encoded.contains("128"));
        assert!(encoded.contains("<redacted>"));
    }
}

fn connector_descriptor(
    kind: &'static str,
    registered_name: &str,
    info: &ConnectorInfo,
) -> ConnectorFactoryDescriptor {
    let mut config_keys = info
        .config_keys
        .iter()
        .map(|spec| ConnectorConfigKeyDescriptor {
            key: spec.key.clone(),
            required: spec.required,
            default: spec
                .default
                .as_deref()
                .map(|value| crate::security::sanitize_identity_value(&spec.key, value)),
        })
        .collect::<Vec<_>>();
    config_keys.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    ConnectorFactoryDescriptor {
        kind,
        registered_name: registered_name.to_owned(),
        implementation_name: info.name.clone(),
        implementation_version: info.version.clone(),
        is_source: info.is_source,
        is_sink: info.is_sink,
        config_keys,
    }
}
