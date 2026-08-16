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
mod tests;
