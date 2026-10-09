//! `MongoDB` CDC source and sink connectors.

pub mod change_event;
pub mod config;
pub mod lookup;
pub mod metrics;
mod schema_metadata;
pub mod sink;
pub mod source;
pub mod timeseries;
pub mod write_model;

// Re-export primary types at module level.
pub use config::{
    FullDocumentMode, MongoDbSinkConfig, MongoDbSourceConfig, SnapshotMode, SourceOutputMode,
};
pub use sink::MongoDbSink;
pub use source::{
    mongodb_history_schema, MongoDbCdcSource, MONGODB_HISTORY_VERSION, SNAPSHOT_OPERATION,
};
pub use timeseries::{CollectionKind, TimeSeriesConfig, TimeSeriesGranularity};
pub use write_model::WriteMode;

const MONGODB_LOOKUP_PROPERTIES: &[&str] = &[
    "connection.uri",
    "database",
    "collection",
    "laminar.source.name",
    "_arrow_schema",
    "_primary_key_columns",
];

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the `MongoDB` CDC source connector with the given registry.
///
/// # Errors
///
/// Returns an error if the connector name is already registered or the registry is frozen.
pub fn register_mongodb_cdc_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
        schema_capabilities: crate::schema::resolution::SchemaCapabilities::metadata(
            &[],
            false,
            crate::schema::resolution::SchemaPreparation::None,
        ),
        name: "mongodb-cdc".to_string(),
        display_name: "MongoDB CDC Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: mongodb_cdc_config_keys(),
    };

    registry.register_source(
        "mongodb-cdc",
        info,
        Arc::new(|registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(MongoDbCdcSource::new(
                MongoDbSourceConfig::default(),
                registry.map(Arc::as_ref),
            )))
        }),
    )?;

    // On-demand (partial cache mode) lookup source: find({ pk: { $in: [...] } }).
    registry.register_lookup_source(
        "mongodb",
        ConnectorInfo {
            schema_capabilities: crate::schema::resolution::SchemaCapabilities::metadata(
                &[],
                false,
                crate::schema::resolution::SchemaPreparation::None,
            ),
            name: "mongodb".to_string(),
            display_name: "MongoDB Lookup Source".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            is_source: true,
            is_sink: false,
            config_keys: mongodb_lookup_config_keys(),
        },
        Arc::new(MongoLookupFactory),
    )
}

struct MongoLookupFactory;

#[async_trait::async_trait]
impl crate::registry::LookupSourceFactory for MongoLookupFactory {
    async fn resolve_schema(
        &self,
        config: &crate::config::ConnectorConfig,
        explicit: Option<arrow_schema::SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, crate::error::ConnectorError> {
        config.reject_unknown_properties(MONGODB_LOOKUP_PROPERTIES, "MongoDB lookup")?;
        lookup::schema_resolution::resolve(config, explicit).await
    }

    async fn build(
        &self,
        config: crate::config::ConnectorConfig,
        declared_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, crate::error::ConnectorError>
    {
        use crate::mongodb::lookup::{MongoLookupSource, MongoLookupSourceConfig};

        let schema = declared_schema.ok_or_else(|| {
            crate::error::ConnectorError::ConfigurationError(
                "mongodb lookup source requires a declared table schema".into(),
            )
        })?;

        let pk_columns: Vec<String> = config
            .get("_primary_key_columns")
            .unwrap_or("")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if pk_columns.is_empty() {
            return Err(crate::error::ConnectorError::ConfigurationError(
                "mongodb lookup source requires primary key columns".into(),
            ));
        }

        config.reject_unknown_properties(MONGODB_LOOKUP_PROPERTIES, "MongoDB lookup")?;
        let lookup_config = MongoLookupSourceConfig {
            connection_uri: config.require("connection.uri")?.to_string(),
            database: config.require("database")?.to_string(),
            collection: config.require("collection")?.to_string(),
            primary_key_columns: pk_columns,
            schema,
        };

        let source = MongoLookupSource::open(lookup_config).await?;
        Ok(Arc::new(source) as Arc<dyn laminar_core::lookup::source::LookupSourceDyn>)
    }
}

/// Registers the `MongoDB` sink connector with the given registry.
///
/// # Errors
///
/// Returns an error if the connector name is already registered or the registry is frozen.
pub fn register_mongodb_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
        schema_capabilities: crate::schema::resolution::SchemaCapabilities::metadata(
            &[],
            true,
            crate::schema::resolution::SchemaPreparation::ExplicitTableCreation,
        ),
        name: "mongodb-sink".to_string(),
        display_name: "MongoDB Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: mongodb_sink_config_keys(),
    };

    registry.register_sink(
        "mongodb-sink",
        info,
        Arc::new(|config, registry: Option<&Arc<prometheus::Registry>>| {
            MongoDbSink::from_connector_config(config, registry.map(Arc::as_ref))
                .map(|sink| Box::new(sink) as Box<dyn crate::connector::SinkConnector>)
        }),
    )
}

fn mongodb_cdc_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("connection.uri", "MongoDB connection URI"),
        ConfigKeySpec::required("database", "Database name"),
        ConfigKeySpec::required("collection", "Fixed collection name"),
        ConfigKeySpec::optional(
            "output.mode",
            "history (immutable event records) or document (keyed puts and deletes)",
            "history",
        ),
        ConfigKeySpec::optional(
            "snapshot.mode",
            "never (changes only) or initial (copy the collection, then stream changes)",
            "never",
        ),
        ConfigKeySpec::optional(
            "full.document.mode",
            "Deterministic full document mode (delta or required post-image)",
            "delta",
        ),
        ConfigKeySpec::optional(
            "objectid.columns",
            "Document-mode VARCHAR columns holding ObjectId values as lowercase hex",
            "",
        ),
        ConfigKeySpec::optional(
            "document.json.column",
            "Document-mode VARCHAR column receiving the whole canonical Extended JSON document",
            "",
        ),
        ConfigKeySpec::optional(
            "pipeline",
            "JSON array of up to 64 $match stages (maximum 256 KiB)",
            "[]",
        ),
        ConfigKeySpec::optional(
            "max.buffered.bytes",
            "Max retained decoded bytes before backpressure (1 MiB to 4 GiB)",
            config::DEFAULT_MAX_BUFFERED_BYTES.to_string(),
        ),
    ]
}

fn mongodb_sink_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("connection.uri", "MongoDB connection URI"),
        ConfigKeySpec::required("database", "Target database name"),
        ConfigKeySpec::required("collection", "Target collection name"),
        ConfigKeySpec::optional(
            "auto.create",
            "Explicit permission to create a missing standard collection",
            "false",
        ),
        ConfigKeySpec::optional("flush.interval.ms", "Max time between flushes (ms)", "250"),
        ConfigKeySpec::optional(
            "write.mode",
            "Write operation mode (insert, upsert, cdc_replay)",
            "insert",
        ),
        ConfigKeySpec::optional(
            "write.mode.key_fields",
            "Comma-separated key fields to match documents in upsert mode",
            "",
        ),
        ConfigKeySpec::optional(
            "timeseries.time_field",
            "The field in each document containing the date",
            "",
        ),
        ConfigKeySpec::optional(
            "timeseries.meta_field",
            "An optional field labeling the data source",
            "",
        ),
        ConfigKeySpec::optional(
            "timeseries.granularity",
            "Bucketing granularity (seconds, minutes, hours, custom)",
            "seconds",
        ),
        ConfigKeySpec::optional(
            "timeseries.bucket_max_span_seconds",
            "Max span of a single bucket in seconds (custom granularity)",
            "",
        ),
        ConfigKeySpec::optional(
            "timeseries.bucket_rounding_seconds",
            "Rounding boundary in seconds (custom granularity)",
            "",
        ),
        ConfigKeySpec::optional(
            "timeseries.expire_after_seconds",
            "TTL in seconds (automatically delete documents after this span)",
            "",
        ),
        ConfigKeySpec::optional(
            "sink.write.timeout.ms",
            "Complete MongoDB sink write deadline in milliseconds",
            "30000",
        ),
    ]
}

fn mongodb_lookup_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("connection.uri", "MongoDB connection URI"),
        ConfigKeySpec::required("database", "Database name"),
        ConfigKeySpec::required("collection", "Collection name"),
    ]
}

#[cfg(test)]
mod tests;
