//! `MongoDB` CDC source and sink connectors.

pub mod change_event;
pub mod config;
pub mod lookup;
pub mod metrics;
pub mod sink;
pub mod source;
pub mod timeseries;
pub mod write_model;

// Re-export primary types at module level.
pub use config::{FullDocumentMode, MongoDbSinkConfig, MongoDbSourceConfig};
pub use sink::MongoDbSink;
pub use source::{mongodb_cdc_envelope_schema, MongoDbCdcSource};
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
pub fn register_mongodb_cdc_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
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
        Arc::new(|registry: Option<&prometheus::Registry>| {
            Box::new(MongoDbCdcSource::new(
                MongoDbSourceConfig::default(),
                registry,
            ))
        }),
    )?;

    // On-demand (partial cache mode) lookup source: find({ pk: { $in: [...] } }).
    registry.register_lookup_source(
        "mongodb",
        ConnectorInfo {
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
pub fn register_mongodb_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    let info = ConnectorInfo {
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
        Arc::new(|config, registry: Option<&prometheus::Registry>| {
            MongoDbSink::from_connector_config(config, registry)
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
            "full.document.mode",
            "Deterministic full document mode (delta or required post-image)",
            "delta",
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
mod tests {
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
}
