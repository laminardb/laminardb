//! MongoDB source and sink connectors.
//!
//! Provides a CDC source connector that streams change events from MongoDB
//! change streams, and a sink connector that writes to MongoDB collections
//! (including time series).
//!
//! # Architecture
//!
//! ```text
//! Ring 0 (Hot Path):  SPSC pop/push only (~5ns, zero MongoDB code)
//! Ring 1 (Background): Change stream consumption / batch writes
//! Ring 2 (Control):    Connection management, collection validation
//! ```
//!
//! # Module Structure
//!
//! - `config` — Source and sink configuration
//! - `change_event` — Change event types and operation mapping
//! - `resume_token` — Resume token persistence (file, MongoDB, in-memory)
//! - `large_event` — Split large event fragment reassembly
//! - `write_model` — Write mode enum and time series validation
//! - `timeseries` — Time series collection configuration
//! - `metrics` — Lock-free atomic CDC and sink metrics
//! - `source` — `MongoDbCdcSource` implementing `SourceConnector`
//! - `sink` — `MongoDbSink` implementing `SinkConnector`
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_connectors::mongodb::{MongoDbCdcSource, MongoDbSourceConfig};
//!
//! let config = MongoDbSourceConfig::new(
//!     "mongodb://localhost:27017",
//!     "mydb",
//!     "users",
//! );
//! let source = MongoDbCdcSource::new(config);
//! ```

pub mod change_event;
pub mod config;
pub mod large_event;
pub mod metrics;
pub mod resume_token;
pub mod sink;
pub mod source;
pub mod timeseries;
pub mod write_model;

// Re-export primary types at module level.
pub use config::{
    FullDocumentMode, MongoDbSinkConfig, MongoDbSourceConfig, WriteConcernConfig, WriteConcernLevel,
};
pub use resume_token::{
    FileResumeTokenStore, InMemoryResumeTokenStore, ResumeToken, ResumeTokenStore,
    ResumeTokenStoreConfig,
};
pub use sink::MongoDbSink;
pub use source::{mongodb_cdc_envelope_schema, MongoDbCdcSource};
pub use timeseries::{CollectionKind, TimeSeriesConfig, TimeSeriesGranularity};
pub use write_model::WriteMode;

#[cfg(feature = "mongodb-cdc")]
pub use resume_token::MongoResumeTokenStore;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the `MongoDB` CDC source connector with the given registry.
pub fn register_mongodb_cdc(registry: &ConnectorRegistry) {
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
        Arc::new(|| Box::new(MongoDbCdcSource::new(MongoDbSourceConfig::default()))),
    );
}

/// Registers the `MongoDB` sink connector with the given registry.
pub fn register_mongodb_sink(registry: &ConnectorRegistry) {
    use arrow_schema::{DataType, Field, Schema};

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
        Arc::new(|| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]));
            Box::new(MongoDbSink::new(schema, MongoDbSinkConfig::default()))
        }),
    );
}

fn mongodb_cdc_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("connection.uri", "MongoDB connection URI"),
        ConfigKeySpec::required("database", "Database name"),
        ConfigKeySpec::required("collection", "Collection name (* for all)"),
        ConfigKeySpec::optional(
            "full.document.mode",
            "Full document mode (delta/update_lookup/required/when_available)",
            "delta",
        ),
        ConfigKeySpec::optional(
            "split.large.events",
            "Enable $changeStreamSplitLargeEvent (requires MongoDB >= 6.0.9)",
            "false",
        ),
        ConfigKeySpec::optional("max.await.time.ms", "getMore await timeout (ms)", "1000"),
        ConfigKeySpec::optional("batch.size", "Cursor batch size", "1000"),
        ConfigKeySpec::optional("max.poll.records", "Max records per poll", "1000"),
        ConfigKeySpec::optional(
            "max.buffered.events",
            "Max events to buffer before backpressure",
            "100000",
        ),
    ]
}

fn mongodb_sink_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("connection.uri", "MongoDB connection URI"),
        ConfigKeySpec::required("database", "Target database name"),
        ConfigKeySpec::required("collection", "Target collection name"),
        ConfigKeySpec::optional("batch.size", "Max documents per flush", "500"),
        ConfigKeySpec::optional("flush.interval.ms", "Max time between flushes (ms)", "1000"),
        ConfigKeySpec::optional(
            "ordered",
            "Ordered writes (fail-fast) vs unordered (higher throughput)",
            "true",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_mongodb_cdc() {
        let registry = ConnectorRegistry::new();
        register_mongodb_cdc(&registry);

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
        register_mongodb_sink(&registry);

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
    }

    #[test]
    fn test_factory_creates_source() {
        let registry = ConnectorRegistry::new();
        register_mongodb_cdc(&registry);

        let config = crate::config::ConnectorConfig::new("mongodb-cdc");
        let source = registry.create_source(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_mongodb_sink(&registry);

        let config = crate::config::ConnectorConfig::new("mongodb-sink");
        let sink = registry.create_sink(&config);
        assert!(sink.is_ok());
    }
}
