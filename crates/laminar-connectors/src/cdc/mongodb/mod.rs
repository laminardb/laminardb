//! MongoDB change stream CDC source connector.
//!
//! Streams change events from MongoDB using change streams. Supports
//! database-level and collection-level watching with optional pipeline
//! filtering and resume token-based checkpointing.
//!
//! # Production Readiness
//!
//! This is a basic implementation suitable for development and testing.
//! For production deployments, the maintainer's PR #255 provides a
//! production-ready version with:
//! - Automatic reconnection with exponential backoff
//! - Resume token lifecycle management (persistence, invalidation)
//! - Graceful handling of `ChangeStreamError` and `InvalidResumeToken`
//! - Connection pool management with configurable pool sizes
//! - Detailed operational metrics (lag, throughput, error rates)
//!
//! Until PR #255 is merged, operators should be aware that this
//! implementation does not automatically recover from connection drops
//! or resume token invalidation. A process restart with a fresh
//! checkpoint is required in those cases.
//!
//! # Architecture
//!
//! ```text
//! MongoDB Replica Set / Sharded Cluster
//!      |
//!      | Change Stream (oplog-backed)
//!      v
//! +------------------------------------------+
//! |          MongoCdcSource                  |
//! |  +-------------+  +------------------+  |
//! |  |  ChangeEvent|  |  Resume Token    |  |
//! |  |  (BSON ->   |  |  (checkpoint +   |  |
//! |  |   JSON)     |  |   resumability)  |  |
//! |  +-------------+  +------------------+  |
//! |          |                |              |
//! |          v                v              |
//! |  +----------------------------------+   |
//! |  |     CDC Envelope RecordBatch     |   |
//! |  | (_collection, _op, _ts_ms, ...)  |   |
//! |  +----------------------------------+   |
//! +------------------------------------------+
//!      |
//!      v
//!  RecordBatch (Arrow)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use laminar_connectors::cdc::mongodb::{MongoCdcSource, MongoCdcConfig};
//!
//! let config = MongoCdcConfig::new("mongodb://localhost:27017", "mydb");
//! let mut source = MongoCdcSource::new(config);
//! source.open(&Default::default()).await?;
//!
//! while let Some(batch) = source.poll_batch(1000).await? {
//!     println!("Received {} rows", batch.num_rows());
//! }
//! ```

mod changelog;
mod config;
mod metrics;
#[cfg(feature = "mongodb-cdc")]
pub mod mongo_io;
mod source;

// Primary types
pub use changelog::{cdc_envelope_schema, events_to_record_batch, CdcOperation, ChangeEvent};
pub use config::{FullDocumentBeforeChangeMode, FullDocumentMode, MongoCdcConfig};
pub use metrics::{MetricsSnapshot, MongoCdcMetrics};
pub use source::MongoCdcSource;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::connector::SourceConnector;
use crate::registry::ConnectorRegistry;

/// Registers the MongoDB CDC source connector factory.
///
/// After calling this function, the connector can be created via:
/// ```ignore
/// let source = registry.create_source(&config)?;
/// ```
pub fn register_mongodb_cdc_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "mongodb-cdc".to_string(),
        display_name: "MongoDB CDC Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: config_key_specs(),
    };

    registry.register_source(
        "mongodb-cdc",
        info.clone(),
        Arc::new(|| {
            Box::new(MongoCdcSource::new(MongoCdcConfig::default())) as Box<dyn SourceConnector>
        }),
    );

    registry.register_table_source(
        "mongodb-cdc",
        info,
        Arc::new(|config| {
            let connector = Box::new(MongoCdcSource::new(MongoCdcConfig::default()));
            Ok(Box::new(crate::lookup::cdc_adapter::CdcTableSource::new(
                connector,
                config.clone(),
                4096,
            )))
        }),
    );
}

/// Returns the configuration key specifications for MongoDB CDC source.
#[must_use]
pub fn config_key_specs() -> Vec<ConfigKeySpec> {
    vec![
        // Connection settings
        ConfigKeySpec::required("connection.uri", "MongoDB connection URI"),
        ConfigKeySpec::required("database", "Database name to watch"),
        ConfigKeySpec::optional("collection", "Collection name (empty = watch database)", ""),
        // Change stream settings
        ConfigKeySpec::optional(
            "pipeline",
            "Semicolon-separated aggregation pipeline stages (JSON)",
            "",
        ),
        ConfigKeySpec::optional("resume.token", "Resume token from previous position", ""),
        ConfigKeySpec::optional(
            "full.document",
            "Full document mode (off/update_lookup/when_available/required)",
            "update_lookup",
        ),
        ConfigKeySpec::optional(
            "full.document.before.change",
            "Pre-image mode (off/when_available/required)",
            "off",
        ),
        // Tuning settings
        ConfigKeySpec::optional(
            "poll.timeout.ms",
            "Timeout for polling change events (milliseconds)",
            "100",
        ),
        ConfigKeySpec::optional("max.poll.records", "Maximum records per poll batch", "1000"),
        ConfigKeySpec::optional(
            "max.buffered.events",
            "Maximum buffered change events",
            "100000",
        ),
        // Collection filtering
        ConfigKeySpec::optional(
            "collection.include",
            "Comma-separated list of collections to include",
            "",
        ),
        ConfigKeySpec::optional(
            "collection.exclude",
            "Comma-separated list of collections to exclude",
            "",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_mongodb_cdc_source() {
        let registry = ConnectorRegistry::new();
        register_mongodb_cdc_source(&registry);

        let source_list = registry.list_sources();
        assert!(source_list.contains(&"mongodb-cdc".to_string()));
    }

    #[test]
    fn test_config_key_specs() {
        let specs = config_key_specs();

        // Should have all expected keys
        assert!(specs.len() >= 10);

        // Required keys
        let required: Vec<_> = specs.iter().filter(|s| s.required).collect();
        assert!(required.iter().any(|s| s.key == "connection.uri"));
        assert!(required.iter().any(|s| s.key == "database"));

        // Optional with defaults
        let collection_spec = specs.iter().find(|s| s.key == "collection").unwrap();
        assert!(!collection_spec.required);

        let full_doc_spec = specs.iter().find(|s| s.key == "full.document").unwrap();
        assert!(!full_doc_spec.required);
        assert_eq!(full_doc_spec.default, Some("update_lookup".to_string()));
    }

    #[test]
    fn test_public_exports() {
        // Verify all expected types are exported
        let _config = MongoCdcConfig::default();
        let _full_doc = FullDocumentMode::UpdateLookup;
        let _before = FullDocumentBeforeChangeMode::Off;
        let _metrics = MongoCdcMetrics::new();
        let _op = CdcOperation::Insert;
    }
}
