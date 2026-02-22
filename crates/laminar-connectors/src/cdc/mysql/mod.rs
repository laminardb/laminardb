//! MySQL binlog replication CDC source connector.
//!
// Note: Many functions are not yet used because actual I/O is not implemented.
// These allows will be removed when we add the binlog reader.
#![allow(dead_code)]
// MySQL CDC docs reference many MySQL-specific terms that clippy wants backticks for.
// This is a domain-specific module where MySQL terminology is ubiquitous.
#![allow(clippy::doc_markdown)]

//!
//! This module implements a MySQL CDC source that reads change events from
//! MySQL binary log (binlog) replication stream. It supports:
//!
//! - GTID-based and file/position-based replication
//! - Row-based replication format (INSERT/UPDATE/DELETE events)
//! - Table filtering with include/exclude patterns
//! - SSL/TLS connections
//! - Automatic schema discovery via TABLE_MAP events
//! - Z-set changelog format
//!
//! # Architecture
//!
//! ```text
//! MySQL Server
//!      │
//!      │ Binlog Replication Protocol
//!      ▼
//! ┌─────────────────────────────────────────┐
//! │           MySqlCdcSource                │
//! │  ┌─────────────┐  ┌─────────────────┐  │
//! │  │   Decoder   │  │   TableCache    │  │
//! │  │ (binlog →   │  │ (TABLE_MAP →    │  │
//! │  │  messages)  │  │  Arrow schema)  │  │
//! │  └─────────────┘  └─────────────────┘  │
//! │          │                │            │
//! │          ▼                ▼            │
//! │  ┌─────────────────────────────────┐   │
//! │  │        ChangeEvent              │   │
//! │  │  (Z-set with before/after)      │   │
//! │  └─────────────────────────────────┘   │
//! └─────────────────────────────────────────┘
//!      │
//!      ▼
//!  RecordBatch (Arrow)
//! ```
//!
//! # Example
//!
//! ```ignore
//! use laminar_connectors::cdc::mysql::{MySqlCdcSource, MySqlCdcConfig};
//!
//! let config = MySqlCdcConfig {
//!     host: "localhost".to_string(),
//!     port: 3306,
//!     username: "replicator".to_string(),
//!     password: "secret".to_string(),
//!     database: Some("mydb".to_string()),
//!     server_id: 12345,
//!     use_gtid: true,
//!     ..Default::default()
//! };
//!
//! let mut source = MySqlCdcSource::new(config);
//! source.open(&Default::default()).await?;
//!
//! while let Some(batch) = source.poll_batch(1000).await? {
//!     // Process CDC events as Arrow RecordBatch
//!     println!("Received {} rows", batch.num_rows());
//! }
//! ```

mod changelog;
mod config;
mod decoder;
mod gtid;
mod metrics;
#[cfg(feature = "mysql-cdc")]
pub mod mysql_io;
mod schema;
mod source;
mod types;

// Primary types
pub use changelog::{
    column_value_to_json, delete_to_events, events_to_record_batch, insert_to_events, row_to_json,
    update_to_events, CdcOperation, ChangeEvent,
};
pub use config::{MySqlCdcConfig, SnapshotMode, SslMode};
pub use decoder::{
    BeginMessage, BinlogMessage, BinlogPosition, ColumnValue, CommitMessage, DecoderError,
    DeleteMessage, InsertMessage, QueryMessage, RotateMessage, RowData, TableMapMessage,
    UpdateMessage, UpdateRowData,
};
pub use gtid::{Gtid, GtidRange, GtidSet};
pub use metrics::{MetricsSnapshot, MySqlCdcMetrics};
pub use schema::{cdc_envelope_schema, TableCache, TableInfo};
pub use source::MySqlCdcSource;
pub use types::{mysql_type, mysql_type_name, mysql_type_to_arrow, mysql_type_to_sql, MySqlColumn};

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::connector::SourceConnector;
use crate::registry::ConnectorRegistry;

/// Registers the MySQL CDC source connector factory.
///
/// After calling this function, the connector can be created via:
/// ```ignore
/// let source = registry.create_source(&config)?;
/// ```
pub fn register_mysql_cdc_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "mysql-cdc".to_string(),
        display_name: "MySQL CDC Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: config_key_specs(),
    };

    registry.register_source(
        "mysql-cdc",
        info,
        Arc::new(|| {
            Box::new(MySqlCdcSource::new(MySqlCdcConfig::default())) as Box<dyn SourceConnector>
        }),
    );
}

/// Returns the configuration key specifications for `MySQL` CDC source.
///
/// This is used for configuration discovery and validation.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn config_key_specs() -> Vec<ConfigKeySpec> {
    vec![
        // Connection settings
        ConfigKeySpec::optional("host", "MySQL server hostname", "localhost"),
        ConfigKeySpec::optional("port", "MySQL server port", "3306"),
        ConfigKeySpec::optional("database", "Database name to replicate", ""),
        ConfigKeySpec::required("username", "MySQL replication user"),
        ConfigKeySpec::required("password", "MySQL replication password"),
        // SSL settings
        ConfigKeySpec::optional(
            "ssl.mode",
            "SSL connection mode (disabled/preferred/required/verify_ca/verify_identity)",
            "preferred",
        ),
        // Replication settings
        ConfigKeySpec::required("server.id", "Unique server ID for this replication client"),
        ConfigKeySpec::optional(
            "use.gtid",
            "Use GTID-based replication (recommended)",
            "true",
        ),
        ConfigKeySpec::optional("gtid.set", "Starting GTID set for replication", ""),
        ConfigKeySpec::optional(
            "binlog.filename",
            "Starting binlog filename (if not using GTID)",
            "",
        ),
        ConfigKeySpec::optional(
            "binlog.position",
            "Starting binlog position (if not using GTID)",
            "4",
        ),
        // Snapshot settings
        ConfigKeySpec::optional(
            "snapshot.mode",
            "Snapshot mode (initial/never/always/schema_only)",
            "initial",
        ),
        // Table filtering
        ConfigKeySpec::optional(
            "table.include",
            "Comma-separated list of tables to include",
            "",
        ),
        ConfigKeySpec::optional(
            "table.exclude",
            "Comma-separated list of tables to exclude",
            "",
        ),
        ConfigKeySpec::optional("database.filter", "Database name filter pattern", ""),
        // Tuning settings
        ConfigKeySpec::optional(
            "poll.timeout.ms",
            "Timeout for polling binlog events (milliseconds)",
            "1000",
        ),
        ConfigKeySpec::optional("max.poll.records", "Maximum records per poll batch", "1000"),
        ConfigKeySpec::optional(
            "heartbeat.interval.ms",
            "Heartbeat interval (milliseconds)",
            "30000",
        ),
        ConfigKeySpec::optional(
            "connect.timeout.ms",
            "Connection timeout (milliseconds)",
            "10000",
        ),
        ConfigKeySpec::optional("read.timeout.ms", "Read timeout (milliseconds)", "60000"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_mysql_cdc_source() {
        let registry = ConnectorRegistry::new();
        register_mysql_cdc_source(&registry);

        // Verify the source type is registered
        let source_list = registry.list_sources();
        assert!(source_list.contains(&"mysql-cdc".to_string()));
    }

    #[test]
    fn test_config_key_specs() {
        let specs = config_key_specs();

        // Should have all expected keys
        assert!(specs.len() >= 15);

        // Required keys
        let required: Vec<_> = specs.iter().filter(|s| s.required).collect();
        assert!(required.iter().any(|s| s.key == "username"));
        assert!(required.iter().any(|s| s.key == "password"));
        assert!(required.iter().any(|s| s.key == "server.id"));

        // Optional with defaults
        let host_spec = specs.iter().find(|s| s.key == "host").unwrap();
        assert!(!host_spec.required);
        assert_eq!(host_spec.default, Some("localhost".to_string()));

        let port_spec = specs.iter().find(|s| s.key == "port").unwrap();
        assert!(!port_spec.required);
        assert_eq!(port_spec.default, Some("3306".to_string()));

        let ssl_spec = specs.iter().find(|s| s.key == "ssl.mode").unwrap();
        assert!(!ssl_spec.required);
        assert_eq!(ssl_spec.default, Some("preferred".to_string()));
    }

    #[test]
    fn test_public_exports() {
        // Verify all expected types are exported
        let _config = MySqlCdcConfig::default();
        let _ssl = SslMode::Preferred;
        let _snapshot = SnapshotMode::Initial;
        let _metrics = MySqlCdcMetrics::new();
        let _cache = TableCache::new();
        let _op = CdcOperation::Insert;
    }
}
