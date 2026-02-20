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
        ConfigKeySpec {
            key: "host".to_string(),
            description: "MySQL server hostname".to_string(),
            required: true,
            default: Some("localhost".to_string()),
        },
        ConfigKeySpec {
            key: "port".to_string(),
            description: "MySQL server port".to_string(),
            required: false,
            default: Some("3306".to_string()),
        },
        ConfigKeySpec {
            key: "database".to_string(),
            description: "Database name to replicate".to_string(),
            required: false,
            default: None,
        },
        ConfigKeySpec {
            key: "username".to_string(),
            description: "MySQL replication user".to_string(),
            required: true,
            default: None,
        },
        ConfigKeySpec {
            key: "password".to_string(),
            description: "MySQL replication password".to_string(),
            required: true,
            default: None,
        },
        // SSL settings
        ConfigKeySpec {
            key: "ssl_mode".to_string(),
            description:
                "SSL connection mode (disabled/preferred/required/verify_ca/verify_identity)"
                    .to_string(),
            required: false,
            default: Some("preferred".to_string()),
        },
        // Replication settings
        ConfigKeySpec {
            key: "server_id".to_string(),
            description: "Unique server ID for this replication client".to_string(),
            required: true,
            default: None,
        },
        ConfigKeySpec {
            key: "use_gtid".to_string(),
            description: "Use GTID-based replication (recommended)".to_string(),
            required: false,
            default: Some("true".to_string()),
        },
        ConfigKeySpec {
            key: "gtid_set".to_string(),
            description: "Starting GTID set for replication".to_string(),
            required: false,
            default: None,
        },
        ConfigKeySpec {
            key: "binlog_filename".to_string(),
            description: "Starting binlog filename (if not using GTID)".to_string(),
            required: false,
            default: None,
        },
        ConfigKeySpec {
            key: "binlog_position".to_string(),
            description: "Starting binlog position (if not using GTID)".to_string(),
            required: false,
            default: Some("4".to_string()),
        },
        // Snapshot settings
        ConfigKeySpec {
            key: "snapshot_mode".to_string(),
            description: "Snapshot mode (initial/never/always/schema_only)".to_string(),
            required: false,
            default: Some("initial".to_string()),
        },
        // Table filtering
        ConfigKeySpec {
            key: "table_include".to_string(),
            description: "Comma-separated list of tables to include".to_string(),
            required: false,
            default: None,
        },
        ConfigKeySpec {
            key: "table_exclude".to_string(),
            description: "Comma-separated list of tables to exclude".to_string(),
            required: false,
            default: None,
        },
        ConfigKeySpec {
            key: "database_filter".to_string(),
            description: "Database name filter pattern".to_string(),
            required: false,
            default: None,
        },
        // Tuning settings
        ConfigKeySpec {
            key: "poll_timeout_ms".to_string(),
            description: "Timeout for polling binlog events (milliseconds)".to_string(),
            required: false,
            default: Some("1000".to_string()),
        },
        ConfigKeySpec {
            key: "max_poll_records".to_string(),
            description: "Maximum records per poll batch".to_string(),
            required: false,
            default: Some("1000".to_string()),
        },
        ConfigKeySpec {
            key: "heartbeat_interval_ms".to_string(),
            description: "Heartbeat interval (milliseconds)".to_string(),
            required: false,
            default: Some("30000".to_string()),
        },
        ConfigKeySpec {
            key: "connect_timeout_ms".to_string(),
            description: "Connection timeout (milliseconds)".to_string(),
            required: false,
            default: Some("10000".to_string()),
        },
        ConfigKeySpec {
            key: "read_timeout_ms".to_string(),
            description: "Read timeout (milliseconds)".to_string(),
            required: false,
            default: Some("60000".to_string()),
        },
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
        assert!(required.iter().any(|s| s.key == "host"));
        assert!(required.iter().any(|s| s.key == "username"));
        assert!(required.iter().any(|s| s.key == "password"));
        assert!(required.iter().any(|s| s.key == "server_id"));

        // Optional with defaults
        let port_spec = specs.iter().find(|s| s.key == "port").unwrap();
        assert!(!port_spec.required);
        assert_eq!(port_spec.default, Some("3306".to_string()));

        let ssl_spec = specs.iter().find(|s| s.key == "ssl_mode").unwrap();
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
