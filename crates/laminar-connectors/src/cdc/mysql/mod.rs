//! MySQL binlog replication CDC source connector. Reads row-level changes from
//! a MySQL server's binlog (GTID or file/position based), decodes them via
//! [`decoder`], resolves column types against the [`schema::TableCache`], and
//! emits Z-set [`changelog::ChangeEvent`]s that `MySqlCdcSource` converts into
//! Arrow `RecordBatch`es on `poll_batch`.
#![allow(dead_code)] // reader path is feature-gated; helpers are used conditionally
#![allow(clippy::doc_markdown)] // MySQL terminology dominates — backtick-warning noise

pub mod changelog;
pub mod config;
pub mod decoder;
pub mod gtid;
pub mod metrics;
#[cfg(feature = "mysql-cdc")]
pub mod mysql_io;
pub mod schema;
pub mod source;
pub mod types;

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

/// Registers the MySQL CDC source connector factory on the given registry.
pub fn register_mysql_cdc_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "mysql-cdc".to_string(),
        display_name: "MySQL CDC Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: mysql_cdc_config_keys(),
    };

    registry.register_source(
        "mysql-cdc",
        info.clone(),
        Arc::new(|| {
            Box::new(MySqlCdcSource::new(MySqlCdcConfig::default())) as Box<dyn SourceConnector>
        }),
    );

    registry.register_table_source(
        "mysql-cdc",
        info,
        Arc::new(|config| {
            let connector = Box::new(MySqlCdcSource::new(MySqlCdcConfig::default()));
            Ok(Box::new(crate::lookup::cdc_adapter::CdcTableSource::new(
                connector,
                config.clone(),
                4096,
            )))
        }),
    );
}

/// Returns the configuration key specifications for `MySQL` CDC source.
///
/// This is used for configuration discovery and validation.
#[allow(clippy::too_many_lines)]
fn mysql_cdc_config_keys() -> Vec<ConfigKeySpec> {
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
    fn test_mysql_cdc_config_keys() {
        let specs = mysql_cdc_config_keys();

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
