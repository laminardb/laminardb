//! PostgreSQL CDC source connector.
//!
//! Streams row-level changes from PostgreSQL using logical replication
//! (pgoutput plugin). Supports INSERT, UPDATE, DELETE operations with
//! Z-set changelog integration.
//!
//! # Architecture
//!
//! ```text
//! Ring 0 (Hot Path):  SPSC pop only (~5ns, zero CDC code)
//! Ring 1 (Background): WAL consumption → pgoutput decode → Arrow conversion
//! Ring 2 (Control):    Slot management, schema discovery, health checks
//! ```
//!
//! # Module Structure
//!
//! - `config` - Connection and replication configuration
//! - `lsn` - Log Sequence Number type
//! - `types` - PostgreSQL OID to Arrow type mapping
//! - `decoder` - pgoutput binary protocol parser
//! - `schema` - Relation (table) schema cache
//! - `changelog` - Z-set change event conversion
//! - `metrics` - Lock-free atomic CDC metrics
//! - `source` - `PostgresCdcSource` implementing `SourceConnector`
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_connectors::cdc::postgres::{PostgresCdcSource, PostgresCdcConfig};
//!
//! let config = PostgresCdcConfig::new("localhost", "mydb", "laminar_slot", "laminar_pub");
//! let mut source = PostgresCdcSource::new(config);
//! ```

pub mod changelog;
pub mod config;
pub mod decoder;
pub mod lsn;
pub mod metrics;
pub mod postgres_io;
pub mod schema;
pub mod source;
pub mod types;

// Re-export primary types at module level.
pub use config::{PostgresCdcConfig, SnapshotMode, SslMode};
pub use lsn::Lsn;
pub use source::PostgresCdcSource;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the `PostgreSQL` CDC source connector with the given registry.
pub fn register_postgres_cdc(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "postgres-cdc".to_string(),
        display_name: "PostgreSQL CDC Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: postgres_cdc_config_keys(),
    };

    registry.register_source(
        "postgres-cdc",
        info,
        Arc::new(|| Box::new(PostgresCdcSource::new(PostgresCdcConfig::default()))),
    );
}

fn postgres_cdc_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("host", "PostgreSQL host address"),
        ConfigKeySpec::required("database", "Database name"),
        ConfigKeySpec::required("slot.name", "Logical replication slot name"),
        ConfigKeySpec::required("publication", "Publication name"),
        ConfigKeySpec::optional("port", "PostgreSQL port", "5432"),
        ConfigKeySpec::optional("username", "Connection username", "postgres"),
        ConfigKeySpec::optional("password", "Connection password", ""),
        ConfigKeySpec::optional("ssl.mode", "SSL mode (disable/prefer/require)", "prefer"),
        ConfigKeySpec::optional(
            "snapshot.mode",
            "Snapshot mode (initial/never/always)",
            "initial",
        ),
        ConfigKeySpec::optional("poll.timeout.ms", "Poll timeout in milliseconds", "100"),
        ConfigKeySpec::optional("max.poll.records", "Max records per poll", "1000"),
        ConfigKeySpec::optional(
            "keepalive.interval.ms",
            "Keepalive interval in milliseconds",
            "10000",
        ),
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
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_postgres_cdc() {
        let registry = ConnectorRegistry::new();
        register_postgres_cdc(&registry);

        let info = registry.source_info("postgres-cdc");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "postgres-cdc");
        assert!(info.is_source);
        assert!(!info.is_sink);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_config_keys() {
        let keys = postgres_cdc_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"host"));
        assert!(required.contains(&"database"));
        assert!(required.contains(&"slot.name"));
        assert!(required.contains(&"publication"));
    }
}
