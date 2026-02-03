//! `PostgreSQL` sink connector (F027B).
//!
//! Writes Arrow `RecordBatch` to `PostgreSQL` tables using two strategies:
//! - **Append mode**: COPY BINARY for maximum throughput (>500K rows/sec)
//! - **Upsert mode**: `INSERT ... ON CONFLICT DO UPDATE` with UNNEST arrays
//!
//! Exactly-once semantics use co-transactional offset storage — data and
//! epoch markers committed in the same `PostgreSQL` transaction.
//!
//! # Architecture
//!
//! ```text
//! Ring 0 (Hot Path):  SPSC push only (~5ns, zero sink code)
//! Ring 1 (Background): Batch buffering → COPY/INSERT → transaction mgmt
//! Ring 2 (Control):    Connection pool, table creation, epoch recovery
//! ```
//!
//! # Module Structure
//!
//! - [`sink_config`] - Configuration and enums
//! - [`sink`] - `PostgresSink` implementing `SinkConnector`
//! - [`sink_metrics`] - Lock-free atomic metrics
//! - [`types`] - Arrow → `PostgreSQL` type mapping
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_connectors::postgres::{PostgresSink, PostgresSinkConfig, WriteMode};
//!
//! let config = PostgresSinkConfig {
//!     hostname: "localhost".to_string(),
//!     database: "mydb".to_string(),
//!     table_name: "events".to_string(),
//!     write_mode: WriteMode::Upsert,
//!     primary_key_columns: vec!["id".to_string()],
//!     ..Default::default()
//! };
//!
//! let sink = PostgresSink::new(schema, config);
//! ```

pub mod sink;
pub mod sink_config;
pub mod sink_metrics;
pub mod types;

// Re-export primary types at module level.
pub use sink::PostgresSink;
pub use sink_config::{DeliveryGuarantee, PostgresSinkConfig, SslMode, WriteMode};
pub use sink_metrics::PostgresSinkMetrics;

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the `PostgreSQL` sink connector with the given registry.
pub fn register_postgres_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "postgres-sink".to_string(),
        display_name: "PostgreSQL Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: postgres_sink_config_keys(),
    };

    registry.register_sink(
        "postgres-sink",
        info,
        Arc::new(|| {
            // Default schema (overridden during open).
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]));
            Box::new(PostgresSink::new(schema, PostgresSinkConfig::default()))
        }),
    );
}

fn postgres_sink_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("hostname", "PostgreSQL server hostname"),
        ConfigKeySpec::required("database", "Target database name"),
        ConfigKeySpec::required("username", "Authentication username"),
        ConfigKeySpec::required("table.name", "Target table name"),
        ConfigKeySpec::optional("password", "Authentication password", ""),
        ConfigKeySpec::optional("port", "PostgreSQL port", "5432"),
        ConfigKeySpec::optional("schema.name", "Target schema name", "public"),
        ConfigKeySpec::optional(
            "write.mode",
            "Write mode: 'append' (COPY BINARY) or 'upsert' (ON CONFLICT)",
            "append",
        ),
        ConfigKeySpec::optional(
            "primary.key",
            "Comma-separated primary key columns (required for upsert mode)",
            "",
        ),
        ConfigKeySpec::optional("batch.size", "Max records before flush", "4096"),
        ConfigKeySpec::optional("flush.interval.ms", "Max time before flush (ms)", "1000"),
        ConfigKeySpec::optional("pool.size", "Connection pool size", "4"),
        ConfigKeySpec::optional("connect.timeout.ms", "Connection timeout (ms)", "10000"),
        ConfigKeySpec::optional(
            "ssl.mode",
            "SSL mode: disable/prefer/require/verify-ca/verify-full",
            "prefer",
        ),
        ConfigKeySpec::optional(
            "auto.create.table",
            "Create target table from Arrow schema if missing",
            "false",
        ),
        ConfigKeySpec::optional(
            "changelog.mode",
            "Handle F063 Z-set records (split INSERT/DELETE by _op)",
            "false",
        ),
        ConfigKeySpec::optional(
            "delivery.guarantee",
            "at_least_once or exactly_once",
            "at_least_once",
        ),
        ConfigKeySpec::optional(
            "sink.id",
            "Unique ID for offset tracking (auto-generated if not set)",
            "",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_postgres_sink() {
        let registry = ConnectorRegistry::new();
        register_postgres_sink(&registry);

        let info = registry.sink_info("postgres-sink");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "postgres-sink");
        assert!(info.is_sink);
        assert!(!info.is_source);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_config_keys_required() {
        let keys = postgres_sink_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"hostname"));
        assert!(required.contains(&"database"));
        assert!(required.contains(&"username"));
        assert!(required.contains(&"table.name"));
    }

    #[test]
    fn test_config_keys_optional_present() {
        let keys = postgres_sink_config_keys();
        let optional: Vec<&str> = keys
            .iter()
            .filter(|k| !k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(optional.contains(&"port"));
        assert!(optional.contains(&"write.mode"));
        assert!(optional.contains(&"primary.key"));
        assert!(optional.contains(&"batch.size"));
        assert!(optional.contains(&"delivery.guarantee"));
        assert!(optional.contains(&"changelog.mode"));
        assert!(optional.contains(&"ssl.mode"));
    }

    #[test]
    fn test_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_postgres_sink(&registry);

        let config = crate::config::ConnectorConfig::new("postgres-sink");
        let sink = registry.create_sink(&config);
        assert!(sink.is_ok());
    }
}
