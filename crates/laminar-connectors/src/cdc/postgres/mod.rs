//! `PostgreSQL` CDC source connector.

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
pub fn register_postgres_cdc_source(registry: &ConnectorRegistry) {
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
        info.clone(),
        Arc::new(|| Box::new(PostgresCdcSource::new(PostgresCdcConfig::default()))),
    );

    // Also register as a table source so CREATE LOOKUP TABLE ... WITH
    // ('connector' = 'postgres-cdc') can use CDC for reference table refresh.
    registry.register_table_source(
        "postgres-cdc",
        info,
        Arc::new(|config| {
            let connector = Box::new(PostgresCdcSource::new(PostgresCdcConfig::default()));
            Ok(Box::new(crate::lookup::cdc_adapter::CdcTableSource::new(
                connector,
                config.clone(),
                4096,
            )))
        }),
    );

    // Register standalone "postgres" table source for poll-based snapshot
    // lookups (no replication slot / CDC required).
    let pg_info = ConnectorInfo {
        name: "postgres".to_string(),
        display_name: "PostgreSQL Lookup Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: vec![],
    };
    registry.register_table_source(
        "postgres",
        pg_info,
        Arc::new(|config| {
            Ok(Box::new(
                crate::lookup::postgres_reference::PostgresReferenceTableSource::new(
                    config.clone(),
                ),
            ))
        }),
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
        // SSL certificate paths
        ConfigKeySpec::optional("ssl.ca.cert.path", "Path to CA certificate file", ""),
        ConfigKeySpec::optional(
            "ssl.client.cert.path",
            "Path to client certificate file",
            "",
        ),
        ConfigKeySpec::optional("ssl.client.key.path", "Path to client private key file", ""),
        ConfigKeySpec::optional("ssl.sni.hostname", "SNI hostname for SSL connections", ""),
        // Replication start position
        ConfigKeySpec::optional("start.lsn", "Starting LSN for replication", ""),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_postgres_cdc_source() {
        let registry = ConnectorRegistry::new();
        register_postgres_cdc_source(&registry);

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
