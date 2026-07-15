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
pub use config::{PostgresCdcConfig, SslMode};
pub use lsn::Lsn;
pub use source::PostgresCdcSource;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the `PostgreSQL` CDC source connector with the given registry.
pub fn register_postgres_cdc_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|registry: Option<&prometheus::Registry>| {
            Box::new(PostgresCdcSource::new(
                PostgresCdcConfig::default(),
                registry,
            ))
        }),
    )?;

    // Register standalone finite snapshots (no replication slot required).
    let pg_info = ConnectorInfo {
        name: "postgres".to_string(),
        display_name: "PostgreSQL Lookup Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: postgres_lookup_config_keys(),
    };
    registry.register_table_source(
        "postgres",
        pg_info.clone(),
        Arc::new(|config, declared_schema| {
            Ok(Box::new(
                crate::lookup::postgres_reference::PostgresReferenceTableSource::new(
                    config.clone(),
                    declared_schema,
                ),
            ))
        }),
    )?;

    // On-demand (partial cache mode) lookup source: pooled + WHERE pk = ANY($1).
    registry.register_lookup_source("postgres", pg_info, Arc::new(PostgresLookupFactory))
}

fn postgres_lookup_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("table", "Qualified PostgreSQL table name"),
        ConfigKeySpec::optional(
            "connection",
            "libpq connection string (alternative to individual connection properties)",
            "",
        ),
        ConfigKeySpec::optional("connection_string", "Alias for connection", ""),
        ConfigKeySpec::optional("host", "PostgreSQL host", "localhost"),
        ConfigKeySpec::optional("port", "PostgreSQL port", "5432"),
        ConfigKeySpec::optional("database", "Database name (alternative to dbname)", ""),
        ConfigKeySpec::optional("dbname", "Alias for database", ""),
        ConfigKeySpec::optional("user", "Database user (alternative to username)", ""),
        ConfigKeySpec::optional("username", "Alias for user", ""),
        ConfigKeySpec::optional("password", "Database password", ""),
        ConfigKeySpec::optional("options", "PostgreSQL command-line options", ""),
        ConfigKeySpec::optional("pool_size", "On-demand lookup connection-pool size", "4"),
        ConfigKeySpec::optional(
            "ssl.mode",
            "Connection security: verify-full or explicit disable",
            "verify-full",
        ),
        ConfigKeySpec::optional(
            "ssl.ca.cert.path",
            "PEM file with trusted CA certificates; defaults to webpki roots",
            "",
        ),
    ]
}

struct PostgresLookupFactory;

#[async_trait::async_trait]
impl crate::registry::LookupSourceFactory for PostgresLookupFactory {
    async fn build(
        &self,
        config: crate::config::ConnectorConfig,
        _declared_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, crate::error::ConnectorError>
    {
        use crate::lookup::postgres_lookup::{PostgresLookupSource, PostgresLookupSourceConfig};

        let pk_columns: Vec<String> = config
            .get("_primary_key_columns")
            .unwrap_or("")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if pk_columns.is_empty() {
            return Err(crate::error::ConnectorError::ConfigurationError(
                "postgres lookup source requires primary key columns".into(),
            ));
        }

        let table = config
            .get("table")
            .ok_or_else(|| {
                crate::error::ConnectorError::ConfigurationError(
                    "postgres lookup source requires a 'table' property".into(),
                )
            })?
            .to_string();

        let pool_size = if let Some(s) = config.get("pool_size") {
            s.parse::<usize>().map_err(|e| {
                crate::error::ConnectorError::ConfigurationError(format!(
                    "invalid 'pool_size' value '{s}': {e}"
                ))
            })?
        } else {
            4
        };

        let lookup_config = PostgresLookupSourceConfig {
            properties: config.properties().clone(),
            table,
            primary_key_columns: pk_columns,
            pool_size,
        };

        let source = PostgresLookupSource::open(lookup_config).await?;
        Ok(Arc::new(source) as Arc<dyn laminar_core::lookup::source::LookupSourceDyn>)
    }
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
        ConfigKeySpec::optional(
            "ssl.mode",
            "Connection security: verify-full or explicit disable",
            "verify-full",
        ),
        ConfigKeySpec::optional(
            "ssl.ca.cert.path",
            "PEM file with trusted CA certificates; defaults to webpki roots",
            "",
        ),
        ConfigKeySpec::optional(
            "table.include",
            "Comma-separated schema-qualified tables to include",
            "",
        ),
        ConfigKeySpec::optional(
            "table.exclude",
            "Comma-separated schema-qualified tables to exclude",
            "",
        ),
        ConfigKeySpec::optional(
            "max.buffered.bytes",
            "Total connector-owned payload budget in bytes",
            "268435456",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_postgres_cdc_source() {
        let registry = ConnectorRegistry::new();
        register_postgres_cdc_source(&registry).unwrap();

        let info = registry.source_info("postgres-cdc");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "postgres-cdc");
        assert!(info.is_source);
        assert!(!info.is_sink);
        assert!(!info.config_keys.is_empty());
        assert!(!registry
            .list_table_sources()
            .contains(&"postgres-cdc".to_string()));
        let error = registry
            .create_table_source(
                &crate::config::ConnectorConfig::new("postgres-cdc"),
                Arc::new(arrow_schema::Schema::empty()),
            )
            .err()
            .expect("CDC polling gaps cannot define snapshot completion");
        assert!(error.to_string().contains("snapshot-capable table source"));
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
        assert!(!required.contains(&"ssl.mode"));
        assert!(keys.iter().any(|key| key.key == "ssl.ca.cert.path"));
        for removed in [
            "snapshot.mode",
            "max.poll.records",
            "wal.sender.timeout.ms",
            "poll.timeout.ms",
            "keepalive.interval.ms",
            "backpressure.high.watermark",
            "max.buffered.events",
            "start.lsn",
            "ssl.client.cert.path",
            "ssl.client.key.path",
            "ssl.sni.hostname",
        ] {
            assert!(keys.iter().all(|key| key.key != removed));
        }
        assert!(keys.iter().any(|key| key.key == "max.buffered.bytes"));
    }

    #[test]
    fn test_lookup_config_keys_match_snapshot_and_on_demand_paths() {
        let keys = postgres_lookup_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|key| key.required)
            .map(|key| key.key.as_str())
            .collect();
        assert_eq!(required, ["table"]);

        for supported in [
            "connection",
            "connection_string",
            "host",
            "port",
            "database",
            "dbname",
            "user",
            "username",
            "password",
            "options",
            "pool_size",
            "ssl.mode",
            "ssl.ca.cert.path",
        ] {
            assert!(
                keys.iter().any(|key| key.key == supported),
                "PostgreSQL lookup descriptor omits {supported}"
            );
        }

        for internal_or_rejected in ["_primary_key_columns", "sslmode", "sslrootcert"] {
            assert!(keys.iter().all(|key| key.key != internal_or_rejected));
        }
    }
}
