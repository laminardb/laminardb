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
pub use crate::postgres::SslMode;
pub use config::PostgresCdcConfig;
pub use lsn::Lsn;
pub use source::PostgresCdcSource;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the `PostgreSQL` CDC source connector with the given registry.
///
/// # Errors
///
/// Returns an error if the connector name is already registered or the registry is frozen.
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
        Arc::new(|registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(PostgresCdcSource::new(
                PostgresCdcConfig::default(),
                registry.map(Arc::as_ref),
            )))
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
                crate::postgres::reference::PostgresReferenceTableSource::new(
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
        use crate::postgres::lookup::{PostgresLookupSource, PostgresLookupSourceConfig};

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
