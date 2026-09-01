//! PostgreSQL connector-specific configuration and implementations.

#[cfg(feature = "postgres-cdc")]
pub mod cdc;
#[cfg(feature = "postgres-cdc")]
pub mod lookup;
#[cfg(feature = "postgres-cdc")]
pub mod reference;
#[cfg(feature = "postgres-sink")]
pub mod sink;
#[cfg(feature = "postgres-sink")]
pub mod sink_config;
#[cfg(feature = "postgres-sink")]
pub mod sink_metrics;
mod tls;
#[cfg(feature = "postgres-sink")]
pub mod types;

pub(crate) use tls::make_rustls_connector;
/// `PostgreSQL` connection security policy.
pub use tls::SslMode;

#[cfg(feature = "postgres-cdc")]
pub use cdc::{register_postgres_cdc_source, Lsn, PostgresCdcConfig, PostgresCdcSource};
#[cfg(feature = "postgres-cdc")]
pub use lookup::{PostgresLookupSource, PostgresLookupSourceConfig};
#[cfg(feature = "postgres-cdc")]
pub use reference::PostgresReferenceTableSource;

// Re-export primary sink types at module level.
#[cfg(feature = "postgres-sink")]
pub use sink::PostgresSink;
#[cfg(feature = "postgres-sink")]
pub use sink_config::{PostgresSinkConfig, WriteMode};
#[cfg(feature = "postgres-sink")]
pub use sink_metrics::PostgresSinkMetrics;

#[cfg(feature = "postgres-cdc")]
use std::future::Future;
#[cfg(feature = "postgres-sink")]
use std::sync::Arc;

#[cfg(feature = "postgres-sink")]
use crate::config::{ConfigKeySpec, ConnectorInfo};
#[cfg(feature = "postgres-sink")]
use crate::registry::ConnectorRegistry;

/// Poll a driver future from a task whose lifetime is independent of its caller.
/// Dropping the waiter detaches the task; it does not cancel the driver operation.
#[cfg(feature = "postgres-cdc")]
async fn await_owned_driver<T, E>(
    future: impl Future<Output = Result<T, E>> + Send + 'static,
    join_error: impl FnOnce(tokio::task::JoinError) -> E + Send + 'static,
) -> Result<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    match tokio::spawn(future).await {
        Ok(result) => result,
        Err(error) => Err(join_error(error)),
    }
}

/// Registers the `PostgreSQL` sink connector with the given registry.
///
/// # Errors
///
/// Returns an error if the connector name is already registered or the registry is frozen.
#[cfg(feature = "postgres-sink")]
pub fn register_postgres_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|config, registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(PostgresSink::from_connector_config(
                config,
                registry.map(Arc::as_ref),
            )?))
        }),
    )
}

#[cfg(feature = "postgres-sink")]
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
        ConfigKeySpec::optional("flush.interval.ms", "Max time before flush (ms)", "250"),
        ConfigKeySpec::optional("connect.timeout.ms", "Connection timeout (ms)", "10000"),
        ConfigKeySpec::optional("statement.timeout.ms", "Statement timeout (ms)", "30000"),
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
            "auto.create.table",
            "Create target table from Arrow schema if missing",
            "false",
        ),
        ConfigKeySpec::optional(
            "changelog.mode",
            "Handle Z-set records (split INSERT/DELETE by _op)",
            "false",
        ),
    ]
}

#[cfg(all(test, feature = "postgres-cdc"))]
mod driver_tests;
