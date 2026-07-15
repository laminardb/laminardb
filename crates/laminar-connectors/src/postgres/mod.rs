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
/// PostgreSQL connection security policy.
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
        Arc::new(|config, registry: Option<&prometheus::Registry>| {
            Ok(Box::new(PostgresSink::from_connector_config(
                config, registry,
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
mod driver_tests {
    use super::await_owned_driver;

    #[tokio::test]
    async fn owned_driver_outlives_a_cancelled_waiter() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();

        let waiter = tokio::spawn(async move {
            await_owned_driver(
                async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    let _ = completed_tx.send(());
                    Ok::<(), ()>(())
                },
                |_| (),
            )
            .await
        });

        started_rx.await.expect("owned task started");
        waiter.abort();
        assert!(waiter
            .await
            .expect_err("waiter must be cancelled")
            .is_cancelled());
        release_tx
            .send(())
            .expect("owned task still receives release");
        tokio::time::timeout(std::time::Duration::from_secs(1), completed_rx)
            .await
            .expect("owned task must finish after waiter cancellation")
            .expect("completion signal");
    }
}

#[cfg(all(test, feature = "postgres-sink"))]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    fn base_factory_config() -> crate::config::ConnectorConfig {
        let mut config = crate::config::ConnectorConfig::new("postgres-sink");
        config.set("hostname", "localhost");
        config.set("database", "analytics");
        config.set("username", "writer");
        config.set("table.name", "events");
        config
    }

    fn factory_config(schema: &SchemaRef) -> crate::config::ConnectorConfig {
        let mut config = base_factory_config();
        config.set(
            "_arrow_schema",
            crate::config::encode_arrow_schema_ipc(schema.as_ref()),
        );
        config
    }

    #[test]
    fn test_register_postgres_sink() {
        let registry = ConnectorRegistry::new();
        register_postgres_sink(&registry).unwrap();

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
        assert!(!optional.contains(&"batch.size"));
        assert!(!optional.contains(&"pool.size"));
        assert!(!optional.contains(&"delivery.guarantee"));
        assert!(optional.contains(&"changelog.mode"));
        assert!(optional.contains(&"ssl.mode"));
        assert!(optional.contains(&"ssl.ca.cert.path"));
        assert!(optional.contains(&"statement.timeout.ms"));
    }

    #[test]
    fn test_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_postgres_sink(&registry).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("sequence", DataType::Int64, false),
            Field::new("enabled", DataType::Boolean, true),
        ]));
        let sink = registry
            .create_sink(&factory_config(&schema), None)
            .unwrap();
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn test_factory_rejects_missing_or_malformed_arrow_schema() {
        let registry = ConnectorRegistry::new();
        register_postgres_sink(&registry).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let missing = base_factory_config();
        let missing_error = registry
            .create_sink(&missing, None)
            .err()
            .expect("missing schema must fail")
            .to_string();
        assert!(missing_error.contains("_arrow_schema"), "{missing_error}");

        let mut malformed = factory_config(&schema);
        malformed.set("_arrow_schema", "not-arrow-ipc");
        let malformed_error = registry
            .create_sink(&malformed, None)
            .err()
            .expect("malformed schema must fail")
            .to_string();
        assert!(
            malformed_error.contains("invalid") && malformed_error.contains("_arrow_schema"),
            "{malformed_error}"
        );
    }
}
