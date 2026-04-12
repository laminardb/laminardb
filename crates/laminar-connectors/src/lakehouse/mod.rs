//! Lakehouse connectors (Delta Lake, Apache Iceberg).

// Delta Lake modules
pub mod delta;
pub mod delta_config;
#[cfg(feature = "delta-lake")]
pub mod delta_io;
pub mod delta_metrics;
pub mod delta_source;
pub mod delta_source_config;
#[cfg(feature = "delta-lake")]
pub mod delta_table_provider;
#[cfg(feature = "delta-lake-unity")]
pub(crate) mod unity_catalog;

// Apache Iceberg modules
pub mod iceberg;
pub mod iceberg_config;
#[cfg(feature = "iceberg")]
pub mod iceberg_incremental;
#[cfg(feature = "iceberg")]
pub mod iceberg_io;
pub mod iceberg_reference;
pub mod iceberg_source;

// Common metrics
pub mod metrics;

// Re-export Delta Lake types at module level.
pub use delta::DeltaLakeSink;
pub use delta_config::{
    CompactionConfig, DeliveryGuarantee, DeltaCatalogType, DeltaLakeSinkConfig, DeltaWriteMode,
};
pub use delta_metrics::DeltaLakeSinkMetrics;
pub use delta_source::DeltaSource;
pub use delta_source_config::{DeltaReadMode, DeltaSourceConfig, SchemaEvolutionAction};
pub use metrics::LakehouseSinkMetrics;

// Re-export Iceberg types at module level.
pub use iceberg::IcebergSink;
pub use iceberg_config::{
    IcebergCatalogConfig, IcebergCatalogType, IcebergSinkConfig, IcebergSourceConfig,
};
pub use iceberg_reference::IcebergReferenceTableSource;
pub use iceberg_source::IcebergSource;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the Delta Lake sink connector with the given registry.
pub fn register_delta_lake_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "delta-lake".to_string(),
        display_name: "Delta Lake Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: delta_lake_config_keys(),
    };

    registry.register_sink(
        "delta-lake",
        info,
        Arc::new(|registry: Option<&prometheus::Registry>| {
            Box::new(DeltaLakeSink::new(DeltaLakeSinkConfig::default(), registry))
        }),
    );
}

/// Registers the Delta Lake source connector with the given registry.
pub fn register_delta_lake_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "delta-lake".to_string(),
        display_name: "Delta Lake Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: delta_lake_source_config_keys(),
    };

    registry.register_source(
        "delta-lake",
        info.clone(),
        Arc::new(|registry: Option<&prometheus::Registry>| {
            Box::new(DeltaSource::new(DeltaSourceConfig::default(), registry))
        }),
    );

    // Also register as a table source so CREATE LOOKUP TABLE ... WITH
    // (connector = 'delta-lake') can use Delta tables as reference data.
    #[cfg(feature = "delta-lake")]
    registry.register_table_source(
        "delta-lake",
        info,
        Arc::new(|config| {
            Ok(Box::new(
                crate::lookup::delta_reference::DeltaReferenceTableSource::from_connector_config(
                    config,
                )?,
            ))
        }),
    );

    // Register lookup source factory for on-demand/partial cache mode.
    #[cfg(feature = "delta-lake")]
    registry.register_lookup_source(
        "delta-lake",
        Arc::new(|config| {
            Box::pin(async move {
                use crate::lakehouse::delta_source_config::DeltaSourceConfig;
                use crate::lookup::delta_lookup::{DeltaLookupSource, DeltaLookupSourceConfig};

                let pk_columns: Vec<String> = config
                    .get("_primary_key_columns")
                    .unwrap_or("")
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if pk_columns.is_empty() {
                    return Err(crate::error::ConnectorError::ConfigurationError(
                        "delta-lake lookup source requires primary key columns".into(),
                    ));
                }

                let src_config = DeltaSourceConfig::from_config(&config)?;

                let (resolved_path, resolved_opts) =
                    crate::lakehouse::delta_io::resolve_catalog_options(
                        &src_config.catalog_type,
                        src_config.catalog_database.as_deref(),
                        src_config.catalog_name.as_deref(),
                        src_config.catalog_schema.as_deref(),
                        &src_config.table_path,
                        &src_config.storage_options,
                    )
                    .await?;

                let lookup_config = DeltaLookupSourceConfig {
                    table_path: resolved_path,
                    storage_options: resolved_opts,
                    primary_key_columns: pk_columns,
                    table_name: "delta_lookup".to_string(),
                };

                let source = DeltaLookupSource::open(lookup_config).await.map_err(|e| {
                    crate::error::ConnectorError::Internal(format!("delta lookup source open: {e}"))
                })?;

                Ok(Arc::new(source) as Arc<dyn laminar_core::lookup::source::LookupSourceDyn>)
            })
        }),
    );
}

/// Registers the Iceberg sink connector with the given registry.
#[allow(clippy::missing_panics_doc)]
pub fn register_iceberg_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "iceberg".to_string(),
        display_name: "Apache Iceberg Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: iceberg_sink_config_keys(),
    };

    registry.register_sink(
        "iceberg",
        info,
        Arc::new(|registry: Option<&prometheus::Registry>| {
            // Default config with placeholder values — real config arrives via open().
            let mut cfg = crate::config::ConnectorConfig::new("iceberg");
            cfg.set("catalog.uri", "http://localhost:8181");
            cfg.set("warehouse", "s3://default/wh");
            cfg.set("namespace", "default");
            cfg.set("table.name", "default");
            Box::new(IcebergSink::new(
                IcebergSinkConfig::from_config(&cfg).expect("default iceberg sink config"),
                registry,
            ))
        }),
    );
}

/// Registers the Iceberg source connector with the given registry.
#[allow(clippy::missing_panics_doc)]
pub fn register_iceberg_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "iceberg".to_string(),
        display_name: "Apache Iceberg Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: iceberg_source_config_keys(),
    };

    registry.register_source(
        "iceberg",
        info.clone(),
        Arc::new(|registry: Option<&prometheus::Registry>| {
            let mut cfg = crate::config::ConnectorConfig::new("iceberg");
            cfg.set("catalog.uri", "http://localhost:8181");
            cfg.set("warehouse", "s3://default/wh");
            cfg.set("namespace", "default");
            cfg.set("table.name", "default");
            Box::new(IcebergSource::new(
                IcebergSourceConfig::from_config(&cfg).expect("default iceberg source config"),
                registry,
            ))
        }),
    );

    // Register as table source for CREATE LOOKUP TABLE ... WITH (connector = 'iceberg').
    #[cfg(feature = "iceberg")]
    registry.register_table_source(
        "iceberg",
        info,
        Arc::new(|config| {
            Ok(Box::new(
                IcebergReferenceTableSource::from_connector_config(config)?,
            ))
        }),
    );
}

/// Registers all lakehouse sink connectors (Delta Lake, Iceberg).
pub fn register_lakehouse_sinks(registry: &ConnectorRegistry) {
    register_delta_lake_sink(registry);
    register_iceberg_sink(registry);
}

/// Registers all lakehouse source connectors.
pub fn register_lakehouse_sources(registry: &ConnectorRegistry) {
    register_delta_lake_source(registry);
    register_iceberg_source(registry);
}

#[allow(clippy::too_many_lines)]
fn delta_lake_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required(
            "table.path",
            "Path to Delta Lake table (local, s3://, az://, gs://)",
        ),
        ConfigKeySpec::optional(
            "partition.columns",
            "Comma-separated partition column names",
            "",
        ),
        ConfigKeySpec::optional(
            "target.file.size",
            "Target Parquet file size in bytes",
            "134217728",
        ),
        ConfigKeySpec::optional(
            "max.buffer.records",
            "Maximum records to buffer before flushing",
            "100000",
        ),
        ConfigKeySpec::optional(
            "max.buffer.duration.ms",
            "Maximum time to buffer before flushing (ms)",
            "60000",
        ),
        ConfigKeySpec::optional(
            "checkpoint.interval",
            "Create Delta checkpoint every N commits",
            "10",
        ),
        ConfigKeySpec::optional(
            "schema.evolution",
            "Enable automatic schema evolution (additive columns)",
            "false",
        ),
        ConfigKeySpec::optional(
            "write.mode",
            "Write mode: append, overwrite, upsert",
            "append",
        ),
        ConfigKeySpec::optional(
            "merge.key.columns",
            "Key columns for upsert MERGE (required for upsert mode)",
            "",
        ),
        ConfigKeySpec::optional(
            "delivery.guarantee",
            "exactly-once or at-least-once",
            "at-least-once",
        ),
        ConfigKeySpec::optional(
            "compaction.enabled",
            "Enable background OPTIMIZE compaction",
            "true",
        ),
        ConfigKeySpec::optional(
            "compaction.z-order.columns",
            "Columns for Z-ORDER clustering",
            "",
        ),
        ConfigKeySpec::optional(
            "compaction.target-file-size",
            "Target file size after compaction (bytes, defaults to target.file.size)",
            "",
        ),
        ConfigKeySpec::optional(
            "compaction.min-files",
            "Minimum files before triggering compaction",
            "10",
        ),
        ConfigKeySpec::optional(
            "compaction.check-interval.ms",
            "How often to check if compaction is needed (milliseconds)",
            "3600000",
        ),
        ConfigKeySpec::optional(
            "vacuum.retention.hours",
            "Hours to retain old files during VACUUM",
            "168",
        ),
        ConfigKeySpec::optional(
            "writer.id",
            "Writer ID for exactly-once deduplication (auto UUID if not set)",
            "",
        ),
        // ── Catalog configuration ──
        ConfigKeySpec::optional("catalog.type", "Catalog type: none, glue, unity", "none"),
        ConfigKeySpec::optional(
            "catalog.database",
            "Catalog database name (required for Glue)",
            "",
        ),
        ConfigKeySpec::optional("catalog.name", "Catalog name (required for Unity)", ""),
        ConfigKeySpec::optional(
            "catalog.schema",
            "Catalog schema name (required for Unity)",
            "",
        ),
        ConfigKeySpec::optional(
            "catalog.workspace_url",
            "Databricks workspace URL (required for Unity)",
            "",
        ),
        ConfigKeySpec::optional(
            "catalog.access_token",
            "Databricks access token (required for Unity)",
            "",
        ),
        ConfigKeySpec::optional(
            "catalog.storage.location",
            "Storage location for auto-created UC external tables (e.g. s3://bucket/path)",
            "",
        ),
        ConfigKeySpec::optional(
            "max.commit.retries",
            "Maximum retries on optimistic concurrency conflicts",
            "3",
        ),
        // ── LogStore configuration ──
        ConfigKeySpec::optional(
            "storage.s3_locking_provider",
            "S3 locking provider: 'dynamodb' for DynamoDB-backed log store",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.dynamodb_table_name",
            "DynamoDB table name for S3 locking (default: delta_log)",
            "",
        ),
        // ── Cloud storage credentials (resolved via StorageCredentialResolver) ──
        ConfigKeySpec::optional(
            "storage.aws_access_key_id",
            "AWS access key ID (falls back to AWS_ACCESS_KEY_ID env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_secret_access_key",
            "AWS secret access key (falls back to AWS_SECRET_ACCESS_KEY env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_region",
            "AWS region for S3 paths (falls back to AWS_REGION env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_session_token",
            "AWS session token for temporary credentials (falls back to AWS_SESSION_TOKEN)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_endpoint",
            "Custom S3 endpoint (MinIO, LocalStack; falls back to AWS_ENDPOINT_URL)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.aws_profile",
            "AWS profile name (falls back to AWS_PROFILE env var)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_account_name",
            "Azure storage account name (falls back to AZURE_STORAGE_ACCOUNT_NAME)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_account_key",
            "Azure storage account key (falls back to AZURE_STORAGE_ACCOUNT_KEY)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_sas_token",
            "Azure SAS token (falls back to AZURE_STORAGE_SAS_TOKEN)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_client_id",
            "Azure client ID for service principal auth (falls back to AZURE_CLIENT_ID)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.google_service_account_path",
            "Path to GCS service account JSON (falls back to GOOGLE_APPLICATION_CREDENTIALS)",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.google_service_account_key",
            "Inline GCS service account JSON (falls back to GOOGLE_SERVICE_ACCOUNT_KEY)",
            "",
        ),
    ]
}

fn delta_lake_source_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required(
            "table.path",
            "Path to Delta Lake table (local, s3://, az://, gs://)",
        ),
        ConfigKeySpec::optional(
            "starting.version",
            "Starting version to read from (default: latest)",
            "",
        ),
        ConfigKeySpec::optional(
            "poll.interval.ms",
            "How often to poll for new versions (ms)",
            "1000",
        ),
        ConfigKeySpec::optional(
            "read.mode",
            "Read mode: 'incremental' (changes only) or 'snapshot' (full re-read)",
            "incremental",
        ),
        ConfigKeySpec::optional(
            "partition.filter",
            "SQL predicate for partition filter pushdown (e.g. \"date = '2024-01-01'\")",
            "",
        ),
        ConfigKeySpec::optional(
            "schema.evolution.action",
            "Action on schema change: 'warn' or 'error'",
            "warn",
        ),
        ConfigKeySpec::optional(
            "cdf.enabled",
            "Use Change Data Feed for incremental reads (requires CDF on table)",
            "false",
        ),
        // ── Catalog configuration ──
        ConfigKeySpec::optional("catalog.type", "Catalog type: none, glue, unity", "none"),
        ConfigKeySpec::optional(
            "catalog.database",
            "Catalog database name (required for Glue)",
            "",
        ),
        ConfigKeySpec::optional("catalog.name", "Catalog name (required for Unity)", ""),
        ConfigKeySpec::optional(
            "catalog.schema",
            "Catalog schema name (required for Unity)",
            "",
        ),
        ConfigKeySpec::optional(
            "catalog.workspace_url",
            "Databricks workspace URL (required for Unity)",
            "",
        ),
        ConfigKeySpec::optional(
            "catalog.access_token",
            "Databricks access token (required for Unity)",
            "",
        ),
        // ── LogStore configuration ──
        ConfigKeySpec::optional(
            "storage.s3_locking_provider",
            "S3 locking provider: 'dynamodb' for DynamoDB-backed log store",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.dynamodb_table_name",
            "DynamoDB table name for S3 locking (default: delta_log)",
            "",
        ),
        // ── Cloud storage credentials ──
        ConfigKeySpec::optional("storage.aws_access_key_id", "AWS access key ID", ""),
        ConfigKeySpec::optional("storage.aws_secret_access_key", "AWS secret access key", ""),
        ConfigKeySpec::optional("storage.aws_region", "AWS region for S3 paths", ""),
        ConfigKeySpec::optional(
            "storage.azure_storage_account_name",
            "Azure storage account name",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.azure_storage_account_key",
            "Azure storage account key",
            "",
        ),
        ConfigKeySpec::optional(
            "storage.google_service_account_path",
            "Path to GCS service account JSON",
            "",
        ),
    ]
}

fn iceberg_sink_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required(
            "catalog.uri",
            "REST catalog URI (e.g., http://polaris:8181)",
        ),
        ConfigKeySpec::required(
            "warehouse",
            "Warehouse location (e.g., s3://bucket/warehouse)",
        ),
        ConfigKeySpec::required("namespace", "Iceberg namespace (e.g., prod)"),
        ConfigKeySpec::required("table.name", "Table name within the namespace"),
        ConfigKeySpec::optional("catalog.type", "Catalog type: rest", "rest"),
        ConfigKeySpec::optional(
            "compression",
            "Parquet compression: zstd, snappy, none",
            "zstd",
        ),
        ConfigKeySpec::optional("auto.create", "Auto-create table if not exists", "false"),
        ConfigKeySpec::optional(
            "writer.id",
            "Writer ID for exactly-once deduplication (auto UUID if not set)",
            "",
        ),
    ]
}

fn iceberg_source_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required(
            "catalog.uri",
            "REST catalog URI (e.g., http://polaris:8181)",
        ),
        ConfigKeySpec::required(
            "warehouse",
            "Warehouse location (e.g., s3://bucket/warehouse)",
        ),
        ConfigKeySpec::required("namespace", "Iceberg namespace (e.g., prod)"),
        ConfigKeySpec::required("table.name", "Table name within the namespace"),
        ConfigKeySpec::optional("catalog.type", "Catalog type: rest", "rest"),
        ConfigKeySpec::optional(
            "poll.interval.ms",
            "How often to poll for new snapshots (ms)",
            "60000",
        ),
        ConfigKeySpec::optional("snapshot.id", "Pin to a specific snapshot ID", ""),
        ConfigKeySpec::optional(
            "select.columns",
            "Comma-separated column names to select (empty = all)",
            "",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_delta_lake_sink() {
        let registry = ConnectorRegistry::new();
        register_delta_lake_sink(&registry);

        let info = registry.sink_info("delta-lake");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "delta-lake");
        assert!(info.is_sink);
        assert!(!info.is_source);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_config_keys_required() {
        let keys = delta_lake_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"table.path"));
        assert_eq!(required.len(), 1);
    }

    #[test]
    fn test_config_keys_include_cloud_storage() {
        let keys = delta_lake_config_keys();
        let key_names: Vec<&str> = keys.iter().map(|k| k.key.as_str()).collect();
        assert!(key_names.contains(&"storage.aws_access_key_id"));
        assert!(key_names.contains(&"storage.aws_secret_access_key"));
        assert!(key_names.contains(&"storage.aws_region"));
        assert!(key_names.contains(&"storage.azure_storage_account_name"));
        assert!(key_names.contains(&"storage.azure_storage_account_key"));
        assert!(key_names.contains(&"storage.google_service_account_path"));
    }

    #[test]
    fn test_config_keys_optional_present() {
        let keys = delta_lake_config_keys();
        let optional: Vec<&str> = keys
            .iter()
            .filter(|k| !k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(optional.contains(&"partition.columns"));
        assert!(optional.contains(&"target.file.size"));
        assert!(optional.contains(&"write.mode"));
        assert!(optional.contains(&"delivery.guarantee"));
        assert!(optional.contains(&"merge.key.columns"));
        assert!(optional.contains(&"schema.evolution"));
        assert!(optional.contains(&"compaction.enabled"));
        assert!(optional.contains(&"compaction.z-order.columns"));
        assert!(optional.contains(&"vacuum.retention.hours"));
        assert!(optional.contains(&"writer.id"));
        // Catalog keys
        assert!(optional.contains(&"catalog.type"));
        assert!(optional.contains(&"catalog.database"));
        assert!(optional.contains(&"catalog.name"));
        assert!(optional.contains(&"catalog.schema"));
        assert!(optional.contains(&"catalog.workspace_url"));
        assert!(optional.contains(&"catalog.access_token"));
        assert!(optional.contains(&"catalog.storage.location"));
    }

    #[test]
    fn test_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_delta_lake_sink(&registry);

        let config = crate::config::ConnectorConfig::new("delta-lake");
        let sink = registry.create_sink(&config, None);
        assert!(sink.is_ok());
    }

    // ── Delta Lake source registration tests ──

    #[test]
    fn test_register_delta_lake_source() {
        let registry = ConnectorRegistry::new();
        register_delta_lake_source(&registry);

        let info = registry.source_info("delta-lake");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "delta-lake");
        assert!(info.is_source);
        assert!(!info.is_sink);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_source_config_keys() {
        let keys = delta_lake_source_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"table.path"));
        assert_eq!(required.len(), 1);

        let optional: Vec<&str> = keys
            .iter()
            .filter(|k| !k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(optional.contains(&"starting.version"));
        assert!(optional.contains(&"poll.interval.ms"));
        // Catalog keys
        assert!(optional.contains(&"catalog.type"));
        assert!(optional.contains(&"catalog.database"));
    }

    #[test]
    fn test_factory_creates_source() {
        let registry = ConnectorRegistry::new();
        register_delta_lake_source(&registry);

        let config = crate::config::ConnectorConfig::new("delta-lake");
        let source = registry.create_source(&config, None);
        assert!(source.is_ok());
    }

    #[test]
    fn test_register_lakehouse_sinks() {
        let registry = ConnectorRegistry::new();
        register_lakehouse_sinks(&registry);

        assert!(registry.sink_info("delta-lake").is_some());
        assert!(registry.sink_info("iceberg").is_some());
    }

    // ── Iceberg registration tests ──

    #[test]
    fn test_register_iceberg_sink() {
        let registry = ConnectorRegistry::new();
        register_iceberg_sink(&registry);

        let info = registry.sink_info("iceberg");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "iceberg");
        assert!(info.is_sink);
        assert!(!info.is_source);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_register_iceberg_source() {
        let registry = ConnectorRegistry::new();
        register_iceberg_source(&registry);

        let info = registry.source_info("iceberg");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "iceberg");
        assert!(info.is_source);
        assert!(!info.is_sink);
    }

    #[test]
    fn test_iceberg_sink_config_keys() {
        let keys = iceberg_sink_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"catalog.uri"));
        assert!(required.contains(&"warehouse"));
        assert!(required.contains(&"namespace"));
        assert!(required.contains(&"table.name"));
        assert_eq!(required.len(), 4);
    }

    #[test]
    fn test_iceberg_source_config_keys() {
        let keys = iceberg_source_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"catalog.uri"));
        assert!(required.contains(&"warehouse"));
        assert!(required.contains(&"namespace"));
        assert!(required.contains(&"table.name"));
        assert_eq!(required.len(), 4);
    }

    #[test]
    fn test_factory_creates_iceberg_sink() {
        let registry = ConnectorRegistry::new();
        register_iceberg_sink(&registry);

        let config = crate::config::ConnectorConfig::new("iceberg");
        let sink = registry.create_sink(&config, None);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_factory_creates_iceberg_source() {
        let registry = ConnectorRegistry::new();
        register_iceberg_source(&registry);

        let config = crate::config::ConnectorConfig::new("iceberg");
        let source = registry.create_source(&config, None);
        assert!(source.is_ok());
    }
}
