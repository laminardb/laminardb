//! Lakehouse connectors (Delta Lake, Apache Iceberg).

// Versioned envelope for Delta coordinated-commit descriptors.
#[cfg(feature = "delta-lake")]
mod commit_descriptor;

// Delta Lake modules
pub mod delta;
pub mod delta_config;
#[cfg(feature = "delta-lake")]
pub mod delta_io;
#[cfg(feature = "delta-lake")]
pub mod delta_lookup;
pub mod delta_metrics;
#[cfg(feature = "delta-lake")]
pub mod delta_reference;
pub mod delta_source;
pub mod delta_source_config;
#[cfg(feature = "delta-lake")]
pub mod delta_table_provider;
#[cfg(feature = "delta-lake-unity")]
pub(crate) mod unity_catalog;

// Apache Iceberg modules
pub mod iceberg;
pub mod iceberg_config;
#[cfg(feature = "iceberg-core")]
pub mod iceberg_io;
#[cfg(feature = "iceberg-core")]
pub mod iceberg_lookup;
#[cfg(feature = "iceberg-core")]
pub mod iceberg_reference;
pub mod iceberg_source;

// Common metrics
pub mod metrics;
#[cfg(any(test, feature = "delta-lake", feature = "iceberg-core"))]
mod snapshot_schema;

// Re-export Delta Lake types at module level.
pub use delta::DeltaLakeSink;
pub use delta_config::{DeltaCatalogType, DeltaLakeSinkConfig, DeltaWriteMode};
#[cfg(feature = "delta-lake")]
pub use delta_lookup::{DeltaLookupSource, DeltaLookupSourceConfig};
pub use delta_metrics::DeltaLakeSinkMetrics;
#[cfg(feature = "delta-lake")]
pub use delta_reference::DeltaReferenceTableSource;
pub use delta_source::DeltaSource;
pub use delta_source_config::DeltaSourceConfig;
pub use metrics::LakehouseSinkMetrics;

// Re-export Iceberg types at module level.
pub use iceberg::IcebergSink;
use iceberg_config::{
    sink_config_keys as iceberg_sink_config_keys, source_config_keys as iceberg_source_config_keys,
};
pub use iceberg_config::{
    IcebergCatalogAuthType, IcebergCatalogConfig, IcebergCatalogType, IcebergNullOrder,
    IcebergPartitionField, IcebergReadBootstrap, IcebergReadMode, IcebergSchemaEvolutionMode,
    IcebergSinkConfig, IcebergSortDirection, IcebergSortField, IcebergSourceConfig,
    IcebergStorageConfig, IcebergStorageEncryption, IcebergStorageType, IcebergTransform,
    IcebergWriteDistributionMode, IcebergWriteMode,
};
#[cfg(feature = "iceberg-core")]
pub use iceberg_lookup::{IcebergLookupSource, IcebergLookupSourceConfig};
#[cfg(feature = "iceberg-core")]
pub use iceberg_reference::IcebergReferenceTableSource;
pub use iceberg_source::IcebergSource;

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the Delta Lake sink connector with the given registry.
///
/// # Errors
///
/// Returns the registry error when a name is already registered or the registry is frozen.
pub fn register_delta_lake_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|config, registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(DeltaLakeSink::new(
                DeltaLakeSinkConfig::from_config(config)?,
                registry.map(Arc::as_ref),
            )))
        }),
    )
}

/// Registers the Delta Lake source connector with the given registry.
///
/// # Errors
///
/// Returns the registry error when a name is already registered or the registry is frozen.
pub fn register_delta_lake_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(DeltaSource::new(
                DeltaSourceConfig::default(),
                registry.map(Arc::as_ref),
            )))
        }),
    )?;

    // Register finite startup snapshots for replicated reference tables.
    #[cfg(feature = "delta-lake")]
    registry.register_table_source(
        "delta-lake",
        info.clone(),
        Arc::new(|config, declared_schema| {
            Ok(Box::new(DeltaReferenceTableSource::from_connector_config(
                config,
                declared_schema,
            )?))
        }),
    )?;

    // Register lookup source factory for on-demand/partial cache mode.
    #[cfg(feature = "delta-lake")]
    registry.register_lookup_source("delta-lake", info, Arc::new(DeltaLookupFactory))?;

    Ok(())
}

#[cfg(feature = "delta-lake")]
struct DeltaLookupFactory;

#[cfg(feature = "delta-lake")]
#[async_trait::async_trait]
impl crate::registry::LookupSourceFactory for DeltaLookupFactory {
    async fn build(
        &self,
        config: crate::config::ConnectorConfig,
        _declared_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, crate::error::ConnectorError>
    {
        use crate::lakehouse::delta_source_config::DeltaSourceConfig;
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

        let (resolved_path, resolved_opts) = crate::lakehouse::delta_io::resolve_catalog_options(
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

        // `From<LookupError>` preserves transient/non-transient class.
        let source = DeltaLookupSource::open(lookup_config).await?;

        Ok(Arc::new(source) as Arc<dyn laminar_core::lookup::source::LookupSourceDyn>)
    }
}

/// Registers the Iceberg sink connector with the given registry.
///
/// # Errors
///
/// Returns the registry error when a name is already registered or the registry is frozen.
pub fn register_iceberg_sink(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|config, registry: Option<&Arc<prometheus::Registry>>| {
            Ok(Box::new(IcebergSink::new(
                IcebergSinkConfig::from_config(config)?,
                registry.map(Arc::as_ref),
            )))
        }),
    )
}

/// Registers the Iceberg source connector with the given registry.
///
/// # Errors
///
/// Returns the registry error when a name is already registered or the registry is frozen.
pub fn register_iceberg_source(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
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
        Arc::new(|registry: Option<&Arc<prometheus::Registry>>| {
            let mut config = crate::config::ConnectorConfig::new("iceberg");
            config.set("catalog.uri", "http://localhost:8181");
            config.set("catalog.warehouse", "s3://default/wh");
            config.set("namespace", "default");
            config.set("table.name", "default");
            Ok(Box::new(IcebergSource::new(
                IcebergSourceConfig::from_config(&config)?,
                registry.map(Arc::as_ref),
            )))
        }),
    )?;

    // Register finite startup snapshots for replicated reference tables.
    #[cfg(feature = "iceberg-core")]
    registry.register_table_source(
        "iceberg",
        info.clone(),
        Arc::new(|config, declared_schema| {
            Ok(Box::new(
                IcebergReferenceTableSource::from_connector_config(config, declared_schema)?,
            ))
        }),
    )?;

    // Register lookup source factory for on-demand/partial cache mode.
    #[cfg(feature = "iceberg-core")]
    registry.register_lookup_source("iceberg", info, Arc::new(IcebergLookupFactory))?;

    Ok(())
}

#[cfg(feature = "iceberg-core")]
struct IcebergLookupFactory;

#[cfg(feature = "iceberg-core")]
#[async_trait::async_trait]
impl crate::registry::LookupSourceFactory for IcebergLookupFactory {
    async fn build(
        &self,
        config: crate::config::ConnectorConfig,
        _declared_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<Arc<dyn laminar_core::lookup::source::LookupSourceDyn>, crate::error::ConnectorError>
    {
        use crate::lakehouse::iceberg_config::{IcebergCatalogConfig, IcebergStorageConfig};
        let pk_columns: Vec<String> = config
            .get("_primary_key_columns")
            .unwrap_or("")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if pk_columns.is_empty() {
            return Err(crate::error::ConnectorError::ConfigurationError(
                "iceberg lookup source requires primary key columns".into(),
            ));
        }

        let catalog = IcebergCatalogConfig::from_config(&config)?;
        let storage = IcebergStorageConfig::from_config(&config)?;
        let lookup_config = IcebergLookupSourceConfig {
            catalog,
            storage,
            primary_key_columns: pk_columns,
        };

        let source = IcebergLookupSource::open(lookup_config).await?;
        Ok(Arc::new(source) as Arc<dyn laminar_core::lookup::source::LookupSourceDyn>)
    }
}

/// Registers all lakehouse sink connectors (Delta Lake, Iceberg).
///
/// # Errors
///
/// Returns the first registry error.
pub fn register_lakehouse_sinks(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    register_delta_lake_sink(registry)?;
    register_iceberg_sink(registry)
}

/// Registers all lakehouse source connectors.
///
/// # Errors
///
/// Returns the first registry error.
pub fn register_lakehouse_sources(
    registry: &ConnectorRegistry,
) -> Result<(), crate::error::ConnectorError> {
    register_delta_lake_source(registry)?;
    register_iceberg_source(registry)
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
            "First version to read (default: only versions committed after startup)",
            "",
        ),
        ConfigKeySpec::optional(
            "poll.interval.ms",
            "How often to poll for new versions (ms)",
            "1000",
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

#[cfg(test)]
mod tests;
