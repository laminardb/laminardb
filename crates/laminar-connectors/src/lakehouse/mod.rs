//! Lakehouse connectors (Delta Lake, Iceberg).
//!
//! Writes Arrow `RecordBatch` data to lakehouse table formats with
//! ACID transactions and exactly-once semantics.
//!
//! # Architecture
//!
//! ```text
//! Ring 0 (Hot Path):  SPSC push only (~5ns, zero sink code)
//! Ring 1 (Background): Batch buffering -> Parquet writes -> transaction commits
//! Ring 2 (Control):    Schema management, configuration, health checks
//! ```
//!
//! # Module Structure
//!
//! ## Delta Lake
//! - `delta` - `DeltaLakeSink` implementing `SinkConnector`
//! - `delta_config` - Configuration and enums
//! - `delta_metrics` - Lock-free atomic metrics
//!
//! ## Apache Iceberg
//! - `iceberg` - `IcebergSink` implementing `SinkConnector`
//! - `iceberg_config` - Configuration, catalog types, partition transforms
//! - `iceberg_metrics` - Lock-free atomic metrics
//!
//! # Usage
//!
//! ## Delta Lake
//!
//! ```rust,ignore
//! use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig, DeltaWriteMode};
//!
//! let config = DeltaLakeSinkConfig {
//!     table_path: "s3://data-lake/trades/".to_string(),
//!     write_mode: DeltaWriteMode::Append,
//!     partition_columns: vec!["trade_date".to_string()],
//!     ..Default::default()
//! };
//!
//! let sink = DeltaLakeSink::new(config);
//! ```
//!
//! ## Apache Iceberg
//!
//! ```rust,ignore
//! use laminar_connectors::lakehouse::{IcebergSink, IcebergSinkConfig, IcebergWriteMode};
//!
//! let config = IcebergSinkConfig {
//!     warehouse: "s3://data-lake/warehouse".to_string(),
//!     namespace: vec!["analytics".to_string(), "prod".to_string()],
//!     table_name: "trades".to_string(),
//!     write_mode: IcebergWriteMode::Append,
//!     ..Default::default()
//! };
//!
//! let sink = IcebergSink::new(config);
//! ```

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

// Apache Iceberg modules
pub mod iceberg;
pub mod iceberg_config;
pub mod iceberg_metrics;

// Common metrics
pub mod metrics;

// Re-export Delta Lake types at module level.
pub use delta::DeltaLakeSink;
pub use delta_config::{
    CompactionConfig, DeliveryGuarantee, DeltaCatalogType, DeltaLakeSinkConfig, DeltaWriteMode,
};
pub use delta_metrics::DeltaLakeSinkMetrics;
pub use delta_source::DeltaSource;
pub use delta_source_config::DeltaSourceConfig;

// Re-export Iceberg types at module level.
pub use iceberg::IcebergSink;
pub use iceberg_config::{
    IcebergCatalogType, IcebergFileFormat, IcebergPartitionField, IcebergSinkConfig,
    IcebergTransform, IcebergWriteMode, MaintenanceConfig, NullOrder, SortDirection, SortField,
};
pub use iceberg_metrics::IcebergSinkMetrics;
pub use metrics::LakehouseSinkMetrics;

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
        Arc::new(|| Box::new(DeltaLakeSink::new(DeltaLakeSinkConfig::default()))),
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
        info,
        Arc::new(|| Box::new(DeltaSource::new(DeltaSourceConfig::default()))),
    );
}

/// Registers the Apache Iceberg sink connector with the given registry.
pub fn register_iceberg_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "iceberg".to_string(),
        display_name: "Apache Iceberg Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: iceberg_config_keys(),
    };

    registry.register_sink(
        "iceberg",
        info,
        Arc::new(|| Box::new(IcebergSink::new(IcebergSinkConfig::default()))),
    );
}

/// Registers all lakehouse sink connectors (Delta Lake, Iceberg).
pub fn register_lakehouse_sinks(registry: &ConnectorRegistry) {
    register_delta_lake_sink(registry);
    register_iceberg_sink(registry);
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
            "exactly-once",
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
            "compaction.min-files",
            "Minimum files before triggering compaction",
            "10",
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
            "catalog.prop.*",
            "Catalog-specific properties (pass-through)",
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
            "catalog.prop.*",
            "Catalog-specific properties (pass-through)",
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

#[allow(clippy::too_many_lines)]
fn iceberg_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        // ── Required keys ──
        ConfigKeySpec::required(
            "warehouse",
            "Warehouse location (e.g., s3://bucket/warehouse)",
        ),
        ConfigKeySpec::required(
            "namespace",
            "Dot-separated namespace (e.g., analytics.prod)",
        ),
        ConfigKeySpec::required("table.name", "Table name within the namespace"),
        // ── Catalog configuration ──
        ConfigKeySpec::optional(
            "catalog.type",
            "Catalog type: rest, glue, hive, memory",
            "rest",
        ),
        ConfigKeySpec::optional(
            "catalog.uri",
            "Catalog URI (REST endpoint, Glue ARN, or Hive thrift URI)",
            "",
        ),
        // ── Partitioning ──
        ConfigKeySpec::optional(
            "partition.spec",
            "Partition transforms, e.g., DAYS(event_time), BUCKET(16, user_id)",
            "",
        ),
        // ── Buffering & file management ──
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
            "file.format",
            "Data file format: parquet, orc, avro",
            "parquet",
        ),
        // ── Write mode ──
        ConfigKeySpec::optional("write.mode", "Write mode: append, upsert", "append"),
        ConfigKeySpec::optional(
            "equality.delete.columns",
            "Columns for equality deletes in upsert mode (required for upsert)",
            "",
        ),
        // ── Schema & delivery ──
        ConfigKeySpec::optional(
            "schema.evolution",
            "Enable automatic schema evolution (additive columns)",
            "false",
        ),
        ConfigKeySpec::optional(
            "delivery.guarantee",
            "exactly-once or at-least-once",
            "exactly-once",
        ),
        ConfigKeySpec::optional(
            "writer.id",
            "Writer ID for exactly-once deduplication (auto UUID if not set)",
            "",
        ),
        // ── Maintenance ──
        ConfigKeySpec::optional(
            "maintenance.compaction",
            "Enable background file compaction",
            "true",
        ),
        ConfigKeySpec::optional(
            "maintenance.compaction.min-files",
            "Minimum files before triggering compaction",
            "10",
        ),
        ConfigKeySpec::optional(
            "maintenance.expire-snapshots",
            "Enable automatic snapshot expiry",
            "true",
        ),
        ConfigKeySpec::optional(
            "maintenance.snapshot-retention.hours",
            "Hours to retain snapshots before expiry",
            "168",
        ),
        ConfigKeySpec::optional(
            "maintenance.max-snapshots",
            "Maximum number of snapshots to retain",
            "100",
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
        // ── Catalog-specific properties ──
        ConfigKeySpec::optional(
            "catalog.prop.*",
            "Catalog-specific properties (e.g., catalog.prop.token for REST)",
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
        assert!(optional.contains(&"catalog.prop.*"));
    }

    #[test]
    fn test_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_delta_lake_sink(&registry);

        let config = crate::config::ConnectorConfig::new("delta-lake");
        let sink = registry.create_sink(&config);
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
        let source = registry.create_source(&config);
        assert!(source.is_ok());
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
        assert_eq!(info.display_name, "Apache Iceberg Sink");
        assert!(info.is_sink);
        assert!(!info.is_source);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_iceberg_config_keys_required() {
        let keys = iceberg_config_keys();
        let required: Vec<&str> = keys
            .iter()
            .filter(|k| k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(required.contains(&"warehouse"));
        assert!(required.contains(&"namespace"));
        assert!(required.contains(&"table.name"));
        assert_eq!(required.len(), 3);
    }

    #[test]
    fn test_iceberg_config_keys_include_cloud_storage() {
        let keys = iceberg_config_keys();
        let key_names: Vec<&str> = keys.iter().map(|k| k.key.as_str()).collect();
        assert!(key_names.contains(&"storage.aws_access_key_id"));
        assert!(key_names.contains(&"storage.aws_secret_access_key"));
        assert!(key_names.contains(&"storage.aws_region"));
        assert!(key_names.contains(&"storage.azure_storage_account_name"));
        assert!(key_names.contains(&"storage.azure_storage_account_key"));
        assert!(key_names.contains(&"storage.google_service_account_path"));
    }

    #[test]
    fn test_iceberg_config_keys_optional_present() {
        let keys = iceberg_config_keys();
        let optional: Vec<&str> = keys
            .iter()
            .filter(|k| !k.required)
            .map(|k| k.key.as_str())
            .collect();
        assert!(optional.contains(&"catalog.type"));
        assert!(optional.contains(&"catalog.uri"));
        assert!(optional.contains(&"partition.spec"));
        assert!(optional.contains(&"target.file.size"));
        assert!(optional.contains(&"write.mode"));
        assert!(optional.contains(&"equality.delete.columns"));
        assert!(optional.contains(&"delivery.guarantee"));
        assert!(optional.contains(&"schema.evolution"));
        assert!(optional.contains(&"file.format"));
        assert!(optional.contains(&"maintenance.compaction"));
        assert!(optional.contains(&"maintenance.expire-snapshots"));
        assert!(optional.contains(&"maintenance.snapshot-retention.hours"));
        assert!(optional.contains(&"maintenance.max-snapshots"));
        assert!(optional.contains(&"writer.id"));
    }

    #[test]
    fn test_iceberg_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_iceberg_sink(&registry);

        let config = crate::config::ConnectorConfig::new("iceberg");
        let sink = registry.create_sink(&config);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_register_lakehouse_sinks() {
        let registry = ConnectorRegistry::new();
        register_lakehouse_sinks(&registry);

        assert!(registry.sink_info("delta-lake").is_some());
        assert!(registry.sink_info("iceberg").is_some());
    }
}
