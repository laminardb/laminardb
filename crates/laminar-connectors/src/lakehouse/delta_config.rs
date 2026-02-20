//! Delta Lake sink connector configuration.
//!
//! [`DeltaLakeSinkConfig`] encapsulates all settings for writing Arrow
//! `RecordBatch` data to Delta Lake tables, parsed from SQL `WITH (...)`
//! clauses via [`from_config`](DeltaLakeSinkConfig::from_config).

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::storage::{
    CloudConfigValidator, ResolvedStorageOptions, SecretMasker, StorageCredentialResolver,
    StorageProvider,
};

/// Configuration for the Delta Lake sink connector.
///
/// Parsed from SQL `WITH (...)` clause options or constructed programmatically.
#[derive(Debug, Clone)]
pub struct DeltaLakeSinkConfig {
    /// Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`).
    pub table_path: String,

    /// Columns to partition by (e.g., `["trade_date", "hour"]`).
    pub partition_columns: Vec<String>,

    /// Target Parquet file size in bytes (default: 128 MB).
    pub target_file_size: usize,

    /// Maximum number of records to buffer before flushing to Parquet.
    pub max_buffer_records: usize,

    /// Maximum time to buffer records before flushing.
    pub max_buffer_duration: Duration,

    /// Delta Lake checkpoint interval (create checkpoint every N commits).
    pub checkpoint_interval: u64,

    /// Whether to enable schema evolution (auto-merge new columns).
    pub schema_evolution: bool,

    /// Write mode: Append, Overwrite, or Upsert (CDC merge).
    pub write_mode: DeltaWriteMode,

    /// Key columns for upsert/merge operations (required for Upsert mode).
    pub merge_key_columns: Vec<String>,

    /// Storage options (S3 credentials, Azure keys, etc.).
    pub storage_options: HashMap<String, String>,

    /// Compaction configuration.
    pub compaction: CompactionConfig,

    /// Vacuum retention period for old files.
    pub vacuum_retention: Duration,

    /// Delivery guarantee: `AtLeastOnce` or `ExactlyOnce`.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Writer ID for exactly-once deduplication.
    pub writer_id: String,

    /// Catalog type for table discovery.
    pub catalog_type: DeltaCatalogType,

    /// Catalog database name (required for Glue).
    pub catalog_database: Option<String>,

    /// Catalog name (required for Unity).
    pub catalog_name: Option<String>,

    /// Catalog schema name (required for Unity).
    pub catalog_schema: Option<String>,

    /// Additional catalog-specific properties.
    pub catalog_properties: HashMap<String, String>,
}

impl Default for DeltaLakeSinkConfig {
    fn default() -> Self {
        Self {
            table_path: String::new(),
            partition_columns: Vec::new(),
            target_file_size: 128 * 1024 * 1024, // 128 MB
            max_buffer_records: 100_000,
            max_buffer_duration: Duration::from_secs(60),
            checkpoint_interval: 10,
            schema_evolution: false,
            write_mode: DeltaWriteMode::Append,
            merge_key_columns: Vec::new(),
            storage_options: HashMap::new(),
            compaction: CompactionConfig::default(),
            vacuum_retention: Duration::from_secs(7 * 24 * 3600), // 7 days
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
            writer_id: uuid::Uuid::new_v4().to_string(),
            catalog_type: DeltaCatalogType::None,
            catalog_database: None,
            catalog_name: None,
            catalog_schema: None,
            catalog_properties: HashMap::new(),
        }
    }
}

impl DeltaLakeSinkConfig {
    /// Creates a minimal config for testing.
    #[must_use]
    pub fn new(table_path: &str) -> Self {
        Self {
            table_path: table_path.to_string(),
            ..Default::default()
        }
    }

    /// Parses a sink config from a [`ConnectorConfig`] (SQL WITH clause).
    ///
    /// # Required keys
    ///
    /// - `table.path` - Path to Delta Lake table
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if required keys are absent,
    /// or `ConnectorError::ConfigurationError` on invalid values.
    #[allow(clippy::too_many_lines)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            table_path: config.require("table.path")?.to_string(),
            ..Self::default()
        };

        if let Some(v) = config.get("partition.columns") {
            cfg.partition_columns = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        if let Some(v) = config.get("target.file.size") {
            cfg.target_file_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid target.file.size: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("max.buffer.records") {
            cfg.max_buffer_records = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid max.buffer.records: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("max.buffer.duration.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid max.buffer.duration.ms: '{v}'"))
            })?;
            cfg.max_buffer_duration = Duration::from_millis(ms);
        }
        if let Some(v) = config.get("checkpoint.interval") {
            cfg.checkpoint_interval = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid checkpoint.interval: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("schema.evolution") {
            cfg.schema_evolution = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("write.mode") {
            cfg.write_mode = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid write.mode: '{v}' (expected 'append', 'overwrite', or 'upsert')"
                ))
            })?;
        }
        if let Some(v) = config.get("merge.key.columns") {
            cfg.merge_key_columns = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        if let Some(v) = config.get("delivery.guarantee") {
            cfg.delivery_guarantee = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid delivery.guarantee: '{v}' \
                     (expected 'exactly-once' or 'at-least-once')"
                ))
            })?;
        }
        if let Some(v) = config.get("compaction.enabled") {
            cfg.compaction.enabled = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("compaction.z-order.columns") {
            cfg.compaction.z_order_columns = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        if let Some(v) = config.get("compaction.min-files") {
            cfg.compaction.min_files_for_compaction = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid compaction.min-files: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("vacuum.retention.hours") {
            let hours: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid vacuum.retention.hours: '{v}'"))
            })?;
            cfg.vacuum_retention = Duration::from_secs(hours * 3600);
        }
        if let Some(v) = config.get("writer.id") {
            cfg.writer_id = v.to_string();
        }

        // ── Catalog configuration ──
        if let Some(v) = config.get("catalog.type") {
            cfg.catalog_type = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid catalog.type: '{v}' (expected 'none', 'glue', or 'unity')"
                ))
            })?;
        }
        if let Some(v) = config.get("catalog.database") {
            cfg.catalog_database = Some(v.to_string());
        }
        if let Some(v) = config.get("catalog.name") {
            cfg.catalog_name = Some(v.to_string());
        }
        if let Some(v) = config.get("catalog.schema") {
            cfg.catalog_schema = Some(v.to_string());
        }
        // Unity-specific: populate workspace_url and access_token into the enum variant.
        if let DeltaCatalogType::Unity {
            ref mut workspace_url,
            ref mut access_token,
        } = cfg.catalog_type
        {
            if let Some(v) = config.get("catalog.workspace_url") {
                *workspace_url = v.to_string();
            }
            if let Some(v) = config.get("catalog.access_token") {
                *access_token = v.to_string();
            }
        }
        cfg.catalog_properties = config.properties_with_prefix("catalog.prop.");

        // Resolve storage credentials: explicit options + environment variable fallbacks.
        let explicit_storage = config.properties_with_prefix("storage.");
        let resolved = StorageCredentialResolver::resolve(&cfg.table_path, &explicit_storage);
        cfg.storage_options = resolved.options;

        cfg.validate()?;
        Ok(cfg)
    }

    /// Formats the storage options for safe logging with secrets redacted.
    #[must_use]
    pub fn display_storage_options(&self) -> String {
        SecretMasker::display_map(&self.storage_options)
    }

    /// Validates the configuration for consistency.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid combinations.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.table_path.is_empty() {
            return Err(ConnectorError::MissingConfig("table.path".into()));
        }
        if self.write_mode == DeltaWriteMode::Upsert && self.merge_key_columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "upsert mode requires 'merge.key.columns' to be set".into(),
            ));
        }
        if self.max_buffer_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.buffer.records must be > 0".into(),
            ));
        }
        if self.target_file_size == 0 {
            return Err(ConnectorError::ConfigurationError(
                "target.file.size must be > 0".into(),
            ));
        }
        if self.checkpoint_interval == 0 {
            return Err(ConnectorError::ConfigurationError(
                "checkpoint.interval must be > 0".into(),
            ));
        }

        // Validate catalog-specific requirements.
        match &self.catalog_type {
            DeltaCatalogType::None => {}
            DeltaCatalogType::Glue => {
                if self.catalog_database.is_none() {
                    return Err(ConnectorError::ConfigurationError(
                        "Glue catalog requires 'catalog.database' to be set".into(),
                    ));
                }
            }
            DeltaCatalogType::Unity {
                workspace_url,
                access_token,
            } => {
                if workspace_url.is_empty() {
                    return Err(ConnectorError::ConfigurationError(
                        "Unity catalog requires 'catalog.workspace_url' to be set".into(),
                    ));
                }
                if access_token.is_empty() {
                    return Err(ConnectorError::ConfigurationError(
                        "Unity catalog requires 'catalog.access_token' to be set".into(),
                    ));
                }
                if self.catalog_name.is_none() {
                    return Err(ConnectorError::ConfigurationError(
                        "Unity catalog requires 'catalog.name' to be set".into(),
                    ));
                }
                if self.catalog_schema.is_none() {
                    return Err(ConnectorError::ConfigurationError(
                        "Unity catalog requires 'catalog.schema' to be set".into(),
                    ));
                }
            }
        }

        // Validate cloud storage credentials for the detected provider.
        let resolved = ResolvedStorageOptions {
            provider: StorageProvider::detect(&self.table_path),
            options: self.storage_options.clone(),
            env_resolved_keys: Vec::new(),
        };
        let cloud_result = CloudConfigValidator::validate(&resolved);
        if !cloud_result.is_valid() {
            return Err(ConnectorError::ConfigurationError(
                cloud_result.error_message(),
            ));
        }

        Ok(())
    }
}

/// Delta Lake write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaWriteMode {
    /// Append-only: all records are inserts. Most efficient for immutable streams.
    Append,
    /// Overwrite: replace partition contents. Used for batch-style recomputation.
    Overwrite,
    /// Upsert/Merge: CDC-style insert/update/delete via MERGE statement.
    /// Requires `merge_key_columns` to be set. Integrates with Z-sets.
    Upsert,
}

impl FromStr for DeltaWriteMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "append" => Ok(Self::Append),
            "overwrite" => Ok(Self::Overwrite),
            "upsert" | "merge" => Ok(Self::Upsert),
            other => Err(format!("unknown write mode: '{other}'")),
        }
    }
}

impl fmt::Display for DeltaWriteMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Append => write!(f, "append"),
            Self::Overwrite => write!(f, "overwrite"),
            Self::Upsert => write!(f, "upsert"),
        }
    }
}

/// Delivery guarantee level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// May produce duplicates on recovery. Simpler, slightly higher throughput.
    AtLeastOnce,
    /// No duplicates. Uses epoch-to-Delta-version mapping.
    ExactlyOnce,
}

impl FromStr for DeliveryGuarantee {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "at_least_once" | "atleastonce" => Ok(Self::AtLeastOnce),
            "exactly_once" | "exactlyonce" => Ok(Self::ExactlyOnce),
            other => Err(format!("unknown delivery guarantee: '{other}'")),
        }
    }
}

impl fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtLeastOnce => write!(f, "at-least-once"),
            Self::ExactlyOnce => write!(f, "exactly-once"),
        }
    }
}

/// Delta Lake catalog type for table discovery.
///
/// Catalogs enable referencing tables by logical names instead of raw paths.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DeltaCatalogType {
    /// No catalog — table path is a direct file or cloud URI.
    #[default]
    None,
    /// AWS Glue Data Catalog.
    Glue,
    /// Databricks Unity Catalog.
    Unity {
        /// Databricks workspace URL (e.g., `https://xxx.cloud.databricks.com`).
        workspace_url: String,
        /// Databricks access token.
        access_token: String,
    },
}

impl FromStr for DeltaCatalogType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" | "" => Ok(Self::None),
            "glue" => Ok(Self::Glue),
            "unity" => Ok(Self::Unity {
                workspace_url: String::new(),
                access_token: String::new(),
            }),
            other => Err(format!("unknown catalog type: '{other}'")),
        }
    }
}

impl fmt::Display for DeltaCatalogType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Glue => write!(f, "glue"),
            Self::Unity { .. } => write!(f, "unity"),
        }
    }
}

/// Configuration for background compaction.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Whether to run automatic compaction.
    pub enabled: bool,

    /// Minimum number of files before triggering compaction.
    pub min_files_for_compaction: usize,

    /// Target file size after compaction.
    pub target_file_size: usize,

    /// Columns for Z-ORDER clustering (optional).
    pub z_order_columns: Vec<String>,

    /// How often to check if compaction is needed.
    pub check_interval: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_files_for_compaction: 10,
            target_file_size: 128 * 1024 * 1024, // 128 MB
            z_order_columns: Vec::new(),
            check_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("delta-lake");
        for (k, v) in pairs {
            config.set(*k, *v);
        }
        config
    }

    fn required_pairs() -> Vec<(&'static str, &'static str)> {
        vec![("table.path", "/data/warehouse/trades")]
    }

    // ── Config parsing tests ──

    #[test]
    fn test_parse_required_fields() {
        let config = make_config(&required_pairs());
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.table_path, "/data/warehouse/trades");
        assert_eq!(cfg.write_mode, DeltaWriteMode::Append);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
        assert!(cfg.partition_columns.is_empty());
        assert!(cfg.merge_key_columns.is_empty());
        assert_eq!(cfg.target_file_size, 128 * 1024 * 1024);
        assert_eq!(cfg.max_buffer_records, 100_000);
        assert_eq!(cfg.checkpoint_interval, 10);
        assert!(!cfg.schema_evolution);
        assert!(cfg.compaction.enabled);
    }

    #[test]
    fn test_missing_table_path() {
        let config = ConnectorConfig::new("delta-lake");
        assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_parse_all_optional_fields() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("partition.columns", "trade_date, hour"),
            ("target.file.size", "67108864"),
            ("max.buffer.records", "50000"),
            ("max.buffer.duration.ms", "30000"),
            ("checkpoint.interval", "20"),
            ("schema.evolution", "true"),
            ("write.mode", "upsert"),
            ("merge.key.columns", "customer_id, order_id"),
            ("delivery.guarantee", "at-least-once"),
            ("compaction.enabled", "true"),
            ("compaction.z-order.columns", "customer_id, product_id"),
            ("compaction.min-files", "20"),
            ("vacuum.retention.hours", "336"),
            ("writer.id", "my-writer"),
            ("storage.aws_access_key_id", "AKID123"),
            ("storage.aws_region", "us-east-1"),
        ]);
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();

        assert_eq!(cfg.partition_columns, vec!["trade_date", "hour"]);
        assert_eq!(cfg.target_file_size, 67_108_864);
        assert_eq!(cfg.max_buffer_records, 50_000);
        assert_eq!(cfg.max_buffer_duration, Duration::from_millis(30_000));
        assert_eq!(cfg.checkpoint_interval, 20);
        assert!(cfg.schema_evolution);
        assert_eq!(cfg.write_mode, DeltaWriteMode::Upsert);
        assert_eq!(cfg.merge_key_columns, vec!["customer_id", "order_id"]);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
        assert!(cfg.compaction.enabled);
        assert_eq!(
            cfg.compaction.z_order_columns,
            vec!["customer_id", "product_id"]
        );
        assert_eq!(cfg.compaction.min_files_for_compaction, 20);
        assert_eq!(cfg.vacuum_retention, Duration::from_secs(336 * 3600));
        assert_eq!(cfg.writer_id, "my-writer");
        assert_eq!(
            cfg.storage_options.get("aws_access_key_id"),
            Some(&"AKID123".to_string())
        );
        assert_eq!(
            cfg.storage_options.get("aws_region"),
            Some(&"us-east-1".to_string())
        );
    }

    #[test]
    fn test_upsert_requires_merge_key() {
        let mut pairs = required_pairs();
        pairs.push(("write.mode", "upsert"));
        let config = make_config(&pairs);
        let result = DeltaLakeSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("merge.key.columns"), "error: {err}");
    }

    #[test]
    fn test_empty_table_path_rejected() {
        let mut cfg = DeltaLakeSinkConfig::default();
        cfg.table_path = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_zero_max_buffer_records_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("max.buffer.records", "0"));
        let config = make_config(&pairs);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_zero_target_file_size_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("target.file.size", "0"));
        let config = make_config(&pairs);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_zero_checkpoint_interval_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("checkpoint.interval", "0"));
        let config = make_config(&pairs);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_invalid_target_file_size() {
        let mut pairs = required_pairs();
        pairs.push(("target.file.size", "abc"));
        let config = make_config(&pairs);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_invalid_write_mode() {
        let mut pairs = required_pairs();
        pairs.push(("write.mode", "unknown"));
        let config = make_config(&pairs);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_storage_options_prefix_stripping() {
        let mut pairs = required_pairs();
        pairs.push(("storage.aws_access_key_id", "AKID"));
        pairs.push(("storage.aws_secret_access_key", "SECRET"));
        pairs.push(("table.path", "/data/test"));
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();

        assert_eq!(cfg.storage_options.len(), 2);
        assert!(cfg.storage_options.contains_key("aws_access_key_id"));
        assert!(cfg.storage_options.contains_key("aws_secret_access_key"));
        assert!(!cfg
            .storage_options
            .contains_key("storage.aws_access_key_id"));
    }

    #[test]
    fn test_defaults() {
        let cfg = DeltaLakeSinkConfig::default();
        assert!(cfg.table_path.is_empty());
        assert_eq!(cfg.target_file_size, 128 * 1024 * 1024);
        assert_eq!(cfg.max_buffer_records, 100_000);
        assert_eq!(cfg.max_buffer_duration, Duration::from_secs(60));
        assert_eq!(cfg.checkpoint_interval, 10);
        assert!(!cfg.schema_evolution);
        assert_eq!(cfg.write_mode, DeltaWriteMode::Append);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
        assert!(!cfg.writer_id.is_empty());
    }

    #[test]
    fn test_new_helper() {
        let cfg = DeltaLakeSinkConfig::new("/tmp/test_table");
        assert_eq!(cfg.table_path, "/tmp/test_table");
        assert_eq!(cfg.write_mode, DeltaWriteMode::Append);
    }

    // ── Enum tests ──

    #[test]
    fn test_write_mode_parse() {
        assert_eq!(
            "append".parse::<DeltaWriteMode>().unwrap(),
            DeltaWriteMode::Append
        );
        assert_eq!(
            "overwrite".parse::<DeltaWriteMode>().unwrap(),
            DeltaWriteMode::Overwrite
        );
        assert_eq!(
            "upsert".parse::<DeltaWriteMode>().unwrap(),
            DeltaWriteMode::Upsert
        );
        assert_eq!(
            "merge".parse::<DeltaWriteMode>().unwrap(),
            DeltaWriteMode::Upsert
        );
        assert!("unknown".parse::<DeltaWriteMode>().is_err());
    }

    #[test]
    fn test_write_mode_display() {
        assert_eq!(DeltaWriteMode::Append.to_string(), "append");
        assert_eq!(DeltaWriteMode::Overwrite.to_string(), "overwrite");
        assert_eq!(DeltaWriteMode::Upsert.to_string(), "upsert");
    }

    #[test]
    fn test_delivery_guarantee_parse() {
        assert_eq!(
            "at-least-once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            "at_least_once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            "exactly-once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::ExactlyOnce
        );
        assert_eq!(
            "exactly_once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::ExactlyOnce
        );
        assert!("unknown".parse::<DeliveryGuarantee>().is_err());
    }

    #[test]
    fn test_delivery_guarantee_display() {
        assert_eq!(DeliveryGuarantee::AtLeastOnce.to_string(), "at-least-once");
        assert_eq!(DeliveryGuarantee::ExactlyOnce.to_string(), "exactly-once");
    }

    #[test]
    fn test_compaction_config_defaults() {
        let cfg = CompactionConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.min_files_for_compaction, 10);
        assert_eq!(cfg.target_file_size, 128 * 1024 * 1024);
        assert!(cfg.z_order_columns.is_empty());
        assert_eq!(cfg.check_interval, Duration::from_secs(300));
    }

    #[test]
    fn test_partition_columns_empty_filter() {
        let mut pairs = required_pairs();
        pairs.push(("partition.columns", "a,,b, ,c"));
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.partition_columns, vec!["a", "b", "c"]);
    }

    // ── Cloud storage integration tests ──

    #[test]
    fn test_s3_path_requires_region() {
        let config = make_config(&[("table.path", "s3://my-bucket/trades")]);
        let result = DeltaLakeSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("aws_region"), "error: {err}");
    }

    #[test]
    fn test_s3_path_with_region_and_credentials() {
        let config = make_config(&[
            ("table.path", "s3://my-bucket/trades"),
            ("storage.aws_region", "us-east-1"),
            ("storage.aws_access_key_id", "AKID123"),
            ("storage.aws_secret_access_key", "SECRET"),
        ]);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.storage_options["aws_region"], "us-east-1");
        assert_eq!(cfg.storage_options["aws_access_key_id"], "AKID123");
    }

    #[test]
    fn test_s3_path_with_region_only_warns_no_error() {
        // Missing credentials is a warning (IAM fallback), not a hard error.
        let config = make_config(&[
            ("table.path", "s3://my-bucket/trades"),
            ("storage.aws_region", "us-east-1"),
        ]);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
    }

    #[test]
    fn test_s3_path_access_key_without_secret_errors() {
        let config = make_config(&[
            ("table.path", "s3://my-bucket/trades"),
            ("storage.aws_region", "us-east-1"),
            ("storage.aws_access_key_id", "AKID123"),
        ]);
        let result = DeltaLakeSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("aws_secret_access_key"), "error: {err}");
    }

    #[test]
    fn test_azure_path_requires_account_name() {
        let config = make_config(&[("table.path", "az://my-container/trades")]);
        let result = DeltaLakeSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("azure_storage_account_name"), "error: {err}");
    }

    #[test]
    fn test_azure_path_with_account_name_and_key() {
        let config = make_config(&[
            ("table.path", "az://my-container/trades"),
            ("storage.azure_storage_account_name", "myaccount"),
            ("storage.azure_storage_account_key", "base64key=="),
        ]);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
    }

    #[test]
    fn test_gcs_path_always_valid() {
        // GCS missing credentials is warning-only (Application Default Credentials).
        let config = make_config(&[("table.path", "gs://my-bucket/trades")]);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
    }

    #[test]
    fn test_local_path_no_cloud_validation() {
        let config = make_config(&[("table.path", "/data/warehouse/trades")]);
        assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
    }

    #[test]
    fn test_display_storage_options_redacts_secrets() {
        let mut cfg = DeltaLakeSinkConfig::new("s3://bucket/path");
        cfg.storage_options
            .insert("aws_region".to_string(), "us-east-1".to_string());
        cfg.storage_options.insert(
            "aws_secret_access_key".to_string(),
            "TOP_SECRET".to_string(),
        );

        let display = cfg.display_storage_options();
        assert!(display.contains("aws_region=us-east-1"));
        assert!(display.contains("aws_secret_access_key=***"));
        assert!(!display.contains("TOP_SECRET"));
    }

    #[test]
    fn test_display_storage_options_empty() {
        let cfg = DeltaLakeSinkConfig::new("/local/path");
        assert!(cfg.display_storage_options().is_empty());
    }

    // ── Catalog tests ──

    #[test]
    fn test_catalog_type_parse() {
        assert_eq!(
            "none".parse::<DeltaCatalogType>().unwrap(),
            DeltaCatalogType::None
        );
        assert_eq!(
            "glue".parse::<DeltaCatalogType>().unwrap(),
            DeltaCatalogType::Glue
        );
        assert!(matches!(
            "unity".parse::<DeltaCatalogType>().unwrap(),
            DeltaCatalogType::Unity { .. }
        ));
        assert!("unknown".parse::<DeltaCatalogType>().is_err());
    }

    #[test]
    fn test_catalog_type_display() {
        assert_eq!(DeltaCatalogType::None.to_string(), "none");
        assert_eq!(DeltaCatalogType::Glue.to_string(), "glue");
        assert_eq!(
            DeltaCatalogType::Unity {
                workspace_url: "url".into(),
                access_token: "tok".into()
            }
            .to_string(),
            "unity"
        );
    }

    #[test]
    fn test_catalog_none_default() {
        let config = make_config(&required_pairs());
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.catalog_type, DeltaCatalogType::None);
        assert!(cfg.catalog_database.is_none());
        assert!(cfg.catalog_name.is_none());
        assert!(cfg.catalog_schema.is_none());
        assert!(cfg.catalog_properties.is_empty());
    }

    #[test]
    fn test_catalog_glue_valid() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("catalog.type", "glue"),
            ("catalog.database", "my_database"),
        ]);
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.catalog_type, DeltaCatalogType::Glue);
        assert_eq!(cfg.catalog_database.as_deref(), Some("my_database"));
    }

    #[test]
    fn test_catalog_glue_missing_database() {
        let mut pairs = required_pairs();
        pairs.push(("catalog.type", "glue"));
        let config = make_config(&pairs);
        let result = DeltaLakeSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("catalog.database"), "error: {err}");
    }

    #[test]
    fn test_catalog_unity_valid() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("catalog.type", "unity"),
            ("catalog.workspace_url", "https://my.databricks.com"),
            ("catalog.access_token", "dapi123"),
            ("catalog.name", "main"),
            ("catalog.schema", "default"),
        ]);
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert!(matches!(
            cfg.catalog_type,
            DeltaCatalogType::Unity {
                ref workspace_url,
                ref access_token
            }
            if workspace_url == "https://my.databricks.com"
                && access_token == "dapi123"
        ));
        assert_eq!(cfg.catalog_name.as_deref(), Some("main"));
        assert_eq!(cfg.catalog_schema.as_deref(), Some("default"));
    }

    #[test]
    fn test_catalog_unity_missing_workspace_url() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("catalog.type", "unity"),
            ("catalog.access_token", "dapi123"),
            ("catalog.name", "main"),
            ("catalog.schema", "default"),
        ]);
        let config = make_config(&pairs);
        let result = DeltaLakeSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("workspace_url"), "error: {err}");
    }

    #[test]
    fn test_catalog_unity_missing_access_token() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("catalog.type", "unity"),
            ("catalog.workspace_url", "https://my.databricks.com"),
            ("catalog.name", "main"),
            ("catalog.schema", "default"),
        ]);
        let config = make_config(&pairs);
        let result = DeltaLakeSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("access_token"), "error: {err}");
    }

    #[test]
    fn test_catalog_properties_prefix() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("catalog.prop.token", "my_token"),
            ("catalog.prop.warehouse", "my_wh"),
        ]);
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(
            cfg.catalog_properties.get("token"),
            Some(&"my_token".to_string())
        );
        assert_eq!(
            cfg.catalog_properties.get("warehouse"),
            Some(&"my_wh".to_string())
        );
    }
}
