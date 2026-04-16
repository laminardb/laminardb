//! Delta Lake source connector configuration.
//!
//! [`DeltaSourceConfig`] encapsulates all settings for reading Arrow
//! `RecordBatch` data from Delta Lake tables, parsed from SQL `WITH (...)`
//! clauses via [`from_config`](DeltaSourceConfig::from_config).
#![allow(clippy::disallowed_types)] // cold path: lakehouse configuration

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

use super::delta_config::DeltaCatalogType;

/// Read mode for the Delta Lake source.
///
/// Controls whether the source reads full snapshots or incremental changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaReadMode {
    /// Read full table snapshot at each new version.
    ///
    /// Every version change triggers a complete table scan. Useful for
    /// batch-style materialization or tables small enough to re-read.
    Snapshot,
    /// Read data version-by-version for incremental processing.
    ///
    /// Walks versions one-by-one from `current_version + 1` to latest.
    /// Each version's data is emitted exactly once.
    #[default]
    Incremental,
}

impl FromStr for DeltaReadMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "snapshot" | "batch" => Ok(Self::Snapshot),
            "incremental" | "streaming" | "stream" => Ok(Self::Incremental),
            other => Err(format!("unknown read mode: '{other}'")),
        }
    }
}

impl fmt::Display for DeltaReadMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot => write!(f, "snapshot"),
            Self::Incremental => write!(f, "incremental"),
        }
    }
}

/// Action to take when schema evolution is detected across versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaEvolutionAction {
    /// Log a warning and continue with the new schema.
    #[default]
    Warn,
    /// Return an error and stop the source.
    Error,
}

impl FromStr for SchemaEvolutionAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "warn" | "warning" => Ok(Self::Warn),
            "error" | "fail" => Ok(Self::Error),
            other => Err(format!("unknown schema evolution action: '{other}'")),
        }
    }
}

impl fmt::Display for SchemaEvolutionAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Warn => write!(f, "warn"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// Configuration for the Delta Lake source connector.
///
/// Parsed from SQL `WITH (...)` clause options or constructed programmatically.
#[derive(Debug, Clone)]
pub struct DeltaSourceConfig {
    /// Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`).
    pub table_path: String,

    /// Starting version to read from. `None` means start from the latest version.
    pub starting_version: Option<i64>,

    /// How often to poll for new versions (default: 1 second).
    pub poll_interval: Duration,

    /// Read mode: snapshot (full re-read) or incremental (changes only).
    pub read_mode: DeltaReadMode,

    /// Optional partition filter predicate (SQL expression, e.g. `"date = '2024-01-01'"`).
    pub partition_filter: Option<String>,

    /// Action to take on schema evolution between versions.
    pub schema_evolution_action: SchemaEvolutionAction,

    /// Use Change Data Feed for incremental reads (requires CDF on table).
    pub cdf_enabled: bool,

    /// Storage options (S3 credentials, Azure keys, etc.).
    pub storage_options: HashMap<String, String>,

    /// Catalog type for table discovery.
    pub catalog_type: DeltaCatalogType,

    /// Catalog database name (required for Glue).
    pub catalog_database: Option<String>,

    /// Catalog name (required for Unity).
    pub catalog_name: Option<String>,

    /// Catalog schema name (required for Unity).
    pub catalog_schema: Option<String>,
}

impl Default for DeltaSourceConfig {
    fn default() -> Self {
        Self {
            table_path: String::new(),
            starting_version: None,
            poll_interval: Duration::from_secs(1),
            read_mode: DeltaReadMode::default(),
            partition_filter: None,
            schema_evolution_action: SchemaEvolutionAction::default(),
            cdf_enabled: false,
            storage_options: HashMap::new(),
            catalog_type: DeltaCatalogType::None,
            catalog_database: None,
            catalog_name: None,
            catalog_schema: None,
        }
    }
}

impl DeltaSourceConfig {
    /// Creates a minimal config for testing.
    #[must_use]
    pub fn new(table_path: &str) -> Self {
        Self {
            table_path: table_path.to_string(),
            ..Default::default()
        }
    }

    /// Parses a source config from a [`ConnectorConfig`] (SQL WITH clause).
    ///
    /// # Required keys
    ///
    /// - `table.path` - Path to Delta Lake table
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if required keys are absent,
    /// or `ConnectorError::ConfigurationError` on invalid values.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            table_path: config.require("table.path")?.to_string(),
            ..Self::default()
        };

        if let Some(v) = config.get("starting.version") {
            cfg.starting_version = Some(v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid starting.version: '{v}'"))
            })?);
        }
        if let Some(v) = config.get("poll.interval.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid poll.interval.ms: '{v}'"))
            })?;
            cfg.poll_interval = Duration::from_millis(ms);
        }
        if let Some(v) = config.get("read.mode") {
            cfg.read_mode = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid read.mode: '{v}' (expected 'snapshot' or 'incremental')"
                ))
            })?;
        }
        if let Some(v) = config.get("partition.filter") {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                cfg.partition_filter = Some(trimmed.to_string());
            }
        }
        if let Some(v) = config.get("cdf.enabled") {
            cfg.cdf_enabled = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("schema.evolution.action") {
            cfg.schema_evolution_action = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid schema.evolution.action: '{v}' (expected 'warn' or 'error')"
                ))
            })?;
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
        // Resolve storage credentials.
        let explicit_storage = config.properties_with_prefix("storage.");
        let resolved = StorageCredentialResolver::resolve(&cfg.table_path, &explicit_storage);
        cfg.storage_options = resolved.options;

        // Map LogStore configuration keys to delta-rs storage options.
        if let Some(v) = config.get("storage.s3_locking_provider") {
            cfg.storage_options
                .insert("AWS_S3_LOCKING_PROVIDER".to_string(), v.to_string());
        }
        if let Some(v) = config.get("storage.dynamodb_table_name") {
            cfg.storage_options
                .insert("DELTA_DYNAMO_TABLE_NAME".to_string(), v.to_string());
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("delta-lake-source");
        for (k, v) in pairs {
            config.set(*k, *v);
        }
        config
    }

    #[test]
    fn test_defaults() {
        let cfg = DeltaSourceConfig::default();
        assert!(cfg.table_path.is_empty());
        assert!(cfg.starting_version.is_none());
        assert_eq!(cfg.poll_interval, Duration::from_secs(1));
    }

    #[test]
    fn test_new_helper() {
        let cfg = DeltaSourceConfig::new("/tmp/test_table");
        assert_eq!(cfg.table_path, "/tmp/test_table");
    }

    #[test]
    fn test_parse_required_fields() {
        let config = make_config(&[("table.path", "/data/warehouse/trades")]);
        let cfg = DeltaSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.table_path, "/data/warehouse/trades");
        assert!(cfg.starting_version.is_none());
    }

    #[test]
    fn test_missing_table_path() {
        let config = ConnectorConfig::new("delta-lake-source");
        assert!(DeltaSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_parse_optional_fields() {
        let config = make_config(&[
            ("table.path", "/data/test"),
            ("starting.version", "5"),
            ("poll.interval.ms", "500"),
        ]);
        let cfg = DeltaSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.starting_version, Some(5));
        assert_eq!(cfg.poll_interval, Duration::from_millis(500));
    }

    #[test]
    fn test_invalid_starting_version() {
        let config = make_config(&[("table.path", "/data/test"), ("starting.version", "abc")]);
        assert!(DeltaSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_empty_table_path_rejected() {
        let mut cfg = DeltaSourceConfig::default();
        cfg.table_path = String::new();
        assert!(cfg.validate().is_err());
    }

    // ── New config fields tests ──

    #[test]
    fn test_read_mode_defaults_to_incremental() {
        let cfg = DeltaSourceConfig::default();
        assert_eq!(cfg.read_mode, DeltaReadMode::Incremental);
    }

    #[test]
    fn test_read_mode_parse() {
        assert_eq!(
            "snapshot".parse::<DeltaReadMode>().unwrap(),
            DeltaReadMode::Snapshot
        );
        assert_eq!(
            "batch".parse::<DeltaReadMode>().unwrap(),
            DeltaReadMode::Snapshot
        );
        assert_eq!(
            "incremental".parse::<DeltaReadMode>().unwrap(),
            DeltaReadMode::Incremental
        );
        assert_eq!(
            "streaming".parse::<DeltaReadMode>().unwrap(),
            DeltaReadMode::Incremental
        );
        assert_eq!(
            "stream".parse::<DeltaReadMode>().unwrap(),
            DeltaReadMode::Incremental
        );
        assert!("unknown".parse::<DeltaReadMode>().is_err());
    }

    #[test]
    fn test_read_mode_display() {
        assert_eq!(DeltaReadMode::Snapshot.to_string(), "snapshot");
        assert_eq!(DeltaReadMode::Incremental.to_string(), "incremental");
    }

    #[test]
    fn test_read_mode_from_config() {
        let config = make_config(&[("table.path", "/data/test"), ("read.mode", "snapshot")]);
        let cfg = DeltaSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.read_mode, DeltaReadMode::Snapshot);
    }

    #[test]
    fn test_read_mode_invalid() {
        let config = make_config(&[("table.path", "/data/test"), ("read.mode", "invalid")]);
        assert!(DeltaSourceConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_partition_filter_from_config() {
        let config = make_config(&[
            ("table.path", "/data/test"),
            ("partition.filter", "date = '2024-01-01'"),
        ]);
        let cfg = DeltaSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.partition_filter.as_deref(), Some("date = '2024-01-01'"));
    }

    #[test]
    fn test_partition_filter_empty_is_none() {
        let config = make_config(&[("table.path", "/data/test"), ("partition.filter", "")]);
        let cfg = DeltaSourceConfig::from_config(&config).unwrap();
        assert!(cfg.partition_filter.is_none());
    }

    #[test]
    fn test_schema_evolution_action_parse() {
        assert_eq!(
            "warn".parse::<SchemaEvolutionAction>().unwrap(),
            SchemaEvolutionAction::Warn
        );
        assert_eq!(
            "error".parse::<SchemaEvolutionAction>().unwrap(),
            SchemaEvolutionAction::Error
        );
        assert_eq!(
            "fail".parse::<SchemaEvolutionAction>().unwrap(),
            SchemaEvolutionAction::Error
        );
        assert!("unknown".parse::<SchemaEvolutionAction>().is_err());
    }

    #[test]
    fn test_schema_evolution_action_from_config() {
        let config = make_config(&[
            ("table.path", "/data/test"),
            ("schema.evolution.action", "error"),
        ]);
        let cfg = DeltaSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.schema_evolution_action, SchemaEvolutionAction::Error);
    }

    #[test]
    fn test_schema_evolution_action_default() {
        let cfg = DeltaSourceConfig::default();
        assert_eq!(cfg.schema_evolution_action, SchemaEvolutionAction::Warn);
    }
}
