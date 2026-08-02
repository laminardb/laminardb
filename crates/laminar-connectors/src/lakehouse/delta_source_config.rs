//! Delta Lake source config. Parsed from the resolved connector config via
//! [`DeltaSourceConfig::from_config`].
#![allow(clippy::disallowed_types)] // cold path: lakehouse configuration

use std::collections::HashMap;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::storage::{
    CloudConfigValidator, ResolvedStorageOptions, SecretMasker, StorageCredentialResolver,
    StorageProvider,
};

use super::delta_config::DeltaCatalogType;

/// Configuration for the Delta Lake source connector.
///
/// Parsed from resolved source connector options or constructed programmatically.
#[derive(Debug, Clone)]
pub struct DeltaSourceConfig {
    /// Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`).
    pub table_path: String,

    /// First table version to read. `None` reads only versions committed after startup.
    pub starting_version: Option<i64>,

    /// How often to poll for new versions (default: 1 second).
    pub poll_interval: Duration,

    /// Storage options (S3 credentials, Azure keys, etc.).
    pub storage_options: HashMap<String, String>,

    /// Option keys populated from the environment during parsing.
    env_resolved_storage_keys: Vec<String>,

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
            storage_options: HashMap::new(),
            env_resolved_storage_keys: Vec::new(),
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

    /// Parses a source config from a resolved [`ConnectorConfig`].
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
        if config.get("read.mode").is_some() {
            return Err(ConnectorError::ConfigurationError(
                "Delta source does not support read.mode".into(),
            ));
        }
        if config.get("cdf.enabled").is_some() {
            return Err(ConnectorError::ConfigurationError(
                "Delta source does not support cdf.enabled".into(),
            ));
        }
        if config.get("partition.filter").is_some() {
            return Err(ConnectorError::ConfigurationError(
                "Delta source does not support partition.filter".into(),
            ));
        }
        if config.get("schema.evolution.action").is_some() {
            return Err(ConnectorError::ConfigurationError(
                "Delta CDF source always fails on schema evolution; schema.evolution.action is unsupported"
                    .into(),
            ));
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
        cfg.env_resolved_storage_keys = resolved.env_resolved_keys;
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

    /// Returns configured options without values copied from the environment.
    #[cfg(feature = "delta-lake")]
    pub(crate) fn stable_storage_options(&self) -> HashMap<String, String> {
        let mut options = self.storage_options.clone();
        for key in &self.env_resolved_storage_keys {
            options.remove(key);
        }
        options
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
            return Err(ConnectorError::missing_config("table.path"));
        }
        if self.starting_version.is_some_and(|version| version < 0) {
            return Err(ConnectorError::ConfigurationError(
                "starting.version must be non-negative".into(),
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
        for value in ["abc", "-1"] {
            let config = make_config(&[("table.path", "/data/test"), ("starting.version", value)]);
            assert!(DeltaSourceConfig::from_config(&config).is_err());
        }
    }

    #[test]
    fn test_empty_table_path_rejected() {
        let mut cfg = DeltaSourceConfig::default();
        cfg.table_path = String::new();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn removed_options_fail_closed() {
        for (key, value) in [
            ("read.mode", "incremental"),
            ("cdf.enabled", "true"),
            ("partition.filter", ""),
            ("partition.filter", "date = '2024-01-01'"),
            ("schema.evolution.action", "warn"),
        ] {
            assert!(DeltaSourceConfig::from_config(&make_config(&[
                ("table.path", "/data/test"),
                (key, value),
            ]))
            .is_err());
        }
    }

    #[test]
    fn stable_storage_options_exclude_environment_fallbacks() {
        let mut explicit = HashMap::new();
        explicit.insert("aws_region".into(), "eu-west-2".into());
        let resolved =
            StorageCredentialResolver::resolve_with_env("s3://bucket/table", &explicit, |key| {
                match key {
                    "AWS_ACCESS_KEY_ID" => Some("rotating-key".into()),
                    "AWS_SECRET_ACCESS_KEY" => Some("rotating-secret".into()),
                    _ => None,
                }
            });
        let config = DeltaSourceConfig {
            table_path: "s3://bucket/table".into(),
            storage_options: resolved.options,
            env_resolved_storage_keys: resolved.env_resolved_keys,
            ..DeltaSourceConfig::default()
        };

        let stable = config.stable_storage_options();
        assert_eq!(
            stable.get("aws_region").map(String::as_str),
            Some("eu-west-2")
        );
        assert!(!stable.contains_key("aws_access_key_id"));
        assert!(!stable.contains_key("aws_secret_access_key"));
    }
}
