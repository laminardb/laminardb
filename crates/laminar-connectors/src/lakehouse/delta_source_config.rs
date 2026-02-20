//! Delta Lake source connector configuration.
//!
//! [`DeltaSourceConfig`] encapsulates all settings for reading Arrow
//! `RecordBatch` data from Delta Lake tables, parsed from SQL `WITH (...)`
//! clauses via [`from_config`](DeltaSourceConfig::from_config).

use std::collections::HashMap;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::storage::{
    CloudConfigValidator, ResolvedStorageOptions, SecretMasker, StorageCredentialResolver,
    StorageProvider,
};

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

    /// Storage options (S3 credentials, Azure keys, etc.).
    pub storage_options: HashMap<String, String>,
}

impl Default for DeltaSourceConfig {
    fn default() -> Self {
        Self {
            table_path: String::new(),
            starting_version: None,
            poll_interval: Duration::from_millis(1000),
            storage_options: HashMap::new(),
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

        // Resolve storage credentials.
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
        assert_eq!(cfg.poll_interval, Duration::from_millis(1000));
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
}
