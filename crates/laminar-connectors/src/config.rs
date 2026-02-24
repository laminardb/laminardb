//! Connector configuration types.
//!
//! Provides a generic configuration model for connectors:
//! - `ConnectorConfig`: Key-value configuration with validation
//! - `ConfigKeySpec`: Specification for a configuration key
//! - `ConnectorInfo`: Metadata about a connector implementation
//! - `ConnectorState`: Lifecycle state of a running connector

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::ConnectorError;

/// Configuration for a connector instance.
///
/// Connectors receive their configuration as a string key-value map,
/// typically parsed from SQL `WITH (...)` clauses or programmatic config.
#[derive(Debug, Clone, Default)]
pub struct ConnectorConfig {
    /// The connector type identifier (e.g., "kafka", "postgres-cdc").
    connector_type: String,

    /// Configuration properties.
    properties: HashMap<String, String>,
}

impl ConnectorConfig {
    /// Creates a new connector config with the given type.
    #[must_use]
    pub fn new(connector_type: impl Into<String>) -> Self {
        Self {
            connector_type: connector_type.into(),
            properties: HashMap::new(),
        }
    }

    /// Creates a config from existing properties.
    #[must_use]
    pub fn with_properties(
        connector_type: impl Into<String>,
        properties: HashMap<String, String>,
    ) -> Self {
        Self {
            connector_type: connector_type.into(),
            properties,
        }
    }

    /// Returns the connector type identifier.
    #[must_use]
    pub fn connector_type(&self) -> &str {
        &self.connector_type
    }

    /// Sets a configuration property.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }

    /// Gets a configuration property.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    /// Gets a required configuration property, returning an error if missing.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if the key is not set.
    pub fn require(&self, key: &str) -> Result<&str, ConnectorError> {
        self.get(key)
            .ok_or_else(|| ConnectorError::MissingConfig(key.to_string()))
    }

    /// Gets a property parsed as the given type.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the value cannot be parsed.
    pub fn get_parsed<T: std::str::FromStr>(&self, key: &str) -> Result<Option<T>, ConnectorError>
    where
        T::Err: fmt::Display,
    {
        match self.get(key) {
            Some(v) => v.parse::<T>().map(Some).map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid value for '{key}': {e}"))
            }),
            None => Ok(None),
        }
    }

    /// Gets a required property parsed as the given type.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if the key is missing, or
    /// `ConnectorError::ConfigurationError` if parsing fails.
    pub fn require_parsed<T: std::str::FromStr>(&self, key: &str) -> Result<T, ConnectorError>
    where
        T::Err: fmt::Display,
    {
        let value = self.require(key)?;
        value.parse::<T>().map_err(|e| {
            ConnectorError::ConfigurationError(format!("invalid value for '{key}': {e}"))
        })
    }

    /// Returns all properties as a reference.
    #[must_use]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Returns properties with a given prefix, with the prefix stripped.
    #[must_use]
    pub fn properties_with_prefix(&self, prefix: &str) -> HashMap<String, String> {
        self.properties
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix(prefix)
                    .map(|stripped| (stripped.to_string(), v.clone()))
            })
            .collect()
    }

    /// Returns the SQL-defined Arrow schema passed via `_arrow_schema`,
    /// or `None` if absent or unparseable.
    ///
    /// The compact format is `"col1:Type1,col2:Type2,..."` as produced by
    /// `encode_arrow_schema` in `laminar-db`.
    ///
    /// **Note:** This method re-parses the schema string on each call.
    /// Cache the result if you need it in a hot loop.
    #[must_use]
    pub fn arrow_schema(&self) -> Option<SchemaRef> {
        let s = self.get("_arrow_schema")?;
        let fields: Vec<Field> = s
            .split(',')
            .filter(|part| !part.is_empty())
            .filter_map(|part| {
                let (name, type_str) = part.split_once(':')?;
                let dt = match type_str {
                    "Utf8" => DataType::Utf8,
                    "LargeUtf8" => DataType::LargeUtf8,
                    "Float64" => DataType::Float64,
                    "Float32" => DataType::Float32,
                    "Int64" => DataType::Int64,
                    "Int32" => DataType::Int32,
                    "Int16" => DataType::Int16,
                    "Int8" => DataType::Int8,
                    "UInt64" => DataType::UInt64,
                    "UInt32" => DataType::UInt32,
                    "Boolean" => DataType::Boolean,
                    "Date32" => DataType::Date32,
                    "Date64" => DataType::Date64,
                    _ => return None,
                };
                Some(Field::new(name, dt, true))
            })
            .collect();

        if fields.is_empty() {
            None
        } else {
            Some(Arc::new(Schema::new(fields)))
        }
    }

    /// Formats all properties for display with secrets redacted.
    ///
    /// Uses [`SecretMasker`](crate::storage::SecretMasker) to replace
    /// values of secret keys (passwords, access keys, tokens) with `"***"`.
    #[must_use]
    pub fn display_redacted(&self) -> String {
        use crate::storage::SecretMasker;
        SecretMasker::display_map(&self.properties)
    }

    /// Validates the configuration against a set of key specifications.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` for missing required keys, or
    /// `ConnectorError::ConfigurationError` for invalid values.
    pub fn validate(&self, specs: &[ConfigKeySpec]) -> Result<(), ConnectorError> {
        for spec in specs {
            if spec.required && self.get(&spec.key).is_none() {
                if let Some(ref default) = spec.default {
                    // Has a default, skip
                    let _ = default;
                } else {
                    return Err(ConnectorError::MissingConfig(spec.key.clone()));
                }
            }
        }
        Ok(())
    }
}

/// Validates that a string field is non-empty.
///
/// # Errors
///
/// Returns `ConnectorError::ConfigurationError` if the value is empty.
pub fn require_non_empty(value: &str, field_name: &str) -> Result<(), ConnectorError> {
    if value.is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "{field_name} must not be empty",
        )));
    }
    Ok(())
}

/// Specification for a configuration key.
///
/// Used by connectors to declare their expected configuration.
#[derive(Debug, Clone)]
pub struct ConfigKeySpec {
    /// The configuration key name.
    pub key: String,

    /// Human-readable description.
    pub description: String,

    /// Whether this key is required.
    pub required: bool,

    /// Default value if not provided.
    pub default: Option<String>,
}

impl ConfigKeySpec {
    /// Creates a required configuration key spec.
    #[must_use]
    pub fn required(key: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            description: description.into(),
            required: true,
            default: None,
        }
    }

    /// Creates an optional configuration key spec with a default value.
    #[must_use]
    pub fn optional(
        key: impl Into<String>,
        description: impl Into<String>,
        default: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            description: description.into(),
            required: false,
            default: Some(default.into()),
        }
    }
}

/// Metadata about a connector implementation.
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    /// Unique connector type name (e.g., "kafka", "postgres-cdc").
    pub name: String,

    /// Human-readable display name.
    pub display_name: String,

    /// Version string.
    pub version: String,

    /// Whether this is a source connector.
    pub is_source: bool,

    /// Whether this is a sink connector.
    pub is_sink: bool,

    /// Configuration keys this connector accepts.
    pub config_keys: Vec<ConfigKeySpec>,
}

/// Lifecycle state of a running connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    /// Connector has been created but not yet opened.
    Created,

    /// Connector is initializing (connecting, schema discovery, etc.).
    Initializing,

    /// Connector is running and processing data.
    Running,

    /// Connector is paused (e.g., due to backpressure or manual pause).
    Paused,

    /// Connector encountered an error and is attempting recovery.
    Recovering,

    /// Connector has been closed.
    Closed,

    /// Connector has failed and cannot recover.
    Failed,
}

impl fmt::Display for ConnectorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorState::Created => write!(f, "Created"),
            ConnectorState::Initializing => write!(f, "Initializing"),
            ConnectorState::Running => write!(f, "Running"),
            ConnectorState::Paused => write!(f, "Paused"),
            ConnectorState::Recovering => write!(f, "Recovering"),
            ConnectorState::Closed => write!(f, "Closed"),
            ConnectorState::Failed => write!(f, "Failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_basic_operations() {
        let mut config = ConnectorConfig::new("kafka");
        config.set("bootstrap.servers", "localhost:9092");
        config.set("topic", "events");

        assert_eq!(config.connector_type(), "kafka");
        assert_eq!(config.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(config.get("topic"), Some("events"));
        assert_eq!(config.get("missing"), None);
    }

    #[test]
    fn test_config_require() {
        let mut config = ConnectorConfig::new("kafka");
        config.set("topic", "events");

        assert!(config.require("topic").is_ok());
        assert!(config.require("missing").is_err());
    }

    #[test]
    fn test_config_parsed() {
        let mut config = ConnectorConfig::new("kafka");
        config.set("batch.size", "1000");
        config.set("bad_number", "not_a_number");

        let size: Option<usize> = config.get_parsed("batch.size").unwrap();
        assert_eq!(size, Some(1000));

        let missing: Option<usize> = config.get_parsed("missing").unwrap();
        assert_eq!(missing, None);

        let bad: Result<Option<usize>, _> = config.get_parsed("bad_number");
        assert!(bad.is_err());
    }

    #[test]
    fn test_config_require_parsed() {
        let mut config = ConnectorConfig::new("test");
        config.set("port", "8080");

        let port: u16 = config.require_parsed("port").unwrap();
        assert_eq!(port, 8080);

        let missing: Result<u16, _> = config.require_parsed("missing");
        assert!(missing.is_err());
    }

    #[test]
    fn test_config_prefix_extraction() {
        let mut config = ConnectorConfig::new("kafka");
        config.set("kafka.bootstrap.servers", "localhost:9092");
        config.set("kafka.group.id", "my-group");
        config.set("topic", "events");

        let kafka_props = config.properties_with_prefix("kafka.");
        assert_eq!(kafka_props.len(), 2);
        assert_eq!(
            kafka_props.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(kafka_props.get("group.id"), Some(&"my-group".to_string()));
    }

    #[test]
    fn test_config_validate() {
        let specs = vec![
            ConfigKeySpec::required("topic", "Kafka topic"),
            ConfigKeySpec::optional("batch.size", "Batch size", "100"),
        ];

        let mut config = ConnectorConfig::new("kafka");
        config.set("topic", "events");

        assert!(config.validate(&specs).is_ok());

        let empty_config = ConnectorConfig::new("kafka");
        assert!(empty_config.validate(&specs).is_err());
    }

    #[test]
    fn test_config_with_properties() {
        let mut props = HashMap::new();
        props.insert("key1".to_string(), "val1".to_string());
        props.insert("key2".to_string(), "val2".to_string());

        let config = ConnectorConfig::with_properties("test", props);
        assert_eq!(config.get("key1"), Some("val1"));
        assert_eq!(config.get("key2"), Some("val2"));
    }

    #[test]
    fn test_connector_state_display() {
        assert_eq!(ConnectorState::Running.to_string(), "Running");
        assert_eq!(ConnectorState::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_display_redacted_masks_secrets() {
        let mut config = ConnectorConfig::new("delta-lake");
        config.set("aws_region", "us-east-1");
        config.set("aws_secret_access_key", "TOP_SECRET");
        config.set("aws_access_key_id", "AKID123");

        let display = config.display_redacted();
        assert!(display.contains("aws_region=us-east-1"));
        assert!(display.contains("aws_secret_access_key=***"));
        assert!(display.contains("aws_access_key_id=AKID123"));
        assert!(!display.contains("TOP_SECRET"));
    }

    #[test]
    fn test_display_redacted_empty() {
        let config = ConnectorConfig::new("test");
        assert!(config.display_redacted().is_empty());
    }

    #[test]
    fn test_arrow_schema_parses_compact_encoding() {
        let mut config = ConnectorConfig::new("websocket");
        config.set("_arrow_schema", "s:Utf8,p:Float64,q:Float64,T:Int64");

        let schema = config.arrow_schema().expect("should parse");
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "s");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).name(), "p");
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(3).name(), "T");
        assert_eq!(schema.field(3).data_type(), &DataType::Int64);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn test_arrow_schema_returns_none_when_absent() {
        let config = ConnectorConfig::new("kafka");
        assert!(config.arrow_schema().is_none());
    }

    #[test]
    fn test_arrow_schema_returns_none_for_empty_value() {
        let mut config = ConnectorConfig::new("kafka");
        config.set("_arrow_schema", "");
        assert!(config.arrow_schema().is_none());
    }
}
