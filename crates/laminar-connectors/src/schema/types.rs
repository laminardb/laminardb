//! Schema types used across the connector framework.
//!
//! Defines the core data structures for schema inference, resolution,
//! and connector configuration:
//!
//! - [`RawRecord`]: A raw record with key, value, timestamp, and headers
//! - [`SourceMetadata`]: Type-erased metadata from a source connector
//! - [`FieldMeta`]: Per-field metadata for schema annotations
//! - [`SourceConfig`]: Configuration for a source connector (schema module)
//! - [`SinkConfig`]: Configuration for a sink connector (schema module)

use std::any::Any;
use std::collections::HashMap;

use crate::config::ConnectorConfig;
use crate::serde::Format;

/// A raw record read from a source before schema application.
///
/// Carries the key, value, optional timestamp, headers, and arbitrary
/// source-specific metadata.
#[derive(Debug, Clone)]
pub struct RawRecord {
    /// Optional record key (e.g., Kafka message key).
    pub key: Option<Vec<u8>>,

    /// Record value (payload bytes).
    pub value: Vec<u8>,

    /// Optional event-time timestamp in milliseconds since epoch.
    pub timestamp: Option<i64>,

    /// Optional key-value headers (e.g., Kafka headers).
    pub headers: HashMap<String, Vec<u8>>,

    /// Source-specific metadata (e.g., partition, offset, topic).
    pub metadata: SourceMetadata,
}

impl RawRecord {
    /// Creates a new raw record with only a value.
    #[must_use]
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            key: None,
            value,
            timestamp: None,
            headers: HashMap::new(),
            metadata: SourceMetadata::empty(),
        }
    }

    /// Sets the record key.
    #[must_use]
    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    /// Sets the event-time timestamp.
    #[must_use]
    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Adds a header.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: Vec<u8>) -> Self {
        self.headers.insert(key.into(), value);
        self
    }

    /// Sets the source metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: SourceMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Type-erased metadata from a source connector.
///
/// Wraps a `Box<dyn Any + Send + Sync>` to allow connectors to attach
/// arbitrary metadata (e.g., Kafka offset, CDC LSN) to raw records.
pub struct SourceMetadata {
    inner: Option<Box<dyn Any + Send + Sync>>,
}

impl SourceMetadata {
    /// Creates empty metadata.
    #[must_use]
    pub fn empty() -> Self {
        Self { inner: None }
    }

    /// Creates metadata from a typed value.
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self {
            inner: Some(Box::new(value)),
        }
    }

    /// Returns `true` if no metadata is present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_none()
    }

    /// Attempts to downcast the metadata to a concrete type.
    #[must_use]
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.inner.as_ref()?.downcast_ref::<T>()
    }
}

impl std::fmt::Debug for SourceMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.inner.is_some() {
            write!(f, "SourceMetadata(<opaque>)")
        } else {
            write!(f, "SourceMetadata(empty)")
        }
    }
}

impl Clone for SourceMetadata {
    fn clone(&self) -> Self {
        // Metadata is not cloneable in general; cloning produces empty metadata.
        Self::empty()
    }
}

/// Per-field metadata for schema annotations.
///
/// Provides additional information about a field beyond what Arrow's
/// `Field` captures (description, original source type, etc.).
#[derive(Debug, Clone, Default)]
pub struct FieldMeta {
    /// Optional stable field identifier (for evolution tracking).
    pub field_id: Option<u32>,

    /// Human-readable description of the field.
    pub description: Option<String>,

    /// Original type name in the source system (e.g., `"VARCHAR(255)"`).
    pub source_type: Option<String>,

    /// Default expression if the field is missing (e.g., `"0"`, `"now()"`).
    pub default_expr: Option<String>,

    /// Arbitrary key-value properties.
    pub properties: HashMap<String, String>,
}

impl FieldMeta {
    /// Creates empty field metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the field ID.
    #[must_use]
    pub fn with_field_id(mut self, id: u32) -> Self {
        self.field_id = Some(id);
        self
    }

    /// Sets the description.
    #[must_use]
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Sets the original source type.
    #[must_use]
    pub fn with_source_type(mut self, src_type: impl Into<String>) -> Self {
        self.source_type = Some(src_type.into());
        self
    }

    /// Sets the default expression.
    #[must_use]
    pub fn with_default(mut self, expr: impl Into<String>) -> Self {
        self.default_expr = Some(expr.into());
        self
    }

    /// Sets an arbitrary property.
    #[must_use]
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Configuration for a source connector in the schema module.
///
/// This is distinct from [`crate::config::ConnectorConfig`] — it adds
/// format awareness and typed accessors for schema-related options.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Connector type identifier (e.g., `"kafka"`, `"postgres-cdc"`).
    pub connector_type: String,

    /// Data format (e.g., JSON, CSV, Avro).
    pub format: Format,

    /// Arbitrary key-value options.
    pub options: HashMap<String, String>,
}

impl SourceConfig {
    /// Creates a new source config.
    #[must_use]
    pub fn new(connector_type: impl Into<String>, format: Format) -> Self {
        Self {
            connector_type: connector_type.into(),
            format,
            options: HashMap::new(),
        }
    }

    /// Sets an option.
    #[must_use]
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Gets an option value.
    #[must_use]
    pub fn get_option(&self, key: &str) -> Option<&str> {
        self.options.get(key).map(String::as_str)
    }

    /// Converts to a [`ConnectorConfig`] for use with existing connector APIs.
    #[must_use]
    pub fn as_connector_config(&self) -> ConnectorConfig {
        let mut props = self.options.clone();
        props.insert("format".to_string(), self.format.to_string());
        ConnectorConfig::with_properties(&self.connector_type, props)
    }
}

/// Configuration for a sink connector in the schema module.
///
/// Mirror of [`SourceConfig`] for sink connectors.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    /// Connector type identifier (e.g., `"kafka"`, `"postgres"`).
    pub connector_type: String,

    /// Output data format.
    pub format: Format,

    /// Arbitrary key-value options.
    pub options: HashMap<String, String>,
}

impl SinkConfig {
    /// Creates a new sink config.
    #[must_use]
    pub fn new(connector_type: impl Into<String>, format: Format) -> Self {
        Self {
            connector_type: connector_type.into(),
            format,
            options: HashMap::new(),
        }
    }

    /// Sets an option.
    #[must_use]
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Gets an option value.
    #[must_use]
    pub fn get_option(&self, key: &str) -> Option<&str> {
        self.options.get(key).map(String::as_str)
    }

    /// Converts to a [`ConnectorConfig`] for use with existing connector APIs.
    #[must_use]
    pub fn as_connector_config(&self) -> ConnectorConfig {
        let mut props = self.options.clone();
        props.insert("format".to_string(), self.format.to_string());
        ConnectorConfig::with_properties(&self.connector_type, props)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_record_builder() {
        let record = RawRecord::new(b"hello".to_vec())
            .with_key(b"k1".to_vec())
            .with_timestamp(1000)
            .with_header("content-type", b"application/json".to_vec());

        assert_eq!(record.key.as_deref(), Some(b"k1".as_slice()));
        assert_eq!(record.value, b"hello");
        assert_eq!(record.timestamp, Some(1000));
        assert!(record.headers.contains_key("content-type"));
    }

    #[test]
    fn test_source_metadata_empty() {
        let meta = SourceMetadata::empty();
        assert!(meta.is_empty());
        assert!(meta.downcast_ref::<String>().is_none());
    }

    #[test]
    fn test_source_metadata_typed() {
        let meta = SourceMetadata::new(42u64);
        assert!(!meta.is_empty());
        assert_eq!(meta.downcast_ref::<u64>(), Some(&42u64));
        assert!(meta.downcast_ref::<String>().is_none());
    }

    #[test]
    fn test_source_metadata_debug() {
        let empty = SourceMetadata::empty();
        assert!(format!("{empty:?}").contains("empty"));

        let full = SourceMetadata::new("data");
        assert!(format!("{full:?}").contains("opaque"));
    }

    #[test]
    fn test_field_meta_builder() {
        let meta = FieldMeta::new()
            .with_field_id(1)
            .with_description("User ID")
            .with_source_type("BIGINT")
            .with_default("0")
            .with_property("pii", "true");

        assert_eq!(meta.field_id, Some(1));
        assert_eq!(meta.description.as_deref(), Some("User ID"));
        assert_eq!(meta.source_type.as_deref(), Some("BIGINT"));
        assert_eq!(meta.default_expr.as_deref(), Some("0"));
        assert_eq!(meta.properties.get("pii").map(String::as_str), Some("true"));
    }

    #[test]
    fn test_source_config() {
        let cfg = SourceConfig::new("kafka", Format::Json)
            .with_option("topic", "events")
            .with_option("group.id", "my-group");

        assert_eq!(cfg.connector_type, "kafka");
        assert_eq!(cfg.format, Format::Json);
        assert_eq!(cfg.get_option("topic"), Some("events"));

        let cc = cfg.as_connector_config();
        assert_eq!(cc.connector_type(), "kafka");
        assert_eq!(cc.get("format"), Some("json"));
        assert_eq!(cc.get("topic"), Some("events"));
    }

    #[test]
    fn test_sink_config() {
        let cfg = SinkConfig::new("postgres", Format::Json).with_option("table", "output");

        assert_eq!(cfg.connector_type, "postgres");
        assert_eq!(cfg.get_option("table"), Some("output"));

        let cc = cfg.as_connector_config();
        assert_eq!(cc.get("format"), Some("json"));
    }
}
