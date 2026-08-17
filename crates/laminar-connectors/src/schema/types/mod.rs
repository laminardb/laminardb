//! Schema types used across the connector framework.
//!
//! Defines the core data structures for schema inference, resolution,
//! and schema annotations:
//!
//! - [`RawRecord`]: A raw record with key, value, timestamp, and headers
//! - [`SourceMetadata`]: Type-erased metadata from a source connector
//! - [`FieldMeta`]: Per-field metadata for schema annotations

#![allow(clippy::disallowed_types)] // cold path: schema management

use std::any::Any;
use std::collections::HashMap;

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

#[cfg(test)]
mod tests;
