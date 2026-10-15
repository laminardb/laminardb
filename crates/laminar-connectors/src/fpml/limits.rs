//! Per-document resource bounds, enforced before libxml2 allocates a tree.

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

/// Hard ceiling for `fpml.max.document.bytes`; libxml2 takes the size as a C `int`.
const MAX_DOCUMENT_BYTES_CEILING: usize = 256 * 1024 * 1024;

/// Bounds applied to every document. Each is checked before the work it guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Complete document bytes (one file or one message).
    pub max_document_bytes: usize,
    /// Element nesting depth.
    pub max_depth: usize,
    /// Elements in one document.
    pub max_elements: usize,
    /// Attributes, including namespace declarations, on one element.
    pub max_attributes_per_element: usize,
    /// Bytes of one text node, CDATA section, comment, or attribute value.
    pub max_text_bytes: usize,
    /// Namespace declarations in one document.
    pub max_namespace_declarations: usize,
    /// `id` attributes in one document.
    pub max_ids: usize,
    /// `href` attributes in one document.
    pub max_references: usize,
    /// Rows one document may project.
    pub max_rows: usize,
    /// Arrow bytes staged for one document before it is published.
    pub max_staged_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_document_bytes: 16 * 1024 * 1024,
            max_depth: 64,
            max_elements: 250_000,
            max_attributes_per_element: 32,
            max_text_bytes: 1024 * 1024,
            max_namespace_declarations: 64,
            max_ids: 100_000,
            max_references: 100_000,
            max_rows: 10_000,
            max_staged_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Limits {
    /// Read `fpml.max.*` overrides.
    ///
    /// # Errors
    /// Returns a configuration error for a malformed or zero bound, or a document bound above
    /// 256 MiB.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let defaults = Self::default();
        let limits = Self {
            max_document_bytes: bound(config, "fpml.max.document.bytes", defaults.max_document_bytes)?,
            max_depth: bound(config, "fpml.max.depth", defaults.max_depth)?,
            max_elements: bound(config, "fpml.max.elements", defaults.max_elements)?,
            max_attributes_per_element: bound(
                config,
                "fpml.max.attributes.per.element",
                defaults.max_attributes_per_element,
            )?,
            max_text_bytes: bound(config, "fpml.max.text.bytes", defaults.max_text_bytes)?,
            max_namespace_declarations: bound(
                config,
                "fpml.max.namespace.declarations",
                defaults.max_namespace_declarations,
            )?,
            max_ids: bound(config, "fpml.max.ids", defaults.max_ids)?,
            max_references: bound(config, "fpml.max.references", defaults.max_references)?,
            max_rows: bound(config, "fpml.max.rows", defaults.max_rows)?,
            max_staged_bytes: bound(config, "fpml.max.staged.bytes", defaults.max_staged_bytes)?,
        };
        if limits.max_document_bytes > MAX_DOCUMENT_BYTES_CEILING {
            return Err(ConnectorError::ConfigurationError(format!(
                "'fpml.max.document.bytes' must not exceed {MAX_DOCUMENT_BYTES_CEILING}"
            )));
        }
        Ok(limits)
    }
}

fn bound(config: &ConnectorConfig, key: &str, default: usize) -> Result<usize, ConnectorError> {
    let value = config.get_parsed::<usize>(key)?.unwrap_or(default);
    if value == 0 {
        return Err(ConnectorError::ConfigurationError(format!(
            "'{key}' must be greater than zero"
        )));
    }
    Ok(value)
}
