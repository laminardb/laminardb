//! Durable connector contracts. Native schemas remain separate from Arrow read/input schemas.

use std::collections::BTreeMap;

use arrow_schema::Schema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Current schema contract and declarative mapping representation.
pub const SCHEMA_BINDING_VERSION: u16 = 1;
/// Maximum canonical size of one connector contract, including native references.
pub const MAX_SCHEMA_BINDING_BYTES: usize = 4 * 1024 * 1024;
/// Maximum number of logical or external fields in one binding.
pub const MAX_SCHEMA_FIELDS: usize = 4096;
/// Maximum transitive native schema artifacts in one binding.
pub const MAX_SCHEMA_REFERENCES: usize = 64;

/// Direction in which a connector applies its schema contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaDirection {
    /// External data is projected into a logical read contract.
    Source,
    /// Bound query output is mapped into a writer contract.
    Sink,
}

/// Authority that established the immutable logical schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaOrigin {
    /// User-supplied fields, including legacy catalog reconstruction.
    Explicit,
    /// A fixed connector protocol or generator definition.
    BuiltIn,
    /// Authoritative registry, database, table, or file metadata.
    Metadata,
    /// The output of a query bound against the catalog.
    Query,
    /// A separately authorized, bounded sample; never an authoritative constraint.
    Sample,
}

/// One immutable native schema, including the semantics Arrow cannot express.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSchema {
    /// Codec or native metadata representation (for example `avro` or `iceberg`).
    pub format: String,
    /// Stable resource identity and concrete schema selection, without credentials.
    pub identity: BTreeMap<String, String>,
    /// Complete native definition, preserving ordered fields, unions, defaults and IDs.
    pub definition: serde_json::Value,
    /// Transitive definitions in deterministic order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<NativeSchemaArtifact>,
}

/// A transitive native definition and its scoped identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSchemaArtifact {
    /// Native name used by the referring definition.
    pub name: String,
    /// Scoped concrete version/identifier.
    pub identity: BTreeMap<String, String>,
    /// Complete definition, including its own reference declarations.
    pub definition: serde_json::Value,
}

/// Declarative mapping compiled once by the connector when it activates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaFieldMapping {
    /// Field in the read or query-input contract.
    pub logical: String,
    /// Field in the external or serialization contract.
    pub external: String,
}

/// Immutable connector binding for one catalog generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaBinding {
    /// Version of this encoding and mapping semantics.
    pub version: u16,
    /// Connector type, independent of credentials and connection handles.
    pub connector: String,
    /// Source or sink binding.
    pub direction: SchemaDirection,
    /// Origin of the logical contract.
    pub origin: SchemaOrigin,
    /// Complete logical reader or bound query-output schema.
    pub logical: Schema,
    /// Native reader/writer fields, when an external authority exists.
    pub external: Option<Schema>,
    /// Value/table/protocol contract, distinct from observed message writers.
    pub value: Option<NativeSchema>,
    /// Independently selected key contract, when the codec uses one.
    pub key: Option<NativeSchema>,
    /// Field mapping in logical field order.
    pub mapping: Vec<SchemaFieldMapping>,
    /// Engine changelog fields consumed by a sink and excluded from business writes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_fields: Vec<String>,
}

/// Structural or directional incompatibility in a resolved schema contract.
#[derive(Debug, Error)]
pub enum SchemaBindingError {
    /// Invalid contract, unsupported representation, or resource limit.
    #[error("invalid schema contract: {0}")]
    Invalid(String),
    /// A field cannot be read or written under the declared policy.
    #[error("incompatible schema contract: {0}")]
    Incompatible(String),
    /// Encoding failed.
    #[error("schema contract encoding: {0}")]
    Encoding(#[from] serde_json::Error),
}

impl SchemaBinding {
    /// Construct a logical-only contract; no external mutation or inference occurs.
    ///
    /// # Errors
    /// Rejects empty/duplicate fields and contracts exceeding the representation bounds.
    pub fn logical(
        connector: impl Into<String>,
        direction: SchemaDirection,
        origin: SchemaOrigin,
        logical: Schema,
    ) -> Result<Self, SchemaBindingError> {
        let mapping = logical
            .fields()
            .iter()
            .map(|field| SchemaFieldMapping {
                logical: field.name().clone(),
                external: field.name().clone(),
            })
            .collect();
        let binding = Self {
            version: SCHEMA_BINDING_VERSION,
            connector: connector.into(),
            direction,
            origin,
            logical,
            external: None,
            value: None,
            key: None,
            mapping,
            control_fields: Vec::new(),
        };
        binding.canonical_bytes()?;
        Ok(binding)
    }

    /// Bind fields by exact name and type. Sources may project external fields;
    /// sinks must supply every writer field and may not discard extra input fields.
    /// Casts, defaults and nullable fills require a destination-specific policy.
    ///
    /// # Errors
    /// Rejects missing fields, differing types or unsafe directional nullability.
    pub fn bind_external(&mut self, external: Schema) -> Result<(), SchemaBindingError> {
        validation::validate_schema(&external)?;
        let mapping = validation::mapping(
            self.direction,
            &self.logical,
            &external,
            &self.control_fields,
        )?;
        self.external = Some(external);
        self.mapping = mapping;
        Ok(())
    }

    /// Validate structure and produce deterministic JSON without reordering fields or unions.
    ///
    /// # Errors
    /// Rejects invalid versions, incomplete mappings, and oversized contracts.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, SchemaBindingError> {
        validation::validate(self)?;
        let mut value = serde_json::to_value(self)?;
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value)?;
        if bytes.len() > MAX_SCHEMA_BINDING_BYTES {
            return Err(SchemaBindingError::Invalid(format!(
                "contract exceeds {MAX_SCHEMA_BINDING_BYTES} bytes"
            )));
        }
        Ok(bytes)
    }

    /// SHA-256 of the versioned canonical encoding; native identity is also retained.
    ///
    /// # Errors
    /// Returns structural or encoding errors for an invalid contract.
    pub fn fingerprint(&self) -> Result<String, SchemaBindingError> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let digest = Sha256::digest(self.canonical_bytes()?);
        let mut fingerprint = String::with_capacity(64);
        for byte in digest {
            fingerprint.push(char::from(HEX[usize::from(byte >> 4)]));
            fingerprint.push(char::from(HEX[usize::from(byte & 15)]));
        }
        Ok(fingerprint)
    }

    /// Decode and validate a persisted contract without contacting its external authority.
    ///
    /// # Errors
    /// Rejects malformed, unsupported, or oversized representations.
    pub fn decode(bytes: &[u8]) -> Result<Self, SchemaBindingError> {
        if bytes.len() > MAX_SCHEMA_BINDING_BYTES {
            return Err(SchemaBindingError::Invalid(
                "encoded contract is oversized".into(),
            ));
        }
        let binding: Self = serde_json::from_slice(bytes)?;
        binding.canonical_bytes()?;
        Ok(binding)
    }
}

#[cfg(test)]
mod tests;
mod validation;
pub use validation::same_logical_type;
