//! `MongoDB` change-event classification and canonical Extended JSON rendering.
//!
//! Change events stay as raw BSON from the driver until Arrow assembly decodes them once.
//! Operation names are the server's own `operationType` strings, so history records never
//! use the engine's `_op` changelog vocabulary.

use mongodb::bson::{Bson, RawBsonRef, RawDocument};

use crate::error::ConnectorError;

/// Semantic class of a change-stream `operationType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    /// A document was inserted.
    Insert,
    /// A document was modified; the event carries a delta and optionally a post-image.
    Update,
    /// A document was replaced in full.
    Replace,
    /// A document was deleted; only its key is present.
    Delete,
    /// The change stream was invalidated; only `startAfter` can continue it.
    Invalidate,
    /// The watched collection was dropped.
    Drop,
    /// The watched collection was renamed.
    Rename,
    /// The database containing the collection was dropped.
    DropDatabase,
    /// Expanded collection metadata event that leaves documents unchanged.
    Metadata,
    /// An operation this connector does not recognize.
    Unknown,
}

impl ChangeOperation {
    /// Classify a server `operationType`.
    #[must_use]
    pub fn classify(operation: &str) -> Self {
        match operation {
            "insert" => Self::Insert,
            "update" => Self::Update,
            "replace" => Self::Replace,
            "delete" => Self::Delete,
            "invalidate" => Self::Invalidate,
            "drop" => Self::Drop,
            "rename" => Self::Rename,
            "dropDatabase" => Self::DropDatabase,
            "create"
            | "createIndexes"
            | "dropIndexes"
            | "modify"
            | "shardCollection"
            | "refineCollectionShardKey"
            | "reshardCollection" => Self::Metadata,
            _ => Self::Unknown,
        }
    }
}

/// Canonical Extended JSON v2 text for one BSON value. Canonical form keeps every BSON type
/// distinct (`Int32` versus `Int64`, `ObjectId` versus string, `Decimal128`, dates).
///
/// # Errors
/// Returns an error when the raw value is malformed.
pub fn canonical_extjson(value: RawBsonRef<'_>) -> Result<String, ConnectorError> {
    let value = Bson::try_from(value.to_raw_bson()).map_err(|error| {
        ConnectorError::ReadError(format!("malformed MongoDB BSON value: {error}"))
    })?;
    Ok(value.into_canonical_extjson().to_string())
}

/// Canonical Extended JSON v2 text for one raw document.
///
/// # Errors
/// Returns an error when the document is malformed.
pub fn canonical_document_extjson(document: &RawDocument) -> Result<String, ConnectorError> {
    canonical_extjson(RawBsonRef::Document(document))
}

#[cfg(test)]
mod tests;
