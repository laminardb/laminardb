//! `MongoDB` change stream event types.
//!
//! Represents the structure of change events emitted by `MongoDB` change
//! streams. Maps `MongoDB`'s change event document structure to typed Rust
//! structs for safe downstream processing.
//!
//! # Operation Types
//!
//! `MongoDB` change streams emit events for the following operations:
//!
//! - `insert` — new document created; `full_document` always present
//! - `update` — document modified; `update_description` contains delta
//! - `replace` — full document replacement
//! - `delete` — document removed; only `document_key` present
//! - `drop` — collection dropped (lifecycle event)
//! - `rename` — collection renamed (lifecycle event)
//! - `invalidate` — stream invalidated; `resumeAfter` is no longer legal

use std::collections::HashMap;

/// The type of operation described by a change event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperationType {
    /// A new document was inserted.
    Insert,
    /// An existing document was updated (delta available).
    Update,
    /// An existing document was fully replaced.
    Replace,
    /// A document was deleted.
    Delete,
    /// The collection was dropped.
    Drop,
    /// The collection was renamed.
    Rename,
    /// The change stream was invalidated and must be restarted.
    Invalidate,
    /// The database was dropped.
    DropDatabase,
    /// An operation type not yet mapped by this enum.
    Other(String),
}

impl OperationType {
    /// Returns the single-character code for the operation, compatible with
    /// the CDC envelope format used elsewhere in `LaminarDB`.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Insert => "I",
            Self::Update => "U",
            Self::Replace => "R",
            Self::Delete => "D",
            Self::Drop => "DROP",
            Self::Rename => "RENAME",
            Self::Invalidate => "INVALIDATE",
            Self::DropDatabase => "DROP_DATABASE",
            Self::Other(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Describes the fields changed during an `update` operation.
///
/// Mirrors `MongoDB`'s `updateDescription` subdocument in change events.
/// Only populated for `update` operations; `insert`, `replace`, and
/// `delete` events do not include this.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct UpdateDescription {
    /// Fields that were set or modified, as key-value JSON pairs.
    pub updated_fields: HashMap<String, serde_json::Value>,

    /// Fields that were removed from the document.
    pub removed_fields: Vec<String>,

    /// Arrays that were truncated (`MongoDB` 6.0+).
    #[serde(default)]
    pub truncated_arrays: Vec<TruncatedArray>,

    /// Ambiguous update paths mapped to their exact path components.
    ///
    /// Replay must not treat these keys as ordinary dotted paths. The `MongoDB` sink rejects a
    /// non-empty map until it can express the update faithfully with `$setField` semantics.
    #[serde(default)]
    pub disambiguated_paths: HashMap<String, serde_json::Value>,
}

/// Describes a truncated array in an update operation (`MongoDB` 6.0+).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TruncatedArray {
    /// Dot-notation path of the truncated array field.
    pub field: String,
    /// New size of the array after truncation.
    pub new_size: u32,
}

/// The namespace (database + collection) for a change event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Namespace {
    /// Database name.
    pub db: String,
    /// Collection name.
    pub coll: String,
}

impl Namespace {
    /// Returns the fully qualified namespace as `db.coll`.
    #[must_use]
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.db, self.coll)
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.db, self.coll)
    }
}

/// A change event from a `MongoDB` change stream.
///
/// This struct captures all fields relevant for CDC processing. The
/// `resume_token` is the opaque `_id` field from the change event document,
/// used for resuming the stream.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MongoDbChangeEvent {
    /// The operation type.
    pub operation_type: OperationType,

    /// The namespace (database + collection).
    pub namespace: Namespace,

    /// The document key (`_id` and any shard key fields).
    /// Serialized as a JSON string for Arrow compatibility.
    pub document_key: String,

    /// The full document (present for insert, replace, and optionally
    /// for update events depending on `FullDocumentMode`).
    pub full_document: Option<String>,

    /// Update delta (only for `update` operations).
    pub update_description: Option<UpdateDescription>,

    /// Server timestamp of the operation (cluster time).
    pub cluster_time_secs: u32,

    /// Cluster time increment.
    pub cluster_time_inc: u32,

    /// The opaque resume token for this event, serialized as JSON.
    pub resume_token: String,

    /// Wall clock timestamp in milliseconds since Unix epoch.
    pub wall_time_ms: i64,
}

#[cfg(test)]
mod tests;
