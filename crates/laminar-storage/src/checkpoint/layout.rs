//! Object-store checkpoint layout for distributed checkpoints.
//!
//! Defines the directory structure, manifest format, and path conventions
//! for checkpoints stored in object stores (S3, GCS, Azure Blob, local FS).
//!
//! ## Directory Layout
//!
//! ```text
//! checkpoints/
//! ├── _latest                          # Pointer to latest manifest
//! ├── {checkpoint_id}/
//! │   ├── manifest.json                # CheckpointManifestV2
//! │   ├── operators/
//! │   │   └── {operator_name}/
//! │   │       └── partition-{id}.snap  # Full state snapshot
//! │   │       └── partition-{id}.delta # Incremental delta
//! │   └── offsets/
//! │       └── {source_name}.json       # Source offset data
//! └── {checkpoint_id}/
//!     └── ...
//! ```
//!
//! ## Checkpoint IDs
//!
//! [`CheckpointId`] wraps a UUID v7, which is time-sortable. This means
//! lexicographic sorting of checkpoint directories equals chronological
//! ordering — no need to parse timestamps or sequence numbers.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A time-sortable checkpoint identifier based on UUID v7.
///
/// UUID v7 embeds a Unix timestamp in the most significant bits, so
/// lexicographic ordering of the hex representation matches chronological
/// order. This simplifies object-store listing and garbage collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CheckpointId(Uuid);

impl CheckpointId {
    /// Generate a new checkpoint ID with the current timestamp.
    #[must_use]
    pub fn now() -> Self {
        Self(Uuid::now_v7())
    }

    /// Create a checkpoint ID from an existing UUID.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Return the hyphenated string representation.
    #[must_use]
    pub fn to_string_id(&self) -> String {
        self.0.to_string()
    }
}

impl fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Deterministic path generator for checkpoint artifacts in object stores.
///
/// All paths are relative to a configurable `base_prefix`. This struct
/// is stateless — it only computes paths, never performs I/O.
#[derive(Debug, Clone)]
pub struct CheckpointPaths {
    /// Base prefix in the object store (e.g., `"checkpoints/"`)
    pub(crate) base_prefix: String,
}

impl CheckpointPaths {
    /// Create a new path generator with the given base prefix.
    ///
    /// The prefix should end with `/` (one is appended if missing).
    #[must_use]
    pub fn new(base_prefix: &str) -> Self {
        let base_prefix = if base_prefix.ends_with('/') {
            base_prefix.to_string()
        } else {
            format!("{base_prefix}/")
        };
        Self { base_prefix }
    }

    /// Path to the `_latest` pointer file.
    #[must_use]
    pub fn latest_pointer(&self) -> String {
        format!("{}_{}", self.base_prefix, "latest")
    }

    /// Root directory for a specific checkpoint.
    #[must_use]
    pub fn checkpoint_dir(&self, id: &CheckpointId) -> String {
        format!("{}{}/", self.base_prefix, id)
    }

    /// Path to the manifest file for a checkpoint.
    #[must_use]
    pub fn manifest(&self, id: &CheckpointId) -> String {
        format!("{}{}manifest.json", self.base_prefix, id)
    }

    /// Path to a full state snapshot for an operator partition.
    #[must_use]
    pub fn snapshot(&self, id: &CheckpointId, operator: &str, partition: u32) -> String {
        format!(
            "{}{}operators/{}/partition-{partition}.snap",
            self.base_prefix, id, operator
        )
    }

    /// Path to an incremental delta for an operator partition.
    #[must_use]
    pub fn delta(&self, id: &CheckpointId, operator: &str, partition: u32) -> String {
        format!(
            "{}{}operators/{}/partition-{partition}.delta",
            self.base_prefix, id, operator
        )
    }

    /// Path to a source offset file.
    #[must_use]
    pub fn source_offset(&self, id: &CheckpointId, source_name: &str) -> String {
        format!(
            "{}{}offsets/{source_name}.json",
            self.base_prefix, id
        )
    }
}

impl Default for CheckpointPaths {
    fn default() -> Self {
        Self::new("checkpoints/")
    }
}

/// V2 checkpoint manifest for distributed object-store checkpoints.
///
/// This extends the Phase 1-3 [`CheckpointManifest`](super::super::CheckpointManifest)
/// with per-operator partition entries, incremental delta support, and
/// source offset tracking suitable for multi-partition recovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckpointManifestV2 {
    /// Manifest format version (always 2 for this type).
    pub version: u32,
    /// Unique checkpoint identifier (UUID v7).
    pub checkpoint_id: CheckpointId,
    /// Monotonically increasing epoch number.
    pub epoch: u64,
    /// Timestamp when checkpoint was created (millis since Unix epoch).
    pub timestamp_ms: u64,

    /// Per-operator snapshot entries.
    #[serde(default)]
    pub operators: HashMap<String, OperatorSnapshotEntry>,

    /// Per-source offset entries.
    #[serde(default)]
    pub source_offsets: HashMap<String, SourceOffsetEntry>,

    /// Parent checkpoint ID for incremental checkpoints.
    #[serde(default)]
    pub parent_id: Option<CheckpointId>,

    /// Global watermark at checkpoint time.
    #[serde(default)]
    pub watermark: Option<i64>,

    /// Total size of all checkpoint artifacts in bytes.
    #[serde(default)]
    pub total_size_bytes: u64,
}

impl CheckpointManifestV2 {
    /// Create a new V2 manifest.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(checkpoint_id: CheckpointId, epoch: u64) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            version: 2,
            checkpoint_id,
            epoch,
            timestamp_ms,
            operators: HashMap::new(),
            source_offsets: HashMap::new(),
            parent_id: None,
            watermark: None,
            total_size_bytes: 0,
        }
    }
}

/// Per-operator snapshot metadata in a V2 manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperatorSnapshotEntry {
    /// Per-partition snapshot/delta entries.
    pub partitions: Vec<PartitionSnapshotEntry>,
    /// Total bytes across all partitions.
    #[serde(default)]
    pub total_bytes: u64,
}

/// Per-partition snapshot or delta entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionSnapshotEntry {
    /// Partition index.
    pub partition_id: u32,
    /// Whether this is a full snapshot or an incremental delta.
    pub is_delta: bool,
    /// Object-store path to the artifact (relative to checkpoint dir).
    pub path: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// SHA-256 hex digest for integrity verification.
    #[serde(default)]
    pub sha256: Option<String>,
}

/// Per-source offset entry for exactly-once recovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceOffsetEntry {
    /// Source type (e.g., "kafka", "postgres-cdc", "file").
    pub source_type: String,
    /// Source-specific offset data (key-value pairs).
    pub offsets: HashMap<String, String>,
    /// Epoch this offset entry belongs to.
    pub epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_id_time_sortable() {
        let id1 = CheckpointId::now();
        // Ensure different timestamps (UUIDv7 has ms precision)
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = CheckpointId::now();

        // UUID v7 is time-sortable: id1 < id2
        assert!(id1 < id2, "UUID v7 should be time-sortable");

        // String representation is also sortable
        assert!(id1.to_string_id() < id2.to_string_id());
    }

    #[test]
    fn test_checkpoint_id_display() {
        let id = CheckpointId::now();
        let s = id.to_string();
        // UUID v7 format: 8-4-4-4-12
        assert_eq!(s.len(), 36);
        assert_eq!(s.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn test_checkpoint_paths() {
        let paths = CheckpointPaths::new("s3://my-bucket/checkpoints");
        let id = CheckpointId::now();

        let latest = paths.latest_pointer();
        assert!(latest.ends_with("_latest"));

        let manifest = paths.manifest(&id);
        assert!(manifest.ends_with("manifest.json"));
        assert!(manifest.contains(&id.to_string()));

        let snap = paths.snapshot(&id, "window-agg", 3);
        assert!(snap.contains("operators/window-agg/"));
        assert!(snap.ends_with("partition-3.snap"));

        let delta = paths.delta(&id, "window-agg", 3);
        assert!(delta.ends_with("partition-3.delta"));

        let offset = paths.source_offset(&id, "kafka-trades");
        assert!(offset.ends_with("kafka-trades.json"));
    }

    #[test]
    fn test_checkpoint_paths_trailing_slash() {
        let paths1 = CheckpointPaths::new("prefix/");
        let paths2 = CheckpointPaths::new("prefix");
        let id = CheckpointId::now();

        // Both should produce the same paths
        assert_eq!(paths1.manifest(&id), paths2.manifest(&id));
    }

    #[test]
    fn test_manifest_v2_json_round_trip() {
        let id = CheckpointId::now();
        let mut manifest = CheckpointManifestV2::new(id, 10);
        manifest.watermark = Some(5000);
        manifest.parent_id = Some(CheckpointId::now());

        manifest.operators.insert(
            "window-agg".into(),
            OperatorSnapshotEntry {
                partitions: vec![
                    PartitionSnapshotEntry {
                        partition_id: 0,
                        is_delta: false,
                        path: "operators/window-agg/partition-0.snap".into(),
                        size_bytes: 1024,
                        sha256: Some("abcd1234".into()),
                    },
                    PartitionSnapshotEntry {
                        partition_id: 1,
                        is_delta: true,
                        path: "operators/window-agg/partition-1.delta".into(),
                        size_bytes: 256,
                        sha256: None,
                    },
                ],
                total_bytes: 1280,
            },
        );

        manifest.source_offsets.insert(
            "kafka-trades".into(),
            SourceOffsetEntry {
                source_type: "kafka".into(),
                offsets: HashMap::from([
                    ("partition-0".into(), "1234".into()),
                    ("partition-1".into(), "5678".into()),
                ]),
                epoch: 10,
            },
        );

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let restored: CheckpointManifestV2 = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.version, 2);
        assert_eq!(restored.checkpoint_id, id);
        assert_eq!(restored.epoch, 10);
        assert_eq!(restored.watermark, Some(5000));
        assert!(restored.parent_id.is_some());

        let op = restored.operators.get("window-agg").unwrap();
        assert_eq!(op.partitions.len(), 2);
        assert_eq!(op.total_bytes, 1280);

        let src = restored.source_offsets.get("kafka-trades").unwrap();
        assert_eq!(src.source_type, "kafka");
        assert_eq!(src.offsets.get("partition-0"), Some(&"1234".into()));
    }

    #[test]
    fn test_manifest_v2_backward_compat_missing_fields() {
        let id = CheckpointId::now();
        let json = format!(
            r#"{{
                "version": 2,
                "checkpoint_id": "{id}",
                "epoch": 1,
                "timestamp_ms": 1000
            }}"#
        );

        let manifest: CheckpointManifestV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(manifest.version, 2);
        assert!(manifest.operators.is_empty());
        assert!(manifest.source_offsets.is_empty());
        assert!(manifest.parent_id.is_none());
        assert!(manifest.watermark.is_none());
    }
}
