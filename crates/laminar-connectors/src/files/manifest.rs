//! Exact file ingestion inventory.
//!
//! Only immutable file identities (paths) are correctness state. The exact set
//! supports logarithmic membership/insertion, while an immutable serialized
//! fragment log makes source-checkpoint snapshots constant-time.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::checkpoint::{PersistentOffset, SourceCheckpoint};

/// Read-only exact membership view used by the discovery task.
///
/// The view shares the live path index, so newly completed files become known
/// without rebuilding or copying the full inventory.
#[derive(Clone)]
pub(super) struct FileInventorySnapshot {
    paths: Arc<RwLock<BTreeSet<String>>>,
}

impl FileInventorySnapshot {
    /// Returns `true` only when the exact path has been ingested.
    pub(super) fn contains(&self, path: &str) -> bool {
        self.paths
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(path)
    }
}

impl fmt::Debug for FileInventorySnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileInventorySnapshot")
            .field(
                "path_count",
                &self
                    .paths
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish()
    }
}

/// Tracks every ingested file with exact membership.
pub struct FileIngestionManifest {
    paths: Arc<RwLock<BTreeSet<String>>>,
    serialized_paths: PersistentOffset,
}

impl FileIngestionManifest {
    /// Creates an empty manifest.
    #[must_use]
    pub fn new() -> Self {
        Self {
            paths: Arc::new(RwLock::new(BTreeSet::new())),
            serialized_paths: PersistentOffset::new("[", ",", "]"),
        }
    }

    /// Returns `true` only when the exact path has been ingested.
    pub fn contains(&self, path: &str) -> bool {
        self.paths
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(path)
    }

    /// Inserts an immutable file identity in `O(log N)`.
    pub fn insert(&mut self, path: String) {
        let fragment = serde_json::Value::String(path.clone()).to_string();
        let inserted = self
            .paths
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(path);
        if inserted {
            self.serialized_paths.push_fragment(fragment);
        }
    }

    /// Returns the number of processed paths.
    #[must_use]
    pub fn processed_count(&self) -> usize {
        self.paths
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Creates an `O(1)` exact membership view for the discovery engine.
    #[must_use]
    pub(super) fn snapshot_for_dedup(&self) -> FileInventorySnapshot {
        FileInventorySnapshot {
            paths: Arc::clone(&self.paths),
        }
    }

    /// Adds the immutable serialized inventory snapshot to a source checkpoint
    /// without materializing or copying the full path set.
    pub fn to_checkpoint(&self, checkpoint: &mut SourceCheckpoint) {
        checkpoint.set_persistent_offset("manifest", self.serialized_paths.clone());
    }

    /// Restores the exact inventory from a durable checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_checkpoint(checkpoint: &SourceCheckpoint) -> Result<Self, serde_json::Error> {
        let paths: BTreeSet<String> = match checkpoint.get_offset("manifest") {
            Some(json) => serde_json::from_str(json)?,
            None => BTreeSet::new(),
        };
        let mut manifest = Self::new();
        for path in paths {
            manifest.insert(path);
        }
        Ok(manifest)
    }
}

impl Default for FileIngestionManifest {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for FileIngestionManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileIngestionManifest")
            .field("path_count", &self.processed_count())
            .field(
                "serialized_fragments",
                &self.serialized_paths.fragment_count(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_exact_membership() {
        let mut manifest = FileIngestionManifest::new();
        assert!(!manifest.contains("a.csv"));
        manifest.insert("a.csv".into());
        assert!(manifest.contains("a.csv"));
        assert!(!manifest.contains("b.csv"));
    }

    #[test]
    fn inventory_never_evicts_or_false_positives() {
        let mut manifest = FileIngestionManifest::new();
        for i in 0..10_000 {
            manifest.insert(format!("file_{i}.csv"));
        }
        assert_eq!(manifest.processed_count(), 10_000);
        for i in 10_000..20_000 {
            assert!(!manifest.contains(&format!("file_{i}.csv")));
        }
    }

    #[test]
    fn checkpoint_snapshot_is_immutable_and_roundtrips() {
        let mut manifest = FileIngestionManifest::new();
        manifest.insert("a.csv".into());
        let mut old_checkpoint = SourceCheckpoint::new();
        manifest.to_checkpoint(&mut old_checkpoint);

        manifest.insert("b.csv".into());
        let mut new_checkpoint = SourceCheckpoint::new();
        manifest.to_checkpoint(&mut new_checkpoint);

        let old = FileIngestionManifest::from_checkpoint(&old_checkpoint).unwrap();
        assert!(old.contains("a.csv"));
        assert!(!old.contains("b.csv"));
        let new = FileIngestionManifest::from_checkpoint(&new_checkpoint).unwrap();
        assert!(new.contains("a.csv"));
        assert!(new.contains("b.csv"));
    }

    #[test]
    fn discovery_snapshot_is_constant_time_live_exact_view() {
        let mut manifest = FileIngestionManifest::new();
        manifest.insert("a.csv".into());
        let snapshot = manifest.snapshot_for_dedup();
        manifest.insert("b.csv".into());

        assert!(snapshot.contains("a.csv"));
        assert!(snapshot.contains("b.csv"));
        assert!(!snapshot.contains("unknown.csv"));
    }

    #[test]
    fn duplicate_insert_does_not_grow_serialized_inventory() {
        let mut manifest = FileIngestionManifest::new();
        manifest.insert("a.csv".into());
        manifest.insert("a.csv".into());
        assert_eq!(manifest.processed_count(), 1);
        assert_eq!(manifest.serialized_paths.fragment_count(), 1);
    }

    #[test]
    fn empty_manifest_checkpoint_roundtrip() {
        let manifest = FileIngestionManifest::new();
        let mut checkpoint = SourceCheckpoint::new();
        manifest.to_checkpoint(&mut checkpoint);
        let restored = FileIngestionManifest::from_checkpoint(&checkpoint).unwrap();
        assert_eq!(restored.processed_count(), 0);
    }
}
