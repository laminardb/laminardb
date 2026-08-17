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
mod tests;
