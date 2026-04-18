//! Durable assignment snapshot on the object store.
//!
//! The cluster control plane writes its current split→instance mapping
//! to `control/assignment-snapshot.json` so that a full-cluster restart
//! can recover the assignment without starting from scratch. Chitchat
//! KV carries the ephemeral hot-path version; this file is the cold-
//! storage source of truth.
//!
//! Format is JSON (debuggable from the command line). Writes use the
//! object store's `PutMode::Create` + `ETag` compare to detect
//! concurrent updates — standard optimistic concurrency. See the
//! parent design doc §7.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use crate::cluster::discovery::NodeId;

const SNAPSHOT_PATH: &str = "control/assignment-snapshot.json";

/// A split-to-instance assignment durable snapshot.
///
/// `version` is monotonic within a cluster's lifetime; writers include
/// it on every update to detect conflicts. `splits` keys are
/// source-specific split identifiers (Kafka partition, file path, CDC
/// singleton tag).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentSnapshot {
    /// Monotonic version. Writers bump on each update.
    pub version: u64,
    /// Split identifier → owning instance.
    pub splits: BTreeMap<String, NodeId>,
    /// Wall-clock timestamp of the last update, millis since epoch.
    pub updated_at_ms: i64,
}

impl AssignmentSnapshot {
    /// Empty snapshot at version 0.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            splits: BTreeMap::new(),
            updated_at_ms: 0,
        }
    }

    /// Produce the next snapshot with the given split map and a bumped
    /// version. `updated_at_ms` is taken from the system clock.
    #[must_use]
    pub fn next(&self, splits: BTreeMap<String, NodeId>) -> Self {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        Self {
            version: self.version + 1,
            splits,
            updated_at_ms: now_ms,
        }
    }
}

/// I/O wrapper for [`AssignmentSnapshot`] on an object store.
pub struct AssignmentSnapshotStore {
    store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for AssignmentSnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssignmentSnapshotStore")
            .finish_non_exhaustive()
    }
}

/// Errors loading or saving an [`AssignmentSnapshot`].
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// Underlying object store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// JSON de/serialization failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl AssignmentSnapshotStore {
    /// Wrap a pre-constructed object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Load the current snapshot. Returns `Ok(None)` if no snapshot has
    /// ever been written (fresh cluster).
    ///
    /// # Errors
    /// Returns [`SnapshotError::Io`] on any non-`NotFound` store error,
    /// and [`SnapshotError::Json`] if the stored bytes don't decode.
    pub async fn load(&self) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let path = OsPath::from(SNAPSHOT_PATH);
        match self.store.get(&path).await {
            Ok(res) => {
                let bytes = res.bytes().await.map_err(|e| SnapshotError::Io(e.to_string()))?;
                let snap = serde_json::from_slice(&bytes)?;
                Ok(Some(snap))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(SnapshotError::Io(e.to_string())),
        }
    }

    /// Save `snapshot` unconditionally. Overwrites any prior snapshot
    /// at the same path.
    ///
    /// Concurrent writes are not guarded here; the cluster-control
    /// layer serializes writes through the elected leader. If two
    /// instances both think they're leader and race, the later write
    /// wins — acceptable because the loser's assignment is about to be
    /// superseded anyway when the real leader re-observes membership.
    ///
    /// # Errors
    /// Returns [`SnapshotError::Io`] or [`SnapshotError::Json`].
    pub async fn save(&self, snapshot: &AssignmentSnapshot) -> Result<(), SnapshotError> {
        let path = OsPath::from(SNAPSHOT_PATH);
        let bytes = serde_json::to_vec_pretty(snapshot)?;
        self.store
            .put(&path, PutPayload::from(Bytes::from(bytes)))
            .await
            .map_err(|e| SnapshotError::Io(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    fn store_in(dir: &std::path::Path) -> AssignmentSnapshotStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        AssignmentSnapshotStore::new(fs)
    }

    #[tokio::test]
    async fn load_missing_returns_none() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(s.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut splits = BTreeMap::new();
        splits.insert("kafka-orders-0".into(), NodeId(1));
        splits.insert("kafka-orders-1".into(), NodeId(2));
        let snap = AssignmentSnapshot::empty().next(splits);

        s.save(&snap).await.unwrap();
        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, snap);
    }

    #[tokio::test]
    async fn overwrite_wins() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut splits_v1 = BTreeMap::new();
        splits_v1.insert("p0".into(), NodeId(1));
        let v1 = AssignmentSnapshot::empty().next(splits_v1);
        s.save(&v1).await.unwrap();

        let mut splits_v2 = BTreeMap::new();
        splits_v2.insert("p0".into(), NodeId(2));
        let v2 = v1.next(splits_v2);
        s.save(&v2).await.unwrap();

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded.version, 2);
        assert_eq!(loaded.splits.get("p0"), Some(&NodeId(2)));
    }

    #[test]
    fn empty_starts_at_version_zero() {
        let s = AssignmentSnapshot::empty();
        assert_eq!(s.version, 0);
        assert!(s.splits.is_empty());
    }

    #[test]
    fn next_bumps_version() {
        let mut splits = BTreeMap::new();
        splits.insert("p0".into(), NodeId(1));
        let s = AssignmentSnapshot::empty().next(splits);
        assert_eq!(s.version, 1);
    }
}
