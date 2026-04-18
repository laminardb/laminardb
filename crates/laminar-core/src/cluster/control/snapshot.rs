//! Durable split→instance assignment snapshot at
//! `control/assignment-snapshot.json`. Chitchat carries the ephemeral
//! copy; this file survives full-cluster restart.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use crate::cluster::discovery::NodeId;

const SNAPSHOT_PATH: &str = "control/assignment-snapshot.json";

/// Durable split-to-instance assignment snapshot.
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

    /// Next snapshot with bumped version and current wall-clock time.
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

    /// Load the current snapshot; `Ok(None)` on fresh cluster.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
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

    /// Save `snapshot`, last-write-wins. Leader serializes writes.
    ///
    /// # Errors
    /// Object-store I/O or JSON encode failure.
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
