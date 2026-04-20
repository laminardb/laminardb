//! Durable vnode→instance assignment snapshot at
//! `control/assignment-snapshot.json`. Chitchat carries the ephemeral
//! copy; this file survives full-cluster restart.
//!
//! Serialises the `VnodeRegistry`'s assignment so every node reaches
//! the same vnode topology from the same source of truth — critical for
//! split-brain correctness: both partitioned halves observe the same
//! stored snapshot instead of each independently computing a fresh
//! round-robin that would have disjoint vnode owners. See Phase 1.2
//! in `docs/plans/cluster-production-readiness.md`.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};

use crate::cluster::discovery::NodeId;

const SNAPSHOT_PATH: &str = "control/assignment-snapshot.json";

/// Durable vnode-to-instance assignment snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentSnapshot {
    /// Monotonic version. Writers bump on each update.
    pub version: u64,
    /// Vnode id → owning instance. `BTreeMap` (not `Vec`) so snapshots
    /// with different `vnode_count` are still deserializable — sparse
    /// indices surface as missing keys the caller can diagnose.
    pub vnodes: BTreeMap<u32, NodeId>,
    /// Wall-clock timestamp of the last update, millis since epoch.
    pub updated_at_ms: i64,
}

impl AssignmentSnapshot {
    /// Empty snapshot at version 0.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            vnodes: BTreeMap::new(),
            updated_at_ms: 0,
        }
    }

    /// Next snapshot with bumped version and current wall-clock time.
    #[must_use]
    pub fn next(&self, vnodes: BTreeMap<u32, NodeId>) -> Self {
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        Self {
            version: self.version + 1,
            vnodes,
            updated_at_ms: now_ms,
        }
    }

    /// Convert a `Vec<NodeId>` (one entry per vnode id, dense) into the
    /// `BTreeMap` shape this snapshot uses. Mirrors the layout returned
    /// by `round_robin_assignment`.
    #[must_use]
    pub fn vnodes_from_vec(assignment: &[NodeId]) -> BTreeMap<u32, NodeId> {
        #[allow(clippy::cast_possible_truncation)]
        assignment
            .iter()
            .enumerate()
            .map(|(i, n)| (i as u32, *n))
            .collect()
    }

    /// Dense `Vec<NodeId>` of length `vnode_count`. Missing entries (a
    /// stale snapshot from a smaller vnode topology) are filled with
    /// `NodeId::UNASSIGNED` so callers can detect the mismatch.
    #[must_use]
    pub fn to_vnode_vec(&self, vnode_count: u32) -> Vec<NodeId> {
        (0..vnode_count)
            .map(|v| self.vnodes.get(&v).copied().unwrap_or(NodeId::UNASSIGNED))
            .collect()
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
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| SnapshotError::Io(e.to_string()))?;
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

    /// CAS-create: write `snapshot` only if no snapshot exists yet.
    /// Returns:
    /// - `Ok(Some(snapshot))` on successful create; snapshot is now the
    ///   canonical version,
    /// - `Ok(None)` on conflict (someone else created one first) — the
    ///   caller should [`load`](Self::load) the winner and adopt it,
    /// - `Err` on other I/O errors.
    ///
    /// Used by boot-time assignment coordination so split-brained halves
    /// don't each overwrite the other's initial assignment: the first
    /// writer wins, losers fall back to whatever's durably stored. See
    /// Phase 1.2 in `docs/plans/cluster-production-readiness.md`.
    ///
    /// # Errors
    /// Object-store I/O or JSON encode failure (distinct from conflict,
    /// which is `Ok(None)`).
    pub async fn save_if_absent(
        &self,
        snapshot: &AssignmentSnapshot,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let path = OsPath::from(SNAPSHOT_PATH);
        let bytes = serde_json::to_vec_pretty(snapshot)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(bytes)), opts)
            .await
        {
            Ok(_) => Ok(Some(snapshot.clone())),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(None),
            Err(e) => Err(SnapshotError::Io(e.to_string())),
        }
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

        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, NodeId(1));
        vnodes.insert(1, NodeId(2));
        let snap = AssignmentSnapshot::empty().next(vnodes);

        s.save(&snap).await.unwrap();
        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, snap);
    }

    #[tokio::test]
    async fn overwrite_wins() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut v1_map = BTreeMap::new();
        v1_map.insert(0, NodeId(1));
        let v1 = AssignmentSnapshot::empty().next(v1_map);
        s.save(&v1).await.unwrap();

        let mut v2_map = BTreeMap::new();
        v2_map.insert(0, NodeId(2));
        let v2 = v1.next(v2_map);
        s.save(&v2).await.unwrap();

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded.version, 2);
        assert_eq!(loaded.vnodes.get(&0), Some(&NodeId(2)));
    }

    #[tokio::test]
    async fn save_if_absent_first_writer_wins() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut first_map = BTreeMap::new();
        first_map.insert(0, NodeId(1));
        first_map.insert(1, NodeId(2));
        let first = AssignmentSnapshot::empty().next(first_map);

        let winner = s.save_if_absent(&first).await.unwrap();
        assert_eq!(winner.as_ref(), Some(&first), "first writer must win");

        // Second writer attempts a different assignment; should be
        // rejected without mutating the store.
        let mut second_map = BTreeMap::new();
        second_map.insert(0, NodeId(99));
        let second = AssignmentSnapshot::empty().next(second_map);
        let rejected = s.save_if_absent(&second).await.unwrap();
        assert!(rejected.is_none(), "second writer must lose the CAS");

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, first, "stored snapshot is the first writer's");
    }

    #[test]
    fn empty_starts_at_version_zero() {
        let s = AssignmentSnapshot::empty();
        assert_eq!(s.version, 0);
        assert!(s.vnodes.is_empty());
    }

    #[test]
    fn next_bumps_version() {
        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, NodeId(1));
        let s = AssignmentSnapshot::empty().next(vnodes);
        assert_eq!(s.version, 1);
    }

    #[test]
    fn roundtrip_vec_conversions() {
        let assignment = vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)];
        let map = AssignmentSnapshot::vnodes_from_vec(&assignment);
        let snap = AssignmentSnapshot::empty().next(map);
        let back = snap.to_vnode_vec(u32::try_from(assignment.len()).expect("test len fits u32"));
        assert_eq!(back, assignment);
    }
}
