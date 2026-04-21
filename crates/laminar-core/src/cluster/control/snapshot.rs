//! Durable vnode→instance assignment snapshots. One object per
//! version at `control/assignment-snapshots/v{N:016}.json`. Chitchat
//! carries the ephemeral copy; these files survive full-cluster
//! restart.
//!
//! Serialises the `VnodeRegistry`'s assignment so every node reaches
//! the same vnode topology from the same source of truth — critical for
//! split-brain correctness: both partitioned halves observe the same
//! stored snapshot instead of each independently computing a fresh
//! round-robin that would have disjoint vnode owners. See Phase 1.2
//! in `docs/plans/cluster-production-readiness.md`.
//!
//! ## CAS protocol
//!
//! Rotation uses `PutMode::Create` on a per-version path — the first
//! writer to land `v{N}.json` wins that version; later writers see
//! `AlreadyExists` and must reload. Backend-agnostic: every object
//! store in the `object_store` crate supports `PutMode::Create`,
//! including `LocalFileSystem` (the harness's backend) which does not
//! yet support `PutMode::Update`. Older etag-based schemes require
//! conditional PUTs that `LocalFileSystem` can't provide.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::cluster::discovery::NodeId;

const SNAPSHOT_PREFIX: &str = "control/assignment-snapshots/";

fn snapshot_path(version: u64) -> OsPath {
    // Fixed-width so lexicographic list order matches numeric order.
    OsPath::from(format!("{SNAPSHOT_PREFIX}v{version:016}.json"))
}

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

    /// Scan the snapshot prefix and return every stored version in
    /// ascending order. Cheap on small clusters (rotations are rare);
    /// the list operation is one round trip on every backend.
    async fn list_versions(&self) -> Result<Vec<u64>, SnapshotError> {
        let prefix = OsPath::from(SNAPSHOT_PREFIX);
        let mut entries = self.store.list(Some(&prefix));
        let mut versions: Vec<u64> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| SnapshotError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
            // Accept only `v{N:016}.json` entries so unrelated
            // siblings in the bucket can't shift the CAS token.
            let Some(rest) = loc.strip_prefix(SNAPSHOT_PREFIX) else {
                continue;
            };
            let Some(num) = rest.strip_prefix('v').and_then(|s| s.strip_suffix(".json")) else {
                continue;
            };
            if let Ok(v) = num.parse::<u64>() {
                versions.push(v);
            }
        }
        versions.sort_unstable();
        Ok(versions)
    }

    /// Load the current (highest-versioned) snapshot; `Ok(None)` on
    /// fresh cluster.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
    pub async fn load(&self) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let versions = self.list_versions().await?;
        let Some(&latest) = versions.last() else {
            return Ok(None);
        };
        self.load_version(latest).await
    }

    /// Load a specific version's snapshot. `Ok(None)` if that version
    /// was never written or has been pruned.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
    pub async fn load_version(
        &self,
        version: u64,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let path = snapshot_path(version);
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

    /// CAS-create a snapshot at `snapshot.version`. Each version is
    /// its own object, so the first writer to land the `v{N}.json`
    /// path for that version wins. Use this for the initial seed
    /// (`snapshot.version == 1`) AND for every rotation.
    ///
    /// Returns:
    /// - `Ok(Some(snapshot))` if our write landed,
    /// - `Ok(None)` on CAS conflict (another writer got there first),
    /// - `Err` on any non-CAS I/O or JSON error.
    ///
    /// # Errors
    /// Object-store I/O or JSON encode failure (conflict is
    /// `Ok(None)`).
    pub async fn save_if_absent(
        &self,
        snapshot: &AssignmentSnapshot,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let path = snapshot_path(snapshot.version);
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

    /// Rotate: propose `snapshot` as the next canonical version,
    /// assuming the current durable version is `prior_version`. The
    /// CAS lands iff no other writer has already produced
    /// `prior_version + 1`. Enforces `snapshot.version ==
    /// prior_version + 1` so callers can't accidentally skip a
    /// generation.
    ///
    /// Returns:
    /// - [`RotateOutcome::Rotated`] if our write landed,
    /// - [`RotateOutcome::Conflict`] carrying the canonical snapshot
    ///   a racing writer produced (caller must adopt it rather than
    ///   retry),
    /// - `Err` on any non-CAS I/O or JSON error.
    ///
    /// # Errors
    /// Object-store I/O, JSON encode, or a version-mismatch bug in
    /// the caller.
    pub async fn save_if_version(
        &self,
        snapshot: &AssignmentSnapshot,
        prior_version: u64,
    ) -> Result<RotateOutcome, SnapshotError> {
        if snapshot.version != prior_version + 1 {
            return Err(SnapshotError::Io(format!(
                "save_if_version requires monotonic +1 bump: prior={prior_version}, \
                 proposed={}",
                snapshot.version,
            )));
        }
        if self.save_if_absent(snapshot).await?.is_some() {
            return Ok(RotateOutcome::Rotated);
        }
        // Someone else produced `prior_version + 1`. Load the
        // canonical successor (which is necessarily the
        // highest-versioned object at this point, because versions
        // are monotonic and CAS is exclusive).
        let winner = self
            .load_version(snapshot.version)
            .await?
            .ok_or_else(|| {
                SnapshotError::Io(
                    "CAS conflict on save_if_absent but subsequent load returned None"
                        .into(),
                )
            })?;
        Ok(RotateOutcome::Conflict(winner))
    }

    /// Delete every snapshot object with `version < before`. Called
    /// after leadership confirms every live node has adopted a
    /// newer version, so old ones are no longer reachable. Missing
    /// objects are tolerated (idempotent retry).
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn prune_before(&self, before: u64) -> Result<(), SnapshotError> {
        let versions = self.list_versions().await?;
        for v in versions {
            if v >= before {
                break;
            }
            let path = snapshot_path(v);
            match self.store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => {
                    tracing::warn!(version = v, error = %e, "snapshot prune: delete failed");
                }
            }
        }
        Ok(())
    }
}

/// Outcome of [`AssignmentSnapshotStore::save_if_version`].
#[derive(Debug, Clone)]
pub enum RotateOutcome {
    /// Our write landed. The snapshot we passed in is now canonical.
    Rotated,
    /// Another writer (a racing leader) won the CAS. The attached
    /// snapshot is what's currently durable; the caller must adopt it
    /// rather than retry with a stale view.
    Conflict(AssignmentSnapshot),
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
    async fn save_if_absent_then_load_roundtrip() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, NodeId(1));
        vnodes.insert(1, NodeId(2));
        let snap = AssignmentSnapshot::empty().next(vnodes);

        assert_eq!(
            s.save_if_absent(&snap).await.unwrap().as_ref(),
            Some(&snap),
        );
        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, snap);
    }

    #[tokio::test]
    async fn load_returns_highest_version() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut v1_map = BTreeMap::new();
        v1_map.insert(0, NodeId(1));
        let v1 = AssignmentSnapshot::empty().next(v1_map);
        s.save_if_absent(&v1).await.unwrap();

        let mut v2_map = BTreeMap::new();
        v2_map.insert(0, NodeId(2));
        let v2 = v1.next(v2_map);
        // Rotate via save_if_version — the canonical post-boot path.
        assert!(matches!(
            s.save_if_version(&v2, v1.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded.version, 2);
        assert_eq!(loaded.vnodes.get(&0), Some(&NodeId(2)));

        // Older version is still readable directly until pruned.
        let v1_loaded = s.load_version(1).await.unwrap().unwrap();
        assert_eq!(v1_loaded, v1);
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

    #[tokio::test]
    async fn save_if_version_rejects_non_monotonic_bump() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut m = BTreeMap::new();
        m.insert(0, NodeId(1));
        let v1 = AssignmentSnapshot::empty().next(m);
        s.save_if_absent(&v1).await.unwrap();

        // Caller builds v3 but claims prior=1 — enforcing monotonic +1
        // catches accidental gap-skipping bugs before they land on
        // durable storage.
        let mut m2 = BTreeMap::new();
        m2.insert(0, NodeId(2));
        let v2 = v1.next(m2);
        let mut m3 = BTreeMap::new();
        m3.insert(0, NodeId(3));
        let v3 = v2.next(m3);
        let err = s.save_if_version(&v3, 1).await.unwrap_err();
        assert!(
            matches!(err, SnapshotError::Io(msg) if msg.contains("monotonic")),
            "non-monotonic bump must surface a clear error",
        );
    }

    #[tokio::test]
    async fn save_if_version_succeeds_on_match() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut v1_map = BTreeMap::new();
        v1_map.insert(0, NodeId(1));
        let first = AssignmentSnapshot::empty().next(v1_map);
        s.save_if_absent(&first).await.unwrap();

        let mut v2_map = BTreeMap::new();
        v2_map.insert(0, NodeId(2));
        let second = first.next(v2_map);
        let outcome = s.save_if_version(&second, first.version).await.unwrap();
        assert!(matches!(outcome, RotateOutcome::Rotated));

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, second);
    }

    #[tokio::test]
    async fn save_if_version_conflict_surfaces_winner() {
        // Two racing rotations both propose v2 from v1. CAS at
        // `v{2}.json` picks one; the loser reloads and finds the
        // winner's canonical snapshot.
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut seed = BTreeMap::new();
        seed.insert(0, NodeId(1));
        let v1 = AssignmentSnapshot::empty().next(seed);
        s.save_if_absent(&v1).await.unwrap();

        let mut winner_map = BTreeMap::new();
        winner_map.insert(0, NodeId(10));
        let winner = v1.next(winner_map);
        assert!(matches!(
            s.save_if_version(&winner, v1.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        let mut loser_map = BTreeMap::new();
        loser_map.insert(0, NodeId(20));
        let loser = v1.next(loser_map);
        match s.save_if_version(&loser, v1.version).await.unwrap() {
            RotateOutcome::Conflict(current) => {
                assert_eq!(
                    current, winner,
                    "conflict must surface the winner's snapshot",
                );
            }
            RotateOutcome::Rotated => {
                panic!("stale-token update must not win the CAS");
            }
        }

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, winner, "stored snapshot is the CAS winner's");
    }

    #[tokio::test]
    async fn prune_before_drops_old_versions() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        // Seed v1..=v4 by repeatedly rotating.
        let mut m = BTreeMap::new();
        m.insert(0, NodeId(1));
        let mut current = AssignmentSnapshot::empty().next(m);
        s.save_if_absent(&current).await.unwrap();
        for _ in 0..3 {
            let next = current.next(current.vnodes.clone());
            s.save_if_version(&next, current.version).await.unwrap();
            current = next;
        }

        s.prune_before(3).await.unwrap();

        assert!(s.load_version(1).await.unwrap().is_none());
        assert!(s.load_version(2).await.unwrap().is_none());
        assert!(s.load_version(3).await.unwrap().is_some());
        assert!(s.load_version(4).await.unwrap().is_some());
        // `load()` still returns the most recent surviving snapshot.
        assert_eq!(s.load().await.unwrap().unwrap().version, 4);
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
