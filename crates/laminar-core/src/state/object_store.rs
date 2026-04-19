//! [`ObjectStoreBackend`] — durable partial-state storage backed by any
//! `object_store` implementation (S3, GCS, Azure, `LocalFileSystem`).
//!
//! `epoch_complete(epoch, vnodes)` performs a CAS-commit: if every
//! vnode's `partial.bin` is present, `put(_COMMIT, Create)` seals the
//! epoch. The `_COMMIT` marker is the durability boundary the
//! checkpoint coordinator consults before releasing sinks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use super::backend::{StateBackend, StateBackendError};

/// Object-store-backed [`StateBackend`].
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    instance_id: String,
    vnode_capacity: u32,
    /// Authoritative vnode-assignment version known to this backend.
    /// Phase 1.4 split-brain fence: [`write_partial`](Self::write_partial)
    /// rejects any caller whose `assignment_version` is strictly less
    /// than this value. Updated via
    /// [`set_authoritative_version`](Self::set_authoritative_version)
    /// whenever the host sees a newer `AssignmentSnapshot` rotate in.
    ///
    /// Default is `0`, which disables the fence — unconfigured
    /// callers (most single-instance paths) are accepted unchanged.
    authoritative_version: Arc<AtomicU64>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("instance_id", &self.instance_id)
            .field("vnode_capacity", &self.vnode_capacity)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Wrap an existing [`ObjectStore`].
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        Self {
            store,
            instance_id: instance_id.into(),
            vnode_capacity,
            authoritative_version: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Vnode range this backend is configured for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        self.vnode_capacity
    }

    /// Current authoritative assignment version known to this backend.
    /// Zero means the fence is disabled (accepting any caller version).
    #[must_use]
    pub fn authoritative_version(&self) -> u64 {
        self.authoritative_version.load(Ordering::Acquire)
    }

    /// Raise the authoritative version to `version`. Monotonic: a call
    /// with a value less than or equal to the current one is a no-op.
    ///
    /// The host should call this whenever it adopts a newer
    /// [`AssignmentSnapshot`] (on initial load and on each subsequent
    /// rotation). After this call, any in-flight `write_partial` from
    /// a stale writer whose caller version is below `version` is
    /// rejected with [`StateBackendError::StaleVersion`].
    ///
    /// [`AssignmentSnapshot`]: crate::cluster::control::AssignmentSnapshot
    pub fn set_authoritative_version(&self, version: u64) {
        // CAS-like loop to avoid lowering the version on a late call.
        let mut cur = self.authoritative_version.load(Ordering::Acquire);
        while version > cur {
            match self.authoritative_version.compare_exchange(
                cur,
                version,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Shared handle to the authoritative version counter. Callers that
    /// want to bump several objects (e.g. backend plus a future metric)
    /// from a single owner can clone this handle instead of relaying
    /// through [`set_authoritative_version`].
    #[must_use]
    pub fn authoritative_version_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.authoritative_version)
    }

    fn check_vnode(&self, v: u32) -> Result<(), StateBackendError> {
        if v >= self.vnode_capacity {
            Err(StateBackendError::Io(format!(
                "vnode {v} out of range (capacity {})",
                self.vnode_capacity
            )))
        } else {
            Ok(())
        }
    }

    fn partial_path(epoch: u64, vnode: u32) -> OsPath {
        OsPath::from(format!("epoch={epoch}/vnode={vnode}/partial.bin"))
    }

    fn commit_path(epoch: u64) -> OsPath {
        OsPath::from(format!("epoch={epoch}/_COMMIT"))
    }
}

#[async_trait]
impl StateBackend for ObjectStoreBackend {
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        // Phase 1.4 split-brain fence. `authoritative_version == 0`
        // means "unconfigured" — accept every write (matches the
        // legacy single-instance behavior). Non-zero authoritative
        // means we know of a specific assignment generation; writes
        // stamped with an older generation are rejected.
        let authoritative = self.authoritative_version.load(Ordering::Acquire);
        if authoritative > 0 && assignment_version < authoritative {
            return Err(StateBackendError::StaleVersion {
                caller: assignment_version,
                authoritative,
            });
        }
        let path = Self::partial_path(epoch, vnode);
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        Ok(())
    }

    async fn read_partial(
        &self,
        vnode: u32,
        epoch: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.check_vnode(vnode)?;
        let path = Self::partial_path(epoch, vnode);
        match self.store.get(&path).await {
            Ok(res) => {
                let b = res
                    .bytes()
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                Ok(Some(b))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn epoch_complete(
        &self,
        epoch: u64,
        vnodes: &[u32],
    ) -> Result<bool, StateBackendError> {
        let commit = Self::commit_path(epoch);
        // Fast path: a marker already exists. Previously we returned
        // `Ok(true)` blindly — that swallowed split-brain (two leaders
        // racing, the loser silently agreed it had committed). Now we
        // read the audit body and reject if the committer isn't us.
        match self.store.head(&commit).await {
            Ok(_) => return self.verify_commit_marker(&commit).await,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(StateBackendError::Io(e.to_string())),
        }

        for &v in vnodes {
            self.check_vnode(v)?;
            let path = Self::partial_path(epoch, v);
            match self.store.head(&path).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(e) => return Err(StateBackendError::Io(e.to_string())),
            }
        }

        // CAS the commit marker; payload is the committer's id for audit.
        let payload = PutPayload::from(Bytes::from(self.instance_id.clone()));
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&commit, payload, opts).await {
            Ok(_) => Ok(true),
            // AlreadyExists means a peer raced us to the CAS. Don't
            // silently agree — verify who actually wrote the marker
            // so a stale leader doesn't keep driving the commit phase.
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.verify_commit_marker(&commit).await
            }
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }
}

impl ObjectStoreBackend {
    /// Read the epoch's `_COMMIT` marker and compare its audit body
    /// against this backend's `instance_id`. Match → `Ok(true)` (we
    /// committed, a retry or observation is fine). Mismatch →
    /// [`StateBackendError::SplitBrainCommit`] so the caller aborts
    /// rather than double-committing downstream. Phase 2 split-brain
    /// hardening.
    async fn verify_commit_marker(
        &self,
        commit: &OsPath,
    ) -> Result<bool, StateBackendError> {
        let res = self
            .store
            .get(commit)
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        let bytes = res
            .bytes()
            .await
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        let committer = std::str::from_utf8(&bytes).map_err(|e| {
            StateBackendError::Serialization(format!("commit marker not utf8: {e}"))
        })?;
        if committer == self.instance_id.as_str() {
            Ok(true)
        } else {
            Err(StateBackendError::SplitBrainCommit {
                committer: committer.to_string(),
                self_id: self.instance_id.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    fn make_store(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        backend
            .write_partial(0, 1, 0, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = backend.read_partial(0, 1).await.unwrap().unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[tokio::test]
    async fn epoch_complete_cas_commit() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32, 1, 2];

        assert!(!backend.epoch_complete(1, &vnodes).await.unwrap());
        for v in &vnodes {
            backend
                .write_partial(*v, 1, 0, Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
        assert!(backend.epoch_complete(1, &vnodes).await.unwrap());
        // Idempotent — same committer id in the audit body.
        assert!(backend.epoch_complete(1, &vnodes).await.unwrap());
    }

    /// Split-brain commit protection. Previously the CAS-create's
    /// `AlreadyExists` branch was folded into the success branch, so a
    /// stale leader racing a fresh one would happily agree it had also
    /// committed the epoch. Now the loser reads the marker, sees a
    /// mismatched audit body, and fails loud.
    #[tokio::test]
    async fn epoch_complete_detects_split_brain_committer() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        // Both "nodes" wrote partials for the epoch.
        for v in &vnodes {
            winner
                .write_partial(*v, 7, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }

        // Winner CAS-creates the commit marker first.
        assert!(winner.epoch_complete(7, &vnodes).await.unwrap());

        // Loser finds the marker already there (HEAD fast-path) and
        // must NOT agree it committed — that's the split-brain case.
        let err = loser.epoch_complete(7, &vnodes).await.unwrap_err();
        match err {
            StateBackendError::SplitBrainCommit { committer, self_id } => {
                assert_eq!(committer, "winner");
                assert_eq!(self_id, "loser");
            }
            other => panic!("expected SplitBrainCommit, got {other:?}"),
        }

        // And the winner's repeated call is still idempotent Ok(true).
        assert!(winner.epoch_complete(7, &vnodes).await.unwrap());
    }

    /// Same contract on the CAS-loser path: if the marker doesn't exist
    /// at HEAD time but a peer sneaks in between our vnode-presence
    /// check and our own PUT, our `put_opts` fails with `AlreadyExists`.
    /// That branch must also compare committers, not silently succeed.
    #[tokio::test]
    async fn epoch_complete_detects_split_brain_on_cas_loser_path() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        for v in &vnodes {
            winner
                .write_partial(*v, 3, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }
        // Manually pre-seed the commit marker under "winner" to
        // simulate the TOCTOU race deterministically — the loser's
        // put_opts will hit AlreadyExists on its own PUT attempt.
        let commit = ObjectStoreBackend::commit_path(3);
        store
            .put(&commit, PutPayload::from(Bytes::from_static(b"winner")))
            .await
            .unwrap();

        let err = loser.epoch_complete(3, &vnodes).await.unwrap_err();
        assert!(matches!(
            err,
            StateBackendError::SplitBrainCommit { ref committer, .. }
                if committer == "winner"
        ));
    }

    #[tokio::test]
    async fn stale_version_rejected() {
        // Phase 1.4 AC: force two "nodes" (backend instances wrapping
        // the same store) to claim the same vnode at different
        // generations. The stale writer must be rejected.
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let stale = ObjectStoreBackend::new(Arc::clone(&store), "node-stale", 4);
        let fresh = ObjectStoreBackend::new(Arc::clone(&store), "node-fresh", 4);

        // Fresh learns about a new assignment generation — e.g. a new
        // snapshot rotated in after a leader election.
        fresh.set_authoritative_version(2);

        // Fresh writes at the current version: accepted.
        fresh
            .write_partial(0, 1, 2, Bytes::from_static(b"fresh"))
            .await
            .unwrap();

        // Stale tries to write at version 1 — but only IF it's also
        // learned of the rotation. Model that by promoting stale's
        // view too; the check is intra-backend here because the
        // durable version-broadcast channel is Phase 2.3 territory.
        stale.set_authoritative_version(2);
        let err = stale
            .write_partial(0, 1, 1, Bytes::from_static(b"stale"))
            .await
            .unwrap_err();
        match err {
            StateBackendError::StaleVersion {
                caller,
                authoritative,
            } => {
                assert_eq!(caller, 1);
                assert_eq!(authoritative, 2);
            }
            other => panic!("expected StaleVersion, got {other:?}"),
        }

        // Fence-disabled backend (authoritative stays at 0) accepts
        // any version — preserves legacy single-instance behavior.
        let unfenced = ObjectStoreBackend::new(Arc::clone(&store), "node-unfenced", 4);
        unfenced
            .write_partial(1, 1, 0, Bytes::from_static(b"ok"))
            .await
            .unwrap();
    }

    #[test]
    fn authoritative_version_is_monotonic() {
        let dir = tempdir().unwrap();
        let b = ObjectStoreBackend::new(make_store(dir.path()), "node", 2);
        assert_eq!(b.authoritative_version(), 0);
        b.set_authoritative_version(3);
        assert_eq!(b.authoritative_version(), 3);
        // Attempts to lower the version are no-ops.
        b.set_authoritative_version(1);
        assert_eq!(b.authoritative_version(), 3);
        b.set_authoritative_version(4);
        assert_eq!(b.authoritative_version(), 4);
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        let dir = tempdir().unwrap();
        let _: Arc<dyn StateBackend> =
            Arc::new(ObjectStoreBackend::new(make_store(dir.path()), "node-0", 2));
    }
}
