//! [`ObjectStoreBackend`] — durable shared-storage backend over the
//! `object_store` crate. Works against S3, GCS, Azure, and
//! `LocalFileSystem` (used by tests). Path layout is wire-identical to
//! [`LocalBackend`](super::local::LocalBackend), so migrating between
//! the two is a config change.
//!
//! ## Split-brain fence
//!
//! Every write carries an `assignment_version`. An externally-managed
//! `authoritative_version` (supplied by the coordination tier — Raft in
//! constellation mode, chitchat gossip in distributed-embedded mode) is
//! compared against the caller's version before the object is put. If
//! the caller's version is older, the write is rejected with
//! [`StateBackendError::StaleAssignment`].
//!
//! In standalone (single-instance) deployments the two versions are
//! identical and never advance; the fence is a no-op.
//!
//! ## Epoch commit
//!
//! `epoch_complete(epoch, vnodes)` performs a CAS-commit:
//!
//! 1. If `epoch={n}/_COMMIT` already exists → return `true`.
//! 2. Otherwise, `HEAD` each `partial.bin`; any missing → `false`.
//! 3. All present → `put_opts(_COMMIT, PutMode::Create)`. On success
//!    or `AlreadyExists` → `true`.
//!
//! The `_COMMIT` marker is the durability boundary downstream readers
//! use to prove an epoch is complete without listing every vnode.
//!
//! ## Watermarks
//!
//! Watermarks are held in-memory until a coordination tier is wired.
//! Watermark advancement is on the hot path; polling the object store
//! for watermark files would add ~100 ms of latency per read. The
//! distributed-embedded tier (added in a later iteration) swaps this
//! in-memory table for a chitchat-gossip feed.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
    assignment_version: AtomicU64,
    authoritative_version: Arc<AtomicU64>,
    watermarks: Arc<Vec<AtomicI64>>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("instance_id", &self.instance_id)
            .field("vnode_capacity", &self.vnode_capacity)
            .field(
                "assignment_version",
                &self.assignment_version.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Wrap an existing [`ObjectStore`]. The `assignment_version` starts
    /// at 1, matching a freshly-created authoritative version.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        let watermarks: Vec<AtomicI64> = (0..vnode_capacity)
            .map(|_| AtomicI64::new(i64::MIN))
            .collect();
        Self {
            store,
            instance_id: instance_id.into(),
            vnode_capacity,
            assignment_version: AtomicU64::new(1),
            authoritative_version: Arc::new(AtomicU64::new(1)),
            watermarks: Arc::new(watermarks),
        }
    }

    /// Vnode range this backend is configured for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        self.vnode_capacity
    }

    /// The local assignment version this instance is writing with.
    #[must_use]
    pub fn assignment_version(&self) -> u64 {
        self.assignment_version.load(Ordering::Acquire)
    }

    /// Bump this instance's assignment version (e.g., after it takes
    /// over a vnode set from a failed peer).
    pub fn advance_assignment_version(&self, v: u64) {
        self.assignment_version
            .fetch_max(v, Ordering::AcqRel);
    }

    /// Handle to the authoritative version atomic. The coordination
    /// tier advances this on rebalance so stale writers are fenced out.
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

    fn check_fence(&self) -> Result<(), StateBackendError> {
        let caller = self.assignment_version.load(Ordering::Acquire);
        let authoritative = self.authoritative_version.load(Ordering::Acquire);
        if caller < authoritative {
            Err(StateBackendError::StaleAssignment {
                caller,
                backend: authoritative,
            })
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
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        self.check_fence()?;
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

    async fn publish_watermark(
        &self,
        vnode: u32,
        ts_ms: i64,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        let slot = &self.watermarks[vnode as usize];
        let mut cur = slot.load(Ordering::Acquire);
        loop {
            if ts_ms <= cur {
                return Ok(());
            }
            match slot.compare_exchange_weak(cur, ts_ms, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(()),
                Err(actual) => cur = actual,
            }
        }
    }

    async fn global_watermark(&self, vnodes: &[u32]) -> Result<i64, StateBackendError> {
        if vnodes.is_empty() {
            return Ok(i64::MIN);
        }
        let mut min = i64::MAX;
        for &v in vnodes {
            self.check_vnode(v)?;
            let val = self.watermarks[v as usize].load(Ordering::Acquire);
            if val < min {
                min = val;
            }
        }
        Ok(min)
    }

    async fn epoch_complete(
        &self,
        epoch: u64,
        vnodes: &[u32],
    ) -> Result<bool, StateBackendError> {
        // Fast path — committed manifest already exists.
        let commit = Self::commit_path(epoch);
        match self.store.head(&commit).await {
            Ok(_) => return Ok(true),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(StateBackendError::Io(e.to_string())),
        }

        // Verify every vnode's partial is present.
        for &v in vnodes {
            self.check_vnode(v)?;
            let path = Self::partial_path(epoch, v);
            match self.store.head(&path).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(e) => return Err(StateBackendError::Io(e.to_string())),
            }
        }

        // CAS the commit marker. Payload is the committer's instance_id
        // so operators can audit who sealed the epoch.
        let payload = PutPayload::from(Bytes::from(self.instance_id.clone()));
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&commit, payload, opts).await {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(true),
            Err(e) => Err(StateBackendError::Io(e.to_string())),
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
            .write_partial(0, 1, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = backend.read_partial(0, 1).await.unwrap().unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[tokio::test]
    async fn stale_assignment_is_rejected() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        // Simulate coordination tier bumping authoritative past us.
        let auth = backend.authoritative_version_handle();
        auth.store(5, Ordering::Release);
        let err = backend
            .write_partial(0, 1, Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        match err {
            StateBackendError::StaleAssignment { caller, backend } => {
                assert_eq!(caller, 1);
                assert_eq!(backend, 5);
            }
            other => panic!("expected StaleAssignment, got {other:?}"),
        }
        // After advancing our version to match, writes succeed.
        backend.advance_assignment_version(5);
        backend
            .write_partial(0, 1, Bytes::from_static(b"x"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn epoch_complete_cas_commit() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32, 1, 2];

        assert!(!backend.epoch_complete(1, &vnodes).await.unwrap());
        for v in &vnodes {
            backend
                .write_partial(*v, 1, Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
        // First call seals the manifest.
        assert!(backend.epoch_complete(1, &vnodes).await.unwrap());
        // Subsequent calls are idempotent (fast path).
        assert!(backend.epoch_complete(1, &vnodes).await.unwrap());
    }

    #[tokio::test]
    async fn path_layout_matches_local_backend() {
        // Wire-compat check: a LocalBackend pointed at the same root
        // reads back what ObjectStoreBackend wrote.
        use super::super::local::LocalBackend;
        let dir = tempdir().unwrap();
        let partials_root = dir.path().join("partials");
        std::fs::create_dir_all(&partials_root).unwrap();

        let os_backend = ObjectStoreBackend::new(make_store(&partials_root), "node-0", 4);
        os_backend
            .write_partial(3, 5, Bytes::from_static(b"shared"))
            .await
            .unwrap();

        let local = LocalBackend::open(dir.path(), 4).unwrap();
        let got = local.read_partial(3, 5).await.unwrap().unwrap();
        assert_eq!(&got[..], b"shared");
    }

    #[tokio::test]
    async fn watermarks_are_monotone() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        backend.publish_watermark(0, 1_000).await.unwrap();
        backend.publish_watermark(0, 500).await.unwrap();
        backend.publish_watermark(0, 2_000).await.unwrap();
        assert_eq!(backend.global_watermark(&[0]).await.unwrap(), 2_000);
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        let dir = tempdir().unwrap();
        let _: Arc<dyn StateBackend> =
            Arc::new(ObjectStoreBackend::new(make_store(dir.path()), "node-0", 2));
    }
}
