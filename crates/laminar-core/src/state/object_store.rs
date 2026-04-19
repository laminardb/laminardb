//! [`ObjectStoreBackend`] — durable partial-state storage backed by any
//! `object_store` implementation (S3, GCS, Azure, `LocalFileSystem`).
//!
//! `epoch_complete(epoch, vnodes)` performs a CAS-commit: if every
//! vnode's `partial.bin` is present, `put(_COMMIT, Create)` seals the
//! epoch. The `_COMMIT` marker is the durability boundary the
//! checkpoint coordinator consults before releasing sinks.

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
        }
    }

    /// Vnode range this backend is configured for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        self.vnode_capacity
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
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
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
        match self.store.head(&commit).await {
            Ok(_) => return Ok(true),
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
        assert!(backend.epoch_complete(1, &vnodes).await.unwrap());
        // Idempotent.
        assert!(backend.epoch_complete(1, &vnodes).await.unwrap());
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        let dir = tempdir().unwrap();
        let _: Arc<dyn StateBackend> =
            Arc::new(ObjectStoreBackend::new(make_store(dir.path()), "node-0", 2));
    }
}
