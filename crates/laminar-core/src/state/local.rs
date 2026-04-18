//! [`LocalBackend`] — single-node durable state. `mmap` for watermarks,
//! `object_store::LocalFileSystem` for partials.
//!
//! ## Layout on disk
//!
//! ```text
//! <root>/
//!   watermarks.bin              # mmap, vnode_capacity × 8 bytes
//!   partials/
//!     epoch=1/
//!       vnode=0/partial.bin
//!       vnode=1/partial.bin
//!     epoch=2/...
//! ```
//!
//! ## Why `mmap` for watermarks
//!
//! Watermark advancement is on the hot path. A naive `write + fsync`
//! per update costs tens of microseconds per call; an `mmap`'d
//! `AtomicI64` store is a single `release` store (~5ns). The OS
//! handles dirty-page writeback lazily, which is acceptable because
//! watermarks are recoverable from upstream sources on restart (they
//! are a cache, not a durability invariant).
//!
//! ## Why `object_store::LocalFileSystem` for partials
//!
//! So the object-store layout is wire-identical between `LocalBackend`
//! and `ObjectStoreBackend`. A developer can flip from one to the other
//! by changing config alone without migrating partials.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use memmap2::MmapMut;
use object_store::local::LocalFileSystem;
use object_store::path::Path as OsPath;
use object_store::{ObjectStoreExt, PutPayload};

use super::backend::{StateBackend, StateBackendError};

/// Single-node, durable state backend.
pub struct LocalBackend {
    vnode_capacity: u32,
    mmap: MmapMut,
    partials: LocalFileSystem,
    root: PathBuf,
}

impl std::fmt::Debug for LocalBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalBackend")
            .field("vnode_capacity", &self.vnode_capacity)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl LocalBackend {
    /// Open (or create) a `LocalBackend` rooted at `path`. Creates the
    /// root and the `partials/` subdirectory if they do not exist.
    ///
    /// # Errors
    /// - [`StateBackendError::Io`] if the filesystem is unwritable, the
    ///   watermark file cannot be opened/resized, or `mmap` fails.
    pub fn open(
        path: impl AsRef<Path>,
        vnode_capacity: u32,
    ) -> Result<Self, StateBackendError> {
        let root = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root).map_err(|e| StateBackendError::Io(e.to_string()))?;
        let partials_root = root.join("partials");
        std::fs::create_dir_all(&partials_root).map_err(|e| StateBackendError::Io(e.to_string()))?;

        let wm_path = root.join("watermarks.bin");
        let was_new = !wm_path.exists();
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&wm_path)
            .map_err(|e| StateBackendError::Io(e.to_string()))?;
        let expected_len = u64::from(vnode_capacity) * 8;
        let current_len = file
            .metadata()
            .map_err(|e| StateBackendError::Io(e.to_string()))?
            .len();
        if current_len < expected_len {
            file.set_len(expected_len)
                .map_err(|e| StateBackendError::Io(e.to_string()))?;
        }
        // SAFETY: Mapping our own exclusively-opened file; the file
        // persists for the lifetime of `mmap` because we hold the fd
        // inside the `MmapMut`'s private handle.
        let mut mmap = unsafe {
            MmapMut::map_mut(&file).map_err(|e| StateBackendError::Io(e.to_string()))?
        };
        drop(file);

        if was_new {
            // Initialize all slots to i64::MIN so a fresh backend
            // reports "no watermark observed" until a publish lands.
            let sentinel = i64::MIN.to_ne_bytes();
            for v in 0..vnode_capacity as usize {
                let offset = v * 8;
                mmap[offset..offset + 8].copy_from_slice(&sentinel);
            }
            mmap.flush().map_err(|e| StateBackendError::Io(e.to_string()))?;
        }

        let partials = LocalFileSystem::new_with_prefix(&partials_root)
            .map_err(|e| StateBackendError::Io(e.to_string()))?;

        Ok(Self {
            vnode_capacity,
            mmap,
            partials,
            root,
        })
    }

    /// Vnode range this backend is configured for.
    #[must_use]
    pub fn vnode_capacity(&self) -> u32 {
        self.vnode_capacity
    }

    /// Filesystem root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
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

    fn watermark_slot(&self, vnode: u32) -> &AtomicI64 {
        let offset = (vnode as usize) * 8;
        // SAFETY:
        // - `vnode < vnode_capacity` is checked by callers.
        // - The mmap is `vnode_capacity * 8` bytes, so `offset..offset+8` is in bounds.
        // - `mmap.as_ptr()` is page-aligned; every slot starts at a
        //   multiple of 8 → the resulting pointer is 8-byte aligned.
        // - `AtomicI64` has the same layout as `i64` (`#[repr(transparent)]`),
        //   so the cast is well-defined.
        // - Shared `&AtomicI64` is sufficient for atomic reads/writes
        //   because the atomic type uses interior mutability.
        unsafe {
            let base = self.mmap.as_ptr();
            #[allow(clippy::cast_ptr_alignment)] // mmap is page-aligned, offset is a multiple of 8
            let slot_ptr = base.add(offset).cast::<AtomicI64>();
            &*slot_ptr
        }
    }

    fn partial_path(epoch: u64, vnode: u32) -> OsPath {
        OsPath::from(format!("epoch={epoch}/vnode={vnode}/partial.bin"))
    }
}

#[async_trait]
impl StateBackend for LocalBackend {
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        let path = Self::partial_path(epoch, vnode);
        self.partials
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
        match self.partials.get(&path).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                Ok(Some(bytes))
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
        let slot = self.watermark_slot(vnode);
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
            let val = self.watermark_slot(v).load(Ordering::Acquire);
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
        for &v in vnodes {
            self.check_vnode(v)?;
            let path = Self::partial_path(epoch, v);
            match self.partials.head(&path).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(e) => return Err(StateBackendError::Io(e.to_string())),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let backend = LocalBackend::open(dir.path(), 4).unwrap();
        let payload = Bytes::from_static(b"hello");
        backend
            .write_partial(2, 7, payload.clone())
            .await
            .unwrap();
        let got = backend.read_partial(2, 7).await.unwrap().unwrap();
        assert_eq!(got, payload);
        assert!(backend.read_partial(2, 8).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn watermarks_survive_reopen() {
        let dir = tempdir().unwrap();
        {
            let backend = LocalBackend::open(dir.path(), 4).unwrap();
            backend.publish_watermark(1, 1_000_000).await.unwrap();
            backend.publish_watermark(2, 2_000_000).await.unwrap();
        }
        // Reopen and confirm watermarks persisted across "process" boundary.
        let backend = LocalBackend::open(dir.path(), 4).unwrap();
        assert_eq!(
            backend.global_watermark(&[1, 2]).await.unwrap(),
            1_000_000
        );
        assert_eq!(
            backend.global_watermark(&[2]).await.unwrap(),
            2_000_000
        );
    }

    #[tokio::test]
    async fn partials_survive_reopen() {
        let dir = tempdir().unwrap();
        {
            let backend = LocalBackend::open(dir.path(), 4).unwrap();
            backend
                .write_partial(0, 1, Bytes::from_static(b"first-epoch"))
                .await
                .unwrap();
        }
        let backend = LocalBackend::open(dir.path(), 4).unwrap();
        let got = backend.read_partial(0, 1).await.unwrap().unwrap();
        assert_eq!(&got[..], b"first-epoch");
    }

    #[tokio::test]
    async fn epoch_complete_derived_from_filesystem() {
        let dir = tempdir().unwrap();
        let backend = LocalBackend::open(dir.path(), 4).unwrap();
        let vnodes = [0u32, 1, 2];
        assert!(!backend.epoch_complete(1, &vnodes).await.unwrap());

        for v in &vnodes {
            backend
                .write_partial(*v, 1, Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
        // Drop + reopen — epoch_complete must still say true (derived
        // from filesystem, not in-memory state).
        drop(backend);
        let backend = LocalBackend::open(dir.path(), 4).unwrap();
        assert!(backend.epoch_complete(1, &vnodes).await.unwrap());
        assert!(!backend.epoch_complete(2, &vnodes).await.unwrap());
    }

    #[tokio::test]
    async fn watermark_monotone() {
        let dir = tempdir().unwrap();
        let backend = LocalBackend::open(dir.path(), 4).unwrap();
        backend.publish_watermark(0, 100).await.unwrap();
        backend.publish_watermark(0, 50).await.unwrap();
        backend.publish_watermark(0, 200).await.unwrap();
        assert_eq!(backend.global_watermark(&[0]).await.unwrap(), 200);
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        use std::sync::Arc;
        let dir = tempdir().unwrap();
        let _: Arc<dyn StateBackend> = Arc::new(LocalBackend::open(dir.path(), 2).unwrap());
    }
}
