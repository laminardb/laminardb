//! [`InProcessBackend`] — non-durable [`StateBackend`] backed by an
//! in-memory hashmap. Used for tests and embedded single-process runs.

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use super::backend::{StateBackend, StateBackendError};

/// In-process, non-durable state backend.
#[derive(Debug)]
pub struct InProcessBackend {
    partials: RwLock<FxHashMap<(u32, u64), Bytes>>,
    vnode_capacity: u32,
}

impl InProcessBackend {
    /// Create a new backend sized for `vnode_capacity` vnodes.
    #[must_use]
    pub fn new(vnode_capacity: u32) -> Self {
        Self {
            partials: RwLock::new(FxHashMap::default()),
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
}

#[async_trait]
impl StateBackend for InProcessBackend {
    /// In-process backend opts out of the split-brain fence — there's
    /// only one process so the scenario is moot. `assignment_version`
    /// is accepted and ignored.
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
        _assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        self.partials.write().insert((vnode, epoch), bytes);
        Ok(())
    }

    async fn read_partial(
        &self,
        vnode: u32,
        epoch: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.check_vnode(vnode)?;
        Ok(self.partials.read().get(&(vnode, epoch)).cloned())
    }

    async fn epoch_complete(
        &self,
        epoch: u64,
        vnodes: &[u32],
    ) -> Result<bool, StateBackendError> {
        let map = self.partials.read();
        for &v in vnodes {
            self.check_vnode(v)?;
            if !map.contains_key(&(v, epoch)) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
        // Without this, every checkpoint leaks one Bytes per vnode
        // forever.
        self.partials.write().retain(|&(_, epoch), _| epoch >= before);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_read_roundtrip() {
        let b = InProcessBackend::new(4);
        let payload = Bytes::from_static(b"hello");
        b.write_partial(2, 7, 0, payload.clone()).await.unwrap();
        let got = b.read_partial(2, 7).await.unwrap().unwrap();
        assert_eq!(got, payload);
        assert!(b.read_partial(2, 8).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn epoch_complete_requires_every_vnode() {
        let b = InProcessBackend::new(4);
        let vnodes = [0u32, 1, 2];
        assert!(!b.epoch_complete(1, &vnodes).await.unwrap());
        b.write_partial(0, 1, 0, Bytes::from_static(b"a")).await.unwrap();
        b.write_partial(1, 1, 0, Bytes::from_static(b"b")).await.unwrap();
        assert!(!b.epoch_complete(1, &vnodes).await.unwrap());
        b.write_partial(2, 1, 0, Bytes::from_static(b"c")).await.unwrap();
        assert!(b.epoch_complete(1, &vnodes).await.unwrap());
        assert!(!b.epoch_complete(2, &vnodes).await.unwrap());
    }

    #[tokio::test]
    async fn out_of_range_vnode_errors() {
        let b = InProcessBackend::new(2);
        let r = b
            .write_partial(5, 1, 0, Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(matches!(r, StateBackendError::Io(_)));
    }

    #[test]
    fn state_backend_is_object_safe() {
        let _: std::sync::Arc<dyn StateBackend> =
            std::sync::Arc::new(InProcessBackend::new(2));
    }

    #[tokio::test]
    async fn prune_before_drops_old_epochs() {
        let b = InProcessBackend::new(4);
        for epoch in 1..=5 {
            b.write_partial(0, epoch, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
            b.write_partial(1, epoch, 0, Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
        // Retain epochs >= 4. Entries for 1,2,3 must go away.
        b.prune_before(4).await.unwrap();
        for epoch in 1..=3 {
            assert!(b.read_partial(0, epoch).await.unwrap().is_none());
            assert!(b.read_partial(1, epoch).await.unwrap().is_none());
        }
        for epoch in 4..=5 {
            assert!(b.read_partial(0, epoch).await.unwrap().is_some());
            assert!(b.read_partial(1, epoch).await.unwrap().is_some());
        }
    }
}
