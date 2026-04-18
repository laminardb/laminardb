//! [`InProcessBackend`] — the simplest, fastest [`StateBackend`]. All
//! state lives in process memory. Watermarks are a vector of atomics
//! with no locks on the read path; partials are in a `parking_lot`
//! `RwLock`-guarded map, cold-path-only.
//!
//! Suitable for:
//!
//! - Unit and integration tests that need a real backend.
//! - Embedded, single-process users who do not require durability
//!   across restarts.
//!
//! Not suitable when restart survival or multi-node coordination is
//! required — reach for [`LocalBackend`](super::local::LocalBackend) or
//! [`ObjectStoreBackend`](super::object_store::ObjectStoreBackend) instead.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use super::backend::{StateBackend, StateBackendError};

/// In-process, non-durable state backend.
#[derive(Debug)]
pub struct InProcessBackend {
    watermarks: Arc<Vec<AtomicI64>>,
    partials: RwLock<FxHashMap<(u32, u64), Bytes>>,
    vnode_capacity: u32,
}

impl InProcessBackend {
    /// Create a new backend sized for `vnode_capacity` vnodes.
    ///
    /// Vnode IDs `[0, vnode_capacity)` are valid; anything outside the
    /// range returns [`StateBackendError::Io`] from the respective
    /// method.
    #[must_use]
    pub fn new(vnode_capacity: u32) -> Self {
        let watermarks: Vec<AtomicI64> = (0..vnode_capacity)
            .map(|_| AtomicI64::new(i64::MIN))
            .collect();
        Self {
            watermarks: Arc::new(watermarks),
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
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
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

    async fn publish_watermark(
        &self,
        vnode: u32,
        ts_ms: i64,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        let slot = &self.watermarks[vnode as usize];
        // Monotone CAS loop: watermarks never go backwards.
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
        let map = self.partials.read();
        for &v in vnodes {
            self.check_vnode(v)?;
            if !map.contains_key(&(v, epoch)) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_read_roundtrip() {
        let b = InProcessBackend::new(4);
        let payload = Bytes::from_static(b"hello");
        b.write_partial(2, 7, payload.clone()).await.unwrap();
        let got = b.read_partial(2, 7).await.unwrap().unwrap();
        assert_eq!(got, payload);
        assert!(b.read_partial(2, 8).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn watermark_monotone() {
        let b = InProcessBackend::new(4);
        b.publish_watermark(0, 100).await.unwrap();
        b.publish_watermark(0, 50).await.unwrap(); // ignored
        b.publish_watermark(0, 200).await.unwrap();
        assert_eq!(b.global_watermark(&[0]).await.unwrap(), 200);
    }

    #[tokio::test]
    async fn global_watermark_is_min() {
        let b = InProcessBackend::new(4);
        b.publish_watermark(0, 500).await.unwrap();
        b.publish_watermark(1, 200).await.unwrap();
        b.publish_watermark(2, 300).await.unwrap();
        assert_eq!(b.global_watermark(&[0, 1, 2]).await.unwrap(), 200);
        assert_eq!(b.global_watermark(&[0, 2]).await.unwrap(), 300);
    }

    #[tokio::test]
    async fn epoch_complete_requires_every_vnode() {
        let b = InProcessBackend::new(4);
        let vnodes = [0u32, 1, 2];
        assert!(!b.epoch_complete(1, &vnodes).await.unwrap());
        b.write_partial(0, 1, Bytes::from_static(b"a")).await.unwrap();
        b.write_partial(1, 1, Bytes::from_static(b"b")).await.unwrap();
        assert!(!b.epoch_complete(1, &vnodes).await.unwrap());
        b.write_partial(2, 1, Bytes::from_static(b"c")).await.unwrap();
        assert!(b.epoch_complete(1, &vnodes).await.unwrap());
        // Different epoch still incomplete.
        assert!(!b.epoch_complete(2, &vnodes).await.unwrap());
    }

    #[tokio::test]
    async fn out_of_range_vnode_errors() {
        let b = InProcessBackend::new(2);
        let r = b
            .write_partial(5, 1, Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(matches!(r, StateBackendError::Io(_)));
    }

    #[tokio::test]
    async fn empty_vnode_set_returns_i64_min() {
        let b = InProcessBackend::new(4);
        assert_eq!(b.global_watermark(&[]).await.unwrap(), i64::MIN);
    }

    #[test]
    fn state_backend_is_object_safe() {
        let _: Arc<dyn StateBackend> = Arc::new(InProcessBackend::new(2));
    }
}
