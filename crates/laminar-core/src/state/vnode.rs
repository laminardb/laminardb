//! [`VnodeRegistry`] — runtime-configurable virtual node topology.
//!
//! Replaces the compile-time `VNODE_COUNT` constant that previously
//! lived in `laminar-storage`. A registry owns:
//!
//! - the current vnode count (configurable; 256 by default),
//! - the node-per-vnode assignment (for distributed modes),
//! - a monotonically increasing `assignment_version` used by
//!   [`ObjectStoreBackend`](super::object_store::ObjectStoreBackend) to
//!   fence out stale writers.
//!
//! Vnode assignment is derived from the row's primary key via
//! [`key_hash`] (xxh3) and modulo `vnode_count`. Connectors that
//! need a vnode ID for an event call [`VnodeRegistry::vnode_for_key`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

/// Identifier for a cluster node owning some subset of vnodes.
///
/// `u64::MAX` is the sentinel for "unassigned".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Sentinel value for an unowned vnode.
    pub const UNASSIGNED: Self = Self(u64::MAX);

    /// Returns true if this is the unassigned sentinel.
    #[must_use]
    pub const fn is_unassigned(&self) -> bool {
        self.0 == u64::MAX
    }
}

/// Runtime registry of vnode topology and assignment.
pub struct VnodeRegistry {
    vnode_count: u32,
    assignment: RwLock<Arc<[NodeId]>>,
    assignment_version: AtomicU64,
}

impl std::fmt::Debug for VnodeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VnodeRegistry")
            .field("vnode_count", &self.vnode_count)
            .field(
                "assignment_version",
                &self.assignment_version.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl VnodeRegistry {
    /// Create a registry sized for `vnode_count` vnodes, all marked
    /// as [`NodeId::UNASSIGNED`]. The assignment version starts at 1.
    #[must_use]
    pub fn new(vnode_count: u32) -> Self {
        let assignment: Arc<[NodeId]> = std::iter::repeat_n(NodeId::UNASSIGNED, vnode_count as usize)
            .collect::<Vec<_>>()
            .into();
        Self {
            vnode_count,
            assignment: RwLock::new(assignment),
            assignment_version: AtomicU64::new(1),
        }
    }

    /// Create a registry where every vnode is owned by the same node.
    ///
    /// Used by single-instance / embedded deployments.
    #[must_use]
    pub fn single_owner(vnode_count: u32, owner: NodeId) -> Self {
        let assignment: Arc<[NodeId]> = std::iter::repeat_n(owner, vnode_count as usize)
            .collect::<Vec<_>>()
            .into();
        Self {
            vnode_count,
            assignment: RwLock::new(assignment),
            assignment_version: AtomicU64::new(1),
        }
    }

    /// Number of vnodes.
    #[must_use]
    pub fn vnode_count(&self) -> u32 {
        self.vnode_count
    }

    /// Current monotonic assignment version.
    #[must_use]
    pub fn assignment_version(&self) -> u64 {
        self.assignment_version.load(Ordering::Acquire)
    }

    /// Owner of a given vnode. Returns [`NodeId::UNASSIGNED`] if the
    /// vnode is out of range or unassigned.
    #[must_use]
    pub fn owner(&self, vnode: u32) -> NodeId {
        if vnode >= self.vnode_count {
            return NodeId::UNASSIGNED;
        }
        self.assignment.read()[vnode as usize]
    }

    /// Snapshot the current assignment vector. Cheap — internally an
    /// `Arc::clone`.
    #[must_use]
    pub fn snapshot(&self) -> Arc<[NodeId]> {
        Arc::clone(&self.assignment.read())
    }

    /// Replace the full assignment and bump the version.
    ///
    /// # Panics
    /// Panics if `new_assignment.len() != self.vnode_count`.
    pub fn set_assignment(&self, new_assignment: Arc<[NodeId]>) {
        assert_eq!(
            new_assignment.len(),
            self.vnode_count as usize,
            "assignment length mismatch: got {}, expected {}",
            new_assignment.len(),
            self.vnode_count,
        );
        *self.assignment.write() = new_assignment;
        self.assignment_version.fetch_add(1, Ordering::AcqRel);
    }

    /// Map a primary key to a vnode.
    #[must_use]
    pub fn vnode_for_key(&self, key: &[u8]) -> u32 {
        #[allow(clippy::cast_possible_truncation)]
        let h = (key_hash(key) % u64::from(self.vnode_count)) as u32;
        h
    }
}

/// Hash a key to a 64-bit value. Used to derive vnode IDs and for any
/// other keyed-partitioning decisions.
///
/// Fixed to xxh3 so all pipeline stages produce the same vnode for the
/// same key without needing to share a hasher instance.
#[must_use]
pub fn key_hash(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_registry_is_unassigned() {
        let r = VnodeRegistry::new(8);
        assert_eq!(r.vnode_count(), 8);
        for v in 0..8 {
            assert!(r.owner(v).is_unassigned());
        }
    }

    #[test]
    fn single_owner_populates_all_slots() {
        let r = VnodeRegistry::single_owner(4, NodeId(42));
        for v in 0..4 {
            assert_eq!(r.owner(v), NodeId(42));
        }
    }

    #[test]
    fn set_assignment_bumps_version() {
        let r = VnodeRegistry::new(4);
        let v0 = r.assignment_version();
        let new_assign: Arc<[NodeId]> = vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)].into();
        r.set_assignment(new_assign);
        assert!(r.assignment_version() > v0);
        assert_eq!(r.owner(0), NodeId(1));
        assert_eq!(r.owner(1), NodeId(2));
    }

    #[test]
    fn vnode_for_key_in_range() {
        let r = VnodeRegistry::new(16);
        for i in 0..100 {
            let v = r.vnode_for_key(format!("k-{i}").as_bytes());
            assert!(v < 16);
        }
    }

    #[test]
    #[should_panic(expected = "assignment length mismatch")]
    fn set_assignment_rejects_wrong_length() {
        let r = VnodeRegistry::new(4);
        let bad: Arc<[NodeId]> = vec![NodeId(1)].into();
        r.set_assignment(bad);
    }

    #[test]
    fn owner_out_of_range_returns_unassigned() {
        let r = VnodeRegistry::single_owner(4, NodeId(1));
        assert!(r.owner(10).is_unassigned());
    }

    #[test]
    fn vnode_for_key_is_deterministic() {
        let r = VnodeRegistry::new(16);
        assert_eq!(r.vnode_for_key(b"key-x"), r.vnode_for_key(b"key-x"));
    }
}
