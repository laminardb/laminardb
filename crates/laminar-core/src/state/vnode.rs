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

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Unique identifier for a node. Also the owner id for vnodes; cluster
/// membership and vnode ownership identify the same thing.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Sentinel meaning "unassigned".
    pub const UNASSIGNED: Self = Self(0);

    /// True if this is the unassigned sentinel.
    #[must_use]
    pub const fn is_unassigned(&self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
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
        let assignment: Arc<[NodeId]> =
            std::iter::repeat_n(NodeId::UNASSIGNED, vnode_count as usize)
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

/// Build a vnode-to-owner assignment by round-robin across sorted peers.
///
/// Deterministic for a given `(vnode_count, peers)` input — every node
/// computing this independently agrees, given the same peer list. Uses
/// sort-by-id + modulo: vnode `v` → `peers[v % peers.len()]` after
/// sorting. Simple and fine for uniform key distributions.
///
/// **Not** consistent hashing — a peer join/leave reshuffles every
/// vnode. Acceptable for the current phase since reassignment happens
/// only at cluster start; dynamic rebalance is deferred work.
///
/// # Panics
/// Panics if `peers` is empty.
#[must_use]
pub fn round_robin_assignment(vnode_count: u32, peers: &[NodeId]) -> Arc<[NodeId]> {
    assert!(
        !peers.is_empty(),
        "round_robin_assignment needs at least one peer"
    );
    let mut sorted: Vec<NodeId> = peers.to_vec();
    sorted.sort_by_key(|n| n.0);
    (0..vnode_count)
        .map(|v| sorted[(v as usize) % sorted.len()])
        .collect::<Vec<_>>()
        .into()
}

/// Vnodes currently assigned to `owner`.
///
/// Used by the checkpoint coordinator to decide which vnodes' durability
/// markers it is responsible for writing each epoch, and by the leader's
/// `epoch_complete` gate to know the full set to check.
#[must_use]
pub fn owned_vnodes(registry: &VnodeRegistry, owner: NodeId) -> Vec<u32> {
    (0..registry.vnode_count())
        .filter(|&v| registry.owner(v) == owner)
        .collect()
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

    #[test]
    fn owned_vnodes_filters_by_owner() {
        let r = VnodeRegistry::new(4);
        r.set_assignment(vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)].into());
        assert_eq!(owned_vnodes(&r, NodeId(1)), vec![0, 2]);
        assert_eq!(owned_vnodes(&r, NodeId(2)), vec![1, 3]);
        assert!(owned_vnodes(&r, NodeId(99)).is_empty());
    }

    #[test]
    fn owned_vnodes_single_owner_returns_all() {
        let r = VnodeRegistry::single_owner(8, NodeId(42));
        assert_eq!(owned_vnodes(&r, NodeId(42)), (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn round_robin_is_deterministic_and_balanced() {
        // 8 vnodes, 3 peers → 3+3+2 distribution after modulo.
        let peers = vec![NodeId(7), NodeId(3), NodeId(5)];
        let assignment = round_robin_assignment(8, &peers);
        // Sorted: [3, 5, 7]. vnode v owned by sorted[v % 3].
        assert_eq!(
            &*assignment,
            &[
                NodeId(3),
                NodeId(5),
                NodeId(7),
                NodeId(3),
                NodeId(5),
                NodeId(7),
                NodeId(3),
                NodeId(5),
            ][..]
        );
        // Input order doesn't matter.
        let reversed = vec![NodeId(3), NodeId(5), NodeId(7)];
        assert_eq!(round_robin_assignment(8, &reversed), assignment);
    }

    #[test]
    fn round_robin_single_peer_owns_everything() {
        let assignment = round_robin_assignment(4, &[NodeId(99)]);
        assert!(assignment.iter().all(|&n| n == NodeId(99)));
    }

    #[test]
    #[should_panic(expected = "at least one peer")]
    fn round_robin_rejects_empty_peer_list() {
        let _ = round_robin_assignment(4, &[]);
    }
}
