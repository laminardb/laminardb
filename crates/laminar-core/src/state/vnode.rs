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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
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

/// Per-vnode lifecycle state. Distinct from ownership: a vnode this node
/// owns can still be [`Restoring`](Self::Restoring) while its committed
/// state is being rehydrated from durable storage after a rebalance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VnodeLifecycleState {
    /// Fully owned and serving — state is consistent.
    Active,
    /// Newly acquired in a rebalance; durable state is still being
    /// applied. Operators suppress emission for keys in this vnode until
    /// it flips back to [`Active`](Self::Active).
    Restoring,
}

impl VnodeLifecycleState {
    const ACTIVE: u8 = 0;
    const RESTORING: u8 = 1;

    const fn to_u8(self) -> u8 {
        match self {
            Self::Active => Self::ACTIVE,
            Self::Restoring => Self::RESTORING,
        }
    }
}

/// Opaque source-checkpoint offsets (connector-defined `"key" -> "value"`
/// strings) staged for a rotation handoff. A private alias keeps the single
/// `disallowed_types` exception (cold path, mirrors the checkpoint map shape) in
/// one place rather than blanket-allowing this perf-sensitive module.
#[allow(clippy::disallowed_types)]
type ResumeOffsets = std::collections::HashMap<String, String>;

/// Runtime registry of vnode topology and assignment.
pub struct VnodeRegistry {
    vnode_count: u32,
    assignment: RwLock<Arc<[NodeId]>>,
    assignment_version: AtomicU64,
    /// Per-vnode lifecycle, indexed by vnode id. `0` = `Active`,
    /// `1` = `Restoring`. Lock-free: rebalance flips individual entries
    /// and the hot emission gate reads them without taking a lock. Not
    /// serialized — rebuilt (all `Active`) from the `AssignmentSnapshot`
    /// on boot, so adding it never touches a wire format.
    lifecycle: Arc<[AtomicU8]>,
    /// Opaque source-checkpoint offsets (connector-defined key/value strings)
    /// staged by the engine just before an assignment-version bump. A partitioned
    /// source reads them on rebind to resume partitions it is acquiring from the
    /// previous owner's sealed position instead of `auto.offset.reset`. The engine
    /// stays connector-agnostic; the source interprets the keys.
    resume_offsets: parking_lot::Mutex<Arc<ResumeOffsets>>,
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
    ///
    /// # Panics
    /// Panics if `vnode_count == 0`.
    #[must_use]
    pub fn new(vnode_count: u32) -> Self {
        assert!(vnode_count > 0, "vnode_count must be > 0");
        let assignment: Arc<[NodeId]> =
            std::iter::repeat_n(NodeId::UNASSIGNED, vnode_count as usize)
                .collect::<Vec<_>>()
                .into();
        Self {
            vnode_count,
            assignment: RwLock::new(assignment),
            assignment_version: AtomicU64::new(1),
            lifecycle: new_lifecycle(vnode_count),
            resume_offsets: parking_lot::Mutex::new(Arc::new(ResumeOffsets::new())),
        }
    }

    /// Create a registry where every vnode is owned by the same node.
    ///
    /// Used by single-instance / embedded deployments.
    ///
    /// # Panics
    /// Panics if `vnode_count == 0`.
    #[must_use]
    pub fn single_owner(vnode_count: u32, owner: NodeId) -> Self {
        assert!(vnode_count > 0, "vnode_count must be > 0");
        let assignment: Arc<[NodeId]> = std::iter::repeat_n(owner, vnode_count as usize)
            .collect::<Vec<_>>()
            .into();
        Self {
            vnode_count,
            assignment: RwLock::new(assignment),
            assignment_version: AtomicU64::new(1),
            lifecycle: new_lifecycle(vnode_count),
            resume_offsets: parking_lot::Mutex::new(Arc::new(ResumeOffsets::new())),
        }
    }

    /// Stage opaque source-checkpoint offsets for the next rebind. MUST be called
    /// before [`set_assignment_and_version`](Self::set_assignment_and_version) so
    /// the source observes them. Replaces any prior staging; an empty map is
    /// ignored so a transient load failure can't clobber a good snapshot.
    pub fn stage_resume_offsets(&self, offsets: ResumeOffsets) {
        if offsets.is_empty() {
            return;
        }
        *self.resume_offsets.lock() = Arc::new(offsets);
    }

    /// The staged source-checkpoint offsets (see
    /// [`stage_resume_offsets`](Self::stage_resume_offsets)). Reads (does not
    /// drain), so every source on this node sees the same snapshot on rebind.
    #[must_use]
    pub fn resume_offsets(&self) -> Arc<ResumeOffsets> {
        Arc::clone(&self.resume_offsets.lock())
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

    /// Replace the full assignment and set the version to `version`
    /// atomically. For recovery paths that must restore the registry to
    /// a persisted fence generation, not a fresh bump.
    ///
    /// # Panics
    /// Panics on length mismatch, or if `version` is less than the
    /// current one (assignment versions are monotonic).
    pub fn set_assignment_and_version(&self, new_assignment: Arc<[NodeId]>, version: u64) {
        assert_eq!(
            new_assignment.len(),
            self.vnode_count as usize,
            "assignment length mismatch: got {}, expected {}",
            new_assignment.len(),
            self.vnode_count,
        );
        let mut guard = self.assignment.write();
        let current = self.assignment_version.load(Ordering::Acquire);
        assert!(
            version >= current,
            "assignment version must be monotonic: got {version}, current {current}",
        );
        *guard = new_assignment;
        self.assignment_version.store(version, Ordering::Release);
    }

    /// Map a primary key to a vnode.
    #[must_use]
    pub fn vnode_for_key(&self, key: &[u8]) -> u32 {
        #[allow(clippy::cast_possible_truncation)]
        let h = (key_hash(key) % u64::from(self.vnode_count)) as u32;
        h
    }

    /// Mark `vnodes` as [`Restoring`](VnodeLifecycleState::Restoring).
    ///
    /// Called during a rebalance for the vnodes a node newly acquires,
    /// before their committed state has been applied. Out-of-range ids
    /// are ignored.
    pub fn mark_restoring(&self, vnodes: &[u32]) {
        self.set_lifecycle(vnodes, VnodeLifecycleState::Restoring);
    }

    /// Mark `vnodes` as [`Active`](VnodeLifecycleState::Active).
    ///
    /// Called once a newly-acquired vnode's state has been applied (or
    /// immediately for vnodes that had no durable state to restore).
    /// Out-of-range ids are ignored.
    pub fn mark_active(&self, vnodes: &[u32]) {
        self.set_lifecycle(vnodes, VnodeLifecycleState::Active);
    }

    fn set_lifecycle(&self, vnodes: &[u32], state: VnodeLifecycleState) {
        let byte = state.to_u8();
        for &v in vnodes {
            if let Some(slot) = self.lifecycle.get(v as usize) {
                slot.store(byte, Ordering::Release);
            }
        }
    }

    /// Whether `vnode` is currently [`Restoring`](VnodeLifecycleState::Restoring).
    /// Out-of-range ids are reported as not restoring.
    #[must_use]
    pub fn is_restoring(&self, vnode: u32) -> bool {
        self.lifecycle
            .get(vnode as usize)
            .is_some_and(|s| s.load(Ordering::Acquire) == VnodeLifecycleState::RESTORING)
    }

    /// Whether any vnode is currently restoring. Cheap pre-check the
    /// emission gate uses to skip per-row work in the common case.
    #[must_use]
    pub fn any_restoring(&self) -> bool {
        self.lifecycle
            .iter()
            .any(|s| s.load(Ordering::Acquire) == VnodeLifecycleState::RESTORING)
    }

    /// Vnodes currently [`Restoring`](VnodeLifecycleState::Restoring), ascending.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // index < vnode_count, which is u32
    pub fn restoring_vnodes(&self) -> Vec<u32> {
        self.lifecycle
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                (s.load(Ordering::Acquire) == VnodeLifecycleState::RESTORING).then_some(i as u32)
            })
            .collect()
    }
}

/// Build a fresh lifecycle array with every vnode [`Active`].
fn new_lifecycle(vnode_count: u32) -> Arc<[AtomicU8]> {
    std::iter::repeat_with(|| AtomicU8::new(VnodeLifecycleState::ACTIVE))
        .take(vnode_count as usize)
        .collect::<Vec<_>>()
        .into()
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

/// Build a vnode-to-owner assignment using Rendezvous Hashing (Highest Random Weight).
///
/// Deterministic for a given `(vnode_count, peers)` input. Minimizes partition
/// reshuffling on membership changes (node joins/leaves).
///
/// # Panics
/// Panics if `peers` is empty.
#[must_use]
pub fn rendezvous_assignment(vnode_count: u32, peers: &[NodeId]) -> Arc<[NodeId]> {
    assert!(
        !peers.is_empty(),
        "rendezvous_assignment needs at least one peer"
    );
    let mut sorted_peers = peers.to_vec();
    sorted_peers.sort_by_key(|n| n.0);

    let mut assignment = Vec::with_capacity(vnode_count as usize);
    for v in 0..vnode_count {
        let mut max_weight = 0;
        let mut selected_node = sorted_peers[0];

        for &node in &sorted_peers {
            // Hash the combination of vnode ID and node ID
            let mut buf = [0u8; 16];
            buf[0..8].copy_from_slice(&u64::from(v).to_le_bytes());
            buf[8..16].copy_from_slice(&node.0.to_le_bytes());
            let weight = xxhash_rust::xxh3::xxh3_64(&buf);

            // Highest weight wins, tie-break by NodeId
            if weight > max_weight || (weight == max_weight && node.0 > selected_node.0) {
                max_weight = weight;
                selected_node = node;
            }
        }
        assignment.push(selected_node);
    }
    assignment.into()
}

/// A node's failure-domain locality: ordered tier values, coarsest first
/// (e.g. `["us-east-1", "us-east-1a", "r17"]`). Parsed from the node's
/// `failure_domain` gossip string.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Locality {
    tiers: Vec<String>,
}

impl Locality {
    /// Build from ordered tier values, coarsest first.
    #[must_use]
    pub fn new(tiers: Vec<String>) -> Self {
        Self { tiers }
    }

    /// Parse `"region=us-east-1;zone=us-east-1a;rack=r17"` (or a bare label
    /// `"rack17"`) into its tier values. Tier names are ignored.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        let tiers = s
            .split(';')
            .map(str::trim)
            .filter(|seg| !seg.is_empty())
            .map(|seg| {
                seg.split_once('=')
                    .map_or(seg, |(_, v)| v.trim())
                    .to_string()
            })
            .collect();
        Self { tiers }
    }

    /// Failure-domain key at `tier`: the `;`-joined value prefix `0..=tier`.
    /// Two nodes share a domain iff equal; an unlabeled node yields the empty key.
    #[must_use]
    pub fn domain_at(&self, tier: usize) -> String {
        if self.tiers.is_empty() {
            return String::new();
        }
        let end = tier.min(self.tiers.len() - 1);
        self.tiers[..=end].join(";")
    }
}

/// Resolve each node's failure-domain key at `isolation_tier`.
fn resolve_domains(nodes: &[(NodeId, Locality)], isolation_tier: usize) -> Vec<(NodeId, String)> {
    nodes
        .iter()
        .map(|(id, loc)| (*id, loc.domain_at(isolation_tier)))
        .collect()
}

/// Owner counts per failure domain at `isolation_tier`. The largest value over
/// `vnode_count` is the blast radius — the share of state that goes `Restoring`
/// if that one domain fails at once.
#[must_use]
pub fn owners_per_domain(
    owners: &[NodeId],
    nodes: &[(NodeId, Locality)],
    isolation_tier: usize,
) -> BTreeMap<String, u32> {
    let dom: BTreeMap<NodeId, String> =
        resolve_domains(nodes, isolation_tier).into_iter().collect();
    let mut counts = BTreeMap::new();
    for &o in owners {
        *counts
            .entry(dom.get(&o).cloned().unwrap_or_default())
            .or_default() += 1;
    }
    counts
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

/// Distinct assigned nodes other than `self_id`, sorted by id — the peer set a
/// node fans checkpoint barriers and shuffle data out to.
#[must_use]
pub fn peer_owners(registry: &VnodeRegistry, self_id: NodeId) -> Vec<NodeId> {
    let mut peers: Vec<NodeId> = (0..registry.vnode_count())
        .map(|v| registry.owner(v))
        .filter(|o| !o.is_unassigned() && *o != self_id)
        .collect();
    peers.sort_unstable();
    peers.dedup();
    peers
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
    fn rendezvous_is_deterministic() {
        let peers = vec![NodeId(7), NodeId(3), NodeId(5)];
        let assignment = rendezvous_assignment(8, &peers);
        // Input order doesn't matter.
        let reversed = vec![NodeId(3), NodeId(5), NodeId(7)];
        assert_eq!(rendezvous_assignment(8, &reversed), assignment);
    }

    #[test]
    fn rendezvous_single_peer_owns_everything() {
        let assignment = rendezvous_assignment(4, &[NodeId(99)]);
        assert!(assignment.iter().all(|&n| n == NodeId(99)));
    }

    #[test]
    #[should_panic(expected = "needs at least one peer")]
    fn rendezvous_rejects_empty_peer_list() {
        let _ = rendezvous_assignment(4, &[]);
    }

    #[test]
    fn rendezvous_minimizes_state_movement() {
        let peers3 = vec![NodeId(1), NodeId(2), NodeId(3)];
        let peers4 = vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)];

        let a3 = rendezvous_assignment(256, &peers3);
        let a4 = rendezvous_assignment(256, &peers4);

        let mut moved = 0;
        let mut moved_between_existing = 0;

        for v in 0..256usize {
            let o3 = a3[v];
            let o4 = a4[v];
            if o3 != o4 {
                moved += 1;
                if o4 != NodeId(4) {
                    moved_between_existing += 1;
                }
            }
        }

        assert_eq!(
            moved_between_existing, 0,
            "No vnode should move between existing peers on a node join"
        );
        assert!(
            moved > 40 && moved < 90,
            "Expected roughly 25% of vnodes to move to the new peer, got {moved}"
        );

        for v in 0..256usize {
            if a3[v] != a4[v] {
                assert_eq!(a4[v], NodeId(4));
            }
        }
    }

    #[test]
    fn vnodes_start_active() {
        let r = VnodeRegistry::new(4);
        assert!(!r.any_restoring());
        for v in 0..4 {
            assert!(!r.is_restoring(v));
        }
        assert!(r.restoring_vnodes().is_empty());
    }

    #[test]
    fn mark_restoring_and_active_round_trip() {
        let r = VnodeRegistry::new(4);
        r.mark_restoring(&[1, 3]);
        assert!(r.any_restoring());
        assert!(r.is_restoring(1));
        assert!(r.is_restoring(3));
        assert!(!r.is_restoring(0));
        assert_eq!(r.restoring_vnodes(), vec![1, 3]);

        r.mark_active(&[1]);
        assert!(!r.is_restoring(1));
        assert_eq!(r.restoring_vnodes(), vec![3]);

        r.mark_active(&[3]);
        assert!(!r.any_restoring());
    }

    #[test]
    fn lifecycle_ignores_out_of_range() {
        let r = VnodeRegistry::new(2);
        r.mark_restoring(&[5, 99]); // no panic
        assert!(!r.is_restoring(5));
        assert!(!r.any_restoring());
    }

    #[test]
    fn lifecycle_independent_of_assignment() {
        // Reassigning ownership must not clear lifecycle state — the two
        // are orthogonal and the caller drives the Restoring→Active flip.
        let r = VnodeRegistry::new(4);
        r.mark_restoring(&[2]);
        r.set_assignment(vec![NodeId(1), NodeId(1), NodeId(1), NodeId(1)].into());
        assert!(r.is_restoring(2));
    }

    // -- Topology-aware placement --------------------------------------------

    /// A node at (region, zone, rack).
    fn node(id: u64, region: &str, zone: &str, rack: &str) -> (NodeId, Locality) {
        (
            NodeId(id),
            Locality::new(vec![region.into(), zone.into(), rack.into()]),
        )
    }

    const TIER_ZONE: usize = 1;

    #[test]
    fn locality_parse_and_domain_at() {
        let l = Locality::parse("region=us-east-1;zone=us-east-1a;rack=r17");
        assert_eq!(l.domain_at(0), "us-east-1");
        assert_eq!(l.domain_at(1), "us-east-1;us-east-1a");
        assert_eq!(l.domain_at(2), "us-east-1;us-east-1a;r17");
        assert_eq!(l.domain_at(99), "us-east-1;us-east-1a;r17"); // clamps to finest
        assert_eq!(Locality::parse("rack17").domain_at(0), "rack17"); // bare label
        assert_eq!(Locality::parse("").domain_at(0), ""); // unknown → empty domain
    }

    #[test]
    fn owners_per_domain_counts_by_zone() {
        let nodes = vec![node(1, "r", "z1", "a"), node(2, "r", "z2", "a")];
        // z1 owns 2, z2 owns 1, and an unassigned owner folds into the empty domain.
        let owners = [NodeId(1), NodeId(1), NodeId(2), NodeId::UNASSIGNED];
        let counts = owners_per_domain(&owners, &nodes, TIER_ZONE);
        assert_eq!(counts["r;z1"], 2);
        assert_eq!(counts["r;z2"], 1);
        assert_eq!(counts[""], 1);
    }
}
