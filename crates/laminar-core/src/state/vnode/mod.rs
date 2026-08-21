//! [`VnodeRegistry`] — runtime-configurable virtual node topology.
//!
//! A registry owns:
//!
//! - the runtime-selected vnode count,
//! - the node-per-vnode assignment (for distributed modes),
//! - a monotonically increasing `assignment_version` used to fence stale
//!   assignment publications.
//!
//! Vnode assignment is derived from the row's primary key via
//! [`key_hash`] (xxh3) and modulo `vnode_count`. Connectors that
//! need a vnode ID for an event call [`VnodeRegistry::vnode_for_key`].

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};

/// Durable encoding and hashing contract used to map keys to key groups.
///
/// Any change to Arrow row encoding, key hashing, or modulo mapping requires a
/// version bump; recovery rejects checkpoints with a different identity. Placement
/// is not part of this ABI: durable assignment publications are the ownership authority.
pub const PARTITIONING_ABI_VERSION: u16 = 1;

/// Validated number of stable key groups in a pipeline.
///
/// Zero and values above [`u16::MAX`] are rejected. The compact upper bound keeps
/// ownership metadata bounded while providing substantially more rescale slots
/// than a production pipeline should need.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyGroupCount(NonZeroU16);

/// Default stable key-group count for every deployment tier.
pub const DEFAULT_KEY_GROUP_COUNT: KeyGroupCount = match NonZeroU16::new(256) {
    Some(value) => KeyGroupCount(value),
    None => unreachable!(),
};

/// Largest key-group count representable by the persisted checkpoint ABI.
pub const MAX_KEY_GROUP_COUNT: u32 = u16::MAX as u32;

impl KeyGroupCount {
    /// Build from a nonzero value.
    #[must_use]
    pub const fn new(value: NonZeroU16) -> Self {
        Self(value)
    }

    /// Exact underlying count.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }

    /// Exact nonzero representation.
    #[must_use]
    pub const fn into_non_zero(self) -> NonZeroU16 {
        self.0
    }
}

impl fmt::Display for KeyGroupCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

/// A key-group count outside the supported `1..=65535` range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("key-group count {value} is outside 1..={}", u16::MAX)]
pub struct InvalidKeyGroupCount {
    value: u32,
}

impl InvalidKeyGroupCount {
    /// Rejected input value.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.value
    }
}

impl TryFrom<u16> for KeyGroupCount {
    type Error = InvalidKeyGroupCount;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        NonZeroU16::new(value)
            .map(Self)
            .ok_or(InvalidKeyGroupCount {
                value: u32::from(value),
            })
    }
}

impl TryFrom<u32> for KeyGroupCount {
    type Error = InvalidKeyGroupCount;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let narrowed = u16::try_from(value).map_err(|_| InvalidKeyGroupCount { value })?;
        Self::try_from(narrowed).map_err(|_| InvalidKeyGroupCount { value })
    }
}

impl From<NonZeroU16> for KeyGroupCount {
    fn from(value: NonZeroU16) -> Self {
        Self::new(value)
    }
}

impl From<KeyGroupCount> for NonZeroU16 {
    fn from(value: KeyGroupCount) -> Self {
        value.into_non_zero()
    }
}

impl From<KeyGroupCount> for u16 {
    fn from(value: KeyGroupCount) -> Self {
        value.get()
    }
}

impl From<KeyGroupCount> for u32 {
    fn from(value: KeyGroupCount) -> Self {
        u32::from(value.get())
    }
}

impl From<KeyGroupCount> for usize {
    fn from(value: KeyGroupCount) -> Self {
        usize::from(value.get())
    }
}

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

/// Stable owner identity for embedded and standalone runtimes.
pub const LOCAL_NODE_ID: NodeId = NodeId(1);

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

/// One immutable publication of vnode ownership and its assignment version.
#[derive(Clone)]
pub struct VnodeAssignmentSnapshot {
    version: u64,
    owners: Arc<[NodeId]>,
    owner_changed_versions: Arc<[u64]>,
}

impl VnodeAssignmentSnapshot {
    /// Monotonic assignment version for this publication.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Immutable vnode-owner vector for this publication.
    #[must_use]
    pub fn owners(&self) -> &[NodeId] {
        &self.owners
    }

    /// Version of the last ownership change for `vnode`.
    #[must_use]
    pub fn owner_changed_version(&self, vnode: u32) -> Option<u64> {
        self.owner_changed_versions.get(vnode as usize).copied()
    }
}

/// Read guard that pins one assignment publication while a source captures a
/// data batch or checkpoint cursor.
pub struct VnodeAssignmentReadGuard<'a>(RwLockReadGuard<'a, VnodeAssignmentSnapshot>);

impl std::ops::Deref for VnodeAssignmentReadGuard<'_> {
    type Target = VnodeAssignmentSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn changed_owner_versions(
    current: &VnodeAssignmentSnapshot,
    next_owners: &[NodeId],
    next_version: u64,
) -> Arc<[u64]> {
    let skipped_generation = next_version > current.version.saturating_add(1);
    current
        .owners
        .iter()
        .zip(next_owners)
        .enumerate()
        .map(|(vnode, (current_owner, next_owner))| {
            if current_owner == next_owner && !skipped_generation {
                current.owner_changed_versions[vnode]
            } else {
                next_version
            }
        })
        .collect::<Vec<_>>()
        .into()
}

/// Runtime registry of vnode topology and assignment.
pub struct VnodeRegistry {
    vnode_count: u32,
    assignment: RwLock<VnodeAssignmentSnapshot>,
    /// Lock-free observation fence for hot runtime readiness checks. The
    /// authoritative version also lives inside `assignment`, where it is bound
    /// to owners.
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
            assignment: RwLock::new(VnodeAssignmentSnapshot {
                version: 1,
                owners: assignment,
                owner_changed_versions: std::iter::repeat_n(1, vnode_count as usize)
                    .collect::<Vec<_>>()
                    .into(),
            }),
            assignment_version: AtomicU64::new(1),
        }
    }

    /// Create a registry with every vnode unassigned at version 0, so the first stored
    /// assignment snapshot (version >= 1) adopts through the standard rotation path.
    ///
    /// # Panics
    /// Panics if `vnode_count == 0`.
    #[must_use]
    pub fn new_unassigned(vnode_count: u32) -> Self {
        let registry = Self::new(vnode_count);
        {
            let mut initial = registry.assignment.write();
            initial.version = 0;
            initial.owner_changed_versions = std::iter::repeat_n(0, vnode_count as usize)
                .collect::<Vec<_>>()
                .into();
        }
        registry.assignment_version.store(0, Ordering::Release);
        registry
    }

    /// Create a registry where every vnode is owned by the same node.
    ///
    /// Used by single-instance / embedded deployments.
    ///
    /// # Panics
    /// Panics if `vnode_count == 0` or `owner` is [`NodeId::UNASSIGNED`].
    #[must_use]
    pub fn single_owner(vnode_count: u32, owner: NodeId) -> Self {
        assert!(vnode_count > 0, "vnode_count must be > 0");
        assert_ne!(owner, NodeId::UNASSIGNED, "owner must be assigned");
        let assignment: Arc<[NodeId]> = std::iter::repeat_n(owner, vnode_count as usize)
            .collect::<Vec<_>>()
            .into();
        Self {
            vnode_count,
            assignment: RwLock::new(VnodeAssignmentSnapshot {
                version: 1,
                owners: assignment,
                owner_changed_versions: std::iter::repeat_n(1, vnode_count as usize)
                    .collect::<Vec<_>>()
                    .into(),
            }),
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
        self.assignment.read().owners[vnode as usize]
    }

    /// Snapshot the current assignment vector. Cheap — internally an
    /// `Arc::clone`.
    #[must_use]
    pub fn snapshot(&self) -> Arc<[NodeId]> {
        Arc::clone(&self.assignment.read().owners)
    }

    /// Consistent ownership, version, and source-handoff publication.
    #[must_use]
    pub fn versioned_snapshot(&self) -> VnodeAssignmentSnapshot {
        self.assignment.read().clone()
    }

    /// Pin the current publication for a short, non-blocking source capture.
    #[must_use]
    pub fn read_assignment(&self) -> VnodeAssignmentReadGuard<'_> {
        VnodeAssignmentReadGuard(self.assignment.read())
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
        let mut current = self.assignment.write();
        let version = current
            .version
            .checked_add(1)
            .expect("assignment version overflow");
        let owner_changed_versions = changed_owner_versions(&current, &new_assignment, version);
        *current = VnodeAssignmentSnapshot {
            version,
            owners: new_assignment,
            owner_changed_versions,
        };
        self.assignment_version
            .store(current.version, Ordering::Release);
    }

    /// Replace the full assignment and set the version to `version`
    /// atomically. For recovery paths that must restore the registry to
    /// a persisted fence generation, not a fresh bump.
    ///
    /// # Panics
    /// Panics on length mismatch, or if `version` is less than the
    /// current one (assignment versions are monotonic).
    pub fn set_assignment_and_version(&self, new_assignment: Arc<[NodeId]>, version: u64) {
        self.publish_assignment(new_assignment, version);
    }

    fn publish_assignment(&self, new_assignment: Arc<[NodeId]>, version: u64) {
        assert_eq!(
            new_assignment.len(),
            self.vnode_count as usize,
            "assignment length mismatch: got {}, expected {}",
            new_assignment.len(),
            self.vnode_count,
        );
        let mut guard = self.assignment.write();
        let current = guard.version;
        assert!(
            version > current,
            "assignment version must advance: got {version}, current {current}",
        );
        let owner_changed_versions = changed_owner_versions(&guard, &new_assignment, version);
        *guard = VnodeAssignmentSnapshot {
            version,
            owners: new_assignment,
            owner_changed_versions,
        };
        self.assignment_version.store(version, Ordering::Release);
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
/// `vnode_count` is the blast radius — the share of state affected if that one
/// domain fails at once.
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
/// `seal_checkpoint` gate to know the full set to check.
#[must_use]
pub fn owned_vnodes(registry: &VnodeRegistry, owner: NodeId) -> Vec<u32> {
    let assignment = registry.snapshot();
    assignment
        .iter()
        .enumerate()
        .filter(|(_, assigned)| **assigned == owner)
        .filter_map(|(vnode, _)| u32::try_from(vnode).ok())
        .collect()
}

/// Distinct assigned nodes other than `self_id`, sorted by id — the peer set a
/// node fans checkpoint barriers and shuffle data out to.
#[must_use]
pub fn peer_owners(registry: &VnodeRegistry, self_id: NodeId) -> Vec<NodeId> {
    let assignment = registry.snapshot();
    let mut peers: Vec<NodeId> = assignment
        .iter()
        .copied()
        .filter(|o| !o.is_unassigned() && *o != self_id)
        .collect();
    peers.sort_unstable();
    peers.dedup();
    peers
}

#[cfg(test)]
mod tests;
