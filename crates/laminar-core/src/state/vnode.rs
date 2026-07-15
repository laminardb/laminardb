//! [`VnodeRegistry`] — runtime-configurable virtual node topology.
//!
//! Replaces the compile-time `VNODE_COUNT` constant that previously
//! lived in `laminar-storage`. A registry owns:
//!
//! - the runtime-selected vnode count,
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
use std::num::NonZeroU16;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointWatermark, CommittedSourceHandoff, SourceHandoffState};
use crate::state::CheckpointAttempt;

/// Durable encoding and hashing contract used to map keys to key groups.
///
/// Any change to Arrow row encoding, key hashing, or modulo mapping requires a
/// version bump and a coordinated compatibility fence. Placement is not part of
/// this ABI: durable assignment publications are the ownership authority.
pub const PARTITIONING_ABI_VERSION: u16 = 1;

/// Validated number of stable key groups in a pipeline.
///
/// Zero and values above [`u16::MAX`] are rejected. The compact upper bound keeps
/// ownership metadata bounded while providing substantially more rescale slots
/// than a production pipeline should need.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyGroupCount(NonZeroU16);

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

enum SourceHandoffPublication {
    Clear,
    Replace(Arc<CommittedSourceHandoff>),
    Carry,
}

/// One immutable publication of vnode ownership and the source cursors that
/// belong to the same assignment version.
#[derive(Clone)]
pub struct VnodeAssignmentSnapshot {
    version: u64,
    owners: Arc<[NodeId]>,
    owner_changed_versions: Arc<[u64]>,
    source_handoff: Option<Arc<CommittedSourceHandoff>>,
    source_handoff_installed_version: Option<u64>,
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

    /// Whether this publication carries a durable decided checkpoint cut.
    #[must_use]
    pub const fn has_committed_handoff(&self) -> bool {
        self.source_handoff.is_some()
    }

    /// Validated committed state for one source, without cloning its checkpoint maps.
    #[must_use]
    pub fn source_handoff(&self, source: &str) -> Option<&SourceHandoffState> {
        self.source_handoff
            .as_deref()
            .and_then(|handoff| handoff.source(source))
    }

    /// Complete committed source cut carried by this publication, if present.
    #[must_use]
    pub fn committed_source_handoff(&self) -> Option<&CommittedSourceHandoff> {
        self.source_handoff.as_deref()
    }

    /// Assignment version that installed the current handoff. Carry-only
    /// publications preserve the earlier version so runtimes do not restore
    /// the same recovery cut for an unrelated roster update.
    #[must_use]
    pub const fn source_handoff_installed_version(&self) -> Option<u64> {
        self.source_handoff_installed_version
    }

    /// Exact checkpoint attempt supplying the committed handoff, if present.
    #[must_use]
    pub fn source_handoff_attempt(&self) -> Option<CheckpointAttempt> {
        self.source_handoff
            .as_deref()
            .map(CommittedSourceHandoff::attempt)
    }

    /// Assignment version sealed by the committed handoff, if present.
    #[must_use]
    pub fn source_handoff_assignment_version(&self) -> Option<u64> {
        self.source_handoff
            .as_deref()
            .map(CommittedSourceHandoff::checkpoint_assignment_version)
    }

    /// Explicit cluster event-time status at the committed cut, if present.
    #[must_use]
    pub fn source_handoff_cluster_watermark(&self) -> Option<CheckpointWatermark> {
        self.source_handoff
            .as_deref()
            .map(CommittedSourceHandoff::cluster_watermark)
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
    /// to owners and handoff cursors.
    assignment_version: AtomicU64,
    /// Per-vnode lifecycle, indexed by vnode id. `0` = `Active`,
    /// `1` = `Restoring`. Lock-free: rebalance flips individual entries
    /// and the hot emission gate reads them without taking a lock. Not
    /// serialized — rebuilt (all `Active`) from the `AssignmentSnapshot`
    /// on boot, so adding it never touches a wire format.
    lifecycle: Arc<[AtomicU8]>,
    /// Per-vnode "draining" flag, set on a vnode this node is about to lose in a
    /// rotation so a partitioned source pauses that vnode's input until the cut.
    /// Orthogonal to `lifecycle`: a draining vnode is still owned and still emits;
    /// it only stops *consuming*. `draining_generation` lets the source detect a
    /// change lock-free without an assignment-version bump.
    draining: Arc<[AtomicBool]>,
    draining_generation: AtomicU64,
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
                source_handoff: None,
                source_handoff_installed_version: None,
            }),
            assignment_version: AtomicU64::new(1),
            lifecycle: new_lifecycle(vnode_count),
            draining: new_draining(vnode_count),
            draining_generation: AtomicU64::new(0),
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
    /// Panics if `vnode_count == 0`.
    #[must_use]
    pub fn single_owner(vnode_count: u32, owner: NodeId) -> Self {
        assert!(vnode_count > 0, "vnode_count must be > 0");
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
                source_handoff: None,
                source_handoff_installed_version: None,
            }),
            assignment_version: AtomicU64::new(1),
            lifecycle: new_lifecycle(vnode_count),
            draining: new_draining(vnode_count),
            draining_generation: AtomicU64::new(0),
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
            source_handoff: None,
            source_handoff_installed_version: None,
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
        self.publish_assignment(new_assignment, version, SourceHandoffPublication::Clear);
    }

    /// Atomically publish ownership, its monotonic version, and the sealed
    /// connector cursors used by sources acquiring partitions in that version.
    ///
    /// # Panics
    /// Panics on length mismatch or version regression.
    pub fn set_assignment_and_version_with_source_handoff(
        &self,
        new_assignment: Arc<[NodeId]>,
        version: u64,
        source_handoff: Arc<CommittedSourceHandoff>,
    ) {
        self.publish_assignment(
            new_assignment,
            version,
            SourceHandoffPublication::Replace(source_handoff),
        );
    }

    /// Publish a version that does not acquire local ownership while retaining
    /// the prior version-bound handoff until the source has reconciled it.
    pub fn set_assignment_and_version_carrying_source_handoff(
        &self,
        new_assignment: Arc<[NodeId]>,
        version: u64,
    ) {
        self.publish_assignment(new_assignment, version, SourceHandoffPublication::Carry);
    }

    fn publish_assignment(
        &self,
        new_assignment: Arc<[NodeId]>,
        version: u64,
        source_handoff: SourceHandoffPublication,
    ) {
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
        let (source_handoff, source_handoff_installed_version) = match source_handoff {
            SourceHandoffPublication::Clear => (None, None),
            SourceHandoffPublication::Replace(source_handoff) => {
                (Some(source_handoff), Some(version))
            }
            SourceHandoffPublication::Carry => (
                guard.source_handoff.clone(),
                guard.source_handoff_installed_version,
            ),
        };
        *guard = VnodeAssignmentSnapshot {
            version,
            owners: new_assignment,
            owner_changed_versions,
            source_handoff,
            source_handoff_installed_version,
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

    /// Mark `vnodes` as draining for a pending rotation so a partitioned source
    /// pauses their input until the pre-rotation checkpoint cut. Out-of-range ids
    /// are ignored. Bumps the draining generation so the source observes it.
    pub fn mark_draining(&self, vnodes: &[u32]) {
        for &v in vnodes {
            if let Some(slot) = self.draining.get(v as usize) {
                slot.store(true, Ordering::Release);
            }
        }
        self.draining_generation.fetch_add(1, Ordering::Release);
    }

    /// Clear every draining flag (rotation committed or aborted). Bumps the
    /// generation so the source resumes any partitions it paused for the drain.
    pub fn clear_draining(&self) {
        for slot in self.draining.iter() {
            slot.store(false, Ordering::Release);
        }
        self.draining_generation.fetch_add(1, Ordering::Release);
    }

    /// Whether `vnode` is currently draining. Out-of-range ids report false.
    #[must_use]
    pub fn is_draining(&self, vnode: u32) -> bool {
        self.draining
            .get(vnode as usize)
            .is_some_and(|s| s.load(Ordering::Acquire))
    }

    /// Monotonic counter bumped on each draining change; the source compares it
    /// lock-free to detect drain/undrain without an assignment-version bump.
    #[must_use]
    pub fn draining_generation(&self) -> u64 {
        self.draining_generation.load(Ordering::Acquire)
    }
}

/// Build a fresh lifecycle array with every vnode [`Active`].
fn new_lifecycle(vnode_count: u32) -> Arc<[AtomicU8]> {
    std::iter::repeat_with(|| AtomicU8::new(VnodeLifecycleState::ACTIVE))
        .take(vnode_count as usize)
        .collect::<Vec<_>>()
        .into()
}

/// Build a fresh draining array with every vnode not draining.
fn new_draining(vnode_count: u32) -> Arc<[AtomicBool]> {
    std::iter::repeat_with(|| AtomicBool::new(false))
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
mod tests {
    use super::*;
    use crate::checkpoint::{
        CheckpointAssignmentFence, CheckpointParticipant, ClusterRecoveryCapsule,
        ConnectorCheckpoint, ParticipantRecoveryRef, PipelineIdentity,
        CLUSTER_RECOVERY_CAPSULE_VERSION, PIPELINE_IDENTITY_VERSION,
    };

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    #[test]
    fn partitioning_key_group_count_rejects_every_out_of_range_value() {
        assert_eq!(KeyGroupCount::try_from(0_u16).unwrap_err().value(), 0);
        assert_eq!(KeyGroupCount::try_from(0_u32).unwrap_err().value(), 0);
        assert_eq!(
            KeyGroupCount::try_from(u32::from(u16::MAX) + 1)
                .unwrap_err()
                .value(),
            u32::from(u16::MAX) + 1
        );

        let one = KeyGroupCount::try_from(1_u16).unwrap();
        let max = KeyGroupCount::try_from(u32::from(u16::MAX)).unwrap();
        assert_eq!(u16::from(one), 1);
        assert_eq!(u32::from(max), u32::from(u16::MAX));
        assert_eq!(usize::from(max), usize::from(u16::MAX));
        assert_eq!(NonZeroU16::from(one), NonZeroU16::MIN);
    }

    #[test]
    fn partitioning_abi_v1_raw_key_hash_golden_vectors() {
        assert_eq!(PARTITIONING_ABI_VERSION, 1);
        let actual = [
            key_hash(b""),
            key_hash(b"a"),
            key_hash(b"laminardb"),
            key_hash(&[0, 1, 0xff]),
            key_hash("key-☃".as_bytes()),
        ];
        assert_eq!(
            actual,
            [
                3_244_421_341_483_603_138,
                16_629_034_431_890_738_719,
                16_801_042_214_008_847_674,
                10_014_172_824_849_140_082,
                17_604_077_472_932_801_374,
            ]
        );
    }

    #[test]
    fn rendezvous_placement_policy_golden_vector() {
        // Placement can evolve through an assignment transition; this vector
        // detects accidental churn in the current policy but is not ABI v1.
        let actual = rendezvous_assignment(12, &[NodeId(7), NodeId(3), NodeId(5)]);
        assert_eq!(
            actual.as_ref(),
            &[
                NodeId(5),
                NodeId(7),
                NodeId(3),
                NodeId(5),
                NodeId(5),
                NodeId(7),
                NodeId(5),
                NodeId(5),
                NodeId(5),
                NodeId(5),
                NodeId(5),
                NodeId(5),
            ]
        );
    }

    fn committed_handoff(
        source: Option<(&str, ConnectorCheckpoint, Option<i64>)>,
        cluster_watermark: CheckpointWatermark,
    ) -> Arc<CommittedSourceHandoff> {
        let mut source_offsets = BTreeMap::new();
        let mut source_metadata = BTreeMap::new();
        let mut source_assignment_versions = BTreeMap::new();
        let mut source_watermarks = BTreeMap::new();
        if let Some((source, checkpoint, watermark)) = source {
            let ConnectorCheckpoint {
                offsets,
                metadata,
                source_assignment_version,
            } = checkpoint;
            source_offsets.insert(source.to_string(), offsets.into_iter().collect());
            source_metadata.insert(source.to_string(), metadata.into_iter().collect());
            if let Some(assignment_version) = source_assignment_version {
                source_assignment_versions.insert(source.to_string(), assignment_version);
            }
            if let Some(watermark) = watermark {
                source_watermarks.insert(source.to_string(), watermark);
            }
        }

        let participant = CheckpointParticipant {
            node_id: 7,
            boot_incarnation: uuid::Uuid::from_u128(77),
        };
        let capsule = ClusterRecoveryCapsule {
            version: CLUSTER_RECOVERY_CAPSULE_VERSION,
            attempt: CheckpointAttempt::new(5, 9),
            deployment_id: uuid::Uuid::from_u128(99).to_string(),
            pipeline_identity: PipelineIdentity {
                canonical_version: PIPELINE_IDENTITY_VERSION,
                sha256: digest(1),
            },
            assignment_fence: CheckpointAssignmentFence::from_owner_map(
                17,
                &[7],
                vec![participant],
            )
            .unwrap(),
            seal_inventory_sha256: digest(2),
            participants: vec![ParticipantRecoveryRef {
                participant_id: 7,
                readiness_sha256: digest(3),
                manifest_sha256: digest(4),
                portable_state_sha256: digest(5),
            }],
            source_offsets,
            source_metadata,
            source_assignment_versions,
            source_watermarks,
            cluster_watermark,
            recovery_watermark_frontier: cluster_watermark.active_value(),
            portable_state_sha256: digest(5),
        };
        Arc::new(CommittedSourceHandoff::try_from(&capsule).unwrap())
    }

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
    fn committed_empty_source_is_distinct_from_a_missing_source() {
        let r = VnodeRegistry::new_unassigned(4);
        let owners = r.snapshot();
        let orders = ConnectorCheckpoint {
            offsets: [("orders:0".into(), "41".into())].into_iter().collect(),
            metadata: [("topic".into(), "orders".into())].into_iter().collect(),
            source_assignment_version: std::num::NonZeroU64::new(17),
        };
        let version_one_handoff = committed_handoff(
            Some(("orders", orders, Some(700))),
            CheckpointWatermark::Uninitialized,
        );
        r.set_assignment_and_version_with_source_handoff(
            Arc::clone(&owners),
            1,
            version_one_handoff,
        );
        let version_one = r.versioned_snapshot();
        assert_eq!(version_one.version(), 1);
        assert!(version_one.has_committed_handoff());
        assert_eq!(
            version_one
                .source_handoff("orders")
                .unwrap()
                .checkpoint()
                .offsets
                .get("orders:0")
                .map(String::as_str),
            Some("41")
        );
        assert_eq!(
            version_one
                .source_handoff("orders")
                .unwrap()
                .checkpoint()
                .metadata
                .get("topic")
                .map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            version_one
                .source_handoff("orders")
                .unwrap()
                .checkpoint()
                .source_assignment_version,
            std::num::NonZeroU64::new(17)
        );
        assert_eq!(
            version_one.source_handoff("orders").unwrap().watermark(),
            Some(700)
        );
        assert_eq!(
            version_one.source_handoff_cluster_watermark(),
            Some(CheckpointWatermark::Uninitialized)
        );
        assert_eq!(
            version_one.source_handoff_attempt(),
            Some(CheckpointAttempt::new(5, 9))
        );
        assert_eq!(version_one.source_handoff_assignment_version(), Some(17));
        assert_eq!(version_one.source_handoff_installed_version(), Some(1));

        let committed_empty_source = committed_handoff(
            Some(("orders", ConnectorCheckpoint::new(), None)),
            CheckpointWatermark::Idle,
        );
        r.set_assignment_and_version_with_source_handoff(owners, 2, committed_empty_source);
        let published = r.versioned_snapshot();
        assert_eq!(published.version(), 2);
        assert_eq!(published.source_handoff_installed_version(), Some(2));
        assert!(published.has_committed_handoff());
        let orders = published.source_handoff("orders").unwrap();
        assert!(orders.checkpoint().offsets.is_empty());
        assert!(orders.checkpoint().metadata.is_empty());
        assert_eq!(orders.watermark(), None);
        assert!(published.source_handoff("missing").is_none());
        assert_eq!(
            published.source_handoff_cluster_watermark(),
            Some(CheckpointWatermark::Idle)
        );
        assert_eq!(
            version_one
                .source_handoff("orders")
                .unwrap()
                .checkpoint()
                .offsets
                .get("orders:0")
                .map(String::as_str),
            Some("41"),
            "older immutable publications keep their version-bound handoff"
        );
    }

    #[test]
    fn owner_generations_and_handoff_survive_skipped_publications() {
        let r = VnodeRegistry::new_unassigned(1);
        let self_id = NodeId(7);
        let other = NodeId(8);
        let handoff = committed_handoff(
            Some((
                "events",
                ConnectorCheckpoint::with_offsets(
                    [("events:0".to_string(), "41".to_string())]
                        .into_iter()
                        .collect(),
                ),
                None,
            )),
            CheckpointWatermark::Idle,
        );
        r.set_assignment_and_version_with_source_handoff(
            vec![self_id].into(),
            1,
            Arc::clone(&handoff),
        );
        r.set_assignment_and_version_carrying_source_handoff(vec![other].into(), 2);
        r.set_assignment_and_version_carrying_source_handoff(vec![self_id].into(), 3);

        let published = r.versioned_snapshot();
        assert_eq!(published.owner_changed_version(0), Some(3));
        assert!(published.has_committed_handoff());
        assert_eq!(published.source_handoff_installed_version(), Some(1));
        assert!(std::ptr::eq(
            published.committed_source_handoff().unwrap(),
            handoff.as_ref()
        ));
        assert_eq!(
            published
                .source_handoff("events")
                .unwrap()
                .checkpoint()
                .offsets
                .get("events:0")
                .map(String::as_str),
            Some("41")
        );
    }

    #[test]
    fn skipped_assignment_generation_forces_owner_reconciliation() {
        let r = VnodeRegistry::new_unassigned(1);
        let self_id = NodeId(7);
        r.set_assignment_and_version(vec![self_id].into(), 1);
        assert_eq!(r.versioned_snapshot().owner_changed_version(0), Some(1));

        r.set_assignment_and_version(vec![self_id].into(), 3);
        assert_eq!(
            r.versioned_snapshot().owner_changed_version(0),
            Some(3),
            "a missed intermediate generation may have transferred ownership"
        );
    }

    #[test]
    #[should_panic(expected = "assignment version must advance")]
    fn assignment_publication_rejects_equal_version_mutation() {
        let r = VnodeRegistry::new(1);
        r.set_assignment_and_version(vec![NodeId(9)].into(), 1);
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

    #[test]
    fn draining_marks_clear_and_bump_generation() {
        let r = VnodeRegistry::new(4);
        let g0 = r.draining_generation();
        assert!(!r.is_draining(1));

        r.mark_draining(&[1, 3]);
        assert!(r.is_draining(1));
        assert!(r.is_draining(3));
        assert!(!r.is_draining(0));
        assert!(r.draining_generation() > g0, "mark bumps the generation");

        let g1 = r.draining_generation();
        r.clear_draining();
        assert!(!r.is_draining(1));
        assert!(!r.is_draining(3));
        assert!(r.draining_generation() > g1, "clear bumps the generation");
    }

    #[test]
    fn draining_is_orthogonal_to_lifecycle_and_ignores_out_of_range() {
        let r = VnodeRegistry::new(2);
        // A draining vnode still emits (lifecycle stays Active) — only consumption pauses.
        r.mark_draining(&[0]);
        assert!(r.is_draining(0));
        assert!(!r.is_restoring(0));
        // Out-of-range ids are ignored, no panic.
        r.mark_draining(&[5, 99]);
        assert!(!r.is_draining(5));
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
