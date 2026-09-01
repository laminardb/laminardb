//! Canonical assignment snapshots, recovery references, and drain finalizations.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    current_time_millis, SnapshotError, DRAIN_FINALIZATION_VERSION, MAX_RECOVERY_PROPOSAL_BYTES,
    RECOVERY_MATERIALIZATION_VERSION,
};
use crate::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, LeaderProof,
    MAX_CHECKPOINT_PARTICIPANTS,
};
use crate::cluster::discovery::NodeId;
use crate::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};

/// Durable vnode-to-instance assignment snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentSnapshot {
    /// Monotonic version. Writers bump on each update.
    pub version: u64,
    /// Key encoding, hashing, and key-group mapping contract used by this owner map.
    pub partitioning_abi_version: u16,
    /// Vnode id → owning instance. `BTreeMap` (not `Vec`) so snapshots
    /// with different `vnode_count` are still deserializable — sparse
    /// indices surface as missing keys the caller can diagnose.
    pub vnodes: BTreeMap<u32, NodeId>,
    /// Exact process roster certified for this assignment generation. A stable node restart
    /// changes this roster and therefore requires a new version even when vnode owners do not.
    pub participants: Vec<CheckpointParticipant>,
    /// Wall-clock timestamp of the last update, millis since epoch.
    pub updated_at_ms: i64,
    /// Pre-rotation drain phase: when set, this snapshot carries the *intended*
    /// next assignment but ownership has NOT changed yet. Nodes mark the vnodes
    /// they are about to lose as draining (pausing those source partitions) so the
    /// pre-rotation checkpoint is a clean cut; the leader then publishes the same
    /// assignment with `draining = false` to commit the rotation.
    pub draining: bool,
    /// Exact predecessor, successor, and durable leader term for a draining generation.
    /// Present if and only if `draining` is true.
    #[serde(deserialize_with = "deserialize_drain_transition")]
    pub drain_transition: Option<AssignmentDrainTransition>,
}

fn deserialize_drain_transition<'de, D>(
    deserializer: D,
) -> Result<Option<AssignmentDrainTransition>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::deserialize(deserializer)
}

/// Exact immutable reference to a staged failure-recovery successor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentSnapshotRef {
    /// Assignment generation carried by the referenced snapshot.
    pub version: u64,
    /// Lowercase hexadecimal SHA-256 of its canonical JSON body.
    pub sha256: String,
    /// Exact canonical body length.
    pub encoded_len: u64,
}

/// Create-only logical assignment head for an authority-selected recovery proposal.
///
/// Recovery uses a separate namespace from graceful-drain intents. A delayed drain writer may
/// still win the raw snapshot key after losing leadership, but it cannot replace this immutable
/// materialization winner for the same assignment version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryMaterialization {
    protocol_version: u16,
    pub(super) proposal: AssignmentSnapshotRef,
    pub(super) snapshot: AssignmentSnapshot,
}

impl RecoveryMaterialization {
    pub(super) fn new(
        proposal: AssignmentSnapshotRef,
        snapshot: AssignmentSnapshot,
    ) -> Result<Self, SnapshotError> {
        let materialization = Self {
            protocol_version: RECOVERY_MATERIALIZATION_VERSION,
            proposal,
            snapshot,
        };
        materialization.validate()?;
        Ok(materialization)
    }

    pub(super) fn validate(&self) -> Result<(), SnapshotError> {
        self.proposal.validate()?;
        if self.protocol_version != RECOVERY_MATERIALIZATION_VERSION {
            return Err(SnapshotError::Invalid(format!(
                "unsupported recovery materialization version {}",
                self.protocol_version
            )));
        }
        let (_, actual_reference) = self.snapshot.encode_recovery_proposal()?;
        if actual_reference != self.proposal {
            return Err(SnapshotError::Invalid(
                "recovery materialization body does not match its proposal reference".into(),
            ));
        }
        Ok(())
    }
}

impl AssignmentSnapshotRef {
    /// Validate the reference independently of the staged object.
    ///
    /// # Errors
    /// Rejects a non-successor version, malformed digest, or unsafe encoded length.
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.version < 2 {
            return Err(SnapshotError::Invalid(
                "recovery proposal must be a successor generation".into(),
            ));
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SnapshotError::Invalid(
                "recovery proposal SHA-256 must be 64 lowercase hexadecimal characters".into(),
            ));
        }
        let encoded_len = usize::try_from(self.encoded_len).map_err(|_| {
            SnapshotError::Invalid("recovery proposal encoded length exceeds usize".into())
        })?;
        if encoded_len == 0 || encoded_len > MAX_RECOVERY_PROPOSAL_BYTES {
            return Err(SnapshotError::Invalid(format!(
                "recovery proposal encoded length {} is outside 1..={MAX_RECOVERY_PROPOSAL_BYTES}",
                self.encoded_len
            )));
        }
        Ok(())
    }
}

/// Immutable winner that settles one draining snapshot without changing its certified target
/// version. The original transition remains append-only at the snapshot path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DrainFinalization {
    protocol_version: u16,
    transition_digest: [u8; 32],
    pub(super) proposal: AssignmentSnapshot,
}

impl DrainFinalization {
    pub(super) fn new(
        draining: &AssignmentSnapshot,
        proposal: AssignmentSnapshot,
    ) -> Result<Self, SnapshotError> {
        let transition = draining.drain_transition.as_ref().ok_or_else(|| {
            SnapshotError::Invalid("drain finalization requires a draining transition".into())
        })?;
        let finalization = Self {
            protocol_version: DRAIN_FINALIZATION_VERSION,
            transition_digest: transition.digest(),
            proposal,
        };
        finalization.validate_against(draining)?;
        Ok(finalization)
    }

    pub(super) fn validate_against(
        &self,
        draining: &AssignmentSnapshot,
    ) -> Result<(), SnapshotError> {
        draining.validate()?;
        self.proposal.validate()?;
        let transition = draining.drain_transition.as_ref().ok_or_else(|| {
            SnapshotError::Invalid("drain finalization requires a draining transition".into())
        })?;
        if self.protocol_version != DRAIN_FINALIZATION_VERSION
            || self.transition_digest != transition.digest()
            || !draining.draining
            || self.proposal.draining
            || self.proposal.version != draining.version
            || self.proposal.drain_transition.is_some()
        {
            return Err(SnapshotError::Invalid(
                "drain finalization does not preserve the exact transition identity".into(),
            ));
        }
        let proposed_fence = self.proposal.assignment_fence()?;
        let predecessor = &transition.predecessor;
        let commits_target = proposed_fence == transition.target;
        let aborts_to_predecessor = proposed_fence.assignment_version
            == transition.target.assignment_version
            && proposed_fence.vnode_count == predecessor.vnode_count
            && proposed_fence.assignment_digest == predecessor.assignment_digest
            && proposed_fence.participants == predecessor.participants;
        if !commits_target && !aborts_to_predecessor {
            return Err(SnapshotError::Invalid(
                "drain finalization is neither the certified target nor exact predecessor rollback"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl AssignmentSnapshot {
    /// Empty snapshot at version 0.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            partitioning_abi_version: PARTITIONING_ABI_VERSION,
            vnodes: BTreeMap::new(),
            participants: Vec::new(),
            updated_at_ms: 0,
            draining: false,
            drain_transition: None,
        }
    }

    /// Next snapshot with bumped version and current wall-clock time.
    ///
    /// # Errors
    ///
    /// Returns an error if the version overflows or the successor is noncanonical.
    pub fn next(&self, vnodes: BTreeMap<u32, NodeId>) -> Result<Self, SnapshotError> {
        self.next_for_participants(vnodes, self.participants.clone())
    }

    /// Next snapshot bound to the supplied canonical process roster.
    ///
    /// # Errors
    ///
    /// Returns an error if the version overflows or the successor is noncanonical.
    pub fn next_for_participants(
        &self,
        vnodes: BTreeMap<u32, NodeId>,
        participants: Vec<CheckpointParticipant>,
    ) -> Result<Self, SnapshotError> {
        let version = self
            .version
            .checked_add(1)
            .ok_or_else(|| SnapshotError::Invalid("assignment snapshot version overflow".into()))?;
        let next = Self {
            version,
            partitioning_abi_version: self.partitioning_abi_version,
            vnodes,
            participants,
            updated_at_ms: current_time_millis(),
            draining: false,
            drain_transition: None,
        };
        next.validate()?;
        Ok(next)
    }

    fn validate_assignment(&self) -> Result<(), SnapshotError> {
        if self.partitioning_abi_version != PARTITIONING_ABI_VERSION {
            return Err(SnapshotError::Invalid(format!(
                "assignment snapshot partitioning ABI {} does not match runtime ABI {PARTITIONING_ABI_VERSION}",
                self.partitioning_abi_version
            )));
        }
        if self.participants.len() > MAX_CHECKPOINT_PARTICIPANTS {
            return Err(SnapshotError::Invalid(format!(
                "assignment snapshot has {} participants; maximum is {MAX_CHECKPOINT_PARTICIPANTS}",
                self.participants.len()
            )));
        }
        let vnode_count = u32::try_from(self.vnodes.len()).map_err(|_| {
            SnapshotError::Invalid("assignment snapshot has more than u32::MAX key groups".into())
        })?;
        KeyGroupCount::try_from(vnode_count).map_err(|_| {
            SnapshotError::Invalid(format!(
                "assignment snapshot key-group count must be between 1 and {}, got {vnode_count}",
                crate::state::MAX_KEY_GROUP_COUNT
            ))
        })?;
        let dense = !self.vnodes.is_empty()
            && self
                .vnodes
                .keys()
                .copied()
                .zip(0_u32..)
                .all(|(actual, expected)| actual == expected);
        let canonical_participants = !self.participants.is_empty()
            && self
                .participants
                .windows(2)
                .all(|pair| pair[0].node_id < pair[1].node_id)
            && self.participants.iter().all(|participant| {
                participant.node_id != 0 && !participant.boot_incarnation.is_nil()
            })
            && {
                let owners: BTreeSet<u64> = self.vnodes.values().map(|owner| owner.0).collect();
                owners.len() == self.participants.len()
                    && self
                        .participants
                        .iter()
                        .all(|participant| owners.contains(&participant.node_id))
            };
        if self.version == 0 || !dense || !canonical_participants {
            return Err(SnapshotError::Invalid(
                "assignment snapshot is not canonical".into(),
            ));
        }
        Ok(())
    }

    /// Validate the durable owner map, process roster, and optional drain transition.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot or drain transition is noncanonical.
    pub fn validate(&self) -> Result<(), SnapshotError> {
        self.validate_assignment()?;
        match (self.draining, self.drain_transition.as_ref()) {
            (false, None) => Ok(()),
            (true, Some(transition)) => {
                let target = self.assignment_fence_unchecked()?;
                if !transition.is_canonical() || transition.target != target {
                    return Err(SnapshotError::Invalid(
                        "draining snapshot does not match its exact target transition".into(),
                    ));
                }
                Ok(())
            }
            _ => Err(SnapshotError::Invalid(
                "assignment drain flag and transition disagree".into(),
            )),
        }
    }

    pub(super) fn encode_recovery_proposal(
        &self,
    ) -> Result<(Vec<u8>, AssignmentSnapshotRef), SnapshotError> {
        self.validate()?;
        if self.draining || self.drain_transition.is_some() || self.version < 2 {
            return Err(SnapshotError::Invalid(
                "recovery proposal must be a committed successor generation".into(),
            ));
        }
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > MAX_RECOVERY_PROPOSAL_BYTES {
            return Err(SnapshotError::Invalid(format!(
                "encoded recovery proposal is {} bytes; maximum is {MAX_RECOVERY_PROPOSAL_BYTES}",
                encoded.len()
            )));
        }
        let reference = AssignmentSnapshotRef {
            version: self.version,
            sha256: format!("{:x}", Sha256::digest(&encoded)),
            encoded_len: u64::try_from(encoded.len()).map_err(|_| {
                SnapshotError::Invalid("recovery proposal encoded length overflow".into())
            })?,
        };
        reference.validate()?;
        Ok((encoded, reference))
    }

    fn assignment_fence_unchecked(&self) -> Result<CheckpointAssignmentFence, SnapshotError> {
        let owners: Vec<u64> = self.vnodes.values().map(|owner| owner.0).collect();
        CheckpointAssignmentFence::from_owner_map(self.version, &owners, self.participants.clone())
            .map_err(SnapshotError::Invalid)
    }

    /// Exact checkpoint certificate represented by this snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the assignment map or process roster is noncanonical.
    pub fn assignment_fence(&self) -> Result<CheckpointAssignmentFence, SnapshotError> {
        self.validate_assignment()?;
        self.assignment_fence_unchecked()
    }

    /// Create the exact successor as a leader-fenced draining generation.
    ///
    /// # Errors
    /// Rejects a non-committed predecessor, invalid successor, or leader proof outside both
    /// certified rosters.
    pub fn next_draining(
        &self,
        vnodes: BTreeMap<u32, NodeId>,
        participants: Vec<CheckpointParticipant>,
        leader: LeaderProof,
    ) -> Result<Self, SnapshotError> {
        self.validate()?;
        if self.draining {
            return Err(SnapshotError::Invalid(
                "cannot start a drain from a draining assignment".into(),
            ));
        }
        let predecessor = self.assignment_fence()?;
        let mut target = self.next_for_participants(vnodes, participants)?;
        let target_fence = target.assignment_fence()?;
        target.drain_transition = Some(
            AssignmentDrainTransition::new(predecessor, target_fence, leader)
                .map_err(SnapshotError::Invalid)?,
        );
        target.draining = true;
        target.validate()?;
        Ok(target)
    }

    /// Convert a draining generation into its committed target without changing identity.
    ///
    /// # Errors
    ///
    /// Returns an error unless this is a valid draining snapshot.
    pub fn committed_target(&self) -> Result<Self, SnapshotError> {
        self.validate()?;
        if !self.draining {
            return Err(SnapshotError::Invalid(
                "only a draining assignment has a target to commit".into(),
            ));
        }
        let mut committed = self.clone();
        committed.draining = false;
        committed.drain_transition = None;
        committed.updated_at_ms = current_time_millis();
        committed.validate()?;
        Ok(committed)
    }

    /// Convert a draining generation into a committed rollback of its predecessor map.
    ///
    /// # Errors
    ///
    /// Returns an error when either snapshot is invalid or the predecessor does not match.
    pub fn aborted_target(&self, predecessor: &Self) -> Result<Self, SnapshotError> {
        self.validate()?;
        predecessor.validate()?;
        let transition = self.drain_transition.as_ref().ok_or_else(|| {
            SnapshotError::Invalid("draining assignment has no transition".into())
        })?;
        if predecessor.draining
            || predecessor.assignment_fence()? != transition.predecessor
            || self.version != predecessor.version.saturating_add(1)
        {
            return Err(SnapshotError::Invalid(
                "drain rollback does not match the exact predecessor".into(),
            ));
        }
        let mut aborted = predecessor.clone();
        aborted.version = self.version;
        aborted.updated_at_ms = current_time_millis();
        aborted.draining = false;
        aborted.drain_transition = None;
        aborted.validate()?;
        Ok(aborted)
    }

    /// Whether the durable process roster is canonical and covers every vnode owner.
    #[must_use]
    pub fn has_canonical_participants(&self) -> bool {
        self.validate().is_ok()
    }

    /// Convert a `Vec<NodeId>` (one entry per vnode id, dense) into the
    /// `BTreeMap` shape this snapshot uses. Mirrors the layout returned
    /// by `rendezvous_assignment`.
    #[must_use]
    pub fn vnodes_from_vec(assignment: &[NodeId]) -> BTreeMap<u32, NodeId> {
        #[allow(clippy::cast_possible_truncation)]
        assignment
            .iter()
            .enumerate()
            .map(|(i, n)| (i as u32, *n))
            .collect()
    }

    /// Convert the canonical owner map to a dense vector of exactly `vnode_count` entries.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot is invalid or its vnode map is not exactly dense.
    pub fn to_vnode_vec(&self, vnode_count: u32) -> Result<Vec<NodeId>, SnapshotError> {
        self.validate()?;
        if usize::try_from(vnode_count).ok() != Some(self.vnodes.len()) {
            return Err(SnapshotError::Invalid(format!(
                "assignment {} vnode cardinality {} does not match runtime cardinality {vnode_count}",
                self.version,
                self.vnodes.len()
            )));
        }
        (0..vnode_count)
            .map(|v| {
                self.vnodes.get(&v).copied().ok_or_else(|| {
                    SnapshotError::Invalid(format!(
                        "assignment {} is missing vnode {v}",
                        self.version
                    ))
                })
            })
            .collect()
    }
}
