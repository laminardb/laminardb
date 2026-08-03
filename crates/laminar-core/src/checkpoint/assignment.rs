//! Feature-neutral checkpoint assignment certificate.

use uuid::Uuid;

use super::LeaderProof;
use crate::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};

/// Maximum participants supported by the current all-to-all shuffle transport.
///
/// Every receiver admits one persistent stream from each of the other 128 participants; the
/// receiver itself does not consume an inbound peer stream. Assignment certificates above this
/// bound cannot establish a cluster-wide shuffle barrier and therefore fail admission.
pub const MAX_CHECKPOINT_PARTICIPANTS: usize = 128 + 1;

/// One exact process participating in a checkpoint cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointParticipant {
    /// Stable logical node identifier.
    pub node_id: u64,
    /// Boot-unique process identity. A same-node restart is a different participant.
    pub boot_incarnation: Uuid,
}

/// One process's exact adopted assignment identity and local vnode-state readiness, published into
/// its control-plane slot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "UncheckedCheckpointAssignmentAdoption")]
pub struct CheckpointAssignmentAdoption {
    /// Reporting process.
    pub participant: CheckpointParticipant,
    /// Adopted assignment version.
    pub assignment_version: u64,
    /// Key encoding, hashing, and key-group mapping contract that was adopted.
    pub partitioning_abi_version: u16,
    /// Adopted vnode count.
    pub vnode_count: u32,
    /// Digest of the adopted partitioning ABI and ordered vnode-owner map.
    pub assignment_digest: [u8; 32],
    /// Whether every vnode transition for this assignment has completed semantic graph install.
    /// Transport activation intentionally does not require this bit; assignment rotation does.
    pub vnode_state_ready: bool,
}

#[derive(serde::Deserialize)]
struct UncheckedCheckpointAssignmentAdoption {
    participant: CheckpointParticipant,
    assignment_version: u64,
    partitioning_abi_version: u16,
    vnode_count: u32,
    assignment_digest: [u8; 32],
    vnode_state_ready: bool,
}

impl TryFrom<UncheckedCheckpointAssignmentAdoption> for CheckpointAssignmentAdoption {
    type Error = &'static str;

    fn try_from(unchecked: UncheckedCheckpointAssignmentAdoption) -> Result<Self, Self::Error> {
        let adoption = Self {
            participant: unchecked.participant,
            assignment_version: unchecked.assignment_version,
            partitioning_abi_version: unchecked.partitioning_abi_version,
            vnode_count: unchecked.vnode_count,
            assignment_digest: unchecked.assignment_digest,
            vnode_state_ready: unchecked.vnode_state_ready,
        };
        adoption
            .is_canonical()
            .then_some(adoption)
            .ok_or("checkpoint assignment adoption is not canonical")
    }
}

impl CheckpointAssignmentAdoption {
    /// Whether every field has its canonical production shape.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && self.assignment_version != 0
            && self.partitioning_abi_version == PARTITIONING_ABI_VERSION
            && KeyGroupCount::try_from(self.vnode_count).is_ok()
            && self.assignment_digest != [0; 32]
    }

    /// Whether this report adopted the same map identity as `fence`.
    #[must_use]
    pub fn matches_fence(&self, fence: &CheckpointAssignmentFence) -> bool {
        self.assignment_version == fence.assignment_version
            && self.partitioning_abi_version == fence.partitioning_abi_version
            && self.vnode_count == fence.vnode_count
            && self.assignment_digest == fence.assignment_digest
            && fence.participant_incarnation(self.participant.node_id)
                == Some(self.participant.boot_incarnation)
    }
}

/// Versioned proof of one exact vnode-owner map and its process-complete participant roster.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "UncheckedCheckpointAssignmentFence")]
pub struct CheckpointAssignmentFence {
    /// Exact vnode-assignment version covered by this proof.
    pub assignment_version: u64,
    /// Key encoding, hashing, and key-group mapping contract covered by this proof.
    pub partitioning_abi_version: u16,
    /// Number of vnodes in the ordered owner map.
    pub vnode_count: u32,
    /// SHA-256 of the partitioning ABI, `vnode_count`, and ordered vnode-to-owner map.
    pub assignment_digest: [u8; 32],
    /// Canonical participants, sorted by node id and bound to exact boot incarnations.
    pub participants: Vec<CheckpointParticipant>,
}

#[derive(serde::Deserialize)]
struct UncheckedCheckpointAssignmentFence {
    assignment_version: u64,
    partitioning_abi_version: u16,
    vnode_count: u32,
    assignment_digest: [u8; 32],
    participants: Vec<CheckpointParticipant>,
}

impl TryFrom<UncheckedCheckpointAssignmentFence> for CheckpointAssignmentFence {
    type Error = &'static str;

    fn try_from(unchecked: UncheckedCheckpointAssignmentFence) -> Result<Self, Self::Error> {
        let fence = Self {
            assignment_version: unchecked.assignment_version,
            partitioning_abi_version: unchecked.partitioning_abi_version,
            vnode_count: unchecked.vnode_count,
            assignment_digest: unchecked.assignment_digest,
            participants: unchecked.participants,
        };
        fence
            .is_canonical()
            .then_some(fence)
            .ok_or("checkpoint assignment certificate is not canonical")
    }
}

impl CheckpointAssignmentFence {
    /// Build a certificate from the exact ordered vnode-owner map.
    ///
    /// # Errors
    /// Returns an error when the version, owner map, or participant roster is non-canonical.
    pub fn from_owner_map(
        assignment_version: u64,
        owners: &[u64],
        participants: Vec<CheckpointParticipant>,
    ) -> Result<Self, String> {
        if participants.len() > MAX_CHECKPOINT_PARTICIPANTS {
            return Err(format!(
                "checkpoint assignment has {} participants; maximum is {MAX_CHECKPOINT_PARTICIPANTS}",
                participants.len()
            ));
        }
        let vnode_count = u32::try_from(owners.len())
            .map_err(|_| "checkpoint assignment has more than u32::MAX vnodes".to_string())?;
        KeyGroupCount::try_from(vnode_count).map_err(|_| {
            format!(
                "checkpoint assignment key-group count must be between 1 and {}, got {vnode_count}",
                crate::state::MAX_KEY_GROUP_COUNT
            )
        })?;
        let fence = Self {
            assignment_version,
            partitioning_abi_version: PARTITIONING_ABI_VERSION,
            vnode_count,
            assignment_digest: Self::owner_map_digest(vnode_count, owners),
            participants,
        };
        if !fence.is_canonical() {
            return Err("checkpoint assignment certificate is not canonical".into());
        }
        let owner_ids: std::collections::BTreeSet<u64> = owners.iter().copied().collect();
        if owner_ids.len() != fence.participants.len()
            || fence
                .participants
                .iter()
                .any(|participant| !owner_ids.contains(&participant.node_id))
        {
            return Err("checkpoint participants must be the exact vnode-owner set".into());
        }
        Ok(fence)
    }

    /// Stable digest of the current partitioning ABI and a canonical ordered vnode-owner map.
    #[must_use]
    pub fn owner_map_digest(vnode_count: u32, owners: &[u64]) -> [u8; 32] {
        Self::owner_map_digest_for_abi(PARTITIONING_ABI_VERSION, vnode_count, owners)
    }

    fn owner_map_digest_for_abi(
        partitioning_abi_version: u16,
        vnode_count: u32,
        owners: &[u64],
    ) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hash = Sha256::new();
        hash.update(b"laminardb-vnode-owner-map-v2\0");
        hash.update(partitioning_abi_version.to_le_bytes());
        hash.update(vnode_count.to_le_bytes());
        hash.update(
            u64::try_from(owners.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for owner in owners {
            hash.update(owner.to_le_bytes());
        }
        hash.finalize().into()
    }

    /// Whether every field has its canonical production shape.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.assignment_version != 0
            && self.partitioning_abi_version == PARTITIONING_ABI_VERSION
            && KeyGroupCount::try_from(self.vnode_count).is_ok()
            && self.assignment_digest != [0; 32]
            && !self.participants.is_empty()
            && self.participants.len() <= MAX_CHECKPOINT_PARTICIPANTS
            && self
                .participants
                .windows(2)
                .all(|pair| pair[0].node_id < pair[1].node_id)
            && self.participants.iter().all(|participant| {
                participant.node_id != 0 && !participant.boot_incarnation.is_nil()
            })
    }

    /// Whether `node_id` belongs to the canonical participant roster.
    #[must_use]
    pub fn contains(&self, node_id: u64) -> bool {
        self.participants
            .binary_search_by_key(&node_id, |participant| participant.node_id)
            .is_ok()
    }

    /// Exact boot identity certified for `node_id`.
    #[must_use]
    pub fn participant_incarnation(&self, node_id: u64) -> Option<Uuid> {
        self.participants
            .binary_search_by_key(&node_id, |participant| participant.node_id)
            .ok()
            .map(|index| self.participants[index].boot_incarnation)
    }

    /// Canonical participant node ids.
    #[must_use]
    pub fn participant_ids(&self) -> Vec<u64> {
        self.participants
            .iter()
            .map(|participant| participant.node_id)
            .collect()
    }

    /// Whether this certificate binds the supplied exact ordered owner map.
    #[must_use]
    pub fn matches_owner_map(&self, owners: &[u64]) -> bool {
        self.partitioning_abi_version == PARTITIONING_ABI_VERSION
            && usize::try_from(self.vnode_count).ok() == Some(owners.len())
            && self.assignment_digest == Self::owner_map_digest(self.vnode_count, owners)
            && {
                let owner_ids: std::collections::BTreeSet<u64> = owners.iter().copied().collect();
                owner_ids.len() == self.participants.len()
                    && self
                        .participants
                        .iter()
                        .all(|participant| owner_ids.contains(&participant.node_id))
            }
    }

    /// Stable SHA-256 binding of every certificate dimension.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hash = Sha256::new();
        hash.update(b"laminardb-checkpoint-assignment-v3\0");
        hash.update(self.assignment_version.to_le_bytes());
        hash.update(self.partitioning_abi_version.to_le_bytes());
        hash.update(self.vnode_count.to_le_bytes());
        hash.update(self.assignment_digest);
        hash.update(
            u64::try_from(self.participants.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        for participant in &self.participants {
            hash.update(participant.node_id.to_le_bytes());
            hash.update(participant.boot_incarnation.as_bytes());
        }
        hash.finalize().into()
    }
}

/// Compact identity of one exact assignment transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AssignmentDrainId {
    /// Assignment version that remains authoritative while sources drain.
    pub predecessor_version: u64,
    /// Exact successor version sources are preparing to adopt.
    pub target_version: u64,
    /// SHA-256 binding both assignment certificates and the initiating leader term.
    pub digest: [u8; 32],
}

impl AssignmentDrainId {
    /// Whether every identity field has a canonical production value.
    #[must_use]
    pub fn is_canonical(self) -> bool {
        self.predecessor_version != 0
            && self.predecessor_version.checked_add(1) == Some(self.target_version)
            && self.digest != [0; 32]
    }
}

/// Exact predecessor-to-target transition at which every predecessor source must stop input.
///
/// Required drain participants always come from `predecessor`. A process that only joins the
/// target has no predecessor input authority and therefore cannot block the drain quorum.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "UncheckedAssignmentDrainTransition")]
pub struct AssignmentDrainTransition {
    /// Assignment that remains authoritative through the pre-rotation checkpoint.
    pub predecessor: CheckpointAssignmentFence,
    /// Exact successor assignment to install after that checkpoint is durable.
    pub target: CheckpointAssignmentFence,
    /// Leader term that created the durable draining transition.
    pub leader: LeaderProof,
}

#[derive(serde::Deserialize)]
struct UncheckedAssignmentDrainTransition {
    predecessor: CheckpointAssignmentFence,
    target: CheckpointAssignmentFence,
    leader: LeaderProof,
}

impl TryFrom<UncheckedAssignmentDrainTransition> for AssignmentDrainTransition {
    type Error = &'static str;

    fn try_from(unchecked: UncheckedAssignmentDrainTransition) -> Result<Self, Self::Error> {
        Self::new(unchecked.predecessor, unchecked.target, unchecked.leader)
            .map_err(|_| "assignment drain transition is not canonical")
    }
}

impl AssignmentDrainTransition {
    /// Construct and validate one exact successor transition.
    ///
    /// # Errors
    /// Returns an error when either certificate, the version relation, vnode count, or captured
    /// leader term is not canonical for the predecessor roster.
    pub fn new(
        predecessor: CheckpointAssignmentFence,
        target: CheckpointAssignmentFence,
        leader: LeaderProof,
    ) -> Result<Self, String> {
        let transition = Self {
            predecessor,
            target,
            leader,
        };
        if !transition.is_canonical() {
            return Err("assignment drain transition is not canonical".into());
        }
        Ok(transition)
    }

    /// Whether the transition is an exact, authority-bound successor of the predecessor.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.predecessor.is_canonical()
            && self.target.is_canonical()
            && self.leader.is_canonical()
            && self.predecessor.vnode_count == self.target.vnode_count
            && self.predecessor.assignment_version.checked_add(1)
                == Some(self.target.assignment_version)
            && (self
                .predecessor
                .participant_incarnation(self.leader.owner.node_id)
                == Some(self.leader.owner.boot_id)
                || self
                    .target
                    .participant_incarnation(self.leader.owner.node_id)
                    == Some(self.leader.owner.boot_id))
    }

    /// Compact identity carried by source commands and receipts.
    #[must_use]
    pub fn id(&self) -> AssignmentDrainId {
        AssignmentDrainId {
            predecessor_version: self.predecessor.assignment_version,
            target_version: self.target.assignment_version,
            digest: self.digest(),
        }
    }

    /// Exact processes that must publish drain acknowledgements.
    #[must_use]
    pub fn required_participants(&self) -> &[CheckpointParticipant] {
        &self.predecessor.participants
    }

    /// Stable SHA-256 binding both assignment maps, both process rosters, and the leader term.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hash = Sha256::new();
        hash.update(b"laminardb-assignment-drain-transition-v1\0");
        hash.update(self.predecessor.digest());
        hash.update(self.target.digest());
        hash.update(self.leader.owner.node_id.to_le_bytes());
        hash.update(self.leader.owner.boot_id.as_bytes());
        hash.update(self.leader.owner.process_term.to_le_bytes());
        hash.update(self.leader.fencing_token.to_le_bytes());
        hash.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AssignmentDrainTransition, CheckpointAssignmentAdoption, CheckpointAssignmentFence,
        CheckpointParticipant, MAX_CHECKPOINT_PARTICIPANTS,
    };
    use crate::checkpoint::{LeaderProof, LeaderProofOwner};
    use crate::state::{MAX_KEY_GROUP_COUNT, PARTITIONING_ABI_VERSION};
    use uuid::Uuid;

    fn participant(node_id: u64, boot: u128) -> CheckpointParticipant {
        CheckpointParticipant {
            node_id,
            boot_incarnation: Uuid::from_u128(boot),
        }
    }

    fn leader(node_id: u64, boot: u128) -> LeaderProof {
        LeaderProof {
            owner: LeaderProofOwner {
                node_id,
                boot_id: Uuid::from_u128(boot),
                process_term: 4,
            },
            fencing_token: 9,
        }
    }

    #[test]
    fn certificate_binds_map_version_and_process_roster() {
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2, 1, 9],
            vec![participant(1, 11), participant(2, 22), participant(9, 99)],
        )
        .unwrap();
        assert!(fence.is_canonical());
        assert!(fence.matches_owner_map(&[1, 2, 1, 9]));
        assert_eq!(fence.participant_ids(), [1, 2, 9]);

        let different_map =
            CheckpointAssignmentFence::from_owner_map(7, &[2, 1, 1, 9], fence.participants.clone())
                .unwrap();
        assert_ne!(fence.digest(), different_map.digest());

        let restarted = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2, 1, 9],
            vec![participant(1, 111), participant(2, 22), participant(9, 99)],
        )
        .unwrap();
        assert_ne!(fence.digest(), restarted.digest());
    }

    #[test]
    fn certificate_digest_binds_partitioning_abi() {
        let fence = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2],
            vec![participant(1, 11), participant(2, 22)],
        )
        .unwrap();
        let mut wrong_abi = fence.clone();
        wrong_abi.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;

        assert_ne!(fence.digest(), wrong_abi.digest());
        assert_ne!(
            CheckpointAssignmentFence::owner_map_digest(2, &[1, 2]),
            CheckpointAssignmentFence::owner_map_digest_for_abi(
                PARTITIONING_ABI_VERSION + 1,
                2,
                &[1, 2]
            )
        );
        assert!(!wrong_abi.is_canonical());
        assert!(!wrong_abi.matches_owner_map(&[1, 2]));
    }

    #[test]
    fn certificate_and_adoption_require_current_partitioning_abi() {
        let fence =
            CheckpointAssignmentFence::from_owner_map(7, &[1], vec![participant(1, 11)]).unwrap();
        let mut missing_fence = serde_json::to_value(&fence).unwrap();
        missing_fence
            .as_object_mut()
            .unwrap()
            .remove("partitioning_abi_version");
        assert!(serde_json::from_value::<CheckpointAssignmentFence>(missing_fence).is_err());

        let mut wrong_fence = fence.clone();
        wrong_fence.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;
        assert!(serde_json::from_value::<CheckpointAssignmentFence>(
            serde_json::to_value(wrong_fence).unwrap()
        )
        .is_err());

        let adoption = CheckpointAssignmentAdoption {
            participant: participant(1, 11),
            assignment_version: fence.assignment_version,
            partitioning_abi_version: PARTITIONING_ABI_VERSION,
            vnode_count: fence.vnode_count,
            assignment_digest: fence.assignment_digest,
            vnode_state_ready: true,
        };
        assert!(adoption.is_canonical());
        assert!(adoption.matches_fence(&fence));

        let mut missing_adoption = serde_json::to_value(&adoption).unwrap();
        missing_adoption
            .as_object_mut()
            .unwrap()
            .remove("partitioning_abi_version");
        assert!(serde_json::from_value::<CheckpointAssignmentAdoption>(missing_adoption).is_err());

        let mut missing_readiness = serde_json::to_value(&adoption).unwrap();
        missing_readiness
            .as_object_mut()
            .unwrap()
            .remove("vnode_state_ready");
        assert!(serde_json::from_value::<CheckpointAssignmentAdoption>(missing_readiness).is_err());

        let mut wrong_adoption = adoption;
        wrong_adoption.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;
        assert!(serde_json::from_value::<CheckpointAssignmentAdoption>(
            serde_json::to_value(wrong_adoption).unwrap()
        )
        .is_err());
    }

    #[test]
    fn malformed_or_incomplete_certificates_fail_closed() {
        assert!(
            CheckpointAssignmentFence::from_owner_map(7, &[1, 2], vec![participant(1, 11)])
                .is_err()
        );
        assert!(CheckpointAssignmentFence::from_owner_map(
            7,
            &[1],
            vec![participant(1, 11), participant(1, 12)]
        )
        .is_err());
        assert!(
            CheckpointAssignmentFence::from_owner_map(0, &[1], vec![participant(1, 11)]).is_err()
        );
        assert!(matches!(
            CheckpointAssignmentFence::from_owner_map(
                7,
                &[1],
                vec![participant(1, 11), participant(2, 22)]
            ),
            Err(message) if message.contains("exact vnode-owner set")
        ));
    }

    #[test]
    fn certificate_rejects_more_than_the_partitioning_abi_limit() {
        let owner_count = usize::try_from(MAX_KEY_GROUP_COUNT).unwrap() + 1;
        let owners = vec![1; owner_count];

        assert!(matches!(
            CheckpointAssignmentFence::from_owner_map(7, &owners, vec![participant(1, 11)]),
            Err(message) if message.contains("key-group count")
        ));
    }

    #[test]
    fn certificate_participant_limit_accepts_129_and_rejects_130() {
        let maximum = u64::try_from(MAX_CHECKPOINT_PARTICIPANTS).unwrap();
        let participants = (1..=maximum)
            .map(|node_id| participant(node_id, u128::from(node_id)))
            .collect();
        let owners = (1..=maximum).collect::<Vec<_>>();
        let fence = CheckpointAssignmentFence::from_owner_map(7, &owners, participants).unwrap();
        assert!(fence.is_canonical());
        assert_eq!(fence.participants.len(), MAX_CHECKPOINT_PARTICIPANTS);

        let oversized = (1..=maximum + 1)
            .map(|node_id| participant(node_id, u128::from(node_id)))
            .collect();
        let oversized_owners = (1..=maximum + 1).collect::<Vec<_>>();
        assert!(matches!(
            CheckpointAssignmentFence::from_owner_map(8, &oversized_owners, oversized),
            Err(message) if message.contains("maximum is 129")
        ));

        let mut forged = fence;
        forged
            .participants
            .push(participant(maximum + 1, u128::from(maximum + 1)));
        assert!(!forged.is_canonical());

        let encoded = serde_json::to_vec(&forged).unwrap();
        assert!(serde_json::from_slice::<CheckpointAssignmentFence>(&encoded).is_err());
    }

    #[test]
    fn drain_transition_acks_predecessor_roster_and_accepts_target_only_leader() {
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2],
            vec![participant(1, 11), participant(2, 22)],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            8,
            &[2, 3],
            vec![participant(2, 22), participant(3, 33)],
        )
        .unwrap();
        let transition =
            AssignmentDrainTransition::new(predecessor, target, leader(3, 33)).unwrap();

        assert_eq!(
            transition
                .required_participants()
                .iter()
                .map(|participant| participant.node_id)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(transition.id().is_canonical());
        assert_ne!(transition.digest(), [0; 32]);
    }

    #[test]
    fn drain_transition_accepts_predecessor_only_leader() {
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1, 2],
            vec![participant(1, 11), participant(2, 22)],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            8,
            &[2, 3],
            vec![participant(2, 22), participant(3, 33)],
        )
        .unwrap();
        assert!(AssignmentDrainTransition::new(predecessor, target, leader(1, 11)).is_ok());
    }

    #[test]
    fn drain_identity_rejects_version_overflow() {
        let identity = super::AssignmentDrainId {
            predecessor_version: u64::MAX,
            target_version: u64::MAX,
            digest: [1; 32],
        };
        assert!(!identity.is_canonical());
    }
}
