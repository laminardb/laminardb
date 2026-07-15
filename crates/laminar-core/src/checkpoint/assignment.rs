//! Feature-neutral checkpoint assignment certificate.

use uuid::Uuid;

use super::LeaderProof;

/// Canonical wire version for source-drain receipts.
pub const SOURCE_DRAIN_RECEIPT_VERSION: u16 = 1;

/// Maximum participants supported by the current all-to-all shuffle transport.
///
/// Every receiver admits one persistent stream from each of the other 128 participants; the
/// receiver itself does not consume an inbound peer stream. Assignment certificates above this
/// bound cannot seal a cluster-wide shuffle barrier and therefore fail admission.
pub const MAX_CHECKPOINT_PARTICIPANTS: usize = 128 + 1;

/// One exact process participating in a checkpoint cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointParticipant {
    /// Stable logical node identifier.
    pub node_id: u64,
    /// Boot-unique process identity. A same-node restart is a different participant.
    pub boot_incarnation: Uuid,
}

/// One process's exact adopted assignment identity, published into its control-plane slot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointAssignmentAdoption {
    /// Reporting process.
    pub participant: CheckpointParticipant,
    /// Adopted assignment version.
    pub assignment_version: u64,
    /// Adopted vnode count.
    pub vnode_count: u32,
    /// Digest of the adopted ordered vnode-owner map.
    pub assignment_digest: [u8; 32],
}

impl CheckpointAssignmentAdoption {
    /// Whether every field has its canonical production shape.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && self.assignment_version != 0
            && self.vnode_count != 0
            && self.assignment_digest != [0; 32]
    }

    /// Whether this report adopted the same map identity as `fence`.
    #[must_use]
    pub fn matches_fence(&self, fence: &CheckpointAssignmentFence) -> bool {
        self.assignment_version == fence.assignment_version
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
    /// Number of vnodes in the ordered owner map.
    pub vnode_count: u32,
    /// SHA-256 of `vnode_count` and the ordered vnode-to-owner map.
    pub assignment_digest: [u8; 32],
    /// Canonical participants, sorted by node id and bound to exact boot incarnations.
    pub participants: Vec<CheckpointParticipant>,
}

#[derive(serde::Deserialize)]
struct UncheckedCheckpointAssignmentFence {
    assignment_version: u64,
    vnode_count: u32,
    assignment_digest: [u8; 32],
    participants: Vec<CheckpointParticipant>,
}

impl TryFrom<UncheckedCheckpointAssignmentFence> for CheckpointAssignmentFence {
    type Error = &'static str;

    fn try_from(unchecked: UncheckedCheckpointAssignmentFence) -> Result<Self, Self::Error> {
        let fence = Self {
            assignment_version: unchecked.assignment_version,
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
        let fence = Self {
            assignment_version,
            vnode_count,
            assignment_digest: Self::owner_map_digest(vnode_count, owners),
            participants,
        };
        if !fence.is_canonical() {
            return Err("checkpoint assignment certificate is not canonical".into());
        }
        if owners.iter().any(|owner| !fence.contains(*owner)) {
            return Err("checkpoint assignment owner is absent from the participant roster".into());
        }
        Ok(fence)
    }

    /// Stable digest of a canonical ordered vnode-to-owner map.
    #[must_use]
    pub fn owner_map_digest(vnode_count: u32, owners: &[u64]) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hash = Sha256::new();
        hash.update(b"laminardb-vnode-owner-map-v1\0");
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
            && self.vnode_count != 0
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
        usize::try_from(self.vnode_count).ok() == Some(owners.len())
            && self.assignment_digest == Self::owner_map_digest(self.vnode_count, owners)
    }

    /// Stable SHA-256 binding of every certificate dimension.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hash = Sha256::new();
        hash.update(b"laminardb-checkpoint-assignment-v2\0");
        hash.update(self.assignment_version.to_le_bytes());
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

/// Exact predecessor-to-target transition whose revoking source inputs must drain.
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

/// Canonical digest of an ascending, duplicate-free vnode set.
///
/// # Errors
/// Returns an error when the set is not strictly ascending.
pub fn source_drain_vnode_digest(vnodes: &[u32]) -> Result<[u8; 32], String> {
    use sha2::{Digest, Sha256};

    if !vnodes.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err("source drain vnode set is not strictly ascending".into());
    }
    let mut hash = Sha256::new();
    hash.update(b"laminardb-source-drain-vnodes-v1\0");
    hash.update(
        u64::try_from(vnodes.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for vnode in vnodes {
        hash.update(vnode.to_le_bytes());
    }
    Ok(hash.finalize().into())
}

/// Stable source identity used by drain receipt sets.
#[must_use]
pub fn source_drain_source_id(source_name: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut hash = Sha256::new();
    hash.update(b"laminardb-source-drain-source-v1\0");
    hash.update(
        u64::try_from(source_name.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hash.update(source_name.as_bytes());
    hash.finalize().into()
}

/// Receipt proving one exact local source reached its external-input cut.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceDrainReceipt {
    /// Receipt schema version.
    pub protocol_version: u16,
    /// Exact predecessor/target/leader transition.
    pub round: AssignmentDrainId,
    /// Process that owns this source task.
    pub participant: CheckpointParticipant,
    /// Stable catalog source identity.
    pub source_id: [u8; 32],
    /// Boot-unique source-task identity, preventing reuse across an in-process restart.
    pub source_task_incarnation: Uuid,
    /// Number of revoking vnodes observed by the source task.
    pub revoking_vnode_count: u32,
    /// Canonical digest of those vnodes.
    pub revoking_vnode_digest: [u8; 32],
    /// Number of concrete connector inputs paused for the cut.
    pub revoked_input_count: u32,
    /// Canonical connector-defined digest of those inputs.
    pub revoked_input_digest: [u8; 32],
    /// Canonical connector-defined digest of their next-to-read positions.
    pub cut_cursor_digest: [u8; 32],
}

impl SourceDrainReceipt {
    /// Whether every field has its canonical production shape.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.protocol_version == SOURCE_DRAIN_RECEIPT_VERSION
            && self.round.is_canonical()
            && self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && self.source_id != [0; 32]
            && !self.source_task_incarnation.is_nil()
            && self.revoking_vnode_digest != [0; 32]
            && self.revoked_input_digest != [0; 32]
            && self.cut_cursor_digest != [0; 32]
    }

    /// Stable digest included in the node-local receipt-set proof.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hash = Sha256::new();
        hash.update(b"laminardb-source-drain-receipt-v1\0");
        hash.update(self.protocol_version.to_le_bytes());
        hash.update(self.round.predecessor_version.to_le_bytes());
        hash.update(self.round.target_version.to_le_bytes());
        hash.update(self.round.digest);
        hash.update(self.participant.node_id.to_le_bytes());
        hash.update(self.participant.boot_incarnation.as_bytes());
        hash.update(self.source_id);
        hash.update(self.source_task_incarnation.as_bytes());
        hash.update(self.revoking_vnode_count.to_le_bytes());
        hash.update(self.revoking_vnode_digest);
        hash.update(self.revoked_input_count.to_le_bytes());
        hash.update(self.revoked_input_digest);
        hash.update(self.cut_cursor_digest);
        hash.finalize().into()
    }
}

/// Canonical digest of the exact local source plan required to acknowledge a drain.
///
/// # Errors
/// Returns an error for an empty/non-canonical identity or duplicate source identity.
pub fn source_drain_plan_digest(source_ids: &[[u8; 32]]) -> Result<[u8; 32], String> {
    use sha2::{Digest, Sha256};

    let mut sorted = source_ids.to_vec();
    sorted.sort_unstable();
    if sorted.iter().any(|identity| *identity == [0; 32]) {
        return Err("source drain plan contains an empty source identity".into());
    }
    if !sorted.windows(2).all(|pair| pair[0] != pair[1]) {
        return Err("source drain plan contains a duplicate source identity".into());
    }
    let mut hash = Sha256::new();
    hash.update(b"laminardb-source-drain-plan-v1\0");
    hash.update(
        u64::try_from(sorted.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for identity in sorted {
        hash.update(identity);
    }
    Ok(hash.finalize().into())
}

/// Bounded node-local proof that every required source task reached the same drain round.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeDrainReceiptAggregate {
    /// Exact predecessor/target/leader transition digest.
    pub round_digest: [u8; 32],
    /// Canonical digest of the source identities required on this process.
    pub source_plan_digest: [u8; 32],
    /// Exact number of receipts included in `receipt_set_digest`.
    pub receipt_count: u32,
    /// Canonical sorted digest of the complete local receipt set.
    pub receipt_set_digest: [u8; 32],
}

impl NodeDrainReceiptAggregate {
    /// Build the aggregate for one exact process and transition.
    ///
    /// # Errors
    /// Returns an error for a missing, duplicate, stale, or cross-process source receipt.
    pub fn new(
        transition: &AssignmentDrainTransition,
        participant: CheckpointParticipant,
        receipts: &[SourceDrainReceipt],
    ) -> Result<Self, String> {
        use sha2::{Digest, Sha256};

        if !transition.is_canonical()
            || transition
                .predecessor
                .participant_incarnation(participant.node_id)
                != Some(participant.boot_incarnation)
        {
            return Err("source drain aggregate participant is absent from predecessor".into());
        }
        let round = transition.id();
        let mut ordered = receipts.to_vec();
        ordered.sort_unstable_by_key(|receipt| receipt.source_id);
        if ordered
            .windows(2)
            .any(|pair| pair[0].source_id == pair[1].source_id)
        {
            return Err("source drain aggregate contains duplicate source receipts".into());
        }
        if ordered.iter().any(|receipt| {
            !receipt.is_canonical() || receipt.round != round || receipt.participant != participant
        }) {
            return Err("source drain aggregate contains a stale or non-canonical receipt".into());
        }
        let source_ids: Vec<[u8; 32]> = ordered.iter().map(|receipt| receipt.source_id).collect();
        let source_plan_digest = source_drain_plan_digest(&source_ids)?;
        let receipt_count = u32::try_from(ordered.len())
            .map_err(|_| "source drain receipt count exceeds u32::MAX")?;
        let mut hash = Sha256::new();
        hash.update(b"laminardb-source-drain-receipt-set-v1\0");
        hash.update(round.digest);
        hash.update(source_plan_digest);
        hash.update(receipt_count.to_le_bytes());
        for receipt in ordered {
            hash.update(receipt.digest());
        }
        Ok(Self {
            round_digest: round.digest,
            source_plan_digest,
            receipt_count,
            receipt_set_digest: hash.finalize().into(),
        })
    }

    /// Whether every aggregate field has a canonical production value.
    #[must_use]
    pub fn is_canonical(self) -> bool {
        self.round_digest != [0; 32]
            && self.source_plan_digest != [0; 32]
            && self.receipt_set_digest != [0; 32]
    }
}

#[cfg(test)]
mod tests {
    use super::{
        source_drain_source_id, source_drain_vnode_digest, AssignmentDrainTransition,
        CheckpointAssignmentFence, CheckpointParticipant, NodeDrainReceiptAggregate,
        SourceDrainReceipt, MAX_CHECKPOINT_PARTICIPANTS, SOURCE_DRAIN_RECEIPT_VERSION,
    };
    use crate::checkpoint::{LeaderProof, LeaderProofOwner};
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
    }

    #[test]
    fn certificate_participant_limit_accepts_129_and_rejects_130() {
        let maximum = u64::try_from(MAX_CHECKPOINT_PARTICIPANTS).unwrap();
        let participants = (1..=maximum)
            .map(|node_id| participant(node_id, u128::from(node_id)))
            .collect();
        let fence = CheckpointAssignmentFence::from_owner_map(7, &[1], participants).unwrap();
        assert!(fence.is_canonical());
        assert_eq!(fence.participants.len(), MAX_CHECKPOINT_PARTICIPANTS);

        let oversized = (1..=maximum + 1)
            .map(|node_id| participant(node_id, u128::from(node_id)))
            .collect();
        assert!(matches!(
            CheckpointAssignmentFence::from_owner_map(8, &[1], oversized),
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

    #[test]
    fn node_drain_aggregate_is_canonical_and_rejects_duplicate_source_receipts() {
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
            AssignmentDrainTransition::new(predecessor, target, leader(1, 11)).unwrap();
        let process = participant(1, 11);
        let receipt = SourceDrainReceipt {
            protocol_version: SOURCE_DRAIN_RECEIPT_VERSION,
            round: transition.id(),
            participant: process,
            source_id: source_drain_source_id("orders"),
            source_task_incarnation: Uuid::from_u128(101),
            revoking_vnode_count: 1,
            revoking_vnode_digest: source_drain_vnode_digest(&[0]).unwrap(),
            revoked_input_count: 1,
            revoked_input_digest: [7; 32],
            cut_cursor_digest: [8; 32],
        };
        let aggregate =
            NodeDrainReceiptAggregate::new(&transition, process, std::slice::from_ref(&receipt))
                .unwrap();
        assert_eq!(aggregate.receipt_count, 1);
        assert!(aggregate.is_canonical());

        assert!(
            NodeDrainReceiptAggregate::new(&transition, process, &[receipt.clone(), receipt])
                .is_err()
        );
    }
}
