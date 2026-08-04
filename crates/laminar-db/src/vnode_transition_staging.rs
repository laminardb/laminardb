//! Assignment-fenced vnode state transition staged for one graph generation.

use std::sync::Arc;

use laminar_core::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, PipelineIdentity,
    StateFrameKey,
};
use laminar_core::state::NodeId;

use crate::error::DbError;
use crate::rebalance::AuditedCommittedDrainTransition;
use crate::recovery_manager::RecoveredStateFrame;

pub(crate) type PendingVnodeTransitionHandle =
    Arc<parking_lot::Mutex<Option<Arc<PendingVnodeTransition>>>>;

pub(crate) type InstalledVnodeStateHandle =
    Arc<parking_lot::Mutex<Option<InstalledVnodeStateBinding>>>;

pub(crate) fn retire_exact_pending_vnode_transition(
    handle: &PendingVnodeTransitionHandle,
    expected: &Arc<PendingVnodeTransition>,
) -> bool {
    let mut pending = handle.lock();
    if !pending
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        return false;
    }
    pending.take();
    true
}

/// Exact assignment and state ABI installed in the current graph generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstalledVnodeStateBinding {
    assignment: CheckpointAssignmentFence,
    pipeline_identity: PipelineIdentity,
}

impl InstalledVnodeStateBinding {
    pub(crate) fn new(
        assignment: CheckpointAssignmentFence,
        pipeline_identity: PipelineIdentity,
    ) -> Result<Self, DbError> {
        if !assignment.is_canonical() {
            return Err(transition_error(
                "installed vnode state has a non-canonical assignment certificate",
            ));
        }
        Ok(Self {
            assignment,
            pipeline_identity,
        })
    }

    #[must_use]
    pub(crate) fn matches(
        &self,
        assignment: &CheckpointAssignmentFence,
        pipeline_identity: &PipelineIdentity,
    ) -> bool {
        self.assignment == *assignment && self.pipeline_identity == *pipeline_identity
    }
}

/// Completion authority for one assignment transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VnodeTransitionKind {
    /// The process remains in the target assignment, with or without local revocation.
    RetainedOwner,
    /// The process loses its last vnode under an audited committed drain.
    CommittedFinalOwnerExit(AssignmentDrainTransition),
}

/// Immutable state transition published before the target assignment becomes executable.
#[derive(Debug)]
pub(crate) struct PendingVnodeTransition {
    predecessor: CheckpointAssignmentFence,
    kind: VnodeTransitionKind,
    target: CheckpointAssignmentFence,
    pipeline_identity: PipelineIdentity,
    revoked_vnodes: Box<[u32]>,
    acquired_vnodes: Box<[u32]>,
    state_frames: Box<[RecoveredStateFrame]>,
    requires_predecessor_binding: bool,
}

impl PendingVnodeTransition {
    pub(crate) fn assignment_change(
        predecessor: CheckpointAssignmentFence,
        predecessor_owners: &[NodeId],
        target: CheckpointAssignmentFence,
        target_owners: &[NodeId],
        participant: CheckpointParticipant,
        pipeline_identity: PipelineIdentity,
        state_frames: Vec<RecoveredStateFrame>,
        final_owner_exit: Option<AuditedCommittedDrainTransition>,
    ) -> Result<Self, DbError> {
        Self::validate_fence_owners("predecessor", &predecessor, predecessor_owners)?;
        Self::validate_fence_owners("target", &target, target_owners)?;
        if predecessor.assignment_version.checked_add(1) != Some(target.assignment_version) {
            return Err(transition_error(format!(
                "assignment transition must be adjacent; observed {} -> {}",
                predecessor.assignment_version, target.assignment_version
            )));
        }
        if target.vnode_count != predecessor.vnode_count {
            return Err(transition_error(
                "assignment transition changed the vnode cardinality",
            ));
        }
        Self::validate_target_process(&target, target_owners, participant)?;

        let requires_predecessor_binding = predecessor.participant_incarnation(participant.node_id)
            == Some(participant.boot_incarnation);
        let predecessor_owned = if requires_predecessor_binding {
            owned_vnodes(predecessor_owners, participant.node_id)?
        } else {
            Vec::new()
        };
        let target_owned = owned_vnodes(target_owners, participant.node_id)?;
        let acquired = sorted_difference(&target_owned, &predecessor_owned);
        validate_state_frames(&state_frames, &acquired, predecessor_owners)?;
        let revoked_vnodes = sorted_difference(&predecessor_owned, &target_owned);
        let kind = match (
            target_owned.is_empty(),
            predecessor_owned.is_empty(),
            final_owner_exit,
        ) {
            (true, false, Some(authority)) => {
                let transition = authority.into_transition();
                if transition.predecessor != predecessor || transition.target != target {
                    return Err(transition_error(
                        "final-owner exit authority does not match the transition endpoints",
                    ));
                }
                VnodeTransitionKind::CommittedFinalOwnerExit(transition)
            }
            (true, false, None) => {
                return Err(transition_error(
                    "losing the final local vnode requires an audited committed drain",
                ));
            }
            (_, _, Some(_)) => {
                return Err(transition_error(
                    "final-owner exit authority was supplied for a non-final transition",
                ));
            }
            _ => VnodeTransitionKind::RetainedOwner,
        };

        Ok(Self {
            predecessor,
            kind,
            target,
            pipeline_identity,
            revoked_vnodes: revoked_vnodes.into_boxed_slice(),
            acquired_vnodes: acquired.into_boxed_slice(),
            state_frames: state_frames.into_boxed_slice(),
            requires_predecessor_binding,
        })
    }

    fn validate_fence_owners(
        label: &str,
        fence: &CheckpointAssignmentFence,
        owners: &[NodeId],
    ) -> Result<(), DbError> {
        let owner_ids: Vec<u64> = owners.iter().map(|owner| owner.0).collect();
        if !fence.is_canonical() || !fence.matches_owner_map(&owner_ids) {
            return Err(transition_error(format!(
                "{label} assignment certificate does not match its owner map"
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_target_process(
        target: &CheckpointAssignmentFence,
        target_owners: &[NodeId],
        participant: CheckpointParticipant,
    ) -> Result<(), DbError> {
        let owned = target_owners
            .iter()
            .any(|owner| owner.0 == participant.node_id);
        let target_incarnation = target.participant_incarnation(participant.node_id);
        if (owned && target_incarnation != Some(participant.boot_incarnation))
            || (!owned && target_incarnation.is_some())
        {
            return Err(transition_error(
                "target assignment does not bind local ownership to this process incarnation",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub(crate) const fn predecessor(&self) -> &CheckpointAssignmentFence {
        &self.predecessor
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> &VnodeTransitionKind {
        &self.kind
    }

    #[must_use]
    pub(crate) const fn target(&self) -> &CheckpointAssignmentFence {
        &self.target
    }

    #[must_use]
    pub(crate) const fn pipeline_identity(&self) -> &PipelineIdentity {
        &self.pipeline_identity
    }

    #[must_use]
    pub(crate) fn revoked_vnodes(&self) -> &[u32] {
        &self.revoked_vnodes
    }

    #[must_use]
    pub(crate) fn acquired_vnodes(&self) -> &[u32] {
        &self.acquired_vnodes
    }

    #[must_use]
    pub(crate) fn state_frames(&self) -> &[RecoveredStateFrame] {
        &self.state_frames
    }

    #[must_use]
    pub(crate) const fn requires_predecessor_binding(&self) -> bool {
        self.requires_predecessor_binding
    }
}

fn validate_state_frames(
    frames: &[RecoveredStateFrame],
    acquired: &[u32],
    predecessor_owners: &[NodeId],
) -> Result<(), DbError> {
    if frames.windows(2).any(|pair| {
        (pair[0].participant_id, &pair[0].key) >= (pair[1].participant_id, &pair[1].key)
    }) {
        return Err(transition_error(
            "recovered transition frames are not in canonical participant/key order",
        ));
    }
    let donors = acquired
        .iter()
        .map(|vnode| predecessor_owners[*vnode as usize].0)
        .collect::<std::collections::BTreeSet<_>>();
    for frame in frames {
        let operator_id = match &frame.key {
            StateFrameKey::OperatorWhole { operator_id }
            | StateFrameKey::Vnode { operator_id, .. } => operator_id,
        };
        if operator_id.strip_prefix("graph:").is_none_or(str::is_empty) {
            return Err(transition_error(format!(
                "recovered transition contains non-graph state frame '{operator_id}'"
            )));
        }
        match &frame.key {
            StateFrameKey::OperatorWhole { .. } => {
                if !donors.contains(&frame.participant_id) {
                    return Err(transition_error(format!(
                        "whole-operator frame donor {} owns no acquired vnode",
                        frame.participant_id
                    )));
                }
            }
            StateFrameKey::Vnode { vnode, .. } => {
                let vnode = u32::from(*vnode);
                if acquired.binary_search(&vnode).is_err()
                    || predecessor_owners.get(vnode as usize).map(|owner| owner.0)
                        != Some(frame.participant_id)
                {
                    return Err(transition_error(format!(
                        "vnode {vnode} frame donor {} does not own an acquired predecessor vnode",
                        frame.participant_id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn owned_vnodes(owners: &[NodeId], node_id: u64) -> Result<Vec<u32>, DbError> {
    owners
        .iter()
        .enumerate()
        .filter(|(_, owner)| owner.0 == node_id)
        .map(|(vnode, _)| {
            u32::try_from(vnode)
                .map_err(|_| transition_error("vnode owner map exceeds the u32 identifier space"))
        })
        .collect()
}

fn sorted_difference(left: &[u32], right: &[u32]) -> Vec<u32> {
    left.iter()
        .copied()
        .filter(|vnode| right.binary_search(vnode).is_err())
        .collect()
}

fn transition_error(message: impl Into<String>) -> DbError {
    DbError::Checkpoint(format!("[LDB-6053] {}", message.into()))
}

#[cfg(test)]
mod tests {
    use laminar_core::checkpoint::{LeaderProof, LeaderProofOwner};

    use super::*;

    fn participant(node_id: u64, boot: u128) -> CheckpointParticipant {
        CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(boot),
        }
    }

    fn fence(
        version: u64,
        owners: &[u64],
        participants: Vec<CheckpointParticipant>,
    ) -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(version, owners, participants).unwrap()
    }

    #[test]
    fn assignment_change_accepts_exact_revoke_and_acquisition_rosters() {
        let local = participant(1, 1);
        let remote = participant(2, 2);
        let predecessor = fence(1, &[1, 1], vec![local]);
        let target = fence(2, &[2, 1], vec![local, remote]);

        let transition = PendingVnodeTransition::assignment_change(
            predecessor,
            &[NodeId(1), NodeId(1)],
            target,
            &[NodeId(2), NodeId(1)],
            local,
            PipelineIdentity::empty(),
            Vec::new(),
            None,
        )
        .unwrap();
        assert_eq!(transition.kind(), &VnodeTransitionKind::RetainedOwner);
        assert_eq!(transition.revoked_vnodes(), &[0]);

        let acquisition_predecessor = fence(1, &[2, 1], vec![local, remote]);
        let acquisition_target = fence(2, &[1, 1], vec![local]);
        let frame = RecoveredStateFrame {
            participant_id: remote.node_id,
            key: StateFrameKey::Vnode {
                operator_id: "graph:join".into(),
                vnode: 0,
            },
            payload: bytes::Bytes::from_static(b"state"),
        };
        let transition = PendingVnodeTransition::assignment_change(
            acquisition_predecessor,
            &[NodeId(2), NodeId(1)],
            acquisition_target,
            &[NodeId(1), NodeId(1)],
            local,
            PipelineIdentity::empty(),
            vec![frame],
            None,
        )
        .unwrap();
        assert_eq!(transition.acquired_vnodes(), &[0]);
        assert_eq!(transition.state_frames().len(), 1);
    }

    #[test]
    fn assignment_change_preserves_topology_only_generation() {
        let local = participant(1, 1);
        let old_remote = participant(2, 2);
        let new_remote = participant(3, 3);
        let transition = PendingVnodeTransition::assignment_change(
            fence(1, &[1, 2], vec![local, old_remote]),
            &[NodeId(1), NodeId(2)],
            fence(2, &[1, 3], vec![local, new_remote]),
            &[NodeId(1), NodeId(3)],
            local,
            PipelineIdentity::empty(),
            Vec::new(),
            None,
        )
        .unwrap();

        assert_eq!(transition.kind(), &VnodeTransitionKind::RetainedOwner);
        assert!(transition.revoked_vnodes().is_empty());
    }

    #[test]
    fn final_owner_exit_requires_matching_audited_drain() {
        let local = participant(1, 1);
        let remote = participant(2, 2);
        let predecessor = fence(1, &[1], vec![local]);
        let target = fence(2, &[2], vec![remote]);

        let error = PendingVnodeTransition::assignment_change(
            predecessor.clone(),
            &[NodeId(1)],
            target.clone(),
            &[NodeId(2)],
            local,
            PipelineIdentity::empty(),
            Vec::new(),
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("losing the final local vnode requires an audited committed drain"),
            "{error}"
        );

        let drain = AssignmentDrainTransition::new(
            predecessor.clone(),
            target.clone(),
            LeaderProof {
                owner: LeaderProofOwner {
                    node_id: local.node_id,
                    boot_id: local.boot_incarnation,
                    process_term: 1,
                },
                fencing_token: 1,
            },
        )
        .unwrap();
        let authority = AuditedCommittedDrainTransition::from_canonical_for_test(drain).unwrap();
        let transition = PendingVnodeTransition::assignment_change(
            predecessor,
            &[NodeId(1)],
            target,
            &[NodeId(2)],
            local,
            PipelineIdentity::empty(),
            Vec::new(),
            Some(authority),
        )
        .unwrap();
        assert!(matches!(
            transition.kind(),
            VnodeTransitionKind::CommittedFinalOwnerExit(_)
        ));
        assert_eq!(transition.revoked_vnodes(), &[0]);
    }
}
