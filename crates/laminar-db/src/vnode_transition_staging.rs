//! Assignment-fenced vnode revocation staged for one graph generation.

use std::sync::Arc;

use laminar_core::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, PipelineIdentity,
};
use laminar_core::state::NodeId;

use crate::error::DbError;
use crate::rebalance::AuditedCommittedDrainTransition;

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

/// Completion authority for one revoke-only transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VnodeTransitionKind {
    /// The process retains at least one vnode after dropping the revoked set.
    Revoke,
    /// The process loses its last vnode under an audited committed drain.
    CommittedFinalOwnerExit(AssignmentDrainTransition),
}

/// Immutable revoke batch published before the target assignment becomes executable.
#[derive(Debug)]
pub(crate) struct PendingVnodeTransition {
    predecessor: CheckpointAssignmentFence,
    kind: VnodeTransitionKind,
    target: CheckpointAssignmentFence,
    pipeline_identity: PipelineIdentity,
    revoked_vnodes: Box<[u32]>,
}

impl PendingVnodeTransition {
    pub(crate) fn assignment_change(
        predecessor: CheckpointAssignmentFence,
        predecessor_owners: &[NodeId],
        target: CheckpointAssignmentFence,
        target_owners: &[NodeId],
        participant: CheckpointParticipant,
        pipeline_identity: PipelineIdentity,
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

        let predecessor_owned = if predecessor.participant_incarnation(participant.node_id)
            == Some(participant.boot_incarnation)
        {
            owned_vnodes(predecessor_owners, participant.node_id)?
        } else {
            Vec::new()
        };
        let target_owned = owned_vnodes(target_owners, participant.node_id)?;
        let acquired = sorted_difference(&target_owned, &predecessor_owned);
        if !acquired.is_empty() {
            return Err(transition_error(format!(
                "committed-frame vnode reassignment is not implemented; refusing to acquire vnodes {acquired:?} before assignment publication"
            )));
        }
        let revoked_vnodes = sorted_difference(&predecessor_owned, &target_owned);
        if revoked_vnodes.is_empty() {
            return Err(transition_error(
                "assignment transition contains no local vnode work",
            ));
        }

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
            _ => VnodeTransitionKind::Revoke,
        };

        Ok(Self {
            predecessor,
            kind,
            target,
            pipeline_identity,
            revoked_vnodes: revoked_vnodes.into_boxed_slice(),
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
    fn assignment_change_accepts_revoke_and_rejects_acquisition() {
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
            None,
        )
        .unwrap();
        assert_eq!(transition.kind(), &VnodeTransitionKind::Revoke);
        assert_eq!(transition.revoked_vnodes(), &[0]);

        let acquisition_predecessor = fence(1, &[2, 1], vec![local, remote]);
        let acquisition_target = fence(2, &[1, 1], vec![local]);
        let error = PendingVnodeTransition::assignment_change(
            acquisition_predecessor,
            &[NodeId(2), NodeId(1)],
            acquisition_target,
            &[NodeId(1), NodeId(1)],
            local,
            PipelineIdentity::empty(),
            None,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("committed-frame vnode reassignment is not implemented"),
            "{error}"
        );
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
