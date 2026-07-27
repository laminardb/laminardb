//! Immutable publication record for one managed vnode ownership transition.

use std::sync::Arc;

use bytes::Bytes;
use laminar_core::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, PipelineIdentity,
};
use laminar_core::state::NodeId;

use crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut;
use crate::error::DbError;
use crate::rebalance::AuditedCommittedDrainTransition;
use crate::recovery_manager::vnode_chains::LoadedVnodeChains;

pub(crate) type PendingVnodeTransitionHandle =
    Arc<parking_lot::Mutex<Option<Arc<PendingVnodeTransition>>>>;

pub(crate) type InstalledVnodeStateHandle =
    Arc<parking_lot::Mutex<Option<InstalledVnodeStateBinding>>>;

/// Exact assignment and logical state ABI installed in the current graph generation.
///
/// This is a success marker, not recovery intent. It is cleared before a transition can mutate
/// operator state and published only after every callback and authority revalidation succeeds.
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

/// How the process obtained authority to publish a pending transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VnodeTransitionOrigin {
    /// Startup is rebuilding a new graph from one exact committed cut.
    BootRecovery,
    /// A durable assignment successor names the exact assignment being replaced.
    AssignmentChange {
        predecessor: CheckpointAssignmentFence,
    },
}

/// Completion rule for one pending transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VnodeTransitionKind {
    /// The target grants this process data-plane ownership after boot or assignment change.
    OwnershipChange,
    /// The process lost its last vnode under an audited committed drain.
    CommittedFinalOwnerExit(AssignmentDrainTransition),
}

/// One vnode's exact committed recovery-chain bytes, pending semantic installation.
#[derive(Debug)]
pub(crate) struct PendingVnodeRestore {
    vnode: u32,
    chain: Box<[Bytes]>,
}

impl PendingVnodeRestore {
    #[must_use]
    pub(crate) const fn vnode(&self) -> u32 {
        self.vnode
    }

    #[must_use]
    pub(crate) fn chain(&self) -> &[Bytes] {
        &self.chain
    }
}

/// Complete immutable input to one graph-level revoke/restore callback batch.
///
/// The outer handle publishes this value as one `Arc`. Consumers must clear the handle only when
/// it still contains that exact allocation; value equality is deliberately insufficient because
/// an identical-looking replacement represents newer work.
#[derive(Debug)]
pub(crate) struct PendingVnodeTransition {
    origin: VnodeTransitionOrigin,
    kind: VnodeTransitionKind,
    target: CheckpointAssignmentFence,
    pipeline_identity: PipelineIdentity,
    restore_cut: Option<ValidatedClusterVnodeRestoreCut>,
    acquired_vnodes: Box<[u32]>,
    revoked_vnodes: Box<[u32]>,
    restores: Box<[PendingVnodeRestore]>,
}

impl PendingVnodeTransition {
    pub(crate) fn boot_recovery(
        target: CheckpointAssignmentFence,
        target_owners: &[NodeId],
        participant: CheckpointParticipant,
        pipeline_identity: PipelineIdentity,
        restore_cut: ValidatedClusterVnodeRestoreCut,
        loaded: LoadedVnodeChains,
    ) -> Result<Self, DbError> {
        Self::validate_fence_owners("boot target", &target, target_owners)?;
        Self::validate_target_process(&target, target_owners, participant)?;
        let acquired_vnodes = owned_vnodes(target_owners, participant.node_id)?;
        if acquired_vnodes.is_empty() {
            return Err(transition_error(
                "boot recovery cannot publish an empty vnode transition",
            ));
        }
        let restores =
            Self::validate_restores(&acquired_vnodes, &pipeline_identity, &restore_cut, loaded)?;
        Ok(Self {
            origin: VnodeTransitionOrigin::BootRecovery,
            kind: VnodeTransitionKind::OwnershipChange,
            target,
            pipeline_identity,
            restore_cut: Some(restore_cut),
            acquired_vnodes: acquired_vnodes.into_boxed_slice(),
            revoked_vnodes: Box::default(),
            restores: restores.into_boxed_slice(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn assignment_change(
        predecessor: CheckpointAssignmentFence,
        predecessor_owners: &[NodeId],
        target: CheckpointAssignmentFence,
        target_owners: &[NodeId],
        participant: CheckpointParticipant,
        pipeline_identity: PipelineIdentity,
        restore_cut: Option<ValidatedClusterVnodeRestoreCut>,
        loaded: LoadedVnodeChains,
        local_state_requires_full_restore: bool,
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
            // A failure-recovery successor may retain the stable node id while replacing its
            // process incarnation. This process owns none of the predecessor's in-memory state;
            // every target-owned vnode therefore requires restore.
            Vec::new()
        };
        let target_owned = owned_vnodes(target_owners, participant.node_id)?;
        let acquired_vnodes = if local_state_requires_full_restore {
            target_owned.clone()
        } else {
            sorted_difference(&target_owned, &predecessor_owned)
        };
        let revoked_vnodes = sorted_difference(&predecessor_owned, &target_owned);

        if restore_cut
            .as_ref()
            .is_some_and(|cut| cut.outcome().assignment_fence.as_ref() != Some(&predecessor))
        {
            return Err(transition_error(
                "live assignment restore cut does not match the exact predecessor certificate",
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
            _ => VnodeTransitionKind::OwnershipChange,
        };

        let restores = match (acquired_vnodes.is_empty(), restore_cut.as_ref()) {
            (true, None) if loaded.attempt.is_none() && loaded.chains.is_empty() => Vec::new(),
            (true, _) => {
                return Err(transition_error(
                    "a transition without acquired vnodes carried restore state",
                ));
            }
            (false, Some(cut)) => {
                Self::validate_restores(&acquired_vnodes, &pipeline_identity, cut, loaded)?
            }
            (false, None) => {
                return Err(transition_error(
                    "acquired vnodes require one exact committed restore cut",
                ));
            }
        };
        if acquired_vnodes.is_empty() && revoked_vnodes.is_empty() {
            return Err(transition_error(
                "assignment transition contains no local vnode work",
            ));
        }

        Ok(Self {
            origin: VnodeTransitionOrigin::AssignmentChange { predecessor },
            kind,
            target,
            pipeline_identity,
            restore_cut,
            acquired_vnodes: acquired_vnodes.into_boxed_slice(),
            revoked_vnodes: revoked_vnodes.into_boxed_slice(),
            restores: restores.into_boxed_slice(),
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
                "target assignment does not bind the local ownership to this process incarnation",
            ));
        }
        Ok(())
    }

    fn validate_restores(
        acquired_vnodes: &[u32],
        pipeline_identity: &PipelineIdentity,
        restore_cut: &ValidatedClusterVnodeRestoreCut,
        loaded: LoadedVnodeChains,
    ) -> Result<Vec<PendingVnodeRestore>, DbError> {
        restore_cut
            .validate_transition_binding()
            .map_err(|error| transition_error(format!("invalid committed restore cut: {error}")))?;
        if restore_cut.pipeline_identity() != pipeline_identity {
            return Err(transition_error(
                "restore cut pipeline identity does not match the pending transition",
            ));
        }
        if loaded.attempt != Some(restore_cut.attempt()) {
            return Err(transition_error(format!(
                "loaded vnode attempt {:?} does not match restore cut {:?}",
                loaded.attempt,
                restore_cut.attempt()
            )));
        }
        let inventory = restore_cut.inventory();
        let mut loaded_vnodes: Vec<u32> = loaded.chains.keys().copied().collect();
        loaded_vnodes.sort_unstable();
        if loaded_vnodes != acquired_vnodes {
            return Err(transition_error(format!(
                "loaded vnode roster {loaded_vnodes:?} does not match acquired roster {acquired_vnodes:?}"
            )));
        }
        if acquired_vnodes
            .iter()
            .any(|vnode| inventory.required_vnodes.binary_search(vnode).is_err())
        {
            return Err(transition_error(
                "restore cut seal does not attest every acquired vnode",
            ));
        }
        let mut chains = loaded.chains;
        acquired_vnodes
            .iter()
            .map(|vnode| {
                let chain = chains.remove(vnode).ok_or_else(|| {
                    transition_error(format!("acquired vnode {vnode} has no loaded chain"))
                })?;
                if chain.is_empty() {
                    return Err(transition_error(format!(
                        "acquired vnode {vnode} has an empty recovery chain"
                    )));
                }
                Ok(PendingVnodeRestore {
                    vnode: *vnode,
                    chain: chain.into_boxed_slice(),
                })
            })
            .collect()
    }

    #[must_use]
    pub(crate) const fn origin(&self) -> &VnodeTransitionOrigin {
        &self.origin
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
    pub(crate) const fn restore_cut(&self) -> Option<&ValidatedClusterVnodeRestoreCut> {
        self.restore_cut.as_ref()
    }

    #[must_use]
    pub(crate) fn acquired_vnodes(&self) -> &[u32] {
        &self.acquired_vnodes
    }

    #[must_use]
    pub(crate) fn revoked_vnodes(&self) -> &[u32] {
        &self.revoked_vnodes
    }

    #[must_use]
    pub(crate) fn restores(&self) -> &[PendingVnodeRestore] {
        &self.restores
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
mod tests;
