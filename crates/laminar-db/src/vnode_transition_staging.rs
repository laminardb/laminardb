//! Immutable publication record for one managed vnode ownership transition.

use std::sync::Arc;

use bytes::Bytes;
use laminar_core::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, PipelineIdentity,
};
use laminar_core::state::NodeId;

use crate::checkpoint_coordinator::ValidatedClusterVnodeTransitionBinding;
use crate::error::DbError;
use crate::rebalance::AuditedCommittedDrainTransition;
use crate::recovery_manager::vnode_chains::LoadedVnodeChains;
use crate::vnode_restore_input::{
    VnodeRestoreAlignmentCopyReservation, VnodeRestoreInputReservation, VnodeRestoreInputUsage,
};

pub(crate) type PendingVnodeTransitionHandle =
    Arc<parking_lot::Mutex<Option<Arc<PendingVnodeTransition>>>>;

pub(crate) type InstalledVnodeStateHandle =
    Arc<parking_lot::Mutex<Option<InstalledVnodeStateBinding>>>;

/// Remove one abandoned transition without consuming a newer replacement in the shared slot.
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
    restore_binding: Option<ValidatedClusterVnodeTransitionBinding>,
    acquired_vnodes: Box<[u32]>,
    revoked_vnodes: Box<[u32]>,
    restores: Box<[PendingVnodeRestore]>,
    /// Held for exactly as long as the raw checkpoint bodies in `restores`.
    restore_input_reservation: Option<VnodeRestoreInputReservation>,
}

struct ValidatedVnodeRestores {
    restores: Vec<PendingVnodeRestore>,
    reservation: VnodeRestoreInputReservation,
}

impl PendingVnodeTransition {
    /// Whether this transition owns raw checkpoint bodies that must be retired with a terminally
    /// failed restore attempt. Revoke-only final-owner exits deliberately retain their durable
    /// staging authority for retry and therefore return `false`.
    pub(crate) fn has_restore_input_reservation(&self) -> bool {
        self.restore_input_reservation.is_some()
    }

    pub(crate) fn boot_recovery(
        target: CheckpointAssignmentFence,
        target_owners: &[NodeId],
        participant: CheckpointParticipant,
        pipeline_identity: PipelineIdentity,
        restore_binding: ValidatedClusterVnodeTransitionBinding,
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
        let validated = Self::validate_restores(
            &acquired_vnodes,
            &pipeline_identity,
            &restore_binding,
            loaded,
        )?;
        Ok(Self {
            origin: VnodeTransitionOrigin::BootRecovery,
            kind: VnodeTransitionKind::OwnershipChange,
            target,
            pipeline_identity,
            restore_binding: Some(restore_binding),
            acquired_vnodes: acquired_vnodes.into_boxed_slice(),
            revoked_vnodes: Box::default(),
            restores: validated.restores.into_boxed_slice(),
            restore_input_reservation: Some(validated.reservation),
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
        restore_binding: Option<ValidatedClusterVnodeTransitionBinding>,
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

        if restore_binding
            .as_ref()
            .is_some_and(|binding| !binding.matches_source_assignment(&predecessor))
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

        let (restores, restore_input_reservation) =
            match (acquired_vnodes.is_empty(), restore_binding.as_ref()) {
                (true, None) if loaded.attempt.is_none() && loaded.chains.is_empty() => {
                    validate_restore_input_usage(&[], loaded.input_usage())?;
                    let mut loaded = loaded;
                    if loaded.take_input_reservation().is_some() {
                        return Err(transition_error(
                            "a transition without acquired vnodes carried a raw-input reservation",
                        ));
                    }
                    (Vec::new(), None)
                }
                (true, _) => {
                    return Err(transition_error(
                        "a transition without acquired vnodes carried restore state",
                    ));
                }
                (false, Some(cut)) => {
                    let validated =
                        Self::validate_restores(&acquired_vnodes, &pipeline_identity, cut, loaded)?;
                    (validated.restores, Some(validated.reservation))
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
            restore_binding,
            acquired_vnodes: acquired_vnodes.into_boxed_slice(),
            revoked_vnodes: revoked_vnodes.into_boxed_slice(),
            restores: restores.into_boxed_slice(),
            restore_input_reservation,
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
        restore_binding: &ValidatedClusterVnodeTransitionBinding,
        mut loaded: LoadedVnodeChains,
    ) -> Result<ValidatedVnodeRestores, DbError> {
        if restore_binding.pipeline_identity() != pipeline_identity {
            return Err(transition_error(
                "restore cut pipeline identity does not match the pending transition",
            ));
        }
        if loaded.attempt != Some(restore_binding.attempt()) {
            return Err(transition_error(format!(
                "loaded vnode attempt {:?} does not match restore cut {:?}",
                loaded.attempt,
                restore_binding.attempt()
            )));
        }
        let mut loaded_vnodes: Vec<u32> = loaded.chains.keys().copied().collect();
        loaded_vnodes.sort_unstable();
        if loaded_vnodes != acquired_vnodes {
            return Err(transition_error(format!(
                "loaded vnode roster {loaded_vnodes:?} does not match acquired roster {acquired_vnodes:?}"
            )));
        }
        if acquired_vnodes
            .iter()
            .any(|vnode| !restore_binding.covers_vnode_index(*vnode))
        {
            return Err(transition_error(
                "restore cut sealed domain does not cover every acquired vnode",
            ));
        }
        let usage = loaded.input_usage();
        let reservation = loaded.take_input_reservation();
        let mut chains = loaded.chains;
        let restores = acquired_vnodes
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
            .collect::<Result<Vec<_>, DbError>>()?;
        validate_restore_input_usage(&restores, usage)?;
        let reservation = reservation.ok_or_else(|| {
            transition_error("a nonempty vnode restore has no raw-input reservation")
        })?;
        if !reservation.matches(usage) {
            return Err(transition_error(
                "vnode restore raw-input reservation does not match its declared lineage",
            ));
        }
        Ok(ValidatedVnodeRestores {
            restores,
            reservation,
        })
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
    pub(crate) const fn restore_binding(&self) -> Option<&ValidatedClusterVnodeTransitionBinding> {
        self.restore_binding.as_ref()
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

    pub(crate) fn reserve_inner_alignment_copy(
        &self,
        bytes: u64,
    ) -> Result<VnodeRestoreAlignmentCopyReservation, DbError> {
        self.restore_input_reservation
            .as_ref()
            .ok_or_else(|| {
                transition_error(
                    "a vnode restore cannot align inner archives without its input reservation",
                )
            })?
            .try_reserve_inner_alignment_copy(bytes)
    }
}

fn validate_restore_input_usage(
    restores: &[PendingVnodeRestore],
    usage: VnodeRestoreInputUsage,
) -> Result<(), DbError> {
    usage
        .validate_for_loaded_chains(restores.len())
        .map_err(|message| {
            transition_error(format!("invalid vnode restore input usage: {message}"))
        })?;

    let (retained_bytes, retained_artifacts) =
        restores
            .iter()
            .try_fold((0_u64, 0_u64), |(total_bytes, total_artifacts), restore| {
                let chain_bytes = restore.chain.iter().try_fold(0_u64, |total, body| {
                    let body_bytes = u64::try_from(body.len()).map_err(|_| {
                        transition_error("retained vnode restore body size does not fit u64")
                    })?;
                    total.checked_add(body_bytes).ok_or_else(|| {
                        transition_error("retained vnode restore body byte accounting overflow")
                    })
                })?;
                let chain_artifacts = u64::try_from(restore.chain.len()).map_err(|_| {
                    transition_error("retained vnode restore link count does not fit u64")
                })?;
                let total_bytes = total_bytes.checked_add(chain_bytes).ok_or_else(|| {
                    transition_error("retained vnode restore body byte accounting overflow")
                })?;
                let total_artifacts =
                    total_artifacts
                        .checked_add(chain_artifacts)
                        .ok_or_else(|| {
                            transition_error("retained vnode restore artifact accounting overflow")
                        })?;
                Ok::<_, DbError>((total_bytes, total_artifacts))
            })?;
    if retained_bytes > usage.verified_body_bytes()
        || retained_artifacts > usage.verified_body_artifacts()
    {
        return Err(transition_error(format!(
            "vnode restore input usage covers {} bytes/{} artifacts, but the retained apply chains require {retained_bytes} bytes/{retained_artifacts} artifacts",
            usage.verified_body_bytes(),
            usage.verified_body_artifacts()
        )));
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
mod tests;
