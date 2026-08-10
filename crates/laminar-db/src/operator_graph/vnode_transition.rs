//! Managed vnode state transitions at assignment boundaries.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use laminar_core::checkpoint::StateFrameKey;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    publish_cluster_execution_poison, ManagedVnodeRestore, ManagedVnodeTransition,
    ManagedVnodeTransitionMode, ManagedWholeRestore, OperatorGraph,
};
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::operator::sql_query::ClusterShuffleConfig;
use crate::recovery_manager::RecoveredStateFrame;
use crate::vnode_transition_staging::{
    validate_recovered_transition_frames, InstalledVnodeStateBinding, InstalledVnodeStateHandle,
    PendingVnodeTransition, PendingVnodeTransitionHandle, VnodeTransitionKind,
};

struct VnodeTransitionAuthoritySnapshot {
    registry: Arc<laminar_core::state::VnodeRegistry>,
    self_id: laminar_core::state::NodeId,
    assignment: laminar_core::state::VnodeAssignmentSnapshot,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    transport_digest: Option<[u8; 32]>,
}

#[derive(Default)]
struct ProjectedTransitionFrames<'a> {
    restores: Vec<ManagedVnodeRestore<'a>>,
    whole_restores: Vec<ManagedWholeRestore<'a>>,
}

impl VnodeTransitionAuthoritySnapshot {
    fn capture(
        config: &ClusterShuffleConfig,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<Self, DbError> {
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if target.assignment_version != assignment.version()
            || !target.matches_owner_map(&owners)
            || config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != config.receiver.incarnation()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode transition target/process does not match assignment {}",
                assignment.version()
            )));
        }
        let transport_digest = if target.contains(config.self_id.0) {
            match (
                config.sender.assignment_version(),
                config.receiver.assignment_version(),
                config.sender.active_assignment_digest(),
                config.receiver.active_assignment_digest(),
            ) {
                (sender_version, receiver_version, Some(sender), Some(receiver))
                    if target.participant_incarnation(config.self_id.0)
                        == Some(config.sender.incarnation())
                        && sender_version == assignment.version()
                        && receiver_version == assignment.version()
                        && sender == receiver
                        && sender == target.digest() =>
                {
                    Some(sender)
                }
                _ => {
                    return Err(DbError::ShuffleNotReady(
                        "[LDB-6051] vnode transition requires matching active sender and receiver assignment certificates"
                            .into(),
                    ));
                }
            }
        } else {
            if target.participant_incarnation(config.self_id.0).is_some()
                || config.sender.assignment_version() != 0
                || config.receiver.assignment_version() != 0
                || config.sender.active_assignment_digest().is_some()
                || config.receiver.active_assignment_digest().is_some()
            {
                return Err(DbError::ShuffleNotReady(
                    "[LDB-6051] zero-owner vnode transition requires inactive shuffle endpoints"
                        .into(),
                ));
            }
            None
        };
        Ok(Self {
            registry: Arc::clone(&config.registry),
            self_id: config.self_id,
            assignment,
            sender: Arc::clone(&config.sender),
            receiver: Arc::clone(&config.receiver),
            transport_digest,
        })
    }

    fn owns(&self, vnode: u32) -> bool {
        self.assignment.owners().get(vnode as usize).copied() == Some(self.self_id)
    }

    fn contains_vnode(&self, vnode: u32) -> bool {
        self.assignment.owners().get(vnode as usize).is_some()
    }

    fn revalidate_for_publication(&self) -> Result<(), DbError> {
        let transport_matches = self.transport_digest.map_or_else(
            || {
                self.sender.assignment_version() == 0
                    && self.receiver.assignment_version() == 0
                    && self.sender.active_assignment_digest().is_none()
                    && self.receiver.active_assignment_digest().is_none()
            },
            |digest| {
                self.sender.assignment_version() == self.assignment.version()
                    && self.receiver.assignment_version() == self.assignment.version()
                    && self.sender.active_assignment_digest() == Some(digest)
                    && self.receiver.active_assignment_digest() == Some(digest)
            },
        );
        if !transport_matches {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] shuffle assignment certificate changed before vnode-state publication"
                    .into(),
            ));
        }
        let current = self.registry.versioned_snapshot();
        if current.version() != self.assignment.version()
            || current.owners() != self.assignment.owners()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode assignment changed from {} before vnode-state publication",
                self.assignment.version()
            )));
        }
        Ok(())
    }
}

struct FinalOwnerExitAuthoritySnapshot {
    registry: Arc<laminar_core::state::VnodeRegistry>,
    self_id: laminar_core::state::NodeId,
    assignment: laminar_core::state::VnodeAssignmentSnapshot,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
}

impl FinalOwnerExitAuthoritySnapshot {
    fn capture(
        config: &ClusterShuffleConfig,
        transition: &laminar_core::checkpoint::AssignmentDrainTransition,
        revoked: &FxHashSet<u32>,
    ) -> Result<Self, DbError> {
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if !transition.is_canonical()
            || transition.target.assignment_version != assignment.version()
            || transition.target.vnode_count != config.registry.vnode_count()
            || !transition.target.matches_owner_map(&owners)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] final-owner-exit authority does not match target assignment {}",
                assignment.version()
            )));
        }
        if transition.target.contains(config.self_id.0) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] final-owner-exit target still certifies local node {}",
                config.self_id.0
            )));
        }
        let predecessor_incarnation = transition
            .predecessor
            .participant_incarnation(config.self_id.0)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] final-owner-exit predecessor does not certify local node {}",
                    config.self_id.0
                ))
            })?;
        if config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != predecessor_incarnation
            || config.receiver.incarnation() != predecessor_incarnation
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit predecessor process identity does not match local shuffle endpoints"
                    .into(),
            ));
        }
        if config.sender.assignment_version() != 0
            || config.receiver.assignment_version() != 0
            || config.sender.active_assignment_digest().is_some()
            || config.receiver.active_assignment_digest().is_some()
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup requires inactive shuffle endpoints".into(),
            ));
        }
        if revoked.is_empty()
            || revoked.iter().any(|vnode| {
                assignment
                    .owners()
                    .get(*vnode as usize)
                    .is_none_or(|owner| *owner == config.self_id)
            })
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit revoke roster is empty, out of range, or still target-owned"
                    .into(),
            ));
        }
        Ok(Self {
            registry: Arc::clone(&config.registry),
            self_id: config.self_id,
            assignment,
            sender: Arc::clone(&config.sender),
            receiver: Arc::clone(&config.receiver),
        })
    }

    fn revalidate_for_publication(&self) -> Result<(), DbError> {
        if self.sender.assignment_version() != 0
            || self.receiver.assignment_version() != 0
            || self.sender.active_assignment_digest().is_some()
            || self.receiver.active_assignment_digest().is_some()
        {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] shuffle authority became active before final-owner-exit publication"
                    .into(),
            ));
        }
        let current = self.registry.versioned_snapshot();
        if current.version() != self.assignment.version()
            || current.owners() != self.assignment.owners()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode assignment changed from {} before final-owner-exit publication",
                self.assignment.version()
            )));
        }
        if current.owners().contains(&self.self_id) {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] final-owner-exit target regained ownership".into(),
            ));
        }
        Ok(())
    }
}

struct PreparedVnodeTransition {
    authority: VnodeTransitionAuthoritySnapshot,
    pending_handle: PendingVnodeTransitionHandle,
    pending: Arc<PendingVnodeTransition>,
    revoked: FxHashSet<u32>,
    installed_state: InstalledVnodeStateHandle,
    installed_binding: Option<InstalledVnodeStateBinding>,
}

struct PreparedFinalOwnerExit {
    authority: FinalOwnerExitAuthoritySnapshot,
    pending_handle: PendingVnodeTransitionHandle,
    pending: Arc<PendingVnodeTransition>,
    revoked: FxHashSet<u32>,
    installed_state: InstalledVnodeStateHandle,
}

struct PreparedManagedOperators {
    node_indices: Vec<usize>,
}

struct VnodeTransitionUnwindGuard {
    poisoned: Arc<AtomicBool>,
    installed_state: InstalledVnodeStateHandle,
    pending_handle: PendingVnodeTransitionHandle,
    pending: Arc<PendingVnodeTransition>,
    armed: bool,
}

impl VnodeTransitionUnwindGuard {
    fn armed(
        poisoned: Arc<AtomicBool>,
        installed_state: InstalledVnodeStateHandle,
        pending_handle: PendingVnodeTransitionHandle,
        pending: Arc<PendingVnodeTransition>,
    ) -> Self {
        Self {
            poisoned,
            installed_state,
            pending_handle,
            pending,
            armed: true,
        }
    }

    fn disarmed(
        poisoned: Arc<AtomicBool>,
        installed_state: InstalledVnodeStateHandle,
        pending_handle: PendingVnodeTransitionHandle,
        pending: Arc<PendingVnodeTransition>,
    ) -> Self {
        Self {
            poisoned,
            installed_state,
            pending_handle,
            pending,
            armed: false,
        }
    }

    fn arm(&mut self) {
        self.armed = true;
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for VnodeTransitionUnwindGuard {
    fn drop(&mut self) {
        if self.armed {
            publish_cluster_execution_poison(
                &self.poisoned,
                Some(&self.installed_state),
                Some((&self.pending_handle, &self.pending)),
            );
        }
    }
}

#[cfg(test)]
fn callback_error(
    poisoned: &AtomicBool,
    installed_state: &InstalledVnodeStateHandle,
    pending_handle: &PendingVnodeTransitionHandle,
    pending: &Arc<PendingVnodeTransition>,
    phase: &str,
    error: DbError,
) -> DbError {
    publish_cluster_execution_poison(
        poisoned,
        Some(installed_state),
        Some((pending_handle, pending)),
    );
    match error {
        DbError::BackpressureFail(reason) => DbError::BackpressureFail(format!(
            "[LDB-6051] vnode transition {phase} failed terminally: {reason}"
        )),
        DbError::ShuffleTerminal(reason) => DbError::ShuffleTerminal(format!(
            "[LDB-6051] vnode transition {phase} failed terminally: {reason}"
        )),
        error => DbError::StatefulOperatorPartialApply(format!(
            "[LDB-6051] vnode transition {phase} returned an indeterminate outcome: {error}"
        )),
    }
}

fn exact_pending_slot<'a>(
    handle: &'a PendingVnodeTransitionHandle,
    expected: &Arc<PendingVnodeTransition>,
    phase: &str,
) -> Result<parking_lot::MutexGuard<'a, Option<Arc<PendingVnodeTransition>>>, DbError> {
    let guard = handle.lock();
    if guard
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        return Ok(guard);
    }
    Err(DbError::ShuffleNotReady(format!(
        "[LDB-6051] pending vnode transition changed during {phase} before publication"
    )))
}

impl OperatorGraph {
    pub(super) fn has_pending_final_owner_exit(&self) -> bool {
        self.pending_vnode_transition
            .as_ref()
            .and_then(|handle| handle.lock().clone())
            .is_some_and(|pending| {
                matches!(
                    pending.kind(),
                    VnodeTransitionKind::CommittedFinalOwnerExit(_)
                )
            })
    }

    pub(super) fn has_pending_vnode_transition(&self) -> bool {
        self.pending_vnode_transition
            .as_ref()
            .is_some_and(|handle| handle.lock().is_some())
    }

    pub(super) fn apply_pending_vnode_transition(&mut self) -> Result<(), DbError> {
        let Some(transition) = self.prepare_pending_vnode_transition()? else {
            return Ok(());
        };
        let mut unwind = VnodeTransitionUnwindGuard::armed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&transition.installed_state),
            Arc::clone(&transition.pending_handle),
            Arc::clone(&transition.pending),
        );
        let prepared = match self.prepare_managed_operators(
            transition.pending.predecessor(),
            transition.pending.target(),
            &transition.revoked,
            transition.pending.acquired_vnodes(),
            transition.pending.state_frames(),
            ManagedVnodeTransitionMode::Live,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                unwind.complete();
                return Err(error);
            }
        };
        #[cfg(test)]
        let callbacks_mutated_state = match self.run_test_vnode_revoke_callbacks(
            &transition.installed_state,
            &transition.pending_handle,
            &transition.pending,
            &transition.revoked,
        ) {
            Ok(callbacks_run) => callbacks_run,
            Err(error) => {
                self.abort_and_finish_managed_operators(&prepared);
                unwind.complete();
                return Err(error);
            }
        };
        #[cfg(not(test))]
        let callbacks_mutated_state = false;
        unwind.complete();
        self.publish_vnode_transition(transition, &prepared, callbacks_mutated_state)
    }

    pub(crate) fn restore_reassigned_vnode_state(
        mut self,
        predecessor: &laminar_core::checkpoint::CheckpointAssignmentFence,
        predecessor_owners: &[laminar_core::state::NodeId],
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
        state_frames: &[RecoveredStateFrame],
    ) -> Result<(Self, usize), DbError> {
        if !self.whole_restore_open {
            return Err(DbError::Checkpoint(
                "[LDB-6051] reassigned checkpoint restore requires a pristine graph".into(),
            ));
        }
        let predecessor_owner_ids = predecessor_owners
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        if predecessor.assignment_version.checked_add(1) != Some(target.assignment_version)
            || predecessor.vnode_count != u32::from(self.key_group_count)
            || target.vnode_count != predecessor.vnode_count
            || !predecessor.is_canonical()
            || !predecessor.matches_owner_map(&predecessor_owner_ids)
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] reassigned checkpoint restore is not an exact adjacent predecessor cut"
                    .into(),
            ));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] reassigned checkpoint restore has no cluster ownership scope".into(),
            )
        })?;
        let authority = VnodeTransitionAuthoritySnapshot::capture(config, target)?;
        let acquired = authority
            .assignment
            .owners()
            .iter()
            .enumerate()
            .filter_map(|(vnode, owner)| {
                (*owner == authority.self_id)
                    .then_some(u32::try_from(vnode).expect("vnode count is represented by u32"))
            })
            .collect::<Vec<_>>();
        if acquired.is_empty() {
            return Err(DbError::Checkpoint(
                "[LDB-6051] reassigned checkpoint bootstrap has no target-owned vnode".into(),
            ));
        }
        validate_recovered_transition_frames(state_frames, &acquired, predecessor_owners)?;

        let revoked = FxHashSet::default();
        let prepared = self.prepare_managed_operators(
            predecessor,
            target,
            &revoked,
            &acquired,
            state_frames,
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners },
        )?;
        if let Err(error) = authority.revalidate_for_publication() {
            self.abort_and_finish_managed_operators(&prepared);
            return Err(error);
        }
        self.publish_managed_operators(&prepared);
        self.observe_managed_state_accounting(&prepared.node_indices);
        self.finish_managed_operators(&prepared);
        self.whole_restore_open = false;
        self.validate_managed_state_budget("reassigned checkpoint restore")?;
        Ok((self, state_frames.len()))
    }

    pub(super) fn apply_committed_final_owner_exit(&mut self) -> Result<(), DbError> {
        let transition = self.prepare_final_owner_exit()?;
        let mut unwind = VnodeTransitionUnwindGuard::armed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&transition.installed_state),
            Arc::clone(&transition.pending_handle),
            Arc::clone(&transition.pending),
        );
        let prepared = match self.prepare_managed_operators(
            transition.pending.predecessor(),
            transition.pending.target(),
            &transition.revoked,
            &[],
            &[],
            ManagedVnodeTransitionMode::Live,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                unwind.complete();
                return Err(error);
            }
        };
        #[cfg(test)]
        let callbacks_mutated_state = match self.run_test_vnode_revoke_callbacks(
            &transition.installed_state,
            &transition.pending_handle,
            &transition.pending,
            &transition.revoked,
        ) {
            Ok(callbacks_run) => callbacks_run,
            Err(error) => {
                self.abort_and_finish_managed_operators(&prepared);
                unwind.complete();
                return Err(error);
            }
        };
        #[cfg(not(test))]
        let callbacks_mutated_state = false;
        unwind.complete();
        self.publish_final_owner_exit(&transition, &prepared, callbacks_mutated_state)
    }

    fn pending_transition_snapshot(
        &self,
    ) -> Option<(PendingVnodeTransitionHandle, Arc<PendingVnodeTransition>)> {
        let handle = self.pending_vnode_transition.as_ref().map(Arc::clone)?;
        let pending = handle.lock().clone()?;
        Some((handle, pending))
    }

    fn validate_pending_pipeline_identity(
        &self,
        pending: &PendingVnodeTransition,
    ) -> Result<(), DbError> {
        if self.pipeline_identity.as_ref() != Some(pending.pipeline_identity()) {
            return Err(DbError::Checkpoint(
                "[LDB-6051] pending vnode transition pipeline identity does not match the graph"
                    .into(),
            ));
        }
        Ok(())
    }

    fn installed_state_handle(&self) -> Result<InstalledVnodeStateHandle, DbError> {
        self.installed_vnode_state
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] pending vnode transition has no installed-state publication handle"
                        .into(),
                )
            })
    }

    fn validate_installed_predecessor(
        pending: &PendingVnodeTransition,
        installed_state: &InstalledVnodeStateHandle,
    ) -> Result<(), DbError> {
        if !Self::installed_state_matches_pending(pending, installed_state.lock().as_ref()) {
            return Err(DbError::Checkpoint(
                "[LDB-6051] graph installed-state binding does not match the staged transition"
                    .into(),
            ));
        }
        Ok(())
    }

    fn installed_state_matches_pending(
        pending: &PendingVnodeTransition,
        installed: Option<&InstalledVnodeStateBinding>,
    ) -> bool {
        if pending.requires_predecessor_binding() {
            installed.is_some_and(|binding| {
                binding.matches(pending.predecessor(), pending.pipeline_identity())
            })
        } else {
            installed.is_none()
        }
    }

    fn prepare_final_owner_exit(&self) -> Result<PreparedFinalOwnerExit, DbError> {
        let (pending_handle, pending) = self.pending_transition_snapshot().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup has no pending transition".into(),
            )
        })?;
        self.validate_pending_pipeline_identity(&pending)?;
        let transition = match pending.kind() {
            VnodeTransitionKind::CommittedFinalOwnerExit(transition) => transition.clone(),
            VnodeTransitionKind::RetainedOwner => {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] retained-owner transition has no committed final-owner-exit authority"
                        .into(),
                ));
            }
        };
        if *pending.predecessor() != transition.predecessor {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit transition has invalid predecessor authority".into(),
            ));
        }
        let revoked: FxHashSet<u32> = pending.revoked_vnodes().iter().copied().collect();
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup has no cluster ownership scope".into(),
            )
        })?;
        let installed_state = self.installed_state_handle()?;
        Self::validate_installed_predecessor(&pending, &installed_state)?;
        let authority = FinalOwnerExitAuthoritySnapshot::capture(config, &transition, &revoked)?;
        Ok(PreparedFinalOwnerExit {
            authority,
            pending_handle,
            pending,
            revoked,
            installed_state,
        })
    }

    fn prepare_pending_vnode_transition(&self) -> Result<Option<PreparedVnodeTransition>, DbError> {
        let Some((pending_handle, pending)) = self.pending_transition_snapshot() else {
            return Ok(None);
        };
        if !matches!(pending.kind(), VnodeTransitionKind::RetainedOwner) {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] committed final-owner-exit cleanup requires the fenced control path"
                    .into(),
            ));
        }
        self.validate_pending_pipeline_identity(&pending)?;
        if pending.predecessor().assignment_version.checked_add(1)
            != Some(pending.target().assignment_version)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] pending assignment transition is not adjacent: {} -> {}",
                pending.predecessor().assignment_version,
                pending.target().assignment_version
            )));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] pending vnode transition has no cluster ownership scope".into(),
            )
        })?;
        let authority = VnodeTransitionAuthoritySnapshot::capture(config, pending.target())?;
        let revoked: FxHashSet<u32> = pending.revoked_vnodes().iter().copied().collect();
        let mut invalid: Vec<u32> = revoked
            .iter()
            .copied()
            .filter(|vnode| !authority.contains_vnode(*vnode) || authority.owns(*vnode))
            .collect();
        invalid.sort_unstable();
        if !invalid.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] revoked vnodes {invalid:?} are outside the target or remain locally owned"
            )));
        }
        let mut invalid_acquired = pending
            .acquired_vnodes()
            .iter()
            .copied()
            .filter(|vnode| !authority.contains_vnode(*vnode) || !authority.owns(*vnode))
            .collect::<Vec<_>>();
        invalid_acquired.sort_unstable();
        if !invalid_acquired.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] acquired vnodes {invalid_acquired:?} are outside the target or not locally owned"
            )));
        }
        let installed_state = self.installed_state_handle()?;
        Self::validate_installed_predecessor(&pending, &installed_state)?;
        let installed_binding = pending
            .target()
            .contains(config.self_id.0)
            .then(|| {
                InstalledVnodeStateBinding::new(
                    pending.target().clone(),
                    pending.pipeline_identity().clone(),
                )
            })
            .transpose()?;
        Ok(Some(PreparedVnodeTransition {
            authority,
            pending_handle,
            pending,
            revoked,
            installed_state,
            installed_binding,
        }))
    }

    fn relevant_revoked_vnodes(
        &self,
        node_idx: usize,
        revoked: &FxHashSet<u32>,
    ) -> Result<FxHashSet<u32>, DbError> {
        let node = &self.nodes[node_idx];
        match node.capability.state_class {
            OperatorStateClass::GlobalSingleton => Ok(if revoked.contains(&0) {
                [0].into_iter().collect()
            } else {
                FxHashSet::default()
            }),
            OperatorStateClass::VnodeKeyed => Ok(revoked.clone()),
            state_class => Err(DbError::Checkpoint(format!(
                "[LDB-6051] managed operator '{}' has unsupported revoke placement {state_class:?}",
                node.name
            ))),
        }
    }

    fn validate_relevant_acquired_vnodes(
        &self,
        node_idx: usize,
        acquired: &[u32],
        restored: &[u32],
    ) -> Result<bool, DbError> {
        let node = &self.nodes[node_idx];
        match node.capability.state_class {
            OperatorStateClass::GlobalSingleton => {
                let relevant = acquired.binary_search(&0).is_ok();
                let matches = if relevant {
                    restored == [0]
                } else {
                    restored.is_empty()
                };
                if !matches {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6051] operator '{}' transition restore roster {restored:?} does not match acquired singleton state",
                        node.name
                    )));
                }
                Ok(relevant)
            }
            OperatorStateClass::VnodeKeyed => {
                if restored != acquired {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6051] operator '{}' transition restore roster {restored:?} does not match acquired vnodes {acquired:?}",
                        node.name
                    )));
                }
                Ok(!acquired.is_empty())
            }
            state_class => Err(DbError::Checkpoint(format!(
                "[LDB-6051] managed operator '{}' has unsupported transition placement {state_class:?}",
                node.name
            ))),
        }
    }

    fn project_transition_frames<'a>(
        &self,
        node_indices: &[usize],
        state_frames: &'a [RecoveredStateFrame],
    ) -> Result<Vec<ProjectedTransitionFrames<'a>>, DbError> {
        let mut operator_positions = FxHashMap::default();
        let mut projected = (0..node_indices.len())
            .map(|_| ProjectedTransitionFrames::default())
            .collect::<Vec<_>>();
        for (position, &node_idx) in node_indices.iter().enumerate() {
            #[cfg(test)]
            {
                let contract = self.nodes[node_idx]
                    .capability
                    .managed_state
                    .expect("managed operator inventory was filtered above");
                if matches!(contract, ManagedStateContract::TestVnodeStateV1) {
                    continue;
                }
            }
            let name = Arc::clone(&self.nodes[node_idx].name);
            if operator_positions
                .insert(Arc::clone(&name), position)
                .is_some()
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] managed operator name '{name}' is not unique"
                )));
            }
        }

        for frame in state_frames {
            let (operator_id, vnode) = match &frame.key {
                StateFrameKey::OperatorWhole { operator_id } => (operator_id, None),
                StateFrameKey::Vnode { operator_id, vnode } => {
                    (operator_id, Some(u32::from(*vnode)))
                }
            };
            let name = operator_id.strip_prefix("graph:").ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] transition frame {:?} is not graph-managed state",
                    frame.key
                ))
            })?;
            let position = operator_positions.get(name).copied().ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] transition frame {:?} has no managed graph operator",
                    frame.key
                ))
            })?;
            if let Some(vnode) = vnode {
                projected[position].restores.push(ManagedVnodeRestore {
                    participant_id: frame.participant_id,
                    vnode,
                    state: frame.payload.as_ref(),
                });
            } else {
                projected[position]
                    .whole_restores
                    .push(ManagedWholeRestore {
                        participant_id: frame.participant_id,
                        state: frame.payload.as_ref(),
                    });
            }
        }
        for frames in &mut projected {
            frames
                .restores
                .sort_unstable_by_key(|restore| restore.vnode);
        }
        Ok(projected)
    }

    fn transition_payload_bytes(&self, frames: &[RecoveredStateFrame]) -> Result<usize, DbError> {
        frames.iter().try_fold(0usize, |total, frame| {
            total.checked_add(frame.payload.len()).ok_or_else(|| {
                DbError::ManagedStateBudgetExceeded {
                    context: "vnode transition staged payload".into(),
                    accounted_bytes: usize::MAX,
                    limit_bytes: self.max_managed_state_bytes,
                }
            })
        })
    }

    fn validate_transition_state_budget(
        &self,
        payload_bytes: usize,
        context: impl Into<String>,
    ) -> Result<(), DbError> {
        let accounted_bytes = self
            .managed_state_accounted_bytes()
            .saturating_add(payload_bytes);
        if accounted_bytes > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: context.into(),
                accounted_bytes,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(())
    }

    fn canonical_managed_operator_indices(&self) -> Vec<usize> {
        let mut indices: Vec<usize> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| !node.removed && node.capability.managed_state.is_some())
            .map(|(node_idx, _)| node_idx)
            .collect();
        indices.sort_unstable_by(|left, right| {
            self.nodes[*left]
                .name
                .cmp(&self.nodes[*right].name)
                .then_with(|| left.cmp(right))
        });
        indices
    }

    fn prepare_managed_operators(
        &mut self,
        predecessor: &laminar_core::checkpoint::CheckpointAssignmentFence,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
        revoked: &FxHashSet<u32>,
        acquired: &[u32],
        state_frames: &[RecoveredStateFrame],
        mode: ManagedVnodeTransitionMode<'_>,
    ) -> Result<PreparedManagedOperators, DbError> {
        let payload_bytes = self.transition_payload_bytes(state_frames)?;
        self.validate_transition_state_budget(payload_bytes, "vnode transition staged payload")?;
        let node_indices = self.canonical_managed_operator_indices();
        let projected = self.project_transition_frames(&node_indices, state_frames)?;
        let mut attempted = Vec::new();
        for (node_idx, frames) in node_indices.into_iter().zip(projected) {
            let contract = self.nodes[node_idx]
                .capability
                .managed_state
                .expect("managed operator inventory was filtered above");
            match contract {
                ManagedStateContract::SqlAggregateV1
                | ManagedStateContract::CoreWindowV1
                | ManagedStateContract::BoundedIntervalJoinV1
                | ManagedStateContract::TemporalJoinV1 => {}
                #[cfg(test)]
                ManagedStateContract::TestVnodeStateV1 => continue,
            }
            let relevant_revoked = match self.relevant_revoked_vnodes(node_idx, revoked) {
                Ok(relevant) => relevant,
                Err(error) => {
                    let prepared = PreparedManagedOperators {
                        node_indices: attempted,
                    };
                    self.abort_and_finish_managed_operators(&prepared);
                    return Err(error);
                }
            };
            let restored_vnodes = frames
                .restores
                .iter()
                .map(|restore| restore.vnode)
                .collect::<Vec<_>>();
            let relevant_acquired = match self.validate_relevant_acquired_vnodes(
                node_idx,
                acquired,
                &restored_vnodes,
            ) {
                Ok(relevant) => relevant,
                Err(error) => {
                    let prepared = PreparedManagedOperators {
                        node_indices: attempted,
                    };
                    self.abort_and_finish_managed_operators(&prepared);
                    return Err(error);
                }
            };
            if !frames.whole_restores.is_empty()
                && !matches!(
                    contract,
                    ManagedStateContract::BoundedIntervalJoinV1
                        | ManagedStateContract::CoreWindowV1
                        | ManagedStateContract::TemporalJoinV1
                )
            {
                let name = Arc::clone(&self.nodes[node_idx].name);
                let prepared = PreparedManagedOperators {
                    node_indices: attempted,
                };
                self.abort_and_finish_managed_operators(&prepared);
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] operator '{name}' does not accept portable whole transition state"
                )));
            }
            let relevant = !relevant_revoked.is_empty()
                || relevant_acquired
                || matches!(
                    contract,
                    ManagedStateContract::SqlAggregateV1
                        | ManagedStateContract::BoundedIntervalJoinV1
                        | ManagedStateContract::CoreWindowV1
                        | ManagedStateContract::TemporalJoinV1
                );
            if !relevant {
                continue;
            }
            attempted.push(node_idx);
            let input = ManagedVnodeTransition {
                predecessor,
                target,
                revoked: &relevant_revoked,
                restores: &frames.restores,
                whole_restores: &frames.whole_restores,
                mode,
            };
            if let Err(error) = self.nodes[node_idx]
                .operator
                .prepare_vnode_transition(input)
            {
                let name = Arc::clone(&self.nodes[node_idx].name);
                let prepared = PreparedManagedOperators {
                    node_indices: attempted,
                };
                self.abort_and_finish_managed_operators(&prepared);
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode transition preparation for operator '{name}' failed: {error}"
                )));
            }
            if let Err(error) =
                self.validate_transition_state_budget(payload_bytes, "vnode transition preparation")
            {
                let prepared = PreparedManagedOperators {
                    node_indices: attempted,
                };
                self.abort_and_finish_managed_operators(&prepared);
                return Err(error);
            }
        }
        let prepared = PreparedManagedOperators {
            node_indices: attempted,
        };
        self.observe_managed_state_accounting(&prepared.node_indices);
        Ok(prepared)
    }

    fn abort_managed_operators(&mut self, prepared: &PreparedManagedOperators) {
        for node_idx in prepared.node_indices.iter().rev().copied() {
            self.nodes[node_idx].operator.abort_vnode_transition();
        }
    }

    fn abort_and_finish_managed_operators(&mut self, prepared: &PreparedManagedOperators) {
        self.observe_managed_state_accounting(&prepared.node_indices);
        self.abort_managed_operators(prepared);
        self.finish_managed_operators(prepared);
    }

    fn publish_managed_operators(&mut self, prepared: &PreparedManagedOperators) {
        for node_idx in prepared.node_indices.iter().copied() {
            let frontier = {
                let node = &mut self.nodes[node_idx];
                node.operator.publish_vnode_transition();
                node.operator.restored_output_frontier()
            };
            if let Some(frontier) = frontier {
                self.output_watermarks[node_idx] = frontier.watermark_or_min();
                self.output_idle[node_idx] = frontier.idle;
            }
        }
    }

    fn finish_managed_operators(&mut self, prepared: &PreparedManagedOperators) {
        for node_idx in prepared.node_indices.iter().copied() {
            self.nodes[node_idx].operator.finish_vnode_transition();
        }
    }

    #[cfg(test)]
    fn run_test_vnode_revoke_callbacks(
        &mut self,
        installed_state: &InstalledVnodeStateHandle,
        pending_handle: &PendingVnodeTransitionHandle,
        pending: &Arc<PendingVnodeTransition>,
        revoked: &FxHashSet<u32>,
    ) -> Result<bool, DbError> {
        let mut callbacks: Vec<(usize, FxHashSet<u32>)> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                !node.removed
                    && node.capability.managed_state == Some(ManagedStateContract::TestVnodeStateV1)
            })
            .map(|(index, _)| {
                self.relevant_revoked_vnodes(index, revoked)
                    .map(|relevant| (index, relevant))
            })
            .collect::<Result<_, DbError>>()?;
        callbacks.retain(|(_, relevant)| !relevant.is_empty());
        callbacks.sort_unstable_by(|(left, _), (right, _)| {
            self.nodes[*left]
                .name
                .cmp(&self.nodes[*right].name)
                .then_with(|| left.cmp(right))
        });
        let callbacks_run = !callbacks.is_empty();
        for (node_idx, relevant) in callbacks {
            let node = &mut self.nodes[node_idx];
            if let Err(error) = node.operator.drop_owned_vnodes(&relevant) {
                let phase = format!("revoke callback for operator '{}'", node.name);
                return Err(callback_error(
                    &self.execution_poisoned,
                    installed_state,
                    pending_handle,
                    pending,
                    &phase,
                    error,
                ));
            }
        }
        Ok(callbacks_run)
    }

    fn abort_prepublication(
        &mut self,
        prepared: &PreparedManagedOperators,
        installed_state: &InstalledVnodeStateHandle,
        pending_handle: &PendingVnodeTransitionHandle,
        pending: &Arc<PendingVnodeTransition>,
        callbacks_mutated_state: bool,
        error: DbError,
    ) -> DbError {
        self.abort_and_finish_managed_operators(prepared);
        if callbacks_mutated_state {
            publish_cluster_execution_poison(
                &self.execution_poisoned,
                Some(installed_state),
                Some((pending_handle, pending)),
            );
            DbError::StatefulOperatorPartialApply(format!(
                "[LDB-6051] test vnode callback mutated live state before publication failed: {error}"
            ))
        } else {
            error
        }
    }

    fn publish_vnode_transition(
        &mut self,
        transition: PreparedVnodeTransition,
        prepared: &PreparedManagedOperators,
        callbacks_mutated_state: bool,
    ) -> Result<(), DbError> {
        let PreparedVnodeTransition {
            authority,
            pending_handle,
            pending,
            revoked,
            installed_state,
            installed_binding,
        } = transition;
        let mut publication = VnodeTransitionUnwindGuard::disarmed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&installed_state),
            Arc::clone(&pending_handle),
            Arc::clone(&pending),
        );
        let mut pending_slot =
            match exact_pending_slot(&pending_handle, &pending, "vnode lifecycle callbacks") {
                Ok(slot) => slot,
                Err(error) => {
                    return Err(self.abort_prepublication(
                        prepared,
                        &installed_state,
                        &pending_handle,
                        &pending,
                        callbacks_mutated_state,
                        error,
                    ));
                }
            };
        let mut installed = installed_state.lock();
        let validation = if Self::installed_state_matches_pending(&pending, installed.as_ref()) {
            authority.revalidate_for_publication()
        } else {
            Err(DbError::Checkpoint(
                "[LDB-6051] installed predecessor binding changed before vnode publication".into(),
            ))
        };
        if let Err(error) = validation {
            drop(installed);
            drop(pending_slot);
            return Err(self.abort_prepublication(
                prepared,
                &installed_state,
                &pending_handle,
                &pending,
                callbacks_mutated_state,
                error,
            ));
        }

        publication.arm();
        self.publish_managed_operators(prepared);
        let retired = std::mem::replace(&mut *installed, installed_binding);
        *pending_slot = None;
        drop(installed);
        drop(pending_slot);
        self.observe_managed_state_accounting(&prepared.node_indices);
        self.finish_managed_operators(prepared);
        drop(retired);
        publication.complete();
        tracing::info!(
            assignment_version = authority.assignment.version(),
            revoked_vnodes = revoked.len(),
            acquired_vnodes = pending.acquired_vnodes().len(),
            "completed staged vnode transition"
        );
        Ok(())
    }

    fn publish_final_owner_exit(
        &mut self,
        transition: &PreparedFinalOwnerExit,
        prepared: &PreparedManagedOperators,
        callbacks_mutated_state: bool,
    ) -> Result<(), DbError> {
        let mut publication = VnodeTransitionUnwindGuard::disarmed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&transition.installed_state),
            Arc::clone(&transition.pending_handle),
            Arc::clone(&transition.pending),
        );
        let mut pending_slot = match exact_pending_slot(
            &transition.pending_handle,
            &transition.pending,
            "final-owner-exit callbacks",
        ) {
            Ok(slot) => slot,
            Err(error) => {
                return Err(self.abort_prepublication(
                    prepared,
                    &transition.installed_state,
                    &transition.pending_handle,
                    &transition.pending,
                    callbacks_mutated_state,
                    error,
                ));
            }
        };
        let mut installed = transition.installed_state.lock();
        let validation = if installed.as_ref().is_some_and(|binding| {
            binding.matches(
                transition.pending.predecessor(),
                transition.pending.pipeline_identity(),
            )
        }) {
            transition.authority.revalidate_for_publication()
        } else {
            Err(DbError::Checkpoint(
                "[LDB-6051] installed predecessor binding changed before final-owner publication"
                    .into(),
            ))
        };
        if let Err(error) = validation {
            drop(installed);
            drop(pending_slot);
            return Err(self.abort_prepublication(
                prepared,
                &transition.installed_state,
                &transition.pending_handle,
                &transition.pending,
                callbacks_mutated_state,
                error,
            ));
        }

        publication.arm();
        self.publish_managed_operators(prepared);
        let retired = installed.take();
        *pending_slot = None;
        drop(installed);
        drop(pending_slot);
        self.observe_managed_state_accounting(&prepared.node_indices);
        self.finish_managed_operators(prepared);
        drop(retired);
        publication.complete();
        tracing::info!(
            assignment_version = transition.authority.assignment.version(),
            revoked_vnodes = transition.revoked.len(),
            "completed committed final-owner-exit vnode cleanup"
        );
        Ok(())
    }
}
