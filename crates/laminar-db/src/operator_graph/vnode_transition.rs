//! Managed vnode revocation at an assignment boundary.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use rustc_hash::FxHashSet;

use super::{publish_cluster_execution_poison, ManagedVnodeTransition, OperatorGraph};
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::operator::sql_query::ClusterShuffleConfig;
use crate::vnode_transition_staging::{
    InstalledVnodeStateBinding, InstalledVnodeStateHandle, PendingVnodeTransition,
    PendingVnodeTransitionHandle, VnodeTransitionKind,
};

struct VnodeTransitionAuthoritySnapshot {
    registry: Arc<laminar_core::state::VnodeRegistry>,
    self_id: laminar_core::state::NodeId,
    assignment: laminar_core::state::VnodeAssignmentSnapshot,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    transport_digest: [u8; 32],
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
            || target.participant_incarnation(config.self_id.0) != Some(config.sender.incarnation())
            || config.sender.incarnation() != config.receiver.incarnation()
            || config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode transition target/process does not match assignment {}",
                assignment.version()
            )));
        }
        let transport_digest = match (
            config.sender.active_assignment_digest(),
            config.receiver.active_assignment_digest(),
        ) {
            (Some(sender), Some(receiver)) if sender == receiver && sender == target.digest() => {
                sender
            }
            _ => {
                return Err(DbError::ShuffleNotReady(
                    "[LDB-6051] vnode transition requires matching active sender and receiver assignment certificates"
                        .into(),
                ));
            }
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
        if self.sender.assignment_version() != self.assignment.version()
            || self.receiver.assignment_version() != self.assignment.version()
            || self.sender.active_assignment_digest() != Some(self.transport_digest)
            || self.receiver.active_assignment_digest() != Some(self.transport_digest)
        {
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
    installed_binding: InstalledVnodeStateBinding,
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
        let prepared = match self
            .prepare_managed_operators(transition.pending.target(), &transition.revoked)
        {
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

    pub(super) fn apply_committed_final_owner_exit(&mut self) -> Result<(), DbError> {
        let transition = self.prepare_final_owner_exit()?;
        let mut unwind = VnodeTransitionUnwindGuard::armed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&transition.installed_state),
            Arc::clone(&transition.pending_handle),
            Arc::clone(&transition.pending),
        );
        let prepared = match self
            .prepare_managed_operators(transition.pending.target(), &transition.revoked)
        {
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
        if !installed_state.lock().as_ref().is_some_and(|installed| {
            installed.matches(pending.predecessor(), pending.pipeline_identity())
        }) {
            return Err(DbError::Checkpoint(
                "[LDB-6051] graph does not have the exact predecessor assignment state installed"
                    .into(),
            ));
        }
        Ok(())
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
            VnodeTransitionKind::Revoke => {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] revoke transition has no committed final-owner-exit authority"
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
        if !matches!(pending.kind(), VnodeTransitionKind::Revoke) {
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
        if revoked.is_empty() {
            return Err(DbError::Checkpoint(
                "[LDB-6051] pending vnode transition contains no graph work".into(),
            ));
        }
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
        let installed_state = self.installed_state_handle()?;
        Self::validate_installed_predecessor(&pending, &installed_state)?;
        let installed_binding = InstalledVnodeStateBinding::new(
            pending.target().clone(),
            pending.pipeline_identity().clone(),
        )?;
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
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
        revoked: &FxHashSet<u32>,
    ) -> Result<PreparedManagedOperators, DbError> {
        let mut attempted = Vec::new();
        for node_idx in self.canonical_managed_operator_indices() {
            let contract = self.nodes[node_idx]
                .capability
                .managed_state
                .expect("managed operator inventory was filtered above");
            match contract {
                ManagedStateContract::SqlAggregateV1
                | ManagedStateContract::CoreWindowV1
                | ManagedStateContract::BoundedIntervalJoinV1 => {}
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
            if relevant_revoked.is_empty() {
                continue;
            }
            attempted.push(node_idx);
            let input = ManagedVnodeTransition {
                target,
                revoked: &relevant_revoked,
                restores: &[],
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
                    "[LDB-6051] vnode revocation preparation for operator '{name}' failed: {error}"
                )));
            }
        }
        let prepared = PreparedManagedOperators {
            node_indices: attempted,
        };
        self.observe_managed_state_accounting(&prepared.node_indices);
        if let Err(error) = self.validate_managed_state_budget("vnode revocation preparation") {
            self.abort_and_finish_managed_operators(&prepared);
            return Err(error);
        }
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
            self.nodes[node_idx].operator.publish_vnode_transition();
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
        let validation = if installed.as_ref().is_some_and(|binding| {
            binding.matches(pending.predecessor(), pending.pipeline_identity())
        }) {
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
        let retired = installed.replace(installed_binding);
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
            "completed staged vnode revocation"
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
