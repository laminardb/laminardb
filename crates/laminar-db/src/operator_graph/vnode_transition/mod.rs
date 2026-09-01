//! Managed vnode state transitions at assignment boundaries.

mod authority;
mod preparation;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use rustc_hash::FxHashSet;

use authority::{FinalOwnerExitAuthoritySnapshot, VnodeTransitionAuthoritySnapshot};

use super::{
    execution_poison::publish_cluster_execution_poison, ManagedVnodeRestore,
    ManagedVnodeTransition, ManagedVnodeTransitionMode, ManagedWholeRestore, OperatorGraph,
};
use crate::error::DbError;
use crate::operator::capability::ManagedStateContract;
use crate::recovery_manager::RecoveredStateFrame;
use crate::vnode_transition_staging::{
    validate_recovered_transition_frames, InstalledVnodeStateBinding, InstalledVnodeStateHandle,
    PendingVnodeTransition, PendingVnodeTransitionHandle, VnodeTransitionKind,
};

#[derive(Default)]
struct ProjectedTransitionFrames<'a> {
    restores: Vec<ManagedVnodeRestore<'a>>,
    whole_restores: Vec<ManagedWholeRestore<'a>>,
}

fn accepts_portable_whole_state(contract: ManagedStateContract) -> bool {
    matches!(
        contract,
        ManagedStateContract::SqlAggregateV1
            | ManagedStateContract::BoundedIntervalJoinV3
            | ManagedStateContract::CoreWindowV1
            | ManagedStateContract::TemporalJoinV1
    )
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
        if predecessor.assignment_version >= target.assignment_version
            || predecessor.vnode_count != u32::from(self.key_group_count)
            || target.vnode_count != predecessor.vnode_count
            || target.partitioning_abi_version != predecessor.partitioning_abi_version
            || !predecessor.is_canonical()
            || !predecessor.matches_owner_map(&predecessor_owner_ids)
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] reassigned checkpoint restore is not a compatible older committed cut"
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

#[cfg(test)]
mod portable_whole_tests {
    use super::*;

    #[test]
    fn sql_aggregate_accepts_portable_whole_transition_state() {
        assert!(accepts_portable_whole_state(
            ManagedStateContract::SqlAggregateV1
        ));
    }
}
