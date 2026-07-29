//! Managed vnode revoke/restore transition phases for [`OperatorGraph`].

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use rustc_hash::FxHashSet;

use super::{
    publish_cluster_execution_poison, ManagedVnodeRestore, ManagedVnodeTransition, OperatorGraph,
};
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::operator::sql_query::ClusterShuffleConfig;
use crate::vnode_partial::VnodePartial;
use crate::vnode_transition_staging::{
    InstalledVnodeStateBinding, InstalledVnodeStateHandle, PendingVnodeRestore,
    PendingVnodeTransition, PendingVnodeTransitionHandle, VnodeTransitionKind,
    VnodeTransitionOrigin,
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
                    "[LDB-6051] vnode transition requires matching active sender and receiver \
                     assignment certificates"
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

    fn restoring_vnodes_by_ownership(&self) -> (Vec<u32>, Vec<u32>) {
        self.registry
            .restoring_vnodes()
            .into_iter()
            .partition(|vnode| self.owns(*vnode))
    }

    fn revalidate_for_publication(&self, expected_vnodes: &[u32]) -> Result<(), DbError> {
        if self.sender.assignment_version() != self.assignment.version()
            || self.receiver.assignment_version() != self.assignment.version()
            || self.sender.active_assignment_digest() != Some(self.transport_digest)
            || self.receiver.active_assignment_digest() != Some(self.transport_digest)
        {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] shuffle assignment certificate changed before vnode-state \
                 publication"
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
        let current_expected = self.registry.restoring_vnodes();
        if current_expected != expected_vnodes {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] restoring vnode roster changed from {expected_vnodes:?} to \
                 {current_expected:?} before vnode-state publication"
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
                "[LDB-6051] final-owner-exit predecessor process identity does not match the \
                 local shuffle endpoints"
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
                "[LDB-6051] final-owner-exit revoke roster is empty, out of range, or still \
                 target-owned"
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
                "[LDB-6051] final-owner-exit target acquired local ownership before publication"
                    .into(),
            ));
        }
        if self.registry.any_restoring() {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] vnode restore lifecycle appeared before final-owner-exit publication"
                    .into(),
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
    expected_vnodes: Vec<u32>,
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

struct DecodedVnode {
    vnode: u32,
    attempt: laminar_core::state::CheckpointAttempt,
    chain: Vec<VnodePartial>,
    operators: Vec<(usize, String)>,
}

struct ResolvedOperator<'a> {
    node_idx: usize,
    #[cfg(test)]
    name: &'a str,
    base: &'a [u8],
    deltas: Vec<&'a [u8]>,
}

struct ResolvedVnode<'a> {
    vnode: u32,
    attempt: laminar_core::state::CheckpointAttempt,
    links: usize,
    operators: Vec<ResolvedOperator<'a>>,
}

struct PreparedManagedOperators {
    node_indices: Vec<usize>,
}

/// Scope panic cleanup to the rare transition path instead of retaining checkpoint bodies or
/// locking the transition slot for every steady-state graph cycle. Callers disarm ordinary
/// validation/authority errors; publication arms only after every fallible check passes.
struct VnodeTransitionUnwindGuard {
    poisoned: Arc<AtomicBool>,
    installed_state: InstalledVnodeStateHandle,
    pending_handle: PendingVnodeTransitionHandle,
    pending: Arc<PendingVnodeTransition>,
    armed: bool,
}

impl VnodeTransitionUnwindGuard {
    fn new(
        poisoned: Arc<AtomicBool>,
        installed_state: InstalledVnodeStateHandle,
        pending_handle: PendingVnodeTransitionHandle,
        pending: Arc<PendingVnodeTransition>,
        armed: bool,
    ) -> Self {
        Self {
            poisoned,
            installed_state,
            pending_handle,
            pending,
            armed,
        }
    }

    fn armed(
        poisoned: Arc<AtomicBool>,
        installed_state: InstalledVnodeStateHandle,
        pending_handle: PendingVnodeTransitionHandle,
        pending: Arc<PendingVnodeTransition>,
    ) -> Self {
        Self::new(poisoned, installed_state, pending_handle, pending, true)
    }

    fn disarmed(
        poisoned: Arc<AtomicBool>,
        installed_state: InstalledVnodeStateHandle,
        pending_handle: PendingVnodeTransitionHandle,
        pending: Arc<PendingVnodeTransition>,
    ) -> Self {
        Self::new(poisoned, installed_state, pending_handle, pending, false)
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

    /// Complete one caller-staged revoke/rehydration batch at the top of a graph cycle.
    ///
    /// Structural validation covers the exact owned/restoring vnode roster and every operator
    /// chain before any live state changes. Production managed operators prepare complete private
    /// replacements first. Any preparation or authority failure aborts those replacements without
    /// poisoning this graph generation. Publication is synchronous and infallible; only an unwind
    /// after publication begins has an indeterminate outcome.
    pub(super) fn apply_pending_vnode_transition(&mut self) -> Result<(), DbError> {
        let Some(transition) = self.prepare_pending_vnode_transition()? else {
            return Ok(());
        };
        let mut transition_unwind = VnodeTransitionUnwindGuard::armed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&transition.installed_state),
            Arc::clone(&transition.pending_handle),
            Arc::clone(&transition.pending),
        );
        let restore_cut = transition.pending.restore_cut();
        let attempt = restore_cut
            .map(crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut::attempt);
        let restore_profile = restore_cut.map(|cut| cut.restore_head().contract().limits.profile);
        let decoded = match self.preflight_decoded_vnodes(
            transition.pending.restores(),
            attempt,
            restore_profile,
        ) {
            Ok(decoded) => decoded,
            Err(error) => {
                transition_unwind.complete();
                return Err(error);
            }
        };
        let resolved = match self.resolve_decoded_vnodes(&decoded) {
            Ok(resolved) => resolved,
            Err(error) => {
                transition_unwind.complete();
                return Err(error);
            }
        };
        let prepared = match self.prepare_managed_operators(
            transition.pending.target(),
            &transition.revoked,
            &resolved,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                transition_unwind.complete();
                return Err(error);
            }
        };
        #[cfg(test)]
        let test_callbacks_mutated_state = match self.run_test_vnode_transition_callbacks(
            &transition.installed_state,
            &transition.pending_handle,
            &transition.pending,
            &transition.revoked,
            &resolved,
        ) {
            Ok(callbacks_run) => callbacks_run,
            Err(error) => {
                self.abort_and_finish_managed_operators(&prepared);
                transition_unwind.complete();
                return Err(error);
            }
        };
        #[cfg(not(test))]
        let test_callbacks_mutated_state = false;
        transition_unwind.complete();
        self.publish_vnode_transition(
            transition,
            &resolved,
            &prepared,
            test_callbacks_mutated_state,
        )
    }

    pub(super) fn apply_committed_final_owner_exit(&mut self) -> Result<(), DbError> {
        let transition = self.prepare_final_owner_exit()?;
        let mut transition_unwind = VnodeTransitionUnwindGuard::armed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&transition.installed_state),
            Arc::clone(&transition.pending_handle),
            Arc::clone(&transition.pending),
        );
        let prepared = match self.prepare_managed_operators(
            transition.pending.target(),
            &transition.revoked,
            &[],
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                transition_unwind.complete();
                return Err(error);
            }
        };
        #[cfg(test)]
        let test_callbacks_mutated_state = match self.run_test_vnode_transition_callbacks(
            &transition.installed_state,
            &transition.pending_handle,
            &transition.pending,
            &transition.revoked,
            &[],
        ) {
            Ok(callbacks_run) => callbacks_run,
            Err(error) => {
                self.abort_and_finish_managed_operators(&prepared);
                transition_unwind.complete();
                return Err(error);
            }
        };
        #[cfg(not(test))]
        let test_callbacks_mutated_state = false;
        transition_unwind.complete();
        self.publish_final_owner_exit(&transition, &prepared, test_callbacks_mutated_state)
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

    fn prepare_final_owner_exit(&self) -> Result<PreparedFinalOwnerExit, DbError> {
        let (pending_handle, pending) = self.pending_transition_snapshot().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup has no pending transition".into(),
            )
        })?;
        self.validate_pending_pipeline_identity(&pending)?;
        let transition = match pending.kind() {
            VnodeTransitionKind::CommittedFinalOwnerExit(transition) => transition.clone(),
            VnodeTransitionKind::OwnershipChange => {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] control-only vnode transition has no committed final-owner-exit authority"
                        .into(),
                ));
            }
        };
        match pending.origin() {
            VnodeTransitionOrigin::AssignmentChange { predecessor }
                if *predecessor == transition.predecessor => {}
            _ => {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] final-owner-exit transition has invalid predecessor authority"
                        .into(),
                ));
            }
        }
        if !pending.acquired_vnodes().is_empty()
            || !pending.restores().is_empty()
            || pending.restore_cut().is_some()
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup cannot include vnode restore state".into(),
            ));
        }
        let revoked: FxHashSet<u32> = pending.revoked_vnodes().iter().copied().collect();
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup has no cluster ownership scope".into(),
            )
        })?;
        if config.registry.any_restoring() {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup cannot include restoring vnodes".into(),
            ));
        }
        let installed_state = self
            .installed_vnode_state
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] final-owner-exit cleanup has no installed-state publication handle"
                        .into(),
                )
            })?;
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
        let pending_snapshot = self.pending_transition_snapshot();
        if pending_snapshot.is_none() {
            if self
                .cluster_shuffle
                .as_ref()
                .is_some_and(|config| config.registry.any_restoring())
            {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] vnode lifecycle has restoring slots but no pending transition"
                        .into(),
                ));
            }
            return Ok(None);
        }
        let (pending_handle, pending) = pending_snapshot.expect("checked above");
        if matches!(
            pending.kind(),
            VnodeTransitionKind::CommittedFinalOwnerExit(_)
        ) {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] committed final-owner-exit cleanup requires the fenced control-only execution path"
                    .into(),
            ));
        }
        self.validate_pending_pipeline_identity(&pending)?;
        match pending.origin() {
            VnodeTransitionOrigin::BootRecovery => {
                if pending.restore_cut().is_none() || !pending.revoked_vnodes().is_empty() {
                    return Err(DbError::Checkpoint(
                        "[LDB-6051] boot recovery transition has invalid restore/revoke authority"
                            .into(),
                    ));
                }
            }
            VnodeTransitionOrigin::AssignmentChange { predecessor }
                if predecessor.assignment_version.checked_add(1)
                    == Some(pending.target().assignment_version) => {}
            VnodeTransitionOrigin::AssignmentChange { predecessor } => {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] pending assignment transition is not adjacent: {} -> {}",
                    predecessor.assignment_version,
                    pending.target().assignment_version
                )));
            }
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] pending vnode transition has no cluster ownership scope".into(),
            )
        })?;
        let authority = VnodeTransitionAuthoritySnapshot::capture(config, pending.target())?;
        let (expected_vnodes, unowned_restoring_vnodes) = authority.restoring_vnodes_by_ownership();
        if !unowned_restoring_vnodes.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] unowned vnodes remain in restoring state: {unowned_restoring_vnodes:?}"
            )));
        }
        if pending.acquired_vnodes() != expected_vnodes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] pending acquired roster {:?} does not match the exact owned/restoring roster {expected_vnodes:?}",
                pending.acquired_vnodes()
            )));
        }
        let revoked: FxHashSet<u32> = pending.revoked_vnodes().iter().copied().collect();
        let mut invalid_revokes: Vec<u32> = revoked
            .iter()
            .copied()
            .filter(|vnode| !authority.contains_vnode(*vnode))
            .collect();
        invalid_revokes.sort_unstable();
        if !invalid_revokes.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] revoked vnode ids {invalid_revokes:?} are outside the target assignment cardinality"
            )));
        }
        let mut owned_revokes_without_restore: Vec<u32> = revoked
            .iter()
            .copied()
            .filter(|vnode| {
                authority.owns(*vnode) && pending.acquired_vnodes().binary_search(vnode).is_err()
            })
            .collect();
        owned_revokes_without_restore.sort_unstable();
        if !owned_revokes_without_restore.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] target-owned revoked vnodes {owned_revokes_without_restore:?} have no matching restore"
            )));
        }
        if revoked.is_empty() && pending.restores().is_empty() {
            return Err(DbError::Checkpoint(
                "[LDB-6051] pending vnode transition contains no graph work".into(),
            ));
        }
        let installed_state = self
            .installed_vnode_state
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] pending vnode transition has no installed-state publication handle"
                        .into(),
                )
            })?;
        let installed_binding = InstalledVnodeStateBinding::new(
            pending.target().clone(),
            pending.pipeline_identity().clone(),
        )?;
        Ok(Some(PreparedVnodeTransition {
            authority,
            pending_handle,
            pending,
            revoked,
            expected_vnodes,
            installed_state,
            installed_binding,
        }))
    }

    fn preflight_decoded_vnodes(
        &self,
        staged: &[PendingVnodeRestore],
        attempt: Option<laminar_core::state::CheckpointAttempt>,
        restore_profile: Option<laminar_core::checkpoint::VnodeRestoreLimitProfile>,
    ) -> Result<Vec<DecodedVnode>, DbError> {
        // Decode and bind every participant before the first revoke or restore callback.
        let mut decoded = Vec::with_capacity(staged.len());
        for rehydrated in staged {
            let vnode = rehydrated.vnode();
            let Some(attempt) = attempt else {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] pending vnode restore has no exact committed attempt".into(),
                ));
            };
            let Some(restore_profile) = restore_profile else {
                return Err(DbError::Checkpoint(
                    "[LDB-6051] pending vnode restore has no committed restore profile".into(),
                ));
            };
            if rehydrated.chain().is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} rehydration chain has no links"
                )));
            }
            let expected = self.managed_vnode_participants(vnode)?;
            let chain: Vec<VnodePartial> = rehydrated
                .chain()
                .iter()
                .enumerate()
                .map(|(link, bytes)| {
                    #[cfg(test)]
                    let decoded = if expected.len() > 1 {
                        VnodePartial::decode_for_restore_test_roster(
                            bytes,
                            restore_profile,
                            expected.len(),
                        )
                    } else {
                        VnodePartial::decode_for_restore(bytes, restore_profile)
                    };
                    #[cfg(not(test))]
                    let decoded = VnodePartial::decode_for_restore(bytes, restore_profile);
                    decoded.map_err(|error| {
                        DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration chain link {link} is corrupt: \
                             {error}"
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            let mut artifact_names = BTreeSet::new();
            for (link, partial) in chain.iter().enumerate() {
                let mut link_names = BTreeSet::new();
                for name in partial
                    .operators
                    .iter()
                    .map(|(name, _)| name)
                    .chain(partial.deltas.iter().map(|(name, _)| name))
                {
                    if name.is_empty() {
                        return Err(DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration chain link {link} has an empty \
                             operator name"
                        )));
                    }
                    if !link_names.insert(name.as_str()) {
                        return Err(DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration chain link {link} repeats \
                             operator '{name}'"
                        )));
                    }
                    artifact_names.insert(name.clone());
                }
            }

            let expected_names: BTreeSet<&str> = expected.iter().map(|(_, name)| *name).collect();
            let missing: Vec<&str> = expected_names
                .iter()
                .copied()
                .filter(|name| !artifact_names.contains(*name))
                .collect();
            let unexpected: Vec<&str> = artifact_names
                .iter()
                .map(String::as_str)
                .filter(|name| !expected_names.contains(name))
                .collect();
            if !missing.is_empty() || !unexpected.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} managed-state roster mismatch: missing={missing:?}, unexpected={unexpected:?}"
                )));
            }
            let operators = expected
                .into_iter()
                .map(|(node_idx, name)| (node_idx, name.to_string()))
                .collect();
            decoded.push(DecodedVnode {
                vnode,
                attempt,
                chain,
                operators,
            });
        }
        Ok(decoded)
    }

    fn resolve_decoded_vnodes<'decoded>(
        &self,
        decoded: &'decoded [DecodedVnode],
    ) -> Result<Vec<ResolvedVnode<'decoded>>, DbError> {
        decoded
            .iter()
            .map(|vnode| {
                let operators = vnode
                    .operators
                    .iter()
                    .map(|(node_idx, name)| {
                        let (base, deltas) =
                            crate::recovery_manager::vnode_chains::resolve_op_chain(
                                &vnode.chain,
                                name,
                            )
                            .ok_or_else(|| {
                                DbError::Checkpoint(format!(
                                    "[LDB-6051] vnode {} rehydration chain has no FULL base \
                                         for operator '{name}'",
                                    vnode.vnode
                                ))
                            })?;
                        Ok(ResolvedOperator {
                            node_idx: *node_idx,
                            #[cfg(test)]
                            name,
                            base,
                            deltas,
                        })
                    })
                    .collect::<Result<Vec<_>, DbError>>()?;
                Ok(ResolvedVnode {
                    vnode: vnode.vnode,
                    attempt: vnode.attempt,
                    links: vnode.chain.len(),
                    operators,
                })
            })
            .collect()
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
                "[LDB-6051] managed operator '{}' has unsupported revoke placement \
                 {state_class:?}",
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
        resolved: &[ResolvedVnode<'_>],
    ) -> Result<PreparedManagedOperators, DbError> {
        let mut attempted = Vec::new();
        for node_idx in self.canonical_managed_operator_indices() {
            let contract = self.nodes[node_idx]
                .capability
                .managed_state
                .expect("managed operator inventory was filtered above");
            match contract {
                ManagedStateContract::SqlAggregateV1 => {}
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
            let restores: Vec<ManagedVnodeRestore<'_>> = resolved
                .iter()
                .filter_map(|vnode| {
                    vnode
                        .operators
                        .iter()
                        .find(|operator| operator.node_idx == node_idx)
                        .map(|operator| ManagedVnodeRestore {
                            vnode: vnode.vnode,
                            base: operator.base,
                            deltas: &operator.deltas,
                        })
                })
                .collect();
            if relevant_revoked.is_empty() && restores.is_empty() {
                continue;
            }

            // Include the current participant in abort-all even if its prepare method stages a
            // private value and then discovers a later validation error.
            attempted.push(node_idx);
            let input = ManagedVnodeTransition {
                target,
                revoked: &relevant_revoked,
                restores: &restores,
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
                    "[LDB-6051] vnode transition preparation for operator '{name}' failed: \
                     {error}"
                )));
            }
        }
        Ok(PreparedManagedOperators {
            node_indices: attempted,
        })
    }

    fn abort_managed_operators(&mut self, prepared: &PreparedManagedOperators) {
        for node_idx in prepared.node_indices.iter().rev().copied() {
            self.nodes[node_idx].operator.abort_vnode_transition();
        }
    }

    fn abort_and_finish_managed_operators(&mut self, prepared: &PreparedManagedOperators) {
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
    fn run_test_vnode_transition_callbacks(
        &mut self,
        installed_state: &InstalledVnodeStateHandle,
        pending_handle: &PendingVnodeTransitionHandle,
        pending: &Arc<PendingVnodeTransition>,
        revoked: &FxHashSet<u32>,
        resolved: &[ResolvedVnode<'_>],
    ) -> Result<bool, DbError> {
        let mut callbacks_run = false;
        if !revoked.is_empty() {
            let mut revoke_callbacks: Vec<(usize, FxHashSet<u32>)> = self
                .nodes
                .iter()
                .enumerate()
                .filter(|(_, node)| {
                    !node.removed
                        && node.capability.managed_state
                            == Some(ManagedStateContract::TestVnodeStateV1)
                })
                .map(|(index, _)| {
                    self.relevant_revoked_vnodes(index, revoked)
                        .map(|relevant| (index, relevant))
                })
                .collect::<Result<_, DbError>>()?;
            revoke_callbacks.retain(|(_, relevant)| !relevant.is_empty());
            revoke_callbacks.sort_unstable_by(|(left, _), (right, _)| {
                self.nodes[*left]
                    .name
                    .cmp(&self.nodes[*right].name)
                    .then_with(|| left.cmp(right))
            });
            for (node_idx, relevant) in revoke_callbacks {
                callbacks_run = true;
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
        }

        for vnode in resolved {
            for operator in &vnode.operators {
                if self.nodes[operator.node_idx].capability.managed_state
                    != Some(ManagedStateContract::TestVnodeStateV1)
                {
                    continue;
                }
                callbacks_run = true;
                if let Err(error) = self.nodes[operator.node_idx].operator.apply_vnode_chain(
                    vnode.vnode,
                    operator.base,
                    &operator.deltas,
                ) {
                    let phase = format!(
                        "restore callback for vnode {} operator '{}'",
                        vnode.vnode, operator.name
                    );
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
        }
        Ok(callbacks_run)
    }

    fn abort_prepublication(
        &mut self,
        prepared: &PreparedManagedOperators,
        installed_state: &InstalledVnodeStateHandle,
        pending_handle: &PendingVnodeTransitionHandle,
        pending: &Arc<PendingVnodeTransition>,
        test_callbacks_mutated_state: bool,
        error: DbError,
    ) -> DbError {
        self.abort_and_finish_managed_operators(prepared);
        if test_callbacks_mutated_state {
            publish_cluster_execution_poison(
                &self.execution_poisoned,
                Some(installed_state),
                Some((pending_handle, pending)),
            );
            DbError::StatefulOperatorPartialApply(format!(
                "[LDB-6051] test vnode callback mutated live state before publication failed: \
                 {error}"
            ))
        } else {
            error
        }
    }

    fn publish_vnode_transition(
        &mut self,
        transition: PreparedVnodeTransition,
        resolved: &[ResolvedVnode<'_>],
        prepared: &PreparedManagedOperators,
        test_callbacks_mutated_state: bool,
    ) -> Result<(), DbError> {
        let PreparedVnodeTransition {
            authority,
            pending_handle,
            pending,
            revoked,
            expected_vnodes,
            installed_state,
            installed_binding,
        } = transition;
        // Declare the unwind guard before both mutex guards. On a publication panic, Rust drops
        // those guards before exact pending cleanup attempts to lock either handle.
        let mut publication = VnodeTransitionUnwindGuard::disarmed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&installed_state),
            Arc::clone(&pending_handle),
            Arc::clone(&pending),
        );
        let pending_slot =
            exact_pending_slot(&pending_handle, &pending, "vnode lifecycle callbacks");
        let mut pending_slot = match pending_slot {
            Ok(slot) => slot,
            Err(error) => {
                return Err(self.abort_prepublication(
                    prepared,
                    &installed_state,
                    &pending_handle,
                    &pending,
                    test_callbacks_mutated_state,
                    error,
                ));
            }
        };
        let mut installed_state_guard = installed_state.lock();
        if let Err(error) = authority.revalidate_for_publication(&expected_vnodes) {
            drop(installed_state_guard);
            drop(pending_slot);
            return Err(self.abort_prepublication(
                prepared,
                &installed_state,
                &pending_handle,
                &pending,
                test_callbacks_mutated_state,
                error,
            ));
        }

        publication.arm();
        self.publish_managed_operators(prepared);
        authority.registry.mark_active(&expected_vnodes);
        let retired_installed_binding = installed_state_guard.replace(installed_binding);
        *pending_slot = None;
        drop(installed_state_guard);
        drop(pending_slot);
        self.finish_managed_operators(prepared);
        drop(retired_installed_binding);
        publication.complete();

        for vnode in resolved {
            tracing::debug!(
                vnode = vnode.vnode,
                epoch = vnode.attempt.epoch,
                checkpoint_id = vnode.attempt.checkpoint_id,
                operators = vnode.operators.len(),
                links = vnode.links,
                "completed vnode recovery-chain publication"
            );
        }
        tracing::info!(
            assignment_version = authority.assignment.version(),
            revoked_vnodes = revoked.len(),
            restored_vnodes = resolved.len(),
            "completed staged vnode transition"
        );
        Ok(())
    }

    fn publish_final_owner_exit(
        &mut self,
        transition: &PreparedFinalOwnerExit,
        prepared: &PreparedManagedOperators,
        test_callbacks_mutated_state: bool,
    ) -> Result<(), DbError> {
        let mut publication = VnodeTransitionUnwindGuard::disarmed(
            Arc::clone(&self.execution_poisoned),
            Arc::clone(&transition.installed_state),
            Arc::clone(&transition.pending_handle),
            Arc::clone(&transition.pending),
        );
        let pending_slot = exact_pending_slot(
            &transition.pending_handle,
            &transition.pending,
            "final-owner-exit callbacks",
        );
        let mut pending_slot = match pending_slot {
            Ok(slot) => slot,
            Err(error) => {
                return Err(self.abort_prepublication(
                    prepared,
                    &transition.installed_state,
                    &transition.pending_handle,
                    &transition.pending,
                    test_callbacks_mutated_state,
                    error,
                ));
            }
        };
        let mut installed_state = transition.installed_state.lock();
        if let Err(error) = transition.authority.revalidate_for_publication() {
            drop(installed_state);
            drop(pending_slot);
            return Err(self.abort_prepublication(
                prepared,
                &transition.installed_state,
                &transition.pending_handle,
                &transition.pending,
                test_callbacks_mutated_state,
                error,
            ));
        }

        publication.arm();
        self.publish_managed_operators(prepared);
        let retired_installed_binding = installed_state.take();
        *pending_slot = None;
        drop(installed_state);
        drop(pending_slot);
        self.finish_managed_operators(prepared);
        drop(retired_installed_binding);
        publication.complete();
        tracing::info!(
            assignment_version = transition.authority.assignment.version(),
            revoked_vnodes = transition.revoked.len(),
            "completed committed final-owner-exit vnode cleanup"
        );
        Ok(())
    }
}
