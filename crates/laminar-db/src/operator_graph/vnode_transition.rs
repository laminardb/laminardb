//! Managed vnode revoke/restore transition phases for [`OperatorGraph`].

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rustc_hash::FxHashSet;

use super::OperatorGraph;
use crate::db::RehydratedVnode;
use crate::error::DbError;
use crate::operator::sql_query::ClusterShuffleConfig;
use crate::vnode_partial::VnodePartial;

#[allow(clippy::disallowed_types)] // shares the DB's std-HashMap-typed staging handle
type StagedVnodeStateHandle =
    Arc<parking_lot::Mutex<std::collections::HashMap<u32, RehydratedVnode>>>;
type StagedVnodeRevokeHandle = Arc<parking_lot::Mutex<Option<crate::db::StagedVnodeRevocation>>>;

struct VnodeTransitionAuthoritySnapshot {
    registry: Arc<laminar_core::state::VnodeRegistry>,
    self_id: laminar_core::state::NodeId,
    assignment: laminar_core::state::VnodeAssignmentSnapshot,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    transport_digest: [u8; 32],
}

impl VnodeTransitionAuthoritySnapshot {
    fn capture(config: &ClusterShuffleConfig) -> Result<Self, DbError> {
        let assignment = config.registry.versioned_snapshot();
        if config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode transition transport does not match assignment {}",
                assignment.version()
            )));
        }
        let transport_digest = match (
            config.sender.active_assignment_digest(),
            config.receiver.active_assignment_digest(),
        ) {
            (Some(sender), Some(receiver)) if sender == receiver => sender,
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

    fn revalidate_after_callbacks(&self, expected_vnodes: &[u32]) -> Result<(), DbError> {
        if self.sender.assignment_version() != self.assignment.version()
            || self.receiver.assignment_version() != self.assignment.version()
            || self.sender.active_assignment_digest() != Some(self.transport_digest)
            || self.receiver.active_assignment_digest() != Some(self.transport_digest)
        {
            return Err(DbError::StatefulOperatorPartialApply(
                "[LDB-6051] shuffle assignment certificate changed after vnode lifecycle \
                 callbacks; recovery is required"
                    .into(),
            ));
        }
        let current = self.registry.versioned_snapshot();
        if current.version() != self.assignment.version()
            || current.owners() != self.assignment.owners()
        {
            return Err(DbError::StatefulOperatorPartialApply(format!(
                "[LDB-6051] vnode assignment changed from {} after lifecycle callbacks; recovery \
                 is required",
                self.assignment.version()
            )));
        }
        let current_expected: Vec<u32> = self
            .registry
            .restoring_vnodes()
            .into_iter()
            .filter(|vnode| self.owns(*vnode))
            .collect();
        if current_expected != expected_vnodes {
            return Err(DbError::StatefulOperatorPartialApply(format!(
                "[LDB-6051] owned/restoring vnode roster changed from {expected_vnodes:?} to \
                 {current_expected:?} after lifecycle callbacks; recovery is required"
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

    fn revalidate_after_callbacks(&self) -> Result<(), DbError> {
        if self.sender.assignment_version() != 0
            || self.receiver.assignment_version() != 0
            || self.sender.active_assignment_digest().is_some()
            || self.receiver.active_assignment_digest().is_some()
        {
            return Err(DbError::StatefulOperatorPartialApply(
                "[LDB-6051] shuffle authority became active during final-owner-exit callbacks; \
                 recovery is required"
                    .into(),
            ));
        }
        let current = self.registry.versioned_snapshot();
        if current.version() != self.assignment.version()
            || current.owners() != self.assignment.owners()
        {
            return Err(DbError::StatefulOperatorPartialApply(format!(
                "[LDB-6051] vnode assignment changed from {} during final-owner-exit callbacks; \
                 recovery is required",
                self.assignment.version()
            )));
        }
        if current.owners().contains(&self.self_id) {
            return Err(DbError::StatefulOperatorPartialApply(
                "[LDB-6051] final-owner-exit target acquired local ownership during callbacks; \
                 recovery is required"
                    .into(),
            ));
        }
        if self.registry.any_restoring() {
            return Err(DbError::StatefulOperatorPartialApply(
                "[LDB-6051] vnode restore lifecycle appeared during final-owner-exit callbacks; \
                 recovery is required"
                    .into(),
            ));
        }
        Ok(())
    }
}

struct StagedVnodeTransition {
    authority: VnodeTransitionAuthoritySnapshot,
    revoke_handle: Option<StagedVnodeRevokeHandle>,
    rehydration_handle: Option<StagedVnodeStateHandle>,
    revoked: FxHashSet<u32>,
    staged: Vec<(u32, RehydratedVnode)>,
    staged_vnodes: Vec<u32>,
    expected_vnodes: Vec<u32>,
}

struct StagedFinalOwnerExit {
    authority: FinalOwnerExitAuthoritySnapshot,
    revoke_handle: StagedVnodeRevokeHandle,
    rehydration_handle: StagedVnodeStateHandle,
    transition: laminar_core::checkpoint::AssignmentDrainTransition,
    revoked: FxHashSet<u32>,
}

struct DecodedVnode {
    vnode: u32,
    attempt: laminar_core::state::CheckpointAttempt,
    chain: Vec<VnodePartial>,
    operators: Vec<(usize, String)>,
}

struct ResolvedOperator<'a> {
    node_idx: usize,
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

fn callback_error(poisoned: &AtomicBool, phase: &str, error: DbError) -> DbError {
    poisoned.store(true, Ordering::Release);
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

impl OperatorGraph {
    pub(super) fn has_staged_final_owner_exit(&self) -> bool {
        self.staged_vnode_revocation.as_ref().is_some_and(|handle| {
            handle
                .lock()
                .as_ref()
                .is_some_and(|staged| staged.committed_transition().is_some())
        })
    }

    pub(super) fn has_staged_vnode_transition(&self) -> bool {
        // Assignment adoption holds these in the same order while staging both halves of the
        // transition. Taking both here prevents a checkpoint sample from observing an impossible
        // mixed staging point.
        match (
            self.staged_vnode_revocation.as_ref(),
            self.rehydrated_vnode_state.as_ref(),
        ) {
            (Some(revoked), Some(rehydrated)) => {
                let revoked = revoked.lock();
                let rehydrated = rehydrated.lock();
                revoked.is_some() || !rehydrated.is_empty()
            }
            (Some(revoked), None) => revoked.lock().is_some(),
            (None, Some(rehydrated)) => !rehydrated.lock().is_empty(),
            (None, None) => false,
        }
    }

    /// Complete one caller-staged revoke/rehydration batch at the top of a graph cycle.
    ///
    /// Structural validation covers the exact owned/restoring vnode roster and every operator
    /// chain before any callback. Existing callbacks still combine preparation with mutation, so a
    /// callback error has an indeterminate live outcome: the whole graph generation is poisoned,
    /// all owned current-target staging is retained, and no owned target vnode is activated. Fresh
    /// graph recovery must retry the complete batch. Successful callbacks finish before the
    /// acquired-vnode activation phase begins and staging is removed. Per-vnode lifecycle stores
    /// are not an atomic batch.
    pub(super) fn complete_staged_vnode_transition(&mut self) -> Result<(), DbError> {
        let Some(transition) = self.collect_staged_vnode_transition()? else {
            return Ok(());
        };
        let decoded = self.preflight_decoded_vnodes(&transition.staged)?;
        let resolved = self.resolve_decoded_vnodes(&decoded)?;
        self.run_vnode_transition_callbacks(&transition.revoked, &resolved)?;
        self.finalize_vnode_transition(&transition, &resolved)
    }

    pub(super) fn complete_committed_final_owner_exit(&mut self) -> Result<(), DbError> {
        let transition = self.collect_final_owner_exit()?;
        self.run_vnode_transition_callbacks(&transition.revoked, &[])?;
        self.finalize_final_owner_exit(&transition)
    }

    fn collect_final_owner_exit(&self) -> Result<StagedFinalOwnerExit, DbError> {
        let revoke_handle = self
            .staged_vnode_revocation
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] final-owner-exit cleanup has no staged revoke handle".into(),
                )
            })?;
        let staged_revoke = revoke_handle.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup has no staged revoke batch".into(),
            )
        })?;
        let transition = staged_revoke
            .committed_transition()
            .cloned()
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] control-only vnode transition has no committed final-owner-exit \
                     authority"
                        .into(),
                )
            })?;
        let revoked = staged_revoke.vnodes().clone();
        let rehydration_handle = self
            .rehydrated_vnode_state
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] final-owner-exit cleanup has no staged restore handle".into(),
                )
            })?;
        if !rehydration_handle.lock().is_empty() {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup cannot include staged vnode restore state"
                    .into(),
            ));
        }
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
        let authority = FinalOwnerExitAuthoritySnapshot::capture(config, &transition, &revoked)?;
        Ok(StagedFinalOwnerExit {
            authority,
            revoke_handle,
            rehydration_handle,
            transition,
            revoked,
        })
    }

    fn collect_staged_vnode_transition(&self) -> Result<Option<StagedVnodeTransition>, DbError> {
        if !self.has_staged_vnode_transition()
            && self
                .cluster_shuffle
                .as_ref()
                .is_none_or(|config| !config.registry.any_restoring())
        {
            return Ok(None);
        }

        let revoke_handle = self.staged_vnode_revocation.as_ref().map(Arc::clone);
        let rehydration_handle = self.rehydrated_vnode_state.as_ref().map(Arc::clone);
        let staged_revoke = revoke_handle
            .as_ref()
            .and_then(|handle| handle.lock().clone());
        if staged_revoke
            .as_ref()
            .is_some_and(|staged| staged.committed_transition().is_some())
        {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] committed final-owner-exit cleanup requires the fenced control-only \
                 execution path"
                    .into(),
            ));
        }
        let revoked = staged_revoke
            .as_ref()
            .map_or_else(FxHashSet::default, |staged| staged.vnodes().clone());
        let authority = self
            .cluster_shuffle
            .as_ref()
            .map(VnodeTransitionAuthoritySnapshot::capture)
            .transpose()?;

        // Discard only chains that are definitively outside the pinned owner map. Owned chains stay
        // staged until the complete callback batch and activation succeed.
        let mut staged: Vec<(u32, RehydratedVnode)> = if let Some(handle) = &rehydration_handle {
            let mut guard = handle.lock();
            if let Some(authority) = &authority {
                guard.retain(|vnode, _| authority.owns(*vnode));
            }
            guard
                .iter()
                .map(|(vnode, state)| (*vnode, state.clone()))
                .collect()
        } else {
            Vec::new()
        };
        staged.sort_unstable_by_key(|(vnode, _)| *vnode);

        let staged_attempt = staged.first().map(|(_, state)| state.attempt);
        if let Some(attempt) = staged_attempt {
            if !attempt.is_canonical() || staged.iter().any(|(_, state)| state.attempt != attempt) {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] staged vnode chains do not share one canonical checkpoint attempt; first attempt is {attempt:?}"
                )));
            }
        }

        if authority.is_none() && (!staged.is_empty() || !revoked.is_empty()) {
            return Err(DbError::Checkpoint(
                "[LDB-6051] staged vnode transition has no cluster ownership scope".into(),
            ));
        }

        if let Some(authority) = &authority {
            let mut invalid_revokes: Vec<u32> = revoked
                .iter()
                .copied()
                .filter(|vnode| !authority.contains_vnode(*vnode))
                .collect();
            invalid_revokes.sort_unstable();
            if !invalid_revokes.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] revoked vnode ids {invalid_revokes:?} are outside the pinned \
                     assignment cardinality"
                )));
            }
        }

        let (expected_vnodes, unowned_restoring_vnodes) = authority.as_ref().map_or_else(
            || (Vec::new(), Vec::new()),
            VnodeTransitionAuthoritySnapshot::restoring_vnodes_by_ownership,
        );
        // Ownership is the serving authority. A stale lifecycle bit on an unowned vnode cannot
        // authorize output, but clearing it prevents a discarded acquire from pinning the common
        // path in its conservative restoring slow path forever.
        if let Some(authority) = &authority {
            authority.registry.mark_active(&unowned_restoring_vnodes);
        }
        let staged_vnodes: Vec<u32> = staged.iter().map(|(vnode, _)| *vnode).collect();
        if staged_vnodes != expected_vnodes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] staged vnode roster {staged_vnodes:?} does not match the exact \
                 owned/restoring roster {expected_vnodes:?}"
            )));
        }
        if let Some(authority) = &authority {
            let mut owned_revokes_without_restore: Vec<u32> = revoked
                .iter()
                .copied()
                .filter(|vnode| {
                    authority.owns(*vnode) && staged_vnodes.binary_search(vnode).is_err()
                })
                .collect();
            owned_revokes_without_restore.sort_unstable();
            if !owned_revokes_without_restore.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] currently owned revoked vnodes {owned_revokes_without_restore:?} \
                     have no matching restoring state"
                )));
            }
        }
        if revoked.is_empty() && staged.is_empty() {
            return Ok(None);
        }

        let Some(authority) = authority else {
            return Err(DbError::Checkpoint(
                "[LDB-6051] staged vnode transition has no cluster ownership scope".into(),
            ));
        };
        Ok(Some(StagedVnodeTransition {
            authority,
            revoke_handle,
            rehydration_handle,
            revoked,
            staged,
            staged_vnodes,
            expected_vnodes,
        }))
    }

    fn preflight_decoded_vnodes(
        &self,
        staged: &[(u32, RehydratedVnode)],
    ) -> Result<Vec<DecodedVnode>, DbError> {
        // Decode and bind every participant before the first revoke or restore callback.
        let mut decoded = Vec::with_capacity(staged.len());
        for (vnode, rehydrated) in staged {
            if rehydrated.chain.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6051] vnode {vnode} rehydration chain has no links"
                )));
            }
            let chain: Vec<VnodePartial> = rehydrated
                .chain
                .iter()
                .enumerate()
                .map(|(link, bytes)| {
                    VnodePartial::decode(bytes).map_err(|error| {
                        DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration chain link {link} is corrupt: \
                             {error}"
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            let mut names = BTreeSet::new();
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
                    names.insert(name.clone());
                }
            }

            let mut operators = Vec::with_capacity(names.len());
            for name in names {
                let node_idx = self
                    .nodes
                    .iter()
                    .position(|node| !node.removed && &*node.name == name.as_str())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "[LDB-6051] vnode {vnode} rehydration requires missing operator \
                             '{name}' (topology drift)"
                        ))
                    })?;
                operators.push((node_idx, name));
            }
            decoded.push(DecodedVnode {
                vnode: *vnode,
                attempt: rehydrated.attempt,
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
                            crate::recovery_manager::resolve_op_chain(&vnode.chain, name)
                                .ok_or_else(|| {
                                    DbError::Checkpoint(format!(
                                        "[LDB-6051] vnode {} rehydration chain has no FULL base \
                                         for operator '{name}'",
                                        vnode.vnode
                                    ))
                                })?;
                        Ok(ResolvedOperator {
                            node_idx: *node_idx,
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

    fn run_vnode_transition_callbacks(
        &mut self,
        revoked: &FxHashSet<u32>,
        resolved: &[ResolvedVnode<'_>],
    ) -> Result<(), DbError> {
        if !revoked.is_empty() {
            let mut node_indices: Vec<usize> = self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(index, node)| (!node.removed).then_some(index))
                .collect();
            node_indices.sort_unstable_by(|left, right| {
                self.nodes[*left]
                    .name
                    .cmp(&self.nodes[*right].name)
                    .then_with(|| left.cmp(right))
            });
            for node_idx in node_indices {
                let node = &mut self.nodes[node_idx];
                if let Err(error) = node.operator.drop_owned_vnodes(revoked) {
                    let phase = format!("revoke callback for operator '{}'", node.name);
                    return Err(callback_error(&self.execution_poisoned, &phase, error));
                }
            }
        }

        for vnode in resolved {
            for operator in &vnode.operators {
                if let Err(error) = self.nodes[operator.node_idx].operator.apply_vnode_chain(
                    vnode.vnode,
                    operator.base,
                    &operator.deltas,
                ) {
                    let phase = format!(
                        "restore callback for vnode {} operator '{}'",
                        vnode.vnode, operator.name
                    );
                    return Err(callback_error(&self.execution_poisoned, &phase, error));
                }
            }
        }
        Ok(())
    }

    fn finalize_vnode_transition(
        &self,
        transition: &StagedVnodeTransition,
        resolved: &[ResolvedVnode<'_>],
    ) -> Result<(), DbError> {
        // The normal rotation read fence prevents this change. Revalidation also fails closed for
        // direct/test callers that omit that fence and for a callback that mutates cluster scope.
        if let Err(error) = transition
            .authority
            .revalidate_after_callbacks(&transition.expected_vnodes)
        {
            self.execution_poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        if let Some(handle) = &transition.revoke_handle {
            let exact = handle.lock().as_ref().is_some_and(|staged| {
                staged.committed_transition().is_none() && staged.vnodes() == &transition.revoked
            });
            if !transition.revoked.is_empty() && !exact {
                self.execution_poisoned.store(true, Ordering::Release);
                return Err(DbError::StatefulOperatorPartialApply(
                    "[LDB-6051] staged target-scoped revoke batch changed after lifecycle \
                     callbacks; recovery is required"
                        .into(),
                ));
            }
        }
        // Activation starts only after every callback and revalidation succeeds. Individual
        // lifecycle slots are sequential atomics, not one atomic batch publication.
        transition
            .authority
            .registry
            .mark_active(&transition.expected_vnodes);

        let applied: FxHashSet<u32> = transition.staged_vnodes.iter().copied().collect();
        if let Some(handle) = &transition.revoke_handle {
            if !transition.revoked.is_empty() {
                *handle.lock() = None;
            }
        }
        if let Some(handle) = &transition.rehydration_handle {
            handle.lock().retain(|vnode, _| !applied.contains(vnode));
        }

        for vnode in resolved {
            tracing::debug!(
                vnode = vnode.vnode,
                epoch = vnode.attempt.epoch,
                checkpoint_id = vnode.attempt.checkpoint_id,
                operators = vnode.operators.len(),
                links = vnode.links,
                "completed vnode recovery-chain callbacks"
            );
        }
        tracing::info!(
            assignment_version = transition.authority.assignment.version(),
            revoked_vnodes = transition.revoked.len(),
            restored_vnodes = resolved.len(),
            "completed staged vnode transition"
        );
        Ok(())
    }

    fn finalize_final_owner_exit(&self, transition: &StagedFinalOwnerExit) -> Result<(), DbError> {
        if let Err(error) = transition.authority.revalidate_after_callbacks() {
            self.execution_poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        let mut staged = transition.revoke_handle.lock();
        let rehydrated = transition.rehydration_handle.lock();
        if !rehydrated.is_empty() || transition.authority.registry.any_restoring() {
            self.execution_poisoned.store(true, Ordering::Release);
            return Err(DbError::StatefulOperatorPartialApply(
                "[LDB-6051] vnode restore state appeared during final-owner-exit callbacks; \
                 recovery is required"
                    .into(),
            ));
        }
        let exact = staged.as_ref().is_some_and(|current| {
            current.committed_transition() == Some(&transition.transition)
                && current.vnodes() == &transition.revoked
        });
        if !exact {
            self.execution_poisoned.store(true, Ordering::Release);
            return Err(DbError::StatefulOperatorPartialApply(
                "[LDB-6051] staged final-owner-exit revoke batch changed after callbacks; \
                 recovery is required"
                    .into(),
            ));
        }
        *staged = None;
        tracing::info!(
            assignment_version = transition.authority.assignment.version(),
            revoked_vnodes = transition.revoked.len(),
            "completed committed final-owner-exit vnode cleanup"
        );
        Ok(())
    }
}
