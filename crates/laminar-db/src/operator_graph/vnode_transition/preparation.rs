//! Validation and immutable preparation for vnode-state transitions.

use std::sync::Arc;

use laminar_core::checkpoint::StateFrameKey;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    accepts_portable_whole_state, FinalOwnerExitAuthoritySnapshot, ManagedVnodeRestore,
    ManagedVnodeTransition, ManagedVnodeTransitionMode, ManagedWholeRestore, OperatorGraph,
    PreparedFinalOwnerExit, PreparedManagedOperators, PreparedVnodeTransition,
    ProjectedTransitionFrames, VnodeTransitionAuthoritySnapshot,
};
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::recovery_manager::RecoveredStateFrame;
use crate::vnode_transition_staging::{
    InstalledVnodeStateBinding, InstalledVnodeStateHandle, PendingVnodeTransition,
    PendingVnodeTransitionHandle, VnodeTransitionKind,
};

impl OperatorGraph {
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

    pub(super) fn installed_state_matches_pending(
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

    pub(super) fn prepare_final_owner_exit(&self) -> Result<PreparedFinalOwnerExit, DbError> {
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

    pub(super) fn prepare_pending_vnode_transition(
        &self,
    ) -> Result<Option<PreparedVnodeTransition>, DbError> {
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

    pub(super) fn relevant_revoked_vnodes(
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
            frames
                .whole_restores
                .sort_unstable_by_key(|restore| restore.participant_id);
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

    pub(in crate::operator_graph) fn validate_transition_state_budget(
        &mut self,
        payload_bytes: usize,
        context: impl Into<String>,
    ) -> Result<(), DbError> {
        let mut accounted_bytes = self
            .managed_state_accounted_bytes()
            .saturating_add(payload_bytes);
        if accounted_bytes > self.max_managed_state_bytes {
            let _reported_evicted_bytes = self
                .nodes
                .iter_mut()
                .filter(|node| !node.removed)
                .fold(0usize, |total, node| {
                    total.saturating_add(node.operator.evict_optional_managed_state())
                });
            accounted_bytes = self
                .managed_state_accounted_bytes()
                .saturating_add(payload_bytes);
        }
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

    pub(super) fn prepare_managed_operators(
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
                | ManagedStateContract::BoundedIntervalJoinV3
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
            if !frames.whole_restores.is_empty() && !accepts_portable_whole_state(contract) {
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
                        | ManagedStateContract::BoundedIntervalJoinV3
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
                if error.requires_pipeline_halt() {
                    return Err(error);
                }
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
}
