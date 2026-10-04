//! Bind the existing stopped/restore/Release protocol to the irreversible catalog decision.

use super::topology_admission::CONTROL_TIMEOUT;
use super::{
    ClusterCheckpointAuthorityError, DecisionError, LeaderAuthorityRecord, LeaderLeaseStore,
};
use crate::checkpoint::CheckpointScope;
use crate::cluster::control::{
    AssignmentSnapshotStore, CheckpointAssignmentFence, LocalProcessAuthorityIdentity,
    ProcessLeaseAuthority, RecoveryAnnouncement, RecoveryRound, TopologyRecoveryBinding,
};

impl LeaderLeaseStore {
    /// Check placement compatibility before fencing processes for an assignment recovery.
    /// This is read-only preflight; the shared assignment append rechecks the same constraint.
    /// An irreversible topology Commit currently supports replacement in the same node slots,
    /// with its complete vnode map, rather than membership changes or state redistribution.
    ///
    /// # Errors
    /// Rejects uncommitted preparation, changed placement, corrupt evidence or a bounded read
    /// failure. Committed/Activating targets may replace processes before their first checkpoint.
    pub async fn validate_topology_assignment_proposal(
        &self,
        proposal: &CheckpointAssignmentFence,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let before = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            before.reject_uncommitted_topology_preparation()?;
            self.validate_topology_assignment_proposal_from(&before, proposal)
                .await?;
            let after = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            after.reject_uncommitted_topology_preparation()?;
            if before.lease.proof() != after.lease.proof()
                || before.committed_topology_identity() != after.committed_topology_identity()
            {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            Ok(())
        })
        .await
        .map_err(|_| DecisionError::Conflict("topology assignment preflight timed out".into()))?
    }

    pub(super) async fn validate_topology_assignment_proposal_from(
        &self,
        current: &LeaderAuthorityRecord,
        proposal: &CheckpointAssignmentFence,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        if !proposal.is_canonical() {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        let Some(operation) = current.committed_topology_operation() else {
            return Ok(());
        };
        self.audit_topology_operation(operation).await?;
        let plan = self
            .load_topology_plan(&operation.plan)
            .await
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if proposal.assignment_version < plan.assignment.assignment_version
            || proposal.assignment_digest != plan.assignment.assignment_digest
            || proposal.vnode_count != plan.assignment.vnode_count
            || proposal.partitioning_abi_version != plan.assignment.partitioning_abi_version
            || !proposal
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .eq(plan
                    .assignment
                    .participants
                    .iter()
                    .map(|participant| participant.node_id))
        {
            return Err(DecisionError::Conflict(
                "committed topology requires the unchanged complete owner map; replace failed processes in their original node slots".into()
            ).into());
        }
        Ok(())
    }

    pub(crate) async fn recovery_topology_binding(
        &self,
        round: &RecoveryRound,
        context: Option<(&AssignmentSnapshotStore, &ProcessLeaseAuthority)>,
    ) -> Result<Option<TopologyRecoveryBinding>, ClusterCheckpointAuthorityError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let before = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let binding = if let Some(operation) = before.committed_topology_operation() {
                let (assignments, processes) = context.ok_or_else(|| {
                    DecisionError::Conflict(
                        "topology recovery requires configured assignment and process authority"
                            .into(),
                    )
                })?;
                self.audit_topology_operation(operation).await?;
                let assignment = assignments
                    .load()
                    .await
                    .map_err(|error| DecisionError::Conflict(error.to_string()))?
                    .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
                if assignment.draining
                    || assignment.assignment_fence().ok().as_ref() != Some(&round.assignment_fence)
                {
                    return Err(ClusterCheckpointAuthorityError::Fenced);
                }
                let mut roster = Vec::with_capacity(round.assignment_fence.participants.len());
                for participant in &round.assignment_fence.participants {
                    let lease = processes
                        .store_for(crate::cluster::discovery::NodeId(participant.node_id))
                        .load()
                        .await
                        .map_err(|error| DecisionError::Conflict(error.to_string()))?
                        .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
                    let identity = LocalProcessAuthorityIdentity {
                        participant: *participant,
                        process_term: lease.term,
                    };
                    self.require_topology_process(processes, identity)
                        .await
                        .map_err(|error| DecisionError::Conflict(error.to_string()))?;
                    roster.push(identity);
                }
                Some(
                    TopologyRecoveryBinding::new(
                        operation
                            .commit
                            .clone()
                            .ok_or(ClusterCheckpointAuthorityError::Fenced)?,
                        roster,
                    )
                    .map_err(|error| DecisionError::Conflict(error.to_string()))?,
                )
            } else {
                None
            };
            let after = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if before.lease.proof() != after.lease.proof()
                || before.committed_topology_identity() != after.committed_topology_identity()
            {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            Ok(binding)
        })
        .await
        .map_err(|_| DecisionError::Conflict("recovery topology binding read timed out".into()))?
    }

    pub(crate) async fn recovery_round_topology_is_current(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, ClusterCheckpointAuthorityError> {
        let head = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        Ok(Self::recovery_topology_matches(&head, round))
    }

    pub(super) fn recovery_topology_matches(
        head: &LeaderAuthorityRecord,
        round: &RecoveryRound,
    ) -> bool {
        head.committed_topology_operation()
            .and_then(|operation| operation.commit.as_ref())
            == round
                .topology_binding()
                .map(TopologyRecoveryBinding::commit)
    }

    /// Audit the target and, when selecting/installing/committing Release, its exact greatest cut.
    /// Later release-consumption audits use only the immutable target: a healthy released runtime
    /// continues checkpointing and must never rewind to its recovery terminal's old epoch.
    pub(crate) async fn audit_recovery_topology(
        &self,
        round: &RecoveryRound,
        epoch: Option<u64>,
        context: Option<(&AssignmentSnapshotStore, &ProcessLeaseAuthority)>,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let head = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            self.audit_recovery_topology_from(&head, round, epoch, context)
                .await?;
            let after = self
                .load_record()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            if !Self::recovery_topology_matches(&after, round)
                || head.commit_head != after.commit_head
                || head.committed_topology_operation() != after.committed_topology_operation()
                || (epoch.is_some()
                    && (head.assignment_handoff_pin != after.assignment_handoff_pin
                        || head.active_checkpoint_artifacts != after.active_checkpoint_artifacts
                        || head.artifact_cleanup != after.artifact_cleanup
                        || head.assignment_drain_reservation != after.assignment_drain_reservation))
            {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            Ok(())
        })
        .await
        .map_err(|_| DecisionError::Conflict("recovery topology audit timed out".into()))?
    }

    pub(super) async fn audit_recovery_topology_from(
        &self,
        head: &LeaderAuthorityRecord,
        round: &RecoveryRound,
        epoch: Option<u64>,
        context: Option<(&AssignmentSnapshotStore, &ProcessLeaseAuthority)>,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        if round.topology_binding().is_none() {
            head.reject_pending_topology_commit()?;
        }
        if !Self::recovery_topology_matches(head, round) {
            return Err(DecisionError::Conflict(
                "recovery round does not bind the current topology Commit".into(),
            )
            .into());
        }
        let Some(binding) = round.topology_binding() else {
            return Ok(());
        };
        let (assignments, processes) = context.ok_or_else(|| {
            DecisionError::Conflict(
                "topology recovery requires configured assignment and process authority".into(),
            )
        })?;
        binding
            .validate()
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let operation = head
            .committed_topology_operation()
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        self.audit_topology_operation(operation).await?;
        let plan = self
            .load_topology_plan(&operation.plan)
            .await
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let descriptor = self
            .audit_topology_compatibility(&plan)
            .await
            .map_err(|error| DecisionError::Conflict(error.to_string()))?
            .ok_or_else(|| DecisionError::Conflict("recovery topology has no descriptor".into()))?;
        let assignment = assignments
            .load()
            .await
            .map_err(|error| DecisionError::Conflict(error.to_string()))?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        let fence = assignment
            .assignment_fence()
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if assignment.draining
            || fence != round.assignment_fence
            || fence.assignment_version < plan.assignment.assignment_version
            || fence.assignment_digest != plan.assignment.assignment_digest
            || fence.vnode_count != plan.assignment.vnode_count
            || fence.partitioning_abi_version != plan.assignment.partitioning_abi_version
            || !fence
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .eq(plan
                    .assignment
                    .participants
                    .iter()
                    .map(|participant| participant.node_id))
        {
            return Err(DecisionError::Conflict("recovery topology requires the exact current boots and unchanged complete owner map".into()).into());
        }
        for process in binding.processes() {
            self.require_topology_process(processes, *process)
                .await
                .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        }
        let Some(epoch) = epoch else {
            return Ok(());
        };
        if head.active_checkpoint_artifacts.is_some()
            || head.artifact_cleanup.is_some()
            || head.assignment_drain_reservation.is_some()
        {
            return Err(DecisionError::Conflict("topology recovery Start/install/Release requires settled checkpoint, cleanup and assignment authority".into()).into());
        }
        let root_ref = &operation
            .migration_root
            .as_ref()
            .ok_or_else(|| DecisionError::Conflict("recovery topology has no root".into()))?
            .root;
        let root = self
            .load_topology_root(root_ref)
            .await
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let committed_head = head.commit_head.ok_or_else(|| {
            DecisionError::Conflict("recovery topology has no checkpoint Commit".into())
        })?;
        if epoch != committed_head.epoch {
            return Err(DecisionError::Conflict(
                "recovery Start/Release does not select the greatest topology checkpoint cut"
                    .into(),
            )
            .into());
        }
        let (outcome, checkpoint) = self
            .cluster_outcome_with_committed_checkpoint(epoch)
            .await?
            .ok_or_else(|| {
                DecisionError::Conflict("recovery topology checkpoint outcome is missing".into())
            })?;
        let checkpoint = checkpoint.ok_or_else(|| {
            DecisionError::Conflict("recovery topology cut is not committed".into())
        })?;
        let root_cut = committed_head.sequence == root.cut.authority_sequence;
        let checkpoint_assignment = checkpoint
            .assignment_fence
            .as_ref()
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !root_cut
            && (checkpoint_assignment.assignment_version < plan.assignment.assignment_version
                || checkpoint_assignment.assignment_version > fence.assignment_version
                || checkpoint_assignment.assignment_digest != fence.assignment_digest
                || checkpoint_assignment.vnode_count != fence.vnode_count
                || checkpoint_assignment.partitioning_abi_version != fence.partitioning_abi_version
                || !checkpoint_assignment
                    .participants
                    .iter()
                    .map(|participant| participant.node_id)
                    .eq(fence
                        .participants
                        .iter()
                        .map(|participant| participant.node_id)))
        {
            return Err(DecisionError::Conflict(
                "target recovery checkpoint differs from the frozen owner map".into(),
            )
            .into());
        }
        if !outcome.is_commit()
            || outcome.checkpoint_id != committed_head.checkpoint_id
            || checkpoint.scope != CheckpointScope::Cluster
            || checkpoint.deployment_id != descriptor.deployment_id
            || (root_cut
                && (outcome.committed_checkpoint.as_ref() != Some(&root.cut.checkpoint)
                    || checkpoint.pipeline_identity != descriptor.parent_pipeline
                    || checkpoint.assignment_fence.as_ref() != Some(&plan.assignment)))
            || (!root_cut
                && (committed_head.sequence <= binding.commit().authority_sequence
                    || checkpoint.pipeline_identity != descriptor.target_pipeline
                    || checkpoint.epoch <= root.cut.checkpoint.epoch))
        {
            return Err(DecisionError::Conflict(
                "recovery cut is neither the exact migration root nor target checkpoint progress"
                    .into(),
            )
            .into());
        }
        // Recovery's handoff pin keeps this exact cut alive until the first checkpoint from
        // the replacement assignment. Waiting for it to disappear would prevent that runtime
        // from ever starting. Retain the pin and accept only its exact target and payload.
        if head.assignment_handoff_pin.as_ref().is_some_and(|pin| {
            pin.target != fence || outcome.committed_checkpoint.as_ref() != Some(&pin.checkpoint)
        }) {
            return Err(DecisionError::Conflict(
                "topology recovery handoff pin differs from its exact assignment or selected checkpoint"
                    .into(),
            )
            .into());
        }
        Ok(())
    }

    pub(super) async fn audit_topology_recovery_terminal(
        &self,
        head: &LeaderAuthorityRecord,
        terminal: &RecoveryAnnouncement,
        context: Option<(&AssignmentSnapshotStore, &ProcessLeaseAuthority)>,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        self.audit_recovery_topology_from(head, &terminal.round, None, context)
            .await
    }

    pub(super) fn release_recovered_topology(
        candidate: &mut LeaderAuthorityRecord,
        round: &RecoveryRound,
        sequence: u64,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let binding = round
            .topology_binding()
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        let operation = candidate
            .topology_operations
            .iter_mut()
            .find(|operation| operation.commit.as_ref() == Some(binding.commit()))
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if operation.phase == crate::cluster::control::TopologyAdmissionPhase::Active {
            // Never change the original installed-runtime UUIDs or released roster.
            return Ok(());
        }
        let activation = operation.activation.as_mut().ok_or_else(|| {
            DecisionError::Conflict("recovered target has no held installation evidence".into())
        })?;
        if activation.recovery_round != Some(round.id)
            || activation.leader != round.leader_proof
            || activation.assignment != round.assignment_fence
            || activation.processes != binding.processes()
            || !activation.installation_complete()
            || activation.release.is_some()
        {
            return Err(DecisionError::Conflict("first target Release requires every exact recovered runtime under this stopped/restore round".into()).into());
        }
        activation.release = Some(crate::cluster::control::TopologyRelease {
            authority_sequence: sequence,
        });
        operation.phase = crate::cluster::control::TopologyAdmissionPhase::Active;
        operation.status_sequence = sequence;
        Ok(())
    }
}
