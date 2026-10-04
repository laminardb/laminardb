//! Exact old-graph cut admission and participant-complete sink settlement.

use super::topology_admission::{
    topology_assignment_error, topology_checkpoint_error, CONTROL_TIMEOUT, MAX_ADMISSION_ATTEMPTS,
};
use super::*;
use crate::checkpoint::{CheckpointAttempt, CheckpointAttemptRelation, CheckpointParticipant};
use crate::cluster::control::topology::{
    TopologyAbortReason, TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyCheckpointCut,
    TopologyCutCommit, TopologyError, TopologyOperationId, TopologyPlanRef,
};

impl LeaderAuthorityRecord {
    pub(super) fn validate_topology_checkpoint_inventory(
        &self,
        inventory: &CheckpointArtifactInventory,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let Some(operation) = self
            .topology_operations
            .iter()
            .find(|entry| entry.blocks_admission())
        else {
            return Ok(());
        };
        if operation.phase == TopologyAdmissionPhase::Quiescing
            && operation
                .cut
                .as_ref()
                .is_some_and(|cut| cut.inventory == *inventory && cut.committed.is_none())
        {
            return Ok(());
        }
        Err(DecisionError::Conflict("checkpoint is not the exact bound topology cut".into()).into())
    }

    pub(super) fn record_topology_cut_outcome(
        &mut self,
        outcome: &CheckpointOutcome,
    ) -> Result<(), LeaseError> {
        let Some(operation) = self
            .topology_operations
            .iter_mut()
            .find(|entry| entry.is_preparing() && entry.cut.is_some())
        else {
            return Ok(());
        };
        let cut = operation
            .cut
            .as_mut()
            .ok_or_else(|| LeaseError::Invalid("topology cut binding is missing".into()))?;
        if cut.inventory.attempt != CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id)
            || cut.inventory.assignment_fence != outcome.assignment_fence
            || cut.inventory.deployment_id != outcome.deployment_id
        {
            return Err(LeaseError::Invalid(
                "terminal checkpoint does not settle the exact topology cut".into(),
            ));
        }
        if outcome.is_commit() {
            cut.committed = Some(TopologyCutCommit {
                checkpoint: outcome.committed_checkpoint.clone().ok_or_else(|| {
                    LeaseError::Invalid("cut Commit has no checkpoint reference".into())
                })?,
                authority_sequence: self.lease.seq,
            });
        } else {
            operation.phase = TopologyAdmissionPhase::Aborted {
                reason: TopologyAbortReason::CheckpointAborted,
            };
        }
        operation.status_sequence = self.lease.seq;
        Ok(())
    }

    pub(super) fn topology_cut_blocks_cleanup(&self, _protected: &CommittedCheckpointRef) -> bool {
        self.topology_operations.iter().any(|operation| {
            // Before Release, no target checkpoint can replace the root's restore obligation.
            // Active roots are retained by the cleanup stop boundary and live-state inventory.
            operation.blocks_admission()
        })
    }
}

impl LeaderLeaseStore {
    pub(super) async fn validate_committed_topology_checkpoint_inventory(
        &self,
        current: &LeaderAuthorityRecord,
        inventory: &CheckpointArtifactInventory,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let Some(operation) = current.committed_topology_operation() else {
            return Ok(());
        };
        let plan = self
            .load_topology_plan(&operation.plan)
            .await
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let descriptor = self
            .audit_topology_compatibility(&plan)
            .await
            .map_err(|error| DecisionError::Conflict(error.to_string()))?
            .ok_or_else(|| {
                DecisionError::Conflict("committed topology has no state descriptor".into())
            })?;
        if inventory.pipeline_identity != descriptor.target_pipeline
            || inventory.deployment_id != descriptor.deployment_id
        {
            return Err(DecisionError::Conflict(
                "checkpoint does not belong to the committed target topology".into(),
            )
            .into());
        }
        Ok(())
    }

    /// Read the reserved old-graph cut owner for checkpoint control, under the exact leader proof.
    /// This never validates candidate compatibility or grants target execution.
    ///
    /// # Errors
    /// Rejects changed authority, assignment evidence, damaged plans, or a bounded read timeout.
    pub async fn topology_checkpoint_operation(
        &self,
        proof: &LeaderProof,
        assignment: &CheckpointAssignmentFence,
    ) -> Result<Option<TopologyAdmissionStatus>, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let current = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            if !proof.is_canonical() || !current.lease.matches_proof(proof) {
                return Err(TopologyError::Fenced);
            }
            let Some(operation) = current
                .topology_operations
                .iter()
                .find(|entry| entry.is_preparing())
            else {
                return Ok(None);
            };
            let plan = self.load_topology_plan(&operation.plan).await?;
            if plan.assignment != *assignment || operation.admitted_by != *proof {
                return Err(TopologyError::Conflict(
                    "topology cut assignment or leader changed".into(),
                ));
            }
            self.audit_topology_operation(operation).await?;
            Ok(Some(operation.clone()))
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// Atomically bind a reserved operation to its old-topology attempt and artifact inventory.
    /// Sources remain open until this exact barrier reaches capture. No target actor is admitted.
    /// The assignment store must be the controller's configured, namespace-verified store.
    ///
    /// # Errors
    /// Rejects a stale proof, payload/attempt rebind, changed assignment, unresolved authority,
    /// or more than 16 append attempts within 15 seconds. Retry the same binding after ambiguity.
    pub async fn begin_topology_checkpoint_cut(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        processes: &crate::cluster::control::ProcessLeaseAuthority,
        operation_id: TopologyOperationId,
        expected_plan: &TopologyPlanRef,
        inventory: CheckpointArtifactInventory,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        inventory.validate().map_err(TopologyError::Invalid)?;
        expected_plan.validate()?;
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self.load_published_authority_head().await?.ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                if !proof.is_canonical() || !current.lease.matches_proof(proof) {
                    return Err(TopologyError::Fenced);
                }
                let index = current.topology_operations.iter().position(|entry| entry.operation_id == operation_id)
                    .ok_or_else(|| TopologyError::Conflict("unknown topology operation".into()))?;
                let operation = &current.topology_operations[index];
                if operation.plan != *expected_plan {
                    return Err(TopologyError::Conflict("operation payload differs".into()));
                }
                self.audit_topology_operation(operation).await?;
                if let Some(cut) = &operation.cut {
                    if cut.inventory != inventory {
                        return Err(TopologyError::Conflict("topology operation is already bound to another exact checkpoint inventory".into()));
                    }
                    return Ok(operation.clone());
                }
                if !matches!(operation.phase, TopologyAdmissionPhase::Planned | TopologyAdmissionPhase::Preparing) || operation.admitted_by != *proof {
                    return Err(TopologyError::Conflict("topology operation cannot start a cut in its current disposition".into()));
                }
                let plan = self.load_topology_plan(&operation.plan).await?;
                let descriptor = self.require_topology_prepared(operation, &plan, processes).await?;
                if inventory.pipeline_identity != descriptor.parent_pipeline {
                    return Err(TopologyError::Conflict("cut pipeline differs from the certified parent".into()));
                }
                let baseline = current.topology_baseline.as_ref().ok_or_else(|| TopologyError::Invalid("cut has no adopted parent".into()))?;
                if inventory.deployment_id != baseline.deployment_id || inventory.assignment_fence.as_ref() != Some(&plan.assignment)
                    || current.committed_topology_identity() != Some((plan.expected_parent, &plan.parent_manifest))
                {
                    return Err(TopologyError::Conflict("cut inventory does not bind the admitted parent and assignment".into()));
                }
                if current.active_checkpoint_artifacts.is_some() || current.artifact_cleanup.is_some()
                    || current.assignment_drain_reservation.is_some() || current.assignment_handoff_pin.is_some()
                    || current.recovery_fault_slots.iter().any(|slot| slot.active || slot.disposition == RecoveryFaultDisposition::Terminal)
                {
                    return Err(TopologyError::Conflict("cut overlaps unresolved checkpoint, assignment, cleanup or recovery authority".into()));
                }
                let assignment = assignments.load().await.map_err(topology_assignment_error)?
                    .ok_or_else(|| TopologyError::Conflict("cut assignment is missing".into()))?;
                if assignment.draining || assignment.assignment_fence().map_err(|error| TopologyError::Invalid(error.to_string()))? != plan.assignment {
                    return Err(TopologyError::Conflict("cut assignment or process roster changed".into()));
                }
                self.reject_consumed_checkpoint_assignment(current, &plan.assignment).await.map_err(topology_checkpoint_error)?;
                if current.outcome_head.is_some_and(|head| inventory.attempt.relation_to(CheckpointAttempt::new(head.epoch, head.checkpoint_id)) != CheckpointAttemptRelation::Newer) {
                    return Err(TopologyError::Conflict("topology cut does not advance the terminal checkpoint".into()));
                }
                let mut lease = current.lease.clone();
                lease.seq = lease.seq.checked_add(1).ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                next.version = next.version.max(TOPOLOGY_CUT_RECORD_VERSION);
                next.active_checkpoint_artifacts = Some(inventory.clone());
                next.active_checkpoint_artifact_leader_proof = Some(proof.clone());
                let operation = &mut next.topology_operations[index];
                operation.phase = TopologyAdmissionPhase::Quiescing;
                operation.status_sequence = sequence;
                operation.cut = Some(TopologyCheckpointCut { inventory: inventory.clone(), bound_sequence: sequence, committed: None, completed_participants: Vec::new() });
                let result = operation.clone();
                match self.create_authority_record(Some(&published), &next).await? {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => return Ok(result),
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        }).await.map_err(|_| TopologyError::Contended)?
    }

    /// Validate a reversible barrier against the same authority that owns its cut binding.
    #[cfg(feature = "cluster")]
    pub(in crate::cluster::control) async fn validate_topology_checkpoint_barrier(
        &self,
        proof: &LeaderProof,
        attempt: CheckpointAttempt,
        assignment: Option<&CheckpointAssignmentFence>,
        flags: u64,
    ) -> Result<(), LeaseError> {
        let current = self
            .load_record()
            .await?
            .ok_or_else(|| LeaseError::Invalid("no durable leader lease exists".into()))?;
        if !current.lease.matches_proof(proof) {
            return Err(LeaseError::Invalid(
                "checkpoint does not match the latest durable leader lease".into(),
            ));
        }
        let operation = current
            .topology_operations
            .iter()
            .find(|entry| entry.blocks_admission());
        if flags & crate::checkpoint::flags::TOPOLOGY_CUT == 0 {
            return if operation.is_none() {
                Ok(())
            } else {
                Err(LeaseError::Invalid(
                    "ordinary barrier cannot cross a topology reservation".into(),
                ))
            };
        }
        let operation = operation.ok_or_else(|| {
            LeaseError::Invalid("topology barrier has no reserved operation".into())
        })?;
        let cut = operation.cut.as_ref().ok_or_else(|| {
            LeaseError::Invalid("topology barrier has no bound checkpoint".into())
        })?;
        if flags != crate::checkpoint::flags::TOPOLOGY_CUT
            || operation.phase != TopologyAdmissionPhase::Quiescing
            || operation.admitted_by != *proof
            || cut.inventory.attempt != attempt
            || assignment.is_none()
            || cut.inventory.assignment_fence.as_ref() != assignment
            || cut.committed.is_some()
        {
            return Err(LeaseError::Invalid(
                "topology barrier differs from its exact live cut".into(),
            ));
        }
        Ok(())
    }

    /// Record a process's completed old-cut tail through shared authority.
    /// The leader receipt follows globally aggregated external sink settlement; followers report
    /// local checkpoint application. The complete frozen roster includes that leader receipt.
    /// The runtime caller must hold intake and sink succession and own a live process lease.
    /// Receipts prove cut application, not actor retirement, state compatibility or target install.
    ///
    /// # Errors
    /// Rejects stale authority, an uncommitted/different cut, changed process identity, recovery,
    /// or bounded contention. A missing receipt or timeout leaves Quiescing visible.
    pub async fn complete_topology_checkpoint_cut(
        &self,
        proof: &LeaderProof,
        attempt: CheckpointAttempt,
        participant: CheckpointParticipant,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self
                    .load_published_authority_head()
                    .await?
                    .ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                if !proof.is_canonical() || !current.lease.matches_proof(proof) {
                    return Err(TopologyError::Fenced);
                }
                let index = current
                    .topology_operations
                    .iter()
                    .position(|entry| {
                        entry
                            .cut
                            .as_ref()
                            .is_some_and(|cut| cut.inventory.attempt == attempt)
                    })
                    .ok_or_else(|| {
                        TopologyError::Conflict("checkpoint is not a bound topology cut".into())
                    })?;
                let operation = &current.topology_operations[index];
                let cut = operation
                    .cut
                    .as_ref()
                    .ok_or_else(|| TopologyError::Invalid("cut binding is missing".into()))?;
                if !operation.is_preparing()
                    || operation.admitted_by != *proof
                    || cut.inventory.assignment_fence.as_ref().is_none_or(|fence| {
                        fence.participant_incarnation(participant.node_id)
                            != Some(participant.boot_incarnation)
                    })
                    || current.recovery_fault_slots.iter().any(|slot| {
                        slot.active || slot.disposition == RecoveryFaultDisposition::Terminal
                    })
                {
                    return Err(TopologyError::Fenced);
                }
                self.audit_topology_operation(operation).await?;
                if cut.committed.is_none() {
                    return Err(TopologyError::Conflict(
                        "cut has no definitive Commit; sink completion cannot certify it".into(),
                    ));
                }
                if cut.completed_participants.contains(&participant) {
                    return Ok(operation.clone());
                }
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                let operation = &mut next.topology_operations[index];
                let cut = operation
                    .cut
                    .as_mut()
                    .ok_or_else(|| TopologyError::Invalid("cut binding is missing".into()))?;
                cut.completed_participants.push(participant);
                cut.completed_participants
                    .sort_unstable_by_key(|entry| entry.node_id);
                if cut
                    .inventory
                    .assignment_fence
                    .as_ref()
                    .is_some_and(|fence| cut.completed_participants == fence.participants)
                {
                    operation.phase = TopologyAdmissionPhase::CutPrepared;
                }
                operation.status_sequence = sequence;
                let result = operation.clone();
                match self
                    .create_authority_record(Some(&published), &next)
                    .await?
                {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        return Ok(result)
                    }
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    pub(super) async fn audit_topology_cut(
        &self,
        operation: &TopologyAdmissionStatus,
    ) -> Result<(), LeaseError> {
        let Some(cut) = &operation.cut else {
            return Ok(());
        };
        let bound = read_authority_record(self.store.as_ref(), cut.bound_sequence)
            .await?
            .ok_or_else(|| {
                LeaseError::Invalid("topology cut admission anchor is missing".into())
            })?;
        let anchored = bound
            .topology_operations
            .iter()
            .find(|entry| entry.operation_id == operation.operation_id)
            .ok_or_else(|| {
                LeaseError::Invalid("topology cut is absent from its admission anchor".into())
            })?;
        if anchored.phase != TopologyAdmissionPhase::Quiescing
            || anchored.plan != operation.plan
            || anchored.admitted_by != operation.admitted_by
            || anchored.admitted_sequence != operation.admitted_sequence
            || anchored.status_sequence != cut.bound_sequence
            || anchored.cut.as_ref().is_none_or(|entry| {
                entry.inventory != cut.inventory
                    || entry.bound_sequence != cut.bound_sequence
                    || entry.committed.is_some()
                    || !entry.completed_participants.is_empty()
            })
            || bound.active_checkpoint_artifacts.as_ref() != Some(&cut.inventory)
            || bound.active_checkpoint_artifact_leader_proof.as_ref()
                != Some(&operation.admitted_by)
        {
            return Err(LeaseError::Invalid(
                "topology cut differs from its admitted checkpoint inventory".into(),
            ));
        }
        if let Some(commit) = &cut.committed {
            let record = read_authority_record(self.store.as_ref(), commit.authority_sequence)
                .await?
                .ok_or_else(|| {
                    LeaseError::Invalid("topology cut Commit anchor is missing".into())
                })?;
            let outcome = record.checkpoint_outcome.as_ref().ok_or_else(|| {
                LeaseError::Invalid("topology cut anchor has no terminal outcome".into())
            })?;
            if !outcome.is_commit()
                || outcome.committed_checkpoint.as_ref() != Some(&commit.checkpoint)
                || outcome.assignment_fence != cut.inventory.assignment_fence
                || outcome.deployment_id != cut.inventory.deployment_id
                || outcome.leader_proof.as_ref() != Some(&operation.admitted_by)
                || record
                    .topology_operations
                    .iter()
                    .find(|entry| entry.operation_id == operation.operation_id)
                    .is_none_or(|entry| {
                        entry.phase != TopologyAdmissionPhase::Quiescing
                            || entry.status_sequence != commit.authority_sequence
                            || entry.cut.as_ref().is_none_or(|anchored| {
                                anchored.inventory != cut.inventory
                                    || anchored.committed.as_ref() != Some(commit)
                                    || !anchored.completed_participants.is_empty()
                            })
                    })
            {
                return Err(LeaseError::Invalid(
                    "topology cut does not bind its exact definitive Commit".into(),
                ));
            }
            if operation.blocks_admission() {
                let index = CheckpointDecisionStore::new(self.store.clone())
                    .load_committed_checkpoint(&commit.checkpoint)
                    .await
                    .map_err(|error| match error {
                        DecisionError::Io(reason) => LeaseError::Io(reason),
                        error => LeaseError::Invalid(error.to_string()),
                    })?;
                if index.pipeline_identity != cut.inventory.pipeline_identity
                    || index.assignment_fence != cut.inventory.assignment_fence
                    || index.deployment_id != cut.inventory.deployment_id
                {
                    return Err(LeaseError::Invalid(
                        "topology cut index differs from its old pipeline identity".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}
