//! Current runtime observations and Release share the catalog/checkpoint authority append.

use uuid::Uuid;

use super::topology_admission::{CONTROL_TIMEOUT, MAX_ADMISSION_ATTEMPTS};
use super::{
    read_authority_record, AssignmentSnapshotStore, AuthorityCreateOutcome, LeaderLeaseStore,
    LeaseError, RecoveryFaultDisposition, TOPOLOGY_INSTALLATION_RECORD_VERSION,
};
use crate::checkpoint::LeaderProof;
use crate::cluster::control::{
    ProcessLeaseAuthority, TopologyActivation, TopologyAdmissionPhase, TopologyAdmissionStatus,
    TopologyError, TopologyInstallationReceipt, TopologyRelease, TopologyRestoreInput,
    TOPOLOGY_INSTALLATION_PROTOCOL_VERSION,
};

impl LeaderLeaseStore {
    /// Revalidate a published Release against the current full process/assignment roster and
    /// fault inventory. Historical Release remains immutable across a harmless leader change.
    ///
    /// # Errors
    /// Fails closed on damaged authority or the bounded read/process-audit budget.
    pub async fn authorize_topology_release(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        input: &TopologyRestoreInput,
        runtime_id: Uuid,
    ) -> Result<bool, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            let head = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            let fresh = self
                .committed_topology_restore_input(
                    assignments,
                    processes,
                    input.operation().operation_id,
                    input.process(),
                )
                .await?;
            if !fresh.same_installed_generation(input)
                || head.committed_topology_operation() != Some(fresh.operation())
            {
                return Ok(false);
            }
            Self::require_topology_release_authority(&head, &fresh)?;
            let Some(round) = &fresh.operation().activation else {
                return Ok(false);
            };
            if fresh.operation().phase != TopologyAdmissionPhase::Active
                || round.release.is_none()
                || round.assignment != *fresh.assignment()
                || round.processes != fresh.processes()
                || !round.installation_complete()
                || !round.installations.iter().any(|receipt| {
                    receipt.process == input.process() && receipt.runtime_id == runtime_id
                })
            {
                return Ok(false);
            }
            if round.recovery_round.is_some() {
                // This first Release shares a recovery terminal; only that exact round may open
                // its replacement runtime. The ordinary installation API cannot borrow it.
                return Ok(false);
            }
            if let Some(recovery) = head.recovery_release_head.as_ref().filter(|recovery| {
                round
                    .release
                    .as_ref()
                    .is_some_and(|release| recovery.sequence > release.authority_sequence)
            }) {
                let terminal = self
                    .recovery_release_terminal_from(&head, recovery)
                    .await
                    .map_err(super::topology_admission::topology_checkpoint_error)?;
                if terminal.round.topology_binding().is_some_and(|binding| {
                    fresh.operation().commit.as_ref() == Some(binding.commit())
                }) {
                    return Ok(false);
                }
            }
            for process in &round.processes {
                self.require_topology_process(processes, *process).await?;
            }
            let after = self.load_record().await?.ok_or(TopologyError::Fenced)?;
            Self::require_topology_release_authority(&after, &fresh)?;
            Ok(after.lease.proof() == head.lease.proof()
                && after.recovery_fault_revision == head.recovery_fault_revision
                && after.committed_topology_operation() == head.committed_topology_operation()
                && assignments
                    .load()
                    .await
                    .map_err(super::topology_admission::topology_assignment_error)?
                    .is_some_and(|assignment| {
                        !assignment.draining
                            && assignment.assignment_fence().ok().as_ref()
                                == Some(fresh.assignment())
                    }))
        })
        .await
        .map_err(|_| TopologyError::ReadTimedOut)?
    }

    /// Certify one actual held runtime, through the controller after runtime-owned readiness.
    /// A historical restore/retirement receipt is insufficient. Every current owner/evidence
    /// process must independently certify protocol five, and identical runtime retries are stable.
    ///
    /// # Errors
    /// Rejects stale runtime/process/assignment/leader, mixed capability, competing checkpoint,
    /// recovery/cleanup or an uncertain 15-second/16-CAS budget. Reread the same operation.
    pub async fn certify_topology_installation(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        input: &TopologyRestoreInput,
        runtime_id: Uuid,
        protocol_version: u16,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        self.certify_topology_installation_for_round(
            assignments,
            processes,
            input,
            runtime_id,
            protocol_version,
            None,
        )
        .await
    }

    /// Certify a held replacement runtime under the existing exact stopped/restore round.
    /// Before the first topology Release this collects a new full installation roster. An already
    /// released topology retains its original roster and requires the recovery readiness terminal.
    ///
    /// # Errors
    /// Rejects a different target/cut, process term, fault set, round or competing runtime UUID.
    pub async fn certify_topology_recovery_installation(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        input: &TopologyRestoreInput,
        runtime_id: Uuid,
        round: &crate::cluster::control::RecoveryRound,
        epoch: u64,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        self.certify_topology_installation_for_round(
            assignments,
            processes,
            input,
            runtime_id,
            TOPOLOGY_INSTALLATION_PROTOCOL_VERSION,
            Some((round, epoch)),
        )
        .await
    }

    async fn certify_topology_installation_for_round(
        &self,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        input: &TopologyRestoreInput,
        runtime_id: Uuid,
        protocol_version: u16,
        recovery: Option<(&crate::cluster::control::RecoveryRound, u64)>,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        if protocol_version != TOPOLOGY_INSTALLATION_PROTOCOL_VERSION || runtime_id.is_nil() {
            return Err(TopologyError::Protocol(
                "installation requires protocol five and an exact nonzero runtime identity".into(),
            ));
        }
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self
                    .load_published_authority_head()
                    .await?
                    .ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                let fresh = self
                    .committed_topology_restore_input(
                        assignments,
                        processes,
                        input.operation().operation_id,
                        input.process(),
                    )
                    .await?;
                if !fresh.same_restore_requirements(input)
                    || Some(current.lease.proof()) != fresh.current_leader()
                {
                    return Err(TopologyError::Fenced);
                }
                let index = current
                    .topology_operations
                    .iter()
                    .position(|operation| operation.operation_id == input.operation().operation_id)
                    .ok_or(TopologyError::Fenced)?;
                let operation = &current.topology_operations[index];
                if operation != fresh.operation() {
                    tokio::task::yield_now().await;
                    continue;
                }
                if let Some((round, epoch)) = recovery {
                    self.audit_recovery_topology_from(
                        current,
                        round,
                        Some(epoch),
                        Some((assignments, processes)),
                    )
                    .await
                    .map_err(super::topology_admission::topology_checkpoint_error)?;
                    let inventory = Self::recovery_fault_inventory_from(current);
                    if round.topology_binding().is_none()
                        || inventory.revision() != round.fault_revision()
                        || inventory.faults() != round.faults
                        || inventory.has_terminal_fault()
                        || round.assignment_fence != *fresh.assignment()
                        || fresh.current_leader().as_ref() != Some(&round.leader_proof)
                        || round
                            .topology_binding()
                            .is_none_or(|binding| binding.processes() != fresh.processes())
                        || current.active_checkpoint_artifacts.is_some()
                        || current.artifact_cleanup.is_some()
                        || current.assignment_drain_reservation.is_some()
                    {
                        return Err(TopologyError::Fenced);
                    }
                    if operation.phase == TopologyAdmissionPhase::Active {
                        return Ok(operation.clone());
                    }
                } else {
                    Self::require_topology_installation_authority(current, &fresh)?;
                }
                let same_round = operation.activation.as_ref().filter(|round| {
                    round.leader == current.lease.proof()
                        && round.recovery_round == recovery.map(|(round, _)| round.id)
                });
                if let Some(round) = same_round {
                    if round.assignment != *fresh.assignment()
                        || round.processes != fresh.processes()
                    {
                        return Err(TopologyError::Fenced);
                    }
                    if let Some(receipt) = round.installations.iter().find(|receipt| {
                        receipt.process.participant.node_id == input.process().participant.node_id
                    }) {
                        if receipt.process != input.process()
                            || receipt.runtime_id != runtime_id
                            || receipt.protocol_version != protocol_version
                        {
                            return Err(TopologyError::Fenced);
                        }
                        return Ok(operation.clone());
                    }
                } else if operation
                    .activation
                    .as_ref()
                    .is_some_and(|round| round.release.is_some())
                {
                    return Err(TopologyError::Fenced);
                }
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                next.version = next.version.max(TOPOLOGY_INSTALLATION_RECORD_VERSION);
                if recovery.is_some() {
                    next.version = next.version.max(super::TOPOLOGY_RECOVERY_RECORD_VERSION);
                }
                let operation = &mut next.topology_operations[index];
                let mut round = same_round.cloned().unwrap_or_else(|| TopologyActivation {
                    leader: current.lease.proof(),
                    recovery_round: recovery.map(|(round, _)| round.id),
                    assignment: fresh.assignment().clone(),
                    processes: fresh.processes().to_vec(),
                    authority_sequence: sequence,
                    installations: Vec::new(),
                    release: None,
                });
                round.installations.push(TopologyInstallationReceipt {
                    process: input.process(),
                    runtime_id,
                    protocol_version,
                    authority_sequence: sequence,
                });
                round
                    .installations
                    .sort_by_key(|receipt| receipt.process.participant.node_id);
                operation.activation = Some(round);
                operation.phase = TopologyAdmissionPhase::Activating;
                operation.status_sequence = sequence;
                for process in fresh.processes() {
                    self.require_topology_process(processes, *process).await?;
                }
                match self
                    .create_authority_record(Some(&published), &next)
                    .await?
                {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        let after = self
                            .committed_topology_restore_input(
                                assignments,
                                processes,
                                input.operation().operation_id,
                                input.process(),
                            )
                            .await?;
                        if !after.same_restore_requirements(input) {
                            return Err(TopologyError::Fenced);
                        }
                        return Ok(after.operation().clone());
                    }
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    /// Publish Release only for the exact complete current installation roster. This permission
    /// does not itself open a local gate or admit a sink epoch. Every local owner must match its
    /// receipt and revalidate the current runtime before applying it.
    ///
    /// # Errors
    /// Rejects incomplete/stale/divergent installation, leader loss, recovery/faults or competing
    /// authority. A lost response may have committed Release; retry the same operation/round.
    pub async fn release_topology_target(
        &self,
        proof: &LeaderProof,
        assignments: &AssignmentSnapshotStore,
        processes: &ProcessLeaseAuthority,
        input: &TopologyRestoreInput,
    ) -> Result<TopologyAdmissionStatus, TopologyError> {
        tokio::time::timeout(CONTROL_TIMEOUT, async {
            for _ in 0..MAX_ADMISSION_ATTEMPTS {
                let published = self
                    .load_published_authority_head()
                    .await?
                    .ok_or(TopologyError::Fenced)?;
                let current = &published.record;
                if !current.lease.matches_proof(proof)
                    || proof.owner.node_id != input.process().participant.node_id
                    || proof.owner.boot_id != input.process().participant.boot_incarnation
                {
                    return Err(TopologyError::Fenced);
                }
                let fresh = self
                    .committed_topology_restore_input(
                        assignments,
                        processes,
                        input.operation().operation_id,
                        input.process(),
                    )
                    .await?;
                if !fresh.same_restore_requirements(input)
                    || fresh.current_leader().as_ref() != Some(proof)
                {
                    return Err(TopologyError::Fenced);
                }
                let index = current
                    .topology_operations
                    .iter()
                    .position(|operation| operation.operation_id == input.operation().operation_id)
                    .ok_or(TopologyError::Fenced)?;
                let operation = &current.topology_operations[index];
                if operation != fresh.operation() {
                    tokio::task::yield_now().await;
                    continue;
                }
                Self::require_topology_release_authority(current, &fresh)?;
                let round = operation.activation.as_ref().ok_or_else(|| {
                    TopologyError::Conflict("target runtime installation has not begun".into())
                })?;
                if round.assignment != *fresh.assignment() || round.processes != fresh.processes() {
                    return Err(TopologyError::Fenced);
                }
                if !round.installation_complete() {
                    return Err(TopologyError::Conflict(
                        "Release requires every current owner and evidence runtime, including zero-vnode participants".into(),
                    ));
                }
                if round.release.is_some() {
                    return Ok(operation.clone());
                }
                Self::require_topology_installation_authority(current, &fresh)?;
                if round.leader != *proof {
                    return Err(TopologyError::Fenced);
                }
                for process in &round.processes {
                    self.require_topology_process(processes, *process).await?;
                }
                let mut lease = current.lease.clone();
                lease.seq = lease
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("authority sequence exhausted".into()))?;
                let sequence = lease.seq;
                let mut next = current.preserve_with_lease(lease);
                let operation = &mut next.topology_operations[index];
                operation
                    .activation
                    .as_mut()
                    .ok_or(TopologyError::Fenced)?
                    .release = Some(TopologyRelease {
                    authority_sequence: sequence,
                });
                operation.phase = TopologyAdmissionPhase::Active;
                operation.status_sequence = sequence;
                match self
                    .create_authority_record(Some(&published), &next)
                    .await?
                {
                    AuthorityCreateOutcome::Created | AuthorityCreateOutcome::ExistingIdentical => {
                        let after = self
                            .committed_topology_restore_input(
                                assignments,
                                processes,
                                input.operation().operation_id,
                                input.process(),
                            )
                            .await?;
                        if !after.same_restore_requirements(input) {
                            return Err(TopologyError::Fenced);
                        }
                        return Ok(after.operation().clone());
                    }
                    AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
                }
            }
            Err(TopologyError::Contended)
        })
        .await
        .map_err(|_| TopologyError::Contended)?
    }

    fn require_topology_installation_authority(
        head: &super::LeaderAuthorityRecord,
        input: &TopologyRestoreInput,
    ) -> Result<(), TopologyError> {
        if head.active_checkpoint_artifacts.is_some() {
            return Err(TopologyError::Conflict(
                "held runtime installation cannot overlap a checkpoint".into(),
            ));
        }
        Self::require_topology_release_authority(head, input)
    }

    fn require_topology_release_authority(
        head: &super::LeaderAuthorityRecord,
        input: &TopologyRestoreInput,
    ) -> Result<(), TopologyError> {
        // A released runtime continues to serve its own checkpoints. That ordinary control
        // work must not withdraw its exact assignment authority during periodic refresh.
        if head
            .active_checkpoint_artifacts
            .as_ref()
            .is_some_and(|inventory| {
                input.operation().phase != TopologyAdmissionPhase::Active
                    || inventory.pipeline_identity != input.descriptor().target_pipeline
                    || inventory.deployment_id != input.descriptor().deployment_id
                    || inventory.assignment_fence.as_ref() != Some(input.assignment())
            })
            || head.artifact_cleanup.is_some()
            || head.assignment_drain_reservation.is_some()
            || head.assignment_handoff_pin.is_some()
            || head
                .recovery_fault_slots
                .iter()
                .any(|slot| slot.active || slot.disposition == RecoveryFaultDisposition::Terminal)
            || head
                .committed_topology_operation()
                .is_none_or(|operation| operation.operation_id != input.operation().operation_id)
        {
            return Err(TopologyError::Conflict("installation/Release requires the exact committed target without competing checkpoint, assignment, cleanup or recovery authority".into()));
        }
        Ok(())
    }

    pub(super) async fn audit_topology_activation(
        &self,
        operation: &TopologyAdmissionStatus,
    ) -> Result<(), LeaseError> {
        let Some(round) = &operation.activation else {
            return Ok(());
        };
        let sequences = std::iter::once(round.authority_sequence)
            .chain(
                round
                    .installations
                    .iter()
                    .map(|receipt| receipt.authority_sequence),
            )
            .chain(
                round
                    .release
                    .iter()
                    .map(|release| release.authority_sequence),
            );
        for sequence in sequences {
            let anchor = read_authority_record(self.store.as_ref(), sequence)
                .await?
                .ok_or_else(|| {
                    LeaseError::Invalid(
                        "topology installation/Release authority anchor is missing".into(),
                    )
                })?;
            let stored = anchor
                .topology_operations
                .iter()
                .find(|entry| entry.operation_id == operation.operation_id)
                .ok_or_else(|| {
                    LeaseError::Invalid("installation operation anchor is missing".into())
                })?;
            let installed = stored.activation.as_ref().ok_or_else(|| {
                LeaseError::Invalid("installation round anchor is missing".into())
            })?;
            let expected_installations = round
                .installations
                .iter()
                .filter(|receipt| receipt.authority_sequence <= sequence)
                .cloned()
                .collect::<Vec<_>>();
            let expected_release = round
                .release
                .as_ref()
                .filter(|release| release.authority_sequence <= sequence);
            if anchor.version < TOPOLOGY_INSTALLATION_RECORD_VERSION
                || !anchor.lease.matches_proof(&round.leader)
                || !stored.same_migration_binding(operation)
                || stored.status_sequence != sequence
                || stored.phase
                    != if expected_release.is_some() {
                        TopologyAdmissionPhase::Active
                    } else {
                        TopologyAdmissionPhase::Activating
                    }
                || stored.commit != operation.commit
                || installed.leader != round.leader
                || installed.recovery_round != round.recovery_round
                || installed.assignment != round.assignment
                || installed.processes != round.processes
                || installed.authority_sequence != round.authority_sequence
                || installed.installations != expected_installations
                || installed.release.as_ref() != expected_release
            {
                return Err(LeaseError::Invalid(
                    "installation/Release differs from its exact immutable append".into(),
                ));
            }
        }
        Ok(())
    }
}
