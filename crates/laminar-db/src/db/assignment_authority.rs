use super::{AssignmentAdoptionMode, AssignmentAuthorityActivation, DbError, DbState, LaminarDB};
use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant, LeaderProof};
use laminar_core::cluster::control::{
    ClusterController, RecoverPhase, RecoveryAnnouncement, RecoveryControlError, RecoveryRound,
};

impl AssignmentAdoptionMode {
    pub(super) fn after_authority_audit(
        self,
        audited_recovery: bool,
        terminal_drain: bool,
        state: DbState,
    ) -> (Self, bool) {
        let faulted = state == DbState::Faulted;
        let faulted_terminal_drain = faulted && terminal_drain;
        let use_cold =
            self == Self::LiveTransition && faulted && (audited_recovery || terminal_drain);
        let selected = if use_cold { Self::ColdRecovery } else { self };
        (selected, faulted_terminal_drain)
    }
}

async fn observe_stopped_round(
    controller: &ClusterController,
    durable_proof: Option<&LeaderProof>,
) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
    match durable_proof {
        Some(proof) => {
            controller
                .observe_recover_control_for_durable_proof(proof)
                .await
        }
        None => controller.observe_recover_control().await,
    }
}

pub(crate) async fn audited_stopped_terminal_round(
    controller: &ClusterController,
    predecessor: &CheckpointAssignmentFence,
    deadline: tokio::time::Instant,
) -> Result<Option<RecoveryRound>, DbError> {
    audited_stopped_round(controller, predecessor, None, deadline).await
}

pub(crate) async fn audited_stopped_recovery_successor_round(
    controller: &ClusterController,
    predecessor: &CheckpointAssignmentFence,
    leader_proof: &LeaderProof,
    deadline: tokio::time::Instant,
) -> Result<Option<RecoveryRound>, DbError> {
    audited_stopped_round(controller, predecessor, Some(leader_proof), deadline).await
}

async fn audited_stopped_round(
    controller: &ClusterController,
    predecessor: &CheckpointAssignmentFence,
    durable_proof: Option<&LeaderProof>,
    deadline: tokio::time::Instant,
) -> Result<Option<RecoveryRound>, DbError> {
    let active =
        tokio::time::timeout_at(deadline, observe_stopped_round(controller, durable_proof))
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "stopped-recovery Prepare authority observation timed out".into(),
                )
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "stopped-recovery Prepare authority observation failed: {error}"
                ))
            })?;
    let Some(RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Prepare,
    }) = active
    else {
        return Ok(None);
    };
    let local = controller.instance_id();
    if round.assignment_fence != *predecessor
        || (durable_proof.is_none() && !controller.recovery_driver_is_current(&round))
        || !controller.recovery_round_requires_current_process_stop(&round)
        || !controller.process_lease_is_live()
    {
        return Ok(None);
    }
    let reports = tokio::time::timeout_at(deadline, controller.read_stopped(&round, &[local]))
        .await
        .map_err(|_| {
            DbError::Checkpoint("stopped-recovery local stopped-report read timed out".into())
        })?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "stopped-recovery local stopped-report read failed: {error}"
            ))
        })?;
    let expected = CheckpointParticipant {
        node_id: local.0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    if reports.len() != 1
        || reports[0].publisher() != expected
        || reports[0].validate(&round).is_err()
    {
        return Ok(None);
    }
    let confirmed =
        tokio::time::timeout_at(deadline, observe_stopped_round(controller, durable_proof))
            .await
            .map_err(|_| {
                DbError::Checkpoint("stopped-recovery Prepare authority recheck timed out".into())
            })?
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "stopped-recovery Prepare authority recheck failed: {error}"
                ))
            })?;
    if confirmed
        != Some(RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        })
        || !controller.process_lease_is_live()
    {
        return Ok(None);
    }
    Ok(Some(round))
}

#[cfg(feature = "cluster")]
impl LaminarDB {
    fn assignment_authority_is_current(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        expected_drain: Option<&laminar_core::checkpoint::AssignmentDrainTransition>,
        expected_leader: u64,
        expected_revision: u64,
        deadline: tokio::time::Instant,
    ) -> bool {
        tokio::time::Instant::now() < deadline
            && !self
                .terminal_pipeline_halt
                .load(std::sync::atomic::Ordering::Acquire)
            && !self
                .durable_terminal_recovery_fence
                .load(std::sync::atomic::Ordering::Acquire)
            && self
                .assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire)
                == expected_revision
            && !controller.is_recovering()
            && controller.process_lease_is_live()
            && controller.current_leader().map(|leader| leader.0) == Some(expected_leader)
            && controller
                .checkpoint_assignment_fence(fence.assignment_version)
                .as_ref()
                == Some(fence)
            && controller.checkpoint_drain_transition().as_ref() == expected_drain
    }

    fn withdraw_inactive_assignment(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
    ) -> AssignmentAuthorityActivation {
        self.withdraw_assignment_authority(controller);
        AssignmentAuthorityActivation {
            installed: false,
            intake_open: false,
            revision: self
                .assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }

    async fn ensure_assignment_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut coordinator = tokio::time::timeout_at(deadline, self.coordinator.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "assignment sink epoch timed out waiting for the coordinator".into(),
                )
            })?;
        match coordinator.as_mut() {
            Some(coordinator) => {
                coordinator
                    .ensure_assignment_sink_epoch_until(deadline)
                    .await
            }
            None => Ok(()),
        }
    }

    fn fail_assignment_sink_epoch(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
        error: DbError,
    ) -> DbError {
        // RECOVERY: target assignment authority cannot outlive an unresolved exact-sink
        // transition. Queueing is synchronous and durable publication is monitor-owned.
        controller.set_recovering(true);
        let fault = crate::coordinated_recovery::queue_local_fault(
            controller,
            &self.pending_recovery_fault,
        );
        self.withdraw_assignment_authority(controller);
        match fault {
            Ok(()) => error,
            Err(fault) => {
                DbError::Checkpoint(format!("{error}; recovery fault queue failed: {fault}"))
            }
        }
    }

    pub(super) async fn open_assignment_intake_after_audit(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        expected_drain: Option<&laminar_core::checkpoint::AssignmentDrainTransition>,
        expected_leader: u64,
        expected_revision: u64,
        deadline: tokio::time::Instant,
    ) -> Result<AssignmentAuthorityActivation, DbError> {
        if !self.assignment_authority_is_current(
            controller,
            fence,
            expected_drain,
            expected_leader,
            expected_revision,
            deadline,
        ) {
            return Ok(self.withdraw_inactive_assignment(controller));
        }

        // A terminal HANDOFF closes the predecessor sink epoch without reserving a successor.
        // The target certificate is installed while intake remains closed, so this is the first
        // point where an exact successor can be admitted against the target assignment.
        if let Err(error) = self.ensure_assignment_sink_epoch_until(deadline).await {
            return Err(self.fail_assignment_sink_epoch(controller, error));
        }
        if !self.assignment_authority_is_current(
            controller,
            fence,
            expected_drain,
            expected_leader,
            expected_revision,
            deadline,
        ) {
            return Ok(self.withdraw_inactive_assignment(controller));
        }

        self.set_source_gate(false);
        if !self.assignment_authority_is_current(
            controller,
            fence,
            expected_drain,
            expected_leader,
            expected_revision,
            deadline,
        ) {
            return Ok(self.withdraw_inactive_assignment(controller));
        }
        Ok(AssignmentAuthorityActivation {
            installed: true,
            intake_open: true,
            revision: expected_revision,
        })
    }
}
