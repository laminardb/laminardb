use super::{AssignmentAdoptionMode, AssignmentAuthorityActivation, DbError, DbState, LaminarDB};
use std::time::Duration;

use laminar_core::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, LeaderProof,
};
use laminar_core::cluster::control::{
    ClusterController, RecoverPhase, RecoveryAnnouncement, RecoveryControlError, RecoveryRound,
};

impl AssignmentAdoptionMode {
    pub(super) async fn after_authority_audit(
        self,
        db: &LaminarDB,
        audited_recovery: bool,
        terminal_drain: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(Self, bool), DbError> {
        let state = self
            .state_after_retired_generation(db, audited_recovery, deadline)
            .await?;
        let faulted = state == DbState::Faulted;
        let faulted_terminal_drain = faulted && terminal_drain;
        let use_cold =
            self == Self::LiveTransition && faulted && (audited_recovery || terminal_drain);
        let selected = if use_cold { Self::ColdRecovery } else { self };
        Ok((selected, faulted_terminal_drain))
    }

    async fn state_after_retired_generation(
        self,
        db: &LaminarDB,
        audited_recovery: bool,
        deadline: tokio::time::Instant,
    ) -> Result<DbState, DbError> {
        let state = DbState::load(&db.state);
        let retired_during_fault = self == Self::LiveTransition
            && audited_recovery
            && matches!(state, DbState::Running | DbState::ShuttingDown)
            && db
                .pending_recovery_fault
                .load(std::sync::atomic::Ordering::Acquire)
                != 0
            && db.installed_vnode_state.lock().is_none();
        if !retired_during_fault {
            return Ok(state);
        }

        // RECOVERY: graph poison retires its state binding before the compute runtime finishes
        // draining non-abortable storage work and publishes Faulted. Reusing that heap is unsafe;
        // wait under the adoption deadline for the lifecycle owner to expose the stable boundary.
        loop {
            tokio::select! {
                biased;
                () = db.assignment_restore_shutdown.cancelled() => {
                    return Err(DbError::Shutdown);
                }
                () = tokio::time::sleep_until(deadline) => {
                    return Err(DbError::Checkpoint(
                        "[LDB-6053] recovery assignment adoption timed out waiting for the retired compute generation lifecycle boundary".into(),
                    ));
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
            }
            let state = DbState::load(&db.state);
            if !matches!(state, DbState::Running | DbState::ShuttingDown) {
                return Ok(state);
            }
        }
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
    pub(super) fn shuffle_assignment_authority_is_exact(
        &self,
        fence: &CheckpointAssignmentFence,
    ) -> bool {
        let expected_digest = fence.digest();
        let receiver_exact = self
            .shuffle_receiver
            .lock()
            .as_ref()
            .is_none_or(|endpoint| {
                endpoint.assignment_version() == fence.assignment_version
                    && endpoint.active_assignment_digest() == Some(expected_digest)
            });
        let sender_exact = self.shuffle_sender.lock().as_ref().is_none_or(|endpoint| {
            endpoint.assignment_version() == fence.assignment_version
                && endpoint.active_assignment_digest() == Some(expected_digest)
        });
        receiver_exact && sender_exact
    }

    /// Activate watcher-owned authority, retaining an exact installation made by recovery while
    /// the watcher's original deadline expired waiting for assignment serialization.
    pub(crate) async fn activate_watcher_assignment_authority(
        &self,
        controller: &ClusterController,
        fence: &CheckpointAssignmentFence,
        drain_transition: Option<AssignmentDrainTransition>,
        expected_revision: u64,
        deadline: tokio::time::Instant,
        reconciliation_timeout: Duration,
    ) -> Result<AssignmentAuthorityActivation, DbError> {
        let expected_drain = drain_transition.clone();
        let error = match self
            .activate_assignment_authority(fence, drain_transition, expected_revision, deadline)
            .await
        {
            Ok(activation) => return Ok(activation),
            Err(error) => error,
        };

        let reconciliation_deadline = tokio::time::Instant::now() + reconciliation_timeout;
        match self
            .settle_failed_watcher_activation(
                controller,
                fence,
                expected_drain.as_ref(),
                expected_revision,
                reconciliation_deadline,
            )
            .await
        {
            Ok(true) => Ok(AssignmentAuthorityActivation {
                installed: true,
                intake_open: false,
                revision: expected_revision,
            }),
            Ok(false) => Err(error),
            Err(settlement) => Err(DbError::Checkpoint(format!(
                "{error}; failed to settle watcher activation: {settlement}"
            ))),
        }
    }

    async fn settle_failed_watcher_activation(
        &self,
        controller: &ClusterController,
        fence: &CheckpointAssignmentFence,
        expected_drain: Option<&AssignmentDrainTransition>,
        expected_revision: u64,
        deadline: tokio::time::Instant,
    ) -> Result<bool, DbError> {
        self.set_source_gate(true);
        let adoption =
            tokio::time::timeout_at(deadline, self.assignment_adoption_lock.lock()).await;
        let Ok(_adoption) = adoption else {
            // Advancing the revision outside the lock makes any concurrent activation fail its
            // final revision check, so timeout cleanup cannot leave half-published authority.
            self.withdraw_assignment_authority(controller);
            return Err(DbError::Checkpoint(
                "timed out reconciling failed watcher assignment activation; authority withdrawn"
                    .into(),
            ));
        };
        let exact_recovery_installation = controller.is_recovering()
            && controller.process_lease_is_live()
            && self.cluster_intake_fenced()
            && self
                .assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire)
                == expected_revision
            && controller
                .checkpoint_assignment_fence(fence.assignment_version)
                .as_ref()
                == Some(fence)
            && controller.checkpoint_drain_transition().as_ref() == expected_drain
            && self.shuffle_assignment_authority_is_exact(fence);
        if exact_recovery_installation {
            return Ok(true);
        }

        self.withdraw_assignment_authority(controller);
        Ok(false)
    }

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
