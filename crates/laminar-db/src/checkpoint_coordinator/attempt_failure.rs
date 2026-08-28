use std::time::Instant;

use super::{
    checked_successor_epoch, CheckpointAttempt, CheckpointCoordinator,
    CheckpointFailureDisposition, CheckpointPhase, CheckpointResult, CheckpointScope, DbError,
    LeaderProof, SinkEpochPublication,
};
#[cfg(feature = "cluster")]
use super::{publish_terminal_hint_until, BarrierAnnouncement, Phase};

impl CheckpointCoordinator {
    pub(super) async fn record_outcome_until(
        &self,
        attempt: CheckpointAttempt,
        verdict: laminar_core::checkpoint_decision::CheckpointVerdict,
        committed_checkpoint: Option<super::CommittedCheckpointRef>,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<laminar_core::checkpoint_decision::CheckpointOutcome, DbError> {
        use laminar_core::checkpoint_decision::RecordOutcomeResult;

        #[cfg(feature = "cluster")]
        let result = if let Some(controller) = self.cluster_controller.as_ref() {
            let proof = leader_proof
                .as_ref()
                .ok_or_else(|| DbError::Checkpoint("cluster outcome has no leader proof".into()))?;
            let fence = assignment_fence.clone().ok_or_else(|| {
                DbError::Checkpoint("cluster outcome has no assignment fence".into())
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
            })?;
            tokio::time::timeout_at(
                deadline,
                authority.record_cluster_outcome(
                    proof,
                    attempt.epoch,
                    attempt.checkpoint_id,
                    fence,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("cluster outcome create: {error}")))?
        } else {
            let store = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("checkpoint outcome requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                store.record_outcome(
                    attempt.epoch,
                    attempt.checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("checkpoint outcome create: {error}")))?
        };
        #[cfg(not(feature = "cluster"))]
        let result = {
            let store = self.decision_store.as_ref().ok_or_else(|| {
                DbError::Checkpoint("checkpoint outcome requires a decision store".into())
            })?;
            tokio::time::timeout_at(
                deadline,
                store.record_outcome(
                    attempt.epoch,
                    attempt.checkpoint_id,
                    CheckpointScope::Local,
                    None,
                    None,
                    verdict.clone(),
                    committed_checkpoint.clone(),
                ),
            )
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint outcome create timed out".into()))?
            .map_err(|error| DbError::Checkpoint(format!("checkpoint outcome create: {error}")))?
        };

        let winner = match result {
            RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => {
                outcome
            }
            RecordOutcomeResult::Conflict { winner } => winner,
        };
        if winner.epoch != attempt.epoch
            || winner.checkpoint_id != attempt.checkpoint_id
            || winner.verdict != verdict
            || winner.committed_checkpoint != committed_checkpoint
            || winner.assignment_fence != assignment_fence
            || winner.leader_proof != leader_proof
            || winner.deployment_id != self.expected_deployment_id()?
        {
            return Err(DbError::Checkpoint(format!(
                "checkpoint {} lost its immutable outcome race",
                attempt.checkpoint_id
            )));
        }
        Ok(winner)
    }

    pub(super) async fn abort_attempt_until(
        &mut self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        self.phase = CheckpointPhase::Deciding;
        #[cfg(feature = "cluster")]
        let cluster_scope = self.cluster_controller.is_some();
        #[cfg(not(feature = "cluster"))]
        let cluster_scope = false;
        self.record_outcome_until(
            attempt,
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
            None,
            assignment_fence,
            leader_proof,
            deadline,
        )
        .await?;
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let rollback = self
            .rollback_sinks_until(attempt.epoch, cleanup_deadline)
            .await;
        let witness_cleanup = if rollback.is_ok() {
            self.clear_sink_witness_until(cleanup_deadline).await
        } else {
            Ok(())
        };
        let artifact_cleanup = if cluster_scope {
            self.failure_requires_recovery = true;
            Ok(())
        } else {
            self.cleanup_local_checkpoint_artifacts_until(attempt, cleanup_deadline)
                .await
        };
        rollback?;
        witness_cleanup?;
        artifact_cleanup?;
        self.allocator.advance_epoch_to(checked_successor_epoch(
            attempt.epoch,
            "closing an aborted checkpoint",
        )?);
        self.phase = CheckpointPhase::Idle;
        Ok(())
    }

    pub(super) fn failed_result(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        error: String,
        disposition: CheckpointFailureDisposition,
    ) -> CheckpointResult {
        let duration = started.elapsed();
        self.phase = CheckpointPhase::Idle;
        self.record_checkpoint_outcome(false, attempt, duration, None);
        if disposition == CheckpointFailureDisposition::RequiresRecovery {
            self.failure_requires_recovery = true;
        }
        CheckpointResult {
            success: false,
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            duration,
            error: Some(error),
            failure_disposition: Some(disposition),
        }
    }

    pub(super) async fn fail_before_commit(
        &mut self,
        attempt: CheckpointAttempt,
        started: Instant,
        error: DbError,
        flags: u64,
        assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<LeaderProof>,
        _attempt_deadline: tokio::time::Instant,
        sink_epoch_publication: SinkEpochPublication,
    ) -> CheckpointResult {
        #[cfg(not(feature = "cluster"))]
        let _ = flags;
        // Once an exact attempt has been reserved, failure settlement owns a private cleanup
        // budget. The attempt deadline fences new capture/durable work, but must not cancel the
        // durable Abort and rollback that make the attempt terminal.
        let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        let mut message = error.to_string();
        if let Err(seal) = self
            .seal_sink_epoch_until(attempt.epoch, cleanup_deadline)
            .await
        {
            self.failure_requires_recovery = true;
            message = format!("{message}; {seal}");
        }
        match self
            .abort_attempt_until(
                attempt,
                assignment_fence.clone(),
                leader_proof.clone(),
                cleanup_deadline,
            )
            .await
        {
            Ok(()) => {
                // Durable Abort is already terminal. Bound its best-effort cluster hint under a
                // fresh, explicit cleanup window rather than reviving the expired attempt deadline
                // or allowing notification I/O to hang.
                #[cfg(feature = "cluster")]
                if let Some(controller) = self.cluster_controller.as_ref() {
                    let notification_deadline =
                        tokio::time::Instant::now() + self.config.cleanup_timeout;
                    publish_terminal_hint_until(
                        notification_deadline,
                        controller.announce_barrier(&BarrierAnnouncement {
                            epoch: attempt.epoch,
                            checkpoint_id: attempt.checkpoint_id,
                            assignment_fence,
                            leader_proof,
                            phase: Phase::Abort,
                            flags,
                        }),
                    )
                    .await;
                }
                // A slow best-effort hint must not consume the local successor epoch's required
                // continuation budget.
                let continuation_deadline =
                    tokio::time::Instant::now() + self.config.cleanup_timeout;
                let requires_recovery = self.failure_requires_recovery;
                let successor = if !requires_recovery && self.has_checkpoint_committable_sinks() {
                    self.begin_sink_epoch_until(continuation_deadline, sink_epoch_publication)
                        .await
                        .err()
                } else {
                    None
                };
                let (error, disposition) = match (requires_recovery, successor) {
                    (true, _) => (message, CheckpointFailureDisposition::RequiresRecovery),
                    (false, Some(successor)) => (
                        format!("{message}; successor sink epoch failed: {successor}"),
                        CheckpointFailureDisposition::RequiresRecovery,
                    ),
                    (false, None) => (message, CheckpointFailureDisposition::Retryable),
                };
                self.failed_result(attempt, started, error, disposition)
            }
            Err(abort) => self.failed_result(
                attempt,
                started,
                format!("{message}; durable Abort or rollback failed: {abort}"),
                CheckpointFailureDisposition::RequiresRecovery,
            ),
        }
    }
}
