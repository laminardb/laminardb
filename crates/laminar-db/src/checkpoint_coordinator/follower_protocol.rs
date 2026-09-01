use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::checkpoint::{CheckpointAttempt, CheckpointAttemptRelation, LeaderProof};
use laminar_core::cluster::control::{BarrierAnnouncement, Phase, QuorumOutcome};

use super::{
    require_canonical_attempt, sink_epoch_admission, CheckpointCoordinator, CheckpointRequest,
    DbError, FollowerPrepareOutcome, PrepareQuorum,
};

const FOLLOWER_DECISION_POLL: Duration = Duration::from_millis(250);

impl CheckpointCoordinator {
    fn validate_cluster_watermark_candidate(
        controller: &laminar_core::cluster::control::ClusterController,
        observed: laminar_core::checkpoint::CheckpointWatermark,
    ) -> Result<laminar_core::checkpoint::CheckpointWatermark, String> {
        observed
            .validate()
            .map_err(|error| format!("invalid checkpoint watermark: {error}"))?;
        match (controller.cluster_min_watermark(), observed) {
            (Some(current), laminar_core::checkpoint::CheckpointWatermark::Active(watermark))
                if watermark < current =>
            {
                Err(format!(
                    "cluster watermark {watermark} regresses committed frontier {current}"
                ))
            }
            (Some(current), laminar_core::checkpoint::CheckpointWatermark::Uninitialized) => Err(
                format!("uninitialized watermark cannot replace committed frontier {current}"),
            ),
            _ => Ok(observed),
        }
    }

    pub(crate) async fn run_prepare_quorum(
        controller: &Arc<laminar_core::cluster::control::ClusterController>,
        quorum_timeout: Duration,
        request: PrepareQuorum<'_>,
    ) -> Result<
        (
            laminar_core::checkpoint::CheckpointWatermark,
            Vec<laminar_core::cluster::discovery::NodeId>,
            bool,
        ),
        String,
    > {
        let PrepareQuorum {
            attempt,
            local_watermark,
            assignment_fence,
            leader_proof,
            flags,
        } = request;
        if !controller.proof_is_live(leader_proof) {
            return Err("leader proof is stale before checkpoint Prepare".into());
        }
        let announcement = BarrierAnnouncement {
            epoch: attempt.epoch,
            checkpoint_id: attempt.checkpoint_id,
            assignment_fence: Some(assignment_fence.clone()),
            leader_proof: Some(leader_proof.clone()),
            phase: Phase::Prepare,
            flags,
        };
        let mut followers = assignment_fence
            .participants
            .iter()
            .map(|participant| laminar_core::cluster::discovery::NodeId(participant.node_id))
            .filter(|participant| *participant != controller.instance_id())
            .collect::<Vec<_>>();
        followers.sort_unstable_by_key(|participant| participant.0);

        let outcome = controller
            .wait_for_quorum(&announcement, &followers, quorum_timeout)
            .await;
        if !controller.proof_is_live(leader_proof) {
            return Err("leader proof expired during checkpoint Prepare".into());
        }
        match outcome {
            QuorumOutcome::Reached {
                follower_watermark,
                ref acks,
                handoff_replay_pending,
            } => {
                controller.note_responsive(acks);
                let watermark = if followers.is_empty() {
                    local_watermark
                } else {
                    local_watermark.cluster_min(follower_watermark)
                };
                Ok((
                    Self::validate_cluster_watermark_candidate(controller, watermark)?,
                    followers,
                    handoff_replay_pending,
                ))
            }
            QuorumOutcome::TimedOut { missing, .. } => {
                controller.note_unresponsive(&missing);
                Err(format!(
                    "checkpoint Prepare timed out waiting for {} participants",
                    missing.len()
                ))
            }
            QuorumOutcome::Failed { failures } => Err(format!(
                "checkpoint Prepare failed on {} participants: {}",
                failures.len(),
                failures
                    .first()
                    .map_or("unknown", |(_, message)| message.as_str())
            )),
        }
    }

    pub(super) async fn certify_follower_assignment_until(
        controller: &laminar_core::cluster::control::ClusterController,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        proof: &LeaderProof,
        deadline: tokio::time::Instant,
        context: &'static str,
    ) -> Result<(), DbError> {
        let certified = tokio::time::timeout_at(
            deadline,
            controller.checkpoint_assignment_fence_for_leader(fence.assignment_version, proof),
        )
        .await
        .map_err(|_| DbError::Checkpoint(format!("{context} authority validation timed out")))?;
        if certified.as_ref() != Some(fence) {
            return Err(DbError::Checkpoint(format!(
                "{context} authority is no longer current"
            )));
        }
        Ok(())
    }

    async fn validate_follower_prepare_context(
        controller: &laminar_core::cluster::control::ClusterController,
        request: &CheckpointRequest,
        announcement: &BarrierAnnouncement,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            laminar_core::checkpoint::CheckpointAssignmentFence,
            LeaderProof,
        ),
        DbError,
    > {
        require_canonical_attempt(
            CheckpointAttempt::new(announcement.epoch, announcement.checkpoint_id),
            "follower Prepare",
        )?;
        if announcement.phase != Phase::Prepare {
            return Err(DbError::Checkpoint(
                "follower checkpoint did not originate from Prepare".into(),
            ));
        }
        let fence = request.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("follower checkpoint has no assignment fence".into())
        })?;
        let proof = announcement
            .leader_proof
            .as_ref()
            .ok_or_else(|| DbError::Checkpoint("follower checkpoint has no leader proof".into()))?;
        if announcement.assignment_fence.as_ref() != Some(fence)
            || announcement.flags != request.flags
            || !fence.contains(controller.instance_id().0)
            || fence.participant_incarnation(proof.owner.node_id) != Some(proof.owner.boot_id)
        {
            return Err(DbError::Checkpoint(
                "follower Prepare does not match the certified assignment".into(),
            ));
        }
        Self::certify_follower_assignment_until(
            controller,
            fence,
            proof,
            deadline,
            "follower Prepare",
        )
        .await?;
        Ok((fence.clone(), proof.clone()))
    }

    /// Legacy direct follower entry point.
    ///
    /// This API owns no callback-supervised immutable capture tail, so it deliberately publishes
    /// `Captured` only after local phase-one packing/persistence has returned. The streaming
    /// runtime uses its early-capture path instead and acknowledges immediately after transferring
    /// the sealed capture into the supervised follower tail.
    pub async fn follower_checkpoint(
        &mut self,
        request: CheckpointRequest,
        announcement: BarrierAnnouncement,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::cluster::control::{BarrierAck, BarrierAckDisposition};

        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started) + self.config.checkpoint_timeout;
        let controller = self.cluster_controller.clone().ok_or_else(|| {
            DbError::Checkpoint("follower checkpoint has no cluster controller".into())
        })?;
        let (fence, proof) =
            Self::validate_follower_prepare_context(&controller, &request, &announcement, deadline)
                .await?;
        let handoff_replay_pending = request.handoff_replay_pending;
        let terminal_handoff =
            sink_epoch_admission::is_terminal_handoff(request.flags, handoff_replay_pending);
        let prepare_outcome = self
            .follower_prepare_acked_until(
                request,
                proof,
                announcement.epoch,
                announcement.checkpoint_id,
                deadline,
            )
            .await?;
        let captured_ack_error = match tokio::time::timeout_at(
            deadline,
            controller.ack_barrier(&BarrierAck {
                epoch: announcement.epoch,
                checkpoint_id: announcement.checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags: announcement.flags,
                disposition: if handoff_replay_pending {
                    BarrierAckDisposition::CapturedWithReplay
                } else {
                    BarrierAckDisposition::Captured
                },
                error: None,
                watermark: self.local_watermark,
            }),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(format!("follower captured ack failed: {error}")),
            Err(_) => Some("follower captured ack timed out".to_string()),
        };
        if let Some(error) = captured_ack_error.as_deref() {
            // The local prepared image and any phase-one sink state cannot be discarded merely
            // because the best-effort Captured notification was ambiguous. The leader may have
            // observed it and committed, so continue through exact terminal settlement.
            tracing::warn!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                %error,
                "follower Captured acknowledgement was not confirmed; awaiting authority"
            );
        }
        let required_settlement_deadline = deadline
            .checked_add(self.config.cleanup_timeout)
            .ok_or_else(|| DbError::Checkpoint("follower settlement deadline overflowed".into()))?;
        let decision_timeout = decision_timeout.max(
            required_settlement_deadline.saturating_duration_since(tokio::time::Instant::now()),
        );
        if prepare_outcome == FollowerPrepareOutcome::InDoubt {
            tracing::debug!(
                checkpoint_id = announcement.checkpoint_id,
                epoch = announcement.epoch,
                "preserving in-doubt follower preparation through terminal observation"
            );
        }
        let committed = match Self::await_follower_decision(
            &controller,
            announcement.epoch,
            announcement.checkpoint_id,
            &fence,
            decision_timeout,
        )
        .await
        {
            Ok(committed) => committed,
            Err(settlement) => {
                self.failure_requires_recovery = true;
                let message = captured_ack_error.map_or_else(
                    || settlement.to_string(),
                    |ack| format!("{ack}; terminal settlement failed: {settlement}"),
                );
                return Err(DbError::Checkpoint(message));
            }
        };
        let result = self
            .follower_finish(
                announcement.epoch,
                announcement.checkpoint_id,
                committed,
                started,
                terminal_handoff,
            )
            .await;
        if result.is_err() {
            self.failure_requires_recovery = true;
        }
        result
    }

    pub(crate) async fn await_follower_decision(
        controller: &laminar_core::cluster::control::ClusterController,
        epoch: u64,
        checkpoint_id: u64,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        decision_timeout: Duration,
    ) -> Result<bool, DbError> {
        use laminar_core::checkpoint_decision::CheckpointVerdict;
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower decision",
        )?;
        if !assignment_fence.is_canonical()
            || !assignment_fence.contains(controller.instance_id().0)
        {
            return Err(DbError::Checkpoint(
                "follower decision has an invalid assignment fence".into(),
            ));
        }
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
        })?;
        let deadline = tokio::time::Instant::now() + decision_timeout;
        loop {
            let settlement =
                tokio::time::timeout_at(deadline, authority.cluster_attempt_settlement(attempt))
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "follower decision timed out for checkpoint {checkpoint_id}"
                        ))
                    })?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("follower decision read failed: {error}"))
                    })?;
            if let Some(settlement) = settlement {
                let settled = CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id);
                match settled.relation_to(attempt) {
                    CheckpointAttemptRelation::Exact
                        if settlement.verdict == CheckpointVerdict::Abort =>
                    {
                        return Ok(false);
                    }
                    CheckpointAttemptRelation::Exact => {
                        let exact = tokio::time::timeout_at(
                            deadline,
                            authority.cluster_outcome_with_committed_checkpoint(epoch),
                        )
                        .await
                        .map_err(|_| {
                            DbError::Checkpoint("follower committed-index read timed out".into())
                        })?
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "follower committed-index read failed: {error}"
                            ))
                        })?
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "Commit outcome has no committed checkpoint index".into(),
                            )
                        })?;
                        let (outcome, index) = exact;
                        let index = index.ok_or_else(|| {
                            DbError::Checkpoint(
                                "Commit outcome has no committed checkpoint body".into(),
                            )
                        })?;
                        if outcome != settlement
                            || outcome.assignment_fence.as_ref() != Some(assignment_fence)
                            || index.epoch != epoch
                            || index.checkpoint_id != checkpoint_id
                            || index.assignment_fence.as_ref() != Some(assignment_fence)
                            || !index.participants.iter().any(|participant| {
                                participant.participant_id == controller.instance_id().0
                            })
                        {
                            return Err(DbError::Checkpoint(
                                "follower Commit does not match its prepared participant cut"
                                    .into(),
                            ));
                        }
                        index.validate().map_err(DbError::Checkpoint)?;
                        let source_watermarks = index
                            .effective_source_watermarks()
                            .map_err(DbError::Checkpoint)?;
                        controller
                            .publish_committed_checkpoint_progress(
                                &index.channel_progress,
                                &source_watermarks,
                            )
                            .map_err(DbError::Checkpoint)?;
                        return Ok(true);
                    }
                    CheckpointAttemptRelation::Newer => return Ok(false),
                    CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                        return Err(DbError::Checkpoint(
                            "follower observed an incompatible terminal checkpoint".into(),
                        ));
                    }
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(DbError::Checkpoint(format!(
                    "follower decision timed out for checkpoint {checkpoint_id}"
                )));
            }
            tokio::time::sleep(FOLLOWER_DECISION_POLL.min(remaining)).await;
        }
    }
}
