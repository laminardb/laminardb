#[cfg(debug_assertions)]
use super::attempt::checkpoint_kill_gate;
use super::{
    require_canonical_attempt, CheckpointAttempt, CheckpointCoordinator, CheckpointPhase,
    CheckpointRequest, DbError, FollowerPrepareOutcome, LeaderProof,
};

impl CheckpointCoordinator {
    pub(crate) async fn follower_prepare_acked_until(
        &mut self,
        request: CheckpointRequest,
        leader_proof: LeaderProof,
        epoch: u64,
        checkpoint_id: u64,
        deadline: tokio::time::Instant,
    ) -> Result<FollowerPrepareOutcome, DbError> {
        use laminar_core::cluster::control::{BarrierAck, BarrierAckDisposition};

        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower Prepare",
        )?;
        let controller = self.cluster_controller.clone().ok_or_else(|| {
            DbError::Checkpoint("follower Prepare has no cluster controller".into())
        })?;
        let fence = request.assignment_fence.clone().ok_or_else(|| {
            DbError::Checkpoint("follower Prepare has no assignment fence".into())
        })?;
        Self::certify_follower_assignment_until(
            &controller,
            &fence,
            &leader_proof,
            deadline,
            "follower Prepare",
        )
        .await?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
        })?;
        let expected_inventory =
            self.checkpoint_artifact_inventory(attempt, Some(fence.clone()))?;
        let active_admission =
            tokio::time::timeout_at(deadline, authority.cluster_checkpoint_artifact_admission())
                .await
                .map_err(|_| {
                    DbError::Checkpoint("follower artifact admission read timed out".into())
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!("follower artifact admission read failed: {error}"))
                })?;
        if active_admission.as_ref() != Some(&(expected_inventory, leader_proof)) {
            return Err(DbError::Checkpoint(
                "follower checkpoint has no exact durable artifact and leader-term admission"
                    .into(),
            ));
        }
        self.consume_follower_sink_epoch_until(attempt, deadline)
            .await?;
        let flags = request.flags;
        self.allocator.advance_epoch_to(epoch);
        self.phase = CheckpointPhase::PreCommitting;
        let descriptors = self.pre_commit_sinks_until(epoch, deadline).await;
        let (prepared, persistence_in_doubt) = match descriptors {
            Ok(descriptors) => match self
                .pack_checkpoint(attempt, request, descriptors, deadline)
                .await
            {
                Ok(packed) => (
                    self.persist_checkpoint_until(&packed, deadline)
                        .await
                        .map(|_| ()),
                    true,
                ),
                Err(error) => (Err(error), false),
            },
            Err(error) => (Err(error), false),
        };
        if let Err(error) = prepared {
            if persistence_in_doubt {
                // A timed-out/failed Create may already be visible. After Captured quorum the
                // leader is permitted to prove Commit from that exact manifest, so rolling back
                // phase-one sink state or superseding the cached Captured acknowledgement here
                // could contradict the authoritative outcome. Keep the retained prepared image
                // and let the normal decision path commit or abort it.
                tracing::warn!(
                    checkpoint_id,
                    epoch,
                    %error,
                    "follower manifest persistence is in doubt; awaiting authoritative decision"
                );
                self.phase = CheckpointPhase::Idle;
                return Ok(FollowerPrepareOutcome::InDoubt);
            }
            let acknowledgement = BarrierAck {
                epoch,
                checkpoint_id,
                assignment_digest: Some(fence.digest()),
                flags,
                disposition: BarrierAckDisposition::Failed,
                error: Some(error.to_string()),
                watermark: self.local_watermark,
            };
            // Once local phase one has failed before persistence starts, rollback is both safe
            // and required. A slow best-effort Failed acknowledgement must not consume the
            // attempt's remaining budget and strand the coordinator in PreCommitting, so give
            // rollback its private cleanup window and run the notification alongside it.
            let cleanup_deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
            let notify = async {
                let _ = tokio::time::timeout_at(
                    cleanup_deadline,
                    controller.ack_barrier(&acknowledgement),
                )
                .await;
            };
            let rollback = self.rollback_sinks_until(epoch, cleanup_deadline);
            let ((), rollback) = tokio::join!(notify, rollback);
            self.phase = CheckpointPhase::Idle;
            if let Err(rollback) = rollback {
                self.failure_requires_recovery = true;
                return Err(DbError::Checkpoint(format!(
                    "follower Prepare failed ({error}); rollback also failed ({rollback})"
                )));
            }
            // The durable active inventory owns every ambiguous Create until coordinated Abort
            // replaces the exact paths with permanent seals. Deleting here would reopen a path
            // for a late writer and discard manifest evidence needed to locate a candidate index.
            return Err(error);
        }
        #[cfg(debug_assertions)]
        checkpoint_kill_gate(
            "follower",
            attempt,
            self.last_committed_ref
                .as_ref()
                .map(|reference| (reference.checkpoint_id, reference.epoch)),
        )
        .await;
        self.phase = CheckpointPhase::Idle;
        Ok(FollowerPrepareOutcome::Prepared)
    }
}
