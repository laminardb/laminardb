use std::time::Instant;

use laminar_core::checkpoint::CheckpointAttemptRelation;

use super::{
    checked_successor_epoch, require_canonical_attempt, CheckpointAttempt, CheckpointCoordinator,
    CheckpointPhase, DbError, SinkEpochPublication,
};

impl CheckpointCoordinator {
    pub(crate) async fn follower_finish(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
        started: Instant,
        terminal_handoff: bool,
    ) -> Result<bool, DbError> {
        self.follower_finish_with_publication(
            epoch,
            checkpoint_id,
            committed,
            started,
            terminal_handoff,
            SinkEpochPublication::Immediate,
        )
        .await
    }

    pub(crate) async fn follower_finish_deferred(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
        started: Instant,
        terminal_handoff: bool,
    ) -> Result<bool, DbError> {
        self.follower_finish_with_publication(
            epoch,
            checkpoint_id,
            committed,
            started,
            terminal_handoff,
            SinkEpochPublication::DeferredToTail,
        )
        .await
    }

    async fn install_authoritative_follower_commit_until(
        &mut self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let (manifest, manifest_bytes) = self.prepared.get(&attempt).cloned().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "follower checkpoint {} has no prepared manifest",
                attempt.checkpoint_id
            ))
        })?;
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("follower completion has no cluster controller".into())
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
        })?;
        let (outcome, index) = tokio::time::timeout_at(
            deadline,
            authority.cluster_outcome_with_committed_checkpoint(attempt.epoch),
        )
        .await
        .map_err(|_| DbError::Checkpoint("follower Commit verification timed out".into()))?
        .map_err(|error| {
            DbError::Checkpoint(format!("follower Commit verification failed: {error}"))
        })?
        .ok_or_else(|| DbError::Checkpoint("follower Commit disappeared".into()))?;
        let index = index.ok_or_else(|| {
            DbError::Checkpoint("follower Commit has no committed checkpoint index".into())
        })?;
        let reference = outcome.committed_checkpoint.clone().ok_or_else(|| {
            DbError::Checkpoint("follower Commit has no committed checkpoint reference".into())
        })?;
        let source_watermarks = index
            .effective_source_watermarks()
            .map_err(DbError::Checkpoint)?;
        let participant = index
            .participants
            .iter()
            .find(|participant| participant.participant_id == self.store.participant_id())
            .ok_or_else(|| {
                DbError::Checkpoint("follower is absent from committed participant set".into())
            })?;
        participant
            .verify_manifest(manifest.as_ref(), &manifest_bytes)
            .map_err(DbError::Checkpoint)?;
        self.last_committed_ref = Some(reference.clone());
        self.last_committed_source_watermarks = Some((reference, source_watermarks));
        self.last_committed_manifest = Some(manifest);
        self.prepared.remove(&attempt);
        Ok(())
    }

    async fn verify_authoritative_follower_abort_until(
        &mut self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("follower completion has no cluster controller".into())
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("follower checkpoint authority: {error}"))
        })?;
        let settlement =
            tokio::time::timeout_at(deadline, authority.cluster_attempt_settlement(attempt))
                .await
                .map_err(|_| DbError::Checkpoint("follower Abort verification timed out".into()))?
                .map_err(|error| {
                    DbError::Checkpoint(format!("follower Abort verification failed: {error}"))
                })?
                .ok_or_else(|| DbError::Checkpoint("follower Abort is unresolved".into()))?;
        let settled = CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id);
        match settled.relation_to(attempt) {
            CheckpointAttemptRelation::Exact
                if settlement.verdict
                    == laminar_core::checkpoint_decision::CheckpointVerdict::Abort => {}
            CheckpointAttemptRelation::Newer => {}
            _ => {
                return Err(DbError::Checkpoint(
                    "follower cannot discard a checkpoint without an authoritative Abort or \
                     superseding terminal outcome"
                        .into(),
                ));
            }
        }
        self.rollback_sinks_until(attempt.epoch, deadline).await?;
        self.failure_requires_recovery = true;
        Ok(())
    }

    async fn follower_finish_with_publication(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
        committed: bool,
        started: Instant,
        terminal_handoff: bool,
        sink_epoch_publication: SinkEpochPublication,
    ) -> Result<bool, DbError> {
        let attempt = require_canonical_attempt(
            CheckpointAttempt::new(epoch, checkpoint_id),
            "follower completion",
        )?;
        let deadline = tokio::time::Instant::now() + self.config.cleanup_timeout;
        if committed {
            self.install_authoritative_follower_commit_until(attempt, deadline)
                .await?;
        } else {
            self.verify_authoritative_follower_abort_until(attempt, deadline)
                .await?;
        }
        self.clear_sink_artifact_intents(attempt);
        self.allocator.advance_epoch_to(checked_successor_epoch(
            epoch,
            "closing a follower checkpoint",
        )?);
        let continuation = if !self.failure_requires_recovery
            && self.has_checkpoint_committable_sinks()
            && !(committed && terminal_handoff)
        {
            self.begin_sink_epoch_until(deadline, sink_epoch_publication)
                .await
        } else {
            Ok(())
        };
        let checkpoint_bytes = if committed {
            self.last_committed_manifest
                .as_ref()
                .map(|manifest| manifest.node_data.object_length)
        } else {
            None
        };
        self.phase = CheckpointPhase::Idle;
        self.record_checkpoint_outcome(committed, attempt, started.elapsed(), checkpoint_bytes);
        if continuation.is_err() {
            self.failure_requires_recovery = true;
        }
        continuation?;
        Ok(committed)
    }
}
