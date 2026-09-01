//! Mechanically extracted coordinator responsibility.

use super::{
    AlignedCheckpointContext, BarrierOutcome, CheckpointAttempt, CheckpointBarrier,
    CheckpointCleanupOwner, CycleError, PipelineCallback, SourceCheckpoint, StreamingCoordinator,
};

impl StreamingCoordinator {
    /// Handle a barrier from a source.
    pub(super) async fn handle_barrier(
        &mut self,
        source_idx: usize,
        barrier: &CheckpointBarrier,
        barrier_checkpoint: &SourceCheckpoint,
        callback: &mut impl PipelineCallback,
    ) -> Result<(), CycleError> {
        let barrier_attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
        if !self.pending_barrier.active || self.pending_barrier.attempt != Some(barrier_attempt) {
            self.release_source_barrier_for(source_idx, barrier_attempt);
            return Ok(());
        }
        if self.pending_barrier.flags != barrier.flags {
            let reason = format!(
                "source barrier flags {:#x} do not match admitted checkpoint flags {:#x}",
                barrier.flags, self.pending_barrier.flags
            );
            self.cancel_pending_barrier_for_stop(callback, &reason, true)
                .await
                .map_err(CycleError::Recovery)?;
            return Err(CycleError::Recovery(reason));
        }
        #[cfg(feature = "cluster")]
        self.require_process_authority("source barrier handling")
            .map_err(|error| CycleError::Recovery(error.to_string()))?;

        let source_name = self.source_names.get(source_idx).ok_or_else(|| {
            CycleError::Recovery(format!(
                "source barrier referenced unknown runtime index {source_idx}"
            ))
        })?;
        callback.reconcile_source_input_channels(
            source_name,
            barrier_checkpoint.input_channels_arc().cloned(),
        )?;
        self.capture_replayable_barrier_cursor(source_idx, barrier_checkpoint);

        self.pending_barrier.sources_aligned.insert(source_idx);

        if self.pending_barrier.sources_aligned.len() >= self.pending_barrier.sources_total {
            tracing::info!(
                checkpoint_id = barrier_attempt.checkpoint_id,
                epoch = barrier_attempt.epoch,
                sources_total = self.pending_barrier.sources_total,
                elapsed = ?self.pending_barrier.started_at.elapsed(),
                "checkpoint source barriers fully aligned"
            );
            let checkpoints = std::mem::take(&mut self.pending_barrier.source_checkpoints);
            // Clone for fan-out so each source gets the exact checkpoint that was persisted.
            let fan_out = checkpoints.clone();
            let attempt = barrier_attempt;
            let attempt_started = self.pending_barrier.started_at;
            let flags = self.pending_barrier.flags;
            let assignment_fence = self.pending_barrier.assignment_fence.clone();
            let cleanup_owner = self.pending_barrier.cleanup_owner;
            let attempt_deadline = self
                .pending_barrier
                .attempt_deadline(self.config.checkpoint_timeout);
            self.pending_barrier.clear();
            if let Err(error) = callback
                .drain_checkpoint_edges_until(attempt_deadline)
                .await
            {
                let error = match error {
                    CycleError::Halt(error) => CycleError::Halt(error),
                    CycleError::Fatal(error) | CycleError::Recovery(error) => {
                        CycleError::Recovery(error)
                    }
                };
                let reason = error.to_string();
                let cleanup = self
                    .handle_aligned_checkpoint_outcome(
                        callback,
                        BarrierOutcome::Failed,
                        AlignedCheckpointContext {
                            cleanup_owner,
                            attempt,
                            started_at: attempt_started,
                            flags,
                            assignment_fence,
                        },
                        &fan_out,
                    )
                    .await;
                if let Err(cleanup) = cleanup {
                    let combined = format!("{reason}; checkpoint cleanup failed: {cleanup}");
                    return Err(if matches!(&error, CycleError::Halt(_)) {
                        CycleError::Halt(combined)
                    } else {
                        CycleError::Recovery(combined)
                    });
                }
                return Err(error);
            }
            tracing::info!(
                checkpoint_id = attempt.checkpoint_id,
                epoch = attempt.epoch,
                elapsed = ?attempt_started.elapsed(),
                "checkpoint pre-capture graph drain completed"
            );
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("aligned checkpoint capture") {
                let reason = error.to_string();
                let cleanup = Self::cleanup_checkpoint_attempt(
                    callback,
                    cleanup_owner,
                    attempt,
                    &reason,
                    flags,
                    assignment_fence.clone(),
                )
                .await;
                self.fail_manual_attempt(attempt, &reason);
                self.release_source_barrier_attempt(attempt);
                cleanup.map_err(|cleanup| {
                    CycleError::Recovery(format!("{reason}; checkpoint cleanup failed: {cleanup}"))
                })?;
                return Err(CycleError::Recovery(reason));
            }
            if let Err(error) = callback.reserve_subscription_cut(attempt) {
                self.handle_aligned_checkpoint_outcome(
                    callback,
                    BarrierOutcome::Failed,
                    AlignedCheckpointContext {
                        cleanup_owner,
                        attempt,
                        started_at: attempt_started,
                        flags,
                        assignment_fence,
                    },
                    &fan_out,
                )
                .await
                .map_err(|cleanup| {
                    CycleError::Recovery(format!("{error}; checkpoint cleanup failed: {cleanup}"))
                })?;
                return Err(CycleError::Recovery(error));
            }
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("aligned checkpoint capture start") {
                let reason = error.to_string();
                callback.abort_subscription_cut(attempt);
                let cleanup = Self::cleanup_checkpoint_attempt(
                    callback,
                    cleanup_owner,
                    attempt,
                    &reason,
                    flags,
                    assignment_fence.clone(),
                )
                .await;
                self.fail_manual_attempt(attempt, &reason);
                self.release_source_barrier_attempt(attempt);
                cleanup.map_err(|cleanup| {
                    CycleError::Recovery(format!("{reason}; checkpoint cleanup failed: {cleanup}"))
                })?;
                return Err(CycleError::Recovery(reason));
            }
            let outcome = callback
                .checkpoint_with_barrier(
                    checkpoints,
                    attempt,
                    attempt_started,
                    attempt_deadline,
                    flags,
                    assignment_fence.clone(),
                )
                .await;
            let topology_cancelled = matches!(&outcome, BarrierOutcome::CancelledBeforeCapture);
            let durable_tail_pending = matches!(&outcome, BarrierOutcome::Async);
            let cleanup = self
                .handle_aligned_checkpoint_outcome(
                    callback,
                    outcome,
                    AlignedCheckpointContext {
                        cleanup_owner,
                        attempt,
                        started_at: attempt_started,
                        flags,
                        assignment_fence,
                    },
                    &fan_out,
                )
                .await;
            if let Err(cleanup) = cleanup {
                if let Some(halt) = callback.take_pipeline_halt() {
                    return Err(CycleError::Halt(format!(
                        "{halt}; checkpoint cleanup also failed: {cleanup}"
                    )));
                }
                return Err(CycleError::Recovery(cleanup));
            }
            if let Some(error) = callback.take_pipeline_halt() {
                return Err(CycleError::Halt(error));
            }
            if let Some(error) = callback.take_pipeline_fault() {
                return Err(CycleError::Recovery(error));
            }
            // Capture or exact cleanup has completed. A cleanup failure or sticky replay fault
            // returns above and deliberately leaves the sources held for coordinated recovery.
            self.release_source_barrier_attempt(attempt);
            if topology_cancelled && cleanup_owner == CheckpointCleanupOwner::Originator {
                self.defer_checkpoint_until_topology_ready();
            } else if !topology_cancelled && !durable_tail_pending {
                self.advance_checkpoint_cadence();
            }
        }
        Ok(())
    }
}
