//! Mechanically extracted coordinator responsibility.

use super::{
    BarrierOutcome, CheckpointAdmission, CheckpointAssignmentAdmission, CheckpointAttempt,
    CheckpointBarrier, CheckpointCleanupOwner, CheckpointControlOutcome, CoordinatorRunState,
    Duration, FxHashMap, Instant, Ordering, PipelineCallback, StreamingCoordinator,
    CHECKPOINT_RETRY_BASE, CHECKPOINT_RETRY_MAX,
};

impl StreamingCoordinator {
    pub(super) fn checkpoint_capacity_available(&self) -> bool {
        !self.pending_barrier.active && self.checkpoint_in_flight.load(Ordering::Acquire) < 1
    }

    pub(super) fn advance_checkpoint_cadence(&mut self) {
        self.last_checkpoint = Instant::now();
        self.checkpoint_retry_not_before = None;
        self.checkpoint_retry_backoff = Duration::ZERO;
    }

    pub(super) fn observe_intake_gate_for_checkpoint_cadence(
        &mut self,
        state: &mut CoordinatorRunState,
        intake_paused: bool,
    ) {
        if state.intake_was_paused && !intake_paused {
            // Recovery and terminal HANDOFF holds can outlive the configured periodic interval.
            // Restart that interval at the reopen boundary so the first ordinary cut does not
            // immediately absorb the release backlog. Manual requests bypass the interval in
            // `checkpoint_admission` and therefore remain topology-control responsive.
            self.advance_checkpoint_cadence();
        }
        state.intake_was_paused = intake_paused;
    }

    pub(super) fn defer_checkpoint_until_topology_ready(&mut self) {
        let backoff = if self.checkpoint_retry_backoff.is_zero() {
            CHECKPOINT_RETRY_BASE
        } else {
            self.checkpoint_retry_backoff
                .saturating_mul(2)
                .min(CHECKPOINT_RETRY_MAX)
        };
        self.checkpoint_retry_backoff = backoff;
        self.checkpoint_retry_not_before = Some(Instant::now() + backoff);
    }

    pub(super) async fn checkpoint_admission(
        &mut self,
        callback: &mut impl PipelineCallback,
    ) -> Option<CheckpointAdmission> {
        self.prune_manual_requests();
        // Requests arriving after a manual attempt was admitted belong to a later cut. Never let
        // an intervening periodic attempt consume them or attach them to the active attempt.
        if !self.manual_waiting.is_empty() && self.manual_active.is_some() {
            return None;
        }
        let manual = !self.manual_waiting.is_empty();
        let leader = callback.is_leader();
        if manual && !leader {
            self.fail_waiting_manual("only the cluster leader may admit a manual checkpoint");
            return None;
        }

        #[cfg(feature = "cluster")]
        let output_checkpoint_due = callback.external_output_pressure().checkpoint_due();
        #[cfg(not(feature = "cluster"))]
        let output_checkpoint_due = false;
        let interval = leader
            && self
                .config
                .checkpoint_schedule
                .periodic_interval()
                .is_some_and(|value| {
                    output_checkpoint_due || self.last_checkpoint.elapsed() >= value
                });
        let retry_ready = self
            .checkpoint_retry_not_before
            .is_none_or(|deadline| Instant::now() >= deadline);
        if !manual && (!interval || !retry_ready) {
            return None;
        }

        // Every trigger observes the same recovery and assignment fence. Periodic work remains
        // due through `last_checkpoint`; a caller waiting on a manual request gets a prompt
        // rejection without burning an exact attempt ID.
        if callback.is_recovering() {
            if manual {
                self.fail_waiting_manual(
                    "manual checkpoint rejected while coordinated recovery is in progress",
                );
            }
            return None;
        }
        let default_deadline = tokio::time::Instant::now() + self.config.checkpoint_timeout;
        let deadline = if manual {
            self.manual_deadline(default_deadline)
        } else {
            default_deadline
        };
        let assignment_admission = callback.checkpoint_assignment_for_admission(deadline).await;
        // Durable assignment admission may block. A timed-out or cancelled waiter must not own a
        // later attempt merely because the readiness result raced its cancellation.
        self.prune_manual_requests();
        if tokio::time::Instant::now() >= deadline || (manual && self.manual_waiting.is_empty()) {
            return None;
        }
        let (assignment_fence, flags, assignment_guard) = match assignment_admission {
            CheckpointAssignmentAdmission::Ready {
                assignment_fence,
                flags,
                assignment_guard,
            } => (assignment_fence, flags, assignment_guard),
            CheckpointAssignmentAdmission::Deferred(reason) => {
                tracing::debug!(reason = %reason, "checkpoint admission waits for stable topology");
                self.defer_checkpoint_until_topology_ready();
                if manual {
                    self.fail_waiting_manual(format!(
                        "[LDB-6056] manual checkpoint rejected: {reason}"
                    ));
                }
                return None;
            }
            CheckpointAssignmentAdmission::Fault(reason) => {
                callback.record_checkpoint_admission_failure(&reason);
                if manual {
                    self.fail_waiting_manual(format!(
                        "[LDB-6056] manual checkpoint rejected: {reason}"
                    ));
                }
                return None;
            }
        };
        // HANDOFF is a topology-control cut owned by the explicit rebalance request. A periodic
        // attempt that observes the same assignment fence must leave the cadence due and defer;
        // otherwise it can commit the handoff, fence intake, and leave the manual owner waiting
        // for a second reservation that can no longer be admitted.
        if !manual && flags & laminar_core::checkpoint::flags::HANDOFF != 0 {
            tracing::debug!(
                "periodic checkpoint admission deferred to the assignment handoff owner"
            );
            return None;
        }
        if manual
            && self.manual_handoff_required
            && flags & laminar_core::checkpoint::flags::HANDOFF == 0
        {
            let reason = "assignment handoff ended before its replay-quiescent checkpoint";
            callback.record_checkpoint_admission_failure(reason);
            self.fail_waiting_manual(reason);
            return None;
        }
        if !self.checkpoint_capacity_available() {
            return None;
        }
        Some(CheckpointAdmission {
            manual,
            flags,
            assignment_fence,
            assignment_guard,
            deadline,
        })
    }

    pub(super) async fn reserve_prepared_checkpoint_attempt(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt_started: Instant,
    ) -> Result<CheckpointAttempt, String> {
        self.prune_manual_requests();
        if admission.manual && self.manual_waiting.is_empty() {
            return Err("manual checkpoint was cancelled before exact attempt reservation".into());
        }
        if tokio::time::Instant::now() >= admission.deadline {
            return Err("checkpoint deadline expired before exact attempt reservation".into());
        }
        if admission.manual && !self.claim_waiting_manual_requests() {
            return Err("manual checkpoint was cancelled before exact attempt reservation".into());
        }
        // The claim above is the public cancellation boundary. The allocator is hard-bounded by
        // this admission deadline; whether it returns an exact ID or an error, claimed callers now
        // wait for the coordinator's terminal reply.
        let attempt = callback
            .reserve_checkpoint_attempt(admission.deadline)
            .await?;
        // Durable reservation succeeded. Install the exact attempt before notifying claimed
        // callers; Prepare and every later failure path must publish or abandon it before reply.
        if admission.manual && !self.activate_manual_attempt(attempt, admission.flags) {
            let reason = self
                .abandon_reserved_checkpoint_attempt(
                    callback,
                    admission,
                    attempt,
                    "manual checkpoint claim disappeared after exact attempt reservation".into(),
                    "reserved",
                )
                .await;
            return Err(reason);
        }
        tracing::info!(
            checkpoint_id = attempt.checkpoint_id,
            epoch = attempt.epoch,
            "checkpoint attempt reserved"
        );
        if tokio::time::Instant::now() >= admission.deadline {
            let reason = self
                .abandon_reserved_checkpoint_attempt(
                    callback,
                    admission,
                    attempt,
                    "checkpoint deadline expired after exact attempt reservation".into(),
                    "reserved",
                )
                .await;
            return Err(reason);
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint prepare publication") {
            let reason = self
                .abandon_reserved_checkpoint_attempt(
                    callback,
                    admission,
                    attempt,
                    error.to_string(),
                    "reserved",
                )
                .await;
            return Err(reason);
        }
        if let Err(error) = callback
            .publish_checkpoint_prepare(
                attempt,
                attempt_started,
                admission.deadline,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await
        {
            let reason = self
                .abandon_reserved_checkpoint_attempt(
                    callback, admission, attempt, error, "reserved",
                )
                .await;
            return Err(reason);
        }
        self.prune_manual_requests();
        if tokio::time::Instant::now() >= admission.deadline {
            let reason = self
                .abandon_reserved_checkpoint_attempt(
                    callback,
                    admission,
                    attempt,
                    "checkpoint deadline expired after Prepare publication".into(),
                    "prepared",
                )
                .await;
            return Err(reason);
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint prepare completion") {
            let reason = self
                .abandon_reserved_checkpoint_attempt(
                    callback,
                    admission,
                    attempt,
                    error.to_string(),
                    "prepared",
                )
                .await;
            return Err(reason);
        }
        Ok(attempt)
    }

    pub(super) async fn abandon_reserved_checkpoint_attempt(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt: CheckpointAttempt,
        reason: String,
        cleanup_phase: &str,
    ) -> String {
        let terminal_reason = match callback
            .abandon_checkpoint_attempt(
                attempt,
                &reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await
        {
            Ok(()) => reason,
            Err(cleanup) => {
                format!("{reason}; {cleanup_phase} checkpoint cleanup failed: {cleanup}")
            }
        };
        self.fail_manual_attempt(attempt, &terminal_reason);
        terminal_reason
    }

    pub(super) async fn handle_source_less_checkpoint_outcome(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt: CheckpointAttempt,
        outcome: BarrierOutcome,
    ) -> Result<(), String> {
        let (cleanup_reason, manual_reason, record_failure) = match outcome {
            BarrierOutcome::Committed(epoch) if epoch == attempt.epoch => {
                #[cfg(feature = "cluster")]
                if let Err(error) =
                    self.require_process_authority("source-less checkpoint publication")
                {
                    let reason = error.to_string();
                    callback.abort_subscription_cut(attempt);
                    self.fail_manual_attempt(attempt, &reason);
                    return Err(reason);
                }
                let publication_error = callback.publish_barrier(attempt).err();
                self.broadcast_epoch_committed(epoch, &FxHashMap::default());
                self.finish_manual_success(
                    attempt,
                    &crate::checkpoint_coordinator::CheckpointResult {
                        success: true,
                        checkpoint_id: attempt.checkpoint_id,
                        epoch: attempt.epoch,
                        duration: Duration::ZERO,
                        error: None,
                        failure_disposition: None,
                    },
                );
                return publication_error.map_or(Ok(()), Err);
            }
            BarrierOutcome::Committed(epoch) => {
                let reason = format!(
                    "checkpoint callback committed epoch {epoch} for reserved epoch {}",
                    attempt.epoch
                );
                (reason.clone(), reason, true)
            }
            BarrierOutcome::Async => return Ok(()),
            BarrierOutcome::Skipped(reason) => {
                tracing::debug!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason = %reason,
                    "source-less checkpoint skipped"
                );
                (
                    reason.to_string(),
                    format!("manual checkpoint skipped: {reason}"),
                    false,
                )
            }
            BarrierOutcome::CancelledBeforeCapture => {
                let reason = "checkpoint topology closed before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Aborted => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    "source-less checkpoint was authoritatively aborted"
                );
                let reason =
                    "checkpoint was aborted by authoritative cluster control before state capture";
                (reason.into(), format!("manual {reason}"), false)
            }
            BarrierOutcome::Failed => (
                "source-less checkpoint failed before durable tail".into(),
                "manual source-less checkpoint failed before the durable tail".into(),
                true,
            ),
        };
        callback.abort_subscription_cut(attempt);
        Self::cleanup_checkpoint_attempt(
            callback,
            CheckpointCleanupOwner::Originator,
            attempt,
            &cleanup_reason,
            admission.flags,
            admission.assignment_fence.clone(),
        )
        .await?;
        if record_failure {
            callback.record_checkpoint_failure(attempt.checkpoint_id, &cleanup_reason);
        }
        self.fail_manual_attempt(attempt, manual_reason);
        Ok(())
    }

    pub(super) async fn admit_source_less_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &mut CheckpointAdmission,
    ) {
        let attempt_started = Instant::now();
        let attempt = match self
            .reserve_prepared_checkpoint_attempt(callback, admission, attempt_started)
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => {
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        // Prepare is the source-less attempt's durable assignment boundary. Release the admission
        // claim before graph drain/capture: `checkpoint_with_barrier` takes the separate fair
        // rotation read fence and a queued assignment writer would otherwise create a nested-read
        // deadlock.
        drop(admission.assignment_guard.take());
        self.complete_prepared_source_less_checkpoint(
            callback,
            admission,
            attempt,
            attempt_started,
        )
        .await;
    }

    pub(super) async fn complete_prepared_source_less_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt: CheckpointAttempt,
        attempt_started: Instant,
    ) {
        if tokio::time::Instant::now() >= admission.deadline {
            if let Err(error) = self
                .handle_source_less_checkpoint_outcome(
                    callback,
                    admission,
                    attempt,
                    BarrierOutcome::Failed,
                )
                .await
            {
                callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
            }
            self.advance_checkpoint_cadence();
            return;
        }
        let attempt_deadline = admission.deadline;
        if let Err(error) = callback
            .drain_checkpoint_edges_until(attempt_deadline)
            .await
        {
            tracing::error!(%error, "source-less checkpoint graph drain failed");
            if let Err(cleanup_error) = self
                .handle_source_less_checkpoint_outcome(
                    callback,
                    admission,
                    attempt,
                    BarrierOutcome::Failed,
                )
                .await
            {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("checkpoint cleanup failed after graph drain: {cleanup_error}"),
                );
            }
            self.advance_checkpoint_cadence();
            return;
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source-less checkpoint capture") {
            let reason = error.to_string();
            let cleanup = Self::cleanup_checkpoint_attempt(
                callback,
                CheckpointCleanupOwner::Originator,
                attempt,
                &reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
            self.fail_manual_attempt(attempt, &reason);
            callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
            if let Err(cleanup_error) = cleanup {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{reason}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            self.advance_checkpoint_cadence();
            return;
        }
        if let Err(error) = callback.reserve_subscription_cut(attempt) {
            tracing::error!(%error, "source-less subscription cut reservation failed");
            if let Err(cleanup_error) = self
                .handle_source_less_checkpoint_outcome(
                    callback,
                    admission,
                    attempt,
                    BarrierOutcome::Failed,
                )
                .await
            {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{error}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            self.advance_checkpoint_cadence();
            return;
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source-less checkpoint capture start") {
            let reason = error.to_string();
            callback.abort_subscription_cut(attempt);
            let cleanup = Self::cleanup_checkpoint_attempt(
                callback,
                CheckpointCleanupOwner::Originator,
                attempt,
                &reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
            self.fail_manual_attempt(attempt, &reason);
            callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
            if let Err(cleanup_error) = cleanup {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{reason}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            self.advance_checkpoint_cadence();
            return;
        }
        let outcome = callback
            .checkpoint_with_barrier(
                FxHashMap::default(),
                attempt,
                attempt_started,
                attempt_deadline,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
        let retry_after_topology_change =
            matches!(&outcome, BarrierOutcome::CancelledBeforeCapture);
        let durable_tail_pending = matches!(&outcome, BarrierOutcome::Async);
        if let Err(error) = self
            .handle_source_less_checkpoint_outcome(callback, admission, attempt, outcome)
            .await
        {
            callback.record_checkpoint_continuation_fault(attempt, &error);
        }
        if retry_after_topology_change {
            self.defer_checkpoint_until_topology_ready();
        } else if !durable_tail_pending {
            self.advance_checkpoint_cadence();
        }
    }

    pub(super) async fn admit_source_barrier_checkpoint(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &mut CheckpointAdmission,
    ) {
        if self
            .source_handles
            .iter()
            .any(|handle| !handle.barrier_injector.can_trigger())
        {
            tracing::debug!(
                "checkpoint admission deferred: a source barrier injector is still busy"
            );
            return;
        }

        let attempt_started = Instant::now();
        let attempt = match self
            .reserve_prepared_checkpoint_attempt(callback, admission, attempt_started)
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => {
                callback.record_checkpoint_admission_failure(&error);
                if admission.manual {
                    self.fail_waiting_manual(format!(
                        "manual checkpoint attempt reservation failed: {error}"
                    ));
                }
                return;
            }
        };
        self.inject_prepared_source_barrier_attempt(callback, admission, attempt, attempt_started)
            .await;
        // Every source has now accepted (or the attempt has synchronously cleaned up) the exact
        // prepared barrier command. Drain activation may proceed without changing this attempt's
        // predecessor binding.
        drop(admission.assignment_guard.take());
    }

    pub(super) async fn inject_prepared_source_barrier_attempt(
        &mut self,
        callback: &mut impl PipelineCallback,
        admission: &CheckpointAdmission,
        attempt: CheckpointAttempt,
        attempt_started: Instant,
    ) {
        if tokio::time::Instant::now() >= admission.deadline {
            let reason = "checkpoint deadline expired before source barrier injection";
            let cleanup = Self::cleanup_checkpoint_attempt(
                callback,
                CheckpointCleanupOwner::Originator,
                attempt,
                reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
            self.fail_manual_attempt(attempt, reason);
            callback.record_checkpoint_failure(attempt.checkpoint_id, reason);
            if let Err(error) = cleanup {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{reason}; checkpoint cleanup failed: {error}"),
                );
            }
            return;
        }
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("source barrier injection") {
            let reason = error.to_string();
            let cleanup = Self::cleanup_checkpoint_attempt(
                callback,
                CheckpointCleanupOwner::Originator,
                attempt,
                &reason,
                admission.flags,
                admission.assignment_fence.clone(),
            )
            .await;
            self.fail_manual_attempt(attempt, &reason);
            callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
            if let Err(cleanup_error) = cleanup {
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    &format!("{reason}; checkpoint cleanup failed: {cleanup_error}"),
                );
            }
            return;
        }
        self.pending_barrier.reset_with_assignment(
            attempt,
            self.source_handles.len(),
            admission.flags,
            admission.assignment_fence.clone(),
            Some(admission.deadline),
        );
        // Attempt time includes reservation, alignment, capture, quorum, and publication.
        self.pending_barrier.started_at = attempt_started;
        let barrier = CheckpointBarrier {
            checkpoint_id: attempt.checkpoint_id,
            epoch: attempt.epoch,
            flags: admission.flags,
        };

        for handle in &self.source_handles {
            if !handle.barrier_injector.trigger(barrier) {
                self.pending_barrier.clear();
                self.cancel_local_source_barriers(barrier);
                let cleanup = Self::cleanup_checkpoint_attempt(
                    callback,
                    CheckpointCleanupOwner::Originator,
                    attempt,
                    "source barrier injection was rejected after preflight",
                    admission.flags,
                    admission.assignment_fence.clone(),
                )
                .await;
                if cleanup.is_ok() {
                    self.release_source_barrier_attempt(attempt);
                } else if let Err(error) = cleanup {
                    callback.record_checkpoint_failure(
                        attempt.checkpoint_id,
                        &format!("source barrier injection cleanup failed: {error}"),
                    );
                }
                callback.record_checkpoint_failure(
                    attempt.checkpoint_id,
                    "source barrier injection was rejected after preflight",
                );
                self.fail_manual_attempt(
                    attempt,
                    "manual checkpoint source barrier injection was rejected after preflight",
                );
                return;
            }
        }
    }

    /// Service periodic, manual, or leader-announced checkpoint admission.
    pub(super) async fn maybe_checkpoint(&mut self, callback: &mut impl PipelineCallback) -> bool {
        self.drain_manual_requests();
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint admission") {
            let reason = error.to_string();
            callback.record_checkpoint_admission_failure(&reason);
            self.fail_waiting_manual(reason);
            return true;
        }

        // Followers do not originate attempts. Preserve their resource cap while servicing the
        // leader's exact control announcement; leader/local admission applies its own cap below.
        if !callback.is_leader() {
            if !self.manual_waiting.is_empty() {
                self.fail_waiting_manual("only the cluster leader may admit a manual checkpoint");
            }
            if !self.checkpoint_capacity_available() {
                return false;
            }
            #[cfg(feature = "cluster")]
            if let Err(error) = self.require_process_authority("checkpoint control admission") {
                callback.record_checkpoint_admission_failure(&error.to_string());
                return true;
            }
            let outcome = callback
                .service_checkpoint_control(self.current_source_offsets())
                .await;
            #[cfg(feature = "cluster")]
            if let Err(error) =
                self.require_process_authority("follower checkpoint control application")
            {
                let authority_reason = error.to_string();
                match &outcome {
                    CheckpointControlOutcome::Started { attempt, .. }
                    | CheckpointControlOutcome::Failed { attempt, .. } => {
                        callback
                            .record_checkpoint_failure(attempt.checkpoint_id, &authority_reason);
                    }
                    CheckpointControlOutcome::AdmissionFailed { error } => callback
                        .record_checkpoint_admission_failure(&format!(
                            "{error}; {authority_reason}"
                        )),
                    CheckpointControlOutcome::Idle
                    | CheckpointControlOutcome::Aborted { .. }
                    | CheckpointControlOutcome::Cancelled { .. } => {
                        callback.record_checkpoint_admission_failure(&authority_reason);
                    }
                }
                return true;
            }
            match outcome {
                CheckpointControlOutcome::Idle => {}
                CheckpointControlOutcome::AdmissionFailed { error } => {
                    callback.record_checkpoint_admission_failure(&error);
                }
                CheckpointControlOutcome::Started {
                    attempt,
                    captured,
                    flags,
                } => {
                    if !captured {
                        self.pending_barrier.reset_follower(
                            attempt,
                            self.source_handles.len(),
                            flags,
                        );
                    }
                }
                CheckpointControlOutcome::Aborted { attempt } => {
                    if self.pending_barrier.attempt == Some(attempt) {
                        self.pending_barrier.clear();
                        self.barrier_seen.clear();
                    }
                    self.release_source_barrier_attempt(attempt);
                    self.fail_manual_attempt(
                        attempt,
                        "manual checkpoint was aborted by authoritative cluster control",
                    );
                }
                CheckpointControlOutcome::Cancelled { attempt } => {
                    if self.pending_barrier.attempt == Some(attempt) {
                        self.pending_barrier.clear();
                        self.barrier_seen.clear();
                    }
                    self.release_source_barrier_attempt(attempt);
                    self.fail_manual_attempt(
                        attempt,
                        "manual checkpoint was cancelled after its shuffle scope closed",
                    );
                }
                CheckpointControlOutcome::Failed { attempt, error } => {
                    callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
                }
            }
            return true;
        }
        let Some(mut admission) = self.checkpoint_admission(callback).await else {
            return true;
        };
        #[cfg(feature = "cluster")]
        if let Err(error) = self.require_process_authority("checkpoint attempt creation") {
            let reason = error.to_string();
            callback.record_checkpoint_admission_failure(&reason);
            self.fail_waiting_manual(reason);
            return true;
        }
        if self.source_handles.is_empty() {
            self.admit_source_less_checkpoint(callback, &mut admission)
                .await;
        } else {
            self.admit_source_barrier_checkpoint(callback, &mut admission)
                .await;
        }
        true
    }
}
