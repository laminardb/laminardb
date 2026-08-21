//! Mechanically extracted coordinator responsibility.

use super::{
    Arc, AtomicU64, CheckpointAttempt, CheckpointAttemptRelation, CheckpointBarrier,
    CheckpointCleanupOwner, CheckpointCompletion, DbError, ManualCheckpointAttempt, Ordering,
    PipelineCallback, StreamingCoordinator, SHUTDOWN_COMPLETION_TICK,
};

impl StreamingCoordinator {
    /// Wire in the callback's admission counter so the coordinator gates new barriers.
    pub(crate) fn with_checkpoint_admission(mut self, in_flight: Arc<AtomicU64>) -> Self {
        self.checkpoint_in_flight = in_flight;
        self
    }

    pub(crate) fn with_checkpoint_complete_rx(
        mut self,
        rx: crossfire::AsyncRx<crossfire::mpsc::Array<CheckpointCompletion>>,
    ) -> Self {
        self.checkpoint_complete_rx = Some(rx);
        self
    }

    pub(crate) fn with_force_checkpoint_rx(mut self, rx: crate::db::ForceCheckpointRx) -> Self {
        self.force_ckpt_rx = Some(rx);
        self
    }

    pub(crate) fn with_terminal_shutdown(
        mut self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        self.terminal_shutdown = shutdown;
        self
    }

    pub(super) fn drain_manual_requests(&mut self) {
        let Some(rx) = self.force_ckpt_rx.as_ref() else {
            return;
        };
        while let Ok(request) = rx.try_recv() {
            self.manual_waiting.push(request);
        }
    }

    /// Remove callers that cancelled or exhausted their deadline before exact-attempt ownership.
    pub(super) fn prune_manual_requests(&mut self) {
        let now = tokio::time::Instant::now();
        let requests = std::mem::take(&mut self.manual_waiting);
        for request in requests {
            // A committed intermediate HANDOFF cut can return an already-acknowledged owner to
            // this queue for its replay-quiescent successor. Its public admission deadline and
            // receiver lifetime no longer cancel the coordinator-owned handoff sequence.
            if request.reservation_claim.is_none() {
                self.manual_waiting.push(request);
                continue;
            }
            if request.reply.is_disconnected() {
                continue;
            }
            if now >= request.deadline {
                request.reply.send(Err(DbError::Checkpoint(
                    "manual checkpoint deadline expired before exact attempt reservation".into(),
                )));
                continue;
            }
            self.manual_waiting.push(request);
        }
        if self.manual_waiting.is_empty() {
            self.manual_handoff_required = false;
        }
    }

    pub(super) fn manual_deadline(&self, default: tokio::time::Instant) -> tokio::time::Instant {
        // Once any replay owner has crossed the exact-reservation boundary, the next HANDOFF uses
        // a fresh coordinator attempt budget. A newly coalesced public caller must not shorten or
        // cancel the already-owned replay sequence.
        if self
            .manual_waiting
            .iter()
            .any(|request| request.reservation_claim.is_none())
        {
            return default;
        }
        self.manual_waiting
            .iter()
            .map(|request| request.deadline)
            .min()
            .unwrap_or(default)
            .min(default)
    }

    /// Claim every still-live public waiter before starting the bounded durable reservation call.
    /// Already-claimed HANDOFF replay owners have no token and remain attached automatically.
    pub(super) fn claim_waiting_manual_requests(&mut self) -> bool {
        if self
            .manual_waiting
            .iter()
            .any(|request| request.reservation_claim.is_none())
        {
            // The replay owner alone authorizes the fresh allocator budget. Newly coalesced public
            // callers remain independently deadline-cancellable until an exact successor exists.
            return true;
        }
        let requests = std::mem::take(&mut self.manual_waiting);
        for request in requests {
            let claimed = request
                .reservation_claim
                .as_ref()
                .is_none_or(crate::db::ForceCheckpointReservationClaim::try_claim);
            if claimed {
                self.manual_waiting.push(request);
            }
        }
        if self.manual_waiting.is_empty() {
            self.manual_handoff_required = false;
        }
        !self.manual_waiting.is_empty()
    }

    /// Attach claimed callers to an exact attempt and notify them that durable reservation won.
    pub(super) fn activate_manual_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        flags: u64,
    ) -> bool {
        if self.manual_waiting.is_empty() {
            return false;
        }
        debug_assert!(self.manual_active.is_none());
        self.manual_active = Some(ManualCheckpointAttempt {
            attempt,
            flags,
            requests: std::mem::take(&mut self.manual_waiting),
        });
        let active = self
            .manual_active
            .as_mut()
            .expect("manual attempt was just installed");
        let requests = std::mem::take(&mut active.requests);
        for mut request in requests {
            let Some(claim) = request.reservation_claim.take() else {
                active.requests.push(request);
                continue;
            };
            match claim.attach(request.deadline, tokio::time::Instant::now()) {
                crate::db::ForceCheckpointReservationAttachment::Attached => {
                    active.requests.push(request);
                }
                crate::db::ForceCheckpointReservationAttachment::Expired => {
                    request.reply.send(Err(DbError::Checkpoint(
                        "manual checkpoint deadline expired before exact attempt reservation"
                            .into(),
                    )));
                }
                crate::db::ForceCheckpointReservationAttachment::Cancelled => {}
            }
        }
        let owned = self
            .manual_active
            .as_ref()
            .is_some_and(|active| !active.requests.is_empty());
        if !owned {
            self.manual_active = None;
        }
        self.manual_handoff_required = false;
        owned
    }

    pub(super) fn retry_manual_handoff(&mut self, attempt: CheckpointAttempt) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        if active.flags & laminar_core::checkpoint::flags::HANDOFF == 0 {
            for request in active.requests {
                request.reply.send(Err(DbError::Checkpoint(
                    "non-handoff checkpoint reported pending handoff replay".into(),
                )));
            }
            return;
        }
        let mut requests = active.requests;
        requests.append(&mut self.manual_waiting);
        self.manual_waiting = requests;
        self.manual_handoff_required = true;
    }

    pub(super) fn fail_waiting_manual(&mut self, error: impl Into<String>) {
        let error = error.into();
        for request in self.manual_waiting.drain(..) {
            request.reply.send(Err(DbError::Checkpoint(error.clone())));
        }
        self.manual_handoff_required = false;
    }

    pub(super) fn finish_manual_success(
        &mut self,
        attempt: CheckpointAttempt,
        result: &crate::checkpoint_coordinator::CheckpointResult,
    ) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
        if completed != attempt || !result.success {
            let reason = format!(
                "manual checkpoint terminal result mismatch: admitted epoch={} id={}, \
                 completed epoch={} id={} success={}",
                attempt.epoch,
                attempt.checkpoint_id,
                completed.epoch,
                completed.checkpoint_id,
                result.success,
            );
            for request in active.requests {
                request.reply.send(Err(DbError::Checkpoint(reason.clone())));
            }
            return;
        }
        for request in active.requests {
            request.reply.send(Ok(result.clone()));
        }
    }

    pub(super) fn fail_manual_attempt(
        &mut self,
        attempt: CheckpointAttempt,
        error: impl Into<String>,
    ) {
        let Some(active) = self.manual_active.take() else {
            return;
        };
        if active.attempt != attempt {
            self.manual_active = Some(active);
            return;
        }
        let error = error.into();
        for request in active.requests {
            request.reply.send(Err(DbError::Checkpoint(error.clone())));
        }
    }

    pub(super) fn fail_all_manual(&mut self, error: &str) {
        self.fail_waiting_manual(error);
        if let Some(active) = self.manual_active.take() {
            for request in active.requests {
                request
                    .reply
                    .send(Err(DbError::Checkpoint(error.to_owned())));
            }
        }
    }

    pub(super) async fn cleanup_checkpoint_attempt(
        callback: &mut impl PipelineCallback,
        cleanup_owner: CheckpointCleanupOwner,
        attempt: CheckpointAttempt,
        reason: &str,
        flags: u64,
        assignment_fence: Option<laminar_core::cluster::control::CheckpointAssignmentFence>,
    ) -> Result<(), String> {
        match cleanup_owner {
            CheckpointCleanupOwner::Originator => {
                callback
                    .abandon_checkpoint_attempt(attempt, reason, flags, assignment_fence)
                    .await
            }
            CheckpointCleanupOwner::Follower => {
                callback
                    .cancel_source_barrier_attempt(attempt, reason)
                    .await
            }
        }
    }

    pub(super) async fn cancel_pending_barrier_for_stop(
        &mut self,
        callback: &mut impl PipelineCallback,
        reason: &str,
        release_sources: bool,
    ) -> Result<(), String> {
        let was_active = self.pending_barrier.active;
        let flags = self.pending_barrier.flags;
        let assignment_fence = self.pending_barrier.assignment_fence.clone();
        let attempt = self.pending_barrier.take_active_attempt();
        self.barrier_seen.clear();

        match attempt {
            Some((attempt, cleanup_owner)) => {
                tracing::info!(
                    checkpoint_id = attempt.checkpoint_id,
                    epoch = attempt.epoch,
                    reason,
                    "abandoning checkpoint interrupted before source alignment"
                );
                if cleanup_owner == CheckpointCleanupOwner::Originator {
                    self.cancel_local_source_barriers(CheckpointBarrier {
                        checkpoint_id: attempt.checkpoint_id,
                        epoch: attempt.epoch,
                        flags,
                    });
                }
                Self::cleanup_checkpoint_attempt(
                    callback,
                    cleanup_owner,
                    attempt,
                    reason,
                    flags,
                    assignment_fence,
                )
                .await?;
                if release_sources {
                    self.release_source_barrier_attempt(attempt);
                }
                callback.record_checkpoint_failure(attempt.checkpoint_id, reason);
                self.fail_manual_attempt(
                    attempt,
                    format!("manual checkpoint was interrupted: {reason}"),
                );
            }
            None if was_active => {
                callback.record_checkpoint_admission_failure(
                    "active source barrier had no exact reserved attempt during shutdown",
                );
                return Err(
                    "active source barrier had no exact reserved attempt during shutdown".into(),
                );
            }
            None => {}
        }
        Ok(())
    }

    /// Settle every captured durable tail before source or sink lifecycle teardown.
    ///
    /// The counter is claimed synchronously before a tail is spawned. A tail sends its terminal
    /// completion before dropping the claim, so waiting for zero and then draining the channel
    /// preserves exact source acknowledgements, public barriers, and manual replies. The tick
    /// handles tails that legitimately terminate without a completion (cluster followers) and
    /// avoids relying on a channel event after the atomic reaches zero.
    pub(super) async fn settle_checkpoint_tails(
        &mut self,
        callback: &mut impl PipelineCallback,
    ) -> Option<String> {
        let mut continuation_fault = None;
        loop {
            self.drain_manual_requests();
            self.fail_waiting_manual("pipeline is stopping; no new checkpoint can be admitted");

            while let Some(completion) = self
                .checkpoint_complete_rx
                .as_ref()
                .and_then(|rx| rx.try_recv().ok())
            {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    continuation_fault.get_or_insert(error);
                }
            }

            if self.checkpoint_in_flight.load(Ordering::Acquire) == 0 {
                break;
            }

            let completion = if let Some(rx) = self.checkpoint_complete_rx.as_mut() {
                match tokio::time::timeout(SHUTDOWN_COMPLETION_TICK, rx.recv()).await {
                    Ok(Ok(completion)) => Some(completion),
                    Ok(Err(_)) => {
                        tokio::time::sleep(SHUTDOWN_COMPLETION_TICK).await;
                        None
                    }
                    Err(_) => None,
                }
            } else {
                tokio::time::sleep(SHUTDOWN_COMPLETION_TICK).await;
                None
            };
            if let Some(completion) = completion {
                if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                    continuation_fault.get_or_insert(error);
                }
            }
        }

        if let Err(error) = callback.settle_checkpoint_tail_tasks().await {
            continuation_fault.get_or_insert(error);
        }

        // A sender enqueues its completion before dropping the in-flight guard. Once the counter
        // reaches zero, drain the enqueue that may have raced with our last atomic load.
        while let Some(completion) = self
            .checkpoint_complete_rx
            .as_ref()
            .and_then(|rx| rx.try_recv().ok())
        {
            if let Some(error) = self.handle_checkpoint_completion(completion, callback) {
                continuation_fault.get_or_insert(error);
            }
        }
        self.drain_manual_requests();
        self.fail_all_manual("pipeline stopped before the checkpoint reached a terminal result");
        continuation_fault
    }

    pub(super) fn handle_checkpoint_completion(
        &mut self,
        completion: CheckpointCompletion,
        callback: &mut impl PipelineCallback,
    ) -> Option<String> {
        let attempt = completion.attempt();
        match completion {
            CheckpointCompletion::Committed {
                result,
                source_checkpoints,
                handoff_replay_pending,
                ..
            } => {
                let completed = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
                if !result.success || completed != attempt {
                    let reason = format!(
                        "checkpoint terminal identity mismatch: admitted epoch={} id={}, \
                         completed epoch={} id={} success={}",
                        attempt.epoch,
                        attempt.checkpoint_id,
                        completed.epoch,
                        completed.checkpoint_id,
                        result.success,
                    );
                    callback.abort_subscription_cut(attempt);
                    self.fail_manual_attempt(attempt, &reason);
                    callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                } else {
                    if self.last_published_checkpoint.is_some_and(|last| {
                        last.relation_to(attempt) != CheckpointAttemptRelation::Older
                    }) {
                        let last = self.last_published_checkpoint.unwrap();
                        let reason = format!(
                            "checkpoint completion is not strictly newer: last published epoch={} id={}, \
                             received epoch={} id={}",
                            last.epoch,
                            last.checkpoint_id,
                            attempt.epoch,
                            attempt.checkpoint_id,
                        );
                        callback.abort_subscription_cut(attempt);
                        self.fail_manual_attempt(attempt, &reason);
                        callback.record_checkpoint_failure(attempt.checkpoint_id, &reason);
                        return Some(reason);
                    }
                    #[cfg(feature = "cluster")]
                    if let Err(error) =
                        self.require_process_authority("durable checkpoint publication")
                    {
                        let reason = error.to_string();
                        callback.abort_subscription_cut(attempt);
                        self.fail_manual_attempt(attempt, &reason);
                        callback.record_checkpoint_continuation_fault(attempt, &reason);
                        return Some(reason);
                    }
                    let continuation_error = result.continuation_error().map(str::to_owned);
                    // Ordering is semantic: N reached its durable point, so source and public
                    // acknowledgements for N must be published even when N+1 cannot be opened.
                    self.last_published_checkpoint = Some(attempt);
                    let publication_error = callback.publish_barrier(attempt).err();
                    self.broadcast_epoch_committed(attempt.epoch, &source_checkpoints);
                    let continuation_error = publication_error.or(continuation_error);
                    if handoff_replay_pending {
                        self.replay_pending = true;
                        if let Some(reason) = continuation_error.as_deref() {
                            self.fail_manual_attempt(attempt, reason);
                        } else {
                            self.retry_manual_handoff(attempt);
                        }
                    } else {
                        self.finish_manual_success(attempt, &result);
                        self.advance_checkpoint_cadence();
                    }
                    if let Some(reason) = continuation_error.as_deref() {
                        callback.record_checkpoint_continuation_fault(attempt, reason);
                    }
                    return continuation_error;
                }
            }
            CheckpointCompletion::Failed { error, .. } => {
                callback.abort_subscription_cut(attempt);
                self.fail_manual_attempt(attempt, &error);
                callback.record_checkpoint_failure(attempt.checkpoint_id, &error);
                self.advance_checkpoint_cadence();
            }
        }
        None
    }
}
