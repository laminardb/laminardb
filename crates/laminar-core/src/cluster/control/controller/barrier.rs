//! Checkpoint barrier announcement, observation, and acknowledgement.

use super::*;

impl ClusterController {
    /// Leader-side announce.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::announce`] errors.
    pub async fn announce_barrier(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        self.barrier.announce(ann).await
    }

    /// Leader-side assignment-certified Prepare publication with its configured quorum window.
    ///
    /// # Errors
    /// Propagates process-lease and [`BarrierCoordinator::announce_prepare`] errors.
    #[cfg(feature = "cluster")]
    pub async fn announce_prepare_barrier(
        &self,
        ann: &BarrierAnnouncement,
        quorum_window: Duration,
    ) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        self.barrier.announce_prepare(ann, quorum_window).await
    }

    /// Leader-side Prepare publication whose fan-out is fenced by the exact attempt deadline.
    /// The retry window remains independent so a long checkpoint timeout does not create long
    /// individual RPC attempts.
    ///
    /// # Errors
    /// Propagates process-lease and [`BarrierCoordinator::announce_prepare_until`] errors.
    #[cfg(feature = "cluster")]
    pub async fn announce_prepare_barrier_until(
        &self,
        ann: &BarrierAnnouncement,
        attempt_deadline: tokio::time::Instant,
        retry_window: Duration,
    ) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        self.barrier
            .announce_prepare_until(ann, attempt_deadline, retry_window)
            .await
    }

    /// Observe the merged barrier history, validating durable authority only when `predicate`
    /// selects the announcement. Malformed or conflicting histories fail before filtering.
    ///
    /// # Errors
    /// Propagates merge, transport, and matching reversible-authority failures.
    pub async fn observe_barrier_matching<F>(
        &self,
        mut predicate: F,
    ) -> Result<Option<BarrierAnnouncement>, String>
    where
        F: FnMut(&BarrierAnnouncement) -> bool,
    {
        let Some(leader) = self.current_leader() else {
            return Ok(None);
        };
        let Some(announcement) = self.barrier.observe_hint(leader).await? else {
            return Ok(None);
        };
        if !predicate(&announcement) {
            return Ok(None);
        }
        #[cfg(feature = "cluster")]
        self.barrier.validate_observed(&announcement).await?;
        Ok(Some(announcement))
    }

    /// Observe a clustered `Prepare` with a bounded compatibility timeout.
    ///
    /// # Errors
    /// Rejects missing, stale, or conflicting authority and assignment certificates.
    #[cfg(feature = "cluster")]
    pub async fn observe_checkpoint_prepare(
        &self,
    ) -> Result<Option<CheckpointPrepareObservation>, String> {
        self.observe_checkpoint_prepare_until(CHECKPOINT_PREPARE_OBSERVATION_TIMEOUT)
            .await
    }

    /// Observe a clustered `Prepare`, validate its leader with one durable-authority read, and
    /// report the local assignment disposition without consulting authority again. Hint I/O is
    /// bounded by the caller's budget. Once an exact Prepare is decoded, authority validation uses
    /// the earlier of its same-identity direct receipt or stable first gossip observation, which
    /// retries cannot refresh. An unrelated stale direct Prepare never shortens the observation
    /// window for a newer gossip-only attempt.
    ///
    /// # Errors
    /// Rejects a zero or overflowing timeout, observation/authority timeout, or missing, stale,
    /// conflicting authority and assignment certificates.
    #[cfg(feature = "cluster")]
    pub async fn observe_checkpoint_prepare_until(
        &self,
        checkpoint_timeout: Duration,
    ) -> Result<Option<CheckpointPrepareObservation>, String> {
        if checkpoint_timeout.is_zero() {
            return Err("checkpoint Prepare observation timeout must be greater than zero".into());
        }
        let Some(leader) = self.current_leader() else {
            return Ok(None);
        };
        let observation_started = tokio::time::Instant::now();
        let observation_deadline = observation_started
            .checked_add(checkpoint_timeout)
            .ok_or_else(|| "checkpoint Prepare observation deadline overflowed".to_string())?;
        if tokio::time::Instant::now() >= observation_deadline {
            return Err("checkpoint Prepare observation deadline already elapsed".into());
        }
        let Some(announcement) =
            tokio::time::timeout_at(observation_deadline, self.barrier.observe_hint(leader))
                .await
                .map_err(|_| "checkpoint Prepare hint observation timed out".to_string())??
        else {
            return Ok(None);
        };
        if announcement.phase != super::super::Phase::Prepare {
            return Ok(None);
        }
        let attempt = crate::checkpoint::CheckpointAttempt::new(
            announcement.epoch,
            announcement.checkpoint_id,
        );
        if self
            .barrier
            .prepare_settlement_covers(attempt.checkpoint_id)
        {
            return Ok(None);
        }
        let received_at = self
            .barrier
            .prepare_received_at_or_insert(&announcement, observation_started.into_std())
            .ok_or_else(|| "checkpoint Prepare lost its exact observation clock".to_string())?;
        let attempt_deadline = tokio::time::Instant::from_std(received_at)
            .checked_add(checkpoint_timeout)
            .ok_or_else(|| "checkpoint Prepare attempt deadline overflowed".to_string())?;
        if tokio::time::Instant::now() >= attempt_deadline {
            return self
                .ignore_durably_settled_prepare(
                    attempt,
                    observation_deadline,
                    "checkpoint Prepare attempt deadline elapsed before authority validation",
                )
                .await;
        }
        let validation = tokio::time::timeout_at(
            attempt_deadline,
            self.barrier.validate_checkpoint_prepare(&announcement),
        )
        .await;
        let validation_error = match validation {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some("checkpoint Prepare authority validation timed out".to_string()),
        };
        if let Some(error) = validation_error {
            return self
                .ignore_durably_settled_prepare(attempt, observation_deadline, &error)
                .await;
        }
        let proof = announcement
            .leader_proof
            .as_ref()
            .filter(|proof| proof.is_canonical())
            .ok_or_else(|| "leader Prepare omitted its canonical authority proof".to_string())?;
        let assignment_error = match announcement.assignment_fence.as_ref() {
            None => Some(
                "[LDB-6055] leader Prepare omitted its canonical assignment certificate"
                    .to_string(),
            ),
            Some(fence) if !fence.is_canonical() => Some(
                "[LDB-6055] leader Prepare carried a non-canonical assignment certificate"
                    .to_string(),
            ),
            Some(fence) => self
                .checkpoint_assignment_fence(fence.assignment_version)
                .and_then(|certified| {
                    self.checkpoint_assignment_fence_after_authority_validation(certified, proof)
                })
                .map_or_else(
                    || {
                        Some(format!(
                            "[LDB-6055] follower cannot certify leader Prepare assignment {}",
                            fence.assignment_version
                        ))
                    },
                    |certified| {
                        (certified != *fence).then(|| {
                            format!(
                                "[LDB-6055] follower assignment differs from leader Prepare assignment {}",
                                fence.assignment_version
                            )
                        })
                    },
                ),
        };
        Ok(Some(match assignment_error {
            Some(error) => CheckpointPrepareObservation::AssignmentRejected {
                announcement,
                error,
            },
            None => CheckpointPrepareObservation::AssignmentReady(announcement),
        }))
    }

    #[cfg(feature = "cluster")]
    async fn ignore_durably_settled_prepare(
        &self,
        attempt: crate::checkpoint::CheckpointAttempt,
        deadline: tokio::time::Instant,
        validation_error: &str,
    ) -> Result<Option<CheckpointPrepareObservation>, String> {
        if self
            .barrier
            .prepare_settlement_covers(attempt.checkpoint_id)
        {
            return Ok(None);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(validation_error.to_owned());
        }
        let authority = self.checkpoint_authority().map_err(|error| {
            format!(
                "{validation_error}; checkpoint Prepare settlement authority is unavailable: {error}"
            )
        })?;
        let settlement =
            tokio::time::timeout_at(deadline, authority.cluster_attempt_settlement(attempt))
                .await
                .map_err(|_| {
                    format!("{validation_error}; checkpoint Prepare settlement audit timed out")
                })?
                .map_err(|error| {
                    format!(
                        "{validation_error}; checkpoint Prepare settlement audit failed: {error}"
                    )
                })?;
        let Some(settlement) = settlement else {
            return Err(validation_error.to_owned());
        };
        let settled =
            crate::checkpoint::CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id);
        match settled.relation_to(attempt) {
            crate::checkpoint::CheckpointAttemptRelation::Exact
            | crate::checkpoint::CheckpointAttemptRelation::Newer => {
                self.barrier
                    .record_prepare_settlement(settled.checkpoint_id);
                Ok(None)
            }
            crate::checkpoint::CheckpointAttemptRelation::Older
            | crate::checkpoint::CheckpointAttemptRelation::Conflict => Err(format!(
                "{validation_error}; checkpoint Prepare settlement {settled:?} does not close {attempt:?}"
            )),
        }
    }

    /// Subscribe to direct checkpoint announcements. Consumers must retain a bounded KV poll:
    /// the watch is a latency path, while the merged gossip history remains the fallback.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn checkpoint_announcement_watch(
        &self,
    ) -> Option<watch::Receiver<Option<BarrierAnnouncement>>> {
        self.barrier.announcement_watch()
    }

    /// Stable local monotonic receipt or first-observation time for this exact Prepare.
    #[must_use]
    pub fn checkpoint_prepare_received_at(
        &self,
        prepare: &BarrierAnnouncement,
    ) -> Option<std::time::Instant> {
        self.barrier.prepare_received_at(prepare)
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Propagates [`BarrierCoordinator::ack`] errors.
    pub async fn ack_barrier(&self, ack: &BarrierAck) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        self.barrier.ack(ack).await
    }
}
