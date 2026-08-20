#[cfg(feature = "cluster")]
use super::{
    checked_successor_epoch, require_canonical_attempt, CheckpointArtifactInventory, Duration,
    EpochAllocator, LeaderProof, SinkEpochReservation,
};
use super::{CheckpointAttempt, CheckpointCoordinator, DbError};

#[cfg(feature = "cluster")]
const CLUSTER_SINK_EPOCH_POLL_INITIAL: Duration = Duration::from_millis(10);
#[cfg(feature = "cluster")]
const CLUSTER_SINK_EPOCH_POLL_MAX: Duration = Duration::from_millis(250);

#[cfg(feature = "cluster")]
impl EpochAllocator {
    pub(super) async fn reserve_certified_sink_epoch_until(
        &self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        use std::sync::atomic::Ordering;

        let attempt = require_canonical_attempt(attempt, "certified sink epoch reservation")?;
        let successor = checked_successor_epoch(attempt.epoch, "reserving certified sink epoch")?;
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("sink epoch allocator lock timed out".into()))?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} is already reserved",
                reservation.attempt().epoch
            )));
        }
        let floor = self.next_id_floor.load(Ordering::Acquire).max(1);
        if attempt.checkpoint_id < floor {
            return Err(DbError::Checkpoint(format!(
                "certified sink epoch {} is below local checkpoint floor {floor}",
                attempt.epoch
            )));
        }
        self.next_id_floor.fetch_max(successor, Ordering::AcqRel);
        *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::Opening(attempt));
        Ok(())
    }
}

impl CheckpointCoordinator {
    pub(super) async fn reserve_sink_epoch_for_runtime_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_controller.is_some() {
            return match self.reserve_cluster_sink_epoch_until(deadline).await {
                Ok(attempt) => Ok(attempt),
                Err(error) => {
                    self.failure_requires_recovery = true;
                    Err(error)
                }
            };
        }
        self.allocator.reserve_sink_epoch_until(deadline).await
    }

    #[cfg(feature = "cluster")]
    pub(super) async fn consume_follower_sink_epoch_until(
        &mut self,
        attempt: CheckpointAttempt,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(());
        }
        let reserved = self.allocator.consume_sink_epoch_until(deadline).await?;
        if reserved == attempt {
            return Ok(());
        }
        self.failure_requires_recovery = true;
        Err(DbError::Checkpoint(format!(
            "follower checkpoint attempt {attempt:?} does not match reserved sink epoch {reserved:?}"
        )))
    }

    #[cfg(feature = "cluster")]
    async fn install_cluster_sink_epoch_admission_until(
        &self,
        inventory: CheckpointArtifactInventory,
        proof: LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        self.validate_checkpoint_artifact_inventory(&inventory)?;
        let fence = inventory.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster sink epoch admission has no assignment fence".into())
        })?;
        if !fence.contains(self.store.participant_id()) {
            return Err(DbError::Checkpoint(format!(
                "cluster sink epoch admission excludes participant {}",
                self.store.participant_id()
            )));
        }
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster sink epoch admission has no cluster controller".into())
        })?;
        Self::certify_follower_assignment_until(
            controller,
            fence,
            &proof,
            deadline,
            "cluster sink epoch admission",
        )
        .await?;
        self.allocator
            .reserve_certified_sink_epoch_until(inventory.attempt, deadline)
            .await?;
        Ok(inventory.attempt)
    }

    #[cfg(feature = "cluster")]
    async fn admit_cluster_sink_epoch_as_leader_until(
        &self,
        proof: LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster sink epoch admission has no cluster controller".into())
        })?;
        let fence = tokio::time::timeout_at(
            deadline,
            controller.checkpoint_assignment_fence_for_leader(self.assignment_version, &proof),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster sink epoch fence read timed out".into()))?
        .ok_or_else(|| {
            DbError::Checkpoint(
                "cluster sink epoch leader has no exact certified assignment fence".into(),
            )
        })?;
        if !fence.contains(self.store.participant_id()) {
            return Err(DbError::Checkpoint(format!(
                "cluster sink epoch assignment excludes leader participant {}",
                self.store.participant_id()
            )));
        }

        let attempt = self.allocator.reserve_sink_epoch_until(deadline).await?;
        if let Err(error) = self
            .begin_checkpoint_artifacts_until(attempt, Some(fence.clone()), Some(&proof), deadline)
            .await
        {
            self.allocator.clear_sink_epoch(attempt);
            return Err(error);
        }
        let certified = tokio::time::timeout_at(
            deadline,
            controller.checkpoint_assignment_fence_for_leader(self.assignment_version, &proof),
        )
        .await
        .map_err(|_| DbError::Checkpoint("cluster sink epoch revalidation timed out".into()))?;
        if certified.as_ref() != Some(&fence) || !controller.proof_is_live(&proof) {
            self.allocator.clear_sink_epoch(attempt);
            return Err(DbError::Checkpoint(
                "cluster sink epoch leader authority changed after durable admission".into(),
            ));
        }
        Ok(attempt)
    }

    #[cfg(feature = "cluster")]
    async fn reserve_cluster_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Checkpoint("cluster sink epoch reservation has no cluster controller".into())
        })?;
        let authority = controller.checkpoint_authority().map_err(|error| {
            DbError::Checkpoint(format!("cluster checkpoint authority: {error}"))
        })?;
        let mut backoff = CLUSTER_SINK_EPOCH_POLL_INITIAL;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(DbError::Checkpoint(
                    "cluster sink epoch admission timed out".into(),
                ));
            }
            let admission = tokio::time::timeout_at(
                deadline,
                authority.cluster_checkpoint_artifact_admission(),
            )
            .await
            .map_err(|_| DbError::Checkpoint("cluster sink epoch admission read timed out".into()))?
            .map_err(|error| {
                DbError::Checkpoint(format!("cluster sink epoch admission read failed: {error}"))
            })?;
            if let Some((inventory, proof)) = admission {
                return self
                    .install_cluster_sink_epoch_admission_until(inventory, proof, deadline)
                    .await;
            }
            if let Some(proof) = controller.capture_leader_proof() {
                return self
                    .admit_cluster_sink_epoch_as_leader_until(proof, deadline)
                    .await;
            }
            let now = tokio::time::Instant::now();
            tokio::time::sleep_until((now + backoff).min(deadline)).await;
            backoff = backoff.saturating_mul(2).min(CLUSTER_SINK_EPOCH_POLL_MAX);
        }
    }
}
