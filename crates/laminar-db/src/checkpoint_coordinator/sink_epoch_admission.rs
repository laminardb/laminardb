#[cfg(feature = "cluster")]
use super::{
    checked_successor_epoch, require_canonical_attempt, CheckpointArtifactInventory, Duration,
    EpochAllocator, SinkEpochReservation,
};
use super::{CheckpointAttempt, CheckpointCoordinator, DbError, LeaderProof};

#[cfg(feature = "cluster")]
const CLUSTER_SINK_EPOCH_POLL_INITIAL: Duration = Duration::from_millis(10);
#[cfg(feature = "cluster")]
const CLUSTER_SINK_EPOCH_POLL_MAX: Duration = Duration::from_millis(250);

pub(crate) const fn is_terminal_handoff(flags: u64, handoff_replay_pending: bool) -> bool {
    flags & laminar_core::checkpoint::flags::HANDOFF != 0 && !handoff_replay_pending
}

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
    #[cfg(feature = "cluster")]
    pub(crate) fn certified_idle_process(&self) -> Result<bool, DbError> {
        let Some(controller) = self.cluster_controller.as_ref() else {
            return Ok(false);
        };
        let Some(fence) = controller.checkpoint_assignment_fence(self.assignment_version) else {
            return Ok(false);
        };
        if !fence.is_canonical()
            || fence.assignment_version != self.assignment_version
            || fence.vnode_count != u32::from(self.store.key_group_count().get())
        {
            return Err(DbError::Checkpoint(format!(
                "cluster assignment {} has an invalid checkpoint certificate",
                self.assignment_version
            )));
        }
        let participant_id = self.store.participant_id();
        match fence.participant_incarnation(participant_id) {
            Some(incarnation) if incarnation == controller.recovery_incarnation() => Ok(false),
            Some(_) => Err(DbError::Checkpoint(format!(
                "cluster assignment {} certifies another incarnation of participant {participant_id}",
                self.assignment_version
            ))),
            None if self.owned_vnodes.is_empty() => Ok(true),
            None => Err(DbError::Checkpoint(format!(
                "cluster assignment {} excludes participant {participant_id} with owned vnodes {:?}",
                self.assignment_version, self.owned_vnodes
            ))),
        }
    }

    #[cfg(feature = "cluster")]
    pub(super) fn initial_sink_epoch_required(&self) -> Result<bool, DbError> {
        if !self.has_checkpoint_committable_sinks() {
            return Ok(false);
        }
        if let Some(controller) = self.cluster_controller.as_ref() {
            let Some(fence) = controller.checkpoint_assignment_fence(self.assignment_version)
            else {
                return Ok(true);
            };
            if fence.contains(self.store.participant_id()) {
                return Ok(true);
            }
            if self.owned_vnodes.is_empty() {
                return Ok(false);
            }
            return Err(DbError::Checkpoint(format!(
                "cluster initial sink epoch excludes participant {} with owned vnodes {:?}",
                self.store.participant_id(),
                self.owned_vnodes
            )));
        }
        Ok(true)
    }

    #[cfg(feature = "cluster")]
    pub(crate) async fn ensure_assignment_sink_epoch_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        if !self.initial_sink_epoch_required()? {
            return Ok(());
        }
        let reservation = *self.allocator.sink_epoch_reservation.lock();
        match reservation {
            None => {
                self.begin_sink_epoch_until(deadline, super::SinkEpochPublication::Immediate)
                    .await
            }
            Some(SinkEpochReservation::Ready(attempt)) => {
                for sink in self
                    .sinks
                    .iter()
                    .filter(|sink| sink.handle.checkpoint_committable())
                {
                    let admission = sink
                        .handle
                        .wait_for_open_epoch_until(Some(deadline))
                        .await
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "sink '{}' assignment epoch is not writable: {error}",
                                sink.name
                            ))
                        })?
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "sink '{}' has no checkpoint-committable assignment epoch",
                                sink.name
                            ))
                        })?;
                    if admission.epoch != attempt.epoch {
                        return Err(DbError::Checkpoint(format!(
                            "sink '{}' opened epoch {}, expected assignment epoch {}",
                            sink.name, admission.epoch, attempt.epoch
                        )));
                    }
                }
                Ok(())
            }
            Some(SinkEpochReservation::Opening(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} is still opening during assignment activation",
                attempt.epoch
            ))),
            Some(SinkEpochReservation::InDoubt(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} requires recovery during assignment activation",
                attempt.epoch
            ))),
        }
    }

    pub(super) async fn continue_committed_sink_epoch_until(
        &mut self,
        external_commit: Result<(), DbError>,
        index: &laminar_core::checkpoint::CommittedCheckpointIndex,
        leader_proof: Option<&LeaderProof>,
        terminal_handoff: bool,
        deadline: tokio::time::Instant,
        publication: super::SinkEpochPublication,
    ) -> Result<(), DbError> {
        external_commit?;
        self.clear_sink_artifact_intents(CheckpointAttempt::new(index.epoch, index.checkpoint_id));
        self.schedule_retention(index.clone(), leader_proof);
        self.clear_sink_witness_until(deadline).await?;
        if self.has_checkpoint_committable_sinks() && !terminal_handoff {
            self.begin_sink_epoch_until(deadline, publication).await?;
        }
        Ok(())
    }

    pub(super) async fn reserve_sink_epoch_for_runtime_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<laminar_core::checkpoint_decision::CheckpointArtifactInventory, DbError> {
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
        let attempt = self.allocator.reserve_sink_epoch_until(deadline).await?;
        let inventory = self.checkpoint_artifact_inventory(attempt, None)?;
        if let Err(error) = self
            .begin_checkpoint_artifacts_until(attempt, None, None, deadline)
            .await
        {
            self.allocator.mark_sink_epoch_in_doubt(attempt);
            self.failure_requires_recovery = true;
            return Err(error);
        }
        Ok(inventory)
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
    ) -> Result<CheckpointArtifactInventory, DbError> {
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
        Ok(inventory)
    }

    #[cfg(feature = "cluster")]
    async fn admit_cluster_sink_epoch_as_leader_until(
        &self,
        proof: LeaderProof,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointArtifactInventory, DbError> {
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
        self.checkpoint_artifact_inventory(attempt, Some(fence))
    }

    #[cfg(feature = "cluster")]
    async fn reserve_cluster_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointArtifactInventory, DbError> {
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
