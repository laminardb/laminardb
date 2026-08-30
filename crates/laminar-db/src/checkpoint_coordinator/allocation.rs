use std::sync::Arc;

use super::{CheckpointAttempt, CheckpointCoordinator, DbError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SinkEpochReservation {
    Opening(CheckpointAttempt),
    Ready(CheckpointAttempt),
    InDoubt(CheckpointAttempt),
}

impl SinkEpochReservation {
    pub(super) const fn attempt(self) -> CheckpointAttempt {
        match self {
            Self::Opening(attempt) | Self::Ready(attempt) | Self::InDoubt(attempt) => attempt,
        }
    }
}

#[derive(Debug)]
pub(crate) struct EpochAllocator {
    pub(super) next_id_floor: std::sync::atomic::AtomicU64,
    observed_id_floor: std::sync::atomic::AtomicU64,
    pub(super) allocation_lock: tokio::sync::Mutex<()>,
    pub(super) sink_epoch_reservation: parking_lot::Mutex<Option<SinkEpochReservation>>,
    decision_store:
        std::sync::OnceLock<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
}

pub(super) fn checked_successor_epoch(epoch: u64, context: &str) -> Result<u64, DbError> {
    epoch.checked_add(1).ok_or_else(|| {
        DbError::Checkpoint(format!(
            "checkpoint epoch space exhausted at {epoch} while {context}"
        ))
    })
}

pub(super) fn require_canonical_attempt(
    attempt: CheckpointAttempt,
    context: &str,
) -> Result<CheckpointAttempt, DbError> {
    if attempt.is_canonical() {
        Ok(attempt)
    } else {
        Err(DbError::Checkpoint(format!(
            "{context} requires one nonzero canonical checkpoint ID; received epoch {} and checkpoint ID {}",
            attempt.epoch, attempt.checkpoint_id
        )))
    }
}

impl EpochAllocator {
    pub(super) fn new() -> Self {
        Self {
            next_id_floor: std::sync::atomic::AtomicU64::new(1),
            observed_id_floor: std::sync::atomic::AtomicU64::new(0),
            allocation_lock: tokio::sync::Mutex::new(()),
            sink_epoch_reservation: parking_lot::Mutex::new(None),
            decision_store: std::sync::OnceLock::new(),
        }
    }

    pub(super) fn bind_decision_store(
        &self,
        store: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
    ) -> Result<(), DbError> {
        if let Some(bound) = self.decision_store.get() {
            return if Arc::ptr_eq(bound, &store) {
                Ok(())
            } else {
                Err(DbError::Checkpoint(
                    "checkpoint allocator decision store is already bound".into(),
                ))
            };
        }
        self.decision_store.set(store).map_err(|_| {
            DbError::Checkpoint("checkpoint allocator decision store is already bound".into())
        })
    }

    async fn allocate_fresh_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        use std::sync::atomic::Ordering;

        let store = self.decision_store.get().ok_or_else(|| {
            DbError::Checkpoint("checkpoint ID allocation requires a decision store".into())
        })?;
        loop {
            let minimum = self.next_id_floor.load(Ordering::Acquire).max(1);
            let checkpoint_id =
                tokio::time::timeout_at(deadline, store.allocate_checkpoint_id_at_least(minimum))
                    .await
                    .map_err(|_| DbError::Checkpoint("checkpoint ID allocation timed out".into()))?
                    .map_err(|error| {
                        DbError::Checkpoint(format!("checkpoint ID allocation failed: {error}"))
                    })?;
            let successor = checked_successor_epoch(checkpoint_id, "advancing allocation")?;
            let mut floor = self.next_id_floor.load(Ordering::Acquire);
            loop {
                if checkpoint_id < floor {
                    break;
                }
                match self.next_id_floor.compare_exchange_weak(
                    floor,
                    successor.max(floor),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(CheckpointAttempt::canonical(checkpoint_id)),
                    Err(observed) => floor = observed,
                }
            }
            tokio::task::yield_now().await;
        }
    }

    pub(crate) async fn allocate_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint allocator lock timed out".into()))?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} must be consumed before allocating another checkpoint",
                reservation.attempt().epoch
            )));
        }
        self.allocate_fresh_until(deadline).await
    }

    pub(super) async fn reserve_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("sink epoch allocator lock timed out".into()))?;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            return Err(DbError::Checkpoint(format!(
                "sink epoch {} is already reserved",
                reservation.attempt().epoch
            )));
        }
        let attempt = self.allocate_fresh_until(deadline).await?;
        *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::Opening(attempt));
        Ok(attempt)
    }

    pub(super) fn mark_sink_epoch_ready(&self, attempt: CheckpointAttempt) -> Result<(), DbError> {
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Opening(current)) if current == attempt => {
                *reservation = Some(SinkEpochReservation::Ready(attempt));
                Ok(())
            }
            current => Err(DbError::Checkpoint(format!(
                "sink epoch reservation mismatch for {attempt:?}: {current:?}"
            ))),
        }
    }

    pub(super) fn mark_sink_epoch_in_doubt(&self, attempt: CheckpointAttempt) {
        *self.sink_epoch_reservation.lock() = Some(SinkEpochReservation::InDoubt(attempt));
    }

    pub(crate) async fn consume_sink_epoch_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        let _guard = tokio::time::timeout_at(deadline, self.allocation_lock.lock())
            .await
            .map_err(|_| DbError::Checkpoint("checkpoint allocator lock timed out".into()))?;
        let mut reservation = self.sink_epoch_reservation.lock();
        match *reservation {
            Some(SinkEpochReservation::Ready(attempt)) => {
                reservation.take();
                Ok(attempt)
            }
            Some(SinkEpochReservation::Opening(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} is still opening",
                attempt.epoch
            ))),
            Some(SinkEpochReservation::InDoubt(attempt)) => Err(DbError::Checkpoint(format!(
                "sink epoch {} requires recovery",
                attempt.epoch
            ))),
            None => Err(DbError::Checkpoint(
                "checkpoint-committable sinks have no open epoch".into(),
            )),
        }
    }

    #[cfg(feature = "cluster")]
    pub(super) fn clear_sink_epoch(&self, attempt: CheckpointAttempt) {
        let mut reservation = self.sink_epoch_reservation.lock();
        if reservation.is_some_and(|current| current.attempt() == attempt) {
            reservation.take();
        }
    }

    pub(crate) fn peek_epoch(&self) -> u64 {
        use std::sync::atomic::Ordering;
        if let Some(reservation) = *self.sink_epoch_reservation.lock() {
            reservation.attempt().epoch
        } else {
            self.next_id_floor.load(Ordering::Acquire)
        }
    }

    pub(crate) fn advance_epoch_to(&self, epoch: u64) {
        use std::sync::atomic::Ordering;
        self.next_id_floor.fetch_max(epoch, Ordering::AcqRel);
        self.observed_id_floor.fetch_max(epoch, Ordering::AcqRel);
    }
}

impl CheckpointCoordinator {
    pub(super) async fn allocate_attempt_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CheckpointAttempt, DbError> {
        if self.has_checkpoint_committable_sinks() {
            self.allocator.consume_sink_epoch_until(deadline).await
        } else {
            self.allocator.allocate_until(deadline).await
        }
    }
}
