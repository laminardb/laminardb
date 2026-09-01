//! Deadline-bounded fencing and current-participant verification.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use uuid::Uuid;

use crate::cluster::discovery::NodeId;

use super::{
    now_millis, ProcessLease, ProcessLeaseError, ProcessLeaseFence, ProcessLeaseOutcome,
    ProcessLeaseStore,
};

/// Shared authority for proving that an exact process incarnation has been durably revoked.
pub struct ProcessLeaseAuthority {
    store: Arc<dyn ObjectStore>,
    ttl: Duration,
    ttl_ms: i64,
}

impl std::fmt::Debug for ProcessLeaseAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessLeaseAuthority")
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl ProcessLeaseAuthority {
    /// Bind all stable-node lease namespaces on one shared object store and TTL.
    ///
    /// # Errors
    /// Rejects a zero, sub-millisecond, fractional-millisecond, or oversized TTL.
    pub fn new(store: Arc<dyn ObjectStore>, ttl: Duration) -> Result<Self, ProcessLeaseError> {
        let ttl_ms = i64::try_from(ttl.as_millis())
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| ProcessLeaseError::Invalid("process lease TTL is invalid".into()))?;
        if Duration::from_millis(u64::try_from(ttl_ms).unwrap_or(u64::MAX)) != ttl {
            return Err(ProcessLeaseError::Invalid(
                "process lease TTL must use whole milliseconds".into(),
            ));
        }
        Ok(Self { store, ttl, ttl_ms })
    }

    /// Open one stable-node namespace over the shared authority.
    #[must_use]
    pub fn store_for(&self, node: NodeId) -> Arc<ProcessLeaseStore> {
        Arc::new(ProcessLeaseStore::new(
            Arc::clone(&self.store),
            node,
            self.ttl_ms,
        ))
    }

    /// Build a monotonic deadline that covers the mandatory full-TTL observation plus bounded
    /// authority I/O. Callers need not duplicate the process-lease TTL as another runtime knob.
    ///
    /// # Errors
    /// Rejects a zero I/O budget or monotonic-clock overflow.
    pub fn fencing_deadline(
        &self,
        io_budget: Duration,
    ) -> Result<tokio::time::Instant, ProcessLeaseError> {
        if io_budget.is_zero() {
            return Err(ProcessLeaseError::Invalid(
                "process lease fencing I/O budget must be nonzero".into(),
            ));
        }
        tokio::time::Instant::now()
            .checked_add(self.ttl)
            .and_then(|deadline| deadline.checked_add(io_budget))
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease fencing deadline overflow".into())
            })
    }

    async fn bounded<T>(
        deadline: tokio::time::Instant,
        operation: &str,
        future: impl Future<Output = Result<T, ProcessLeaseError>>,
    ) -> Result<T, ProcessLeaseError> {
        if tokio::time::Instant::now() >= deadline {
            return Err(ProcessLeaseError::Deadline(format!(
                "deadline expired before {operation}"
            )));
        }
        tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| {
                ProcessLeaseError::Deadline(format!("deadline expired during {operation}"))
            })?
    }

    async fn recover_won_fence(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        head: ProcessLease,
        deadline: tokio::time::Instant,
    ) -> Result<ProcessLeaseFence, ProcessLeaseError> {
        let node = NodeId(participant.node_id);
        if head.node != node || head.owner == participant.boot_incarnation {
            return Err(ProcessLeaseError::Invalid(
                "process lease head has not superseded the requested incarnation".into(),
            ));
        }
        let store = self.store_for(node);
        let fence = Self::bounded(
            deadline,
            "locating process lease takeover evidence",
            store.find_takeover_from(participant.boot_incarnation),
        )
        .await?
        .ok_or_else(|| {
            ProcessLeaseError::Invalid(format!(
                "process lease takeover evidence for {} is missing",
                participant.boot_incarnation
            ))
        })?;
        if !self.verify_fence(&fence, deadline).await? {
            return Err(ProcessLeaseError::Invalid(
                "process lease takeover is no longer durably verifiable".into(),
            ));
        }
        Ok(fence)
    }

    /// Durably supersede an unchanged process incarnation after observing it for one full TTL.
    ///
    /// A retry after the create-only takeover won reconstructs the exact fence from the two
    /// retained history records. It never waits a second TTL for that already-durable result.
    ///
    /// # Errors
    /// Fails closed on renewal, deadline expiry, missing history, or any authority I/O failure.
    pub async fn fence_incarnation(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<ProcessLeaseFence, ProcessLeaseError> {
        let node = NodeId(participant.node_id);
        if node.is_unassigned() || participant.boot_incarnation.is_nil() {
            return Err(ProcessLeaseError::Invalid(
                "process lease fence participant is not canonical".into(),
            ));
        }
        let store = self.store_for(node);
        let head = Self::bounded(deadline, "loading process lease fence head", store.load())
            .await?
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease fence history is missing".into())
            })?;
        if head.owner != participant.boot_incarnation {
            return self.recover_won_fence(participant, head, deadline).await;
        }

        let observation = store.observe_rival(&head)?;
        let observation_until = tokio::time::Instant::now()
            .checked_add(self.ttl)
            .ok_or_else(|| ProcessLeaseError::Invalid("process lease TTL overflows time".into()))?;
        if observation_until >= deadline {
            return Err(ProcessLeaseError::Deadline(
                "deadline does not cover one full process lease TTL".into(),
            ));
        }
        tokio::time::sleep_until(observation_until).await;

        let mut revoker = Uuid::new_v4();
        while revoker.is_nil() || revoker == participant.boot_incarnation {
            revoker = Uuid::new_v4();
        }
        let outcome = Self::bounded(
            deadline,
            "publishing process lease takeover",
            store.try_takeover(revoker, &observation, now_millis()),
        )
        .await?;
        match outcome {
            ProcessLeaseOutcome::Acquired(successor) => {
                let fence = ProcessLeaseFence::new(head, successor)?;
                if !self.verify_fence(&fence, deadline).await? {
                    return Err(ProcessLeaseError::Invalid(
                        "won process lease takeover could not be verified".into(),
                    ));
                }
                Ok(fence)
            }
            ProcessLeaseOutcome::Held(current) if current.owner == participant.boot_incarnation => {
                Err(ProcessLeaseError::Invalid(
                    "process incarnation renewed during the full-TTL observation".into(),
                ))
            }
            ProcessLeaseOutcome::Held(current) => {
                self.recover_won_fence(participant, current, deadline).await
            }
        }
    }

    /// Verify that an exact boot incarnation is the current durable owner of its stable node.
    ///
    /// # Errors
    /// Fails closed on deadline expiry, missing takeover evidence, malformed state, or I/O.
    pub async fn verify_current_participant(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        self.verify_current_participant_identity(participant, None, deadline)
            .await
    }

    /// Verify an exact boot and process term against the current durable stable-node authority.
    ///
    /// # Errors
    /// Fails closed on deadline expiry, missing takeover evidence, malformed state, or I/O.
    pub async fn verify_current_participant_term(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        process_term: u64,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        if process_term == 0 {
            return Err(ProcessLeaseError::Invalid(
                "process lease term must be nonzero".into(),
            ));
        }
        self.verify_current_participant_identity(participant, Some(process_term), deadline)
            .await
    }

    async fn verify_current_participant_identity(
        &self,
        participant: crate::checkpoint::CheckpointParticipant,
        process_term: Option<u64>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        let node = NodeId(participant.node_id);
        if node.is_unassigned() || participant.boot_incarnation.is_nil() {
            return Err(ProcessLeaseError::Invalid(
                "process lease participant is not canonical".into(),
            ));
        }
        let store = self.store_for(node);
        let Some(head) =
            Self::bounded(deadline, "loading current process lease", store.load()).await?
        else {
            return Ok(false);
        };
        if head.owner != participant.boot_incarnation
            || process_term.is_some_and(|term| head.term != term)
        {
            return Ok(false);
        }
        Self::bounded(
            deadline,
            "verifying current process term evidence",
            store.ensure_current_term_fence(&head),
        )
        .await?;
        let Some(after) =
            Self::bounded(deadline, "rechecking current process lease", store.load()).await?
        else {
            return Ok(false);
        };
        Ok(after.owner == head.owner
            && after.term == head.term
            && after.seq >= head.seq
            && process_term.is_none_or(|term| after.term == term))
    }

    /// Verify that both exact fence records remain present and the fenced owner is not current.
    ///
    /// # Errors
    /// Fails closed on deadline expiry, missing history, malformed records, or authority I/O.
    pub async fn verify_fence(
        &self,
        fence: &ProcessLeaseFence,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ProcessLeaseError> {
        if !fence.is_canonical() {
            return Err(ProcessLeaseError::Invalid(
                "process lease fence is not canonical".into(),
            ));
        }
        let store = self.store_for(fence.predecessor.node);
        let durable_fence = Self::bounded(
            deadline,
            "verifying indexed process lease fence",
            ProcessLeaseStore::load_fence(
                &store.store,
                fence.predecessor.node,
                fence.predecessor.owner,
            ),
        )
        .await?
        .ok_or_else(|| {
            ProcessLeaseError::Invalid("indexed process lease fence is missing".into())
        })?;
        let head = Self::bounded(deadline, "verifying process lease fence head", store.load())
            .await?
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease fence durable head is missing".into())
            })?;
        Ok(durable_fence == *fence
            && head.seq >= fence.successor.seq
            && head.term >= fence.successor.term
            && (head.term != fence.successor.term || head.owner == fence.successor.owner)
            && head.owner != fence.predecessor.owner)
    }
}
