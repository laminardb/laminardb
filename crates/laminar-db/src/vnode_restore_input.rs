//! Resource ownership for raw vnode checkpoint bodies during restore.

use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

use crate::error::DbError;

pub(crate) const MAX_CONCURRENT_VNODE_BODY_READS: usize = 32;

/// Immutable accounting for the checkpoint bodies declared and verified by one restore load.
///
/// The declared ceilings come from sealed transitive lineage metadata before any vnode body read.
/// The actual counters include every verified body, including reference-only artifacts omitted
/// from the returned apply chain.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VnodeRestoreInputUsage {
    declared_lineage_bytes: u64,
    declared_lineage_artifacts: u64,
    verified_body_bytes: u64,
    verified_body_artifacts: u64,
}

impl VnodeRestoreInputUsage {
    pub(crate) const fn declared(
        declared_lineage_bytes: u64,
        declared_lineage_artifacts: u64,
    ) -> Self {
        Self {
            declared_lineage_bytes,
            declared_lineage_artifacts,
            verified_body_bytes: 0,
            verified_body_artifacts: 0,
        }
    }

    /// Validate that the receipt can describe the returned vnode-chain collection.
    ///
    /// A nonempty load verifies at least one body per chain. It may consume less than its complete
    /// declared lineage ceiling when every requested operator finds a newer FULL base.
    pub(crate) fn validate_for_loaded_chains(self, chain_count: usize) -> Result<(), &'static str> {
        if chain_count == 0 {
            return if self == Self::default() {
                Ok(())
            } else {
                Err("an empty vnode restore must have zero input usage")
            };
        }
        let chain_count = u64::try_from(chain_count)
            .map_err(|_| "vnode restore chain count does not fit usage accounting")?;
        if self.declared_lineage_bytes == 0
            || self.verified_body_bytes == 0
            || self.declared_lineage_artifacts < chain_count
            || self.verified_body_artifacts < chain_count
        {
            return Err("a nonempty vnode restore has incomplete input usage");
        }
        if self.verified_body_bytes > self.declared_lineage_bytes
            || self.verified_body_artifacts > self.declared_lineage_artifacts
        {
            return Err("verified vnode restore input exceeds its declared lineage ceiling");
        }
        Ok(())
    }

    #[must_use]
    pub(crate) const fn declared_lineage_bytes(self) -> u64 {
        self.declared_lineage_bytes
    }

    #[must_use]
    pub(crate) const fn declared_lineage_artifacts(self) -> u64 {
        self.declared_lineage_artifacts
    }

    #[must_use]
    pub(crate) const fn verified_body_bytes(self) -> u64 {
        self.verified_body_bytes
    }

    #[must_use]
    pub(crate) const fn verified_body_artifacts(self) -> u64 {
        self.verified_body_artifacts
    }

    #[cfg(test)]
    pub(crate) const fn from_counts_for_test(
        declared_lineage_bytes: u64,
        declared_lineage_artifacts: u64,
        verified_body_bytes: u64,
        verified_body_artifacts: u64,
    ) -> Self {
        Self {
            declared_lineage_bytes,
            declared_lineage_artifacts,
            verified_body_bytes,
            verified_body_artifacts,
        }
    }

    pub(crate) fn add_verified_body(&mut self, bytes: u64, artifacts: u64) -> Result<(), DbError> {
        self.verified_body_bytes =
            self.verified_body_bytes.checked_add(bytes).ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] verified vnode restore body byte accounting overflow".into(),
                )
            })?;
        self.verified_body_artifacts = self
            .verified_body_artifacts
            .checked_add(artifacts)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6051] verified vnode restore artifact accounting overflow".into(),
                )
            })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VnodeRestoreInputLimits {
    pub(crate) max_lineage_bytes: u64,
    pub(crate) max_lineage_artifacts: u64,
}

#[derive(Debug, Default)]
struct ReservedInput {
    bytes: u64,
    artifacts: u64,
}

/// One worker's fail-fast budget for retained raw vnode restore bodies.
#[derive(Debug)]
pub(crate) struct VnodeRestoreInputBudget {
    limits: VnodeRestoreInputLimits,
    reserved: parking_lot::Mutex<ReservedInput>,
    body_reads: Arc<tokio::sync::Semaphore>,
}

impl VnodeRestoreInputBudget {
    pub(crate) fn new(limits: VnodeRestoreInputLimits) -> Result<Self, DbError> {
        if limits.max_lineage_bytes == 0 || limits.max_lineage_artifacts == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode restore raw-input limits must be nonzero".into(),
            ));
        }
        Ok(Self {
            limits,
            reserved: parking_lot::Mutex::new(ReservedInput::default()),
            body_reads: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_VNODE_BODY_READS)),
        })
    }

    #[must_use]
    pub(crate) const fn limits(&self) -> VnodeRestoreInputLimits {
        self.limits
    }

    /// Atomically reserve both dimensions. Contention fails before any body read so assignment
    /// retry policy, rather than an unbounded waiter, owns backoff.
    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        usage: VnodeRestoreInputUsage,
    ) -> Result<VnodeRestoreInputReservation, DbError> {
        let bytes = usage.declared_lineage_bytes();
        let artifacts = usage.declared_lineage_artifacts();
        if bytes == 0 || artifacts == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] nonempty vnode restore requires a nonzero raw-input reservation".into(),
            ));
        }

        let mut reserved = self.reserved.lock();
        let next_bytes = reserved.bytes.checked_add(bytes).ok_or_else(|| {
            DbError::Checkpoint("[LDB-6050] vnode restore byte reservation overflow".into())
        })?;
        let next_artifacts = reserved.artifacts.checked_add(artifacts).ok_or_else(|| {
            DbError::Checkpoint("[LDB-6050] vnode restore artifact reservation overflow".into())
        })?;
        if next_bytes > self.limits.max_lineage_bytes
            || next_artifacts > self.limits.max_lineage_artifacts
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] vnode restore raw-input reservation unavailable: requested {bytes} bytes/{artifacts} artifacts with {} bytes/{} artifacts already reserved under a {} byte/{} artifact limit",
                reserved.bytes,
                reserved.artifacts,
                self.limits.max_lineage_bytes,
                self.limits.max_lineage_artifacts
            )));
        }
        reserved.bytes = next_bytes;
        reserved.artifacts = next_artifacts;
        drop(reserved);

        Ok(VnodeRestoreInputReservation {
            budget: Arc::clone(self),
            bytes,
            artifacts,
        })
    }

    #[cfg(test)]
    pub(crate) fn reserved_for_test(&self) -> (u64, u64) {
        let reserved = self.reserved.lock();
        (reserved.bytes, reserved.artifacts)
    }
}

/// Exact raw-input charge carried with the retained checkpoint bodies.
#[derive(Debug)]
pub(crate) struct VnodeRestoreInputReservation {
    budget: Arc<VnodeRestoreInputBudget>,
    bytes: u64,
    artifacts: u64,
}

impl VnodeRestoreInputReservation {
    #[must_use]
    pub(crate) fn matches(&self, usage: VnodeRestoreInputUsage) -> bool {
        self.bytes == usage.declared_lineage_bytes()
            && self.artifacts == usage.declared_lineage_artifacts()
    }

    pub(crate) async fn acquire_body_read(
        &self,
        deadline: tokio::time::Instant,
        cancel: &CancellationToken,
    ) -> Result<OwnedSemaphorePermit, DbError> {
        if cancel.is_cancelled() {
            return Err(restore_cancelled());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(restore_timed_out());
        }
        let acquire = Arc::clone(&self.budget.body_reads).acquire_owned();
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(restore_cancelled()),
            result = tokio::time::timeout_at(deadline, acquire) => match result {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(_)) => Err(DbError::Checkpoint(
                    "[LDB-6050] vnode restore body-read budget is closed".into(),
                )),
                Err(_) => Err(restore_timed_out()),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(usage: VnodeRestoreInputUsage) -> Option<Self> {
        let limits = VnodeRestoreInputLimits {
            max_lineage_bytes: usage.declared_lineage_bytes(),
            max_lineage_artifacts: usage.declared_lineage_artifacts(),
        };
        (limits.max_lineage_bytes > 0 && limits.max_lineage_artifacts > 0).then(|| {
            let budget = Arc::new(VnodeRestoreInputBudget::new(limits).unwrap());
            budget.try_reserve(usage).unwrap()
        })
    }
}

impl Drop for VnodeRestoreInputReservation {
    fn drop(&mut self) {
        let mut reserved = self.budget.reserved.lock();
        reserved.bytes = reserved
            .bytes
            .checked_sub(self.bytes)
            .expect("vnode restore byte reservation ownership");
        reserved.artifacts = reserved
            .artifacts
            .checked_sub(self.artifacts)
            .expect("vnode restore artifact reservation ownership");
    }
}

pub(crate) fn restore_timed_out() -> DbError {
    DbError::Checkpoint("[LDB-6050] vnode restore exceeded its absolute deadline".into())
}

pub(crate) fn restore_cancelled() -> DbError {
    DbError::Checkpoint("[LDB-6050] vnode restore was cancelled before staging".into())
}
