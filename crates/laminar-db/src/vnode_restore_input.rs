//! Resource ownership for raw vnode checkpoint bodies and their logical read envelope.

use std::sync::Arc;

use bytes::Bytes;
use laminar_core::state::SealedPartialReadEnvelope;
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
    read_envelope_bytes: u64,
    inner_alignment_copy_bytes: u64,
}

/// One worker's fail-fast budget for retained raw vnode bodies and their logical read envelope.
#[derive(Debug)]
pub(crate) struct VnodeRestoreInputBudget {
    limits: VnodeRestoreInputLimits,
    read_envelope: SealedPartialReadEnvelope,
    max_read_envelope_bytes: u64,
    reserved: parking_lot::Mutex<ReservedInput>,
    body_reads: Arc<tokio::sync::Semaphore>,
}

impl VnodeRestoreInputBudget {
    pub(crate) fn new(
        limits: VnodeRestoreInputLimits,
        read_envelope: SealedPartialReadEnvelope,
    ) -> Result<Self, DbError> {
        if limits.max_lineage_bytes == 0 || limits.max_lineage_artifacts == 0 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode restore raw-input limits must be nonzero".into(),
            ));
        }
        if read_envelope.payload_multiplier() < 2 {
            return Err(DbError::Checkpoint(
                "[LDB-6050] vnode restore read envelope must charge the returned payload and one worst-case alignment copy"
                    .into(),
            ));
        }
        let max_read_envelope_bytes = read_envelope
            .checked_bytes(limits.max_lineage_bytes, limits.max_lineage_artifacts)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] vnode restore read-envelope limit overflows u64".into(),
                )
            })?;
        Ok(Self {
            limits,
            read_envelope,
            max_read_envelope_bytes,
            reserved: parking_lot::Mutex::new(ReservedInput::default()),
            body_reads: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_VNODE_BODY_READS)),
        })
    }

    #[must_use]
    pub(crate) const fn limits(&self) -> VnodeRestoreInputLimits {
        self.limits
    }

    #[must_use]
    pub(crate) const fn read_envelope(&self) -> SealedPartialReadEnvelope {
        self.read_envelope
    }

    /// Atomically reserve payload, artifact, and logical read-envelope dimensions. Contention
    /// fails before any body read so assignment retry policy, rather than an unbounded waiter,
    /// owns backoff.
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
        let read_envelope_bytes = self
            .read_envelope
            .checked_bytes(bytes, artifacts)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] vnode restore read-envelope reservation overflows u64".into(),
                )
            })?;

        let mut reserved = self.reserved.lock();
        let next_bytes = reserved.bytes.checked_add(bytes).ok_or_else(|| {
            DbError::Checkpoint("[LDB-6050] vnode restore byte reservation overflow".into())
        })?;
        let next_artifacts = reserved.artifacts.checked_add(artifacts).ok_or_else(|| {
            DbError::Checkpoint("[LDB-6050] vnode restore artifact reservation overflow".into())
        })?;
        let next_read_envelope_bytes = reserved
            .read_envelope_bytes
            .checked_add(read_envelope_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] vnode restore read-envelope reservation overflow".into(),
                )
            })?;
        if next_bytes > self.limits.max_lineage_bytes
            || next_artifacts > self.limits.max_lineage_artifacts
            || next_read_envelope_bytes > self.max_read_envelope_bytes
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] vnode restore input reservation unavailable: requested {bytes} bytes/{artifacts} artifacts/{read_envelope_bytes} logical read-envelope bytes with {} bytes/{} artifacts/{} envelope bytes already reserved under a {} byte/{} artifact/{} envelope-byte limit",
                reserved.bytes,
                reserved.artifacts,
                reserved.read_envelope_bytes,
                self.limits.max_lineage_bytes,
                self.limits.max_lineage_artifacts,
                self.max_read_envelope_bytes,
            )));
        }
        reserved.bytes = next_bytes;
        reserved.artifacts = next_artifacts;
        reserved.read_envelope_bytes = next_read_envelope_bytes;
        drop(reserved);

        Ok(VnodeRestoreInputReservation {
            budget: Arc::clone(self),
            bytes,
            artifacts,
            read_envelope_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn reserved_for_test(&self) -> (u64, u64) {
        let reserved = self.reserved.lock();
        (reserved.bytes, reserved.artifacts)
    }

    #[cfg(test)]
    pub(crate) fn reserved_read_envelope_bytes_for_test(&self) -> u64 {
        self.reserved.lock().read_envelope_bytes
    }

    #[cfg(test)]
    pub(crate) fn reserved_inner_alignment_copy_bytes_for_test(&self) -> u64 {
        self.reserved.lock().inner_alignment_copy_bytes
    }

    #[cfg(test)]
    pub(crate) const fn max_read_envelope_bytes_for_test(&self) -> u64 {
        self.max_read_envelope_bytes
    }
}

/// Exact raw-input and logical read-envelope charge carried with retained checkpoint bodies.
#[derive(Debug)]
pub(crate) struct VnodeRestoreInputReservation {
    budget: Arc<VnodeRestoreInputBudget>,
    bytes: u64,
    artifacts: u64,
    read_envelope_bytes: u64,
}

impl VnodeRestoreInputReservation {
    #[must_use]
    pub(crate) fn matches(&self, usage: VnodeRestoreInputUsage) -> bool {
        self.bytes == usage.declared_lineage_bytes()
            && self.artifacts == usage.declared_lineage_artifacts()
            && Some(self.read_envelope_bytes)
                == self.budget.read_envelope.checked_bytes(
                    usage.declared_lineage_bytes(),
                    usage.declared_lineage_artifacts(),
                )
    }

    /// Reserve the exact extra bytes needed to align nested operator archives.
    ///
    /// The copy lane uses the existing committed lineage-byte ceiling. It is separate from the
    /// backend read envelope because nested archives are created only after outer-body decode.
    pub(crate) fn try_reserve_inner_alignment_copy(
        &self,
        bytes: u64,
    ) -> Result<VnodeRestoreAlignmentCopyReservation, DbError> {
        if bytes > self.bytes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] inner archive alignment requires {bytes} copy bytes, exceeding this transition's {}-byte lineage reservation",
                self.bytes
            )));
        }

        let mut reserved = self.budget.reserved.lock();
        let next = reserved
            .inner_alignment_copy_bytes
            .checked_add(bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] inner archive alignment-copy reservation overflow".into(),
                )
            })?;
        if next > self.budget.limits.max_lineage_bytes {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6050] inner archive alignment-copy reservation unavailable: requested {bytes} bytes with {} bytes already reserved under a {}-byte limit",
                reserved.inner_alignment_copy_bytes,
                self.budget.limits.max_lineage_bytes,
            )));
        }
        reserved.inner_alignment_copy_bytes = next;
        drop(reserved);

        Ok(VnodeRestoreAlignmentCopyReservation {
            budget: Arc::clone(&self.budget),
            bytes,
        })
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
            let budget = Arc::new(
                VnodeRestoreInputBudget::new(limits, SealedPartialReadEnvelope::new(2, 0)).unwrap(),
            );
            budget.try_reserve(usage).unwrap()
        })
    }
}

/// Exact charge for nested archives that required a 16-byte-aligned copy.
#[derive(Debug)]
pub(crate) struct VnodeRestoreAlignmentCopyReservation {
    budget: Arc<VnodeRestoreInputBudget>,
    bytes: u64,
}

impl Drop for VnodeRestoreAlignmentCopyReservation {
    fn drop(&mut self) {
        let mut reserved = self.budget.reserved.lock();
        reserved.inner_alignment_copy_bytes = reserved
            .inner_alignment_copy_bytes
            .checked_sub(self.bytes)
            .expect("vnode restore inner alignment-copy reservation ownership");
    }
}

/// One nested operator archive, borrowed when already aligned and owned only when a copy is needed.
#[derive(Debug)]
pub(crate) enum VnodeRestoreArchive<'a> {
    Borrowed(&'a [u8]),
    Aligned(rkyv::util::AlignedVec<16>),
}

impl VnodeRestoreArchive<'_> {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Aligned(bytes) => bytes,
        }
    }

    pub(crate) fn alignment_copy_bytes(&self) -> usize {
        match self {
            Self::Borrowed(bytes) if restore_archive_requires_alignment(bytes) => bytes.len(),
            Self::Borrowed(_) | Self::Aligned(_) => 0,
        }
    }

    pub(crate) fn normalize_alignment(&mut self) -> Result<(), DbError> {
        let Self::Borrowed(bytes) = self else {
            return Ok(());
        };
        if !restore_archive_requires_alignment(bytes) {
            return Ok(());
        }
        if bytes.len() > rkyv::util::AlignedVec::<16>::MAX_CAPACITY {
            return Err(DbError::Checkpoint(
                "[LDB-6050] inner restore archive exceeds aligned allocation capacity".into(),
            ));
        }
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        *self = Self::Aligned(aligned);
        Ok(())
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
        reserved.read_envelope_bytes = reserved
            .read_envelope_bytes
            .checked_sub(self.read_envelope_bytes)
            .expect("vnode restore read-envelope reservation ownership");
    }
}

pub(crate) fn restore_archive_requires_alignment(bytes: &[u8]) -> bool {
    const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

    !bytes.is_empty() && bytes.as_ptr().align_offset(ARCHIVE_ALIGNMENT) != 0
}

/// Give a retained archive the alignment assumed by borrowed restore preflights.
///
/// Outer bodies use the backend read envelope for this possible copy. Nested archives acquire the
/// separate exact copy token above before making the equivalent aligned copy.
pub(crate) fn normalize_restore_archive_alignment(bytes: Bytes) -> Bytes {
    if !restore_archive_requires_alignment(&bytes) {
        return bytes;
    }
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(&bytes);
    Bytes::from_owner(aligned)
}

pub(crate) fn restore_timed_out() -> DbError {
    DbError::Checkpoint("[LDB-6050] vnode restore exceeded its absolute deadline".into())
}

pub(crate) fn restore_cancelled() -> DbError {
    DbError::Checkpoint("[LDB-6050] vnode restore was cancelled before staging".into())
}
