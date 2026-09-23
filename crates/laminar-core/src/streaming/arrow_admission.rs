//! Retained Arrow storage admission for in-process source queues.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::StreamingError;

/// Default Arrow-byte limit per source, shared by its input ring and broadcast (64 MiB).
pub const DEFAULT_SOURCE_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;

/// Largest byte limit supported by source semaphore reservations.
pub const MAX_SOURCE_QUEUED_BYTES: usize = if Semaphore::MAX_PERMITS < u32::MAX as usize {
    Semaphore::MAX_PERMITS
} else {
    u32::MAX as usize
};

/// Validates an in-process source's Arrow-byte limit.
///
/// # Errors
/// Returns [`StreamingError::InvalidConfig`] for zero or values above
/// [`MAX_SOURCE_QUEUED_BYTES`].
pub fn validate_source_max_queued_bytes(bytes: usize) -> Result<(), StreamingError> {
    if bytes == 0 || bytes > MAX_SOURCE_QUEUED_BYTES {
        return Err(StreamingError::InvalidConfig(
            "max_queued_bytes must be nonzero and at most MAX_SOURCE_QUEUED_BYTES".into(),
        ));
    }
    Ok(())
}

/// Arrow-reported retained array storage plus fixed batch/column charges.
///
/// Slices, nested arrays and views include their backing storage. Aliases are charged
/// independently. Schema metadata, allocator overhead and caller-owned copies are excluded.
/// Arithmetic overflow saturates so admission fails closed.
#[must_use]
pub fn retained_arrow_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .fold(std::mem::size_of::<RecordBatch>(), |total, column| {
            total
                .saturating_add(column.get_array_memory_size())
                .saturating_add(std::mem::size_of::<ArrayRef>())
        })
}

pub(crate) struct QueuedArrowBatch {
    batch: RecordBatch,
    _permit: OwnedSemaphorePermit,
}

impl QueuedArrowBatch {
    pub(crate) fn admit(
        batch: RecordBatch,
        budget: &Arc<Semaphore>,
        limit: usize,
    ) -> Result<Arc<Self>, StreamingError> {
        validate_source_max_queued_bytes(limit)?;
        let bytes = retained_arrow_bytes(&batch);
        let permits = u32::try_from(bytes)
            .ok()
            .filter(|_| bytes <= limit)
            .ok_or(StreamingError::BatchTooLarge { bytes, limit })?;
        let permit =
            Arc::clone(budget)
                .try_acquire_many_owned(permits)
                .map_err(|error| match error {
                    TryAcquireError::NoPermits => StreamingError::ChannelFull,
                    TryAcquireError::Closed => StreamingError::Disconnected,
                })?;
        Ok(Arc::new(Self {
            batch,
            _permit: permit,
        }))
    }

    pub(crate) fn into_batch(self: Arc<Self>) -> RecordBatch {
        // The last subscriber can transfer the batch without cloning its column vector.
        // Other subscribers share the reservation until their queued references are gone.
        match Arc::try_unwrap(self) {
            Ok(queued) => queued.batch,
            Err(shared) => shared.batch.clone(),
        }
    }
}
