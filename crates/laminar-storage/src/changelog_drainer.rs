//! Ring 1 changelog drainer (F-CKP-005).
//!
//! Consumes entries from the Ring 0 [`StateChangelogBuffer`] and batches
//! them for WAL writes. Runs in the background (Ring 1) on a periodic
//! or checkpoint-triggered schedule.
//!
//! ## Design
//!
//! - Drains the SPSC buffer without allocation (reads pre-allocated entries)
//! - Batches entries for efficient WAL group writes
//! - Tracks drain metrics for observability
//! - Supports explicit flush for checkpoint coordination

use crate::incremental::{StateChangelogBuffer, StateChangelogEntry};

/// Drains a Ring 0 [`StateChangelogBuffer`] from Ring 1.
///
/// The drainer is the consumer side of the SPSC changelog buffer.
/// It collects entries in a batch and makes them available for
/// WAL writes or checkpoint operations.
pub struct ChangelogDrainer {
    /// Reference to the shared changelog buffer (producer: Ring 0, consumer: this).
    buffer: std::sync::Arc<StateChangelogBuffer>,
    /// Accumulated entries since last flush.
    pending: Vec<StateChangelogEntry>,
    /// Maximum batch size before auto-yielding.
    max_batch_size: usize,
    /// Total entries drained over the lifetime of this drainer.
    total_drained: u64,
}

impl ChangelogDrainer {
    /// Creates a new drainer for the given changelog buffer.
    #[must_use]
    pub fn new(buffer: std::sync::Arc<StateChangelogBuffer>, max_batch_size: usize) -> Self {
        Self {
            buffer,
            pending: Vec::with_capacity(max_batch_size),
            max_batch_size,
            total_drained: 0,
        }
    }

    /// Drains available entries from the buffer into the pending batch.
    ///
    /// Returns the number of entries drained in this call.
    pub fn drain(&mut self) -> usize {
        let mut count = 0;
        while count < self.max_batch_size {
            match self.buffer.pop() {
                Some(entry) => {
                    self.pending.push(entry);
                    count += 1;
                }
                None => break,
            }
        }
        self.total_drained += count as u64;
        count
    }

    /// Takes the pending batch, leaving an empty pending buffer.
    ///
    /// After calling this, the drainer's pending buffer is cleared and
    /// ready to accumulate more entries.
    pub fn take_pending(&mut self) -> Vec<StateChangelogEntry> {
        std::mem::take(&mut self.pending)
    }

    /// Returns the number of pending (un-taken) entries.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns a reference to the pending entries.
    #[must_use]
    pub fn pending(&self) -> &[StateChangelogEntry] {
        &self.pending
    }

    /// Returns the total number of entries drained over the drainer's lifetime.
    #[must_use]
    pub fn total_drained(&self) -> u64 {
        self.total_drained
    }

    /// Returns a reference to the underlying changelog buffer.
    #[must_use]
    pub fn buffer(&self) -> &StateChangelogBuffer {
        &self.buffer
    }
}

impl std::fmt::Debug for ChangelogDrainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangelogDrainer")
            .field("pending", &self.pending.len())
            .field("max_batch_size", &self.max_batch_size)
            .field("total_drained", &self.total_drained)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incremental::StateChangelogEntry;
    use std::sync::Arc;

    #[test]
    fn test_drainer_empty_buffer() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));
        let mut drainer = ChangelogDrainer::new(buf, 100);

        assert_eq!(drainer.drain(), 0);
        assert_eq!(drainer.pending_count(), 0);
        assert_eq!(drainer.total_drained(), 0);
    }

    #[test]
    fn test_drainer_basic_drain() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));

        // Push some entries
        buf.push(StateChangelogEntry::put(1, 100, 0, 10));
        buf.push(StateChangelogEntry::put(1, 200, 10, 20));
        buf.push(StateChangelogEntry::delete(1, 300));

        let mut drainer = ChangelogDrainer::new(buf, 100);
        let count = drainer.drain();

        assert_eq!(count, 3);
        assert_eq!(drainer.pending_count(), 3);
        assert_eq!(drainer.total_drained(), 3);
    }

    #[test]
    fn test_drainer_take_pending() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));
        buf.push(StateChangelogEntry::put(1, 100, 0, 10));
        buf.push(StateChangelogEntry::put(1, 200, 10, 20));

        let mut drainer = ChangelogDrainer::new(buf, 100);
        drainer.drain();

        let entries = drainer.take_pending();
        assert_eq!(entries.len(), 2);
        assert_eq!(drainer.pending_count(), 0);

        // Verify entry contents
        assert!(entries[0].is_put());
        assert_eq!(entries[0].key_hash, 100);
        assert!(entries[1].is_put());
        assert_eq!(entries[1].key_hash, 200);
    }

    #[test]
    fn test_drainer_respects_max_batch_size() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));

        // Push more entries than the max batch size
        for i in 0..10 {
            buf.push(StateChangelogEntry::put(1, i, 0, 1));
        }

        let mut drainer = ChangelogDrainer::new(buf, 3);
        let count = drainer.drain();

        // Should only drain 3
        assert_eq!(count, 3);
        assert_eq!(drainer.pending_count(), 3);

        // Drain again for next batch
        let count2 = drainer.drain();
        assert_eq!(count2, 3);
        assert_eq!(drainer.pending_count(), 6);
    }

    #[test]
    fn test_drainer_multiple_drain_cycles() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));

        // First cycle
        buf.push(StateChangelogEntry::put(1, 100, 0, 10));
        let mut drainer = ChangelogDrainer::new(buf.clone(), 100);
        drainer.drain();
        let batch1 = drainer.take_pending();
        assert_eq!(batch1.len(), 1);

        // Second cycle
        buf.push(StateChangelogEntry::delete(2, 200));
        buf.push(StateChangelogEntry::put(2, 300, 20, 30));
        drainer.drain();
        let batch2 = drainer.take_pending();
        assert_eq!(batch2.len(), 2);

        assert_eq!(drainer.total_drained(), 3);
    }

    #[test]
    fn test_drainer_debug() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));
        let drainer = ChangelogDrainer::new(buf, 100);
        let debug = format!("{drainer:?}");
        assert!(debug.contains("ChangelogDrainer"));
        assert!(debug.contains("pending: 0"));
    }
}
