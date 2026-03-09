//! Ring 1 changelog drainer.
//!
//! Consumes entries from the Ring 0 [`StateChangelogBuffer`] to relieve
//! SPSC backpressure. Runs in the background (Ring 1) on a periodic
//! or checkpoint-triggered schedule.
//!
//! ## Design
//!
//! - Drains the SPSC buffer without allocation (reads pre-allocated entries)
//! - Tracks drain metrics for observability
//! - Supports explicit flush for checkpoint coordination
//! - Bounded `pending` buffer prevents unbounded memory growth

use crate::incremental::{StateChangelogBuffer, StateChangelogEntry};

/// Drains a Ring 0 [`StateChangelogBuffer`] from Ring 1.
///
/// The drainer is the consumer side of the SPSC changelog buffer.
/// It pops entries to relieve Ring 0 backpressure and tracks
/// metadata for observability. Pending entries are cleared after
/// each successful checkpoint via [`clear_pending()`](Self::clear_pending).
pub struct ChangelogDrainer {
    /// Reference to the shared changelog buffer (producer: Ring 0, consumer: this).
    buffer: std::sync::Arc<StateChangelogBuffer>,
    /// Accumulated entries since last flush.
    pending: Vec<StateChangelogEntry>,
    /// Maximum entries to pop per drain call.
    max_batch_size: usize,
    /// Upper bound on `pending.len()` — older entries are discarded when exceeded.
    max_pending: usize,
    /// Total entries drained over the lifetime of this drainer.
    total_drained: u64,
}

/// Default upper bound on pending entries (256K entries × 32 bytes ≈ 8 MB).
const DEFAULT_MAX_PENDING: usize = 256 * 1024;

impl ChangelogDrainer {
    /// Creates a new drainer for the given changelog buffer.
    #[must_use]
    pub fn new(buffer: std::sync::Arc<StateChangelogBuffer>, max_batch_size: usize) -> Self {
        Self {
            buffer,
            pending: Vec::with_capacity(max_batch_size),
            max_batch_size,
            max_pending: DEFAULT_MAX_PENDING,
            total_drained: 0,
        }
    }

    /// Sets the upper bound on pending entries.
    ///
    /// When `pending` exceeds this limit during [`drain()`](Self::drain),
    /// older entries are discarded to prevent unbounded memory growth.
    #[must_use]
    pub fn with_max_pending(mut self, max_pending: usize) -> Self {
        self.max_pending = max_pending;
        self
    }

    /// Drains available entries from the buffer into the pending batch.
    ///
    /// If `pending` is at the `max_pending` limit, the oldest half of
    /// pending entries are discarded to make room. Returns the number
    /// of new entries drained from the buffer.
    pub fn drain(&mut self) -> usize {
        // Enforce max_pending: if we're at the limit, shed the oldest half
        // to make room while preserving recent entries. This is preferable
        // to clearing everything — the newest entries are most likely to
        // be needed for the next checkpoint.
        if self.pending.len() >= self.max_pending {
            let keep = self.max_pending / 2;
            let drop_count = self.pending.len() - keep;
            tracing::warn!(
                dropped = drop_count,
                kept = keep,
                max_pending = self.max_pending,
                "changelog drainer pending buffer at limit, shedding oldest entries"
            );
            self.pending.drain(..drop_count);
        }

        let room = self.max_pending.saturating_sub(self.pending.len());
        let limit = self.max_batch_size.min(room);

        let mut count = 0;
        while count < limit {
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
    /// ready to accumulate more entries. The allocation is NOT reused.
    pub fn take_pending(&mut self) -> Vec<StateChangelogEntry> {
        std::mem::take(&mut self.pending)
    }

    /// Clears the pending buffer, reusing the existing allocation.
    ///
    /// Call this after a successful checkpoint to release the metadata
    /// entries — they're no longer needed once the checkpoint has
    /// captured the full state.
    pub fn clear_pending(&mut self) {
        self.pending.clear();
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
            .field("max_pending", &self.max_pending)
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
        assert!(debug.contains("max_pending"));
    }

    #[test]
    fn test_clear_pending_reuses_allocation() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));
        buf.push(StateChangelogEntry::put(1, 100, 0, 10));
        buf.push(StateChangelogEntry::put(1, 200, 10, 20));

        let mut drainer = ChangelogDrainer::new(buf, 100);
        drainer.drain();
        assert_eq!(drainer.pending_count(), 2);

        drainer.clear_pending();
        assert_eq!(drainer.pending_count(), 0);
        // total_drained is not reset
        assert_eq!(drainer.total_drained(), 2);
    }

    #[test]
    fn test_max_pending_bounds() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));

        // Push 10 entries
        for i in 0..10 {
            buf.push(StateChangelogEntry::put(1, i, 0, 1));
        }

        // Create drainer with max_pending = 6
        let mut drainer = ChangelogDrainer::new(buf.clone(), 100).with_max_pending(6);

        // First drain: gets 5 (room = 6 - 0 - 1, but actually 6 entries fit with room=6)
        let count = drainer.drain();
        assert_eq!(count, 6);
        assert_eq!(drainer.pending_count(), 6);

        // Second drain: pending is at max_pending (6 >= 6), so shed oldest half (3),
        // keeping 3 recent entries. Then drain remaining 4 from buffer, but room is
        // only 6-3=3, so only 3 more are drained.
        let count2 = drainer.drain();
        assert_eq!(count2, 3);
        assert_eq!(drainer.pending_count(), 6); // 3 kept + 3 new
        assert_eq!(drainer.total_drained(), 9);

        // Third drain: again at limit, shed oldest half (3), keep 3,
        // drain remaining 1 from buffer.
        let count3 = drainer.drain();
        assert_eq!(count3, 1);
        assert_eq!(drainer.pending_count(), 4); // 3 kept + 1 new
        assert_eq!(drainer.total_drained(), 10);
    }

    #[test]
    fn test_max_pending_does_not_exceed() {
        let buf = Arc::new(StateChangelogBuffer::with_capacity(64));
        for i in 0..3 {
            buf.push(StateChangelogEntry::put(1, i, 0, 1));
        }

        let mut drainer = ChangelogDrainer::new(buf, 100).with_max_pending(5);
        let count = drainer.drain();
        assert_eq!(count, 3);
        assert_eq!(drainer.pending_count(), 3);
        // Still below max_pending, should not clear
    }
}
