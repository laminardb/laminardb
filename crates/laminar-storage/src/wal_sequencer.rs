//! WAL Sequencer for parallel ingestion.
//!
//! Inspired by QuestDB's WAL sequencer, this module provides a lightweight
//! sequencing layer that assigns monotonic sequence IDs to WAL segments
//! from multiple concurrent writers. This enables multiple source tasks
//! to write WAL segments concurrently while the coordinator applies
//! them in sequence order.
//!
//! # Architecture
//!
//! ```text
//! Source Task 0 ──► WalSegmentWriter ──┐
//! Source Task 1 ──► WalSegmentWriter ──┼──► Sequencer ──► Coordinator
//! Source Task 2 ──► WalSegmentWriter ──┘     (AtomicU64)    (applies in order)
//! ```
//!
//! # Key Invariant
//!
//! Segments are applied in strictly increasing sequence order. If segment
//! N+1 arrives before segment N, it is held in a reorder buffer until N
//! is applied first.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A sequenced WAL segment produced by a writer.
#[derive(Debug, Clone)]
pub struct SequencedSegment {
    /// Monotonic sequence ID assigned by the sequencer.
    pub sequence_id: u64,
    /// Writer ID that produced this segment.
    pub writer_id: usize,
    /// Serialized WAL entries (opaque bytes).
    pub data: Vec<u8>,
    /// Number of entries in this segment.
    pub entry_count: usize,
}

/// Lightweight sequencer that assigns monotonic IDs to WAL segments.
///
/// Thread-safe: multiple writers can call `next_id()` concurrently.
/// The `AtomicU64` counter ensures unique, strictly increasing IDs
/// without locks.
#[derive(Debug)]
pub struct WalSequencer {
    /// Monotonic counter for sequence IDs.
    next_id: AtomicU64,
    /// Number of registered writers.
    num_writers: usize,
}

impl WalSequencer {
    /// Create a new sequencer for the given number of writers.
    #[must_use]
    pub fn new(num_writers: usize) -> Self {
        Self {
            next_id: AtomicU64::new(0),
            num_writers,
        }
    }

    /// Assign the next monotonic sequence ID.
    ///
    /// This is lock-free and safe to call from multiple threads.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the current sequence counter value (next ID to be assigned).
    #[must_use]
    pub fn current(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }

    /// Get the number of registered writers.
    #[must_use]
    pub fn num_writers(&self) -> usize {
        self.num_writers
    }

    /// Create a shared handle to this sequencer.
    #[must_use]
    pub fn into_shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}

/// Reorder buffer that accumulates out-of-order segments and yields
/// them in sequence order.
///
/// The coordinator inserts segments as they arrive (possibly out of order)
/// and drains contiguous segments starting from `next_expected`.
#[derive(Debug)]
pub struct SegmentReorderBuffer {
    /// Next sequence ID expected for in-order application.
    next_expected: u64,
    /// Buffered out-of-order segments, keyed by sequence ID.
    pending: BTreeMap<u64, SequencedSegment>,
    /// Total segments applied in order.
    total_applied: u64,
    /// Total segments that arrived out of order (needed buffering).
    total_reordered: u64,
}

impl SegmentReorderBuffer {
    /// Create a new reorder buffer starting at sequence 0.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_expected: 0,
            pending: BTreeMap::new(),
            total_applied: 0,
            total_reordered: 0,
        }
    }

    /// Insert a segment into the reorder buffer.
    ///
    /// Returns a vec of contiguous segments ready for application
    /// (in sequence order). If the segment fills a gap, multiple
    /// segments may be returned.
    pub fn insert(&mut self, segment: SequencedSegment) -> Vec<SequencedSegment> {
        let seq = segment.sequence_id;

        if seq < self.next_expected {
            // Duplicate or stale segment — skip.
            tracing::warn!(
                sequence_id = seq,
                next_expected = self.next_expected,
                "WAL sequencer: stale segment received, skipping"
            );
            return Vec::new();
        }

        if seq > self.next_expected {
            // Out of order — buffer it.
            self.total_reordered += 1;
            self.pending.insert(seq, segment);
            return Vec::new();
        }

        // seq == next_expected: this segment is next in line.
        let mut ready = vec![segment];
        self.next_expected += 1;
        self.total_applied += 1;

        // Drain any contiguous buffered segments.
        while let Some(entry) = self.pending.remove(&self.next_expected) {
            ready.push(entry);
            self.next_expected += 1;
            self.total_applied += 1;
        }

        ready
    }

    /// Get the next expected sequence ID.
    #[must_use]
    pub fn next_expected(&self) -> u64 {
        self.next_expected
    }

    /// Get the number of segments currently buffered (waiting for gaps).
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get total segments applied in order.
    #[must_use]
    pub fn total_applied(&self) -> u64 {
        self.total_applied
    }

    /// Get total segments that arrived out of order.
    #[must_use]
    pub fn total_reordered(&self) -> u64 {
        self.total_reordered
    }
}

impl Default for SegmentReorderBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_segment(seq: u64, writer: usize) -> SequencedSegment {
        SequencedSegment {
            sequence_id: seq,
            writer_id: writer,
            data: vec![seq as u8; 10],
            entry_count: 1,
        }
    }

    #[test]
    fn test_sequencer_monotonic() {
        let seq = WalSequencer::new(4);
        assert_eq!(seq.next_id(), 0);
        assert_eq!(seq.next_id(), 1);
        assert_eq!(seq.next_id(), 2);
        assert_eq!(seq.current(), 3);
    }

    #[test]
    fn test_sequencer_concurrent() {
        let seq = Arc::new(WalSequencer::new(4));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let seq = Arc::clone(&seq);
                std::thread::spawn(move || {
                    let mut ids = Vec::new();
                    for _ in 0..1000 {
                        ids.push(seq.next_id());
                    }
                    ids
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all_ids.sort_unstable();
        all_ids.dedup();

        // All 4000 IDs should be unique.
        assert_eq!(all_ids.len(), 4000);
        // Should be 0..4000.
        assert_eq!(*all_ids.first().unwrap(), 0);
        assert_eq!(*all_ids.last().unwrap(), 3999);
    }

    #[test]
    fn test_reorder_buffer_in_order() {
        let mut buf = SegmentReorderBuffer::new();

        let ready = buf.insert(make_segment(0, 0));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].sequence_id, 0);

        let ready = buf.insert(make_segment(1, 1));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].sequence_id, 1);

        assert_eq!(buf.next_expected(), 2);
        assert_eq!(buf.total_applied(), 2);
        assert_eq!(buf.total_reordered(), 0);
    }

    #[test]
    fn test_reorder_buffer_out_of_order() {
        let mut buf = SegmentReorderBuffer::new();

        // Segment 2 arrives first.
        let ready = buf.insert(make_segment(2, 0));
        assert!(ready.is_empty());
        assert_eq!(buf.pending_count(), 1);

        // Segment 1 arrives.
        let ready = buf.insert(make_segment(1, 1));
        assert!(ready.is_empty());
        assert_eq!(buf.pending_count(), 2);

        // Segment 0 arrives — should drain 0, 1, 2.
        let ready = buf.insert(make_segment(0, 2));
        assert_eq!(ready.len(), 3);
        assert_eq!(ready[0].sequence_id, 0);
        assert_eq!(ready[1].sequence_id, 1);
        assert_eq!(ready[2].sequence_id, 2);

        assert_eq!(buf.next_expected(), 3);
        assert_eq!(buf.pending_count(), 0);
        assert_eq!(buf.total_reordered(), 2);
    }

    #[test]
    fn test_reorder_buffer_gap() {
        let mut buf = SegmentReorderBuffer::new();

        // Insert 0, then 2 (gap at 1).
        let ready = buf.insert(make_segment(0, 0));
        assert_eq!(ready.len(), 1);

        let ready = buf.insert(make_segment(2, 0));
        assert!(ready.is_empty());
        assert_eq!(buf.pending_count(), 1);

        // Fill the gap.
        let ready = buf.insert(make_segment(1, 0));
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].sequence_id, 1);
        assert_eq!(ready[1].sequence_id, 2);
    }

    #[test]
    fn test_reorder_buffer_duplicate() {
        let mut buf = SegmentReorderBuffer::new();

        let ready = buf.insert(make_segment(0, 0));
        assert_eq!(ready.len(), 1);

        // Duplicate of 0 — should be skipped.
        let ready = buf.insert(make_segment(0, 0));
        assert!(ready.is_empty());
    }

    #[test]
    fn test_sequencer_num_writers() {
        let seq = WalSequencer::new(8);
        assert_eq!(seq.num_writers(), 8);
    }
}
