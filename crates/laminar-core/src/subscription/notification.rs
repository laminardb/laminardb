//! Zero-allocation Ring 0 notification mechanism.
//!
//! Provides the bridge between Ring 0 (hot path) and Ring 1 (dispatch) for the
//! reactive subscription system. Three types work together:
//!
//! - [`NotificationSlot`] — per-source atomic sequence counter (64-byte aligned)
//! - [`NotificationRing`] — SPSC pre-allocated ring buffer carrying [`NotificationRef`]
//! - [`NotificationHub`] — manages slots + ring, provides the end-to-end notify path
//!
//! # Ring 0 Contract
//!
//! The [`NotificationSlot::notify`] and [`NotificationHub::notify_source`] methods are
//! designed for the Ring 0 hot path: zero allocations, no locks, single atomic
//! `fetch_add` plus one SPSC push.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::subscription::event::{EventType, NotificationRef};
use crate::tpc::CachePadded;

// ---------------------------------------------------------------------------
// NotificationSlot — per-source atomic sequence counter
// ---------------------------------------------------------------------------

/// Per-source notification slot with atomic sequence counter.
///
/// Occupies exactly one cache line (64 bytes) to prevent false sharing when
/// multiple slots are stored contiguously in [`NotificationHub`].
///
/// # Layout
///
/// ```text
/// offset  0: sequence   (AtomicU64, 8 bytes)
/// offset  8: source_id  (u32, 4 bytes)
/// offset 12: active     (AtomicBool, 1 byte)
/// offset 13: _pad       ([u8; 51])
/// total: 64 bytes = 1 cache line
/// ```
///
/// # Thread Safety
///
/// - `notify()` is called from Ring 0 (single writer per slot).
/// - `current_sequence()` is called from Ring 1 (readers).
/// - `deactivate()` / `reactivate()` are Ring 2 lifecycle operations.
#[repr(C, align(64))]
pub struct NotificationSlot {
    /// Monotonically increasing sequence number, incremented on each `notify()`.
    sequence: AtomicU64,
    /// Immutable source identifier assigned at registration time.
    source_id: u32,
    /// Whether this slot is active. Inactive slots are skipped by the hub.
    active: AtomicBool,
    /// Padding to fill the cache line.
    _pad: [u8; 51],
}

// Compile-time size/alignment assertions
const _: () = assert!(std::mem::size_of::<NotificationSlot>() == 64);
const _: () = assert!(std::mem::align_of::<NotificationSlot>() == 64);

impl NotificationSlot {
    /// Creates a new notification slot for the given source.
    ///
    /// Initial state: `sequence = 0`, `active = true`.
    #[must_use]
    pub const fn new(source_id: u32) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            source_id,
            active: AtomicBool::new(true),
            _pad: [0; 51],
        }
    }

    /// Increments the sequence counter and returns the new value.
    ///
    /// This is the Ring 0 hot-path operation: a single `fetch_add` with
    /// `Release` ordering so that Ring 1 readers see the updated sequence.
    #[inline]
    pub fn notify(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Release) + 1
    }

    /// Returns the current sequence number (Acquire load).
    #[inline]
    #[must_use]
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }

    /// Returns the source identifier.
    #[inline]
    #[must_use]
    pub fn source_id(&self) -> u32 {
        self.source_id
    }

    /// Returns `true` if this slot is active.
    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Marks this slot as inactive (Ring 2 lifecycle operation).
    ///
    /// Inactive slots are skipped by [`NotificationHub::notify_source`].
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }

    /// Re-activates a previously deactivated slot (Ring 2 lifecycle operation).
    pub fn reactivate(&self) {
        self.active.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// NotificationRing — SPSC pre-allocated ring buffer
// ---------------------------------------------------------------------------

/// Lock-free SPSC ring buffer carrying [`NotificationRef`] from Ring 0 to Ring 1.
///
/// Pre-allocates a power-of-2 sized buffer of [`NotificationRef`] slots so that
/// the hot-path `push` never allocates. Uses `UnsafeCell` for interior mutability
/// with the SPSC invariant: single writer (Ring 0), single reader (Ring 1).
///
/// # Capacity
///
/// The capacity is always rounded up to the next power of two. This allows
/// fast index computation via bitmask (`pos & mask`) instead of modulo.
pub struct NotificationRing {
    /// Pre-allocated slot buffer.
    buffer: Box<[UnsafeCell<NotificationRef>]>,
    /// Writer position (Ring 0, separate cache line from `read_pos`).
    write_pos: CachePadded<AtomicU64>,
    /// Reader position (Ring 1, separate cache line from `write_pos`).
    read_pos: CachePadded<AtomicU64>,
    /// Power-of-2 capacity.
    capacity: usize,
    /// Bitmask for fast modulo: `capacity - 1`.
    mask: usize,
}

// SAFETY: NotificationRing is designed for SPSC use — a single writer thread
// (Ring 0) calls `push`, and a single reader thread (Ring 1) calls `pop`.
// These never access the same slot concurrently because `write_pos` is always
// ahead of `read_pos`, and the writer only writes to slots the reader has
// already consumed. The atomic positions enforce the correct ordering.
unsafe impl Send for NotificationRing {}
unsafe impl Sync for NotificationRing {}

impl NotificationRing {
    /// Creates a new notification ring with the given capacity.
    ///
    /// The capacity is rounded up to the next power of two.
    /// Minimum effective capacity is 2.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(2).next_power_of_two();
        let mask = capacity - 1;

        // Pre-allocate zeroed slots.
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(UnsafeCell::new(NotificationRef::new(
                0,
                0,
                EventType::Insert,
                0,
                0,
                0,
            )));
        }

        Self {
            buffer: slots.into_boxed_slice(),
            write_pos: CachePadded::new(AtomicU64::new(0)),
            read_pos: CachePadded::new(AtomicU64::new(0)),
            capacity,
            mask,
        }
    }

    /// Pushes a notification into the ring (Ring 0 writer).
    ///
    /// Returns `true` on success, `false` if the ring is full (backpressure).
    /// Zero allocations on the hot path.
    #[inline]
    pub fn push(&self, notif: NotificationRef) -> bool {
        let write = self.write_pos.load(Ordering::Relaxed);
        let read = self.read_pos.load(Ordering::Acquire);

        // Full when writer is one full lap ahead of reader.
        if write.wrapping_sub(read) >= self.capacity as u64 {
            return false;
        }

        #[allow(clippy::cast_possible_truncation)] // masked to capacity
        let idx = (write as usize) & self.mask;
        // SAFETY: Single writer (Ring 0). The slot at `idx` has been consumed
        // by the reader (or is in the initial pre-allocated state) because
        // write - read < capacity.
        unsafe {
            *self.buffer[idx].get() = notif;
        }
        self.write_pos
            .store(write.wrapping_add(1), Ordering::Release);
        true
    }

    /// Pops a notification from the ring (Ring 1 reader).
    ///
    /// Returns `None` if the ring is empty.
    #[inline]
    pub fn pop(&self) -> Option<NotificationRef> {
        let read = self.read_pos.load(Ordering::Relaxed);
        let write = self.write_pos.load(Ordering::Acquire);

        if read == write {
            return None;
        }

        #[allow(clippy::cast_possible_truncation)] // masked to capacity
        let idx = (read as usize) & self.mask;
        // SAFETY: Single reader (Ring 1). The slot at `idx` has been written
        // by the writer because write > read.
        let notif = unsafe { *self.buffer[idx].get() };
        self.read_pos.store(read.wrapping_add(1), Ordering::Release);
        Some(notif)
    }

    /// Drains all pending notifications, calling `f` for each one.
    ///
    /// Returns the number of notifications drained.
    #[inline]
    pub fn drain_into<F: FnMut(NotificationRef)>(&self, mut f: F) -> usize {
        let mut count = 0;
        while let Some(notif) = self.pop() {
            f(notif);
            count += 1;
        }
        count
    }

    /// Returns the number of pending (unread) notifications.
    #[must_use]
    pub fn len(&self) -> usize {
        let write = self.write_pos.load(Ordering::Acquire);
        let read = self.read_pos.load(Ordering::Acquire);
        #[allow(clippy::cast_possible_truncation)] // bounded by capacity
        let len = write.wrapping_sub(read) as usize;
        len
    }

    /// Returns `true` if there are no pending notifications.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the ring capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

// ---------------------------------------------------------------------------
// NotificationHub — slot registry + ring integration
// ---------------------------------------------------------------------------

/// Manages notification slots and the SPSC ring for a single reactor core.
///
/// Provides the end-to-end path from Ring 0 event emission to Ring 1 dispatch:
///
/// 1. **Registration** (Ring 2): [`Self::register_source`] allocates a slot.
/// 2. **Notification** (Ring 0): [`Self::notify_source`] increments the slot sequence
///    and pushes a [`NotificationRef`] into the SPSC ring.
/// 3. **Drain** (Ring 1): [`Self::drain_notifications`] pops all pending notifications.
pub struct NotificationHub {
    /// Notification slots indexed by `source_id`.
    slots: Vec<NotificationSlot>,
    /// SPSC ring carrying notifications from Ring 0 to Ring 1.
    ring: NotificationRing,
    /// Next `source_id` to assign.
    next_id: u32,
    /// Maximum number of slots.
    max_slots: usize,
}

impl NotificationHub {
    /// Creates a new notification hub.
    ///
    /// # Arguments
    ///
    /// * `max_slots` — Maximum number of source slots (determines Vec capacity).
    /// * `ring_capacity` — SPSC ring capacity (rounded up to power of 2).
    #[must_use]
    pub fn new(max_slots: usize, ring_capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(max_slots),
            ring: NotificationRing::new(ring_capacity),
            next_id: 0,
            max_slots,
        }
    }

    /// Registers a new source and returns its `source_id`.
    ///
    /// Returns `None` if the maximum slot count has been reached.
    /// This is a Ring 2 operation (may allocate).
    pub fn register_source(&mut self) -> Option<u32> {
        if self.slots.len() >= self.max_slots {
            return None;
        }
        let id = self.next_id;
        self.slots.push(NotificationSlot::new(id));
        self.next_id += 1;
        Some(id)
    }

    /// Marks the given source as inactive.
    ///
    /// Inactive sources are skipped by [`Self::notify_source`]. The slot is not
    /// deallocated — use [`NotificationSlot::reactivate`] to re-enable.
    pub fn deactivate_source(&self, source_id: u32) {
        if let Some(slot) = self.slots.get(source_id as usize) {
            slot.deactivate();
        }
    }

    /// Notifies Ring 1 of a data change on the given source (Ring 0 hot path).
    ///
    /// Increments the slot's sequence counter and pushes a [`NotificationRef`]
    /// into the SPSC ring. Returns `false` if:
    /// - The `source_id` is out of range
    /// - The slot is inactive
    /// - The ring is full (backpressure)
    #[inline]
    pub fn notify_source(
        &self,
        source_id: u32,
        event_type: EventType,
        row_count: u32,
        timestamp: i64,
        batch_offset: u64,
    ) -> bool {
        let Some(slot) = self.slots.get(source_id as usize) else {
            return false;
        };
        if !slot.is_active() {
            return false;
        }
        let seq = slot.notify();
        let notif = NotificationRef::new(
            seq,
            source_id,
            event_type,
            row_count,
            timestamp,
            batch_offset,
        );
        self.ring.push(notif)
    }

    /// Drains all pending notifications from the ring, calling `f` for each.
    ///
    /// Returns the number of notifications drained. This is a Ring 1 operation.
    #[inline]
    pub fn drain_notifications<F: FnMut(NotificationRef)>(&self, f: F) -> usize {
        self.ring.drain_into(f)
    }

    /// Returns a reference to the underlying notification ring.
    #[must_use]
    pub fn notification_ring(&self) -> &NotificationRing {
        &self.ring
    }

    /// Returns the number of registered sources.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.slots.len()
    }

    /// Returns a reference to the slot for the given `source_id`.
    #[must_use]
    pub fn slot(&self, source_id: u32) -> Option<&NotificationSlot> {
        self.slots.get(source_id as usize)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use std::mem;

    // --- NotificationSlot tests ---

    #[test]
    fn test_notification_slot_size() {
        assert_eq!(mem::size_of::<NotificationSlot>(), 64);
        assert_eq!(mem::align_of::<NotificationSlot>(), 64);
    }

    #[test]
    fn test_notification_slot_new() {
        let slot = NotificationSlot::new(7);
        assert_eq!(slot.source_id(), 7);
        assert_eq!(slot.current_sequence(), 0);
        assert!(slot.is_active());
    }

    #[test]
    fn test_notification_slot_notify() {
        let slot = NotificationSlot::new(0);
        assert_eq!(slot.notify(), 1);
        assert_eq!(slot.notify(), 2);
        assert_eq!(slot.notify(), 3);
        assert_eq!(slot.current_sequence(), 3);
    }

    #[test]
    fn test_notification_slot_deactivate() {
        let slot = NotificationSlot::new(0);
        assert!(slot.is_active());
        slot.deactivate();
        assert!(!slot.is_active());
    }

    #[test]
    fn test_notification_slot_reactivate() {
        let slot = NotificationSlot::new(0);
        slot.deactivate();
        assert!(!slot.is_active());
        slot.reactivate();
        assert!(slot.is_active());
    }

    // --- NotificationRing tests ---

    #[test]
    fn test_notification_ring_new() {
        // Rounds up to power of 2
        let ring = NotificationRing::new(3);
        assert_eq!(ring.capacity(), 4);

        let ring = NotificationRing::new(8);
        assert_eq!(ring.capacity(), 8);

        // Minimum capacity is 2
        let ring = NotificationRing::new(1);
        assert_eq!(ring.capacity(), 2);

        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn test_notification_ring_push_pop() {
        let ring = NotificationRing::new(4);
        let notif = NotificationRef::new(1, 0, EventType::Insert, 10, 1000, 0);

        assert!(ring.push(notif));
        assert_eq!(ring.len(), 1);

        let popped = ring.pop().unwrap();
        assert_eq!(popped.sequence, 1);
        assert_eq!(popped.source_id, 0);
        assert_eq!(popped.event_type, EventType::Insert);
        assert_eq!(popped.row_count, 10);
        assert_eq!(popped.timestamp, 1000);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_notification_ring_ordering() {
        let ring = NotificationRing::new(8);
        for i in 0..4u64 {
            let notif = NotificationRef::new(i, 0, EventType::Insert, 0, 0, 0);
            assert!(ring.push(notif));
        }
        // FIFO order preserved
        for i in 0..4u64 {
            let popped = ring.pop().unwrap();
            assert_eq!(popped.sequence, i);
        }
        assert!(ring.pop().is_none());
    }

    #[test]
    fn test_notification_ring_full() {
        let ring = NotificationRing::new(4);
        // Fill to capacity
        for i in 0..4u64 {
            let notif = NotificationRef::new(i, 0, EventType::Insert, 0, 0, 0);
            assert!(ring.push(notif));
        }
        assert_eq!(ring.len(), 4);

        // Next push should fail (backpressure)
        let notif = NotificationRef::new(99, 0, EventType::Insert, 0, 0, 0);
        assert!(!ring.push(notif));
    }

    #[test]
    fn test_notification_ring_empty() {
        let ring = NotificationRing::new(4);
        assert!(ring.pop().is_none());
        assert!(ring.is_empty());
    }

    #[test]
    fn test_notification_ring_wraparound() {
        let ring = NotificationRing::new(4); // capacity = 4
                                             // Fill and drain multiple times to exercise wraparound
        for round in 0..5u64 {
            for i in 0..4u64 {
                let seq = round * 4 + i;
                let notif = NotificationRef::new(seq, 0, EventType::Insert, 0, 0, 0);
                assert!(ring.push(notif), "push failed at round={round} i={i}");
            }
            for i in 0..4u64 {
                let expected = round * 4 + i;
                let popped = ring.pop().unwrap();
                assert_eq!(popped.sequence, expected);
            }
            assert!(ring.is_empty());
        }
    }

    #[test]
    fn test_notification_ring_drain() {
        let ring = NotificationRing::new(8);
        for i in 0..5u64 {
            let notif = NotificationRef::new(i, 0, EventType::Insert, 0, 0, 0);
            ring.push(notif);
        }

        let mut collected = Vec::new();
        let count = ring.drain_into(|n| collected.push(n.sequence));
        assert_eq!(count, 5);
        assert_eq!(collected, vec![0, 1, 2, 3, 4]);
        assert!(ring.is_empty());
    }

    // --- NotificationHub tests ---

    #[test]
    fn test_notification_hub_register() {
        let mut hub = NotificationHub::new(4, 16);
        assert_eq!(hub.register_source(), Some(0));
        assert_eq!(hub.register_source(), Some(1));
        assert_eq!(hub.register_source(), Some(2));
        assert_eq!(hub.source_count(), 3);
    }

    #[test]
    fn test_notification_hub_notify() {
        let mut hub = NotificationHub::new(4, 16);
        let id = hub.register_source().unwrap();

        // Notify and drain
        assert!(hub.notify_source(id, EventType::Insert, 10, 1000, 0));
        assert!(hub.notify_source(id, EventType::Delete, 5, 2000, 64));

        let mut notifications = Vec::new();
        let count = hub.drain_notifications(|n| notifications.push(n));
        assert_eq!(count, 2);

        assert_eq!(notifications[0].sequence, 1);
        assert_eq!(notifications[0].source_id, id);
        assert_eq!(notifications[0].event_type, EventType::Insert);
        assert_eq!(notifications[0].row_count, 10);
        assert_eq!(notifications[0].timestamp, 1000);

        assert_eq!(notifications[1].sequence, 2);
        assert_eq!(notifications[1].event_type, EventType::Delete);
        assert_eq!(notifications[1].row_count, 5);
        assert_eq!(notifications[1].timestamp, 2000);
        assert_eq!(notifications[1].batch_offset, 64);
    }

    #[test]
    fn test_notification_hub_deactivate() {
        let mut hub = NotificationHub::new(4, 16);
        let id = hub.register_source().unwrap();

        // Notify succeeds while active
        assert!(hub.notify_source(id, EventType::Insert, 1, 100, 0));

        // Deactivate — notify returns false
        hub.deactivate_source(id);
        assert!(!hub.notify_source(id, EventType::Insert, 1, 200, 0));

        // Only the first notification should be in the ring
        let mut count = 0;
        hub.drain_notifications(|_| count += 1);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_notification_hub_max_slots() {
        let mut hub = NotificationHub::new(2, 16);
        assert_eq!(hub.register_source(), Some(0));
        assert_eq!(hub.register_source(), Some(1));
        // At capacity — returns None
        assert_eq!(hub.register_source(), None);
        assert_eq!(hub.source_count(), 2);
    }

    #[test]
    fn test_notification_hub_slot_access() {
        let mut hub = NotificationHub::new(4, 16);
        let id = hub.register_source().unwrap();

        let slot = hub.slot(id).unwrap();
        assert_eq!(slot.source_id(), id);
        assert!(slot.is_active());

        // Out-of-range returns None
        assert!(hub.slot(99).is_none());
    }

    #[test]
    fn test_notification_ring_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(NotificationRing::new(1024));
        let ring_writer = Arc::clone(&ring);
        let ring_reader = Arc::clone(&ring);

        let n = 10_000u64;

        let writer = thread::spawn(move || {
            let mut pushed = 0u64;
            while pushed < n {
                let notif = NotificationRef::new(pushed, 0, EventType::Insert, 0, 0, 0);
                if ring_writer.push(notif) {
                    pushed += 1;
                } else {
                    // Backpressure — spin
                    std::hint::spin_loop();
                }
            }
        });

        let reader = thread::spawn(move || {
            let mut received = Vec::with_capacity(n as usize);
            while received.len() < n as usize {
                if let Some(notif) = ring_reader.pop() {
                    received.push(notif.sequence);
                } else {
                    std::hint::spin_loop();
                }
            }
            received
        });

        writer.join().unwrap();
        let received = reader.join().unwrap();

        // Verify all items received in order
        assert_eq!(received.len(), n as usize);
        for (i, &seq) in received.iter().enumerate() {
            assert_eq!(seq, i as u64, "out-of-order at index {i}");
        }
    }
}
