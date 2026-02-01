# F-SUB-002: Notification Slot (Ring 0)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-002 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SUB-001, F013 (Thread-Per-Core), F014 (SPSC Queues) |
| **Blocks** | F-SUB-004 (Dispatcher) |
| **Created** | 2026-02-01 |

## Summary

Implement a zero-allocation notification mechanism in Ring 0 that signals data changes to Ring 1. Each materialized view / operator output gets a cache-line-aligned `NotificationSlot` with an atomic sequence counter. On data mutation, Ring 0 increments the sequence number. A per-core SPSC notification ring carries `NotificationRef` values to the Ring 1 dispatcher without blocking or allocating.

**Research Reference**: [Reactive Subscriptions Research - Section 4: Zero-Copy Push Notifications](../../../research/reactive-subscriptions-research-2026.md)

> Key insight from research: "Separate the notification (lightweight signal) from the data (zero-copy reference). Don't push the actual data through the channel."

## Requirements

### Functional Requirements

- **FR-1**: One `NotificationSlot` per registered MV/operator output
- **FR-2**: `notify()` increments atomic sequence counter (single instruction on x86)
- **FR-3**: Per-core SPSC notification ring carries `NotificationRef` to Ring 1
- **FR-4**: `NotificationHub` manages all slots, supports dynamic registration
- **FR-5**: Slot allocation is a Ring 2 operation (not hot path)
- **FR-6**: Support per-core notification isolation (thread-per-core compatible)

### Non-Functional Requirements

- **NFR-1**: `notify()` latency < 100ns (atomic fetch_add + SPSC push)
- **NFR-2**: Zero heap allocations on notification path
- **NFR-3**: Cache-line aligned slots to prevent false sharing
- **NFR-4**: Lock-free: no mutexes, no RwLock on hot path
- **NFR-5**: Supports 1000+ concurrent notification slots

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RING 0: HOT PATH                         │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                  NotificationHub                         │   │
│  │                                                          │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │   │
│  │  │ Slot[0]      │  │ Slot[1]      │  │ Slot[n]      │  │   │
│  │  │ seq: AtomicU64│  │ seq: AtomicU64│  │ seq: AtomicU64│  │   │
│  │  │ source_id: 0 │  │ source_id: 1 │  │ source_id: n │  │   │
│  │  │ @align(64)   │  │ @align(64)   │  │ @align(64)   │  │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │   │
│  │         │                  │                  │          │   │
│  │         └──────────────────┼──────────────────┘          │   │
│  │                            │                             │   │
│  │                            ▼                             │   │
│  │                   ┌────────────────┐                     │   │
│  │                   │ SPSC Notif Ring│ ◀── per-core        │   │
│  │                   │ (pre-allocated │                     │   │
│  │                   │  NotificationRef)                    │   │
│  │                   └───────┬────────┘                     │   │
│  └───────────────────────────┼──────────────────────────────┘   │
│                              │                                   │
├──────────────────────────────┼───────────────────────────────────┤
│                        RING 1│                                   │
│                              ▼                                   │
│                   ┌──────────────────┐                           │
│                   │   Dispatcher     │ (F-SUB-004)               │
│                   └──────────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use crate::tpc::CachePadded;

/// Cache-line-aligned notification slot for a single MV/operator output.
///
/// Each slot is independent and owned by a single Ring 0 core.
/// The sequence number is incremented on every data mutation.
/// Ring 1 polls the SPSC notification ring, not the slots directly.
///
/// # Performance
///
/// - `notify()`: Single atomic fetch_add + SPSC push = ~20-50ns
/// - No false sharing: each slot is 64-byte aligned
/// - No locks: pure atomic operations
#[repr(C, align(64))]
pub struct NotificationSlot {
    /// Monotonically increasing sequence number.
    /// Incremented on every data change for this source.
    sequence: AtomicU64,
    /// Source/MV identifier (immutable after creation).
    source_id: u32,
    /// Whether this slot is active (can be deactivated without deallocation).
    active: AtomicBool,
    /// Padding to fill cache line.
    _pad: [u8; 43],
}

impl NotificationSlot {
    /// Creates a new notification slot.
    pub const fn new(source_id: u32) -> Self {
        Self {
            sequence: AtomicU64::new(0),
            source_id,
            active: AtomicBool::new(true),
            _pad: [0; 43],
        }
    }

    /// Notifies subscribers of a data change.
    ///
    /// Returns the new sequence number. This is the only method
    /// called on the Ring 0 hot path.
    ///
    /// # Performance
    ///
    /// Single `fetch_add` instruction on x86 (~5-10ns).
    #[inline(always)]
    pub fn notify(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Release) + 1
    }

    /// Returns the current sequence number without incrementing.
    #[inline]
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Acquire)
    }

    /// Returns the source ID for this slot.
    #[inline]
    pub fn source_id(&self) -> u32 {
        self.source_id
    }

    /// Returns true if this slot is active.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Deactivates this slot (Ring 2 operation).
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }
}

/// Pre-allocated notification ring for SPSC transfer from Ring 0 to Ring 1.
///
/// This is a fixed-capacity ring buffer of `NotificationRef` values.
/// Ring 0 writes, Ring 1 reads. No allocation on push or pop.
///
/// One ring per core in thread-per-core mode.
pub struct NotificationRing {
    /// Pre-allocated buffer of notification refs.
    buffer: Box<[NotificationRef]>,
    /// Write position (Ring 0 writer).
    write_pos: CachePadded<AtomicU64>,
    /// Read position (Ring 1 reader).
    read_pos: CachePadded<AtomicU64>,
    /// Capacity (power of 2).
    capacity: usize,
    /// Mask for fast modulo (capacity - 1).
    mask: usize,
}

impl NotificationRing {
    /// Creates a new notification ring with the given capacity.
    ///
    /// Capacity is rounded up to the next power of 2.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        let buffer = (0..capacity)
            .map(|_| NotificationRef::new(0, 0, EventType::Insert, 0, 0, 0))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            buffer,
            write_pos: CachePadded::new(AtomicU64::new(0)),
            read_pos: CachePadded::new(AtomicU64::new(0)),
            capacity,
            mask: capacity - 1,
        }
    }

    /// Pushes a notification reference (Ring 0 writer).
    ///
    /// Returns `false` if the ring is full (backpressure signal).
    /// Does NOT allocate.
    #[inline(always)]
    pub fn push(&self, notif: NotificationRef) -> bool {
        let write = self.write_pos.load(Ordering::Relaxed);
        let read = self.read_pos.load(Ordering::Acquire);

        // Check if full
        if write.wrapping_sub(read) >= self.capacity as u64 {
            return false;
        }

        let idx = (write as usize) & self.mask;

        // SAFETY: idx is bounded by mask, and we checked capacity.
        // Single writer guaranteed by thread-per-core.
        unsafe {
            let slot = self.buffer.as_ptr().add(idx) as *mut NotificationRef;
            std::ptr::write(slot, notif);
        }

        self.write_pos.store(write.wrapping_add(1), Ordering::Release);
        true
    }

    /// Pops a notification reference (Ring 1 reader).
    ///
    /// Returns `None` if the ring is empty.
    #[inline]
    pub fn pop(&self) -> Option<NotificationRef> {
        let read = self.read_pos.load(Ordering::Relaxed);
        let write = self.write_pos.load(Ordering::Acquire);

        if read >= write {
            return None;
        }

        let idx = (read as usize) & self.mask;
        let notif = self.buffer[idx];

        self.read_pos.store(read.wrapping_add(1), Ordering::Release);
        Some(notif)
    }

    /// Drains all available notifications into a callback.
    ///
    /// Returns the number of notifications drained.
    #[inline]
    pub fn drain_into<F>(&self, mut f: F) -> usize
    where
        F: FnMut(NotificationRef),
    {
        let mut count = 0;
        while let Some(notif) = self.pop() {
            f(notif);
            count += 1;
        }
        count
    }

    /// Returns the number of pending notifications.
    #[inline]
    pub fn len(&self) -> usize {
        let write = self.write_pos.load(Ordering::Acquire);
        let read = self.read_pos.load(Ordering::Acquire);
        write.wrapping_sub(read) as usize
    }

    /// Returns true if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Hub managing all notification slots for a core.
///
/// Ring 2 registers/unregisters slots.
/// Ring 0 calls `notify_source()` on data change.
/// Ring 1 drains the notification ring via `drain_notifications()`.
pub struct NotificationHub {
    /// Notification slots indexed by source_id.
    /// Pre-allocated array for O(1) lookup.
    slots: Vec<NotificationSlot>,
    /// SPSC notification ring to Ring 1.
    ring: NotificationRing,
    /// Next available source_id.
    next_id: u32,
    /// Maximum slots.
    max_slots: usize,
}

impl NotificationHub {
    /// Creates a new hub with the given capacity.
    pub fn new(max_slots: usize, ring_capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(max_slots),
            ring: NotificationRing::new(ring_capacity),
            next_id: 0,
            max_slots,
        }
    }

    /// Registers a new source/MV and returns its source_id.
    ///
    /// This is a Ring 2 operation (allocates, not hot path).
    pub fn register_source(&mut self) -> Option<u32> {
        if self.slots.len() >= self.max_slots {
            return None;
        }

        let id = self.next_id;
        self.next_id += 1;
        self.slots.push(NotificationSlot::new(id));
        Some(id)
    }

    /// Deactivates a source/MV.
    pub fn deactivate_source(&self, source_id: u32) {
        if let Some(slot) = self.slots.get(source_id as usize) {
            slot.deactivate();
        }
    }

    /// Notifies subscribers of a data change for a source.
    ///
    /// This is the Ring 0 hot path entry point.
    /// Increments the slot's sequence and pushes a NotificationRef
    /// onto the SPSC ring for Ring 1.
    ///
    /// # Performance
    ///
    /// ~20-50ns: atomic increment + SPSC push.
    #[inline(always)]
    pub fn notify_source(
        &self,
        source_id: u32,
        event_type: EventType,
        row_count: u32,
        timestamp: i64,
        batch_offset: u64,
    ) -> bool {
        let slot = match self.slots.get(source_id as usize) {
            Some(s) if s.is_active() => s,
            _ => return false,
        };

        let sequence = slot.notify();

        let notif = NotificationRef::new(
            sequence,
            source_id,
            event_type,
            row_count,
            timestamp,
            batch_offset,
        );

        self.ring.push(notif)
    }

    /// Drains all pending notifications (Ring 1 reader).
    #[inline]
    pub fn drain_notifications<F>(&self, f: F) -> usize
    where
        F: FnMut(NotificationRef),
    {
        self.ring.drain_into(f)
    }

    /// Returns the notification ring (for direct access by Ring 1).
    pub fn notification_ring(&self) -> &NotificationRing {
        &self.ring
    }

    /// Returns the number of registered sources.
    pub fn source_count(&self) -> usize {
        self.slots.len()
    }
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| DagExecutor | `laminar-core/src/dag/executor.rs` | Add NotificationHub, call `notify_source()` after operator output |
| TPC Reactor | `laminar-core/src/tpc/reactor.rs` | Per-core NotificationHub instance |
| Streaming Sink | `laminar-core/src/streaming/sink.rs` | Optional notification on write |

### New Files

- `crates/laminar-core/src/subscription/notification.rs` - NotificationSlot, NotificationRing, NotificationHub

## Test Plan

### Unit Tests

- [ ] `test_notification_slot_size` - Assert size_of == 64
- [ ] `test_notification_slot_notify` - Sequence increment
- [ ] `test_notification_slot_deactivate` - Active flag
- [ ] `test_notification_ring_push_pop` - Basic SPSC operation
- [ ] `test_notification_ring_full` - Backpressure when full
- [ ] `test_notification_ring_empty` - Pop from empty
- [ ] `test_notification_ring_wraparound` - Sequence wrapping
- [ ] `test_notification_ring_drain` - Drain callback
- [ ] `test_notification_hub_register` - Source registration
- [ ] `test_notification_hub_notify` - End-to-end notify + drain
- [ ] `test_notification_hub_deactivate` - Deactivated source skipped
- [ ] `test_notification_hub_max_slots` - Capacity limit
- [ ] `test_notification_ring_concurrent` - Multi-threaded push/pop

### Benchmarks

- [ ] `bench_notification_slot_notify` - Target: < 10ns
- [ ] `bench_notification_ring_push` - Target: < 20ns
- [ ] `bench_notification_ring_push_pop` - Target: < 40ns round-trip
- [ ] `bench_notification_hub_notify_source` - Target: < 50ns
- [ ] `bench_notification_hub_drain_100` - Target: < 500ns for 100 notifications

## Completion Checklist

- [ ] NotificationSlot implemented (64-byte aligned, atomic sequence)
- [ ] NotificationRing implemented (SPSC, pre-allocated, zero-alloc)
- [ ] NotificationHub implemented (slot registry + ring integration)
- [ ] Zero allocations verified on notify path
- [ ] Thread-per-core compatible (per-core instances)
- [ ] Unit tests passing (13+ tests)
- [ ] Benchmarks meeting targets
- [ ] `#[inline(always)]` on hot path methods
- [ ] Documentation with ring architecture diagram
- [ ] Code reviewed

## References

- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [F013: Thread-Per-Core](../../phase-2/F013-thread-per-core.md)
- [F014: SPSC Queues](../../phase-2/F014-spsc-queues.md)
- [iceoryx2 Zero-Copy Notifications](https://github.com/eclipse-iceoryx/iceoryx2)
