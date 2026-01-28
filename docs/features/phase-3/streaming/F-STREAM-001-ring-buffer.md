# F-STREAM-001: Ring Buffer

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-001 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F071 (Zero-Allocation) |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

Pre-allocated, lock-free ring buffer providing the foundational data structure for LaminarDB's streaming channels. The ring buffer eliminates allocation overhead on the hot path by pre-allocating all slots at construction time, using power-of-2 sizing for efficient modulo operations, and cache-line padding to prevent false sharing.

## Goals

- Zero allocation on push/poll operations
- Sub-100ns uncontended latency
- Cache-friendly memory layout with false-sharing prevention
- Support for RecordBatch payloads
- Power-of-2 sizing with bitwise modulo

## Non-Goals

- Multi-producer or multi-consumer support (handled by channel layer)
- Persistence or durability (handled by WAL/checkpoint)
- Dynamic resizing (capacity fixed at construction)
- Serialization (payloads stored as-is)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/ring_buffer.rs`

The ring buffer is the foundational primitive for all streaming channels. It provides:
- Pre-allocated slot array
- Separate read/write sequence counters
- Cache-line padding to prevent false sharing

```
┌─────────────────────────────────────────────────────────────────┐
│                      Ring Buffer Layout                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ CachePadded<WriteSeq>  │ 64 bytes (cache line aligned)  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ CachePadded<ReadSeq>   │ 64 bytes (cache line aligned)  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Slots[0] │ Slots[1] │ Slots[2] │ ... │ Slots[N-1]       │   │
│  │   64B    │   64B    │   64B    │     │    64B           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Power-of-2 capacity: index = sequence & mask                   │
│  mask = capacity - 1 (e.g., 65535 for 65536 slots)              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache-line padding to prevent false sharing.
/// Standard cache line size on modern CPUs is 64 bytes.
#[repr(align(64))]
pub struct CachePadded<T> {
    value: T,
    // Padding ensures this takes a full cache line
    _padding: [u8; 64 - std::mem::size_of::<T>()],
}

impl<T> CachePadded<T> {
    pub const fn new(value: T) -> Self {
        Self {
            value,
            _padding: [0u8; 64 - std::mem::size_of::<T>()],
        }
    }

    #[inline(always)]
    pub fn get(&self) -> &T {
        &self.value
    }

    #[inline(always)]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// Slot wrapper with cache-line alignment for individual elements.
/// Optional - only needed if T is small and false sharing on slots matters.
#[repr(align(64))]
pub struct Slot<T> {
    /// The actual value, uninitialized until written
    value: UnsafeCell<MaybeUninit<T>>,
    /// Sequence number for this slot (for multi-producer scenarios)
    sequence: AtomicU64,
}

/// Pre-allocated, lock-free ring buffer.
///
/// # Safety
///
/// This structure uses interior mutability and raw pointers for
/// zero-copy performance. It is safe when used with proper
/// producer/consumer synchronization (SPSC or MPSC wrapper).
#[repr(C)]
pub struct RingBuffer<T> {
    /// Pre-allocated slots (power-of-2 size)
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,

    /// Bitmask for fast modulo: index = seq & mask
    mask: u64,

    /// Write sequence counter (producer's position)
    /// Cache-line padded to prevent false sharing with read_seq
    write_seq: CachePadded<AtomicU64>,

    /// Read sequence counter (consumer's position)
    /// Cache-line padded to prevent false sharing with write_seq
    read_seq: CachePadded<AtomicU64>,

    /// Capacity (power of 2)
    capacity: usize,
}

// SAFETY: RingBuffer can be shared across threads when wrapped
// in proper synchronization (SPSC/MPSC channel).
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

/// Result of a try_push operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryPushError<T> {
    /// Buffer is full, value returned
    Full(T),
}

/// Result of a try_pop operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryPopError {
    /// Buffer is empty
    Empty,
}
```

### API/Interface

```rust
impl<T> RingBuffer<T> {
    /// Create a new ring buffer with the specified capacity.
    ///
    /// # Panics
    ///
    /// Panics if capacity is not a power of 2 or is zero.
    ///
    /// # Example
    ///
    /// ```rust
    /// let buffer: RingBuffer<u64> = RingBuffer::new(65536);
    /// assert!(buffer.is_empty());
    /// assert_eq!(buffer.capacity(), 65536);
    /// ```
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of 2");
        assert!(capacity > 0, "capacity must be non-zero");

        // Pre-allocate all slots
        let slots: Vec<_> = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();

        Self {
            slots: slots.into_boxed_slice(),
            mask: (capacity - 1) as u64,
            write_seq: CachePadded::new(AtomicU64::new(0)),
            read_seq: CachePadded::new(AtomicU64::new(0)),
            capacity,
        }
    }

    /// Returns the capacity of the ring buffer.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current number of items in the buffer.
    ///
    /// Note: This is an approximation in concurrent scenarios.
    #[inline]
    pub fn len(&self) -> usize {
        let write = self.write_seq.get().load(Ordering::Acquire);
        let read = self.read_seq.get().load(Ordering::Acquire);
        (write - read) as usize
    }

    /// Returns true if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns true if the buffer is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Returns the number of available slots for writing.
    #[inline]
    pub fn available(&self) -> usize {
        self.capacity - self.len()
    }

    // ========================================================================
    // Low-level access (used by channel implementations)
    // ========================================================================

    /// Get the current write sequence number.
    #[inline(always)]
    pub fn write_seq(&self) -> u64 {
        self.write_seq.get().load(Ordering::Acquire)
    }

    /// Get the current read sequence number.
    #[inline(always)]
    pub fn read_seq(&self) -> u64 {
        self.read_seq.get().load(Ordering::Acquire)
    }

    /// Get a pointer to the slot at the given sequence number.
    ///
    /// # Safety
    ///
    /// Caller must ensure proper synchronization and that the
    /// sequence number is valid (between read_seq and write_seq).
    #[inline(always)]
    pub unsafe fn slot_ptr(&self, seq: u64) -> *mut T {
        let index = (seq & self.mask) as usize;
        (*self.slots[index].get()).as_mut_ptr()
    }

    /// Write a value to the slot at the given sequence number.
    ///
    /// # Safety
    ///
    /// - Caller must have exclusive write access to this slot
    /// - Sequence must be valid (not overwriting unread data)
    #[inline(always)]
    pub unsafe fn write_slot(&self, seq: u64, value: T) {
        let index = (seq & self.mask) as usize;
        (*self.slots[index].get()).write(value);
    }

    /// Read and take a value from the slot at the given sequence number.
    ///
    /// # Safety
    ///
    /// - Caller must have exclusive read access to this slot
    /// - Slot must have been previously written
    #[inline(always)]
    pub unsafe fn read_slot(&self, seq: u64) -> T {
        let index = (seq & self.mask) as usize;
        (*self.slots[index].get()).assume_init_read()
    }

    /// Advance the write sequence counter.
    ///
    /// # Safety
    ///
    /// Caller must have written to all slots up to this sequence.
    #[inline(always)]
    pub unsafe fn advance_write_seq(&self, new_seq: u64) {
        self.write_seq.get().store(new_seq, Ordering::Release);
    }

    /// Advance the read sequence counter.
    ///
    /// # Safety
    ///
    /// Caller must have read all slots up to this sequence.
    #[inline(always)]
    pub unsafe fn advance_read_seq(&self, new_seq: u64) {
        self.read_seq.get().store(new_seq, Ordering::Release);
    }

    /// Compare-and-swap on write sequence (for MPSC).
    #[inline(always)]
    pub fn cas_write_seq(&self, expected: u64, new: u64) -> Result<u64, u64> {
        self.write_seq.get().compare_exchange(
            expected,
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Drop any remaining items in the buffer
        let write = self.write_seq.get().load(Ordering::Acquire);
        let mut read = self.read_seq.get().load(Ordering::Acquire);

        while read < write {
            unsafe {
                let index = (read & self.mask) as usize;
                std::ptr::drop_in_place((*self.slots[index].get()).as_mut_ptr());
            }
            read += 1;
        }
    }
}
```

### Memory Layout

```
Memory Layout (64-byte aligned):

Offset 0:    write_seq (CachePadded<AtomicU64>) - 64 bytes
             ┌────────────────────────────────────────────────────────────┐
             │ AtomicU64 │ padding[56]                                    │
             └────────────────────────────────────────────────────────────┘

Offset 64:   read_seq (CachePadded<AtomicU64>) - 64 bytes
             ┌────────────────────────────────────────────────────────────┐
             │ AtomicU64 │ padding[56]                                    │
             └────────────────────────────────────────────────────────────┘

Offset 128:  mask (u64) - 8 bytes
Offset 136:  capacity (usize) - 8 bytes
Offset 144:  slots (Box<[UnsafeCell<MaybeUninit<T>>]>) - heap allocated

Heap (slots array):
             ┌─────────┬─────────┬─────────┬─────────┬─────────┐
             │ Slot[0] │ Slot[1] │ Slot[2] │  ...    │Slot[N-1]│
             └─────────┴─────────┴─────────┴─────────┴─────────┘

Power-of-2 indexing:
  capacity = 65536 (2^16)
  mask = 65535 (0xFFFF)
  index = sequence & mask  // Fast bitwise AND, no division
```

### Algorithm/Flow

#### Push Operation (SPSC Context)

```
1. Load write_seq (Acquire)
2. Load read_seq (Acquire)
3. Check: write_seq - read_seq < capacity?
   - No: Buffer full, return error or block
   - Yes: Continue
4. Write value to slots[write_seq & mask]
5. Store write_seq + 1 (Release)
```

#### Pop Operation (SPSC Context)

```
1. Load read_seq (Acquire)
2. Load write_seq (Acquire)
3. Check: read_seq < write_seq?
   - No: Buffer empty, return None or block
   - Yes: Continue
4. Read value from slots[read_seq & mask]
5. Store read_seq + 1 (Release)
```

### Integration with Three-Ring Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RING 0: HOT PATH                          │
│                                                                  │
│  ┌──────────────┐                          ┌──────────────┐     │
│  │   Producer   │                          │   Consumer   │     │
│  │   (Source)   │                          │   (Sink)     │     │
│  └──────┬───────┘                          └───────▲──────┘     │
│         │                                          │            │
│         │  write_seq         read_seq              │            │
│         ▼                    ▲                     │            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                     RingBuffer<T>                        │   │
│  │  ┌───────┬───────┬───────┬───────┬───────┬───────┐      │   │
│  │  │ Slot0 │ Slot1 │ Slot2 │ Slot3 │  ...  │SlotN-1│      │   │
│  │  └───────┴───────┴───────┴───────┴───────┴───────┘      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Zero allocation: slots pre-allocated, no runtime malloc        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `TryPushError::Full` | Buffer at capacity | Caller can block, drop, or reject |
| `TryPopError::Empty` | No data available | Caller can block or return None |
| Panic on construction | Non-power-of-2 capacity | Fix at construction time |

## Test Plan

### Unit Tests

- [ ] `test_new_creates_empty_buffer`
- [ ] `test_new_panics_on_non_power_of_two`
- [ ] `test_new_panics_on_zero_capacity`
- [ ] `test_capacity_returns_correct_value`
- [ ] `test_len_empty_buffer_returns_zero`
- [ ] `test_is_empty_true_on_new_buffer`
- [ ] `test_is_full_false_on_new_buffer`
- [ ] `test_single_write_read_roundtrip`
- [ ] `test_fill_to_capacity`
- [ ] `test_wrap_around_sequence_numbers`
- [ ] `test_drop_cleans_up_remaining_items`
- [ ] `test_bitwise_modulo_matches_regular_modulo`

### Integration Tests

- [ ] SPSC channel using RingBuffer
- [ ] MPSC channel using RingBuffer with CAS
- [ ] Multi-threaded stress test

### Property Tests

- [ ] `prop_len_never_exceeds_capacity`
- [ ] `prop_write_seq_always_gte_read_seq`
- [ ] `prop_items_read_in_fifo_order`
- [ ] `prop_no_data_loss_under_concurrent_access`

### Benchmarks

- [ ] `bench_push_pop_uncontended` - Target: < 50ns
- [ ] `bench_push_pop_contended` - Target: < 100ns
- [ ] `bench_batch_push` - Target: < 10ns/item
- [ ] `bench_batch_pop` - Target: < 10ns/item
- [ ] `bench_cache_line_isolation` - Verify no false sharing

## Rollout Plan

1. **Phase 1**: Core RingBuffer implementation + unit tests
2. **Phase 2**: Integration with SPSC channel (F-STREAM-002)
3. **Phase 3**: Benchmarks + optimization
4. **Phase 4**: Documentation
5. **Phase 5**: Code review + merge

## Open Questions

- [x] **Slot alignment**: Should individual slots be cache-line aligned?
  - Decision: Only if T is small. For RecordBatch, slot size > cache line anyway.
- [x] **Huge pages**: Use huge pages for large buffers?
  - Decision: Defer to Phase 4. Add madvise hints in future.
- [ ] **Slot sequence counters**: Add per-slot sequence for MPSC?
  - Leaning: Keep separate MPSC implementation for clarity.

## Completion Checklist

- [ ] Code implemented in `laminar-core/src/streaming/ring_buffer.rs`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets (< 100ns uncontended)
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

**LMAX Disruptor Influence**: This design is heavily influenced by the LMAX Disruptor pattern:
- Pre-allocated ring eliminates allocation overhead
- Sequence counters for lock-free coordination
- Cache-line padding prevents false sharing
- Power-of-2 sizing enables bitwise modulo

**Memory Ordering Choices**:
- `Acquire` on reads: Ensures we see writes before the sequence update
- `Release` on writes: Ensures our data is visible before sequence update
- `AcqRel` on CAS: For MPSC producer coordination

## References

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
- [F071: Zero-Allocation Enforcement](../../phase-2/F071-zero-allocation-enforcement.md)
- [F014: SPSC Queue Communication](../../phase-2/F014-spsc-queues.md)
