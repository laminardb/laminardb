//! Zero-copy multicast buffer for shared intermediate stages.
//!
//! [`MulticastBuffer<T>`] implements a pre-allocated ring buffer with per-slot
//! reference counting for single-producer, multiple-consumer (SPMC) multicast.
//! Designed for Ring 0 hot path: zero allocations after construction.
//!
//! # Design
//!
//! - Pre-allocated slots with power-of-2 capacity and bitmask indexing
//! - Single writer via [`publish()`](MulticastBuffer::publish), multiple
//!   readers via [`consume()`](MulticastBuffer::consume)
//! - Per-slot atomic refcount tracks outstanding consumers
//! - Backpressure: `publish()` fails when slowest consumer hasn't freed a slot
//!
//! # Safety
//!
//! The single-writer invariant is upheld by the DAG executor (F-DAG-003),
//! which ensures exactly one thread calls `publish()` on any given buffer.
//! Multiple threads may call `consume()` with distinct `consumer_idx` values.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use super::error::DagError;

/// Pre-allocated SPMC multicast buffer with reference-counted slots.
///
/// Provides zero-allocation publish/consume on the hot path. The buffer
/// is constructed in Ring 2 and used in Ring 0.
///
/// # Type Parameters
///
/// * `T` - The event type. Must be `Clone` for consumers (typically
///   `Arc<RecordBatch>` where clone is an O(1) atomic increment).
///
/// # Performance Targets
///
/// | Operation | Target |
/// |-----------|--------|
/// | `publish()` | < 100ns |
/// | `consume()` | < 50ns |
pub struct MulticastBuffer<T> {
    /// Pre-allocated ring buffer slots.
    slots: Box<[UnsafeCell<Option<T>>]>,
    /// Per-slot reference counts. 0 = free, N = N consumers still need to read.
    refcounts: Box<[AtomicU32]>,
    /// Monotonically increasing write position (single writer).
    write_pos: AtomicU64,
    /// Per-consumer read positions.
    read_positions: Box<[AtomicU64]>,
    /// Buffer capacity (power of 2).
    capacity: usize,
    /// Bitmask for modular indexing (`capacity - 1`).
    mask: usize,
    /// Number of consumers.
    consumer_count: u32,
}

// SAFETY: MulticastBuffer is designed for SPMC (single-producer, multi-consumer):
// - Single writer thread calls publish() (enforced by DAG executor)
// - Multiple consumer threads call consume() with distinct indices
// - All shared state uses atomic operations with appropriate memory ordering
// - UnsafeCell access is guarded by refcount/write_pos synchronization
unsafe impl<T: Send> Send for MulticastBuffer<T> {}
// SAFETY: See above. Consumers access distinct read_positions entries.
// Slot reads are protected by the write_pos/refcount protocol.
unsafe impl<T: Send> Sync for MulticastBuffer<T> {}

impl<T> MulticastBuffer<T> {
    /// Creates a new multicast buffer.
    ///
    /// Allocates all slots up front (Ring 2). The hot path
    /// (`publish`/`consume`) is allocation-free.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of slots (must be a power of 2, > 0)
    /// * `consumer_count` - Number of downstream consumers
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is not a power of 2 or is 0.
    #[must_use]
    pub fn new(capacity: usize, consumer_count: usize) -> Self {
        assert!(
            capacity > 0 && capacity.is_power_of_two(),
            "capacity must be a non-zero power of 2, got {capacity}"
        );

        let slots: Vec<UnsafeCell<Option<T>>> =
            (0..capacity).map(|_| UnsafeCell::new(None)).collect();
        let refcounts: Vec<AtomicU32> = (0..capacity).map(|_| AtomicU32::new(0)).collect();
        let read_positions: Vec<AtomicU64> =
            (0..consumer_count).map(|_| AtomicU64::new(0)).collect();

        Self {
            slots: slots.into_boxed_slice(),
            refcounts: refcounts.into_boxed_slice(),
            write_pos: AtomicU64::new(0),
            read_positions: read_positions.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            #[allow(clippy::cast_possible_truncation)] // Consumer count bounded by MAX_FAN_OUT (8)
            consumer_count: consumer_count as u32,
        }
    }

    /// Publishes a value to all consumers.
    ///
    /// Writes the value into the next available slot and sets the refcount
    /// to `consumer_count`. All consumers will be able to read this value
    /// via [`consume()`](Self::consume).
    ///
    /// # Errors
    ///
    /// Returns [`DagError::BackpressureFull`] if the target slot is still
    /// in use by a slow consumer (backpressure).
    ///
    /// # Safety Contract
    ///
    /// Must be called from a single writer thread only. The DAG executor
    /// enforces this by assigning exactly one producer per shared stage.
    pub fn publish(&self, value: T) -> Result<(), DagError> {
        let pos = self.write_pos.load(Ordering::Relaxed);
        // Bitmask truncates to capacity range, so u64->usize narrowing is safe.
        #[allow(clippy::cast_possible_truncation)]
        let slot_idx = (pos as usize) & self.mask;

        // Check if slot is free (all consumers have finished reading).
        if self.refcounts[slot_idx].load(Ordering::Acquire) != 0 {
            return Err(DagError::BackpressureFull);
        }

        // SAFETY: Single writer guarantees exclusive write access to this slot.
        // The Acquire load of refcount above ensures all consumers have completed
        // their reads (refcount == 0 means no outstanding readers).
        unsafe { *self.slots[slot_idx].get() = Some(value) };

        // Set refcount before advancing write_pos. Release ordering ensures
        // the slot value is visible before consumers can observe the new write_pos.
        self.refcounts[slot_idx].store(self.consumer_count, Ordering::Release);

        // Advance write position (Release makes new data visible to consumers).
        self.write_pos.store(pos + 1, Ordering::Release);

        Ok(())
    }

    /// Consumes the next value for a given consumer.
    ///
    /// Returns `None` if no new data is available. Each consumer maintains
    /// its own read position and will receive every published value in order.
    ///
    /// # Arguments
    ///
    /// * `consumer_idx` - The consumer's index (0-based, must be < `consumer_count`)
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `consumer_idx >= consumer_count`.
    pub fn consume(&self, consumer_idx: usize) -> Option<T>
    where
        T: Clone,
    {
        debug_assert!(
            consumer_idx < self.consumer_count as usize,
            "consumer_idx {consumer_idx} >= consumer_count {}",
            self.consumer_count
        );

        let read_pos = self.read_positions[consumer_idx].load(Ordering::Relaxed);
        let write_pos = self.write_pos.load(Ordering::Acquire);

        if read_pos >= write_pos {
            return None; // No data available
        }

        // Bitmask truncates to capacity range, so u64->usize narrowing is safe.
        #[allow(clippy::cast_possible_truncation)]
        let slot_idx = (read_pos as usize) & self.mask;

        // SAFETY: write_pos > read_pos guarantees this slot contains valid data.
        // The Acquire load of write_pos above synchronizes-with the Release store
        // in publish(), ensuring the slot value is visible.
        let value = unsafe { (*self.slots[slot_idx].get()).as_ref().unwrap().clone() };

        // Advance read position (prevents re-reading this slot).
        self.read_positions[consumer_idx].store(read_pos + 1, Ordering::Release);

        // Decrement refcount. AcqRel: the release side ensures our slot read
        // happens-before the publisher can reuse the slot after seeing refcount == 0.
        self.refcounts[slot_idx].fetch_sub(1, Ordering::AcqRel);

        Some(value)
    }

    /// Returns the buffer capacity (number of slots).
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of consumers.
    #[inline]
    #[must_use]
    pub fn consumer_count(&self) -> u32 {
        self.consumer_count
    }

    /// Returns the current write position (total number of publishes).
    #[inline]
    #[must_use]
    pub fn write_position(&self) -> u64 {
        self.write_pos.load(Ordering::Relaxed)
    }

    /// Returns the current read position for a consumer.
    ///
    /// # Panics
    ///
    /// Panics if `consumer_idx` is out of bounds.
    #[inline]
    #[must_use]
    pub fn read_position(&self, consumer_idx: usize) -> u64 {
        self.read_positions[consumer_idx].load(Ordering::Relaxed)
    }
}

impl<T> std::fmt::Debug for MulticastBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MulticastBuffer")
            .field("capacity", &self.capacity)
            .field("consumer_count", &self.consumer_count)
            .field("write_pos", &self.write_pos.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
