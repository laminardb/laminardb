//! Lock-free ring buffer for streaming channels.
//!
//! This module provides a heap-allocated ring buffer optimized for
//! single-producer single-consumer (SPSC) scenarios with support for
//! MPSC mode via atomic slot claiming.
//!
//! ## Design
//!
//! - Heap-allocated with runtime capacity (unlike const-generic `alloc::RingBuffer`)
//! - Power-of-2 capacity with bitmask indexing for fast modulo
//! - Cache-padded head/tail indices prevent false sharing
//! - Acquire/Release memory ordering for lock-free operation
//! - Separate `claim_counter` for MPSC slot claiming
//!
//! ## Performance
//!
//! Target: < 20ns per push/pop operation in SPSC mode.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::tpc::CachePadded;

use super::config::{MAX_BUFFER_SIZE, MIN_BUFFER_SIZE};

/// A lock-free ring buffer with runtime-specified capacity.
///
/// This buffer supports both SPSC and MPSC modes:
/// - SPSC: Single producer uses `push`, single consumer uses `pop`
/// - MPSC: Multiple producers use `claim_and_write`, single consumer uses `pop`
///
/// # Safety
///
/// The buffer is safe to use from multiple threads as long as:
/// - In SPSC mode: Exactly one producer and one consumer thread
/// - In MPSC mode: Any number of producers, exactly one consumer thread
pub struct RingBuffer<T> {
    /// Ring buffer storage.
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,

    /// Head index (consumer reads from here).
    /// Cache-padded to prevent false sharing with tail.
    head: CachePadded<AtomicUsize>,

    /// Tail index (producer writes here in SPSC mode).
    /// Cache-padded to prevent false sharing with head.
    tail: CachePadded<AtomicUsize>,

    /// Claim counter for MPSC mode.
    /// Producers atomically increment this to claim slots.
    claim_counter: CachePadded<AtomicU64>,

    /// Capacity mask for fast modulo (capacity - 1).
    capacity_mask: usize,
}

// SAFETY: RingBuffer can be sent between threads as long as T is Send
unsafe impl<T: Send> Send for RingBuffer<T> {}

// SAFETY: RingBuffer can be shared between threads (with appropriate
// producer/consumer constraints) as long as T is Send
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    /// Creates a new ring buffer with the given capacity.
    ///
    /// The capacity will be clamped to `[MIN_BUFFER_SIZE, MAX_BUFFER_SIZE]`
    /// and rounded up to the next power of 2 for efficiency.
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        // Clamp and round up to next power of 2
        let capacity = capacity
            .clamp(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE)
            .next_power_of_two();

        // Allocate the buffer
        let buffer: Vec<UnsafeCell<MaybeUninit<T>>> = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();

        Self {
            buffer: buffer.into_boxed_slice(),
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            claim_counter: CachePadded::new(AtomicU64::new(0)),
            capacity_mask: capacity - 1,
        }
    }

    /// Returns the capacity of the buffer.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity_mask + 1
    }

    /// Returns true if the buffer is empty.
    ///
    /// Note: This is a snapshot and may change immediately after returning.
    /// Uses Relaxed ordering since this is an approximate query.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        head == tail
    }

    /// Returns true if the buffer is full.
    ///
    /// Note: This is a snapshot and may change immediately after returning.
    /// Uses Relaxed ordering since this is an approximate query.
    #[inline]
    #[must_use]
    pub fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        self.next_index(tail) == head
    }

    /// Returns the current number of items in the buffer.
    ///
    /// Note: This is a snapshot and may change immediately after returning.
    /// Uses Relaxed ordering since this is an approximate query.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        tail.wrapping_sub(head) & self.capacity_mask
    }

    /// Returns the number of free slots in the buffer.
    ///
    /// Note: This is a snapshot and may change immediately after returning.
    #[inline]
    #[must_use]
    pub fn free_slots(&self) -> usize {
        // One slot is always reserved to distinguish full from empty
        self.capacity() - 1 - self.len()
    }

    /// Pushes an item to the buffer (SPSC mode).
    ///
    /// Returns `Ok(())` if successful, or `Err(item)` if the buffer is full.
    ///
    /// # Errors
    ///
    /// Returns the item back if the buffer is full.
    ///
    /// # Safety
    ///
    /// In SPSC mode, this must only be called by the single producer thread.
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let next_tail = self.next_index(tail);

        // Check if buffer is full
        if next_tail == self.head.load(Ordering::Acquire) {
            return Err(item);
        }

        // SAFETY: We have exclusive write access to this slot because:
        // 1. We are the only producer (SPSC constraint)
        // 2. The consumer only reads slots where head < tail
        // 3. We haven't published this slot yet (tail not updated)
        unsafe {
            (*self.buffer[tail].get()).write(item);
        }

        // Publish the item by updating tail
        self.tail.store(next_tail, Ordering::Release);

        Ok(())
    }

    /// Pops an item from the buffer.
    ///
    /// Returns `Some(item)` if successful, or `None` if the buffer is empty.
    ///
    /// # Safety
    ///
    /// This must only be called by the single consumer thread.
    #[inline]
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);

        // Check if buffer is empty
        if head == self.tail.load(Ordering::Acquire) {
            return None;
        }

        // SAFETY: We have exclusive read access to this slot because:
        // 1. We are the only consumer
        // 2. The producer only writes to slots where tail > head
        // 3. This slot has been published (we checked tail > head)
        let item = unsafe { (*self.buffer[head].get()).assume_init_read() };

        // Consume the item by updating head
        self.head.store(self.next_index(head), Ordering::Release);

        Some(item)
    }

    /// Peeks at the next item without removing it.
    ///
    /// Returns `None` if the buffer is empty.
    ///
    /// # Safety
    ///
    /// This must only be called by the single consumer thread.
    #[inline]
    #[must_use]
    pub fn peek(&self) -> Option<&T> {
        let head = self.head.load(Ordering::Relaxed);

        if head == self.tail.load(Ordering::Acquire) {
            return None;
        }

        // SAFETY: Same reasoning as pop()
        unsafe { Some((*self.buffer[head].get()).assume_init_ref()) }
    }

    /// Claims a slot for writing (MPSC mode).
    ///
    /// Returns the slot index if successful, or `None` if the buffer is full.
    /// The caller must then write to the slot and call `publish_slot`.
    ///
    /// This uses atomic increment on `claim_counter` to ensure each producer
    /// gets a unique slot.
    #[inline]
    pub fn claim_slot(&self) -> Option<usize> {
        // Atomic claim - each caller gets a unique slot
        let claim = self.claim_counter.fetch_add(1, Ordering::AcqRel);

        // Convert claim to slot index
        // We use u64 for claim_counter to avoid wrap-around issues
        #[allow(clippy::cast_possible_truncation)]
        let slot = usize::try_from(claim).unwrap_or(usize::MAX) & self.capacity_mask;

        // Calculate how many slots are claimed but not yet published
        let tail = self.tail.load(Ordering::Acquire);
        let pending = claim.saturating_sub(tail as u64);

        // If we've claimed more than capacity - 1 slots ahead, we're full
        if pending >= (self.capacity() - 1) as u64 {
            // Undo the claim (best effort - another producer may have claimed)
            // This is safe because we haven't written anything yet
            self.claim_counter.fetch_sub(1, Ordering::AcqRel);
            return None;
        }

        Some(slot)
    }

    /// Writes to a claimed slot (MPSC mode).
    ///
    /// # Safety
    ///
    /// The slot must have been obtained from `claim_slot` and not yet written to.
    /// After calling this, you must call `try_publish` to make the item visible.
    #[inline]
    pub unsafe fn write_slot(&self, slot: usize, item: T) {
        debug_assert!(slot < self.capacity());
        (*self.buffer[slot].get()).write(item);
    }

    /// Attempts to publish written slots up to the given claim (MPSC mode).
    ///
    /// This advances the tail if all slots up to `claim` have been written.
    /// Returns true if the tail was advanced.
    ///
    /// In MPSC mode, producers write out-of-order but the consumer sees
    /// items in order. This function is typically called by each producer
    /// after writing, but only succeeds when all prior slots are also written.
    ///
    /// For simplicity, this implementation uses a "publish barrier" approach:
    /// producers write to their slots, then the tail is advanced by whichever
    /// producer happens to have the lowest claimed slot.
    #[inline]
    pub fn try_advance_tail(&self, target_tail: usize) -> bool {
        let current_tail = self.tail.load(Ordering::Acquire);

        // Only advance if target is the next slot after current tail
        if target_tail == self.next_index(current_tail) {
            // Try to advance tail
            self.tail
                .compare_exchange(
                    current_tail,
                    target_tail,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
        } else {
            false
        }
    }

    /// Pushes multiple items to the buffer.
    ///
    /// Returns the number of items successfully pushed.
    ///
    /// # Safety
    ///
    /// In SPSC mode, this must only be called by the single producer thread.
    #[inline]
    pub fn push_batch(&self, items: impl IntoIterator<Item = T>) -> usize {
        let mut count = 0;
        for item in items {
            if self.push(item).is_err() {
                break;
            }
            count += 1;
        }
        count
    }

    /// Pops multiple items from the buffer.
    ///
    /// Returns a vector of up to `max_count` items.
    ///
    /// # Safety
    ///
    /// This must only be called by the single consumer thread.
    ///
    /// # Performance Warning
    ///
    /// **This method allocates a `Vec` on every call.** Do not use on hot paths
    /// where allocation overhead matters. For zero-allocation consumption, use
    /// [`pop_each`](Self::pop_each) or [`pop_batch_into`](Self::pop_batch_into).
    #[cold]
    #[must_use]
    pub fn pop_batch(&self, max_count: usize) -> Vec<T> {
        let mut items = Vec::with_capacity(max_count.min(self.len()));
        for _ in 0..max_count {
            if let Some(item) = self.pop() {
                items.push(item);
            } else {
                break;
            }
        }
        items
    }

    /// Pops items and calls a callback for each (zero-allocation).
    ///
    /// Processing stops when:
    /// - `max_count` items have been processed
    /// - The buffer becomes empty
    /// - The callback returns `false`
    ///
    /// Returns the number of items processed.
    ///
    /// # Safety
    ///
    /// This must only be called by the single consumer thread.
    #[inline]
    pub fn pop_each<F>(&self, max_count: usize, mut f: F) -> usize
    where
        F: FnMut(T) -> bool,
    {
        if max_count == 0 {
            return 0;
        }

        let mut current_head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // Calculate available items
        let available = if tail >= current_head {
            tail - current_head
        } else {
            (self.capacity_mask + 1) - current_head + tail
        };

        let to_pop = available.min(max_count);

        if to_pop == 0 {
            return 0;
        }

        let mut popped = 0;
        for _ in 0..to_pop {
            // SAFETY: We have exclusive read access
            let item = unsafe { (*self.buffer[current_head].get()).assume_init_read() };

            popped += 1;
            current_head = self.next_index(current_head);

            if !f(item) {
                break;
            }
        }

        if popped > 0 {
            self.head.store(current_head, Ordering::Release);
        }

        popped
    }

    /// Pops items into a caller-provided buffer (zero-allocation).
    ///
    /// Returns the number of items popped.
    ///
    /// # Safety
    ///
    /// This must only be called by the single consumer thread.
    /// After this method returns `n`, the first `n` elements of `buffer`
    /// are initialized.
    #[inline]
    pub fn pop_batch_into(&self, buffer: &mut [MaybeUninit<T>]) -> usize {
        if buffer.is_empty() {
            return 0;
        }

        let mut current_head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        let available = if tail >= current_head {
            tail - current_head
        } else {
            (self.capacity_mask + 1) - current_head + tail
        };

        let count = available.min(buffer.len());

        if count == 0 {
            return 0;
        }

        for slot in buffer.iter_mut().take(count) {
            // SAFETY: We have exclusive read access
            unsafe {
                let src = (*self.buffer[current_head].get()).assume_init_read();
                slot.write(src);
            }
            current_head = self.next_index(current_head);
        }

        self.head.store(current_head, Ordering::Release);
        count
    }

    /// Calculate the next index with wrap-around.
    #[inline]
    const fn next_index(&self, index: usize) -> usize {
        (index + 1) & self.capacity_mask
    }

    /// Resets the buffer to empty state.
    ///
    /// # Safety
    ///
    /// This must only be called when no other threads are accessing the buffer.
    pub fn reset(&mut self) {
        // Drop any remaining items
        while self.pop().is_some() {}

        // Reset indices
        self.head.store(0, Ordering::Release);
        self.tail.store(0, Ordering::Release);
        self.claim_counter.store(0, Ordering::Release);
    }
}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Drop any remaining items
        while self.pop().is_some() {}
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for RingBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RingBuffer")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .field("is_empty", &self.is_empty())
            .field("is_full", &self.is_full())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_new_buffer() {
        let buffer: RingBuffer<i32> = RingBuffer::new(100);
        // Should round up to 128
        assert_eq!(buffer.capacity(), 128);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_push_pop() {
        let buffer: RingBuffer<i32> = RingBuffer::new(4);

        assert!(buffer.push(1).is_ok());
        assert!(buffer.push(2).is_ok());
        assert!(buffer.push(3).is_ok());
        // Capacity 4 means 3 usable slots
        assert!(buffer.is_full());
        assert!(buffer.push(4).is_err());

        assert_eq!(buffer.pop(), Some(1));
        assert_eq!(buffer.pop(), Some(2));
        assert_eq!(buffer.pop(), Some(3));
        assert_eq!(buffer.pop(), None);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_fifo_order() {
        let buffer: RingBuffer<i32> = RingBuffer::new(16);

        for i in 0..10 {
            assert!(buffer.push(i).is_ok());
        }

        for i in 0..10 {
            assert_eq!(buffer.pop(), Some(i));
        }
    }

    #[test]
    fn test_wrap_around() {
        let buffer: RingBuffer<i32> = RingBuffer::new(4);

        // Fill and empty multiple times to test wrap-around
        for iteration in 0..5 {
            for i in 0..3 {
                assert!(buffer.push(iteration * 10 + i).is_ok());
            }
            for i in 0..3 {
                assert_eq!(buffer.pop(), Some(iteration * 10 + i));
            }
        }
    }

    #[test]
    fn test_peek() {
        let buffer: RingBuffer<i32> = RingBuffer::new(4);

        assert!(buffer.peek().is_none());

        buffer.push(42).unwrap();
        assert_eq!(buffer.peek(), Some(&42));
        assert_eq!(buffer.peek(), Some(&42)); // Still there

        assert_eq!(buffer.pop(), Some(42));
        assert!(buffer.peek().is_none());
    }

    #[test]
    fn test_push_batch() {
        let buffer: RingBuffer<i32> = RingBuffer::new(8);

        let pushed = buffer.push_batch(vec![1, 2, 3, 4, 5]);
        assert_eq!(pushed, 5);
        assert_eq!(buffer.len(), 5);

        // Try to push more than capacity
        let pushed = buffer.push_batch(vec![6, 7, 8, 9, 10]);
        assert_eq!(pushed, 2); // Only 2 more fit
    }

    #[test]
    fn test_pop_batch() {
        let buffer: RingBuffer<i32> = RingBuffer::new(8);

        buffer.push_batch(vec![1, 2, 3, 4, 5]);

        let items = buffer.pop_batch(3);
        assert_eq!(items, vec![1, 2, 3]);
        assert_eq!(buffer.len(), 2);

        let items = buffer.pop_batch(10);
        assert_eq!(items, vec![4, 5]);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_pop_each() {
        let buffer: RingBuffer<i32> = RingBuffer::new(16);

        buffer.push_batch(vec![1, 2, 3, 4, 5]);

        let mut sum = 0;
        let count = buffer.pop_each(10, |item| {
            sum += item;
            true
        });

        assert_eq!(count, 5);
        assert_eq!(sum, 15);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_pop_each_early_stop() {
        let buffer: RingBuffer<i32> = RingBuffer::new(16);

        buffer.push_batch(vec![1, 2, 3, 4, 5]);

        let mut items = Vec::new();
        let count = buffer.pop_each(10, |item| {
            items.push(item);
            item < 3
        });

        assert_eq!(count, 3);
        assert_eq!(items, vec![1, 2, 3]);
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn test_pop_batch_into() {
        let buffer: RingBuffer<i32> = RingBuffer::new(16);

        buffer.push_batch(vec![1, 2, 3]);

        let mut dest: [MaybeUninit<i32>; 8] = [MaybeUninit::uninit(); 8];
        let count = buffer.pop_batch_into(&mut dest);

        assert_eq!(count, 3);
        unsafe {
            assert_eq!(dest[0].assume_init(), 1);
            assert_eq!(dest[1].assume_init(), 2);
            assert_eq!(dest[2].assume_init(), 3);
        }
    }

    #[test]
    fn test_free_slots() {
        let buffer: RingBuffer<i32> = RingBuffer::new(8);

        assert_eq!(buffer.free_slots(), 7); // capacity - 1

        buffer.push(1).unwrap();
        buffer.push(2).unwrap();
        assert_eq!(buffer.free_slots(), 5);

        let _ = buffer.pop();
        assert_eq!(buffer.free_slots(), 6);
    }

    #[test]
    fn test_concurrent_spsc() {
        const ITEMS: i32 = 10_000;
        let buffer = Arc::new(RingBuffer::<i32>::new(1024));
        let producer_buffer = Arc::clone(&buffer);
        let consumer_buffer = Arc::clone(&buffer);

        let producer = thread::spawn(move || {
            for i in 0..ITEMS {
                while producer_buffer.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(ITEMS as usize);
            while received.len() < ITEMS as usize {
                if let Some(item) = consumer_buffer.pop() {
                    received.push(item);
                } else {
                    thread::yield_now();
                }
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        assert_eq!(received.len(), ITEMS as usize);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i32::try_from(i).unwrap());
        }
    }

    #[test]
    fn test_drop_items() {
        use std::sync::atomic::AtomicUsize;

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct DropCounter;
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let buffer: RingBuffer<DropCounter> = RingBuffer::new(8);
            for _ in 0..5 {
                buffer.push(DropCounter).unwrap();
            }
            let _ = buffer.pop();
            let _ = buffer.pop();
            // 3 remaining, 2 dropped via pop
        }

        // All 5 should be dropped (2 via pop, 3 via buffer drop)
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn test_reset() {
        let mut buffer: RingBuffer<i32> = RingBuffer::new(8);

        buffer.push_batch(vec![1, 2, 3, 4, 5]);
        assert_eq!(buffer.len(), 5);

        buffer.reset();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_debug() {
        let buffer: RingBuffer<i32> = RingBuffer::new(8);
        buffer.push(1).unwrap();
        buffer.push(2).unwrap();

        let debug_str = format!("{buffer:?}");
        assert!(debug_str.contains("RingBuffer"));
        assert!(debug_str.contains("capacity"));
        assert!(debug_str.contains("len"));
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn test_zero_capacity_panics() {
        let _: RingBuffer<i32> = RingBuffer::new(0);
    }

    #[test]
    fn test_capacity_clamping() {
        // Very small
        let buffer: RingBuffer<i32> = RingBuffer::new(1);
        assert!(buffer.capacity() >= MIN_BUFFER_SIZE);

        // Very large
        let buffer: RingBuffer<i32> = RingBuffer::new(usize::MAX / 2);
        assert!(buffer.capacity() <= MAX_BUFFER_SIZE.next_power_of_two());
    }
}
