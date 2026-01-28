//! # SPSC Queue
//!
//! Lock-free single-producer single-consumer bounded queue optimized for
//! inter-core communication in thread-per-core architectures.
//!
//! ## Design
//!
//! - Cache-line padded head/tail indices prevent false sharing
//! - Power-of-2 capacity for fast modulo via bitmask
//! - Acquire/Release memory ordering for lock-free operation
//! - Batch operations for throughput optimization
//!
//! ## Performance
//!
//! Target: < 50ns per push/pop operation.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A wrapper that pads a value to a cache line boundary to prevent false sharing.
///
/// False sharing occurs when two threads access different data that happens to
/// reside on the same cache line, causing unnecessary cache invalidations.
///
/// # Example
///
/// ```rust
/// use laminar_core::tpc::CachePadded;
/// use std::sync::atomic::AtomicUsize;
///
/// // Each counter gets its own cache line
/// let counter1 = CachePadded::new(AtomicUsize::new(0));
/// let counter2 = CachePadded::new(AtomicUsize::new(0));
///
/// // Access the inner value
/// assert_eq!(counter1.load(std::sync::atomic::Ordering::Relaxed), 0);
/// ```
#[repr(C, align(64))]
pub struct CachePadded<T> {
    value: T,
}

// SAFETY: CachePadded is Send if T is Send
#[allow(unsafe_code)]
unsafe impl<T: Send> Send for CachePadded<T> {}

// SAFETY: CachePadded is Sync if T is Sync
#[allow(unsafe_code)]
unsafe impl<T: Sync> Sync for CachePadded<T> {}

impl<T> CachePadded<T> {
    /// Creates a new cache-padded value.
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Returns a reference to the inner value.
    #[must_use]
    pub const fn get(&self) -> &T {
        &self.value
    }

    /// Returns a mutable reference to the inner value.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Consumes the wrapper and returns the inner value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T: Default> Default for CachePadded<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: Clone> Clone for CachePadded<T> {
    fn clone(&self) -> Self {
        Self::new(self.value.clone())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for CachePadded<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachePadded")
            .field("value", &self.value)
            .finish()
    }
}

/// A lock-free single-producer single-consumer bounded queue.
///
/// This queue is designed for high-performance inter-core communication.
/// It uses atomic operations with Acquire/Release ordering to ensure
/// correct synchronization without locks.
///
/// # Safety
///
/// This queue is only safe when there is exactly one producer thread and
/// one consumer thread. Multiple producers or consumers will cause data races.
///
/// # Example
///
/// ```rust
/// use laminar_core::tpc::SpscQueue;
///
/// let queue: SpscQueue<i32> = SpscQueue::new(1024);
///
/// // Producer
/// assert!(queue.push(42).is_ok());
///
/// // Consumer
/// assert_eq!(queue.pop(), Some(42));
/// ```
pub struct SpscQueue<T> {
    /// Ring buffer storage
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Head index (consumer reads from here)
    head: CachePadded<AtomicUsize>,
    /// Tail index (producer writes here)
    tail: CachePadded<AtomicUsize>,
    /// Capacity mask for fast modulo (capacity - 1)
    capacity_mask: usize,
}

// SAFETY: SpscQueue can be sent between threads as long as T is Send
#[allow(unsafe_code)]
unsafe impl<T: Send> Send for SpscQueue<T> {}

// SAFETY: SpscQueue can be shared between threads (one producer, one consumer)
// as long as T is Send. The atomic operations ensure correct synchronization.
#[allow(unsafe_code)]
unsafe impl<T: Send> Sync for SpscQueue<T> {}

impl<T> SpscQueue<T> {
    /// Creates a new SPSC queue with the given capacity.
    ///
    /// The capacity will be rounded up to the next power of 2 for efficiency.
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0 or would overflow when rounded to power of 2.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        // Round up to next power of 2
        let capacity = capacity.next_power_of_two();

        // Allocate the buffer
        let buffer: Vec<UnsafeCell<MaybeUninit<T>>> =
            (0..capacity).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();

        Self {
            buffer: buffer.into_boxed_slice(),
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            capacity_mask: capacity - 1,
        }
    }

    /// Returns the capacity of the queue.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity_mask + 1
    }

    /// Returns true if the queue is empty.
    ///
    /// Note: This is a snapshot and may change immediately after returning.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head == tail
    }

    /// Returns true if the queue is full.
    ///
    /// Note: This is a snapshot and may change immediately after returning.
    #[must_use]
    pub fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        self.next_index(tail) == head
    }

    /// Returns the current number of items in the queue.
    ///
    /// Note: This is a snapshot and may change immediately after returning.
    #[must_use]
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        tail.wrapping_sub(head) & self.capacity_mask
    }

    /// Push an item to the queue.
    ///
    /// Returns `Ok(())` if successful, or `Err(item)` if the queue is full.
    ///
    /// # Errors
    ///
    /// Returns the item back if the queue is full.
    ///
    /// # Safety
    ///
    /// This method must only be called by the single producer thread.
    pub fn push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let next_tail = self.next_index(tail);

        // Check if queue is full
        if next_tail == self.head.load(Ordering::Acquire) {
            return Err(item);
        }

        // SAFETY: We have exclusive write access to this slot because:
        // 1. We are the only producer
        // 2. The consumer only reads slots where head < tail
        // 3. We haven't published this slot yet (tail not updated)
        #[allow(unsafe_code)]
        unsafe {
            (*self.buffer[tail].get()).write(item);
        }

        // Publish the item by updating tail
        self.tail.store(next_tail, Ordering::Release);

        Ok(())
    }

    /// Pop an item from the queue.
    ///
    /// Returns `Some(item)` if successful, or `None` if the queue is empty.
    ///
    /// # Safety
    ///
    /// This method must only be called by the single consumer thread.
    pub fn pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);

        // Check if queue is empty
        if head == self.tail.load(Ordering::Acquire) {
            return None;
        }

        // SAFETY: We have exclusive read access to this slot because:
        // 1. We are the only consumer
        // 2. The producer only writes to slots where tail > head
        // 3. This slot has been published (we checked tail > head)
        #[allow(unsafe_code)]
        let item = unsafe { (*self.buffer[head].get()).assume_init_read() };

        // Consume the item by updating head
        self.head.store(self.next_index(head), Ordering::Release);

        Some(item)
    }

    /// Push multiple items to the queue.
    ///
    /// Returns the number of items successfully pushed. Items are pushed
    /// in order, stopping at the first failure.
    ///
    /// # Safety
    ///
    /// This method must only be called by the single producer thread.
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

    /// Pop multiple items from the queue.
    ///
    /// Returns a vector of up to `max_count` items.
    ///
    /// # Safety
    ///
    /// This method must only be called by the single consumer thread.
    ///
    /// # Note
    ///
    /// This method allocates memory. For zero-allocation polling, use
    /// [`pop_batch_into`](Self::pop_batch_into) or [`pop_each`](Self::pop_each) instead.
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

    /// Pop multiple items into a caller-provided buffer (zero-allocation).
    ///
    /// Items are written to `buffer` starting at index 0. Returns the number
    /// of items actually popped (0 if queue empty or buffer full).
    ///
    /// # Safety
    ///
    /// This method must only be called by the single consumer thread.
    ///
    /// After this method returns `n`, the first `n` elements of `buffer`
    /// are initialized and can be safely read with `assume_init_read()`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use laminar_core::tpc::SpscQueue;
    /// use std::mem::MaybeUninit;
    ///
    /// let queue: SpscQueue<i32> = SpscQueue::new(16);
    /// queue.push(1).unwrap();
    /// queue.push(2).unwrap();
    ///
    /// let mut buffer: [MaybeUninit<i32>; 8] = [MaybeUninit::uninit(); 8];
    /// let count = queue.pop_batch_into(&mut buffer);
    ///
    /// assert_eq!(count, 2);
    /// // SAFETY: We just initialized these elements
    /// unsafe {
    ///     assert_eq!(buffer[0].assume_init(), 1);
    ///     assert_eq!(buffer[1].assume_init(), 2);
    /// }
    /// ```
    #[inline]
    pub fn pop_batch_into(&self, buffer: &mut [MaybeUninit<T>]) -> usize {
        if buffer.is_empty() {
            return 0;
        }

        let mut current_head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        // Calculate available items (handle wrap-around)
        let available = if tail >= current_head {
            tail - current_head
        } else {
            (self.capacity_mask + 1) - current_head + tail
        };

        let count = available.min(buffer.len());

        if count == 0 {
            return 0;
        }

        // Copy items to buffer
        for slot in buffer.iter_mut().take(count) {
            // SAFETY: We have exclusive read access to slots between head and tail.
            // The producer only writes to slots where tail > head, and we've verified
            // that these slots contain valid data by checking tail.
            #[allow(unsafe_code)]
            unsafe {
                let src = (*self.buffer[current_head].get()).assume_init_read();
                slot.write(src);
            }

            // Advance head with wrap-around
            current_head = self.next_index(current_head);
        }

        // Update head to release slots
        self.head.store(current_head, Ordering::Release);

        count
    }

    /// Pop items and call a callback for each one (zero-allocation).
    ///
    /// Processing stops when either:
    /// - `max_count` items have been processed
    /// - The queue becomes empty
    /// - The callback returns `false`
    ///
    /// Returns the number of items processed.
    ///
    /// # Safety
    ///
    /// This method must only be called by the single consumer thread.
    ///
    /// # Example
    ///
    /// ```rust
    /// use laminar_core::tpc::SpscQueue;
    ///
    /// let queue: SpscQueue<i32> = SpscQueue::new(16);
    /// queue.push(1).unwrap();
    /// queue.push(2).unwrap();
    /// queue.push(3).unwrap();
    ///
    /// let mut sum = 0;
    /// let count = queue.pop_each(10, |item| {
    ///     sum += item;
    ///     true // Continue processing
    /// });
    ///
    /// assert_eq!(count, 3);
    /// assert_eq!(sum, 6);
    /// ```
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

        // Calculate available items (handle wrap-around)
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
            // SAFETY: We have exclusive read access to slots between head and tail.
            #[allow(unsafe_code)]
            let item = unsafe { (*self.buffer[current_head].get()).assume_init_read() };

            popped += 1;

            // Advance head with wrap-around
            current_head = self.next_index(current_head);

            // Call the callback; stop if it returns false
            if !f(item) {
                break;
            }
        }

        // Update head to release processed slots
        if popped > 0 {
            self.head.store(current_head, Ordering::Release);
        }

        popped
    }

    /// Peek at the next item without removing it.
    ///
    /// Returns `None` if the queue is empty.
    ///
    /// # Safety
    ///
    /// This method must only be called by the single consumer thread.
    pub fn peek(&self) -> Option<&T> {
        let head = self.head.load(Ordering::Relaxed);

        if head == self.tail.load(Ordering::Acquire) {
            return None;
        }

        // SAFETY: Same reasoning as pop() - we have exclusive read access
        #[allow(unsafe_code)]
        unsafe {
            Some((*self.buffer[head].get()).assume_init_ref())
        }
    }

    /// Calculate the next index with wrap-around.
    #[inline]
    const fn next_index(&self, index: usize) -> usize {
        (index + 1) & self.capacity_mask
    }
}

impl<T> Drop for SpscQueue<T> {
    fn drop(&mut self) {
        // Drop any remaining items in the queue
        while self.pop().is_some() {}
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SpscQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpscQueue")
            .field("capacity", &self.capacity())
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_cache_padded_size() {
        // Verify CachePadded provides proper alignment (64 bytes = cache line)
        assert!(std::mem::align_of::<CachePadded<AtomicUsize>>() == 64);
    }

    #[test]
    fn test_cache_padded_operations() {
        let padded = CachePadded::new(42u32);
        assert_eq!(*padded, 42);
        assert_eq!(*padded.get(), 42);

        let mut padded = CachePadded::new(42u32);
        *padded.get_mut() = 100;
        assert_eq!(*padded, 100);

        let inner = padded.into_inner();
        assert_eq!(inner, 100);
    }

    #[test]
    fn test_cache_padded_default() {
        let padded: CachePadded<u32> = CachePadded::default();
        assert_eq!(*padded, 0);
    }

    #[test]
    fn test_cache_padded_clone() {
        let padded = CachePadded::new(42u32);
        let cloned = padded.clone();
        assert_eq!(*cloned, 42);
    }

    #[test]
    fn test_new_queue() {
        let queue: SpscQueue<i32> = SpscQueue::new(100);
        // Should round up to 128
        assert_eq!(queue.capacity(), 128);
        assert!(queue.is_empty());
        assert!(!queue.is_full());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_push_pop() {
        let queue: SpscQueue<i32> = SpscQueue::new(4);

        assert!(queue.push(1).is_ok());
        assert!(queue.push(2).is_ok());
        assert!(queue.push(3).is_ok());
        // Queue of capacity 4 can only hold 3 items (one slot reserved)
        assert!(queue.is_full());
        assert!(queue.push(4).is_err());

        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_fifo_order() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        for i in 0..10 {
            assert!(queue.push(i).is_ok());
        }

        for i in 0..10 {
            assert_eq!(queue.pop(), Some(i));
        }
    }

    #[test]
    fn test_wrap_around() {
        let queue: SpscQueue<i32> = SpscQueue::new(4);

        // Fill and empty multiple times to test wrap-around
        for iteration in 0..5 {
            for i in 0..3 {
                assert!(queue.push(iteration * 10 + i).is_ok());
            }
            for i in 0..3 {
                assert_eq!(queue.pop(), Some(iteration * 10 + i));
            }
        }
    }

    #[test]
    fn test_peek() {
        let queue: SpscQueue<i32> = SpscQueue::new(4);

        assert!(queue.peek().is_none());

        queue.push(42).unwrap();
        assert_eq!(queue.peek(), Some(&42));
        assert_eq!(queue.peek(), Some(&42)); // Still there

        assert_eq!(queue.pop(), Some(42));
        assert!(queue.peek().is_none());
    }

    #[test]
    fn test_push_batch() {
        let queue: SpscQueue<i32> = SpscQueue::new(8);

        let pushed = queue.push_batch(vec![1, 2, 3, 4, 5]);
        assert_eq!(pushed, 5);
        assert_eq!(queue.len(), 5);

        // Try to push more than capacity
        let pushed = queue.push_batch(vec![6, 7, 8, 9, 10]);
        assert_eq!(pushed, 2); // Only 2 more fit (7 slots max, 5 used)
    }

    #[test]
    fn test_pop_batch() {
        let queue: SpscQueue<i32> = SpscQueue::new(8);

        queue.push_batch(vec![1, 2, 3, 4, 5]);

        let items = queue.pop_batch(3);
        assert_eq!(items, vec![1, 2, 3]);
        assert_eq!(queue.len(), 2);

        let items = queue.pop_batch(10); // Request more than available
        assert_eq!(items, vec![4, 5]);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_concurrent_producer_consumer() {
        const ITEMS: i32 = 10_000;
        let queue = Arc::new(SpscQueue::<i32>::new(1024));
        let queue_producer = Arc::clone(&queue);
        let queue_consumer = Arc::clone(&queue);

        // Producer thread
        let producer = thread::spawn(move || {
            for i in 0..ITEMS {
                while queue_producer.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        // Consumer thread
        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(ITEMS as usize);
            while received.len() < ITEMS as usize {
                if let Some(item) = queue_consumer.pop() {
                    received.push(item);
                } else {
                    thread::yield_now();
                }
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        // Verify all items received in order
        assert_eq!(received.len(), ITEMS as usize);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i32::try_from(i).unwrap(), "Item out of order at index {i}");
        }
    }

    #[derive(Debug)]
    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_drop() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::Arc;

        let drop_count = Arc::new(AtomicUsize::new(0));



        {
            let queue: SpscQueue<DropCounter> = SpscQueue::new(8);
            for _ in 0..5 {
                queue.push(DropCounter(Arc::clone(&drop_count))).unwrap();
            }
            // Pop 2 items
            queue.pop();
            queue.pop();
            // Queue drops with 3 items remaining
        }

        // All 5 items should be dropped (2 popped + 3 on drop)
        assert_eq!(drop_count.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn test_debug() {
        let queue: SpscQueue<i32> = SpscQueue::new(8);
        queue.push(1).unwrap();
        queue.push(2).unwrap();

        let debug_str = format!("{queue:?}");
        assert!(debug_str.contains("SpscQueue"));
        assert!(debug_str.contains("capacity"));
        assert!(debug_str.contains("len"));
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn test_zero_capacity_panics() {
        let _: SpscQueue<i32> = SpscQueue::new(0);
    }

    #[test]
    fn test_pop_batch_into() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        // Push some items
        queue.push(1).unwrap();
        queue.push(2).unwrap();
        queue.push(3).unwrap();

        // Pop into buffer
        let mut buffer: [MaybeUninit<i32>; 8] = [MaybeUninit::uninit(); 8];
        let count = queue.pop_batch_into(&mut buffer);

        assert_eq!(count, 3);

        // SAFETY: We just initialized these elements
        #[allow(unsafe_code)]
        unsafe {
            assert_eq!(buffer[0].assume_init(), 1);
            assert_eq!(buffer[1].assume_init(), 2);
            assert_eq!(buffer[2].assume_init(), 3);
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_pop_batch_into_partial() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        // Push 5 items
        for i in 0..5 {
            queue.push(i).unwrap();
        }

        // Pop only 3 (buffer smaller than available)
        let mut buffer: [MaybeUninit<i32>; 3] = [MaybeUninit::uninit(); 3];
        let count = queue.pop_batch_into(&mut buffer);

        assert_eq!(count, 3);
        assert_eq!(queue.len(), 2); // 2 items remaining

        // SAFETY: We just initialized these elements
        #[allow(unsafe_code)]
        unsafe {
            assert_eq!(buffer[0].assume_init(), 0);
            assert_eq!(buffer[1].assume_init(), 1);
            assert_eq!(buffer[2].assume_init(), 2);
        }
    }

    #[test]
    fn test_pop_batch_into_empty() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        let mut buffer: [MaybeUninit<i32>; 8] = [MaybeUninit::uninit(); 8];
        let count = queue.pop_batch_into(&mut buffer);

        assert_eq!(count, 0);
    }

    #[test]
    fn test_pop_batch_into_empty_buffer() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);
        queue.push(1).unwrap();

        let mut buffer: [MaybeUninit<i32>; 0] = [];
        let count = queue.pop_batch_into(&mut buffer);

        assert_eq!(count, 0);
        assert_eq!(queue.len(), 1); // Item still in queue
    }

    #[test]
    fn test_pop_each() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        queue.push(1).unwrap();
        queue.push(2).unwrap();
        queue.push(3).unwrap();

        let mut sum = 0;
        let count = queue.pop_each(10, |item| {
            sum += item;
            true
        });

        assert_eq!(count, 3);
        assert_eq!(sum, 6);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_pop_each_early_stop() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        queue.push(1).unwrap();
        queue.push(2).unwrap();
        queue.push(3).unwrap();
        queue.push(4).unwrap();
        queue.push(5).unwrap();

        let mut items = Vec::new();
        let count = queue.pop_each(10, |item| {
            items.push(item);
            item < 3 // Stop after item 3
        });

        assert_eq!(count, 3); // Processed 1, 2, 3
        assert_eq!(items, vec![1, 2, 3]);
        assert_eq!(queue.len(), 2); // 4, 5 remaining
    }

    #[test]
    fn test_pop_each_max_count() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        let mut count_processed = 0;
        let count = queue.pop_each(5, |_| {
            count_processed += 1;
            true
        });

        assert_eq!(count, 5);
        assert_eq!(count_processed, 5);
        assert_eq!(queue.len(), 5); // 5 remaining
    }

    #[test]
    fn test_pop_each_empty() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);

        let mut called = false;
        let count = queue.pop_each(10, |_| {
            called = true;
            true
        });

        assert_eq!(count, 0);
        assert!(!called);
    }

    #[test]
    fn test_pop_each_zero_max() {
        let queue: SpscQueue<i32> = SpscQueue::new(16);
        queue.push(1).unwrap();

        let count = queue.pop_each(0, |_| true);

        assert_eq!(count, 0);
        assert_eq!(queue.len(), 1); // Item still in queue
    }

    #[test]
    fn test_pop_batch_into_wrap_around() {
        let queue: SpscQueue<i32> = SpscQueue::new(4); // Capacity 4

        // Fill and empty to advance indices
        for _ in 0..3 {
            for i in 0..3 {
                queue.push(i).unwrap();
            }
            for _ in 0..3 {
                queue.pop();
            }
        }

        // Now indices are wrapped, push new items
        queue.push(10).unwrap();
        queue.push(11).unwrap();

        let mut buffer: [MaybeUninit<i32>; 4] = [MaybeUninit::uninit(); 4];
        let count = queue.pop_batch_into(&mut buffer);

        assert_eq!(count, 2);

        #[allow(unsafe_code)]
        unsafe {
            assert_eq!(buffer[0].assume_init(), 10);
            assert_eq!(buffer[1].assume_init(), 11);
        }
    }

    #[test]
    fn test_pop_each_wrap_around() {
        let queue: SpscQueue<i32> = SpscQueue::new(4);

        // Fill and empty to advance indices
        for _ in 0..3 {
            for i in 0..3 {
                queue.push(i).unwrap();
            }
            let _ = queue.pop_batch(3);
        }

        // Now push with wrapped indices
        queue.push(100).unwrap();
        queue.push(200).unwrap();

        let mut items = Vec::new();
        queue.pop_each(10, |item| {
            items.push(item);
            true
        });

        assert_eq!(items, vec![100, 200]);
    }
}
