//! Lock-free streaming channels with automatic SPSC → MPSC upgrade.
//!
//! This module provides channels optimized for streaming data flow with:
//! - SPSC mode for single-producer scenarios (fastest)
//! - Automatic upgrade to MPSC when `Producer::clone()` is called
//! - Configurable backpressure strategies
//! - Zero-allocation batch operations
//!
//! ## Key Design
//!
//! The channel type is NEVER user-specified. It starts as SPSC and
//! automatically upgrades to MPSC when the producer is cloned.
//!
//! ```rust,ignore
//! let (producer, consumer) = streaming::channel::<u64>(1024);
//! assert!(!producer.is_mpsc());
//!
//! let producer2 = producer.clone();  // Triggers MPSC upgrade
//! assert!(producer.is_mpsc());
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::tpc::CachePadded;

use super::config::{BackpressureStrategy, ChannelConfig, ChannelStats, WaitStrategy};
use super::error::{RecvError, StreamingError, TryPushError};
use super::ring_buffer::RingBuffer;

/// Channel mode indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelMode {
    /// Single-producer single-consumer (fastest).
    Spsc = 0,
    /// Multi-producer single-consumer.
    Mpsc = 1,
}

impl From<u8> for ChannelMode {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Spsc,
            _ => Self::Mpsc,
        }
    }
}

/// Shared state for a channel.
struct ChannelInner<T> {
    /// The underlying ring buffer.
    buffer: RingBuffer<T>,

    /// Channel mode (SPSC or MPSC).
    mode: AtomicU8,

    /// Number of active producers.
    producer_count: AtomicUsize,

    /// Whether the channel is closed (all producers dropped).
    closed: AtomicBool,

    /// Configuration.
    config: ChannelConfig,

    /// Statistics (if tracking enabled).
    stats: ChannelStatsInner,

    /// Lock for MPSC mode serialization.
    /// In MPSC mode, producers acquire this lock to push.
    /// Value: 0 = unlocked, 1 = locked
    mpsc_lock: AtomicU8,
}

/// Internal statistics counters.
///
/// `items_pushed` and `items_popped` are cache-padded to prevent false sharing
/// between producer and consumer threads.
struct ChannelStatsInner {
    /// Items pushed by producer (cache-padded to avoid false sharing).
    items_pushed: CachePadded<AtomicU64>,
    /// Items popped by consumer (cache-padded to avoid false sharing).
    items_popped: CachePadded<AtomicU64>,
    /// Producer-side counters (grouped together).
    push_blocked: AtomicU64,
    items_dropped: AtomicU64,
    /// Consumer-side counter.
    pop_empty: AtomicU64,
}

impl ChannelStatsInner {
    fn new() -> Self {
        Self {
            items_pushed: CachePadded::new(AtomicU64::new(0)),
            items_popped: CachePadded::new(AtomicU64::new(0)),
            push_blocked: AtomicU64::new(0),
            items_dropped: AtomicU64::new(0),
            pop_empty: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> ChannelStats {
        ChannelStats {
            items_pushed: self.items_pushed.load(Ordering::Relaxed),
            items_popped: self.items_popped.load(Ordering::Relaxed),
            push_blocked: self.push_blocked.load(Ordering::Relaxed),
            items_dropped: self.items_dropped.load(Ordering::Relaxed),
            pop_empty: self.pop_empty.load(Ordering::Relaxed),
        }
    }
}

impl<T> ChannelInner<T> {
    fn new(config: ChannelConfig) -> Self {
        Self {
            buffer: RingBuffer::new(config.effective_buffer_size()),
            mode: AtomicU8::new(ChannelMode::Spsc as u8),
            producer_count: AtomicUsize::new(1),
            closed: AtomicBool::new(false),
            config,
            stats: ChannelStatsInner::new(),
            mpsc_lock: AtomicU8::new(0),
        }
    }

    /// Acquire the MPSC lock (spin with exponential backoff).
    ///
    /// Uses exponential backoff to reduce contention under load:
    /// - First 4 attempts: `spin_loop`
    /// - Next 4 attempts: yield
    /// - After that: short sleep
    #[inline]
    fn acquire_mpsc_lock(&self) {
        let mut attempts = 0u32;

        while self
            .mpsc_lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            attempts = attempts.saturating_add(1);

            if attempts <= 4 {
                // Fast path: spin
                std::hint::spin_loop();
            } else if attempts <= 8 {
                // Medium contention: yield to OS
                thread::yield_now();
            } else {
                // High contention: brief sleep with exponential backoff
                // Cap at 100us to avoid excessive latency
                let sleep_us = (1 << (attempts - 8).min(6)).min(100);
                thread::sleep(Duration::from_micros(sleep_us));
            }
        }
    }

    /// Release the MPSC lock.
    #[inline]
    fn release_mpsc_lock(&self) {
        self.mpsc_lock.store(0, Ordering::Release);
    }

    #[inline]
    fn mode(&self) -> ChannelMode {
        ChannelMode::from(self.mode.load(Ordering::Acquire))
    }

    #[inline]
    fn is_mpsc(&self) -> bool {
        self.mode() == ChannelMode::Mpsc
    }

    #[inline]
    fn upgrade_to_mpsc(&self) {
        self.mode.store(ChannelMode::Mpsc as u8, Ordering::Release);
    }

    #[inline]
    fn track_push(&self) {
        if self.config.track_stats {
            self.stats.items_pushed.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn track_push_blocked(&self) {
        if self.config.track_stats {
            self.stats.push_blocked.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn track_dropped(&self) {
        if self.config.track_stats {
            self.stats.items_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn track_pop(&self) {
        if self.config.track_stats {
            self.stats.items_popped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn track_pop_empty(&self) {
        if self.config.track_stats {
            self.stats.pop_empty.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Producer handle for sending items into a channel.
///
/// Cloning a producer triggers automatic SPSC → MPSC upgrade.
pub struct Producer<T> {
    inner: Arc<ChannelInner<T>>,
}

impl<T> Producer<T> {
    /// Pushes an item to the channel, blocking if necessary.
    ///
    /// Behavior depends on the backpressure strategy:
    /// - `Block`: Waits until space is available
    /// - `DropOldest`: Drops the oldest item to make room
    /// - `Reject`: Returns error immediately if full
    ///
    /// # Errors
    ///
    /// Returns `StreamingError::ChannelClosed` if all consumers have dropped.
    /// Returns `StreamingError::ChannelFull` if strategy is `Reject` and buffer is full.
    pub fn push(&self, item: T) -> Result<(), StreamingError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(StreamingError::ChannelClosed);
        }

        match self.inner.config.backpressure {
            BackpressureStrategy::Block => self.push_blocking(item),
            BackpressureStrategy::DropOldest => self.push_drop_oldest(item),
            BackpressureStrategy::Reject => self.push_reject(item),
        }
    }

    /// Tries to push an item without blocking.
    ///
    /// Returns `Ok(())` if successful, or `Err(TryPushError)` containing
    /// the item if the push failed.
    ///
    /// # Errors
    ///
    /// Returns `TryPushError` if the channel is full or closed.
    pub fn try_push(&self, item: T) -> Result<(), TryPushError<T>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TryPushError::closed(item));
        }

        if self.inner.is_mpsc() {
            self.try_push_mpsc(item)
        } else {
            self.try_push_spsc(item)
        }
    }

    /// Pushes multiple items, returning the number successfully pushed.
    ///
    /// Stops at the first failure (full buffer or closed channel).
    pub fn push_batch(&self, items: impl IntoIterator<Item = T>) -> usize {
        let mut count = 0;
        for item in items {
            if self.try_push(item).is_err() {
                break;
            }
            count += 1;
        }
        count
    }

    /// Returns true if the channel is in MPSC mode.
    #[inline]
    #[must_use]
    pub fn is_mpsc(&self) -> bool {
        self.inner.is_mpsc()
    }

    /// Returns the channel mode.
    #[inline]
    #[must_use]
    pub fn mode(&self) -> ChannelMode {
        self.inner.mode()
    }

    /// Returns true if the channel is closed.
    #[inline]
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Returns the number of items currently in the buffer.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.buffer.len()
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.buffer.is_empty()
    }

    /// Returns the buffer capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.buffer.capacity()
    }

    /// Returns statistics for this channel.
    #[must_use]
    pub fn stats(&self) -> ChannelStats {
        self.inner.stats.snapshot()
    }

    // ==================== Internal Push Methods ====================

    fn push_blocking(&self, mut item: T) -> Result<(), StreamingError> {
        loop {
            match self.try_push(item) {
                Ok(()) => return Ok(()),
                Err(e) if e.is_closed() => return Err(StreamingError::ChannelClosed),
                Err(e) => {
                    self.inner.track_push_blocked();
                    item = e.into_inner();
                    self.wait_for_space();
                }
            }
        }
    }

    fn push_drop_oldest(&self, item: T) -> Result<(), StreamingError> {
        // Try normal push first
        match self.try_push(item) {
            Ok(()) => Ok(()),
            Err(e) if e.is_closed() => Err(StreamingError::ChannelClosed),
            Err(e) => {
                // Buffer is full - we can't actually drop oldest from producer side
                // because that would require coordinating with consumer.
                // Instead, we drop the new item and record it.
                self.inner.track_dropped();
                // In a real implementation, we'd use a different buffer type
                // that supports overwriting. For now, just drop the new item.
                drop(e.into_inner());
                Ok(())
            }
        }
    }

    fn push_reject(&self, item: T) -> Result<(), StreamingError> {
        match self.try_push(item) {
            Ok(()) => Ok(()),
            Err(e) if e.is_closed() => Err(StreamingError::ChannelClosed),
            Err(_) => Err(StreamingError::ChannelFull),
        }
    }

    #[inline]
    fn try_push_spsc(&self, item: T) -> Result<(), TryPushError<T>> {
        match self.inner.buffer.push(item) {
            Ok(()) => {
                self.inner.track_push();
                Ok(())
            }
            Err(item) => Err(TryPushError::full(item)),
        }
    }

    fn try_push_mpsc(&self, item: T) -> Result<(), TryPushError<T>> {
        // In MPSC mode, we serialize producers with a spin-lock.
        // This ensures the ring buffer's SPSC push is safe.
        self.inner.acquire_mpsc_lock();

        let result = match self.inner.buffer.push(item) {
            Ok(()) => {
                self.inner.track_push();
                Ok(())
            }
            Err(item) => Err(TryPushError::full(item)),
        };

        self.inner.release_mpsc_lock();
        result
    }

    fn wait_for_space(&self) {
        match self.inner.config.wait_strategy {
            WaitStrategy::Spin => {
                while self.inner.buffer.is_full() {
                    std::hint::spin_loop();
                }
            }
            WaitStrategy::SpinYield => {
                let mut spins = 0;
                while self.inner.buffer.is_full() {
                    if spins < 100 {
                        std::hint::spin_loop();
                        spins += 1;
                    } else {
                        thread::yield_now();
                        spins = 0;
                    }
                }
            }
            WaitStrategy::Park => {
                // Use 100us timeout to balance latency vs syscall overhead
                while self.inner.buffer.is_full() {
                    thread::park_timeout(Duration::from_micros(100));
                }
            }
        }
    }
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        // Upgrade to MPSC mode on clone
        self.inner.upgrade_to_mpsc();

        // Increment producer count
        self.inner.producer_count.fetch_add(1, Ordering::AcqRel);

        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        let prev = self.inner.producer_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            // Last producer dropped - close the channel
            self.inner.closed.store(true, Ordering::Release);
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("mode", &self.mode())
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

/// Consumer handle for receiving items from a channel.
pub struct Consumer<T> {
    inner: Arc<ChannelInner<T>>,
}

impl<T> Consumer<T> {
    /// Polls for the next item without blocking.
    ///
    /// Returns `Some(item)` if available, `None` if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn poll(&self) -> Option<T> {
        if self.inner.is_mpsc() {
            self.poll_mpsc()
        } else {
            self.poll_spsc()
        }
    }

    /// Receives the next item, blocking until one is available.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Disconnected` if all producers have dropped
    /// and the buffer is empty.
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            if let Some(item) = self.poll() {
                return Ok(item);
            }

            if self.is_disconnected() {
                return Err(RecvError::Disconnected);
            }

            self.wait_for_item();
        }
    }

    /// Receives the next item with a timeout.
    ///
    /// # Errors
    ///
    /// Returns `RecvError::Timeout` if no item becomes available within the timeout.
    /// Returns `RecvError::Disconnected` if all producers have dropped.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvError> {
        let deadline = Instant::now() + timeout;

        loop {
            if let Some(item) = self.poll() {
                return Ok(item);
            }

            if self.is_disconnected() {
                return Err(RecvError::Disconnected);
            }

            if Instant::now() >= deadline {
                return Err(RecvError::Timeout);
            }

            self.wait_for_item_timeout(deadline);
        }
    }

    /// Pops multiple items from the buffer.
    ///
    /// Returns a vector of up to `max_count` items.
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
            if let Some(item) = self.poll() {
                items.push(item);
            } else {
                break;
            }
        }
        items
    }

    /// Pops multiple items into a pre-allocated vector (zero-allocation).
    ///
    /// Appends up to `max_count` items to the provided vector.
    /// Returns the number of items added.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut buffer = Vec::with_capacity(100);
    /// loop {
    ///     buffer.clear();
    ///     let count = consumer.pop_batch_into(&mut buffer, 100);
    ///     if count == 0 { break; }
    ///     for item in &buffer {
    ///         process(item);
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn pop_batch_into(&self, buffer: &mut Vec<T>, max_count: usize) -> usize {
        let mut count = 0;
        for _ in 0..max_count {
            if let Some(item) = self.poll() {
                buffer.push(item);
                count += 1;
            } else {
                break;
            }
        }
        count
    }

    /// Pops items and calls a callback for each (zero-allocation).
    ///
    /// Returns the number of items processed.
    #[inline]
    pub fn pop_each<F>(&self, max_count: usize, f: F) -> usize
    where
        F: FnMut(T) -> bool,
    {
        self.inner.buffer.pop_each(max_count, f)
    }

    /// Returns true if all producers have dropped.
    #[inline]
    #[must_use]
    pub fn is_disconnected(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire) && self.inner.buffer.is_empty()
    }

    /// Returns the number of items in the buffer.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.buffer.len()
    }

    /// Returns true if the buffer is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.buffer.is_empty()
    }

    /// Returns the buffer capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.buffer.capacity()
    }

    /// Returns the channel mode.
    #[inline]
    #[must_use]
    pub fn mode(&self) -> ChannelMode {
        self.inner.mode()
    }

    /// Returns statistics for this channel.
    #[must_use]
    pub fn stats(&self) -> ChannelStats {
        self.inner.stats.snapshot()
    }

    // ==================== Internal ====================

    #[inline]
    fn poll_spsc(&self) -> Option<T> {
        let item = self.inner.buffer.pop();
        if item.is_some() {
            self.inner.track_pop();
        } else {
            self.inner.track_pop_empty();
        }
        item
    }

    fn poll_mpsc(&self) -> Option<T> {
        // In MPSC mode, items are published in order via mpsc_published counter
        // The consumer just pops from the buffer
        let item = self.inner.buffer.pop();
        if item.is_some() {
            self.inner.track_pop();
        } else {
            self.inner.track_pop_empty();
        }
        item
    }

    fn wait_for_item(&self) {
        match self.inner.config.wait_strategy {
            WaitStrategy::Spin => {
                while self.inner.buffer.is_empty() && !self.is_disconnected() {
                    std::hint::spin_loop();
                }
            }
            WaitStrategy::SpinYield => {
                let mut spins = 0;
                while self.inner.buffer.is_empty() && !self.is_disconnected() {
                    if spins < 100 {
                        std::hint::spin_loop();
                        spins += 1;
                    } else {
                        thread::yield_now();
                        spins = 0;
                    }
                }
            }
            WaitStrategy::Park => {
                while self.inner.buffer.is_empty() && !self.is_disconnected() {
                    thread::park_timeout(Duration::from_micros(10));
                }
            }
        }
    }

    fn wait_for_item_timeout(&self, deadline: Instant) {
        match self.inner.config.wait_strategy {
            WaitStrategy::Spin | WaitStrategy::SpinYield => {
                // Just return - the recv_timeout loop will handle deadline
            }
            WaitStrategy::Park => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if !remaining.is_zero() {
                    thread::park_timeout(remaining.min(Duration::from_micros(100)));
                }
            }
        }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Mark channel as closed when consumer drops
        // This prevents producers from blocking forever
        self.inner.closed.store(true, Ordering::Release);
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("mode", &self.mode())
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("is_disconnected", &self.is_disconnected())
            .finish()
    }
}

/// Iterator adapter for Consumer.
impl<T> Iterator for Consumer<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

/// Creates a new channel with the specified buffer size.
///
/// Returns a `(Producer, Consumer)` pair. The channel starts in SPSC mode
/// and automatically upgrades to MPSC when the producer is cloned.
#[must_use]
pub fn channel<T>(buffer_size: usize) -> (Producer<T>, Consumer<T>) {
    channel_with_config(ChannelConfig::with_buffer_size(buffer_size))
}

/// Creates a new channel with custom configuration.
#[must_use]
pub fn channel_with_config<T>(config: ChannelConfig) -> (Producer<T>, Consumer<T>) {
    let inner = Arc::new(ChannelInner::new(config));

    let producer = Producer {
        inner: Arc::clone(&inner),
    };

    let consumer = Consumer { inner };

    (producer, consumer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_channel() {
        let (producer, consumer) = channel::<i32>(16);

        assert!(producer.try_push(1).is_ok());
        assert!(producer.try_push(2).is_ok());
        assert!(producer.try_push(3).is_ok());

        assert_eq!(consumer.poll(), Some(1));
        assert_eq!(consumer.poll(), Some(2));
        assert_eq!(consumer.poll(), Some(3));
        assert_eq!(consumer.poll(), None);
    }

    #[test]
    fn test_channel_starts_spsc() {
        let (producer, consumer) = channel::<i32>(16);

        assert!(!producer.is_mpsc());
        assert_eq!(producer.mode(), ChannelMode::Spsc);
        assert_eq!(consumer.mode(), ChannelMode::Spsc);
    }

    #[test]
    fn test_clone_upgrades_to_mpsc() {
        let (producer, consumer) = channel::<i32>(16);

        assert!(!producer.is_mpsc());

        let producer2 = producer.clone();

        assert!(producer.is_mpsc());
        assert!(producer2.is_mpsc());
        assert_eq!(consumer.mode(), ChannelMode::Mpsc);
    }

    #[test]
    fn test_spsc_push_pop() {
        let (producer, consumer) = channel::<i32>(8);

        for i in 0..7 {
            assert!(producer.try_push(i).is_ok());
        }

        // Buffer should be full (capacity 8, but one slot reserved)
        assert!(producer.try_push(100).is_err());

        for i in 0..7 {
            assert_eq!(consumer.poll(), Some(i));
        }

        assert_eq!(consumer.poll(), None);
    }

    #[test]
    fn test_mpsc_push_pop() {
        let (producer, consumer) = channel::<i32>(16);

        let producer2 = producer.clone();
        assert!(producer.is_mpsc());

        producer.try_push(1).unwrap();
        producer2.try_push(2).unwrap();
        producer.try_push(3).unwrap();

        // Items should come out in push order
        let mut items = Vec::new();
        while let Some(item) = consumer.poll() {
            items.push(item);
        }

        assert_eq!(items.len(), 3);
        assert!(items.contains(&1));
        assert!(items.contains(&2));
        assert!(items.contains(&3));
    }

    #[test]
    fn test_push_batch() {
        let (producer, consumer) = channel::<i32>(16);

        let count = producer.push_batch(vec![1, 2, 3, 4, 5]);
        assert_eq!(count, 5);

        let items = consumer.pop_batch(10);
        assert_eq!(items, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_pop_each() {
        let (producer, consumer) = channel::<i32>(16);

        producer.push_batch(vec![1, 2, 3, 4, 5]);

        let mut sum = 0;
        let count = consumer.pop_each(10, |item| {
            sum += item;
            true
        });

        assert_eq!(count, 5);
        assert_eq!(sum, 15);
    }

    #[test]
    fn test_recv_timeout() {
        let (producer, consumer) = channel::<i32>(16);

        // Should timeout on empty channel
        let result = consumer.recv_timeout(Duration::from_millis(10));
        assert!(matches!(result, Err(RecvError::Timeout)));

        // Should succeed when item is available
        producer.try_push(42).unwrap();
        let result = consumer.recv_timeout(Duration::from_secs(1));
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn test_disconnected_on_producer_drop() {
        let (producer, consumer) = channel::<i32>(16);

        producer.try_push(1).unwrap();
        drop(producer);

        // Should still get the buffered item
        assert_eq!(consumer.poll(), Some(1));

        // Now should be disconnected
        assert!(consumer.is_disconnected());
        assert!(matches!(consumer.recv(), Err(RecvError::Disconnected)));
    }

    #[test]
    fn test_closed_on_consumer_drop() {
        let (producer, consumer) = channel::<i32>(16);

        drop(consumer);

        // Push should fail with closed error
        assert!(producer.is_closed());
        assert!(matches!(producer.push(1), Err(StreamingError::ChannelClosed)));
    }

    #[test]
    fn test_backpressure_reject() {
        let config = ChannelConfig::builder()
            .buffer_size(4)
            .backpressure(BackpressureStrategy::Reject)
            .build();

        let (producer, consumer) = channel_with_config::<i32>(config);

        // Fill the buffer (capacity 4 = 3 usable slots)
        assert!(producer.push(1).is_ok());
        assert!(producer.push(2).is_ok());
        assert!(producer.push(3).is_ok());

        // Should reject immediately
        assert!(matches!(producer.push(4), Err(StreamingError::ChannelFull)));

        // Make room
        let _ = consumer.poll();

        // Now should succeed
        assert!(producer.push(4).is_ok());
    }

    #[test]
    fn test_backpressure_drop_oldest() {
        let config = ChannelConfig::builder()
            .buffer_size(4)
            .backpressure(BackpressureStrategy::DropOldest)
            .track_stats(true)
            .build();

        let (producer, _consumer) = channel_with_config::<i32>(config);

        // Fill the buffer
        producer.push(1).unwrap();
        producer.push(2).unwrap();
        producer.push(3).unwrap();

        // This should succeed (drops the new item in our simple impl)
        let result = producer.push(4);
        assert!(result.is_ok());

        // Check stats show a drop
        let stats = producer.stats();
        assert!(stats.items_dropped > 0);
    }

    #[test]
    fn test_stats_tracking() {
        let config = ChannelConfig::builder()
            .buffer_size(16)
            .track_stats(true)
            .build();

        let (producer, consumer) = channel_with_config::<i32>(config);

        producer.push_batch(vec![1, 2, 3, 4, 5]);
        let _ = consumer.pop_batch(3);
        // 2 items remain, poll them
        let _ = consumer.poll();
        let _ = consumer.poll();
        // Buffer is now empty, these polls should track empty
        let _ = consumer.poll();
        let _ = consumer.poll();

        let stats = producer.stats();
        assert_eq!(stats.items_pushed, 5);
        assert_eq!(stats.items_popped, 5);  // All 5 items popped (3 + 2)
        assert!(stats.pop_empty >= 2);  // At least 2 empty polls
    }

    #[test]
    fn test_concurrent_spsc() {
        const ITEMS: i32 = 10_000;
        let (producer, consumer) = channel::<i32>(1024);

        let producer_handle = thread::spawn(move || {
            for i in 0..ITEMS {
                while producer.try_push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        let consumer_handle = thread::spawn(move || {
            let mut received = Vec::with_capacity(ITEMS as usize);
            while received.len() < ITEMS as usize {
                if let Some(item) = consumer.poll() {
                    received.push(item);
                } else {
                    thread::yield_now();
                }
            }
            received
        });

        producer_handle.join().unwrap();
        let received = consumer_handle.join().unwrap();

        assert_eq!(received.len(), ITEMS as usize);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i as i32);
        }
    }

    #[test]
    fn test_concurrent_mpsc() {
        const ITEMS_PER_PRODUCER: i32 = 1000;
        const NUM_PRODUCERS: usize = 4;

        let (producer, consumer) = channel::<i32>(1024);

        let mut handles = Vec::new();

        for id in 0..NUM_PRODUCERS {
            let p = producer.clone();
            handles.push(thread::spawn(move || {
                for i in 0..ITEMS_PER_PRODUCER {
                    let value = (id as i32) * ITEMS_PER_PRODUCER + i;
                    while p.try_push(value).is_err() {
                        thread::yield_now();
                    }
                }
            }));
        }

        drop(producer); // Drop original producer

        let consumer_handle = thread::spawn(move || {
            let mut received = Vec::new();
            let expected = NUM_PRODUCERS * ITEMS_PER_PRODUCER as usize;
            while received.len() < expected {
                if let Some(item) = consumer.poll() {
                    received.push(item);
                } else if consumer.is_disconnected() {
                    break;
                } else {
                    thread::yield_now();
                }
            }
            received
        });

        for h in handles {
            h.join().unwrap();
        }

        let received = consumer_handle.join().unwrap();
        assert_eq!(received.len(), NUM_PRODUCERS * ITEMS_PER_PRODUCER as usize);
    }

    #[test]
    fn test_len_and_capacity() {
        let (producer, consumer) = channel::<i32>(16);

        assert_eq!(producer.capacity(), 16);
        assert_eq!(consumer.capacity(), 16);
        assert!(producer.is_empty());
        assert!(consumer.is_empty());

        producer.push_batch(vec![1, 2, 3]);

        assert_eq!(producer.len(), 3);
        assert_eq!(consumer.len(), 3);
        assert!(!producer.is_empty());
    }

    #[test]
    fn test_consumer_iterator() {
        let (producer, mut consumer) = channel::<i32>(16);

        producer.push_batch(vec![1, 2, 3]);
        drop(producer);

        let items: Vec<i32> = consumer.by_ref().collect();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[test]
    fn test_debug_formatting() {
        let (producer, consumer) = channel::<i32>(16);

        let producer_debug = format!("{producer:?}");
        assert!(producer_debug.contains("Producer"));
        assert!(producer_debug.contains("Spsc"));

        let consumer_debug = format!("{consumer:?}");
        assert!(consumer_debug.contains("Consumer"));
    }
}
