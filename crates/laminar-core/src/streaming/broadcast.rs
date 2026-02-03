//! Broadcast Channel for multi-consumer streaming.
//!
//! [`BroadcastChannel<T>`] implements a shared ring buffer with per-subscriber cursors
//! for single-producer, multiple-consumer (SPMC) broadcast. Designed for Ring 0 hot path:
//! zero allocations after construction.
//!
//! # Design
//!
//! - Pre-allocated ring buffer with power-of-2 capacity and bitmask indexing
//! - Single producer via [`broadcast()`](BroadcastChannel::broadcast)
//! - Dynamic subscribers via [`subscribe()`](BroadcastChannel::subscribe) and
//!   [`unsubscribe()`](BroadcastChannel::unsubscribe)
//! - Configurable slow subscriber policies: `Block`, `DropSlow`, `SkipForSlow`
//! - Per-subscriber lag tracking for backpressure monitoring
//!
//! # Key Principle
//!
//! **Broadcast is derived from query plan analysis, not user configuration.**
//! The planner determines when multiple MVs read from the same source and
//! auto-upgrades to broadcast mode.
//!
//! # Safety
//!
//! The single-writer invariant is upheld by the DAG executor, which ensures
//! exactly one thread calls `broadcast()` on any given channel. Multiple threads
//! may call `read()` with distinct subscriber IDs.
//!
//! # Performance Targets
//!
//! | Operation | Target |
//! |-----------|--------|
//! | `broadcast()` | < 100ns (2 subscribers) |
//! | `read()` | < 50ns |
//! | `subscribe()` | O(1), takes write lock (Ring 2 only) |

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use crate::tpc::CachePadded;

/// Default buffer capacity (power of 2).
pub const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

/// Default maximum subscribers.
pub const DEFAULT_MAX_SUBSCRIBERS: usize = 64;

/// Default slow subscriber timeout.
pub const DEFAULT_SLOW_SUBSCRIBER_TIMEOUT: Duration = Duration::from_millis(100);

/// Default lag warning threshold.
pub const DEFAULT_LAG_WARNING_THRESHOLD: u64 = 1000;

/// Slow subscriber handling policy.
///
/// Determines what happens when the slowest subscriber is too far behind
/// the producer (about to overwrite unread data).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SlowSubscriberPolicy {
    /// Block producer until slow subscriber catches up (default).
    ///
    /// Best for exactly-once semantics where no data loss is acceptable.
    /// May cause head-of-line blocking if one subscriber is permanently slow.
    #[default]
    Block,

    /// Drop the slowest subscriber and continue.
    ///
    /// Best for systems where continuing is more important than any single
    /// subscriber. The dropped subscriber receives a disconnection signal.
    DropSlow,

    /// Skip messages for slow subscribers (they lose data).
    ///
    /// Best for real-time systems where freshness matters more than
    /// completeness. Slow subscribers simply miss events.
    SkipForSlow,
}

/// Broadcast channel configuration.
#[derive(Debug, Clone)]
pub struct BroadcastConfig {
    /// Buffer capacity (will be rounded to power of 2).
    pub capacity: usize,

    /// Maximum allowed subscribers.
    pub max_subscribers: usize,

    /// Policy when slowest subscriber is too far behind.
    pub slow_subscriber_policy: SlowSubscriberPolicy,

    /// Timeout for blocking on slow subscriber (Block policy).
    pub slow_subscriber_timeout: Duration,

    /// Lag threshold for warnings.
    pub lag_warning_threshold: u64,
}

impl Default for BroadcastConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_BROADCAST_CAPACITY,
            max_subscribers: DEFAULT_MAX_SUBSCRIBERS,
            slow_subscriber_policy: SlowSubscriberPolicy::Block,
            slow_subscriber_timeout: DEFAULT_SLOW_SUBSCRIBER_TIMEOUT,
            lag_warning_threshold: DEFAULT_LAG_WARNING_THRESHOLD,
        }
    }
}

impl BroadcastConfig {
    /// Creates a new configuration with the specified capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }

    /// Creates a builder for custom configuration.
    #[must_use]
    pub fn builder() -> BroadcastConfigBuilder {
        BroadcastConfigBuilder::default()
    }

    /// Returns the effective capacity (rounded to power of 2).
    #[must_use]
    pub fn effective_capacity(&self) -> usize {
        self.capacity.max(4).next_power_of_two()
    }
}

/// Builder for [`BroadcastConfig`].
#[derive(Debug, Default)]
pub struct BroadcastConfigBuilder {
    capacity: Option<usize>,
    max_subscribers: Option<usize>,
    slow_subscriber_policy: Option<SlowSubscriberPolicy>,
    slow_subscriber_timeout: Option<Duration>,
    lag_warning_threshold: Option<u64>,
}

impl BroadcastConfigBuilder {
    /// Sets the buffer capacity.
    #[must_use]
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// Sets the maximum number of subscribers.
    #[must_use]
    pub fn max_subscribers(mut self, max: usize) -> Self {
        self.max_subscribers = Some(max);
        self
    }

    /// Sets the slow subscriber policy.
    #[must_use]
    pub fn slow_subscriber_policy(mut self, policy: SlowSubscriberPolicy) -> Self {
        self.slow_subscriber_policy = Some(policy);
        self
    }

    /// Sets the slow subscriber timeout (for Block policy).
    #[must_use]
    pub fn slow_subscriber_timeout(mut self, timeout: Duration) -> Self {
        self.slow_subscriber_timeout = Some(timeout);
        self
    }

    /// Sets the lag warning threshold.
    #[must_use]
    pub fn lag_warning_threshold(mut self, threshold: u64) -> Self {
        self.lag_warning_threshold = Some(threshold);
        self
    }

    /// Builds the configuration.
    #[must_use]
    pub fn build(self) -> BroadcastConfig {
        BroadcastConfig {
            capacity: self.capacity.unwrap_or(DEFAULT_BROADCAST_CAPACITY),
            max_subscribers: self.max_subscribers.unwrap_or(DEFAULT_MAX_SUBSCRIBERS),
            slow_subscriber_policy: self.slow_subscriber_policy.unwrap_or_default(),
            slow_subscriber_timeout: self
                .slow_subscriber_timeout
                .unwrap_or(DEFAULT_SLOW_SUBSCRIBER_TIMEOUT),
            lag_warning_threshold: self
                .lag_warning_threshold
                .unwrap_or(DEFAULT_LAG_WARNING_THRESHOLD),
        }
    }
}

/// Broadcast channel errors.
#[derive(Debug, thiserror::Error)]
pub enum BroadcastError {
    /// Maximum subscribers reached.
    #[error("maximum subscribers ({0}) reached")]
    MaxSubscribersReached(usize),

    /// Slow subscriber timeout.
    #[error("slow subscriber timeout after {0:?}")]
    SlowSubscriberTimeout(Duration),

    /// No active subscribers.
    #[error("no active subscribers")]
    NoSubscribers,

    /// Subscriber not found.
    #[error("subscriber {0} not found")]
    SubscriberNotFound(usize),

    /// Buffer full (used internally).
    #[error("buffer full")]
    BufferFull,

    /// Channel closed.
    #[error("channel closed")]
    Closed,
}

/// Per-subscriber cursor state.
///
/// Each subscriber has its own read position, allowing independent progress
/// through the shared buffer.
struct SubscriberCursor {
    /// Unique subscriber ID.
    id: usize,
    /// Read position (monotonically increasing).
    read_seq: AtomicU64,
    /// Whether this cursor is active.
    active: AtomicBool,
    /// Subscriber name (for debugging/metrics).
    name: String,
}

impl SubscriberCursor {
    fn new(id: usize, name: String, start_seq: u64) -> Self {
        Self {
            id,
            read_seq: AtomicU64::new(start_seq),
            active: AtomicBool::new(true),
            name,
        }
    }

    #[inline]
    fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    #[inline]
    fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
    }

    #[inline]
    fn read_position(&self) -> u64 {
        self.read_seq.load(Ordering::Acquire)
    }
}

/// Broadcast channel for multi-consumer scenarios.
///
/// Uses a shared ring buffer with per-subscriber cursors for memory efficiency.
/// Slowest consumer determines retention. Supports dynamic subscribe/unsubscribe.
///
/// # Type Parameters
///
/// * `T` - The event type. Must be `Clone` for subscribers (typically
///   `Arc<RecordBatch>` where clone is an O(1) atomic increment).
///
/// # Performance Targets
///
/// | Operation | Target |
/// |-----------|--------|
/// | `broadcast()` (2 subs) | < 100ns |
/// | `broadcast()` (4 subs) | < 150ns |
/// | `read()` | < 50ns |
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::streaming::broadcast::{BroadcastChannel, BroadcastConfig};
///
/// let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());
///
/// // Subscribe
/// let sub1 = channel.subscribe("mv1").unwrap();
/// let sub2 = channel.subscribe("mv2").unwrap();
///
/// // Broadcast
/// channel.broadcast(42).unwrap();
///
/// // Each subscriber receives the value
/// assert_eq!(channel.read(sub1), Some(42));
/// assert_eq!(channel.read(sub2), Some(42));
/// ```
pub struct BroadcastChannel<T> {
    /// Shared ring buffer (pre-allocated slots).
    buffer: Box<[UnsafeCell<Option<T>>]>,

    /// Write sequence (single producer, monotonically increasing).
    /// Cache-padded to prevent false sharing with read cursors.
    write_seq: CachePadded<AtomicU64>,

    /// Per-subscriber cursors (`RwLock` for dynamic management).
    /// Write lock: subscribe/unsubscribe (Ring 2, setup time)
    /// Read lock: find slowest cursor (hot path, fast)
    cursors: RwLock<Vec<SubscriberCursor>>,

    /// Next subscriber ID.
    next_id: AtomicUsize,

    /// Configuration.
    config: BroadcastConfig,

    /// Capacity (power of 2).
    capacity: usize,

    /// Bitmask for modular indexing.
    mask: usize,

    /// Whether the channel is closed.
    closed: AtomicBool,
}

// SAFETY: BroadcastChannel is designed for SPMC (single-producer, multi-consumer):
// - Single writer thread calls broadcast() (enforced by DAG executor)
// - Multiple consumer threads call read() with distinct subscriber IDs
// - All shared state uses atomic operations with appropriate memory ordering
// - UnsafeCell access is guarded by write_seq/cursor synchronization
unsafe impl<T: Send> Send for BroadcastChannel<T> {}
// SAFETY: See above. Consumers access distinct cursor entries.
// Slot reads are protected by the write_seq protocol.
unsafe impl<T: Send> Sync for BroadcastChannel<T> {}

impl<T> BroadcastChannel<T> {
    /// Creates a new broadcast channel with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Channel configuration
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let channel = BroadcastChannel::<i32>::new(BroadcastConfig::with_capacity(256));
    /// ```
    #[must_use]
    pub fn new(config: BroadcastConfig) -> Self {
        let capacity = config.effective_capacity();
        let mask = capacity - 1;

        let buffer: Vec<UnsafeCell<Option<T>>> =
            (0..capacity).map(|_| UnsafeCell::new(None)).collect();

        Self {
            buffer: buffer.into_boxed_slice(),
            write_seq: CachePadded::new(AtomicU64::new(0)),
            cursors: RwLock::new(Vec::with_capacity(config.max_subscribers)),
            next_id: AtomicUsize::new(0),
            config,
            capacity,
            mask,
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the slowest cursor position among active subscribers.
    ///
    /// Returns `u64::MAX` if there are no active subscribers.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn slowest_cursor(&self) -> u64 {
        let cursors = self.cursors.read().unwrap();
        cursors
            .iter()
            .filter(|c| c.is_active())
            .map(SubscriberCursor::read_position)
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Returns the lag (unread messages) for a subscriber.
    ///
    /// Returns 0 if the subscriber is not found or inactive.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn subscriber_lag(&self, subscriber_id: usize) -> u64 {
        let cursors = self.cursors.read().unwrap();
        if let Some(cursor) = cursors.iter().find(|c| c.id == subscriber_id && c.is_active()) {
            let write_pos = self.write_seq.load(Ordering::Acquire);
            let read_pos = cursor.read_position();
            write_pos.saturating_sub(read_pos)
        } else {
            0
        }
    }

    /// Returns the number of active subscribers.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        let cursors = self.cursors.read().unwrap();
        cursors.iter().filter(|c| c.is_active()).count()
    }

    /// Returns true if the subscriber is lagging beyond the warning threshold.
    #[must_use]
    pub fn is_lagging(&self, subscriber_id: usize) -> bool {
        self.subscriber_lag(subscriber_id) >= self.config.lag_warning_threshold
    }

    /// Returns the current write position.
    #[must_use]
    pub fn write_position(&self) -> u64 {
        self.write_seq.load(Ordering::Relaxed)
    }

    /// Returns the buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &BroadcastConfig {
        &self.config
    }

    /// Returns true if the channel is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Closes the channel.
    ///
    /// After closing, `broadcast()` returns `Err(Closed)` and subscribers
    /// can only read remaining buffered data.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Returns subscriber information for debugging.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn subscriber_info(&self, subscriber_id: usize) -> Option<SubscriberInfo> {
        let cursors = self.cursors.read().unwrap();
        cursors.iter().find(|c| c.id == subscriber_id).map(|c| {
            let write_pos = self.write_seq.load(Ordering::Acquire);
            let read_pos = c.read_position();
            SubscriberInfo {
                id: c.id,
                name: c.name.clone(),
                active: c.is_active(),
                read_position: read_pos,
                lag: write_pos.saturating_sub(read_pos),
            }
        })
    }

    /// Lists all active subscribers.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn list_subscribers(&self) -> Vec<SubscriberInfo> {
        let cursors = self.cursors.read().unwrap();
        let write_pos = self.write_seq.load(Ordering::Acquire);
        cursors
            .iter()
            .filter(|c| c.is_active())
            .map(|c| {
                let read_pos = c.read_position();
                SubscriberInfo {
                    id: c.id,
                    name: c.name.clone(),
                    active: true,
                    read_position: read_pos,
                    lag: write_pos.saturating_sub(read_pos),
                }
            })
            .collect()
    }

    /// Calculates slot index from sequence number.
    #[inline]
    fn slot_index(&self, seq: u64) -> usize {
        // Bitmask truncates to capacity range, so u64->usize narrowing is safe.
        #[allow(clippy::cast_possible_truncation)]
        let idx = (seq as usize) & self.mask;
        idx
    }

    /// Unsubscribes a subscriber.
    ///
    /// The subscriber's cursor is deactivated but not removed (to avoid
    /// reordering issues). Subsequent reads with this ID will return `None`.
    ///
    /// # Performance
    ///
    /// Takes a read lock (fast). Cursor is deactivated atomically.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    pub fn unsubscribe(&self, subscriber_id: usize) {
        let cursors = self.cursors.read().unwrap();
        if let Some(cursor) = cursors.iter().find(|c| c.id == subscriber_id) {
            cursor.deactivate();
        }
    }
}

impl<T: Clone> BroadcastChannel<T> {

    /// Broadcasts a value to all subscribers.
    ///
    /// Writes the value into the next available slot. All active subscribers
    /// will be able to read this value via [`read()`](Self::read).
    ///
    /// # Errors
    ///
    /// - [`BroadcastError::NoSubscribers`] if there are no active subscribers
    /// - [`BroadcastError::SlowSubscriberTimeout`] if Block policy times out
    /// - [`BroadcastError::Closed`] if the channel is closed
    ///
    /// # Safety Contract
    ///
    /// Must be called from a single writer thread only. The DAG executor
    /// enforces this by assigning exactly one producer per broadcast channel.
    pub fn broadcast(&self, value: T) -> Result<(), BroadcastError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(BroadcastError::Closed);
        }

        let write_pos = self.write_seq.load(Ordering::Relaxed);
        let slot_idx = self.slot_index(write_pos);

        // Check if we need to wait for slow subscribers
        let min_read = self.slowest_cursor();
        if min_read == u64::MAX {
            return Err(BroadcastError::NoSubscribers);
        }

        // Check if the target slot would overwrite unread data
        if write_pos >= min_read + self.capacity as u64 {
            self.handle_slow_subscriber(write_pos)?;
        }

        // SAFETY: Single writer guarantees exclusive write access to this slot.
        // The slot is available because we've either waited for slow subscribers
        // or applied the configured policy.
        unsafe { *self.buffer[slot_idx].get() = Some(value) };

        // Advance write position (Release makes new data visible to consumers).
        self.write_seq.store(write_pos + 1, Ordering::Release);

        Ok(())
    }

    /// Registers a new subscriber.
    ///
    /// Returns the subscriber ID which can be used with [`read()`](Self::read).
    /// New subscribers start reading from the current write position (they don't
    /// see historical data).
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastError::MaxSubscribersReached`] if the maximum
    /// subscriber limit is reached.
    ///
    /// # Performance
    ///
    /// Takes a write lock. Should only be called during setup, not on hot path.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    pub fn subscribe(&self, name: impl Into<String>) -> Result<usize, BroadcastError> {
        let mut cursors = self.cursors.write().unwrap();

        // Check subscriber limit
        let active_count = cursors.iter().filter(|c| c.is_active()).count();
        if active_count >= self.config.max_subscribers {
            return Err(BroadcastError::MaxSubscribersReached(
                self.config.max_subscribers,
            ));
        }

        // Allocate new subscriber ID
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // New subscribers start at current write position
        let start_seq = self.write_seq.load(Ordering::Acquire);

        cursors.push(SubscriberCursor::new(id, name.into(), start_seq));

        Ok(id)
    }

    /// Reads the next value for a subscriber.
    ///
    /// Returns `Some(value)` if data is available, or `None` if the subscriber
    /// is caught up with the producer or has been unsubscribed.
    ///
    /// # Arguments
    ///
    /// * `subscriber_id` - The subscriber's ID from [`subscribe()`](Self::subscribe)
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    pub fn read(&self, subscriber_id: usize) -> Option<T> {
        let cursors = self.cursors.read().unwrap();
        let cursor = cursors.iter().find(|c| c.id == subscriber_id)?;

        if !cursor.is_active() {
            return None;
        }

        let read_pos = cursor.read_seq.load(Ordering::Relaxed);
        let write_pos = self.write_seq.load(Ordering::Acquire);

        if read_pos >= write_pos {
            return None; // No data available
        }

        let slot_idx = self.slot_index(read_pos);

        // SAFETY: write_pos > read_pos guarantees this slot contains valid data.
        // The Acquire load of write_seq above synchronizes-with the Release store
        // in broadcast(), ensuring the slot value is visible.
        let value = unsafe { (*self.buffer[slot_idx].get()).as_ref()?.clone() };

        // Advance read position
        cursor.read_seq.store(read_pos + 1, Ordering::Release);

        Some(value)
    }

    /// Tries to read without blocking.
    ///
    /// Returns `Ok(Some(value))` if data is available, `Ok(None)` if caught up,
    /// or `Err` if the subscriber is invalid or unsubscribed.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastError::SubscriberNotFound`] if the subscriber ID is invalid
    /// or the subscriber has been unsubscribed.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    pub fn try_read(&self, subscriber_id: usize) -> Result<Option<T>, BroadcastError> {
        let cursors = self.cursors.read().unwrap();
        let cursor = cursors
            .iter()
            .find(|c| c.id == subscriber_id)
            .ok_or(BroadcastError::SubscriberNotFound(subscriber_id))?;

        if !cursor.is_active() {
            return Err(BroadcastError::SubscriberNotFound(subscriber_id));
        }

        drop(cursors); // Release lock before cloning
        Ok(self.read(subscriber_id))
    }

    /// Handles slow subscriber based on policy.
    fn handle_slow_subscriber(&self, target_write: u64) -> Result<(), BroadcastError> {
        match self.config.slow_subscriber_policy {
            SlowSubscriberPolicy::Block => self.wait_for_slowest(target_write),
            SlowSubscriberPolicy::DropSlow => {
                self.drop_slowest_subscriber();
                Ok(())
            }
            SlowSubscriberPolicy::SkipForSlow => {
                // Just overwrite - slow subscribers will skip ahead
                Ok(())
            }
        }
    }

    /// Waits for the slowest subscriber to catch up (Block policy).
    fn wait_for_slowest(&self, target_write: u64) -> Result<(), BroadcastError> {
        let start = std::time::Instant::now();
        let timeout = self.config.slow_subscriber_timeout;

        loop {
            let min_read = self.slowest_cursor();
            if min_read == u64::MAX {
                return Err(BroadcastError::NoSubscribers);
            }

            // Check if we have room
            if target_write < min_read + self.capacity as u64 {
                return Ok(());
            }

            // Check timeout
            if start.elapsed() >= timeout {
                return Err(BroadcastError::SlowSubscriberTimeout(timeout));
            }

            // Yield to allow slow subscriber to make progress
            std::hint::spin_loop();
        }
    }

    /// Drops the slowest subscriber (`DropSlow` policy).
    fn drop_slowest_subscriber(&self) {
        let cursors = self.cursors.read().unwrap();

        // Find the slowest active subscriber
        let slowest = cursors
            .iter()
            .filter(|c| c.is_active())
            .min_by_key(|c| c.read_position());

        if let Some(cursor) = slowest {
            cursor.deactivate();
        }
    }
}

impl<T> std::fmt::Debug for BroadcastChannel<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastChannel")
            .field("capacity", &self.capacity)
            .field("write_position", &self.write_position())
            .field("subscriber_count", &self.subscriber_count())
            .field("slowest_cursor", &self.slowest_cursor())
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// Information about a subscriber.
#[derive(Debug, Clone)]
pub struct SubscriberInfo {
    /// Subscriber ID.
    pub id: usize,
    /// Subscriber name.
    pub name: String,
    /// Whether the subscriber is active.
    pub active: bool,
    /// Current read position.
    pub read_position: u64,
    /// Lag (unread messages).
    pub lag: u64,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // --- Configuration tests ---

    #[test]
    fn test_default_config() {
        let config = BroadcastConfig::default();
        assert_eq!(config.capacity, DEFAULT_BROADCAST_CAPACITY);
        assert_eq!(config.max_subscribers, DEFAULT_MAX_SUBSCRIBERS);
        assert_eq!(config.slow_subscriber_policy, SlowSubscriberPolicy::Block);
        assert_eq!(
            config.slow_subscriber_timeout,
            DEFAULT_SLOW_SUBSCRIBER_TIMEOUT
        );
        assert_eq!(config.lag_warning_threshold, DEFAULT_LAG_WARNING_THRESHOLD);
    }

    #[test]
    fn test_config_with_capacity() {
        let config = BroadcastConfig::with_capacity(256);
        assert_eq!(config.capacity, 256);
        assert_eq!(config.effective_capacity(), 256);
    }

    #[test]
    fn test_config_effective_capacity_rounds_up() {
        let config = BroadcastConfig::with_capacity(100);
        assert_eq!(config.effective_capacity(), 128); // Next power of 2

        let config = BroadcastConfig::with_capacity(1);
        assert_eq!(config.effective_capacity(), 4); // Minimum is 4
    }

    #[test]
    fn test_config_builder() {
        let config = BroadcastConfig::builder()
            .capacity(512)
            .max_subscribers(8)
            .slow_subscriber_policy(SlowSubscriberPolicy::DropSlow)
            .slow_subscriber_timeout(Duration::from_secs(1))
            .lag_warning_threshold(500)
            .build();

        assert_eq!(config.capacity, 512);
        assert_eq!(config.max_subscribers, 8);
        assert_eq!(config.slow_subscriber_policy, SlowSubscriberPolicy::DropSlow);
        assert_eq!(config.slow_subscriber_timeout, Duration::from_secs(1));
        assert_eq!(config.lag_warning_threshold, 500);
    }

    #[test]
    fn test_slow_subscriber_policy_default() {
        let policy: SlowSubscriberPolicy = Default::default();
        assert_eq!(policy, SlowSubscriberPolicy::Block);
    }

    #[test]
    fn test_slow_subscriber_policy_variants() {
        assert_eq!(SlowSubscriberPolicy::Block, SlowSubscriberPolicy::Block);
        assert_ne!(
            SlowSubscriberPolicy::Block,
            SlowSubscriberPolicy::DropSlow
        );
        assert_ne!(
            SlowSubscriberPolicy::Block,
            SlowSubscriberPolicy::SkipForSlow
        );
    }

    // --- BroadcastChannel creation tests ---

    #[test]
    fn test_channel_creation() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());
        assert_eq!(channel.capacity(), DEFAULT_BROADCAST_CAPACITY);
        assert_eq!(channel.subscriber_count(), 0);
        assert_eq!(channel.write_position(), 0);
        assert!(!channel.is_closed());
    }

    #[test]
    fn test_channel_custom_capacity() {
        let config = BroadcastConfig::with_capacity(64);
        let channel = BroadcastChannel::<i32>::new(config);
        assert_eq!(channel.capacity(), 64);
    }

    // --- Subscribe tests ---

    #[test]
    fn test_subscribe() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id1 = channel.subscribe("sub1").unwrap();
        let id2 = channel.subscribe("sub2").unwrap();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(channel.subscriber_count(), 2);
    }

    #[test]
    fn test_subscribe_max_limit() {
        let config = BroadcastConfig::builder().max_subscribers(2).build();
        let channel = BroadcastChannel::<i32>::new(config);

        channel.subscribe("sub1").unwrap();
        channel.subscribe("sub2").unwrap();

        let result = channel.subscribe("sub3");
        assert!(matches!(result, Err(BroadcastError::MaxSubscribersReached(2))));
    }

    #[test]
    fn test_unsubscribe() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id = channel.subscribe("sub1").unwrap();
        assert_eq!(channel.subscriber_count(), 1);

        channel.unsubscribe(id);
        assert_eq!(channel.subscriber_count(), 0);
    }

    #[test]
    fn test_unsubscribe_nonexistent() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());
        channel.unsubscribe(999); // Should not panic
    }

    // --- Broadcast and read tests ---

    #[test]
    fn test_broadcast_no_subscribers() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let result = channel.broadcast(42);
        assert!(matches!(result, Err(BroadcastError::NoSubscribers)));
    }

    #[test]
    fn test_broadcast_and_read_single_subscriber() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id = channel.subscribe("sub1").unwrap();

        channel.broadcast(1).unwrap();
        channel.broadcast(2).unwrap();
        channel.broadcast(3).unwrap();

        assert_eq!(channel.read(id), Some(1));
        assert_eq!(channel.read(id), Some(2));
        assert_eq!(channel.read(id), Some(3));
        assert_eq!(channel.read(id), None); // Caught up
    }

    #[test]
    fn test_broadcast_and_read_multiple_subscribers() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id1 = channel.subscribe("sub1").unwrap();
        let id2 = channel.subscribe("sub2").unwrap();

        channel.broadcast(42).unwrap();

        // Both subscribers should receive the same value
        assert_eq!(channel.read(id1), Some(42));
        assert_eq!(channel.read(id2), Some(42));
    }

    #[test]
    fn test_read_unsubscribed() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id = channel.subscribe("sub1").unwrap();
        channel.broadcast(42).unwrap();

        channel.unsubscribe(id);

        assert_eq!(channel.read(id), None);
    }

    #[test]
    fn test_read_nonexistent_subscriber() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());
        assert_eq!(channel.read(999), None);
    }

    #[test]
    fn test_try_read() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id = channel.subscribe("sub1").unwrap();
        channel.broadcast(42).unwrap();

        assert_eq!(channel.try_read(id).unwrap(), Some(42));
        assert_eq!(channel.try_read(id).unwrap(), None); // Caught up
    }

    #[test]
    fn test_try_read_nonexistent() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());
        let result = channel.try_read(999);
        assert!(matches!(result, Err(BroadcastError::SubscriberNotFound(999))));
    }

    // --- Lag tracking tests ---

    #[test]
    fn test_subscriber_lag() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id = channel.subscribe("sub1").unwrap();

        assert_eq!(channel.subscriber_lag(id), 0);

        channel.broadcast(1).unwrap();
        channel.broadcast(2).unwrap();
        channel.broadcast(3).unwrap();

        assert_eq!(channel.subscriber_lag(id), 3);

        channel.read(id);
        assert_eq!(channel.subscriber_lag(id), 2);
    }

    #[test]
    fn test_slowest_cursor() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        // No subscribers
        assert_eq!(channel.slowest_cursor(), u64::MAX);

        let id1 = channel.subscribe("sub1").unwrap();
        let _id2 = channel.subscribe("sub2").unwrap();

        channel.broadcast(1).unwrap();
        channel.broadcast(2).unwrap();

        // Both at position 0
        assert_eq!(channel.slowest_cursor(), 0);

        // sub1 reads one
        channel.read(id1);
        assert_eq!(channel.slowest_cursor(), 0); // sub2 is still at 0
    }

    #[test]
    fn test_is_lagging() {
        let config = BroadcastConfig::builder()
            .capacity(256)
            .lag_warning_threshold(3)
            .build();
        let channel = BroadcastChannel::<i32>::new(config);

        let id = channel.subscribe("sub1").unwrap();

        assert!(!channel.is_lagging(id));

        channel.broadcast(1).unwrap();
        channel.broadcast(2).unwrap();
        assert!(!channel.is_lagging(id));

        channel.broadcast(3).unwrap();
        assert!(channel.is_lagging(id)); // Now lagging (3 >= threshold 3)
    }

    // --- Slow subscriber policy tests ---

    #[test]
    fn test_skip_for_slow_policy() {
        let config = BroadcastConfig::builder()
            .capacity(4)
            .slow_subscriber_policy(SlowSubscriberPolicy::SkipForSlow)
            .build();
        let channel = BroadcastChannel::<i32>::new(config);

        let _id = channel.subscribe("slow").unwrap();

        // Fill buffer beyond capacity - should not error
        for i in 0..10 {
            channel.broadcast(i).unwrap();
        }

        // Slow subscriber missed some values
        assert_eq!(channel.write_position(), 10);
    }

    #[test]
    fn test_drop_slow_policy() {
        let config = BroadcastConfig::builder()
            .capacity(4)
            .slow_subscriber_policy(SlowSubscriberPolicy::DropSlow)
            .build();
        let channel = BroadcastChannel::<i32>::new(config);

        let id1 = channel.subscribe("slow").unwrap();
        let id2 = channel.subscribe("fast").unwrap();

        // Fill buffer - slow subscriber should be dropped
        for i in 0..10 {
            // Fast subscriber reads immediately
            channel.read(id2);
            channel.broadcast(i).unwrap();
        }

        // Slow subscriber was dropped
        assert!(channel.subscriber_info(id1).map_or(true, |i| !i.active));
    }

    // --- Channel close tests ---

    #[test]
    fn test_channel_close() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());
        let id = channel.subscribe("sub1").unwrap();

        channel.broadcast(42).unwrap();
        channel.close();

        assert!(channel.is_closed());

        // Can still read buffered data
        assert_eq!(channel.read(id), Some(42));

        // Cannot broadcast new data
        let result = channel.broadcast(43);
        assert!(matches!(result, Err(BroadcastError::Closed)));
    }

    // --- Subscriber info tests ---

    #[test]
    fn test_subscriber_info() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        let id = channel.subscribe("test_sub").unwrap();
        channel.broadcast(1).unwrap();
        channel.broadcast(2).unwrap();

        let info = channel.subscriber_info(id).unwrap();
        assert_eq!(info.id, id);
        assert_eq!(info.name, "test_sub");
        assert!(info.active);
        assert_eq!(info.read_position, 0);
        assert_eq!(info.lag, 2);
    }

    #[test]
    fn test_list_subscribers() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::default());

        channel.subscribe("sub1").unwrap();
        channel.subscribe("sub2").unwrap();
        let id3 = channel.subscribe("sub3").unwrap();

        channel.unsubscribe(id3);

        let subscribers = channel.list_subscribers();
        assert_eq!(subscribers.len(), 2);
        assert!(subscribers.iter().any(|s| s.name == "sub1"));
        assert!(subscribers.iter().any(|s| s.name == "sub2"));
    }

    // --- Debug format test ---

    #[test]
    fn test_debug_format() {
        let channel = BroadcastChannel::<i32>::new(BroadcastConfig::with_capacity(16));
        channel.subscribe("sub1").unwrap();

        let debug = format!("{channel:?}");
        assert!(debug.contains("BroadcastChannel"));
        assert!(debug.contains("capacity"));
        assert!(debug.contains("subscriber_count"));
    }

    // --- Error display tests ---

    #[test]
    fn test_error_display() {
        let e1 = BroadcastError::MaxSubscribersReached(10);
        assert!(e1.to_string().contains("maximum subscribers (10)"));

        let e2 = BroadcastError::SlowSubscriberTimeout(Duration::from_secs(5));
        assert!(e2.to_string().contains("slow subscriber timeout"));

        let e3 = BroadcastError::NoSubscribers;
        assert!(e3.to_string().contains("no active subscribers"));

        let e4 = BroadcastError::SubscriberNotFound(42);
        assert!(e4.to_string().contains("subscriber 42 not found"));

        let e5 = BroadcastError::BufferFull;
        assert!(e5.to_string().contains("buffer full"));

        let e6 = BroadcastError::Closed;
        assert!(e6.to_string().contains("channel closed"));
    }

    // --- Concurrent tests ---

    #[test]
    fn test_concurrent_subscribe_read() {
        let channel = Arc::new(BroadcastChannel::<i32>::new(BroadcastConfig::default()));
        let channel_clone = Arc::clone(&channel);

        // Subscribe in main thread
        let id = channel.subscribe("main").unwrap();

        // Broadcast in another thread
        let producer = thread::spawn(move || {
            for i in 0..100 {
                channel_clone.broadcast(i).unwrap();
            }
        });

        // Read in main thread
        let mut received = Vec::new();
        loop {
            if let Some(val) = channel.read(id) {
                received.push(val);
                if received.len() == 100 {
                    break;
                }
            }
            thread::yield_now();
        }

        producer.join().unwrap();

        assert_eq!(received.len(), 100);
        for (i, val) in received.iter().enumerate() {
            assert_eq!(*val, i as i32);
        }
    }

    #[test]
    fn test_multiple_concurrent_readers() {
        let channel = Arc::new(BroadcastChannel::<i32>::new(BroadcastConfig::default()));

        let id1 = channel.subscribe("reader1").unwrap();
        let id2 = channel.subscribe("reader2").unwrap();

        let channel1 = Arc::clone(&channel);
        let channel2 = Arc::clone(&channel);
        let channel_prod = Arc::clone(&channel);

        // Producer
        let producer = thread::spawn(move || {
            for i in 0..50 {
                channel_prod.broadcast(i).unwrap();
            }
        });

        // Reader 1
        let reader1 = thread::spawn(move || {
            let mut received = Vec::new();
            loop {
                if let Some(val) = channel1.read(id1) {
                    received.push(val);
                    if received.len() == 50 {
                        break;
                    }
                }
                thread::yield_now();
            }
            received
        });

        // Reader 2
        let reader2 = thread::spawn(move || {
            let mut received = Vec::new();
            loop {
                if let Some(val) = channel2.read(id2) {
                    received.push(val);
                    if received.len() == 50 {
                        break;
                    }
                }
                thread::yield_now();
            }
            received
        });

        producer.join().unwrap();
        let r1 = reader1.join().unwrap();
        let r2 = reader2.join().unwrap();

        // Both readers should receive all 50 values in order
        assert_eq!(r1.len(), 50);
        assert_eq!(r2.len(), 50);
        assert_eq!(r1, r2);
    }
}
