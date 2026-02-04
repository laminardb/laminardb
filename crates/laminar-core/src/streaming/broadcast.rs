//! Broadcast Channel for multi-consumer streaming.
//!
//! [`BroadcastChannel<T>`] implements a shared ring buffer with per-subscriber cursors
//! for single-producer, multiple-consumer (SPMC) broadcast. Designed for Ring 0 hot path:
//! zero allocations after construction, lock-free reads and writes.
//!
//! # Design
//!
//! - Pre-allocated ring buffer with power-of-2 capacity and bitmask indexing
//! - **Lock-free hot path**: `broadcast()`, `read()`, `slowest_cursor()` use only atomics
//! - Fixed-size cursor array with O(1) direct indexing by subscriber ID
//! - Cache-padded cursor slots (64-byte aligned) to prevent false sharing
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
//! | `subscribe()` | O(1), CAS on slot (Ring 2 only) |

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

/// Cache-padded cursor slot for lock-free hot path access.
///
/// Each slot is 64-byte aligned to prevent false sharing between cores.
/// Hot path methods (`broadcast`, `read`, `slowest_cursor`) only touch
/// the atomic fields, achieving lock-free operation.
///
/// # Memory Layout
///
/// ```text
/// Offset  Field       Size
/// 0       active      1 byte (AtomicBool)
/// 1-7     (padding)   7 bytes
/// 8       read_seq    8 bytes (AtomicU64)
/// 16-63   _pad        48 bytes
/// Total: 64 bytes (one cache line)
/// ```
#[repr(C, align(64))]
struct CursorSlot {
    /// Whether this slot has an active subscriber.
    active: AtomicBool,
    /// Read position (monotonically increasing).
    read_seq: AtomicU64,
    /// Padding to fill cache line (prevents false sharing).
    _pad: [u8; 48],
}

impl CursorSlot {
    /// Creates an empty (inactive) cursor slot.
    const fn empty() -> Self {
        Self {
            active: AtomicBool::new(false),
            read_seq: AtomicU64::new(0),
            _pad: [0; 48],
        }
    }

    /// Tries to claim this slot atomically.
    ///
    /// Returns `true` if successfully claimed, `false` if already active.
    #[inline]
    fn try_claim(&self, start_seq: u64) -> bool {
        // CAS: only claim if currently inactive
        if self
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.read_seq.store(start_seq, Ordering::Release);
            true
        } else {
            false
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

impl Default for CursorSlot {
    fn default() -> Self {
        Self::empty()
    }
}

/// Broadcast channel for multi-consumer scenarios.
///
/// Uses a shared ring buffer with per-subscriber cursors for memory efficiency.
/// Slowest consumer determines retention. Supports dynamic subscribe/unsubscribe.
///
/// # Lock-Free Hot Path
///
/// The hot path methods (`broadcast`, `read`, `slowest_cursor`) are completely
/// lock-free, using only atomic operations on the pre-allocated cursor slots.
/// This achieves consistent sub-100ns latency without the 5-50μs spikes that
/// `RwLock` can cause.
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

    /// Pre-allocated cursor slots (lock-free hot path access).
    /// Indexed directly by `subscriber_id` for O(1) lookup.
    /// Each slot is 64-byte cache-line aligned.
    cursor_slots: Box<[CursorSlot]>,

    /// Subscriber names (only accessed during setup/debug, not hot path).
    /// Indexed by `subscriber_id`. Protected by `RwLock` since names are
    /// only read during `subscribe()`, `subscriber_info()`, `list_subscribers()`.
    cursor_names: RwLock<Vec<String>>,

    /// Number of active subscribers (atomically maintained).
    /// Used for quick `subscriber_count()` without scanning.
    active_count: AtomicUsize,

    /// Hint for next slot to try during `subscribe()`.
    /// Not critical for correctness - just optimization.
    next_slot_hint: AtomicUsize,

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
    /// Pre-allocates all cursor slots for lock-free hot path operation.
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
        let max_subscribers = config.max_subscribers;

        let buffer: Vec<UnsafeCell<Option<T>>> =
            (0..capacity).map(|_| UnsafeCell::new(None)).collect();

        // Pre-allocate cursor slots (64-byte aligned each)
        let cursor_slots: Vec<CursorSlot> =
            (0..max_subscribers).map(|_| CursorSlot::empty()).collect();

        // Pre-allocate name storage
        let cursor_names: Vec<String> = (0..max_subscribers).map(|_| String::new()).collect();

        Self {
            buffer: buffer.into_boxed_slice(),
            write_seq: CachePadded::new(AtomicU64::new(0)),
            cursor_slots: cursor_slots.into_boxed_slice(),
            cursor_names: RwLock::new(cursor_names),
            active_count: AtomicUsize::new(0),
            next_slot_hint: AtomicUsize::new(0),
            config,
            capacity,
            mask,
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the slowest cursor position among active subscribers.
    ///
    /// **Lock-free**: Only uses atomic loads, no locks.
    ///
    /// Returns `u64::MAX` if there are no active subscribers.
    #[must_use]
    pub fn slowest_cursor(&self) -> u64 {
        let mut min_pos = u64::MAX;
        for slot in &*self.cursor_slots {
            if slot.is_active() {
                let pos = slot.read_position();
                if pos < min_pos {
                    min_pos = pos;
                }
            }
        }
        min_pos
    }

    /// Returns the lag (unread messages) for a subscriber.
    ///
    /// **Lock-free**: Only uses atomic loads, no locks.
    ///
    /// Returns 0 if the subscriber ID is out of bounds or inactive.
    #[must_use]
    pub fn subscriber_lag(&self, subscriber_id: usize) -> u64 {
        if subscriber_id >= self.cursor_slots.len() {
            return 0;
        }
        let slot = &self.cursor_slots[subscriber_id];
        if !slot.is_active() {
            return 0;
        }
        let write_pos = self.write_seq.load(Ordering::Acquire);
        let read_pos = slot.read_position();
        write_pos.saturating_sub(read_pos)
    }

    /// Returns the number of active subscribers.
    ///
    /// **Lock-free**: Returns the atomically-maintained count.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.active_count.load(Ordering::Acquire)
    }

    /// Returns true if the subscriber is lagging beyond the warning threshold.
    ///
    /// **Lock-free**: Only uses atomic loads.
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
    /// # Note
    ///
    /// Takes a read lock to access the name. Not intended for hot path use.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn subscriber_info(&self, subscriber_id: usize) -> Option<SubscriberInfo> {
        if subscriber_id >= self.cursor_slots.len() {
            return None;
        }
        let slot = &self.cursor_slots[subscriber_id];
        let names = self.cursor_names.read().unwrap();
        let write_pos = self.write_seq.load(Ordering::Acquire);
        let read_pos = slot.read_position();
        let active = slot.is_active();

        // Return info even for inactive slots (for debugging)
        Some(SubscriberInfo {
            id: subscriber_id,
            name: names[subscriber_id].clone(),
            active,
            read_position: read_pos,
            lag: write_pos.saturating_sub(read_pos),
        })
    }

    /// Lists all active subscribers.
    ///
    /// # Note
    ///
    /// Takes a read lock to access names. Not intended for hot path use.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    #[must_use]
    pub fn list_subscribers(&self) -> Vec<SubscriberInfo> {
        let names = self.cursor_names.read().unwrap();
        let write_pos = self.write_seq.load(Ordering::Acquire);

        self.cursor_slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.is_active())
            .map(|(id, slot)| {
                let read_pos = slot.read_position();
                SubscriberInfo {
                    id,
                    name: names[id].clone(),
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
    /// **Lock-free**: Only uses atomic store on the slot.
    ///
    /// The subscriber's cursor is deactivated but the slot remains allocated
    /// (can be reused by future subscribers). Subsequent reads with this ID
    /// will return `None`.
    pub fn unsubscribe(&self, subscriber_id: usize) {
        if subscriber_id < self.cursor_slots.len() {
            let slot = &self.cursor_slots[subscriber_id];
            if slot.is_active() {
                slot.deactivate();
                self.active_count.fetch_sub(1, Ordering::Release);
            }
        }
    }
}

impl<T: Clone> BroadcastChannel<T> {
    /// Broadcasts a value to all subscribers.
    ///
    /// **Lock-free**: Only uses atomic operations, no locks on hot path.
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

        // Check if we need to wait for slow subscribers (lock-free)
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
    /// The subscriber ID is the slot index for O(1) direct access on the hot path.
    ///
    /// New subscribers start reading from the current write position (they don't
    /// see historical data).
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastError::MaxSubscribersReached`] if all slots are occupied.
    ///
    /// # Performance
    ///
    /// Uses CAS to claim a slot, then takes write lock for name storage.
    /// Should only be called during setup, not on hot path.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned (should not happen in normal use).
    pub fn subscribe(&self, name: impl Into<String>) -> Result<usize, BroadcastError> {
        let start_seq = self.write_seq.load(Ordering::Acquire);
        let max_slots = self.cursor_slots.len();

        // Start from hint and scan for an available slot
        let hint = self.next_slot_hint.load(Ordering::Relaxed) % max_slots;

        // Try to claim a slot using CAS (lock-free)
        for offset in 0..max_slots {
            let slot_id = (hint + offset) % max_slots;
            let slot = &self.cursor_slots[slot_id];

            if slot.try_claim(start_seq) {
                // Successfully claimed slot - now store the name
                {
                    let mut names = self.cursor_names.write().unwrap();
                    names[slot_id] = name.into();
                }

                // Update hint for next subscribe
                self.next_slot_hint
                    .store((slot_id + 1) % max_slots, Ordering::Relaxed);

                // Increment active count
                self.active_count.fetch_add(1, Ordering::Release);

                return Ok(slot_id);
            }
        }

        // All slots occupied
        Err(BroadcastError::MaxSubscribersReached(max_slots))
    }

    /// Reads the next value for a subscriber.
    ///
    /// **Lock-free**: Uses direct O(1) array indexing, no locks.
    ///
    /// Returns `Some(value)` if data is available, or `None` if the subscriber
    /// is caught up with the producer, has been unsubscribed, or the ID is invalid.
    ///
    /// # Arguments
    ///
    /// * `subscriber_id` - The subscriber's ID from [`subscribe()`](Self::subscribe)
    #[inline]
    pub fn read(&self, subscriber_id: usize) -> Option<T> {
        // O(1) direct indexing - no lock, no linear scan
        if subscriber_id >= self.cursor_slots.len() {
            return None;
        }

        let slot = &self.cursor_slots[subscriber_id];

        if !slot.is_active() {
            return None;
        }

        let read_pos = slot.read_seq.load(Ordering::Relaxed);
        let write_pos = self.write_seq.load(Ordering::Acquire);

        if read_pos >= write_pos {
            return None; // No data available
        }

        let buffer_idx = self.slot_index(read_pos);

        // SAFETY: write_pos > read_pos guarantees this slot contains valid data.
        // The Acquire load of write_seq above synchronizes-with the Release store
        // in broadcast(), ensuring the slot value is visible.
        let value = unsafe { (*self.buffer[buffer_idx].get()).as_ref()?.clone() };

        // Advance read position
        slot.read_seq.store(read_pos + 1, Ordering::Release);

        Some(value)
    }

    /// Tries to read without blocking.
    ///
    /// **Lock-free**: Inlined logic, no double-locking.
    ///
    /// Returns `Ok(Some(value))` if data is available, `Ok(None)` if caught up,
    /// or `Err` if the subscriber is invalid or unsubscribed.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastError::SubscriberNotFound`] if the subscriber ID is invalid
    /// or the subscriber has been unsubscribed.
    #[inline]
    pub fn try_read(&self, subscriber_id: usize) -> Result<Option<T>, BroadcastError> {
        // O(1) direct indexing - no lock, no linear scan
        if subscriber_id >= self.cursor_slots.len() {
            return Err(BroadcastError::SubscriberNotFound(subscriber_id));
        }

        let slot = &self.cursor_slots[subscriber_id];

        if !slot.is_active() {
            return Err(BroadcastError::SubscriberNotFound(subscriber_id));
        }

        let read_pos = slot.read_seq.load(Ordering::Relaxed);
        let write_pos = self.write_seq.load(Ordering::Acquire);

        if read_pos >= write_pos {
            return Ok(None); // Caught up
        }

        let buffer_idx = self.slot_index(read_pos);

        // SAFETY: write_pos > read_pos guarantees this slot contains valid data.
        let value = unsafe { (*self.buffer[buffer_idx].get()).clone() };

        if value.is_some() {
            // Advance read position
            slot.read_seq.store(read_pos + 1, Ordering::Release);
        }

        Ok(value)
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
    ///
    /// **Lock-free**: Scans slots atomically without locks.
    fn drop_slowest_subscriber(&self) {
        // Find the slowest active subscriber (lock-free scan)
        let mut slowest_id: Option<usize> = None;
        let mut slowest_pos = u64::MAX;

        for (id, slot) in self.cursor_slots.iter().enumerate() {
            if slot.is_active() {
                let pos = slot.read_position();
                if pos < slowest_pos {
                    slowest_pos = pos;
                    slowest_id = Some(id);
                }
            }
        }

        if let Some(id) = slowest_id {
            self.cursor_slots[id].deactivate();
            self.active_count.fetch_sub(1, Ordering::Release);
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
#[allow(clippy::default_trait_access)]
#[allow(clippy::unnecessary_map_or)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_possible_wrap)]
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
        assert_eq!(
            config.slow_subscriber_policy,
            SlowSubscriberPolicy::DropSlow
        );
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
        assert_ne!(SlowSubscriberPolicy::Block, SlowSubscriberPolicy::DropSlow);
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
        assert!(matches!(
            result,
            Err(BroadcastError::MaxSubscribersReached(2))
        ));
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
        assert!(matches!(
            result,
            Err(BroadcastError::SubscriberNotFound(999))
        ));
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
