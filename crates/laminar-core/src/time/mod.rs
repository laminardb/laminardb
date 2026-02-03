//! # Time Module
//!
//! Event time processing, watermarks, and timer management.
//!
//! ## Concepts
//!
//! - **Event Time**: Timestamp when the event actually occurred
//! - **Processing Time**: Timestamp when the event is processed
//! - **Watermark**: Assertion that no events with timestamp < watermark will arrive
//! - **Timer**: Scheduled callback for window triggers or timeouts
//!
//! ## Event Time Extraction
//!
//! Use [`EventTimeExtractor`] to extract timestamps from Arrow `RecordBatch` columns:
//!
//! ```ignore
//! use laminar_core::time::{EventTimeExtractor, TimestampFormat, ExtractionMode};
//!
//! // Extract millisecond timestamps from a column
//! let mut extractor = EventTimeExtractor::from_column("event_time", TimestampFormat::UnixMillis);
//!
//! // Use Max mode for multi-row batches
//! let extractor = extractor.with_mode(ExtractionMode::Max);
//!
//! let timestamp = extractor.extract(&batch)?;
//! ```
//!
//! ## Watermark Generation
//!
//! Use watermark generators to track event-time progress:
//!
//! ```rust
//! use laminar_core::time::{BoundedOutOfOrdernessGenerator, WatermarkGenerator, Watermark};
//!
//! // Allow events to be up to 1 second late
//! let mut generator = BoundedOutOfOrdernessGenerator::new(1000);
//!
//! // Process events
//! let wm = generator.on_event(5000);
//! assert_eq!(wm, Some(Watermark::new(4000))); // 5000 - 1000
//! ```
//!
//! ## Multi-Source Watermark Tracking
//!
//! For operators with multiple inputs, use [`WatermarkTracker`]:
//!
//! ```rust
//! use laminar_core::time::{WatermarkTracker, Watermark};
//!
//! let mut tracker = WatermarkTracker::new(2);
//! tracker.update_source(0, 5000);
//! tracker.update_source(1, 3000);
//!
//! // Combined watermark is the minimum
//! assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));
//! ```
//!
//! ## Per-Partition Watermark Tracking
//!
//! For Kafka sources with multiple partitions, use [`PartitionedWatermarkTracker`]:
//!
//! ```rust
//! use laminar_core::time::{PartitionedWatermarkTracker, PartitionId, Watermark};
//!
//! let mut tracker = PartitionedWatermarkTracker::new();
//!
//! // Register a Kafka source with 4 partitions
//! tracker.register_source(0, 4);
//!
//! // Update individual partitions
//! tracker.update_partition(PartitionId::new(0, 0), 5000);
//! tracker.update_partition(PartitionId::new(0, 1), 3000);
//!
//! // Combined watermark is minimum across active partitions
//! assert_eq!(tracker.current_watermark(), Some(Watermark::new(3000)));
//!
//! // Mark slow partition as idle to allow progress
//! tracker.mark_partition_idle(PartitionId::new(0, 1));
//! assert_eq!(tracker.current_watermark(), Some(Watermark::new(5000)));
//! ```
//!
//! ## Per-Key Watermark Tracking (F065)
//!
//! For multi-tenant workloads or scenarios with significant event-time skew between
//! keys, use [`KeyedWatermarkTracker`] to achieve 99%+ accuracy vs 63-67% with global:
//!
//! ```rust
//! use laminar_core::time::{KeyedWatermarkTracker, KeyedWatermarkConfig, Watermark};
//! use std::time::Duration;
//!
//! let config = KeyedWatermarkConfig::with_bounded_delay(Duration::from_secs(5));
//! let mut tracker: KeyedWatermarkTracker<String> = KeyedWatermarkTracker::new(config);
//!
//! // Fast tenant advances quickly
//! tracker.update("tenant_a".to_string(), 15_000);
//!
//! // Slow tenant at earlier time
//! tracker.update("tenant_b".to_string(), 5_000);
//!
//! // Per-key watermarks differ - each key has independent tracking
//! assert_eq!(tracker.watermark_for_key(&"tenant_a".to_string()), Some(10_000));
//! assert_eq!(tracker.watermark_for_key(&"tenant_b".to_string()), Some(0));
//!
//! // Events for tenant_b at 3000 are NOT late (their key watermark is 0)
//! assert!(!tracker.is_late(&"tenant_b".to_string(), 3000));
//!
//! // But events for tenant_a at 3000 ARE late (their key watermark is 10000)
//! assert!(tracker.is_late(&"tenant_a".to_string(), 3000));
//! ```
//!
//! ## Watermark Alignment Groups (F066)
//!
//! For stream-stream joins and multi-source operators, use [`WatermarkAlignmentGroup`]
//! to prevent unbounded state growth when sources have different processing speeds:
//!
//! ```rust
//! use laminar_core::time::{
//!     WatermarkAlignmentGroup, AlignmentGroupConfig, AlignmentGroupId,
//!     EnforcementMode, AlignmentAction,
//! };
//! use std::time::Duration;
//!
//! let config = AlignmentGroupConfig::new("orders-payments")
//!     .with_max_drift(Duration::from_secs(300)); // 5 minute max drift
//!
//! let mut group = WatermarkAlignmentGroup::new(config);
//! group.register_source(0); // orders
//! group.register_source(1); // payments
//!
//! // Both start at 0
//! group.report_watermark(0, 0);
//! group.report_watermark(1, 0);
//!
//! // Orders advances within limit - OK
//! let action = group.report_watermark(0, 200_000); // 200 seconds
//! assert_eq!(action, AlignmentAction::Continue);
//!
//! // Orders advances beyond limit - PAUSED
//! let action = group.report_watermark(0, 400_000); // 400 seconds (drift > 300)
//! assert_eq!(action, AlignmentAction::Pause);
//! ```

mod alignment_group;
mod event_time;
mod keyed_watermark;
mod partitioned_watermark;
mod watermark;

pub use alignment_group::{
    AlignmentAction, AlignmentError, AlignmentGroupConfig, AlignmentGroupCoordinator,
    AlignmentGroupId, AlignmentGroupMetrics, AlignmentSourceState, EnforcementMode,
    WatermarkAlignmentGroup,
};

pub use event_time::{
    EventTimeError, EventTimeExtractor, ExtractionMode, TimestampField, TimestampFormat,
};

pub use keyed_watermark::{
    KeyEvictionPolicy, KeyWatermarkState, KeyedWatermarkConfig, KeyedWatermarkError,
    KeyedWatermarkMetrics, KeyedWatermarkTracker, KeyedWatermarkTrackerWithLateHandling,
};

pub use partitioned_watermark::{
    CoreWatermarkState, GlobalWatermarkCollector, PartitionId, PartitionWatermarkState,
    PartitionedWatermarkMetrics, PartitionedWatermarkTracker, WatermarkError,
};

pub use watermark::{
    AscendingTimestampsGenerator, BoundedOutOfOrdernessGenerator, MeteredGenerator,
    PeriodicGenerator, PunctuatedGenerator, SourceProvidedGenerator, WatermarkGenerator,
    WatermarkMetrics, WatermarkTracker,
};

use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Timer key type optimized for window IDs (16 bytes).
///
/// Uses `SmallVec` to avoid heap allocation for keys up to 16 bytes,
/// which covers the common case of `WindowId` keys.
pub type TimerKey = SmallVec<[u8; 16]>;

/// Collection type for fired timers.
///
/// Uses `SmallVec` to avoid heap allocation when few timers fire per poll.
/// Size 8 covers most practical cases where timers fire in small batches.
pub type FiredTimersVec = SmallVec<[TimerRegistration; 8]>;

/// A watermark indicating event time progress.
///
/// Watermarks are monotonically increasing assertions that no events with
/// timestamps earlier than the watermark will arrive. They are used to:
///
/// - Trigger window emissions
/// - Detect late events
/// - Coordinate time progress across operators
///
/// # Example
///
/// ```rust
/// use laminar_core::time::Watermark;
///
/// let watermark = Watermark::new(1000);
///
/// // Check if an event is late
/// assert!(watermark.is_late(999));  // Before watermark
/// assert!(!watermark.is_late(1000)); // At watermark
/// assert!(!watermark.is_late(1001)); // After watermark
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Watermark(pub i64);

impl Watermark {
    /// Creates a new watermark with the given timestamp.
    #[inline]
    #[must_use]
    pub fn new(timestamp: i64) -> Self {
        Self(timestamp)
    }

    /// Returns the watermark timestamp in milliseconds.
    #[inline]
    #[must_use]
    pub fn timestamp(&self) -> i64 {
        self.0
    }

    /// Checks if an event is late relative to this watermark.
    ///
    /// An event is considered late if its timestamp is strictly less than
    /// the watermark timestamp.
    #[inline]
    #[must_use]
    pub fn is_late(&self, event_time: i64) -> bool {
        event_time < self.0
    }

    /// Returns the minimum (earlier) of two watermarks.
    #[must_use]
    pub fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }

    /// Returns the maximum (later) of two watermarks.
    #[must_use]
    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self(i64::MIN)
    }
}

impl From<i64> for Watermark {
    fn from(timestamp: i64) -> Self {
        Self(timestamp)
    }
}

impl From<Watermark> for i64 {
    fn from(watermark: Watermark) -> Self {
        watermark.0
    }
}

/// A timer registration for delayed processing.
///
/// Timers are used by operators to schedule future actions, typically for
/// window triggering or timeouts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerRegistration {
    /// Unique timer ID
    pub id: u64,
    /// Scheduled timestamp (event time, in milliseconds)
    pub timestamp: i64,
    /// Timer key (for keyed operators).
    /// Uses `TimerKey` (`SmallVec`) to avoid heap allocation for keys up to 16 bytes.
    pub key: Option<TimerKey>,
    /// The index of the operator that registered this timer
    pub operator_index: Option<usize>,
}

impl Ord for TimerRegistration {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior (earliest first)
        match other.timestamp.cmp(&self.timestamp) {
            Ordering::Equal => other.id.cmp(&self.id),
            ord => ord,
        }
    }
}

impl PartialOrd for TimerRegistration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Timer service for scheduling and managing timers.
///
/// The timer service maintains a priority queue of timer registrations,
/// ordered by timestamp. Operators can register timers to be fired at
/// specific event times.
///
/// # Example
///
/// ```rust
/// use laminar_core::time::{TimerService, TimerKey};
///
/// let mut service = TimerService::new();
///
/// // Register timers at different times
/// let id1 = service.register_timer(100, None, None);
/// let id2 = service.register_timer(50, Some(TimerKey::from_slice(&[1, 2, 3])), None);
///
/// // Poll for timers that should fire at time 75
/// let fired = service.poll_timers(75);
/// assert_eq!(fired.len(), 1);
/// assert_eq!(fired[0].id, id2); // Timer at t=50 fires first
/// ```
pub struct TimerService {
    timers: BinaryHeap<TimerRegistration>,
    next_timer_id: u64,
}

impl TimerService {
    /// Creates a new timer service.
    #[must_use]
    pub fn new() -> Self {
        Self {
            timers: BinaryHeap::new(),
            next_timer_id: 0,
        }
    }

    /// Registers a new timer.
    ///
    /// Returns the unique timer ID that can be used to cancel the timer.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The event time at which the timer should fire
    /// * `key` - Optional key for keyed operators
    /// * `operator_index` - Optional index of the operator registering the timer(must match the index in the reactor)
    pub fn register_timer(
        &mut self,
        timestamp: i64,
        key: Option<TimerKey>,
        operator_index: Option<usize>,
    ) -> u64 {
        let id = self.next_timer_id;
        self.next_timer_id += 1;

        self.timers.push(TimerRegistration {
            id,
            timestamp,
            key,
            operator_index,
        });

        id
    }

    /// Polls for timers that should fire at or before the given timestamp.
    ///
    /// Returns all timers with timestamps <= `current_time`, in order.
    /// Uses `FiredTimersVec` (`SmallVec`) to avoid heap allocation when few timers fire.
    ///
    /// # Panics
    ///
    /// This function should not panic under normal circumstances. The internal
    /// `expect` is only called after verifying the heap is not empty via `peek`.
    #[inline]
    pub fn poll_timers(&mut self, current_time: i64) -> FiredTimersVec {
        let mut fired = FiredTimersVec::new();

        while let Some(timer) = self.timers.peek() {
            if timer.timestamp <= current_time {
                // SAFETY: We just peeked and confirmed the heap is not empty
                fired.push(self.timers.pop().expect("heap should not be empty"));
            } else {
                break;
            }
        }

        fired
    }

    /// Cancels a timer by ID.
    ///
    /// Returns `true` if the timer was found and cancelled.
    pub fn cancel_timer(&mut self, id: u64) -> bool {
        let count_before = self.timers.len();
        self.timers.retain(|t| t.id != id);
        self.timers.len() < count_before
    }

    /// Returns the number of pending timers.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.timers.len()
    }

    /// Returns the timestamp of the next timer to fire, if any.
    #[must_use]
    pub fn next_timer_timestamp(&self) -> Option<i64> {
        self.timers.peek().map(|t| t.timestamp)
    }

    /// Clears all pending timers.
    pub fn clear(&mut self) {
        self.timers.clear();
    }
}

impl Default for TimerService {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur in time operations.
#[derive(Debug, thiserror::Error)]
pub enum TimeError {
    /// Invalid timestamp value
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),

    /// Timer not found
    #[error("Timer not found: {0}")]
    TimerNotFound(u64),

    /// Watermark regression (going backwards)
    #[error("Watermark regression: current={current}, new={new}")]
    WatermarkRegression {
        /// Current watermark value
        current: i64,
        /// Attempted new watermark value
        new: i64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_creation() {
        let watermark = Watermark::new(1000);
        assert_eq!(watermark.timestamp(), 1000);
    }

    #[test]
    fn test_watermark_late_detection() {
        let watermark = Watermark::new(1000);
        assert!(watermark.is_late(999));
        assert!(!watermark.is_late(1000));
        assert!(!watermark.is_late(1001));
    }

    #[test]
    fn test_watermark_min_max() {
        let w1 = Watermark::new(1000);
        let w2 = Watermark::new(2000);

        assert_eq!(w1.min(w2), Watermark::new(1000));
        assert_eq!(w1.max(w2), Watermark::new(2000));
    }

    #[test]
    fn test_watermark_ordering() {
        let w1 = Watermark::new(1000);
        let w2 = Watermark::new(2000);

        assert!(w1 < w2);
        assert!(w2 > w1);
        assert_eq!(w1, Watermark::new(1000));
    }

    #[test]
    fn test_watermark_conversions() {
        let wm = Watermark::from(1000i64);
        assert_eq!(wm.timestamp(), 1000);

        let ts: i64 = wm.into();
        assert_eq!(ts, 1000);
    }

    #[test]
    fn test_watermark_default() {
        let wm = Watermark::default();
        assert_eq!(wm.timestamp(), i64::MIN);
    }

    #[test]
    fn test_timer_service_creation() {
        let service = TimerService::new();
        assert_eq!(service.pending_count(), 0);
        assert_eq!(service.next_timer_timestamp(), None);
    }

    #[test]
    fn test_timer_registration() {
        let mut service = TimerService::new();

        let id1 = service.register_timer(100, None, None);
        let id2 = service.register_timer(50, Some(TimerKey::from_slice(&[1, 2, 3])), Some(1));

        assert_eq!(service.pending_count(), 2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_timer_poll_order() {
        let mut service = TimerService::new();

        let id1 = service.register_timer(100, None, None);
        let id2 = service.register_timer(50, Some(TimerKey::from_slice(&[1, 2, 3])), Some(0));
        let _id3 = service.register_timer(150, None, None);

        // Poll at time 75 - should get timer at t=50
        let fired = service.poll_timers(75);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].id, id2);
        assert_eq!(fired[0].key, Some(TimerKey::from_slice(&[1, 2, 3])));

        // Poll at time 125 - should get timer at t=100
        let fired = service.poll_timers(125);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].id, id1);

        // Poll at time 200 - should get timer at t=150
        let fired = service.poll_timers(200);
        assert_eq!(fired.len(), 1);

        assert_eq!(service.pending_count(), 0);
    }

    #[test]
    fn test_timer_poll_multiple() {
        let mut service = TimerService::new();

        service.register_timer(50, None, None);
        service.register_timer(75, None, None);
        service.register_timer(100, None, None);

        // Poll at time 80 - should get timers at t=50 and t=75
        let fired = service.poll_timers(80);
        assert_eq!(fired.len(), 2);
        // Should be in timestamp order
        assert_eq!(fired[0].timestamp, 50);
        assert_eq!(fired[1].timestamp, 75);
    }

    #[test]
    fn test_timer_cancel() {
        let mut service = TimerService::new();

        let id1 = service.register_timer(100, None, None);
        let id2 = service.register_timer(200, None, None);

        assert!(service.cancel_timer(id1));
        assert_eq!(service.pending_count(), 1);

        // Should not be able to cancel again
        assert!(!service.cancel_timer(id1));

        // Cancel the remaining timer
        assert!(service.cancel_timer(id2));
        assert_eq!(service.pending_count(), 0);
    }

    #[test]
    fn test_timer_next_timestamp() {
        let mut service = TimerService::new();

        assert_eq!(service.next_timer_timestamp(), None);

        service.register_timer(100, None, None);
        assert_eq!(service.next_timer_timestamp(), Some(100));

        service.register_timer(50, None, None);
        assert_eq!(service.next_timer_timestamp(), Some(50));
    }

    #[test]
    fn test_timer_clear() {
        let mut service = TimerService::new();

        service.register_timer(100, None, None);
        service.register_timer(200, None, None);
        service.register_timer(300, None, None);

        service.clear();
        assert_eq!(service.pending_count(), 0);
        assert_eq!(service.next_timer_timestamp(), None);
    }

    #[test]
    fn test_bounded_watermark_generator() {
        let mut generator = BoundedOutOfOrdernessGenerator::new(100);

        // First event
        let wm1 = generator.on_event(1000);
        assert_eq!(wm1, Some(Watermark::new(900)));

        // Out of order event - no new watermark
        let wm2 = generator.on_event(800);
        assert!(wm2.is_none());

        // New max timestamp
        let wm3 = generator.on_event(1200);
        assert_eq!(wm3, Some(Watermark::new(1100)));
    }

    #[test]
    fn test_ascending_watermark_generator() {
        let mut generator = AscendingTimestampsGenerator::new();

        let wm1 = generator.on_event(1000);
        assert_eq!(wm1, Some(Watermark::new(1000)));

        let wm2 = generator.on_event(2000);
        assert_eq!(wm2, Some(Watermark::new(2000)));

        // Out of order - no watermark
        let wm3 = generator.on_event(1500);
        assert_eq!(wm3, None);
    }

    #[test]
    fn test_watermark_tracker_basic() {
        let mut tracker = WatermarkTracker::new(2);

        tracker.update_source(0, 1000);
        let wm = tracker.update_source(1, 500);

        assert_eq!(wm, Some(Watermark::new(500)));
    }

    #[test]
    fn test_watermark_tracker_idle() {
        let mut tracker = WatermarkTracker::new(2);

        tracker.update_source(0, 5000);
        tracker.update_source(1, 1000);

        // Mark slow source as idle
        let wm = tracker.mark_idle(1);
        assert_eq!(wm, Some(Watermark::new(5000)));

        assert!(tracker.is_idle(1));
        assert!(!tracker.is_idle(0));
    }
}
