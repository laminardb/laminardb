//! Event time, watermarks, and timer management.
mod cast;
mod duration_str;
mod event_time;
mod filter;
mod watermark;

pub use cast::{cast_to_millis_array, CastError};
pub use duration_str::parse_duration_str;
pub use event_time::{EventTimeError, EventTimeExtractor, ExtractionMode, TimestampField};

pub use filter::{filter_batch_by_timestamp, FilterError, ThresholdOp};

pub use watermark::{
    AscendingTimestampsGenerator, BoundedOutOfOrdernessGenerator, PeriodicGenerator,
    ProcessingTimeGenerator, PunctuatedGenerator, SourceProvidedGenerator, WatermarkGenerator,
    WatermarkRestoreError, WatermarkTracker, DEFAULT_MAX_FUTURE_SKEW_MS,
};

use smallvec::SmallVec;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall clock as epoch milliseconds; `0` if unreadable (callers fail open).
#[must_use]
pub fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

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

/// A monotonically-increasing assertion that no events with timestamps
/// earlier than this will arrive. Drives window emission, late-event
/// detection, and cross-operator time alignment.
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

// Threshold at which the timer service logs a warning about accumulated timers.
// This typically indicates a stalled watermark preventing timer firing.
const TIMER_WARN_THRESHOLD: usize = 100_000;

/// Timer service for scheduling and managing timers.
///
/// The timer service maintains a priority queue of timer registrations,
/// ordered by timestamp. Operators can register timers to be fired at
/// specific event times.
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

    /// Creates a new timer service with pre-allocated capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            timers: BinaryHeap::with_capacity(capacity),
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

        if self.timers.len() == TIMER_WARN_THRESHOLD {
            tracing::warn!(
                pending = self.timers.len(),
                "Timer heap reached {} pending timers — watermark may be stalled",
                TIMER_WARN_THRESHOLD,
            );
        }

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
mod tests;
