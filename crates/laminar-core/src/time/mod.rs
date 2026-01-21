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

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A watermark indicating event time progress
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermark(pub i64);

impl Watermark {
    /// Creates a new watermark
    pub fn new(timestamp: i64) -> Self {
        Self(timestamp)
    }

    /// Gets the watermark timestamp
    pub fn timestamp(&self) -> i64 {
        self.0
    }

    /// Checks if an event is late relative to this watermark
    pub fn is_late(&self, event_time: i64) -> bool {
        event_time < self.0
    }
}

/// Watermark generator that tracks event time progress
pub trait WatermarkGenerator: Send {
    /// Process an event timestamp and potentially emit a new watermark
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark>;

    /// Called periodically to emit watermarks based on wall clock time
    fn on_periodic(&mut self) -> Option<Watermark>;
}

/// Periodic watermark generator with bounded out-of-orderness
pub struct BoundedOutOfOrdernessGenerator {
    max_out_of_orderness: i64,
    current_max_timestamp: i64,
}

impl BoundedOutOfOrdernessGenerator {
    /// Creates a new generator allowing events to be at most `max_out_of_orderness` late
    pub fn new(max_out_of_orderness: i64) -> Self {
        Self {
            max_out_of_orderness,
            current_max_timestamp: i64::MIN,
        }
    }
}

impl WatermarkGenerator for BoundedOutOfOrdernessGenerator {
    fn on_event(&mut self, timestamp: i64) -> Option<Watermark> {
        if timestamp > self.current_max_timestamp {
            self.current_max_timestamp = timestamp;
            let watermark = timestamp.saturating_sub(self.max_out_of_orderness);
            Some(Watermark::new(watermark))
        } else {
            None
        }
    }

    fn on_periodic(&mut self) -> Option<Watermark> {
        // For bounded out-of-orderness, we don't emit periodic watermarks
        None
    }
}

/// A timer registration for delayed processing
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerRegistration {
    /// Timer ID
    pub id: u64,
    /// Scheduled timestamp
    pub timestamp: i64,
    /// Timer key (for keyed operators)
    pub key: Option<Vec<u8>>,
}

impl Ord for TimerRegistration {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior
        other.timestamp.cmp(&self.timestamp)
    }
}

impl PartialOrd for TimerRegistration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Timer service for scheduling callbacks
pub struct TimerService {
    timers: BinaryHeap<TimerRegistration>,
    next_timer_id: u64,
}

impl TimerService {
    /// Creates a new timer service
    pub fn new() -> Self {
        Self {
            timers: BinaryHeap::new(),
            next_timer_id: 0,
        }
    }

    /// Registers a new timer
    pub fn register_timer(&mut self, timestamp: i64, key: Option<Vec<u8>>) -> u64 {
        let id = self.next_timer_id;
        self.next_timer_id += 1;

        self.timers.push(TimerRegistration {
            id,
            timestamp,
            key,
        });

        id
    }

    /// Gets all timers that should fire at or before the given timestamp
    pub fn poll_timers(&mut self, current_time: i64) -> Vec<TimerRegistration> {
        let mut fired = Vec::new();

        while let Some(timer) = self.timers.peek() {
            if timer.timestamp <= current_time {
                fired.push(self.timers.pop().unwrap());
            } else {
                break;
            }
        }

        fired
    }

    /// Cancels a timer by ID
    pub fn cancel_timer(&mut self, id: u64) -> bool {
        let count_before = self.timers.len();
        self.timers.retain(|t| t.id != id);
        self.timers.len() < count_before
    }
}

impl Default for TimerService {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur in time operations
#[derive(Debug, thiserror::Error)]
pub enum TimeError {
    /// Invalid timestamp
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),

    /// Timer not found
    #[error("Timer not found: {0}")]
    TimerNotFound(u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_late_detection() {
        let watermark = Watermark::new(1000);
        assert!(watermark.is_late(999));
        assert!(!watermark.is_late(1000));
        assert!(!watermark.is_late(1001));
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
    fn test_timer_service() {
        let mut service = TimerService::new();

        // Register timers
        let id1 = service.register_timer(100, None);
        let id2 = service.register_timer(50, Some(vec![1, 2, 3]));
        let _id3 = service.register_timer(150, None);

        // Poll at time 75 - should get timer 2
        let fired = service.poll_timers(75);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].id, id2);

        // Poll at time 125 - should get timer 1
        let fired = service.poll_timers(125);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].id, id1);

        // Poll at time 200 - should get timer 3
        let fired = service.poll_timers(200);
        assert_eq!(fired.len(), 1);
    }
}