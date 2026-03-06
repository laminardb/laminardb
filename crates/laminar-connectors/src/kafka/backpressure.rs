//! Backpressure controller for Kafka source consumption.
//!
//! [`KafkaBackpressureController`] monitors downstream channel utilization
//! and signals when the consumer should pause or resume partition
//! consumption to prevent unbounded memory growth.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Controls consumption rate based on downstream channel fill level.
///
/// Uses hysteresis (high/low watermark) to avoid rapid pause/resume
/// oscillation. When the channel fill ratio exceeds the high watermark,
/// consumption pauses. It resumes only when the ratio drops below the
/// low watermark.
///
/// **Callers must wire the `channel_len` `Arc<AtomicUsize>`**: increment
/// when sending messages into the downstream channel, decrement when
/// receiving. Without this wiring, `fill_ratio()` always returns 0.0
/// and backpressure never triggers.
#[derive(Debug)]
pub struct KafkaBackpressureController {
    /// Fill ratio above which to pause (e.g., 0.8 = 80%).
    high_watermark: f64,
    /// Fill ratio below which to resume (e.g., 0.5 = 50%).
    low_watermark: f64,
    /// Maximum channel capacity.
    channel_capacity: usize,
    /// Current number of items in the channel.
    channel_len: Arc<AtomicUsize>,
    /// Whether consumption is currently paused.
    is_paused: bool,
}

impl KafkaBackpressureController {
    /// Creates a new backpressure controller.
    ///
    /// # Arguments
    ///
    /// * `high_watermark` - Fill ratio to trigger pause (0.0-1.0)
    /// * `low_watermark` - Fill ratio to trigger resume (0.0-1.0)
    /// * `channel_capacity` - Maximum channel capacity
    /// * `channel_len` - Shared atomic counter of current channel size
    #[must_use]
    pub fn new(
        high_watermark: f64,
        low_watermark: f64,
        channel_capacity: usize,
        channel_len: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            high_watermark,
            low_watermark,
            channel_capacity,
            channel_len,
            is_paused: false,
        }
    }

    /// Returns `true` if consumption should be paused.
    ///
    /// Triggers when the fill ratio exceeds the high watermark
    /// and the controller is not already paused.
    #[must_use]
    pub fn should_pause(&self) -> bool {
        !self.is_paused && self.fill_ratio() >= self.high_watermark
    }

    /// Returns `true` if consumption should resume.
    ///
    /// Triggers when the fill ratio drops below the low watermark
    /// and the controller is currently paused.
    #[must_use]
    pub fn should_resume(&self) -> bool {
        self.is_paused && self.fill_ratio() <= self.low_watermark
    }

    /// Sets the paused state.
    pub fn set_paused(&mut self, paused: bool) {
        self.is_paused = paused;
    }

    /// Returns whether consumption is currently paused.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    /// Returns the current channel fill ratio (0.0-1.0).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn fill_ratio(&self) -> f64 {
        if self.channel_capacity == 0 {
            return 0.0;
        }
        let len = self.channel_len.load(Ordering::Relaxed);
        len as f64 / self.channel_capacity as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_controller(len: usize, capacity: usize) -> KafkaBackpressureController {
        let channel_len = Arc::new(AtomicUsize::new(len));
        KafkaBackpressureController::new(0.8, 0.5, capacity, channel_len)
    }

    #[test]
    fn test_should_pause_above_high_watermark() {
        let ctrl = make_controller(90, 100);
        assert!(ctrl.should_pause()); // 90% > 80%
        assert!(!ctrl.should_resume());
    }

    #[test]
    fn test_no_pause_below_high_watermark() {
        let ctrl = make_controller(70, 100);
        assert!(!ctrl.should_pause()); // 70% < 80%
    }

    #[test]
    fn test_should_resume_below_low_watermark() {
        let mut ctrl = make_controller(40, 100);
        ctrl.set_paused(true);
        assert!(ctrl.should_resume()); // 40% < 50%
        assert!(!ctrl.should_pause());
    }

    #[test]
    fn test_hysteresis_no_resume_between_watermarks() {
        let mut ctrl = make_controller(60, 100);
        ctrl.set_paused(true);
        assert!(!ctrl.should_resume()); // 60% is between 50% and 80%
        assert!(!ctrl.should_pause()); // already paused
    }

    #[test]
    fn test_fill_ratio() {
        let ctrl = make_controller(50, 100);
        assert!((ctrl.fill_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_fill_ratio_empty_channel() {
        let ctrl = make_controller(0, 0);
        assert!((ctrl.fill_ratio() - 0.0).abs() < f64::EPSILON);
    }
}
