//! # Sliding (Hopping) Window Assigner
//!
//! Fixed-size, overlapping windows with a configurable slide interval.
//!
//! ## Example
//!
//! ```rust
//! use laminar_core::operator::sliding_window::SlidingWindowAssigner;
//! use std::time::Duration;
//!
//! // 10-second windows sliding every 5 seconds
//! let assigner = SlidingWindowAssigner::new(
//!     Duration::from_secs(10),
//!     Duration::from_secs(5),
//! );
//! assert_eq!(assigner.windows_per_event(), 2);
//! ```

use super::window::{WindowAssigner, WindowId, WindowIdVec};
use std::time::Duration;

/// Sliding window assigner.
///
/// Each event is assigned to one or more overlapping windows.
/// The number of windows per event is `ceil(size / slide)`.
#[derive(Debug, Clone)]
pub struct SlidingWindowAssigner {
    /// Window size in milliseconds
    size_ms: i64,
    /// Slide interval in milliseconds
    slide_ms: i64,
    /// Number of windows per event (cached for performance)
    windows_per_event: usize,
    /// Offset in milliseconds for timezone-aligned windows
    offset_ms: i64,
}

impl SlidingWindowAssigner {
    /// Creates a new sliding window assigner.
    ///
    /// # Panics
    ///
    /// Panics if size or slide is zero/negative, or if slide > size.
    #[must_use]
    pub fn new(size: Duration, slide: Duration) -> Self {
        let size_ms = i64::try_from(size.as_millis()).expect("Window size must fit in i64");
        let slide_ms = i64::try_from(slide.as_millis()).expect("Slide interval must fit in i64");

        assert!(size_ms > 0, "Window size must be positive");
        assert!(slide_ms > 0, "Slide interval must be positive");
        assert!(
            slide_ms <= size_ms,
            "Slide must not exceed size (use tumbling windows for non-overlapping)"
        );

        let windows_per_event = usize::try_from((size_ms + slide_ms - 1) / slide_ms)
            .expect("Windows per event should fit in usize");

        Self {
            size_ms,
            slide_ms,
            windows_per_event,
            offset_ms: 0,
        }
    }

    /// Creates a new sliding window assigner with sizes in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if size or slide is zero/negative, or if slide > size.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn from_millis(size_ms: i64, slide_ms: i64) -> Self {
        assert!(size_ms > 0, "Window size must be positive");
        assert!(slide_ms > 0, "Slide interval must be positive");
        assert!(
            slide_ms <= size_ms,
            "Slide must not exceed size (use tumbling windows for non-overlapping)"
        );

        let windows_per_event =
            usize::try_from((size_ms + slide_ms - 1) / slide_ms).unwrap_or(usize::MAX);

        Self {
            size_ms,
            slide_ms,
            windows_per_event,
            offset_ms: 0,
        }
    }

    /// Set window offset in milliseconds.
    #[must_use]
    pub fn with_offset_ms(mut self, offset_ms: i64) -> Self {
        self.offset_ms = offset_ms;
        self
    }

    /// Returns the window size in milliseconds.
    #[must_use]
    pub fn size_ms(&self) -> i64 {
        self.size_ms
    }

    /// Returns the slide interval in milliseconds.
    #[must_use]
    pub fn slide_ms(&self) -> i64 {
        self.slide_ms
    }

    /// Returns the number of windows each event belongs to.
    #[must_use]
    pub fn windows_per_event(&self) -> usize {
        self.windows_per_event
    }

    /// Returns the window offset in milliseconds.
    #[must_use]
    pub fn offset_ms(&self) -> i64 {
        self.offset_ms
    }

    /// Computes the last window start that could contain this timestamp.
    #[inline]
    fn last_window_start(&self, timestamp: i64) -> i64 {
        let adjusted = timestamp - self.offset_ms;
        let base = if adjusted >= 0 {
            (adjusted / self.slide_ms) * self.slide_ms
        } else {
            (adjusted.saturating_sub(self.slide_ms).saturating_add(1) / self.slide_ms)
                * self.slide_ms
        };
        base + self.offset_ms
    }
}

impl WindowAssigner for SlidingWindowAssigner {
    /// Assigns a timestamp to all overlapping windows.
    ///
    /// Returns windows in order from earliest to latest start time.
    #[inline]
    fn assign_windows(&self, timestamp: i64) -> WindowIdVec {
        let mut windows = WindowIdVec::new();

        let last_start = self.last_window_start(timestamp);

        let mut window_start = last_start;
        while window_start + self.size_ms > timestamp {
            let window_end = window_start + self.size_ms;
            windows.push(WindowId::new(window_start, window_end));
            let prev = window_start;
            window_start = window_start.saturating_sub(self.slide_ms);
            if window_start == prev {
                break;
            }
        }

        windows.reverse();
        windows
    }

    fn max_timestamp(&self, window_end: i64) -> i64 {
        window_end - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliding_assigner_basic() {
        let assigner = SlidingWindowAssigner::from_millis(10_000, 5_000);
        let windows = assigner.assign_windows(7_000);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].start, 0);
        assert_eq!(windows[0].end, 10_000);
        assert_eq!(windows[1].start, 5_000);
        assert_eq!(windows[1].end, 15_000);
    }

    #[test]
    fn test_sliding_assigner_windows_per_event() {
        let assigner = SlidingWindowAssigner::from_millis(10_000, 5_000);
        assert_eq!(assigner.windows_per_event(), 2);

        let assigner = SlidingWindowAssigner::from_millis(15_000, 5_000);
        assert_eq!(assigner.windows_per_event(), 3);
    }
}
