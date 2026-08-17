//! Sliding (hopping) window assigner.

use super::window::{WindowAssigner, WindowAssignmentError, WindowId, WindowIdVec};
use std::time::Duration;

/// Lazy sliding-window assignments for one timestamp.
#[derive(Debug, Clone)]
pub struct SlidingWindowIter {
    next_start: i64,
    size_ms: i64,
    slide_ms: i64,
    remaining: usize,
}

impl Iterator for SlidingWindowIter {
    type Item = WindowId;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let start = self.next_start;
        self.remaining -= 1;
        if self.remaining != 0 {
            self.next_start += self.slide_ms;
        }
        Some(WindowId::new(start, start + self.size_ms))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for SlidingWindowIter {}

/// Sliding window assigner.
///
/// Each event is assigned to one or more overlapping windows.
/// The maximum number of windows per event is `ceil(size / slide)`.
#[derive(Debug, Clone)]
pub struct SlidingWindowAssigner {
    /// Window size in milliseconds
    size_ms: i64,
    /// Slide interval in milliseconds
    slide_ms: i64,
    /// Maximum windows per event (cached for admission)
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

        let windows_per_event = usize::try_from(1 + (size_ms - 1) / slide_ms)
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
    pub fn from_millis(size_ms: i64, slide_ms: i64) -> Self {
        assert!(size_ms > 0, "Window size must be positive");
        assert!(slide_ms > 0, "Slide interval must be positive");
        assert!(
            slide_ms <= size_ms,
            "Slide must not exceed size (use tumbling windows for non-overlapping)"
        );

        let windows_per_event = usize::try_from(1 + (size_ms - 1) / slide_ms)
            .expect("Windows per event should fit in usize");

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

    /// Returns the maximum number of windows an event can belong to.
    #[must_use]
    pub fn windows_per_event(&self) -> usize {
        self.windows_per_event
    }

    /// Returns the window offset in milliseconds.
    #[must_use]
    pub fn offset_ms(&self) -> i64 {
        self.offset_ms
    }

    /// Iterates over containing windows in ascending start-time order.
    ///
    /// # Errors
    ///
    /// Returns an error when any required boundary is outside the `i64` timestamp range.
    #[inline]
    pub fn try_iter_windows(
        &self,
        timestamp: i64,
    ) -> Result<SlidingWindowIter, WindowAssignmentError> {
        if let Some(adjusted) = timestamp.checked_sub(self.offset_ms) {
            let quotient = adjusted.div_euclid(self.slide_ms);
            if let Some(last_start) = quotient
                .checked_mul(self.slide_ms)
                .and_then(|start| start.checked_add(self.offset_ms))
            {
                let since_last_start = timestamp - last_start;
                let preceding_windows = (self.size_ms - since_last_start - 1) / self.slide_ms;
                let size_remainder = self.size_ms % self.slide_ms;
                let remaining = self.windows_per_event
                    - usize::from(size_remainder != 0 && since_last_start >= size_remainder);
                if let (Some(first_start), Some(_)) = (
                    preceding_windows
                        .checked_mul(self.slide_ms)
                        .and_then(|delta| last_start.checked_sub(delta)),
                    last_start.checked_add(self.size_ms),
                ) {
                    return Ok(SlidingWindowIter {
                        next_start: first_start,
                        size_ms: self.size_ms,
                        slide_ms: self.slide_ms,
                        remaining,
                    });
                }
            }
        }

        self.iter_windows_wide(timestamp)
    }

    #[cold]
    fn iter_windows_wide(
        &self,
        timestamp: i64,
    ) -> Result<SlidingWindowIter, WindowAssignmentError> {
        let timestamp_wide = i128::from(timestamp);
        let size_ms = i128::from(self.size_ms);
        let slide_ms = i128::from(self.slide_ms);
        let offset_ms = i128::from(self.offset_ms);
        let adjusted = timestamp_wide - offset_ms;
        let last_start = adjusted.div_euclid(slide_ms) * slide_ms + offset_ms;
        let since_last_start = timestamp_wide - last_start;
        let preceding_windows = (size_ms - since_last_start - 1) / slide_ms;
        let first_start = last_start - preceding_windows * slide_ms;
        let last_end = last_start + size_ms;
        let size_remainder = size_ms % slide_ms;
        let remaining = self.windows_per_event
            - usize::from(size_remainder != 0 && since_last_start >= size_remainder);

        let first_start = i64::try_from(first_start).map_err(|_| {
            WindowAssignmentError::new(timestamp, first_start, first_start + size_ms)
        })?;
        i64::try_from(last_end)
            .map_err(|_| WindowAssignmentError::new(timestamp, last_start, last_end))?;

        Ok(SlidingWindowIter {
            next_start: first_start,
            size_ms: self.size_ms,
            slide_ms: self.slide_ms,
            remaining,
        })
    }
}

impl WindowAssigner for SlidingWindowAssigner {
    /// Assigns a timestamp to all overlapping windows.
    ///
    /// Returns windows in order from earliest to latest start time.
    #[inline]
    fn assign_windows(&self, timestamp: i64) -> WindowIdVec {
        self.try_iter_windows(timestamp)
            .expect("sliding window boundaries must fit in i64")
            .collect()
    }

    fn max_timestamp(&self, window_end: i64) -> i64 {
        window_end - 1
    }
}

#[cfg(test)]
mod tests;
