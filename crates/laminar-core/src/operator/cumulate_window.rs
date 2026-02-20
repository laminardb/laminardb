//! Cumulate window assigner.
//!
//! CUMULATE windows grow incrementally within a fixed max-size epoch.
//! Each event is assigned to multiple windows: all cumulating windows
//! within its epoch that contain it.
//!
//! For example, with `step=1min` and `size=5min`:
//! - An event at `t=0m30s` belongs to windows `[0,1m)`, `[0,2m)`, `[0,3m)`, `[0,4m)`, `[0,5m)`
//! - An event at `t=1m30s` belongs to windows `[0,2m)`, `[0,3m)`, `[0,4m)`, `[0,5m)`
//! - An event at `t=4m30s` belongs to only `[0,5m)`
//!
//! This is the Flink-style cumulating window semantics.

use std::time::Duration;

use super::window::{WindowAssigner, WindowId, WindowIdVec};

/// Cumulate window assigner.
///
/// Assigns each event to one or more cumulating windows within an epoch.
/// The epoch is aligned to `size`. Within each epoch, windows grow by
/// `step` increments: `[epoch, epoch+step)`, `[epoch, epoch+2*step)`, ...,
/// `[epoch, epoch+size)`.
///
/// # Invariants
///
/// - `step > 0`
/// - `size > 0`
/// - `step <= size`
/// - `size % step == 0`
#[derive(Debug, Clone)]
pub struct CumulateWindowAssigner {
    /// Step size in milliseconds (window growth increment)
    step_ms: i64,
    /// Max window size in milliseconds (epoch size)
    size_ms: i64,
    /// Number of steps per epoch (`size / step`)
    num_steps: i64,
}

impl CumulateWindowAssigner {
    /// Creates a new cumulate window assigner.
    ///
    /// # Panics
    ///
    /// Panics if step or size is zero, step > size, or size is not
    /// evenly divisible by step.
    #[must_use]
    pub fn new(step: Duration, size: Duration) -> Self {
        let step_ms = i64::try_from(step.as_millis()).expect("Step must fit in i64");
        let size_ms = i64::try_from(size.as_millis()).expect("Size must fit in i64");
        assert!(step_ms > 0, "Step must be positive");
        assert!(size_ms > 0, "Size must be positive");
        assert!(step_ms <= size_ms, "Step must not exceed size");
        assert!(
            size_ms % step_ms == 0,
            "Size must be evenly divisible by step"
        );
        Self {
            step_ms,
            size_ms,
            num_steps: size_ms / step_ms,
        }
    }

    /// Creates a new cumulate window assigner with millisecond values.
    ///
    /// # Panics
    ///
    /// Panics if step or size is zero/negative, step > size, or size
    /// is not evenly divisible by step.
    #[must_use]
    pub fn from_millis(step_ms: i64, size_ms: i64) -> Self {
        assert!(step_ms > 0, "Step must be positive");
        assert!(size_ms > 0, "Size must be positive");
        assert!(step_ms <= size_ms, "Step must not exceed size");
        assert!(
            size_ms % step_ms == 0,
            "Size must be evenly divisible by step"
        );
        Self {
            step_ms,
            size_ms,
            num_steps: size_ms / step_ms,
        }
    }

    /// Returns the step size in milliseconds.
    #[must_use]
    pub fn step_ms(&self) -> i64 {
        self.step_ms
    }

    /// Returns the max window size in milliseconds.
    #[must_use]
    pub fn size_ms(&self) -> i64 {
        self.size_ms
    }

    /// Computes the epoch start for a given timestamp.
    #[inline]
    #[must_use]
    pub fn epoch_start(&self, timestamp: i64) -> i64 {
        if timestamp >= 0 {
            (timestamp / self.size_ms) * self.size_ms
        } else {
            ((timestamp - self.size_ms + 1) / self.size_ms) * self.size_ms
        }
    }
}

impl WindowAssigner for CumulateWindowAssigner {
    fn assign_windows(&self, timestamp: i64) -> WindowIdVec {
        let epoch = self.epoch_start(timestamp);
        // Which step within the epoch does this timestamp fall into?
        let offset = timestamp - epoch;
        let step_index = offset / self.step_ms;

        let mut windows = WindowIdVec::new();
        // The event belongs to windows [epoch, epoch+(i+1)*step) for i in step_index..num_steps
        for i in step_index..self.num_steps {
            windows.push(WindowId::new(epoch, epoch + (i + 1) * self.step_ms));
        }
        windows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_cumulate_first_step() {
        // step=1min, size=5min
        let assigner =
            CumulateWindowAssigner::new(Duration::from_secs(60), Duration::from_secs(300));

        // Event at 30s → in first step, should get 5 windows
        let windows = assigner.assign_windows(30_000);
        assert_eq!(windows.len(), 5);
        assert_eq!(windows[0], WindowId::new(0, 60_000));
        assert_eq!(windows[1], WindowId::new(0, 120_000));
        assert_eq!(windows[2], WindowId::new(0, 180_000));
        assert_eq!(windows[3], WindowId::new(0, 240_000));
        assert_eq!(windows[4], WindowId::new(0, 300_000));
    }

    #[test]
    fn test_cumulate_middle_step() {
        let assigner =
            CumulateWindowAssigner::new(Duration::from_secs(60), Duration::from_secs(300));

        // Event at 90s → in second step (index 1), should get 4 windows
        let windows = assigner.assign_windows(90_000);
        assert_eq!(windows.len(), 4);
        assert_eq!(windows[0], WindowId::new(0, 120_000));
        assert_eq!(windows[1], WindowId::new(0, 180_000));
        assert_eq!(windows[2], WindowId::new(0, 240_000));
        assert_eq!(windows[3], WindowId::new(0, 300_000));
    }

    #[test]
    fn test_cumulate_last_step() {
        let assigner =
            CumulateWindowAssigner::new(Duration::from_secs(60), Duration::from_secs(300));

        // Event at 270s → in last step (index 4), should get 1 window
        let windows = assigner.assign_windows(270_000);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], WindowId::new(0, 300_000));
    }

    #[test]
    fn test_cumulate_epoch_boundary() {
        let assigner =
            CumulateWindowAssigner::new(Duration::from_secs(60), Duration::from_secs(300));

        // Event at exactly 300s → starts a new epoch
        let windows = assigner.assign_windows(300_000);
        assert_eq!(windows.len(), 5);
        assert_eq!(windows[0], WindowId::new(300_000, 360_000));
        assert_eq!(windows[4], WindowId::new(300_000, 600_000));
    }

    #[test]
    fn test_cumulate_multiple_epochs() {
        let assigner =
            CumulateWindowAssigner::new(Duration::from_secs(60), Duration::from_secs(300));

        // Event at 620s (epoch 2, offset 20s → step 0)
        let windows = assigner.assign_windows(620_000);
        assert_eq!(windows.len(), 5);
        assert_eq!(windows[0], WindowId::new(600_000, 660_000));
        assert_eq!(windows[4], WindowId::new(600_000, 900_000));
    }

    #[test]
    fn test_cumulate_from_millis() {
        let assigner = CumulateWindowAssigner::from_millis(60_000, 300_000);
        assert_eq!(assigner.step_ms(), 60_000);
        assert_eq!(assigner.size_ms(), 300_000);
    }

    #[test]
    fn test_cumulate_epoch_start() {
        let assigner = CumulateWindowAssigner::from_millis(60_000, 300_000);
        assert_eq!(assigner.epoch_start(0), 0);
        assert_eq!(assigner.epoch_start(299_999), 0);
        assert_eq!(assigner.epoch_start(300_000), 300_000);
    }

    #[test]
    fn test_cumulate_step_equals_size() {
        // Degenerate case: step == size → same as tumbling
        let assigner =
            CumulateWindowAssigner::new(Duration::from_secs(300), Duration::from_secs(300));

        let windows = assigner.assign_windows(30_000);
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0], WindowId::new(0, 300_000));
    }

    #[test]
    #[should_panic(expected = "Step must be positive")]
    fn test_cumulate_zero_step_panics() {
        let _ = CumulateWindowAssigner::from_millis(0, 300_000);
    }

    #[test]
    #[should_panic(expected = "Size must be positive")]
    fn test_cumulate_zero_size_panics() {
        let _ = CumulateWindowAssigner::from_millis(60_000, 0);
    }

    #[test]
    #[should_panic(expected = "Step must not exceed size")]
    fn test_cumulate_step_exceeds_size_panics() {
        let _ = CumulateWindowAssigner::from_millis(600_000, 300_000);
    }

    #[test]
    #[should_panic(expected = "evenly divisible")]
    fn test_cumulate_not_divisible_panics() {
        let _ = CumulateWindowAssigner::from_millis(70_000, 300_000);
    }
}
