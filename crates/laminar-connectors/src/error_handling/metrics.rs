//! Per-source error metrics with atomic counters and sliding window.
//!
//! [`ErrorMetrics`] is updated atomically from Ring 1 (sync) and queried
//! from Ring 2 or the observability API. All operations are lock-free.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

/// Per-source error metrics, updated atomically from Ring 1.
///
/// These metrics are exposed via `SourceConnector::metrics()` and the
/// pipeline observability API.
pub struct ErrorMetrics {
    /// Total decode errors since source was opened.
    pub errors_total: AtomicU64,
    /// Errors in the current sliding window (for rate calculation).
    errors_window: AtomicU64,
    /// Timestamp of the window start (epoch millis).
    window_start_ms: AtomicU64,
    /// Timestamp of the last error (epoch millis). 0 = no errors.
    pub last_error_ms: AtomicU64,
    /// Total records skipped (Skip strategy).
    pub skipped_total: AtomicU64,
    /// Total records sent to DLQ.
    pub dlq_total: AtomicU64,
    /// Total records rate-limited (would have gone to DLQ but exceeded rate).
    pub rate_limited_total: AtomicU64,
}

impl ErrorMetrics {
    /// Creates new zeroed metrics.
    #[must_use]
    pub fn new() -> Self {
        Self {
            errors_total: AtomicU64::new(0),
            errors_window: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(0),
            last_error_ms: AtomicU64::new(0),
            skipped_total: AtomicU64::new(0),
            dlq_total: AtomicU64::new(0),
            rate_limited_total: AtomicU64::new(0),
        }
    }

    /// Records an error occurrence, updating total count and sliding window.
    #[allow(clippy::cast_possible_truncation)]
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        // Truncation from u128→u64 is acceptable: epoch millis won't overflow u64
        // until year 584,942,417.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_error_ms.store(now, Ordering::Relaxed);

        // Rolling window: reset every second
        let window = self.window_start_ms.load(Ordering::Relaxed);
        if now.saturating_sub(window) >= 1000 {
            self.errors_window.store(1, Ordering::Relaxed);
            self.window_start_ms.store(now, Ordering::Relaxed);
        } else {
            self.errors_window.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns the approximate error rate (errors in the current window).
    #[must_use]
    pub fn errors_per_second(&self) -> u64 {
        self.errors_window.load(Ordering::Relaxed)
    }

    /// Returns a snapshot of all metrics for observability.
    #[must_use]
    pub fn snapshot(&self) -> ErrorMetricsSnapshot {
        ErrorMetricsSnapshot {
            errors_total: self.errors_total.load(Ordering::Relaxed),
            errors_per_second: self.errors_per_second(),
            last_error_ms: self.last_error_ms.load(Ordering::Relaxed),
            skipped_total: self.skipped_total.load(Ordering::Relaxed),
            dlq_total: self.dlq_total.load(Ordering::Relaxed),
            rate_limited_total: self.rate_limited_total.load(Ordering::Relaxed),
        }
    }
}

impl Default for ErrorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ErrorMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorMetrics")
            .field("errors_total", &self.errors_total.load(Ordering::Relaxed))
            .field("errors_window", &self.errors_window.load(Ordering::Relaxed))
            .field(
                "window_start_ms",
                &self.window_start_ms.load(Ordering::Relaxed),
            )
            .field(
                "last_error_ms",
                &self.last_error_ms.load(Ordering::Relaxed),
            )
            .field(
                "skipped_total",
                &self.skipped_total.load(Ordering::Relaxed),
            )
            .field("dlq_total", &self.dlq_total.load(Ordering::Relaxed))
            .field(
                "rate_limited_total",
                &self.rate_limited_total.load(Ordering::Relaxed),
            )
            .finish()
    }
}

/// Immutable snapshot of error metrics for reporting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorMetricsSnapshot {
    /// Total decode errors since source was opened.
    pub errors_total: u64,
    /// Approximate errors per second (current window).
    pub errors_per_second: u64,
    /// Timestamp of last error (epoch millis), 0 if no errors.
    pub last_error_ms: u64,
    /// Total records skipped.
    pub skipped_total: u64,
    /// Total records sent to DLQ.
    pub dlq_total: u64,
    /// Total records rate-limited.
    pub rate_limited_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_metrics_are_zero() {
        let m = ErrorMetrics::new();
        assert_eq!(m.errors_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.last_error_ms.load(Ordering::Relaxed), 0);
        assert_eq!(m.skipped_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.dlq_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.rate_limited_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors_per_second(), 0);
    }

    #[test]
    fn test_record_error_increments_total() {
        let m = ErrorMetrics::new();
        m.record_error();
        m.record_error();
        m.record_error();
        assert_eq!(m.errors_total.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_record_error_updates_last_error_ms() {
        let m = ErrorMetrics::new();
        assert_eq!(m.last_error_ms.load(Ordering::Relaxed), 0);
        m.record_error();
        assert!(m.last_error_ms.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_errors_per_second_window() {
        let m = ErrorMetrics::new();
        m.record_error();
        m.record_error();
        // Within the same second, window should show 2
        assert!(m.errors_per_second() >= 1);
    }

    #[test]
    fn test_snapshot_correctness() {
        let m = ErrorMetrics::new();
        m.record_error();
        m.skipped_total.fetch_add(5, Ordering::Relaxed);
        m.dlq_total.fetch_add(3, Ordering::Relaxed);
        m.rate_limited_total.fetch_add(2, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(snap.errors_total, 1);
        assert_eq!(snap.skipped_total, 5);
        assert_eq!(snap.dlq_total, 3);
        assert_eq!(snap.rate_limited_total, 2);
        assert!(snap.last_error_ms > 0);
    }

    #[test]
    fn test_debug_format() {
        let m = ErrorMetrics::new();
        let dbg = format!("{m:?}");
        assert!(dbg.contains("ErrorMetrics"));
        assert!(dbg.contains("errors_total"));
    }
}
