//! Budget violation monitoring and alerting.
//!
//! Metrics intentionally use f64 for rate calculations, accepting precision loss
//! for very large counts (>2^52). This is acceptable for monitoring purposes.

#![allow(clippy::cast_precision_loss)] // Metrics don't need full u64 precision

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Tracks budget violations over time windows for alerting.
///
/// The monitor aggregates violations per task and checks against
/// configurable thresholds. When a task exceeds its threshold,
/// an alert is generated.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::budget::BudgetMonitor;
/// use std::time::Duration;
///
/// let mut monitor = BudgetMonitor::new(
///     Duration::from_secs(60),  // 1 minute window
///     10.0,                      // Alert if > 10 violations/sec
/// );
///
/// // Record violations
/// monitor.record_violation("ring0_event", 0, 100);
///
/// // Check for alerts
/// let alerts = monitor.check_alerts();
/// for alert in alerts {
///     eprintln!("ALERT: {} has {} violations/sec", alert.task, alert.violation_rate);
/// }
/// ```
#[derive(Debug)]
pub struct BudgetMonitor {
    /// Window size for rate calculation
    window: Duration,
    /// Alert threshold (violations per second)
    threshold: f64,
    /// Per-task violation windows
    violations: HashMap<String, ViolationWindow>,
    /// Last check time
    last_check: Instant,
}

impl BudgetMonitor {
    /// Create a new budget monitor.
    ///
    /// # Arguments
    ///
    /// * `window` - Time window for rate calculation (e.g., 60 seconds)
    /// * `threshold` - Alert threshold in violations per second
    #[must_use]
    pub fn new(window: Duration, threshold: f64) -> Self {
        Self {
            window,
            threshold,
            violations: HashMap::new(),
            last_check: Instant::now(),
        }
    }

    /// Record a budget violation.
    ///
    /// # Arguments
    ///
    /// * `task` - Task name that violated budget
    /// * `ring` - Ring number (0, 1, or 2)
    /// * `exceeded_by_ns` - How much the budget was exceeded in nanoseconds
    pub fn record_violation(&mut self, task: &str, ring: u8, exceeded_by_ns: u64) {
        let entry = self
            .violations
            .entry(task.to_string())
            .or_insert_with(|| ViolationWindow::new(self.window));

        entry.record(ring, exceeded_by_ns);
    }

    /// Check for alerts based on configured threshold.
    ///
    /// Returns a list of alerts for tasks exceeding the threshold.
    /// Call this periodically (e.g., every second).
    pub fn check_alerts(&mut self) -> Vec<BudgetAlert> {
        let now = Instant::now();
        self.last_check = now;

        let mut alerts = Vec::new();

        for (task, window) in &mut self.violations {
            // Prune old violations outside the window
            window.prune(now);

            // Calculate rate based on the window duration
            let window_secs = self.window.as_secs_f64();
            let rate = if window_secs > 0.0 {
                window.count as f64 / window_secs
            } else {
                0.0
            };

            if rate > self.threshold {
                alerts.push(BudgetAlert {
                    task: task.clone(),
                    ring: window.primary_ring,
                    violation_rate: rate,
                    avg_exceeded_by_ns: window.avg_exceeded_ns(),
                    total_violations: window.count,
                    window_duration: self.window,
                });
            }
        }

        alerts
    }

    /// Get current violation count for a task.
    #[must_use]
    pub fn violation_count(&self, task: &str) -> u64 {
        self.violations
            .get(task)
            .map_or(0, |w| w.count)
    }

    /// Get the alert threshold (violations per second).
    #[must_use]
    pub fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Update the alert threshold.
    pub fn set_threshold(&mut self, threshold: f64) {
        self.threshold = threshold;
    }

    /// Get the window duration.
    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Reset all violation tracking.
    pub fn reset(&mut self) {
        self.violations.clear();
        self.last_check = Instant::now();
    }

    /// Get all current violation windows.
    #[must_use]
    pub fn violations(&self) -> &HashMap<String, ViolationWindow> {
        &self.violations
    }
}

/// Tracks violations within a time window.
#[derive(Debug)]
pub struct ViolationWindow {
    /// Violation timestamps (for pruning)
    timestamps: Vec<Instant>,
    /// Total count in current window
    count: u64,
    /// Total exceeded time in nanoseconds
    total_exceeded_ns: u64,
    /// Window duration
    window: Duration,
    /// Primary ring for violations (most common)
    primary_ring: u8,
    /// Ring violation counts
    ring_counts: [u64; 3],
}

impl ViolationWindow {
    /// Create a new violation window.
    fn new(window: Duration) -> Self {
        Self {
            timestamps: Vec::with_capacity(1024),
            count: 0,
            total_exceeded_ns: 0,
            window,
            primary_ring: 0,
            ring_counts: [0; 3],
        }
    }

    /// Record a violation.
    fn record(&mut self, ring: u8, exceeded_by_ns: u64) {
        self.timestamps.push(Instant::now());
        self.count += 1;
        self.total_exceeded_ns += exceeded_by_ns;

        // Track ring counts
        if ring < 3 {
            self.ring_counts[ring as usize] += 1;
            // Update primary ring if this one has more violations
            if self.ring_counts[ring as usize] > self.ring_counts[self.primary_ring as usize] {
                self.primary_ring = ring;
            }
        }
    }

    /// Prune violations outside the window.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn prune(&mut self, now: Instant) {
        // Use checked_sub with fallback to handle edge cases (e.g., system time issues)
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        let before_len = self.timestamps.len();
        self.timestamps.retain(|&t| t > cutoff);
        let pruned = before_len - self.timestamps.len();

        // Adjust count (approximate - we don't track per-violation exceeded time)
        if pruned > 0 && self.count >= pruned as u64 {
            self.count -= pruned as u64;
            // Approximately reduce exceeded time proportionally
            if before_len > 0 {
                let ratio = (before_len - pruned) as f64 / before_len as f64;
                // This is approximate anyway, truncation is acceptable
                self.total_exceeded_ns = (self.total_exceeded_ns as f64 * ratio) as u64;
            }
        }
    }

    /// Calculate violation rate (per second) for a given window duration.
    #[must_use]
    pub fn violation_rate(&self, window_secs: f64) -> f64 {
        if window_secs <= 0.0 {
            return 0.0;
        }
        self.count as f64 / window_secs
    }

    /// Calculate average exceeded time.
    fn avg_exceeded_ns(&self) -> u64 {
        if self.count == 0 {
            return 0;
        }
        self.total_exceeded_ns / self.count
    }

    /// Get violation count.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Get total exceeded nanoseconds.
    #[must_use]
    pub fn total_exceeded_ns(&self) -> u64 {
        self.total_exceeded_ns
    }

    /// Get primary ring (most violations).
    #[must_use]
    pub fn primary_ring(&self) -> u8 {
        self.primary_ring
    }
}

/// Alert generated when a task exceeds violation threshold.
#[derive(Debug, Clone)]
pub struct BudgetAlert {
    /// Task name that triggered the alert
    pub task: String,
    /// Primary ring for violations
    pub ring: u8,
    /// Violation rate (per second)
    pub violation_rate: f64,
    /// Average exceeded time in nanoseconds
    pub avg_exceeded_by_ns: u64,
    /// Total violations in window
    pub total_violations: u64,
    /// Window duration
    pub window_duration: Duration,
}

impl BudgetAlert {
    /// Get a human-readable summary.
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "Task '{}' (Ring {}) exceeds budget: {:.1} violations/sec, avg exceeded by {}ns",
            self.task, self.ring, self.violation_rate, self.avg_exceeded_by_ns
        )
    }

    /// Get severity based on violation rate.
    ///
    /// Returns:
    /// - "critical" if rate > 100/sec
    /// - "high" if rate > 50/sec
    /// - "medium" if rate > 10/sec
    /// - "low" otherwise
    #[must_use]
    pub fn severity(&self) -> &'static str {
        if self.violation_rate > 100.0 {
            "critical"
        } else if self.violation_rate > 50.0 {
            "high"
        } else if self.violation_rate > 10.0 {
            "medium"
        } else {
            "low"
        }
    }

    /// Get ring name.
    #[must_use]
    pub fn ring_name(&self) -> &'static str {
        match self.ring {
            0 => "Ring 0 (Hot Path)",
            1 => "Ring 1 (Background)",
            2 => "Ring 2 (Control Plane)",
            _ => "Unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_monitor_new() {
        let monitor = BudgetMonitor::new(Duration::from_secs(60), 10.0);
        assert_eq!(monitor.threshold(), 10.0);
        assert_eq!(monitor.window(), Duration::from_secs(60));
    }

    #[test]
    fn test_record_violation() {
        let mut monitor = BudgetMonitor::new(Duration::from_secs(60), 10.0);
        monitor.record_violation("test_task", 0, 100);
        monitor.record_violation("test_task", 0, 200);

        assert_eq!(monitor.violation_count("test_task"), 2);
        assert_eq!(monitor.violation_count("other_task"), 0);
    }

    #[test]
    fn test_alert_triggered() {
        let mut monitor = BudgetMonitor::new(Duration::from_secs(1), 5.0);

        // Record many violations quickly
        for _ in 0..20 {
            monitor.record_violation("test_task", 0, 100);
        }

        // Small delay to get non-zero elapsed time
        thread::sleep(Duration::from_millis(10));

        let alerts = monitor.check_alerts();
        assert!(!alerts.is_empty());
        assert_eq!(alerts[0].task, "test_task");
        assert!(alerts[0].violation_rate > 5.0);
    }

    #[test]
    fn test_no_alert_under_threshold() {
        let mut monitor = BudgetMonitor::new(Duration::from_secs(1), 100.0);

        // Record few violations
        for _ in 0..5 {
            monitor.record_violation("test_task", 0, 100);
        }

        thread::sleep(Duration::from_millis(10));

        let alerts = monitor.check_alerts();
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_alert_summary() {
        let alert = BudgetAlert {
            task: "ring0_event".to_string(),
            ring: 0,
            violation_rate: 25.5,
            avg_exceeded_by_ns: 500,
            total_violations: 100,
            window_duration: Duration::from_secs(60),
        };

        let summary = alert.summary();
        assert!(summary.contains("ring0_event"));
        assert!(summary.contains("Ring 0"));
        assert!(summary.contains("25.5"));
    }

    #[test]
    fn test_alert_severity() {
        let mut alert = BudgetAlert {
            task: "test".to_string(),
            ring: 0,
            violation_rate: 5.0,
            avg_exceeded_by_ns: 0,
            total_violations: 0,
            window_duration: Duration::from_secs(1),
        };

        assert_eq!(alert.severity(), "low");

        alert.violation_rate = 25.0;
        assert_eq!(alert.severity(), "medium");

        alert.violation_rate = 75.0;
        assert_eq!(alert.severity(), "high");

        alert.violation_rate = 150.0;
        assert_eq!(alert.severity(), "critical");
    }

    #[test]
    fn test_ring_name() {
        let alert = BudgetAlert {
            task: "test".to_string(),
            ring: 0,
            violation_rate: 0.0,
            avg_exceeded_by_ns: 0,
            total_violations: 0,
            window_duration: Duration::from_secs(1),
        };
        assert_eq!(alert.ring_name(), "Ring 0 (Hot Path)");

        let alert1 = BudgetAlert { ring: 1, ..alert.clone() };
        assert_eq!(alert1.ring_name(), "Ring 1 (Background)");

        let alert2 = BudgetAlert { ring: 2, ..alert };
        assert_eq!(alert2.ring_name(), "Ring 2 (Control Plane)");
    }

    #[test]
    fn test_monitor_reset() {
        let mut monitor = BudgetMonitor::new(Duration::from_secs(60), 10.0);
        monitor.record_violation("test_task", 0, 100);
        assert_eq!(monitor.violation_count("test_task"), 1);

        monitor.reset();
        assert_eq!(monitor.violation_count("test_task"), 0);
    }

    #[test]
    fn test_set_threshold() {
        let mut monitor = BudgetMonitor::new(Duration::from_secs(60), 10.0);
        assert_eq!(monitor.threshold(), 10.0);

        monitor.set_threshold(50.0);
        assert_eq!(monitor.threshold(), 50.0);
    }

    #[test]
    fn test_primary_ring_tracking() {
        let mut monitor = BudgetMonitor::new(Duration::from_secs(60), 10.0);

        // Record more Ring 1 violations
        monitor.record_violation("test", 0, 100);
        monitor.record_violation("test", 1, 100);
        monitor.record_violation("test", 1, 100);
        monitor.record_violation("test", 1, 100);

        let window = monitor.violations().get("test").unwrap();
        assert_eq!(window.primary_ring(), 1);
    }

    #[test]
    fn test_violation_window_accessors() {
        let mut monitor = BudgetMonitor::new(Duration::from_secs(60), 10.0);
        monitor.record_violation("test", 0, 500);
        monitor.record_violation("test", 0, 300);

        let window = monitor.violations().get("test").unwrap();
        assert_eq!(window.count(), 2);
        assert_eq!(window.total_exceeded_ns(), 800);
        assert_eq!(window.primary_ring(), 0);
    }
}
