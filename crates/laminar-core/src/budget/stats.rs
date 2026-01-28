//! Budget metrics tracking with lock-free counters.
//!
//! Metrics intentionally use f64 for rate calculations, accepting precision loss
//! for very large counts (>2^52). This is acceptable for monitoring purposes.

#![allow(clippy::cast_precision_loss)] // Metrics don't need full u64 precision

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// Global budget metrics instance.
static GLOBAL_METRICS: OnceLock<BudgetMetrics> = OnceLock::new();

/// Aggregated metrics for task budget tracking.
///
/// Uses atomic counters for lock-free updates from multiple threads.
/// Metrics are aggregated globally and can be queried via `snapshot()`.
///
/// # Thread Safety
///
/// All operations are lock-free and safe to call from any thread.
/// The metrics are eventually consistent - reads may not reflect
/// the most recent writes from other threads.
#[derive(Debug)]
pub struct BudgetMetrics {
    // Global counters
    total_tasks: AtomicU64,
    total_violations: AtomicU64,
    total_exceeded_ns: AtomicU64,
    total_duration_ns: AtomicU64,

    // Ring 0 specific
    ring0_tasks: AtomicU64,
    ring0_violations: AtomicU64,
    ring0_exceeded_ns: AtomicU64,

    // Ring 1 specific
    ring1_tasks: AtomicU64,
    ring1_violations: AtomicU64,
    ring1_exceeded_ns: AtomicU64,

    // Yield counters
    yields_budget: AtomicU64,
    yields_priority: AtomicU64,
    yields_empty: AtomicU64,
    yields_other: AtomicU64,
}

impl BudgetMetrics {
    /// Create a new metrics instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            total_tasks: AtomicU64::new(0),
            total_violations: AtomicU64::new(0),
            total_exceeded_ns: AtomicU64::new(0),
            total_duration_ns: AtomicU64::new(0),
            ring0_tasks: AtomicU64::new(0),
            ring0_violations: AtomicU64::new(0),
            ring0_exceeded_ns: AtomicU64::new(0),
            ring1_tasks: AtomicU64::new(0),
            ring1_violations: AtomicU64::new(0),
            ring1_exceeded_ns: AtomicU64::new(0),
            yields_budget: AtomicU64::new(0),
            yields_priority: AtomicU64::new(0),
            yields_empty: AtomicU64::new(0),
            yields_other: AtomicU64::new(0),
        }
    }

    /// Get the global metrics instance.
    #[must_use]
    pub fn global() -> &'static Self {
        GLOBAL_METRICS.get_or_init(Self::new)
    }

    /// Record a completed task.
    ///
    /// # Arguments
    ///
    /// * `name` - Task name (for future per-task metrics)
    /// * `ring` - Ring number (0, 1, or 2)
    /// * `budget_ns` - Budget in nanoseconds
    /// * `elapsed_ns` - Actual elapsed time in nanoseconds
    pub fn record_task(&self, _name: &'static str, ring: u8, budget_ns: u64, elapsed_ns: u64) {
        // Update global counters
        self.total_tasks.fetch_add(1, Ordering::Relaxed);
        self.total_duration_ns.fetch_add(elapsed_ns, Ordering::Relaxed);

        // Check for violation
        if elapsed_ns > budget_ns {
            let exceeded_by = elapsed_ns - budget_ns;
            self.total_violations.fetch_add(1, Ordering::Relaxed);
            self.total_exceeded_ns.fetch_add(exceeded_by, Ordering::Relaxed);

            // Update ring-specific counters
            match ring {
                0 => {
                    self.ring0_violations.fetch_add(1, Ordering::Relaxed);
                    self.ring0_exceeded_ns.fetch_add(exceeded_by, Ordering::Relaxed);
                }
                1 => {
                    self.ring1_violations.fetch_add(1, Ordering::Relaxed);
                    self.ring1_exceeded_ns.fetch_add(exceeded_by, Ordering::Relaxed);
                }
                _ => {}
            }
        }

        // Update ring task counts
        match ring {
            0 => {
                self.ring0_tasks.fetch_add(1, Ordering::Relaxed);
            }
            1 => {
                self.ring1_tasks.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a yield event.
    ///
    /// # Arguments
    ///
    /// * `reason` - The yield reason
    pub fn record_yield(&self, reason: &super::YieldReason) {
        use super::YieldReason;

        match reason {
            YieldReason::BudgetExceeded => {
                self.yields_budget.fetch_add(1, Ordering::Relaxed);
            }
            YieldReason::Ring0Priority => {
                self.yields_priority.fetch_add(1, Ordering::Relaxed);
            }
            YieldReason::QueueEmpty => {
                self.yields_empty.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.yields_other.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get a snapshot of current metrics.
    #[must_use]
    pub fn snapshot(&self) -> BudgetMetricsSnapshot {
        BudgetMetricsSnapshot {
            total_tasks: self.total_tasks.load(Ordering::Relaxed),
            total_violations: self.total_violations.load(Ordering::Relaxed),
            total_exceeded_ns: self.total_exceeded_ns.load(Ordering::Relaxed),
            total_duration_ns: self.total_duration_ns.load(Ordering::Relaxed),
            ring0_tasks: self.ring0_tasks.load(Ordering::Relaxed),
            ring0_violations: self.ring0_violations.load(Ordering::Relaxed),
            ring0_exceeded_ns: self.ring0_exceeded_ns.load(Ordering::Relaxed),
            ring1_tasks: self.ring1_tasks.load(Ordering::Relaxed),
            ring1_violations: self.ring1_violations.load(Ordering::Relaxed),
            ring1_exceeded_ns: self.ring1_exceeded_ns.load(Ordering::Relaxed),
            yields_budget: self.yields_budget.load(Ordering::Relaxed),
            yields_priority: self.yields_priority.load(Ordering::Relaxed),
            yields_empty: self.yields_empty.load(Ordering::Relaxed),
            yields_other: self.yields_other.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics to zero.
    ///
    /// Useful for testing or periodic metric collection.
    pub fn reset(&self) {
        self.total_tasks.store(0, Ordering::Relaxed);
        self.total_violations.store(0, Ordering::Relaxed);
        self.total_exceeded_ns.store(0, Ordering::Relaxed);
        self.total_duration_ns.store(0, Ordering::Relaxed);
        self.ring0_tasks.store(0, Ordering::Relaxed);
        self.ring0_violations.store(0, Ordering::Relaxed);
        self.ring0_exceeded_ns.store(0, Ordering::Relaxed);
        self.ring1_tasks.store(0, Ordering::Relaxed);
        self.ring1_violations.store(0, Ordering::Relaxed);
        self.ring1_exceeded_ns.store(0, Ordering::Relaxed);
        self.yields_budget.store(0, Ordering::Relaxed);
        self.yields_priority.store(0, Ordering::Relaxed);
        self.yields_empty.store(0, Ordering::Relaxed);
        self.yields_other.store(0, Ordering::Relaxed);
    }
}

impl Default for BudgetMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of budget metrics at a point in time.
///
/// Use this for reporting, alerting, or exporting to external systems.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetMetricsSnapshot {
    /// Total number of tasks tracked
    pub total_tasks: u64,
    /// Total number of budget violations
    pub total_violations: u64,
    /// Total nanoseconds exceeded across all violations
    pub total_exceeded_ns: u64,
    /// Total duration of all tasks in nanoseconds
    pub total_duration_ns: u64,

    /// Ring 0 task count
    pub ring0_tasks: u64,
    /// Ring 0 violation count
    pub ring0_violations: u64,
    /// Ring 0 total exceeded nanoseconds
    pub ring0_exceeded_ns: u64,

    /// Ring 1 task count
    pub ring1_tasks: u64,
    /// Ring 1 violation count
    pub ring1_violations: u64,
    /// Ring 1 total exceeded nanoseconds
    pub ring1_exceeded_ns: u64,

    /// Yields due to budget exceeded
    pub yields_budget: u64,
    /// Yields due to Ring 0 priority
    pub yields_priority: u64,
    /// Yields due to empty queue
    pub yields_empty: u64,
    /// Yields due to other reasons
    pub yields_other: u64,
}

impl BudgetMetricsSnapshot {
    /// Calculate violation rate (violations per 1000 tasks).
    #[must_use]
    pub fn violation_rate_per_1000(&self) -> f64 {
        if self.total_tasks == 0 {
            return 0.0;
        }
        (self.total_violations as f64 / self.total_tasks as f64) * 1000.0
    }

    /// Calculate average exceeded time in nanoseconds.
    #[must_use]
    pub fn avg_exceeded_ns(&self) -> u64 {
        if self.total_violations == 0 {
            return 0;
        }
        self.total_exceeded_ns / self.total_violations
    }

    /// Calculate average task duration in nanoseconds.
    #[must_use]
    pub fn avg_duration_ns(&self) -> u64 {
        if self.total_tasks == 0 {
            return 0;
        }
        self.total_duration_ns / self.total_tasks
    }

    /// Calculate Ring 0 violation rate (violations per 1000 tasks).
    #[must_use]
    pub fn ring0_violation_rate_per_1000(&self) -> f64 {
        if self.ring0_tasks == 0 {
            return 0.0;
        }
        (self.ring0_violations as f64 / self.ring0_tasks as f64) * 1000.0
    }

    /// Calculate Ring 1 violation rate (violations per 1000 tasks).
    #[must_use]
    pub fn ring1_violation_rate_per_1000(&self) -> f64 {
        if self.ring1_tasks == 0 {
            return 0.0;
        }
        (self.ring1_violations as f64 / self.ring1_tasks as f64) * 1000.0
    }

    /// Total yield count across all reasons.
    #[must_use]
    pub fn total_yields(&self) -> u64 {
        self.yields_budget + self.yields_priority + self.yields_empty + self.yields_other
    }

    /// Calculate priority yield percentage (of all yields).
    #[must_use]
    pub fn priority_yield_percentage(&self) -> f64 {
        let total = self.total_yields();
        if total == 0 {
            return 0.0;
        }
        (self.yields_priority as f64 / total as f64) * 100.0
    }
}

/// Statistics for a specific task type.
///
/// Used for per-task tracking in the monitor.
#[derive(Debug, Clone, Default)]
pub struct TaskStats {
    /// Number of times this task has run
    pub count: u64,
    /// Number of budget violations
    pub violations: u64,
    /// Total exceeded time in nanoseconds
    pub total_exceeded_ns: u64,
    /// Minimum duration seen
    pub min_duration_ns: u64,
    /// Maximum duration seen
    pub max_duration_ns: u64,
    /// Sum of durations (for average calculation)
    pub total_duration_ns: u64,
}

impl TaskStats {
    /// Create new empty stats.
    #[must_use]
    pub fn new() -> Self {
        Self {
            count: 0,
            violations: 0,
            total_exceeded_ns: 0,
            min_duration_ns: u64::MAX,
            max_duration_ns: 0,
            total_duration_ns: 0,
        }
    }

    /// Record a task execution.
    pub fn record(&mut self, budget_ns: u64, elapsed_ns: u64) {
        self.count += 1;
        self.total_duration_ns += elapsed_ns;

        if elapsed_ns < self.min_duration_ns {
            self.min_duration_ns = elapsed_ns;
        }
        if elapsed_ns > self.max_duration_ns {
            self.max_duration_ns = elapsed_ns;
        }

        if elapsed_ns > budget_ns {
            self.violations += 1;
            self.total_exceeded_ns += elapsed_ns - budget_ns;
        }
    }

    /// Calculate average duration.
    #[must_use]
    pub fn avg_duration_ns(&self) -> u64 {
        if self.count == 0 {
            return 0;
        }
        self.total_duration_ns / self.count
    }

    /// Calculate average exceeded time (only for violations).
    #[must_use]
    pub fn avg_exceeded_ns(&self) -> u64 {
        if self.violations == 0 {
            return 0;
        }
        self.total_exceeded_ns / self.violations
    }

    /// Calculate violation rate as percentage.
    #[must_use]
    pub fn violation_rate_percent(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        (self.violations as f64 / self.count as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = BudgetMetrics::new();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_tasks, 0);
        assert_eq!(snapshot.total_violations, 0);
    }

    #[test]
    fn test_record_task_no_violation() {
        let metrics = BudgetMetrics::new();
        metrics.record_task("test", 0, 1000, 500); // Under budget

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_tasks, 1);
        assert_eq!(snapshot.total_violations, 0);
        assert_eq!(snapshot.total_duration_ns, 500);
        assert_eq!(snapshot.ring0_tasks, 1);
    }

    #[test]
    fn test_record_task_with_violation() {
        let metrics = BudgetMetrics::new();
        metrics.record_task("test", 0, 1000, 1500); // Over budget by 500

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_tasks, 1);
        assert_eq!(snapshot.total_violations, 1);
        assert_eq!(snapshot.total_exceeded_ns, 500);
        assert_eq!(snapshot.ring0_violations, 1);
        assert_eq!(snapshot.ring0_exceeded_ns, 500);
    }

    #[test]
    fn test_record_ring1_task() {
        let metrics = BudgetMetrics::new();
        metrics.record_task("test", 1, 1000, 1500);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.ring1_tasks, 1);
        assert_eq!(snapshot.ring1_violations, 1);
        assert_eq!(snapshot.ring1_exceeded_ns, 500);
    }

    #[test]
    fn test_record_yield() {
        let metrics = BudgetMetrics::new();
        metrics.record_yield(&super::super::YieldReason::BudgetExceeded);
        metrics.record_yield(&super::super::YieldReason::Ring0Priority);
        metrics.record_yield(&super::super::YieldReason::QueueEmpty);
        metrics.record_yield(&super::super::YieldReason::ShutdownRequested);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.yields_budget, 1);
        assert_eq!(snapshot.yields_priority, 1);
        assert_eq!(snapshot.yields_empty, 1);
        assert_eq!(snapshot.yields_other, 1);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = BudgetMetrics::new();
        metrics.record_task("test", 0, 1000, 1500);
        metrics.record_yield(&super::super::YieldReason::BudgetExceeded);

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_tasks, 0);
        assert_eq!(snapshot.total_violations, 0);
        assert_eq!(snapshot.yields_budget, 0);
    }

    #[test]
    fn test_snapshot_calculations() {
        let metrics = BudgetMetrics::new();

        // Record 10 tasks, 2 violations
        for i in 0..10 {
            let elapsed = if i < 2 { 1500 } else { 500 };
            metrics.record_task("test", 0, 1000, elapsed);
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_tasks, 10);
        assert_eq!(snapshot.total_violations, 2);

        // 2 violations per 10 tasks = 200 per 1000
        assert!((snapshot.violation_rate_per_1000() - 200.0).abs() < 0.01);

        // Each violation exceeded by 500ns, average = 500
        assert_eq!(snapshot.avg_exceeded_ns(), 500);
    }

    #[test]
    fn test_task_stats() {
        let mut stats = TaskStats::new();

        stats.record(1000, 500); // Under budget
        stats.record(1000, 1200); // Over by 200
        stats.record(1000, 800); // Under budget

        assert_eq!(stats.count, 3);
        assert_eq!(stats.violations, 1);
        assert_eq!(stats.total_exceeded_ns, 200);
        assert_eq!(stats.min_duration_ns, 500);
        assert_eq!(stats.max_duration_ns, 1200);
        assert_eq!(stats.avg_duration_ns(), (500 + 1200 + 800) / 3);
    }

    #[test]
    fn test_priority_yield_percentage() {
        let metrics = BudgetMetrics::new();
        metrics.record_yield(&super::super::YieldReason::Ring0Priority);
        metrics.record_yield(&super::super::YieldReason::Ring0Priority);
        metrics.record_yield(&super::super::YieldReason::BudgetExceeded);
        metrics.record_yield(&super::super::YieldReason::QueueEmpty);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_yields(), 4);
        // 2 priority out of 4 total = 50%
        assert!((snapshot.priority_yield_percentage() - 50.0).abs() < 0.01);
    }

    #[test]
    fn test_global_metrics() {
        // Access global metrics
        let global = BudgetMetrics::global();

        // Should be the same instance on subsequent calls
        let global2 = BudgetMetrics::global();
        assert!(std::ptr::eq(global, global2));
    }
}
