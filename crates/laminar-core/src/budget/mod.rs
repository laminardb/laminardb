//! # Task Budget Enforcement
//!
//! Implements task budget enforcement to ensure Ring 0 latency guarantees are met.
//! Each operation has a time budget, and exceeding it triggers metrics/alerts.
//! Ring 1 background tasks cooperatively yield when Ring 0 has pending work or
//! when their budget is exhausted.
//!
//! ## Budget Constants
//!
//! | Ring | Operation | Budget | Notes |
//! |------|-----------|--------|-------|
//! | 0 | Single event | 500ns | p99 latency target |
//! | 0 | Event batch | 5μs | Up to 10 events |
//! | 0 | State lookup | 200ns | Fast path |
//! | 0 | Window trigger | 10μs | Aggregation emission |
//! | 1 | Background chunk | 1ms | Cooperative yielding |
//! | 1 | Checkpoint prep | 10ms | Async operation |
//! | 1 | WAL flush | 100μs | Group commit |
//!
//! ## Usage
//!
//! ```rust,ignore
//! use laminar_core::budget::TaskBudget;
//!
//! // Ring 0: Track single event processing
//! fn process_event(event: &Event) {
//!     let _budget = TaskBudget::ring0_event();
//!     // Process event - metrics recorded automatically on drop
//!     process(event);
//! }
//!
//! // Ring 1: Cooperative yielding
//! fn process_background_work(&mut self) -> YieldReason {
//!     let budget = TaskBudget::ring1_chunk();
//!
//!     while !budget.exceeded() {
//!         if self.ring0_has_pending() {
//!             return YieldReason::Ring0Priority;
//!         }
//!         // Process work...
//!     }
//!
//!     YieldReason::BudgetExceeded
//! }
//! ```
//!
//! ## Metrics
//!
//! The module tracks:
//! - Task duration histograms (per task type, per ring)
//! - Budget violation counts
//! - Amount exceeded (for capacity planning)
//!
//! Access metrics via `BudgetMetrics::global()` or per-task via `TaskBudget`.

mod monitor;
mod stats;
mod task_budget;
mod yield_reason;

pub use monitor::{BudgetAlert, BudgetMonitor, ViolationWindow};
pub use stats::{BudgetMetrics, BudgetMetricsSnapshot, TaskStats};
pub use task_budget::TaskBudget;
pub use yield_reason::YieldReason;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_budget_exceeded_detection() {
        let budget = TaskBudget::ring0_event();

        // Simulate work that takes longer than 500ns
        thread::sleep(Duration::from_micros(10));

        assert!(budget.exceeded());
        assert!(budget.remaining_ns() < 0);
    }

    #[test]
    fn test_budget_not_exceeded() {
        let budget = TaskBudget::ring1_chunk(); // 1ms budget

        // Very quick operation
        let _ = 1 + 1;

        assert!(!budget.exceeded());
        assert!(budget.remaining_ns() > 0);
    }

    #[test]
    fn test_budget_almost_exceeded() {
        let budget = TaskBudget::custom("test", 0, 1_000_000); // 1ms

        // Sleep for 900μs (90% of budget)
        thread::sleep(Duration::from_micros(900));

        // Should be almost exceeded (>80% used)
        assert!(budget.almost_exceeded());
        // But not fully exceeded yet
        // Note: timing can be imprecise, so we don't assert !exceeded()
    }

    #[test]
    fn test_yield_reason_display() {
        assert_eq!(format!("{}", YieldReason::BudgetExceeded), "BudgetExceeded");
        assert_eq!(format!("{}", YieldReason::Ring0Priority), "Ring0Priority");
        assert_eq!(format!("{}", YieldReason::QueueEmpty), "QueueEmpty");
        assert_eq!(
            format!("{}", YieldReason::ShutdownRequested),
            "ShutdownRequested"
        );
    }

    #[test]
    fn test_budget_metrics_snapshot() {
        let metrics = BudgetMetrics::new();

        // Record some test data
        metrics.record_task("test_task", 0, 100, 50); // Under budget
        metrics.record_task("test_task", 0, 100, 150); // Over budget by 50

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_tasks, 2);
        assert_eq!(snapshot.total_violations, 1);
        assert_eq!(snapshot.total_exceeded_ns, 50);
    }

    #[test]
    fn test_budget_monitor_alert() {
        // Use a 1-second window with threshold of 10 violations/sec
        let mut monitor = BudgetMonitor::new(Duration::from_secs(1), 10.0);

        // Record 20 violations - that's 20/sec which exceeds the 10/sec threshold
        for _ in 0..20 {
            monitor.record_violation("test_task", 0, 100);
        }

        let alerts = monitor.check_alerts();
        assert!(
            !alerts.is_empty(),
            "Should have alerts for 20 violations in 1 second"
        );
        assert_eq!(alerts[0].task, "test_task");
        // Rate should be 20 violations / 1 second = 20/sec
        assert!(
            alerts[0].violation_rate >= 10.0,
            "Rate {} should be >= 10.0",
            alerts[0].violation_rate
        );
    }

    #[test]
    fn test_custom_budget() {
        let budget = TaskBudget::custom("my_task", 2, 50_000); // 50μs

        assert_eq!(budget.name(), "my_task");
        assert_eq!(budget.ring(), 2);
        assert_eq!(budget.budget_ns(), 50_000);
    }

    #[test]
    fn test_ring0_budgets() {
        let event = TaskBudget::ring0_event();
        assert_eq!(event.budget_ns(), TaskBudget::RING0_EVENT_NS);
        assert_eq!(event.ring(), 0);

        let batch = TaskBudget::ring0_batch();
        assert_eq!(batch.budget_ns(), TaskBudget::RING0_BATCH_NS);

        let lookup = TaskBudget::ring0_lookup();
        assert_eq!(lookup.budget_ns(), TaskBudget::RING0_LOOKUP_NS);

        let window = TaskBudget::ring0_window();
        assert_eq!(window.budget_ns(), TaskBudget::RING0_WINDOW_NS);
    }

    #[test]
    fn test_ring1_budgets() {
        let chunk = TaskBudget::ring1_chunk();
        assert_eq!(chunk.budget_ns(), TaskBudget::RING1_CHUNK_NS);
        assert_eq!(chunk.ring(), 1);

        let checkpoint = TaskBudget::ring1_checkpoint();
        assert_eq!(checkpoint.budget_ns(), TaskBudget::RING1_CHECKPOINT_NS);

        let wal_flush = TaskBudget::ring1_wal_flush();
        assert_eq!(wal_flush.budget_ns(), TaskBudget::RING1_WAL_FLUSH_NS);
    }

    #[test]
    fn test_budget_overhead() {
        // Measure overhead of creating/dropping budgets
        // This should be < 100ns on modern hardware (we aim for < 10ns)
        let iterations = 10_000;

        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let _budget = TaskBudget::ring0_event();
            // Budget dropped here
        }
        let elapsed = start.elapsed();

        let overhead_ns = elapsed.as_nanos() / iterations as u128;
        // Debug builds disable inlining and add extra checks, so the threshold
        // must be relaxed. Release builds should stay under 200ns.
        let threshold = if cfg!(debug_assertions) { 2_000 } else { 200 };
        assert!(
            overhead_ns < threshold,
            "Budget overhead {} ns is too high (target < {}ns)",
            overhead_ns,
            threshold,
        );
    }
}
