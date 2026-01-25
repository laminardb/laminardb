//! Task budget tracking with automatic metrics on drop.

use std::time::Instant;

use super::stats::BudgetMetrics;

/// Tracks execution time budget for a task.
///
/// Created at task start, automatically records metrics on drop.
/// Use for all Ring 0 operations and Ring 1 chunks.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::budget::TaskBudget;
///
/// fn process_event(event: &Event) {
///     let _budget = TaskBudget::ring0_event();
///     // Process event...
///     // Metrics recorded automatically when _budget is dropped
/// }
/// ```
///
/// # Performance
///
/// The budget tracking overhead is designed to be < 10ns, consisting of:
/// - `Instant::now()` on creation (~10-30ns depending on platform)
/// - Elapsed time calculation on drop (~10-30ns)
/// - Atomic counter increments for metrics (~10ns)
///
/// For Ring 0 hot paths where even this is too much, use `TaskBudget::ring0_event_untracked()`
/// which skips metrics recording.
#[derive(Debug)]
pub struct TaskBudget {
    /// When this task started
    start: Instant,
    /// Budget in nanoseconds
    budget_ns: u64,
    /// Task name for metrics
    name: &'static str,
    /// Ring for this task (0, 1, or 2)
    ring: u8,
    /// Whether to record metrics on drop
    record_metrics: bool,
}

impl TaskBudget {
    // ============================================================
    // Ring 0 budgets (microseconds or less)
    // ============================================================

    /// Single event processing budget: 500ns
    pub const RING0_EVENT_NS: u64 = 500;

    /// Batch of events budget: 5μs (up to ~10 events)
    pub const RING0_BATCH_NS: u64 = 5_000;

    /// State lookup budget: 200ns
    pub const RING0_LOOKUP_NS: u64 = 200;

    /// Window trigger budget: 10μs
    pub const RING0_WINDOW_NS: u64 = 10_000;

    /// Iteration budget: 10μs (for one reactor poll cycle)
    pub const RING0_ITERATION_NS: u64 = 10_000;

    // ============================================================
    // Ring 1 budgets (milliseconds)
    // ============================================================

    /// Background work chunk budget: 1ms
    pub const RING1_CHUNK_NS: u64 = 1_000_000;

    /// Checkpoint preparation budget: 10ms
    pub const RING1_CHECKPOINT_NS: u64 = 10_000_000;

    /// WAL flush budget: 100μs
    pub const RING1_WAL_FLUSH_NS: u64 = 100_000;

    /// Compaction chunk budget: 5ms
    pub const RING1_COMPACTION_NS: u64 = 5_000_000;

    // ============================================================
    // Ring 0 factory methods
    // ============================================================

    /// Create budget for Ring 0 single event.
    ///
    /// Budget: 500ns
    #[inline]
    #[must_use]
    pub fn ring0_event() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_EVENT_NS,
            name: "ring0_event",
            ring: 0,
            record_metrics: true,
        }
    }

    /// Create budget for Ring 0 single event without metrics.
    ///
    /// Use this in extremely hot paths where even atomic counter
    /// updates are too expensive.
    #[inline]
    #[must_use]
    pub fn ring0_event_untracked() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_EVENT_NS,
            name: "ring0_event",
            ring: 0,
            record_metrics: false,
        }
    }

    /// Create budget for Ring 0 batch of events.
    ///
    /// Budget: 5μs
    #[inline]
    #[must_use]
    pub fn ring0_batch() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_BATCH_NS,
            name: "ring0_batch",
            ring: 0,
            record_metrics: true,
        }
    }

    /// Create budget for Ring 0 state lookup.
    ///
    /// Budget: 200ns
    #[inline]
    #[must_use]
    pub fn ring0_lookup() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_LOOKUP_NS,
            name: "ring0_lookup",
            ring: 0,
            record_metrics: true,
        }
    }

    /// Create budget for Ring 0 window trigger.
    ///
    /// Budget: 10μs
    #[inline]
    #[must_use]
    pub fn ring0_window() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_WINDOW_NS,
            name: "ring0_window",
            ring: 0,
            record_metrics: true,
        }
    }

    /// Create budget for Ring 0 iteration (one poll cycle).
    ///
    /// Budget: 10μs
    #[inline]
    #[must_use]
    pub fn ring0_iteration() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING0_ITERATION_NS,
            name: "ring0_iteration",
            ring: 0,
            record_metrics: true,
        }
    }

    // ============================================================
    // Ring 1 factory methods
    // ============================================================

    /// Create budget for Ring 1 background chunk.
    ///
    /// Budget: 1ms
    #[inline]
    #[must_use]
    pub fn ring1_chunk() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING1_CHUNK_NS,
            name: "ring1_chunk",
            ring: 1,
            record_metrics: true,
        }
    }

    /// Create budget for Ring 1 checkpoint preparation.
    ///
    /// Budget: 10ms
    #[inline]
    #[must_use]
    pub fn ring1_checkpoint() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING1_CHECKPOINT_NS,
            name: "ring1_checkpoint",
            ring: 1,
            record_metrics: true,
        }
    }

    /// Create budget for Ring 1 WAL flush.
    ///
    /// Budget: 100μs
    #[inline]
    #[must_use]
    pub fn ring1_wal_flush() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING1_WAL_FLUSH_NS,
            name: "ring1_wal_flush",
            ring: 1,
            record_metrics: true,
        }
    }

    /// Create budget for Ring 1 compaction chunk.
    ///
    /// Budget: 5ms
    #[inline]
    #[must_use]
    pub fn ring1_compaction() -> Self {
        Self {
            start: Instant::now(),
            budget_ns: Self::RING1_COMPACTION_NS,
            name: "ring1_compaction",
            ring: 1,
            record_metrics: true,
        }
    }

    // ============================================================
    // Custom budget
    // ============================================================

    /// Create a custom budget.
    ///
    /// # Arguments
    ///
    /// * `name` - Task name for metrics (must be a static string)
    /// * `ring` - Ring number (0, 1, or 2)
    /// * `budget_ns` - Budget in nanoseconds
    #[inline]
    #[must_use]
    pub fn custom(name: &'static str, ring: u8, budget_ns: u64) -> Self {
        Self {
            start: Instant::now(),
            budget_ns,
            name,
            ring,
            record_metrics: true,
        }
    }

    /// Create a custom budget without metrics recording.
    #[inline]
    #[must_use]
    pub fn custom_untracked(name: &'static str, ring: u8, budget_ns: u64) -> Self {
        Self {
            start: Instant::now(),
            budget_ns,
            name,
            ring,
            record_metrics: false,
        }
    }

    // ============================================================
    // Accessors
    // ============================================================

    /// Get the task name.
    #[inline]
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the ring number.
    #[inline]
    #[must_use]
    pub fn ring(&self) -> u8 {
        self.ring
    }

    /// Get the budget in nanoseconds.
    #[inline]
    #[must_use]
    pub fn budget_ns(&self) -> u64 {
        self.budget_ns
    }

    // ============================================================
    // Budget checking
    // ============================================================

    /// Get elapsed time in nanoseconds.
    ///
    /// Note: Truncation from u128 to u64 is acceptable here because:
    /// - u64 can hold ~584 years of nanoseconds
    /// - Budget tracking is for sub-second operations
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    /// Get remaining budget in nanoseconds (negative if exceeded).
    ///
    /// Returns a signed value where:
    /// - Positive: nanoseconds of budget remaining
    /// - Negative: nanoseconds over budget
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_wrap)]
    pub fn remaining_ns(&self) -> i64 {
        // Note: These casts are safe because:
        // - budget_ns is at most ~10ms = 10_000_000ns, well under i64::MAX
        // - elapsed_ns is measured wall-clock time, practically bounded
        self.budget_ns as i64 - self.elapsed_ns() as i64
    }

    /// Check if budget is exceeded.
    #[inline]
    #[must_use]
    pub fn exceeded(&self) -> bool {
        self.elapsed_ns() > self.budget_ns
    }

    /// Check if budget is almost exceeded (>80% used).
    ///
    /// Useful for early warnings and preemptive yielding.
    #[inline]
    #[must_use]
    pub fn almost_exceeded(&self) -> bool {
        self.elapsed_ns() > (self.budget_ns * 8) / 10
    }

    /// Check if budget is half used (>50%).
    ///
    /// Useful for chunking decisions.
    #[inline]
    #[must_use]
    pub fn half_used(&self) -> bool {
        self.elapsed_ns() > self.budget_ns / 2
    }

    /// Get the percentage of budget used (0-100+).
    ///
    /// Values over 100 indicate the budget was exceeded.
    #[inline]
    #[must_use]
    pub fn percentage_used(&self) -> u64 {
        let elapsed = self.elapsed_ns();
        if self.budget_ns == 0 {
            return 100;
        }
        (elapsed * 100) / self.budget_ns
    }
}

impl Drop for TaskBudget {
    fn drop(&mut self) {
        if self.record_metrics {
            let elapsed = self.elapsed_ns();
            BudgetMetrics::global().record_task(self.name, self.ring, self.budget_ns, elapsed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_elapsed_increases() {
        let budget = TaskBudget::ring1_chunk();
        let t1 = budget.elapsed_ns();
        thread::sleep(Duration::from_micros(100));
        let t2 = budget.elapsed_ns();
        assert!(t2 > t1);
    }

    #[test]
    fn test_remaining_decreases() {
        let budget = TaskBudget::ring1_chunk();
        let r1 = budget.remaining_ns();
        thread::sleep(Duration::from_micros(100));
        let r2 = budget.remaining_ns();
        assert!(r2 < r1);
    }

    #[test]
    fn test_percentage_used() {
        let budget = TaskBudget::custom("test", 0, 100_000); // 100μs

        // Very early, should be low percentage
        let pct = budget.percentage_used();
        assert!(pct < 50, "Early percentage {} should be low", pct);
    }

    #[test]
    fn test_half_used() {
        let budget = TaskBudget::custom("test", 0, 100_000); // 100μs

        // Should not be half used immediately
        assert!(!budget.half_used());

        // Sleep for 60μs (60%)
        thread::sleep(Duration::from_micros(60));

        // Should be half used now
        assert!(budget.half_used());
    }

    #[test]
    fn test_untracked_budget() {
        let budget = TaskBudget::ring0_event_untracked();
        assert!(!budget.record_metrics);

        let budget2 = TaskBudget::custom_untracked("test", 0, 1000);
        assert!(!budget2.record_metrics);
    }

    #[test]
    fn test_ring0_iteration() {
        let budget = TaskBudget::ring0_iteration();
        assert_eq!(budget.name(), "ring0_iteration");
        assert_eq!(budget.budget_ns(), TaskBudget::RING0_ITERATION_NS);
    }

    #[test]
    fn test_ring1_compaction() {
        let budget = TaskBudget::ring1_compaction();
        assert_eq!(budget.name(), "ring1_compaction");
        assert_eq!(budget.budget_ns(), TaskBudget::RING1_COMPACTION_NS);
    }
}
