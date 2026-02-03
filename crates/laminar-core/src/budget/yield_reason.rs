//! Yield reasons for Ring 1 cooperative scheduling.

use std::fmt;

/// Reason Ring 1 yielded control.
///
/// Ring 1 background tasks cooperatively yield to ensure Ring 0
/// hot path operations are not delayed. This enum captures why
/// the yield occurred for metrics and debugging.
///
/// # Example
///
/// ```rust,ignore
/// use laminar_core::budget::{TaskBudget, YieldReason};
///
/// fn process_background_work(&mut self) -> YieldReason {
///     let budget = TaskBudget::ring1_chunk();
///
///     while !budget.exceeded() {
///         // Check if Ring 0 needs attention (priority)
///         if self.ring0_has_pending() {
///             return YieldReason::Ring0Priority;
///         }
///
///         // Get next work item
///         match self.background_queue.pop() {
///             Some(work) => self.process_work_item(work),
///             None => return YieldReason::QueueEmpty,
///         }
///     }
///
///     YieldReason::BudgetExceeded
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum YieldReason {
    /// Budget exceeded, need to give Ring 0 a chance.
    ///
    /// The Ring 1 task has used its entire time budget (typically 1ms)
    /// and must yield to avoid blocking Ring 0 operations.
    BudgetExceeded,

    /// Ring 0 has pending events (priority yield).
    ///
    /// Even if budget remains, Ring 0 events take priority over
    /// Ring 1 background work. This ensures latency SLAs.
    Ring0Priority,

    /// No more work in queue.
    ///
    /// The background work queue is empty. The task can sleep
    /// or wait for new work to arrive.
    QueueEmpty,

    /// Shutdown has been requested.
    ///
    /// The task should stop processing and allow graceful shutdown.
    ShutdownRequested,

    /// External interrupt (e.g., signal).
    ///
    /// An external event has interrupted processing.
    Interrupted,

    /// Yielded to allow checkpoint coordination.
    ///
    /// During checkpointing, tasks may need to yield to ensure
    /// consistent state snapshots.
    CheckpointBarrier,

    /// Yielded due to backpressure.
    ///
    /// Downstream cannot accept more outputs, so the task yields
    /// to avoid buffering too much data.
    Backpressure,

    /// Yielded to allow other Ring 1 tasks to run.
    ///
    /// Fair scheduling among multiple Ring 1 tasks.
    FairScheduling,
}

impl YieldReason {
    /// Returns true if this yield reason indicates the task should stop.
    #[must_use]
    pub fn should_stop(&self) -> bool {
        matches!(
            self,
            YieldReason::ShutdownRequested | YieldReason::Interrupted
        )
    }

    /// Returns true if this yield reason indicates work is available.
    #[must_use]
    pub fn has_more_work(&self) -> bool {
        matches!(
            self,
            YieldReason::BudgetExceeded
                | YieldReason::Ring0Priority
                | YieldReason::CheckpointBarrier
                | YieldReason::FairScheduling
        )
    }

    /// Returns true if the task can immediately resume after yield.
    #[must_use]
    pub fn can_resume_immediately(&self) -> bool {
        matches!(
            self,
            YieldReason::BudgetExceeded | YieldReason::Ring0Priority | YieldReason::FairScheduling
        )
    }

    /// Returns true if this yield is due to Ring 0 priority.
    #[must_use]
    pub fn is_ring0_priority(&self) -> bool {
        matches!(self, YieldReason::Ring0Priority)
    }

    /// Returns true if this yield is due to budget constraints.
    #[must_use]
    pub fn is_budget_related(&self) -> bool {
        matches!(self, YieldReason::BudgetExceeded)
    }

    /// Get metric label for this yield reason.
    #[must_use]
    pub fn metric_label(&self) -> &'static str {
        match self {
            YieldReason::BudgetExceeded => "budget_exceeded",
            YieldReason::Ring0Priority => "ring0_priority",
            YieldReason::QueueEmpty => "queue_empty",
            YieldReason::ShutdownRequested => "shutdown",
            YieldReason::Interrupted => "interrupted",
            YieldReason::CheckpointBarrier => "checkpoint",
            YieldReason::Backpressure => "backpressure",
            YieldReason::FairScheduling => "fair_scheduling",
        }
    }
}

impl fmt::Display for YieldReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            YieldReason::BudgetExceeded => write!(f, "BudgetExceeded"),
            YieldReason::Ring0Priority => write!(f, "Ring0Priority"),
            YieldReason::QueueEmpty => write!(f, "QueueEmpty"),
            YieldReason::ShutdownRequested => write!(f, "ShutdownRequested"),
            YieldReason::Interrupted => write!(f, "Interrupted"),
            YieldReason::CheckpointBarrier => write!(f, "CheckpointBarrier"),
            YieldReason::Backpressure => write!(f, "Backpressure"),
            YieldReason::FairScheduling => write!(f, "FairScheduling"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_stop() {
        assert!(!YieldReason::BudgetExceeded.should_stop());
        assert!(!YieldReason::Ring0Priority.should_stop());
        assert!(!YieldReason::QueueEmpty.should_stop());
        assert!(YieldReason::ShutdownRequested.should_stop());
        assert!(YieldReason::Interrupted.should_stop());
        assert!(!YieldReason::CheckpointBarrier.should_stop());
        assert!(!YieldReason::Backpressure.should_stop());
        assert!(!YieldReason::FairScheduling.should_stop());
    }

    #[test]
    fn test_has_more_work() {
        assert!(YieldReason::BudgetExceeded.has_more_work());
        assert!(YieldReason::Ring0Priority.has_more_work());
        assert!(!YieldReason::QueueEmpty.has_more_work());
        assert!(!YieldReason::ShutdownRequested.has_more_work());
        assert!(YieldReason::CheckpointBarrier.has_more_work());
        assert!(!YieldReason::Backpressure.has_more_work());
        assert!(YieldReason::FairScheduling.has_more_work());
    }

    #[test]
    fn test_can_resume_immediately() {
        assert!(YieldReason::BudgetExceeded.can_resume_immediately());
        assert!(YieldReason::Ring0Priority.can_resume_immediately());
        assert!(!YieldReason::QueueEmpty.can_resume_immediately());
        assert!(!YieldReason::ShutdownRequested.can_resume_immediately());
        assert!(!YieldReason::CheckpointBarrier.can_resume_immediately());
        assert!(YieldReason::FairScheduling.can_resume_immediately());
    }

    #[test]
    fn test_metric_labels() {
        assert_eq!(
            YieldReason::BudgetExceeded.metric_label(),
            "budget_exceeded"
        );
        assert_eq!(YieldReason::Ring0Priority.metric_label(), "ring0_priority");
        assert_eq!(YieldReason::QueueEmpty.metric_label(), "queue_empty");
        assert_eq!(YieldReason::ShutdownRequested.metric_label(), "shutdown");
        assert_eq!(YieldReason::Interrupted.metric_label(), "interrupted");
        assert_eq!(YieldReason::CheckpointBarrier.metric_label(), "checkpoint");
        assert_eq!(YieldReason::Backpressure.metric_label(), "backpressure");
        assert_eq!(
            YieldReason::FairScheduling.metric_label(),
            "fair_scheduling"
        );
    }

    #[test]
    fn test_is_ring0_priority() {
        assert!(!YieldReason::BudgetExceeded.is_ring0_priority());
        assert!(YieldReason::Ring0Priority.is_ring0_priority());
        assert!(!YieldReason::QueueEmpty.is_ring0_priority());
    }

    #[test]
    fn test_is_budget_related() {
        assert!(YieldReason::BudgetExceeded.is_budget_related());
        assert!(!YieldReason::Ring0Priority.is_budget_related());
        assert!(!YieldReason::QueueEmpty.is_budget_related());
    }

    #[test]
    fn test_equality() {
        assert_eq!(YieldReason::BudgetExceeded, YieldReason::BudgetExceeded);
        assert_ne!(YieldReason::BudgetExceeded, YieldReason::Ring0Priority);
    }

    #[test]
    fn test_clone() {
        let reason = YieldReason::Ring0Priority;
        let cloned = reason;
        assert_eq!(reason, cloned);
    }

    #[test]
    fn test_debug() {
        let debug = format!("{:?}", YieldReason::BudgetExceeded);
        assert!(debug.contains("BudgetExceeded"));
    }
}
