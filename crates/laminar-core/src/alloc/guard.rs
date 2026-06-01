//! RAII guard for priority class enforcement.

/// Priority class for the current execution context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityClass {
    /// Event processing — <10ms cycle budget, no blocking I/O.
    EventProcessing,
    /// Background I/O — checkpoint, WAL, table polling.
    BackgroundIo,
    /// Control plane — DDL, config changes, metrics.
    Control,
}

/// RAII guard that sets the priority class.
#[derive(Debug)]
pub struct PriorityGuard;

impl PriorityGuard {
    /// Enter a priority class section.
    #[inline]
    #[must_use]
    pub fn enter(#[allow(unused_variables)] class: PriorityClass) -> Self {
        Self
    }
}
