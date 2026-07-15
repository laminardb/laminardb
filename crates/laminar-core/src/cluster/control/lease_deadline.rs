use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Process-local monotonic deadline for a renewable durable lease.
pub struct LeaseDeadline {
    origin: Instant,
    valid_until_ns: AtomicU64,
}

impl LeaseDeadline {
    /// Create a fenced deadline.
    #[must_use]
    pub fn fenced() -> Self {
        Self {
            origin: Instant::now(),
            valid_until_ns: AtomicU64::new(0),
        }
    }

    /// Create a deadline live for `remaining`.
    #[must_use]
    pub fn live_for(remaining: Duration) -> Self {
        let deadline = Self::fenced();
        deadline.extend(remaining);
        deadline
    }

    pub(crate) fn extend(&self, remaining: Duration) {
        let until = self.origin.elapsed().saturating_add(remaining).as_nanos();
        let until = u64::try_from(until).unwrap_or(u64::MAX).max(1);
        self.valid_until_ns.store(until, Ordering::Release);
    }

    pub(crate) fn fence(&self) {
        self.valid_until_ns.store(0, Ordering::Release);
    }

    /// Whether the holder remains inside its last successful renewal deadline.
    #[must_use]
    pub fn is_live(&self) -> bool {
        let deadline = self.valid_until_ns.load(Ordering::Acquire);
        deadline != 0 && self.origin.elapsed().as_nanos() < u128::from(deadline)
    }
}

impl std::fmt::Debug for LeaseDeadline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseDeadline")
            .field("live", &self.is_live())
            .finish()
    }
}
