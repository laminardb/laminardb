//! Lock-free per-source task metrics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-source task metrics using atomics (no locks on the data path).
#[derive(Debug, Default)]
pub struct SourceTaskMetrics {
    /// Total batches produced by this source task.
    pub batches: AtomicU64,
    /// Total records produced by this source task.
    pub records: AtomicU64,
    /// Total poll errors.
    pub errors: AtomicU64,
    /// Last poll latency in nanoseconds.
    pub last_poll_ns: AtomicU64,
}

impl SourceTaskMetrics {
    /// Records a successful poll.
    pub fn record_poll(&self, records: u64, latency_ns: u64) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.records.fetch_add(records, Ordering::Relaxed);
        self.last_poll_ns.store(latency_ns, Ordering::Relaxed);
    }

    /// Records a poll error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a snapshot of the current metrics.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches: self.batches.load(Ordering::Relaxed),
            records: self.records.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            last_poll_ns: self.last_poll_ns.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of source task metrics.
#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    /// Total batches.
    pub batches: u64,
    /// Total records.
    pub records: u64,
    /// Total errors.
    pub errors: u64,
    /// Last poll latency in nanoseconds.
    pub last_poll_ns: u64,
}
