//! MongoDB CDC source connector metrics.
//!
//! Lock-free atomic counters for change stream event processing statistics.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Metrics for MongoDB CDC source connector.
#[derive(Debug, Default)]
pub struct MongoCdcMetrics {
    /// Total number of change events received.
    pub events_received: AtomicU64,

    /// Total number of INSERT operations processed.
    pub inserts: AtomicU64,

    /// Total number of UPDATE operations processed.
    pub updates: AtomicU64,

    /// Total number of REPLACE operations processed.
    pub replaces: AtomicU64,

    /// Total number of DELETE operations processed.
    pub deletes: AtomicU64,

    /// Total number of bytes received from change stream.
    pub bytes_received: AtomicU64,

    /// Total number of errors encountered.
    pub errors: AtomicU64,

    /// Total number of resume token updates.
    pub resume_token_updates: AtomicU64,
}

impl MongoCdcMetrics {
    /// Creates new metrics with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the change events received counter.
    pub fn inc_events_received(&self) {
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the INSERT counter by `count`.
    pub fn inc_inserts(&self, count: u64) {
        self.inserts.fetch_add(count, Ordering::Relaxed);
    }

    /// Increments the UPDATE counter by `count`.
    pub fn inc_updates(&self, count: u64) {
        self.updates.fetch_add(count, Ordering::Relaxed);
    }

    /// Increments the REPLACE counter by `count`.
    pub fn inc_replaces(&self, count: u64) {
        self.replaces.fetch_add(count, Ordering::Relaxed);
    }

    /// Increments the DELETE counter by `count`.
    pub fn inc_deletes(&self, count: u64) {
        self.deletes.fetch_add(count, Ordering::Relaxed);
    }

    /// Adds bytes to the bytes received counter.
    pub fn add_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Increments the error counter.
    pub fn inc_errors(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the resume token update counter.
    pub fn inc_resume_token_updates(&self) {
        self.resume_token_updates.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the total number of row events (insert + update + replace + delete).
    #[must_use]
    pub fn total_row_events(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
            + self.updates.load(Ordering::Relaxed)
            + self.replaces.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
    }

    /// Returns a snapshot of all metrics.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_received: self.events_received.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            replaces: self.replaces.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            resume_token_updates: self.resume_token_updates.load(Ordering::Relaxed),
        }
    }

    /// Converts to generic connector metrics, including per-operation CDC counters.
    #[must_use]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.total_row_events(),
            bytes_total: self.bytes_received.load(Ordering::Relaxed),
            errors_total: self.errors.load(Ordering::Relaxed),
            ..ConnectorMetrics::default()
        };
        m.add_custom(
            "cdc.events_received",
            self.events_received.load(Ordering::Relaxed) as f64,
        );
        m.add_custom("cdc.inserts", self.inserts.load(Ordering::Relaxed) as f64);
        m.add_custom("cdc.updates", self.updates.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "cdc.replaces",
            self.replaces.load(Ordering::Relaxed) as f64,
        );
        m.add_custom("cdc.deletes", self.deletes.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "cdc.resume_token_updates",
            self.resume_token_updates.load(Ordering::Relaxed) as f64,
        );
        m
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        self.events_received.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.updates.store(0, Ordering::Relaxed);
        self.replaces.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.resume_token_updates.store(0, Ordering::Relaxed);
    }
}

/// A snapshot of metrics values at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Total change events received.
    pub events_received: u64,
    /// Total INSERT operations.
    pub inserts: u64,
    /// Total UPDATE operations.
    pub updates: u64,
    /// Total REPLACE operations.
    pub replaces: u64,
    /// Total DELETE operations.
    pub deletes: u64,
    /// Total bytes received.
    pub bytes_received: u64,
    /// Total errors.
    pub errors: u64,
    /// Total resume token updates.
    pub resume_token_updates: u64,
}

impl MetricsSnapshot {
    /// Returns the total row events.
    #[must_use]
    pub fn total_row_events(&self) -> u64 {
        self.inserts + self.updates + self.replaces + self.deletes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_metrics() {
        let m = MongoCdcMetrics::new();
        assert_eq!(m.events_received.load(Ordering::Relaxed), 0);
        assert_eq!(m.inserts.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inc_events_received() {
        let m = MongoCdcMetrics::new();
        m.inc_events_received();
        m.inc_events_received();
        assert_eq!(m.events_received.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_inc_row_events() {
        let m = MongoCdcMetrics::new();
        m.inc_inserts(5);
        m.inc_updates(3);
        m.inc_replaces(1);
        m.inc_deletes(2);
        assert_eq!(m.total_row_events(), 11);
    }

    #[test]
    fn test_add_bytes() {
        let m = MongoCdcMetrics::new();
        m.add_bytes_received(100);
        m.add_bytes_received(50);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_snapshot() {
        let m = MongoCdcMetrics::new();
        m.inc_inserts(10);
        m.inc_updates(5);
        m.inc_resume_token_updates();

        let snap = m.snapshot();
        assert_eq!(snap.inserts, 10);
        assert_eq!(snap.updates, 5);
        assert_eq!(snap.resume_token_updates, 1);
        assert_eq!(snap.total_row_events(), 15);
    }

    #[test]
    fn test_to_connector_metrics() {
        let m = MongoCdcMetrics::new();
        m.inc_inserts(10);
        m.add_bytes_received(1000);
        m.inc_errors();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 10);
        assert_eq!(cm.bytes_total, 1000);
        assert_eq!(cm.errors_total, 1);
    }

    #[test]
    fn test_reset() {
        let m = MongoCdcMetrics::new();
        m.inc_inserts(10);
        m.inc_errors();
        m.add_bytes_received(500);

        m.reset();

        assert_eq!(m.inserts.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inc_all_counters() {
        let m = MongoCdcMetrics::new();

        m.inc_events_received();
        m.inc_inserts(1);
        m.inc_updates(1);
        m.inc_replaces(1);
        m.inc_deletes(1);
        m.add_bytes_received(100);
        m.inc_errors();
        m.inc_resume_token_updates();

        let snap = m.snapshot();
        assert_eq!(snap.events_received, 1);
        assert_eq!(snap.inserts, 1);
        assert_eq!(snap.updates, 1);
        assert_eq!(snap.replaces, 1);
        assert_eq!(snap.deletes, 1);
        assert_eq!(snap.bytes_received, 100);
        assert_eq!(snap.errors, 1);
        assert_eq!(snap.resume_token_updates, 1);
    }
}
