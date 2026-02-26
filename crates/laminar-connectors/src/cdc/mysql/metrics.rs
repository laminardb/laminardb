//! MySQL CDC source connector metrics.
//!
//! Lock-free atomic counters for CDC event processing statistics.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Metrics for MySQL CDC source connector.
#[derive(Debug, Default)]
pub struct MySqlCdcMetrics {
    /// Total number of binlog events received.
    pub events_received: AtomicU64,

    /// Total number of INSERT row events processed.
    pub inserts: AtomicU64,

    /// Total number of UPDATE row events processed.
    pub updates: AtomicU64,

    /// Total number of DELETE row events processed.
    pub deletes: AtomicU64,

    /// Total number of transactions seen.
    pub transactions: AtomicU64,

    /// Total number of TABLE_MAP events processed.
    pub table_maps: AtomicU64,

    /// Total number of bytes received from binlog.
    pub bytes_received: AtomicU64,

    /// Total number of errors encountered.
    pub errors: AtomicU64,

    /// Total number of heartbeats received.
    pub heartbeats: AtomicU64,

    /// Total number of DDL (query) events.
    pub ddl_events: AtomicU64,

    /// Current binlog position (low 32 bits).
    pub binlog_position: AtomicU64,
}

impl MySqlCdcMetrics {
    /// Creates new metrics with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_events_received(&self) {
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_inserts(&self, count: u64) {
        self.inserts.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_updates(&self, count: u64) {
        self.updates.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_deletes(&self, count: u64) {
        self.deletes.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_transactions(&self) {
        self.transactions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_table_maps(&self) {
        self.table_maps.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds bytes to the bytes received counter.
    pub fn add_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn inc_errors(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_heartbeats(&self) {
        self.heartbeats.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ddl_events(&self) {
        self.ddl_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Updates the current binlog position.
    pub fn set_binlog_position(&self, position: u64) {
        self.binlog_position.store(position, Ordering::Relaxed);
    }

    /// Returns the current binlog position.
    #[must_use]
    pub fn get_binlog_position(&self) -> u64 {
        self.binlog_position.load(Ordering::Relaxed)
    }

    /// Returns the total number of row events (insert + update + delete).
    #[must_use]
    pub fn total_row_events(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
            + self.updates.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
    }

    /// Returns a snapshot of all metrics.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_received: self.events_received.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            transactions: self.transactions.load(Ordering::Relaxed),
            table_maps: self.table_maps.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            heartbeats: self.heartbeats.load(Ordering::Relaxed),
            ddl_events: self.ddl_events.load(Ordering::Relaxed),
            binlog_position: self.binlog_position.load(Ordering::Relaxed),
        }
    }

    /// Converts to generic connector metrics.
    #[must_use]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.total_row_events(),
            bytes_total: self.bytes_received.load(Ordering::Relaxed),
            errors_total: self.errors.load(Ordering::Relaxed),
            ..ConnectorMetrics::default()
        }
    }

    /// Resets all counters to zero.
    pub fn reset(&self) {
        self.events_received.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.updates.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.transactions.store(0, Ordering::Relaxed);
        self.table_maps.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.heartbeats.store(0, Ordering::Relaxed);
        self.ddl_events.store(0, Ordering::Relaxed);
        self.binlog_position.store(0, Ordering::Relaxed);
    }
}

/// A snapshot of metrics values at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Total binlog events received.
    pub events_received: u64,
    /// Total INSERT operations.
    pub inserts: u64,
    /// Total UPDATE operations.
    pub updates: u64,
    /// Total DELETE operations.
    pub deletes: u64,
    /// Total transactions.
    pub transactions: u64,
    /// Total TABLE_MAP events.
    pub table_maps: u64,
    /// Total bytes received.
    pub bytes_received: u64,
    /// Total errors.
    pub errors: u64,
    /// Total heartbeats.
    pub heartbeats: u64,
    /// Total DDL events.
    pub ddl_events: u64,
    /// Current binlog position.
    pub binlog_position: u64,
}

impl MetricsSnapshot {
    /// Returns the total row events.
    #[must_use]
    pub fn total_row_events(&self) -> u64 {
        self.inserts + self.updates + self.deletes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_metrics() {
        let m = MySqlCdcMetrics::new();
        assert_eq!(m.events_received.load(Ordering::Relaxed), 0);
        assert_eq!(m.inserts.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inc_events_received() {
        let m = MySqlCdcMetrics::new();
        m.inc_events_received();
        m.inc_events_received();
        assert_eq!(m.events_received.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_inc_row_events() {
        let m = MySqlCdcMetrics::new();
        m.inc_inserts(5);
        m.inc_updates(3);
        m.inc_deletes(2);
        assert_eq!(m.total_row_events(), 10);
    }

    #[test]
    fn test_add_bytes() {
        let m = MySqlCdcMetrics::new();
        m.add_bytes_received(100);
        m.add_bytes_received(50);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn test_binlog_position() {
        let m = MySqlCdcMetrics::new();
        m.set_binlog_position(12345);
        assert_eq!(m.get_binlog_position(), 12345);
    }

    #[test]
    fn test_snapshot() {
        let m = MySqlCdcMetrics::new();
        m.inc_inserts(10);
        m.inc_updates(5);
        m.inc_transactions();

        let snap = m.snapshot();
        assert_eq!(snap.inserts, 10);
        assert_eq!(snap.updates, 5);
        assert_eq!(snap.transactions, 1);
        assert_eq!(snap.total_row_events(), 15);
    }

    #[test]
    fn test_to_connector_metrics() {
        let m = MySqlCdcMetrics::new();
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
        let m = MySqlCdcMetrics::new();
        m.inc_inserts(10);
        m.inc_errors();
        m.set_binlog_position(12345);

        m.reset();

        assert_eq!(m.inserts.load(Ordering::Relaxed), 0);
        assert_eq!(m.errors.load(Ordering::Relaxed), 0);
        assert_eq!(m.binlog_position.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inc_all_counters() {
        let m = MySqlCdcMetrics::new();

        m.inc_events_received();
        m.inc_inserts(1);
        m.inc_updates(1);
        m.inc_deletes(1);
        m.inc_transactions();
        m.inc_table_maps();
        m.add_bytes_received(100);
        m.inc_errors();
        m.inc_heartbeats();
        m.inc_ddl_events();

        let snap = m.snapshot();
        assert_eq!(snap.events_received, 1);
        assert_eq!(snap.inserts, 1);
        assert_eq!(snap.updates, 1);
        assert_eq!(snap.deletes, 1);
        assert_eq!(snap.transactions, 1);
        assert_eq!(snap.table_maps, 1);
        assert_eq!(snap.bytes_received, 100);
        assert_eq!(snap.errors, 1);
        assert_eq!(snap.heartbeats, 1);
        assert_eq!(snap.ddl_events, 1);
    }
}
