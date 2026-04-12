//! MySQL CDC source connector metrics.
//!
//! Prometheus-backed counters/gauges for CDC event processing statistics.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::metrics::ConnectorMetrics;

/// Metrics for MySQL CDC source connector.
#[derive(Debug, Clone)]
pub struct MySqlCdcMetrics {
    /// Total number of binlog events received.
    pub events_received: IntCounter,

    /// Total number of INSERT row events processed.
    pub inserts: IntCounter,

    /// Total number of UPDATE row events processed.
    pub updates: IntCounter,

    /// Total number of DELETE row events processed.
    pub deletes: IntCounter,

    /// Total number of transactions seen.
    pub transactions: IntCounter,

    /// Total number of TABLE_MAP events processed.
    pub table_maps: IntCounter,

    /// Total number of bytes received from binlog.
    pub bytes_received: IntCounter,

    /// Total number of errors encountered.
    pub errors: IntCounter,

    /// Total number of heartbeats received.
    pub heartbeats: IntCounter,

    /// Total number of DDL (query) events.
    pub ddl_events: IntCounter,

    /// Current binlog position (low 32 bits).
    pub binlog_position: IntGauge,
}

impl MySqlCdcMetrics {
    /// Creates new metrics with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let local;
        let reg = if let Some(r) = registry {
            r
        } else {
            local = Registry::new();
            &local
        };

        let events_received = IntCounter::new(
            "mysql_cdc_events_received_total",
            "Total binlog events received",
        )
        .unwrap();
        let inserts =
            IntCounter::new("mysql_cdc_inserts_total", "Total INSERT row events").unwrap();
        let updates =
            IntCounter::new("mysql_cdc_updates_total", "Total UPDATE row events").unwrap();
        let deletes =
            IntCounter::new("mysql_cdc_deletes_total", "Total DELETE row events").unwrap();
        let transactions =
            IntCounter::new("mysql_cdc_transactions_total", "Total transactions").unwrap();
        let table_maps =
            IntCounter::new("mysql_cdc_table_maps_total", "Total TABLE_MAP events").unwrap();
        let bytes_received =
            IntCounter::new("mysql_cdc_bytes_received_total", "Total bytes from binlog").unwrap();
        let errors = IntCounter::new("mysql_cdc_errors_total", "Total CDC errors").unwrap();
        let heartbeats =
            IntCounter::new("mysql_cdc_heartbeats_total", "Total heartbeats received").unwrap();
        let ddl_events = IntCounter::new("mysql_cdc_ddl_events_total", "Total DDL events").unwrap();
        let binlog_position =
            IntGauge::new("mysql_cdc_binlog_position", "Current binlog position").unwrap();

        let _ = reg.register(Box::new(events_received.clone()));
        let _ = reg.register(Box::new(inserts.clone()));
        let _ = reg.register(Box::new(updates.clone()));
        let _ = reg.register(Box::new(deletes.clone()));
        let _ = reg.register(Box::new(transactions.clone()));
        let _ = reg.register(Box::new(table_maps.clone()));
        let _ = reg.register(Box::new(bytes_received.clone()));
        let _ = reg.register(Box::new(errors.clone()));
        let _ = reg.register(Box::new(heartbeats.clone()));
        let _ = reg.register(Box::new(ddl_events.clone()));
        let _ = reg.register(Box::new(binlog_position.clone()));

        Self {
            events_received,
            inserts,
            updates,
            deletes,
            transactions,
            table_maps,
            bytes_received,
            errors,
            heartbeats,
            ddl_events,
            binlog_position,
        }
    }

    /// Increments the binlog events received counter.
    pub fn inc_events_received(&self) {
        self.events_received.inc();
    }

    /// Increments the INSERT row event counter by `count`.
    pub fn inc_inserts(&self, count: u64) {
        self.inserts.inc_by(count);
    }

    /// Increments the UPDATE row event counter by `count`.
    pub fn inc_updates(&self, count: u64) {
        self.updates.inc_by(count);
    }

    /// Increments the DELETE row event counter by `count`.
    pub fn inc_deletes(&self, count: u64) {
        self.deletes.inc_by(count);
    }

    /// Increments the transaction counter.
    pub fn inc_transactions(&self) {
        self.transactions.inc();
    }

    /// Increments the TABLE_MAP event counter.
    pub fn inc_table_maps(&self) {
        self.table_maps.inc();
    }

    /// Adds bytes to the bytes received counter.
    pub fn add_bytes_received(&self, bytes: u64) {
        self.bytes_received.inc_by(bytes);
    }

    /// Increments the error counter.
    pub fn inc_errors(&self) {
        self.errors.inc();
    }

    /// Increments the heartbeat counter.
    pub fn inc_heartbeats(&self) {
        self.heartbeats.inc();
    }

    /// Increments the DDL event counter.
    pub fn inc_ddl_events(&self) {
        self.ddl_events.inc();
    }

    /// Updates the current binlog position.
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_binlog_position(&self, position: u64) {
        self.binlog_position.set(position as i64);
    }

    /// Returns the current binlog position.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn get_binlog_position(&self) -> u64 {
        self.binlog_position.get() as u64
    }

    /// Returns the total number of row events (insert + update + delete).
    #[must_use]
    pub fn total_row_events(&self) -> u64 {
        self.inserts.get() + self.updates.get() + self.deletes.get()
    }

    /// Returns a snapshot of all metrics.
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_received: self.events_received.get(),
            inserts: self.inserts.get(),
            updates: self.updates.get(),
            deletes: self.deletes.get(),
            transactions: self.transactions.get(),
            table_maps: self.table_maps.get(),
            bytes_received: self.bytes_received.get(),
            errors: self.errors.get(),
            heartbeats: self.heartbeats.get(),
            ddl_events: self.ddl_events.get(),
            binlog_position: self.binlog_position.get() as u64,
        }
    }

    /// Converts to generic connector metrics.
    #[must_use]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.total_row_events(),
            bytes_total: self.bytes_received.get(),
            errors_total: self.errors.get(),
            ..ConnectorMetrics::default()
        }
    }
}

impl Default for MySqlCdcMetrics {
    fn default() -> Self {
        Self::new(None)
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
        let m = MySqlCdcMetrics::new(None);
        assert_eq!(m.events_received.get(), 0);
        assert_eq!(m.inserts.get(), 0);
        assert_eq!(m.errors.get(), 0);
    }

    #[test]
    fn test_inc_events_received() {
        let m = MySqlCdcMetrics::new(None);
        m.inc_events_received();
        m.inc_events_received();
        assert_eq!(m.events_received.get(), 2);
    }

    #[test]
    fn test_inc_row_events() {
        let m = MySqlCdcMetrics::new(None);
        m.inc_inserts(5);
        m.inc_updates(3);
        m.inc_deletes(2);
        assert_eq!(m.total_row_events(), 10);
    }

    #[test]
    fn test_add_bytes() {
        let m = MySqlCdcMetrics::new(None);
        m.add_bytes_received(100);
        m.add_bytes_received(50);
        assert_eq!(m.bytes_received.get(), 150);
    }

    #[test]
    fn test_binlog_position() {
        let m = MySqlCdcMetrics::new(None);
        m.set_binlog_position(12345);
        assert_eq!(m.get_binlog_position(), 12345);
    }

    #[test]
    fn test_snapshot() {
        let m = MySqlCdcMetrics::new(None);
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
        let m = MySqlCdcMetrics::new(None);
        m.inc_inserts(10);
        m.add_bytes_received(1000);
        m.inc_errors();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 10);
        assert_eq!(cm.bytes_total, 1000);
        assert_eq!(cm.errors_total, 1);
    }

    #[test]
    fn test_inc_all_counters() {
        let m = MySqlCdcMetrics::new(None);

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
