//! `PostgreSQL` CDC source connector metrics.
//!
//! Prometheus-backed counters/gauges for tracking CDC replication performance.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::metrics::ConnectorMetrics;

/// Metrics for the `PostgreSQL` CDC source connector.
///
/// All counters are prometheus-backed and will appear in the scrape
/// output when a shared registry is provided.
#[derive(Debug, Clone)]
pub struct PostgresCdcMetrics {
    /// Total change events received (insert + update + delete).
    pub events_received: IntCounter,

    /// Total bytes received from the WAL stream.
    pub bytes_received: IntCounter,

    /// Total errors encountered.
    pub errors: IntCounter,

    /// Total batches produced for downstream.
    pub batches_produced: IntCounter,

    /// Total INSERT operations received.
    pub inserts: IntCounter,

    /// Total UPDATE operations received.
    pub updates: IntCounter,

    /// Total DELETE operations received.
    pub deletes: IntCounter,

    /// Total transactions (commit messages) received.
    pub transactions: IntCounter,

    /// Current confirmed flush LSN (as raw u64).
    pub confirmed_flush_lsn: IntGauge,

    /// Current replication lag in bytes (`write_lsn` - `confirmed_flush_lsn`).
    pub replication_lag_bytes: IntGauge,

    /// Total keepalive/heartbeat messages sent.
    pub keepalives_sent: IntCounter,

    /// Total events dropped due to buffer cap enforcement.
    pub events_dropped: IntCounter,
}

impl PostgresCdcMetrics {
    /// Creates a new metrics instance with all counters at zero.
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
            "postgres_cdc_events_received_total",
            "Total CDC change events received",
        )
        .unwrap();
        let bytes_received = IntCounter::new(
            "postgres_cdc_bytes_received_total",
            "Total bytes from WAL stream",
        )
        .unwrap();
        let errors = IntCounter::new("postgres_cdc_errors_total", "Total CDC errors").unwrap();
        let batches_produced = IntCounter::new(
            "postgres_cdc_batches_produced_total",
            "Total batches produced",
        )
        .unwrap();
        let inserts = IntCounter::new("postgres_cdc_inserts_total", "Total INSERT events").unwrap();
        let updates = IntCounter::new("postgres_cdc_updates_total", "Total UPDATE events").unwrap();
        let deletes = IntCounter::new("postgres_cdc_deletes_total", "Total DELETE events").unwrap();
        let transactions = IntCounter::new(
            "postgres_cdc_transactions_total",
            "Total transactions received",
        )
        .unwrap();
        let confirmed_flush_lsn = IntGauge::new(
            "postgres_cdc_confirmed_flush_lsn",
            "Current confirmed flush LSN",
        )
        .unwrap();
        let replication_lag_bytes = IntGauge::new(
            "postgres_cdc_replication_lag_bytes",
            "Replication lag in bytes",
        )
        .unwrap();
        let keepalives_sent = IntCounter::new(
            "postgres_cdc_keepalives_sent_total",
            "Total keepalive messages sent",
        )
        .unwrap();
        let events_dropped = IntCounter::new(
            "postgres_cdc_events_dropped_total",
            "Total events dropped (buffer cap)",
        )
        .unwrap();

        let _ = reg.register(Box::new(events_received.clone()));
        let _ = reg.register(Box::new(bytes_received.clone()));
        let _ = reg.register(Box::new(errors.clone()));
        let _ = reg.register(Box::new(batches_produced.clone()));
        let _ = reg.register(Box::new(inserts.clone()));
        let _ = reg.register(Box::new(updates.clone()));
        let _ = reg.register(Box::new(deletes.clone()));
        let _ = reg.register(Box::new(transactions.clone()));
        let _ = reg.register(Box::new(confirmed_flush_lsn.clone()));
        let _ = reg.register(Box::new(replication_lag_bytes.clone()));
        let _ = reg.register(Box::new(keepalives_sent.clone()));
        let _ = reg.register(Box::new(events_dropped.clone()));

        Self {
            events_received,
            bytes_received,
            errors,
            batches_produced,
            inserts,
            updates,
            deletes,
            transactions,
            confirmed_flush_lsn,
            replication_lag_bytes,
            keepalives_sent,
            events_dropped,
        }
    }

    /// Records a received INSERT event.
    pub fn record_insert(&self) {
        self.inserts.inc();
        self.events_received.inc();
    }

    /// Records a received UPDATE event.
    pub fn record_update(&self) {
        self.updates.inc();
        self.events_received.inc();
    }

    /// Records a received DELETE event.
    pub fn record_delete(&self) {
        self.deletes.inc();
        self.events_received.inc();
    }

    /// Records a received transaction commit.
    pub fn record_transaction(&self) {
        self.transactions.inc();
    }

    /// Records bytes received from the WAL stream.
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_received.inc_by(bytes);
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors.inc();
    }

    /// Records a batch produced for downstream.
    pub fn record_batch(&self) {
        self.batches_produced.inc();
    }

    /// Updates the confirmed flush LSN.
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_confirmed_flush_lsn(&self, lsn: u64) {
        self.confirmed_flush_lsn.set(lsn as i64);
    }

    /// Updates the replication lag in bytes.
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_replication_lag_bytes(&self, lag: u64) {
        self.replication_lag_bytes.set(lag as i64);
    }

    /// Records a keepalive sent to `PostgreSQL`.
    pub fn record_keepalive(&self) {
        self.keepalives_sent.inc();
    }

    /// Records events dropped due to buffer cap.
    pub fn record_dropped(&self, count: u64) {
        self.events_dropped.inc_by(count);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        m.records_total = self.events_received.get();
        m.bytes_total = self.bytes_received.get();
        m.errors_total = self.errors.get();
        m.lag = self.replication_lag_bytes.get() as u64;

        m.add_custom("inserts", self.inserts.get() as f64);
        m.add_custom("updates", self.updates.get() as f64);
        m.add_custom("deletes", self.deletes.get() as f64);
        m.add_custom("transactions", self.transactions.get() as f64);
        m.add_custom("confirmed_flush_lsn", self.confirmed_flush_lsn.get() as f64);
        m.add_custom("events_dropped", self.events_dropped.get() as f64);
        m.add_custom("keepalives_sent", self.keepalives_sent.get() as f64);
        m
    }
}

impl Default for PostgresCdcMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_operations() {
        let m = PostgresCdcMetrics::new(None);
        m.record_insert();
        m.record_insert();
        m.record_update();
        m.record_delete();
        m.record_transaction();
        m.record_bytes(1024);
        m.record_error();
        m.record_batch();
        m.record_keepalive();

        assert_eq!(m.events_received.get(), 4);
        assert_eq!(m.inserts.get(), 2);
        assert_eq!(m.updates.get(), 1);
        assert_eq!(m.deletes.get(), 1);
        assert_eq!(m.transactions.get(), 1);
        assert_eq!(m.bytes_received.get(), 1024);
        assert_eq!(m.errors.get(), 1);
        assert_eq!(m.batches_produced.get(), 1);
        assert_eq!(m.keepalives_sent.get(), 1);
    }

    #[test]
    fn test_lsn_and_lag_tracking() {
        let m = PostgresCdcMetrics::new(None);
        m.set_confirmed_flush_lsn(0x1234_ABCD);
        m.set_replication_lag_bytes(4096);

        assert_eq!(m.confirmed_flush_lsn.get(), 0x1234_ABCD_i64);
        assert_eq!(m.replication_lag_bytes.get(), 4096);
    }

    #[test]
    fn test_to_connector_metrics() {
        let m = PostgresCdcMetrics::new(None);
        m.record_insert();
        m.record_bytes(512);
        m.record_error();
        m.set_replication_lag_bytes(100);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 1);
        assert_eq!(cm.bytes_total, 512);
        assert_eq!(cm.errors_total, 1);
        assert_eq!(cm.lag, 100);
        assert!(!cm.custom.is_empty());
    }
}
