//! `PostgreSQL` CDC source connector metrics.
//!
//! Lock-free atomic counters for tracking CDC replication performance.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Metrics for the `PostgreSQL` CDC source connector.
///
/// All counters use relaxed atomic ordering for lock-free access
/// from the Ring 1 async runtime.
#[derive(Debug)]
pub struct CdcMetrics {
    /// Total change events received (insert + update + delete).
    pub events_received: AtomicU64,

    /// Total bytes received from the WAL stream.
    pub bytes_received: AtomicU64,

    /// Total errors encountered.
    pub errors: AtomicU64,

    /// Total batches produced for downstream.
    pub batches_produced: AtomicU64,

    /// Total INSERT operations received.
    pub inserts: AtomicU64,

    /// Total UPDATE operations received.
    pub updates: AtomicU64,

    /// Total DELETE operations received.
    pub deletes: AtomicU64,

    /// Total transactions (commit messages) received.
    pub transactions: AtomicU64,

    /// Current confirmed flush LSN (as raw u64).
    pub confirmed_flush_lsn: AtomicU64,

    /// Current replication lag in bytes (`write_lsn` - `confirmed_flush_lsn`).
    pub replication_lag_bytes: AtomicU64,

    /// Total keepalive/heartbeat messages sent.
    pub keepalives_sent: AtomicU64,

    /// Total events dropped due to buffer cap enforcement.
    pub events_dropped: AtomicU64,
}

impl CdcMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            batches_produced: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            confirmed_flush_lsn: AtomicU64::new(0),
            replication_lag_bytes: AtomicU64::new(0),
            keepalives_sent: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
        }
    }

    /// Records a received INSERT event.
    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a received UPDATE event.
    pub fn record_update(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a received DELETE event.
    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a received transaction commit.
    pub fn record_transaction(&self) {
        self.transactions.fetch_add(1, Ordering::Relaxed);
    }

    /// Records bytes received from the WAL stream.
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a batch produced for downstream.
    pub fn record_batch(&self) {
        self.batches_produced.fetch_add(1, Ordering::Relaxed);
    }

    /// Updates the confirmed flush LSN.
    pub fn set_confirmed_flush_lsn(&self, lsn: u64) {
        self.confirmed_flush_lsn.store(lsn, Ordering::Relaxed);
    }

    /// Updates the replication lag in bytes.
    pub fn set_replication_lag_bytes(&self, lag: u64) {
        self.replication_lag_bytes.store(lag, Ordering::Relaxed);
    }

    /// Records a keepalive sent to `PostgreSQL`.
    pub fn record_keepalive(&self) {
        self.keepalives_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Records events dropped due to buffer cap.
    pub fn record_dropped(&self, count: u64) {
        self.events_dropped.fetch_add(count, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        m.records_total = self.events_received.load(Ordering::Relaxed);
        m.bytes_total = self.bytes_received.load(Ordering::Relaxed);
        m.errors_total = self.errors.load(Ordering::Relaxed);
        m.lag = self.replication_lag_bytes.load(Ordering::Relaxed);

        m.add_custom("inserts", self.inserts.load(Ordering::Relaxed) as f64);
        m.add_custom("updates", self.updates.load(Ordering::Relaxed) as f64);
        m.add_custom("deletes", self.deletes.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "transactions",
            self.transactions.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "confirmed_flush_lsn",
            self.confirmed_flush_lsn.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "events_dropped",
            self.events_dropped.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "keepalives_sent",
            self.keepalives_sent.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for CdcMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_operations() {
        let m = CdcMetrics::new();
        m.record_insert();
        m.record_insert();
        m.record_update();
        m.record_delete();
        m.record_transaction();
        m.record_bytes(1024);
        m.record_error();
        m.record_batch();
        m.record_keepalive();

        assert_eq!(m.events_received.load(Ordering::Relaxed), 4);
        assert_eq!(m.inserts.load(Ordering::Relaxed), 2);
        assert_eq!(m.updates.load(Ordering::Relaxed), 1);
        assert_eq!(m.deletes.load(Ordering::Relaxed), 1);
        assert_eq!(m.transactions.load(Ordering::Relaxed), 1);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 1024);
        assert_eq!(m.errors.load(Ordering::Relaxed), 1);
        assert_eq!(m.batches_produced.load(Ordering::Relaxed), 1);
        assert_eq!(m.keepalives_sent.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_lsn_and_lag_tracking() {
        let m = CdcMetrics::new();
        m.set_confirmed_flush_lsn(0x1234_ABCD);
        m.set_replication_lag_bytes(4096);

        assert_eq!(m.confirmed_flush_lsn.load(Ordering::Relaxed), 0x1234_ABCD);
        assert_eq!(m.replication_lag_bytes.load(Ordering::Relaxed), 4096);
    }

    #[test]
    fn test_to_connector_metrics() {
        let m = CdcMetrics::new();
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
