//! `PostgreSQL` sink connector metrics.
//!
//! [`PostgresSinkMetrics`] provides lock-free atomic counters for
//! tracking write statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for `PostgreSQL` sink connector statistics.
#[derive(Debug)]
pub struct PostgresSinkMetrics {
    /// Total records written to `PostgreSQL`.
    pub records_written: AtomicU64,

    /// Total bytes written (estimated from `RecordBatch` sizes).
    pub bytes_written: AtomicU64,

    /// Total errors encountered.
    pub errors_total: AtomicU64,

    /// Total batches flushed.
    pub batches_flushed: AtomicU64,

    /// Total COPY BINARY operations (append mode).
    pub copy_operations: AtomicU64,

    /// Total upsert operations (upsert mode).
    pub upsert_operations: AtomicU64,

    /// Total epochs committed (exactly-once).
    pub epochs_committed: AtomicU64,

    /// Total epochs rolled back.
    pub epochs_rolled_back: AtomicU64,

    /// Total changelog deletes applied (Z-set weight -1).
    pub changelog_deletes: AtomicU64,
}

impl PostgresSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            copy_operations: AtomicU64::new(0),
            upsert_operations: AtomicU64::new(0),
            epochs_committed: AtomicU64::new(0),
            epochs_rolled_back: AtomicU64::new(0),
            changelog_deletes: AtomicU64::new(0),
        }
    }

    /// Records a successful write of `records` records totaling `bytes`.
    pub fn record_write(&self, records: u64, bytes: u64) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a successful batch flush.
    pub fn record_flush(&self) {
        self.batches_flushed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a COPY BINARY operation.
    pub fn record_copy(&self) {
        self.copy_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an upsert operation.
    pub fn record_upsert(&self) {
        self.upsert_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a write or connection error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful epoch commit.
    pub fn record_commit(&self) {
        self.epochs_committed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Records changelog DELETE operations.
    pub fn record_deletes(&self, count: u64) {
        self.changelog_deletes.fetch_add(count, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_written.load(Ordering::Relaxed),
            bytes_total: self.bytes_written.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "pg.batches_flushed",
            self.batches_flushed.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "pg.copy_operations",
            self.copy_operations.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "pg.upsert_operations",
            self.upsert_operations.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "pg.epochs_committed",
            self.epochs_committed.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "pg.epochs_rolled_back",
            self.epochs_rolled_back.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "pg.changelog_deletes",
            self.changelog_deletes.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for PostgresSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = PostgresSinkMetrics::new();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_write() {
        let m = PostgresSinkMetrics::new();
        m.record_write(100, 5000);
        m.record_write(200, 10_000);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 300);
        assert_eq!(cm.bytes_total, 15_000);
    }

    #[test]
    fn test_flush_and_copy_metrics() {
        let m = PostgresSinkMetrics::new();
        m.record_flush();
        m.record_flush();
        m.record_copy();

        let cm = m.to_connector_metrics();
        let flushed = cm.custom.iter().find(|(k, _)| k == "pg.batches_flushed");
        assert_eq!(flushed.unwrap().1, 2.0);
        let copies = cm.custom.iter().find(|(k, _)| k == "pg.copy_operations");
        assert_eq!(copies.unwrap().1, 1.0);
    }

    #[test]
    fn test_epoch_metrics() {
        let m = PostgresSinkMetrics::new();
        m.record_commit();
        m.record_commit();
        m.record_rollback();

        let cm = m.to_connector_metrics();
        let committed = cm.custom.iter().find(|(k, _)| k == "pg.epochs_committed");
        assert_eq!(committed.unwrap().1, 2.0);
        let rolled_back = cm.custom.iter().find(|(k, _)| k == "pg.epochs_rolled_back");
        assert_eq!(rolled_back.unwrap().1, 1.0);
    }

    #[test]
    fn test_changelog_deletes() {
        let m = PostgresSinkMetrics::new();
        m.record_deletes(50);
        m.record_deletes(30);

        let cm = m.to_connector_metrics();
        let deletes = cm.custom.iter().find(|(k, _)| k == "pg.changelog_deletes");
        assert_eq!(deletes.unwrap().1, 80.0);
    }

    #[test]
    fn test_error_counting() {
        let m = PostgresSinkMetrics::new();
        m.record_error();
        m.record_error();
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 3);
    }

    #[test]
    fn test_upsert_metric() {
        let m = PostgresSinkMetrics::new();
        m.record_upsert();

        let cm = m.to_connector_metrics();
        let upserts = cm.custom.iter().find(|(k, _)| k == "pg.upsert_operations");
        assert_eq!(upserts.unwrap().1, 1.0);
    }
}
