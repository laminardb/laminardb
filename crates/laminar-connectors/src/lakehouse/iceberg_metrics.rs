//! Apache Iceberg sink connector metrics.
//!
//! [`IcebergSinkMetrics`] provides lock-free atomic counters for
//! tracking write statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for Iceberg sink connector statistics.
#[derive(Debug)]
pub struct IcebergSinkMetrics {
    /// Total rows flushed to Parquet files.
    pub rows_flushed: AtomicU64,

    /// Total bytes written to storage (estimated from `RecordBatch` sizes).
    pub bytes_written: AtomicU64,

    /// Total number of Parquet flush operations.
    pub flush_count: AtomicU64,

    /// Total number of Iceberg snapshot commits (epoch commits).
    pub commits: AtomicU64,

    /// Total errors encountered.
    pub errors_total: AtomicU64,

    /// Total epochs rolled back.
    pub epochs_rolled_back: AtomicU64,

    /// Total data files written (Parquet files).
    pub data_files_written: AtomicU64,

    /// Total equality delete files written (upsert mode).
    pub delete_files_written: AtomicU64,

    /// Total changelog deletes processed (Z-set weight -1).
    pub changelog_deletes: AtomicU64,

    /// Last Iceberg snapshot ID committed.
    pub last_snapshot_id: AtomicI64,

    /// Current Iceberg table version (sequential commit count).
    pub table_version: AtomicU64,
}

impl IcebergSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows_flushed: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            epochs_rolled_back: AtomicU64::new(0),
            data_files_written: AtomicU64::new(0),
            delete_files_written: AtomicU64::new(0),
            changelog_deletes: AtomicU64::new(0),
            last_snapshot_id: AtomicI64::new(0),
            table_version: AtomicU64::new(0),
        }
    }

    /// Records a successful flush of `records` rows totaling `bytes`.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.rows_flushed.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records data files written during a flush.
    pub fn record_data_files(&self, count: u64) {
        self.data_files_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Records equality delete files written (upsert mode).
    pub fn record_delete_files(&self, count: u64) {
        self.delete_files_written.fetch_add(count, Ordering::Relaxed);
    }

    /// Records a successful epoch commit with the snapshot ID.
    pub fn record_commit(&self, snapshot_id: i64) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.last_snapshot_id.store(snapshot_id, Ordering::Relaxed);
        self.table_version.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a write or I/O error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Records changelog DELETE operations processed.
    pub fn record_deletes(&self, count: u64) {
        self.changelog_deletes.fetch_add(count, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.rows_flushed.load(Ordering::Relaxed),
            bytes_total: self.bytes_written.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "iceberg.flush_count",
            self.flush_count.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "iceberg.commits",
            self.commits.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "iceberg.epochs_rolled_back",
            self.epochs_rolled_back.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "iceberg.data_files_written",
            self.data_files_written.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "iceberg.delete_files_written",
            self.delete_files_written.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "iceberg.changelog_deletes",
            self.changelog_deletes.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "iceberg.last_snapshot_id",
            self.last_snapshot_id.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "iceberg.table_version",
            self.table_version.load(Ordering::Relaxed) as f64,
        );
        m
    }

    /// Returns a snapshot of all metric values.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            rows_flushed: self.rows_flushed.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            commits: self.commits.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            epochs_rolled_back: self.epochs_rolled_back.load(Ordering::Relaxed),
            data_files_written: self.data_files_written.load(Ordering::Relaxed),
            delete_files_written: self.delete_files_written.load(Ordering::Relaxed),
            changelog_deletes: self.changelog_deletes.load(Ordering::Relaxed),
            last_snapshot_id: self.last_snapshot_id.load(Ordering::Relaxed),
            table_version: self.table_version.load(Ordering::Relaxed),
        }
    }
}

impl Default for IcebergSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of all Iceberg sink metrics.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Total rows flushed to Parquet files.
    pub rows_flushed: u64,
    /// Total bytes written to storage.
    pub bytes_written: u64,
    /// Total number of Parquet flush operations.
    pub flush_count: u64,
    /// Total number of Iceberg snapshot commits.
    pub commits: u64,
    /// Total errors encountered.
    pub errors_total: u64,
    /// Total epochs rolled back.
    pub epochs_rolled_back: u64,
    /// Total data files written.
    pub data_files_written: u64,
    /// Total equality delete files written.
    pub delete_files_written: u64,
    /// Total changelog deletes processed.
    pub changelog_deletes: u64,
    /// Last Iceberg snapshot ID committed.
    pub last_snapshot_id: i64,
    /// Current table version.
    pub table_version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = IcebergSinkMetrics::new();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_flush() {
        let m = IcebergSinkMetrics::new();
        m.record_flush(100, 5000);
        m.record_flush(200, 10_000);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 300);
        assert_eq!(cm.bytes_total, 15_000);

        let flushes = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.flush_count");
        assert_eq!(flushes.unwrap().1, 2.0);
    }

    #[test]
    fn test_record_commit() {
        let m = IcebergSinkMetrics::new();
        m.record_commit(12345);
        m.record_commit(12350);

        let cm = m.to_connector_metrics();
        let commits = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.commits");
        assert_eq!(commits.unwrap().1, 2.0);

        let snapshot_id = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.last_snapshot_id");
        assert_eq!(snapshot_id.unwrap().1, 12350.0);

        let version = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.table_version");
        assert_eq!(version.unwrap().1, 2.0);
    }

    #[test]
    fn test_error_counting() {
        let m = IcebergSinkMetrics::new();
        m.record_error();
        m.record_error();
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 3);
    }

    #[test]
    fn test_rollback_counting() {
        let m = IcebergSinkMetrics::new();
        m.record_rollback();
        m.record_rollback();

        let cm = m.to_connector_metrics();
        let rolled_back = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.epochs_rolled_back");
        assert_eq!(rolled_back.unwrap().1, 2.0);
    }

    #[test]
    fn test_data_files_counting() {
        let m = IcebergSinkMetrics::new();
        m.record_data_files(3);
        m.record_data_files(2);

        let cm = m.to_connector_metrics();
        let data_files = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.data_files_written");
        assert_eq!(data_files.unwrap().1, 5.0);
    }

    #[test]
    fn test_delete_files_counting() {
        let m = IcebergSinkMetrics::new();
        m.record_delete_files(1);
        m.record_delete_files(2);

        let cm = m.to_connector_metrics();
        let delete_files = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.delete_files_written");
        assert_eq!(delete_files.unwrap().1, 3.0);
    }

    #[test]
    fn test_changelog_deletes() {
        let m = IcebergSinkMetrics::new();
        m.record_deletes(50);
        m.record_deletes(30);

        let cm = m.to_connector_metrics();
        let deletes = cm
            .custom
            .iter()
            .find(|(k, _)| k == "iceberg.changelog_deletes");
        assert_eq!(deletes.unwrap().1, 80.0);
    }

    #[test]
    fn test_snapshot() {
        let m = IcebergSinkMetrics::new();
        m.record_flush(100, 5000);
        m.record_commit(99999);
        m.record_data_files(2);
        m.record_delete_files(1);
        m.record_error();
        m.record_rollback();
        m.record_deletes(10);

        let snap = m.snapshot();
        assert_eq!(snap.rows_flushed, 100);
        assert_eq!(snap.bytes_written, 5000);
        assert_eq!(snap.flush_count, 1);
        assert_eq!(snap.commits, 1);
        assert_eq!(snap.errors_total, 1);
        assert_eq!(snap.epochs_rolled_back, 1);
        assert_eq!(snap.data_files_written, 2);
        assert_eq!(snap.delete_files_written, 1);
        assert_eq!(snap.changelog_deletes, 10);
        assert_eq!(snap.last_snapshot_id, 99999);
        assert_eq!(snap.table_version, 1);
    }

    #[test]
    fn test_default() {
        let m = IcebergSinkMetrics::default();
        let snap = m.snapshot();
        assert_eq!(snap.rows_flushed, 0);
        assert_eq!(snap.commits, 0);
        assert_eq!(snap.last_snapshot_id, 0);
    }
}
