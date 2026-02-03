//! Delta Lake sink connector metrics.
//!
//! [`DeltaLakeSinkMetrics`] provides lock-free atomic counters for
//! tracking write statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for Delta Lake sink connector statistics.
#[derive(Debug)]
pub struct DeltaLakeSinkMetrics {
    /// Total rows flushed to Parquet files.
    pub rows_flushed: AtomicU64,

    /// Total bytes written to storage (estimated from `RecordBatch` sizes).
    pub bytes_written: AtomicU64,

    /// Total number of Parquet flush operations.
    pub flush_count: AtomicU64,

    /// Total number of Delta Lake commits (epoch commits).
    pub commits: AtomicU64,

    /// Total errors encountered.
    pub errors_total: AtomicU64,

    /// Total epochs rolled back.
    pub epochs_rolled_back: AtomicU64,

    /// Total MERGE operations (upsert mode).
    pub merge_operations: AtomicU64,

    /// Total changelog deletes applied (Z-set weight -1).
    pub changelog_deletes: AtomicU64,

    /// Last Delta Lake table version committed.
    pub last_delta_version: AtomicU64,
}

impl DeltaLakeSinkMetrics {
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
            merge_operations: AtomicU64::new(0),
            changelog_deletes: AtomicU64::new(0),
            last_delta_version: AtomicU64::new(0),
        }
    }

    /// Records a successful flush of `records` rows totaling `bytes`.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.rows_flushed.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful epoch commit.
    pub fn record_commit(&self, delta_version: u64) {
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.last_delta_version
            .store(delta_version, Ordering::Relaxed);
    }

    /// Records a write or I/O error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a MERGE operation (upsert mode).
    pub fn record_merge(&self) {
        self.merge_operations.fetch_add(1, Ordering::Relaxed);
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
            records_total: self.rows_flushed.load(Ordering::Relaxed),
            bytes_total: self.bytes_written.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "delta.flush_count",
            self.flush_count.load(Ordering::Relaxed) as f64,
        );
        m.add_custom("delta.commits", self.commits.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "delta.epochs_rolled_back",
            self.epochs_rolled_back.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.merge_operations",
            self.merge_operations.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.changelog_deletes",
            self.changelog_deletes.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.last_version",
            self.last_delta_version.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for DeltaLakeSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = DeltaLakeSinkMetrics::new();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_flush() {
        let m = DeltaLakeSinkMetrics::new();
        m.record_flush(100, 5000);
        m.record_flush(200, 10_000);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 300);
        assert_eq!(cm.bytes_total, 15_000);

        let flushes = cm.custom.iter().find(|(k, _)| k == "delta.flush_count");
        assert_eq!(flushes.unwrap().1, 2.0);
    }

    #[test]
    fn test_record_commit() {
        let m = DeltaLakeSinkMetrics::new();
        m.record_commit(1);
        m.record_commit(5);

        let cm = m.to_connector_metrics();
        let commits = cm.custom.iter().find(|(k, _)| k == "delta.commits");
        assert_eq!(commits.unwrap().1, 2.0);

        let version = cm.custom.iter().find(|(k, _)| k == "delta.last_version");
        assert_eq!(version.unwrap().1, 5.0);
    }

    #[test]
    fn test_error_counting() {
        let m = DeltaLakeSinkMetrics::new();
        m.record_error();
        m.record_error();
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 3);
    }

    #[test]
    fn test_rollback_counting() {
        let m = DeltaLakeSinkMetrics::new();
        m.record_rollback();
        m.record_rollback();

        let cm = m.to_connector_metrics();
        let rolled_back = cm
            .custom
            .iter()
            .find(|(k, _)| k == "delta.epochs_rolled_back");
        assert_eq!(rolled_back.unwrap().1, 2.0);
    }

    #[test]
    fn test_merge_operations() {
        let m = DeltaLakeSinkMetrics::new();
        m.record_merge();

        let cm = m.to_connector_metrics();
        let merges = cm
            .custom
            .iter()
            .find(|(k, _)| k == "delta.merge_operations");
        assert_eq!(merges.unwrap().1, 1.0);
    }

    #[test]
    fn test_changelog_deletes() {
        let m = DeltaLakeSinkMetrics::new();
        m.record_deletes(50);
        m.record_deletes(30);

        let cm = m.to_connector_metrics();
        let deletes = cm
            .custom
            .iter()
            .find(|(k, _)| k == "delta.changelog_deletes");
        assert_eq!(deletes.unwrap().1, 80.0);
    }
}
