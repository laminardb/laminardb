//! Delta Lake sink connector metrics.
//!
//! [`DeltaLakeSinkMetrics`] provides lock-free atomic counters for
//! tracking write statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicU64, Ordering};

use super::metrics::LakehouseSinkMetrics;
use crate::metrics::ConnectorMetrics;

/// Atomic counters for Delta Lake sink connector statistics.
#[derive(Debug)]
pub struct DeltaLakeSinkMetrics {
    /// Common metrics (rows flushed, bytes written, commits, etc.).
    pub common: LakehouseSinkMetrics,

    /// Total MERGE operations (upsert mode).
    pub merge_operations: AtomicU64,

    /// Last Delta Lake table version committed.
    pub last_delta_version: AtomicU64,

    /// Total compaction runs completed.
    pub compaction_runs: AtomicU64,

    /// Total files added by compaction.
    pub compaction_files_added: AtomicU64,

    /// Total files removed by compaction.
    pub compaction_files_removed: AtomicU64,

    /// Total files deleted by vacuum.
    pub vacuum_files_deleted: AtomicU64,
}

impl DeltaLakeSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            common: LakehouseSinkMetrics::new(),
            merge_operations: AtomicU64::new(0),
            last_delta_version: AtomicU64::new(0),
            compaction_runs: AtomicU64::new(0),
            compaction_files_added: AtomicU64::new(0),
            compaction_files_removed: AtomicU64::new(0),
            vacuum_files_deleted: AtomicU64::new(0),
        }
    }

    /// Records a successful flush of `records` rows totaling `bytes`.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.common.record_flush(records, bytes);
    }

    /// Records a successful epoch commit.
    pub fn record_commit(&self, delta_version: u64) {
        self.common.record_commit();
        self.last_delta_version
            .store(delta_version, Ordering::Relaxed);
    }

    /// Records a write or I/O error.
    pub fn record_error(&self) {
        self.common.record_error();
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.common.record_rollback();
    }

    /// Records a MERGE operation (upsert mode).
    pub fn record_merge(&self) {
        self.merge_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Records changelog DELETE operations.
    pub fn record_deletes(&self, count: u64) {
        self.common.record_deletes(count);
    }

    /// Records a completed compaction run.
    pub fn record_compaction(&self, files_added: u64, files_removed: u64) {
        self.compaction_runs.fetch_add(1, Ordering::Relaxed);
        self.compaction_files_added
            .fetch_add(files_added, Ordering::Relaxed);
        self.compaction_files_removed
            .fetch_add(files_removed, Ordering::Relaxed);
    }

    /// Records files deleted by vacuum.
    pub fn record_vacuum(&self, files_deleted: u64) {
        self.vacuum_files_deleted
            .fetch_add(files_deleted, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        self.common.populate_metrics(&mut m, "delta");

        m.add_custom(
            "delta.merge_operations",
            self.merge_operations.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.last_version",
            self.last_delta_version.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.compaction_runs",
            self.compaction_runs.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.compaction_files_added",
            self.compaction_files_added.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.compaction_files_removed",
            self.compaction_files_removed.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "delta.vacuum_files_deleted",
            self.vacuum_files_deleted.load(Ordering::Relaxed) as f64,
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
#[allow(clippy::float_cmp)]
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
