//! Delta Lake sink connector metrics.
//!
//! [`DeltaLakeSinkMetrics`] provides prometheus-backed counters and gauges
//! for tracking write statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use prometheus::{IntCounter, IntGauge, Registry};

use super::metrics::LakehouseSinkMetrics;
use crate::metrics::ConnectorMetrics;

/// Prometheus-backed counters/gauges for Delta Lake sink connector statistics.
#[derive(Debug, Clone)]
pub struct DeltaLakeSinkMetrics {
    /// Common metrics (rows flushed, bytes written, commits, etc.).
    pub common: LakehouseSinkMetrics,

    /// Total MERGE operations (upsert mode).
    pub merge_operations: IntCounter,

    /// Last Delta Lake table version committed.
    pub last_delta_version: IntGauge,

    /// Total compaction runs completed.
    pub compaction_runs: IntCounter,

    /// Total files added by compaction.
    pub compaction_files_added: IntCounter,

    /// Total files removed by compaction.
    pub compaction_files_removed: IntCounter,

    /// Total files deleted by vacuum.
    pub vacuum_files_deleted: IntCounter,
}

impl DeltaLakeSinkMetrics {
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

        let merge_operations = IntCounter::new(
            "delta_sink_merge_operations_total",
            "Total MERGE operations (upsert)",
        )
        .unwrap();
        let last_delta_version = IntGauge::new(
            "delta_sink_last_version",
            "Last committed Delta table version",
        )
        .unwrap();
        let compaction_runs =
            IntCounter::new("delta_sink_compaction_runs_total", "Total compaction runs").unwrap();
        let compaction_files_added = IntCounter::new(
            "delta_sink_compaction_files_added_total",
            "Total files added by compaction",
        )
        .unwrap();
        let compaction_files_removed = IntCounter::new(
            "delta_sink_compaction_files_removed_total",
            "Total files removed by compaction",
        )
        .unwrap();
        let vacuum_files_deleted = IntCounter::new(
            "delta_sink_vacuum_files_deleted_total",
            "Total files deleted by vacuum",
        )
        .unwrap();

        let _ = reg.register(Box::new(merge_operations.clone()));
        let _ = reg.register(Box::new(last_delta_version.clone()));
        let _ = reg.register(Box::new(compaction_runs.clone()));
        let _ = reg.register(Box::new(compaction_files_added.clone()));
        let _ = reg.register(Box::new(compaction_files_removed.clone()));
        let _ = reg.register(Box::new(vacuum_files_deleted.clone()));

        Self {
            common: LakehouseSinkMetrics::new(registry),
            merge_operations,
            last_delta_version,
            compaction_runs,
            compaction_files_added,
            compaction_files_removed,
            vacuum_files_deleted,
        }
    }

    /// Records a successful flush of `records` rows totaling `bytes`.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.common.record_flush(records, bytes);
    }

    /// Records a successful epoch commit.
    #[allow(clippy::cast_possible_wrap)]
    pub fn record_commit(&self, delta_version: u64) {
        self.common.record_commit();
        self.last_delta_version.set(delta_version as i64);
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
        self.merge_operations.inc();
    }

    /// Records changelog DELETE operations.
    pub fn record_deletes(&self, count: u64) {
        self.common.record_deletes(count);
    }

    /// Records a completed compaction run.
    pub fn record_compaction(&self, files_added: u64, files_removed: u64) {
        self.compaction_runs.inc();
        self.compaction_files_added.inc_by(files_added);
        self.compaction_files_removed.inc_by(files_removed);
    }

    /// Records files deleted by vacuum.
    pub fn record_vacuum(&self, files_deleted: u64) {
        self.vacuum_files_deleted.inc_by(files_deleted);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        self.common.populate_metrics(&mut m, "delta");

        m.add_custom("delta.merge_operations", self.merge_operations.get() as f64);
        m.add_custom("delta.last_version", self.last_delta_version.get() as f64);
        m.add_custom("delta.compaction_runs", self.compaction_runs.get() as f64);
        m.add_custom(
            "delta.compaction_files_added",
            self.compaction_files_added.get() as f64,
        );
        m.add_custom(
            "delta.compaction_files_removed",
            self.compaction_files_removed.get() as f64,
        );
        m.add_custom(
            "delta.vacuum_files_deleted",
            self.vacuum_files_deleted.get() as f64,
        );
        m
    }
}

impl Default for DeltaLakeSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = DeltaLakeSinkMetrics::new(None);
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_flush() {
        let m = DeltaLakeSinkMetrics::new(None);
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
        let m = DeltaLakeSinkMetrics::new(None);
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
        let m = DeltaLakeSinkMetrics::new(None);
        m.record_error();
        m.record_error();
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 3);
    }

    #[test]
    fn test_rollback_counting() {
        let m = DeltaLakeSinkMetrics::new(None);
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
        let m = DeltaLakeSinkMetrics::new(None);
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
        let m = DeltaLakeSinkMetrics::new(None);
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
