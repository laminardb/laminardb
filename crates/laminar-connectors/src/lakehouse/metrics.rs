//! Common metrics for Lakehouse sink connectors.
//!
//! Provides shared prometheus-backed counters for tracking statistics across
//! different table formats (Delta Lake, Iceberg, Hudi, Paimon).

use prometheus::{IntCounter, Registry};

use crate::prom::reg_or_local;

/// Prometheus-backed counters for Lakehouse sink connector statistics.
#[derive(Debug, Clone)]
pub struct LakehouseSinkMetrics {
    /// Total rows flushed to storage (Parquet/ORC/etc.).
    pub rows_flushed: IntCounter,

    /// Total bytes written to storage.
    pub bytes_written: IntCounter,

    /// Total number of flush operations.
    pub flush_count: IntCounter,

    /// Total number of commits (transactions/snapshots).
    pub commits: IntCounter,

    /// Total errors encountered.
    pub errors_total: IntCounter,

    /// Total epochs rolled back.
    pub epochs_rolled_back: IntCounter,

    /// Total changelog DELETE operations processed.
    pub changelog_deletes: IntCounter,
}

impl LakehouseSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);

        Self {
            rows_flushed: reg.counter(
                "lakehouse_sink_rows_flushed_total",
                "Total rows flushed to storage",
            ),
            bytes_written: reg.counter(
                "lakehouse_sink_bytes_written_total",
                "Total bytes written to storage",
            ),
            flush_count: reg.counter("lakehouse_sink_flush_count_total", "Total flush operations"),
            commits: reg.counter(
                "lakehouse_sink_commits_total",
                "Total commits (transactions/snapshots)",
            ),
            errors_total: reg.counter("lakehouse_sink_errors_total", "Total lakehouse sink errors"),
            epochs_rolled_back: reg.counter(
                "lakehouse_sink_epochs_rolled_back_total",
                "Total epochs rolled back",
            ),
            changelog_deletes: reg.counter(
                "lakehouse_sink_changelog_deletes_total",
                "Total changelog DELETE operations",
            ),
        }
    }

    /// Records a successful flush of `records` rows totaling `bytes`.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.rows_flushed.inc_by(records);
        self.bytes_written.inc_by(bytes);
        self.flush_count.inc();
    }

    /// Records a successful commit (transaction/snapshot).
    pub fn record_commit(&self) {
        self.commits.inc();
    }

    /// Records a write or I/O error.
    pub fn record_error(&self) {
        self.errors_total.inc();
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.inc();
    }

    /// Records changelog DELETE operations processed.
    pub fn record_deletes(&self, count: u64) {
        self.changelog_deletes.inc_by(count);
    }
}

impl Default for LakehouseSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}
