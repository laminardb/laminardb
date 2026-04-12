//! Common metrics for Lakehouse sink connectors.
//!
//! Provides shared prometheus-backed counters for tracking statistics across
//! different table formats (Delta Lake, Iceberg, Hudi, Paimon).

use prometheus::{IntCounter, Registry};

use crate::metrics::ConnectorMetrics;

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
        let local;
        let reg = if let Some(r) = registry {
            r
        } else {
            local = Registry::new();
            &local
        };

        let rows_flushed = IntCounter::new(
            "lakehouse_sink_rows_flushed_total",
            "Total rows flushed to storage",
        )
        .unwrap();
        let bytes_written = IntCounter::new(
            "lakehouse_sink_bytes_written_total",
            "Total bytes written to storage",
        )
        .unwrap();
        let flush_count =
            IntCounter::new("lakehouse_sink_flush_count_total", "Total flush operations").unwrap();
        let commits = IntCounter::new(
            "lakehouse_sink_commits_total",
            "Total commits (transactions/snapshots)",
        )
        .unwrap();
        let errors_total =
            IntCounter::new("lakehouse_sink_errors_total", "Total lakehouse sink errors").unwrap();
        let epochs_rolled_back = IntCounter::new(
            "lakehouse_sink_epochs_rolled_back_total",
            "Total epochs rolled back",
        )
        .unwrap();
        let changelog_deletes = IntCounter::new(
            "lakehouse_sink_changelog_deletes_total",
            "Total changelog DELETE operations",
        )
        .unwrap();

        let _ = reg.register(Box::new(rows_flushed.clone()));
        let _ = reg.register(Box::new(bytes_written.clone()));
        let _ = reg.register(Box::new(flush_count.clone()));
        let _ = reg.register(Box::new(commits.clone()));
        let _ = reg.register(Box::new(errors_total.clone()));
        let _ = reg.register(Box::new(epochs_rolled_back.clone()));
        let _ = reg.register(Box::new(changelog_deletes.clone()));

        Self {
            rows_flushed,
            bytes_written,
            flush_count,
            commits,
            errors_total,
            epochs_rolled_back,
            changelog_deletes,
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

    /// Populates standard `ConnectorMetrics` fields and adds common custom metrics with a prefix.
    ///
    /// The `prefix` is used for custom metrics keys, e.g., `"{prefix}.flush_count"`.
    #[allow(clippy::cast_precision_loss)]
    pub fn populate_metrics(&self, metrics: &mut ConnectorMetrics, prefix: &str) {
        metrics.records_total = self.rows_flushed.get();
        metrics.bytes_total = self.bytes_written.get();
        metrics.errors_total = self.errors_total.get();

        metrics.add_custom(
            format!("{prefix}.flush_count"),
            self.flush_count.get() as f64,
        );
        metrics.add_custom(format!("{prefix}.commits"), self.commits.get() as f64);
        metrics.add_custom(
            format!("{prefix}.epochs_rolled_back"),
            self.epochs_rolled_back.get() as f64,
        );
        metrics.add_custom(
            format!("{prefix}.changelog_deletes"),
            self.changelog_deletes.get() as f64,
        );
    }
}

impl Default for LakehouseSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}
