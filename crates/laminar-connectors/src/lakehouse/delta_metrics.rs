//! Prometheus-backed Delta Lake sink metrics.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};

use super::metrics::LakehouseSinkMetrics;
use crate::prom::reg_or_local;

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

    /// Total optimistic-concurrency conflicts encountered (per retry).
    pub conflicts: IntCounter,

    /// Total retry attempts kicked off (both conflict and timeout).
    pub retries: IntCounter,

    /// End-to-end flush duration histogram (concat → write → checkpoint).
    /// Buckets cover 5ms up to ~160s (0.005 * 2^15).
    pub flush_duration: Histogram,

    /// Total changelog rows entering collapse (pre-dedup, per upsert flush).
    pub collapse_rows_in: IntCounter,

    /// Total upsert rows emitted by collapse (`_op = U`).
    pub collapse_upserts_out: IntCounter,

    /// Total delete rows emitted by collapse (`_op = D`).
    pub collapse_deletes_out: IntCounter,

    /// Changelog-collapse duration histogram (per upsert flush).
    /// Buckets cover 100µs up to ~3.3s (0.0001 * 2^15).
    pub collapse_duration: Histogram,
}

impl DeltaLakeSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let handle = reg_or_local(registry, &mut local);

        let flush_duration = Histogram::with_opts(
            HistogramOpts::new(
                "delta_sink_flush_duration_seconds",
                "End-to-end Delta Lake flush duration (pre-concat → write → checkpoint)",
            )
            .buckets(prometheus::exponential_buckets(0.005, 2.0, 16).unwrap()),
        )
        .unwrap();
        // Best-effort registration (matches `kafka/metrics.rs` pattern):
        // `AlreadyReg` is benign on re-init; surface anything else so a
        // dropped histogram doesn't disappear silently.
        if let Err(e) = handle.registry().register(Box::new(flush_duration.clone())) {
            tracing::warn!(
                metric = "delta_sink_flush_duration_seconds",
                error = %e,
                "failed to register delta lake flush_duration histogram"
            );
        }

        let collapse_duration = Histogram::with_opts(
            HistogramOpts::new(
                "delta_sink_collapse_duration_seconds",
                "Changelog collapse duration per upsert flush (Z-set/CDC dedup)",
            )
            .buckets(prometheus::exponential_buckets(0.0001, 2.0, 16).unwrap()),
        )
        .unwrap();
        if let Err(e) = handle
            .registry()
            .register(Box::new(collapse_duration.clone()))
        {
            tracing::warn!(
                metric = "delta_sink_collapse_duration_seconds",
                error = %e,
                "failed to register delta lake collapse_duration histogram"
            );
        }

        Self {
            common: LakehouseSinkMetrics::new(registry),
            merge_operations: handle.counter(
                "delta_sink_merge_operations_total",
                "Total MERGE operations (upsert)",
            ),
            last_delta_version: handle.gauge(
                "delta_sink_last_version",
                "Last committed Delta table version",
            ),
            compaction_runs: handle
                .counter("delta_sink_compaction_runs_total", "Total compaction runs"),
            compaction_files_added: handle.counter(
                "delta_sink_compaction_files_added_total",
                "Total files added by compaction",
            ),
            compaction_files_removed: handle.counter(
                "delta_sink_compaction_files_removed_total",
                "Total files removed by compaction",
            ),
            vacuum_files_deleted: handle.counter(
                "delta_sink_vacuum_files_deleted_total",
                "Total files deleted by vacuum",
            ),
            conflicts: handle.counter(
                "delta_sink_conflicts_total",
                "Delta Lake optimistic-concurrency conflicts observed",
            ),
            retries: handle.counter(
                "delta_sink_retries_total",
                "Retry attempts kicked off (conflict + timeout)",
            ),
            flush_duration,
            collapse_rows_in: handle.counter(
                "delta_sink_collapse_rows_in_total",
                "Changelog rows entering collapse (pre-dedup)",
            ),
            collapse_upserts_out: handle.counter(
                "delta_sink_collapse_upserts_out_total",
                "Upsert rows emitted by collapse (_op = U)",
            ),
            collapse_deletes_out: handle.counter(
                "delta_sink_collapse_deletes_out_total",
                "Delete rows emitted by collapse (_op = D)",
            ),
            collapse_duration,
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

    /// Records an optimistic-concurrency conflict (one per retry-triggering conflict).
    pub fn record_conflict(&self) {
        self.conflicts.inc();
    }

    /// Records a retry attempt.
    pub fn record_retry(&self) {
        self.retries.inc();
    }

    /// Records a completed flush duration (seconds).
    pub fn observe_flush_duration(&self, seconds: f64) {
        self.flush_duration.observe(seconds);
    }

    /// Records one changelog-collapse pass: `rows_in` rows folded down to
    /// `upserts_out` upserts and `deletes_out` deletes in `seconds`.
    pub fn observe_collapse(&self, rows_in: u64, upserts_out: u64, deletes_out: u64, seconds: f64) {
        self.collapse_rows_in.inc_by(rows_in);
        self.collapse_upserts_out.inc_by(upserts_out);
        self.collapse_deletes_out.inc_by(deletes_out);
        self.collapse_duration.observe(seconds);
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
        assert_eq!(m.common.rows_flushed.get(), 0);
        assert_eq!(m.common.bytes_written.get(), 0);
        assert_eq!(m.common.errors_total.get(), 0);
    }

    #[test]
    fn test_record_flush() {
        let m = DeltaLakeSinkMetrics::new(None);
        m.record_flush(100, 5000);
        m.record_flush(200, 10_000);

        assert_eq!(m.common.rows_flushed.get(), 300);
        assert_eq!(m.common.bytes_written.get(), 15_000);
        assert_eq!(m.common.flush_count.get(), 2);
    }

    #[test]
    fn test_record_commit() {
        let m = DeltaLakeSinkMetrics::new(None);
        m.record_commit(1);
        m.record_commit(5);

        assert_eq!(m.common.commits.get(), 2);
        assert_eq!(m.last_delta_version.get(), 5);
    }

    #[test]
    fn test_error_counting() {
        let m = DeltaLakeSinkMetrics::new(None);
        m.record_error();
        m.record_error();
        m.record_error();

        assert_eq!(m.common.errors_total.get(), 3);
    }

    #[test]
    fn test_rollback_counting() {
        let m = DeltaLakeSinkMetrics::new(None);
        m.record_rollback();
        m.record_rollback();

        assert_eq!(m.common.epochs_rolled_back.get(), 2);
    }

    #[test]
    fn test_merge_operations() {
        let m = DeltaLakeSinkMetrics::new(None);
        m.record_merge();

        assert_eq!(m.merge_operations.get(), 1);
    }

    #[test]
    fn test_changelog_deletes() {
        let m = DeltaLakeSinkMetrics::new(None);
        m.record_deletes(50);
        m.record_deletes(30);

        assert_eq!(m.common.changelog_deletes.get(), 80);
    }
}
