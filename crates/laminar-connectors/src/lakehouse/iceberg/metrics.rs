//! Low-cardinality Iceberg connector metrics.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};

use crate::prom::reg_or_local;

#[derive(Debug, Clone)]
pub(super) struct IcebergMetrics {
    pub(super) buffered_rows: IntGauge,
    pub(super) buffered_bytes: IntGauge,
    pub(super) active_partition_writers: IntGauge,
    pub(super) file_size_bytes: Histogram,
    pub(super) file_rows: Histogram,
    pub(super) pre_commit_duration: Histogram,
    pub(super) publication_duration: Histogram,
    pub(super) reconciliation_duration: Histogram,
    pub(super) commit_conflicts: IntCounter,
    pub(super) commit_retries: IntCounter,
    pub(super) unknown_outcomes: IntCounter,
    pub(super) committed_checkpoint: IntGauge,
    pub(super) last_successful_commit_timestamp: IntGauge,
}

impl IcebergMetrics {
    pub(super) fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let handle = reg_or_local(registry, &mut local);
        let histogram = |name: &str, help: &str, buckets: Vec<f64>| {
            let metric = Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))
                .unwrap_or_else(|error| panic!("invalid Iceberg histogram '{name}': {error}"));
            handle.registry().register(Box::new(metric.clone())).ok();
            metric
        };

        Self {
            buffered_rows: handle.gauge(
                "iceberg_sink_buffered_rows",
                "Rows in the current bounded Iceberg write call",
            ),
            buffered_bytes: handle.gauge(
                "iceberg_sink_buffered_bytes",
                "Arrow bytes in the current bounded Iceberg write call",
            ),
            active_partition_writers: handle.gauge(
                "iceberg_sink_active_partition_writers",
                "Open Iceberg partition writers",
            ),
            file_size_bytes: histogram(
                "iceberg_sink_file_size_bytes",
                "Completed Iceberg data file sizes",
                prometheus::exponential_buckets(1_048_576.0, 2.0, 12)
                    .unwrap_or_else(|error| panic!("invalid file-size buckets: {error}")),
            ),
            file_rows: histogram(
                "iceberg_sink_file_rows",
                "Completed Iceberg data file row counts",
                prometheus::exponential_buckets(1_000.0, 2.0, 12)
                    .unwrap_or_else(|error| panic!("invalid file-row buckets: {error}")),
            ),
            pre_commit_duration: histogram(
                "iceberg_sink_pre_commit_duration_seconds",
                "Iceberg participant pre-commit duration",
                vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
            ),
            publication_duration: histogram(
                "iceberg_sink_publication_duration_seconds",
                "Iceberg coordinated publication duration",
                vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
            ),
            reconciliation_duration: histogram(
                "iceberg_sink_reconciliation_duration_seconds",
                "Iceberg unknown-outcome reconciliation duration",
                vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
            ),
            commit_conflicts: handle.counter(
                "iceberg_sink_commit_conflicts_total",
                "Iceberg optimistic commit conflicts",
            ),
            commit_retries: handle.counter(
                "iceberg_sink_commit_retries_total",
                "Iceberg coordinated publication retries",
            ),
            unknown_outcomes: handle.counter(
                "iceberg_sink_unknown_outcomes_total",
                "Iceberg publications with an initially unknown outcome",
            ),
            committed_checkpoint: handle.gauge(
                "iceberg_sink_committed_checkpoint",
                "Highest reconciled LaminarDB checkpoint in Iceberg metadata",
            ),
            last_successful_commit_timestamp: handle.gauge(
                "iceberg_sink_last_successful_commit_timestamp_seconds",
                "Unix timestamp of the latest successful Iceberg publication",
            ),
        }
    }

    pub(super) fn set_buffer(&self, rows: usize, bytes: usize) {
        self.buffered_rows
            .set(i64::try_from(rows).unwrap_or(i64::MAX));
        self.buffered_bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    pub(super) fn set_active_writers(&self, writers: usize) {
        self.active_partition_writers
            .set(i64::try_from(writers).unwrap_or(i64::MAX));
    }

    #[allow(clippy::cast_precision_loss)] // Prometheus histograms accept f64 observations.
    pub(super) fn observe_files(&self, files: &[iceberg::spec::DataFile]) {
        for file in files {
            self.file_size_bytes
                .observe(file.file_size_in_bytes() as f64);
            self.file_rows.observe(file.record_count() as f64);
        }
    }
}
