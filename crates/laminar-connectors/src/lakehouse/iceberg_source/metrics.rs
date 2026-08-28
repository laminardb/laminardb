use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Registry};

use crate::prom::reg_or_local;

#[derive(Debug, Clone)]
pub(super) struct IcebergSourceMetrics {
    pub(super) current_snapshot_id: IntGauge,
    pub(super) processed_snapshot_id: IntGauge,
    pub(super) snapshot_lag: IntGauge,
    pub(super) sequence_lag: IntGauge,
    pub(super) poll_duration: Histogram,
    pub(super) planning_duration: Histogram,
    pub(super) read_duration: Histogram,
    pub(super) planned_files: IntCounter,
    pub(super) read_rows: IntCounter,
    pub(super) read_bytes: IntCounter,
}

impl IcebergSourceMetrics {
    pub(super) fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let handle = reg_or_local(registry, &mut local);
        let histogram = |name: &str, help: &str| {
            let metric = Histogram::with_opts(
                HistogramOpts::new(name, help)
                    .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
            )
            .unwrap_or_else(|error| panic!("invalid Iceberg source histogram '{name}': {error}"));
            handle.registry().register(Box::new(metric.clone())).ok();
            metric
        };

        Self {
            current_snapshot_id: handle.gauge(
                "iceberg_source_current_snapshot_id",
                "Current snapshot ID on the configured Iceberg ref",
            ),
            processed_snapshot_id: handle.gauge(
                "iceberg_source_processed_snapshot_id",
                "Last completely emitted Iceberg snapshot ID",
            ),
            snapshot_lag: handle.gauge(
                "iceberg_source_snapshot_lag",
                "Planned Iceberg snapshots not yet completely emitted",
            ),
            sequence_lag: handle.gauge(
                "iceberg_source_sequence_lag",
                "Sequence-number distance from the configured Iceberg ref",
            ),
            poll_duration: histogram(
                "iceberg_source_poll_duration_seconds",
                "Iceberg append catalog poll duration",
            ),
            planning_duration: histogram(
                "iceberg_source_planning_duration_seconds",
                "Iceberg append lineage and manifest planning duration",
            ),
            read_duration: histogram(
                "iceberg_source_read_duration_seconds",
                "Iceberg snapshot scan duration",
            ),
            planned_files: handle.counter(
                "iceberg_source_planned_files_total",
                "Iceberg data files admitted by append planning",
            ),
            read_rows: handle.counter(
                "iceberg_source_read_rows_total",
                "Rows read from Iceberg data files",
            ),
            read_bytes: handle.counter(
                "iceberg_source_read_arrow_bytes_total",
                "Arrow memory bytes read from Iceberg data files",
            ),
        }
    }

    pub(super) fn observe_batch(&self, batch: &arrow_array::RecordBatch) {
        self.read_rows
            .inc_by(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        self.read_bytes
            .inc_by(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX));
    }
}
