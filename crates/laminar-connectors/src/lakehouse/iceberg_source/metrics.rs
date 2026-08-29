use prometheus::{Gauge, Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};

use crate::prom::reg_or_local;

#[derive(Debug, Clone)]
pub(super) struct IcebergSourceMetrics {
    pub(super) current_snapshot_id: IntGauge,
    pub(super) processed_snapshot_id: IntGauge,
    pub(super) snapshot_lag: IntGauge,
    pub(super) sequence_lag: IntGauge,
    pub(super) snapshot_count: IntGauge,
    pub(super) data_file_count: IntGauge,
    pub(super) delete_file_count: IntGauge,
    pub(super) delete_to_data_file_ratio: Gauge,
    pub(super) poll_duration: Histogram,
    pub(super) planning_duration: Histogram,
    pub(super) read_duration: Histogram,
    pub(super) planned_files: IntCounter,
    pub(super) planned_manifests: IntCounter,
    pub(super) read_files: IntCounter,
    pub(super) read_rows: IntCounter,
    pub(super) read_bytes: IntCounter,
    pub(super) read_storage_bytes: IntCounter,
    pub(super) credential_refresh_failures: IntCounter,
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
        let gauge = |name: &str, help: &str| {
            let metric = Gauge::with_opts(Opts::new(name, help))
                .unwrap_or_else(|error| panic!("invalid Iceberg source gauge '{name}': {error}"));
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
            snapshot_count: handle.gauge(
                "iceberg_source_snapshot_count",
                "Snapshots currently retained in Iceberg table metadata",
            ),
            data_file_count: handle.gauge(
                "iceberg_source_data_file_count",
                "Data files in the selected Iceberg snapshot, or -1 when unavailable",
            ),
            delete_file_count: handle.gauge(
                "iceberg_source_delete_file_count",
                "Delete files in the selected Iceberg snapshot, or -1 when unavailable",
            ),
            delete_to_data_file_ratio: gauge(
                "iceberg_source_delete_to_data_file_ratio",
                "Delete-file to data-file ratio in the selected snapshot; NaN when unavailable",
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
            planned_manifests: handle.counter(
                "iceberg_source_planned_manifests_total",
                "Iceberg manifests examined by append planning",
            ),
            read_files: handle.counter(
                "iceberg_source_read_files_total",
                "Iceberg data-file scan tasks completed by successful scans",
            ),
            read_rows: handle.counter(
                "iceberg_source_read_rows_total",
                "Rows read from Iceberg data files",
            ),
            read_bytes: handle.counter(
                "iceberg_source_read_arrow_bytes_total",
                "Arrow memory bytes read from Iceberg data files",
            ),
            read_storage_bytes: handle.counter(
                "iceberg_source_read_storage_bytes_total",
                "Object bytes read from Iceberg data and delete files by successful scans",
            ),
            credential_refresh_failures: handle.counter(
                "iceberg_source_credential_refresh_failures_total",
                "Failed proactive Iceberg catalog credential refreshes",
            ),
        }
    }

    pub(super) fn observe_completed_read(&self, files: u64, storage_bytes: u64) {
        self.read_files.inc_by(files);
        self.read_storage_bytes.inc_by(storage_bytes);
    }

    pub(super) fn observe_batch(&self, batch: &arrow_array::RecordBatch) {
        self.read_rows
            .inc_by(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        self.read_bytes
            .inc_by(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX));
    }

    pub(super) fn observe_table(&self, metadata: &iceberg::spec::TableMetadata, table_ref: &str) {
        self.snapshot_count
            .set(i64::try_from(metadata.snapshots().len()).unwrap_or(i64::MAX));
        let Some(snapshot) = metadata.snapshot_for_ref(table_ref) else {
            self.data_file_count.set(0);
            self.delete_file_count.set(0);
            self.delete_to_data_file_ratio.set(0.0);
            return;
        };
        let properties = &snapshot.summary().additional_properties;
        let data_files = summary_count(properties, "total-data-files");
        let delete_files = summary_count(properties, "total-delete-files");
        self.data_file_count.set(data_files.unwrap_or(-1));
        self.delete_file_count.set(delete_files.unwrap_or(-1));
        self.delete_to_data_file_ratio
            .set(file_ratio(data_files, delete_files));
    }
}

fn summary_count(properties: &std::collections::HashMap<String, String>, key: &str) -> Option<i64> {
    properties
        .get(key)?
        .parse::<u64>()
        .ok()
        .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
}

#[allow(clippy::cast_precision_loss)] // Prometheus gauges expose ratios as f64.
fn file_ratio(data_files: Option<i64>, delete_files: Option<i64>) -> f64 {
    match (data_files, delete_files) {
        (Some(0), Some(0)) => 0.0,
        (Some(0), Some(_)) => f64::INFINITY,
        (Some(data), Some(deletes)) => deletes as f64 / data as f64,
        _ => f64::NAN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    #[tokio::test]
    async fn table_inventory_uses_the_selected_snapshot_summary() {
        let fixture = create_test_table(false).await;
        let (table, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("observed"))]).await;
        let metrics = IcebergSourceMetrics::new(None);
        metrics.observe_table(table.metadata(), "main");

        assert_eq!(metrics.snapshot_count.get(), 1);
        assert_eq!(metrics.data_file_count.get(), 1);
        assert_eq!(metrics.delete_file_count.get(), 0);
        assert_eq!(metrics.delete_to_data_file_ratio.get(), 0.0);
    }
}
