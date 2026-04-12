//! `PostgreSQL` sink connector metrics.
//!
//! [`PostgresSinkMetrics`] provides prometheus-backed counters for
//! tracking write statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use prometheus::{IntCounter, Registry};

use crate::metrics::ConnectorMetrics;

/// Prometheus-backed counters for `PostgreSQL` sink connector statistics.
#[derive(Debug, Clone)]
pub struct PostgresSinkMetrics {
    /// Total records written to `PostgreSQL`.
    pub records_written: IntCounter,

    /// Total bytes written (estimated from `RecordBatch` sizes).
    pub bytes_written: IntCounter,

    /// Total errors encountered.
    pub errors_total: IntCounter,

    /// Total batches flushed.
    pub batches_flushed: IntCounter,

    /// Total COPY BINARY operations (append mode).
    pub copy_operations: IntCounter,

    /// Total upsert operations (upsert mode).
    pub upsert_operations: IntCounter,

    /// Total epochs committed (exactly-once).
    pub epochs_committed: IntCounter,

    /// Total epochs rolled back.
    pub epochs_rolled_back: IntCounter,

    /// Total changelog deletes applied (Z-set weight -1).
    pub changelog_deletes: IntCounter,
}

impl PostgresSinkMetrics {
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

        let records_written = IntCounter::new(
            "postgres_sink_records_written_total",
            "Total records written to PostgreSQL",
        )
        .unwrap();
        let bytes_written = IntCounter::new(
            "postgres_sink_bytes_written_total",
            "Total bytes written to PostgreSQL",
        )
        .unwrap();
        let errors_total =
            IntCounter::new("postgres_sink_errors_total", "Total PostgreSQL sink errors").unwrap();
        let batches_flushed = IntCounter::new(
            "postgres_sink_batches_flushed_total",
            "Total batches flushed",
        )
        .unwrap();
        let copy_operations = IntCounter::new(
            "postgres_sink_copy_operations_total",
            "Total COPY BINARY operations",
        )
        .unwrap();
        let upsert_operations = IntCounter::new(
            "postgres_sink_upsert_operations_total",
            "Total upsert operations",
        )
        .unwrap();
        let epochs_committed = IntCounter::new(
            "postgres_sink_epochs_committed_total",
            "Total epochs committed",
        )
        .unwrap();
        let epochs_rolled_back = IntCounter::new(
            "postgres_sink_epochs_rolled_back_total",
            "Total epochs rolled back",
        )
        .unwrap();
        let changelog_deletes = IntCounter::new(
            "postgres_sink_changelog_deletes_total",
            "Total changelog deletes applied",
        )
        .unwrap();

        let _ = reg.register(Box::new(records_written.clone()));
        let _ = reg.register(Box::new(bytes_written.clone()));
        let _ = reg.register(Box::new(errors_total.clone()));
        let _ = reg.register(Box::new(batches_flushed.clone()));
        let _ = reg.register(Box::new(copy_operations.clone()));
        let _ = reg.register(Box::new(upsert_operations.clone()));
        let _ = reg.register(Box::new(epochs_committed.clone()));
        let _ = reg.register(Box::new(epochs_rolled_back.clone()));
        let _ = reg.register(Box::new(changelog_deletes.clone()));

        Self {
            records_written,
            bytes_written,
            errors_total,
            batches_flushed,
            copy_operations,
            upsert_operations,
            epochs_committed,
            epochs_rolled_back,
            changelog_deletes,
        }
    }

    /// Records a successful write of `records` records totaling `bytes`.
    pub fn record_write(&self, records: u64, bytes: u64) {
        self.records_written.inc_by(records);
        self.bytes_written.inc_by(bytes);
    }

    /// Records a successful batch flush.
    pub fn record_flush(&self) {
        self.batches_flushed.inc();
    }

    /// Records a COPY BINARY operation.
    pub fn record_copy(&self) {
        self.copy_operations.inc();
    }

    /// Records an upsert operation.
    pub fn record_upsert(&self) {
        self.upsert_operations.inc();
    }

    /// Records a write or connection error.
    pub fn record_error(&self) {
        self.errors_total.inc();
    }

    /// Records a successful epoch commit.
    pub fn record_commit(&self) {
        self.epochs_committed.inc();
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.inc();
    }

    /// Records changelog DELETE operations.
    pub fn record_deletes(&self, count: u64) {
        self.changelog_deletes.inc_by(count);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_written.get(),
            bytes_total: self.bytes_written.get(),
            errors_total: self.errors_total.get(),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom("pg.batches_flushed", self.batches_flushed.get() as f64);
        m.add_custom("pg.copy_operations", self.copy_operations.get() as f64);
        m.add_custom("pg.upsert_operations", self.upsert_operations.get() as f64);
        m.add_custom("pg.epochs_committed", self.epochs_committed.get() as f64);
        m.add_custom(
            "pg.epochs_rolled_back",
            self.epochs_rolled_back.get() as f64,
        );
        m.add_custom("pg.changelog_deletes", self.changelog_deletes.get() as f64);
        m
    }
}

impl Default for PostgresSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = PostgresSinkMetrics::new(None);
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_write() {
        let m = PostgresSinkMetrics::new(None);
        m.record_write(100, 5000);
        m.record_write(200, 10_000);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 300);
        assert_eq!(cm.bytes_total, 15_000);
    }

    #[test]
    fn test_flush_and_copy_metrics() {
        let m = PostgresSinkMetrics::new(None);
        m.record_flush();
        m.record_flush();
        m.record_copy();

        let cm = m.to_connector_metrics();
        let flushed = cm.custom.iter().find(|(k, _)| k == "pg.batches_flushed");
        assert_eq!(flushed.unwrap().1, 2.0);
        let copies = cm.custom.iter().find(|(k, _)| k == "pg.copy_operations");
        assert_eq!(copies.unwrap().1, 1.0);
    }

    #[test]
    fn test_epoch_metrics() {
        let m = PostgresSinkMetrics::new(None);
        m.record_commit();
        m.record_commit();
        m.record_rollback();

        let cm = m.to_connector_metrics();
        let committed = cm.custom.iter().find(|(k, _)| k == "pg.epochs_committed");
        assert_eq!(committed.unwrap().1, 2.0);
        let rolled_back = cm.custom.iter().find(|(k, _)| k == "pg.epochs_rolled_back");
        assert_eq!(rolled_back.unwrap().1, 1.0);
    }

    #[test]
    fn test_changelog_deletes() {
        let m = PostgresSinkMetrics::new(None);
        m.record_deletes(50);
        m.record_deletes(30);

        let cm = m.to_connector_metrics();
        let deletes = cm.custom.iter().find(|(k, _)| k == "pg.changelog_deletes");
        assert_eq!(deletes.unwrap().1, 80.0);
    }

    #[test]
    fn test_error_counting() {
        let m = PostgresSinkMetrics::new(None);
        m.record_error();
        m.record_error();
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 3);
    }

    #[test]
    fn test_upsert_metric() {
        let m = PostgresSinkMetrics::new(None);
        m.record_upsert();

        let cm = m.to_connector_metrics();
        let upserts = cm.custom.iter().find(|(k, _)| k == "pg.upsert_operations");
        assert_eq!(upserts.unwrap().1, 1.0);
    }
}
