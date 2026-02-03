//! Kafka sink connector metrics.
//!
//! [`KafkaSinkMetrics`] provides lock-free atomic counters for
//! tracking production statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for Kafka sink connector statistics.
#[derive(Debug)]
pub struct KafkaSinkMetrics {
    /// Total records written to Kafka.
    pub records_written: AtomicU64,
    /// Total bytes written to Kafka (payload only).
    pub bytes_written: AtomicU64,
    /// Total errors encountered.
    pub errors_total: AtomicU64,
    /// Total epochs committed.
    pub epochs_committed: AtomicU64,
    /// Total epochs rolled back.
    pub epochs_rolled_back: AtomicU64,
    /// Total records routed to dead letter queue.
    pub dlq_records: AtomicU64,
    /// Total serialization errors.
    pub serialization_errors: AtomicU64,
}

impl KafkaSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            epochs_committed: AtomicU64::new(0),
            epochs_rolled_back: AtomicU64::new(0),
            dlq_records: AtomicU64::new(0),
            serialization_errors: AtomicU64::new(0),
        }
    }

    /// Records a successful write of `records` records totaling `bytes`.
    pub fn record_write(&self, records: u64, bytes: u64) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a production or serialization error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful epoch commit.
    pub fn record_commit(&self) {
        self.epochs_committed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a DLQ routing event.
    pub fn record_dlq(&self) {
        self.dlq_records.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a serialization error.
    pub fn record_serialization_error(&self) {
        self.serialization_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_written.load(Ordering::Relaxed),
            bytes_total: self.bytes_written.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "kafka.epochs_committed",
            self.epochs_committed.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "kafka.epochs_rolled_back",
            self.epochs_rolled_back.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "kafka.dlq_records",
            self.dlq_records.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "kafka.serialization_errors",
            self.serialization_errors.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for KafkaSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = KafkaSinkMetrics::new();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_write() {
        let m = KafkaSinkMetrics::new();
        m.record_write(100, 5000);
        m.record_write(200, 10000);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 300);
        assert_eq!(cm.bytes_total, 15000);
    }

    #[test]
    fn test_epoch_metrics() {
        let m = KafkaSinkMetrics::new();
        m.record_commit();
        m.record_commit();
        m.record_rollback();

        let cm = m.to_connector_metrics();
        let committed = cm
            .custom
            .iter()
            .find(|(k, _)| k == "kafka.epochs_committed");
        assert_eq!(committed.unwrap().1, 2.0);
        let rolled_back = cm
            .custom
            .iter()
            .find(|(k, _)| k == "kafka.epochs_rolled_back");
        assert_eq!(rolled_back.unwrap().1, 1.0);
    }

    #[test]
    fn test_dlq_and_serde_errors() {
        let m = KafkaSinkMetrics::new();
        m.record_dlq();
        m.record_dlq();
        m.record_serialization_error();
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 1);
        let dlq = cm.custom.iter().find(|(k, _)| k == "kafka.dlq_records");
        assert_eq!(dlq.unwrap().1, 2.0);
        let serde = cm
            .custom
            .iter()
            .find(|(k, _)| k == "kafka.serialization_errors");
        assert_eq!(serde.unwrap().1, 1.0);
    }
}
