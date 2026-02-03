//! Kafka source connector metrics.
//!
//! [`KafkaSourceMetrics`] provides lock-free atomic counters for
//! tracking consumption statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for Kafka source connector statistics.
#[derive(Debug)]
pub struct KafkaSourceMetrics {
    /// Total records polled from Kafka.
    pub records_polled: AtomicU64,
    /// Total bytes polled from Kafka.
    pub bytes_polled: AtomicU64,
    /// Total deserialization or consumer errors.
    pub errors: AtomicU64,
    /// Total batches returned from `poll_batch()`.
    pub batches_polled: AtomicU64,
    /// Total offset commits to Kafka.
    pub commits: AtomicU64,
    /// Total consumer group rebalances.
    pub rebalances: AtomicU64,
}

impl KafkaSourceMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records_polled: AtomicU64::new(0),
            bytes_polled: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            batches_polled: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            rebalances: AtomicU64::new(0),
        }
    }

    /// Records a successful poll of `records` records totaling `bytes`.
    pub fn record_poll(&self, records: u64, bytes: u64) {
        self.records_polled.fetch_add(records, Ordering::Relaxed);
        self.bytes_polled.fetch_add(bytes, Ordering::Relaxed);
        self.batches_polled.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a consumer or deserialization error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful offset commit.
    pub fn record_commit(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a consumer group rebalance event.
    pub fn record_rebalance(&self) {
        self.rebalances.fetch_add(1, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_polled.load(Ordering::Relaxed),
            bytes_total: self.bytes_polled.load(Ordering::Relaxed),
            errors_total: self.errors.load(Ordering::Relaxed),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "kafka.batches_polled",
            self.batches_polled.load(Ordering::Relaxed) as f64,
        );
        m.add_custom("kafka.commits", self.commits.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "kafka.rebalances",
            self.rebalances.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for KafkaSourceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = KafkaSourceMetrics::new();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_poll() {
        let m = KafkaSourceMetrics::new();
        m.record_poll(100, 5000);
        m.record_poll(200, 10000);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 300);
        assert_eq!(cm.bytes_total, 15000);
    }

    #[test]
    fn test_record_error_and_commit() {
        let m = KafkaSourceMetrics::new();
        m.record_error();
        m.record_error();
        m.record_commit();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 2);
        assert_eq!(cm.custom.len(), 3);
        // Check custom metrics
        let commits = cm.custom.iter().find(|(k, _)| k == "kafka.commits");
        assert_eq!(commits.unwrap().1, 1.0);
    }

    #[test]
    fn test_record_rebalance() {
        let m = KafkaSourceMetrics::new();
        m.record_rebalance();
        m.record_rebalance();

        let cm = m.to_connector_metrics();
        let rebalances = cm.custom.iter().find(|(k, _)| k == "kafka.rebalances");
        assert_eq!(rebalances.unwrap().1, 2.0);
    }
}
