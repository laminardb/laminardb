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
    /// Consumer lag (sum across all partitions of high_watermark - current_offset).
    pub lag: AtomicU64,
}

impl KafkaSourceMetrics {
    /// All counters start at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records_polled: AtomicU64::new(0),
            bytes_polled: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            batches_polled: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            rebalances: AtomicU64::new(0),
            lag: AtomicU64::new(0),
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

    /// Updates the consumer lag value.
    pub fn set_lag(&self, lag: u64) {
        self.lag.store(lag, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_polled.load(Ordering::Relaxed),
            bytes_total: self.bytes_polled.load(Ordering::Relaxed),
            errors_total: self.errors.load(Ordering::Relaxed),
            lag: self.lag.load(Ordering::Relaxed),
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

    #[test]
    fn test_set_lag() {
        let m = KafkaSourceMetrics::new();
        assert_eq!(m.to_connector_metrics().lag, 0);

        m.set_lag(42);
        assert_eq!(m.to_connector_metrics().lag, 42);

        m.set_lag(100);
        assert_eq!(m.to_connector_metrics().lag, 100);
    }

    #[test]
    fn test_lag_computation() {
        // Simulates 3 partitions: high_watermark - (offset + 1) for each.
        let partitions = vec![
            (1000_i64, 900_i64), // lag = 1000 - (900 + 1) = 99
            (500, 499),          // lag = 500 - (499 + 1) = 0
            (2000, 1500),        // lag = 2000 - (1500 + 1) = 499
        ];
        let total_lag: u64 = partitions
            .iter()
            .map(|(hw, off)| {
                let lag = hw - (off + 1);
                if lag > 0 {
                    lag as u64
                } else {
                    0
                }
            })
            .sum();
        assert_eq!(total_lag, 99 + 0 + 499);

        let m = KafkaSourceMetrics::new();
        m.set_lag(total_lag);
        assert_eq!(m.to_connector_metrics().lag, 598);
    }
}
