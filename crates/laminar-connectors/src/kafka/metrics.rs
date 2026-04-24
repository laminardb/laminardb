//! Prometheus-backed Kafka source metrics.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::metrics::ConnectorMetrics;

/// Prometheus-backed counters/gauges for Kafka source connector statistics.
#[derive(Debug, Clone)]
pub struct KafkaSourceMetrics {
    /// Total records polled from Kafka.
    pub records_polled: IntCounter,
    /// Total bytes polled from Kafka.
    pub bytes_polled: IntCounter,
    /// Total deserialization or consumer errors.
    pub errors: IntCounter,
    /// Total batches returned from `poll_batch()`.
    pub batches_polled: IntCounter,
    /// Total offset commits to Kafka.
    pub commits: IntCounter,
    /// Total offset commit failures (broker rejected, timeout, task panic).
    pub commit_failures: IntCounter,
    /// Total consumer group rebalances.
    pub rebalances: IntCounter,
    /// Consumer lag (sum across all partitions of `high_watermark - current_offset`).
    pub lag: IntGauge,
    /// Count of successful Schema Registry discoveries at DDL time.
    pub sr_discovery_successes: IntCounter,
    /// Count of Schema Registry discovery failures (HTTP error, parse error).
    pub sr_discovery_failures: IntCounter,
    /// Count of Schema Registry discovery timeouts.
    pub sr_discovery_timeouts: IntCounter,
}

impl KafkaSourceMetrics {
    /// If `registry` is `Some`, counters are registered there (visible
    /// in the Prometheus scrape); otherwise a throwaway registry is used.
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

        let records_polled = IntCounter::new(
            "kafka_source_records_polled_total",
            "Total records polled from Kafka",
        )
        .unwrap();
        let bytes_polled = IntCounter::new(
            "kafka_source_bytes_polled_total",
            "Total bytes polled from Kafka",
        )
        .unwrap();
        let errors =
            IntCounter::new("kafka_source_errors_total", "Total Kafka consumer errors").unwrap();
        let batches_polled = IntCounter::new(
            "kafka_source_batches_polled_total",
            "Total batches polled from Kafka",
        )
        .unwrap();
        let commits = IntCounter::new(
            "kafka_source_commits_total",
            "Total offset commits to Kafka",
        )
        .unwrap();
        let commit_failures = IntCounter::new(
            "kafka_source_commit_failures_total",
            "Total offset commit failures (broker rejection, timeout, panic)",
        )
        .unwrap();
        let rebalances = IntCounter::new(
            "kafka_source_rebalances_total",
            "Total consumer group rebalances",
        )
        .unwrap();
        let lag = IntGauge::new(
            "kafka_source_consumer_lag",
            "Consumer lag (sum across partitions)",
        )
        .unwrap();
        let sr_discovery_successes = IntCounter::new(
            "kafka_source_sr_discovery_successes_total",
            "Schema Registry discovery successes",
        )
        .unwrap();
        let sr_discovery_failures = IntCounter::new(
            "kafka_source_sr_discovery_failures_total",
            "Schema Registry discovery failures",
        )
        .unwrap();
        let sr_discovery_timeouts = IntCounter::new(
            "kafka_source_sr_discovery_timeouts_total",
            "Schema Registry discovery timeouts",
        )
        .unwrap();

        // Best-effort registration — ignore `AlreadyReg` if another
        // Kafka source is already on the same registry.
        let _ = reg.register(Box::new(records_polled.clone()));
        let _ = reg.register(Box::new(bytes_polled.clone()));
        let _ = reg.register(Box::new(errors.clone()));
        let _ = reg.register(Box::new(batches_polled.clone()));
        let _ = reg.register(Box::new(commits.clone()));
        let _ = reg.register(Box::new(commit_failures.clone()));
        let _ = reg.register(Box::new(rebalances.clone()));
        let _ = reg.register(Box::new(lag.clone()));
        let _ = reg.register(Box::new(sr_discovery_successes.clone()));
        let _ = reg.register(Box::new(sr_discovery_failures.clone()));
        let _ = reg.register(Box::new(sr_discovery_timeouts.clone()));

        Self {
            records_polled,
            bytes_polled,
            errors,
            batches_polled,
            commits,
            commit_failures,
            rebalances,
            lag,
            sr_discovery_successes,
            sr_discovery_failures,
            sr_discovery_timeouts,
        }
    }

    /// Records a successful poll of `records` records totaling `bytes`.
    pub fn record_poll(&self, records: u64, bytes: u64) {
        self.records_polled.inc_by(records);
        self.bytes_polled.inc_by(bytes);
        self.batches_polled.inc();
    }

    /// Records a consumer or deserialization error.
    pub fn record_error(&self) {
        self.errors.inc();
    }

    /// Records a successful offset commit.
    pub fn record_commit(&self) {
        self.commits.inc();
    }

    /// Records an offset commit failure (broker rejection, timeout, panic).
    pub fn record_commit_failure(&self) {
        self.commit_failures.inc();
    }

    /// Records a consumer group rebalance event.
    pub fn record_rebalance(&self) {
        self.rebalances.inc();
    }

    /// Updates the consumer lag value.
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_lag(&self, lag: u64) {
        self.lag.set(lag as i64);
    }

    /// Records a successful Schema Registry discovery at DDL time.
    pub fn record_sr_discovery_success(&self) {
        self.sr_discovery_successes.inc();
    }

    /// Records a Schema Registry discovery failure.
    pub fn record_sr_discovery_failure(&self) {
        self.sr_discovery_failures.inc();
    }

    /// Records a Schema Registry discovery timeout.
    pub fn record_sr_discovery_timeout(&self) {
        self.sr_discovery_timeouts.inc();
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_polled.get(),
            bytes_total: self.bytes_polled.get(),
            errors_total: self.errors.get(),
            lag: self.lag.get() as u64,
            custom: Vec::new(),
        };
        m.add_custom("kafka.batches_polled", self.batches_polled.get() as f64);
        m.add_custom("kafka.commits", self.commits.get() as f64);
        m.add_custom("kafka.commit_failures", self.commit_failures.get() as f64);
        m.add_custom("kafka.rebalances", self.rebalances.get() as f64);
        m.add_custom(
            "kafka.sr_discovery_successes",
            self.sr_discovery_successes.get() as f64,
        );
        m.add_custom(
            "kafka.sr_discovery_failures",
            self.sr_discovery_failures.get() as f64,
        );
        m.add_custom(
            "kafka.sr_discovery_timeouts",
            self.sr_discovery_timeouts.get() as f64,
        );
        m
    }
}

impl Default for KafkaSourceMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = KafkaSourceMetrics::new(None);
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_poll() {
        let m = KafkaSourceMetrics::new(None);
        m.record_poll(100, 5000);
        m.record_poll(200, 10000);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 300);
        assert_eq!(cm.bytes_total, 15000);
    }

    #[test]
    fn test_record_error_and_commit() {
        let m = KafkaSourceMetrics::new(None);
        m.record_error();
        m.record_error();
        m.record_commit();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 2);
        // 4 base (batches_polled, commits, commit_failures, rebalances) +
        // 3 SR-discovery counters.
        assert_eq!(cm.custom.len(), 7);
        let commits = cm.custom.iter().find(|(k, _)| k == "kafka.commits");
        assert_eq!(commits.unwrap().1, 1.0);
    }

    #[test]
    fn test_record_commit_failure() {
        let m = KafkaSourceMetrics::new(None);
        m.record_commit_failure();
        m.record_commit_failure();

        let cm = m.to_connector_metrics();
        let failures = cm.custom.iter().find(|(k, _)| k == "kafka.commit_failures");
        assert_eq!(failures.unwrap().1, 2.0);
    }

    #[test]
    fn test_record_rebalance() {
        let m = KafkaSourceMetrics::new(None);
        m.record_rebalance();
        m.record_rebalance();

        let cm = m.to_connector_metrics();
        let rebalances = cm.custom.iter().find(|(k, _)| k == "kafka.rebalances");
        assert_eq!(rebalances.unwrap().1, 2.0);
    }

    #[test]
    fn test_set_lag() {
        let m = KafkaSourceMetrics::new(None);
        assert_eq!(m.to_connector_metrics().lag, 0);

        m.set_lag(42);
        assert_eq!(m.to_connector_metrics().lag, 42);

        m.set_lag(100);
        assert_eq!(m.to_connector_metrics().lag, 100);
    }

    #[test]
    fn test_sr_discovery_counters() {
        let m = KafkaSourceMetrics::new(None);
        m.record_sr_discovery_success();
        m.record_sr_discovery_success();
        m.record_sr_discovery_failure();
        m.record_sr_discovery_timeout();

        let cm = m.to_connector_metrics();
        let successes = cm
            .custom
            .iter()
            .find(|(k, _)| k == "kafka.sr_discovery_successes")
            .unwrap();
        let failures = cm
            .custom
            .iter()
            .find(|(k, _)| k == "kafka.sr_discovery_failures")
            .unwrap();
        let timeouts = cm
            .custom
            .iter()
            .find(|(k, _)| k == "kafka.sr_discovery_timeouts")
            .unwrap();
        assert_eq!(successes.1, 2.0);
        assert_eq!(failures.1, 1.0);
        assert_eq!(timeouts.1, 1.0);
    }

    #[test]
    fn test_lag_computation() {
        // Simulates 3 partitions: high_watermark - (offset + 1) for each.
        let partitions = [
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
        assert_eq!(total_lag, 598);

        let m = KafkaSourceMetrics::new(None);
        m.set_lag(total_lag);
        assert_eq!(m.to_connector_metrics().lag, 598);
    }

    #[test]
    fn test_registered_on_prometheus_registry() {
        let reg = Registry::new();
        let m = KafkaSourceMetrics::new(Some(&reg));
        m.record_poll(10, 500);
        m.record_error();

        // Verify the metrics are registered on the registry.
        let families = reg.gather();
        let names: Vec<&str> = families
            .iter()
            .map(prometheus::proto::MetricFamily::name)
            .collect();
        assert!(names.contains(&"kafka_source_records_polled_total"));
        assert!(names.contains(&"kafka_source_errors_total"));
    }
}
