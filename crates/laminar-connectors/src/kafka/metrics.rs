//! Prometheus-backed Kafka source metrics.

use prometheus::{IntCounter, Registry};

use crate::prom::reg_or_local;

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
    /// Broker progress commits that failed locally or remotely.
    pub commit_failures: IntCounter,
    /// Total consumer group rebalances.
    pub rebalances: IntCounter,
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
    #[allow(clippy::missing_panics_doc, clippy::too_many_lines)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let handle = reg_or_local(registry, &mut local);

        Self {
            records_polled: handle.counter(
                "kafka_source_records_polled_total",
                "Total records polled from Kafka",
            ),
            bytes_polled: handle.counter(
                "kafka_source_bytes_polled_total",
                "Total bytes polled from Kafka",
            ),
            errors: handle.counter("kafka_source_errors_total", "Total Kafka consumer errors"),
            batches_polled: handle.counter(
                "kafka_source_batches_polled_total",
                "Total batches polled from Kafka",
            ),
            commits: handle.counter(
                "kafka_source_commits_total",
                "Total offset commits to Kafka",
            ),
            commit_failures: handle.counter(
                "kafka_source_commit_failures_total",
                "Kafka broker progress commit failures",
            ),
            rebalances: handle.counter(
                "kafka_source_rebalances_total",
                "Total consumer group rebalances",
            ),
            sr_discovery_successes: handle.counter(
                "kafka_source_sr_discovery_successes_total",
                "Schema Registry discovery successes",
            ),
            sr_discovery_failures: handle.counter(
                "kafka_source_sr_discovery_failures_total",
                "Schema Registry discovery failures",
            ),
            sr_discovery_timeouts: handle.counter(
                "kafka_source_sr_discovery_timeouts_total",
                "Schema Registry discovery timeouts",
            ),
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

    /// Records a consumer group rebalance event.
    pub fn record_rebalance(&self) {
        self.rebalances.inc();
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
        assert_eq!(m.records_polled.get(), 0);
        assert_eq!(m.bytes_polled.get(), 0);
        assert_eq!(m.errors.get(), 0);
    }

    #[test]
    fn test_record_poll() {
        let m = KafkaSourceMetrics::new(None);
        m.record_poll(100, 5000);
        m.record_poll(200, 10000);

        assert_eq!(m.records_polled.get(), 300);
        assert_eq!(m.bytes_polled.get(), 15000);
    }

    #[test]
    fn test_record_error() {
        let m = KafkaSourceMetrics::new(None);
        m.record_error();
        m.record_error();

        assert_eq!(m.errors.get(), 2);
    }

    #[test]
    fn test_record_commit_failure() {
        let m = KafkaSourceMetrics::new(None);
        m.commit_failures.inc();
        assert_eq!(m.commit_failures.get(), 1);
    }

    #[test]
    fn test_record_rebalance() {
        let m = KafkaSourceMetrics::new(None);
        m.record_rebalance();
        m.record_rebalance();

        assert_eq!(m.rebalances.get(), 2);
    }

    #[test]
    fn test_sr_discovery_counters() {
        let m = KafkaSourceMetrics::new(None);
        m.record_sr_discovery_success();
        m.record_sr_discovery_success();
        m.record_sr_discovery_failure();
        m.record_sr_discovery_timeout();

        assert_eq!(m.sr_discovery_successes.get(), 2);
        assert_eq!(m.sr_discovery_failures.get(), 1);
        assert_eq!(m.sr_discovery_timeouts.get(), 1);
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
