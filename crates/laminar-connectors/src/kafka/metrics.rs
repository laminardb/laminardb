//! Prometheus-backed Kafka source metrics.

use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};

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
    /// Broker rejected the commit.
    pub commit_failures_rejected: IntCounter,
    /// Commit `spawn_blocking` task panicked.
    pub commit_failures_panic: IntCounter,
    /// `notify_epoch_committed` could not enqueue (commit task gone).
    pub commit_failures_enqueue_dropped: IntCounter,
    /// Total consumer group rebalances.
    pub rebalances: IntCounter,
    /// Consumer lag (sum across all partitions of `high_watermark - current_offset`).
    pub lag: IntGauge,
    /// Duration of a broker offset commit, in seconds.
    pub broker_commit_duration: Histogram,
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
        let reg = handle.registry();

        // Const-label counters (`reason=...`) share a metric name and
        // can't go through the shared helper.
        let make_failure = |reason: &str| {
            let c = IntCounter::with_opts(
                Opts::new(
                    "kafka_source_commit_failures_total",
                    "Offset commit failures by reason",
                )
                .const_label("reason", reason),
            )
            .unwrap();
            // Best-effort registration — ignore `AlreadyReg` if another
            // Kafka source is already on the same registry.
            let _ = reg.register(Box::new(c.clone()));
            c
        };
        let commit_failures_rejected = make_failure("rejected");
        let commit_failures_panic = make_failure("panic");
        let commit_failures_enqueue_dropped = make_failure("enqueue_dropped");

        let broker_commit_duration = Histogram::with_opts(
            HistogramOpts::new(
                "kafka_source_broker_commit_duration_seconds",
                "Duration of broker offset commits, in seconds",
            )
            .buckets(prometheus::exponential_buckets(0.01, 4.0, 5).unwrap()),
        )
        .unwrap();
        let _ = reg.register(Box::new(broker_commit_duration.clone()));

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
            commit_failures_rejected,
            commit_failures_panic,
            commit_failures_enqueue_dropped,
            rebalances: handle.counter(
                "kafka_source_rebalances_total",
                "Total consumer group rebalances",
            ),
            lag: handle.gauge(
                "kafka_source_consumer_lag",
                "Consumer lag (sum across partitions)",
            ),
            broker_commit_duration,
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

    /// Records a successful offset commit.
    pub fn record_commit(&self) {
        self.commits.inc();
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

    /// Records a single broker offset commit duration in seconds.
    pub fn observe_broker_commit_duration(&self, secs: f64) {
        self.broker_commit_duration.observe(secs);
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
    fn test_record_error_and_commit() {
        let m = KafkaSourceMetrics::new(None);
        m.record_error();
        m.record_error();
        m.record_commit();

        assert_eq!(m.errors.get(), 2);
        assert_eq!(m.commits.get(), 1);
    }

    #[test]
    fn test_record_commit_failure() {
        let m = KafkaSourceMetrics::new(None);
        m.commit_failures_rejected.inc();
        m.commit_failures_panic.inc();

        let total = m.commit_failures_rejected.get() + m.commit_failures_panic.get();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_record_rebalance() {
        let m = KafkaSourceMetrics::new(None);
        m.record_rebalance();
        m.record_rebalance();

        assert_eq!(m.rebalances.get(), 2);
    }

    #[test]
    fn test_set_lag() {
        let m = KafkaSourceMetrics::new(None);
        assert_eq!(m.lag.get(), 0);

        m.set_lag(42);
        assert_eq!(m.lag.get(), 42);

        m.set_lag(100);
        assert_eq!(m.lag.get(), 100);
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
        assert_eq!(m.lag.get(), 598);
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
