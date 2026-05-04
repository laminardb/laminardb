//! Kafka sink connector metrics.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::prom::reg_or_local;

/// Prometheus-backed counters for Kafka sink connector statistics.
#[derive(Debug, Clone)]
pub struct KafkaSinkMetrics {
    /// Records written to Kafka.
    pub records_written: IntCounter,
    /// Bytes written to Kafka (payload only).
    pub bytes_written: IntCounter,
    /// Errors encountered.
    pub errors_total: IntCounter,
    /// Epochs committed.
    pub epochs_committed: IntCounter,
    /// Epochs rolled back.
    pub epochs_rolled_back: IntCounter,
    /// Records routed to dead letter queue.
    pub dlq_records: IntCounter,
    /// Serialization errors.
    pub serialization_errors: IntCounter,
    /// Sum of produce delivery latencies in microseconds.
    pub produce_latency_sum_us: IntCounter,
    /// Maximum produce delivery latency in microseconds.
    pub produce_latency_max_us: IntGauge,
    /// Number of produce delivery latency samples.
    pub produce_latency_count: IntCounter,
}

impl KafkaSinkMetrics {
    /// All counters start at zero. Registers on `registry` if provided.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);

        Self {
            records_written: reg.counter(
                "kafka_sink_records_written_total",
                "Records written to Kafka",
            ),
            bytes_written: reg.counter("kafka_sink_bytes_written_total", "Bytes written to Kafka"),
            errors_total: reg.counter("kafka_sink_errors_total", "Kafka sink errors"),
            epochs_committed: reg.counter("kafka_sink_epochs_committed_total", "Epochs committed"),
            epochs_rolled_back: reg
                .counter("kafka_sink_epochs_rolled_back_total", "Epochs rolled back"),
            dlq_records: reg.counter("kafka_sink_dlq_records_total", "Records routed to DLQ"),
            serialization_errors: reg.counter(
                "kafka_sink_serialization_errors_total",
                "Serialization errors",
            ),
            produce_latency_sum_us: reg.counter(
                "kafka_sink_produce_latency_sum_us",
                "Sum of produce latencies (us)",
            ),
            produce_latency_count: reg.counter(
                "kafka_sink_produce_latency_count",
                "Produce latency samples",
            ),
            produce_latency_max_us: reg.gauge(
                "kafka_sink_produce_latency_max_us",
                "Max produce delivery latency (us)",
            ),
        }
    }

    /// Records a successful write of `records` records totaling `bytes`.
    pub fn record_write(&self, records: u64, bytes: u64) {
        self.records_written.inc_by(records);
        self.bytes_written.inc_by(bytes);
    }

    /// Records a production error.
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

    /// Records a DLQ routing event.
    pub fn record_dlq(&self) {
        self.dlq_records.inc();
    }

    /// Records a serialization error.
    pub fn record_serialization_error(&self) {
        self.serialization_errors.inc();
    }

    /// Records a produce delivery latency sample in microseconds.
    #[allow(clippy::cast_possible_wrap)]
    pub fn record_produce_latency(&self, latency_us: u64) {
        self.produce_latency_sum_us.inc_by(latency_us);
        self.produce_latency_count.inc();
        if latency_us as i64 > self.produce_latency_max_us.get() {
            self.produce_latency_max_us.set(latency_us as i64);
        }
    }
}

impl Default for KafkaSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = KafkaSinkMetrics::new(None);
        assert_eq!(m.records_written.get(), 0);
        assert_eq!(m.bytes_written.get(), 0);
        assert_eq!(m.errors_total.get(), 0);
    }

    #[test]
    fn test_record_write() {
        let m = KafkaSinkMetrics::new(None);
        m.record_write(100, 5000);
        m.record_write(200, 10000);
        assert_eq!(m.records_written.get(), 300);
        assert_eq!(m.bytes_written.get(), 15000);
    }

    #[test]
    fn test_produce_latency() {
        let m = KafkaSinkMetrics::new(None);
        m.record_produce_latency(100);
        m.record_produce_latency(300);
        m.record_produce_latency(50);

        assert_eq!(m.produce_latency_count.get(), 3);
        assert_eq!(m.produce_latency_sum_us.get(), 450);
        assert_eq!(m.produce_latency_max_us.get(), 300);
    }
}
