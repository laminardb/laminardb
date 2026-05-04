//! NATS connector metrics. No per-subject labels — subjects are
//! wildcard-addressable and unbounded-cardinality.

use prometheus::core::Collector;
use prometheus::{Error as PromError, IntCounter, IntGauge, Registry};
use tracing::warn;

use crate::prom::reg_or_local;

fn register_collector<C: Collector + Clone + 'static>(reg: &Registry, name: &str, c: &C) {
    match reg.register(Box::new(c.clone())) {
        Ok(()) => {}
        // Multiple connectors on a shared registry can collide; the
        // second registration silently drops so its counts won't scrape.
        Err(PromError::AlreadyReg) => {
            warn!(
                metric = name,
                "metric already registered; use separate registries per connector"
            );
        }
        Err(e) => warn!(metric = name, error = ?e, "failed to register metric"),
    }
}

/// Prometheus counters for the NATS source.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct NatsSourceMetrics {
    pub records_total: IntCounter,
    pub bytes_total: IntCounter,
    pub fetch_errors_total: IntCounter,
    pub acks_total: IntCounter,
    pub ack_errors_total: IntCounter,
    pub pending_acks: IntGauge,
    /// Stream messages not yet delivered to the consumer.
    pub consumer_lag: IntGauge,
}

impl NatsSourceMetrics {
    /// Registers on `registry` if provided; otherwise on a local one.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let handle = reg_or_local(registry, &mut local);
        let reg = handle.registry();

        macro_rules! reg_c {
            ($name:expr, $help:expr) => {{
                let c = IntCounter::new($name, $help).unwrap();
                register_collector(reg, $name, &c);
                c
            }};
        }
        let pending_acks =
            IntGauge::new("nats_source_pending_acks", "Unacked JetStream messages").unwrap();
        register_collector(reg, "nats_source_pending_acks", &pending_acks);
        let consumer_lag = IntGauge::new(
            "nats_source_consumer_lag",
            "Stream messages not yet delivered to the consumer",
        )
        .unwrap();
        register_collector(reg, "nats_source_consumer_lag", &consumer_lag);

        Self {
            records_total: reg_c!("nats_source_records_total", "Records delivered"),
            bytes_total: reg_c!("nats_source_bytes_total", "Payload bytes delivered"),
            fetch_errors_total: reg_c!("nats_source_fetch_errors_total", "Fetch errors"),
            acks_total: reg_c!("nats_source_acks_total", "Successful acks"),
            ack_errors_total: reg_c!("nats_source_ack_errors_total", "Failed acks"),
            pending_acks,
            consumer_lag,
        }
    }

    #[allow(missing_docs)]
    pub fn record_poll(&self, records: u64, bytes: u64) {
        self.records_total.inc_by(records);
        self.bytes_total.inc_by(bytes);
    }

    #[allow(missing_docs)]
    pub fn record_fetch_error(&self) {
        self.fetch_errors_total.inc();
    }

    #[allow(missing_docs)]
    pub fn record_ack(&self) {
        self.acks_total.inc();
    }

    #[allow(missing_docs)]
    pub fn record_ack_error(&self) {
        self.ack_errors_total.inc();
    }

    #[allow(missing_docs, clippy::cast_possible_wrap)]
    pub fn set_pending_acks(&self, n: usize) {
        self.pending_acks.set(n as i64);
    }

    #[allow(missing_docs, clippy::cast_possible_wrap)]
    pub fn set_consumer_lag(&self, n: u64) {
        self.consumer_lag.set(n as i64);
    }
}

/// Prometheus counters for the NATS sink.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct NatsSinkMetrics {
    pub records_total: IntCounter,
    pub bytes_total: IntCounter,
    pub publish_errors_total: IntCounter,
    pub ack_errors_total: IntCounter,
    /// Publishes the broker dropped as `Nats-Msg-Id` duplicates.
    pub dedup_total: IntCounter,
    pub epochs_rolled_back: IntCounter,
    pub pending_futures: IntGauge,
}

impl NatsSinkMetrics {
    /// Registers on `registry` if provided; otherwise on a local one.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let handle = reg_or_local(registry, &mut local);
        let reg = handle.registry();

        macro_rules! reg_c {
            ($name:expr, $help:expr) => {{
                let c = IntCounter::new($name, $help).unwrap();
                register_collector(reg, $name, &c);
                c
            }};
        }
        let pending_futures =
            IntGauge::new("nats_sink_pending_futures", "Outstanding PublishAckFutures").unwrap();
        register_collector(reg, "nats_sink_pending_futures", &pending_futures);

        Self {
            records_total: reg_c!("nats_sink_records_total", "Records published"),
            bytes_total: reg_c!("nats_sink_bytes_total", "Payload bytes published"),
            publish_errors_total: reg_c!("nats_sink_publish_errors_total", "Publish errors"),
            ack_errors_total: reg_c!("nats_sink_ack_errors_total", "Publish-ack errors"),
            dedup_total: reg_c!("nats_sink_dedup_total", "Broker-dropped duplicates"),
            epochs_rolled_back: reg_c!("nats_sink_epochs_rolled_back_total", "Epochs rolled back"),
            pending_futures,
        }
    }

    #[allow(missing_docs)]
    pub fn record_published_row(&self, bytes: u64) {
        self.records_total.inc();
        self.bytes_total.inc_by(bytes);
    }

    #[allow(missing_docs)]
    pub fn record_publish_error(&self) {
        self.publish_errors_total.inc();
    }

    #[allow(missing_docs)]
    pub fn record_ack_error(&self) {
        self.ack_errors_total.inc();
    }

    #[allow(missing_docs)]
    pub fn record_dedup(&self) {
        self.dedup_total.inc();
    }

    #[allow(missing_docs)]
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.inc();
    }

    #[allow(missing_docs, clippy::cast_possible_wrap)]
    pub fn set_pending_futures(&self, n: usize) {
        self.pending_futures.set(n as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_initial_zero() {
        let m = NatsSourceMetrics::new(None);
        assert_eq!(m.records_total.get(), 0);
        assert_eq!(m.bytes_total.get(), 0);
        assert_eq!(m.fetch_errors_total.get(), 0);
        assert_eq!(m.consumer_lag.get(), 0);
    }

    #[test]
    fn source_records_increment() {
        let m = NatsSourceMetrics::new(None);
        m.record_poll(100, 4096);
        m.record_poll(50, 1024);
        m.record_ack();
        m.record_ack();
        m.record_fetch_error();
        m.set_pending_acks(7);

        m.set_consumer_lag(42);

        assert_eq!(m.records_total.get(), 150);
        assert_eq!(m.bytes_total.get(), 5120);
        assert_eq!(m.fetch_errors_total.get(), 1);
        assert_eq!(m.consumer_lag.get(), 42);
        assert_eq!(m.acks_total.get(), 2);
        assert_eq!(m.pending_acks.get(), 7);
    }

    #[test]
    fn sink_records_and_dedup() {
        let m = NatsSinkMetrics::new(None);
        for _ in 0..10 {
            m.record_published_row(200);
        }
        m.record_dedup();
        m.record_dedup();
        m.record_rollback();
        m.set_pending_futures(3);

        assert_eq!(m.records_total.get(), 10);
        assert_eq!(m.bytes_total.get(), 2000);
        assert_eq!(m.pending_futures.get(), 3);
        assert_eq!(m.dedup_total.get(), 2);
        assert_eq!(m.epochs_rolled_back.get(), 1);
    }

    #[test]
    fn metrics_register_on_shared_registry() {
        let reg = Registry::new();
        let _s = NatsSourceMetrics::new(Some(&reg));
        let _k = NatsSinkMetrics::new(Some(&reg));
        let names: Vec<String> = reg.gather().iter().map(|f| f.name().to_string()).collect();
        assert!(names.contains(&"nats_source_records_total".to_string()));
        assert!(names.contains(&"nats_sink_records_total".to_string()));
        assert!(names.contains(&"nats_sink_dedup_total".to_string()));
    }
}
