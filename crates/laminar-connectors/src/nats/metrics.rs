//! NATS connector metrics.
//!
//! Mirrors the shape of `KafkaSourceMetrics` / `KafkaSinkMetrics`: a
//! handful of Prometheus counters + gauges registered on the shared
//! registry (if any), with a `record_*` method per interesting event
//! and `to_connector_metrics()` folding down to the SDK's
//! [`ConnectorMetrics`].
//!
//! We intentionally omit per-subject labels. NATS subjects are
//! wildcard-addressable and often unbounded-cardinality; attaching
//! them to a counter is a Prometheus footgun. If a user needs
//! per-subject visibility, they should aggregate at the server side.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::metrics::ConnectorMetrics;

/// Prometheus counters for the NATS source.
#[derive(Debug, Clone)]
pub struct NatsSourceMetrics {
    /// Total records delivered to `poll_batch` callers.
    pub records_total: IntCounter,
    /// Total bytes of payload delivered.
    pub bytes_total: IntCounter,
    /// Errors from `consumer.fetch()` (the pull loop).
    pub fetch_errors_total: IntCounter,
    /// Successful `JetStream` acks.
    pub acks_total: IntCounter,
    /// Ack failures (broker rejected, connection dropped mid-ack, etc).
    pub ack_errors_total: IntCounter,
    /// Retained message handles not yet acked (pending + sealed).
    pub pending_acks: IntGauge,
}

impl NatsSourceMetrics {
    /// All counters start at zero. If `registry` is `Some`, the metrics
    /// register on it; otherwise a local throwaway registry is used.
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

        macro_rules! reg_c {
            ($name:expr, $help:expr) => {{
                let c = IntCounter::new($name, $help).unwrap();
                let _ = reg.register(Box::new(c.clone()));
                c
            }};
        }
        let pending_acks =
            IntGauge::new("nats_source_pending_acks", "Unacked JetStream messages").unwrap();
        let _ = reg.register(Box::new(pending_acks.clone()));

        Self {
            records_total: reg_c!(
                "nats_source_records_total",
                "Records delivered to poll_batch"
            ),
            bytes_total: reg_c!("nats_source_bytes_total", "Payload bytes delivered"),
            fetch_errors_total: reg_c!(
                "nats_source_fetch_errors_total",
                "Errors from consumer.fetch()"
            ),
            acks_total: reg_c!("nats_source_acks_total", "Successful JetStream acks"),
            ack_errors_total: reg_c!("nats_source_ack_errors_total", "Failed acks"),
            pending_acks,
        }
    }

    /// Records a poll batch of `records` with `bytes` total payload.
    pub fn record_poll(&self, records: u64, bytes: u64) {
        self.records_total.inc_by(records);
        self.bytes_total.inc_by(bytes);
    }

    /// Records a fetch-loop error.
    pub fn record_fetch_error(&self) {
        self.fetch_errors_total.inc();
    }

    /// Records one successful ack.
    pub fn record_ack(&self) {
        self.acks_total.inc();
    }

    /// Records one failed ack.
    pub fn record_ack_error(&self) {
        self.ack_errors_total.inc();
    }

    /// Sets the pending-ack gauge to `n`.
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_pending_acks(&self, n: usize) {
        self.pending_acks.set(n as i64);
    }

    /// Folds to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_total.get(),
            bytes_total: self.bytes_total.get(),
            errors_total: self.fetch_errors_total.get() + self.ack_errors_total.get(),
            lag: self.pending_acks.get() as u64,
            custom: Vec::new(),
        };
        m.add_custom("nats.acks", self.acks_total.get() as f64);
        m.add_custom("nats.ack_errors", self.ack_errors_total.get() as f64);
        m
    }
}

impl Default for NatsSourceMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Prometheus counters for the NATS sink.
#[derive(Debug, Clone)]
pub struct NatsSinkMetrics {
    /// Total records published (one per row in `write_batch`).
    pub records_total: IntCounter,
    /// Total bytes of payload published.
    pub bytes_total: IntCounter,
    /// Publish errors (serialize → publish).
    pub publish_errors_total: IntCounter,
    /// Publish-ack errors (`JetStream` did not acknowledge within the
    /// configured timeout, or returned an error status).
    pub ack_errors_total: IntCounter,
    /// Publishes the server identified as duplicates (arrived inside
    /// `duplicate_window` with a repeated `Nats-Msg-Id`).
    pub dedup_total: IntCounter,
    /// Outstanding `PublishAckFuture`s.
    pub pending_futures: IntGauge,
}

impl NatsSinkMetrics {
    /// All counters start at zero. If `registry` is `Some`, the metrics
    /// register on it.
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

        macro_rules! reg_c {
            ($name:expr, $help:expr) => {{
                let c = IntCounter::new($name, $help).unwrap();
                let _ = reg.register(Box::new(c.clone()));
                c
            }};
        }
        let pending_futures =
            IntGauge::new("nats_sink_pending_futures", "Outstanding PublishAckFutures").unwrap();
        let _ = reg.register(Box::new(pending_futures.clone()));

        Self {
            records_total: reg_c!("nats_sink_records_total", "Records published"),
            bytes_total: reg_c!("nats_sink_bytes_total", "Payload bytes published"),
            publish_errors_total: reg_c!("nats_sink_publish_errors_total", "Publish errors"),
            ack_errors_total: reg_c!("nats_sink_ack_errors_total", "Publish-ack errors"),
            dedup_total: reg_c!(
                "nats_sink_dedup_total",
                "Publishes identified by the server as duplicates"
            ),
            pending_futures,
        }
    }

    /// Records a successful publish of `records` rows and `bytes` total.
    pub fn record_publish(&self, records: u64, bytes: u64) {
        self.records_total.inc_by(records);
        self.bytes_total.inc_by(bytes);
    }

    /// Records one publish error.
    pub fn record_publish_error(&self) {
        self.publish_errors_total.inc();
    }

    /// Records one ack error.
    pub fn record_ack_error(&self) {
        self.ack_errors_total.inc();
    }

    /// Records one server-identified duplicate publish.
    pub fn record_dedup(&self) {
        self.dedup_total.inc();
    }

    /// Sets the pending-futures gauge.
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_pending_futures(&self, n: usize) {
        self.pending_futures.set(n as i64);
    }

    /// Folds to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_total.get(),
            bytes_total: self.bytes_total.get(),
            errors_total: self.publish_errors_total.get() + self.ack_errors_total.get(),
            lag: self.pending_futures.get() as u64,
            custom: Vec::new(),
        };
        m.add_custom("nats.dedup", self.dedup_total.get() as f64);
        m
    }
}

impl Default for NatsSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_initial_zero() {
        let m = NatsSourceMetrics::new(None);
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
        assert_eq!(cm.lag, 0);
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

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 150);
        assert_eq!(cm.bytes_total, 5120);
        assert_eq!(cm.errors_total, 1); // fetch error
        assert_eq!(cm.lag, 7);
        assert!(cm.custom.iter().any(|(k, v)| k == "nats.acks" && *v == 2.0));
    }

    #[test]
    fn sink_records_and_dedup() {
        let m = NatsSinkMetrics::new(None);
        m.record_publish(10, 2048);
        m.record_dedup();
        m.record_dedup();
        m.set_pending_futures(3);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 10);
        assert_eq!(cm.lag, 3);
        assert!(cm
            .custom
            .iter()
            .any(|(k, v)| k == "nats.dedup" && *v == 2.0));
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
