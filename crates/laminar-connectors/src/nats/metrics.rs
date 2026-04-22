//! NATS connector metrics. No per-subject labels — NATS subjects are
//! wildcard-addressable and often unbounded-cardinality.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::metrics::ConnectorMetrics;

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
}

impl NatsSourceMetrics {
    /// Registers the metrics on `registry` if provided.
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

    /// Record a poll batch.
    pub fn record_poll(&self, records: u64, bytes: u64) {
        self.records_total.inc_by(records);
        self.bytes_total.inc_by(bytes);
    }

    /// Record a fetch-loop error.
    pub fn record_fetch_error(&self) {
        self.fetch_errors_total.inc();
    }

    /// Record one successful ack.
    pub fn record_ack(&self) {
        self.acks_total.inc();
    }

    /// Record one failed ack.
    pub fn record_ack_error(&self) {
        self.ack_errors_total.inc();
    }

    /// Set the pending-ack gauge.
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_pending_acks(&self, n: usize) {
        self.pending_acks.set(n as i64);
    }

    /// Folds to the SDK's [`ConnectorMetrics`]. `lag` stays 0 until
    /// we poll `consumer.info()` for the real broker-side lag.
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.records_total.get(),
            bytes_total: self.bytes_total.get(),
            errors_total: self.fetch_errors_total.get() + self.ack_errors_total.get(),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom("nats.acks", self.acks_total.get() as f64);
        m.add_custom("nats.ack_errors", self.ack_errors_total.get() as f64);
        m.add_custom("nats.pending_acks", self.pending_acks.get() as f64);
        m
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
    /// Publishes the server dropped as `Nats-Msg-Id` duplicates.
    pub dedup_total: IntCounter,
    pub epochs_rolled_back: IntCounter,
    pub pending_futures: IntGauge,
}

impl NatsSinkMetrics {
    /// Registers the metrics on `registry` if provided.
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
            epochs_rolled_back: reg_c!("nats_sink_epochs_rolled_back_total", "Epochs rolled back"),
            pending_futures,
        }
    }

    /// Record one successful publish of `bytes`.
    pub fn record_published_row(&self, bytes: u64) {
        self.records_total.inc();
        self.bytes_total.inc_by(bytes);
    }

    /// Record one publish error.
    pub fn record_publish_error(&self) {
        self.publish_errors_total.inc();
    }

    /// Record one ack error.
    pub fn record_ack_error(&self) {
        self.ack_errors_total.inc();
    }

    /// Record one server-identified duplicate.
    pub fn record_dedup(&self) {
        self.dedup_total.inc();
    }

    /// Record one epoch rollback.
    pub fn record_rollback(&self) {
        self.epochs_rolled_back.inc();
    }

    /// Set the pending-futures gauge.
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
        m.add_custom(
            "nats.epochs_rolled_back",
            self.epochs_rolled_back.get() as f64,
        );
        m
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
        assert_eq!(cm.lag, 0, "lag stays zero until we poll consumer.info()");
        assert!(cm.custom.iter().any(|(k, v)| k == "nats.acks" && *v == 2.0));
        assert!(cm
            .custom
            .iter()
            .any(|(k, v)| k == "nats.pending_acks" && *v == 7.0));
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

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 10);
        assert_eq!(cm.bytes_total, 2000);
        assert_eq!(cm.lag, 3);
        assert!(cm
            .custom
            .iter()
            .any(|(k, v)| k == "nats.dedup" && *v == 2.0));
        assert!(cm
            .custom
            .iter()
            .any(|(k, v)| k == "nats.epochs_rolled_back" && *v == 1.0));
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
