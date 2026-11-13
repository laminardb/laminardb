//! NATS connector metrics. No per-subject labels — subjects are
//! wildcard-addressable and unbounded-cardinality.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::prom::{reg_or_local, Registration};

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
    _registration: Registration,
}

impl NatsSourceMetrics {
    /// Registers on `registry` if provided; otherwise on a local one.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);
        Self {
            records_total: reg.counter("nats_source_records_total", "Records delivered"),
            bytes_total: reg.counter("nats_source_bytes_total", "Payload bytes delivered"),
            fetch_errors_total: reg.counter("nats_source_fetch_errors_total", "Fetch errors"),
            acks_total: reg.counter("nats_source_acks_total", "Successful acks"),
            ack_errors_total: reg.counter("nats_source_ack_errors_total", "Failed acks"),
            pending_acks: reg.gauge(
                "nats_source_pending_acks",
                "JetStream acks queued or in flight",
            ),
            consumer_lag: reg.gauge(
                "nats_source_consumer_lag",
                "Stream messages not yet delivered to the consumer",
            ),
            _registration: reg.into_registration(),
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

    pub(crate) fn record_ack_enqueued(&self) {
        self.pending_acks.inc();
    }

    #[allow(missing_docs)]
    pub fn record_ack(&self) {
        self.acks_total.inc();
        self.pending_acks.dec();
    }

    #[allow(missing_docs)]
    pub fn record_ack_error(&self) {
        self.ack_errors_total.inc();
        self.pending_acks.dec();
    }

    pub(crate) fn record_ack_enqueue_errors(&self, n: usize) {
        self.ack_errors_total
            .inc_by(u64::try_from(n).unwrap_or(u64::MAX));
    }

    #[allow(missing_docs, clippy::cast_possible_wrap)]
    pub fn record_ack_abandoned(&self, n: usize) {
        self.ack_errors_total
            .inc_by(u64::try_from(n).unwrap_or(u64::MAX));
        self.pending_acks.sub(n as i64);
    }

    pub(crate) fn record_abandoned_acks(&self) {
        let pending = self.pending_acks.get();
        if pending > 0 {
            self.ack_errors_total
                .inc_by(u64::try_from(pending).unwrap_or(u64::MAX));
            self.pending_acks.set(0);
        }
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
    pub pending_futures: IntGauge,
    _registration: Registration,
}

impl NatsSinkMetrics {
    /// Registers on `registry` if provided; otherwise on a local one.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);
        Self {
            records_total: reg.counter("nats_sink_records_total", "Records published"),
            bytes_total: reg.counter("nats_sink_bytes_total", "Payload bytes published"),
            publish_errors_total: reg.counter("nats_sink_publish_errors_total", "Publish errors"),
            ack_errors_total: reg.counter("nats_sink_ack_errors_total", "Publish-ack errors"),
            dedup_total: reg.counter("nats_sink_dedup_total", "Broker-dropped duplicates"),
            pending_futures: reg
                .gauge("nats_sink_pending_futures", "Outstanding PublishAckFutures"),
            _registration: reg.into_registration(),
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

    #[allow(missing_docs, clippy::cast_possible_wrap)]
    pub fn set_pending_futures(&self, n: usize) {
        self.pending_futures.set(n as i64);
    }
}

#[cfg(test)]
mod tests;
