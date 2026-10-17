//! Prometheus metrics for one RabbitMQ source or sink, labelled only by its configured name.
//! No queue, routing-key, or message-identifier labels: those are unbounded.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::error::ConnectorError;
use crate::prom::LabelledFamilies;

/// Source metrics.
pub(super) struct SourceMetrics {
    pub(super) messages: IntCounter,
    pub(super) bytes: IntCounter,
    pub(super) rows: IntCounter,
    pub(super) redelivered: IntCounter,
    pub(super) acked: IntCounter,
    pub(super) rejected: IntCounter,
    pub(super) unacked: IntGauge,
    _families: LabelledFamilies,
}

/// Sink metrics.
pub(super) struct SinkMetrics {
    pub(super) published: IntCounter,
    pub(super) bytes: IntCounter,
    pub(super) confirmed: IntCounter,
    pub(super) nacked: IntCounter,
    pub(super) returned: IntCounter,
    pub(super) ambiguous: IntCounter,
    pub(super) pending: IntGauge,
    pub(super) pending_bytes: IntGauge,
    _families: LabelledFamilies,
}

fn descriptor(error: &prometheus::Error) -> ConnectorError {
    ConnectorError::Internal(format!("RabbitMQ metric descriptor: {error}"))
}

impl SourceMetrics {
    /// Metrics for the source named `source`, exported through `registry` when given.
    pub(super) fn new(source: &str, registry: Option<&Registry>) -> Result<Self, ConnectorError> {
        let mut f = LabelledFamilies::new("source", source);
        let built = (|| {
            Ok::<_, prometheus::Error>((
                f.counter(
                    "rabbitmq_source_messages_total",
                    "Deliveries handed to the pipeline",
                )?,
                f.counter(
                    "rabbitmq_source_bytes_total",
                    "Payload bytes handed to the pipeline",
                )?,
                f.counter(
                    "rabbitmq_source_rows_total",
                    "Rows decoded from handed-off deliveries",
                )?,
                f.counter(
                    "rabbitmq_source_redelivered_total",
                    "Handed-off deliveries the broker flagged as redelivered",
                )?,
                f.counter(
                    "rabbitmq_source_acked_total",
                    "Deliveries acknowledged after a durable checkpoint",
                )?,
                f.counter(
                    "rabbitmq_source_rejected_total",
                    "Deliveries rejected without requeue by on.decode.error = reject",
                )?,
                f.gauge(
                    "rabbitmq_source_unacked",
                    "Handed-off deliveries awaiting a durable checkpoint",
                )?,
            ))
        })();
        let (messages, bytes, rows, redelivered, acked, rejected, unacked) =
            built.map_err(|error| descriptor(&error))?;
        Ok(Self {
            messages,
            bytes,
            rows,
            redelivered,
            acked,
            rejected,
            unacked,
            _families: f.register(registry),
        })
    }
}

impl SinkMetrics {
    /// Metrics for the sink named `sink`, exported through `registry` when given.
    pub(super) fn new(sink: &str, registry: Option<&Registry>) -> Result<Self, ConnectorError> {
        let mut f = LabelledFamilies::new("sink", sink);
        let built = (|| {
            Ok::<_, prometheus::Error>((
                f.counter(
                    "rabbitmq_sink_published_total",
                    "Messages handed to the client",
                )?,
                f.counter(
                    "rabbitmq_sink_bytes_total",
                    "Payload bytes handed to the client",
                )?,
                f.counter(
                    "rabbitmq_sink_confirmed_total",
                    "Publishes the broker confirmed",
                )?,
                f.counter(
                    "rabbitmq_sink_nacked_total",
                    "Publishes the broker negatively confirmed",
                )?,
                f.counter(
                    "rabbitmq_sink_returned_total",
                    "Mandatory publishes returned as unroutable",
                )?,
                f.counter(
                    "rabbitmq_sink_ambiguous_total",
                    "Publishes whose broker outcome is unknown",
                )?,
                f.gauge(
                    "rabbitmq_sink_pending_confirms",
                    "Publishes awaiting a broker confirm",
                )?,
                f.gauge(
                    "rabbitmq_sink_pending_bytes",
                    "Payload bytes awaiting a broker confirm",
                )?,
            ))
        })();
        let (published, bytes, confirmed, nacked, returned, ambiguous, pending, pending_bytes) =
            built.map_err(|error| descriptor(&error))?;
        Ok(Self {
            published,
            bytes,
            confirmed,
            nacked,
            returned,
            ambiguous,
            pending,
            pending_bytes,
            _families: f.register(registry),
        })
    }
}

/// A count or size as a gauge value, saturating at `i64::MAX`.
pub(super) fn saturating(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
