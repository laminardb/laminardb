//! Prometheus metrics for one IBM MQ source or sink, labelled only by its configured name.
//! No queue or message-identifier labels: those are unbounded.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::error::ConnectorError;
use crate::prom::LabelledFamilies;

/// Source metrics.
pub(super) struct SourceMetrics {
    pub(super) messages: IntCounter,
    pub(super) bytes: IntCounter,
    pub(super) rows: IntCounter,
    pub(super) redelivered: IntCounter,
    pub(super) moved: IntCounter,
    pub(super) committed: IntCounter,
    pub(super) uncommitted: IntGauge,
    _families: LabelledFamilies,
}

/// Sink metrics.
pub(super) struct SinkMetrics {
    pub(super) put: IntCounter,
    pub(super) bytes: IntCounter,
    pub(super) commits: IntCounter,
    pub(super) backouts: IntCounter,
    pub(super) ambiguous: IntCounter,
    _families: LabelledFamilies,
}

fn descriptor(error: &prometheus::Error) -> ConnectorError {
    ConnectorError::Internal(format!("IBM MQ metric descriptor: {error}"))
}

impl SourceMetrics {
    /// Metrics for the source named `source`, exported through `registry` when given.
    pub(super) fn new(source: &str, registry: Option<&Registry>) -> Result<Self, ConnectorError> {
        let mut f = LabelledFamilies::new("source", source);
        let built = (|| {
            Ok::<_, prometheus::Error>((
                f.counter(
                    "ibmmq_source_messages_total",
                    "Messages got under syncpoint",
                )?,
                f.counter(
                    "ibmmq_source_bytes_total",
                    "Payload bytes got under syncpoint",
                )?,
                f.counter(
                    "ibmmq_source_rows_total",
                    "Rows decoded from handed-off messages",
                )?,
                f.counter(
                    "ibmmq_source_redelivered_total",
                    "Messages got with a nonzero BackoutCount",
                )?,
                f.counter(
                    "ibmmq_source_backout_moves_total",
                    "Messages moved to the backout queue (poison or undecodable)",
                )?,
                f.counter(
                    "ibmmq_source_committed_total",
                    "Messages committed after a durable checkpoint",
                )?,
                f.gauge(
                    "ibmmq_source_uncommitted",
                    "Messages in the open unit of work awaiting a durable checkpoint",
                )?,
            ))
        })();
        let (messages, bytes, rows, redelivered, moved, committed, uncommitted) =
            built.map_err(|error| descriptor(&error))?;
        Ok(Self {
            messages,
            bytes,
            rows,
            redelivered,
            moved,
            committed,
            uncommitted,
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
                f.counter("ibmmq_sink_put_total", "Messages put under syncpoint")?,
                f.counter(
                    "ibmmq_sink_bytes_total",
                    "Payload bytes put under syncpoint",
                )?,
                f.counter("ibmmq_sink_commits_total", "Units of work committed")?,
                f.counter("ibmmq_sink_backouts_total", "Units of work backed out")?,
                f.counter(
                    "ibmmq_sink_ambiguous_total",
                    "Commits whose outcome is unknown",
                )?,
            ))
        })();
        let (put, bytes, commits, backouts, ambiguous) =
            built.map_err(|error| descriptor(&error))?;
        Ok(Self {
            put,
            bytes,
            commits,
            backouts,
            ambiguous,
            _families: f.register(registry),
        })
    }
}
