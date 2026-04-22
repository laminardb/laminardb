//! NATS sink connector.
//!
//! Core-mode publishes are fire-and-forget. `JetStream` publishes collect
//! `PublishAckFuture`s and drain them concurrently in `flush`/`pre_commit`.
//! Exactly-once uses server-side `Nats-Msg-Id` dedup: the sink refuses to
//! start unless the target stream's `duplicate_window` is at least
//! `min.duplicate.window.ms`, so rollback redelivery always lands inside
//! the dedup horizon.

use std::collections::VecDeque;
use std::future::IntoFuture;
use std::time::Duration;

use arrow_array::{cast::AsArray, Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, context::PublishAckFuture};
use async_nats::{Client, HeaderMap};
use async_trait::async_trait;
use futures_util::future::try_join_all;

use super::config::{Mode, NatsSinkConfig, SubjectSpec};
use super::metrics::NatsSinkMetrics;
use crate::config::ConnectorConfig;
use crate::connector::{DeliveryGuarantee, SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;
use crate::serde::{self, RecordSerializer};

/// NATS sink — core and `JetStream` modes behind a single type.
pub struct NatsSink {
    schema: SchemaRef,
    config: Option<NatsSinkConfig>,
    serializer: Option<Box<dyn RecordSerializer>>,
    runtime: Option<Runtime>,
    metrics: NatsSinkMetrics,
    /// Publish acks from the in-flight epoch, drained in `flush`/`pre_commit`.
    pending_acks: VecDeque<PublishAckFuture>,
}

enum Runtime {
    Core { client: Client },
    JetStream { context: jetstream::Context },
}

impl NatsSink {
    /// Creates a new NATS sink with the given input schema.
    ///
    /// If `registry` is `Some`, metrics register on it.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        Self {
            schema,
            config: None,
            serializer: None,
            runtime: None,
            metrics: NatsSinkMetrics::new(registry),
            pending_acks: VecDeque::new(),
        }
    }

    /// Parsed config — available after [`SinkConnector::open`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSinkConfig> {
        self.config.as_ref()
    }
}

#[async_trait]
impl SinkConnector for NatsSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let cfg = NatsSinkConfig::from_config(config)?;
        self.serializer = Some(
            serde::create_serializer(cfg.format)
                .map_err(|e| err(&format!("serializer for format {:?}: {e}", cfg.format)))?,
        );
        let client = async_nats::ConnectOptions::new()
            .connect(&cfg.servers)
            .await
            .map_err(|e| err(&format!("nats connect({:?}): {e}", cfg.servers)))?;
        self.runtime = Some(match cfg.mode {
            Mode::Core => Runtime::Core { client },
            Mode::JetStream => {
                let context = jetstream::new(client);
                if let Some(stream_name) = cfg.stream.as_deref() {
                    // Touch the stream now so a bad name fails open(),
                    // not later on the first publish.
                    let stream = context
                        .get_stream(stream_name)
                        .await
                        .map_err(|e| err(&format!("get_stream('{stream_name}') failed: {e}")))?;
                    if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
                        let info = stream.cached_info();
                        let actual = info.config.duplicate_window;
                        if actual < cfg.min_duplicate_window {
                            return Err(err(&format!(
                                "[LDB-5056] stream '{stream_name}' has duplicate_window={actual:?}, \
                                 below the configured minimum {:?}. Rollback redelivery could land \
                                 outside the dedup horizon. Reconfigure the stream or lower \
                                 'min.duplicate.window.ms'.",
                                cfg.min_duplicate_window,
                            )));
                        }
                    }
                }
                Runtime::JetStream { context }
            }
        });
        self.config = Some(cfg);
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        // Split-borrow `self` so the runtime can be &mut while we keep
        // immutable refs to config + serializer.
        let Self {
            config,
            serializer,
            runtime,
            pending_acks,
            metrics,
            schema: _,
        } = self;
        let cfg = config.as_ref().ok_or_else(|| err("sink: open() first"))?;
        let ser = serializer
            .as_ref()
            .ok_or_else(|| err("sink: open() first"))?;
        let rt = runtime.as_mut().ok_or_else(|| err("sink: open() first"))?;

        // Resolve column references once per batch so the per-row loop
        // does no hashmap lookups.
        let subject_col = match &cfg.subject {
            SubjectSpec::Column(name) => Some(resolve_utf8(batch, name)?),
            SubjectSpec::Literal(_) => None,
        };
        let header_cols: Vec<(&str, &StringArray)> = cfg
            .header_columns
            .iter()
            .map(|n| resolve_utf8(batch, n).map(|arr| (n.as_str(), arr)))
            .collect::<Result<_, _>>()?;
        let expected_stream = cfg.expected_stream.as_deref();
        // Only emit Nats-Msg-Id under exactly-once — that's the delivery
        // mode whose duplicate_window we validate at open(). Honoring
        // the column under at-least-once would give silent dedup without
        // the safety check, which is the worst of both worlds.
        let dedup_col = if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            cfg.dedup_id_column
                .as_deref()
                .map(|n| resolve_utf8(batch, n).map(|arr| (n, arr)))
                .transpose()?
        } else {
            None
        };

        let records = ser
            .serialize(batch)
            .map_err(|e| err(&format!("serialize batch: {e}")))?;

        let mut bytes_total: u64 = 0;
        let mut rows_written: usize = 0;
        for (row, payload) in records.into_iter().enumerate() {
            let subject: &str = match (&cfg.subject, subject_col) {
                (SubjectSpec::Literal(s), _) => s.as_str(),
                (SubjectSpec::Column(name), Some(arr)) => {
                    non_null(arr, row, "subject.column", name)?
                }
                (SubjectSpec::Column(_), None) => unreachable!("resolved above"),
            };
            let msg_id = dedup_col
                .map(|(name, arr)| non_null(arr, row, "dedup.id.column", name))
                .transpose()?;
            let headers = build_headers(expected_stream, msg_id, &header_cols, row);
            let payload_len = payload.len() as u64;
            let payload = bytes::Bytes::from(payload);

            match rt {
                Runtime::Core { client } => {
                    let result = if let Some(h) = headers {
                        client
                            .publish_with_headers(subject.to_string(), h, payload)
                            .await
                    } else {
                        client.publish(subject.to_string(), payload).await
                    };
                    if let Err(e) = result {
                        metrics.record_publish_error();
                        return Err(err(&format!("core publish: {e}")));
                    }
                }
                Runtime::JetStream { context } => {
                    let publish_result = if let Some(h) = headers {
                        context
                            .publish_with_headers(subject.to_string(), h, payload)
                            .await
                    } else {
                        context.publish(subject.to_string(), payload).await
                    };
                    match publish_result {
                        Ok(fut) => pending_acks.push_back(fut),
                        Err(e) => {
                            metrics.record_publish_error();
                            return Err(err(&format!("jetstream publish: {e}")));
                        }
                    }
                }
            }

            // Record per-row so partial-failure batches don't vanish
            // from the success counter.
            metrics.record_published_row(payload_len);
            rows_written += 1;
            bytes_total += payload_len;
        }

        metrics.set_pending_futures(pending_acks.len());
        Ok(WriteResult::new(rows_written, bytes_total))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = SinkConnectorCapabilities::new(Duration::from_secs(5))
            .with_idempotent()
            .with_partitioned();
        if matches!(
            self.config.as_ref().map(|c| c.delivery_guarantee),
            Some(DeliveryGuarantee::ExactlyOnce)
        ) {
            caps = caps.with_exactly_once().with_two_phase_commit();
        }
        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        // Push buffered core publishes to the wire; drain landed JS acks
        // (bounded so a slow round-trip doesn't stall the flush timer).
        match self.runtime.as_ref() {
            Some(Runtime::Core { client }) => client
                .flush()
                .await
                .map_err(|e| err(&format!("core flush: {e}")))?,
            Some(Runtime::JetStream { .. }) | None => {}
        }
        drain_acks(
            &mut self.pending_acks,
            &self.metrics,
            Duration::from_secs(1),
        )
        .await
    }

    async fn pre_commit(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(30), |c| c.ack_timeout);
        drain_acks(&mut self.pending_acks, &self.metrics, timeout).await
    }

    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        // Dedup handles any landed publishes on retry (see LDB-5056).
        self.pending_acks.clear();
        self.metrics.set_pending_futures(0);
        self.metrics.record_rollback();
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(5), |c| c.ack_timeout);
        // Flush any buffered core publishes before dropping the client —
        // async-nats queues publishes locally and they'd be lost on a
        // bare drop.
        if let Some(Runtime::Core { client }) = self.runtime.as_ref() {
            let _ = client.flush().await;
        }
        let _ = drain_acks(&mut self.pending_acks, &self.metrics, timeout).await;
        self.runtime = None;
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.config.as_ref() {
            None => HealthStatus::Unknown,
            Some(cfg) => {
                let cap = cfg.max_pending.max(1) as u64;
                #[allow(clippy::cast_sign_loss)]
                let pending = self.metrics.pending_futures.get().max(0) as u64;
                if pending * 2 >= cap {
                    HealthStatus::Degraded(format!(
                        "pending publish acks {pending}/{cap} — ack drain may stall pre_commit"
                    ))
                } else {
                    HealthStatus::Healthy
                }
            }
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }
}

/// Concurrently drain every `PublishAckFuture` in `pending`, bounded by
/// `timeout`. Matches the Kafka sink's rollback shape — serial drains
/// killed throughput. `PublishAckFuture` only implements `IntoFuture`,
/// hence the explicit conversion. Counts server-identified duplicates
/// via `PubAck.duplicate`.
async fn drain_acks(
    pending: &mut VecDeque<PublishAckFuture>,
    metrics: &NatsSinkMetrics,
    timeout: Duration,
) -> Result<(), ConnectorError> {
    if pending.is_empty() {
        return Ok(());
    }
    let futures: Vec<_> = pending.drain(..).map(IntoFuture::into_future).collect();
    let result = match tokio::time::timeout(timeout, try_join_all(futures)).await {
        Ok(Ok(acks)) => {
            for ack in acks {
                if ack.duplicate {
                    metrics.record_dedup();
                }
            }
            Ok(())
        }
        Ok(Err(e)) => {
            metrics.record_ack_error();
            Err(err(&format!("jetstream publish ack: {e}")))
        }
        Err(_) => {
            metrics.record_ack_error();
            Err(err(&format!(
                "jetstream publish ack: timed out after {timeout:?}"
            )))
        }
    };
    metrics.set_pending_futures(pending.len());
    result
}

fn resolve_utf8<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, ConnectorError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| err(&format!("column '{name}' not in batch schema")))?;
    col.as_string_opt::<i32>()
        .ok_or_else(|| err(&format!("column '{name}' must be Utf8")))
}

fn non_null<'a>(
    arr: &'a StringArray,
    row: usize,
    kind: &str,
    name: &str,
) -> Result<&'a str, ConnectorError> {
    if arr.is_null(row) {
        Err(err(&format!("{kind} '{name}' is null at row {row}")))
    } else {
        Ok(arr.value(row))
    }
}

fn build_headers(
    expected_stream: Option<&str>,
    msg_id: Option<&str>,
    header_cols: &[(&str, &StringArray)],
    row: usize,
) -> Option<HeaderMap> {
    if header_cols.is_empty() && expected_stream.is_none() && msg_id.is_none() {
        return None;
    }
    let mut h = HeaderMap::new();
    if let Some(s) = expected_stream {
        h.insert("Nats-Expected-Stream", s);
    }
    if let Some(id) = msg_id {
        h.insert("Nats-Msg-Id", id);
    }
    for (name, arr) in header_cols {
        if !arr.is_null(row) {
            h.insert(*name, arr.value(row));
        }
    }
    Some(h)
}

fn err(msg: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(msg.to_string())
}
