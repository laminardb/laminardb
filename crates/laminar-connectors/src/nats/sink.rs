//! NATS sink connector.
//!
//! At-least-once publishing in this milestone. Core-mode publishes are
//! fire-and-forget; `JetStream` publishes collect `PublishAckFuture`s and
//! drain them concurrently in `flush`/`pre_commit`. Exactly-once via
//! `Nats-Msg-Id` dedup lands in a follow-up.

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
use crate::config::ConnectorConfig;
use crate::connector::{DeliveryGuarantee, SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::serde::{self, RecordSerializer};

/// NATS sink — core and `JetStream` modes behind a single type.
pub struct NatsSink {
    schema: SchemaRef,
    config: Option<NatsSinkConfig>,
    serializer: Option<Box<dyn RecordSerializer>>,
    runtime: Option<Runtime>,
    /// Publish acks from the in-flight epoch, drained in `flush`/`pre_commit`.
    pending_acks: VecDeque<PublishAckFuture>,
}

enum Runtime {
    Core { client: Client },
    JetStream { context: jetstream::Context },
}

impl NatsSink {
    /// Creates a new NATS sink with the given input schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            config: None,
            serializer: None,
            runtime: None,
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
                    // Touch the stream now so a bad name fails open(), not
                    // later on the first publish.
                    context
                        .get_stream(stream_name)
                        .await
                        .map_err(|e| err(&format!("get_stream('{stream_name}') failed: {e}")))?;
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

        let records = ser
            .serialize(batch)
            .map_err(|e| err(&format!("serialize batch: {e}")))?;

        let mut bytes_total: u64 = 0;
        let rows = batch.num_rows();
        for (row, payload) in records.into_iter().enumerate() {
            let subject: &str = match (&cfg.subject, subject_col) {
                (SubjectSpec::Literal(s), _) => s.as_str(),
                (SubjectSpec::Column(name), Some(arr)) => {
                    if arr.is_null(row) {
                        return Err(err(&format!(
                            "subject.column '{name}' is null at row {row}"
                        )));
                    }
                    arr.value(row)
                }
                (SubjectSpec::Column(_), None) => unreachable!("resolved above"),
            };
            let headers = build_headers(expected_stream, &header_cols, row);
            let payload_len = payload.len() as u64;
            let payload = bytes::Bytes::from(payload);
            bytes_total += payload_len;

            match rt {
                Runtime::Core { client } => {
                    let result = if let Some(h) = headers {
                        client
                            .publish_with_headers(subject.to_string(), h, payload)
                            .await
                    } else {
                        client.publish(subject.to_string(), payload).await
                    };
                    result.map_err(|e| err(&format!("core publish: {e}")))?;
                }
                Runtime::JetStream { context } => {
                    let fut = if let Some(h) = headers {
                        context
                            .publish_with_headers(subject.to_string(), h, payload)
                            .await
                            .map_err(|e| err(&format!("jetstream publish: {e}")))?
                    } else {
                        context
                            .publish(subject.to_string(), payload)
                            .await
                            .map_err(|e| err(&format!("jetstream publish: {e}")))?
                    };
                    pending_acks.push_back(fut);
                }
            }
        }

        Ok(WriteResult::new(rows, bytes_total))
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
        // Drain whatever has landed, bounded by 1s so a slow round-trip
        // doesn't stall the periodic flush timer. pre_commit uses the
        // user-configured ack timeout for the strict drain.
        drain_acks(&mut self.pending_acks, Duration::from_secs(1)).await
    }

    async fn pre_commit(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(30), |c| c.ack_timeout);
        drain_acks(&mut self.pending_acks, timeout).await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(5), |c| c.ack_timeout);
        let _ = drain_acks(&mut self.pending_acks, timeout).await;
        self.runtime = None;
        Ok(())
    }
}

/// Concurrently drain every `PublishAckFuture` in `pending`, bounded by
/// `timeout`. Matches the Kafka sink's rollback shape — serial drains
/// killed throughput. `PublishAckFuture` only implements `IntoFuture`,
/// hence the explicit conversion.
async fn drain_acks(
    pending: &mut VecDeque<PublishAckFuture>,
    timeout: Duration,
) -> Result<(), ConnectorError> {
    if pending.is_empty() {
        return Ok(());
    }
    let futures: Vec<_> = pending.drain(..).map(IntoFuture::into_future).collect();
    match tokio::time::timeout(timeout, try_join_all(futures)).await {
        Ok(Ok(_acks)) => Ok(()),
        Ok(Err(e)) => Err(err(&format!("jetstream publish ack: {e}"))),
        Err(_) => Err(err(&format!(
            "jetstream publish ack: timed out after {timeout:?}"
        ))),
    }
}

fn resolve_utf8<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, ConnectorError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| err(&format!("column '{name}' not in batch schema")))?;
    col.as_string_opt::<i32>()
        .ok_or_else(|| err(&format!("column '{name}' must be Utf8")))
}

fn build_headers(
    expected_stream: Option<&str>,
    header_cols: &[(&str, &StringArray)],
    row: usize,
) -> Option<HeaderMap> {
    if header_cols.is_empty() && expected_stream.is_none() {
        return None;
    }
    let mut h = HeaderMap::new();
    if let Some(s) = expected_stream {
        h.insert("Nats-Expected-Stream", s);
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
