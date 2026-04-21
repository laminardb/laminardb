//! NATS sink connector.
//!
//! At-least-once publishing in this milestone. Core-mode publishes are
//! fire-and-forget; `JetStream` publishes collect `PublishAckFuture`s and
//! drain them in `flush`/`pre_commit`. Exactly-once via `Nats-Msg-Id`
//! dedup lands in a follow-up.

use std::collections::VecDeque;
use std::time::Duration;

use arrow_array::{cast::AsArray, Array, RecordBatch};
use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, context::PublishAckFuture};
use async_nats::{Client, HeaderMap};
use async_trait::async_trait;

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

    fn subject_for_row(&self, batch: &RecordBatch, row: usize) -> Result<String, ConnectorError> {
        let cfg = self.config.as_ref().expect("open() before write_batch");
        match &cfg.subject {
            SubjectSpec::Literal(s) => Ok(s.clone()),
            SubjectSpec::Column(name) => {
                let col = batch
                    .column_by_name(name)
                    .ok_or_else(|| err(&format!("subject.column '{name}' not in batch schema")))?;
                let strings = col.as_string_opt::<i32>().ok_or_else(|| {
                    err(&format!("subject.column '{name}' must be a Utf8 column"))
                })?;
                if strings.is_null(row) {
                    return Err(err(&format!(
                        "subject.column '{name}' is null at row {row}"
                    )));
                }
                Ok(strings.value(row).to_string())
            }
        }
    }

    fn headers_for_row(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Option<HeaderMap>, ConnectorError> {
        let cfg = self.config.as_ref().expect("open() before write_batch");
        if cfg.header_columns.is_empty() && cfg.expected_stream.is_none() {
            return Ok(None);
        }
        let mut headers = HeaderMap::new();
        if let Some(s) = cfg.expected_stream.as_deref() {
            headers.insert("Nats-Expected-Stream", s);
        }
        for col_name in &cfg.header_columns {
            let col = batch
                .column_by_name(col_name)
                .ok_or_else(|| err(&format!("header.columns: '{col_name}' not in batch schema")))?;
            let strings = col
                .as_string_opt::<i32>()
                .ok_or_else(|| err(&format!("header.columns '{col_name}' must be Utf8")))?;
            if !strings.is_null(row) {
                headers.insert(col_name.as_str(), strings.value(row));
            }
        }
        Ok(Some(headers))
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
        let ser = self
            .serializer
            .as_ref()
            .ok_or_else(|| err("serializer not initialized"))?;
        let records = ser
            .serialize(batch)
            .map_err(|e| err(&format!("serialize batch: {e}")))?;

        let mut bytes_total: u64 = 0;
        let rows = batch.num_rows();
        for (row, payload) in records.into_iter().enumerate() {
            if row >= rows {
                break;
            }
            let subject = self.subject_for_row(batch, row)?;
            let headers = self.headers_for_row(batch, row)?;
            bytes_total += payload.len() as u64;
            let payload = bytes::Bytes::from(payload);

            match self.runtime.as_mut() {
                Some(Runtime::Core { client }) => {
                    let result = if let Some(h) = headers {
                        client.publish_with_headers(subject, h, payload).await
                    } else {
                        client.publish(subject, payload).await
                    };
                    result.map_err(|e| err(&format!("core publish: {e}")))?;
                }
                Some(Runtime::JetStream { context }) => {
                    let fut = if let Some(h) = headers {
                        context
                            .publish_with_headers(subject, h, payload)
                            .await
                            .map_err(|e| err(&format!("jetstream publish: {e}")))?
                    } else {
                        context
                            .publish(subject, payload)
                            .await
                            .map_err(|e| err(&format!("jetstream publish: {e}")))?
                    };
                    self.pending_acks.push_back(fut);
                }
                None => return Err(err("sink: open() was not called")),
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
        // Drain whatever has landed without blocking the flow for long.
        // pre_commit does the strict drain with a user-configured timeout.
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
        // Best-effort drain with the sink's ack timeout.
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(5), |c| c.ack_timeout);
        let _ = drain_acks(&mut self.pending_acks, timeout).await;
        self.runtime = None;
        Ok(())
    }
}

async fn drain_acks(
    pending: &mut VecDeque<PublishAckFuture>,
    timeout: Duration,
) -> Result<(), ConnectorError> {
    if pending.is_empty() {
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + timeout;
    while let Some(fut) = pending.pop_front() {
        match tokio::time::timeout_at(deadline, fut).await {
            Ok(Ok(_ack)) => {}
            Ok(Err(e)) => return Err(err(&format!("jetstream publish ack: {e}"))),
            Err(_) => {
                return Err(err(&format!(
                    "jetstream publish ack: timed out after {timeout:?} with acks still outstanding"
                )));
            }
        }
    }
    Ok(())
}

fn err(msg: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(msg.to_string())
}
