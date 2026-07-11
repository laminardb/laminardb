//! NATS sink. Core publishes are fire-and-forget; `JetStream` collects
//! `PublishAckFuture`s and drains them in `flush`.
//! Optional server-side `Nats-Msg-Id` dedup suppresses replay duplicates within
//! the broker window, but is not a coordinated checkpoint commit protocol.

use std::collections::VecDeque;
use std::future::IntoFuture;
use std::time::Duration;

use arrow_array::{cast::AsArray, Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, context::PublishAckFuture};
use async_nats::{Client, HeaderMap};
use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};

use super::config::{build_connect_options, Mode, NatsSinkConfig, SubjectSpec};
use super::metrics::NatsSinkMetrics;
use crate::config::ConnectorConfig;
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::serde::{self, RecordSerializer};

/// A single replica survives a process restart when file-backed, but not a server/node loss.
/// Three is the smallest `JetStream` quorum that preserves availability and one-fault durability.
const MIN_DURABLE_REPLICAS: usize = 3;

fn validate_durable_stream(
    stream_name: &str,
    config: &jetstream::stream::Config,
) -> Result<(), ConnectorError> {
    if config.storage != jetstream::stream::StorageType::File {
        return Err(err(&format!(
            "[LDB-5072] JetStream '{stream_name}' uses {:?} storage; durable at-least-once \
             requires file-backed storage",
            config.storage
        )));
    }
    if config.num_replicas < MIN_DURABLE_REPLICAS {
        return Err(err(&format!(
            "[LDB-5073] JetStream '{stream_name}' has {} replica(s); production durable \
             at-least-once requires at least {MIN_DURABLE_REPLICAS}",
            config.num_replicas
        )));
    }
    if config.no_ack {
        return Err(err(&format!(
            "[LDB-5074] JetStream '{stream_name}' disables publish acknowledgements; durable \
             at-least-once requires broker acknowledgements"
        )));
    }
    Ok(())
}

/// NATS sink — core and `JetStream` modes.
pub struct NatsSink {
    schema: SchemaRef,
    config: Option<NatsSinkConfig>,
    serializer: Option<Box<dyn RecordSerializer>>,
    runtime: Option<Runtime>,
    metrics: NatsSinkMetrics,
    /// Drained in `flush`. A negative/ambiguous acknowledgement consumes its future, so every
    /// drain error must propagate to the owning sink task's sticky epoch fence.
    pending_acks: VecDeque<PublishAckFuture>,
}

enum Runtime {
    Core { client: Client },
    JetStream { context: jetstream::Context },
}

impl NatsSink {
    /// Metrics register on `registry` if provided.
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

    /// Available after [`SinkConnector::open`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSinkConfig> {
        self.config.as_ref()
    }

    /// Snapshot accessor for the prometheus-backed metrics struct.
    #[must_use]
    pub fn metrics_handle(&self) -> &NatsSinkMetrics {
        &self.metrics
    }
}

#[async_trait]
impl SinkConnector for NatsSink {
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let cfg = NatsSinkConfig::from_config(config)?;
        let consistency = match cfg.mode {
            Mode::Core => SinkConsistency::Ephemeral,
            Mode::JetStream => SinkConsistency::DurableAtLeastOnce,
        };
        Ok(SinkContract::new(
            consistency,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ))
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let cfg = NatsSinkConfig::from_config(config)?;
        self.serializer = Some(
            serde::create_serializer(cfg.format)
                .map_err(|e| err(&format!("serializer for format {:?}: {e}", cfg.format)))?,
        );
        let client = build_connect_options(&cfg.auth, &cfg.tls)?
            .connect(&cfg.servers)
            .await
            .map_err(|e| err(&format!("nats connect({:?}): {e}", cfg.servers)))?;
        self.runtime = Some(match cfg.mode {
            Mode::Core => Runtime::Core { client },
            Mode::JetStream => {
                let context = jetstream::new(client);
                let stream_name = cfg
                    .stream
                    .as_deref()
                    .expect("NatsSinkConfig validates the JetStream target");
                let stream = context
                    .get_stream(stream_name)
                    .await
                    .map_err(|e| err(&format!("get_stream('{stream_name}') failed: {e}")))?;
                let info = stream.cached_info();
                validate_durable_stream(stream_name, &info.config)?;
                if cfg.dedup_id_column.is_some() {
                    let actual = info.config.duplicate_window;
                    if actual < cfg.min_duplicate_window {
                        return Err(err(&format!(
                            "[LDB-5056] stream '{stream_name}' has duplicate_window={actual:?}, \
                             below the configured minimum {:?}. Replay or redelivery could land \
                             outside the dedup horizon. Reconfigure the stream or lower \
                             'min.duplicate.window.ms'.",
                            cfg.min_duplicate_window,
                        )));
                    }
                }
                Runtime::JetStream { context }
            }
        });
        self.config = Some(cfg);
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        // Split-borrow: `runtime` &mut, config/serializer &.
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

        let subject_col = match &cfg.subject {
            SubjectSpec::Column(name) => Some(resolve_utf8(batch, name)?),
            SubjectSpec::Literal(_) => None,
        };
        let header_cols: Vec<(&str, &StringArray)> = cfg
            .header_columns
            .iter()
            .map(|n| resolve_utf8(batch, n).map(|arr| (n.as_str(), arr)))
            .collect::<Result<_, _>>()?;
        // The validated target also fences every publish. A subject that resolves to another
        // stream fails at the broker instead of being durably acknowledged to the wrong target.
        let expected_stream = cfg.stream.as_deref();
        // `dedup.id.column` directly enables bounded `Nats-Msg-Id` deduplication. LDB-5056
        // validates the stream's `duplicate_window` at open, but the sink contract remains
        // durable at-least-once because broker dedup is not a coordinated checkpoint commit.
        let dedup_col = cfg
            .dedup_id_column
            .as_deref()
            .map(|n| resolve_utf8(batch, n).map(|arr| (n, arr)))
            .transpose()?;

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
                    if pending_acks.len() >= cfg.max_pending {
                        drain_acks(pending_acks, metrics, cfg.ack_timeout).await?;
                    }
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

            // Per-row so partial failures still credit successes.
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

    fn suggested_write_timeout(&self) -> Duration {
        // The health deadline must include the broker acknowledgement window;
        // otherwise the runtime cancels a healthy configured ack wait early.
        self.config
            .as_ref()
            .map_or(Duration::from_secs(30), |config| config.ack_timeout)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        match self.runtime.as_ref() {
            Some(Runtime::Core { client }) => client
                .flush()
                .await
                .map_err(|e| err(&format!("core flush: {e}")))?,
            Some(Runtime::JetStream { .. }) | None => {}
        }
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(30), |c| c.ack_timeout);
        drain_acks(&mut self.pending_acks, &self.metrics, timeout).await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(5), |c| c.ack_timeout);
        // async-nats buffers core publishes client-side; flush before drop.
        let core_flush = if let Some(Runtime::Core { client }) = self.runtime.as_ref() {
            client
                .flush()
                .await
                .map_err(|error| err(&format!("core close flush: {error}")))
        } else {
            Ok(())
        };
        let ack_drain = drain_acks(&mut self.pending_acks, &self.metrics, timeout).await;
        self.runtime = None;
        match (core_flush, ack_drain) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(core_error), Err(ack_error)) => Err(err(&format!(
                "{core_error}; JetStream acknowledgement drain also failed: {ack_error}"
            ))),
        }
    }
}

/// Drain `pending` concurrently, bounded by `timeout`. On deadline,
/// each still-unresolved ack bumps `record_ack_error` once; the publish
/// may have landed server-side. Broker dedup can suppress a replay within its
/// configured window, but the sink contract remains durable at-least-once.
async fn drain_acks(
    pending: &mut VecDeque<PublishAckFuture>,
    metrics: &NatsSinkMetrics,
    timeout: Duration,
) -> Result<(), ConnectorError> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut set: FuturesUnordered<_> = pending.drain(..).map(IntoFuture::into_future).collect();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut first_err: Option<ConnectorError> = None;

    loop {
        if set.is_empty() {
            break;
        }
        match tokio::time::timeout_at(deadline, set.next()).await {
            Ok(Some(Ok(ack))) => {
                if ack.duplicate {
                    metrics.record_dedup();
                }
            }
            Ok(Some(Err(e))) => {
                metrics.record_ack_error();
                if first_err.is_none() {
                    first_err = Some(err(&format!("jetstream publish ack: {e}")));
                }
            }
            Ok(None) => break,
            Err(_) => {
                let lost = set.len();
                for _ in 0..lost {
                    metrics.record_ack_error();
                }
                metrics.set_pending_futures(pending.len());
                return Err(err(&format!(
                    "jetstream publish ack: timed out with {lost} still in flight"
                )));
            }
        }
    }

    metrics.set_pending_futures(pending.len());
    first_err.map_or(Ok(()), Err)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sink_config(mode: Mode) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("nats");
        config.set("servers", "nats://localhost:4222");
        config.set("subject", "events");
        config.set(
            "mode",
            match mode {
                Mode::Core => "core",
                Mode::JetStream => "jetstream",
            },
        );
        if mode == Mode::JetStream {
            config.set("stream", "EVENTS");
        }
        config
    }

    fn durable_stream_config() -> jetstream::stream::Config {
        jetstream::stream::Config {
            name: "EVENTS".into(),
            storage: jetstream::stream::StorageType::File,
            num_replicas: MIN_DURABLE_REPLICAS,
            no_ack: false,
            ..Default::default()
        }
    }

    #[test]
    fn core_contract_is_ephemeral_multi_writer() {
        let sink = NatsSink::new(std::sync::Arc::new(arrow_schema::Schema::empty()), None);
        let contract = sink.contract(&sink_config(Mode::Core)).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
        assert_eq!(contract.topology, SinkTopology::MultiWriter);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    }

    #[test]
    fn named_jetstream_contract_is_durable_pending_open_validation() {
        let sink = NatsSink::new(std::sync::Arc::new(arrow_schema::Schema::empty()), None);
        let contract = sink.contract(&sink_config(Mode::JetStream)).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
        assert_eq!(contract.topology, SinkTopology::MultiWriter);
    }

    #[test]
    fn durable_stream_validation_accepts_file_quorum_with_acks() {
        validate_durable_stream("EVENTS", &durable_stream_config()).unwrap();
    }

    #[test]
    fn durable_stream_validation_rejects_memory_storage() {
        let mut config = durable_stream_config();
        config.storage = jetstream::stream::StorageType::Memory;
        let error = validate_durable_stream("EVENTS", &config).unwrap_err();
        assert!(error.to_string().contains("LDB-5072"));
    }

    #[test]
    fn durable_stream_validation_rejects_non_quorum_replication() {
        let mut config = durable_stream_config();
        config.num_replicas = MIN_DURABLE_REPLICAS - 1;
        let error = validate_durable_stream("EVENTS", &config).unwrap_err();
        assert!(error.to_string().contains("LDB-5073"));
    }

    #[test]
    fn durable_stream_validation_rejects_disabled_acks() {
        let mut config = durable_stream_config();
        config.no_ack = true;
        let error = validate_durable_stream("EVENTS", &config).unwrap_err();
        assert!(error.to_string().contains("LDB-5074"));
    }
}
