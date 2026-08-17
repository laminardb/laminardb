//! NATS sink. Core publishes are fire-and-forget; `JetStream` collects
//! `PublishAckFuture`s and drains them in `flush`.
//! Optional server-side `Nats-Msg-Id` dedup suppresses replay duplicates within
//! the broker window, but is not a coordinated checkpoint commit protocol.

use std::collections::VecDeque;
use std::future::IntoFuture;
use std::str::FromStr;
use std::time::Duration;

use arrow_array::{cast::AsArray, Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, context::PublishAckFuture};
use async_nats::{Client, HeaderMap, HeaderName, HeaderValue, Subject};
use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};

use super::config::{build_connect_options, Mode, NatsSinkConfig, SubjectSpec};
use super::metrics::NatsSinkMetrics;
use super::setup::{classify_connect_error, classify_get_stream_error, track_connection_tasks};
use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::serde::{self, RecordSerializer};

mod message;

#[cfg(test)]
use message::encoded_header_len;
use message::{
    build_headers, operation_has_prior_output, resolve_utf8, validate_headers_and_encoded_len,
    validate_message_size, validate_non_null, validate_publish_subjects,
};

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
    /// Core publish only proves enqueue into the client. Keep that uncertainty across batches
    /// until the client transport has flushed its write buffer.
    core_dirty: bool,
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

enum Runtime {
    Core { client: Client },
    JetStream { context: jetstream::Context },
}

async fn bounded_nats_setup_until<T, E>(
    deadline: tokio::time::Instant,
    total_timeout: Duration,
    future: impl std::future::Future<Output = Result<T, E>>,
    classify: impl FnOnce(E) -> ConnectorError,
) -> Result<T, ConnectorError> {
    match tokio::time::timeout_at(deadline, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(classify(error)),
        Err(_) => Err(ConnectorError::Timeout(timeout_millis(total_timeout))),
    }
}

#[derive(Debug)]
enum PublishEnqueueFailure<E> {
    Client(E),
    TimedOut(Duration),
}

async fn bounded_publish_enqueue<T, E>(
    deadline: tokio::time::Instant,
    total_timeout: Duration,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, PublishEnqueueFailure<E>> {
    match tokio::time::timeout_at(deadline, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(PublishEnqueueFailure::Client(error)),
        Err(_) => Err(PublishEnqueueFailure::TimedOut(total_timeout)),
    }
}

fn known_not_dispatched_error(
    operation: &str,
    detail: impl std::fmt::Display,
    retryable: bool,
    prior_output: bool,
) -> ConnectorError {
    if prior_output {
        ConnectorError::outcome_unknown(
            format!(
                "{operation} did not dispatch the current message, but prior NATS output may \
                 already have been applied: {detail}"
            ),
            retryable,
        )
    } else if retryable {
        ConnectorError::WriteError(format!(
            "{operation} did not dispatch the message: {detail}"
        ))
    } else {
        ConnectorError::ConfigurationError(format!(
            "{operation} rejected the message before dispatch: {detail}"
        ))
    }
}

fn preserve_prior_applied(error: ConnectorError, applied: usize) -> ConnectorError {
    if applied == 0 || error.is_outcome_unknown() {
        error
    } else {
        let retryable = error.is_transient();
        ConnectorError::outcome_unknown(
            format!(
                "JetStream acknowledgement failed after {applied} earlier publish(es) were \
                 acknowledged: {error}"
            ),
            retryable,
        )
    }
}

fn classify_core_publish_failure(
    failure: PublishEnqueueFailure<async_nats::PublishError>,
    prior_output: bool,
) -> ConnectorError {
    let (detail, retryable) = match failure {
        PublishEnqueueFailure::Client(error) => {
            let retryable = matches!(error.kind(), async_nats::client::PublishErrorKind::Send);
            (error.to_string(), retryable)
        }
        PublishEnqueueFailure::TimedOut(timeout) => (
            format!(
                "client enqueue timed out after {}ms",
                timeout_millis(timeout)
            ),
            true,
        ),
    };
    known_not_dispatched_error("NATS core publish", detail, retryable, prior_output)
}

fn classify_jetstream_enqueue_failure(
    failure: PublishEnqueueFailure<jetstream::context::PublishError>,
    prior_output: bool,
) -> ConnectorError {
    let (detail, retryable) = match failure {
        PublishEnqueueFailure::Client(error) => {
            use jetstream::context::PublishErrorKind;

            let invalid_subject = std::error::Error::source(&error)
                .is_some_and(<dyn std::error::Error>::is::<async_nats::SubjectError>);
            let retryable = match error.kind() {
                PublishErrorKind::StreamNotFound
                | PublishErrorKind::WrongLastMessageId
                | PublishErrorKind::WrongLastSequence => false,
                PublishErrorKind::Other if invalid_subject => false,
                PublishErrorKind::TimedOut
                | PublishErrorKind::BrokenPipe
                | PublishErrorKind::MaxAckPending
                | PublishErrorKind::Other => true,
            };
            (error.to_string(), retryable)
        }
        PublishEnqueueFailure::TimedOut(timeout) => (
            format!(
                "client enqueue timed out after {}ms",
                timeout_millis(timeout)
            ),
            true,
        ),
    };
    known_not_dispatched_error("NATS JetStream publish", detail, retryable, prior_output)
}

fn timeout_millis(timeout: Duration) -> u64 {
    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
}

async fn flush_core(
    client: &Client,
    timeout: Duration,
    operation: &'static str,
) -> Result<(), ConnectorError> {
    match tokio::time::timeout(timeout, client.flush()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ConnectorError::outcome_unknown(
            format!(
                "{operation} failed after Core publishes were enqueued; whether their bytes \
                 reached the transport is unknown: {error}"
            ),
            true,
        )),
        Err(_) => Err(ConnectorError::outcome_unknown(
            format!(
                "{operation} timed out after {}ms with Core publishes still buffered or in flight",
                timeout_millis(timeout)
            ),
            true,
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AckCertainty {
    Rejected,
    OutcomeUnknown,
}

#[derive(Debug)]
struct AckFailure {
    certainty: AckCertainty,
    detail: String,
    retryable: bool,
}

fn classify_jetstream_ack_failure(error: &jetstream::context::PublishError) -> AckFailure {
    use jetstream::context::PublishErrorKind;

    let source = std::error::Error::source(error);
    let (certainty, retryable) = match error.kind() {
        PublishErrorKind::StreamNotFound
        | PublishErrorKind::WrongLastMessageId
        | PublishErrorKind::WrongLastSequence => (AckCertainty::Rejected, false),
        PublishErrorKind::TimedOut | PublishErrorKind::BrokenPipe => {
            (AckCertainty::OutcomeUnknown, true)
        }
        PublishErrorKind::Other => {
            if let Some(broker_error) =
                source.and_then(|source| source.downcast_ref::<jetstream::Error>())
            {
                let retryable = matches!(broker_error.code(), 408 | 429 | 500..=599);
                if retryable {
                    (AckCertainty::OutcomeUnknown, true)
                } else {
                    (AckCertainty::Rejected, false)
                }
            } else if source.is_some_and(<dyn std::error::Error>::is::<serde_json::Error>) {
                // A response arrived, but an invalid ack cannot prove whether persistence happened.
                (AckCertainty::OutcomeUnknown, false)
            } else {
                // `PublishAckFuture` currently reports `Other` only with a broker error or a
                // malformed acknowledgement as its source. An untyped fallback is a client
                // contract violation, not a reason to retry forever.
                (AckCertainty::OutcomeUnknown, false)
            }
        }
        // This kind belongs to pre-dispatch semaphore acquisition. Seeing it on an ack future
        // would violate the client contract, so fail closed rather than claim a rejection.
        PublishErrorKind::MaxAckPending => (AckCertainty::OutcomeUnknown, false),
    };
    AckFailure {
        certainty,
        detail: error.to_string(),
        retryable,
    }
}

struct AckAggregate {
    total: usize,
    applied: usize,
    rejected: usize,
    ambiguous: usize,
    first_error: Option<String>,
    retryable: bool,
}

impl Default for AckAggregate {
    fn default() -> Self {
        Self {
            total: 0,
            applied: 0,
            rejected: 0,
            ambiguous: 0,
            first_error: None,
            retryable: true,
        }
    }
}

impl AckAggregate {
    fn record_applied(&mut self) {
        self.total += 1;
        self.applied += 1;
    }

    fn record_failure(&mut self, failure: AckFailure) {
        self.total += 1;
        self.retryable &= failure.retryable;
        match failure.certainty {
            AckCertainty::Rejected => self.rejected += 1,
            AckCertainty::OutcomeUnknown => self.ambiguous += 1,
        }
        self.first_error.get_or_insert(failure.detail);
    }

    fn record_unresolved_timeout(&mut self, count: usize, timeout: Duration) {
        self.total += count;
        self.ambiguous += count;
        self.first_error.get_or_insert_with(|| {
            format!(
                "{count} acknowledgement(s) remained in flight after {}ms",
                timeout_millis(timeout)
            )
        });
    }

    fn into_result(self) -> Result<usize, ConnectorError> {
        if self.rejected == 0 && self.ambiguous == 0 {
            return Ok(self.applied);
        }
        let detail = format!(
            "{} rejected, {} outcome unknown, {} acknowledged out of {}; first error: {}",
            self.rejected,
            self.ambiguous,
            self.applied,
            self.total,
            self.first_error.unwrap_or_else(|| "unknown".into())
        );
        if self.ambiguous > 0 || self.applied > 0 {
            Err(ConnectorError::outcome_unknown(
                format!("JetStream acknowledgement drain was not atomic: {detail}"),
                self.retryable,
            ))
        } else if self.retryable {
            Err(ConnectorError::WriteError(format!(
                "JetStream rejected every publish acknowledgement: {detail}"
            )))
        } else {
            Err(ConnectorError::ConfigurationError(format!(
                "JetStream rejected every publish acknowledgement: {detail}"
            )))
        }
    }
}

impl NatsSink {
    /// Metrics register on `registry` if provided.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            schema,
            config: None,
            serializer: None,
            runtime: None,
            metrics: NatsSinkMetrics::new(registry),
            pending_acks: VecDeque::new(),
            core_dirty: false,
            task_owner,
            task_tracker,
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

impl Drop for NatsSink {
    fn drop(&mut self) {
        retire_pending_acks(&mut self.pending_acks, &self.metrics, &self.task_owner);
    }
}

// `async_trait` reports this cohesive lifecycle implementation as one generated function.
#[async_trait]
impl SinkConnector for NatsSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

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
        if self.core_dirty {
            return Err(ConnectorError::InvalidState {
                expected: "a new sink generation after an unresolved Core flush".into(),
                actual: "this sink still has unconfirmed Core publishes".into(),
            });
        }
        let cfg = NatsSinkConfig::from_config(config)?;
        self.serializer = Some(
            serde::create_serializer(cfg.format)
                .map_err(|e| err(&format!("serializer for format {:?}: {e}", cfg.format)))?,
        );
        let setup_timeout = cfg.ack_timeout;
        let setup_deadline = tokio::time::Instant::now() + setup_timeout;
        let connect_options = track_connection_tasks(
            build_connect_options(&cfg.auth, &cfg.tls)?,
            &self.task_owner,
            "sink",
        )?;
        let client = bounded_nats_setup_until(
            setup_deadline,
            setup_timeout,
            Box::pin(connect_options.connect(&cfg.servers)),
            |error| classify_connect_error(&error),
        )
        .await?;
        self.runtime = Some(match cfg.mode {
            Mode::Core => Runtime::Core { client },
            Mode::JetStream => {
                // Match the connector's retained-ack bound to the client's semaphore and fail
                // immediately if the invariant is ever violated. Waiting for a permit while this
                // connector owns every outstanding ack would deadlock the sink actor.
                let context = jetstream::context::ContextBuilder::new()
                    .timeout(cfg.ack_timeout)
                    .ack_timeout(cfg.ack_timeout)
                    .max_ack_inflight(cfg.max_pending)
                    .backpressure_on_inflight(false)
                    .build(client);
                let stream_name = cfg
                    .stream
                    .as_deref()
                    .expect("NatsSinkConfig validates the JetStream target");
                let stream = bounded_nats_setup_until(
                    setup_deadline,
                    setup_timeout,
                    context.get_stream(stream_name),
                    |error| classify_get_stream_error(&error, stream_name),
                )
                .await?;
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
            core_dirty,
            metrics,
            task_owner,
            task_tracker: _,
            schema: _,
        } = self;
        let cfg = config.as_ref().ok_or_else(|| err("sink: open() first"))?;
        // The runtime advertises this exact bound for the complete call. Every enqueue and
        // acknowledgement wait shares it; row count cannot multiply the configured timeout.
        let write_timeout = cfg.ack_timeout;
        let write_deadline = tokio::time::Instant::now() + write_timeout;
        let ser = serializer
            .as_ref()
            .ok_or_else(|| err("sink: open() first"))?;
        let rt = runtime.as_mut().ok_or_else(|| err("sink: open() first"))?;

        let subject_col = match &cfg.subject {
            SubjectSpec::Column(name) => Some(resolve_utf8(batch, name)?),
            SubjectSpec::Literal(_) => None,
        };
        let header_cols: Vec<(&HeaderName, &StringArray)> = cfg
            .header_columns
            .iter()
            .map(|name| resolve_utf8(batch, name.as_ref()).map(|array| (name, array)))
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

        // Validate required per-row routing metadata before the first publish. A late null would
        // otherwise turn a deterministic input error into a partially dispatched batch.
        if let Some(arr) = subject_col {
            let SubjectSpec::Column(name) = &cfg.subject else {
                unreachable!("subject column exists only for a column subject")
            };
            validate_non_null(arr, "subject.column", name)?;
        }
        validate_publish_subjects(&cfg.subject, subject_col, batch.num_rows())?;
        if let Some((name, arr)) = dedup_col {
            validate_non_null(arr, "dedup.id.column", name)?;
        }

        let records = ser
            .serialize(batch)
            .map_err(|e| err(&format!("serialize batch: {e}")))?;
        if records.len() != batch.num_rows() {
            return Err(ConnectorError::Internal(format!(
                "NATS serializer returned {} records for a {}-row batch",
                records.len(),
                batch.num_rows()
            )));
        }

        // Validate every deterministic message property before the first publish. This keeps a
        // malformed later row from turning an input/configuration error into a partially applied
        // batch. async-nats checks plain payload size itself, but its header-publish path does not.
        let max_payload = match &*rt {
            Runtime::Core { client } => client.server_info().max_payload,
            Runtime::JetStream { context } => context.client().server_info().max_payload,
        };
        for (row, payload) in records.iter().enumerate() {
            let msg_id = dedup_col.map(|(_, array)| array.value(row));
            let header_len =
                validate_headers_and_encoded_len(expected_stream, msg_id, &header_cols, row)?;
            validate_message_size(row, payload.len(), header_len, max_payload)?;
        }

        let mut bytes_total: u64 = 0;
        let mut rows_written: usize = 0;
        let mut acknowledged_in_write: usize = 0;
        for (row, payload) in records.into_iter().enumerate() {
            let subject = match (&cfg.subject, subject_col) {
                (SubjectSpec::Literal(subject), _) => subject.as_str(),
                (SubjectSpec::Column(_), Some(array)) => array.value(row),
                (SubjectSpec::Column(_), None) => unreachable!("resolved above"),
            };
            let subject = Subject::from(subject);
            let msg_id = dedup_col.map(|(_, array)| array.value(row));
            let headers = build_headers(expected_stream, msg_id, &header_cols, row)?;
            let payload = bytes::Bytes::from(payload);
            let payload_len = payload.len() as u64;

            match rt {
                Runtime::Core { client } => {
                    let prior_output = *core_dirty;
                    let result = if let Some(h) = headers {
                        bounded_publish_enqueue(
                            write_deadline,
                            write_timeout,
                            client.publish_with_headers(subject, h, payload),
                        )
                        .await
                    } else {
                        bounded_publish_enqueue(
                            write_deadline,
                            write_timeout,
                            client.publish(subject, payload),
                        )
                        .await
                    };
                    match result {
                        Ok(()) => *core_dirty = true,
                        Err(failure) => {
                            metrics.record_publish_error();
                            return Err(classify_core_publish_failure(failure, prior_output));
                        }
                    }
                }
                Runtime::JetStream { context } => {
                    if pending_acks.len() >= cfg.max_pending {
                        let acknowledged = drain_acks_until(
                            pending_acks,
                            metrics,
                            task_owner,
                            write_deadline,
                            write_timeout,
                        )
                        .await
                        .map_err(|error| preserve_prior_applied(error, acknowledged_in_write))?;
                        acknowledged_in_write += acknowledged;
                    }
                    let prior_output = operation_has_prior_output(
                        rows_written,
                        pending_acks.len(),
                        acknowledged_in_write,
                    );
                    let publish_result = if let Some(h) = headers {
                        bounded_publish_enqueue(
                            write_deadline,
                            write_timeout,
                            context.publish_with_headers(subject, h, payload),
                        )
                        .await
                    } else {
                        bounded_publish_enqueue(
                            write_deadline,
                            write_timeout,
                            context.publish(subject, payload),
                        )
                        .await
                    };
                    match publish_result {
                        Ok(fut) => pending_acks.push_back(fut),
                        Err(failure) => {
                            metrics.record_publish_error();
                            metrics.set_pending_futures(pending_acks.len());
                            return Err(classify_jetstream_enqueue_failure(failure, prior_output));
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
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(30), |c| c.ack_timeout);
        if let Some(Runtime::Core { client }) = self.runtime.as_ref() {
            if !self.core_dirty {
                return Ok(());
            }
            let result = flush_core(client, timeout, "NATS core flush").await;
            if result.is_ok() {
                self.core_dirty = false;
            }
            return result;
        }
        drain_acks(
            &mut self.pending_acks,
            &self.metrics,
            &self.task_owner,
            timeout,
        )
        .await
        .map(|_| ())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let timeout = self
            .config
            .as_ref()
            .map_or(Duration::from_secs(5), |c| c.ack_timeout);
        let result = if let Some(Runtime::Core { client }) = self.runtime.as_ref() {
            // async-nats buffers Core publishes client-side; empty its transport buffer before
            // drop. This is not a broker acknowledgement, so Core remains explicitly ephemeral.
            if self.core_dirty {
                let result = flush_core(client, timeout, "NATS core close flush").await;
                if result.is_ok() {
                    self.core_dirty = false;
                }
                result
            } else {
                Ok(())
            }
        } else {
            drain_acks(
                &mut self.pending_acks,
                &self.metrics,
                &self.task_owner,
                timeout,
            )
            .await
            .map(|_| ())
        };
        self.runtime = None;
        result
    }
}

/// Drain `pending` concurrently, bounded by `timeout`. On deadline,
/// each still-unresolved ack bumps `record_ack_error` once; the publish
/// may have landed server-side. Broker dedup can suppress a replay within its
/// configured window, but the sink contract remains durable at-least-once.
async fn drain_acks(
    pending: &mut VecDeque<PublishAckFuture>,
    metrics: &NatsSinkMetrics,
    task_owner: &ConnectorTaskOwner,
    timeout: Duration,
) -> Result<usize, ConnectorError> {
    let deadline = tokio::time::Instant::now() + timeout;
    drain_acks_until(pending, metrics, task_owner, deadline, timeout).await
}

async fn drain_acks_until(
    pending: &mut VecDeque<PublishAckFuture>,
    metrics: &NatsSinkMetrics,
    task_owner: &ConnectorTaskOwner,
    deadline: tokio::time::Instant,
    total_timeout: Duration,
) -> Result<usize, ConnectorError> {
    if pending.is_empty() {
        return Ok(0);
    }
    let set: FuturesUnordered<_> = pending.drain(..).map(IntoFuture::into_future).collect();
    let task_guard = task_owner.track().ok_or_else(|| {
        ConnectorError::Internal("NATS sink task generation is already retired".into())
    })?;
    let task_metrics = metrics.clone();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();

    // A caller deadline or cancellation must not drop unresolved PublishAckFutures: async-nats
    // otherwise hands them to an internal acker whose JoinHandle is not exposed. The owned task
    // returns the deadline result promptly, then lets every unresolved future reach the client's
    // own timeout before releasing its generation guard.
    drop(tokio::spawn(async move {
        let _task_guard = task_guard;
        run_ack_drain(set, task_metrics, deadline, total_timeout, result_tx).await;
    }));

    result_rx.await.map_err(|_| {
        ConnectorError::Internal("NATS acknowledgement drain task terminated unexpectedly".into())
    })?
}

fn retire_pending_acks(
    pending: &mut VecDeque<PublishAckFuture>,
    metrics: &NatsSinkMetrics,
    task_owner: &ConnectorTaskOwner,
) {
    if pending.is_empty() {
        return;
    }
    let count = pending.len();
    let Some(task_guard) = task_owner.track() else {
        for _ in 0..count {
            metrics.record_ack_error();
        }
        pending.clear();
        metrics.set_pending_futures(0);
        return;
    };
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        drop(task_guard);
        for _ in 0..count {
            metrics.record_ack_error();
        }
        pending.clear();
        metrics.set_pending_futures(0);
        return;
    };

    let mut set: FuturesUnordered<_> = pending.drain(..).map(IntoFuture::into_future).collect();
    let metrics = metrics.clone();
    drop(runtime.spawn(async move {
        let _task_guard = task_guard;
        while let Some(result) = set.next().await {
            match result {
                Ok(ack) if ack.duplicate => metrics.record_dedup(),
                Ok(_) => {}
                Err(_) => metrics.record_ack_error(),
            }
        }
        metrics.set_pending_futures(0);
    }));
}

async fn run_ack_drain<F>(
    mut set: FuturesUnordered<F>,
    metrics: NatsSinkMetrics,
    deadline: tokio::time::Instant,
    total_timeout: Duration,
    result_tx: tokio::sync::oneshot::Sender<Result<usize, ConnectorError>>,
) where
    F: std::future::Future<
            Output = Result<jetstream::publish::PublishAck, jetstream::context::PublishError>,
        > + Send
        + 'static,
{
    let mut aggregate = AckAggregate::default();
    loop {
        if set.is_empty() {
            metrics.set_pending_futures(0);
            let _ = result_tx.send(aggregate.into_result());
            return;
        }
        match tokio::time::timeout_at(deadline, set.next()).await {
            Ok(Some(Ok(ack))) => {
                aggregate.record_applied();
                if ack.duplicate {
                    metrics.record_dedup();
                }
            }
            Ok(Some(Err(error))) => {
                metrics.record_ack_error();
                aggregate.record_failure(classify_jetstream_ack_failure(&error));
            }
            Ok(None) => {
                metrics.set_pending_futures(0);
                let _ = result_tx.send(aggregate.into_result());
                return;
            }
            Err(_) => {
                let unresolved = set.len();
                for _ in 0..unresolved {
                    metrics.record_ack_error();
                }
                aggregate.record_unresolved_timeout(unresolved, total_timeout);
                metrics.set_pending_futures(unresolved);
                let _ = result_tx.send(aggregate.into_result());

                while let Some(result) = set.next().await {
                    if let Ok(ack) = result {
                        if ack.duplicate {
                            metrics.record_dedup();
                        }
                    }
                }
                metrics.set_pending_futures(0);
                return;
            }
        }
    }
}

fn err(msg: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(msg.to_string())
}

#[cfg(test)]
mod tests;
