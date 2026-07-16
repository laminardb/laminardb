//! NATS source: `JetStream` pull consumer with bounded asynchronous acknowledgement, or core
//! subscribe (at-most-once). Messages are acknowledged only after successful deserialization;
//! the source remains explicitly ephemeral and does not couple broker acks to checkpoints.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, consumer::pull};
use async_trait::async_trait;
use bytes::Bytes;
use crossfire::{mpsc, AsyncRx, MAsyncTx, TryRecvError};
use futures_util::StreamExt;
use tokio::sync::{mpsc as tokio_mpsc, watch, Notify};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, warn};

use super::config::{build_connect_options, AckPolicy, DeliverPolicy, Mode, NatsSourceConfig};
use super::metrics::NatsSourceMetrics;
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourcePosition, SourceStart,
    SourceTopology,
};
use crate::error::ConnectorError;
use crate::serde::{self, RecordDeserializer};

const ACK_IO_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ACK_CONCURRENCY: usize = 64;
const MAX_ACK_BACKLOG: usize = 16_384;

/// `ack` is `Some` only on the `JetStream` path.
struct Incoming {
    payload: Bytes,
    ack: Option<jetstream::Message>,
}

struct AckRuntime {
    tx: Option<tokio_mpsc::Sender<jetstream::Message>>,
    handle: JoinHandle<()>,
}

struct Running {
    deserializer: Box<dyn RecordDeserializer>,
    rx: Option<AsyncRx<mpsc::Array<Incoming>>>,
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
    ack_runtime: Option<AckRuntime>,
}

impl Drop for AckRuntime {
    fn drop(&mut self) {
        self.tx.take();
        self.handle.abort();
    }
}

impl Running {
    fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // Drop is the final backstop for a cancelled startup/close future. The watch update is
        // observable even if the reader has not entered `changed()` yet; abort guarantees that a
        // misbehaving transport future cannot become a detached loop.
        self.request_shutdown();
        self.handle.abort();
    }
}

impl AckRuntime {
    fn spawn(cfg: &NatsSourceConfig, metrics: NatsSourceMetrics) -> Self {
        let (backlog, concurrency) = ack_runtime_limits(cfg);
        let (tx, rx) = tokio_mpsc::channel(backlog);
        let handle = tokio::spawn(run_ack_worker(rx, concurrency, metrics));
        Self {
            tx: Some(tx),
            handle,
        }
    }
}

/// NATS source — core and `JetStream` modes.
pub struct NatsSource {
    schema: SchemaRef,
    config: Option<NatsSourceConfig>,
    data_ready: Arc<Notify>,
    metrics: NatsSourceMetrics,
    running: Option<Running>,
}

impl NatsSource {
    /// Metrics register on `registry` if provided.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        Self {
            schema,
            config: None,
            data_ready: Arc::new(Notify::new()),
            metrics: NatsSourceMetrics::new(registry),
            running: None,
        }
    }

    /// Available after [`SourceConnector::start`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSourceConfig> {
        self.config.as_ref()
    }

    /// Snapshot accessor for the prometheus-backed metrics struct.
    #[must_use]
    pub fn metrics_handle(&self) -> &NatsSourceMetrics {
        &self.metrics
    }

    async fn open_jetstream(
        &mut self,
        cfg: &NatsSourceConfig,
        deserializer: Box<dyn RecordDeserializer>,
    ) -> Result<(), ConnectorError> {
        let client = connect(cfg).await?;
        let js = jetstream::new(client);

        let stream_name = cfg
            .stream
            .as_deref()
            .ok_or_else(|| err("stream name missing after validation"))?;
        let consumer_name = cfg
            .consumer
            .as_deref()
            .ok_or_else(|| err("consumer name missing after validation"))?;

        let pull_cfg = build_pull_config(cfg, consumer_name)?;
        let stream = js
            .get_stream(stream_name)
            .await
            .map_err(|e| err(&format!("get_stream('{stream_name}') failed: {e}")))?;
        let consumer = stream
            .create_consumer(pull_cfg)
            .await
            .map_err(|e| classify_create_consumer_error(&e, consumer_name))?;

        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let requires_ack = cfg.ack_policy == AckPolicy::Explicit;
        let ack_runtime = requires_ack.then(|| AckRuntime::spawn(cfg, self.metrics.clone()));

        let reader = JsReader {
            consumer,
            tx,
            shutdown: shutdown_rx,
            consecutive_errors: Arc::new(AtomicU32::new(0)),
            data_ready: Arc::clone(&self.data_ready),
            metrics: self.metrics.clone(),
            batch_size: cfg.fetch_batch,
            max_wait: cfg.fetch_max_wait,
            lag_poll_interval: cfg.lag_poll_interval,
            requires_ack,
        };
        let handle = tokio::spawn(reader.run());

        self.running = Some(Running {
            deserializer,
            rx: Some(rx),
            shutdown,
            handle,
            ack_runtime,
        });
        Ok(())
    }

    async fn open_core(
        &mut self,
        cfg: &NatsSourceConfig,
        deserializer: Box<dyn RecordDeserializer>,
    ) -> Result<(), ConnectorError> {
        let client = connect(cfg).await?;
        let subject = cfg
            .subject
            .clone()
            .ok_or_else(|| err("subject missing after validation"))?;
        let subscriber = if let Some(group) = cfg.queue_group.as_deref() {
            client
                .queue_subscribe(subject, group.to_string())
                .await
                .map_err(|e| err(&format!("queue_subscribe: {e}")))?
        } else {
            client
                .subscribe(subject)
                .await
                .map_err(|e| err(&format!("subscribe: {e}")))?
        };

        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let (shutdown, shutdown_rx) = watch::channel(false);

        let reader = CoreReader {
            subscriber,
            tx,
            shutdown: shutdown_rx,
            data_ready: Arc::clone(&self.data_ready),
        };
        let handle = tokio::spawn(reader.run());

        self.running = Some(Running {
            deserializer,
            rx: Some(rx),
            shutdown,
            handle,
            ack_runtime: None,
        });
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for NatsSource {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        // Neither Core NATS nor the current JetStream implementation can
        // rewind an abandoned checkpoint attempt deterministically. Durable
        // consumers alone are insufficient for LaminarDB replay semantics.
        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::Singleton,
        ))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let SourceStart {
            config, position, ..
        } = request;
        if let SourcePosition::Resume { attempt, .. } = position {
            return Err(ConnectorError::ConfigurationError(format!(
                "NATS is an ephemeral source and cannot resume checkpoint attempt {attempt:?}"
            )));
        }
        let config = &config;

        let cfg = NatsSourceConfig::from_config(config)?;
        // SQL DDL schema overrides the registry placeholder.
        if let Some(schema) = config.arrow_schema() {
            self.schema = schema;
        }
        let deserializer = serde::create_deserializer(cfg.format)
            .map_err(|e| err(&format!("deserializer for format {:?}: {e}", cfg.format)))?;
        match cfg.mode {
            Mode::JetStream => self.open_jetstream(&cfg, deserializer).await?,
            Mode::Core => self.open_core(&cfg, deserializer).await?,
        }
        self.config = Some(cfg);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(running) = self.running.as_mut() else {
            return Ok(None);
        };

        let mut payloads: Vec<Bytes> = Vec::new();
        let mut new_acks: Vec<jetstream::Message> = Vec::new();
        let mut reader_disconnected = false;

        while payloads.len() < max_records {
            let incoming = match running
                .rx
                .as_mut()
                .expect("running NATS source owns its receiver")
                .try_recv()
            {
                Ok(m) => m,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    reader_disconnected = true;
                    break;
                }
            };
            payloads.push(incoming.payload);
            if let Some(msg) = incoming.ack {
                new_acks.push(msg);
            }
        }

        if payloads.is_empty() {
            if reader_disconnected {
                return Err(ConnectorError::ReadError(
                    "NATS reader task terminated unexpectedly".into(),
                ));
            }
            return Ok(None);
        }

        let records: Vec<&[u8]> = payloads.iter().map(Bytes::as_ref).collect();
        let bytes_total: u64 = records.iter().map(|r| r.len() as u64).sum();
        // Deserialize before scheduling acks: on failure the handles drop unacked and the broker
        // redelivers after ack_wait. Ack enqueue is non-blocking to keep this poll path bounded.
        let batch = running
            .deserializer
            .deserialize_batch(&records, &self.schema)
            .map_err(|e| err(&format!("deserialize batch: {e}")))?;

        enqueue_acks(running.ack_runtime.as_ref(), new_acks, &self.metrics);

        self.metrics
            .record_poll(batch.num_rows() as u64, bytes_total);

        Ok(Some(SourceBatch::new(batch)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        // Ephemeral sources deliberately expose no recovery cursor. JetStream acknowledgements
        // are delivery progress, not checkpoint-owned state.
        SourceCheckpoint::new()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let Some(mut running) = self.running.take() else {
            return Ok(());
        };
        let close_deadline = tokio::time::Instant::now() + CLOSE_DRAIN_TIMEOUT;
        running.request_shutdown();
        match tokio::time::timeout_at(close_deadline, &mut running.handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "NATS reader task failed while closing"),
            Err(_) => {
                warn!("NATS reader did not stop before close deadline; aborting task");
                running.handle.abort();
                let _ = (&mut running.handle).await;
            }
        }
        // Drop unread messages only after the reader has stopped. Their unacked JetStream
        // handles remain eligible for broker redelivery.
        running.rx.take();

        if let Some(mut ack_runtime) = running.ack_runtime.take() {
            ack_runtime.tx.take();
            match tokio::time::timeout_at(close_deadline, &mut ack_runtime.handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(%error, "NATS ack worker failed while closing");
                    self.metrics.record_abandoned_acks();
                }
                Err(_) => {
                    warn!("NATS ack worker did not drain before close deadline; aborting task");
                    ack_runtime.handle.abort();
                    let _ = (&mut ack_runtime.handle).await;
                    self.metrics.record_abandoned_acks();
                }
            }
        }
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }
}

// ── helpers ──

fn err(msg: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(msg.to_string())
}

fn ack_runtime_limits(cfg: &NatsSourceConfig) -> (usize, usize) {
    let broker_limit = usize::try_from(cfg.max_ack_pending)
        .ok()
        .filter(|limit| *limit > 0);
    let fallback = cfg.fetch_batch.saturating_mul(2);
    let backlog = broker_limit.unwrap_or(fallback).clamp(1, MAX_ACK_BACKLOG);
    let concurrency = cfg.fetch_batch.clamp(1, MAX_ACK_CONCURRENCY).min(backlog);
    (backlog, concurrency)
}

fn enqueue_acks(
    runtime: Option<&AckRuntime>,
    messages: Vec<jetstream::Message>,
    metrics: &NatsSourceMetrics,
) {
    if messages.is_empty() {
        return;
    }
    let Some(runtime) = runtime else {
        metrics.record_ack_enqueue_errors(messages.len());
        warn!(
            rejected = messages.len(),
            "JetStream ack worker is unavailable; broker will redeliver"
        );
        return;
    };

    let mut rejected = 0usize;
    for message in messages {
        // Increment before publication: a fast worker may complete immediately after try_send.
        metrics.record_ack_enqueued();
        if runtime
            .tx
            .as_ref()
            .is_none_or(|tx| tx.try_send(message).is_err())
        {
            metrics.record_ack_error();
            rejected += 1;
        }
    }
    if rejected > 0 {
        warn!(
            rejected,
            "JetStream ack backlog is full or closed; broker will redeliver"
        );
    }
}

async fn run_ack_worker(
    mut rx: tokio_mpsc::Receiver<jetstream::Message>,
    concurrency: usize,
    metrics: NatsSourceMetrics,
) {
    let mut in_flight = JoinSet::new();
    loop {
        while in_flight.len() >= concurrency {
            if let Some(result) = in_flight.join_next().await {
                observe_ack_task(result, &metrics);
            }
        }

        tokio::select! {
            biased;
            result = in_flight.join_next(), if !in_flight.is_empty() => {
                if let Some(result) = result {
                    observe_ack_task(result, &metrics);
                }
            }
            message = rx.recv() => {
                let Some(message) = message else {
                    break;
                };
                let task_metrics = metrics.clone();
                in_flight.spawn(async move {
                    acknowledge_message(message, &task_metrics).await;
                });
            }
        }
    }

    while let Some(result) = in_flight.join_next().await {
        observe_ack_task(result, &metrics);
    }
}

fn observe_ack_task(result: Result<(), tokio::task::JoinError>, metrics: &NatsSourceMetrics) {
    if let Err(error) = result {
        metrics.record_ack_error();
        warn!(%error, "JetStream ack task failed; broker will redeliver");
    }
}

async fn acknowledge_message(message: jetstream::Message, metrics: &NatsSourceMetrics) {
    match tokio::time::timeout(ACK_IO_TIMEOUT, message.ack()).await {
        Ok(Ok(())) => metrics.record_ack(),
        Ok(Err(error)) => {
            metrics.record_ack_error();
            warn!(%error, "JetStream ack failed; broker will redeliver");
        }
        Err(_) => {
            metrics.record_ack_error();
            warn!(
                timeout_ms = ACK_IO_TIMEOUT.as_millis(),
                "JetStream ack timed out; broker will redeliver"
            );
        }
    }
}

async fn connect(cfg: &NatsSourceConfig) -> Result<async_nats::Client, ConnectorError> {
    build_connect_options(&cfg.auth, &cfg.tls)?
        .connect(&cfg.servers)
        .await
        .map_err(|e| err(&format!("nats connect({:?}): {e}", cfg.servers)))
}

fn build_pull_config(
    cfg: &NatsSourceConfig,
    consumer_name: &str,
) -> Result<pull::Config, ConnectorError> {
    let filter_subjects = if cfg.subject_filters.is_empty() {
        cfg.subject.iter().cloned().collect()
    } else {
        cfg.subject_filters.clone()
    };

    Ok(pull::Config {
        durable_name: Some(consumer_name.to_string()),
        filter_subjects,
        deliver_policy: map_deliver_policy(cfg)?,
        ack_policy: map_ack_policy(cfg.ack_policy),
        ack_wait: cfg.ack_wait,
        max_deliver: cfg.max_deliver,
        max_ack_pending: cfg.max_ack_pending,
        ..Default::default()
    })
}

fn map_deliver_policy(
    cfg: &NatsSourceConfig,
) -> Result<async_nats::jetstream::consumer::DeliverPolicy, ConnectorError> {
    use async_nats::jetstream::consumer::DeliverPolicy as Nats;
    Ok(match cfg.deliver_policy {
        DeliverPolicy::All => Nats::All,
        DeliverPolicy::New => Nats::New,
        DeliverPolicy::ByStartSequence => Nats::ByStartSequence {
            start_sequence: cfg.start_sequence.unwrap_or(1),
        },
        DeliverPolicy::ByStartTime => {
            let raw = cfg
                .start_time
                .as_deref()
                .ok_or_else(|| err("deliver.policy=by_start_time requires 'start.time'"))?;
            let start_time =
                time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| err(&format!("start.time '{raw}' is not valid RFC3339: {e}")))?;
            Nats::ByStartTime { start_time }
        }
    })
}

fn map_ack_policy(p: AckPolicy) -> async_nats::jetstream::consumer::AckPolicy {
    use async_nats::jetstream::consumer::AckPolicy as Nats;
    match p {
        AckPolicy::Explicit => Nats::Explicit,
        AckPolicy::None => Nats::None,
    }
}

/// 500ms, 1s, 2s, 4s, cap 5s.
fn fetch_backoff_base(consecutive_errors: u32) -> Duration {
    let exp = consecutive_errors.saturating_sub(1).min(4);
    let ms = 500u64.saturating_mul(1u64 << exp);
    Duration::from_millis(ms.min(5000))
}

/// `base ± 20%`. Tests pass a fixed `entropy` seed.
fn with_jitter(base: Duration, entropy: u64) -> Duration {
    let base_ms = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let range = (base_ms / 5).max(1); // 20%
    let window = range * 2 + 1;
    let offset = entropy % window;
    let jittered = base_ms.saturating_add(offset).saturating_sub(range);
    Duration::from_millis(jittered)
}

fn fetch_backoff(consecutive_errors: u32, entropy: u64) -> Duration {
    with_jitter(fetch_backoff_base(consecutive_errors), entropy)
}

/// Server 10148 / 10013 → consumer exists with a conflicting config;
/// raise LDB-5070 with an operator fix-up.
fn classify_create_consumer_error(
    e: &async_nats::jetstream::stream::ConsumerError,
    consumer_name: &str,
) -> ConnectorError {
    use async_nats::jetstream::stream::ConsumerErrorKind;
    use async_nats::jetstream::ErrorCode;

    let drift_code = match e.kind() {
        ConsumerErrorKind::JetStream(server_err) => matches!(
            server_err.error_code(),
            ErrorCode::CONSUMER_ALREADY_EXISTS | ErrorCode::CONSUMER_NAME_EXIST
        ),
        _ => false,
    };
    if drift_code {
        err(&format!(
            "[LDB-5070] consumer '{consumer_name}' exists with incompatible config; \
             rotate the durable name or delete the consumer out-of-band. \
             Server said: {e}"
        ))
    } else {
        err(&format!("create_consumer('{consumer_name}') failed: {e}"))
    }
}

/// Wall-clock nanos for `with_jitter`. `Instant::now().elapsed()` is ~0
/// and produces correlated jitter across tasks.
#[allow(clippy::cast_possible_truncation)]
fn entropy_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

struct JsReader {
    consumer: jetstream::consumer::Consumer<pull::Config>,
    tx: MAsyncTx<mpsc::Array<Incoming>>,
    shutdown: watch::Receiver<bool>,
    consecutive_errors: Arc<AtomicU32>,
    data_ready: Arc<Notify>,
    metrics: NatsSourceMetrics,
    batch_size: usize,
    max_wait: Duration,
    /// `Duration::ZERO` disables the poll.
    lag_poll_interval: Duration,
    requires_ack: bool,
}

impl JsReader {
    async fn run(self) {
        let Self {
            mut consumer,
            tx,
            mut shutdown,
            consecutive_errors,
            data_ready,
            metrics,
            batch_size,
            max_wait,
            lag_poll_interval,
            requires_ack,
        } = self;

        let mut last_lag_poll = Instant::now();
        let lag_poll_enabled = !lag_poll_interval.is_zero();

        loop {
            if shutdown_requested(&shutdown) {
                break;
            }
            let fetch_result = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                r = consumer.fetch().max_messages(batch_size).expires(max_wait).messages() => r,
            };

            let mut stream = match fetch_result {
                Ok(s) => s,
                Err(e) => {
                    let errs = consecutive_errors.fetch_add(1, Ordering::AcqRel) + 1;
                    metrics.record_fetch_error();
                    warn!(
                        error = %e,
                        consecutive_errors = errs,
                        "nats fetch() errored; backing off",
                    );
                    let backoff = fetch_backoff(errs, entropy_now());
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => break,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    continue;
                }
            };

            let mut forwarded = 0usize;
            let mut stream_errors = 0usize;
            loop {
                let msg_result = tokio::select! {
                    biased;
                    _ = shutdown.changed() => return,
                    r = stream.next() => match r {
                        Some(r) => r,
                        None => break,
                    },
                };
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        metrics.record_fetch_error();
                        stream_errors += 1;
                        warn!(error = %e, "nats message error");
                        continue;
                    }
                };
                let payload = msg.payload.clone();
                let incoming = Incoming {
                    payload,
                    ack: requires_ack.then_some(msg),
                };
                let send_result = tokio::select! {
                    biased;
                    _ = shutdown.changed() => return,
                    result = tx.send(incoming) => result,
                };
                if send_result.is_err() {
                    debug!("nats reader: downstream channel closed");
                    return;
                }
                forwarded += 1;
            }

            // Reset on progress; an iteration with only errors counts
            // as one failure; idle iterations don't bump.
            if forwarded > 0 {
                consecutive_errors.store(0, Ordering::Release);
                data_ready.notify_one();
            } else if stream_errors > 0 {
                let errs = consecutive_errors.fetch_add(1, Ordering::AcqRel) + 1;
                let backoff = fetch_backoff(errs, entropy_now());
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    () = tokio::time::sleep(backoff) => {}
                }
            }

            if lag_poll_enabled && last_lag_poll.elapsed() >= lag_poll_interval {
                last_lag_poll = Instant::now();
                match consumer.info().await {
                    Ok(info) => metrics.set_consumer_lag(info.num_pending),
                    Err(e) => warn!(error = %e, "consumer.info() failed; skipping lag update"),
                }
            }
        }
    }
}

struct CoreReader {
    subscriber: async_nats::Subscriber,
    tx: MAsyncTx<mpsc::Array<Incoming>>,
    shutdown: watch::Receiver<bool>,
    data_ready: Arc<Notify>,
}

impl CoreReader {
    async fn run(self) {
        let Self {
            mut subscriber,
            tx,
            mut shutdown,
            data_ready,
        } = self;

        loop {
            if shutdown_requested(&shutdown) {
                break;
            }
            let msg = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                m = subscriber.next() => match m {
                    Some(m) => m,
                    None => break,
                },
            };
            let incoming = Incoming {
                payload: msg.payload,
                ack: None,
            };
            let send_result = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                result = tx.send(incoming) => result,
            };
            if send_result.is_err() {
                break;
            }
            data_ready.notify_one();
        }
        // Wake the coordinator so it observes the now-disconnected channel immediately instead
        // of treating a terminated Core subscription as an indefinitely idle source.
        data_ready.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;

    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    fn pending_task(
        started: tokio::sync::oneshot::Sender<()>,
        dropped: tokio::sync::oneshot::Sender<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(dropped));
            let _ = started.send(());
            std::future::pending::<()>().await;
        })
    }

    #[test]
    fn source_contract_is_ephemeral_even_for_jetstream_config() {
        let source = NatsSource::new(Arc::new(Schema::empty()), None);
        let mut config = ConnectorConfig::new("nats");
        config.set("mode", "jetstream");
        let contract = source.contract(&config).expect("static NATS contract");
        assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
        assert_eq!(contract.topology, SourceTopology::Singleton);
    }

    #[test]
    fn ephemeral_source_checkpoint_has_no_protocol_state() {
        let src = NatsSource::new(Arc::new(Schema::empty()), None);
        assert!(src.checkpoint().is_empty());
    }

    #[tokio::test]
    async fn disconnected_reader_drains_queued_payload_before_terminal_error() {
        let mut src = NatsSource::new(Arc::new(Schema::empty()), None);
        let (tx, rx) = mpsc::bounded_async::<Incoming>(2);
        assert!(tx
            .try_send(Incoming {
                payload: Bytes::from_static(b"one"),
                ack: None,
            })
            .is_ok());
        drop(tx);
        let (shutdown, _) = watch::channel(false);
        src.running = Some(Running {
            deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
            rx: Some(rx),
            shutdown,
            handle: tokio::spawn(async {}),
            ack_runtime: None,
        });

        let final_batch = src.poll_batch(10).await.unwrap().unwrap();
        assert_eq!(final_batch.records.num_rows(), 1);

        let error = src.poll_batch(10).await.unwrap_err();
        assert!(matches!(error, ConnectorError::ReadError(_)));
        assert!(error.to_string().contains("reader task terminated"));
    }

    #[tokio::test]
    async fn dropping_source_signals_and_aborts_the_owned_reader() {
        let mut source = NatsSource::new(Arc::new(Schema::empty()), None);
        let (_, rx) = mpsc::bounded_async::<Incoming>(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        source.running = Some(Running {
            deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
            rx: Some(rx),
            shutdown,
            handle: pending_task(started_tx, dropped_tx),
            ack_runtime: None,
        });
        started_rx.await.expect("reader task started");

        drop(source);

        assert!(*shutdown_rx.borrow(), "drop must publish shutdown");
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("reader must be aborted on drop")
            .expect("reader drop signal");
    }

    #[tokio::test]
    async fn cancelling_close_does_not_detach_reader_or_ack_tasks() {
        let mut source = NatsSource::new(Arc::new(Schema::empty()), None);
        let (_, rx) = mpsc::bounded_async::<Incoming>(1);
        let (shutdown, _) = watch::channel(false);
        let (reader_started_tx, reader_started_rx) = tokio::sync::oneshot::channel();
        let (reader_dropped_tx, reader_dropped_rx) = tokio::sync::oneshot::channel();
        let reader_handle = pending_task(reader_started_tx, reader_dropped_tx);

        let (ack_tx, ack_rx) = tokio_mpsc::channel::<jetstream::Message>(1);
        let (ack_started_tx, ack_started_rx) = tokio::sync::oneshot::channel();
        let (ack_dropped_tx, ack_dropped_rx) = tokio::sync::oneshot::channel();
        let ack_handle = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(ack_dropped_tx));
            let _ack_rx = ack_rx;
            let _ = ack_started_tx.send(());
            std::future::pending::<()>().await;
        });
        source.running = Some(Running {
            deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
            rx: Some(rx),
            shutdown,
            handle: reader_handle,
            ack_runtime: Some(AckRuntime {
                tx: Some(ack_tx),
                handle: ack_handle,
            }),
        });
        reader_started_rx.await.expect("reader task started");
        ack_started_rx.await.expect("ack task started");

        let close = tokio::spawn(async move { source.close().await });
        tokio::task::yield_now().await;
        assert!(!close.is_finished(), "close must be waiting for the reader");
        close.abort();
        assert!(close
            .await
            .expect_err("close waiter cancelled")
            .is_cancelled());

        for (name, dropped) in [("reader", reader_dropped_rx), ("ack", ack_dropped_rx)] {
            tokio::time::timeout(Duration::from_secs(1), dropped)
                .await
                .unwrap_or_else(|_| panic!("{name} task remained detached"))
                .unwrap_or_else(|_| panic!("{name} drop signal closed"));
        }
    }

    #[test]
    fn backoff_base_grows_then_caps_at_5s() {
        assert_eq!(fetch_backoff_base(1), Duration::from_millis(500));
        assert_eq!(fetch_backoff_base(2), Duration::from_millis(1000));
        assert_eq!(fetch_backoff_base(3), Duration::from_millis(2000));
        assert_eq!(fetch_backoff_base(4), Duration::from_millis(4000));
        assert_eq!(fetch_backoff_base(5), Duration::from_millis(5000));
        assert_eq!(fetch_backoff_base(100), Duration::from_millis(5000));
    }

    #[test]
    fn jitter_stays_within_plus_minus_20_percent() {
        let base = Duration::from_millis(1000);
        for entropy in [0u64, 1, 99, 12345, u64::MAX] {
            let j = with_jitter(base, entropy);
            assert!(
                j >= Duration::from_millis(800) && j <= Duration::from_millis(1200),
                "entropy {entropy}: jittered = {j:?} outside ±20% of {base:?}"
            );
        }
    }
}
