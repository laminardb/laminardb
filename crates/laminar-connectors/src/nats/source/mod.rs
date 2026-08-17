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
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use tokio::sync::{mpsc as tokio_mpsc, watch, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::config::{build_connect_options, AckPolicy, DeliverPolicy, Mode, NatsSourceConfig};
use super::metrics::NatsSourceMetrics;
use super::setup::{
    classify_connect_error, classify_create_consumer_error, classify_get_stream_error,
    classify_subscribe_error, track_connection_tasks,
};
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorTaskGuard, ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector,
    SourceConsistency, SourceContract, SourceInputMode, SourcePosition, SourceStart,
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
    shutdown: watch::Sender<bool>,
    task: TrackedTask,
}

struct Running {
    deserializer: Box<dyn RecordDeserializer>,
    rx: Option<AsyncRx<mpsc::Array<Incoming>>>,
    shutdown: watch::Sender<bool>,
    reader: TrackedTask,
    ack_runtime: Option<AckRuntime>,
}

struct TrackedTask {
    handle: Option<JoinHandle<()>>,
    reaper_guard: Option<ConnectorTaskGuard>,
    name: &'static str,
}

enum TaskWait {
    Completed(Result<(), tokio::task::JoinError>),
    TimedOut,
}

impl TrackedTask {
    fn spawn(
        owner: &ConnectorTaskOwner,
        name: &'static str,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<Self, ConnectorError> {
        let task_guard = owner.track().ok_or_else(|| {
            ConnectorError::Internal("NATS source task generation is already retired".into())
        })?;
        let reaper_guard = owner.track().ok_or_else(|| {
            ConnectorError::Internal("NATS source task generation is already retired".into())
        })?;
        let handle = tokio::spawn(async move {
            let _task_guard = task_guard;
            future.await;
        });
        Ok(Self {
            handle: Some(handle),
            reaper_guard: Some(reaper_guard),
            name,
        })
    }

    async fn wait_until(&mut self, deadline: tokio::time::Instant) -> TaskWait {
        let Some(handle) = self.handle.as_mut() else {
            return TaskWait::Completed(Ok(()));
        };
        match tokio::time::timeout_at(deadline, handle).await {
            Ok(result) => {
                self.handle.take();
                self.reaper_guard.take();
                TaskWait::Completed(result)
            }
            Err(_) => TaskWait::TimedOut,
        }
    }

    fn retire(&mut self) {
        let Some(handle) = self.handle.take() else {
            self.reaper_guard.take();
            return;
        };
        let reaper_guard = self.reaper_guard.take();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            // Runtime destruction drops the task future and its task guard. That guard is the
            // completion proof; a join task is useful for cleanup but is not itself the proof.
            drop(handle);
            drop(reaper_guard);
            return;
        };
        let name = self.name;
        drop(runtime.spawn(async move {
            let _reaper_guard = reaper_guard;
            if let Err(error) = handle.await {
                debug!(task = name, %error, "retired NATS source task reaped");
            }
        }));
    }
}

impl Drop for TrackedTask {
    fn drop(&mut self) {
        self.retire();
    }
}

impl Running {
    fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // Drop is the final backstop for a cancelled startup/close future. Tasks retain their
        // generation guards until they actually exit; the reapers retain the join handles.
        self.request_shutdown();
        self.rx.take();
        if let Some(ack_runtime) = self.ack_runtime.as_mut() {
            ack_runtime.request_shutdown();
            ack_runtime.task.retire();
        }
        self.reader.retire();
    }
}

impl AckRuntime {
    fn spawn(
        cfg: &NatsSourceConfig,
        metrics: NatsSourceMetrics,
        owner: &ConnectorTaskOwner,
    ) -> Result<Self, ConnectorError> {
        let (backlog, concurrency) = ack_runtime_limits(cfg);
        let (tx, rx) = tokio_mpsc::channel(backlog);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = TrackedTask::spawn(
            owner,
            "ack-worker",
            run_ack_worker(rx, shutdown_rx, concurrency, metrics),
        )?;
        Ok(Self {
            tx: Some(tx),
            shutdown,
            task,
        })
    }

    fn request_shutdown(&mut self) {
        self.shutdown.send_replace(true);
        self.tx.take();
    }
}

impl Drop for AckRuntime {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

/// NATS source — core and `JetStream` modes.
pub struct NatsSource {
    schema: SchemaRef,
    config: Option<NatsSourceConfig>,
    data_ready: Arc<Notify>,
    metrics: NatsSourceMetrics,
    running: Option<Running>,
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

impl NatsSource {
    /// Metrics register on `registry` if provided.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            schema,
            config: None,
            data_ready: Arc::new(Notify::new()),
            metrics: NatsSourceMetrics::new(registry),
            running: None,
            task_owner,
            task_tracker,
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
        let client = connect(cfg, &self.task_owner).await?;
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
            .map_err(|error| classify_get_stream_error(&error, stream_name))?;
        let consumer = stream
            .create_consumer(pull_cfg)
            .await
            .map_err(|error| classify_create_consumer_error(&error, consumer_name))?;

        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let requires_ack = cfg.ack_policy == AckPolicy::Explicit;
        let ack_runtime = if requires_ack {
            Some(AckRuntime::spawn(
                cfg,
                self.metrics.clone(),
                &self.task_owner,
            )?)
        } else {
            None
        };

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
        let reader = TrackedTask::spawn(&self.task_owner, "jetstream-reader", reader.run())?;

        self.running = Some(Running {
            deserializer,
            rx: Some(rx),
            shutdown,
            reader,
            ack_runtime,
        });
        Ok(())
    }

    async fn open_core(
        &mut self,
        cfg: &NatsSourceConfig,
        deserializer: Box<dyn RecordDeserializer>,
    ) -> Result<(), ConnectorError> {
        let client = connect(cfg, &self.task_owner).await?;
        let subject = cfg
            .subject
            .clone()
            .ok_or_else(|| err("subject missing after validation"))?;
        let subscriber = if let Some(group) = cfg.queue_group.as_deref() {
            client
                .queue_subscribe(subject, group.to_string())
                .await
                .map_err(|error| classify_subscribe_error(&error, "NATS queue subscribe"))?
        } else {
            client
                .subscribe(subject)
                .await
                .map_err(|error| classify_subscribe_error(&error, "NATS subscribe"))?
        };

        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let (shutdown, shutdown_rx) = watch::channel(false);

        let reader = CoreReader {
            subscriber,
            tx,
            shutdown: shutdown_rx,
            data_ready: Arc::clone(&self.data_ready),
        };
        let reader = TrackedTask::spawn(&self.task_owner, "core-reader", reader.run())?;

        self.running = Some(Running {
            deserializer,
            rx: Some(rx),
            shutdown,
            reader,
            ack_runtime: None,
        });
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for NatsSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let format = match config.get("format") {
            Some(value) => serde::Format::parse(value)
                .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?,
            None => self
                .config
                .as_ref()
                .map_or(serde::Format::Json, |config| config.format),
        };
        let input_mode = if format == serde::Format::Debezium {
            SourceInputMode::KeyedUpsert
        } else {
            SourceInputMode::AppendOnly
        };

        // Neither Core NATS nor the current JetStream implementation can
        // rewind an abandoned checkpoint attempt deterministically. Durable
        // consumers alone are insufficient for LaminarDB replay semantics.
        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::Singleton,
            input_mode,
        ))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        if let SourcePosition::Resume { attempt, .. } = position {
            return Err(ConnectorError::ConfigurationError(format!(
                "NATS is an ephemeral source and cannot resume checkpoint attempt {attempt:?}"
            )));
        }
        let config = &config;

        let cfg = NatsSourceConfig::from_config(config)?;
        // Keep the candidate schema local until network admission succeeds so
        // cancelling start leaves the existing instance unchanged.
        let candidate_schema = config.arrow_schema();
        let deserializer = serde::create_deserializer(cfg.format)
            .map_err(|e| err(&format!("deserializer for format {:?}: {e}", cfg.format)))?;
        match cfg.mode {
            Mode::JetStream => self.open_jetstream(&cfg, deserializer).await?,
            Mode::Core => self.open_core(&cfg, deserializer).await?,
        }
        if let Some(schema) = candidate_schema {
            self.schema = schema;
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
        match running.reader.wait_until(close_deadline).await {
            TaskWait::Completed(Ok(())) => {}
            TaskWait::Completed(Err(error)) => {
                warn!(%error, "NATS reader task failed while closing");
            }
            TaskWait::TimedOut => warn!(
                "NATS reader exceeded its close deadline; its tracked reaper retains shutdown ownership"
            ),
        }
        // Drop unread messages only after the reader has stopped. Their unacked JetStream
        // handles remain eligible for broker redelivery.
        running.rx.take();

        if let Some(mut ack_runtime) = running.ack_runtime.take() {
            ack_runtime.request_shutdown();
            match ack_runtime.task.wait_until(close_deadline).await {
                TaskWait::Completed(Ok(())) => {}
                TaskWait::Completed(Err(error)) => {
                    warn!(%error, "NATS ack worker failed while closing");
                    self.metrics.record_abandoned_acks();
                }
                TaskWait::TimedOut => {
                    warn!(
                        "NATS ack worker exceeded its close deadline; its tracked reaper retains shutdown ownership"
                    );
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
    rx: tokio_mpsc::Receiver<jetstream::Message>,
    shutdown: watch::Receiver<bool>,
    concurrency: usize,
    metrics: NatsSourceMetrics,
) {
    // Ack calls stay scoped under the worker. Its single generation guard therefore proves that
    // the receiver and every in-flight acknowledgement have all been dropped or completed.
    let task_metrics = metrics.clone();
    let abandoned = run_bounded_queue(rx, shutdown, concurrency, move |message| {
        let metrics = task_metrics.clone();
        async move {
            acknowledge_message(message, &metrics).await;
        }
    })
    .await;
    if abandoned > 0 {
        metrics.record_ack_abandoned(abandoned);
        warn!(
            abandoned,
            "discarded queued JetStream acknowledgements during shutdown; broker will redeliver"
        );
    }
}

async fn run_bounded_queue<T, F, Fut>(
    mut rx: tokio_mpsc::Receiver<T>,
    mut shutdown: watch::Receiver<bool>,
    concurrency: usize,
    process: F,
) -> usize
where
    T: Send + 'static,
    F: Fn(T) -> Fut,
    Fut: std::future::Future<Output = ()> + Send,
{
    debug_assert!(concurrency > 0);
    let mut in_flight = FuturesUnordered::new();
    'input: loop {
        if shutdown_requested(&shutdown) || rx.is_closed() {
            break;
        }
        while in_flight.len() >= concurrency {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break 'input,
                _ = in_flight.next() => {}
            }
            if rx.is_closed() {
                break 'input;
            }
        }

        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = in_flight.next(), if !in_flight.is_empty() => {}
            message = rx.recv() => {
                let Some(message) = message else {
                    break;
                };
                in_flight.push(process(message));
            }
        }
    }

    // Closing the receiver makes queued-but-unstarted messages immediately eligible for broker
    // redelivery. Only work already admitted to `in_flight` is allowed to consume the close budget.
    rx.close();
    let mut abandoned = 0usize;
    while rx.try_recv().is_ok() {
        abandoned = abandoned.saturating_add(1);
    }
    while in_flight.next().await.is_some() {}
    abandoned
}

async fn acknowledge_message(message: jetstream::Message, metrics: &NatsSourceMetrics) {
    match tokio::time::timeout(ACK_IO_TIMEOUT, message.double_ack()).await {
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

async fn connect(
    cfg: &NatsSourceConfig,
    owner: &ConnectorTaskOwner,
) -> Result<async_nats::Client, ConnectorError> {
    track_connection_tasks(build_connect_options(&cfg.auth, &cfg.tls)?, owner, "source")?
        .connect(&cfg.servers)
        .await
        .map_err(|error| classify_connect_error(&error))
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
mod tests;
