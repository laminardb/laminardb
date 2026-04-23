//! NATS source: `JetStream` pull consumer with ack-on-commit, or core
//! subscribe (at-most-once). A background task forwards messages through
//! an `mpsc` channel; JS message handles are retained until
//! `notify_epoch_committed` fires, then acked in bulk.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, consumer::pull};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use rustc_hash::FxHashMap;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::config::{build_connect_options, AckPolicy, DeliverPolicy, Mode, NatsSourceConfig};
use super::metrics::NatsSourceMetrics;
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{PartitionInfo, SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;
use crate::serde::{self, RecordDeserializer};

/// `ack` is `Some` only on the `JetStream` path.
struct Incoming {
    subject: String,
    payload: Bytes,
    stream_seq: Option<u64>,
    ack: Option<jetstream::Message>,
}

struct Running {
    deserializer: Box<dyn RecordDeserializer>,
    rx: mpsc::Receiver<Incoming>,
    shutdown: Arc<Notify>,
    /// `Some` on `JetStream`; `None` on core.
    consecutive_errors: Option<Arc<AtomicU32>>,
    handle: JoinHandle<()>,
}

/// NATS source — core and `JetStream` modes.
pub struct NatsSource {
    schema: SchemaRef,
    config: Option<NatsSourceConfig>,
    data_ready: Arc<Notify>,
    metrics: NatsSourceMetrics,
    running: Option<Running>,

    // Interior mutability so `checkpoint()` (takes `&self`) can seal
    // `pending` into `sealed` atomically with offset capture.
    pending: parking_lot::Mutex<Vec<jetstream::Message>>,
    sealed: parking_lot::Mutex<VecDeque<Vec<jetstream::Message>>>,
    offsets: parking_lot::Mutex<FxHashMap<String, u64>>,
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
            pending: parking_lot::Mutex::new(Vec::new()),
            sealed: parking_lot::Mutex::new(VecDeque::new()),
            offsets: parking_lot::Mutex::new(FxHashMap::default()),
        }
    }

    fn update_pending_gauge(&self) {
        let pending_len = self.pending.lock().len();
        let sealed_total: usize = self.sealed.lock().iter().map(Vec::len).sum();
        self.metrics.set_pending_acks(pending_len + sealed_total);
    }

    /// Available after [`SourceConnector::open`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSourceConfig> {
        self.config.as_ref()
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

        let (tx, rx) = mpsc::channel::<Incoming>(cfg.fetch_batch * 2);
        let shutdown = Arc::new(Notify::new());
        let consecutive_errors = Arc::new(AtomicU32::new(0));

        let reader = JsReader {
            consumer,
            tx,
            shutdown: Arc::clone(&shutdown),
            consecutive_errors: Arc::clone(&consecutive_errors),
            data_ready: Arc::clone(&self.data_ready),
            metrics: self.metrics.clone(),
            batch_size: cfg.fetch_batch,
            max_wait: cfg.fetch_max_wait,
            lag_poll_interval: cfg.lag_poll_interval,
        };
        let handle = tokio::spawn(reader.run());

        self.running = Some(Running {
            deserializer,
            rx,
            shutdown,
            consecutive_errors: Some(consecutive_errors),
            handle,
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

        let (tx, rx) = mpsc::channel::<Incoming>(cfg.fetch_batch * 2);
        let shutdown = Arc::new(Notify::new());

        let reader = CoreReader {
            subscriber,
            tx,
            shutdown: Arc::clone(&shutdown),
            data_ready: Arc::clone(&self.data_ready),
        };
        let handle = tokio::spawn(reader.run());

        self.running = Some(Running {
            deserializer,
            rx,
            shutdown,
            consecutive_errors: None,
            handle,
        });
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for NatsSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let cfg = NatsSourceConfig::from_config(config)?;
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
        let mut partition: Option<PartitionInfo> = None;
        let mut new_acks: Vec<jetstream::Message> = Vec::new();
        let mut offset_updates: Vec<(String, u64)> = Vec::new();

        while payloads.len() < max_records {
            let Ok(incoming) = running.rx.try_recv() else {
                break;
            };
            if let Some(seq) = incoming.stream_seq {
                offset_updates.push((incoming.subject.clone(), seq));
                if partition.is_none() {
                    partition = Some(PartitionInfo::new(&incoming.subject, seq.to_string()));
                }
            }
            payloads.push(incoming.payload);
            if let Some(msg) = incoming.ack {
                new_acks.push(msg);
            }
        }

        if payloads.is_empty() {
            return Ok(None);
        }

        let records: Vec<&[u8]> = payloads.iter().map(Bytes::as_ref).collect();
        let bytes_total: u64 = records.iter().map(|r| r.len() as u64).sum();
        // Deserialize before parking acks: on failure the handles drop
        // un-acked and the broker redelivers after ack_wait.
        let batch = running
            .deserializer
            .deserialize_batch(&records, &self.schema)
            .map_err(|e| err(&format!("deserialize batch: {e}")))?;

        if !offset_updates.is_empty() {
            let mut offsets = self.offsets.lock();
            for (subject, seq) in offset_updates {
                offsets
                    .entry(subject)
                    .and_modify(|s| *s = (*s).max(seq))
                    .or_insert(seq);
            }
        }
        if !new_acks.is_empty() {
            self.pending.lock().extend(new_acks);
        }

        self.metrics
            .record_poll(batch.num_rows() as u64, bytes_total);
        self.update_pending_gauge();

        Ok(Some(SourceBatch {
            records: batch,
            partition,
        }))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        // Seal now-pending acks into this epoch; anything arriving after
        // goes into a fresh batch committed with a later epoch. Without
        // this split, an ack could fire before its manifest record lands.
        {
            let mut pending = self.pending.lock();
            if !pending.is_empty() {
                let batch = std::mem::take(&mut *pending);
                self.sealed.lock().push_back(batch);
            }
        }
        let mut cp = SourceCheckpoint::default();
        for (subject, seq) in &*self.offsets.lock() {
            cp.set_offset(subject.as_str(), seq.to_string());
        }
        cp
    }

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        // Durable consumer ack floor on the server is authoritative.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let Some(running) = self.running.take() else {
            return Ok(());
        };
        running.shutdown.notify_one();
        let _ = tokio::time::timeout(Duration::from_secs(5), running.handle).await;
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn supports_replay(&self) -> bool {
        // JetStream is replayable (durable consumer); core NATS is not.
        !matches!(self.config.as_ref().map(|c| c.mode), Some(Mode::Core))
    }

    async fn notify_epoch_committed(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        // Per-msg ack errors don't roll the epoch back; the broker
        // redelivers on ack_wait.
        loop {
            let Some(batch) = self.sealed.lock().pop_front() else {
                break;
            };
            for msg in batch {
                match msg.ack().await {
                    Ok(()) => self.metrics.record_ack(),
                    Err(e) => {
                        self.metrics.record_ack_error();
                        warn!(error = %e, "JetStream ack failed; broker will redeliver");
                    }
                }
            }
        }
        self.update_pending_gauge();
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        let Some(cfg) = self.config.as_ref() else {
            return HealthStatus::Unknown;
        };

        // Threshold of zero disables the flip.
        if cfg.fetch_error_threshold > 0 {
            if let Some(errors) = self
                .running
                .as_ref()
                .and_then(|r| r.consecutive_errors.as_ref())
            {
                let errs = errors.load(Ordering::Acquire);
                if errs >= cfg.fetch_error_threshold {
                    return HealthStatus::Unhealthy(format!(
                        "{errs} consecutive fetch errors (threshold {})",
                        cfg.fetch_error_threshold
                    ));
                }
            }
        }

        // Flag at 50% of `max_ack_pending`; -1 means unlimited.
        if cfg.max_ack_pending > 0 {
            #[allow(clippy::cast_sign_loss)]
            let cap = cfg.max_ack_pending as u64;
            #[allow(clippy::cast_sign_loss)]
            let pending = self.metrics.pending_acks.get().max(0) as u64;
            if pending * 2 >= cap {
                return HealthStatus::Degraded(format!(
                    "pending acks {pending}/{cap} — broker may throttle delivery"
                ));
            }
        }
        HealthStatus::Healthy
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }
}

// ── helpers ──

fn err(msg: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(msg.to_string())
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

struct JsReader {
    consumer: jetstream::consumer::Consumer<pull::Config>,
    tx: mpsc::Sender<Incoming>,
    shutdown: Arc<Notify>,
    consecutive_errors: Arc<AtomicU32>,
    data_ready: Arc<Notify>,
    metrics: NatsSourceMetrics,
    batch_size: usize,
    max_wait: Duration,
    /// `Duration::ZERO` disables the poll.
    lag_poll_interval: Duration,
}

impl JsReader {
    async fn run(self) {
        let Self {
            mut consumer,
            tx,
            shutdown,
            consecutive_errors,
            data_ready,
            metrics,
            batch_size,
            max_wait,
            lag_poll_interval,
        } = self;

        let mut last_lag_poll = Instant::now();
        let lag_poll_enabled = !lag_poll_interval.is_zero();

        loop {
            let fetch_result = tokio::select! {
                biased;
                () = shutdown.notified() => break,
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
                        () = shutdown.notified() => break,
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
                    () = shutdown.notified() => return,
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
                let incoming = Incoming {
                    subject: msg.subject.to_string(),
                    payload: msg.payload.clone(),
                    stream_seq: msg.info().ok().map(|i| i.stream_sequence),
                    ack: Some(msg),
                };
                if tx.send(incoming).await.is_err() {
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
                    () = shutdown.notified() => break,
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
    tx: mpsc::Sender<Incoming>,
    shutdown: Arc<Notify>,
    data_ready: Arc<Notify>,
}

impl CoreReader {
    async fn run(self) {
        let Self {
            mut subscriber,
            tx,
            shutdown,
            data_ready,
        } = self;

        loop {
            let msg = tokio::select! {
                biased;
                () = shutdown.notified() => break,
                m = subscriber.next() => match m {
                    Some(m) => m,
                    None => break,
                },
            };
            let incoming = Incoming {
                subject: msg.subject.to_string(),
                payload: msg.payload,
                stream_seq: None,
                ack: None,
            };
            if tx.send(incoming).await.is_err() {
                return;
            }
            data_ready.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;

    #[test]
    fn checkpoint_empty_pending_is_noop() {
        // `jetstream::Message` can't be constructed without a server,
        // so we only cover the empty path here; non-empty sealing is
        // exercised in `tests/nats_integration.rs`.
        let src = NatsSource::new(Arc::new(Schema::empty()), None);
        let _ = src.checkpoint();
        assert!(src.sealed.lock().is_empty());
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
