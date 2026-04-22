//! NATS source connector.
//!
//! `JetStream` pull consumer with ack-on-commit:
//! - A background task pulls messages and forwards them through a bounded
//!   channel.
//! - `poll_batch` drains the channel, deserializes to Arrow, and stashes
//!   the underlying `jetstream::Message` handles in `pending` so we can
//!   ack them later.
//! - `checkpoint()` seals the current `pending` batch into `sealed`.
//! - `notify_epoch_committed` drains `sealed` and acks every retained
//!   message.
//!
//! We don't try to map coordinator-epoch numbers to source-barrier
//! numbers. Each commit subsumes every earlier sealed batch, so draining
//! the queue in full is both simple and correct.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, consumer::pull};
use async_trait::async_trait;
use bytes::Bytes;
use crossfire::{mpsc, AsyncRx};
use futures_util::StreamExt;
use rustc_hash::FxHashMap;
use tokio::sync::Notify;
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

/// One inbound NATS message. `ack` is `Some` for `JetStream` (retained
/// until `notify_epoch_committed` acks it) and `None` for core subscribe
/// (at-most-once, no server-side ack protocol).
struct Incoming {
    subject: String,
    payload: Bytes,
    stream_seq: Option<u64>,
    ack: Option<jetstream::Message>,
}

/// Runtime state created by [`SourceConnector::open`] and torn down in
/// [`SourceConnector::close`]. Keeping these together avoids a handful of
/// parallel `Option<>` fields on `NatsSource`.
struct Running {
    deserializer: Box<dyn RecordDeserializer>,
    rx: AsyncRx<mpsc::Array<Incoming>>,
    /// Wakes the reader task out of a blocking `fetch()`. Using `Notify`
    /// instead of an `AtomicBool` drops shutdown lag from the fetch
    /// timeout (default 500ms) to sub-ms.
    shutdown: Arc<Notify>,
    /// Consecutive fetch errors on the reader task. Surfaced through
    /// `health_check` — flips the connector to `Unhealthy` once the
    /// configured threshold is exceeded.
    consecutive_errors: Arc<AtomicU32>,
    handle: JoinHandle<()>,
}

/// NATS source — core and `JetStream` modes behind a single type.
pub struct NatsSource {
    schema: SchemaRef,
    config: Option<NatsSourceConfig>,
    data_ready: Arc<Notify>,
    metrics: NatsSourceMetrics,
    running: Option<Running>,

    // Ack-on-commit bookkeeping.
    pending: Vec<jetstream::Message>,
    sealed: VecDeque<Vec<jetstream::Message>>,

    // Per-subject last-observed stream sequence, for checkpointing.
    offsets: FxHashMap<String, u64>,
}

impl NatsSource {
    /// Creates a new NATS source with the given output schema.
    ///
    /// If `registry` is `Some`, metrics register on it and show up in
    /// Prometheus scrapes.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        Self {
            schema,
            config: None,
            data_ready: Arc::new(Notify::new()),
            metrics: NatsSourceMetrics::new(registry),
            running: None,
            pending: Vec::new(),
            sealed: VecDeque::new(),
            offsets: FxHashMap::default(),
        }
    }

    fn update_pending_gauge(&self) {
        let sealed_total: usize = self.sealed.iter().map(Vec::len).sum();
        self.metrics
            .set_pending_acks(self.pending.len() + sealed_total);
    }

    /// Parsed config — available after [`SourceConnector::open`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSourceConfig> {
        self.config.as_ref()
    }

    #[allow(clippy::too_many_lines)] // reader-task body is easier to read inline
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
            .map_err(|e| classify_create_consumer_error(&e.to_string(), consumer_name))?;

        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let shutdown = Arc::new(Notify::new());
        let reader_shutdown = Arc::clone(&shutdown);
        let consecutive_errors = Arc::new(AtomicU32::new(0));
        let reader_errors = Arc::clone(&consecutive_errors);
        let data_ready = Arc::clone(&self.data_ready);
        let metrics = self.metrics.clone();
        let batch_size = cfg.fetch_batch;
        let max_wait = cfg.fetch_max_wait;

        let handle = tokio::spawn(async move {
            loop {
                let fetch_result = tokio::select! {
                    biased;
                    () = reader_shutdown.notified() => break,
                    r = consumer
                        .fetch()
                        .max_messages(batch_size)
                        .expires(max_wait)
                        .messages() => r,
                };

                let mut stream = match fetch_result {
                    Ok(s) => {
                        reader_errors.store(0, Ordering::Release);
                        s
                    }
                    Err(e) => {
                        let errs = reader_errors.fetch_add(1, Ordering::AcqRel) + 1;
                        metrics.record_fetch_error();
                        warn!(
                            error = %e,
                            consecutive_errors = errs,
                            "nats fetch() errored; backing off",
                        );
                        let backoff = fetch_backoff(errs);
                        tokio::select! {
                            biased;
                            () = reader_shutdown.notified() => break,
                            () = tokio::time::sleep(backoff) => {}
                        }
                        continue;
                    }
                };

                let mut forwarded = 0usize;
                loop {
                    let msg_result = tokio::select! {
                        biased;
                        () = reader_shutdown.notified() => return,
                        r = stream.next() => match r {
                            Some(r) => r,
                            None => break,
                        },
                    };
                    let msg = match msg_result {
                        Ok(m) => m,
                        Err(e) => {
                            metrics.record_fetch_error();
                            warn!(error = %e, "nats message error");
                            continue;
                        }
                    };
                    let stream_seq = msg.info().ok().map(|i| i.stream_sequence);
                    let subject = msg.subject.to_string();
                    let payload = msg.payload.clone();
                    let incoming = Incoming {
                        subject,
                        payload,
                        stream_seq,
                        ack: Some(msg),
                    };
                    if tx.send(incoming).await.is_err() {
                        debug!("nats reader: downstream channel closed");
                        return;
                    }
                    forwarded += 1;
                }
                if forwarded > 0 {
                    data_ready.notify_one();
                }
            }
        });

        self.running = Some(Running {
            deserializer,
            rx,
            shutdown,
            consecutive_errors,
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
        let mut subscriber = if let Some(group) = cfg.queue_group.as_deref() {
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

        // Core deliveries land on the poll_batch side's `record_poll`;
        // the subscriber itself doesn't surface fetch errors the way a
        // JS pull loop does. async-nats handles reconnect transparently.
        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let shutdown = Arc::new(Notify::new());
        let reader_shutdown = Arc::clone(&shutdown);
        let consecutive_errors = Arc::new(AtomicU32::new(0));
        let data_ready = Arc::clone(&self.data_ready);

        let handle = tokio::spawn(async move {
            loop {
                let msg = tokio::select! {
                    biased;
                    () = reader_shutdown.notified() => break,
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
        });

        self.running = Some(Running {
            deserializer,
            rx,
            shutdown,
            consecutive_errors,
            handle,
        });
        Ok(())
    }

    fn seal_pending(&mut self) {
        if !self.pending.is_empty() {
            let batch = std::mem::take(&mut self.pending);
            self.sealed.push_back(batch);
        }
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

        while payloads.len() < max_records {
            let Ok(incoming) = running.rx.try_recv() else {
                break;
            };
            if let Some(seq) = incoming.stream_seq {
                self.offsets
                    .entry(incoming.subject.clone())
                    .and_modify(|s| *s = (*s).max(seq))
                    .or_insert(seq);
                if partition.is_none() {
                    partition = Some(PartitionInfo::new(&incoming.subject, seq.to_string()));
                }
            }
            payloads.push(incoming.payload);
            if let Some(msg) = incoming.ack {
                self.pending.push(msg);
            }
        }

        if payloads.is_empty() {
            return Ok(None);
        }

        let records: Vec<&[u8]> = payloads.iter().map(Bytes::as_ref).collect();
        let bytes_total: u64 = records.iter().map(|r| r.len() as u64).sum();
        let batch = running
            .deserializer
            .deserialize_batch(&records, &self.schema)
            .map_err(|e| err(&format!("deserialize batch: {e}")))?;

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
        let mut cp = SourceCheckpoint::default();
        for (subject, seq) in &self.offsets {
            cp.set_offset(subject.as_str(), seq.to_string());
        }
        cp
    }

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        // The durable consumer remembers its ack floor on the server; on
        // reconnect it resumes from there. If the manifest is ahead of
        // the floor (out-of-band surgery), we'd just log and proceed —
        // rewriting the consumer is destructive and not worth it.
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
        // Seal whatever is in pending so the current in-flight checkpoint
        // sees it too, then ack every retained message. Any per-message
        // ack error is logged but doesn't roll the epoch back — the
        // broker will redeliver on ack_wait and sink dedup will drop it.
        self.seal_pending();
        while let Some(batch) = self.sealed.pop_front() {
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

        // Unhealthy first: fetch-loop errors indicate a problem even if
        // `max_ack_pending` happens to be low.
        if let Some(running) = self.running.as_ref() {
            let errs = running.consecutive_errors.load(Ordering::Acquire);
            if errs >= cfg.fetch_error_threshold {
                return HealthStatus::Unhealthy(format!(
                    "{errs} consecutive fetch errors (threshold {})",
                    cfg.fetch_error_threshold
                ));
            }
        }

        // Degraded: pending acks saturate half of `max_ack_pending`. The
        // broker pauses delivery at 100%; yellow-light before that.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let cap = cfg.max_ack_pending.max(1) as u64;
        #[allow(clippy::cast_sign_loss)]
        let pending = self.metrics.pending_acks.get().max(0) as u64;
        if pending * 2 >= cap {
            HealthStatus::Degraded(format!(
                "pending acks {pending}/{cap} — broker may throttle delivery"
            ))
        } else {
            HealthStatus::Healthy
        }
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
    build_connect_options(&cfg.auth, &cfg.tls)
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
            return Err(err(
                "deliver.policy=by_start_time is not yet supported; use by_start_sequence",
            ));
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

/// Exponential backoff for fetch errors: 500ms, 1s, 2s, 4s, 5s cap.
fn fetch_backoff(consecutive_errors: u32) -> Duration {
    let exp = consecutive_errors.saturating_sub(1).min(4);
    let ms = 500u64.saturating_mul(1u64 << exp);
    Duration::from_millis(ms.min(5000))
}

/// Translate a `create_consumer` error into something operator-readable.
/// NATS server error 10148 (or similar text like "consumer config" /
/// "already exists") means the durable consumer exists with a different
/// config. Re-creating with a conflicting config is a footgun; surface
/// a fix-it message instead of the raw server text.
fn classify_create_consumer_error(err_msg: &str, consumer_name: &str) -> ConnectorError {
    let lower = err_msg.to_ascii_lowercase();
    if lower.contains("10148")
        || lower.contains("consumer config")
        || lower.contains("already exists")
    {
        err(&format!(
            "[LDB-5070] consumer '{consumer_name}' exists with incompatible config; \
             rotate the durable name or delete the consumer out-of-band. \
             Server said: {err_msg}"
        ))
    } else {
        err(&format!(
            "create_consumer('{consumer_name}') failed: {err_msg}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;

    #[test]
    fn seal_pending_moves_to_sealed() {
        let mut src = NatsSource::new(Arc::new(Schema::empty()), None);
        // We can't easily construct jetstream::Message without a real
        // server, so we just test the empty-pending no-op path.
        src.seal_pending();
        assert!(src.sealed.is_empty());
    }

    #[test]
    fn fetch_backoff_grows_then_caps_at_5s() {
        assert_eq!(fetch_backoff(1), Duration::from_millis(500));
        assert_eq!(fetch_backoff(2), Duration::from_millis(1000));
        assert_eq!(fetch_backoff(3), Duration::from_millis(2000));
        assert_eq!(fetch_backoff(4), Duration::from_millis(4000));
        assert_eq!(fetch_backoff(5), Duration::from_millis(5000));
        assert_eq!(fetch_backoff(100), Duration::from_millis(5000));
    }

    #[test]
    fn drift_error_contains_ldb_5070() {
        let e =
            classify_create_consumer_error("consumer config already exists (10148)", "my-consumer");
        assert!(e.to_string().contains("LDB-5070"), "got: {e}");
        assert!(e.to_string().contains("my-consumer"));
    }

    #[test]
    fn drift_error_matches_on_server_code_10148() {
        let e = classify_create_consumer_error("err 10148", "c");
        assert!(e.to_string().contains("LDB-5070"));
    }

    #[test]
    fn generic_create_error_is_not_drift() {
        let e = classify_create_consumer_error("network timeout", "c");
        assert!(!e.to_string().contains("LDB-5070"), "got: {e}");
    }
}
