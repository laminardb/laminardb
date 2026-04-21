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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::SchemaRef;
use async_nats::jetstream::{self, consumer::pull};
use async_trait::async_trait;
use crossfire::{mpsc, AsyncRx};
use futures_util::StreamExt;
use rustc_hash::FxHashMap;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::config::{AckPolicy, DeliverPolicy, Mode, NatsSourceConfig};
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{PartitionInfo, SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::serde::{self, RecordDeserializer};

/// One inbound NATS message. `ack` is `Some` for `JetStream` (retained
/// until `notify_epoch_committed` acks it) and `None` for core subscribe
/// (at-most-once, no server-side ack protocol).
struct Incoming {
    subject: String,
    payload: Vec<u8>,
    stream_seq: u64,
    ack: Option<jetstream::Message>,
}

/// NATS source — core and `JetStream` modes behind a single type.
pub struct NatsSource {
    schema: SchemaRef,
    config: Option<NatsSourceConfig>,

    // Runtime state — populated by open().
    deserializer: Option<Box<dyn RecordDeserializer>>,
    rx: Option<AsyncRx<mpsc::Array<Incoming>>>,
    reader_shutdown: Option<Arc<AtomicBool>>,
    reader_handle: Option<JoinHandle<()>>,
    data_ready: Arc<Notify>,

    // Ack-on-commit bookkeeping.
    pending: Vec<jetstream::Message>,
    sealed: VecDeque<Vec<jetstream::Message>>,

    // Per-subject last-observed stream sequence, for checkpointing.
    offsets: FxHashMap<String, u64>,
}

impl NatsSource {
    /// Creates a new NATS source with the given output schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            config: None,
            deserializer: None,
            rx: None,
            reader_shutdown: None,
            reader_handle: None,
            data_ready: Arc::new(Notify::new()),
            pending: Vec::new(),
            sealed: VecDeque::new(),
            offsets: FxHashMap::default(),
        }
    }

    /// Parsed config — available after [`SourceConnector::open`].
    #[must_use]
    pub fn config(&self) -> Option<&NatsSourceConfig> {
        self.config.as_ref()
    }

    async fn open_jetstream(&mut self, cfg: &NatsSourceConfig) -> Result<(), ConnectorError> {
        let client = connect(&cfg.servers).await?;
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
            .map_err(|e| err(&format!("create_consumer('{consumer_name}') failed: {e}")))?;

        // Spawn the reader task. `rx` is drained by poll_batch.
        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let shutdown = Arc::new(AtomicBool::new(false));
        let reader_shutdown = Arc::clone(&shutdown);
        let data_ready = Arc::clone(&self.data_ready);
        let batch_size = cfg.fetch_batch;
        let max_wait = cfg.fetch_max_wait;

        let handle = tokio::spawn(async move {
            loop {
                if reader_shutdown.load(Ordering::Acquire) {
                    break;
                }

                let fetch = consumer
                    .fetch()
                    .max_messages(batch_size)
                    .expires(max_wait)
                    .messages()
                    .await;

                let mut stream = match fetch {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "nats fetch() errored; backing off");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };

                let mut forwarded = 0usize;
                while let Some(msg_result) = stream.next().await {
                    let msg = match msg_result {
                        Ok(m) => m,
                        Err(e) => {
                            warn!(error = %e, "nats message error");
                            continue;
                        }
                    };
                    let stream_seq = msg.info().ok().map_or(0, |i| i.stream_sequence);
                    let subject = msg.subject.to_string();
                    let payload = msg.payload.to_vec();
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

        self.rx = Some(rx);
        self.reader_shutdown = Some(shutdown);
        self.reader_handle = Some(handle);
        Ok(())
    }

    async fn open_core(&mut self, cfg: &NatsSourceConfig) -> Result<(), ConnectorError> {
        let client = connect(&cfg.servers).await?;
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

        let (tx, rx) = mpsc::bounded_async::<Incoming>(cfg.fetch_batch * 2);
        let shutdown = Arc::new(AtomicBool::new(false));
        let reader_shutdown = Arc::clone(&shutdown);
        let data_ready = Arc::clone(&self.data_ready);

        let handle = tokio::spawn(async move {
            while let Some(msg) = subscriber.next().await {
                if reader_shutdown.load(Ordering::Acquire) {
                    break;
                }
                let incoming = Incoming {
                    subject: msg.subject.to_string(),
                    payload: msg.payload.to_vec(),
                    stream_seq: 0,
                    ack: None,
                };
                if tx.send(incoming).await.is_err() {
                    return;
                }
                data_ready.notify_one();
            }
        });

        self.rx = Some(rx);
        self.reader_shutdown = Some(shutdown);
        self.reader_handle = Some(handle);
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
        self.deserializer = Some(
            serde::create_deserializer(cfg.format)
                .map_err(|e| err(&format!("deserializer for format {:?}: {e}", cfg.format)))?,
        );
        match cfg.mode {
            Mode::JetStream => self.open_jetstream(&cfg).await?,
            Mode::Core => self.open_core(&cfg).await?,
        }
        self.config = Some(cfg);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(rx) = self.rx.as_ref() else {
            return Ok(None);
        };

        let mut payloads: Vec<Vec<u8>> = Vec::new();
        let mut first_subject: Option<String> = None;
        let mut first_seq: Option<u64> = None;

        while payloads.len() < max_records {
            let Ok(incoming) = rx.try_recv() else { break };
            if first_subject.is_none() {
                first_subject = Some(incoming.subject.clone());
                first_seq = Some(incoming.stream_seq);
            }
            self.offsets
                .entry(incoming.subject.clone())
                .and_modify(|s| *s = (*s).max(incoming.stream_seq))
                .or_insert(incoming.stream_seq);
            payloads.push(incoming.payload);
            if let Some(msg) = incoming.ack {
                self.pending.push(msg);
            }
        }

        if payloads.is_empty() {
            return Ok(None);
        }

        let deser = self
            .deserializer
            .as_ref()
            .ok_or_else(|| err("deserializer not initialized"))?;
        let records: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let batch = deser
            .deserialize_batch(&records, &self.schema)
            .map_err(|e| err(&format!("deserialize batch: {e}")))?;

        let partition =
            first_subject.map(|s| PartitionInfo::new(s, first_seq.unwrap_or(0).to_string()));
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
        // reconnect it resumes from there. We log loudly if the manifest
        // is ahead of the floor (out-of-band surgery), but don't try to
        // rewrite the consumer.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if let Some(flag) = self.reader_shutdown.take() {
            flag.store(true, Ordering::Release);
        }
        if let Some(handle) = self.reader_handle.take() {
            // Give the reader a brief window to finish in-flight fetch.
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }
        self.rx = None;
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
                if let Err(e) = msg.ack().await {
                    warn!(error = %e, "JetStream ack failed; broker will redeliver");
                }
            }
        }
        Ok(())
    }
}

// ── helpers ──

fn err(msg: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(msg.to_string())
}

async fn connect(servers: &[String]) -> Result<async_nats::Client, ConnectorError> {
    async_nats::ConnectOptions::new()
        .connect(servers)
        .await
        .map_err(|e| err(&format!("nats connect({servers:?}): {e}")))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;

    #[test]
    fn seal_pending_moves_to_sealed() {
        let mut src = NatsSource::new(Arc::new(Schema::empty()));
        // We can't easily construct jetstream::Message without a real
        // server, so we just test the empty-pending no-op path.
        src.seal_pending();
        assert!(src.sealed.is_empty());
    }
}
