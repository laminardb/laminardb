//! Kafka source connector implementation.
//!
//! [`KafkaSource`] implements the [`SourceConnector`] trait, consuming
//! from Kafka topics via rdkafka's `StreamConsumer`, deserializing
//! messages using pluggable formats, and producing Arrow `RecordBatch`
//! data through the connector SDK.

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use rdkafka::TopicPartitionList;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use super::rebalance::LaminarConsumerContext;

/// Locks a mutex, recovering from poison if a prior holder panicked.
///
/// Used for state shared with rdkafka's rebalance callback thread.
/// Poison indicates a panic in the callback — the data may be stale
/// but is structurally sound, so we recover rather than propagate.
fn lock_or_recover<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        tracing::warn!("mutex poisoned, recovering");
        poisoned.into_inner()
    })
}

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{PartitionInfo, SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;
use crate::serde::{self, Format, RecordDeserializer};

use super::avro::AvroDeserializer;
use super::config::{
    resolve_value_subject, KafkaSourceConfig, SchemaEvolutionStrategy, StartupMode,
    TopicSubscription,
};
use super::metrics::KafkaSourceMetrics;
use super::offsets::OffsetTracker;
use super::rebalance::RebalanceState;
use super::schema_registry::SchemaRegistryClient;
use super::watermarks::KafkaWatermarkTracker;

use crate::schema::evolution::SchemaEvolution;
use crate::schema::traits::{CompatibilityMode, EvolutionVerdict};

/// Payload sent from the background Kafka reader task to [`KafkaSource::poll_batch`].
struct KafkaPayload {
    data: Vec<u8>,
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    timestamp_ms: Option<i64>,
    /// Kafka message headers serialized as JSON string ("{key: value, ...}").
    /// Only populated when `include_headers` is enabled.
    headers_json: Option<String>,
}

/// Single-consumer async receiver for the reader → `poll_batch` queue.
type KafkaPayloadRx = crossfire::AsyncRx<crossfire::mpsc::Array<KafkaPayload>>;

/// Kafka source connector that consumes messages and produces Arrow batches.
///
/// Operates in Ring 1 (background) and pushes deserialized `RecordBatch`
/// data to Ring 0 via the streaming `Source<T>` API.
///
/// # Lifecycle
///
/// 1. Create with [`KafkaSource::new`] or [`KafkaSource::with_schema_registry`]
/// 2. Call `open()` to connect to Kafka and subscribe to topics
/// 3. Call `poll_batch()` in a loop to consume messages
/// 4. Call `checkpoint()` / `restore()` for fault tolerance
/// 5. Call `close()` for clean shutdown
pub struct KafkaSource {
    consumer: Option<StreamConsumer<LaminarConsumerContext>>,
    config: KafkaSourceConfig,
    deserializer: Box<dyn RecordDeserializer>,
    offsets: OffsetTracker,
    state: ConnectorState,
    metrics: KafkaSourceMetrics,
    schema: SchemaRef,
    channel_len: Arc<AtomicUsize>,
    rebalance_state: Arc<Mutex<RebalanceState>>,
    /// Shared rebalance counter bridging `LaminarConsumerContext` → `KafkaSourceMetrics`.
    rebalance_counter: Arc<AtomicU64>,
    /// Monotonic counter bumped on each partition revoke event.
    ///
    /// Shared with `LaminarConsumerContext` for lock-free revoke detection
    /// from `poll_batch()`. The source compares `last_seen_revoke_gen`
    /// against this value each poll cycle and only locks `rebalance_state`
    /// when a change is detected to purge revoked partition offsets.
    revoke_generation: Arc<AtomicU64>,
    /// Last observed value of `revoke_generation`, cached per poll cycle.
    last_seen_revoke_gen: u64,
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    data_ready: Arc<Notify>,
    checkpoint_request: Arc<AtomicBool>,
    msg_rx: Option<KafkaPayloadRx>,
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    commit_handle: Option<tokio::task::JoinHandle<()>>,
    hwm_handle: Option<tokio::task::JoinHandle<()>>,
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,
    offset_commit_tx: Option<tokio::sync::watch::Sender<TopicPartitionList>>,
    watermark_tracker: Option<KafkaWatermarkTracker>,
    /// Receiver for high watermark data from the background reader task.
    /// Each entry is `(topic, partition, high_watermark)` for lag computation.
    #[allow(clippy::type_complexity)]
    high_watermarks_rx: Option<tokio::sync::watch::Receiver<Vec<(Arc<str>, i32, i64)>>>,
    /// Shared flag: `true` when the reader task has paused Kafka partitions
    /// due to downstream backpressure. Used to re-pause newly assigned
    /// partitions during rebalance.
    reader_paused: Arc<AtomicBool>,
    /// Set by `commit_callback` on async commit failure; reader task
    /// escalates to `CommitMode::Sync` on the next commit timer tick.
    commit_retry_needed: Arc<AtomicBool>,
    /// Offset snapshot shared with the rebalance callback for seek-on-assign.
    /// Updated once per `poll_batch()` cycle (not per message).
    offset_snapshot: Arc<Mutex<OffsetTracker>>,

    /// Last Avro writer schema from the schema registry, used to diff
    /// successive versions for evolution detection.
    last_avro_schema: Option<SchemaRef>,

    // Reusable poll_batch buffers — cleared each cycle, capacity retained.
    poll_payload_buf: Vec<u8>,
    poll_payload_offsets: Vec<(usize, usize)>,
    poll_meta_partitions: Vec<i32>,
    poll_meta_offsets: Vec<i64>,
    poll_meta_timestamps: Vec<Option<i64>>,
    poll_meta_headers: Vec<Option<String>>,
}

impl KafkaSource {
    /// Creates a new Kafka source connector with explicit schema.
    #[must_use]
    pub fn new(schema: SchemaRef, config: KafkaSourceConfig) -> Self {
        Self::build_base(schema, config, select_deserializer, None)
    }

    /// Creates a new Kafka source connector with Schema Registry.
    #[must_use]
    pub fn with_schema_registry(
        schema: SchemaRef,
        config: KafkaSourceConfig,
        sr_client: SchemaRegistryClient,
    ) -> Self {
        let sr = Arc::new(sr_client);
        let sr_clone = Arc::clone(&sr);
        let deser_factory = move |format: Format| -> Box<dyn RecordDeserializer> {
            if format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(sr_clone))
            } else {
                select_deserializer(format)
            }
        };
        Self::build_base(schema, config, deser_factory, Some(sr))
    }

    /// Build a Schema Registry client from the parsed config, or
    /// `Ok(None)` when `schema.registry.url` is not set.
    fn build_sr_client(
        config: &KafkaSourceConfig,
    ) -> Result<Option<SchemaRegistryClient>, ConnectorError> {
        let Some(sr_url) = config.schema_registry_url.as_ref() else {
            return Ok(None);
        };
        let client = if let Some(ca) = config.schema_registry_ssl_ca_location.as_deref() {
            SchemaRegistryClient::with_tls_mtls(
                sr_url.clone(),
                config.schema_registry_auth.clone(),
                ca,
                config.schema_registry_ssl_certificate_location.as_deref(),
                config.schema_registry_ssl_key_location.as_deref(),
            )?
        } else {
            SchemaRegistryClient::new(sr_url.clone(), config.schema_registry_auth.clone())
        };
        Ok(Some(client))
    }

    fn build_base(
        schema: SchemaRef,
        config: KafkaSourceConfig,
        deser_factory: impl FnOnce(Format) -> Box<dyn RecordDeserializer>,
        schema_registry: Option<Arc<SchemaRegistryClient>>,
    ) -> Self {
        let deserializer = deser_factory(config.format);
        let channel_len = Arc::new(AtomicUsize::new(0));

        let watermark_tracker = if config.enable_watermark_tracking {
            Some(
                KafkaWatermarkTracker::new(0, config.idle_timeout)
                    .with_max_out_of_orderness(config.max_out_of_orderness),
            )
        } else {
            None
        };

        Self {
            consumer: None,
            config,
            deserializer,
            offsets: OffsetTracker::new(),
            state: ConnectorState::Created,
            metrics: KafkaSourceMetrics::new(),
            schema,
            channel_len,
            rebalance_state: Arc::new(Mutex::new(RebalanceState::new())),
            rebalance_counter: Arc::new(AtomicU64::new(0)),
            revoke_generation: Arc::new(AtomicU64::new(0)),
            last_seen_revoke_gen: 0,
            schema_registry,
            data_ready: Arc::new(Notify::new()),
            checkpoint_request: Arc::new(AtomicBool::new(false)),
            msg_rx: None,
            reader_handle: None,
            commit_handle: None,
            hwm_handle: None,
            reader_shutdown: None,
            offset_commit_tx: None,
            watermark_tracker,
            high_watermarks_rx: None,
            reader_paused: Arc::new(AtomicBool::new(false)),
            commit_retry_needed: Arc::new(AtomicBool::new(false)),
            offset_snapshot: Arc::new(Mutex::new(OffsetTracker::new())),
            last_avro_schema: None,
            poll_payload_buf: Vec::new(),
            poll_payload_offsets: Vec::new(),
            poll_meta_partitions: Vec::new(),
            poll_meta_offsets: Vec::new(),
            poll_meta_timestamps: Vec::new(),
            poll_meta_headers: Vec::new(),
        }
    }

    /// Lifecycle state (Created → Initializing → Running → Closed).
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Per-topic-partition offset state for checkpoint and monitoring.
    #[must_use]
    pub fn offsets(&self) -> &OffsetTracker {
        &self.offsets
    }

    /// Shared backpressure fill counter for downstream wiring.
    #[must_use]
    pub fn channel_len(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.channel_len)
    }

    /// Shared partition assignment state (updated by rebalance callbacks).
    #[must_use]
    pub fn rebalance_state(&self) -> Arc<Mutex<RebalanceState>> {
        Arc::clone(&self.rebalance_state)
    }

    /// Whether a Schema Registry client is configured.
    #[must_use]
    pub fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
    }

    /// Current combined watermark from the watermark tracker.
    ///
    /// Returns `None` if watermark tracking is disabled or no partitions
    /// have received data yet.
    #[must_use]
    pub fn current_watermark(&self) -> Option<i64> {
        self.watermark_tracker
            .as_ref()
            .and_then(KafkaWatermarkTracker::current_watermark)
    }

    /// Returns the configured event-time column name, if any.
    ///
    /// Used by the pipeline to identify which column contains event timestamps
    /// for watermark generation, instead of hardcoding `event_time`/`timestamp`.
    #[must_use]
    pub fn event_time_column(&self) -> Option<&str> {
        self.config.event_time_column.as_deref()
    }

    /// Spawns background tasks on first `poll_batch()` call.
    ///
    /// Three tasks handle separate concerns:
    /// - **Reader**: consumes messages, manages backpressure, detects revokes
    /// - **Commit**: periodic advisory broker offset commits
    /// - **HWM**: periodic high watermark queries for lag monitoring
    ///
    /// Deferred to allow `restore()` to access the consumer directly after `open()`.
    #[allow(clippy::too_many_lines)]
    fn ensure_reader_started(&mut self) {
        if self.reader_handle.is_some() || self.consumer.is_none() {
            return;
        }

        let consumer = Arc::new(self.consumer.take().unwrap());
        let (msg_tx, msg_rx) =
            crossfire::mpsc::bounded_async::<KafkaPayload>(self.config.reader_channel_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (offset_tx, offset_rx) = tokio::sync::watch::channel(TopicPartitionList::new());
        let reader_offset_rx = offset_rx.clone(); // reader reads final offsets on shutdown
        let (hwm_tx, hwm_rx) = tokio::sync::watch::channel(Vec::new());
        // Channel for the reader to publish seen partitions to the HWM task.
        let (seen_tx, seen_rx) = tokio::sync::watch::channel(Vec::<(Arc<str>, i32)>::new());
        let data_ready = Arc::clone(&self.data_ready);
        let channel_len = Arc::clone(&self.channel_len);
        let capture_headers = self.config.include_headers;
        let broker_commit_interval = self.config.broker_commit_interval;
        let reader_channel_capacity = self.config.reader_channel_capacity;
        let reader_paused = Arc::clone(&self.reader_paused);
        let revoke_generation = Arc::clone(&self.revoke_generation);
        let rebalance_state = Arc::clone(&self.rebalance_state);
        let commit_retry_needed = Arc::clone(&self.commit_retry_needed);
        let pause_threshold = self.config.backpressure_high_watermark;
        let resume_threshold = self.config.backpressure_low_watermark;

        // -- Commit task: periodic advisory broker offset commits --
        let commit_consumer = Arc::clone(&consumer);
        let commit_retry = Arc::clone(&commit_retry_needed);
        let mut commit_shutdown = shutdown_rx.clone();
        let commit_handle = tokio::spawn(async move {
            if broker_commit_interval.is_zero() {
                // Disabled — wait for shutdown.
                let _ = commit_shutdown.changed().await;
                return;
            }
            let mut timer = tokio::time::interval(broker_commit_interval);
            timer.tick().await; // skip first

            loop {
                tokio::select! {
                    biased;
                    _ = commit_shutdown.changed() => break,
                    _ = timer.tick() => {
                        let tpl = offset_rx.borrow().clone();
                        if tpl.count() == 0 { continue; }
                        if commit_retry
                            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                            .is_ok()
                        {
                            let c = Arc::clone(&commit_consumer);
                            let flag = Arc::clone(&commit_retry);
                            let n = tpl.count();
                            // Await the blocking commit so it completes before
                            // close() drops the consumer Arc.
                            match tokio::time::timeout(
                                std::time::Duration::from_secs(10),
                                tokio::task::spawn_blocking(move || {
                                    c.commit(&tpl, CommitMode::Sync)
                                }),
                            )
                            .await
                            {
                                Ok(Ok(Ok(()))) => info!(partition_count = n, "sync offset commit retry succeeded"),
                                Ok(Ok(Err(e))) => {
                                    flag.store(true, Ordering::Release);
                                    warn!(error = %e, "sync offset commit retry failed");
                                }
                                Ok(Err(e)) => warn!(error = %e, "sync commit blocking task panicked"),
                                Err(_) => {
                                    flag.store(true, Ordering::Release);
                                    warn!("sync offset commit retry timed out");
                                }
                            }
                        } else {
                            match commit_consumer.commit(&tpl, CommitMode::Async) {
                                Ok(()) => info!(partition_count = tpl.count(), "periodic broker offset commit (advisory)"),
                                Err(e) => warn!(error = %e, "periodic broker offset commit failed"),
                            }
                        }
                    }
                }
            }
        });

        // -- HWM task: periodic high watermark queries for lag monitoring --
        let hwm_consumer = Arc::clone(&consumer);
        let mut hwm_shutdown = shutdown_rx.clone();
        let hwm_handle = tokio::spawn(async move {
            let mut timer = tokio::time::interval(std::time::Duration::from_secs(30));
            timer.tick().await; // skip first

            loop {
                tokio::select! {
                    biased;
                    _ = hwm_shutdown.changed() => break,
                    _ = timer.tick() => {
                        let partitions: Vec<_> = seen_rx.borrow().clone();
                        if partitions.is_empty() { continue; }
                        let c = Arc::clone(&hwm_consumer);
                        let watermarks = tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            tokio::task::spawn_blocking(move || {
                                let mut results = Vec::with_capacity(partitions.len());
                                for (topic, partition) in &partitions {
                                    match c.fetch_watermarks(topic, *partition, std::time::Duration::from_secs(1)) {
                                        Ok((_low, high)) => results.push((Arc::clone(topic), *partition, high)),
                                        Err(e) => debug!(%topic, partition, error = %e, "HWM fetch failed"),
                                    }
                                }
                                results
                            }),
                        )
                        .await
                        .unwrap_or(Ok(Vec::new()))
                        .unwrap_or_default();
                        if !watermarks.is_empty() {
                            let _ = hwm_tx.send(watermarks);
                        }
                    }
                }
            }
        });

        // -- Reader task: message consumption, backpressure, revoke pruning --
        let mut reader_shutdown = shutdown_rx;
        let reader_handle = tokio::spawn(async move {
            let mut cached_topic: Arc<str> = Arc::from("");
            let mut seen_partitions: std::collections::HashSet<(Arc<str>, i32)> =
                std::collections::HashSet::new();
            let mut is_paused = false;
            let mut last_revoke_gen: u64 = 0;

            loop {
                // Prune seen_partitions on revoke so HWM task stops querying them.
                let current_gen = revoke_generation.load(Ordering::Acquire);
                if current_gen != last_revoke_gen {
                    last_revoke_gen = current_gen;
                    let assigned = lock_or_recover(&rebalance_state)
                        .assigned_partitions()
                        .clone();
                    seen_partitions.retain(|(t, p)| assigned.contains(&(t.to_string(), *p)));
                    let _ = seen_tx.send(seen_partitions.iter().cloned().collect());
                }

                // Backpressure: pause/resume Kafka partitions based on channel fill.
                #[allow(clippy::cast_precision_loss)]
                let fill = if reader_channel_capacity > 0 {
                    channel_len.load(Ordering::Acquire) as f64 / reader_channel_capacity as f64
                } else {
                    0.0
                };
                if fill >= pause_threshold && !is_paused {
                    if let Ok(assignment) = consumer.assignment() {
                        if consumer.pause(&assignment).is_ok() {
                            is_paused = true;
                            reader_paused.store(true, Ordering::Release);
                            debug!("reader: paused Kafka partitions (fill={fill:.2})");
                        }
                    }
                } else if fill <= resume_threshold && is_paused {
                    if let Ok(assignment) = consumer.assignment() {
                        if consumer.resume(&assignment).is_ok() {
                            is_paused = false;
                            reader_paused.store(false, Ordering::Release);
                            debug!("reader: resumed Kafka partitions (fill={fill:.2})");
                        }
                    }
                }

                let msg_result = tokio::select! {
                    biased;
                    _ = reader_shutdown.changed() => break,
                    msg = tokio::time::timeout(
                        std::time::Duration::from_millis(200),
                        consumer.recv(),
                    ) => match msg {
                        Ok(result) => result,
                        Err(_timeout) => continue,
                    },
                };
                match msg_result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload() {
                            let topic = msg.topic();
                            if &*cached_topic != topic {
                                cached_topic = Arc::from(topic);
                            }
                            if seen_partitions.insert((Arc::clone(&cached_topic), msg.partition()))
                            {
                                let _ = seen_tx.send(seen_partitions.iter().cloned().collect());
                            }
                            let timestamp_ms = match msg.timestamp() {
                                rdkafka::Timestamp::CreateTime(ts)
                                | rdkafka::Timestamp::LogAppendTime(ts) => Some(ts),
                                rdkafka::Timestamp::NotAvailable => None,
                            };
                            let headers_json = if capture_headers {
                                use rdkafka::message::Headers;
                                msg.headers().and_then(|hdrs| {
                                    let pairs: Vec<(String, serde_json::Value)> = (0..hdrs.count())
                                        .map(|i| {
                                            let h = hdrs.get(i);
                                            let val = match h.value {
                                                Some(v) => serde_json::Value::String(
                                                    String::from_utf8_lossy(v).into_owned(),
                                                ),
                                                None => serde_json::Value::Null,
                                            };
                                            (h.key.to_string(), val)
                                        })
                                        .collect();
                                    serde_json::to_string(&pairs).ok()
                                })
                            } else {
                                None
                            };
                            let kp = KafkaPayload {
                                data: payload.to_vec(),
                                topic: Arc::clone(&cached_topic),
                                partition: msg.partition(),
                                offset: msg.offset(),
                                timestamp_ms,
                                headers_json,
                            };
                            match msg_tx.try_send(kp) {
                                Ok(()) => {
                                    channel_len.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(crossfire::TrySendError::Full(kp)) => {
                                    if !is_paused {
                                        if let Ok(assignment) = consumer.assignment() {
                                            if consumer.pause(&assignment).is_ok() {
                                                is_paused = true;
                                                reader_paused.store(true, Ordering::Release);
                                                debug!("reader: paused partitions (channel full)");
                                            }
                                        }
                                    }
                                    channel_len.fetch_add(1, Ordering::Relaxed);
                                    let send_ok = tokio::select! {
                                        biased;
                                        _ = reader_shutdown.changed() => false,
                                        result = msg_tx.send(kp) => result.is_ok(),
                                    };
                                    if !send_ok {
                                        channel_len.fetch_sub(1, Ordering::Relaxed);
                                        break;
                                    }
                                }
                                Err(crossfire::TrySendError::Disconnected(_)) => break,
                            }
                            data_ready.notify_one();
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Kafka consumer error");
                    }
                }
            }

            // Final commit + unsubscribe (reader owns the consumer Arc).
            // The close() method sends latest offsets via offset_tx before
            // signaling shutdown, so the commit task may have already
            // committed them — this is a best-effort final flush.
            let tpl = reader_offset_rx.borrow().clone();
            if tpl.count() > 0 {
                match consumer.commit(&tpl, CommitMode::Sync) {
                    Ok(()) => info!(
                        partition_count = tpl.count(),
                        "committed final offsets on shutdown"
                    ),
                    Err(e) => warn!(error = %e, "failed to commit final offsets on shutdown"),
                }
            }
            consumer.unsubscribe();
        });

        self.msg_rx = Some(msg_rx);
        self.reader_handle = Some(reader_handle);
        self.commit_handle = Some(commit_handle);
        self.hwm_handle = Some(hwm_handle);
        self.reader_shutdown = Some(shutdown_tx);
        self.offset_commit_tx = Some(offset_tx);
        self.high_watermarks_rx = Some(hwm_rx);
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)] // poll_batch has legitimate complexity (backpressure + deser + poison pill fallback)
impl SourceConnector for KafkaSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // If config provided, re-parse (supports runtime config override).
        let kafka_config = if config.properties().is_empty() {
            self.config.clone()
        } else {
            let parsed = KafkaSourceConfig::from_config(config)?;
            self.config = parsed.clone();
            parsed
        };

        // Re-select deserializer (factory defaults to JSON).
        if let Some(sr_client) = Self::build_sr_client(&kafka_config)? {
            let sr = Arc::new(sr_client);
            self.schema_registry = Some(Arc::clone(&sr));
            self.deserializer = if kafka_config.format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(sr))
            } else {
                select_deserializer(kafka_config.format)
            };
        } else if let Some(ref sr) = self.schema_registry {
            // Preserve SR client injected via with_schema_registry().
            self.deserializer = if kafka_config.format == Format::Avro {
                Box::new(AvroDeserializer::with_schema_registry(Arc::clone(sr)))
            } else {
                select_deserializer(kafka_config.format)
            };
        } else {
            self.deserializer = select_deserializer(kafka_config.format);
        }

        // New deserializer has empty known_ids; reset evolution baseline to match.
        self.last_avro_schema = None;

        // Override schema from SQL DDL if provided.
        if let Some(schema) = config.arrow_schema() {
            info!(
                fields = schema.fields().len(),
                "using SQL-defined schema for deserialization"
            );
            self.schema = schema;
        }

        info!(
            brokers = %kafka_config.bootstrap_servers,
            subscription = ?kafka_config.subscription,
            group_id = %kafka_config.group_id,
            format = %kafka_config.format,
            schema_fields = self.schema.fields().len(),
            "opening Kafka source connector"
        );

        // Build rdkafka consumer with rebalance-aware context.
        let rdkafka_config: ClientConfig = kafka_config.to_rdkafka_config();
        let context = LaminarConsumerContext::new(
            Arc::clone(&self.checkpoint_request),
            Arc::clone(&self.rebalance_state),
            Arc::clone(&self.rebalance_counter),
            Arc::clone(&self.revoke_generation),
            Arc::clone(&self.reader_paused),
            Arc::clone(&self.commit_retry_needed),
            Arc::clone(&self.offset_snapshot),
        );
        let consumer: StreamConsumer<LaminarConsumerContext> =
            rdkafka_config.create_with_context(context).map_err(|e| {
                ConnectorError::ConnectionFailed(format!("failed to create consumer: {e}"))
            })?;

        // Subscribe to topics (list or regex pattern).
        match &kafka_config.subscription {
            TopicSubscription::Topics(topics) => {
                let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
                consumer.subscribe(&topic_refs).map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("failed to subscribe: {e}"))
                })?;
            }
            TopicSubscription::Pattern(pattern) => {
                // rdkafka requires a ^ prefix for regex patterns
                let regex_pattern = if pattern.starts_with('^') {
                    pattern.clone()
                } else {
                    format!("^{pattern}")
                };
                consumer.subscribe(&[&regex_pattern]).map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("failed to subscribe to pattern: {e}"))
                })?;
            }
        }

        // Apply startup mode positioning before starting the reader.
        match &kafka_config.startup_mode {
            // GroupOffsets/Earliest/Latest are handled via auto.offset.reset in to_rdkafka_config().
            StartupMode::GroupOffsets | StartupMode::Earliest | StartupMode::Latest => {}
            StartupMode::SpecificOffsets(offsets) => {
                let mut tpl = rdkafka::TopicPartitionList::new();
                let topics = match &kafka_config.subscription {
                    TopicSubscription::Topics(t) => t.clone(),
                    TopicSubscription::Pattern(_) => Vec::new(),
                };
                for topic in &topics {
                    for (&partition, &offset) in offsets {
                        if let Err(e) = tpl.add_partition_offset(
                            topic,
                            partition,
                            rdkafka::Offset::Offset(offset),
                        ) {
                            tracing::warn!(
                                %topic, partition, offset,
                                error = %e,
                                "failed to add specific offset to partition list"
                            );
                        }
                    }
                }
                if tpl.count() > 0 {
                    consumer.assign(&tpl).map_err(|e| {
                        ConnectorError::ConnectionFailed(format!(
                            "failed to assign specific offsets: {e}"
                        ))
                    })?;
                    info!(
                        partition_count = tpl.count(),
                        "assigned consumer to specific offsets"
                    );
                }
            }
            StartupMode::Timestamp(ts_ms) => {
                // rdkafka requires assignment before offsets_for_times.
                // Wait briefly for partition assignment from the group coordinator,
                // then seek each assigned partition to the target timestamp.
                let mut tpl = rdkafka::TopicPartitionList::new();
                let topics = match &kafka_config.subscription {
                    TopicSubscription::Topics(t) => t.clone(),
                    TopicSubscription::Pattern(_) => Vec::new(),
                };
                // Query metadata to discover partition count per topic.
                if let Ok(metadata) = consumer.fetch_metadata(
                    topics.first().map(String::as_str),
                    std::time::Duration::from_secs(10),
                ) {
                    for topic_meta in metadata.topics() {
                        for partition_meta in topic_meta.partitions() {
                            if let Err(e) = tpl.add_partition_offset(
                                topic_meta.name(),
                                partition_meta.id(),
                                rdkafka::Offset::Offset(*ts_ms),
                            ) {
                                tracing::warn!(
                                    topic = topic_meta.name(),
                                    partition = partition_meta.id(),
                                    error = %e,
                                    "failed to add timestamp offset to partition list"
                                );
                            }
                        }
                    }
                }
                if tpl.count() > 0 {
                    match consumer.offsets_for_times(tpl, std::time::Duration::from_secs(10)) {
                        Ok(resolved) => {
                            consumer.assign(&resolved).map_err(|e| {
                                ConnectorError::ConnectionFailed(format!(
                                    "failed to assign timestamp offsets: {e}"
                                ))
                            })?;
                            info!(
                                timestamp_ms = ts_ms,
                                partition_count = resolved.count(),
                                "assigned consumer to timestamp offsets"
                            );
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                timestamp_ms = ts_ms,
                                "failed to resolve timestamp offsets, falling back to group offsets"
                            );
                        }
                    }
                }
            }
        }

        self.consumer = Some(consumer);
        self.state = ConnectorState::Running;

        // Reader is NOT started here — deferred to first `poll_batch()`.
        // This allows `restore()` to access the consumer directly between
        // `open()` and the first poll, calling `consumer.assign()` to seek
        // to checkpoint offsets. The pipeline's fallback poll interval
        // ensures the first `poll_batch()` fires promptly even without
        // a `data_ready_notify()` signal.

        // Eagerly fetch the SR schema so the Arrow schema is available at
        // plan time (before the first poll_batch).
        if let Some(ref sr) = self.schema_registry {
            if let TopicSubscription::Topics(topics) = &kafka_config.subscription {
                if topics.len() > 1 {
                    warn!("multiple topics with schema registry — using first topic's schema");
                }
                if let Some(topic) = topics.first() {
                    let subject = resolve_value_subject(
                        kafka_config.schema_registry_subject_strategy,
                        kafka_config.schema_registry_record_name.as_deref(),
                        topic,
                    );
                    match tokio::time::timeout(
                        kafka_config.schema_registry_discovery_timeout,
                        sr.get_latest_schema(&subject),
                    )
                    .await
                    {
                        Ok(Ok(cached)) => {
                            if let Some(avro_deser) = self
                                .deserializer
                                .as_any_mut()
                                .and_then(|any| any.downcast_mut::<AvroDeserializer>())
                            {
                                if let Err(e) =
                                    avro_deser.register_schema(cached.id, &cached.schema_str)
                                {
                                    warn!(%subject, error = %e, "SR schema register failed");
                                } else {
                                    // Keep the catalog schema pinned — planner
                                    // plans are already built against it.
                                    log_schema_drift(&self.schema, &cached.arrow_schema, &subject);
                                    info!(%subject, schema_id = cached.id,
                                        "SR schema fetched at open()");
                                    self.last_avro_schema = Some(cached.arrow_schema);
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            warn!(%subject, error = %e, "SR unavailable at open(), will resolve lazily");
                        }
                        Err(_elapsed) => {
                            warn!(%subject, "SR prefetch timed out at open(), will resolve lazily");
                        }
                    }
                }
            }
        }

        info!("Kafka source connector opened successfully");
        Ok(())
    }

    async fn discover_schema(&mut self, properties: &std::collections::HashMap<String, String>) {
        let cfg = crate::config::ConnectorConfig::with_properties("kafka", properties.clone());
        let Ok(kafka_config) = KafkaSourceConfig::from_config(&cfg) else {
            return;
        };
        if kafka_config.format != Format::Avro {
            return;
        }

        let topic = match &kafka_config.subscription {
            TopicSubscription::Topics(topics) => match topics.first() {
                Some(t) => {
                    if topics.len() > 1 {
                        warn!(topics = ?topics, chosen = %t,
                            "multi-topic source: using first topic's SR schema");
                    }
                    t.clone()
                }
                None => return,
            },
            TopicSubscription::Pattern(pattern) => {
                warn!(%pattern,
                    "topic.pattern cannot auto-discover a schema — declare columns explicitly");
                return;
            }
        };

        let sr_client = match Self::build_sr_client(&kafka_config) {
            Ok(Some(c)) => c,
            Ok(None) => return,
            Err(e) => {
                warn!(error = %e, "Schema Registry client build failed");
                return;
            }
        };

        let subject = resolve_value_subject(
            kafka_config.schema_registry_subject_strategy,
            kafka_config.schema_registry_record_name.as_deref(),
            &topic,
        );
        let timeout = kafka_config.schema_registry_discovery_timeout;

        match tokio::time::timeout(timeout, sr_client.get_latest_schema(&subject)).await {
            Ok(Ok(cached)) => {
                self.metrics.record_sr_discovery_success();
                info!(%subject, schema_id = cached.id,
                    fields = cached.arrow_schema.fields().len(),
                    "discovered Avro schema from Schema Registry");
                self.schema = cached.arrow_schema;
            }
            Ok(Err(e)) => {
                self.metrics.record_sr_discovery_failure();
                warn!(%subject, error = %e, "Schema Registry lookup failed");
            }
            Err(_) => {
                self.metrics.record_sr_discovery_timeout();
                warn!(%subject, timeout_secs = timeout.as_secs(),
                    "Schema Registry lookup timed out");
            }
        }
    }

    #[allow(clippy::cast_possible_truncation)] // Kafka partition/offset values fit in narrower types
    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        // Lazily spawn the background reader task on first poll.
        self.ensure_reader_started();

        let rx = self
            .msg_rx
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "reader initialized".into(),
                actual: "reader is None".into(),
            })?;

        let limit = max_records.min(self.config.max_poll_records);

        // Reuse struct-level buffers — clear without freeing capacity.
        self.poll_payload_buf.clear();
        self.poll_payload_offsets.clear();
        self.poll_meta_partitions.clear();
        self.poll_meta_offsets.clear();
        self.poll_meta_timestamps.clear();
        self.poll_meta_headers.clear();

        let mut total_bytes: u64 = 0;
        let mut last_topic = String::new();
        let mut last_partition_id: i32 = 0;
        let mut last_offset: i64 = -1;
        let include_metadata = self.config.include_metadata;
        let include_headers = self.config.include_headers;

        while self.poll_payload_offsets.len() < limit {
            match rx.try_recv() {
                Ok(kp) => {
                    self.channel_len.fetch_sub(1, Ordering::Release);
                    total_bytes += kp.data.len() as u64;
                    let start = self.poll_payload_buf.len();
                    self.poll_payload_buf.extend_from_slice(&kp.data);
                    self.poll_payload_offsets.push((start, kp.data.len()));

                    self.offsets.update_arc(&kp.topic, kp.partition, kp.offset);

                    if include_metadata {
                        self.poll_meta_partitions.push(kp.partition);
                        self.poll_meta_offsets.push(kp.offset);
                        self.poll_meta_timestamps.push(kp.timestamp_ms);
                    }
                    if include_headers {
                        self.poll_meta_headers.push(kp.headers_json);
                    }

                    // Update watermark tracker with event timestamp.
                    if let Some(ref mut tracker) = self.watermark_tracker {
                        if let Some(ts) = kp.timestamp_ms {
                            tracker.update_partition(kp.partition, ts);
                        }
                    }

                    if last_topic.as_str() != &*kp.topic || last_partition_id != kp.partition {
                        last_topic = kp.topic.to_string();
                        last_partition_id = kp.partition;
                    }
                    last_offset = kp.offset;
                }
                Err(crossfire::TryRecvError::Empty) => break,
                Err(crossfire::TryRecvError::Disconnected) => {
                    self.state = ConnectorState::Failed;
                    return Err(ConnectorError::Internal(
                        "Kafka reader task exited unexpectedly".into(),
                    ));
                }
            }
        }

        // Check for idle partitions on each poll cycle.
        if let Some(ref mut tracker) = self.watermark_tracker {
            tracker.check_idle_partitions();
        }

        // Sync rebalance counter → metrics (bridge from rdkafka background thread).
        let rebalance_events = self.rebalance_counter.swap(0, Ordering::Relaxed);
        for _ in 0..rebalance_events {
            self.metrics.record_rebalance();
        }

        // Lock-free revoke detection: check if a rebalance revoke happened
        // since the last poll cycle. If so, purge offsets for revoked partitions.
        let current_revoke_gen = self.revoke_generation.load(Ordering::Acquire);
        let had_revoke = current_revoke_gen != self.last_seen_revoke_gen;
        if had_revoke {
            self.last_seen_revoke_gen = current_revoke_gen;
            // Clone is intentional: only runs on rebalance events (rare), not per-poll.
            let assigned = lock_or_recover(&self.rebalance_state)
                .assigned_partitions()
                .clone();
            let before = self.offsets.partition_count();
            self.offsets.retain_assigned(&assigned);
            let after = self.offsets.partition_count();
            if before != after {
                debug!(
                    before,
                    after, "purged revoked partition offsets after rebalance"
                );
            }
        }

        // Publish current offsets to the reader task for periodic broker commits
        // and to the rebalance callback for seek-on-assign.
        // After retain_assigned, self.offsets only contains assigned partitions.
        if had_revoke || !self.poll_payload_offsets.is_empty() {
            if let Some(ref tx) = self.offset_commit_tx {
                let tpl = self.offsets.to_topic_partition_list();
                if tx.send(tpl).is_err() {
                    debug!("offset_commit_tx closed, reader task shutting down");
                }
            }
            lock_or_recover(&self.offset_snapshot).clone_from(&self.offsets);
        }

        // Compute consumer lag from high watermarks (moved from metrics()
        // to avoid side-effects in a &self getter).
        if let Some(ref hwm_rx) = self.high_watermarks_rx {
            let watermarks = hwm_rx.borrow();
            let mut total_lag: u64 = 0;
            for (topic, partition, high_watermark) in watermarks.iter() {
                if let Some(current_offset) = self.offsets.get(topic, *partition) {
                    let lag = high_watermark.saturating_sub(current_offset + 1);
                    #[allow(clippy::cast_sign_loss)]
                    if lag > 0 {
                        total_lag += lag as u64;
                    }
                }
            }
            self.metrics.set_lag(total_lag);
        }

        if self.poll_payload_offsets.is_empty() {
            return Ok(None);
        }

        // PartitionInfo reflects the last topic-partition seen in this batch.
        // Per-partition offsets are tracked correctly in `self.offsets` and
        // persisted via checkpoint(); this field is informational only.
        let last_partition = if last_offset >= 0 {
            Some(PartitionInfo::new(
                format!("{last_topic}-{last_partition_id}"),
                last_offset.to_string(),
            ))
        } else {
            None
        };

        // Resolve Avro schemas from Schema Registry before deserialization.
        // Also detect schema evolution when new schema IDs appear.
        if let Some(avro_deser) = self
            .deserializer
            .as_any_mut()
            .and_then(|any| any.downcast_mut::<AvroDeserializer>())
        {
            let mut new_schema_ids = Vec::new();
            for &(start, len) in &self.poll_payload_offsets {
                if let Some(schema_id) = AvroDeserializer::extract_confluent_id(
                    &self.poll_payload_buf[start..start + len],
                ) {
                    let is_new = avro_deser
                        .ensure_schema_registered(schema_id)
                        .await
                        .map_err(ConnectorError::Serde)?;
                    if is_new {
                        new_schema_ids.push(schema_id);
                    }
                }
            }

            // Detect schema evolution by diffing successive writer schemas.
            if !new_schema_ids.is_empty()
                && self.config.schema_evolution_strategy != SchemaEvolutionStrategy::Ignore
            {
                if let Some(ref sr) = self.schema_registry {
                    let compat = self
                        .config
                        .schema_compatibility
                        .map_or(CompatibilityMode::Backward, CompatibilityMode::from);
                    let evolver = SchemaEvolution::new(compat);

                    for id in new_schema_ids {
                        let cached = sr.resolve_confluent_id(id).await.map_err(|e| {
                            ConnectorError::SchemaMismatch(format!(
                                "failed to resolve schema {id}: {e}"
                            ))
                        })?;

                        let Some(ref prev) = self.last_avro_schema else {
                            // First schema — establish baseline, nothing to diff.
                            info!(schema_id = id, "initial Avro schema registered");
                            self.last_avro_schema = Some(Arc::clone(&cached.arrow_schema));
                            continue;
                        };

                        let changes = evolver.diff_schemas(prev, &cached.arrow_schema);
                        self.last_avro_schema = Some(Arc::clone(&cached.arrow_schema));

                        if changes.is_empty() {
                            info!(
                                schema_id = id,
                                "new Avro schema ID registered, no field changes"
                            );
                            continue;
                        }
                        let verdict = evolver.evaluate_evolution(&changes);
                        match &verdict {
                            EvolutionVerdict::Compatible => {
                                info!(schema_id = id, ?changes, "schema evolved (compatible)");
                            }
                            EvolutionVerdict::RequiresMigration => {
                                warn!(
                                    schema_id = id,
                                    ?changes,
                                    "schema evolved (requires migration)"
                                );
                            }
                            EvolutionVerdict::Incompatible(reason) => {
                                if self.config.schema_evolution_strategy
                                    == SchemaEvolutionStrategy::Reject
                                {
                                    return Err(ConnectorError::SchemaMismatch(format!(
                                        "incompatible schema evolution for ID {id}: {reason}"
                                    )));
                                }
                                warn!(
                                    schema_id = id, %reason, ?changes,
                                    "incompatible schema evolution detected"
                                );
                            }
                        }
                    }
                }
            }
        }

        let refs: Vec<&[u8]> = self
            .poll_payload_offsets
            .iter()
            .map(|&(start, len)| &self.poll_payload_buf[start..start + len])
            .collect();

        // Try batch deserialization first (fast path). If it fails, fall back
        // to per-record deserialization to isolate poison pills.
        let (batch, good_indices) = match self.deserializer.deserialize_batch(&refs, &self.schema) {
            Ok(batch) => (batch, None),
            Err(batch_err) => {
                // Per-record fallback: deserialize one at a time, collect
                // successful batches directly (avoids double-deserialization).
                // Track indices of successful records so metadata vectors can
                // be filtered to match the reduced row count.
                let mut good_batches = Vec::with_capacity(refs.len());
                let mut good_idx = Vec::with_capacity(refs.len());
                let mut error_count = 0u64;
                for (i, r) in refs.iter().enumerate() {
                    match self
                        .deserializer
                        .deserialize_batch(std::slice::from_ref(r), &self.schema)
                    {
                        Ok(batch) => {
                            good_batches.push(batch);
                            good_idx.push(i);
                        }
                        Err(e) => {
                            error_count += 1;
                            self.metrics.record_error();
                            warn!(error = %e, "skipping poison pill record");
                        }
                    }
                }
                if good_batches.is_empty() {
                    return Err(ConnectorError::Serde(batch_err));
                }
                // Escalate if the error rate exceeds the configured threshold.
                #[allow(clippy::cast_precision_loss)]
                if error_count > 0 {
                    let error_rate = error_count as f64 / refs.len() as f64;
                    if error_rate > self.config.max_deser_error_rate {
                        return Err(ConnectorError::Serde(batch_err));
                    }
                    warn!(
                        skipped = error_count,
                        total = refs.len(),
                        error_rate = %format_args!("{error_rate:.1}"),
                        "deserialized batch with poison pill isolation"
                    );
                }
                let concat_schema = good_batches[0].schema();
                let batch = arrow_select::concat::concat_batches(&concat_schema, &good_batches)
                    .map_err(|e| {
                        ConnectorError::Internal(format!("failed to concat batches: {e}"))
                    })?;
                (batch, Some(good_idx))
            }
        };

        // If poison pill fallback filtered records, also filter metadata
        // vectors so their lengths match the deserialized batch row count.
        if let Some(ref idx) = good_indices {
            if include_metadata {
                self.poll_meta_partitions =
                    idx.iter().map(|&i| self.poll_meta_partitions[i]).collect();
                self.poll_meta_offsets = idx.iter().map(|&i| self.poll_meta_offsets[i]).collect();
                self.poll_meta_timestamps =
                    idx.iter().map(|&i| self.poll_meta_timestamps[i]).collect();
            }
            if include_headers {
                self.poll_meta_headers = idx
                    .iter()
                    .map(|&i| std::mem::take(&mut self.poll_meta_headers[i]))
                    .collect();
            }
        }

        // Append metadata columns if configured.
        let needs_meta = include_metadata && !self.poll_meta_partitions.is_empty();
        let needs_headers = include_headers && !self.poll_meta_headers.is_empty();
        let batch = if needs_meta || needs_headers {
            use arrow_schema::{DataType, Field};

            let mut fields = batch.schema().fields().to_vec();
            let mut columns: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();

            if needs_meta {
                use arrow_array::{Int32Array, Int64Array, TimestampMillisecondArray};
                use arrow_schema::TimeUnit;
                fields.push(Arc::new(Field::new("_partition", DataType::Int32, false)));
                columns.push(Arc::new(Int32Array::from(std::mem::take(
                    &mut self.poll_meta_partitions,
                ))));
                fields.push(Arc::new(Field::new("_offset", DataType::Int64, false)));
                columns.push(Arc::new(Int64Array::from(std::mem::take(
                    &mut self.poll_meta_offsets,
                ))));
                fields.push(Arc::new(Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )));
                columns.push(Arc::new(TimestampMillisecondArray::from(std::mem::take(
                    &mut self.poll_meta_timestamps,
                ))));
            }
            if needs_headers {
                fields.push(Arc::new(Field::new("_headers", DataType::Utf8, true)));
                columns.push(Arc::new(arrow_array::StringArray::from(std::mem::take(
                    &mut self.poll_meta_headers,
                ))));
            }

            let meta_schema = Arc::new(arrow_schema::Schema::new(fields));
            arrow_array::RecordBatch::try_new(meta_schema, columns).map_err(|e| {
                ConnectorError::Internal(format!("failed to append metadata columns: {e}"))
            })?
        } else {
            batch
        };

        let num_rows = batch.num_rows();
        self.metrics.record_poll(num_rows as u64, total_bytes);

        let source_batch = if let Some(partition) = last_partition {
            SourceBatch::with_partition(batch, partition)
        } else {
            SourceBatch::new(batch)
        };

        debug!(
            records = num_rows,
            bytes = total_bytes,
            "polled batch from Kafka"
        );

        Ok(Some(source_batch))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let assigned = lock_or_recover(&self.rebalance_state)
            .assigned_partitions()
            .clone();

        // Push filtered offsets to the reader task so it can commit them
        // on shutdown even if close() is called without a subsequent checkpoint.
        if let Some(ref tx) = self.offset_commit_tx {
            let tpl = self.offsets.to_topic_partition_list_filtered(&assigned);
            let _ = tx.send(tpl);
        }

        self.offsets.to_checkpoint_filtered(&assigned)
    }

    /// Restores the consumer to checkpointed offsets.
    ///
    /// **Single-instance limitation**: This implementation assumes a single
    /// consumer instance per `group.id`. Checkpoint offsets are stored in
    /// `LaminarDB`'s manifest and restored via `consumer.assign()`, bypassing
    /// the Kafka consumer group protocol. Running multiple instances with
    /// the same `group.id` will cause offset conflicts between the manifest
    /// and broker-managed group offsets.
    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        info!(
            epoch = checkpoint.epoch(),
            "restoring Kafka source from checkpoint"
        );

        self.offsets = OffsetTracker::from_checkpoint(checkpoint);

        // Propagate restored offsets to the rebalance callback so a
        // rebalance before the first poll_batch() also seeks correctly.
        match self.offset_snapshot.lock() {
            Ok(mut snapshot) => snapshot.clone_from(&self.offsets),
            Err(poisoned) => poisoned.into_inner().clone_from(&self.offsets),
        }

        if let Some(ref consumer) = self.consumer {
            let tpl = self.offsets.to_topic_partition_list();
            consumer.assign(&tpl).map_err(|e| {
                ConnectorError::CheckpointError(format!("failed to seek to offsets: {e}"))
            })?;
            info!(
                partition_count = self.offsets.partition_count(),
                "restored consumer to checkpointed offsets"
            );
        }

        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => {
                if self.reader_paused.load(Ordering::Acquire) {
                    HealthStatus::Degraded("backpressure: consumption paused".into())
                } else {
                    HealthStatus::Healthy
                }
            }
            ConnectorState::Created | ConnectorState::Initializing => HealthStatus::Unknown,
            ConnectorState::Paused => HealthStatus::Degraded("connector paused".into()),
            ConnectorState::Recovering => HealthStatus::Degraded("recovering".into()),
            ConnectorState::Closed => HealthStatus::Unhealthy("closed".into()),
            ConnectorState::Failed => HealthStatus::Unhealthy("failed".into()),
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn checkpoint_requested(&self) -> Option<Arc<AtomicBool>> {
        Some(Arc::clone(&self.checkpoint_request))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Kafka source connector");

        let assigned = lock_or_recover(&self.rebalance_state)
            .assigned_partitions()
            .clone();

        // Send final filtered offsets to the reader task before signaling shutdown.
        if let Some(ref tx) = self.offset_commit_tx {
            let tpl = self.offsets.to_topic_partition_list_filtered(&assigned);
            if tpl.count() > 0 {
                let _ = tx.send(tpl);
            }
        }

        // Signal shutdown and wait for all background tasks to exit.
        if let Some(tx) = self.reader_shutdown.take() {
            let _ = tx.send(true);
        }
        let timeout = std::time::Duration::from_secs(5);
        if let Some(handle) = self.reader_handle.take() {
            let _ = tokio::time::timeout(timeout, handle).await;
        }
        if let Some(handle) = self.commit_handle.take() {
            let _ = tokio::time::timeout(timeout, handle).await;
        }
        if let Some(handle) = self.hwm_handle.take() {
            let _ = tokio::time::timeout(timeout, handle).await;
        }
        self.msg_rx = None;
        self.offset_commit_tx = None;

        // If consumer was never moved to the reader (e.g., close() called
        // before any poll_batch()), commit directly.
        if let Some(ref consumer) = self.consumer {
            let tpl = self.offsets.to_topic_partition_list_filtered(&assigned);
            if tpl.count() > 0 {
                if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
                    warn!(error = %e, "failed to commit final offsets");
                }
            }
            consumer.unsubscribe();
        }

        self.consumer = None;
        self.state = ConnectorState::Closed;
        info!("Kafka source connector closed");
        Ok(())
    }
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource")
            .field("state", &self.state)
            .field("subscription", &self.config.subscription)
            .field("group_id", &self.config.group_id)
            .field("format", &self.config.format)
            .field("partitions", &self.offsets.partition_count())
            .finish_non_exhaustive()
    }
}

/// Warn if the CREATE-SOURCE catalog schema has drifted from the live
/// Schema Registry schema. Empty `declared` means nothing was declared.
fn log_schema_drift(declared: &arrow_schema::Schema, live: &arrow_schema::Schema, subject: &str) {
    if declared.fields().is_empty() || declared.fields() == live.fields() {
        return;
    }
    let decl: BTreeSet<&str> = declared
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let lv: BTreeSet<&str> = live.fields().iter().map(|f| f.name().as_str()).collect();
    warn!(
        %subject,
        missing_in_sr = ?decl.difference(&lv).collect::<Vec<_>>(),
        added_in_sr = ?lv.difference(&decl).collect::<Vec<_>>(),
        "schema drift: re-apply CREATE SOURCE DDL to pick up the current SR schema"
    );
}

fn select_deserializer(format: Format) -> Box<dyn RecordDeserializer> {
    match format {
        Format::Avro => Box::new(AvroDeserializer::new()),
        other => serde::create_deserializer(other).unwrap_or_else(|_| {
            warn!(format = %other, "unsupported format, falling back to JSON");
            Box::new(serde::json::JsonDeserializer::new())
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn test_config() -> KafkaSourceConfig {
        let mut cfg = KafkaSourceConfig::default();
        cfg.bootstrap_servers = "localhost:9092".into();
        cfg.group_id = "test-group".into();
        cfg.subscription = TopicSubscription::Topics(vec!["events".into()]);
        cfg
    }

    #[test]
    fn test_new_defaults() {
        let source = KafkaSource::new(test_schema(), test_config());
        assert_eq!(source.state(), ConnectorState::Created);
        assert!(source.consumer.is_none());
        assert_eq!(source.offsets().partition_count(), 0);
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let source = KafkaSource::new(schema.clone(), test_config());
        assert_eq!(source.schema(), schema);
    }

    #[test]
    fn test_checkpoint_empty() {
        let source = KafkaSource::new(test_schema(), test_config());
        let cp = source.checkpoint();
        assert!(cp.is_empty());
    }

    #[test]
    fn test_checkpoint_with_offsets() {
        let mut source = KafkaSource::new(test_schema(), test_config());
        source.offsets.update("events", 0, 100);
        source.offsets.update("events", 1, 200);

        // Simulate rebalance assign so partitions are in the assigned set.
        {
            let mut state = source.rebalance_state.lock().unwrap();
            state.on_assign(&[("events".into(), 0), ("events".into(), 1)]);
        }

        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("events-0"), Some("100"));
        assert_eq!(cp.get_offset("events-1"), Some("200"));
    }

    #[test]
    fn test_health_check_created() {
        let source = KafkaSource::new(test_schema(), test_config());
        assert_eq!(source.health_check(), HealthStatus::Unknown);
    }

    #[test]
    fn test_health_check_running() {
        let mut source = KafkaSource::new(test_schema(), test_config());
        source.state = ConnectorState::Running;
        assert_eq!(source.health_check(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_closed() {
        let mut source = KafkaSource::new(test_schema(), test_config());
        source.state = ConnectorState::Closed;
        assert!(matches!(source.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_metrics_initial() {
        let source = KafkaSource::new(test_schema(), test_config());
        let m = source.metrics();
        assert_eq!(m.records_total, 0);
        assert_eq!(m.bytes_total, 0);
        assert_eq!(m.errors_total, 0);
    }

    #[test]
    fn test_deserializer_selection_json() {
        let source = KafkaSource::new(test_schema(), test_config());
        assert_eq!(source.deserializer.format(), Format::Json);
    }

    #[test]
    fn test_deserializer_selection_csv() {
        let mut cfg = test_config();
        cfg.format = Format::Csv;
        let source = KafkaSource::new(test_schema(), cfg);
        assert_eq!(source.deserializer.format(), Format::Csv);
    }

    #[test]
    fn test_with_schema_registry() {
        let sr = SchemaRegistryClient::new("http://localhost:8081", None);
        let mut cfg = test_config();
        cfg.format = Format::Avro;
        cfg.schema_registry_url = Some("http://localhost:8081".into());

        let source = KafkaSource::with_schema_registry(test_schema(), cfg, sr);
        assert!(source.schema_registry.is_some());
        assert_eq!(source.deserializer.format(), Format::Avro);
    }

    #[tokio::test]
    async fn test_open_preserves_injected_schema_registry() {
        let sr = SchemaRegistryClient::new("http://localhost:8081", None);
        let mut cfg = test_config();
        cfg.format = Format::Avro;
        cfg.schema_registry_url = Some("http://localhost:8081".into());
        let mut source = KafkaSource::with_schema_registry(test_schema(), cfg, sr);

        // open() with empty config should preserve injected SR.
        let empty_config = crate::config::ConnectorConfig::new("kafka");
        // open() will fail to connect (no broker), but the deserializer
        // re-selection happens before the connection attempt.
        let _ = source.open(&empty_config).await;
        assert!(source.schema_registry.is_some());
        assert_eq!(source.deserializer.format(), Format::Avro);
    }

    #[test]
    fn test_debug_output() {
        let source = KafkaSource::new(test_schema(), test_config());
        let debug = format!("{source:?}");
        assert!(debug.contains("KafkaSource"));
        assert!(debug.contains("events"));
    }

    #[test]
    fn test_checkpoint_filters_revoked_partitions() {
        let mut source = KafkaSource::new(test_schema(), test_config());
        source.offsets.update("events", 0, 100);
        source.offsets.update("events", 1, 200);
        source.offsets.update("events", 2, 300);

        // Simulate rebalance: only partitions 0 and 2 are assigned.
        {
            let mut state = source.rebalance_state.lock().unwrap();
            state.on_assign(&[("events".into(), 0), ("events".into(), 2)]);
        }

        let cp = source.checkpoint();
        assert_eq!(cp.get_offset("events-0"), Some("100"));
        assert_eq!(cp.get_offset("events-1"), None); // revoked — filtered out
        assert_eq!(cp.get_offset("events-2"), Some("300"));
    }

    #[test]
    fn test_checkpoint_empty_before_first_rebalance() {
        let mut source = KafkaSource::new(test_schema(), test_config());
        source.offsets.update("events", 0, 100);
        source.offsets.update("events", 1, 200);

        // No rebalance has occurred — assigned_partitions is empty.
        // No assigned partitions means no offsets should be checkpointed.
        let cp = source.checkpoint();
        assert!(cp.is_empty());
    }

    // discover_schema tests. Control-flow cases (when to skip, when to
    // fail) use plain config inputs; the happy path uses a wiremock HTTP
    // server mocking Confluent Schema Registry's REST API.

    fn empty_schema() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn props(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[tokio::test]
    async fn discover_schema_skips_non_avro_format() {
        let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default());
        source
            .discover_schema(&props(&[
                ("bootstrap.servers", "localhost:9092"),
                ("group.id", "g"),
                ("topic", "t"),
                ("format", "json"),
                ("schema.registry.url", "http://localhost:8081"),
            ]))
            .await;
        // Schema must remain untouched (still empty).
        assert_eq!(source.schema().fields().len(), 0);
    }

    #[tokio::test]
    async fn discover_schema_skips_without_sr_url() {
        let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default());
        source
            .discover_schema(&props(&[
                ("bootstrap.servers", "localhost:9092"),
                ("group.id", "g"),
                ("topic", "t"),
                ("format", "avro"),
            ]))
            .await;
        assert_eq!(source.schema().fields().len(), 0);
    }

    #[tokio::test]
    async fn discover_schema_skips_topic_pattern() {
        // topic.pattern cannot map to a single SR subject. Must skip
        // cleanly, not panic or select an arbitrary topic.
        let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default());
        source
            .discover_schema(&props(&[
                ("bootstrap.servers", "localhost:9092"),
                ("group.id", "g"),
                ("topic.pattern", "events-.*"),
                ("format", "avro"),
                ("schema.registry.url", "http://localhost:8081"),
            ]))
            .await;
        assert_eq!(source.schema().fields().len(), 0);
    }

    #[tokio::test]
    async fn discover_schema_unreachable_sr_leaves_schema_empty() {
        // Use a reserved-documentation IP on a closed port and bound the
        // whole test by a generous wall-clock budget so a bug where we
        // forgot the timeout would fail loudly.
        let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default());
        let start = std::time::Instant::now();
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            source.discover_schema(&props(&[
                ("bootstrap.servers", "localhost:9092"),
                ("group.id", "g"),
                ("topic", "t"),
                ("format", "avro"),
                // TEST-NET-1 per RFC 5737 — guaranteed not to route.
                ("schema.registry.url", "http://192.0.2.1:65535"),
            ])),
        )
        .await
        .expect("discover_schema must honor its own 10s timeout");
        assert!(
            start.elapsed() < std::time::Duration::from_secs(15),
            "discover_schema should have returned well before the outer 20s budget"
        );
        assert_eq!(source.schema().fields().len(), 0);
    }

    /// Happy path: wiremock SR returns a record-with-map Avro schema
    /// (the original "No Field name data" bug shape); `discover_schema`
    /// converts it correctly and preserves the Map type.
    #[tokio::test]
    async fn discover_schema_happy_path_with_wiremock_sr() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let avro_schema = serde_json::json!({
            "type": "record",
            "name": "event",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "data", "type": {"type": "map", "values": "string"}}
            ]
        })
        .to_string();

        let sr = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/ion_tw-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 42,
                "version": 1,
                "subject": "ion_tw-value",
                "schema": avro_schema,
                "schemaType": "AVRO",
            })))
            .mount(&sr)
            .await;

        let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default());
        source
            .discover_schema(&props(&[
                ("bootstrap.servers", "localhost:9092"),
                ("group.id", "g"),
                ("topic", "ion_tw"),
                ("format", "avro"),
                ("schema.registry.url", &sr.uri()),
            ]))
            .await;

        let schema = source.schema();
        assert_eq!(schema.fields().len(), 2, "expected [id, data]");
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "data");
        assert!(
            matches!(
                schema.field(1).data_type(),
                arrow_schema::DataType::Map(_, _)
            ),
            "'data' field must survive as a Map type (got {:?})",
            schema.field(1).data_type()
        );
    }

    /// Record-name subject strategy resolves to `{record_name}-value`
    /// rather than the default `{topic}-value`.
    #[tokio::test]
    async fn discover_schema_happy_path_record_name_strategy() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let avro_schema = serde_json::json!({
            "type": "record",
            "name": "com.acme.Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "double"}
            ]
        })
        .to_string();

        let sr = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/com.acme.Order-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 7,
                "version": 1,
                "subject": "com.acme.Order-value",
                "schema": avro_schema,
                "schemaType": "AVRO",
            })))
            .mount(&sr)
            .await;

        let mut source = KafkaSource::new(empty_schema(), KafkaSourceConfig::default());
        source
            .discover_schema(&props(&[
                ("bootstrap.servers", "localhost:9092"),
                ("group.id", "g"),
                ("topic", "orders"),
                ("format", "avro"),
                ("schema.registry.url", &sr.uri()),
                ("schema.registry.subject.name.strategy", "record-name"),
                ("schema.registry.record.name", "com.acme.Order"),
            ]))
            .await;

        let schema = source.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "order_id");
        assert_eq!(schema.field(1).name(), "amount");
    }

    /// Drift detection: catalog has a stale 2-field schema, live SR
    /// has evolved to 3 fields. Catalog stays pinned; only
    /// `last_avro_schema` tracks the live SR shape.
    #[tokio::test]
    async fn open_logs_drift_when_sr_evolved_since_ddl() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let evolved_schema = serde_json::json!({
            "type": "record",
            "name": "event",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "data", "type": {"type": "map", "values": "string"}},
                {"name": "version", "type": "int"}
            ]
        })
        .to_string();

        let sr = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/ion_tw-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 99,
                "version": 2,
                "subject": "ion_tw-value",
                "schema": evolved_schema,
                "schemaType": "AVRO",
            })))
            .mount(&sr)
            .await;

        // Catalog schema baked at CREATE SOURCE time — only two fields,
        // predates the `version` field that was just added in SR.
        let stale_catalog = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "data",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(arrow_schema::Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]));

        let mut cfg = KafkaSourceConfig::default();
        cfg.bootstrap_servers = "localhost:9092".into();
        cfg.group_id = "g".into();
        cfg.subscription = TopicSubscription::Topics(vec!["ion_tw".into()]);
        cfg.format = Format::Avro;
        cfg.schema_registry_url = Some(sr.uri());
        let sr_client = SchemaRegistryClient::new(sr.uri(), None);
        let mut source = KafkaSource::with_schema_registry(stale_catalog, cfg, sr_client);

        let empty_cfg = crate::config::ConnectorConfig::new("kafka");
        let _ = source.open(&empty_cfg).await; // broker unreachable — later errors irrelevant

        assert_eq!(
            source.schema().fields().len(),
            2,
            "catalog schema must stay pinned even after SR drift"
        );
        assert_eq!(
            source.last_avro_schema.as_ref().map(|s| s.fields().len()),
            Some(3),
            "last_avro_schema should reflect the evolved SR shape"
        );
    }
}
