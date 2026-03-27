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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use super::rebalance::LaminarConsumerContext;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{PartitionInfo, SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;
use crate::serde::{self, Format, RecordDeserializer};

use super::avro::AvroDeserializer;
use super::backpressure::KafkaBackpressureController;
use super::config::{KafkaSourceConfig, StartupMode, TopicSubscription};
use super::metrics::KafkaSourceMetrics;
use super::offsets::OffsetTracker;
use super::rebalance::RebalanceState;
use super::schema_registry::SchemaRegistryClient;
use super::watermarks::KafkaWatermarkTracker;

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
    backpressure: KafkaBackpressureController,
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
    msg_rx: Option<tokio::sync::mpsc::Receiver<KafkaPayload>>,
    reader_handle: Option<tokio::task::JoinHandle<()>>,
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
    ///
    /// # Arguments
    ///
    /// * `schema` - Arrow schema for output batches
    /// * `config` - Parsed Kafka source configuration
    #[must_use]
    pub fn new(schema: SchemaRef, config: KafkaSourceConfig) -> Self {
        let deserializer = select_deserializer(config.format);
        let channel_len = Arc::new(AtomicUsize::new(0));
        let backpressure = KafkaBackpressureController::new(
            config.backpressure_high_watermark,
            config.backpressure_low_watermark,
            config.reader_channel_capacity,
            Arc::clone(&channel_len),
        );

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
            backpressure,
            channel_len,
            rebalance_state: Arc::new(Mutex::new(RebalanceState::new())),
            rebalance_counter: Arc::new(AtomicU64::new(0)),
            revoke_generation: Arc::new(AtomicU64::new(0)),
            last_seen_revoke_gen: 0,
            schema_registry: None,
            data_ready: Arc::new(Notify::new()),
            checkpoint_request: Arc::new(AtomicBool::new(false)),
            msg_rx: None,
            reader_handle: None,
            reader_shutdown: None,
            offset_commit_tx: None,
            watermark_tracker,
            high_watermarks_rx: None,
            reader_paused: Arc::new(AtomicBool::new(false)),
            commit_retry_needed: Arc::new(AtomicBool::new(false)),
            offset_snapshot: Arc::new(Mutex::new(OffsetTracker::new())),
            poll_payload_buf: Vec::new(),
            poll_payload_offsets: Vec::new(),
            poll_meta_partitions: Vec::new(),
            poll_meta_offsets: Vec::new(),
            poll_meta_timestamps: Vec::new(),
            poll_meta_headers: Vec::new(),
        }
    }

    /// Creates a new Kafka source connector with Schema Registry.
    ///
    /// # Arguments
    ///
    /// * `schema` - Arrow schema for output batches
    /// * `config` - Parsed Kafka source configuration
    /// * `sr_client` - Schema Registry client
    #[must_use]
    pub fn with_schema_registry(
        schema: SchemaRef,
        config: KafkaSourceConfig,
        sr_client: SchemaRegistryClient,
    ) -> Self {
        let sr = Arc::new(sr_client);
        let deserializer: Box<dyn RecordDeserializer> = if config.format == Format::Avro {
            Box::new(AvroDeserializer::with_schema_registry(Arc::clone(&sr)))
        } else {
            select_deserializer(config.format)
        };

        let channel_len = Arc::new(AtomicUsize::new(0));
        let backpressure = KafkaBackpressureController::new(
            config.backpressure_high_watermark,
            config.backpressure_low_watermark,
            config.reader_channel_capacity,
            Arc::clone(&channel_len),
        );

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
            backpressure,
            channel_len,
            rebalance_state: Arc::new(Mutex::new(RebalanceState::new())),
            rebalance_counter: Arc::new(AtomicU64::new(0)),
            revoke_generation: Arc::new(AtomicU64::new(0)),
            last_seen_revoke_gen: 0,
            schema_registry: Some(sr),
            data_ready: Arc::new(Notify::new()),
            checkpoint_request: Arc::new(AtomicBool::new(false)),
            msg_rx: None,
            reader_handle: None,
            reader_shutdown: None,
            offset_commit_tx: None,
            watermark_tracker,
            high_watermarks_rx: None,
            reader_paused: Arc::new(AtomicBool::new(false)),
            commit_retry_needed: Arc::new(AtomicBool::new(false)),
            offset_snapshot: Arc::new(Mutex::new(OffsetTracker::new())),
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

    /// Spawns the background reader task on first `poll_batch()` call.
    ///
    /// Deferred to allow `restore()` to access the consumer directly after `open()`.
    #[allow(clippy::too_many_lines)]
    fn ensure_reader_started(&mut self) {
        if self.reader_handle.is_some() || self.consumer.is_none() {
            return;
        }

        // Wrap in Arc so the blocking watermark task can share the consumer
        // with the async reader loop. librdkafka handles are thread-safe.
        let consumer = Arc::new(self.consumer.take().unwrap());
        let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(self.config.reader_channel_capacity);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let (offset_tx, offset_rx) = tokio::sync::watch::channel(TopicPartitionList::new());
        let (hwm_tx, hwm_rx) = tokio::sync::watch::channel(Vec::new());
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

        // The reader task owns the consumer. On shutdown it commits the latest
        // offsets received via the offset_rx watch channel, then unsubscribes.
        let reader_handle = tokio::spawn(async move {
            let mut cached_topic: Arc<str> = Arc::from("");

            // Track seen topic-partitions for high watermark queries.
            let mut seen_partitions: std::collections::HashSet<(Arc<str>, i32)> =
                std::collections::HashSet::new();

            // Periodic broker commit timer. Advisory — keeps kafka-consumer-groups
            // lag monitoring accurate. Zero interval disables periodic commits.
            let commit_enabled = !broker_commit_interval.is_zero();
            let mut commit_timer = tokio::time::interval(if commit_enabled {
                broker_commit_interval
            } else {
                // tokio::time::interval panics on Duration::ZERO; use a
                // large interval that will never fire before shutdown.
                std::time::Duration::from_secs(86400)
            });
            // Skip the first tick (fires immediately).
            commit_timer.tick().await;

            // Periodic high watermark query timer (30s).
            let mut hwm_timer = tokio::time::interval(std::time::Duration::from_secs(30));
            hwm_timer.tick().await; // Skip the first tick.

            let mut is_paused = false;
            let mut last_revoke_gen: u64 = 0;

            loop {
                // Prune seen_partitions when a revoke event has occurred, so
                // the HWM timer stops querying watermarks for revoked partitions.
                // Reads rebalance_state (updated in pre_rebalance) rather than
                // consumer.assignment() which may lag the callback.
                let current_gen = revoke_generation.load(Ordering::Relaxed);
                if current_gen != last_revoke_gen {
                    last_revoke_gen = current_gen;
                    let assigned = match rebalance_state.lock() {
                        Ok(state) => state.assigned_partitions().clone(),
                        Err(poisoned) => poisoned.into_inner().assigned_partitions().clone(),
                    };
                    seen_partitions.retain(|(t, p)| assigned.contains(&(t.to_string(), *p)));
                }

                // Check channel fill ratio for pause/resume of Kafka partitions.
                // This keeps consumer.recv() calling rd_kafka_consumer_poll()
                // even when paused, preventing max.poll.interval.ms violations.
                #[allow(clippy::cast_precision_loss)]
                let fill = if reader_channel_capacity > 0 {
                    channel_len.load(Ordering::Relaxed) as f64 / reader_channel_capacity as f64
                } else {
                    0.0
                };
                if fill >= pause_threshold && !is_paused {
                    if let Ok(assignment) = consumer.assignment() {
                        if let Err(e) = consumer.pause(&assignment) {
                            warn!(error = %e, "failed to pause Kafka partitions");
                        } else {
                            is_paused = true;
                            reader_paused.store(true, Ordering::Relaxed);
                            debug!("reader: paused Kafka partitions (fill={fill:.2})");
                        }
                    }
                } else if fill <= resume_threshold && is_paused {
                    if let Ok(assignment) = consumer.assignment() {
                        if let Err(e) = consumer.resume(&assignment) {
                            warn!(error = %e, "failed to resume Kafka partitions");
                        } else {
                            is_paused = false;
                            reader_paused.store(false, Ordering::Relaxed);
                            debug!("reader: resumed Kafka partitions (fill={fill:.2})");
                        }
                    }
                }

                let msg_result = tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => break,
                    _ = commit_timer.tick(), if commit_enabled => {
                        let tpl = offset_rx.borrow().clone();
                        if tpl.count() > 0 {
                            if commit_retry_needed
                                .compare_exchange(
                                    true,
                                    false,
                                    Ordering::AcqRel,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                            {
                                // Escalate to sync commit on the blocking pool
                                // so we don't stall the reader or block shutdown.
                                let c = Arc::clone(&consumer);
                                let flag = Arc::clone(&commit_retry_needed);
                                let n = tpl.count();
                                tokio::task::spawn_blocking(move || {
                                    match c.commit(&tpl, CommitMode::Sync) {
                                        Ok(()) => {
                                            info!(
                                                partition_count = n,
                                                "sync offset commit retry succeeded"
                                            );
                                        }
                                        Err(e) => {
                                            flag.store(true, Ordering::Release);
                                            warn!(
                                                error = %e,
                                                "sync offset commit retry failed"
                                            );
                                        }
                                    }
                                });
                            } else {
                                match consumer.commit(&tpl, CommitMode::Async) {
                                    Ok(()) => info!(
                                        partition_count = tpl.count(),
                                        "periodic broker offset commit (advisory)"
                                    ),
                                    Err(e) => warn!(
                                        error = %e,
                                        "periodic broker offset commit failed"
                                    ),
                                }
                            }
                        }
                        continue;
                    },
                    _ = hwm_timer.tick() => {
                        // fetch_watermarks is a blocking FFI call — run on the
                        // blocking pool to avoid stalling the async reader.
                        if !seen_partitions.is_empty() {
                            let partitions: Vec<_> = seen_partitions.iter().cloned().collect();
                            let c = Arc::clone(&consumer);
                            let watermarks = tokio::task::spawn_blocking(move || {
                                let mut results = Vec::with_capacity(partitions.len());
                                for (topic, partition) in &partitions {
                                    if let Ok((_low, high)) = c.fetch_watermarks(
                                        topic,
                                        *partition,
                                        std::time::Duration::from_secs(5),
                                    ) {
                                        results.push((Arc::clone(topic), *partition, high));
                                    }
                                }
                                results
                            })
                            .await
                            .unwrap_or_default();
                            if !watermarks.is_empty() {
                                let _ = hwm_tx.send(watermarks);
                            }
                        }
                        continue;
                    },
                    // Timeout ensures we re-check backpressure and timers even
                    // when paused partitions yield no messages.
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
                            // Track partition for watermark queries.
                            seen_partitions.insert((Arc::clone(&cached_topic), msg.partition()));
                            let timestamp_ms = match msg.timestamp() {
                                rdkafka::Timestamp::CreateTime(ts)
                                | rdkafka::Timestamp::LogAppendTime(ts) => Some(ts),
                                rdkafka::Timestamp::NotAvailable => None,
                            };
                            let headers_json = if capture_headers {
                                use rdkafka::message::Headers;
                                msg.headers().and_then(|hdrs| {
                                    let mut map = serde_json::Map::with_capacity(hdrs.count());
                                    for i in 0..hdrs.count() {
                                        let h = hdrs.get(i);
                                        let val = h
                                            .value
                                            .map(|v| String::from_utf8_lossy(v).into_owned())
                                            .unwrap_or_default();
                                        map.insert(
                                            h.key.to_string(),
                                            serde_json::Value::String(val),
                                        );
                                    }
                                    serde_json::to_string(&map).ok()
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
                                Err(tokio::sync::mpsc::error::TrySendError::Full(kp)) => {
                                    // Pause partitions BEFORE blocking so rdkafka
                                    // stops prefetching while we wait for drain.
                                    if !is_paused {
                                        if let Ok(assignment) = consumer.assignment() {
                                            if consumer.pause(&assignment).is_ok() {
                                                is_paused = true;
                                                reader_paused.store(true, Ordering::Relaxed);
                                                debug!("reader: paused partitions (channel full)");
                                            }
                                        }
                                    }
                                    channel_len.fetch_add(1, Ordering::Relaxed);
                                    if msg_tx.send(kp).await.is_err() {
                                        channel_len.fetch_sub(1, Ordering::Relaxed);
                                        break;
                                    }
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                            }
                            data_ready.notify_one();
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Kafka consumer error");
                    }
                }
            }

            // Commit final offsets before unsubscribing. The close() method
            // sends the latest TPL via offset_tx before signaling shutdown,
            // so offset_rx.borrow() contains the most recent offsets.
            let tpl = offset_rx.borrow().clone();
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
        if let Some(ref sr_url) = kafka_config.schema_registry_url {
            let sr_client = if let Some(ref ca) = kafka_config.schema_registry_ssl_ca_location {
                SchemaRegistryClient::with_tls_mtls(
                    sr_url.clone(),
                    kafka_config.schema_registry_auth.clone(),
                    ca,
                    kafka_config
                        .schema_registry_ssl_certificate_location
                        .as_deref(),
                    kafka_config.schema_registry_ssl_key_location.as_deref(),
                )?
            } else {
                SchemaRegistryClient::new(sr_url.clone(), kafka_config.schema_registry_auth.clone())
            };
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

        info!("Kafka source connector opened successfully");
        Ok(())
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

        // Update backpressure state — never skip the drain below (deadlocks).
        if self.backpressure.should_pause() {
            self.backpressure.set_paused(true);
            debug!("backpressure: pausing consumption");
        } else if self.backpressure.should_resume() {
            self.backpressure.set_paused(false);
            debug!("backpressure: resuming consumption");
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
                    self.channel_len.fetch_sub(1, Ordering::Relaxed);
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
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
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
        let current_revoke_gen = self.revoke_generation.load(Ordering::Relaxed);
        let had_revoke = current_revoke_gen != self.last_seen_revoke_gen;
        if had_revoke {
            self.last_seen_revoke_gen = current_revoke_gen;
            let assigned = match self.rebalance_state.lock() {
                Ok(state) => state.assigned_partitions().clone(),
                Err(poisoned) => poisoned.into_inner().assigned_partitions().clone(),
            };
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
            match self.offset_snapshot.lock() {
                Ok(mut snapshot) => snapshot.clone_from(&self.offsets),
                Err(poisoned) => poisoned.into_inner().clone_from(&self.offsets),
            }
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
        if let Some(avro_deser) = self
            .deserializer
            .as_any_mut()
            .and_then(|any| any.downcast_mut::<AvroDeserializer>())
        {
            for &(start, len) in &self.poll_payload_offsets {
                if let Some(schema_id) = AvroDeserializer::extract_confluent_id(
                    &self.poll_payload_buf[start..start + len],
                ) {
                    avro_deser
                        .ensure_schema_registered(schema_id)
                        .await
                        .map_err(ConnectorError::Serde)?;
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
        let batch = match self.deserializer.deserialize_batch(&refs, &self.schema) {
            Ok(batch) => batch,
            Err(batch_err) => {
                // Per-record fallback: deserialize one at a time, skip failures.
                let mut good_refs = Vec::with_capacity(refs.len());
                let mut error_count = 0u64;
                for r in &refs {
                    match self
                        .deserializer
                        .deserialize_batch(std::slice::from_ref(r), &self.schema)
                    {
                        Ok(_) => good_refs.push(*r),
                        Err(e) => {
                            error_count += 1;
                            self.metrics.record_error();
                            warn!(error = %e, "skipping poison pill record");
                        }
                    }
                }
                if good_refs.is_empty() {
                    // All records failed — propagate the original batch error.
                    return Err(ConnectorError::Serde(batch_err));
                }
                if error_count > 0 {
                    warn!(
                        skipped = error_count,
                        total = refs.len(),
                        "deserialized batch with poison pill isolation"
                    );
                }
                self.deserializer
                    .deserialize_batch(&good_refs, &self.schema)
                    .map_err(ConnectorError::Serde)?
            }
        };

        // Append metadata columns if configured.
        let needs_meta = include_metadata && !self.poll_meta_partitions.is_empty();
        let needs_headers = include_headers && !self.poll_meta_headers.is_empty();
        let batch = if needs_meta || needs_headers {
            use arrow_schema::{DataType, Field};

            let mut fields = batch.schema().fields().to_vec();
            let mut columns: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();

            if needs_meta {
                use arrow_array::{Int32Array, Int64Array};
                fields.push(Arc::new(Field::new("_partition", DataType::Int32, false)));
                columns.push(Arc::new(Int32Array::from(std::mem::take(
                    &mut self.poll_meta_partitions,
                ))));
                fields.push(Arc::new(Field::new("_offset", DataType::Int64, false)));
                columns.push(Arc::new(Int64Array::from(std::mem::take(
                    &mut self.poll_meta_offsets,
                ))));
                fields.push(Arc::new(Field::new("_timestamp", DataType::Int64, true)));
                columns.push(Arc::new(Int64Array::from(std::mem::take(
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
        let assigned = match self.rebalance_state.lock() {
            Ok(state) => state.assigned_partitions().clone(),
            Err(poisoned) => poisoned.into_inner().assigned_partitions().clone(),
        };

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
                if self.backpressure.is_paused() {
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
        // Compute consumer lag from high watermarks, filtered to currently
        // assigned partitions so revoked partitions don't contribute stale lag.
        if let Some(ref hwm_rx) = self.high_watermarks_rx {
            let watermarks = hwm_rx.borrow();
            let mut total_lag: u64 = 0;
            for (topic, partition, high_watermark) in watermarks.iter() {
                // Only count partitions we still track offsets for (assigned).
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

        let assigned = match self.rebalance_state.lock() {
            Ok(state) => state.assigned_partitions().clone(),
            Err(poisoned) => poisoned.into_inner().assigned_partitions().clone(),
        };

        // Send final filtered offsets to the reader task before signaling shutdown.
        if let Some(ref tx) = self.offset_commit_tx {
            let tpl = self.offsets.to_topic_partition_list_filtered(&assigned);
            if tpl.count() > 0 {
                let _ = tx.send(tpl);
            }
        }

        // Signal shutdown and wait for the reader task to commit and exit.
        if let Some(tx) = self.reader_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.reader_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
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
}
