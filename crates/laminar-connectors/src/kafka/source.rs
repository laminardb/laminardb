//! Kafka source connector implementation.
//!
//! [`KafkaSource`] implements the [`SourceConnector`] trait, consuming
//! from Kafka topics via rdkafka's `StreamConsumer`, deserializing
//! messages using pluggable formats, and producing Arrow `RecordBatch`
//! data through the connector SDK.

use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
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
use super::backpressure::BackpressureController;
use super::config::{KafkaSourceConfig, TopicSubscription};
use super::metrics::KafkaSourceMetrics;
use super::offsets::OffsetTracker;
use super::rebalance::RebalanceState;
use super::schema_registry::SchemaRegistryClient;

/// Payload sent from the background Kafka reader task to [`KafkaSource::poll_batch`].
struct KafkaPayload {
    data: Vec<u8>,
    topic: String,
    partition: i32,
    offset: i64,
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
    /// rdkafka consumer (set during `open()`).
    consumer: Option<StreamConsumer<LaminarConsumerContext>>,
    /// Parsed Kafka configuration.
    config: KafkaSourceConfig,
    /// Format-specific deserializer.
    deserializer: Box<dyn RecordDeserializer>,
    /// Per-partition offset tracking.
    offsets: OffsetTracker,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Consumption metrics.
    metrics: KafkaSourceMetrics,
    /// Arrow schema for output batches.
    schema: SchemaRef,
    /// Backpressure controller.
    backpressure: BackpressureController,
    /// Shared backpressure channel fill counter.
    ///
    /// Clone this Arc and update it from the downstream consumer to wire
    /// backpressure. Increment when batches are buffered, decrement when
    /// consumed. Without wiring, the counter stays at 0 and backpressure
    /// is effectively disabled (correct for sequential polling pipelines).
    channel_len: Arc<AtomicUsize>,
    /// Consumer group rebalance tracking.
    rebalance_state: RebalanceState,
    /// Optional Schema Registry client (shared with Avro deserializer).
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    /// Notification handle signalled when Kafka messages arrive from the reader task.
    data_ready: Arc<Notify>,
    /// Flag set to `true` on consumer group rebalance (partition revocation).
    checkpoint_request: Arc<AtomicBool>,
    /// Channel receiver for Kafka messages from the background reader task.
    msg_rx: Option<tokio::sync::mpsc::Receiver<KafkaPayload>>,
    /// Background Kafka reader task handle.
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    /// Shutdown signal for the background reader task.
    reader_shutdown: Option<tokio::sync::watch::Sender<bool>>,
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
        let backpressure = BackpressureController::new(
            config.backpressure_high_watermark,
            config.backpressure_low_watermark,
            config.max_poll_records * 10, // rough channel capacity estimate
            Arc::clone(&channel_len),
        );

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
            rebalance_state: RebalanceState::new(),
            schema_registry: None,
            data_ready: Arc::new(Notify::new()),
            checkpoint_request: Arc::new(AtomicBool::new(false)),
            msg_rx: None,
            reader_handle: None,
            reader_shutdown: None,
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
        let backpressure = BackpressureController::new(
            config.backpressure_high_watermark,
            config.backpressure_low_watermark,
            config.max_poll_records * 10,
            Arc::clone(&channel_len),
        );

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
            rebalance_state: RebalanceState::new(),
            schema_registry: Some(sr),
            data_ready: Arc::new(Notify::new()),
            checkpoint_request: Arc::new(AtomicBool::new(false)),
            msg_rx: None,
            reader_handle: None,
            reader_shutdown: None,
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns a reference to the offset tracker.
    #[must_use]
    pub fn offsets(&self) -> &OffsetTracker {
        &self.offsets
    }

    /// Returns the shared backpressure channel fill counter.
    ///
    /// The downstream consumer should clone this `Arc` and update the
    /// counter as batches are buffered and consumed:
    /// - Increment (`fetch_add`) when a batch is placed into a buffer
    /// - Decrement (`fetch_sub`) when a batch is processed
    ///
    /// The [`BackpressureController`] reads this counter to decide when
    /// to pause/resume the Kafka consumer. Without wiring, the counter
    /// stays at 0 and backpressure is disabled (correct for sequential
    /// polling pipelines that inherently throttle by processing rate).
    #[must_use]
    pub fn channel_len(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.channel_len)
    }

    /// Returns a reference to the rebalance state.
    #[must_use]
    pub fn rebalance_state(&self) -> &RebalanceState {
        &self.rebalance_state
    }

    /// Returns whether a Schema Registry client is configured.
    #[must_use]
    pub fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
    }

    /// Spawns the background reader task on first `poll_batch()` call.
    ///
    /// Deferred to allow `restore()` to access the consumer directly after `open()`.
    fn ensure_reader_started(&mut self) {
        if self.reader_handle.is_some() || self.consumer.is_none() {
            return;
        }

        let consumer = self.consumer.take().unwrap();
        let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(4096);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let data_ready = Arc::clone(&self.data_ready);
        let commit_interval = self.config.commit_interval;

        let reader_handle = tokio::spawn(async move {
            let mut reader_offsets = OffsetTracker::new();
            let mut last_commit = Instant::now();

            loop {
                let msg_result = tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => break,
                    msg = consumer.recv() => msg,
                };
                match msg_result {
                    Ok(msg) => {
                        if let Some(payload) = msg.payload() {
                            let topic = msg.topic();
                            let partition = msg.partition();
                            let offset = msg.offset();
                            reader_offsets.update(topic, partition, offset);

                            let kp = KafkaPayload {
                                data: payload.to_vec(),
                                topic: topic.to_string(),
                                partition,
                                offset,
                            };
                            if msg_tx.send(kp).await.is_err() {
                                break;
                            }
                            data_ready.notify_one();
                        }

                        if last_commit.elapsed() >= commit_interval
                            && reader_offsets.partition_count() > 0
                        {
                            let tpl = reader_offsets.to_topic_partition_list();
                            let _ = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async);
                            last_commit = Instant::now();
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Kafka consumer error");
                    }
                }
            }

            // Final sync commit.
            if reader_offsets.partition_count() > 0 {
                let tpl = reader_offsets.to_topic_partition_list();
                let _ = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync);
            }
            consumer.unsubscribe();
        });

        self.msg_rx = Some(msg_rx);
        self.reader_handle = Some(reader_handle);
        self.reader_shutdown = Some(shutdown_tx);
    }
}

#[async_trait]
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
        let context = LaminarConsumerContext::new(Arc::clone(&self.checkpoint_request));
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

        self.consumer = Some(consumer);
        self.state = ConnectorState::Running;
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

        // Check backpressure.
        if self.backpressure.should_pause() {
            self.backpressure.set_paused(true);
            debug!("backpressure: pausing consumption");
            return Ok(None);
        }
        if self.backpressure.should_resume() {
            self.backpressure.set_paused(false);
            debug!("backpressure: resuming consumption");
        }
        if self.backpressure.is_paused() {
            return Ok(None);
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

        // Drain messages from the background reader task.
        let mut payload_buf: Vec<u8> = Vec::with_capacity(limit * 256);
        let mut payload_offsets: Vec<(usize, usize)> = Vec::with_capacity(limit);
        let mut total_bytes: u64 = 0;
        let mut last_topic = String::new();
        let mut last_partition_id: i32 = 0;
        let mut last_offset: i64 = -1;

        while payload_offsets.len() < limit {
            match rx.try_recv() {
                Ok(kp) => {
                    total_bytes += kp.data.len() as u64;
                    let start = payload_buf.len();
                    payload_buf.extend_from_slice(&kp.data);
                    payload_offsets.push((start, kp.data.len()));

                    self.offsets.update(&kp.topic, kp.partition, kp.offset);

                    if last_topic.as_str() != kp.topic || last_partition_id != kp.partition {
                        last_topic = kp.topic;
                        last_partition_id = kp.partition;
                    }
                    last_offset = kp.offset;
                }
                Err(_) => break,
            }
        }

        if payload_offsets.is_empty() {
            return Ok(None);
        }

        let last_partition = if last_offset >= 0 {
            Some(PartitionInfo::new(
                format!("{last_topic}-{last_partition_id}"),
                last_offset.to_string(),
            ))
        } else {
            None
        };

        let refs: Vec<&[u8]> = payload_offsets
            .iter()
            .map(|&(start, len)| &payload_buf[start..start + len])
            .collect();
        let batch = self
            .deserializer
            .deserialize_batch(&refs, &self.schema)
            .map_err(ConnectorError::Serde)?;

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
        self.offsets.to_checkpoint()
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        info!(
            epoch = checkpoint.epoch(),
            "restoring Kafka source from checkpoint"
        );

        self.offsets = OffsetTracker::from_checkpoint(checkpoint);

        if let Some(ref consumer) = self.consumer {
            let tpl = self.offsets.to_topic_partition_list();
            consumer.assign(&tpl).map_err(|e| {
                ConnectorError::CheckpointError(format!("failed to seek to offsets: {e}"))
            })?;
            info!(
                partitions = self.offsets.partition_count(),
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

        // Shut down the reader task (it handles final commit and unsubscribe).
        if let Some(tx) = self.reader_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.reader_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        }
        self.msg_rx = None;

        // If consumer was never moved to the reader (no poll_batch called),
        // do cleanup directly.
        if let Some(ref consumer) = self.consumer {
            if self.offsets.partition_count() > 0 {
                let tpl = self.offsets.to_topic_partition_list();
                if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync) {
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

/// Selects the appropriate deserializer for the given format.
fn select_deserializer(format: Format) -> Box<dyn RecordDeserializer> {
    match format {
        Format::Avro => Box::new(AvroDeserializer::new()),
        other => serde::create_deserializer(other)
            .expect("supported format should always create a deserializer"),
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

    #[test]
    fn test_debug_output() {
        let source = KafkaSource::new(test_schema(), test_config());
        let debug = format!("{source:?}");
        assert!(debug.contains("KafkaSource"));
        assert!(debug.contains("events"));
    }
}
