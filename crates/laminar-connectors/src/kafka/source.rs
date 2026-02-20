//! Kafka source connector implementation.
//!
//! [`KafkaSource`] implements the [`SourceConnector`] trait, consuming
//! from Kafka topics via rdkafka's `StreamConsumer`, deserializing
//! messages using pluggable formats, and producing Arrow `RecordBatch`
//! data through the connector SDK.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use tracing::{debug, info, warn};

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
    consumer: Option<StreamConsumer>,
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
    /// Last time offsets were committed to Kafka.
    last_commit_time: Instant,
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
            last_commit_time: Instant::now(),
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
            last_commit_time: Instant::now(),
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

    /// Commits offsets to Kafka if the commit interval has elapsed.
    fn maybe_commit_offsets(&mut self) -> Result<(), ConnectorError> {
        if self.last_commit_time.elapsed() < self.config.commit_interval {
            return Ok(());
        }

        if let Some(ref consumer) = self.consumer {
            if self.offsets.partition_count() > 0 {
                let tpl = self.offsets.to_topic_partition_list();
                consumer
                    .commit(&tpl, rdkafka::consumer::CommitMode::Async)
                    .map_err(|e| {
                        ConnectorError::CheckpointError(format!("offset commit failed: {e}"))
                    })?;
                self.metrics.record_commit();
                debug!(
                    partitions = self.offsets.partition_count(),
                    "committed offsets to Kafka"
                );
            }
        }

        self.last_commit_time = Instant::now();
        Ok(())
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
        if let Some(schema_str) = config.get("_arrow_schema") {
            if let Some(schema) = parse_arrow_schema(schema_str) {
                info!(
                    fields = schema.fields().len(),
                    "using SQL-defined schema for deserialization"
                );
                self.schema = Arc::new(schema);
            }
        }

        info!(
            brokers = %kafka_config.bootstrap_servers,
            subscription = ?kafka_config.subscription,
            group_id = %kafka_config.group_id,
            format = %kafka_config.format,
            schema_fields = self.schema.fields().len(),
            "opening Kafka source connector"
        );

        // Build rdkafka consumer.
        let rdkafka_config: ClientConfig = kafka_config.to_rdkafka_config();
        let consumer: StreamConsumer = rdkafka_config.create().map_err(|e| {
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

        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "consumer initialized".into(),
                actual: "consumer is None".into(),
            })?;

        let limit = max_records.min(self.config.max_poll_records);
        let timeout = self.config.poll_timeout;

        // Collect raw messages.
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(limit);
        let mut total_bytes: u64 = 0;
        let mut last_partition: Option<PartitionInfo> = None;

        let poll_start = Instant::now();
        while payloads.len() < limit && poll_start.elapsed() < timeout {
            let remaining = timeout.saturating_sub(poll_start.elapsed());
            match tokio::time::timeout(remaining, consumer.recv()).await {
                Ok(Ok(msg)) => {
                    if let Some(payload) = msg.payload() {
                        total_bytes += payload.len() as u64;
                        payloads.push(payload.to_vec());

                        let topic = msg.topic();
                        let partition = msg.partition();
                        let offset = msg.offset();

                        self.offsets.update(topic, partition, offset);
                        last_partition = Some(PartitionInfo::new(
                            format!("{topic}-{partition}"),
                            offset.to_string(),
                        ));
                    }
                }
                Ok(Err(e)) => {
                    self.metrics.record_error();
                    warn!(error = %e, "Kafka consumer error");
                    // Break on error but return what we have so far.
                    break;
                }
                Err(_) => {
                    // Timeout — no more messages available.
                    break;
                }
            }
        }

        // Periodically commit offsets.
        self.maybe_commit_offsets()?;

        if payloads.is_empty() {
            return Ok(None);
        }

        // Deserialize messages into RecordBatch.
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
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

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Kafka source connector");

        // Commit final offsets.
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

/// Parse an Arrow schema from the compact `name:type,name:type,...` encoding
/// produced by `encode_arrow_schema` in laminar-db.
fn parse_arrow_schema(s: &str) -> Option<arrow_schema::Schema> {
    use arrow_schema::{DataType, Field};

    let fields: Vec<Field> = s
        .split(',')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (name, type_str) = part.split_once(':')?;
            let dt = match type_str {
                "Utf8" => DataType::Utf8,
                "LargeUtf8" => DataType::LargeUtf8,
                "Float64" => DataType::Float64,
                "Float32" => DataType::Float32,
                "Int64" => DataType::Int64,
                "Int32" => DataType::Int32,
                "Int16" => DataType::Int16,
                "Int8" => DataType::Int8,
                "UInt64" => DataType::UInt64,
                "UInt32" => DataType::UInt32,
                "Boolean" => DataType::Boolean,
                "Date32" => DataType::Date32,
                "Date64" => DataType::Date64,
                _ => return None,
            };
            Some(Field::new(name, dt, true))
        })
        .collect();

    if fields.is_empty() {
        None
    } else {
        Some(arrow_schema::Schema::new(fields))
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
