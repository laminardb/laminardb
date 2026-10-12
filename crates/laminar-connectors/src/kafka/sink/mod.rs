//! Kafka sink connector.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use tracing::{debug, info, warn};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, CoordinatedCommitter, DeliveryGuarantee,
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::serde::RecordSerializer;

use super::metadata_error::{fetch_error, invalid_response, topic_error};
use super::partitioner::{
    KafkaPartitioner, KeyHashPartitioner, RoundRobinPartitioner, StickyPartitioner,
};
use super::schema_registry::SchemaRegistryClient;
use super::sink_config::{KafkaSinkConfig, PartitionStrategy, SinkEnvelope};
use super::sink_metrics::KafkaSinkMetrics;

mod commit_marker;
mod exact_commit;
mod failure;
mod keys;
mod lifecycle;
mod serialization;
mod staged_epoch;
mod upsert;

use exact_commit::KafkaExactCommitter;
use failure::{
    kafka_write_timeout, producer_creation_error, queue_retry_delay, record_failure,
    unresolved_delivery_error, validate_payload_cardinality, KafkaFailure, KafkaFailureCertainty,
    KafkaFailureScope, QUEUE_RETRY_TIMEOUT,
};
use keys::{KeyBuffer, KeyEncoder};
use serialization::select_serializer;
use staged_epoch::StagedEpoch;

fn required_producer(producer: Option<&FutureProducer>) -> Result<&FutureProducer, ConnectorError> {
    producer.ok_or_else(|| ConnectorError::InvalidState {
        expected: "producer initialized".into(),
        actual: "producer is None".into(),
    })
}

/// Kafka sink connector that writes Arrow `RecordBatch` data to Kafka topics.
///
/// Operates in Ring 1 (background) receiving data from Ring 0 via the
/// subscription API.
///
/// # Lifecycle
///
/// 1. Create with [`KafkaSink::new`]
/// 2. Call `open()` to create the producer and connect to Kafka
/// 3. `write_batch()` serializes and produces records; checkpoint flushes provide
///    durable at-least-once delivery
/// 4. Call `close()` for clean shutdown
///
/// This connector deliberately rejects exactly-once admission because it does
/// not expose a coordinated external checkpoint namespace/cursor.
pub struct KafkaSink {
    /// rdkafka producer (set during `open()`).
    producer: Option<FutureProducer>,
    /// Parsed Kafka sink configuration.
    config: KafkaSinkConfig,
    /// Format-specific serializer.
    serializer: Box<dyn RecordSerializer>,
    /// Partitioner for determining target partitions.
    partitioner: Box<dyn KafkaPartitioner>,
    /// Connector lifecycle state.
    state: ConnectorState,
    /// Dead letter queue producer (separate, non-transactional).
    dlq_producer: Option<FutureProducer>,
    /// Production metrics.
    metrics: KafkaSinkMetrics,
    /// Arrow schema for input batches.
    schema: SchemaRef,
    writer_schema: Option<SchemaRef>,
    writer_projection: Vec<usize>,
    /// Optional Schema Registry client.
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    /// Shared Avro schema ID (updated after SR registration).
    avro_schema_id: Arc<std::sync::atomic::AtomicU32>,
    /// Partition count keyed records hash over: `partition.count` when pinned, otherwise the
    /// broker count observed at open.
    topic_partition_count: Option<i32>,
    /// Key columns bound to the input schema at open.
    key_encoder: Option<KeyEncoder>,
    /// Serializer for `key.format = 'json'`.
    json_keys: crate::serde::json::JsonSerializer,
    /// Partition for records without a key in the current batch; advances once per batch.
    unkeyed_partition: i32,
    /// Exactly-once only: the open epoch's routed records, published by the committer after the
    /// checkpoint commits.
    staged: Option<StagedEpoch>,
    /// Exactly-once only: the designated committer for this sink's namespace.
    exact: Option<Arc<KafkaExactCommitter>>,
    /// Sole admission authority for detached producer destruction.
    task_owner: ConnectorTaskOwner,
    /// Cloneable terminal observer returned to the connector runtime.
    task_tracker: ConnectorTaskTracker,
}

impl KafkaSink {
    /// Creates a new Kafka sink connector with explicit schema.
    ///
    /// # Panics
    ///
    /// Panics if `config.format` is not a supported serialization format.
    /// Call [`KafkaSinkConfig::validate`] first to catch this at config time.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        config: KafkaSinkConfig,
        registry: Option<&prometheus::Registry>,
    ) -> Self {
        let avro_schema_id = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let serializer =
            select_serializer(config.format, &schema, Arc::clone(&avro_schema_id), None)
                .expect("format validated in KafkaSinkConfig::validate()");
        let partitioner = select_partitioner(config.partitioner);
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        let (staged, exact) = exactly_once_parts(&config, &task_owner);

        Self {
            producer: None,
            config,
            serializer,
            partitioner,
            state: ConnectorState::Created,
            dlq_producer: None,
            metrics: KafkaSinkMetrics::new(registry),
            schema,
            writer_schema: None,
            writer_projection: Vec::new(),
            schema_registry: None,
            avro_schema_id,
            topic_partition_count: None,
            key_encoder: None,
            json_keys: crate::serde::json::JsonSerializer::new(),
            unkeyed_partition: 0,
            staged,
            exact,
            task_owner,
            task_tracker,
        }
    }

    /// Creates a new Kafka sink with Schema Registry integration.
    ///
    /// # Panics
    ///
    /// Panics if `config.format` is not a supported serialization format.
    /// Call [`KafkaSinkConfig::validate`] first to catch this at config time.
    #[must_use]
    pub fn with_schema_registry(
        schema: SchemaRef,
        config: KafkaSinkConfig,
        sr_client: SchemaRegistryClient,
    ) -> Self {
        let sr = Arc::new(sr_client);
        let avro_schema_id = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let serializer = select_serializer(
            config.format,
            &schema,
            Arc::clone(&avro_schema_id),
            Some(Arc::clone(&sr)),
        )
        .expect("format validated in KafkaSinkConfig::validate()");
        let partitioner = select_partitioner(config.partitioner);
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        let (staged, exact) = exactly_once_parts(&config, &task_owner);

        Self {
            producer: None,
            config,
            serializer,
            partitioner,
            state: ConnectorState::Created,
            dlq_producer: None,
            metrics: KafkaSinkMetrics::new(None),
            schema,
            writer_schema: None,
            writer_projection: Vec::new(),
            schema_registry: Some(sr),
            avro_schema_id,
            topic_partition_count: None,
            key_encoder: None,
            json_keys: crate::serde::json::JsonSerializer::new(),
            unkeyed_partition: 0,
            staged,
            exact,
            task_owner,
            task_tracker,
        }
    }

    /// Lifecycle state (Created → Running → Closed).
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Whether Avro schema registration is available.
    #[must_use]
    pub fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
    }

    fn serialize_payloads(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Vec<Vec<u8>>, ConnectorError> {
        let projected;
        let batch = if let Some(writer) = &self.writer_schema {
            let columns = self
                .writer_projection
                .iter()
                .map(|index| Arc::clone(batch.column(*index)))
                .collect();
            projected = arrow_array::RecordBatch::try_new(Arc::clone(writer), columns).map_err(
                |error| ConnectorError::SchemaMismatch(format!("writer projection: {error}")),
            )?;
            &projected
        } else {
            batch
        };
        let payloads = self.serializer.serialize(batch).map_err(|error| {
            self.metrics.record_serialization_error();
            ConnectorError::Serde(error)
        })?;
        validate_payload_cardinality(batch.num_rows(), payloads.len())?;
        Ok(payloads)
    }

    /// Bind the configured key columns to the committed input schema.
    fn bind_keys(&mut self) -> Result<(), ConnectorError> {
        let columns = self.config.key_columns();
        self.key_encoder = if columns.is_empty() {
            None
        } else {
            Some(KeyEncoder::bind(
                &self.schema,
                &columns,
                self.config.key_format,
            )?)
        };
        Ok(())
    }

    fn required_partition_count(&self) -> Result<i32, ConnectorError> {
        self.topic_partition_count
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "broker topic metadata installed".into(),
                actual: "partition count is unavailable".into(),
            })
    }

    /// Encoded keys for every row, or `None` when no key column is configured.
    fn extract_keys(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Option<KeyBuffer>, ConnectorError> {
        self.key_encoder
            .as_ref()
            .map(|encoder| encoder.encode(batch, &self.json_keys))
            .transpose()
    }

    /// Route one record: the configured strategy, falling back to the batch's rotating partition
    /// for a record without a key under key-hash routing.
    fn route(&mut self, key: Option<&[u8]>, partition_count: i32) -> i32 {
        self.partitioner
            .partition(key, partition_count)
            .unwrap_or(self.unkeyed_partition)
    }

    /// Advance the partition used by unkeyed records, once per written batch.
    fn rotate_unkeyed_partition(&mut self, partition_count: i32) {
        self.unkeyed_partition = (self.unkeyed_partition + 1) % partition_count;
    }

    /// Each row's encoded key and target partition; the batch's unkeyed rows share one partition.
    fn route_batch(
        &mut self,
        batch: &arrow_array::RecordBatch,
        rows: usize,
    ) -> Result<(Option<KeyBuffer>, Vec<i32>), ConnectorError> {
        let keys = self.extract_keys(batch)?;
        let partition_count = self.required_partition_count()?;
        let partitions = (0..rows)
            .map(|row| {
                self.route(
                    keys.as_ref().and_then(|keys| keys.key(row)),
                    partition_count,
                )
            })
            .collect();
        self.rotate_unkeyed_partition(partition_count);
        Ok((keys, partitions))
    }

    /// Enqueues a definitely rejected record to the dead letter queue. The
    /// caller retains and drains the returned delivery future with all other
    /// accepted main/DLQ records from the write.
    async fn enqueue_dlq(
        &self,
        payload: &[u8],
        key: Option<&[u8]>,
        error_msg: &str,
        queue_deadline: Instant,
    ) -> Result<DeliveryFuture, KafkaFailure> {
        let dlq_producer = self.dlq_producer.as_ref().ok_or_else(|| KafkaFailure {
            certainty: KafkaFailureCertainty::DefinitelyNotPersisted,
            scope: KafkaFailureScope::Connector,
            detail: "DLQ topic or producer is not configured".into(),
            retryable: false,
        })?;
        let dlq_topic = self.config.dlq_topic.as_ref().ok_or_else(|| KafkaFailure {
            certainty: KafkaFailureCertainty::DefinitelyNotPersisted,
            scope: KafkaFailureScope::Connector,
            detail: "DLQ topic or producer is not configured".into(),
            retryable: false,
        })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| {
                tracing::warn!("system clock before Unix epoch — using 0 for DLQ timestamp");
                std::time::Duration::ZERO
            })
            .as_millis()
            .to_string();
        let headers = OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "__dlq.error",
                value: Some(error_msg.as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "__dlq.topic",
                value: Some(self.config.topic.as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "__dlq.timestamp",
                value: Some(now.as_bytes()),
            });

        let mut record = FutureRecord::to(dlq_topic)
            .payload(payload)
            .headers(headers);

        if let Some(k) = key {
            record = record.key(k);
        }

        Self::enqueue_with_queue_retry(dlq_producer, record, queue_deadline)
            .await
            .map_err(|error| KafkaFailure::enqueue(&error, "Kafka DLQ"))
    }

    /// Synchronously enqueue a record with a short retry on `QueueFull`.
    /// Uses `send_result` rather than `send`, because the latter is
    /// `async fn` in rdkafka 0.39+ and only enqueues when polled — which
    /// would defeat the Vec-of-futures pipelining in `write_batch`.
    async fn enqueue_with_queue_retry(
        producer: &FutureProducer,
        mut record: FutureRecord<'_, [u8], [u8]>,
        queue_deadline: Instant,
    ) -> Result<DeliveryFuture, KafkaError> {
        loop {
            // The deadline limits waiting, not the immediate enqueue attempt.
            match producer.send_result(record) {
                Ok(fut) => return Ok(fut),
                Err((error @ KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                    let Some(delay) = queue_retry_delay(queue_deadline, Instant::now()) else {
                        return Err(error);
                    };
                    record = r;
                    tokio::time::sleep(delay).await;
                }
                Err((error, _)) => return Err(error),
            }
        }
    }
}

#[async_trait]
impl SinkConnector for KafkaSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let cfg = if config.properties().is_empty() {
            self.config.clone()
        } else {
            KafkaSinkConfig::from_config(config)?
        };
        let exactly_once = cfg.delivery == DeliveryGuarantee::ExactlyOnce;
        let consistency = if exactly_once {
            SinkConsistency::CheckpointCommittable
        } else {
            SinkConsistency::DurableAtLeastOnce
        };
        // Exactly-once output is published only by the fenced designated committer, so staged
        // upserts from any participant compose. Without it, an upsert record carries no fenced
        // generation and a former owner could overwrite a newer value for the same compacted key.
        let topology = if cfg.envelope == SinkEnvelope::Upsert && !exactly_once {
            SinkTopology::Singleton
        } else {
            SinkTopology::MultiWriter
        };
        let input_mode = if cfg.envelope == SinkEnvelope::Upsert {
            SinkInputMode::FullChangelog
        } else {
            SinkInputMode::AppendOnly
        };
        Ok(SinkContract::new(consistency, topology, input_mode))
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        input: SchemaRef,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        super::schema_resolution::resolve_sink(config, input).await
    }

    async fn prepare_schema(
        &mut self,
        config: &ConnectorConfig,
        binding: &mut crate::schema::resolution::SchemaBinding,
    ) -> Result<(), ConnectorError> {
        super::schema_resolution::prepare_sink(config, binding).await
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        if !config.properties().is_empty() {
            let parsed = KafkaSinkConfig::from_config(config)?;
            self.config = parsed;
            self.serializer = select_serializer(
                self.config.format,
                &self.schema,
                Arc::clone(&self.avro_schema_id),
                self.schema_registry.clone(),
            )?;
            self.partitioner = select_partitioner(self.config.partitioner);
        }
        self.config.validate()?;
        info!(
            brokers = %self.config.bootstrap_servers,
            topic = %self.config.topic,
            format = %self.config.format,
            "opening Kafka sink connector"
        );

        self.install_schema_contract(config).await?;
        self.bind_keys()?;
        info!(
            settings = ?super::librdkafka::redacted_settings(&self.config.to_rdkafka_config()),
            "Kafka producer effective configuration"
        );

        let rdkafka_config: ClientConfig = self.config.to_rdkafka_config();
        let producer: FutureProducer = rdkafka_config
            .create()
            .map_err(|error| producer_creation_error("main", &error))?;
        self.producer = Some(producer);

        // Keep DLQ production decoupled from the main producer. Once a producer
        // exists it lives in `self`, so every later error and cancellation is
        // routed through tracked off-runtime teardown.
        if self.config.dlq_topic.is_some() {
            let dlq_config = self.config.to_dlq_rdkafka_config();
            match dlq_config.create::<FutureProducer>() {
                Ok(dlq_producer) => self.dlq_producer = Some(dlq_producer),
                Err(error) => {
                    self.retire_producers();
                    return Err(producer_creation_error("DLQ", &error));
                }
            }
        }

        // Clear stale metadata before lookup. Custom routing must never run
        // against an assumed partition count after a failed reopen.
        self.topic_partition_count = None;
        let producer = self
            .producer
            .as_ref()
            .expect("Kafka producer was installed above")
            .clone();
        let topic = self.config.topic.clone();
        let metadata_guard = self
            .task_owner
            .track()
            .expect("live Kafka sink must admit its metadata lookup");
        let metadata = tokio::task::spawn_blocking(move || -> Result<i32, ConnectorError> {
            let _metadata_guard = metadata_guard;
            let metadata = producer
                .client()
                .fetch_metadata(Some(&topic), Duration::from_secs(5))
                .map_err(|error| fetch_error(&topic, &error))?;
            let topic_metadata = metadata
                .topics()
                .iter()
                .find(|candidate| candidate.name() == topic.as_str())
                .ok_or_else(|| invalid_response(&topic, "metadata response omitted the topic"))?;
            if let Some(error) = topic_metadata.error() {
                return Err(topic_error(&topic, error.into()));
            }
            i32::try_from(topic_metadata.partitions().len())
                .ok()
                .filter(|count| *count > 0)
                .ok_or_else(|| invalid_response(&topic, "broker returned no partitions"))
        })
        .await;
        match metadata {
            Ok(Ok(count)) => {
                let routed = match self.config.partition_count {
                    Some(pinned) if count < pinned => {
                        self.retire_producers();
                        return Err(ConnectorError::ConfigurationError(format!(
                            "Kafka topic '{}' has {count} partitions but partition.count is \
                             {pinned}; keyed records would route to partitions that do not exist",
                            self.config.topic
                        )));
                    }
                    Some(pinned) => pinned,
                    None => count,
                };
                self.topic_partition_count = Some(routed);
                self.unkeyed_partition = 0;
                info!(
                    topic = %self.config.topic,
                    partitions = count,
                    routed_partitions = routed,
                    "queried topic partition count from broker"
                );
            }
            Ok(Err(error)) => {
                self.retire_producers();
                return Err(error);
            }
            Err(error) => {
                self.retire_producers();
                return Err(ConnectorError::Internal(format!(
                    "Kafka metadata worker for topic '{}' failed: {error}",
                    self.config.topic
                )));
            }
        }

        (self.staged, self.exact) = exactly_once_parts(&self.config, &self.task_owner);
        self.state = ConnectorState::Running;
        info!("Kafka sink connector opened successfully");
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    // Record batch row/byte counts fit in narrower types
    // WHY: Keep main enqueue, DLQ enqueue, and both delivery drains in one chronological protocol.
    // Splitting these phases across async helpers would add hot-path futures and make it harder to
    // audit which accepted deliveries are drained before an outcome-unknown error is returned.
    #[allow(clippy::too_many_lines)]
    async fn write_batch(
        &mut self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }

        // Upsert envelope: collapse the Z-set changelog per merge key and produce keyed records
        // (live groups) + null-value tombstones (removed groups). See `write_upsert_batch`.
        if self.config.envelope == SinkEnvelope::Upsert {
            return self.write_upsert_batch(batch).await;
        }

        self.ensure_schema_ready(&batch.schema())?;

        let payloads = self.serialize_payloads(batch)?;

        let (keys, partitions) = self.route_batch(batch, payloads.len())?;
        if let Some(stage) = self.staged.as_mut() {
            let values = payloads.iter().map(|payload| Some(payload.as_slice()));
            return stage_records(stage, &partitions, keys.as_ref(), values);
        }
        let producer = required_producer(self.producer.as_ref())?;

        // Phase 1: enqueue every record into librdkafka's bounded internal queue.
        // A record-local terminal rejection may continue only when it can be
        // routed to DLQ. Every other enqueue error stops new dispatch, but all
        // delivery futures already accepted by librdkafka are still drained.
        let mut delivery_futures = Vec::with_capacity(payloads.len());
        let mut dlq_candidates = Vec::new();
        let mut enqueue_failure: Option<(KafkaFailure, usize)> = None;
        let main_queue_deadline = Instant::now() + QUEUE_RETRY_TIMEOUT;
        for (i, payload) in payloads.iter().enumerate() {
            let mut record = FutureRecord::to(&self.config.topic)
                .payload(payload.as_slice())
                .partition(partitions[i]);
            if let Some(key) = keys.as_ref().and_then(|keys| keys.key(i)) {
                record = record.key(key);
            }

            match Self::enqueue_with_queue_retry(producer, record, main_queue_deadline).await {
                Ok(future) => delivery_futures.push((i, Instant::now(), future)),
                Err(error) => {
                    self.metrics.record_error();
                    let mut failure = KafkaFailure::enqueue(&error, "Kafka");
                    if self.dlq_producer.is_some() && failure.dlq_eligible() {
                        dlq_candidates.push((i, failure.detail));
                        continue;
                    }

                    let affected = payloads.len() - i;
                    if affected > 1 {
                        let _ = write!(
                            failure.detail,
                            "; {} later record(s) were not attempted",
                            affected - 1
                        );
                    }
                    enqueue_failure = Some((failure, affected));
                    break;
                }
            }
        }

        // Enqueue every DLQ candidate before awaiting any delivery report. Both
        // producers then make progress concurrently on their polling threads,
        // so draining the future vectors below does not multiply the driver
        // delivery deadline by the number of rejected rows.
        let dlq_candidate_count = dlq_candidates.len();
        let mut dlq_delivery_futures = Vec::with_capacity(dlq_candidate_count);
        let mut dlq_enqueue_failure: Option<(KafkaFailure, usize)> = None;
        let dlq_queue_deadline = Instant::now() + QUEUE_RETRY_TIMEOUT;
        for (position, (row, original_error)) in dlq_candidates.into_iter().enumerate() {
            let key = keys.as_ref().and_then(|buffer| buffer.key(row));
            match self
                .enqueue_dlq(&payloads[row], key, &original_error, dlq_queue_deadline)
                .await
            {
                Ok(future) => dlq_delivery_futures.push((row, original_error, future)),
                Err(mut failure) => {
                    let affected = dlq_candidate_count - position;
                    if affected > 1 {
                        let _ = write!(
                            failure.detail,
                            "; {} later DLQ record(s) were not attempted",
                            affected - 1
                        );
                    }
                    dlq_enqueue_failure = Some((failure, affected));
                    break;
                }
            }
        }

        let mut records_written: usize = 0;
        let mut bytes_written: u64 = 0;
        let mut definitely_not_persisted: usize = 0;
        let mut ambiguous: usize = 0;
        let mut dlq_records: usize = 0;
        let mut dlq_bytes: u64 = 0;
        let mut first_error: Option<String> = None;
        let mut retryable = true;
        for (row, send_time, future) in delivery_futures {
            match future.await {
                Ok(Ok(_)) => {
                    let latency_us = send_time.elapsed().as_micros() as u64;
                    self.metrics.record_produce_latency(latency_us);
                    records_written += 1;
                    bytes_written += payloads[row].len() as u64;
                }
                Ok(Err((error, _))) => {
                    self.metrics.record_error();
                    let failure = KafkaFailure::delivery(&error, "Kafka");
                    record_failure(
                        &failure,
                        1,
                        &mut definitely_not_persisted,
                        &mut ambiguous,
                        &mut first_error,
                        &mut retryable,
                    );
                }
                Err(_) => {
                    self.metrics.record_error();
                    let failure = KafkaFailure::canceled("Kafka");
                    record_failure(
                        &failure,
                        1,
                        &mut definitely_not_persisted,
                        &mut ambiguous,
                        &mut first_error,
                        &mut retryable,
                    );
                }
            }
        }
        for (row, original_error, future) in dlq_delivery_futures {
            match future.await {
                Ok(Ok(_)) => {
                    self.metrics.record_dlq();
                    dlq_records += 1;
                    dlq_bytes += payloads[row].len() as u64;
                }
                Ok(Err((error, _))) => {
                    self.metrics.record_error();
                    let mut failure = KafkaFailure::delivery(&error, "Kafka DLQ");
                    failure.detail = format!("original: {original_error}; {}", failure.detail);
                    warn!(
                        original_error = %original_error,
                        dlq_error = %failure.detail,
                        "failed to route definitely rejected Kafka record to DLQ"
                    );
                    record_failure(
                        &failure,
                        1,
                        &mut definitely_not_persisted,
                        &mut ambiguous,
                        &mut first_error,
                        &mut retryable,
                    );
                }
                Err(_) => {
                    self.metrics.record_error();
                    let mut failure = KafkaFailure::canceled("Kafka DLQ");
                    failure.detail = format!("original: {original_error}; {}", failure.detail);
                    record_failure(
                        &failure,
                        1,
                        &mut definitely_not_persisted,
                        &mut ambiguous,
                        &mut first_error,
                        &mut retryable,
                    );
                }
            }
        }
        if let Some((failure, affected)) = dlq_enqueue_failure {
            record_failure(
                &failure,
                affected,
                &mut definitely_not_persisted,
                &mut ambiguous,
                &mut first_error,
                &mut retryable,
            );
        }
        if let Some((failure, affected)) = enqueue_failure {
            record_failure(
                &failure,
                affected,
                &mut definitely_not_persisted,
                &mut ambiguous,
                &mut first_error,
                &mut retryable,
            );
        }

        self.metrics
            .record_write(records_written as u64, bytes_written);

        debug!(
            records = records_written,
            dlq_records,
            bytes = bytes_written,
            definitely_not_persisted,
            ambiguous,
            "wrote batch to Kafka"
        );

        let applied = records_written + dlq_records;
        if definitely_not_persisted > 0 || ambiguous > 0 {
            return Err(unresolved_delivery_error(
                "produce",
                payloads.len(),
                applied,
                definitely_not_persisted,
                ambiguous,
                first_error,
                retryable,
            ));
        }

        Ok(WriteResult::new(applied, bytes_written + dlq_bytes))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        if self.staged.as_ref().is_some_and(|stage| !stage.is_empty()) {
            return Err(ConnectorError::InvalidState {
                expected: "the previous exactly-once epoch to be prepared or rolled back".into(),
                actual: "staged Kafka output remains".into(),
            });
        }
        Ok(())
    }

    async fn pre_commit(&mut self, _epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        let Some(stage) = self.staged.as_mut() else {
            return Ok(None);
        };
        if stage.is_empty() {
            return Ok(None);
        }
        let partition_count =
            self.topic_partition_count
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "broker topic metadata installed".into(),
                    actual: "partition count is unavailable".into(),
                })?;
        let descriptor = stage.encode(&self.config.topic, partition_count)?;
        stage.clear();
        Ok(Some(descriptor))
    }

    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        if let Some(stage) = self.staged.as_mut() {
            stage.clear();
        }
        Ok(())
    }

    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        self.exact
            .as_deref()
            .map(|committer| committer as &dyn CoordinatedCommitter)
    }

    fn suggested_write_timeout(&self) -> Duration {
        // The runtime deadline must dominate librdkafka's delivery deadline.
        // Main and DLQ deliveries are concurrent, so only bounded queue retry
        // and scheduling headroom are added rather than a per-row multiplier.
        kafka_write_timeout(self.config.delivery_timeout)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        // Every successful write_batch awaits every delivery report (including
        // DLQ delivery). There can be no acknowledged connector write with
        // producer work still in flight, so a blocking librdkafka flush adds no
        // checkpoint durability and is unsafe to cancel on generation retirement.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Kafka sink connector");

        // Completed writes have already observed delivery reports. A cancelled
        // write retires the whole producer generation and is replayed from the
        // engine checkpoint, so close must not start another blocking flush.
        self.retire_producers();
        self.state = ConnectorState::Closed;
        info!("Kafka sink connector closed");
        Ok(())
    }
}

impl Drop for KafkaSink {
    fn drop(&mut self) {
        self.retire_producers();
    }
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("state", &self.state)
            .field("topic", &self.config.topic)
            .field("format", &self.config.format)
            .finish_non_exhaustive()
    }
}

/// Exactly-once: hold routed records for the checkpoint's transaction instead of producing them.
fn stage_records<'a>(
    stage: &mut StagedEpoch,
    partitions: &[i32],
    keys: Option<&KeyBuffer>,
    values: impl ExactSizeIterator<Item = Option<&'a [u8]>>,
) -> Result<WriteResult, ConnectorError> {
    let rows = values.len();
    for (row, value) in values.enumerate() {
        stage.push(partitions[row], keys.and_then(|keys| keys.key(row)), value)?;
    }
    Ok(WriteResult::new(rows, 0))
}

/// The epoch stage and designated committer of an exactly-once sink; `None` otherwise.
fn exactly_once_parts(
    config: &KafkaSinkConfig,
    task_owner: &ConnectorTaskOwner,
) -> (Option<StagedEpoch>, Option<Arc<KafkaExactCommitter>>) {
    match (config.delivery, config.commit_topic.as_ref()) {
        (DeliveryGuarantee::ExactlyOnce, Some(commit_topic)) => (
            Some(StagedEpoch::new(
                config.epoch_max_bytes,
                config.message_max_bytes().unwrap_or(usize::MAX),
            )),
            Some(Arc::new(KafkaExactCommitter::new(
                config.clone(),
                commit_topic.clone(),
                task_owner.admission(),
            ))),
        ),
        _ => (None, None),
    }
}

/// Selects the appropriate partitioner for the given strategy.
fn select_partitioner(strategy: PartitionStrategy) -> Box<dyn KafkaPartitioner> {
    match strategy {
        PartitionStrategy::KeyHash => Box::new(KeyHashPartitioner::new()),
        PartitionStrategy::RoundRobin => Box::new(RoundRobinPartitioner::new()),
        PartitionStrategy::Sticky => Box::new(StickyPartitioner::new(100)),
    }
}

#[cfg(test)]
mod tests;
