//! Kafka sink connector.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, StringArray};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use tracing::{debug, info, warn};

use crate::changelog::collapse_changelog;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::{ConnectorError, SerdeError};
use crate::serde::{self, Format, RecordSerializer};

use super::avro_serializer::AvroSerializer;
use super::metadata_error::{fetch_error, invalid_response, topic_error};
use super::partitioner::{
    KafkaPartitioner, KeyHashPartitioner, RoundRobinPartitioner, StickyPartitioner,
};
use super::schema_registry::SchemaRegistryClient;
use super::sink_config::{KafkaSinkConfig, PartitionStrategy, SinkEnvelope};
use super::sink_metrics::KafkaSinkMetrics;

/// One queue-full retry deadline is shared by every record sent through a
/// producer phase. A non-record failure stops new enqueue work.
const QUEUE_RETRY_TIMEOUT: Duration = Duration::from_millis(500);
const QUEUE_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const WRITE_TIMEOUT_HEADROOM: Duration = Duration::from_secs(5);

fn queue_retry_delay(deadline: Instant, now: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(now);
    (!remaining.is_zero()).then_some(remaining.min(QUEUE_RETRY_INTERVAL))
}

fn delivery_outcome_unknown(
    operation: &str,
    detail: impl std::fmt::Display,
    retryable: bool,
) -> ConnectorError {
    ConnectorError::outcome_unknown(
        format!(
            "Kafka {operation} was dispatched but its external outcome is not fully known: {detail}"
        ),
        retryable,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KafkaFailureCertainty {
    DefinitelyNotPersisted,
    OutcomeUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KafkaFailureScope {
    Record,
    Infrastructure,
    Connector,
}

#[derive(Debug)]
struct KafkaFailure {
    certainty: KafkaFailureCertainty,
    scope: KafkaFailureScope,
    retryable: bool,
    detail: String,
}

impl KafkaFailure {
    /// `FutureProducer::send_result` returned the record, proving that this
    /// attempt never entered librdkafka's queue.
    fn enqueue(error: &KafkaError, operation: &str) -> Self {
        let (scope, retryable) = kafka_error_policy(error);
        Self {
            certainty: KafkaFailureCertainty::DefinitelyNotPersisted,
            scope,
            retryable,
            detail: format!("{operation} enqueue failed before dispatch: {error}"),
        }
    }

    /// rdkafka 0.39's `FutureProducer` discards native
    /// `rd_kafka_message_status` when it creates an owned delivery result.
    /// Error codes alone cannot prove non-persistence after driver retries.
    fn delivery(error: &KafkaError, operation: &str) -> Self {
        let (scope, retryable) = kafka_error_policy(error);
        Self {
            certainty: KafkaFailureCertainty::OutcomeUnknown,
            scope,
            retryable,
            detail: format!("{operation} delivery failed: {error}"),
        }
    }

    fn canceled(operation: &str) -> Self {
        Self {
            certainty: KafkaFailureCertainty::OutcomeUnknown,
            scope: KafkaFailureScope::Infrastructure,
            retryable: true,
            detail: format!("{operation} delivery canceled because the producer was dropped"),
        }
    }

    fn dlq_eligible(&self) -> bool {
        self.certainty == KafkaFailureCertainty::DefinitelyNotPersisted
            && self.scope == KafkaFailureScope::Record
            && !self.retryable
    }
}

/// Error scope and retryability are independent of persistence certainty. This
/// is deliberately a positive transient list: fatal, unknown, and future codes
/// fail closed instead of creating an unbounded restart loop.
fn kafka_error_policy(error: &KafkaError) -> (KafkaFailureScope, bool) {
    match error.rdkafka_error_code() {
        Some(
            RDKafkaErrorCode::KeySerialization
            | RDKafkaErrorCode::ValueSerialization
            | RDKafkaErrorCode::MessageSizeTooLarge
            | RDKafkaErrorCode::InvalidTimestamp
            | RDKafkaErrorCode::InvalidRecord,
        ) => (KafkaFailureScope::Record, false),
        Some(
            RDKafkaErrorCode::BrokerDestroy
            | RDKafkaErrorCode::BrokerTransportFailure
            | RDKafkaErrorCode::Resolve
            | RDKafkaErrorCode::MessageTimedOut
            | RDKafkaErrorCode::AllBrokersDown
            | RDKafkaErrorCode::OperationTimedOut
            | RDKafkaErrorCode::QueueFull
            | RDKafkaErrorCode::ISRInsufficient
            | RDKafkaErrorCode::TimedOutQueue
            | RDKafkaErrorCode::WaitCache
            | RDKafkaErrorCode::Interrupted
            | RDKafkaErrorCode::Retry
            | RDKafkaErrorCode::PurgeQueue
            | RDKafkaErrorCode::PurgeInflight
            | RDKafkaErrorCode::DestroyBroker
            | RDKafkaErrorCode::UnknownTopicOrPartition
            | RDKafkaErrorCode::LeaderNotAvailable
            | RDKafkaErrorCode::NotLeaderForPartition
            | RDKafkaErrorCode::RequestTimedOut
            | RDKafkaErrorCode::BrokerNotAvailable
            | RDKafkaErrorCode::ReplicaNotAvailable
            | RDKafkaErrorCode::NetworkException
            | RDKafkaErrorCode::NotEnoughReplicas
            | RDKafkaErrorCode::NotEnoughReplicasAfterAppend
            | RDKafkaErrorCode::NotController
            | RDKafkaErrorCode::KafkaStorageError
            | RDKafkaErrorCode::ReassignmentInProgress
            | RDKafkaErrorCode::FencedLeaderEpoch
            | RDKafkaErrorCode::UnknownLeaderEpoch
            | RDKafkaErrorCode::StaleBrokerEpoch
            | RDKafkaErrorCode::EligibleLeadersNotAvailable
            | RDKafkaErrorCode::ThrottlingQuotaExceeded
            | RDKafkaErrorCode::UnknownTopicId,
        ) => (KafkaFailureScope::Infrastructure, true),
        _ => (KafkaFailureScope::Connector, false),
    }
}

fn unresolved_delivery_error(
    operation: &str,
    total: usize,
    applied: usize,
    definitely_not_persisted: usize,
    ambiguous: usize,
    first_error: Option<String>,
    retryable: bool,
) -> ConnectorError {
    let detail = format!(
        "{definitely_not_persisted} definitely not persisted, {ambiguous} outcome unknown, \
         {applied} already applied out of {total}; first error: {}",
        first_error.unwrap_or_else(|| "unknown".into())
    );
    if ambiguous > 0 || applied > 0 {
        delivery_outcome_unknown(operation, detail, retryable)
    } else if retryable {
        ConnectorError::WriteError(format!("Kafka {operation} failed: {detail}"))
    } else {
        ConnectorError::ConfigurationError(format!("Kafka {operation} was rejected: {detail}"))
    }
}

fn record_failure(
    failure: &KafkaFailure,
    count: usize,
    definitely_not_persisted: &mut usize,
    ambiguous: &mut usize,
    first_error: &mut Option<String>,
    retryable: &mut bool,
) {
    match failure.certainty {
        KafkaFailureCertainty::DefinitelyNotPersisted => *definitely_not_persisted += count,
        KafkaFailureCertainty::OutcomeUnknown => *ambiguous += count,
    }
    *retryable &= failure.retryable;
    first_error.get_or_insert_with(|| failure.detail.clone());
}

fn kafka_write_timeout(delivery_timeout: Duration) -> Duration {
    delivery_timeout
        .saturating_add(QUEUE_RETRY_TIMEOUT)
        .saturating_add(QUEUE_RETRY_TIMEOUT)
        .saturating_add(WRITE_TIMEOUT_HEADROOM)
}

fn producer_creation_error(role: &str, error: &KafkaError) -> ConnectorError {
    ConnectorError::ConfigurationError(format!("failed to create Kafka {role} producer: {error}"))
}

fn validate_payload_cardinality(expected: usize, actual: usize) -> Result<(), ConnectorError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ConnectorError::Serde(SerdeError::RecordCountMismatch {
            expected,
            got: actual,
        }))
    }
}

/// Contiguous key buffer — stores all key bytes in a single allocation
/// with per-row `(offset, length)` pairs. Avoids N separate heap
/// allocations for N rows.
struct KeyBuffer {
    data: Vec<u8>,
    offsets: Vec<(usize, usize)>,
}

impl KeyBuffer {
    fn with_capacity(num_rows: usize, avg_key_len: usize) -> Self {
        Self {
            data: Vec::with_capacity(num_rows * avg_key_len),
            offsets: Vec::with_capacity(num_rows),
        }
    }

    fn push(&mut self, key: &[u8]) {
        let start = self.data.len();
        self.data.extend_from_slice(key);
        self.offsets.push((start, key.len()));
    }

    fn push_empty(&mut self) {
        self.offsets.push((0, 0));
    }

    fn key(&self, i: usize) -> &[u8] {
        let (start, len) = self.offsets[i];
        &self.data[start..start + len]
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.offsets.len()
    }
}

impl std::ops::Index<usize> for KeyBuffer {
    type Output = [u8];

    fn index(&self, i: usize) -> &[u8] {
        self.key(i)
    }
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
    /// Optional Schema Registry client.
    schema_registry: Option<Arc<SchemaRegistryClient>>,
    /// Shared Avro schema ID (updated after SR registration).
    avro_schema_id: Arc<std::sync::atomic::AtomicU32>,
    /// Cached topic partition count (queried from broker metadata after open).
    topic_partition_count: Option<i32>,
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

        Self {
            producer: None,
            config,
            serializer,
            partitioner,
            state: ConnectorState::Created,
            dlq_producer: None,
            metrics: KafkaSinkMetrics::new(registry),
            schema,
            schema_registry: None,
            avro_schema_id,
            topic_partition_count: None,
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

        Self {
            producer: None,
            config,
            serializer,
            partitioner,
            state: ConnectorState::Created,
            dlq_producer: None,
            metrics: KafkaSinkMetrics::new(None),
            schema,
            schema_registry: Some(sr),
            avro_schema_id,
            topic_partition_count: None,
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

    /// Destroy the final producer references away from Tokio workers. rdkafka
    /// purges, flushes for up to 500 ms, and joins its polling thread in Drop.
    fn retire_producers(&mut self) {
        if let Some(producer) = self.producer.take() {
            self.spawn_producer_drop(producer, "main");
        }
        if let Some(producer) = self.dlq_producer.take() {
            self.spawn_producer_drop(producer, "DLQ");
        }
    }

    fn spawn_producer_drop(&self, producer: FutureProducer, role: &'static str) {
        let Some(terminal_guard) = self.task_owner.track() else {
            // The owner is a field on this live connector, so sealing before
            // Drop is an invariant violation rather than a recoverable state.
            tracing::error!(
                role,
                "Kafka producer teardown could not enter the terminal task tracker"
            );
            return;
        };
        let teardown = move || {
            let _terminal_guard = terminal_guard;
            drop(producer);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn_blocking(teardown));
        } else if let Err(error) = std::thread::Builder::new()
            .name("laminardb-kafka-producer-drop".into())
            .spawn(teardown)
        {
            // This branch cannot run on a Tokio worker. The closure has already
            // been dropped by `spawn`, so only report the resource failure.
            tracing::error!(role, %error, "failed to start Kafka producer teardown thread");
        }
    }

    /// Ensures the sink schema and SR registration match the actual data.
    async fn ensure_schema_ready(
        &mut self,
        batch_schema: &SchemaRef,
    ) -> Result<(), ConnectorError> {
        let schema_changed = self.schema != *batch_schema;
        let needs_registration = self.config.format == Format::Avro
            && (schema_changed
                || self
                    .avro_schema_id
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == 0);

        // Register with SR *before* advancing schema/serializer so a failure
        // doesn't leave avro_schema_id stale while the serializer already
        // encodes with the new schema.
        if needs_registration {
            if let Some(ref sr) = self.schema_registry {
                let subject = format!("{}-value", self.config.topic);
                let avro_schema =
                    super::schema_registry::arrow_to_avro_schema(batch_schema, &self.config.topic)
                        .map_err(ConnectorError::Serde)?;
                let schema_id = sr
                    .register_schema(
                        &subject,
                        &avro_schema,
                        super::schema_registry::SchemaType::Avro,
                    )
                    .await?;
                #[allow(clippy::cast_sign_loss)]
                self.avro_schema_id
                    .store(schema_id as u32, std::sync::atomic::Ordering::Relaxed);
                info!(subject = %subject, schema_id, "registered Avro schema");
            }
        }

        if schema_changed {
            debug!(
                old = ?self.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                new = ?batch_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                "sink schema updated from incoming batch"
            );
            self.schema = batch_schema.clone();
            self.serializer = select_serializer(
                self.config.format,
                &self.schema,
                Arc::clone(&self.avro_schema_id),
                self.schema_registry.clone(),
            )?;
        }

        Ok(())
    }

    /// Contiguous key buffer: all key bytes in one allocation with per-row offsets.
    ///
    /// Returns `None` if no key column is configured.
    fn extract_keys(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Option<KeyBuffer>, ConnectorError> {
        let Some(key_col) = &self.config.key_column else {
            return Ok(None);
        };

        let col_idx = batch.schema().index_of(key_col).map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "key column '{key_col}' not found in schema"
            ))
        })?;

        let array = batch.column(col_idx);
        let num_rows = batch.num_rows();
        let mut buf = KeyBuffer::with_capacity(num_rows, 32);

        // Try to get string values; fall back to display representation.
        if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
            for i in 0..num_rows {
                if str_array.is_null(i) {
                    buf.push_empty();
                } else {
                    buf.push(str_array.value(i).as_bytes());
                }
            }
        } else {
            use std::fmt::Write;
            let formatter = arrow_cast::display::ArrayFormatter::try_new(
                array,
                &arrow_cast::display::FormatOptions::default(),
            )
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "failed to create array formatter for key column: {e}"
                ))
            })?;
            let mut fmt_buf = String::with_capacity(64);
            for i in 0..num_rows {
                if array.is_null(i) {
                    buf.push_empty();
                } else {
                    fmt_buf.clear();
                    let _ = write!(fmt_buf, "{}", formatter.value(i));
                    buf.push(fmt_buf.as_bytes());
                }
            }
        }

        Ok(Some(buf))
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

    /// Upsert-envelope produce: collapse the Z-set changelog to one record per merge key, then
    /// emit a keyed value for a live group (`_op = U`) or a null-value tombstone for a removed group
    /// (`_op = D`). The topic must be log-compacted and keyed on the merge key for the tombstones to
    /// GC and for the latest-per-key state to be recoverable from offset 0.
    #[allow(clippy::cast_possible_truncation, clippy::too_many_lines)] // matches write_batch
    async fn write_upsert_batch(
        &mut self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<WriteResult, ConnectorError> {
        let key_col = self.config.key_column.clone().ok_or_else(|| {
            ConnectorError::ConfigurationError("envelope = 'upsert' requires 'key.column'".into())
        })?;
        let collapsed = collapse_changelog(batch, std::slice::from_ref(&key_col))?;
        let rows = collapsed.num_rows();
        if rows == 0 {
            return Ok(WriteResult::new(0, 0));
        }
        let op_idx = collapsed
            .schema()
            .index_of("_op")
            .map_err(|_| ConnectorError::Internal("collapsed changelog missing _op".into()))?;
        let ops = collapsed
            .column(op_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ConnectorError::Internal("_op column is not Utf8".into()))?
            .clone();

        // The value is the collapsed row without the `_op` tag — i.e. the plain MV row.
        let value_idxs: Vec<usize> = (0..collapsed.num_columns())
            .filter(|&i| i != op_idx)
            .collect();
        let value_batch = collapsed
            .project(&value_idxs)
            .map_err(|e| ConnectorError::Internal(format!("project value columns: {e}")))?;

        self.ensure_schema_ready(&value_batch.schema()).await?;
        let payloads = self.serializer.serialize(&value_batch).map_err(|e| {
            self.metrics.record_serialization_error();
            ConnectorError::Serde(e)
        })?;
        validate_payload_cardinality(rows, payloads.len())?;
        let keys = self.extract_keys(&collapsed)?;
        // Reject empty/NULL merge keys before producing ANY record: a compacted topic can't
        // represent an unkeyed row, and a mid-loop bail would leave earlier rows already enqueued.
        if let Some(kb) = keys.as_ref() {
            for i in 0..payloads.len() {
                if kb.key(i).is_empty() {
                    return Err(ConnectorError::WriteError(format!(
                        "upsert envelope: row {i} has an empty/NULL merge key"
                    )));
                }
            }
        }

        let partition_count =
            self.topic_partition_count
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "broker topic metadata installed".into(),
                    actual: "partition count is unavailable".into(),
                })?;
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "producer initialized".into(),
                actual: "producer is None".into(),
            })?;

        let mut delivery_futures = Vec::with_capacity(rows);
        let mut enqueue_failure: Option<(KafkaFailure, usize)> = None;
        let queue_deadline = Instant::now() + QUEUE_RETRY_TIMEOUT;
        for (i, payload) in payloads.iter().enumerate() {
            let key: Option<&[u8]> = keys.as_ref().map(|kb| kb.key(i));
            let is_delete = ops.value(i) == "D";
            let partition = self.partitioner.partition(key, partition_count);
            // No `.payload()` for a delete → null value = Kafka tombstone.
            let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&self.config.topic);
            if let Some(k) = key {
                record = record.key(k);
            }
            if !is_delete {
                record = record.payload(payload.as_slice());
            }
            if let Some(p) = partition {
                record = record.partition(p);
            }
            match Self::enqueue_with_queue_retry(producer, record, queue_deadline).await {
                Ok(future) => delivery_futures.push((Instant::now(), future, is_delete, i)),
                Err(error) => {
                    let mut failure = KafkaFailure::enqueue(&error, "Kafka upsert");
                    let affected = rows - i;
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

        let mut records_written: usize = 0;
        let mut bytes_written: u64 = 0;
        let mut definitely_not_persisted: usize = 0;
        let mut ambiguous: usize = 0;
        let mut first_error: Option<String> = None;
        let mut retryable = true;
        for (send_time, future, is_delete, i) in delivery_futures {
            match future.await {
                Ok(Ok(_)) => {
                    self.metrics
                        .record_produce_latency(send_time.elapsed().as_micros() as u64);
                    records_written += 1;
                    if !is_delete {
                        bytes_written += payloads[i].len() as u64;
                    }
                }
                Ok(Err((err, _))) => {
                    let failure = KafkaFailure::delivery(&err, "Kafka upsert");
                    record_failure(
                        &failure,
                        1,
                        &mut definitely_not_persisted,
                        &mut ambiguous,
                        &mut first_error,
                        &mut retryable,
                    );
                }
                Err(_canceled) => {
                    let failure = KafkaFailure::canceled("Kafka upsert");
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
        if definitely_not_persisted > 0 || ambiguous > 0 {
            self.metrics.record_error();
            return Err(unresolved_delivery_error(
                "upsert produce",
                rows,
                records_written,
                definitely_not_persisted,
                ambiguous,
                first_error,
                retryable,
            ));
        }
        Ok(WriteResult::new(records_written, bytes_written))
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
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
        // Kafka acknowledges these idempotent writes with all in-sync replicas,
        // but this connector does not expose an external checkpoint cursor.
        let consistency = SinkConsistency::DurableAtLeastOnce;
        // Append records from independent writers compose safely. Upsert records do not carry a
        // fenced generation, so an old writer can otherwise overwrite a newer value for the same
        // compacted key after ownership handoff.
        let topology = if cfg.envelope == SinkEnvelope::Upsert {
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

        if let Some(ref url) = self.config.schema_registry_url {
            if self.schema_registry.is_none() {
                let sr = if let Some(ref ca_path) = self.config.schema_registry_ssl_ca_location {
                    SchemaRegistryClient::with_tls(
                        url,
                        self.config.schema_registry_auth.clone(),
                        ca_path,
                    )?
                } else {
                    SchemaRegistryClient::new(url, self.config.schema_registry_auth.clone())?
                };
                self.schema_registry = Some(Arc::new(sr));
            }
        }

        // Schema registration is deferred to the first write_batch(), where the real pipeline
        // output schema is known — the factory default is a placeholder that would pollute the
        // registry and break compat checks.
        if self.config.format == Format::Avro {
            if let Some(ref sr) = self.schema_registry {
                if let Some(ref compat) = self.config.schema_compatibility {
                    let subject = format!("{}-value", self.config.topic);
                    sr.set_compatibility_level(&subject, *compat).await?;
                }
            }
        }

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
                self.topic_partition_count = Some(count);
                info!(
                    topic = %self.config.topic,
                    partitions = count,
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

        self.state = ConnectorState::Running;
        info!("Kafka sink connector opened successfully");
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)] // Record batch row/byte counts fit in narrower types
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

        self.ensure_schema_ready(&batch.schema()).await?;

        let payloads = self.serializer.serialize(batch).map_err(|e| {
            self.metrics.record_serialization_error();
            ConnectorError::Serde(e)
        })?;
        validate_payload_cardinality(batch.num_rows(), payloads.len())?;

        let keys = self.extract_keys(batch)?;
        let partition_count =
            self.topic_partition_count
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "broker topic metadata installed".into(),
                    actual: "partition count is unavailable".into(),
                })?;
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "producer initialized".into(),
                actual: "producer is None".into(),
            })?;

        // Phase 1: enqueue every record into librdkafka's bounded internal queue.
        // A record-local terminal rejection may continue only when it can be
        // routed to DLQ. Every other enqueue error stops new dispatch, but all
        // delivery futures already accepted by librdkafka are still drained.
        let mut delivery_futures = Vec::with_capacity(payloads.len());
        let mut dlq_candidates = Vec::new();
        let mut enqueue_failure: Option<(KafkaFailure, usize)> = None;
        let main_queue_deadline = Instant::now() + QUEUE_RETRY_TIMEOUT;
        for (i, payload) in payloads.iter().enumerate() {
            let key: Option<&[u8]> = keys.as_ref().map(|kb| kb.key(i)).filter(|k| !k.is_empty());
            let partition = self.partitioner.partition(key, partition_count);

            let mut record = FutureRecord::to(&self.config.topic).payload(payload.as_slice());
            if let Some(k) = key {
                record = record.key(k);
            }
            if let Some(p) = partition {
                record = record.partition(p);
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
            let key = keys
                .as_ref()
                .map(|buffer| buffer.key(row))
                .filter(|key| !key.is_empty());
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

/// Selects the appropriate serializer for the given format.
///
/// For Avro, uses the shared `schema_id` handle so that Schema Registry
/// registration updates are visible to the serializer.
fn select_serializer(
    format: Format,
    schema: &SchemaRef,
    schema_id: Arc<std::sync::atomic::AtomicU32>,
    registry: Option<Arc<SchemaRegistryClient>>,
) -> Result<Box<dyn RecordSerializer>, ConnectorError> {
    match format {
        Format::Avro => Ok(Box::new(AvroSerializer::with_shared_schema_id(
            schema.clone(),
            schema_id,
            registry,
        ))),
        other => serde::create_serializer(other).map_err(|e| {
            ConnectorError::ConfigurationError(format!("unsupported sink format '{other}': {e}"))
        }),
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
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    struct MismatchedSerializer;

    impl RecordSerializer for MismatchedSerializer {
        fn serialize(&self, _batch: &arrow_array::RecordBatch) -> Result<Vec<Vec<u8>>, SerdeError> {
            Ok(vec![b"one-payload".to_vec()])
        }

        fn format(&self) -> Format {
            Format::Json
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn test_config() -> KafkaSinkConfig {
        let mut cfg = KafkaSinkConfig::default();
        cfg.bootstrap_servers = "localhost:9092".into();
        cfg.topic = "output-events".into();
        cfg
    }

    fn two_row_batch() -> arrow_array::RecordBatch {
        arrow_array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_new_defaults() {
        let sink = KafkaSink::new(test_schema(), test_config(), None);
        assert_eq!(sink.state(), ConnectorState::Created);
        assert!(sink.producer.is_none());
        assert_eq!(sink.topic_partition_count, None);
    }

    #[test]
    fn local_producer_creation_failure_is_terminal_configuration() {
        let mut invalid = ClientConfig::new();
        invalid.set("laminardb.invalid.kafka.property", "value");
        let Err(error) = invalid.create::<FutureProducer>() else {
            panic!("an unknown local librdkafka option must fail client creation");
        };

        let mapped = producer_creation_error("main", &error);
        assert!(matches!(mapped, ConnectorError::ConfigurationError(_)));
        assert!(!mapped.is_transient());
    }

    #[test]
    fn malformed_broker_metadata_is_terminal_and_has_no_partition_fallback() {
        let error = invalid_response("orders", "metadata response omitted the topic");
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
        assert!(!error.is_transient());

        let sink = KafkaSink::new(test_schema(), test_config(), None);
        assert_eq!(sink.topic_partition_count, None);
    }

    #[tokio::test]
    async fn append_cardinality_mismatch_fails_before_producer_access() {
        let mut sink = KafkaSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;
        sink.topic_partition_count = Some(3);
        sink.serializer = Box::new(MismatchedSerializer);

        let error = sink.write_batch(&two_row_batch()).await.unwrap_err();
        assert!(matches!(
            error,
            ConnectorError::Serde(SerdeError::RecordCountMismatch {
                expected: 2,
                got: 1
            })
        ));
    }

    #[tokio::test]
    async fn upsert_cardinality_mismatch_fails_before_producer_access() {
        let mut config = test_config();
        config.envelope = SinkEnvelope::Upsert;
        config.key_column = Some("id".into());
        let mut sink = KafkaSink::new(test_schema(), config, None);
        sink.state = ConnectorState::Running;
        sink.topic_partition_count = Some(3);
        sink.serializer = Box::new(MismatchedSerializer);

        let error = sink.write_batch(&two_row_batch()).await.unwrap_err();
        assert!(matches!(
            error,
            ConnectorError::Serde(SerdeError::RecordCountMismatch {
                expected: 2,
                got: 1
            })
        ));
    }

    #[tokio::test]
    async fn schema_registration_preserves_terminal_registry_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/output-events-value/versions"))
            .respond_with(ResponseTemplate::new(422).set_body_string("invalid schema"))
            .mount(&server)
            .await;
        let mut config = test_config();
        config.format = Format::Avro;
        config.schema_registry_url = Some(server.uri());
        let registry = SchemaRegistryClient::new(server.uri(), None).unwrap();
        let mut sink = KafkaSink::with_schema_registry(test_schema(), config, registry);

        let error = sink.ensure_schema_ready(&test_schema()).await.unwrap_err();
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
        assert!(!error.is_transient());
        assert!(error.to_string().contains("output-events-value"));
    }

    #[tokio::test]
    async fn compatibility_put_preserves_terminal_registry_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/config/output-events-value"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid credentials"))
            .mount(&server)
            .await;
        let mut config = test_config();
        config.format = Format::Avro;
        config.schema_registry_url = Some(server.uri());
        config.schema_compatibility = Some(crate::kafka::config::CompatibilityLevel::Backward);
        let registry = SchemaRegistryClient::new(server.uri(), None).unwrap();
        let mut sink = KafkaSink::with_schema_registry(test_schema(), config, registry);

        let error = sink.open(&ConnectorConfig::new("kafka")).await.unwrap_err();
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
        assert!(!error.is_transient());
        assert!(error.to_string().contains("output-events-value"));
    }

    #[test]
    fn terminal_tracker_seals_when_sink_is_dropped() {
        let sink = KafkaSink::new(test_schema(), test_config(), None);
        let terminal = sink.terminal_task_tracker().unwrap();
        assert!(!terminal.is_terminated());
        drop(sink);
        assert!(terminal.is_terminated());
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let sink = KafkaSink::new(schema.clone(), test_config(), None);
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn contract_is_multi_writer_durable_at_least_once() {
        let sink = KafkaSink::new(test_schema(), test_config(), None);
        let contract = sink.contract(&ConnectorConfig::new("kafka")).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
        assert_eq!(contract.topology, SinkTopology::MultiWriter);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
        assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(126));
    }

    #[test]
    fn delivery_error_codes_never_claim_non_persistence_without_native_status() {
        for code in [
            RDKafkaErrorCode::MessageTimedOut,
            RDKafkaErrorCode::TimedOutQueue,
            RDKafkaErrorCode::PurgeQueue,
            RDKafkaErrorCode::PurgeInflight,
            RDKafkaErrorCode::MessageSizeTooLarge,
            RDKafkaErrorCode::TopicAuthorizationFailed,
        ] {
            let error = KafkaError::MessageProduction(code);
            assert_eq!(
                KafkaFailure::delivery(&error, "test").certainty,
                KafkaFailureCertainty::OutcomeUnknown,
                "delivery code {code:?}"
            );
        }
    }

    #[test]
    fn only_terminal_record_local_enqueue_failures_are_dlq_eligible() {
        let too_large = KafkaFailure::enqueue(
            &KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
            "test",
        );
        assert_eq!(
            too_large.certainty,
            KafkaFailureCertainty::DefinitelyNotPersisted
        );
        assert_eq!(too_large.scope, KafkaFailureScope::Record);
        assert!(!too_large.retryable);
        assert!(too_large.dlq_eligible());

        let queue_full = KafkaFailure::enqueue(
            &KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
            "test",
        );
        assert_eq!(queue_full.scope, KafkaFailureScope::Infrastructure);
        assert!(queue_full.retryable);
        assert!(!queue_full.dlq_eligible());

        let unauthorized = KafkaFailure::enqueue(
            &KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed),
            "test",
        );
        assert_eq!(unauthorized.scope, KafkaFailureScope::Connector);
        assert!(!unauthorized.retryable);
        assert!(!unauthorized.dlq_eligible());
    }

    #[test]
    fn fatal_and_unknown_codes_fail_closed() {
        for code in [
            RDKafkaErrorCode::Unknown,
            RDKafkaErrorCode::Fatal,
            RDKafkaErrorCode::ProducerFenced,
        ] {
            let failure = KafkaFailure::delivery(&KafkaError::MessageProduction(code), "test");
            assert_eq!(failure.scope, KafkaFailureScope::Connector);
            assert!(!failure.retryable, "delivery code {code:?}");
        }
    }

    #[test]
    fn aggregate_retryability_is_the_conjunction_of_every_failure() {
        let transient = KafkaFailure::delivery(
            &KafkaError::MessageProduction(RDKafkaErrorCode::RequestTimedOut),
            "test",
        );
        let terminal = KafkaFailure::enqueue(
            &KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
            "test",
        );
        let mut definitely_not_persisted = 0;
        let mut ambiguous = 0;
        let mut first_error = None;
        let mut retryable = true;
        record_failure(
            &transient,
            1,
            &mut definitely_not_persisted,
            &mut ambiguous,
            &mut first_error,
            &mut retryable,
        );
        record_failure(
            &terminal,
            2,
            &mut definitely_not_persisted,
            &mut ambiguous,
            &mut first_error,
            &mut retryable,
        );

        assert_eq!(definitely_not_persisted, 2);
        assert_eq!(ambiguous, 1);
        assert!(!retryable);
    }

    #[test]
    fn suggested_timeout_tracks_driver_deadline_with_constant_headroom() {
        let mut config = test_config();
        config.delivery_timeout = Duration::from_secs(42);
        let sink = KafkaSink::new(test_schema(), config, None);
        assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(48));
    }

    #[test]
    fn queue_retry_wait_is_bounded_across_records() {
        let start = Instant::now();
        let deadline = start + QUEUE_RETRY_TIMEOUT;
        let mut now = start;
        let mut total_wait = Duration::ZERO;

        for _ in 0..32 {
            if let Some(delay) = queue_retry_delay(deadline, now) {
                total_wait += delay;
                now += delay;
            }
        }

        assert_eq!(total_wait, QUEUE_RETRY_TIMEOUT);
        assert_eq!(now, deadline);
        assert_eq!(queue_retry_delay(deadline, now), None);
    }

    #[test]
    fn later_record_cannot_restart_an_expired_queue_retry_budget() {
        let start = Instant::now();
        let deadline = start + QUEUE_RETRY_TIMEOUT;

        assert_eq!(
            queue_retry_delay(
                deadline,
                deadline.checked_sub(Duration::from_millis(25)).unwrap(),
            ),
            Some(Duration::from_millis(25))
        );
        assert_eq!(
            queue_retry_delay(deadline, deadline + Duration::from_secs(1)),
            None
        );
    }

    #[test]
    fn partial_or_ambiguous_batch_requires_generation_retirement() {
        let error =
            unresolved_delivery_error("produce", 3, 1, 2, 0, Some("rejected".into()), false);
        assert!(error.is_outcome_unknown());
        assert!(!error.is_transient());

        let error =
            unresolved_delivery_error("produce", 1, 0, 0, 1, Some("timed out".into()), true);
        assert!(error.is_outcome_unknown());
        assert!(error.is_transient());

        let error =
            unresolved_delivery_error("produce", 1, 0, 1, 0, Some("too large".into()), false);
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    }

    #[test]
    fn upsert_contract_requires_singleton_writer() {
        let mut config = test_config();
        config.envelope = SinkEnvelope::Upsert;
        config.key_column = Some("id".into());
        let sink = KafkaSink::new(test_schema(), config, None);

        let contract = sink.contract(&ConnectorConfig::new("kafka")).unwrap();

        assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
    }

    #[test]
    fn test_serializer_selection_json() {
        let sink = KafkaSink::new(test_schema(), test_config(), None);
        assert_eq!(sink.serializer.format(), Format::Json);
    }

    #[test]
    fn test_serializer_selection_avro() {
        let mut cfg = test_config();
        cfg.format = Format::Avro;
        let sink = KafkaSink::new(test_schema(), cfg, None);
        assert_eq!(sink.serializer.format(), Format::Avro);
    }

    #[test]
    fn test_with_schema_registry() {
        let sr = SchemaRegistryClient::new("http://localhost:8081", None).unwrap();
        let mut cfg = test_config();
        cfg.format = Format::Avro;
        cfg.schema_registry_url = Some("http://localhost:8081".into());

        let sink = KafkaSink::with_schema_registry(test_schema(), cfg, sr);
        assert!(sink.has_schema_registry());
        assert_eq!(sink.serializer.format(), Format::Avro);
    }

    #[test]
    fn test_debug_output() {
        let sink = KafkaSink::new(test_schema(), test_config(), None);
        let debug = format!("{sink:?}");
        assert!(debug.contains("KafkaSink"));
        assert!(debug.contains("output-events"));
    }

    #[test]
    fn test_extract_keys_no_key_column() {
        let sink = KafkaSink::new(test_schema(), test_config(), None);
        let batch = arrow_array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        assert!(sink.extract_keys(&batch).unwrap().is_none());
    }

    #[test]
    fn test_extract_keys_with_key_column() {
        let mut cfg = test_config();
        cfg.key_column = Some("value".into());
        let sink = KafkaSink::new(test_schema(), cfg, None);
        let batch = arrow_array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["key-a", "key-b"])),
            ],
        )
        .unwrap();
        let keys = sink.extract_keys(&batch).unwrap().unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(&keys[0], b"key-a");
        assert_eq!(&keys[1], b"key-b");
    }
}
