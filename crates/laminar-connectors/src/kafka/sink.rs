//! Kafka sink connector.

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

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;
use crate::serde::{self, Format, RecordSerializer};

use super::avro_serializer::AvroSerializer;
use super::partitioner::{
    KafkaPartitioner, KeyHashPartitioner, RoundRobinPartitioner, StickyPartitioner,
};
use super::schema_registry::SchemaRegistryClient;
use super::sink_config::{KafkaSinkConfig, PartitionStrategy};
use super::sink_metrics::KafkaSinkMetrics;
use crate::connector::DeliveryGuarantee;

/// Fallback partition count used only when broker metadata query fails.
const FALLBACK_PARTITION_COUNT: i32 = 1;

/// Short deadline used by `SinkConnector::flush` — kept well below any
/// outer tokio timeout so a `spawn_blocking` flush can't outlive its
/// caller and leak a blocking thread. Thorough drains go through
/// `pre_commit` / `commit_epoch` / `close` with their own budgets.
const PERIODIC_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

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
/// 3. For each epoch:
///    - `begin_epoch()` starts a Kafka transaction (exactly-once only)
///    - `write_batch()` serializes and produces records
///    - `commit_epoch()` commits the transaction
/// 4. Call `close()` for clean shutdown
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
    /// Current `LaminarDB` epoch.
    current_epoch: u64,
    /// Last successfully committed epoch.
    last_committed_epoch: u64,
    /// Whether a Kafka transaction is currently active.
    transaction_active: bool,
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
    topic_partition_count: i32,
}

impl KafkaSink {
    /// Creates a new Kafka sink connector with explicit schema.
    ///
    /// # Panics
    ///
    /// Panics if `config.format` is not a supported serialization format.
    /// Call [`KafkaSinkConfig::validate`] first to catch this at config time.
    #[must_use]
    pub fn new(schema: SchemaRef, config: KafkaSinkConfig) -> Self {
        let avro_schema_id = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let serializer =
            select_serializer(config.format, &schema, Arc::clone(&avro_schema_id), None)
                .expect("format validated in KafkaSinkConfig::validate()");
        let partitioner = select_partitioner(config.partitioner);

        Self {
            producer: None,
            config,
            serializer,
            partitioner,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            transaction_active: false,
            dlq_producer: None,
            metrics: KafkaSinkMetrics::new(),
            schema,
            schema_registry: None,
            avro_schema_id,
            topic_partition_count: FALLBACK_PARTITION_COUNT,
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

        Self {
            producer: None,
            config,
            serializer,
            partitioner,
            state: ConnectorState::Created,
            current_epoch: 0,
            last_committed_epoch: 0,
            transaction_active: false,
            dlq_producer: None,
            metrics: KafkaSinkMetrics::new(),
            schema,
            schema_registry: Some(sr),
            avro_schema_id,
            topic_partition_count: FALLBACK_PARTITION_COUNT,
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

    /// Active epoch (incremented by checkpoint coordinator).
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Last epoch that was successfully committed to Kafka.
    #[must_use]
    pub fn last_committed_epoch(&self) -> u64 {
        self.last_committed_epoch
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
                    .await
                    .map_err(|e| {
                        ConnectorError::ConnectionFailed(format!(
                            "failed to register Avro schema for '{subject}': {e}"
                        ))
                    })?;
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
            // For non-string columns, use the Arrow array formatter.
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
            // Reusable string buffer for formatted values.
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

    /// Routes a failed record to the dead letter queue.
    async fn route_to_dlq(
        &self,
        payload: &[u8],
        key: Option<&[u8]>,
        error_msg: &str,
    ) -> Result<(), ConnectorError> {
        let dlq_producer = self
            .dlq_producer
            .as_ref()
            .ok_or_else(|| ConnectorError::ConfigurationError("DLQ topic not configured".into()))?;
        let dlq_topic =
            self.config.dlq_topic.as_ref().ok_or_else(|| {
                ConnectorError::ConfigurationError("DLQ topic not configured".into())
            })?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| {
                tracing::warn!("system clock before Unix epoch — using 0 for DLQ timestamp");
                std::time::Duration::ZERO
            })
            .as_millis()
            .to_string();
        let epoch_str = self.current_epoch.to_string();

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
            })
            .insert(rdkafka::message::Header {
                key: "__dlq.epoch",
                value: Some(epoch_str.as_bytes()),
            });

        let mut record = FutureRecord::to(dlq_topic)
            .payload(payload)
            .headers(headers);

        if let Some(k) = key {
            record = record.key(k);
        }

        dlq_producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| ConnectorError::WriteError(format!("DLQ send failed: {e}")))?;

        self.metrics.record_dlq();
        Ok(())
    }

    /// Synchronously enqueue a record with a short retry on `QueueFull`.
    /// Uses `send_result` rather than `send`, because the latter is
    /// `async fn` in rdkafka 0.39+ and only enqueues when polled — which
    /// would defeat the Vec-of-futures pipelining in `write_batch`.
    async fn enqueue_with_queue_retry(
        producer: &FutureProducer,
        mut record: FutureRecord<'_, [u8], [u8]>,
        queue_timeout: Duration,
    ) -> Result<DeliveryFuture, ConnectorError> {
        let start = Instant::now();
        loop {
            match producer.send_result(record) {
                Ok(fut) => return Ok(fut),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r))
                    if start.elapsed() < queue_timeout =>
                {
                    record = r;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err((e, _)) => {
                    return Err(ConnectorError::WriteError(format!(
                        "Kafka enqueue failed: {e}"
                    )));
                }
            }
        }
    }

    /// Flush on the blocking pool — `Producer::flush()` is a synchronous
    /// FFI call that would stall the sink task's async select loop.
    async fn flush_producer_async(
        producer: &FutureProducer,
        timeout: Duration,
    ) -> Result<(), rdkafka::error::KafkaError> {
        let p = producer.clone();
        match tokio::task::spawn_blocking(move || p.flush(timeout)).await {
            Ok(result) => result,
            Err(join_err) => {
                warn!("flush blocking task failed: {join_err}");
                Err(rdkafka::error::KafkaError::Canceled)
            }
        }
    }

    /// Run a blocking producer operation on the thread pool. All
    /// librdkafka transaction calls (`begin_transaction`,
    /// `commit_transaction`, `abort_transaction`) are synchronous FFI.
    async fn producer_blocking<F, R>(producer: &FutureProducer, op: F) -> R
    where
        F: FnOnce(&FutureProducer) -> R + Send + 'static,
        R: Send + 'static,
    {
        let p = producer.clone();
        tokio::task::spawn_blocking(move || op(&p))
            .await
            .expect("producer_blocking: blocking task panicked")
    }
}

#[async_trait]
#[allow(clippy::too_many_lines)]
impl SinkConnector for KafkaSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
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

        info!(
            brokers = %self.config.bootstrap_servers,
            topic = %self.config.topic,
            format = %self.config.format,
            delivery = %self.config.delivery_guarantee,
            "opening Kafka sink connector"
        );

        // Build rdkafka producer.
        let rdkafka_config: ClientConfig = self.config.to_rdkafka_config();
        let producer: FutureProducer = rdkafka_config.create().map_err(|e| {
            ConnectorError::ConnectionFailed(format!("failed to create producer: {e}"))
        })?;

        // Initialize transactions if exactly-once.
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            producer
                .init_transactions(self.config.transaction_timeout)
                .map_err(|e| {
                    ConnectorError::TransactionError(format!("failed to init transactions: {e}"))
                })?;
        }

        // Create DLQ producer if configured. Inherits security settings
        // (SASL, SSL) from the main producer config but is non-transactional.
        // DLQ records bypass the exactly-once transaction to avoid coupling
        // error routing with the main data path.
        if self.config.dlq_topic.is_some() {
            let dlq_config = self.config.to_dlq_rdkafka_config();
            let dlq_producer: FutureProducer = dlq_config.create().map_err(|e| {
                ConnectorError::ConnectionFailed(format!("failed to create DLQ producer: {e}"))
            })?;
            self.dlq_producer = Some(dlq_producer);
        }

        // Initialize Schema Registry client if configured.
        if let Some(ref url) = self.config.schema_registry_url {
            if self.schema_registry.is_none() {
                let sr = if let Some(ref ca_path) = self.config.schema_registry_ssl_ca_location {
                    SchemaRegistryClient::with_tls(
                        url,
                        self.config.schema_registry_auth.clone(),
                        ca_path,
                    )?
                } else {
                    SchemaRegistryClient::new(url, self.config.schema_registry_auth.clone())
                };
                self.schema_registry = Some(Arc::new(sr));
            }
        }

        // Set SR compatibility level if configured.
        // Schema registration is deferred to first write_batch() where the
        // real pipeline output schema is known (the factory default is a
        // placeholder that would pollute the registry and break compat checks).
        if self.config.format == Format::Avro {
            if let Some(ref sr) = self.schema_registry {
                if let Some(ref compat) = self.config.schema_compatibility {
                    let subject = format!("{}-value", self.config.topic);
                    sr.set_compatibility_level(&subject, *compat)
                        .await
                        .map_err(|e| {
                            ConnectorError::ConnectionFailed(format!(
                                "failed to set SR compatibility for '{subject}': {e}"
                            ))
                        })?;
                }
            }
        }

        // Query broker metadata for actual topic partition count.
        // Reset to fallback first so a reopened sink doesn't keep a stale count.
        self.topic_partition_count = FALLBACK_PARTITION_COUNT;
        match producer
            .client()
            .fetch_metadata(Some(&self.config.topic), Duration::from_secs(5))
        {
            Ok(metadata) => {
                if let Some(topic_meta) = metadata.topics().first() {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    let count = topic_meta.partitions().len() as i32;
                    if count > 0 {
                        self.topic_partition_count = count;
                        info!(
                            topic = %self.config.topic,
                            partitions = count,
                            "queried topic partition count from broker"
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    topic = %self.config.topic,
                    error = %e,
                    fallback = FALLBACK_PARTITION_COUNT,
                    "failed to query topic metadata — using fallback partition count"
                );
            }
        }

        self.producer = Some(producer);
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

        self.ensure_schema_ready(&batch.schema()).await?;

        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "producer initialized".into(),
                actual: "producer is None".into(),
            })?;

        // Serialize the RecordBatch into per-row byte payloads.
        let payloads = self.serializer.serialize(batch).map_err(|e| {
            self.metrics.record_serialization_error();
            ConnectorError::Serde(e)
        })?;

        // Extract keys if key column is configured.
        let keys = self.extract_keys(batch)?;

        let mut records_written: usize = 0;
        let mut bytes_written: u64 = 0;

        // Phase 1: enqueue every record into librdkafka's internal queue.
        // Flush every flush_batch_size records to bound in-flight memory.
        let flush_threshold = self.config.flush_batch_size;
        let mut delivery_futures = Vec::with_capacity(payloads.len());
        for (i, payload) in payloads.iter().enumerate() {
            let key: Option<&[u8]> = keys.as_ref().map(|kb| kb.key(i)).filter(|k| !k.is_empty());
            let partition = self.partitioner.partition(key, self.topic_partition_count);

            let mut record = FutureRecord::to(&self.config.topic).payload(payload.as_slice());
            if let Some(k) = key {
                record = record.key(k);
            }
            if let Some(p) = partition {
                record = record.partition(p);
            }

            // 500ms matches the old `send(record, 500ms)` contract: ride
            // out transient QueueFull bursts before giving up.
            let fut = Self::enqueue_with_queue_retry(producer, record, Duration::from_millis(500))
                .await?;
            delivery_futures.push((Instant::now(), fut));

            if flush_threshold > 0 && (i + 1) % flush_threshold == 0 {
                Self::flush_producer_async(producer, self.config.delivery_timeout)
                    .await
                    .map_err(|e| ConnectorError::WriteError(format!("flush failed: {e}")))?;
            }
        }

        // Phase 2: await each delivery report. Outer Err = oneshot canceled
        // (producer dropped); inner Err = Kafka delivery error.
        let mut failed: usize = 0;
        let mut first_error: Option<String> = None;
        for (i, (send_time, future)) in delivery_futures.into_iter().enumerate() {
            let err_msg = match future.await {
                Ok(Ok(_)) => {
                    let latency_us = send_time.elapsed().as_micros() as u64;
                    self.metrics.record_produce_latency(latency_us);
                    records_written += 1;
                    bytes_written += payloads[i].len() as u64;
                    continue;
                }
                Ok(Err((err, _))) => err.to_string(),
                Err(_canceled) => "delivery canceled — producer dropped before ack".into(),
            };

            self.metrics.record_error();
            failed += 1;
            if first_error.is_none() {
                first_error = Some(err_msg.clone());
            }

            if self.dlq_producer.is_some() {
                let key: Option<&[u8]> =
                    keys.as_ref().map(|kb| kb.key(i)).filter(|k| !k.is_empty());
                if let Err(dlq_err) = self.route_to_dlq(&payloads[i], key, &err_msg).await {
                    warn!(
                        original_error = %err_msg,
                        dlq_error = %dlq_err,
                        "failed to route record to DLQ — record lost"
                    );
                }
            }
        }

        self.metrics
            .record_write(records_written as u64, bytes_written);

        debug!(
            records = records_written,
            bytes = bytes_written,
            failed,
            "wrote batch to Kafka"
        );

        // With DLQ, failures are already routed — report success. Without,
        // surface the aggregate so the sink task can poison the epoch.
        if failed > 0 && self.dlq_producer.is_none() {
            return Err(ConnectorError::WriteError(format!(
                "Kafka produce: {failed}/{} records failed, first error: {}",
                payloads.len(),
                first_error.unwrap_or_else(|| "unknown".into())
            )));
        }

        Ok(WriteResult::new(records_written, bytes_written))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch = epoch;

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            let producer = self
                .producer
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "Running".into(),
                    actual: self.state.to_string(),
                })?;

            if self.transaction_active {
                warn!(epoch, "aborting stale transaction before new epoch");
                let txn_timeout = self.config.transaction_timeout;
                Self::producer_blocking(producer, move |p| p.abort_transaction(txn_timeout))
                    .await
                    .map_err(|e| {
                        ConnectorError::TransactionError(format!(
                            "cannot begin epoch {epoch}: abort of stale transaction failed: {e}"
                        ))
                    })?;
                self.transaction_active = false;
            }

            Self::producer_blocking(producer, FutureProducer::begin_transaction)
                .await
                .map_err(|e| {
                    ConnectorError::TransactionError(format!(
                        "failed to begin transaction for epoch {epoch}: {e}"
                    ))
                })?;

            self.transaction_active = true;
        }

        self.partitioner.reset();
        debug!(epoch, "began epoch");
        Ok(())
    }

    async fn pre_commit(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if epoch != self.current_epoch {
            return Err(ConnectorError::TransactionError(format!(
                "epoch mismatch in pre_commit: expected {}, got {epoch}",
                self.current_epoch
            )));
        }

        // Flush all pending messages to Kafka brokers (phase 1).
        if let Some(ref producer) = self.producer {
            Self::flush_producer_async(producer, self.config.delivery_timeout)
                .await
                .map_err(|e| {
                    ConnectorError::TransactionError(format!(
                        "failed to flush before pre-commit for epoch {epoch}: {e}"
                    ))
                })?;
        }

        debug!(epoch, "pre-committed epoch (flushed)");
        Ok(())
    }

    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if epoch != self.current_epoch {
            return Err(ConnectorError::TransactionError(format!(
                "epoch mismatch: expected {}, got {epoch}",
                self.current_epoch
            )));
        }

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            let producer = self
                .producer
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "Running".into(),
                    actual: self.state.to_string(),
                })?;

            Self::flush_producer_async(producer, self.config.delivery_timeout)
                .await
                .map_err(|e| {
                    ConnectorError::TransactionError(format!("failed to flush before commit: {e}"))
                })?;

            let txn_timeout = self.config.transaction_timeout;
            Self::producer_blocking(producer, move |p| p.commit_transaction(txn_timeout))
                .await
                .map_err(|e| {
                    ConnectorError::TransactionError(format!(
                        "failed to commit transaction for epoch {epoch}: {e}"
                    ))
                })?;

            self.transaction_active = false;
        } else {
            // At-least-once: just flush pending messages.
            if let Some(ref producer) = self.producer {
                Self::flush_producer_async(producer, self.config.delivery_timeout)
                    .await
                    .map_err(|e| {
                        ConnectorError::TransactionError(format!(
                            "failed to flush for epoch {epoch}: {e}"
                        ))
                    })?;
            }
        }

        self.last_committed_epoch = epoch;
        self.metrics.record_commit();
        debug!(epoch, "committed epoch");
        Ok(())
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && self.transaction_active
        {
            let producer = self
                .producer
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "Running".into(),
                    actual: self.state.to_string(),
                })?;

            let txn_timeout = self.config.transaction_timeout;
            Self::producer_blocking(producer, move |p| p.abort_transaction(txn_timeout))
                .await
                .map_err(|e| {
                    ConnectorError::TransactionError(format!(
                        "failed to abort transaction for epoch {epoch}: {e}"
                    ))
                })?;

            self.transaction_active = false;
        }

        self.metrics.record_rollback();
        debug!(epoch, "rolled back epoch");
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => HealthStatus::Healthy,
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

    fn capabilities(&self) -> SinkConnectorCapabilities {
        // Catch stuck-broker scenarios faster than librdkafka's
        // delivery.timeout.ms (default 5min).
        let mut caps = SinkConnectorCapabilities::new(Duration::from_secs(10))
            .with_idempotent()
            .with_partitioned();

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            caps = caps.with_exactly_once().with_two_phase_commit();
        }

        if self.schema_registry.is_some() {
            caps = caps.with_schema_evolution();
        }

        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if let Some(ref producer) = self.producer {
            Self::flush_producer_async(producer, PERIODIC_FLUSH_TIMEOUT)
                .await
                .map_err(|e| ConnectorError::WriteError(format!("flush failed: {e}")))?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        info!("closing Kafka sink connector");

        // Abort any active transaction.
        if self.transaction_active {
            if let Err(e) = self.rollback_epoch(self.current_epoch).await {
                warn!(error = %e, "failed to abort active transaction on close");
            }
        }

        let mut first_err: Option<ConnectorError> = None;

        if let Some(ref producer) = self.producer {
            if let Err(e) = Self::flush_producer_async(producer, Duration::from_secs(30)).await {
                warn!(error = %e, "failed to flush on close");
                first_err.get_or_insert(ConnectorError::WriteError(format!(
                    "flush failed on close: {e}"
                )));
            }
        }

        if let Some(ref dlq) = self.dlq_producer {
            if let Err(e) = Self::flush_producer_async(dlq, Duration::from_secs(10)).await {
                warn!(error = %e, "failed to flush DLQ producer on close");
                first_err.get_or_insert(ConnectorError::WriteError(format!(
                    "DLQ flush failed on close: {e}"
                )));
            }
        }

        self.producer = None;
        self.dlq_producer = None;
        self.state = ConnectorState::Closed;
        info!("Kafka sink connector closed");
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("state", &self.state)
            .field("topic", &self.config.topic)
            .field("delivery", &self.config.delivery_guarantee)
            .field("format", &self.config.format)
            .field("current_epoch", &self.current_epoch)
            .field("last_committed_epoch", &self.last_committed_epoch)
            .field("transaction_active", &self.transaction_active)
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

    #[test]
    fn test_new_defaults() {
        let sink = KafkaSink::new(test_schema(), test_config());
        assert_eq!(sink.state(), ConnectorState::Created);
        assert!(sink.producer.is_none());
        assert_eq!(sink.current_epoch(), 0);
        assert_eq!(sink.last_committed_epoch(), 0);
        assert!(!sink.transaction_active);
    }

    #[test]
    fn test_schema_returned() {
        let schema = test_schema();
        let sink = KafkaSink::new(schema.clone(), test_config());
        assert_eq!(sink.schema(), schema);
    }

    #[test]
    fn test_health_check_created() {
        let sink = KafkaSink::new(test_schema(), test_config());
        assert_eq!(sink.health_check(), HealthStatus::Unknown);
    }

    #[test]
    fn test_health_check_running() {
        let mut sink = KafkaSink::new(test_schema(), test_config());
        sink.state = ConnectorState::Running;
        assert_eq!(sink.health_check(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_closed() {
        let mut sink = KafkaSink::new(test_schema(), test_config());
        sink.state = ConnectorState::Closed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
    }

    #[test]
    fn test_metrics_initial() {
        let sink = KafkaSink::new(test_schema(), test_config());
        let m = sink.metrics();
        assert_eq!(m.records_total, 0);
        assert_eq!(m.bytes_total, 0);
        assert_eq!(m.errors_total, 0);
    }

    #[test]
    fn test_capabilities_at_least_once() {
        let sink = KafkaSink::new(test_schema(), test_config());
        let caps = sink.capabilities();
        assert!(!caps.exactly_once);
        assert!(caps.idempotent);
        assert!(caps.partitioned);
        assert!(!caps.schema_evolution);
    }

    #[test]
    fn test_capabilities_exactly_once() {
        let mut cfg = test_config();
        cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
        let sink = KafkaSink::new(test_schema(), cfg);
        let caps = sink.capabilities();
        assert!(caps.exactly_once);
        assert!(caps.idempotent);
        assert!(caps.partitioned);
    }

    #[test]
    fn test_serializer_selection_json() {
        let sink = KafkaSink::new(test_schema(), test_config());
        assert_eq!(sink.serializer.format(), Format::Json);
    }

    #[test]
    fn test_serializer_selection_avro() {
        let mut cfg = test_config();
        cfg.format = Format::Avro;
        let sink = KafkaSink::new(test_schema(), cfg);
        assert_eq!(sink.serializer.format(), Format::Avro);
    }

    #[test]
    fn test_with_schema_registry() {
        let sr = SchemaRegistryClient::new("http://localhost:8081", None);
        let mut cfg = test_config();
        cfg.format = Format::Avro;
        cfg.schema_registry_url = Some("http://localhost:8081".into());

        let sink = KafkaSink::with_schema_registry(test_schema(), cfg, sr);
        assert!(sink.has_schema_registry());
        assert_eq!(sink.serializer.format(), Format::Avro);
        let caps = sink.capabilities();
        assert!(caps.schema_evolution);
    }

    #[test]
    fn test_debug_output() {
        let sink = KafkaSink::new(test_schema(), test_config());
        let debug = format!("{sink:?}");
        assert!(debug.contains("KafkaSink"));
        assert!(debug.contains("output-events"));
    }

    #[test]
    fn test_extract_keys_no_key_column() {
        let sink = KafkaSink::new(test_schema(), test_config());
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
        let sink = KafkaSink::new(test_schema(), cfg);
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
