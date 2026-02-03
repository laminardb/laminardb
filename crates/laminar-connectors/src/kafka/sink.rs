//! Kafka sink connector implementation.
//!
//! [`KafkaSink`] implements the [`SinkConnector`] trait, writing Arrow
//! `RecordBatch` data to Kafka topics via rdkafka's `FutureProducer`.
//! Supports at-least-once (idempotent) and exactly-once (transactional)
//! delivery, configurable partitioning, and dead-letter queue routing.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, StringArray};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use tokio::sync::Mutex;
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
use super::sink_config::{DeliveryGuarantee, KafkaSinkConfig, PartitionStrategy};
use super::sink_metrics::KafkaSinkMetrics;

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
    schema_registry: Option<Arc<Mutex<SchemaRegistryClient>>>,
}

impl KafkaSink {
    /// Creates a new Kafka sink connector with explicit schema.
    #[must_use]
    pub fn new(schema: SchemaRef, config: KafkaSinkConfig) -> Self {
        let serializer = select_serializer(config.format, &schema);
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
        }
    }

    /// Creates a new Kafka sink with Schema Registry integration.
    #[must_use]
    pub fn with_schema_registry(
        schema: SchemaRef,
        config: KafkaSinkConfig,
        sr_client: SchemaRegistryClient,
    ) -> Self {
        let sr = Arc::new(Mutex::new(sr_client));
        let serializer: Box<dyn RecordSerializer> = if config.format == Format::Avro {
            // Schema ID 0 as placeholder — will be updated during open()
            // after schema registration with the registry.
            Box::new(AvroSerializer::with_schema_registry(
                schema.clone(),
                0,
                sr.clone(),
            ))
        } else {
            select_serializer(config.format, &schema)
        };

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
        }
    }

    /// Returns the current connector state.
    #[must_use]
    pub fn state(&self) -> ConnectorState {
        self.state
    }

    /// Returns whether a Schema Registry client is configured.
    #[must_use]
    pub fn has_schema_registry(&self) -> bool {
        self.schema_registry.is_some()
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Returns the last committed epoch.
    #[must_use]
    pub fn last_committed_epoch(&self) -> u64 {
        self.last_committed_epoch
    }

    /// Extracts key bytes from the configured key column.
    ///
    /// Returns `None` if no key column is configured.
    fn extract_keys(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Option<Vec<Vec<u8>>>, ConnectorError> {
        let Some(key_col) = &self.config.key_column else {
            return Ok(None);
        };

        let col_idx = batch.schema().index_of(key_col).map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "key column '{key_col}' not found in schema"
            ))
        })?;

        let array = batch.column(col_idx);
        let mut keys = Vec::with_capacity(batch.num_rows());

        // Try to get string values; fall back to display representation.
        if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
            for i in 0..batch.num_rows() {
                if str_array.is_null(i) {
                    keys.push(Vec::new());
                } else {
                    keys.push(str_array.value(i).as_bytes().to_vec());
                }
            }
        } else {
            // For non-string columns, use the Arrow array formatter.
            let formatter = arrow_cast::display::ArrayFormatter::try_new(
                array,
                &arrow_cast::display::FormatOptions::default(),
            )
            .map_err(|e| {
                ConnectorError::Internal(format!(
                    "failed to create array formatter for key column: {e}"
                ))
            })?;
            for i in 0..batch.num_rows() {
                if array.is_null(i) {
                    keys.push(Vec::new());
                } else {
                    keys.push(formatter.value(i).to_string().into_bytes());
                }
            }
        }

        Ok(Some(keys))
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
            .unwrap_or_default()
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
}

#[async_trait]
impl SinkConnector for KafkaSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;

        // Re-parse config if properties provided.
        if !config.properties().is_empty() {
            let parsed = KafkaSinkConfig::from_config(config)?;
            self.config = parsed;
            self.serializer = select_serializer(self.config.format, &self.schema);
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

        // Create DLQ producer if configured.
        if self.config.dlq_topic.is_some() {
            let dlq_producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.bootstrap_servers)
                .set("enable.idempotence", "true")
                .create()
                .map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("failed to create DLQ producer: {e}"))
                })?;
            self.dlq_producer = Some(dlq_producer);
        }

        // Initialize Schema Registry client if configured.
        if let Some(ref url) = self.config.schema_registry_url {
            if self.schema_registry.is_none() {
                self.schema_registry = Some(Arc::new(Mutex::new(SchemaRegistryClient::new(
                    url,
                    self.config.schema_registry_auth.clone(),
                ))));
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

        for (i, payload) in payloads.iter().enumerate() {
            let key: Option<&[u8]> = keys
                .as_ref()
                .map(|k| k[i].as_slice())
                .filter(|k| !k.is_empty());

            // Determine partition.
            // Use a reasonable default for num_partitions when metadata isn't cached.
            let partition = self.partitioner.partition(key, 6);

            // Build the Kafka record.
            let mut record = FutureRecord::to(&self.config.topic).payload(payload);

            if let Some(k) = key {
                record = record.key(k);
            }
            if let Some(p) = partition {
                record = record.partition(p);
            }

            // Send to Kafka (async).
            match producer.send(record, Duration::from_secs(0)).await {
                Ok(_delivery) => {
                    records_written += 1;
                    bytes_written += payload.len() as u64;
                }
                Err((err, _msg)) => {
                    self.metrics.record_error();
                    let err_msg = err.to_string();

                    if self.dlq_producer.is_some() {
                        self.route_to_dlq(payload, key, &err_msg).await?;
                    } else {
                        return Err(ConnectorError::WriteError(format!(
                            "Kafka produce failed: {err_msg}"
                        )));
                    }
                }
            }
        }

        self.metrics
            .record_write(records_written as u64, bytes_written);

        debug!(
            records = records_written,
            bytes = bytes_written,
            "wrote batch to Kafka"
        );

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

            producer.begin_transaction().map_err(|e| {
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

            // Flush all pending messages.
            producer.flush(self.config.delivery_timeout).map_err(|e| {
                ConnectorError::TransactionError(format!("failed to flush before commit: {e}"))
            })?;

            // Commit the Kafka transaction.
            producer
                .commit_transaction(self.config.transaction_timeout)
                .map_err(|e| {
                    ConnectorError::TransactionError(format!(
                        "failed to commit transaction for epoch {epoch}: {e}"
                    ))
                })?;

            self.transaction_active = false;
        } else {
            // At-least-once: just flush pending messages.
            if let Some(ref producer) = self.producer {
                producer.flush(self.config.delivery_timeout).map_err(|e| {
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

            producer
                .abort_transaction(self.config.transaction_timeout)
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
        let mut caps = SinkConnectorCapabilities::default()
            .with_idempotent()
            .with_partitioned();

        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            caps = caps.with_exactly_once();
        }

        if self.schema_registry.is_some() {
            caps = caps.with_schema_evolution();
        }

        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if let Some(ref producer) = self.producer {
            producer
                .flush(self.config.delivery_timeout)
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

        // Flush remaining messages.
        if let Some(ref producer) = self.producer {
            if let Err(e) = producer.flush(Duration::from_secs(30)) {
                warn!(error = %e, "failed to flush on close");
            }
        }

        self.producer = None;
        self.dlq_producer = None;
        self.state = ConnectorState::Closed;
        info!("Kafka sink connector closed");
        Ok(())
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
fn select_serializer(format: Format, schema: &SchemaRef) -> Box<dyn RecordSerializer> {
    match format {
        Format::Avro => {
            // Without schema registry, use schema ID 0 (placeholder).
            Box::new(AvroSerializer::new(schema.clone(), 0))
        }
        other => serde::create_serializer(other)
            .expect("supported format should always create a serializer"),
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
        assert_eq!(keys[0], b"key-a");
        assert_eq!(keys[1], b"key-b");
    }
}
