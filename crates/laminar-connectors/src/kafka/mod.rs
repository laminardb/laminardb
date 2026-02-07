//! Kafka source and sink connectors for LaminarDB.
//!
//! Provides a `KafkaSource` that consumes from Kafka topics and
//! produces Arrow `RecordBatch` data through the [`SourceConnector`]
//! trait, and a `KafkaSink` that writes Arrow `RecordBatch` data
//! to Kafka topics through the [`SinkConnector`] trait.
//!
//! Both connectors support JSON, CSV, Raw, Debezium, and Avro
//! formats, with full Confluent Schema Registry integration for Avro.
//!
//! # Features
//!
//! - Per-partition offset tracking with checkpoint/restore (source)
//! - At-least-once and exactly-once delivery (sink)
//! - Confluent Schema Registry with caching and compatibility checking
//! - Avro serialization/deserialization via `arrow-avro` (Confluent wire format)
//! - Configurable partitioning: key-hash, round-robin, sticky (sink)
//! - Backpressure control with high/low watermark hysteresis (source)
//! - Consumer group rebalance tracking (source)
//! - Dead letter queue for failed records (sink)
//! - Atomic metrics counters
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_connectors::kafka::{KafkaSource, KafkaSourceConfig};
//! use laminar_connectors::kafka::{KafkaSink, KafkaSinkConfig};
//!
//! // Source
//! let config = KafkaSourceConfig::from_config(&connector_config)?;
//! let source = KafkaSource::new(schema, config);
//!
//! // Sink
//! let config = KafkaSinkConfig::from_config(&connector_config)?;
//! let sink = KafkaSink::new(schema, config);
//! ```
//!
//! [`SourceConnector`]: crate::connector::SourceConnector
//! [`SinkConnector`]: crate::connector::SinkConnector

// Source modules
pub mod avro;
pub mod backpressure;
pub mod config;
pub mod metrics;
pub mod offsets;
pub mod rebalance;
pub mod source;
pub mod watermarks;

// Sink modules
pub mod avro_serializer;
pub mod partitioner;
pub mod sink;
pub mod sink_config;
pub mod sink_metrics;

// Shared modules
pub mod schema_registry;

// Source re-exports
pub use avro::AvroDeserializer;
pub use config::{
    AssignmentStrategy, CompatibilityLevel, IsolationLevel, KafkaSourceConfig, OffsetReset,
    SaslMechanism, SecurityProtocol, SrAuth, StartupMode, TopicSubscription,
};
pub use metrics::KafkaSourceMetrics;
pub use offsets::OffsetTracker;
pub use source::KafkaSource;
pub use watermarks::{
    AlignmentCheckResult, KafkaAlignmentConfig, KafkaAlignmentMode, KafkaWatermarkTracker,
    WatermarkMetrics, WatermarkMetricsSnapshot,
};

// Sink re-exports
pub use avro_serializer::AvroSerializer;
pub use partitioner::{
    KafkaPartitioner, KeyHashPartitioner, RoundRobinPartitioner, StickyPartitioner,
};
pub use sink::KafkaSink;
pub use sink_config::{
    Acks, CompressionType, DeliveryGuarantee, KafkaSinkConfig, PartitionStrategy,
};
pub use sink_metrics::KafkaSinkMetrics;

// Shared re-exports
pub use schema_registry::{CachedSchema, CompatibilityResult, SchemaRegistryClient, SchemaType};

use std::sync::Arc;

use crate::config::{ConfigKeySpec, ConnectorInfo};
use crate::registry::ConnectorRegistry;

/// Registers the Kafka source connector with the given registry.
///
/// After registration, the runtime can instantiate `KafkaSource` by
/// name when processing `CREATE SOURCE ... WITH (connector = 'kafka')`.
pub fn register_kafka_source(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "kafka".to_string(),
        display_name: "Apache Kafka Source".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: true,
        is_sink: false,
        config_keys: kafka_source_config_keys(),
    };

    registry.register_source(
        "kafka",
        info,
        Arc::new(|| {
            use arrow_schema::{DataType, Field, Schema};

            // Default schema — will be overridden during open() or via SQL DDL.
            let default_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]));
            Box::new(KafkaSource::new(
                default_schema,
                KafkaSourceConfig::default(),
            ))
        }),
    );
}

/// Registers the Kafka sink connector with the given registry.
///
/// After registration, the runtime can instantiate `KafkaSink` by
/// name when processing `CREATE SINK ... WITH (connector = 'kafka')`.
pub fn register_kafka_sink(registry: &ConnectorRegistry) {
    let info = ConnectorInfo {
        name: "kafka".to_string(),
        display_name: "Apache Kafka Sink".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        is_source: false,
        is_sink: true,
        config_keys: kafka_sink_config_keys(),
    };

    registry.register_sink(
        "kafka",
        info,
        Arc::new(|| {
            use arrow_schema::{DataType, Field, Schema};

            // Default schema — will be overridden during open() or via SQL DDL.
            let default_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]));
            Box::new(KafkaSink::new(default_schema, KafkaSinkConfig::default()))
        }),
    );
}

/// Returns the configuration key specifications for the Kafka source.
#[allow(clippy::too_many_lines)]
fn kafka_source_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        // Required
        ConfigKeySpec::required("bootstrap.servers", "Kafka broker addresses"),
        ConfigKeySpec::required("group.id", "Consumer group identifier"),
        ConfigKeySpec::required("topic", "Comma-separated list of topics"),
        // Topic subscription (alternative to 'topic')
        ConfigKeySpec::optional("topic.pattern", "Regex pattern for topic subscription", ""),
        // Format
        ConfigKeySpec::optional("format", "Data format (json/csv/avro/raw/debezium)", "json"),
        // Security
        ConfigKeySpec::optional(
            "security.protocol",
            "Security protocol (plaintext/ssl/sasl_plaintext/sasl_ssl)",
            "plaintext",
        ),
        ConfigKeySpec::optional(
            "sasl.mechanism",
            "SASL mechanism (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512/GSSAPI/OAUTHBEARER)",
            "",
        ),
        ConfigKeySpec::optional("sasl.username", "SASL username for PLAIN/SCRAM", ""),
        ConfigKeySpec::optional("sasl.password", "SASL password for PLAIN/SCRAM", ""),
        ConfigKeySpec::optional("ssl.ca.location", "SSL CA certificate file path", ""),
        ConfigKeySpec::optional(
            "ssl.certificate.location",
            "Client SSL certificate file path",
            "",
        ),
        ConfigKeySpec::optional("ssl.key.location", "Client SSL private key file path", ""),
        ConfigKeySpec::optional("ssl.key.password", "Password for encrypted SSL key", ""),
        // Consumer tuning
        ConfigKeySpec::optional(
            "startup.mode",
            "Startup mode (group-offsets/earliest/latest)",
            "group-offsets",
        ),
        ConfigKeySpec::optional(
            "startup.specific.offsets",
            "Start from specific offsets (format: 'partition:offset,...')",
            "",
        ),
        ConfigKeySpec::optional(
            "startup.timestamp.ms",
            "Start from timestamp (milliseconds since epoch)",
            "",
        ),
        ConfigKeySpec::optional(
            "auto.offset.reset",
            "Fallback when no committed offset (earliest/latest/none)",
            "earliest",
        ),
        ConfigKeySpec::optional(
            "isolation.level",
            "Transaction isolation (read_uncommitted/read_committed)",
            "read_committed",
        ),
        ConfigKeySpec::optional("max.poll.records", "Max records per poll", "1000"),
        ConfigKeySpec::optional("poll.timeout.ms", "Poll timeout in milliseconds", "100"),
        ConfigKeySpec::optional("commit.interval.ms", "Offset commit interval", "5000"),
        ConfigKeySpec::optional(
            "partition.assignment.strategy",
            "Partition assignment (range/roundrobin/cooperative-sticky)",
            "range",
        ),
        // Fetch tuning
        ConfigKeySpec::optional("fetch.min.bytes", "Minimum bytes per fetch request", "1"),
        ConfigKeySpec::optional(
            "fetch.max.bytes",
            "Maximum bytes per fetch request",
            "52428800",
        ),
        ConfigKeySpec::optional(
            "fetch.max.wait.ms",
            "Max wait time for fetch.min.bytes",
            "500",
        ),
        ConfigKeySpec::optional(
            "max.partition.fetch.bytes",
            "Max bytes per partition per fetch",
            "1048576",
        ),
        // Metadata
        ConfigKeySpec::optional(
            "include.metadata",
            "Include _partition/_offset/_timestamp columns",
            "false",
        ),
        ConfigKeySpec::optional("include.headers", "Include _headers column", "false"),
        ConfigKeySpec::optional(
            "event.time.column",
            "Column name for event time extraction",
            "",
        ),
        // Watermark
        ConfigKeySpec::optional(
            "max.out.of.orderness.ms",
            "Max out-of-orderness for watermarks",
            "5000",
        ),
        ConfigKeySpec::optional("idle.timeout.ms", "Idle partition timeout", "30000"),
        ConfigKeySpec::optional(
            "enable.watermark.tracking",
            "Enable per-partition watermark tracking (F064)",
            "false",
        ),
        ConfigKeySpec::optional(
            "alignment.group.id",
            "Alignment group ID for multi-source coordination (F066)",
            "",
        ),
        ConfigKeySpec::optional(
            "alignment.max.drift.ms",
            "Maximum allowed drift between sources in alignment group",
            "",
        ),
        ConfigKeySpec::optional(
            "alignment.mode",
            "Alignment enforcement mode (pause/warn-only/drop-excess)",
            "pause",
        ),
        // Backpressure
        ConfigKeySpec::optional(
            "backpressure.high.watermark",
            "Channel fill ratio to pause",
            "0.8",
        ),
        ConfigKeySpec::optional(
            "backpressure.low.watermark",
            "Channel fill ratio to resume",
            "0.5",
        ),
        // Schema Registry
        ConfigKeySpec::optional(
            "schema.registry.url",
            "Confluent Schema Registry URL (required for Avro)",
            "",
        ),
        ConfigKeySpec::optional("schema.registry.username", "Schema Registry username", ""),
        ConfigKeySpec::optional("schema.registry.password", "Schema Registry password", ""),
        ConfigKeySpec::optional(
            "schema.registry.ssl.ca.location",
            "Schema Registry SSL CA cert path",
            "",
        ),
        ConfigKeySpec::optional(
            "schema.registry.ssl.certificate.location",
            "Schema Registry SSL client cert path",
            "",
        ),
        ConfigKeySpec::optional(
            "schema.registry.ssl.key.location",
            "Schema Registry SSL client key path",
            "",
        ),
        ConfigKeySpec::optional(
            "schema.compatibility",
            "Schema compatibility level override",
            "",
        ),
    ]
}

/// Returns the configuration key specifications for the Kafka sink.
fn kafka_sink_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        // Required
        ConfigKeySpec::required("bootstrap.servers", "Kafka broker addresses"),
        ConfigKeySpec::required("topic", "Target Kafka topic"),
        // Format
        ConfigKeySpec::optional("format", "Serialization format (json/csv/avro/raw)", "json"),
        // Security
        ConfigKeySpec::optional(
            "security.protocol",
            "Security protocol (plaintext/ssl/sasl_plaintext/sasl_ssl)",
            "plaintext",
        ),
        ConfigKeySpec::optional(
            "sasl.mechanism",
            "SASL mechanism (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512/GSSAPI/OAUTHBEARER)",
            "",
        ),
        ConfigKeySpec::optional("sasl.username", "SASL username for PLAIN/SCRAM", ""),
        ConfigKeySpec::optional("sasl.password", "SASL password for PLAIN/SCRAM", ""),
        ConfigKeySpec::optional("ssl.ca.location", "SSL CA certificate file path", ""),
        ConfigKeySpec::optional(
            "ssl.certificate.location",
            "Client SSL certificate file path",
            "",
        ),
        ConfigKeySpec::optional("ssl.key.location", "Client SSL private key file path", ""),
        ConfigKeySpec::optional("ssl.key.password", "Password for encrypted SSL key", ""),
        // Delivery & Transactions
        ConfigKeySpec::optional(
            "delivery.guarantee",
            "Delivery guarantee (at-least-once/exactly-once)",
            "at-least-once",
        ),
        ConfigKeySpec::optional(
            "transactional.id",
            "Transactional ID prefix (auto-generated if not set)",
            "",
        ),
        ConfigKeySpec::optional(
            "transaction.timeout.ms",
            "Transaction timeout in milliseconds",
            "60000",
        ),
        ConfigKeySpec::optional("acks", "Acknowledgment level (0/1/all)", "all"),
        ConfigKeySpec::optional(
            "max.in.flight.requests",
            "Max in-flight requests (<=5 for exactly-once)",
            "5",
        ),
        ConfigKeySpec::optional(
            "delivery.timeout.ms",
            "Delivery timeout in milliseconds",
            "120000",
        ),
        // Partitioning
        ConfigKeySpec::optional("key.column", "Column name to use as Kafka message key", ""),
        ConfigKeySpec::optional(
            "partitioner",
            "Partitioning strategy (key-hash/round-robin/sticky)",
            "key-hash",
        ),
        // Batching & Compression
        ConfigKeySpec::optional("linger.ms", "Producer linger time in milliseconds", "5"),
        ConfigKeySpec::optional("batch.size", "Producer batch size in bytes", "16384"),
        ConfigKeySpec::optional("batch.num.messages", "Max messages per batch", "10000"),
        ConfigKeySpec::optional(
            "compression.type",
            "Compression (none/gzip/snappy/lz4/zstd)",
            "none",
        ),
        // Error Handling
        ConfigKeySpec::optional(
            "dlq.topic",
            "Dead letter queue topic for failed records",
            "",
        ),
        ConfigKeySpec::optional(
            "flush.batch.size",
            "Max records to buffer before flushing",
            "1000",
        ),
        // Schema Registry
        ConfigKeySpec::optional(
            "schema.registry.url",
            "Confluent Schema Registry URL (required for Avro)",
            "",
        ),
        ConfigKeySpec::optional("schema.registry.username", "Schema Registry username", ""),
        ConfigKeySpec::optional("schema.registry.password", "Schema Registry password", ""),
        ConfigKeySpec::optional(
            "schema.registry.ssl.ca.location",
            "Schema Registry SSL CA cert path",
            "",
        ),
        ConfigKeySpec::optional(
            "schema.compatibility",
            "Schema compatibility level override",
            "",
        ),
    ]
}

/// Round-trip integration tests: serialize → deserialize → verify identity.
#[cfg(test)]
mod avro_roundtrip_tests {
    use std::sync::Arc;

    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::avro::AvroDeserializer;
    use super::avro_serializer::AvroSerializer;
    use super::schema_registry::arrow_to_avro_schema;
    use crate::serde::{RecordDeserializer, RecordSerializer};

    /// Serializes a batch, deserializes each record, and asserts equality.
    fn roundtrip(batch: &RecordBatch, schema: &SchemaRef) -> RecordBatch {
        let avro_schema_json =
            arrow_to_avro_schema(schema, "roundtrip_test").expect("Arrow→Avro schema");

        let ser = AvroSerializer::new(schema.clone(), 1);
        let records = ser.serialize(batch).expect("serialize");

        let mut deser = AvroDeserializer::new();
        deser
            .register_schema(1, &avro_schema_json)
            .expect("register schema");

        let record_refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
        deser
            .deserialize_batch(&record_refs, schema)
            .expect("deserialize")
    }

    #[test]
    fn test_roundtrip_primitives() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["AAPL", "GOOG", "MSFT"])),
                Arc::new(Float64Array::from(vec![150.0, 2800.0, 300.0])),
            ],
        )
        .unwrap();

        let result = roundtrip(&batch, &schema);
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 3);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);

        let names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "AAPL");
        assert_eq!(names.value(1), "GOOG");
        assert_eq!(names.value(2), "MSFT");

        let prices = result
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((prices.value(0) - 150.0).abs() < f64::EPSILON);
        assert!((prices.value(1) - 2800.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_roundtrip_all_primitive_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Int32Array::from(vec![42, -1])),
                Arc::new(Int64Array::from(vec![100_000_000, -999])),
                Arc::new(Float32Array::from(vec![3.14f32, -0.001f32])),
                Arc::new(Float64Array::from(vec![2.718, 1e10])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let result = roundtrip(&batch, &schema);
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 6);

        let bools = result
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(bools.value(0));
        assert!(!bools.value(1));

        let ints = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ints.value(0), 42);
        assert_eq!(ints.value(1), -1);
    }

    #[test]
    fn test_roundtrip_single_row() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![99])),
                Arc::new(StringArray::from(vec!["single"])),
            ],
        )
        .unwrap();

        let result = roundtrip(&batch, &schema);
        assert_eq!(result.num_rows(), 1);
        let val = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(val.value(0), "single");
    }

    #[test]
    fn test_roundtrip_confluent_wire_format() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let ser = AvroSerializer::new(schema.clone(), 42);
        let records = ser.serialize(&batch).unwrap();

        // Verify Confluent wire format: 0x00 + 4-byte BE schema ID.
        for record in &records {
            assert!(record.len() >= 5, "record too short");
            assert_eq!(record[0], 0x00, "magic byte");
            let schema_id = u32::from_be_bytes([record[1], record[2], record[3], record[4]]);
            assert_eq!(schema_id, 42, "schema ID in header");
        }

        // Also verify round-trip works with schema ID 42.
        let avro_schema_json = arrow_to_avro_schema(&schema, "test").unwrap();
        let mut deser = AvroDeserializer::new();
        deser.register_schema(42, &avro_schema_json).unwrap();

        let record_refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
        let result = deser.deserialize_batch(&record_refs, &schema).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_roundtrip_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema.clone());

        let ser = AvroSerializer::new(schema.clone(), 1);
        let records = ser.serialize(&batch).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_roundtrip_many_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let n = 100;
        let ids: Vec<i64> = (0..n).collect();
        let labels: Vec<String> = (0..n).map(|i| format!("row-{i}")).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .unwrap();

        let result = roundtrip(&batch, &schema);
        assert_eq!(result.num_rows(), n as usize);

        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..n as usize {
            assert_eq!(ids.value(i), i as i64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_kafka_source() {
        let registry = ConnectorRegistry::new();
        register_kafka_source(&registry);

        let sources = registry.list_sources();
        assert!(sources.contains(&"kafka".to_string()));

        let info = registry.source_info("kafka");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "kafka");
        assert!(info.is_source);
        assert!(!info.is_sink);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_factory_creates_source() {
        let registry = ConnectorRegistry::new();
        register_kafka_source(&registry);

        let config = crate::config::ConnectorConfig::new("kafka");
        let source = registry.create_source(&config);
        assert!(source.is_ok());
    }

    #[test]
    fn test_register_kafka_sink() {
        let registry = ConnectorRegistry::new();
        register_kafka_sink(&registry);

        let sinks = registry.list_sinks();
        assert!(sinks.contains(&"kafka".to_string()));

        let info = registry.sink_info("kafka");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.name, "kafka");
        assert!(!info.is_source);
        assert!(info.is_sink);
        assert!(!info.config_keys.is_empty());
    }

    #[test]
    fn test_factory_creates_sink() {
        let registry = ConnectorRegistry::new();
        register_kafka_sink(&registry);

        let config = crate::config::ConnectorConfig::new("kafka");
        let sink = registry.create_sink(&config);
        assert!(sink.is_ok());
    }
}
