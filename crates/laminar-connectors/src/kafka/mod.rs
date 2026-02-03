//! Kafka source and sink connectors for LaminarDB.
//!
//! Provides a [`KafkaSource`] that consumes from Kafka topics and
//! produces Arrow `RecordBatch` data through the [`SourceConnector`]
//! trait, and a [`KafkaSink`] that writes Arrow `RecordBatch` data
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
pub use config::{AssignmentStrategy, CompatibilityLevel, KafkaSourceConfig, OffsetReset, SrAuth};
pub use metrics::KafkaSourceMetrics;
pub use offsets::OffsetTracker;
pub use source::KafkaSource;

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
fn kafka_source_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("bootstrap.servers", "Kafka broker addresses"),
        ConfigKeySpec::required("group.id", "Consumer group identifier"),
        ConfigKeySpec::required("topic", "Comma-separated list of topics"),
        ConfigKeySpec::optional("format", "Data format (json/csv/avro/raw/debezium)", "json"),
        ConfigKeySpec::optional(
            "auto.offset.reset",
            "Where to start (earliest/latest)",
            "earliest",
        ),
        ConfigKeySpec::optional("max.poll.records", "Max records per poll", "1000"),
        ConfigKeySpec::optional("poll.timeout.ms", "Poll timeout in milliseconds", "100"),
        ConfigKeySpec::optional("commit.interval.ms", "Offset commit interval", "5000"),
        ConfigKeySpec::optional(
            "include.metadata",
            "Include _partition/_offset columns",
            "false",
        ),
        ConfigKeySpec::optional(
            "schema.registry.url",
            "Confluent Schema Registry URL (required for Avro)",
            "",
        ),
    ]
}

/// Returns the configuration key specifications for the Kafka sink.
fn kafka_sink_config_keys() -> Vec<ConfigKeySpec> {
    vec![
        ConfigKeySpec::required("bootstrap.servers", "Kafka broker addresses"),
        ConfigKeySpec::required("topic", "Target Kafka topic"),
        ConfigKeySpec::optional("format", "Serialization format (json/csv/avro/raw)", "json"),
        ConfigKeySpec::optional(
            "delivery.guarantee",
            "Delivery guarantee (at-least-once/exactly-once)",
            "at-least-once",
        ),
        ConfigKeySpec::optional("key.column", "Column name to use as Kafka message key", ""),
        ConfigKeySpec::optional(
            "partitioner",
            "Partitioning strategy (key-hash/round-robin/sticky)",
            "key-hash",
        ),
        ConfigKeySpec::optional(
            "compression.type",
            "Compression (none/gzip/snappy/lz4/zstd)",
            "none",
        ),
        ConfigKeySpec::optional("acks", "Acknowledgment level (none/leader/all)", "all"),
        ConfigKeySpec::optional("linger.ms", "Producer linger time in milliseconds", "5"),
        ConfigKeySpec::optional("batch.size", "Producer batch size in bytes", "65536"),
        ConfigKeySpec::optional(
            "schema.registry.url",
            "Confluent Schema Registry URL (required for Avro)",
            "",
        ),
        ConfigKeySpec::optional(
            "dlq.topic",
            "Dead letter queue topic for failed records",
            "",
        ),
    ]
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
