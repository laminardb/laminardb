//! Exercise registered factories through the shared resolution boundary.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
#[cfg(any(feature = "websocket", feature = "delta-lake"))]
use laminar_connectors::config::encode_arrow_schema_ipc;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::registry::ConnectorRegistry;
#[cfg(feature = "websocket")]
use laminar_connectors::schema::resolution::SchemaDirection;
use laminar_connectors::schema::resolution::SchemaOrigin;

fn input() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

#[tokio::test]
async fn native_protocol_factories_reject_codec_selection_before_metadata_io() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::generator::register_generator_source(&registry).unwrap();
    #[cfg(feature = "otel")]
    laminar_connectors::otel::register_otel_source(&registry).unwrap();
    #[cfg(feature = "postgres-cdc")]
    laminar_connectors::postgres::cdc::register_postgres_cdc_source(&registry).unwrap();
    #[cfg(feature = "postgres-sink")]
    laminar_connectors::postgres::register_postgres_sink(&registry).unwrap();
    #[cfg(feature = "mongodb-cdc")]
    {
        laminar_connectors::mongodb::register_mongodb_cdc_source(&registry).unwrap();
        laminar_connectors::mongodb::register_mongodb_sink(&registry).unwrap();
    }
    #[cfg(feature = "delta-lake")]
    {
        laminar_connectors::lakehouse::register_delta_lake_source(&registry).unwrap();
        laminar_connectors::lakehouse::register_delta_lake_sink(&registry).unwrap();
    }
    #[cfg(feature = "iceberg")]
    {
        laminar_connectors::lakehouse::register_iceberg_source(&registry).unwrap();
        laminar_connectors::lakehouse::register_iceberg_sink(&registry).unwrap();
    }
    for connector in registry.list_sources() {
        let mut config = ConnectorConfig::new(&connector);
        config.set("format", "avro");
        let error = registry
            .resolve_source_schema(&config, None)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("omit FORMAT"),
            "{connector}: {error}"
        );
        if registry.has_table_source(&connector) {
            let error = registry
                .resolve_table_schema(&config, None)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("omit FORMAT"),
                "{connector}: {error}"
            );
            let error = registry
                .resolve_lookup_schema(&config, None)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("omit FORMAT"),
                "{connector}: {error}"
            );
        }
    }
    for connector in registry.list_sinks() {
        let mut config = ConnectorConfig::new(&connector);
        config.set("format", "avro");
        let error = registry
            .resolve_sink_schema(&config, input())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("omit FORMAT"),
            "{connector}: {error}"
        );
    }
    #[cfg(feature = "mongodb-cdc")]
    {
        let mut config = ConnectorConfig::new("mongodb");
        config.set("format", "avro");
        let error = registry
            .resolve_lookup_schema(&config, None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("omit FORMAT"), "{error}");
    }
}

#[tokio::test]
async fn generator_factory_has_a_fixed_contract_and_rejects_incompatible_overrides() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::generator::register_generator_source(&registry).unwrap();
    let config = ConnectorConfig::new("generator");
    let binding = registry.resolve_source_schema(&config, None).await.unwrap();
    assert_eq!(binding.origin, SchemaOrigin::BuiltIn);
    assert_eq!(binding.logical.field(0).name(), "seq");
    assert!(registry
        .resolve_source_schema(&config, Some(input()))
        .await
        .is_err());
}

#[cfg(feature = "otel")]
#[tokio::test]
async fn otlp_factory_resolves_each_protocol_signal_without_listening() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::otel::register_otel_source(&registry).unwrap();
    for signal in ["traces", "metrics", "logs"] {
        let mut config = ConnectorConfig::new("otel");
        config.set("signals", signal);
        let binding = registry.resolve_source_schema(&config, None).await.unwrap();
        assert_eq!(binding.origin, SchemaOrigin::BuiltIn);
        assert!(!binding.logical.fields().is_empty());
        assert!(registry
            .resolve_source_schema(&config, Some(input()))
            .await
            .is_err());
    }
}

#[cfg(feature = "nats")]
#[tokio::test]
async fn nats_factories_require_declared_json_and_have_builtin_raw_and_query_writers() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::nats::register_nats_source(&registry).unwrap();
    laminar_connectors::nats::register_nats_sink(&registry).unwrap();
    let mut config = ConnectorConfig::new("nats");
    config.set("servers", "nats://127.0.0.1:1");
    config.set("mode", "core");
    config.set("subject", "events");
    assert!(registry.resolve_source_schema(&config, None).await.is_err());
    for format in ["json", "csv"] {
        config.set("format", format);
        let reader = registry
            .resolve_source_schema(&config, Some(input()))
            .await
            .unwrap();
        assert_eq!(reader.logical, *input());
        let writer = registry
            .resolve_sink_schema(&config, input())
            .await
            .unwrap();
        assert_eq!(writer.origin, SchemaOrigin::Query);
    }
    config.set("format", "raw");
    assert_eq!(
        registry
            .resolve_source_schema(&config, None)
            .await
            .unwrap()
            .origin,
        SchemaOrigin::BuiltIn
    );
    assert!(registry
        .resolve_sink_schema(&config, input())
        .await
        .is_err());
    let raw = Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        true,
    )]));
    registry.resolve_sink_schema(&config, raw).await.unwrap();
}

#[cfg(feature = "websocket")]
#[tokio::test]
async fn websocket_factories_validate_reader_codecs_and_both_writer_modes_without_io() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::websocket::register_websocket_source(&registry).unwrap();
    laminar_connectors::websocket::register_websocket_sink(&registry).unwrap();
    let mut config = ConnectorConfig::new("websocket");
    config.set("url", "ws://127.0.0.1:1");
    assert!(registry.resolve_source_schema(&config, None).await.is_err());
    for format in ["json", "csv"] {
        config.set("format", format);
        registry
            .resolve_source_schema(&config, Some(input()))
            .await
            .unwrap();
    }
    let decimal = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Decimal128(10, 2),
        true,
    )]));
    assert!(registry
        .resolve_source_schema(&config, Some(decimal))
        .await
        .is_err());
    config.set("format", "binary");
    for data_type in [DataType::Binary, DataType::LargeBinary] {
        let binary = Arc::new(Schema::new(vec![Field::new("payload", data_type, false)]));
        registry
            .resolve_source_schema(&config, Some(binary))
            .await
            .unwrap();
    }
    assert!(registry
        .resolve_source_schema(&config, Some(input()))
        .await
        .is_err());
    let multiple = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Binary, false),
        Field::new("b", DataType::Binary, false),
    ]));
    assert!(registry
        .resolve_source_schema(&config, Some(multiple))
        .await
        .is_err());
    let mut sink = ConnectorConfig::new("websocket");
    sink.set("mode", "server");
    sink.set("bind.address", "127.0.0.1:0");
    sink.set("_arrow_schema", encode_arrow_schema_ipc(&input()));
    assert_eq!(
        registry
            .resolve_sink_schema(&sink, input())
            .await
            .unwrap()
            .direction,
        SchemaDirection::Sink
    );
    sink.set("mode", "client");
    sink.set("url", "ws://127.0.0.1:1");
    let mut properties = sink.properties().clone();
    properties.remove("bind.address");
    let mut sink = ConnectorConfig::with_properties("websocket", properties);
    registry.resolve_sink_schema(&sink, input()).await.unwrap();
    for mode in ["server", "client"] {
        sink.set("mode", mode);
        for format in ["json", "csv", "binary"] {
            sink.set("format", format);
            assert!(registry
                .resolve_sink_schema(&sink, input())
                .await
                .unwrap_err()
                .to_string()
                .contains("wire format is always JSON"));
        }
    }
}

#[cfg(feature = "kafka")]
#[tokio::test]
async fn kafka_plain_formats_are_distinct_from_registry_discovery() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::kafka::register_kafka_source(&registry).unwrap();
    laminar_connectors::kafka::register_kafka_sink(&registry).unwrap();
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", "127.0.0.1:1");
    config.set("topic", "events");
    config.set("group.id", "conformance");
    for format in ["json", "csv"] {
        config.set("format", format);
        assert!(registry.resolve_source_schema(&config, None).await.is_err());
        assert_eq!(
            registry
                .resolve_source_schema(&config, Some(input()))
                .await
                .unwrap()
                .logical,
            *input()
        );
        registry
            .resolve_sink_schema(&config, input())
            .await
            .unwrap();
    }
    config.set("format", "raw");
    assert_eq!(
        registry
            .resolve_source_schema(&config, None)
            .await
            .unwrap()
            .origin,
        SchemaOrigin::BuiltIn
    );
    assert!(registry
        .resolve_sink_schema(&config, input())
        .await
        .is_err());
    config.set("format", "json");
    config.set("schema.registry.url", "http://127.0.0.1:1");
    assert!(registry
        .resolve_source_schema(&config, Some(input()))
        .await
        .unwrap_err()
        .to_string()
        .contains("FORMAT AVRO"));
}

#[cfg(feature = "files")]
#[tokio::test]
async fn file_factories_resolve_embedded_schema_and_query_writers_without_consuming_rows() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::files::register_file_source(&registry).unwrap();
    laminar_connectors::files::register_file_sink(&registry).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, input(), None).unwrap();
    writer
        .write(&arrow_array::RecordBatch::new_empty(input()))
        .unwrap();
    writer.close().unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let mut config = ConnectorConfig::new("files");
    config.set("path", dir.path().to_str().unwrap());
    config.set("format", "parquet");
    config.set("include_metadata", "false");
    let binding = registry.resolve_source_schema(&config, None).await.unwrap();
    assert_eq!(binding.logical, *input());
    assert_eq!(binding.origin, SchemaOrigin::Metadata);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    config.set("path", dir.path().join("new-output").to_str().unwrap());
    for format in ["json", "csv", "parquet", "arrow"] {
        config.set("format", format);
        let binding = registry
            .resolve_sink_schema(&config, input())
            .await
            .unwrap();
        assert_eq!(binding.origin, SchemaOrigin::Query);
        assert!(!dir.path().join("new-output").exists());
    }
    config.set("format", "text");
    assert_eq!(
        registry
            .resolve_source_schema(&config, None)
            .await
            .unwrap()
            .origin,
        SchemaOrigin::BuiltIn
    );
}

#[cfg(feature = "files")]
#[tokio::test]
async fn file_sampling_is_opt_in_deterministic_and_rejects_inconclusive_samples() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::files::register_file_source(&registry).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.jsonl");
    std::fs::write(&path, b"{\"id\":1}\n{\"id\":2}\n").unwrap();
    let mut config = ConnectorConfig::new("files");
    config.set("path", dir.path().to_str().unwrap());
    config.set("format", "json");
    config.set("include_metadata", "false");
    assert!(registry.resolve_source_schema(&config, None).await.is_err());
    config.set("schema.inference", "true");
    let binding = registry.resolve_source_schema(&config, None).await.unwrap();
    assert_eq!(binding.origin, SchemaOrigin::Sample);
    assert_eq!(binding.logical.field(0).data_type(), &DataType::Int64);
    std::fs::write(&path, b"{\"id\":null}\n").unwrap();
    assert!(registry.resolve_source_schema(&config, None).await.is_err());
    std::fs::write(&path, b"").unwrap();
    assert!(registry.resolve_source_schema(&config, None).await.is_err());
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn delta_factories_separate_readonly_resolution_creation_and_resource_identity() {
    let registry = ConnectorRegistry::new();
    laminar_connectors::lakehouse::register_delta_lake_source(&registry).unwrap();
    laminar_connectors::lakehouse::register_delta_lake_sink(&registry).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("table");
    let mut config = ConnectorConfig::new("delta-lake");
    config.set("table.path", path.to_str().unwrap());
    config.set("auto.create", "true");
    config.set("_arrow_schema", encode_arrow_schema_ipc(&input()));
    let mut binding = registry
        .resolve_sink_schema(&config, input())
        .await
        .unwrap();
    assert!(binding.value.is_none());
    assert!(!path.join("_delta_log").exists());
    let mut sink = registry.create_sink(&config, None).unwrap();
    sink.prepare_schema(&config, &mut binding).await.unwrap();
    assert!(binding.value.is_some());
    let reader = registry.resolve_source_schema(&config, None).await.unwrap();
    assert_eq!(
        reader.logical,
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("__weight", DataType::Int64, false),
        ])
    );
    let explicit = registry
        .resolve_source_schema(&config, Some(input()))
        .await
        .unwrap();
    assert_eq!(explicit.logical, *input());
    assert_eq!(
        reader.value.as_ref().unwrap().identity,
        binding.value.as_ref().unwrap().identity
    );
    let reference = registry.resolve_table_schema(&config, None).await.unwrap();
    assert_eq!(reference.logical, *input());
    let lookup = registry.resolve_lookup_schema(&config, None).await.unwrap();
    assert_eq!(lookup.logical, *input());
    let mut committed = config.clone();
    committed.set_schema_binding(binding).unwrap();
    let mut read_config = config.clone();
    read_config.set_schema_binding(reader.clone()).unwrap();
    std::fs::rename(&path, dir.path().join("original-table")).unwrap();
    let mut replacement = registry
        .resolve_sink_schema(&config, input())
        .await
        .unwrap();
    let mut replacement_sink = registry.create_sink(&config, None).unwrap();
    replacement_sink
        .prepare_schema(&config, &mut replacement)
        .await
        .unwrap();
    assert_ne!(
        replacement.value.as_ref().unwrap().identity["table_id"],
        reader.value.as_ref().unwrap().identity["table_id"]
    );
    let mut stale_sink = registry.create_sink(&committed, None).unwrap();
    assert!(stale_sink.open(&committed).await.is_err());
    let mut stale_source = registry.create_source(&read_config, None).unwrap();
    assert!(stale_source
        .start(
            laminar_connectors::connector::SourceStart::new(
                read_config,
                laminar_connectors::connector::SourcePosition::Initial,
                laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap()
        )
        .await
        .is_err());
    let invalid = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    config.set("table.path", dir.path().join("invalid").to_str().unwrap());
    assert!(registry
        .resolve_sink_schema(&config, invalid)
        .await
        .is_err());
    assert!(!dir.path().join("invalid/_delta_log").exists());
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn delta_native_writer_mapping_and_frozen_reader_follow_new_data_versions() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use laminar_connectors::connector::{DeliveryGuarantee, SourcePosition, SourceStart};

    let registry = ConnectorRegistry::new();
    laminar_connectors::lakehouse::register_delta_lake_source(&registry).unwrap();
    laminar_connectors::lakehouse::register_delta_lake_sink(&registry).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let native = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let query = Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let mut config = ConnectorConfig::new("delta-lake");
    config.set(
        "table.path",
        directory.path().join("table").to_str().unwrap(),
    );
    config.set("auto.create", "true");
    config.set("poll.interval.ms", "0");
    let mut pending = registry.resolve_sink_schema(&config, native).await.unwrap();
    registry
        .create_sink(&config, None)
        .unwrap()
        .prepare_schema(&config, &mut pending)
        .await
        .unwrap();
    let reader_binding = registry.resolve_source_schema(&config, None).await.unwrap();
    let writer_binding = registry
        .resolve_sink_schema(&config, query.clone())
        .await
        .unwrap();
    assert_eq!(writer_binding.logical, *query);
    assert_eq!(writer_binding.mapping[0].external, "label");
    let mut read_config = config.clone();
    read_config
        .set_schema_binding(reader_binding.clone())
        .unwrap();
    let mut reader = registry.create_source(&read_config, None).unwrap();
    reader
        .start(
            SourceStart::new(
                read_config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    config.set("_arrow_schema", encode_arrow_schema_ipc(&query));
    config.set_schema_binding(writer_binding).unwrap();
    let mut writer = registry.create_sink(&config, None).unwrap();
    writer.open(&config).await.unwrap();
    for id in [73, 74] {
        let batch = RecordBatch::try_new(
            query.clone(),
            vec![
                Arc::new(StringArray::from(vec!["named"])),
                Arc::new(Int64Array::from(vec![id])),
            ],
        )
        .unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.flush().await.unwrap();
        let output = reader.poll_batch(16).await.unwrap().unwrap().records;
        assert_eq!(output.schema().as_ref(), &reader_binding.logical);
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            id
        );
        assert_eq!(
            output
                .column_by_name("label")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "named"
        );
    }
    writer.close().await.unwrap();
    reader.close().await.unwrap();
}
