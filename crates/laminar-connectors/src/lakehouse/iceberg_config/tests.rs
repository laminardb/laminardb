use super::*;

#[test]
fn test_catalog_type_parse() {
    assert_eq!(
        "rest".parse::<IcebergCatalogType>().unwrap(),
        IcebergCatalogType::Rest
    );
    assert!("unknown".parse::<IcebergCatalogType>().is_err());
}

#[test]
fn test_sink_config_from_config() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("compression", "snappy");

    let cfg = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.catalog.catalog_uri, "http://localhost:8181");
    assert_eq!(cfg.catalog.warehouse, "s3://bucket/wh");
    assert_eq!(cfg.catalog.namespace, "prod");
    assert_eq!(cfg.catalog.table_name, "events");
    assert_eq!(cfg.compression, "snappy");
}

#[cfg(feature = "iceberg-core")]
#[test]
fn stable_catalog_identity_is_platform_canonical_and_ignores_rotated_secrets() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.type", "rest");
    config.set("catalog.uri", "https://catalog.example/v1");
    config.set("catalog.warehouse", "s3://warehouse/root");
    config.set("catalog.prefix", "prod");
    config.set("catalog.auth.type", "bearer");
    config.set("catalog.property.token", "first-secret");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("storage.type", "s3");
    config.set("storage.endpoint", "https://objects.example");
    config.set("storage.region", "eu-west-2");
    config.set("storage.property.secret-access-key", "first-storage-secret");

    let first = IcebergSinkConfig::from_config(&config).unwrap();
    let identity = stable_catalog_identity(&first.catalog, &first.storage);
    assert_eq!(
        identity,
        "682af55fe1359d4824fbe34beb29aadf02f2b11da497cd3df1468132ad78cdc4"
    );

    config.set("catalog.property.token", "rotated-secret");
    config.set(
        "storage.property.secret-access-key",
        "rotated-storage-secret",
    );
    let rotated = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(
        identity,
        stable_catalog_identity(&rotated.catalog, &rotated.storage)
    );

    config.set("storage.region", "us-east-1");
    let different = IcebergSinkConfig::from_config(&config).unwrap();
    assert_ne!(
        identity,
        stable_catalog_identity(&different.catalog, &different.storage)
    );
}

#[test]
fn test_source_config_from_config() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "dim_customers");
    config.set("poll.interval.ms", "30000");
    config.set("snapshot.id", "42");
    config.set("read.max.manifest.list.bytes", "8192");
    config.set("read.max.manifest.bytes", "4096");
    config.set("read.max.manifests.per.snapshot", "16");

    let cfg = IcebergSourceConfig::from_config(&config).unwrap();
    assert_eq!(cfg.poll_interval, Duration::from_secs(30));
    assert_eq!(cfg.snapshot_id, Some(42));
    assert_eq!(cfg.max_manifest_list_bytes, 8192);
    assert_eq!(cfg.max_manifest_bytes, 4096);
    assert_eq!(cfg.max_manifests_per_snapshot, 16);
}

#[test]
fn source_metadata_limits_must_be_nonzero() {
    for key in [
        "read.max.manifest.list.bytes",
        "read.max.manifest.bytes",
        "read.max.manifests.per.snapshot",
    ] {
        let mut config = table_definition_config();
        config.set(key, "0");
        let error = IcebergSourceConfig::from_config(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "got: {error}");
    }
}

#[test]
fn programmatic_zero_source_limit_fails_closed() {
    let mut parsed = IcebergSourceConfig::from_config(&table_definition_config()).unwrap();
    parsed.scan_concurrency = 0;
    let error = parsed.validate_read_limits().unwrap_err().to_string();
    assert!(error.contains("read.scan.concurrency"));

    let mut parsed = IcebergSourceConfig::from_config(&table_definition_config()).unwrap();
    parsed.catalog.connect_timeout = Duration::ZERO;
    let error = parsed.validate_read_limits().unwrap_err().to_string();
    assert!(error.contains("catalog.connect_timeout"));
}

#[test]
fn test_missing_required_field() {
    let config = ConnectorConfig::new("iceberg");
    assert!(IcebergSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_defaults() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");

    let cfg = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.compression, "zstd");
    assert!(!cfg.auto_create);
    assert_eq!(cfg.catalog.connect_timeout, Duration::from_secs(10));
}

#[test]
fn catalog_connect_timeout_is_typed_and_validated() {
    let mut config = table_definition_config();
    config.set("catalog.connect_timeout", "250ms");
    let parsed = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(parsed.catalog.connect_timeout, Duration::from_millis(250));

    config.set("catalog.connect_timeout", "0s");
    let error = IcebergSinkConfig::from_config(&config)
        .unwrap_err()
        .to_string();
    assert!(error.contains("catalog.connect_timeout must be greater than zero"));
}

fn table_definition_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("catalog.warehouse", "file:///warehouse");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config
}

#[test]
fn auto_create_table_definition_is_strongly_typed() {
    let mut config = table_definition_config();
    config.set("auto.create", "true");
    config.set("identifier.fields", "id");
    config.set(
        "partition.spec",
        r#"[{"source":"event_time","name":"event_month","transform":"month"}]"#,
    );
    config.set(
        "sort.order",
        r#"[{"source":"id","transform":"bucket[16]","direction":"desc","null_order":"nulls-first"}]"#,
    );
    config.set("table.property.owner", "streaming");

    let parsed = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(parsed.identifier_fields, ["id"]);
    assert_eq!(parsed.partition_spec.len(), 1);
    assert_eq!(parsed.partition_spec[0].transform, IcebergTransform::Month);
    assert_eq!(parsed.sort_order.len(), 1);
    assert_eq!(parsed.sort_order[0].transform, IcebergTransform::Bucket(16));
    assert_eq!(
        parsed
            .initial_table_properties
            .get("owner")
            .map(String::as_str),
        Some("streaming")
    );
}

#[test]
fn table_creation_options_require_auto_create() {
    for (key, value) in [
        ("format.version", "3"),
        (
            "partition.spec",
            r#"[{"source":"id","name":"id","transform":"identity"}]"#,
        ),
        ("table.property.owner", "streaming"),
    ] {
        let mut config = table_definition_config();
        config.set(key, value);
        let error = IcebergSinkConfig::from_config(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains("auto.create=true"), "got: {error}");
    }
}

#[test]
fn malformed_table_definitions_and_codecs_fail_closed() {
    let mut transform = table_definition_config();
    transform.set("auto.create", "true");
    transform.set(
        "partition.spec",
        r#"[{"source":"id","name":"id_bucket","transform":"bucket[0]"}]"#,
    );
    assert!(IcebergSinkConfig::from_config(&transform).is_err());

    let mut codec = table_definition_config();
    codec.set("parquet.compression", "made-up");
    let error = IcebergSinkConfig::from_config(&codec)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unsupported parquet.compression"));
}

#[test]
fn programmatic_writer_limits_fail_closed() {
    let mut parsed = IcebergSinkConfig::from_config(&table_definition_config()).unwrap();
    parsed.max_buffer_rows = 0;
    let error = parsed.validate_writer_limits().unwrap_err().to_string();
    assert!(error.contains("max.buffer.rows must be greater than zero"));

    let mut parsed = IcebergSinkConfig::from_config(&table_definition_config()).unwrap();
    parsed.max_flush_age = Duration::ZERO;
    let error = parsed.validate_writer_limits().unwrap_err().to_string();
    assert!(error.contains("max.flush.age must be greater than zero"));

    let mut parsed = IcebergSinkConfig::from_config(&table_definition_config()).unwrap();
    parsed.max_files_per_checkpoint = ICEBERG_MAX_FILES_PER_CHECKPOINT + 1;
    let error = parsed.validate_writer_limits().unwrap_err().to_string();
    assert!(error.contains("max.files.per.checkpoint must not exceed"));

    let mut parsed = IcebergSinkConfig::from_config(&table_definition_config()).unwrap();
    parsed.max_open_partitions = parsed.max_files_per_checkpoint + 1;
    let error = parsed.validate_writer_limits().unwrap_err().to_string();
    assert!(error.contains("max.open.partitions must not exceed"));

    let mut parsed = IcebergSinkConfig::from_config(&table_definition_config()).unwrap();
    parsed.max_descriptor_bytes = crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES + 1;
    let error = parsed.validate_writer_limits().unwrap_err().to_string();
    assert!(error.contains("max.descriptor.bytes must not exceed"));

    let mut parsed = IcebergSinkConfig::from_config(&table_definition_config()).unwrap();
    parsed.catalog.connect_timeout = Duration::ZERO;
    let error = parsed.validate_writer_limits().unwrap_err().to_string();
    assert!(error.contains("catalog.connect_timeout must be greater than zero"));

    let mut parsed = IcebergSinkConfig::from_config(&table_definition_config()).unwrap();
    parsed.storage.connect_timeout = Duration::ZERO;
    let error = parsed.validate_writer_limits().unwrap_err().to_string();
    assert!(error.contains("storage.connect_timeout must be greater than zero"));
}

#[test]
fn configured_writer_limits_fail_during_parsing() {
    for (key, value) in [
        (
            "max.files.per.checkpoint",
            (ICEBERG_MAX_FILES_PER_CHECKPOINT + 1).to_string(),
        ),
        (
            "max.open.partitions",
            (ICEBERG_MAX_FILES_PER_CHECKPOINT + 1).to_string(),
        ),
        (
            "max.descriptor.bytes",
            (crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES + 1).to_string(),
        ),
    ] {
        let mut config = table_definition_config();
        config.set(key, value);
        let error = IcebergSinkConfig::from_config(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "got: {error}");
    }
}

#[test]
fn identifier_field_count_is_bounded_before_table_creation() {
    let mut config = table_definition_config();
    config.set(
        "identifier.fields",
        (0..129)
            .map(|index| format!("field_{index}"))
            .collect::<Vec<_>>()
            .join(","),
    );
    let error = IcebergSinkConfig::from_config(&config)
        .unwrap_err()
        .to_string();
    assert!(error.contains("more than 128 entries"));
}

#[test]
fn table_properties_cannot_persist_or_expose_secrets() {
    let mut config = table_definition_config();
    config.set("auto.create", "true");
    config.set("table.property.password", "do-not-log-this");
    let error = IcebergSinkConfig::from_config(&config)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("do-not-log-this"));

    let mut safe = table_definition_config();
    safe.set("auto.create", "true");
    safe.set("table.property.owner", "private-owner-value");
    let debug = format!("{:?}", IcebergSinkConfig::from_config(&safe).unwrap());
    assert!(debug.contains("initial_table_property_count"));
    assert!(!debug.contains("owner"));
    assert!(!debug.contains("private-owner-value"));
}

#[test]
fn typed_writer_properties_cannot_be_duplicated() {
    let mut config = table_definition_config();
    config.set("auto.create", "true");
    config.set("table.property.write.target-file-size-bytes", "1");
    let error = IcebergSinkConfig::from_config(&config)
        .unwrap_err()
        .to_string();
    assert!(error.contains("duplicates a typed writer option"));
}

#[test]
fn typed_storage_backends_parse_without_silent_fallback() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "demo");
    config.set("storage.type", "gcs");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    let parsed = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(parsed.storage.storage_type, Some(IcebergStorageType::Gcs));

    config.set("storage.type", "hdfs");
    assert!(IcebergSinkConfig::from_config(&config).is_err());
}

#[test]
fn legacy_and_typed_option_aliases_match() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("snapshot.id", "42");
    config.set("select.columns", "id, payload");
    config.set("target.file.size", "4096");
    config.set("catalog.property.s3.path-style-access", "true");

    let source = IcebergSourceConfig::from_config(&config).unwrap();
    let sink = IcebergSinkConfig::from_config(&config).unwrap();
    assert_eq!(source.snapshot_id, Some(42));
    assert_eq!(source.select_columns, ["id", "payload"]);
    assert_eq!(sink.target_file_size_bytes, 4096);
    assert!(sink.storage.path_style);
}

#[test]
fn debug_redacts_catalog_and_storage_values() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "https://catalog.test");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("catalog.property.token", "secret-token");
    config.set("storage.endpoint", "https://secret-endpoint.test");
    config.set("storage.property.aws_secret_access_key", "secret-key");

    let parsed = IcebergSinkConfig::from_config(&config).unwrap();
    let debug = format!("{parsed:?}");
    for secret in ["secret-token", "secret-endpoint", "secret-key"] {
        assert!(!debug.contains(secret), "Debug leaked {secret}");
    }
}

#[test]
fn inline_uri_credentials_are_rejected_without_echoing() {
    for (key, value) in [
        ("catalog.uri", "https://user:inline-secret@catalog.test"),
        (
            "catalog.warehouse",
            "s3://bucket/warehouse?token=inline-secret",
        ),
        (
            "storage.endpoint",
            "https://objects.test?credential=inline-secret",
        ),
    ] {
        let mut config = table_definition_config();
        config.set(key, value);
        let error = IcebergSinkConfig::from_config(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "got: {error}");
        assert!(!error.contains("inline-secret"), "got: {error}");
    }
}

#[test]
fn invalid_mode_combinations_are_rejected() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://bucket/wh");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("read.mode", "snapshot");
    config.set("read.bootstrap", "none");
    assert!(IcebergSourceConfig::from_config(&config).is_err());
}

#[test]
fn catalog_auth_is_inferred_without_permitting_mixed_credentials() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "https://catalog.test");
    config.set("catalog.warehouse", "s3://bucket/warehouse");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("catalog.property.token", "resolved-secret");
    assert_eq!(
        IcebergCatalogConfig::from_config(&config)
            .unwrap()
            .auth_type,
        IcebergCatalogAuthType::Bearer
    );

    config.set("catalog.auth.type", "oauth2");
    assert!(IcebergCatalogConfig::from_config(&config).is_err());
    config.set("catalog.property.credential", "client:resolved-secret");
    assert!(IcebergCatalogConfig::from_config(&config).is_err());
}

#[test]
fn catalog_auth_rejects_empty_secret_material() {
    for (auth_type, property) in [("bearer", "token"), ("oauth2", "credential")] {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "https://catalog.test");
        config.set("catalog.warehouse", "s3://bucket/warehouse");
        config.set("namespace", "prod");
        config.set("table.name", "events");
        config.set("catalog.auth.type", auth_type);
        config.set(format!("catalog.property.{property}"), "");
        assert!(IcebergCatalogConfig::from_config(&config).is_err());
    }
}

#[test]
fn oauth_typed_client_configuration_is_preserved() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "https://catalog.test");
    config.set("catalog.warehouse", "s3://bucket/warehouse");
    config.set("namespace", "prod");
    config.set("table.name", "events");
    config.set("catalog.auth.type", "oauth2");
    config.set("catalog.oauth2.server_uri", "https://identity.test/token");
    config.set("catalog.oauth2.client_id", "laminar-client");
    config.set("catalog.oauth2.scope", "catalog:read catalog:write");
    config.set("catalog.property.credential", "resolved-client-secret");

    let parsed = IcebergCatalogConfig::from_config(&config).unwrap();
    assert_eq!(parsed.auth_type, IcebergCatalogAuthType::OAuth2);
    assert_eq!(parsed.oauth2_client_id.as_deref(), Some("laminar-client"));
    assert_eq!(
        parsed.oauth2_server_uri.as_deref(),
        Some("https://identity.test/token")
    );
}

// ── Schema validation tests ──

use arrow_schema::{DataType, Field, Schema};

fn schema(fields: Vec<(&str, DataType)>) -> Schema {
    Schema::new(
        fields
            .into_iter()
            .map(|(n, t)| Field::new(n, t, true))
            .collect::<Vec<_>>(),
    )
}

#[test]
fn test_validate_matching_schemas() {
    let s = schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
    assert!(validate_sink_schema(&s, &s).is_ok());
}

#[test]
fn test_validate_missing_field() {
    let pipeline = schema(vec![("id", DataType::Int64), ("extra", DataType::Utf8)]);
    let table = schema(vec![("id", DataType::Int64)]);
    let err = validate_sink_schema(&pipeline, &table).unwrap_err();
    assert!(err.to_string().contains("extra"));
}

#[test]
fn test_validate_type_mismatch() {
    let pipeline = schema(vec![("id", DataType::Int64)]);
    let table = schema(vec![("id", DataType::Utf8)]);
    let err = validate_sink_schema(&pipeline, &table).unwrap_err();
    assert!(err.to_string().contains("incompatible"));
}

#[test]
fn test_validate_extra_table_columns_ok() {
    let pipeline = schema(vec![("id", DataType::Int64)]);
    let table = schema(vec![("id", DataType::Int64), ("extra", DataType::Utf8)]);
    assert!(validate_sink_schema(&pipeline, &table).is_ok());
}

#[test]
fn test_validate_safe_widening() {
    let pipeline = schema(vec![("n", DataType::Int32), ("f", DataType::Float32)]);
    let table = schema(vec![("n", DataType::Int64), ("f", DataType::Float64)]);
    assert!(validate_sink_schema(&pipeline, &table).is_ok());
}
