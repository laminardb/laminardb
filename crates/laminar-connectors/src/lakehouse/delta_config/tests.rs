use super::*;

fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("delta-lake");
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    config
}

fn required_pairs() -> Vec<(&'static str, &'static str)> {
    vec![("table.path", "/data/warehouse/trades")]
}

// ── Config parsing tests ──

#[test]
fn test_parse_required_fields() {
    let config = make_config(&required_pairs());
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.table_path, "/data/warehouse/trades");
    assert_eq!(cfg.write_mode, DeltaWriteMode::Append);
    assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
    assert!(cfg.partition_columns.is_empty());
    assert!(cfg.merge_key_columns.is_empty());
    assert_eq!(cfg.target_file_size, 128 * 1024 * 1024);
    assert_eq!(cfg.max_buffer_records, 100_000);
    assert!(!cfg.schema_evolution);
}

#[test]
fn test_missing_table_path() {
    let config = ConnectorConfig::new("delta-lake");
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_parse_all_optional_fields() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("partition.columns", "trade_date, hour"),
        ("target.file.size", "67108864"),
        ("max.buffer.records", "50000"),
        ("max.buffer.duration.ms", "30000"),
        ("schema.evolution", "true"),
        ("write.mode", "upsert"),
        ("merge.key.columns", "customer_id, order_id"),
        ("delivery.guarantee", "at-least-once"),
        ("storage.aws_access_key_id", "AKID123"),
        ("storage.aws_region", "us-east-1"),
    ]);
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();

    assert_eq!(cfg.partition_columns, vec!["trade_date", "hour"]);
    assert_eq!(cfg.target_file_size, 67_108_864);
    assert_eq!(cfg.max_buffer_records, 50_000);
    assert_eq!(cfg.max_buffer_duration, Duration::from_secs(30));
    assert!(cfg.schema_evolution);
    assert_eq!(cfg.write_mode, DeltaWriteMode::Upsert);
    assert_eq!(cfg.merge_key_columns, vec!["customer_id", "order_id"]);
    assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
    assert_eq!(
        cfg.storage_options.get("aws_access_key_id"),
        Some(&"AKID123".to_string())
    );
    assert_eq!(
        cfg.storage_options.get("aws_region"),
        Some(&"us-east-1".to_string())
    );
}

#[test]
fn test_upsert_requires_merge_key() {
    let mut pairs = required_pairs();
    pairs.push(("write.mode", "upsert"));
    let config = make_config(&pairs);
    let result = DeltaLakeSinkConfig::from_config(&config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("merge.key.columns"), "error: {err}");
}

#[test]
fn test_empty_table_path_rejected() {
    let mut cfg = DeltaLakeSinkConfig::default();
    cfg.table_path = String::new();
    assert!(cfg.validate().is_err());
}

#[test]
fn test_zero_max_buffer_records_rejected() {
    let mut pairs = required_pairs();
    pairs.push(("max.buffer.records", "0"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_zero_target_file_size_rejected() {
    let mut pairs = required_pairs();
    pairs.push(("target.file.size", "0"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_invalid_target_file_size() {
    let mut pairs = required_pairs();
    pairs.push(("target.file.size", "abc"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_invalid_write_mode() {
    let mut pairs = required_pairs();
    pairs.push(("write.mode", "unknown"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_storage_options_prefix_stripping() {
    let mut pairs = required_pairs();
    pairs.push(("storage.aws_access_key_id", "AKID"));
    pairs.push(("storage.aws_secret_access_key", "SECRET"));
    pairs.push(("table.path", "/data/test"));
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();

    assert_eq!(cfg.storage_options.len(), 2);
    assert!(cfg.storage_options.contains_key("aws_access_key_id"));
    assert!(cfg.storage_options.contains_key("aws_secret_access_key"));
    assert!(!cfg
        .storage_options
        .contains_key("storage.aws_access_key_id"));
}

#[test]
fn test_defaults() {
    let cfg = DeltaLakeSinkConfig::default();
    assert!(cfg.table_path.is_empty());
    assert_eq!(cfg.target_file_size, 128 * 1024 * 1024);
    assert_eq!(cfg.max_buffer_records, 100_000);
    assert_eq!(cfg.max_buffer_duration, Duration::from_secs(60));
    assert!(!cfg.schema_evolution);
    assert_eq!(cfg.write_mode, DeltaWriteMode::Append);
    assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
}

#[test]
fn test_new_helper() {
    let cfg = DeltaLakeSinkConfig::new("/tmp/test_table");
    assert_eq!(cfg.table_path, "/tmp/test_table");
    assert_eq!(cfg.write_mode, DeltaWriteMode::Append);
}

// ── Enum tests ──

#[test]
fn test_write_mode_parse() {
    assert_eq!(
        "append".parse::<DeltaWriteMode>().unwrap(),
        DeltaWriteMode::Append
    );
    assert_eq!(
        "overwrite".parse::<DeltaWriteMode>().unwrap(),
        DeltaWriteMode::Overwrite
    );
    assert_eq!(
        "upsert".parse::<DeltaWriteMode>().unwrap(),
        DeltaWriteMode::Upsert
    );
    assert_eq!(
        "merge".parse::<DeltaWriteMode>().unwrap(),
        DeltaWriteMode::Upsert
    );
    assert!("unknown".parse::<DeltaWriteMode>().is_err());
}

#[test]
fn test_write_mode_display() {
    assert_eq!(DeltaWriteMode::Append.to_string(), "append");
    assert_eq!(DeltaWriteMode::Overwrite.to_string(), "overwrite");
    assert_eq!(DeltaWriteMode::Upsert.to_string(), "upsert");
}

#[test]
fn test_delivery_guarantee_parse() {
    assert_eq!(
        "at-least-once".parse::<DeliveryGuarantee>().unwrap(),
        DeliveryGuarantee::AtLeastOnce
    );
    assert_eq!(
        "at_least_once".parse::<DeliveryGuarantee>().unwrap(),
        DeliveryGuarantee::AtLeastOnce
    );
    assert_eq!(
        "exactly-once".parse::<DeliveryGuarantee>().unwrap(),
        DeliveryGuarantee::ExactlyOnce
    );
    assert_eq!(
        "exactly_once".parse::<DeliveryGuarantee>().unwrap(),
        DeliveryGuarantee::ExactlyOnce
    );
    assert!("unknown".parse::<DeliveryGuarantee>().is_err());
}

#[test]
fn test_delivery_guarantee_display() {
    assert_eq!(DeliveryGuarantee::AtLeastOnce.to_string(), "at-least-once");
    assert_eq!(DeliveryGuarantee::ExactlyOnce.to_string(), "exactly-once");
}

#[test]
fn test_partition_columns_empty_filter() {
    let mut pairs = required_pairs();
    pairs.push(("partition.columns", "a,,b, ,c"));
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.partition_columns, vec!["a", "b", "c"]);
}

// ── Cloud storage integration tests ──

#[test]
fn test_s3_path_retains_the_downstream_region_chain() {
    let config = make_config(&[("table.path", "s3://my-bucket/trades")]);
    let parsed = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert!(!parsed.storage_options.contains_key("aws_region"));
}

#[test]
fn test_s3_path_with_region_and_credentials() {
    let config = make_config(&[
        ("table.path", "s3://my-bucket/trades"),
        ("storage.aws_region", "us-east-1"),
        ("storage.aws_access_key_id", "AKID123"),
        ("storage.aws_secret_access_key", "SECRET"),
    ]);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.storage_options["aws_region"], "us-east-1");
    assert_eq!(cfg.storage_options["aws_access_key_id"], "AKID123");
}

#[test]
fn test_s3_path_with_region_only_warns_no_error() {
    // Missing credentials is a warning (IAM fallback), not a hard error.
    let config = make_config(&[
        ("table.path", "s3://my-bucket/trades"),
        ("storage.aws_region", "us-east-1"),
    ]);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
}

#[test]
fn test_s3_path_access_key_without_secret_errors() {
    let config = make_config(&[
        ("table.path", "s3://my-bucket/trades"),
        ("storage.aws_region", "us-east-1"),
        ("storage.aws_access_key_id", "AKID123"),
    ]);
    let result = DeltaLakeSinkConfig::from_config(&config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("aws_secret_access_key"), "error: {err}");
}

#[test]
fn test_azure_path_requires_account_name() {
    let config = make_config(&[("table.path", "az://my-container/trades")]);
    let result = DeltaLakeSinkConfig::from_config(&config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("azure_storage_account_name"), "error: {err}");
}

#[test]
fn test_azure_path_with_account_name_and_key() {
    let config = make_config(&[
        ("table.path", "az://my-container/trades"),
        ("storage.azure_storage_account_name", "myaccount"),
        ("storage.azure_storage_account_key", "base64key=="),
    ]);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
}

#[test]
fn test_gcs_path_always_valid() {
    // GCS missing credentials is warning-only (Application Default Credentials).
    let config = make_config(&[("table.path", "gs://my-bucket/trades")]);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
}

#[test]
fn test_local_path_no_cloud_validation() {
    let config = make_config(&[("table.path", "/data/warehouse/trades")]);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_ok());
}

#[test]
fn test_display_storage_options_redacts_secrets() {
    let mut cfg = DeltaLakeSinkConfig::new("s3://bucket/path");
    cfg.storage_options
        .insert("aws_region".to_string(), "us-east-1".to_string());
    cfg.storage_options.insert(
        "aws_secret_access_key".to_string(),
        "TOP_SECRET".to_string(),
    );

    let display = cfg.display_storage_options();
    assert!(display.contains("aws_region=us-east-1"));
    assert!(display.contains("aws_secret_access_key=***"));
    assert!(!display.contains("TOP_SECRET"));
}

#[test]
fn test_display_storage_options_empty() {
    let cfg = DeltaLakeSinkConfig::new("/local/path");
    assert!(cfg.display_storage_options().is_empty());
}

// ── Catalog tests ──

#[test]
fn test_catalog_type_parse() {
    assert_eq!(
        "none".parse::<DeltaCatalogType>().unwrap(),
        DeltaCatalogType::None
    );
    assert_eq!(
        "glue".parse::<DeltaCatalogType>().unwrap(),
        DeltaCatalogType::Glue
    );
    assert!(matches!(
        "unity".parse::<DeltaCatalogType>().unwrap(),
        DeltaCatalogType::Unity { .. }
    ));
    assert!("unknown".parse::<DeltaCatalogType>().is_err());
}

#[test]
fn test_catalog_type_display() {
    assert_eq!(DeltaCatalogType::None.to_string(), "none");
    assert_eq!(DeltaCatalogType::Glue.to_string(), "glue");
    assert_eq!(
        DeltaCatalogType::Unity {
            workspace_url: "url".into(),
            access_token: "tok".into()
        }
        .to_string(),
        "unity"
    );
}

#[test]
fn test_catalog_none_default() {
    let config = make_config(&required_pairs());
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.catalog_type, DeltaCatalogType::None);
    assert!(cfg.catalog_database.is_none());
    assert!(cfg.catalog_name.is_none());
    assert!(cfg.catalog_schema.is_none());
    assert!(cfg.catalog_storage_location.is_none());
}

#[cfg(feature = "delta-lake-glue")]
#[test]
fn test_catalog_glue_valid() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("catalog.type", "glue"),
        ("catalog.database", "my_database"),
    ]);
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.catalog_type, DeltaCatalogType::Glue);
    assert_eq!(cfg.catalog_database.as_deref(), Some("my_database"));
}

#[cfg(feature = "delta-lake-glue")]
#[test]
fn test_catalog_glue_missing_database() {
    let mut pairs = required_pairs();
    pairs.push(("catalog.type", "glue"));
    let config = make_config(&pairs);
    let result = DeltaLakeSinkConfig::from_config(&config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("catalog.database"), "error: {err}");
}

#[cfg(feature = "delta-lake-unity")]
#[test]
fn test_catalog_unity_valid() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("catalog.type", "unity"),
        ("catalog.workspace_url", "https://my.databricks.com"),
        ("catalog.access_token", "dapi123"),
        ("catalog.name", "main"),
        ("catalog.schema", "default"),
    ]);
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert!(matches!(
        cfg.catalog_type,
        DeltaCatalogType::Unity {
            ref workspace_url,
            ref access_token
        }
        if workspace_url == "https://my.databricks.com"
            && access_token == "dapi123"
    ));
    assert_eq!(cfg.catalog_name.as_deref(), Some("main"));
    assert_eq!(cfg.catalog_schema.as_deref(), Some("default"));
}

#[cfg(feature = "delta-lake-unity")]
#[test]
fn test_catalog_unity_missing_workspace_url() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("catalog.type", "unity"),
        ("catalog.access_token", "dapi123"),
        ("catalog.name", "main"),
        ("catalog.schema", "default"),
    ]);
    let config = make_config(&pairs);
    let result = DeltaLakeSinkConfig::from_config(&config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("workspace_url"), "error: {err}");
}

#[cfg(feature = "delta-lake-unity")]
#[test]
fn test_catalog_unity_missing_access_token() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("catalog.type", "unity"),
        ("catalog.workspace_url", "https://my.databricks.com"),
        ("catalog.name", "main"),
        ("catalog.schema", "default"),
    ]);
    let config = make_config(&pairs);
    let result = DeltaLakeSinkConfig::from_config(&config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("access_token"), "error: {err}");
}

#[test]
fn test_catalog_storage_location_default_none() {
    let config = make_config(&required_pairs());
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert!(cfg.catalog_storage_location.is_none());
}

#[test]
fn test_catalog_storage_location_parsed() {
    let mut pairs = required_pairs();
    pairs.push(("catalog.storage.location", "s3://bucket/warehouse/table"));
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(
        cfg.catalog_storage_location.as_deref(),
        Some("s3://bucket/warehouse/table")
    );
}

// ── Parquet config tests ──

#[test]
fn test_parquet_config_defaults() {
    let cfg = ParquetWriteConfig::default();
    assert_eq!(cfg.compression, "zstd");
    assert_eq!(cfg.compression_level, 1);
    assert!(cfg.dictionary_enabled);
    assert_eq!(cfg.statistics, "page");
    assert!(cfg.bloom_filter_columns.is_empty());
    assert!((cfg.bloom_filter_fpp - 0.01).abs() < f64::EPSILON);
    assert_eq!(cfg.bloom_filter_ndv, 0);
    assert_eq!(cfg.max_row_group_size, 1_000_000);
}

#[test]
fn test_parquet_compression_parsing() {
    for codec in &["zstd", "snappy", "lz4", "gzip", "none"] {
        let mut pairs = required_pairs();
        pairs.push(("parquet.compression", codec));
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.parquet.compression, *codec);
    }
}

#[test]
fn test_parquet_compression_level_parsing() {
    let mut pairs = required_pairs();
    pairs.push(("parquet.compression.level", "5"));
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.parquet.compression_level, 5);
}

#[test]
fn test_parquet_compression_level_invalid() {
    let mut pairs = required_pairs();
    pairs.push(("parquet.compression.level", "abc"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_parquet_bloom_filter_columns_parsing() {
    let mut pairs = required_pairs();
    pairs.push((
        "parquet.bloom.filter.columns",
        " user_id , event_type , ts ",
    ));
    let config = make_config(&pairs);
    let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
    assert_eq!(
        cfg.parquet.bloom_filter_columns,
        vec!["user_id", "event_type", "ts"]
    );
}

#[test]
fn test_parquet_bloom_filter_fpp_validation() {
    // fpp = 0.0 should be rejected
    let mut pairs = required_pairs();
    pairs.push(("parquet.bloom.filter.fpp", "0.0"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());

    // fpp = 1.0 should be rejected
    let mut pairs = required_pairs();
    pairs.push(("parquet.bloom.filter.fpp", "1.0"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_parquet_max_row_group_size_zero_rejected() {
    let mut pairs = required_pairs();
    pairs.push(("parquet.max.row.group.size", "0"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_parquet_statistics_parsing() {
    for stat in &["none", "chunk", "page"] {
        let mut pairs = required_pairs();
        pairs.push(("parquet.statistics", stat));
        let config = make_config(&pairs);
        let cfg = DeltaLakeSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.parquet.statistics, *stat);
    }
}

#[test]
fn test_parquet_invalid_statistics_rejected() {
    let mut pairs = required_pairs();
    pairs.push(("parquet.statistics", "full"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_parquet_invalid_compression_rejected() {
    let mut pairs = required_pairs();
    pairs.push(("parquet.compression", "brotli"));
    let config = make_config(&pairs);
    assert!(DeltaLakeSinkConfig::from_config(&config).is_err());
}

#[cfg(feature = "delta-lake")]
#[test]
fn test_writer_properties_default_zstd() {
    let cfg = ParquetWriteConfig::default();
    assert!(cfg.to_writer_properties().is_ok());
}

#[cfg(feature = "delta-lake")]
#[test]
fn test_writer_properties_invalid_codec() {
    let mut cfg = ParquetWriteConfig::default();
    cfg.compression = "brotli".to_string();
    assert!(cfg.to_writer_properties().is_err());
}

#[cfg(feature = "delta-lake")]
#[test]
fn test_writer_properties_with_bloom_filters() {
    let mut cfg = ParquetWriteConfig::default();
    cfg.bloom_filter_columns = vec!["user_id".to_string(), "event_type".to_string()];
    assert!(cfg.to_writer_properties().is_ok());
}
