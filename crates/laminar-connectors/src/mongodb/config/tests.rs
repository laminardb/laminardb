use super::*;

// ── Source config tests ──

#[test]
fn test_source_config_default() {
    let cfg = MongoDbSourceConfig::default();
    assert_eq!(cfg.connection_uri, "mongodb://localhost:27017");
    assert_eq!(cfg.full_document_mode, FullDocumentMode::Delta);
    assert_eq!(cfg.max_buffered_bytes, 64 * 1024 * 1024);
}

#[test]
fn test_source_config_new() {
    let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "mydb", "users");
    assert_eq!(cfg.connection_uri, "mongodb://db:27017");
    assert_eq!(cfg.database, "mydb");
    assert_eq!(cfg.collection, "users");
}

#[test]
fn test_source_config_validate_empty_uri() {
    let cfg = MongoDbSourceConfig::new("", "db", "coll");
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("connection_uri"));
}

#[test]
fn test_source_config_validate_empty_database() {
    let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "", "coll");
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("database"));
}

#[test]
fn source_config_rejects_database_wildcard_watch() {
    let cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "*");
    let error = cfg.validate().unwrap_err();
    assert!(error.to_string().contains("fixed collection"), "{error}");
    assert!(error.to_string().contains("UUID"), "{error}");
}

#[test]
fn source_buffer_byte_bound_has_a_finite_operational_range() {
    let mut cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "coll");
    for invalid in [MIN_BUFFERED_BYTES - 1, MAX_BUFFERED_BYTES + 1] {
        cfg.max_buffered_bytes = invalid;
        let error = cfg.validate().unwrap_err();
        assert!(error.to_string().contains("max.buffered.bytes"), "{error}");
    }
    for valid in [MIN_BUFFERED_BYTES, MAX_BUFFERED_BYTES] {
        cfg.max_buffered_bytes = valid;
        cfg.validate().unwrap();
    }
}

#[test]
fn removed_source_properties_are_rejected_explicitly() {
    for key in REMOVED_SOURCE_CONFIG_KEYS {
        let mut config = ConnectorConfig::new("mongodb-cdc");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "events");
        config.set(*key, "removed-value");
        let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains(key));
    }
}

#[test]
fn test_source_config_from_connector_config() {
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "events");
    config.set("full.document.mode", "required");
    config.set(
        "pipeline",
        r#"[{"$match":{"z":1,"operationType":"insert"}}]"#,
    );
    config.set("max.buffered.bytes", "33554432");

    let cfg = MongoDbSourceConfig::from_config(&config).unwrap();
    assert_eq!(cfg.connection_uri, "mongodb://host:27017");
    assert_eq!(cfg.database, "testdb");
    assert_eq!(cfg.collection, "events");
    assert_eq!(cfg.full_document_mode, FullDocumentMode::RequirePostImage);
    assert_eq!(
        canonical_pipeline_json(&cfg.pipeline),
        r#"[{"$match":{"operationType":"insert","z":1}}]"#
    );
    assert_eq!(cfg.max_buffered_bytes, 32 * 1024 * 1024);
    assert_eq!(cfg.reader_channel_capacity(), 512);
    assert_eq!(cfg.cursor_batch_size(), 512);
}

#[test]
fn cursor_batch_hint_is_derived_from_buffer_budget() {
    let mut cfg = MongoDbSourceConfig::new("mongodb://db:27017", "db", "coll");
    cfg.max_buffered_bytes = MIN_BUFFERED_BYTES;
    assert_eq!(cfg.reader_channel_capacity(), 16);
    assert_eq!(cfg.cursor_batch_size(), 16);

    cfg.max_buffered_bytes = DEFAULT_MAX_BUFFERED_BYTES;
    assert_eq!(cfg.reader_channel_capacity(), 1024);
    assert_eq!(cfg.cursor_batch_size(), 1000);

    cfg.max_buffered_bytes = usize::MAX;
    assert_eq!(cfg.reader_channel_capacity(), MAX_READER_CHANNEL_ITEMS);
    assert_eq!(cfg.cursor_batch_size(), 1000);
}

#[test]
fn test_source_config_from_config_missing_required() {
    let config = ConnectorConfig::new("mongodb-cdc");
    assert!(MongoDbSourceConfig::from_config(&config).is_err());
}

#[test]
fn source_config_rejects_unknown_property() {
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "events");
    config.set("max.await.time.mss", "25");

    let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("max.await.time.mss"));
}

// ── Pipeline validation tests ──

#[test]
fn test_pipeline_valid_match() {
    let pipeline = vec![serde_json::json!({
        "$match": { "operationType": "insert" }
    })];
    validate_pipeline(&pipeline).unwrap();
}

#[test]
fn pipeline_stage_must_be_a_document() {
    let error = validate_pipeline(&[serde_json::json!("$match")]).unwrap_err();
    assert!(error.to_string().contains("must be a JSON object"));
}

#[test]
fn pipeline_stage_must_be_bson_representable() {
    let pipeline = vec![serde_json::json!({
        "$match": { "value": u64::MAX }
    })];
    let error = validate_pipeline(&pipeline).unwrap_err();
    assert!(error.to_string().contains("cannot be represented as BSON"));
}

#[test]
fn pipeline_only_accepts_match_stages() {
    for stage in [
        serde_json::json!({ "$project": { "_id": 1, "name": 1 } }),
        serde_json::json!({ "$unset": "_id" }),
        serde_json::json!({ "$set": { "_id": "overwritten" } }),
        serde_json::json!({ "$replaceRoot": { "newRoot": "$fullDocument" } }),
        serde_json::json!({ "$match": {}, "$project": { "_id": 1 } }),
    ] {
        let error = validate_pipeline(&[stage]).unwrap_err();
        assert!(error.to_string().contains("unsafe"), "{error}");
    }
}

#[test]
fn pipeline_property_requires_a_bounded_json_array() {
    let mut config = ConnectorConfig::new("mongodb-cdc");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "events");

    config.set("pipeline", r#"{"$match":{}}"#);
    let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("JSON array"), "{error}");

    let stages = vec![serde_json::json!({ "$match": {} }); MAX_PIPELINE_STAGES + 1];
    config.set("pipeline", serde_json::to_string(&stages).unwrap());
    let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("maximum"), "{error}");

    let oversized = format!(
        r#"[{{"$match":{{"payload":"{}"}}}}]"#,
        "x".repeat(MAX_PIPELINE_JSON_BYTES)
    );
    config.set("pipeline", oversized);
    let error = MongoDbSourceConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("maximum"), "{error}");
}

#[test]
fn pipeline_is_recursively_canonicalized() {
    let pipeline = parse_pipeline_property(r#"[{"$match":{"z":1,"a":{"y":2,"b":3}}}]"#).unwrap();
    assert_eq!(
        canonical_pipeline_json(&pipeline),
        r#"[{"$match":{"a":{"b":3,"y":2},"z":1}}]"#
    );
}

#[test]
fn pipeline_match_expression_must_be_a_document() {
    let error = validate_pipeline(&[serde_json::json!({ "$match": "insert" })]).unwrap_err();
    assert!(error.to_string().contains("as $match"), "{error}");
}

// ── Full document mode tests ──

#[test]
fn test_full_document_mode_fromstr() {
    assert_eq!(
        "delta".parse::<FullDocumentMode>().unwrap(),
        FullDocumentMode::Delta
    );
    assert_eq!(
        "required".parse::<FullDocumentMode>().unwrap(),
        FullDocumentMode::RequirePostImage
    );
    assert!("update_lookup".parse::<FullDocumentMode>().is_err());
    assert!("when_available".parse::<FullDocumentMode>().is_err());
    assert!("bad".parse::<FullDocumentMode>().is_err());
}

#[test]
fn test_full_document_mode_display() {
    assert_eq!(FullDocumentMode::Delta.to_string(), "delta");
    assert_eq!(FullDocumentMode::RequirePostImage.to_string(), "required");
}

// ── Sink config tests ──

#[test]
fn test_sink_config_default() {
    let cfg = MongoDbSinkConfig::default();
    assert_eq!(cfg.flush_interval_ms, 250);
    assert!(matches!(cfg.collection_kind, CollectionKind::Standard));
}

#[test]
fn test_sink_config_new() {
    let cfg = MongoDbSinkConfig::new("mongodb://db:27017", "mydb", "events");
    assert_eq!(cfg.connection_uri, "mongodb://db:27017");
    assert_eq!(cfg.database, "mydb");
    assert_eq!(cfg.collection, "events");
}

#[test]
fn test_sink_config_validate_empty_uri() {
    let cfg = MongoDbSinkConfig::new("", "db", "coll");
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("connection_uri"));
}

#[test]
fn test_sink_config_validate_zero_flush_interval() {
    let mut cfg = MongoDbSinkConfig::new("mongodb://db:27017", "db", "coll");
    cfg.flush_interval_ms = 0;
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("flush_interval_ms"));
}

#[test]
fn sink_requires_a_fixed_destination_collection() {
    let cfg = MongoDbSinkConfig::new("mongodb://db:27017", "db", "*");
    let error = cfg.validate().unwrap_err();
    assert!(error.to_string().contains("fixed destination"));
}

#[test]
fn test_sink_config_timeseries_upsert_rejected() {
    let mut cfg = MongoDbSinkConfig::new("mongodb://db:27017", "db", "ts");
    cfg.collection_kind = CollectionKind::TimeSeries(super::super::timeseries::TimeSeriesConfig {
        time_field: "ts".to_string(),
        meta_field: None,
        granularity: super::super::timeseries::TimeSeriesGranularity::Seconds,
        expire_after_seconds: None,
    });
    cfg.write_mode = WriteMode::Upsert {
        key_fields: vec!["id".to_string()],
    };
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("time series"));
}

#[test]
fn test_sink_config_flush_interval() {
    let cfg = MongoDbSinkConfig::default();
    assert_eq!(cfg.flush_interval(), Duration::from_millis(250));
}

#[test]
fn test_sink_config_from_connector_config() {
    let mut config = ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "out");
    config.set("write.mode", "upsert");
    config.set("write.mode.key_fields", "id");

    let cfg = MongoDbSinkConfig::from_config(&config).unwrap();
    assert!(matches!(cfg.write_mode, WriteMode::Upsert { .. }));
}

#[test]
fn removed_sink_properties_are_rejected() {
    for key in REMOVED_SINK_CONFIG_KEYS {
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set(*key, "1000");

        let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains(key));
    }
}

#[test]
fn sink_upsert_keys_must_be_non_empty_and_unique() {
    for keys in [
        vec![],
        vec![String::new()],
        vec!["id".to_string(), "id".to_string()],
        vec!["_op".to_string()],
        vec!["$expr".to_string()],
        vec!["customer.id".to_string()],
    ] {
        let mut config = MongoDbSinkConfig::new("mongodb://host:27017", "db", "out");
        config.write_mode = WriteMode::Upsert { key_fields: keys };
        assert!(config.validate().is_err());
    }
}

#[test]
fn sink_config_rejects_mode_irrelevant_properties() {
    let cases = [
        ("insert", "write.mode.key_fields", "id"),
        ("insert", "ordered", "false"),
        ("insert", "write.mode.upsert_on_missing", "true"),
        ("insert", "write_concern.journal", "false"),
    ];

    for (mode, key, value) in cases {
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set("write.mode", mode);
        config.set(key, value);

        let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains(key), "{error}");
    }

    let mut config = ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "out");
    config.set("write.mode", "replace");
    let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("replace"), "{error}");
}

#[test]
fn sink_config_rejects_irrelevant_timeseries_properties() {
    for key in [
        "timeseries.meta_field",
        "timeseries.granularity",
        "timeseries.bucket_max_span_seconds",
        "timeseries.bucket_rounding_seconds",
        "timeseries.expire_after_seconds",
    ] {
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://host:27017");
        config.set("database", "testdb");
        config.set("collection", "out");
        config.set(key, "60");

        let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains(key), "{error}");
    }

    let mut config = ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "out");
    config.set("timeseries.time_field", "timestamp");
    config.set("timeseries.granularity", "seconds");
    config.set("timeseries.bucket_max_span_seconds", "60");

    let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
    assert!(error
        .to_string()
        .contains("timeseries.bucket_max_span_seconds"));
}

#[test]
fn sink_config_rejects_unknown_property() {
    let mut config = ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "out");
    config.set("flush.intervall.ms", "10");

    let error = MongoDbSinkConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("flush.intervall.ms"));
}

#[test]
fn test_sink_config_timeseries_parsing() {
    // Test standard granularity with metadata and TTL
    let mut config = ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "ts_out");
    config.set("timeseries.time_field", "timestamp");
    config.set("timeseries.meta_field", "sensor_id");
    config.set("timeseries.granularity", "minutes");
    config.set("timeseries.expire_after_seconds", "86400");

    let cfg = MongoDbSinkConfig::from_config(&config).unwrap();
    if let CollectionKind::TimeSeries(ts) = cfg.collection_kind {
        assert_eq!(ts.time_field, "timestamp");
        assert_eq!(ts.meta_field.as_deref(), Some("sensor_id"));
        assert_eq!(ts.granularity, TimeSeriesGranularity::Minutes);
        assert_eq!(ts.expire_after_seconds, Some(86400));
    } else {
        panic!("Expected TimeSeries collection kind");
    }

    // Test custom granularity
    let mut config_custom = ConnectorConfig::new("mongodb-sink");
    config_custom.set("connection.uri", "mongodb://host:27017");
    config_custom.set("database", "testdb");
    config_custom.set("collection", "ts_custom");
    config_custom.set("timeseries.time_field", "timestamp");
    config_custom.set("timeseries.granularity", "custom");
    config_custom.set("timeseries.bucket_max_span_seconds", "3600");
    config_custom.set("timeseries.bucket_rounding_seconds", "3600");

    let cfg_custom = MongoDbSinkConfig::from_config(&config_custom).unwrap();
    if let CollectionKind::TimeSeries(ts) = cfg_custom.collection_kind {
        assert_eq!(ts.time_field, "timestamp");
        assert_eq!(
            ts.granularity,
            TimeSeriesGranularity::Custom {
                bucket_max_span_seconds: 3600,
                bucket_rounding_seconds: 3600,
            }
        );
    } else {
        panic!("Expected TimeSeries collection kind");
    }
}

#[test]
fn test_sink_config_timeseries_empty_time_field_rejected() {
    let mut config = ConnectorConfig::new("mongodb-sink");
    config.set("connection.uri", "mongodb://host:27017");
    config.set("database", "testdb");
    config.set("collection", "ts");
    config.set("timeseries.time_field", "  ");
    let err = MongoDbSinkConfig::from_config(&config).unwrap_err();
    assert!(err.to_string().contains("time_field"));
}
