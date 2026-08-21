use super::*;

#[test]
fn test_register_source() {
    let mut mgr = ConnectorManager::new();
    mgr.register_source(SourceRegistration {
        name: "clicks".to_string(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::from([("topic".to_string(), "clicks".to_string())]),
        format: Some("JSON".to_string()),
        format_options: HashMap::new(),
    });
    assert_eq!(mgr.source_names(), vec!["clicks"]);
    assert!(mgr.has_external_connectors());
}

#[test]
fn test_register_sink() {
    let mut mgr = ConnectorManager::new();
    mgr.register_sink(SinkRegistration {
        name: "output".to_string(),
        input: "events".to_string(),
        query_inputs: Vec::new(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        filter_expr: None,
    });
    assert_eq!(mgr.sink_names(), vec!["output"]);
}

#[test]
fn test_register_stream() {
    let mut mgr = ConnectorManager::new();
    mgr.register_stream(StreamRegistration {
        name: "agg_stream".to_string(),
        query_sql: "SELECT count(*) FROM events".to_string(),
        emit_clause: None,
        window_config: None,
        order_config: None,
        join_config: None,
        has_analytic: false,
        has_frame: false,
        incremental: false,
    });
    assert_eq!(mgr.stream_names(), vec!["agg_stream"]);
}

#[test]
fn test_unregister() {
    let mut mgr = ConnectorManager::new();
    mgr.register_source(SourceRegistration {
        name: "test".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    });
    assert!(mgr.unregister_source("test"));
    assert!(!mgr.unregister_source("test"));
}

#[test]
fn test_registration_count() {
    let mut mgr = ConnectorManager::new();
    assert_eq!(mgr.registration_count(), 0);
    mgr.register_source(SourceRegistration {
        name: "s1".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    });
    mgr.register_sink(SinkRegistration {
        name: "k1".to_string(),
        input: "s1".to_string(),
        query_inputs: Vec::new(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        filter_expr: None,
    });
    assert_eq!(mgr.registration_count(), 2);
}

#[test]
fn test_no_external_connectors() {
    let mut mgr = ConnectorManager::new();
    mgr.register_source(SourceRegistration {
        name: "test".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    });
    assert!(!mgr.has_external_connectors());
}

#[test]
fn test_clear() {
    let mut mgr = ConnectorManager::new();
    mgr.register_source(SourceRegistration {
        name: "test".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    });
    mgr.clear();
    assert_eq!(mgr.registration_count(), 0);
}

#[test]
fn test_default_trait() {
    let mgr = ConnectorManager::default();
    assert_eq!(mgr.registration_count(), 0);
}

#[test]
fn test_debug_format() {
    let mgr = ConnectorManager::new();
    let debug = format!("{mgr:?}");
    assert!(debug.contains("ConnectorManager"));
    assert!(debug.contains("sources: 0"));
}

#[test]
fn test_get_source() {
    let mut mgr = ConnectorManager::new();
    assert!(mgr.get_source("test").is_none());
    mgr.register_source(SourceRegistration {
        name: "test".to_string(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    });
    let src = mgr.get_source("test").unwrap();
    assert_eq!(src.connector_type.as_deref(), Some("KAFKA"));
}

#[test]
fn test_get_sink() {
    let mut mgr = ConnectorManager::new();
    assert!(mgr.get_sink("test").is_none());
    mgr.register_sink(SinkRegistration {
        name: "test".to_string(),
        input: "events".to_string(),
        query_inputs: Vec::new(),
        connector_type: Some("POSTGRES".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        filter_expr: Some("id > 10".to_string()),
    });
    let sink = mgr.get_sink("test").unwrap();
    assert_eq!(sink.connector_type.as_deref(), Some("POSTGRES"));
    assert_eq!(sink.filter_expr.as_deref(), Some("id > 10"));
}

#[test]
fn test_overwrite_registration() {
    let mut mgr = ConnectorManager::new();
    mgr.register_source(SourceRegistration {
        name: "test".to_string(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    });
    mgr.register_source(SourceRegistration {
        name: "test".to_string(),
        connector_type: Some("POSTGRES".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    });
    assert_eq!(mgr.source_names().len(), 1);
    assert_eq!(
        mgr.get_source("test").unwrap().connector_type.as_deref(),
        Some("POSTGRES")
    );
}

#[test]
fn test_unregister_sink_and_stream() {
    let mut mgr = ConnectorManager::new();
    mgr.register_sink(SinkRegistration {
        name: "s1".to_string(),
        input: "src".to_string(),
        query_inputs: Vec::new(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        filter_expr: None,
    });
    mgr.register_stream(StreamRegistration {
        name: "st1".to_string(),
        query_sql: "SELECT 1".to_string(),
        emit_clause: None,
        window_config: None,
        order_config: None,
        join_config: None,
        has_analytic: false,
        has_frame: false,
        incremental: false,
    });
    assert!(mgr.unregister_sink("s1"));
    assert!(!mgr.unregister_sink("s1"));
    assert!(mgr.unregister_stream("st1"));
    assert!(!mgr.unregister_stream("st1"));
    assert_eq!(mgr.registration_count(), 0);
}

#[test]
fn test_register_table() {
    let mut mgr = ConnectorManager::new();
    mgr.register_table(TableRegistration {
        name: "instruments".to_string(),
        primary_key: "symbol".to_string(),
        connector_type: Some("kafka".to_string()),
        connector_options: HashMap::from([("topic".to_string(), "instruments".to_string())]),
        format: Some("JSON".to_string()),
        format_options: HashMap::new(),
        on_demand: false,
        cache_max_bytes: None,
        cache_ttl: None,
    });
    assert_eq!(mgr.table_names().len(), 1);
    assert!(mgr.has_external_connectors());
}

#[test]
fn test_unregister_table() {
    let mut mgr = ConnectorManager::new();
    mgr.register_table(TableRegistration {
        name: "t".to_string(),
        primary_key: "id".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        on_demand: false,
        cache_max_bytes: None,
        cache_ttl: None,
    });
    assert!(mgr.unregister_table("t"));
    assert!(!mgr.unregister_table("t"));
}

#[test]
fn test_table_in_registration_count() {
    let mut mgr = ConnectorManager::new();
    assert_eq!(mgr.registration_count(), 0);
    mgr.register_table(TableRegistration {
        name: "t".to_string(),
        primary_key: "id".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        on_demand: false,
        cache_max_bytes: None,
        cache_ttl: None,
    });
    assert_eq!(mgr.registration_count(), 1);
    mgr.clear();
    assert_eq!(mgr.registration_count(), 0);
}

#[test]
fn test_build_source_config_valid() {
    let reg = SourceRegistration {
        name: "clicks".to_string(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::from([
            ("topic".to_string(), "clicks".to_string()),
            (
                "bootstrap.servers".to_string(),
                "localhost:9092".to_string(),
            ),
        ]),
        format: Some("JSON".to_string()),
        format_options: HashMap::from([(
            "schema.registry.url".to_string(),
            "http://registry:8081".to_string(),
        )]),
    };
    let config = build_source_config(&reg).unwrap();
    assert_eq!(config.connector_type(), "kafka"); // normalized lowercase
    assert_eq!(config.get("topic"), Some("clicks"));
    assert_eq!(config.get("bootstrap.servers"), Some("localhost:9092"));
    assert_eq!(config.get("format"), Some("json"));
    assert_eq!(
        config.get("schema.registry.url"),
        Some("http://registry:8081")
    );
    assert_eq!(config.get("format.schema.registry.url"), None);
}

#[test]
fn connector_options_are_not_rewritten_by_the_generic_bridge() {
    let reg = SourceRegistration {
        name: "custom".to_string(),
        connector_type: Some("custom".to_string()),
        connector_options: HashMap::from([
            ("provider.endpoint".to_string(), "host:1234".to_string()),
            ("opaque_key".to_string(), "provider-value".to_string()),
        ]),
        format: None,
        format_options: HashMap::new(),
    };

    let config = build_source_config(&reg).unwrap();
    assert_eq!(config.get("provider.endpoint"), Some("host:1234"));
    assert_eq!(config.get("opaque_key"), Some("provider-value"));
}

#[test]
fn test_build_source_config_missing_type() {
    let reg = SourceRegistration {
        name: "clicks".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    };
    let err = build_source_config(&reg).unwrap_err();
    assert!(err.to_string().contains("no connector type"));
}

#[test]
fn test_build_source_config_invalid_format() {
    let reg = SourceRegistration {
        name: "clicks".to_string(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::new(),
        format: Some("BADFORMAT".to_string()),
        format_options: HashMap::new(),
    };
    let err = build_source_config(&reg).unwrap_err();
    assert!(err.to_string().contains("Invalid format"));
    assert!(err.to_string().contains("BADFORMAT"));
}

#[test]
fn test_build_source_config_no_format() {
    let reg = SourceRegistration {
        name: "clicks".to_string(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    };
    let config = build_source_config(&reg).unwrap();
    assert_eq!(config.connector_type(), "kafka");
    assert_eq!(config.get("format"), None); // not set when absent
}

#[test]
fn test_build_sink_config_valid() {
    let reg = SinkRegistration {
        name: "output".to_string(),
        input: "events".to_string(),
        query_inputs: Vec::new(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::from([("topic".to_string(), "output".to_string())]),
        format: Some("JSON".to_string()),
        format_options: HashMap::from([("json.path".to_string(), "payload".to_string())]),
        filter_expr: Some("id > 10".to_string()),
    };
    let config = build_sink_config(
        &reg,
        laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap();
    assert_eq!(config.connector_type(), "kafka");
    assert_eq!(config.get("topic"), Some("output"));
    assert_eq!(config.get("format"), Some("json"));
    assert_eq!(config.get("json.path"), Some("payload"));
    assert_eq!(config.get("format.json.path"), None);
    assert_eq!(
        config.get("delivery.guarantee"),
        Some("at-least-once"),
        "the runtime must inject the pipeline-wide delivery contract"
    );
}

#[test]
fn test_build_sink_config_rejects_per_sink_delivery() {
    let reg = SinkRegistration {
        name: "output".to_string(),
        input: "events".to_string(),
        query_inputs: Vec::new(),
        connector_type: Some("kafka".to_string()),
        connector_options: HashMap::from([(
            "DELIVERY.GUARANTEE".to_string(),
            "exactly-once".to_string(),
        )]),
        format: None,
        format_options: HashMap::new(),
        filter_expr: None,
    };

    let error = build_sink_config(
        &reg,
        laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
    )
    .expect_err("per-sink delivery must not be silently overwritten");
    let message = error.to_string();
    assert!(
        message.to_ascii_lowercase().contains("delivery.guarantee"),
        "{message}"
    );
    assert!(message.contains("owned by the runtime"), "{message}");
}

#[test]
fn connector_and_format_option_namespaces_must_not_collide() {
    let connector_options = HashMap::from([("format".to_string(), "json".to_string())]);
    let error =
        validate_connector_format_options("Source", &connector_options, None, &HashMap::new())
            .unwrap_err();
    assert!(error.to_string().contains("FORMAT clause"));

    let connector_options =
        HashMap::from([("SCHEMA.REGISTRY.URL".to_string(), "first".to_string())]);
    let format_options = HashMap::from([("schema.registry.url".to_string(), "second".to_string())]);
    let error = validate_connector_format_options(
        "Sink",
        &connector_options,
        Some("avro"),
        &format_options,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("both connector options and FORMAT WITH"));
}

#[test]
fn test_build_sink_config_missing_type() {
    let reg = SinkRegistration {
        name: "output".to_string(),
        input: "events".to_string(),
        query_inputs: Vec::new(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        filter_expr: None,
    };
    let err = build_sink_config(
        &reg,
        laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap_err();
    assert!(err.to_string().contains("no connector type"));
}

#[test]
fn test_build_sink_config_invalid_format() {
    let reg = SinkRegistration {
        name: "output".to_string(),
        input: "events".to_string(),
        query_inputs: Vec::new(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::new(),
        format: Some("NOPE".to_string()),
        format_options: HashMap::new(),
        filter_expr: None,
    };
    let err = build_sink_config(
        &reg,
        laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap_err();
    assert!(err.to_string().contains("Invalid format"));
}

#[test]
fn test_build_source_config_case_insensitive_format() {
    // Avro, avro, AVRO should all work
    for fmt in ["avro", "AVRO", "Avro"] {
        let reg = SourceRegistration {
            name: "s".to_string(),
            connector_type: Some("kafka".to_string()),
            connector_options: HashMap::new(),
            format: Some(fmt.to_string()),
            format_options: HashMap::new(),
        };
        let config = build_source_config(&reg).unwrap();
        assert_eq!(config.get("format"), Some("avro"));
    }
}

#[test]
fn test_build_table_config_valid() {
    let reg = TableRegistration {
        name: "instruments".to_string(),
        primary_key: "symbol".to_string(),
        connector_type: Some("KAFKA".to_string()),
        connector_options: HashMap::from([("topic".to_string(), "instruments".to_string())]),
        format: Some("JSON".to_string()),
        format_options: HashMap::new(),
        on_demand: false,
        cache_max_bytes: None,
        cache_ttl: None,
    };
    let config = build_table_config(&reg).unwrap();
    assert_eq!(config.connector_type(), "kafka");
    assert_eq!(config.get("topic"), Some("instruments"));
    assert_eq!(config.get("format"), Some("json"));
}

#[test]
fn test_build_table_config_missing_type() {
    let reg = TableRegistration {
        name: "t".to_string(),
        primary_key: "id".to_string(),
        connector_type: None,
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        on_demand: false,
        cache_max_bytes: None,
        cache_ttl: None,
    };
    let err = build_table_config(&reg).unwrap_err();
    assert!(err.to_string().contains("no connector type"));
}

#[test]
fn connector_type_normalization_preserves_provider_identifiers() {
    assert_eq!(normalize_connector_type("delta-lake"), "delta-lake");
    assert_eq!(normalize_connector_type("delta_lake"), "delta_lake");
    assert_eq!(normalize_connector_type("DELTA_LAKE"), "delta_lake");
    assert_eq!(normalize_connector_type("DELTA-LAKE"), "delta-lake");
    assert_eq!(normalize_connector_type("Vendor_V2"), "vendor_v2");
}

#[test]
fn test_normalize_connector_type_simple_names() {
    // Names without hyphens or underscores are just lowercased.
    assert_eq!(normalize_connector_type("kafka"), "kafka");
    assert_eq!(normalize_connector_type("KAFKA"), "kafka");
    assert_eq!(normalize_connector_type("websocket"), "websocket");
}

#[test]
fn test_normalize_connector_type_hyphenated() {
    assert_eq!(normalize_connector_type("postgres-cdc"), "postgres-cdc");
    assert_eq!(normalize_connector_type("POSTGRES-CDC"), "postgres-cdc");
    assert_eq!(normalize_connector_type("postgres-sink"), "postgres-sink");
    assert_eq!(normalize_connector_type("POSTGRES-SINK"), "postgres-sink");
}

#[test]
fn test_build_source_config_normalizes_case_only() {
    let reg = SourceRegistration {
        name: "cdc".to_string(),
        connector_type: Some("POSTGRES-CDC".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
    };
    let config = build_source_config(&reg).unwrap();
    assert_eq!(config.connector_type(), "postgres-cdc");
}

#[test]
fn test_build_sink_config_normalizes_case_only() {
    let reg = SinkRegistration {
        name: "lake".to_string(),
        input: "events".to_string(),
        query_inputs: Vec::new(),
        connector_type: Some("DELTA-LAKE".to_string()),
        connector_options: HashMap::new(),
        format: None,
        format_options: HashMap::new(),
        filter_expr: None,
    };
    let config = build_sink_config(
        &reg,
        laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce,
    )
    .unwrap();
    assert_eq!(config.connector_type(), "delta-lake");
}
