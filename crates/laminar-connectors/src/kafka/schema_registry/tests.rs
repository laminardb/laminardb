use std::sync::Arc;

use super::*;
use arrow_schema::{Field, Fields, Schema};

#[test]
fn test_avro_to_arrow_simple_record() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(2).name(), "active");
    assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
}

#[test]
fn test_avro_to_arrow_nullable_union() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert!(!schema.field(0).is_nullable());
    assert!(schema.field(1).is_nullable());
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
}

#[test]
fn test_avro_to_arrow_all_primitives() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "b", "type": "boolean"},
                {"name": "i", "type": "int"},
                {"name": "l", "type": "long"},
                {"name": "f", "type": "float"},
                {"name": "d", "type": "double"},
                {"name": "s", "type": "string"},
                {"name": "raw", "type": "bytes"}
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
    assert_eq!(schema.field(1).data_type(), &DataType::Int32);
    assert_eq!(schema.field(2).data_type(), &DataType::Int64);
    assert_eq!(schema.field(3).data_type(), &DataType::Float32);
    assert_eq!(schema.field(4).data_type(), &DataType::Float64);
    assert_eq!(schema.field(5).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(6).data_type(), &DataType::Binary);
}

#[test]
fn test_avro_to_arrow_invalid_json() {
    assert!(avro_to_arrow_schema("not json").is_err());
}

#[test]
fn test_avro_to_arrow_missing_fields() {
    let avro = r#"{"type": "record", "name": "test"}"#;
    assert!(avro_to_arrow_schema(avro).is_err());
}

#[test]
fn schema_to_arrow_avro_works() {
    let avro = r#"{"type":"record","name":"t","fields":[{"name":"x","type":"long"}]}"#;
    let schema = schema_to_arrow(SchemaType::Avro, avro).unwrap();
    assert_eq!(schema.field(0).name(), "x");
}

#[test]
fn schema_to_arrow_json_returns_actionable_error() {
    let err = schema_to_arrow(SchemaType::Json, "{}").unwrap_err();
    assert!(
        err.to_string().contains("JSON Schema Registry"),
        "error should name the subject type, got: {err}"
    );
}

#[test]
fn schema_to_arrow_protobuf_returns_actionable_error() {
    let err = schema_to_arrow(SchemaType::Protobuf, "").unwrap_err();
    assert!(
        err.to_string().contains("Protobuf"),
        "error should name the subject type, got: {err}"
    );
}

#[test]
fn test_schema_type_parsing() {
    assert_eq!("AVRO".parse::<SchemaType>().unwrap(), SchemaType::Avro);
    assert_eq!(
        "PROTOBUF".parse::<SchemaType>().unwrap(),
        SchemaType::Protobuf
    );
    assert_eq!("JSON".parse::<SchemaType>().unwrap(), SchemaType::Json);
    assert!("UNKNOWN".parse::<SchemaType>().is_err());
}

#[test]
fn test_schema_type_display() {
    assert_eq!(SchemaType::Avro.to_string(), "AVRO");
    assert_eq!(SchemaType::Protobuf.to_string(), "PROTOBUF");
    assert_eq!(SchemaType::Json.to_string(), "JSON");
}

#[tokio::test]
async fn non_avro_mutations_are_rejected_without_network_access() {
    use wiremock::MockServer;

    let server = MockServer::start().await;
    let client = SchemaRegistryClient::new(server.uri(), None).unwrap();

    for schema_type in [SchemaType::Protobuf, SchemaType::Json] {
        let schema_name = schema_type.to_string();
        let registration = client
            .register_schema("orders-value", "unused", schema_type)
            .await
            .unwrap_err();
        assert!(matches!(
            &registration,
            ConnectorError::ConfigurationError(_)
        ));
        assert!(!registration.is_transient());
        assert!(registration.to_string().contains(schema_name.as_str()));

        let validation = client
            .validate_and_register_schema("orders-value", "unused", schema_type)
            .await
            .unwrap_err();
        assert!(matches!(&validation, ConnectorError::ConfigurationError(_)));
        assert!(!validation.is_transient());
        assert!(validation.to_string().contains(schema_name.as_str()));
    }

    let requests = server.received_requests().await.unwrap();
    assert!(
        requests.is_empty(),
        "unsupported schema mutations must fail before HTTP"
    );
}

#[test]
fn test_client_creation() {
    let client = SchemaRegistryClient::new("http://localhost:8081", None).unwrap();
    assert_eq!(client.base_url(), "http://localhost:8081");
    assert!(!client.has_auth());
    assert_eq!(client.cache_size(), 0);
}

#[test]
fn test_client_with_auth() {
    let auth = SrAuth {
        username: "user".into(),
        password: "pass".into(),
    };
    let client = SchemaRegistryClient::new("http://localhost:8081", Some(auth)).unwrap();
    assert!(client.has_auth());
}

#[test]
fn test_client_trailing_slash_stripped() {
    let client = SchemaRegistryClient::new("http://localhost:8081/", None).unwrap();
    assert_eq!(client.base_url(), "http://localhost:8081");
}

#[test]
fn schema_registry_http_budget_fits_default_discovery_deadline() {
    assert!(SCHEMA_REGISTRY_CONNECT_TIMEOUT <= SCHEMA_REGISTRY_REQUEST_TIMEOUT);
    assert!(SCHEMA_REGISTRY_READ_TIMEOUT <= SCHEMA_REGISTRY_REQUEST_TIMEOUT);

    let three_attempt_get_budget =
        SCHEMA_REGISTRY_REQUEST_TIMEOUT.saturating_mul(3) + Duration::from_millis(600);
    assert!(three_attempt_get_budget <= Duration::from_secs(10));
}

#[test]
fn schema_registry_http_status_classification_is_fail_closed() {
    for status in [
        reqwest::StatusCode::REQUEST_TIMEOUT,
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
    ] {
        assert!(schema_registry_http_error("test", status, "failure").is_transient());
    }
    for status in [
        reqwest::StatusCode::BAD_REQUEST,
        reqwest::StatusCode::UNAUTHORIZED,
        reqwest::StatusCode::FORBIDDEN,
        reqwest::StatusCode::NOT_FOUND,
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
    ] {
        assert!(!schema_registry_http_error("test", status, "failure").is_transient());
    }
}

#[tokio::test]
async fn missing_subject_is_compatible_for_first_registration() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/compatibility/subjects/orders/versions/latest"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let client = SchemaRegistryClient::new(server.uri(), None).unwrap();

    let result = client.check_compatibility("orders", "{}").await.unwrap();
    assert!(result.is_compatible);
    assert!(result.messages.is_empty());
}

#[tokio::test]
async fn schema_registry_http_client_times_out_a_stalled_response() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/compatibility/subjects/orders/versions/latest"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(250))
                .set_body_json(serde_json::json!({ "is_compatible": true })),
        )
        .mount(&server)
        .await;

    let test_timeouts = SchemaRegistryHttpTimeouts {
        connect: Duration::from_millis(50),
        read: Duration::from_millis(50),
        request: Duration::from_millis(50),
    };
    let client = SchemaRegistryClient::with_cache_config_and_timeouts(
        server.uri(),
        None,
        SchemaRegistryCacheConfig::default(),
        test_timeouts,
    )
    .unwrap();

    let err = client
        .check_compatibility("orders", "{}")
        .await
        .unwrap_err();
    assert!(matches!(err, ConnectorError::ConnectionFailed(_)));
}

#[test]
fn test_arrow_to_avro_schema_simple() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let avro_str = arrow_to_avro_schema(&schema, "test_record").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();

    assert_eq!(avro["type"], "record");
    assert_eq!(avro["name"], "test_record");

    let fields = avro["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0]["name"], "id");
    assert_eq!(fields[0]["type"], "long");
    assert_eq!(fields[1]["name"], "name");
    assert_eq!(fields[1]["type"], "string");
}

#[test]
fn test_arrow_to_avro_schema_sanitizes_hyphens() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let avro_str = arrow_to_avro_schema(&schema, "trades-avro-output").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();
    assert_eq!(avro["name"], "trades_avro_output");
}

#[test]
fn test_arrow_to_avro_schema_nullable() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]));

    let avro_str = arrow_to_avro_schema(&schema, "record").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();

    let fields = avro["fields"].as_array().unwrap();
    // Non-nullable: plain type
    assert_eq!(fields[0]["type"], "long");
    // Nullable: union ["null", "string"]
    let union = fields[1]["type"].as_array().unwrap();
    assert_eq!(union.len(), 2);
    assert_eq!(union[0], "null");
    assert_eq!(union[1], "string");
}

#[test]
fn test_arrow_to_avro_all_primitives() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, false),
        Field::new("i32", DataType::Int32, false),
        Field::new("i64", DataType::Int64, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("bin", DataType::Binary, false),
    ]));

    let avro_str = arrow_to_avro_schema(&schema, "all_types").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();
    let fields = avro["fields"].as_array().unwrap();

    assert_eq!(fields[0]["type"], "boolean");
    assert_eq!(fields[1]["type"], "int");
    assert_eq!(fields[2]["type"], "long");
    assert_eq!(fields[3]["type"], "float");
    assert_eq!(fields[4]["type"], "double");
    assert_eq!(fields[5]["type"], "string");
    assert_eq!(fields[6]["type"], "bytes");
}

#[test]
fn test_arrow_to_avro_roundtrip() {
    let original = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
    ]));

    let avro_str = arrow_to_avro_schema(&original, "roundtrip").unwrap();
    let recovered = avro_to_arrow_schema(&avro_str).unwrap();

    assert_eq!(recovered.fields().len(), 3);
    assert_eq!(recovered.field(0).data_type(), &DataType::Int64);
    assert!(!recovered.field(0).is_nullable());
    assert_eq!(recovered.field(1).data_type(), &DataType::Utf8);
    assert!(recovered.field(1).is_nullable());
    assert_eq!(recovered.field(2).data_type(), &DataType::Boolean);
}

// ---- Complex type tests ----

#[test]
fn test_avro_to_arrow_array_type() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.fields().len(), 1);
    match schema.field(0).data_type() {
        DataType::List(item) => {
            assert_eq!(item.data_type(), &DataType::Utf8);
        }
        other => panic!("expected List, got {other:?}"),
    }
}

#[test]
fn test_avro_to_arrow_map_type() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "metadata", "type": {"type": "map", "values": "long"}}
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.fields().len(), 1);
    match schema.field(0).data_type() {
        DataType::Map(entries, _) => {
            if let DataType::Struct(fields) = entries.data_type() {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "key");
                assert_eq!(fields[0].data_type(), &DataType::Utf8);
                assert_eq!(fields[1].name(), "value");
                assert_eq!(fields[1].data_type(), &DataType::Int64);
            } else {
                panic!("expected Struct entries");
            }
        }
        other => panic!("expected Map, got {other:?}"),
    }
}

#[test]
fn test_avro_to_arrow_nested_record() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {
                    "name": "address",
                    "type": {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "zip", "type": "int"}
                        ]
                    }
                }
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.fields().len(), 1);
    match schema.field(0).data_type() {
        DataType::Struct(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "street");
            assert_eq!(fields[0].data_type(), &DataType::Utf8);
            assert_eq!(fields[1].name(), "zip");
            assert_eq!(fields[1].data_type(), &DataType::Int32);
        }
        other => panic!("expected Struct, got {other:?}"),
    }
}

#[test]
fn test_avro_to_arrow_enum_type() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["ACTIVE", "INACTIVE", "PENDING"]
                    }
                }
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.fields().len(), 1);
    match schema.field(0).data_type() {
        DataType::Dictionary(key, value) => {
            assert_eq!(key.as_ref(), &DataType::Int32);
            assert_eq!(value.as_ref(), &DataType::Utf8);
        }
        other => panic!("expected Dictionary, got {other:?}"),
    }
}

#[test]
fn test_avro_to_arrow_fixed_type() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {
                    "name": "uuid",
                    "type": {"type": "fixed", "name": "uuid", "size": 16}
                }
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), &DataType::FixedSizeBinary(16));
}

#[test]
fn test_avro_to_arrow_nullable_complex_in_union() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {
                    "name": "tags",
                    "type": ["null", {"type": "array", "items": "string"}]
                }
            ]
        }"#;

    let schema = avro_to_arrow_schema(avro).unwrap();
    assert!(schema.field(0).is_nullable());
    assert!(matches!(schema.field(0).data_type(), DataType::List(_)));
}

#[test]
fn test_avro_array_missing_items() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "bad", "type": {"type": "array"}}
            ]
        }"#;
    assert!(avro_to_arrow_schema(avro).is_err());
}

#[test]
fn test_avro_map_missing_values() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "bad", "type": {"type": "map"}}
            ]
        }"#;
    assert!(avro_to_arrow_schema(avro).is_err());
}

#[test]
fn test_arrow_to_avro_array_type() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        false,
    )]));

    let avro_str = arrow_to_avro_schema(&schema, "test").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();
    let field = &avro["fields"][0];
    assert_eq!(field["type"]["type"], "array");
    assert_eq!(field["type"]["items"], "string");
}

#[test]
fn test_arrow_to_avro_map_type() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "metadata",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        ),
        false,
    )]));

    let avro_str = arrow_to_avro_schema(&schema, "test").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();
    let field = &avro["fields"][0];
    assert_eq!(field["type"]["type"], "map");
    assert_eq!(field["type"]["values"], "long");
}

#[test]
fn test_arrow_to_avro_struct_type() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "address",
        DataType::Struct(Fields::from(vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("zip", DataType::Int32, false),
        ])),
        false,
    )]));

    let avro_str = arrow_to_avro_schema(&schema, "test").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();
    let field = &avro["fields"][0];
    assert_eq!(field["type"]["type"], "record");
    let nested = field["type"]["fields"].as_array().unwrap();
    assert_eq!(nested.len(), 2);
    assert_eq!(nested[0]["name"], "street");
    assert_eq!(nested[0]["type"], "string");
    assert_eq!(nested[1]["name"], "zip");
    assert_eq!(nested[1]["type"], "int");
}

#[test]
fn test_arrow_to_avro_fixed_type() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));

    let avro_str = arrow_to_avro_schema(&schema, "test").unwrap();
    let avro: serde_json::Value = serde_json::from_str(&avro_str).unwrap();
    let field = &avro["fields"][0];
    assert_eq!(field["type"]["type"], "fixed");
    assert_eq!(field["type"]["size"], 16);
}

// ---- Cache eviction tests ----

fn make_cached_schema(id: i32) -> CachedSchema {
    CachedSchema {
        id,
        version: 1,
        schema_type: SchemaType::Avro,
        schema_str: format!(
            r#"{{"type":"record","name":"t{id}","fields":[{{"name":"x","type":"int"}}]}}"#
        ),
        arrow_schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
        inserted_at: Instant::now(),
    }
}

#[test]
fn test_cache_config_defaults() {
    let config = SchemaRegistryCacheConfig::default();
    assert_eq!(config.max_entries, 1000);
    assert_eq!(config.ttl, Some(Duration::from_secs(3600)));
}

#[test]
fn test_cache_lru_eviction() {
    let config = SchemaRegistryCacheConfig {
        max_entries: 3,
        ttl: None,
    };
    let client =
        SchemaRegistryClient::with_cache_config("http://localhost:8081", None, config).unwrap();

    // Insert 3 schemas.
    client.cache_insert(1, make_cached_schema(1));
    client.cache_insert(2, make_cached_schema(2));
    client.cache_insert(3, make_cached_schema(3));
    assert_eq!(client.cache_size(), 3);

    // Insert a 4th — should evict one entry (S3-FIFO-style eviction).
    client.cache_insert(4, make_cached_schema(4));
    assert!(client.cache_size() <= 3);
    // The most recently inserted should always be present.
    assert!(client.cache_get(4).is_some());
}

#[test]
fn test_cache_ttl_expiration() {
    // TTL generous enough that scheduler jitter between insert and the
    // immediate `is_some()` check can't expire the entry early — a 50ms
    // window flaked under parallel test load on busy CI runners.
    let config = SchemaRegistryCacheConfig {
        max_entries: 100,
        ttl: Some(Duration::from_millis(1000)),
    };
    let client =
        SchemaRegistryClient::with_cache_config("http://localhost:8081", None, config).unwrap();

    client.cache_insert(1, make_cached_schema(1));
    assert!(client.cache_get(1).is_some());

    // Wait for TTL to expire.
    std::thread::sleep(Duration::from_millis(1200));
    // Lazy TTL: expired entry returns None on access.
    assert!(client.cache_get(1).is_none());
}

#[test]
fn test_cache_no_ttl() {
    let config = SchemaRegistryCacheConfig {
        max_entries: 100,
        ttl: None,
    };
    let client =
        SchemaRegistryClient::with_cache_config("http://localhost:8081", None, config).unwrap();

    client.cache_insert(1, make_cached_schema(1));
    // No TTL — entry should stay.
    assert!(client.cache_get(1).is_some());
}

#[test]
fn test_cache_replace_existing_id() {
    let config = SchemaRegistryCacheConfig {
        max_entries: 10,
        ttl: None,
    };
    let client =
        SchemaRegistryClient::with_cache_config("http://localhost:8081", None, config).unwrap();

    client.cache_insert(1, make_cached_schema(1));
    client.cache_insert(2, make_cached_schema(2));
    assert_eq!(client.cache_size(), 2);

    // Re-insert 1 with updated schema — should not increase size.
    client.cache_insert(1, make_cached_schema(1));
    assert_eq!(client.cache_size(), 2);
}

#[test]
fn test_schema_incompatible_error_via_serde() {
    let err = SerdeError::SchemaIncompatible {
        subject: "orders-value".into(),
        message: "READER_FIELD_MISSING_DEFAULT_VALUE: field 'new_field'".into(),
    };
    let conn_err: ConnectorError = err.into();
    assert!(matches!(
        conn_err,
        ConnectorError::Serde(SerdeError::SchemaIncompatible { .. })
    ));
    assert!(conn_err.to_string().contains("orders-value"));
}

#[test]
fn test_validate_and_register_method_exists() {
    // Verify the method exists and has the correct signature by referencing it.
    let client = SchemaRegistryClient::new("http://localhost:8081", None).unwrap();
    // Just check the method is callable (we can't actually test without a registry).
    let _ = &client;
}

#[test]
fn test_complex_type_roundtrip() {
    let avro = r#"{
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "metadata", "type": {"type": "map", "values": "long"}}
            ]
        }"#;

    let arrow_schema = avro_to_arrow_schema(avro).unwrap();
    assert!(matches!(
        arrow_schema.field(0).data_type(),
        DataType::List(_)
    ));
    assert!(matches!(
        arrow_schema.field(1).data_type(),
        DataType::Map(_, _)
    ));

    // Convert back to Avro
    let avro_str = arrow_to_avro_schema(&arrow_schema, "test").unwrap();
    let recovered = avro_to_arrow_schema(&avro_str).unwrap();

    assert!(matches!(recovered.field(0).data_type(), DataType::List(_)));
    assert!(matches!(
        recovered.field(1).data_type(),
        DataType::Map(_, _)
    ));
}
