use super::*;
use arrow_schema::{Field, Schema, TimeUnit};
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ]))
}

#[test]
fn test_arrow_to_uc_columns_basic() {
    let schema = test_schema();
    let cols = arrow_to_uc_columns(&schema);

    assert_eq!(cols.len(), 4);

    assert_eq!(cols[0]["name"], "id");
    assert_eq!(cols[0]["type_name"], "LONG");
    assert_eq!(cols[0]["type_text"], "bigint");
    assert_eq!(cols[0]["type_json"], "\"bigint\"");
    assert_eq!(cols[0]["position"], 0);
    assert_eq!(cols[0]["nullable"], false);

    assert_eq!(cols[1]["name"], "name");
    assert_eq!(cols[1]["type_name"], "STRING");
    assert_eq!(cols[1]["type_text"], "string");
    assert_eq!(cols[1]["nullable"], true);

    assert_eq!(cols[2]["name"], "price");
    assert_eq!(cols[2]["type_name"], "DOUBLE");
    assert_eq!(cols[2]["type_text"], "double");

    assert_eq!(cols[3]["name"], "ts");
    assert_eq!(cols[3]["type_name"], "TIMESTAMP");
    assert_eq!(cols[3]["type_text"], "timestamp");
}

#[test]
fn test_arrow_to_uc_columns_decimal() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        false,
    )]));
    let cols = arrow_to_uc_columns(&schema);

    assert_eq!(cols[0]["type_name"], "DECIMAL");
    assert_eq!(cols[0]["type_text"], "decimal(10,2)");
    assert_eq!(cols[0]["type_json"], "\"decimal(10,2)\"");
    assert_eq!(cols[0]["type_precision"], 10);
    assert_eq!(cols[0]["type_scale"], 2);
}

#[test]
fn test_arrow_to_uc_columns_timestamp_ntz() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "created_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    let cols = arrow_to_uc_columns(&schema);

    assert_eq!(cols[0]["type_name"], "TIMESTAMP_NTZ");
    assert_eq!(cols[0]["type_text"], "timestamp_ntz");
}

#[test]
fn test_arrow_to_uc_type_coverage() {
    // Verify all common types map without panic.
    let types = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
        DataType::Date64,
        DataType::Null,
    ];
    for dt in types {
        let (name, text) = arrow_type_to_uc(&dt);
        assert!(!name.is_empty());
        assert!(!text.is_empty());
    }
}

#[test]
fn test_arrow_to_uc_columns_position_sequential() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));
    let cols = arrow_to_uc_columns(&schema);
    for (i, col) in cols.iter().enumerate() {
        assert_eq!(col["position"], i);
    }
}

#[tokio::test]
async fn catalog_auth_error_does_not_echo_response_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables/catalog.schema.table"))
        .respond_with(
            ResponseTemplate::new(403).set_body_string("signed URL contains sig=do-not-disclose"),
        )
        .mount(&server)
        .await;

    let error =
        get_table_storage_location(&server.uri(), "configured-token", "catalog.schema.table")
            .await
            .unwrap_err()
            .to_string();

    assert!(error.contains("HTTP 403"));
    assert!(!error.contains("do-not-disclose"));
    assert!(!error.contains("configured-token"));
}

#[tokio::test]
async fn bounded_already_exists_response_remains_idempotent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/tables/"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error_code": "ALREADY_EXISTS",
            "message": "contains sig=do-not-disclose"
        })))
        .mount(&server)
        .await;

    create_uc_table(
        &server.uri(),
        "configured-token",
        "catalog",
        "schema",
        "table",
        "s3://bucket/table",
        &[],
    )
    .await
    .unwrap();
}
