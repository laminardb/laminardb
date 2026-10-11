use super::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
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

fn json_type(column: &ColumnInfo) -> serde_json::Value {
    serde_json::from_str(column.type_json.as_deref().unwrap()).unwrap()
}

#[test]
fn primitive_columns_use_delta_types_and_catalog_strings() {
    let cols = arrow_to_uc_columns(&test_schema(), &["name".to_string()]).unwrap();
    assert_eq!(cols.len(), 4);
    assert_eq!(cols[0].type_name, Some(ColumnTypeName::Long));
    assert_eq!(cols[0].type_text.as_deref(), Some("bigint"));
    assert_eq!(
        json_type(&cols[0]),
        serde_json::json!({"name": "id", "type": "long", "nullable": false, "metadata": {}})
    );
    assert_eq!((cols[0].position, cols[0].nullable), (0, false));
    assert_eq!(cols[1].partition_index, Some(0));
    assert_eq!(cols[0].partition_index, None);
    assert_eq!(cols[2].type_text.as_deref(), Some("double"));
    // Millisecond input is registered as the microsecond UTC timestamp Delta stores.
    assert_eq!(cols[3].type_name, Some(ColumnTypeName::Timestamp));
    assert_eq!(json_type(&cols[3])["type"], "timestamp");
}

#[test]
fn decimal_and_ntz_columns_keep_precision_and_kind() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Decimal128(10, 2), false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let cols = arrow_to_uc_columns(&schema, &[]).unwrap();
    assert_eq!(cols[0].type_name, Some(ColumnTypeName::Decimal));
    assert_eq!(cols[0].type_text.as_deref(), Some("decimal(10,2)"));
    assert_eq!(
        (cols[0].type_precision, cols[0].type_scale),
        (Some(10), Some(2))
    );
    assert_eq!(json_type(&cols[0])["type"], "decimal(10,2)");
    assert_eq!(cols[1].type_name, Some(ColumnTypeName::TimestampNtz));
    assert_eq!(cols[1].type_text.as_deref(), Some("timestamp_ntz"));
}

#[test]
fn nested_columns_keep_child_types_and_nullability() {
    let point = DataType::Struct(
        vec![
            Field::new("x", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
        ]
        .into(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "points",
            DataType::List(Arc::new(Field::new("element", point, true))),
            true,
        ),
        Field::new(
            "scores",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Decimal128(12, 3), true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
    ]));
    let cols = arrow_to_uc_columns(&schema, &[]).unwrap();
    assert_eq!(cols[0].type_name, Some(ColumnTypeName::Array));
    assert_eq!(
        cols[0].type_text.as_deref(),
        Some("array<struct<x:int,label:string>>")
    );
    assert_eq!(
        json_type(&cols[0])["type"],
        serde_json::json!({
            "type": "array",
            "elementType": {"type": "struct", "fields": [
                {"name": "x", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "label", "type": "string", "nullable": true, "metadata": {}}
            ]},
            "containsNull": true
        })
    );
    assert_eq!(cols[1].type_name, Some(ColumnTypeName::Map));
    assert_eq!(
        cols[1].type_text.as_deref(),
        Some("map<string,decimal(12,3)>")
    );
    assert_eq!(json_type(&cols[1])["type"]["valueContainsNull"], true);
    assert!(!cols[1].nullable);
}

#[test]
fn types_without_a_delta_representation_fail_closed() {
    for data_type in [
        DataType::Duration(TimeUnit::Second),
        DataType::Interval(arrow_schema::IntervalUnit::DayTime),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::List(Arc::new(Field::new(
            "element",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ))),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new("c", data_type.clone(), true)]));
        assert!(
            arrow_to_uc_columns(&schema, &[]).is_err(),
            "{data_type} must not be stringified or truncated"
        );
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

#[tokio::test]
async fn oversized_response_with_bounded_already_exists_marker_remains_idempotent() {
    let server = MockServer::start().await;
    let body = format!("ALREADY_EXISTS{}", "x".repeat(MAX_ERROR_RESPONSE_BYTES + 1));
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/tables/"))
        .respond_with(ResponseTemplate::new(400).set_body_string(body))
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
