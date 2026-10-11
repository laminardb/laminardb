use super::*;
use crate::lakehouse::delta_config::{UnityAuth, UnityCatalogConfig, UnityStorageCredentials};
use crate::lakehouse::delta_io::CatalogAccess;
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

fn unity(
    server: &MockServer,
    auth: UnityAuth,
    credentials: UnityStorageCredentials,
) -> UnityCatalogConfig {
    UnityCatalogConfig {
        workspace_url: server.uri(),
        auth,
        storage_credentials: credentials,
    }
}

fn token() -> UnityAuth {
    UnityAuth::Token("configured-token".into())
}

fn table_json(table_type: &str, format: &str) -> serde_json::Value {
    serde_json::json!({
        "name": "events", "catalog_name": "main", "schema_name": "lake",
        "table_type": table_type, "data_source_format": format, "columns": [],
        "storage_location": "s3://bucket/lake/events", "properties": {},
        "created_at": 0, "updated_at": 0, "table_id": "6f1b2c3d-uc-table"
    })
}

async fn mount_table(server: &MockServer, table_type: &str, format: &str) {
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables/main.lake.events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(table_json(table_type, format)))
        .mount(server)
        .await;
}

fn vended(expires_in: chrono::Duration) -> serde_json::Value {
    serde_json::json!({
        "aws_temp_credentials": {
            "access_key_id": "ASIAVENDED", "secret_access_key": "vended-secret",
            "session_token": "vended-session"
        },
        "expiration_time": (chrono::Utc::now() + expires_in).timestamp_millis(),
        "url": "s3://bucket/lake/events"
    })
}

const TABLE: &str = "uc://main.lake.events";

/// Vended credentials are requested for the table and access mode, merged into the storage
/// options, and carry their expiry so callers renew them before storage I/O.
#[tokio::test]
async fn vending_is_scoped_to_the_table_and_operation() {
    use wiremock::matchers::body_partial_json;

    let server = MockServer::start().await;
    mount_table(&server, "EXTERNAL", "DELTA").await;
    for operation in ["READ", "READ_WRITE"] {
        Mock::given(method("POST"))
            .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
            .and(body_partial_json(serde_json::json!({
                "table_id": "6f1b2c3d-uc-table", "operation": operation
            })))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(vended(chrono::Duration::hours(1))),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    let config = unity(&server, token(), UnityStorageCredentials::Vended);
    for access in [CatalogAccess::Read, CatalogAccess::Write] {
        let resolved = resolve_table(&config, TABLE, access).await.unwrap();
        assert_eq!(resolved.storage_location, "s3://bucket/lake/events");
        assert!(resolved
            .vended_options
            .values()
            .any(|value| value == "ASIAVENDED"));
        let expires = resolved.expires_at.unwrap();
        assert!(!crate::lakehouse::delta_io::credentials_need_refresh(Some(
            expires
        )));
    }
    assert!(crate::lakehouse::delta_io::credentials_need_refresh(Some(
        std::time::SystemTime::now() + std::time::Duration::from_secs(60)
    )));
}

#[tokio::test]
async fn configured_credentials_never_call_the_vending_endpoint() {
    let server = MockServer::start().await;
    mount_table(&server, "EXTERNAL", "DELTA").await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let config = unity(&server, token(), UnityStorageCredentials::Configured);
    let resolved = resolve_table(&config, TABLE, CatalogAccess::Write)
        .await
        .unwrap();
    assert!(resolved.vended_options.is_empty());
    assert!(resolved.expires_at.is_none());
}

/// Managed tables are readable through vended credentials only, and never writable; other
/// kinds and formats are rejected before any storage access.
#[tokio::test]
async fn table_kind_and_format_gate_each_access_mode() {
    let table = |table_type: &str, format: &str| {
        serde_json::from_value::<deltalake::unity_catalog::models::Table>(table_json(
            table_type, format,
        ))
        .unwrap()
    };
    let cases = [
        (
            "EXTERNAL",
            "DELTA",
            CatalogAccess::Write,
            UnityStorageCredentials::Configured,
            None,
        ),
        (
            "MANAGED",
            "DELTA",
            CatalogAccess::Read,
            UnityStorageCredentials::Vended,
            None,
        ),
        (
            "MANAGED",
            "DELTA",
            CatalogAccess::Read,
            UnityStorageCredentials::Configured,
            Some("vended"),
        ),
        (
            "MANAGED",
            "DELTA",
            CatalogAccess::Write,
            UnityStorageCredentials::Vended,
            Some("catalog-managed"),
        ),
        (
            "VIEW",
            "DELTA",
            CatalogAccess::Read,
            UnityStorageCredentials::Vended,
            Some("View"),
        ),
        (
            "EXTERNAL",
            "PARQUET",
            CatalogAccess::Read,
            UnityStorageCredentials::Vended,
            Some("not Delta"),
        ),
    ];
    for (kind, format, access, credentials, rejected) in cases {
        let result = resolve::validate_table(&table(kind, format), access, credentials);
        match (result, rejected) {
            (Ok(()), None) => {}
            (Err(error), Some(needle)) => {
                assert!(
                    error.to_string().contains(needle),
                    "{kind}/{format}: {error}"
                );
            }
            (result, _) => panic!("{kind}/{format}/{access:?}/{credentials:?}: {result:?}"),
        }
    }
}

/// A refused vending request fails without falling back to raw storage access or echoing the
/// server's message.
#[tokio::test]
async fn refused_vending_fails_closed_without_echoing_the_response() {
    let server = MockServer::start().await;
    mount_table(&server, "EXTERNAL", "DELTA").await;
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error_code": "PERMISSION_DENIED",
            "message": "contains sig=do-not-disclose"
        })))
        .mount(&server)
        .await;
    let config = unity(&server, token(), UnityStorageCredentials::Vended);
    let error = resolve_table(&config, TABLE, CatalogAccess::Read)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("PERMISSION_DENIED"), "{error}");
    assert!(error.contains("EXTERNAL USE SCHEMA"), "{error}");
    assert!(!error.contains("do-not-disclose"), "{error}");
}

#[tokio::test]
async fn catalog_auth_error_does_not_echo_response_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables/main.lake.events"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error_code": "PERMISSION_DENIED",
            "message": "signed URL contains sig=do-not-disclose"
        })))
        .mount(&server)
        .await;
    let config = unity(&server, token(), UnityStorageCredentials::Vended);
    let error = resolve_table(&config, TABLE, CatalogAccess::Read)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("PERMISSION_DENIED"), "{error}");
    assert!(!error.contains("do-not-disclose"), "{error}");
    assert!(!error.contains("configured-token"), "{error}");
}

/// OAuth machine-to-machine credentials obtain a workspace token that authorizes catalog calls.
#[tokio::test]
async fn oauth_client_credentials_authorize_catalog_requests() {
    use wiremock::matchers::header;

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oidc/v1/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "oauth-workspace-token", "token_type": "Bearer", "expires_in": 3600
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables/main.lake.events"))
        .and(header("authorization", "Bearer oauth-workspace-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(table_json("EXTERNAL", "DELTA")))
        .expect(1)
        .mount(&server)
        .await;
    let config = unity(
        &server,
        UnityAuth::OAuth {
            client_id: "sp-1".into(),
            client_secret: "sp-secret".into(),
        },
        UnityStorageCredentials::Configured,
    );
    let table = find_table(&config, TABLE).await.unwrap().unwrap();
    assert_eq!(table.table_id, "6f1b2c3d-uc-table");
}

#[tokio::test]
async fn absent_tables_and_insecure_endpoints_are_distinguished() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error_code": "TABLE_DOES_NOT_EXIST", "message": "no such table"
        })))
        .mount(&server)
        .await;
    let config = unity(&server, token(), UnityStorageCredentials::Vended);
    assert!(find_table(&config, TABLE).await.unwrap().is_none());
    assert!(find_table(&config, "uc://main.events").await.is_err());

    let remote_http = UnityCatalogConfig {
        workspace_url: "http://workspace.example".into(),
        ..config
    };
    let error = find_table(&remote_http, TABLE).await.unwrap_err();
    assert!(error.to_string().contains("https"), "{error}");
}
