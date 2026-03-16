//! Unity Catalog REST client for auto-creating external Delta tables.
//!
//! When the sink targets a `uc://` table that doesn't yet exist in Unity
//! Catalog, this module creates it via the Databricks REST API before
//! delta-rs opens it.
//!
//! Endpoint: `POST /api/2.0/unity-catalog/tables/`

use arrow_schema::{DataType, SchemaRef};
use serde_json::json;
use tracing::info;

use crate::error::ConnectorError;

/// Converts an Arrow `SchemaRef` into Unity Catalog `ColumnInfo` JSON objects.
///
/// Each column gets `type_name`, `type_text`, `type_json`, `type_precision`,
/// `type_scale`, `position`, and `nullable` fields as required by the
/// Databricks Unity Catalog REST API.
pub(crate) fn arrow_to_uc_columns(schema: &SchemaRef) -> Vec<serde_json::Value> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(pos, field)| {
            let (type_name, type_text) = arrow_type_to_uc(field.data_type());
            let (precision, scale) = match field.data_type() {
                DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                    (i64::from(*p), i64::from(*s))
                }
                _ => (0, 0),
            };
            json!({
                "name": field.name(),
                "type_name": type_name,
                "type_text": type_text,
                "type_json": format!("\"{type_text}\""),
                "type_precision": precision,
                "type_scale": scale,
                "position": pos,
                "nullable": field.is_nullable(),
            })
        })
        .collect()
}

/// Maps an Arrow `DataType` to `(UC type_name, UC type_text)`.
#[allow(clippy::match_same_arms)] // explicit arms document the mapping
fn arrow_type_to_uc(dt: &DataType) -> (&'static str, &'static str) {
    match dt {
        DataType::Boolean => ("BOOLEAN", "boolean"),
        DataType::Int8 => ("BYTE", "tinyint"),
        DataType::Int16 | DataType::UInt8 => ("SHORT", "smallint"),
        DataType::Int32 | DataType::UInt16 => ("INT", "int"),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => ("LONG", "bigint"),
        DataType::Float16 | DataType::Float32 => ("FLOAT", "float"),
        DataType::Float64 => ("DOUBLE", "double"),
        DataType::Decimal128(..) | DataType::Decimal256(..) => ("DECIMAL", "decimal"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ("STRING", "string"),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => ("BINARY", "binary"),
        DataType::Date32 | DataType::Date64 => ("DATE", "date"),
        DataType::Timestamp(_, Some(_)) => ("TIMESTAMP", "timestamp"),
        DataType::Timestamp(_, None) => ("TIMESTAMP_NTZ", "timestamp_ntz"),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            ("ARRAY", "array")
        }
        DataType::Map(_, _) => ("MAP", "map"),
        DataType::Struct(_) => ("STRUCT", "struct"),
        DataType::Null => ("NULL", "null"),
        // Duration, Interval, RunEndEncoded, etc. — use STRING fallback.
        _ => ("STRING", "string"),
    }
}

/// Creates an external Delta table in Unity Catalog via the REST API.
///
/// Sends `POST /api/2.0/unity-catalog/tables/` with the table metadata.
/// Treats HTTP 200 as success and HTTP 409 (already exists) as idempotent
/// success. All other errors are propagated.
pub(crate) async fn create_uc_table(
    workspace_url: &str,
    access_token: &str,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    storage_location: &str,
    columns: &[serde_json::Value],
) -> Result<(), ConnectorError> {
    let url = format!(
        "{}/api/2.0/unity-catalog/tables/",
        workspace_url.trim_end_matches('/')
    );

    let body = json!({
        "name": table_name,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "storage_location": storage_location,
        "columns": columns,
    });

    info!(
        catalog = catalog_name,
        schema = schema_name,
        table = table_name,
        storage_location,
        "creating external Delta table in Unity Catalog"
    );

    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .bearer_auth(access_token)
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            ConnectorError::ConnectionFailed(format!("Unity Catalog REST request failed: {e}"))
        })?;

    let status = resp.status();
    if status.is_success() {
        info!(
            catalog = catalog_name,
            schema = schema_name,
            table = table_name,
            "created table in Unity Catalog"
        );
        return Ok(());
    }

    // 409 Conflict = table already exists (race with another sink instance).
    if status.as_u16() == 409 {
        info!(
            catalog = catalog_name,
            schema = schema_name,
            table = table_name,
            "table already exists in Unity Catalog (409), proceeding"
        );
        return Ok(());
    }

    // Read body for error details.
    let error_body = resp.text().await.unwrap_or_default();

    // Already-exists can also come as 400 with ALREADY_EXISTS error code.
    if error_body.contains("ALREADY_EXISTS") {
        info!(
            catalog = catalog_name,
            schema = schema_name,
            table = table_name,
            "table already exists in Unity Catalog, proceeding"
        );
        return Ok(());
    }

    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(ConnectorError::AuthenticationFailed(format!(
            "Unity Catalog auth failed (HTTP {status}): {error_body}"
        )));
    }

    Err(ConnectorError::ConnectionFailed(format!(
        "Unity Catalog create table failed (HTTP {status}): {error_body}"
    )))
}

/// Resolves a Unity Catalog table's storage location via the REST API.
///
/// Calls `GET /api/2.0/unity-catalog/tables/{full_name}` and extracts
/// the `storage_location` field from the response. This bypasses
/// delta-rs's built-in `uc://` handling which requires credential vending
/// (denied outside Databricks compute).
pub(crate) async fn get_table_storage_location(
    workspace_url: &str,
    access_token: &str,
    full_table_name: &str,
) -> Result<String, ConnectorError> {
    let url = format!(
        "{}/api/2.0/unity-catalog/tables/{}",
        workspace_url.trim_end_matches('/'),
        full_table_name,
    );

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|e| {
            ConnectorError::ConnectionFailed(format!("Unity Catalog REST request failed: {e}"))
        })?;

    let status = resp.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::AuthenticationFailed(format!(
            "Unity Catalog auth failed (HTTP {status}): {body}"
        )));
    }

    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::ConnectionFailed(format!(
            "Unity Catalog get table failed (HTTP {status}): {body}"
        )));
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| {
        ConnectorError::ConnectionFailed(format!("failed to parse UC response: {e}"))
    })?;

    let location = body["storage_location"]
        .as_str()
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "Unity Catalog table response missing 'storage_location' field".into(),
            )
        })?
        .to_string();

    info!(
        table = full_table_name,
        storage_location = %location,
        "resolved storage location from Unity Catalog"
    );

    Ok(location)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema, TimeUnit};
    use std::sync::Arc;

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
        assert_eq!(cols[0]["type_text"], "decimal");
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
}
