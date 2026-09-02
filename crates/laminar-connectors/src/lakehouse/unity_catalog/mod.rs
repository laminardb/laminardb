//! Unity Catalog REST client for auto-creating external Delta tables.
//!
//! When the sink targets a `uc://` table that doesn't yet exist in Unity
//! Catalog, this module creates it via the Databricks REST API before
//! delta-rs opens it.
//!
//! Endpoint: `POST /api/2.1/unity-catalog/tables/`

use std::time::Duration;

use arrow_schema::{DataType, SchemaRef};
use serde_json::json;
use tracing::info;

use crate::error::ConnectorError;

const MAX_ERROR_RESPONSE_BYTES: usize = 64 * 1024;
const ALREADY_EXISTS_MARKER: &[u8] = b"ALREADY_EXISTS";

/// Builds a shared `reqwest::Client` with a 30-second timeout.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client")
}

fn request_error(error: &reqwest::Error) -> ConnectorError {
    let class = if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connection"
    } else if error.is_request() {
        "request"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else {
        "transport"
    };
    ConnectorError::ConnectionFailed(format!(
        "Unity Catalog REST request failed ({class}); verify the configured workspace endpoint"
    ))
}

async fn response_reports_already_exists(
    mut response: reqwest::Response,
) -> Result<bool, ConnectorError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| {
        ConnectorError::ConnectionFailed(
            "Unity Catalog error response body could not be read".into(),
        )
    })? {
        let remaining = MAX_ERROR_RESPONSE_BYTES.saturating_sub(body.len());
        body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if body
            .windows(ALREADY_EXISTS_MARKER.len())
            .any(|window| window == ALREADY_EXISTS_MARKER)
        {
            return Ok(true);
        }
        if chunk.len() > remaining {
            return Err(ConnectorError::ConnectionFailed(format!(
                "Unity Catalog error response exceeds the {MAX_ERROR_RESPONSE_BYTES}-byte limit"
            )));
        }
    }
    Ok(false)
}

fn storage_provider_name(location: &str) -> &'static str {
    laminar_core::storage_location::StorageProvider::detect_uri(location)
        .map_or("unknown", |provider| provider.name())
}

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
            // Decimal needs precision/scale in type_text: "decimal(10,2)"
            let (precision, scale, type_text) = match field.data_type() {
                DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                    (i64::from(*p), i64::from(*s), format!("decimal({p},{s})"))
                }
                _ => (0, 0, type_text.to_string()),
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
/// Sends `POST /api/2.1/unity-catalog/tables/` with the table metadata.
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
        "{}/api/2.1/unity-catalog/tables/",
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
        storage_provider = storage_provider_name(storage_location),
        "creating external Delta table in Unity Catalog"
    );

    let client = http_client();
    let resp = client
        .post(&url)
        .bearer_auth(access_token)
        .json(&body)
        .send()
        .await
        .map_err(|error| request_error(&error))?;

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

    // Already-exists can also come as 400 with ALREADY_EXISTS error code.
    if status.as_u16() == 400 && response_reports_already_exists(resp).await? {
        info!(
            catalog = catalog_name,
            schema = schema_name,
            table = table_name,
            "table already exists in Unity Catalog, proceeding"
        );
        return Ok(());
    }

    if status.as_u16() == 401 || status.as_u16() == 403 {
        // Non-transient — credentials are wrong, retry won't help.
        return Err(ConnectorError::ConfigurationError(format!(
            "Unity Catalog auth failed (HTTP {status}); verify the configured access token"
        )));
    }

    Err(ConnectorError::ConnectionFailed(format!(
        "Unity Catalog create table failed (HTTP {status})"
    )))
}

/// Resolves a Unity Catalog table's storage location via the REST API.
///
/// Calls `GET /api/2.1/unity-catalog/tables/{full_name}` and extracts
/// the `storage_location` field from the response. This bypasses
/// delta-rs's built-in `uc://` handling which requires credential vending
/// (denied outside Databricks compute).
pub(crate) async fn get_table_storage_location(
    workspace_url: &str,
    access_token: &str,
    full_table_name: &str,
) -> Result<String, ConnectorError> {
    let url = format!(
        "{}/api/2.1/unity-catalog/tables/{}",
        workspace_url.trim_end_matches('/'),
        full_table_name,
    );

    let client = http_client();
    let resp = client
        .get(&url)
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|error| request_error(&error))?;

    let status = resp.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        // Non-transient — credentials are wrong, retry won't help.
        return Err(ConnectorError::ConfigurationError(format!(
            "Unity Catalog auth failed (HTTP {status}); verify the configured access token"
        )));
    }

    if !status.is_success() {
        return Err(ConnectorError::ConnectionFailed(format!(
            "Unity Catalog get table failed (HTTP {status})"
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
        storage_provider = storage_provider_name(&location),
        "resolved storage location from Unity Catalog"
    );

    Ok(location)
}

#[cfg(test)]
mod tests;
