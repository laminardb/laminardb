//! Unity Catalog integration: table resolution and credential vending through the delta-rs
//! Unity client, and registration of external Delta tables.
//!
//! The pinned client has no table-creation call, so registration posts
//! `POST /api/2.1/unity-catalog/tables/` directly.

mod resolve;

pub(crate) use resolve::{find_table, resolve_table};

use std::time::Duration;

use arrow_schema::SchemaRef;
use deltalake::kernel::{DataType as KernelType, PrimitiveType, StructType};
use deltalake::unity_catalog::models::{ColumnInfo, ColumnTypeName};
use serde_json::json;
use tracing::info;

use crate::error::ConnectorError;

const MAX_ERROR_RESPONSE_BYTES: usize = 64 * 1024;
const ALREADY_EXISTS_MARKER: &[u8] = b"ALREADY_EXISTS";

/// Builds a shared `reqwest::Client` with a 30-second timeout.
fn http_client() -> Result<reqwest::Client, ConnectorError> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| {
            ConnectorError::ConnectionFailed(
                "Unity Catalog HTTP client initialization failed".into(),
            )
        })
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

/// Maximum nesting accepted when rendering a column type; deeper schemas are rejected.
const MAX_TYPE_DEPTH: usize = 64;

/// Converts an Arrow schema into Unity Catalog column registrations.
///
/// The schema goes through the same Delta kernel conversion that creates the table, so nested
/// element, field, and value types, nullability, decimal precision/scale, and timestamp kind are
/// the ones written to the Delta log. `type_json` is the Delta/Spark `StructField` JSON and
/// `type_text` its catalog string; types without a Unity Catalog column type fail closed.
pub(crate) fn arrow_to_uc_columns(
    schema: &SchemaRef,
    partition_columns: &[String],
) -> Result<Vec<ColumnInfo>, ConnectorError> {
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;

    let normalized = super::delta_io::widen_millisecond_timestamps(schema);
    let kernel: StructType = normalized.as_ref().try_into_kernel().map_err(|error| {
        ConnectorError::SchemaMismatch(format!(
            "Unity Catalog registration cannot represent the Delta schema: {error}"
        ))
    })?;
    kernel
        .fields()
        .enumerate()
        .map(|(position, field)| {
            let type_json = serde_json::to_string(field).map_err(|error| {
                ConnectorError::SchemaMismatch(format!(
                    "column '{}' type JSON encoding failed: {error}",
                    field.name()
                ))
            })?;
            let (type_precision, type_scale) = match field.data_type() {
                KernelType::Primitive(PrimitiveType::Decimal(decimal)) => (
                    Some(i32::from(decimal.precision())),
                    Some(i32::from(decimal.scale())),
                ),
                _ => (None, None),
            };
            let partition_index = partition_columns
                .iter()
                .position(|column| column == field.name())
                .map(|index| {
                    i32::try_from(index).map_err(|_| {
                        ConnectorError::SchemaMismatch("too many partition columns".into())
                    })
                })
                .transpose()?;
            Ok(ColumnInfo {
                name: field.name().clone(),
                type_text: Some(catalog_string(field.data_type(), 0)?),
                type_json: Some(type_json),
                type_name: Some(column_type_name(field.data_type())?),
                type_precision,
                type_scale,
                position: u32::try_from(position).map_err(|_| {
                    ConnectorError::SchemaMismatch("too many Unity Catalog columns".into())
                })?,
                nullable: field.is_nullable(),
                partition_index,
                ..ColumnInfo::default()
            })
        })
        .collect()
}

fn column_type_name(data_type: &KernelType) -> Result<ColumnTypeName, ConnectorError> {
    Ok(match data_type {
        KernelType::Primitive(primitive) => match primitive {
            PrimitiveType::String => ColumnTypeName::String,
            PrimitiveType::Long => ColumnTypeName::Long,
            PrimitiveType::Integer => ColumnTypeName::Int,
            PrimitiveType::Short => ColumnTypeName::Short,
            PrimitiveType::Byte => ColumnTypeName::Byte,
            PrimitiveType::Float => ColumnTypeName::Float,
            PrimitiveType::Double => ColumnTypeName::Double,
            PrimitiveType::Boolean => ColumnTypeName::Boolean,
            PrimitiveType::Binary => ColumnTypeName::Binary,
            PrimitiveType::Date => ColumnTypeName::Date,
            PrimitiveType::Timestamp => ColumnTypeName::Timestamp,
            PrimitiveType::TimestampNtz => ColumnTypeName::TimestampNtz,
            PrimitiveType::Decimal(_) => ColumnTypeName::Decimal,
        },
        KernelType::Array(_) => ColumnTypeName::Array,
        KernelType::Struct(_) => ColumnTypeName::Struct,
        KernelType::Map(_) => ColumnTypeName::Map,
        KernelType::Variant(_) => {
            return Err(ConnectorError::SchemaMismatch(
                "Unity Catalog column registration does not support the variant type".into(),
            ))
        }
    })
}

/// Spark `catalogString` of a Delta type, as Unity Catalog stores in `type_text`.
fn catalog_string(data_type: &KernelType, depth: usize) -> Result<String, ConnectorError> {
    if depth > MAX_TYPE_DEPTH {
        return Err(ConnectorError::SchemaMismatch(format!(
            "column type nesting exceeds {MAX_TYPE_DEPTH} levels"
        )));
    }
    let next = depth + 1;
    Ok(match data_type {
        KernelType::Primitive(primitive) => match primitive {
            PrimitiveType::String => "string".into(),
            PrimitiveType::Long => "bigint".into(),
            PrimitiveType::Integer => "int".into(),
            PrimitiveType::Short => "smallint".into(),
            PrimitiveType::Byte => "tinyint".into(),
            PrimitiveType::Float => "float".into(),
            PrimitiveType::Double => "double".into(),
            PrimitiveType::Boolean => "boolean".into(),
            PrimitiveType::Binary => "binary".into(),
            PrimitiveType::Date => "date".into(),
            PrimitiveType::Timestamp => "timestamp".into(),
            PrimitiveType::TimestampNtz => "timestamp_ntz".into(),
            PrimitiveType::Decimal(decimal) => {
                format!("decimal({},{})", decimal.precision(), decimal.scale())
            }
        },
        KernelType::Array(array) => {
            format!("array<{}>", catalog_string(array.element_type(), next)?)
        }
        KernelType::Map(map) => format!(
            "map<{},{}>",
            catalog_string(map.key_type(), next)?,
            catalog_string(map.value_type(), next)?
        ),
        KernelType::Struct(fields) => {
            let fields = fields
                .fields()
                .map(|field| {
                    Ok(format!(
                        "{}:{}",
                        field.name(),
                        catalog_string(field.data_type(), next)?
                    ))
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;
            format!("struct<{}>", fields.join(","))
        }
        KernelType::Variant(_) => {
            return Err(ConnectorError::SchemaMismatch(
                "Unity Catalog column registration does not support the variant type".into(),
            ))
        }
    })
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
    columns: &[ColumnInfo],
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

    let client = http_client()?;
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

#[cfg(test)]
mod tests;
