//! Apache Iceberg connector configuration.
//!
//! [`IcebergSinkConfig`] and [`IcebergSourceConfig`] encapsulate settings for
//! writing to and reading from Iceberg tables, parsed from resolved connector
//! configs via their respective `from_config` methods.
#![allow(clippy::disallowed_types)] // cold path: lakehouse configuration

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

/// Iceberg catalog type.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum IcebergCatalogType {
    /// REST catalog (Polaris, Nessie, Unity, Glue adapter).
    #[default]
    Rest,
}

impl FromStr for IcebergCatalogType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "rest" => Ok(Self::Rest),
            other => Err(format!("unsupported iceberg catalog type: '{other}'")),
        }
    }
}

impl fmt::Display for IcebergCatalogType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rest => write!(f, "rest"),
        }
    }
}

/// Shared catalog connection settings for both source and sink.
#[derive(Debug, Clone)]
pub struct IcebergCatalogConfig {
    /// Catalog type (currently only REST).
    pub catalog_type: IcebergCatalogType,
    /// REST catalog URI (e.g., `http://polaris:8181`).
    pub catalog_uri: String,
    /// Warehouse URL (Hadoop-style: `s3://bucket/wh`) or name (REST catalogs).
    pub warehouse: String,
    /// Explicit storage backend. Required when `warehouse` is a name.
    pub storage_type: Option<String>,
    /// Iceberg namespace (e.g., `prod` or `prod.analytics`).
    pub namespace: String,
    /// Table name within the namespace.
    pub table_name: String,
    /// Additional catalog properties (credentials, endpoints, etc.).
    pub properties: HashMap<String, String>,
}

impl IcebergCatalogConfig {
    /// Parses shared catalog settings from a [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if required keys are absent.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let catalog_type = if let Some(v) = config.get("catalog.type") {
            v.parse()
                .map_err(|e: String| ConnectorError::ConfigurationError(e))?
        } else {
            IcebergCatalogType::default()
        };

        let catalog_uri = config.require("catalog.uri")?.to_string();
        let warehouse = config.require("warehouse")?.to_string();
        let storage_type = config
            .get("storage.type")
            .map(str::trim)
            .filter(|storage| !storage.is_empty())
            .map(str::to_ascii_lowercase);
        if let Some(storage) = storage_type.as_deref() {
            if !matches!(storage, "s3" | "s3a" | "fs") {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unsupported Iceberg storage.type '{storage}'; expected s3 | s3a | fs"
                )));
            }
        }
        if let Some((scheme, _)) = warehouse.split_once("://") {
            let scheme = scheme.to_ascii_lowercase();
            if !matches!(scheme.as_str(), "s3" | "s3a" | "file") {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unsupported Iceberg warehouse scheme '{scheme}'; expected s3 | s3a | file"
                )));
            }
        }
        let namespace = config.require("namespace")?.to_string();
        let table_name = config.require("table.name")?.to_string();

        let properties = config.properties_with_prefix("catalog.property.");

        Ok(Self {
            catalog_type,
            catalog_uri,
            warehouse,
            storage_type,
            namespace,
            table_name,
            properties,
        })
    }
}

// ── Sink Configuration ──

/// Configuration for the Iceberg sink connector.
#[derive(Debug, Clone)]
pub struct IcebergSinkConfig {
    /// Shared catalog connection settings.
    pub catalog: IcebergCatalogConfig,
    /// Parquet compression codec (default: zstd).
    pub compression: String,
    /// Auto-create table if it doesn't exist.
    pub auto_create: bool,
}

impl IcebergSinkConfig {
    /// Parses a sink config from a resolved [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on missing or invalid values.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let catalog = IcebergCatalogConfig::from_config(config)?;

        let compression = config.get("compression").unwrap_or("zstd").to_string();

        let auto_create = config
            .get("auto.create")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        Ok(Self {
            catalog,
            compression,
            auto_create,
        })
    }
}

// ── Source Configuration ──

/// Configuration for the Iceberg source connector (lookup/reference table).
#[derive(Debug, Clone)]
pub struct IcebergSourceConfig {
    /// Shared catalog connection settings.
    pub catalog: IcebergCatalogConfig,
    /// How often to poll for new snapshots (default: 60s).
    pub poll_interval: Duration,
    /// Pin to a specific snapshot ID (no polling if set).
    pub snapshot_id: Option<i64>,
    /// Columns to select (empty = all columns).
    pub select_columns: Vec<String>,
}

impl IcebergSourceConfig {
    /// Parses a source config from a resolved [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on missing or invalid values.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let catalog = IcebergCatalogConfig::from_config(config)?;

        let poll_interval = config
            .get_parsed::<u64>("poll.interval.ms")?
            .map_or(Duration::from_secs(60), Duration::from_millis);

        let snapshot_id = config.get_parsed::<i64>("snapshot.id")?;

        let select_columns = config
            .get("select.columns")
            .unwrap_or("")
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        Ok(Self {
            catalog,
            poll_interval,
            snapshot_id,
            select_columns,
        })
    }
}

/// Checks if `from` can be safely widened to `to` without data loss.
fn is_safe_widening(from: &arrow_schema::DataType, to: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType;
    matches!(
        (from, to),
        (
            DataType::Int8,
            DataType::Int16 | DataType::Int32 | DataType::Int64
        ) | (DataType::Int16, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64)
            | (DataType::Float32, DataType::Float64)
            | (DataType::Utf8, DataType::LargeUtf8)
    )
}

/// Validates that a pipeline's output schema is compatible with an Iceberg
/// table's Arrow schema.
///
/// Every field in `pipeline` must exist in `table` with a matching or
/// safely-widenable type. Extra columns in `table` are acceptable (Iceberg
/// fills them with nulls).
///
/// # Errors
///
/// Returns `ConnectorError::SchemaMismatch` on incompatible fields.
pub fn validate_sink_schema(
    pipeline: &arrow_schema::Schema,
    table: &arrow_schema::Schema,
) -> Result<(), ConnectorError> {
    for field in pipeline.fields() {
        match table.field_with_name(field.name()) {
            Ok(table_field) => {
                if field.data_type() != table_field.data_type()
                    && !is_safe_widening(field.data_type(), table_field.data_type())
                {
                    return Err(ConnectorError::SchemaMismatch(format!(
                        "field '{}': pipeline type {} incompatible with table type {}",
                        field.name(),
                        field.data_type(),
                        table_field.data_type(),
                    )));
                }
            }
            Err(_) => {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "pipeline field '{}' ({}) not found in Iceberg table schema",
                    field.name(),
                    field.data_type(),
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
