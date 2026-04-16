//! Apache Iceberg connector configuration.
//!
//! [`IcebergSinkConfig`] and [`IcebergSourceConfig`] encapsulate settings for
//! writing to and reading from Iceberg tables, parsed from SQL `WITH (...)`
//! clauses via their respective `from_config` methods.
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
    /// Warehouse location (e.g., `s3://bucket/warehouse`).
    pub warehouse: String,
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
        let namespace = config.require("namespace")?.to_string();
        let table_name = config.require("table.name")?.to_string();

        let properties = config.properties_with_prefix("catalog.property.");

        Ok(Self {
            catalog_type,
            catalog_uri,
            warehouse,
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
    /// Writer ID for exactly-once deduplication (auto UUID if not set).
    pub writer_id: String,
}

impl IcebergSinkConfig {
    /// Parses a sink config from a [`ConnectorConfig`] (SQL WITH clause).
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

        let writer_id = config
            .get("writer.id")
            .filter(|v| !v.is_empty())
            .map_or_else(|| uuid::Uuid::now_v7().to_string(), ToString::to_string);

        Ok(Self {
            catalog,
            compression,
            auto_create,
            writer_id,
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
    /// Parses a source config from a [`ConnectorConfig`] (SQL WITH clause).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` on missing or invalid values.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let catalog = IcebergCatalogConfig::from_config(config)?;

        let poll_interval = config
            .get_parsed::<u64>("poll.interval.ms")?
            .map_or(Duration::from_mins(1), Duration::from_millis);

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
mod tests {
    use super::*;

    #[test]
    fn test_catalog_type_parse() {
        assert_eq!(
            "rest".parse::<IcebergCatalogType>().unwrap(),
            IcebergCatalogType::Rest
        );
        assert!("unknown".parse::<IcebergCatalogType>().is_err());
    }

    #[test]
    fn test_sink_config_from_config() {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://bucket/wh");
        config.set("namespace", "prod");
        config.set("table.name", "events");
        config.set("compression", "snappy");

        let cfg = IcebergSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.catalog.catalog_uri, "http://localhost:8181");
        assert_eq!(cfg.catalog.warehouse, "s3://bucket/wh");
        assert_eq!(cfg.catalog.namespace, "prod");
        assert_eq!(cfg.catalog.table_name, "events");
        assert_eq!(cfg.compression, "snappy");
        assert!(!cfg.writer_id.is_empty());
    }

    #[test]
    fn test_source_config_from_config() {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://bucket/wh");
        config.set("namespace", "prod");
        config.set("table.name", "dim_customers");
        config.set("poll.interval.ms", "30000");
        config.set("snapshot.id", "42");

        let cfg = IcebergSourceConfig::from_config(&config).unwrap();
        assert_eq!(cfg.poll_interval, Duration::from_secs(30));
        assert_eq!(cfg.snapshot_id, Some(42));
    }

    #[test]
    fn test_missing_required_field() {
        let config = ConnectorConfig::new("iceberg");
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_defaults() {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://localhost:8181");
        config.set("warehouse", "s3://bucket/wh");
        config.set("namespace", "prod");
        config.set("table.name", "events");

        let cfg = IcebergSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.compression, "zstd");
        assert!(!cfg.auto_create);
    }

    // ── Schema validation tests ──

    use arrow_schema::{DataType, Field, Schema};

    fn schema(fields: Vec<(&str, DataType)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_validate_matching_schemas() {
        let s = schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        assert!(validate_sink_schema(&s, &s).is_ok());
    }

    #[test]
    fn test_validate_missing_field() {
        let pipeline = schema(vec![("id", DataType::Int64), ("extra", DataType::Utf8)]);
        let table = schema(vec![("id", DataType::Int64)]);
        let err = validate_sink_schema(&pipeline, &table).unwrap_err();
        assert!(err.to_string().contains("extra"));
    }

    #[test]
    fn test_validate_type_mismatch() {
        let pipeline = schema(vec![("id", DataType::Int64)]);
        let table = schema(vec![("id", DataType::Utf8)]);
        let err = validate_sink_schema(&pipeline, &table).unwrap_err();
        assert!(err.to_string().contains("incompatible"));
    }

    #[test]
    fn test_validate_extra_table_columns_ok() {
        let pipeline = schema(vec![("id", DataType::Int64)]);
        let table = schema(vec![("id", DataType::Int64), ("extra", DataType::Utf8)]);
        assert!(validate_sink_schema(&pipeline, &table).is_ok());
    }

    #[test]
    fn test_validate_safe_widening() {
        let pipeline = schema(vec![("n", DataType::Int32), ("f", DataType::Float32)]);
        let table = schema(vec![("n", DataType::Int64), ("f", DataType::Float64)]);
        assert!(validate_sink_schema(&pipeline, &table).is_ok());
    }
}
