use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::validate_property_map_bounds;

pub(crate) const TARGET_FILE_SIZE_PROPERTY: &str = "write.target-file-size-bytes";
pub(crate) const PARQUET_ROW_GROUP_SIZE_PROPERTY: &str = "write.parquet.row-group-size-bytes";
pub(crate) const PARQUET_COMPRESSION_PROPERTY: &str = "write.parquet.compression-codec";
const MAX_TABLE_DEFINITION_FIELDS: usize = 128;
const MAX_TABLE_DEFINITION_BYTES: usize = 1024 * 1024;

/// Iceberg partition or sort transform used for table creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IcebergTransform {
    /// Preserve the source value.
    Identity,
    /// Hash into a fixed number of buckets.
    Bucket(u32),
    /// Truncate values to a fixed width.
    Truncate(u32),
    /// Extract the calendar year.
    Year,
    /// Extract the calendar month.
    Month,
    /// Extract the calendar day.
    Day,
    /// Extract the calendar hour.
    Hour,
    /// Always produce null.
    Void,
}

impl FromStr for IcebergTransform {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim().to_ascii_lowercase();
        match value.as_str() {
            "identity" => Ok(Self::Identity),
            "year" => Ok(Self::Year),
            "month" => Ok(Self::Month),
            "day" => Ok(Self::Day),
            "hour" => Ok(Self::Hour),
            "void" => Ok(Self::Void),
            _ => parameterized_transform(&value, "bucket", Self::Bucket)
                .or_else(|| parameterized_transform(&value, "truncate", Self::Truncate))
                .ok_or_else(|| format!("unsupported Iceberg transform '{value}'")),
        }
    }
}

impl<'de> Deserialize<'de> for IcebergTransform {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for IcebergTransform {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity => formatter.write_str("identity"),
            Self::Bucket(count) => write!(formatter, "bucket[{count}]"),
            Self::Truncate(width) => write!(formatter, "truncate[{width}]"),
            Self::Year => formatter.write_str("year"),
            Self::Month => formatter.write_str("month"),
            Self::Day => formatter.write_str("day"),
            Self::Hour => formatter.write_str("hour"),
            Self::Void => formatter.write_str("void"),
        }
    }
}

fn parameterized_transform(
    value: &str,
    name: &str,
    constructor: impl FnOnce(u32) -> IcebergTransform,
) -> Option<IcebergTransform> {
    let parameter = value
        .strip_prefix(name)?
        .strip_prefix('[')?
        .strip_suffix(']')?;
    let parameter = parameter.parse::<u32>().ok().filter(|value| *value > 0)?;
    Some(constructor(parameter))
}

/// One partition field in an auto-created Iceberg table.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergPartitionField {
    /// Source field name, including a dotted nested path when needed.
    pub source: String,
    /// Stable partition field name stored in Iceberg metadata.
    pub name: String,
    /// Transform applied to the source field.
    pub transform: IcebergTransform,
}

/// Sort direction used by an auto-created Iceberg table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IcebergSortDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

/// Null placement used by an auto-created Iceberg table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IcebergNullOrder {
    /// Nulls precede non-null values.
    NullsFirst,
    /// Nulls follow non-null values.
    NullsLast,
}

/// One sort field in an auto-created Iceberg table.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSortField {
    /// Source field name, including a dotted nested path when needed.
    pub source: String,
    /// Transform applied before ordering.
    pub transform: IcebergTransform,
    /// Sort direction.
    pub direction: IcebergSortDirection,
    /// Placement of null values.
    pub null_order: IcebergNullOrder,
}

pub(crate) fn parse_parquet_compression(value: &str) -> Result<String, ConnectorError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "zstd" => Ok("zstd".into()),
        "snappy" => Ok("snappy".into()),
        "lz4" => Ok("lz4".into()),
        "none" | "uncompressed" => Ok("uncompressed".into()),
        other => Err(ConnectorError::ConfigurationError(format!(
            "unsupported parquet.compression '{other}'; expected zstd, snappy, lz4, or uncompressed"
        ))),
    }
}

pub(crate) fn parse_table_fields<T>(
    config: &ConnectorConfig,
    key: &str,
) -> Result<Vec<T>, ConnectorError>
where
    T: serde::de::DeserializeOwned,
{
    let Some(value) = config.get(key) else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    if value.len() > MAX_TABLE_DEFINITION_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} is {} bytes; the limit is {MAX_TABLE_DEFINITION_BYTES}",
            value.len()
        )));
    }
    let fields = serde_json::from_str::<Vec<T>>(value).map_err(|error| {
        ConnectorError::ConfigurationError(format!("invalid {key} JSON: {error}"))
    })?;
    if fields.len() > MAX_TABLE_DEFINITION_FIELDS {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} contains {} fields; the limit is {MAX_TABLE_DEFINITION_FIELDS}",
            fields.len()
        )));
    }
    Ok(fields)
}

pub(crate) fn validate_distinct_names(values: &[String], key: &str) -> Result<(), ConnectorError> {
    if values.len() > MAX_TABLE_DEFINITION_FIELDS {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} contains {} fields; the limit is {MAX_TABLE_DEFINITION_FIELDS}",
            values.len()
        )));
    }
    let mut names = HashSet::with_capacity(values.len());
    for value in values {
        if value.is_empty() || value.trim() != value || !names.insert(value) {
            return Err(ConnectorError::ConfigurationError(format!(
                "{key} contains an empty, whitespace-padded, or duplicate field name"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_table_definition(
    partition_spec: &[IcebergPartitionField],
    sort_order: &[IcebergSortField],
) -> Result<(), ConnectorError> {
    let mut partition_names = HashSet::with_capacity(partition_spec.len());
    let mut partition_sources = HashSet::with_capacity(partition_spec.len());
    for field in partition_spec {
        if field.source.trim() != field.source
            || field.name.trim() != field.name
            || field.source.is_empty()
            || field.name.is_empty()
            || !partition_names.insert(field.name.as_str())
            || !partition_sources.insert((field.source.as_str(), field.transform))
        {
            return Err(ConnectorError::ConfigurationError(
                "partition.spec contains an empty, whitespace-padded, or duplicate field".into(),
            ));
        }
    }
    let mut sort_sources = HashSet::with_capacity(sort_order.len());
    for field in sort_order {
        if field.source.trim() != field.source
            || field.source.is_empty()
            || !sort_sources.insert((field.source.as_str(), field.transform))
        {
            return Err(ConnectorError::ConfigurationError(
                "sort.order contains an empty, whitespace-padded, or duplicate field".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_persisted_properties(
    properties: &HashMap<String, String>,
) -> Result<(), ConnectorError> {
    validate_property_map_bounds("table.property.*", properties)?;
    if let Some(key) = properties.keys().find(|key| {
        matches!(
            key.as_str(),
            TARGET_FILE_SIZE_PROPERTY
                | PARQUET_ROW_GROUP_SIZE_PROPERTY
                | PARQUET_COMPRESSION_PROPERTY
        )
    }) {
        return Err(ConnectorError::ConfigurationError(format!(
            "table.property.{key} duplicates a typed writer option"
        )));
    }
    if let Some(key) = properties.keys().find(|key| {
        let normalized = key.to_ascii_lowercase().replace(['.', '-'], "_");
        crate::security::is_secret_option_key(key) || normalized.contains("access_key")
    }) {
        return Err(ConnectorError::ConfigurationError(format!(
            "table.property.{key} cannot persist credential material in Iceberg metadata"
        )));
    }
    if let Some((key, _)) = properties
        .iter()
        .find(|(_, value)| crate::security::value_contains_uri_secret(value, false))
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "table.property.{key} contains credentials in a URI and cannot be persisted"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameterized_transforms_require_positive_values() {
        assert_eq!("bucket[32]".parse(), Ok(IcebergTransform::Bucket(32)));
        assert_eq!("truncate[8]".parse(), Ok(IcebergTransform::Truncate(8)));
        assert!("bucket[0]".parse::<IcebergTransform>().is_err());
        assert!("bucket(16)".parse::<IcebergTransform>().is_err());
    }

    #[test]
    fn metadata_identifiers_are_not_confused_with_credential_material() {
        let properties = HashMap::from([
            ("tenant_id".to_string(), "tenant-reference".to_string()),
            ("client_id".to_string(), "client-reference".to_string()),
            ("profile".to_string(), "analytics".to_string()),
        ]);
        validate_persisted_properties(&properties).unwrap();
    }

    #[test]
    fn access_keys_remain_forbidden_in_table_metadata() {
        for key in ["access-key-id", "aws_access_key_id", "secret_access_key"] {
            let properties = HashMap::from([(key.to_string(), "credential".to_string())]);
            assert!(validate_persisted_properties(&properties).is_err(), "{key}");
        }
    }
}
