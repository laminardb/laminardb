//! Typed Apache Iceberg connector configuration.
#![allow(clippy::disallowed_types)] // cold path: connector configuration

mod modes;
mod registry;

use std::collections::HashMap;
use std::fmt;
#[cfg(feature = "iceberg-core")]
use std::fmt::Write;
use std::time::Duration;

#[cfg(feature = "iceberg-core")]
use sha2::{Digest, Sha256};

use crate::config::ConnectorConfig;
use crate::connector::DeliveryGuarantee;
use crate::error::ConnectorError;

pub use modes::{
    IcebergCatalogAuthType, IcebergCatalogType, IcebergReadBootstrap, IcebergReadMode,
    IcebergSchemaEvolutionMode, IcebergStorageEncryption, IcebergStorageType,
    IcebergWriteDistributionMode, IcebergWriteMode,
};
pub(crate) use registry::{sink_config_keys, source_config_keys};

const MIB: usize = 1024 * 1024;

#[cfg(feature = "iceberg-core")]
pub(crate) fn stable_catalog_identity(
    catalog: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
) -> String {
    let mut hasher = Sha256::new();
    for value in [
        catalog.catalog_type.to_string(),
        catalog.catalog_uri.clone(),
        catalog.warehouse.clone(),
        catalog.prefix.clone().unwrap_or_default(),
        catalog.auth_type.to_string(),
        storage
            .storage_type
            .map(|storage| storage.to_string())
            .unwrap_or_default(),
        storage.endpoint.clone().unwrap_or_default(),
        storage.region.clone().unwrap_or_default(),
    ] {
        hasher.update(value.len().to_le_bytes());
        hasher.update(value.as_bytes());
    }
    let mut encoded = String::with_capacity(64);
    for byte in hasher.finalize() {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Catalog connection settings shared by Iceberg sources and sinks.
#[derive(Clone)]
pub struct IcebergCatalogConfig {
    /// Catalog implementation.
    pub catalog_type: IcebergCatalogType,
    /// Catalog endpoint.
    pub catalog_uri: String,
    /// Warehouse name or location.
    pub warehouse: String,
    /// Optional catalog namespace prefix.
    pub prefix: Option<String>,
    /// Authentication mechanism.
    pub auth_type: IcebergCatalogAuthType,
    /// `OAuth2` token endpoint.
    pub oauth2_server_uri: Option<String>,
    /// `OAuth2` client identifier.
    pub oauth2_client_id: Option<String>,
    /// `OAuth2` requested scope.
    pub oauth2_scope: Option<String>,
    /// Whether REST credential vending may be used.
    pub access_delegation: bool,
    /// Bound for an individual catalog request.
    pub request_timeout: Duration,
    /// End-to-end catalog commit bound.
    pub commit_timeout: Duration,
    /// Iceberg namespace.
    pub namespace: String,
    /// Table name within the namespace.
    pub table_name: String,
    /// Additional catalog properties. Values may contain secrets.
    pub properties: HashMap<String, String>,
}

impl fmt::Debug for IcebergCatalogConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut property_keys: Vec<&str> = self.properties.keys().map(String::as_str).collect();
        property_keys.sort_unstable();
        formatter
            .debug_struct("IcebergCatalogConfig")
            .field("catalog_type", &self.catalog_type)
            .field("catalog_uri", &"<configured>")
            .field("warehouse", &"<configured>")
            .field("prefix", &self.prefix)
            .field("auth_type", &self.auth_type)
            .field(
                "oauth2_server_uri",
                &self.oauth2_server_uri.as_ref().map(|_| "<configured>"),
            )
            .field(
                "oauth2_client_id",
                &self.oauth2_client_id.as_ref().map(|_| "<configured>"),
            )
            .field("oauth2_scope", &self.oauth2_scope)
            .field("access_delegation", &self.access_delegation)
            .field("request_timeout", &self.request_timeout)
            .field("commit_timeout", &self.commit_timeout)
            .field("namespace", &self.namespace)
            .field("table_name", &self.table_name)
            .field("property_keys", &property_keys)
            .finish()
    }
}

impl IcebergCatalogConfig {
    /// Parses catalog settings from resolved connector configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for missing, malformed, or contradictory settings.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let catalog_type = parse_or_default(config, "catalog.type")?;
        let catalog_uri = config.require("catalog.uri")?.trim().to_string();
        let warehouse = required_alias(config, "catalog.warehouse", "warehouse")?.to_string();
        let namespace = config.require("namespace")?.trim().to_string();
        let table_name = config.require("table.name")?.trim().to_string();
        for (name, value) in [
            ("catalog.uri", catalog_uri.as_str()),
            ("catalog.warehouse", warehouse.as_str()),
            ("namespace", namespace.as_str()),
            ("table.name", table_name.as_str()),
        ] {
            if value.is_empty() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "{name} must not be empty"
                )));
            }
        }
        let properties = config.properties_with_prefix("catalog.property.");
        let auth_type = match config.get("catalog.auth.type") {
            Some(value) => value.parse().map_err(ConnectorError::ConfigurationError)?,
            None if properties.contains_key("token") => IcebergCatalogAuthType::Bearer,
            None if properties.contains_key("credential") => IcebergCatalogAuthType::OAuth2,
            None => IcebergCatalogAuthType::None,
        };
        match auth_type {
            IcebergCatalogAuthType::Bearer if !properties.contains_key("token") => {
                return Err(ConnectorError::ConfigurationError(
                    "catalog.auth.type=bearer requires catalog.property.token".into(),
                ));
            }
            IcebergCatalogAuthType::Bearer if properties.contains_key("credential") => {
                return Err(ConnectorError::ConfigurationError(
                    "catalog.auth.type=bearer cannot also configure catalog.property.credential"
                        .into(),
                ));
            }
            IcebergCatalogAuthType::OAuth2 if !properties.contains_key("credential") => {
                return Err(ConnectorError::ConfigurationError(
                    "catalog.auth.type=oauth2 requires a resolved catalog.property.credential"
                        .into(),
                ));
            }
            IcebergCatalogAuthType::OAuth2 if properties.contains_key("token") => {
                return Err(ConnectorError::ConfigurationError(
                    "catalog.auth.type=oauth2 cannot also configure catalog.property.token".into(),
                ));
            }
            IcebergCatalogAuthType::None
            | IcebergCatalogAuthType::Bearer
            | IcebergCatalogAuthType::OAuth2 => {}
        }

        Ok(Self {
            catalog_type,
            catalog_uri,
            warehouse,
            prefix: optional_non_empty(config, "catalog.prefix"),
            auth_type,
            oauth2_server_uri: optional_non_empty(config, "catalog.oauth2.server_uri"),
            oauth2_client_id: optional_non_empty(config, "catalog.oauth2.client_id"),
            oauth2_scope: optional_non_empty(config, "catalog.oauth2.scope"),
            access_delegation: parse_bool(config, "catalog.access_delegation", false)?,
            request_timeout: parse_duration(
                config,
                "catalog.request_timeout",
                Duration::from_secs(30),
            )?,
            commit_timeout: parse_duration(
                config,
                "catalog.commit_timeout",
                Duration::from_secs(120),
            )?,
            namespace,
            table_name,
            properties,
        })
    }
}

/// Storage settings kept separate from catalog authentication.
#[derive(Clone)]
pub struct IcebergStorageConfig {
    /// Explicit storage implementation, or one inferred from the warehouse URI.
    pub storage_type: Option<IcebergStorageType>,
    /// Storage service endpoint override.
    pub endpoint: Option<String>,
    /// Cloud region.
    pub region: Option<String>,
    /// Whether S3 path-style addressing is required.
    pub path_style: bool,
    /// Bound for storage requests.
    pub request_timeout: Duration,
    /// Bound for storage connection establishment.
    pub connect_timeout: Duration,
    /// Server-side encryption mode.
    pub encryption: IcebergStorageEncryption,
    /// KMS key identifier, resolved without logging its value.
    pub kms_key: Option<String>,
    /// Additional storage properties. Values may contain secrets.
    pub properties: HashMap<String, String>,
}

impl fmt::Debug for IcebergStorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut property_keys: Vec<&str> = self.properties.keys().map(String::as_str).collect();
        property_keys.sort_unstable();
        formatter
            .debug_struct("IcebergStorageConfig")
            .field("storage_type", &self.storage_type)
            .field("endpoint", &self.endpoint.as_ref().map(|_| "<configured>"))
            .field("region", &self.region)
            .field("path_style", &self.path_style)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("encryption", &self.encryption)
            .field("kms_key", &self.kms_key.as_ref().map(|_| "<configured>"))
            .field("property_keys", &property_keys)
            .finish()
    }
}

impl IcebergStorageConfig {
    /// Parses storage settings from resolved connector configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or contradictory settings.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let storage_type = config
            .get("storage.type")
            .map(str::parse)
            .transpose()
            .map_err(ConnectorError::ConfigurationError)?;
        let encryption = parse_or_default(config, "storage.encryption")?;
        let kms_key = optional_non_empty(config, "storage.kms_key");
        if encryption == IcebergStorageEncryption::Kms && kms_key.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "storage.kms_key is required when storage.encryption=kms".into(),
            ));
        }
        if encryption != IcebergStorageEncryption::Kms && kms_key.is_some() {
            return Err(ConnectorError::ConfigurationError(
                "storage.kms_key requires storage.encryption=kms".into(),
            ));
        }
        let path_style = parse_optional_bool(
            "storage.path_style",
            config
                .get("storage.path_style")
                .or_else(|| config.get("storage.property.s3.path-style-access"))
                .or_else(|| config.get("catalog.property.s3.path-style-access")),
            false,
        )?;

        Ok(Self {
            storage_type,
            endpoint: optional_non_empty(config, "storage.endpoint"),
            region: optional_non_empty(config, "storage.region"),
            path_style,
            request_timeout: parse_duration(
                config,
                "storage.request_timeout",
                Duration::from_secs(30),
            )?,
            connect_timeout: parse_duration(
                config,
                "storage.connect_timeout",
                Duration::from_secs(10),
            )?,
            encryption,
            kms_key,
            properties: config.properties_with_prefix("storage.property."),
        })
    }
}

/// Configuration for the Iceberg append sink.
#[derive(Debug, Clone)]
pub struct IcebergSinkConfig {
    /// Catalog settings.
    pub catalog: IcebergCatalogConfig,
    /// Table storage settings.
    pub storage: IcebergStorageConfig,
    /// Requested mutation mode.
    pub write_mode: IcebergWriteMode,
    /// Parquet compression codec.
    pub compression: String,
    /// Target size used by Iceberg rolling writers.
    pub target_file_size_bytes: usize,
    /// Parquet row-group size.
    pub parquet_row_group_size_bytes: usize,
    /// Maximum rows accepted in one in-flight batch.
    pub max_buffer_rows: usize,
    /// Maximum Arrow bytes accepted in one in-flight batch.
    pub max_buffer_bytes: usize,
    /// Maximum simultaneously open partition writers.
    pub max_open_partitions: usize,
    /// Maximum data files produced by one checkpoint participant.
    pub max_files_per_checkpoint: usize,
    /// Maximum encoded participant descriptor size.
    pub max_descriptor_bytes: usize,
    /// Maximum age of an open data file.
    pub max_flush_age: Duration,
    /// Partition-writer distribution policy.
    pub distribution_mode: IcebergWriteDistributionMode,
    /// Iceberg identifier field names.
    pub identifier_fields: Vec<String>,
    /// Permitted table schema evolution.
    pub schema_evolution_mode: IcebergSchemaEvolutionMode,
    /// Pipeline delivery guarantee.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Table ref to publish.
    pub table_ref: String,
    /// Auto-create the table if absent.
    pub auto_create: bool,
    /// Iceberg format version for auto-created tables.
    pub format_version: u8,
    /// Declarative partition spec for auto-create.
    pub partition_spec: Option<String>,
    /// Declarative sort order for auto-create.
    pub sort_order: Option<String>,
    /// Initial table properties for auto-create.
    pub initial_table_properties: HashMap<String, String>,
}

impl IcebergSinkConfig {
    /// Parses sink settings from resolved connector configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for missing, malformed, or contradictory settings.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let catalog = IcebergCatalogConfig::from_config(config)?;
        let storage = IcebergStorageConfig::from_config(config)?;
        let delivery_guarantee = config
            .get("delivery.guarantee")
            .unwrap_or("at-least-once")
            .parse()
            .map_err(ConnectorError::ConfigurationError)?;
        let format_version = config.get_parsed("format.version")?.unwrap_or(2);
        if !matches!(format_version, 1..=3) {
            return Err(ConnectorError::ConfigurationError(
                "format.version must be 1, 2, or 3".into(),
            ));
        }

        Ok(Self {
            catalog,
            storage,
            write_mode: parse_or_default(config, "write.mode")?,
            compression: optional_alias(config, "parquet.compression", "compression")
                .unwrap_or("zstd")
                .to_string(),
            target_file_size_bytes: parse_nonzero_alias(
                config,
                "target.file.size.bytes",
                "target.file.size",
                128 * MIB,
            )?,
            parquet_row_group_size_bytes: parse_nonzero(
                config,
                "parquet.row.group.size.bytes",
                128 * MIB,
            )?,
            max_buffer_rows: parse_nonzero(config, "max.buffer.rows", 65_536)?,
            max_buffer_bytes: parse_nonzero(config, "max.buffer.bytes", 64 * MIB)?,
            max_open_partitions: parse_nonzero(config, "max.open.partitions", 64)?,
            max_files_per_checkpoint: parse_nonzero(config, "max.files.per.checkpoint", 4_096)?,
            max_descriptor_bytes: parse_nonzero(config, "max.descriptor.bytes", 16 * MIB)?,
            max_flush_age: parse_duration(config, "max.flush.age", Duration::from_secs(300))?,
            distribution_mode: parse_or_default(config, "write.distribution.mode")?,
            identifier_fields: parse_list(config.get("identifier.fields")),
            schema_evolution_mode: parse_or_default(config, "schema.evolution.mode")?,
            delivery_guarantee,
            table_ref: config.get("table.ref").unwrap_or("main").trim().to_string(),
            auto_create: parse_bool(config, "auto.create", false)?,
            format_version,
            partition_spec: optional_non_empty(config, "partition.spec"),
            sort_order: optional_non_empty(config, "sort.order"),
            initial_table_properties: config.properties_with_prefix("table.property."),
        })
    }
}

/// Configuration for bounded snapshot and append reads.
#[derive(Debug, Clone)]
pub struct IcebergSourceConfig {
    /// Catalog settings.
    pub catalog: IcebergCatalogConfig,
    /// Table storage settings.
    pub storage: IcebergStorageConfig,
    /// Requested read semantics.
    pub read_mode: IcebergReadMode,
    /// Append bootstrap policy.
    pub bootstrap: IcebergReadBootstrap,
    /// How often append mode refreshes table metadata.
    pub poll_interval: Duration,
    /// Selected start or bounded snapshot.
    pub snapshot_id: Option<i64>,
    /// Named Iceberg table ref.
    pub table_ref: String,
    /// Projected columns; empty selects all columns.
    pub select_columns: Vec<String>,
    /// Serialized Iceberg predicate pushed into scan planning.
    pub filter: Option<String>,
    /// Maximum lineage snapshots processed by one poll.
    pub max_snapshots_per_poll: usize,
    /// Maximum added files retained while planning one poll.
    pub max_planned_files: usize,
    /// Capacity of the scan-to-ingestion batch channel.
    pub scan_channel_capacity: usize,
    /// Maximum concurrent manifest and data-file reads.
    pub scan_concurrency: usize,
}

impl IcebergSourceConfig {
    /// Parses source settings from resolved connector configuration.
    ///
    /// `snapshot` mode without an explicit ID selects the current snapshot at
    /// open and remains pinned to it. `append` mode uses that same selection as
    /// its lineage root.
    ///
    /// # Errors
    ///
    /// Returns an error for missing, malformed, or contradictory settings.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let read_mode = parse_or_default(config, "read.mode")?;
        if config.get("read.bootstrap").is_some() && read_mode != IcebergReadMode::Append {
            return Err(ConnectorError::ConfigurationError(
                "read.bootstrap is only valid when read.mode=append".into(),
            ));
        }
        let poll_interval = match optional_alias(config, "poll.interval", "poll.interval.ms") {
            Some(value) if config.get("poll.interval").is_some() => {
                parse_duration_value("poll.interval", value)?
            }
            Some(value) => Duration::from_millis(
                u64::try_from(parse_nonzero_value("poll.interval.ms", value)?).map_err(|_| {
                    ConnectorError::ConfigurationError("poll.interval.ms is too large".into())
                })?,
            ),
            None => Duration::from_secs(60),
        };

        Ok(Self {
            catalog: IcebergCatalogConfig::from_config(config)?,
            storage: IcebergStorageConfig::from_config(config)?,
            read_mode,
            bootstrap: parse_or_default(config, "read.bootstrap")?,
            poll_interval,
            snapshot_id: parse_optional_alias(config, "start.snapshot.id", "snapshot.id")?,
            table_ref: config.get("table.ref").unwrap_or("main").trim().to_string(),
            select_columns: parse_list(optional_alias(config, "projection", "select.columns")),
            filter: optional_non_empty(config, "filter"),
            max_snapshots_per_poll: parse_nonzero(config, "read.max.snapshots.per.poll", 1_024)?,
            max_planned_files: parse_nonzero(config, "read.max.planned.files", 65_536)?,
            scan_channel_capacity: parse_nonzero(config, "read.channel.capacity", 2)?,
            scan_concurrency: parse_nonzero(config, "read.scan.concurrency", 4)?,
        })
    }
}

fn parse_or_default<T>(config: &ConnectorConfig, key: &str) -> Result<T, ConnectorError>
where
    T: Default + std::str::FromStr<Err = String>,
{
    config
        .get(key)
        .map(str::parse)
        .transpose()
        .map(Option::unwrap_or_default)
        .map_err(ConnectorError::ConfigurationError)
}

fn parse_bool(config: &ConnectorConfig, key: &str, default: bool) -> Result<bool, ConnectorError> {
    parse_optional_bool(key, config.get(key), default)
}

fn parse_optional_bool(
    key: &str,
    value: Option<&str>,
    default: bool,
) -> Result<bool, ConnectorError> {
    match value {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) => Err(ConnectorError::ConfigurationError(format!(
            "invalid {key}: '{value}'; expected true or false"
        ))),
    }
}

fn parse_nonzero(
    config: &ConnectorConfig,
    key: &str,
    default: usize,
) -> Result<usize, ConnectorError> {
    config
        .get(key)
        .map_or(Ok(default), |value| parse_nonzero_value(key, value))
}

fn parse_nonzero_alias(
    config: &ConnectorConfig,
    key: &str,
    alias: &str,
    default: usize,
) -> Result<usize, ConnectorError> {
    optional_alias(config, key, alias).map_or(Ok(default), |value| parse_nonzero_value(key, value))
}

fn parse_nonzero_value(key: &str, value: &str) -> Result<usize, ConnectorError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| ConnectorError::ConfigurationError(format!("invalid {key}: '{value}'")))?;
    if parsed == 0 {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn parse_duration(
    config: &ConnectorConfig,
    key: &str,
    default: Duration,
) -> Result<Duration, ConnectorError> {
    config
        .get(key)
        .map_or(Ok(default), |value| parse_duration_value(key, value))
}

fn parse_duration_value(key: &str, value: &str) -> Result<Duration, ConnectorError> {
    let value = value.trim();
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else {
        (value, 1)
    };
    let milliseconds = number.trim().parse::<u64>().map_err(|_| {
        ConnectorError::ConfigurationError(format!(
            "invalid {key}: '{value}'; expected an integer with ms, s, or m"
        ))
    })?;
    if milliseconds == 0 {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} must be greater than zero"
        )));
    }
    milliseconds
        .checked_mul(multiplier)
        .map(Duration::from_millis)
        .ok_or_else(|| ConnectorError::ConfigurationError(format!("{key} is too large")))
}

fn optional_non_empty(config: &ConnectorConfig, key: &str) -> Option<String> {
    config
        .get(key)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn optional_alias<'a>(config: &'a ConnectorConfig, key: &str, alias: &str) -> Option<&'a str> {
    config.get(key).or_else(|| config.get(alias))
}

fn required_alias<'a>(
    config: &'a ConnectorConfig,
    key: &str,
    alias: &str,
) -> Result<&'a str, ConnectorError> {
    optional_alias(config, key, alias).ok_or_else(|| ConnectorError::missing_config(key))
}

fn parse_optional_alias<T>(
    config: &ConnectorConfig,
    key: &str,
    alias: &str,
) -> Result<Option<T>, ConnectorError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    optional_alias(config, key, alias)
        .map(|value| {
            value.parse().map_err(|error| {
                ConnectorError::ConfigurationError(format!("invalid {key}: {error}"))
            })
        })
        .transpose()
}

fn parse_list(value: Option<&str>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

/// Validates an Arrow pipeline schema against an Iceberg-derived Arrow schema.
///
/// # Errors
///
/// Returns a schema mismatch for missing fields or unsafe type changes.
pub fn validate_sink_schema(
    pipeline: &arrow_schema::Schema,
    table: &arrow_schema::Schema,
) -> Result<(), ConnectorError> {
    for field in pipeline.fields() {
        let table_field = table.field_with_name(field.name()).map_err(|_| {
            ConnectorError::SchemaMismatch(format!(
                "pipeline field '{}' ({}) not found in Iceberg table schema",
                field.name(),
                field.data_type()
            ))
        })?;
        if field.data_type() != table_field.data_type()
            && !is_safe_iceberg_widening(field.data_type(), table_field.data_type())
        {
            return Err(ConnectorError::SchemaMismatch(format!(
                "field '{}': pipeline type {} incompatible with table type {}",
                field.name(),
                field.data_type(),
                table_field.data_type()
            )));
        }
    }
    Ok(())
}

pub(crate) fn is_safe_iceberg_widening(
    from: &arrow_schema::DataType,
    to: &arrow_schema::DataType,
) -> bool {
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

#[cfg(test)]
mod tests;
