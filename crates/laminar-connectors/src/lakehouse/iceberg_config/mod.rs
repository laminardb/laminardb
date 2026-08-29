//! Typed Apache Iceberg connector configuration.
#![allow(clippy::disallowed_types)] // cold path: connector configuration

mod modes;
mod registry;
mod source;
mod table_definition;

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
pub use source::IcebergSourceConfig;
pub(crate) use table_definition::{
    parse_parquet_compression, parse_table_fields, validate_distinct_names,
    validate_persisted_properties, validate_table_definition,
};
pub use table_definition::{
    IcebergNullOrder, IcebergPartitionField, IcebergSortDirection, IcebergSortField,
    IcebergTransform,
};
#[cfg(feature = "iceberg-core")]
pub(crate) use table_definition::{
    PARQUET_COMPRESSION_PROPERTY, PARQUET_ROW_GROUP_SIZE_PROPERTY, TARGET_FILE_SIZE_PROPERTY,
};

const MIB: usize = 1024 * 1024;
const MAX_CONFIG_LIST_BYTES: usize = MIB;

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
        for (name, value) in [
            ("catalog.uri", catalog_uri.as_str()),
            ("catalog.warehouse", warehouse.as_str()),
        ] {
            if crate::security::value_contains_uri_secret(value, false) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "{name} must not embed credentials; use resolved catalog or storage secret options"
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

        let oauth2_server_uri = optional_non_empty(config, "catalog.oauth2.server_uri");
        if oauth2_server_uri
            .as_deref()
            .is_some_and(|value| crate::security::value_contains_uri_secret(value, false))
        {
            return Err(ConnectorError::ConfigurationError(
                "catalog.oauth2.server_uri must not embed credentials".into(),
            ));
        }

        Ok(Self {
            catalog_type,
            catalog_uri,
            warehouse,
            prefix: optional_non_empty(config, "catalog.prefix"),
            auth_type,
            oauth2_server_uri,
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

        let endpoint = optional_non_empty(config, "storage.endpoint");
        if endpoint
            .as_deref()
            .is_some_and(|value| crate::security::value_contains_uri_secret(value, false))
        {
            return Err(ConnectorError::ConfigurationError(
                "storage.endpoint must not embed credentials".into(),
            ));
        }

        Ok(Self {
            storage_type,
            endpoint,
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
#[derive(Clone)]
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
    pub partition_spec: Vec<IcebergPartitionField>,
    /// Declarative sort order for auto-create.
    pub sort_order: Vec<IcebergSortField>,
    /// Initial table properties for auto-create.
    pub initial_table_properties: HashMap<String, String>,
}

impl fmt::Debug for IcebergSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IcebergSinkConfig")
            .field("catalog", &self.catalog)
            .field("storage", &self.storage)
            .field("write_mode", &self.write_mode)
            .field("compression", &self.compression)
            .field("target_file_size_bytes", &self.target_file_size_bytes)
            .field(
                "parquet_row_group_size_bytes",
                &self.parquet_row_group_size_bytes,
            )
            .field("max_buffer_rows", &self.max_buffer_rows)
            .field("max_buffer_bytes", &self.max_buffer_bytes)
            .field("max_open_partitions", &self.max_open_partitions)
            .field("max_files_per_checkpoint", &self.max_files_per_checkpoint)
            .field("max_descriptor_bytes", &self.max_descriptor_bytes)
            .field("max_flush_age", &self.max_flush_age)
            .field("distribution_mode", &self.distribution_mode)
            .field("identifier_fields", &self.identifier_fields)
            .field("schema_evolution_mode", &self.schema_evolution_mode)
            .field("delivery_guarantee", &self.delivery_guarantee)
            .field("table_ref", &self.table_ref)
            .field("auto_create", &self.auto_create)
            .field("format_version", &self.format_version)
            .field("partition_spec", &self.partition_spec)
            .field("sort_order", &self.sort_order)
            .field(
                "initial_table_property_count",
                &self.initial_table_properties.len(),
            )
            .finish()
    }
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
        let compression = parse_parquet_compression(
            optional_alias(config, "parquet.compression", "compression").unwrap_or("zstd"),
        )?;
        let target_file_size_bytes = parse_nonzero_alias(
            config,
            "target.file.size.bytes",
            "target.file.size",
            128 * MIB,
        )?;
        let parquet_row_group_size_bytes =
            parse_nonzero(config, "parquet.row.group.size.bytes", 128 * MIB)?;
        let format_version = config.get_parsed("format.version")?.unwrap_or(2);
        if !matches!(format_version, 1..=3) {
            return Err(ConnectorError::ConfigurationError(
                "format.version must be 1, 2, or 3".into(),
            ));
        }
        let auto_create = parse_bool(config, "auto.create", false)?;
        let partition_spec = parse_table_fields(config, "partition.spec")?;
        let sort_order = parse_table_fields(config, "sort.order")?;
        validate_table_definition(&partition_spec, &sort_order)?;
        let identifier_fields =
            parse_comma_list(config.get("identifier.fields"), "identifier.fields", 128)?;
        validate_distinct_names(&identifier_fields, "identifier.fields")?;
        let initial_table_properties = config.properties_with_prefix("table.property.");
        validate_persisted_properties(&initial_table_properties)?;
        if !auto_create
            && (config.get("format.version").is_some()
                || !partition_spec.is_empty()
                || !sort_order.is_empty()
                || !initial_table_properties.is_empty())
        {
            return Err(ConnectorError::ConfigurationError(
                "format.version, partition.spec, sort.order, and table.property.* require auto.create=true"
                    .into(),
            ));
        }
        if auto_create && format_version == 1 && !identifier_fields.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "identifier.fields require format.version=2 or 3 for an auto-created table".into(),
            ));
        }

        Ok(Self {
            catalog,
            storage,
            write_mode: parse_or_default(config, "write.mode")?,
            compression,
            target_file_size_bytes,
            parquet_row_group_size_bytes,
            max_buffer_rows: parse_nonzero(config, "max.buffer.rows", 65_536)?,
            max_buffer_bytes: parse_nonzero(config, "max.buffer.bytes", 64 * MIB)?,
            max_open_partitions: parse_nonzero(config, "max.open.partitions", 64)?,
            max_files_per_checkpoint: parse_nonzero(config, "max.files.per.checkpoint", 4_096)?,
            max_descriptor_bytes: parse_nonzero(config, "max.descriptor.bytes", 16 * MIB)?,
            max_flush_age: parse_duration(config, "max.flush.age", Duration::from_secs(300))?,
            distribution_mode: parse_or_default(config, "write.distribution.mode")?,
            identifier_fields,
            schema_evolution_mode: parse_or_default(config, "schema.evolution.mode")?,
            delivery_guarantee,
            table_ref: config.get("table.ref").unwrap_or("main").trim().to_string(),
            auto_create,
            format_version,
            partition_spec,
            sort_order,
            initial_table_properties,
        })
    }

    #[cfg(feature = "iceberg-core")]
    pub(crate) fn validate_table_creation(&self) -> Result<(), ConnectorError> {
        if !matches!(self.format_version, 1..=3) {
            return Err(ConnectorError::ConfigurationError(
                "format.version must be 1, 2, or 3".into(),
            ));
        }
        parse_parquet_compression(&self.compression)?;
        validate_distinct_names(&self.identifier_fields, "identifier.fields")?;
        validate_table_definition(&self.partition_spec, &self.sort_order)?;
        validate_persisted_properties(&self.initial_table_properties)?;
        if self.format_version == 1 && !self.identifier_fields.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "identifier.fields require format.version=2 or 3 for an auto-created table".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_writer_limits(&self) -> Result<(), ConnectorError> {
        for (name, value) in [
            ("target.file.size.bytes", self.target_file_size_bytes),
            (
                "parquet.row.group.size.bytes",
                self.parquet_row_group_size_bytes,
            ),
            ("max.buffer.rows", self.max_buffer_rows),
            ("max.buffer.bytes", self.max_buffer_bytes),
            ("max.open.partitions", self.max_open_partitions),
            ("max.files.per.checkpoint", self.max_files_per_checkpoint),
            ("max.descriptor.bytes", self.max_descriptor_bytes),
        ] {
            if value == 0 {
                return Err(ConnectorError::ConfigurationError(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        parse_parquet_compression(&self.compression)?;
        Ok(())
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

fn parse_comma_list(
    value: Option<&str>,
    key: &str,
    max_entries: usize,
) -> Result<Vec<String>, ConnectorError> {
    let value = value.unwrap_or_default();
    if value.len() > MAX_CONFIG_LIST_BYTES {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} is {} bytes; the limit is {MAX_CONFIG_LIST_BYTES}",
            value.len()
        )));
    }
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .take(max_entries.saturating_add(1))
        .collect::<Vec<_>>();
    if values.len() > max_entries {
        return Err(ConnectorError::ConfigurationError(format!(
            "{key} contains more than {max_entries} entries"
        )));
    }
    Ok(values)
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
