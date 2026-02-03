//! Apache Iceberg sink connector configuration.
//!
//! [`IcebergSinkConfig`] encapsulates all settings for writing Arrow
//! `RecordBatch` data to Iceberg tables, parsed from SQL `WITH (...)`
//! clauses via [`from_config`](IcebergSinkConfig::from_config).

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::storage::{
    CloudConfigValidator, ResolvedStorageOptions, SecretMasker, StorageCredentialResolver,
    StorageProvider,
};

/// Configuration for the Apache Iceberg sink connector.
///
/// Parsed from SQL `WITH (...)` clause options or constructed programmatically.
#[derive(Debug, Clone)]
pub struct IcebergSinkConfig {
    /// Iceberg catalog type.
    pub catalog_type: IcebergCatalogType,

    /// Iceberg catalog URI (REST endpoint, Glue ARN, or Hive thrift URI).
    pub catalog_uri: String,

    /// Warehouse location (e.g., `s3://bucket/warehouse`).
    pub warehouse: String,

    /// Namespace for the table (e.g., `["analytics", "prod"]`).
    pub namespace: Vec<String>,

    /// Table name within the namespace.
    pub table_name: String,

    /// Partition specification using Iceberg transforms.
    /// Empty = unpartitioned table.
    pub partition_spec: Vec<IcebergPartitionField>,

    /// Target Parquet data file size in bytes (default: 128 MB).
    pub target_file_size: usize,

    /// Maximum number of records to buffer before flushing to Parquet.
    pub max_buffer_records: usize,

    /// Maximum time to buffer records before flushing.
    pub max_buffer_duration: Duration,

    /// Whether to enable schema evolution (auto-merge new columns).
    pub schema_evolution: bool,

    /// Write mode: Append or Upsert (equality delete + insert).
    pub write_mode: IcebergWriteMode,

    /// Key columns for upsert/merge operations (required for Upsert mode).
    /// These define the equality delete fields.
    pub equality_delete_columns: Vec<String>,

    /// Delivery guarantee: `AtLeastOnce` or `ExactlyOnce`.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Writer ID for exactly-once deduplication.
    pub writer_id: String,

    /// Catalog-specific properties (e.g., Glue region, auth tokens).
    pub catalog_properties: HashMap<String, String>,

    /// Storage-specific properties (S3 credentials, etc.).
    pub storage_options: HashMap<String, String>,

    /// Maintenance configuration (compaction, snapshot expiry).
    pub maintenance: MaintenanceConfig,

    /// Sort order columns for data files (optional).
    pub sort_order: Vec<SortField>,

    /// File format for data files.
    pub file_format: IcebergFileFormat,
}

impl Default for IcebergSinkConfig {
    fn default() -> Self {
        Self {
            catalog_type: IcebergCatalogType::Rest,
            catalog_uri: String::new(),
            warehouse: String::new(),
            namespace: Vec::new(),
            table_name: String::new(),
            partition_spec: Vec::new(),
            target_file_size: 128 * 1024 * 1024, // 128 MB
            max_buffer_records: 100_000,
            max_buffer_duration: Duration::from_secs(60),
            schema_evolution: false,
            write_mode: IcebergWriteMode::Append,
            equality_delete_columns: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::ExactlyOnce,
            writer_id: uuid::Uuid::new_v4().to_string(),
            catalog_properties: HashMap::new(),
            storage_options: HashMap::new(),
            maintenance: MaintenanceConfig::default(),
            sort_order: Vec::new(),
            file_format: IcebergFileFormat::Parquet,
        }
    }
}

impl IcebergSinkConfig {
    /// Creates a minimal config for testing.
    #[must_use]
    pub fn new(warehouse: &str, namespace: &str, table_name: &str) -> Self {
        Self {
            warehouse: warehouse.to_string(),
            namespace: namespace.split('.').map(|s| s.trim().to_string()).collect(),
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// Parses a sink config from a [`ConnectorConfig`] (SQL WITH clause).
    ///
    /// # Required keys
    ///
    /// - `warehouse` - Warehouse location (e.g., `s3://bucket/warehouse`)
    /// - `namespace` - Dot-separated namespace (e.g., `analytics.prod`)
    /// - `table.name` - Table name within the namespace
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if required keys are absent,
    /// or `ConnectorError::ConfigurationError` on invalid values.
    #[allow(clippy::too_many_lines)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            warehouse: config.require("warehouse")?.to_string(),
            table_name: config.require("table.name")?.to_string(),
            ..Self::default()
        };

        // Parse namespace (required).
        let namespace_str = config.require("namespace")?;
        cfg.namespace = namespace_str
            .split('.')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Parse catalog type.
        if let Some(v) = config.get("catalog.type") {
            cfg.catalog_type = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid catalog.type: '{v}' (expected 'rest', 'glue', 'hive', 'memory')"
                ))
            })?;
        }

        // Catalog URI defaults based on catalog type.
        cfg.catalog_uri = config.get("catalog.uri").unwrap_or_default().to_string();

        // Parse partition spec.
        if let Some(v) = config.get("partition.spec") {
            cfg.partition_spec = Self::parse_partition_spec(Some(&v.to_string()))?;
        }

        // Parse optional fields.
        if let Some(v) = config.get("target.file.size") {
            cfg.target_file_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid target.file.size: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("max.buffer.records") {
            cfg.max_buffer_records = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid max.buffer.records: '{v}'"))
            })?;
        }
        if let Some(v) = config.get("max.buffer.duration.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid max.buffer.duration.ms: '{v}'"))
            })?;
            cfg.max_buffer_duration = Duration::from_millis(ms);
        }
        if let Some(v) = config.get("schema.evolution") {
            cfg.schema_evolution = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("write.mode") {
            cfg.write_mode = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid write.mode: '{v}' (expected 'append', 'upsert')"
                ))
            })?;
        }
        if let Some(v) = config.get("equality.delete.columns") {
            cfg.equality_delete_columns = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        if let Some(v) = config.get("delivery.guarantee") {
            cfg.delivery_guarantee = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid delivery.guarantee: '{v}' (expected 'exactly-once', 'at-least-once')"
                ))
            })?;
        }
        if let Some(v) = config.get("writer.id") {
            cfg.writer_id = v.to_string();
        }
        if let Some(v) = config.get("file.format") {
            cfg.file_format = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid file.format: '{v}' (expected 'parquet', 'orc', 'avro')"
                ))
            })?;
        }

        // Maintenance config.
        if let Some(v) = config.get("maintenance.compaction") {
            cfg.maintenance.compaction_enabled = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("maintenance.expire-snapshots") {
            cfg.maintenance.expire_snapshots_enabled = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("maintenance.snapshot-retention.hours") {
            let hours: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid maintenance.snapshot-retention.hours: '{v}'"
                ))
            })?;
            cfg.maintenance.snapshot_retention = Duration::from_secs(hours * 3600);
        }
        if let Some(v) = config.get("maintenance.max-snapshots") {
            cfg.maintenance.max_snapshots = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid maintenance.max-snapshots: '{v}'"
                ))
            })?;
        }
        if let Some(v) = config.get("maintenance.compaction.min-files") {
            cfg.maintenance.compaction_min_files = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid maintenance.compaction.min-files: '{v}'"
                ))
            })?;
        }

        // Collect catalog-prefixed options (catalog.prop.*).
        cfg.catalog_properties = config
            .properties_with_prefix("catalog.prop.")
            .into_iter()
            .collect();

        // Resolve storage credentials: explicit options + environment variable fallbacks.
        let explicit_storage = config.properties_with_prefix("storage.");
        let resolved = StorageCredentialResolver::resolve(&cfg.warehouse, &explicit_storage);
        cfg.storage_options = resolved.options;

        cfg.validate()?;
        Ok(cfg)
    }

    /// Parses a partition spec string into partition fields.
    ///
    /// Format: `"DAYS(event_time), BUCKET(16, user_id), IDENTITY(region)"`
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid format.
    pub fn parse_partition_spec(
        spec: Option<&String>,
    ) -> Result<Vec<IcebergPartitionField>, ConnectorError> {
        let spec_str = match spec {
            Some(s) if !s.is_empty() => s,
            _ => return Ok(Vec::new()),
        };

        let mut fields = Vec::new();
        // Split on commas that are not inside parentheses.
        let parts = Self::split_respecting_parens(spec_str);
        for part in parts {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let field = Self::parse_single_partition_field(part)?;
            fields.push(field);
        }
        Ok(fields)
    }

    /// Splits a string on commas, but respects parentheses.
    /// E.g., `"DAYS(a), BUCKET(16, b)"` -> `["DAYS(a)", "BUCKET(16, b)"]`
    fn split_respecting_parens(s: &str) -> Vec<&str> {
        let mut parts = Vec::new();
        let mut depth: i32 = 0;
        let mut start = 0;

        for (i, c) in s.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    parts.push(&s[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        // Add the last part.
        if start < s.len() {
            parts.push(&s[start..]);
        }
        parts
    }

    /// Parses a single partition field like `DAYS(event_time)` or `BUCKET(16, user_id)`.
    fn parse_single_partition_field(
        spec: &str,
    ) -> Result<IcebergPartitionField, ConnectorError> {
        let open = spec.find('(').ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "invalid partition field '{spec}': expected TRANSFORM(column)"
            ))
        })?;
        let close = spec.rfind(')').ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "invalid partition field '{spec}': missing closing parenthesis"
            ))
        })?;

        let transform_name = spec[..open].trim().to_uppercase();
        let args: Vec<&str> = spec[open + 1..close].split(',').map(str::trim).collect();

        let (transform, source_column) = match transform_name.as_str() {
            "IDENTITY" => {
                if args.len() != 1 {
                    return Err(ConnectorError::ConfigurationError(
                        "IDENTITY transform requires exactly 1 argument: IDENTITY(column)".into(),
                    ));
                }
                (IcebergTransform::Identity, args[0].to_string())
            }
            "BUCKET" => {
                if args.len() != 2 {
                    return Err(ConnectorError::ConfigurationError(
                        "BUCKET transform requires 2 arguments: BUCKET(n, column)".into(),
                    ));
                }
                let n: u32 = args[0].parse().map_err(|_| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid bucket count: '{}'",
                        args[0]
                    ))
                })?;
                (IcebergTransform::Bucket(n), args[1].to_string())
            }
            "TRUNCATE" => {
                if args.len() != 2 {
                    return Err(ConnectorError::ConfigurationError(
                        "TRUNCATE transform requires 2 arguments: TRUNCATE(width, column)".into(),
                    ));
                }
                let w: u32 = args[0].parse().map_err(|_| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid truncate width: '{}'",
                        args[0]
                    ))
                })?;
                (IcebergTransform::Truncate(w), args[1].to_string())
            }
            "YEAR" | "YEARS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::ConfigurationError(
                        "YEAR transform requires exactly 1 argument: YEAR(column)".into(),
                    ));
                }
                (IcebergTransform::Year, args[0].to_string())
            }
            "MONTH" | "MONTHS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::ConfigurationError(
                        "MONTH transform requires exactly 1 argument: MONTH(column)".into(),
                    ));
                }
                (IcebergTransform::Month, args[0].to_string())
            }
            "DAY" | "DAYS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::ConfigurationError(
                        "DAY transform requires exactly 1 argument: DAY(column)".into(),
                    ));
                }
                (IcebergTransform::Day, args[0].to_string())
            }
            "HOUR" | "HOURS" => {
                if args.len() != 1 {
                    return Err(ConnectorError::ConfigurationError(
                        "HOUR transform requires exactly 1 argument: HOUR(column)".into(),
                    ));
                }
                (IcebergTransform::Hour, args[0].to_string())
            }
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown partition transform: '{other}'. \
                     Expected: IDENTITY, BUCKET, TRUNCATE, YEAR, MONTH, DAY, HOUR"
                )));
            }
        };

        Ok(IcebergPartitionField {
            source_column,
            transform,
            name: None,
        })
    }

    /// Formats the storage options for safe logging with secrets redacted.
    #[must_use]
    pub fn display_storage_options(&self) -> String {
        SecretMasker::display_map(&self.storage_options)
    }

    /// Returns the full table identifier as `namespace.table_name`.
    #[must_use]
    pub fn full_table_name(&self) -> String {
        if self.namespace.is_empty() {
            self.table_name.clone()
        } else {
            format!("{}.{}", self.namespace.join("."), self.table_name)
        }
    }

    /// Validates the configuration for consistency.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid combinations.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.warehouse.is_empty() {
            return Err(ConnectorError::MissingConfig("warehouse".into()));
        }
        if self.namespace.is_empty() {
            return Err(ConnectorError::MissingConfig("namespace".into()));
        }
        if self.table_name.is_empty() {
            return Err(ConnectorError::MissingConfig("table.name".into()));
        }
        if self.write_mode == IcebergWriteMode::Upsert
            && self.equality_delete_columns.is_empty()
        {
            return Err(ConnectorError::ConfigurationError(
                "upsert mode requires 'equality.delete.columns' to be set".into(),
            ));
        }
        if self.max_buffer_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max.buffer.records must be > 0".into(),
            ));
        }
        if self.target_file_size == 0 {
            return Err(ConnectorError::ConfigurationError(
                "target.file.size must be > 0".into(),
            ));
        }

        // Validate cloud storage credentials for the detected provider.
        let resolved = ResolvedStorageOptions {
            provider: StorageProvider::detect(&self.warehouse),
            options: self.storage_options.clone(),
            env_resolved_keys: Vec::new(),
        };
        let cloud_result = CloudConfigValidator::validate(&resolved);
        if !cloud_result.is_valid() {
            return Err(ConnectorError::ConfigurationError(
                cloud_result.error_message(),
            ));
        }

        Ok(())
    }
}

/// Iceberg catalog type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IcebergCatalogType {
    /// REST catalog (Iceberg REST `OpenAPI` spec).
    /// Compatible with Polaris, Tabular, Nessie REST, Unity Catalog.
    #[default]
    Rest,
    /// AWS Glue Data Catalog.
    Glue,
    /// Hive Metastore (Thrift).
    Hive,
    /// In-memory catalog (testing only).
    Memory,
}

impl FromStr for IcebergCatalogType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "rest" => Ok(Self::Rest),
            "glue" => Ok(Self::Glue),
            "hive" => Ok(Self::Hive),
            "memory" | "mem" => Ok(Self::Memory),
            other => Err(format!("unknown catalog type: '{other}'")),
        }
    }
}

impl fmt::Display for IcebergCatalogType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rest => write!(f, "rest"),
            Self::Glue => write!(f, "glue"),
            Self::Hive => write!(f, "hive"),
            Self::Memory => write!(f, "memory"),
        }
    }
}

/// Iceberg write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IcebergWriteMode {
    /// Append-only: all records are inserts. Most efficient mode.
    #[default]
    Append,
    /// Upsert: equality delete + insert for CDC/changelog streams.
    /// Requires `equality_delete_columns` to be set.
    /// Uses Iceberg v2 equality delete files for row-level operations.
    Upsert,
}

impl FromStr for IcebergWriteMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "append" => Ok(Self::Append),
            "upsert" | "merge" => Ok(Self::Upsert),
            other => Err(format!("unknown write mode: '{other}'")),
        }
    }
}

impl fmt::Display for IcebergWriteMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Append => write!(f, "append"),
            Self::Upsert => write!(f, "upsert"),
        }
    }
}

/// Delivery guarantee level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryGuarantee {
    /// May produce duplicates on recovery. Simpler, slightly higher throughput.
    AtLeastOnce,
    /// No duplicates. Uses epoch-to-snapshot mapping via summary properties.
    #[default]
    ExactlyOnce,
}

impl FromStr for DeliveryGuarantee {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "_").as_str() {
            "at_least_once" | "atleastonce" => Ok(Self::AtLeastOnce),
            "exactly_once" | "exactlyonce" => Ok(Self::ExactlyOnce),
            other => Err(format!("unknown delivery guarantee: '{other}'")),
        }
    }
}

impl fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtLeastOnce => write!(f, "at-least-once"),
            Self::ExactlyOnce => write!(f, "exactly-once"),
        }
    }
}

/// Iceberg partition field with transform.
#[derive(Debug, Clone)]
pub struct IcebergPartitionField {
    /// Source column name.
    pub source_column: String,
    /// Partition transform to apply.
    pub transform: IcebergTransform,
    /// Optional partition field name (defaults to `transform_source`).
    pub name: Option<String>,
}

impl IcebergPartitionField {
    /// Creates a new partition field with the given transform.
    #[must_use]
    pub fn new(source_column: &str, transform: IcebergTransform) -> Self {
        Self {
            source_column: source_column.to_string(),
            transform,
            name: None,
        }
    }

    /// Creates a partition field with an explicit name.
    #[must_use]
    pub fn with_name(source_column: &str, transform: IcebergTransform, name: &str) -> Self {
        Self {
            source_column: source_column.to_string(),
            transform,
            name: Some(name.to_string()),
        }
    }
}

impl fmt::Display for IcebergPartitionField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.transform {
            IcebergTransform::Identity => write!(f, "IDENTITY({})", self.source_column),
            IcebergTransform::Bucket(n) => write!(f, "BUCKET({}, {})", n, self.source_column),
            IcebergTransform::Truncate(w) => write!(f, "TRUNCATE({}, {})", w, self.source_column),
            IcebergTransform::Year => write!(f, "YEAR({})", self.source_column),
            IcebergTransform::Month => write!(f, "MONTH({})", self.source_column),
            IcebergTransform::Day => write!(f, "DAY({})", self.source_column),
            IcebergTransform::Hour => write!(f, "HOUR({})", self.source_column),
        }
    }
}

/// Iceberg partition transforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergTransform {
    /// Identity transform: partition by raw column value.
    Identity,
    /// Bucket hash transform: distribute into N buckets.
    Bucket(u32),
    /// Truncate transform: truncate to width W.
    Truncate(u32),
    /// Year transform: extract year from timestamp/date.
    Year,
    /// Month transform: extract year-month from timestamp/date.
    Month,
    /// Day transform: extract date from timestamp.
    Day,
    /// Hour transform: extract date-hour from timestamp.
    Hour,
}

impl fmt::Display for IcebergTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity => write!(f, "identity"),
            Self::Bucket(n) => write!(f, "bucket[{n}]"),
            Self::Truncate(w) => write!(f, "truncate[{w}]"),
            Self::Year => write!(f, "year"),
            Self::Month => write!(f, "month"),
            Self::Day => write!(f, "day"),
            Self::Hour => write!(f, "hour"),
        }
    }
}

/// Sort field for data file ordering.
#[derive(Debug, Clone)]
pub struct SortField {
    /// Column name.
    pub column: String,
    /// Sort direction.
    pub direction: SortDirection,
    /// Null ordering.
    pub null_order: NullOrder,
}

impl SortField {
    /// Creates a new ascending sort field.
    #[must_use]
    pub fn ascending(column: &str) -> Self {
        Self {
            column: column.to_string(),
            direction: SortDirection::Ascending,
            null_order: NullOrder::NullsLast,
        }
    }

    /// Creates a new descending sort field.
    #[must_use]
    pub fn descending(column: &str) -> Self {
        Self {
            column: column.to_string(),
            direction: SortDirection::Descending,
            null_order: NullOrder::NullsLast,
        }
    }
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortDirection {
    /// Ascending order (default).
    #[default]
    Ascending,
    /// Descending order.
    Descending,
}

/// Null ordering in sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullOrder {
    /// Nulls sort before all non-null values.
    NullsFirst,
    /// Nulls sort after all non-null values (default).
    #[default]
    NullsLast,
}

/// Iceberg data file format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IcebergFileFormat {
    /// Apache Parquet (recommended, default).
    #[default]
    Parquet,
    /// Apache ORC.
    Orc,
    /// Apache Avro.
    Avro,
}

impl FromStr for IcebergFileFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "orc" => Ok(Self::Orc),
            "avro" => Ok(Self::Avro),
            other => Err(format!("unknown file format: '{other}'")),
        }
    }
}

impl fmt::Display for IcebergFileFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parquet => write!(f, "parquet"),
            Self::Orc => write!(f, "orc"),
            Self::Avro => write!(f, "avro"),
        }
    }
}

/// Configuration for background table maintenance.
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// Whether to run automatic compaction (rewrite data files).
    pub compaction_enabled: bool,
    /// Minimum number of small files before triggering compaction.
    pub compaction_min_files: usize,
    /// Target file size after compaction.
    pub compaction_target_file_size: usize,
    /// Whether to expire old snapshots automatically.
    pub expire_snapshots_enabled: bool,
    /// Retention period for snapshots before expiry.
    pub snapshot_retention: Duration,
    /// Maximum number of snapshots to retain.
    pub max_snapshots: usize,
    /// How often to check if maintenance is needed.
    pub check_interval: Duration,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            compaction_enabled: true,
            compaction_min_files: 10,
            compaction_target_file_size: 128 * 1024 * 1024, // 128 MB
            expire_snapshots_enabled: true,
            snapshot_retention: Duration::from_secs(7 * 24 * 3600), // 7 days
            max_snapshots: 100,
            check_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("iceberg");
        for (k, v) in pairs {
            config.set(*k, *v);
        }
        config
    }

    fn required_pairs() -> Vec<(&'static str, &'static str)> {
        vec![
            ("warehouse", "/data/warehouse"),
            ("namespace", "analytics.prod"),
            ("table.name", "trades"),
        ]
    }

    // ── Config parsing tests ──

    #[test]
    fn test_parse_required_fields() {
        let config = make_config(&required_pairs());
        let cfg = IcebergSinkConfig::from_config(&config).unwrap();
        assert_eq!(cfg.warehouse, "/data/warehouse");
        assert_eq!(cfg.namespace, vec!["analytics", "prod"]);
        assert_eq!(cfg.table_name, "trades");
        assert_eq!(cfg.catalog_type, IcebergCatalogType::Rest);
        assert_eq!(cfg.write_mode, IcebergWriteMode::Append);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
        assert!(cfg.partition_spec.is_empty());
        assert_eq!(cfg.target_file_size, 128 * 1024 * 1024);
        assert_eq!(cfg.max_buffer_records, 100_000);
        assert!(!cfg.schema_evolution);
        assert!(cfg.maintenance.compaction_enabled);
    }

    #[test]
    fn test_missing_warehouse() {
        let config = make_config(&[
            ("namespace", "analytics"),
            ("table.name", "trades"),
        ]);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_missing_namespace() {
        let config = make_config(&[
            ("warehouse", "/data/warehouse"),
            ("table.name", "trades"),
        ]);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_missing_table_name() {
        let config = make_config(&[
            ("warehouse", "/data/warehouse"),
            ("namespace", "analytics"),
        ]);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_parse_all_optional_fields() {
        let mut pairs = required_pairs();
        pairs.extend_from_slice(&[
            ("catalog.type", "glue"),
            ("catalog.uri", "arn:aws:glue:us-east-1:123456789:catalog"),
            ("partition.spec", "DAYS(event_time), BUCKET(16, customer_id)"),
            ("target.file.size", "67108864"),
            ("max.buffer.records", "50000"),
            ("max.buffer.duration.ms", "30000"),
            ("schema.evolution", "true"),
            ("write.mode", "upsert"),
            ("equality.delete.columns", "id, ts"),
            ("delivery.guarantee", "at-least-once"),
            ("file.format", "parquet"),
            ("writer.id", "my-writer"),
            ("maintenance.compaction", "true"),
            ("maintenance.expire-snapshots", "true"),
            ("maintenance.snapshot-retention.hours", "336"),
            ("maintenance.max-snapshots", "50"),
            ("maintenance.compaction.min-files", "20"),
        ]);
        let config = make_config(&pairs);
        let cfg = IcebergSinkConfig::from_config(&config).unwrap();

        assert_eq!(cfg.catalog_type, IcebergCatalogType::Glue);
        assert_eq!(cfg.catalog_uri, "arn:aws:glue:us-east-1:123456789:catalog");
        assert_eq!(cfg.partition_spec.len(), 2);
        assert_eq!(cfg.target_file_size, 67_108_864);
        assert_eq!(cfg.max_buffer_records, 50_000);
        assert_eq!(cfg.max_buffer_duration, Duration::from_millis(30_000));
        assert!(cfg.schema_evolution);
        assert_eq!(cfg.write_mode, IcebergWriteMode::Upsert);
        assert_eq!(cfg.equality_delete_columns, vec!["id", "ts"]);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);
        assert_eq!(cfg.file_format, IcebergFileFormat::Parquet);
        assert_eq!(cfg.writer_id, "my-writer");
        assert!(cfg.maintenance.compaction_enabled);
        assert!(cfg.maintenance.expire_snapshots_enabled);
        assert_eq!(
            cfg.maintenance.snapshot_retention,
            Duration::from_secs(336 * 3600)
        );
        assert_eq!(cfg.maintenance.max_snapshots, 50);
        assert_eq!(cfg.maintenance.compaction_min_files, 20);
    }

    #[test]
    fn test_upsert_requires_equality_delete_columns() {
        let mut pairs = required_pairs();
        pairs.push(("write.mode", "upsert"));
        let config = make_config(&pairs);
        let result = IcebergSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("equality.delete.columns"), "error: {err}");
    }

    #[test]
    fn test_empty_warehouse_rejected() {
        let mut cfg = IcebergSinkConfig::default();
        cfg.warehouse = String::new();
        cfg.namespace = vec!["analytics".to_string()];
        cfg.table_name = "trades".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_empty_namespace_rejected() {
        let mut cfg = IcebergSinkConfig::default();
        cfg.warehouse = "/data".to_string();
        cfg.namespace = Vec::new();
        cfg.table_name = "trades".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_zero_max_buffer_records_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("max.buffer.records", "0"));
        let config = make_config(&pairs);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_zero_target_file_size_rejected() {
        let mut pairs = required_pairs();
        pairs.push(("target.file.size", "0"));
        let config = make_config(&pairs);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_invalid_catalog_type() {
        let mut pairs = required_pairs();
        pairs.push(("catalog.type", "unknown"));
        let config = make_config(&pairs);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_invalid_write_mode() {
        let mut pairs = required_pairs();
        pairs.push(("write.mode", "unknown"));
        let config = make_config(&pairs);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    #[test]
    fn test_invalid_file_format() {
        let mut pairs = required_pairs();
        pairs.push(("file.format", "csv"));
        let config = make_config(&pairs);
        assert!(IcebergSinkConfig::from_config(&config).is_err());
    }

    // ── Partition spec parsing tests ──

    #[test]
    fn test_parse_partition_spec_identity() {
        let spec = "IDENTITY(region)";
        let fields = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string())).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].source_column, "region");
        assert_eq!(fields[0].transform, IcebergTransform::Identity);
    }

    #[test]
    fn test_parse_partition_spec_bucket() {
        let spec = "BUCKET(16, customer_id)";
        let fields = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string())).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].source_column, "customer_id");
        assert_eq!(fields[0].transform, IcebergTransform::Bucket(16));
    }

    #[test]
    fn test_parse_partition_spec_truncate() {
        let spec = "TRUNCATE(4, product_id)";
        let fields = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string())).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].source_column, "product_id");
        assert_eq!(fields[0].transform, IcebergTransform::Truncate(4));
    }

    #[test]
    fn test_parse_partition_spec_time_transforms() {
        let spec = "YEAR(ts), MONTH(ts), DAY(ts), HOUR(ts)";
        let fields = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string())).unwrap();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].transform, IcebergTransform::Year);
        assert_eq!(fields[1].transform, IcebergTransform::Month);
        assert_eq!(fields[2].transform, IcebergTransform::Day);
        assert_eq!(fields[3].transform, IcebergTransform::Hour);
    }

    #[test]
    fn test_parse_partition_spec_multi() {
        let spec = "DAYS(event_time), BUCKET(32, user_id), IDENTITY(region)";
        let fields = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string())).unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].transform, IcebergTransform::Day);
        assert_eq!(fields[1].transform, IcebergTransform::Bucket(32));
        assert_eq!(fields[2].transform, IcebergTransform::Identity);
    }

    #[test]
    fn test_parse_partition_spec_empty() {
        let fields = IcebergSinkConfig::parse_partition_spec(None).unwrap();
        assert!(fields.is_empty());

        let fields =
            IcebergSinkConfig::parse_partition_spec(Some(&String::new())).unwrap();
        assert!(fields.is_empty());
    }

    #[test]
    fn test_parse_partition_spec_invalid_no_parens() {
        let spec = "IDENTITY";
        let result = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_partition_spec_unknown_transform() {
        let spec = "UNKNOWN(col)";
        let result = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_partition_spec_bucket_wrong_args() {
        let spec = "BUCKET(col)";
        let result = IcebergSinkConfig::parse_partition_spec(Some(&spec.to_string()));
        assert!(result.is_err());
    }

    // ── Enum tests ──

    #[test]
    fn test_catalog_type_parse() {
        assert_eq!(
            "rest".parse::<IcebergCatalogType>().unwrap(),
            IcebergCatalogType::Rest
        );
        assert_eq!(
            "glue".parse::<IcebergCatalogType>().unwrap(),
            IcebergCatalogType::Glue
        );
        assert_eq!(
            "hive".parse::<IcebergCatalogType>().unwrap(),
            IcebergCatalogType::Hive
        );
        assert_eq!(
            "memory".parse::<IcebergCatalogType>().unwrap(),
            IcebergCatalogType::Memory
        );
        assert!("unknown".parse::<IcebergCatalogType>().is_err());
    }

    #[test]
    fn test_catalog_type_display() {
        assert_eq!(IcebergCatalogType::Rest.to_string(), "rest");
        assert_eq!(IcebergCatalogType::Glue.to_string(), "glue");
        assert_eq!(IcebergCatalogType::Hive.to_string(), "hive");
        assert_eq!(IcebergCatalogType::Memory.to_string(), "memory");
    }

    #[test]
    fn test_write_mode_parse() {
        assert_eq!(
            "append".parse::<IcebergWriteMode>().unwrap(),
            IcebergWriteMode::Append
        );
        assert_eq!(
            "upsert".parse::<IcebergWriteMode>().unwrap(),
            IcebergWriteMode::Upsert
        );
        assert_eq!(
            "merge".parse::<IcebergWriteMode>().unwrap(),
            IcebergWriteMode::Upsert
        );
        assert!("unknown".parse::<IcebergWriteMode>().is_err());
    }

    #[test]
    fn test_write_mode_display() {
        assert_eq!(IcebergWriteMode::Append.to_string(), "append");
        assert_eq!(IcebergWriteMode::Upsert.to_string(), "upsert");
    }

    #[test]
    fn test_delivery_guarantee_parse() {
        assert_eq!(
            "at-least-once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            "at_least_once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::AtLeastOnce
        );
        assert_eq!(
            "exactly-once".parse::<DeliveryGuarantee>().unwrap(),
            DeliveryGuarantee::ExactlyOnce
        );
        assert!("unknown".parse::<DeliveryGuarantee>().is_err());
    }

    #[test]
    fn test_delivery_guarantee_display() {
        assert_eq!(DeliveryGuarantee::AtLeastOnce.to_string(), "at-least-once");
        assert_eq!(DeliveryGuarantee::ExactlyOnce.to_string(), "exactly-once");
    }

    #[test]
    fn test_file_format_parse() {
        assert_eq!(
            "parquet".parse::<IcebergFileFormat>().unwrap(),
            IcebergFileFormat::Parquet
        );
        assert_eq!(
            "orc".parse::<IcebergFileFormat>().unwrap(),
            IcebergFileFormat::Orc
        );
        assert_eq!(
            "avro".parse::<IcebergFileFormat>().unwrap(),
            IcebergFileFormat::Avro
        );
        assert!("csv".parse::<IcebergFileFormat>().is_err());
    }

    #[test]
    fn test_file_format_display() {
        assert_eq!(IcebergFileFormat::Parquet.to_string(), "parquet");
        assert_eq!(IcebergFileFormat::Orc.to_string(), "orc");
        assert_eq!(IcebergFileFormat::Avro.to_string(), "avro");
    }

    #[test]
    fn test_partition_field_display() {
        let f = IcebergPartitionField::new("event_time", IcebergTransform::Day);
        assert_eq!(f.to_string(), "DAY(event_time)");

        let f = IcebergPartitionField::new("user_id", IcebergTransform::Bucket(16));
        assert_eq!(f.to_string(), "BUCKET(16, user_id)");
    }

    #[test]
    fn test_transform_display() {
        assert_eq!(IcebergTransform::Identity.to_string(), "identity");
        assert_eq!(IcebergTransform::Bucket(16).to_string(), "bucket[16]");
        assert_eq!(IcebergTransform::Truncate(4).to_string(), "truncate[4]");
        assert_eq!(IcebergTransform::Year.to_string(), "year");
        assert_eq!(IcebergTransform::Month.to_string(), "month");
        assert_eq!(IcebergTransform::Day.to_string(), "day");
        assert_eq!(IcebergTransform::Hour.to_string(), "hour");
    }

    #[test]
    fn test_full_table_name() {
        let cfg = IcebergSinkConfig::new("/warehouse", "analytics.prod", "trades");
        assert_eq!(cfg.full_table_name(), "analytics.prod.trades");

        let mut cfg2 = cfg.clone();
        cfg2.namespace = Vec::new();
        assert_eq!(cfg2.full_table_name(), "trades");
    }

    #[test]
    fn test_defaults() {
        let cfg = IcebergSinkConfig::default();
        assert!(cfg.warehouse.is_empty());
        assert!(cfg.namespace.is_empty());
        assert!(cfg.table_name.is_empty());
        assert_eq!(cfg.catalog_type, IcebergCatalogType::Rest);
        assert_eq!(cfg.target_file_size, 128 * 1024 * 1024);
        assert_eq!(cfg.max_buffer_records, 100_000);
        assert_eq!(cfg.max_buffer_duration, Duration::from_secs(60));
        assert!(!cfg.schema_evolution);
        assert_eq!(cfg.write_mode, IcebergWriteMode::Append);
        assert_eq!(cfg.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
        assert_eq!(cfg.file_format, IcebergFileFormat::Parquet);
        assert!(!cfg.writer_id.is_empty());
    }

    #[test]
    fn test_new_helper() {
        let cfg = IcebergSinkConfig::new("/tmp/warehouse", "db.schema", "test_table");
        assert_eq!(cfg.warehouse, "/tmp/warehouse");
        assert_eq!(cfg.namespace, vec!["db", "schema"]);
        assert_eq!(cfg.table_name, "test_table");
        assert_eq!(cfg.write_mode, IcebergWriteMode::Append);
    }

    #[test]
    fn test_maintenance_config_defaults() {
        let cfg = MaintenanceConfig::default();
        assert!(cfg.compaction_enabled);
        assert_eq!(cfg.compaction_min_files, 10);
        assert_eq!(cfg.compaction_target_file_size, 128 * 1024 * 1024);
        assert!(cfg.expire_snapshots_enabled);
        assert_eq!(cfg.snapshot_retention, Duration::from_secs(7 * 24 * 3600));
        assert_eq!(cfg.max_snapshots, 100);
        assert_eq!(cfg.check_interval, Duration::from_secs(300));
    }

    #[test]
    fn test_sort_field() {
        let asc = SortField::ascending("event_time");
        assert_eq!(asc.column, "event_time");
        assert_eq!(asc.direction, SortDirection::Ascending);
        assert_eq!(asc.null_order, NullOrder::NullsLast);

        let desc = SortField::descending("price");
        assert_eq!(desc.direction, SortDirection::Descending);
    }

    // ── Cloud storage integration tests ──

    #[test]
    fn test_s3_warehouse_requires_region() {
        let config = make_config(&[
            ("warehouse", "s3://my-bucket/warehouse"),
            ("namespace", "analytics"),
            ("table.name", "trades"),
        ]);
        let result = IcebergSinkConfig::from_config(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("aws_region"), "error: {err}");
    }

    #[test]
    fn test_s3_warehouse_with_region() {
        let config = make_config(&[
            ("warehouse", "s3://my-bucket/warehouse"),
            ("namespace", "analytics"),
            ("table.name", "trades"),
            ("storage.aws_region", "us-east-1"),
        ]);
        assert!(IcebergSinkConfig::from_config(&config).is_ok());
    }

    #[test]
    fn test_local_warehouse_no_cloud_validation() {
        let config = make_config(&[
            ("warehouse", "/data/warehouse"),
            ("namespace", "analytics"),
            ("table.name", "trades"),
        ]);
        assert!(IcebergSinkConfig::from_config(&config).is_ok());
    }

    #[test]
    fn test_display_storage_options_redacts_secrets() {
        let mut cfg = IcebergSinkConfig::new("s3://bucket/warehouse", "db", "table");
        cfg.storage_options
            .insert("aws_region".to_string(), "us-east-1".to_string());
        cfg.storage_options
            .insert("aws_secret_access_key".to_string(), "TOP_SECRET".to_string());

        let display = cfg.display_storage_options();
        assert!(display.contains("aws_region=us-east-1"));
        assert!(display.contains("aws_secret_access_key=***"));
        assert!(!display.contains("TOP_SECRET"));
    }

    #[test]
    fn test_catalog_properties_prefix_stripping() {
        let mut pairs = required_pairs();
        pairs.push(("catalog.prop.token", "my-token"));
        pairs.push(("catalog.prop.scope", "catalog:read"));
        let config = make_config(&pairs);
        let cfg = IcebergSinkConfig::from_config(&config).unwrap();

        assert_eq!(cfg.catalog_properties.len(), 2);
        assert_eq!(cfg.catalog_properties.get("token"), Some(&"my-token".to_string()));
        assert_eq!(cfg.catalog_properties.get("scope"), Some(&"catalog:read".to_string()));
    }
}
