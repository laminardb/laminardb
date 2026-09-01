//! Delta Lake sink config. Parsed from the resolved connector config via
//! [`DeltaLakeSinkConfig::from_config`].
#![allow(clippy::disallowed_types)] // cold path: lakehouse configuration

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use crate::connector::DeliveryGuarantee;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::storage::{
    CloudConfigValidator, ResolvedStorageOptions, SecretMasker, StorageCredentialResolver,
    StorageProvider,
};

/// Configuration for the Delta Lake sink connector.
///
/// Parsed from resolved sink connector options or constructed programmatically.
#[derive(Debug, Clone)]
pub struct DeltaLakeSinkConfig {
    /// Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`).
    pub table_path: String,

    /// Columns to partition by (e.g., `["trade_date", "hour"]`).
    pub partition_columns: Vec<String>,

    /// Target Parquet file size in bytes (default: 128 MB).
    pub target_file_size: usize,

    /// Maximum number of records to buffer before flushing to Parquet.
    pub max_buffer_records: usize,

    /// Maximum time to buffer records before flushing.
    pub max_buffer_duration: Duration,

    /// Whether to enable schema evolution (auto-merge new columns).
    pub schema_evolution: bool,

    /// Write mode: Append, Overwrite, or Upsert (CDC merge).
    pub write_mode: DeltaWriteMode,

    /// Key columns for upsert/merge operations (required for Upsert mode).
    pub merge_key_columns: Vec<String>,

    /// Storage options (S3 credentials, Azure keys, etc.).
    pub storage_options: HashMap<String, String>,

    /// Delivery guarantee: `AtLeastOnce` or `ExactlyOnce`.
    pub delivery_guarantee: DeliveryGuarantee,

    /// Catalog type for table discovery.
    pub catalog_type: DeltaCatalogType,

    /// Catalog database name (required for Glue).
    pub catalog_database: Option<String>,

    /// Catalog name (required for Unity).
    pub catalog_name: Option<String>,

    /// Catalog schema name (required for Unity).
    pub catalog_schema: Option<String>,

    /// Storage location for auto-created Unity Catalog external tables.
    /// When set and the `uc://` table doesn't exist, the sink creates it
    /// via the Unity Catalog REST API at this storage location.
    pub catalog_storage_location: Option<String>,

    /// End-to-end timeout for one Delta write, including table reopen and all
    /// optimistic commit retries (default: 30s).
    pub write_timeout: Duration,

    /// Parquet writer properties (compression, bloom filters, statistics, etc.).
    pub parquet: ParquetWriteConfig,
}

impl Default for DeltaLakeSinkConfig {
    fn default() -> Self {
        Self {
            table_path: String::new(),
            partition_columns: Vec::new(),
            target_file_size: 128 * 1024 * 1024, // 128 MB
            max_buffer_records: 100_000,
            max_buffer_duration: Duration::from_secs(60),
            schema_evolution: false,
            write_mode: DeltaWriteMode::Append,
            merge_key_columns: Vec::new(),
            storage_options: HashMap::new(),
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            catalog_type: DeltaCatalogType::None,
            catalog_database: None,
            catalog_name: None,
            catalog_schema: None,
            catalog_storage_location: None,
            write_timeout: Duration::from_secs(30),
            parquet: ParquetWriteConfig::default(),
        }
    }
}

impl DeltaLakeSinkConfig {
    /// Creates a minimal config for testing.
    #[must_use]
    pub fn new(table_path: &str) -> Self {
        Self {
            table_path: table_path.to_string(),
            ..Default::default()
        }
    }

    /// Parses a sink config from a resolved [`ConnectorConfig`].
    ///
    /// # Required keys
    ///
    /// - `table.path` - Path to Delta Lake table
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::MissingConfig` if required keys are absent,
    /// or `ConnectorError::ConfigurationError` on invalid values.
    #[allow(clippy::too_many_lines)]
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let mut cfg = Self {
            table_path: config.require("table.path")?.to_string(),
            ..Self::default()
        };

        if let Some(v) = config.get("partition.columns") {
            cfg.partition_columns = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
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
                    "invalid write.mode: '{v}' (expected 'append', 'overwrite', or 'upsert')"
                ))
            })?;
        }
        if let Some(v) = config.get("merge.key.columns") {
            cfg.merge_key_columns = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        if let Some(v) = config.get("delivery.guarantee") {
            cfg.delivery_guarantee = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid delivery.guarantee: '{v}' \
                     (expected 'exactly-once' or 'at-least-once')"
                ))
            })?;
        }
        if cfg.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && cfg.write_mode != DeltaWriteMode::Append
        {
            return Err(ConnectorError::ConfigurationError(
                "Delta exactly-once is supported only for coordinated append mode; \
                 upsert/overwrite do not expose a certified distributed committable"
                    .into(),
            ));
        }

        // ── Catalog configuration ──
        if let Some(v) = config.get("catalog.type") {
            cfg.catalog_type = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid catalog.type: '{v}' (expected 'none', 'glue', or 'unity')"
                ))
            })?;
        }
        if let Some(v) = config.get("catalog.database") {
            cfg.catalog_database = Some(v.to_string());
        }
        if let Some(v) = config.get("catalog.name") {
            cfg.catalog_name = Some(v.to_string());
        }
        if let Some(v) = config.get("catalog.schema") {
            cfg.catalog_schema = Some(v.to_string());
        }
        // Unity-specific: populate workspace_url and access_token into the enum variant.
        if let DeltaCatalogType::Unity {
            ref mut workspace_url,
            ref mut access_token,
        } = cfg.catalog_type
        {
            if let Some(v) = config.get("catalog.workspace_url") {
                *workspace_url = v.to_string();
            }
            if let Some(v) = config.get("catalog.access_token") {
                *access_token = v.to_string();
            }
        }
        if let Some(v) = config.get("catalog.storage.location") {
            cfg.catalog_storage_location = Some(v.to_string());
        }
        if let Some(v) = config.get("write.timeout.ms") {
            let ms: u64 = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!("invalid write.timeout.ms: '{v}'"))
            })?;
            cfg.write_timeout = Duration::from_millis(ms);
        }

        // ── Parquet writer configuration ──
        if let Some(v) = config.get("parquet.compression") {
            cfg.parquet.compression = v.to_string();
        }
        if let Some(v) = config.get("parquet.compression.level") {
            cfg.parquet.compression_level = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid parquet.compression.level: '{v}'"
                ))
            })?;
        }
        if let Some(v) = config.get("parquet.dictionary.enabled") {
            cfg.parquet.dictionary_enabled = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = config.get("parquet.statistics") {
            cfg.parquet.statistics = v.to_string();
        }
        if let Some(v) = config.get("parquet.bloom.filter.columns") {
            cfg.parquet.bloom_filter_columns = v
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
        if let Some(v) = config.get("parquet.bloom.filter.fpp") {
            cfg.parquet.bloom_filter_fpp = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid parquet.bloom.filter.fpp: '{v}'"
                ))
            })?;
        }
        if let Some(v) = config.get("parquet.bloom.filter.ndv") {
            cfg.parquet.bloom_filter_ndv = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid parquet.bloom.filter.ndv: '{v}'"
                ))
            })?;
        }
        if let Some(v) = config.get("parquet.max.row.group.size") {
            cfg.parquet.max_row_group_size = v.parse().map_err(|_| {
                ConnectorError::ConfigurationError(format!(
                    "invalid parquet.max.row.group.size: '{v}'"
                ))
            })?;
        }

        // Resolve storage credentials: explicit options + environment variable fallbacks.
        let explicit_storage = config.properties_with_prefix("storage.");
        let resolved = StorageCredentialResolver::resolve(&cfg.table_path, &explicit_storage);
        cfg.storage_options = resolved.options;

        // Map LogStore configuration keys to delta-rs storage options.
        if let Some(v) = config.get("storage.s3_locking_provider") {
            cfg.storage_options
                .insert("AWS_S3_LOCKING_PROVIDER".to_string(), v.to_string());
        }
        if let Some(v) = config.get("storage.dynamodb_table_name") {
            cfg.storage_options
                .insert("DELTA_DYNAMO_TABLE_NAME".to_string(), v.to_string());
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Formats the storage options for safe logging with secrets redacted.
    #[must_use]
    pub fn display_storage_options(&self) -> String {
        SecretMasker::display_map(&self.storage_options)
    }

    /// Validates the configuration for consistency.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid combinations.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.table_path.is_empty() {
            return Err(ConnectorError::missing_config("table.path"));
        }
        if self.write_mode == DeltaWriteMode::Upsert && self.merge_key_columns.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "upsert mode requires 'merge.key.columns' to be set".into(),
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
        if self.write_timeout < Duration::from_secs(5) {
            return Err(ConnectorError::ConfigurationError(
                "write.timeout.ms must be >= 5000 (5 seconds)".into(),
            ));
        }

        match self.parquet.compression.to_lowercase().as_str() {
            "zstd" | "snappy" | "lz4" | "gzip" | "none" | "uncompressed" => {}
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown parquet.compression: '{other}' \
                     (expected 'zstd', 'snappy', 'lz4', 'gzip', or 'none')"
                )));
            }
        }
        match self.parquet.statistics.to_lowercase().as_str() {
            "none" | "chunk" | "page" => {}
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown parquet.statistics: '{other}' (expected 'none', 'chunk', or 'page')"
                )));
            }
        }
        if self.parquet.bloom_filter_fpp <= 0.0 || self.parquet.bloom_filter_fpp >= 1.0 {
            return Err(ConnectorError::ConfigurationError(
                "parquet.bloom.filter.fpp must be in (0.0, 1.0)".into(),
            ));
        }
        if self.parquet.max_row_group_size == 0 {
            return Err(ConnectorError::ConfigurationError(
                "parquet.max.row.group.size must be > 0".into(),
            ));
        }
        // Eagerly validate that WriterProperties can be built so invalid
        // codec/level combos are caught at config time, not first write.
        #[cfg(feature = "delta-lake")]
        {
            self.parquet.to_writer_properties()?;
        }
        self.validate_catalog()?;

        // Validate cloud storage credentials (skip when catalog resolves the path).
        if self.catalog_type == DeltaCatalogType::None {
            let resolved = ResolvedStorageOptions {
                provider: StorageProvider::detect(&self.table_path),
                options: self.storage_options.clone(),
                env_resolved_keys: Vec::new(),
            };
            let cloud_result = CloudConfigValidator::validate(&resolved);
            if !cloud_result.is_valid() {
                return Err(ConnectorError::ConfigurationError(
                    cloud_result.error_message(),
                ));
            }
        }

        Ok(())
    }

    /// Validates catalog-specific requirements.
    fn validate_catalog(&self) -> Result<(), ConnectorError> {
        match &self.catalog_type {
            DeltaCatalogType::None => {}
            DeltaCatalogType::Glue => {
                #[cfg(not(feature = "delta-lake-glue"))]
                return Err(ConnectorError::ConfigurationError(
                    "Glue catalog requires the 'delta-lake-glue' feature. \
                     Build with: cargo build --features delta-lake-glue"
                        .into(),
                ));
                #[cfg(feature = "delta-lake-glue")]
                if self.catalog_database.is_none() {
                    return Err(ConnectorError::ConfigurationError(
                        "Glue catalog requires 'catalog.database' to be set".into(),
                    ));
                }
            }
            DeltaCatalogType::Unity {
                workspace_url,
                access_token,
            } => {
                #[cfg(not(feature = "delta-lake-unity"))]
                {
                    let _ = (workspace_url, access_token);
                    return Err(ConnectorError::ConfigurationError(
                        "Unity catalog requires the 'delta-lake-unity' feature. \
                         Build with: cargo build --features delta-lake-unity"
                            .into(),
                    ));
                }
                #[cfg(feature = "delta-lake-unity")]
                {
                    if workspace_url.is_empty() {
                        return Err(ConnectorError::ConfigurationError(
                            "Unity catalog requires 'catalog.workspace_url' to be set".into(),
                        ));
                    }
                    if access_token.is_empty() {
                        return Err(ConnectorError::ConfigurationError(
                            "Unity catalog requires 'catalog.access_token' to be set".into(),
                        ));
                    }
                    if self.catalog_storage_location.is_some() {
                        if self.catalog_name.is_none() {
                            return Err(ConnectorError::ConfigurationError(
                                "Unity catalog auto-create requires 'catalog.name' to be set"
                                    .into(),
                            ));
                        }
                        if self.catalog_schema.is_none() {
                            return Err(ConnectorError::ConfigurationError(
                                "Unity catalog auto-create requires 'catalog.schema' to be set"
                                    .into(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Delta Lake write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaWriteMode {
    /// Append-only: all records are inserts. Most efficient for immutable streams.
    Append,
    /// Overwrite: replace partition contents. Used for batch-style recomputation.
    Overwrite,
    /// Upsert/Merge: CDC-style insert/update/delete via MERGE statement.
    /// Requires `merge_key_columns` to be set. Integrates with Z-sets.
    Upsert,
}

impl FromStr for DeltaWriteMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "append" => Ok(Self::Append),
            "overwrite" => Ok(Self::Overwrite),
            "upsert" | "merge" => Ok(Self::Upsert),
            other => Err(format!("unknown write mode: '{other}'")),
        }
    }
}

impl fmt::Display for DeltaWriteMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Append => write!(f, "append"),
            Self::Overwrite => write!(f, "overwrite"),
            Self::Upsert => write!(f, "upsert"),
        }
    }
}

/// Delta Lake catalog type for table discovery.
///
/// Catalogs enable referencing tables by logical names instead of raw paths.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum DeltaCatalogType {
    /// No catalog — table path is a direct file or cloud URI.
    #[default]
    None,
    /// AWS Glue Data Catalog.
    Glue,
    /// Databricks Unity Catalog.
    Unity {
        /// Databricks workspace URL (e.g., `https://xxx.cloud.databricks.com`).
        workspace_url: String,
        /// Databricks access token.
        access_token: String,
    },
}

impl FromStr for DeltaCatalogType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" | "" => Ok(Self::None),
            "glue" => Ok(Self::Glue),
            "unity" => Ok(Self::Unity {
                workspace_url: String::new(),
                access_token: String::new(),
            }),
            other => Err(format!("unknown catalog type: '{other}'")),
        }
    }
}

impl fmt::Display for DeltaCatalogType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Glue => write!(f, "glue"),
            Self::Unity { .. } => write!(f, "unity"),
        }
    }
}

/// Configuration for Parquet writer properties (compression, dictionary
/// encoding, statistics, bloom filters, row group sizing).
#[derive(Debug, Clone)]
pub struct ParquetWriteConfig {
    /// Compression codec: `"zstd"`, `"snappy"`, `"lz4"`, `"gzip"`, or `"none"`.
    pub compression: String,
    /// Compression level (default: 1 — ZSTD L1 for hot writes).
    pub compression_level: i32,
    /// Whether to enable dictionary encoding (default: true).
    pub dictionary_enabled: bool,
    /// Statistics granularity: `"none"`, `"chunk"`, or `"page"` (default: `"page"`).
    pub statistics: String,
    /// Columns to build bloom filters for (default: empty).
    pub bloom_filter_columns: Vec<String>,
    /// Bloom filter false-positive probability (default: 0.01).
    pub bloom_filter_fpp: f64,
    /// Bloom filter expected number of distinct values (0 = parquet default).
    pub bloom_filter_ndv: u64,
    /// Maximum rows per row group (default: 1,000,000).
    pub max_row_group_size: usize,
}

impl Default for ParquetWriteConfig {
    fn default() -> Self {
        Self {
            compression: "zstd".to_string(),
            compression_level: 1,
            dictionary_enabled: true,
            statistics: "page".to_string(),
            bloom_filter_columns: Vec::new(),
            bloom_filter_fpp: 0.01,
            bloom_filter_ndv: 0,
            max_row_group_size: 1_000_000,
        }
    }
}

#[cfg(feature = "delta-lake")]
impl ParquetWriteConfig {
    /// Builds `WriterProperties` for hot-path writes (uses `compression_level`).
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on invalid codec/level.
    pub fn to_writer_properties(
        &self,
    ) -> Result<deltalake::parquet::file::properties::WriterProperties, ConnectorError> {
        self.build_properties(self.compression_level)
    }

    /// Shared builder: maps string codec → `Compression`, sets dictionary,
    /// statistics, bloom filters, and row group size.
    fn build_properties(
        &self,
        level: i32,
    ) -> Result<deltalake::parquet::file::properties::WriterProperties, ConnectorError> {
        use deltalake::parquet::basic::{Compression, GzipLevel, ZstdLevel};
        use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties};
        use deltalake::parquet::schema::types::ColumnPath;

        let compression = match self.compression.to_lowercase().as_str() {
            "zstd" => {
                let zstd_level = ZstdLevel::try_new(level).map_err(|e| {
                    ConnectorError::ConfigurationError(format!("invalid ZSTD level {level}: {e}"))
                })?;
                Compression::ZSTD(zstd_level)
            }
            "snappy" => Compression::SNAPPY,
            "lz4" => Compression::LZ4_RAW,
            "gzip" => {
                let level_u32: u32 = level.try_into().map_err(|_| {
                    ConnectorError::ConfigurationError(format!(
                        "invalid GZIP level {level}: must be non-negative"
                    ))
                })?;
                let gzip_level = GzipLevel::try_new(level_u32).map_err(|e| {
                    ConnectorError::ConfigurationError(format!("invalid GZIP level {level}: {e}"))
                })?;
                Compression::GZIP(gzip_level)
            }
            "none" | "uncompressed" => Compression::UNCOMPRESSED,
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown parquet.compression: '{other}' \
                     (expected 'zstd', 'snappy', 'lz4', 'gzip', or 'none')"
                )));
            }
        };

        let statistics = match self.statistics.to_lowercase().as_str() {
            "none" => EnabledStatistics::None,
            "chunk" => EnabledStatistics::Chunk,
            "page" => EnabledStatistics::Page,
            other => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown parquet.statistics: '{other}' (expected 'none', 'chunk', or 'page')"
                )));
            }
        };

        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_dictionary_enabled(self.dictionary_enabled)
            .set_statistics_enabled(statistics)
            .set_max_row_group_row_count(Some(self.max_row_group_size));

        for col_name in &self.bloom_filter_columns {
            let col_path = ColumnPath::from(col_name.as_str());
            builder = builder
                .set_column_bloom_filter_enabled(col_path.clone(), true)
                .set_column_bloom_filter_fpp(col_path.clone(), self.bloom_filter_fpp);
            if self.bloom_filter_ndv > 0 {
                builder = builder.set_column_bloom_filter_ndv(col_path, self.bloom_filter_ndv);
            }
        }

        Ok(builder.build())
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests;
