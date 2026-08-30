use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

use super::{
    optional_alias, optional_non_empty, parse_comma_list, parse_duration_value, parse_nonzero,
    parse_nonzero_value, parse_optional_alias, parse_or_default, IcebergCatalogConfig,
    IcebergReadBootstrap, IcebergReadMode, IcebergStorageConfig, MIB,
};

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
    /// Maximum files exposed by scan planning.
    pub max_planned_files: usize,
    /// Maximum encoded manifest-list size.
    pub max_manifest_list_bytes: usize,
    /// Maximum encoded size of one manifest.
    pub max_manifest_bytes: usize,
    /// Maximum manifests referenced by one snapshot.
    pub max_manifests_per_snapshot: usize,
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

        let parsed = Self {
            catalog: IcebergCatalogConfig::from_config(config)?,
            storage: IcebergStorageConfig::from_config(config)?,
            read_mode,
            bootstrap: parse_or_default(config, "read.bootstrap")?,
            poll_interval,
            snapshot_id: parse_optional_alias(config, "start.snapshot.id", "snapshot.id")?,
            table_ref: config.get("table.ref").unwrap_or("main").trim().to_string(),
            select_columns: parse_comma_list(
                optional_alias(config, "projection", "select.columns"),
                "projection",
                1_024,
            )?,
            filter: optional_non_empty(config, "filter"),
            max_snapshots_per_poll: parse_nonzero(config, "read.max.snapshots.per.poll", 1_024)?,
            max_planned_files: parse_nonzero(config, "read.max.planned.files", 65_536)?,
            max_manifest_list_bytes: parse_nonzero(
                config,
                "read.max.manifest.list.bytes",
                64 * MIB,
            )?,
            max_manifest_bytes: parse_nonzero(config, "read.max.manifest.bytes", 64 * MIB)?,
            max_manifests_per_snapshot: parse_nonzero(
                config,
                "read.max.manifests.per.snapshot",
                65_536,
            )?,
            scan_channel_capacity: parse_nonzero(config, "read.channel.capacity", 2)?,
            scan_concurrency: parse_nonzero(config, "read.scan.concurrency", 4)?,
        };
        parsed.validate_read_limits()?;
        Ok(parsed)
    }

    /// Validates programmatically constructed source bounds before external I/O.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when a resource or timeout bound is zero.
    pub fn validate_read_limits(&self) -> Result<(), ConnectorError> {
        for (name, value) in [
            ("read.max.snapshots.per.poll", self.max_snapshots_per_poll),
            ("read.max.planned.files", self.max_planned_files),
            ("read.max.manifest.list.bytes", self.max_manifest_list_bytes),
            ("read.max.manifest.bytes", self.max_manifest_bytes),
            (
                "read.max.manifests.per.snapshot",
                self.max_manifests_per_snapshot,
            ),
            ("read.channel.capacity", self.scan_channel_capacity),
            ("read.scan.concurrency", self.scan_concurrency),
        ] {
            if value == 0 {
                return Err(ConnectorError::ConfigurationError(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        for (name, value) in [
            ("poll.interval", self.poll_interval),
            ("catalog.connect_timeout", self.catalog.connect_timeout),
            ("catalog.request_timeout", self.catalog.request_timeout),
            ("storage.request_timeout", self.storage.request_timeout),
            ("storage.connect_timeout", self.storage.connect_timeout),
        ] {
            if value.is_zero() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        if self.table_ref.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "table.ref must not be empty".into(),
            ));
        }
        Ok(())
    }
}
