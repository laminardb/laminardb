//! Configuration types for the file source and sink connectors.

use std::collections::HashMap;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::storage::{StorageConsumer, StorageLocation, StorageLocationError, StorageProvider};
use laminar_core::time::parse_duration_str;

/// Parsed configuration for [`super::source::FileSource`].
#[derive(Debug, Clone)]
pub struct FileSourceConfig {
    /// Local directory path or glob pattern.
    pub path: String,

    /// Data format (`csv`, `json`, `text`, `parquet`). `None` = auto-detect.
    pub format: Option<FileFormat>,

    /// Discovery polling interval.
    pub poll_interval: Duration,

    /// Wait after last modify event before considering a file complete.
    pub stabilisation_delay: Duration,

    /// Maximum files to process per `poll_batch` call.
    pub max_files_per_poll: usize,

    /// Whether to append a `_metadata` struct column.
    pub include_metadata: bool,

    /// Safety limit for reading a single file (bytes). Primarily for Parquet.
    pub max_file_bytes: usize,

    /// Additional glob pattern to filter discovered file names.
    pub glob_pattern: Option<String>,

    /// CSV-specific: field delimiter.
    pub csv_delimiter: u8,

    /// CSV-specific: whether the first row is a header.
    pub csv_has_header: bool,
}

impl FileSourceConfig {
    /// Parse from connector properties.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if required options are
    /// missing or values cannot be parsed.
    pub fn from_connector_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let props = config.properties();

        let path = props
            .get("path")
            .ok_or_else(|| ConnectorError::ConfigurationError("'path' is required".into()))
            .and_then(|path| local_files_path(path))?;

        let format = props
            .get("format")
            .map(|s| FileFormat::parse(s))
            .transpose()?;

        let poll_interval = parse_duration(props, "poll_interval", Duration::from_secs(10))?;
        let stabilisation_delay =
            parse_duration(props, "stabilisation_delay", Duration::from_secs(1))?;
        let max_files_per_poll = parse_usize(props, "max_files_per_poll", 100)?;
        if max_files_per_poll == 0 {
            return Err(ConnectorError::ConfigurationError(
                "'max_files_per_poll' must be greater than zero".into(),
            ));
        }
        let include_metadata = parse_bool(props, "include_metadata", false)?;
        for removed in [
            "allow_overwrites",
            "manifest_retention_count",
            "manifest_retention_age_days",
        ] {
            if props.contains_key(removed) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "file source option '{removed}' was removed; processed paths are immutable and retained in one exact checkpoint inventory"
                )));
            }
        }
        let max_file_bytes = parse_usize(props, "max_file_bytes", 256 * 1024 * 1024)?;
        if max_file_bytes == 0 {
            return Err(ConnectorError::ConfigurationError(
                "'max_file_bytes' must be greater than zero".into(),
            ));
        }
        let glob_pattern = props.get("glob_pattern").cloned();

        // Default delimiter: tab for tsv, comma otherwise.
        let is_tsv = props
            .get("format")
            .is_some_and(|f| f.eq_ignore_ascii_case("tsv"));
        let csv_delimiter = props
            .get("csv.delimiter")
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(if is_tsv { b'\t' } else { b',' });
        let csv_has_header = parse_bool(props, "csv.has_header", true)?;

        Ok(Self {
            path,
            format,
            poll_interval,
            stabilisation_delay,
            max_files_per_poll,
            include_metadata,
            max_file_bytes,
            glob_pattern,
            csv_delimiter,
            csv_has_header,
        })
    }
}

/// Parsed configuration for [`super::sink::FileSink`].
#[derive(Debug, Clone)]
pub struct FileSinkConfig {
    /// Output directory path.
    pub path: String,

    /// Output format.
    pub format: FileFormat,

    /// File name prefix for immutable output files.
    pub prefix: String,

    /// Maximum file size before rotation (row formats only).
    pub max_file_size: Option<usize>,

    /// Parquet compression codec.
    pub compression: String,

    /// Maximum batches to buffer between bulk-format flushes (default: 10,000).
    pub max_buffered_batches: usize,
}

impl FileSinkConfig {
    /// Parse from connector properties.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if required options are
    /// missing or values cannot be parsed.
    pub fn from_connector_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let props = config.properties();

        let path = props
            .get("path")
            .ok_or_else(|| ConnectorError::ConfigurationError("'path' is required".into()))
            .and_then(|path| local_files_path(path))?;

        let format = props
            .get("format")
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("'format' is required for file sink".into())
            })
            .and_then(|s| FileFormat::parse(s))?;

        if props.contains_key("mode") {
            return Err(ConnectorError::ConfigurationError(
                "file sink option 'mode' was removed; files are always published as immutable rolling files"
                    .into(),
            ));
        }

        let prefix = props
            .get("prefix")
            .cloned()
            .unwrap_or_else(|| "part".to_string());
        if prefix.is_empty()
            || prefix == "."
            || prefix == ".."
            || prefix.contains('/')
            || prefix.contains('\\')
        {
            return Err(ConnectorError::ConfigurationError(
                "file sink 'prefix' must be a non-empty file-name component".into(),
            ));
        }

        let max_file_size = props
            .get("max_file_size")
            .map(|s| {
                s.parse::<usize>().map_err(|e| {
                    ConnectorError::ConfigurationError(format!("invalid max_file_size: {e}"))
                })
            })
            .transpose()?;

        let compression = props
            .get("compression")
            .cloned()
            .unwrap_or_else(|| "snappy".to_string());

        if props.contains_key("max_epoch_batches") {
            return Err(ConnectorError::ConfigurationError(
                "file sink option 'max_epoch_batches' was replaced by 'max_buffered_batches'"
                    .into(),
            ));
        }
        let max_buffered_batches = props
            .get("max_buffered_batches")
            .map(|s| {
                s.parse::<usize>().map_err(|e| {
                    ConnectorError::ConfigurationError(format!("invalid max_buffered_batches: {e}"))
                })
            })
            .transpose()?
            .unwrap_or(10_000);
        if max_buffered_batches == 0 {
            return Err(ConnectorError::ConfigurationError(
                "max_buffered_batches must be > 0".into(),
            ));
        }

        Ok(Self {
            path,
            format,
            prefix,
            max_file_size,
            compression,
            max_buffered_batches,
        })
    }
}

pub(super) fn local_files_path(path: &str) -> Result<String, ConnectorError> {
    if path.trim().is_empty() {
        return Err(ConnectorError::ConfigurationError(
            "file connector 'path' must not be empty".into(),
        ));
    }
    if std::path::Path::new(path).is_absolute() || !path.contains("://") {
        return Ok(path.to_string());
    }
    let location = match StorageLocation::parse(path) {
        Ok(location) => location,
        Err(StorageLocationError::UnsupportedScheme(_)) => return Err(remote_files_error()),
        Err(error) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "invalid file connector path: {error}"
            )))
        }
    };
    if location.provider != StorageProvider::Local {
        return Err(remote_files_error());
    }
    let adapted = location
        .adapt(StorageConsumer::ObjectStore)
        .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
    let url = url::Url::parse(&adapted.url).map_err(|error| {
        ConnectorError::ConfigurationError(format!("invalid canonical file URL: {error}"))
    })?;
    url.to_file_path()
        .map_err(|()| {
            ConnectorError::ConfigurationError(
                "file connector URL must identify an absolute local path".into(),
            )
        })?
        .into_os_string()
        .into_string()
        .map_err(|_| {
            ConnectorError::ConfigurationError(
                "file connector path cannot be represented as UTF-8".into(),
            )
        })
}

fn remote_files_error() -> ConnectorError {
    ConnectorError::FeatureUnsupported(
        "remote Files connector backends are not enabled; use a local path or absolute file:// URL"
            .into(),
    )
}

/// Supported file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// Comma-separated values.
    Csv,
    /// Newline-delimited JSON.
    Json,
    /// Plain text (one line per record).
    Text,
    /// Apache Parquet.
    Parquet,
    /// Arrow IPC file format (random-access `.arrow` files).
    ArrowIpc,
}

impl FileFormat {
    /// Parse a format string.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` for unknown formats.
    pub fn parse(s: &str) -> Result<Self, ConnectorError> {
        match s.to_lowercase().as_str() {
            "csv" | "tsv" => Ok(Self::Csv),
            "json" | "jsonl" | "ndjson" | "json_lines" => Ok(Self::Json),
            "text" | "txt" | "plain" => Ok(Self::Text),
            "parquet" | "parq" => Ok(Self::Parquet),
            "arrow" | "ipc" | "arrow_ipc" => Ok(Self::ArrowIpc),
            other => Err(ConnectorError::ConfigurationError(format!(
                "unknown file format: '{other}' (expected csv, json, text, parquet, or arrow)"
            ))),
        }
    }

    /// Detect format from a file path extension.
    pub fn from_extension(path: &str) -> Option<Self> {
        let ext = path.rsplit('.').next()?.to_lowercase();
        match ext.as_str() {
            "csv" | "tsv" => Some(Self::Csv),
            "json" | "jsonl" | "ndjson" => Some(Self::Json),
            "txt" | "log" => Some(Self::Text),
            "parquet" | "parq" => Some(Self::Parquet),
            "arrow" | "ipc" => Some(Self::ArrowIpc),
            _ => None,
        }
    }

    /// Returns the canonical file extension for this format.
    #[must_use]
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "jsonl",
            Self::Text => "txt",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow",
        }
    }

    /// Whether this is a columnar/bulk format (cannot be truncated mid-file).
    #[must_use]
    pub fn is_bulk_format(&self) -> bool {
        matches!(self, Self::Parquet | Self::ArrowIpc)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn parse_duration(
    props: &HashMap<String, String>,
    key: &str,
    default: Duration,
) -> Result<Duration, ConnectorError> {
    match props.get(key) {
        Some(s) => parse_duration_str(s).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("invalid duration for {key}: '{s}'"))
        }),
        None => Ok(default),
    }
}

fn parse_usize(
    props: &HashMap<String, String>,
    key: &str,
    default: usize,
) -> Result<usize, ConnectorError> {
    match props.get(key) {
        Some(s) => s
            .parse()
            .map_err(|e| ConnectorError::ConfigurationError(format!("invalid {key}: {e}"))),
        None => Ok(default),
    }
}

fn parse_bool(
    props: &HashMap<String, String>,
    key: &str,
    default: bool,
) -> Result<bool, ConnectorError> {
    match props.get(key) {
        Some(s) => match s.to_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            other => Err(ConnectorError::ConfigurationError(format!(
                "invalid boolean for {key}: '{other}'"
            ))),
        },
        None => Ok(default),
    }
}
#[cfg(test)]
mod tests;
