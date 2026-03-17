//! Configuration types for the file source and sink connectors.

use std::collections::HashMap;
use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

/// Parsed configuration for [`super::source::FileSource`].
#[derive(Debug, Clone)]
pub struct FileSourceConfig {
    /// Directory path, glob pattern, or cloud URL.
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

    /// Whether to allow re-ingesting files whose size has changed.
    pub allow_overwrites: bool,

    /// Maximum active entries in the file ingestion manifest.
    pub manifest_retention_count: usize,

    /// Maximum age (days) for manifest entries before eviction.
    pub manifest_retention_age_days: u64,

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
            .cloned()
            .ok_or_else(|| ConnectorError::ConfigurationError("'path' is required".into()))?;

        let format = props
            .get("format")
            .map(|s| FileFormat::parse(s))
            .transpose()?;

        let poll_interval = parse_duration(props, "poll_interval", Duration::from_secs(10))?;
        let stabilisation_delay =
            parse_duration(props, "stabilisation_delay", Duration::from_secs(1))?;
        let max_files_per_poll = parse_usize(props, "max_files_per_poll", 100)?;
        let include_metadata = parse_bool(props, "include_metadata", false)?;
        let allow_overwrites = parse_bool(props, "allow_overwrites", false)?;
        let manifest_retention_count = parse_usize(props, "manifest_retention_count", 100_000)?;
        let manifest_retention_age_days = parse_u64(props, "manifest_retention_age_days", 90)?;
        let max_file_bytes = parse_usize(props, "max_file_bytes", 256 * 1024 * 1024)?;
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
            allow_overwrites,
            manifest_retention_count,
            manifest_retention_age_days,
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

    /// Write mode.
    pub mode: SinkMode,

    /// File name prefix for rolling mode.
    pub prefix: String,

    /// Maximum file size before rotation (row formats only).
    pub max_file_size: Option<usize>,

    /// Parquet compression codec.
    pub compression: String,

    /// Maximum number of record batches to buffer per epoch for bulk formats
    /// (Parquet). Prevents OOM under burst load (default: 10,000).
    pub max_epoch_batches: usize,
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
            .cloned()
            .ok_or_else(|| ConnectorError::ConfigurationError("'path' is required".into()))?;

        let format = props
            .get("format")
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("'format' is required for file sink".into())
            })
            .and_then(|s| FileFormat::parse(s))?;

        let mode = match props.get("mode").map(String::as_str) {
            Some("append") => SinkMode::Append,
            Some("rolling") | None => SinkMode::Rolling,
            Some(other) => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "unknown sink mode: '{other}' (expected 'append' or 'rolling')"
                )));
            }
        };

        let prefix = props
            .get("prefix")
            .cloned()
            .unwrap_or_else(|| "part".to_string());

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

        let max_epoch_batches = props
            .get("max_epoch_batches")
            .map(|s| {
                s.parse::<usize>().map_err(|e| {
                    ConnectorError::ConfigurationError(format!("invalid max_epoch_batches: {e}"))
                })
            })
            .transpose()?
            .unwrap_or(10_000);

        Ok(Self {
            path,
            format,
            mode,
            prefix,
            max_file_size,
            compression,
            max_epoch_batches,
        })
    }
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
            other => Err(ConnectorError::ConfigurationError(format!(
                "unknown file format: '{other}' (expected csv, json, text, or parquet)"
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
        }
    }

    /// Whether this is a columnar/bulk format (cannot be truncated mid-file).
    #[must_use]
    pub fn is_bulk_format(&self) -> bool {
        matches!(self, Self::Parquet)
    }
}

/// Sink write mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
    /// Append to a single file.
    Append,
    /// Create a new file per epoch (with optional mid-epoch rotation for row formats).
    Rolling,
}

// ── Helpers ──────────────────────────────────────────────────────────

fn parse_duration(
    props: &HashMap<String, String>,
    key: &str,
    default: Duration,
) -> Result<Duration, ConnectorError> {
    match props.get(key) {
        Some(s) => parse_duration_str(s),
        None => Ok(default),
    }
}

fn parse_duration_str(s: &str) -> Result<Duration, ConnectorError> {
    // Accept plain seconds or suffixed values (e.g. "10s", "5000ms").
    if let Some(ms) = s.strip_suffix("ms") {
        let n: u64 = ms
            .parse()
            .map_err(|e| ConnectorError::ConfigurationError(format!("invalid duration: {e}")))?;
        return Ok(Duration::from_millis(n));
    }
    let secs_str = s.strip_suffix('s').unwrap_or(s);
    let n: u64 = secs_str
        .parse()
        .map_err(|e| ConnectorError::ConfigurationError(format!("invalid duration: {e}")))?;
    Ok(Duration::from_secs(n))
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

fn parse_u64(
    props: &HashMap<String, String>,
    key: &str,
    default: u64,
) -> Result<u64, ConnectorError> {
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
mod tests {
    use super::*;

    #[test]
    fn test_file_format_parse() {
        assert_eq!(FileFormat::parse("csv").unwrap(), FileFormat::Csv);
        assert_eq!(FileFormat::parse("JSON").unwrap(), FileFormat::Json);
        assert_eq!(FileFormat::parse("jsonl").unwrap(), FileFormat::Json);
        assert_eq!(FileFormat::parse("ndjson").unwrap(), FileFormat::Json);
        assert_eq!(FileFormat::parse("text").unwrap(), FileFormat::Text);
        assert_eq!(FileFormat::parse("parquet").unwrap(), FileFormat::Parquet);
        assert_eq!(FileFormat::parse("parq").unwrap(), FileFormat::Parquet);
        assert!(FileFormat::parse("xml").is_err());
    }

    #[test]
    fn test_file_format_from_extension() {
        assert_eq!(
            FileFormat::from_extension("/data/logs/app.csv"),
            Some(FileFormat::Csv)
        );
        assert_eq!(
            FileFormat::from_extension("events.jsonl"),
            Some(FileFormat::Json)
        );
        assert_eq!(
            FileFormat::from_extension("data.parquet"),
            Some(FileFormat::Parquet)
        );
        assert_eq!(
            FileFormat::from_extension("log.txt"),
            Some(FileFormat::Text)
        );
        assert_eq!(FileFormat::from_extension("file.bin"), None);
    }

    #[test]
    fn test_file_format_extension() {
        assert_eq!(FileFormat::Csv.extension(), "csv");
        assert_eq!(FileFormat::Json.extension(), "jsonl");
        assert_eq!(FileFormat::Text.extension(), "txt");
        assert_eq!(FileFormat::Parquet.extension(), "parquet");
    }

    #[test]
    fn test_file_format_is_bulk() {
        assert!(!FileFormat::Csv.is_bulk_format());
        assert!(!FileFormat::Json.is_bulk_format());
        assert!(!FileFormat::Text.is_bulk_format());
        assert!(FileFormat::Parquet.is_bulk_format());
    }

    #[test]
    fn test_source_config_from_connector() {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/data/logs/*.csv");
        config.set("format", "csv");
        config.set("max_files_per_poll", "50");
        config.set("include_metadata", "true");

        let src = FileSourceConfig::from_connector_config(&config).unwrap();
        assert_eq!(src.path, "/data/logs/*.csv");
        assert_eq!(src.format, Some(FileFormat::Csv));
        assert_eq!(src.max_files_per_poll, 50);
        assert!(src.include_metadata);
        assert!(!src.allow_overwrites);
    }

    #[test]
    fn test_source_config_missing_path() {
        let config = ConnectorConfig::new("files");
        assert!(FileSourceConfig::from_connector_config(&config).is_err());
    }

    #[test]
    fn test_sink_config_from_connector() {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/output");
        config.set("format", "parquet");
        config.set("mode", "rolling");
        config.set("compression", "zstd");

        let sink = FileSinkConfig::from_connector_config(&config).unwrap();
        assert_eq!(sink.path, "/output");
        assert_eq!(sink.format, FileFormat::Parquet);
        assert_eq!(sink.mode, SinkMode::Rolling);
        assert_eq!(sink.compression, "zstd");
    }

    #[test]
    fn test_sink_config_missing_format() {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/output");
        assert!(FileSinkConfig::from_connector_config(&config).is_err());
    }

    #[test]
    fn test_parse_duration_str() {
        assert_eq!(parse_duration_str("10").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration_str("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(
            parse_duration_str("500ms").unwrap(),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_sink_mode_default() {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/output");
        config.set("format", "csv");
        let sink = FileSinkConfig::from_connector_config(&config).unwrap();
        assert_eq!(sink.mode, SinkMode::Rolling);
    }
}
