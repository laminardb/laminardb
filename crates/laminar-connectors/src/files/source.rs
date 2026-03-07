//! File source connector implementing [`SourceConnector`].
//!
//! Watches a directory or cloud path for new files, decodes them using the
//! configured format (CSV, JSON, text, Parquet), and produces `RecordBatch`es.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use tracing::{debug, info, warn};

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

use super::config::{FileFormat, FileSourceConfig};
use super::discovery::{DiscoveryConfig, FileDiscoveryEngine};
use super::manifest::{FileEntry, FileIngestionManifest};
use super::text_decoder::TextLineDecoder;

/// AutoLoader-style file source connector.
///
/// Watches a directory (local or cloud) for new files, infers schema if
/// needed, and produces `RecordBatch`es via `poll_batch()`.
pub struct FileSource {
    /// Parsed configuration.
    config: Option<FileSourceConfig>,
    /// Output Arrow schema (resolved in `open()`).
    schema: SchemaRef,
    /// Format decoder (created in `open()`).
    decoder: Option<Box<dyn FormatDecoder>>,
    /// File discovery engine (started in `open()`).
    discovery: Option<FileDiscoveryEngine>,
    /// File ingestion manifest (tracks processed files).
    manifest: FileIngestionManifest,
    /// Whether the connector is open.
    is_open: bool,
}

impl FileSource {
    /// Creates a new file source with a placeholder schema.
    #[must_use]
    pub fn new() -> Self {
        let empty_schema = Arc::new(Schema::empty());
        Self {
            config: None,
            schema: empty_schema,
            decoder: None,
            discovery: None,
            manifest: FileIngestionManifest::new(),
            is_open: false,
        }
    }
}

impl Default for FileSource {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for FileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSource")
            .field("is_open", &self.is_open)
            .field("schema_fields", &self.schema.fields().len())
            .field("manifest_count", &self.manifest.active_count())
            .finish()
    }
}

#[async_trait]
impl SourceConnector for FileSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let src_config = FileSourceConfig::from_connector_config(config)?;

        // Resolve format (explicit or auto-detect from path).
        let format = match src_config.format {
            Some(f) => f,
            None => FileFormat::from_extension(&src_config.path).ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "cannot detect format from path; specify 'format' explicitly".into(),
                )
            })?,
        };

        // Build decoder and resolve schema.
        let (decoder, schema) = build_decoder_and_schema(format, &src_config, config)?;

        // Optionally append _metadata struct column.
        let final_schema = if src_config.include_metadata {
            let mut fields: Vec<Field> =
                schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.push(Field::new(
                "_metadata",
                DataType::Struct(
                    vec![
                        Field::new("file_path", DataType::Utf8, false),
                        Field::new("file_name", DataType::Utf8, false),
                        Field::new("file_size", DataType::UInt64, false),
                        Field::new("file_modification_time", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            ));
            Arc::new(Schema::new(fields))
        } else {
            schema
        };

        // Start discovery engine with a snapshot of the current manifest for dedup.
        let discovery_config = DiscoveryConfig {
            path: src_config.path.clone(),
            poll_interval: src_config.poll_interval,
            stabilisation_delay: src_config.stabilisation_delay,
            glob_pattern: src_config.glob_pattern.clone(),
        };
        let known = Arc::new(self.manifest.snapshot_for_dedup());
        let discovery = FileDiscoveryEngine::start(discovery_config, known);

        self.config = Some(src_config);
        self.schema = final_schema;
        self.decoder = Some(decoder);
        self.discovery = Some(discovery);
        self.is_open = true;

        info!(
            "file source opened: format={format:?}, schema_fields={}",
            self.schema.fields().len()
        );
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "closed".into(),
            })?;
        let discovery = self
            .discovery
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "discovery running".into(),
                actual: "no discovery".into(),
            })?;
        let decoder = self
            .decoder
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "decoder ready".into(),
                actual: "no decoder".into(),
            })?;

        let files = discovery.drain(config.max_files_per_poll);
        if files.is_empty() {
            return Ok(None);
        }

        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for file in &files {
            // Size-change guard.
            if !config.allow_overwrites && self.manifest.size_changed(&file.path, file.size) {
                warn!(
                    "file source: skipping '{}' — size changed (was {}, now {})",
                    file.path,
                    self.manifest
                        .active_entries()
                        .find(|(p, _)| *p == file.path)
                        .map(|(_, e)| e.size)
                        .unwrap_or(0),
                    file.size
                );
                continue;
            }

            // Already ingested check (belt-and-suspenders with discovery dedup).
            if self.manifest.contains(&file.path) {
                continue;
            }

            // Max file size guard (primarily for Parquet).
            if file.size > config.max_file_bytes as u64 {
                warn!(
                    "file source: skipping '{}' — size {} exceeds max_file_bytes {}",
                    file.path, file.size, config.max_file_bytes
                );
                continue;
            }

            // Read file contents.
            let bytes = match read_file_bytes(&file.path).await {
                Ok(b) => b,
                Err(e) => {
                    warn!("file source: cannot read '{}': {e}", file.path);
                    continue;
                }
            };

            // Decode.
            let record = RawRecord::new(bytes);
            match decoder.decode_batch(&[record]) {
                Ok(batch) if batch.num_rows() > 0 => {
                    let batch = if config.include_metadata {
                        append_metadata_column(&batch, &file.path, file.size, file.modified_ms)?
                    } else {
                        batch
                    };
                    all_batches.push(batch);
                }
                Ok(_) => {
                    debug!("file source: empty batch from '{}'", file.path);
                }
                Err(e) => {
                    warn!("file source: decode error for '{}': {e}", file.path);
                    continue;
                }
            }

            // Record in manifest.
            self.manifest.insert(
                file.path.clone(),
                FileEntry {
                    size: file.size,
                    discovered_at: file.modified_ms,
                    ingested_at: now_millis(),
                },
            );
        }

        // Evict old manifest entries if needed.
        if let Some(cfg) = &self.config {
            let max_age_ms = cfg.manifest_retention_age_days * 24 * 60 * 60 * 1000;
            self.manifest
                .maybe_evict(cfg.manifest_retention_count, max_age_ms);
        }

        if all_batches.is_empty() {
            return Ok(None);
        }

        // Concatenate all batches.
        let combined = if all_batches.len() == 1 {
            all_batches.into_iter().next().unwrap()
        } else {
            arrow_select::concat::concat_batches(&self.schema, &all_batches)
                .map_err(|e| ConnectorError::ReadError(format!("batch concat error: {e}")))?
        };

        Ok(Some(SourceBatch::new(combined)))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(0);
        self.manifest.to_checkpoint(&mut cp);
        cp
    }

    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        match FileIngestionManifest::from_checkpoint(checkpoint) {
            Ok(manifest) => {
                info!(
                    "file source: restored manifest with {} active entries",
                    manifest.active_count()
                );
                self.manifest = manifest;
            }
            Err(e) => {
                warn!("file source: manifest restore failed: {e} — starting fresh");
                self.manifest = FileIngestionManifest::new();
            }
        }
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        if self.is_open {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.discovery = None;
        self.decoder = None;
        self.is_open = false;
        info!("file source closed");
        Ok(())
    }

    fn supports_replay(&self) -> bool {
        true
    }

    fn as_schema_evolvable(&self) -> Option<&dyn crate::schema::traits::SchemaEvolvable> {
        // Delegate to DefaultSchemaEvolver when needed.
        None
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn build_decoder_and_schema(
    format: FileFormat,
    src_config: &FileSourceConfig,
    connector_config: &ConnectorConfig,
) -> Result<(Box<dyn FormatDecoder>, SchemaRef), ConnectorError> {
    match format {
        FileFormat::Csv => {
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let csv_config = crate::schema::CsvDecoderConfig {
                delimiter: src_config.csv_delimiter,
                has_header: src_config.csv_has_header,
                ..crate::schema::CsvDecoderConfig::default()
            };
            let decoder = crate::schema::CsvDecoder::with_config(schema.clone(), csv_config);
            Ok((Box::new(decoder), schema))
        }
        FileFormat::Json => {
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let json_config = crate::schema::JsonDecoderConfig::default();
            let decoder = crate::schema::JsonDecoder::with_config(schema.clone(), json_config);
            Ok((Box::new(decoder), schema))
        }
        FileFormat::Text => {
            let decoder = TextLineDecoder::new();
            let schema = decoder.output_schema();
            Ok((Box::new(decoder), schema))
        }
        FileFormat::Parquet => {
            // For Parquet, schema comes from the file footer (authoritative).
            // Use a placeholder schema; it will be refined on the first file read.
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let decoder = crate::schema::parquet::ParquetDecoder::new(schema.clone());
            Ok((Box::new(decoder), schema))
        }
    }
}

async fn read_file_bytes(path: &str) -> Result<Vec<u8>, ConnectorError> {
    // Cloud paths would use object_store; local paths use tokio::fs.
    if path.starts_with("s3://")
        || path.starts_with("gs://")
        || path.starts_with("az://")
        || path.starts_with("abfs://")
        || path.starts_with("abfss://")
    {
        Err(ConnectorError::ConfigurationError(
            "cloud file reading not yet implemented in poll_batch; use local paths".into(),
        ))
    } else {
        tokio::fs::read(path)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("cannot read file '{path}': {e}")))
    }
}

fn append_metadata_column(
    batch: &RecordBatch,
    file_path: &str,
    file_size: u64,
    modified_ms: u64,
) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::{ArrayRef, Int64Array, StringArray, StructArray, UInt64Array};

    let n = batch.num_rows();
    let file_name = std::path::Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(file_path);

    let path_array: ArrayRef = Arc::new(StringArray::from(vec![file_path; n]));
    let name_array: ArrayRef = Arc::new(StringArray::from(vec![file_name; n]));
    let size_array: ArrayRef = Arc::new(UInt64Array::from(vec![file_size; n]));
    #[allow(clippy::cast_possible_wrap)]
    let mod_array: ArrayRef = Arc::new(Int64Array::from(vec![modified_ms as i64; n]));

    let fields = vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_size", DataType::UInt64, false),
        Field::new("file_modification_time", DataType::Int64, true),
    ];
    let struct_array = StructArray::try_new(
        fields.into(),
        vec![path_array, name_array, size_array, mod_array],
        None,
    )
    .map_err(|e| ConnectorError::ReadError(format!("metadata struct error: {e}")))?;

    // Append struct column to batch.
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(struct_array));

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(
        "_metadata",
        DataType::Struct(
            vec![
                Field::new("file_path", DataType::Utf8, false),
                Field::new("file_name", DataType::Utf8, false),
                Field::new("file_size", DataType::UInt64, false),
                Field::new("file_modification_time", DataType::Int64, true),
            ]
            .into(),
        ),
        false,
    ));

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| ConnectorError::ReadError(format!("metadata append error: {e}")))
}

#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_source_default() {
        let source = FileSource::new();
        assert!(!source.is_open);
        assert_eq!(source.manifest.active_count(), 0);
    }

    #[tokio::test]
    async fn test_open_missing_path() {
        let mut source = FileSource::new();
        let config = ConnectorConfig::new("files");
        let result = source.open(&config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_with_text_format() {
        let mut source = FileSource::new();
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/tmp");
        config.set("format", "text");
        let result = source.open(&config).await;
        assert!(result.is_ok());
        assert!(source.is_open);
        assert_eq!(source.schema().field(0).name(), "line");
        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_poll_batch_when_not_open() {
        let mut source = FileSource::new();
        let result = source.poll_batch(100).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let mut source = FileSource::new();
        source.manifest.insert(
            "test.csv".into(),
            FileEntry {
                size: 100,
                discovered_at: 1000,
                ingested_at: 2000,
            },
        );
        let cp = source.checkpoint();
        assert!(cp.get_offset("manifest").is_some());
    }

    #[tokio::test]
    async fn test_restore_from_checkpoint() {
        let mut source = FileSource::new();

        // Build a checkpoint with manifest data.
        let mut cp = SourceCheckpoint::new(1);
        cp.set_offset(
            "manifest",
            r#"{"a.csv":{"size":100,"discovered_at":900,"ingested_at":1000}}"#,
        );

        source.restore(&cp).await.unwrap();
        assert_eq!(source.manifest.active_count(), 1);
        assert!(source.manifest.contains("a.csv"));
    }

    #[test]
    fn test_health_check() {
        let source = FileSource::new();
        assert!(matches!(source.health_check(), HealthStatus::Unknown));
    }

    #[test]
    fn test_supports_replay() {
        let source = FileSource::new();
        assert!(source.supports_replay());
    }
}
