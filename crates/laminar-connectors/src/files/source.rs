//! File source connector implementing [`SourceConnector`].
//!
//! Watches a local directory for new files, decodes them using the
//! configured format (CSV, JSON, text, Parquet), and produces `RecordBatch`es.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use tracing::{debug, info};

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorTaskGuard, ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector,
    SourceConsistency, SourceContract, SourceInputMode, SourcePosition, SourceStart,
    SourceTopology,
};
use crate::error::ConnectorError;
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

use super::config::{FileFormat, FileSourceConfig};
use super::discovery::{DiscoveredFile, DiscoveryConfig, FileDiscoveryEngine};
use super::manifest::FileIngestionManifest;
use super::text_decoder::TextLineDecoder;

const DISCOVERY_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const FILE_CHECKPOINT_CONNECTOR: &str = "files";
const CHECKPOINT_VERSION_METADATA: &str = "checkpoint.version";
const FILE_CHECKPOINT_VERSION: &str = "1";

fn validate_file_checkpoint(checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
    match checkpoint.get_metadata("connector") {
        Some(FILE_CHECKPOINT_CONNECTOR) => {}
        Some(connector) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "file checkpoint belongs to connector '{connector}'"
            )));
        }
        None => {
            return Err(ConnectorError::ConfigurationError(
                "file checkpoint is missing connector identity".into(),
            ));
        }
    }
    if checkpoint.get_metadata(CHECKPOINT_VERSION_METADATA) != Some(FILE_CHECKPOINT_VERSION) {
        return Err(ConnectorError::ConfigurationError(format!(
            "file checkpoint requires {CHECKPOINT_VERSION_METADATA}={FILE_CHECKPOINT_VERSION}"
        )));
    }
    Ok(())
}

#[async_trait]
trait FileReader: Send + Sync {
    async fn read(
        &self,
        path: &str,
        task_guard: ConnectorTaskGuard,
    ) -> Result<Vec<u8>, ConnectorError>;
}

struct LocalFileReader;

#[async_trait]
impl FileReader for LocalFileReader {
    async fn read(
        &self,
        path: &str,
        task_guard: ConnectorTaskGuard,
    ) -> Result<Vec<u8>, ConnectorError> {
        read_file_bytes(path, task_guard).await
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileProgress {
    path: String,
    size: u64,
    modified_ms: u64,
    content_sha256: String,
    next_row: u64,
}

#[derive(Debug, Clone)]
struct PendingFile {
    discovered: DiscoveredFile,
    resume: Option<FileProgress>,
}

#[derive(Debug)]
struct DecodedFile {
    discovered: DiscoveredFile,
    content_sha256: String,
    records: RecordBatch,
    next_row: usize,
}

/// AutoLoader-style file source connector.
///
/// Watches a local directory for new files, infers schema if
/// needed, and produces `RecordBatch`es via `poll_batch()`.
pub struct FileSource {
    /// Parsed configuration.
    config: Option<FileSourceConfig>,
    /// Output Arrow schema (resolved in `start()`).
    schema: SchemaRef,
    /// Format decoder (created in `start()`).
    decoder: Option<Box<dyn FormatDecoder>>,
    /// File discovery engine (started in `start()` after cursor validation).
    discovery: Option<FileDiscoveryEngine>,
    /// File ingestion manifest (tracks processed files).
    manifest: FileIngestionManifest,
    /// Discovered files staged in connector-owned memory. The front entry is
    /// retained across cancellation until it has been published or rejected.
    pending_files: VecDeque<PendingFile>,
    /// Decoded file being emitted in `max_records`-bounded slices.
    current_file: Option<DecodedFile>,
    /// File bytes provider. Local filesystem I/O in production; injectable in tests.
    reader: Arc<dyn FileReader>,
    /// Whether the connector has started.
    is_open: bool,
    /// A failed, timed-out, or cancelled shutdown permanently retires this instance.
    restart_forbidden: bool,
    /// Admission authority for background and blocking work owned by this generation.
    task_owner: ConnectorTaskOwner,
    /// Terminal observer handed to the runtime before this generation is dropped.
    task_tracker: ConnectorTaskTracker,
}

impl FileSource {
    /// Creates a new file source with a placeholder schema.
    #[must_use]
    pub fn new() -> Self {
        Self::with_registry(None)
    }

    /// Creates a new file source with an optional Prometheus registry.
    #[must_use]
    pub fn with_registry(_registry: Option<&prometheus::Registry>) -> Self {
        let empty_schema = Arc::new(Schema::empty());
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            config: None,
            schema: empty_schema,
            decoder: None,
            discovery: None,
            manifest: FileIngestionManifest::new(),
            pending_files: VecDeque::new(),
            current_file: None,
            reader: Arc::new(LocalFileReader),
            is_open: false,
            restart_forbidden: false,
            task_owner,
            task_tracker,
        }
    }

    async fn close_until(&mut self, deadline: tokio::time::Instant) -> Result<(), ConnectorError> {
        let Some(discovery) = self.discovery.as_mut() else {
            if self.is_open {
                self.restart_forbidden = true;
                return Err(ConnectorError::InvalidState {
                    expected: "running discovery task".into(),
                    actual: "open source without discovery task".into(),
                });
            }
            return Ok(());
        };

        // This latch deliberately flips before the await: cancellation of the
        // close future must not make the partially closed instance reusable.
        let was_restart_forbidden = self.restart_forbidden;
        self.restart_forbidden = true;
        discovery.abort_and_join_until(deadline).await?;

        self.discovery = None;
        self.decoder = None;
        self.pending_files.clear();
        self.current_file = None;
        self.is_open = false;
        self.restart_forbidden = was_restart_forbidden;
        info!("file source closed");
        Ok(())
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
            .field("restart_forbidden", &self.restart_forbidden)
            .field("schema_fields", &self.schema.fields().len())
            .field("manifest_count", &self.manifest.processed_count())
            .field("pending_files", &self.pending_files.len())
            .field("has_current_file", &self.current_file.is_some())
            .finish()
    }
}

#[async_trait]
impl SourceConnector for FileSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        // Replayability is backed by an exact processed-path inventory plus an
        // exact file/row cursor for a partially emitted file.
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if self.is_open || self.discovery.is_some() || self.restart_forbidden {
            return Err(ConnectorError::InvalidState {
                expected: "new or cleanly closed file source".into(),
                actual: if self.restart_forbidden {
                    "retired file source generation".into()
                } else {
                    "file source already open".into()
                },
            });
        }

        let (config, position, _) = request.into_parts();
        let src_config = FileSourceConfig::from_connector_config(&config)?;

        // Decode and validate the durable manifest before discovery can observe a
        // single path. A corrupt engine checkpoint is fatal: starting from an empty
        // manifest would rediscover and duplicate every previously ingested file.
        let (manifest, progress) = match position {
            SourcePosition::Initial => (FileIngestionManifest::new(), None),
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => {
                validate_file_checkpoint(&checkpoint)?;
                if checkpoint.get_offset("manifest").is_none() {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "file checkpoint {attempt:?} is missing required manifest state"
                    )));
                }
                let manifest =
                    FileIngestionManifest::from_checkpoint(&checkpoint).map_err(|e| {
                        ConnectorError::ConfigurationError(format!(
                            "invalid file manifest in checkpoint {attempt:?}: {e}"
                        ))
                    })?;
                let progress = checkpoint
                    .get_offset("file_progress")
                    .map(serde_json::from_str::<FileProgress>)
                    .transpose()
                    .map_err(|e| {
                        ConnectorError::ConfigurationError(format!(
                            "invalid file progress in checkpoint {attempt:?}: {e}"
                        ))
                    })?;
                if progress
                    .as_ref()
                    .is_some_and(|p| manifest.contains(&p.path) || p.next_row == 0)
                {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "file checkpoint {attempt:?} contains contradictory progress"
                    )));
                }
                (manifest, progress)
            }
        };

        // The file source is local-only; reject unsupported paths before
        // discovery or reads can start.
        if is_cloud_url(&src_config.path) {
            return Err(ConnectorError::ConfigurationError(format!(
                "cloud paths are not supported by the 'files' source: {}",
                src_config.path
            )));
        }

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
        let (decoder, schema) = build_decoder_and_schema(format, &src_config, &config)?;

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
                        Field::new(
                            "file_modification_time",
                            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                            true,
                        ),
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
        let known = Arc::new(manifest.snapshot_for_dedup());
        let discovery_guard = self
            .task_owner
            .track()
            .expect("live file source cannot have a retired task owner");
        let initial_scan_guard = self
            .task_owner
            .track()
            .expect("live file source cannot have a retired task owner");
        let discovery = FileDiscoveryEngine::start(
            discovery_config,
            known,
            discovery_guard,
            initial_scan_guard,
        );

        self.config = Some(src_config);
        self.schema = final_schema;
        self.decoder = Some(decoder);
        self.discovery = Some(discovery);
        self.manifest = manifest;
        self.pending_files.clear();
        self.current_file = None;
        if let Some(progress) = progress {
            self.pending_files.push_back(PendingFile {
                discovered: DiscoveredFile {
                    path: progress.path.clone(),
                    size: progress.size,
                    modified_ms: progress.modified_ms,
                },
                resume: Some(progress),
            });
        }
        self.is_open = true;

        info!(
            "file source opened: format={format:?}, schema_fields={}",
            self.schema.fields().len()
        );
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if max_records == 0 {
            return Err(ConnectorError::ConfigurationError(
                "file source poll max_records must be greater than zero".into(),
            ));
        }

        let config = self
            .config
            .clone()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "started".into(),
                actual: "closed".into(),
            })?;
        if self.decoder.is_none() {
            return Err(ConnectorError::InvalidState {
                expected: "decoder ready".into(),
                actual: "no decoder".into(),
            });
        }

        loop {
            if let Some(mut decoded) = self.current_file.take() {
                let rows_left = decoded.records.num_rows() - decoded.next_row;
                let row_count = rows_left.min(max_records);
                let result = decoded.records.slice(decoded.next_row, row_count);
                decoded.next_row += row_count;

                if decoded.next_row == decoded.records.num_rows() {
                    self.manifest.insert(decoded.discovered.path);
                } else {
                    self.current_file = Some(decoded);
                }

                // No await is permitted between advancing the row/file cursor
                // above and returning the corresponding zero-copy slice.
                return Ok(Some(SourceBatch::new(result)));
            }

            if self.pending_files.is_empty() {
                let discovery =
                    self.discovery
                        .as_mut()
                        .ok_or_else(|| ConnectorError::InvalidState {
                            expected: "discovery running".into(),
                            actual: "no discovery".into(),
                        })?;
                let discovered = match discovery.drain(config.max_files_per_poll).await {
                    Ok(discovered) => discovered,
                    Err(error) => {
                        self.restart_forbidden = true;
                        return Err(error);
                    }
                };
                self.pending_files
                    .extend(discovered.into_iter().map(|discovered| PendingFile {
                        discovered,
                        resume: None,
                    }));
            }

            let Some(pending) = self.pending_files.front().cloned() else {
                return Ok(None);
            };

            if self.manifest.contains(&pending.discovered.path) {
                self.pending_files.pop_front();
                continue;
            }

            if pending.discovered.size > config.max_file_bytes as u64 {
                return Err(ConnectorError::ConfigurationError(format!(
                    "file '{}' size {} exceeds max_file_bytes {}",
                    pending.discovered.path, pending.discovered.size, config.max_file_bytes
                )));
            }

            let read_guard = self
                .task_owner
                .track()
                .expect("live file source cannot have a retired task owner");
            let bytes = Arc::clone(&self.reader)
                .read(&pending.discovered.path, read_guard)
                .await?;
            if bytes.len() > config.max_file_bytes {
                return Err(ConnectorError::ConfigurationError(format!(
                    "file '{}' actual size {} exceeds max_file_bytes {}",
                    pending.discovered.path,
                    bytes.len(),
                    config.max_file_bytes
                )));
            }
            if u64::try_from(bytes.len()).ok() != Some(pending.discovered.size) {
                return Err(ConnectorError::ReadError(format!(
                    "file '{}' changed size after discovery (expected {}, read {})",
                    pending.discovered.path,
                    pending.discovered.size,
                    bytes.len()
                )));
            }

            let content_sha256 = sha256_hex(&bytes);
            let next_row = if let Some(progress) = &pending.resume {
                if progress.content_sha256 != content_sha256 {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "file '{}' changed content since its checkpointed partial read",
                        pending.discovered.path
                    )));
                }
                usize::try_from(progress.next_row).map_err(|_| {
                    ConnectorError::ConfigurationError(format!(
                        "file '{}' checkpoint row {} exceeds this runtime's address space",
                        pending.discovered.path, progress.next_row
                    ))
                })?
            } else {
                0
            };

            let mut records = self
                .decoder
                .as_ref()
                .expect("decoder checked above")
                .decode_batch(&[RawRecord::new(bytes)])
                .map_err(ConnectorError::from)?;
            if config.include_metadata {
                records = append_metadata_column(
                    &records,
                    &pending.discovered.path,
                    pending.discovered.size,
                    pending.discovered.modified_ms,
                )?;
            }
            if pending.resume.is_some() && next_row >= records.num_rows() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "file '{}' checkpoint row {} is outside decoded row count {}",
                    pending.discovered.path,
                    next_row,
                    records.num_rows()
                )));
            }

            self.pending_files.pop_front();
            if records.num_rows() == 0 {
                debug!(
                    "file source: empty batch from '{}'",
                    pending.discovered.path
                );
                self.manifest.insert(pending.discovered.path);
                return Ok(None);
            }
            self.current_file = Some(DecodedFile {
                discovered: pending.discovered,
                content_sha256,
                records,
                next_row,
            });
            // Loop once without awaiting to publish the first bounded slice.
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new();
        cp.set_metadata("connector", FILE_CHECKPOINT_CONNECTOR);
        cp.set_metadata(CHECKPOINT_VERSION_METADATA, FILE_CHECKPOINT_VERSION);
        self.manifest.to_checkpoint(&mut cp);
        if let Some(file) = &self.current_file {
            let progress = FileProgress {
                path: file.discovered.path.clone(),
                size: file.discovered.size,
                modified_ms: file.discovered.modified_ms,
                content_sha256: file.content_sha256.clone(),
                next_row: u64::try_from(file.next_row)
                    .expect("Arrow record counts are representable as u64"),
            };
            cp.set_offset(
                "file_progress",
                serde_json::to_string(&progress)
                    .expect("file progress contains only infallibly serializable fields"),
            );
        }
        cp
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_until(tokio::time::Instant::now() + DISCOVERY_CLOSE_TIMEOUT)
            .await
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
        FileFormat::ArrowIpc => {
            // Arrow IPC files embed their schema in the header — use it as
            // authoritative. The DDL schema is used as placeholder until
            // the first file is read.
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let decoder = super::arrow_ipc_codec::ArrowIpcDecoder::new(schema.clone());
            Ok((Box::new(decoder), schema))
        }
    }
}

fn is_cloud_url(path: &str) -> bool {
    const CLOUD_SCHEMES: &[&str] = &["s3", "s3a", "s3n", "gs", "gcs", "az", "abfs", "abfss"];
    let Some((scheme, _)) = path.split_once("://") else {
        return false;
    };
    CLOUD_SCHEMES.iter().any(|s| scheme.eq_ignore_ascii_case(s))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    format!("{:x}", Sha256::digest(bytes))
}

async fn read_file_bytes(
    path: &str,
    task_guard: ConnectorTaskGuard,
) -> Result<Vec<u8>, ConnectorError> {
    // Cloud paths are rejected at `start()`; this path is local-only.
    debug_assert!(
        !is_cloud_url(path),
        "cloud paths must be rejected at start()"
    );
    let read_path = path.to_owned();
    let error_path = read_path.clone();
    tokio::task::spawn_blocking(move || {
        let _task_guard = task_guard;
        std::fs::read(read_path)
    })
    .await
    .map_err(|e| ConnectorError::ReadError(format!("file read worker failed: {e}")))?
    .map_err(|e| ConnectorError::ReadError(format!("cannot read file '{error_path}': {e}")))
}

fn append_metadata_column(
    batch: &RecordBatch,
    file_path: &str,
    file_size: u64,
    modified_ms: u64,
) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::{ArrayRef, StringArray, StructArray, UInt64Array};

    let n = batch.num_rows();
    let file_name = std::path::Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(file_path);

    let path_array: ArrayRef = Arc::new(StringArray::from(vec![file_path; n]));
    let name_array: ArrayRef = Arc::new(StringArray::from(vec![file_name; n]));
    let size_array: ArrayRef = Arc::new(UInt64Array::from(vec![file_size; n]));
    #[allow(clippy::cast_possible_wrap)]
    let mod_array: ArrayRef = Arc::new(arrow_array::TimestampMillisecondArray::from(vec![
        modified_ms as i64;
        n
    ]));

    let fields = vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_size", DataType::UInt64, false),
        Field::new(
            "file_modification_time",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            true,
        ),
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
                Field::new(
                    "file_modification_time",
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                    true,
                ),
            ]
            .into(),
        ),
        false,
    ));

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| ConnectorError::ReadError(format!("metadata append error: {e}")))
}

#[cfg(test)]
mod tests;
