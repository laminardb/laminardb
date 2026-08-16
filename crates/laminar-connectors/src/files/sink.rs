//! File sink connector implementing [`SinkConnector`].
//!
//! Every flush publishes immutable files by atomically renaming same-directory
//! temporary files. Publication generations are independent of checkpoint
//! epochs so periodic flushes and process restarts cannot reuse final names.
//! This is durable at-least-once output, not an atomic external checkpoint
//! commit protocol.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use laminar_core::durable_fs::{durable_rename, DurableRenameMode};
use tracing::{debug, info};

use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::schema::traits::FormatEncoder;

use super::config::{FileFormat, FileSinkConfig};

enum FileBlockingTaskError {
    Retired,
    Worker(tokio::task::JoinError),
}

fn blocking_task_error(operation: &'static str, error: FileBlockingTaskError) -> ConnectorError {
    match error {
        FileBlockingTaskError::Retired => ConnectorError::InvalidState {
            expected: "active file connector generation".into(),
            actual: "retired".into(),
        },
        FileBlockingTaskError::Worker(error) => {
            ConnectorError::Internal(format!("{operation}: {error}"))
        }
    }
}

fn ambiguous_blocking_task_error(
    operation: &'static str,
    error: FileBlockingTaskError,
) -> ConnectorError {
    match error {
        FileBlockingTaskError::Retired => ConnectorError::InvalidState {
            expected: "active file connector generation".into(),
            actual: "retired".into(),
        },
        FileBlockingTaskError::Worker(error) => ConnectorError::outcome_unknown(
            format!("{operation}: the worker ended after filesystem dispatch: {error}"),
            error.is_cancelled(),
        ),
    }
}

enum FilePublicationError {
    BeforeDispatch(ConnectorError),
    AfterDispatch {
        published: usize,
        error: ConnectorError,
    },
}

/// File sink connector that publishes immutable rolling files.
pub struct FileSink {
    /// Parsed configuration.
    config: Option<FileSinkConfig>,
    /// Output schema.
    schema: SchemaRef,
    /// Format encoder.
    encoder: Option<Box<dyn FormatEncoder>>,
    /// Generation reserved for the next publication.
    next_generation: u64,
    /// Buffered batches awaiting a bulk-format file flush.
    buffered_batches: Vec<RecordBatch>,
    /// Current segment index within the publication generation.
    current_segment: usize,
    /// Bytes written in current segment.
    segment_bytes: u64,
    /// Buffered file writer for the current segment (row formats only).
    writer: Option<Arc<Mutex<BufWriter<std::fs::File>>>>,
    /// Active `.tmp` file paths in the current publication generation.
    active_tmp_files: Vec<PathBuf>,
    /// Whether the sink is open.
    is_open: bool,
    /// Prevents admitted but not-yet-running blocking work from starting after retirement.
    retired: Arc<AtomicBool>,
    /// Admission authority for blocking work owned by this generation.
    task_owner: ConnectorTaskOwner,
    /// Terminal observer handed to the runtime before this generation is dropped.
    task_tracker: ConnectorTaskTracker,
}

impl FileSink {
    /// Creates a new file sink with a placeholder schema.
    #[must_use]
    pub fn new() -> Self {
        Self::with_registry(None)
    }

    /// Creates a new file sink with an optional Prometheus registry.
    #[must_use]
    pub fn with_registry(_registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            config: None,
            schema: Arc::new(arrow_schema::Schema::empty()),
            encoder: None,
            next_generation: 1,
            buffered_batches: Vec::new(),
            current_segment: 0,
            segment_bytes: 0,
            writer: None,
            active_tmp_files: Vec::new(),
            is_open: false,
            retired: Arc::new(AtomicBool::new(false)),
            task_owner,
            task_tracker,
        }
    }

    async fn run_blocking<T, F>(&self, operation: F) -> Result<T, FileBlockingTaskError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        if self.retired.load(Ordering::Acquire) {
            return Err(FileBlockingTaskError::Retired);
        }
        let guard = self
            .task_owner
            .track()
            .expect("live file sink cannot have a retired task owner");
        let retired = Arc::clone(&self.retired);
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            if retired.load(Ordering::Acquire) {
                Err(FileBlockingTaskError::Retired)
            } else {
                Ok(operation())
            }
        })
        .await
        .map_err(FileBlockingTaskError::Worker)?
    }

    fn ensure_open(&self) -> Result<(), ConnectorError> {
        if self.is_open {
            Ok(())
        } else {
            Err(ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "closed".into(),
            })
        }
    }

    /// Opens a new same-directory temporary segment without ever truncating an
    /// existing path. The filesystem call stays off the async runtime.
    async fn open_segment_async(&mut self) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "configured".into(),
                actual: "unconfigured".into(),
            })?;
        let filename = format!(
            "{}_{:06}_{:03}.{}.tmp",
            config.prefix,
            self.next_generation,
            self.current_segment,
            config.format.extension()
        );
        let path = Path::new(&config.path).join(filename);
        let open_path = path.clone();
        // Register the path before awaiting. If the caller cancels while the
        // blocking open completes, a later retry still owns the file.
        self.active_tmp_files.push(path.clone());
        let file = match self
            .run_blocking(move || {
                std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&open_path)
                    .map_err(|e| {
                        ConnectorError::WriteError(format!(
                            "cannot create temporary output '{}': {e}",
                            open_path.display()
                        ))
                    })
            })
            .await
            .map_err(|error| blocking_task_error("temporary-file open failed", error))?
        {
            Ok(file) => file,
            Err(error) => {
                self.active_tmp_files.pop();
                return Err(error);
            }
        };

        self.writer = Some(Arc::new(Mutex::new(BufWriter::new(file))));
        self.segment_bytes = 0;
        Ok(())
    }

    /// Ensures a writer is open for the current segment.
    async fn ensure_writer_async(&mut self) -> Result<(), ConnectorError> {
        if self.writer.is_none() {
            self.open_segment_async().await?;
        }
        Ok(())
    }

    /// Closes and data-syncs the current writer on the blocking thread pool.
    async fn close_writer_async(&mut self) -> Result<(), ConnectorError> {
        let Some(writer) = self.writer.clone() else {
            return Ok(());
        };
        self.run_blocking(move || -> Result<(), ConnectorError> {
            let mut w = writer
                .lock()
                .map_err(|_| ConnectorError::WriteError("file writer lock was poisoned".into()))?;
            w.flush()
                .map_err(|e| ConnectorError::WriteError(format!("flush error: {e}")))?;
            w.get_ref()
                .sync_all()
                .map_err(|e| ConnectorError::WriteError(format!("file sync error: {e}")))
        })
        .await
        .map_err(|error| ambiguous_blocking_task_error("writer flush failed", error))?
        .map_err(|error| {
            let retryable = error.is_transient();
            ConnectorError::outcome_unknown(
                format!("temporary-file flush or sync may have partially completed: {error}"),
                retryable,
            )
        })?;
        self.writer = None;
        Ok(())
    }

    fn advance_segment(&mut self) -> Result<(), ConnectorError> {
        self.current_segment = self.current_segment.checked_add(1).ok_or_else(|| {
            ConnectorError::outcome_unknown(
                "file data was staged but the segment counter is exhausted",
                false,
            )
        })?;
        self.segment_bytes = 0;
        Ok(())
    }

    fn finish_published_generation(&mut self) -> Result<(), ConnectorError> {
        self.next_generation = self.next_generation.checked_add(1).ok_or_else(|| {
            ConnectorError::outcome_unknown(
                "files were published but the generation counter is exhausted",
                false,
            )
        })?;
        self.current_segment = 0;
        self.segment_bytes = 0;
        Ok(())
    }

    /// Encodes buffered bulk-format batches and writes their self-contained
    /// file blob to the current temporary generation.
    async fn materialize_bulk_batches(&mut self) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        let is_bulk = self
            .config
            .as_ref()
            .is_some_and(|config| config.format.is_bulk_format());
        if !is_bulk || self.buffered_batches.is_empty() {
            return Ok(());
        }

        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "encoder ready".into(),
                actual: "no encoder".into(),
            })?;
        let batches = self.buffered_batches.clone();
        let schema = self.schema.clone();
        let (encoder, encoded) = self
            .run_blocking(move || {
                let combined = if batches.len() == 1 {
                    Ok(batches[0].clone())
                } else {
                    arrow_select::concat::concat_batches(&schema, &batches)
                        .map_err(|e| ConnectorError::WriteError(format!("batch concat error: {e}")))
                };
                let encoded = combined.and_then(|combined| {
                    encoder
                        .encode_batch(&combined)
                        .map_err(|e| ConnectorError::WriteError(format!("bulk encode error: {e}")))
                });
                (encoder, encoded)
            })
            .await
            .map_err(|error| blocking_task_error("bulk encoder worker failed", error))?;
        self.encoder = Some(encoder);
        let mut encoded = encoded?;
        if encoded.len() != 1 {
            return Err(ConnectorError::WriteError(format!(
                "bulk file encoder returned {} blobs; expected exactly one self-contained file",
                encoded.len()
            )));
        }

        let file_bytes = encoded.pop().expect("length checked above");
        self.open_segment_async().await?;
        let writer = self
            .writer
            .as_ref()
            .expect("open_segment_async just ran")
            .clone();
        self.run_blocking(move || -> Result<(), ConnectorError> {
            let mut writer = writer
                .lock()
                .map_err(|_| ConnectorError::WriteError("file writer lock was poisoned".into()))?;
            writer
                .write_all(&file_bytes)
                .map_err(|e| ConnectorError::WriteError(format!("write error: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|error| ambiguous_blocking_task_error("bulk file write failed", error))?
        .map_err(|error| {
            let retryable = error.is_transient();
            ConnectorError::outcome_unknown(
                format!("bulk temporary-file write may have partially completed: {error}"),
                retryable,
            )
        })?;
        self.buffered_batches.clear();
        Ok(())
    }

    /// Close and sync every pending temporary file before publication.
    async fn prepare_pending_files(&mut self) -> Result<(), ConnectorError> {
        self.materialize_bulk_batches().await?;
        self.close_writer_async().await?;

        let paths = self.active_tmp_files.clone();
        self.run_blocking(move || {
            for path in paths {
                let file = std::fs::OpenOptions::new()
                    // Windows requires write access for FlushFileBuffers.
                    .write(true)
                    .open(&path)
                    .map_err(|e| {
                        ConnectorError::WriteError(format!(
                            "cannot open '{}' for sync: {e}",
                            path.display()
                        ))
                    })?;
                file.sync_all().map_err(|e| {
                    ConnectorError::WriteError(format!(
                        "file sync failed on '{}': {e}",
                        path.display()
                    ))
                })?;
            }
            Ok::<(), ConnectorError>(())
        })
        .await
        .map_err(|error| blocking_task_error("temporary-file sync failed", error))??;
        Ok(())
    }

    /// Atomically publishes every prepared file. Each rename is atomic, while
    /// the set as a whole deliberately has at-least-once (not atomic) semantics.
    async fn publish_pending_files(&mut self) -> Result<(), ConnectorError> {
        if self.active_tmp_files.is_empty() {
            return Ok(());
        }
        if self.writer.is_some() || !self.buffered_batches.is_empty() {
            return Err(ConnectorError::InvalidState {
                expected: "prepared temporary files".into(),
                actual: "unflushed file data".into(),
            });
        }

        let paths = self.active_tmp_files.clone();
        let outcome = self
            .run_blocking(move || -> Result<usize, FilePublicationError> {
                let finals = paths
                    .iter()
                    .map(|path| {
                        let final_path = final_path_for_tmp(path)?;
                        match final_path.try_exists() {
                            Ok(false) => Ok(final_path),
                            Ok(true) => Err(ConnectorError::WriteError(format!(
                                "refusing to overwrite existing output '{}'",
                                final_path.display()
                            ))),
                            Err(e) => Err(ConnectorError::WriteError(format!(
                                "cannot inspect final output '{}': {e}",
                                final_path.display()
                            ))),
                        }
                    })
                    .collect::<Result<Vec<_>, ConnectorError>>()
                    .map_err(FilePublicationError::BeforeDispatch)?;

                let mut published = 0;
                for (tmp_path, final_path) in paths.iter().zip(&finals) {
                    if let Err(e) =
                        durable_rename(tmp_path, final_path, DurableRenameMode::NoReplace)
                    {
                        return Err(FilePublicationError::AfterDispatch {
                            published,
                            error: ConnectorError::WriteError(format!(
                                "cannot publish '{}' as '{}': {e}",
                                tmp_path.display(),
                                final_path.display()
                            )),
                        });
                    }
                    published += 1;
                    debug!(path = %final_path.display(), "file sink published immutable file");
                }
                Ok(published)
            })
            .await
            .map_err(|error| ambiguous_blocking_task_error("file publication failed", error))?;

        let published = match outcome {
            Ok(published) => published,
            Err(FilePublicationError::BeforeDispatch(error)) => return Err(error),
            Err(FilePublicationError::AfterDispatch { published, error }) => {
                let retryable = error.is_transient();
                return Err(ConnectorError::outcome_unknown(
                    format!(
                        "file publication failed after dispatch; {published} file(s) were confirmed published: {error}"
                    ),
                    retryable,
                ));
            }
        };
        if published != self.active_tmp_files.len() {
            return Err(ConnectorError::outcome_unknown(
                format!(
                    "file publication reported {published} completed file(s) for {} pending path(s)",
                    self.active_tmp_files.len()
                ),
                false,
            ));
        }
        self.active_tmp_files.clear();
        self.finish_published_generation()?;
        Ok(())
    }

    async fn flush_and_publish(&mut self) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        self.prepare_pending_files().await?;
        self.publish_pending_files().await
    }
}

impl Default for FileSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FileSink {
    fn drop(&mut self) {
        self.retired.store(true, Ordering::Release);
    }
}

impl std::fmt::Debug for FileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSink")
            .field("is_open", &self.is_open)
            .field("next_generation", &self.next_generation)
            .field("buffered_batches", &self.buffered_batches.len())
            .field("active_tmp_files", &self.active_tmp_files.len())
            .finish()
    }
}

#[async_trait]
impl SinkConnector for FileSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        FileSinkConfig::from_connector_config(config)?;
        if !cfg!(any(unix, windows)) {
            return Err(ConnectorError::ConfigurationError(
                "durable file publication is unsupported on this platform".into(),
            ));
        }
        // A checkpoint may fsync files, but this implementation has no
        // recoverable external commit cursor/namespace. Do not overclaim it as
        // checkpoint-committable.
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::Singleton,
            SinkInputMode::AppendOnly,
        ))
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if self.is_open {
            return Err(ConnectorError::InvalidState {
                expected: "closed".into(),
                actual: "already open".into(),
            });
        }
        let sink_config = FileSinkConfig::from_connector_config(config)?;

        // Validate the encoder before creating or cleaning any files.
        let schema = config
            .arrow_schema()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        let encoder = build_encoder(sink_config.format, &schema, &sink_config)?;

        let out_dir = PathBuf::from(&sink_config.path);
        let prefix = sink_config.prefix.clone();
        let extension = sink_config.format.extension().to_string();
        let next_generation = self
            .run_blocking(move || initialise_output_directory(&out_dir, &prefix, &extension))
            .await
            .map_err(|error| {
                blocking_task_error("output-directory initialisation failed", error)
            })??;

        self.config = Some(sink_config);
        self.schema = schema;
        self.encoder = Some(encoder);
        self.next_generation = next_generation;
        self.current_segment = 0;
        self.segment_bytes = 0;
        self.buffered_batches.clear();
        self.active_tmp_files.clear();
        self.writer = None;
        self.is_open = true;

        info!(next_generation, "file sink opened");
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.ensure_open()?;
        let is_bulk = self
            .config
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open".into(),
                actual: "closed".into(),
            })?
            .format
            .is_bulk_format();
        let max_file_size = self.config.as_ref().and_then(|c| c.max_file_size);

        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        let rows = batch.num_rows();

        if is_bulk {
            let max_buffered = self
                .config
                .as_ref()
                .map_or(10_000, |config| config.max_buffered_batches);
            if self.buffered_batches.len() >= max_buffered {
                return Err(ConnectorError::WriteError(format!(
                    "file sink: bulk batch buffer full ({max_buffered} batches) — \
                     increase max_buffered_batches or flush more frequently"
                )));
            }
            // Bulk encoders must create a complete self-contained file, so
            // retain batches until the next periodic/checkpoint/close flush.
            self.buffered_batches.push(batch.clone());
            return Ok(WriteResult::new(rows, 0));
        }

        // Row encoding and file writes can both be CPU- or filesystem-bound.
        // Keep them off the Tokio worker pool and owned by this generation.
        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "encoder ready".into(),
                actual: "no encoder".into(),
            })?;
        let batch = batch.clone();
        let (encoder, encoded) = self
            .run_blocking(move || {
                let encoded = encoder
                    .encode_batch(&batch)
                    .map_err(|e| ConnectorError::WriteError(format!("encode error: {e}")));
                (encoder, encoded)
            })
            .await
            .map_err(|error| blocking_task_error("row encoder worker failed", error))?;
        self.encoder = Some(encoder);
        let encoded = encoded?;
        validate_encoded_row_count(rows, encoded.len())?;

        self.ensure_writer_async().await?;
        let writer = self
            .writer
            .as_ref()
            .expect("ensure_writer_async just ran")
            .clone();
        let bytes_written = self
            .run_blocking(move || -> Result<_, ConnectorError> {
                let mut writer = writer.lock().map_err(|_| {
                    ConnectorError::WriteError("file writer lock was poisoned".into())
                })?;
                let mut total: u64 = 0;
                for record_bytes in &encoded {
                    writer
                        .write_all(record_bytes)
                        .map_err(|e| ConnectorError::WriteError(format!("write error: {e}")))?;
                    writer
                        .write_all(b"\n")
                        .map_err(|e| ConnectorError::WriteError(format!("write error: {e}")))?;
                    total += record_bytes.len() as u64 + 1;
                }
                Ok(total)
            })
            .await
            .map_err(|error| ambiguous_blocking_task_error("row file write failed", error))?
            .map_err(|error| {
                let retryable = error.is_transient();
                ConnectorError::outcome_unknown(
                    format!("row temporary-file write may have partially completed: {error}"),
                    retryable,
                )
            })?;
        self.segment_bytes = self
            .segment_bytes
            .checked_add(bytes_written)
            .ok_or_else(|| {
                ConnectorError::outcome_unknown(
                    "file data was staged but the byte counter is exhausted",
                    false,
                )
            })?;

        // Size-based rotation within the current publication generation.
        if let Some(max_size) = max_file_size {
            if self.segment_bytes >= max_size as u64 {
                debug!("file sink: rotating at {} bytes", self.segment_bytes);
                self.close_writer_async().await?;
                self.advance_segment()?;
            }
        }

        Ok(WriteResult::new(rows, bytes_written))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flush_and_publish().await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if !self.is_open {
            return Ok(());
        }
        self.flush_and_publish().await?;
        self.is_open = false;
        self.config = None;
        self.encoder = None;
        info!("file sink closed");
        Ok(())
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn validate_encoded_row_count(expected: usize, actual: usize) -> Result<(), ConnectorError> {
    if actual == expected {
        return Ok(());
    }
    Err(crate::error::SerdeError::RecordCountMismatch {
        expected,
        got: actual,
    }
    .into())
}

fn build_encoder(
    format: FileFormat,
    schema: &SchemaRef,
    config: &FileSinkConfig,
) -> Result<Box<dyn FormatEncoder>, ConnectorError> {
    match format {
        FileFormat::Csv => {
            let csv_config = crate::schema::CsvEncoderConfig {
                delimiter: b',',
                has_header: false,
            };
            let encoder = crate::schema::CsvEncoder::with_config(schema.clone(), csv_config);
            Ok(Box::new(encoder))
        }
        FileFormat::Json | FileFormat::Text => {
            let encoder = crate::schema::JsonEncoder::new(schema.clone());
            Ok(Box::new(encoder))
        }
        FileFormat::Parquet => {
            use parquet::basic::Compression;
            let compression = match config.compression.to_lowercase().as_str() {
                "none" | "uncompressed" => Compression::UNCOMPRESSED,
                "snappy" => Compression::SNAPPY,
                "gzip" => Compression::GZIP(parquet::basic::GzipLevel::default()),
                "zstd" => Compression::ZSTD(parquet::basic::ZstdLevel::default()),
                "lz4" => Compression::LZ4,
                other => {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "unknown Parquet compression: '{other}'"
                    )));
                }
            };
            let parquet_config = crate::schema::parquet::ParquetEncoderConfig::default()
                .with_compression(compression);
            let encoder =
                crate::schema::parquet::ParquetEncoder::with_config(schema.clone(), parquet_config);
            Ok(Box::new(encoder))
        }
        FileFormat::ArrowIpc => {
            let encoder = super::arrow_ipc_codec::ArrowIpcEncoder::new(schema.clone());
            Ok(Box::new(encoder))
        }
    }
}

fn final_path_for_tmp(tmp_path: &Path) -> Result<PathBuf, ConnectorError> {
    let final_name = tmp_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".tmp"))
        .ok_or_else(|| {
            ConnectorError::WriteError(format!(
                "temporary output '{}' has no .tmp suffix",
                tmp_path.display()
            ))
        })?;
    Ok(tmp_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(final_name))
}

fn initialise_output_directory(
    dir: &Path,
    prefix: &str,
    extension: &str,
) -> Result<u64, ConnectorError> {
    std::fs::create_dir_all(dir).map_err(|e| {
        ConnectorError::WriteError(format!(
            "cannot create output directory '{}': {e}",
            dir.display()
        ))
    })?;
    if !dir.is_dir() {
        return Err(ConnectorError::WriteError(format!(
            "file sink output '{}' is not a directory",
            dir.display()
        )));
    }
    scan_next_generation(dir, prefix, extension)
}

/// Finds a generation strictly above every existing final or temporary file
/// for this exact prefix and format. Temporary files are deliberately retained:
/// deleting them during `open` could destroy a still-live writer that was
/// accidentally configured with the same target. They can be garbage-collected
/// only by an operator after establishing exclusive ownership of the target.
/// Malformed unrelated names are ignored; `create_new` and final-path
/// no-overwrite checks remain authoritative at publication time.
fn scan_next_generation(dir: &Path, prefix: &str, extension: &str) -> Result<u64, ConnectorError> {
    let entries = std::fs::read_dir(dir).map_err(|e| {
        ConnectorError::WriteError(format!(
            "cannot scan output directory '{}': {e}",
            dir.display()
        ))
    })?;
    let name_prefix = format!("{prefix}_");
    let final_suffix = format!(".{extension}");
    let temporary_suffix = format!(".{extension}.tmp");
    let mut highest = None::<u64>;
    for entry in entries {
        let entry = entry.map_err(|e| {
            ConnectorError::WriteError(format!(
                "cannot read an entry in output directory '{}': {e}",
                dir.display()
            ))
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(name) = name.strip_prefix(&name_prefix) else {
            continue;
        };
        let body = name
            .strip_suffix(&temporary_suffix)
            .or_else(|| name.strip_suffix(&final_suffix));
        let Some(body) = body else { continue };
        let Some((generation, segment)) = body.rsplit_once('_') else {
            continue;
        };
        if segment.parse::<usize>().is_err() {
            continue;
        }
        let Ok(generation) = generation.parse::<u64>() else {
            continue;
        };
        highest = Some(highest.map_or(generation, |current| current.max(generation)));
    }
    highest
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| ConnectorError::WriteError("file sink generation space is exhausted".into()))
}

#[cfg(test)]
mod tests;
