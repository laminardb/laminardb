//! File sink connector implementing [`SinkConnector`].
//!
//! Every flush publishes immutable files by atomically renaming same-directory
//! temporary files. Publication generations are independent of checkpoint
//! epochs so periodic flushes and process restarts cannot reuse final names.
//! This is durable at-least-once output, not an atomic external checkpoint
//! commit protocol.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use laminar_core::durable_fs::{durable_rename, DurableRenameMode};
use tracing::{debug, info};

use crate::config::ConnectorConfig;
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::schema::traits::FormatEncoder;

use super::config::{FileFormat, FileSinkConfig};

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
    /// Whether this generation has published at least one final file. This is
    /// retained across a partial multi-file failure so retries cannot reuse it.
    generation_has_published_files: bool,
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
        Self {
            config: None,
            schema: Arc::new(arrow_schema::Schema::empty()),
            encoder: None,
            next_generation: 1,
            generation_has_published_files: false,
            buffered_batches: Vec::new(),
            current_segment: 0,
            segment_bytes: 0,
            writer: None,
            active_tmp_files: Vec::new(),
            is_open: false,
        }
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
        let file = match tokio::task::spawn_blocking(move || {
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
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))?
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
        tokio::task::spawn_blocking(move || -> Result<(), ConnectorError> {
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
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))??;
        self.writer = None;
        Ok(())
    }

    fn advance_segment(&mut self) -> Result<(), ConnectorError> {
        self.current_segment = self.current_segment.checked_add(1).ok_or_else(|| {
            ConnectorError::WriteError("file sink segment counter overflow".into())
        })?;
        self.segment_bytes = 0;
        Ok(())
    }

    fn finish_resolved_generation(&mut self) -> Result<(), ConnectorError> {
        if self.generation_has_published_files && self.active_tmp_files.is_empty() {
            self.next_generation = self.next_generation.checked_add(1).ok_or_else(|| {
                ConnectorError::WriteError("file sink generation counter overflow".into())
            })?;
            self.generation_has_published_files = false;
            self.current_segment = 0;
            self.segment_bytes = 0;
        }
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

        let combined = if self.buffered_batches.len() == 1 {
            self.buffered_batches[0].clone()
        } else {
            arrow_select::concat::concat_batches(&self.schema, &self.buffered_batches)
                .map_err(|e| ConnectorError::WriteError(format!("batch concat error: {e}")))?
        };

        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "encoder ready".into(),
                actual: "no encoder".into(),
            })?;
        let (encoder, encoded) = tokio::task::spawn_blocking(move || {
            let encoded = encoder
                .encode_batch(&combined)
                .map_err(|e| ConnectorError::WriteError(format!("bulk encode error: {e}")));
            (encoder, encoded)
        })
        .await
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))?;
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
        tokio::task::spawn_blocking(move || -> Result<(), ConnectorError> {
            let mut writer = writer
                .lock()
                .map_err(|_| ConnectorError::WriteError("file writer lock was poisoned".into()))?;
            writer
                .write_all(&file_bytes)
                .map_err(|e| ConnectorError::WriteError(format!("write error: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))??;
        self.buffered_batches.clear();
        Ok(())
    }

    /// Close and sync every pending temporary file before publication.
    async fn prepare_pending_files(&mut self) -> Result<(), ConnectorError> {
        self.materialize_bulk_batches().await?;
        self.close_writer_async().await?;

        let paths = self.active_tmp_files.clone();
        tokio::task::spawn_blocking(move || {
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
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))??;
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
        let outcome =
            tokio::task::spawn_blocking(move || -> Result<usize, (usize, ConnectorError)> {
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
                    .map_err(|error| (0, error))?;

                let mut published = 0;
                for (tmp_path, final_path) in paths.iter().zip(&finals) {
                    if let Err(e) =
                        durable_rename(tmp_path, final_path, DurableRenameMode::NoReplace)
                    {
                        return Err((
                            published,
                            ConnectorError::WriteError(format!(
                                "cannot publish '{}' as '{}': {e}",
                                tmp_path.display(),
                                final_path.display()
                            )),
                        ));
                    }
                    published += 1;
                    debug!(path = %final_path.display(), "file sink published immutable file");
                }
                Ok(published)
            })
            .await
            .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))?;

        let (published, error) = match outcome {
            Ok(published) => (published, None),
            Err((published, error)) => (published, Some(error)),
        };
        if published > 0 {
            self.active_tmp_files.drain(..published);
            self.generation_has_published_files = true;
        }
        self.finish_resolved_generation()?;
        if let Some(error) = error {
            return Err(error);
        }
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

impl std::fmt::Debug for FileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSink")
            .field("is_open", &self.is_open)
            .field("next_generation", &self.next_generation)
            .field(
                "generation_has_published_files",
                &self.generation_has_published_files,
            )
            .field("buffered_batches", &self.buffered_batches.len())
            .field("active_tmp_files", &self.active_tmp_files.len())
            .finish()
    }
}

#[async_trait]
impl SinkConnector for FileSink {
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
        let next_generation = tokio::task::spawn_blocking(move || {
            initialise_output_directory(&out_dir, &prefix, &extension)
        })
        .await
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))??;

        self.config = Some(sink_config);
        self.schema = schema;
        self.encoder = Some(encoder);
        self.next_generation = next_generation;
        self.generation_has_published_files = false;
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

        // Row format: encode in-memory, then run the blocking write
        // loop on the blocking pool so it can't stall the runtime.
        let encoded = self
            .encoder
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "encoder ready".into(),
                actual: "no encoder".into(),
            })?
            .encode_batch(batch)
            .map_err(|e| ConnectorError::WriteError(format!("encode error: {e}")))?;
        if encoded.is_empty() {
            return Err(ConnectorError::WriteError(
                "file encoder produced no records for a non-empty batch".into(),
            ));
        }

        self.ensure_writer_async().await?;
        let writer = self
            .writer
            .as_ref()
            .expect("ensure_writer_async just ran")
            .clone();
        let bytes_written = tokio::task::spawn_blocking(move || -> Result<_, ConnectorError> {
            let mut writer = writer
                .lock()
                .map_err(|_| ConnectorError::WriteError("file writer lock was poisoned".into()))?;
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
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))??;
        self.segment_bytes = self
            .segment_bytes
            .checked_add(bytes_written)
            .ok_or_else(|| ConnectorError::WriteError("file sink byte counter overflow".into()))?;

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
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    fn test_config(out_path: &Path, format: &str) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("files");
        config.set("path", out_path.to_str().unwrap());
        config.set("format", format);
        config.set(
            "_arrow_schema",
            crate::config::encode_arrow_schema_ipc(test_schema().as_ref()),
        );
        config
    }

    fn final_files(out_path: &Path) -> Vec<PathBuf> {
        let mut files = std::fs::read_dir(out_path)
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| !path.to_string_lossy().ends_with(".tmp"))
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    #[test]
    fn test_sink_default() {
        let sink = FileSink::new();
        assert!(!sink.is_open);
    }

    #[tokio::test]
    async fn test_sink_open_creates_dir() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");

        let mut sink = FileSink::new();
        let config = test_config(&out_path, "json");

        sink.open(&config).await.unwrap();
        assert!(sink.is_open);
        assert!(out_path.exists());
        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn open_preserves_orphan_tmp_and_advances_past_its_generation() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");
        std::fs::create_dir_all(&out_path).unwrap();

        // A prior crash may leave an unpublished file. Startup must not delete
        // it because another process could still own that path.
        let orphan = out_path.join("part_000007_000.jsonl.tmp");
        std::fs::write(&orphan, b"orphan").unwrap();

        let mut sink = FileSink::new();
        let config = test_config(&out_path, "json");

        sink.open(&config).await.unwrap();

        assert!(orphan.exists());
        assert_eq!(sink.next_generation, 8);

        sink.close().await.unwrap();
    }

    #[test]
    fn contract_is_singleton_durable_at_least_once() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path(), "json");
        let sink = FileSink::new();
        let contract = sink.contract(&config).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
        assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn periodic_flush_publishes_pending_rows() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");
        let config = test_config(&out_path, "json");
        let schema = test_schema();

        let mut sink = FileSink::new();
        sink.open(&config).await.unwrap();
        sink.write_batch(&test_batch(&schema)).await.unwrap();
        assert_eq!(final_files(&out_path).len(), 0);

        sink.flush().await.unwrap();

        let files = final_files(&out_path);
        assert_eq!(files.len(), 1);
        assert!(files[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("_000001_"));
        assert!(sink.active_tmp_files.is_empty());
        assert!(sink.writer.is_none());
        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_publishes_pending_rows() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");
        let config = test_config(&out_path, "json");
        let schema = test_schema();

        let mut sink = FileSink::new();
        sink.open(&config).await.unwrap();
        sink.write_batch(&test_batch(&schema)).await.unwrap();
        sink.close().await.unwrap();

        assert_eq!(final_files(&out_path).len(), 1);
        assert!(!std::fs::read_dir(&out_path)
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp")));
    }

    #[tokio::test]
    async fn restart_uses_a_strictly_higher_generation() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");
        let config = test_config(&out_path, "json");
        let schema = test_schema();

        let mut first = FileSink::new();
        first.open(&config).await.unwrap();
        first.write_batch(&test_batch(&schema)).await.unwrap();
        first.flush().await.unwrap();
        first.close().await.unwrap();
        let first_path = final_files(&out_path).pop().unwrap();
        let first_contents = std::fs::read(&first_path).unwrap();

        let mut restarted = FileSink::new();
        restarted.open(&config).await.unwrap();
        assert_eq!(restarted.next_generation, 2);
        restarted.write_batch(&test_batch(&schema)).await.unwrap();
        restarted.flush().await.unwrap();
        restarted.close().await.unwrap();

        let files = final_files(&out_path);
        assert_eq!(files.len(), 2);
        assert!(files[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("_000001_"));
        assert!(files[1]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("_000002_"));
        assert_eq!(std::fs::read(first_path).unwrap(), first_contents);
    }

    #[tokio::test]
    async fn periodic_flush_materializes_bulk_batches() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");
        let config = test_config(&out_path, "arrow");
        let schema = test_schema();

        let mut sink = FileSink::new();
        sink.open(&config).await.unwrap();
        sink.write_batch(&test_batch(&schema)).await.unwrap();
        assert_eq!(sink.buffered_batches.len(), 1);

        sink.flush().await.unwrap();

        let files = final_files(&out_path);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].extension().unwrap(), "arrow");
        assert!(sink.buffered_batches.is_empty());
        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn partial_publication_retry_finishes_generation_without_reuse() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");
        let mut config = test_config(&out_path, "json");
        config.set("max_file_size", "1");
        let schema = test_schema();

        let mut sink = FileSink::new();
        sink.open(&config).await.unwrap();
        sink.write_batch(&test_batch(&schema)).await.unwrap();
        sink.write_batch(&test_batch(&schema)).await.unwrap();
        sink.prepare_pending_files().await.unwrap();
        assert_eq!(sink.active_tmp_files.len(), 2);

        // Model a filesystem failure after preparation but before the second
        // rename. The first segment publishes; the missing second one fails.
        std::fs::remove_file(&sink.active_tmp_files[1]).unwrap();
        let error = sink.publish_pending_files().await.unwrap_err();
        assert!(error.to_string().contains("cannot publish"));
        assert_eq!(final_files(&out_path).len(), 1);
        assert!(sink.generation_has_published_files);

        // Restore the missing temporary segment and retry. The already-published
        // segment is retained, and the generation advances only after all files
        // have resolved.
        std::fs::write(&sink.active_tmp_files[0], b"recovered").unwrap();
        sink.publish_pending_files().await.unwrap();
        assert_eq!(sink.next_generation, 2);
        assert!(!sink.generation_has_published_files);
        assert_eq!(final_files(&out_path).len(), 2);
        sink.close().await.unwrap();
    }
}
