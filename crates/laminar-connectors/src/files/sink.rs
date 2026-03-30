//! File sink connector implementing [`SinkConnector`].
//!
//! Supports two modes:
//! - **Append**: writes directly to the output file, flushing on pre-commit.
//! - **Rolling**: creates a new `.tmp` file per epoch, atomically renamed on commit.
//!
//! For bulk formats (Parquet), mid-epoch rotation is disabled — files are
//! written as a single unit on `pre_commit()`.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tracing::{debug, info};

use crate::config::ConnectorConfig;
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::schema::traits::FormatEncoder;

use super::config::{FileFormat, FileSinkConfig, SinkMode};

/// File sink connector with append or rolling mode.
pub struct FileSink {
    /// Parsed configuration.
    config: Option<FileSinkConfig>,
    /// Output schema.
    schema: SchemaRef,
    /// Format encoder.
    encoder: Option<Box<dyn FormatEncoder>>,
    /// Current epoch.
    current_epoch: u64,
    /// Buffered batches for the current epoch (Parquet bulk writes only).
    epoch_batches: Vec<RecordBatch>,
    /// Current segment index within epoch (for mid-epoch rotation).
    current_segment: usize,
    /// Bytes written in current segment.
    segment_bytes: u64,
    /// Buffered file writer for the current segment (row formats only).
    writer: Option<BufWriter<std::fs::File>>,
    /// Active .tmp file paths in the current epoch.
    active_tmp_files: Vec<PathBuf>,
    /// Whether the sink is open.
    is_open: bool,
}

impl FileSink {
    /// Creates a new file sink with a placeholder schema.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: None,
            schema: Arc::new(arrow_schema::Schema::empty()),
            encoder: None,
            current_epoch: 0,
            epoch_batches: Vec::new(),
            current_segment: 0,
            segment_bytes: 0,
            writer: None,
            active_tmp_files: Vec::new(),
            is_open: false,
        }
    }

    /// Opens (or rotates to) a new `.tmp` segment file.
    fn open_segment(&mut self) -> Result<(), ConnectorError> {
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
            self.current_epoch,
            self.current_segment,
            config.format.extension()
        );
        let path = Path::new(&config.path).join(&filename);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| {
                ConnectorError::WriteError(format!("cannot open '{}': {e}", path.display()))
            })?;

        self.writer = Some(BufWriter::new(file));
        self.active_tmp_files.push(path);
        self.segment_bytes = 0;
        Ok(())
    }

    /// Ensures a writer is open for the current segment.
    fn ensure_writer(&mut self) -> Result<(), ConnectorError> {
        if self.writer.is_none() {
            self.open_segment()?;
        }
        Ok(())
    }

    /// Closes the current writer (flushes `BufWriter`).
    fn close_writer(&mut self) -> Result<(), ConnectorError> {
        if let Some(mut w) = self.writer.take() {
            w.flush()
                .map_err(|e| ConnectorError::WriteError(format!("flush error: {e}")))?;
        }
        Ok(())
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
            .field("current_epoch", &self.current_epoch)
            .field("epoch_batches", &self.epoch_batches.len())
            .field("active_tmp_files", &self.active_tmp_files.len())
            .finish()
    }
}

#[async_trait]
impl SinkConnector for FileSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let sink_config = FileSinkConfig::from_connector_config(config)?;

        // Ensure output directory exists.
        let out_dir = Path::new(&sink_config.path);
        let out_dir_owned = out_dir.to_path_buf();
        let prefix = sink_config.prefix.clone();
        tokio::task::spawn_blocking(move || {
            if !out_dir_owned.exists() {
                std::fs::create_dir_all(&out_dir_owned)?;
            }
            cleanup_tmp_files(&out_dir_owned, &prefix);
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))?
        .map_err(|e| ConnectorError::WriteError(format!("cannot init output dir: {e}")))?;

        // Resolve schema from connector config.
        let schema = config
            .arrow_schema()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));

        let encoder = build_encoder(sink_config.format, &schema, &sink_config)?;

        self.config = Some(sink_config);
        self.schema = schema;
        self.encoder = Some(encoder);
        self.is_open = true;

        info!("file sink opened");
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
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
            let max_epoch = self.config.as_ref().map_or(10_000, |c| c.max_epoch_batches);
            if self.epoch_batches.len() >= max_epoch {
                return Err(ConnectorError::WriteError(format!(
                    "file sink: epoch batch buffer full ({max_epoch} batches) — \
                     increase max_epoch_batches or flush more frequently"
                )));
            }
            // Buffer for bulk write on pre_commit (Parquet).
            self.epoch_batches.push(batch.clone());
            return Ok(WriteResult::new(rows, 0));
        }

        // Row format: encode and write immediately via buffered writer.
        let encoder = self
            .encoder
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "encoder ready".into(),
                actual: "no encoder".into(),
            })?;

        let encoded = encoder
            .encode_batch(batch)
            .map_err(|e| ConnectorError::WriteError(format!("encode error: {e}")))?;

        self.ensure_writer()?;
        let writer = self.writer.as_mut().unwrap();

        let mut bytes_written: u64 = 0;
        for record_bytes in &encoded {
            writer
                .write_all(record_bytes)
                .map_err(|e| ConnectorError::WriteError(format!("write error: {e}")))?;
            writer
                .write_all(b"\n")
                .map_err(|e| ConnectorError::WriteError(format!("write error: {e}")))?;
            bytes_written += record_bytes.len() as u64 + 1;
        }

        self.segment_bytes += bytes_written;

        // Mid-epoch rotation for row formats (if max_file_size exceeded).
        if let Some(max_size) = max_file_size {
            if self.segment_bytes >= max_size as u64 {
                debug!("file sink: rotating at {} bytes", self.segment_bytes);
                self.close_writer()?;
                self.current_segment += 1;
                // Next write_batch will open a new segment via ensure_writer().
            }
        }

        Ok(WriteResult::new(rows, bytes_written))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        self.current_epoch = epoch;
        self.epoch_batches.clear();
        self.current_segment = 0;
        self.segment_bytes = 0;
        self.close_writer()?;
        self.active_tmp_files.clear();
        Ok(())
    }

    async fn pre_commit(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        let is_bulk = self
            .config
            .as_ref()
            .map_or(false, |c| c.format.is_bulk_format());

        if is_bulk && !self.epoch_batches.is_empty() {
            // Flush all buffered batches as a single Parquet file.
            let encoder = self
                .encoder
                .as_ref()
                .ok_or_else(|| ConnectorError::InvalidState {
                    expected: "encoder ready".into(),
                    actual: "no encoder".into(),
                })?;

            let combined = if self.epoch_batches.len() == 1 {
                self.epoch_batches[0].clone()
            } else {
                arrow_select::concat::concat_batches(&self.schema, &self.epoch_batches)
                    .map_err(|e| ConnectorError::WriteError(format!("batch concat error: {e}")))?
            };

            let encoded = encoder
                .encode_batch(&combined)
                .map_err(|e| ConnectorError::WriteError(format!("Parquet encode error: {e}")))?;

            // Parquet encoder returns exactly one file blob.
            if let Some(file_bytes) = encoded.first() {
                self.open_segment()?;
                let writer = self.writer.as_mut().unwrap();
                writer
                    .write_all(file_bytes)
                    .map_err(|e| ConnectorError::WriteError(format!("write error: {e}")))?;
            }
            self.epoch_batches.clear();
        }

        // Flush and close the buffered writer.
        self.close_writer()?;

        // Fsync all tmp files. Propagate errors — fsync failure means data
        // is not durable and we must not let the checkpoint proceed.
        let paths: Vec<PathBuf> = self.active_tmp_files.clone();
        tokio::task::spawn_blocking(move || {
            for path in &paths {
                if path.exists() {
                    let f = std::fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(|e| {
                            ConnectorError::WriteError(format!(
                                "cannot open '{}' for fsync: {e}",
                                path.display()
                            ))
                        })?;
                    f.sync_all().map_err(|e| {
                        ConnectorError::WriteError(format!(
                            "fsync failed on '{}': {e}",
                            path.display()
                        ))
                    })?;
                }
            }
            Ok::<(), ConnectorError>(())
        })
        .await
        .map_err(|e| ConnectorError::WriteError(format!("spawn_blocking failed: {e}")))?
    }

    async fn commit_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        let is_append = self
            .config
            .as_ref()
            .map_or(false, |c| c.mode == SinkMode::Append);

        if is_append {
            return Ok(());
        }

        // Rolling mode: rename .tmp → final.
        for tmp_path in &self.active_tmp_files {
            if !tmp_path.exists() {
                continue;
            }
            let final_name = tmp_path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_suffix(".tmp"))
                .ok_or_else(|| {
                    ConnectorError::WriteError(format!(
                        "tmp file '{}' has no .tmp suffix — cannot commit",
                        tmp_path.display()
                    ))
                })?;
            let final_path = tmp_path.parent().unwrap_or(Path::new(".")).join(final_name);

            std::fs::rename(tmp_path, &final_path).map_err(|e| {
                ConnectorError::WriteError(format!(
                    "cannot rename '{}' -> '{}': {e}",
                    tmp_path.display(),
                    final_path.display()
                ))
            })?;
            debug!("file sink: committed {}", final_path.display());
        }

        self.active_tmp_files.clear();
        Ok(())
    }

    async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
        self.close_writer()?;
        for tmp_path in &self.active_tmp_files {
            if tmp_path.exists() {
                let _ = std::fs::remove_file(tmp_path);
                debug!("file sink: rolled back {}", tmp_path.display());
            }
        }
        self.active_tmp_files.clear();
        self.epoch_batches.clear();
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        if self.is_open {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        SinkConnectorCapabilities::default()
            .with_exactly_once()
            .with_two_phase_commit()
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_writer()?;
        for tmp_path in &self.active_tmp_files {
            if tmp_path.exists() {
                let _ = std::fs::remove_file(tmp_path);
            }
        }
        self.active_tmp_files.clear();
        self.is_open = false;
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

/// Removes orphaned `.tmp` files matching the given prefix from a previous crash.
fn cleanup_tmp_files(dir: &Path, prefix: &str) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with(prefix) && name.ends_with(".tmp") {
                info!("file sink: removing orphaned tmp file: {}", path.display());
                let _ = std::fs::remove_file(&path);
            }
        }
    }
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

    #[test]
    fn test_sink_default() {
        let sink = FileSink::new();
        assert!(!sink.is_open);
        assert_eq!(sink.current_epoch, 0);
    }

    #[tokio::test]
    async fn test_sink_open_creates_dir() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");

        let mut sink = FileSink::new();
        let mut config = ConnectorConfig::new("files");
        config.set("path", out_path.to_str().unwrap());
        config.set("format", "json");

        sink.open(&config).await.unwrap();
        assert!(sink.is_open);
        assert!(out_path.exists());
        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sink_rolling_json_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");

        let mut sink = FileSink::new();
        let mut config = ConnectorConfig::new("files");
        config.set("path", out_path.to_str().unwrap());
        config.set("format", "json");

        sink.open(&config).await.unwrap();
        sink.begin_epoch(1).await.unwrap();

        let schema = test_schema();
        let batch = test_batch(&schema);
        let result = sink.write_batch(&batch).await.unwrap();
        assert_eq!(result.records_written, 3);

        sink.pre_commit(1).await.unwrap();
        sink.commit_epoch(1).await.unwrap();

        // Check that a final file exists (not .tmp).
        let files: Vec<_> = std::fs::read_dir(&out_path)
            .unwrap()
            .flatten()
            .filter(|e| {
                let name = e.file_name();
                let n = name.to_str().unwrap();
                !n.ends_with(".tmp")
            })
            .collect();
        assert!(!files.is_empty(), "expected committed file in output dir");

        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sink_rollback_deletes_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");

        let mut sink = FileSink::new();
        let mut config = ConnectorConfig::new("files");
        config.set("path", out_path.to_str().unwrap());
        config.set("format", "json");

        sink.open(&config).await.unwrap();
        sink.begin_epoch(1).await.unwrap();

        let schema = test_schema();
        let batch = test_batch(&schema);
        sink.write_batch(&batch).await.unwrap();

        sink.rollback_epoch(1).await.unwrap();

        let tmp_count = std::fs::read_dir(&out_path)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_str().unwrap().ends_with(".tmp"))
            .count();
        assert_eq!(
            tmp_count, 0,
            "tmp files should be cleaned up after rollback"
        );

        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_cleanup_tmp_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let out_path = dir.path().join("output");
        std::fs::create_dir_all(&out_path).unwrap();

        // Create orphaned .tmp files — only the ones matching our prefix should be removed.
        std::fs::write(out_path.join("part_000001_000.jsonl.tmp"), b"orphan").unwrap();
        std::fs::write(out_path.join("other_000001_000.jsonl.tmp"), b"keep").unwrap();

        let mut sink = FileSink::new();
        let mut config = ConnectorConfig::new("files");
        config.set("path", out_path.to_str().unwrap());
        config.set("format", "json");

        sink.open(&config).await.unwrap();

        // Only "part_*" orphaned tmp should be removed.
        assert!(!out_path.join("part_000001_000.jsonl.tmp").exists());
        assert!(out_path.join("other_000001_000.jsonl.tmp").exists());

        sink.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_capabilities() {
        let sink = FileSink::new();
        let caps = sink.capabilities();
        assert!(caps.exactly_once);
        assert!(caps.two_phase_commit);
    }
}
