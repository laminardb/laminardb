# F033: Parquet File Source Connector

## Feature Specification v1.0

**Target Phase:** Phase 3 (Connectors & Integration)
**Priority:** P2 (Useful for bootstrapping, testing, and historical backfill)
**Estimated Complexity:** Medium (3-5 days)
**Prerequisites:** F005 (DataFusion Integration), F034 (Connector SDK)

---

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F033 |
| **Status** | Draft |
| **Priority** | P2 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F005 (DataFusion Integration), F034 (Connector SDK) |
| **Blocks** | F061 (Historical Backfill) |
| **Owner** | TBD |
| **Crate** | `laminar-connectors` (feature: `parquet-source`) |
| **Created** | 2026-02-01 |

---

## Executive Summary

This specification defines the Parquet File Source Connector for LaminarDB. The connector reads Apache Parquet files from local filesystem or cloud object storage (S3, ADLS, GCS) and emits Arrow `RecordBatch` data into the streaming pipeline. It supports predicate pushdown for row-group filtering, column projection for reading only needed columns, directory scanning for partitioned datasets (Hive-style), and both bounded (one-shot) and continuous (polling) read modes.

The Parquet Source is primarily used for:
- **Bootstrapping state**: Load historical data into materialized views before enabling streaming
- **Testing**: Feed test data from Parquet files into pipelines without external dependencies
- **Historical backfill**: Read historical data alongside live streams (F061 integration)
- **Batch-to-stream bridge**: Convert batch datasets into streaming events

The source implements the `SourceConnector` trait from F034 and runs in Ring 1 (background I/O).

---

## 1. Problem Statement

### 1.1 Current Limitation

LaminarDB can currently only receive data from Kafka (F025) and PostgreSQL CDC (F027). There is no way to read data from files — the most basic and universal data source format. Parquet is the standard columnar storage format used across the data ecosystem (Spark, Trino, Pandas, Polars, DuckDB).

Without a Parquet source, users cannot:

- Bootstrap streaming materialized views with historical data
- Run integration tests with deterministic file-based data
- Bridge batch datasets into real-time pipelines
- Read from data lake exports for reprocessing

### 1.2 Required Capability

A Parquet File Source Connector that:

1. Reads Parquet files from local filesystem or cloud storage (S3, ADLS, GCS)
2. Supports column projection (read only needed columns for I/O efficiency)
3. Supports predicate pushdown (skip row groups via min/max statistics)
4. Handles Hive-style partitioned directories (e.g., `year=2026/month=01/data.parquet`)
5. Operates in bounded mode (read once, finish) or continuous mode (poll for new files)
6. Implements the `SourceConnector` trait with checkpoint support
7. Reads concurrently with configurable parallelism
8. Produces Arrow `RecordBatch` data with zero-copy deserialization

---

## 2. Design Principles

| Principle | Application |
|-----------|-------------|
| **Arrow-native** | Parquet-to-Arrow is zero-copy via `arrow` crate; no serialization overhead |
| **Push-down everything** | Column projection and predicate pushdown minimize I/O and memory |
| **Bounded by default** | File sources are finite; continuous mode is opt-in for directory polling |
| **Checkpoint-capable** | Track which files and row groups have been read for recovery |
| **Partition-aware** | Hive-style partition directories are auto-discovered and injected as columns |

---

## 3. Architecture

### 3.1 Core Data Structures

```rust
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::checkpoint::SourceCheckpoint;
use crate::connector::{
    ConnectorConfig, ConnectorError, SourceBatch, SourceConnector,
};
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

/// Configuration for the Parquet file source connector.
#[derive(Debug, Clone)]
pub struct ParquetSourceConfig {
    /// Path to a single Parquet file, directory, or glob pattern.
    /// Supports local paths, s3://, az://, gs:// prefixes.
    pub path: String,
    /// Read mode: bounded (one-shot) or continuous (poll).
    pub mode: ParquetReadMode,
    /// Columns to read (None = all columns).
    /// Reduces I/O by reading only projected columns.
    pub projection: Option<Vec<String>>,
    /// Predicate for row-group filtering.
    /// Uses column statistics (min/max) to skip irrelevant row groups.
    pub filter: Option<ParquetFilter>,
    /// Maximum number of rows per batch emitted to the pipeline.
    pub batch_size: usize,
    /// Whether to discover and inject Hive partition columns.
    /// For directories like `year=2026/month=01/`, injects
    /// `year` and `month` as additional columns.
    pub hive_partitioning: bool,
    /// Number of files to read concurrently.
    pub read_parallelism: usize,
    /// Storage options (S3 credentials, Azure keys, etc.).
    pub storage_options: HashMap<String, String>,
    /// Optional event time column name.
    /// If set, the source assigns event time from this column
    /// and generates watermarks.
    pub event_time_column: Option<String>,
    /// File ordering for deterministic reads.
    pub file_order: FileOrder,
}

impl Default for ParquetSourceConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            mode: ParquetReadMode::Bounded,
            projection: None,
            filter: None,
            batch_size: 8192,
            hive_partitioning: true,
            read_parallelism: 1,
            storage_options: HashMap::new(),
            event_time_column: None,
            file_order: FileOrder::Name,
        }
    }
}

/// Read mode for the Parquet source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetReadMode {
    /// Bounded: read all matching files once, then signal completion.
    /// Used for bootstrapping, testing, and one-shot loads.
    Bounded,
    /// Continuous: poll the directory for new files at an interval.
    /// Used for watching a directory for new data files.
    Continuous {
        /// How often to check for new files.
        poll_interval: Duration,
    },
}

/// Predicate filter for row-group pruning.
#[derive(Debug, Clone)]
pub enum ParquetFilter {
    /// Column equals a value.
    Eq(String, FilterValue),
    /// Column is greater than a value.
    Gt(String, FilterValue),
    /// Column is greater than or equal to a value.
    Gte(String, FilterValue),
    /// Column is less than a value.
    Lt(String, FilterValue),
    /// Column is less than or equal to a value.
    Lte(String, FilterValue),
    /// Column is between two values (inclusive).
    Between(String, FilterValue, FilterValue),
    /// Column is in a set of values.
    In(String, Vec<FilterValue>),
    /// Logical AND of multiple filters.
    And(Vec<ParquetFilter>),
    /// Logical OR of multiple filters.
    Or(Vec<ParquetFilter>),
}

/// A scalar value used in filters.
#[derive(Debug, Clone)]
pub enum FilterValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bool(bool),
    Date(i32),
    Timestamp(i64),
}

/// Ordering for file processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOrder {
    /// Alphabetical by file name (default, deterministic).
    Name,
    /// By modification time (oldest first).
    ModificationTime,
    /// No particular order (fastest discovery).
    Unordered,
}
```

### 3.2 Parquet Source Implementation

```rust
/// Parquet File Source Connector.
///
/// Reads Parquet files from local or cloud storage and emits Arrow
/// RecordBatch data into the streaming pipeline.
///
/// # Lifecycle
///
/// 1. `open()` - Discover files, read schema
/// 2. `poll_batch()` - Read batches in a loop
/// 3. `checkpoint()` / `restore()` - Track file + row group offsets
/// 4. `close()` - Clean shutdown
///
/// # Ring Architecture
///
/// All Parquet I/O runs in Ring 1 (background async I/O).
/// Data is pushed into Ring 0 via `Source::push_arrow()`.
pub struct ParquetSource {
    /// Connector configuration.
    config: ParquetSourceConfig,
    /// Arrow schema of the Parquet files.
    schema: Option<SchemaRef>,
    /// Discovered files to read.
    file_list: Vec<ParquetFileInfo>,
    /// Index of the current file being read.
    current_file_index: usize,
    /// Current Parquet file reader.
    current_reader: Option<ParquetFileReader>,
    /// Row group index within the current file.
    current_row_group: usize,
    /// Total rows emitted.
    rows_emitted: u64,
    /// Total bytes read.
    bytes_read: u64,
    /// Whether the bounded source has completed.
    completed: bool,
    /// Set of files already processed (for continuous mode dedup).
    processed_files: HashSet<String>,
    /// Object store for cloud storage access.
    object_store: Option<Arc<dyn ObjectStore>>,
    /// Metrics.
    metrics: ParquetSourceMetrics,
}

/// Information about a discovered Parquet file.
#[derive(Debug, Clone)]
struct ParquetFileInfo {
    /// Full path to the file.
    path: String,
    /// File size in bytes.
    size: u64,
    /// Hive partition columns extracted from the path.
    /// e.g., {"year": "2026", "month": "01"} for year=2026/month=01/data.parquet
    partition_values: HashMap<String, String>,
    /// Number of row groups in the file.
    num_row_groups: usize,
}

impl ParquetSource {
    /// Creates a new Parquet source with the given configuration.
    pub fn new(config: ParquetSourceConfig) -> Self {
        Self {
            config,
            schema: None,
            file_list: Vec::new(),
            current_file_index: 0,
            current_reader: None,
            current_row_group: 0,
            rows_emitted: 0,
            bytes_read: 0,
            completed: false,
            processed_files: HashSet::new(),
            object_store: None,
            metrics: ParquetSourceMetrics::default(),
        }
    }

    /// Creates a Parquet source from SQL connector config.
    pub fn from_connector_config(
        connector_config: &ConnectorConfig,
    ) -> Result<Self, ConnectorError> {
        let path = connector_config.require("path")?.to_string();

        let mode = match connector_config
            .options
            .get("mode")
            .map(|s| s.as_str())
        {
            Some("bounded") | None => ParquetReadMode::Bounded,
            Some("continuous") => {
                let interval = connector_config
                    .options
                    .get("poll.interval")
                    .and_then(|s| parse_duration(s).ok())
                    .unwrap_or(Duration::from_secs(10));
                ParquetReadMode::Continuous { poll_interval: interval }
            }
            Some(other) => {
                return Err(ConnectorError::Config(format!(
                    "Unknown mode: '{other}'. Expected: bounded, continuous"
                )));
            }
        };

        let projection = connector_config
            .options
            .get("columns")
            .map(|s| s.split(',').map(|c| c.trim().to_string()).collect());

        let batch_size: usize = connector_config
            .get("batch.size")
            .unwrap_or(8192);

        let hive_partitioning = connector_config
            .options
            .get("hive.partitioning")
            .map(|s| s == "true")
            .unwrap_or(true);

        let read_parallelism: usize = connector_config
            .get("parallelism")
            .unwrap_or(1);

        let event_time_column = connector_config
            .options
            .get("event.time.column")
            .cloned();

        let file_order = match connector_config
            .options
            .get("file.order")
            .map(|s| s.as_str())
        {
            Some("name") | None => FileOrder::Name,
            Some("modification_time") => FileOrder::ModificationTime,
            Some("unordered") => FileOrder::Unordered,
            Some(other) => {
                return Err(ConnectorError::Config(format!(
                    "Unknown file order: '{other}'. Expected: name, modification_time, unordered"
                )));
            }
        };

        let storage_options: HashMap<String, String> = connector_config
            .options
            .iter()
            .filter(|(k, _)| k.starts_with("storage."))
            .map(|(k, v)| {
                (k.strip_prefix("storage.").unwrap().to_string(), v.clone())
            })
            .collect();

        let config = ParquetSourceConfig {
            path,
            mode,
            projection,
            batch_size,
            hive_partitioning,
            read_parallelism,
            storage_options,
            event_time_column,
            file_order,
            ..Default::default()
        };

        Ok(Self::new(config))
    }

    /// Discovers Parquet files at the configured path.
    ///
    /// Handles:
    /// - Single file path
    /// - Directory listing (recursive)
    /// - Glob patterns (e.g., "data/*.parquet")
    /// - Cloud storage prefixes (s3://bucket/prefix/)
    async fn discover_files(&mut self) -> Result<Vec<ParquetFileInfo>, ConnectorError> {
        let store = self.get_or_create_object_store().await?;
        let path = &self.config.path;

        let mut files = Vec::new();

        // List files matching the path pattern.
        let file_paths = list_parquet_files(store.as_ref(), path).await
            .map_err(|e| ConnectorError::Connection(format!(
                "Failed to list Parquet files at '{}': {e}", path,
            )))?;

        for file_path in file_paths {
            // Skip already-processed files (continuous mode).
            if self.processed_files.contains(&file_path.to_string()) {
                continue;
            }

            let meta = store.head(&file_path).await
                .map_err(|e| ConnectorError::Io(
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                ))?;

            // Extract Hive partition values from path if enabled.
            let partition_values = if self.config.hive_partitioning {
                extract_hive_partitions(&file_path)
            } else {
                HashMap::new()
            };

            // Read Parquet metadata to get row group count.
            let parquet_meta = read_parquet_metadata(store.as_ref(), &file_path).await
                .map_err(|e| ConnectorError::Io(
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                ))?;

            files.push(ParquetFileInfo {
                path: file_path.to_string(),
                size: meta.size as u64,
                partition_values,
                num_row_groups: parquet_meta.num_row_groups(),
            });
        }

        // Sort files based on configured order.
        match self.config.file_order {
            FileOrder::Name => files.sort_by(|a, b| a.path.cmp(&b.path)),
            FileOrder::ModificationTime => {
                // Already sorted by modification time from listing.
            }
            FileOrder::Unordered => {
                // No sorting needed.
            }
        }

        Ok(files)
    }

    /// Opens a Parquet file for reading with projection and filter pushdown.
    async fn open_file_reader(
        &self,
        file_info: &ParquetFileInfo,
    ) -> Result<ParquetFileReader, ConnectorError> {
        let store = self.object_store.as_ref()
            .ok_or_else(|| ConnectorError::InvalidState(
                "Object store not initialized".into(),
            ))?;

        let mut builder = ParquetRecordBatchReaderBuilder::new(
            store.get(&file_info.path.into()).await
                .map_err(|e| ConnectorError::Io(
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
                ))?,
        )
        .map_err(|e| ConnectorError::Io(
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
        ))?;

        // Apply column projection.
        if let Some(ref columns) = self.config.projection {
            let schema = builder.schema();
            let indices: Vec<usize> = columns.iter()
                .filter_map(|col| schema.index_of(col).ok())
                .collect();
            builder = builder.with_projection(ProjectionMask::roots(
                builder.parquet_schema(),
                indices,
            ));
        }

        // Apply predicate pushdown (row-group filtering).
        if let Some(ref filter) = self.config.filter {
            let predicate = build_arrow_predicate(filter)
                .map_err(|e| ConnectorError::Config(format!(
                    "Invalid filter predicate: {e}"
                )))?;
            builder = builder.with_row_filter(predicate);
        }

        // Set batch size.
        builder = builder.with_batch_size(self.config.batch_size);

        let reader = builder.build()
            .map_err(|e| ConnectorError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            ))?;

        Ok(ParquetFileReader {
            reader,
            file_info: file_info.clone(),
        })
    }

    /// Injects Hive partition columns into a RecordBatch.
    ///
    /// For a file at `year=2026/month=01/data.parquet`, adds
    /// `year` (Utf8, "2026") and `month` (Utf8, "01") columns.
    fn inject_partition_columns(
        batch: RecordBatch,
        partition_values: &HashMap<String, String>,
    ) -> Result<RecordBatch, ConnectorError> {
        if partition_values.is_empty() {
            return Ok(batch);
        }

        let num_rows = batch.num_rows();
        let mut fields = batch.schema().fields().to_vec();
        let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();

        for (name, value) in partition_values {
            let array = Arc::new(StringArray::from(vec![value.as_str(); num_rows]));
            fields.push(Arc::new(Field::new(name, DataType::Utf8, false)));
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
            ))
    }

    /// Gets or creates the object store for file access.
    async fn get_or_create_object_store(
        &mut self,
    ) -> Result<Arc<dyn ObjectStore>, ConnectorError> {
        if let Some(ref store) = self.object_store {
            return Ok(Arc::clone(store));
        }

        let store = build_object_store(
            &self.config.path,
            &self.config.storage_options,
        )
        .map_err(|e| ConnectorError::Connection(format!(
            "Failed to create object store for '{}': {e}", self.config.path,
        )))?;

        self.object_store = Some(Arc::clone(&store));
        Ok(store)
    }
}
```

### 3.3 SourceConnector Trait Implementation

```rust
#[async_trait]
impl SourceConnector for ParquetSource {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        // Initialize object store.
        self.get_or_create_object_store().await?;

        // Discover files.
        self.file_list = self.discover_files().await?;

        if self.file_list.is_empty() {
            tracing::warn!(
                path = %self.config.path,
                "Parquet source: no files found"
            );
        } else {
            tracing::info!(
                path = %self.config.path,
                files = self.file_list.len(),
                total_bytes = self.file_list.iter().map(|f| f.size).sum::<u64>(),
                "Parquet source: discovered files"
            );
        }

        // Read schema from first file.
        if let Some(first) = self.file_list.first() {
            let reader = self.open_file_reader(first).await?;
            self.schema = Some(reader.reader.schema());
        }

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.completed {
            return Ok(None);
        }

        loop {
            // Try to read from the current file reader.
            if let Some(ref mut reader) = self.current_reader {
                match reader.reader.next() {
                    Some(Ok(batch)) => {
                        let num_rows = batch.num_rows();

                        // Inject Hive partition columns if applicable.
                        let batch = Self::inject_partition_columns(
                            batch,
                            &reader.file_info.partition_values,
                        )?;

                        self.rows_emitted += num_rows as u64;
                        self.metrics.rows_emitted += num_rows as u64;
                        self.metrics.batches_emitted += 1;

                        return Ok(Some(SourceBatch::new(batch)));
                    }
                    Some(Err(e)) => {
                        self.metrics.errors += 1;
                        return Err(ConnectorError::Io(
                            std::io::Error::new(
                                std::io::ErrorKind::Other, e.to_string(),
                            ),
                        ));
                    }
                    None => {
                        // Current file exhausted; move to next.
                        let file_path = reader.file_info.path.clone();
                        self.processed_files.insert(file_path.clone());
                        self.current_reader = None;
                        self.current_file_index += 1;
                        self.current_row_group = 0;
                        self.metrics.files_completed += 1;

                        tracing::debug!(
                            file = %file_path,
                            "Parquet source: completed file"
                        );
                    }
                }
            }

            // Open the next file.
            if self.current_file_index < self.file_list.len() {
                let file_info = &self.file_list[self.current_file_index];
                tracing::debug!(
                    file = %file_info.path,
                    size = file_info.size,
                    row_groups = file_info.num_row_groups,
                    "Parquet source: opening file"
                );
                self.current_reader = Some(
                    self.open_file_reader(file_info).await?,
                );
                continue;
            }

            // All files consumed.
            match self.config.mode {
                ParquetReadMode::Bounded => {
                    self.completed = true;
                    tracing::info!(
                        rows = self.rows_emitted,
                        files = self.processed_files.len(),
                        "Parquet source: all files read (bounded mode)"
                    );
                    return Ok(None);
                }
                ParquetReadMode::Continuous { poll_interval } => {
                    // Poll for new files.
                    tokio::time::sleep(poll_interval).await;
                    let new_files = self.discover_files().await?;

                    if new_files.is_empty() {
                        return Ok(None); // No new data yet.
                    }

                    self.file_list = new_files;
                    self.current_file_index = 0;
                    continue;
                }
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone().unwrap_or_else(|| {
            Arc::new(Schema::empty())
        })
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut state = HashMap::new();
        state.insert(
            "current_file_index".to_string(),
            self.current_file_index.to_string(),
        );
        state.insert(
            "current_row_group".to_string(),
            self.current_row_group.to_string(),
        );
        state.insert(
            "rows_emitted".to_string(),
            self.rows_emitted.to_string(),
        );

        // Store list of processed files for continuous mode.
        let processed: Vec<String> = self.processed_files.iter().cloned().collect();
        state.insert(
            "processed_files".to_string(),
            serde_json::to_string(&processed).unwrap_or_default(),
        );

        SourceCheckpoint { state }
    }

    async fn restore(
        &mut self,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        if let Some(idx) = checkpoint.state.get("current_file_index") {
            self.current_file_index = idx.parse().unwrap_or(0);
        }
        if let Some(rg) = checkpoint.state.get("current_row_group") {
            self.current_row_group = rg.parse().unwrap_or(0);
        }
        if let Some(rows) = checkpoint.state.get("rows_emitted") {
            self.rows_emitted = rows.parse().unwrap_or(0);
        }
        if let Some(files) = checkpoint.state.get("processed_files") {
            if let Ok(list) = serde_json::from_str::<Vec<String>>(files) {
                self.processed_files = list.into_iter().collect();
            }
        }

        tracing::info!(
            file_index = self.current_file_index,
            row_group = self.current_row_group,
            rows_emitted = self.rows_emitted,
            "Parquet source: restored from checkpoint"
        );

        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        HealthStatus::Healthy
    }

    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.metrics.rows_emitted,
            bytes_total: self.metrics.bytes_read,
            errors_total: self.metrics.errors,
            ..ConnectorMetrics::default()
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.current_reader = None;
        tracing::info!(
            rows = self.rows_emitted,
            files = self.processed_files.len(),
            "Parquet source: closed"
        );
        Ok(())
    }
}
```

### 3.4 Hive Partition Extraction

```rust
/// Extracts Hive-style partition values from a file path.
///
/// Given a path like `s3://bucket/table/year=2026/month=01/data.parquet`,
/// returns `{"year": "2026", "month": "01"}`.
fn extract_hive_partitions(path: &str) -> HashMap<String, String> {
    let mut partitions = HashMap::new();

    for segment in path.split('/') {
        if let Some(eq_pos) = segment.find('=') {
            let key = &segment[..eq_pos];
            let value = &segment[eq_pos + 1..];
            if !key.is_empty() && !value.is_empty() {
                partitions.insert(key.to_string(), value.to_string());
            }
        }
    }

    partitions
}
```

### 3.5 Metrics

```rust
/// Metrics for the Parquet source connector.
#[derive(Debug, Default)]
pub struct ParquetSourceMetrics {
    /// Total rows emitted to the pipeline.
    pub rows_emitted: u64,
    /// Total bytes read from storage.
    pub bytes_read: u64,
    /// Total batches emitted.
    pub batches_emitted: u64,
    /// Total files completed.
    pub files_completed: u64,
    /// Total row groups skipped by predicate pushdown.
    pub row_groups_skipped: u64,
    /// Total errors encountered.
    pub errors: u64,
}
```

---

## 4. SQL Integration

### 4.1 CREATE SOURCE Syntax

```sql
-- Read all Parquet files from a directory (bounded)
CREATE SOURCE historical_trades
WITH (
    connector = 'parquet',
    'path' = 's3://data-lake/trades/',
    'mode' = 'bounded',
    'columns' = 'symbol, price, quantity, event_time',
    'event.time.column' = 'event_time',
    'batch.size' = '8192',
    'storage.aws_region' = 'us-east-1'
);

-- Read a single Parquet file
CREATE SOURCE test_data
WITH (
    connector = 'parquet',
    'path' = '/data/test/sample.parquet',
    'mode' = 'bounded'
);

-- Watch a directory for new files (continuous)
CREATE SOURCE landing_zone
WITH (
    connector = 'parquet',
    'path' = 's3://landing/events/',
    'mode' = 'continuous',
    'poll.interval' = '30s',
    'hive.partitioning' = 'true',
    'file.order' = 'modification_time'
);

-- Read with column projection and Hive partitions
CREATE SOURCE sensor_history
WITH (
    connector = 'parquet',
    'path' = '/warehouse/sensors/',
    'columns' = 'device_id, temperature, humidity',
    'hive.partitioning' = 'true',
    'parallelism' = '4'
);

-- Use in queries (bootstrapping a materialized view)
CREATE MATERIALIZED VIEW trade_ohlc AS
SELECT
    symbol,
    FIRST_VALUE(price) AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    LAST_VALUE(price) AS close,
    SUM(quantity) AS volume
FROM historical_trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE)
EMIT ON WINDOW CLOSE;
```

---

## 5. Configuration Reference

### 5.1 Required Options

| Option | Type | Description |
|--------|------|-------------|
| `connector` | String | Must be `'parquet'` |
| `path` | String | Path to file, directory, or glob. Supports local, `s3://`, `az://`, `gs://` |

### 5.2 Optional Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mode` | String | `bounded` | Read mode: `bounded`, `continuous` |
| `poll.interval` | Duration | `10s` | Poll interval for continuous mode |
| `columns` | String (CSV) | (all) | Column projection (comma-separated names) |
| `batch.size` | Integer | `8192` | Maximum rows per emitted batch |
| `hive.partitioning` | Boolean | `true` | Auto-detect Hive partition columns from path |
| `parallelism` | Integer | `1` | Number of files to read concurrently |
| `event.time.column` | String | (none) | Column to use as event time for watermarks |
| `file.order` | String | `name` | File processing order: `name`, `modification_time`, `unordered` |

### 5.3 Storage Backend Options

Same `storage.*` prefix convention as F031 and F032. See those specs for S3, Azure, and GCS options.

---

## 6. Ring Integration

| Operation | Ring | Latency | Notes |
|-----------|------|---------|-------|
| File discovery / listing | Ring 1 | 10-500ms | Object store list operation |
| Parquet metadata read | Ring 1 | 1-10ms | Read file footer |
| Row group read + decode | Ring 1 | 1-50ms | Depends on row group size |
| Hive partition injection | Ring 1 | ~1us | Column array construction |
| Push to Ring 0 source | Ring 0 | ~5ns | SPSC channel write |
| **Ring 0 impact** | **Ring 0** | **~5ns** | **SPSC channel write only** |

---

## 7. Checkpoint and Recovery

### 7.1 Checkpoint State

```json
{
    "current_file_index": 5,
    "current_row_group": 3,
    "rows_emitted": 1250000,
    "processed_files": [
        "s3://bucket/table/part-0001.parquet",
        "s3://bucket/table/part-0002.parquet",
        "s3://bucket/table/part-0003.parquet",
        "s3://bucket/table/part-0004.parquet",
        "s3://bucket/table/part-0005.parquet"
    ]
}
```

### 7.2 Recovery Behavior

1. On restart, restore checkpoint state
2. Skip files in `processed_files` set
3. Resume reading from `current_file_index` + `current_row_group`
4. For bounded mode: re-read from the file index (may re-emit some rows from partial file)
5. For continuous mode: new files discovered since checkpoint are processed

---

## 8. Implementation Roadmap

### Phase 1: Core Reader and Bounded Mode (2-3 days)

- [ ] Define `ParquetSourceConfig`, `ParquetReadMode`, `FileOrder`
- [ ] Implement `ParquetSource` struct with file discovery
- [ ] Implement `SourceConnector` trait (open, poll_batch, close)
- [ ] Implement local filesystem reading
- [ ] Implement column projection
- [ ] Implement batch size control
- [ ] Unit tests for config parsing
- [ ] Integration test: read single file, verify batch content
- [ ] Integration test: read directory of files

### Phase 2: Predicate Pushdown and Hive Partitioning (1-2 days)

- [ ] Implement `ParquetFilter` to Arrow RowFilter conversion
- [ ] Implement row-group pruning via column statistics
- [ ] Implement `extract_hive_partitions()` path parser
- [ ] Implement `inject_partition_columns()` batch enrichment
- [ ] Test: predicate skips row groups correctly
- [ ] Test: Hive partitions injected as columns

### Phase 3: Cloud Storage and Continuous Mode (1-2 days)

- [ ] Implement object store builder for S3, ADLS, GCS
- [ ] Implement continuous mode with poll interval
- [ ] Implement file deduplication (processed_files set)
- [ ] Implement checkpoint/restore for recovery
- [ ] Test: read from S3-compatible storage (MinIO for CI)
- [ ] Test: continuous mode discovers new files
- [ ] Test: checkpoint and restore works correctly

### Phase 4: SQL Integration (1 day)

- [ ] Register `parquet` connector in `ConnectorRegistry`
- [ ] Wire `CREATE SOURCE ... WITH (connector = 'parquet')` to factory
- [ ] SQL integration tests
- [ ] API documentation

---

## 9. Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Read throughput (local) | > 500K records/sec | Benchmark with 1KB records |
| Read throughput (S3) | > 100K records/sec | Benchmark with 1KB records to S3 |
| Column projection savings | > 50% I/O reduction | Compare full vs projected reads |
| Predicate pushdown savings | > 30% row group skip rate | Count skipped vs total row groups |
| Hive partition injection | < 1us overhead per batch | Benchmark with/without partitions |
| Checkpoint recovery | Resume within 1 file | Integration test |
| Test count | 20+ tests | cargo test |

---

## 10. Module Structure

```
crates/laminar-connectors/src/
├── parquet/
│   ├── mod.rs            # Module declarations, re-exports
│   ├── config.rs         # ParquetSourceConfig, ParquetReadMode, enums
│   ├── source.rs         # ParquetSource (SourceConnector impl)
│   ├── discovery.rs      # File listing, glob matching, Hive partition extraction
│   ├── filter.rs         # ParquetFilter -> Arrow RowFilter conversion
│   └── metrics.rs        # ParquetSourceMetrics
```

---

## 11. Dependencies

### Rust Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| `parquet` | 54+ | Parquet file reading, metadata, row group access |
| `arrow` | 54+ | RecordBatch, Schema, Array types |
| `object_store` | 0.12+ | Multi-cloud storage abstraction (S3, ADLS, GCS, local) |
| `tokio` | 1.x | Async runtime for Ring 1 I/O |
| `serde_json` | 1.x | Checkpoint state serialization |
| `tracing` | 0.1 | Structured logging |

### Feature Flags

```toml
[features]
parquet-source = ["dep:parquet", "dep:object_store"]
parquet-s3 = ["parquet-source", "object_store/aws"]
parquet-azure = ["parquet-source", "object_store/azure"]
parquet-gcs = ["parquet-source", "object_store/gcp"]
```

---

## 12. References

1. **Apache Parquet Format** - Columnar storage specification
   - https://parquet.apache.org/documentation/latest/

2. **arrow-rs Parquet Crate** - Rust Parquet reader/writer
   - https://github.com/apache/arrow-rs/tree/main/parquet

3. **object_store Crate** - Multi-cloud storage abstraction
   - https://github.com/apache/arrow-rs/tree/main/object_store

4. **F034: Connector SDK** - LaminarDB connector trait hierarchy
   - Defines `SourceConnector`, `ConnectorRegistry`

5. **F061: Historical Backfill** - Reads from file sources for backfill
   - Uses `ParquetSource` for historical data loading

6. **Data Lake Research** - `docs/research/laminardb-datalake-research.md`
   - Format comparison and integration strategy
