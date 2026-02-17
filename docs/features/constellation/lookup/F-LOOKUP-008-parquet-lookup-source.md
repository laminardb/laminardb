# F-LOOKUP-008: ParquetLookupSource (S3 Bootstrap + Pushdown)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-008 |
| **Status** | 📝 Draft |
| **Priority** | P2 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-LOOKUP-002 (LookupSource trait), F-LOOKUP-003 (Predicate types) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/lookup/parquet_source.rs` |

## Summary

`ParquetLookupSource` implements the `LookupSource` trait for Apache Parquet files stored on S3 or local disk. This is primarily used for two scenarios:

1. **Initial bootstrap**: On startup, when the cache is empty, load the lookup table from a Parquet snapshot before switching to CDC for live updates. This avoids a full table scan against the production Postgres database.

2. **Static reference data**: Some lookup tables are static (e.g., ISO country codes, exchange instrument lists, NAICS codes) and are distributed as Parquet files rather than maintained in a live database.

The source supports predicate pushdown via Parquet row-group statistics pruning and projection pushdown via column pruning. Both are native Parquet capabilities that significantly reduce I/O for selective queries.

Uses `arrow-rs` (`parquet` crate) with `ParquetRecordBatchReader` for reading, and `object_store` crate for S3/local file access.

## Goals

- Implement `LookupSource` trait for Parquet files
- Support S3 and local file system as storage backends
- Predicate pushdown via Parquet row-group statistics pruning
- Projection pushdown via Parquet column selection (read only needed columns)
- Bootstrap flow: read entire Parquet file into foyer cache on startup
- Efficient batch key lookup using row-group pruning + primary key filtering
- Support Parquet files up to 10GB (typical lookup table snapshot sizes)
- Integration with `object_store` for S3 access (with credentials)

## Non-Goals

- Writing Parquet files (this is read-only)
- Delta Lake or Iceberg table format support (separate features)
- Parquet schema evolution (assume schema matches lookup table definition)
- Real-time streaming from Parquet (this is batch/snapshot access)
- Parquet files larger than 10GB (use partitioned Parquet for larger)
- Predicate pushdown for bloom filters (future enhancement)
- Dictionary encoding optimizations (handled by arrow-rs internally)
- Page-level statistics filtering (only row-group level for now)

## Technical Design

### Architecture

**Ring**: Ring 1 (Async) -- file I/O (S3 or local disk)
**Crate**: `laminar-connectors`
**Module**: `laminar-connectors/src/lookup/parquet_source.rs`

The Parquet source is optimized for bulk reads rather than point lookups. For bootstrap, it reads the entire file sequentially. For ad-hoc lookups, it uses row-group pruning to skip irrelevant portions of the file.

```
Bootstrap Flow:
┌──────────────────────────────────────────────────────────────────┐
│  Startup                                                         │
│  ┌───────────────────────┐                                       │
│  │ 1. Cache is empty     │                                       │
│  │ 2. Read Parquet file  │                                       │
│  │    from S3             │                                       │
│  └───────────┬───────────┘                                       │
│              │                                                   │
│              ▼                                                   │
│  ┌───────────────────────┐      ┌───────────────────────┐       │
│  │ ParquetLookupSource   │      │ S3 / Local FS          │       │
│  │   .query(             │ ───> │ s3://bucket/table.parq │       │
│  │     keys=[],          │      │ /data/table.parquet    │       │
│  │     predicates=[],    │      └───────────────────────┘       │
│  │     projection=[]     │                                       │
│  │   )                   │                                       │
│  └───────────┬───────────┘                                       │
│              │                                                   │
│              ▼  Vec<Option<Vec<u8>>> (all rows)                  │
│  ┌───────────────────────┐                                       │
│  │ CdcCacheAdapter       │                                       │
│  │   .warm(entries)      │                                       │
│  │ → foyer cache loaded  │                                       │
│  └───────────────────────┘                                       │
│                                                                  │
│  3. Start CDC stream for live updates (F-LOOKUP-006)             │
└──────────────────────────────────────────────────────────────────┘

Ad-hoc Lookup (cache miss):
┌──────────────────────────────────────────────────────────────────┐
│  ParquetLookupSource.query(keys=[42], predicates, projection)    │
│                                                                  │
│  1. Read Parquet metadata (cached after first read)              │
│  2. For each row group:                                          │
│     a. Check column statistics vs predicates                     │
│     b. Skip row group if stats prove no match                    │
│  3. Read non-skipped row groups (only projected columns)         │
│  4. Filter individual rows against predicates + key match        │
│  5. Return matching rows                                         │
└──────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use crate::lookup::predicate::{ColumnId, Predicate, ScalarValue};
use crate::lookup::source::{LookupSource, SourceCapabilities};
use crate::lookup::LookupError;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use std::sync::Arc;

/// Configuration for the Parquet lookup source.
#[derive(Debug, Clone)]
pub struct ParquetLookupSourceConfig {
    /// Path to the Parquet file.
    /// Supports:
    /// - S3: "s3://bucket/path/to/file.parquet"
    /// - Local: "/data/lookup/file.parquet"
    /// - GCS: "gs://bucket/path/to/file.parquet" (future)
    pub file_path: String,

    /// Primary key column name(s) in the Parquet schema.
    /// Used for key-based filtering after row-group pruning.
    pub primary_key_columns: Vec<String>,

    /// All column names in schema order.
    /// Column index maps to ColumnId.
    pub column_names: Vec<String>,

    /// Row group read batch size.
    /// Number of rows per RecordBatch when reading.
    /// Default: 8192.
    pub batch_size: usize,

    /// S3 configuration (if file_path is s3://).
    pub s3_config: Option<S3Config>,

    /// Whether to cache the Parquet metadata after first read.
    /// Default: true. Metadata is typically < 1MB.
    pub cache_metadata: bool,

    /// Maximum file size to read (safety limit).
    /// Default: 10GB.
    pub max_file_size_bytes: u64,
}

/// S3 access configuration.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// AWS region.
    pub region: String,
    /// Access key ID (or use IAM role).
    pub access_key_id: Option<String>,
    /// Secret access key.
    pub secret_access_key: Option<String>,
    /// Custom endpoint (for MinIO, LocalStack, etc.).
    pub endpoint: Option<String>,
}

impl Default for ParquetLookupSourceConfig {
    fn default() -> Self {
        Self {
            file_path: String::new(),
            primary_key_columns: Vec::new(),
            column_names: Vec::new(),
            batch_size: 8192,
            s3_config: None,
            cache_metadata: true,
            max_file_size_bytes: 10 * 1024 * 1024 * 1024,  // 10GB
        }
    }
}

/// Parquet implementation of the LookupSource trait.
///
/// Reads Parquet files from S3 or local disk with row-group pruning
/// for predicate pushdown and column selection for projection pushdown.
///
/// # Pushdown Capabilities
///
/// | Capability | Supported | Implementation |
/// |-----------|-----------|----------------|
/// | Predicate pushdown | Yes | Row-group statistics pruning |
/// | Projection pushdown | Yes | Column selection in reader |
/// | Batch lookup | Partial | Key filtering after row-group prune |
/// | Range scan | Yes | Min/max statistics on sorted columns |
///
/// # Bootstrap Usage
///
/// ```sql
/// CREATE LOOKUP TABLE customers (
///   id INT PRIMARY KEY,
///   name VARCHAR,
///   region VARCHAR
/// ) WITH (
///   'source' = 'postgres',
///   'snapshot' = 's3://data-lake/customers/snapshot.parquet',
///   'cdc' = 'postgres_cdc'
/// );
/// ```
///
/// On startup, if cache is empty, the Parquet file is read first
/// to populate the cache quickly (S3 read is faster than full
/// Postgres table scan for large tables). Then CDC starts from
/// the snapshot's consistent point.
pub struct ParquetLookupSource {
    /// Configuration.
    config: ParquetLookupSourceConfig,

    /// Object store for file access (S3, local, etc.).
    object_store: Arc<dyn ObjectStore>,

    /// Cached Parquet metadata (if cache_metadata is true).
    cached_metadata: tokio::sync::RwLock<Option<Arc<ParquetMetaData>>>,

    /// Arrow schema of the Parquet file.
    schema: tokio::sync::RwLock<Option<SchemaRef>>,

    /// Metrics.
    metrics: std::sync::Mutex<ParquetSourceMetrics>,
}

/// Metrics for the Parquet lookup source.
#[derive(Debug, Clone, Default)]
pub struct ParquetSourceMetrics {
    /// Total queries executed.
    pub queries: u64,
    /// Total row groups read.
    pub row_groups_read: u64,
    /// Total row groups pruned (skipped by statistics).
    pub row_groups_pruned: u64,
    /// Total rows read from disk.
    pub rows_read: u64,
    /// Total rows returned after filtering.
    pub rows_returned: u64,
    /// Total bytes read from storage.
    pub bytes_read: u64,
    /// Total query errors.
    pub errors: u64,
    /// Prune ratio: row_groups_pruned / (read + pruned).
    pub prune_ratio: f64,
}

impl ParquetLookupSource {
    /// Create a new Parquet lookup source.
    ///
    /// Initializes the object store and optionally reads/caches metadata.
    pub async fn new(config: ParquetLookupSourceConfig) -> Result<Self, LookupError> {
        let object_store = Self::create_object_store(&config)?;

        let source = Self {
            config,
            object_store,
            cached_metadata: tokio::sync::RwLock::new(None),
            schema: tokio::sync::RwLock::new(None),
            metrics: std::sync::Mutex::new(ParquetSourceMetrics::default()),
        };

        // Optionally load and cache metadata on construction
        if source.config.cache_metadata {
            source.load_metadata().await?;
        }

        Ok(source)
    }

    /// Create an ObjectStore instance based on the file path.
    fn create_object_store(
        config: &ParquetLookupSourceConfig,
    ) -> Result<Arc<dyn ObjectStore>, LookupError> {
        if config.file_path.starts_with("s3://") {
            // Create S3 object store
            let s3_config = config.s3_config.as_ref().ok_or_else(|| {
                LookupError::ConfigError {
                    reason: "S3 config required for s3:// paths".into(),
                }
            })?;
            let _ = s3_config; // Use to build AWS S3 ObjectStore
            todo!("Build object_store::aws::AmazonS3 from config")
        } else {
            // Local file system
            let local = object_store::local::LocalFileSystem::new();
            Ok(Arc::new(local))
        }
    }

    /// Load and cache Parquet metadata.
    async fn load_metadata(&self) -> Result<(), LookupError> {
        let path = object_store::path::Path::from(self.config.file_path.as_str());
        let meta = self.object_store.head(&path).await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?;

        if meta.size as u64 > self.config.max_file_size_bytes {
            return Err(LookupError::ConfigError {
                reason: format!(
                    "Parquet file size {} exceeds max {}",
                    meta.size, self.config.max_file_size_bytes
                ),
            });
        }

        // Read file footer to get metadata
        let bytes = self.object_store.get(&path).await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?
            .bytes()
            .await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?;

        let schema = reader.schema().clone();
        let metadata = reader.metadata().clone();

        *self.schema.write().await = Some(schema);
        *self.cached_metadata.write().await = Some(metadata);

        Ok(())
    }

    /// Evaluate whether a row group can be pruned based on statistics.
    ///
    /// Returns true if the row group can be skipped (no matching rows).
    fn can_prune_row_group(
        &self,
        row_group_meta: &parquet::file::metadata::RowGroupMetaData,
        predicates: &[Predicate],
    ) -> bool {
        for pred in predicates {
            let col_id = pred.column_id() as usize;
            if col_id >= row_group_meta.num_columns() as usize {
                continue;
            }

            let col_meta = row_group_meta.column(col_id);
            let stats = match col_meta.statistics() {
                Some(s) => s,
                None => continue,  // No stats: cannot prune
            };

            // Check if statistics prove no rows can match
            let can_prune = match pred {
                Predicate::Eq(_, val) => {
                    Self::stat_min_exceeds(stats, val)
                        || Self::stat_max_below(stats, val)
                }
                Predicate::Gt(_, val) | Predicate::Gte(_, val) => {
                    Self::stat_max_below(stats, val)
                }
                Predicate::Lt(_, val) | Predicate::Lte(_, val) => {
                    Self::stat_min_exceeds(stats, val)
                }
                Predicate::IsNull(_) => {
                    stats.null_count() == 0
                }
                Predicate::IsNotNull(_) => {
                    stats.null_count() as u64 == row_group_meta.num_rows() as u64
                }
                _ => false,  // NotEq, In: cannot prune with min/max
            };

            if can_prune {
                return true;
            }
        }
        false
    }

    /// Check if the column's minimum value exceeds the predicate value.
    fn stat_min_exceeds(
        _stats: &parquet::file::statistics::Statistics,
        _val: &ScalarValue,
    ) -> bool {
        todo!("Compare parquet Statistics min with ScalarValue")
    }

    /// Check if the column's maximum value is below the predicate value.
    fn stat_max_below(
        _stats: &parquet::file::statistics::Statistics,
        _val: &ScalarValue,
    ) -> bool {
        todo!("Compare parquet Statistics max with ScalarValue")
    }
}

impl LookupSource for ParquetLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<Vec<u8>>>, LookupError> {
        let start = std::time::Instant::now();

        let path = object_store::path::Path::from(self.config.file_path.as_str());
        let data = self.object_store.get(&path).await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?
            .bytes()
            .await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(data)
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?;

        // Projection pushdown: select only needed columns
        if !projection.is_empty() {
            let proj_mask = parquet::arrow::ProjectionMask::leaves(
                builder.parquet_schema(),
                projection.iter().map(|&c| c as usize),
            );
            builder = builder.with_projection(proj_mask);
        }

        // Set batch size
        builder = builder.with_batch_size(self.config.batch_size);

        // Build reader (row-group pruning happens during iteration)
        let reader = builder.build()
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?;

        // Read and filter batches
        let mut results = Vec::new();
        let mut rows_read = 0u64;

        for batch_result in reader {
            let batch: RecordBatch = batch_result
                .map_err(|e| LookupError::SourceError {
                    table_id: self.config.file_path.clone(),
                    source: Box::new(e),
                })?;

            rows_read += batch.num_rows() as u64;

            // Apply key filtering and predicate filtering to batch
            let filtered = self.filter_batch(&batch, keys, predicates)?;
            results.extend(filtered);
        }

        // Update metrics
        let duration = start.elapsed();
        if let Ok(mut m) = self.metrics.lock() {
            m.queries += 1;
            m.rows_read += rows_read;
            m.rows_returned += results.iter().filter(|r| r.is_some()).count() as u64;
        }

        // Align results with input keys if keys were provided
        if keys.is_empty() {
            Ok(results)
        } else {
            Ok(self.align_with_keys(keys, results))
        }
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            supports_predicate_pushdown: true,
            supports_projection_pushdown: true,
            supports_batch_lookup: false,  // No native key index
            supports_range_scan: true,     // Row-group pruning on sorted data
            max_batch_size: 10_000,        // Limited by memory
            max_predicate_count: 10,
            estimated_single_lookup_ms: 50.0,   // S3 round-trip
            estimated_batch_lookup_ms: 100.0,   // Full scan
        }
    }

    fn source_name(&self) -> &str {
        "parquet"
    }

    fn estimated_row_count(&self) -> Option<u64> {
        // Can be read from Parquet metadata
        if let Ok(guard) = self.cached_metadata.try_read() {
            guard.as_ref().map(|m| {
                m.row_groups()
                    .iter()
                    .map(|rg| rg.num_rows() as u64)
                    .sum()
            })
        } else {
            None
        }
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        let path = object_store::path::Path::from(self.config.file_path.as_str());
        self.object_store.head(&path).await
            .map_err(|e| LookupError::SourceError {
                table_id: self.config.file_path.clone(),
                source: Box::new(e),
            })?;
        Ok(())
    }
}

impl ParquetLookupSource {
    /// Filter a RecordBatch by keys and predicates.
    fn filter_batch(
        &self,
        _batch: &RecordBatch,
        _keys: &[&[u8]],
        _predicates: &[Predicate],
    ) -> Result<Vec<Option<Vec<u8>>>, LookupError> {
        todo!("Apply key filter + predicate filter to RecordBatch rows")
    }

    /// Align filtered results with input keys.
    fn align_with_keys(
        &self,
        _keys: &[&[u8]],
        _results: Vec<Option<Vec<u8>>>,
    ) -> Vec<Option<Vec<u8>>> {
        todo!("Map results back to input key order")
    }

    /// Get current metrics snapshot.
    pub fn metrics(&self) -> ParquetSourceMetrics {
        self.metrics.lock().unwrap().clone()
    }
}
```

### Data Structures

```rust
/// Bootstrap configuration embedded in SQL DDL.
///
/// Example:
/// ```sql
/// CREATE LOOKUP TABLE customers WITH (
///   'source' = 'postgres',
///   'snapshot' = 's3://bucket/customers.parquet',
///   'cdc' = 'postgres_cdc'
/// );
/// ```
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    /// Parquet snapshot path for initial cache population.
    pub snapshot_path: String,
    /// Source type for live CDC updates after bootstrap.
    pub live_source_type: String,
    /// Source configuration for live updates.
    pub live_source_config: std::collections::HashMap<String, String>,
}
```

### Algorithm/Flow

#### Bootstrap Flow

```
1. Pipeline startup, lookup table created from config
2. Check if foyer cache is empty (first start or after failure)
3. If empty and snapshot path configured:
   a. Create ParquetLookupSource with snapshot path
   b. Call source.query(keys=[], predicates=[], projection=[])
      → reads entire Parquet file
   c. For each row: extract (primary_key, serialized_row)
   d. Insert all into foyer hot cache + HybridCache
   e. Record row count and bootstrap timestamp
4. Start CDC stream from the snapshot's consistent point
5. CDC events update the cache going forward
6. Parquet source is no longer needed (can be dropped)
```

#### Row-Group Pruning Flow

```
Given: Parquet file with 100 row groups, 1M rows each
       Predicate: Gt(timestamp_col, 2026-01-01)

1. Read Parquet metadata (cached)
2. For each row group (i = 0..99):
   a. Read column statistics for timestamp_col
   b. Get max(timestamp_col) for this row group
   c. If max < 2026-01-01: PRUNE (skip this row group)
   d. If max >= 2026-01-01: READ this row group
3. Pruned 80 row groups → read only 20 (80% reduction)
4. Within non-pruned row groups, apply predicate per-row
5. Return matching rows
```

#### Projection Pushdown Flow

```
Given: 50-column Parquet file, projection=[0, 1, 3] (id, name, region)

1. Build ParquetRecordBatchReader with ProjectionMask
2. Reader only deserializes columns 0, 1, 3
3. Remaining 47 columns are never read from disk
4. I/O savings: ~94% if columns are similar size
5. RecordBatch contains only 3 columns
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| File not found | Wrong path, S3 bucket/key | Fail with clear error, check path config |
| S3 access denied | Missing credentials or permissions | Fail with auth error, check IAM/keys |
| Corrupt Parquet file | Invalid footer, bad page header | Fail query, log error, try alternate snapshot |
| Schema mismatch | Parquet columns don't match config | Fail startup, log expected vs actual schema |
| File too large | Exceeds max_file_size_bytes | Reject with config error |
| Network timeout on S3 | Slow or unreachable S3 | Retry with backoff (3 attempts) |
| OOM during read | File too large for memory | Limit batch_size, process row groups one at a time |
| Statistics unavailable | Parquet written without stats | Cannot prune, read all row groups (degraded perf) |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| Bootstrap (local, 1M rows) | < 5s | 256-byte rows, NVMe | `bench_parquet_bootstrap_1m` |
| Bootstrap (S3, 1M rows) | < 30s | 256-byte rows, ~100MB file | `bench_parquet_bootstrap_s3_1m` |
| Row-group pruning | < 1ms | 100 row groups, metadata cached | `bench_parquet_prune_100rg` |
| Point lookup (local) | 10-50ms | With row-group pruning | `bench_parquet_point_lookup` |
| Projection savings | > 80% I/O reduction | 3 of 50 columns selected | `bench_parquet_projection` |
| Metadata caching | < 1μs after first read | Cached in memory | `bench_parquet_metadata_cached` |

## Test Plan

### Unit Tests

- [ ] `test_parquet_config_default` -- verify defaults
- [ ] `test_parquet_create_object_store_local` -- local FS
- [ ] `test_parquet_create_object_store_s3_missing_config` -- error
- [ ] `test_parquet_capabilities` -- predicate+projection pushdown true
- [ ] `test_parquet_source_name` -- returns "parquet"
- [ ] `test_parquet_estimated_row_count` -- from metadata
- [ ] `test_parquet_can_prune_row_group_eq` -- prune when val outside range
- [ ] `test_parquet_can_prune_row_group_gt` -- prune when max below val
- [ ] `test_parquet_can_prune_row_group_lt` -- prune when min above val
- [ ] `test_parquet_can_prune_row_group_is_null` -- prune when null_count=0
- [ ] `test_parquet_can_prune_row_group_is_not_null` -- prune when all null
- [ ] `test_parquet_cannot_prune_no_stats` -- no stats means no pruning
- [ ] `test_bootstrap_config_parsing` -- from SQL DDL options

### Integration Tests

- [ ] `test_parquet_source_read_local_file` -- read small Parquet file
- [ ] `test_parquet_source_full_scan` -- no keys, no predicates
- [ ] `test_parquet_source_with_predicates` -- row-group pruning works
- [ ] `test_parquet_source_with_projection` -- only selected columns returned
- [ ] `test_parquet_source_key_lookup` -- find specific keys
- [ ] `test_parquet_source_key_not_found` -- returns None for missing key
- [ ] `test_parquet_source_bootstrap_flow` -- read all, populate cache
- [ ] `test_parquet_source_metadata_caching` -- second read uses cached metadata
- [ ] `test_parquet_source_health_check_local` -- file exists check
- [ ] `test_parquet_source_s3_integration` -- requires S3/MinIO testcontainer
- [ ] `test_parquet_source_large_file` -- 1M rows, verify performance

### Benchmarks

- [ ] `bench_parquet_bootstrap_1m` -- Target: < 5s local, 1M rows
- [ ] `bench_parquet_prune_100rg` -- Target: < 1ms for 100 row groups
- [ ] `bench_parquet_point_lookup` -- Target: 10-50ms local
- [ ] `bench_parquet_projection` -- Target: > 80% I/O savings
- [ ] `bench_parquet_metadata_cached` -- Target: < 1μs

## Rollout Plan

1. **Step 1**: Add `parquet`, `arrow`, `object_store` deps to `laminar-connectors/Cargo.toml`
2. **Step 2**: Define `ParquetLookupSourceConfig` and `S3Config`
3. **Step 3**: Implement `ParquetLookupSource::new()` with object store setup
4. **Step 4**: Implement metadata loading and caching
5. **Step 5**: Implement row-group pruning logic
6. **Step 6**: Implement `LookupSource::query()` with projection pushdown
7. **Step 7**: Implement bootstrap flow integration
8. **Step 8**: Unit tests with generated Parquet test files
9. **Step 9**: Integration tests with local files and S3 (MinIO testcontainer)
10. **Step 10**: Benchmarks
11. **Step 11**: Code review and merge

## Open Questions

- [ ] Should bootstrap read the Parquet file in streaming fashion (row-group by row-group) or load the entire file into memory? Streaming uses less memory but is more complex.
- [ ] Should we support partitioned Parquet datasets (multiple files in a directory) for very large lookup tables? This is common in Delta Lake/Iceberg but adds directory listing logic.
- [ ] How to determine the "consistent point" of a Parquet snapshot for CDC resume? The snapshot should include a metadata field with the source LSN/offset at the time of export.
- [ ] Should we support Parquet files with nested schemas (structs, lists, maps)? For lookup tables, flat schemas are typical but nested data exists in practice.

## Completion Checklist

- [ ] `ParquetLookupSourceConfig` with defaults
- [ ] `S3Config` for S3 access
- [ ] `ParquetLookupSource` implementation
- [ ] `LookupSource::query()` implemented
- [ ] Row-group pruning for predicate pushdown
- [ ] Column selection for projection pushdown
- [ ] Metadata caching
- [ ] Bootstrap flow integration
- [ ] `ParquetSourceMetrics` for monitoring
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-LOOKUP-002: LookupSource Trait](./F-LOOKUP-002-lookup-source-trait.md) -- trait being implemented
- [F-LOOKUP-003: Predicate Types](./F-LOOKUP-003-predicate-types.md) -- predicate translation
- [F-LOOKUP-006: CDC-to-Cache Adapter](./F-LOOKUP-006-cdc-cache-adapter.md) -- bootstrap integration
- [Apache Parquet Format](https://parquet.apache.org/docs/file-format/) -- file format spec
- [arrow-rs parquet crate](https://docs.rs/parquet/) -- Rust Parquet reader
- [object_store crate](https://docs.rs/object_store/) -- S3/local file access
