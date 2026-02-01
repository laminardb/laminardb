# LaminarDB Data Lake Integration Research

**Version:** 1.0 | **Updated:** February 2026  
**Scope:** Delta Lake, Apache Iceberg, Apache Hudi integration for LaminarDB

---

## 1. Executive Summary

This document provides research for adding data lake format support to LaminarDB. These integrate into your **existing crate structure**:

- **Sinks** → `laminar-storage/lakehouse/` (already exists)
- **Sources** → `laminar-connectors/` (already exists)

**No new crates required** - use feature flags instead.

---

## 2. Format Comparison (January 2026)

| Aspect | Delta Lake | Apache Iceberg | Apache Hudi |
|--------|------------|----------------|-------------|
| **Rust Crate** | `deltalake` 0.24+ | `iceberg` 0.4+ | `hudi` 0.4+ |
| **Maturity** | ✅ Production | ✅ Production | ⚠️ Newer |
| **DataFusion** | ✅ Built-in | ✅ iceberg-datafusion | ✅ Feature flag |
| **Streaming Source** | ✅ CDF (Change Data Feed) | ⚠️ Incremental scan | ✅ Timeline query |
| **Best For** | General lakehouse | Multi-engine analytics | Upsert/CDC workloads |
| **Community** | Very active | Very active | Growing |

### Recommendation

1. **Delta Lake (P0)** - Start here. Best Rust support, CDF for streaming, UniForm for Iceberg interop
2. **Apache Iceberg (P0)** - Add second. Industry standard, excellent catalog support
3. **Apache Hudi (P2)** - Only if upsert workloads needed

---

## 3. Rust Crate Versions & Dependencies

```toml
# Add to workspace Cargo.toml [workspace.dependencies]

# Data lake formats
deltalake = { version = "0.24", default-features = false }
iceberg = "0.4"
iceberg-datafusion = "0.4"
iceberg-catalog-rest = "0.4"
hudi = { version = "0.4", default-features = false }

# Shared dependencies (likely already have these)
object_store = { version = "0.12", features = ["aws", "gcp", "azure"] }
parquet = "54"
arrow = "54"
url = "2"
```

---

## 4. Integration Points in Existing Crates

### 4.1 Sinks → `laminar-storage/lakehouse/`

```
crates/laminar-storage/
├── Cargo.toml              # Add feature flags here
└── src/
    ├── lib.rs
    ├── checkpoint/
    ├── wal/
    └── lakehouse/          # ← Data lake sinks go here
        ├── mod.rs
        ├── sink.rs         # Common sink trait
        ├── delta.rs        # Delta Lake sink (feature = "delta")
        ├── iceberg.rs      # Iceberg sink (feature = "iceberg")
        └── hudi.rs         # Hudi sink (feature = "hudi")
```

**Feature flags in `laminar-storage/Cargo.toml`:**

```toml
[features]
default = []
delta = ["deltalake"]
iceberg = ["dep:iceberg", "dep:iceberg-datafusion", "dep:iceberg-catalog-rest"]
hudi = ["dep:hudi"]
all-lakes = ["delta", "iceberg", "hudi"]

[dependencies]
# Required
object_store = { workspace = true }
parquet = { workspace = true }
arrow = { workspace = true }
async-trait = { workspace = true }

# Optional - data lake formats
deltalake = { workspace = true, optional = true, features = ["datafusion", "s3", "gcs", "azure"] }
iceberg = { workspace = true, optional = true }
iceberg-datafusion = { workspace = true, optional = true }
iceberg-catalog-rest = { workspace = true, optional = true }
hudi = { workspace = true, optional = true, features = ["datafusion"] }
```

### 4.2 Sources → `laminar-connectors/`

```
crates/laminar-connectors/
├── Cargo.toml              # Add feature flags here
└── src/
    ├── lib.rs
    ├── kafka/
    ├── cdc/
    ├── lookup/
    └── datalake/           # ← Data lake sources go here
        ├── mod.rs
        ├── source.rs       # Common source trait  
        ├── delta.rs        # Delta Lake source (feature = "delta-source")
        ├── iceberg.rs      # Iceberg source (feature = "iceberg-source")
        └── hudi.rs         # Hudi source (feature = "hudi-source")
```

---

## 5. Core Traits (Add to existing code)

### 5.1 Sink Trait (for `laminar-storage/lakehouse/sink.rs`)

```rust
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

/// Metadata about a prepared but not yet committed write
pub struct PreparedCommit {
    pub checkpoint_id: u64,
    pub data_files: Vec<String>,
    pub bytes_written: u64,
    pub records_written: u64,
}

/// Result of finalizing a commit
pub struct CommitInfo {
    pub checkpoint_id: u64,
    pub version: u64,  // Table version after commit
    pub commit_timestamp: i64,
}

/// Two-phase commit sink for exactly-once semantics
#[async_trait]
pub trait LakehouseSink: Send + Sync {
    /// Initialize sink with schema (called once at pipeline start)
    async fn initialize(&mut self, schema: &arrow::datatypes::Schema) -> Result<()>;
    
    /// Write a batch (Ring 1 - can buffer internally)
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    
    /// Phase 1: Prepare commit - flush files but don't commit metadata
    async fn prepare_commit(&mut self, checkpoint_id: u64) -> Result<PreparedCommit>;
    
    /// Phase 2: Finalize - commit metadata to table
    async fn finalize_commit(&mut self, prepared: PreparedCommit) -> Result<CommitInfo>;
    
    /// Abort an uncommitted prepare (on failure/recovery)
    async fn abort(&mut self, prepared: Option<PreparedCommit>) -> Result<()>;
    
    /// Close the sink gracefully
    async fn close(&mut self) -> Result<()>;
}
```

### 5.2 Source Trait (for `laminar-connectors/datalake/source.rs`)

```rust
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

/// Source reading mode
pub enum SourceMode {
    /// Read entire table once (bounded)
    Batch,
    /// Read specific version/timestamp
    Snapshot { version: Option<u64>, timestamp: Option<i64> },
    /// Read changes since checkpoint
    Incremental { since_version: u64 },
    /// Poll for new data continuously (unbounded)
    Continuous { poll_interval: std::time::Duration },
}

/// Checkpoint state for source recovery
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SourceCheckpoint {
    pub table_version: u64,
    pub files_processed: Vec<String>,
    pub timestamp: i64,
}

#[async_trait]
pub trait LakehouseSource: Send + Sync {
    /// Get the schema of the source
    fn schema(&self) -> arrow::datatypes::SchemaRef;
    
    /// Start reading from checkpoint (or beginning if None)
    async fn start(&mut self, checkpoint: Option<SourceCheckpoint>) -> Result<()>;
    
    /// Poll for next batch (None = end of bounded source or no new data)
    async fn poll_next(&mut self) -> Result<Option<RecordBatch>>;
    
    /// Get current checkpoint state (for snapshotting)
    fn current_checkpoint(&self) -> SourceCheckpoint;
    
    /// Commit that we've processed up to this checkpoint
    async fn commit(&mut self, checkpoint: SourceCheckpoint) -> Result<()>;
    
    /// Close the source
    async fn close(&mut self) -> Result<()>;
}
```

---

## 6. Delta Lake Implementation

### 6.1 Delta Lake Sink

```rust
// laminar-storage/src/lakehouse/delta.rs

use deltalake::arrow::record_batch::RecordBatch;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::write::WriteBuilder;
use deltalake::DeltaTable;
use std::sync::Arc;

pub struct DeltaSinkConfig {
    pub location: String,                    // s3://bucket/path or file:///path
    pub partition_columns: Vec<String>,
    pub target_file_size_bytes: usize,       // Default 128MB
    pub enable_uniform: bool,                // UniForm for Iceberg compat
    pub storage_options: HashMap<String, String>,
}

pub struct DeltaSink {
    config: DeltaSinkConfig,
    table: Option<DeltaTable>,
    buffer: Vec<RecordBatch>,
    buffer_size: usize,
    pending_files: Vec<String>,
}

#[async_trait]
impl LakehouseSink for DeltaSink {
    async fn initialize(&mut self, schema: &Schema) -> Result<()> {
        // Create or open table
        let table = match DeltaTable::try_from_uri(&self.config.location).await {
            Ok(t) => t,
            Err(_) => {
                CreateBuilder::new()
                    .with_location(&self.config.location)
                    .with_columns(schema_to_delta_fields(schema))
                    .with_partition_columns(&self.config.partition_columns)
                    .with_configuration_property(
                        "delta.universalFormat.enabledFormats", 
                        if self.config.enable_uniform { "iceberg" } else { "" }
                    )
                    .await?
            }
        };
        self.table = Some(table);
        Ok(())
    }
    
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.buffer.push(batch.clone());
        self.buffer_size += batch.get_array_memory_size();
        
        // Flush if buffer exceeds target file size
        if self.buffer_size >= self.config.target_file_size_bytes {
            self.flush_buffer().await?;
        }
        Ok(())
    }
    
    async fn prepare_commit(&mut self, checkpoint_id: u64) -> Result<PreparedCommit> {
        // Flush any remaining buffer
        self.flush_buffer().await?;
        
        Ok(PreparedCommit {
            checkpoint_id,
            data_files: self.pending_files.clone(),
            bytes_written: self.buffer_size as u64,
            records_written: 0, // Track separately
        })
    }
    
    async fn finalize_commit(&mut self, prepared: PreparedCommit) -> Result<CommitInfo> {
        let table = self.table.as_mut().unwrap();
        
        // Commit pending files to Delta log
        let version = table.version() + 1;
        // ... commit logic using deltalake APIs
        
        self.pending_files.clear();
        
        Ok(CommitInfo {
            checkpoint_id: prepared.checkpoint_id,
            version,
            commit_timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }
    
    async fn abort(&mut self, _prepared: Option<PreparedCommit>) -> Result<()> {
        // Delete uncommitted files
        for file in &self.pending_files {
            // Delete from object store
        }
        self.pending_files.clear();
        self.buffer.clear();
        self.buffer_size = 0;
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        // Ensure clean shutdown
        Ok(())
    }
}

impl DeltaSink {
    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        
        let table = self.table.as_ref().unwrap();
        let batches = std::mem::take(&mut self.buffer);
        
        // Write to parquet files (don't commit yet)
        let files = WriteBuilder::new(table.log_store(), table.snapshot()?)
            .with_input_batches(batches.into_iter())
            .await?;
        
        self.pending_files.extend(files);
        self.buffer_size = 0;
        Ok(())
    }
}
```

### 6.2 Delta Lake Source (with Change Data Feed)

```rust
// laminar-connectors/src/datalake/delta.rs

use deltalake::DeltaTable;

pub struct DeltaSourceConfig {
    pub location: String,
    pub mode: SourceMode,
    pub enable_cdf: bool,  // Use Change Data Feed for streaming
    pub storage_options: HashMap<String, String>,
}

pub struct DeltaSource {
    config: DeltaSourceConfig,
    table: Option<DeltaTable>,
    current_version: u64,
    schema: Option<SchemaRef>,
    file_iterator: Option<Box<dyn Iterator<Item = String>>>,
}

#[async_trait]
impl LakehouseSource for DeltaSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone().unwrap()
    }
    
    async fn start(&mut self, checkpoint: Option<SourceCheckpoint>) -> Result<()> {
        let table = DeltaTable::try_from_uri(&self.config.location).await?;
        self.schema = Some(Arc::new(table.schema()?.into()));
        
        self.current_version = checkpoint
            .map(|c| c.table_version)
            .unwrap_or(0);
        
        self.table = Some(table);
        Ok(())
    }
    
    async fn poll_next(&mut self) -> Result<Option<RecordBatch>> {
        let table = self.table.as_mut().unwrap();
        
        match &self.config.mode {
            SourceMode::Batch => {
                // Read all files once
                self.read_next_file().await
            }
            SourceMode::Continuous { poll_interval } => {
                // Check for new version
                table.update().await?;
                
                if table.version() > self.current_version {
                    if self.config.enable_cdf {
                        // Read from _change_data/ directory
                        self.read_cdf_changes(self.current_version, table.version()).await
                    } else {
                        // Diff file lists between versions
                        self.read_incremental(self.current_version, table.version()).await
                    }
                } else {
                    // No new data, sleep
                    tokio::time::sleep(*poll_interval).await;
                    Ok(None)
                }
            }
            _ => todo!()
        }
    }
    
    fn current_checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint {
            table_version: self.current_version,
            files_processed: vec![],
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }
    
    async fn commit(&mut self, checkpoint: SourceCheckpoint) -> Result<()> {
        self.current_version = checkpoint.table_version;
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

impl DeltaSource {
    /// Read Change Data Feed for row-level changes
    async fn read_cdf_changes(&mut self, from: u64, to: u64) -> Result<Option<RecordBatch>> {
        let table = self.table.as_ref().unwrap();
        
        // CDF provides _change_type column: insert, update_preimage, update_postimage, delete
        let df = table
            .cdf_scan(from as i64, to as i64)?
            .await?;
        
        // Convert to RecordBatch with change_type metadata
        // ...
        
        self.current_version = to;
        Ok(Some(batch))
    }
}
```

---

## 7. Apache Iceberg Implementation

### 7.1 Iceberg Sink

```rust
// laminar-storage/src/lakehouse/iceberg.rs

use iceberg::{Catalog, Table, TableIdent};
use iceberg::io::FileIO;
use iceberg::writer::RecordBatchWriter;

pub struct IcebergSinkConfig {
    pub catalog_type: CatalogType,   // Rest, Glue, Hive
    pub catalog_uri: String,
    pub warehouse: String,
    pub namespace: Vec<String>,
    pub table_name: String,
    pub partition_spec: Option<Vec<PartitionField>>,
}

pub enum CatalogType {
    Rest { uri: String },
    Glue { region: String },
    Hive { uri: String },
}

pub struct IcebergSink {
    config: IcebergSinkConfig,
    catalog: Arc<dyn Catalog>,
    table: Option<Table>,
    writer: Option<RecordBatchWriter>,
    pending_files: Vec<DataFile>,
}

#[async_trait]
impl LakehouseSink for IcebergSink {
    async fn initialize(&mut self, schema: &Schema) -> Result<()> {
        // Connect to catalog
        let catalog = match &self.config.catalog_type {
            CatalogType::Rest { uri } => {
                iceberg_catalog_rest::RestCatalog::new(uri).await?
            }
            CatalogType::Glue { region } => {
                iceberg_catalog_glue::GlueCatalog::new(region).await?
            }
            // ...
        };
        
        let table_ident = TableIdent::new(
            self.config.namespace.clone(),
            self.config.table_name.clone(),
        );
        
        // Create or load table
        let table = match catalog.load_table(&table_ident).await {
            Ok(t) => t,
            Err(_) => {
                catalog.create_table(
                    &self.config.namespace,
                    &self.config.table_name,
                    arrow_to_iceberg_schema(schema),
                    self.config.partition_spec.clone(),
                ).await?
            }
        };
        
        self.catalog = Arc::new(catalog);
        self.table = Some(table);
        Ok(())
    }
    
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self.writer.get_or_insert_with(|| {
            RecordBatchWriter::new(self.table.as_ref().unwrap().clone())
        });
        
        writer.write(batch).await?;
        Ok(())
    }
    
    async fn prepare_commit(&mut self, checkpoint_id: u64) -> Result<PreparedCommit> {
        let writer = self.writer.take().unwrap();
        let data_files = writer.close().await?;
        
        self.pending_files = data_files;
        
        Ok(PreparedCommit {
            checkpoint_id,
            data_files: self.pending_files.iter().map(|f| f.path.clone()).collect(),
            bytes_written: self.pending_files.iter().map(|f| f.file_size_in_bytes).sum(),
            records_written: self.pending_files.iter().map(|f| f.record_count).sum(),
        })
    }
    
    async fn finalize_commit(&mut self, prepared: PreparedCommit) -> Result<CommitInfo> {
        let table = self.table.as_mut().unwrap();
        
        // Append files to table (Iceberg handles optimistic concurrency)
        let snapshot = table
            .new_append()
            .append_data_files(std::mem::take(&mut self.pending_files))
            .commit()
            .await?;
        
        Ok(CommitInfo {
            checkpoint_id: prepared.checkpoint_id,
            version: snapshot.snapshot_id as u64,
            commit_timestamp: snapshot.timestamp_ms,
        })
    }
    
    async fn abort(&mut self, _prepared: Option<PreparedCommit>) -> Result<()> {
        // Iceberg: uncommitted files can be cleaned up by GC
        self.pending_files.clear();
        self.writer = None;
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
```

### 7.2 Iceberg Source

```rust
// laminar-connectors/src/datalake/iceberg.rs

pub struct IcebergSourceConfig {
    pub catalog_type: CatalogType,
    pub catalog_uri: String,
    pub namespace: Vec<String>,
    pub table_name: String,
    pub mode: SourceMode,
}

pub struct IcebergSource {
    config: IcebergSourceConfig,
    table: Option<Table>,
    current_snapshot_id: Option<i64>,
    scan: Option<TableScan>,
}

#[async_trait]
impl LakehouseSource for IcebergSource {
    async fn poll_next(&mut self) -> Result<Option<RecordBatch>> {
        match &self.config.mode {
            SourceMode::Batch => {
                // Full table scan
                self.read_next_batch().await
            }
            SourceMode::Snapshot { version, timestamp } => {
                // Time travel query
                let scan = self.table.as_ref().unwrap()
                    .scan()
                    .snapshot_id(*version)
                    .build()?;
                self.read_from_scan(scan).await
            }
            SourceMode::Continuous { poll_interval } => {
                // Poll for new snapshots
                let table = self.table.as_mut().unwrap();
                table.refresh().await?;
                
                let latest = table.current_snapshot();
                if latest.map(|s| s.snapshot_id) != self.current_snapshot_id {
                    // New snapshot available - read incrementally
                    self.read_incremental_snapshot().await
                } else {
                    tokio::time::sleep(*poll_interval).await;
                    Ok(None)
                }
            }
            _ => todo!()
        }
    }
}
```

---

## 8. SQL Integration

### 8.1 CREATE SINK Syntax

```sql
-- Delta Lake sink
CREATE SINK trades_delta
WITH (
    type = 'delta',
    location = 's3://my-bucket/warehouse/trades',
    partition_by = 'date',
    uniform_iceberg = true,  -- Enable UniForm
    target_file_size = '128MB',
    aws_region = 'us-east-1'
)
AS SELECT 
    symbol,
    price,
    quantity,
    CAST(event_time AS DATE) as date
FROM trades_stream;

-- Iceberg sink  
CREATE SINK orders_iceberg
WITH (
    type = 'iceberg',
    catalog_type = 'rest',
    catalog_uri = 'http://iceberg-catalog:8181',
    warehouse = 's3://my-bucket/warehouse',
    namespace = 'analytics',
    table_name = 'orders',
    partition_by = 'DAYS(order_time)'  -- Hidden partitioning
)
AS SELECT * FROM orders_stream;
```

### 8.2 CREATE SOURCE Syntax

```sql
-- Delta Lake source (streaming with CDF)
CREATE SOURCE trades_from_delta
WITH (
    type = 'delta',
    location = 's3://my-bucket/warehouse/trades',
    mode = 'continuous',
    change_data_feed = true,
    poll_interval = '10 seconds'
);

-- Iceberg source (batch)
CREATE SOURCE orders_snapshot
WITH (
    type = 'iceberg',
    catalog_type = 'glue',
    catalog_uri = 'arn:aws:glue:us-east-1:123456789:catalog',
    namespace = 'analytics',
    table_name = 'orders',
    mode = 'batch'
);

-- Use in queries
SELECT symbol, SUM(quantity)
FROM trades_from_delta
WHERE _change_type IN ('insert', 'update_postimage')
GROUP BY symbol;
```

---

## 9. Checkpoint Integration

### 9.1 Two-Phase Commit Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    Checkpoint Barrier Flow                        │
│                                                                   │
│  Source ──▶ Operators ──▶ LakehouseSink                          │
│    │            │              │                                  │
│  [barrier]   [barrier]      [barrier]                            │
│    │            │              │                                  │
│    ▼            ▼              ▼                                  │
│  ┌─────┐     ┌─────┐       ┌────────────┐                        │
│  │Offset│    │State│       │ Phase 1:   │                        │
│  │Track │    │Snap │       │ Flush files│                        │
│  └─────┘     └─────┘       │ prepare()  │                        │
│    │            │          └────────────┘                        │
│    └────────────┴──────────────┬─────────────────────────────────┤
│                                │                                  │
│                                ▼                                  │
│                    ┌───────────────────────┐                     │
│                    │  Checkpoint Store     │                     │
│                    │  (Store PreparedCommit)│                     │
│                    └───────────────────────┘                     │
│                                │                                  │
│                                ▼ (after checkpoint confirmed)    │
│                    ┌───────────────────────┐                     │
│                    │  Phase 2:             │                     │
│                    │  finalize_commit()    │                     │
│                    │  (Commit to catalog)  │                     │
│                    └───────────────────────┘                     │
└──────────────────────────────────────────────────────────────────┘
```

### 9.2 Recovery Flow

```rust
async fn recover_sink(sink: &mut impl LakehouseSink, checkpoint: &Checkpoint) -> Result<()> {
    // Check for pending (prepared but not finalized) commits
    if let Some(pending) = checkpoint.pending_lake_commit.clone() {
        // Decide: finalize or abort?
        // If checkpoint was fully confirmed, finalize
        // Otherwise, abort
        if checkpoint.fully_confirmed {
            sink.finalize_commit(pending).await?;
        } else {
            sink.abort(Some(pending)).await?;
        }
    }
    
    Ok(())
}
```

---

## 10. Testing Strategy

### 10.1 Integration Tests

```rust
#[tokio::test]
async fn test_delta_sink_exactly_once() {
    // Use testcontainers for MinIO
    let minio = MinioContainer::start().await;
    
    let config = DeltaSinkConfig {
        location: format!("s3://{}/test-table", minio.bucket()),
        enable_uniform: true,
        ..Default::default()
    };
    
    let mut sink = DeltaSink::new(config);
    sink.initialize(&test_schema()).await.unwrap();
    
    // Write some batches
    for batch in test_batches() {
        sink.write_batch(&batch).await.unwrap();
    }
    
    // Prepare commit
    let prepared = sink.prepare_commit(1).await.unwrap();
    assert!(!prepared.data_files.is_empty());
    
    // Simulate crash before finalize
    drop(sink);
    
    // Recover - files should be cleaned up
    let mut sink2 = DeltaSink::new(config.clone());
    sink2.abort(Some(prepared)).await.unwrap();
    
    // Table should have 0 versions (nothing committed)
    let table = DeltaTable::try_from_uri(&config.location).await.unwrap();
    assert_eq!(table.version(), 0);
}

#[tokio::test]
async fn test_delta_source_cdf() {
    // Write some data with CDF enabled
    let table = create_test_delta_table_with_cdf().await;
    
    // Insert, update, delete
    insert_rows(&table, vec![row1, row2]).await;
    update_row(&table, row1_updated).await;
    delete_row(&table, row2).await;
    
    // Read via CDF
    let mut source = DeltaSource::new(DeltaSourceConfig {
        location: table.location().to_string(),
        mode: SourceMode::Continuous { poll_interval: Duration::from_millis(100) },
        enable_cdf: true,
        ..Default::default()
    });
    
    source.start(None).await.unwrap();
    
    let mut changes = vec![];
    while let Some(batch) = source.poll_next().await.unwrap() {
        changes.push(batch);
    }
    
    // Should see: insert(row1), insert(row2), update_pre(row1), update_post(row1), delete(row2)
    assert_change_types(&changes, &["insert", "insert", "update_preimage", "update_postimage", "delete"]);
}
```

### 10.2 Interoperability Tests

```rust
#[tokio::test]
async fn test_delta_uniform_read_as_iceberg() {
    // Write with Delta + UniForm
    let mut delta_sink = DeltaSink::new(DeltaSinkConfig {
        location: "s3://bucket/table".into(),
        enable_uniform: true,
        ..Default::default()
    });
    
    delta_sink.write_batch(&test_batch()).await.unwrap();
    delta_sink.prepare_commit(1).await.unwrap();
    delta_sink.finalize_commit(...).await.unwrap();
    
    // Read with Iceberg
    let iceberg_source = IcebergSource::new(IcebergSourceConfig {
        catalog_type: CatalogType::Rest { uri: "...".into() },
        // UniForm creates Iceberg metadata automatically
        ..Default::default()
    });
    
    let batch = iceberg_source.poll_next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), test_batch().num_rows());
}
```

---

## 11. Performance Considerations

### 11.1 Ring Integration

| Operation | Ring | Budget | Notes |
|-----------|------|--------|-------|
| Buffer batch | Ring 0 | <1μs | Just append to Vec |
| Flush to Parquet | Ring 1 | 10-100ms | Async I/O |
| Prepare commit | Ring 1 | 1-10ms | File listing |
| Finalize commit | Ring 2 | 10-100ms | Catalog update |

### 11.2 Optimization Tips

1. **Batch size**: Target 128MB files for optimal read performance
2. **Partition pruning**: Partition by frequently filtered columns
3. **Z-ordering**: Use Delta's OPTIMIZE ZORDER for multi-column queries
4. **Compaction**: Schedule periodic OPTIMIZE in Ring 2

---

## 12. References

| Resource | URL |
|----------|-----|
| delta-rs docs | https://delta-io.github.io/delta-rs/ |
| iceberg-rust | https://github.com/apache/iceberg-rust |
| hudi-rs | https://github.com/apache/hudi-rs |
| Delta UniForm | https://docs.delta.io/latest/delta-uniform.html |
| Iceberg spec | https://iceberg.apache.org/spec/ |

---

# Claude Code Prompt

## Using This Document

Save this file as `docs/research/datalake-formats.md` in your LaminarDB project.

Then use these prompts with Claude Code:

---

### Prompt: Generate or Extend Feature Spec

```
@docs/research/datalake-formats.md

Read the data lake research document, then:

1. Check what exists in the codebase:
   - Look at `crates/laminar-storage/src/lakehouse/`
   - Look at `crates/laminar-connectors/src/`
   - Check Cargo.toml for existing feature flags

2. Based on what exists, either:
   a) GENERATE a new feature spec if nothing exists
   b) EXTEND the existing feature spec if partial implementation exists

Feature: [FEATURE_NAME]
Phase: [PHASE_NUMBER] 
Priority: [P0/P1/P2]

Requirements:
- [List specific requirements]

Output the feature spec to: docs/features/phase-[N]/[feature-name].md

Follow LaminarDB conventions:
- Slot into existing crates (laminar-storage, laminar-connectors)
- Use feature flags, not new crates
- Integrate with existing checkpoint system
- Maintain sub-500ns Ring 0 budget (buffering only)
```

---

### Example: Delta Lake Sink

```
@docs/research/datalake-formats.md

Read the data lake research document, then generate or extend a feature spec for:

Feature: Delta Lake Sink
Phase: 3
Priority: P0

Requirements:
- Write streaming data to Delta Lake tables
- Support S3, GCS, Azure, and local filesystem
- Enable UniForm for Iceberg compatibility by default
- Two-phase commit for exactly-once semantics
- Integrate with existing checkpoint barriers

Check:
- Does `crates/laminar-storage/src/lakehouse/delta.rs` exist?
- Is `deltalake` in Cargo.toml dependencies?

If exists: Extend the spec with any missing capabilities
If not: Generate complete spec

Output to: docs/features/phase-3/delta-lake-sink.md
```

---

### Example: Iceberg Source

```
@docs/research/datalake-formats.md

Read the data lake research document, then generate or extend a feature spec for:

Feature: Apache Iceberg Source  
Phase: 3
Priority: P1

Requirements:
- Read from Iceberg tables as streaming source
- Support REST, Glue, and Hive catalogs
- Continuous mode with snapshot polling
- Incremental reads between snapshots
- Checkpoint integration for exactly-once

Check:
- Does `crates/laminar-connectors/src/datalake/iceberg.rs` exist?
- Is `iceberg` in Cargo.toml dependencies?

If exists: Extend the spec with any missing capabilities
If not: Generate complete spec

Output to: docs/features/phase-3/iceberg-source.md
```

---

### Example: Add Hudi Support (Lower Priority)

```
@docs/research/datalake-formats.md

Read the data lake research document, then generate a feature spec for:

Feature: Apache Hudi Sink + Source
Phase: 3
Priority: P2

Requirements:
- Support both CoW and MoR table types
- Upsert operations via record key
- Timeline-based incremental queries
- Lower priority than Delta/Iceberg

This is likely a NEW feature. Generate complete spec.

Output to: docs/features/phase-3/hudi-connector.md
```

---

### Quick Commands

```bash
# Check what exists
ls crates/laminar-storage/src/lakehouse/
ls crates/laminar-connectors/src/
grep -r "deltalake\|iceberg\|hudi" Cargo.toml

# Generate Delta sink spec
# (Use prompt above)

# Generate Iceberg source spec  
# (Use prompt above)

# List all data lake features
ls docs/features/phase-3/*lake* docs/features/phase-3/*iceberg* docs/features/phase-3/*delta* docs/features/phase-3/*hudi* 2>/dev/null
```

---

## Feature Spec Template

When generating specs, use this structure:

```markdown
# F0XX: [Feature Name]

## Metadata
| Field | Value |
|-------|-------|
| ID | F0XX |
| Status | 📝 Draft |
| Priority | P0/P1/P2 |
| Phase | 3 |
| Crate | laminar-storage or laminar-connectors |
| Feature Flag | `delta` / `iceberg` / `hudi` |

## Summary
[2-3 sentences]

## Goals
- Goal 1
- Goal 2

## Non-Goals
- What this doesn't do

## Design

### Location in Codebase
```
crates/laminar-storage/src/lakehouse/delta.rs  # or wherever it goes
```

### Dependencies (add to Cargo.toml)
```toml
[dependencies]
deltalake = { workspace = true, optional = true }

[features]
delta = ["deltalake"]
```

### Implementation
[Code examples from research doc]

## Ring Integration
| Operation | Ring | Budget |
|-----------|------|--------|
| ... | ... | ... |

## SQL Interface
```sql
CREATE SINK/SOURCE ...
```

## Testing
- Unit tests
- Integration tests (with testcontainers)
- Interop tests

## Success Criteria
| Metric | Target |
|--------|--------|
| ... | ... |
```
