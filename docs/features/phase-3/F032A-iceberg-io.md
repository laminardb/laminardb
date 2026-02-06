# F032A: Iceberg I/O Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F032A |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F032 (Iceberg Sink), F-CLOUD-001, F-CLOUD-002, F-CLOUD-003 |
| **Blocks** | F032B (Recovery), F032C (Compaction), F061 (Historical Backfill) |
| **Blocked By** | `iceberg-datafusion` 0.9.0 release (requires DataFusion 52.0 compatibility) |
| **Created** | 2026-02-05 |

## Summary

Replace the stubbed I/O methods in `IcebergSink` with actual `iceberg-datafusion` crate integration. This covers the core write path: connecting to Iceberg catalogs (REST, Glue, Hive), writing Arrow `RecordBatch` to Parquet data files, committing Iceberg snapshots, and writing equality delete files for CDC/upsert mode.

F032 implemented all business logic (buffering, epoch management, changelog splitting, metrics, partition transforms). F032A wires that logic to the `iceberg-datafusion` crate for actual storage I/O.

**Blocked by**: `iceberg-datafusion` version 0.8.0 requires DataFusion 51.0, but LaminarDB uses DataFusion 52.0. A compatibility PR has been merged and tests pass. Wait for `iceberg-datafusion` 0.9.0 release (~April 2026 estimate).

## Version Compatibility

| Component | LaminarDB | iceberg-datafusion 0.8.0 | iceberg-datafusion 0.9.0 (expected) |
|-----------|-----------|--------------------------|-------------------------------------|
| DataFusion | 52.0 | 51.0 (incompatible) | 52.0 (compatible) |
| Arrow | 57.2 | 57.0 | 57.x |
| Parquet | 57.2 | 57.0 | 57.x |

Track releases at: [apache/iceberg-rust releases](https://github.com/apache/iceberg-rust/releases)

## Requirements

### Functional Requirements

- **FR-1**: `open()` connects to Iceberg catalog via `iceberg::Catalog` trait (REST, Glue, Hive, Memory)
- **FR-2**: `open()` loads or creates table, reads existing schema and validates against input schema
- **FR-3**: `flush_buffer()` writes buffered `RecordBatch` to Parquet data files via `iceberg::writer::DataFileWriter`
- **FR-4**: `commit_epoch()` commits Iceberg snapshot with summary properties for exactly-once (`laminardb.writer-id`, `laminardb.epoch`)
- **FR-5**: Equality delete file writing for upsert mode using Iceberg v2 delete file API
- **FR-6**: Hidden partitioning with all 7 transforms (identity, bucket, truncate, year, month, day, hour)
- **FR-7**: Pass resolved cloud credentials to catalog and file I/O for S3/Azure/GCS
- **FR-8**: Support schema evolution (additive columns, type widening) via Iceberg schema update API
- **FR-9**: Last committed epoch resolution from snapshot history on recovery

### Non-Functional Requirements

- **NFR-1**: All I/O in Ring 1 (async, never blocks Ring 0)
- **NFR-2**: Write throughput > 200K records/sec on local storage
- **NFR-3**: Write throughput > 50K records/sec on S3
- **NFR-4**: Commit latency < 100ms local, < 500ms S3 (slightly higher than Delta due to catalog round-trip)
- **NFR-5**: Feature-gated behind `iceberg-sink` Cargo feature

## Technical Design

### Feature Flags

```toml
[dependencies]
iceberg = { version = "0.9", optional = true }
iceberg-datafusion = { version = "0.9", optional = true }
iceberg-catalog-rest = { version = "0.9", optional = true }
iceberg-catalog-glue = { version = "0.9", optional = true }
iceberg-catalog-hive = { version = "0.9", optional = true }

[features]
default = []
iceberg-sink = ["dep:iceberg", "dep:iceberg-datafusion", "dep:iceberg-catalog-rest"]
iceberg-glue = ["iceberg-sink", "dep:iceberg-catalog-glue"]
iceberg-hive = ["iceberg-sink", "dep:iceberg-catalog-hive"]
```

### Stubbed Methods to Replace

| Method | Current (F032) | Target (F032A) |
|--------|---------------|----------------|
| `open()` | Sets state to Running, mock epoch discovery | Catalog connection, table load, snapshot history scan |
| `flush_buffer_local()` | Clears buffer, updates metrics | `DataFileWriter` Parquet write with partition transforms |
| `commit_local()` | Increments mock snapshot ID | `AppendFiles` or `OverwriteFiles` with summary properties |
| `split_changelog_batch()` | Returns filtered RecordBatch | Same + equality delete file writer for upsert mode |
| `resolve_last_committed_epoch()` | Returns 0 | Scan snapshot history for `laminardb.writer-id` match |

### Catalog Connection

```rust
#[cfg(feature = "iceberg-sink")]
use iceberg::Catalog;
use iceberg_catalog_rest::RestCatalog;
#[cfg(feature = "iceberg-glue")]
use iceberg_catalog_glue::GlueCatalog;
#[cfg(feature = "iceberg-hive")]
use iceberg_catalog_hive::HiveCatalog;

impl IcebergSink {
    async fn connect_catalog(&mut self) -> Result<Arc<dyn Catalog>, ConnectorError> {
        match self.config.catalog_type {
            IcebergCatalogType::Rest => {
                let config = RestCatalogConfig::builder()
                    .uri(&self.config.catalog_uri)
                    .props(self.config.catalog_properties.clone())
                    .build();
                Ok(Arc::new(RestCatalog::new(config).await?))
            }
            #[cfg(feature = "iceberg-glue")]
            IcebergCatalogType::Glue => {
                let config = GlueCatalogConfig::builder()
                    .warehouse(&self.config.warehouse)
                    .props(self.resolved_storage_options())
                    .build();
                Ok(Arc::new(GlueCatalog::new(config).await?))
            }
            #[cfg(feature = "iceberg-hive")]
            IcebergCatalogType::Hive => {
                let config = HiveCatalogConfig::builder()
                    .uri(&self.config.catalog_uri)
                    .build();
                Ok(Arc::new(HiveCatalog::new(config).await?))
            }
            IcebergCatalogType::Memory => {
                Ok(Arc::new(iceberg::catalog::MemoryCatalog::new()))
            }
        }
    }
}
```

### Data File Writing

```rust
#[cfg(feature = "iceberg-sink")]
impl IcebergSink {
    async fn flush_buffer_iceberg(&mut self) -> Result<WriteResult, ConnectorError> {
        let table = self.table.as_ref()
            .ok_or(ConnectorError::InvalidState {
                expected: "table loaded".into(),
                actual: "no table".into(),
            })?;

        let batches = std::mem::take(&mut self.buffer);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Create data file writer with partition spec.
        let writer = table
            .data_file_writer_builder()
            .with_partition_spec(table.metadata().default_partition_spec())
            .with_target_file_size(self.config.target_file_size)
            .build()?;

        // Write batches to Parquet files.
        let mut data_files = Vec::new();
        for batch in batches {
            let files = writer.write(batch).await?;
            data_files.extend(files);
        }

        self.pending_data_files.extend(data_files.clone());
        self.metrics.record_data_files(data_files.len() as u64);
        self.metrics.record_flush(total_rows as u64, estimated_bytes);

        Ok(WriteResult::new(total_rows, estimated_bytes))
    }
}
```

### Snapshot Commit with Exactly-Once

```rust
#[cfg(feature = "iceberg-sink")]
impl IcebergSink {
    async fn commit_iceberg(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        let table = self.table.as_mut()
            .ok_or(ConnectorError::InvalidState {
                expected: "table loaded".into(),
                actual: "no table".into(),
            })?;

        let data_files = std::mem::take(&mut self.pending_data_files);
        let delete_files = std::mem::take(&mut self.pending_delete_files);

        // Build append or overwrite operation.
        let mut append = table.append_files();

        for file in data_files {
            append = append.with_data_file(file);
        }

        for file in delete_files {
            append = append.with_delete_file(file);
        }

        // Add summary properties for exactly-once tracking.
        let summary = vec![
            ("laminardb.writer-id".to_string(), self.config.writer_id.clone()),
            ("laminardb.epoch".to_string(), epoch.to_string()),
        ];

        let snapshot = append
            .with_summary_properties(summary)
            .commit()
            .await
            .map_err(|e| ConnectorError::CommitFailed(format!("Iceberg commit failed: {e}")))?;

        self.metrics.record_commit(snapshot.snapshot_id());
        self.last_committed_epoch = epoch;

        Ok(())
    }

    /// Resolves last committed epoch from snapshot history.
    async fn resolve_last_committed_epoch(&self) -> Result<u64, ConnectorError> {
        let table = self.table.as_ref()
            .ok_or(ConnectorError::InvalidState {
                expected: "table loaded".into(),
                actual: "no table".into(),
            })?;

        // Scan snapshots from newest to oldest.
        for snapshot in table.metadata().snapshots().rev() {
            if let Some(summary) = snapshot.summary() {
                let writer_id = summary.get("laminardb.writer-id");
                let epoch_str = summary.get("laminardb.epoch");

                if writer_id == Some(&self.config.writer_id) {
                    if let Some(epoch) = epoch_str.and_then(|s| s.parse::<u64>().ok()) {
                        return Ok(epoch);
                    }
                }
            }
        }

        Ok(0) // No prior epoch found for this writer.
    }
}
```

### Equality Delete Files (Upsert Mode)

```rust
#[cfg(feature = "iceberg-sink")]
impl IcebergSink {
    /// Writes equality delete file for CDC deletes.
    async fn write_delete_file(
        &mut self,
        delete_batch: RecordBatch,
    ) -> Result<(), ConnectorError> {
        let table = self.table.as_ref()
            .ok_or(ConnectorError::InvalidState {
                expected: "table loaded".into(),
                actual: "no table".into(),
            })?;

        // Build equality delete writer with configured key columns.
        let delete_writer = table
            .equality_delete_writer_builder()
            .with_equality_ids(self.equality_field_ids()?)
            .build()?;

        let delete_file = delete_writer.write(delete_batch).await?;
        self.pending_delete_files.push(delete_file);
        self.metrics.record_delete_files(1);
        self.metrics.record_deletes(delete_batch.num_rows() as u64);

        Ok(())
    }

    /// Gets field IDs for equality delete columns.
    fn equality_field_ids(&self) -> Result<Vec<i32>, ConnectorError> {
        let table = self.table.as_ref()
            .ok_or(ConnectorError::InvalidState {
                expected: "table loaded".into(),
                actual: "no table".into(),
            })?;

        let schema = table.metadata().current_schema();
        let mut ids = Vec::new();

        for col_name in &self.config.equality_delete_columns {
            let field = schema.field_by_name(col_name)
                .ok_or_else(|| ConnectorError::SchemaMismatch(
                    format!("equality delete column not found: {col_name}")
                ))?;
            ids.push(field.id());
        }

        Ok(ids)
    }
}
```

### Module Changes

```
crates/laminar-connectors/src/lakehouse/
  iceberg.rs          # Add #[cfg(feature = "iceberg-sink")] impl blocks
  iceberg_io.rs       # NEW: iceberg crate integration (catalog, write, commit, delete)
```

## Test Plan

### Unit Tests (no cloud dependency)

- [ ] `test_feature_gate_compiles` - Compiles with and without `iceberg-sink` feature
- [ ] `test_connect_rest_catalog` - REST catalog connection with mock server
- [ ] `test_connect_memory_catalog` - Memory catalog for testing
- [ ] `test_load_existing_table` - Load table from catalog
- [ ] `test_create_new_table` - Create table with Arrow schema
- [ ] `test_write_batch_to_parquet` - RecordBatch -> Parquet data file
- [ ] `test_commit_with_summary_properties` - `laminardb.writer-id` and `laminardb.epoch` in snapshot
- [ ] `test_resolve_last_committed_epoch` - Scan snapshot history for writer ID
- [ ] `test_equality_delete_file` - Equality delete for upsert mode
- [ ] `test_hidden_partitioning` - Partition transforms applied invisibly
- [ ] `test_schema_read_from_table` - Schema discovery on load
- [ ] `test_schema_mismatch_error` - Input vs table schema mismatch

### Integration Tests (local filesystem + Memory catalog)

- [ ] `test_end_to_end_append` - Write batches, verify Parquet files via DataFusion query
- [ ] `test_end_to_end_upsert` - Changelog writes with equality deletes, verify final state
- [ ] `test_epoch_idempotency` - Same epoch skipped on recovery
- [ ] `test_datafusion_table_provider` - Query Iceberg table via DataFusion
- [ ] `test_partition_pruning` - Query with partition filter uses hidden partitions
- [ ] `test_cloud_options_passed_through` - Resolved options reach object_store

### Performance Benchmarks

- [ ] `bench_local_write_throughput` - Target: > 200K records/sec
- [ ] `bench_commit_latency` - Target: < 100ms local
- [ ] `bench_delete_file_write` - Measure equality delete overhead

## Completion Checklist

- [ ] `iceberg-sink` feature gate in Cargo.toml
- [ ] Catalog connection (`RestCatalog`, `GlueCatalog`, `HiveCatalog`, `MemoryCatalog`)
- [ ] Table load/create via catalog
- [ ] `flush_buffer_iceberg()` via `DataFileWriter`
- [ ] `commit_iceberg()` with summary properties for exactly-once
- [ ] `resolve_last_committed_epoch()` from snapshot history
- [ ] Equality delete files for upsert mode
- [ ] Hidden partitioning with all 7 transforms
- [ ] `ResolvedStorageOptions` integration for cloud credentials
- [ ] Schema discovery and validation
- [ ] Unit tests passing (12+ tests)
- [ ] Integration tests passing (6+ tests)
- [ ] Performance benchmarks meeting targets
- [ ] Documentation updated

## Differences from Delta Lake (F031A)

| Aspect | F031A (Delta Lake) | F032A (Iceberg) |
|--------|-------------------|-----------------|
| Metadata | File-based `_delta_log/` | Catalog-based (REST/Glue/Hive) |
| Exactly-once | `txn` action in commit | Snapshot summary properties |
| Delete mechanism | MERGE operation | Equality delete files (v2) |
| Partitioning | Explicit columns | Hidden transforms |
| Crate | `deltalake` | `iceberg`, `iceberg-datafusion` |
| DataFusion integration | `deltalake-datafusion` provider | `iceberg-datafusion` TableProvider |

## References

- [F032: Iceberg Sink](F032-iceberg-sink.md)
- [F-CLOUD-001: Storage Credential Resolver](cloud/F-CLOUD-001-credential-resolver.md)
- [iceberg-rust on lib.rs](https://lib.rs/crates/iceberg-datafusion)
- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [iceberg-rust GitHub](https://github.com/apache/iceberg-rust)
