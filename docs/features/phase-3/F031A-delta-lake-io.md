# F031A: Delta Lake I/O Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F031A |
| **Status** | 📝 Draft (Ready for Implementation) |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F031 (Delta Lake Sink), F-CLOUD-001, F-CLOUD-002, F-CLOUD-003 |
| **Blocks** | F031B (Recovery), F031C (Compaction), F061 (Historical Backfill) |
| **Blocked By** | ~~`deltalake` crate compatibility~~ ✅ **UNBLOCKED** (2026-02-05) |
| **Created** | 2026-02-02 |
| **Updated** | 2026-02-05 |

## Summary

Replace the stubbed I/O methods in `DeltaLakeSink` with actual `deltalake` crate integration. This covers the core write path: opening/creating Delta Lake tables, converting Arrow `RecordBatch` to Parquet, committing Delta log transactions, and MERGE execution for CDC/upsert mode.

F031 implemented all business logic (buffering, epoch management, changelog splitting, metrics). F031A wires that logic to the `deltalake` crate for actual storage I/O.

### Dependency Status

✅ **UNBLOCKED** (2026-02-05): The `deltalake` crate is now available via git dependency from the main branch, which uses DataFusion 52.1.0 (compatible with LaminarDB's DataFusion 52.0).

```toml
# Current dependency in laminar-connectors/Cargo.toml
deltalake = { git = "https://github.com/delta-io/delta-rs", branch = "main", ... }
```

When `deltalake` 0.31.0 is released with DataFusion 52.x, switch to a versioned dependency.

## Requirements

### Functional Requirements

- **FR-1**: `open()` creates or opens Delta Lake table via `deltalake::open_table_with_storage_options()`
- **FR-2**: `open()` reads existing table schema and validates against input schema
- **FR-3**: `flush_buffer()` converts buffered `RecordBatch` to Parquet using `deltalake::write` API
- **FR-4**: `commit_epoch()` writes Delta log entry with `txn` application transaction metadata
- **FR-5**: MERGE execution for upsert mode using Delta Lake merge builder API
- **FR-6**: Partitioned writes (partition Parquet files by configured columns)
- **FR-7**: Pass `ResolvedStorageOptions` to `open_table_with_storage_options()` for S3/Azure/GCS
- **FR-8**: Support `DeltaWriteMode::Overwrite` via partition replacement

### Non-Functional Requirements

- **NFR-1**: All I/O in Ring 1 (async, never blocks Ring 0)
- **NFR-2**: Write throughput > 200K records/sec on local storage
- **NFR-3**: Write throughput > 50K records/sec on S3
- **NFR-4**: Commit latency < 50ms local, < 500ms S3
- **NFR-5**: Feature-gated behind `delta-lake` Cargo feature

## Technical Design

### Feature Flags

✅ **Implemented** in `laminar-connectors/Cargo.toml`:

```toml
[features]
# Delta Lake - using git main branch with DataFusion 52.x
delta-lake = ["dep:deltalake", "deltalake/datafusion"]
delta-lake-s3 = ["delta-lake", "deltalake/s3"]
delta-lake-azure = ["delta-lake", "deltalake/azure"]
delta-lake-gcs = ["delta-lake", "deltalake/gcs"]
delta-lake-unity = ["delta-lake", "deltalake/unity-experimental"]
delta-lake-glue = ["delta-lake", "deltalake/glue"]
```

### Stubbed Methods to Replace

| Method | Current (F031) | Target (F031A) |
|--------|---------------|----------------|
| `open()` | Sets state to Running | `open_table_with_storage_options()`, schema discovery |
| `flush_buffer_local()` | Clears buffer, updates metrics | `deltalake::write::WriteBuilder` Parquet write |
| `commit_local()` | Increments version counter | `CommitBuilder` with `txn` action |
| `split_changelog_batch()` | Returns filtered RecordBatch | Same + `MergeBuilder` for upsert mode |

### Key Implementation

```rust
#[cfg(feature = "delta-lake")]
impl DeltaLakeSink {
    /// Opens or creates the Delta Lake table.
    async fn open_table(&mut self) -> Result<(), ConnectorError> {
        let resolved = StorageCredentialResolver::resolve(
            &self.config.table_path,
            &self.config.storage_options,
        );

        self.table = Some(
            deltalake::open_table_with_storage_options(
                &self.config.table_path,
                resolved.options,
            )
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(
                format!("failed to open Delta table: {e}")
            ))?,
        );

        // Read table schema for validation.
        if let Some(ref table) = self.table {
            self.schema = Some(Arc::new(
                table.schema()
                    .map_err(|e| ConnectorError::SchemaMismatch(
                        format!("failed to read table schema: {e}")
                    ))?
                    .try_into()
                    .map_err(|e: ArrowError| ConnectorError::SchemaMismatch(
                        format!("schema conversion failed: {e}")
                    ))?,
            ));
        }

        Ok(())
    }

    /// Flushes buffer to Parquet file(s) via deltalake write API.
    async fn flush_buffer_delta(&mut self) -> Result<WriteResult, ConnectorError> {
        let table = self.table.as_mut()
            .ok_or(ConnectorError::InvalidState {
                expected: "table opened".into(),
                actual: "no table".into(),
            })?;

        let batches = std::mem::take(&mut self.buffer);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let mut write = deltalake::write::WriteBuilder::new(
            table.log_store(),
            table.state.clone(),
        )
        .with_write_mode(self.delta_write_mode())
        .with_input_batches(batches.into_iter());

        if !self.config.partition_columns.is_empty() {
            write = write.with_partition_columns(
                self.config.partition_columns.clone()
            );
        }

        let new_version = write.await
            .map_err(|e| ConnectorError::WriteError(
                format!("Delta write failed: {e}")
            ))?;

        self.pending_files += 1;
        self.delta_version = new_version as u64;
        // ... update metrics ...

        Ok(WriteResult::new(total_rows, estimated_bytes))
    }

    /// Commits with txn metadata for exactly-once.
    async fn commit_delta(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        let table = self.table.as_mut()
            .ok_or(ConnectorError::InvalidState {
                expected: "table opened".into(),
                actual: "no table".into(),
            })?;

        // Add txn action for exactly-once tracking.
        let txn_action = deltalake::protocol::Action::txn(
            deltalake::protocol::Txn {
                app_id: self.config.writer_id.clone(),
                version: epoch as i64,
                last_updated: Some(chrono::Utc::now().timestamp_millis()),
            },
        );

        // Commit is atomic — either all pending adds + txn succeed or none do.
        // ...

        Ok(())
    }
}
```

### Module Changes

```
crates/laminar-connectors/src/lakehouse/
  delta.rs          # Add #[cfg(feature = "delta-lake")] impl blocks
  delta_io.rs       # NEW: deltalake crate integration (table open, write, commit, merge)
```

## Test Plan

### Unit Tests (no cloud dependency)

- [ ] `test_feature_gate_compiles` - Compiles with and without `delta-lake` feature
- [ ] `test_open_local_table` - Open/create table on local filesystem
- [ ] `test_write_batch_to_parquet` - RecordBatch -> Parquet file
- [ ] `test_commit_with_txn_metadata` - txn action in Delta log
- [ ] `test_partitioned_write` - Files organized by partition columns
- [ ] `test_overwrite_mode` - Partition replacement
- [ ] `test_schema_read_from_table` - Schema discovery on open
- [ ] `test_schema_mismatch_error` - Input vs table schema mismatch

### Integration Tests (local filesystem)

- [ ] `test_end_to_end_append` - Write batches, verify Parquet files exist
- [ ] `test_end_to_end_upsert` - Changelog writes with MERGE
- [ ] `test_epoch_to_version_mapping` - txn metadata in _delta_log
- [ ] `test_cloud_options_passed_through` - Resolved options reach object_store

### Performance Benchmarks

- [ ] `bench_local_write_throughput` - Target: > 200K records/sec
- [ ] `bench_commit_latency` - Target: < 50ms local

## Completion Checklist

- [x] `delta-lake` feature gate in Cargo.toml ✅ (2026-02-05)
- [ ] `open_table()` via `deltalake::open_table_with_storage_options()`
- [ ] `flush_buffer_delta()` via `WriteBuilder`
- [ ] `commit_delta()` with `txn` action metadata
- [ ] Partitioned writes
- [ ] MERGE for upsert mode
- [ ] `ResolvedStorageOptions` integration
- [ ] Schema discovery and validation
- [ ] Unit tests passing (8+ tests)
- [ ] Integration tests passing (4+ tests)
- [ ] Performance benchmarks meeting targets
- [ ] Documentation updated

## References

- [F031: Delta Lake Sink](F031-delta-lake-sink.md)
- [F-CLOUD-001: Storage Credential Resolver](cloud/F-CLOUD-001-credential-resolver.md)
- [delta-rs write API](https://docs.rs/deltalake/latest/deltalake/write/index.html)
- [delta-rs protocol actions](https://docs.rs/deltalake/latest/deltalake/protocol/index.html)
- [Delta Lake transaction protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
