# F-CONN-002D: RocksDB-Backed Persistent Table Store

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CONN-002D |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | XL (8-12 days) |
| **Dependencies** | F-CONN-002C (PARTIAL Cache), F022 (Incremental Checkpointing), F069 (Three-Ring I/O) |
| **Crate** | laminar-db, laminar-storage |
| **Created** | 2026-02-06 |
| **ADR** | [ADR-005: Tiered Reference Tables](../../adr/ADR-005-tiered-reference-tables.md) |

## Summary

Replace the in-memory `HashMap<String, RecordBatch>` backing store in
`TableStore` with an optional RocksDB-backed persistent store for reference
tables that exceed available memory. Data persists across restarts, eliminating
re-snapshot from source on recovery. Uses the existing F022 RocksDB
infrastructure and integrates with Three-Ring I/O (F069) for non-blocking
point lookups from Ring 0.

## Motivation

The PARTIAL cache mode (F-CONN-002C) enables reference tables larger than memory
by routing cache misses to an async lookup. But the backing store is still the
in-memory `TableStore` — meaning large tables still OOM. This feature provides
the disk-backed tier:

- **100M+ row reference tables** stored on SSD, not in memory
- **Persistent across restarts**: no re-snapshot from PostgreSQL/Kafka on recovery
- **RocksDB point-lookup optimized**: bloom filters per SST, block cache, direct I/O
- **Checkpoint = RocksDB checkpoint**: zero-copy hard-link snapshots

This completes the three-tier architecture from ADR-005:

```
Ring 0: LRU Cache (hot keys)     → < 500ns
Ring 1: RocksDB (warm/cold keys) → < 100us (SSD)
Source: Kafka/PG/MySQL (refresh)  → background CDC
```

## Current State

| Component | Status |
|-----------|--------|
| `TableStore` in-memory HashMap | Done (F-CONN-002) |
| F022 `IncrementalCheckpointer` (RocksDB) | Done |
| F069 Three-Ring I/O | Done |
| Ring 0 LRU + xor filter | F-CONN-002C (planned) |
| RocksDB column families per table | **NOT DONE** |
| Point-lookup optimized RocksDB config | **NOT DONE** |
| io_uring-based reads from Ring 0 | **NOT DONE** |
| RocksDB checkpoint integration | **NOT DONE** |
| Automatic spill threshold | **NOT DONE** |

## Design

### Storage Backend Abstraction

The `TableStore` gets a pluggable backend:

```rust
/// Storage backend for reference table rows.
pub(crate) enum TableBackend {
    /// All rows in memory (default for small tables).
    InMemory {
        rows: HashMap<String, RecordBatch>,
    },
    /// Rows stored in RocksDB column family (for large tables).
    Persistent {
        db: Arc<rocksdb::DB>,
        cf_name: String,
        schema: SchemaRef,
    },
}
```

The `TableState` struct is updated:

```rust
struct TableState {
    schema: SchemaRef,
    primary_key: String,
    pk_index: usize,
    backend: TableBackend,       // NEW: replaces `rows: HashMap`
    ready: bool,
    connector: Option<String>,
    row_count: usize,            // NEW: tracked explicitly (RocksDB doesn't have O(1) count)
}
```

### RocksDB Configuration

Optimized for point-lookup workload (not range scans or write-heavy):

```rust
fn table_store_rocksdb_options() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_max_open_files(256);

    // Point-lookup optimization
    opts.set_plain_table_factory(&PlainTableFactoryOptions {
        user_key_length: 0,     // Variable-length keys
        bloom_bits_per_key: 10, // Bloom filter for point lookups
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        ..Default::default()
    });

    // Block cache for hot keys (shared across column families)
    let cache = rocksdb::Cache::new_lru_cache(128 * 1024 * 1024); // 128MB
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&block_opts);

    // Compression: LZ4 for balance of speed and size
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

    // Direct I/O to bypass page cache (we manage our own cache)
    opts.set_use_direct_reads(true);

    opts
}
```

Each reference table gets its own RocksDB column family. This allows:
- Independent compaction per table
- Per-table bloom filter configuration
- Table-level checkpoints via RocksDB `create_checkpoint()`

### Row Serialization

Rows are stored as key-value pairs in RocksDB:

- **Key**: Primary key as UTF-8 bytes (same as the HashMap key)
- **Value**: Arrow IPC serialized `RecordBatch` (single row)

For point lookups, the overhead of IPC deserialization (~1us per row) is
acceptable since it happens in Ring 1, not Ring 0. The LRU cache in Ring 0
stores deserialized `RecordBatch` values.

### io_uring Integration

For PARTIAL cache mode, cache misses are served via io_uring to avoid
blocking Ring 1:

```rust
/// Ring 1 handler for cache miss lookups.
async fn handle_cache_miss(
    db: &rocksdb::DB,
    cf: &str,
    key: &str,
) -> Option<RecordBatch> {
    // RocksDB point lookup (< 100us on SSD with bloom filter)
    let cf_handle = db.cf_handle(cf)?;
    let value = db.get_cf(&cf_handle, key.as_bytes()).ok()??;

    // Deserialize Arrow IPC
    let cursor = std::io::Cursor::new(value);
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).ok()?;
    reader.into_iter().next()?.ok()
}
```

In the future, this can be further optimized by using io_uring for the
RocksDB read syscalls via the Three-Ring I/O architecture (F069). RocksDB
supports `Env` customization for I/O, and a custom `io_uring`-backed `Env`
would eliminate all blocking I/O from Ring 1.

### Automatic Backend Selection

Tables automatically use RocksDB when they exceed a configurable threshold:

```rust
const DEFAULT_SPILL_THRESHOLD: usize = 1_000_000;  // 1M rows

impl TableStore {
    fn maybe_spill_to_rocksdb(&mut self, name: &str) -> Result<(), DbError> {
        let state = self.tables.get_mut(name).ok_or(...)?;
        if let TableBackend::InMemory { ref rows } = state.backend {
            if rows.len() >= self.spill_threshold {
                // Migrate in-memory rows to RocksDB
                let cf = self.create_column_family(name)?;
                for (key, batch) in rows.drain() {
                    let serialized = serialize_batch(&batch)?;
                    self.db.put_cf(&cf, key.as_bytes(), &serialized)?;
                }
                state.backend = TableBackend::Persistent {
                    db: self.db.clone(),
                    cf_name: name.to_string(),
                    schema: state.schema.clone(),
                };
            }
        }
        Ok(())
    }
}
```

Can also be forced via DDL:

```sql
CREATE TABLE large_customers (
    id INT PRIMARY KEY, ...
) WITH (
    connector = 'postgres',
    storage = 'persistent',       -- Force RocksDB backend
    cache_mode = 'partial',
    cache_max_entries = 100000
);
```

### Checkpoint Integration

RocksDB provides native checkpoint support via hard-link snapshots:

```rust
impl TableStore {
    fn checkpoint_persistent_tables(&self, dir: &Path) -> Result<(), DbError> {
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        checkpoint.create_checkpoint(dir)?;
        Ok(())
    }
}
```

This integrates with the existing `PipelineCheckpointManager`:
- `PipelineCheckpoint` includes `table_store_checkpoint_path`
- On recovery, open RocksDB from checkpointed directory
- No re-snapshot needed — table data is persistent

### `to_record_batch()` for RocksDB Backend

The existing `to_record_batch()` method (used for DataFusion MemTable sync)
iterates all rows — expensive for large RocksDB-backed tables. For PARTIAL
mode tables, we skip MemTable sync entirely and provide a custom
`TableProvider` that routes queries through the cache/RocksDB path:

```rust
/// DataFusion TableProvider that reads from TableStore (cache + RocksDB).
pub struct ReferenceTableProvider {
    table_name: String,
    schema: SchemaRef,
    table_store: Arc<Mutex<TableStore>>,
}

impl TableProvider for ReferenceTableProvider {
    fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        // For full-table scans (SELECT * FROM table):
        // iterate RocksDB column family
        // For point lookups (WHERE pk = 'value'):
        // use cache + RocksDB point lookup
    }
}
```

This avoids the expensive `to_record_batch()` → MemTable cycle for large tables.

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `table_backend` | 8 | InMemory operations, Persistent operations, upsert/delete/lookup via RocksDB, row serialization round-trip |
| `rocksdb_config` | 4 | Point-lookup options, column family per table, bloom filter effectiveness, block cache sizing |
| `auto_spill` | 6 | Spill threshold detection, in-memory → RocksDB migration, forced persistent via DDL, data integrity after spill, row count tracking, drop table cleans CF |
| `checkpoint` | 5 | RocksDB checkpoint creation, recovery from checkpoint, no re-snapshot after recovery, checkpoint with mixed backends, prune old checkpoints |
| `table_provider` | 5 | Full scan from RocksDB, point lookup via provider, schema propagation, empty table scan, provider + LRU cache integration |
| `integration` | 4 | Large table end-to-end (insert 100K rows, verify spill, lookup, checkpoint/recover), persistent + CDC refresh, mixed FULL + persistent tables |
| **Total** | **~32** | |

## Files

### New Files
- `crates/laminar-db/src/table_backend.rs` — `TableBackend` enum, RocksDB serialization
- `crates/laminar-db/src/table_provider.rs` — `ReferenceTableProvider` for DataFusion

### Modified Files
- `crates/laminar-db/src/table_store.rs` — Replace `rows: HashMap` with `TableBackend`, add `maybe_spill_to_rocksdb()`
- `crates/laminar-db/src/db.rs` — Register `ReferenceTableProvider` instead of MemTable for persistent tables
- `crates/laminar-db/src/pipeline_checkpoint.rs` — Add RocksDB checkpoint path
- `crates/laminar-db/Cargo.toml` — RocksDB already a dependency (F022)

### Dependencies
- `rocksdb` crate (already in workspace via F022)
- `arrow-ipc` (already in workspace) for row serialization
