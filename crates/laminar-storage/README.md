# laminar-storage

Storage layer for LaminarDB -- WAL, checkpointing, and lakehouse integration.

## Overview

Ring 1 crate handling all durability concerns. Provides write-ahead logging with per-core segments, incremental checkpointing with RocksDB, and Delta Lake/Iceberg sink backends. Designed to never block Ring 0 operations.

## Key Modules

| Module | Purpose |
|--------|---------|
| `wal` | Write-ahead log with CRC32C checksums, torn write detection, fdatasync |
| `per_core_wal` | Per-core WAL segments for lock-free per-core writers |
| `incremental` | Incremental checkpointing with RocksDB backend |
| `checkpoint_manifest` | `CheckpointManifest`, `ConnectorCheckpoint`, `OperatorCheckpoint` |
| `checkpoint_store` | `CheckpointStore` trait, `FileSystemCheckpointStore` (atomic writes) |
| `changelog_drainer` | Ring 1 SPSC changelog consumer |

## Feature Flags

| Flag | Purpose |
|------|---------|
| `rocksdb` (default) | RocksDB-backed incremental checkpointing |
| `delta` | Delta Lake integration via delta_kernel |
| `io-uring` | Forwards to laminar-core io_uring support |

## Benchmarks

```bash
cargo bench -p laminar-storage --bench wal_bench          # WAL write throughput
cargo bench -p laminar-storage --bench checkpoint_bench   # Checkpoint/recovery time
```

## Related Crates

- [`laminar-core`](../laminar-core) -- Ring 0 state stores that produce changelog entries
- [`laminar-db`](../laminar-db) -- Checkpoint coordinator and recovery manager
