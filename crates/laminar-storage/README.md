# laminar-storage

Storage layer for LaminarDB -- WAL, checkpointing, and recovery.

## Overview

Ring 1 crate handling all durability concerns. Provides write-ahead logging with per-core segments, incremental checkpointing with directory-based snapshots, and checkpoint manifests. Designed to never block Ring 0 operations.

Note: Lakehouse sinks (Delta Lake, Iceberg) are in `laminar-connectors`, not here. This crate handles LaminarDB's internal durability.

## Key Modules

| Module | Purpose |
|--------|---------|
| `wal` | Write-ahead log with CRC32C checksums, torn write detection, fdatasync |
| `per_core_wal` | Per-core WAL segments for lock-free per-core writers, epoch ordering |
| `incremental` | Incremental checkpointing with directory-based delta snapshots |
| `checkpoint` | Checkpoint manager, object-store checkpointer, checkpoint layout |
| `checkpoint_manifest` | `CheckpointManifest`, `ConnectorCheckpoint`, `OperatorCheckpoint` |
| `checkpoint_store` | `CheckpointStore` trait, `FileSystemCheckpointStore` (atomic writes) |
| `changelog_drainer` | Ring 1 SPSC changelog consumer |
| `wal_state_store` | Combines state store with WAL for durability |
| `io_uring_wal` | io_uring-backed WAL for Linux 5.10+ (feature-gated) |

## Key Types

- **`WriteAheadLog`** -- Core WAL with CRC32C checksums and torn write detection
- **`PerCoreWalManager`** / **`CoreWalWriter`** -- Per-core WAL segments (one per CPU core)
- **`IncrementalCheckpointManager`** -- Directory-based incremental checkpointing
- **`ObjectStoreCheckpointer`** -- Cloud object store checkpoint backend
- **`CheckpointManifest`** -- Serializable snapshot of all operator and connector state
- **`RecoveryManager`** -- Restores state from checkpoint + WAL replay
- **`ChangelogDrainer`** -- Consumes Ring 0 changelog entries via SPSC queues
- **`CheckpointCoordinator`** -- Coordinates per-core WAL checkpoint epochs

## Checkpoint Architecture

```
Ring 0                          Ring 1
+------------------+            +------------------+
| ChangelogAware   |  SPSC -->  | Changelog        |
| StateStore       |            | Drainer          |
+------------------+            +--------+---------+
                                         |
                                         v
                                +--------+---------+
                                | Per-Core WAL     |
                                | (CRC32C, epoch)  |
                                +--------+---------+
                                         |
                                         v
                                +--------+---------+
                                | Incremental      |
                                | Checkpoint Mgr   |
                                +--------+---------+
                                         |
                                         v
                                +--------+---------+
                                | Checkpoint Store  |
                                | (FS / Object)    |
                                +------------------+
```

## Benchmarks

```bash
cargo bench -p laminar-storage --bench wal_bench          # WAL write throughput
cargo bench -p laminar-storage --bench checkpoint_bench   # Checkpoint/recovery time
```

## Feature Flags

| Flag | Purpose |
|------|---------|
| `io-uring` | io_uring-backed WAL (Linux 5.10+ only) |

## Related Crates

- [`laminar-core`](../laminar-core) -- Ring 0 state stores that produce changelog entries
- [`laminar-db`](../laminar-db) -- Checkpoint coordinator and recovery manager
