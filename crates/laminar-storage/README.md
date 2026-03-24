# laminar-storage

Storage layer for LaminarDB -- WAL, checkpointing, and recovery.

## Overview

Ring 1 durability: incremental checkpointing with directory-based snapshots, checkpoint manifests, and a single-writer WAL for the incremental subsystem. Never blocks Ring 0.

Note: Lakehouse connectors (Delta Lake) are in `laminar-connectors`, not here. This crate handles LaminarDB's internal durability.

## Key Modules

| Module | Purpose |
|--------|---------|
| `wal` | Write-ahead log with CRC32C checksums, torn write detection, fdatasync |
| `incremental` | Incremental checkpointing with directory-based delta snapshots |
| `checkpoint` | Checkpoint manager, object-store checkpointer, checkpoint layout |
| `checkpoint_manifest` | `CheckpointManifest`, `ConnectorCheckpoint`, `OperatorCheckpoint` |
| `checkpoint_store` | `CheckpointStore` trait, `FileSystemCheckpointStore` (atomic writes) |
| `changelog_drainer` | Ring 1 SPSC changelog consumer |

## Key Types

- **`WriteAheadLog`** -- Core WAL with CRC32C checksums and torn write detection
- **`IncrementalCheckpointManager`** -- Directory-based incremental checkpointing
- **`ObjectStoreCheckpointer`** -- Cloud object store checkpoint backend
- **`CheckpointManifest`** -- Serializable snapshot of all operator and connector state
- **`RecoveryManager`** -- Restores state from checkpoint manifests
- **`ChangelogDrainer`** -- Consumes Ring 0 changelog entries via SPSC queues

## Checkpoint Architecture

```
Ring 0                          Ring 1
+------------------+            +------------------+
| Operators        |  snapshot  | Checkpoint       |
| (FxHashMap state)|  -------> | Coordinator      |
+------------------+            +--------+---------+
                                         |
                                         v
                                +--------+---------+
                                | Checkpoint Store  |
                                | (FS / Object)    |
                                +------------------+
```

Recovery loads the latest `CheckpointManifest` and restores operator state
and source offsets directly -- no WAL replay.

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
