# laminar-storage

Checkpoint persistence and object store integration for LaminarDB.

## Overview

Durability layer: checkpoint manifests, filesystem/object-store persistence, and recovery validation. Never blocks the hot path.

Note: Lakehouse connectors (Delta Lake) are in `laminar-connectors`, not here. This crate handles LaminarDB's internal durability.

## Key Modules

| Module | Purpose |
|--------|---------|
| `checkpoint_manifest` | `CheckpointManifest`, `ConnectorCheckpoint`, `OperatorCheckpoint` |
| `checkpoint_store` | `CheckpointStore` trait, `FileSystemCheckpointStore`, `ObjectStoreCheckpointStore` |
| `object_store_builder` | Factory for S3, GCS, Azure, or local object store backends |

## Key Types

- **`CheckpointManifest`** -- Serializable snapshot of all operator and connector state
- **`CheckpointStore`** -- Trait for atomic manifest persistence (filesystem or cloud)
- **`FileSystemCheckpointStore`** -- Local filesystem with atomic temp-file + rename
- **`ObjectStoreCheckpointStore`** -- Cloud object stores (S3, GCS, Azure)

## Checkpoint Architecture

```
Streaming Coordinator           Background I/O
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
and source offsets directly.

## Feature Flags

| Flag | Purpose |
|------|---------|
| `aws` | S3 backend for checkpoints |
| `gcs` | Google Cloud Storage backend |
| `azure` | Azure Blob Storage backend |

## Related Crates

- [`laminar-db`](../laminar-db) -- Checkpoint coordinator and recovery manager
- [`laminar-connectors`](../laminar-connectors) -- Lakehouse sinks (Delta Lake, Iceberg)
