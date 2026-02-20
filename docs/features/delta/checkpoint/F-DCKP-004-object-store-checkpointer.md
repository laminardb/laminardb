# F-DCKP-004: Object Store Checkpointer

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-004 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-DCKP-003 (Object Store Layout), F-STATE-001 (State Store Interface) |
| **Blocks** | F-DCKP-005 (Recovery Manager), F-DCKP-008 (Distributed Coordination) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-storage` |
| **Module** | `laminar-storage/src/checkpoint/checkpointer.rs` |

## Summary

Implements the `Checkpointer` trait backed by the `object_store` crate, providing a unified interface for saving, loading, listing, and garbage-collecting checkpoints across S3, GCS, Azure Blob Storage, and local filesystems. The `ObjectStoreCheckpointer` uploads operator state snapshots (rkyv-serialized) and source offsets to object storage following the layout defined in F-DCKP-003, then writes the manifest as the atomic commit marker. It supports concurrent uploads with configurable parallelism, integrity verification via SHA-256, and automatic retry with exponential backoff.

## Goals

- Define the `Checkpointer` trait as the storage abstraction for all checkpoint backends
- Implement `ObjectStoreCheckpointer` using the `object_store` crate for S3/GCS/local
- `save()`: Upload operator snapshots and source offsets, then write manifest atomically
- `load()`: Download a checkpoint's manifest and snapshot data
- `list()`: List all valid checkpoints sorted by time (newest first)
- `gc()`: Retain the last N checkpoints, delete the rest
- Support concurrent uploads with configurable parallelism (default: 8)
- SHA-256 integrity verification on upload and download
- Automatic retry with exponential backoff for transient failures
- Ring 2 (async) operation: all I/O is async, never blocks the hot path

## Non-Goals

- Incremental checkpoint delta computation (covered by F-DCKP-007)
- Checkpoint encryption (covered by F044)
- Recovery orchestration (covered by F-DCKP-005)
- Barrier protocol and injection (covered by F-DCKP-001)
- RocksDB or other embedded storage backends (future work)

## Technical Design

### Architecture

**Ring**: Ring 2 (Async I/O) -- all object store operations are async.

**Crate**: `laminar-storage`

**Module**: `laminar-storage/src/checkpoint/checkpointer.rs`

The `ObjectStoreCheckpointer` sits in Ring 2 and is invoked by the checkpoint coordinator after all operators have completed their state snapshots. It takes the in-memory `CheckpointData` and persists it to object storage. During recovery, it downloads the checkpoint data and provides it to the `RecoveryManager` (F-DCKP-005).

```
┌─────────────────────────────────────────────────────────────────┐
│  Checkpoint Coordinator (Ring 1)                                 │
│                                                                  │
│  on_all_barriers_aligned():                                      │
│    1. Collect operator snapshots                                 │
│    2. Collect source offsets                                     │
│    3. Build CheckpointData                                       │
│    4. Spawn Ring 2 task:                                         │
│       checkpointer.save(checkpoint_data).await                  │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  ObjectStoreCheckpointer (Ring 2)                          │  │
│  │                                                            │  │
│  │  save():                                                   │  │
│  │    ├── Upload operator/{op_id}/{part_id}.snap  (parallel)  │  │
│  │    ├── Upload sources/{source_id}.offsets       (parallel)  │  │
│  │    ├── Write _manifest.tmp                                 │  │
│  │    └── Rename to manifest.json  (atomic commit)            │  │
│  │                                                            │  │
│  │  Object Store (S3 / GCS / Local)                           │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use crate::checkpoint::manifest::*;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;

/// Aggregated checkpoint data to be persisted.
#[derive(Debug, Clone)]
pub struct CheckpointData {
    /// Unique checkpoint identifier.
    pub id: CheckpointId,

    /// Monotonically increasing epoch.
    pub epoch: u64,

    /// Epoch of the partition owner at checkpoint time.
    /// Used for fencing on recovery.
    ///
    /// AUDIT FIX (H8): Explicitly tracks the PartitionGuard's epoch
    /// so the manifest can be validated against the current epoch on load.
    pub owner_epoch: u64,

    /// Node that owned the partition at checkpoint time.
    ///
    /// AUDIT FIX (H8): Enables identifying the checkpoint author for
    /// debugging and split-brain detection.
    pub owner_node_id: NodeId,

    /// Operator state snapshots: operator_id -> partition_id -> serialized bytes.
    /// Files may be full snapshots (.snap) or incremental deltas (.delta).
    pub operator_snapshots: HashMap<OperatorId, HashMap<PartitionId, SnapshotData>>,

    /// Source offsets at checkpoint time.
    pub source_offsets: HashMap<SourceId, SourceOffset>,

    /// Timestamp when the checkpoint was initiated.
    pub timestamp: DateTime<Utc>,

    /// Previous checkpoint ID for incremental chaining.
    pub previous_checkpoint_id: Option<CheckpointId>,

    /// Whether this was an unaligned checkpoint.
    pub is_unaligned: bool,
}

/// Snapshot data for a single operator partition — either full or incremental.
///
/// AUDIT FIX (C1): Supports both full snapshots and incremental deltas.
#[derive(Debug, Clone)]
pub enum SnapshotData {
    /// Full state snapshot (rkyv-serialized StateSnapshot).
    Full(Vec<u8>),
    /// Incremental delta since the base epoch (rkyv-serialized IncrementalSnapshot).
    Delta { bytes: Vec<u8>, base_epoch: u64 },
}

/// Result of a garbage collection operation.
#[derive(Debug, Clone)]
pub struct GcResult {
    /// Number of checkpoints deleted.
    pub deleted_count: usize,
    /// Number of checkpoints retained.
    pub retained_count: usize,
    /// Total bytes reclaimed.
    pub bytes_reclaimed: u64,
    /// Any checkpoints that failed to delete.
    pub failed_deletions: Vec<(CheckpointId, String)>,
}

/// The Checkpointer trait: abstraction over checkpoint storage backends.
///
/// All methods are async and intended for Ring 2 execution.
#[async_trait]
pub trait Checkpointer: Send + Sync {
    /// Save a checkpoint to storage.
    ///
    /// Uploads all operator snapshots and source offsets, then writes
    /// the manifest as the atomic commit marker.
    ///
    /// Returns the checkpoint ID on success.
    async fn save(&self, checkpoint: CheckpointData) -> Result<CheckpointId, CheckpointError>;

    /// List all valid checkpoints, sorted by time (newest first).
    ///
    /// Only checkpoints with a valid manifest.json are returned.
    async fn list(&self) -> Result<Vec<CheckpointManifest>, CheckpointError>;

    /// Load a specific checkpoint by ID.
    ///
    /// Downloads the manifest and all snapshot data.
    async fn load(&self, id: &CheckpointId) -> Result<CheckpointData, CheckpointError>;

    /// Garbage-collect old checkpoints, retaining the newest `retain` count.
    ///
    /// Returns the number of checkpoints deleted.
    async fn gc(&self, retain: usize) -> Result<GcResult, CheckpointError>;

    /// Load only the manifest for a specific checkpoint (without snapshot data).
    async fn load_manifest(&self, id: &CheckpointId) -> Result<CheckpointManifest, CheckpointError>;

    /// Load the latest valid checkpoint.
    ///
    /// Reads the `_latest` pointer; falls back to listing if pointer is stale.
    async fn load_latest(&self) -> Result<Option<CheckpointData>, CheckpointError>;
}
```

### Data Structures

```rust
/// Configuration for the ObjectStoreCheckpointer.
#[derive(Debug, Clone)]
pub struct ObjectStoreCheckpointerConfig {
    /// Maximum concurrent uploads during a checkpoint save.
    pub max_concurrent_uploads: usize,

    /// Maximum concurrent downloads during a checkpoint load.
    pub max_concurrent_downloads: usize,

    /// Maximum number of retry attempts for transient failures.
    pub max_retries: u32,

    /// Base delay for exponential backoff (doubles on each retry).
    pub retry_base_delay: std::time::Duration,

    /// Maximum delay between retries.
    pub retry_max_delay: std::time::Duration,

    /// Whether to verify SHA-256 integrity on download.
    pub verify_integrity: bool,

    /// Whether to compute and store SHA-256 on upload.
    pub compute_checksum: bool,

    /// Multipart upload threshold (bytes). Files larger than this
    /// use multipart upload for better reliability.
    pub multipart_threshold: usize,

    /// Multipart upload chunk size.
    pub multipart_chunk_size: usize,
}

impl Default for ObjectStoreCheckpointerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: 8,
            max_concurrent_downloads: 8,
            max_retries: 3,
            retry_base_delay: std::time::Duration::from_millis(100),
            retry_max_delay: std::time::Duration::from_secs(10),
            verify_integrity: true,
            compute_checksum: true,
            multipart_threshold: 8 * 1024 * 1024, // 8 MiB
            multipart_chunk_size: 8 * 1024 * 1024, // 8 MiB
        }
    }
}

/// The ObjectStoreCheckpointer implementation.
pub struct ObjectStoreCheckpointer {
    /// The object store backend (S3, GCS, local, etc.).
    store: Arc<dyn ObjectStore>,

    /// Path builder for constructing object store paths.
    paths: CheckpointPaths,

    /// Configuration.
    config: ObjectStoreCheckpointerConfig,
}

impl ObjectStoreCheckpointer {
    /// Create a new checkpointer with the given object store and base path.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        base_path: impl Into<String>,
        config: ObjectStoreCheckpointerConfig,
    ) -> Self {
        Self {
            store,
            paths: CheckpointPaths::new(base_path),
            config,
        }
    }

    /// Upload a single snapshot file with retry logic.
    async fn upload_with_retry(
        &self,
        path: &object_store::path::Path,
        data: Bytes,
    ) -> Result<String, CheckpointError> {
        let sha256 = if self.config.compute_checksum {
            use sha2::{Sha256, Digest};
            let mut hasher = Sha256::new();
            hasher.update(&data);
            hex::encode(hasher.finalize())
        } else {
            String::new()
        };

        let mut attempt = 0;
        let mut delay = self.config.retry_base_delay;

        loop {
            match self.store.put(path, data.clone().into()).await {
                Ok(_) => return Ok(sha256),
                Err(e) if attempt < self.config.max_retries => {
                    attempt += 1;
                    tracing::warn!(
                        path = %path,
                        attempt,
                        error = %e,
                        "checkpoint upload failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(self.config.retry_max_delay);
                }
                Err(e) => {
                    return Err(CheckpointError::UploadFailed {
                        path: path.to_string(),
                        source: Box::new(e),
                    });
                }
            }
        }
    }

    /// Download a file from object store with retry logic.
    async fn download_with_retry(
        &self,
        path: &object_store::path::Path,
    ) -> Result<Bytes, CheckpointError> {
        let mut attempt = 0;
        let mut delay = self.config.retry_base_delay;

        loop {
            match self.store.get(path).await {
                Ok(result) => {
                    let data = result.bytes().await.map_err(|e| {
                        CheckpointError::DownloadFailed {
                            path: path.to_string(),
                            source: Box::new(e),
                        }
                    })?;
                    return Ok(data);
                }
                Err(e) if attempt < self.config.max_retries => {
                    attempt += 1;
                    tracing::warn!(
                        path = %path,
                        attempt,
                        error = %e,
                        "checkpoint download failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(self.config.retry_max_delay);
                }
                Err(e) => {
                    return Err(CheckpointError::DownloadFailed {
                        path: path.to_string(),
                        source: Box::new(e),
                    });
                }
            }
        }
    }
}

/// Errors from checkpoint operations.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// Failed to upload a snapshot file.
    #[error("failed to upload {path}: {source}")]
    UploadFailed {
        path: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Failed to download a snapshot file.
    #[error("failed to download {path}: {source}")]
    DownloadFailed {
        path: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Manifest write failed.
    #[error("failed to write manifest: {0}")]
    ManifestWriteFailed(String),

    /// Manifest parse error.
    #[error("failed to parse manifest: {0}")]
    ManifestParseError(#[from] serde_json::Error),

    /// Checkpoint not found.
    #[error("checkpoint not found: {0}")]
    NotFound(String),

    /// Integrity check failed.
    #[error("integrity check failed for {path}: expected {expected}, got {actual}")]
    IntegrityCheckFailed {
        path: String,
        expected: String,
        actual: String,
    },

    /// Layout error.
    #[error("layout error: {0}")]
    LayoutError(#[from] CheckpointLayoutError),

    /// Object store error.
    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    /// Checkpoint is corrupt or incomplete.
    #[error("corrupt checkpoint {id}: {reason}")]
    CorruptCheckpoint { id: String, reason: String },

    /// Timeout during checkpoint operation.
    #[error("checkpoint operation timed out after {0:?}")]
    Timeout(std::time::Duration),
}
```

### Algorithm/Flow

#### Save Flow

```
checkpointer.save(data, guard):

AUDIT FIX (H9): save() now accepts a PartitionGuard reference for
epoch re-validation after upload completes.

1. Generate paths for all snapshot files
   - Full snapshots → .snap extension
   - Incremental deltas → .delta extension (AUDIT FIX C1)
2. Spawn concurrent upload tasks (bounded by max_concurrent_uploads):
   For each (operator_id, partitions) in data.operator_snapshots:
     For each (partition_id, snapshot_data) in partitions:
       a. extension = match snapshot_data { Full → ".snap", Delta → ".delta" }
       b. path = paths.snapshot_path(id, operator_id, partition_id, extension)
       c. sha256 = upload_with_retry(path, snapshot_data.bytes())
       d. Record PartitionSnapshotEntry { partition_id, path, size, sha256, is_delta }
3. Upload source offsets (in parallel with snapshots):
   For each (source_id, offset) in data.source_offsets:
     a. Serialize offset as JSON
     b. path = paths.offset_path(id, source_id)
     c. upload_with_retry(path, json_bytes)
4. Await all uploads, collect results
5. If any upload failed → return error (checkpoint is incomplete)
6. AUDIT FIX (H9): Re-validate epoch before writing manifest:
   a. guard.validate(metadata)?
   b. If StaleEpoch → delete all uploaded files, return EpochError
   c. This prevents a stale node from committing a checkpoint after
      ownership was revoked during the upload window
7. Build CheckpointManifest from collected entries
   - Include owner_epoch and owner_node_id from data (AUDIT FIX H8)
8. Serialize manifest to JSON
9. Upload manifest:
   AUDIT FIX (H6): S3 does not support atomic rename. The manifest
   is written directly to manifest.json using PUT (not copy+delete).
   This is safe because:
   - Only one checkpoint coordinator writes to a given checkpoint ID path
   - Epoch fencing (C5) prevents stale coordinators from writing
   a. PUT manifest.json directly (not temp+rename)
   b. For local filesystem: write to .tmp + rename (truly atomic)
10. Update _latest pointer file
11. Return CheckpointId
```

#### Checkpoint Commit Protocol

> **AUDIT FIX (C9):** This section documents the end-to-end commit flow
> that links object storage persistence (this spec) with Raft metadata
> registration (F-COORD-001). Without this linkage, a checkpoint could
> exist in object storage but be invisible to recovery.

```
Full checkpoint commit protocol (orchestrated by checkpoint coordinator):

1. Collect operator snapshots and source offsets from all partitions
2. Build CheckpointData with owner_epoch and owner_node_id from PartitionGuard
3. Call checkpointer.save(checkpoint_data, guard) → persists to object storage
4. On success: call raft.commit_checkpoint(partition_id, checkpoint_ref)
   → records in DeltaMetadata so RecoveryManager can find it
5. On failure: log error, abort checkpoint. Object storage GC will clean up
   the incomplete checkpoint (no manifest = GC-eligible).

Recovery safety:
- Crash after step 3 but before step 4:
  The checkpoint is in object storage WITH a manifest (step 9 writes it),
  but NOT in Raft metadata.
  RecoveryManager::load_latest() reads the _latest pointer OR lists
  object storage directly → finds the checkpoint even without Raft.
  This is safe because the checkpoint is self-contained.

- Crash during step 3:
  Incomplete checkpoint (no manifest). GC deletes it. Recovery uses
  the previous checkpoint from metadata.

- Stale node attempts step 3:
  Epoch re-validation (step 6) rejects the upload. Files already uploaded
  are orphaned and cleaned by GC.
```

#### Load Flow

```
checkpointer.load(id):

1. Download manifest.json for the given checkpoint_id
2. Parse JSON → CheckpointManifest
3. Validate manifest (version, integrity)
4. Spawn concurrent download tasks (bounded by max_concurrent_downloads):
   For each operator in manifest.operators:
     For each partition in operator.partitions:
       a. Download snapshot file
       b. If verify_integrity: compute SHA-256, compare with manifest
       c. Collect into HashMap<OperatorId, HashMap<PartitionId, Vec<u8>>>
5. Download source offsets:
   For each source in manifest.sources:
     a. Download offset file
     b. Parse JSON → SourceOffset
6. Await all downloads
7. Build CheckpointData from downloaded data
8. Return CheckpointData
```

#### Garbage Collection Flow

```
checkpointer.gc(retain):

1. List all checkpoint directories
2. For each: check if manifest.json exists
3. Sort valid checkpoints by CheckpointId (time order)
4. Keep the newest `retain` checkpoints
5. For each checkpoint to delete:
   a. List all objects with checkpoint prefix
   b. Delete all objects
   c. Track bytes reclaimed
6. For incomplete checkpoints (no manifest, older than 1 hour):
   a. Delete all objects
7. Return GcResult { deleted_count, retained_count, bytes_reclaimed }
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `UploadFailed` | Network error, S3 rate limiting, permissions | Retry with exponential backoff (3 attempts); abort checkpoint on exhaustion |
| `DownloadFailed` | Network error, object not found | Retry with backoff; fall back to previous checkpoint on exhaustion |
| `IntegrityCheckFailed` | Data corruption in transit or at rest | Re-download; if persistent, mark checkpoint as corrupt |
| `ManifestWriteFailed` | Failed to write manifest (permissions, quota) | Retry; leave incomplete checkpoint for GC |
| `NotFound` | Requested checkpoint ID does not exist | Return error; caller falls back to listing |
| `Timeout` | Operation exceeded deadline | Cancel in-flight uploads/downloads, return error |
| `CorruptCheckpoint` | Manifest references missing files or invalid data | Skip checkpoint, log error, try previous |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Checkpoint save (10 MB state, S3) | < 2s | Integration benchmark |
| Checkpoint save (100 MB state, S3) | < 10s | Integration benchmark |
| Checkpoint save (1 GB state, S3) | < 60s | Integration benchmark |
| Checkpoint load (10 MB state, S3) | < 1s | Integration benchmark |
| Checkpoint list (100 checkpoints, S3) | < 500ms | Integration benchmark |
| GC (delete 10 checkpoints, S3) | < 5s | Integration benchmark |
| Upload parallelism overhead | < 5% vs sequential for 1 file | Benchmark comparison |
| SHA-256 throughput | > 500 MB/s | `bench_sha256_throughput` |

## Test Plan

### Unit Tests

- [ ] `test_checkpointer_config_defaults` - Verify default configuration values
- [ ] `test_upload_with_retry_succeeds_first_attempt` - No retry needed
- [ ] `test_upload_with_retry_succeeds_after_retry` - Transient failure then success
- [ ] `test_upload_with_retry_exhausts_retries` - All retries fail, returns error
- [ ] `test_download_with_retry_succeeds` - Successful download
- [ ] `test_download_with_retry_integrity_check` - SHA-256 verified
- [ ] `test_sha256_computation_correct` - Known hash value
- [ ] `test_checkpoint_data_builder` - Build CheckpointData struct

### Integration Tests

- [ ] `test_save_and_load_roundtrip_local` - Save then load on local filesystem
- [ ] `test_save_and_load_roundtrip_memory` - Save then load with in-memory object store
- [ ] `test_save_multiple_operators_and_partitions` - Complex checkpoint structure
- [ ] `test_list_returns_newest_first` - Time-sorted listing
- [ ] `test_list_skips_incomplete_checkpoints` - No manifest = not listed
- [ ] `test_gc_retains_newest_n` - GC deletes old, keeps new
- [ ] `test_gc_cleans_incomplete_checkpoints` - Incomplete checkpoints removed
- [ ] `test_load_latest_follows_pointer` - _latest pointer is read
- [ ] `test_load_latest_falls_back_to_listing` - Stale pointer handled
- [ ] `test_concurrent_save_does_not_corrupt` - Two saves interleaved
- [ ] `test_integrity_check_catches_corruption` - Tampered data detected
- [ ] `test_save_with_previous_checkpoint_id` - Incremental chain reference

### Benchmarks

- [ ] `bench_save_10mb_local` - Target: < 100ms (local filesystem)
- [ ] `bench_save_100mb_local` - Target: < 1s (local filesystem)
- [ ] `bench_load_10mb_local` - Target: < 50ms (local filesystem)
- [ ] `bench_list_100_checkpoints_local` - Target: < 50ms (local filesystem)
- [ ] `bench_sha256_computation` - Target: > 500 MB/s

## Rollout Plan

1. **Phase 1**: `Checkpointer` trait definition + `CheckpointData` types
2. **Phase 2**: `ObjectStoreCheckpointer` with `save()` using local filesystem
3. **Phase 3**: `load()` and `load_manifest()` implementation
4. **Phase 4**: `list()` and `gc()` implementation
5. **Phase 5**: Retry logic and integrity verification
6. **Phase 6**: S3/GCS integration testing (requires credentials)
7. **Phase 7**: Concurrent upload/download with semaphore
8. **Phase 8**: Benchmarks + optimization
9. **Phase 9**: Documentation and code review

## Open Questions

- [ ] Should we support streaming uploads for very large snapshots (> 1 GB)? The `object_store` crate supports multipart uploads, which could be leveraged.
- [ ] Should `gc()` be automatic (run after every N checkpoints) or manual? Leaning toward a configurable auto-GC with manual override.
- [ ] Should we support checkpoint compression (e.g., zstd) transparently? This would reduce storage costs and upload time for compressible state.
- [ ] Should the `_latest` pointer use conditional writes (ETags) for consistency in multi-writer scenarios?

## Completion Checklist

- [ ] `Checkpointer` trait defined
- [ ] `ObjectStoreCheckpointer` implemented with save/load/list/gc
- [ ] Retry logic with exponential backoff
- [ ] SHA-256 integrity verification
- [ ] Concurrent upload/download with bounded parallelism
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing (local filesystem)
- [ ] S3 integration test (optional, requires credentials)
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [object_store crate](https://docs.rs/object_store/latest/object_store/) - Backend-agnostic object storage API
- [F-DCKP-003: Object Store Layout](F-DCKP-003-object-store-layout.md) - Directory layout and manifest schema
- [F-DCKP-005: Recovery Manager](F-DCKP-005-recovery-manager.md) - Consumes loaded checkpoint data
- [F-STATE-001: State Store Interface](../../phase-1/F003-state-store-interface.md) - State store providing snapshot data
- [Apache Flink State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/) - Reference architecture
