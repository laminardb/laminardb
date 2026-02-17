# F-DCKP-003: Object Store Checkpoint Layout

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-003 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | None |
| **Blocks** | F-DCKP-004 (Object Store Checkpointer), F-DCKP-005 (Recovery Manager) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-storage` |
| **Module** | `laminar-storage/src/checkpoint/manifest.rs` |

## Summary

Defines the canonical directory layout and manifest schema for checkpoints stored in object storage (S3, GCS, Azure Blob, or local filesystem). Checkpoint IDs use UUID v7 for time-sortability, enabling efficient listing and garbage collection. The manifest file serves as the atomic commit marker -- a checkpoint is only valid when its `manifest.json` exists. Operator state snapshots are serialized with rkyv for zero-copy deserialization during recovery. Source offsets are stored separately to enable fast seek-without-restore workflows.

## Goals

- Define the object store directory layout for checkpoint data
- Define the `manifest.json` schema as the atomic commit marker
- Use UUID v7 (time-sortable) for checkpoint identifiers
- Support per-operator, per-partition snapshot storage
- Support per-source offset storage for replay positioning
- Enable atomic checkpoint commit via write-temp-then-rename pattern
- Support efficient listing, loading, and garbage collection of checkpoints
- Be storage-backend agnostic (S3, GCS, Azure, local filesystem)

## Non-Goals

- Implementing the upload/download logic (covered by F-DCKP-004)
- Implementing recovery orchestration (covered by F-DCKP-005)
- Incremental checkpoint deltas (AUDIT FIX C1: now included — see delta layout below; F-DCKP-007 superseded)
- Encryption of checkpoint data at rest (covered by F044)
- Compression algorithms (deferred; raw rkyv bytes initially)

## Technical Design

### Architecture

**Ring**: Ring 2 (Async I/O) -- object store operations are inherently async.

**Crate**: `laminar-storage`

**Module**: `laminar-storage/src/checkpoint/manifest.rs`

The checkpoint layout is designed for object stores that support atomic rename (or conditional put) and efficient prefix listing. The manifest file is the last object written during a checkpoint and serves as the commit marker. If a checkpoint directory exists without a manifest, it is considered incomplete and subject to garbage collection.

### Object Store Directory Layout

```
{base_path}/checkpoints/
├── {checkpoint-id-1}/                          # UUID v7, time-sortable
│   ├── manifest.json                           # Atomic commit marker (written last)
│   ├── operators/
│   │   ├── {operator-id-a}/
│   │   │   ├── {partition-0}.snap              # rkyv serialized FULL operator state
│   │   │   ├── {partition-0}.delta             # rkyv serialized INCREMENTAL delta (C1)
│   │   │   ├── {partition-1}.snap
│   │   │   └── {partition-2}.delta
│   │   └── {operator-id-b}/
│   │       ├── {partition-0}.delta
│   │       └── {partition-1}.snap
│   └── sources/
│       ├── {source-id-x}.offsets               # Source position data (JSON)
│       └── {source-id-y}.offsets
├── {checkpoint-id-2}/
│   ├── manifest.json
│   ├── operators/...
│   └── sources/...
└── _latest                                     # Pointer to latest valid checkpoint ID
```

#### Path Conventions

- `{base_path}`: Configurable root, e.g., `s3://my-bucket/laminardb/` or `/var/lib/laminardb/`
- `{checkpoint-id}`: UUID v7 string representation (e.g., `0192a3b4-c5d6-7e8f-9a0b-1c2d3e4f5a6b`)
- `{operator-id}`: Stable operator identifier from the query plan (e.g., `agg_window_01`)
- `{partition-id}`: Partition number (integer)
- `.snap`: rkyv-serialized binary **full** state snapshot (`StateSnapshot`)
- `.delta`: rkyv-serialized binary **incremental** state delta (`IncrementalSnapshot`) — AUDIT FIX (C1)
- `.offsets`: JSON-encoded source position data
- `_latest`: Single-line text file containing the checkpoint ID of the latest valid checkpoint

### API/Interface

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Unique checkpoint identifier based on UUID v7 for time-sortability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CheckpointId(pub uuid::Uuid);

impl CheckpointId {
    /// Generate a new time-sortable checkpoint ID.
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    /// Create from an existing UUID (for deserialization).
    pub const fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the string representation for use in object store paths.
    pub fn to_path_string(&self) -> String {
        self.0.to_string()
    }

    /// Parse from a path string.
    pub fn from_path_string(s: &str) -> Result<Self, CheckpointLayoutError> {
        uuid::Uuid::parse_str(s)
            .map(Self)
            .map_err(|e| CheckpointLayoutError::InvalidCheckpointId(e.to_string()))
    }

    /// Extract the timestamp from the UUID v7 for sorting/display.
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        let ts = self.0.get_timestamp()?;
        let (secs, nanos) = ts.to_unix();
        DateTime::from_timestamp(secs as i64, nanos).map(|dt| dt.with_timezone(&Utc))
    }
}

/// Typed operator identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperatorId(pub String);

/// Typed partition identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u32);

/// Typed source identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);

/// The checkpoint manifest -- the atomic commit marker.
///
/// A checkpoint is considered valid if and only if its manifest.json
/// exists in object storage. The manifest is the LAST file written
/// during a checkpoint; if the process crashes before writing the
/// manifest, the checkpoint is incomplete and will be cleaned up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointManifest {
    /// Schema version for forward compatibility.
    pub version: u32,

    /// Unique identifier for this checkpoint.
    pub checkpoint_id: CheckpointId,

    /// Monotonically increasing epoch number.
    pub epoch: u64,

    /// List of operator snapshots included in this checkpoint.
    pub operators: Vec<OperatorSnapshotEntry>,

    /// Source offset entries for replay positioning.
    pub sources: Vec<SourceOffsetEntry>,

    /// UTC timestamp when the checkpoint was initiated.
    pub started_at: DateTime<Utc>,

    /// UTC timestamp when the checkpoint was committed (manifest written).
    pub completed_at: DateTime<Utc>,

    /// Total size of all snapshot data in bytes.
    pub total_size_bytes: u64,

    /// ID of the previous checkpoint (for incremental chain).
    /// None if this is the first checkpoint or a full checkpoint.
    pub previous_checkpoint_id: Option<CheckpointId>,

    /// Whether this checkpoint was taken in unaligned mode.
    pub is_unaligned: bool,

    /// Optional metadata for extensibility.
    pub metadata: HashMap<String, String>,
}

impl CheckpointManifest {
    /// Current manifest schema version.
    pub const CURRENT_VERSION: u32 = 1;

    /// Create a new manifest builder.
    pub fn builder(checkpoint_id: CheckpointId, epoch: u64) -> CheckpointManifestBuilder {
        CheckpointManifestBuilder {
            checkpoint_id,
            epoch,
            operators: Vec::new(),
            sources: Vec::new(),
            started_at: Utc::now(),
            previous_checkpoint_id: None,
            is_unaligned: false,
            metadata: HashMap::new(),
        }
    }

    /// Validate the manifest for internal consistency.
    pub fn validate(&self) -> Result<(), CheckpointLayoutError> {
        if self.version != Self::CURRENT_VERSION {
            return Err(CheckpointLayoutError::UnsupportedVersion {
                expected: Self::CURRENT_VERSION,
                found: self.version,
            });
        }
        if self.operators.is_empty() && self.sources.is_empty() {
            return Err(CheckpointLayoutError::EmptyManifest);
        }
        if self.completed_at < self.started_at {
            return Err(CheckpointLayoutError::InvalidTimestamps);
        }
        Ok(())
    }
}

/// Builder for constructing a checkpoint manifest.
pub struct CheckpointManifestBuilder {
    checkpoint_id: CheckpointId,
    epoch: u64,
    operators: Vec<OperatorSnapshotEntry>,
    sources: Vec<SourceOffsetEntry>,
    started_at: DateTime<Utc>,
    previous_checkpoint_id: Option<CheckpointId>,
    is_unaligned: bool,
    metadata: HashMap<String, String>,
}

impl CheckpointManifestBuilder {
    /// Add an operator snapshot entry.
    pub fn add_operator(mut self, entry: OperatorSnapshotEntry) -> Self {
        self.operators.push(entry);
        self
    }

    /// Add a source offset entry.
    pub fn add_source(mut self, entry: SourceOffsetEntry) -> Self {
        self.sources.push(entry);
        self
    }

    /// Set the previous checkpoint ID for incremental chains.
    pub fn previous_checkpoint(mut self, id: CheckpointId) -> Self {
        self.previous_checkpoint_id = Some(id);
        self
    }

    /// Mark this checkpoint as unaligned.
    pub fn unaligned(mut self, is_unaligned: bool) -> Self {
        self.is_unaligned = is_unaligned;
        self
    }

    /// Add custom metadata.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Build the manifest.
    pub fn build(self) -> CheckpointManifest {
        let total_size: u64 = self.operators.iter()
            .flat_map(|op| op.partitions.iter())
            .map(|p| p.size_bytes)
            .sum();

        CheckpointManifest {
            version: CheckpointManifest::CURRENT_VERSION,
            checkpoint_id: self.checkpoint_id,
            epoch: self.epoch,
            operators: self.operators,
            sources: self.sources,
            started_at: self.started_at,
            completed_at: Utc::now(),
            total_size_bytes: total_size,
            previous_checkpoint_id: self.previous_checkpoint_id,
            is_unaligned: self.is_unaligned,
            metadata: self.metadata,
        }
    }
}
```

### Data Structures

```rust
/// Entry describing a single operator's snapshot data in the checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorSnapshotEntry {
    /// Operator identifier.
    pub operator_id: OperatorId,

    /// Per-partition snapshot details.
    pub partitions: Vec<PartitionSnapshotEntry>,

    /// The operator type (for validation during recovery).
    pub operator_type: String,

    /// State backend used (e.g., "mmap", "heap", "rocksdb").
    pub state_backend: String,
}

/// Entry describing a single partition's snapshot file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSnapshotEntry {
    /// Partition identifier.
    pub partition_id: PartitionId,

    /// Relative path to the snapshot file within the checkpoint directory.
    /// e.g., "operators/agg_01/0.snap"
    pub path: String,

    /// Size of the snapshot file in bytes.
    pub size_bytes: u64,

    /// SHA-256 hash of the snapshot file for integrity verification.
    pub sha256: String,

    /// Whether this is an incremental delta (vs full snapshot).
    pub is_incremental: bool,
}

/// Entry describing a source's offset position at checkpoint time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceOffsetEntry {
    /// Source identifier.
    pub source_id: SourceId,

    /// Source-specific offset data.
    pub offset: SourceOffset,

    /// Relative path to the offset file.
    pub path: String,
}

/// Source-specific offset information for replay positioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceOffset {
    /// Kafka source offset.
    Kafka {
        /// Topic-partition offsets.
        offsets: HashMap<String, HashMap<i32, i64>>,
    },
    /// File-based source offset.
    File {
        /// File path and byte offset.
        path: String,
        byte_offset: u64,
    },
    /// CDC source offset (Postgres, MySQL).
    Cdc {
        /// Replication slot name or binlog position.
        position: String,
        /// LSN or GTID.
        lsn: String,
    },
    /// Generic offset as opaque bytes.
    Opaque {
        /// Base64-encoded offset data.
        data: String,
    },
}

/// Path builder for constructing object store paths.
pub struct CheckpointPaths {
    base_path: String,
}

impl CheckpointPaths {
    /// Create a new path builder with the given base path.
    pub fn new(base_path: impl Into<String>) -> Self {
        let mut base = base_path.into();
        if !base.ends_with('/') {
            base.push('/');
        }
        Self { base_path: base }
    }

    /// Root checkpoints directory.
    pub fn checkpoints_root(&self) -> String {
        format!("{}checkpoints/", self.base_path)
    }

    /// Directory for a specific checkpoint.
    pub fn checkpoint_dir(&self, id: &CheckpointId) -> String {
        format!("{}checkpoints/{}/", self.base_path, id.to_path_string())
    }

    /// Path to the manifest file for a checkpoint.
    pub fn manifest_path(&self, id: &CheckpointId) -> String {
        format!("{}checkpoints/{}/manifest.json", self.base_path, id.to_path_string())
    }

    /// Path to a temporary manifest (used during atomic write).
    pub fn manifest_tmp_path(&self, id: &CheckpointId) -> String {
        format!("{}checkpoints/{}/_manifest.tmp", self.base_path, id.to_path_string())
    }

    /// Path to an operator's snapshot file for a partition.
    pub fn snapshot_path(
        &self,
        checkpoint_id: &CheckpointId,
        operator_id: &OperatorId,
        partition_id: &PartitionId,
    ) -> String {
        format!(
            "{}checkpoints/{}/operators/{}/{}.snap",
            self.base_path,
            checkpoint_id.to_path_string(),
            operator_id.0,
            partition_id.0,
        )
    }

    /// Path to a source's offset file.
    pub fn offset_path(
        &self,
        checkpoint_id: &CheckpointId,
        source_id: &SourceId,
    ) -> String {
        format!(
            "{}checkpoints/{}/sources/{}.offsets",
            self.base_path,
            checkpoint_id.to_path_string(),
            source_id.0,
        )
    }

    /// Path to the `_latest` pointer file.
    pub fn latest_path(&self) -> String {
        format!("{}checkpoints/_latest", self.base_path)
    }
}

/// Errors related to the checkpoint layout.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointLayoutError {
    /// Invalid checkpoint ID format.
    #[error("invalid checkpoint ID: {0}")]
    InvalidCheckpointId(String),

    /// Unsupported manifest version.
    #[error("unsupported manifest version: expected {expected}, found {found}")]
    UnsupportedVersion { expected: u32, found: u32 },

    /// Manifest has no operators and no sources.
    #[error("empty manifest: no operators or sources")]
    EmptyManifest,

    /// Timestamps are inconsistent.
    #[error("completed_at is before started_at")]
    InvalidTimestamps,

    /// Manifest JSON parsing error.
    #[error("manifest parse error: {0}")]
    ParseError(#[from] serde_json::Error),

    /// SHA-256 checksum mismatch.
    #[error("integrity check failed for {path}: expected {expected}, got {actual}")]
    IntegrityCheckFailed {
        path: String,
        expected: String,
        actual: String,
    },
}
```

### Algorithm/Flow

#### Checkpoint Write Flow (Atomic Commit)

```
1. Generate CheckpointId (UUID v7)
2. For each operator:
   a. For each partition:
      i.  Serialize state with rkyv → bytes
      ii. Compute SHA-256 hash
      iii. Upload to: {base}/checkpoints/{id}/operators/{op}/{part}.snap
3. For each source:
   a. Serialize offset as JSON
   b. Upload to: {base}/checkpoints/{id}/sources/{source}.offsets
4. Build CheckpointManifest with all entries
5. Serialize manifest to JSON
6. Write to temporary path: {base}/checkpoints/{id}/_manifest.tmp
7. Rename (or copy+delete) to: {base}/checkpoints/{id}/manifest.json
8. Update {base}/checkpoints/_latest with new checkpoint ID

Step 7 is the atomic commit point. If the process crashes before step 7,
the checkpoint is incomplete and will be cleaned up by garbage collection.
```

#### Checkpoint List Flow

```
1. List objects with prefix: {base}/checkpoints/
2. For each subdirectory (UUID v7 format):
   a. Check for manifest.json existence
   b. If present: parse manifest, add to result list
   c. If absent: skip (incomplete checkpoint)
3. Sort by CheckpointId (UUID v7 = time-sorted)
4. Return sorted list (newest first)
```

#### Garbage Collection Flow

```
Given: retain = N (number of checkpoints to keep)
1. List all valid checkpoints (those with manifest.json)
2. Sort by time (UUID v7 ordering)
3. Keep the newest N checkpoints
4. For each checkpoint to delete:
   a. Delete all objects with prefix: {base}/checkpoints/{id}/
   b. Log deletion
5. For incomplete checkpoints (no manifest.json):
   a. If older than grace period (default: 1 hour): delete all objects
   b. Otherwise: skip (may be in-progress)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `InvalidCheckpointId` | UUID string cannot be parsed | Skip entry during listing, log warning |
| `UnsupportedVersion` | Manifest version newer than this binary | Reject checkpoint, require binary upgrade |
| `EmptyManifest` | Manifest with no operator or source entries | Treat as invalid, skip during listing |
| `IntegrityCheckFailed` | SHA-256 mismatch on downloaded snapshot | Retry download; if persistent, mark checkpoint as corrupt |
| `ParseError` | Malformed JSON in manifest or offsets | Skip checkpoint, log error, fall back to previous |
| `InvalidTimestamps` | completed_at before started_at | Log warning, accept manifest (clock skew tolerance) |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Manifest serialization (JSON) | < 1ms for 100 operators | `bench_manifest_serialize` |
| Manifest deserialization (JSON) | < 1ms for 100 operators | `bench_manifest_deserialize` |
| Path construction | < 100ns per path | `bench_path_construction` |
| UUID v7 generation | < 50ns | `bench_uuid_generation` |
| Checkpoint listing (100 checkpoints) | < 500ms (S3) | Integration benchmark |
| Manifest file size | < 64 KiB for 1000 operators | Size validation test |

## Test Plan

### Unit Tests

- [ ] `test_checkpoint_id_new_is_time_sortable` - Two sequential IDs sort correctly
- [ ] `test_checkpoint_id_to_from_path_string` - Round-trip conversion
- [ ] `test_checkpoint_id_timestamp_extraction` - Embedded timestamp is correct
- [ ] `test_manifest_builder_basic` - Build a minimal manifest
- [ ] `test_manifest_builder_with_operators_and_sources` - Full manifest
- [ ] `test_manifest_builder_total_size_calculation` - Sum of partition sizes
- [ ] `test_manifest_validate_current_version` - Valid manifest passes
- [ ] `test_manifest_validate_wrong_version` - Wrong version fails
- [ ] `test_manifest_validate_empty` - Empty manifest fails
- [ ] `test_manifest_serialize_deserialize_roundtrip` - JSON round-trip
- [ ] `test_paths_checkpoint_dir` - Correct path construction
- [ ] `test_paths_manifest_path` - Correct manifest path
- [ ] `test_paths_snapshot_path` - Correct snapshot path
- [ ] `test_paths_offset_path` - Correct offset path
- [ ] `test_paths_latest_path` - Correct _latest path
- [ ] `test_paths_base_path_trailing_slash` - Handles trailing slash normalization
- [ ] `test_source_offset_kafka_serialization` - Kafka offset round-trip
- [ ] `test_source_offset_cdc_serialization` - CDC offset round-trip

### Integration Tests

- [ ] `test_manifest_write_and_read_local_fs` - Write/read manifest on local filesystem
- [ ] `test_checkpoint_layout_listing_order` - Multiple checkpoints list in time order
- [ ] `test_incomplete_checkpoint_skipped` - Directory without manifest is skipped
- [ ] `test_garbage_collection_retains_latest` - GC keeps N newest checkpoints

### Benchmarks

- [ ] `bench_manifest_serialize` - Target: < 1ms (100 operators)
- [ ] `bench_manifest_deserialize` - Target: < 1ms (100 operators)
- [ ] `bench_path_construction` - Target: < 100ns
- [ ] `bench_uuid_v7_generation` - Target: < 50ns

## Rollout Plan

1. **Phase 1**: Core types (`CheckpointId`, `OperatorId`, `PartitionId`, `SourceId`) + tests
2. **Phase 2**: `CheckpointManifest` and builder + serialization tests
3. **Phase 3**: `CheckpointPaths` path builder + tests
4. **Phase 4**: `SourceOffset` enum variants + serialization tests
5. **Phase 5**: Integration tests with local filesystem
6. **Phase 6**: Documentation and code review

## Open Questions

- [ ] Should `manifest.json` include a checksum of itself for integrity? Self-referential hashing is tricky; an alternative is signing the manifest.
- [ ] Should we support manifest compression (e.g., gzip) for very large deployments? Likely not needed given target manifest sizes.
- [ ] Should the `_latest` pointer be updated atomically? On S3, single-object writes are atomic; on local FS, use write-then-rename.
- [ ] Should we include Iceberg/Delta Lake manifest format compatibility? Deferred to connector-level integration.

## Completion Checklist

- [ ] Core types implemented (`CheckpointId`, `OperatorId`, etc.)
- [ ] `CheckpointManifest` and builder implemented
- [ ] `CheckpointPaths` path builder implemented
- [ ] `SourceOffset` variants for Kafka, CDC, File, Opaque
- [ ] JSON serialization/deserialization with serde
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [UUID v7 Specification (RFC 9562)](https://www.rfc-editor.org/rfc/rfc9562) - Time-sortable UUID format
- [Apache Flink Checkpoint Storage](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints/) - Inspiration for layout design
- [object_store crate](https://docs.rs/object_store/latest/object_store/) - Backend-agnostic object storage
- [rkyv crate](https://docs.rs/rkyv/latest/rkyv/) - Zero-copy serialization for snapshots
- [F-DCKP-004: Object Store Checkpointer](F-DCKP-004-object-store-checkpointer.md) - Upload/download implementation
