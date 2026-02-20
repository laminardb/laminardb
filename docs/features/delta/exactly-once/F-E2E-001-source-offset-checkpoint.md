# F-E2E-001: Source Offset Checkpoint (Layer 1: Source to Engine)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-E2E-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | L (5-10 days) |
| **Dependencies** | F-DCKP-003 (Checkpoint Manifest), F-DCKP-004 (State Snapshot) |
| **Blocks** | F-E2E-002 (Transactional Sink), F-E2E-003 (Idempotent Sink) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-storage` |
| **Module** | `laminar-storage/src/checkpoint/source_offsets.rs` |

## Summary

Implements the first layer of LaminarDB's three-layer exactly-once semantics: checkpointing source offsets atomically with operator state. **Integration with existing code**: The `laminar-storage` crate already has a comprehensive `CheckpointManifest` struct (at `checkpoint_manifest.rs`) that includes `source_offsets: HashMap<String, ConnectorCheckpoint>`, `sink_epochs`, `operator_states`, and WAL positions. This feature EXTENDS the existing manifest — it does NOT replace it. The typed `SourcePosition` enum introduced here provides stronger typing than the existing `ConnectorCheckpoint { offsets: HashMap<String, String> }` generic approach. Migration is additive: the existing string-based offsets continue to work, while new source connectors can use the typed `SourcePosition` variants. When a checkpoint barrier completes, the system writes a manifest that includes both the operator state snapshots AND the source read positions (offsets). On recovery, the system restores operator state from the snapshot and seeks all sources back to their checkpointed offsets. Events replayed from the source between the checkpoint and the failure produce identical state because all operators are deterministic. This layer guarantees that the source-to-engine boundary never duplicates or loses events, assuming deterministic processing within the engine.

## Goals

- Define `SourceOffset` and `SourcePosition` types covering Kafka, Postgres CDC, MySQL CDC, and file sources
- Atomically commit source offsets alongside operator snapshots in the checkpoint manifest
- On recovery, restore operator state AND seek sources to checkpointed positions
- Enforce determinism requirements on operators (same input + same state = same output)
- Support multi-source pipelines (each source has independent offsets)
- Integrate with the existing checkpoint barrier protocol (F-DCKP-001)
- Define clear contracts for `SourceConnector` implementations to report and restore offsets

## Non-Goals

- Sink-side exactly-once guarantees (covered by F-E2E-002 and F-E2E-003)
- Non-deterministic operators (e.g., operators using wall-clock time or random values)
- Cross-pipeline source offset coordination
- Source offset compaction or garbage collection (checkpoint GC handles manifest lifecycle)
- Exactly-once for user-defined functions that have external side effects

## Technical Design

### Architecture

**Ring**: Ring 1 (Background) -- checkpoint commit is async I/O work performed after Ring 0 barrier processing.

**Crate**: `laminar-storage`

**Module**: `laminar-storage/src/checkpoint/source_offsets.rs`

Source offset checkpointing works in conjunction with the barrier protocol. When a checkpoint barrier reaches a source operator, the source captures its current read position and includes it in the barrier's metadata. When all barriers have traversed the DAG and all operator states are snapshotted, the checkpoint coordinator writes a manifest that atomically records both the source offsets and the operator snapshots. On failure recovery, the recovery manager reads the latest manifest, restores operator states, and instructs each source to seek to its checkpointed position.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  CHECKPOINT FLOW                                                          │
│                                                                           │
│  1. Barrier injected at source                                           │
│     ┌──────────┐     ┌──────────┐     ┌──────────┐                      │
│     │ Source    │ ──> │ Operator │ ──> │ Operator │ ──> Sink             │
│     │ offset=42│     │ state=S1 │     │ state=S2 │                      │
│     └────┬─────┘     └────┬─────┘     └────┬─────┘                      │
│          │                │                │                              │
│          ▼                ▼                ▼                              │
│     ┌──────────────────────────────────────────────┐                     │
│     │ Checkpoint Manifest                           │                     │
│     │ {                                             │                     │
│     │   epoch: 7,                                   │                     │
│     │   source_offsets: [                           │                     │
│     │     { source: "kafka-orders", offset: 42 }   │                     │
│     │   ],                                          │                     │
│     │   operator_snapshots: [                       │                     │
│     │     { op: "window-1", snapshot: S1 },         │                     │
│     │     { op: "agg-1", snapshot: S2 }             │                     │
│     │   ]                                           │                     │
│     │ }                                             │                     │
│     └──────────────────────────────────────────────┘                     │
│                                                                           │
│  2. On recovery: restore S1, S2 + seek source to offset 42              │
│     Replayed events [42..] + state S1,S2 = identical output              │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use std::collections::HashMap;
use std::fmt;

/// Unique identifier for a source within a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceId(pub String);

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A source's read position at checkpoint time.
///
/// Captures the exact position in the source stream so that
/// on recovery, the source can seek back to this point and
/// replay events deterministically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceOffset {
    /// Which source this offset belongs to.
    pub source_id: SourceId,
    /// The source-specific position.
    pub offset: SourcePosition,
}

/// Source-specific position type.
///
/// Each source type has its own notion of "position" in the stream.
/// All variants must be serializable and comparable for progress tracking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePosition {
    /// Apache Kafka: topic + partition + offset.
    Kafka {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// PostgreSQL CDC: log sequence number + replication slot.
    PostgresCdc {
        lsn: u64,
        slot: String,
    },
    /// MySQL CDC: binlog file + position.
    MysqlCdc {
        binlog_file: String,
        binlog_position: u64,
    },
    /// File source: path + byte offset.
    File {
        path: String,
        byte_offset: u64,
    },
    /// Generic: opaque bytes for custom source implementations.
    Custom {
        source_type: String,
        position_bytes: Vec<u8>,
    },
}

impl SourcePosition {
    /// Serialize to JSON for manifest storage.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            SourcePosition::Kafka { topic, partition, offset } => {
                serde_json::json!({
                    "type": "kafka",
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                })
            }
            SourcePosition::PostgresCdc { lsn, slot } => {
                serde_json::json!({
                    "type": "postgres_cdc",
                    "lsn": lsn,
                    "slot": slot,
                })
            }
            SourcePosition::MysqlCdc { binlog_file, binlog_position } => {
                serde_json::json!({
                    "type": "mysql_cdc",
                    "binlog_file": binlog_file,
                    "binlog_position": binlog_position,
                })
            }
            SourcePosition::File { path, byte_offset } => {
                serde_json::json!({
                    "type": "file",
                    "path": path,
                    "byte_offset": byte_offset,
                })
            }
            SourcePosition::Custom { source_type, position_bytes } => {
                serde_json::json!({
                    "type": "custom",
                    "source_type": source_type,
                    // NOTE: Use base64 0.22+ API: base64::engine::general_purpose::STANDARD.encode/decode
                    "position_bytes": base64::engine::general_purpose::STANDARD.encode(position_bytes),
                })
            }
        }
    }

    /// Deserialize from JSON manifest entry.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, CheckpointError> {
        let type_str = value["type"]
            .as_str()
            .ok_or_else(|| CheckpointError::MalformedManifest("missing source type".into()))?;

        match type_str {
            "kafka" => Ok(SourcePosition::Kafka {
                topic: value["topic"].as_str().unwrap_or_default().to_string(),
                partition: value["partition"].as_i64().unwrap_or(0) as i32,
                offset: value["offset"].as_i64().unwrap_or(0),
            }),
            "postgres_cdc" => Ok(SourcePosition::PostgresCdc {
                lsn: value["lsn"].as_u64().unwrap_or(0),
                slot: value["slot"].as_str().unwrap_or_default().to_string(),
            }),
            "mysql_cdc" => Ok(SourcePosition::MysqlCdc {
                binlog_file: value["binlog_file"].as_str().unwrap_or_default().to_string(),
                binlog_position: value["binlog_position"].as_u64().unwrap_or(0),
            }),
            "file" => Ok(SourcePosition::File {
                path: value["path"].as_str().unwrap_or_default().to_string(),
                byte_offset: value["byte_offset"].as_u64().unwrap_or(0),
            }),
            "custom" => Ok(SourcePosition::Custom {
                source_type: value["source_type"].as_str().unwrap_or_default().to_string(),
                // NOTE: Use base64 0.22+ API: base64::engine::general_purpose::STANDARD.encode/decode
                position_bytes: base64::engine::general_purpose::STANDARD.decode(
                    value["position_bytes"].as_str().unwrap_or_default(),
                )
                .map_err(|e| CheckpointError::MalformedManifest(e.to_string()))?,
            }),
            other => Err(CheckpointError::MalformedManifest(
                format!("unknown source type: {}", other),
            )),
        }
    }
}

/// Trait extension for source connectors to support offset checkpointing.
///
/// Source connectors implementing this trait participate in the
/// exactly-once checkpoint protocol by reporting their current position
/// and seeking back on recovery.
pub trait CheckpointableSource: Send {
    /// Get the source's unique identifier.
    fn source_id(&self) -> &SourceId;

    /// Capture the current read position.
    ///
    /// Called when a checkpoint barrier reaches the source operator.
    /// The returned offset will be included in the checkpoint manifest.
    ///
    /// This method MUST be called AFTER the last event emitted before
    /// the barrier, and BEFORE the first event emitted after the barrier.
    fn current_offset(&self) -> SourceOffset;

    /// Seek the source back to a previously checkpointed position.
    ///
    /// Called during recovery to restore the source to its checkpoint
    /// position. After seeking, the next event read from the source
    /// will be the first event AFTER the checkpointed offset.
    ///
    /// # Errors
    ///
    /// Returns `SourceError::SeekFailed` if the source cannot seek
    /// to the requested position (e.g., Kafka topic compacted past offset).
    fn seek_to_offset(&mut self, offset: &SourceOffset) -> Result<(), SourceError>;

    /// Validate that the source can seek to the given offset.
    ///
    /// Called before committing to a recovery plan. Returns false if
    /// the offset is no longer reachable (e.g., past retention).
    fn can_seek_to(&self, offset: &SourceOffset) -> bool;
}

/// Atomic checkpoint manifest entry for source offsets.
///
/// This is included in the checkpoint manifest alongside operator
/// snapshots, ensuring they are committed atomically.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceOffsetManifestEntry {
    /// All source offsets at this checkpoint epoch.
    pub offsets: Vec<SourceOffsetEntry>,
}

/// A single source offset entry in the manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceOffsetEntry {
    /// Source identifier.
    pub source_id: String,
    /// Serialized source position.
    pub position: serde_json::Value,
}
```

### Data Structures

```rust
// NOTE: Do NOT redefine CheckpointManifest — use the existing one from
// `laminar_storage::checkpoint_manifest::CheckpointManifest`.
//
// The existing manifest already includes:
//   pub source_offsets: HashMap<String, ConnectorCheckpoint>,
//   pub sink_epochs: HashMap<String, u64>,
//   pub sink_commit_statuses: HashMap<String, SinkCommitStatus>,
//   pub operator_states: HashMap<String, OperatorCheckpoint>,
//   pub wal_position: u64,
//   pub per_core_wal_positions: Vec<u64>,
//   pub watermark: Option<i64>,
//   pub table_store_checkpoint_path: Option<String>,
//   ... and more fields
//
// This feature adds typed source positions via `SourcePosition` enum,
// which is stored WITHIN the existing `ConnectorCheckpoint.metadata`
// field or as a new `typed_position: Option<SourcePosition>` field
// added to `ConnectorCheckpoint`.
//
// use laminar_storage::checkpoint_manifest::CheckpointManifest;

/// Reference to a persisted operator snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OperatorSnapshotRef {
    /// Operator identifier.
    pub operator_id: String,
    /// Storage path to the snapshot file.
    pub snapshot_path: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Checksum for integrity verification.
    pub checksum: String,
}

/// Recovery plan built from a checkpoint manifest.
///
/// Used by the recovery manager to orchestrate source seeking
/// and state restoration.
pub struct RecoveryPlan {
    /// Manifest to recover from.
    pub manifest: CheckpointManifest,
    /// Parsed source offsets ready for seeking.
    pub source_offsets: Vec<SourceOffset>,
    /// Operator snapshot paths for restoration.
    pub operator_snapshots: Vec<OperatorSnapshotRef>,
}

impl RecoveryPlan {
    /// Build a recovery plan from a checkpoint manifest.
    pub fn from_manifest(manifest: CheckpointManifest) -> Result<Self, CheckpointError> {
        let source_offsets: Vec<SourceOffset> = manifest
            .source_offsets
            .offsets
            .iter()
            .map(|entry| {
                let position = SourcePosition::from_json(&entry.position)?;
                Ok(SourceOffset {
                    source_id: SourceId(entry.source_id.clone()),
                    offset: position,
                })
            })
            .collect::<Result<Vec<_>, CheckpointError>>()?;

        Ok(Self {
            operator_snapshots: manifest.operator_snapshots.clone(),
            source_offsets,
            manifest,
        })
    }
}

/// Determinism validator for operators in exactly-once pipelines.
///
/// Operators must produce identical output given identical input
/// and state. This validator checks known non-determinism sources.
pub struct DeterminismValidator;

impl DeterminismValidator {
    /// Validate that an operator is deterministic.
    ///
    /// Returns a list of warnings for potential non-determinism sources.
    pub fn validate(operator: &dyn OperatorDescriptor) -> Vec<DeterminismWarning> {
        let mut warnings = Vec::new();

        if operator.uses_wall_clock() {
            warnings.push(DeterminismWarning::WallClockUsage {
                operator_id: operator.id().to_string(),
                suggestion: "Use event-time instead of processing-time".to_string(),
            });
        }

        if operator.uses_random() {
            warnings.push(DeterminismWarning::RandomUsage {
                operator_id: operator.id().to_string(),
                suggestion: "Use deterministic seed or remove randomness".to_string(),
            });
        }

        if operator.has_external_side_effects() {
            warnings.push(DeterminismWarning::ExternalSideEffect {
                operator_id: operator.id().to_string(),
                suggestion: "Move side effects to a sink connector".to_string(),
            });
        }

        warnings
    }
}

/// Warning about potential non-determinism in an operator.
#[derive(Debug, Clone)]
pub enum DeterminismWarning {
    WallClockUsage {
        operator_id: String,
        suggestion: String,
    },
    RandomUsage {
        operator_id: String,
        suggestion: String,
    },
    ExternalSideEffect {
        operator_id: String,
        suggestion: String,
    },
}
```

### Existing Code Integration

| Existing Type | Location | Relationship |
|---------------|----------|-------------|
| `CheckpointManifest` | `laminar_storage::checkpoint_manifest` | Extended (not replaced) |
| `ConnectorCheckpoint` | `laminar_storage::checkpoint_manifest` | `SourcePosition` stored in its `metadata` |
| `SourceCheckpoint` | `laminar_connectors::checkpoint` | Existing per-connector checkpoint type |
| `SourceConnector::checkpoint()` | `laminar_connectors::connector` | Returns `SourceCheckpoint` — bridge to `SourceOffset` |
| `SourceConnector::restore()` | `laminar_connectors::connector` | Existing seek mechanism — `CheckpointableSource` wraps this |
| `CheckpointStore` trait | `laminar_storage::checkpoint_store` | Manifest persistence layer (unchanged) |
| `FileSystemCheckpointStore` | `laminar_storage::checkpoint_store` | Production storage backend (unchanged) |
| `RecoveryManager` | `laminar_db::recovery_manager` | Orchestrates recovery using manifest (unchanged) |

The `CheckpointableSource` trait in this feature is an EXTENSION of the existing
`SourceConnector` trait's `checkpoint()`/`restore()` methods, providing typed
offset capture instead of the generic `SourceCheckpoint { offsets: HashMap }`.

### Algorithm/Flow

#### Checkpoint Commit Flow

```
1. Checkpoint coordinator injects barrier into all sources
2. Each source captures its current offset:
   a. Kafka source: records (topic, partition, offset) for all assigned partitions
   b. Postgres CDC: records current LSN and slot name
   c. File source: records current byte offset
3. Barrier flows through the DAG; each operator snapshots its state
4. When all barriers have been acknowledged:
   a. Coordinator collects all source offsets from sources
   b. Coordinator collects all operator snapshot references
   c. Coordinator builds CheckpointManifest:
      {
        epoch: N,
        source_offsets: [{source_id, position}, ...],
        operator_snapshots: [{operator_id, path, checksum}, ...],
      }
   d. Coordinator writes manifest atomically (write-then-rename)
5. Checkpoint is now committed; manifest file is the commit point
```

#### Recovery Flow

```
1. On startup, recovery manager finds latest valid manifest
2. Build RecoveryPlan from manifest:
   a. Parse source offsets from manifest
   b. Parse operator snapshot references
3. Restore operator states:
   a. For each operator, load snapshot from storage path
   b. Verify checksum integrity
   c. Call operator.restore(snapshot)
4. Seek sources to checkpoint positions:
   a. For each source, call source.can_seek_to(offset)
   b. If seekable: call source.seek_to_offset(offset)
   c. If not seekable (retention expired):
      - Log error
      - Attempt older checkpoint
      - Or fail with unrecoverable error
5. Resume processing:
   a. Sources emit events from checkpoint position forward
   b. Operators process events with restored state
   c. Replayed events produce identical state (determinism guarantee)
   d. Engine reaches the point where it originally failed
   e. Continues processing new events normally
```

#### Determinism Guarantee

```
Precondition: All operators in the pipeline are deterministic.
  - Same input events + same initial state = same output events
  - No wall-clock time, no randomness, no external lookups that change

Given:
  - Checkpoint at epoch N captured:
    source_offsets = [kafka: offset 42]
    operator_states = [window_state, agg_state]

Recovery:
  1. Restore operator_states
  2. Seek kafka source to offset 42
  3. Replay events [42, 43, 44, ...] through operators
  4. Because operators are deterministic:
     window_state + events[42..] → identical output events
  5. No duplicates, no lost events in the engine

This is the "replay-based exactly-once" model used by Flink, Kafka Streams.
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `CheckpointError::ManifestWriteFailed` | I/O error writing manifest file | Retry write; if persistent, abort checkpoint (next checkpoint will try again) |
| `CheckpointError::MalformedManifest` | Corrupt or unparseable manifest during recovery | Try previous checkpoint manifest |
| `CheckpointError::SnapshotMissing` | Operator snapshot file not found at path | Try previous checkpoint; if persistent, full state rebuild |
| `CheckpointError::ChecksumMismatch` | Snapshot data integrity failure | Try previous checkpoint; report corruption |
| `SourceError::SeekFailed` | Source cannot seek to requested offset | Log error, try previous checkpoint; if all fail, start from earliest |
| `SourceError::RetentionExpired` | Kafka topic compacted or file deleted past checkpoint offset | Start from earliest available offset; warn about data gap |
| `SourceError::SlotNotFound` | Postgres replication slot dropped | Recreate slot; snapshot from scratch |
| `DeterminismError::NonDeterministicOperator` | Operator flagged as non-deterministic in exactly-once pipeline | Warn at pipeline creation; block deployment if strict mode |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Source offset capture latency | < 100ns | `bench_capture_source_offset` |
| Manifest serialization | < 50us (10 sources, 20 operators) | `bench_manifest_serialize` |
| Manifest write (local SSD) | < 1ms | `bench_manifest_write_local` |
| Manifest write (S3) | < 100ms | `bench_manifest_write_s3` |
| Source seek latency (Kafka) | < 10ms | `bench_kafka_seek` |
| Source seek latency (Postgres CDC) | < 5ms | `bench_postgres_cdc_seek` |
| Recovery time (10 sources, 20 operators) | < 5s | `bench_full_recovery` |
| Checkpoint overhead on throughput | < 1% | `bench_checkpoint_throughput_impact` |

## Test Plan

### Unit Tests

- [ ] `test_source_position_kafka_json_roundtrip` - Serialize and deserialize Kafka offset
- [ ] `test_source_position_postgres_cdc_json_roundtrip` - Serialize and deserialize Postgres CDC
- [ ] `test_source_position_mysql_cdc_json_roundtrip` - Serialize and deserialize MySQL CDC
- [ ] `test_source_position_file_json_roundtrip` - Serialize and deserialize file offset
- [ ] `test_source_position_custom_json_roundtrip` - Serialize and deserialize custom bytes
- [ ] `test_source_position_from_json_invalid_type` - Unknown type returns error
- [ ] `test_checkpoint_manifest_serialization` - Full manifest JSON roundtrip
- [ ] `test_checkpoint_manifest_atomic_write` - Write-then-rename atomicity
- [ ] `test_recovery_plan_from_manifest` - Parse manifest into recovery plan
- [ ] `test_recovery_plan_invalid_offset_returns_error` - Malformed offset in manifest
- [ ] `test_determinism_validator_wall_clock_warning` - Detects wall-clock usage
- [ ] `test_determinism_validator_random_warning` - Detects random usage
- [ ] `test_determinism_validator_side_effect_warning` - Detects external side effects
- [ ] `test_determinism_validator_clean_operator_no_warnings` - Deterministic operator passes

### Integration Tests

- [ ] `test_checkpoint_and_recovery_kafka_source` - Full cycle: process events, checkpoint, crash, recover, verify no duplicates
- [ ] `test_checkpoint_and_recovery_multi_source` - Pipeline with Kafka + file source, both offsets restored
- [ ] `test_checkpoint_and_recovery_preserves_window_state` - Window operator state matches post-recovery
- [ ] `test_recovery_from_second_oldest_checkpoint` - Latest checkpoint corrupt, falls back to previous
- [ ] `test_recovery_kafka_retention_expired` - Kafka offset past retention, graceful degradation
- [ ] `test_exactly_once_end_to_end` - Inject events, checkpoint, inject more events, crash, recover, verify output count
- [ ] `test_multi_partition_source_offset_capture` - Kafka source with multiple partitions captures all offsets

### Benchmarks

- [ ] `bench_capture_source_offset` - Target: < 100ns
- [ ] `bench_manifest_serialize` - Target: < 50us
- [ ] `bench_manifest_write_local` - Target: < 1ms
- [ ] `bench_kafka_seek` - Target: < 10ms
- [ ] `bench_full_recovery` - Target: < 5s
- [ ] `bench_checkpoint_throughput_impact` - Target: < 1% throughput reduction

## Rollout Plan

1. **Phase 1**: Define `SourceOffset`, `SourcePosition`, `SourceId` types
2. **Phase 2**: Define `CheckpointableSource` trait
3. **Phase 3**: Extend `CheckpointManifest` with `SourceOffsetManifestEntry`
4. **Phase 4**: Implement offset capture in existing source connectors (Kafka, Postgres CDC)
5. **Phase 5**: Implement `RecoveryPlan` builder and recovery flow
6. **Phase 6**: Implement `DeterminismValidator`
7. **Phase 7**: Unit tests for all serialization and validation
8. **Phase 8**: Integration tests with Kafka + state checkpoint recovery
9. **Phase 9**: Benchmarks and performance validation
10. **Phase 10**: Documentation and code review

## Open Questions

- [ ] Should `SourcePosition` use an enum or a trait object for extensibility? Enum is simpler and avoids dynamic dispatch; trait object allows third-party source types without modifying the core enum.
- [ ] How to handle sources that do not support seeking (e.g., HTTP webhook source)? Should they be excluded from exactly-once, or should we buffer events in a local WAL?
- [ ] Should the manifest include a hash of the source schema to detect schema evolution between checkpoints?
- [ ] How to handle the case where a Kafka source's partition assignment changes between checkpoint and recovery (consumer group rebalance)?
- [ ] Should determinism validation be a hard gate (prevent pipeline deployment) or a soft warning (log and proceed)?

## Completion Checklist

- [ ] `SourceId`, `SourceOffset`, `SourcePosition` types implemented
- [ ] `CheckpointableSource` trait defined
- [ ] `CheckpointManifest` extended with source offsets
- [ ] Source offset capture in Kafka connector
- [ ] Source offset capture in Postgres CDC connector
- [ ] `RecoveryPlan` builder and executor
- [ ] `DeterminismValidator` with warning types
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with full checkpoint/recovery cycle
- [ ] Benchmarks meet targets
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Apache Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#checkpointing) -- Production model this design follows
- [Kafka Streams Exactly-Once](https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/) -- Kafka-native offset checkpoint approach
- [F-DCKP-001: Barrier Protocol](../checkpoint/F-DCKP-001-barrier-protocol.md) -- Barrier injection and propagation
- [F-DCKP-003: Checkpoint Manifest](../checkpoint/F-DCKP-003-checkpoint-manifest.md) -- Manifest format and persistence
- [F-DCKP-004: State Snapshot](../checkpoint/F-DCKP-004-state-snapshot.md) -- Operator state snapshotting
- [F025: Kafka Source](../../phase-3/F025-kafka-source.md) -- Kafka source connector
- [F027: Postgres CDC](../../phase-3/F027-postgres-cdc.md) -- Postgres CDC connector
