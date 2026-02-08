# Checkpoint Production Readiness: 2026 SOTA Gap Analysis & Remediation Plan

> Date: 2026-02-08
> Scope: Full audit of LaminarDB checkpointing vs 2026 production-grade streaming databases

---

## 1. 2026 State of the Art

### 1.1 Chandy-Lamport Evolution

The classic Chandy-Lamport barrier-based snapshot algorithm remains the foundation, but 2025-2026 systems have made critical advances:

| System | Approach | Key Innovation |
|--------|----------|----------------|
| **Flink 2.x** | Unaligned checkpoints | Barriers overtake in-flight data; buffers captured with checkpoint. Eliminates backpressure-induced stalls |
| **Feldera/DBSP** | Incremental Z-set snapshots | No barriers needed — Z-set deltas are the checkpoint. Commit the delta log |
| **Arroyo 0.12+** | Async operator snapshots | Operators snapshot concurrently, no stop-the-world. RocksDB `checkpoint()` for zero-copy |
| **RisingWave** | Hummock LSM state store | Shared-storage state (S3), SST-level checkpoints. No local state to snapshot |
| **Redpanda** | Raft + shadow indexing | Per-partition Raft log IS the checkpoint. No separate snapshot needed |
| **ScyllaDB/Seastar** | Per-shard snapshots | Thread-per-core: each shard snapshots independently, coordinator merges metadata |

**Key 2026 principles:**
1. **Barriers should not block processing** — unaligned or async snapshots
2. **State backend determines checkpoint strategy** — LSM/RocksDB enables incremental; mmap requires full snapshot
3. **Changelog IS the checkpoint** — systems moving toward changelog-as-state (DBSP, Materialize)
4. **Per-core isolation** — thread-per-core systems snapshot cores independently

### 1.2 Multi-Source/Multi-Sink Exactly-Once

Production-grade 2026 systems coordinate across heterogeneous connectors using a **two-phase checkpoint protocol**:

```
Phase 1: PRE-COMMIT (barrier arrives at sinks)
  ├── Kafka Sink: flush producer, do NOT commit transaction yet
  ├── PostgreSQL Sink: execute writes in open transaction, do NOT COMMIT
  ├── Delta Lake Sink: write Parquet files, do NOT commit to Delta log
  └── All sources: snapshot offsets (Kafka consumer offsets, CDC LSNs, GTIDs)

Phase 2: COMMIT (coordinator confirms all sinks pre-committed)
  ├── Kafka Sink: commit_transaction()
  ├── PostgreSQL Sink: COMMIT (with epoch in _offsets table)
  ├── Delta Lake Sink: commit Delta log with txn metadata
  ├── All sources: commit offsets to brokers/slots
  └── Coordinator: persist checkpoint metadata atomically
```

**Critical requirement**: If ANY sink fails Phase 2, ALL sinks must rollback and the checkpoint is aborted. Sources do NOT advance their committed offsets.

**Per-connector contracts:**

| Connector | Pre-commit | Commit | Rollback | Idempotency Key |
|-----------|-----------|--------|----------|-----------------|
| Kafka Source | Save partition→offset map | Commit offsets to consumer group | No-op (re-read) | Partition+offset |
| Kafka Sink | `flush()` in open txn | `commit_transaction()` | `abort_transaction()` | Transactional producer ID |
| PostgreSQL CDC | Save confirmed_flush_lsn | Send standby status update | Re-read from LSN | LSN |
| PostgreSQL Sink | Execute in open txn | `COMMIT` + epoch in offset table | `ROLLBACK` | Epoch in offset table |
| MySQL CDC | Save GTID set or binlog pos | Acknowledge to server | Re-read from GTID | GTID set |
| Delta Lake | Write Parquet files | Commit Delta log with `txn` | Delete orphaned Parquet | `txn.appId` + `txn.version` |
| Iceberg | Write data files | Commit snapshot | Delete orphaned files | Snapshot sequence number |
| RabbitMQ | Buffer acks | `basic_ack(multiple=true)` | `basic_nack(requeue=true)` | Delivery tag |

### 1.3 Thread-Per-Core Checkpoint Strategy

For LaminarDB's architecture (Seastar-inspired), the 2026 consensus is:

```
┌──────────────────────────────────────────────────────────┐
│ Coordinator (Ring 2)                                     │
│   1. Broadcast CHECKPOINT_BARRIER to all cores           │
│   2. Wait for all cores to ACK                           │
│   3. Persist unified checkpoint metadata                 │
│   4. Notify cores: CHECKPOINT_COMPLETE                   │
└──────────────────────────────────────────────────────────┘
        │                                │
        ▼                                ▼
┌──────────────────┐        ┌──────────────────┐
│ Core 0           │        │ Core 1           │
│  1. Drain SPSC   │        │  1. Drain SPSC   │
│  2. Snapshot     │        │  2. Snapshot      │
│     operators    │        │     operators     │
│  3. Flush WAL    │        │  3. Flush WAL     │
│  4. RocksDB      │        │  4. RocksDB       │
│     checkpoint() │        │     checkpoint()  │
│  5. ACK to coord │        │  5. ACK to coord  │
└──────────────────┘        └──────────────────┘
```

**Key design points:**
- Each core snapshots **independently** (no cross-core locks)
- Per-core WAL segments (already implemented in F062)
- RocksDB `checkpoint()` creates hard-link snapshots (O(1), no data copy)
- Coordinator only merges metadata, never touches state
- Barrier propagation via SPSC queues (already in architecture)

### 1.4 Performance Targets (2026 Production Grade)

| Metric | Target | Flink 2.x | Arroyo | RisingWave |
|--------|--------|-----------|--------|------------|
| Checkpoint duration | < 100ms | ~200ms (aligned) | ~50ms | ~500ms |
| Recovery time | < 5s (1GB state) | ~10s | ~3s | ~30s |
| Throughput impact | < 5% | ~10% (aligned), ~2% (unaligned) | ~3% | ~5% |
| Max state size | > 10GB/core | 100GB+ | 10GB | Unbounded (S3) |
| Checkpoint interval | 10s-60s | 10s default | 10s | 10s |
| Concurrent checkpoints | 1-2 | 1 (configurable) | 1 | 1 |

---

## 2. Current LaminarDB Checkpoint Architecture

### 2.1 Three Disconnected Systems

LaminarDB currently has **three checkpoint systems that do NOT coordinate**:

```
┌─────────────────────────────────────────────────────────────────┐
│ System A: WAL State Store (laminar-storage)                     │
│   MmapStateStore → snapshot → WAL marker → disk                 │
│   Scope: Ring 0 operator state (windows, joins)                 │
│   Storage: Binary snapshot + WAL replay                         │
│   Status: WORKS but mmap index not persisted                    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ System B: Pipeline Checkpoint (laminar-db)                      │
│   Source offsets + sink epochs → JSON → disk                    │
│   Scope: Connector state (Kafka offsets, CDC LSNs)              │
│   Storage: JSON files in pipeline_checkpoints/                  │
│   Status: WORKS but no operator state, no atomicity             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ System C: DAG Checkpoint (laminar-core)                         │
│   Barrier → align → snapshot operators → in-memory              │
│   Scope: DAG operator state snapshots                           │
│   Storage: IN-MEMORY ONLY (Vec<DagCheckpointSnapshot>)          │
│   Status: BROKEN — snapshots LOST on restart                    │
└─────────────────────────────────────────────────────────────────┘
```

**The fundamental problem**: These three systems capture different slices of state at different times with no coordination. Recovery cannot guarantee consistency.

### 2.2 What Each System Captures

| State | System A (WAL) | System B (Pipeline) | System C (DAG) |
|-------|---------------|--------------------|-----------------|
| Operator state (windows, joins) | ✅ via snapshot | ❌ | ✅ but not persisted |
| Source offsets (Kafka, CDC) | ✅ (simple u64) | ✅ (full SourceCheckpoint) | ✅ (HashMap) |
| Sink epochs | ❌ | ✅ | ❌ |
| Table store state | ❌ | ⚠️ (path saved, not loaded) | ❌ |
| Watermark | ✅ | ❌ | ✅ |
| Persisted to disk | ✅ | ✅ | ❌ |
| Atomic write | ⚠️ (WAL append) | ❌ (plain JSON write) | N/A |

### 2.3 Per-Connector Audit

| Connector | checkpoint() | restore() | Exactly-Once | Recovery Tested |
|-----------|-------------|-----------|--------------|-----------------|
| Kafka Source | ✅ partition→offset | ✅ sets tracker | ⚠️ race with commit | ❌ no e2e test |
| Kafka Sink | N/A (epoch-based) | N/A | ✅ transactions | ❌ no e2e test |
| PostgreSQL CDC | ✅ LSN | ✅ parses LSN | ⚠️ slot not managed | ❌ no e2e test |
| PostgreSQL Sink | N/A (epoch-based) | N/A | ✅ txn + offset table | ❌ no e2e test |
| MySQL CDC | ✅ GTID/binlog pos | ✅ parses GTID | ⚠️ purge not handled | ❌ no e2e test |
| Delta Lake | N/A (txn-based) | N/A | ✅ Delta txn metadata | ❌ no e2e test |
| Iceberg | ❌ stub only | ❌ stub only | ❌ not implemented | ❌ |

---

## 3. Gap Analysis

### 3.1 Critical Gaps (P0 — Data Loss / Correctness)

#### GAP-1: DAG Snapshots Not Persisted
- **What**: `DagCheckpointCoordinator` creates operator snapshots but stores them only in `Vec<DagCheckpointSnapshot>` in memory
- **Impact**: ALL operator state (window contents, join buffers, aggregation accumulators) lost on restart
- **Where**: `crates/laminar-connectors/src/bridge/runtime.rs:306` — `recovery_manager.add_snapshot()` stores in-memory only
- **SOTA gap**: Every production system persists operator snapshots to durable storage

#### GAP-2: No Unified Checkpoint Coordinator
- **What**: Three systems checkpoint independently — no shared epoch, no shared barrier, no atomicity
- **Impact**: After recovery, source offsets may be from epoch 10, operator state from epoch 9, sink at epoch 8 — inconsistent state
- **Where**: No coordinator exists; `pipeline_checkpoint.rs` and `dag/checkpoint.rs` are separate codepaths
- **SOTA gap**: Flink, Arroyo, RisingWave all have a single checkpoint coordinator

#### GAP-3: MmapStateStore Index Not Loaded
- **What**: `load_from_mmap()` returns empty BTreeMap — all lookups fail after mmap reload
- **Impact**: WAL replay required for ALL state; if WAL truncated, permanent data loss
- **Where**: `crates/laminar-core/src/state/mmap.rs:333-356`
- **SOTA gap**: Every state backend must be self-contained for recovery

#### GAP-4: No Two-Phase Sink Coordination
- **What**: Sinks commit independently — Kafka sink may commit while PostgreSQL sink fails
- **Impact**: Partial commits across sinks = exactly-once violation
- **Where**: `bridge/runtime.rs` calls `commit_epoch()` per sink sequentially with no rollback-all-on-failure
- **SOTA gap**: Production systems use two-phase commit across all sinks

### 3.2 High Priority Gaps (P1 — Production Blockers)

#### GAP-5: No Atomic Checkpoint Writes
- **What**: Pipeline checkpoint writes JSON directly — no temp-file + rename pattern
- **Impact**: Crash during write = corrupted checkpoint file; recovery loads garbage
- **Where**: `crates/laminar-db/src/pipeline_checkpoint.rs`

#### GAP-6: WAL Never Truncated
- **What**: WAL grows unbounded; comment says "optionally truncate" but no implementation
- **Impact**: Disk fills up; recovery replays entire history
- **Where**: `crates/laminar-storage/src/wal_state_store.rs:365-366`

#### GAP-7: RocksDB Table Store Not Recovered
- **What**: `table_store_checkpoint_path` saved in checkpoint but never loaded on recovery
- **Impact**: Persistent reference tables lost after restart
- **Where**: `crates/laminar-db/src/pipeline_checkpoint.rs:75` (saved), missing in recovery

#### GAP-8: Changelog Buffer Dead Code
- **What**: `StateChangelogBuffer` defined but never drained; `apply_changelog()` never called
- **Impact**: The zero-allocation Ring 0 → Ring 1 changelog path is non-functional
- **Where**: `crates/laminar-storage/src/incremental/changelog.rs`

#### GAP-9: No End-to-End Recovery Tests
- **What**: No integration test simulates crash + recovery across sources, operators, and sinks
- **Impact**: Recovery path is untested; unknown failure modes in production
- **Where**: All connector tests are unit-level only

### 3.3 Medium Priority Gaps (P2 — Operational)

#### GAP-10: Kafka Offset Commit Race
- **What**: Checkpoint saves offset but Kafka consumer group commit is async — restart may re-read
- **Impact**: At-least-once, not exactly-once for Kafka source
- **Where**: `crates/laminar-connectors/src/kafka/source.rs`

#### GAP-11: PostgreSQL Replication Slot Not Managed
- **What**: Slot creation/drop not implemented; slot can block WAL growth on PostgreSQL server
- **Where**: `crates/laminar-connectors/src/cdc/postgres/source.rs`

#### GAP-12: MySQL GTID Purge Not Handled
- **What**: If MySQL purges binlogs before recovery, GTID-based restore fails silently
- **Where**: `crates/laminar-connectors/src/cdc/mysql/source.rs`

#### GAP-13: No Checkpoint Metrics
- **What**: No observability for checkpoint duration, size, failure rate, recovery time
- **Impact**: Operators cannot monitor checkpoint health

---

## 4. Remediation Plan

### 4.1 Architecture: Unified Checkpoint Coordinator

Replace the three disconnected systems with a single **Checkpoint Coordinator** that orchestrates all state:

```
┌─────────────────────────────────────────────────────────────────┐
│                   CheckpointCoordinator (Ring 2)                │
│                                                                 │
│  Owns: global epoch counter, checkpoint storage, recovery       │
│                                                                 │
│  trigger_checkpoint(epoch):                                     │
│    1. Inject barrier into all sources                           │
│    2. Barrier propagates through DAG (Chandy-Lamport)           │
│    3. Each operator snapshots state → CheckpointStore           │
│    4. Each sink pre-commits (Phase 1)                           │
│    5. Coordinator verifies all nodes complete                   │
│    6. All sinks commit (Phase 2)                                │
│    7. All sources commit offsets                                │
│    8. Persist unified CheckpointManifest                        │
│    9. ACK checkpoint complete                                   │
│                                                                 │
│  recover(checkpoint_id):                                        │
│    1. Load CheckpointManifest                                   │
│    2. Restore operator states from CheckpointStore              │
│    3. Restore source offsets (seek connectors)                  │
│    4. Reset sink epochs                                         │
│    5. Resume processing                                         │
└─────────────────────────────────────────────────────────────────┘
```

**Unified Checkpoint Manifest:**
```rust
struct CheckpointManifest {
    checkpoint_id: u64,
    epoch: u64,
    timestamp_ms: u64,

    // Source state
    source_offsets: HashMap<String, SourceCheckpoint>,

    // Operator state (references to checkpoint store)
    operator_snapshots: HashMap<NodeId, SnapshotReference>,

    // Sink state
    sink_epochs: HashMap<String, u64>,

    // Table state
    table_offsets: HashMap<String, SourceCheckpoint>,
    table_store_checkpoint: Option<PathBuf>,

    // Watermarks
    watermarks: HashMap<String, i64>,

    // Metadata
    dag_topology_hash: u64,  // Detect topology changes
    state_size_bytes: u64,
}
```

### 4.2 Phased Implementation

#### Phase A: Foundation (P0 fixes — no architecture change)

**A1. Persist DAG snapshots to disk**
- Extend `DagRecoveryManager` to write snapshots to `{data_dir}/dag_checkpoints/`
- Serialize via rkyv (already used for state snapshots)
- Load on recovery before operator restore
- ~2-3 days

**A2. Fix mmap index persistence**
- Wire `save_index()` into checkpoint path
- Wire `load_index()` into recovery path
- Verify round-trip with integration test
- ~1 day

**A3. Atomic checkpoint writes**
- Write to `.tmp` file, then `fs::rename()` (atomic on both Linux and Windows)
- Apply to both pipeline checkpoint JSON and DAG snapshot files
- ~0.5 days

**A4. Two-phase sink commit**
- Add `pre_commit()` to `SinkConnector` trait
- In bridge runtime: call `pre_commit()` on ALL sinks, then `commit()` on ALL
- If any `pre_commit()` fails: `rollback_epoch()` on all sinks
- ~2 days

#### Phase B: Unification (merge three systems)

**B1. Create CheckpointCoordinator**
- New module: `crates/laminar-db/src/checkpoint_coordinator.rs`
- Owns epoch counter, checkpoint trigger interval, checkpoint storage
- Replaces both `PipelineCheckpointManager` and `DagCheckpointCoordinator`
- ~3-4 days

**B2. Unified CheckpointManifest**
- Single manifest captures ALL state (sources, operators, sinks, tables, watermarks)
- Binary format (rkyv) with JSON sidecar for debugging
- Stored in `{data_dir}/checkpoints/epoch_{N}/manifest.bin`
- ~2 days

**B3. Operator state → CheckpointStore**
- Operator snapshots stored as individual files: `epoch_{N}/node_{id}.state`
- For RocksDB-backed state: `checkpoint()` creates hard-link snapshot in `epoch_{N}/rocksdb/`
- Reference in manifest via `SnapshotReference` (path + size + checksum)
- ~2-3 days

**B4. Unified recovery path**
- Single `recover()` method on `CheckpointCoordinator`
- Loads manifest → restores operators → restores sources → resets sinks
- Validates topology hash matches (detect DAG changes since checkpoint)
- ~2-3 days

#### Phase C: Production Hardening

**C1. WAL truncation**
- After successful checkpoint, truncate WAL up to checkpoint's WAL position
- Keep one extra segment for safety
- ~1 day

**C2. Changelog buffer wiring**
- Drain `StateChangelogBuffer` in Ring 1
- Feed to `IncrementalCheckpointManager.apply_changelog()`
- Enables true incremental checkpoints (only changed keys)
- ~2-3 days

**C3. RocksDB table store recovery**
- On recovery, open RocksDB from `table_store_checkpoint_path`
- Re-register tables with DataFusion
- ~1-2 days

**C4. End-to-end recovery integration tests**
- Test: Kafka source → window operator → PostgreSQL sink, crash, recover
- Test: CDC source → join → Delta Lake sink, crash, recover
- Test: Multi-sink checkpoint with partial failure
- Test: Recovery after topology change (should fail gracefully)
- ~3-4 days

#### Phase D: Connector Hardening

**D1. Kafka offset commit synchronization**
- On checkpoint: `commit_offsets_sync()` instead of async
- Or: record committed offsets in checkpoint manifest and verify on recovery
- ~1 day

**D2. PostgreSQL slot lifecycle management**
- `create_replication_slot()` on first connect
- `drop_replication_slot()` on pipeline delete
- Monitor slot lag in metrics
- ~2 days

**D3. MySQL GTID validation**
- On restore: query `SHOW BINARY LOGS` / `SELECT @@gtid_purged`
- If checkpoint GTID overlaps with purged set: error with actionable message
- ~1 day

**D4. Checkpoint observability**
- Metrics: `checkpoint_duration_ms`, `checkpoint_size_bytes`, `recovery_time_ms`
- Metrics: `checkpoint_failure_total`, `checkpoint_epoch_current`
- Wire into F-OBS-001 `PipelineMetrics`
- ~1-2 days

### 4.3 Implementation Order & Dependencies

```
Phase A (P0 Foundation) ─────────────────────────── ~6-8 days
  A1: Persist DAG snapshots ──┐
  A2: Fix mmap index ─────────┤
  A3: Atomic writes ──────────┤─→ All independent, can parallelize
  A4: Two-phase sink commit ──┘

Phase B (Unification) ──────────────────────────── ~10-12 days
  B1: CheckpointCoordinator ──→ B2: Manifest ──→ B3: Store ──→ B4: Recovery
  (sequential dependency chain)

Phase C (Hardening) ─────────────────────────────── ~7-10 days
  C1: WAL truncation ────┐
  C2: Changelog wiring ──┤─→ Independent of each other
  C3: Table recovery ────┤     but all depend on Phase B
  C4: Integration tests ─┘

Phase D (Connectors) ───────────────────────────── ~5-6 days
  D1: Kafka sync commit ──┐
  D2: PostgreSQL slots ───┤─→ Independent, can parallelize
  D3: MySQL GTID ─────────┤     Depend on Phase B for test harness
  D4: Checkpoint metrics ─┘
```

**Total estimated: ~28-36 days**

### 4.4 Migration Strategy

The unified system must be backward-compatible with existing pipeline checkpoints:

1. **Phase B1**: `CheckpointCoordinator` checks for old-format `pipeline_checkpoints/` directory
2. If found: loads as legacy checkpoint, migrates source offsets to new manifest format
3. Operator state starts fresh (no old DAG snapshots exist on disk anyway)
4. Old format directory renamed to `pipeline_checkpoints.legacy/`
5. All new checkpoints use unified format

---

## 5. Target Architecture (Post-Remediation)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Ring 0: Hot Path                              │
│                                                                 │
│  Source → Operators → Sinks                                     │
│              │                                                  │
│              │ SPSC (changelog entries)                          │
│              ▼                                                  │
├─────────────────────────────────────────────────────────────────┤
│                    Ring 1: Background                            │
│                                                                 │
│  ┌──────────────────┐   ┌──────────────────┐                   │
│  │ StateChangelog    │──▶│ IncrementalCkpt  │                   │
│  │ Buffer (SPSC)    │   │ Manager (RocksDB)│                   │
│  └──────────────────┘   └────────┬─────────┘                   │
│                                  │                              │
│  ┌──────────────────┐            │                              │
│  │ Per-Core WAL     │────────────┤                              │
│  │ (F062)           │            │                              │
│  └──────────────────┘            │                              │
│                                  ▼                              │
│                    ┌──────────────────────┐                     │
│                    │  CheckpointStore     │                     │
│                    │  epoch_N/            │                     │
│                    │    manifest.bin      │                     │
│                    │    node_0.state      │                     │
│                    │    node_1.state      │                     │
│                    │    rocksdb/ (links)  │                     │
│                    │    wal_position.txt  │                     │
│                    └──────────────────────┘                     │
├─────────────────────────────────────────────────────────────────┤
│                    Ring 2: Control Plane                         │
│                                                                 │
│  ┌──────────────────────────────────────────────────────┐      │
│  │              CheckpointCoordinator                    │      │
│  │                                                       │      │
│  │  1. Timer fires → inject barriers                     │      │
│  │  2. Collect operator snapshots                        │      │
│  │  3. Pre-commit all sinks (Phase 1)                    │      │
│  │  4. Verify all complete                               │      │
│  │  5. Commit all sinks (Phase 2)                        │      │
│  │  6. Commit source offsets                             │      │
│  │  7. Write CheckpointManifest atomically               │      │
│  │  8. Prune old checkpoints                             │      │
│  │  9. Emit checkpoint metrics                           │      │
│  └──────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────┘
```

### 5.1 Checkpoint Protocol (Unified)

```
Time ─────────────────────────────────────────────────────────▶

Coordinator:  TRIGGER ────────────────────── VERIFY ── COMMIT
                │                               │        │
Source A:    ···│·BARRIER·····snapshot·····ACK··│········│···
Source B:    ···│·····BARRIER··snapshot··ACK····│········│···
                │         │                     │        │
Operator 1:  ··│·········BARRIER·snap·ACK······│········│···
Operator 2:  ··│··········BARRIER·snap·ACK·····│········│···
                │              │                │        │
Sink X:      ··│··············BARRIER·precommit│·commit·│···
Sink Y:      ··│···············BARRIER·precomm·│·commit·│···
                │                               │        │
Manifest:    ──│───────────────────────────────│──WRITE─│──
```

### 5.2 Recovery Protocol (Unified)

```
1. Find latest valid CheckpointManifest
2. Validate manifest integrity (CRC32C)
3. Validate DAG topology hash matches current topology
4. For each operator: load snapshot from CheckpointStore
5. For each source: call restore(SourceCheckpoint)
6. For each sink: set epoch to manifest.sink_epochs[name]
7. For table store: open RocksDB from checkpoint path
8. Replay per-core WALs from manifest.wal_position
9. Resume processing
```

---

## 6. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Two-phase commit adds latency | Medium | Low (< 10ms per checkpoint) | Pre-commit is cheap; batched flush |
| Coordinator becomes bottleneck | Low | Medium | Coordinator only handles metadata; heavy lifting is per-core |
| Migration breaks existing demos | Medium | Medium | Legacy format detection + automatic migration |
| RocksDB hard-link snapshots fail on Windows | High | Low | Fall back to full copy on Windows |
| Large operator state slows checkpoint | Medium | Medium | Incremental snapshots (changelog-based) after Phase C |

---

## 7. Success Criteria

| Metric | Current | Target | Validation |
|--------|---------|--------|------------|
| Checkpoint consistency | ❌ 3 systems | ✅ 1 unified | Architecture review |
| Operator state persisted | ❌ In-memory only | ✅ Durable | Recovery test |
| Exactly-once across sinks | ❌ Independent commits | ✅ Two-phase | Multi-sink test |
| Recovery time (1GB state) | ❌ Unknown | < 5s | Benchmark |
| Checkpoint throughput impact | ❌ Unknown | < 5% | Benchmark |
| Checkpoint duration | ❌ Unknown | < 100ms | Benchmark |
| End-to-end recovery tests | ❌ 0 | ✅ 4+ scenarios | CI |
| WAL bounded growth | ❌ Unbounded | ✅ Truncated | Integration test |

---

## 8. References

- Apache Flink: Unaligned Checkpoints (FLIP-76, Flink 1.11+, matured in 2.x)
- Feldera/DBSP: Z-set incremental computation (Budiu et al., VLDB 2023)
- Arroyo: Rust streaming engine checkpoint design (arroyo.dev docs)
- RisingWave: Hummock state store (SIGMOD 2024)
- Chandy-Lamport: Distributed Snapshots (1985, foundation)
- ScyllaDB: Per-shard snapshot architecture (scylladb.com/architecture)
- Redpanda: Raft-based partition snapshots (redpanda.com/blog)
