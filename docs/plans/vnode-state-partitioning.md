# Plan: VNode State Partitioning for Distribution-Ready Checkpoints

## Status: PLANNED
## Date: 2026-03-13

## Problem

LaminarDB has two execution paths with different state management:

1. **TPC/SQL path (production)**: `StreamExecutor` owns state in `FxHashMap`s. Checkpointed via `checkpoint_state()` → JSON. No `StateStore` involvement.

2. **DAG/Reactor path (retained for future programmatic API)**: Operators use `ctx.state: &mut dyn StateStore`. Not checkpointed — the Reactor's `trigger_checkpoint()` omits the `StateStore`.

Neither path embeds a virtual partition (vnode) in state keys. This means:
- Checkpoints are monolithic — cannot be split across nodes
- Rescaling requires reading ALL state and filtering
- Distribution requires a breaking checkpoint format change

## Architecture Findings

### What the research says (2025-2026)

| System | Partition Unit | Count | Immutable? | Key Format |
|--------|---------------|-------|------------|------------|
| Flink 2.0 | Key Group | 128-32768 | Yes, forever | `[key_group_id][serialized_key]` |
| RisingWave | VNode | 256 | Yes, forever | `[vnode_id][table_prefix][key]` |
| Kafka Streams | Topic Partition | N/A | Tied to topic | N/A (RocksDB per partition) |
| Arroyo | Subtask shard | Dynamic | Per checkpoint | Parquet files per shard |

**Universal lesson**: The virtual partition count is frozen at first checkpoint. Get it right now.

### What LaminarDB has today

**TPC/SQL production path** — StreamExecutor state:
- `agg_states: FxHashMap<usize, IncrementalAggState>` — keyed by query index
- `eowc_agg_states: FxHashMap<usize, IncrementalEowcState>` — keyed by query index
- `core_window_states: FxHashMap<usize, CoreWindowState>` — keyed by query index
- Each `IncrementalAggState` internally keys by GROUP BY values

**DAG/Reactor path** — 4 operators use `ctx.state`:
- `stream_join`: prefix `sjl:`/`sjr:` + key_hash(8) + timestamp(8) + event_id(8)
- `session_window`: prefix `six:`/`sac:` + key_hash(8)
- `sliding_window`: prefix `slw:` + WindowId(16)
- `window` (tumbling): prefix `win:` + WindowId(16)

**6 operators manage their own internal FxHashMap state** (lag_lead, partitioned_topk, asof_join, temporal_join, watermark_sort, window_sort).

### What the Delta module has

- `PartitionGuard` with `partition_id: u32` + `epoch: u64`
- Consistent-hash assigner with 100 vnodes per core
- Epoch-fenced guard check: ~10ns (Ring 0 safe)
- But: partition_id is NOT embedded in any state key

## Design: VNode-Aware State

### Constants

```rust
// crates/laminar-core/src/state/mod.rs
/// Virtual partition count. Immutable after first checkpoint.
/// 256 balances partition granularity vs overhead.
pub const VNODE_COUNT: u16 = 256;
```

Why 256:
- Flink lower bound is 128, RisingWave uses 256
- Supports up to 256-way parallelism (cores or nodes)
- 2-byte prefix overhead per key is negligible
- Power of 2 for bitmask-based assignment

### VNode Assignment Function

```rust
// crates/laminar-core/src/state/mod.rs
/// Assigns a key to a vnode using murmur3 hash.
/// Uses murmur3 (not FxHash) because modular arithmetic
/// requires uniform distribution — FxHash is fast but biased.
#[inline]
pub fn vnode_of(key: &[u8]) -> u16 {
    // murmur3 finalize of 32-bit hash, mod VNODE_COUNT
    let hash = murmur3_32(key, 0);
    (hash % u32::from(VNODE_COUNT)) as u16
}
```

### Phase 0 Changes (Immediate — fix checkpoint gap)

**These changes fix STATE-1/2/3 without adding vnodes yet.**

#### 1. StreamExecutor: add vnode to aggregate group keys

In `IncrementalAggState::checkpoint_groups()`, the group key serialization should include a vnode prefix derived from the GROUP BY key values. This tags each group's checkpoint data with its vnode, enabling per-vnode restore later.

```rust
// aggregate_state.rs - checkpoint_groups()
// For each (group_key, accumulators) pair:
let vnode = vnode_of(&group_key_bytes);
// Store as: { vnode -> [(group_key, accumulator_data)] }
```

#### 2. StreamExecutorCheckpoint: add vnode_count field

```rust
pub struct StreamExecutorCheckpoint {
    pub version: u32,  // bump to 2
    pub vnode_count: u16,  // NEW — 256
    pub agg_states: FxHashMap<String, ...>,
    pub eowc_states: FxHashMap<String, ...>,
    pub core_window_states: FxHashMap<String, ...>,
    pub join_states: FxHashMap<String, ...>,
    pub raw_eowc_states: FxHashMap<String, ...>,
}
```

#### 3. CheckpointManifest: add vnode_count

```rust
// checkpoint_manifest.rs
pub vnode_count: Option<u16>,  // None for legacy checkpoints
```

#### 4. DAG/Reactor path: snapshot StateStore

```rust
// reactor/mod.rs
pub fn trigger_checkpoint(&mut self) -> (Vec<OperatorState>, StateSnapshot) {
    let ops = self.operators.iter().map(|op| op.checkpoint()).collect();
    let snap = self.state_store.snapshot();
    (ops, snap)
}
```

This fixes STATE-1 for the DAG path. The StateStore contents are captured.

### Phase 1 Changes — REVISED: VNode tagging at checkpoint time, NOT in live keys

**Design decision (2026-03-13):** Embedding vnodes in live state keys was rejected
because it breaks O(log n) prefix scans on the Ring 0 hot path. Operators use
`ctx.state.prefix_scan(b"sjl:")` to find matching rows — prepending a vnode would
require scanning 256 × prefix combinations.

Instead, vnodes are computed at **checkpoint serialization time**:
- `StateSnapshot` captures all keys as-is (no vnode prefix)
- When writing per-vnode checkpoint files (Phase 2), compute `vnode_of(key)` and
  partition the snapshot entries by vnode
- On restore, load only the vnode ranges assigned to this node

This matches the RisingWave approach: operators see plain keys; the storage/checkpoint
layer handles vnode routing.

#### 5. (Deferred to Phase 2) Per-vnode checkpoint file writer

When Phase 2 is implemented, the checkpoint persistence path will:
```rust
// At checkpoint write time (Ring 2, cold path):
for (key, value) in snapshot.data() {
    let vnode = vnode_of(key);
    vnode_buckets[vnode as usize].push((key, value));
}
// Write one file per vnode range
```

This is cold-path code (runs once per checkpoint, not per event), so the O(n)
iteration is acceptable.

### Phase 2 Changes (Distribution-Ready Checkpoints)

#### 7. Per-vnode-range checkpoint files

Instead of one `state.bin`, write N files (e.g., 16 files, each covering 16 vnodes):

```
checkpoint_000042/
  manifest.json         # vnode_count: 256, vnode_ranges: [...]
  state_vr_00.bin       # vnodes 0-15
  state_vr_01.bin       # vnodes 16-31
  ...
  state_vr_15.bin       # vnodes 240-255
```

Each node in distributed mode loads only its assigned vnode ranges.

### What NOT to Build

| Skip | Why |
|------|-----|
| State migration protocol | Shared storage (S3 checkpoint) eliminates node-to-node transfers |
| Raft state replication | Checkpoint-to-S3 is the replication mechanism |
| Standby replicas | Any node loads from checkpoint storage on demand |
| Distributed barrier protocol | Single-coordinator barrier injection is sufficient initially |
| Async state access (Flink V2) | Only needed when state is remote; LaminarDB's hot path is local |

## Implementation Order

| Step | What | Files | LOC | Status |
|------|------|-------|-----|--------|
| 0a | `VNODE_COUNT` constant + `vnode_of()` function | state/mod.rs | 29 | DONE |
| 0b | `vnode_count` in `CheckpointManifest` | checkpoint_manifest.rs | 7 | DONE |
| 0c | `vnode_count` in `StreamExecutorCheckpoint` (version bump to 2) | aggregate_state.rs, stream_executor.rs | 6 | DONE |
| 0d | Reactor `trigger_checkpoint` returns StateSnapshot | reactor/mod.rs, core_handle.rs, operator/mod.rs | 19 | DONE |
| ~~1a~~ | ~~Prepend vnode to stream_join state keys~~ | — | — | REJECTED (breaks prefix scans) |
| ~~1b~~ | ~~Prepend vnode to session_window state keys~~ | — | — | REJECTED |
| ~~1c~~ | ~~Prepend vnode to window/sliding_window state keys~~ | — | — | REJECTED |
| ~~1d~~ | ~~Add `snapshot_vnode_range()` to StateStore trait~~ | — | — | DEFERRED (Phase 2, no callers yet) |
| 2a | Per-vnode checkpoint file writer (compute vnode at serialization time) | checkpoint_store.rs | ~100 | FUTURE |
| 2b | Restore from vnode-range files | recovery_manager.rs | ~80 | FUTURE |

**Steps 1a-1c REJECTED**: Embedding vnodes in live state keys would break O(log n)
`prefix_scan()` on the Ring 0 hot path. Instead, vnodes are computed at checkpoint
serialization time (Phase 2). This matches RisingWave's architecture.

**Step 1d DEFERRED**: `snapshot_vnode_range()` has no callers until Phase 2. Adding
a trait method with zero call sites is dead code.

## Risk Assessment

| Risk | Mitigation |
|------|-----------|
| Wrong vnode count | 256 is safe for years; no backward compat burden to change |
| FNV-1a distribution quality | Well-studied; used by many hash tables; mod-256 distribution is uniform |
| `vnode_of()` has no callers yet | 10-line utility; Phase 2 foundation; cost of keeping < cost of forgetting |

## References

- Flink 2.0 ForSt: VLDB 2025, "Disaggregated State Management in Apache Flink 2.0"
- RisingWave Hummock: risingwave.com/blog/hummock-a-storage-engine-designed-for-stream-processing/
- Styx: SIGMOD 2025, "Transactional Stateful Functions on Streaming Dataflows"
- Flink Key Groups: flink.apache.org/2017/07/04/a-deep-dive-into-rescalable-state-in-apache-flink/
- Kafka Streams lessons: engineering.contentsquare.com/2021/ten-flink-gotchas/
