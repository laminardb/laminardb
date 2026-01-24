# ADR-004: Checkpoint Strategy for Exactly-Once Semantics

## Status
Proposed

## Context

LaminarDB requires a checkpointing strategy that:

1. Maintains Ring 0 latency (<500ns) during checkpoint operations
2. Provides exactly-once semantics for source replay
3. Supports fast recovery (<60s for 10GB state)
4. Integrates with thread-per-core architecture (F013)
5. Enables WAL truncation to bound storage

The current F008 implementation has critical limitations:
- Checkpoint blocks Ring 0 (synchronous file I/O)
- Full snapshots only (no incremental)
- Single WAL shared across cores (contention with F013)
- No RocksDB integration (despite dependency existing)

Research from January 2026 identifies best practices from Flink, RisingWave, and LMDB.

## Decision Drivers

1. **Ring 0 Constraints**
   - Zero allocations on hot path
   - No locks or blocking I/O
   - <500ns state operations
   - <10ns epoch advance

2. **Durability Requirements**
   - Per-commit durability (WAL + fsync)
   - Periodic checkpoints for recovery speed
   - Source offset tracking for exactly-once
   - Watermark persistence for correct resumption

3. **Performance Requirements**
   - Checkpoint creation: <500ms
   - Recovery: <60s for 10GB state
   - Incremental size: <10% of full state for 1% changes
   - WAL group commit: <100μs

4. **Architectural Requirements**
   - Thread-per-core compatible (F013)
   - No cross-core contention
   - Background I/O only

## Considered Options

### Option 1: Full Snapshot Checkpointing (Current)

**Approach:**
- Serialize entire state to file
- Block reactor during checkpoint
- No incremental tracking

**Pros:**
- Simple implementation
- Proven working (F008)
- No RocksDB dependency

**Cons:**
- Blocks Ring 0 (violates latency requirement)
- Full snapshot for every checkpoint
- O(state) checkpoint size regardless of changes
- Single WAL contention with F013

### Option 2: WAL-Only (No Checkpoints)

**Approach:**
- Rely entirely on WAL for durability
- Replay entire WAL on recovery

**Pros:**
- Simplest implementation
- Per-commit durability
- No checkpoint overhead

**Cons:**
- Recovery time O(all events ever)
- WAL grows unbounded
- Eventually unrecoverable

### Option 3: Copy-on-Write mmap (LMDB Style)

**Approach:**
- Use MAP_PRIVATE for copy-on-write
- Fork for checkpoints
- Parent continues processing while child writes

**Pros:**
- Zero hot-path overhead
- Incremental via OS memory tracking
- Proven by LMDB

**Cons:**
- Windows compatibility issues
- Fork overhead (~1ms for large processes)
- Complex memory management
- Difficult with thread-per-core (fork + threads)

### Option 4: Three-Tier Hybrid (Recommended)

**Approach:**
```
Ring 0: mmap State Store ──▶ Changelog Buffer (zero-alloc)
                │
                ▼ async drain
Ring 1: WAL Writer ──▶ RocksDB ──▶ Incremental Checkpoint
                │
                ▼ periodic
Ring 2: Object Storage (future)
```

**Pros:**
- Ring 0 stays lock-free (<500ns)
- Async I/O in Ring 1
- Incremental via RocksDB SSTables (hard-links)
- Per-core WAL eliminates contention
- Industry proven (Flink GIC, RisingWave)

**Cons:**
- RocksDB dependency
- More complex implementation
- Multiple failure modes to handle

### Option 5: Unaligned Checkpoints (Flink 1.11+)

**Approach:**
- Barriers overtake in-flight data
- Snapshot includes in-flight buffers

**Pros:**
- Eliminates backpressure impact
- Faster checkpoint completion

**Cons:**
- Larger checkpoint size
- Complex barrier handling
- More difficult to implement correctly
- Overkill for embedded use case

## Decision

**Implement Three-Tier Hybrid Architecture (Option 4)**

This approach:
1. Maintains Ring 0 constraints with zero-alloc changelog
2. Enables thread-per-core with per-core WAL segments
3. Provides incremental checkpoints via RocksDB
4. Allows WAL truncation after checkpoint
5. Follows proven industry patterns (Flink GIC, RisingWave)

**Core Invariant:**
```
Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State
```

### Implementation Components

| Component | Ring | Feature | Status |
|-----------|------|---------|--------|
| ChangelogBuffer | 0 | F022 | New |
| Per-Core WAL | 1 | F062 | New |
| RocksDB Incremental | 1 | F022 | New |
| Checkpoint Coordinator | 1 | F022 | New |
| Recovery Manager | 1 | F022 | Update |

### Data Flow

**Write Path:**
1. Event arrives at Ring 0
2. `mmap_state.put()` writes to mmap (~100ns)
3. `changelog.push(offset_ref)` stores reference (~50ns)
4. Background: `changelog.drain()` → `wal.append()` → `wal.sync()`
5. Periodic: `wal.replay()` → `rocksdb.write_batch()` → `checkpoint.create()`

**Recovery Path:**
1. Find latest valid checkpoint (metadata.json exists)
2. Open RocksDB from checkpoint directory
3. Full scan RocksDB → rebuild mmap state store
4. Merge per-core WAL segments by epoch
5. Replay WAL entries after checkpoint.wal_position
6. Return source_offsets for upstream replay

## Consequences

### Positive

- Ring 0 latency maintained (<500ns)
- Incremental checkpoints reduce I/O
- Per-core WAL eliminates F013 contention
- WAL truncation bounds storage
- Fast recovery via RocksDB SSTable reads
- Industry-aligned architecture

### Negative

- RocksDB adds ~10MB binary size
- More complex failure handling
- RocksDB memory overhead (~100MB default)
- Need to tune RocksDB for streaming workload

### Risks

- RocksDB version compatibility
- Checkpoint corruption requires full replay
- Per-core WAL merge complexity
- Memory pressure during large checkpoints

### Mitigations

1. **RocksDB tuning**: Disable compression, use small blocks
2. **Corruption**: Keep N checkpoints, fall back on corruption
3. **Merge**: Simple epoch-based sorting, deterministic
4. **Memory**: Incremental RocksDB writes, not full load

## Implementation Plan

### Phase 1: F022 Update (1-2 weeks)
- ChangelogBuffer with SpscQueue
- Async WAL drain
- RocksDB integration
- Incremental checkpoints

### Phase 2: F062 Per-Core WAL (3-5 days)
- Per-core WAL writers
- Checkpoint coordination
- Segment merge logic
- Recovery from multiple segments

### Phase 3: Integration (3-5 days)
- Thread-per-core integration
- End-to-end recovery tests
- Performance validation
- Documentation

## Metrics & Validation

| Metric | Target | Validation |
|--------|--------|------------|
| `state.put()` latency | <500ns | Benchmark during checkpoint |
| Checkpoint creation | <500ms | Measure end-to-end |
| Recovery time | <60s/10GB | Load test with 10GB state |
| Incremental size | <10% | Compare 1% mutation checkpoint |
| Ring 0 p99 | <1μs | Measure during checkpoint |

## References

- [Checkpoint Implementation Prompt](../research/checkpoint-implementation-prompt.md)
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpoints/)
- [Flink GIC (Generic Incremental Checkpoints)](https://flink.apache.org/2022/05/30/generalized-incremental-checkpointing/)
- [RocksDB Checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints)
- [LMDB Copy-on-Write](https://www.symas.com/lmdb)
- [Chandy-Lamport Algorithm](https://en.wikipedia.org/wiki/Chandy%E2%80%93Lamport_algorithm)
