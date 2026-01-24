# LaminarDB Phase 1 Audit Report

> **Date**: January 2026
> **Audited Against**: 2025-2026 streaming database best practices

## Executive Summary

| Category | Count |
|----------|-------|
| Features Audited | 12 |
| Fully Implemented | 5 |
| Partial (gaps found) | 6 |
| Needs Work | 1 |

**Overall Assessment**: Phase 1 is functionally complete but has critical gaps in WAL durability and watermark persistence that must be fixed before Phase 2.

---

## Feature-by-Feature Audit

### F001: Core Reactor Event Loop

**Files**: `crates/laminar-core/src/reactor/mod.rs`

| Check | Status | Notes |
|-------|--------|-------|
| io_uring used | ❌ | Using blocking I/O and `std::thread::yield_now()` |
| SQPOLL enabled | ❌ | Not applicable - no io_uring |
| Registered buffers | ❌ | Not applicable - no io_uring |
| CPU affinity | ✅ | `sched_setaffinity` (Linux) + `SetThreadAffinityMask` (Windows) |
| Ring 0/1/2 scheduling | ⚠️ | Documented but Ring 1 communication not fully implemented |
| SPSC queues lock-free | ⚠️ | Uses `VecDeque` (may allocate), `AtomicBool` for shutdown |
| Buffer swapping | ✅ | `operator_buffer_1`/`operator_buffer_2` pattern avoids allocations |
| Graceful shutdown | ✅ | Drains queue before exit, flushes sink |

**Recommendation**: io_uring integration is P1 for Phase 2 (F013 thread-per-core).

---

### F002: Memory-Mapped State Store

**Files**: `crates/laminar-core/src/state/mmap.rs`

| Check | Status | Notes |
|-------|--------|-------|
| MAP_PRIVATE (CoW) | ❌ | Uses `map_mut` (shared mapping), no checkpoint isolation |
| Huge pages | ❌ | No `MAP_HUGETLB` support |
| madvise hints | ❌ | No `MADV_SEQUENTIAL`/`MADV_RANDOM` calls |
| FxHashMap index | ✅ | Fast hash map for small keys |
| Compaction | ✅ | `compact()` method to reclaim space |
| Fragmentation tracking | ✅ | `fragmentation()` method |
| Memory limit/eviction | ❌ | No cap on unbounded state growth |
| Index persistence | ❌ | Index lost on restart (only mmap file kept) |

**Recommendation**: Add MAP_PRIVATE for checkpoint isolation in P1.

---

### F003: State Store Interface

**Files**: `crates/laminar-core/src/state/mod.rs`

| Check | Status | Notes |
|-------|--------|-------|
| Iterator pooling | ❌ | Creates new `Box<dyn Iterator>` per scan |
| Prefix scan efficiency | ⚠️ | O(n) full table scan with filter |
| Range deletes | ❌ | Not implemented |
| State namespacing | ⚠️ | Window uses `win:` prefix manually, not enforced |
| State size API | ✅ | `size_bytes()` for backpressure decisions |
| rkyv typed access | ✅ | `StateStoreExt::get_typed/put_typed` |

**Recommendation**: Prefix scan optimization is P2 (affects large state performance).

---

### F004: Tumbling Windows

**Files**: `crates/laminar-core/src/operator/window.rs`

| Check | Status | Notes |
|-------|--------|-------|
| Window state keyed | ✅ | `(prefix, window_start, window_end)` in 20-byte key |
| Incremental aggregates | ✅ | Accumulators update in place |
| Watermark-triggered emission | ✅ | Timers fire at `window_end + allowed_lateness` |
| Late data handling | ✅ | `LateDataConfig`, side output routing |
| State GC after watermark | ✅ | `delete_accumulator` in `on_timer` |
| SmallVec optimization | ✅ | `WindowIdVec = SmallVec<[WindowId; 4]>` |

**Status**: Complete, no gaps.

---

### F005: DataFusion Integration

**Files**: `crates/laminar-sql/src/datafusion/`

| Check | Status | Notes |
|-------|--------|-------|
| StreamSource trait | ✅ | Implemented in `source.rs` |
| Push-to-pull bridge | ✅ | `StreamBridge` with tokio mpsc |
| StreamingTableProvider | ✅ | DataFusion table provider implemented |
| Custom streaming rules | ⚠️ | Basic integration, no advanced optimizer rules |
| Predicate pushdown | ⚠️ | Uses DataFusion defaults |
| EXPLAIN STREAMING | ❌ | Not implemented |

**Recommendation**: EXPLAIN STREAMING is P2 (debugging feature).

---

### F006: Basic SQL Parser

**Files**: `crates/laminar-sql/src/parser/parser_simple.rs`

| Check | Status | Notes |
|-------|--------|-------|
| CREATE SOURCE | ✅ | Parsed (simplified implementation) |
| CREATE SINK | ✅ | Parsed (simplified implementation) |
| CREATE CONTINUOUS QUERY | ✅ | Parsed with EMIT clause extraction |
| EMIT clause | ✅ | ON WATERMARK, ON UPDATE, EVERY INTERVAL |
| Window functions | ⚠️ | Detection only (`has_window_function`) |
| WATERMARK FOR syntax | ❌ | Not implemented in CREATE SOURCE |

**Status**: POC implementation. Production parser needed for Phase 2 (see ADR-003).

---

### F007: Write-Ahead Log

**Files**: `crates/laminar-storage/src/wal.rs`

| Check | Status | Notes |
|-------|--------|-------|
| Per-core WAL | ❌ | Single WAL instance (cross-core contention) |
| Group commit | ✅ | `sync_interval` batches fsyncs |
| fdatasync vs fsync | ❌ | **Uses `sync_all()` (full fsync including metadata)** |
| io_uring writes | ❌ | Blocking `BufWriter::write_all` |
| Segment rotation | ❌ | Single file, grows unbounded |
| WAL truncation | ✅ | `truncate()` after checkpoint |
| Source offset tracking | ⚠️ | `WalEntry::Commit { offsets }` exists but not integrated |
| CRC32 checksum | ❌ | **No integrity validation** |
| Torn write detection | ❌ | **Partial entry at tail not detected** |
| Length-prefixed entries | ✅ | 4-byte length + entry bytes |

**Critical Issues**:
1. `sync_all()` → `sync_data()` saves 50-100μs per sync
2. No CRC32 means corruption goes undetected
3. No torn write detection means recovery may fail after crash

---

### F008: Basic Checkpointing

**Files**: `crates/laminar-storage/src/checkpoint.rs`

| Check | Status | Notes |
|-------|--------|-------|
| Async checkpoint creation | ❌ | Blocking `fs::write` calls |
| Incremental checkpointing | ❌ | Full state snapshot each time |
| Checkpoint metadata | ✅ | ID, timestamp, WAL position, source offsets |
| Recovery tested | ⚠️ | Unit tests exist, no integration test |
| Changelog buffer | ❌ | No Ring 0 → Ring 1 buffer |
| Old checkpoint GC | ✅ | `cleanup_old_checkpoints()` with `max_retained` |
| Checkpoint validation | ❌ | No checksum verification |

**Recommendation**: Async checkpointing is P1 to avoid blocking Ring 0.

---

### F009: Event Time Processing

**Files**: `crates/laminar-core/src/time/event_time.rs`

| Check | Status | Notes |
|-------|--------|-------|
| Configurable extraction | ✅ | Column name or index |
| Epoch timestamp format | ✅ | Millis/secs/micros/nanos supported |
| Multiple timestamp columns | ❌ | Single extractor per batch |
| Precision configurable | ✅ | Via `TimestampFormat` enum |
| SQL TIMESTAMP BY | ❌ | Not implemented |
| Column index caching | ✅ | Avoids repeated lookups |

**Status**: Complete for Phase 1 requirements.

---

### F010: Watermarks

**Files**: `crates/laminar-core/src/time/watermark.rs`

| Check | Status | Notes |
|-------|--------|-------|
| Per-source watermark | ✅ | `WatermarkTracker` per-source |
| MIN across inputs | ✅ | `update_combined()` computes min |
| Idle source detection | ✅ | `idle_timeout` + `check_idle_sources()` |
| Idle timeout configurable | ✅ | `with_idle_timeout()` constructor |
| Watermark alignment | ❌ | No pause-fast-sources mechanism |
| Advance during backpressure | ⚠️ | Watermark generator is event-driven |
| CURRENT_WATERMARK() function | ❌ | Not exposed in SQL |
| Watermark in WAL | ❌ | **Not persisted for recovery** |

**Critical Issue**: Watermark must be persisted in WAL for recovery.

---

### F011: EMIT Clause

**Files**: `crates/laminar-core/src/operator/window.rs`

| Check | Status | Notes |
|-------|--------|-------|
| EMIT ON UPDATE | ✅ | `EmitStrategy::OnUpdate` |
| EMIT ON WINDOW CLOSE | ✅ | `EmitStrategy::OnWatermark` |
| Per-view EMIT | ⚠️ | Per-operator, not SQL-level |
| Watermark-based close | ✅ | Timer fires at `window_end + lateness` |
| emit_final equivalent | ❌ | Not implemented |
| Retraction suppression | ⚠️ | No formal retraction system yet |

**Status**: Complete for Phase 1 requirements.

---

### F012: Late Data Handling

**Files**: `crates/laminar-core/src/operator/window.rs`

| Check | Status | Notes |
|-------|--------|-------|
| Drop by default | ✅ | `LateDataConfig::drop()` |
| ALLOWED LATENESS | ✅ | `allowed_lateness_ms` in operator |
| Side output | ✅ | `Output::SideOutput { name, event }` |
| Retractions | ❌ | No -1/+1 weight system |
| Late event metrics | ✅ | `LateDataMetrics` tracking |
| LATENESS for GC | ❌ | No automatic state GC hint |

**Recommendation**: Retraction system needed for Phase 2 joins.

---

## Gap Analysis Summary

### P0 - Critical (Blocks Phase 2) - ALL COMPLETE ✅

| # | Gap | Feature | Status | Impact |
|---|-----|---------|--------|--------|
| 1 | WAL uses fsync not fdatasync | F007 | ✅ **FIXED** | 50-100μs wasted per sync |
| 2 | No CRC32 checksum in WAL | F007 | ✅ **FIXED** | Cannot detect corruption |
| 3 | No torn write detection | F007 | ✅ **FIXED** | Crash recovery may fail |
| 4 | Watermark not persisted | F010 | ✅ **FIXED** | Recovery loses progress |
| 5 | No recovery integration test | F007/F008 | ✅ **FIXED** | 6 comprehensive tests added |

### P1 - High (Early Phase 2)

| # | Gap | Feature | Effort | Impact |
|---|-----|---------|--------|--------|
| 6 | Per-core WAL segments | F007 | 2-3 days | Required for F013 |
| 7 | Async checkpointing | F008 | 2-3 days | Blocks Ring 0 currently |
| 8 | MAP_PRIVATE for checkpoints | F002 | 2-3 days | CoW isolation |
| 9 | io_uring integration | F001/F007 | 3-5 days | Blocking I/O on hot path |
| 10 | Production SQL parser | F006 | 1 week | POC only, see ADR-003 |

### P2 - Medium (Phase 2+)

| # | Gap | Feature | Effort | Impact |
|---|-----|---------|--------|--------|
| 11 | Prefix scan O(n) | F003 | 2-3 days | Slow for large state |
| 12 | Incremental checkpoints | F008 | 3-5 days | Checkpoint overhead |
| 13 | Retraction system | F012 | 1 week | Required for joins |
| 14 | madvise hints | F002 | 1 day | TLB optimization |
| 15 | Huge page support | F002 | 1 day | Large state optimization |
| 16 | WATERMARK FOR SQL | F006 | 2 days | RisingWave compatibility |

---

## Implementation Plan

### Phase 1 Hardening Sprint (P0 items)

**Goal**: Fix all P0 issues before starting Phase 2.

#### Task 1: WAL fsync → fdatasync (1 hour)

```rust
// crates/laminar-storage/src/wal.rs:187
// Change:
self.file.sync_all()?;
// To:
self.file.sync_data()?;
```

#### Task 2: WAL CRC32 Checksum (4 hours)

1. Add `crc32c` crate dependency
2. Create wrapper struct for WAL entries:
   ```rust
   pub struct WalRecord {
       pub crc32: u32,
       pub entry: WalEntry,
   }
   ```
3. Compute CRC32 before serialization
4. Validate CRC32 in `WalReader::next()`
5. Return error on mismatch

#### Task 3: Torn Write Detection (4 hours)

1. In `WalReader`, detect partial entry at tail:
   - Length prefix present but data incomplete
   - CRC32 fails on last entry
2. Truncate WAL to last valid entry
3. Add test: simulate crash mid-write

#### Task 4: Watermark Persistence (4 hours)

1. Add `watermark: i64` to `WalEntry::Commit`:
   ```rust
   Commit {
       epoch: u64,
       offsets: HashMap<String, u64>,
       watermark: i64,  // Add this
   }
   ```
2. Add `watermark: i64` to `CheckpointMetadata`
3. Restore watermark on recovery

#### Task 5: Recovery Integration Test (1 day)

1. Create state with multiple keys
2. Create checkpoint
3. Add more WAL entries after checkpoint
4. Simulate crash (drop without clean shutdown)
5. Recover and verify:
   - All checkpoint state restored
   - WAL entries after checkpoint replayed
   - Watermark restored correctly
6. Test both clean and dirty recovery paths

---

## Test Coverage Status

| Component | Unit Tests | Property Tests | Integration | Chaos |
|-----------|------------|----------------|-------------|-------|
| Reactor | ✅ | ❌ | ❌ | ❌ |
| State Store | ✅ | ❌ | ❌ | ❌ |
| WAL | ✅ | ❌ | ❌ | ❌ |
| Checkpoint | ✅ | ❌ | ⚠️ Partial | ❌ |
| Windows | ✅ | ❌ | ❌ | ❌ |
| Watermarks | ✅ | ❌ | ❌ | ❌ |

**Recommended additions**:
- Property tests for rkyv serialization roundtrips
- Integration test for full recovery flow (P0)
- Chaos test: kill during checkpoint
- Chaos test: corrupt WAL tail

---

## Performance Validation Status

| Metric | Target | Verified | Notes |
|--------|--------|----------|-------|
| State lookup | < 500ns | ⚠️ Claimed | Needs benchmark |
| Throughput/core | 500K/sec | ❌ Not tested | Needs benchmark |
| p99 latency | < 10μs | ❌ Not tested | Needs histogram |
| Checkpoint recovery | < 10s | ❌ Not tested | Needs benchmark |

**Action**: Add criterion benchmarks before Phase 2 gate.

---

## Appendix: Code Locations

### WAL (F007)
- **WalEntry enum**: `crates/laminar-storage/src/wal.rs:29-42`
- **sync_all call**: `crates/laminar-storage/src/wal.rs:187`
- **WalReader**: `crates/laminar-storage/src/wal.rs:200-280`

### Checkpoint (F008)
- **CheckpointMetadata**: `crates/laminar-storage/src/checkpoint.rs:25-35`
- **create_checkpoint**: `crates/laminar-storage/src/checkpoint.rs:100-150`
- **recover**: `crates/laminar-storage/src/checkpoint.rs:200-250`

### Watermark (F010)
- **WatermarkTracker**: `crates/laminar-core/src/time/watermark.rs:200-350`
- **current_watermark**: `crates/laminar-core/src/time/watermark.rs:280`

### Window Operator (F004, F011, F012)
- **TumblingWindowOperator**: `crates/laminar-core/src/operator/window.rs:200-500`
- **EmitStrategy**: `crates/laminar-core/src/operator/window.rs:30-55`
- **LateDataConfig**: `crates/laminar-core/src/operator/window.rs:59-105`
