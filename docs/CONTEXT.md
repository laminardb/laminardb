# Session Context

> This file tracks session continuity. Update at the end of each session.

## Last Session

**Date**: 2026-01-24
**Duration**: Continued session

### What Was Accomplished
- ✅ **F071: Zero-Allocation Enforcement** - IMPLEMENTATION COMPLETE
  - `HotPathDetectingAlloc` - Custom global allocator that panics on hot path allocation
  - `HotPathGuard` - RAII guard with nesting support for marking hot path sections
  - `hot_path!` macro - Convenience macro for function entry
  - `ObjectPool<T, N>` - Fixed-size pool for zero-allocation acquire/release
  - `RingBuffer<T, N>` - Fixed-capacity circular buffer
  - `ScratchBuffer` - Thread-local temporary storage with 64KB default
  - Feature flag `allocation-tracking` for opt-in detection
  - 33 new unit tests, all passing
- ✅ **Previous Session**: Thread-Per-Core Research specs (F067-F072)

### F071 Implementation Details

**New Module**: `crates/laminar-core/src/alloc/`
```
alloc/
├── mod.rs          # Public exports
├── detector.rs     # HotPathDetectingAlloc, AllocationStats
├── guard.rs        # HotPathGuard, hot_path! macro
├── object_pool.rs  # ObjectPool<T, N>
├── ring_buffer.rs  # RingBuffer<T, N>
└── scratch.rs      # ScratchBuffer, thread-local API
```

**Usage**:
```rust
use laminar_core::alloc::{HotPathGuard, ObjectPool, RingBuffer, ScratchBuffer};
use laminar_core::hot_path;

// Mark hot path section (panics on allocation with allocation-tracking feature)
fn process_event(event: &Event) {
    let _guard = HotPathGuard::enter("process_event");
    // or use: hot_path!("process_event");

    // Zero-allocation patterns:
    let mut pool: ObjectPool<Buffer, 16> = ObjectPool::new();
    let buf = pool.acquire().unwrap();
    pool.release(buf);
}
```

**Feature Flag**:
```toml
[dependencies]
laminar-core = { version = "0.1", features = ["allocation-tracking"] }
```

### Previous Session Accomplishments

### Thread-Per-Core Research Analysis Summary

From `docs/research/laminardb-thread-per-core-2026-research.md`, identified critical gaps:

| Gap | Research Finding | Current (F013) | Fix |
|-----|------------------|----------------|-----|
| io_uring basic only | "2.05x improvement with SQPOLL" | ❌ No io_uring | **F067** |
| No NUMA awareness | "2-3x latency on remote access" | ❌ Generic allocation | **F068** |
| Single I/O ring | "3 rings: latency/main/poll" | ❌ Single reactor | **F069** |
| No task budgeting | "Ring 0: 500ns budget" | ❌ No enforcement | **F070** |
| No allocation detection | "Zero-alloc verification" | ✅ Implemented | **F071** |
| No XDP steering | "26M packets/sec/core" | ❌ Standard sockets | **F072** |
| CPU pinning | "Cache efficiency" | ✅ Implemented | F013 |
| Lock-free SPSC | "~4.8ns per op" | ✅ Implemented | F014 |

**Thread-Per-Core Evolution Path**:
```
F013 (Foundation) ──┬──▶ F067 (io_uring) ──▶ F069 (Three-Ring)
      ✅ Complete   ├──▶ F068 (NUMA) ──▶ Production Deployment
                    ├──▶ F070 (Task Budget) ──▶ Latency SLAs
                    └──▶ F071 (Zero-Alloc) ──▶ F072 (XDP) [P2]
```

### Previous Session Accomplishments
- ✅ **Emit Patterns Research Analysis** - Compared 2026 research against implementation
- ✅ **F011B: EMIT Clause Extension** - NEW SPEC created for OnWindowClose, Changelog, Final strategies
- ✅ **F063: Changelog/Retraction (Z-Sets)** - NEW SPEC for Z-set weights, CDC envelope, retractable aggregators
- ✅ **F023 Updated** - Added dependencies on F011B and F063
- ✅ All feature specs aligned with 2026 emit patterns research

### Previous Accomplishments (same session)
- ✅ **F013: Thread-Per-Core Architecture** - Full implementation complete
- ✅ **F014: SPSC Queue** - Lock-free bounded queue with cache padding
- ✅ **Credit-Based Backpressure** - Apache Flink-style flow control added
- ✅ **F016: Sliding Windows** - Overlapping window support with multi-window assignment
- ✅ **F019: Stream-Stream Joins** - Time-bounded joins with Inner/Left/Right/Full types
- ✅ **F020: Lookup Joins** - Cached reference table lookups with TTL, inner/left join support
- ✅ All 369 tests passing across all crates (282 core + 56 sql + 25 storage + 6 connectors)
- ✅ Clippy clean for all crates
- ✅ TPC benchmarks added (`cargo bench --bench tpc_bench`)

### Emit Patterns Analysis Summary

From `docs/research/emit-patterns-research-2026.md`, identified critical gaps:

| Gap | Research Finding | Status | Fix |
|-----|------------------|--------|-----|
| EMIT ON WINDOW CLOSE | Essential for append-only sinks | SQL parsed, not in EmitStrategy | **F011B** |
| Changelog/Retraction | DBSP Z-sets fundamental | Not implemented | **F063** |
| EMIT CHANGES | CDC pipelines need delta | Missing | **F011B** |
| EMIT FINAL | BI reporting needs exact | Missing | **F011B** |
| CDC Envelope | Debezium compatibility | Missing | **F063** |

**New Dependency Chain**:
```
F011 (EMIT Clause) ──► F011B (Extension) ──┐
                                           ├──► F023 (Exactly-Once Sinks)
F063 (Changelog/Retraction) ──────────────┘
                           │
                           └──► F060 (Cascading MVs)
```

### F013/F014 Implementation Details

**Module Structure**:
```
crates/laminar-core/src/tpc/
├── mod.rs           # Public exports, TpcError enum
├── spsc.rs          # Lock-free SPSC queue with CachePadded<T>
├── router.rs        # KeyRouter for event partitioning
├── core_handle.rs   # CoreHandle per-core reactor wrapper
├── backpressure.rs  # Credit-based flow control (NEW)
└── runtime.rs       # ThreadPerCoreRuntime multi-core orchestration
```

**Key Components**:
- `SpscQueue<T>` - Lock-free single-producer single-consumer queue
  - Atomic head/tail with Acquire/Release ordering
  - Power-of-2 capacity for fast modulo
  - Batch push/pop operations
  - Achieved: ~4.8ns per operation (10x better than 50ns target)

- `CachePadded<T>` - 64-byte aligned wrapper to prevent false sharing

- `KeyRouter` - Routes events to cores by key hash
  - FxHash for fast, consistent hashing
  - Supports column names, indices, round-robin, all-columns

- `CoreHandle` - Per-core reactor thread management
  - CPU affinity (Linux/Windows)
  - SPSC inbox/outbox queues
  - Credit-based backpressure integration
  - CoreMessage enum for events, watermarks, checkpoints

- `CreditGate` / `BackpressureConfig` - Credit-based flow control
  - Exclusive + floating credits (like Flink's network stack)
  - High/low watermarks for hysteresis
  - Three overflow strategies: Block, Drop, Error
  - Lock-free atomic credit tracking
  - Per-core metrics (acquired, released, blocked, dropped)

- `ThreadPerCoreRuntime` - Multi-core orchestration
  - Builder pattern for configuration
  - submit/poll/stats operations
  - OperatorFactory for per-core operators

### F016 Sliding Windows Implementation

**Module**: `crates/laminar-core/src/operator/sliding_window.rs`

**Key Components**:
- `SlidingWindowAssigner` - Assigns events to multiple overlapping windows
  - Configurable size and slide interval
  - `windows_per_event` cached for performance (ceil(size/slide))
  - Handles negative timestamps correctly
  - Returns windows in chronological order via `SmallVec<[WindowId; 4]>`

- `SlidingWindowOperator<A: Aggregator>` - Processes events through overlapping windows
  - Each event updates `ceil(size/slide)` windows
  - Supports all EmitStrategies (OnWatermark, Periodic, OnUpdate)
  - Late data handling with side outputs
  - Checkpoint/restore for fault tolerance
  - Skips closed windows when processing late (but within lateness) events

**Example**:
```rust
use laminar_core::operator::sliding_window::{
    SlidingWindowAssigner, SlidingWindowOperator,
};
use laminar_core::operator::window::CountAggregator;
use std::time::Duration;

// 1-hour window with 15-minute slide (4 windows per event)
let assigner = SlidingWindowAssigner::new(
    Duration::from_secs(3600),  // 1 hour
    Duration::from_secs(900),   // 15 minutes
);
let operator = SlidingWindowOperator::new(
    assigner,
    CountAggregator::new(),
    Duration::from_secs(60),  // 1 minute grace period
);
```

### F019 Stream-Stream Joins Implementation

**Module**: `crates/laminar-core/src/operator/stream_join.rs`

**Key Components**:
- `StreamJoinOperator` - Time-bounded stream-stream join operator
  - Supports Inner, Left, Right, and Full outer joins
  - Configurable time bound for matching events
  - State stored using prefixed keys (`sjl:` for left, `sjr:` for right)
  - Automatic cleanup via watermark-based timers
  - Late event handling

- `JoinType` - Enum for join semantics
  - `Inner`: Only emit matched pairs
  - `Left`: Emit all left events, with right match if exists
  - `Right`: Emit all right events, with left match if exists
  - `Full`: Emit all events, with matches where they exist

- `JoinSide` - Identifies event source (Left/Right)

- `JoinRow` - Serialized event storage using Arrow IPC
  - Stores timestamp, key value, and serialized batch data
  - Tracks matched state for outer joins

- `JoinMetrics` - Operational metrics
  - Event counts (left/right)
  - Match counts
  - Unmatched event counts (for outer joins)
  - Late event and cleanup counters

**Example**:
```rust
use laminar_core::operator::stream_join::{
    StreamJoinOperator, JoinType, JoinSide,
};
use std::time::Duration;

// Join orders with payments within 1 hour
let mut operator = StreamJoinOperator::new(
    "order_id".to_string(),  // left key column
    "order_id".to_string(),  // right key column
    Duration::from_secs(3600), // 1 hour time bound
    JoinType::Inner,
);

// Process left-side event (order)
let outputs = operator.process_side(&order_event, JoinSide::Left, &mut ctx);

// Process right-side event (payment)
let outputs = operator.process_side(&payment_event, JoinSide::Right, &mut ctx);
```

### Where We Left Off
Phase 2 P0 features F013, F016, and F019 complete. Ready to continue with lookup joins and exactly-once sinks.

### Immediate Next Steps

1. **Continue Phase 2** - Production Hardening
   - F020: Lookup Joins (P0)
   - F023: Exactly-Once Sinks (P0)
   - F017: Session Windows (P1)

### Open Issues

| Issue | Severity | Feature | Notes |
|-------|----------|---------|-------|
| None | - | - | Phase 2 underway |

### Code Pointers

**TPC Public API**:
```rust
use laminar_core::tpc::{TpcConfig, ThreadPerCoreRuntime, KeySpec};

// Configure for 4 cores, routing by "user_id" column
let config = TpcConfig::builder()
    .num_cores(4)
    .key_columns(vec!["user_id".to_string()])
    .cpu_pinning(true)
    .build()?;

let runtime = ThreadPerCoreRuntime::new(config)?;

// Submit events - automatically routed by key
runtime.submit(event)?;

// Poll all cores for outputs
let outputs = runtime.poll();
```

**Backpressure Configuration**:
```rust
use laminar_core::tpc::{BackpressureConfig, OverflowStrategy, CoreConfig};

// Configure credit-based flow control
let bp_config = BackpressureConfig::builder()
    .exclusive_credits(4)      // Per-sender reserved credits
    .floating_credits(8)       // Shared pool for backlog priority
    .high_watermark(0.8)       // Start throttling at 80% queue usage
    .low_watermark(0.5)        // Resume at 50% queue usage
    .overflow_strategy(OverflowStrategy::Block)  // or Drop, Error
    .build();

// CoreHandle exposes backpressure state
let handle: &CoreHandle = ...;
handle.is_backpressured();     // Check if throttling active
handle.available_credits();    // Current credit count
handle.credit_metrics();       // Acquired, released, blocked, dropped
```

---

## Phase 2 Progress

| Feature | Status | Notes |
|---------|--------|-------|
| F013: Thread-Per-Core | ✅ Complete | SPSC queues, key routing, CPU pinning |
| F014: SPSC Queues | ✅ Complete | Part of F013 implementation |
| F015: CPU Pinning | ✅ Complete | Included in F013 |
| F016: Sliding Windows | ✅ Complete | Multi-window assignment, 25 tests |
| F017: Session Windows | 📝 Not started | |
| F018: Hopping Windows | ✅ Complete | Alias for sliding windows |
| F019: Stream-Stream Joins | ✅ Complete | Inner/Left/Right/Full, 14 tests |
| F020: Lookup Joins | ✅ Complete | Cached lookups with TTL, 16 tests |
| F023: Exactly-Once Sinks | 📝 Not started | |
| F071: Zero-Alloc Enforcement | ✅ Complete | HotPathGuard, ObjectPool, RingBuffer, 33 tests |

---

## Quick Reference

### Current Focus
- **Phase**: 2 Production Hardening
- **Active Feature**: F071 complete (8/28), ready for F067 (io_uring) or F023 (exactly-once)

### Key Files
```
crates/laminar-core/src/alloc/
├── mod.rs           # Public exports, hot_path! macro
├── detector.rs      # HotPathDetectingAlloc, AllocationStats
├── guard.rs         # HotPathGuard (RAII, nesting support)
├── object_pool.rs   # ObjectPool<T, N> (fixed-size pool)
├── ring_buffer.rs   # RingBuffer<T, N> (circular buffer)
└── scratch.rs       # ScratchBuffer (thread-local temp storage)

crates/laminar-core/src/tpc/
├── mod.rs           # TpcError, public exports
├── spsc.rs          # SpscQueue<T>, CachePadded<T>
├── router.rs        # KeyRouter, KeySpec
├── core_handle.rs   # CoreHandle, CoreConfig, CoreMessage
└── runtime.rs       # ThreadPerCoreRuntime, TpcConfig

crates/laminar-core/src/operator/
├── mod.rs           # Operator trait, Event, Output types
├── window.rs        # TumblingWindowOperator, WindowAssigner trait
├── sliding_window.rs# SlidingWindowOperator, SlidingWindowAssigner
├── stream_join.rs   # StreamJoinOperator, JoinType, JoinSide
└── lookup_join.rs   # LookupJoinOperator, TableLoader trait

crates/laminar-connectors/src/lookup.rs  # TableLoader trait, InMemoryTableLoader

Benchmarks: crates/laminar-core/benches/tpc_bench.rs

Tests: 369 passing (282 core, 56 sql, 25 storage, 6 connectors)
```

### Useful Commands
```bash
# Run all tests
cargo test --all --lib

# Run TPC tests only
cargo test -p laminar-core tpc --lib

# Run TPC benchmarks
cargo bench --bench tpc_bench

# Clippy
cargo clippy --all -- -D warnings
```

### Recent Decisions
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-24 | Custom SPSC over crossbeam | Precise cache layout control |
| 2026-01-24 | `#[repr(C, align(64))]` for CachePadded | Hardware cache line alignment |
| 2026-01-24 | FxHash for key routing | Faster than std HashMap for small keys |
| 2026-01-24 | Factory pattern for per-core operators | No shared state between cores |

---

## History

### Previous Sessions

<details>
<summary>Session - 2026-01-24 (F013 Thread-Per-Core)</summary>

**Accomplished**:
- Implemented F013 Thread-Per-Core Architecture
- Created tpc module with spsc.rs, router.rs, core_handle.rs, runtime.rs
- Lock-free SPSC queue with CachePadded wrapper
- KeyRouter for FxHash-based event partitioning
- CoreHandle with CPU affinity (Linux/Windows)
- ThreadPerCoreRuntime with builder pattern
- Added tpc_bench.rs with comprehensive benchmarks
- All 267 tests passing, clippy clean

**Key Files**:
- `crates/laminar-core/src/tpc/` - TPC module
- `crates/laminar-core/benches/tpc_bench.rs` - Benchmarks

</details>

<details>
<summary>Session - 2026-01-24 (WAL Hardening)</summary>

**Accomplished**:
- Changed `sync_all()` to `sync_data()` (fdatasync)
- Added CRC32C checksums to WAL records
- Added torn write detection with `WalReadResult` enum
- Added `repair()` method to truncate to last valid record
- Added watermark to `WalEntry::Commit` and `CheckpointMetadata`
- All 217 tests passing

**Key Changes**:
- WAL record format: `[length: 4][crc32: 4][data: length]`
- `WalError::ChecksumMismatch` and `WalError::TornWrite` error types
- Recovery restores watermark from checkpoint

</details>

<details>
<summary>Session - 2026-01-24 (Phase 1 Audit)</summary>

**Accomplished**:
- Comprehensive audit of all 12 Phase 1 features
- Identified 16 gaps against 2025-2026 best practices
- Prioritized into P0/P1/P2 categories
- Created PHASE1_AUDIT.md with full audit report

**Key Findings**:
- WAL durability issues (fsync, no checksums, no torn write detection)
- Watermark not persisted (recovery loses progress)
- No recovery integration test

</details>

<details>
<summary>Session - 2026-01-24 (Late Data Handling - F012)</summary>

**Accomplished**:
- Implemented F012 - Late Data Handling
- Added `LateDataConfig` struct with drop/side-output options
- Added `LateDataMetrics` for tracking late events
- Phase 1 features complete (100%)

</details>

<details>
<summary>Session - 2026-01-24 (EMIT Clause - F011)</summary>

**Accomplished**:
- Implemented F011 - EMIT Clause with 3 strategies
- OnWatermark, Periodic, OnUpdate emit modes
- Periodic timer system with special key encoding

</details>

<details>
<summary>Session - 2026-01-24 (Watermarks - F010)</summary>

**Accomplished**:
- Implemented F010 - Watermarks with 5 generation strategies
- WatermarkTracker for multi-source alignment
- Idle source detection and MeteredGenerator wrapper

</details>

<details>
<summary>Session - 2026-01-23 (Checkpointing - F008)</summary>

**Accomplished**:
- Fixed Ring 0 hot path violations
- Implemented reactor features (CPU affinity, sinks, graceful shutdown)
- Implemented F008 - Basic Checkpointing

</details>

<details>
<summary>Session - 2026-01-22 (rkyv migration)</summary>

**Accomplished**:
- Migrated serialization from bincode to rkyv
- Updated all types for zero-copy deserialization

</details>
