# Session Context

> This file tracks session continuity. Update at the end of each session.

## Last Session

**Date**: 2026-01-25
**Duration**: Continued session

### What Was Accomplished
- ✅ **F022: Incremental Checkpointing** - IMPLEMENTATION COMPLETE
  - New `incremental` module in `crates/laminar-storage/src/incremental/`
  - `StateChangelogEntry` - 32-byte zero-allocation changelog entry for Ring 0
  - `StateChangelogBuffer` - SPSC ring buffer for changelog with atomic indices
  - `IncrementalCheckpointManager` - RocksDB-backed incremental checkpoints
  - `RecoveryManager` - Checkpoint + WAL replay recovery
  - `CheckpointConfig` - Configuration with interval, retention, WAL truncation
  - `RecoveryConfig` - Configuration with WAL repair, state change collection
  - Supports incremental checkpoints via RocksDB hard-linked SSTables
  - Core invariant: `Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State`
  - Automatic cleanup of old checkpoints beyond retention limit
  - 37 new unit tests, all passing
  - **Total tests**: 577 (448 core + 61 sql + 62 storage + 6 connectors)

- ✅ **F017: Session Windows** - IMPLEMENTATION COMPLETE (previous session)
  - New `session_window` module in `crates/laminar-core/src/operator/session_window.rs`
  - `SessionWindowOperator` - Dynamic windows based on activity gaps
  - `SessionState` - Per-key session tracking (start, end, key)
  - Gap-based session detection and closure via timers
  - Per-key session tracking with configurable key column
  - Session extension when events arrive within gap period
  - All emit strategies supported (OnWatermark, OnUpdate, Changelog, Final, OnWindowClose)
  - Late data handling (drop, side output, or silent drop with Final)
  - Checkpoint/restore support via rkyv serialization
  - Prepared for future session merging when late data bridges sessions
  - 23 new unit tests, all passing

- ✅ **F023: Exactly-Once Sinks** - IMPLEMENTATION COMPLETE (previous session)
  - New `sink` module in `crates/laminar-core/src/sink/`
  - `ExactlyOnceSink` trait for transactional sinks
  - `IdempotentSink` wrapper for non-transactional sinks with deduplication
  - `SinkCheckpoint` and `SinkCheckpointManager` for recovery
  - `DeduplicationStore` trait with `InMemoryDedup` and `BloomFilterDedup` implementations
  - `TransactionState` and `TransactionCoordinator` for 2PC support
  - `TwoPhaseCommitSink` trait for distributed transactions
  - `RecordIdExtractor` trait with `DefaultIdExtractor` and `ColumnIdExtractor`
  - 48 new unit tests, all passing
  - **Total tests**: 517 (425 core + 61 sql + 25 storage + 6 connectors)

- ✅ **F059: FIRST/LAST Value Aggregates** - IMPLEMENTATION COMPLETE (previous session)
  - `FirstValueAggregator` and `FirstValueAccumulator` for i64
  - `LastValueAggregator` and `LastValueAccumulator` for i64
  - `FirstValueF64Aggregator` and `FirstValueF64Accumulator` for f64
  - `LastValueF64Aggregator` and `LastValueF64Accumulator` for f64
  - Timestamp-ordered semantics (earliest/latest by event time)
  - Proper merge behavior for distributed aggregation
  - Checkpoint/restore support via rkyv serialization
  - 18 new unit tests including OHLC simulation
  - **Total tests**: 469 (377 core + 61 sql + 25 storage + 6 connectors)

- ✅ **F063: Changelog/Retraction (Z-Sets)** - IMPLEMENTATION COMPLETE (previous session)
  - New `changelog` module in `crates/laminar-core/src/operator/changelog.rs`
  - `ChangelogRef` - Zero-allocation changelog reference for Ring 0 (12 bytes)
  - `ChangelogBuffer` - Pre-allocated buffer for hot path (no allocation after warmup)
  - `RetractableAccumulator` trait - Extension for aggregators supporting retraction
  - `RetractableCountAccumulator` - O(1) retraction for count
  - `RetractableSumAccumulator` - O(1) retraction for sum
  - `RetractableAvgAccumulator` - O(1) retraction for average
  - `RetractableMinAccumulator` - Value-tracking min with retraction (O(n) worst case)
  - `RetractableMaxAccumulator` - Value-tracking max with retraction (O(n) worst case)
  - `LateDataRetractionGenerator` - Generates retractions for late data corrections
  - `CdcEnvelope<T>` - Debezium-compatible CDC format (insert/update/delete/read)
  - `CdcSource` - Source metadata for CDC envelope
  - Added `to_u8()`/`from_u8()` to `CdcOperation` for compact storage
  - 31 new unit tests, all passing
  - **Total tests**: 451 (359 core + 61 sql + 25 storage + 6 connectors)

### F063 Implementation Details

**New Module**: `crates/laminar-core/src/operator/changelog.rs`

**Ring 0 Zero-Allocation Types**:
```rust
// Compact changelog reference (12 bytes)
pub struct ChangelogRef {
    pub batch_offset: u32,
    pub row_index: u32,
    pub weight: i16,
    operation_raw: u8,
}

// Pre-allocated buffer for hot path
pub struct ChangelogBuffer {
    refs: Vec<ChangelogRef>,  // Pre-warmed
    len: usize,
    capacity: usize,
}
```

**Retractable Aggregators**:
```rust
pub trait RetractableAccumulator: Default + Clone + Send {
    type Input;
    type Output;

    fn add(&mut self, value: Self::Input);
    fn retract(&mut self, value: &Self::Input);  // Inverse of add
    fn merge(&mut self, other: &Self);
    fn result(&self) -> Self::Output;
    fn is_empty(&self) -> bool;
    fn supports_efficient_retraction(&self) -> bool;
    fn reset(&mut self);
}

// Implementations: RetractableCountAccumulator, RetractableSumAccumulator,
// RetractableAvgAccumulator, RetractableMinAccumulator, RetractableMaxAccumulator
```

**Late Data Retraction**:
```rust
pub struct LateDataRetractionGenerator {
    emitted_results: FxHashMap<WindowId, EmittedResult>,
    enabled: bool,
}

impl LateDataRetractionGenerator {
    // Returns (old_data, new_data) if retraction needed
    pub fn check_retraction(&mut self, window_id: &WindowId,
                            new_data: &[u8], timestamp: i64)
        -> Option<(Vec<u8>, Vec<u8>)>;
}
```

**CDC Envelope (Debezium-Compatible)**:
```rust
pub struct CdcEnvelope<T> {
    pub op: String,        // "c" (create), "u" (update), "d" (delete), "r" (read)
    pub ts_ms: i64,
    pub source: CdcSource,
    pub before: Option<T>, // For updates/deletes
    pub after: Option<T>,  // For inserts/updates
}
```

**Usage Example**:
```rust
use laminar_core::operator::changelog::{
    ChangelogBuffer, ChangelogRef, RetractableSumAccumulator,
    RetractableAccumulator, CdcEnvelope, CdcSource,
};

// Ring 0: Zero-allocation changelog tracking
let mut buffer = ChangelogBuffer::with_capacity(1024);
buffer.push(ChangelogRef::insert(0, 0));
buffer.push_retraction(0, 1, 2);  // UpdateBefore + UpdateAfter pair

// Ring 1: Retractable aggregation
let mut agg = RetractableSumAccumulator::default();
agg.add(10);
agg.add(20);
assert_eq!(agg.result(), 30);
agg.retract(&10);
assert_eq!(agg.result(), 20);

// CDC envelope for sinks
let source = CdcSource::new("laminardb", "default", "orders");
let envelope = CdcEnvelope::insert(json!({"id": 1}), source, 1000);
let json = envelope.to_json().unwrap();
```

### Previous Session: F011B: EMIT Clause Extension - IMPLEMENTATION COMPLETE
  - Extended `EmitStrategy` enum with `OnWindowClose`, `Changelog`, `Final`
  - Added helper methods: `emits_intermediate()`, `requires_changelog()`, `is_append_only_compatible()`, `generates_retractions()`, `suppresses_intermediate()`, `drops_late_data()`
  - Added `CdcOperation` enum: Insert, Delete, UpdateBefore, UpdateAfter with Z-set weights
  - Added `ChangelogRecord` type for CDC output
  - Added `Output::Changelog(ChangelogRecord)` variant
  - Updated `TumblingWindowOperator` for new strategies
  - Updated `SlidingWindowOperator` for new strategies
  - Updated SQL parser for `EMIT CHANGES` and `EMIT FINAL`
  - Updated reactor `StdoutSink` for changelog output
  - 15 new unit tests, all passing
  - **Total tests**: 420 (328 core + 61 sql + 25 storage + 6 connectors)

### F011B Implementation Details

**Core Types** (`crates/laminar-core/src/operator/window.rs`):
```rust
// Extended EmitStrategy enum
pub enum EmitStrategy {
    OnWatermark,       // Existing: emit when watermark passes
    Periodic(Duration), // Existing: emit at intervals
    OnUpdate,          // Existing: emit on every update
    OnWindowClose,     // F011B: only emit when window closes (append-only sinks)
    Changelog,         // F011B: emit CDC records with Z-set weights
    Final,             // F011B: suppress intermediate, drop late data
}

// New CDC operation type
pub enum CdcOperation {
    Insert,        // +1 weight
    Delete,        // -1 weight
    UpdateBefore,  // -1 weight (retraction)
    UpdateAfter,   // +1 weight (new value)
}

// New changelog record type
pub struct ChangelogRecord {
    pub operation: CdcOperation,
    pub weight: i32,           // Z-set weight (+1 or -1)
    pub emit_timestamp: i64,
    pub event: Event,
}
```

**SQL Syntax**:
```sql
-- OnWindowClose: for append-only sinks (Kafka, S3, Delta Lake)
CREATE CONTINUOUS QUERY orders_hourly
AS SELECT COUNT(*) FROM orders
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT ON WINDOW CLOSE;

-- Changelog: for CDC pipelines and cascading MVs
CREATE CONTINUOUS QUERY cdc_pipeline
AS SELECT * FROM orders
EMIT CHANGES;

-- Final: for BI reporting (no retractions)
CREATE CONTINUOUS QUERY bi_report
AS SELECT SUM(amount) FROM sales
EMIT FINAL;
```

**Usage Example**:
```rust
use laminar_core::operator::window::{
    EmitStrategy, TumblingWindowOperator, CountAggregator, ChangelogRecord, CdcOperation,
};

// Create operator with OnWindowClose for append-only sink
let mut operator = TumblingWindowOperator::new(assigner, aggregator, lateness);
operator.set_emit_strategy(EmitStrategy::OnWindowClose);

// Or use Changelog for CDC
operator.set_emit_strategy(EmitStrategy::Changelog);

// Check strategy properties
assert!(EmitStrategy::OnWindowClose.is_append_only_compatible());
assert!(EmitStrategy::Changelog.requires_changelog());
assert!(EmitStrategy::Final.drops_late_data());
```

### Previous Session Accomplishments
- ✅ **F068: NUMA-Aware Memory Allocation** - IMPLEMENTATION COMPLETE
  - NumaTopology detection via sysfs (/sys/devices/system/node/) or hwlocality
  - NumaAllocator with alloc_local, alloc_on_node, alloc_interleaved
  - NumaPlacement enum (Local, ProducerLocal, Interleaved, Any)
  - NumaBuffer and NumaVec for NUMA-aware containers
  - Raw libc mmap + mbind syscalls (no libnuma dependency)
  - Optional hwlocality feature for enhanced topology discovery
  - 64-byte cache-line alignment, MADV_HUGEPAGE hints
  - Platform fallback for Windows/macOS (single NUMA node)
  - 11 new unit tests, all passing
  - **Integration with CoreHandle**: `numa_aware` config, `numa_node` tracking
  - **Integration with TpcConfig**: `numa_aware()` builder method
- ✅ **F067: io_uring Advanced Optimization** - IMPLEMENTATION COMPLETE + INTEGRATION
  - SQPOLL mode for syscall elimination (kernel polling thread)
  - Registered buffer pool for zero-copy I/O
  - IOPOLL mode for NVMe storage operations
  - Per-core ring manager for thread-per-core integration
  - Linux-only with graceful fallback on other platforms
  - 13 new unit tests, all passing
  - Feature flag `io-uring` for opt-in on Linux
  - **Integration with WAL**: `IoUringWal` in laminar-storage for async durability
  - **Integration with CoreHandle**: Optional `io_uring_config` per-core thread
  - **Integration with Reactor**: `IoUringSink` for async output
- ✅ **F071: Zero-Allocation Enforcement** - IMPLEMENTATION COMPLETE + INTEGRATION
  - `HotPathDetectingAlloc` - Custom global allocator that panics on hot path allocation
  - `HotPathGuard` - RAII guard with nesting support for marking hot path sections
  - `hot_path!` macro - Convenience macro for function entry
  - `ObjectPool<T, N>` - Fixed-size pool for zero-allocation acquire/release
  - `RingBuffer<T, N>` - Fixed-capacity circular buffer
  - `ScratchBuffer` - Thread-local temporary storage with 64KB default
  - Feature flag `allocation-tracking` for opt-in detection
  - 33 new unit tests, all passing
  - **Integration**: Guards added to `Reactor::poll()` and `core_thread_main()`
- ✅ **Previous Session**: Thread-Per-Core Research specs (F067-F072)

### F068 Implementation Details

**New Module**: `crates/laminar-core/src/numa/`
```
numa/
├── mod.rs          # Public exports, integration tests
├── error.rs        # NumaError enum
├── topology.rs     # NumaTopology (sysfs + hwlocality detection)
└── allocator.rs    # NumaAllocator, NumaPlacement, NumaBuffer, NumaVec
```

**Usage**:
```rust
use laminar_core::numa::{NumaTopology, NumaAllocator, NumaPlacement};

// Detect topology
let topo = NumaTopology::detect();
println!("{}", topo.summary());  // "NUMA: 2 nodes, 32 CPUs\n  Node 0: 16 CPUs, 64 GB..."

// Create allocator
let alloc = NumaAllocator::new();

// Allocate on current core's NUMA node
let buf = alloc.alloc_local(4096, 64)?;

// Allocate on specific node
let buf = alloc.alloc_on_node(0, 4096, 64)?;

// Allocate interleaved across all nodes (for shared read-only data)
let buf = alloc.alloc_interleaved(4096, 64)?;

// Use NumaPlacement enum
let buf = alloc.alloc_with_placement(4096, 64, NumaPlacement::Local(0))?;
```

**Feature Flag** (optional, for enhanced topology detection):
```toml
[dependencies]
laminar-core = { version = "0.1", features = ["hwloc"] }
```

**Integration with CoreHandle**:
```rust
use laminar_core::tpc::{CoreConfig, CoreHandle, TpcConfig};

// Enable NUMA-aware allocation for per-core state
let config = CoreConfig {
    numa_aware: true,
    ..Default::default()
};
let handle = CoreHandle::spawn(config)?;
println!("Core is on NUMA node {}", handle.numa_node());

// Or via TpcConfig builder
let tpc = TpcConfig::builder()
    .num_cores(4)
    .numa_aware(true)
    .build();
```

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

**Hot Path Integration Points**:
- `Reactor::poll()` in `crates/laminar-core/src/reactor/mod.rs:198`
- `core_thread_main()` in `crates/laminar-core/src/tpc/core_handle.rs:427`

These cover all Ring 0 hot path code including operator processing and state access.

### F067 Implementation Details

**New Modules**:

`crates/laminar-core/src/io_uring/`
```
io_uring/
├── mod.rs          # Public exports, platform detection
├── config.rs       # IoUringConfig, RingMode (SqPoll, IoPoll, etc.)
├── error.rs        # IoUringError enum
├── ring.rs         # Ring creation (create_optimized_ring, create_iopoll_ring)
├── buffer_pool.rs  # RegisteredBufferPool (zero-copy I/O)
├── manager.rs      # CoreRingManager (per-core ring management)
└── sink.rs         # IoUringSink (async Reactor sink)
```

`crates/laminar-storage/src/io_uring_wal.rs` - `io_uring`-backed WAL

**Usage**:
```rust
use laminar_core::io_uring::{IoUringConfig, RingMode, CoreRingManager};

// Configure optimized ring with SQPOLL
let config = IoUringConfig::builder()
    .ring_entries(256)
    .enable_sqpoll(1000)  // 1s idle timeout
    .sqpoll_cpu(0)        // Pin to CPU 0
    .buffer_size(64 * 1024)
    .buffer_count(256)
    .build()?;

// Create per-core manager
let mut manager = CoreRingManager::new(0, &config)?;

// Zero-copy write using registered buffer
let (idx, buf) = manager.acquire_buffer()?;
buf[..5].copy_from_slice(b"hello");
let user_data = manager.submit_write(fd, idx, 0, 5)?;
manager.submit()?;

// Poll for completions
let completions = manager.poll_completions();
for c in completions {
    if c.is_success() {
        println!("Wrote {} bytes", c.bytes_transferred().unwrap());
    }
}
manager.release_buffer(idx);
```

**Feature Flag**:
```toml
[dependencies]
laminar-core = { version = "0.1", features = ["io-uring"] }
laminar-storage = { version = "0.1", features = ["io-uring"] }
```

**Integration Usage**:
```rust
// CoreHandle with io_uring
use laminar_core::tpc::{CoreConfig, CoreHandle};
use laminar_core::io_uring::{IoUringConfig, RingMode};

let config = CoreConfig {
    io_uring_config: Some(IoUringConfig::builder()
        .ring_entries(256)
        .mode(RingMode::SqPoll)
        .buffer_size(64 * 1024)
        .buffer_count(64)
        .build_unchecked()),
    ..Default::default()
};
let handle = CoreHandle::spawn(config)?;

// io_uring-backed WAL
use laminar_storage::IoUringWal;
use std::time::Duration;

let wal = IoUringWal::new("/tmp/data.wal", Duration::from_millis(100), None)?;

// io_uring-backed Reactor Sink
use laminar_core::io_uring::IoUringSink;

let sink = IoUringSink::new("/tmp/output.log", config)?;
reactor.set_sink(Box::new(sink));
```

**Key Components**:
- `IoUringConfig` - Builder for ring configuration (entries, mode, buffers)
- `RingMode` - Standard, SqPoll, IoPoll, SqPollIoPoll
- `RegisteredBufferPool` - Pre-registered buffers for zero-copy I/O
- `CoreRingManager` - Per-core ring with pending operation tracking
- `Completion` - Result type with latency tracking and error handling
- `IoUringSink` - Reactor sink for async file output
- `IoUringWal` - Write-ahead log with group commit support

### Previous Session Accomplishments

### Thread-Per-Core Research Analysis Summary

From `docs/research/laminardb-thread-per-core-2026-research.md`, identified critical gaps:

| Gap | Research Finding | Current (F013) | Fix |
|-----|------------------|----------------|-----|
| io_uring basic only | "2.05x improvement with SQPOLL" | ✅ Implemented | **F067** |
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
      ✅ Complete   │         ✅ Complete
                    ├──▶ F068 (NUMA) ──▶ Production Deployment
                    ├──▶ F070 (Task Budget) ──▶ Latency SLAs
                    └──▶ F071 (Zero-Alloc) ──▶ F072 (XDP) [P2]
                              ✅ Complete
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
Phase 2 features continue. F022 Incremental Checkpointing is now complete (15/29 features).

### Immediate Next Steps

1. **Continue Phase 2** - Production Hardening
   - F062: Per-Core WAL Segments (P1)
   - F021: Temporal Joins (P2)
   - F069: Three-Ring I/O Architecture (P1)
   - F070: Task Budget Enforcement (P1)

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
| F017: Session Windows | ✅ Complete | Gap-based sessions, per-key tracking, 23 tests |
| F018: Hopping Windows | ✅ Complete | Alias for sliding windows |
| F019: Stream-Stream Joins | ✅ Complete | Inner/Left/Right/Full, 14 tests |
| F020: Lookup Joins | ✅ Complete | Cached lookups with TTL, 16 tests |
| F011B: EMIT Clause Extension | ✅ Complete | OnWindowClose, Changelog, Final, 15 tests |
| F023: Exactly-Once Sinks | ✅ Complete | Transactional + idempotent sinks, 48 tests |
| F059: FIRST/LAST Aggregates | ✅ Complete | Essential for OHLC bars, 18 tests |
| F063: Changelog/Retraction | ✅ Complete | Z-set foundation, 31 tests |
| F067: io_uring Advanced | ✅ Complete | SQPOLL, IOPOLL, registered buffers, 13 tests |
| F068: NUMA-Aware Memory | ✅ Complete | NumaAllocator, NumaTopology, 11 tests |
| F071: Zero-Alloc Enforcement | ✅ Complete | HotPathGuard, ObjectPool, RingBuffer, 33 tests |
| F022: Incremental Checkpointing | ✅ Complete | RocksDB backend, SPSC changelog, recovery, 37 tests |

---

## Quick Reference

### Current Focus
- **Phase**: 2 Production Hardening
- **Active Feature**: F022 complete (16/29), ready for F062 (per-core WAL) or F021 (temporal joins)

### Key Files
```
crates/laminar-core/src/sink/
├── mod.rs           # Public exports, ExactlyOnceSink trait
├── error.rs         # SinkError enum
├── traits.rs        # ExactlyOnceSink, TransactionId, SinkCapabilities
├── checkpoint.rs    # SinkCheckpoint, SinkCheckpointManager, SinkOffset
├── dedup.rs         # DeduplicationStore, InMemoryDedup, BloomFilterDedup
├── transaction.rs   # TransactionState, TransactionCoordinator, TwoPhaseCommitSink
└── idempotent.rs    # IdempotentSink wrapper, RecordIdExtractor

crates/laminar-core/src/io_uring/
├── mod.rs           # Public exports, platform detection
├── config.rs        # IoUringConfig, RingMode (SqPoll, IoPoll)
├── error.rs         # IoUringError enum
├── ring.rs          # Ring creation (SQPOLL, IOPOLL modes)
├── buffer_pool.rs   # RegisteredBufferPool (zero-copy I/O)
└── manager.rs       # CoreRingManager (per-core ring management)

crates/laminar-core/src/alloc/
├── mod.rs           # Public exports, hot_path! macro
├── detector.rs      # HotPathDetectingAlloc, AllocationStats
├── guard.rs         # HotPathGuard (RAII, nesting support)
├── object_pool.rs   # ObjectPool<T, N> (fixed-size pool)
├── ring_buffer.rs   # RingBuffer<T, N> (circular buffer)
└── scratch.rs       # ScratchBuffer (thread-local temp storage)

crates/laminar-core/src/numa/
├── mod.rs           # Public exports, integration tests
├── error.rs         # NumaError enum
├── topology.rs      # NumaTopology (sysfs + hwlocality detection)
└── allocator.rs     # NumaAllocator, NumaPlacement, NumaBuffer, NumaVec

crates/laminar-core/src/tpc/
├── mod.rs           # TpcError, public exports
├── spsc.rs          # SpscQueue<T>, CachePadded<T>
├── router.rs        # KeyRouter, KeySpec
├── core_handle.rs   # CoreHandle, CoreConfig, CoreMessage
└── runtime.rs       # ThreadPerCoreRuntime, TpcConfig

crates/laminar-core/src/operator/
├── mod.rs             # Operator trait, Event, Output types
├── window.rs          # TumblingWindowOperator, WindowAssigner trait, CdcOperation
├── sliding_window.rs  # SlidingWindowOperator, SlidingWindowAssigner
├── session_window.rs  # F017: SessionWindowOperator, SessionState (gap-based sessions)
├── stream_join.rs     # StreamJoinOperator, JoinType, JoinSide
├── lookup_join.rs     # LookupJoinOperator, TableLoader trait
└── changelog.rs       # F063: ChangelogRef, ChangelogBuffer, RetractableAccumulator,
                       #       LateDataRetractionGenerator, CdcEnvelope, CdcSource

crates/laminar-connectors/src/lookup.rs  # TableLoader trait, InMemoryTableLoader

crates/laminar-storage/src/incremental/
├── mod.rs           # Public exports, module documentation
├── error.rs         # IncrementalCheckpointError enum
├── changelog.rs     # F022: StateChangelogEntry, StateChangelogBuffer (SPSC)
├── manager.rs       # IncrementalCheckpointManager, CheckpointConfig
└── recovery.rs      # RecoveryManager, RecoveryConfig, RecoveredState

Benchmarks: crates/laminar-core/benches/tpc_bench.rs, io_uring_bench.rs

Tests: 577 passing (448 core, 61 sql, 62 storage, 6 connectors)
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
