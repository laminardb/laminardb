# Session Context Archive

> Historical session details moved from CONTEXT.md for performance.
> For current state, see [CONTEXT.md](./CONTEXT.md)

---

## Implementation Details (Phase 2 Features)

### F063: Changelog/Retraction (Z-Sets)

**Module**: `crates/laminar-core/src/operator/changelog.rs`

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
```

**Late Data Retraction**:
```rust
pub struct LateDataRetractionGenerator {
    emitted_results: FxHashMap<WindowId, EmittedResult>,
    enabled: bool,
}

impl LateDataRetractionGenerator {
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
    pub before: Option<T>,
    pub after: Option<T>,
}
```

---

### F011B: EMIT Clause Extension

**Core Types** (`crates/laminar-core/src/operator/window.rs`):
```rust
pub enum EmitStrategy {
    OnWatermark,       // emit when watermark passes
    Periodic(Duration), // emit at intervals
    OnUpdate,          // emit on every update
    OnWindowClose,     // F011B: only emit when window closes
    Changelog,         // F011B: emit CDC records with Z-set weights
    Final,             // F011B: suppress intermediate, drop late data
}

pub enum CdcOperation {
    Insert,        // +1 weight
    Delete,        // -1 weight
    UpdateBefore,  // -1 weight (retraction)
    UpdateAfter,   // +1 weight (new value)
}

pub struct ChangelogRecord {
    pub operation: CdcOperation,
    pub weight: i32,
    pub emit_timestamp: i64,
    pub event: Event,
}
```

**SQL Syntax**:
```sql
-- OnWindowClose: for append-only sinks
CREATE CONTINUOUS QUERY orders_hourly
AS SELECT COUNT(*) FROM orders
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)
EMIT ON WINDOW CLOSE;

-- Changelog: for CDC pipelines
CREATE CONTINUOUS QUERY cdc_pipeline
AS SELECT * FROM orders
EMIT CHANGES;

-- Final: for BI reporting
CREATE CONTINUOUS QUERY bi_report
AS SELECT SUM(amount) FROM sales
EMIT FINAL;
```

---

### F068: NUMA-Aware Memory Allocation

**Module**: `crates/laminar-core/src/numa/`

**Usage**:
```rust
use laminar_core::numa::{NumaTopology, NumaAllocator, NumaPlacement};

let topo = NumaTopology::detect();
let alloc = NumaAllocator::new();

// Allocate on current core's NUMA node
let buf = alloc.alloc_local(4096, 64)?;

// Allocate interleaved across all nodes
let buf = alloc.alloc_interleaved(4096, 64)?;
```

**Integration with CoreHandle**:
```rust
let config = CoreConfig {
    numa_aware: true,
    ..Default::default()
};
let handle = CoreHandle::spawn(config)?;
```

---

### F071: Zero-Allocation Enforcement

**Module**: `crates/laminar-core/src/alloc/`

**Usage**:
```rust
use laminar_core::alloc::{HotPathGuard, ObjectPool, RingBuffer};
use laminar_core::hot_path;

fn process_event(event: &Event) {
    let _guard = HotPathGuard::enter("process_event");
    // or: hot_path!("process_event");

    let mut pool: ObjectPool<Buffer, 16> = ObjectPool::new();
    let buf = pool.acquire().unwrap();
    pool.release(buf);
}
```

**Hot Path Integration Points**:
- `Reactor::poll()` in `crates/laminar-core/src/reactor/mod.rs:198`
- `core_thread_main()` in `crates/laminar-core/src/tpc/core_handle.rs:427`

---

### F067: io_uring Advanced Optimization

**Module**: `crates/laminar-core/src/io_uring/`

**Usage**:
```rust
use laminar_core::io_uring::{IoUringConfig, RingMode, CoreRingManager};

let config = IoUringConfig::builder()
    .ring_entries(256)
    .enable_sqpoll(1000)
    .sqpoll_cpu(0)
    .buffer_size(64 * 1024)
    .buffer_count(256)
    .build()?;

let mut manager = CoreRingManager::new(0, &config)?;

let (idx, buf) = manager.acquire_buffer()?;
buf[..5].copy_from_slice(b"hello");
let user_data = manager.submit_write(fd, idx, 0, 5)?;
manager.submit()?;

let completions = manager.poll_completions();
manager.release_buffer(idx);
```

**Key Components**:
- `IoUringConfig` - Builder for ring configuration
- `RingMode` - Standard, SqPoll, IoPoll, SqPollIoPoll
- `RegisteredBufferPool` - Pre-registered buffers for zero-copy I/O
- `CoreRingManager` - Per-core ring with pending operation tracking
- `IoUringSink` - Reactor sink for async file output
- `IoUringWal` - Write-ahead log with group commit support

---

### F013/F014: Thread-Per-Core & SPSC

**Module Structure**:
```
crates/laminar-core/src/tpc/
├── mod.rs           # Public exports, TpcError enum
├── spsc.rs          # Lock-free SPSC queue with CachePadded<T>
├── router.rs        # KeyRouter for event partitioning
├── core_handle.rs   # CoreHandle per-core reactor wrapper
├── backpressure.rs  # Credit-based flow control
└── runtime.rs       # ThreadPerCoreRuntime multi-core orchestration
```

**Key Components**:
- `SpscQueue<T>` - Lock-free queue (~4.8ns per operation)
- `CachePadded<T>` - 64-byte aligned wrapper
- `KeyRouter` - Routes events by key hash (FxHash)
- `CoreHandle` - Per-core reactor thread with CPU affinity
- `CreditGate` / `BackpressureConfig` - Flink-style flow control
- `ThreadPerCoreRuntime` - Multi-core orchestration

---

### F016: Sliding Windows

**Module**: `crates/laminar-core/src/operator/sliding_window.rs`

```rust
use laminar_core::operator::sliding_window::{
    SlidingWindowAssigner, SlidingWindowOperator,
};

// 1-hour window with 15-minute slide (4 windows per event)
let assigner = SlidingWindowAssigner::new(
    Duration::from_secs(3600),
    Duration::from_secs(900),
);
let operator = SlidingWindowOperator::new(
    assigner,
    CountAggregator::new(),
    Duration::from_secs(60),
);
```

---

### F019: Stream-Stream Joins

**Module**: `crates/laminar-core/src/operator/stream_join.rs`

```rust
use laminar_core::operator::stream_join::{
    StreamJoinOperator, JoinType, JoinSide,
};

let mut operator = StreamJoinOperator::new(
    "order_id".to_string(),
    "order_id".to_string(),
    Duration::from_secs(3600),
    JoinType::Inner,
);

let outputs = operator.process_side(&order_event, JoinSide::Left, &mut ctx);
```

---

## Research Analysis Summaries

### Thread-Per-Core Research (2026)

From `docs/research/laminardb-thread-per-core-2026-research.md`:

| Gap | Research Finding | Fix |
|-----|------------------|-----|
| io_uring basic only | "2.05x improvement with SQPOLL" | **F067** |
| No NUMA awareness | "2-3x latency on remote access" | **F068** |
| Single I/O ring | "3 rings: latency/main/poll" | **F069** |
| No task budgeting | "Ring 0: 500ns budget" | **F070** |
| No allocation detection | "Zero-alloc verification" | **F071** |
| No XDP steering | "26M packets/sec/core" | **F072** |

### Emit Patterns Research (2026)

From `docs/research/emit-patterns-research-2026.md`:

| Gap | Research Finding | Fix |
|-----|------------------|-----|
| EMIT ON WINDOW CLOSE | Essential for append-only sinks | **F011B** |
| Changelog/Retraction | DBSP Z-sets fundamental | **F063** |
| EMIT CHANGES | CDC pipelines need delta | **F011B** |
| EMIT FINAL | BI reporting needs exact | **F011B** |

---

## Code Snippets

### TPC Public API
```rust
use laminar_core::tpc::{TpcConfig, ThreadPerCoreRuntime, KeySpec};

let config = TpcConfig::builder()
    .num_cores(4)
    .key_columns(vec!["user_id".to_string()])
    .cpu_pinning(true)
    .build()?;

let runtime = ThreadPerCoreRuntime::new(config)?;
runtime.submit(event)?;
let outputs = runtime.poll();
```

### Backpressure Configuration
```rust
use laminar_core::tpc::{BackpressureConfig, OverflowStrategy, CoreConfig};

let bp_config = BackpressureConfig::builder()
    .exclusive_credits(4)
    .floating_credits(8)
    .high_watermark(0.8)
    .low_watermark(0.5)
    .overflow_strategy(OverflowStrategy::Block)
    .build();

handle.is_backpressured();
handle.available_credits();
handle.credit_metrics();
```

---

## Session History

### Session - 2026-01-24 (F013 Thread-Per-Core)

**Accomplished**:
- Implemented F013 Thread-Per-Core Architecture
- Created tpc module with spsc.rs, router.rs, core_handle.rs, runtime.rs
- Lock-free SPSC queue with CachePadded wrapper
- KeyRouter for FxHash-based event partitioning
- CoreHandle with CPU affinity (Linux/Windows)
- ThreadPerCoreRuntime with builder pattern
- Added tpc_bench.rs with comprehensive benchmarks
- All 267 tests passing, clippy clean

---

### Session - 2026-01-24 (WAL Hardening)

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

---

### Session - 2026-01-24 (Phase 1 Audit)

**Accomplished**:
- Comprehensive audit of all 12 Phase 1 features
- Identified 16 gaps against 2025-2026 best practices
- Prioritized into P0/P1/P2 categories
- Created PHASE1_AUDIT.md with full audit report

**Key Findings**:
- WAL durability issues (fsync, no checksums, no torn write detection)
- Watermark not persisted (recovery loses progress)
- No recovery integration test

---

### Session - 2026-01-24 (Late Data Handling - F012)

- Implemented F012 - Late Data Handling
- Added `LateDataConfig` struct with drop/side-output options
- Added `LateDataMetrics` for tracking late events
- Phase 1 features complete (100%)

---

### Session - 2026-01-24 (EMIT Clause - F011)

- Implemented F011 - EMIT Clause with 3 strategies
- OnWatermark, Periodic, OnUpdate emit modes
- Periodic timer system with special key encoding

---

### Session - 2026-01-24 (Watermarks - F010)

- Implemented F010 - Watermarks with 5 generation strategies
- WatermarkTracker for multi-source alignment
- Idle source detection and MeteredGenerator wrapper

---

### Session - 2026-01-23 (Checkpointing - F008)

- Fixed Ring 0 hot path violations
- Implemented reactor features (CPU affinity, sinks, graceful shutdown)
- Implemented F008 - Basic Checkpointing

---

### Session - 2026-01-22 (rkyv migration)

- Migrated serialization from bincode to rkyv
- Updated all types for zero-copy deserialization

---

## Recent Decisions

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-24 | Custom SPSC over crossbeam | Precise cache layout control |
| 2026-01-24 | `#[repr(C, align(64))]` for CachePadded | Hardware cache line alignment |
| 2026-01-24 | FxHash for key routing | Faster than std HashMap for small keys |
| 2026-01-24 | Factory pattern for per-core operators | No shared state between cores |
