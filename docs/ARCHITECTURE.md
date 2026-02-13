# Architecture

## Overview

LaminarDB is an embedded streaming database designed for sub-microsecond latency. It combines the deployment simplicity of SQLite with the streaming capabilities of Apache Flink.

## Design Principles

1. **Embedded First** -- Single binary, no external dependencies
2. **Sub-Microsecond Latency** -- Zero allocations on hot path
3. **SQL Native** -- Full SQL support via Apache DataFusion
4. **Thread-Per-Core** -- Linear scaling with CPU cores
5. **Exactly-Once** -- End-to-end exactly-once semantics
6. **Arrow-Native** -- Apache Arrow RecordBatch at every boundary

## Three-Ring Architecture

LaminarDB separates concerns into three concentric rings, each with different latency budgets and constraints:

```
┌─────────────────────────────────────────────────────────────────┐
│                        RING 0: HOT PATH                         │
│  Constraints: Zero allocations, no locks, < 1us latency         │
│                                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │ Reactor │─>│Operators│─>│  State  │─>│  Emit   │            │
│  │  Loop   │  │ (map,   │  │  Store  │  │ (output)│            │
│  │         │  │ filter, │  │ (lookup)│  │         │            │
│  │         │  │ window) │  │         │  │         │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
│       |                                                         │
│       | SPSC Queues (lock-free)                                 │
│       v                                                         │
├─────────────────────────────────────────────────────────────────┤
│                     RING 1: BACKGROUND                          │
│  Constraints: Can allocate, async I/O, bounded latency impact   │
│                                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │Checkpoint│ │   WAL   │  │Compaction│ │  Timer  │            │
│  │ Manager │  │  Writer │  │  Thread │  │  Wheel  │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
│       |                                                         │
│       | Channels (bounded)                                      │
│       v                                                         │
├─────────────────────────────────────────────────────────────────┤
│                     RING 2: CONTROL PLANE                       │
│  Constraints: No latency requirements, full flexibility         │
│                                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │
│  │  Admin  │  │ Metrics │  │  Auth   │  │  Config │            │
│  │   API   │  │ Export  │  │ Engine  │  │ Manager │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

### Ring 0: Hot Path

The event processing path where latency matters most. All code in Ring 0 runs on a CPU-pinned reactor thread.

**Components:**
- **Reactor Loop** -- Single-threaded event loop that pulls batches from sources, runs them through operators, and emits results. CPU-pinned via the thread-per-core runtime (`laminar-core/src/tpc/`).
- **Operators** -- Stateless transforms (map, filter, project) and stateful operators (tumbling/sliding/hopping/session windows, stream-stream joins, ASOF joins, temporal joins, lookup joins, lag/lead, ranking).
- **State Store** -- FxHashMap-based in-memory store wrapped in `ChangelogAwareStore` for zero-allocation changelog capture (~2-5ns per mutation). Supports mmap-backed and RocksDB-backed persistence.
- **Emit** -- Pushes output RecordBatches to downstream streams and sinks via SPSC queues.

**Constraints:**
- No heap allocations (use bump/arena allocators; enforced by `allocation-tracking` feature)
- No locks (SPSC queues for inter-ring communication)
- No system calls on the fast path (io_uring for async I/O on Linux)
- Predictable branching (likely/unlikely hints)
- Task budget enforcement prevents any single operator from exceeding its time slice

**Optional JIT compilation** (Cranelift): DataFusion logical plans can be compiled into native machine code for Ring 0 execution. The `AdaptiveQueryRunner` runs queries interpreted first, compiles in the background, and hot-swaps to compiled execution when ready. See `laminar-core/src/compiler/`.

### Ring 1: Background

Handles durability and I/O without blocking the hot path. Runs on Tokio async runtime.

**Components:**
- **Checkpoint Manager** -- Incremental checkpointing with RocksDB backend. Unified `CheckpointCoordinator` orchestrates two-phase commit across all operators and sinks (`laminar-db/src/checkpoint_coordinator.rs`).
- **WAL Writer** -- Per-core write-ahead log segments with CRC32C checksums, torn write detection, and fdatasync durability (`laminar-storage/src/per_core_wal/`).
- **Changelog Drainer** -- Consumes changelog entries from Ring 0's SPSC queues and persists them (`laminar-storage/src/changelog_drainer.rs`).
- **Recovery Manager** -- Loads the latest checkpoint manifest and restores all operator state, connector offsets, and watermarks on startup (`laminar-db/src/recovery_manager.rs`).

**Communication with Ring 0:**
- SPSC queues for changelog entries and checkpoint barriers
- Never blocks Ring 0 operations

### Ring 2: Control Plane

Administrative and observability functions with no latency requirements.

**Components:**
- **Admin API** -- REST API via Axum with Swagger UI (utoipa) for pipeline management (`laminar-admin/`)
- **Metrics Export** -- Prometheus metrics and OpenTelemetry tracing (`laminar-observe/`)
- **Auth Engine** -- JWT authentication, RBAC/ABAC authorization (`laminar-auth/`)
- **Config Manager** -- Dynamic configuration, connector registry

## Data Flow

An event's journey through LaminarDB:

```
                         Ring 0 (< 1us)
                    ┌────────────────────┐
  Source ──> Ingest ──> Window/Join/Agg ──> Emit ──> Subscribers
    |                       |                           |
    |                       v                           |
    |               ┌──────────────┐                    |
    |               │ State Store  │                    |
    |               │  (FxHashMap) │                    |
    |               └──────┬───────┘                    |
    |                      |                            |
    |            Ring 1    | SPSC changelog              |
    |            ┌─────────v────────┐                   |
    |            │  WAL + RocksDB   │                   |
    |            │  Checkpoints     │                   |
    |            └──────────────────┘                   |
    |                                                   |
    └───────────── Offset Tracking ─────────────────────┘
```

1. **Source ingestion**: Data arrives as Arrow RecordBatches via `SourceHandle::push()` or from external connectors (Kafka, PostgreSQL CDC, MySQL CDC).
2. **Watermark tracking**: Each source maintains an `EventTimeExtractor` + `BoundedOutOfOrdernessGenerator` for watermark computation. Late rows are filtered.
3. **Operator processing**: The reactor loop runs batches through the operator DAG (windows, joins, aggregations, filters). State mutations are captured by `ChangelogAwareStore`.
4. **Emit**: Results are published to named streams. Subscribers receive RecordBatches via `Subscription<ArrowRecord>`.
5. **Durability**: Changelog entries flow via SPSC queues to Ring 1, which writes to WAL and periodically takes incremental checkpoints.
6. **Sink output**: External sinks (Kafka, PostgreSQL, Delta Lake) receive batches with exactly-once semantics via two-phase commit.

## Crate Map

```
laminar-core          Ring 0: reactor, operators, state stores, time/watermarks,
                      streaming channels, DAG pipeline, subscriptions, JIT compiler
                      |
laminar-sql           SQL parser (streaming extensions), query planner,
                      DataFusion integration, operator config translators
                      |
laminar-storage       Ring 1: WAL, incremental checkpointing, RocksDB backend,
                      per-core WAL segments, changelog drainer
                      |
laminar-connectors    Kafka source/sink, PostgreSQL CDC/sink, MySQL CDC,
                      Delta Lake/Iceberg, connector SDK, serde formats
                      |
laminar-db            Unified facade: LaminarDB struct, SQL execution,
                      checkpoint coordination, recovery, FFI API
                      |
laminar-auth          JWT authentication, RBAC, ABAC (Ring 2)
laminar-admin         REST API with Axum, Swagger UI (Ring 2)
laminar-observe       Prometheus metrics, OpenTelemetry tracing (Ring 2)
laminar-derive        Proc macros: #[derive(Record, FromRecordBatch)]
laminar-server        Standalone binary: config, CLI, entrypoint
```

## Key Abstractions

### LaminarDB (Database Facade)

The primary user-facing type. Manages the lifecycle of sources, streams, sinks, and the streaming pipeline.

```rust
let db = LaminarDB::open()?;

// DDL
db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts BIGINT)").await?;
db.execute("CREATE STREAM avg_price AS SELECT symbol, AVG(price) ...").await?;

// Data ingestion
let source = db.source_untyped("trades")?;
source.push(record_batch);

// Subscriptions
let sub = db.subscribe::<MyType>("avg_price")?;

// Lifecycle
db.start().await?;
db.shutdown().await?;
```

### State Store

All operator state goes through the `StateStore` trait, optionally wrapped in `ChangelogAwareStore` for zero-allocation changelog capture:

```rust
pub trait StateStore: Send {
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn prefix_scan(&self, prefix: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])>;
}
```

Implementations:
- `InMemoryStateStore` -- FxHashMap-based, used for Ring 0
- `ChangelogAwareStore<S>` -- Wraps any StateStore, captures mutations at ~2-5ns overhead
- RocksDB-backed store (via `--features rocksdb`) for persistent state

### Streaming Channels

Lock-free communication between components:

- **RingBuffer** -- Fixed-capacity, cache-line-padded ring buffer
- **SPSC Channel** -- Single-producer single-consumer, zero-allocation on hot path
- **MPSC Channel** -- Auto-upgrading multi-producer variant
- **Broadcast Channel** -- One-to-many fan-out

### Connector SDK

Custom connectors implement the `SourceConnector` and `SinkConnector` traits:

```rust
#[async_trait]
pub trait SourceConnector: Send + Sync {
    async fn start(&mut self) -> Result<()>;
    async fn poll(&mut self) -> Result<Option<ConnectorBatch>>;
    async fn commit(&mut self, offsets: &ConnectorOffsets) -> Result<()>;
}

#[async_trait]
pub trait SinkConnector: Send + Sync {
    async fn write(&mut self, batch: RecordBatch) -> Result<()>;
    async fn pre_commit(&mut self, epoch: u64) -> Result<()>;
    async fn commit_epoch(&mut self, epoch: u64) -> Result<()>;
}
```

The SDK provides retry policies, rate limiting, circuit breakers, and a test harness.

## Streaming SQL

LaminarDB extends standard SQL (via sqlparser-rs) with streaming constructs:

| Extension | Syntax | Example |
|-----------|--------|---------|
| Sources | `CREATE SOURCE name (columns...)` | Data ingestion endpoints |
| Streams | `CREATE STREAM name AS SELECT...` | Continuous queries |
| Tumbling windows | `tumble(ts_col, INTERVAL)` | Fixed-size non-overlapping |
| Sliding windows | `slide(ts_col, size, slide)` | Overlapping windows |
| Hopping windows | `hop(ts_col, size, hop)` | Periodic windows |
| Session windows | `session(ts_col, gap)` | Activity-based |
| Watermarks | `WATERMARK FOR col AS expr` | Event time tracking |
| Late data | `ALLOWED_LATENESS INTERVAL` | Grace periods |
| EMIT clause | `EMIT ON WINDOW CLOSE` | Output control |
| ASOF JOIN | `ASOF JOIN ... MATCH_CONDITION(...)` | Point-in-time lookups |
| LAG/LEAD | `LAG(col, offset) OVER (...)` | Sliding analytics |

Queries are planned by `StreamingPlanner` and executed either via DataFusion (interpreted) or via the JIT compiler (compiled to native code for Ring 0).

## Performance Characteristics

| Operation | Target | Technique |
|-----------|--------|-----------|
| State lookup | < 500ns | FxHashMap, cache-aligned keys |
| Event processing | < 1us | Zero allocation, inlined operators |
| Throughput/core | 500K/s | Batch processing, Arrow columnar |
| Checkpoint | < 10s recovery | Incremental RocksDB, async I/O |
| Window trigger | < 10us | Hierarchical timer wheel |
| Changelog overhead | ~2-5ns/mutation | `ChangelogAwareStore` wrapper |

## Thread-Per-Core Model

Each CPU core runs an independent reactor with its own:
- Event loop (CPU-pinned)
- State store partition
- WAL segment
- SPSC queues to Ring 1

Key-based routing ensures all events for a given key are processed on the same core, avoiding cross-core synchronization. NUMA-aware memory allocation keeps data local to the processing core's memory node.

```
┌───────────┐  ┌───────────┐  ┌───────────┐
│  Core 0   │  │  Core 1   │  │  Core 2   │
│ ┌───────┐ │  │ ┌───────┐ │  │ ┌───────┐ │
│ │Reactor│ │  │ │Reactor│ │  │ │Reactor│ │
│ │ Loop  │ │  │ │ Loop  │ │  │ │ Loop  │ │
│ └───┬───┘ │  │ └───┬───┘ │  │ └───┬───┘ │
│ ┌───┴───┐ │  │ ┌───┴───┐ │  │ ┌───┴───┐ │
│ │ State │ │  │ │ State │ │  │ │ State │ │
│ │ Store │ │  │ │ Store │ │  │ │ Store │ │
│ └───┬───┘ │  │ └───┬───┘ │  │ └───┬───┘ │
│ ┌───┴───┐ │  │ ┌───┴───┐ │  │ ┌───┴───┐ │
│ │  WAL  │ │  │ │  WAL  │ │  │ │  WAL  │ │
│ └───────┘ │  │ └───────┘ │  │ └───────┘ │
└───────────┘  └───────────┘  └───────────┘
      |              |              |
      └──── Key-based routing ──────┘
```

## Exactly-Once Semantics

LaminarDB provides exactly-once processing through:

1. **Source offsets** -- Tracked per-source, persisted in checkpoint manifests
2. **Changelog capture** -- `ChangelogAwareStore` records every state mutation
3. **WAL** -- Per-core WAL segments with CRC32C checksums and torn write detection
4. **Incremental checkpoints** -- RocksDB-backed snapshots of operator state
5. **Two-phase commit** -- Coordinated across all sinks via `CheckpointCoordinator`
6. **Recovery** -- `RecoveryManager` restores from the latest checkpoint manifest, replays WAL entries, and resumes from committed offsets
