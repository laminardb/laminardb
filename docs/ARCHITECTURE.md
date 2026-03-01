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
+------------------------------------------------------------------+
|                        RING 0: HOT PATH                          |
|  Constraints: Zero allocations, no locks, < 1us latency         |
|                                                                  |
|  +---------+  +---------+  +---------+  +---------+             |
|  | Reactor |->|Operators|->|  State  |->|  Emit   |             |
|  |  Loop   |  | (map,   |  |  Store  |  | (output)|             |
|  |         |  | filter, |  | (lookup)|  |         |             |
|  |         |  | window) |  |         |  |         |             |
|  +---------+  +---------+  +---------+  +---------+             |
|       |                                                          |
|       | SPSC Queues (lock-free)                                  |
|       v                                                          |
+------------------------------------------------------------------+
|                     RING 1: BACKGROUND                           |
|  Constraints: Can allocate, async I/O, bounded latency impact   |
|                                                                  |
|  +---------+  +---------+  +---------+  +---------+             |
|  |Checkpoint  |   WAL   |  |Compaction  |  Timer  |             |
|  | Manager |  |  Writer |  |  Thread |  |  Wheel  |             |
|  +---------+  +---------+  +---------+  +---------+             |
|       |                                                          |
|       | Channels (bounded)                                       |
|       v                                                          |
+------------------------------------------------------------------+
|                     RING 2: CONTROL PLANE                        |
|  Constraints: No latency requirements, full flexibility         |
|                                                                  |
|  +---------+  +---------+  +---------+  +---------+             |
|  |  Admin  |  | Metrics |  |  Auth   |  |  Config |             |
|  |   API   |  | Export  |  | Engine  |  | Manager |             |
|  +---------+  +---------+  +---------+  +---------+             |
+------------------------------------------------------------------+
```

### Ring 0: Hot Path

The event processing path where latency matters most. All code in Ring 0 runs on a CPU-pinned reactor thread.

**Components:**
- **Reactor Loop** -- Single-threaded event loop that pulls batches from sources, runs them through operators, and emits results. CPU-pinned via the thread-per-core runtime (`laminar-core/src/tpc/`).
- **Operators** -- Stateless transforms (map, filter, project) and stateful operators (tumbling/sliding/hopping/session windows, stream-stream joins, ASOF joins, temporal joins, lookup joins, lag/lead, ranking).
- **State Store** -- AHashMap/FxHashMap-based in-memory store wrapped in `ChangelogAwareStore` for zero-allocation changelog capture (~2-5ns per mutation). Supports mmap-backed persistence. Ring 0 SQL operator routing (`core_window_state.rs`) directs tumbling/hopping/session window aggregates through optimized `CoreWindowAssigner` state instead of the generic DataFusion path.
- **Emit** -- Pushes output RecordBatches to downstream streams and sinks via SPSC queues.

**Constraints:**
- No heap allocations (use bump/arena allocators; enforced by `allocation-tracking` feature)
- No locks (SPSC queues for inter-ring communication)
- No system calls on the fast path (io_uring for async I/O on Linux)
- Predictable branching (likely/unlikely hints)
- Task budget enforcement prevents any single operator from exceeding its time slice

**Optional JIT compilation** (Cranelift): DataFusion logical plans can be compiled into native machine code for Ring 0 execution. The `AdaptiveQueryRunner` runs queries interpreted first, compiles in the background, and hot-swaps to compiled execution when ready. See `laminar-core/src/compiler/`.

**Streaming physical optimizer** (`StreamingPhysicalValidator`): Catches invalid physical plans (e.g., SortExec on unbounded streams) before execution. Configurable via `StreamingValidatorMode` (Reject, Warn, Off).

**Dynamic watermark filter pushdown** (`WatermarkDynamicFilter`): Pushes `ts >= watermark` predicates down to `StreamingScanExec` so late rows are dropped before expression evaluation, using shared `Arc<AtomicI64>` watermarks.

**Cooperative scheduling**: DataFusion's cooperative scheduling integration marks `StreamingScanExec` as `NonCooperative` so the engine wraps it with budget-aware `CooperativeExec` automatically.

**Structured error codes**: Every error carries a stable `LDB-NNNN` code (8 code ranges from general through internal). Ring 0 uses a zero-alloc `HotPathError` enum (2 bytes, `Copy`).

### Ring 1: Background

Handles durability and I/O without blocking the hot path. Runs on Tokio async runtime.

**Components:**
- **Checkpoint Manager** -- Incremental checkpointing with directory-based snapshots. Unified `CheckpointCoordinator` orchestrates two-phase commit across all operators and sinks (`laminar-db/src/checkpoint_coordinator.rs`).
- **WAL Writer** -- Per-core write-ahead log segments with CRC32C checksums, torn write detection, and fdatasync durability (`laminar-storage/src/per_core_wal/`).
- **Changelog Drainer** -- Consumes changelog entries from Ring 0's SPSC queues and persists them (`laminar-storage/src/changelog_drainer.rs`).
- **Recovery Manager** -- Loads the latest checkpoint manifest and restores all operator state, connector offsets, and watermarks on startup (`laminar-db/src/recovery_manager.rs`).
- **Connectors** -- External source/sink connectors (Kafka, CDC, Delta Lake, WebSocket) run as Tokio tasks in Ring 1.

**Communication with Ring 0:**
- SPSC queues for changelog entries and checkpoint barriers
- Never blocks Ring 0 operations

### Ring 2: Control Plane

Administrative and observability functions with no latency requirements.

**Components (planned -- crate stubs exist, no implementation yet):**
- **Admin API** -- REST API via Axum with Swagger UI (`laminar-admin/`)
- **Metrics Export** -- Prometheus metrics and OpenTelemetry tracing (`laminar-observe/`)
- **Auth Engine** -- JWT authentication, RBAC/ABAC authorization (`laminar-auth/`)
- **Config Manager** -- Dynamic configuration, connector registry

Note: While the Ring 2 crate stubs (`laminar-auth`, `laminar-admin`, `laminar-observe`) exist in the workspace, their source files contain only module-level doc comments with no implementation. These are planned for Phase 4 (Enterprise Security) and Phase 5 (Admin & Observability).

## Data Flow

An event's journey through LaminarDB:

```
                         Ring 0 (< 1us)
                    +--------------------+
  Source --> Ingest --> Window/Join/Agg --> Emit --> Subscribers
    |                       |                           |
    |                       v                           |
    |               +--------------+                    |
    |               | State Store  |                    |
    |               |  (AHashMap)  |                    |
    |               +------+-------+                    |
    |                      |                            |
    |            Ring 1    | SPSC changelog              |
    |            +---------v--------+                   |
    |            |  WAL + Delta     |                   |
    |            |  Checkpoints     |                   |
    |            +------------------+                   |
    |                                                   |
    +------------- Offset Tracking --------------------+
```

1. **Source ingestion**: Data arrives as Arrow RecordBatches via `SourceHandle::push()` or from external connectors (Kafka, PostgreSQL CDC, MySQL CDC, WebSocket).
2. **Watermark tracking**: Each source maintains an `EventTimeExtractor` + `BoundedOutOfOrdernessGenerator` for watermark computation. Late rows are filtered. Watermarks can be per-partition, per-key, or aligned across sources.
3. **Operator processing**: The reactor loop runs batches through the operator DAG (windows, joins, aggregations, filters). State mutations are captured by `ChangelogAwareStore`.
4. **Emit**: Results are published to named streams. Subscribers receive RecordBatches via typed `TypedSubscription<T>` or callback subscriptions.
5. **Durability**: Changelog entries flow via SPSC queues to Ring 1, which writes to WAL and periodically takes incremental checkpoints.
6. **Sink output**: External sinks (Kafka, PostgreSQL, Delta Lake, WebSocket) receive batches with exactly-once semantics via two-phase commit.

## Crate Map

```
laminar-core          Ring 0: reactor, operators, state stores, time/watermarks,
                      streaming channels, DAG pipeline, subscriptions, JIT compiler,
                      lookup tables, secondary indexes, cross-partition aggregation,
                      checkpoint barriers, NUMA allocation, io_uring, task budgets
                      |
laminar-sql           SQL parser (streaming extensions), query planner,
                      DataFusion integration, operator config translators,
                      custom UDFs (tumble, hop, session, slide, first_value, last_value),
                      streaming physical optimizer, watermark filter pushdown,
                      cooperative scheduling, PROCTIME() UDF
                      |
laminar-storage       Ring 1: WAL, incremental checkpointing, per-core WAL segments,
                      checkpoint manifest, checkpoint store, changelog drainer,
                      io_uring WAL (Linux only)
                      |
laminar-connectors    Kafka source/sink, PostgreSQL CDC/sink, MySQL CDC,
                      WebSocket source/sink, Delta Lake sink/source, Iceberg sink,
                      connector SDK, schema framework (inference, evolution, DLQ),
                      format decoders (JSON, CSV, Avro, Parquet),
                      lookup tables, reference tables, cloud storage infrastructure
                      |
laminar-db            Unified facade: LaminarDB struct, LaminarDbBuilder,
                      SQL execution, checkpoint coordination, recovery manager,
                      connector manager, pipeline observability, deployment profiles,
                      FFI API (C bindings, Arrow C Data Interface),
                      Ring 0 SQL operator routing (core_window_state),
                      EOWC incremental window accumulators (eowc_state)
                      |
laminar-auth          JWT authentication, RBAC, ABAC (Ring 2 -- stubs only)
laminar-admin         REST API with Axum, Swagger UI (Ring 2 -- stubs only)
laminar-observe       Prometheus metrics, OpenTelemetry tracing (Ring 2 -- stubs only)
laminar-derive        Proc macros: #[derive(Record, FromRecordBatch, FromRow, ConnectorConfig)]
laminar-server        Standalone binary: CLI args, logging init (skeleton with TODOs)
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

Key public methods:
- `open()` / `builder().build()` -- construct the database
- `execute(sql)` -- execute DDL or DML (CREATE SOURCE, CREATE STREAM, CREATE SINK, etc.)
- `source_untyped(name)` / `source::<T>(name)` -- get handles for data ingestion
- `subscribe::<T>(name)` -- subscribe to a stream's output
- `start()` / `shutdown()` -- lifecycle management
- `checkpoint()` -- trigger a manual checkpoint
- `metrics()` / `pipeline_state()` -- observability
- `connector_registry()` -- access the connector registry for custom connector registration

### LaminarDbBuilder

Fluent builder for constructing `LaminarDB` with custom configuration:

```rust
let db = LaminarDB::builder()
    .config_var("KAFKA_BROKERS", "localhost:9092")
    .buffer_size(131_072)
    .storage_dir("./data")
    .checkpoint(checkpoint_config)
    .profile(Profile::Durable)
    .register_udf(my_scalar_udf)
    .register_udaf(my_aggregate_udf)
    .register_connector(|registry| {
        registry.register_source("my-source", info, factory);
    })
    .build()
    .await?;
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
- `InMemoryStateStore` -- AHashMap-based, used for Ring 0
- `ChangelogAwareStore<S>` -- Wraps any StateStore, captures mutations at ~2-5ns overhead
- Mmap-backed store for persistent state

### Streaming Channels

Lock-free communication between components:

- **RingBuffer** -- Fixed-capacity, cache-line-padded ring buffer
- **SPSC Channel** -- Single-producer single-consumer, zero-allocation on hot path
- **MPSC Channel** -- Auto-upgrading multi-producer variant
- **Broadcast Channel** -- One-to-many fan-out

### DAG Pipeline

The DAG executor connects operators into a directed acyclic graph:

- **Core DAG Topology** -- Nodes (operators) and edges (channels) with type-safe wiring
- **Multicast & Routing** -- Fan-out and key-based routing between DAG nodes
- **DAG Executor** -- Processes batches through the DAG with backpressure
- **DAG Checkpointing** -- Barrier-based consistent snapshots across the DAG
- **Connector Bridge** -- Integrates external source/sink connectors as DAG nodes

### Connector SDK

Custom connectors implement the `SourceConnector` and `SinkConnector` traits:

```rust
#[async_trait]
pub trait SourceConnector: Send {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;
    async fn poll_batch(&mut self, max_records: usize) -> Result<Option<SourceBatch>, ConnectorError>;
    fn schema(&self) -> SchemaRef;
    fn checkpoint(&self) -> SourceCheckpoint;
    async fn restore(&mut self, checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError>;
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

#[async_trait]
pub trait SinkConnector: Send {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;
    fn schema(&self) -> SchemaRef;
    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;
    async fn pre_commit(&mut self, epoch: u64) -> Result<(), ConnectorError>;
    async fn commit_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;
    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;
    async fn close(&mut self) -> Result<(), ConnectorError>;
}
```

The SDK provides retry policies, rate limiting, circuit breakers, and a test harness.

### Lookup Tables

Lookup tables provide enrichment join support with multiple caching strategies:

- **Full cache (Ring 0)** -- foyer in-memory cache with S3-FIFO eviction
- **Hybrid cache (Ring 1)** -- foyer HybridCache with disk-backed overflow
- **Partial cache with Xor filter** -- probabilistic membership test to avoid cache misses
- **CDC-to-cache adapter** -- keep lookup tables fresh from CDC streams
- **Lookup sources** -- PostgresLookupSource, ParquetLookupSource
- **redb secondary indexes** -- B-tree indexes for non-primary-key lookups

### Deployment Profiles

Pre-configured deployment tiers:

| Profile | Description |
|---------|-------------|
| `InMemory` | Default. All state in memory, no durability. |
| `Durable` | Object-store checkpoints for recovery. Requires `durable` feature. |
| `Delta` | Full distributed mode with gossip, Raft, gRPC. Requires `delta` feature. |

## Streaming SQL

LaminarDB extends standard SQL (via sqlparser-rs) with streaming constructs:

| Extension | Syntax | Example |
|-----------|--------|---------|
| Sources | `CREATE SOURCE name (columns...)` | Data ingestion endpoints |
| Streams | `CREATE STREAM name AS SELECT...` | Continuous queries |
| Sinks | `CREATE SINK name FROM stream` | Output endpoints |
| Tumbling windows | `tumble(ts_col, INTERVAL)` | Fixed-size non-overlapping |
| Sliding windows | `slide(ts_col, size, slide)` | Overlapping windows |
| Hopping windows | `hop(ts_col, size, hop)` | Periodic windows |
| Session windows | `session(ts_col, gap)` | Activity-based |
| Watermarks | `WATERMARK FOR col AS expr` | Event time tracking |
| Late data | `ALLOWED_LATENESS INTERVAL` | Grace periods |
| EMIT clause | `EMIT ON WINDOW CLOSE` | Output control |
| ASOF JOIN | `ASOF JOIN ... ON ... AND ts >= ts` | Point-in-time lookups |
| Lookup tables | `CREATE LOOKUP TABLE ... FROM POSTGRES(...)` | Reference data |
| LAG/LEAD | `LAG(col, offset) OVER (...)` | Sliding analytics |
| Ranking | `ROW_NUMBER() OVER (...)` | Ranking functions |
| Window frames | `ROWS BETWEEN ... AND ...` | Custom frame bounds |
| Config vars | `${VAR}` in SQL strings | Variable substitution |

Queries are planned by `StreamingPlanner` and executed either via DataFusion (interpreted) or via the JIT compiler (compiled to native code for Ring 0).

## Performance Characteristics

| Operation | Target | Measured | Technique |
|-----------|--------|----------|-----------|
| State lookup | < 500ns | 10-105ns (AHash get_ref: 10-16ns) | AHashMap, cache-aligned keys |
| Event processing | < 1us | 0.55-1.16us | Zero allocation, inlined operators |
| Throughput/core | 500K/s | 1.1-1.46M/s | Batch processing, Arrow columnar |
| Checkpoint | < 10s recovery | 1.39ms | Incremental snapshots, async I/O |
| Window trigger | < 10us | -- | Hierarchical timer wheel |
| Changelog overhead | ~2-5ns/mutation | -- | `ChangelogAwareStore` wrapper |

See [BENCHMARKS.md](BENCHMARKS.md) for full benchmark baselines with hardware details.

## Thread-Per-Core Model

Each CPU core runs an independent reactor with its own:
- Event loop (CPU-pinned)
- State store partition
- WAL segment
- SPSC queues to Ring 1

Key-based routing ensures all events for a given key are processed on the same core, avoiding cross-core synchronization. NUMA-aware memory allocation keeps data local to the processing core's memory node.

```
+---+---+-----+  +---+---+-----+  +---+---+-----+
|  Core 0     |  |  Core 1     |  |  Core 2     |
| +---------+ |  | +---------+ |  | +---------+ |
| | Reactor | |  | | Reactor | |  | | Reactor | |
| |  Loop   | |  | |  Loop   | |  | |  Loop   | |
| +----+----+ |  | +----+----+ |  | +----+----+ |
| +----+----+ |  | +----+----+ |  | +----+----+ |
| |  State  | |  | |  State  | |  | |  State  | |
| |  Store  | |  | |  Store  | |  | |  Store  | |
| +----+----+ |  | +----+----+ |  | +----+----+ |
| +----+----+ |  | +----+----+ |  | +----+----+ |
| |   WAL   | |  | |   WAL   | |  | |   WAL   | |
| +---------+ |  | +---------+ |  | +---------+ |
+---+---+-----+  +---+---+-----+  +---+---+-----+
      |                |                |
      +---- Key-based routing ----------+
```

## Exactly-Once Semantics

LaminarDB provides exactly-once processing through:

1. **Source offsets** -- Tracked per-source, persisted in checkpoint manifests
2. **Changelog capture** -- `ChangelogAwareStore` records every state mutation
3. **WAL** -- Per-core WAL segments with CRC32C checksums and torn write detection
4. **Incremental checkpoints** -- Directory-based snapshots of operator state
5. **Two-phase commit** -- Coordinated across all sinks via `CheckpointCoordinator`
6. **Recovery** -- `RecoveryManager` restores from the latest checkpoint manifest, replays WAL entries, and resumes from committed offsets

## Delta Architecture (Distributed Mode)

When enabled with the `delta` feature, LaminarDB extends to multi-node operation:

- **Discovery** -- Static configuration, gossip-based (chitchat), or Kafka group discovery
- **Coordination** -- Raft-based metadata consensus via openraft
- **Partition Ownership** -- Epoch-fenced partition guards with consistent assignment
- **Distributed Checkpoints** -- Cross-node barrier coordination
- **Cross-Node Aggregation** -- Gossip partial aggregates and gRPC fan-out
- **Inter-Node RPC** -- gRPC service definitions for remote lookups, barrier forwarding, aggregate fan-out

The Delta architecture maintains Ring 0's sub-500ns hot path guarantees while adding distributed coordination in Ring 1 and Ring 2.
