# Architecture

## Overview

LaminarDB is an embedded streaming database designed for sub-microsecond latency. You link it into your application like SQLite, but instead of querying stored data, you're querying data as it arrives.

## Design Principles

1. **Embedded First** -- Single binary, no external dependencies
2. **Sub-Microsecond Latency** -- Minimal allocations on hot path
3. **SQL Native** -- Full SQL support via Apache DataFusion
4. **Exactly-Once** -- End-to-end exactly-once semantics
5. **Arrow-Native** -- Apache Arrow RecordBatch at every boundary

## Architecture Overview

The system has three layers with different latency budgets:

```
+------------------------------------------------------------------+
|                     SOURCE CONNECTORS                             |
|  Kafka, Postgres CDC, MySQL CDC, WebSocket, Files                |
|  (tokio tasks, push RecordBatches via mpsc channels)             |
+------------------------------------------------------------------+
|                  STREAMING COORDINATOR                             |
|  Single tokio task: SQL cycles, compiled projections,             |
|  cached logical plans, checkpoint barriers                        |
|  +-----------+ +-----------+ +-----------+ +-----------+         |
|  | Compiled  | | Operators | |   State   | |   Sink    |         |
|  | Projection| | (window,  | | (FxHashMap| |  Writers  |         |
|  | / Cached  | |  join,    | |  per-group| |           |         |
|  |   Plans   | |  filter)  | |  accum.)  | |           |         |
|  +-----------+ +-----------+ +-----------+ +-----------+         |
+------------------------------------------------------------------+
|                     BACKGROUND I/O                                |
|  Tokio async runtime, bounded latency impact                      |
|  +-----------+ +-----------+ +-----------+ +-----------+         |
|  | Checkpoint| |    WAL    | | Changelog | |   Timer   |         |
|  |  Manager  | |   Writer  | |  Drainer  | |   Wheel   |         |
|  +-----------+ +-----------+ +-----------+ +-----------+         |
+------------------------------------------------------------------+
|                      CONTROL PLANE                                |
|  +-----------+ +-----------+ +-----------+                       |
|  |   Admin   | |  Metrics  | |   Config  |                      |
|  |    API    | |   Export   | |  Manager  |                      |
|  +-----------+ +-----------+ +-----------+                       |
+------------------------------------------------------------------+
```

### Streaming Coordinator (Hot Path)

A single `StreamingCoordinator` tokio task receives batches from source connectors via `tokio::sync::mpsc`, executes SQL cycles via `PipelineCallback`, and delegates checkpoint barriers.

**Components:**
- **StreamExecutor** -- Drives DataFusion SQL execution per cycle. Three optimization tiers: `CompiledProjection` (single-source non-aggregate queries compiled to `PhysicalExpr`), `IncrementalAggState` (incremental GROUP BY with per-group accumulators), and `CoreWindowState` (tumbling/hopping/session windows via optimized `CoreWindowAssigner`). Queries that don't match these tiers fall back to full DataFusion execution.
- **Operators** -- Stateless transforms (map, filter, project) and stateful operators (tumbling/sliding/hopping/session windows, stream-stream joins, ASOF joins, temporal joins, lookup joins, lag/lead, ranking).
- **State** -- Operators hold state in internal `FxHashMap`s (per-group accumulators, window buffers, join buffers). Checkpointed via JSON serialization of `StreamExecutorCheckpoint`.
- **Emit** -- Pushes output RecordBatches to downstream streams and sinks via mpsc channels.

**Compiled query execution**: Non-aggregate single-source queries are compiled to `PhysicalExpr` projections on first execution, eliminating per-cycle SQL overhead. Complex queries cache their optimized logical plans.

**Streaming physical optimizer** (`StreamingPhysicalValidator`): Catches invalid physical plans (e.g., SortExec on unbounded streams) before execution. Configurable via `StreamingValidatorMode` (Reject, Warn, Off).

**Dynamic watermark filter pushdown** (`WatermarkDynamicFilter`): Pushes `ts >= watermark` predicates down to `StreamingScanExec` so late rows are dropped before expression evaluation, using shared `Arc<AtomicI64>` watermarks.

**Cooperative scheduling**: DataFusion's cooperative scheduling integration marks `StreamingScanExec` as `NonCooperative` so the engine wraps it with budget-aware `CooperativeExec` automatically.

**Structured error codes**: Every error carries a stable `LDB-NNNN` code (8 code ranges from general through internal). Hot-path errors use a zero-alloc `HotPathError` enum (2 bytes, `Copy`).

### Background I/O

Durability and I/O, runs on Tokio async runtime.

**Components:**
- **Checkpoint Manager** -- Incremental checkpointing with directory-based snapshots. `CheckpointCoordinator` orchestrates two-phase commit across exactly-once sinks (`laminar-db/src/checkpoint_coordinator.rs`).
- **WAL Writer** -- Per-core write-ahead log segments with CRC32C checksums, torn write detection, and fdatasync durability (`laminar-storage/src/per_core_wal/`).
- **Changelog Drainer** -- Consumes changelog entries and persists them (`laminar-storage/src/changelog_drainer.rs`).
- **Recovery Manager** -- Loads the latest checkpoint manifest and restores all operator state, connector offsets, and watermarks on startup (`laminar-db/src/recovery_manager.rs`).
- **Connectors** -- External source/sink connectors (Kafka, CDC, Delta Lake, WebSocket) run as Tokio tasks.

### Control Plane

Admin and observability. No latency requirements.

**Components:**
- **Admin API** -- REST endpoints currently live in `laminar-server/src/http.rs`
- **Metrics Export** -- Prometheus metrics and OpenTelemetry tracing (Phase 4/5 future work)
- **Auth Engine** -- JWT authentication, RBAC/ABAC authorization (Phase 4/5 future work)
- **Config Manager** -- Dynamic configuration, connector registry

## Data Flow

How an event moves through the system:

```
                    Streaming Coordinator (< 1us per event for compiled queries)
                    +--------------------+
  Source --> mpsc --> Window/Join/Agg --> Emit --> Subscribers
    |                       |                           |
    |                       v                           |
    |               +--------------+                    |
    |               | State (per-  |                    |
    |               | group accum.)|                    |
    |               +------+-------+                    |
    |                      |                            |
    |          Background  | checkpoint                 |
    |            +---------v--------+                   |
    |            |  WAL + Directory |                   |
    |            |  Checkpoints     |                   |
    |            +------------------+                   |
    |                                                   |
    +------------- Offset Tracking --------------------+
```

1. **Source ingestion**: Data arrives as Arrow RecordBatches via `SourceHandle::push()` or from external connectors (Kafka, PostgreSQL CDC, MySQL CDC, WebSocket).
2. **Watermark tracking**: Each source maintains an `EventTimeExtractor` + `BoundedOutOfOrdernessGenerator` for watermark computation. Late rows are filtered. Watermarks can be per-partition, per-key, or aligned across sources.
3. **Operator processing**: The coordinator runs batches through SQL execution cycles (windows, joins, aggregations, filters). State is held in per-group accumulators and window buffers.
4. **Emit**: Results are published to named streams. Subscribers receive RecordBatches via typed `TypedSubscription<T>` or callback subscriptions.
5. **Durability**: Changelog entries flow to background I/O, which writes to WAL and periodically takes directory-based checkpoints.
6. **Sink output**: External sinks (Kafka, PostgreSQL, Delta Lake, WebSocket) receive batches with exactly-once semantics via two-phase commit.

## Crate Map

```
laminar-core          Core: reactor, operators, state stores, time/watermarks,
                      streaming channels, DAG pipeline, subscriptions,
                      lookup tables, secondary indexes, cross-partition aggregation,
                      checkpoint barriers, task budgets
                      |
laminar-sql           SQL parser (streaming extensions), query planner,
                      DataFusion integration, operator config translators,
                      custom UDFs (tumble, hop, session, slide, first_value, last_value),
                      streaming physical optimizer, watermark filter pushdown,
                      cooperative scheduling, PROCTIME() UDF
                      |
laminar-storage       Background I/O: WAL, incremental checkpointing, per-core WAL
                      segments, checkpoint manifest, checkpoint store,
                      changelog drainer
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
                      SQL operator routing (core_window_state),
                      EOWC incremental window accumulators (eowc_state)
                      |
laminar-derive        Proc macros: #[derive(Record, FromRecordBatch, FromRow, ConnectorConfig)]
laminar-server        Standalone binary: CLI args, logging init (skeleton with TODOs)
```

## Key Abstractions

### LaminarDB (Database Facade)

The main entry point. Owns sources, streams, sinks, and the pipeline lifecycle.

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

The `StateStore` trait provides key-value storage for DAG operators:

```rust
pub trait StateStore: Send {
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn prefix_scan(&self, prefix: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])>;
}
```

Implementations:
- `InMemoryStore` -- BTreeMap-based, used by DAG operators
- `AHashMapStore` -- AHashMap-based, for fast key lookups
- `MmapStateStore` -- Memory-mapped file backing for persistent state

The production SQL execution path (`StreamExecutor`) holds state in internal FxHashMaps (per-group accumulators, window buffers) and checkpoints via JSON serialization. DAG operators that use `StateStore` are: `stream_join`, `session_window`, `sliding_window`, `window`.

### Streaming Channels

Lock-free channels between components:

- **RingBuffer** -- Fixed-capacity, cache-line-padded ring buffer
- **SPSC Channel** -- Single-producer single-consumer, zero-allocation on hot path
- **MPSC Channel** -- Auto-upgrading multi-producer variant
- **Broadcast Channel** -- One-to-many fan-out

### DAG Pipeline

Operators wired into a DAG:

- **Core DAG Topology** -- Nodes (operators) and edges (channels) with type-safe wiring
- **Multicast & Routing** -- Fan-out and key-based routing between DAG nodes
- **DAG Executor** -- Processes batches through the DAG with backpressure
- **DAG Checkpointing** -- Barrier-based consistent snapshots across the DAG
- **Connector Bridge** -- Integrates external source/sink connectors as DAG nodes

### Connector SDK

Custom connectors implement `SourceConnector` and `SinkConnector`:

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

The SDK adds retry policies, rate limiting, circuit breakers, and a test harness.

### Lookup Tables

Enrichment joins via `CREATE LOOKUP TABLE` DDL with hash-probe physical execution:

- **Hash-indexed snapshot** -- lookup data pre-indexed at query planning time via Arrow `RowConverter`
- **Predicate pushdown** -- `PredicateSplitterRule` splits WHERE predicates; pushdown predicates filter the snapshot before index build
- **CDC refresh** -- `CdcTableSource` adapter wraps any `SourceConnector` (Postgres CDC, MySQL CDC) as a `ReferenceTableSource` for snapshot + incremental updates
- **Partial cache with Xor filter** -- probabilistic membership test to avoid full scans
- **Lookup sources** -- `PostgresLookupSource`, `ParquetLookupSource` for direct queries

### Deployment Profiles

Pre-configured deployment tiers:

| Profile | Description |
|---------|-------------|
| `InMemory` | Default. All state in memory, no durability. |
| `Durable` | Object-store checkpoints for recovery. Requires `durable` feature. |
| `Delta` | Full distributed mode with gossip, Raft, gRPC. Requires `delta` feature. |

## Streaming SQL

SQL parsing goes through sqlparser-rs with these streaming extensions:

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

Queries are planned by `StreamingPlanner` and executed via DataFusion, with compiled projections and cached logical plans eliminating per-cycle planning overhead for hot-path queries.

## Performance Characteristics

| Operation | Target | Measured | Technique |
|-----------|--------|----------|-----------|
| State lookup | < 500ns | 10-105ns (AHash get_ref: 10-16ns) | AHashMap, cache-aligned keys |
| Event processing | < 1us | 0.55-1.16us | Zero allocation, inlined operators |
| Throughput/core | 500K/s | 1.1-1.46M/s | Batch processing, Arrow columnar |
| Checkpoint | < 10s recovery | 1.39ms | Full snapshots, per-core WAL |
| Window trigger | < 10us | not yet measured | Hierarchical timer wheel |

See [BENCHMARKS.md](BENCHMARKS.md) for full benchmark baselines with hardware details.

## Execution Model

The thread-per-core model described in earlier documentation was removed in PR #204. The current execution model is a single `StreamingCoordinator` tokio task that:

1. Receives batches from source connectors via `tokio::sync::mpsc` channels
2. Executes SQL cycles via `StreamExecutor` (compiled projections, incremental aggregations, DataFusion fallback)
3. Manages checkpoint barriers for exactly-once semantics
4. Routes results to sink connectors

## Exactly-Once Semantics

Exactly-once processing works through:

1. **Source offsets** -- Tracked per-source, persisted in checkpoint manifests
2. **Barrier-based snapshots** -- `StreamingCoordinator` injects checkpoint barriers at sources; all sources align on the barrier before operator state is captured. Barriers flow between sources and the coordinator (not through the operator graph) in the production path. The DAG path (`dag/checkpoint.rs`) has true operator-level barrier alignment but is not the default execution path yet
3. **WAL** -- Per-core WAL segments with CRC32C checksums and torn write detection
4. **Incremental checkpoints** -- Directory-based snapshots of operator state (JSON-serialized `StreamExecutorCheckpoint`)
5. **Two-phase commit** -- Coordinated across exactly-once sinks via `CheckpointCoordinator` (at-most-once sinks receive no pre_commit/commit guarantees)
6. **Recovery** -- `RecoveryManager` restores from the latest checkpoint manifest, replays WAL entries, and resumes from committed offsets

## Delta Architecture (Distributed Mode)

With the `delta` feature enabled, multi-node operation:

- **Discovery** -- Static configuration, gossip-based (chitchat), or Kafka group discovery. Discovery via chitchat gossip is implemented.
- **Coordination** -- Raft-based metadata consensus via openraft
- **Partition Ownership** -- Epoch-fenced partition guards with consistent assignment
- **Distributed Checkpoints** -- Cross-node barrier coordination (planned; not yet implemented in checkpoint_coordinator)
- **Cross-Node Aggregation** -- Gossip partial aggregates and gRPC fan-out
- **Inter-Node RPC** -- gRPC service definitions for remote lookups, barrier forwarding, aggregate fan-out

**Status**: Discovery and coordination are implemented but not yet production-hardened. Production hardening is planned for Phase 6c.
