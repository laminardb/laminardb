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

The system has a coordinator layer (SQL execution, compiled projections), a background I/O layer (checkpoints, connector I/O), and a control plane (admin API, metrics).

```text
+------------------------------------------------------------------+
|                     SOURCE CONNECTORS                             |
|  Kafka, Postgres CDC, MySQL CDC, MongoDB CDC, WebSocket,          |
|  OpenTelemetry OTLP/gRPC, Files (AutoLoader), Delta Lake,         |
|  Iceberg                                                          |
|  (tokio tasks on the main runtime, push RecordBatches via         |
|   tokio::sync::mpsc to the compute thread)                        |
+------------------------------------------------------------------+
|                  STREAMING COORDINATOR                             |
|  Single tokio task on dedicated `laminar-compute` thread:          |
|  SQL cycles, compiled projections, cached logical plans,          |
|  checkpoint barrier injection and alignment                       |
|  +-----------+ +-----------+ +-----------+ +-----------+         |
|  | Compiled  | | Operators | |   State   | |   Sink    |         |
|  | Projection| | (window,  | | (FxHashMap| |  Writers  |         |
|  | / Cached  | |  join,    | |  per-group| |           |         |
|  |   Plans   | |  filter)  | |  accum.)  | |           |         |
|  +-----------+ +-----------+ +-----------+ +-----------+         |
+------------------------------------------------------------------+
|                     BACKGROUND I/O                                |
|  Tokio async runtime, bounded latency impact                      |
|  +--------------+ +--------------+                                |
|  |  Checkpoint  | |   Sink I/O   |                                |
|  | Coordinator  | |  (external   |                                |
|  |  (2PC, store)| |  writers)    |                                |
|  +--------------+ +--------------+                                |
+------------------------------------------------------------------+
|                      CONTROL PLANE                                |
|  +-----------+ +-----------+ +-----------+                       |
|  |   Admin   | |  Metrics  | |   Config  |                      |
|  |    API    | |   Export  | |  Manager  |                      |
|  +-----------+ +-----------+ +-----------+                       |
+------------------------------------------------------------------+
```

### Streaming Coordinator (Hot Path)

A single `StreamingCoordinator` tokio task runs on a dedicated single-threaded
runtime (the `laminar-compute` thread, spawned with
`tokio::runtime::Builder::new_current_thread()`), isolating CPU-bound event
processing from I/O tasks on the main work-stealing runtime. Source tokio
tasks deliver batches to the coordinator over `tokio::sync::mpsc` channels,
and the coordinator executes SQL cycles via `PipelineCallback::execute_cycle()`
and injects/aligns checkpoint barriers (Chandy-Lamport protocol).

**Components:**
- **StreamExecutor** -- Drives DataFusion SQL execution per cycle. Optimization tiers: `CompiledProjection` (single-source non-aggregate queries compiled to `PhysicalExpr`), `IncrementalAggState` (incremental GROUP BY with per-group accumulators), and `CoreWindowState` (tumbling/hopping/session windows via optimized `CoreWindowAssigner`). Queries that don't match these tiers fall back to full DataFusion execution.
- **Operators** -- Stateless transforms (map, filter, project) and stateful operators (tumbling/sliding/hopping/session windows, stream-stream joins, ASOF joins, temporal joins, lookup joins, lag/lead, ranking).
- **State** -- Operators hold state in internal `FxHashMap`s (per-group accumulators, window buffers, join buffers). Checkpointed via JSON serialization into the `CheckpointManifest`.
- **Emit** -- Pushes output RecordBatches to downstream streams and sinks via tokio mpsc channels.

**Compiled query execution**: Non-aggregate single-source queries are compiled to `PhysicalExpr` projections on first execution, eliminating per-cycle SQL overhead. Complex queries cache their optimized logical plans.

**Streaming physical optimizer** (`StreamingPhysicalValidator`): Catches invalid physical plans (e.g., SortExec on unbounded streams) before execution. Configurable via `StreamingValidatorMode` (Reject, Warn, Off).

**Dynamic watermark filter pushdown** (`WatermarkDynamicFilter`): Pushes `ts >= watermark` predicates down to `StreamingScanExec` so late rows are dropped before expression evaluation, using shared `Arc<AtomicI64>` watermarks.

**Cooperative scheduling**: DataFusion's cooperative scheduling integration marks `StreamingScanExec` as `NonCooperative` so the engine wraps it with budget-aware `CooperativeExec` automatically.

**Structured error codes**: Every error carries a stable `LDB-NNNN` code (8 code ranges from general through internal). Hot-path errors use a zero-alloc `HotPathError` enum (2 bytes, `Copy`).

### Background I/O

Durability and I/O, runs on the main tokio async runtime (not the compute thread).

**Components:**
- **Checkpoint Coordinator** -- Orchestrates periodic full-state snapshots with manifest-based persistence via two-phase commit across exactly-once sinks (`laminar-db/src/checkpoint_coordinator.rs`). Manifests are written via filesystem or object store (`laminar-storage/src/checkpoint_store.rs`).
- **Recovery Manager** -- Loads the latest checkpoint manifest and restores operator state, connector offsets, and watermarks on startup (`laminar-db/src/recovery_manager.rs`).
- **Connectors** -- External source/sink connectors (Kafka, CDC, Delta Lake, Iceberg, WebSocket, OTEL, Files) run as tokio tasks on the main runtime.

### Control Plane

Admin and observability. No latency requirements.

**Components:**
- **Admin API** -- REST endpoints currently live in `laminar-server/src/http.rs`
- **Metrics Export** -- Prometheus metrics and OpenTelemetry tracing
- **Config Manager** -- Dynamic configuration, connector registry

## Data Flow

How an event moves through the system:

```text
                    Streaming Coordinator (≈0.55–1.16µs per event for compiled queries)
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
    |            |  Directory       |                   |
    |            |  Checkpoints     |                   |
    |            +------------------+                   |
    |                                                   |
    +------------- Offset Tracking --------------------+
```

1. **Source ingestion**: Data arrives as Arrow RecordBatches via `SourceHandle::push()` or from external connectors (Kafka, PostgreSQL CDC, MySQL CDC, WebSocket).
2. **Watermark tracking**: Each source maintains an `EventTimeExtractor` + `BoundedOutOfOrdernessGenerator` for watermark computation. Late rows are filtered. Watermarks can be per-partition, per-key, or aligned across sources.
3. **Operator processing**: The coordinator runs batches through SQL execution cycles (windows, joins, aggregations, filters). State is held in per-group accumulators and window buffers.
4. **Emit**: Results are published to named streams. Subscribers receive RecordBatches via typed `TypedSubscription<T>` or callback subscriptions.
5. **Durability**: Operator state is periodically snapshotted to directory-based checkpoints (JSON manifest + optional binary sidecar), persisted atomically via temp-file + rename.
6. **Sink output**: External sinks (Kafka, PostgreSQL, Delta Lake, WebSocket) receive batches with exactly-once semantics via two-phase commit.

## Crate Map

```text
laminar-core          Core: operators, window assigners, time/watermarks,
                      streaming channels (crossfire), subscriptions,
                      lookup tables, checkpoint barrier protocol, error codes
                      |
laminar-sql           SQL parser (streaming extensions), query planner,
                      DataFusion integration, operator config translators,
                      custom UDFs (tumble, hop, session, slide, first_value, last_value),
                      streaming physical optimizer, watermark filter pushdown,
                      cooperative scheduling, PROCTIME() UDF,
                      temporal probe join translator
                      |
laminar-storage       Checkpoint persistence: manifest, checkpoint store
                      (filesystem + object store), object store builder
                      |
laminar-connectors    Kafka source/sink, PostgreSQL CDC/sink, MySQL CDC,
                      MongoDB CDC source/sink, WebSocket source/sink,
                      OpenTelemetry OTLP/gRPC source, file source/sink,
                      Delta Lake source/sink, Iceberg source/sink,
                      schema framework (inference, evolution),
                      format decoders (JSON, CSV, Avro, Parquet),
                      lookup tables, reference tables, cloud storage infrastructure
                      |
laminar-db            Unified facade: LaminarDB struct, LaminarDbBuilder,
                      StreamingCoordinator, checkpoint coordinator, recovery manager,
                      connector manager, pipeline observability, deployment profiles,
                      FFI API (C bindings, Arrow C Data Interface),
                      SQL operator routing (core_window_state),
                      EOWC incremental window accumulators (eowc_state)
                      |
laminar-derive        Proc macros: #[derive(Record, FromRecordBatch, FromRow, ConnectorConfig)]
laminar-server        Standalone binary: TOML config, Axum HTTP REST + WebSocket,
                      Prometheus metrics, hot reload, checkpoint validation CLI
```

## Key Abstractions

### LaminarDB (Database Facade)

The main entry point. Owns sources, streams, sinks, and the pipeline lifecycle.

```rust
let db = LaminarDB::open()?;

// DDL
db.execute("CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP)").await?;
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

### Operator State

The production SQL execution path (`StreamExecutor`) holds state in internal FxHashMaps (per-group accumulators, window buffers) and checkpoints via JSON serialization. Each stateful operator (`SqlQuery`, `EowcQuery`, `CoreWindowState`, `IncrementalAggState`) implements its own `checkpoint()`/`restore()` methods.

### Streaming Channels

Source and Sink objects in the public streaming API (`laminar_core::streaming`) are backed by `crossfire::mpsc::Array<T>` channels (bounded, blocking sender + async receiver). Clone the `Source<T>` for multi-producer use. Internally, the `StreamingCoordinator` uses `tokio::sync::mpsc` for source-task → coordinator communication across runtimes, and subscribers receive updates via the subscription registry in `laminar_core::streaming::subscription`.

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

Pre-configured deployment tiers (`laminar_db::Profile`). Each tier includes all capabilities of the tiers below it: `BareMetal ⊂ Embedded ⊂ Durable ⊂ Delta`.

| Profile | Description |
|---------|-------------|
| `BareMetal` | Default. In-memory only, no persistence. Fastest startup. |
| `Embedded` | Local filesystem checkpoint persistence for single-node embedded use. |
| `Durable` | Object-store checkpoints (S3/GCS/Azure) for recovery. |
| `Delta` | Distributed scaffolding (gossip, Raft, gRPC stubs). Not production-ready. Requires the `delta` feature. |

The builder can auto-detect the appropriate profile from the configured checkpoint URL and discovery settings.

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
| Late data | `ALLOW LATENESS INTERVAL` / `LATE DATA TO <sink>` | Grace periods, side outputs |
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
| Event processing | < 1us | 0.55-1.16us | Zero per-event allocation, compiled projections |
| Throughput/core | 500K/s | 1.1-1.46M/s | Batch processing, Arrow columnar |
| Checkpoint | < 10s recovery | 1.39ms | Full snapshots, manifest-based recovery |
| Window trigger | < 10us | not yet measured | Watermark-driven emission |

See [BENCHMARKS.md](BENCHMARKS.md) for historical benchmark baselines with hardware details.

## Execution Model

The thread-per-core model described in earlier documentation was removed in PR #204. The current execution model is a single `StreamingCoordinator` tokio task that:

1. Receives batches from source connectors via `tokio::sync::mpsc` channels
2. Executes SQL cycles via `StreamExecutor` (compiled projections, incremental aggregations, DataFusion fallback)
3. Manages checkpoint barriers for exactly-once semantics
4. Routes results to sink connectors

### Coordinator and Executor

**Execution:** SQL queries are executed through `StreamExecutor::execute_cycle()`, processing `RecordBatch` micro-batches with per-group `FxHashMap` state. The `OperatorGraph` manages the operator topology.

**Coordinator → Executor relationship:** `StreamingCoordinator` is the
single tokio task that owns the event loop. It calls `PipelineCallback::execute_cycle()`,
which delegates to `StreamExecutor::execute_cycle()`. The coordinator drives the
executor through a callback interface — there is a single execution loop, not
multiple competing ones.

### Deployment Model

LaminarDB is a **single-node** embedded database. All operator state is keyed by
operator index (position in the operator graph). Multi-node partitioning
(VNode-scoped state, distributed barriers, partition migration) is deferred to
a future phase.

## Exactly-Once Semantics

Exactly-once processing works through:

1. **Source offsets** -- Tracked per-source, persisted in checkpoint manifests
2. **Barrier-based snapshots** -- `StreamingCoordinator` injects checkpoint barriers at sources; all sources align on the barrier before operator state is captured
3. **Checkpoints** -- Periodic full-state snapshots: JSON-serialized operator state, source offsets, sink epochs, watermarks. Persisted atomically via temp-file + rename
4. **Two-phase commit** -- Coordinated across exactly-once sinks via `CheckpointCoordinator` (at-most-once sinks receive no pre_commit/commit guarantees)
5. **Recovery** -- `RecoveryManager` loads the latest checkpoint manifest, restores source offsets, rolls back exactly-once sinks, and resumes from committed state. Falls back to older checkpoints if latest is corrupt

## Delta Architecture (Distributed Mode)

With the `delta` feature enabled, multi-node operation:

- **Discovery** -- Static configuration, gossip-based (chitchat), or Kafka group discovery. Discovery via chitchat gossip is implemented.
- **Coordination** -- Metadata consensus (scaffolding only, not production-ready)
- **Partition Ownership** -- Epoch-fenced partition guards with consistent assignment
- **Distributed Checkpoints** -- Cross-node barrier coordination (planned; not yet implemented in checkpoint_coordinator)
- **Cross-Node Aggregation** -- Gossip partial aggregates and gRPC fan-out
- **Inter-Node RPC** -- gRPC service definitions for remote lookups, barrier forwarding, aggregate fan-out

**Status**: Discovery and coordination are implemented but not yet production-hardened. Production hardening is planned for Phase 6c.
