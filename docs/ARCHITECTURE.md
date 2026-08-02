# Architecture

## Overview

LaminarDB is an embedded streaming database designed for sub-microsecond latency. You link it into your application like SQLite, but instead of querying stored data, you're querying data as it arrives.

## Design Principles

1. **Embedded First** -- Single binary, no external dependencies
2. **Sub-Microsecond Latency** -- Minimal allocations on hot path
3. **SQL Native** -- Full SQL support via Apache DataFusion
4. **Explicit Delivery Contracts** -- Best-effort, at-least-once, or connector-gated exactly-once, validated before connector I/O
5. **Arrow-Native** -- Apache Arrow RecordBatch at every boundary

## Architecture Overview

The system has a coordinator layer (SQL execution, compiled projections), a background I/O layer (checkpoints, connector I/O), and a control plane (admin API, metrics).

```text
+------------------------------------------------------------------+
|                     SOURCE CONNECTORS                             |
|  Kafka, Postgres CDC, MongoDB CDC, WebSocket, OpenTelemetry,      |
|  OTLP/gRPC, Files (AutoLoader), Delta Lake,                       |
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
|  | (seal, decide)| |  writers)    |                                |
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
and injects/aligns in-band checkpoint barriers.

**Components:**
- **StreamExecutor** -- Drives DataFusion SQL execution per cycle. Optimization tiers: `CompiledProjection` (single-source non-aggregate queries compiled to `PhysicalExpr`), `IncrementalAggState` (incremental GROUP BY with per-group accumulators), and `CoreWindowState` (tumbling/hopping/session windows via optimized `CoreWindowAssigner`). Queries that don't match these tiers fall back to full DataFusion execution.
- **Operators** -- Stateless transforms (map, filter, project) and stateful operators (tumbling/sliding/hopping/session windows, the bounded interval join, lag/lead, ranking).
- **State** -- Stateful operators use one managed capture/restore/ownership lifecycle with operator-specific in-memory layouts. A checkpoint serializes and seals state under an immutable attempt; the manifest inventories the exact state keys, lengths, and digests.
- **Emit** -- Pushes output RecordBatches to downstream streams and sinks via tokio mpsc channels.

**Compiled query execution**: Non-aggregate single-source queries are compiled to `PhysicalExpr` projections on first execution, eliminating per-cycle SQL overhead. Complex queries cache their optimized logical plans.

**Streaming physical optimizer** (`StreamingPhysicalValidator`): Catches invalid physical plans (e.g., SortExec on unbounded streams) before execution. Configurable via `StreamingValidatorMode` (Reject, Warn, Off).

**Cooperative scheduling**: DataFusion's cooperative scheduling integration marks `StreamingScanExec` as `NonCooperative` so the engine wraps it with budget-aware `CooperativeExec` automatically.

**Structured error codes**: Every error carries a stable `LDB-NNNN` code (8 code ranges from general through internal). Hot-path errors use a zero-alloc `HotPathError` enum (2 bytes, `Copy`).

### Background I/O

Durability and I/O, runs on the main tokio async runtime (not the compute thread).

**Components:**
- **Checkpoint Coordinator** -- Orchestrates exact checkpoint attempts, durable manifests/decisions, source positions, and coordinated external commits for checkpoint-committable sinks (`laminar-db/src/checkpoint_coordinator.rs`). Manifests are written via filesystem or object store (`crates/laminar-core/src/checkpoint/checkpoint_store.rs`).
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

1. **Source ingestion**: Data arrives as Arrow RecordBatches via `SourceHandle::push()` or from external connectors (Kafka, PostgreSQL CDC, MongoDB CDC, WebSocket).
2. **Watermark tracking**: Each source maintains an `EventTimeExtractor` + `BoundedOutOfOrdernessGenerator` for watermark computation. Late rows are filtered. Watermarks can be per-partition, per-key, or aligned across sources.
3. **Operator processing**: The coordinator runs batches through SQL execution cycles (windows, joins, aggregations, filters). State is held in per-group accumulators and window buffers.
4. **Emit**: Results are published to named streams. Subscribers receive RecordBatches via typed `TypedSubscription<T>` or callback subscriptions.
5. **Durability**: Operator state and connector positions are captured under an exact attempt, sealed with a participant-complete inventory, then referenced by a prepared/finalized manifest and durable decision.
6. **Sink output**: Each sink advertises durability, topology, and input-mode contracts. Kafka, PostgreSQL, MongoDB, file, and Iceberg sinks provide at-least-once writes; coordinated append-mode Delta Lake implements external publication for admitted local exactly-once pipelines and direct S3/S3A cluster exactly-once pipelines.

## Crate Map

```text
laminar-core          Core: operators, window assigners, time/watermarks,
                      streaming channels (crossfire), subscriptions,
                      lookup tables, checkpoint barrier protocol, error codes,
                      checkpoint persistence: manifest, checkpoint store
                      (filesystem + object store), object store builder
                      |
laminar-sql           SQL parser (streaming extensions), query planner,
                      DataFusion integration, operator config translators,
                      custom UDFs (tumble, hop, session, slide, first_value, last_value),
                      streaming physical optimizer, cooperative scheduling,
                      PROCTIME() UDF, bounded interval-join translator
                      |
laminar-connectors    Kafka source/sink, PostgreSQL CDC/sink,
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
                      SQL operator routing and managed vnode state
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

Hot state stays in operator-specific in-memory maps and buffers under one managed vnode lifecycle.
The bounded interval join uses per-vnode state, aligned checkpoint capture, bounded restore, and
fenced ownership swaps in both local and cluster execution. Non-windowed keyed aggregates use the
same lifecycle in both modes; event-time windowed aggregation is not yet admitted in cluster mode.
Join output is hard-capped at 262,144 rows and 64 MiB per cycle, so excessive hot-key fanout causes
a terminal controlled failure instead of spilling or exhausting the process.

`laminar_core::state::StateBackend` persists immutable checkpoint-attempt vnode artifacts and the
exact-attempt durability seal. It is recovery authority, not a point/range store on the row path.
Unbounded working sets can still outgrow RAM, so deployments must size or bound them until the
selected worker-local TidesDB path is integrated and qualified against the production gates. See
[ADR-008](architecture-decisions/ADR-008-managed-vnode-keyed-state.md).

### Streaming Channels

Source and Sink objects in the public streaming API (`laminar_core::streaming`) are backed by `crossfire::mpsc::Array<T>` channels (bounded, blocking sender + async receiver). Clone the `Source<T>` for multi-producer use. Internally, the `StreamingCoordinator` uses `tokio::sync::mpsc` for source-task → coordinator communication across runtimes, and subscribers receive updates via the subscription registry in `laminar_core::streaming::subscription`.

### Connector SDK

Custom connectors implement `SourceConnector` and `SinkConnector`. Core lifecycle methods are shown below; optional hooks are omitted:

```rust
#[async_trait]
pub trait SourceConnector: Send {
    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError>;
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError>;
    async fn poll_batch(&mut self, max_records: usize) -> Result<Option<SourceBatch>, ConnectorError>;
    fn schema(&self) -> SchemaRef;
    fn checkpoint(&self) -> SourceCheckpoint;
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

#[async_trait]
pub trait SinkConnector: Send {
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError>;
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError>;
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError>;
    fn schema(&self) -> SchemaRef;
    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;
    async fn pre_commit(&mut self, epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError>;
    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError>;
    fn suggested_write_timeout(&self) -> Duration;
    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter>;
    async fn close(&mut self) -> Result<(), ConnectorError>;
}
```

`SourceCheckpoint` contains only connector-specific offsets and metadata. Exact
attempt identity is carried once by the resume/barrier protocol and the enclosing
checkpoint manifest, so connector payloads cannot disagree with recovery about
which attempt they belong to.

The typed source/sink contracts are the sole admission authority; independent capability booleans
cannot form contradictory protocol combinations. Checkpoint-committable sinks return an immutable prepared descriptor. The runtime seals every participant's
descriptor, records one exact durable decision, and only then invokes the designated coordinated
committer; there is no per-writer inline `commit_epoch` path. The SDK also adds retry policies,
rate limiting, circuit breakers, and a test harness.

### Lookup Tables

Enrichment joins via `CREATE LOOKUP TABLE` DDL with hash-probe physical execution:

- **Hash-indexed snapshot** -- lookup data pre-indexed at query planning time via Arrow `RowConverter`
- **Predicate pushdown** -- `PredicateSplitterRule` splits WHERE predicates; pushdown predicates filter the snapshot before index build
- **Refresh admission** -- only sources with an explicit snapshot-completion boundary can hydrate a reference table; an empty CDC poll is not a snapshot boundary
- **Partial cache with Xor filter** -- probabilistic membership test to avoid full scans
- **Lookup sources** -- `PostgresLookupSource`, `ParquetLookupSource` for direct queries

### Deployment Profiles

Pre-configured deployment tiers (`laminar_db::Profile`). Each tier includes all capabilities of the tiers below it: `BareMetal ⊂ Embedded ⊂ Durable ⊂ Cluster`.

| Profile | Description |
|---------|-------------|
| `BareMetal` | Default. In-memory only, no persistence. Fastest startup. |
| `Embedded` | Local filesystem checkpoint persistence for single-node embedded use. |
| `Durable` | Object-store checkpoints (S3/GCS/Azure) for recovery. |
| `Cluster` | Distributed deployment with static/Chitchat discovery, shared-store assignment CAS plus a renewable leader lease, and gRPC/Arrow-Flight shuffles. |

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
| Bounded joins | `INNER`, `LEFT`, `RIGHT`, `FULL`, `LEFT/RIGHT SEMI`, `LEFT/RIGHT ANTI` | Direct watermarked append-only sources |
| Lookup tables | `CREATE LOOKUP TABLE ... (...) WITH ('connector' = 'postgres', ...)` | Reference data |
| LAG/LEAD | `LAG(col, offset) OVER (...)` | Sliding analytics |
| Ranking | `ROW_NUMBER() OVER (...)` | Ranking functions |
| Window frames | `ROWS BETWEEN ... AND ...` | Custom frame bounds |
| Config vars | `${VAR}` in SQL strings | Variable substitution |

Queries are planned by `StreamingPlanner` and executed via DataFusion, with compiled projections and cached logical plans eliminating per-cycle planning overhead for hot-path queries.

## Performance Characteristics

Historical mean-latency measurements (laptop hardware, Criterion microbenchmarks):

| Operation | Target | Measured | Technique |
|-----------|--------|----------|-----------|
| Event processing (mean) | < 10us | 0.55-1.16us | Zero per-event allocation, compiled projections |
| Throughput/core (reactor+window) | 500K/s | 1.1-1.46M/s | Batch processing, Arrow columnar |
| Checkpoint recovery | < 10s | 1.39ms | Full snapshots, manifest-based recovery |

These are Criterion means from a developer laptop, not continuously
validated in CI. True p99 under sustained load is not currently
measured. Run `cargo bench` on your own hardware to get numbers that
mean something for your workload.

## Execution Model

The thread-per-core model described in earlier documentation was removed in PR #204. The current execution model is a single `StreamingCoordinator` tokio task that:

1. Receives batches from source connectors via `tokio::sync::mpsc` channels
2. Executes SQL cycles via `StreamExecutor` (compiled projections, incremental aggregations, DataFusion fallback)
3. Manages checkpoint barriers for the selected pipeline-wide recovery contract
4. Routes results to sink connectors

### Coordinator and Executor

**Execution:** SQL queries are executed through `StreamExecutor::execute_cycle()`, processing `RecordBatch` micro-batches with per-group `FxHashMap` state. The `OperatorGraph` manages the operator topology.

**Coordinator → Executor relationship:** `StreamingCoordinator` is the
single tokio task that owns the event loop. It calls `PipelineCallback::execute_cycle()`,
which delegates to `StreamExecutor::execute_cycle()`. The coordinator drives the
executor through a callback interface. There is a single execution loop, not
multiple competing ones.

### Deployment Model

LaminarDB runs embedded, as a standalone single node, or as a cluster. Cluster
state is vnode-scoped and requires a cloud object store that is visible to every
node. Local paths and `file://` storage are node-durable only.

## Exactly-Once Semantics

Exactly-once delivery works through:

1. **Source offsets** -- Tracked per-source, persisted in checkpoint manifests
2. **Barrier-based snapshots** -- `StreamingCoordinator` injects checkpoint barriers at sources; all sources align on the barrier before operator state is captured
3. **Checkpoints** -- Immutable attempts bind operator-state seals, source positions, watermarks, participant markers, parent links, pipeline identity, and deployment incarnation before finalization
4. **Coordinated external commit** -- Participant-complete prepared markers are sealed before a designated committer publishes a namespaced checkpoint cut to append-mode Delta Lake
5. **Recovery** -- `RecoveryManager` accepts an identity-matching Finalized manifest or an exact
   decided Prepared manifest (which it finalizes), restores state/source positions, and resumes
   external commits from their exact cursor
6. **Decision retention** -- Before artifact deletion, the coordinator publishes a monotonic,
   deployment-scoped durable GC floor containing the full canonical predecessor decision. That
   anchor preserves external-cursor continuity after the corresponding raw decision is removed.
7. **Shutdown ownership** -- An issued decision write remains owned until its task settles.
   Teardown never cancels or detaches it before releasing deployment or recovery fences.

Embedded and single-node startup additionally require node-durable state, the built-in local
checkpoint/decision store held by an exclusive OS deployment lock, an exact-delivery-certified
source, and a `CheckpointCommittable` sink. In the standalone server, a
`file://` checkpoint URL selects that built-in store. Shared object-store URLs
and library-injected object or decision stores fail closed with `[LDB-0014]`
because their provenance cannot yet prove that the local lock fences every
writer. Incompatible connectors fail before external I/O;
there is no per-connector delivery override or public writer ID.

Exactly-once admission requires an exact-certified source and a `CheckpointCommittable` sink. Kafka
source is replayable, splittable, and exact-certified. Delta coordinated append is the local exact
path; only direct S3/S3A Delta is cluster-exact certified. Standard Azure and GCS Delta targets are
shared at-least-once targets but await native-provider fault soaks for cluster EO. Iceberg append is
`DurableAtLeastOnce` and has no checkpoint-bound catalog cursor, so EO admission rejects it. The
Kafka sink forces `acks=all` but remains `DurableAtLeastOnce`; it does not implement transactional
checkpoint commit. Unsupported connector combinations fail before I/O, and the end-to-end
production soak remains pending.

PostgreSQL CDC is resume-only: fresh `Initial` startup is rejected before I/O. MongoDB CDC has
event-level resume but no initial snapshot or transaction-group guarantee. Delta Lake and Iceberg
sources are ephemeral singletons, so they are local `BestEffort`-only sources and are unavailable in
cluster mode.

## Cluster Architecture (Distributed Mode)

With the `cluster` feature enabled, multi-node operation provides:

- **Discovery** -- Static seed configuration or Chitchat gossip membership.
- **Coordination** -- Shared-store assignment CAS and a renewable leader lease; no embedded Raft service
- **Partition Ownership** -- Epoch-fenced partition guards with consistent assignment
- **Distributed Checkpoints** -- Cross-node capture and shared durable state for admitted at-least-once and exactly-once recovery
- **Cross-Node Streaming** -- Vnode-keyed row shuffle with assignment and process-generation fencing
- **Inter-Node Control** -- Process-bound gRPC barrier delivery with durable authority validation

**Delivery boundary**: cluster mode requires cluster-shared S3/GCS/Azure state. At-least-once uses
replayable sources and durable sinks. Exactly-once is admitted only for certified Kafka input and
direct S3/S3A append-mode Delta; incompatible sources or sinks fail with `[LDB-5035]`
before connector I/O.

**SQL boundary**: cluster `CREATE STREAM` admits stateless pipelines, supported non-windowed keyed
aggregates, and one bounded append-only event-time equi-join stage over direct watermarked sources.
That stage supports `INNER`, `LEFT`, `RIGHT`, `FULL`, `LEFT/RIGHT SEMI`, and `LEFT/RIGHT ANTI` with
ordered `Utf8`/`Int64` composite keys and explicit aliases for every projected expression. A named
join stream from any of the eight kinds may feed a separate keyed aggregate stream. Fused
`JOIN ... GROUP BY`, cluster windowed aggregation, intermediate-input or multi-way joins,
temporal/lookup joins, and cluster
materialized views fail closed. See [distributed state](DISTRIBUTED_STATE.md).
