# Unified Execution Audit: Critical Architecture Review

**Date**: 2026-03-14
**Scope**: TPC, io_uring, Reactor, DagExecutor, StreamExecutor, Compiler

---

## Executive Summary

LaminarDB has **three separate execution substrates** that do not share code,
state management, or scheduling. The thread-per-core (TPC) infrastructure —
4,336 LOC of SPSC queues, core pinning, backpressure, and idle strategies —
processes **zero operators** in production. Ring 0 cores receive events from
source I/O threads and immediately relay them to the coordinator via SPSC
outbox. All actual operator processing (aggregation, windowing, joins) runs
on a single tokio task via DataFusion SQL evaluation.

The io_uring module (2,555 LOC) is feature-gated, `enable_storage_io` defaults
to `false` everywhere, and no code path enables it. It is dead code.

The codebase maintains 10,208 LOC of TPC/io_uring/storage_io/reactor/pipeline
infrastructure that adds **two unnecessary thread hops** (source→core→coordinator)
where zero would suffice.

**Recommendation**: Full architectural rewrite. Remove the current three-substrate
architecture and replace with a single execution model.

---

## Phase 1: Extended Divergence Audit

### 1.0 The Three Execution Substrates

```
Substrate 1: REACTOR (Ring 0)
  Location:   crates/laminar-core/src/reactor/mod.rs
  LOC:        899
  Model:      Linear operator chain (Vec<Box<dyn Operator>>)
  State:      Single shared Box<dyn StateStore>
  Scheduling: spin_loop() in core_thread_main
  Used by:    core_thread_main (but with EMPTY operator list)
  Status:     PASSTHROUGH RELAY — no operators registered

Substrate 2: DAG EXECUTOR
  Location:   crates/laminar-core/src/dag/executor.rs
  LOC:        7,341 (including tests, topology, routing)
  Model:      Topological graph with RoutingTable
  State:      Per-node Box<dyn StateStore>
  Scheduling: Synchronous process_dag() in topological order
  Used by:    Tests and benchmarks ONLY
  Status:     ZERO PRODUCTION CALLERS

Substrate 3: STREAM EXECUTOR (SQL / Ring 1)
  Location:   crates/laminar-db/src/stream_executor.rs
  LOC:        3,841 + 2,700 (aggregate_state.rs)
  Model:      DataFusion micro-batch SQL execution
  State:      FxHashMap<usize, IncrementalAggState/EowcState/CoreWindowState>
  Scheduling: Async — ctx.sql().await?.collect().await on tokio task
  Used by:    TpcPipelineCoordinator (THE production path)
  Status:     ALL PRODUCTION WORK HAPPENS HERE
```

### 1.1 Production Data Flow (What Actually Happens)

```
Source (tokio::spawn)
  │
  │  connector.poll_batch(max_records).await  [tokio I/O]
  │  Event::new(timestamp, RecordBatch)
  │
  ├──── THREAD HOP #1 ────►  SPSC inbox  ────►  Core Thread (Ring 0)
  │                                                │
  │                                                │  reactor.submit(event)
  │                                                │  reactor.poll_into()  ← EMPTY operators
  │                                                │  (event passes through unchanged)
  │                                                │
  │                           SPSC outbox  ◄────   │
  │                                │
  ├──── THREAD HOP #2 ────►  TpcPipelineCoordinator (tokio task)
  │                                │
  │                                │  Convert TaggedOutput → HashMap<String, Vec<RecordBatch>>
  │                                │  callback.extract_watermark()
  │                                │  callback.filter_late_rows()
  │                                │
  │                                │  callback.execute_cycle()  [THE ACTUAL WORK]
  │                                │    │
  │                                │    ├─ register_source_tables() ← MemTable::try_new() ALLOC
  │                                │    ├─ for query in topo_order:
  │                                │    │   ├─ ctx.sql(&sql).await?.collect().await  ← DF ALLOC
  │                                │    │   ├─ IncrementalAggState::process_batch() ← HashMap ALLOC
  │                                │    │   └─ register intermediate MemTable     ← clone ALLOC
  │                                │    └─ cleanup_source_tables()
  │                                │
  │                                │  callback.write_to_sinks()
  │                                ▼
                                 Sinks
```

**The core thread is a message relay.** Evidence:
- `tpc_runtime.rs:70`: `Vec::new()` passed as operators
- `tpc_coordinator.rs:299`: log message says `"cores currently run no operators"`

### 1.2 What TPC Costs vs What It Delivers

```
COST:
  LOC:           4,336 (TPC) + 882 (storage_io) + 899 (reactor) + 1,536 (pipeline) = 7,653
  Thread hops:   2 per event (source→core→coordinator)
  Memory:        N core threads × (8KB inbox + 8KB outbox + reactor + poll_buffer)
  Complexity:    SPSC queue, CachePadded, tiered idle, catch_unwind, credit gate
  CPU waste:     N-1 cores spin-sleeping with no work (N = available_parallelism)
  Latency:       +spin_loop→park_timeout wakeup + SPSC push/pop ≈ 200ns-1ms added

DELIVERS:
  - Wakeup notification (core_thread.unpark() → output_notify.notify_one())
  - Message ordering (FIFO through SPSC)
  - Checkpoint barrier forwarding (drain reactor before barrier)

  Could be replaced by:
  - tokio::sync::mpsc channel (source → coordinator direct)
  - tokio::sync::Notify for wakeup
  - Barrier forwarding in the channel protocol
```

### 1.3 What io_uring Costs vs What It Delivers

```
COST:
  LOC:           2,555 (io_uring) + 882 (storage_io) = 3,437
  Complexity:    CoreRingManager, BufferPool, SQE/CQE management, SQPOLL
  Feature gate:  cfg(all(target_os = "linux", feature = "io-uring"))
  Dependencies:  io-uring crate (optional)

DELIVERS:
  - Nothing. enable_storage_io = false everywhere.
  - No production code path enables it.
  - WAL writes use tokio::fs, not io_uring.
  - Checkpoint writes use object_store, not io_uring.

VERDICT: Dead code. 3,437 LOC for zero production value.
```

### 1.4 What the DAG Executor Costs vs What It Delivers

```
COST:
  LOC:           7,341 (including 3,671 test LOC)
  Complexity:    RoutingTable, DagWatermarkTracker, multicast, topology builder
  Maintenance:   Operator trait contract must be maintained for both paths

DELIVERS:
  - Foundation for future programmatic API (non-SQL)
  - Correct topological execution model
  - Zero-allocation routing via pre-computed RoutingTable
  - Per-node state isolation

  BUT: Zero production callers. Not wired to TPC. No integration test with real data.
```

### 1.5 What the Compiler Costs vs What It Delivers

```
COST:
  LOC:           11,579
  Complexity:    Cranelift JIT, row format, pipeline bridge, batch reader
  Dependencies:  cranelift-* crates (optional, feature-gated)

DELIVERS:
  - Compiles Filter+Project to native code (zero-alloc hot path)
  - Row-oriented event format with bump allocation
  - SPSC bridge between Ring 0 compiled path and Ring 1 output

  BUT: Only handles stateless operators. Any query with GROUP BY, JOIN,
  ORDER BY, or windows falls back to DataFusion. Feature-gated behind "jit".
  Not connected to TPC Ring 0 or StreamExecutor.
```

### 1.6 Platform Analysis

```
LINUX:
  CPU pinning:    ✓ sched_setaffinity
  NUMA binding:   ✓ (optional)
  io_uring:       ✓ (feature-gated, disabled)
  TPC value:      Minimal — cores do no work

WINDOWS (current dev platform):
  CPU pinning:    ✓ SetThreadAffinityMask (< 64 cores)
  NUMA binding:   ✗
  io_uring:       ✗ (not available)
  TPC value:      None — same passthrough relay, no platform-specific benefit

macOS:
  CPU pinning:    ✗ (no-op)
  NUMA binding:   ✗
  io_uring:       ✗
  TPC value:      None
```

### 1.7 Ring 0 Violation Summary (All Substrates)

| Location | Violation | Severity |
|----------|-----------|----------|
| StreamExecutor (Ring 1, should be Ring 0) | `ctx.sql().await.collect().await` — unbounded DataFusion allocation every cycle | CRITICAL |
| StreamExecutor | `MemTable::try_new()` + `.clone()` per intermediate result | CRITICAL |
| IncrementalAggState | `Box::new(Accumulator)` per new group, RowConverter per batch | CRITICAL |
| DagExecutor | `Box<dyn Operator>` dynamic dispatch per node | SLOP |
| DagExecutor | `event.clone()` on multicast fan-out | BROKEN |
| Reactor | `Vec<Box<dyn Operator>>` dynamic dispatch | SLOP |
| Reactor | `output_buffer.shrink_to(4096)` reallocation | BROKEN |
| core_thread_main | Fallback `Box::new(CheckpointCompleteData)` | BROKEN |

---

## Phase 2: How Production Systems Solve This

### Arroyo (Rust, 2024-2025) — Verified via Source Code

**Compilation pipeline** (3 stages):
1. SQL → DataFusion `LogicalPlan` (via `arroyo-planner`)
2. `LogicalPlan` → `LogicalGraph` (petgraph DAG with `OperatorName` enum + protobuf config)
3. `LogicalGraph` → Physical execution via `construct_operator()` mapping each
   `OperatorName` to a concrete `ArrowOperator` implementation

**Core operator trait** (`arroyo-operator/src/operator.rs`):
```rust
#[async_trait]
pub trait ArrowOperator: Send + 'static {
    async fn process_batch(&mut self, batch: RecordBatch, ctx: &mut OperatorContext,
                           collector: &mut dyn Collector) -> DataflowResult<()>;
    async fn handle_watermark(&mut self, watermark: Watermark, ...) -> ...;
    async fn handle_checkpoint(&mut self, barrier: CheckpointBarrier, ...) -> ...;
}
```

**Scheduling**: Standard tokio multi-threaded runtime (`tokio::spawn`).
Each subtask is one tokio task. NO thread-per-core, NO CPU pinning, NO SPSC.
Uses tokio MPSC with atomic backpressure.

**Operator chaining** (0.13+): Adjacent stateless operators are fused into a
`ChainedOperator` linked list. Output flows through `ChainedCollector` with
**no queue between chained operators**. Only breaks at shuffle boundaries.

**Fan-out**: `ArrowCollector::collect()` hash-partitions batches using
`hash_utils::create_hashes()` then Arrow `sort_to_indices` + `partition`.
Zero-copy slicing on `RecordBatch`. Barriers broadcast to all outputs.

**Checkpoint**: Chandy-Lamport aligned barriers. `CheckpointCounter` tracks
which inputs have sent barriers. Blocked inputs removed from `FuturesUnordered`
until all barriers arrive. State serialized to object store.

**No io_uring**: Zero references in codebase. Uses tokio for all I/O.
Arrow handles memory through its own allocator. No arena allocators.

**Key lesson**: Arroyo targets millisecond latency, not microsecond. Throughput
comes from columnar Arrow processing (SIMD kernels), not from thread pinning.
Their architecture is dramatically simpler than LaminarDB's TPC and handles
real production workloads.

### Apache Flink 2.0 (Java, 2025) — Verified via Documentation + VLDB 2025

**Compilation pipeline** (4 stages):
1. Both SQL/Table API and DataStream API produce `Transformation` objects
2. `StreamGraphGenerator` → `StreamGraph` (logical DAG of `StreamNode`)
3. `StreamGraph` → `JobGraph` (operator chaining: chainable ops fused into one `JobVertex`)
4. `JobGraph` → `ExecutionGraph` (physical parallel plan with concrete resources)

**Critical architectural point**: SQL and DataStream API converge at the
`Transformation` level. SQL operators are code-generated implementations of
the same `StreamOperator` interface (`OneInputStreamOperator.processElement()`).
Both run inside the same `StreamTask` with the same `MailboxProcessor`.

**MailboxProcessor** (FLINK-12477):
```
while (running) {
    processMail();           // checkpoint triggers, timer firings, control
    mailboxDefaultAction();  // processInput() — one event from input
}
```
Single-threaded guarantee: record processing and control actions (checkpoint,
timer) execute on the same thread, never concurrently. No locks needed. External
threads post `Runnable` actions to the mailbox via concurrent queue, but
execution is single-threaded.

**Threading**: NOT thread-per-core. One dedicated thread per task chain.
`taskSlots = CPU cores` approximates TPC but is fundamentally "one thread
per task chain" with static assignment. No work stealing.

**FLIP-315 Operator Fusion Codegen**: Fuses operator DAG into single generated
class eliminating vtable calls. 12% overall gain (>30% on some queries).
Currently batch-only.

**FLIP-423/424/425 Disaggregated State (ForSt)**: State lives on remote DFS
(S3/HDFS) with local disk cache. Async state APIs (`FLIP-424`) post callbacks
to mailbox, maintaining single-threaded guarantee. 2.5x TPS improvement over
synchronous local RocksDB. VLDB 2025 paper.

**Lesson**: MailboxProcessor is the gold standard for single-threaded operator
execution. SQL compiles to same operator types as programmatic API.

### Feldera / DBSP (Rust, 2025) — Verified via Source Code + Papers

**Compilation pipeline**:
1. SQL → Calcite parser → `CalciteProgram`
2. `CalciteToDBSPCompiler` → `DBSPCircuit` (incremental Z-set operators)
3. `CircuitOptimizer` → optimized circuit
4. **Rust code generation** → native machine code via `rustc`

**Programmatic API**: Yes — `dbsp` crate provides typed `Stream<Circuit, Data>`
API. SQL compiler literally emits Rust code calling the same API. Both share
the same `RootCircuit` and synchronous evaluation loop.

**Synchronous clock model**:
- Single logical clock divides time into intervals
- Each tick: collect input changes (Z-sets) → compute all outputs → emit
- Work per tick proportional to **change size**, not data size
- **No barrier alignment needed**: synchronous model = all operators at same
  logical time. Checkpoints happen at clock boundaries naturally.

**Threading**: Shared-nothing worker threads with static sharding.
Each worker processes full circuit on its shard. Hash-based sharding for joins.
Not TPC — worker-based parallelism. Background threads for storage ops.

**Operator abstraction**: Dynamic dispatch (trait objects). Explicit trade-off:
compilation speed vs runtime performance. Complex circuits would produce
enormous binaries with monomorphization.

**State**: RocksDB per-thread. Designed to exceed RAM (spill to NVMe).
Passes all 7 million SQL Logic Tests.

**Lesson**: Simplest model that works. Synchronous step eliminates checkpoint
complexity. SQL compiles to same primitives as programmatic API.

### RisingWave (Rust, 2025) — Verified via Developer Guide + Architecture Docs

**Architecture**: SQL-only (no programmatic API). SQL → streaming plan →
fragments → actors → executor chains.

**Actor model**:
- Fragment (sub-graph of operators) × parallelism = Actor
- Each actor: Merger → Executor chain → Dispatcher
- Merger handles barrier alignment for multi-input operators
- Executors process `Message` types (data chunks + barriers)

**Scheduling**: Tokio multi-threaded async runtime with work-stealing.
Actors are async tasks. No thread-per-core (explicitly rejected — operator
load varies, static assignment wastes cores). Config: `actor_runtime_worker_threads_num`.

**Checkpoint**: Chandy-Lamport barriers. Meta service injects at sources.
Merger does alignment (buffers data from channels that delivered barrier,
waits for remaining). State → shared buffer → SST files → S3.
Hummock: custom LSM-tree state store.

**Lesson**: Actors + tokio work-stealing scales well. Barrier alignment
is straightforward in-process.

### Cross-Cutting Answers

| Question | Answer |
|----------|--------|
| Does SQL lower into same primitives as programmatic API? | **YES in all four systems.** Flink: both produce `Transformation` → `StreamGraph`. Arroyo: both produce `ArrowOperator`. Feldera: SQL emits same Rust circuit API. RisingWave: SQL-only, single executor trait. |
| Canonical operator abstraction? | Flink: Java interface (`StreamOperator`). Arroyo: async trait (`ArrowOperator`). Feldera: trait objects (explicit dynamic dispatch trade-off). RisingWave: async executor trait. **All use trait-based dispatch, not enum.** |
| Fan-out without allocation? | **Batch-level cloning**: Arrow `RecordBatch` clone = Arc refcount bump O(1). Flink: pre-allocated network buffer pool. Feldera: Z-set batch passing. None achieve per-record zero-alloc fan-out; batching amortizes. |
| Operator-to-core assignment? | Flink: static (one thread per task chain). Feldera: static sharding (workers). Arroyo/RisingWave: tokio work-stealing. **None use TPC.** |
| Checkpoint in single-process? | Feldera's synchronous clock model is ideal: checkpoints at clock boundaries, no barrier alignment needed. Alternatively, Chandy-Lamport barriers through in-process channels (Flink/Arroyo/RisingWave). |
| io_uring for streaming? | **None of the four use io_uring.** arXiv 2025 paper confirms: io_uring as drop-in for epoll yields only 1.10x. Meaningful gains (2.05x) require full architectural redesign around SQ batching. Only useful for **storage I/O** (WAL, checkpoint), not streaming operator I/O. |

---

## Phase 3: Architectural Recommendation

### The Verdict: TPC is Over-Engineered for This System

Thread-per-core is the right architecture for a **network server** processing
millions of independent requests per second (ScyllaDB, Seastar, Redpanda).
It is the **wrong architecture** for a **streaming database** where:

1. **Operators are stateful and interdependent** — aggregate state, window state,
   and join state create data dependencies that prevent parallel execution
   without partitioning.

2. **The hot path is SQL evaluation** — DataFusion's async execution model is
   fundamentally incompatible with synchronous spin-loop Ring 0.

3. **Workload is bursty, not uniform** — Kafka polls return variable batch sizes.
   Static core assignment wastes cores during idle periods.

4. **Single-process deployment** — no network I/O between operators.
   io_uring's advantage (kernel-bypassed I/O) is irrelevant when all data
   flows through in-memory channels.

5. **The evidence is clear** — after extensive development, the team was unable
   to wire operators into Ring 0. The production path bypasses it entirely.

### 3.1 Recommended Architecture: Simplified Streaming Executor

**Delete**:
- TPC module (core_handle, SPSC, backpressure, router) — 4,336 LOC
- io_uring module — 2,555 LOC
- storage_io module — 882 LOC
- Reactor module — 899 LOC
- Pipeline coordination layer (tpc_runtime, tpc_coordinator) — 1,536 LOC
- **Total deleted: ~10,208 LOC**

**Keep and evolve**:
- DagExecutor (7,341 LOC) — the correct execution model, needs to become primary
- Operator trait + all operators (27,240 LOC) — the real value
- StreamExecutor state management (agg_state, eowc_state, core_window_state)
- Compiler module (11,579 LOC) — future JIT optimization path
- Checkpoint infrastructure (barriers, alignment, manifests)

**New architecture**:

```
Source (tokio task)
  │
  │  connector.poll_batch().await
  │
  ├──── mpsc channel ────►  StreamingCoordinator (single tokio task)
  │                              │
  │                              │  1. Receive batches from all sources
  │                              │  2. Convert to Event format
  │                              │  3. Execute through OperatorGraph
  │                              │  4. Handle checkpoint barriers
  │                              │  5. Write to sinks
  │                              ▼
                               Sinks
```

### 3.2 The Unified OperatorGraph

Replace all three substrates with a single `OperatorGraph` that:

1. **Reuses DagExecutor's topology** — topological ordering, RoutingTable,
   per-node state, fan-out via multicast
2. **Reuses StreamExecutor's state types** — IncrementalAggState and
   CoreWindowState are well-tested and correct
3. **Uses the Operator trait** — all 12+ operator implementations already
   exist and work

```rust
/// Single execution substrate for all query types.
///
/// This replaces DagExecutor, Reactor, and StreamExecutor with one struct.
/// SQL queries and programmatic DAG pipelines both produce an OperatorGraph.
pub struct OperatorGraph {
    /// Operators indexed by NodeId. None = passthrough (source/sink nodes).
    operators: Vec<Option<OperatorSlot>>,
    /// Per-node runtime state (timer service, state store, watermark gen).
    runtimes: Vec<Option<NodeRuntime>>,
    /// Pre-computed routing table for O(1) dispatch.
    routing: RoutingTable,
    /// Topological execution order (from finalized DAG).
    execution_order: Vec<NodeId>,
    /// Pre-allocated input queues per node.
    input_queues: Vec<VecDeque<Event>>,
    /// Collected sink outputs per node.
    sink_outputs: Vec<Vec<Event>>,
    /// Source and sink node IDs.
    source_nodes: Vec<NodeId>,
    sink_nodes: Vec<NodeId>,
    /// Temporary buffer for draining (avoids allocation).
    temp_events: Vec<Event>,
    /// Metrics.
    metrics: GraphMetrics,
}

/// Each node slot holds one of two operator kinds.
///
/// Enum dispatch (not Box<dyn>) eliminates vtable indirection on the hot path.
/// Both variants support checkpoint/restore via their respective interfaces.
pub enum OperatorSlot {
    /// Core streaming operator (window, join, session, lag/lead, topk, etc.)
    /// Uses the existing Operator trait with per-node StateStore.
    /// This covers all 12+ operator types already implemented.
    Core(Box<dyn Operator>),

    /// SQL batch operator wrapping DataFusion accumulator state.
    /// Processes RecordBatch input, maintains incremental state, emits batches.
    /// This replaces StreamExecutor's per-query routing logic.
    Sql(SqlOperator),
}

/// SQL operator variants — replaces StreamExecutor's per-query dispatch.
pub enum SqlOperator {
    /// Incremental running aggregate (non-windowed GROUP BY).
    /// Wraps IncrementalAggState from aggregate_state.rs.
    IncrementalAgg(IncrementalAggState),

    /// Windowed aggregate with EMIT ON WINDOW CLOSE.
    /// Wraps CoreWindowState from core_window_state.rs.
    WindowedAgg(CoreWindowState),

    /// EOWC fallback for non-aggregate windowed queries.
    /// Wraps IncrementalEowcState from eowc_state.rs.
    EowcAgg(IncrementalEowcState),

    /// Stateless filter + projection (compiled or DataFusion-evaluated).
    /// For queries with no aggregation, join, or window.
    Stateless(StatelessSqlOp),

    /// ASOF join operator.
    AsofJoin(AsofJoinState),

    /// Temporal (versioned) join operator.
    TemporalJoin(TemporalJoinState),
}
```

**Why enum dispatch instead of trait objects**:
- `OperatorSlot` is 2 variants → branch predictor handles well
- `SqlOperator` is 6 variants → still predictable (common path is 2-3)
- No vtable load on hot path (saves ~5ns per dispatch)
- Arroyo uses a similar `OperatorName` enum → concrete constructor pattern
- Flink uses `StreamOperator` trait but amortizes over batches

### 3.3 SQL Lowering: DataFusion → OperatorGraph

DataFusion remains the SQL parser and logical optimizer. But instead of
executing SQL at runtime, lower the logical plan to OperatorGraph nodes:

```
SQL: SELECT symbol, SUM(volume)
     FROM trades
     GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE)

Logical Plan (DataFusion):
  Aggregate(group=[symbol, tumble(...)], aggr=[SUM(volume)])
    └─ TableScan(trades)

OperatorGraph:
  Source("trades")  ──►  WindowedAgg(TumblingWindowAssigner(60s),
                                     groups=[symbol],
                                     accumulators=[SUM(volume)])
                    ──►  Sink("output")
```

For stateless queries (no agg/join/window):
```
SQL: SELECT symbol, price * 1.1 AS adjusted
     FROM trades
     WHERE price > 100

OperatorGraph:
  Source("trades")  ──►  Compiled(filter: price > 100,
                                  project: [symbol, price * 1.1])
                    ──►  Sink("output")
```

### 3.4 Coordinator Simplification

```rust
/// Single coordinator replaces TpcPipelineCoordinator + TpcRuntime + Reactor.
///
/// One tokio task. Sources feed it via mpsc channels. Graph execution is
/// synchronous (no .await during operator processing). Sink writes are async.
pub struct StreamingCoordinator {
    /// Source receivers (one per source).
    /// Each source runs as a separate tokio task calling connector.poll_batch().
    sources: Vec<SourceHandle>,
    /// The unified execution graph (synchronous processing).
    graph: OperatorGraph,
    /// Sink writers (async I/O).
    sinks: Vec<Box<dyn SinkConnector>>,
    /// Checkpoint coordinator (barrier injection, alignment, persistence).
    checkpointer: CheckpointCoordinator,
    /// Per-source watermark tracking.
    watermarks: WatermarkTracker,
    /// Shutdown signal.
    shutdown: tokio::sync::Notify,
    /// Batch coalescing window (amortizes overhead for high-throughput sources).
    batch_window: Duration,
}

/// Handle to a running source task.
struct SourceHandle {
    source_idx: usize,
    name: String,
    /// Receives SourceBatch or BarrierMsg from the source task.
    rx: mpsc::Receiver<SourceMsg>,
    /// For injecting checkpoint barriers into the source.
    barrier_tx: watch::Sender<Option<CheckpointBarrier>>,
}

enum SourceMsg {
    Batch(SourceBatch),
    Barrier(CheckpointBarrier),
    Error(String),
    Eof,
}

impl StreamingCoordinator {
    pub async fn run(&mut self) {
        // Merged stream: select from all sources + shutdown + checkpoint timer
        loop {
            // 1. Wait for data from any source (or shutdown/timer)
            let msg = tokio::select! {
                biased;
                () = self.shutdown.notified() => break,
                msg = self.receive_next() => msg,
                () = self.checkpoint_timer() => {
                    self.inject_checkpoint_barriers();
                    continue;
                }
            };

            // 2. Process the message
            match msg {
                SourceMsg::Batch(batch) => {
                    // Convert SourceBatch → Event (timestamp extraction)
                    let event = Event::new(
                        extract_timestamp(&batch.records),
                        batch.records,
                    );

                    // Advance watermark for this source
                    self.watermarks.on_event(batch.source_idx, event.timestamp);

                    // Execute through graph (SYNCHRONOUS — no .await, no alloc)
                    let source_node = self.graph.source_node_for(batch.source_idx);
                    self.graph.process_event(source_node, event).ok();

                    // Fire timers if watermark advanced
                    let wm = self.watermarks.current_watermark();
                    self.graph.fire_timers(wm);
                }
                SourceMsg::Barrier(barrier) => {
                    // Track barrier alignment across sources
                    if self.checkpointer.mark_aligned(barrier.source_idx) {
                        // All sources aligned — snapshot graph state
                        let states = self.graph.checkpoint();
                        self.checkpointer.persist(states).await;
                    }
                }
                SourceMsg::Error(e) => {
                    tracing::warn!("Source error: {e}");
                }
                SourceMsg::Eof => {
                    // Source exhausted — check if all done
                    if self.all_sources_done() { break; }
                }
            }

            // 3. Drain sink outputs (async I/O)
            let outputs = self.graph.take_all_sink_outputs();
            if !outputs.is_empty() {
                self.write_to_sinks(&outputs).await;
            }
        }
    }
}
```

**Key differences from current TpcPipelineCoordinator**:
- **No thread hops**: Sources push directly to coordinator via mpsc channel
- **No Reactor/SPSC**: Eliminated two unnecessary intermediaries
- **Synchronous graph execution**: `graph.process_event()` is sync within the async task
- **Single watermark tracker**: Replaces per-source watermark extraction in callback
- **Barrier alignment built-in**: No separate PendingBarrier struct

### 3.5 SQL Lowering Rules: DataFusion LogicalPlan → OperatorGraph

DataFusion parses SQL and produces a `LogicalPlan`. We walk this plan and emit
`OperatorGraph` nodes. DataFusion is **never called at runtime** — only at
query registration time.

```
SQL Pattern                          OperatorGraph Node
─────────────────────────────────    ────────────────────────────────
SELECT cols FROM source              Passthrough (source → sink)
WHERE predicate                      SqlOperator::Stateless(filter)
SELECT expr AS alias                 SqlOperator::Stateless(project)
WHERE + SELECT (fusible)             SqlOperator::Stateless(filter+project)

GROUP BY keys                        SqlOperator::IncrementalAgg
  with SUM/COUNT/AVG/MIN/MAX           (DataFusion accumulators)

GROUP BY keys,                       SqlOperator::WindowedAgg
  TUMBLE(event_time, interval)         (CoreWindowState + TumblingWindowAssigner)
  EMIT ON WINDOW CLOSE

GROUP BY keys,                       SqlOperator::WindowedAgg
  HOP(event_time, slide, size)         (CoreWindowState + SlidingWindowAssigner)
  EMIT ON WINDOW CLOSE

GROUP BY keys,                       SqlOperator::WindowedAgg
  SESSION(event_time, gap)             (CoreWindowState + session logic)
  EMIT ON WINDOW CLOSE

a JOIN b ON a.key = b.key            OperatorSlot::Core(StreamJoinOperator)
  WITHIN interval                      (existing operator, unchanged)

a ASOF JOIN b                        SqlOperator::AsofJoin
                                       (existing AsofJoinState)

a TEMPORAL JOIN b                    SqlOperator::TemporalJoin
  FOR SYSTEM_TIME                      (existing TemporalJoinState)

Fan-out (multiple queries on         RoutingTable multicast
  same source)                         (clone Event — Arc<RecordBatch> is O(1))

Multi-stage (query depends on        Graph edge: upstream sink → downstream source
  another query's output)               (topological ordering ensures correct order)
```

**Lowering implementation** (~1,500 LOC, new file `crates/laminar-db/src/sql_lowering.rs`):

```rust
/// Lower a DataFusion LogicalPlan to OperatorGraph nodes.
///
/// Called once at query registration time (Ring 2). The resulting graph
/// is frozen and executed repeatedly (Ring 0/1).
pub fn lower_sql_to_graph(
    plan: &LogicalPlan,
    graph: &mut OperatorGraphBuilder,
    source_node: NodeId,
    query_name: &str,
) -> Result<NodeId, LoweringError> {
    match plan {
        // Stateless: fuse adjacent Filter + Projection
        LogicalPlan::Filter { predicate, input } => {
            let upstream = lower_sql_to_graph(input, graph, source_node, query_name)?;
            let node = graph.add_sql_operator(
                query_name,
                SqlOperator::Stateless(StatelessSqlOp::filter(predicate.clone())),
            );
            graph.connect(upstream, node);
            Ok(node)
        }
        // Aggregate: detect window type and create appropriate operator
        LogicalPlan::Aggregate { group_expr, aggr_expr, input } => {
            let upstream = lower_sql_to_graph(input, graph, source_node, query_name)?;
            let (window_config, agg_config) = extract_window_and_agg(group_expr, aggr_expr)?;
            let op = match window_config {
                Some(wc) => SqlOperator::WindowedAgg(
                    CoreWindowState::from_config(&wc, &agg_config)?
                ),
                None => SqlOperator::IncrementalAgg(
                    IncrementalAggState::from_config(&agg_config)?
                ),
            };
            let node = graph.add_sql_operator(query_name, op);
            graph.connect(upstream, node);
            Ok(node)
        }
        // Join: use existing core operator
        LogicalPlan::Join { left, right, on, join_type, .. } => {
            let left_node = lower_sql_to_graph(left, graph, source_node, query_name)?;
            let right_node = lower_sql_to_graph(right, graph, source_node, query_name)?;
            let join_op = StreamJoinOperator::from_plan(on, *join_type)?;
            let node = graph.add_core_operator(query_name, Box::new(join_op));
            graph.connect(left_node, node);
            graph.connect(right_node, node);
            Ok(node)
        }
        // Table scan: return the source node directly
        LogicalPlan::TableScan { table_name, .. } => {
            Ok(graph.resolve_source(table_name)?)
        }
        _ => Err(LoweringError::UnsupportedPlan(format!("{plan:?}"))),
    }
}
```

**Fallback**: If lowering fails for any query, fall back to the existing
`StreamExecutor` path (DataFusion SQL execution). This ensures zero regression
during migration. Queries can be migrated one pattern at a time.

### 3.6 Feature Gate and NUMA Cleanup

**Principle**: Unless it's a hardware/architecture dependency, remove the gate.
No backward compatibility. No over-engineering.

```
DELETE ENTIRELY:
  allocation-tracking    593 LOC   Dev-only, never enabled in prod
  xdp feature + libbpf   ~50 LOC   Feature gate exists, zero implementation
  numa/ module           450 LOC   Over-engineering, never benchmarked,
                                    only relevant with TPC (being removed)

MAKE ALWAYS-ON:
  dag-metrics             ~30 cfg annotations removed
                          5-10ns overhead, critical for observability

KEEP (hardware/architecture):
  io-uring               Linux kernel API — correct gate
  jit                    Cranelift dependency — correct gate
  delta                  Server-only distributed mode — correct gate
```

### 3.7 What About Thread-Per-Core for the Future?

TPC is not wrong in principle — it's wrong for **now**. The path to TPC is:

1. First, build the unified OperatorGraph on a single thread (correct first)
2. Profile real workloads and identify where single-thread becomes the bottleneck
3. If/when bottleneck is CPU-bound operator processing:
   - Partition the OperatorGraph by key ranges (VNode-based)
   - Pin each partition to a core
   - Use SPSC channels between partitions
   - This is what Flink and RisingWave do at scale

But this is a Phase 5+ concern. The current codebase has not reached the
point where single-thread execution is the bottleneck — the bottleneck is
DataFusion SQL evaluation overhead.

### 3.6 What About io_uring?

io_uring is useful for:
- WAL writes (fdatasync without blocking the event loop)
- Checkpoint persistence (large state snapshots)
- NOT for streaming operator I/O (no network I/O between operators)

Recommendation:
- Delete the current io_uring infrastructure (dead code)
- When WAL performance becomes a bottleneck, add io_uring for WAL only
- Use tokio_uring or glommio if/when needed
- This is a Phase 6+ concern

### 3.8 Migration Strategy

No backward compatibility. No feature flags for the rewrite. Delete and replace.

```
Phase A: Delete dead code
  - io_uring/ module         (2,555 LOC)
  - storage_io/ module       (882 LOC)
  - numa/ module             (450 LOC)
  - alloc/detector.rs        (251 LOC)
  - xdp feature + libbpf-rs
  - CreditGate backpressure  (unused acquire path)
  - Make dag-metrics always-on (remove 30 cfg annotations)
  Validation: cargo check, cargo test, cargo clippy

Phase B: Build OperatorGraph
  - Extend DagExecutor with OperatorSlot enum (Core + Sql variants)
  - Move IncrementalAggState/CoreWindowState/EowcState into SqlOperator enum
  - Implement OperatorSlot::process() dispatching to both variants
  - Test: existing DAG tests + new SQL operator tests
  Validation: all operator tests pass through OperatorGraph

Phase C: Build SQL lowering
  - New file: crates/laminar-db/src/sql_lowering.rs
  - Walk DataFusion LogicalPlan → emit OperatorGraph nodes
  - Support: Filter, Project, Aggregate, Window, Join, ASOF, Temporal
  - Fallback: unsupported patterns → existing StreamExecutor
  Validation: SQL integration tests pass through lowered path

Phase D: Build StreamingCoordinator
  - Replace TpcRuntime + TpcPipelineCoordinator with single tokio task
  - Sources push via mpsc channel (no SPSC, no core threads)
  - Graph execution is synchronous within async task
  - Checkpoint barriers flow through graph
  Validation: end-to-end pipeline tests

Phase E: Delete old infrastructure
  - tpc/ module              (4,336 LOC)
  - reactor/ module          (899 LOC)
  - pipeline/ (db)           (1,536 LOC)
  - stream_executor.rs       (3,841 LOC) — replaced by OperatorGraph
  - pipeline_callback.rs
  - pipeline_lifecycle.rs (rewrite to use StreamingCoordinator)
  Validation: cargo check, cargo test, cargo clippy, benchmarks

Phase F: Final audit
  - Confirm single execution path
  - Confirm zero Box<dyn> in hot path (enum dispatch)
  - Confirm no DataFusion ctx.sql() at runtime
  - Benchmark: latency and throughput vs baseline
```

Each phase produces a clean commit. No feature flags needed — we have
zero external users.

---

## LOC Impact Summary

```
DELETE:
  TPC module:              4,336 LOC
  io_uring module:         2,555 LOC
  storage_io module:         882 LOC
  Reactor module:            899 LOC
  numa module:               450 LOC
  alloc/detector.rs:         251 LOC
  Pipeline (db):           1,536 LOC
  StreamExecutor:          3,841 LOC  (replaced by OperatorGraph)
  pipeline_callback.rs:      ~80 LOC
  xdp feature deps:          ~50 LOC
  ──────────────────────────────────
  Total:                  14,880 LOC

SIMPLIFY (remove feature gates):
  dag-metrics:        ~30 cfg annotations removed (LOC neutral)

KEEP:
  DAG module:          7,341 LOC  (evolves into OperatorGraph)
  Operators:          27,240 LOC  (unchanged)
  Compiler:           11,579 LOC  (unchanged, future JIT path)
  Agg/Window state:    5,400 LOC  (moved into OperatorSlot::Sql variants)
  Checkpoint infra:   ~4,000 LOC  (unchanged)
  Connectors:        ~15,000 LOC  (unchanged)
  SQL parsing:       ~20,000 LOC  (unchanged)

NEW:
  OperatorGraph:         ~500 LOC  (extends DagExecutor with OperatorSlot)
  SQL lowering:        ~1,500 LOC  (LogicalPlan → OperatorGraph)
  StreamingCoordinator:  ~500 LOC  (replaces TPC pipeline)
  ──────────────────────────────────
  Total new:           ~2,500 LOC

NET: -12,380 LOC  (14,880 deleted - 2,500 new)
     244,099 → ~231,719  (-5.1% total codebase)
```

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| SQL lowering incomplete — some queries can't be lowered | Keep DataFusion as fallback for unsupported query patterns |
| Performance regression — single thread slower than TPC | TPC was not processing operators; removing it removes 2 thread hops |
| Checkpoint protocol breaks | Reuse existing barrier protocol, test with E2E checkpoint tests |
| Operator trait doesn't fit SQL state types | OperatorNode enum wraps both — no trait change needed |
| Windows dev environment lacks io_uring | io_uring is being deleted, not relied upon |

---

## Appendix: The Smoking Guns

1. **`tpc_runtime.rs:70`**: `Vec::new()` — empty operators passed to CoreHandle
2. **`tpc_coordinator.rs:299`**: `"cores currently run no operators"` — self-documenting
3. **`enable_storage_io: false`** — every single Default impl and test
4. **`CreditGate`** — released but never acquired
5. **`DagExecutor`** — zero callers outside tests/benches
6. **`Reactor::run()`** — never called in production (only `poll_into()` as relay)
