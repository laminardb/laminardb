# LaminarDB Research Compendium: 2025-2026 Streaming Database Best Practices

**Last Updated:** January 2026
**Purpose:** Reference document for feature specification generation
**Scope:** Streaming databases, real-time processing, embedded systems, financial data

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Streaming Architecture Patterns](#2-streaming-architecture-patterns)
3. [Checkpointing & Fault Tolerance](#3-checkpointing--fault-tolerance)
4. [State Management](#4-state-management)
5. [Latency Optimization](#5-latency-optimization)
6. [DAG & Pipeline Topologies](#6-dag--pipeline-topologies)
7. [Incremental Computation](#7-incremental-computation)
8. [Storage & I/O Patterns](#8-storage--io-patterns)
9. [SQL & Query Processing](#9-sql--query-processing)
10. [Competitive Landscape](#10-competitive-landscape)
11. [Industry Use Cases](#11-industry-use-cases)
12. [Research Papers & References](#12-research-papers--references)

---

## 1. Executive Summary

### 1.1 Key Industry Trends (2025-2026)

| Trend | Description | Implications for LaminarDB |
|-------|-------------|---------------------------|
| **Disaggregated State** | Separation of compute and state storage | Enables elastic scaling, faster recovery |
| **Incremental View Maintenance** | Process only deltas, not full recomputation | Critical for sub-millisecond latency |
| **Unified Batch/Stream** | Single engine for both paradigms | Simplifies architecture, reduces tooling |
| **io_uring Adoption** | Kernel bypass for I/O operations | Essential for sub-microsecond latency |
| **Zero-Copy Networking** | Eliminate memory copies in data path | Linux 6.15 brings io_uring zero-copy RX |
| **Thread-Per-Core** | Avoid context switching overhead | Matches LaminarDB's Phase 2 architecture |

### 1.2 Performance Benchmarks (Industry Standards)

| Metric | Ultra-Low Latency | Low Latency | Standard |
|--------|-------------------|-------------|----------|
| End-to-end latency | <5ms | 5-100ms | >100ms |
| Hot path operations | <1μs | <10μs | <100μs |
| Checkpoint overhead | <5% | <15% | <30% |
| Recovery time (1GB state) | <5s | <30s | <5min |
| Throughput | >1M events/sec | >100K events/sec | >10K events/sec |

### 1.3 LaminarDB Competitive Position

LaminarDB occupies a unique market position:
- **vs kdb+:** Open, affordable, standard SQL (kdb+ is expensive, proprietary q language)
- **vs QuestDB:** Streaming support (QuestDB is analytics-focused, no streaming)
- **vs RisingWave:** Embeddable (RisingWave requires distributed deployment)
- **vs Flink:** Sub-microsecond latency, embedded (Flink is millisecond-range, distributed)

---

## 2. Streaming Architecture Patterns

### 2.1 Actor Model Execution

**Source:** RisingWave, Flink

The actor model provides isolation and message-passing semantics ideal for streaming:

```
┌─────────────────────────────────────────────────────────────┐
│                        Actor Structure                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────┐    ┌──────────────────┐    ┌────────────┐     │
│  │ Merger  │───▶│ Executor Chain   │───▶│ Dispatcher │     │
│  └─────────┘    └──────────────────┘    └────────────┘     │
│       │                   │                    │            │
│  Aligns barriers    Processes data      Routes output      │
│  from upstreams     transformations     to downstreams     │
└─────────────────────────────────────────────────────────────┘
```

**Key Properties:**
- Each actor is single-threaded (no internal parallelism)
- Message passing via channels
- Barriers flow through for checkpoint coordination
- Dispatchers implement routing strategies (hash, round-robin, broadcast)

**RisingWave Implementation:**
- Actors run on tokio async runtime
- Infinite loop consuming messages until stop signal
- Merger handles barrier alignment for multi-input operators

### 2.2 Three-Tier Architecture

**Source:** kdb+ tick architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Feed Handler │───▶│ Tickerplant  │───▶│     RDB      │
└──────────────┘    └──────────────┘    └──────────────┘
                           │                    │
                           │                    ▼
                           │            ┌──────────────┐
                           └───────────▶│     HDB      │
                                        └──────────────┘
```

**Components:**
1. **Feed Handler:** Ingests external data, normalizes format
2. **Tickerplant:** Central hub, logs data, publishes to subscribers
3. **RDB (Real-time DB):** In-memory, current day's data
4. **HDB (Historical DB):** On-disk, partitioned by date

**Key Insights:**
- Tickerplant provides resilience via logging
- Zero-latency mode: immediate publish (no batching)
- Chained tickerplants can aggregate for slower consumers
- End-of-day rollover coordinated by tickerplant

### 2.3 Dataflow Graph Model

**Source:** Flink, DBSP

Programs compile to streaming dataflows:
- **Nodes:** Operators (sources, transforms, sinks)
- **Edges:** Data streams with partitioning
- **Execution:** Parallel subtasks per operator

**Flink's Four-Level Transformation:**
1. Program → StreamGraph (logical)
2. StreamGraph → JobGraph (optimized)
3. JobGraph → ExecutionGraph (parallel)
4. ExecutionGraph → Physical Execution

---

## 3. Checkpointing & Fault Tolerance

### 3.1 Asynchronous Barrier Snapshots

**Source:** Flink (Chandy-Lamport variant), Research papers 2025

**Algorithm:**
1. Coordinator injects barriers at sources
2. Barriers flow through topology with data
3. Operators snapshot state when barrier arrives
4. Multi-input operators align barriers before snapshot
5. Checkpoint complete when all sinks acknowledge

**Barrier Types:**

| Type | Behavior | Use Case |
|------|----------|----------|
| **Aligned** | Wait for all input barriers, buffer data | Standard, consistent |
| **Unaligned** | Snapshot immediately, include in-flight data | Backpressure scenarios |

**Performance (2025 Research):**
- Asynchronous snapshots reduce checkpoint duration by 40-94%
- Causal consistency protocols reduce coordination overhead vs 2PC
- Decouples checkpoint persistence from critical processing path

### 3.2 Incremental Checkpointing

**Source:** Flink RocksDB backend, DBSP

**Full Checkpoint:**
- Entire state captured
- Simple recovery
- High storage/network cost
- Recovery time: O(state_size)

**Incremental Checkpoint:**
- Only deltas since last checkpoint
- Leverages LSM-tree compaction (RocksDB)
- Lower checkpoint time
- Recovery requires base + deltas
- Recovery time: O(num_checkpoints × delta_size)

**Best Practice:**
- Use incremental for large state (>1GB)
- Periodic full checkpoints to bound recovery chain
- Adaptive interval based on workload

### 3.3 Adaptive Checkpointing (Ca-Stream)

**Source:** Wiley 2025 research

**Factors for Dynamic Interval Adjustment:**
1. Predicted fault rate (linear regression)
2. CPU/memory consumption per task
3. Task execution times and data volumes
4. Slow task detection

**Results:**
- 38% reduction in checkpoint consumption time
- 33% reduction in recovery latency
- 47% reduction in CPU occupancy
- 37% reduction in memory occupancy

### 3.4 Disaggregated State (Flink 2.0)

**Source:** VLDB 2025

**Traditional (Flink 1.x):**
- Embedded state backends (heap or RocksDB)
- State co-located with compute
- Checkpoints copy state to remote storage

**Disaggregated (Flink 2.0):**
- Remote state storage as primary
- Local caching for performance
- Faster rescaling (no state redistribution)
- Faster recovery (state already remote)

**Results:**
- Up to 94% reduction in checkpoint duration
- Up to 49× faster recovery
- Up to 50% cost savings

---

## 4. State Management

### 4.1 State Backend Options

| Backend | Storage | Latency | Capacity | Use Case |
|---------|---------|---------|----------|----------|
| **Heap/HashMap** | JVM Heap | <1μs | Limited by RAM | Small state, lowest latency |
| **RocksDB** | Off-heap + disk | ~10μs | Disk-limited | Large state, spill to disk |
| **Memory-mapped** | mmap files | <1μs | RAM + swap | LaminarDB approach |
| **Disaggregated** | Remote storage | ~1ms | Unlimited | Cloud-native, elastic |

### 4.2 State Partitioning

**Keyed State:**
- Partitioned by key (e.g., symbol, user_id)
- Each partition processed by one task
- Enables parallel processing
- State locality with data

**Operator State:**
- Tied to operator instance
- Used for sources (offsets), sinks (buffers)
- Redistributed on rescaling

### 4.3 State Access Patterns

**Hot Path Requirements:**
- Sub-microsecond reads/writes
- No allocation in critical path
- Pre-allocated buffers
- Memory-mapped for zero-copy

**Background Operations:**
- Compaction (LSM-tree)
- Checkpointing
- Garbage collection
- Can tolerate millisecond latency

---

## 5. Latency Optimization

### 5.1 Latency Hierarchy

**Source:** StreamNative 2025

```
Operation                          Latency
─────────────────────────────────────────────
CPU L1 cache reference             ~1 ns
CPU L2 cache reference             ~4 ns
Main memory reference              ~100 ns
NVMe SSD read                      ~10-100 μs
SSD fsync                          ~150 μs
Network (same datacenter)          ~500 μs
HDD seek                           ~10 ms
Network (cross-region)             ~50-150 ms
```

### 5.2 Sub-Microsecond Techniques

| Technique | Benefit | Implementation |
|-----------|---------|----------------|
| **Zero allocation** | No GC pauses | Pre-allocated ring buffers |
| **Memory-mapped I/O** | Kernel bypass for state | mmap with MAP_POPULATE |
| **io_uring** | Async I/O without syscalls | Submission/completion queues |
| **Zero-copy networking** | Eliminate memcpy | io_uring ZC RX (Linux 6.15) |
| **Thread-per-core** | No context switching | Pin threads to CPUs |
| **NUMA awareness** | Local memory access | Allocate on local node |
| **Lock-free structures** | No mutex contention | Atomic operations |
| **Branch prediction** | CPU pipeline efficiency | Likely/unlikely hints |

### 5.3 io_uring Zero-Copy (Linux 6.15)

**Source:** Phoronix, Kernel documentation

**Traditional Path:**
```
NIC → Kernel buffer → User buffer → Application
      (copy 1)        (copy 2)
```

**Zero-Copy RX:**
```
NIC → User memory (directly)
      (zero copies)
```

**Results:**
- 200Gbps link saturated from single CPU core
- GPU memory support planned
- Header/data splitting for kernel stack compatibility

### 5.4 Hot Path Budget

For LaminarDB's <500ns target:

| Component | Budget | Notes |
|-----------|--------|-------|
| Message receive | 50ns | Lock-free queue dequeue |
| Routing decision | 50ns | Pre-computed table lookup |
| Operator execution | 200ns | Depends on operation |
| State access | 150ns | Memory-mapped read |
| Message send | 50ns | Lock-free queue enqueue |
| **Total** | **500ns** | |

---

## 6. DAG & Pipeline Topologies

### 6.1 Topology Types

**Linear Pipeline:**
```
Source → Op1 → Op2 → Op3 → Sink
```

**Fan-Out (Broadcast/Split):**
```
              ┌→ Op2a → Sink1
Source → Op1 ─┼→ Op2b → Sink2
              └→ Op2c → Sink3
```

**Fan-In (Merge/Union):**
```
Source1 → Op1a ─┐
Source2 → Op1b ─┼→ Op2 → Sink
Source3 → Op1c ─┘
```

**Diamond (Shared Intermediate):**
```
              ┌→ Op2a ─┐
Source → Op1 ─┤        ├→ Op3 → Sink
              └→ Op2b ─┘
```

**Complex DAG:**
```
Source1 ─┬→ Dedup ─┬→ Agg1 ──────┬→ Join → Sink1
         │         │              │
Source2 ─┘         └→ Filter ─┬→ ┘
                              │
                              └→ Alert → Sink2
```

### 6.2 Channel Types for DAG Edges

| Topology | Channel Type | Characteristics |
|----------|--------------|-----------------|
| Linear (1:1) | SPSC | Fastest, no synchronization |
| Fan-out (1:N) | SPMC | Single writer, multiple readers |
| Fan-in (N:1) | MPSC | Multiple writers, single reader |
| Shared (N:M) | MPMC | Full synchronization |

### 6.3 Partitioning Strategies

| Strategy | Use Case | Data Distribution |
|----------|----------|-------------------|
| **Forward** | Same parallelism | 1:1 mapping |
| **Hash** | Keyed operations | By key hash |
| **Broadcast** | Small reference data | To all partitions |
| **Round-robin** | Load balancing | Even distribution |
| **Custom** | Domain-specific | User-defined function |

### 6.4 Shared Stage Implementation

**Requirements:**
1. Write once, read by multiple consumers
2. Reference counting for memory management
3. Backpressure from slowest consumer
4. Zero-copy multicast

**Pattern:**
```rust
struct MulticastBuffer<T> {
    slots: RingBuffer<T>,
    write_pos: AtomicU64,
    read_positions: Vec<AtomicU64>,  // Per consumer
    refcounts: Vec<AtomicU32>,       // Per slot
}
```

---

## 7. Incremental Computation

### 7.1 DBSP Framework

**Source:** Feldera, VLDB 2023/2025

**Core Concept:**
- Streams are sequences of values over time
- Operators transform streams
- Integration (∫) accumulates values
- Differentiation (D) computes deltas

**Key Insight:**
Any computation can be incrementalized by:
1. Express as composition of operators
2. Replace each operator with incremental version
3. Cost proportional to delta size, not total state

### 7.2 Incremental Operators

| Operator | Batch Cost | Incremental Cost |
|----------|------------|------------------|
| Filter | O(n) | O(Δn) |
| Map | O(n) | O(Δn) |
| Join | O(n × m) | O(Δn × m + n × Δm) |
| Aggregate | O(n) | O(Δn) |
| Distinct | O(n) | O(Δn) |

### 7.3 Z-Sets (Change Representation)

**Definition:** Multiset with integer multiplicities
- Positive: insertions
- Negative: deletions
- Zero: no change

**Example:**
```
Initial: {(A, 1), (B, 1), (C, 1)}
Delta:   {(A, -1), (D, 1)}         // Delete A, Insert D
Result:  {(B, 1), (C, 1), (D, 1)}
```

### 7.4 Recursive Queries

DBSP supports recursive queries (e.g., transitive closure):
- Fixed-point iteration
- Incremental updates to recursive results
- Bounded iterations per input delta

---

## 8. Storage & I/O Patterns

### 8.1 Log-Structured Storage

**Pattern:** Append-only writes, periodic compaction

**Advantages:**
- Sequential writes (fast on all storage)
- Natural audit trail
- Easy replication

**Disadvantages:**
- Read amplification
- Space amplification
- Compaction overhead

### 8.2 Memory-Mapped State

**LaminarDB Approach:**
```rust
// Memory-mapped state store
let mmap = MmapMut::map_mut(&file)?;
// Direct memory access, no syscalls
let value = &mmap[offset..offset + len];
```

**Benefits:**
- Sub-microsecond access
- Kernel handles paging
- Transparent persistence

**Considerations:**
- Page faults can cause latency spikes
- Use MAP_POPULATE for hot data
- NUMA-aware allocation

### 8.3 Tiered Storage

| Tier | Medium | Latency | Use Case |
|------|--------|---------|----------|
| **L0** | CPU cache | <10ns | Hot keys |
| **L1** | RAM/mmap | <1μs | Active state |
| **L2** | NVMe SSD | <100μs | Warm state |
| **L3** | Object storage | <100ms | Checkpoints, cold data |

### 8.4 Write-Ahead Logging

**Purpose:** Durability before state update

**kdb+ Tickerplant Pattern:**
1. Receive message
2. Write to log file
3. Update in-memory state
4. Publish to subscribers

**Recovery:**
1. Load last checkpoint
2. Replay log from checkpoint position
3. Resume processing

---

## 9. SQL & Query Processing

### 9.1 Streaming SQL Extensions

| Extension | Purpose | Example |
|-----------|---------|---------|
| **TUMBLE** | Fixed windows | `TUMBLE(ts, INTERVAL '1' MINUTE)` |
| **HOP** | Sliding windows | `HOP(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)` |
| **SESSION** | Gap-based windows | `SESSION(ts, INTERVAL '5' MINUTE)` |
| **EMIT** | Output control | `EMIT AFTER WATERMARK` |
| **MATCH_RECOGNIZE** | Pattern matching | CEP patterns |

### 9.2 Materialized Views

**Traditional (PostgreSQL):**
- Manual refresh
- Full recomputation
- Stale between refreshes

**Streaming (RisingWave/Materialize):**
- Continuous, automatic updates
- Incremental maintenance
- Always fresh

**Cascading Views:**
```sql
CREATE MATERIALIZED VIEW mv1 AS SELECT ... FROM source;
CREATE MATERIALIZED VIEW mv2 AS SELECT ... FROM mv1;
CREATE MATERIALIZED VIEW mv3 AS SELECT ... FROM mv1, mv2;
```

### 9.3 Query Optimization for Streaming

| Optimization | Benefit |
|--------------|---------|
| **Predicate pushdown** | Filter early, reduce data volume |
| **Projection pushdown** | Select only needed columns |
| **Join reordering** | Smallest tables first |
| **Incremental aggregation** | Partial aggregates per partition |
| **State sharing** | Reuse common subexpressions |

---

## 10. Competitive Landscape

### 10.1 Detailed Comparison

| Feature | LaminarDB | kdb+ | QuestDB | RisingWave | Flink | Materialize |
|---------|-----------|------|---------|------------|-------|-------------|
| **Deployment** | Embedded | Embedded | Server | Distributed | Distributed | Cloud |
| **Latency** | <1μs | <1μs | ~1ms | ~10ms | ~100ms | ~10ms |
| **Query Language** | SQL | q | SQL | SQL | SQL/Java | SQL |
| **Streaming** | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| **Incremental** | ✅ | Partial | ❌ | ✅ | ✅ | ✅ |
| **Open Source** | ✅ | ❌ | ✅ | ✅ | ✅ | Partial |
| **Checkpointing** | ✅ | Manual | N/A | ✅ | ✅ | ✅ |
| **DAG Support** | Planned | Limited | ❌ | ✅ | ✅ | ✅ |

### 10.2 Positioning Summary

**LaminarDB's Unique Value:**
1. **Embedded + Low Latency + SQL + Streaming** - No other solution offers all four
2. **Thread-per-core + NUMA** - Modern hardware utilization
3. **Three-ring architecture** - Clean separation of concerns
4. **Accessible pricing** - Unlike kdb+ ($100K+ licenses)

---

## 11. Industry Use Cases

### 11.1 Financial Trading

**Requirements:**
- Sub-millisecond latency
- High throughput (millions of events/sec)
- Complex event processing
- Audit trail

**Patterns:**
- VWAP calculation
- Order book reconstruction
- Anomaly detection
- Position tracking

### 11.2 Real-Time Analytics

**Requirements:**
- Fresh dashboards
- Ad-hoc queries
- Historical + real-time

**Patterns:**
- Continuous aggregation
- Time-series analysis
- Sessionization

### 11.3 IoT / Telemetry

**Requirements:**
- High cardinality
- Time-series storage
- Edge deployment

**Patterns:**
- Downsampling
- Alerting
- Predictive maintenance

---

## 12. Research Papers & References

### 12.1 Foundational Papers

1. **Chandy-Lamport Algorithm** (1985)
   - Distributed snapshots
   - Basis for Flink checkpointing

2. **DBSP: Automatic Incremental View Maintenance** (VLDB 2023)
   - Budiu, Chajed, McSherry, Ryzhyk, Tannen
   - Formal framework for incremental computation

3. **Disaggregated State Management in Apache Flink 2.0** (VLDB 2025)
   - Mei, Xia, Lan, Hu, et al.
   - 94% checkpoint reduction, 49× faster recovery

### 12.2 Industry Resources

1. **StreamNative: Latency Numbers Every Streaming Engineer Should Know** (2025)
   - Comprehensive latency reference

2. **kdb+ Architecture Documentation**
   - Tick architecture patterns
   - Real-time engine design

3. **RisingWave Developer Guide**
   - Actor model implementation
   - Materialized view consistency

### 12.3 Kernel & Systems

1. **io_uring Zero-Copy RX** (Linux 6.15, 2025)
   - 200Gbps from single core
   - Direct NIC to userspace

2. **NUMA-Aware Memory Allocation**
   - Linux numactl, libnuma
   - Performance implications

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Actor** | Isolated execution unit with message passing |
| **Barrier** | Marker in stream for checkpoint coordination |
| **Backpressure** | Slow consumer causing upstream to slow down |
| **CDC** | Change Data Capture |
| **CEP** | Complex Event Processing |
| **DAG** | Directed Acyclic Graph |
| **Delta** | Change since last state |
| **Fan-in** | Multiple inputs to one operator |
| **Fan-out** | One output to multiple operators |
| **HDB** | Historical Database |
| **IVM** | Incremental View Maintenance |
| **LSM** | Log-Structured Merge tree |
| **NUMA** | Non-Uniform Memory Access |
| **RDB** | Real-time Database |
| **SPSC/MPSC/SPMC/MPMC** | Channel types (Single/Multi Producer/Consumer) |
| **Watermark** | Progress indicator for event time |
| **Z-Set** | Multiset with multiplicities for changes |

---

## Appendix B: Configuration Recommendations

### B.1 Checkpoint Configuration

```yaml
checkpointing:
  interval: 30s           # Adaptive based on workload
  mode: incremental       # For state > 1GB
  timeout: 10m            # Max checkpoint duration
  min_pause: 5s           # Between checkpoints
  max_concurrent: 1       # Parallel checkpoints
  storage:
    type: s3              # Or local, rocksdb
    path: s3://bucket/checkpoints
```

### B.2 State Backend Configuration

```yaml
state:
  backend: memory_mapped  # LaminarDB default
  path: /var/lib/laminardb/state
  max_size: 10GB
  checkpoint_interval: 30s
  compaction:
    enabled: true
    interval: 5m
```

### B.3 Performance Tuning

```yaml
performance:
  thread_per_core: true
  numa_aware: true
  io_uring:
    enabled: true
    sq_entries: 4096
    cq_entries: 8192
  channels:
    buffer_size: 65536
    batch_size: 1024
```

---

*This document should be updated as new research emerges and as LaminarDB's implementation evolves.*
