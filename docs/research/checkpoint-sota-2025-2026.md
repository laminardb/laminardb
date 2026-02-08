# Checkpointing in Streaming Databases: State of the Art (2025-2026)

> Research Date: February 2026
> Purpose: Inform LaminarDB's checkpoint architecture (F022, F062, F-CONN-001) with latest industry advances

---

## Table of Contents

1. [Chandy-Lamport and Beyond: Modern Checkpointing Approaches](#1-chandy-lamport-and-beyond)
2. [Multi-Source/Multi-Sink Exactly-Once Coordination](#2-multi-sourcemulti-sink-exactly-once)
3. [Thread-Per-Core Checkpoint Strategies](#3-thread-per-core-checkpoint-strategies)
4. [Embedded/Streaming Database Checkpointing](#4-embeddedstreaming-database-checkpointing)
5. [Performance Benchmarks and Targets](#5-performance-benchmarks-and-targets)
6. [Key Research Papers](#6-key-research-papers-2024-2026)
7. [Implications for LaminarDB](#7-implications-for-laminardb)

---

## 1. Chandy-Lamport and Beyond

### 1.1 Classical Coordinated Checkpointing (Aligned Barriers)

The foundational approach, derived from Chandy-Lamport (1985) and refined by Carbone et al. (2015) as Asynchronous Barrier Snapshotting (ABS), remains the dominant production technique. The core mechanism:

1. A coordinator injects barrier markers into all source streams
2. Barriers flow downstream with data records
3. When an operator receives barriers on ALL inputs, it snapshots its state
4. During alignment, the operator buffers records from channels that have already delivered their barrier while waiting for remaining barriers

Key limitation: Under backpressure, barrier alignment stalls checkpoint progress because barriers queue behind buffered data. Checkpoint duration becomes proportional to end-to-end latency, not state size.

### 1.2 Unaligned Checkpoints (Flink 1.11+, refined through 2.x)

Flink's unaligned checkpoints (FLIP-76) fundamentally change the barrier protocol:

- Barriers overtake in-flight data rather than waiting behind it
- In-flight records (buffered between operators) are persisted as part of the checkpoint state
- Checkpoint duration becomes independent of throughput/backpressure

Technical mechanism:

```
Standard Aligned:
  Operator waits for barriers on ALL inputs
  -> Buffers data from "ahead" channels
  -> Snapshots state when all barriers arrive
  -> Checkpoint duration = f(backpressure)

Unaligned:
  Barrier arrives on ANY input
  -> Immediately snapshots state + all in-flight buffers
  -> Forwards barrier immediately
  -> Persists in-flight data asynchronously
  -> Checkpoint duration = f(state_size), NOT backpressure
```

Trade-offs:
- Larger checkpoint size (includes in-flight data)
- Does NOT support concurrent checkpoints (only one at a time)
- Recovery must replay in-flight data from checkpoint
- In Flink 2.0+, unaligned checkpoints are disabled for sink expansions (committer, pre/post-commit topology) because committables must be at the respective operators on notifyCheckpointComplete

Sources: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpointing_under_backpressure/ and https://cwiki.apache.org/confluence/display/FLINK/FLIP-76:+Unaligned+Checkpoints

### 1.3 Generic Log-Based Incremental Checkpoints (FLIP-158, Flink 1.16+)

This is the most significant checkpoint innovation of the past 3 years. The core insight: decouple checkpoint timing from state backend materialization.

Architecture:

```
State Change -> Apply to State Table (RocksDB/HashMap)
            |-> Append to State Changelog (continuous, async)

Checkpoint trigger:
  - Only flush un-uploaded changelog tail to DFS
  - NO full state snapshot required
  - Background materialization runs independently (default: every 10 minutes)

Recovery:
  - Load last materialized snapshot
  - Replay changelog entries since materialization
```

Key properties:
- Synchronous phase: near-zero (no disk flush needed, data already streaming)
- Asynchronous phase: tiny and stable (only delta since last upload)
- Materialization interval >> checkpoint interval (10 min vs 1-10 sec)
- Long-tail latency dramatically improved (no more compaction stalls)
- Works with ANY state backend (RocksDB, heap-based HashMap)

Performance target: p99 checkpoint duration < 1 second regardless of state size.

Trade-off: Slightly increased remote storage usage (changelog + materialized snapshots overlap), ~1-5% throughput reduction for continuous changelog writes.

Sources: https://cwiki.apache.org/confluence/display/FLINK/FLIP-158:+Generalized+incremental+checkpoints and https://flink.apache.org/2022/05/30/improving-speed-and-stability-of-checkpointing-with-generic-log-based-incremental-checkpoints/

### 1.4 Flink 2.0: Disaggregated State with ForSt (VLDB 2025)

The most important 2025 development. Flink 2.0 introduces ForSt (Flink on RocksDB over Storage), a novel state store that disaggregates computation from state:

Architecture:

```
Compute Node                    Remote DFS (S3/HDFS)
+------------------+           +------------------+
| Async Runtime    |           | Primary State    |
| ForSt (RocksDB)  |  ------>  | (SST Files)      |
| Local Cache      |           | Changelog Files  |
| (configurable)   |           | Checkpoints      |
+------------------+           +------------------+
```

Key innovations:
1. State updates stream continuously to DFS -- no checkpoint-triggered bulk upload
2. Local disks serve as cache only -- state survives node failure without checkpoint
3. Asynchronous runtime -- operators issue state access asynchronously, pipeline multiple requests
4. Unified file system -- single abstraction for local and remote storage

Benchmark results (Nexmark + production workloads):

| Metric | Traditional Flink | Flink 2.0 ForSt | Improvement |
|--------|-------------------|------------------|-------------|
| Checkpoint duration | 10s-60s | 3-4s consistently | Up to 94% reduction |
| Recovery time (200GB state) | Several minutes | ~10 seconds | 40-49x faster |
| Rescaling time | Minutes | < 10 seconds | 40x faster |
| Cost | Baseline | 50% reduction | 2x cheaper |
| Throughput (stateful NexMark) | Baseline | 75-120% | Comparable or better |

Source: https://www.vldb.org/pvldb/vol18/p4846-mei.pdf and https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/

### 1.5 FLIP-306: Unified File Merging for Checkpoints (Flink 1.20+)

Addresses the "file flood" problem where frequent checkpoints create millions of small files:

- Merges scattered checkpoint files (keyed state, non-keyed state, channel state, changelog state) into larger files per Task Manager
- Reduces file creation/deletion by 40% per checkpoint for operator + channel state
- When merging across multiple checkpoints: 88% reduction in file operations
- Enabled via `state.checkpoints.file-merging.enabled = true`

Source: https://cwiki.apache.org/confluence/display/FLINK/FLIP-306:+Unified+File+Merging+Mechanism+for+Checkpoints

### 1.6 Uncoordinated Checkpointing: The CheckMate Findings (ICDE 2024)

The CheckMate paper (TU Delft, ICDE 2024) is the first rigorous comparison of checkpointing protocols for streaming:

Three protocols compared:
1. Coordinated (Flink-style aligned barriers)
2. Uncoordinated (each operator checkpoints independently, logs in-flight messages)
3. Communication-Induced (piggybacked metadata to avoid domino effect)

Results:

| Workload | Best Protocol | Key Finding |
|----------|---------------|-------------|
| Uniform distribution | Coordinated | Lowest latency, fastest recovery |
| Skewed distribution | Uncoordinated | Outperforms coordinated despite message logging |
| Cyclic dataflow | Uncoordinated | Handles cycles naturally (coordinated requires special handling) |

Critical finding: The domino effect (theoretical cascade of rollbacks in uncoordinated checkpointing) never occurred in any experiment. The communication-induced protocol is never competitive due to its metadata overhead.

Conclusion: "Rather than blindly employing coordinated checkpointing, research should focus on optimizing the uncoordinated approach."

Relevance to LaminarDB: For a single-node embedded system, per-core uncoordinated checkpointing may be simpler and equally correct, since there is no network partition risk.

Source: https://arxiv.org/abs/2403.13629

### 1.7 StreamShield: Production Resiliency at ByteDance (2025)

ByteDance's StreamShield (arXiv 2602.03189) introduces three production-proven techniques deployed across 70,000+ concurrent Flink jobs and 11 million+ resource slots:

1. Region Checkpointing: Partitions the job into execution regions bounded by blocking exchanges. A failure in one region does not invalidate the entire checkpoint -- only that region's state is affected.

2. Single-Task Recovery: On failure, restart only the failed region and its dependent operators, not the entire job graph. This dramatically reduces recovery blast radius.

3. State LazyLoad: Decouple job resumption from full state materialization. Operators resume execution with partial states while remaining state is asynchronously restored in the background. This is particularly valuable for large-state jobs where full state restoration takes minutes.

Source: https://arxiv.org/html/2602.03189v1

---

## 2. Multi-Source/Multi-Sink Exactly-Once

### 2.1 The Coordination Problem

End-to-end exactly-once across heterogeneous sources and sinks requires coordinating three independent transaction systems:
1. Source offset tracking (Kafka consumer offsets, CDC replication slots, file positions)
2. Processor state checkpoint (operator state, window buffers, join state)
3. Sink transactional writes (database 2PC, idempotent writes, lakehouse atomic commits)

The fundamental challenge: these three systems have different transaction protocols, failure modes, and durability guarantees.

### 2.2 Kafka: Offset Commit Coordination

Kafka Streams `exactly_once_v2` (Kafka 2.5+):
- Consumer offsets, state store snapshots, and produced records are committed in a single Kafka transaction
- Transaction coordinator manages 2PC protocol
- On failure, roll back to last committed transaction boundary
- Kafka 4.0+ (KRaft mode): Faster transaction commits, reduced coordinator overhead

Flink + Kafka Sink uses `TwoPhaseCommitSinkFunction`:
1. On checkpoint trigger: pre-commit (flush buffered records to Kafka with transactional producer)
2. On checkpoint complete notification: commit Kafka transaction
3. On failure: abort uncommitted transactions, replay from last committed offset

Source: https://www.conduktor.io/glossary/exactly-once-semantics

### 2.3 PostgreSQL: Two-Phase Commit

PostgreSQL natively supports 2PC via PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED:

```sql
BEGIN;
INSERT INTO sink_table VALUES (...);
PREPARE TRANSACTION 'checkpoint_42_sink_pg';
-- Later, after global checkpoint confirmation:
COMMIT PREPARED 'checkpoint_42_sink_pg';
```

Key integration pattern:
1. On checkpoint barrier: execute PREPARE TRANSACTION with checkpoint-specific ID
2. On global checkpoint success: COMMIT PREPARED
3. On checkpoint failure/timeout: ROLLBACK PREPARED
4. On recovery: scan pg_prepared_xacts for orphaned prepared transactions

PostgreSQL logical replication slots (for CDC sources): Slots track confirmed LSN position. On checkpoint, persist the slot's confirmed_flush_lsn. On recovery, replay from that LSN.

Source: https://www.postgresql.org/docs/current/two-phase.html

### 2.4 KIP-939: Kafka as 2PC Participant (Emerging Standard)

KIP-939 is the most important emerging protocol for multi-system exactly-once. It extends Kafka's transaction protocol to allow external systems to participate as 2PC coordinators:

Protocol flow:

```
External Coordinator (e.g., Flink JobManager / LaminarDB Checkpoint Coordinator)
  |
  |-- PREPARE --> Kafka Transaction Coordinator
  |-- PREPARE --> PostgreSQL (PREPARE TRANSACTION)
  |-- PREPARE --> Delta Lake (stage commit)
  |
  |-- (All ACK) --
  |
  |-- COMMIT  --> Kafka (COMMIT_TRANSACTION)
  |-- COMMIT  --> PostgreSQL (COMMIT PREPARED)
  |-- COMMIT  --> Delta Lake (finalize commit)
```

Flink integration: FLIP-319 integrates KIP-939 into Flink's KafkaSink for robust exactly-once between Flink and Kafka. The external coordinator (Flink's JobManager) drives the 2PC, and Kafka participates as one of the resource managers.

Status: KIP-939 is accepted; implementation is in progress. Flink's FLIP-319 integration is planned.

Sources: https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC and https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=255071710

### 2.5 Delta Lake: Transaction Log Atomic Commits

Delta Lake achieves exactly-once through its transaction log (ordered sequence of JSON/Parquet files in `_delta_log/`):

1. Writer reads current table version N
2. Writer stages data files (Parquet) to the table directory
3. Writer attempts to write `_delta_log/N+1.json` atomically (rename/put-if-absent)
4. If conflict: retry with optimistic concurrency control

Checkpoint coordination: The streaming processor writes data files during processing and stages the commit action. On checkpoint completion, it atomically writes the delta log entry. On failure before commit, staged data files are orphans (cleaned up by VACUUM).

Compaction: Delta Lake compacts its transaction log into Parquet checkpoint files every 10 commits by default.

Throughput: Delta Lake supports ~200K events/sec with exactly-once semantics for streaming ingestion.

### 2.6 Iceberg: Manifest-Based Atomic Commits

Apache Iceberg uses snapshot isolation with manifest files:

1. Each snapshot has a manifest list pointing to multiple manifest files
2. Each manifest contains data file paths + column-level statistics
3. Commit = atomic compare-and-swap of the snapshot pointer (catalog-level)
4. Uses optimistic concurrency: read current pointer, stage new metadata, atomic CAS

For exactly-once streaming sinks: Writer accumulates data files across a checkpoint interval, stages a new snapshot with all accumulated manifest entries, and commits atomically on checkpoint completion. Iceberg does not natively support idempotent streaming writes (unlike Delta Lake), requiring additional coordination.

Source: https://www.conduktor.io/glossary/streaming-to-lakehouse-tables

### 2.7 Coordinating ALL Sinks Simultaneously

The production pattern for multi-sink exactly-once:

```
Checkpoint Trigger (barrier arrives at all operators)
  |
  v
Phase 1: PRE-COMMIT (parallel across all sinks)
  |- Kafka: beginTransaction(), produce records
  |- PostgreSQL: BEGIN; INSERT ...; PREPARE TRANSACTION 'ckpt_N_pg'
  |- Delta Lake: stage data files + prepare commit entry
  |- Iceberg: accumulate manifests, prepare snapshot metadata
  |
  v
Phase 2: COORDINATOR CONFIRMS (checkpoint state persisted)
  |
  v
Phase 3: COMMIT (parallel across all sinks)
  |- Kafka: commitTransaction()
  |- PostgreSQL: COMMIT PREPARED 'ckpt_N_pg'
  |- Delta Lake: write _delta_log/N.json (atomic rename)
  |- Iceberg: commit snapshot (atomic CAS)
  |
  v
Phase 4: On failure at ANY point:
  |- Kafka: abortTransaction() or auto-timeout
  |- PostgreSQL: ROLLBACK PREPARED 'ckpt_N_pg'
  |- Delta Lake: staged files become orphans (VACUUM cleans)
  |- Iceberg: snapshot not committed, no visible effect
```

Critical invariant: The checkpoint coordinator must persist the set of prepared transactions before issuing any commits. On recovery, it must complete or abort all prepared transactions from the last checkpoint.

---

## 3. Thread-Per-Core Checkpoint Strategies

### 3.1 ScyllaDB: Per-Shard LSM with Cooperative Snapshots

ScyllaDB (built on Seastar) provides the most mature thread-per-core checkpoint model:

Architecture:
- Each CPU core manages its own memtable, own SSTables, own compaction
- No cross-core data sharing -- each core's partitions are disjoint
- Memtable flush = checkpoint of in-memory state to immutable SSTable
- SSTables are immutable -- snapshots are just hard links (zero-copy, instant)

Snapshot mechanism:

```
Per-core:
  1. Pin current memtable in memory (read-only)
  2. Create new empty memtable for incoming writes
  3. Hard-link all current SSTables
  4. Flush pinned memtable to new SSTable
  5. Result: point-in-time snapshot = set of hard-linked SSTables
```

Key properties:
- No global coordination needed for per-shard snapshots
- No locks -- memtable pin is a pointer swap
- No write amplification from snapshot -- hard links are free
- Compaction is per-core -- no cross-core interference

Source: https://www.scylladb.com/product/technology/shard-per-core-architecture/

### 3.2 Redpanda: Per-Partition Raft Snapshots

Redpanda (also Seastar-based) uses Raft consensus per partition:

Snapshot architecture:
- Each partition has its own Raft log and state machine
- Controller partition snapshots every 60 seconds (or after each command)
- Data partitions use the Raft log itself as the checkpoint mechanism
- Tiered storage (Shadow Indexing): Each partition tracks upload state in a local archival metadata snapshot replicated via Raft

Per-partition checkpoint flow:

```
Leader uploads segment to S3
  -> Adds configuration batch to Raft log (upload metadata)
  -> Followers apply batch, update local archival snapshot
  -> Snapshot contains: segment locations in S3, upload offsets

Leadership transfer:
  -> New leader reads local archival snapshot
  -> Resumes uploading from where predecessor stopped
  -> No need to download manifest from S3
```

Thread-per-core considerations:
- Each core handles multiple partitions
- Snapshot I/O is submitted via Seastar's async I/O (io_uring on Linux)
- Buffer management is per-core with explicit message passing between cores

Sources: https://docs.redpanda.com/current/get-started/architecture/ and https://www.redpanda.com/blog/tiered-storage-architecture-shadow-indexing-deep-dive

### 3.3 Key Patterns for Thread-Per-Core Checkpointing

Synthesizing ScyllaDB, Redpanda, and Seastar patterns:

| Pattern | Description | Benefit |
|---------|-------------|---------|
| Per-core state isolation | Each core owns disjoint state partitions | No locks, no coordination for snapshot |
| Immutable snapshots | Hard-link or CoW for point-in-time capture | Zero hot-path overhead |
| Cooperative scheduling | Checkpoint I/O yields to event processing | Predictable latency budget |
| Message-passing coordination | Global checkpoint = collect per-core snapshots via SMP queues | No shared mutable state |
| Async I/O submission | io_uring for snapshot writes | No blocking on hot path |
| Epoch-based ordering | Global epoch counter to order per-core snapshots | Consistency without barriers |

Anti-patterns to avoid:
- Global locks or mutexes for checkpoint coordination
- Synchronous I/O on any core
- Cross-core state access during snapshot
- Stop-the-world pauses

---

## 4. Embedded/Streaming Database Checkpointing

### 4.1 DuckDB: WAL + Threshold-Based Checkpointing

DuckDB's approach is optimized for analytical workloads:

- WAL-based: All writes go through a write-ahead log
- Threshold-triggered: Checkpoint when WAL reaches checkpoint_threshold (default 16MB)
- Checkpoint = apply WAL to main database file (merge changes into columnar storage)
- DuckDB 1.4.0 (2025): In-memory tables now support checkpointing + compression (5-10x boost for some queries). Checkpointing triggers vacuuming of deleted rows.

Key design choice: DuckDB checkpoints are blocking -- they apply all pending WAL changes to the database file. This is acceptable for OLAP (batch-oriented) but would be unacceptable for a streaming hot path.

Source: https://duckdb.org/docs/stable/sql/statements/checkpoint

### 4.2 Feldera (DBSP): Incremental Computation Checkpointing

Feldera's approach is the most relevant to LaminarDB because of shared design principles (Rust, embedded, incremental, zero-copy):

DBSP Incremental Model:
- Every operator processes changes (Z-sets: weighted multisets with insert/delete/update)
- State is the accumulation of all changes
- Checkpointing captures the accumulated state, not the input stream

Checkpoint architecture:

```
Pipeline Step N:
  - Apply input changes to operator state (in-memory LSM spine)
  - If checkpoint interval reached:
    - commit() -> creates UUID-named checkpoint directory
    - Incrementally persist state:
      - Reuse unchanged portions of previous checkpoint (hard-link or refcount)
      - Write only changed runs/batches
      - Free data from previous checkpoint that is no longer referenced
```

Key technical details:
- Custom LSM-based storage engine (NOT RocksDB -- see below)
- Zero-copy deserialization via rkyv library
- Shared-nothing per-thread: Each thread manages its own data
- Transactional commits: Storage changes are transactional, committed only between pipeline steps
- Recovery: Load most recent checkpoint, replay input from connectors starting at the step number recorded in the checkpoint
- Default interval: Checkpoint every 60 seconds
- Exactly-once output: On recovery, discard output already produced beyond the checkpoint (at-least-once with dedup)

Why Feldera rejected RocksDB:
1. RocksDB doesn't scale beyond a few threads (pipelines performed best limited to 1 thread)
2. Column family creation/destruction is pathologically slow (2-minute test suite became 30 minutes)
3. Background compaction threads compete with dataflow execution
4. RocksDB's durability/atomicity/concurrency features are orthogonal to streaming needs

Feldera built a custom LSM-spine storage engine with:
- Per-thread shared-nothing data management
- Cooperative merge scheduling alongside operator execution
- rkyv zero-copy serialization (matching LaminarDB's choice)
- No background threads -- merges interleaved with processing

Sources: https://www.feldera.com/blog/fault-tolerance-technical-details and https://www.feldera.com/blog/feldera-storage and https://www.feldera.com/blog/rocksdb-not-a-good-choice-for-high-performance-streaming

### 4.3 Materialize: Timely/Differential Dataflow Persistence

Materialize's approach provides an alternative to traditional checkpointing:

- Built on Timely Dataflow (scheduling) + Differential Dataflow (incremental computation)
- State lives in `arrange` operators (custom LSM-like, per-worker, cooperatively scheduled)
- No RocksDB -- same reasoning as Feldera: background compaction competes with dataflow
- Persistence strategy: Store raw input streams (append-only) rather than complex state snapshots
- Recovery = replay input streams through the dataflow graph
- Trade-off: Fast writes, slower recovery for large state

Key insight: "RocksDB is optimized for durability, atomicity, and high read concurrency -- objectives orthogonal to stream processing needs."

Source: https://materialize.com/blog/why-not-rocksdb/

### 4.4 Arroyo: Chandy-Lamport + Parquet on S3

Arroyo (Rust, DataFusion-based, now part of Cloudflare) uses a Chandy-Lamport inspired approach:

Architecture:
- State stored in worker memory with flexible per-operator data structures
- Checkpoint = serialize state as Parquet files to object storage (S3/GCS/ABS)
- Default checkpoint interval: 10 seconds
- Incremental: Each checkpoint writes only changes since last checkpoint

Notable innovation -- mid-file Parquet checkpoint:
- Arroyo can checkpoint in the middle of writing a Parquet file
- Tracks S3 multi-part upload state within the checkpoint
- On recovery, resumes the in-progress multi-part upload
- Other systems (including Flink) cannot do this -- they must finish/abort the file

State access patterns:
- Unlike Flink's generic state backends (Map, List), Arroyo optimizes data structures per operator
- Example: Time-based window eviction uses specialized data structures

Performance: 3x improvement after rebuilding on Arrow/DataFusion (v0.10, 2024). SIMD vectorization, better cache utilization.

Source: https://doc.arroyo.dev/architecture/ and https://www.arroyo.dev/blog/streaming-to-s3-is-hard/

### 4.5 RisingWave: Hummock (Cloud-Native LSM on S3)

RisingWave's Hummock is the most sophisticated cloud-native state store for streaming:

Architecture:

```
Compute Node                    Meta Service
+------------------+           +------------------+
| Stateful Ops     |           | Epoch Generator  |
| Shared Buffer    |   <--->   | Barrier Injector |
| Local Cache      |           | Checkpoint Coord |
+------------------+           +------------------+
        |                              |
        v                              v
+------------------+           +------------------+
| Hummock          |           | Compactor Nodes  |
| (LSM on S3)     |           | (Background)     |
+------------------+           +------------------+
        |                              |
        v                              v
+------------------------------------------+
|              S3 / Object Storage          |
|  (SSTables, metadata, snapshots)         |
+------------------------------------------+
```

Epoch-based checkpoint flow:
1. Meta service generates global epoch periodically
2. Injects barrier into all source nodes
3. Barriers flow through DAG with data
4. On barrier receipt, each operator dumps current epoch's state to Shared Buffer
5. Shared Buffer uploads state to S3, registers with Meta service
6. When all operators for epoch N complete: epoch N is committed
7. New snapshot = MVCC version bound to barrier epoch

Key properties:
- Asynchronous barrier processing: Operator takes current epoch state, resets to empty, continues processing next epoch immediately
- Offloaded compaction: Dedicated compactor nodes pull SSTables from S3, merge, push back -- compute nodes never compact
- Multi-tier caching: In-memory cache -> disk cache -> S3 (queries never hit S3 directly)
- Sub-100ms end-to-end freshness achievable with this architecture

Performance: Checkpoint duration is bounded by upload time, not state size. Recovery time proportional to cache warm-up.

Sources: https://risingwave.com/blog/hummock-a-storage-engine-designed-for-stream-processing/ and https://risingwave.com/blog/sub-100ms-stream-processing-s3-cloud-native/ and https://www.risingwave.com/blog/state-management-for-cloud-native-streaming-getting-to-the-core/

### 4.6 SQLite WAL

For reference, SQLite's WAL mode:
- Readers see consistent snapshot from before WAL
- Writers append to WAL
- Checkpoint = merge WAL changes back into main database
- PRAGMA wal_checkpoint(TRUNCATE) does a full checkpoint
- Blocking operation, but very fast for small databases

Not directly applicable to streaming, but the WAL + periodic merge model is the foundation LaminarDB already uses.

---

## 5. Performance Benchmarks and Targets

### 5.1 Production Checkpoint Performance (2025-2026)

| System | Checkpoint Duration | Recovery Time | State Size | Throughput Impact |
|--------|-------------------|---------------|------------|-------------------|
| Flink 2.0 (ForSt) | 3-4s consistently | ~10s (200GB state) | 200GB+ | < 5% with GIC |
| Flink 1.x (RocksDB incremental) | 10-60s (variable) | Minutes | 200GB+ | 10-30% during checkpoint |
| Flink GIC (changelog) | < 1s (p99 target) | Faster (less replay) | Any | ~1-5% continuous |
| RisingWave (Hummock) | Bounded by upload | Cache warm-up time | Unbounded (S3) | Minimal (async) |
| Arroyo | ~seconds (10s interval) | Replay from last ckpt | Memory-bound | Low (Parquet serialization) |
| Feldera | Per-step commit | Replay from step N | Disk-bound | Low (incremental reuse) |
| ByteDance StreamShield | Improved by region isolation | Partial (State LazyLoad) | 70K+ jobs | Reduced blast radius |

### 5.2 Research Benchmark Results

Ca-Stream (2025, Wiley) -- Adaptive checkpointing:
- 38% reduction in checkpoint consumption time vs Flink baseline
- 33% reduction in system recovery latency
- 47% reduction in CPU occupancy during checkpoint
- 37% reduction in memory occupancy during checkpoint

CheckMate (ICDE 2024) -- Protocol comparison:
- Coordinated: Best latency in uniform workloads, highest checkpoint time
- Uncoordinated: Competitive in uniform, superior in skewed workloads
- Communication-induced: Never competitive (excessive metadata overhead)
- Domino effect: Never observed in practice

Prime Video StreamProcessor (AWS, 2025):
- Checkpoint interval: 70 seconds
- State persisted to S3 Express One Zone
- Recovery: Resume from last saved state

### 5.3 Target Performance Numbers for Embedded Streaming (LaminarDB-class)

Based on the research, competitive targets for an embedded streaming database in 2026:

| Metric | Conservative Target | Aggressive Target | Rationale |
|--------|---------------------|-------------------|-----------|
| Checkpoint duration | < 100ms | < 10ms | No network I/O (embedded), per-core isolation |
| Recovery time | < 5s | < 1s | Local state, no remote fetch |
| Throughput impact | < 5% | < 1% | Changelog-based, no stop-the-world |
| State size | 10GB per core | 100GB per core | Disk-backed with memory-mapped access |
| Checkpoint interval | 10s | 1s | Bounds recovery replay window |
| Hot-path overhead | < 100ns per event | < 50ns per event | Append-only changelog, no sync I/O |

---

## 6. Key Research Papers (2024-2026)

### 6.1 Must-Read Papers

1. "Disaggregated State Management in Apache Flink 2.0" (VLDB 2025)
   - Authors: Yuan Mei, Rui Xia, et al.
   - URL: https://www.vldb.org/pvldb/vol18/p4846-mei.pdf
   - Key: ForSt state store, 94% checkpoint duration reduction, 49x faster recovery

2. "CheckMate: Evaluating Checkpointing Protocols for Streaming Dataflows" (ICDE 2024)
   - Authors: George Siachamis, Kyriakos Psarakis, et al. (TU Delft)
   - URL: https://arxiv.org/abs/2403.13629
   - Key: First rigorous comparison; uncoordinated checkpointing competitive/superior in skewed workloads

3. "Lightweight Asynchronous Snapshots for Distributed Dataflows" (2015, foundational)
   - Authors: Paris Carbone, et al.
   - URL: https://arxiv.org/abs/1506.08603
   - Key: ABS algorithm that Flink implements; persists only operator states on acyclic topologies

4. "StreamShield: A Production-Proven Resiliency Solution for Apache Flink at ByteDance" (2025)
   - Authors: Yong Fang, Yuxing Han, et al.
   - URL: https://arxiv.org/html/2602.03189v1
   - Key: Region checkpointing, single-task recovery, State LazyLoad at 70K+ job scale

5. "DBSP: Automatic Incremental View Maintenance for Rich Query Languages" (VLDB 2023)
   - Authors: Mihai Budiu, Tej Chajed, Frank McSherry, Leonid Ryzhyk, Val Tannen
   - URL: https://docs.feldera.com/vldb23.pdf
   - Key: Z-set mathematical foundation for incremental computation

6. "Enhancing Checkpointing and State Recovery for Large-Scale Stream Processing" (WJARR 2025)
   - URL: https://journalwjarr.com/sites/default/files/fulltext_pdf/WJARR-2025-1638.pdf
   - Key: Adaptive checkpoint intervals, incremental state snapshots, asynchronous commit techniques

7. "Toward High-Availability Distributed Stream Computing Systems via Checkpoint Adaptation" (Ca-Stream, 2025)
   - URL: https://onlinelibrary.wiley.com/doi/10.1002/cpe.70171
   - Key: Multi-factor adaptive checkpoint intervals, 38% reduction in checkpoint time

8. "Asynchronous Latency and Fast Atomic Snapshot" (DISC 2025)
   - URL: https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.DISC.2025.15
   - Key: New latency metric for asynchronous snapshot protocols

### 6.2 Key FLIPs and KIPs

| ID | Title | Status | Relevance |
|----|-------|--------|-----------|
| FLIP-76 | Unaligned Checkpoints | Production (Flink 1.11+) | Backpressure-resistant checkpointing |
| FLIP-151 | Incremental Snapshots for Heap Backend | Production | Copy-on-write for in-memory state |
| FLIP-158 | Generalized Incremental Checkpoints | Production (Flink 1.16+) | Changelog-based checkpointing |
| FLIP-306 | Unified File Merging | Production (Flink 1.20+) | 88% file operation reduction |
| FLIP-319 | Integrate with KIP-939 | Planned | Flink + Kafka proper 2PC |
| KIP-939 | Kafka 2PC Participation | Accepted, in-progress | Cross-system exactly-once |

---

## 7. Implications for LaminarDB

### 7.1 Current LaminarDB Checkpoint Architecture

LaminarDB already implements a three-tier checkpoint model (F022, F062):

```
Ring 0: Changelog Buffer (lock-free, zero-alloc, per-core)
  -> SPSC drain to Ring 1
Ring 1: Per-Core WAL + RocksDB incremental checkpoint
Ring 2: (future) Object storage for disaster recovery
```

### 7.2 Validated Design Choices

The research validates several LaminarDB design decisions:

1. Changelog-based checkpointing (FLIP-158 alignment): LaminarDB's Ring 0 changelog buffer that drains to Ring 1 mirrors Flink's GIC architecture. This is the state-of-the-art for minimizing checkpoint latency.

2. Per-core WAL (F062): Aligns with ScyllaDB and Redpanda's per-shard/per-partition isolation. The CheckMate findings suggest per-core uncoordinated checkpointing is viable and may outperform coordinated approaches for skewed workloads.

3. Z-set changelog model (F063): Matches Feldera's DBSP approach exactly. The incremental computation model naturally produces minimal checkpoint deltas.

4. rkyv zero-copy serialization: Both LaminarDB and Feldera use rkyv. Feldera's experience validates this is the right choice for streaming state serialization.

### 7.3 Gaps and Recommendations

Based on the 2025-2026 research, the following enhancements should be considered:

#### Gap 1: RocksDB Scalability Concern (HIGH PRIORITY)

Feldera and Materialize both explicitly rejected RocksDB for streaming. Key concerns:
- Background compaction threads compete with hot-path execution
- Column family operations are pathologically slow
- Designed for OLTP objectives (durability, atomicity, concurrency) orthogonal to streaming

Recommendation: For production, consider a custom LSM spine (like Feldera) or use RocksDB only for Ring 1 background checkpointing (NOT on any hot path). LaminarDB's current architecture already isolates RocksDB in Ring 1, which is correct. Ensure compaction never interferes with Ring 0. Long-term, evaluate building a custom per-core LSM storage engine with cooperative merge scheduling.

#### Gap 2: Incremental Checkpoint Reuse

Feldera's checkpoint architecture reuses unchanged portions of previous checkpoints via hard-links or reference counting. LaminarDB should implement similar incremental checkpoint reuse:
- Track which state partitions changed since last checkpoint
- Hard-link unchanged SSTables/state files
- Only serialize and write changed partitions

#### Gap 3: State LazyLoad on Recovery (MEDIUM PRIORITY)

ByteDance's StreamShield demonstrates that operators can resume with partial state, loading remaining state asynchronously. For LaminarDB:
- On recovery, load hot state (frequently accessed keys) first
- Serve reads from checkpoint for cold state
- Background-load remaining state progressively

#### Gap 4: Region/Per-Operator Checkpoint Isolation

StreamShield's region checkpointing prevents a slow operator from stalling the entire checkpoint. For LaminarDB:
- Each DAG operator could checkpoint independently
- Global checkpoint = collect individual operator checkpoints
- Failed operator checkpoints don't invalidate the entire checkpoint

#### Gap 5: Adaptive Checkpoint Intervals

Ca-Stream research shows multi-factor adaptive intervals outperform static intervals:
- Factor in: throughput, state change rate, backpressure level, failure probability
- Shorten interval during high throughput (more state to lose)
- Lengthen interval during low activity (reduce overhead)

#### Gap 6: Object Storage Tier (Future)

For disaster recovery and cloud deployment, add S3/object storage as a third tier:
- Follow RisingWave's Hummock pattern: local cache -> disk -> object storage
- Follow Redpanda's Shadow Indexing: per-partition metadata tracking upload state
- Follow Arroyo's Parquet-on-S3: state serialized as Parquet for queryability

### 7.4 Recommended Architecture Evolution

```
Current (2026):
  Ring 0: mmap State -> Changelog Buffer (SPSC) -> Ring 1
  Ring 1: Per-Core WAL -> RocksDB Incremental Checkpoint
  Ring 2: Control plane only

Recommended Next Steps:
  Ring 0: mmap State -> Changelog Buffer (SPSC) -> Ring 1
    [NEW] Per-core epoch counter for uncoordinated snapshots
    [NEW] CoW/hard-link snapshot of mmap state (zero-cost)

  Ring 1: Per-Core WAL -> Custom LSM Spine (evaluate replacing RocksDB)
    [NEW] Incremental checkpoint reuse (hard-link unchanged files)
    [NEW] Background materialization (FLIP-158 pattern)
    [NEW] Adaptive checkpoint interval
    [NEW] State LazyLoad on recovery

  Ring 2: Control plane + Object Storage
    [NEW] S3/GCS tiered storage for checkpoints
    [NEW] Per-partition upload tracking (Redpanda pattern)
    [NEW] Parquet-format state for queryable checkpoints
```

### 7.5 Performance Targets (Validated by Research)

| Metric | LaminarDB Target | Industry Best | Rationale |
|--------|-------------------|---------------|-----------|
| Checkpoint duration | < 50ms | 3-4s (Flink 2.0, 200GB) | Embedded, per-core, no network |
| Recovery time | < 2s | 10s (Flink 2.0, 200GB) | Local state, smaller scale |
| Throughput impact | < 2% | < 5% (Flink GIC) | Changelog-based, no stop-the-world |
| Hot-path overhead | < 50ns/event | N/A (no comparable embedded system) | Append-only changelog |
| Max state per core | 10GB | 200GB (Flink 2.0) | Embedded workloads |
| Checkpoint interval | 1-10s (adaptive) | 10-70s (production systems) | Faster interval feasible at lower scale |

---

## Sources

- Flink Checkpointing Under Backpressure: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpointing_under_backpressure/
- FLIP-76 Unaligned Checkpoints: https://cwiki.apache.org/confluence/display/FLINK/FLIP-76:+Unaligned+Checkpoints
- FLIP-158 Generalized Incremental Checkpoints: https://cwiki.apache.org/confluence/display/FLINK/FLIP-158:+Generalized+incremental+checkpoints
- FLIP-306 Unified File Merging: https://cwiki.apache.org/confluence/display/FLINK/FLIP-306:+Unified+File+Merging+Mechanism+for+Checkpoints
- FLIP-319 Kafka 2PC Integration: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=255071710
- Flink 2.0 Release: https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/
- Flink Blog GIC: https://flink.apache.org/2022/05/30/improving-speed-and-stability-of-checkpointing-with-generic-log-based-incremental-checkpoints/
- Flink 2.0 ForSt VLDB Paper: https://www.vldb.org/pvldb/vol18/p4846-mei.pdf
- Flink Disaggregated State Docs: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/disaggregated_state/
- KIP-939 Kafka 2PC: https://cwiki.apache.org/confluence/display/KAFKA/KIP-939:+Support+Participation+in+2PC
- CheckMate ICDE 2024: https://arxiv.org/abs/2403.13629
- Lightweight Async Snapshots 2015: https://arxiv.org/abs/1506.08603
- StreamShield ByteDance 2025: https://arxiv.org/html/2602.03189v1
- DBSP VLDB 2023: https://docs.feldera.com/vldb23.pdf
- Ca-Stream 2025: https://onlinelibrary.wiley.com/doi/10.1002/cpe.70171
- WJARR 2025 Checkpoint Survey: https://journalwjarr.com/sites/default/files/fulltext_pdf/WJARR-2025-1638.pdf
- DISC 2025 Async Snapshot: https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.DISC.2025.15
- Feldera Fault Tolerance: https://www.feldera.com/blog/fault-tolerance-technical-details
- Feldera Storage Engine: https://www.feldera.com/blog/feldera-storage
- Feldera RocksDB Analysis: https://www.feldera.com/blog/rocksdb-not-a-good-choice-for-high-performance-streaming
- Feldera Checkpoint Sync: https://docs.feldera.com/pipelines/checkpoint-sync/
- Materialize Why Not RocksDB: https://materialize.com/blog/why-not-rocksdb/
- Arroyo Architecture: https://doc.arroyo.dev/architecture/
- Arroyo Streaming to S3: https://www.arroyo.dev/blog/streaming-to-s3-is-hard/
- RisingWave Hummock: https://risingwave.com/blog/hummock-a-storage-engine-designed-for-stream-processing/
- RisingWave Sub-100ms: https://risingwave.com/blog/sub-100ms-stream-processing-s3-cloud-native/
- RisingWave State Management: https://www.risingwave.com/blog/state-management-for-cloud-native-streaming-getting-to-the-core/
- ScyllaDB Shard-per-Core: https://www.scylladb.com/product/technology/shard-per-core-architecture/
- Redpanda Architecture: https://docs.redpanda.com/current/get-started/architecture/
- Redpanda Shadow Indexing: https://www.redpanda.com/blog/tiered-storage-architecture-shadow-indexing-deep-dive
- Redpanda TPC Buffers: https://www.redpanda.com/blog/tpc-buffers
- DuckDB CHECKPOINT: https://duckdb.org/docs/stable/sql/statements/checkpoint
- DuckDB 1.4.0 Release: https://duckdb.org/2025/09/16/announcing-duckdb-140
- PostgreSQL 2PC: https://www.postgresql.org/docs/current/two-phase.html
- Delta Lake Transaction Log: https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html
- Conduktor Streaming to Lakehouse: https://www.conduktor.io/glossary/streaming-to-lakehouse-tables
- Conduktor Exactly-Once: https://www.conduktor.io/glossary/exactly-once-semantics
- Flink End-to-End Exactly-Once: https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/
- AWS Prime Video S3 Express: https://www.amazonaws.cn/en/blog-selection/prime-video-improved-stream-analytics-performance-with-amazon-s3-express-one-zone/
- Redpanda InfoQ Talk: https://www.infoq.com/presentations/high-performance-asynchronous3/
