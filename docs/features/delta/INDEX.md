# Delta Architecture — Feature Index

## Overview

The Delta Architecture extends LaminarDB from an embedded single-process streaming engine to a multi-node distributed system without changing operator code. Three deployment tiers share the same SQL, the same operators, the same state traits — only the infrastructure configuration changes.

**Design Philosophy:**
- **Ring 0 is sacred.** The hot path (sub-500ns per operation) uses only synchronous, zero-allocation, zero-lock data structures.
- **Location-transparent state.** Operator code calls `state.get(key)` regardless of deployment tier.
- **Object storage is the single source of truth.** Every node is ephemeral. Durable state lives in S3/GCS/local filesystem via the `object_store` crate.

**Deployment Tiers:**
- **Tier 1 — Embedded Single-Process:** Library linked into your application
- **Tier 2 — Multi-Partition Single Machine:** Thread-per-core, NUMA-aware
- **Tier 3 — Delta:** Multiple nodes, gossip discovery, Raft coordination

**Naming Conventions:**

| Concept | Name | Rationale |
|---------|------|-----------|
| Single node | Star | Self-sufficient unit |
| Multi-node group | Delta | Peers forming recognizable pattern |
| Node discovery | Starfinding | Discovery of peers |
| Partition map | Star Chart | Map of partition → node assignments |
| Node joining | Rising | Astronomical term for appearance |
| Node leaving | Setting | Astronomical term for departure |
| Multi-region (future) | Galaxy | Contains multiple deltas |

## Feature Summary

| Phase | Total | Draft | Superseded | In Progress | Done |
|-------|-------|-------|------------|-------------|------|
| Phase 6a — Partition-Parallel Embedded | 29 | 0 | 2 | 0 | 27 |
| Phase 6b — Delta Foundation | 14 | 14 | 0 | 0 | 0 |
| Phase 6c — Production Hardening | 10 | 9 | 1 | 0 | 0 |
| Phase 7a — Platform Cleanup | 2 | 0 | 0 | 0 | 2 |
| Phase 7b — Cloud-Native Storage | 4 | 0 | 0 | 0 | 4 |
| Phase 7c — Distributed Operations | 4 | 0 | 0 | 0 | 4 |
| **Total** | **63** | **21** | **3** | **0** | **37** |

## Key Dependencies

See [DEPENDENCIES.md](DEPENDENCIES.md) for the full project dependency graph.

**Delta prerequisites (from earlier phases):**
- F-STREAM-002 (SPSC Channel) — barrier protocol uses same channels
- F013 (Thread-Per-Core) — cross-partition aggregation
- F006B (Production SQL Parser) — CREATE LOOKUP TABLE DDL
- F005B (Advanced DataFusion) — lookup join plan nodes
- F025/F026 (Kafka Source/Sink) — Kafka discovery, transactional sinks
- F027 (PostgreSQL CDC) — CDC-to-cache adapter
- F-CKP-001–009 (Unified Checkpoint) — foundation for distributed checkpointing

---

## Phase 6a: Partition-Parallel Embedded

> **Goal**: State store infrastructure, checkpoint barriers, lookup tables with foyer caching, source pushdown, SQL integration, and baseline benchmarks. Single-process multi-partition with object store checkpoints.

### State Store Architecture

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-STATE-001 | Revised StateStore Trait | ✅ | P0 | M | [Link](state/F-STATE-001-state-store-trait.md) |
| F-STATE-002 | InMemoryStateStore (AHashMap/BTreeSet) | ✅ | P0 | S | [Link](state/F-STATE-002-inmemory-state-store.md) |
| F-STATE-003 | ~~MmapStateStore~~ | ❌ | — | — | [Link](state/F-STATE-003-mmap-state-store.md) |
| F-STATE-004 | ~~Pluggable Snapshot Strategies~~ | ❌ | — | — | [Link](state/F-STATE-004-pluggable-snapshots.md) |

### Distributed Checkpoint (Embedded)

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-DCKP-001 | Checkpoint Barrier Protocol | ✅ | P0 | M | [Link](checkpoint/F-DCKP-001-barrier-protocol.md) |
| F-DCKP-002 | Barrier Alignment | ✅ | P0 | M | [Link](checkpoint/F-DCKP-002-barrier-alignment.md) |
| F-DCKP-003 | Object Store Checkpoint Layout | ✅ | P0 | S | [Link](checkpoint/F-DCKP-003-object-store-layout.md) |
| F-DCKP-004 | ObjectStoreCheckpointer | ✅ | P0 | M | [Link](checkpoint/F-DCKP-004-object-store-checkpointer.md) |
| F-DCKP-005 | Recovery Manager | ✅ | P0 | L | [Link](checkpoint/F-DCKP-005-recovery-manager.md) |

### Lookup Tables

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-LOOKUP-001 | LookupTable Trait & Strategy | ✅ | P0 | M | [Link](lookup/F-LOOKUP-001-lookup-table-trait.md) |
| F-LOOKUP-002 | LookupSource Trait | ✅ | P0 | M | [Link](lookup/F-LOOKUP-002-lookup-source-trait.md) |
| F-LOOKUP-003 | Predicate Types | ✅ | P0 | S | [Link](lookup/F-LOOKUP-003-predicate-types.md) |
| F-LOOKUP-004 | foyer In-Memory Cache (Ring 0) | ✅ | P0 | M | [Link](lookup/F-LOOKUP-004-foyer-memory-cache.md) |
| F-LOOKUP-005 | foyer Hybrid Cache (Ring 1) | ✅ | P1 | M | [Link](lookup/F-LOOKUP-005-foyer-hybrid-cache.md) |
| F-LOOKUP-006 | CDC-to-Cache Adapter | ✅ | P0 | S | [Link](lookup/F-LOOKUP-006-cdc-cache-adapter.md) |
| F-LOOKUP-007 | PostgresLookupSource | ✅ | P1 | M | [Link](lookup/F-LOOKUP-007-postgres-lookup-source.md) |
| F-LOOKUP-008 | ParquetLookupSource | ✅ | P2 | M | [Link](lookup/F-LOOKUP-008-parquet-lookup-source.md) |
| F-LOOKUP-010 | Remove RocksDB Dependency | ✅ | P0 | S | [Link](lookup/F-LOOKUP-010-rocksdb-removal.md) |

### Secondary Indexes

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-IDX-001 | redb Secondary Indexes | ✅ | P1 | M | [Link](indexes/F-IDX-001-redb-secondary-indexes.md) |

### Deployment Profiles

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-PROFILE-001 | Deployment Profiles | ✅ | P0 | M | [Link](profiles/F-PROFILE-001-deployment-profiles.md) |

### Cross-Partition Aggregation

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-XAGG-001 | Cross-Partition Lock-Free HashMap | ✅ | P1 | M | [Link](aggregation/F-XAGG-001-cross-partition-hashmap.md) |

### Exactly-Once (Source Layer)

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-E2E-001 | Source Offset Checkpoint | ✅ | P0 | M | [Link](exactly-once/F-E2E-001-source-offset-checkpoint.md) |

### SQL Integration

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-LSQL-001 | CREATE LOOKUP TABLE DDL | ✅ | P0 | M | [Link](sql/F-LSQL-001-create-lookup-table.md) |
| F-LSQL-002 | Lookup Join Plan Node | ✅ | P0 | L | [Link](sql/F-LSQL-002-lookup-join-plan-node.md) |
| F-LSQL-003 | Predicate Splitting & Pushdown | ✅ | P1 | M | [Link](sql/F-LSQL-003-predicate-splitting.md) |

### Performance Benchmarks (Baseline)

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-PERF-001 | StateStore Microbenchmarks | ✅ | P0 | S | [Link](benchmarks/F-PERF-001-state-store-benchmarks.md) |
| F-PERF-002 | Cache Hit/Miss Ratio Benchmarks | ✅ | P1 | S | [Link](benchmarks/F-PERF-002-cache-benchmarks.md) |
| F-PERF-003 | Checkpoint Cycle Benchmark | ✅ | P1 | S | [Link](benchmarks/F-PERF-003-checkpoint-cycle-benchmark.md) |
| F-PERF-005 | Lookup Join Throughput | ✅ | P1 | S | [Link](benchmarks/F-PERF-005-lookup-join-throughput.md) |

---

## Phase 6b: Delta Foundation

> **Goal**: Multi-node discovery (gossip), Raft-based metadata consensus, partition ownership with epoch fencing, distributed checkpointing, partitioned lookups, cross-node aggregation, inter-node RPC.

### Discovery

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-DISC-001 | Discovery Trait & Static Discovery | 📝 | P0 | M | [Link](discovery/F-DISC-001-static-discovery.md) |
| F-DISC-002 | Gossip Discovery (chitchat) | 📝 | P0 | L | [Link](discovery/F-DISC-002-gossip-discovery.md) |
| F-DISC-003 | Kafka Group Discovery | 📝 | P2 | M | [Link](discovery/F-DISC-003-kafka-discovery.md) |

### Coordination

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-COORD-001 | Raft Metadata Consensus | 📝 | P0 | XL | [Link](coordination/F-COORD-001-raft-metadata.md) |
| F-COORD-002 | Delta Orchestration | 📝 | P0 | L | [Link](coordination/F-COORD-002-delta-orchestration.md) |

### Partition Ownership

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-EPOCH-001 | PartitionGuard & Epoch Fencing | 📝 | P0 | M | [Link](partition/F-EPOCH-001-partition-guard.md) |
| F-EPOCH-002 | Partition Assignment Algorithm | 📝 | P1 | M | [Link](partition/F-EPOCH-002-assignment-algorithm.md) |
| F-EPOCH-003 | Partition Reassignment Protocol | 📝 | P0 | L | [Link](partition/F-EPOCH-003-reassignment-protocol.md) |

### Distributed Checkpoint (Delta)

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-DCKP-008 | Distributed Checkpoint Coordination | 📝 | P0 | XL | [Link](checkpoint/F-DCKP-008-distributed-coordination.md) |

### Delta Lookup

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-LOOKUP-009 | Partitioned Lookup Strategy | 📝 | P1 | L | [Link](lookup/F-LOOKUP-009-partitioned-lookup.md) |

### Cross-Node Aggregation

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-XAGG-002 | Gossip Partial Aggregates | 📝 | P1 | M | [Link](aggregation/F-XAGG-002-gossip-aggregates.md) |
| F-XAGG-003 | gRPC Aggregate Fan-Out | 📝 | P2 | M | [Link](aggregation/F-XAGG-003-grpc-aggregate-fanout.md) |

### Inter-Node RPC

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-RPC-001 | gRPC Service Definitions | 📝 | P0 | M | [Link](rpc/F-RPC-001-grpc-service-definitions.md) |
| F-RPC-002 | Remote Lookup Service | 📝 | P1 | M | [Link](rpc/F-RPC-002-remote-lookup-service.md) |
| F-RPC-003 | Barrier Forwarding Service | 📝 | P0 | M | [Link](rpc/F-RPC-003-barrier-forwarding.md) |

### Performance Benchmarks (Delta)

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-PERF-004 | Delta Checkpoint Benchmark | 📝 | P1 | S | [Link](benchmarks/F-PERF-004-delta-checkpoint-benchmark.md) |

---

## Phase 6c: Production Hardening

> **Goal**: Unaligned checkpoints, exactly-once sinks, laminardb-server binary with TOML config and HTTP API, hot reload, rolling restarts.

### Advanced Checkpointing

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-DCKP-006 | Unaligned Checkpoints | 📝 | P1 | L | [Link](checkpoint/F-DCKP-006-unaligned-checkpoints.md) |
| F-DCKP-007 | ~~Incremental Mmap Checkpoints~~ | ❌ | — | — | [Link](checkpoint/F-DCKP-007-incremental-mmap-checkpoints.md) |

### Exactly-Once Sinks

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-E2E-002 | Transactional Sink (2PC) | 📝 | P0 | L | [Link](exactly-once/F-E2E-002-transactional-sink.md) |
| F-E2E-003 | Idempotent Sink | 📝 | P1 | M | [Link](exactly-once/F-E2E-003-idempotent-sink.md) |

### laminardb-server

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-SERVER-001 | TOML Configuration | 📝 | P0 | M | [Link](server/F-SERVER-001-toml-config.md) |
| F-SERVER-002 | Engine Construction | 📝 | P0 | M | [Link](server/F-SERVER-002-engine-construction.md) |
| F-SERVER-003 | HTTP API | 📝 | P0 | M | [Link](server/F-SERVER-003-http-api.md) |
| F-SERVER-004 | Hot Reload | 📝 | P1 | M | [Link](server/F-SERVER-004-hot-reload.md) |
| F-SERVER-005 | Delta Server Mode | 📝 | P1 | M | [Link](server/F-SERVER-005-delta-mode.md) |
| F-SERVER-006 | Graceful Rolling Restart | 📝 | P2 | L | [Link](server/F-SERVER-006-rolling-restart.md) |

---

## Phase 7a: Platform Cleanup

> **Goal**: Remove feature gate complexity and simplify config model. Unblock all downstream cloud-native work.

### Feature Gate Removal

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-GATE-001 | Feature Gate Removal | ✅ | P0 | M | [Link](cloud/F-GATE-001-feature-gate-removal.md) |

### Config Model

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-CFG-001 | Orthogonal Config Model | ✅ | P0 | M | [Link](cloud/F-CFG-001-orthogonal-config.md) |

---

## Phase 7b: Cloud-Native Storage

> **Goal**: Production-ready S3 integration with crash safety, cost-optimized batching, and storage class tiering. Follows industry consensus: S3 as durable source of truth, never read from S3 on query path.

### S3 Object Layout & Cost Optimization

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-S3-001 | S3 Object Layout | ✅ | P0 | L | [Link](cloud/F-S3-001-object-layout.md) |
| F-S3-002 | Checkpoint Batching | ✅ | P1 | M | [Link](cloud/F-S3-002-checkpoint-batching.md) |
| F-S3-003 | Storage Class Tiering | ✅ | P2 | M | [Link](cloud/F-S3-003-storage-class-tiering.md) |

### Crash Safety

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-CRASH-001 | Checkpoint Validation & Crash Recovery | ✅ | P0 | L | [Link](cloud/F-CRASH-001-checkpoint-validation.md) |

---

## Phase 7c: Distributed Operations

> **Goal**: Disaggregated state on S3, correct cross-partition aggregation, safe partition migration, and self-tuning checkpoints.

### Disaggregated State

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-DISAGG-001 | Disaggregated State Backend | ✅ | P1 | XL | [Link](cloud/F-DISAGG-001-disaggregated-state.md) |

### Cross-Partition Aggregation

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-XAGG-004 | Two-Phase Aggregation | ✅ | P1 | L | [Link](cloud/F-XAGG-004-two-phase-aggregation.md) |

### Partition Migration

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-MIGRATE-001 | Partition Migration Protocol | ✅ | P1 | L | [Link](cloud/F-MIGRATE-001-partition-migration.md) |

### Adaptive Checkpointing

| ID | Feature | Status | Priority | Effort | Spec |
|----|---------|--------|----------|--------|------|
| F-DCKP-009 | Adaptive Checkpoint Interval | ✅ | P2 | M | [Link](cloud/F-DCKP-009-adaptive-checkpoint.md) |

---

## Performance Budget

### Ring 0 (Hot Path) — < 500ns per event

| Operation | Target | Backend |
|-----------|--------|---------|
| StateStore::get | < 40ns | FxHashMap (in-memory) |
| StateStore::put | < 60ns | FxHashMap (in-memory) |
| StateStore::range | < 200ns | BTreeMap (in-memory) |
| Lookup cache hit (memory-only foyer) | < 500ns | foyer::Cache (sync) |
| Barrier processing | < 50ns | Enum match, no allocation |
| Watermark advance | < 100ns | Comparison + state update |

### Ring 1 (Background) — < 100us typical

| Operation | Target | Notes |
|-----------|--------|-------|
| Checkpoint snapshot (rkyv serialize) | < 10ms | Per-partition rkyv zero-copy serialization |
| Lookup cache miss → foyer disk | 10-100us | NVMe read latency |
| redb index point lookup | < 10us | B-tree key lookup |
| Cross-partition aggregate read | 1-10us | papaya lock-free HashMap |

### Ring 2 (Control Plane) — milliseconds acceptable

| Operation | Target | Notes |
|-----------|--------|-------|
| Checkpoint upload to S3 | 100ms-10s | Depends on state size |
| Lookup cache miss → Postgres | 1-10ms | Network + query |
| Partition reassignment | 1-30s | Download + restore |
| Gossip convergence | < 2s | chitchat Scuttlebutt |
| Raft consensus round | < 10ms | openraft (3-node) |

---

## Crate Dependencies

All pure Rust. No C++ FFI.

| Crate | Version | Purpose | Ring |
|-------|---------|---------|------|
| `object_store` | 0.13+ | S3/GCS/local checkpoint storage | 2 |
| `foyer` | latest | Hybrid cache for lookup tables | 0/1 |
| `chitchat` | latest | Gossip discovery + failure detection | 2 |
| `openraft` | 0.10+ | Metadata consensus | 2 |
| `papaya` | latest | Lock-free concurrent HashMap | 1 |
| `redb` | latest | Secondary indexes (B-tree, pure Rust) | 1/2 |
| `rkyv` | 0.8+ | Zero-copy state serialization | 1 |
| `tonic` | latest | gRPC inter-node communication | 2 |
| `axum` | latest | HTTP API for laminardb-server | 2 |
| `toml` | latest | Config file parsing | 2 |

**Always compiled** (after F-GATE-001): chitchat, openraft, tonic, axum, toml.

**Platform-gated:** `aws` (S3), `gcs` (GCS), `azure` (ABS), `kafka`, `postgres-cdc`, `io-uring`, `jemalloc`.

**Deferred evaluation:** `slatedb` 0.10+ (after Phase 7c, if disaggregated backend proves insufficient).

---

## Architecture Audit & Fix Plan

An architecture audit was conducted on 2026-02-17 across all 55 delta feature specs. The audit identified **9 CRITICAL**, **27 HIGH**, **24 MEDIUM**, and **12 LOW** issues.

The prioritized fix plan is in **[ARCHITECTURE-FIX-PLAN.md](ARCHITECTURE-FIX-PLAN.md)**.

**Top 5 systemic issues:**
1. **No snapshot consistency mechanism** — O(n) clone at barrier time after mmap supersession (C1)
2. **Incomplete epoch fencing** — validates epoch but not owner node_id (C5, C6)
3. **rkyv key encoding breaks index ordering** — little-endian doesn't sort correctly (C2)
4. **`unreachable!()` on Ring 0** — watermarks hitting barrier aligner crash the partition (C4)
5. **LookupTable lifetime unsoundness** — foyer CacheEntry RAII vs borrowed slice (C3)

---

## Decision Log

| Decision | Chosen | Rejected | Rationale |
|----------|--------|----------|-----------|
| State backend default | FxHashMap/BTreeMap (in-memory) | MmapStateStore, SlateDB, RocksDB | FxHashMap ~40ns get vs. mmap ~200ns; rkyv + object_store provides durability without mmap complexity |
| Serialization | rkyv 0.8 | bincode, protobuf | Zero-copy deserialization |
| Discovery | chitchat | Custom gossip | Battle-tested at Quickwit |
| Consensus | openraft | raft-rs, etcd | Pure Rust; async event-driven |
| Lookup cache | foyer | CacheLib (C++) | Production-proven at RisingWave |
| Cross-partition aggregation | papaya | dashmap | Lock-free; higher throughput |
| Checkpoint protocol | Chandy-Lamport | 2PC, Saga | Standard for streaming |
| Cluster model | Node-ownership | Capacity-based | Deterministic latency |
| Server config | TOML | YAML, JSON | Rust ecosystem standard |
| Secondary indexes | redb | RocksDB, sled | Pure Rust, ACID, single-writer/multi-reader fits thread-per-core |
| Deployment model | Named profiles (4 tiers) | Feature flag combos | Validated configs, clear capability matrix |
| Predicate pushdown | Flat enum | Expression tree | Scope containment |
| StateStore get_async | Removed | Keep in base trait | Clean Ring 0 interface |
| Mmap state backend | Removed | MmapStateStore | FxHashMap faster; rkyv + object_store simpler durability |
| Feature gate strategy | Always-compile (runtime config) | Cargo feature gates for durable/delta | Eliminates combinatorial testing; platform gates kept for C deps |
| S3 role | Source of truth (write-only on hot path) | Backup/archive only | Industry consensus (RisingWave, WarpStream, AutoMQ); S3 Express pricing viable |
| Durable state backend | Custom disaggregated (rkyv + object_store) | SlateDB LSM | Simpler for write-once checkpoint model; SlateDB deferred for evaluation |
| Checkpoint object size | 4-32MB batched | Per-record or per-micro-batch | S3 PUT cost optimization ($0.005/1000); batching reduces cost ~10x |

---

## References

- [Delta Architecture Research](../../research/delta-architecture-v3.md)
- [ARCHITECTURE.md](../../ARCHITECTURE.md) — Ring model and system design
- [ROADMAP.md](../../ROADMAP.md) — Phase timeline
- [STEERING.md](../../STEERING.md) — Current priorities
- Apache Flink — Chandy-Lamport checkpoint barriers
- RisingWave / Hummock — foyer hybrid cache validation
- Feldera / DBSP — Simplified fault tolerance
- WarpStream — Stateless compute + object storage pattern
