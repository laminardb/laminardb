# Roadmap

## Overview

LaminarDB development is organized into phases, each building on the previous. Development has progressed significantly beyond the original 5-phase plan, with two additional major phases (2.5 and 6) added.

```
+------------------------------------------------------------------+
|                        Development Phases                        |
+------------------------------------------------------------------+
|                                                                  |
|  Phase 1       Phase 1.5    Phase 2       Phase 2.5    Phase 3  |
|  +------+      +------+    +------+      +------+    +------+  |
|  | Core |----->| SQL  |--->|Harden|--->  | JIT  |--->|Connect|  |
|  |Engine|      |Parser|    | ing  |      |Compil|    | ors  |  |
|  +------+      +------+    +------+      +------+    +------+  |
|  DONE          DONE        DONE          DONE        85%       |
|                                                                  |
|  Phase 4       Phase 5     Phase 6a      Phase 6b    Phase 6c  |
|  +------+      +------+    +------+      +------+    +------+  |
|  |Secure|      |Admin |    |Partit|      |Delta |    | Prod |  |
|  |      |      |      |    |  ion |      |Found |    |Harden|  |
|  +------+      +------+    +------+      +------+    +------+  |
|  Planned       Planned     93%           DONE        Planned   |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Phase 1: Core Engine -- COMPLETE

**Goal**: Build the foundational streaming engine with basic functionality.

**Completed Features (12/12):**
- F001: Core Reactor Event Loop
- F002: Memory-Mapped State Store
- F003: State Store Interface
- F004: Tumbling Windows
- F005: DataFusion Integration
- F006: Basic SQL Parser
- F007: Write-Ahead Log (with CRC32C, torn write detection, fdatasync)
- F008: Basic Checkpointing
- F009: Event Time Processing
- F010: Watermarks
- F011: EMIT Clause
- F012: Late Data Handling

---

## Phase 1.5: Production SQL Parser -- COMPLETE

**Completed Features (1/1):**
- F006B: Production SQL Parser (129 tests)

---

## Phase 2: Production Hardening -- COMPLETE

**Goal**: Make the engine production-ready with advanced features.

**Completed Features (38/38):**
- Thread-per-core architecture with CPU pinning (F013-F015)
- Sliding, hopping, and session windows with merge support (F016-F018)
- Stream-stream, lookup, temporal, and ASOF joins (F019-F021, F056-F057)
- Incremental checkpointing and per-core WAL (F022, F062)
- Exactly-once sinks with two-phase commit (F023-F024)
- FIRST/LAST aggregates, cascading materialized views (F059-F060)
- EMIT clause extension, changelog/retraction Z-sets (F011B, F063)
- Per-partition, keyed, and alignment group watermarks (F064-F066)
- io_uring optimization, NUMA-aware memory (F067-F068)
- Three-ring I/O architecture, task budget enforcement (F069-F070)
- Zero-allocation enforcement, zero-allocation polling (F071, F073)
- XDP/eBPF network optimization (F072)
- Advanced DataFusion integration, composite aggregator (F005B, F074-F077)

---

## Phase 2.5: JIT Compiler -- COMPLETE

**Goal**: Compile DataFusion logical plans into zero-allocation JIT functions for Ring 0.

**Completed Features (12/12):**
- Event Row Format (F078)
- Compiled Expression Evaluator (F079)
- Plan Compiler Core (F080)
- Ring 0/Ring 1 Pipeline Bridge (F081)
- Streaming Query Lifecycle (F082)
- Batch Row Reader (F083)
- SQL Compiler Orchestrator (F084)
- LaminarDB JIT Query Execution (F085)
- Adaptive Compilation Warmup (F086)
- Compiled Stateful Pipeline Bridge (F087)
- Schema-Aware Event Time Extraction (F088)
- Compilation Metrics & Observability (F089)

---

## Phase 3: Connectors & Integration -- 85% COMPLETE (85/100)

**Goal**: Connect to external systems for real-world data pipelines.

**Completed:**
- Streaming API (ring buffer, SPSC/MPSC channels, source/sink abstractions, broadcast, checkpointing) -- 9/9
- DAG pipeline (topology, multicast, executor, checkpointing, SQL/MV integration, connector bridge) -- 7/7
- Reactive subscriptions (change events, notification slot, registry, dispatcher, push/callback/stream subscriptions, backpressure) -- 8/8
- Cloud storage infrastructure (credentials, validation, secret masking) -- 3/3
- External connectors: Kafka source/sink, PostgreSQL CDC/sink, MySQL CDC, Delta Lake sink/source, Iceberg sink, Connector SDK -- 14/19
- SQL extensions: ASOF JOIN, LAG/LEAD, ROW_NUMBER/RANK/DENSE_RANK, HAVING, multi-way JOINs, window frames, multi-partition scans -- 7/7
- Stateful streaming SQL: aggregation hardening, EOWC incremental accumulators, checkpoint integration, Ring 0 SQL operator routing (tumbling/hopping/session), streaming physical optimizer, cooperative scheduling, dynamic watermark filter pushdown -- 7/7
- Connector infrastructure: checkpoint recovery, reference tables, partial cache, RocksDB table store, Avro hardening -- 6/6
- Pipeline observability -- 1/1
- Demo application (market data pipeline, Ratatui TUI, Kafka mode, DAG visualization) -- 6/6
- Unified checkpoint system (manifest, 2PC, coordinator, operator state, changelog, WAL coordination, recovery, observability) -- 9/9
- FFI & language bindings (API module, C headers, Arrow C Data Interface, async callbacks) -- 4/4
- Schema framework (trait architecture, resolver, inference registry, JSON/CSV/Avro/Parquet decoders, evolution, DLQ, JSON functions, array/struct/map functions, format bridges, schema hints) -- 15/16

**Remaining (15 features, all Draft status):**
- MongoDB CDC Source (F029)
- Redis Lookup Table (F030)
- Delta Lake Recovery, Compaction, Schema Evolution (F031B-D)
- Iceberg I/O Integration (F032A) -- blocked by iceberg-datafusion compatibility
- Parquet File Source (F033)
- Async State Access (F058)
- Historical Backfill (F061)
- Protobuf Format Decoder (F-SCHEMA-008)

---

## Phase 4: Enterprise & Security -- PLANNED (0/11)

**Goal**: Add enterprise security features for production deployments.

Crate stubs exist (`laminar-auth/`) but source files contain only doc comments.

**Planned Features:**
- F035: Authentication Framework
- F036: JWT Authentication
- F037: mTLS Authentication
- F038: LDAP Integration
- F039: Role-Based Access Control (RBAC)
- F040: Attribute-Based Access Control (ABAC)
- F041: Row-Level Security
- F042: Column-Level Security
- F043: Audit Logging
- F044: Encryption at Rest
- F045: Key Management

---

## Phase 5: Admin & Observability -- PLANNED (0/10)

**Goal**: Provide operational tools for managing LaminarDB.

Crate stubs exist (`laminar-admin/`, `laminar-observe/`) but source files contain only doc comments.

**Planned Features:**
- F046: Admin REST API
- F047: Web Dashboard
- F048: Real-Time Metrics
- F049: SQL Query Console
- F050: Prometheus Export
- F051: OpenTelemetry Tracing
- F052: Health Check Endpoints
- F053: Alerting Integration
- F054: Configuration Management
- F055: CLI Tools

---

## Phase 6a: Partition-Parallel Embedded -- 93% COMPLETE (27/29)

**Goal**: State store infrastructure, checkpoint barriers, lookup tables with foyer caching, SQL integration, baseline benchmarks.

**Completed:**
- State store architecture (revised trait, InMemoryStateStore) -- 2/2 (2 superseded)
- Distributed checkpoint barriers and alignment -- 5/5
- Lookup tables (trait, sources, predicates, foyer caches, CDC adapter, Postgres/Parquet sources, RocksDB removal) -- 9/9
- Secondary indexes (redb) -- 1/1
- Deployment profiles -- 1/1
- Cross-partition lock-free HashMap -- 1/1
- Source offset checkpoint -- 1/1
- SQL integration (CREATE LOOKUP TABLE DDL, lookup join plan node, predicate pushdown) -- 3/3
- Performance benchmarks (state store, cache, checkpoint, lookup join) -- 4/4

---

## Phase 6b: Delta Foundation -- COMPLETE (14/14)

**Goal**: Multi-node discovery, Raft-based metadata consensus, partition ownership, distributed checkpointing, inter-node RPC.

**Completed:**
- Discovery (static, gossip/chitchat, Kafka group) -- 3/3
- Coordination (Raft metadata consensus, delta orchestration) -- 2/2
- Partition ownership (epoch fencing, assignment algorithm, reassignment protocol) -- 3/3
- Distributed checkpoint coordination -- 1/1
- Partitioned lookup strategy -- 1/1
- Cross-node aggregation (gossip partial, gRPC fan-out) -- 2/2
- Inter-node RPC (gRPC services, remote lookup, barrier forwarding) -- 3/3
- Delta checkpoint benchmark -- 1/1

---

## Phase 6c: Delta Production Hardening -- PLANNED (0/10)

**Goal**: Unaligned checkpoints, exactly-once sinks, laminardb-server binary with TOML config, HTTP API, hot reload, rolling restarts.

**Planned Features:**
- Unaligned Checkpoints (F-DCKP-006)
- Transactional Sink 2PC (F-E2E-002)
- Idempotent Sink (F-E2E-003)
- TOML Configuration (F-SERVER-001)
- Engine Construction (F-SERVER-002)
- HTTP API (F-SERVER-003)
- Hot Reload (F-SERVER-004)
- Delta Server Mode (F-SERVER-005)
- Graceful Rolling Restart (F-SERVER-006)

(1 feature superseded: Incremental Mmap Checkpoints)

---

## Performance Optimization -- PLANNED (0/12)

Architectural performance improvements identified by audit. All actionable local fixes (40+ findings) have been implemented.

- Zero-Copy Prefix Scan (F-POPT-001)
- Monomorphized Reactor Dispatch (F-POPT-002)
- AHashMapStore Key Deduplication (F-POPT-003)
- MmapStateStore Compaction (F-POPT-004)
- Zero-Copy Join Row Encoding (F-POPT-005)
- Pooled Arrow Builders (F-POPT-006)
- LogicalPlan Passthrough (F-POPT-007)
- Binary JSON (JSONB) (F-POPT-008)
- SQL Plan Cache (F-POPT-009)
- MVCC TableStore (F-POPT-010)
- WAL Scatter-Gather Writes (F-POPT-011)
- Avro Decoder Reuse (F-POPT-012)

---

## Summary

| Phase | Features | Complete | Status |
|-------|----------|----------|--------|
| Phase 1: Core Engine | 12 | 12 | COMPLETE |
| Phase 1.5: SQL Parser | 1 | 1 | COMPLETE |
| Phase 2: Hardening | 38 | 38 | COMPLETE |
| Phase 2.5: JIT Compiler | 12 | 12 | COMPLETE |
| Phase 3: Connectors | 100 | 85 | 85% |
| Phase 4: Security | 11 | 0 | Planned |
| Phase 5: Admin | 10 | 0 | Planned |
| Phase 6a: Partition-Parallel | 29 | 27 | 93% |
| Phase 6b: Delta Foundation | 14 | 14 | COMPLETE |
| Phase 6c: Delta Hardening | 10 | 0 | Planned |
| Perf Optimization | 12 | 0 | Planned |
| **Total** | **249** | **189** | **76%** |

Note: Phases 4 and 5 can be developed in parallel with Phase 3 completion and Phase 6c since they build on Phase 1 independently.
