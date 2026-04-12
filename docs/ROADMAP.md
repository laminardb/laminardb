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
|  | Core |----->| SQL  |--->|Harden|--------------->|Connect|  |
|  |Engine|      |Parser|    | ing  |               | ors  |  |
|  +------+      +------+    +------+               +------+  |
|  DONE          DONE        DONE                   95%       |
|                                                                  |
|  Phase 4       Phase 5     Phase 6a      Phase 6b    Phase 6c  |
|  +------+      +------+    +------+      +------+    +------+  |
|  |Secure|      |Admin |    |Partit|      |Delta |    | Prod |  |
|  |      |      |      |    |  ion |      |Found |    |Harden|  |
|  +------+      +------+    +------+      +------+    +------+  |
|  Planned       40%         93%           DONE        80%       |
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

**Goal**: Harden the engine with advanced window types, joins, exactly-once sinks, and performance infrastructure.

**Completed Features (38/38):**
- Thread-per-core architecture with CPU pinning (F013-F015) (superseded by StreamingCoordinator, PR #204)
- Sliding, hopping, and session windows with merge support (F016-F018)
- Stream-stream, lookup, temporal, and ASOF joins (F019-F021, F056-F057)
- Incremental checkpointing (F022) (per-core WAL F062 removed — dead code)
- Exactly-once sinks with two-phase commit (F023-F024)
- FIRST/LAST aggregates, cascading materialized views (F059-F060)
- EMIT clause extension, changelog/retraction Z-sets (F011B, F063)
- Per-partition, keyed, and alignment group watermarks (F064-F066)
- io_uring optimization, NUMA-aware memory (F067-F068) (removed — dead feature flags with no implementation)
- StreamingCoordinator I/O architecture, task budget enforcement (F069-F070)
- Zero-allocation enforcement, zero-allocation polling (F071, F073)
- XDP/eBPF network optimization (F072)
- Advanced DataFusion integration, composite aggregator (F005B, F074-F077)

---

## Phase 2.5: JIT Compiler -- REMOVED

Cranelift JIT compilation was removed in favor of compiled `PhysicalExpr` projections
and cached logical plans, which achieve equivalent per-cycle overhead reduction without
the 12K LOC Cranelift dependency. Non-JIT compiler infrastructure (row format, bridge,
pipeline bridge, event time extraction) is retained for Ring 0.

---

## Phase 3: Connectors & Integration -- 95% COMPLETE (95/100)

**Goal**: Connect to external systems for real-world data pipelines.

**Completed:**
- Streaming API (ring buffer, SPSC/MPSC channels, source/sink abstractions, broadcast, checkpointing) -- 9/9
- DAG pipeline (topology, multicast, executor, checkpointing, SQL/MV integration, connector bridge) -- 7/7
- Reactive subscriptions (change events, notification slot, registry, dispatcher, push/callback/stream subscriptions, backpressure) -- 8/8
- Cloud storage infrastructure (credentials, validation, secret masking) -- 3/3
- External connectors: Kafka source/sink, PostgreSQL CDC/sink, MySQL CDC, MongoDB CDC, Delta Lake sink/source (with recovery, compaction, schema evolution), Iceberg source/sink with I/O integration, Connector SDK -- 19/19
- SQL extensions: ASOF JOIN, LAG/LEAD, ROW_NUMBER/RANK/DENSE_RANK, HAVING, multi-way JOINs, window frames, multi-partition scans -- 7/7
- Stateful streaming SQL: aggregation hardening, EOWC incremental accumulators, checkpoint integration, Ring 0 SQL operator routing (tumbling/hopping/session), streaming physical optimizer, cooperative scheduling, dynamic watermark filter pushdown -- 7/7
- Connector infrastructure: checkpoint recovery, reference tables, partial cache, RocksDB table store, Avro hardening -- 6/6
- Pipeline observability -- 1/1
- Demo application (market data pipeline, Ratatui TUI, Kafka mode, DAG visualization) -- 6/6
- Unified checkpoint system (manifest, 2PC, coordinator, operator state, changelog, WAL coordination, recovery, observability) -- 9/9
- FFI & language bindings (API module, C headers, Arrow C Data Interface, async callbacks) -- 4/4
- Schema framework (trait architecture, resolver, inference registry, JSON/CSV/Avro/Parquet decoders, evolution, DLQ, JSON functions, array/struct/map functions, format bridges, schema hints) -- 15/16

**Remaining (5 features):**
- Redis Lookup Table (F030)
- Parquet File Source (F033) -- lookup source exists, streaming source not started
- Async State Access (F058)
- Historical Backfill (F061)
- Protobuf Format Decoder (F-SCHEMA-008)

---

## Phase 4: Enterprise & Security -- PLANNED (0/11)

**Goal**: Add enterprise security features for production deployments.

Crate stubs (`laminar-auth/`, `laminar-admin/`, `laminar-observe/`) were deleted as empty in commit a0461ef. No implementation exists.

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

## Phase 5: Admin & Observability -- 40% COMPLETE (4/10)

**Goal**: Provide operational tools for managing LaminarDB.

Implemented in `laminar-server` (crate stubs deleted). REST API, Prometheus, health checks, and CLI are production-ready.

**Completed Features (4/10):**
- F046: Admin REST API (Axum, 13 endpoints, CORS, request logging)
- F050: Prometheus Export (`/metrics` with 15+ counters)
- F052: Health Check Endpoints (`/health` liveness, `/ready` readiness)
- F055: CLI Tools (clap args, `--validate-checkpoints` mode)

**Remaining Features (6/10):**
- F047: Web Dashboard
- F048: Real-Time Metrics (streaming beyond Prometheus)
- F049: SQL Query Console (interactive REPL)
- F051: OpenTelemetry Tracing
- F053: Alerting Integration
- F054: Configuration Management (advanced)

---

## Phase 6a: Partition-Parallel Embedded -- 93% COMPLETE (27/29)

**Goal**: State store infrastructure, checkpoint barriers, lookup tables with foyer caching, SQL integration, baseline benchmarks.

**Completed:**
- State store architecture (revised trait, InMemoryStateStore) -- 2/2 (2 superseded)
- Distributed checkpoint barriers and alignment -- 5/5
- Lookup tables (trait, sources, predicates, foyer caches, CDC adapter, Postgres/Parquet sources, RocksDB removal, physical executor, predicate pushdown) -- 9/9
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

## Phase 6c: Delta Production Hardening -- 80% COMPLETE (8/10)

**Goal**: Unaligned checkpoints, exactly-once sinks, laminardb-server binary with TOML config, HTTP API, hot reload, rolling restarts.

**Completed Features (8/10):**
- TOML Configuration (F-SERVER-001) -- full parsing, env-var substitution, validation
- Engine Construction (F-SERVER-002) -- config-to-DDL translation, builder API
- HTTP API (F-SERVER-003) -- Axum, 13 REST endpoints, CORS
- Hot Reload (F-SERVER-004) -- file watcher, config diff, incremental DDL application
- Delta Server Mode (F-SERVER-005) -- discovery, membership, partition assignment
- Graceful Rolling Restart (F-SERVER-006) -- signal handling, drain logic
- Transactional Sink 2PC (F-E2E-002) -- trait + Kafka/Postgres/Delta/Files implementations
- Idempotent Sink (F-E2E-003) -- per-sink deduplication strategies

**Remaining Features (1/10):**
- Unaligned Checkpoints (F-DCKP-006) -- config structs defined, state machine deferred

(1 feature superseded: Incremental Mmap Checkpoints)

---

## Performance Optimization -- 17% COMPLETE (2/12)

Architectural performance improvements identified by audit. All actionable local fixes (40+ findings) have been implemented.

**Completed:**
- Binary JSON (JSONB) (F-POPT-008) -- compact binary encoding with O(log n) field access
- SQL Plan Cache (F-POPT-009) -- compiled PhysicalExpr projections with cached logical plan fallback

**Remaining:**
- Zero-Copy Prefix Scan (F-POPT-001)
- Monomorphized Reactor Dispatch (F-POPT-002)
- AHashMapStore Key Deduplication (F-POPT-003)
- MmapStateStore Compaction (F-POPT-004)
- Zero-Copy Join Row Encoding (F-POPT-005)
- Pooled Arrow Builders (F-POPT-006)
- LogicalPlan Passthrough (F-POPT-007)
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
| Phase 2.5: JIT Compiler | 12 | 12 | REMOVED |
| Phase 3: Connectors | 100 | 95 | 95% |
| Phase 4: Security | 11 | 0 | Planned |
| Phase 5: Admin | 10 | 4 | 40% |
| Phase 6a: Partition-Parallel | 29 | 27 | 93% |
| Phase 6b: Delta Foundation | 14 | 14 | COMPLETE |
| Phase 6c: Delta Hardening | 10 | 8 | 80% |
| Perf Optimization | 12 | 2 | 17% |
| **Total** | **249** | **213** | **86%** |

Note: Phases 4 and 5 can be developed in parallel with Phase 3 completion since they build on Phase 1 independently.
