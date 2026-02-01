# Feature Index

## Overview

| Phase | Total | Draft | In Progress | Hardening | Done |
|-------|-------|-------|-------------|-----------|------|
| Phase 1 | 12 | 0 | 0 | 0 | 12 |
| Phase 1.5 | 1 | 0 | 0 | 0 | 1 |
| Phase 2 | 34 | 0 | 0 | 0 | 34 |
| Phase 3 | 33 | 13 | 1 | 0 | 19 |
| Phase 4 | 11 | 11 | 0 | 0 | 0 |
| Phase 5 | 10 | 10 | 0 | 0 | 0 |
| **Total** | **101** | **34** | **1** | **0** | **66** |

## Status Legend

- 📝 Draft - Specification written, not started
- 🚧 In Progress - Active development
- 🔧 Hardening - Functional but has gaps to fix
- ✅ Done - Complete and merged
- ⏸️ Paused - On hold
- ❌ Cancelled - Will not implement

---

## Phase 1: Core Engine

> **Status**: ✅ **PHASE 1 COMPLETE!** All P0 hardening tasks done. Ready for Phase 2. See [PHASE1_AUDIT.md](../PHASE1_AUDIT.md) for details.

| ID | Feature | Priority | Status | Gaps | Spec |
|----|---------|----------|--------|------|------|
| F001 | Core Reactor Event Loop | P0 | ✅ | No io_uring (P1) | [Link](phase-1/F001-core-reactor-event-loop.md) |
| F002 | Memory-Mapped State Store | P0 | ✅ | No CoW/huge pages (P1) | [Link](phase-1/F002-memory-mapped-state-store.md) |
| F003 | State Store Interface | P0 | ✅ | None (prefix_scan now O(log n + k)) | [Link](phase-1/F003-state-store-interface.md) |
| F004 | Tumbling Windows | P0 | ✅ | None | [Link](phase-1/F004-tumbling-windows.md) |
| F005 | DataFusion Integration | P0 | ✅ | No EXPLAIN (P2) | [Link](phase-1/F005-datafusion-integration.md) |
| F006 | Basic SQL Parser | P0 | ✅ | Superseded by F006B | [Link](phase-1/F006-basic-sql-parser.md) |
| F007 | Write-Ahead Log | P1 | ✅ | CRC32, fdatasync, torn write - all fixed | [Link](phase-1/F007-write-ahead-log.md) |
| F008 | Basic Checkpointing | P1 | 🔧 | Blocking I/O (P1 for Phase 2) | [Link](phase-1/F008-basic-checkpointing.md) |
| F009 | Event Time Processing | P1 | ✅ | None | [Link](phase-1/F009-event-time-processing.md) |
| F010 | Watermarks | P1 | ✅ | Persistence fixed in WAL + checkpoint | [Link](phase-1/F010-watermarks.md) |
| F011 | EMIT Clause | P2 | ✅ | None | [Link](phase-1/F011-emit-clause.md) |
| F012 | Late Data Handling | P2 | ✅ | No retractions (P2) | [Link](phase-1/F012-late-data-handling.md) |

### Phase 1 Hardening Tasks (P0) - ALL COMPLETE ✅

| Task | Feature | Status | Notes |
|------|---------|--------|-------|
| WAL: fsync → fdatasync | F007 | ✅ Done | `sync_data()` saves 50-100μs/sync |
| WAL: Add CRC32 checksum | F007 | ✅ Done | CRC32C hardware accelerated |
| WAL: Torn write detection | F007 | ✅ Done | `WalReadResult::TornWrite`, `repair()` |
| Watermark persistence | F010 | ✅ Done | In WAL commits and checkpoints |
| Recovery integration test | F007/F008 | ✅ Done | 6 comprehensive tests |

---

## Phase 1.5: SQL Parser Production Upgrade

> **Status**: ✅ **COMPLETE** - All 6 phases implemented

| ID | Feature | Priority | Status | Effort | Spec |
|----|---------|----------|--------|--------|------|
| F006B | Production SQL Parser | P0 | ✅ | L (2-3 weeks) | [Link](phase-1/F006B-production-sql-parser.md) |

### F006B Implementation Phases - ALL COMPLETE ✅

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | CREATE SOURCE/SINK parsing | ✅ Done |
| 2 | Window function extraction (TUMBLE/HOP/SESSION) | ✅ Done |
| 3 | EMIT/Late Data integration | ✅ Done |
| 4 | Join query parsing (stream-stream, lookup) | ✅ Done |
| 5 | Query planner integration | ✅ Done |
| 6 | Aggregator detection (COUNT/SUM/MIN/MAX/AVG) | ✅ Done |

### Key Deliverables - ALL COMPLETE ✅

- [x] CREATE SOURCE/SINK parsing (`parser/streaming_parser.rs`)
- [x] Window function extraction (`parser/window_rewriter.rs`)
- [x] EMIT/Late Data integration (`parser/emit_parser.rs`)
- [x] Join query parsing (`parser/join_parser.rs`)
- [x] Query planner integration (`planner/mod.rs`)
- [x] Aggregation detection (`parser/aggregation_parser.rs`)
- [x] Window translator (`translator/window_translator.rs`)
- [x] Join translator (`translator/join_translator.rs`)
- [x] 129 tests in laminar-sql (exceeded 30+ target)

---

## Phase 2: Production Hardening

> **Status**: ✅ **PHASE 2 COMPLETE!** All 30 features implemented.

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F013 | Thread-Per-Core Architecture | P0 | ✅ | [Link](phase-2/F013-thread-per-core.md) |
| F014 | SPSC Queue Communication | P0 | ✅ | [Link](phase-2/F014-spsc-queues.md) |
| F015 | CPU Pinning | P1 | ✅ | Included in F013 |
| F016 | Sliding Windows | P0 | ✅ | [Link](phase-2/F016-sliding-windows.md) |
| F017 | Session Windows | P1 | ✅ | [Link](phase-2/F017-session-windows.md) |
| F018 | Hopping Windows | P1 | ✅ | Same as F016 (sliding) |
| F019 | Stream-Stream Joins | P0 | ✅ | [Link](phase-2/F019-stream-stream-joins.md) |
| F020 | Lookup Joins | P0 | ✅ | [Link](phase-2/F020-lookup-joins.md) |
| F021 | Temporal Joins | P2 | ✅ | [Link](phase-2/F021-temporal-joins.md) |
| F022 | Incremental Checkpointing | P1 | ✅ | [Link](phase-2/F022-incremental-checkpointing.md) |
| F023 | Exactly-Once Sinks | P0 | ✅ | [Link](phase-2/F023-exactly-once-sinks.md) |
| F024 | Two-Phase Commit | P1 | ✅ | [Link](phase-2/F024-two-phase-commit.md) |
| F056 | ASOF Joins | P1 | ✅ | [Link](phase-2/F056-asof-joins.md) |
| F057 | Stream Join Optimizations | P1 | ✅ | [Link](phase-2/F057-stream-join-optimizations.md) |
| F059 | FIRST/LAST Value Aggregates | P0 | ✅ | [Link](phase-2/F059-first-last-aggregates.md) |
| F060 | Cascading Materialized Views | P1 | ✅ | [Link](phase-2/F060-cascading-materialized-views.md) |
| F062 | Per-Core WAL Segments | P1 | ✅ | [Link](phase-2/F062-per-core-wal.md) |
| **F011B** | **EMIT Clause Extension** | **P0** | ✅ | [Link](phase-2/F011B-emit-clause-extension.md) |
| **F063** | **Changelog/Retraction (Z-Sets)** | **P0** | ✅ | [Link](phase-2/F063-changelog-retraction.md) |
| **F064** | **Per-Partition Watermarks** | **P1** | ✅ | [Link](phase-2/F064-per-partition-watermarks.md) |
| **F065** | **Keyed Watermarks** | **P1** | ✅ | [Link](phase-2/F065-keyed-watermarks.md) |
| **F066** | **Watermark Alignment Groups** | **P2** | ✅ | [Link](phase-2/F066-watermark-alignment-groups.md) |
| **F067** | **io_uring Advanced Optimization** | **P0** | ✅ | [Link](phase-2/F067-io-uring-optimization.md) |
| **F068** | **NUMA-Aware Memory Allocation** | **P0** | ✅ | [Link](phase-2/F068-numa-aware-memory.md) |
| **F069** | **Three-Ring I/O Architecture** | **P1** | ✅ | [Link](phase-2/F069-three-ring-io.md) |
| **F070** | **Task Budget Enforcement** | **P1** | ✅ | [Link](phase-2/F070-task-budget-enforcement.md) |
| **F071** | **Zero-Allocation Enforcement** | **P0** | ✅ | [Link](phase-2/F071-zero-allocation-enforcement.md) |
| **F072** | **XDP/eBPF Network Optimization** | **P2** | ✅ | [Link](phase-2/F072-xdp-network-optimization.md) |
| **F073** | **Zero-Allocation Polling** | **P1** | ✅ | [Link](phase-2/F073-zero-allocation-polling.md) |
| **F005B** | **Advanced DataFusion Integration** | **P1** | ✅ | [Link](phase-2/F005B-advanced-datafusion-integration.md) |
| **F074** | **Composite Aggregator & f64 Type Support** | **P0** | ✅ | [Link](phase-2/F074-composite-aggregator.md) |
| **F075** | **DataFusion Aggregate Bridge** | **P1** | ✅ | [Link](phase-2/F075-datafusion-aggregate-bridge.md) |
| **F076** | **Retractable FIRST/LAST Accumulators** | **P0** | ✅ | [Link](phase-2/F076-retractable-first-last.md) |
| **F077** | **Extended Aggregation Parser** | **P1** | ✅ | [Link](phase-2/F077-extended-aggregation-parser.md) |

### Phase 2 Thread-Per-Core Research Gap Analysis (NEW)

> Based on [Thread-Per-Core 2026 Research](../research/laminardb-thread-per-core-2026-research.md)

| Gap | Research Finding | Current (F013) | Target | Feature |
|-----|------------------|----------------|--------|---------|
| **io_uring basic only** | "SQPOLL + registered buffers = 2.05x" | ❌ No io_uring | SQPOLL, registered buffers, IOPOLL | **F067** |
| **No NUMA awareness** | "2-3x latency on remote access" | ❌ Generic allocation | NUMA-local per core | **F068** |
| **Single I/O ring** | "3 rings: latency/main/poll" | ❌ Single reactor | Priority-based rings | **F069** |
| **No task budgeting** | "Ring 0: 500ns, Ring 1: 1ms budgets" | ❌ No enforcement | Budget + metrics + yielding | **F070** |
| **No allocation detection** | "Zero-alloc hot path verification" | ✅ Implemented | Debug-mode detector + CI | **F071** |
| ~~No XDP steering~~ | "26M packets/sec/core" | ✅ Implemented | CPU steering by partition | **F072** |
| CPU pinning | "Cache efficiency" | ✅ Implemented | - | F013/F015 |
| Lock-free SPSC | "~4.8ns per operation" | ✅ Implemented | - | F014 |
| Credit-based backpressure | "Flink-style flow control" | ✅ Implemented | - | F014 |

**Key Research Findings:**
> "Simply replacing I/O with io_uring yields only **1.06-1.10x** improvement. Careful optimization achieves **2.05x** or more." - TU Munich, Dec 2024

> "On multi-socket systems, memory access latency varies by **2-3x** depending on whether memory is local or remote to the CPU."

**Thread-Per-Core Evolution Path:**
```
F013 (TPC Foundation) ──┬──▶ F067 (io_uring) ──▶ F069 (Three-Ring)
      ✅ Complete       │
                        ├──▶ F068 (NUMA) ──▶ Performance
                        │
                        ├──▶ F070 (Task Budget) ──▶ Latency SLAs
                        │
                        └──▶ F071 (Zero-Alloc) ──▶ Hot Path Verification
                                    │
                                    └──▶ F072 (XDP) [P2]
```

### Phase 2 Watermark Research Gap Analysis (NEW)

> Based on [Watermark Generator Research 2026](../research/watermark-generator-research-2026.md)

| Gap | Research Finding | Current (F010) | Target | Feature |
|-----|------------------|----------------|--------|---------|
| **Keyed Watermarks** | "99%+ accuracy vs 63-67% global" | ❌ Global only | Per-key tracking | **F065** |
| **Per-Partition Tracking** | "Kafka partitions need independent watermarks" | ❌ Per-source only | Per-partition | **F064** |
| **Alignment Groups** | "Prevent unbounded state growth" | ❌ No drift limits | Bounded drift + pause | **F066** |
| Idle Detection | "Critical for pipeline progress" | ✅ Implemented | - | F010 |
| Bounded Out-of-Orderness | "Default strategy" | ✅ Implemented | - | F010 |

**Key Research Finding:**
> "Keyed watermarks achieve **99%+ accuracy** compared to **63-67%** with global watermarks." - ScienceDirect, March 2025

**Watermark Evolution Path:**
```
F010 (Global) ──► F064 (Per-Partition) ──► F065 (Per-Key)
                        │
                        └──► F066 (Alignment Groups)
```

### Phase 2 Emit Patterns Gap Analysis

> Based on [Emit Patterns Research 2026](../research/emit-patterns-research-2026.md)

| Gap | Research Finding | Current | Target | Feature |
|-----|------------------|---------|--------|---------|
| **EMIT ON WINDOW CLOSE** | "Essential for append-only sinks" | ❌ Parsed but not implemented | Critical for F023 | **F011B** |
| **Changelog/Retraction** | "DBSP Z-sets fundamental" | ❌ None | Z-set weights, CDC format | **F063** |
| **EMIT CHANGES** | "CDC pipelines need delta emission" | ❌ Missing | Emit +/-/update pairs | **F011B** |
| **EMIT FINAL** | "Suppress intermediate for BI" | ❌ Missing | No retractions | **F011B** |
| CDC Envelope Format | "Debezium standard" | ❌ Missing | Interoperable format | **F063** |
| Emit Strategy Propagation | "Optimizer rule for sink compat" | ❌ Missing | Auto-select by sink type | **F011B** |

**Critical Dependency Chain**:
```
F011 (EMIT Clause) ──► F011B (Extension) ──┐
                                           ├──► F023 (Exactly-Once Sinks)
F063 (Changelog/Retraction) ──────────────┘
                           │
                           └──► F060 (Cascading MVs)
```

### Phase 2 Checkpoint/Recovery Gap Analysis

> Based on [Checkpoint Implementation Prompt](../research/checkpoint-implementation-prompt.md) and [ADR-004: Checkpoint Strategy](../adr/ADR-004-checkpoint-strategy.md)

| Gap | Research Finding | Current | Target | Feature |
|-----|------------------|---------|--------|---------|
| **Checkpoint blocks Ring 0** | "Ring 0 <500ns, checkpoint in Ring 1" | ❌ Blocking | Async in Ring 1 | F022 |
| **No changelog buffer** | "Zero-alloc offset references" | ❌ Missing | ChangelogRef in Ring 0 | F022 |
| **No incremental checkpoints** | "RocksDB hard-linked SSTables" | ❌ Full snapshots | <10% for 1% changes | F022 |
| ~~No per-core WAL~~ | "Required for thread-per-core" | ✅ Implemented | Per-core segments | F062 |
| **No WAL truncation** | "Bound storage after checkpoint" | ❌ Growing WAL | Truncate after checkpoint | F022 |

**Three-Tier Architecture (Target)**:
```
Ring 0: mmap + ChangelogBuffer (zero-alloc) ──▶ Ring 1: WAL + RocksDB ──▶ Ring 2: Object Storage (future)
```

**Core Invariant**: `Checkpoint(epoch) + WAL.replay(epoch..current) = Consistent State`

### Phase 2 Join Research Gap Analysis

> Based on [Stream Joins Research Review 2026](../research/laminardb-stream-joins-research-review-2026.md)

| Gap | Source | Current | Target | Feature |
|-----|--------|---------|--------|---------|
| ASOF Joins | DuckDB/Pinot 2025 | ✅ Done | Full support | F056 |
| CPU-Friendly Encoding | RisingWave July 2025 | ❌ Missing | 50% perf gain | F057 |
| Asymmetric Compaction | Epsio 2025 | ❌ Missing | Reduced overhead | F057 |
| Temporal Join (versioned) | RisingWave 2025 | 📝 Draft | Full impl | F021 |
| Async State Access | Flink 2.0 | ❌ Missing | Phase 3 | F058 |

### Phase 2 Financial Analytics Gap Analysis

> Based on [Time-Series Financial Research 2026](../research/laminardb-timeseries-financial-research-2026.md)

| Gap | Research Finding | Current | Target | Feature |
|-----|------------------|---------|--------|---------|
| **FIRST_VALUE/LAST_VALUE** | "OHLC is just SQL aggregates" | ❌ Missing | Essential for OHLC | F059 |
| Cascading MVs | Multi-resolution OHLC (1s→1m→1h) | ❌ Missing | MVs reading MVs | F060 |
| ASOF Joins | Financial enrichment (trade+quote) | ✅ Done | Phase 2 P1 | F056 |
| Historical Backfill | "Unified live + historical query" | ❌ Missing | Phase 3 P2 | F061 |
| SAMPLE BY Syntax | QuestDB-style time sampling | ❌ Missing | Nice to have (sugar) | - |

**Key Insight from Research**: No custom financial types needed. OHLC bars are just standard SQL aggregates (`FIRST_VALUE`, `MAX`, `MIN`, `LAST_VALUE`, `SUM`) over tumbling windows.

---

## Phase 3: Connectors & Integration

### Streaming API (In-Memory Sources/Sinks)

> **NEW**: In-memory streaming API - embedded Kafka Streams-like semantics.
> See [Streaming API Index](phase-3/streaming/INDEX.md) for details.

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-STREAM-001 | Ring Buffer | P0 | ✅ | [Link](phase-3/streaming/F-STREAM-001-ring-buffer.md) |
| F-STREAM-002 | SPSC Channel | P0 | ✅ | [Link](phase-3/streaming/F-STREAM-002-spsc-channel.md) |
| F-STREAM-003 | MPSC Auto-Upgrade | P0 | ✅ | [Link](phase-3/streaming/F-STREAM-003-mpsc-upgrade.md) |
| F-STREAM-004 | Source | P0 | ✅ | [Link](phase-3/streaming/F-STREAM-004-source.md) |
| F-STREAM-005 | Sink | P0 | ✅ | [Link](phase-3/streaming/F-STREAM-005-sink.md) |
| F-STREAM-006 | Subscription | P0 | ✅ | [Link](phase-3/streaming/F-STREAM-006-subscription.md) |
| F-STREAM-007 | SQL DDL | P0 | ✅ | [Link](phase-3/streaming/F-STREAM-007-sql-ddl.md) |
| F-STREAM-010 | Broadcast Channel | P1 | 📝 | [Link](phase-3/streaming/F-STREAM-010-broadcast-channel.md) |
| F-STREAM-013 | Checkpointing | P1 | ✅ | [Link](phase-3/streaming/F-STREAM-013-checkpointing.md) |

**Key Design Principles**:
- Channel type is NEVER user-specified (auto-derived)
- SPSC → MPSC upgrades automatically on `source.clone()`
- Checkpointing is OPTIONAL (zero overhead when disabled)

### DAG Pipeline

> **NEW**: DAG pipeline with shared intermediate stages, fan-out/fan-in, zero-copy multicast.
> See [DAG Pipeline Index](phase-3/dag/INDEX.md) for details.

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-DAG-001 | Core DAG Topology | P0 | ✅ | [Link](phase-3/dag/F-DAG-001-core-topology.md) |
| F-DAG-002 | Multicast & Routing | P0 | ✅ | [Link](phase-3/dag/F-DAG-002-multicast-routing.md) |
| F-DAG-003 | DAG Executor | P0 | ✅ | [Link](phase-3/dag/F-DAG-003-dag-executor.md) |
| F-DAG-004 | DAG Checkpointing | P1 | ✅ | [Link](phase-3/dag/F-DAG-004-dag-checkpointing.md) |
| F-DAG-005 | SQL & MV Integration | P1 | ✅ | [Link](phase-3/dag/F-DAG-005-sql-mv-integration.md) |
| F-DAG-006 | Connector Bridge | P1 | ✅ | [Link](phase-3/dag/F-DAG-006-connector-bridge.md) |
| F-DAG-007 | Performance Validation | P2 | 📝 | [Link](phase-3/dag/F-DAG-007-performance-validation.md) |

**Key Design Principles**:
- Pre-computed routing table for O(1) hot path dispatch
- Channel type auto-derived from topology (SPSC/SPMC/MPSC)
- Zero-copy multicast via reference-counted slot buffers
- Barrier-based checkpointing (Chandy-Lamport) through DAG edges

### SQL Extensions

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-SQL-001 | ASOF JOIN SQL Support | P1 | 🚧 In Progress | [Link](phase-3/F-SQL-001-asof-join-sql.md) |

### External Connectors

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F025 | Kafka Source Connector | P0 | ✅ | [Link](phase-3/F025-kafka-source.md) |
| F026 | Kafka Sink Connector | P0 | ✅ | [Link](phase-3/F026-kafka-sink.md) |
| F027 | PostgreSQL CDC Source | P0 | ✅ | [Link](phase-3/F027-postgres-cdc.md) |
| F027B | PostgreSQL Sink | P0 | ✅ | [Link](phase-3/F027B-postgres-sink.md) |
| F028 | MySQL CDC Source | P1 | 📝 | [Link](phase-3/F028-mysql-cdc.md) |
| F029 | MongoDB CDC Source | P2 | 📝 | [Link](phase-3/F029-mongodb-cdc.md) |
| F030 | Redis Lookup Table | P1 | 📝 | [Link](phase-3/F030-redis-lookup.md) |
| F031 | Delta Lake Sink | P0 | 📝 | [Link](phase-3/F031-delta-lake-sink.md) |
| F032 | Iceberg Sink | P1 | 📝 | [Link](phase-3/F032-iceberg-sink.md) |
| F033 | Parquet File Source | P2 | 📝 | [Link](phase-3/F033-parquet-source.md) |
| F034 | Connector SDK | P1 | 📝 | [Link](phase-3/F034-connector-sdk.md) |
| F058 | Async State Access | P1 | 📝 | [Link](phase-3/F058-async-state-access.md) |
| F061 | Historical Backfill | P2 | 📝 | [Link](phase-3/F061-historical-backfill.md) |

### Production Demo

> Market data demo with Ratatui TUI dashboard.
> See [Demo Index](phase-3/demo/INDEX.md) for details.

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-DEMO-001 | Market Data Pipeline | P0 | 📝 | [Link](phase-3/demo/F-DEMO-001-market-data-pipeline.md) |
| F-DEMO-002 | Ratatui TUI Dashboard | P0 | 📝 | [Link](phase-3/demo/F-DEMO-002-ratatui-tui.md) |
| F-DEMO-003 | Kafka Integration & Docker | P1 | 📝 | [Link](phase-3/demo/F-DEMO-003-kafka-docker.md) |

---

## Phase 4: Enterprise & Security

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F035 | Authentication Framework | P0 | 📝 | [Link](phase-4/F035-authn-framework.md) |
| F036 | JWT Authentication | P0 | 📝 | [Link](phase-4/F036-jwt-auth.md) |
| F037 | mTLS Authentication | P1 | 📝 | [Link](phase-4/F037-mtls-auth.md) |
| F038 | LDAP Integration | P2 | 📝 | [Link](phase-4/F038-ldap-integration.md) |
| F039 | Role-Based Access Control | P0 | 📝 | [Link](phase-4/F039-rbac.md) |
| F040 | Attribute-Based Access Control | P1 | 📝 | [Link](phase-4/F040-abac.md) |
| F041 | Row-Level Security | P0 | 📝 | [Link](phase-4/F041-row-level-security.md) |
| F042 | Column-Level Security | P2 | 📝 | [Link](phase-4/F042-column-level-security.md) |
| F043 | Audit Logging | P0 | 📝 | [Link](phase-4/F043-audit-logging.md) |
| F044 | Encryption at Rest | P1 | 📝 | [Link](phase-4/F044-encryption-at-rest.md) |
| F045 | Key Management | P1 | 📝 | [Link](phase-4/F045-key-management.md) |

---

## Phase 5: Admin & Observability

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F046 | Admin REST API | P0 | 📝 | [Link](phase-5/F046-admin-api.md) |
| F047 | Web Dashboard | P0 | 📝 | [Link](phase-5/F047-web-dashboard.md) |
| F048 | Real-Time Metrics | P0 | 📝 | [Link](phase-5/F048-realtime-metrics.md) |
| F049 | SQL Query Console | P0 | 📝 | [Link](phase-5/F049-sql-console.md) |
| F050 | Prometheus Export | P1 | 📝 | [Link](phase-5/F050-prometheus-export.md) |
| F051 | OpenTelemetry Tracing | P1 | 📝 | [Link](phase-5/F051-otel-tracing.md) |
| F052 | Health Check Endpoints | P0 | 📝 | [Link](phase-5/F052-health-checks.md) |
| F053 | Alerting Integration | P2 | 📝 | [Link](phase-5/F053-alerting.md) |
| F054 | Configuration Management | P1 | 📝 | [Link](phase-5/F054-config-management.md) |
| F055 | CLI Tools | P1 | 📝 | [Link](phase-5/F055-cli-tools.md) |

---

## Dependency Graph

```
Phase 1:
F001 (Reactor) ──┬──▶ F002 (State Store)
                 ├──▶ F003 (State Interface)
                 ├──▶ F004 (Tumbling Windows)
                 └──▶ F009 (Event Time)
                          │
F005 (DataFusion) ✅ ────▶ F006 (SQL Parser) 🔧
                          │
F007 (WAL) ──────────────▶ F008 (Checkpointing)
                          │
F009 (Event Time) ───────▶ F010 (Watermarks) ──▶ F012 (Late Data)
                                              ──▶ F011 (EMIT)

Phase 1.5 (SQL Parser Production - F006B) ✅ COMPLETE:
┌─────────────────────────────────────────────────────────────────┐
│   F006 ──▶ Phase1 (CREATE SOURCE/SINK) ✅                       │
│                │                                                │
│                ├──▶ Phase2 (Windows) ✅ ──▶ Phase3 (EMIT) ✅    │
│                │                                │               │
│                └──▶ Phase4 (Joins) ✅ ──────────┤               │
│                                                 ▼               │
│                                          Phase5 (Planner) ✅    │
│                                                 │               │
│   Configures: F004, F016, F019, F020 ◀──────────┘               │
│                                                                 │
│   Output: parser/, planner/, translator/ modules (129 tests)    │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
DataFusion Integration (F005B):
┌─────────────────────────────────────────────────────────────────┐
│   F005 (Basic) ──▶ F005B (Advanced)                             │
│        ✅              📝                                       │
│                         │                                       │
│   F006B (Parser) ──────▶├──▶ Window UDFs (TUMBLE/HOP/SESSION)   │
│        ✅               │                                       │
│                         ├──▶ WATERMARK UDF                      │
│                         │                                       │
│                         └──▶ LogicalPlan from StreamingStatement│
│                                     │                           │
│   End-to-end SQL execution ◀────────┘                           │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
Phase 2:
F001 ──▶ F013 (Thread-per-Core) ──▶ F014 (SPSC) ──▶ F015 (CPU Pinning)
F004 ──▶ F016 (Sliding) ──▶ F017 (Session) ──▶ F018 (Hopping)
F003 ──▶ F019 (Stream Joins) ──▶ F020 (Lookup) ──▶ F021 (Temporal)
                    │
                    ├──▶ F056 (ASOF Joins) ◀── Financial/TimeSeries
                    └──▶ F057 (Join Optimizations) ◀── Research 2025-2026
F007 + F013 ──▶ F062 (Per-Core WAL) ──┐
                                      │
F008 ──▶ F022 (Incremental) ◀─────────┘ ──▶ F023 (Exactly-Once) ──▶ F024 (2PC)
                                                    ▲
Emit Patterns (Phase 2 - NEW):                      │
┌────────────────────────────────────────────────────┼─────────────────────┐
│ F011 (EMIT Clause)                                 │                     │
│      │                                             │                     │
│      └──▶ F011B (Extension) ──────────────────────┘                     │
│               │     OnWindowClose, Changelog, Final                     │
│               │                                                         │
│      F063 (Changelog/Retraction) ──────────────────────┐                │
│               │     Z-set weights, CDC envelope        │                │
│               │                                        ▼                │
│               └──────────────────────────────▶ F060 (Cascading MVs)     │
└─────────────────────────────────────────────────────────────────────────┘

Checkpoint Architecture (Phase 2):
┌───────────────────────────────────────────────────────────────────────────┐
│ Ring 0: Changelog ──▶ Ring 1: Per-Core WAL ──▶ RocksDB ──▶ Checkpoint    │
│                                                                           │
│ F002 (mmap) + F063 (ChangelogBuffer) ──▶ F062 (Per-Core WAL) ──▶ F022    │
└───────────────────────────────────────────────────────────────────────────┘

Financial Analytics (Phase 2):
F004 (Tumbling) ──▶ F059 (FIRST/LAST) ──▶ F060 (Cascading MVs) ◀── OHLC Bars
                                              │
                                              ▼
                                    F061 (Historical Backfill) [Phase 3]

Watermark Evolution (Phase 2 - NEW):
┌───────────────────────────────────────────────────────────────────────────┐
│ F010 (Watermarks) - Phase 1 Foundation                                    │
│      │  • BoundedOutOfOrderness, Ascending, Periodic, Punctuated         │
│      │  • WatermarkTracker (multi-source minimum)                        │
│      │  • Idle detection, MeteredGenerator                               │
│      │                                                                    │
│      ├──▶ F064 (Per-Partition) ──┬──▶ F025 (Kafka Source)                │
│      │        • Per-partition tracking                                   │
│      │        • Thread-per-core integration                              │
│      │                           │                                        │
│      │                           └──▶ F065 (Keyed Watermarks)            │
│      │                                   • Per-key tracking              │
│      │                                   • 99%+ accuracy                 │
│      │                                                                    │
│      └──▶ F066 (Alignment Groups) ──▶ F019 (Stream Joins)                │
│               • Bounded drift                                            │
│               • Pause fast sources                                       │
└───────────────────────────────────────────────────────────────────────────┘

Thread-Per-Core Advanced (Phase 2 - NEW):
┌───────────────────────────────────────────────────────────────────────────┐
│ F013 (Thread-Per-Core) - Foundation ✅                                    │
│      │  • SPSC queues (F014) ✅                                          │
│      │  • CPU pinning (F015) ✅                                          │
│      │  • Credit-based backpressure ✅                                   │
│      │                                                                    │
│      ├──▶ F067 (io_uring Advanced) ──┬──▶ F069 (Three-Ring I/O)         │
│      │        • SQPOLL mode           │        • Latency/Main/Poll rings  │
│      │        • Registered buffers    │        • Eventfd wake-up         │
│      │        • IOPOLL for NVMe       │                                  │
│      │                                │                                  │
│      ├──▶ F068 (NUMA Awareness) ─────┴──▶ Production Deployment          │
│      │        • Per-core NUMA-local allocation                           │
│      │        • Interleaved for shared data                              │
│      │                                                                    │
│      ├──▶ F070 (Task Budget) ──────────▶ Latency SLA Enforcement         │
│      │        • Ring 0: 500ns budget                                     │
│      │        • Ring 1: 1ms budget + yielding                            │
│      │                                                                    │
│      ├──▶ F071 (Zero-Alloc) ───────────▶ Hot Path Verification           │
│      │        │ • Debug allocator detector                               │
│      │        │ • CI enforcement                                         │
│      │        │                                                          │
│      │        └──▶ F073 (Zero-Alloc Polling) ──▶ Allocation-Free Poll    │
│      │                 • Pre-allocated buffers                           │
│      │                 • Callback-based APIs                             │
│      │                                                                    │
│      └──▶ F072 (XDP) [P2] ─────────────▶ Wire-speed filtering            │
│               • 26M packets/sec                                          │
│               • CPU steering by partition                                │
└───────────────────────────────────────────────────────────────────────────┘

Phase 3 (blocked by F006B for DDL parsing):
F006B ──▶ F025-F034 (Connectors need CREATE SOURCE/SINK)
F013 + F019 ──▶ F058 (Async State Access) ◀── Flink 2.0 Innovation
F060 + F031/F032 ──▶ F061 (Historical Backfill) ◀── Live+Historical Unification
F063 ──▶ F027/F028 (CDC Connectors need changelog format)
F034 + F023 + F063 ──▶ F027B (PostgreSQL Sink: COPY BINARY + upsert + co-transactional exactly-once)
```

---

## Gap Summary by Priority

### P0 - Critical (Blocks Phase 2) - ✅ ALL COMPLETE

| Gap | Feature | Status | Notes |
|-----|---------|--------|-------|
| ~~WAL uses fsync not fdatasync~~ | F007 | ✅ Fixed | `sync_data()` |
| ~~No CRC32 checksum in WAL~~ | F007 | ✅ Fixed | CRC32C hardware accelerated |
| ~~No torn write detection~~ | F007 | ✅ Fixed | `WalReadResult::TornWrite` |
| ~~Watermark not persisted~~ | F010 | ✅ Fixed | In WAL + checkpoint |
| ~~No recovery integration test~~ | F007/F008 | ✅ Fixed | 6 tests |

### P0 - Critical (Blocks Phase 3)

| Gap | Feature | Impact | Fix |
|-----|---------|--------|-----|
| **SQL parser is POC only** | F006 | Connectors need DDL parsing | **F006B** (2-3 weeks) |

### P0 - Critical (Thread-Per-Core Research - 2026)

> From [Thread-Per-Core 2026 Research](../research/laminardb-thread-per-core-2026-research.md)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| **Basic io_uring only** | F067 | TU Munich 2024 | **NEW SPEC (P0)** - 2.05x improvement |
| **No NUMA awareness** | F068 | Multi-socket research | **NEW SPEC (P0)** - 2-3x latency fix |
| ~~No allocation enforcement~~ | F071 | Hot path research | ✅ **Done** - Debug detector + CI |

### P1 - High (Thread-Per-Core Research - 2026)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| **Single I/O ring** | F069 | Seastar/Glommio | **NEW SPEC (P1)** - Latency ring priority |
| **No task budgeting** | F070 | Cooperative scheduling | **NEW SPEC (P1)** - Budget enforcement |

### P2 - Medium (Thread-Per-Core Research - 2026)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| No XDP steering | F072 | eBPF research | **NEW SPEC (P2)** - 26M packets/sec |
| No CXL tiering | - | Memory research | Future (hardware dependent) |

### P1 - High (Phase 2/3)

| Gap | Feature | Impact | Fix |
|-----|---------|--------|-----|
| ~~No per-core WAL~~ | F062 | Required for F013 | **NEW SPEC** |
| ~~Checkpoint blocks Ring 0~~ | F022 | Latency spikes | **UPDATED SPEC** |
| ~~No incremental checkpoints~~ | F022 | Large checkpoint size | **UPDATED SPEC** |
| No CoW mmap | F002 | Can't isolate snapshots | Phase 3 |
| ~~No io_uring~~ | ~~F001~~ | ~~Blocking I/O on hot path~~ | **F067** |

### P1 - High (Research Gaps - 2025-2026)

> From [Stream Joins Research Review](../research/laminardb-stream-joins-research-review-2026.md)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| ~~No ASOF joins~~ | F056 | DuckDB/Pinot 2025 | ✅ **Done** |
| **No CPU-friendly encoding** | F057 | RisingWave July 2025 | NEW SPEC |
| **No async state access** | F058 | Flink 2.0 VLDB 2025 | NEW SPEC (Phase 3) |
| Temporal join incomplete | F021 | RisingWave 2025 | UPDATED SPEC |

### P0/P1 - High (Financial Analytics Gaps)

> From [Time-Series Financial Research 2026](../research/laminardb-timeseries-financial-research-2026.md)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| **No FIRST_VALUE/LAST_VALUE** | F059 | OHLC = standard aggregates | **NEW SPEC (P0)** |
| **No cascading MVs** | F060 | Multi-resolution OHLC | NEW SPEC (P1) |
| No historical backfill | F061 | Live + historical unification | NEW SPEC (P2, Phase 3) |
| No SAMPLE BY syntax | - | QuestDB-style sugar | Not planned (low priority) |

### P0 - Critical (Emit Patterns Research - 2026)

> From [Emit Patterns Research 2026](../research/emit-patterns-research-2026.md)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| **EMIT ON WINDOW CLOSE** | F011B | RisingWave, Flink | **NEW SPEC (P0)** - Blocks F023 |
| **Changelog/Retraction** | F063 | DBSP/Feldera VLDB 2025 | **NEW SPEC (P0)** - Blocks F023, F060 |
| **EMIT CHANGES** | F011B | ksqlDB, Flink | Included in F011B |
| **EMIT FINAL** | F011B | Spark, RisingWave | Included in F011B |
| **CDC Envelope Format** | F063 | Debezium standard | Included in F063 |

### P1 - High (Watermark Research Gaps - 2026)

> From [Watermark Generator Research 2026](../research/watermark-generator-research-2026.md)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| **No keyed watermarks** | F065 | ScienceDirect March 2025 | **NEW SPEC (P1)** - 99%+ accuracy |
| **No per-partition tracking** | F064 | Flink best practices | **NEW SPEC (P1)** - Kafka integration |
| **No alignment groups** | F066 | Flink 1.17+ | **NEW SPEC (P2)** - Prevents state growth |

### P2 - Medium (Phase 2+)

| Gap | Feature | Impact |
|-----|---------|--------|
| Prefix scan O(n) | F003 | Slow for large state |
| ~~No retractions~~ | ~~F012~~ | ~~Required for joins~~ | **F063 addresses this** |
| No madvise hints | F002 | Suboptimal TLB usage |
| Multi-way join optimization | - | Static join order, no adaptive |
| ~~DBSP incrementalization~~ | - | ~~No formal Z-set~~ | **F063 adds Z-set foundation** |
| Watermark alignment groups | F066 | Join state growth | NEW SPEC (P2) |
