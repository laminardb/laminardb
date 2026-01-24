# Feature Index

## Overview

| Phase | Total | Draft | In Progress | Hardening | Done |
|-------|-------|-------|-------------|-----------|------|
| Phase 1 | 12 | 0 | 0 | 1 | 11 |
| Phase 1.5 | 1 | 1 | 0 | 0 | 0 |
| Phase 2 | 14 | 7 | 0 | 0 | 7 |
| Phase 3 | 11 | 11 | 0 | 0 | 0 |
| Phase 4 | 11 | 11 | 0 | 0 | 0 |
| Phase 5 | 10 | 10 | 0 | 0 | 0 |
| **Total** | **59** | **40** | **0** | **1** | **18** |

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
| F003 | State Store Interface | P0 | ✅ | Prefix scan O(n) (P2) | [Link](phase-1/F003-state-store-interface.md) |
| F004 | Tumbling Windows | P0 | ✅ | None | [Link](phase-1/F004-tumbling-windows.md) |
| F005 | DataFusion Integration | P0 | ✅ | No EXPLAIN (P2) | [Link](phase-1/F005-datafusion-integration.md) |
| F006 | Basic SQL Parser | P0 | 🔧 | **POC only** (P1) | [Link](phase-1/F006-basic-sql-parser.md) |
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

> **Status**: 📝 Draft - Blocks Phase 3 connectors and advanced SQL features

| ID | Feature | Priority | Status | Effort | Spec |
|----|---------|----------|--------|--------|------|
| F006B | Production SQL Parser | P0 | 📝 | L (2-3 weeks) | [Link](phase-1/F006B-production-sql-parser.md) |

### F006B Implementation Phases

| Phase | Scope | Effort | Dependencies |
|-------|-------|--------|--------------|
| 1 | CREATE SOURCE/SINK parsing | 2-3 days | None |
| 2 | Window function extraction (TUMBLE/HOP/SESSION) | 3-4 days | Phase 1 |
| 3 | EMIT/Late Data integration | 2-3 days | Phase 2 |
| 4 | Join query parsing (stream-stream, lookup) | 3-4 days | Phase 1 |
| 5 | Query planner integration | 4-5 days | Phases 2, 3, 4 |
| 6 | Aggregator detection (COUNT/SUM/MIN/MAX/AVG) | 2-3 days | Phase 4 |

### Key Deliverables

- [ ] Fix hardcoded CREATE SOURCE/SINK (`parser_simple.rs:60-76`)
- [ ] Fix hardcoded window args (`window_rewriter.rs:88-100`)
- [ ] Implement `rewrite_select()` (`window_rewriter.rs:47-54`)
- [ ] Replace `todo!()` in planner (`planner/mod.rs:22-25`)
- [ ] New modules: `translator/`, `parser/join_parser.rs`, `parser/aggregation_parser.rs`
- [ ] 30+ new test cases

---

## Phase 2: Production Hardening

> **Status**: 🚧 In Progress (7/12 features complete)

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F013 | Thread-Per-Core Architecture | P0 | ✅ | [Link](phase-2/F013-thread-per-core.md) |
| F014 | SPSC Queue Communication | P0 | ✅ | [Link](phase-2/F014-spsc-queues.md) |
| F015 | CPU Pinning | P1 | ✅ | Included in F013 |
| F016 | Sliding Windows | P0 | ✅ | [Link](phase-2/F016-sliding-windows.md) |
| F017 | Session Windows | P1 | 📝 | [Link](phase-2/F017-session-windows.md) |
| F018 | Hopping Windows | P1 | ✅ | Same as F016 (sliding) |
| F019 | Stream-Stream Joins | P0 | ✅ | [Link](phase-2/F019-stream-stream-joins.md) |
| F020 | Lookup Joins | P0 | ✅ | [Link](phase-2/F020-lookup-joins.md) |
| F021 | Temporal Joins | P2 | 📝 | [Link](phase-2/F021-temporal-joins.md) |
| F022 | Incremental Checkpointing | P1 | 📝 | [Link](phase-2/F022-incremental-checkpointing.md) |
| F023 | Exactly-Once Sinks | P0 | 📝 | [Link](phase-2/F023-exactly-once-sinks.md) |
| F024 | Two-Phase Commit | P1 | 📝 | [Link](phase-2/F024-two-phase-commit.md) |
| F056 | ASOF Joins | P1 | 📝 | [Link](phase-2/F056-asof-joins.md) |
| F057 | Stream Join Optimizations | P1 | 📝 | [Link](phase-2/F057-stream-join-optimizations.md) |

### Phase 2 Join Research Gap Analysis

> Based on [Stream Joins Research Review 2026](../research/laminardb-stream-joins-research-review-2026.md)

| Gap | Source | Current | Target | Feature |
|-----|--------|---------|--------|---------|
| ASOF Joins | DuckDB/Pinot 2025 | ❌ Missing | Full support | F056 |
| CPU-Friendly Encoding | RisingWave July 2025 | ❌ Missing | 50% perf gain | F057 |
| Asymmetric Compaction | Epsio 2025 | ❌ Missing | Reduced overhead | F057 |
| Temporal Join (versioned) | RisingWave 2025 | 📝 Draft | Full impl | F021 |
| Async State Access | Flink 2.0 | ❌ Missing | Phase 3 | F058 |

---

## Phase 3: Connectors & Integration

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F025 | Kafka Source Connector | P0 | 📝 | [Link](phase-3/F025-kafka-source.md) |
| F026 | Kafka Sink Connector | P0 | 📝 | [Link](phase-3/F026-kafka-sink.md) |
| F027 | PostgreSQL CDC Source | P0 | 📝 | [Link](phase-3/F027-postgres-cdc.md) |
| F028 | MySQL CDC Source | P1 | 📝 | [Link](phase-3/F028-mysql-cdc.md) |
| F029 | MongoDB CDC Source | P2 | 📝 | [Link](phase-3/F029-mongodb-cdc.md) |
| F030 | Redis Lookup Table | P1 | 📝 | [Link](phase-3/F030-redis-lookup.md) |
| F031 | Delta Lake Sink | P0 | 📝 | [Link](phase-3/F031-delta-lake-sink.md) |
| F032 | Iceberg Sink | P1 | 📝 | [Link](phase-3/F032-iceberg-sink.md) |
| F033 | Parquet File Source | P2 | 📝 | [Link](phase-3/F033-parquet-source.md) |
| F034 | Connector SDK | P1 | 📝 | [Link](phase-3/F034-connector-sdk.md) |
| F058 | Async State Access | P1 | 📝 | [Link](phase-3/F058-async-state-access.md) |

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
F005 (DataFusion) ───────▶ F006 (SQL Parser) ⚠️ POC
                          │
F007 (WAL) ──────────────▶ F008 (Checkpointing)
                          │
F009 (Event Time) ───────▶ F010 (Watermarks) ──▶ F012 (Late Data)
                                              ──▶ F011 (EMIT)

Phase 1.5 (SQL Parser Production - F006B):
┌─────────────────────────────────────────────────────────────────┐
│   F006 ──▶ Phase1 (CREATE SOURCE/SINK)                          │
│                │                                                │
│                ├──▶ Phase2 (Windows) ──▶ Phase3 (EMIT)          │
│                │                              │                 │
│                └──▶ Phase4 (Joins) ───────────┤                 │
│                                               ▼                 │
│                                        Phase5 (Planner)         │
│                                               │                 │
│   Configures: F004, F016, F019, F020 ◀────────┘                 │
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
F008 ──▶ F022 (Incremental) ──▶ F023 (Exactly-Once) ──▶ F024 (2PC)

Phase 3 (blocked by F006B for DDL parsing):
F006B ──▶ F025-F034 (Connectors need CREATE SOURCE/SINK)
F013 + F019 ──▶ F058 (Async State Access) ◀── Flink 2.0 Innovation
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

### P1 - High (Phase 2/3)

| Gap | Feature | Impact |
|-----|---------|--------|
| No per-core WAL | F007 | Required for F013 |
| Checkpoint blocks Ring 0 | F008 | Latency spikes |
| No CoW mmap | F002 | Can't isolate snapshots |
| No io_uring | F001 | Blocking I/O on hot path |

### P1 - High (Research Gaps - 2025-2026)

> From [Stream Joins Research Review](../research/laminardb-stream-joins-research-review-2026.md)

| Gap | Feature | Source | Fix |
|-----|---------|--------|-----|
| **No ASOF joins** | F056 | DuckDB/Pinot 2025 | NEW SPEC |
| **No CPU-friendly encoding** | F057 | RisingWave July 2025 | NEW SPEC |
| **No async state access** | F058 | Flink 2.0 VLDB 2025 | NEW SPEC (Phase 3) |
| Temporal join incomplete | F021 | RisingWave 2025 | UPDATED SPEC |

### P2 - Medium (Phase 2+)

| Gap | Feature | Impact |
|-----|---------|--------|
| Prefix scan O(n) | F003 | Slow for large state |
| No incremental checkpoints | F008 | Large checkpoint overhead |
| No retractions | F012 | Required for joins |
| No madvise hints | F002 | Suboptimal TLB usage |
| Multi-way join optimization | - | Static join order, no adaptive |
| DBSP incrementalization | - | No formal Z-set, no auto-incr |
