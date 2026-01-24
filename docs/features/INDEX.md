# Feature Index

## Overview

| Phase | Total | Draft | In Progress | Done |
|-------|-------|-------|-------------|------|
| Phase 1 | 12 | 3 | 0 | 9 |
| Phase 2 | 12 | 12 | 0 | 0 |
| Phase 3 | 10 | 10 | 0 | 0 |
| Phase 4 | 11 | 11 | 0 | 0 |
| Phase 5 | 10 | 10 | 0 | 0 |
| **Total** | **55** | **46** | **0** | **9** |

## Status Legend

- 📝 Draft - Specification written, not started
- 🚧 In Progress - Active development
- ✅ Done - Complete and merged
- ⏸️ Paused - On hold
- ❌ Cancelled - Will not implement

---

## Phase 1: Core Engine

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F001 | Core Reactor Event Loop | P0 | ✅ | [Link](phase-1/F001-core-reactor-event-loop.md) |
| F002 | Memory-Mapped State Store | P0 | ✅ | [Link](phase-1/F002-memory-mapped-state-store.md) |
| F003 | State Store Interface | P0 | ✅ | [Link](phase-1/F003-state-store-interface.md) |
| F004 | Tumbling Windows | P0 | ✅ | [Link](phase-1/F004-tumbling-windows.md) |
| F005 | DataFusion Integration | P0 | ✅ | [Link](phase-1/F005-datafusion-integration.md) |
| F006 | Basic SQL Parser | P0 | ✅ | [Link](phase-1/F006-basic-sql-parser.md) |
| F007 | Write-Ahead Log | P1 | ✅ | [Link](phase-1/F007-write-ahead-log.md) |
| F008 | Basic Checkpointing | P1 | ✅ | [Link](phase-1/F008-basic-checkpointing.md) |
| F009 | Event Time Processing | P1 | ✅ | [Link](phase-1/F009-event-time-processing.md) |
| F010 | Watermarks | P1 | 📝 | [Link](phase-1/F010-watermarks.md) |
| F011 | EMIT Clause | P2 | 📝 | [Link](phase-1/F011-emit-clause.md) |
| F012 | Late Data Handling | P2 | 📝 | [Link](phase-1/F012-late-data-handling.md) |

---

## Phase 2: Production Hardening

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F013 | Thread-Per-Core Architecture | P0 | 📝 | [Link](phase-2/F013-thread-per-core.md) |
| F014 | SPSC Queue Communication | P0 | 📝 | [Link](phase-2/F014-spsc-queues.md) |
| F015 | CPU Pinning | P1 | 📝 | [Link](phase-2/F015-cpu-pinning.md) |
| F016 | Sliding Windows | P0 | 📝 | [Link](phase-2/F016-sliding-windows.md) |
| F017 | Session Windows | P1 | 📝 | [Link](phase-2/F017-session-windows.md) |
| F018 | Hopping Windows | P1 | 📝 | [Link](phase-2/F018-hopping-windows.md) |
| F019 | Stream-Stream Joins | P0 | 📝 | [Link](phase-2/F019-stream-stream-joins.md) |
| F020 | Lookup Joins | P0 | 📝 | [Link](phase-2/F020-lookup-joins.md) |
| F021 | Temporal Joins | P2 | 📝 | [Link](phase-2/F021-temporal-joins.md) |
| F022 | Incremental Checkpointing | P1 | 📝 | [Link](phase-2/F022-incremental-checkpointing.md) |
| F023 | Exactly-Once Sinks | P0 | 📝 | [Link](phase-2/F023-exactly-once-sinks.md) |
| F024 | Two-Phase Commit | P1 | 📝 | [Link](phase-2/F024-two-phase-commit.md) |

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
F005 (DataFusion) ───────▶ F006 (SQL Parser)
                          │
F007 (WAL) ──────────────▶ F008 (Checkpointing)
                          │
F009 (Event Time) ───────▶ F010 (Watermarks) ──▶ F012 (Late Data)
                                              ──▶ F011 (EMIT)

Phase 2:
F001 ──▶ F013 (Thread-per-Core) ──▶ F014 (SPSC) ──▶ F015 (CPU Pinning)
F004 ──▶ F016 (Sliding) ──▶ F017 (Session) ──▶ F018 (Hopping)
F003 ──▶ F019 (Stream Joins) ──▶ F020 (Lookup) ──▶ F021 (Temporal)
F008 ──▶ F022 (Incremental) ──▶ F023 (Exactly-Once) ──▶ F024 (2PC)
```
