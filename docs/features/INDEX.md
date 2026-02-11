# Feature Index

## Overview

| Phase | Total | Draft | In Progress | Hardening | Done |
|-------|-------|-------|-------------|-----------|------|
| Phase 1 | 12 | 0 | 0 | 0 | 12 |
| Phase 1.5 | 1 | 0 | 0 | 0 | 1 |
| Phase 2 | 38 | 0 | 0 | 0 | 38 |
| Phase 2.5 | 12 | 0 | 0 | 0 | 12 |
| Phase 3 | 76 | 9 | 0 | 0 | 67 |
| Phase 4 | 11 | 11 | 0 | 0 | 0 |
| Phase 5 | 10 | 10 | 0 | 0 | 0 |
| **Total** | **160** | **23** | **0** | **0** | **130** |

## Status Legend

- 📝 Draft - Specification written, not started
- 🚧 In Progress - Active development
- 🔧 Hardening - Functional but has gaps to fix
- ✅ Done - Complete and merged
- ⏸️ Paused - On hold
- ❌ Cancelled - Will not implement

**See also:** [DEPENDENCIES.md](DEPENDENCIES.md) for feature dependency graph.

---

## Phase 1: Core Engine ✅

> **Status**: Complete. See [PHASE1_AUDIT.md](../PHASE1_AUDIT.md) for details.

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F001 | Core Reactor Event Loop | ✅ | [Link](phase-1/F001-core-reactor-event-loop.md) |
| F002 | Memory-Mapped State Store | ✅ | [Link](phase-1/F002-memory-mapped-state-store.md) |
| F003 | State Store Interface | ✅ | [Link](phase-1/F003-state-store-interface.md) |
| F004 | Tumbling Windows | ✅ | [Link](phase-1/F004-tumbling-windows.md) |
| F005 | DataFusion Integration | ✅ | [Link](phase-1/F005-datafusion-integration.md) |
| F006 | Basic SQL Parser | ✅ | [Link](phase-1/F006-basic-sql-parser.md) |
| F007 | Write-Ahead Log | ✅ | [Link](phase-1/F007-write-ahead-log.md) |
| F008 | Basic Checkpointing | 🔧 | [Link](phase-1/F008-basic-checkpointing.md) |
| F009 | Event Time Processing | ✅ | [Link](phase-1/F009-event-time-processing.md) |
| F010 | Watermarks | ✅ | [Link](phase-1/F010-watermarks.md) |
| F011 | EMIT Clause | ✅ | [Link](phase-1/F011-emit-clause.md) |
| F012 | Late Data Handling | ✅ | [Link](phase-1/F012-late-data-handling.md) |

---

## Phase 1.5: SQL Parser ✅

> **Status**: Complete. 129 tests.

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F006B | Production SQL Parser | ✅ | [Link](phase-1/F006B-production-sql-parser.md) |

---

## Phase 2: Production Hardening

> **Status**: Complete. All 38 features done.

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F013 | Thread-Per-Core Architecture | ✅ | [Link](phase-2/F013-thread-per-core.md) |
| F014 | SPSC Queue Communication | ✅ | [Link](phase-2/F014-spsc-queues.md) |
| F015 | CPU Pinning | ✅ | Included in F013 |
| F016 | Sliding Windows | ✅ | [Link](phase-2/F016-sliding-windows.md) |
| F017 | Session Windows | ✅ | [Link](phase-2/F017-session-windows.md) |
| F017B | Session State Refactoring | ✅ | [Link](phase-2/F017B-session-state-refactoring.md) |
| F017C | Session Merging & Overlap Detection | ✅ | [Link](phase-2/F017C-session-merging.md) |
| F017D | Session Emit Strategies | ✅ | [Link](phase-2/F017D-session-emit-strategies.md) |
| F017E | Watermark Closure & Timer Persistence | ✅ | [Link](phase-2/F017E-watermark-closure-timers.md) |
| F018 | Hopping Windows | ✅ | Same as F016 |
| F019 | Stream-Stream Joins | ✅ | [Link](phase-2/F019-stream-stream-joins.md) |
| F020 | Lookup Joins | ✅ | [Link](phase-2/F020-lookup-joins.md) |
| F021 | Temporal Joins | ✅ | [Link](phase-2/F021-temporal-joins.md) |
| F022 | Incremental Checkpointing | ✅ | [Link](phase-2/F022-incremental-checkpointing.md) |
| F023 | Exactly-Once Sinks | ✅ | [Link](phase-2/F023-exactly-once-sinks.md) |
| F024 | Two-Phase Commit | ✅ | [Link](phase-2/F024-two-phase-commit.md) |
| F056 | ASOF Joins | ✅ | [Link](phase-2/F056-asof-joins.md) |
| F057 | Stream Join Optimizations | ✅ | [Link](phase-2/F057-stream-join-optimizations.md) |
| F059 | FIRST/LAST Value Aggregates | ✅ | [Link](phase-2/F059-first-last-aggregates.md) |
| F060 | Cascading Materialized Views | ✅ | [Link](phase-2/F060-cascading-materialized-views.md) |
| F062 | Per-Core WAL Segments | ✅ | [Link](phase-2/F062-per-core-wal.md) |
| F011B | EMIT Clause Extension | ✅ | [Link](phase-2/F011B-emit-clause-extension.md) |
| F063 | Changelog/Retraction (Z-Sets) | ✅ | [Link](phase-2/F063-changelog-retraction.md) |
| F064 | Per-Partition Watermarks | ✅ | [Link](phase-2/F064-per-partition-watermarks.md) |
| F065 | Keyed Watermarks | ✅ | [Link](phase-2/F065-keyed-watermarks.md) |
| F066 | Watermark Alignment Groups | ✅ | [Link](phase-2/F066-watermark-alignment-groups.md) |
| F067 | io_uring Advanced Optimization | ✅ | [Link](phase-2/F067-io-uring-optimization.md) |
| F068 | NUMA-Aware Memory Allocation | ✅ | [Link](phase-2/F068-numa-aware-memory.md) |
| F069 | Three-Ring I/O Architecture | ✅ | [Link](phase-2/F069-three-ring-io.md) |
| F070 | Task Budget Enforcement | ✅ | [Link](phase-2/F070-task-budget-enforcement.md) |
| F071 | Zero-Allocation Enforcement | ✅ | [Link](phase-2/F071-zero-allocation-enforcement.md) |
| F072 | XDP/eBPF Network Optimization | ✅ | [Link](phase-2/F072-xdp-network-optimization.md) |
| F073 | Zero-Allocation Polling | ✅ | [Link](phase-2/F073-zero-allocation-polling.md) |
| F005B | Advanced DataFusion Integration | ✅ | [Link](phase-2/F005B-advanced-datafusion-integration.md) |
| F074 | Composite Aggregator & f64 Type Support | ✅ | [Link](phase-2/F074-composite-aggregator.md) |
| F075 | DataFusion Aggregate Bridge | ✅ | [Link](phase-2/F075-datafusion-aggregate-bridge.md) |
| F076 | Retractable FIRST/LAST Accumulators | ✅ | [Link](phase-2/F076-retractable-first-last.md) |
| F077 | Extended Aggregation Parser | ✅ | [Link](phase-2/F077-extended-aggregation-parser.md) |

---

## Phase 2.5: Plan Compiler & SQL Compiler Integration

> **Status**: 12/12 features complete (100%) ✅. Compiles DataFusion logical plans into zero-allocation JIT functions for Ring 0, with end-to-end SQL query execution integration.

See [Plan Compiler Index](plan-compiler/INDEX.md) for architecture details and [research](../research/plan-compiler-research-2026.md) for background.

### Plan Compiler Core (F078–F082) ✅

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F078 | Event Row Format | ✅ | [Link](plan-compiler/F078-event-row-format.md) |
| F079 | Compiled Expression Evaluator | ✅ | [Link](plan-compiler/F079-compiled-expression-evaluator.md) |
| F080 | Plan Compiler Core | ✅ | [Link](plan-compiler/F080-plan-compiler-core.md) |
| F081 | Ring 0/Ring 1 Pipeline Bridge | ✅ | [Link](plan-compiler/F081-ring0-ring1-pipeline-bridge.md) |
| F082 | Streaming Query Lifecycle | ✅ | [Link](plan-compiler/F082-streaming-query-lifecycle.md) |

### SQL Compiler Integration (F083–F089) ✅

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F083 | Batch Row Reader | ✅ | - |
| F084 | SQL Compiler Orchestrator | ✅ | - |
| F085 | LaminarDB JIT Query Execution | ✅ | - |
| F086 | Adaptive Compilation Warmup | ✅ | - |
| F087 | Compiled Stateful Pipeline Bridge | ✅ | - |
| F088 | Schema-Aware Event Time Extraction | ✅ | - |
| F089 | Compilation Metrics & Observability | ✅ | - |

---

## Phase 3: Connectors & Integration

> **Status**: 67/76 features complete (88%)

### Streaming API ✅

See [Streaming API Index](phase-3/streaming/INDEX.md).

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-STREAM-001 | Ring Buffer | ✅ | [Link](phase-3/streaming/F-STREAM-001-ring-buffer.md) |
| F-STREAM-002 | SPSC Channel | ✅ | [Link](phase-3/streaming/F-STREAM-002-spsc-channel.md) |
| F-STREAM-003 | MPSC Auto-Upgrade | ✅ | [Link](phase-3/streaming/F-STREAM-003-mpsc-upgrade.md) |
| F-STREAM-004 | Source | ✅ | [Link](phase-3/streaming/F-STREAM-004-source.md) |
| F-STREAM-005 | Sink | ✅ | [Link](phase-3/streaming/F-STREAM-005-sink.md) |
| F-STREAM-006 | Subscription | ✅ | [Link](phase-3/streaming/F-STREAM-006-subscription.md) |
| F-STREAM-007 | SQL DDL | ✅ | [Link](phase-3/streaming/F-STREAM-007-sql-ddl.md) |
| F-STREAM-010 | Broadcast Channel | ✅ | [Link](phase-3/streaming/F-STREAM-010-broadcast-channel.md) |
| F-STREAM-013 | Checkpointing | ✅ | [Link](phase-3/streaming/F-STREAM-013-checkpointing.md) |

### DAG Pipeline ✅

See [DAG Pipeline Index](phase-3/dag/INDEX.md).

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-DAG-001 | Core DAG Topology | ✅ | [Link](phase-3/dag/F-DAG-001-core-topology.md) |
| F-DAG-002 | Multicast & Routing | ✅ | [Link](phase-3/dag/F-DAG-002-multicast-routing.md) |
| F-DAG-003 | DAG Executor | ✅ | [Link](phase-3/dag/F-DAG-003-dag-executor.md) |
| F-DAG-004 | DAG Checkpointing | ✅ | [Link](phase-3/dag/F-DAG-004-dag-checkpointing.md) |
| F-DAG-005 | SQL & MV Integration | ✅ | [Link](phase-3/dag/F-DAG-005-sql-mv-integration.md) |
| F-DAG-006 | Connector Bridge | ✅ | [Link](phase-3/dag/F-DAG-006-connector-bridge.md) |
| F-DAG-007 | Performance Validation | ✅ | [Link](phase-3/dag/F-DAG-007-performance-validation.md) |

### Reactive Subscriptions ✅

See [Subscription Index](phase-3/subscription/INDEX.md).

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-SUB-001 | ChangeEvent Types | ✅ | [Link](phase-3/subscription/F-SUB-001-change-event-types.md) |
| F-SUB-002 | Notification Slot (Ring 0) | ✅ | [Link](phase-3/subscription/F-SUB-002-notification-slot.md) |
| F-SUB-003 | Subscription Registry | ✅ | [Link](phase-3/subscription/F-SUB-003-subscription-registry.md) |
| F-SUB-004 | Subscription Dispatcher (Ring 1) | ✅ | [Link](phase-3/subscription/F-SUB-004-subscription-dispatcher.md) |
| F-SUB-005 | Push Subscription API | ✅ | [Link](phase-3/subscription/F-SUB-005-push-subscription-api.md) |
| F-SUB-006 | Callback Subscriptions | ✅ | [Link](phase-3/subscription/F-SUB-006-callback-subscriptions.md) |
| F-SUB-007 | Stream Subscriptions | ✅ | [Link](phase-3/subscription/F-SUB-007-stream-subscriptions.md) |
| F-SUB-008 | Backpressure & Filtering | ✅ | [Link](phase-3/subscription/F-SUB-008-backpressure-filtering.md) |

### Cloud Storage Infrastructure ✅

See [Cloud Storage Index](phase-3/cloud/INDEX.md).

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-CLOUD-001 | Storage Credential Resolver | ✅ | [Link](phase-3/cloud/F-CLOUD-001-credential-resolver.md) |
| F-CLOUD-002 | Cloud Config Validation | ✅ | [Link](phase-3/cloud/F-CLOUD-002-config-validation.md) |
| F-CLOUD-003 | Secret Masking & Safe Logging | ✅ | [Link](phase-3/cloud/F-CLOUD-003-secret-masking.md) |

### External Connectors

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F025 | Kafka Source Connector | ✅ | [Link](phase-3/F025-kafka-source.md) |
| F026 | Kafka Sink Connector | ✅ | [Link](phase-3/F026-kafka-sink.md) |
| F027 | PostgreSQL CDC Source | ✅ | [Link](phase-3/F027-postgres-cdc.md) |
| F027B | PostgreSQL Sink | ✅ | [Link](phase-3/F027B-postgres-sink.md) |
| F028 | MySQL CDC Source | ✅ | [Link](phase-3/F028-mysql-cdc.md) |
| F028A | MySQL CDC I/O Integration | ✅ | [Link](phase-3/F028A-mysql-cdc-io.md) |
| F029 | MongoDB CDC Source | 📝 | [Link](phase-3/F029-mongodb-cdc.md) |
| F030 | Redis Lookup Table | 📝 | [Link](phase-3/F030-redis-lookup.md) |
| F031 | Delta Lake Sink | ✅ | [Link](phase-3/F031-delta-lake-sink.md) |
| F031A | Delta Lake I/O Integration | ✅ | [Link](phase-3/F031A-delta-lake-io.md) |
| F031B | Delta Lake Recovery | 📝 | [Link](phase-3/F031B-delta-lake-recovery.md) |
| F031C | Delta Lake Compaction | 📝 | [Link](phase-3/F031C-delta-lake-compaction.md) |
| F031D | Delta Lake Schema Evolution | 📝 | [Link](phase-3/F031D-delta-lake-schema-evolution.md) |
| F032 | Iceberg Sink | ✅ | [Link](phase-3/F032-iceberg-sink.md) |
| F032A | Iceberg I/O Integration | 📝 | [Link](phase-3/F032A-iceberg-io.md) |
| F033 | Parquet File Source | 📝 | [Link](phase-3/F033-parquet-source.md) |
| F034 | Connector SDK | ✅ | [Link](phase-3/F034-connector-sdk.md) |
| F058 | Async State Access | 📝 | [Link](phase-3/F058-async-state-access.md) |
| F061 | Historical Backfill | 📝 | [Link](phase-3/F061-historical-backfill.md) |

### SQL Extensions

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-SQL-001 | ASOF JOIN SQL Support | ✅ | [Link](phase-3/F-SQL-001-asof-join-sql.md) |
| F-SQL-002 | LAG/LEAD Window Functions | ✅ | [Link](phase-3/F-SQL-002-lag-lead-functions.md) |
| F-SQL-003 | ROW_NUMBER/RANK/DENSE_RANK | ✅ | [Link](phase-3/F-SQL-003-ranking-functions.md) |
| F-SQL-004 | HAVING Clause Execution | ✅ | [Link](phase-3/F-SQL-004-having-clause.md) |
| F-SQL-005 | Multi-Way JOIN Support | ✅ | [Link](phase-3/F-SQL-005-multi-way-joins.md) |
| F-SQL-006 | Window Frame (ROWS BETWEEN) | ✅ | [Link](phase-3/F-SQL-006-window-frames.md) |

### Connector Infrastructure

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-CONN-001 | Checkpoint Recovery Wiring | ✅ | [Link](phase-3/F-CONN-001-checkpoint-recovery-wiring.md) |
| F-CONN-002 | Reference Table Support | ✅ | [Link](phase-3/F-CONN-002-reference-tables.md) |
| F-CONN-002B | Connector-Backed Table Population | ✅ | [Link](phase-3/F-CONN-002B-table-source-population.md) |
| F-CONN-002C | PARTIAL Cache Mode & Xor Filter | ✅ | [Link](phase-3/F-CONN-002C-partial-cache-xor-filter.md) |
| F-CONN-002D | RocksDB-Backed Persistent Table Store | ✅ | [Link](phase-3/F-CONN-002D-rocksdb-table-store.md) |
| F-CONN-003 | Avro Serialization Hardening | ✅ | [Link](phase-3/F-CONN-003-avro-hardening.md) |

### Pipeline Observability

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-OBS-001 | Pipeline Observability API | ✅ | [Link](phase-3/F-OBS-001-pipeline-observability.md) |

### Production Demo

See [Demo Index](phase-3/demo/INDEX.md).

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-DEMO-001 | Market Data Pipeline | ✅ | [Link](phase-3/demo/F-DEMO-001-market-data-pipeline.md) |
| F-DEMO-002 | Ratatui TUI Dashboard | ✅ | [Link](phase-3/demo/F-DEMO-002-ratatui-tui.md) |
| F-DEMO-003 | Kafka Integration & Docker | ✅ | [Link](phase-3/demo/F-DEMO-003-kafka-docker.md) |
| F-DEMO-004 | DAG Pipeline Visualization | ✅ | [Link](phase-3/demo/F-DEMO-004-dag-visualization.md) |
| F-DEMO-005 | Tumbling Windows & ASOF JOIN Demo | ✅ | - |
| F-DEMO-006 | Kafka Mode TUI Dashboard | ✅ | [Link](phase-3/F-DEMO-006-kafka-tui.md) |

### Unified Checkpoint System

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-CKP-001 | Checkpoint Manifest & Store | ✅ | - |
| F-CKP-002 | Two-Phase Sink Protocol | ✅ | - |
| F-CKP-003 | Checkpoint Coordinator | ✅ | - |
| F-CKP-004 | Operator State Persistence | ✅ | - |
| F-CKP-005 | Changelog Buffer Wiring | ✅ | - |
| F-CKP-006 | WAL Checkpoint Coordination | ✅ | - |
| F-CKP-007 | Unified Recovery Manager | ✅ | - |
| F-CKP-008 | End-to-End Recovery Tests | ✅ | - |
| F-CKP-009 | Checkpoint Observability | ✅ | - |

### FFI & Language Bindings

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-FFI-001 | API Module (Core FFI Surface) | ✅ | [Link](phase-3/F-FFI-001-api-module.md) |
| F-FFI-002 | C Header Generation | ✅ | [Link](phase-3/F-FFI-002-c-headers.md) |
| F-FFI-003 | Arrow C Data Interface | ✅ | [Link](phase-3/F-FFI-003-arrow-c-data-interface.md) |
| F-FFI-004 | Async FFI Callbacks | ✅ | [Link](phase-3/F-FFI-004-async-callbacks.md) |

---

## Phase 4: Enterprise & Security

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F035 | Authentication Framework | 📝 | [Link](phase-4/F035-authn-framework.md) |
| F036 | JWT Authentication | 📝 | [Link](phase-4/F036-jwt-auth.md) |
| F037 | mTLS Authentication | 📝 | [Link](phase-4/F037-mtls-auth.md) |
| F038 | LDAP Integration | 📝 | [Link](phase-4/F038-ldap-integration.md) |
| F039 | Role-Based Access Control | 📝 | [Link](phase-4/F039-rbac.md) |
| F040 | Attribute-Based Access Control | 📝 | [Link](phase-4/F040-abac.md) |
| F041 | Row-Level Security | 📝 | [Link](phase-4/F041-row-level-security.md) |
| F042 | Column-Level Security | 📝 | [Link](phase-4/F042-column-level-security.md) |
| F043 | Audit Logging | 📝 | [Link](phase-4/F043-audit-logging.md) |
| F044 | Encryption at Rest | 📝 | [Link](phase-4/F044-encryption-at-rest.md) |
| F045 | Key Management | 📝 | [Link](phase-4/F045-key-management.md) |

---

## Phase 5: Admin & Observability

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F046 | Admin REST API | 📝 | [Link](phase-5/F046-admin-api.md) |
| F047 | Web Dashboard | 📝 | [Link](phase-5/F047-web-dashboard.md) |
| F048 | Real-Time Metrics | 📝 | [Link](phase-5/F048-realtime-metrics.md) |
| F049 | SQL Query Console | 📝 | [Link](phase-5/F049-sql-console.md) |
| F050 | Prometheus Export | 📝 | [Link](phase-5/F050-prometheus-export.md) |
| F051 | OpenTelemetry Tracing | 📝 | [Link](phase-5/F051-otel-tracing.md) |
| F052 | Health Check Endpoints | 📝 | [Link](phase-5/F052-health-checks.md) |
| F053 | Alerting Integration | 📝 | [Link](phase-5/F053-alerting.md) |
| F054 | Configuration Management | 📝 | [Link](phase-5/F054-config-management.md) |
| F055 | CLI Tools | 📝 | [Link](phase-5/F055-cli-tools.md) |

---

## Active Gaps

Remaining work for Phase 3:

| Gap | Feature | Priority | Notes |
|-----|---------|----------|-------|
| Delta Lake I/O | F031A | P0 | ✅ **COMPLETE** (2026-02-05) - 13 integration tests |
| Delta Lake Advanced | F031B-D | P1 | Recovery, Compaction, Schema Evolution |
| MySQL CDC I/O | F028A | P1 | ✅ **COMPLETE** (2026-02-06) - 21 new tests, mysql_async binlog I/O |
| Iceberg I/O | F032A | P1 | Blocked by iceberg-datafusion 0.9.0 (needs DF 52.0 compat) |

For historical gap analysis, see the [research documents](../research/).
