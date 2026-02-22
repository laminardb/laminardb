# Feature Index

## Overview

| Phase | Total | Draft | In Progress | Hardening | Done | Superseded |
|-------|-------|-------|-------------|-----------|------|------------|
| Phase 1 | 12 | 0 | 0 | 0 | 12 | 0 |
| Phase 1.5 | 1 | 0 | 0 | 0 | 1 | 0 |
| Phase 2 | 38 | 0 | 0 | 0 | 38 | 0 |
| Phase 2.5 | 12 | 0 | 0 | 0 | 12 | 0 |
| Phase 3 | 93 | 12 | 0 | 0 | 81 | 0 |
| Phase 4 | 11 | 11 | 0 | 0 | 0 | 0 |
| Phase 5 | 10 | 10 | 0 | 0 | 0 | 0 |
| Phase 6a | 29 | 0 | 0 | 0 | 27 | 2 |
| Phase 6b | 14 | 0 | 0 | 0 | 14 | 0 |
| Phase 6c | 10 | 9 | 0 | 0 | 0 | 1 |
| Perf Optimization | 12 | 12 | 0 | 0 | 0 | 0 |
| **Total** | **242** | **54** | **0** | **0** | **185** | **3** |

## Status Legend

- 📝 Draft - Specification written, not started
- 🚧 In Progress - Active development
- 🔧 Hardening - Functional but has gaps to fix
- ✅ Done - Complete and merged
- ⏸️ Paused - On hold
- ❌ Cancelled / Superseded - Will not implement or replaced by alternative approach

**See also:** [DEPENDENCIES.md](DEPENDENCIES.md) for feature dependency graph.

---

## Phase 1: Core Engine ✅

> **Status**: Complete.

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

> **Status**: 81/93 features complete (87%)

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
| F-SQL-007 | Multi-Partition Streaming Scans | ✅ | [Link](phase-3/F-SQL-007-multi-partition-scans.md) |

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

### Schema & Format Framework

> Extensible trait-based connector architecture, format decoders, schema inference/evolution, JSON/Array/Struct functions, and schema hints. See [ADR-006](../adr/ADR-006-schema-trait-contract.md) for the binding trait contract.

#### Core Schema Trait Architecture (Group A — Foundation)

| ID | Feature | Priority | Status | Dependencies | Spec |
|----|---------|----------|--------|--------------|------|
| F-SCHEMA-001 | Extensible Connector Trait Framework | P0 | ✅ | F034 | [Link](phase-3/schema/F-SCHEMA-001-connector-trait-framework.md) |
| F-SCHEMA-002 | Schema Resolver & Merge Engine | P0 | ✅ | F-SCHEMA-001 | [Link](phase-3/schema/F-SCHEMA-002-schema-resolver.md) |
| F-SCHEMA-003 | Format Inference Registry | P0 | ✅ | F-SCHEMA-001 | [Link](phase-3/schema/F-SCHEMA-003-format-inference-registry.md) |

#### Format Decoders (Group B — Ring 1 Hot Path)

| ID | Feature | Priority | Status | Dependencies | Spec |
|----|---------|----------|--------|--------------|------|
| F-SCHEMA-004 | JSON Format Decoder & Inference | P0 | ✅ | F-SCHEMA-001, F-SCHEMA-003 | [Link](phase-3/schema/F-SCHEMA-004-json-decoder.md) |
| F-SCHEMA-005 | CSV Format Decoder & Inference | P1 | ✅ | F-SCHEMA-001, F-SCHEMA-003 | [Link](phase-3/schema/F-SCHEMA-005-csv-decoder.md) |
| F-SCHEMA-006 | Avro Format Decoder + Schema Registry | P0 | ✅ | F-SCHEMA-001, F-SCHEMA-003 | [Link](phase-3/schema/F-SCHEMA-006-avro-decoder.md) |
| F-SCHEMA-007 | Parquet Format Decoder | P1 | ✅ | F-SCHEMA-001 | [Link](phase-3/schema/F-SCHEMA-007-parquet-decoder.md) |
| F-SCHEMA-008 | Protobuf Format Decoder | P2 | 📝 | F-SCHEMA-001, F-SCHEMA-006 | [Link](phase-3/schema/F-SCHEMA-008-protobuf-decoder.md) |

#### Schema Evolution & Error Handling (Group C)

| ID | Feature | Priority | Status | Dependencies | Spec |
|----|---------|----------|--------|--------------|------|
| F-SCHEMA-009 | Schema Evolution Engine | P1 | ✅ | F-SCHEMA-001, F-SCHEMA-002 | [Link](phase-3/schema/F-SCHEMA-009-schema-evolution.md) |
| F-SCHEMA-010 | Dead Letter Queue & Error Handling | P1 | ✅ | F-SCHEMA-001 | [Link](phase-3/schema/F-SCHEMA-010-dead-letter-queue.md) |

#### Transformation Functions (Group D)

| ID | Feature | Priority | Status | Dependencies | Spec |
|----|---------|----------|--------|--------------|------|
| F-SCHEMA-011 | PostgreSQL-Compatible JSON Functions | P0 | ✅ | F-SCHEMA-004 | [Link](phase-3/schema/F-SCHEMA-011-json-functions.md) |
| F-SCHEMA-012 | JSON Table-Valued & Path Functions | P1 | ✅ | F-SCHEMA-011 | [Link](phase-3/schema/F-SCHEMA-012-json-tvf.md) |
| F-SCHEMA-013 | LaminarDB JSON Extensions | P2 | ✅ | F-SCHEMA-011 | [Link](phase-3/schema/F-SCHEMA-013-json-extensions.md) |
| F-SCHEMA-014 | Format Bridge Functions | P1 | ✅ | F-SCHEMA-001, F-SCHEMA-004, F-SCHEMA-006 | [Link](phase-3/schema/F-SCHEMA-014-format-bridges.md) |
| F-SCHEMA-015 | Array, Struct & Map Functions | P1 | ✅ | F-SCHEMA-001 | [Link](phase-3/schema/F-SCHEMA-015-array-struct-map-functions.md) |

#### Schema Hints (Group E — Unique to LaminarDB)

| ID | Feature | Priority | Status | Dependencies | Spec |
|----|---------|----------|--------|--------------|------|
| F-SCHEMA-016 | Schema Hints with Wildcard Inference | P1 | ✅ | F-SCHEMA-002, F-SCHEMA-003 | [Link](phase-3/schema/F-SCHEMA-016-schema-hints.md) |

#### Dependency Graph

```
F-SCHEMA-001 (Traits) ─┬─→ F-SCHEMA-002 (Resolver) ──┬─→ F-SCHEMA-009 (Evolution)
                        │                              └─→ F-SCHEMA-016 (Schema Hints)
                        │                                        ↑
                        ├─→ F-SCHEMA-003 (Inference Reg) ──┬────┘
                        │         │                        │
                        │         ├─→ F-SCHEMA-004 (JSON) ─┬─→ F-SCHEMA-011 (JSON Fns)
                        │         │                        │         │
                        │         │                        │         ├─→ F-SCHEMA-012 (TVF)
                        │         │                        │         └─→ F-SCHEMA-013 (Extensions)
                        │         │                        │
                        │         └─→ F-SCHEMA-005 (CSV)   └─→ F-SCHEMA-014 (Format Bridges)
                        │                                            ↑
                        ├─→ F-SCHEMA-006 (Avro+Registry) ──────────┘
                        │         └─→ F-SCHEMA-008 (Protobuf)
                        │
                        ├─→ F-SCHEMA-007 (Parquet)
                        ├─→ F-SCHEMA-010 (Dead Letter)
                        └─→ F-SCHEMA-015 (Array/Struct/Map)
```

#### Build Order (Recommended)

1. **F-SCHEMA-001** (Trait Framework) — everything depends on this
2. **F-SCHEMA-002** + **F-SCHEMA-003** (Resolver + Inference Registry) — parallel
3. **F-SCHEMA-004** + **F-SCHEMA-006** + **F-SCHEMA-007** (JSON + Avro + Parquet decoders) — parallel
4. **F-SCHEMA-005** + **F-SCHEMA-010** + **F-SCHEMA-015** (CSV + DLQ + Array/Struct/Map) — parallel
5. **F-SCHEMA-011** (JSON Functions) — needs JSON decoder
6. **F-SCHEMA-009** + **F-SCHEMA-012** + **F-SCHEMA-014** (Evolution + TVF + Format Bridges) — parallel
7. **F-SCHEMA-008** + **F-SCHEMA-013** + **F-SCHEMA-016** (Protobuf + JSON Extensions + Schema Hints) — parallel

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

## Phase 6: Delta Architecture

> Embedded-to-distributed streaming SQL with sub-500ns hot path guarantees. Extends LaminarDB from single-process to multi-node delta without changing operator code.

See [Delta Index](delta/INDEX.md) for full details, dependency graph, performance budget, and decision log.

### Phase 6a: Partition-Parallel Embedded

> State store infrastructure, checkpoint barriers, lookup tables with foyer caching, source pushdown, SQL integration, baseline benchmarks.

#### State Store Architecture

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-STATE-001 | Revised StateStore Trait | ✅ | [Link](delta/state/F-STATE-001-state-store-trait.md) |
| F-STATE-002 | InMemoryStateStore (AHashMap/BTreeSet) | ✅ | [Link](delta/state/F-STATE-002-inmemory-state-store.md) |
| F-STATE-003 | ~~MmapStateStore~~ | ❌ | [Link](delta/state/F-STATE-003-mmap-state-store.md) |
| F-STATE-004 | ~~Pluggable Snapshot Strategies~~ | ❌ | [Link](delta/state/F-STATE-004-pluggable-snapshots.md) |

#### Distributed Checkpoint (Embedded)

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-DCKP-001 | Checkpoint Barrier Protocol | ✅ | [Link](delta/checkpoint/F-DCKP-001-barrier-protocol.md) |
| F-DCKP-002 | Barrier Alignment | ✅ | [Link](delta/checkpoint/F-DCKP-002-barrier-alignment.md) |
| F-DCKP-003 | Object Store Checkpoint Layout | ✅ | [Link](delta/checkpoint/F-DCKP-003-object-store-layout.md) |
| F-DCKP-004 | ObjectStoreCheckpointer | ✅ | [Link](delta/checkpoint/F-DCKP-004-object-store-checkpointer.md) |
| F-DCKP-005 | Recovery Manager | ✅ | [Link](delta/checkpoint/F-DCKP-005-recovery-manager.md) |

#### Lookup Tables

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-LOOKUP-001 | LookupTable Trait & Strategy | ✅ | [Link](delta/lookup/F-LOOKUP-001-lookup-table-trait.md) |
| F-LOOKUP-002 | LookupSource Trait | ✅ | [Link](delta/lookup/F-LOOKUP-002-lookup-source-trait.md) |
| F-LOOKUP-003 | Predicate Types | ✅ | [Link](delta/lookup/F-LOOKUP-003-predicate-types.md) |
| F-LOOKUP-004 | foyer In-Memory Cache (Ring 0) | ✅ | [Link](delta/lookup/F-LOOKUP-004-foyer-memory-cache.md) |
| F-LOOKUP-005 | foyer Hybrid Cache (Ring 1) | ✅ | [Link](delta/lookup/F-LOOKUP-005-foyer-hybrid-cache.md) |
| F-LOOKUP-006 | CDC-to-Cache Adapter | ✅ | [Link](delta/lookup/F-LOOKUP-006-cdc-cache-adapter.md) |
| F-LOOKUP-007 | PostgresLookupSource | ✅ | [Link](delta/lookup/F-LOOKUP-007-postgres-lookup-source.md) |
| F-LOOKUP-008 | ParquetLookupSource | ✅ | [Link](delta/lookup/F-LOOKUP-008-parquet-lookup-source.md) |
| F-LOOKUP-010 | Remove RocksDB Dependency | ✅ | [Link](delta/lookup/F-LOOKUP-010-rocksdb-removal.md) |

#### Secondary Indexes

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-IDX-001 | redb Secondary Indexes | ✅ | [Link](delta/indexes/F-IDX-001-redb-secondary-indexes.md) |

#### Deployment Profiles

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-PROFILE-001 | Deployment Profiles | ✅ | [Link](delta/profiles/F-PROFILE-001-deployment-profiles.md) |

#### Cross-Partition Aggregation

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-XAGG-001 | Cross-Partition Lock-Free HashMap | ✅ | [Link](delta/aggregation/F-XAGG-001-cross-partition-hashmap.md) |

#### Exactly-Once (Source Layer)

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-E2E-001 | Source Offset Checkpoint | ✅ | [Link](delta/exactly-once/F-E2E-001-source-offset-checkpoint.md) |

#### SQL Integration

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-LSQL-001 | CREATE LOOKUP TABLE DDL | ✅ | [Link](delta/sql/F-LSQL-001-create-lookup-table.md) |
| F-LSQL-002 | Lookup Join Plan Node | ✅ | [Link](delta/sql/F-LSQL-002-lookup-join-plan-node.md) |
| F-LSQL-003 | Predicate Splitting & Pushdown | ✅ | [Link](delta/sql/F-LSQL-003-predicate-splitting.md) |

#### Performance Benchmarks (Baseline)

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-PERF-001 | StateStore Microbenchmarks | ✅ | [Link](delta/benchmarks/F-PERF-001-state-store-benchmarks.md) |
| F-PERF-002 | Cache Hit/Miss Ratio Benchmarks | ✅ | [Link](delta/benchmarks/F-PERF-002-cache-benchmarks.md) |
| F-PERF-003 | Checkpoint Cycle Benchmark | ✅ | [Link](delta/benchmarks/F-PERF-003-checkpoint-cycle-benchmark.md) |
| F-PERF-005 | Lookup Join Throughput | ✅ | [Link](delta/benchmarks/F-PERF-005-lookup-join-throughput.md) |

### Phase 6b: Delta Foundation

> Multi-node discovery (gossip), Raft-based metadata consensus, partition ownership with epoch fencing, distributed checkpointing, partitioned lookups, cross-node aggregation, inter-node RPC.

#### Discovery

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-DISC-001 | Discovery Trait & Static Discovery | ✅ | [Link](delta/discovery/F-DISC-001-static-discovery.md) |
| F-DISC-002 | Gossip Discovery (chitchat) | ✅ | [Link](delta/discovery/F-DISC-002-gossip-discovery.md) |
| F-DISC-003 | Kafka Group Discovery | ✅ | [Link](delta/discovery/F-DISC-003-kafka-discovery.md) |

#### Coordination

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-COORD-001 | Raft Metadata Consensus | ✅ | [Link](delta/coordination/F-COORD-001-raft-metadata.md) |
| F-COORD-002 | Delta Orchestration | ✅ | [Link](delta/coordination/F-COORD-002-delta-orchestration.md) |

#### Partition Ownership

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-EPOCH-001 | PartitionGuard & Epoch Fencing | ✅ | [Link](delta/partition/F-EPOCH-001-partition-guard.md) |
| F-EPOCH-002 | Partition Assignment Algorithm | ✅ | [Link](delta/partition/F-EPOCH-002-assignment-algorithm.md) |
| F-EPOCH-003 | Partition Reassignment Protocol | ✅ | [Link](delta/partition/F-EPOCH-003-reassignment-protocol.md) |

#### Distributed Checkpoint (Delta)

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-DCKP-008 | Distributed Checkpoint Coordination | ✅ | [Link](delta/checkpoint/F-DCKP-008-distributed-coordination.md) |

#### Delta Lookup

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-LOOKUP-009 | Partitioned Lookup Strategy | ✅ | [Link](delta/lookup/F-LOOKUP-009-partitioned-lookup.md) |

#### Cross-Node Aggregation

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-XAGG-002 | Gossip Partial Aggregates | ✅ | [Link](delta/aggregation/F-XAGG-002-gossip-aggregates.md) |
| F-XAGG-003 | gRPC Aggregate Fan-Out | ✅ | [Link](delta/aggregation/F-XAGG-003-grpc-aggregate-fanout.md) |

#### Inter-Node RPC

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-RPC-001 | gRPC Service Definitions | ✅ | [Link](delta/rpc/F-RPC-001-grpc-service-definitions.md) |
| F-RPC-002 | Remote Lookup Service | ✅ | [Link](delta/rpc/F-RPC-002-remote-lookup-service.md) |
| F-RPC-003 | Barrier Forwarding Service | ✅ | [Link](delta/rpc/F-RPC-003-barrier-forwarding.md) |

#### Performance Benchmarks (Delta)

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-PERF-004 | Delta Checkpoint Benchmark | ✅ | [Link](delta/benchmarks/F-PERF-004-delta-checkpoint-benchmark.md) |

### Phase 6c: Production Hardening

> Unaligned checkpoints, exactly-once sinks, laminardb-server binary with TOML config, HTTP API, hot reload, rolling restarts.

#### Advanced Checkpointing

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-DCKP-006 | Unaligned Checkpoints | 📝 | [Link](delta/checkpoint/F-DCKP-006-unaligned-checkpoints.md) |
| F-DCKP-007 | ~~Incremental Mmap Checkpoints~~ | ❌ | [Link](delta/checkpoint/F-DCKP-007-incremental-mmap-checkpoints.md) |

#### Exactly-Once Sinks

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-E2E-002 | Transactional Sink (2PC) | 📝 | [Link](delta/exactly-once/F-E2E-002-transactional-sink.md) |
| F-E2E-003 | Idempotent Sink | 📝 | [Link](delta/exactly-once/F-E2E-003-idempotent-sink.md) |

#### laminardb-server

| ID | Feature | Status | Spec |
|----|---------|--------|------|
| F-SERVER-001 | TOML Configuration | 📝 | [Link](delta/server/F-SERVER-001-toml-config.md) |
| F-SERVER-002 | Engine Construction | 📝 | [Link](delta/server/F-SERVER-002-engine-construction.md) |
| F-SERVER-003 | HTTP API | 📝 | [Link](delta/server/F-SERVER-003-http-api.md) |
| F-SERVER-004 | Hot Reload | 📝 | [Link](delta/server/F-SERVER-004-hot-reload.md) |
| F-SERVER-005 | Delta Server Mode | 📝 | [Link](delta/server/F-SERVER-005-delta-mode.md) |
| F-SERVER-006 | Graceful Rolling Restart | 📝 | [Link](delta/server/F-SERVER-006-rolling-restart.md) |

---

## Performance Optimization

> Architectural performance improvements identified by the 2026-02-22 audit. These require design changes beyond localized code fixes. All actionable local fixes (40+ findings) have been implemented.

### State Store & Reactor

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-POPT-001 | Zero-Copy Prefix Scan (GAT iterator) | P2 | 📝 | [Link](perf-optimization/F-POPT-001-zero-copy-prefix-scan.md) |
| F-POPT-002 | Monomorphized Reactor Dispatch | P2 | 📝 | [Link](perf-optimization/F-POPT-002-monomorphized-reactor.md) |
| F-POPT-003 | AHashMapStore Key Deduplication | P2 | 📝 | [Link](perf-optimization/F-POPT-003-ahash-key-dedup.md) |
| F-POPT-004 | MmapStateStore Compaction | P3 | 📝 | [Link](perf-optimization/F-POPT-004-mmap-compaction.md) |

### Join Operators

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-POPT-005 | Zero-Copy Join Row Encoding | P1 | 📝 | [Link](perf-optimization/F-POPT-005-zero-copy-join-row.md) |
| F-POPT-006 | Pooled Arrow Builders for Join Output | P1 | 📝 | [Link](perf-optimization/F-POPT-006-pooled-join-builders.md) |

### SQL Execution

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-POPT-007 | LogicalPlan Passthrough (eliminate AST round-trip) | P1 | 📝 | [Link](perf-optimization/F-POPT-007-logical-plan-passthrough.md) |
| F-POPT-008 | Binary JSON Representation (JSONB) | P2 | 📝 | [Link](perf-optimization/F-POPT-008-binary-json.md) |
| F-POPT-009 | SQL Plan Cache | P3 | 📝 | [Link](perf-optimization/F-POPT-009-sql-plan-cache.md) |
| F-POPT-010 | MVCC TableStore (eliminate Mutex contention) | P2 | 📝 | [Link](perf-optimization/F-POPT-010-mvcc-table-store.md) |

### Storage & Connectors

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-POPT-011 | WAL Scatter-Gather Writes | P1 | 📝 | [Link](perf-optimization/F-POPT-011-wal-scatter-gather.md) |
| F-POPT-012 | Avro Decoder Reuse | P3 | 📝 | [Link](perf-optimization/F-POPT-012-avro-decoder-reuse.md) |

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
