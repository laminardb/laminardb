# Stateful Streaming SQL Feature Index

> **Phase**: 3 - Connectors & Integration
> **Status**: Complete (all 7 features implemented)
> **Reference**: [Gap Analysis](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)

## Overview

Bridges the gap between LaminarDB's **Ring 0 stateful operators** (production-ready, programmatic API) and the **SQL/StreamExecutor path** (`CREATE STREAM ... AS SELECT ...`), which currently uses stateless DataFusion micro-batch execution. These features incrementally make the SQL path stateful, checkpointable, and ultimately routed through Ring 0 for full parity with the programmatic API.

### Background

Gap 1 (non-EOWC aggregation state loss) has been fixed via `IncrementalAggState` in `aggregate_state.rs`. A production-readiness audit identified 22 hardening gaps in the existing aggregation code (F-SSQL-000). The remaining 6 features address EOWC window accumulators, checkpoint integration, Ring 0 routing, and DataFusion v52 opportunities.

## Feature Summary

| ID | Feature | Priority | Effort | Status | Spec |
|----|---------|----------|--------|--------|------|
| F-SSQL-000 | Streaming Aggregation Hardening | P0 | L | ✅ | [Link](F-SSQL-000-aggregation-hardening.md) |
| F-SSQL-001 | EOWC Incremental Window Accumulators | P1 | L | ✅ | [Link](F-SSQL-001-eowc-incremental-accumulators.md) |
| F-SSQL-002 | StreamExecutor State Checkpoint Integration | P1 | M | ✅ | [Link](F-SSQL-002-checkpoint-integration.md) |
| F-SSQL-003 | Ring 0 SQL Operator Routing | P2 | M-XL | ✅ | [Link](F-SSQL-003-ring0-sql-routing.md) |
| F-SSQL-004 | Streaming Physical Optimizer Rule | P2 | M | ✅ | [Link](F-SSQL-004-streaming-physical-optimizer.md) |
| F-SSQL-005 | DataFusion Cooperative Scheduling | P3 | S | ✅ | [Link](F-SSQL-005-cooperative-scheduling.md) |
| F-SSQL-006 | Dynamic Watermark Filter Pushdown | P3 | M | ✅ | [Link](F-SSQL-006-dynamic-watermark-pushdown.md) |

---

## Dependency Graph

```
Gap 1 fix (done) ──► F-SSQL-000 (Aggregation Hardening)
                         │
                         ▼
                     F-SSQL-001 (EOWC Incremental)
                         │
                         ▼
                     F-SSQL-002 (Checkpoint Integration)
                         │
                         ▼
                     F-SSQL-003 (Ring 0 SQL Routing)


                     F-SSQL-004 (Physical Optimizer)         ← independent


                     F-SSQL-005 (Cooperative Scheduling)     ← independent
                         │
                         ▼
                     F-SSQL-006 (Dynamic Watermark Pushdown)
```

### Cross-Feature Dependencies

| This Feature | Depends On |
|-------------|------------|
| F-SSQL-000 | Gap 1 fix (done) |
| F-SSQL-001 | F-SSQL-000, F075 (DataFusion Aggregate Bridge) |
| F-SSQL-002 | F-SSQL-001, F-CKP-003 (Checkpoint Coordinator) |
| F-SSQL-003 | F-SSQL-001, F-SSQL-002, F075, F082 (Streaming Query Lifecycle) |
| F-SSQL-004 | — |
| F-SSQL-005 | — |
| F-SSQL-006 | F-SSQL-005 |

---

## Implementation Order

### Phase 0 (P0 — Prerequisite)

0. **F-SSQL-000** — Aggregation hardening (22 audit findings: DISTINCT, FILTER, HAVING, panic paths, resource limits)

### Phase A (P1)

1. **F-SSQL-001** — EOWC incremental window accumulators (replaces raw-batch O(N) path)
2. **F-SSQL-002** — Checkpoint/restore for all StreamExecutor aggregation state

### Phase B (P2)

3. **F-SSQL-004** — Streaming physical optimizer (independent, can start in parallel)
4. **F-SSQL-003** — Route SQL aggregation through Ring 0 operators

### Phase C (P3)

5. **F-SSQL-005** — DataFusion cooperative scheduling (one-method addition)
6. **F-SSQL-006** — Dynamic watermark filter pushdown

---

## Key Files

| File | Role |
|------|------|
| `crates/laminar-db/src/stream_executor.rs` | SQL execution loop, EOWC state |
| `crates/laminar-db/src/aggregate_state.rs` | `IncrementalAggState` (Gap 1 fix) |
| `crates/laminar-db/src/eowc_state.rs` | EOWC incremental window accumulators |
| `crates/laminar-db/src/core_window_state.rs` | Ring 0 SQL operator routing (tumbling/hopping/session) |
| `crates/laminar-sql/src/datafusion/aggregate_bridge.rs` | DataFusion ↔ Ring 0 accumulator bridge |
| `crates/laminar-sql/src/datafusion/exec.rs` | `StreamingScanExec` execution plan |
| `crates/laminar-sql/src/planner/streaming_optimizer.rs` | `StreamingPhysicalValidator` rule |
| `crates/laminar-sql/src/datafusion/watermark_filter.rs` | `WatermarkDynamicFilter` pushdown |
| `crates/laminar-db/src/checkpoint_coordinator.rs` | Checkpoint coordination |
| `crates/laminar-core/src/operator/window.rs` | Ring 0 window operators |

---

## References

- [Gap Analysis: Stateful Streaming Execution](../../../GAP-ANALYSIS-STATEFUL-STREAMING.md)
- [F075: DataFusion Aggregate Bridge](../../phase-2/F075-datafusion-aggregate-bridge.md)
- [F082: Streaming Query Lifecycle](../../plan-compiler/F082-streaming-query-lifecycle.md)
- [F-CKP-003: Checkpoint Coordinator](../F-CKP-003-checkpoint-coordinator.md)
