# DAG Pipeline Feature Index

> **Phase**: 3 - Connectors & Integration
> **Status**: 📝 Draft (All specs complete)
> **Reference**: [DAG Pipeline Research](../../../research/laminardb-dag-pipeline-spec.md), [Full Design Spec](../F-DAG-001-dag-pipeline.md)

## Overview

LaminarDB's DAG pipeline enables complex streaming topologies with fan-out, fan-in, and shared intermediate stages while maintaining sub-500ns hot path latency.

### Core Principles

1. **Zero-allocation routing** - Pre-computed routing table in Ring 0
2. **Automatic channel derivation** - SPSC/SPMC/MPSC inferred from topology
3. **Zero-copy multicast** - Shared stages use reference-counted slot buffers
4. **Three-ring separation** - Execution in Ring 0, checkpoints in Ring 1, topology in Ring 2

## Feature Summary

| Tier | Total | Draft | In Progress | Done |
|------|-------|-------|-------------|------|
| Core (P0) | 3 | 0 | 0 | 3 |
| Integration (P1) | 4 | 1 | 0 | 3 |
| Validation (P2) | 1 | 0 | 0 | 1 |

---

## Core (P0)

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-DAG-001 | Core DAG Topology | P0 | ✅ | [Link](F-DAG-001-core-topology.md) |
| F-DAG-002 | Multicast & Routing | P0 | ✅ | [Link](F-DAG-002-multicast-routing.md) |
| F-DAG-003 | DAG Executor | P0 | ✅ | [Link](F-DAG-003-dag-executor.md) |

---

## Integration (P1)

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-DAG-004 | DAG Checkpointing | P1 | ✅ | [Link](F-DAG-004-dag-checkpointing.md) |
| F-DAG-005 | SQL & MV Integration | P1 | ✅ | [Link](F-DAG-005-sql-mv-integration.md) |
| F-DAG-006 | Connector Bridge | P1 | ✅ | [Link](F-DAG-006-connector-bridge.md) |
| F-DAG-008 | Pipeline Introspection | P1 | 📝 | [Link](F-DAG-008-pipeline-introspection.md) |

---

## Validation (P2)

| ID | Feature | Priority | Status | Spec |
|----|---------|----------|--------|------|
| F-DAG-007 | Performance Validation | P2 | ✅ | [Link](F-DAG-007-performance-validation.md) |

---

## Dependency Graph

```
Core (P0):
F-DAG-001 (Topology) ──► F-DAG-002 (Multicast/Routing) ──► F-DAG-003 (Executor)

Integration (P1):
F-DAG-003 ──► F-DAG-004 (Checkpointing)
F-DAG-003 ──► F-DAG-005 (SQL/MV Integration)
F-DAG-003 ──► F-DAG-006 (Connector Bridge)

Validation (P2):
F-DAG-003 + F-DAG-004 + F-DAG-005 ──► F-DAG-007 (Performance)

External Dependencies:
F-STREAM-001..007 (Streaming API) ──► F-DAG-001
F060 (Cascading MVs) ──► F-DAG-005
F063 (Changelog/Retraction) ──► F-DAG-004, F-DAG-005
F022 (Incremental Checkpointing) ──► F-DAG-004
F034 (Connector SDK) ──► F-DAG-006
```

---

## Future Enhancements

| ID | Feature | Priority | Status |
|----|---------|----------|--------|
| F-DAG-010 | Dynamic Topology | P2 | 📝 Idea |
| F-DAG-011 | DAG Visualization (Web UI) | P2 | 📝 Idea (requires F-DAG-008) |
| F-DAG-012 | Operator Fusion | P2 | 📝 Idea |
| F-DAG-013 | Distributed DAG | P3 | 📝 Idea |

---

## Performance Targets

| Metric | Target | Feature |
|--------|--------|---------|
| Routing table lookup | <50ns | F-DAG-002 |
| Multicast per consumer | <100ns | F-DAG-002 |
| Hot path with DAG (e2e) | <500ns p99 | F-DAG-003 |
| Checkpoint overhead | <5% throughput | F-DAG-004 |
| Recovery time | <5s for 1GB state | F-DAG-004 |
| Throughput/core | >500K events/sec | F-DAG-007 |

---

## References

- [DAG Pipeline Research Spec](../../../research/laminardb-dag-pipeline-spec.md)
- [Full Design Spec (monolithic)](../F-DAG-001-dag-pipeline.md)
- [Streaming API Index](../streaming/INDEX.md)
- [F060: Cascading MVs](../../phase-2/F060-cascading-materialized-views.md)
- [F063: Changelog/Retraction](../../phase-2/F063-changelog-retraction.md)
