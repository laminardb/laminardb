# F-DAG-005: SQL & MV Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-005 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-DAG-003 (Executor), F060 (Cascading MVs), F063 (Changelog), F006B (SQL Parser) |
| **Blocks** | F-DAG-007 (Performance Validation) |
| **Owner** | TBD |
| **Created** | 2026-01-30 |
| **Crate** | `laminar-core` (primary), `laminar-sql` (SQL extensions) |
| **Module** | `laminar-core/src/dag/watermark.rs`, `laminar-core/src/dag/changelog.rs`, `laminar-sql/src/planner/dag_planner.rs` |

## Summary

Integrates the DAG subsystem with the existing `MvRegistry` (F060), `CascadingWatermarkTracker`, and `ChangelogBuffer` (F063). Enables automatic DAG construction from SQL `CREATE MATERIALIZED VIEW` chains. Adds `EXPLAIN DAG` SQL command and `laminar_system.dag_topology` introspection table.

## Goals

- `StreamingDag::from_mv_registry()` constructor: converts MV dependency graph to DAG topology
- `DagWatermarkTracker`: min-semantics watermark propagation through DAG (fan-in = min, fan-out = forward)
- Integration with `CascadingWatermarkTracker` (F060), `PartitionedWatermarkTracker` (F064), `WatermarkAlignmentGroup` (F066)
- `DagChangelogPropagator`: per-node `ChangelogBuffer` for delta tracking, drain for Ring 1 WAL
- SQL `EXPLAIN DAG` command returning topology, shared stages, channel types
- `laminar_system.dag_topology` virtual table for introspection

## Non-Goals

- Automatic operator fusion/optimization
- SQL DDL for explicit DAG construction (MVs create DAGs implicitly)
- Custom partitioning strategies via SQL

## Technical Design

See [Full Design Spec, Sections 11-14](../F-DAG-001-dag-pipeline.md#11-changelogretraction-propagation-f063-integration) for detailed implementation.

### from_mv_registry()

1. Create Source nodes for base tables
2. Create MaterializedView nodes in topological order
3. Create edges from MV sources to MV nodes
4. Compute execution order and routing table via `finalize()`

### Watermark Propagation

- Fan-in: node watermark = min(all input watermarks)
- Fan-out: watermark forwarded to all downstream nodes
- Reuses `CascadingWatermarkTracker::update_watermark()` logic

### EXPLAIN DAG Output

```
Execution order: raw_trades -> normalized_trades -> [vwap, anomalies]
Shared stages: normalized_trades (3 consumers)
Channel types: raw_trades->normalized_trades: SPSC, normalized_trades->*: SPMC
```

### Module Structure

```
crates/laminar-core/src/dag/
+-- watermark.rs     # DagWatermarkTracker
+-- changelog.rs     # DagChangelogPropagator

crates/laminar-sql/src/
+-- planner/
|   +-- dag_planner.rs    # StreamingDag from SQL statements
+-- translator/
    +-- dag_translator.rs  # EXPLAIN DAG handler
```

## Test Plan

### Unit Tests (12+)

- [ ] `test_dag_from_mv_registry_linear` - trades -> ohlc_1s -> ohlc_1m
- [ ] `test_dag_from_mv_registry_fan_out` - trades -> {vwap, anomalies, position}
- [ ] `test_dag_from_mv_registry_empty` - Empty registry
- [ ] `test_dag_from_mv_registry_single_mv` - One MV, one base table
- [ ] `test_watermark_linear_propagation` - Source watermark flows to all nodes
- [ ] `test_watermark_fan_in_min` - Min semantics at merge node
- [ ] `test_watermark_fan_out_forward` - Watermark forwarded to all branches
- [ ] `test_watermark_independent_branches` - Different watermarks per branch
- [ ] `test_changelog_record_output` - Record delta at node
- [ ] `test_changelog_drain` - Drain deltas for Ring 1
- [ ] `test_changelog_disabled` - No overhead when disabled
- [ ] `test_explain_dag_output` - Verify EXPLAIN DAG format

## Completion Checklist

- [ ] `StreamingDag::from_mv_registry()` implemented
- [ ] `DagWatermarkTracker` implemented
- [ ] `DagChangelogPropagator` implemented
- [ ] `EXPLAIN DAG` SQL handler
- [ ] 12+ unit tests passing
- [ ] `cargo clippy` clean
