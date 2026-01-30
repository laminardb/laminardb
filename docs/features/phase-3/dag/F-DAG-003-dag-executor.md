# F-DAG-003: DAG Executor

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-003 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-DAG-001 (Topology), F-DAG-002 (Multicast/Routing), F071 (Zero-Alloc) |
| **Blocks** | F-DAG-004, F-DAG-005, F-DAG-006, F-DAG-007 |
| **Owner** | TBD |
| **Created** | 2026-01-30 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/dag/executor.rs` |

## Summary

Ring 0 DAG executor that processes events through the DAG in topological order. Uses the pre-computed routing table for O(1) dispatch and multicast buffer for shared stages. Integrates with the existing `Operator` trait and `HotPathGuard` (F071) for zero-allocation enforcement.

## Goals

- `DagExecutor` with pre-allocated input queues per node
- `process_event(source_node, event, ctx)` drives events through topological order
- `route_output()` dispatches to single target or multicast buffer
- Integration with `Operator::process()` trait for operator dispatch
- `HotPathGuard` integration (F071) in debug mode
- `DagExecutorMetrics` for observability (events_processed, multicast_publishes, backpressure_stalls)
- Pre-allocated `VecDeque<Event>` input queues (no hot-path allocation)

## Non-Goals

- Checkpoint barrier handling (F-DAG-004)
- SQL plan conversion (F-DAG-005)
- Connector lifecycle (F-DAG-006)
- Async operator support (future)

## Technical Design

See [Full Design Spec, Section 6](../F-DAG-001-dag-pipeline.md#6-dag-executor-ring-0) for detailed implementation.

### Ring Architecture

| Ring | Responsibility |
|------|---------------|
| Ring 0 | `DagExecutor::process_event()` - routing, operator calls, multicast |
| Ring 1 | Checkpoint coordination, WAL drain (F-DAG-004) |
| Ring 2 | Topology construction via `DagBuilder` (F-DAG-001) |

### Latency Budget (Ring 0)

| Component | Budget |
|-----------|--------|
| Routing table lookup | <50ns |
| Operator dispatch (`process()`) | <200ns |
| Multicast to N consumers | <100ns |
| State access | <200ns |
| **Total** | **<500ns** |

### Module Structure

```
crates/laminar-core/src/dag/
+-- executor.rs      # DagExecutor, DagExecutorMetrics
```

## Test Plan

### Unit Tests (15+)

- [ ] `test_executor_linear_dag` - Source -> A -> B -> Sink
- [ ] `test_executor_fan_out` - Source -> Shared -> {A, B, C}
- [ ] `test_executor_fan_in` - {A, B} -> Merge -> Sink
- [ ] `test_executor_diamond` - Source -> {A, B} -> Merge -> Sink
- [ ] `test_executor_multicast_dispatch` - Shared stage uses multicast buffer
- [ ] `test_executor_single_target_dispatch` - Linear edge uses direct enqueue
- [ ] `test_executor_topological_order` - Dependencies processed before dependents
- [ ] `test_executor_operator_integration` - Custom operator transforms events
- [ ] `test_executor_passthrough_operator` - No-op operator forwards events
- [ ] `test_executor_metrics_tracking` - events_processed, multicast_publishes
- [ ] `test_executor_empty_queue_skip` - Nodes with no input events are skipped
- [ ] `test_executor_terminal_node` - Sink node has no routing
- [ ] `test_executor_multiple_events` - Process batch of events through DAG
- [ ] `test_executor_backpressure` - Full multicast buffer returns error
- [ ] `test_executor_register_operator` - Register and invoke custom operators

### Integration Tests

- [ ] `test_executor_with_window_operator` - DAG with tumbling window
- [ ] `test_executor_with_filter_operator` - DAG with filter (some events dropped)
- [ ] `test_executor_complex_dag` - 6+ node topology with mixed fan-in/fan-out

### Benchmarks

- [ ] `bench_executor_linear_3_node` - Target: <500ns p99
- [ ] `bench_executor_fan_out_3` - Target: <500ns p99
- [ ] `bench_executor_diamond` - Target: <500ns p99

## Completion Checklist

- [ ] `DagExecutor` implemented in `dag/executor.rs`
- [ ] Integration with `Operator` trait
- [ ] Pre-allocated input queues
- [ ] 15+ unit tests passing
- [ ] Integration tests passing
- [ ] Benchmarks meet <500ns target
- [ ] `cargo clippy` clean
