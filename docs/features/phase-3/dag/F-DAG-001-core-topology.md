# F-DAG-001: Core DAG Topology

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-001 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STREAM-001 (Ring Buffer), F-STREAM-002 (SPSC Channel) |
| **Blocks** | F-DAG-002, F-DAG-003 |
| **Owner** | TBD |
| **Created** | 2026-01-30 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/dag/topology.rs`, `laminar-core/src/dag/builder.rs` |

## Summary

Core DAG topology data structures and builder API. Defines `DagNode`, `DagEdge`, `StreamingDag` with topological ordering, cycle detection, and automatic channel type derivation (SPSC/SPMC/MPSC). Provides `DagBuilder` for programmatic DAG construction in Ring 2.

## Goals

- `DagNode` and `DagEdge` adjacency list representation
- `StreamingDag` with topological sort (Kahn's algorithm)
- Cycle detection (reject cyclic topologies at construction)
- Automatic `DagChannelType` derivation from fan-in/fan-out analysis
- `DagBuilder` fluent API: `source()`, `operator()`, `connect()`, `fan_out()`, `sink()`, `build()`
- `DagError` error types
- Schema compatibility validation on connected edges

## Non-Goals

- Multicast buffer implementation (F-DAG-002)
- Routing table construction (F-DAG-002)
- Event execution (F-DAG-003)
- Dynamic topology modification at runtime

## Technical Design

See [Full Design Spec, Sections 1-3](../F-DAG-001-dag-pipeline.md#1-dag-topology-model) for detailed data structures:

- `DagNode`: id, name, operator, inputs/outputs (SmallVec<[EdgeId; 4]>), output_schema, state_partition, node_type
- `DagEdge`: id, source, target, channel_type, partitioning, source_port, target_port
- `DagNodeType`: Source, StatelessOperator, StatefulOperator, MaterializedView, Sink
- `DagChannelType`: Spsc, Spmc, Mpsc (derived, never user-specified)
- `PartitioningStrategy`: Single, RoundRobin, HashBy, Custom
- `StreamingDag`: nodes, edges, execution_order, shared_stages, source_nodes, sink_nodes
- `DagBuilder` + `FanOutBuilder`: fluent builder pattern

### Module Structure

```
crates/laminar-core/src/dag/
+-- mod.rs           # Public API re-exports
+-- topology.rs      # StreamingDag, DagNode, DagEdge, DagChannelType
+-- builder.rs       # DagBuilder, FanOutBuilder
+-- error.rs         # DagError
```

## Test Plan

### Unit Tests (15+)

- [ ] `test_empty_dag` - Create empty DAG, verify no nodes/edges
- [ ] `test_add_single_node` - Add source node, verify
- [ ] `test_linear_dag` - Source -> A -> B -> Sink, all SPSC
- [ ] `test_fan_out_dag` - Source -> Shared -> {A, B, C}, verify SPMC + shared_stages
- [ ] `test_fan_in_dag` - {A, B} -> Merge -> Sink, verify MPSC
- [ ] `test_diamond_dag` - Source -> {A, B} -> Merge -> Sink
- [ ] `test_cycle_detection` - A -> B -> A, expect CycleDetected error
- [ ] `test_self_loop_detection` - A -> A, expect CycleDetected error
- [ ] `test_disconnected_node` - Node with no edges, expect error
- [ ] `test_node_not_found` - Edge referencing missing node, expect error
- [ ] `test_topological_order` - Verify execution_order respects dependencies
- [ ] `test_channel_type_derivation` - Verify SPSC/SPMC/MPSC assignment
- [ ] `test_shared_stage_detection` - Nodes with fan-out > 1 flagged as shared
- [ ] `test_source_sink_classification` - Sources/sinks correctly identified
- [ ] `test_fan_out_builder` - FanOutBuilder creates branches correctly
- [ ] `test_max_fan_out_limit` - >8 fan-out targets returns error

## Completion Checklist

- [ ] Data structures implemented in `dag/topology.rs`
- [ ] Builder implemented in `dag/builder.rs`
- [ ] Error types in `dag/error.rs`
- [ ] Module re-exports in `dag/mod.rs`
- [ ] 15+ unit tests passing
- [ ] `cargo clippy` clean
- [ ] Feature INDEX.md updated
