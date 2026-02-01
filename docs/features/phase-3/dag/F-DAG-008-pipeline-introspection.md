# F-DAG-008: Pipeline Topology Introspection API

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-008 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | S (1-2 days) |
| **Dependencies** | F-DAG-005 (SQL & MV Integration), F-STREAM-007 (SQL DDL) |
| **Blocks** | F-DAG-011 (DAG Visualization), F-DEMO-004 (Demo DAG View) |
| **Owner** | TBD |
| **Created** | 2026-02-01 |
| **Crate** | `laminar-db` |
| **Module** | `laminar-db/src/handle.rs`, `laminar-db/src/db.rs` |

## Summary

Exposes the pipeline topology (sources -> streams -> sinks) as a structured public API on `LaminarDB`. Currently, the internal `ConnectorManager` tracks all relationships (stream SQL, sink inputs) but nothing is publicly accessible. This feature adds `pipeline_topology()` returning a `PipelineTopology` graph, and `streams()` returning `Vec<StreamInfo>`. Serves both programmatic introspection and UI visualization (F-DAG-011, F-DEMO-004).

## Goals

- `PipelineTopology` struct with typed `PipelineNode` and `PipelineEdge` vectors
- `PipelineNodeType` enum: Source, Stream, Sink
- `LaminarDB::pipeline_topology()` builds graph from catalog + connector manager
- `LaminarDB::streams()` returns `Vec<StreamInfo>` (name + SQL)
- `StreamInfo` struct for stream metadata
- Edge derivation: parse stream SQL for FROM references, use sink `input` field
- Re-export all new types from `laminar_db` crate root

## Non-Goals

- Runtime DAG topology from `StreamingDag` (requires DAG executor integration)
- Performance metrics per node (future: F-DAG-011)
- Dynamic topology changes (future: F-DAG-010)
- MvRegistry dependency graph exposure (separate concern)

## Technical Design

### New Types (handle.rs)

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineNodeType {
    Source,
    Stream,
    Sink,
}

#[derive(Debug, Clone)]
pub struct PipelineNode {
    pub name: String,
    pub node_type: PipelineNodeType,
    pub schema: Option<SchemaRef>,
    pub sql: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PipelineEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone)]
pub struct PipelineTopology {
    pub nodes: Vec<PipelineNode>,
    pub edges: Vec<PipelineEdge>,
}

#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub name: String,
    pub sql: Option<String>,
}
```

### pipeline_topology() Method (db.rs)

1. Read `catalog.list_sources()` -> Source nodes (with schema)
2. Read `connector_manager.streams()` -> Stream nodes (with query SQL)
3. Read `connector_manager.sinks()` -> Sink nodes
4. For each stream: extract FROM table references from `query_sql` -> edges source->stream
5. For each sink: use `input` field -> edge stream->sink
6. FROM extraction: simple string matching against known source/stream names in query SQL

### streams() Method (db.rs)

```rust
pub fn streams(&self) -> Vec<StreamInfo> {
    let mgr = self.connector_manager.lock();
    mgr.streams().iter().map(|(name, reg)| StreamInfo {
        name: name.clone(),
        sql: Some(reg.query_sql.clone()),
    }).collect()
}
```

### Module Structure

```
crates/laminar-db/src/
  handle.rs   # +PipelineTopology, PipelineNode, PipelineEdge, PipelineNodeType, StreamInfo
  db.rs       # +pipeline_topology(), +streams()
  lib.rs      # +re-exports
```

## Test Plan

### Unit Tests (6+)

- [ ] `test_pipeline_topology_empty` - No sources/streams/sinks returns empty topology
- [ ] `test_pipeline_topology_sources_only` - Sources with no streams have no edges
- [ ] `test_pipeline_topology_full_pipeline` - CREATE SOURCE + CREATE STREAM + CREATE SINK -> correct nodes and edges
- [ ] `test_pipeline_topology_fan_out` - One source -> multiple streams -> correct edges
- [ ] `test_streams_method` - Returns registered streams with SQL
- [ ] `test_pipeline_node_types` - Verify correct PipelineNodeType for each node

## Completion Checklist

- [ ] `PipelineTopology`, `PipelineNode`, `PipelineEdge`, `PipelineNodeType` types
- [ ] `StreamInfo` type
- [ ] `pipeline_topology()` method on `LaminarDB`
- [ ] `streams()` method on `LaminarDB`
- [ ] FROM-reference extraction for edge derivation
- [ ] Re-exports from `lib.rs`
- [ ] 6+ unit tests passing
- [ ] `cargo clippy` clean
