# F-DAG-006: Connector Bridge

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-006 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | S (2-3 days) |
| **Dependencies** | F-DAG-003 (Executor), F034 (Connector SDK) |
| **Blocks** | F-DAG-007 (Performance Validation) |
| **Owner** | TBD |
| **Created** | 2026-01-30 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/dag/connector_bridge.rs` |

## Summary

Bridge between DAG source/sink nodes and the Connector SDK (F034). Source connectors (e.g., Kafka) run in Ring 1 and push data into DAG source nodes via SPSC channels. Sink connectors subscribe to DAG sink node outputs. Manages connector lifecycle and checkpoint offset coordination for exactly-once semantics.

## Goals

- `ConnectorDagBridge` with `attach_source()` and `attach_sink()`
- Source connector (Ring 1) -> SPSC channel -> DAG source node (Ring 0)
- DAG sink node output -> Subscription -> Sink connector (Ring 1)
- Source offset tracking for checkpoint coordination (exactly-once replay)
- Connector lifecycle management (start/stop with DAG lifecycle)

## Non-Goals

- Implementing specific connectors (Kafka, CDC - separate features F025-F034)
- Distributed connector coordination
- Schema evolution during DAG execution

## Technical Design

See [Full Design Spec, Sections 16-17](../F-DAG-001-dag-pipeline.md#16-connector-sdk-integration-f034) for detailed implementation.

### Data Flow

```
Ring 1: SourceConnector.poll_batch() -> Source.push_arrow()
                                             |
                                             v (SPSC channel)
Ring 0: DagExecutor reads from source node input queue
                                             |
                                             v (routing table)
Ring 0: ... operator chain ... -> sink node output
                                             |
                                             v (Subscription)
Ring 1: SinkConnector.write_batch() <- Subscription.poll()
```

### Module Structure

```
crates/laminar-core/src/dag/
+-- connector_bridge.rs   # ConnectorDagBridge, SourceHandle, SinkHandle
```

## Test Plan

### Unit Tests (6+)

- [ ] `test_attach_source` - Attach mock source connector to DAG source node
- [ ] `test_attach_sink` - Attach mock sink connector to DAG sink node
- [ ] `test_source_to_dag_flow` - Data flows from source connector through DAG
- [ ] `test_dag_to_sink_flow` - Data flows from DAG through sink connector
- [ ] `test_connector_offset_tracking` - Source offsets recorded in checkpoint
- [ ] `test_connector_lifecycle` - Start/stop connectors with DAG

### Integration Tests

- [ ] `test_end_to_end_mock_connectors` - Mock source -> DAG -> Mock sink

## Completion Checklist

- [ ] `ConnectorDagBridge` implemented
- [ ] Source connector -> DAG integration
- [ ] DAG -> Sink connector integration
- [ ] Offset tracking for checkpoints
- [ ] 6+ unit tests passing
- [ ] Integration test passing
- [ ] `cargo clippy` clean
