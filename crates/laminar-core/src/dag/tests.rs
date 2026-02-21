//! Unit tests for DAG topology, builder, multicast, routing, executor, checkpointing,
//! MV integration, watermark tracking, and changelog propagation.

#![allow(clippy::similar_names)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rustc_hash::FxHashMap;

use super::builder::DagBuilder;
use super::changelog::DagChangelogPropagator;
use super::checkpoint::{
    AlignmentResult, BarrierAligner, BarrierType, CheckpointBarrier, DagCheckpointConfig,
    DagCheckpointCoordinator,
};
use super::error::DagError;
use super::executor::DagExecutor;
use super::multicast::MulticastBuffer;
use super::recovery::{DagCheckpointSnapshot, DagRecoveryManager, SerializableOperatorState};
use super::routing::{RoutingEntry, RoutingTable};
use super::topology::*;
use super::watermark::DagWatermarkTracker;
use crate::mv::{MaterializedView, MvRegistry};
use crate::operator::changelog::ChangelogRef;
use crate::operator::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
};

/// Helper to create a simple int64 schema.
fn int_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

/// Helper to create a schema with two int64 fields.
fn two_field_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Helper to create a float64 schema (incompatible with `int_schema`).
fn float_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]))
}

/// Helper to create an empty schema (type-erased, compatible with anything).
fn empty_schema() -> Arc<Schema> {
    Arc::new(Schema::empty())
}

// ---- StreamingDag direct tests ----

#[test]
fn test_empty_dag() {
    let dag = StreamingDag::new();
    assert_eq!(dag.node_count(), 0);
    assert_eq!(dag.edge_count(), 0);
    assert!(dag.sources().is_empty());
    assert!(dag.sinks().is_empty());
    assert!(dag.execution_order().is_empty());
    assert!(!dag.is_finalized());
}

#[test]
fn test_empty_dag_finalize_error() {
    let mut dag = StreamingDag::new();
    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::EmptyDag)));
}

#[test]
fn test_add_single_node() {
    let mut dag = StreamingDag::new();
    let id = dag
        .add_node("source", DagNodeType::Source, int_schema())
        .unwrap();
    assert_eq!(id, NodeId(0));
    assert_eq!(dag.node_count(), 1);
    assert_eq!(dag.node_id_by_name("source"), Some(NodeId(0)));
    assert_eq!(dag.node_name(NodeId(0)), Some("source".to_string()));
}

#[test]
fn test_duplicate_node_error() {
    let mut dag = StreamingDag::new();
    dag.add_node("src", DagNodeType::Source, int_schema())
        .unwrap();
    let result = dag.add_node("src", DagNodeType::Source, int_schema());
    assert!(matches!(result, Err(DagError::DuplicateNode(_))));
}

#[test]
fn test_add_edge_node_not_found() {
    let mut dag = StreamingDag::new();
    let id = dag
        .add_node("a", DagNodeType::Source, int_schema())
        .unwrap();
    let result = dag.add_edge(id, NodeId(99));
    assert!(matches!(result, Err(DagError::NodeNotFound(_))));
}

#[test]
fn test_self_loop_detection() {
    let mut dag = StreamingDag::new();
    let id = dag
        .add_node("a", DagNodeType::StatefulOperator, int_schema())
        .unwrap();
    let result = dag.add_edge(id, id);
    assert!(matches!(result, Err(DagError::CycleDetected(_))));
}

// ---- DagBuilder tests ----

#[test]
fn test_linear_dag() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .sink("snk", schema.clone())
        .connect("src", "a")
        .connect("a", "b")
        .connect("b", "snk")
        .build()
        .unwrap();

    assert_eq!(dag.node_count(), 4);
    assert_eq!(dag.edge_count(), 3);
    assert!(dag.is_finalized());

    // All edges should be SPSC in a linear DAG
    for edge in dag.edges().values() {
        assert_eq!(edge.channel_type, DagChannelType::Spsc);
    }

    // Source and sink classification
    assert_eq!(dag.sources().len(), 1);
    assert_eq!(dag.sinks().len(), 1);

    // Topological order: src must come before a, a before b, b before snk
    let order = dag.execution_order();
    assert_eq!(order.len(), 4);
    let pos = |name: &str| {
        order
            .iter()
            .position(|&id| dag.node_name(id).unwrap() == name)
            .unwrap()
    };
    assert!(pos("src") < pos("a"));
    assert!(pos("a") < pos("b"));
    assert!(pos("b") < pos("snk"));

    // No shared stages
    assert!(dag.shared_stages().is_empty());
}

#[test]
fn test_fan_out_dag() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
                .branch("c", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .sink_for("c", "sink_c", schema.clone())
        .build()
        .unwrap();

    // src, shared, a, b, c, sink_a, sink_b, sink_c = 8
    assert_eq!(dag.node_count(), 8);
    assert_eq!(dag.sources().len(), 1);
    assert_eq!(dag.sinks().len(), 3);

    // shared has fan-out > 1, so it should be a shared stage
    let shared_id = dag.node_id_by_name("shared").unwrap();
    assert!(dag.shared_stages().contains_key(&shared_id));
    let meta = &dag.shared_stages()[&shared_id];
    assert_eq!(meta.consumer_count, 3);

    // Edges from shared to a/b/c should be SPMC
    let shared_node = dag.node(shared_id).unwrap();
    for &edge_id in &shared_node.outputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Spmc);
    }
}

#[test]
fn test_fan_in_dag() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("a", schema.clone())
        .source("b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("a", "merge")
        .connect("b", "merge")
        .build()
        .unwrap();

    assert_eq!(dag.node_count(), 4);
    assert_eq!(dag.sources().len(), 2);
    assert_eq!(dag.sinks().len(), 1);

    // Edges to merge should be MPSC (fan-in)
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let merge_node = dag.node(merge_id).unwrap();
    assert_eq!(merge_node.inputs.len(), 2);
    for &edge_id in &merge_node.inputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Mpsc);
    }
}

#[test]
fn test_diamond_dag() {
    let schema = int_schema();

    // Diamond: src -> {a, b} -> merge -> snk
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .connect("a", "merge")
        .connect("b", "merge")
        .sink_for("merge", "snk", schema.clone())
        .build()
        .unwrap();

    assert_eq!(dag.node_count(), 5);
    assert_eq!(dag.sources().len(), 1);
    assert_eq!(dag.sinks().len(), 1);

    // Topological order: src before a and b, a and b before merge, merge before snk
    let order = dag.execution_order();
    let pos = |name: &str| {
        order
            .iter()
            .position(|&id| dag.node_name(id).unwrap() == name)
            .unwrap()
    };
    assert!(pos("src") < pos("a"));
    assert!(pos("src") < pos("b"));
    assert!(pos("a") < pos("merge"));
    assert!(pos("b") < pos("merge"));
    assert!(pos("merge") < pos("snk"));

    // src has fan-out > 1, so it should be shared
    let src_id = dag.node_id_by_name("src").unwrap();
    assert!(dag.shared_stages().contains_key(&src_id));

    // Edges from src to a/b should be SPMC (fan-out from src)
    let src_node = dag.node(src_id).unwrap();
    for &edge_id in &src_node.outputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Spmc);
    }

    // Edges from a/b to merge should be MPSC (fan-in to merge)
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let merge_node = dag.node(merge_id).unwrap();
    for &edge_id in &merge_node.inputs {
        let edge = dag.edge(edge_id).unwrap();
        assert_eq!(edge.channel_type, DagChannelType::Mpsc);
    }
}

#[test]
fn test_cycle_detection() {
    // Build a DAG where A -> B -> C, then try to add C -> A
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    let a = dag
        .add_node("a", DagNodeType::StatefulOperator, schema.clone())
        .unwrap();
    let b = dag
        .add_node("b", DagNodeType::StatefulOperator, schema.clone())
        .unwrap();
    let c = dag
        .add_node("c", DagNodeType::StatefulOperator, schema.clone())
        .unwrap();
    dag.add_edge(a, b).unwrap();
    dag.add_edge(b, c).unwrap();
    dag.add_edge(c, a).unwrap(); // Creates a cycle

    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::CycleDetected(_))));
}

#[test]
fn test_disconnected_node_source_no_outputs() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    dag.add_node("src", DagNodeType::Source, schema.clone())
        .unwrap();
    // Source with no outputs is disconnected
    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::DisconnectedNode(_))));
}

#[test]
fn test_disconnected_node_sink_no_inputs() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    dag.add_node("snk", DagNodeType::Sink, schema.clone())
        .unwrap();
    // Sink with no inputs is disconnected
    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::DisconnectedNode(_))));
}

#[test]
fn test_disconnected_operator() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    let src = dag
        .add_node("src", DagNodeType::Source, schema.clone())
        .unwrap();
    let snk = dag
        .add_node("snk", DagNodeType::Sink, schema.clone())
        .unwrap();
    dag.add_edge(src, snk).unwrap();

    // Add a disconnected operator
    dag.add_node("orphan", DagNodeType::StatefulOperator, schema)
        .unwrap();

    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::DisconnectedNode(_))));
}

#[test]
fn test_builder_node_not_found() {
    let schema = int_schema();
    let result = DagBuilder::new()
        .source("src", schema.clone())
        .connect("src", "nonexistent")
        .build();
    assert!(matches!(result, Err(DagError::NodeNotFound(_))));
}

#[test]
fn test_topological_order_complex() {
    //        src
    //       / | \
    //      a  b  c
    //       \ | /
    //       merge
    //         |
    //        snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("c", schema.clone())
        .operator("merge", schema.clone())
        .sink("snk", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("src", "c")
        .connect("a", "merge")
        .connect("b", "merge")
        .connect("c", "merge")
        .connect("merge", "snk")
        .build()
        .unwrap();

    let order = dag.execution_order();
    assert_eq!(order.len(), 6);

    let pos = |name: &str| {
        order
            .iter()
            .position(|&id| dag.node_name(id).unwrap() == name)
            .unwrap()
    };

    // src must be first (only source)
    assert_eq!(pos("src"), 0);
    // a, b, c must come after src and before merge
    assert!(pos("a") < pos("merge"));
    assert!(pos("b") < pos("merge"));
    assert!(pos("c") < pos("merge"));
    // snk must be last
    assert!(pos("merge") < pos("snk"));
}

#[test]
fn test_channel_type_derivation() {
    let schema = int_schema();

    // src -> shared -> {a, b}  AND  {c, d} -> merge -> snk
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .source("c", schema.clone())
        .source("d", schema.clone())
        .operator("merge", schema.clone())
        .connect("c", "merge")
        .connect("d", "merge")
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .sink_for("merge", "sink_merge", schema.clone())
        .build()
        .unwrap();

    // SPSC: src -> shared (1 output, 1 input)
    let src_id = dag.node_id_by_name("src").unwrap();
    let src_node = dag.node(src_id).unwrap();
    let src_edge = dag.edge(src_node.outputs[0]).unwrap();
    assert_eq!(src_edge.channel_type, DagChannelType::Spsc);

    // SPMC: shared -> a, shared -> b (fan-out = 2)
    let shared_id = dag.node_id_by_name("shared").unwrap();
    let shared_node = dag.node(shared_id).unwrap();
    for &eid in &shared_node.outputs {
        assert_eq!(dag.edge(eid).unwrap().channel_type, DagChannelType::Spmc);
    }

    // MPSC: c -> merge, d -> merge (fan-in = 2)
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let merge_node = dag.node(merge_id).unwrap();
    for &eid in &merge_node.inputs {
        assert_eq!(dag.edge(eid).unwrap().channel_type, DagChannelType::Mpsc);
    }
}

#[test]
fn test_shared_stage_detection() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    // Only "shared" should be a shared stage (fan-out > 1)
    assert_eq!(dag.shared_stages().len(), 1);
    let shared_id = dag.node_id_by_name("shared").unwrap();
    assert!(dag.shared_stages().contains_key(&shared_id));

    let meta = &dag.shared_stages()[&shared_id];
    assert_eq!(meta.consumer_count, 2);
    assert_eq!(meta.producer_node, shared_id);
}

#[test]
fn test_source_sink_classification() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s1", schema.clone())
        .source("s2", schema.clone())
        .operator("op", schema.clone())
        .sink("k1", schema.clone())
        .sink("k2", schema.clone())
        .connect("s1", "op")
        .connect("s2", "op")
        .connect("op", "k1")
        .connect("op", "k2")
        .build()
        .unwrap();

    assert_eq!(dag.sources().len(), 2);
    assert_eq!(dag.sinks().len(), 2);

    // Source nodes should have no inputs
    for &src_id in dag.sources() {
        assert_eq!(dag.incoming_edge_count(src_id), 0);
    }

    // Sink nodes should have no outputs
    for &snk_id in dag.sinks() {
        assert_eq!(dag.outgoing_edge_count(snk_id), 0);
    }
}

#[test]
fn test_fan_out_builder() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .connect("src", "shared")
        .operator("shared", schema.clone())
        .fan_out("shared", |b| {
            b.branch("x", schema.clone())
                .branch("y", schema.clone())
                .stateless_branch("z", schema.clone())
        })
        .sink_for("x", "sx", schema.clone())
        .sink_for("y", "sy", schema.clone())
        .sink_for("z", "sz", schema.clone())
        .build()
        .unwrap();

    // Verify stateless branch is StatelessOperator
    let z_id = dag.node_id_by_name("z").unwrap();
    let z_node = dag.node(z_id).unwrap();
    assert_eq!(z_node.node_type, DagNodeType::StatelessOperator);

    // Verify stateful branches are StatefulOperator
    let x_id = dag.node_id_by_name("x").unwrap();
    let x_node = dag.node(x_id).unwrap();
    assert_eq!(x_node.node_type, DagNodeType::StatefulOperator);
}

#[test]
fn test_max_fan_out_limit() {
    let schema = int_schema();
    let mut dag = StreamingDag::new();
    let src = dag
        .add_node("src", DagNodeType::Source, schema.clone())
        .unwrap();

    // Add MAX_FAN_OUT + 1 targets
    for i in 0..=MAX_FAN_OUT {
        let name = format!("t{i}");
        let target = dag
            .add_node(&name, DagNodeType::Sink, schema.clone())
            .unwrap();
        dag.add_edge(src, target).unwrap();
    }

    let result = dag.finalize();
    assert!(matches!(result, Err(DagError::FanOutLimitExceeded { .. })));
}

#[test]
fn test_schema_compatibility_pass() {
    // Same schemas should be compatible
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    assert!(dag.validate().is_ok());
}

#[test]
fn test_schema_compatibility_empty_is_compatible() {
    // Empty schema (type-erased) is compatible with anything
    let dag = DagBuilder::new()
        .source("src", empty_schema())
        .sink_for("src", "snk", int_schema())
        .build()
        .unwrap();

    assert!(dag.validate().is_ok());
}

#[test]
fn test_schema_mismatch_field_count() {
    let dag = DagBuilder::new()
        .source("src", int_schema())
        .sink_for("src", "snk", two_field_schema())
        .build()
        .unwrap();

    let result = dag.validate();
    assert!(matches!(result, Err(DagError::SchemaMismatch { .. })));
}

#[test]
fn test_schema_mismatch_type() {
    let dag = DagBuilder::new()
        .source("src", int_schema())
        .sink_for("src", "snk", float_schema())
        .build()
        .unwrap();

    let result = dag.validate();
    assert!(matches!(result, Err(DagError::SchemaMismatch { .. })));
}

#[test]
fn test_materialized_view_node() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .materialized_view("mv1", schema.clone())
        .connect("src", "mv1")
        .sink_for("mv1", "snk", schema.clone())
        .build()
        .unwrap();

    let mv_id = dag.node_id_by_name("mv1").unwrap();
    let mv_node = dag.node(mv_id).unwrap();
    assert_eq!(mv_node.node_type, DagNodeType::MaterializedView);
}

#[test]
fn test_edge_ports() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s1", schema.clone())
        .source("s2", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("s1", "op")
        .connect("s2", "op")
        .build()
        .unwrap();

    // op has two inputs, so the target_port should differ
    let op_id = dag.node_id_by_name("op").unwrap();
    let op_node = dag.node(op_id).unwrap();
    let ports: Vec<u8> = op_node
        .inputs
        .iter()
        .map(|&eid| dag.edge(eid).unwrap().target_port)
        .collect();
    assert_eq!(ports.len(), 2);
    // Ports should be 0, 1 (assigned incrementally)
    assert!(ports.contains(&0));
    assert!(ports.contains(&1));
}

#[test]
fn test_default_builder() {
    let builder = DagBuilder::default();
    let schema = int_schema();
    let dag = builder
        .source("src", schema.clone())
        .sink_for("src", "snk", schema)
        .build()
        .unwrap();
    assert_eq!(dag.node_count(), 2);
}

#[test]
fn test_dag_debug_format() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema)
        .build()
        .unwrap();

    let debug = format!("{dag:?}");
    assert!(debug.contains("StreamingDag"));
    assert!(debug.contains("node_count: 2"));
}

// ---- MulticastBuffer tests ----

#[test]
fn test_multicast_single_consumer() {
    let buf = MulticastBuffer::new(4, 1);
    buf.publish(42u64).unwrap();
    let val = buf.consume(0);
    assert_eq!(val, Some(42));
}

#[test]
fn test_multicast_multiple_consumers() {
    let buf = MulticastBuffer::new(4, 3);
    buf.publish(100u64).unwrap();

    // All 3 consumers should read the same value.
    assert_eq!(buf.consume(0), Some(100));
    assert_eq!(buf.consume(1), Some(100));
    assert_eq!(buf.consume(2), Some(100));
}

#[test]
fn test_multicast_backpressure() {
    let buf = MulticastBuffer::new(2, 1);
    buf.publish(1u64).unwrap();
    buf.publish(2u64).unwrap();

    // Buffer full (2 slots, neither consumed).
    let result = buf.publish(3u64);
    assert!(result.is_err());
}

#[test]
fn test_multicast_slot_reuse() {
    let buf = MulticastBuffer::new(2, 1);
    buf.publish(1u64).unwrap();
    buf.publish(2u64).unwrap();

    // Consume first slot to free it.
    assert_eq!(buf.consume(0), Some(1));

    // Slot 0 should be free now, publish should succeed.
    buf.publish(3u64).unwrap();

    // Consume remaining values in order.
    assert_eq!(buf.consume(0), Some(2));
    assert_eq!(buf.consume(0), Some(3));
}

#[test]
fn test_multicast_partial_consume() {
    let buf = MulticastBuffer::new(2, 2);
    buf.publish(10u64).unwrap();

    // Only consumer 0 reads.
    assert_eq!(buf.consume(0), Some(10));

    // Slot still in use by consumer 1.
    buf.publish(20u64).unwrap();

    // Buffer full: consumer 1 hasn't freed slot 0.
    let result = buf.publish(30u64);
    assert!(result.is_err());

    // Consumer 1 reads, freeing slot 0.
    assert_eq!(buf.consume(1), Some(10));

    // Now publish should succeed.
    buf.publish(30u64).unwrap();
}

#[test]
fn test_multicast_wrap_around() {
    let buf = MulticastBuffer::new(2, 1);

    for i in 0u64..10 {
        buf.publish(i).unwrap();
        assert_eq!(buf.consume(0), Some(i));
    }

    assert_eq!(buf.write_position(), 10);
    assert_eq!(buf.read_position(0), 10);
}

#[test]
fn test_multicast_empty_consume() {
    let buf: MulticastBuffer<u64> = MulticastBuffer::new(4, 2);

    // No data published, consume should return None.
    assert_eq!(buf.consume(0), None);
    assert_eq!(buf.consume(1), None);
}

#[test]
fn test_multicast_accessors() {
    let buf: MulticastBuffer<u64> = MulticastBuffer::new(8, 3);
    assert_eq!(buf.capacity(), 8);
    assert_eq!(buf.consumer_count(), 3);
    assert_eq!(buf.write_position(), 0);
    assert_eq!(buf.read_position(0), 0);
    assert_eq!(buf.read_position(1), 0);
    assert_eq!(buf.read_position(2), 0);

    // Debug format should work.
    let debug = format!("{buf:?}");
    assert!(debug.contains("MulticastBuffer"));
}

#[test]
fn test_multicast_sequential_values() {
    let buf = MulticastBuffer::new(4, 2);

    buf.publish(10u64).unwrap();
    buf.publish(20u64).unwrap();
    buf.publish(30u64).unwrap();

    // Consumer 0 reads all three.
    assert_eq!(buf.consume(0), Some(10));
    assert_eq!(buf.consume(0), Some(20));
    assert_eq!(buf.consume(0), Some(30));
    assert_eq!(buf.consume(0), None);

    // Consumer 1 also reads all three.
    assert_eq!(buf.consume(1), Some(10));
    assert_eq!(buf.consume(1), Some(20));
    assert_eq!(buf.consume(1), Some(30));
    assert_eq!(buf.consume(1), None);
}

// ---- RoutingTable tests ----

#[test]
fn test_routing_table_build() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);
    assert!(table.entry_count() > 0);
}

#[test]
fn test_routing_table_linear() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // src -> op: single target, not multicast.
    let src_id = dag.node_id_by_name("src").unwrap();
    let entry = table.node_targets(src_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
    assert_eq!(entry.targets[0], dag.node_id_by_name("op").unwrap().0);

    // op -> snk: single target, not multicast.
    let op_id = dag.node_id_by_name("op").unwrap();
    let entry = table.node_targets(op_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
    assert_eq!(entry.targets[0], dag.node_id_by_name("snk").unwrap().0);
}

#[test]
fn test_routing_table_fan_out() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
                .branch("c", schema.clone())
        })
        .sink_for("a", "sa", schema.clone())
        .sink_for("b", "sb", schema.clone())
        .sink_for("c", "sc", schema.clone())
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // shared -> {a, b, c}: multicast with 3 targets.
    let shared_id = dag.node_id_by_name("shared").unwrap();
    let entry = table.node_targets(shared_id);
    assert_eq!(entry.target_count, 3);
    assert!(entry.is_multicast);

    // Check targets include a, b, c.
    let target_ids: Vec<u32> = entry.target_ids().to_vec();
    assert!(target_ids.contains(&dag.node_id_by_name("a").unwrap().0));
    assert!(target_ids.contains(&dag.node_id_by_name("b").unwrap().0));
    assert!(target_ids.contains(&dag.node_id_by_name("c").unwrap().0));
}

#[test]
fn test_routing_table_terminal() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // Sink node has no targets (terminal).
    let snk_id = dag.node_id_by_name("snk").unwrap();
    let entry = table.node_targets(snk_id);
    assert_eq!(entry.target_count, 0);
    assert!(!entry.is_multicast);
    assert!(entry.is_terminal());
}

#[test]
fn test_routing_entry_cache_alignment() {
    assert_eq!(std::mem::size_of::<RoutingEntry>(), 64);
    assert_eq!(std::mem::align_of::<RoutingEntry>(), 64);
}

#[test]
fn test_routing_table_diamond() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);

    // src -> {a, b}: multicast.
    let src_id = dag.node_id_by_name("src").unwrap();
    let entry = table.node_targets(src_id);
    assert_eq!(entry.target_count, 2);
    assert!(entry.is_multicast);

    // a -> merge: single target.
    let a_id = dag.node_id_by_name("a").unwrap();
    let entry = table.node_targets(a_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
    assert_eq!(entry.targets[0], dag.node_id_by_name("merge").unwrap().0);

    // merge -> snk: single target.
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let entry = table.node_targets(merge_id);
    assert_eq!(entry.target_count, 1);
    assert!(!entry.is_multicast);
}

#[test]
fn test_routing_table_max_node_id() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s", schema.clone())
        .sink_for("s", "k", schema.clone())
        .build()
        .unwrap();

    let table = RoutingTable::from_dag(&dag);
    // Node IDs are 0 and 1, so max_node_id should be 1.
    assert_eq!(table.max_node_id(), 1);
}

// ---- DagExecutor tests ----

/// Helper to create a test event with the given timestamp and a single-column payload.
fn test_event(timestamp: i64, value: i64) -> Event {
    let array = Arc::new(Int64Array::from(vec![value]));
    let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
    Event::new(timestamp, batch)
}

/// Extract the i64 payload value from a test event.
fn event_value(event: &Event) -> i64 {
    let col = event
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    col.value(0)
}

/// A passthrough operator that forwards events unchanged.
struct PassthroughOperator;

impl Operator for PassthroughOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let mut v = OutputVec::new();
        v.push(Output::Event(event.clone()));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "passthrough".to_string(),
            data: Vec::new(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

/// A doubling operator that multiplies the event value by 2.
struct DoublingOperator;

impl Operator for DoublingOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let val = event_value(event);
        let mut v = OutputVec::new();
        v.push(Output::Event(test_event(event.timestamp, val * 2)));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "doubling".to_string(),
            data: Vec::new(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

/// A filter operator that drops events where value <= threshold.
struct FilterOperator {
    threshold: i64,
}

impl Operator for FilterOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let val = event_value(event);
        let mut v = OutputVec::new();
        if val > self.threshold {
            v.push(Output::Event(event.clone()));
        }
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "filter".to_string(),
            data: self.threshold.to_le_bytes().to_vec(),
        }
    }
    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.data.len() == 8 {
            self.threshold = i64::from_le_bytes(state.data.try_into().unwrap());
        }
        Ok(())
    }
}

/// An adding operator that adds a constant to the event value.
struct AddOperator {
    addend: i64,
}

impl Operator for AddOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let val = event_value(event);
        let mut v = OutputVec::new();
        v.push(Output::Event(test_event(
            event.timestamp,
            val + self.addend,
        )));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "add".to_string(),
            data: self.addend.to_le_bytes().to_vec(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

#[test]
fn test_executor_linear_dag() {
    // src -> op -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_id, test_event(1000, 42))
        .unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 42);
    assert_eq!(outputs[0].timestamp, 1000);
}

#[test]
fn test_executor_fan_out() {
    // src -> shared -> {a, b} -> {sink_a, sink_b}
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let sink_a_id = dag.node_id_by_name("sink_a").unwrap();
    let sink_b_id = dag.node_id_by_name("sink_b").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_id, test_event(1000, 99))
        .unwrap();

    // Both sinks should receive the event.
    let out_a = executor.take_sink_outputs(sink_a_id);
    let out_b = executor.take_sink_outputs(sink_b_id);
    assert_eq!(out_a.len(), 1);
    assert_eq!(out_b.len(), 1);
    assert_eq!(event_value(&out_a[0]), 99);
    assert_eq!(event_value(&out_b[0]), 99);
}

#[test]
fn test_executor_fan_in() {
    // {src_a, src_b} -> merge -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src_a", "merge")
        .connect("src_b", "merge")
        .build()
        .unwrap();

    let src_a_id = dag.node_id_by_name("src_a").unwrap();
    let src_b_id = dag.node_id_by_name("src_b").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_a_id, test_event(1000, 10))
        .unwrap();
    executor
        .process_event(src_b_id, test_event(2000, 20))
        .unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 2);
    let values: Vec<i64> = outputs.iter().map(event_value).collect();
    assert!(values.contains(&10));
    assert!(values.contains(&20));
}

#[test]
fn test_executor_diamond() {
    // src -> {a, b} -> merge -> snk
    // a doubles, b adds 100
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(a_id, Box::new(DoublingOperator));
    executor.register_operator(b_id, Box::new(AddOperator { addend: 100 }));

    executor.process_event(src_id, test_event(1000, 5)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 2);

    let values: Vec<i64> = outputs.iter().map(event_value).collect();
    // a doubles: 5 * 2 = 10
    // b adds 100: 5 + 100 = 105
    assert!(values.contains(&10));
    assert!(values.contains(&105));
}

#[test]
fn test_executor_operator_integration() {
    // src -> double -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));

    executor.process_event(src_id, test_event(1000, 7)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 14); // 7 * 2
}

#[test]
fn test_executor_passthrough_no_operator() {
    // Without a registered operator, nodes act as passthrough.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    // No operator registered for "op" — passthrough behavior.
    executor.process_event(src_id, test_event(500, 33)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 33);
}

#[test]
fn test_executor_filter_operator() {
    // src -> filter(threshold=50) -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("filter", schema.clone())
        .sink_for("filter", "snk", schema.clone())
        .connect("src", "filter")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let filter_id = dag.node_id_by_name("filter").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(filter_id, Box::new(FilterOperator { threshold: 50 }));

    // Event with value 30 should be dropped.
    executor
        .process_event(src_id, test_event(1000, 30))
        .unwrap();
    let outputs = executor.take_sink_outputs(snk_id);
    assert!(outputs.is_empty());

    // Event with value 70 should pass through.
    executor
        .process_event(src_id, test_event(2000, 70))
        .unwrap();
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 70);
}

#[test]
fn test_executor_metrics_tracking() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    let m = executor.metrics();
    // Three nodes processed: src, op, snk.
    // src and op each process 1 event = 2 events_processed.
    // snk gets collected directly, not processed through operator.
    #[cfg(feature = "dag-metrics")]
    assert!(m.events_processed >= 2);
    // src routes to op, op routes to snk.
    #[cfg(feature = "dag-metrics")]
    assert!(m.events_routed >= 1);
    // snk has no input queue events to skip (it gets events).
    // The executor processes in topological order, some nodes have empty queues.
    // In a linear DAG with one event, all nodes receive it, so nodes_skipped may be 0.
    // Without dag-metrics feature, counters are always 0.
    #[cfg(not(feature = "dag-metrics"))]
    let _ = m;
}

#[test]
fn test_executor_metrics_reset() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    #[cfg(feature = "dag-metrics")]
    assert!(executor.metrics().events_processed > 0);
    executor.reset_metrics();
    assert_eq!(executor.metrics().events_processed, 0);
    assert_eq!(executor.metrics().events_routed, 0);
}

#[test]
fn test_executor_empty_queue_skip() {
    // Fan-in: {src_a, src_b} -> merge -> snk
    // Only inject into src_a. src_b has no events, so it gets skipped.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src_a", "merge")
        .connect("src_b", "merge")
        .build()
        .unwrap();

    let src_a_id = dag.node_id_by_name("src_a").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_a_id, test_event(1000, 1))
        .unwrap();

    // src_b had no events, so it should have been skipped.
    let m = executor.metrics();
    #[cfg(feature = "dag-metrics")]
    assert!(m.nodes_skipped > 0);
    #[cfg(not(feature = "dag-metrics"))]
    let _ = m;
}

#[test]
fn test_executor_multiple_events() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));

    // Process three events.
    for i in 1..=3 {
        executor
            .process_event(src_id, test_event(i * 1000, i * 10))
            .unwrap();
    }

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 3);
    assert_eq!(event_value(&outputs[0]), 20); // 10 * 2
    assert_eq!(event_value(&outputs[1]), 40); // 20 * 2
    assert_eq!(event_value(&outputs[2]), 60); // 30 * 2
}

#[test]
fn test_executor_node_not_found() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    let result = executor.process_event(NodeId(999), test_event(1000, 1));
    assert!(result.is_err());
}

#[test]
fn test_executor_take_all_sink_outputs() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_id, test_event(1000, 55))
        .unwrap();

    let all_outputs = executor.take_all_sink_outputs();
    assert_eq!(all_outputs.len(), 2);

    // Both sinks should have received the event.
    for events in all_outputs.values() {
        assert_eq!(events.len(), 1);
        assert_eq!(event_value(&events[0]), 55);
    }
}

#[test]
fn test_executor_source_and_sink_accessors() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("s1", schema.clone())
        .source("s2", schema.clone())
        .operator("op", schema.clone())
        .sink("k1", schema.clone())
        .connect("s1", "op")
        .connect("s2", "op")
        .connect("op", "k1")
        .build()
        .unwrap();

    let executor = DagExecutor::from_dag(&dag);
    assert_eq!(executor.source_nodes().len(), 2);
    assert_eq!(executor.sink_nodes().len(), 1);
}

#[test]
fn test_executor_node_type() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let executor = DagExecutor::from_dag(&dag);
    assert_eq!(executor.node_type(src_id), Some(DagNodeType::Source));
    assert_eq!(
        executor.node_type(op_id),
        Some(DagNodeType::StatefulOperator)
    );
    assert_eq!(executor.node_type(snk_id), Some(DagNodeType::Sink));
    assert_eq!(executor.node_type(NodeId(999)), None);
}

#[test]
fn test_executor_checkpoint() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let op_id = dag.node_id_by_name("op").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(op_id, Box::new(PassthroughOperator));

    let states = executor.checkpoint();
    assert_eq!(states.len(), 1);
    assert!(states.contains_key(&op_id));
    assert_eq!(states[&op_id].operator_id, "passthrough");
}

#[test]
fn test_executor_multicast_metrics() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    // shared -> {a, b} is a multicast.
    #[cfg(feature = "dag-metrics")]
    assert!(executor.metrics().multicast_publishes > 0);
}

#[test]
fn test_executor_chained_operators() {
    // src -> double -> add(100) -> snk
    // Input: 5 -> double: 10 -> add(100): 110
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .operator("add", schema.clone())
        .sink_for("add", "snk", schema.clone())
        .connect("src", "double")
        .connect("double", "add")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let add_id = dag.node_id_by_name("add").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));
    executor.register_operator(add_id, Box::new(AddOperator { addend: 100 }));

    executor.process_event(src_id, test_event(1000, 5)).unwrap();

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 110); // (5 * 2) + 100
}

#[test]
fn test_executor_take_sink_outputs_drains() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    // First take returns the event.
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);

    // Second take should be empty (already drained).
    let outputs = executor.take_sink_outputs(snk_id);
    assert!(outputs.is_empty());
}

#[test]
fn test_executor_debug_format() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let executor = DagExecutor::from_dag(&dag);
    let debug = format!("{executor:?}");
    assert!(debug.contains("DagExecutor"));
    assert!(debug.contains("slot_count"));
}

#[test]
fn test_executor_complex_dag_filter_and_transform() {
    // Complex DAG:
    //   src -> filter(>50) -> double -> snk
    //
    // Input values: 30, 60, 40, 80
    // After filter(>50): 60, 80
    // After double: 120, 160
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("filter", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "filter")
        .connect("filter", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let filter_id = dag.node_id_by_name("filter").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(filter_id, Box::new(FilterOperator { threshold: 50 }));
    executor.register_operator(double_id, Box::new(DoublingOperator));

    for (i, val) in [30, 60, 40, 80].iter().enumerate() {
        executor
            .process_event(src_id, test_event((i as i64 + 1) * 1000, *val))
            .unwrap();
    }

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 2);
    assert_eq!(event_value(&outputs[0]), 120); // 60 * 2
    assert_eq!(event_value(&outputs[1]), 160); // 80 * 2
}

// ---- BarrierAligner tests ----

#[test]
fn test_barrier_aligner_single_input() {
    let mut aligner = BarrierAligner::new(1);
    assert_eq!(aligner.expected_inputs(), 1);

    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 1000,
        barrier_type: BarrierType::Aligned,
    };

    let result = aligner.on_barrier(NodeId(0), barrier);
    assert!(matches!(result, AlignmentResult::Aligned { .. }));
}

#[test]
fn test_barrier_aligner_two_inputs() {
    let mut aligner = BarrierAligner::new(2);

    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 1000,
        barrier_type: BarrierType::Aligned,
    };

    // First input: still pending.
    let result = aligner.on_barrier(NodeId(0), barrier.clone());
    assert!(matches!(result, AlignmentResult::Pending));
    assert_eq!(aligner.barriers_received_count(), 1);

    // Second input: aligned.
    let result = aligner.on_barrier(NodeId(1), barrier);
    assert!(matches!(result, AlignmentResult::Aligned { .. }));
    assert_eq!(aligner.barriers_received_count(), 2);
}

#[test]
fn test_barrier_aligner_three_inputs() {
    let mut aligner = BarrierAligner::new(3);

    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 1000,
        barrier_type: BarrierType::Aligned,
    };

    // First two: pending.
    let result = aligner.on_barrier(NodeId(0), barrier.clone());
    assert!(matches!(result, AlignmentResult::Pending));
    let result = aligner.on_barrier(NodeId(1), barrier.clone());
    assert!(matches!(result, AlignmentResult::Pending));

    // Third: aligned.
    let result = aligner.on_barrier(NodeId(2), barrier);
    assert!(matches!(result, AlignmentResult::Aligned { .. }));
}

#[test]
fn test_barrier_aligner_buffered_events() {
    let mut aligner = BarrierAligner::new(2);

    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 1000,
        barrier_type: BarrierType::Aligned,
    };

    // Source 0 delivers its barrier first.
    let result = aligner.on_barrier(NodeId(0), barrier.clone());
    assert!(matches!(result, AlignmentResult::Pending));

    // Events from source 0 arrive after its barrier — they should be buffered.
    let ev1 = test_event(2000, 42);
    let ev2 = test_event(3000, 99);
    assert!(aligner.buffer_if_aligned(NodeId(0), ev1));
    assert!(aligner.buffer_if_aligned(NodeId(0), ev2));

    // Events from source 1 should NOT be buffered (it hasn't sent its barrier).
    assert!(!aligner.buffer_if_aligned(NodeId(1), test_event(4000, 77)));

    // Source 1 delivers its barrier — alignment complete.
    let result = aligner.on_barrier(NodeId(1), barrier);
    match result {
        AlignmentResult::Aligned {
            buffered_events, ..
        } => {
            assert_eq!(buffered_events.len(), 2);
            assert_eq!(event_value(&buffered_events[0]), 42);
            assert_eq!(event_value(&buffered_events[1]), 99);
        }
        AlignmentResult::Pending => panic!("expected Aligned"),
    }
}

#[test]
fn test_barrier_aligner_cleanup() {
    let mut aligner = BarrierAligner::new(2);

    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 1000,
        barrier_type: BarrierType::Aligned,
    };

    aligner.on_barrier(NodeId(0), barrier.clone());
    aligner.on_barrier(NodeId(1), barrier);
    assert_eq!(aligner.barriers_received_count(), 2);

    // Complete resets state.
    aligner.complete_checkpoint();
    assert_eq!(aligner.barriers_received_count(), 0);
    assert!(!aligner.is_source_aligned(NodeId(0)));
    assert!(!aligner.is_source_aligned(NodeId(1)));
}

// ---- DagCheckpointCoordinator tests ----

#[test]
fn test_coordinator_trigger() {
    let source_nodes = vec![NodeId(0)];
    let all_nodes = vec![NodeId(0), NodeId(1), NodeId(2)];
    let config = DagCheckpointConfig::default();

    let mut coordinator = DagCheckpointCoordinator::new(source_nodes, all_nodes, config);

    let barrier = coordinator.trigger_checkpoint().unwrap();
    assert_eq!(barrier.checkpoint_id, 1);
    assert_eq!(barrier.epoch, 1);
    assert_eq!(barrier.barrier_type, BarrierType::Aligned);
    assert!(coordinator.is_checkpoint_in_progress());
}

#[test]
fn test_coordinator_trigger_while_in_progress() {
    let source_nodes = vec![NodeId(0)];
    let all_nodes = vec![NodeId(0), NodeId(1)];
    let config = DagCheckpointConfig::default();

    let mut coordinator = DagCheckpointCoordinator::new(source_nodes, all_nodes, config);

    coordinator.trigger_checkpoint().unwrap();
    let result = coordinator.trigger_checkpoint();
    assert!(matches!(result, Err(DagError::CheckpointInProgress(_))));
}

#[test]
fn test_coordinator_progress() {
    let source_nodes = vec![NodeId(0)];
    let all_nodes = vec![NodeId(0), NodeId(1), NodeId(2)];
    let config = DagCheckpointConfig::default();

    let mut coordinator = DagCheckpointCoordinator::new(source_nodes, all_nodes, config);
    coordinator.trigger_checkpoint().unwrap();

    // Only one node reports — finalize should fail.
    let state = OperatorState {
        operator_id: "op0".to_string(),
        data: vec![],
    };
    let all_done = coordinator.on_node_snapshot_complete(NodeId(0), state);
    assert!(!all_done);

    let result = coordinator.finalize_checkpoint();
    assert!(matches!(
        result,
        Err(DagError::CheckpointIncomplete { pending: 2 })
    ));
}

#[test]
fn test_coordinator_finalize() {
    let source_nodes = vec![NodeId(0)];
    let all_nodes = vec![NodeId(0), NodeId(1)];
    let config = DagCheckpointConfig::default();

    let mut coordinator = DagCheckpointCoordinator::new(source_nodes, all_nodes, config);
    coordinator.trigger_checkpoint().unwrap();

    // Both nodes report.
    coordinator.on_node_snapshot_complete(
        NodeId(0),
        OperatorState {
            operator_id: "src".to_string(),
            data: vec![1, 2, 3],
        },
    );
    let all_done = coordinator.on_node_snapshot_complete(
        NodeId(1),
        OperatorState {
            operator_id: "op".to_string(),
            data: vec![4, 5, 6],
        },
    );
    assert!(all_done);

    let snapshot = coordinator.finalize_checkpoint().unwrap();
    assert_eq!(snapshot.checkpoint_id, 1);
    assert_eq!(snapshot.epoch, 1);
    assert_eq!(snapshot.node_states.len(), 2);
    assert!(!coordinator.is_checkpoint_in_progress());
    assert_eq!(coordinator.completed_snapshots().len(), 1);
}

#[test]
fn test_coordinator_multiple_checkpoints() {
    let source_nodes = vec![NodeId(0)];
    let all_nodes = vec![NodeId(0)];
    let config = DagCheckpointConfig::default();

    let mut coordinator = DagCheckpointCoordinator::new(source_nodes, all_nodes, config);

    // First checkpoint.
    let barrier1 = coordinator.trigger_checkpoint().unwrap();
    assert_eq!(barrier1.epoch, 1);
    coordinator.on_node_snapshot_complete(
        NodeId(0),
        OperatorState {
            operator_id: "op".to_string(),
            data: vec![10],
        },
    );
    let snap1 = coordinator.finalize_checkpoint().unwrap();
    assert_eq!(snap1.checkpoint_id, 1);

    // Second checkpoint.
    let barrier2 = coordinator.trigger_checkpoint().unwrap();
    assert_eq!(barrier2.epoch, 2);
    assert_eq!(barrier2.checkpoint_id, 2);
    coordinator.on_node_snapshot_complete(
        NodeId(0),
        OperatorState {
            operator_id: "op".to_string(),
            data: vec![20],
        },
    );
    let snap2 = coordinator.finalize_checkpoint().unwrap();
    assert_eq!(snap2.checkpoint_id, 2);

    assert_eq!(coordinator.completed_snapshots().len(), 2);
}

// ---- Config tests ----

#[test]
fn test_checkpoint_config_defaults() {
    let config = DagCheckpointConfig::default();
    assert_eq!(config.interval, std::time::Duration::from_secs(60));
    assert_eq!(config.barrier_type, BarrierType::Aligned);
    assert_eq!(config.alignment_timeout, std::time::Duration::from_secs(10));
    assert!(!config.incremental);
    assert_eq!(config.max_concurrent, 1);
    assert_eq!(config.max_retained, 3);
}

// ---- Recovery tests ----

#[test]
fn test_recovery_no_checkpoint() {
    let manager = DagRecoveryManager::new();
    assert!(!manager.has_snapshots());
    assert_eq!(manager.snapshot_count(), 0);

    let result = manager.recover_latest();
    assert!(matches!(result, Err(DagError::CheckpointNotFound)));
}

#[test]
fn test_recovery_restore_state() {
    let mut manager = DagRecoveryManager::new();

    let mut node_states = std::collections::HashMap::new();
    node_states.insert(
        1,
        SerializableOperatorState {
            operator_id: "double".to_string(),
            data: vec![42],
        },
    );

    let snapshot = DagCheckpointSnapshot {
        checkpoint_id: 5,
        epoch: 5,
        timestamp: 99999,
        node_states,
        source_offsets: std::collections::HashMap::new(),
        watermark: Some(50000),
    };

    manager.add_snapshot(snapshot);
    assert!(manager.has_snapshots());
    assert_eq!(manager.snapshot_count(), 1);

    let recovered = manager.recover_latest().unwrap();
    assert_eq!(recovered.snapshot.checkpoint_id, 5);
    assert_eq!(recovered.watermark, Some(50000));
    assert_eq!(recovered.operator_states.len(), 1);

    let state = &recovered.operator_states[&NodeId(1)];
    assert_eq!(state.operator_id, "double");
    assert_eq!(state.data, vec![42]);
}

#[test]
fn test_recovery_by_id() {
    let mut manager = DagRecoveryManager::new();

    // Add two snapshots.
    for id in 1..=2u64 {
        let mut node_states = std::collections::HashMap::new();
        node_states.insert(
            0,
            SerializableOperatorState {
                operator_id: format!("op_epoch{id}"),
                data: vec![id as u8],
            },
        );
        manager.add_snapshot(DagCheckpointSnapshot {
            checkpoint_id: id,
            epoch: id,
            timestamp: id as i64 * 1000,
            node_states,
            source_offsets: std::collections::HashMap::new(),
            watermark: None,
        });
    }

    // Recover checkpoint 1.
    let recovered = manager.recover_by_id(1).unwrap();
    assert_eq!(recovered.snapshot.checkpoint_id, 1);
    assert_eq!(
        recovered.operator_states[&NodeId(0)].operator_id,
        "op_epoch1"
    );

    // Recover checkpoint 2.
    let recovered = manager.recover_by_id(2).unwrap();
    assert_eq!(recovered.snapshot.checkpoint_id, 2);

    // Non-existent checkpoint.
    let result = manager.recover_by_id(99);
    assert!(matches!(result, Err(DagError::CheckpointNotFound)));
}

// ---- Integration tests ----

#[test]
fn test_checkpoint_linear_dag() {
    // src -> double -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));

    // Process some events.
    executor.process_event(src_id, test_event(1000, 5)).unwrap();
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 10);

    // Trigger checkpoint via barrier.
    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 2000,
        barrier_type: BarrierType::Aligned,
    };
    let states = executor.process_checkpoint_barrier(&barrier);

    // Only the registered operator produces state.
    assert_eq!(states.len(), 1);
    assert!(states.contains_key(&double_id));
    assert_eq!(states[&double_id].operator_id, "doubling");
}

#[test]
fn test_checkpoint_fan_out_dag() {
    // src -> shared -> {a, b} -> {sink_a, sink_b}
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone()).branch("b", schema.clone())
        })
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(a_id, Box::new(DoublingOperator));
    executor.register_operator(b_id, Box::new(AddOperator { addend: 100 }));

    executor.process_event(src_id, test_event(1000, 5)).unwrap();

    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 2000,
        barrier_type: BarrierType::Aligned,
    };
    let states = executor.process_checkpoint_barrier(&barrier);

    // Both branches should have state.
    assert_eq!(states.len(), 2);
    assert!(states.contains_key(&a_id));
    assert!(states.contains_key(&b_id));
    assert_eq!(states[&a_id].operator_id, "doubling");
    assert_eq!(states[&b_id].operator_id, "add");
}

#[test]
fn test_checkpoint_diamond_dag() {
    // src -> {a, b} -> merge -> snk
    // Tests barrier alignment at fan-in node.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .build()
        .unwrap();

    let merge_id = dag.node_id_by_name("merge").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();

    let executor = DagExecutor::from_dag(&dag);

    // merge has 2 inputs — barrier aligner should require 2 barriers.
    assert_eq!(executor.input_count(merge_id), 2);

    let mut aligner = BarrierAligner::new(executor.input_count(merge_id));

    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 1000,
        barrier_type: BarrierType::Aligned,
    };

    // Barrier from branch a: pending.
    let result = aligner.on_barrier(a_id, barrier.clone());
    assert!(matches!(result, AlignmentResult::Pending));

    // Barrier from branch b: aligned.
    let result = aligner.on_barrier(b_id, barrier);
    assert!(matches!(result, AlignmentResult::Aligned { .. }));
}

#[test]
fn test_full_checkpoint_recovery_cycle() {
    // End-to-end: checkpoint, create fresh executor, restore, verify processing.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("double", schema.clone())
        .sink_for("double", "snk", schema.clone())
        .connect("src", "double")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let double_id = dag.node_id_by_name("double").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    // --- Phase 1: Run, then checkpoint ---
    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(double_id, Box::new(DoublingOperator));

    executor.process_event(src_id, test_event(1000, 7)).unwrap();
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(event_value(&outputs[0]), 14);

    // Checkpoint the executor.
    let barrier = CheckpointBarrier {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 2000,
        barrier_type: BarrierType::Aligned,
    };
    let states = executor.process_checkpoint_barrier(&barrier);

    // Build snapshot through coordinator.
    let all_nodes: Vec<NodeId> = dag.execution_order().to_vec();
    let mut coordinator = DagCheckpointCoordinator::new(
        dag.sources().to_vec(),
        all_nodes,
        DagCheckpointConfig::default(),
    );
    let coord_barrier = coordinator.trigger_checkpoint().unwrap();
    for (node_id, state) in &states {
        coordinator.on_node_snapshot_complete(*node_id, state.clone());
    }
    // Also report nodes without operators as empty state.
    for &node_id in dag.execution_order() {
        if !states.contains_key(&node_id) {
            coordinator.on_node_snapshot_complete(
                node_id,
                OperatorState {
                    operator_id: String::new(),
                    data: Vec::new(),
                },
            );
        }
    }
    let snapshot = coordinator.finalize_checkpoint().unwrap();

    // --- Phase 2: Recovery ---
    let mut recovery_manager = DagRecoveryManager::new();
    recovery_manager.add_snapshot(snapshot);

    let recovered = recovery_manager.recover_latest().unwrap();
    assert_eq!(recovered.snapshot.epoch, coord_barrier.epoch);

    // Create a fresh executor and restore.
    let mut executor2 = DagExecutor::from_dag(&dag);
    executor2.register_operator(double_id, Box::new(DoublingOperator));
    executor2.restore(&recovered.operator_states).unwrap();

    // Verify processing continues correctly after recovery.
    executor2
        .process_event(src_id, test_event(3000, 10))
        .unwrap();
    let outputs = executor2.take_sink_outputs(snk_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 20); // 10 * 2
}

// ===========================================================================
// SQL & MV Integration tests
// ===========================================================================

/// Helper: create a simple MV registry with base tables and schemas map.
fn make_mv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn make_base_schemas(names: &[&str]) -> FxHashMap<String, SchemaRef> {
    let schema = make_mv_schema();
    names
        .iter()
        .map(|n| ((*n).to_string(), schema.clone()))
        .collect()
}

// ---- from_mv_registry tests ----

#[test]
fn test_dag_from_mv_registry_linear() {
    // trades -> ohlc_1s -> ohlc_1m (linear chain)
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");

    let schema = make_mv_schema();
    registry
        .register(MaterializedView::new(
            "ohlc_1s",
            "SELECT ...",
            vec!["trades".into()],
            schema.clone(),
        ))
        .unwrap();
    registry
        .register(MaterializedView::new(
            "ohlc_1m",
            "SELECT ...",
            vec!["ohlc_1s".into()],
            schema,
        ))
        .unwrap();

    let base_schemas = make_base_schemas(&["trades"]);
    let dag = StreamingDag::from_mv_registry(&registry, &base_schemas).unwrap();

    assert_eq!(dag.node_count(), 3); // trades, ohlc_1s, ohlc_1m
    assert_eq!(dag.edge_count(), 2);
    assert_eq!(dag.sources().len(), 1);
    assert!(dag.is_finalized());

    // Verify topological order
    let order = dag.execution_order();
    let pos = |name: &str| {
        order
            .iter()
            .position(|&id| dag.node_name(id).unwrap() == name)
            .unwrap()
    };
    assert!(pos("trades") < pos("ohlc_1s"));
    assert!(pos("ohlc_1s") < pos("ohlc_1m"));

    // Node types
    let trades_id = dag.node_id_by_name("trades").unwrap();
    assert_eq!(dag.node(trades_id).unwrap().node_type, DagNodeType::Source);
    let ohlc_id = dag.node_id_by_name("ohlc_1s").unwrap();
    assert_eq!(
        dag.node(ohlc_id).unwrap().node_type,
        DagNodeType::MaterializedView
    );
}

#[test]
fn test_dag_from_mv_registry_fan_out() {
    // trades -> {vwap, anomaly, position} (fan-out, SPMC)
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");

    let schema = make_mv_schema();
    for name in &["vwap", "anomaly", "position"] {
        registry
            .register(MaterializedView::new(
                *name,
                "SELECT ...",
                vec!["trades".into()],
                schema.clone(),
            ))
            .unwrap();
    }

    let base_schemas = make_base_schemas(&["trades"]);
    let dag = StreamingDag::from_mv_registry(&registry, &base_schemas).unwrap();

    assert_eq!(dag.node_count(), 4); // trades + 3 MVs
    assert_eq!(dag.edge_count(), 3);

    // trades has fan-out > 1, so it should be a shared stage
    let trades_id = dag.node_id_by_name("trades").unwrap();
    assert!(dag.shared_stages().contains_key(&trades_id));
    assert_eq!(dag.shared_stages()[&trades_id].consumer_count, 3);

    // Edges from trades should be SPMC
    let trades_node = dag.node(trades_id).unwrap();
    for &eid in &trades_node.outputs {
        assert_eq!(dag.edge(eid).unwrap().channel_type, DagChannelType::Spmc);
    }
}

#[test]
fn test_dag_from_mv_registry_empty() {
    let registry = MvRegistry::new();
    let base_schemas = FxHashMap::default();
    let result = StreamingDag::from_mv_registry(&registry, &base_schemas);
    assert!(matches!(result, Err(DagError::EmptyDag)));
}

#[test]
fn test_dag_from_mv_registry_single_mv() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("events");

    let schema = make_mv_schema();
    registry
        .register(MaterializedView::new(
            "count_mv",
            "SELECT COUNT(*) FROM events",
            vec!["events".into()],
            schema,
        ))
        .unwrap();

    let base_schemas = make_base_schemas(&["events"]);
    let dag = StreamingDag::from_mv_registry(&registry, &base_schemas).unwrap();

    assert_eq!(dag.node_count(), 2);
    assert_eq!(dag.edge_count(), 1);
    assert_eq!(dag.sources().len(), 1);
}

#[test]
fn test_dag_from_mv_registry_diamond() {
    //       source
    //       /    \
    //      a      b
    //       \    /
    //         c
    let mut registry = MvRegistry::new();
    registry.register_base_table("source");

    let schema = make_mv_schema();
    registry
        .register(MaterializedView::new(
            "a",
            "SELECT ...",
            vec!["source".into()],
            schema.clone(),
        ))
        .unwrap();
    registry
        .register(MaterializedView::new(
            "b",
            "SELECT ...",
            vec!["source".into()],
            schema.clone(),
        ))
        .unwrap();
    registry
        .register(MaterializedView::new(
            "c",
            "SELECT ...",
            vec!["a".into(), "b".into()],
            schema,
        ))
        .unwrap();

    let base_schemas = make_base_schemas(&["source"]);
    let dag = StreamingDag::from_mv_registry(&registry, &base_schemas).unwrap();

    assert_eq!(dag.node_count(), 4); // source, a, b, c
    assert_eq!(dag.edge_count(), 4); // source->a, source->b, a->c, b->c

    // c is a fan-in node: edges to it should be MPSC
    let c_id = dag.node_id_by_name("c").unwrap();
    let c_node = dag.node(c_id).unwrap();
    assert_eq!(c_node.inputs.len(), 2);
    for &eid in &c_node.inputs {
        assert_eq!(dag.edge(eid).unwrap().channel_type, DagChannelType::Mpsc);
    }

    // source has fan-out > 1: shared stage with SPMC edges
    let source_id = dag.node_id_by_name("source").unwrap();
    assert!(dag.shared_stages().contains_key(&source_id));
}

#[test]
fn test_dag_from_mv_registry_missing_schema() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");

    let schema = make_mv_schema();
    registry
        .register(MaterializedView::new(
            "ohlc",
            "SELECT ...",
            vec!["trades".into()],
            schema,
        ))
        .unwrap();

    // Empty schemas map — should fail
    let base_schemas = FxHashMap::default();
    let result = StreamingDag::from_mv_registry(&registry, &base_schemas);
    assert!(matches!(result, Err(DagError::BaseTableSchemaNotFound(_))));
}

// ---- DagWatermarkTracker tests ----

#[test]
fn test_watermark_linear_propagation() {
    // src -> a -> b (linear chain)
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .sink_for("a", "b", schema.clone())
        .connect("src", "a")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    // Initially no watermarks
    assert_eq!(tracker.get_watermark(src_id), None);
    assert_eq!(tracker.get_watermark(a_id), None);

    // Update source watermark
    let updated = tracker.update_watermark(src_id, 1000);
    assert!(!updated.is_empty());

    // Source watermark should be set
    assert_eq!(tracker.get_watermark(src_id), Some(1000));

    // Downstream nodes should have propagated watermark
    assert_eq!(tracker.get_watermark(a_id), Some(1000));
    assert_eq!(tracker.get_watermark(b_id), Some(1000));
}

#[test]
fn test_watermark_fan_in_min() {
    // {src_a, src_b} -> merge -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src_a", "merge")
        .connect("src_b", "merge")
        .build()
        .unwrap();

    let src_a = dag.node_id_by_name("src_a").unwrap();
    let src_b = dag.node_id_by_name("src_b").unwrap();
    let merge_id = dag.node_id_by_name("merge").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    // Only src_a advances — merge should NOT advance (src_b is unset)
    tracker.update_watermark(src_a, 1000);
    assert_eq!(tracker.get_watermark(merge_id), None);

    // Now src_b advances — merge should get min(1000, 500) = 500
    tracker.update_watermark(src_b, 500);
    assert_eq!(tracker.get_watermark(merge_id), Some(500));

    // Advance src_b past src_a — merge should stay at 1000 (limited by src_a)
    tracker.update_watermark(src_b, 2000);
    assert_eq!(tracker.get_watermark(merge_id), Some(1000));
}

#[test]
fn test_watermark_fan_out_forward() {
    // src -> {a, b} -> {sink_a, sink_b}
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);
    tracker.update_watermark(src_id, 5000);

    // Both branches should receive the watermark
    assert_eq!(tracker.get_watermark(a_id), Some(5000));
    assert_eq!(tracker.get_watermark(b_id), Some(5000));
}

#[test]
fn test_watermark_independent_branches() {
    // src_a -> branch_a -> sink_a
    // src_b -> branch_b -> sink_b
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("branch_a", schema.clone())
        .operator("branch_b", schema.clone())
        .sink_for("branch_a", "sink_a", schema.clone())
        .sink_for("branch_b", "sink_b", schema.clone())
        .connect("src_a", "branch_a")
        .connect("src_b", "branch_b")
        .build()
        .unwrap();

    let src_a = dag.node_id_by_name("src_a").unwrap();
    let src_b = dag.node_id_by_name("src_b").unwrap();
    let branch_a = dag.node_id_by_name("branch_a").unwrap();
    let branch_b = dag.node_id_by_name("branch_b").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    tracker.update_watermark(src_a, 1000);
    tracker.update_watermark(src_b, 5000);

    // Each branch has its own independent watermark
    assert_eq!(tracker.get_watermark(branch_a), Some(1000));
    assert_eq!(tracker.get_watermark(branch_b), Some(5000));
}

#[test]
fn test_watermark_checkpoint_restore() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);
    tracker.update_watermark(src_id, 3000);

    // Checkpoint
    let checkpoint = tracker.checkpoint();

    // Create a fresh tracker and restore
    let mut tracker2 = DagWatermarkTracker::from_dag(&dag);
    assert_eq!(tracker2.get_watermark(src_id), None);

    tracker2.restore(&checkpoint);
    assert_eq!(tracker2.get_watermark(src_id), Some(3000));
    assert_eq!(tracker2.get_watermark(op_id), Some(3000));
}

// ---- DagChangelogPropagator tests ----

#[test]
fn test_changelog_record_output() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let op_id = dag.node_id_by_name("op").unwrap();

    let mut propagator = DagChangelogPropagator::from_dag(&dag, 1024);

    // Record inserts and deletes
    assert!(propagator.record(op_id, ChangelogRef::insert(0, 0)));
    assert!(propagator.record(op_id, ChangelogRef::insert(0, 1)));
    assert!(propagator.record(op_id, ChangelogRef::delete(0, 2)));

    assert_eq!(propagator.pending_count(op_id), 3);
    assert!(propagator.has_pending());

    // Drain
    let refs = propagator.drain_node(op_id);
    assert_eq!(refs.len(), 3);
    assert!(refs[0].is_insert());
    assert!(refs[1].is_insert());
    assert!(refs[2].is_delete());

    // After drain, buffer should be empty
    assert_eq!(propagator.pending_count(op_id), 0);
    assert!(!propagator.has_pending());
}

#[test]
fn test_changelog_drain() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .sink_for("a", "sink_a", schema.clone())
        .sink_for("b", "sink_b", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .build()
        .unwrap();

    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();

    let mut propagator = DagChangelogPropagator::from_dag(&dag, 1024);

    propagator.record(a_id, ChangelogRef::insert(0, 0));
    propagator.record(b_id, ChangelogRef::insert(0, 0));
    propagator.record(b_id, ChangelogRef::insert(0, 1));

    assert!(propagator.has_pending());

    // drain_all clears everything
    propagator.drain_all();
    assert!(!propagator.has_pending());
    assert_eq!(propagator.pending_count(a_id), 0);
    assert_eq!(propagator.pending_count(b_id), 0);
}

#[test]
fn test_changelog_disabled() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    // Disabled propagator
    let mut propagator = DagChangelogPropagator::disabled(dag.node_count());

    // Recording should return false (disabled)
    assert!(!propagator.record(src_id, ChangelogRef::insert(0, 0)));
    assert!(!propagator.has_pending());
    assert_eq!(propagator.pending_count(src_id), 0);
    assert!(!propagator.is_globally_enabled());

    // Re-enable globally
    propagator.set_globally_enabled(true);
    // Still node-level disabled
    assert!(!propagator.record(src_id, ChangelogRef::insert(0, 0)));

    // Enable node
    propagator.set_node_enabled(src_id, true);
    // Now the buffer capacity is 0 (created with disabled()), so push returns false
    assert!(!propagator.record(src_id, ChangelogRef::insert(0, 0)));
}

#[test]
fn test_changelog_retraction() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let op_id = dag.node_id_by_name("op").unwrap();

    let mut propagator = DagChangelogPropagator::from_dag(&dag, 1024);

    // Record a retraction pair
    assert!(propagator.record_retraction(op_id, 0, 5, 10));

    // Should have 2 entries (update_before + update_after)
    assert_eq!(propagator.pending_count(op_id), 2);

    let refs = propagator.drain_node(op_id);
    assert_eq!(refs.len(), 2);
    // First is delete (old value), second is insert (new value)
    assert!(refs[0].is_delete());
    assert!(refs[1].is_insert());
}

// ---- Effective watermark tests ----

#[test]
fn test_watermark_effective_watermark() {
    // {src_a, src_b} -> merge -> snk
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("merge", schema.clone())
        .sink_for("merge", "snk", schema.clone())
        .connect("src_a", "merge")
        .connect("src_b", "merge")
        .build()
        .unwrap();

    let src_a = dag.node_id_by_name("src_a").unwrap();
    let src_b = dag.node_id_by_name("src_b").unwrap();
    let merge_id = dag.node_id_by_name("merge").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    // Effective watermark for merge: None (neither input has watermark)
    assert_eq!(tracker.effective_watermark(merge_id), None);

    // Set src_a only — merge still None (src_b missing)
    tracker.update_watermark(src_a, 1000);
    assert_eq!(tracker.effective_watermark(merge_id), None);

    // Set src_b — merge effective = min(1000, 500)
    tracker.update_watermark(src_b, 500);
    assert_eq!(tracker.effective_watermark(merge_id), Some(500));

    // Source node effective watermark = own watermark
    assert_eq!(tracker.effective_watermark(src_a), Some(1000));
}

#[test]
fn test_watermark_propagation_updated_buffer() {
    // src -> a -> b
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .sink_for("a", "b", schema.clone())
        .connect("src", "a")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    // Initial update
    let updated = tracker.update_watermark(src_id, 100).to_vec();
    assert_eq!(updated.len(), 3);
    assert!(updated.contains(&(src_id, 100)));
    assert!(updated.contains(&(a_id, 100)));
    assert!(updated.contains(&(b_id, 100)));

    // Update with same value - should return empty
    let updated = tracker.update_watermark(src_id, 100);
    assert!(updated.is_empty());

    // Update with smaller value - should return empty
    let updated = tracker.update_watermark(src_id, 50);
    assert!(updated.is_empty());

    // Advancement
    let updated = tracker.update_watermark(src_id, 200).to_vec();
    assert_eq!(updated.len(), 3);
    assert!(updated.contains(&(src_id, 200)));
    assert!(updated.contains(&(a_id, 200)));
    assert!(updated.contains(&(b_id, 200)));
}

#[test]
fn test_watermark_diamond_propagation() {
    //      src
    //     /   \
    //    a     b
    //     \   /
    //     merge
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .connect("src", "a")
        .connect("src", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .sink_for("merge", "snk", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let a_id = dag.node_id_by_name("a").unwrap();
    let b_id = dag.node_id_by_name("b").unwrap();
    let merge_id = dag.node_id_by_name("merge").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    // src advances -> a and b advance, and since they are the only inputs to merge,
    // merge and snk also advance.
    let updated = tracker.update_watermark(src_id, 1000).to_vec();
    assert_eq!(updated.len(), 5);
    assert!(updated.contains(&(src_id, 1000)));
    assert!(updated.contains(&(a_id, 1000)));
    assert!(updated.contains(&(b_id, 1000)));
    assert!(updated.contains(&(merge_id, 1000)));
    assert!(updated.contains(&(snk_id, 1000)));
}

#[test]
fn test_watermark_complex_fan_in_partial_advancement() {
    // src1 -> a \
    //             merge -> snk
    // src2 -> b /
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src1", schema.clone())
        .source("src2", schema.clone())
        .operator("a", schema.clone())
        .operator("b", schema.clone())
        .operator("merge", schema.clone())
        .connect("src1", "a")
        .connect("src2", "b")
        .connect("a", "merge")
        .connect("b", "merge")
        .sink_for("merge", "snk", schema.clone())
        .build()
        .unwrap();

    let src1 = dag.node_id_by_name("src1").unwrap();
    let src2 = dag.node_id_by_name("src2").unwrap();
    let a = dag.node_id_by_name("a").unwrap();
    let b = dag.node_id_by_name("b").unwrap();
    let merge = dag.node_id_by_name("merge").unwrap();
    let snk = dag.node_id_by_name("snk").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    // 1. Initialize src1
    tracker.update_watermark(src1, 1000);
    assert_eq!(tracker.get_watermark(a), Some(1000));
    assert_eq!(tracker.get_watermark(merge), None);

    // 2. Initialize src2 with a lower value
    let updated = tracker.update_watermark(src2, 500).to_vec();
    assert!(updated.contains(&(src2, 500)));
    assert!(updated.contains(&(b, 500)));
    assert!(updated.contains(&(merge, 500)));
    assert!(updated.contains(&(snk, 500)));

    // 3. Advance src2 but still below src1
    let updated = tracker.update_watermark(src2, 800).to_vec();
    assert_eq!(updated.len(), 4);
    assert!(updated.contains(&(src2, 800)));
    assert!(updated.contains(&(b, 800)));
    assert!(updated.contains(&(merge, 800)));
    assert!(updated.contains(&(snk, 800)));

    // 4. Advance src2 past src1
    let updated = tracker.update_watermark(src2, 1200).to_vec();
    assert_eq!(updated.len(), 4);
    assert!(updated.contains(&(src2, 1200)));
    assert!(updated.contains(&(b, 1200)));
    assert!(updated.contains(&(merge, 1000))); // limited by a=1000
    assert!(updated.contains(&(snk, 1000)));

    assert_eq!(tracker.get_watermark(merge), Some(1000));
}

#[test]
fn test_watermark_out_of_bounds_node() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema)
        .build()
        .unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);
    let updated = tracker.update_watermark(NodeId(999), 1000);
    assert!(updated.is_empty());

    assert_eq!(tracker.get_watermark(NodeId(999)), None);
    assert_eq!(tracker.effective_watermark(NodeId(999)), None);
}

// --- process_watermark() tests ---

#[test]
fn test_process_watermark_advances_generators() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .connect("src", "op")
        .sink_for("op", "snk", schema)
        .build()
        .unwrap();

    let mut executor = DagExecutor::from_dag(&dag);

    // Find the source node
    let src_id = executor.source_nodes()[0];

    // Process watermark
    executor.process_watermark(src_id, 5000).unwrap();
    #[cfg(feature = "dag-metrics")]
    assert_eq!(executor.metrics().watermarks_processed, 1);

    // Process another watermark
    executor.process_watermark(src_id, 8000).unwrap();
    #[cfg(feature = "dag-metrics")]
    assert_eq!(executor.metrics().watermarks_processed, 2);
}

#[test]
fn test_process_watermark_invalid_node() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .sink_for("src", "snk", schema)
        .build()
        .unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    let result = executor.process_watermark(NodeId(999), 5000);
    assert!(result.is_err());
}

// ---- EOWC (Emit On Window Close) DAG-level integration tests ----

/// Build a tumbling window operator with the given emit strategy.
fn make_eowc_window_operator(
    emit_strategy: crate::operator::window::EmitStrategy,
) -> Box<dyn crate::operator::Operator> {
    use crate::operator::window::{
        CountAggregator, TumblingWindowAssigner, TumblingWindowOperator,
    };
    use std::time::Duration;

    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut op = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "dag_eowc_window".to_string(),
    );
    op.set_emit_strategy(emit_strategy);
    Box::new(op)
}

/// EOWC: Source → TumblingWindow(OnWindowClose) → Sink
///
/// Verifies that events processed through the DAG produce no intermediate
/// output, and that firing timers after watermark advancement produces
/// exactly one emission per window.
#[test]
fn test_eowc_dag_no_intermediate_single_emission() {
    use crate::operator::window::EmitStrategy;

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("window", schema.clone())
        .sink_for("window", "snk", schema)
        .connect("src", "window")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let window_id = dag.node_id_by_name("window").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(
        window_id,
        make_eowc_window_operator(EmitStrategy::OnWindowClose),
    );

    // Process 5 events into window [0, 1000)
    for i in 0..5 {
        executor
            .process_event(src_id, test_event(i * 200, 1))
            .unwrap();
    }

    // No intermediate emissions for OnWindowClose
    let outputs = executor.take_sink_outputs(snk_id);
    assert!(
        outputs.is_empty(),
        "OnWindowClose should not emit intermediate results, got {} events",
        outputs.len()
    );

    // Advance watermark past window end to trigger timer
    executor.process_watermark(src_id, 1100).unwrap();
    executor.fire_timers(1100);

    // Now should have exactly one emission
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(
        outputs.len(),
        1,
        "Expected exactly 1 window emission, got {}",
        outputs.len()
    );
}

/// EOWC: Multiple windows through DAG
///
/// Feeds events into 3 tumbling windows [0,1000), [1000,2000), [2000,3000)
/// and verifies that firing timers produces exactly 3 emissions.
#[test]
fn test_eowc_dag_multiple_windows() {
    use crate::operator::window::EmitStrategy;

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("window", schema.clone())
        .sink_for("window", "snk", schema)
        .connect("src", "window")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let window_id = dag.node_id_by_name("window").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(
        window_id,
        make_eowc_window_operator(EmitStrategy::OnWindowClose),
    );

    // Window [0, 1000): 3 events
    for i in 0..3 {
        executor
            .process_event(src_id, test_event(i * 300, 1))
            .unwrap();
    }
    // Window [1000, 2000): 2 events
    for i in 0..2 {
        executor
            .process_event(src_id, test_event(1000 + i * 400, 1))
            .unwrap();
    }
    // Window [2000, 3000): 1 event
    executor.process_event(src_id, test_event(2500, 1)).unwrap();

    // No outputs yet (OnWindowClose)
    assert!(executor.take_sink_outputs(snk_id).is_empty());

    // Advance watermark past all windows
    executor.process_watermark(src_id, 3100).unwrap();
    executor.fire_timers(3100);

    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(
        outputs.len(),
        3,
        "Expected 3 window emissions, got {}",
        outputs.len()
    );
}

/// EOWC: Checkpoint and restore in DAG
///
/// Processes events, checkpoints, restores to a fresh executor,
/// processes a new event into a new window, and verifies that the
/// new window emits correctly after restore.
///
/// Note: Timer re-registration after restore for existing windows is
/// a known gap in tumbling windows (session windows handle this via
/// `needs_timer_reregistration`). This test verifies that checkpoint/
/// restore preserves the `registered_windows` set and that new windows
/// created after restore work correctly end-to-end.
#[test]
fn test_eowc_dag_checkpoint_restore() {
    use crate::operator::window::EmitStrategy;

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("window", schema.clone())
        .sink_for("window", "snk", schema)
        .connect("src", "window")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let window_id = dag.node_id_by_name("window").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    // Phase 1: Process events and checkpoint
    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(
        window_id,
        make_eowc_window_operator(EmitStrategy::OnWindowClose),
    );

    for i in 0..4 {
        executor
            .process_event(src_id, test_event(i * 200, 1))
            .unwrap();
    }

    let states = executor.checkpoint();
    assert!(
        states.contains_key(&window_id),
        "Checkpoint should contain window operator state"
    );

    // Phase 2: Restore into a fresh executor
    let mut executor2 = DagExecutor::from_dag(&dag);
    executor2.register_operator(
        window_id,
        make_eowc_window_operator(EmitStrategy::OnWindowClose),
    );
    executor2.restore(&states).unwrap();

    // After restore, process events into a NEW window [1000, 2000).
    // This creates a fresh timer registration and verifies the operator
    // is functional post-restore.
    for i in 0..3 {
        executor2
            .process_event(src_id, test_event(1000 + i * 300, 1))
            .unwrap();
    }

    // No intermediate output yet (OnWindowClose)
    assert!(executor2.take_sink_outputs(snk_id).is_empty());

    // Fire timers for the new window
    executor2.process_watermark(src_id, 2100).unwrap();
    executor2.fire_timers(2100);

    let outputs = executor2.take_sink_outputs(snk_id);
    assert!(
        !outputs.is_empty(),
        "Expected at least 1 emission for new window after checkpoint-restore"
    );
}

/// EOWC vs `OnWatermark` comparison at DAG level
///
/// Both strategies should produce the same final result for a single window,
/// but `OnWatermark` may emit intermediate results while `OnWindowClose` does not.
#[test]
fn test_eowc_dag_vs_on_watermark() {
    use crate::operator::window::EmitStrategy;

    let schema = int_schema();

    // --- OnWindowClose pipeline ---
    let dag_eowc = DagBuilder::new()
        .source("src", schema.clone())
        .operator("window", schema.clone())
        .sink_for("window", "snk", schema.clone())
        .connect("src", "window")
        .build()
        .unwrap();

    let src_eowc = dag_eowc.node_id_by_name("src").unwrap();
    let win_eowc = dag_eowc.node_id_by_name("window").unwrap();
    let snk_eowc = dag_eowc.node_id_by_name("snk").unwrap();

    let mut exec_eowc = DagExecutor::from_dag(&dag_eowc);
    exec_eowc.register_operator(
        win_eowc,
        make_eowc_window_operator(EmitStrategy::OnWindowClose),
    );

    // --- OnWatermark pipeline ---
    let dag_wm = DagBuilder::new()
        .source("src", schema.clone())
        .operator("window", schema.clone())
        .sink_for("window", "snk", schema)
        .connect("src", "window")
        .build()
        .unwrap();

    let src_wm = dag_wm.node_id_by_name("src").unwrap();
    let win_wm = dag_wm.node_id_by_name("window").unwrap();
    let snk_wm = dag_wm.node_id_by_name("snk").unwrap();

    let mut exec_wm = DagExecutor::from_dag(&dag_wm);
    exec_wm.register_operator(win_wm, make_eowc_window_operator(EmitStrategy::OnWatermark));

    // Process same events into both
    for i in 0..5 {
        let ts = i * 150;
        exec_eowc
            .process_event(src_eowc, test_event(ts, 1))
            .unwrap();
        exec_wm.process_event(src_wm, test_event(ts, 1)).unwrap();
    }

    // Fire timers for both
    exec_eowc.process_watermark(src_eowc, 1100).unwrap();
    exec_eowc.fire_timers(1100);

    exec_wm.process_watermark(src_wm, 1100).unwrap();
    exec_wm.fire_timers(1100);

    let eowc_outputs = exec_eowc.take_sink_outputs(snk_eowc);
    let wm_outputs = exec_wm.take_sink_outputs(snk_wm);

    // Both should produce at least one output per window
    assert!(
        !eowc_outputs.is_empty(),
        "OnWindowClose should emit after timer fire"
    );
    assert!(
        !wm_outputs.is_empty(),
        "OnWatermark should emit after timer fire"
    );

    // EOWC: exactly 1 emission (no intermediate + 1 final)
    assert_eq!(
        eowc_outputs.len(),
        1,
        "OnWindowClose should produce exactly 1 emission"
    );
}

/// `fire_timers`: basic test that it fires timers and routes outputs.
#[test]
fn test_fire_timers_routes_outputs() {
    use crate::operator::window::{
        CountAggregator, EmitStrategy, TumblingWindowAssigner, TumblingWindowOperator,
    };
    use std::time::Duration;

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("window", schema.clone())
        .sink_for("window", "snk", schema)
        .connect("src", "window")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let window_id = dag.node_id_by_name("window").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);

    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();
    let mut op = TumblingWindowOperator::with_id(
        assigner,
        aggregator,
        Duration::from_millis(0),
        "fire_timer_test".to_string(),
    );
    op.set_emit_strategy(EmitStrategy::OnWatermark);
    executor.register_operator(window_id, Box::new(op));

    // Inject events
    for i in 0..10 {
        executor
            .process_event(src_id, test_event(i * 100, 1))
            .unwrap();
    }

    // No fire yet — timers not due
    executor.fire_timers(500);
    assert!(executor.take_sink_outputs(snk_id).is_empty());

    // Fire at window end time
    executor.fire_timers(1000);
    let outputs = executor.take_sink_outputs(snk_id);
    assert_eq!(
        outputs.len(),
        1,
        "Expected 1 window emission from timer fire"
    );
}

// -- Per-operator node metrics tests (#28B) --

#[test]
#[cfg(feature = "dag-metrics")]
fn test_operator_node_metrics_accumulate() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);

    // Process 3 events
    for i in 0..3 {
        executor
            .process_event(src_id, test_event(1000 + i, i))
            .unwrap();
    }

    let nm = executor.node_metrics();

    // Source node (passthrough): receives and forwards 3 events
    let src_m = &nm[src_id.0 as usize];
    assert_eq!(src_m.events_in, 3, "src events_in");
    assert_eq!(src_m.events_out, 3, "src events_out");

    // Operator node (passthrough, no operator registered): receives and forwards 3 events
    let op_m = &nm[op_id.0 as usize];
    assert_eq!(op_m.events_in, 3, "op events_in");
    assert_eq!(op_m.events_out, 3, "op events_out");
    assert_eq!(op_m.invocations, 3, "op invocations");
}

#[test]
#[cfg(feature = "dag-metrics")]
fn test_operator_node_metrics_timing() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor
        .process_event(src_id, test_event(1000, 42))
        .unwrap();

    let nm = executor.node_metrics();
    let op_m = &nm[op_id.0 as usize];

    // Timing should be > 0 after processing at least one event
    assert!(op_m.total_time_ns > 0, "total_time_ns should be > 0");
    assert_eq!(op_m.invocations, 1);
}

#[test]
#[cfg(feature = "dag-metrics")]
fn test_operator_node_metrics_reset() {
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.process_event(src_id, test_event(1000, 1)).unwrap();

    assert!(executor.node_metrics()[op_id.0 as usize].events_in > 0);

    executor.reset_metrics();

    let op_m = &executor.node_metrics()[op_id.0 as usize];
    assert_eq!(op_m.events_in, 0);
    assert_eq!(op_m.events_out, 0);
    assert_eq!(op_m.total_time_ns, 0);
    assert_eq!(op_m.invocations, 0);
}

// ---- Watermark topology propagation, output routing, and feedback tests ----

#[test]
fn test_watermark_topology_propagation() {
    // Build a 3-node DAG: source -> op -> sink
    // Call process_watermark(source, 1000) and verify propagation.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);

    // Before watermark: checkpoint should show unset (i64::MIN) for all nodes
    let cp_before = executor.checkpoint_watermarks();
    assert_eq!(cp_before.watermarks[src_id.0 as usize], i64::MIN);
    assert_eq!(cp_before.watermarks[op_id.0 as usize], i64::MIN);
    assert_eq!(cp_before.watermarks[snk_id.0 as usize], i64::MIN);

    // Process watermark at the source
    executor.process_watermark(src_id, 1000).unwrap();

    // After watermark: all nodes in the topology should have watermark 1000
    let cp_after = executor.checkpoint_watermarks();
    assert_eq!(cp_after.watermarks[src_id.0 as usize], 1000);
    assert_eq!(cp_after.watermarks[op_id.0 as usize], 1000);
    assert_eq!(cp_after.watermarks[snk_id.0 as usize], 1000);
}

/// An operator that emits a `Changelog(ChangelogRecord::insert(...))` for each input.
struct ChangelogEmittingOperator;

impl Operator for ChangelogEmittingOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        use crate::operator::window::ChangelogRecord;
        let mut v = OutputVec::new();
        v.push(Output::Changelog(ChangelogRecord::insert(
            event.clone(),
            0,
        )));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "changelog_emitter".to_string(),
            data: Vec::new(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

#[test]
fn test_changelog_output_routing() {
    // src -> op (emits Changelog) -> snk
    // Changelog events should be collected in take_sink_outputs for the op node.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(op_id, Box::new(ChangelogEmittingOperator));

    executor
        .process_event(src_id, test_event(1000, 42))
        .unwrap();

    // Changelog outputs are collected at the operator node's sink_outputs slot
    let outputs = executor.take_sink_outputs(op_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 42);
    assert_eq!(outputs[0].timestamp, 1000);
}

/// An operator that emits a `SideOutput { name: "late", event }` for each input.
struct SideOutputEmittingOperator;

impl Operator for SideOutputEmittingOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let mut v = OutputVec::new();
        v.push(Output::SideOutput {
            name: "late".to_string(),
            event: event.clone(),
        });
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "side_output_emitter".to_string(),
            data: Vec::new(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

#[test]
fn test_side_output_routing() {
    // src -> op (emits SideOutput) -> snk
    // Side output events should be collected at the op node's sink_outputs.
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(op_id, Box::new(SideOutputEmittingOperator));

    executor
        .process_event(src_id, test_event(2000, 77))
        .unwrap();

    // Side output events are collected at the operator node
    let outputs = executor.take_sink_outputs(op_id);
    assert_eq!(outputs.len(), 1);
    assert_eq!(event_value(&outputs[0]), 77);
    assert_eq!(outputs[0].timestamp, 2000);
}

/// An operator that emits `Output::Watermark(event.timestamp)` from `process()`.
struct WatermarkEmittingOperator;

impl Operator for WatermarkEmittingOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        let mut v = OutputVec::new();
        // Emit the event's timestamp as a watermark
        v.push(Output::Watermark(event.timestamp));
        v
    }
    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        OutputVec::new()
    }
    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "watermark_emitter".to_string(),
            data: Vec::new(),
        }
    }
    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

#[test]
fn test_watermark_output_feedback() {
    // src -> op (emits Watermark from process) -> snk
    // The operator emits Output::Watermark(timestamp) when processing an event.
    // This should feed back into the watermark tracker for the source node (op).
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("op", schema.clone())
        .sink_for("op", "snk", schema.clone())
        .connect("src", "op")
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let op_id = dag.node_id_by_name("op").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut executor = DagExecutor::from_dag(&dag);
    executor.register_operator(op_id, Box::new(WatermarkEmittingOperator));

    // Process an event — the operator will emit Output::Watermark(3000)
    executor
        .process_event(src_id, test_event(3000, 1))
        .unwrap();

    // The watermark tracker should have the watermark for the op node (source of the
    // watermark output) and it should propagate to snk.
    let cp = executor.checkpoint_watermarks();
    assert_eq!(cp.watermarks[op_id.0 as usize], 3000);
    assert_eq!(cp.watermarks[snk_id.0 as usize], 3000);
}

#[test]
fn test_multi_input_watermark_min() {
    // Build a DAG with 2 sources merging into 1 join node (fan-in):
    //   src_a \
    //          -> join -> snk
    //   src_b /
    //
    // Advance src_a watermark to 1000, src_b to 500.
    // The effective watermark at the join node should be 500 (min semantics).
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src_a", schema.clone())
        .source("src_b", schema.clone())
        .operator("join", schema.clone())
        .sink_for("join", "snk", schema.clone())
        .connect("src_a", "join")
        .connect("src_b", "join")
        .build()
        .unwrap();

    let src_a = dag.node_id_by_name("src_a").unwrap();
    let src_b = dag.node_id_by_name("src_b").unwrap();
    let join_id = dag.node_id_by_name("join").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let mut tracker = DagWatermarkTracker::from_dag(&dag);

    // Only src_a advances first — join should NOT advance (src_b is still unset)
    tracker.update_watermark(src_a, 1000);
    assert_eq!(tracker.get_watermark(join_id), None);

    // Now src_b advances to 500 — join should get min(1000, 500) = 500
    tracker.update_watermark(src_b, 500);
    assert_eq!(tracker.get_watermark(join_id), Some(500));

    // Verify effective_watermark agrees
    assert_eq!(tracker.effective_watermark(join_id), Some(500));

    // The snk downstream of join should also get 500
    assert_eq!(tracker.get_watermark(snk_id), Some(500));

    // Now advance src_b to 2000 — join should advance to min(1000, 2000) = 1000
    tracker.update_watermark(src_b, 2000);
    assert_eq!(tracker.get_watermark(join_id), Some(1000));
    assert_eq!(tracker.effective_watermark(join_id), Some(1000));
    assert_eq!(tracker.get_watermark(snk_id), Some(1000));
}
