#![allow(clippy::disallowed_types)] // test code uses std HashMap

use std::collections::HashMap;

use rustc_hash::FxHashMap;

use crate::dag::topology::NodeId;
use crate::operator::OperatorState;

use super::DagCheckpointResult;

// ---- DagCheckpointResult tests ----

#[test]
fn test_result_from_operator_states() {
    let mut states = FxHashMap::default();
    states.insert(
        NodeId(1),
        OperatorState {
            operator_id: "op1".to_string(),
            version: 1,
            data: vec![10],
        },
    );
    states.insert(
        NodeId(2),
        OperatorState {
            operator_id: "op2".to_string(),
            version: 1,
            data: vec![20],
        },
    );

    let result = DagCheckpointResult::from_operator_states(100, 5, 999, &states);

    assert_eq!(result.checkpoint_id, 100);
    assert_eq!(result.epoch, 5);
    assert_eq!(result.timestamp_ms, 999);
    assert_eq!(result.operator_states.len(), 2);

    assert_eq!(result.operator_states["op1"], vec![10]);
    assert_eq!(result.operator_states["op2"], vec![20]);
}

#[test]
fn test_to_operator_states_by_name() {
    let mut operator_states = HashMap::new();
    operator_states.insert("op_a".to_string(), vec![1, 2, 3]);
    operator_states.insert("op_b".to_string(), vec![4, 5]);

    let result = DagCheckpointResult {
        checkpoint_id: 1,
        epoch: 1,
        timestamp_ms: 0,
        operator_states,
    };

    let mut name_to_node = HashMap::new();
    name_to_node.insert("op_a".to_string(), NodeId(10));
    name_to_node.insert("op_b".to_string(), NodeId(20));

    let states = result.to_operator_states(&name_to_node);
    assert_eq!(states.len(), 2);
    assert_eq!(states[&NodeId(10)].operator_id, "op_a");
    assert_eq!(states[&NodeId(10)].data, vec![1, 2, 3]);
    assert_eq!(states[&NodeId(20)].operator_id, "op_b");
    assert_eq!(states[&NodeId(20)].data, vec![4, 5]);
}

#[test]
fn test_to_operator_states_missing_name_skipped() {
    let mut operator_states = HashMap::new();
    operator_states.insert("known".to_string(), vec![1]);
    operator_states.insert("unknown".to_string(), vec![2]);

    let result = DagCheckpointResult {
        checkpoint_id: 1,
        epoch: 1,
        timestamp_ms: 0,
        operator_states,
    };

    let mut name_to_node = HashMap::new();
    name_to_node.insert("known".to_string(), NodeId(5));
    // "unknown" is not in the mapping

    let states = result.to_operator_states(&name_to_node);
    assert_eq!(states.len(), 1);
    assert!(states.contains_key(&NodeId(5)));
}

#[test]
fn test_to_operator_states_by_index() {
    let mut operator_states = HashMap::new();
    operator_states.insert("1".to_string(), vec![10]);
    operator_states.insert("3".to_string(), vec![30]);
    operator_states.insert("not_a_number".to_string(), vec![99]);

    let result = DagCheckpointResult {
        checkpoint_id: 1,
        epoch: 1,
        timestamp_ms: 0,
        operator_states,
    };

    let states = result.to_operator_states_by_index();
    assert_eq!(states.len(), 2);
    assert_eq!(states[&NodeId(1)].data, vec![10]);
    assert_eq!(states[&NodeId(3)].data, vec![30]);
    // "not_a_number" is skipped
}

#[test]
fn test_result_empty_states() {
    let result = DagCheckpointResult {
        checkpoint_id: 42,
        epoch: 7,
        timestamp_ms: 12345,
        operator_states: HashMap::new(),
    };

    assert_eq!(result.checkpoint_id, 42);
    assert_eq!(result.epoch, 7);
    assert!(result.operator_states.is_empty());
    assert!(result.to_operator_states_by_index().is_empty());
    assert!(result.to_operator_states(&HashMap::new()).is_empty());
}

#[test]
fn test_round_trip_through_coordinator_format() {
    // Simulate: DagExecutor checkpoint → DagCheckpointResult → restore
    let mut original_states = FxHashMap::default();
    original_states.insert(
        NodeId(0),
        OperatorState::v1("0".to_string(), vec![0xAA, 0xBB]),
    );
    original_states.insert(NodeId(5), OperatorState::v1("5".to_string(), vec![0xCC]));

    // Create result (what DagCheckpointCoordinator::finalize_checkpoint() produces)
    let result = DagCheckpointResult::from_operator_states(1, 1, 1000, &original_states);

    // Convert back (what DagExecutor::restore() needs)
    let restored = result.to_operator_states_by_index();

    assert_eq!(restored.len(), 2);
    assert_eq!(restored[&NodeId(0)].data, vec![0xAA, 0xBB]);
    assert_eq!(restored[&NodeId(5)].data, vec![0xCC]);
}
