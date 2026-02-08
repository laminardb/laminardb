use std::collections::HashMap;

use fxhash::FxHashMap;

use crate::dag::error::DagError;
use crate::dag::topology::NodeId;
use crate::operator::OperatorState;

use super::*;

// ---- DagCheckpointSnapshot tests ----

#[test]
fn test_snapshot_serialization() {
    let mut node_states = HashMap::new();
    node_states.insert(
        1,
        SerializableOperatorState {
            operator_id: "test_op".to_string(),
            data: vec![1, 2, 3, 4],
        },
    );

    let mut source_offsets = HashMap::new();
    source_offsets.insert("source1".to_string(), 100);

    let snapshot = DagCheckpointSnapshot {
        checkpoint_id: 1,
        epoch: 10,
        timestamp: 123456789,
        node_states,
        source_offsets,
        watermark: Some(500),
    };

    // Serialize to JSON
    let json = serde_json::to_string(&snapshot).expect("serialization failed");
    assert!(json.contains("\"checkpoint_id\":1"));
    assert!(json.contains("\"epoch\":10"));
    assert!(json.contains("\"test_op\""));

    // Deserialize back
    let deserialized: DagCheckpointSnapshot =
        serde_json::from_str(&json).expect("deserialization failed");

    assert_eq!(deserialized.checkpoint_id, 1);
    assert_eq!(deserialized.epoch, 10);
    assert_eq!(deserialized.timestamp, 123456789);
    assert_eq!(deserialized.watermark, Some(500));
    assert_eq!(deserialized.node_states.len(), 1);
    assert_eq!(deserialized.source_offsets.len(), 1);

    let state = &deserialized.node_states[&1];
    assert_eq!(state.operator_id, "test_op");
    assert_eq!(state.data, vec![1, 2, 3, 4]);

    assert_eq!(deserialized.source_offsets["source1"], 100);
}

#[test]
fn test_snapshot_from_operator_states() {
    let mut states = FxHashMap::default();
    states.insert(
        NodeId(1),
        OperatorState {
            operator_id: "op1".to_string(),
            data: vec![10],
        },
    );
    states.insert(
        NodeId(2),
        OperatorState {
            operator_id: "op2".to_string(),
            data: vec![20],
        },
    );

    let snapshot = DagCheckpointSnapshot::from_operator_states(100, 5, 999, &states);

    assert_eq!(snapshot.checkpoint_id, 100);
    assert_eq!(snapshot.epoch, 5);
    assert_eq!(snapshot.timestamp, 999);
    assert_eq!(snapshot.node_states.len(), 2);

    assert_eq!(snapshot.node_states[&1].operator_id, "op1");
    assert_eq!(snapshot.node_states[&1].data, vec![10]);
    assert_eq!(snapshot.node_states[&2].operator_id, "op2");
    assert_eq!(snapshot.node_states[&2].data, vec![20]);
}

#[test]
fn test_snapshot_to_operator_states() {
    let mut node_states = HashMap::new();
    node_states.insert(
        10,
        SerializableOperatorState {
            operator_id: "op10".to_string(),
            data: vec![0, 1],
        },
    );

    let snapshot = DagCheckpointSnapshot {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 0,
        node_states,
        source_offsets: HashMap::new(),
        watermark: None,
    };

    let states = snapshot.to_operator_states();
    assert_eq!(states.len(), 1);
    assert!(states.contains_key(&NodeId(10)));
    assert_eq!(states[&NodeId(10)].operator_id, "op10");
    assert_eq!(states[&NodeId(10)].data, vec![0, 1]);
}

// ---- DagRecoveryManager tests ----

#[test]
fn test_manager_empty() {
    let manager = DagRecoveryManager::new();
    assert!(!manager.has_snapshots());
    assert_eq!(manager.snapshot_count(), 0);

    assert!(matches!(
        manager.recover_latest(),
        Err(DagError::CheckpointNotFound)
    ));
    assert!(matches!(
        manager.recover_by_id(1),
        Err(DagError::CheckpointNotFound)
    ));
}

#[test]
fn test_manager_add_recover_latest() {
    let mut manager = DagRecoveryManager::new();

    // Add snapshot 1
    manager.add_snapshot(DagCheckpointSnapshot {
        checkpoint_id: 1,
        epoch: 1,
        timestamp: 100,
        node_states: HashMap::new(),
        source_offsets: HashMap::new(),
        watermark: None,
    });
    assert!(manager.has_snapshots());
    assert_eq!(manager.snapshot_count(), 1);

    let recovered = manager.recover_latest().expect("should recover");
    assert_eq!(recovered.snapshot.checkpoint_id, 1);

    // Add snapshot 2 (newer)
    manager.add_snapshot(DagCheckpointSnapshot {
        checkpoint_id: 2,
        epoch: 2,
        timestamp: 200,
        node_states: HashMap::new(),
        source_offsets: HashMap::new(),
        watermark: Some(50),
    });
    assert_eq!(manager.snapshot_count(), 2);

    let recovered = manager.recover_latest().expect("should recover");
    assert_eq!(recovered.snapshot.checkpoint_id, 2);
    assert_eq!(recovered.snapshot.timestamp, 200);
    assert_eq!(recovered.watermark, Some(50));
}

#[test]
fn test_manager_recover_by_id() {
    let mut manager = DagRecoveryManager::new();

    manager.add_snapshot(DagCheckpointSnapshot {
        checkpoint_id: 10,
        epoch: 10,
        timestamp: 1000,
        node_states: HashMap::new(),
        source_offsets: HashMap::new(),
        watermark: None,
    });
    manager.add_snapshot(DagCheckpointSnapshot {
        checkpoint_id: 20,
        epoch: 20,
        timestamp: 2000,
        node_states: HashMap::new(),
        source_offsets: HashMap::new(),
        watermark: None,
    });

    // Recover existing
    let r10 = manager.recover_by_id(10).expect("should find 10");
    assert_eq!(r10.snapshot.checkpoint_id, 10);

    let r20 = manager.recover_by_id(20).expect("should find 20");
    assert_eq!(r20.snapshot.checkpoint_id, 20);

    // Recover non-existent
    assert!(matches!(
        manager.recover_by_id(15),
        Err(DagError::CheckpointNotFound)
    ));
}

#[test]
fn test_manager_with_snapshots() {
    let snaps = vec![
        DagCheckpointSnapshot {
            checkpoint_id: 1,
            epoch: 1,
            timestamp: 1,
            node_states: HashMap::new(),
            source_offsets: HashMap::new(),
            watermark: None,
        },
        DagCheckpointSnapshot {
            checkpoint_id: 3,
            epoch: 3,
            timestamp: 3,
            node_states: HashMap::new(),
            source_offsets: HashMap::new(),
            watermark: None,
        },
    ];

    let manager = DagRecoveryManager::with_snapshots(snaps);
    assert_eq!(manager.snapshot_count(), 2);

    // Should find max ID
    let recovered = manager.recover_latest().unwrap();
    assert_eq!(recovered.snapshot.checkpoint_id, 3);
}

#[test]
fn test_recovered_dag_state_debug() {
    let snapshot = DagCheckpointSnapshot {
        checkpoint_id: 123,
        epoch: 456,
        timestamp: 789,
        node_states: HashMap::new(),
        source_offsets: HashMap::new(),
        watermark: Some(100),
    };

    let recovered = DagRecoveryManager::build_recovered_state(snapshot);
    let debug_str = format!("{:?}", recovered);

    assert!(debug_str.contains("RecoveredDagState"));
    assert!(debug_str.contains("checkpoint_id: 123"));
    assert!(debug_str.contains("epoch: 456"));
    assert!(debug_str.contains("watermark: Some(100)"));
}

#[test]
fn test_serializable_operator_state_conversion() {
    let op_state = OperatorState {
        operator_id: "test".to_string(),
        data: vec![1, 2, 3],
    };

    let serializable: SerializableOperatorState = op_state.clone().into();
    assert_eq!(serializable.operator_id, "test");
    assert_eq!(serializable.data, vec![1, 2, 3]);

    let back: OperatorState = serializable.into();
    assert_eq!(back.operator_id, "test");
    assert_eq!(back.data, vec![1, 2, 3]);
}
