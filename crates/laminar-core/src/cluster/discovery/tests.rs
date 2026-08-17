use super::*;

#[test]
fn test_node_id_display() {
    assert_eq!(NodeId(42).to_string(), "node-42");
}

#[test]
fn test_node_id_unassigned() {
    assert!(NodeId::UNASSIGNED.is_unassigned());
    assert!(!NodeId(1).is_unassigned());
}

#[test]
fn test_node_state_display() {
    assert_eq!(NodeState::Active.to_string(), "active");
    assert_eq!(NodeState::Suspected.to_string(), "suspected");
    assert_eq!(NodeState::Draining.to_string(), "draining");
}

fn info_with(id: u64, state: NodeState) -> NodeInfo {
    NodeInfo {
        id: NodeId(id),
        name: format!("n{id}"),
        rpc_address: String::new(),
        state,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }
}

#[test]
fn assignable_includes_only_active_sorted_deduped() {
    let members = vec![
        info_with(5, NodeState::Active),
        info_with(2, NodeState::Joining),
        info_with(3, NodeState::Suspected),
        info_with(4, NodeState::Draining),
        info_with(6, NodeState::Left),
        info_with(1, NodeState::Active),
        info_with(1, NodeState::Active), // dup
    ];
    assert_eq!(assignable_node_ids(&members), vec![NodeId(1), NodeId(5)]);
}

#[test]
fn assignable_drops_unassigned() {
    let mut unassigned = info_with(7, NodeState::Active);
    unassigned.id = NodeId::UNASSIGNED;
    let members = vec![unassigned, info_with(7, NodeState::Active)];
    assert_eq!(assignable_node_ids(&members), vec![NodeId(7)]);
}

#[test]
fn test_node_metadata_default() {
    let meta = NodeMetadata::default();
    assert_eq!(meta.cores, 1);
    assert_eq!(meta.memory_bytes, 0);
    assert!(meta.failure_domain.is_none());
    assert!(meta.tags.is_empty());
}

#[test]
fn test_node_id_serialization() {
    let id = NodeId(123);
    let json = serde_json::to_string(&id).unwrap();
    let back: NodeId = serde_json::from_str(&json).unwrap();
    assert_eq!(id, back);
}

#[test]
fn test_node_info_serialization() {
    let info = NodeInfo {
        id: NodeId(1),
        name: "test-node".into(),
        rpc_address: "127.0.0.1:9000".into(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 1000,
    };
    let json = serde_json::to_string(&info).unwrap();
    let back: NodeInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(back.id, info.id);
    assert_eq!(back.name, "test-node");
}
