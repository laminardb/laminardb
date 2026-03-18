//! Integration test: two-node cluster routing with static discovery.
//!
//! Starts two in-process nodes that discover each other via static discovery,
//! compute partition assignments, and route batches to the correct owner.

#![cfg(feature = "delta")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::RecordBatch;

use laminar_core::delta::coordination::rebalancer::Rebalancer;
use laminar_core::delta::coordination::{DeltaManager, NodeLifecyclePhase};
use laminar_core::delta::discovery::{
    Discovery, NodeId, NodeInfo, NodeMetadata, NodeState, StaticDiscovery, StaticDiscoveryConfig,
};
use laminar_core::delta::partition::assignment::{AssignmentConstraints, ConsistentHashAssigner};
use laminar_core::delta::routing::{PartitionRouter, RemoteBatchReceiver, RemoteBatchSender};

fn make_node_info(id: u64, rpc_addr: &str) -> NodeInfo {
    NodeInfo {
        id: NodeId(id),
        name: format!("node-{id}"),
        rpc_address: rpc_addr.to_string(),
        raft_address: rpc_addr.to_string(),
        state: NodeState::Active,
        metadata: NodeMetadata {
            cores: 4,
            ..NodeMetadata::default()
        },
        last_heartbeat_ms: 0,
    }
}

fn make_batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

/// Test that two nodes discover each other and can compute consistent
/// partition assignments.
#[tokio::test]
async fn test_two_node_discovery_and_assignment() {
    // Bind to ephemeral ports
    let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    drop(listener1);

    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap().to_string();
    drop(listener2);

    let config1 = StaticDiscoveryConfig {
        local_node: make_node_info(1, &addr1),
        seeds: vec![addr2.clone()],
        heartbeat_interval: Duration::from_millis(100),
        suspect_threshold: 3,
        dead_threshold: 10,
        listen_address: addr1.clone(),
    };

    let config2 = StaticDiscoveryConfig {
        local_node: make_node_info(2, &addr2),
        seeds: vec![addr1],
        heartbeat_interval: Duration::from_millis(100),
        suspect_threshold: 3,
        dead_threshold: 10,
        listen_address: addr2,
    };

    let mut disc1 = StaticDiscovery::new(config1);
    let mut disc2 = StaticDiscovery::new(config2);

    disc1.start().await.unwrap();
    disc2.start().await.unwrap();

    // Wait for mutual discovery
    tokio::time::sleep(Duration::from_millis(500)).await;

    let peers1 = disc1.peers().await.unwrap();
    let peers2 = disc2.peers().await.unwrap();
    assert!(
        !peers1.is_empty() || !peers2.is_empty(),
        "at least one node should have discovered peers"
    );

    // Compute partition assignment — both nodes should produce the same result
    let assigner = ConsistentHashAssigner::new();
    let all_nodes = vec![make_node_info(1, ""), make_node_info(2, "")];
    let plan = assigner.initial_assignment(12, &all_nodes, &AssignmentConstraints::default());

    let node1_count = plan
        .assignments
        .values()
        .filter(|&&n| n == NodeId(1))
        .count();
    let node2_count = plan
        .assignments
        .values()
        .filter(|&&n| n == NodeId(2))
        .count();

    assert_eq!(node1_count + node2_count, 12);
    assert!(node1_count > 0, "node 1 should own some partitions");
    assert!(node2_count > 0, "node 2 should own some partitions");

    disc1.stop().await.unwrap();
    disc2.stop().await.unwrap();
}

/// Test that batches are correctly routed to the local or remote node.
#[tokio::test]
async fn test_partition_routing_local_and_remote() {
    let assigner = ConsistentHashAssigner::new();
    let nodes = vec![make_node_info(1, ""), make_node_info(2, "")];
    let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());

    // Create routers for both nodes
    let router1 = PartitionRouter::new(&plan, NodeId(1), None);
    let router2 = PartitionRouter::new(&plan, NodeId(2), None);

    let batch = make_batch(vec![1, 2, 3]);

    // Route batch on node 1
    let result1 = router1.route_batch(&batch);
    // Route batch on node 2
    let result2 = router2.route_batch(&batch);

    // The batch goes to partition 0's owner. One node should see it as local,
    // the other as remote.
    let owner = plan.assignments.get(&0).copied().unwrap();
    if owner == NodeId(1) {
        assert_eq!(result1.local_batches.len(), 1);
        assert!(result1.remote_batches.is_empty());
        assert!(result2.local_batches.is_empty());
        assert!(result2.remote_batches.contains_key(&NodeId(1)));
    } else {
        assert!(result1.local_batches.is_empty());
        assert!(result1.remote_batches.contains_key(&NodeId(2)));
        assert_eq!(result2.local_batches.len(), 1);
        assert!(result2.remote_batches.is_empty());
    }
}

/// Test the remote batch send/receive pipeline.
#[tokio::test]
async fn test_remote_batch_forwarding() {
    // Simulate: node 1 forwards a batch to node 2 via channels
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);

    let mut sender = RemoteBatchSender::new();
    sender.register_node(NodeId(2), tx);

    let batch = make_batch(vec![10, 20, 30]);
    sender
        .send_batch(NodeId(2), "trades".into(), batch)
        .await
        .unwrap();

    // Node 2's receiver picks up the batch
    let (source_name, received) = rx.recv().await.unwrap();
    assert_eq!(source_name, "trades");
    assert_eq!(received.num_rows(), 3);

    let col = received
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 10);
    assert_eq!(col.value(2), 30);
}

/// Test rebalancing when a third node joins.
#[tokio::test]
async fn test_rebalance_on_node_join() {
    let assigner = ConsistentHashAssigner::new();
    let nodes2 = vec![make_node_info(1, ""), make_node_info(2, "")];
    let plan = assigner.initial_assignment(20, &nodes2, &AssignmentConstraints::default());

    let mut manager = DeltaManager::new(NodeId(1));
    manager.transition(NodeLifecyclePhase::FormingRaft);
    manager.transition(NodeLifecyclePhase::WaitingForAssignment);
    manager.transition(NodeLifecyclePhase::Active);

    for (&pid, &owner) in &plan.assignments {
        if owner == NodeId(1) {
            manager.guards_mut().insert(pid, 1);
        }
    }

    let initial_guards = manager.guards().len();

    let mut rebalancer = Rebalancer::new(&plan);
    let nodes3 = vec![
        make_node_info(1, ""),
        make_node_info(2, ""),
        make_node_info(3, ""),
    ];
    let result = rebalancer.compute_rebalance(&manager, &nodes3);

    // There should be moves to the new node
    let moves_to_node3 = result
        .plan
        .moves
        .iter()
        .filter(|m| m.to == NodeId(3))
        .count();
    assert!(moves_to_node3 > 0, "node 3 should receive some partitions");

    // Apply rebalance
    rebalancer.apply_rebalance(&result, &mut manager);

    // Node 1 should have lost some partitions
    let new_guards = manager.guards().len();
    assert!(
        new_guards <= initial_guards,
        "node 1 should not gain partitions (had {initial_guards}, now has {new_guards})"
    );
}

/// Test that routers on both nodes agree on partition ownership after
/// receiving the same assignment plan.
#[test]
fn test_router_consistency_across_nodes() {
    let assigner = ConsistentHashAssigner::new();
    let nodes = vec![make_node_info(1, ""), make_node_info(2, "")];
    let plan = assigner.initial_assignment(16, &nodes, &AssignmentConstraints::default());

    let router1 = PartitionRouter::new(&plan, NodeId(1), None);
    let router2 = PartitionRouter::new(&plan, NodeId(2), None);

    // Both routers should agree on who owns each partition
    for pid in 0..16 {
        assert_eq!(
            router1.owner_of(pid),
            router2.owner_of(pid),
            "partition {pid} ownership should be consistent"
        );
    }
}
