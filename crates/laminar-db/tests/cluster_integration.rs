//! Integration tests for cluster-mode partition routing.
//!
//! Validates that the `PartitionRouter` correctly splits batches between
//! local and remote nodes, and that `MigrationCoordinator` correctly
//! manages partition ownership transitions with epoch fencing.

#![cfg(feature = "delta")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use laminar_core::delta::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
use laminar_core::delta::partition::assignment::{
    AssignmentConstraints, ConsistentHashAssigner, PartitionMove,
};
use laminar_core::delta::partition::guard::PartitionGuardSet;
use laminar_core::delta::partition::migration::{
    MigrationCoordinator, MigrationPhase, PartitionSnapshot,
};
use laminar_core::delta::remote_state::{
    RemoteStateError, RemoteStateProxy, RemoteStateTransport, RemoteValue,
};
use laminar_core::delta::routing::{PartitionRouter, RoutingConfig};

fn make_node(id: u64, cores: u32) -> NodeInfo {
    NodeInfo {
        id: NodeId(id),
        name: format!("node-{id}"),
        rpc_address: format!("127.0.0.1:{}", 9000 + id),
        raft_address: format!("127.0.0.1:{}", 9100 + id),
        state: NodeState::Active,
        metadata: NodeMetadata {
            cores,
            ..NodeMetadata::default()
        },
        last_heartbeat_ms: 0,
    }
}

fn make_batch(keys: Vec<&str>, values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Two-node cluster: verify that batches are correctly split between nodes
/// based on consistent-hash partition assignment.
#[test]
fn test_two_node_routing_split() {
    let nodes = vec![make_node(1, 4), make_node(2, 4)];
    let assigner = ConsistentHashAssigner::new();
    let plan = assigner.initial_assignment(8, &nodes, &AssignmentConstraints::default());

    // Set up router for node 1
    let config = RoutingConfig {
        partition_key_column: "partition_key".into(),
        num_partitions: 8,
    };
    let router = PartitionRouter::new(config, NodeId(1)).unwrap();
    router.update_assignments(plan.assignments.clone());

    // Create a batch with diverse keys
    let batch = make_batch(
        vec!["alpha", "beta", "gamma", "delta", "epsilon", "zeta"],
        vec![1, 2, 3, 4, 5, 6],
    );

    let routed = router.route_batch(&batch).unwrap();

    // Total rows should be preserved
    let local_rows: usize = routed.local.iter().map(RecordBatch::num_rows).sum();
    let remote_rows: usize = routed
        .remote
        .values()
        .flat_map(|v| v.iter())
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(
        local_rows + remote_rows,
        6,
        "all rows must be accounted for"
    );
}

/// Verify that routing is deterministic: same batch, same assignments,
/// same split.
#[test]
fn test_routing_determinism() {
    let nodes = vec![make_node(1, 4), make_node(2, 4)];
    let assigner = ConsistentHashAssigner::new();
    let plan = assigner.initial_assignment(8, &nodes, &AssignmentConstraints::default());

    let config = RoutingConfig {
        partition_key_column: "partition_key".into(),
        num_partitions: 8,
    };
    let router = PartitionRouter::new(config, NodeId(1)).unwrap();
    router.update_assignments(plan.assignments);

    let batch = make_batch(vec!["a", "b", "c"], vec![1, 2, 3]);

    let routed1 = router.route_batch(&batch).unwrap();
    let routed2 = router.route_batch(&batch).unwrap();

    let local1: usize = routed1.local.iter().map(RecordBatch::num_rows).sum();
    let local2: usize = routed2.local.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(local1, local2);
}

/// Verify rebalance: when a third node joins, some partitions move.
#[test]
fn test_rebalance_partition_moves() {
    let assigner = ConsistentHashAssigner::new();
    let nodes2 = vec![make_node(1, 4), make_node(2, 4)];
    let plan2 = assigner.initial_assignment(12, &nodes2, &AssignmentConstraints::default());

    let nodes3 = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];
    let plan3 = assigner.rebalance(
        &plan2.assignments,
        &nodes3,
        &AssignmentConstraints::default(),
    );

    // Some partitions should move to node 3
    let node3_partitions: Vec<_> = plan3
        .assignments
        .iter()
        .filter(|(_, &owner)| owner == NodeId(3))
        .map(|(pid, _)| *pid)
        .collect();

    assert!(
        !node3_partitions.is_empty(),
        "node 3 should receive some partitions after rebalance"
    );
    assert!(
        plan3.stats.total_moves > 0,
        "rebalance should require some moves"
    );
    assert!(
        plan3.stats.total_moves < 12,
        "consistent hashing should minimize moves"
    );
}

/// Full migration lifecycle: source node gives up partition, target receives it.
#[test]
fn test_migration_lifecycle() {
    // Node 1 owns partition 5 at epoch 9.
    let mut source_guards = PartitionGuardSet::new(NodeId(1));
    source_guards.insert(5, 9);

    // Source coordinator
    let mut source_coord = MigrationCoordinator::new(NodeId(1));
    let moves = vec![PartitionMove {
        partition_id: 5,
        from: Some(NodeId(1)),
        to: NodeId(2),
    }];
    let mut epochs = HashMap::new();
    epochs.insert(5, 10);
    source_coord.plan_migrations(&moves, &epochs).unwrap();

    // Step 1: Pause on source
    assert!(source_coord.begin_pause(5, &source_guards).is_ok());
    assert_eq!(
        source_coord.migration_state(5).unwrap().phase,
        MigrationPhase::Pausing
    );

    // Step 2: Snapshot
    let snapshot = PartitionSnapshot {
        partition_id: 5,
        epoch: 9,
        source_node: NodeId(1),
        state_data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        entry_count: 42,
    };
    assert!(source_coord.record_snapshot(5, snapshot).is_ok());
    assert_eq!(
        source_coord.migration_state(5).unwrap().phase,
        MigrationPhase::Transferring
    );

    // Step 3: Transfer complete, begin restore
    assert!(source_coord.begin_restore(5).is_ok());

    // Step 4: Ack target restore + complete on source side (removes guard)
    assert!(source_coord.ack_target_restore(5).is_ok());
    let new_epoch = source_coord
        .complete_migration(5, &mut source_guards)
        .unwrap();
    assert_eq!(new_epoch, 10);
    assert!(
        source_guards.get(5).is_none(),
        "source should no longer own partition 5"
    );

    // Target side: separate coordinator for node 2
    let mut target_guards = PartitionGuardSet::new(NodeId(2));
    let mut target_coord = MigrationCoordinator::new(NodeId(2));
    target_coord.plan_migrations(&moves, &epochs).unwrap();

    // Walk target through phases using public API
    let state = target_coord.migration_state_mut(5).unwrap();
    state.transition(MigrationPhase::Pausing).unwrap();
    state.transition(MigrationPhase::Transferring).unwrap();
    state.transition(MigrationPhase::Restoring).unwrap();
    state.snapshot_received = true; // target received the snapshot

    let target_epoch = target_coord
        .complete_migration(5, &mut target_guards)
        .unwrap();
    assert_eq!(target_epoch, 10);
    assert!(
        target_guards.get(5).is_some(),
        "target should now own partition 5"
    );
    assert!(target_guards.check(5).is_ok());
}

/// Epoch fencing: verify that a stale epoch prevents migration.
#[test]
fn test_epoch_fencing_prevents_stale_migration() {
    let mut guards = PartitionGuardSet::new(NodeId(1));
    guards.insert(5, 9);

    // Simulate epoch advancing (Raft told us ownership changed)
    guards.update_epoch(5, 10);

    // Now try to start a migration at epoch 9 -- guard check should fail.
    let mut coord = MigrationCoordinator::new(NodeId(1));
    let moves = vec![PartitionMove {
        partition_id: 5,
        from: Some(NodeId(1)),
        to: NodeId(2),
    }];
    let mut epochs = HashMap::new();
    epochs.insert(5, 11);
    coord.plan_migrations(&moves, &epochs).unwrap();

    let result = coord.begin_pause(5, &guards);
    assert!(
        result.is_err(),
        "epoch fence should prevent migration on stale epoch"
    );
}

/// Remote state proxy: local partitions return None (use local store).
#[tokio::test]
async fn test_remote_state_local_returns_none() {
    struct NoopTransport;
    impl RemoteStateTransport for NoopTransport {
        async fn lookup(
            &self,
            _: NodeId,
            _: &str,
            _: u32,
            _: &[u8],
        ) -> Result<Option<RemoteValue>, RemoteStateError> {
            panic!("should not be called for local partitions");
        }
    }

    let proxy = RemoteStateProxy::new(Arc::new(NoopTransport), NodeId(1), 100);
    let mut assignments = HashMap::new();
    assignments.insert(0, NodeId(1)); // local
    proxy.update_assignments(assignments);

    let result = proxy.get(0, b"test_key").await.unwrap();
    assert!(result.is_none());
}

/// Remote state proxy: remote partitions call transport and cache result.
#[tokio::test]
async fn test_remote_state_cache_hit() {
    use std::sync::atomic::{AtomicU32, Ordering};

    struct CountingTransport(AtomicU32);
    impl RemoteStateTransport for CountingTransport {
        async fn lookup(
            &self,
            _: NodeId,
            _: &str,
            _: u32,
            _: &[u8],
        ) -> Result<Option<RemoteValue>, RemoteStateError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Some(RemoteValue {
                data: vec![1, 2, 3],
                partition_id: 0,
                source_node: NodeId(2),
            }))
        }
    }

    let transport = Arc::new(CountingTransport(AtomicU32::new(0)));
    let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 100);

    let mut assignments = HashMap::new();
    assignments.insert(0, NodeId(2)); // remote
    proxy.update_assignments(assignments);
    let mut addrs = HashMap::new();
    addrs.insert(NodeId(2), "127.0.0.1:9000".into());
    proxy.update_node_addresses(addrs);

    // First lookup: calls transport
    let v = proxy.get(0, b"key").await.unwrap().unwrap();
    assert_eq!(v.data, vec![1, 2, 3]);
    assert_eq!(transport.0.load(Ordering::Relaxed), 1);

    // Second lookup: cache hit, no transport call
    let _ = proxy.get(0, b"key").await.unwrap().unwrap();
    assert_eq!(transport.0.load(Ordering::Relaxed), 1);
}

/// End-to-end: query from node 1 with partition data split across nodes.
/// Verifies that router + assignment produces correct local/remote split.
#[test]
fn test_end_to_end_cross_node_query() {
    let nodes = vec![make_node(1, 4), make_node(2, 4)];
    let assigner = ConsistentHashAssigner::new();
    let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());

    // Node 1's router
    let router1 = PartitionRouter::new(
        RoutingConfig {
            partition_key_column: "partition_key".into(),
            num_partitions: 4,
        },
        NodeId(1),
    )
    .unwrap();
    router1.update_assignments(plan.assignments.clone());

    // Node 2's router
    let router2 = PartitionRouter::new(
        RoutingConfig {
            partition_key_column: "partition_key".into(),
            num_partitions: 4,
        },
        NodeId(2),
    )
    .unwrap();
    router2.update_assignments(plan.assignments.clone());

    // Push data to node 1
    let batch = make_batch(vec!["k1", "k2", "k3", "k4", "k5"], vec![10, 20, 30, 40, 50]);

    let routed1 = router1.route_batch(&batch).unwrap();
    let routed2 = router2.route_batch(&batch).unwrap();

    // What node 1 routes as "local" should be what node 2 routes as "remote to node 1"
    // and vice versa. Total coverage should be the same.
    let total1 = routed1
        .local
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>()
        + routed1
            .remote
            .values()
            .flat_map(|v| v.iter())
            .map(RecordBatch::num_rows)
            .sum::<usize>();
    let total2 = routed2
        .local
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>()
        + routed2
            .remote
            .values()
            .flat_map(|v| v.iter())
            .map(RecordBatch::num_rows)
            .sum::<usize>();

    assert_eq!(total1, 5);
    assert_eq!(total2, 5);

    // Node 1's local rows + node 2's local rows should equal total rows
    // (each row goes to exactly one node).
    let n1_local: usize = routed1.local.iter().map(RecordBatch::num_rows).sum();
    let n2_local: usize = routed2.local.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        n1_local + n2_local,
        5,
        "every row should be local on exactly one node"
    );
}
