use super::*;
use laminar_core::state::VnodeRegistry;
use std::sync::Arc;

const SOURCE: &str = "orders_source";
const TOPIC: &str = "orders.v1";

#[test]
fn mapping_has_hard_coded_abi_vectors() {
    assert_eq!(laminar_core::state::PARTITIONING_ABI_VERSION, 1);
    let actual: Vec<u32> = (0..8)
        .map(|partition| partition_vnode(SOURCE, TOPIC, partition, 256).unwrap())
        .collect();
    assert_eq!(actual, [106, 108, 234, 55, 225, 196, 217, 62]);
    assert_eq!(partition_vnodes(SOURCE, TOPIC, 8, 256).unwrap(), actual);
}

#[test]
fn batch_mapping_matches_scalar_mapping() {
    for vnode_count in [1, 2, 7, 256, u32::MAX] {
        let batch = partition_vnodes(SOURCE, TOPIC, 257, vnode_count).unwrap();
        let scalar: Vec<u32> = (0..257)
            .map(|partition| partition_vnode(SOURCE, TOPIC, partition, vnode_count).unwrap())
            .collect();
        assert_eq!(batch, scalar, "vnode count {vnode_count}");
    }
    assert!(partition_vnodes(SOURCE, TOPIC, -1, 8).is_err());
    assert!(partition_vnodes(SOURCE, TOPIC, 0, 0).is_err());
}

#[test]
fn source_and_topic_are_both_routing_domains() {
    let base = partition_vnode(SOURCE, TOPIC, 7, 65_521).unwrap();
    assert_ne!(
        base,
        partition_vnode("returns_source", TOPIC, 7, 65_521).unwrap()
    );
    assert_ne!(
        base,
        partition_vnode(SOURCE, "returns.v1", 7, 65_521).unwrap()
    );
    assert_ne!(
        partition_vnode("ab", "c", 7, 65_521).unwrap(),
        partition_vnode("a", "bc", 7, 65_521).unwrap(),
        "length prefixes must prevent concatenation ambiguity"
    );
}

#[test]
fn union_of_owners_covers_every_partition_once() {
    let owners = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    let assignment: Vec<NodeId> = (0..256).map(|vnode| owners[vnode % owners.len()]).collect();
    let total = 1_024;
    let mut all = Vec::new();
    for owner in owners {
        all.extend(
            owned_partitions_in_assignment(SOURCE, TOPIC, total, &assignment, owner).unwrap(),
        );
    }
    all.sort_unstable();
    assert_eq!(all, (0..total).collect::<Vec<_>>());
}

#[test]
fn registry_reassignment_moves_the_complete_inventory() {
    let registry = VnodeRegistry::new(8);
    registry.set_assignment(Arc::from([NodeId(1); 8]));
    assert_eq!(
        owned_partitions_in_assignment(SOURCE, TOPIC, 16, &registry.snapshot(), NodeId(1),)
            .unwrap(),
        (0..16).collect::<Vec<_>>()
    );
    registry.set_assignment(Arc::from([NodeId(2); 8]));
    let assignment = registry.snapshot();
    assert!(
        owned_partitions_in_assignment(SOURCE, TOPIC, 16, &assignment, NodeId(1))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        owned_partitions_in_assignment(SOURCE, TOPIC, 16, &assignment, NodeId(2)).unwrap(),
        (0..16).collect::<Vec<_>>()
    );
}

#[test]
fn owner_map_rejects_unassigned_vnode_with_its_index() {
    let error = owned_partitions_in_assignment(
        SOURCE,
        TOPIC,
        16,
        &[NodeId(1), NodeId::UNASSIGNED, NodeId(2)],
        NodeId(1),
    )
    .unwrap_err();
    assert!(error.to_string().contains("unassigned owner at vnode 1"));
}

#[test]
fn valid_idle_worker_has_an_empty_partition_assignment() {
    let assignment = [NodeId(2), NodeId(3), NodeId(2), NodeId(3)];
    assert!(
        owned_partitions_in_assignment(SOURCE, TOPIC, 64, &assignment, NodeId(1))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn malformed_inputs_are_rejected() {
    assert!(partition_vnode("", TOPIC, 0, 8).is_err());
    assert!(partition_vnode(SOURCE, "", 0, 8).is_err());
    assert!(partition_vnode(SOURCE, TOPIC, -1, 8).is_err());
    assert!(partition_vnode(SOURCE, TOPIC, 0, 0).is_err());
    assert!(owned_partitions_in_assignment(SOURCE, TOPIC, 1, &[], NodeId(1)).is_err());
    assert!(
        owned_partitions_in_assignment(SOURCE, TOPIC, 1, &[NodeId(1)], NodeId::UNASSIGNED).is_err()
    );
    assert!(owned_partitions_in_assignment(SOURCE, TOPIC, -1, &[NodeId(1)], NodeId(1)).is_err());
}
