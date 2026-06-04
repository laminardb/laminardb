//! Engine-controlled Kafka partition → vnode assignment.
//!
//! In cluster mode LaminarDB owns the partition-to-node mapping rather than
//! delegating it to Kafka's consumer-group coordinator: each Kafka partition
//! is bound to a vnode by `partition % vnode_count`, and a node consumes a
//! partition iff it owns that vnode in the current
//! [`VnodeRegistry`](laminar_core::state::VnodeRegistry) assignment. This keeps
//! Kafka ingestion co-located with the vnode state it feeds, so a vnode and the
//! partitions whose data lands in it always move together on rebalance.
//!
//! This module holds only the pure mapping logic (no rdkafka), so it lives
//! outside the `kafka` feature gate and compiles + is unit-tested without a
//! native Kafka/OpenSSL toolchain or a live broker. The Kafka source consumes
//! it (under the `kafka` feature) and applies the result via manual `assign()`.

use laminar_core::state::{NodeId, VnodeRegistry};

/// The Kafka partitions (of a `total_partitions`-partition topic) this node
/// owns under the registry's current assignment.
///
/// Partition `p` maps to vnode `p % vnode_count`; it is owned iff that vnode's
/// owner is `self_id`. Returned ascending. Negative partition ids (never
/// produced by Kafka metadata) are skipped defensively.
#[must_use]
pub fn owned_partitions(
    total_partitions: i32,
    registry: &VnodeRegistry,
    self_id: NodeId,
) -> Vec<i32> {
    let vnode_count = registry.vnode_count();
    (0..total_partitions)
        .filter(|&p| {
            let Ok(pid) = u32::try_from(p) else {
                return false;
            };
            registry.owner(pid % vnode_count) == self_id
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::state::rendezvous_assignment;
    use std::sync::Arc;

    fn registry_with(vnode_count: u32, owners: &[NodeId]) -> VnodeRegistry {
        let assignment = rendezvous_assignment(vnode_count, owners);
        let r = VnodeRegistry::new(vnode_count);
        r.set_assignment(assignment);
        r
    }

    #[test]
    fn partitions_split_by_modulo_across_two_nodes() {
        // 4 vnodes, nodes 1 and 2.
        let r = registry_with(4, &[NodeId(1), NodeId(2)]);
        // 8 partitions: p % 4 → vnode → owner.
        let expected_node1: Vec<i32> = (0..8)
            .filter(|&p| r.owner((p % 4) as u32) == NodeId(1))
            .collect();
        let expected_node2: Vec<i32> = (0..8)
            .filter(|&p| r.owner((p % 4) as u32) == NodeId(2))
            .collect();

        assert_eq!(owned_partitions(8, &r, NodeId(1)), expected_node1);
        assert_eq!(owned_partitions(8, &r, NodeId(2)), expected_node2);
    }

    #[test]
    fn union_of_owners_covers_all_partitions_disjointly() {
        let r = registry_with(4, &[NodeId(1), NodeId(2), NodeId(3)]);
        let total = 16;
        let mut all: Vec<i32> = Vec::new();
        for owner in [NodeId(1), NodeId(2), NodeId(3)] {
            all.extend(owned_partitions(total, &r, owner));
        }
        all.sort_unstable();
        assert_eq!(
            all,
            (0..total).collect::<Vec<_>>(),
            "every partition owned exactly once"
        );
    }

    #[test]
    fn single_owner_takes_all_partitions() {
        let r = registry_with(8, &[NodeId(7)]);
        assert_eq!(owned_partitions(5, &r, NodeId(7)), vec![0, 1, 2, 3, 4],);
    }

    #[test]
    fn unowned_node_gets_nothing() {
        let r = registry_with(4, &[NodeId(1), NodeId(2)]);
        assert!(owned_partitions(8, &r, NodeId(99)).is_empty());
    }

    #[test]
    fn fewer_partitions_than_vnodes_leaves_some_nodes_empty() {
        // 4 vnodes, only 2 partitions.
        let r = registry_with(4, &[NodeId(1), NodeId(2)]);
        let expected_node1: Vec<i32> = (0..2)
            .filter(|&p| r.owner((p % 4) as u32) == NodeId(1))
            .collect();
        let expected_node2: Vec<i32> = (0..2)
            .filter(|&p| r.owner((p % 4) as u32) == NodeId(2))
            .collect();

        assert_eq!(owned_partitions(2, &r, NodeId(1)), expected_node1);
        assert_eq!(owned_partitions(2, &r, NodeId(2)), expected_node2);
    }

    #[test]
    fn reassignment_moves_partitions() {
        let r = VnodeRegistry::new(4);
        r.set_assignment(Arc::from([NodeId(1), NodeId(2), NodeId(1), NodeId(2)]));
        assert_eq!(owned_partitions(4, &r, NodeId(1)), vec![0, 2]);
        // Rotate every vnode to node 1 — it should now own all partitions.
        r.set_assignment(Arc::from([NodeId(1), NodeId(1), NodeId(1), NodeId(1)]));
        assert_eq!(owned_partitions(4, &r, NodeId(1)), vec![0, 1, 2, 3]);
        assert!(owned_partitions(4, &r, NodeId(2)).is_empty());
    }
}
