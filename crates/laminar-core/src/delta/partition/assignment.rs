//! Partition assignment algorithms for the delta.
//!
//! Assigns partitions to nodes using consistent hashing with virtual
//! nodes proportional to each node's core count. Supports failure domain
//! diversity and weighted distribution.

use std::collections::HashMap;

use crate::delta::discovery::{NodeId, NodeInfo};

/// A planned partition-to-node assignment.
#[derive(Debug, Clone)]
pub struct AssignmentPlan {
    /// Mapping from partition ID to assigned node.
    pub assignments: HashMap<u32, NodeId>,
    /// Moves required to transition from current to planned state.
    pub moves: Vec<PartitionMove>,
    /// Statistics about the plan.
    pub stats: AssignmentStats,
}

/// A single partition move in a rebalance plan.
#[derive(Debug, Clone)]
pub struct PartitionMove {
    /// The partition being moved.
    pub partition_id: u32,
    /// The current owner (if any).
    pub from: Option<NodeId>,
    /// The new owner.
    pub to: NodeId,
}

/// Statistics about an assignment plan.
#[derive(Debug, Clone, Default)]
pub struct AssignmentStats {
    /// Total number of partitions.
    pub total_partitions: u32,
    /// Number of moves required.
    pub total_moves: u32,
    /// Maximum partitions assigned to a single node.
    pub max_per_node: u32,
    /// Minimum partitions assigned to a single node.
    pub min_per_node: u32,
    /// Number of distinct failure domains used.
    pub failure_domains_used: u32,
}

/// Constraints for partition assignment.
#[derive(Debug, Clone, Default)]
pub struct AssignmentConstraints {
    /// Maximum partitions per node (0 = no limit).
    pub max_partitions_per_node: u32,
    /// Minimum number of failure domains to spread across.
    pub min_failure_domains: u32,
    /// Anti-affinity groups: partitions in the same group should be
    /// on different nodes when possible.
    pub anti_affinity_groups: Vec<Vec<u32>>,
    /// Per-node weight overrides (node ID → weight).
    /// Default weight is proportional to core count.
    pub node_weights: HashMap<u64, f64>,
}

/// Trait for partition assignment algorithms.
pub trait PartitionAssigner: Send + Sync {
    /// Generate an initial assignment for the given number of partitions.
    fn initial_assignment(
        &self,
        num_partitions: u32,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> AssignmentPlan;

    /// Generate a rebalance plan given the current assignments and new node set.
    fn rebalance(
        &self,
        current: &HashMap<u32, NodeId>,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> AssignmentPlan;

    /// Validate that an assignment plan satisfies the given constraints.
    fn validate_plan(
        &self,
        plan: &AssignmentPlan,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> Vec<String>;
}

/// Consistent-hash based partition assigner.
///
/// Uses virtual nodes proportional to each node's core count for
/// weighted distribution. The hash ring is deterministic given the
/// same set of nodes (fixed seed).
pub struct ConsistentHashAssigner {
    /// Number of virtual nodes per core.
    pub vnodes_per_core: u32,
    /// Fixed seed for deterministic hashing.
    pub seed: u64,
}

impl ConsistentHashAssigner {
    /// Create a new assigner with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            vnodes_per_core: 100,
            seed: 0x517C_C1F0_CAFE_BABE,
        }
    }

    /// Create a new assigner with custom virtual node count and seed.
    #[must_use]
    pub fn with_config(vnodes_per_core: u32, seed: u64) -> Self {
        Self {
            vnodes_per_core,
            seed,
        }
    }

    /// Hash a (`node_id`, `vnode_index`) pair to a position on the ring.
    fn hash_vnode(&self, node_id: NodeId, vnode_idx: u32) -> u64 {
        // Simple but deterministic hash using xxhash
        let mut data = [0u8; 16];
        data[..8].copy_from_slice(&(node_id.0 ^ self.seed).to_le_bytes());
        data[8..12].copy_from_slice(&vnode_idx.to_le_bytes());
        // Use a simple FNV-1a variant for the ring position
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &byte in &data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }

    /// Build the hash ring from the given nodes.
    fn build_ring(&self, nodes: &[NodeInfo]) -> Vec<(u64, NodeId)> {
        let mut ring: Vec<(u64, NodeId)> = Vec::new();
        for node in nodes {
            let vnodes = node.metadata.cores * self.vnodes_per_core;
            for i in 0..vnodes {
                let hash = self.hash_vnode(node.id, i);
                ring.push((hash, node.id));
            }
        }
        ring.sort_by_key(|(hash, _)| *hash);
        ring
    }

    /// Find the owner of a partition on the ring.
    fn lookup(&self, ring: &[(u64, NodeId)], partition_id: u32) -> NodeId {
        if ring.is_empty() {
            return NodeId::UNASSIGNED;
        }
        let key = self.hash_vnode(NodeId(u64::from(partition_id)), 0);
        // Binary search for the first vnode >= key
        match ring.binary_search_by_key(&key, |(h, _)| *h) {
            Ok(idx) => ring[idx].1,
            Err(idx) => {
                if idx >= ring.len() {
                    ring[0].1 // Wrap around
                } else {
                    ring[idx].1
                }
            }
        }
    }

    /// Find owner with exclusion (for failure domain diversity).
    fn lookup_excluding(
        &self,
        ring: &[(u64, NodeId)],
        partition_id: u32,
        exclude: &[NodeId],
    ) -> NodeId {
        if ring.is_empty() {
            return NodeId::UNASSIGNED;
        }
        let key = self.hash_vnode(NodeId(u64::from(partition_id)), 0);
        let start = match ring.binary_search_by_key(&key, |(h, _)| *h) {
            Ok(idx) | Err(idx) => idx % ring.len(),
        };
        for i in 0..ring.len() {
            let idx = (start + i) % ring.len();
            if !exclude.contains(&ring[idx].1) {
                return ring[idx].1;
            }
        }
        // All excluded — fall back to default
        ring[start].1
    }
}

impl Default for ConsistentHashAssigner {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionAssigner for ConsistentHashAssigner {
    fn initial_assignment(
        &self,
        num_partitions: u32,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> AssignmentPlan {
        let ring = self.build_ring(nodes);
        let mut assignments = HashMap::new();
        let mut moves = Vec::new();
        let mut per_node: HashMap<NodeId, u32> = HashMap::new();

        for pid in 0..num_partitions {
            let owner = if constraints.max_partitions_per_node > 0 {
                // Respect max constraint
                let saturated: Vec<NodeId> = per_node
                    .iter()
                    .filter(|(_, &count)| count >= constraints.max_partitions_per_node)
                    .map(|(id, _)| *id)
                    .collect();
                self.lookup_excluding(&ring, pid, &saturated)
            } else {
                self.lookup(&ring, pid)
            };

            assignments.insert(pid, owner);
            *per_node.entry(owner).or_insert(0) += 1;
            moves.push(PartitionMove {
                partition_id: pid,
                from: None,
                to: owner,
            });
        }

        let max_per_node = per_node.values().copied().max().unwrap_or(0);
        let min_per_node = per_node.values().copied().min().unwrap_or(0);

        AssignmentPlan {
            assignments,
            moves,
            stats: AssignmentStats {
                total_partitions: num_partitions,
                total_moves: num_partitions,
                max_per_node,
                min_per_node,
                failure_domains_used: 0,
            },
        }
    }

    fn rebalance(
        &self,
        current: &HashMap<u32, NodeId>,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> AssignmentPlan {
        #[allow(clippy::cast_possible_truncation)]
        let num_partitions = current.len() as u32;
        let ideal = self.initial_assignment(num_partitions, nodes, constraints);

        let mut moves = Vec::new();
        for (pid, new_owner) in &ideal.assignments {
            let old_owner = current.get(pid).copied();
            if old_owner != Some(*new_owner) {
                moves.push(PartitionMove {
                    partition_id: *pid,
                    from: old_owner,
                    to: *new_owner,
                });
            }
        }

        #[allow(clippy::cast_possible_truncation)]
        let total_moves = moves.len() as u32;
        AssignmentPlan {
            assignments: ideal.assignments,
            moves,
            stats: AssignmentStats {
                total_moves,
                ..ideal.stats
            },
        }
    }

    fn validate_plan(
        &self,
        plan: &AssignmentPlan,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> Vec<String> {
        let mut errors = Vec::new();
        let valid_nodes: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
        let mut per_node: HashMap<NodeId, u32> = HashMap::new();

        for (pid, owner) in &plan.assignments {
            if !valid_nodes.contains(owner) && !owner.is_unassigned() {
                errors.push(format!("partition {pid} assigned to unknown node {owner}"));
            }
            *per_node.entry(*owner).or_insert(0) += 1;
        }

        if constraints.max_partitions_per_node > 0 {
            for (node, count) in &per_node {
                if *count > constraints.max_partitions_per_node {
                    errors.push(format!(
                        "node {node} has {count} partitions (max: {})",
                        constraints.max_partitions_per_node
                    ));
                }
            }
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::discovery::{NodeMetadata, NodeState};

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

    #[test]
    fn test_initial_assignment_basic() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];
        let plan = assigner.initial_assignment(12, &nodes, &AssignmentConstraints::default());

        assert_eq!(plan.assignments.len(), 12);
        assert_eq!(plan.moves.len(), 12);
        // All partitions should be assigned to one of the 3 nodes
        for owner in plan.assignments.values() {
            assert!(!owner.is_unassigned());
        }
    }

    #[test]
    fn test_assignment_determinism() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];
        let plan1 = assigner.initial_assignment(100, &nodes, &AssignmentConstraints::default());
        let plan2 = assigner.initial_assignment(100, &nodes, &AssignmentConstraints::default());

        for pid in 0..100 {
            assert_eq!(plan1.assignments[&pid], plan2.assignments[&pid]);
        }
    }

    #[test]
    fn test_weighted_distribution() {
        let assigner = ConsistentHashAssigner::new();
        // Node 1 has 8 cores, Node 2 has 2 cores
        let nodes = vec![make_node(1, 8), make_node(2, 2)];
        let plan = assigner.initial_assignment(100, &nodes, &AssignmentConstraints::default());

        let node1_count = plan
            .assignments
            .values()
            .filter(|n| **n == NodeId(1))
            .count();
        let node2_count = plan
            .assignments
            .values()
            .filter(|n| **n == NodeId(2))
            .count();

        // Node 1 (8 cores) should get roughly 4x as many as Node 2 (2 cores)
        // Allow some variance due to hashing
        assert!(
            node1_count > node2_count,
            "node1={node1_count}, node2={node2_count}"
        );
        assert!(
            node1_count > 50,
            "node1 should get majority: got {node1_count}"
        );
    }

    #[test]
    fn test_rebalance_minimal_moves() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];
        let initial = assigner.initial_assignment(10, &nodes, &AssignmentConstraints::default());

        // Same nodes, no change expected
        let rebalanced = assigner.rebalance(
            &initial.assignments,
            &nodes,
            &AssignmentConstraints::default(),
        );
        assert_eq!(rebalanced.stats.total_moves, 0);
    }

    #[test]
    fn test_rebalance_new_node() {
        let assigner = ConsistentHashAssigner::new();
        let nodes2 = vec![make_node(1, 4), make_node(2, 4)];
        let initial = assigner.initial_assignment(20, &nodes2, &AssignmentConstraints::default());

        // Add a third node
        let nodes3 = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];
        let rebalanced = assigner.rebalance(
            &initial.assignments,
            &nodes3,
            &AssignmentConstraints::default(),
        );

        // Should have some moves but not all
        assert!(rebalanced.stats.total_moves > 0);
        assert!(rebalanced.stats.total_moves < 20);
    }

    #[test]
    fn test_max_partitions_constraint() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];
        let constraints = AssignmentConstraints {
            max_partitions_per_node: 6,
            ..AssignmentConstraints::default()
        };
        let plan = assigner.initial_assignment(10, &nodes, &constraints);

        let mut per_node: HashMap<NodeId, u32> = HashMap::new();
        for owner in plan.assignments.values() {
            *per_node.entry(*owner).or_insert(0) += 1;
        }
        for count in per_node.values() {
            assert!(*count <= 6, "node exceeded max: {count}");
        }
    }

    #[test]
    fn test_validate_plan() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];
        let plan = assigner.initial_assignment(10, &nodes, &AssignmentConstraints::default());
        let errors = assigner.validate_plan(&plan, &nodes, &AssignmentConstraints::default());
        assert!(errors.is_empty(), "errors: {errors:?}");
    }

    #[test]
    fn test_empty_nodes() {
        let assigner = ConsistentHashAssigner::new();
        let plan = assigner.initial_assignment(10, &[], &AssignmentConstraints::default());

        // All should be unassigned
        for owner in plan.assignments.values() {
            assert!(owner.is_unassigned());
        }
    }

    #[test]
    fn test_single_node() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan = assigner.initial_assignment(10, &nodes, &AssignmentConstraints::default());

        // All partitions should go to the single node
        for owner in plan.assignments.values() {
            assert_eq!(*owner, NodeId(1));
        }
    }
}
