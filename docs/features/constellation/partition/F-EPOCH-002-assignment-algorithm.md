# F-EPOCH-002: Partition Assignment Algorithm

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-EPOCH-002 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6b |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F-EPOCH-001 (PartitionGuard), F-COORD-001 (Raft Metadata) |
| **Blocks** | F-EPOCH-003 (Reassignment Protocol) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/constellation/partition/assignment.rs` |

## Summary

Partition assignment determines which node (Star) in the constellation owns each partition. The algorithm considers node capacity (available cores, memory), current load, failure domain awareness (rack/availability zone), and user-provided affinity hints. It runs at two key moments: initial constellation formation (all partitions assigned from scratch) and rebalancing (on node join/leave). The implementation uses consistent hashing with virtual nodes for assignment stability, minimizing the number of partition moves during cluster topology changes. The algorithm is deterministic: given the same inputs (node set, constraints, partition count), it always produces the same assignment.

## Goals

- Define `PartitionAssigner` trait as the pluggable assignment interface
- Implement `ConsistentHashAssigner` using consistent hashing with virtual nodes
- Support assignment constraints: max partitions per node, rack/AZ diversity, memory limits
- Minimize partition movement on node join/leave (consistent hashing property)
- Produce a deterministic `AssignmentPlan` that can be committed via Raft
- Support affinity hints (e.g., "co-locate partitions 0-3 on same node")
- Enable dry-run mode for plan preview before committing

## Non-Goals

- Partition migration execution (F-EPOCH-003)
- Dynamic partition splitting or merging (future feature)
- Cross-datacenter assignment (single constellation = single datacenter)
- Real-time load balancing (assignment is a discrete event, not continuous)
- Capacity-based scheduling (LaminarDB uses node-ownership, not capacity-based)

## Technical Design

### Architecture

The assignment algorithm runs on the **Raft leader** in **Ring 2 (Control Plane)**. It reads the current constellation state (node list, partition map, constraints) and produces an `AssignmentPlan` that describes the desired partition-to-node mapping. The plan is then committed as a Raft log entry. Non-leader nodes receive the committed plan and update their local state.

```
                        Raft Leader (Ring 2)
                    +---------------------------+
                    |  PartitionAssigner         |
  Node join/leave   |                           |   AssignmentPlan
  ----------------> |  1. Read current state    | ----------------->
                    |  2. Compute new mapping   |   (committed via Raft)
  Constraints       |  3. Minimize movement     |
  ----------------> |  4. Validate constraints  |
                    |  5. Output plan           |
  Affinity hints    |                           |
  ----------------> +---------------------------+
                              |
                              v
                    +---------------------------+
                    |  ConstellationMetadata     |
                    |  (updated partition_map)   |
                    +---------------------------+
```

### API/Interface

```rust
use std::collections::{HashMap, HashSet};

/// Trait for partition assignment strategies.
///
/// Implementations must be deterministic: the same inputs must always
/// produce the same output. This is critical for Raft replicated state
/// machine correctness.
pub trait PartitionAssigner: Send + Sync {
    /// Compute an initial assignment for all partitions.
    ///
    /// Called at constellation formation when no partitions are assigned.
    fn initial_assignment(
        &self,
        partitions: &[PartitionId],
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> Result<AssignmentPlan, AssignmentError>;

    /// Compute a rebalancing plan given the current state and a topology change.
    ///
    /// Called when a node joins (Rising) or leaves (Setting). The plan
    /// should minimize partition movement while respecting constraints.
    fn rebalance(
        &self,
        current: &HashMap<PartitionId, NodeId>,
        nodes: &[NodeInfo],
        change: TopologyChange,
        constraints: &AssignmentConstraints,
    ) -> Result<AssignmentPlan, AssignmentError>;

    /// Validate that an assignment plan respects all constraints.
    fn validate_plan(
        &self,
        plan: &AssignmentPlan,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> Result<(), AssignmentError>;
}

/// Describes a topology change that triggers rebalancing.
#[derive(Debug, Clone)]
pub enum TopologyChange {
    /// A new node joined the constellation (Rising).
    NodeJoined(NodeInfo),

    /// A node left the constellation (Setting).
    NodeLeft(NodeId),

    /// A node's capacity changed (e.g., cores added/removed).
    NodeUpdated(NodeInfo),

    /// Manual rebalance requested by operator.
    ManualRebalance,
}
```

### Data Structures

```rust
/// Information about a node in the constellation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier.
    pub id: NodeId,

    /// Number of available CPU cores for partition processing.
    pub cores: u32,

    /// Available memory in bytes.
    pub memory_bytes: u64,

    /// Failure domain: rack or availability zone.
    /// Partitions should be spread across failure domains.
    pub failure_domain: Option<String>,

    /// Node tags for affinity matching.
    pub tags: HashMap<String, String>,

    /// Current node state.
    pub state: NodeState,

    /// Network address for inter-node communication.
    pub address: String,
}

/// Constraints for partition assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentConstraints {
    /// Maximum number of partitions per node.
    /// Default: (total_partitions / num_nodes) * 1.5, rounded up.
    pub max_partitions_per_node: Option<u32>,

    /// Minimum number of distinct failure domains across all partitions.
    /// E.g., if set to 2, partitions must be spread across at least 2 racks/AZs.
    pub min_failure_domains: Option<u32>,

    /// Require that no two partitions with the same affinity group are on the same node.
    /// Used for anti-affinity (spread partitions of the same table across nodes).
    pub anti_affinity_groups: Vec<AntiAffinityGroup>,

    /// Prefer co-locating these partition sets on the same node.
    /// Soft constraint: best-effort, violated if hard constraints conflict.
    pub affinity_hints: Vec<AffinityHint>,

    /// Maximum memory per node that can be used by assigned partitions.
    pub max_memory_per_node: Option<u64>,

    /// Weight factor for each node (default 1.0). Higher weight = more partitions.
    pub node_weights: HashMap<NodeId, f64>,
}

impl Default for AssignmentConstraints {
    fn default() -> Self {
        Self {
            max_partitions_per_node: None,
            min_failure_domains: None,
            anti_affinity_groups: Vec::new(),
            affinity_hints: Vec::new(),
            max_memory_per_node: None,
            node_weights: HashMap::new(),
        }
    }
}

/// Anti-affinity group: partitions that should be on different nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiAffinityGroup {
    pub name: String,
    pub partition_ids: Vec<PartitionId>,
}

/// Affinity hint: partitions that should preferably be co-located.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffinityHint {
    pub name: String,
    pub partition_ids: Vec<PartitionId>,
    /// Strength: 0.0 (ignore) to 1.0 (must co-locate).
    pub strength: f64,
}

/// The output of the assignment algorithm: a plan of partition moves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentPlan {
    /// The desired mapping from partition to (node, epoch).
    /// For new assignments, epoch is the initial epoch.
    /// For moves, epoch is incremented from the current epoch.
    pub assignments: HashMap<PartitionId, PartitionAssignment>,

    /// Partitions that need to be moved (old_node -> new_node).
    pub moves: Vec<PartitionMove>,

    /// Summary statistics.
    pub stats: AssignmentStats,
}

/// A single partition assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub partition_id: PartitionId,
    pub node_id: NodeId,
    pub epoch: u64,
}

/// A partition move (reassignment from one node to another).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMove {
    pub partition_id: PartitionId,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub old_epoch: u64,
    pub new_epoch: u64,
}

/// Statistics about an assignment plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentStats {
    /// Total number of partitions assigned.
    pub total_partitions: u32,

    /// Number of partitions that moved.
    pub partitions_moved: u32,

    /// Distribution: node_id -> number of partitions.
    pub distribution: HashMap<NodeId, u32>,

    /// Number of distinct failure domains used.
    pub failure_domains_used: u32,

    /// Whether all constraints were satisfied.
    pub constraints_satisfied: bool,

    /// Constraint violations (empty if all satisfied).
    pub violations: Vec<String>,
}

/// Consistent hash-based partition assigner.
///
/// Uses a hash ring with virtual nodes for even distribution.
/// Each physical node gets `virtual_nodes_per_core * cores` virtual nodes
/// on the ring, so nodes with more cores get more partitions proportionally.
pub struct ConsistentHashAssigner {
    /// Number of virtual nodes per core. Default: 150.
    virtual_nodes_per_core: u32,

    /// Hash function seed for deterministic results.
    seed: u64,
}

impl ConsistentHashAssigner {
    /// Create a new consistent hash assigner.
    pub fn new() -> Self {
        Self {
            virtual_nodes_per_core: 150,
            seed: 0x517CC1B7_27220A95, // Fixed seed for determinism
        }
    }

    /// Create with custom virtual node count.
    pub fn with_virtual_nodes(virtual_nodes_per_core: u32) -> Self {
        Self {
            virtual_nodes_per_core,
            seed: 0x517CC1B7_27220A95,
        }
    }

    /// Build the hash ring from the given nodes.
    fn build_ring(&self, nodes: &[NodeInfo]) -> HashRing {
        let mut ring = HashRing::new();
        for node in nodes {
            if node.state != NodeState::Active {
                continue; // Skip non-active nodes
            }
            let weight = node.cores as u32;
            let vnodes = weight * self.virtual_nodes_per_core;
            for i in 0..vnodes {
                let hash = self.hash_vnode(node.id, i);
                ring.insert(hash, node.id);
            }
        }
        ring
    }

    /// Deterministic hash for a virtual node.
    fn hash_vnode(&self, node: NodeId, vnode_index: u32) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        hasher.write_u64(self.seed);
        hasher.write_u64(node.0);
        hasher.write_u32(vnode_index);
        hasher.finish()
    }

    /// Deterministic hash for a partition.
    fn hash_partition(&self, partition: PartitionId) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        hasher.write_u64(self.seed);
        hasher.write_u32(partition.0);
        hasher.finish()
    }
}

/// Internal hash ring data structure (sorted vec of (hash, node_id)).
struct HashRing {
    ring: Vec<(u64, NodeId)>,
}

impl HashRing {
    fn new() -> Self {
        Self { ring: Vec::new() }
    }

    fn insert(&mut self, hash: u64, node: NodeId) {
        self.ring.push((hash, node));
    }

    fn finalize(&mut self) {
        self.ring.sort_by_key(|(hash, _)| *hash);
    }

    /// Find the node responsible for the given hash.
    fn lookup(&self, hash: u64) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        let idx = match self.ring.binary_search_by_key(&hash, |(h, _)| *h) {
            Ok(i) => i,
            Err(i) => i % self.ring.len(),
        };
        Some(self.ring[idx].1)
    }

    /// Find the node for a hash, skipping nodes in the exclusion set.
    /// Used for failure domain diversity.
    fn lookup_excluding(
        &self,
        hash: u64,
        exclude_domains: &HashSet<String>,
        node_domains: &HashMap<NodeId, String>,
    ) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        let start = match self.ring.binary_search_by_key(&hash, |(h, _)| *h) {
            Ok(i) => i,
            Err(i) => i % self.ring.len(),
        };
        for offset in 0..self.ring.len() {
            let idx = (start + offset) % self.ring.len();
            let node = self.ring[idx].1;
            if let Some(domain) = node_domains.get(&node) {
                if !exclude_domains.contains(domain) {
                    return Some(node);
                }
            } else {
                return Some(node); // No domain = no constraint
            }
        }
        None // All nodes excluded
    }
}
```

### Algorithm/Flow

**Initial Assignment:**

1. Build consistent hash ring with virtual nodes proportional to each node's core count.
2. For each partition, hash the partition ID and walk the ring clockwise to find the owning node.
3. Check failure domain constraint: if the assigned node is in an already-saturated failure domain, walk further on the ring to find a node in a different domain.
4. Check max-partitions-per-node constraint: if the node already has the maximum, skip to the next node on the ring.
5. Apply affinity hints as a post-processing step: swap partitions between nodes to improve co-location without violating hard constraints.
6. Assign epoch 1 to all partitions.
7. Validate the plan against all constraints; return violations if any hard constraint is unmet.

**Rebalance on Node Join (Rising):**

1. Rebuild the hash ring with the new node included.
2. For each partition, recompute the owning node.
3. Only move partitions whose computed owner changed (consistent hashing minimizes this).
4. For moved partitions, increment the epoch by 1.
5. Validate constraints and produce the plan.

**Rebalance on Node Leave (Setting):**

1. Rebuild the hash ring without the departed node.
2. All partitions previously on the departed node will naturally map to the next node on the ring.
3. Increment epochs for reassigned partitions.
4. Validate constraints.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `AssignmentError::NoActiveNodes` | All nodes are down or draining | Wait for nodes to recover; alert operator |
| `AssignmentError::InsufficientCapacity` | Not enough cores/memory across all nodes | Add more nodes or reduce partition count |
| `AssignmentError::ConstraintViolation` | Hard constraint cannot be satisfied (e.g., not enough failure domains) | Relax constraints or add nodes in required domains |
| `AssignmentError::MaxPartitionsExceeded` | A node would exceed max partitions after assignment | Increase limit or add more nodes |
| `AssignmentError::AffinityConflict` | Affinity hint conflicts with anti-affinity or failure domain constraint | Hint is soft; log warning and skip |
| `AssignmentError::EmptyPartitionList` | No partitions to assign | No-op; return empty plan |

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Initial assignment (1000 partitions, 10 nodes) | < 50ms | Hash ring build + O(P * log(V)) lookups |
| Rebalance (1 node join, 1000 partitions) | < 20ms | Only recompute affected partitions |
| Rebalance (1 node leave, 1000 partitions) | < 20ms | Same as join |
| Partition movement on single node join | < 1/N of total | Consistent hashing property (N = node count) |
| Hash ring memory (10 nodes, 8 cores each) | < 1 MB | 150 vnodes/core * 80 cores * 16 bytes |
| Plan serialization | < 5ms | JSON for Raft log entry |

## Test Plan

### Unit Tests

- [ ] `test_initial_assignment_all_partitions_assigned` - Every partition gets a node
- [ ] `test_initial_assignment_even_distribution` - Partitions spread evenly (+/- 10%)
- [ ] `test_initial_assignment_deterministic` - Same inputs produce same output
- [ ] `test_rebalance_node_join_minimal_movement` - < 1/N partitions move
- [ ] `test_rebalance_node_leave_all_reassigned` - Departed node's partitions redistributed
- [ ] `test_rebalance_node_leave_epochs_incremented` - Moved partitions get new epochs
- [ ] `test_constraint_max_partitions_per_node` - Limit respected
- [ ] `test_constraint_failure_domain_diversity` - Partitions spread across domains
- [ ] `test_constraint_anti_affinity` - Anti-affinity group partitions on different nodes
- [ ] `test_affinity_hint_co_location` - Co-located partitions on same node when possible
- [ ] `test_no_active_nodes_returns_error` - Error when all nodes down
- [ ] `test_single_node_gets_all_partitions` - Works with single node
- [ ] `test_node_weight_proportional` - Higher weight nodes get more partitions
- [ ] `test_hash_ring_lookup_wraps_around` - Ring wraps correctly
- [ ] `test_validate_plan_detects_violations` - Validation catches constraint violations
- [ ] `test_empty_partition_list` - Empty input produces empty plan

### Integration Tests

- [ ] End-to-end: Form constellation, assign partitions, verify Raft state
- [ ] Node join: Add node, verify rebalance plan committed via Raft, verify epoch increments
- [ ] Node leave: Remove node, verify reassignment, verify no orphaned partitions
- [ ] Rolling restart: Nodes leave and rejoin one at a time; verify minimal total movement
- [ ] Multi-AZ: 3 AZs with 3 nodes each; verify AZ-diverse assignment

### Benchmarks

- [ ] `bench_initial_assignment_100_partitions_5_nodes` - Target: < 5ms
- [ ] `bench_initial_assignment_10000_partitions_50_nodes` - Target: < 200ms
- [ ] `bench_rebalance_node_join` - Target: < 20ms
- [ ] `bench_rebalance_node_leave` - Target: < 20ms
- [ ] `bench_hash_ring_build` - Target: < 10ms for 50 nodes * 8 cores

## Rollout Plan

1. **Phase 1**: Implement `HashRing`, `ConsistentHashAssigner::initial_assignment()` with unit tests
2. **Phase 2**: Add rebalancing logic with topology change handling
3. **Phase 3**: Implement constraint validation (max partitions, failure domains)
4. **Phase 4**: Add affinity/anti-affinity hints
5. **Phase 5**: Integration with Raft for plan commitment
6. **Phase 6**: Benchmarks, documentation, code review

## Open Questions

- [ ] Should we support pluggable hash functions, or is `DefaultHasher` sufficient for consistent hashing?
- [ ] For very large clusters (100+ nodes), should we shard the hash ring or use a different algorithm (e.g., rendezvous hashing)?
- [ ] Should the rebalance threshold be configurable (e.g., only rebalance if imbalance exceeds 20%)?
- [ ] How should we handle the case where a node joins with zero cores (e.g., a pure coordinator node)?
- [ ] Should assignment plans support a "dry-run" mode that shows the plan without committing?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-EPOCH-001: PartitionGuard & Epoch Fencing](F-EPOCH-001-partition-guard.md)
- [F-EPOCH-003: Partition Reassignment Protocol](F-EPOCH-003-reassignment-protocol.md)
- [F-COORD-001: ConstellationMetadata & Raft Integration](../coordination/F-COORD-001-raft-metadata.md)
- [Consistent Hashing and Random Trees (Karger et al., 1997)](https://dl.acm.org/doi/10.1145/258533.258660)
- [Jump Consistent Hash (Lamping & Veach, 2014)](https://arxiv.org/abs/1406.2294)
- [Apache Kafka Partition Assignment Strategies](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy)
