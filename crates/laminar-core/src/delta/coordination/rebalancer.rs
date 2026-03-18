//! Partition rebalancing on membership changes.
//!
//! When a node joins or leaves the cluster, the partition assignment must
//! be recomputed. This module coordinates the rebalancing process:
//!
//! 1. Detect membership change via the `DeltaManager::should_rebalance` check
//! 2. Recompute partition assignment using `ConsistentHashAssigner::rebalance`
//! 3. For each partition that moved:
//!    a. Drain in-flight data on the old owner
//!    b. Snapshot partition state
//!    c. Transfer state to the new owner
//!    d. New owner restores state and starts processing
//! 4. Fence old owners using `EpochGuard` to prevent split-brain

#![allow(clippy::disallowed_types)] // cold path: rebalance planning
use std::collections::HashMap;

use crate::delta::coordination::DeltaManager;
use crate::delta::discovery::{NodeId, NodeInfo};
use crate::delta::partition::assignment::{AssignmentPlan, ConsistentHashAssigner};

/// Result of a rebalancing operation.
#[derive(Debug, Clone)]
pub struct RebalanceResult {
    /// The new assignment plan.
    pub plan: AssignmentPlan,
    /// Partitions that need migration from this node.
    pub outgoing_migrations: Vec<PartitionMigration>,
    /// Partitions that are being migrated to this node.
    pub incoming_migrations: Vec<PartitionMigration>,
}

/// A single partition migration.
#[derive(Debug, Clone)]
pub struct PartitionMigration {
    /// The partition being migrated.
    pub partition_id: u32,
    /// The source node (current owner).
    pub from: NodeId,
    /// The destination node (new owner).
    pub to: NodeId,
    /// The epoch for the new assignment.
    pub new_epoch: u64,
}

/// Status of an in-progress migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Draining in-flight data on the source node.
    Draining,
    /// Snapshotting partition state on the source node.
    Snapshotting,
    /// Transferring state to the destination node.
    Transferring,
    /// Destination node is restoring state.
    Restoring,
    /// Migration is complete.
    Complete,
    /// Migration failed.
    Failed,
}

/// Coordinates partition rebalancing for a node.
pub struct Rebalancer {
    /// The consistent hash assigner.
    assigner: ConsistentHashAssigner,
    /// Current partition-to-node assignments.
    current_assignments: HashMap<u32, NodeId>,
    /// In-progress migrations keyed by partition ID.
    active_migrations: HashMap<u32, (PartitionMigration, MigrationPhase)>,
    /// The next epoch to assign (monotonically increasing).
    next_epoch: u64,
}

impl Rebalancer {
    /// Create a new rebalancer.
    #[must_use]
    pub fn new(initial_plan: &AssignmentPlan) -> Self {
        Self {
            assigner: ConsistentHashAssigner::new(),
            current_assignments: initial_plan.assignments.clone(),
            active_migrations: HashMap::new(),
            next_epoch: 2, // epoch 1 = initial assignment
        }
    }

    /// Check if a rebalance is needed and compute the migration plan.
    ///
    /// Compares the current assignments against the ideal assignment for
    /// the new set of nodes and returns the partition moves.
    pub fn compute_rebalance(
        &mut self,
        manager: &DeltaManager,
        nodes: &[NodeInfo],
    ) -> RebalanceResult {
        let plan = self
            .assigner
            .rebalance(&self.current_assignments, nodes, manager.constraints());

        let local_node = manager.node_id();
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();

        for mv in &plan.moves {
            let epoch = self.next_epoch;
            self.next_epoch += 1;

            let migration = PartitionMigration {
                partition_id: mv.partition_id,
                from: mv.from.unwrap_or(NodeId::UNASSIGNED),
                to: mv.to,
                new_epoch: epoch,
            };

            if mv.from == Some(local_node) {
                outgoing.push(migration.clone());
            }
            if mv.to == local_node {
                incoming.push(migration.clone());
            }
        }

        RebalanceResult {
            plan,
            outgoing_migrations: outgoing,
            incoming_migrations: incoming,
        }
    }

    /// Apply a completed rebalance: update assignments and partition guards.
    ///
    /// This should be called after all migrations in the plan have completed
    /// (or been applied locally). Updates the router's view of assignments.
    pub fn apply_rebalance(&mut self, result: &RebalanceResult, manager: &mut DeltaManager) {
        // Update assignments
        self.current_assignments
            .clone_from(&result.plan.assignments);

        // Apply each move to the manager's partition guards
        for migration in result
            .outgoing_migrations
            .iter()
            .chain(result.incoming_migrations.iter())
        {
            manager.apply_migration_result(
                migration.partition_id,
                migration.from,
                migration.to,
                migration.new_epoch,
            );
        }
    }

    /// Record that a migration is in progress.
    pub fn start_migration(&mut self, migration: PartitionMigration) {
        self.active_migrations.insert(
            migration.partition_id,
            (migration, MigrationPhase::Draining),
        );
    }

    /// Advance a migration to the next phase.
    ///
    /// Returns `true` if the phase was updated, `false` if the migration
    /// is not tracked.
    pub fn advance_migration(&mut self, partition_id: u32, phase: MigrationPhase) -> bool {
        if let Some(entry) = self.active_migrations.get_mut(&partition_id) {
            entry.1 = phase;
            if phase == MigrationPhase::Complete || phase == MigrationPhase::Failed {
                self.active_migrations.remove(&partition_id);
            }
            true
        } else {
            false
        }
    }

    /// Check if there are any active migrations.
    #[must_use]
    pub fn has_active_migrations(&self) -> bool {
        !self.active_migrations.is_empty()
    }

    /// Get the current assignments.
    #[must_use]
    pub fn current_assignments(&self) -> &HashMap<u32, NodeId> {
        &self.current_assignments
    }
}

impl std::fmt::Debug for Rebalancer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rebalancer")
            .field("partitions", &self.current_assignments.len())
            .field("active_migrations", &self.active_migrations.len())
            .field("next_epoch", &self.next_epoch)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::discovery::{NodeMetadata, NodeState};
    use crate::delta::partition::assignment::AssignmentConstraints;

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
    fn test_rebalancer_no_change() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];
        let plan = assigner.initial_assignment(10, &nodes, &AssignmentConstraints::default());

        let mut manager = DeltaManager::new(NodeId(1));
        use crate::delta::coordination::NodeLifecyclePhase;
        manager.transition(NodeLifecyclePhase::FormingRaft);
        manager.transition(NodeLifecyclePhase::WaitingForAssignment);
        manager.transition(NodeLifecyclePhase::Active);

        let mut rebalancer = Rebalancer::new(&plan);
        let result = rebalancer.compute_rebalance(&manager, &nodes);

        // Same nodes → no moves
        assert!(result.plan.moves.is_empty());
        assert!(result.outgoing_migrations.is_empty());
        assert!(result.incoming_migrations.is_empty());
    }

    #[test]
    fn test_rebalancer_node_added() {
        let assigner = ConsistentHashAssigner::new();
        let nodes2 = vec![make_node(1, 4), make_node(2, 4)];
        let plan = assigner.initial_assignment(20, &nodes2, &AssignmentConstraints::default());

        let mut manager = DeltaManager::new(NodeId(1));
        use crate::delta::coordination::NodeLifecyclePhase;
        manager.transition(NodeLifecyclePhase::FormingRaft);
        manager.transition(NodeLifecyclePhase::WaitingForAssignment);
        manager.transition(NodeLifecyclePhase::Active);

        // Set up guards for node 1's partitions
        for (&pid, &owner) in &plan.assignments {
            if owner == NodeId(1) {
                manager.guards_mut().insert(pid, 1);
            }
        }

        let mut rebalancer = Rebalancer::new(&plan);

        // Add a third node
        let nodes3 = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];
        let result = rebalancer.compute_rebalance(&manager, &nodes3);

        // Should have some moves
        assert!(!result.plan.moves.is_empty());
    }

    #[test]
    fn test_rebalancer_apply() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];
        let plan = assigner.initial_assignment(10, &nodes, &AssignmentConstraints::default());

        let mut manager = DeltaManager::new(NodeId(1));
        use crate::delta::coordination::NodeLifecyclePhase;
        manager.transition(NodeLifecyclePhase::FormingRaft);
        manager.transition(NodeLifecyclePhase::WaitingForAssignment);
        manager.transition(NodeLifecyclePhase::Active);

        for (&pid, &owner) in &plan.assignments {
            if owner == NodeId(1) {
                manager.guards_mut().insert(pid, 1);
            }
        }

        let mut rebalancer = Rebalancer::new(&plan);
        let nodes3 = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];
        let result = rebalancer.compute_rebalance(&manager, &nodes3);
        rebalancer.apply_rebalance(&result, &mut manager);

        // Assignments should be updated
        assert_eq!(
            rebalancer.current_assignments().len(),
            result.plan.assignments.len()
        );
    }

    #[test]
    fn test_migration_lifecycle() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let mut rebalancer = Rebalancer::new(&plan);

        let migration = PartitionMigration {
            partition_id: 0,
            from: NodeId(1),
            to: NodeId(2),
            new_epoch: 2,
        };

        rebalancer.start_migration(migration);
        assert!(rebalancer.has_active_migrations());

        assert!(rebalancer.advance_migration(0, MigrationPhase::Snapshotting));
        assert!(rebalancer.has_active_migrations());

        assert!(rebalancer.advance_migration(0, MigrationPhase::Complete));
        assert!(!rebalancer.has_active_migrations());
    }

    #[test]
    fn test_advance_unknown_migration() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let mut rebalancer = Rebalancer::new(&plan);

        assert!(!rebalancer.advance_migration(99, MigrationPhase::Complete));
    }

    #[test]
    fn test_rebalancer_debug() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let rebalancer = Rebalancer::new(&plan);
        let debug = format!("{rebalancer:?}");
        assert!(debug.contains("Rebalancer"));
        assert!(debug.contains("partitions"));
    }
}
