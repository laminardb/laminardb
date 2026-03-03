//! Delta orchestrator — top-level lifecycle management.
//!
//! The `DeltaManager` coordinates the full lifecycle of a node
//! in the delta: discovery → Raft formation → partition assignment
//! → active processing → graceful drain → shutdown.
//!
//! It also drives rebalancing: when membership changes, the leader
//! computes a new partition assignment and generates a serial
//! migration plan.

use std::fmt;

use crate::delta::coordination::metadata::DeltaMetadata;
use crate::delta::discovery::{MembershipEvent, NodeId, NodeInfo, NodeState};
use crate::delta::partition::assignment::AssignmentConstraints;
use crate::delta::partition::guard::PartitionGuardSet;
use crate::delta::partition::migration::{
    MigrationConfig, MigrationError, MigrationExecutor, MigrationPlan, MigrationProtocol,
};

/// The current lifecycle phase of a node in the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeLifecyclePhase {
    /// Discovering other nodes via the configured discovery backend.
    Discovering,
    /// Forming or joining the Raft group.
    FormingRaft,
    /// Waiting for the leader to assign partitions.
    WaitingForAssignment,
    /// Restoring partition state from checkpoints.
    RestoringPartitions,
    /// Fully active, processing events.
    Active,
    /// Gracefully draining partitions before shutdown.
    Draining,
    /// Shutdown complete.
    Shutdown,
}

impl NodeLifecyclePhase {
    /// Returns `true` if the phase allows partition processing.
    #[must_use]
    pub fn is_operational(&self) -> bool {
        matches!(self, Self::Active | Self::Draining)
    }

    /// Returns `true` if the phase is a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Shutdown)
    }
}

impl fmt::Display for NodeLifecyclePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovering => write!(f, "discovering"),
            Self::FormingRaft => write!(f, "forming-raft"),
            Self::WaitingForAssignment => write!(f, "waiting-for-assignment"),
            Self::RestoringPartitions => write!(f, "restoring-partitions"),
            Self::Active => write!(f, "active"),
            Self::Draining => write!(f, "draining"),
            Self::Shutdown => write!(f, "shutdown"),
        }
    }
}

/// Top-level delta lifecycle orchestrator.
///
/// Manages the node lifecycle and coordinates rebalancing when
/// cluster membership changes.
pub struct DeltaManager {
    /// This node's ID.
    pub node_id: NodeId,
    /// Current lifecycle phase.
    pub phase: NodeLifecyclePhase,
    /// Partition guards for this node.
    pub guards: PartitionGuardSet,
    /// Migration executor for active migrations.
    pub executor: MigrationExecutor,
    /// Migration protocol for computing plans and driving phase transitions.
    protocol: MigrationProtocol,
    /// Assignment constraints.
    constraints: AssignmentConstraints,
}

impl DeltaManager {
    /// Create a new manager.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            phase: NodeLifecyclePhase::Discovering,
            guards: PartitionGuardSet::new(node_id),
            executor: MigrationExecutor::new(MigrationConfig::default()),
            protocol: MigrationProtocol::new(),
            constraints: AssignmentConstraints::default(),
        }
    }

    /// Create with custom migration config and constraints.
    #[must_use]
    pub fn with_config(
        node_id: NodeId,
        migration_config: MigrationConfig,
        constraints: AssignmentConstraints,
    ) -> Self {
        Self {
            node_id,
            phase: NodeLifecyclePhase::Discovering,
            guards: PartitionGuardSet::new(node_id),
            executor: MigrationExecutor::new(migration_config),
            protocol: MigrationProtocol::new(),
            constraints,
        }
    }

    /// Transition to the next lifecycle phase.
    ///
    /// Returns `true` if the transition was valid.
    pub fn transition(&mut self, next: NodeLifecyclePhase) -> bool {
        let valid = matches!(
            (&self.phase, &next),
            (
                NodeLifecyclePhase::Discovering,
                NodeLifecyclePhase::FormingRaft
            ) | (
                NodeLifecyclePhase::FormingRaft,
                NodeLifecyclePhase::WaitingForAssignment
            ) | (
                NodeLifecyclePhase::WaitingForAssignment,
                NodeLifecyclePhase::RestoringPartitions | NodeLifecyclePhase::Active
            ) | (
                NodeLifecyclePhase::RestoringPartitions,
                NodeLifecyclePhase::Active
            ) | (NodeLifecyclePhase::Active, NodeLifecyclePhase::Draining)
                | (NodeLifecyclePhase::Draining, NodeLifecyclePhase::Shutdown)
        );

        if valid {
            self.phase = next;
        }
        valid
    }

    /// Compute a rebalance plan based on current metadata and cluster nodes.
    ///
    /// This should be called by the Raft leader when membership changes.
    /// Returns a plan of serial migrations to execute.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError`] if no active nodes are available.
    pub fn rebalance(
        &self,
        metadata: &DeltaMetadata,
        nodes: &[NodeInfo],
    ) -> Result<MigrationPlan, MigrationError> {
        self.protocol
            .plan_rebalance(metadata, nodes, &self.constraints)
    }

    /// Process a membership event and determine if rebalancing is needed.
    ///
    /// Returns `true` if the event warrants a rebalance (the caller should
    /// then call [`rebalance`](Self::rebalance)).
    #[must_use]
    pub fn should_rebalance(&self, event: &MembershipEvent) -> bool {
        if !self.phase.is_operational() {
            return false;
        }

        match event {
            MembershipEvent::NodeJoined(_) | MembershipEvent::NodeLeft(_) => true,
            MembershipEvent::NodeStateChanged {
                new_state,
                old_state,
                ..
            } => {
                // Rebalance if a node became active or left
                matches!(new_state, NodeState::Active | NodeState::Left)
                    || matches!(old_state, NodeState::Active)
            }
        }
    }

    /// Apply the guard changes for a completed migration on this node.
    ///
    /// If this node was the source, removes the partition guard.
    /// If this node was the target, inserts a new guard at the new epoch.
    pub fn apply_migration_result(
        &mut self,
        partition_id: u32,
        from_node: NodeId,
        to_node: NodeId,
        new_epoch: u64,
    ) {
        if from_node == self.node_id {
            // We were the source — fence and remove
            self.guards.update_epoch(partition_id, new_epoch);
            self.guards.remove(partition_id);
        }
        if to_node == self.node_id {
            // We are the target — acquire
            self.guards.insert(partition_id, new_epoch);
        }
    }

    /// Get a reference to the migration protocol.
    #[must_use]
    pub fn protocol(&self) -> &MigrationProtocol {
        &self.protocol
    }

    /// Get the current assignment constraints.
    #[must_use]
    pub fn constraints(&self) -> &AssignmentConstraints {
        &self.constraints
    }

    /// Update the assignment constraints.
    pub fn set_constraints(&mut self, constraints: AssignmentConstraints) {
        self.constraints = constraints;
    }
}

impl fmt::Debug for DeltaManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaManager")
            .field("node_id", &self.node_id)
            .field("phase", &self.phase)
            .field("guards", &self.guards.len())
            .field("active_migrations", &self.executor.active_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::coordination::metadata::MetadataLogEntry;
    use crate::delta::discovery::NodeMetadata;

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

    // ── Lifecycle phase tests ──

    #[test]
    fn test_lifecycle_phase_display() {
        assert_eq!(NodeLifecyclePhase::Active.to_string(), "active");
        assert_eq!(NodeLifecyclePhase::Draining.to_string(), "draining");
        assert_eq!(NodeLifecyclePhase::FormingRaft.to_string(), "forming-raft");
    }

    #[test]
    fn test_lifecycle_operational() {
        assert!(NodeLifecyclePhase::Active.is_operational());
        assert!(NodeLifecyclePhase::Draining.is_operational());
        assert!(!NodeLifecyclePhase::Discovering.is_operational());
        assert!(!NodeLifecyclePhase::Shutdown.is_operational());
    }

    #[test]
    fn test_lifecycle_terminal() {
        assert!(NodeLifecyclePhase::Shutdown.is_terminal());
        assert!(!NodeLifecyclePhase::Active.is_terminal());
    }

    #[test]
    fn test_manager_initial_phase() {
        let mgr = DeltaManager::new(NodeId(1));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Discovering);
        assert!(mgr.guards.is_empty());
        assert_eq!(mgr.executor.active_count(), 0);
    }

    // ── Lifecycle transitions ──

    #[test]
    fn test_valid_transitions() {
        let mut mgr = DeltaManager::new(NodeId(1));

        assert!(mgr.transition(NodeLifecyclePhase::FormingRaft));
        assert_eq!(mgr.phase, NodeLifecyclePhase::FormingRaft);

        assert!(mgr.transition(NodeLifecyclePhase::WaitingForAssignment));
        assert_eq!(mgr.phase, NodeLifecyclePhase::WaitingForAssignment);

        assert!(mgr.transition(NodeLifecyclePhase::RestoringPartitions));
        assert_eq!(mgr.phase, NodeLifecyclePhase::RestoringPartitions);

        assert!(mgr.transition(NodeLifecyclePhase::Active));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Active);

        assert!(mgr.transition(NodeLifecyclePhase::Draining));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Draining);

        assert!(mgr.transition(NodeLifecyclePhase::Shutdown));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Shutdown);
    }

    #[test]
    fn test_skip_restore_transition() {
        let mut mgr = DeltaManager::new(NodeId(1));
        mgr.transition(NodeLifecyclePhase::FormingRaft);
        mgr.transition(NodeLifecyclePhase::WaitingForAssignment);

        // Can skip directly to Active (no state to restore)
        assert!(mgr.transition(NodeLifecyclePhase::Active));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Active);
    }

    #[test]
    fn test_invalid_transitions() {
        let mut mgr = DeltaManager::new(NodeId(1));

        // Can't skip directly to Active from Discovering
        assert!(!mgr.transition(NodeLifecyclePhase::Active));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Discovering);

        // Can't go backwards
        mgr.transition(NodeLifecyclePhase::FormingRaft);
        assert!(!mgr.transition(NodeLifecyclePhase::Discovering));
        assert_eq!(mgr.phase, NodeLifecyclePhase::FormingRaft);
    }

    // ── Rebalance tests ──

    #[test]
    fn test_rebalance_add_node() {
        let mgr = DeltaManager::new(NodeId(1));
        let mut metadata = DeltaMetadata::new();

        // Assign 12 partitions across 2 nodes
        for pid in 0..6 {
            metadata.apply(&MetadataLogEntry::AssignPartition {
                partition_id: pid,
                node_id: NodeId(1),
                epoch: 1,
            });
        }
        for pid in 6..12 {
            metadata.apply(&MetadataLogEntry::AssignPartition {
                partition_id: pid,
                node_id: NodeId(2),
                epoch: 1,
            });
        }

        // Add third node
        let nodes = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];
        let plan = mgr.rebalance(&metadata, &nodes).unwrap();

        assert!(!plan.is_empty());
        // Some partitions move to node 3
        assert!(plan.tasks.iter().any(|t| t.to_node == NodeId(3)));
    }

    #[test]
    fn test_rebalance_stable() {
        use crate::delta::partition::assignment::{
            AssignmentConstraints, ConsistentHashAssigner, PartitionAssigner,
        };

        let mgr = DeltaManager::new(NodeId(1));
        let nodes = vec![make_node(1, 4), make_node(2, 4)];

        // Use the assigner's own assignment for consistency
        let assigner = ConsistentHashAssigner::new();
        let initial = assigner.initial_assignment(12, &nodes, &AssignmentConstraints::default());

        let mut metadata = DeltaMetadata::new();
        for (pid, node_id) in &initial.assignments {
            metadata.apply(&MetadataLogEntry::AssignPartition {
                partition_id: *pid,
                node_id: *node_id,
                epoch: 1,
            });
        }

        let plan = mgr.rebalance(&metadata, &nodes).unwrap();
        assert!(plan.is_empty());
    }

    // ── Membership event tests ──

    #[test]
    fn test_should_rebalance_when_active() {
        let mut mgr = DeltaManager::new(NodeId(1));
        mgr.phase = NodeLifecyclePhase::Active;

        assert!(mgr.should_rebalance(&MembershipEvent::NodeJoined(Box::new(make_node(2, 4)))));
        assert!(mgr.should_rebalance(&MembershipEvent::NodeLeft(NodeId(2))));
        assert!(mgr.should_rebalance(&MembershipEvent::NodeStateChanged {
            node_id: NodeId(2),
            old_state: NodeState::Suspected,
            new_state: NodeState::Left,
        }));
    }

    #[test]
    fn test_should_not_rebalance_when_discovering() {
        let mgr = DeltaManager::new(NodeId(1));
        assert!(!mgr.should_rebalance(&MembershipEvent::NodeJoined(Box::new(make_node(2, 4)))));
    }

    #[test]
    fn test_should_rebalance_state_change_active_to_suspected() {
        let mut mgr = DeltaManager::new(NodeId(1));
        mgr.phase = NodeLifecyclePhase::Active;

        // Node was active but is now suspected — should rebalance
        assert!(mgr.should_rebalance(&MembershipEvent::NodeStateChanged {
            node_id: NodeId(2),
            old_state: NodeState::Active,
            new_state: NodeState::Suspected,
        }));
    }

    #[test]
    fn test_should_not_rebalance_joining_to_suspected() {
        let mut mgr = DeltaManager::new(NodeId(1));
        mgr.phase = NodeLifecyclePhase::Active;

        // Joining → Suspected: not a loss of an active node
        assert!(!mgr.should_rebalance(&MembershipEvent::NodeStateChanged {
            node_id: NodeId(2),
            old_state: NodeState::Joining,
            new_state: NodeState::Suspected,
        }));
    }

    // ── Guard management tests ──

    #[test]
    fn test_apply_migration_result_source() {
        let mut mgr = DeltaManager::new(NodeId(1));
        mgr.guards.insert(5, 1); // We own partition 5 at epoch 1

        mgr.apply_migration_result(5, NodeId(1), NodeId(2), 2);

        // Guard should be removed (we were the source)
        assert!(mgr.guards.is_empty());
    }

    #[test]
    fn test_apply_migration_result_target() {
        let mut mgr = DeltaManager::new(NodeId(2));

        mgr.apply_migration_result(5, NodeId(1), NodeId(2), 2);

        // Guard should be inserted (we are the target)
        assert_eq!(mgr.guards.len(), 1);
        assert!(mgr.guards.check(5).is_ok());
    }

    #[test]
    fn test_apply_migration_result_neither() {
        let mut mgr = DeltaManager::new(NodeId(3));

        // We are neither source nor target
        mgr.apply_migration_result(5, NodeId(1), NodeId(2), 2);
        assert!(mgr.guards.is_empty());
    }

    // ── Full rebalance + guard integration ──

    #[test]
    fn test_full_rebalance_guard_cycle() {
        let mut source_mgr = DeltaManager::new(NodeId(1));
        source_mgr.phase = NodeLifecyclePhase::Active;
        source_mgr.guards.insert(0, 1);
        source_mgr.guards.insert(1, 1);

        let mut target_mgr = DeltaManager::new(NodeId(2));
        target_mgr.phase = NodeLifecyclePhase::Active;

        let mut metadata = DeltaMetadata::new();
        metadata.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1,
        });
        metadata.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 1,
            node_id: NodeId(1),
            epoch: 1,
        });

        // Simulate migration of partition 0: node 1 → node 2
        let task =
            crate::delta::partition::migration::MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);

        // Apply Raft entries
        metadata.apply(&task.release_entry());
        metadata.apply(&task.acquire_entry());

        // Apply guard changes
        source_mgr.apply_migration_result(0, NodeId(1), NodeId(2), 2);
        target_mgr.apply_migration_result(0, NodeId(1), NodeId(2), 2);

        // Source lost partition 0 but keeps partition 1
        assert_eq!(source_mgr.guards.len(), 1);
        assert!(source_mgr.guards.check(0).is_err());
        assert!(source_mgr.guards.check(1).is_ok());

        // Target gained partition 0
        assert_eq!(target_mgr.guards.len(), 1);
        assert!(target_mgr.guards.check(0).is_ok());

        // Metadata reflects new ownership
        let owner = metadata.partition_owner(0).unwrap();
        assert_eq!(owner.node_id, NodeId(2));
        assert_eq!(owner.epoch, 2);
    }

    // ── Config tests ──

    #[test]
    fn test_manager_with_config() {
        let config = MigrationConfig {
            max_concurrent: 4,
            ..MigrationConfig::default()
        };
        let constraints = AssignmentConstraints {
            max_partitions_per_node: 10,
            ..AssignmentConstraints::default()
        };
        let mgr = DeltaManager::with_config(NodeId(1), config, constraints);
        assert_eq!(mgr.executor.config().max_concurrent, 4);
        assert_eq!(mgr.constraints().max_partitions_per_node, 10);
    }

    #[test]
    fn test_manager_set_constraints() {
        let mut mgr = DeltaManager::new(NodeId(1));
        let constraints = AssignmentConstraints {
            max_partitions_per_node: 20,
            ..AssignmentConstraints::default()
        };
        mgr.set_constraints(constraints);
        assert_eq!(mgr.constraints().max_partitions_per_node, 20);
    }

    #[test]
    fn test_manager_debug() {
        let mgr = DeltaManager::new(NodeId(1));
        let debug = format!("{mgr:?}");
        assert!(debug.contains("DeltaManager"));
        assert!(debug.contains("NodeId(1)"));
    }
}
