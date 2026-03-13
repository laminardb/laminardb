//! # Delta Coordination
//!
//! Implements the orchestration layer for the delta.
//! This module manages the lifecycle of a node in the delta:
//! discovery, partition assignment, and graceful shutdown.

use std::fmt;

use crate::delta::discovery::{MembershipEvent, NodeId, NodeState};
use crate::delta::partition::assignment::AssignmentConstraints;
use crate::delta::partition::guard::PartitionGuardSet;

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
/// Manages the node lifecycle and coordinates partition ownership
/// when cluster membership changes.
pub struct DeltaManager {
    /// This node's ID.
    pub node_id: NodeId,
    /// Current lifecycle phase.
    pub phase: NodeLifecyclePhase,
    /// Partition guards for this node.
    pub guards: PartitionGuardSet,
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
            constraints: AssignmentConstraints::default(),
        }
    }

    /// Transition to the next lifecycle phase.
    ///
    /// Returns `true` if the transition was valid.
    pub fn transition(&mut self, next: NodeLifecyclePhase) -> bool {
        let valid = matches!(
            (self.phase, next),
            (NodeLifecyclePhase::Discovering, NodeLifecyclePhase::FormingRaft)
                | (NodeLifecyclePhase::FormingRaft, NodeLifecyclePhase::WaitingForAssignment)
                | (
                    NodeLifecyclePhase::WaitingForAssignment,
                    NodeLifecyclePhase::RestoringPartitions | NodeLifecyclePhase::Active
                )
                | (NodeLifecyclePhase::RestoringPartitions, NodeLifecyclePhase::Active)
                | (NodeLifecyclePhase::Active, NodeLifecyclePhase::Draining)
                // Allow direct shutdown from any phase (crash recovery)
                | (_, NodeLifecyclePhase::Shutdown)
        );

        if valid {
            self.phase = next;
        }
        valid
    }

    /// Process a membership event and determine if rebalancing is needed.
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
                matches!(new_state, NodeState::Active | NodeState::Left)
                    || matches!(old_state, NodeState::Active)
            }
        }
    }

    /// Apply the guard changes for a completed migration on this node.
    pub fn apply_migration_result(
        &mut self,
        partition_id: u32,
        from_node: NodeId,
        to_node: NodeId,
        new_epoch: u64,
    ) {
        if from_node == self.node_id {
            self.guards.update_epoch(partition_id, new_epoch);
            self.guards.remove(partition_id);
        }
        if to_node == self.node_id {
            self.guards.insert(partition_id, new_epoch);
        }
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
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::discovery::NodeMetadata;

    fn make_node(id: u64, cores: u32) -> crate::delta::discovery::NodeInfo {
        crate::delta::discovery::NodeInfo {
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
    }

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
        assert!(mgr.transition(NodeLifecyclePhase::Active));
    }

    #[test]
    fn test_invalid_transitions() {
        let mut mgr = DeltaManager::new(NodeId(1));
        assert!(!mgr.transition(NodeLifecyclePhase::Active));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Discovering);

        mgr.transition(NodeLifecyclePhase::FormingRaft);
        assert!(!mgr.transition(NodeLifecyclePhase::Discovering));
        assert_eq!(mgr.phase, NodeLifecyclePhase::FormingRaft);
    }

    #[test]
    fn test_should_rebalance_when_active() {
        let mut mgr = DeltaManager::new(NodeId(1));
        mgr.phase = NodeLifecyclePhase::Active;

        assert!(mgr.should_rebalance(&MembershipEvent::NodeJoined(Box::new(make_node(2, 4)))));
        assert!(mgr.should_rebalance(&MembershipEvent::NodeLeft(NodeId(2))));
    }

    #[test]
    fn test_should_not_rebalance_when_discovering() {
        let mgr = DeltaManager::new(NodeId(1));
        assert!(!mgr.should_rebalance(&MembershipEvent::NodeJoined(Box::new(make_node(2, 4)))));
    }

    #[test]
    fn test_apply_migration_result_source() {
        let mut mgr = DeltaManager::new(NodeId(1));
        mgr.guards.insert(5, 1);
        mgr.apply_migration_result(5, NodeId(1), NodeId(2), 2);
        assert!(mgr.guards.is_empty());
    }

    #[test]
    fn test_apply_migration_result_target() {
        let mut mgr = DeltaManager::new(NodeId(2));
        mgr.apply_migration_result(5, NodeId(1), NodeId(2), 2);
        assert_eq!(mgr.guards.len(), 1);
        assert!(mgr.guards.check(5).is_ok());
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
