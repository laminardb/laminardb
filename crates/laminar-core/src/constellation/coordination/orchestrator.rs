//! Constellation orchestrator — top-level lifecycle management.
//!
//! The `ConstellationManager` coordinates the full lifecycle of a node
//! in the constellation: discovery → Raft formation → partition assignment
//! → active processing → graceful drain → shutdown.

use std::fmt;

use crate::constellation::discovery::NodeId;

/// The current lifecycle phase of a node in the constellation.
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

/// Top-level constellation lifecycle orchestrator.
///
/// Placeholder — fully implemented in F-COORD-002.
pub struct ConstellationManager {
    /// This node's ID.
    pub node_id: NodeId,
    /// Current lifecycle phase.
    pub phase: NodeLifecyclePhase,
}

impl ConstellationManager {
    /// Create a new manager (placeholder — fully implemented in F-COORD-002).
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            phase: NodeLifecyclePhase::Discovering,
        }
    }
}

impl fmt::Debug for ConstellationManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConstellationManager")
            .field("node_id", &self.node_id)
            .field("phase", &self.phase)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lifecycle_phase_display() {
        assert_eq!(NodeLifecyclePhase::Active.to_string(), "active");
        assert_eq!(NodeLifecyclePhase::Draining.to_string(), "draining");
    }

    #[test]
    fn test_manager_initial_phase() {
        let mgr = ConstellationManager::new(NodeId(1));
        assert_eq!(mgr.phase, NodeLifecyclePhase::Discovering);
    }
}
