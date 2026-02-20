//! Raft coordinator wrapper for delta metadata consensus.
//!
//! Wraps `openraft::Raft` to provide a higher-level API for reading
//! and writing delta metadata through the Raft log.

use std::fmt;

use crate::delta::discovery::NodeId;

/// Errors from coordination operations.
#[derive(Debug, thiserror::Error)]
pub enum CoordinationError {
    /// This node is not the Raft leader.
    #[error("not leader: leader is {leader_id:?} at {leader_address:?}")]
    NotLeader {
        /// The current leader's node ID, if known.
        leader_id: Option<NodeId>,
        /// The current leader's address, if known.
        leader_address: Option<String>,
    },

    /// The Raft log failed to apply.
    #[error("raft apply error: {0}")]
    RaftApply(String),

    /// The Raft read failed.
    #[error("raft read error: {0}")]
    RaftRead(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The coordinator is not initialized.
    #[error("coordinator not initialized")]
    NotInitialized,
}

/// Raft coordinator for delta metadata.
///
/// Wraps `openraft::Raft` and the `DeltaMetadata` state machine
/// to provide typed read/write operations.
pub struct RaftCoordinator {
    /// The local node ID.
    pub node_id: NodeId,
    // openraft::Raft instance and state machine will be added here
}

impl RaftCoordinator {
    /// Create a new Raft coordinator.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

impl fmt::Debug for RaftCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftCoordinator")
            .field("node_id", &self.node_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordination_error_display() {
        let err = CoordinationError::NotLeader {
            leader_id: Some(NodeId(1)),
            leader_address: Some("127.0.0.1:9001".into()),
        };
        assert!(err.to_string().contains("not leader"));
    }

    #[test]
    fn test_coordinator_new() {
        let coord = RaftCoordinator::new(NodeId(1));
        assert_eq!(coord.node_id, NodeId(1));
    }
}
