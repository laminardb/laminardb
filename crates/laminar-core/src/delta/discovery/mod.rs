//! # Node Discovery
//!
//! Traits and implementations for discovering peer nodes in a LaminarDB
//! delta.
//!
//! ## Implementations
//!
//! - `StaticDiscovery`: Pre-configured seed list with TCP heartbeats
//! - `GossipDiscovery`: Chitchat-based gossip protocol

mod static_discovery;
pub use static_discovery::*;

mod gossip_discovery;
pub use gossip_discovery::*;

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// Unique identifier for a node in the delta.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize,
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Sentinel value representing "unassigned" (no owner).
    pub const UNASSIGNED: Self = Self(0);

    /// Returns `true` if this is the unassigned sentinel.
    #[must_use]
    pub const fn is_unassigned(&self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// Current lifecycle state of a node.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum NodeState {
    /// Node is joining the cluster but not yet fully active.
    Joining,
    /// Node is active and participating in the cluster.
    Active,
    /// Node is suspected of failure (missed heartbeats).
    Suspected,
    /// Node is gracefully draining before leaving.
    Draining,
    /// Node has left the cluster.
    Left,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Joining => write!(f, "joining"),
            Self::Active => write!(f, "active"),
            Self::Suspected => write!(f, "suspected"),
            Self::Draining => write!(f, "draining"),
            Self::Left => write!(f, "left"),
        }
    }
}

/// Hardware and deployment metadata for a node.
#[derive(
    Debug, Clone, Serialize, Deserialize,
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct NodeMetadata {
    /// Number of CPU cores available.
    pub cores: u32,
    /// Total memory in bytes.
    pub memory_bytes: u64,
    /// Failure domain (e.g., rack, zone, region).
    pub failure_domain: Option<String>,
    /// Arbitrary key-value tags.
    pub tags: HashMap<String, String>,
    /// Partitions currently owned by this node.
    pub owned_partitions: Vec<u32>,
    /// `LaminarDB` version string.
    pub version: String,
}

impl Default for NodeMetadata {
    fn default() -> Self {
        Self {
            cores: 1,
            memory_bytes: 0,
            failure_domain: None,
            tags: HashMap::new(),
            owned_partitions: Vec::new(),
            version: String::new(),
        }
    }
}

/// Full information about a discovered node.
#[derive(
    Debug, Clone, Serialize, Deserialize,
    rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct NodeInfo {
    /// The node's unique identifier.
    pub id: NodeId,
    /// Human-readable name.
    pub name: String,
    /// Address for gRPC communication.
    pub rpc_address: String,
    /// Address for Raft communication.
    pub raft_address: String,
    /// Current lifecycle state.
    pub state: NodeState,
    /// Hardware/deployment metadata.
    pub metadata: NodeMetadata,
    /// Timestamp of the last received heartbeat (millis since epoch).
    pub last_heartbeat_ms: i64,
}

/// A membership change event.
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    /// A new node has been discovered.
    NodeJoined(Box<NodeInfo>),
    /// A node's state has changed.
    NodeStateChanged {
        /// The node whose state changed.
        node_id: NodeId,
        /// Previous state.
        old_state: NodeState,
        /// New state.
        new_state: NodeState,
    },
    /// A node has left or been removed from the cluster.
    NodeLeft(NodeId),
}

/// Errors that can occur during discovery operations.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    /// Failed to bind to the specified address.
    #[error("bind error: {0}")]
    Bind(String),

    /// Failed to connect to a seed/peer node.
    #[error("connection error to {address}: {reason}")]
    Connection {
        /// The address that failed.
        address: String,
        /// Reason for failure.
        reason: String,
    },

    /// Serialization/deserialization failure.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The discovery service is not running.
    #[error("discovery not started")]
    NotStarted,

    /// The discovery service has been shut down.
    #[error("discovery shut down")]
    ShutDown,

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Trait for node discovery in a delta.
///
/// Implementations provide the mechanism by which nodes find and
/// track each other. The trait is async and designed for long-running
/// background tasks.
#[allow(async_fn_in_trait)]
pub trait Discovery: Send + Sync + 'static {
    /// Start the discovery service.
    ///
    /// This spawns background tasks for heartbeating and failure detection.
    async fn start(&mut self) -> Result<(), DiscoveryError>;

    /// Get the current set of known peers (excluding self).
    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError>;

    /// Announce this node's updated information to the cluster.
    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError>;

    /// Subscribe to membership change events.
    ///
    /// Returns a watch receiver that is updated whenever the membership
    /// changes. The value is the list of all known peers.
    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>>;

    /// Gracefully stop the discovery service.
    async fn stop(&mut self) -> Result<(), DiscoveryError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_display() {
        assert_eq!(NodeId(42).to_string(), "node-42");
    }

    #[test]
    fn test_node_id_unassigned() {
        assert!(NodeId::UNASSIGNED.is_unassigned());
        assert!(!NodeId(1).is_unassigned());
    }

    #[test]
    fn test_node_state_display() {
        assert_eq!(NodeState::Active.to_string(), "active");
        assert_eq!(NodeState::Suspected.to_string(), "suspected");
        assert_eq!(NodeState::Draining.to_string(), "draining");
    }

    #[test]
    fn test_node_metadata_default() {
        let meta = NodeMetadata::default();
        assert_eq!(meta.cores, 1);
        assert_eq!(meta.memory_bytes, 0);
        assert!(meta.failure_domain.is_none());
        assert!(meta.tags.is_empty());
        assert!(meta.owned_partitions.is_empty());
    }

    #[test]
    fn test_node_id_serialization() {
        let id = NodeId(123);
        let json = serde_json::to_string(&id).unwrap();
        let back: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn test_node_info_serialization() {
        let info = NodeInfo {
            id: NodeId(1),
            name: "test-node".into(),
            rpc_address: "127.0.0.1:9000".into(),
            raft_address: "127.0.0.1:9001".into(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 1000,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: NodeInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, info.id);
        assert_eq!(back.name, "test-node");
    }
}
