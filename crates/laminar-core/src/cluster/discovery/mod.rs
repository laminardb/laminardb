//! Peer discovery: `StaticDiscovery` (seed list) and `GossipDiscovery`
//! (chitchat).
#![allow(clippy::disallowed_types)] // cold path: discovery metadata (serde + rkyv)

mod static_discovery;
pub use static_discovery::{StaticDiscovery, StaticDiscoveryConfig};

mod gossip_discovery;
pub use gossip_discovery::{keys, GossipDiscovery, GossipDiscoveryConfig};

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

pub use crate::state::NodeId;

/// Publish the membership watch only on a real change. Unconditional `send` each
/// discovery tick starves consumers that debounce on `changed()` for a quiet period
/// (the rebalance controller, so a dead node's vnodes are never shed).
pub(crate) fn publish_if_changed(tx: &watch::Sender<Vec<NodeInfo>>, mut peer_list: Vec<NodeInfo>) {
    peer_list.sort_by_key(|n| n.id.0); // peer map order is arbitrary; equality must not depend on it
    tx.send_if_modified(|cur| {
        // Ignore `last_heartbeat_ms` — it ticks every refresh and would notify forever.
        fn same_member(a: &NodeInfo, b: &NodeInfo) -> bool {
            // Destructure so a new NodeInfo field forces an update here; compare
            // all fields except last_heartbeat_ms (it ticks every refresh).
            let NodeInfo {
                id,
                name,
                rpc_address,
                state,
                metadata,
                last_heartbeat_ms: _,
            } = a;
            id == &b.id
                && name == &b.name
                && rpc_address == &b.rpc_address
                && state == &b.state
                && metadata == &b.metadata
        }
        let same = cur.len() == peer_list.len()
            && cur.iter().zip(&peer_list).all(|(a, b)| same_member(a, b));
        if same {
            false
        } else {
            *cur = peer_list;
            true
        }
    });
}

/// Current lifecycle state of a node.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
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
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
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
            version: String::new(),
        }
    }
}

/// Full information about a discovered node.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct NodeInfo {
    /// The node's unique identifier.
    pub id: NodeId,
    /// Human-readable name.
    pub name: String,
    /// Address for gRPC communication.
    pub rpc_address: String,
    /// Current lifecycle state.
    pub state: NodeState,
    /// Hardware/deployment metadata.
    pub metadata: NodeMetadata,
    /// Timestamp of the last received heartbeat (millis since epoch).
    pub last_heartbeat_ms: i64,
}

/// Node ids eligible to own vnodes: only `Active` nodes. Joining,
/// Suspected, Draining, and Left are excluded so a draining or failing
/// node sheds its vnodes on the next rotation.
#[must_use]
pub fn assignable_node_ids(members: &[NodeInfo]) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = members
        .iter()
        .filter(|m| matches!(m.state, NodeState::Active))
        .map(|m| m.id)
        .filter(|id| !id.is_unassigned())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
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

/// Trait for node discovery in a cluster.
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
mod tests;
