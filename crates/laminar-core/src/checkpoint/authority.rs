//! Feature-neutral authority values for durable checkpoint protocol records.

use uuid::Uuid;

/// Exact process incarnation that owns one leader term.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeaderProofOwner {
    /// Stable cluster node identity.
    pub node_id: u64,
    /// Boot-unique process identity.
    pub boot_id: Uuid,
    /// Durable process term for the stable node identity.
    pub process_term: u64,
}

impl LeaderProofOwner {
    /// Whether every identity dimension has a canonical production value.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.node_id != 0 && !self.boot_id.is_nil() && self.process_term != 0
    }
}

/// Authority captured for one exact leader term.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeaderProof {
    /// Exact process incarnation that owns the leader term.
    pub owner: LeaderProofOwner,
    /// Durable fencing token for that ownership term.
    pub fencing_token: u64,
}

impl LeaderProof {
    /// Whether the exact owner and fencing token have canonical production values.
    #[must_use]
    pub fn is_canonical(&self) -> bool {
        self.owner.is_canonical() && self.fencing_token != 0
    }
}
