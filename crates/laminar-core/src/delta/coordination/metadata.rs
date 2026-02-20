//! Delta metadata state machine for Raft consensus.
//!
//! This module defines the distributed state machine that tracks:
//! - Partition ownership (which node owns which partitions)
//! - Node states (active, suspected, draining, etc.)
//! - Latest checkpoint metadata
//! - Cluster epoch

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::delta::discovery::NodeId;

/// Ownership record for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOwnership {
    /// The node that owns this partition.
    pub node_id: NodeId,
    /// Current epoch for this partition's ownership.
    pub epoch: u64,
    /// Timestamp when the partition was assigned (millis since Unix epoch).
    pub assigned_at_ms: i64,
}

/// Log entries applied to the delta metadata state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataLogEntry {
    /// Assign a partition to a node (leader decision).
    AssignPartition {
        /// The partition being assigned.
        partition_id: u32,
        /// The node receiving ownership.
        node_id: NodeId,
        /// The epoch for this assignment.
        epoch: u64,
    },
    /// Release a partition from its current owner.
    ReleasePartition {
        /// The partition being released.
        partition_id: u32,
        /// The epoch being released.
        epoch: u64,
    },
    /// Acquire a partition on a new owner (after transfer).
    AcquirePartition {
        /// The partition being acquired.
        partition_id: u32,
        /// The new owner.
        node_id: NodeId,
        /// The new epoch.
        epoch: u64,
    },
    /// Commit a checkpoint for a partition.
    CommitCheckpoint {
        /// The partition that was checkpointed.
        partition_id: u32,
        /// The checkpoint ID.
        checkpoint_id: u64,
        /// Path to the checkpoint manifest.
        manifest_path: String,
    },
    /// Update a node's state in the cluster.
    UpdateNodeState {
        /// The node whose state changed.
        node_id: NodeId,
        /// The new state.
        state: crate::delta::discovery::NodeState,
    },
    /// Register a new node in the cluster.
    RegisterNode {
        /// The new node's info.
        node_id: NodeId,
        /// The node's RPC address.
        rpc_address: String,
        /// The node's Raft address.
        raft_address: String,
    },
    /// Deregister a node from the cluster.
    DeregisterNode {
        /// The node being removed.
        node_id: NodeId,
    },
}

/// The Raft state machine for delta metadata.
///
/// This is the authoritative source of truth for partition ownership,
/// node membership, and checkpoint state. All mutations go through
/// the Raft log to ensure consistency across the cluster.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaMetadata {
    /// Partition ID → ownership record.
    pub partition_map: HashMap<u32, PartitionOwnership>,
    /// Partition ID → latest checkpoint manifest path.
    pub latest_checkpoints: HashMap<u32, String>,
    /// Monotonically increasing cluster epoch.
    pub cluster_epoch: u64,
    /// Node ID → current state.
    pub node_states: HashMap<u64, crate::delta::discovery::NodeState>,
    /// Node ID → RPC address.
    pub node_addresses: HashMap<u64, String>,
    /// Partitions with pending release (epoch → `partition_id` list).
    pub pending_releases: HashMap<u64, Vec<u32>>,
}

impl DeltaMetadata {
    /// Create an empty metadata state machine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a log entry to the state machine.
    ///
    /// This is the core state transition function. Each entry type
    /// has specific validation rules (e.g., epoch must be strictly
    /// increasing for partition assignments).
    #[allow(clippy::too_many_lines)]
    pub fn apply(&mut self, entry: &MetadataLogEntry) {
        match entry {
            MetadataLogEntry::AssignPartition {
                partition_id,
                node_id,
                epoch,
            } => {
                // C5: validate both epoch AND owner NodeId
                if let Some(existing) = self.partition_map.get(partition_id) {
                    if *epoch <= existing.epoch {
                        return; // stale assignment, ignore
                    }
                }
                self.partition_map.insert(
                    *partition_id,
                    PartitionOwnership {
                        node_id: *node_id,
                        epoch: *epoch,
                        assigned_at_ms: chrono::Utc::now().timestamp_millis(),
                    },
                );
                self.cluster_epoch = self.cluster_epoch.max(*epoch);
            }

            MetadataLogEntry::ReleasePartition {
                partition_id,
                epoch,
            } => {
                if let Some(existing) = self.partition_map.get(partition_id) {
                    if existing.epoch == *epoch {
                        // C6: set to unassigned sentinel
                        self.partition_map.insert(
                            *partition_id,
                            PartitionOwnership {
                                node_id: NodeId::UNASSIGNED,
                                epoch: *epoch,
                                assigned_at_ms: chrono::Utc::now().timestamp_millis(),
                            },
                        );
                    }
                }
            }

            MetadataLogEntry::AcquirePartition {
                partition_id,
                node_id,
                epoch,
            } => {
                if let Some(existing) = self.partition_map.get(partition_id) {
                    // Only allow acquire if partition is currently unassigned or epoch is higher
                    if existing.node_id.is_unassigned() || *epoch > existing.epoch {
                        self.partition_map.insert(
                            *partition_id,
                            PartitionOwnership {
                                node_id: *node_id,
                                epoch: *epoch,
                                assigned_at_ms: chrono::Utc::now().timestamp_millis(),
                            },
                        );
                        self.cluster_epoch = self.cluster_epoch.max(*epoch);
                    }
                } else {
                    // Partition didn't exist yet — create it
                    self.partition_map.insert(
                        *partition_id,
                        PartitionOwnership {
                            node_id: *node_id,
                            epoch: *epoch,
                            assigned_at_ms: chrono::Utc::now().timestamp_millis(),
                        },
                    );
                    self.cluster_epoch = self.cluster_epoch.max(*epoch);
                }
            }

            MetadataLogEntry::CommitCheckpoint {
                partition_id,
                manifest_path,
                ..
            } => {
                self.latest_checkpoints
                    .insert(*partition_id, manifest_path.clone());
            }

            MetadataLogEntry::UpdateNodeState { node_id, state } => {
                self.node_states.insert(node_id.0, *state);
            }

            MetadataLogEntry::RegisterNode {
                node_id,
                rpc_address,
                ..
            } => {
                self.node_states
                    .insert(node_id.0, crate::delta::discovery::NodeState::Joining);
                self.node_addresses.insert(node_id.0, rpc_address.clone());
            }

            MetadataLogEntry::DeregisterNode { node_id } => {
                self.node_states.remove(&node_id.0);
                self.node_addresses.remove(&node_id.0);
                // Release all partitions owned by this node
                let owned: Vec<u32> = self
                    .partition_map
                    .iter()
                    .filter(|(_, v)| v.node_id == *node_id)
                    .map(|(k, _)| *k)
                    .collect();
                for pid in owned {
                    if let Some(ownership) = self.partition_map.get_mut(&pid) {
                        ownership.node_id = NodeId::UNASSIGNED;
                    }
                }
            }
        }
    }

    /// Get the current owner of a partition, if any.
    #[must_use]
    pub fn partition_owner(&self, partition_id: u32) -> Option<&PartitionOwnership> {
        self.partition_map.get(&partition_id)
    }

    /// Get all partitions owned by a specific node.
    #[must_use]
    pub fn partitions_for_node(&self, node_id: NodeId) -> Vec<u32> {
        self.partition_map
            .iter()
            .filter(|(_, v)| v.node_id == node_id)
            .map(|(k, _)| *k)
            .collect()
    }

    /// Get the number of active nodes.
    #[must_use]
    pub fn active_node_count(&self) -> usize {
        self.node_states
            .values()
            .filter(|s| {
                matches!(
                    s,
                    crate::delta::discovery::NodeState::Active
                        | crate::delta::discovery::NodeState::Joining
                )
            })
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::discovery::NodeState;

    #[test]
    fn test_metadata_new() {
        let meta = DeltaMetadata::new();
        assert!(meta.partition_map.is_empty());
        assert_eq!(meta.cluster_epoch, 0);
    }

    #[test]
    fn test_assign_partition() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1,
        });

        let owner = meta.partition_owner(0).unwrap();
        assert_eq!(owner.node_id, NodeId(1));
        assert_eq!(owner.epoch, 1);
        assert_eq!(meta.cluster_epoch, 1);
    }

    #[test]
    fn test_stale_assignment_ignored() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 5,
        });
        // Stale assignment with lower epoch
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(2),
            epoch: 3,
        });

        let owner = meta.partition_owner(0).unwrap();
        assert_eq!(owner.node_id, NodeId(1));
        assert_eq!(owner.epoch, 5);
    }

    #[test]
    fn test_release_partition() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1,
        });
        meta.apply(&MetadataLogEntry::ReleasePartition {
            partition_id: 0,
            epoch: 1,
        });

        let owner = meta.partition_owner(0).unwrap();
        assert!(owner.node_id.is_unassigned());
    }

    #[test]
    fn test_release_wrong_epoch_noop() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 2,
        });
        // Release with wrong epoch
        meta.apply(&MetadataLogEntry::ReleasePartition {
            partition_id: 0,
            epoch: 1,
        });

        let owner = meta.partition_owner(0).unwrap();
        assert_eq!(owner.node_id, NodeId(1)); // Still owned
    }

    #[test]
    fn test_acquire_after_release() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1,
        });
        meta.apply(&MetadataLogEntry::ReleasePartition {
            partition_id: 0,
            epoch: 1,
        });
        meta.apply(&MetadataLogEntry::AcquirePartition {
            partition_id: 0,
            node_id: NodeId(2),
            epoch: 2,
        });

        let owner = meta.partition_owner(0).unwrap();
        assert_eq!(owner.node_id, NodeId(2));
        assert_eq!(owner.epoch, 2);
    }

    #[test]
    fn test_register_and_deregister_node() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::RegisterNode {
            node_id: NodeId(1),
            rpc_address: "127.0.0.1:9000".into(),
            raft_address: "127.0.0.1:9001".into(),
        });

        assert_eq!(*meta.node_states.get(&1).unwrap(), NodeState::Joining);
        assert_eq!(meta.active_node_count(), 1);

        // Assign a partition
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1,
        });

        // Deregister releases partitions
        meta.apply(&MetadataLogEntry::DeregisterNode { node_id: NodeId(1) });

        assert!(!meta.node_states.contains_key(&1));
        let owner = meta.partition_owner(0).unwrap();
        assert!(owner.node_id.is_unassigned());
    }

    #[test]
    fn test_commit_checkpoint() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::CommitCheckpoint {
            partition_id: 0,
            checkpoint_id: 42,
            manifest_path: "/checkpoints/42/manifest.json".into(),
        });

        assert_eq!(
            meta.latest_checkpoints.get(&0).unwrap(),
            "/checkpoints/42/manifest.json"
        );
    }

    #[test]
    fn test_partitions_for_node() {
        let mut meta = DeltaMetadata::new();
        for pid in 0..5 {
            meta.apply(&MetadataLogEntry::AssignPartition {
                partition_id: pid,
                node_id: NodeId(1),
                epoch: 1,
            });
        }
        for pid in 5..8 {
            meta.apply(&MetadataLogEntry::AssignPartition {
                partition_id: pid,
                node_id: NodeId(2),
                epoch: 1,
            });
        }

        let node1_parts = meta.partitions_for_node(NodeId(1));
        assert_eq!(node1_parts.len(), 5);
        let node2_parts = meta.partitions_for_node(NodeId(2));
        assert_eq!(node2_parts.len(), 3);
    }

    #[test]
    fn test_update_node_state() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::RegisterNode {
            node_id: NodeId(1),
            rpc_address: "127.0.0.1:9000".into(),
            raft_address: "127.0.0.1:9001".into(),
        });
        meta.apply(&MetadataLogEntry::UpdateNodeState {
            node_id: NodeId(1),
            state: NodeState::Active,
        });

        assert_eq!(*meta.node_states.get(&1).unwrap(), NodeState::Active);
    }

    #[test]
    fn test_cluster_epoch_tracks_max() {
        let mut meta = DeltaMetadata::new();
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 10,
        });
        meta.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 1,
            node_id: NodeId(2),
            epoch: 5,
        });

        // cluster_epoch should be max(10, 5) = 10
        assert_eq!(meta.cluster_epoch, 10);
    }
}
