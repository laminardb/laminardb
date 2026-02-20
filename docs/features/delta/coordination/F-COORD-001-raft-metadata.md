# F-COORD-001: DeltaMetadata & Raft Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-COORD-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | XL (2-3 weeks) |
| **Dependencies** | F-DISC-001 (Discovery Trait) |
| **Blocks** | F-EPOCH-001 (PartitionGuard), F-EPOCH-002 (Assignment Algorithm), F-COORD-002 (Delta Orchestration) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/delta/coordination/raft_coordinator.rs`, `laminar-core/src/delta/coordination/metadata.rs` |

## Summary

`DeltaMetadata` is the authoritative, strongly-consistent state for the entire LaminarDB delta. It stores the partition map (which node owns which partition at which epoch), the latest checkpoint for each partition, the cluster epoch, and node states. This metadata is replicated across a Raft group of 3-5 nodes using the `openraft` crate, providing linearizable reads and writes. **Dependency Note**: Pin `openraft` to a specific version (e.g., `0.10.x`) in `Cargo.toml`. The openraft API has had significant changes between versions. The API patterns used in this spec (e.g., `ensure_linearizable()`, `client_write()`, `SnapshotPolicy::LogsSinceLast`) should be verified against the pinned version. The Raft group operates in Ring 2 (Control Plane) with millisecond-level latency and is used exclusively for metadata operations -- never for hot-path data.

The Raft state machine applies log entries that represent metadata mutations: partition assignment, partition release, checkpoint commits, and node state updates. Snapshots are taken periodically to bound log growth. Linearizable reads are achieved via the read-index protocol, ensuring that queries always return the most recently committed value.

## Goals

- Define `DeltaMetadata` as the canonical cluster metadata structure
- Implement a Raft state machine using `openraft` 0.10+ that replicates metadata
- Define typed log entries for all metadata mutations (assign, release, checkpoint, node state)
- Support linearizable reads via read-index protocol
- Implement Raft snapshots for bounded log storage
- Support 3-5 node Raft groups for fault tolerance
- Provide a `MetadataClient` for reading/writing metadata from any node
- Leader election with automatic failover

## Non-Goals

- Using Raft for data replication (Raft is metadata-only)
- Custom Raft implementation (we use openraft)
- Multi-Raft (single Raft group for all metadata)
- External Raft storage (etcd, Consul, ZooKeeper)
- Hot-path operations through Raft (control plane only)

## Technical Design

### Architecture

The Raft group runs as a background service on 3-5 nodes in the delta. Any node can read metadata, but only the Raft leader can accept writes. Reads are linearizable via the read-index protocol, which ensures the reader's local state is at least as up-to-date as the latest committed entry.

```
+-------------------------------------------------------------+
|  Ring 2: Control Plane                                       |
|                                                              |
|  +------------------+  +------------------+  +------------+  |
|  | Node A (Leader)  |  | Node B (Follower)|  | Node C     |  |
|  |                  |  |                  |  | (Follower) |  |
|  | Raft Log:        |  | Raft Log:        |  | Raft Log:  |  |
|  | [e1,e2,e3,e4]    |  | [e1,e2,e3,e4]    |  | [e1,e2,e3] |  |
|  |                  |  |                  |  |            |  |
|  | State Machine:   |  | State Machine:   |  | State:     |  |
|  | Delta    |  | Delta    |  | Constella  |  |
|  | Metadata         |  | Metadata         |  | tion Meta  |  |
|  +--------+---------+  +--------+---------+  +-----+------+  |
|           |                     |                    |        |
|           +--------Raft Consensus (AppendEntries)----+        |
|                                                              |
+-------------------------------------------------------------+
        |
        | MetadataClient (read/write)
        v
+-------------------------------------------------------------+
|  DeltaManager / PartitionAssigner / MigrationExecutor|
+-------------------------------------------------------------+
```

### API/Interface

```rust
use openraft::{Config as RaftConfig, Raft, Entry, EntryPayload, RaftTypeConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// NOTE: Uses i64 millisecond timestamps instead of chrono::DateTime
// for consistency with the rest of the LaminarDB codebase, which uses
// i64 timestamps throughout (Watermark, Event.timestamp, etc.).
// Helper: fn current_time_ms() -> i64 { SystemTime::now()... }

/// The authoritative metadata for the delta.
///
/// This struct is the Raft state machine's state. It is updated by applying
/// log entries and can be snapshotted/restored for Raft persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMetadata {
    /// Partition map: partition_id -> (owner_node, epoch).
    /// This is the source of truth for who owns which partition.
    pub partition_map: HashMap<PartitionId, PartitionOwnership>,

    /// Latest checkpoint for each partition.
    pub latest_checkpoints: HashMap<PartitionId, CheckpointRef>,

    /// Cluster epoch. Incremented on significant topology changes.
    pub cluster_epoch: u64,

    /// Node states: node_id -> current state.
    pub node_states: HashMap<NodeId, NodeRegistration>,

    /// Release records for in-progress migrations.
    /// Cleared after the new owner acquires the partition.
    pub pending_releases: HashMap<PartitionId, ReleaseInfo>,

    /// Last applied Raft log index (for snapshot metadata).
    pub last_applied_log: u64,
}

/// Ownership record for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionOwnership {
    /// The node that owns this partition.
    pub node_id: NodeId,

    /// The epoch at which ownership was granted.
    pub epoch: u64,

    /// When ownership was assigned (milliseconds since Unix epoch).
    pub assigned_at_ms: i64,
}

/// Reference to a checkpoint stored in shared storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRef {
    /// Unique checkpoint identifier.
    pub checkpoint_id: CheckpointId,

    /// The partition this checkpoint belongs to.
    pub partition_id: PartitionId,

    /// Epoch at which the checkpoint was created.
    pub epoch: u64,

    /// Storage location (object storage path or local path).
    pub storage_path: String,

    /// Source offsets for exactly-once replay.
    pub source_offsets: HashMap<String, HashMap<u32, u64>>,

    /// State size in bytes.
    pub size_bytes: u64,

    /// When the checkpoint was created (milliseconds since Unix epoch).
    pub created_at_ms: i64,
}

/// Node registration in the delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    /// Node identifier.
    pub node_id: NodeId,

    /// Current state.
    pub state: NodeState,

    /// RPC address for inter-node communication.
    pub rpc_address: String,

    /// Raft address.
    pub raft_address: String,

    /// Node capacity and metadata.
    pub metadata: NodeMetadata,

    /// When the node registered (milliseconds since Unix epoch).
    pub registered_at_ms: i64,

    /// When the state was last updated (milliseconds since Unix epoch).
    pub last_updated_ms: i64,
}

impl DeltaMetadata {
    /// Create empty metadata for a new delta.
    pub fn new() -> Self {
        Self {
            partition_map: HashMap::new(),
            latest_checkpoints: HashMap::new(),
            cluster_epoch: 0,
            node_states: HashMap::new(),
            pending_releases: HashMap::new(),
            last_applied_log: 0,
        }
    }

    /// Get the current epoch for a partition.
    pub fn partition_epoch(&self, partition_id: PartitionId) -> Option<u64> {
        self.partition_map.get(&partition_id).map(|o| o.epoch)
    }

    /// Get full ownership info for a partition (epoch + node_id).
    ///
    /// AUDIT FIX (C5): Added to support owner node_id validation in
    /// `PartitionGuard::validate()`, which previously only checked epoch.
    pub fn partition_ownership(&self, partition_id: PartitionId) -> Option<&PartitionOwnership> {
        self.partition_map.get(&partition_id)
    }

    /// Get the owner of a partition.
    pub fn partition_owner(&self, partition_id: PartitionId) -> Option<NodeId> {
        self.partition_map.get(&partition_id).map(|o| o.node_id)
    }

    /// Get all partitions owned by a node.
    pub fn partitions_for_node(&self, node_id: NodeId) -> Vec<PartitionId> {
        self.partition_map
            .iter()
            .filter(|(_, o)| o.node_id == node_id)
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// Get release info for a partition at a specific epoch.
    pub fn get_release_info(
        &self,
        partition_id: PartitionId,
        epoch: u64,
    ) -> Option<&ReleaseInfo> {
        self.pending_releases.get(&partition_id)
            .filter(|r| r.epoch == epoch)
    }

    /// Apply a metadata log entry to the state machine.
    pub fn apply(&mut self, entry: &MetadataLogEntry) {
        match entry {
            MetadataLogEntry::AssignPartition {
                partition_id, node_id, epoch,
            } => {
                self.partition_map.insert(*partition_id, PartitionOwnership {
                    node_id: *node_id,
                    epoch: *epoch,
                    assigned_at_ms: current_time_ms(),
                });
            }

            MetadataLogEntry::ReleasePartition {
                partition_id, epoch, checkpoint_id, source_offsets,
            } => {
                self.pending_releases.insert(*partition_id, ReleaseInfo {
                    partition_id: *partition_id,
                    epoch: *epoch,
                    checkpoint_id: *checkpoint_id,
                    source_offsets: source_offsets.clone(),
                    released_at_ms: current_time_ms(),
                });
            }

            MetadataLogEntry::AcquirePartition {
                partition_id, node_id, epoch,
            } => {
                self.partition_map.insert(*partition_id, PartitionOwnership {
                    node_id: *node_id,
                    epoch: *epoch,
                    assigned_at_ms: current_time_ms(),
                });
                // Clear pending release
                self.pending_releases.remove(partition_id);
            }

            MetadataLogEntry::CommitCheckpoint {
                partition_id, checkpoint,
            } => {
                self.latest_checkpoints.insert(*partition_id, checkpoint.clone());
            }

            MetadataLogEntry::UpdateNodeState {
                node_id, state, metadata,
            } => {
                if let Some(reg) = self.node_states.get_mut(node_id) {
                    reg.state = *state;
                    if let Some(meta) = metadata {
                        reg.metadata = meta.clone();
                    }
                    reg.last_updated_ms = current_time_ms();
                }
            }

            MetadataLogEntry::RegisterNode { registration } => {
                self.node_states.insert(registration.node_id, registration.clone());
            }

            MetadataLogEntry::DeregisterNode { node_id } => {
                self.node_states.remove(node_id);

                // AUDIT FIX (C6): Bump epoch for each partition owned by this node
                // and mark as unassigned (NodeId(0) sentinel). The previous
                // implementation used partition_map.retain() which silently
                // removed partitions without bumping their epoch — other nodes
                // holding PartitionGuards would not detect the removal.
                //
                // With this fix:
                // 1. Partitions remain in the map (epoch queries still work)
                // 2. Epoch is incremented (stale guards fail check())
                // 3. NodeId(0) signals "unassigned" to the PartitionAssigner
                // 4. Cluster epoch is incremented to trigger rebalance
                let owned: Vec<PartitionId> = self.partition_map.iter()
                    .filter(|(_, o)| o.node_id == *node_id)
                    .map(|(pid, _)| *pid)
                    .collect();

                for pid in &owned {
                    if let Some(ownership) = self.partition_map.get_mut(pid) {
                        ownership.epoch += 1;
                        ownership.node_id = NodeId(0); // Unassigned sentinel
                        ownership.assigned_at_ms = current_time_ms();
                    }
                }

                // Signal topology change so PartitionAssigner picks up
                // unassigned partitions
                self.cluster_epoch += 1;
            }

            MetadataLogEntry::IncrementClusterEpoch => {
                self.cluster_epoch += 1;
            }
        }
    }
}
```

### Data Structures

```rust
/// Log entries for the Raft state machine.
///
/// Each variant represents a metadata mutation that is replicated
/// across the Raft group. Entries are serialized to bincode for
/// compact storage in the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataLogEntry {
    /// Assign a partition to a node at a given epoch.
    /// Used during initial assignment and rebalance.
    AssignPartition {
        partition_id: PartitionId,
        node_id: NodeId,
        epoch: u64,
    },

    /// Record that a node has released a partition.
    /// The checkpoint ID and source offsets are stored so the new owner
    /// can download and restore.
    ReleasePartition {
        partition_id: PartitionId,
        epoch: u64,
        checkpoint_id: CheckpointId,
        source_offsets: HashMap<String, HashMap<u32, u64>>,
    },

    /// Record that a node has acquired a partition at a new epoch.
    /// Clears the pending release record.
    AcquirePartition {
        partition_id: PartitionId,
        node_id: NodeId,
        epoch: u64,
    },

    /// Commit a checkpoint for a partition.
    /// Updates the latest checkpoint reference.
    CommitCheckpoint {
        partition_id: PartitionId,
        checkpoint: CheckpointRef,
    },

    /// Update a node's state (Active, Draining, etc.) and optionally metadata.
    UpdateNodeState {
        node_id: NodeId,
        state: NodeState,
        metadata: Option<NodeMetadata>,
    },

    /// Register a new node in the delta.
    RegisterNode {
        registration: NodeRegistration,
    },

    /// Remove a node from the delta.
    DeregisterNode {
        node_id: NodeId,
    },

    /// Increment the cluster epoch (on significant topology changes).
    IncrementClusterEpoch,
}

/// Snapshot format for the Raft state machine.
///
/// The snapshot contains the full `DeltaMetadata` serialized
/// as bincode. Snapshots are taken periodically to bound Raft log growth.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    /// The metadata at the time of snapshot.
    pub metadata: DeltaMetadata,

    /// The Raft log index at which this snapshot was taken.
    pub last_applied_log: u64,

    /// Snapshot creation timestamp (milliseconds since Unix epoch).
    pub created_at_ms: i64,
}

/// openraft type configuration for LaminarDB.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct LaminarRaftTypeConfig;

impl RaftTypeConfig for LaminarRaftTypeConfig {
    type D = MetadataLogEntry;           // Application data in log entries
    type R = MetadataResponse;           // Response to client proposals
    type NodeId = u64;                   // Node identifier type
    type Node = RaftNode;                // Node info for Raft communication
    type Entry = Entry<Self>;            // Log entry type
    type SnapshotData = tokio::io::Cursor<Vec<u8>>; // Snapshot data stream
    type AsyncRuntime = openraft::TokioRuntime;
}

/// Response from applying a metadata log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Optional message.
    pub message: Option<String>,
}

/// Raft node information for network communication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNode {
    /// Raft RPC address.
    pub address: String,
}

impl std::fmt::Display for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.address)
    }
}

/// Configuration for the Raft coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftCoordinatorConfig {
    /// This node's Raft ID (must be unique in the Raft group).
    pub node_id: u64,

    /// This node's Raft address (for AppendEntries RPC).
    pub raft_address: String,

    /// Initial Raft group members: node_id -> address.
    /// Used to bootstrap the Raft group on first start.
    pub initial_members: HashMap<u64, String>,

    /// Raft election timeout range (min, max) in milliseconds.
    /// Default: (150, 300).
    pub election_timeout: (u64, u64),

    /// Raft heartbeat interval in milliseconds.
    /// Default: 50.
    pub heartbeat_interval: u64,

    /// Maximum entries per AppendEntries RPC.
    /// Default: 100.
    pub max_payload_entries: u64,

    /// Snapshot policy: take snapshot every N log entries.
    /// Default: 10000.
    pub snapshot_threshold: u64,

    /// Raft log storage directory.
    pub data_dir: String,
}

impl Default for RaftCoordinatorConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            raft_address: "127.0.0.1:9200".to_string(),
            initial_members: HashMap::new(),
            election_timeout: (150, 300),
            heartbeat_interval: 50,
            max_payload_entries: 100,
            snapshot_threshold: 10000,
            data_dir: "/var/lib/laminar/raft".to_string(),
        }
    }
}
```

```rust
/// The Raft coordinator manages the Raft group and provides a client
/// interface for reading and writing metadata.
pub struct RaftCoordinator {
    /// The openraft Raft instance.
    raft: Raft<LaminarRaftTypeConfig>,

    /// Local copy of the state machine (for reads).
    state_machine: Arc<RwLock<DeltaMetadata>>,

    /// Configuration.
    config: RaftCoordinatorConfig,
}

impl RaftCoordinator {
    /// Create and initialize the Raft coordinator.
    pub async fn new(config: RaftCoordinatorConfig) -> Result<Self, CoordinationError> {
        let raft_config = RaftConfig {
            election_timeout_min: config.election_timeout.0,
            election_timeout_max: config.election_timeout.1,
            heartbeat_interval: config.heartbeat_interval,
            max_payload_entries: config.max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                config.snapshot_threshold,
            ),
            ..Default::default()
        };

        // Initialize storage, network, and state machine
        // (implementation depends on chosen storage backend)
        todo!("initialize openraft with storage and network")
    }

    /// Write a metadata entry via Raft consensus.
    ///
    /// Only succeeds on the leader. Followers forward to the leader
    /// or return an error with the leader's address.
    pub async fn write(
        &self,
        entry: MetadataLogEntry,
    ) -> Result<MetadataResponse, CoordinationError> {
        self.raft
            .client_write(entry)
            .await
            .map(|resp| resp.data)
            .map_err(|e| CoordinationError::RaftError(e.to_string()))
    }

    /// Read metadata with linearizable consistency.
    ///
    /// Uses the read-index protocol: asks the leader to confirm it is
    /// still the leader (via a round of heartbeats), then reads from
    /// the local state machine. Guarantees the read reflects all
    /// committed writes up to this point.
    pub async fn read_linearizable<F, T>(
        &self,
        f: F,
    ) -> Result<T, CoordinationError>
    where
        F: FnOnce(&DeltaMetadata) -> T,
    {
        // Ensure linearizability via read-index
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| CoordinationError::RaftError(e.to_string()))?;

        let state = self.state_machine.read().await;
        Ok(f(&state))
    }

    /// Read metadata from local state (may be stale on followers).
    ///
    /// Faster than linearizable reads but may return slightly outdated data.
    /// Acceptable for metrics, monitoring, and non-critical reads.
    pub async fn read_local<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&DeltaMetadata) -> T,
    {
        let state = self.state_machine.read().await;
        f(&state)
    }

    /// Get the current Raft leader.
    pub async fn current_leader(&self) -> Option<u64> {
        self.raft.current_leader().await
    }

    /// Check if this node is the Raft leader.
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.config.node_id)
    }

    /// Get Raft metrics for observability.
    pub fn metrics(&self) -> watch::Receiver<openraft::RaftMetrics<LaminarRaftTypeConfig>> {
        self.raft.metrics()
    }

    // -- Convenience methods for common operations --

    /// Commit a partition assignment.
    pub async fn commit_partition_assignment(
        &self,
        partition_id: PartitionId,
        node_id: NodeId,
        epoch: u64,
    ) -> Result<(), CoordinationError> {
        self.write(MetadataLogEntry::AssignPartition {
            partition_id,
            node_id,
            epoch,
        }).await?;
        Ok(())
    }

    /// Commit a partition release.
    pub async fn commit_partition_release(
        &self,
        partition_id: PartitionId,
        epoch: u64,
        checkpoint_id: CheckpointId,
        source_offsets: HashMap<String, HashMap<u32, u64>>,
    ) -> Result<(), CoordinationError> {
        self.write(MetadataLogEntry::ReleasePartition {
            partition_id,
            epoch,
            checkpoint_id,
            source_offsets,
        }).await?;
        Ok(())
    }

    /// Commit a partition acquisition.
    pub async fn commit_partition_acquisition(
        &self,
        partition_id: PartitionId,
        node_id: NodeId,
        epoch: u64,
    ) -> Result<(), CoordinationError> {
        self.write(MetadataLogEntry::AcquirePartition {
            partition_id,
            node_id,
            epoch,
        }).await?;
        Ok(())
    }

    /// Register a new node.
    pub async fn register_node(
        &self,
        registration: NodeRegistration,
    ) -> Result<(), CoordinationError> {
        self.write(MetadataLogEntry::RegisterNode { registration }).await?;
        Ok(())
    }
}

/// Errors from the Raft coordinator.
#[derive(Debug, Clone)]
pub enum CoordinationError {
    /// Raft operation failed.
    RaftError(String),

    /// This node is not the leader; redirect to the leader.
    NotLeader {
        leader_id: Option<u64>,
        leader_address: Option<String>,
    },

    /// Raft group not yet initialized.
    NotInitialized,

    /// Snapshot operation failed.
    SnapshotError(String),

    /// Network error communicating with Raft peers.
    NetworkError(String),
}

impl fmt::Display for CoordinationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoordinationError::RaftError(s) => write!(f, "raft error: {}", s),
            CoordinationError::NotLeader { leader_id, .. } => {
                write!(f, "not leader; leader is {:?}", leader_id)
            }
            CoordinationError::NotInitialized => write!(f, "raft not initialized"),
            CoordinationError::SnapshotError(s) => write!(f, "snapshot error: {}", s),
            CoordinationError::NetworkError(s) => write!(f, "network error: {}", s),
        }
    }
}

impl std::error::Error for CoordinationError {}
```

### Algorithm/Flow

1. **Raft Group Formation**: On delta startup, the initial set of nodes (from config or discovery) forms a Raft group. The first node bootstraps as a single-node cluster, then adds other nodes via `add_learner` and `change_membership`.

2. **Leader Election**: openraft handles leader election automatically. Election timeout is randomized between 150-300ms. The leader sends heartbeats every 50ms.

3. **Write Path**: A metadata mutation (e.g., assign partition) is submitted as a `client_write` on the leader. openraft replicates the entry to a majority of nodes, then commits and applies it to the state machine.

4. **Read Path (Linearizable)**: The client calls `ensure_linearizable()`, which verifies the current leader is still valid by completing a round of heartbeats. Then the local state machine is read. This guarantees the read reflects all committed writes.

5. **Read Path (Local)**: For non-critical reads (metrics, monitoring), the local state machine is read directly without linearizability checks. This is faster but may be stale on followers.

6. **Snapshots**: Every `snapshot_threshold` log entries, the state machine is serialized to a snapshot. The snapshot replaces all prior log entries, bounding log storage. New nodes joining the Raft group receive the snapshot first, then replay recent entries.

7. **Network Layer**: Raft RPCs (AppendEntries, RequestVote, InstallSnapshot) are sent over gRPC or a custom TCP protocol between Raft addresses.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `CoordinationError::RaftError` | Log replication failed, proposal rejected | Retry; check Raft health |
| `CoordinationError::NotLeader` | Write submitted to a follower | Redirect to leader (address included in error) |
| `CoordinationError::NotInitialized` | Raft group not yet formed | Wait for initialization to complete |
| `CoordinationError::SnapshotError` | Snapshot creation or restore failed | Rebuild from peers; alert operator |
| `CoordinationError::NetworkError` | Cannot reach Raft peers | Check network; Raft will retry automatically |
| Leader unavailable | Leader crashed; election in progress | Writes blocked until new leader elected (< 300ms) |
| Split brain | Network partition isolates minority | Minority cannot commit (no quorum); majority continues |

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Raft consensus round (write) | < 10ms | Majority acknowledgment on LAN |
| Linearizable read | < 5ms | Read-index protocol |
| Local read | < 100us | Direct state machine read |
| Leader election | < 300ms | From leader failure to new leader |
| Snapshot creation (1000 partitions) | < 100ms | Serialize DeltaMetadata |
| Snapshot size (1000 partitions) | < 1 MB | Bincode serialization |
| Log entry size | < 1 KB | Typical MetadataLogEntry |
| Raft group memory | < 50 MB | Log buffer + state machine |

## Test Plan

### Unit Tests

- [ ] `test_metadata_new_empty` - New metadata has no partitions or nodes
- [ ] `test_metadata_apply_assign_partition` - Assignment creates ownership entry
- [ ] `test_metadata_apply_release_partition` - Release creates pending release
- [ ] `test_metadata_apply_acquire_partition` - Acquire clears pending release, updates ownership
- [ ] `test_metadata_apply_commit_checkpoint` - Checkpoint updates latest
- [ ] `test_metadata_apply_register_node` - Node appears in node_states
- [ ] `test_metadata_apply_deregister_node_bumps_epochs` - Node removed, partition epochs incremented, NodeId(0) sentinel set
- [ ] `test_metadata_apply_deregister_node_increments_cluster_epoch` - Cluster epoch incremented on deregistration
- [ ] `test_metadata_deregister_node_stale_guards_fail` - PartitionGuard.check() fails after deregistration epoch bump
- [ ] `test_metadata_partition_ownership_returns_full_info` - Returns epoch + node_id
- [ ] `test_metadata_apply_increment_cluster_epoch` - Epoch increments
- [ ] `test_metadata_partition_epoch` - Returns correct epoch
- [ ] `test_metadata_partition_owner` - Returns correct owner
- [ ] `test_metadata_partitions_for_node` - Returns all partitions for a node
- [ ] `test_metadata_snapshot_roundtrip` - Serialize and deserialize snapshot
- [ ] `test_log_entry_serialization` - All log entry variants round-trip through bincode
- [ ] `test_metadata_response_serialization` - Response round-trips

### Integration Tests

- [ ] Three-node Raft group: Form group, elect leader, write and read metadata
- [ ] Leader failover: Kill leader, verify new leader elected within 300ms
- [ ] Linearizable read: Write on leader, read on follower with linearizable guarantee
- [ ] Partition assignment: Assign 100 partitions, verify all nodes have consistent view
- [ ] Snapshot and recovery: Create 10000 entries, take snapshot, restart node, verify recovery
- [ ] Node join: Add fourth node to 3-node group, verify it catches up
- [ ] Network partition: Isolate one node, verify majority continues operating
- [ ] Concurrent writes: Multiple writes from different clients, verify serialization

### Benchmarks

- [ ] `bench_raft_write_throughput` - Target: > 1000 writes/sec
- [ ] `bench_raft_read_linearizable` - Target: < 5ms
- [ ] `bench_raft_read_local` - Target: < 100us
- [ ] `bench_metadata_apply` - Target: < 1us per log entry application
- [ ] `bench_snapshot_creation` - Target: < 100ms for 1000 partitions
- [ ] `bench_snapshot_restore` - Target: < 100ms for 1000 partitions

## Rollout Plan

1. **Phase 1**: Define `DeltaMetadata`, `MetadataLogEntry`, and state machine apply logic
2. **Phase 2**: Implement openraft type configuration and storage backend
3. **Phase 3**: Implement Raft network layer (gRPC or TCP)
4. **Phase 4**: Implement `RaftCoordinator` with write, read_linearizable, and read_local
5. **Phase 5**: Implement snapshot creation and restore
6. **Phase 6**: Integration tests with 3-node Raft group
7. **Phase 7**: Convenience methods, error handling, observability metrics
8. **Phase 8**: Benchmarks, documentation, code review

## Open Questions

- [ ] Should we use gRPC (tonic) or a custom TCP protocol for Raft RPCs? gRPC adds a dependency but provides code generation and TLS for free.
- [ ] Should the Raft log be stored on disk (for durability) or in memory (for speed)? For metadata-only Raft, in-memory with periodic snapshots to disk may be sufficient.
- [ ] How should we handle Raft group membership changes during delta scaling (adding/removing Raft voters)?
- [ ] Should we support learner (non-voting) Raft members for read scaling?
- [ ] What is the optimal snapshot threshold? Too low wastes I/O; too high increases recovery time.

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

- [openraft crate documentation](https://docs.rs/openraft)
- [openraft GitHub repository](https://github.com/datafuselabs/openraft)
- [Raft Consensus Algorithm (Ongaro & Ousterhout)](https://raft.github.io/raft.pdf)
- [Read Index in Raft](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf) (Section 6.4)
- [F-DISC-001: Discovery Trait & Static Discovery](../discovery/F-DISC-001-static-discovery.md)
- [F-EPOCH-001: PartitionGuard & Epoch Fencing](../partition/F-EPOCH-001-partition-guard.md)
- [F-DCKP-004: Object Store Checkpointer](../checkpoint/F-DCKP-004-object-store-checkpointer.md) — AUDIT FIX (C9): See "Checkpoint Commit Protocol" section for the save → Raft commit sequence
