# F-COORD-002: Delta Orchestration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-COORD-002 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | XL (2-3 weeks) |
| **Dependencies** | F-COORD-001 (Raft Metadata), F-DISC-002 (Gossip Discovery), F-EPOCH-003 (Reassignment Protocol) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/delta/mod.rs` |

## Summary

The `DeltaManager` is the top-level coordinator that ties together discovery, Raft consensus, and partition management into a unified lifecycle. It orchestrates the complete node lifecycle: startup (discover peers, form/join Raft group, receive partition assignments, start processing), node join (Rising sequence), node leave (Setting sequence), graceful rolling restart, and automatic failure recovery. Every LaminarDB node that participates in a delta runs a `DeltaManager` instance.

The manager is the "brain" of the distributed system. It subscribes to discovery membership events, drives partition assignment on the Raft leader, executes partition migrations, and monitors node health. It ensures that the system converges to a healthy state after any topology change.

## Goals

- Define `DeltaManager` as the top-level orchestrator for distributed LaminarDB
- Implement the complete startup sequence: discover, form Raft, assign partitions, start processing
- Implement the node join (Rising) flow: register, receive assignment, migrate partitions
- Implement the node leave (Setting) flow: drain, migrate partitions, deregister
- Implement failure detection and automatic reassignment
- Support graceful rolling restart with zero partition loss
- Provide observable state machine for cluster health monitoring
- Coordinate between Discovery, RaftCoordinator, PartitionAssigner, and MigrationExecutor

## Non-Goals

- Implementing the underlying components (Discovery, Raft, Migration - these are separate features)
- Multi-delta federation
- Auto-scaling (external orchestrator decides when to add/remove nodes)
- Application-level routing (the manager handles partition ownership, not client request routing)
- Backpressure between nodes (handled by the streaming layer)

## Technical Design

### Architecture

The `DeltaManager` runs in **Ring 2 (Control Plane)** as a set of tokio tasks. It subscribes to events from the Discovery and Raft subsystems and drives actions in response. On the Raft leader, it additionally runs the partition assignment algorithm and initiates migrations.

```
+------------------------------------------------------------------+
|  DeltaManager (Ring 2)                                    |
|                                                                   |
|  +---------------------+  +-------------------+  +-------------+ |
|  | Membership Watcher   |  | Raft Event Watcher|  | Health      | |
|  | (from Discovery)     |  | (from Raft)       |  | Monitor     | |
|  +----------+----------+  +---------+---------+  +------+------+ |
|             |                       |                    |        |
|             v                       v                    v        |
|  +------------------------------------------------------------------+
|  |                    Event Handler Loop                             |
|  |                                                                   |
|  |  match event {                                                    |
|  |    NodeJoined(info) => handle_node_join(info),                    |
|  |    NodeLeft(id)     => handle_node_leave(id),                     |
|  |    NodeSuspected(id)=> handle_node_suspected(id),                 |
|  |    LeaderChanged    => handle_leader_change(),                    |
|  |    HealthCheck      => handle_health_check(),                     |
|  |  }                                                                |
|  +------------------------------------------------------------------+
|             |                       |                    |        |
|             v                       v                    v        |
|  +---------------------+  +-------------------+  +-------------+ |
|  | PartitionAssigner   |  | MigrationExecutor |  | Raft        | |
|  | (on leader only)    |  | (on all nodes)    |  | Coordinator | |
|  +---------------------+  +-------------------+  +-------------+ |
|                                                                   |
+------------------------------------------------------------------+
         |                        |                       |
         v                        v                       v
+------------------+  +-------------------+  +-------------------+
| Discovery        |  | Partition Guards  |  | Reactor (Ring 0)  |
| (Gossip/Static)  |  | (per partition)   |  | (event processing)|
+------------------+  +-------------------+  +-------------------+
```

### API/Interface

```rust
use std::sync::Arc;
use tokio::sync::{mpsc, watch, RwLock};
use std::time::Duration;

/// Configuration for the delta manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaConfig {
    /// This node's unique identifier.
    pub node_id: NodeId,

    /// This node's human-readable name.
    pub node_name: Option<String>,

    /// Discovery backend configuration.
    pub discovery: DiscoveryBackend,

    /// Raft coordinator configuration.
    pub raft: RaftCoordinatorConfig,

    /// Partition assignment constraints.
    pub assignment_constraints: AssignmentConstraints,

    /// Migration configuration.
    pub migration: MigrationConfig,

    /// Number of partitions in the delta.
    /// Must be set at delta creation and cannot change.
    pub partition_count: u32,

    /// How long to wait for a suspected node before triggering reassignment.
    /// Default: 30 seconds.
    pub failure_detection_grace_period: Duration,

    /// How often to run the health check loop.
    /// Default: 5 seconds.
    pub health_check_interval: Duration,

    /// This node's metadata (cores, memory, etc.).
    pub node_metadata: NodeMetadata,
}

/// Discovery backend selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoveryBackend {
    /// Static peer list with TCP heartbeats (F-DISC-001).
    Static(StaticDiscoveryConfig),

    /// Gossip protocol via chitchat (F-DISC-002).
    Gossip(GossipDiscoveryConfig),

    /// Kafka consumer group (F-DISC-003, requires `kafka` feature).
    #[cfg(feature = "kafka")]
    Kafka(KafkaDiscoveryConfig),
}

/// The top-level delta orchestrator.
///
/// Each node in a LaminarDB delta runs exactly one
/// `DeltaManager`. It coordinates discovery, Raft consensus,
/// partition assignment, and migration to maintain a healthy cluster.
pub struct DeltaManager {
    /// Configuration.
    config: DeltaConfig,

    /// Discovery backend.
    discovery: Arc<dyn Discovery>,

    /// Raft coordinator for metadata consensus.
    raft: Arc<RaftCoordinator>,

    /// Partition assigner (used on leader only).
    assigner: Arc<dyn PartitionAssigner>,

    /// Migration executor for this node.
    migration: Arc<MigrationExecutor>,

    /// Partition guard set for this node.
    guard_set: Arc<RwLock<PartitionGuardSet>>,

    /// Current delta state.
    state: Arc<RwLock<DeltaState>>,

    /// Event channel for internal coordination.
    event_tx: mpsc::Sender<DeltaEvent>,
    event_rx: mpsc::Receiver<DeltaEvent>,

    /// Cancellation token for graceful shutdown.
    cancel: tokio_util::sync::CancellationToken,
}

/// The observable state of the delta from this node's perspective.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaState {
    /// Current phase of this node's lifecycle.
    pub node_phase: NodeLifecyclePhase,

    /// Whether this node is the Raft leader.
    pub is_leader: bool,

    /// Known peers and their states.
    pub peers: Vec<NodeInfo>,

    /// Partitions owned by this node.
    pub owned_partitions: Vec<PartitionId>,

    /// Active migrations on this node.
    pub active_migrations: Vec<MigrationTask>,

    /// Cluster health assessment.
    pub cluster_health: ClusterHealth,

    /// Uptime of this node.
    pub uptime: Duration,
}

/// Lifecycle phases of a node in the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeLifecyclePhase {
    /// Node is starting up: discovering peers.
    Discovering,

    /// Node is forming or joining the Raft group.
    FormingRaft,

    /// Node is waiting for partition assignment.
    WaitingForAssignment,

    /// Node is restoring partition state from checkpoints.
    RestoringPartitions,

    /// Node is active and processing events.
    Active,

    /// Node is draining: migrating partitions away.
    Draining,

    /// Node has completed shutdown.
    Shutdown,
}

/// Cluster health assessment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealth {
    /// Number of active nodes.
    pub active_nodes: usize,

    /// Number of suspected nodes.
    pub suspected_nodes: usize,

    /// Number of unassigned partitions (should be 0 in steady state).
    pub unassigned_partitions: usize,

    /// Number of in-progress migrations.
    pub active_migrations: usize,

    /// Whether the Raft group has a leader.
    pub has_leader: bool,

    /// Overall health status.
    pub status: HealthStatus,
}

/// Overall health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// All nodes active, all partitions assigned, no migrations.
    Healthy,

    /// Some nodes suspected or migrations in progress.
    Degraded,

    /// Critical: no leader, unassigned partitions, or majority of nodes down.
    Critical,
}

/// Internal events processed by the DeltaManager event loop.
#[derive(Debug)]
enum DeltaEvent {
    /// A new node joined the delta (from discovery).
    NodeJoined(NodeInfo),

    /// A node left or was detected as failed (from discovery).
    NodeLeft(NodeId),

    /// A node is suspected (missed heartbeats but not confirmed dead).
    NodeSuspected(NodeId),

    /// Raft leadership changed.
    LeaderChanged { new_leader: Option<u64> },

    /// Periodic health check tick.
    HealthCheckTick,

    /// A migration completed (success or failure).
    MigrationCompleted {
        partition: PartitionId,
        result: Result<(), MigrationError>,
    },

    /// Manual rebalance requested by operator.
    ManualRebalance,

    /// Shutdown requested.
    Shutdown,
}
```

### Data Structures

```rust
impl DeltaManager {
    /// Create a new delta manager.
    pub async fn new(
        config: DeltaConfig,
    ) -> Result<Self, DeltaError> {
        // Create discovery backend
        let discovery: Arc<dyn Discovery> = match &config.discovery {
            DiscoveryBackend::Static(cfg) => {
                Arc::new(StaticDiscovery::new(cfg.clone())?)
            }
            DiscoveryBackend::Gossip(cfg) => {
                Arc::new(GossipDiscovery::new(cfg.clone())?)
            }
            #[cfg(feature = "kafka")]
            DiscoveryBackend::Kafka(cfg) => {
                Arc::new(KafkaDiscovery::new(cfg.clone())?)
            }
        };

        // Create Raft coordinator
        let raft = Arc::new(RaftCoordinator::new(config.raft.clone()).await?);

        // Create assigner
        let assigner: Arc<dyn PartitionAssigner> = Arc::new(
            ConsistentHashAssigner::new(),
        );

        let guard_set = Arc::new(RwLock::new(
            PartitionGuardSet::new(config.node_id),
        ));

        let (event_tx, event_rx) = mpsc::channel(256);

        let migration = Arc::new(MigrationExecutor::new(
            config.node_id,
            raft.clone(),
            guard_set.clone(),
            discovery.clone(),
            config.migration.clone(),
        ));

        Ok(Self {
            config,
            discovery,
            raft,
            assigner,
            migration,
            guard_set,
            state: Arc::new(RwLock::new(DeltaState {
                node_phase: NodeLifecyclePhase::Discovering,
                is_leader: false,
                peers: Vec::new(),
                owned_partitions: Vec::new(),
                active_migrations: Vec::new(),
                cluster_health: ClusterHealth {
                    active_nodes: 0,
                    suspected_nodes: 0,
                    unassigned_partitions: 0,
                    active_migrations: 0,
                    has_leader: false,
                    status: HealthStatus::Critical,
                },
                uptime: Duration::ZERO,
            })),
            event_tx,
            event_rx,
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Start the delta manager.
    ///
    /// Executes the full startup sequence:
    /// 1. Start discovery and find peers
    /// 2. Form or join the Raft group
    /// 3. Wait for partition assignment (or run assignment if leader)
    /// 4. Restore partition state from checkpoints
    /// 5. Begin processing
    ///
    /// This method spawns background tasks and returns once the node
    /// is in the Active phase.
    pub async fn start(&mut self) -> Result<(), DeltaError> {
        // Phase 1: Discover peers
        self.update_phase(NodeLifecyclePhase::Discovering).await;
        self.discovery.start().await
            .map_err(|e| DeltaError::DiscoveryFailed(e.to_string()))?;

        let peers = self.wait_for_peers().await?;

        // Phase 2: Form or join Raft group
        self.update_phase(NodeLifecyclePhase::FormingRaft).await;
        self.form_or_join_raft(&peers).await?;

        // Phase 3: Partition assignment
        self.update_phase(NodeLifecyclePhase::WaitingForAssignment).await;
        let my_partitions = self.wait_for_assignment().await?;

        // Phase 4: Restore partitions
        self.update_phase(NodeLifecyclePhase::RestoringPartitions).await;
        self.restore_partitions(&my_partitions).await?;

        // Phase 5: Start processing
        self.update_phase(NodeLifecyclePhase::Active).await;

        // Spawn event loop
        self.spawn_event_loop();

        // Spawn membership watcher
        self.spawn_membership_watcher();

        // Spawn health check
        self.spawn_health_check();

        Ok(())
    }

    /// Initiate graceful shutdown (Setting sequence).
    ///
    /// 1. Set node state to Draining
    /// 2. Stop accepting new events on owned partitions
    /// 3. Migrate all partitions to other nodes
    /// 4. Deregister from Raft
    /// 5. Leave discovery
    pub async fn shutdown(&self) -> Result<(), DeltaError> {
        self.update_phase(NodeLifecyclePhase::Draining).await;

        // Announce draining state
        self.raft.write(MetadataLogEntry::UpdateNodeState {
            node_id: self.config.node_id,
            state: NodeState::Draining,
            metadata: None,
        }).await?;

        // Trigger reassignment on the leader
        self.event_tx.send(DeltaEvent::ManualRebalance).await
            .map_err(|_| DeltaError::ChannelClosed)?;

        // Wait for all partitions to be migrated
        self.wait_for_partition_drain().await?;

        // Deregister from Raft
        self.raft.write(MetadataLogEntry::DeregisterNode {
            node_id: self.config.node_id,
        }).await?;

        // Leave discovery
        self.discovery.stop().await
            .map_err(|e| DeltaError::DiscoveryFailed(e.to_string()))?;

        self.update_phase(NodeLifecyclePhase::Shutdown).await;
        self.cancel.cancel();
        Ok(())
    }

    /// Wait for a minimum number of peers to be discovered.
    async fn wait_for_peers(&self) -> Result<Vec<NodeInfo>, DeltaError> {
        let timeout = Duration::from_secs(30);
        let start = tokio::time::Instant::now();

        loop {
            let peers = self.discovery.peers().await;
            if !peers.is_empty() {
                return Ok(peers);
            }

            if start.elapsed() > timeout {
                return Err(DeltaError::NoPeersFound);
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Form a new Raft group or join an existing one.
    async fn form_or_join_raft(
        &self,
        peers: &[NodeInfo],
    ) -> Result<(), DeltaError> {
        // Check if any peer already has a Raft group
        // If yes: join existing group via add_learner -> change_membership
        // If no: bootstrap new group with discovered peers as initial members
        todo!("implement Raft group formation")
    }

    /// Wait for partition assignment from the Raft leader.
    ///
    /// If this node is the leader, run the assignment algorithm first.
    async fn wait_for_assignment(&self) -> Result<Vec<PartitionId>, DeltaError> {
        if self.raft.is_leader().await {
            // Run initial assignment
            let peers = self.discovery.peers().await;
            let node_infos: Vec<NodeInfo> = peers.into_iter()
                .chain(std::iter::once(self.self_node_info()))
                .collect();

            let partition_ids: Vec<PartitionId> = (0..self.config.partition_count)
                .map(|i| PartitionId(i))
                .collect();

            let plan = self.assigner.initial_assignment(
                &partition_ids,
                &node_infos,
                &self.config.assignment_constraints,
            ).map_err(|e| DeltaError::AssignmentFailed(format!("{:?}", e)))?;

            // Commit assignments via Raft
            for (pid, assignment) in &plan.assignments {
                self.raft.commit_partition_assignment(
                    *pid,
                    assignment.node_id,
                    assignment.epoch,
                ).await?;
            }
        }

        // Wait for this node's partitions to appear in Raft metadata
        let timeout = Duration::from_secs(30);
        let start = tokio::time::Instant::now();

        loop {
            let my_partitions = self.raft.read_linearizable(|meta| {
                meta.partitions_for_node(self.config.node_id)
            }).await?;

            if !my_partitions.is_empty() {
                return Ok(my_partitions);
            }

            if start.elapsed() > timeout {
                return Err(DeltaError::AssignmentTimeout);
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Restore state for all assigned partitions from their latest checkpoints.
    async fn restore_partitions(
        &self,
        partitions: &[PartitionId],
    ) -> Result<(), DeltaError> {
        for pid in partitions {
            // Look up the latest checkpoint in Raft metadata
            let checkpoint = self.raft.read_linearizable(|meta| {
                meta.latest_checkpoints.get(pid).cloned()
            }).await?;

            if let Some(ckpt) = checkpoint {
                // Download and restore from checkpoint
                self.migration.restore_partition(*pid, &ckpt).await
                    .map_err(|e| DeltaError::RestoreFailed {
                        partition: *pid,
                        source: format!("{}", e),
                    })?;
            }

            // Create partition guard
            let epoch = self.raft.read_linearizable(|meta| {
                meta.partition_epoch(*pid).unwrap_or(1)
            }).await?;

            let guard = PartitionGuard::new(*pid, epoch, self.config.node_id);
            self.guard_set.write().await.insert(guard);
        }

        Ok(())
    }

    /// Handle a node join event (on Raft leader).
    async fn handle_node_join(&self, node: NodeInfo) -> Result<(), DeltaError> {
        if !self.raft.is_leader().await {
            return Ok(()); // Only leader handles assignment
        }

        // Register node in Raft
        self.raft.register_node(NodeRegistration {
            node_id: node.id,
            state: NodeState::Joining,
            rpc_address: node.rpc_address.to_string(),
            raft_address: node.raft_address.to_string(),
            metadata: node.metadata.clone(),
            registered_at: chrono::Utc::now(),
            last_updated: chrono::Utc::now(),
        }).await?;

        // Compute rebalance plan
        let current_map = self.raft.read_linearizable(|meta| {
            meta.partition_map.iter()
                .map(|(pid, o)| (*pid, o.node_id))
                .collect::<HashMap<_, _>>()
        }).await?;

        let all_nodes = self.discovery.peers().await;
        let plan = self.assigner.rebalance(
            &current_map,
            &all_nodes,
            TopologyChange::NodeJoined(node),
            &self.config.assignment_constraints,
        ).map_err(|e| DeltaError::AssignmentFailed(format!("{:?}", e)))?;

        // Execute partition moves
        for mv in &plan.moves {
            self.initiate_migration(mv).await?;
        }

        Ok(())
    }

    /// Handle a node failure detection.
    async fn handle_node_failure(&self, node_id: NodeId) -> Result<(), DeltaError> {
        if !self.raft.is_leader().await {
            return Ok(());
        }

        // Mark node as Left in Raft
        self.raft.write(MetadataLogEntry::UpdateNodeState {
            node_id,
            state: NodeState::Left,
            metadata: None,
        }).await?;

        // Rebalance: reassign dead node's partitions
        let current_map = self.raft.read_linearizable(|meta| {
            meta.partition_map.iter()
                .map(|(pid, o)| (*pid, o.node_id))
                .collect::<HashMap<_, _>>()
        }).await?;

        let active_nodes: Vec<NodeInfo> = self.discovery.peers().await
            .into_iter()
            .filter(|n| n.state == NodeState::Active)
            .collect();

        let plan = self.assigner.rebalance(
            &current_map,
            &active_nodes,
            TopologyChange::NodeLeft(node_id),
            &self.config.assignment_constraints,
        ).map_err(|e| DeltaError::AssignmentFailed(format!("{:?}", e)))?;

        // For forced migration (node crashed), use checkpoint-based recovery
        for mv in &plan.moves {
            if mv.from_node == node_id {
                // Forced migration: new owner recovers from last checkpoint
                self.initiate_forced_migration(mv).await?;
            } else {
                // Normal migration from active node
                self.initiate_migration(mv).await?;
            }
        }

        Ok(())
    }

    /// Initiate a graceful partition migration.
    async fn initiate_migration(
        &self,
        mv: &PartitionMove,
    ) -> Result<(), DeltaError> {
        let task = MigrationTask {
            migration_id: self.next_migration_id(),
            partition_id: mv.partition_id,
            from_node: mv.from_node,
            to_node: mv.to_node,
            from_epoch: mv.old_epoch,
            to_epoch: mv.new_epoch,
            phase: MigrationPhase::Planned,
            checkpoint_id: None,
            source_offsets: None,
            started_at: chrono::Utc::now(),
            completed_at: None,
            timeout: self.config.migration.migration_timeout,
        };

        // Send migration task to the appropriate node
        // (if from_node is this node, execute locally; otherwise, send via RPC)
        let _ = task;
        todo!("send migration task to from_node")
    }

    /// Initiate a forced migration (old owner is down).
    async fn initiate_forced_migration(
        &self,
        mv: &PartitionMove,
    ) -> Result<(), DeltaError> {
        // New owner directly recovers from the last known checkpoint
        let task = MigrationTask {
            migration_id: self.next_migration_id(),
            partition_id: mv.partition_id,
            from_node: mv.from_node,
            to_node: mv.to_node,
            from_epoch: mv.old_epoch,
            to_epoch: mv.new_epoch,
            phase: MigrationPhase::Downloading, // Skip Setting phases
            checkpoint_id: None,
            source_offsets: None,
            started_at: chrono::Utc::now(),
            completed_at: None,
            timeout: self.config.migration.migration_timeout,
        };

        let _ = task;
        todo!("send forced migration task to to_node")
    }

    /// Build NodeInfo for this node.
    fn self_node_info(&self) -> NodeInfo {
        NodeInfo {
            id: self.config.node_id,
            name: self.config.node_name.clone(),
            rpc_address: self.config.raft.raft_address.parse().unwrap(),
            raft_address: self.config.raft.raft_address.parse().unwrap(),
            state: NodeState::Active,
            metadata: self.config.node_metadata.clone(),
            last_heartbeat: Some(chrono::Utc::now()),
            first_seen: chrono::Utc::now(),
        }
    }

    /// Update the node's lifecycle phase.
    async fn update_phase(&self, phase: NodeLifecyclePhase) {
        let mut state = self.state.write().await;
        state.node_phase = phase;
    }

    /// Wait for all owned partitions to be migrated away during shutdown.
    async fn wait_for_partition_drain(&self) -> Result<(), DeltaError> {
        let timeout = Duration::from_secs(120);
        let start = tokio::time::Instant::now();

        loop {
            let owned = self.guard_set.read().await.len();
            if owned == 0 {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(DeltaError::DrainTimeout {
                    remaining_partitions: owned,
                });
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Generate a unique migration ID.
    fn next_migration_id(&self) -> u64 {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    /// Spawn the main event loop.
    fn spawn_event_loop(&self) {
        // Process DeltaEvents from the channel
        // Route to appropriate handler based on event type
    }

    /// Spawn the membership watcher that bridges Discovery -> events.
    fn spawn_membership_watcher(&self) {
        // Subscribe to discovery.on_membership_change()
        // Diff old vs new membership
        // Emit NodeJoined/NodeLeft/NodeSuspected events
    }

    /// Spawn the periodic health check.
    fn spawn_health_check(&self) {
        // Every health_check_interval:
        // - Refresh partition guards
        // - Check for stuck migrations
        // - Update cluster health assessment
        // - Emit HealthCheckTick event
    }

    /// Get the current delta state (for monitoring/admin API).
    pub async fn state(&self) -> DeltaState {
        self.state.read().await.clone()
    }

    /// Get the partition guard set (for use by the reactor).
    pub fn guard_set(&self) -> Arc<RwLock<PartitionGuardSet>> {
        self.guard_set.clone()
    }
}

/// Errors from the delta manager.
#[derive(Debug, Clone)]
pub enum DeltaError {
    /// Discovery failed to start or find peers.
    DiscoveryFailed(String),

    /// No peers found within the timeout.
    NoPeersFound,

    /// Failed to form or join the Raft group.
    RaftFormationFailed(String),

    /// Partition assignment failed.
    AssignmentFailed(String),

    /// Timed out waiting for partition assignment.
    AssignmentTimeout,

    /// Failed to restore a partition from checkpoint.
    RestoreFailed {
        partition: PartitionId,
        source: String,
    },

    /// Partition drain timed out during shutdown.
    DrainTimeout {
        remaining_partitions: usize,
    },

    /// Raft coordination error.
    CoordinationError(CoordinationError),

    /// Internal channel closed.
    ChannelClosed,
}

impl From<CoordinationError> for DeltaError {
    fn from(e: CoordinationError) -> Self {
        DeltaError::CoordinationError(e)
    }
}

impl fmt::Display for DeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaError::DiscoveryFailed(s) => write!(f, "discovery failed: {}", s),
            DeltaError::NoPeersFound => write!(f, "no peers found"),
            DeltaError::RaftFormationFailed(s) => write!(f, "raft formation failed: {}", s),
            DeltaError::AssignmentFailed(s) => write!(f, "assignment failed: {}", s),
            DeltaError::AssignmentTimeout => write!(f, "assignment timeout"),
            DeltaError::RestoreFailed { partition, source } => {
                write!(f, "restore failed for {:?}: {}", partition, source)
            }
            DeltaError::DrainTimeout { remaining_partitions } => {
                write!(f, "drain timeout: {} partitions remaining", remaining_partitions)
            }
            DeltaError::CoordinationError(e) => write!(f, "coordination: {}", e),
            DeltaError::ChannelClosed => write!(f, "internal channel closed"),
        }
    }
}

impl std::error::Error for DeltaError {}
```

### Algorithm/Flow

**Startup Sequence:**

1. `DeltaManager::start()` is called.
2. **Discovery**: Start the discovery backend and wait for at least one peer to be found. If no peers are found within 30 seconds, return an error.
3. **Raft Formation**: Check if any discovered peer has an existing Raft group. If yes, join it (add_learner -> change_membership). If no, bootstrap a new group with all discovered peers as initial voters.
4. **Assignment**: If this node is the Raft leader, run `PartitionAssigner::initial_assignment()` to compute the partition map and commit it via Raft. If a follower, wait for the partition map to appear in committed Raft state.
5. **Restore**: For each partition assigned to this node, look up the latest checkpoint in Raft metadata. If a checkpoint exists, download and restore it. Create a `PartitionGuard` for each partition.
6. **Active**: Start the reactor (Ring 0) for all owned partitions. Spawn background tasks for the event loop, membership watcher, and health check.

**Node Join (Rising):**

1. Discovery detects a new node (via gossip or static heartbeat).
2. Membership watcher emits `DeltaEvent::NodeJoined`.
3. On the Raft leader, `handle_node_join()` registers the node in Raft metadata.
4. The leader runs `PartitionAssigner::rebalance()` to compute partition moves.
5. For each move, `initiate_migration()` creates a `MigrationTask` and sends it to the old owner.
6. The old and new owners execute the migration protocol (F-EPOCH-003).
7. Once all migrations complete, the new node is fully active.

**Node Failure Recovery:**

1. Discovery detects a node is unreachable (phi-accrual threshold exceeded).
2. Membership watcher emits `DeltaEvent::NodeSuspected`.
3. After `failure_detection_grace_period`, if the node is still unreachable, the leader confirms it as failed.
4. `handle_node_failure()` marks the node as `Left` in Raft and runs rebalance.
5. For the crashed node's partitions, `initiate_forced_migration()` directs new owners to recover from the last known checkpoint (no Setting phase since the old owner is down).

**Graceful Shutdown (Setting):**

1. Operator calls `shutdown()` on the node.
2. Node sets its state to `Draining` in Raft and discovery.
3. Leader runs rebalance excluding the draining node.
4. Partitions are migrated away via the normal protocol.
5. Once all partitions are migrated, the node deregisters from Raft and leaves discovery.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DeltaError::NoPeersFound` | Network isolation or misconfigured discovery | Check network, verify seed nodes/peer addresses |
| `DeltaError::RaftFormationFailed` | Cannot reach quorum of peers for Raft | Ensure majority of nodes are reachable |
| `DeltaError::AssignmentTimeout` | Leader not elected or assignment not committed | Wait for Raft leader election; check Raft health |
| `DeltaError::RestoreFailed` | Checkpoint missing or corrupted | Alert operator; partition starts fresh (data loss) |
| `DeltaError::DrainTimeout` | Migrations stuck during shutdown | Force shutdown; other nodes will recover via forced migration |
| Leader failure during startup | Raft leader crashes before completing assignment | New leader elected; assignment retried automatically |
| Simultaneous node failures | Multiple nodes fail at once | As long as Raft quorum survives, recovery proceeds |

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Startup time (3-node cluster) | < 10 seconds | Discovery + Raft formation + assignment |
| Node join (Rising) total time | < 30 seconds | Registration + rebalance + migration |
| Failure detection to recovery start | < 35 seconds | 30s grace period + 5s rebalance |
| Graceful shutdown | < 60 seconds | Depends on partition count and state size |
| Health check overhead | < 1% CPU | Periodic background task |
| Event loop latency | < 10ms | Per event processing |
| Cluster state query | < 1ms | Read from local state |

## Test Plan

### Unit Tests

- [ ] `test_delta_config_creation` - Valid config creates manager
- [ ] `test_node_lifecycle_phase_transitions` - Phases progress in order
- [ ] `test_cluster_health_computation` - Health status computed correctly
- [ ] `test_health_status_healthy` - All active, no migrations = Healthy
- [ ] `test_health_status_degraded` - Suspected nodes = Degraded
- [ ] `test_health_status_critical` - No leader = Critical
- [ ] `test_delta_error_display` - All error variants format correctly
- [ ] `test_self_node_info` - Correct NodeInfo generated for self

### Integration Tests

- [ ] Three-node startup: All nodes discover, form Raft, assign partitions, go active
- [ ] Node join: Fourth node joins running 3-node cluster; receives partitions
- [ ] Node graceful leave: Node shuts down; partitions migrated to survivors
- [ ] Node crash: Kill node; survivors detect and recover partitions
- [ ] Rolling restart: Restart all nodes one at a time; all partitions survive
- [ ] Leader failover: Kill Raft leader; new leader elected; cluster continues
- [ ] Network partition: Minority isolated; majority continues; minority rejoins
- [ ] Simultaneous start: All 5 nodes start at the same time; cluster forms correctly
- [ ] Empty cluster: First node bootstraps alone; second node joins later

### Benchmarks

- [ ] `bench_startup_3_nodes` - Target: < 10 seconds
- [ ] `bench_startup_5_nodes` - Target: < 15 seconds
- [ ] `bench_node_join` - Target: < 30 seconds
- [ ] `bench_node_failure_recovery` - Target: < 60 seconds (including grace period)
- [ ] `bench_cluster_state_query` - Target: < 1ms

## Rollout Plan

1. **Phase 1**: Define `DeltaManager`, `DeltaConfig`, `DeltaState`, and `DeltaError`
2. **Phase 2**: Implement startup sequence (discover + Raft formation + assignment)
3. **Phase 3**: Implement event loop and membership watcher
4. **Phase 4**: Implement node join (Rising) flow
5. **Phase 5**: Implement node failure detection and recovery
6. **Phase 6**: Implement graceful shutdown (Setting) flow
7. **Phase 7**: Implement health monitoring and cluster health assessment
8. **Phase 8**: Integration tests with full multi-node setup
9. **Phase 9**: Benchmarks, observability, documentation

## Open Questions

- [ ] Should the delta manager support "single-node mode" where no discovery or Raft is needed (for development/testing)?
- [ ] How should we handle the case where the Raft leader is also being shut down? Transfer leadership first?
- [ ] Should the health check interval be adaptive (more frequent during degraded state)?
- [ ] Should we expose a "manual takeover" operation for operators to force a partition to a specific node?
- [ ] How should we handle version mismatches during rolling upgrades (old and new node versions running simultaneously)?
- [ ] Should the delta manager persist its state locally for faster restart, or always rebuild from Raft?

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

- [F-COORD-001: DeltaMetadata & Raft Integration](F-COORD-001-raft-metadata.md)
- [F-DISC-001: Discovery Trait & Static Discovery](../discovery/F-DISC-001-static-discovery.md)
- [F-DISC-002: Gossip Discovery](../discovery/F-DISC-002-gossip-discovery.md)
- [F-EPOCH-001: PartitionGuard & Epoch Fencing](../partition/F-EPOCH-001-partition-guard.md)
- [F-EPOCH-002: Partition Assignment Algorithm](../partition/F-EPOCH-002-assignment-algorithm.md)
- [F-EPOCH-003: Partition Reassignment Protocol](../partition/F-EPOCH-003-reassignment-protocol.md)
- [Apache Kafka Controller Architecture](https://kafka.apache.org/documentation/#design_replicamanagment)
- [CockroachDB Node Lifecycle](https://www.cockroachlabs.com/docs/stable/cockroach-node.html)
