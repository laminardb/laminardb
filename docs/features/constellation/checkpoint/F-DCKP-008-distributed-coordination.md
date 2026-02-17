# F-DCKP-008: Distributed Checkpoint Coordination

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-008 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | XL (10-15 days) |
| **Dependencies** | F-DCKP-001 (Barrier Protocol), F-DCKP-004 (Object Store Checkpointer), F-COORD-001 (Coordinator Service), F-RPC-003 (Inter-node RPC) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/constellation/checkpoint.rs` |

## Summary

Implements distributed checkpoint coordination for LaminarDB's constellation (multi-node) mode. The checkpoint coordinator, running on the Raft leader node, orchestrates consistent distributed snapshots across all nodes in the cluster. The flow is: (1) coordinator injects barriers into all source nodes via RPC, (2) barriers propagate through cross-node dataflow channels, (3) each node uploads its operator snapshots to the shared object store, (4) nodes report completion to the coordinator, (5) coordinator commits the manifest after all nodes complete using a two-phase commit protocol. Handles partial failures, node crashes during checkpointing, timeout-based abort, and coordinator failover.

## Goals

- Coordinator initiates distributed checkpoints via Raft leader
- Inject barriers into all source nodes across the cluster via RPC
- Track barrier propagation and snapshot completion across all nodes
- Two-phase commit for manifest: prepare (all snapshots uploaded) then commit (write manifest)
- Handle partial failures: abort checkpoint if any node fails
- Timeout handling: abort if checkpoint exceeds configurable deadline
- Coordinator failover: new leader recovers in-progress checkpoint state
- Support both aligned and unaligned distributed checkpoints
- Observable metrics: per-node checkpoint duration, cross-node barrier latency

## Non-Goals

- Single-node checkpointing (covered by F-DCKP-001 through F-DCKP-005)
- Raft consensus implementation (covered by F-COORD-001)
- RPC transport layer (covered by F-RPC-003)
- Checkpoint storage (covered by F-DCKP-004)
- Incremental checkpoints across nodes (each node manages its own incremental strategy)

## Technical Design

### Architecture

**Ring**: Ring 1 (Coordination) on the leader node; Ring 0 (Hot Path) for barrier propagation on worker nodes.

**Crate**: `laminar-core`

**Module**: `laminar-core/src/constellation/checkpoint.rs`

The distributed checkpoint coordinator runs as a component on the Raft leader. It maintains a state machine for each in-progress checkpoint, tracking which nodes have completed their local snapshots. Cross-node barrier propagation uses the same network channels as data events (ensuring causal ordering). The coordinator uses a two-phase commit protocol: Phase 1 (Prepare) waits for all nodes to upload snapshots, Phase 2 (Commit) writes the global manifest.

```
┌──────────────────────────────────────────────────────────────────────┐
│  Raft Leader (Coordinator)                                            │
│                                                                       │
│  DistributedCheckpointCoordinator                                     │
│  ┌──────────────────────────────────────────────────────────┐        │
│  │ 1. Generate CheckpointId                                  │        │
│  │ 2. Send InjectBarrier RPC to all source nodes            │        │
│  │ 3. Wait for SnapshotComplete from all nodes              │        │
│  │ 4. Two-phase commit: prepare → commit manifest           │        │
│  └──────────────────────────────────────────────────────────┘        │
│         │                     │                     │                 │
│    InjectBarrier         InjectBarrier         InjectBarrier          │
│         │                     │                     │                 │
│         ▼                     ▼                     ▼                 │
│  ┌────────────┐       ┌────────────┐       ┌────────────┐           │
│  │   Node 0   │──────>│   Node 1   │──────>│   Node 2   │           │
│  │ (sources)  │barriers│ (operators)│barriers│  (sinks)  │           │
│  │            │───────>│            │───────>│            │           │
│  └────────────┘       └────────────┘       └────────────┘           │
│         │                     │                     │                 │
│    Upload to              Upload to              Upload to            │
│    Object Store          Object Store           Object Store          │
│         │                     │                     │                 │
│         └────── SnapshotComplete ─────────────────┘                  │
│                       │                                               │
│              Coordinator commits manifest                             │
└──────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use crate::checkpoint::barrier::{CheckpointBarrier, InjectBarrierCommand};
use crate::checkpoint::checkpointer::{CheckpointData, CheckpointError, Checkpointer};
use crate::checkpoint::manifest::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, RwLock};

/// Unique identifier for a node in the constellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// Configuration for the distributed checkpoint coordinator.
#[derive(Debug, Clone)]
pub struct DistributedCheckpointConfig {
    /// Interval between checkpoint initiations.
    pub checkpoint_interval: Duration,

    /// Maximum time allowed for a checkpoint to complete across all nodes.
    pub checkpoint_timeout: Duration,

    /// Maximum time to wait for barrier injection acknowledgement.
    pub barrier_injection_timeout: Duration,

    /// Maximum time to wait for a single node's snapshot upload.
    pub node_snapshot_timeout: Duration,

    /// Whether to use unaligned mode as fallback.
    pub enable_unaligned_fallback: bool,

    /// Threshold before switching to unaligned mode.
    pub unaligned_threshold: Duration,

    /// Maximum concurrent checkpoints (typically 1).
    pub max_concurrent_checkpoints: usize,

    /// Number of checkpoints to retain during garbage collection.
    pub gc_retain_count: usize,

    /// Whether to run GC after each successful checkpoint.
    pub auto_gc: bool,
}

impl Default for DistributedCheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(30),
            checkpoint_timeout: Duration::from_secs(300),
            barrier_injection_timeout: Duration::from_secs(10),
            node_snapshot_timeout: Duration::from_secs(120),
            enable_unaligned_fallback: true,
            unaligned_threshold: Duration::from_secs(30),
            max_concurrent_checkpoints: 1,
            gc_retain_count: 5,
            auto_gc: true,
        }
    }
}

/// RPC interface for checkpoint coordination between coordinator and nodes.
///
/// Implemented by the RPC layer (F-RPC-003).
#[async_trait::async_trait]
pub trait CheckpointRpc: Send + Sync {
    /// Send a barrier injection command to a node.
    async fn inject_barrier(
        &self,
        node_id: NodeId,
        command: InjectBarrierCommand,
    ) -> Result<BarrierInjectionAck, CheckpointRpcError>;

    /// Notify a node to prepare for checkpoint (upload snapshots).
    async fn prepare_checkpoint(
        &self,
        node_id: NodeId,
        checkpoint_id: u64,
    ) -> Result<PrepareAck, CheckpointRpcError>;

    /// Notify a node that the checkpoint has been committed.
    async fn commit_checkpoint(
        &self,
        node_id: NodeId,
        checkpoint_id: u64,
    ) -> Result<(), CheckpointRpcError>;

    /// Notify a node that the checkpoint has been aborted.
    async fn abort_checkpoint(
        &self,
        node_id: NodeId,
        checkpoint_id: u64,
    ) -> Result<(), CheckpointRpcError>;
}

/// Acknowledgement from a node that barrier injection was received.
#[derive(Debug)]
pub struct BarrierInjectionAck {
    pub node_id: NodeId,
    pub checkpoint_id: u64,
    pub source_count: usize,
}

/// Acknowledgement from a node that its snapshot is uploaded (prepare phase).
#[derive(Debug)]
pub struct PrepareAck {
    pub node_id: NodeId,
    pub checkpoint_id: u64,
    /// Operator snapshot entries uploaded by this node.
    pub operator_entries: Vec<OperatorSnapshotEntry>,
    /// Source offset entries from this node.
    pub source_entries: Vec<SourceOffsetEntry>,
    /// Total bytes uploaded by this node.
    pub bytes_uploaded: u64,
    /// Duration of this node's snapshot phase.
    pub snapshot_duration: Duration,
}

/// Errors from checkpoint RPC operations.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointRpcError {
    /// Node is unreachable.
    #[error("node {0:?} is unreachable")]
    NodeUnreachable(NodeId),

    /// RPC timed out.
    #[error("RPC to node {0:?} timed out")]
    Timeout(NodeId),

    /// Node rejected the request.
    #[error("node {0:?} rejected checkpoint: {1}")]
    Rejected(NodeId, String),

    /// Transport error.
    #[error("transport error to node {0:?}: {1}")]
    TransportError(NodeId, String),
}
```

### Data Structures

```rust
/// State machine for a single in-progress distributed checkpoint.
#[derive(Debug)]
struct CheckpointState {
    /// The checkpoint being coordinated.
    checkpoint_id: u64,

    /// Global epoch number.
    epoch: u64,

    /// Current phase of the distributed checkpoint.
    phase: CheckpointPhase,

    /// All nodes participating in this checkpoint.
    participating_nodes: HashSet<NodeId>,

    /// Nodes that have acknowledged barrier injection.
    barriers_injected: HashSet<NodeId>,

    /// Nodes that have completed the prepare phase (snapshots uploaded).
    prepared_nodes: HashMap<NodeId, PrepareAck>,

    /// Nodes that have acknowledged the commit.
    committed_nodes: HashSet<NodeId>,

    /// Nodes that have failed during this checkpoint.
    failed_nodes: HashMap<NodeId, String>,

    /// When this checkpoint was initiated.
    started_at: Instant,

    /// When each phase started (for timeout tracking).
    phase_started_at: Instant,

    /// Whether this checkpoint is using unaligned mode.
    is_unaligned: bool,
}

/// Phases of a distributed checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointPhase {
    /// Sending barrier injection commands to all source nodes.
    InjectingBarriers,

    /// Waiting for all nodes to complete local snapshots and upload.
    WaitingForPrepare,

    /// All nodes have prepared; writing the global manifest.
    Committing,

    /// Checkpoint completed successfully.
    Completed,

    /// Checkpoint was aborted due to failure or timeout.
    Aborted,
}

/// The distributed checkpoint coordinator.
///
/// Runs on the Raft leader node and orchestrates checkpoints across
/// the constellation.
pub struct DistributedCheckpointCoordinator {
    /// Configuration.
    config: DistributedCheckpointConfig,

    /// RPC interface for communicating with nodes.
    rpc: Arc<dyn CheckpointRpc>,

    /// Object store checkpointer for manifest operations.
    checkpointer: Arc<dyn Checkpointer>,

    /// Currently in-progress checkpoints.
    active_checkpoints: HashMap<u64, CheckpointState>,

    /// Monotonically increasing checkpoint ID counter.
    next_checkpoint_id: u64,

    /// Current epoch.
    current_epoch: u64,

    /// Known cluster membership (updated by Raft).
    cluster_nodes: HashSet<NodeId>,

    /// Nodes that host source operators (barrier injection targets).
    source_nodes: HashSet<NodeId>,

    /// Metrics.
    metrics: CoordinatorMetrics,
}

impl DistributedCheckpointCoordinator {
    /// Create a new coordinator.
    pub fn new(
        config: DistributedCheckpointConfig,
        rpc: Arc<dyn CheckpointRpc>,
        checkpointer: Arc<dyn Checkpointer>,
        cluster_nodes: HashSet<NodeId>,
        source_nodes: HashSet<NodeId>,
    ) -> Self {
        Self {
            config,
            rpc,
            checkpointer,
            active_checkpoints: HashMap::new(),
            next_checkpoint_id: 1,
            current_epoch: 0,
            cluster_nodes,
            source_nodes,
            metrics: CoordinatorMetrics::default(),
        }
    }

    /// Initiate a new distributed checkpoint.
    ///
    /// This is the main entry point, called periodically by the
    /// coordinator timer or manually by an admin.
    pub async fn initiate_checkpoint(&mut self) -> Result<u64, DistributedCheckpointError> {
        // Check concurrency limit
        let active_count = self.active_checkpoints.values()
            .filter(|s| !matches!(s.phase, CheckpointPhase::Completed | CheckpointPhase::Aborted))
            .count();

        if active_count >= self.config.max_concurrent_checkpoints {
            return Err(DistributedCheckpointError::TooManyConcurrent {
                active: active_count,
                max: self.config.max_concurrent_checkpoints,
            });
        }

        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;
        self.current_epoch += 1;

        let state = CheckpointState {
            checkpoint_id,
            epoch: self.current_epoch,
            phase: CheckpointPhase::InjectingBarriers,
            participating_nodes: self.cluster_nodes.clone(),
            barriers_injected: HashSet::new(),
            prepared_nodes: HashMap::new(),
            committed_nodes: HashSet::new(),
            failed_nodes: HashMap::new(),
            started_at: Instant::now(),
            phase_started_at: Instant::now(),
            is_unaligned: false,
        };

        self.active_checkpoints.insert(checkpoint_id, state);

        // Phase 1: Inject barriers into all source nodes
        self.inject_barriers(checkpoint_id).await?;

        Ok(checkpoint_id)
    }

    /// Phase 1: Inject barriers into all source nodes.
    async fn inject_barriers(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), DistributedCheckpointError> {
        let state = self.active_checkpoints.get(&checkpoint_id)
            .ok_or(DistributedCheckpointError::CheckpointNotFound(checkpoint_id))?;

        let epoch = state.epoch;
        let source_nodes: Vec<NodeId> = self.source_nodes.iter().copied().collect();

        // Send barrier injection RPCs in parallel
        let mut injection_futures = Vec::new();
        for node_id in &source_nodes {
            let rpc = self.rpc.clone();
            let node = *node_id;
            let cmd = InjectBarrierCommand {
                checkpoint_id,
                epoch,
                is_unaligned: false,
            };

            injection_futures.push(async move {
                let result = tokio::time::timeout(
                    Duration::from_secs(10),
                    rpc.inject_barrier(node, cmd),
                ).await;

                match result {
                    Ok(Ok(ack)) => Ok(ack),
                    Ok(Err(e)) => Err((node, e.to_string())),
                    Err(_) => Err((node, "barrier injection timed out".to_string())),
                }
            });
        }

        let results = futures::future::join_all(injection_futures).await;

        let state = self.active_checkpoints.get_mut(&checkpoint_id).unwrap();

        for result in results {
            match result {
                Ok(ack) => {
                    state.barriers_injected.insert(ack.node_id);
                }
                Err((node_id, reason)) => {
                    state.failed_nodes.insert(node_id, reason.clone());
                    tracing::error!(
                        checkpoint_id,
                        ?node_id,
                        reason,
                        "barrier injection failed"
                    );
                }
            }
        }

        // If any source node failed, abort the checkpoint
        if !state.failed_nodes.is_empty() {
            self.abort_checkpoint(checkpoint_id, "barrier injection failed on some nodes").await;
            return Err(DistributedCheckpointError::BarrierInjectionFailed {
                checkpoint_id,
                failed_nodes: state.failed_nodes.keys().copied().collect(),
            });
        }

        // Transition to WaitingForPrepare
        state.phase = CheckpointPhase::WaitingForPrepare;
        state.phase_started_at = Instant::now();

        Ok(())
    }

    /// Called when a node reports that its snapshot is uploaded.
    pub async fn on_node_prepared(
        &mut self,
        checkpoint_id: u64,
        ack: PrepareAck,
    ) -> Result<(), DistributedCheckpointError> {
        let state = self.active_checkpoints.get_mut(&checkpoint_id)
            .ok_or(DistributedCheckpointError::CheckpointNotFound(checkpoint_id))?;

        if state.phase != CheckpointPhase::WaitingForPrepare {
            return Err(DistributedCheckpointError::UnexpectedPhase {
                checkpoint_id,
                expected: CheckpointPhase::WaitingForPrepare,
                actual: state.phase,
            });
        }

        let node_id = ack.node_id;
        state.prepared_nodes.insert(node_id, ack);

        tracing::info!(
            checkpoint_id,
            ?node_id,
            prepared = state.prepared_nodes.len(),
            total = state.participating_nodes.len(),
            "node prepared for checkpoint"
        );

        // Check if all nodes have prepared
        if state.prepared_nodes.len() == state.participating_nodes.len() {
            self.commit_checkpoint(checkpoint_id).await?;
        }

        Ok(())
    }

    /// Phase 2 (Commit): Write the global manifest and notify all nodes.
    async fn commit_checkpoint(
        &mut self,
        checkpoint_id: u64,
    ) -> Result<(), DistributedCheckpointError> {
        let state = self.active_checkpoints.get_mut(&checkpoint_id)
            .ok_or(DistributedCheckpointError::CheckpointNotFound(checkpoint_id))?;

        state.phase = CheckpointPhase::Committing;
        state.phase_started_at = Instant::now();

        // Aggregate all operator and source entries from all nodes
        let mut all_operators = Vec::new();
        let mut all_sources = Vec::new();
        let mut total_bytes = 0u64;

        for ack in state.prepared_nodes.values() {
            all_operators.extend(ack.operator_entries.clone());
            all_sources.extend(ack.source_entries.clone());
            total_bytes += ack.bytes_uploaded;
        }

        // Build and write the global manifest
        let global_id = CheckpointId::new();
        let mut builder = CheckpointManifest::builder(global_id, state.epoch);

        for op in all_operators {
            builder = builder.add_operator(op);
        }
        for src in all_sources {
            builder = builder.add_source(src);
        }

        builder = builder.unaligned(state.is_unaligned);
        let manifest = builder.build();

        // Write manifest to object store (atomic commit point)
        let manifest_json = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| DistributedCheckpointError::ManifestSerializationFailed(e.to_string()))?;

        // This is the point of no return: if the manifest write succeeds,
        // the checkpoint is committed.
        // The checkpointer handles the atomic write (temp + rename).

        // Notify all nodes of commit
        let nodes: Vec<NodeId> = state.participating_nodes.iter().copied().collect();
        let mut commit_futures = Vec::new();

        for node_id in &nodes {
            let rpc = self.rpc.clone();
            let node = *node_id;
            let ckpt_id = checkpoint_id;

            commit_futures.push(async move {
                let result = rpc.commit_checkpoint(node, ckpt_id).await;
                (node, result)
            });
        }

        let commit_results = futures::future::join_all(commit_futures).await;

        let state = self.active_checkpoints.get_mut(&checkpoint_id).unwrap();

        for (node_id, result) in commit_results {
            match result {
                Ok(()) => {
                    state.committed_nodes.insert(node_id);
                }
                Err(e) => {
                    // Commit notification failure is non-fatal: the manifest
                    // is already written, and nodes will discover it on restart.
                    tracing::warn!(
                        checkpoint_id,
                        ?node_id,
                        error = %e,
                        "commit notification failed (non-fatal)"
                    );
                }
            }
        }

        state.phase = CheckpointPhase::Completed;

        let duration = state.started_at.elapsed();
        self.metrics.checkpoints_completed += 1;
        self.metrics.last_checkpoint_duration = duration;
        if duration > self.metrics.max_checkpoint_duration {
            self.metrics.max_checkpoint_duration = duration;
        }
        self.metrics.total_bytes_checkpointed += total_bytes;

        tracing::info!(
            checkpoint_id,
            epoch = state.epoch,
            duration_ms = duration.as_millis(),
            total_bytes,
            nodes = state.participating_nodes.len(),
            "distributed checkpoint completed"
        );

        // Auto-GC if configured
        if self.config.auto_gc {
            if let Err(e) = self.checkpointer.gc(self.config.gc_retain_count).await {
                tracing::warn!(error = %e, "auto-gc failed");
            }
        }

        Ok(())
    }

    /// Abort a checkpoint and notify all nodes.
    async fn abort_checkpoint(&mut self, checkpoint_id: u64, reason: &str) {
        tracing::error!(checkpoint_id, reason, "aborting distributed checkpoint");

        if let Some(state) = self.active_checkpoints.get_mut(&checkpoint_id) {
            state.phase = CheckpointPhase::Aborted;
            self.metrics.checkpoints_aborted += 1;

            let nodes: Vec<NodeId> = state.participating_nodes.iter().copied().collect();

            for node_id in nodes {
                let rpc = self.rpc.clone();
                let _ = rpc.abort_checkpoint(node_id, checkpoint_id).await;
            }
        }
    }

    /// Check for timed-out checkpoints and abort them.
    pub async fn check_timeouts(&mut self) {
        let timed_out: Vec<u64> = self.active_checkpoints.iter()
            .filter(|(_, state)| {
                matches!(
                    state.phase,
                    CheckpointPhase::InjectingBarriers | CheckpointPhase::WaitingForPrepare
                ) && state.started_at.elapsed() > self.config.checkpoint_timeout
            })
            .map(|(id, _)| *id)
            .collect();

        for checkpoint_id in timed_out {
            // Check if we should switch to unaligned mode before aborting
            if let Some(state) = self.active_checkpoints.get(&checkpoint_id) {
                if self.config.enable_unaligned_fallback
                    && !state.is_unaligned
                    && state.phase == CheckpointPhase::WaitingForPrepare
                    && state.started_at.elapsed() > self.config.unaligned_threshold
                {
                    tracing::warn!(
                        checkpoint_id,
                        "switching to unaligned mode due to timeout"
                    );
                    // Re-inject barriers as unaligned
                    // (Implementation detail: send new unaligned barriers to pending nodes)
                    continue;
                }
            }

            self.abort_checkpoint(checkpoint_id, "checkpoint timed out").await;
        }
    }

    /// Handle coordinator failover: recover in-progress checkpoint state.
    ///
    /// Called when this node becomes the new Raft leader.
    pub async fn on_leader_elected(&mut self) -> Result<(), DistributedCheckpointError> {
        tracing::info!("new leader elected, recovering checkpoint state");

        // Discard any in-progress checkpoints from the old leader.
        // They were not committed (no manifest), so they are effectively
        // aborted. Nodes will discard their partial uploads.
        let in_progress: Vec<u64> = self.active_checkpoints.keys().copied().collect();
        for checkpoint_id in in_progress {
            self.abort_checkpoint(checkpoint_id, "leader failover").await;
        }
        self.active_checkpoints.clear();

        // Load the latest committed checkpoint to determine the epoch
        match self.checkpointer.list().await {
            Ok(manifests) => {
                if let Some(latest) = manifests.first() {
                    self.current_epoch = latest.epoch;
                    self.next_checkpoint_id = latest.epoch + 1;
                    tracing::info!(
                        epoch = latest.epoch,
                        checkpoint_id = %latest.checkpoint_id.to_path_string(),
                        "recovered checkpoint state from manifest"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to list checkpoints on leader election");
            }
        }

        Ok(())
    }

    /// Update cluster membership (called when nodes join or leave).
    pub fn update_cluster_membership(
        &mut self,
        nodes: HashSet<NodeId>,
        source_nodes: HashSet<NodeId>,
    ) {
        self.cluster_nodes = nodes;
        self.source_nodes = source_nodes;
    }

    /// Returns metrics for observability.
    pub fn metrics(&self) -> &CoordinatorMetrics {
        &self.metrics
    }
}

/// Metrics for the distributed checkpoint coordinator.
#[derive(Debug, Default, Clone)]
pub struct CoordinatorMetrics {
    /// Total checkpoints completed successfully.
    pub checkpoints_completed: u64,
    /// Total checkpoints aborted.
    pub checkpoints_aborted: u64,
    /// Duration of the last completed checkpoint.
    pub last_checkpoint_duration: Duration,
    /// Maximum checkpoint duration observed.
    pub max_checkpoint_duration: Duration,
    /// Total bytes checkpointed across all checkpoints.
    pub total_bytes_checkpointed: u64,
    /// Number of unaligned checkpoints triggered.
    pub unaligned_checkpoints: u64,
    /// Number of coordinator failovers.
    pub leader_failovers: u64,
}

/// Errors from distributed checkpoint coordination.
#[derive(Debug, thiserror::Error)]
pub enum DistributedCheckpointError {
    /// Too many concurrent checkpoints.
    #[error("too many concurrent checkpoints: {active}/{max}")]
    TooManyConcurrent { active: usize, max: usize },

    /// Checkpoint not found in active checkpoints.
    #[error("checkpoint {0} not found")]
    CheckpointNotFound(u64),

    /// Barrier injection failed on some nodes.
    #[error("barrier injection failed for checkpoint {checkpoint_id} on nodes: {failed_nodes:?}")]
    BarrierInjectionFailed {
        checkpoint_id: u64,
        failed_nodes: Vec<NodeId>,
    },

    /// Unexpected phase transition.
    #[error("checkpoint {checkpoint_id} in unexpected phase: expected {expected:?}, got {actual:?}")]
    UnexpectedPhase {
        checkpoint_id: u64,
        expected: CheckpointPhase,
        actual: CheckpointPhase,
    },

    /// Manifest serialization failed.
    #[error("manifest serialization failed: {0}")]
    ManifestSerializationFailed(String),

    /// Checkpoint was aborted.
    #[error("checkpoint {checkpoint_id} was aborted: {reason}")]
    Aborted {
        checkpoint_id: u64,
        reason: String,
    },

    /// Node failure during checkpoint.
    #[error("node {node_id:?} failed during checkpoint {checkpoint_id}: {reason}")]
    NodeFailed {
        checkpoint_id: u64,
        node_id: NodeId,
        reason: String,
    },

    /// Checkpoint timed out.
    #[error("checkpoint {checkpoint_id} timed out after {duration:?}")]
    Timeout {
        checkpoint_id: u64,
        duration: Duration,
    },

    /// RPC error.
    #[error("RPC error: {0}")]
    RpcError(#[from] CheckpointRpcError),

    /// Checkpointer error.
    #[error("checkpointer error: {0}")]
    CheckpointerError(#[from] CheckpointError),
}
```

### Algorithm/Flow

#### Distributed Checkpoint Protocol (Two-Phase Commit)

```
COORDINATOR (Raft Leader):

PHASE 0: INITIATE
  1. Generate checkpoint_id and increment epoch
  2. Record participating_nodes = current cluster membership
  3. Create CheckpointState in InjectingBarriers phase

PHASE 1a: INJECT BARRIERS
  4. For each source node (in parallel):
     a. Send InjectBarrier RPC with (checkpoint_id, epoch, is_unaligned=false)
     b. Wait for BarrierInjectionAck
     c. If any source fails: abort checkpoint
  5. Transition to WaitingForPrepare phase

  Barriers now propagate through the dataflow graph:
  - Source nodes inject barriers into their output channels
  - Intermediate nodes align barriers (F-DCKP-002) and forward
  - Cross-node channels carry barriers alongside data events
  - Each node snapshots its operator state as barriers arrive

PHASE 1b: WAIT FOR PREPARE
  6. Each node, after all its operators have snapshotted:
     a. Upload operator snapshots to object store
     b. Send PrepareAck to coordinator with snapshot metadata
  7. Coordinator waits until all nodes have sent PrepareAck
  8. If timeout exceeded:
     - If unaligned fallback enabled: switch to unaligned mode
     - Otherwise: abort checkpoint

PHASE 2: COMMIT
  9. Aggregate all operator entries and source offsets from PrepareAcks
  10. Build CheckpointManifest
  11. Write manifest to object store (ATOMIC COMMIT POINT)
  12. Update _latest pointer
  13. Send CommitCheckpoint notification to all nodes
  14. Nodes can now discard pre-checkpoint WAL entries
  15. Transition to Completed phase

ABORT (at any point):
  - Send AbortCheckpoint to all nodes
  - Nodes discard partial snapshot uploads
  - Coordinator marks checkpoint as Aborted
  - Incomplete checkpoint data cleaned up by GC
```

#### Cross-Node Barrier Propagation

```
Node A (source) ──barrier──> Node B (intermediate) ──barrier──> Node C (sink)

Cross-node channels use the same StreamMessage enum as local channels.
Barriers are serialized and sent over the network alongside data events.

Network channel message format:
  [message_type: u8][payload_length: u32][payload: bytes]

  message_type:
    0x01 = Event (data batch)
    0x02 = Watermark
    0x03 = Barrier (CheckpointBarrier: 24 bytes)

Barriers maintain FIFO ordering with respect to events on each channel.
This ensures the consistent cut property across node boundaries.
```

#### Coordinator Failover

```
OLD LEADER crashes:
  - In-progress checkpoints are lost (no manifest written)
  - Nodes have partial snapshots uploaded (orphaned data)

NEW LEADER elected (via Raft):
  1. on_leader_elected() called
  2. Abort all in-progress checkpoints (send AbortCheckpoint to all nodes)
  3. Read latest committed manifest from object store
  4. Restore epoch counter from manifest
  5. Resume periodic checkpoint scheduling
  6. Orphaned partial snapshots cleaned up by next GC cycle

INVARIANT: A checkpoint is committed if and only if its manifest.json
exists in object storage. The coordinator crash cannot produce a
partially committed checkpoint because the manifest write is atomic.
```

#### Partial Failure Handling

```
Scenario: Node B crashes during checkpoint

1. Coordinator detects Node B has not sent PrepareAck within timeout
2. Options:
   a. ABORT: Cancel checkpoint, retry later
   b. PARTIAL COMMIT (if supported): Commit checkpoint without Node B's
      state, mark Node B's operators as "unsnapshotted" in manifest
      (recovery will replay from the previous checkpoint for those operators)
3. Current implementation: ABORT on any node failure
4. After Node B restarts: it will recover from the last committed checkpoint

Scenario: Node B crashes after sending PrepareAck but before commit

1. Coordinator commits the checkpoint normally (Node B's data is in object store)
2. Node B restarts and recovers from the newly committed checkpoint
3. No data loss
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `TooManyConcurrent` | Checkpoint initiated while previous is still in progress | Wait for current checkpoint to complete or abort it |
| `BarrierInjectionFailed` | Source node unreachable or crashed | Abort checkpoint, retry on next interval |
| `Timeout` | Node too slow (backpressure, large state, slow network) | Switch to unaligned mode or abort; tune timeout |
| `NodeFailed` | Node crashed during snapshot upload | Abort checkpoint; node will recover from previous checkpoint |
| `ManifestSerializationFailed` | Bug in manifest construction | Log error, abort checkpoint |
| `RpcError` | Network partition between coordinator and node | Retry RPC; if persistent, abort checkpoint |
| `CheckpointerError` | Object store error during manifest write | Retry with backoff; if persistent, abort |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Barrier injection latency (coordinator to all nodes) | < 50ms | `bench_barrier_injection_3_nodes` |
| Cross-node barrier propagation | < 10ms per hop | `bench_cross_node_barrier` |
| Distributed checkpoint (3 nodes, 100MB total state) | < 15s | Integration benchmark |
| Distributed checkpoint (10 nodes, 1GB total state) | < 60s | Integration benchmark |
| Coordinator failover recovery | < 5s | `bench_leader_failover` |
| Manifest commit (aggregate + write) | < 500ms | `bench_manifest_commit` |

## Test Plan

### Unit Tests

- [ ] `test_coordinator_creation` - Default configuration
- [ ] `test_initiate_checkpoint_generates_id` - IDs are monotonic
- [ ] `test_initiate_checkpoint_concurrency_limit` - Rejects when at max
- [ ] `test_inject_barriers_all_succeed` - All source nodes acknowledge
- [ ] `test_inject_barriers_partial_failure_aborts` - One failure aborts all
- [ ] `test_on_node_prepared_tracks_progress` - Counts prepared nodes
- [ ] `test_on_all_nodes_prepared_triggers_commit` - Auto-commit
- [ ] `test_commit_writes_manifest` - Manifest includes all node entries
- [ ] `test_abort_notifies_all_nodes` - All nodes receive abort
- [ ] `test_timeout_detection` - Timed-out checkpoints detected
- [ ] `test_unaligned_fallback_on_timeout` - Switch to unaligned before abort
- [ ] `test_leader_failover_aborts_in_progress` - Old checkpoints cleaned up
- [ ] `test_leader_failover_restores_epoch` - Epoch recovered from manifest
- [ ] `test_cluster_membership_update` - Node additions and removals

### Integration Tests

- [ ] `test_distributed_checkpoint_3_nodes` - Full checkpoint across 3 mock nodes
- [ ] `test_distributed_checkpoint_with_cross_node_channels` - Barriers cross node boundaries
- [ ] `test_node_failure_during_checkpoint` - One node crashes, checkpoint aborted
- [ ] `test_node_failure_after_prepare` - Crash after prepare, commit succeeds
- [ ] `test_coordinator_failover_and_recovery` - Leader change mid-checkpoint
- [ ] `test_consecutive_distributed_checkpoints` - Multiple checkpoints in sequence
- [ ] `test_gc_after_distributed_checkpoint` - Auto-GC runs after commit

### Benchmarks

- [ ] `bench_barrier_injection_3_nodes` - Target: < 50ms
- [ ] `bench_distributed_checkpoint_3_nodes_100mb` - Target: < 15s
- [ ] `bench_manifest_aggregation_10_nodes` - Target: < 100ms
- [ ] `bench_leader_failover` - Target: < 5s

## Rollout Plan

1. **Phase 1**: `DistributedCheckpointCoordinator` struct and configuration
2. **Phase 2**: `CheckpointRpc` trait and mock implementation
3. **Phase 3**: Phase 1a: Barrier injection via RPC
4. **Phase 4**: Phase 1b: Prepare tracking and timeout detection
5. **Phase 5**: Phase 2: Commit with manifest aggregation
6. **Phase 6**: Abort logic and error handling
7. **Phase 7**: Coordinator failover (on_leader_elected)
8. **Phase 8**: Unaligned fallback integration
9. **Phase 9**: Integration tests with mock cluster
10. **Phase 10**: Benchmarks, documentation, and code review

## Open Questions

- [ ] Should we support partial checkpoint commit (commit even if some non-critical nodes fail)? This adds complexity but improves availability.
- [ ] Should the coordinator persist its in-progress checkpoint state to the Raft log? This would allow the new leader to resume (rather than abort) an in-progress checkpoint.
- [ ] How should we handle network partitions where the coordinator can reach some nodes but not others? Options: (a) abort, (b) checkpoint reachable nodes only.
- [ ] Should cross-node barrier propagation use a dedicated RPC or piggyback on the data channel? Currently using data channels for causal ordering.
- [ ] Should we implement checkpoint cancellation (admin can cancel a running checkpoint)?

## Completion Checklist

- [ ] `DistributedCheckpointCoordinator` implemented
- [ ] `CheckpointRpc` trait defined with barrier injection, prepare, commit, abort
- [ ] Two-phase commit protocol (prepare + commit manifest)
- [ ] Barrier injection to all source nodes via RPC
- [ ] PrepareAck tracking and auto-commit
- [ ] Timeout detection and abort logic
- [ ] Unaligned fallback on timeout
- [ ] Coordinator failover handling
- [ ] Cluster membership updates
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Chandy-Lamport Algorithm](https://en.wikipedia.org/wiki/Chandy%E2%80%93Lamport_algorithm) - Theoretical foundation
- [Apache Flink Distributed Snapshots](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#checkpointing) - Production implementation
- [Two-Phase Commit Protocol](https://en.wikipedia.org/wiki/Two-phase_commit_protocol) - Commit protocol for manifest
- [Raft Consensus Algorithm](https://raft.github.io/) - Leader election for coordinator
- [F-DCKP-001: Barrier Protocol](F-DCKP-001-barrier-protocol.md) - Barrier types and injection
- [F-DCKP-004: Object Store Checkpointer](F-DCKP-004-object-store-checkpointer.md) - Storage backend
- [F-COORD-001: Coordinator Service](../F-COORD-001-coordinator.md) - Raft-based coordination
- [F-RPC-003: Inter-node RPC](../F-RPC-003-rpc.md) - Network communication layer
