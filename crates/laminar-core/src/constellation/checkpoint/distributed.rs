//! Distributed checkpoint coordination using two-phase commit.
//!
//! The coordinator (running on the Raft leader) drives the checkpoint:
//! 1. Inject barriers into all nodes
//! 2. Wait for PrepareAck from all partitions
//! 3. Write aggregate manifest (atomic commit point)
//! 4. Commit checkpoint via Raft
//!
//! Coordinator failover: abort in-progress, recover epoch from manifest.

use std::collections::HashMap;
use std::time::Duration;

use crate::constellation::discovery::NodeId;

/// The phase of a distributed checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointPhase {
    /// Injecting barriers into source operators on all nodes.
    InjectingBarriers,
    /// Waiting for all partitions to prepare their snapshots.
    WaitingForPrepare,
    /// Writing the aggregate manifest and committing.
    Committing,
    /// Checkpoint completed successfully.
    Completed,
    /// Checkpoint was aborted due to failure or timeout.
    Aborted,
}

/// Configuration for distributed checkpoint coordination.
#[derive(Debug, Clone)]
pub struct DistributedCheckpointConfig {
    /// Maximum time to wait for all prepare acks.
    pub prepare_timeout: Duration,
    /// Maximum concurrent distributed checkpoints (typically 1).
    pub max_concurrent: usize,
    /// Whether to fall back to unaligned checkpoints on timeout.
    pub unaligned_fallback: bool,
    /// Threshold before switching to unaligned mode.
    pub unaligned_threshold: Duration,
    /// Interval between checkpoint initiation.
    pub checkpoint_interval: Duration,
}

impl Default for DistributedCheckpointConfig {
    fn default() -> Self {
        Self {
            prepare_timeout: Duration::from_secs(30),
            max_concurrent: 1,
            unaligned_fallback: true,
            unaligned_threshold: Duration::from_secs(10),
            checkpoint_interval: Duration::from_secs(60),
        }
    }
}

/// Trait for checkpoint RPC operations.
///
/// Abstracted to allow testing without real gRPC.
#[allow(async_fn_in_trait)]
pub trait CheckpointRpc: Send + Sync {
    /// Inject a barrier on a remote node.
    async fn inject_barrier(
        &self,
        node_id: NodeId,
        checkpoint_id: u64,
        epoch: u64,
        flags: u64,
        partitions: Vec<u32>,
    ) -> Result<Vec<u32>, String>;

    /// Get the prepare status for a checkpoint on a remote node.
    async fn prepare_checkpoint(
        &self,
        node_id: NodeId,
        checkpoint_id: u64,
    ) -> Result<bool, String>;

    /// Commit a checkpoint on a remote node.
    async fn commit_checkpoint(
        &self,
        node_id: NodeId,
        checkpoint_id: u64,
        manifest_path: String,
    ) -> Result<(), String>;

    /// Abort a checkpoint on a remote node.
    async fn abort_checkpoint(
        &self,
        node_id: NodeId,
        checkpoint_id: u64,
    ) -> Result<(), String>;
}

/// Progress of a single checkpoint across the cluster.
#[derive(Debug)]
pub struct CheckpointProgress {
    /// Checkpoint ID.
    pub checkpoint_id: u64,
    /// Current phase.
    pub phase: CheckpointPhase,
    /// Epoch when this checkpoint was initiated.
    pub epoch: u64,
    /// Node → list of partitions that have prepared.
    pub prepared: HashMap<NodeId, Vec<u32>>,
    /// Nodes that failed to prepare.
    pub failed_nodes: Vec<NodeId>,
    /// Start timestamp.
    pub started_at_ms: i64,
}

/// Distributed checkpoint coordinator (runs on Raft leader).
#[derive(Debug)]
pub struct DistributedCheckpointCoordinator {
    config: DistributedCheckpointConfig,
    /// Active checkpoints being coordinated.
    active: Vec<CheckpointProgress>,
    /// Next checkpoint ID to use.
    next_checkpoint_id: u64,
}

impl DistributedCheckpointCoordinator {
    /// Create a new coordinator.
    #[must_use]
    pub fn new(config: DistributedCheckpointConfig) -> Self {
        Self {
            config,
            active: Vec::new(),
            next_checkpoint_id: 1,
        }
    }

    /// Get the coordinator configuration.
    #[must_use]
    pub fn config(&self) -> &DistributedCheckpointConfig {
        &self.config
    }

    /// Get the active checkpoints.
    #[must_use]
    pub fn active_checkpoints(&self) -> &[CheckpointProgress] {
        &self.active
    }

    /// Initiate a new distributed checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the maximum concurrent checkpoints is exceeded.
    pub fn initiate(&mut self, epoch: u64) -> Result<u64, String> {
        let active_count = self
            .active
            .iter()
            .filter(|c| {
                !matches!(
                    c.phase,
                    CheckpointPhase::Completed | CheckpointPhase::Aborted
                )
            })
            .count();

        if active_count >= self.config.max_concurrent {
            return Err(format!(
                "max concurrent checkpoints ({}) exceeded",
                self.config.max_concurrent
            ));
        }

        let id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;

        self.active.push(CheckpointProgress {
            checkpoint_id: id,
            phase: CheckpointPhase::InjectingBarriers,
            epoch,
            prepared: HashMap::new(),
            failed_nodes: Vec::new(),
            started_at_ms: chrono::Utc::now().timestamp_millis(),
        });

        Ok(id)
    }

    /// Record that a node has prepared its partitions.
    pub fn record_prepare(
        &mut self,
        checkpoint_id: u64,
        node_id: NodeId,
        partitions: Vec<u32>,
    ) {
        if let Some(progress) = self
            .active
            .iter_mut()
            .find(|c| c.checkpoint_id == checkpoint_id)
        {
            progress.prepared.insert(node_id, partitions);
        }
    }

    /// Mark a checkpoint as completed.
    pub fn mark_completed(&mut self, checkpoint_id: u64) {
        if let Some(progress) = self
            .active
            .iter_mut()
            .find(|c| c.checkpoint_id == checkpoint_id)
        {
            progress.phase = CheckpointPhase::Completed;
        }
    }

    /// Abort a checkpoint.
    pub fn abort(&mut self, checkpoint_id: u64) {
        if let Some(progress) = self
            .active
            .iter_mut()
            .find(|c| c.checkpoint_id == checkpoint_id)
        {
            progress.phase = CheckpointPhase::Aborted;
        }
    }

    /// Remove completed/aborted checkpoints.
    pub fn cleanup(&mut self) {
        self.active.retain(|c| {
            !matches!(
                c.phase,
                CheckpointPhase::Completed | CheckpointPhase::Aborted
            )
        });
    }
}

impl Default for DistributedCheckpointCoordinator {
    fn default() -> Self {
        Self::new(DistributedCheckpointConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_phase_display() {
        assert_eq!(
            format!("{:?}", CheckpointPhase::InjectingBarriers),
            "InjectingBarriers"
        );
    }

    #[test]
    fn test_config_default() {
        let config = DistributedCheckpointConfig::default();
        assert_eq!(config.max_concurrent, 1);
        assert!(config.unaligned_fallback);
    }

    #[test]
    fn test_coordinator_initiate() {
        let mut coord = DistributedCheckpointCoordinator::default();
        let id = coord.initiate(1).unwrap();
        assert_eq!(id, 1);
        assert_eq!(coord.active_checkpoints().len(), 1);
        assert_eq!(
            coord.active_checkpoints()[0].phase,
            CheckpointPhase::InjectingBarriers
        );
    }

    #[test]
    fn test_coordinator_max_concurrent() {
        let mut coord = DistributedCheckpointCoordinator::new(
            DistributedCheckpointConfig {
                max_concurrent: 1,
                ..DistributedCheckpointConfig::default()
            },
        );
        coord.initiate(1).unwrap();
        assert!(coord.initiate(2).is_err());
    }

    #[test]
    fn test_coordinator_record_prepare() {
        let mut coord = DistributedCheckpointCoordinator::default();
        let id = coord.initiate(1).unwrap();

        coord.record_prepare(id, NodeId(1), vec![0, 1, 2]);
        coord.record_prepare(id, NodeId(2), vec![3, 4, 5]);

        let progress = &coord.active_checkpoints()[0];
        assert_eq!(progress.prepared.len(), 2);
    }

    #[test]
    fn test_coordinator_complete_and_cleanup() {
        let mut coord = DistributedCheckpointCoordinator::default();
        let id = coord.initiate(1).unwrap();

        coord.mark_completed(id);
        assert_eq!(
            coord.active_checkpoints()[0].phase,
            CheckpointPhase::Completed
        );

        coord.cleanup();
        assert!(coord.active_checkpoints().is_empty());
    }

    #[test]
    fn test_coordinator_abort() {
        let mut coord = DistributedCheckpointCoordinator::default();
        let id = coord.initiate(1).unwrap();

        coord.abort(id);
        assert_eq!(
            coord.active_checkpoints()[0].phase,
            CheckpointPhase::Aborted
        );
    }

    #[test]
    fn test_coordinator_sequential_ids() {
        let mut coord = DistributedCheckpointCoordinator::default();
        let id1 = coord.initiate(1).unwrap();
        coord.mark_completed(id1);
        coord.cleanup();

        let id2 = coord.initiate(2).unwrap();
        assert_eq!(id2, id1 + 1);
    }
}
