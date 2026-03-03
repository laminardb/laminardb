//! Partition migration protocol for the delta.
//!
//! Implements the 11-phase migration protocol:
//! Planned → Draining → Checkpointing → Uploading → Released →
//! Downloading → Restoring → Seeking → Active | Failed | Cancelled
//!
//! The protocol is caller-driven: [`MigrationProtocol::next_action`] returns
//! the action the caller must perform, and [`MigrationProtocol::complete_phase`]
//! advances the task after the caller reports success or failure.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use crate::delta::coordination::metadata::{DeltaMetadata, MetadataLogEntry};
use crate::delta::discovery::{NodeId, NodeInfo};
use crate::delta::partition::assignment::{
    AssignmentConstraints, ConsistentHashAssigner, PartitionAssigner,
};

// ──────────────────────────── Errors ────────────────────────────

/// Errors from migration operations.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// The partition is not in the metadata.
    #[error("partition {0} not found in metadata")]
    PartitionNotFound(u32),

    /// The partition is not owned by the expected node.
    #[error("partition {partition_id} owned by {actual}, expected {expected}")]
    WrongOwner {
        /// The partition ID.
        partition_id: u32,
        /// The expected owner.
        expected: NodeId,
        /// The actual owner.
        actual: NodeId,
    },

    /// The epoch is stale.
    #[error("stale epoch for partition {partition_id}: current {current}, got {got}")]
    StaleEpoch {
        /// The partition ID.
        partition_id: u32,
        /// Current epoch.
        current: u64,
        /// Attempted epoch.
        got: u64,
    },

    /// The executor cannot accept more migrations.
    #[error("executor full: {0}")]
    ExecutorFull(String),

    /// No active nodes available for rebalance.
    #[error("no active nodes for rebalance")]
    NoActiveNodes,

    /// Migration is in a terminal state.
    #[error("migration for partition {0} is already terminal")]
    AlreadyTerminal(u32),
}

// ──────────────────────────── Phases ────────────────────────────

/// The phase of a partition migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Migration has been planned but not yet started.
    Planned,
    /// Old owner is draining in-flight events.
    Draining,
    /// Old owner is taking a checkpoint.
    Checkpointing,
    /// Old owner is uploading checkpoint to shared storage.
    Uploading,
    /// Old owner has released the partition (committed to Raft).
    Released,
    /// New owner is downloading the checkpoint.
    Downloading,
    /// New owner is restoring state from checkpoint.
    Restoring,
    /// New owner is seeking sources to resume from checkpoint offset.
    Seeking,
    /// Migration complete — new owner is active.
    Active,
    /// Migration failed.
    Failed,
    /// Migration was cancelled.
    Cancelled,
}

impl MigrationPhase {
    /// Returns `true` if the action targets the source (old owner) node.
    #[must_use]
    pub fn is_source_side(&self) -> bool {
        matches!(
            self,
            Self::Planned | Self::Draining | Self::Checkpointing | Self::Uploading
        )
    }

    /// Returns `true` if the action targets the target (new owner) node.
    #[must_use]
    pub fn is_target_side(&self) -> bool {
        matches!(
            self,
            Self::Released | Self::Downloading | Self::Restoring | Self::Seeking
        )
    }
}

impl fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planned => write!(f, "planned"),
            Self::Draining => write!(f, "draining"),
            Self::Checkpointing => write!(f, "checkpointing"),
            Self::Uploading => write!(f, "uploading"),
            Self::Released => write!(f, "released"),
            Self::Downloading => write!(f, "downloading"),
            Self::Restoring => write!(f, "restoring"),
            Self::Seeking => write!(f, "seeking"),
            Self::Active => write!(f, "active"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

// ──────────────────────────── Actions ────────────────────────────

/// Action that the caller must perform for the current migration phase.
///
/// The migration protocol is caller-driven: the protocol tells the caller
/// what to do, and the caller reports back the result via [`PhaseResult`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationAction {
    /// Source node: stop accepting new events for this partition.
    DrainPartition {
        /// The partition to drain.
        partition_id: u32,
        /// The source node that should drain.
        source_node: NodeId,
    },

    /// Source node: create a checkpoint for the partition.
    CreateCheckpoint {
        /// The partition to checkpoint.
        partition_id: u32,
        /// The epoch to checkpoint at.
        epoch: u64,
    },

    /// Source node: upload the checkpoint to shared storage (S3).
    UploadCheckpoint {
        /// The partition whose checkpoint to upload.
        partition_id: u32,
        /// The checkpoint path (provided by caller in [`PhaseResult`]).
        checkpoint_path: String,
    },

    /// Commit partition release to Raft (via [`MetadataLogEntry::ReleasePartition`]).
    /// After this, the old owner's guard is fenced.
    RaftRelease {
        /// The Raft log entry to apply.
        entry: MetadataLogEntry,
    },

    /// Target node: download the checkpoint from shared storage.
    DownloadCheckpoint {
        /// The partition to download.
        partition_id: u32,
        /// The path in shared storage.
        checkpoint_path: String,
        /// The target node that should download.
        target_node: NodeId,
    },

    /// Target node: restore state from the downloaded checkpoint.
    RestoreState {
        /// The partition to restore.
        partition_id: u32,
        /// The target node that should restore.
        target_node: NodeId,
    },

    /// Target node: seek sources to resume from the checkpoint offset.
    SeekSources {
        /// The partition to seek.
        partition_id: u32,
        /// The target node.
        target_node: NodeId,
    },

    /// Migration is complete — activate the partition on the new owner.
    Activate {
        /// The partition to activate.
        partition_id: u32,
        /// The new owner.
        target_node: NodeId,
        /// The new epoch.
        epoch: u64,
    },

    /// No action needed (terminal state).
    None,
}

/// Result reported by the caller after performing a [`MigrationAction`].
#[derive(Debug, Clone)]
pub enum PhaseResult {
    /// The action completed successfully.
    Success {
        /// Optional checkpoint path (set after Checkpointing phase).
        checkpoint_path: Option<String>,
    },

    /// The action failed.
    Failure {
        /// Error description.
        reason: String,
    },
}

impl PhaseResult {
    /// Shortcut for a success with no checkpoint path.
    #[must_use]
    pub fn ok() -> Self {
        Self::Success {
            checkpoint_path: None,
        }
    }

    /// Shortcut for a success with a checkpoint path.
    #[must_use]
    pub fn ok_with_path(path: String) -> Self {
        Self::Success {
            checkpoint_path: Some(path),
        }
    }

    /// Shortcut for a failure.
    #[must_use]
    pub fn fail(reason: impl Into<String>) -> Self {
        Self::Failure {
            reason: reason.into(),
        }
    }
}

// ──────────────────────────── Task ────────────────────────────

/// A single partition migration task.
#[derive(Debug, Clone)]
pub struct MigrationTask {
    /// The partition being migrated.
    pub partition_id: u32,
    /// The current owner (setting side).
    pub from_node: NodeId,
    /// The new owner (rising side).
    pub to_node: NodeId,
    /// The epoch for the old ownership.
    pub from_epoch: u64,
    /// The epoch for the new ownership.
    pub to_epoch: u64,
    /// Current phase.
    pub phase: MigrationPhase,
    /// Error message if phase is Failed.
    pub error: Option<String>,
    /// Checkpoint path discovered during the Checkpointing phase.
    pub checkpoint_path: Option<String>,
    /// Number of times this task has been retried.
    pub retry_count: u32,
}

impl MigrationTask {
    /// Create a new migration task.
    #[must_use]
    pub fn new(
        partition_id: u32,
        from_node: NodeId,
        to_node: NodeId,
        from_epoch: u64,
        to_epoch: u64,
    ) -> Self {
        Self {
            partition_id,
            from_node,
            to_node,
            from_epoch,
            to_epoch,
            phase: MigrationPhase::Planned,
            error: None,
            checkpoint_path: None,
            retry_count: 0,
        }
    }

    /// Advance to the next phase. Returns `true` if the transition was valid.
    pub fn advance(&mut self) -> bool {
        let next = match self.phase {
            MigrationPhase::Planned => Some(MigrationPhase::Draining),
            MigrationPhase::Draining => Some(MigrationPhase::Checkpointing),
            MigrationPhase::Checkpointing => Some(MigrationPhase::Uploading),
            MigrationPhase::Uploading => Some(MigrationPhase::Released),
            MigrationPhase::Released => Some(MigrationPhase::Downloading),
            MigrationPhase::Downloading => Some(MigrationPhase::Restoring),
            MigrationPhase::Restoring => Some(MigrationPhase::Seeking),
            MigrationPhase::Seeking => Some(MigrationPhase::Active),
            MigrationPhase::Active | MigrationPhase::Failed | MigrationPhase::Cancelled => None,
        };

        if let Some(phase) = next {
            self.phase = phase;
            true
        } else {
            false
        }
    }

    /// Mark the migration as failed with an error message.
    pub fn fail(&mut self, error: String) {
        self.phase = MigrationPhase::Failed;
        self.error = Some(error);
    }

    /// Cancel the migration.
    pub fn cancel(&mut self) {
        self.phase = MigrationPhase::Cancelled;
    }

    /// Returns `true` if the migration is in a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.phase,
            MigrationPhase::Active | MigrationPhase::Failed | MigrationPhase::Cancelled
        )
    }

    /// Generate the Raft release entry for this migration.
    #[must_use]
    pub fn release_entry(&self) -> MetadataLogEntry {
        MetadataLogEntry::ReleasePartition {
            partition_id: self.partition_id,
            epoch: self.from_epoch,
        }
    }

    /// Generate the Raft acquire entry for this migration.
    #[must_use]
    pub fn acquire_entry(&self) -> MetadataLogEntry {
        MetadataLogEntry::AcquirePartition {
            partition_id: self.partition_id,
            node_id: self.to_node,
            epoch: self.to_epoch,
        }
    }
}

// ──────────────────────────── Config ────────────────────────────

/// Configuration for the migration executor.
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Timeout for the entire migration.
    pub timeout: Duration,
    /// Maximum concurrent migrations.
    pub max_concurrent: usize,
    /// Maximum retries for a failed migration.
    pub max_retries: u32,
    /// Whether to use forced migration when the old owner is unreachable.
    pub allow_forced_migration: bool,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_concurrent: 2,
            max_retries: 3,
            allow_forced_migration: true,
        }
    }
}

// ──────────────────────────── Executor ────────────────────────────

/// Executes partition migrations.
#[derive(Debug)]
pub struct MigrationExecutor {
    config: MigrationConfig,
    active_migrations: Vec<MigrationTask>,
}

impl MigrationExecutor {
    /// Create a new migration executor.
    #[must_use]
    pub fn new(config: MigrationConfig) -> Self {
        Self {
            config,
            active_migrations: Vec::new(),
        }
    }

    /// Get the migration configuration.
    #[must_use]
    pub fn config(&self) -> &MigrationConfig {
        &self.config
    }

    /// Get active (non-terminal) migrations.
    #[must_use]
    pub fn active_migrations(&self) -> &[MigrationTask] {
        &self.active_migrations
    }

    /// Get a mutable reference to a task by partition ID.
    pub fn get_mut(&mut self, partition_id: u32) -> Option<&mut MigrationTask> {
        self.active_migrations
            .iter_mut()
            .find(|t| t.partition_id == partition_id && !t.is_terminal())
    }

    /// Submit a new migration task.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::ExecutorFull`] if the maximum concurrent
    /// migrations would be exceeded.
    pub fn submit(&mut self, task: MigrationTask) -> Result<(), MigrationError> {
        let active_count = self
            .active_migrations
            .iter()
            .filter(|t| !t.is_terminal())
            .count();
        if active_count >= self.config.max_concurrent {
            return Err(MigrationError::ExecutorFull(format!(
                "max concurrent migrations ({}) exceeded",
                self.config.max_concurrent
            )));
        }
        self.active_migrations.push(task);
        Ok(())
    }

    /// Remove all terminal migrations.
    pub fn cleanup_terminal(&mut self) {
        self.active_migrations.retain(|t| !t.is_terminal());
    }

    /// Count of non-terminal migrations.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active_migrations
            .iter()
            .filter(|t| !t.is_terminal())
            .count()
    }

    /// Check if a partition has an active (non-terminal) migration.
    #[must_use]
    pub fn has_active_migration(&self, partition_id: u32) -> bool {
        self.active_migrations
            .iter()
            .any(|t| t.partition_id == partition_id && !t.is_terminal())
    }
}

// ──────────────────────────── Protocol ────────────────────────────

/// A planned set of migrations from a rebalance operation.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// The ordered list of migration tasks (executed serially).
    pub tasks: Vec<MigrationTask>,
    /// The new epoch that all migrations will target.
    pub target_epoch: u64,
    /// Number of partitions that don't need migration.
    pub unchanged: u32,
}

impl MigrationPlan {
    /// Returns `true` if no migrations are needed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Number of migrations in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }
}

/// Orchestrates the migration protocol.
///
/// The protocol is caller-driven: call [`next_action`](Self::next_action)
/// to get the action for a task's current phase, perform it, then call
/// [`complete_phase`](Self::complete_phase) with the result.
///
/// # Example
///
/// ```
/// use laminar_core::delta::partition::migration::*;
/// use laminar_core::delta::coordination::metadata::DeltaMetadata;
/// use laminar_core::delta::discovery::NodeId;
///
/// let protocol = MigrationProtocol::new();
/// let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
///
/// // Get first action
/// let action = protocol.next_action(&task);
/// assert!(matches!(action, MigrationAction::DrainPartition { .. }));
///
/// // After performing the action, advance
/// let next = protocol.complete_phase(&mut task, PhaseResult::ok());
/// assert!(matches!(next, Ok(MigrationAction::CreateCheckpoint { .. })));
/// ```
#[derive(Debug)]
pub struct MigrationProtocol {
    assigner: ConsistentHashAssigner,
}

impl MigrationProtocol {
    /// Create a new migration protocol with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            assigner: ConsistentHashAssigner::new(),
        }
    }

    /// Create with a custom assigner.
    #[must_use]
    pub fn with_assigner(assigner: ConsistentHashAssigner) -> Self {
        Self { assigner }
    }

    /// Compute a migration plan from the current metadata and node list.
    ///
    /// Returns an ordered list of [`MigrationTask`]s to execute serially.
    /// Each task's `to_epoch` is `cluster_epoch + index + 1` to ensure
    /// monotonically increasing epochs.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::NoActiveNodes`] if no nodes are available.
    pub fn plan_rebalance(
        &self,
        metadata: &DeltaMetadata,
        nodes: &[NodeInfo],
        constraints: &AssignmentConstraints,
    ) -> Result<MigrationPlan, MigrationError> {
        let active_nodes: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| {
                matches!(
                    n.state,
                    crate::delta::discovery::NodeState::Active
                        | crate::delta::discovery::NodeState::Joining
                )
            })
            .collect();

        if active_nodes.is_empty() {
            return Err(MigrationError::NoActiveNodes);
        }

        let active_refs: Vec<NodeInfo> = active_nodes.into_iter().cloned().collect();

        // Build current assignment map from metadata
        let current: HashMap<u32, NodeId> = metadata
            .partition_map
            .iter()
            .map(|(pid, own)| (*pid, own.node_id))
            .collect();

        let plan = self.assigner.rebalance(&current, &active_refs, constraints);

        let base_epoch = metadata.cluster_epoch;
        let mut tasks = Vec::new();

        for (i, mv) in plan.moves.iter().enumerate() {
            let from_node = mv.from.unwrap_or(NodeId::UNASSIGNED);
            let from_epoch = metadata
                .partition_map
                .get(&mv.partition_id)
                .map_or(0, |o| o.epoch);

            #[allow(clippy::cast_possible_truncation)]
            let to_epoch = base_epoch + (i as u64) + 1;

            tasks.push(MigrationTask::new(
                mv.partition_id,
                from_node,
                mv.to,
                from_epoch,
                to_epoch,
            ));
        }

        #[allow(clippy::cast_possible_truncation)]
        let total = metadata.partition_map.len() as u32;
        #[allow(clippy::cast_possible_truncation)]
        let moved = tasks.len() as u32;

        Ok(MigrationPlan {
            tasks,
            target_epoch: base_epoch + u64::from(moved),
            unchanged: total.saturating_sub(moved),
        })
    }

    /// Get the action required for the task's current phase.
    ///
    /// For the `Planned` phase, this returns `DrainPartition` (the first
    /// real action). For terminal states, returns `None`.
    #[must_use]
    pub fn next_action(&self, task: &MigrationTask) -> MigrationAction {
        match task.phase {
            MigrationPhase::Planned => MigrationAction::DrainPartition {
                partition_id: task.partition_id,
                source_node: task.from_node,
            },
            MigrationPhase::Draining => MigrationAction::CreateCheckpoint {
                partition_id: task.partition_id,
                epoch: task.from_epoch,
            },
            MigrationPhase::Checkpointing => MigrationAction::UploadCheckpoint {
                partition_id: task.partition_id,
                checkpoint_path: task.checkpoint_path.clone().unwrap_or_default(),
            },
            MigrationPhase::Uploading => MigrationAction::RaftRelease {
                entry: task.release_entry(),
            },
            MigrationPhase::Released => MigrationAction::DownloadCheckpoint {
                partition_id: task.partition_id,
                checkpoint_path: task.checkpoint_path.clone().unwrap_or_default(),
                target_node: task.to_node,
            },
            MigrationPhase::Downloading => MigrationAction::RestoreState {
                partition_id: task.partition_id,
                target_node: task.to_node,
            },
            MigrationPhase::Restoring => MigrationAction::SeekSources {
                partition_id: task.partition_id,
                target_node: task.to_node,
            },
            MigrationPhase::Seeking => MigrationAction::Activate {
                partition_id: task.partition_id,
                target_node: task.to_node,
                epoch: task.to_epoch,
            },
            MigrationPhase::Active | MigrationPhase::Failed | MigrationPhase::Cancelled => {
                MigrationAction::None
            }
        }
    }

    /// Complete the current phase and advance the task.
    ///
    /// On success, the task advances to the next phase and the new action
    /// is returned. On failure, the task is marked as failed.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::AlreadyTerminal`] if the task is already
    /// in a terminal state.
    pub fn complete_phase(
        &self,
        task: &mut MigrationTask,
        result: PhaseResult,
    ) -> Result<MigrationAction, MigrationError> {
        if task.is_terminal() {
            return Err(MigrationError::AlreadyTerminal(task.partition_id));
        }

        match result {
            PhaseResult::Success { checkpoint_path } => {
                if let Some(path) = checkpoint_path {
                    task.checkpoint_path = Some(path);
                }
                task.advance();
                Ok(self.next_action(task))
            }
            PhaseResult::Failure { reason } => {
                task.fail(reason);
                Ok(MigrationAction::None)
            }
        }
    }

    /// Generate the Raft log entries needed to complete a migration.
    ///
    /// Returns the release entry (source side) and acquire entry (target side)
    /// as a pair.
    #[must_use]
    pub fn raft_entries(task: &MigrationTask) -> (MetadataLogEntry, MetadataLogEntry) {
        (task.release_entry(), task.acquire_entry())
    }

    /// Validate that a migration is safe given the current metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration references stale epochs or wrong owners.
    pub fn validate(
        &self,
        task: &MigrationTask,
        metadata: &DeltaMetadata,
    ) -> Result<(), MigrationError> {
        if let Some(ownership) = metadata.partition_map.get(&task.partition_id) {
            // For unassigned source (initial assignment), skip owner check
            if !task.from_node.is_unassigned() && ownership.node_id != task.from_node {
                return Err(MigrationError::WrongOwner {
                    partition_id: task.partition_id,
                    expected: task.from_node,
                    actual: ownership.node_id,
                });
            }
            if task.to_epoch <= ownership.epoch {
                return Err(MigrationError::StaleEpoch {
                    partition_id: task.partition_id,
                    current: ownership.epoch,
                    got: task.to_epoch,
                });
            }
        }
        Ok(())
    }
}

impl Default for MigrationProtocol {
    fn default() -> Self {
        Self::new()
    }
}

// ──────────────────────────── Tests ────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::discovery::{NodeMetadata, NodeState};

    fn make_node(id: u64, cores: u32) -> NodeInfo {
        NodeInfo {
            id: NodeId(id),
            name: format!("node-{id}"),
            rpc_address: format!("127.0.0.1:{}", 9000 + id),
            raft_address: format!("127.0.0.1:{}", 9100 + id),
            state: NodeState::Active,
            metadata: NodeMetadata {
                cores,
                ..NodeMetadata::default()
            },
            last_heartbeat_ms: 0,
        }
    }

    fn setup_metadata_2_nodes() -> DeltaMetadata {
        let mut meta = DeltaMetadata::new();
        for pid in 0..6 {
            meta.apply(&MetadataLogEntry::AssignPartition {
                partition_id: pid,
                node_id: NodeId(1),
                epoch: 1,
            });
        }
        for pid in 6..12 {
            meta.apply(&MetadataLogEntry::AssignPartition {
                partition_id: pid,
                node_id: NodeId(2),
                epoch: 1,
            });
        }
        meta
    }

    // ── Phase FSM tests ──

    #[test]
    fn test_migration_phase_display() {
        assert_eq!(MigrationPhase::Planned.to_string(), "planned");
        assert_eq!(MigrationPhase::Active.to_string(), "active");
        assert_eq!(MigrationPhase::Failed.to_string(), "failed");
        assert_eq!(MigrationPhase::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_migration_phase_sides() {
        assert!(MigrationPhase::Planned.is_source_side());
        assert!(MigrationPhase::Draining.is_source_side());
        assert!(MigrationPhase::Checkpointing.is_source_side());
        assert!(MigrationPhase::Uploading.is_source_side());
        assert!(!MigrationPhase::Released.is_source_side());

        assert!(MigrationPhase::Released.is_target_side());
        assert!(MigrationPhase::Downloading.is_target_side());
        assert!(MigrationPhase::Restoring.is_target_side());
        assert!(MigrationPhase::Seeking.is_target_side());
        assert!(!MigrationPhase::Active.is_target_side());
    }

    #[test]
    fn test_migration_task_advance() {
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        assert_eq!(task.phase, MigrationPhase::Planned);

        let phases = [
            MigrationPhase::Draining,
            MigrationPhase::Checkpointing,
            MigrationPhase::Uploading,
            MigrationPhase::Released,
            MigrationPhase::Downloading,
            MigrationPhase::Restoring,
            MigrationPhase::Seeking,
            MigrationPhase::Active,
        ];
        for expected in &phases {
            assert!(task.advance());
            assert_eq!(task.phase, *expected);
        }
        assert!(!task.advance());
    }

    #[test]
    fn test_migration_task_fail() {
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        task.advance(); // → Draining
        task.fail("timeout".into());
        assert_eq!(task.phase, MigrationPhase::Failed);
        assert!(task.is_terminal());
        assert_eq!(task.error.as_deref(), Some("timeout"));
    }

    #[test]
    fn test_migration_task_cancel() {
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        task.cancel();
        assert_eq!(task.phase, MigrationPhase::Cancelled);
        assert!(task.is_terminal());
    }

    #[test]
    fn test_migration_task_raft_entries() {
        let task = MigrationTask::new(5, NodeId(1), NodeId(2), 3, 4);
        let release = task.release_entry();
        let acquire = task.acquire_entry();

        match release {
            MetadataLogEntry::ReleasePartition {
                partition_id,
                epoch,
            } => {
                assert_eq!(partition_id, 5);
                assert_eq!(epoch, 3);
            }
            _ => panic!("expected ReleasePartition"),
        }

        match acquire {
            MetadataLogEntry::AcquirePartition {
                partition_id,
                node_id,
                epoch,
            } => {
                assert_eq!(partition_id, 5);
                assert_eq!(node_id, NodeId(2));
                assert_eq!(epoch, 4);
            }
            _ => panic!("expected AcquirePartition"),
        }
    }

    // ── Executor tests ──

    #[test]
    fn test_executor_max_concurrent() {
        let config = MigrationConfig {
            max_concurrent: 1,
            ..MigrationConfig::default()
        };
        let mut exec = MigrationExecutor::new(config);

        let task1 = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        assert!(exec.submit(task1).is_ok());

        let task2 = MigrationTask::new(1, NodeId(1), NodeId(2), 1, 2);
        assert!(exec.submit(task2).is_err());
    }

    #[test]
    fn test_executor_cleanup_terminal() {
        let mut exec = MigrationExecutor::new(MigrationConfig::default());

        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        task.fail("err".into());
        exec.active_migrations.push(task);

        exec.active_migrations
            .push(MigrationTask::new(1, NodeId(1), NodeId(2), 1, 2));

        exec.cleanup_terminal();
        assert_eq!(exec.active_migrations.len(), 1);
        assert_eq!(exec.active_migrations[0].partition_id, 1);
    }

    #[test]
    fn test_executor_active_count() {
        let mut exec = MigrationExecutor::new(MigrationConfig::default());
        assert_eq!(exec.active_count(), 0);

        exec.active_migrations
            .push(MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2));
        assert_eq!(exec.active_count(), 1);

        exec.active_migrations[0].fail("err".into());
        assert_eq!(exec.active_count(), 0);
    }

    #[test]
    fn test_executor_has_active_migration() {
        let mut exec = MigrationExecutor::new(MigrationConfig::default());
        exec.active_migrations
            .push(MigrationTask::new(5, NodeId(1), NodeId(2), 1, 2));

        assert!(exec.has_active_migration(5));
        assert!(!exec.has_active_migration(6));
    }

    #[test]
    fn test_executor_get_mut() {
        let mut exec = MigrationExecutor::new(MigrationConfig::default());
        exec.active_migrations
            .push(MigrationTask::new(5, NodeId(1), NodeId(2), 1, 2));

        let task = exec.get_mut(5).unwrap();
        task.advance();
        assert_eq!(task.phase, MigrationPhase::Draining);

        assert!(exec.get_mut(6).is_none());
    }

    // ── Protocol tests ──

    #[test]
    fn test_protocol_next_action_all_phases() {
        let protocol = MigrationProtocol::new();
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        task.checkpoint_path = Some("/cp/0".into());

        // Planned → DrainPartition
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::DrainPartition { .. }));

        task.advance(); // → Draining
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::CreateCheckpoint { .. }));

        task.advance(); // → Checkpointing
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::UploadCheckpoint { .. }));

        task.advance(); // → Uploading
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::RaftRelease { .. }));

        task.advance(); // → Released
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::DownloadCheckpoint { .. }));

        task.advance(); // → Downloading
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::RestoreState { .. }));

        task.advance(); // → Restoring
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::SeekSources { .. }));

        task.advance(); // → Seeking
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::Activate { .. }));

        task.advance(); // → Active
        let action = protocol.next_action(&task);
        assert!(matches!(action, MigrationAction::None));
    }

    #[test]
    fn test_protocol_complete_phase_success() {
        let protocol = MigrationProtocol::new();
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);

        // Complete Planned → Draining
        let action = protocol.complete_phase(&mut task, PhaseResult::ok());
        assert!(action.is_ok());
        assert_eq!(task.phase, MigrationPhase::Draining);

        // Complete Draining → Checkpointing (with checkpoint path)
        let action = protocol.complete_phase(
            &mut task,
            PhaseResult::ok_with_path("/checkpoints/p0/cp-1.rkyv".into()),
        );
        assert!(action.is_ok());
        assert_eq!(task.phase, MigrationPhase::Checkpointing);
        assert_eq!(
            task.checkpoint_path.as_deref(),
            Some("/checkpoints/p0/cp-1.rkyv")
        );
    }

    #[test]
    fn test_protocol_complete_phase_failure() {
        let protocol = MigrationProtocol::new();
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);

        let action = protocol.complete_phase(&mut task, PhaseResult::fail("network error"));
        assert!(action.is_ok());
        assert_eq!(task.phase, MigrationPhase::Failed);
        assert_eq!(task.error.as_deref(), Some("network error"));
    }

    #[test]
    fn test_protocol_complete_phase_already_terminal() {
        let protocol = MigrationProtocol::new();
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        task.fail("done".into());

        let result = protocol.complete_phase(&mut task, PhaseResult::ok());
        assert!(result.is_err());
        assert!(matches!(result, Err(MigrationError::AlreadyTerminal(0))));
    }

    #[test]
    fn test_protocol_full_migration_flow() {
        let protocol = MigrationProtocol::new();
        let mut task = MigrationTask::new(3, NodeId(1), NodeId(2), 5, 6);

        // Walk through all phases successfully
        let phases_and_actions = [
            (MigrationPhase::Draining, "DrainPartition"),
            (MigrationPhase::Checkpointing, "CreateCheckpoint"),
            (MigrationPhase::Uploading, "UploadCheckpoint"),
            (MigrationPhase::Released, "RaftRelease"),
            (MigrationPhase::Downloading, "DownloadCheckpoint"),
            (MigrationPhase::Restoring, "RestoreState"),
            (MigrationPhase::Seeking, "SeekSources"),
            (MigrationPhase::Active, "Activate"),
        ];

        for (i, (expected_phase, _)) in phases_and_actions.iter().enumerate() {
            let result = if i == 1 {
                // Checkpointing phase produces a path
                PhaseResult::ok_with_path("/cp/3/epoch-5.rkyv".into())
            } else {
                PhaseResult::ok()
            };
            let action = protocol.complete_phase(&mut task, result).unwrap();
            assert_eq!(task.phase, *expected_phase);

            if task.phase == MigrationPhase::Active {
                assert!(matches!(action, MigrationAction::None));
            }
        }

        assert!(task.is_terminal());
    }

    // ── Rebalance plan tests ──

    #[test]
    fn test_plan_rebalance_no_active_nodes() {
        let protocol = MigrationProtocol::new();
        let metadata = DeltaMetadata::new();
        let nodes: Vec<NodeInfo> = vec![];

        let result = protocol.plan_rebalance(&metadata, &nodes, &AssignmentConstraints::default());
        assert!(result.is_err());
        assert!(matches!(result, Err(MigrationError::NoActiveNodes)));
    }

    #[test]
    fn test_plan_rebalance_add_node() {
        let protocol = MigrationProtocol::new();
        let metadata = setup_metadata_2_nodes();

        // Add a third node
        let nodes = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];

        let plan = protocol
            .plan_rebalance(&metadata, &nodes, &AssignmentConstraints::default())
            .unwrap();

        // Should have some migrations (partitions moving to node 3)
        assert!(!plan.is_empty());
        // All tasks should target the new assignment
        for task in &plan.tasks {
            assert!(task.to_epoch > metadata.cluster_epoch);
        }
        // Target epoch should be cluster_epoch + number of moves
        #[allow(clippy::cast_possible_truncation)]
        let expected = metadata.cluster_epoch + plan.tasks.len() as u64;
        assert_eq!(plan.target_epoch, expected);
    }

    #[test]
    fn test_plan_rebalance_stable_cluster() {
        let protocol = MigrationProtocol::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];

        // Use the assigner's own initial assignment so it's consistent
        let assigner = ConsistentHashAssigner::new();
        let initial = assigner.initial_assignment(12, &nodes, &AssignmentConstraints::default());

        let mut metadata = DeltaMetadata::new();
        for (pid, node_id) in &initial.assignments {
            metadata.apply(&MetadataLogEntry::AssignPartition {
                partition_id: *pid,
                node_id: *node_id,
                epoch: 1,
            });
        }

        // Same two nodes — no migrations needed
        let plan = protocol
            .plan_rebalance(&metadata, &nodes, &AssignmentConstraints::default())
            .unwrap();

        assert!(plan.is_empty());
        assert_eq!(plan.unchanged, 12);
    }

    #[test]
    fn test_plan_rebalance_remove_node() {
        let protocol = MigrationProtocol::new();
        let metadata = setup_metadata_2_nodes();

        // Remove node 2 — all its partitions must move to node 1
        let nodes = vec![make_node(1, 4)];
        let plan = protocol
            .plan_rebalance(&metadata, &nodes, &AssignmentConstraints::default())
            .unwrap();

        // All of node 2's partitions should move
        for task in &plan.tasks {
            assert_eq!(task.to_node, NodeId(1));
        }
        assert_eq!(plan.tasks.len(), 6); // node 2 had 6 partitions
    }

    #[test]
    fn test_plan_rebalance_epoch_monotonic() {
        let protocol = MigrationProtocol::new();
        let metadata = setup_metadata_2_nodes();

        let nodes = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];
        let plan = protocol
            .plan_rebalance(&metadata, &nodes, &AssignmentConstraints::default())
            .unwrap();

        // Epochs must be strictly increasing
        let mut prev_epoch = metadata.cluster_epoch;
        for task in &plan.tasks {
            assert!(task.to_epoch > prev_epoch);
            prev_epoch = task.to_epoch;
        }
    }

    #[test]
    fn test_plan_filters_non_active_nodes() {
        let protocol = MigrationProtocol::new();
        let metadata = setup_metadata_2_nodes();

        let mut node3 = make_node(3, 4);
        node3.state = NodeState::Left; // Should be excluded
        let nodes = vec![make_node(1, 4), make_node(2, 4), node3];

        let plan = protocol
            .plan_rebalance(&metadata, &nodes, &AssignmentConstraints::default())
            .unwrap();

        // No partitions should be assigned to the Left node
        for task in &plan.tasks {
            assert_ne!(task.to_node, NodeId(3));
        }
    }

    // ── Validation tests ──

    #[test]
    fn test_validate_ok() {
        let protocol = MigrationProtocol::new();
        let metadata = setup_metadata_2_nodes();
        let task = MigrationTask::new(0, NodeId(1), NodeId(3), 1, 2);

        assert!(protocol.validate(&task, &metadata).is_ok());
    }

    #[test]
    fn test_validate_wrong_owner() {
        let protocol = MigrationProtocol::new();
        let metadata = setup_metadata_2_nodes();
        // Partition 0 is owned by node 1, but task says from_node is node 2
        let task = MigrationTask::new(0, NodeId(2), NodeId(3), 1, 2);

        let err = protocol.validate(&task, &metadata).unwrap_err();
        assert!(matches!(err, MigrationError::WrongOwner { .. }));
    }

    #[test]
    fn test_validate_stale_epoch() {
        let protocol = MigrationProtocol::new();
        let metadata = setup_metadata_2_nodes();
        // to_epoch (1) is not greater than current epoch (1)
        let task = MigrationTask::new(0, NodeId(1), NodeId(3), 1, 1);

        let err = protocol.validate(&task, &metadata).unwrap_err();
        assert!(matches!(err, MigrationError::StaleEpoch { .. }));
    }

    #[test]
    fn test_validate_unassigned_source_ok() {
        let protocol = MigrationProtocol::new();
        let metadata = DeltaMetadata::new();
        // Fresh partition, no existing ownership
        let task = MigrationTask::new(0, NodeId::UNASSIGNED, NodeId(1), 0, 1);

        assert!(protocol.validate(&task, &metadata).is_ok());
    }

    // ── Raft entry generation tests ──

    #[test]
    fn test_raft_entries() {
        let task = MigrationTask::new(7, NodeId(1), NodeId(2), 10, 11);
        let (release, acquire) = MigrationProtocol::raft_entries(&task);

        match release {
            MetadataLogEntry::ReleasePartition {
                partition_id,
                epoch,
            } => {
                assert_eq!(partition_id, 7);
                assert_eq!(epoch, 10);
            }
            _ => panic!("expected ReleasePartition"),
        }

        match acquire {
            MetadataLogEntry::AcquirePartition {
                partition_id,
                node_id,
                epoch,
            } => {
                assert_eq!(partition_id, 7);
                assert_eq!(node_id, NodeId(2));
                assert_eq!(epoch, 11);
            }
            _ => panic!("expected AcquirePartition"),
        }
    }

    // ── Metadata integration: full release → acquire cycle ──

    #[test]
    fn test_migration_metadata_integration() {
        let mut metadata = DeltaMetadata::new();

        // Initial assignment: partition 0 → node 1, epoch 1
        metadata.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1,
        });

        // Simulate migration: node 1 → node 2
        let task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);

        // Phase 1: Release
        metadata.apply(&task.release_entry());
        let owner = metadata.partition_owner(0).unwrap();
        assert!(owner.node_id.is_unassigned());

        // Phase 2: Acquire
        metadata.apply(&task.acquire_entry());
        let owner = metadata.partition_owner(0).unwrap();
        assert_eq!(owner.node_id, NodeId(2));
        assert_eq!(owner.epoch, 2);
    }

    #[test]
    fn test_migration_epoch_fencing() {
        let mut metadata = DeltaMetadata::new();

        metadata.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1,
        });

        // Migrate to node 2
        let task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        metadata.apply(&task.release_entry());
        metadata.apply(&task.acquire_entry());

        // Old owner tries to write with stale epoch — should be rejected
        metadata.apply(&MetadataLogEntry::AssignPartition {
            partition_id: 0,
            node_id: NodeId(1),
            epoch: 1, // stale
        });

        // Node 2 still owns it
        let owner = metadata.partition_owner(0).unwrap();
        assert_eq!(owner.node_id, NodeId(2));
        assert_eq!(owner.epoch, 2);
    }

    // ── MigrationPlan tests ──

    #[test]
    fn test_migration_plan_accessors() {
        let plan = MigrationPlan {
            tasks: vec![MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2)],
            target_epoch: 2,
            unchanged: 5,
        };
        assert!(!plan.is_empty());
        assert_eq!(plan.len(), 1);
    }

    #[test]
    fn test_empty_migration_plan() {
        let plan = MigrationPlan {
            tasks: vec![],
            target_epoch: 1,
            unchanged: 12,
        };
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
    }

    // ── PhaseResult tests ──

    #[test]
    fn test_phase_result_constructors() {
        let ok = PhaseResult::ok();
        assert!(matches!(
            ok,
            PhaseResult::Success {
                checkpoint_path: None
            }
        ));

        let ok_path = PhaseResult::ok_with_path("/cp/0".into());
        assert!(matches!(
            ok_path,
            PhaseResult::Success {
                checkpoint_path: Some(_)
            }
        ));

        let fail = PhaseResult::fail("oops");
        assert!(matches!(fail, PhaseResult::Failure { .. }));
    }

    // ── Error display tests ──

    #[test]
    fn test_migration_error_display() {
        let err = MigrationError::PartitionNotFound(5);
        assert!(err.to_string().contains("partition 5"));

        let err = MigrationError::WrongOwner {
            partition_id: 0,
            expected: NodeId(1),
            actual: NodeId(2),
        };
        assert!(err.to_string().contains("node-2"));

        let err = MigrationError::StaleEpoch {
            partition_id: 0,
            current: 5,
            got: 3,
        };
        assert!(err.to_string().contains("stale epoch"));

        let err = MigrationError::NoActiveNodes;
        assert!(err.to_string().contains("no active nodes"));
    }
}
