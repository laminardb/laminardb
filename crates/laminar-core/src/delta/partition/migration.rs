//! Partition migration protocol for the delta.
//!
//! Implements the 11-phase migration protocol:
//! Planned → Draining → Checkpointing → Uploading → Released →
//! Downloading → Restoring → Seeking → Active | Failed | Cancelled

use std::fmt;
use std::time::Duration;

use crate::delta::discovery::NodeId;

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
}

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

    /// Submit a new migration task.
    ///
    /// # Errors
    ///
    /// Returns an error if the maximum concurrent migrations would be exceeded.
    pub fn submit(&mut self, task: MigrationTask) -> Result<(), String> {
        let active_count = self
            .active_migrations
            .iter()
            .filter(|t| !t.is_terminal())
            .count();
        if active_count >= self.config.max_concurrent {
            return Err(format!(
                "max concurrent migrations ({}) exceeded",
                self.config.max_concurrent
            ));
        }
        self.active_migrations.push(task);
        Ok(())
    }

    /// Remove all terminal migrations.
    pub fn cleanup_terminal(&mut self) {
        self.active_migrations.retain(|t| !t.is_terminal());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_phase_display() {
        assert_eq!(MigrationPhase::Planned.to_string(), "planned");
        assert_eq!(MigrationPhase::Active.to_string(), "active");
    }

    #[test]
    fn test_migration_task_advance() {
        let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);
        assert_eq!(task.phase, MigrationPhase::Planned);

        // Walk through all phases
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

        // Can't advance past Active
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
}
