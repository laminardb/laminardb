//! Partition migration protocol for cluster rebalancing.
//!
//! When partitions are reassigned (node join/leave), this module
//! orchestrates the transfer of partition state from the old owner
//! to the new owner:
//!
//! 1. **Pause**: Old owner pauses processing for the partition
//! 2. **Snapshot**: Old owner serializes partition state to bytes
//! 3. **Transfer**: State is sent to the new owner (via gRPC)
//! 4. **Restore**: New owner deserializes and resumes processing
//! 5. **Fence**: Epoch guards prevent split-brain during transfer
//!
//! The protocol is coordinated by the `DeltaManager` and uses
//! `EpochGuard` fencing to ensure exactly-once ownership transitions.

#![allow(clippy::disallowed_types)] // cold path: migration coordination
use std::collections::HashMap;
use std::fmt;

use super::assignment::PartitionMove;
use super::guard::{EpochError, PartitionGuardSet};
use crate::delta::discovery::NodeId;

/// Current phase of a partition migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Migration has been planned but not started.
    Planned,
    /// Old owner is pausing processing and taking a snapshot.
    Pausing,
    /// State snapshot is being transferred to the new owner.
    Transferring,
    /// New owner is restoring state from the snapshot.
    Restoring,
    /// Migration completed successfully.
    Completed,
    /// Migration failed and was rolled back.
    Failed,
}

impl fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planned => write!(f, "planned"),
            Self::Pausing => write!(f, "pausing"),
            Self::Transferring => write!(f, "transferring"),
            Self::Restoring => write!(f, "restoring"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// A snapshot of partition state ready for transfer.
#[derive(Debug, Clone)]
pub struct PartitionSnapshot {
    /// The partition being migrated.
    pub partition_id: u32,
    /// Epoch at which the snapshot was taken.
    pub epoch: u64,
    /// The node that created the snapshot.
    pub source_node: NodeId,
    /// Serialized state data (Arrow IPC encoded batches).
    pub state_data: Vec<u8>,
    /// Number of state entries in the snapshot.
    pub entry_count: u64,
}

impl PartitionSnapshot {
    /// Size of the state data in bytes.
    #[must_use]
    pub fn data_size(&self) -> usize {
        self.state_data.len()
    }
}

/// Errors specific to partition migration.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    /// The partition is not owned by this node.
    #[error("partition {0} not owned by this node")]
    NotOwned(u32),

    /// Epoch fence check failed during migration.
    #[error("epoch fence error: {0}")]
    EpochFence(#[from] EpochError),

    /// The migration was cancelled (e.g., another rebalance started).
    #[error("migration cancelled for partition {0}")]
    Cancelled(u32),

    /// State serialization failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// State deserialization failed.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// Transfer to the target node failed.
    #[error("transfer failed to {target}: {reason}")]
    TransferFailed {
        /// The target node.
        target: NodeId,
        /// Reason for failure.
        reason: String,
    },

    /// Invalid state transition.
    #[error("invalid migration transition from {from} to {to}")]
    InvalidTransition {
        /// Current phase.
        from: MigrationPhase,
        /// Attempted phase.
        to: MigrationPhase,
    },

    /// Target tried to complete migration without receiving the snapshot.
    #[error("snapshot not received for partition {0}")]
    SnapshotNotReceived(u32),

    /// Epoch not assigned for a partition move.
    #[error("no epoch assigned for partition {0}")]
    MissingEpoch(u32),

    /// Source tried to complete before target confirmed restore.
    #[error("target has not confirmed restore for partition {0}")]
    NotRestoredOnTarget(u32),
}

/// Tracks the state of an in-progress partition migration.
#[derive(Debug)]
pub struct MigrationState {
    /// The partition move being executed.
    pub partition_move: PartitionMove,
    /// Current migration phase.
    pub phase: MigrationPhase,
    /// New epoch assigned for this migration.
    pub new_epoch: u64,
    /// Snapshot taken from the old owner (populated during Pausing phase).
    pub snapshot: Option<PartitionSnapshot>,
    /// Whether the target node has received the snapshot data.
    /// Must be `true` before `complete_migration` succeeds on the target side.
    pub snapshot_received: bool,
    /// Whether the target node has confirmed state restoration.
    /// Must be `true` before the *source* can complete the migration.
    pub target_restore_acked: bool,
}

impl MigrationState {
    /// Create a new migration state from a planned move.
    #[must_use]
    pub fn new(partition_move: PartitionMove, new_epoch: u64) -> Self {
        Self {
            partition_move,
            phase: MigrationPhase::Planned,
            new_epoch,
            snapshot: None,
            snapshot_received: false,
            target_restore_acked: false,
        }
    }

    /// Transition to the next phase.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::InvalidTransition`] if the transition is not valid.
    pub fn transition(&mut self, next: MigrationPhase) -> Result<(), MigrationError> {
        let valid = matches!(
            (self.phase, next),
            (MigrationPhase::Planned, MigrationPhase::Pausing)
                | (MigrationPhase::Pausing, MigrationPhase::Transferring)
                | (MigrationPhase::Transferring, MigrationPhase::Restoring)
                | (MigrationPhase::Restoring, MigrationPhase::Completed)
                // Allow failure from any non-terminal phase
                | (
                    MigrationPhase::Planned
                        | MigrationPhase::Pausing
                        | MigrationPhase::Transferring
                        | MigrationPhase::Restoring,
                    MigrationPhase::Failed,
                )
        );

        if valid {
            self.phase = next;
            Ok(())
        } else {
            Err(MigrationError::InvalidTransition {
                from: self.phase,
                to: next,
            })
        }
    }
}

/// Orchestrates partition migrations for a single node.
///
/// Tracks in-flight migrations and coordinates the snapshot/transfer
/// protocol with epoch fencing.
#[derive(Debug)]
pub struct MigrationCoordinator {
    node_id: NodeId,
    /// In-flight migrations keyed by partition ID.
    active_migrations: HashMap<u32, MigrationState>,
}

impl MigrationCoordinator {
    /// Create a new migration coordinator.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            active_migrations: HashMap::new(),
        }
    }

    /// Plan migrations from a set of partition moves with globally assigned epochs.
    ///
    /// The `epochs` map provides the epoch for each partition move, assigned by the
    /// Raft leader to ensure global consistency. Only moves that involve this node
    /// (as source or target) are tracked.
    ///
    /// # Errors
    ///
    /// Returns [`MigrationError::MissingEpoch`] if any relevant partition move
    /// lacks an epoch in the provided map.
    pub fn plan_migrations(
        &mut self,
        moves: &[PartitionMove],
        epochs: &HashMap<u32, u64>,
    ) -> Result<(), MigrationError> {
        for mv in moves {
            if mv.from == Some(self.node_id) || mv.to == self.node_id {
                let epoch = epochs
                    .get(&mv.partition_id)
                    .copied()
                    .ok_or(MigrationError::MissingEpoch(mv.partition_id))?;
                self.active_migrations
                    .insert(mv.partition_id, MigrationState::new(mv.clone(), epoch));
            }
        }
        Ok(())
    }

    /// Begin the pause phase for a partition this node is giving up.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition is not in the Planned phase or
    /// this node is not the source.
    pub fn begin_pause(
        &mut self,
        partition_id: u32,
        guards: &PartitionGuardSet,
    ) -> Result<(), MigrationError> {
        let state = self
            .active_migrations
            .get_mut(&partition_id)
            .ok_or(MigrationError::NotOwned(partition_id))?;

        if state.partition_move.from != Some(self.node_id) {
            return Err(MigrationError::NotOwned(partition_id));
        }

        // Verify we still own the partition via epoch guard.
        guards.check(partition_id)?;
        state.transition(MigrationPhase::Pausing)?;
        Ok(())
    }

    /// Record a snapshot taken during the pause phase.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration is not in the Pausing phase.
    pub fn record_snapshot(
        &mut self,
        partition_id: u32,
        snapshot: PartitionSnapshot,
    ) -> Result<(), MigrationError> {
        let state = self
            .active_migrations
            .get_mut(&partition_id)
            .ok_or(MigrationError::NotOwned(partition_id))?;

        state.snapshot = Some(snapshot);
        state.transition(MigrationPhase::Transferring)?;
        Ok(())
    }

    /// Mark transfer as complete and begin restore on target.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration is not in the Transferring phase.
    pub fn begin_restore(&mut self, partition_id: u32) -> Result<(), MigrationError> {
        let state = self
            .active_migrations
            .get_mut(&partition_id)
            .ok_or(MigrationError::NotOwned(partition_id))?;

        state.transition(MigrationPhase::Restoring)?;
        Ok(())
    }

    /// Record that the target has successfully restored partition state.
    ///
    /// This must be called (typically via an RPC ack from the target node)
    /// before the source can complete its side of the migration.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration is not being tracked.
    pub fn ack_target_restore(&mut self, partition_id: u32) -> Result<(), MigrationError> {
        let state = self
            .active_migrations
            .get_mut(&partition_id)
            .ok_or(MigrationError::NotOwned(partition_id))?;
        state.target_restore_acked = true;
        Ok(())
    }

    /// Complete a migration, applying the epoch change to guards.
    ///
    /// On the target side (`partition_move.to == self.node_id`), the snapshot
    /// must have been received (`snapshot_received == true`) before completion
    /// is allowed.
    ///
    /// On the source side (`partition_move.from == Some(self.node_id)`), the
    /// target must have acknowledged restore (`target_restore_acked == true`)
    /// before the source gives up the partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration is not in the Restoring phase,
    /// the target has not received the snapshot, or the target has not
    /// confirmed restore (source side).
    pub fn complete_migration(
        &mut self,
        partition_id: u32,
        guards: &mut PartitionGuardSet,
    ) -> Result<u64, MigrationError> {
        let state = self
            .active_migrations
            .get_mut(&partition_id)
            .ok_or(MigrationError::NotOwned(partition_id))?;

        // Target must have received the snapshot before completing.
        if state.partition_move.to == self.node_id && !state.snapshot_received {
            return Err(MigrationError::SnapshotNotReceived(partition_id));
        }

        // Source must have received target restore ack before giving up ownership.
        if state.partition_move.from == Some(self.node_id) && !state.target_restore_acked {
            return Err(MigrationError::NotRestoredOnTarget(partition_id));
        }

        state.transition(MigrationPhase::Completed)?;
        let new_epoch = state.new_epoch;

        // Update guards based on role.
        if state.partition_move.from == Some(self.node_id) {
            // We gave up this partition — fence and remove the guard.
            guards.update_epoch(partition_id, new_epoch);
            guards.remove(partition_id);
        }
        if state.partition_move.to == self.node_id {
            // We received this partition — install a new guard.
            guards.insert(partition_id, new_epoch);
        }

        // Remove completed migration.
        self.active_migrations.remove(&partition_id);
        Ok(new_epoch)
    }

    /// Mark a migration as failed and clean up.
    pub fn fail_migration(&mut self, partition_id: u32) {
        if let Some(state) = self.active_migrations.get_mut(&partition_id) {
            let _ = state.transition(MigrationPhase::Failed);
        }
        self.active_migrations.remove(&partition_id);
    }

    /// Get the current state of a migration.
    #[must_use]
    pub fn migration_state(&self, partition_id: u32) -> Option<&MigrationState> {
        self.active_migrations.get(&partition_id)
    }

    /// Get mutable access to a migration state (for coordinated phase transitions).
    pub fn migration_state_mut(&mut self, partition_id: u32) -> Option<&mut MigrationState> {
        self.active_migrations.get_mut(&partition_id)
    }

    /// Number of active (in-flight) migrations.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.active_migrations.len()
    }

    /// Whether any migrations are in progress.
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.active_migrations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::partition::assignment::PartitionMove;

    #[test]
    fn test_migration_phase_display() {
        assert_eq!(MigrationPhase::Planned.to_string(), "planned");
        assert_eq!(MigrationPhase::Transferring.to_string(), "transferring");
        assert_eq!(MigrationPhase::Completed.to_string(), "completed");
        assert_eq!(MigrationPhase::Failed.to_string(), "failed");
    }

    #[test]
    fn test_migration_state_transitions() {
        let mv = PartitionMove {
            partition_id: 0,
            from: Some(NodeId(1)),
            to: NodeId(2),
        };
        let mut state = MigrationState::new(mv, 5);
        assert_eq!(state.phase, MigrationPhase::Planned);

        assert!(state.transition(MigrationPhase::Pausing).is_ok());
        assert_eq!(state.phase, MigrationPhase::Pausing);

        assert!(state.transition(MigrationPhase::Transferring).is_ok());
        assert!(state.transition(MigrationPhase::Restoring).is_ok());
        assert!(state.transition(MigrationPhase::Completed).is_ok());
    }

    #[test]
    fn test_invalid_transition() {
        let mv = PartitionMove {
            partition_id: 0,
            from: Some(NodeId(1)),
            to: NodeId(2),
        };
        let mut state = MigrationState::new(mv, 5);
        // Can't skip Pausing
        assert!(state.transition(MigrationPhase::Transferring).is_err());
    }

    #[test]
    fn test_fail_from_any_phase() {
        let mv = PartitionMove {
            partition_id: 0,
            from: Some(NodeId(1)),
            to: NodeId(2),
        };
        let mut state = MigrationState::new(mv, 5);
        assert!(state.transition(MigrationPhase::Failed).is_ok());
    }

    #[test]
    fn test_coordinator_plan_migrations() {
        let mut coord = MigrationCoordinator::new(NodeId(1));
        let moves = vec![
            PartitionMove {
                partition_id: 0,
                from: Some(NodeId(1)),
                to: NodeId(2),
            },
            PartitionMove {
                partition_id: 1,
                from: Some(NodeId(3)),
                to: NodeId(4),
            }, // not our concern
            PartitionMove {
                partition_id: 2,
                from: Some(NodeId(2)),
                to: NodeId(1),
            },
        ];
        let mut epochs = HashMap::new();
        epochs.insert(0, 10);
        epochs.insert(1, 11);
        epochs.insert(2, 12);
        coord.plan_migrations(&moves, &epochs).unwrap();
        // Only partitions 0 (source) and 2 (target) involve node 1.
        assert_eq!(coord.active_count(), 2);
        assert!(coord.migration_state(0).is_some());
        assert!(coord.migration_state(1).is_none());
        assert!(coord.migration_state(2).is_some());
        // Verify epochs come from the map, not generated locally.
        assert_eq!(coord.migration_state(0).unwrap().new_epoch, 10);
        assert_eq!(coord.migration_state(2).unwrap().new_epoch, 12);
    }

    #[test]
    fn test_coordinator_full_lifecycle_source() {
        let mut coord = MigrationCoordinator::new(NodeId(1));
        let mut guards = PartitionGuardSet::new(NodeId(1));
        guards.insert(5, 9); // we own partition 5 at epoch 9

        let moves = vec![PartitionMove {
            partition_id: 5,
            from: Some(NodeId(1)),
            to: NodeId(2),
        }];
        let mut epochs = HashMap::new();
        epochs.insert(5, 10);
        coord.plan_migrations(&moves, &epochs).unwrap();

        // Begin pause
        assert!(coord.begin_pause(5, &guards).is_ok());

        // Record snapshot
        let snapshot = PartitionSnapshot {
            partition_id: 5,
            epoch: 9,
            source_node: NodeId(1),
            state_data: vec![1, 2, 3],
            entry_count: 1,
        };
        assert!(coord.record_snapshot(5, snapshot).is_ok());

        // Begin restore (would be on target, but we simulate locally)
        assert!(coord.begin_restore(5).is_ok());

        // Ack target restore (simulates RPC ack from target node)
        assert!(coord.ack_target_restore(5).is_ok());

        // Complete (source side — snapshot_received not required, but target ack is)
        let new_epoch = coord.complete_migration(5, &mut guards).unwrap();
        assert_eq!(new_epoch, 10);
        assert!(guards.get(5).is_none()); // guard removed
        assert!(coord.is_idle());
    }

    #[test]
    fn test_coordinator_full_lifecycle_target() {
        let mut coord = MigrationCoordinator::new(NodeId(2));
        let mut guards = PartitionGuardSet::new(NodeId(2));

        let moves = vec![PartitionMove {
            partition_id: 5,
            from: Some(NodeId(1)),
            to: NodeId(2),
        }];
        let mut epochs = HashMap::new();
        epochs.insert(5, 10);
        coord.plan_migrations(&moves, &epochs).unwrap();

        // Walk target through phases.
        let state = coord.active_migrations.get_mut(&5).unwrap();
        state.transition(MigrationPhase::Pausing).unwrap();
        state.transition(MigrationPhase::Transferring).unwrap();
        state.transition(MigrationPhase::Restoring).unwrap();
        // Mark snapshot as received (required for target completion).
        state.snapshot_received = true;

        let new_epoch = coord.complete_migration(5, &mut guards).unwrap();
        assert_eq!(new_epoch, 10);
        assert!(guards.get(5).is_some()); // guard installed
        assert!(guards.check(5).is_ok());
    }

    #[test]
    fn test_target_complete_without_snapshot_fails() {
        let mut coord = MigrationCoordinator::new(NodeId(2));
        let mut guards = PartitionGuardSet::new(NodeId(2));

        let moves = vec![PartitionMove {
            partition_id: 5,
            from: Some(NodeId(1)),
            to: NodeId(2),
        }];
        let mut epochs = HashMap::new();
        epochs.insert(5, 10);
        coord.plan_migrations(&moves, &epochs).unwrap();

        let state = coord.active_migrations.get_mut(&5).unwrap();
        state.transition(MigrationPhase::Pausing).unwrap();
        state.transition(MigrationPhase::Transferring).unwrap();
        state.transition(MigrationPhase::Restoring).unwrap();
        // Do NOT set snapshot_received

        let result = coord.complete_migration(5, &mut guards);
        assert!(result.is_err());
        match result.unwrap_err() {
            MigrationError::SnapshotNotReceived(pid) => assert_eq!(pid, 5),
            other => panic!("expected SnapshotNotReceived, got: {other}"),
        }
    }

    #[test]
    fn test_coordinator_fail_migration() {
        let mut coord = MigrationCoordinator::new(NodeId(1));
        let moves = vec![PartitionMove {
            partition_id: 3,
            from: Some(NodeId(1)),
            to: NodeId(2),
        }];
        let mut epochs = HashMap::new();
        epochs.insert(3, 10);
        coord.plan_migrations(&moves, &epochs).unwrap();
        coord.fail_migration(3);
        assert!(coord.is_idle());
    }

    #[test]
    fn test_snapshot_data_size() {
        let snapshot = PartitionSnapshot {
            partition_id: 0,
            epoch: 1,
            source_node: NodeId(1),
            state_data: vec![0u8; 1024],
            entry_count: 10,
        };
        assert_eq!(snapshot.data_size(), 1024);
    }
}
