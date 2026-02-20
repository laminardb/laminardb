//! Snapshot and recovery management for DAG checkpoints.
//!
//! [`DagCheckpointSnapshot`] captures operator state at a point-in-time.
//! It uses `std::collections::HashMap` (not `FxHashMap`) because it must
//! be `Serialize`/`Deserialize` for persistence by the caller.
//!
//! [`DagRecoveryManager`] holds snapshots and provides recovery APIs.

use std::collections::HashMap;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::operator::OperatorState;

use super::error::DagError;
use super::topology::NodeId;

/// Serializable form of [`OperatorState`].
///
/// Uses standard library types so it can derive `Serialize`/`Deserialize`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableOperatorState {
    /// Operator identifier.
    pub operator_id: String,
    /// Serialized state data.
    pub data: Vec<u8>,
}

impl From<OperatorState> for SerializableOperatorState {
    fn from(state: OperatorState) -> Self {
        Self {
            operator_id: state.operator_id,
            data: state.data,
        }
    }
}

impl From<SerializableOperatorState> for OperatorState {
    fn from(state: SerializableOperatorState) -> Self {
        Self {
            operator_id: state.operator_id,
            data: state.data,
        }
    }
}

/// A point-in-time snapshot of the entire DAG's operator state.
///
/// Produced by [`DagCheckpointCoordinator::finalize_checkpoint()`](super::checkpoint::DagCheckpointCoordinator::finalize_checkpoint).
/// The snapshot is serializable — persistence is the caller's responsibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagCheckpointSnapshot {
    /// Unique checkpoint identifier.
    pub checkpoint_id: u64,
    /// Monotonically increasing epoch.
    pub epoch: u64,
    /// Timestamp when the checkpoint was triggered (millis since epoch).
    pub timestamp: i64,
    /// Per-node operator state, keyed by `NodeId.0`.
    pub node_states: HashMap<u32, SerializableOperatorState>,
    /// Per-source offset tracking (source name → offset).
    pub source_offsets: HashMap<String, u64>,
    /// Watermark at checkpoint time.
    pub watermark: Option<i64>,
}

impl DagCheckpointSnapshot {
    /// Creates a snapshot from a map of operator states.
    ///
    /// Converts from `FxHashMap<NodeId, OperatorState>` (internal) to
    /// `HashMap<u32, SerializableOperatorState>` (serializable).
    pub(crate) fn from_operator_states(
        checkpoint_id: u64,
        epoch: u64,
        timestamp: i64,
        states: &FxHashMap<NodeId, OperatorState>,
    ) -> Self {
        let node_states = states
            .iter()
            .map(|(node_id, state)| (node_id.0, SerializableOperatorState::from(state.clone())))
            .collect();

        Self {
            checkpoint_id,
            epoch,
            timestamp,
            node_states,
            source_offsets: HashMap::new(),
            watermark: None,
        }
    }

    /// Converts node states back to `FxHashMap<NodeId, OperatorState>`.
    #[must_use]
    pub fn to_operator_states(&self) -> FxHashMap<NodeId, OperatorState> {
        self.node_states
            .iter()
            .map(|(&id, state)| (NodeId(id), OperatorState::from(state.clone())))
            .collect()
    }
}

/// Recovered DAG state from a checkpoint snapshot.
pub struct RecoveredDagState {
    /// The snapshot that was used for recovery.
    pub snapshot: DagCheckpointSnapshot,
    /// Operator states converted back to internal representation.
    pub operator_states: FxHashMap<NodeId, OperatorState>,
    /// Source offsets for resuming consumption.
    pub source_offsets: HashMap<String, u64>,
    /// Watermark at the time of the checkpoint.
    pub watermark: Option<i64>,
}

impl std::fmt::Debug for RecoveredDagState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveredDagState")
            .field("checkpoint_id", &self.snapshot.checkpoint_id)
            .field("epoch", &self.snapshot.epoch)
            .field("operator_count", &self.operator_states.len())
            .field("source_offsets", &self.source_offsets)
            .field("watermark", &self.watermark)
            .finish()
    }
}

/// Manages checkpoint snapshots and provides recovery.
///
/// Snapshots are held in memory. Persistence to disk or object storage
/// is the caller's responsibility.
#[derive(Debug)]
pub struct DagRecoveryManager {
    /// Stored snapshots, ordered by `checkpoint_id`.
    snapshots: Vec<DagCheckpointSnapshot>,
}

impl DagRecoveryManager {
    /// Creates an empty recovery manager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshots: Vec::new(),
        }
    }

    /// Creates a recovery manager pre-loaded with snapshots.
    #[must_use]
    pub fn with_snapshots(snapshots: Vec<DagCheckpointSnapshot>) -> Self {
        Self { snapshots }
    }

    /// Adds a snapshot to the manager.
    pub fn add_snapshot(&mut self, snapshot: DagCheckpointSnapshot) {
        self.snapshots.push(snapshot);
    }

    /// Recovers from the latest (highest `checkpoint_id`) snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`DagError::CheckpointNotFound`] if no snapshots exist.
    pub fn recover_latest(&self) -> Result<RecoveredDagState, DagError> {
        let snapshot = self
            .snapshots
            .iter()
            .max_by_key(|s| s.checkpoint_id)
            .ok_or(DagError::CheckpointNotFound)?
            .clone();

        Ok(Self::build_recovered_state(snapshot))
    }

    /// Recovers from a specific checkpoint by ID.
    ///
    /// # Errors
    ///
    /// Returns [`DagError::CheckpointNotFound`] if the checkpoint ID
    /// does not exist.
    pub fn recover_by_id(&self, checkpoint_id: u64) -> Result<RecoveredDagState, DagError> {
        let snapshot = self
            .snapshots
            .iter()
            .find(|s| s.checkpoint_id == checkpoint_id)
            .ok_or(DagError::CheckpointNotFound)?
            .clone();

        Ok(Self::build_recovered_state(snapshot))
    }

    /// Returns the number of stored snapshots.
    #[must_use]
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    /// Returns whether any snapshots are available.
    #[must_use]
    pub fn has_snapshots(&self) -> bool {
        !self.snapshots.is_empty()
    }

    /// Builds a `RecoveredDagState` from a snapshot.
    fn build_recovered_state(snapshot: DagCheckpointSnapshot) -> RecoveredDagState {
        let operator_states = snapshot.to_operator_states();
        let source_offsets = snapshot.source_offsets.clone();
        let watermark = snapshot.watermark;

        RecoveredDagState {
            snapshot,
            operator_states,
            source_offsets,
            watermark,
        }
    }
}

impl Default for DagRecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
