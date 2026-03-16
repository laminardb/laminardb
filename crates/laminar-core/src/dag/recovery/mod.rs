//! Checkpoint result types and recovery helpers for DAG pipelines.
//!
//! [`DagCheckpointResult`] is the output of
//! [`DagCheckpointCoordinator::finalize_checkpoint()`](super::checkpoint::DagCheckpointCoordinator::finalize_checkpoint).
//! It stores operator states in the same `HashMap<String, Vec<u8>>` format
//! that the unified `CheckpointCoordinator` consumes — no conversion needed
//! for persistence.
//!
//! For recovery, [`to_operator_states()`](DagCheckpointResult::to_operator_states)
//! converts back to the `FxHashMap<NodeId, OperatorState>` that
//! [`DagExecutor::restore()`](super::executor::DagExecutor::restore) expects.

#![allow(clippy::disallowed_types)] // HashMap required for cross-crate compatibility

use std::collections::HashMap;

use rustc_hash::FxHashMap;

use crate::operator::OperatorState;

use super::topology::NodeId;

/// Result of a finalized DAG checkpoint.
///
/// Stores operator states as `HashMap<String, Vec<u8>>` keyed by operator name
/// — the same format consumed by `CheckpointCoordinator::checkpoint()`. This
/// eliminates the need for a separate snapshot type and ensures
/// DAG checkpoints can be persisted through the unified checkpoint path.
#[derive(Debug, Clone)]
pub struct DagCheckpointResult {
    /// Unique checkpoint identifier.
    pub checkpoint_id: u64,
    /// Monotonically increasing epoch.
    pub epoch: u64,
    /// Timestamp when the checkpoint was triggered (millis since epoch).
    pub timestamp_ms: i64,
    /// Per-operator state keyed by `OperatorState::operator_id`.
    ///
    /// Values are the raw serialized bytes from `OperatorState::data`.
    /// This map can be passed directly to `CheckpointCoordinator::checkpoint()`
    /// for persistence.
    pub operator_states: HashMap<String, Vec<u8>>,
}

impl DagCheckpointResult {
    /// Creates a checkpoint result from a collected map of operator states.
    ///
    /// Converts from the internal `FxHashMap<NodeId, OperatorState>` (keyed by
    /// DAG node index) to `HashMap<String, Vec<u8>>` (keyed by operator name).
    pub(crate) fn from_operator_states(
        checkpoint_id: u64,
        epoch: u64,
        timestamp_ms: i64,
        states: &FxHashMap<NodeId, OperatorState>,
    ) -> Self {
        let operator_states = states
            .values()
            .map(|state| (state.operator_id.clone(), state.data.clone()))
            .collect();

        Self {
            checkpoint_id,
            epoch,
            timestamp_ms,
            operator_states,
        }
    }

    /// Converts operator states back to `FxHashMap<NodeId, OperatorState>`.
    ///
    /// Requires a name-to-`NodeId` mapping (typically from `StreamingDag`).
    /// Operators whose names are not found in the mapping are skipped with
    /// a debug log.
    ///
    /// # Arguments
    ///
    /// * `name_to_node` — maps operator name to its `NodeId` in the DAG
    #[must_use]
    pub fn to_operator_states(
        &self,
        name_to_node: &HashMap<String, NodeId>,
    ) -> FxHashMap<NodeId, OperatorState> {
        let mut result = FxHashMap::default();
        for (name, data) in &self.operator_states {
            if let Some(&node_id) = name_to_node.get(name) {
                result.insert(
                    node_id,
                    OperatorState {
                        operator_id: name.clone(),
                        version: 1,
                        data: data.clone(),
                    },
                );
            }
        }
        result
    }

    /// Converts operator states using a simple index-based mapping.
    ///
    /// Assumes operator IDs are stringified `NodeId.0` values (e.g., "0", "1", "3").
    /// This matches the convention used by `DagExecutor::checkpoint()` where
    /// operators use their node index as their `operator_id`.
    #[must_use]
    pub fn to_operator_states_by_index(&self) -> FxHashMap<NodeId, OperatorState> {
        let mut result = FxHashMap::default();
        for (name, data) in &self.operator_states {
            if let Ok(idx) = name.parse::<u32>() {
                result.insert(
                    NodeId(idx),
                    OperatorState {
                        operator_id: name.clone(),
                        version: 1,
                        data: data.clone(),
                    },
                );
            }
        }
        result
    }
}

#[cfg(test)]
mod tests;
