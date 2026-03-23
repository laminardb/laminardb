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

/// Versioned operator state bytes stored in a [`DagCheckpointResult`].
#[derive(Debug, Clone)]
pub struct OperatorStateEntry {
    /// State format version (from [`OperatorState::version`]).
    pub version: u32,
    /// Raw serialized bytes (from [`OperatorState::data`]).
    pub data: Vec<u8>,
}

/// Result of a finalized DAG checkpoint.
///
/// Stores operator states keyed by operator name. The raw `data` bytes
/// can be extracted via [`to_data_map()`](Self::to_data_map) for passing
/// to `CheckpointCoordinator::checkpoint()`.
#[derive(Debug, Clone)]
pub struct DagCheckpointResult {
    /// Unique checkpoint identifier.
    pub checkpoint_id: u64,
    /// Monotonically increasing epoch.
    pub epoch: u64,
    /// Timestamp when the checkpoint was triggered (millis since epoch).
    pub timestamp_ms: i64,
    /// Per-operator state keyed by `OperatorState::operator_id`.
    pub operator_states: HashMap<String, OperatorStateEntry>,
}

impl DagCheckpointResult {
    /// Creates a checkpoint result from a collected map of operator states.
    ///
    /// Converts from the internal `FxHashMap<NodeId, OperatorState>` (keyed by
    /// DAG node index) to `HashMap<String, OperatorStateEntry>` (keyed by
    /// operator name). Preserves the original `OperatorState::version`.
    pub(crate) fn from_operator_states(
        checkpoint_id: u64,
        epoch: u64,
        timestamp_ms: i64,
        states: &FxHashMap<NodeId, OperatorState>,
    ) -> Self {
        let operator_states = states
            .values()
            .map(|state| {
                (
                    state.operator_id.clone(),
                    OperatorStateEntry {
                        version: state.version,
                        data: state.data.clone(),
                    },
                )
            })
            .collect();

        Self {
            checkpoint_id,
            epoch,
            timestamp_ms,
            operator_states,
        }
    }

    /// Extracts a `HashMap<String, Vec<u8>>` for passing to
    /// `CheckpointCoordinator::checkpoint()`.
    #[must_use]
    pub fn to_data_map(&self) -> HashMap<String, Vec<u8>> {
        self.operator_states
            .iter()
            .map(|(name, entry)| (name.clone(), entry.data.clone()))
            .collect()
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
        for (name, entry) in &self.operator_states {
            if let Some(&node_id) = name_to_node.get(name) {
                result.insert(
                    node_id,
                    OperatorState {
                        operator_id: name.clone(),
                        version: entry.version,
                        data: entry.data.clone(),
                    },
                );
            } else {
                tracing::debug!(
                    operator = %name,
                    "checkpoint operator not found in current DAG topology, skipping"
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
        for (name, entry) in &self.operator_states {
            if let Ok(idx) = name.parse::<u32>() {
                result.insert(
                    NodeId(idx),
                    OperatorState {
                        operator_id: name.clone(),
                        version: entry.version,
                        data: entry.data.clone(),
                    },
                );
            }
        }
        result
    }
}

#[cfg(test)]
mod tests;
