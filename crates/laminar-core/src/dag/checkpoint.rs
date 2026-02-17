//! Chandy-Lamport barrier checkpointing for DAG pipelines.
//!
//! This module implements barrier-based checkpointing:
//!
//! - [`CheckpointBarrier`] — marker injected at source nodes
//! - [`BarrierAligner`] — buffers events at fan-in (MPSC) nodes until all
//!   upstream inputs have delivered their barrier
//! - [`DagCheckpointCoordinator`] — Ring 1 orchestrator that triggers
//!   checkpoints, tracks progress, and produces snapshots
//! - [`DagCheckpointConfig`] — tuning knobs (interval, timeout, retention)
//!
//! Barriers do NOT flow through event queues — they are handled by a separate
//! orchestration path, keeping the hot-path [`Event`]
//! type unchanged.

use std::collections::VecDeque;
use std::time::Duration;

use rustc_hash::FxHashMap;

use crate::operator::{Event, OperatorState};

use super::error::DagError;
use super::recovery::DagCheckpointSnapshot;
use super::topology::NodeId;

/// Checkpoint identifier.
pub type CheckpointId = u64;

/// Barrier type for checkpoint coordination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierType {
    /// All inputs must deliver the barrier before the node snapshots.
    /// Events from already-aligned inputs are buffered until alignment.
    Aligned,
}

/// A checkpoint barrier injected at source nodes and propagated through the DAG.
#[derive(Debug, Clone)]
pub struct CheckpointBarrier {
    /// Unique identifier for this checkpoint.
    pub checkpoint_id: CheckpointId,
    /// Monotonically increasing epoch counter.
    pub epoch: u64,
    /// Timestamp when the barrier was created (event-time millis).
    pub timestamp: i64,
    /// Barrier alignment strategy.
    pub barrier_type: BarrierType,
}

/// Configuration for DAG checkpointing.
#[derive(Debug, Clone)]
pub struct DagCheckpointConfig {
    /// How often to trigger checkpoints.
    pub interval: Duration,
    /// Barrier alignment strategy.
    pub barrier_type: BarrierType,
    /// Maximum time to wait for barrier alignment at fan-in nodes.
    pub alignment_timeout: Duration,
    /// Whether to use incremental checkpoints (future use).
    pub incremental: bool,
    /// Maximum number of concurrent in-flight checkpoints.
    pub max_concurrent: usize,
    /// Maximum number of completed snapshots to retain.
    pub max_retained: usize,
}

impl Default for DagCheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            barrier_type: BarrierType::Aligned,
            alignment_timeout: Duration::from_secs(10),
            incremental: false,
            max_concurrent: 1,
            max_retained: 3,
        }
    }
}

// ---------------------------------------------------------------------------
// BarrierAligner
// ---------------------------------------------------------------------------

/// Result of presenting a barrier to the aligner.
#[derive(Debug)]
pub enum AlignmentResult {
    /// Not all inputs have delivered their barrier yet.
    Pending,
    /// All inputs have delivered their barrier; checkpoint can proceed.
    Aligned {
        /// The checkpoint barrier (from the final input).
        barrier: CheckpointBarrier,
        /// Events that were buffered from already-aligned inputs.
        buffered_events: Vec<Event>,
    },
}

/// Buffers events at fan-in (MPSC) nodes until all upstream inputs
/// have delivered their checkpoint barrier.
///
/// For nodes with a single input (`expected_inputs == 1`), alignment
/// is immediate — no buffering occurs.
#[derive(Debug)]
pub struct BarrierAligner {
    /// Number of upstream inputs that must deliver a barrier.
    expected_inputs: usize,
    /// Barriers received so far, keyed by source node.
    barriers_received: FxHashMap<NodeId, CheckpointBarrier>,
    /// Events buffered from sources that have already delivered their barrier.
    buffered_events: FxHashMap<NodeId, VecDeque<Event>>,
    /// The checkpoint currently being aligned (if any).
    current_checkpoint_id: Option<CheckpointId>,
}

impl BarrierAligner {
    /// Creates a new barrier aligner for a node with `expected_inputs` upstream edges.
    #[must_use]
    pub fn new(expected_inputs: usize) -> Self {
        Self {
            expected_inputs,
            barriers_received: FxHashMap::default(),
            buffered_events: FxHashMap::default(),
            current_checkpoint_id: None,
        }
    }

    /// Processes a barrier arriving from `source_node`.
    ///
    /// Returns [`AlignmentResult::Pending`] until all inputs have reported,
    /// then returns [`AlignmentResult::Aligned`] with the barrier and any
    /// buffered events.
    ///
    /// # Panics
    ///
    /// Panics if the internal barrier map is empty after insertion (should
    /// never happen).
    pub fn on_barrier(
        &mut self,
        source_node: NodeId,
        barrier: CheckpointBarrier,
    ) -> AlignmentResult {
        self.current_checkpoint_id = Some(barrier.checkpoint_id);
        self.barriers_received.insert(source_node, barrier);

        if self.barriers_received.len() >= self.expected_inputs {
            // All inputs aligned — drain buffered events.
            let mut all_buffered = Vec::new();
            for (_node, mut events) in self.buffered_events.drain() {
                all_buffered.extend(events.drain(..));
            }

            // Take the last barrier received as the canonical one.
            let barrier = self
                .barriers_received
                .values()
                .last()
                .cloned()
                .expect("at least one barrier");

            AlignmentResult::Aligned {
                barrier,
                buffered_events: all_buffered,
            }
        } else {
            AlignmentResult::Pending
        }
    }

    /// Buffers an event from `source_node` if that source has already
    /// delivered its barrier for the current checkpoint.
    ///
    /// Returns `true` if the event was buffered (source already aligned),
    /// `false` if the source has not yet delivered its barrier (event should
    /// be processed normally).
    pub fn buffer_if_aligned(&mut self, source_node: NodeId, event: Event) -> bool {
        if self.barriers_received.contains_key(&source_node) {
            self.buffered_events
                .entry(source_node)
                .or_default()
                .push_back(event);
            true
        } else {
            false
        }
    }

    /// Returns whether `source_node` has already delivered its barrier.
    #[must_use]
    pub fn is_source_aligned(&self, source_node: NodeId) -> bool {
        self.barriers_received.contains_key(&source_node)
    }

    /// Resets alignment state after a checkpoint completes.
    pub fn complete_checkpoint(&mut self) {
        self.barriers_received.clear();
        self.buffered_events.clear();
        self.current_checkpoint_id = None;
    }

    /// Returns how many barriers have been received so far.
    #[must_use]
    pub fn barriers_received_count(&self) -> usize {
        self.barriers_received.len()
    }

    /// Returns the expected number of upstream inputs.
    #[must_use]
    pub fn expected_inputs(&self) -> usize {
        self.expected_inputs
    }
}

// ---------------------------------------------------------------------------
// DagCheckpointCoordinator
// ---------------------------------------------------------------------------

/// Tracks progress of an in-flight checkpoint.
struct CheckpointProgress {
    /// Checkpoint identifier.
    checkpoint_id: CheckpointId,
    /// Epoch number.
    epoch: u64,
    /// Operator states reported by completed nodes.
    completed_nodes: FxHashMap<NodeId, OperatorState>,
    /// Nodes that have not yet reported.
    pending_nodes: Vec<NodeId>,
    /// When the checkpoint was triggered (event-time millis).
    triggered_at: i64,
}

/// Ring 1 checkpoint coordinator.
///
/// Orchestrates the checkpoint lifecycle:
/// 1. [`trigger_checkpoint()`](Self::trigger_checkpoint) — creates a barrier
/// 2. Barrier propagates through the DAG (external to this struct)
/// 3. [`on_node_snapshot_complete()`](Self::on_node_snapshot_complete) —
///    each node reports its state
/// 4. [`finalize_checkpoint()`](Self::finalize_checkpoint) — produces a
///    [`DagCheckpointSnapshot`]
pub struct DagCheckpointCoordinator {
    /// Configuration.
    config: DagCheckpointConfig,
    /// Source node IDs (barrier injection points).
    source_nodes: Vec<NodeId>,
    /// All node IDs in the DAG.
    all_nodes: Vec<NodeId>,
    /// Next epoch counter.
    next_epoch: u64,
    /// Next checkpoint ID counter.
    next_checkpoint_id: CheckpointId,
    /// Currently in-flight checkpoint progress.
    in_progress: Option<CheckpointProgress>,
    /// Completed snapshots (bounded by `max_retained`).
    completed_snapshots: Vec<DagCheckpointSnapshot>,
    /// Maximum number of snapshots to retain.
    max_retained: usize,
}

impl DagCheckpointCoordinator {
    /// Creates a new checkpoint coordinator.
    ///
    /// # Arguments
    ///
    /// * `source_nodes` — DAG source node IDs (barrier injection points)
    /// * `all_nodes` — all node IDs in the DAG
    /// * `config` — checkpoint configuration
    #[must_use]
    pub fn new(
        source_nodes: Vec<NodeId>,
        all_nodes: Vec<NodeId>,
        config: DagCheckpointConfig,
    ) -> Self {
        let max_retained = config.max_retained;
        Self {
            config,
            source_nodes,
            all_nodes,
            next_epoch: 1,
            next_checkpoint_id: 1,
            in_progress: None,
            completed_snapshots: Vec::new(),
            max_retained,
        }
    }

    /// Triggers a new checkpoint by creating a barrier.
    ///
    /// # Errors
    ///
    /// Returns [`DagError::CheckpointInProgress`] if a checkpoint is already
    /// in flight.
    pub fn trigger_checkpoint(&mut self) -> Result<CheckpointBarrier, DagError> {
        if let Some(ref progress) = self.in_progress {
            return Err(DagError::CheckpointInProgress(progress.epoch));
        }

        let checkpoint_id = self.next_checkpoint_id;
        self.next_checkpoint_id += 1;
        let epoch = self.next_epoch;
        self.next_epoch += 1;

        #[allow(clippy::cast_possible_truncation)]
        // Timestamp ms fits i64 for ~292 years from epoch
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);

        let barrier = CheckpointBarrier {
            checkpoint_id,
            epoch,
            timestamp,
            barrier_type: self.config.barrier_type,
        };

        self.in_progress = Some(CheckpointProgress {
            checkpoint_id,
            epoch,
            completed_nodes: FxHashMap::default(),
            pending_nodes: self.all_nodes.clone(),
            triggered_at: barrier.timestamp,
        });

        Ok(barrier)
    }

    /// Records that a node has completed its snapshot.
    ///
    /// Returns `true` if all nodes have now reported (checkpoint is ready
    /// to finalize).
    pub fn on_node_snapshot_complete(&mut self, node_id: NodeId, state: OperatorState) -> bool {
        if let Some(ref mut progress) = self.in_progress {
            progress.pending_nodes.retain(|&n| n != node_id);
            progress.completed_nodes.insert(node_id, state);
            progress.pending_nodes.is_empty()
        } else {
            false
        }
    }

    /// Finalizes the current checkpoint, producing a snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`DagError::NoCheckpointInProgress`] if no checkpoint is active.
    /// Returns [`DagError::CheckpointIncomplete`] if not all nodes have reported.
    pub fn finalize_checkpoint(&mut self) -> Result<DagCheckpointSnapshot, DagError> {
        let progress = self
            .in_progress
            .take()
            .ok_or(DagError::NoCheckpointInProgress)?;

        if !progress.pending_nodes.is_empty() {
            let pending = progress.pending_nodes.len();
            // Put progress back so it can continue.
            self.in_progress = Some(progress);
            return Err(DagError::CheckpointIncomplete { pending });
        }

        let snapshot = DagCheckpointSnapshot::from_operator_states(
            progress.checkpoint_id,
            progress.epoch,
            progress.triggered_at,
            &progress.completed_nodes,
        );

        self.completed_snapshots.push(snapshot.clone());

        // Trim old snapshots.
        while self.completed_snapshots.len() > self.max_retained {
            self.completed_snapshots.remove(0);
        }

        Ok(snapshot)
    }

    /// Returns the in-flight checkpoint progress (if any).
    #[must_use]
    pub fn in_progress(&self) -> Option<&CheckpointBarrier> {
        // We reconstruct a temporary barrier from progress for API consumers.
        // Since we can't return a reference to a temporary, we expose a simpler API.
        None // Use is_checkpoint_in_progress() instead.
    }

    /// Returns whether a checkpoint is currently in progress.
    #[must_use]
    pub fn is_checkpoint_in_progress(&self) -> bool {
        self.in_progress.is_some()
    }

    /// Returns completed snapshots.
    #[must_use]
    pub fn completed_snapshots(&self) -> &[DagCheckpointSnapshot] {
        &self.completed_snapshots
    }

    /// Returns the latest completed snapshot (if any).
    #[must_use]
    pub fn latest_snapshot(&self) -> Option<&DagCheckpointSnapshot> {
        self.completed_snapshots.last()
    }

    /// Returns the current epoch counter (next epoch to be assigned).
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.next_epoch
    }

    /// Returns the source node IDs.
    #[must_use]
    pub fn source_nodes(&self) -> &[NodeId] {
        &self.source_nodes
    }
}

impl std::fmt::Debug for DagCheckpointCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DagCheckpointCoordinator")
            .field("next_epoch", &self.next_epoch)
            .field("next_checkpoint_id", &self.next_checkpoint_id)
            .field("in_progress", &self.in_progress.is_some())
            .field("completed_count", &self.completed_snapshots.len())
            .field("source_nodes", &self.source_nodes)
            .finish_non_exhaustive()
    }
}
