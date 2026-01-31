//! DAG-native changelog propagation.
//!
//! Manages per-node `ChangelogBuffer` instances for tracking Z-set changes
//! through a DAG pipeline. Each node can record insertions, deletions, and
//! retraction pairs that are drained by Ring 1 for checkpointing and sink
//! delivery.
//!
//! # Ring 0 Compatibility
//!
//! All operations use Vec-indexed storage and delegate to `ChangelogBuffer`
//! which is pre-allocated and zero-allocation after warmup.

use std::fmt;

use super::topology::{NodeId, StreamingDag};
use crate::operator::changelog::{ChangelogBuffer, ChangelogRef};

/// Per-node changelog buffer management for DAG pipelines.
///
/// Wraps a `ChangelogBuffer` per node, with global and per-node enable flags
/// for zero-overhead disabled mode.
pub struct DagChangelogPropagator {
    /// Per-node changelog buffers, indexed by NodeId.0.
    buffers: Vec<ChangelogBuffer>,
    /// Per-node enable flags.
    enabled: Vec<bool>,
    /// Number of slots.
    slot_count: usize,
    /// Global enable flag (fast path to skip all recording).
    globally_enabled: bool,
}

impl fmt::Debug for DagChangelogPropagator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DagChangelogPropagator")
            .field("slot_count", &self.slot_count)
            .field("globally_enabled", &self.globally_enabled)
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}

impl DagChangelogPropagator {
    /// Creates a propagator from a finalized DAG with the given buffer capacity.
    ///
    /// All nodes start enabled. Use `set_node_enabled` to selectively disable.
    #[must_use]
    pub fn from_dag(dag: &StreamingDag, capacity: usize) -> Self {
        let max_id = dag
            .nodes()
            .keys()
            .map(|n| n.0 as usize)
            .max()
            .map_or(0, |m| m + 1);

        let buffers = (0..max_id)
            .map(|_| ChangelogBuffer::with_capacity(capacity))
            .collect();
        let enabled = vec![true; max_id];

        Self {
            buffers,
            enabled,
            slot_count: max_id,
            globally_enabled: true,
        }
    }

    /// Creates a disabled propagator (zero overhead).
    ///
    /// No buffers are allocated. All `record` calls return `false` immediately.
    #[must_use]
    pub fn disabled(slot_count: usize) -> Self {
        Self {
            buffers: (0..slot_count)
                .map(|_| ChangelogBuffer::with_capacity(0))
                .collect(),
            enabled: vec![false; slot_count],
            slot_count,
            globally_enabled: false,
        }
    }

    /// Records a changelog reference at a node.
    ///
    /// Returns `true` if the reference was recorded, `false` if the propagator
    /// is disabled, the node is disabled, or the buffer is full (backpressure).
    #[inline]
    pub fn record(&mut self, node: NodeId, changelog_ref: ChangelogRef) -> bool {
        if !self.globally_enabled {
            return false;
        }
        let idx = node.0 as usize;
        if idx >= self.slot_count || !self.enabled[idx] {
            return false;
        }
        self.buffers[idx].push(changelog_ref)
    }

    /// Records a retraction pair (update-before + update-after) at a node.
    ///
    /// Returns `true` if both references were recorded.
    #[inline]
    pub fn record_retraction(
        &mut self,
        node: NodeId,
        batch_offset: u32,
        old_row_index: u32,
        new_row_index: u32,
    ) -> bool {
        if !self.globally_enabled {
            return false;
        }
        let idx = node.0 as usize;
        if idx >= self.slot_count || !self.enabled[idx] {
            return false;
        }
        self.buffers[idx].push_retraction(batch_offset, old_row_index, new_row_index)
    }

    /// Drains all changelog references from a node's buffer.
    ///
    /// Returns the drained references. The buffer is cleared but retains capacity.
    pub fn drain_node(&mut self, node: NodeId) -> Vec<ChangelogRef> {
        let idx = node.0 as usize;
        if idx >= self.slot_count {
            return Vec::new();
        }
        self.buffers[idx].drain().collect()
    }

    /// Drains all changelog references from all node buffers.
    pub fn drain_all(&mut self) {
        for buffer in &mut self.buffers {
            buffer.clear();
        }
    }

    /// Returns `true` if any node has pending changelog references.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        if !self.globally_enabled {
            return false;
        }
        self.buffers.iter().any(|b| !b.is_empty())
    }

    /// Returns the number of pending changelog references at a node.
    #[must_use]
    pub fn pending_count(&self, node: NodeId) -> usize {
        let idx = node.0 as usize;
        if idx >= self.slot_count {
            return 0;
        }
        self.buffers[idx].len()
    }

    /// Enables or disables changelog recording for a specific node.
    pub fn set_node_enabled(&mut self, node: NodeId, enabled: bool) {
        let idx = node.0 as usize;
        if idx < self.slot_count {
            self.enabled[idx] = enabled;
        }
    }

    /// Enables or disables changelog recording globally.
    pub fn set_globally_enabled(&mut self, enabled: bool) {
        self.globally_enabled = enabled;
    }

    /// Returns whether changelog recording is globally enabled.
    #[must_use]
    pub fn is_globally_enabled(&self) -> bool {
        self.globally_enabled
    }

    /// Returns the number of node slots.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.slot_count
    }
}
