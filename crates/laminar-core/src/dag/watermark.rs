//! DAG-native watermark tracking.
//!
//! Provides Vec-indexed O(1) watermark propagation through a DAG topology.
//! Watermarks flow from source nodes downstream using min-semantics at
//! fan-in (merge) nodes, matching the global watermark tracking pattern
//! from F010 but scoped to DAG nodes.
//!
//! # Ring 0 Compatibility
//!
//! All operations use pre-allocated `Vec<T>` indexed by `NodeId.0` for
//! zero-allocation lookups, consistent with `DagExecutor`.

use smallvec::SmallVec;

use super::topology::{NodeId, StreamingDag};

/// Sentinel value indicating no watermark has been set for a node.
const WATERMARK_UNSET: i64 = i64::MIN;

/// Checkpoint state for `DagWatermarkTracker`.
#[derive(Debug, Clone)]
pub struct DagWatermarkCheckpoint {
    /// Per-node watermark values (indexed by slot).
    pub watermarks: Vec<i64>,
}

/// Vec-indexed O(1) watermark tracker for DAG pipelines.
///
/// Propagates watermarks through the DAG using min-semantics:
/// a node's effective watermark is the minimum of its input sources.
/// Uses pre-allocated vectors for Ring 0 compatibility.
#[derive(Debug)]
pub struct DagWatermarkTracker {
    /// Per-node watermark values, indexed by NodeId.0.
    watermarks: Vec<i64>,
    /// Input dependencies per node (which nodes feed into this node).
    source_deps: Vec<SmallVec<[NodeId; 4]>>,
    /// Topological execution order for propagation.
    execution_order: Vec<NodeId>,
    /// Pre-allocated buffer for returning updated nodes.
    updated_buffer: Vec<(NodeId, i64)>,
    /// Number of slots allocated.
    slot_count: usize,
}

impl DagWatermarkTracker {
    /// Builds a watermark tracker from a finalized DAG.
    ///
    /// Extracts the adjacency structure and execution order from the DAG
    /// for efficient watermark propagation.
    #[must_use]
    pub fn from_dag(dag: &StreamingDag) -> Self {
        let max_id = dag
            .nodes()
            .keys()
            .map(|n| n.0 as usize)
            .max()
            .map_or(0, |m| m + 1);

        let mut source_deps = vec![SmallVec::new(); max_id];

        for edge in dag.edges().values() {
            let tgt = edge.target.0 as usize;
            if tgt < max_id {
                source_deps[tgt].push(edge.source);
            }
        }

        Self {
            watermarks: vec![WATERMARK_UNSET; max_id],
            source_deps,
            execution_order: dag.execution_order().to_vec(),
            updated_buffer: Vec::with_capacity(max_id),
            slot_count: max_id,
        }
    }

    /// Updates a source node's watermark and propagates downstream.
    ///
    /// Returns a slice of `(NodeId, new_watermark)` pairs for all nodes
    /// whose effective watermark changed as a result of this update.
    /// The returned slice is valid until the next call to `update_watermark`.
    pub fn update_watermark(&mut self, source: NodeId, wm: i64) -> &[(NodeId, i64)] {
        self.updated_buffer.clear();

        let idx = source.0 as usize;
        if idx >= self.slot_count {
            return &self.updated_buffer;
        }

        // Only update if the new watermark advances
        if wm <= self.watermarks[idx] {
            return &self.updated_buffer;
        }

        self.watermarks[idx] = wm;
        self.updated_buffer.push((source, wm));

        // Propagate through execution order
        for &node in &self.execution_order {
            let n = node.0 as usize;
            if n >= self.slot_count || self.source_deps[n].is_empty() {
                continue;
            }

            // Compute effective watermark = min of all input watermarks
            let mut min_wm = i64::MAX;
            let mut all_set = true;
            for dep in &self.source_deps[n] {
                let dep_wm = self.watermarks[dep.0 as usize];
                if dep_wm == WATERMARK_UNSET {
                    all_set = false;
                    break;
                }
                min_wm = min_wm.min(dep_wm);
            }

            if !all_set {
                continue;
            }

            if min_wm > self.watermarks[n] {
                self.watermarks[n] = min_wm;
                self.updated_buffer.push((node, min_wm));
            }
        }

        &self.updated_buffer
    }

    /// Returns the current watermark for a node, or `None` if unset.
    #[must_use]
    pub fn get_watermark(&self, node: NodeId) -> Option<i64> {
        let idx = node.0 as usize;
        if idx < self.slot_count && self.watermarks[idx] != WATERMARK_UNSET {
            Some(self.watermarks[idx])
        } else {
            None
        }
    }

    /// Returns the effective watermark for a node (min of its inputs).
    ///
    /// For source nodes (no inputs), returns the node's own watermark.
    /// Returns `None` if any input has not yet received a watermark.
    #[must_use]
    pub fn effective_watermark(&self, node: NodeId) -> Option<i64> {
        let idx = node.0 as usize;
        if idx >= self.slot_count {
            return None;
        }

        if self.source_deps[idx].is_empty() {
            // Source node: return own watermark
            return self.get_watermark(node);
        }

        let mut min_wm = i64::MAX;
        for dep in &self.source_deps[idx] {
            let dep_wm = self.watermarks[dep.0 as usize];
            if dep_wm == WATERMARK_UNSET {
                return None;
            }
            min_wm = min_wm.min(dep_wm);
        }
        Some(min_wm)
    }

    /// Creates a checkpoint of the current watermark state.
    #[must_use]
    pub fn checkpoint(&self) -> DagWatermarkCheckpoint {
        DagWatermarkCheckpoint {
            watermarks: self.watermarks.clone(),
        }
    }

    /// Restores watermark state from a checkpoint.
    pub fn restore(&mut self, checkpoint: &DagWatermarkCheckpoint) {
        let len = self.slot_count.min(checkpoint.watermarks.len());
        self.watermarks[..len].copy_from_slice(&checkpoint.watermarks[..len]);
    }

    /// Returns the number of node slots allocated.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        self.slot_count
    }
}
