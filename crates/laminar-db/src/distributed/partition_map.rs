//! Partition-to-node mapping for distributed query routing.
//!
//! The [`PartitionMap`] tracks which node owns each partition and which
//! source name each partition belongs to. The coordinator uses this to
//! decide where to send sub-queries.

#![allow(clippy::disallowed_types)] // cold path: partition map is control-plane metadata

use std::collections::{HashMap, HashSet};
use std::fmt;

use laminar_core::delta::discovery::NodeId;

/// Assignment details for a single partition.
#[derive(Debug, Clone)]
pub struct NodeAssignment {
    /// The node that currently owns this partition.
    pub node_id: NodeId,
    /// Network address for RPC communication with the owning node.
    pub address: String,
    /// The source/table name this partition belongs to.
    pub source_name: String,
}

/// Maps partition IDs to their owning nodes.
///
/// Updated from the Raft metadata state machine whenever partition
/// ownership changes. The `epoch` field detects stale copies.
#[derive(Debug, Clone)]
pub struct PartitionMap {
    /// `partition_id` -> (`node_id`, address, `source_name`)
    assignments: HashMap<u32, NodeAssignment>,
    /// Monotonically increasing epoch for staleness detection.
    epoch: u64,
}

impl PartitionMap {
    /// Create an empty partition map at epoch 0.
    #[must_use]
    pub fn new() -> Self {
        Self {
            assignments: HashMap::new(),
            epoch: 0,
        }
    }

    /// Create a partition map from a set of assignments and an epoch.
    #[must_use]
    pub fn with_assignments(assignments: HashMap<u32, NodeAssignment>, epoch: u64) -> Self {
        Self { assignments, epoch }
    }

    /// The current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Total number of tracked partitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.assignments.len()
    }

    /// Returns `true` if no partitions are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.assignments.is_empty()
    }

    /// Get the assignment for a specific partition.
    #[must_use]
    pub fn get(&self, partition_id: u32) -> Option<&NodeAssignment> {
        self.assignments.get(&partition_id)
    }

    /// Insert or update a partition assignment.
    pub fn insert(&mut self, partition_id: u32, assignment: NodeAssignment) {
        self.assignments.insert(partition_id, assignment);
    }

    /// Advance the epoch.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Return all nodes that own partitions for the given source name.
    ///
    /// Deduplicates by `NodeId` — each node appears at most once.
    #[must_use]
    pub fn nodes_for_source(&self, source_name: &str) -> Vec<&NodeAssignment> {
        let mut seen = HashSet::new();
        self.assignments
            .values()
            .filter(|a| a.source_name == source_name)
            .filter(|a| seen.insert(a.node_id))
            .collect()
    }

    /// Return all partition IDs owned by the given source on a specific node.
    #[must_use]
    pub fn partitions_for_source_on_node(&self, source_name: &str, node_id: NodeId) -> Vec<u32> {
        self.assignments
            .iter()
            .filter(|(_, a)| a.source_name == source_name && a.node_id == node_id)
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// Returns `true` if the partition is owned by the given node.
    #[must_use]
    pub fn is_local(&self, partition_id: u32, local_node: &NodeId) -> bool {
        self.assignments
            .get(&partition_id)
            .is_some_and(|a| a.node_id == *local_node)
    }

    /// Check whether all partitions for a source are on the local node.
    #[must_use]
    pub fn all_local_for_source(&self, source_name: &str, local_node: &NodeId) -> bool {
        self.assignments
            .values()
            .filter(|a| a.source_name == source_name)
            .all(|a| a.node_id == *local_node)
    }

    /// Return the set of distinct node IDs that own partitions for any of
    /// the given source names.
    #[must_use]
    pub fn nodes_for_sources(&self, source_names: &[&str]) -> Vec<NodeId> {
        let mut seen = HashSet::new();
        for a in self.assignments.values() {
            if source_names.contains(&a.source_name.as_str()) {
                seen.insert(a.node_id);
            }
        }
        seen.into_iter().collect()
    }

    /// Return all distinct node IDs in the map.
    #[must_use]
    pub fn all_nodes(&self) -> Vec<NodeId> {
        let mut seen = HashSet::new();
        for a in self.assignments.values() {
            seen.insert(a.node_id);
        }
        seen.into_iter().collect()
    }
}

impl Default for PartitionMap {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PartitionMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartitionMap(partitions={}, epoch={})",
            self.assignments.len(),
            self.epoch
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment(node: u64, source: &str) -> NodeAssignment {
        NodeAssignment {
            node_id: NodeId(node),
            address: format!("127.0.0.1:{}", 9000 + node),
            source_name: source.to_string(),
        }
    }

    #[test]
    fn test_empty_map() {
        let map = PartitionMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        assert_eq!(map.epoch(), 0);
    }

    #[test]
    fn test_insert_and_get() {
        let mut map = PartitionMap::new();
        map.insert(0, assignment(1, "trades"));
        map.insert(1, assignment(2, "trades"));

        assert_eq!(map.len(), 2);
        assert_eq!(map.get(0).unwrap().node_id, NodeId(1));
        assert_eq!(map.get(1).unwrap().node_id, NodeId(2));
        assert!(map.get(99).is_none());
    }

    #[test]
    fn test_is_local() {
        let mut map = PartitionMap::new();
        map.insert(0, assignment(1, "trades"));
        map.insert(1, assignment(2, "trades"));

        assert!(map.is_local(0, &NodeId(1)));
        assert!(!map.is_local(0, &NodeId(2)));
        assert!(!map.is_local(99, &NodeId(1)));
    }

    #[test]
    fn test_all_local_for_source() {
        let mut map = PartitionMap::new();
        map.insert(0, assignment(1, "trades"));
        map.insert(1, assignment(1, "trades"));
        assert!(map.all_local_for_source("trades", &NodeId(1)));

        map.insert(2, assignment(2, "trades"));
        assert!(!map.all_local_for_source("trades", &NodeId(1)));
    }

    #[test]
    fn test_nodes_for_source_deduplicates() {
        let mut map = PartitionMap::new();
        map.insert(0, assignment(1, "trades"));
        map.insert(1, assignment(1, "trades"));
        map.insert(2, assignment(2, "trades"));
        map.insert(3, assignment(2, "orders"));

        let nodes = map.nodes_for_source("trades");
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_partitions_for_source_on_node() {
        let mut map = PartitionMap::new();
        map.insert(0, assignment(1, "trades"));
        map.insert(1, assignment(1, "trades"));
        map.insert(2, assignment(2, "trades"));
        map.insert(3, assignment(1, "orders"));

        let mut parts = map.partitions_for_source_on_node("trades", NodeId(1));
        parts.sort();
        assert_eq!(parts, vec![0, 1]);
    }

    #[test]
    fn test_nodes_for_sources() {
        let mut map = PartitionMap::new();
        map.insert(0, assignment(1, "trades"));
        map.insert(1, assignment(2, "orders"));
        map.insert(2, assignment(3, "quotes"));

        let mut nodes = map.nodes_for_sources(&["trades", "orders"]);
        nodes.sort_by_key(|n| n.0);
        assert_eq!(nodes, vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_all_nodes() {
        let mut map = PartitionMap::new();
        map.insert(0, assignment(1, "a"));
        map.insert(1, assignment(2, "b"));
        map.insert(2, assignment(1, "c"));

        let mut nodes = map.all_nodes();
        nodes.sort_by_key(|n| n.0);
        assert_eq!(nodes, vec![NodeId(1), NodeId(2)]);
    }

    #[test]
    fn test_epoch() {
        let mut map = PartitionMap::new();
        assert_eq!(map.epoch(), 0);
        map.set_epoch(5);
        assert_eq!(map.epoch(), 5);
    }

    #[test]
    fn test_with_assignments() {
        let mut assignments = HashMap::new();
        assignments.insert(0, assignment(1, "trades"));
        let map = PartitionMap::with_assignments(assignments, 42);
        assert_eq!(map.len(), 1);
        assert_eq!(map.epoch(), 42);
    }

    #[test]
    fn test_display() {
        let map = PartitionMap::new();
        let s = format!("{map}");
        assert!(s.contains("partitions=0"));
        assert!(s.contains("epoch=0"));
    }
}
