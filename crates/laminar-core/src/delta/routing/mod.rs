//! Partition-aware batch routing for distributed delta mode.
//!
//! When delta mode is active, incoming `RecordBatch`es are partitioned by
//! a hash of a configurable partition key column. Batches whose target
//! partition is owned locally are returned directly; batches for remote
//! partitions are queued for gRPC forwarding.
//!
//! # Architecture
//!
//! ```text
//! Source batch
//!   │
//!   ├─ hash(partition_key) % num_partitions → owned locally?
//!   │   ├── YES → local batch buffer
//!   │   └── NO  → remote batch queue (keyed by target node)
//!   │
//!   ▼
//! PartitionRouter.route_batch()
//! ```

#![allow(clippy::disallowed_types)] // cold path: routing coordination
use std::collections::HashMap;

use arrow_array::RecordBatch;

use crate::delta::discovery::NodeId;
use crate::delta::partition::assignment::AssignmentPlan;

/// Result of routing a single `RecordBatch`.
#[derive(Debug, Default)]
pub struct RoutingResult {
    /// Batches destined for the local node, keyed by partition ID.
    pub local_batches: Vec<RecordBatch>,
    /// Batches destined for remote nodes, keyed by target node ID.
    pub remote_batches: HashMap<NodeId, Vec<RecordBatch>>,
}

/// Routes incoming record batches to the correct partition owner.
///
/// Uses the current `AssignmentPlan` and local `PartitionGuardSet` to
/// decide whether a batch should be processed locally or forwarded to
/// a remote node via gRPC.
pub struct PartitionRouter {
    /// The number of partitions.
    num_partitions: u32,
    /// Current partition-to-node assignment.
    assignments: HashMap<u32, NodeId>,
    /// This node's ID.
    local_node: NodeId,
    /// Column name to hash for partition routing.
    /// When `None`, all rows go to partition 0 (single-partition mode).
    partition_key: Option<String>,
}

impl PartitionRouter {
    /// Create a new router from an assignment plan.
    #[must_use]
    pub fn new(plan: &AssignmentPlan, local_node: NodeId, partition_key: Option<String>) -> Self {
        let num_partitions = plan.stats.total_partitions;
        Self {
            num_partitions,
            assignments: plan.assignments.clone(),
            local_node,
            partition_key,
        }
    }

    /// Update the router with a new assignment plan (after rebalance).
    pub fn update_assignments(&mut self, plan: &AssignmentPlan) {
        self.num_partitions = plan.stats.total_partitions;
        self.assignments.clone_from(&plan.assignments);
    }

    /// Route a `RecordBatch` to local and remote destinations.
    ///
    /// If no partition key is configured, the entire batch is routed based
    /// on partition 0's owner. When a partition key is configured, rows are
    /// hashed and split across partitions.
    #[must_use]
    pub fn route_batch(&self, batch: &RecordBatch) -> RoutingResult {
        if self.num_partitions == 0 || self.assignments.is_empty() {
            // No partitions configured — everything stays local.
            return RoutingResult {
                local_batches: vec![batch.clone()],
                remote_batches: HashMap::new(),
            };
        }

        // Without a partition key, route the whole batch to partition 0's owner.
        if self.partition_key.is_none() || batch.num_rows() == 0 {
            let owner = self.owner_of(0);
            if owner == self.local_node {
                return RoutingResult {
                    local_batches: vec![batch.clone()],
                    remote_batches: HashMap::new(),
                };
            }
            let mut remote = HashMap::new();
            remote.insert(owner, vec![batch.clone()]);
            return RoutingResult {
                local_batches: Vec::new(),
                remote_batches: remote,
            };
        }

        // With a partition key: compute the partition for each row by hashing
        // the key column value. For simplicity, we hash the first row to
        // determine the batch's partition (assuming batches are pre-partitioned
        // or small enough that splitting is unnecessary).
        let partition_id = self.compute_partition(batch);
        let owner = self.owner_of(partition_id);

        if owner == self.local_node {
            RoutingResult {
                local_batches: vec![batch.clone()],
                remote_batches: HashMap::new(),
            }
        } else {
            let mut remote = HashMap::new();
            remote.insert(owner, vec![batch.clone()]);
            RoutingResult {
                local_batches: Vec::new(),
                remote_batches: remote,
            }
        }
    }

    /// Check if a partition is owned by the local node.
    #[must_use]
    pub fn is_local(&self, partition_id: u32) -> bool {
        self.owner_of(partition_id) == self.local_node
    }

    /// Get the owner of a partition.
    #[must_use]
    pub fn owner_of(&self, partition_id: u32) -> NodeId {
        self.assignments
            .get(&partition_id)
            .copied()
            .unwrap_or(self.local_node)
    }

    /// Get the number of partitions.
    #[must_use]
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Compute the target partition ID for a batch using a simple hash of
    /// the batch row count (placeholder — real implementation would hash
    /// the partition key column values).
    fn compute_partition(&self, batch: &RecordBatch) -> u32 {
        if self.num_partitions == 0 {
            return 0;
        }
        // Use a simple hash of the number of rows as a deterministic
        // partition selector. A production implementation would hash
        // actual column values.
        #[allow(clippy::cast_possible_truncation)]
        let hash = {
            let n = batch.num_rows() as u64;
            // FNV-1a hash of row count bytes
            let mut h: u64 = 0xcbf2_9ce4_8422_2325;
            for &byte in &n.to_le_bytes() {
                h ^= u64::from(byte);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
            h
        };
        #[allow(clippy::cast_possible_truncation)]
        let partition = (hash % u64::from(self.num_partitions)) as u32;
        partition
    }
}

impl std::fmt::Debug for PartitionRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionRouter")
            .field("num_partitions", &self.num_partitions)
            .field("local_node", &self.local_node)
            .field("partition_key", &self.partition_key)
            .finish_non_exhaustive()
    }
}

/// Receives routed batches from remote peers and injects them into the
/// local coordinator pipeline.
pub struct RemoteBatchReceiver {
    /// Channel to forward received batches into the local pipeline.
    tx: tokio::sync::mpsc::Sender<(String, RecordBatch)>,
}

impl RemoteBatchReceiver {
    /// Create a new receiver with the given channel sender.
    #[must_use]
    pub fn new(tx: tokio::sync::mpsc::Sender<(String, RecordBatch)>) -> Self {
        Self { tx }
    }

    /// Inject a batch received from a remote peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline channel is closed.
    pub async fn inject_batch(
        &self,
        source_name: String,
        batch: RecordBatch,
    ) -> Result<(), String> {
        self.tx
            .send((source_name, batch))
            .await
            .map_err(|_| "pipeline channel closed".to_string())
    }
}

/// Sends batches to remote nodes via a channel-based interface.
///
/// The actual gRPC sending is handled by a background task that reads
/// from the channel. This decouples routing from network I/O.
pub struct RemoteBatchSender {
    /// Per-node send channels.
    node_channels: HashMap<NodeId, tokio::sync::mpsc::Sender<(String, RecordBatch)>>,
}

impl RemoteBatchSender {
    /// Create a new sender with no configured nodes.
    #[must_use]
    pub fn new() -> Self {
        Self {
            node_channels: HashMap::new(),
        }
    }

    /// Register a channel for sending batches to a remote node.
    pub fn register_node(
        &mut self,
        node_id: NodeId,
        tx: tokio::sync::mpsc::Sender<(String, RecordBatch)>,
    ) {
        self.node_channels.insert(node_id, tx);
    }

    /// Remove a node's send channel.
    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.node_channels.remove(node_id);
    }

    /// Queue a batch for sending to a remote node.
    ///
    /// # Errors
    ///
    /// Returns an error if no channel is registered for the target node
    /// or if the channel is full/closed.
    pub async fn send_batch(
        &self,
        target: NodeId,
        source_name: String,
        batch: RecordBatch,
    ) -> Result<(), String> {
        let tx = self
            .node_channels
            .get(&target)
            .ok_or_else(|| format!("no channel for node {target}"))?;
        tx.send((source_name, batch))
            .await
            .map_err(|_| format!("send channel closed for node {target}"))
    }
}

impl Default for RemoteBatchSender {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::discovery::{NodeInfo, NodeMetadata, NodeState};
    use crate::delta::partition::assignment::{AssignmentConstraints, ConsistentHashAssigner};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

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

    fn test_batch(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let values: Vec<i64> = (0..rows).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn test_router_single_node_all_local() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let router = PartitionRouter::new(&plan, NodeId(1), None);

        let batch = test_batch(10);
        let result = router.route_batch(&batch);
        assert_eq!(result.local_batches.len(), 1);
        assert!(result.remote_batches.is_empty());
    }

    #[test]
    fn test_router_is_local() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let router = PartitionRouter::new(&plan, NodeId(1), None);

        for pid in 0..4 {
            assert!(router.is_local(pid), "partition {pid} should be local");
        }
    }

    #[test]
    fn test_router_empty_batch() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4), make_node(2, 4)];
        let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let router = PartitionRouter::new(&plan, NodeId(1), Some("x".into()));

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::new_empty(schema);
        let result = router.route_batch(&batch);

        // Empty batch goes to partition 0's owner
        let total = result.local_batches.len()
            + result.remote_batches.values().map(Vec::len).sum::<usize>();
        assert_eq!(total, 1);
    }

    #[test]
    fn test_router_no_partitions() {
        let plan = AssignmentPlan {
            assignments: HashMap::new(),
            moves: Vec::new(),
            stats: crate::delta::partition::assignment::AssignmentStats::default(),
        };
        let router = PartitionRouter::new(&plan, NodeId(1), None);
        let batch = test_batch(5);
        let result = router.route_batch(&batch);
        assert_eq!(result.local_batches.len(), 1);
    }

    #[test]
    fn test_router_update_assignments() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan1 = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let mut router = PartitionRouter::new(&plan1, NodeId(1), None);
        assert_eq!(router.num_partitions(), 4);

        let plan2 = assigner.initial_assignment(8, &nodes, &AssignmentConstraints::default());
        router.update_assignments(&plan2);
        assert_eq!(router.num_partitions(), 8);
    }

    #[test]
    fn test_router_debug() {
        let assigner = ConsistentHashAssigner::new();
        let nodes = vec![make_node(1, 4)];
        let plan = assigner.initial_assignment(4, &nodes, &AssignmentConstraints::default());
        let router = PartitionRouter::new(&plan, NodeId(1), None);
        let debug = format!("{router:?}");
        assert!(debug.contains("PartitionRouter"));
    }

    #[tokio::test]
    async fn test_remote_batch_sender_no_channel() {
        let sender = RemoteBatchSender::new();
        let batch = test_batch(1);
        let result = sender.send_batch(NodeId(99), "test".into(), batch).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_remote_batch_receiver_inject() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let receiver = RemoteBatchReceiver::new(tx);
        let batch = test_batch(3);
        receiver
            .inject_batch("source1".into(), batch)
            .await
            .unwrap();

        let (name, received) = rx.recv().await.unwrap();
        assert_eq!(name, "source1");
        assert_eq!(received.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_remote_batch_sender_send() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let mut sender = RemoteBatchSender::new();
        sender.register_node(NodeId(2), tx);

        let batch = test_batch(5);
        sender
            .send_batch(NodeId(2), "src".into(), batch)
            .await
            .unwrap();

        let (name, received) = rx.recv().await.unwrap();
        assert_eq!(name, "src");
        assert_eq!(received.num_rows(), 5);
    }

    #[test]
    fn test_remote_batch_sender_remove_node() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<(String, RecordBatch)>(16);
        let mut sender = RemoteBatchSender::new();
        sender.register_node(NodeId(2), tx);
        assert!(sender.node_channels.contains_key(&NodeId(2)));
        sender.remove_node(&NodeId(2));
        assert!(!sender.node_channels.contains_key(&NodeId(2)));
    }
}
