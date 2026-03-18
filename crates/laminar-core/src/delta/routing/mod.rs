//! Partition-aware data routing for cluster mode.
//!
//! Routes incoming `RecordBatch` data to the correct partition owners by
//! hashing partition keys, looking up assignments on the consistent hash
//! ring, and splitting batches by destination node using zero-copy
//! `arrow::compute::take`.

#![allow(clippy::disallowed_types)] // cold path: routing metadata
#![allow(dead_code)] // assigner field stored for future rebalance use

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::{Array, RecordBatch, UInt32Array};
use arrow::compute::take;

use crate::delta::discovery::NodeId;
use crate::delta::partition::assignment::ConsistentHashAssigner;
use crate::delta::partition::guard::EpochError;
use crate::delta::partition_for_key;

/// Result of routing a batch through the partition router.
#[derive(Debug)]
pub struct RoutingResult {
    /// Rows owned by this node, ready for local processing.
    pub local_batches: Vec<RecordBatch>,
    /// Rows destined for remote peer nodes.
    pub remote_batches: HashMap<NodeId, Vec<RecordBatch>>,
}

/// Errors that can occur during routing.
#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    /// The partition key column was not found in the batch.
    #[error("partition key column '{0}' not found in batch")]
    PartitionKeyNotFound(String),

    /// The batch has no rows.
    #[error("empty batch")]
    EmptyBatch,

    /// Arrow compute error during batch splitting.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// No assignment exists for a partition.
    #[error("partition {0} has no assignment")]
    UnassignedPartition(u32),

    /// Epoch fencing error.
    #[error("epoch error: {0}")]
    Epoch(#[from] EpochError),

    /// The partition key column has an unsupported data type.
    #[error("unsupported partition key type: {0}")]
    UnsupportedKeyType(String),
}

/// State of a connection to a peer node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Actively connected and ready for data transfer.
    Connected,
    /// Connection attempt in progress.
    Connecting,
    /// Not connected (will reconnect on next send).
    Disconnected,
}

/// A connection handle to a peer node.
///
/// Currently a placeholder that will later wrap gRPC clients for
/// inter-node batch transfer.
#[derive(Debug)]
pub struct PeerConnection {
    /// The peer's node identifier.
    pub node_id: NodeId,
    /// The peer's gRPC address.
    pub address: String,
    /// Current connection state.
    pub state: ConnectionState,
}

impl PeerConnection {
    /// Create a new disconnected peer connection.
    #[must_use]
    pub fn new(node_id: NodeId, address: String) -> Self {
        Self {
            node_id,
            address,
            state: ConnectionState::Disconnected,
        }
    }
}

/// Partition-aware data router.
///
/// Sits between source ingestion and the streaming coordinator. Splits
/// incoming `RecordBatch` data by partition key, routing rows to their
/// owning nodes based on the consistent hash ring assignment.
#[derive(Debug)]
pub struct PartitionRouter {
    /// This node's identifier.
    local_node: NodeId,
    /// Partition assignment via consistent hash ring.
    assigner: Arc<ConsistentHashAssigner>,
    /// Current partition-to-node assignment map.
    assignments: Arc<RwLock<HashMap<u32, NodeId>>>,
    /// Peer connections keyed by node ID.
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
    /// Name of the column used as partition key.
    partition_key: String,
    /// Total number of partitions in the cluster.
    num_partitions: u32,
}

impl PartitionRouter {
    /// Create a new partition router.
    #[must_use]
    pub fn new(
        local_node: NodeId,
        assigner: Arc<ConsistentHashAssigner>,
        assignments: HashMap<u32, NodeId>,
        partition_key: String,
        num_partitions: u32,
    ) -> Self {
        Self {
            local_node,
            assigner,
            assignments: Arc::new(RwLock::new(assignments)),
            peers: Arc::new(RwLock::new(HashMap::new())),
            partition_key,
            num_partitions,
        }
    }

    /// Update the partition-to-node assignment map (e.g., after rebalance).
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub fn update_assignments(&self, new_assignments: HashMap<u32, NodeId>) {
        let mut assignments = self.assignments.write().unwrap();
        *assignments = new_assignments;
    }

    /// Add or update a peer connection.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub fn upsert_peer(&self, connection: PeerConnection) {
        let mut peers = self.peers.write().unwrap();
        peers.insert(connection.node_id, connection);
    }

    /// Remove a peer connection.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub fn remove_peer(&self, node_id: &NodeId) {
        let mut peers = self.peers.write().unwrap();
        peers.remove(node_id);
    }

    /// Route a `RecordBatch` to the correct partition owners.
    ///
    /// Extracts the partition key column, hashes each row to determine
    /// the owning partition and node, then splits the batch using
    /// `arrow::compute::take` for zero-copy slicing.
    ///
    /// # Errors
    ///
    /// Returns `RoutingError` if the partition key column is missing,
    /// the batch is empty, or a partition has no assignment.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub fn route_batch(
        &self,
        batch: &RecordBatch,
        _epoch: u64,
    ) -> Result<RoutingResult, RoutingError> {
        if batch.num_rows() == 0 {
            return Ok(RoutingResult {
                local_batches: vec![],
                remote_batches: HashMap::new(),
            });
        }

        // 1. Extract partition key column
        let key_col_idx = batch
            .schema()
            .index_of(&self.partition_key)
            .map_err(|_| RoutingError::PartitionKeyNotFound(self.partition_key.clone()))?;
        let key_col = batch.column(key_col_idx);

        // 2. Hash each row to determine destination node
        let assignments = self.assignments.read().unwrap();
        let mut node_indices: HashMap<NodeId, Vec<u32>> = HashMap::new();

        for row_idx in 0..batch.num_rows() {
            // Get the key bytes for this row
            let key_bytes = Self::extract_key_bytes(key_col, row_idx)?;
            let partition_id = partition_for_key(&key_bytes, self.num_partitions);

            // 3. Look up partition → node assignment
            let owner = assignments
                .get(&partition_id)
                .copied()
                .ok_or(RoutingError::UnassignedPartition(partition_id))?;

            #[allow(clippy::cast_possible_truncation)] // row index bounded by RecordBatch size
            node_indices.entry(owner).or_default().push(row_idx as u32);
        }

        drop(assignments);

        // 4. Split batch by destination node using arrow::compute::take
        let mut local_batches = Vec::new();
        let mut remote_batches: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();

        for (node_id, indices) in node_indices {
            let indices_array = UInt32Array::from(indices);
            let routed_batch = Self::take_batch(batch, &indices_array)?;

            if node_id == self.local_node {
                local_batches.push(routed_batch);
            } else {
                remote_batches
                    .entry(node_id)
                    .or_default()
                    .push(routed_batch);
            }
        }

        Ok(RoutingResult {
            local_batches,
            remote_batches,
        })
    }

    /// Extract key bytes from an array at the given row index.
    ///
    /// Converts the value to its byte representation for hashing.
    ///
    /// # Errors
    ///
    /// Returns `RoutingError::UnsupportedKeyType` if the array's data type
    /// cannot be converted to key bytes.
    fn extract_key_bytes(array: &dyn Array, row_idx: usize) -> Result<Vec<u8>, RoutingError> {
        use arrow::array as aa;

        // Fast paths for common partition key types
        if let Some(arr) = array.as_any().downcast_ref::<aa::StringArray>() {
            return Ok(arr.value(row_idx).as_bytes().to_vec());
        }
        if let Some(arr) = array.as_any().downcast_ref::<aa::LargeStringArray>() {
            return Ok(arr.value(row_idx).as_bytes().to_vec());
        }
        if let Some(arr) = array.as_any().downcast_ref::<aa::Int64Array>() {
            return Ok(arr.value(row_idx).to_le_bytes().to_vec());
        }
        if let Some(arr) = array.as_any().downcast_ref::<aa::Int32Array>() {
            return Ok(arr.value(row_idx).to_le_bytes().to_vec());
        }
        if let Some(arr) = array.as_any().downcast_ref::<aa::UInt64Array>() {
            return Ok(arr.value(row_idx).to_le_bytes().to_vec());
        }
        if let Some(arr) = array.as_any().downcast_ref::<aa::UInt32Array>() {
            return Ok(arr.value(row_idx).to_le_bytes().to_vec());
        }
        if let Some(arr) = array.as_any().downcast_ref::<aa::BinaryArray>() {
            return Ok(arr.value(row_idx).to_vec());
        }
        if let Some(arr) = array.as_any().downcast_ref::<aa::LargeBinaryArray>() {
            return Ok(arr.value(row_idx).to_vec());
        }

        Err(RoutingError::UnsupportedKeyType(format!(
            "{:?}",
            array.data_type()
        )))
    }

    /// Take rows from a batch at the given indices using `arrow::compute::take`.
    fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, RoutingError> {
        let schema = batch.schema();
        let columns: Vec<Arc<dyn Array>> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), indices, None))
            .collect::<Result<_, _>>()?;
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    /// Get the local node ID.
    #[must_use]
    pub fn local_node(&self) -> NodeId {
        self.local_node
    }

    /// Get the partition key column name.
    #[must_use]
    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    /// Get the number of partitions.
    #[must_use]
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Check if a given partition is owned locally.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    #[must_use]
    pub fn is_local_partition(&self, partition_id: u32) -> bool {
        let assignments = self.assignments.read().unwrap();
        assignments
            .get(&partition_id)
            .is_some_and(|owner| *owner == self.local_node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::delta::discovery::{NodeInfo, NodeMetadata, NodeState};

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

    fn make_test_batch(keys: &[&str], values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_router_with_assignments(
        local_id: u64,
        assignments: HashMap<u32, NodeId>,
        num_partitions: u32,
    ) -> PartitionRouter {
        let assigner = Arc::new(ConsistentHashAssigner::new());
        PartitionRouter::new(
            NodeId(local_id),
            assigner,
            assignments,
            "key".to_string(),
            num_partitions,
        )
    }

    #[test]
    fn test_partition_for_key_deterministic() {
        let a = partition_for_key(b"hello", 16);
        let b = partition_for_key(b"hello", 16);
        assert_eq!(a, b);
    }

    #[test]
    fn test_partition_for_key_distribution() {
        let num_partitions = 8;
        let mut counts = vec![0u32; num_partitions as usize];
        for i in 0..1000 {
            let key = format!("key-{i}");
            let pid = partition_for_key(key.as_bytes(), num_partitions);
            assert!(pid < num_partitions);
            counts[pid as usize] += 1;
        }
        // Every partition should get some keys
        for (pid, count) in counts.iter().enumerate() {
            assert!(*count > 0, "partition {pid} got no keys");
        }
    }

    #[test]
    fn test_partition_for_key_range() {
        for np in [1, 2, 4, 16, 128, 1024] {
            for i in 0..200 {
                let key = format!("test-key-{i}");
                let pid = partition_for_key(key.as_bytes(), np);
                assert!(pid < np, "pid {pid} >= num_partitions {np}");
            }
        }
    }

    #[test]
    fn test_route_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::new_empty(schema);

        let assignments = HashMap::new();
        let router = make_router_with_assignments(1, assignments, 4);

        let result = router.route_batch(&batch, 1).unwrap();
        assert!(result.local_batches.is_empty());
        assert!(result.remote_batches.is_empty());
    }

    #[test]
    fn test_route_all_local() {
        let batch = make_test_batch(&["a", "b", "c"], &[1, 2, 3]);
        let num_partitions = 4;
        let local_node = NodeId(1);

        // Assign ALL partitions to the local node
        let mut assignments = HashMap::new();
        for pid in 0..num_partitions {
            assignments.insert(pid, local_node);
        }

        let router = make_router_with_assignments(1, assignments, num_partitions);
        let result = router.route_batch(&batch, 1).unwrap();

        // All rows should be local
        let total_local: usize = result.local_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_local, 3);
        assert!(result.remote_batches.is_empty());
    }

    #[test]
    fn test_route_all_remote() {
        let batch = make_test_batch(&["a", "b", "c"], &[1, 2, 3]);
        let num_partitions = 4;
        let remote_node = NodeId(2);

        // Assign ALL partitions to a remote node
        let mut assignments = HashMap::new();
        for pid in 0..num_partitions {
            assignments.insert(pid, remote_node);
        }

        let router = make_router_with_assignments(1, assignments, num_partitions);
        let result = router.route_batch(&batch, 1).unwrap();

        assert!(result.local_batches.is_empty());
        let total_remote: usize = result
            .remote_batches
            .values()
            .flat_map(|v| v.iter())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_remote, 3);
        assert!(result.remote_batches.contains_key(&remote_node));
    }

    #[test]
    fn test_route_mixed_local_remote() {
        // Use enough keys to statistically hit multiple partitions
        let keys: Vec<&str> = vec![
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
        ];
        let values: Vec<i64> = (0..keys.len() as i64).collect();
        let batch = make_test_batch(&keys, &values);

        let num_partitions = 4;
        let local_node = NodeId(1);
        let remote_node = NodeId(2);

        // Assign even partitions to local, odd to remote
        let mut assignments = HashMap::new();
        for pid in 0..num_partitions {
            if pid % 2 == 0 {
                assignments.insert(pid, local_node);
            } else {
                assignments.insert(pid, remote_node);
            }
        }

        let router = make_router_with_assignments(1, assignments, num_partitions);
        let result = router.route_batch(&batch, 1).unwrap();

        let total_local: usize = result.local_batches.iter().map(|b| b.num_rows()).sum();
        let total_remote: usize = result
            .remote_batches
            .values()
            .flat_map(|v| v.iter())
            .map(|b| b.num_rows())
            .sum();

        // All rows accounted for
        assert_eq!(total_local + total_remote, keys.len());
        // With 8 keys and 4 partitions, both sides should get some
        assert!(total_local > 0, "expected some local rows");
        assert!(total_remote > 0, "expected some remote rows");
    }

    #[test]
    fn test_route_preserves_schema() {
        let batch = make_test_batch(&["a", "b"], &[1, 2]);
        let num_partitions = 4;

        let mut assignments = HashMap::new();
        for pid in 0..num_partitions {
            assignments.insert(pid, NodeId(1));
        }

        let router = make_router_with_assignments(1, assignments, num_partitions);
        let result = router.route_batch(&batch, 1).unwrap();

        for local_batch in &result.local_batches {
            assert_eq!(local_batch.schema(), batch.schema());
        }
    }

    #[test]
    fn test_route_missing_partition_key() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "not_the_key",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

        let router = make_router_with_assignments(1, HashMap::new(), 4);
        let err = router.route_batch(&batch, 1).unwrap_err();
        assert!(matches!(err, RoutingError::PartitionKeyNotFound(_)));
    }

    #[test]
    fn test_route_unassigned_partition() {
        let batch = make_test_batch(&["a"], &[1]);
        // Empty assignments — no partition is assigned
        let router = make_router_with_assignments(1, HashMap::new(), 4);
        let err = router.route_batch(&batch, 1).unwrap_err();
        assert!(matches!(err, RoutingError::UnassignedPartition(_)));
    }

    #[test]
    fn test_route_int_partition_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 300])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let num_partitions = 4;
        let mut assignments = HashMap::new();
        for pid in 0..num_partitions {
            assignments.insert(pid, NodeId(1));
        }

        let assigner = Arc::new(ConsistentHashAssigner::new());
        let router = PartitionRouter::new(
            NodeId(1),
            assigner,
            assignments,
            "id".to_string(),
            num_partitions,
        );

        let result = router.route_batch(&batch, 1).unwrap();
        let total: usize = result.local_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_route_multiple_remote_nodes() {
        // We need distinct keys for distribution
        let key_strings: Vec<String> = (0..100).map(|i| format!("key-{i}")).collect();
        let key_refs: Vec<&str> = key_strings.iter().map(|s| s.as_str()).collect();
        let values: Vec<i64> = (0..100).collect();
        let batch = make_test_batch(&key_refs, &values);

        let num_partitions = 12;
        let nodes = vec![make_node(1, 4), make_node(2, 4), make_node(3, 4)];

        let assigner = Arc::new(ConsistentHashAssigner::new());
        let plan = assigner.initial_assignment(
            num_partitions,
            &nodes,
            &crate::delta::partition::assignment::AssignmentConstraints::default(),
        );

        let router = PartitionRouter::new(
            NodeId(1),
            Arc::clone(&assigner),
            plan.assignments,
            "key".to_string(),
            num_partitions,
        );

        let result = router.route_batch(&batch, 1).unwrap();

        let total_local: usize = result.local_batches.iter().map(|b| b.num_rows()).sum();
        let total_remote: usize = result
            .remote_batches
            .values()
            .flat_map(|v| v.iter())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_local + total_remote, 100);
    }

    #[test]
    fn test_update_assignments() {
        let router = make_router_with_assignments(1, HashMap::new(), 4);

        let mut new_assignments = HashMap::new();
        new_assignments.insert(0, NodeId(1));
        new_assignments.insert(1, NodeId(2));
        router.update_assignments(new_assignments);

        assert!(router.is_local_partition(0));
        assert!(!router.is_local_partition(1));
    }

    #[test]
    fn test_peer_management() {
        let router = make_router_with_assignments(1, HashMap::new(), 4);

        let conn = PeerConnection::new(NodeId(2), "127.0.0.1:9002".to_string());
        assert_eq!(conn.state, ConnectionState::Disconnected);

        router.upsert_peer(conn);
        {
            let peers = router.peers.read().unwrap();
            assert!(peers.contains_key(&NodeId(2)));
        }

        router.remove_peer(&NodeId(2));
        {
            let peers = router.peers.read().unwrap();
            assert!(!peers.contains_key(&NodeId(2)));
        }
    }

    #[test]
    fn test_is_local_partition() {
        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(1));
        assignments.insert(1, NodeId(2));
        assignments.insert(2, NodeId(1));

        let router = make_router_with_assignments(1, assignments, 4);

        assert!(router.is_local_partition(0));
        assert!(!router.is_local_partition(1));
        assert!(router.is_local_partition(2));
        assert!(!router.is_local_partition(3)); // not in assignments
    }

    #[test]
    fn test_take_batch() {
        let batch = make_test_batch(&["a", "b", "c", "d"], &[1, 2, 3, 4]);
        let indices = UInt32Array::from(vec![0, 2]);
        let result = PartitionRouter::take_batch(&batch, &indices).unwrap();

        assert_eq!(result.num_rows(), 2);
        let keys = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "a");
        assert_eq!(keys.value(1), "c");

        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 3);
    }
}
