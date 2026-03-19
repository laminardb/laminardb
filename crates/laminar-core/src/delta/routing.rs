//! Partition-aware batch routing for distributed mode.
//!
//! Routes incoming `RecordBatch`es to the correct node based on a partition
//! key column and the current partition assignment. Batches destined for the
//! local node are returned directly; batches for remote nodes are grouped by
//! target `NodeId` for async gRPC forwarding.

#![allow(clippy::disallowed_types)] // cold path: routing config and assignment maps
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::RwLock;

use super::discovery::NodeId;

/// Configuration for partition routing.
#[derive(Debug, Clone)]
pub struct RoutingConfig {
    /// Name of the column used as partition key.
    pub partition_key_column: String,
    /// Total number of partitions in the cluster.
    pub num_partitions: u32,
}

/// Result of routing a batch: local batches to process in-place and
/// remote batches grouped by target node.
#[derive(Debug)]
pub struct RoutedBatches {
    /// Batches destined for this node (process locally).
    pub local: Vec<RecordBatch>,
    /// Batches destined for remote nodes, keyed by target `NodeId`.
    pub remote: HashMap<NodeId, Vec<RecordBatch>>,
}

/// Partition router that splits batches by partition key ownership.
///
/// Thread-safe: the assignment map is behind an `RwLock` so the Raft
/// rebalance path can update it while the hot path reads it.
#[derive(Debug, Clone)]
pub struct PartitionRouter {
    config: RoutingConfig,
    local_node: NodeId,
    /// Current partition-to-node assignment (updated on rebalance).
    assignments: Arc<RwLock<HashMap<u32, NodeId>>>,
}

impl PartitionRouter {
    /// Create a new router.
    ///
    /// # Panics
    ///
    /// Panics if `config.num_partitions` is zero (would cause division-by-zero
    /// in `partition_for_hash`).
    #[must_use]
    pub fn new(config: RoutingConfig, local_node: NodeId) -> Self {
        assert!(
            config.num_partitions > 0,
            "RoutingConfig::num_partitions must be greater than zero"
        );
        Self {
            config,
            local_node,
            assignments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update the partition assignment map (called after rebalance).
    pub fn update_assignments(&self, new_assignments: HashMap<u32, NodeId>) {
        let mut guard = self.assignments.write();
        *guard = new_assignments;
    }

    /// Get the current assignments (for testing/inspection).
    #[must_use]
    pub fn assignments(&self) -> HashMap<u32, NodeId> {
        self.assignments.read().clone()
    }

    /// The local node ID.
    #[must_use]
    pub fn local_node(&self) -> NodeId {
        self.local_node
    }

    /// Compute the partition ID for a hash value.
    #[inline]
    #[must_use]
    pub fn partition_for_hash(&self, hash: u64) -> u32 {
        #[allow(clippy::cast_possible_truncation)]
        {
            (hash % u64::from(self.config.num_partitions)) as u32
        }
    }

    /// Look up which node owns a partition.
    #[must_use]
    pub fn owner_of(&self, partition_id: u32) -> Option<NodeId> {
        self.assignments.read().get(&partition_id).copied()
    }

    /// Route a batch based on its partition key column.
    ///
    /// Rows are hashed by the partition key, mapped to a partition ID,
    /// and then routed to the owning node. The batch is split into
    /// per-node sub-batches.
    ///
    /// If the partition key column is not found, the entire batch is
    /// treated as local (graceful fallback for unpartitioned sources).
    ///
    /// # Panics
    ///
    /// Panics if `arrow::compute::take` fails on a valid column (should
    /// not happen with well-formed Arrow arrays).
    #[must_use]
    pub fn route_batch(&self, batch: &RecordBatch) -> RoutedBatches {
        let schema = batch.schema();
        let Some(col_idx) = schema
            .fields()
            .iter()
            .position(|f| f.name() == &self.config.partition_key_column)
        else {
            // No partition key column — treat entire batch as local.
            return RoutedBatches {
                local: vec![batch.clone()],
                remote: HashMap::new(),
            };
        };

        let assignments = self.assignments.read();
        if assignments.is_empty() {
            // No assignments yet — treat as local.
            return RoutedBatches {
                local: vec![batch.clone()],
                remote: HashMap::new(),
            };
        }

        // Build per-node row indices.
        let num_rows = batch.num_rows();
        let mut node_rows: HashMap<NodeId, Vec<usize>> = HashMap::new();
        let column = batch.column(col_idx);

        for row in 0..num_rows {
            let Some(hash) = Self::hash_column_value(column, row) else {
                // Unsupported partition key type — route entire batch locally.
                tracing::warn!(
                    column = %self.config.partition_key_column,
                    data_type = ?column.data_type(),
                    "unsupported partition key column type, routing entire batch locally"
                );
                drop(assignments);
                return RoutedBatches {
                    local: vec![batch.clone()],
                    remote: HashMap::new(),
                };
            };
            let partition_id = self.partition_for_hash(hash);
            let owner = assignments
                .get(&partition_id)
                .copied()
                .unwrap_or(self.local_node);
            node_rows.entry(owner).or_default().push(row);
        }

        drop(assignments);

        // Build per-node sub-batches using take().
        let mut local = Vec::new();
        let mut remote: HashMap<NodeId, Vec<RecordBatch>> = HashMap::new();

        for (node_id, rows) in node_rows {
            let indices = arrow_array::UInt32Array::from(
                rows.iter()
                    .map(|&r| {
                        #[allow(clippy::cast_possible_truncation)]
                        {
                            r as u32
                        }
                    })
                    .collect::<Vec<u32>>(),
            );
            let columns: Vec<arrow_array::ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| arrow::compute::take(col.as_ref(), &indices, None).unwrap())
                .collect();
            if let Ok(sub_batch) = RecordBatch::try_new(schema.clone(), columns) {
                if node_id == self.local_node {
                    local.push(sub_batch);
                } else {
                    remote.entry(node_id).or_default().push(sub_batch);
                }
            }
        }

        RoutedBatches { local, remote }
    }

    /// Hash a single column value for partition assignment.
    ///
    /// Uses a simple but deterministic hash. Supports string, integer,
    /// and binary column types. Returns `None` for unsupported types.
    fn hash_column_value(column: &dyn arrow_array::Array, row: usize) -> Option<u64> {
        use arrow_array::types::UInt64Type;

        if column.is_null(row) {
            return Some(0);
        }

        // Try common types in order of likelihood.
        if let Some(arr) = column.as_any().downcast_ref::<arrow_array::StringArray>() {
            return Some(Self::fnv1a(arr.value(row).as_bytes()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<arrow_array::Int64Array>() {
            return Some(Self::fnv1a(&arr.value(row).to_le_bytes()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<arrow_array::Int32Array>() {
            return Some(Self::fnv1a(&arr.value(row).to_le_bytes()));
        }
        if let Some(arr) = column
            .as_any()
            .downcast_ref::<arrow_array::GenericByteArray<arrow_array::types::Utf8Type>>()
        {
            return Some(Self::fnv1a(arr.value(row).as_bytes()));
        }
        if let Some(arr) = column
            .as_any()
            .downcast_ref::<arrow_array::PrimitiveArray<UInt64Type>>()
        {
            return Some(Self::fnv1a(&arr.value(row).to_le_bytes()));
        }

        None
    }

    /// FNV-1a hash (same algorithm used in partition assignment).
    #[inline]
    fn fnv1a(data: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &byte in data {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::Array;

    fn make_router(num_partitions: u32) -> PartitionRouter {
        let config = RoutingConfig {
            partition_key_column: "key".into(),
            num_partitions,
        };
        PartitionRouter::new(config, NodeId(1))
    }

    fn make_batch(keys: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let key_array = arrow_array::StringArray::from(keys);
        let value_array = arrow_array::Int64Array::from(vec![1i64; key_array.len()]);
        RecordBatch::try_new(schema, vec![Arc::new(key_array), Arc::new(value_array)]).unwrap()
    }

    #[test]
    fn test_route_batch_no_assignments() {
        let router = make_router(4);
        let batch = make_batch(vec!["a", "b", "c"]);
        let routed = router.route_batch(&batch);
        assert_eq!(routed.local.len(), 1);
        assert!(routed.remote.is_empty());
        assert_eq!(routed.local[0].num_rows(), 3);
    }

    #[test]
    fn test_route_batch_all_local() {
        let router = make_router(4);
        let mut assignments = HashMap::new();
        for pid in 0..4 {
            assignments.insert(pid, NodeId(1)); // all local
        }
        router.update_assignments(assignments);

        let batch = make_batch(vec!["a", "b", "c", "d"]);
        let routed = router.route_batch(&batch);
        assert!(routed.remote.is_empty());
        let total_rows: usize = routed.local.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_route_batch_split() {
        let router = make_router(4);
        let mut assignments = HashMap::new();
        // Split: partitions 0,1 -> node 1 (local), partitions 2,3 -> node 2
        assignments.insert(0, NodeId(1));
        assignments.insert(1, NodeId(1));
        assignments.insert(2, NodeId(2));
        assignments.insert(3, NodeId(2));
        router.update_assignments(assignments);

        let batch = make_batch(vec!["a", "b", "c", "d", "e", "f"]);
        let routed = router.route_batch(&batch);

        let local_rows: usize = routed.local.iter().map(RecordBatch::num_rows).sum();
        let remote_rows: usize = routed
            .remote
            .values()
            .flat_map(|v| v.iter())
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(local_rows + remote_rows, 6);
    }

    #[test]
    fn test_route_batch_no_key_column() {
        let router = make_router(4);
        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(2));
        router.update_assignments(assignments);

        // Batch without "key" column
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let routed = router.route_batch(&batch);
        // Falls back to local
        assert_eq!(routed.local.len(), 1);
        assert!(routed.remote.is_empty());
    }

    #[test]
    fn test_partition_for_hash() {
        let router = make_router(8);
        for i in 0..100u64 {
            let pid = router.partition_for_hash(i);
            assert!(pid < 8);
        }
    }

    #[test]
    fn test_fnv1a_deterministic() {
        let h1 = PartitionRouter::fnv1a(b"hello");
        let h2 = PartitionRouter::fnv1a(b"hello");
        assert_eq!(h1, h2);
        let h3 = PartitionRouter::fnv1a(b"world");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_update_assignments() {
        let router = make_router(4);
        assert!(router.assignments().is_empty());

        let mut map = HashMap::new();
        map.insert(0, NodeId(1));
        map.insert(1, NodeId(2));
        router.update_assignments(map.clone());
        assert_eq!(router.assignments(), map);
    }
}
