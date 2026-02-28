//! Partitioned lookup strategy for distributed lookup tables.
//!
//! Routes lookup keys to the owning node via consistent hashing.
//! In embedded mode (single node), all keys are local — zero RPCs.
//!
//! ## Key Routing
//!
//! Uses xxhash (< 50ns) to map keys to partition IDs, then a
//! `PartitionMap` to resolve the owning node.

use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use bytes::Bytes;

use crate::delta::discovery::NodeId;
use crate::lookup::foyer_cache::FoyerMemoryCache;
use crate::lookup::table::{LookupResult, LookupTable};

/// Maps partition IDs to owning nodes.
#[derive(Debug, Clone)]
pub struct PartitionMap {
    /// Total number of partitions.
    pub num_partitions: u32,
    /// Partition ID → owning node.
    partition_to_node: Vec<NodeId>,
    /// This node's ID.
    local_node: NodeId,
}

impl PartitionMap {
    /// Create a new partition map.
    #[must_use]
    pub fn new(num_partitions: u32, local_node: NodeId) -> Self {
        Self {
            num_partitions,
            partition_to_node: vec![local_node; num_partitions as usize],
            local_node,
        }
    }

    /// Create from an explicit mapping.
    #[must_use]
    pub fn from_assignments(
        num_partitions: u32,
        assignments: &HashMap<u32, NodeId>,
        local_node: NodeId,
    ) -> Self {
        let mut map = vec![NodeId::UNASSIGNED; num_partitions as usize];
        for (&pid, &node) in assignments {
            if (pid as usize) < map.len() {
                map[pid as usize] = node;
            }
        }
        Self {
            num_partitions,
            partition_to_node: map,
            local_node,
        }
    }

    /// Hash a key to a partition ID.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn partition_for_key(&self, key: &[u8]) -> u32 {
        let hash = xxhash_rust::xxh3::xxh3_64(key);
        (hash % u64::from(self.num_partitions)) as u32
    }

    /// Get the owning node for a key.
    #[must_use]
    pub fn owner_of(&self, key: &[u8]) -> NodeId {
        let pid = self.partition_for_key(key);
        self.partition_to_node
            .get(pid as usize)
            .copied()
            .unwrap_or(NodeId::UNASSIGNED)
    }

    /// Check if a key is local to this node.
    #[must_use]
    pub fn is_local(&self, key: &[u8]) -> bool {
        self.owner_of(key) == self.local_node
    }

    /// Update the assignment for a partition.
    pub fn set_owner(&mut self, partition_id: u32, node_id: NodeId) {
        if let Some(slot) = self.partition_to_node.get_mut(partition_id as usize) {
            *slot = node_id;
        }
    }

    /// Get the node for a partition ID.
    #[must_use]
    pub fn node_for_partition(&self, partition_id: u32) -> NodeId {
        self.partition_to_node
            .get(partition_id as usize)
            .copied()
            .unwrap_or(NodeId::UNASSIGNED)
    }

    /// Group keys by target node.
    #[must_use]
    pub fn group_by_node<'a>(&self, keys: &'a [&[u8]]) -> FxHashMap<NodeId, Vec<&'a [u8]>> {
        let mut groups: FxHashMap<NodeId, Vec<&[u8]>> = FxHashMap::default();
        for &key in keys {
            let node = self.owner_of(key);
            groups.entry(node).or_default().push(key);
        }
        groups
    }

    /// Get all partitions owned by the local node.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn local_partitions(&self) -> Vec<u32> {
        self.partition_to_node
            .iter()
            .enumerate()
            .filter(|(_, &node)| node == self.local_node)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// Whether all partitions are local (embedded mode).
    #[must_use]
    pub fn is_embedded(&self) -> bool {
        self.partition_to_node
            .iter()
            .all(|&node| node == self.local_node)
    }
}

/// Configuration for a partitioned lookup table.
#[derive(Debug, Clone)]
pub struct PartitionedLookupConfig {
    /// Number of partitions.
    pub num_partitions: u32,
    /// Maximum keys per remote RPC batch.
    pub max_rpc_batch_size: usize,
    /// Timeout for remote RPCs.
    pub rpc_timeout_ms: u64,
    /// Maximum concurrent RPCs.
    pub max_concurrent_rpcs: usize,
}

impl Default for PartitionedLookupConfig {
    fn default() -> Self {
        Self {
            num_partitions: 256,
            max_rpc_batch_size: 500,
            rpc_timeout_ms: 5,
            max_concurrent_rpcs: 8,
        }
    }
}

/// Metrics for partitioned lookup operations.
#[derive(Debug, Default)]
pub struct PartitionedMetrics {
    /// Number of local cache hits.
    pub local_hits: std::sync::atomic::AtomicU64,
    /// Number of local cache misses.
    pub local_misses: std::sync::atomic::AtomicU64,
    /// Number of remote lookups issued.
    pub remote_lookups: std::sync::atomic::AtomicU64,
    /// Number of remote lookup failures.
    pub remote_failures: std::sync::atomic::AtomicU64,
}

/// Snapshot of partitioned metrics.
#[derive(Debug, Clone, Default)]
pub struct PartitionedMetricsSnapshot {
    /// Number of local cache hits.
    pub local_hits: u64,
    /// Number of local cache misses.
    pub local_misses: u64,
    /// Number of remote lookups issued.
    pub remote_lookups: u64,
    /// Number of remote lookup failures.
    pub remote_failures: u64,
}

impl PartitionedMetrics {
    /// Take a snapshot of the current metrics.
    #[must_use]
    pub fn snapshot(&self) -> PartitionedMetricsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        PartitionedMetricsSnapshot {
            local_hits: self.local_hits.load(Relaxed),
            local_misses: self.local_misses.load(Relaxed),
            remote_lookups: self.remote_lookups.load(Relaxed),
            remote_failures: self.remote_failures.load(Relaxed),
        }
    }
}

/// A partitioned lookup table that routes keys to owning nodes.
///
/// In embedded mode (single node), all lookups are local cache hits.
/// In distributed mode, remote lookups are batched per target node.
pub struct PartitionedLookupTable {
    /// Partition-to-node routing map.
    partition_map: PartitionMap,
    /// Local cache for keys owned by this node.
    local_cache: Arc<FoyerMemoryCache>,
    /// Configuration.
    #[allow(dead_code)]
    config: PartitionedLookupConfig,
    /// Metrics.
    metrics: Arc<PartitionedMetrics>,
}

impl PartitionedLookupTable {
    /// Create a new partitioned lookup table.
    #[must_use]
    pub fn new(
        partition_map: PartitionMap,
        local_cache: Arc<FoyerMemoryCache>,
        config: PartitionedLookupConfig,
    ) -> Self {
        Self {
            partition_map,
            local_cache,
            config,
            metrics: Arc::new(PartitionedMetrics::default()),
        }
    }

    /// Get the partition map.
    #[must_use]
    pub fn partition_map(&self) -> &PartitionMap {
        &self.partition_map
    }

    /// Get the metrics.
    #[must_use]
    pub fn metrics(&self) -> &PartitionedMetrics {
        &self.metrics
    }
}

impl std::fmt::Debug for PartitionedLookupTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedLookupTable")
            .field("partition_map", &self.partition_map)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LookupTable for PartitionedLookupTable {
    fn get_cached(&self, key: &[u8]) -> LookupResult {
        if self.partition_map.is_local(key) {
            // Local key — check cache
            let result = self.local_cache.get_cached(key);
            if result.is_hit() {
                self.metrics
                    .local_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                self.metrics
                    .local_misses
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            result
        } else {
            // Remote key — not in local cache
            self.metrics
                .remote_lookups
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            LookupResult::Pending
        }
    }

    fn get(&self, key: &[u8]) -> LookupResult {
        if self.partition_map.is_local(key) {
            self.local_cache.get(key)
        } else {
            // Remote lookup requires async — return Pending
            LookupResult::Pending
        }
    }

    fn insert(&self, key: &[u8], value: Bytes) {
        // Only insert if the key is local
        if self.partition_map.is_local(key) {
            self.local_cache.insert(key, value);
        }
    }

    fn invalidate(&self, key: &[u8]) {
        if self.partition_map.is_local(key) {
            self.local_cache.invalidate(key);
        }
    }

    fn len(&self) -> usize {
        self.local_cache.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lookup::foyer_cache::FoyerMemoryCacheConfig;

    fn make_cache() -> Arc<FoyerMemoryCache> {
        Arc::new(FoyerMemoryCache::new(
            0,
            FoyerMemoryCacheConfig {
                capacity: 1000,
                shards: 1,
            },
        ))
    }

    #[test]
    fn test_partition_map_basic() {
        let map = PartitionMap::new(256, NodeId(1));
        assert_eq!(map.num_partitions, 256);
        assert!(map.is_embedded());
    }

    #[test]
    fn test_partition_for_key_determinism() {
        let map = PartitionMap::new(256, NodeId(1));
        let p1 = map.partition_for_key(b"hello");
        let p2 = map.partition_for_key(b"hello");
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_partition_for_key_range() {
        let map = PartitionMap::new(16, NodeId(1));
        for i in 0..100 {
            let key = format!("key-{i}");
            let pid = map.partition_for_key(key.as_bytes());
            assert!(pid < 16);
        }
    }

    #[test]
    fn test_is_local_embedded() {
        let map = PartitionMap::new(256, NodeId(1));
        assert!(map.is_local(b"any-key"));
        assert!(map.is_embedded());
    }

    #[test]
    fn test_is_local_distributed() {
        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(1));
        assignments.insert(1, NodeId(2));
        let map = PartitionMap::from_assignments(2, &assignments, NodeId(1));

        assert!(!map.is_embedded());
        // Some keys will be local, some remote
        let mut has_local = false;
        let mut has_remote = false;
        for i in 0..100 {
            let key = format!("key-{i}");
            if map.is_local(key.as_bytes()) {
                has_local = true;
            } else {
                has_remote = true;
            }
        }
        assert!(has_local);
        assert!(has_remote);
    }

    #[test]
    fn test_group_by_node() {
        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(1));
        assignments.insert(1, NodeId(2));
        let map = PartitionMap::from_assignments(2, &assignments, NodeId(1));

        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let groups = map.group_by_node(&keys);

        let total: usize = groups.values().map(Vec::len).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_local_partitions() {
        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(1));
        assignments.insert(1, NodeId(2));
        assignments.insert(2, NodeId(1));
        assignments.insert(3, NodeId(2));
        let map = PartitionMap::from_assignments(4, &assignments, NodeId(1));

        let local = map.local_partitions();
        assert_eq!(local.len(), 2);
        assert!(local.contains(&0));
        assert!(local.contains(&2));
    }

    #[test]
    fn test_set_owner() {
        let mut map = PartitionMap::new(4, NodeId(1));
        assert!(map.is_embedded());

        map.set_owner(1, NodeId(2));
        assert!(!map.is_embedded());
        assert_eq!(map.node_for_partition(1), NodeId(2));
    }

    #[test]
    fn test_partitioned_lookup_local() {
        let map = PartitionMap::new(256, NodeId(1));
        let cache = make_cache();
        let table = PartitionedLookupTable::new(map, cache, PartitionedLookupConfig::default());

        // Insert a value
        table.insert(b"key1", Bytes::from_static(b"value1"));

        // Should be a local hit
        let result = table.get_cached(b"key1");
        assert!(result.is_hit());

        let metrics = table.metrics().snapshot();
        assert_eq!(metrics.local_hits, 1);
    }

    #[test]
    fn test_partitioned_lookup_miss() {
        let map = PartitionMap::new(256, NodeId(1));
        let cache = make_cache();
        let table = PartitionedLookupTable::new(map, cache, PartitionedLookupConfig::default());

        let result = table.get_cached(b"nonexistent");
        assert!(!result.is_hit());

        let metrics = table.metrics().snapshot();
        assert_eq!(metrics.local_misses, 1);
    }

    #[test]
    fn test_partitioned_lookup_remote_pending() {
        let mut assignments = HashMap::new();
        // Make all partitions remote
        for i in 0..256u32 {
            assignments.insert(i, NodeId(2));
        }
        let map = PartitionMap::from_assignments(256, &assignments, NodeId(1));
        let cache = make_cache();
        let table = PartitionedLookupTable::new(map, cache, PartitionedLookupConfig::default());

        let result = table.get_cached(b"key1");
        assert_eq!(result, LookupResult::Pending);

        let metrics = table.metrics().snapshot();
        assert_eq!(metrics.remote_lookups, 1);
    }

    #[test]
    fn test_partitioned_insert_only_local() {
        let mut assignments = HashMap::new();
        for i in 0..256u32 {
            assignments.insert(i, NodeId(2));
        }
        let map = PartitionMap::from_assignments(256, &assignments, NodeId(1));
        let cache = make_cache();
        let table = PartitionedLookupTable::new(map, cache, PartitionedLookupConfig::default());

        // Insert to a remote partition should be no-op
        table.insert(b"key1", Bytes::from_static(b"value1"));
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_partitioned_invalidate() {
        let map = PartitionMap::new(256, NodeId(1));
        let cache = make_cache();
        let table = PartitionedLookupTable::new(map, cache, PartitionedLookupConfig::default());

        table.insert(b"key1", Bytes::from_static(b"value1"));
        assert!(table.get_cached(b"key1").is_hit());

        table.invalidate(b"key1");
        assert!(!table.get_cached(b"key1").is_hit());
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = PartitionedMetrics::default();
        metrics
            .local_hits
            .fetch_add(10, std::sync::atomic::Ordering::Relaxed);
        metrics
            .remote_lookups
            .fetch_add(5, std::sync::atomic::Ordering::Relaxed);

        let snap = metrics.snapshot();
        assert_eq!(snap.local_hits, 10);
        assert_eq!(snap.remote_lookups, 5);
    }

    #[test]
    fn test_config_default() {
        let config = PartitionedLookupConfig::default();
        assert_eq!(config.num_partitions, 256);
        assert_eq!(config.max_rpc_batch_size, 500);
    }
}
