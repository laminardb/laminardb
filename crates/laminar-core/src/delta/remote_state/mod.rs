//! # Remote State Access
//!
//! Cross-node state access for cluster mode. When a lookup join needs
//! state owned by another node, [`RemoteStateProxy`] transparently
//! routes reads to the local store or a remote node based on partition
//! ownership.
//!
//! ## Components
//!
//! - [`PartitionMap`]: Partition-to-node routing table
//! - [`RemoteStateProxy`]: [`StateStore`] that delegates to local or remote
//! - [`RemoteLookupSource`]: [`LookupSourceDyn`] for cross-node enrichment
//!
//! ## Cache Strategy
//!
//! Remote lookups are cached in a bounded LRU with configurable capacity.
//! Cache invalidation is epoch-based: when a partition's epoch advances
//! (ownership change), all cached entries for that partition are flushed.

#![allow(clippy::disallowed_types)] // cold path: remote state routing

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;

use crate::delta::discovery::NodeId;
use crate::lookup::predicate::Predicate;
use crate::lookup::source::ColumnId;
use crate::lookup::source::{LookupError, LookupSourceDyn};
use crate::state::{StateError, StateSnapshot, StateStore};

/// Default LRU cache capacity for remote state entries.
const DEFAULT_CACHE_CAPACITY: usize = 10_000;

/// Partition-to-node routing table.
///
/// Maps each partition ID to the node that currently owns it, along
/// with the epoch at which ownership was established. Updated from
/// the Raft state machine on membership changes.
#[derive(Debug, Clone)]
pub struct PartitionMap {
    /// Partition ID -> (owning node, epoch).
    assignments: HashMap<u32, (NodeId, u64)>,
    /// Total number of partitions (immutable after creation).
    num_partitions: u32,
}

impl PartitionMap {
    /// Create a new partition map with the given number of partitions.
    ///
    /// All partitions start as unassigned.
    #[must_use]
    pub fn new(num_partitions: u32) -> Self {
        Self {
            assignments: HashMap::with_capacity(num_partitions as usize),
            num_partitions,
        }
    }

    /// Create a partition map from an existing assignment.
    #[must_use]
    pub fn from_assignments(num_partitions: u32, assignments: HashMap<u32, (NodeId, u64)>) -> Self {
        Self {
            assignments,
            num_partitions,
        }
    }

    /// Get the total number of partitions.
    #[must_use]
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Get the owner and epoch for a partition.
    #[must_use]
    pub fn owner_of(&self, partition_id: u32) -> Option<(NodeId, u64)> {
        self.assignments.get(&partition_id).copied()
    }

    /// Check whether a partition is owned by the given node.
    #[must_use]
    pub fn is_local(&self, partition_id: u32, local_node: &NodeId) -> bool {
        self.assignments
            .get(&partition_id)
            .is_some_and(|(owner, _)| owner == local_node)
    }

    /// Update ownership for a partition.
    pub fn set_owner(&mut self, partition_id: u32, node: NodeId, epoch: u64) {
        self.assignments.insert(partition_id, (node, epoch));
    }

    /// Get the epoch for a partition.
    #[must_use]
    pub fn epoch_of(&self, partition_id: u32) -> Option<u64> {
        self.assignments.get(&partition_id).map(|(_, e)| *e)
    }

    /// Get all partitions owned by a node.
    #[must_use]
    pub fn partitions_for_node(&self, node: &NodeId) -> Vec<u32> {
        self.assignments
            .iter()
            .filter_map(|(pid, (owner, _))| if owner == node { Some(*pid) } else { None })
            .collect()
    }
}

/// Compute the owning partition for a key using xxhash.
///
/// Uses `xxhash-rust`'s xxh3 for fast, high-quality hashing.
#[must_use]
#[allow(clippy::cast_possible_truncation)] // modulo guarantees result fits in u32
pub fn partition_for_key(key: &[u8], num_partitions: u32) -> u32 {
    let hash = xxhash_rust::xxh3::xxh3_64(key);
    (hash % u64::from(num_partitions)) as u32
}

/// LRU cache entry with TTL support.
#[derive(Debug, Clone)]
struct CacheEntry {
    value: Bytes,
    /// Epoch at which this entry was cached.
    epoch: u64,
}

/// Bounded LRU cache for remote state lookups.
///
/// Entries are keyed by the raw state key. Invalidation is epoch-based:
/// if the partition's epoch has advanced since the entry was cached,
/// the entry is considered stale and evicted on access.
#[derive(Debug)]
struct RemoteCache {
    /// Key -> (value, insertion order index).
    entries: HashMap<Vec<u8>, CacheEntry>,
    /// LRU order: most-recently-used at the back.
    order: Vec<Vec<u8>>,
    /// Maximum entries.
    capacity: usize,
}

impl RemoteCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            order: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Get a cached value, returning `None` if missing or stale.
    fn get(&mut self, key: &[u8], current_epoch: u64) -> Option<Bytes> {
        let entry = self.entries.get(key)?;
        if entry.epoch != current_epoch {
            // Stale entry — remove it.
            self.entries.remove(key);
            self.order.retain(|k| k.as_slice() != key);
            return None;
        }
        let value = entry.value.clone();
        // Move to back (most recent).
        self.order.retain(|k| k.as_slice() != key);
        self.order.push(key.to_vec());
        Some(value)
    }

    /// Insert a value into the cache, evicting the LRU entry if full.
    fn insert(&mut self, key: Vec<u8>, value: Bytes, epoch: u64) {
        if self.entries.contains_key(&key) {
            self.order.retain(|k| k != &key);
        } else if self.entries.len() >= self.capacity {
            // Evict LRU (front of order).
            if let Some(evicted) = self.order.first().cloned() {
                self.entries.remove(&evicted);
                self.order.remove(0);
            }
        }
        self.entries
            .insert(key.clone(), CacheEntry { value, epoch });
        self.order.push(key);
    }

    /// Flush all entries for partitions whose epoch has changed.
    fn flush_stale(&mut self, partition_map: &PartitionMap, num_partitions: u32) {
        self.entries.retain(|key, entry| {
            let pid = partition_for_key(key, num_partitions);
            partition_map
                .epoch_of(pid)
                .is_some_and(|e| e == entry.epoch)
        });
        let entries = &self.entries;
        self.order.retain(|k| entries.contains_key(k.as_slice()));
    }

    /// Number of cached entries.
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// A [`StateStore`] proxy that routes reads to local or remote state
/// based on partition ownership.
///
/// For locally-owned partitions, operations delegate directly to the
/// inner `local` store. For remote partitions, reads check the LRU
/// cache and fall back to a tracing warning (the actual gRPC transport
/// will be wired once the RPC module is implemented).
///
/// Writes to remote partitions are rejected — state should only be
/// mutated by the owning node.
pub struct RemoteStateProxy {
    /// Local state store for owned partitions.
    local: Box<dyn StateStore>,
    /// Partition -> node routing.
    partition_map: Arc<RwLock<PartitionMap>>,
    /// Local node ID.
    local_node: NodeId,
    /// Cache for hot remote data (LRU with epoch-based invalidation).
    cache: Mutex<RemoteCache>,
}

impl RemoteStateProxy {
    /// Create a new remote state proxy.
    ///
    /// - `local`: The local state store for partitions this node owns.
    /// - `partition_map`: Shared routing table updated by Raft.
    /// - `local_node`: This node's ID.
    /// - `cache_capacity`: Maximum entries in the remote cache.
    #[must_use]
    pub fn new(
        local: Box<dyn StateStore>,
        partition_map: Arc<RwLock<PartitionMap>>,
        local_node: NodeId,
        cache_capacity: usize,
    ) -> Self {
        Self {
            local,
            partition_map,
            local_node,
            cache: Mutex::new(RemoteCache::new(cache_capacity)),
        }
    }

    /// Create with default cache capacity.
    #[must_use]
    pub fn with_defaults(
        local: Box<dyn StateStore>,
        partition_map: Arc<RwLock<PartitionMap>>,
        local_node: NodeId,
    ) -> Self {
        Self::new(local, partition_map, local_node, DEFAULT_CACHE_CAPACITY)
    }

    /// Check if a key belongs to a locally-owned partition.
    fn is_local_key(&self, key: &[u8]) -> bool {
        let map = self.partition_map.read().unwrap();
        let pid = partition_for_key(key, map.num_partitions());
        map.is_local(pid, &self.local_node)
    }

    /// Get the current epoch for the partition owning a key.
    fn epoch_for_key(&self, key: &[u8]) -> u64 {
        let map = self.partition_map.read().unwrap();
        let pid = partition_for_key(key, map.num_partitions());
        map.epoch_of(pid).unwrap_or(0)
    }

    /// Flush stale cache entries after a partition map update.
    ///
    /// # Panics
    ///
    /// Panics if the partition map or cache lock is poisoned.
    pub fn flush_stale_cache(&self) {
        let map = self.partition_map.read().unwrap();
        let num = map.num_partitions();
        self.cache.lock().unwrap().flush_stale(&map, num);
    }

    /// Number of entries currently in the remote cache.
    ///
    /// # Panics
    ///
    /// Panics if the cache lock is poisoned.
    #[must_use]
    pub fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Insert a value into the remote cache (for use by the gRPC
    /// response handler once wired).
    ///
    /// # Panics
    ///
    /// Panics if the partition map or cache lock is poisoned.
    pub fn cache_insert(&self, key: &[u8], value: Bytes) {
        let epoch = self.epoch_for_key(key);
        self.cache
            .lock()
            .unwrap()
            .insert(key.to_vec(), value, epoch);
    }
}

impl StateStore for RemoteStateProxy {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        if self.is_local_key(key) {
            return self.local.get(key);
        }

        // Remote partition — check cache.
        let epoch = self.epoch_for_key(key);
        if let Some(cached) = self.cache.lock().unwrap().get(key, epoch) {
            return Some(cached);
        }

        // Remote lookup placeholder — will be wired to gRPC RemoteState.Lookup.
        tracing::debug!(
            key_len = key.len(),
            "remote state get: partition not local, cache miss (gRPC not yet wired)"
        );
        None
    }

    fn put(&mut self, key: &[u8], value: Bytes) -> Result<(), StateError> {
        if self.is_local_key(key) {
            return self.local.put(key, value);
        }

        // Writes to remote partitions are not allowed — the owning node
        // must perform the mutation.
        tracing::warn!(
            key_len = key.len(),
            "remote state put: cannot write to remote partition"
        );
        Err(StateError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cannot write to remote partition",
        )))
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), StateError> {
        if self.is_local_key(key) {
            return self.local.delete(key);
        }

        tracing::warn!(
            key_len = key.len(),
            "remote state delete: cannot delete from remote partition"
        );
        Err(StateError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cannot delete from remote partition",
        )))
    }

    fn prefix_scan<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        // Prefix scans only operate on local state. Remote prefix scans
        // would require streaming from multiple nodes — not supported yet.
        self.local.prefix_scan(prefix)
    }

    fn range_scan<'a>(
        &'a self,
        range: Range<&'a [u8]>,
    ) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + 'a> {
        self.local.range_scan(range)
    }

    fn contains(&self, key: &[u8]) -> bool {
        if self.is_local_key(key) {
            return self.local.contains(key);
        }
        let epoch = self.epoch_for_key(key);
        self.cache.lock().unwrap().get(key, epoch).is_some()
    }

    fn size_bytes(&self) -> usize {
        self.local.size_bytes()
    }

    fn len(&self) -> usize {
        self.local.len()
    }

    fn snapshot(&self) -> StateSnapshot {
        self.local.snapshot()
    }

    fn restore(&mut self, snapshot: StateSnapshot) {
        self.local.restore(snapshot);
    }

    fn clear(&mut self) {
        self.local.clear();
        self.cache.lock().unwrap().entries.clear();
        self.cache.lock().unwrap().order.clear();
    }

    fn flush(&mut self) -> Result<(), StateError> {
        self.local.flush()
    }
}

/// A lookup source that fetches enrichment data from a remote node.
///
/// Used when a lookup join's reference table is partitioned and the
/// needed partition lives on another node. Caches a snapshot of the
/// remote data, refreshing on epoch changes.
///
/// The actual gRPC fetch is a placeholder — it will be wired once the
/// RPC module is implemented.
pub struct RemoteLookupSource {
    /// Node that owns this lookup table's partition.
    owner_node: NodeId,
    /// gRPC address of the owning node.
    owner_address: String,
    /// Schema of the lookup table.
    schema: SchemaRef,
    /// Cached snapshot (refreshed on epoch change).
    cached_data: Arc<RwLock<Option<RecordBatch>>>,
    /// Last seen epoch for the owning partition.
    last_epoch: AtomicU64,
}

impl RemoteLookupSource {
    /// Create a new remote lookup source.
    #[must_use]
    pub fn new(owner_node: NodeId, owner_address: String, schema: SchemaRef) -> Self {
        Self {
            owner_node,
            owner_address,
            schema,
            cached_data: Arc::new(RwLock::new(None)),
            last_epoch: AtomicU64::new(0),
        }
    }

    /// Get the owning node ID.
    #[must_use]
    pub fn owner_node(&self) -> NodeId {
        self.owner_node
    }

    /// Get the owner's gRPC address.
    #[must_use]
    pub fn owner_address(&self) -> &str {
        &self.owner_address
    }

    /// Update the cached data (called when gRPC response arrives).
    ///
    /// # Panics
    ///
    /// Panics if the cached data lock is poisoned.
    pub fn update_cache(&self, data: RecordBatch, epoch: u64) {
        *self.cached_data.write().unwrap() = Some(data);
        self.last_epoch.store(epoch, Ordering::Release);
    }

    /// Check if the cache needs a refresh for the given epoch.
    #[must_use]
    pub fn needs_refresh(&self, current_epoch: u64) -> bool {
        self.last_epoch.load(Ordering::Acquire) != current_epoch
    }

    /// Clear the cached snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the cached data lock is poisoned.
    pub fn invalidate(&self) {
        *self.cached_data.write().unwrap() = None;
    }

    /// Get the last cached epoch.
    #[must_use]
    pub fn last_epoch(&self) -> u64 {
        self.last_epoch.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl LookupSourceDyn for RemoteLookupSource {
    async fn query_batch(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        _projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        let cached = self.cached_data.read().unwrap().clone();
        if let Some(batch) = cached {
            // Return the cached batch for every key (the caller applies
            // key matching). This is a simplification — a real implementation
            // would index into the batch by key.
            Ok(keys.iter().map(|_| Some(batch.clone())).collect())
        } else {
            tracing::debug!(
                owner = %self.owner_node,
                address = %self.owner_address,
                num_keys = keys.len(),
                "remote lookup: no cached data (gRPC fetch not yet wired)"
            );
            Ok(keys.iter().map(|_| None).collect())
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemoryStore;

    fn make_partition_map(num_partitions: u32, local_node: NodeId) -> PartitionMap {
        let mut map = PartitionMap::new(num_partitions);
        for pid in 0..num_partitions {
            // Assign even partitions to local, odd to remote.
            let owner = if pid % 2 == 0 { local_node } else { NodeId(99) };
            map.set_owner(pid, owner, 1);
        }
        map
    }

    #[test]
    fn test_partition_for_key_deterministic() {
        let p1 = partition_for_key(b"key1", 16);
        let p2 = partition_for_key(b"key1", 16);
        assert_eq!(p1, p2);
        assert!(p1 < 16);
    }

    #[test]
    fn test_partition_for_key_distribution() {
        let num = 16u32;
        let mut counts = vec![0u32; num as usize];
        for i in 0..1000u32 {
            let key = format!("key-{i}");
            let pid = partition_for_key(key.as_bytes(), num);
            counts[pid as usize] += 1;
        }
        // Every partition should get at least some keys.
        for (i, count) in counts.iter().enumerate() {
            assert!(*count > 0, "partition {i} got 0 keys");
        }
    }

    #[test]
    fn test_partition_map_basics() {
        let local = NodeId(1);
        let remote = NodeId(2);
        let mut map = PartitionMap::new(4);
        map.set_owner(0, local, 1);
        map.set_owner(1, remote, 1);
        map.set_owner(2, local, 1);
        map.set_owner(3, remote, 1);

        assert!(map.is_local(0, &local));
        assert!(!map.is_local(1, &local));
        assert_eq!(map.owner_of(0), Some((local, 1)));
        assert_eq!(map.epoch_of(1), Some(1));
        assert_eq!(map.partitions_for_node(&local), vec![0, 2]);
    }

    #[test]
    fn test_proxy_local_get_put() {
        let local_node = NodeId(1);
        let map = make_partition_map(4, local_node);
        let partition_map = Arc::new(RwLock::new(map));
        let mut proxy = RemoteStateProxy::new(
            Box::new(InMemoryStore::new()),
            Arc::clone(&partition_map),
            local_node,
            100,
        );

        // Find a key that maps to an even partition (local).
        let local_key = find_key_for_partition(0, 4);
        proxy.put(&local_key, Bytes::from_static(b"hello")).unwrap();
        assert_eq!(proxy.get(&local_key).unwrap(), Bytes::from("hello"));
        assert!(proxy.contains(&local_key));
    }

    #[test]
    fn test_proxy_remote_get_returns_none() {
        let local_node = NodeId(1);
        let map = make_partition_map(4, local_node);
        let partition_map = Arc::new(RwLock::new(map));
        let proxy = RemoteStateProxy::new(
            Box::new(InMemoryStore::new()),
            Arc::clone(&partition_map),
            local_node,
            100,
        );

        // Find a key that maps to an odd partition (remote).
        let remote_key = find_key_for_partition(1, 4);
        assert!(proxy.get(&remote_key).is_none());
    }

    #[test]
    fn test_proxy_remote_put_rejected() {
        let local_node = NodeId(1);
        let map = make_partition_map(4, local_node);
        let partition_map = Arc::new(RwLock::new(map));
        let mut proxy = RemoteStateProxy::new(
            Box::new(InMemoryStore::new()),
            Arc::clone(&partition_map),
            local_node,
            100,
        );

        let remote_key = find_key_for_partition(1, 4);
        let result = proxy.put(&remote_key, Bytes::from_static(b"nope"));
        assert!(result.is_err());
    }

    #[test]
    fn test_proxy_cache_hit() {
        let local_node = NodeId(1);
        let map = make_partition_map(4, local_node);
        let partition_map = Arc::new(RwLock::new(map));
        let proxy = RemoteStateProxy::new(
            Box::new(InMemoryStore::new()),
            Arc::clone(&partition_map),
            local_node,
            100,
        );

        let remote_key = find_key_for_partition(1, 4);
        // Pre-populate cache.
        proxy.cache_insert(&remote_key, Bytes::from_static(b"cached-value"));
        assert_eq!(proxy.get(&remote_key).unwrap(), Bytes::from("cached-value"));
        assert_eq!(proxy.cache_len(), 1);
    }

    #[test]
    fn test_proxy_cache_epoch_invalidation() {
        let local_node = NodeId(1);
        let mut map = make_partition_map(4, local_node);
        let partition_map = Arc::new(RwLock::new(map.clone()));
        let proxy = RemoteStateProxy::new(
            Box::new(InMemoryStore::new()),
            Arc::clone(&partition_map),
            local_node,
            100,
        );

        let remote_key = find_key_for_partition(1, 4);
        proxy.cache_insert(&remote_key, Bytes::from_static(b"old-value"));
        assert!(proxy.get(&remote_key).is_some());

        // Advance epoch — simulates ownership change.
        map.set_owner(1, NodeId(99), 2);
        *partition_map.write().unwrap() = map;

        // Cache entry is now stale.
        assert!(proxy.get(&remote_key).is_none());
    }

    #[test]
    fn test_cache_lru_eviction() {
        let mut cache = RemoteCache::new(3);
        cache.insert(b"k1".to_vec(), Bytes::from_static(b"v1"), 1);
        cache.insert(b"k2".to_vec(), Bytes::from_static(b"v2"), 1);
        cache.insert(b"k3".to_vec(), Bytes::from_static(b"v3"), 1);
        assert_eq!(cache.len(), 3);

        // Adding a 4th should evict k1 (LRU).
        cache.insert(b"k4".to_vec(), Bytes::from_static(b"v4"), 1);
        assert_eq!(cache.len(), 3);
        assert!(cache.get(b"k1", 1).is_none());
        assert!(cache.get(b"k2", 1).is_some());
    }

    #[test]
    fn test_cache_access_refreshes_lru() {
        let mut cache = RemoteCache::new(3);
        cache.insert(b"k1".to_vec(), Bytes::from_static(b"v1"), 1);
        cache.insert(b"k2".to_vec(), Bytes::from_static(b"v2"), 1);
        cache.insert(b"k3".to_vec(), Bytes::from_static(b"v3"), 1);

        // Access k1 to move it to most-recent.
        cache.get(b"k1", 1);

        // Insert k4 — should evict k2 (now LRU), not k1.
        cache.insert(b"k4".to_vec(), Bytes::from_static(b"v4"), 1);
        assert!(cache.get(b"k1", 1).is_some());
        assert!(cache.get(b"k2", 1).is_none());
    }

    #[test]
    fn test_remote_lookup_source_no_cache() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let source = RemoteLookupSource::new(NodeId(2), "127.0.0.1:9000".into(), schema.clone());

        assert_eq!(source.owner_node(), NodeId(2));
        assert_eq!(source.owner_address(), "127.0.0.1:9000");
        assert!(source.needs_refresh(1));
        assert_eq!(LookupSourceDyn::schema(&source).fields().len(), 1);
    }

    #[test]
    fn test_remote_lookup_source_cache_update() {
        use arrow_array::Int64Array;

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let source = RemoteLookupSource::new(NodeId(2), "127.0.0.1:9000".into(), schema.clone());

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        source.update_cache(batch.clone(), 5);

        assert!(!source.needs_refresh(5));
        assert!(source.needs_refresh(6));
        assert_eq!(source.last_epoch(), 5);

        // Invalidate.
        source.invalidate();
        assert!(source.cached_data.read().unwrap().is_none());
    }

    #[test]
    fn test_proxy_snapshot_restore() {
        let local_node = NodeId(1);
        let map = make_partition_map(4, local_node);
        let partition_map = Arc::new(RwLock::new(map));
        let mut proxy = RemoteStateProxy::new(
            Box::new(InMemoryStore::new()),
            Arc::clone(&partition_map),
            local_node,
            100,
        );

        let local_key = find_key_for_partition(0, 4);
        proxy.put(&local_key, Bytes::from_static(b"snap")).unwrap();

        let snapshot = proxy.snapshot();
        proxy.clear();
        assert!(proxy.get(&local_key).is_none());

        proxy.restore(snapshot);
        assert_eq!(proxy.get(&local_key).unwrap(), Bytes::from("snap"));
    }

    #[test]
    fn test_flush_stale_cache() {
        let local_node = NodeId(1);
        let map = make_partition_map(4, local_node);
        let partition_map = Arc::new(RwLock::new(map));
        let proxy = RemoteStateProxy::new(
            Box::new(InMemoryStore::new()),
            Arc::clone(&partition_map),
            local_node,
            100,
        );

        let remote_key = find_key_for_partition(1, 4);
        proxy.cache_insert(&remote_key, Bytes::from_static(b"val"));
        assert_eq!(proxy.cache_len(), 1);

        // Advance epoch for partition 1.
        {
            let mut map = partition_map.write().unwrap();
            map.set_owner(1, NodeId(99), 2);
        }

        proxy.flush_stale_cache();
        assert_eq!(proxy.cache_len(), 0);
    }

    /// Helper: find a key that hashes to the given partition.
    fn find_key_for_partition(target_partition: u32, num_partitions: u32) -> Vec<u8> {
        for i in 0u64.. {
            let key = format!("test-key-{i}");
            if partition_for_key(key.as_bytes(), num_partitions) == target_partition {
                return key.into_bytes();
            }
        }
        unreachable!()
    }
}
