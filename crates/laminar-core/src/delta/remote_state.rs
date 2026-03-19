//! Remote state proxy for distributed lookups.
//!
//! Provides async state access across nodes in a delta. When a key
//! lives on a remote partition, the proxy resolves the owning node
//! and fetches the value via a pluggable transport (gRPC in production).
//!
//! An LRU cache reduces repeated remote lookups for hot keys.

#![allow(clippy::disallowed_types)] // cold path: remote state configuration
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::discovery::NodeId;

/// A resolved state value from a remote node.
#[derive(Debug, Clone)]
pub struct RemoteValue {
    /// The raw value bytes (Arrow IPC encoded).
    pub data: Vec<u8>,
    /// The partition the value belongs to.
    pub partition_id: u32,
    /// The node that served the value.
    pub source_node: NodeId,
}

/// Error from remote state operations.
#[derive(Debug, thiserror::Error)]
pub enum RemoteStateError {
    /// The key was not found on the remote node.
    #[error("key not found on {node}")]
    NotFound {
        /// The node that was queried.
        node: NodeId,
    },

    /// The remote node is unreachable.
    #[error("node {node} unreachable: {reason}")]
    Unreachable {
        /// The node that was queried.
        node: NodeId,
        /// Reason for failure.
        reason: String,
    },

    /// The partition is not assigned to any node.
    #[error("partition {0} unassigned")]
    Unassigned(u32),

    /// Transport-level error.
    #[error("transport error: {0}")]
    Transport(String),

    /// Partition ownership changed during the remote lookup.
    #[error("ownership changed for partition {0} during remote lookup")]
    OwnershipChanged(u32),
}

/// Trait for the transport layer that performs remote state lookups.
///
/// Implementations provide the actual network call (gRPC, TCP, etc.).
#[allow(async_fn_in_trait)]
pub trait RemoteStateTransport: Send + Sync + 'static {
    /// Look up a key on a remote node.
    ///
    /// Returns `Ok(Some(value))` if found, `Ok(None)` if not found,
    /// or `Err` on transport failure.
    async fn lookup(
        &self,
        target_node: NodeId,
        target_address: &str,
        partition_id: u32,
        key: &[u8],
    ) -> Result<Option<RemoteValue>, RemoteStateError>;
}

/// LRU cache entry.
#[derive(Debug, Clone)]
struct CacheEntry {
    value: RemoteValue,
    /// Insertion order counter for LRU eviction.
    access_counter: u64,
}

/// Remote state proxy with LRU caching.
///
/// Routes lookups to the correct node based on partition assignment,
/// caching results to reduce network round-trips.
#[derive(Debug)]
pub struct RemoteStateProxy<T: RemoteStateTransport> {
    transport: Arc<T>,
    local_node: NodeId,
    /// Node addresses for RPC (node ID → address).
    node_addresses: Arc<parking_lot::RwLock<HashMap<NodeId, String>>>,
    /// Partition-to-node assignment (shared with `PartitionRouter`).
    assignments: Arc<parking_lot::RwLock<HashMap<u32, NodeId>>>,
    /// LRU cache: key bytes → cached value.
    cache: Arc<Mutex<LruCache>>,
}

/// Simple bounded LRU cache keyed by `(partition_id, key_bytes)`.
#[derive(Debug)]
struct LruCache {
    entries: HashMap<(u32, Vec<u8>), CacheEntry>,
    capacity: usize,
    counter: u64,
}

impl LruCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            capacity,
            counter: 0,
        }
    }

    fn get(&mut self, partition_id: u32, key: &[u8]) -> Option<&RemoteValue> {
        self.counter += 1;
        let counter = self.counter;
        let cache_key = (partition_id, key.to_vec());
        if let Some(entry) = self.entries.get_mut(&cache_key) {
            entry.access_counter = counter;
            Some(&entry.value)
        } else {
            None
        }
    }

    fn insert(&mut self, partition_id: u32, key: Vec<u8>, value: RemoteValue) {
        self.counter += 1;
        let cache_key = (partition_id, key);
        // Update in-place if key already exists (no eviction needed).
        if let Some(entry) = self.entries.get_mut(&cache_key) {
            entry.value = value;
            entry.access_counter = self.counter;
            return;
        }
        // Only evict when at capacity and inserting a new key.
        if self.capacity > 0 && self.entries.len() >= self.capacity {
            self.evict_lru();
        }
        self.entries.insert(
            cache_key,
            CacheEntry {
                value,
                access_counter: self.counter,
            },
        );
    }

    fn evict_lru(&mut self) {
        if let Some(lru_key) = self
            .entries
            .iter()
            .min_by_key(|(_, v)| v.access_counter)
            .map(|(k, _)| k.clone())
        {
            self.entries.remove(&lru_key);
        }
    }

    fn invalidate_partition(&mut self, partition_id: u32) {
        self.entries.retain(|(pid, _), _| *pid != partition_id);
    }
}

impl<T: RemoteStateTransport> RemoteStateProxy<T> {
    /// Create a new remote state proxy.
    #[must_use]
    pub fn new(transport: Arc<T>, local_node: NodeId, cache_capacity: usize) -> Self {
        Self {
            transport,
            local_node,
            node_addresses: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            assignments: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            cache: Arc::new(Mutex::new(LruCache::new(cache_capacity))),
        }
    }

    /// Update partition assignments, invalidating cache for changed partitions.
    ///
    /// Holds the write lock through both the assignment swap and cache
    /// invalidation so that concurrent `get()` calls cannot read new
    /// assignments while stale cache entries still exist.
    pub fn update_assignments(&self, new_assignments: HashMap<u32, NodeId>) {
        let mut guard = self.assignments.write();
        let old = guard.clone();
        // Find partitions whose owner changed or were removed.
        let mut changed = Vec::new();
        for (pid, old_owner) in &old {
            match new_assignments.get(pid) {
                Some(new_owner) if *new_owner != *old_owner => changed.push(*pid),
                None => changed.push(*pid),
                _ => {}
            }
        }

        if !changed.is_empty() {
            let mut cache = self.cache.lock();
            for pid in changed {
                cache.invalidate_partition(pid);
            }
        }

        *guard = new_assignments;
    }

    /// Update known node addresses.
    pub fn update_node_addresses(&self, addresses: HashMap<NodeId, String>) {
        *self.node_addresses.write() = addresses;
    }

    /// Invalidate cache entries for a partition (e.g., after migration).
    pub fn invalidate_partition(&self, partition_id: u32) {
        self.cache.lock().invalidate_partition(partition_id);
    }

    /// Look up a key, routing to the correct node.
    ///
    /// Returns `None` if the key is for a local partition (caller should
    /// use the local state store). Returns `Some(Ok(value))` for remote
    /// hits, or `Some(Err(...))` for remote failures.
    ///
    /// # Errors
    ///
    /// Returns an error if the remote lookup fails.
    pub async fn get(
        &self,
        partition_id: u32,
        key: &[u8],
    ) -> Result<Option<RemoteValue>, RemoteStateError> {
        // Check if partition is local.
        let owner = {
            let assignments = self.assignments.read();
            assignments.get(&partition_id).copied()
        };

        let owner = match owner {
            Some(node) if node == self.local_node => return Ok(None), // local
            Some(node) => node,
            None => return Err(RemoteStateError::Unassigned(partition_id)),
        };

        // Check LRU cache.
        {
            let mut cache = self.cache.lock();
            if let Some(cached) = cache.get(partition_id, key) {
                return Ok(Some(cached.clone()));
            }
        }

        // Look up target address.
        let address = {
            let addresses = self.node_addresses.read();
            addresses.get(&owner).cloned()
        };

        let address = address.ok_or(RemoteStateError::Unreachable {
            node: owner,
            reason: "no known address".into(),
        })?;

        // Perform remote lookup via transport.
        let result = self
            .transport
            .lookup(owner, &address, partition_id, key)
            .await;

        // Hold the assignments read-guard through cache insertion so the
        // assignment cannot change between the ownership check and cache write.
        let assignments_guard = self.assignments.read();
        if assignments_guard.get(&partition_id).copied() != Some(owner) {
            return Err(RemoteStateError::OwnershipChanged(partition_id));
        }

        let result = result?;

        // Validate the response partition matches what we requested.
        if let Some(ref value) = result {
            if value.partition_id != partition_id {
                return Ok(None);
            }
            let mut cache = self.cache.lock();
            cache.insert(partition_id, key.to_vec(), value.clone());
        }

        Ok(result)
    }
}

// Manual Clone for RemoteStateProxy (T doesn't need Clone since it's behind Arc).
impl<T: RemoteStateTransport> Clone for RemoteStateProxy<T> {
    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            local_node: self.local_node,
            node_addresses: Arc::clone(&self.node_addresses),
            assignments: Arc::clone(&self.assignments),
            cache: Arc::clone(&self.cache),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Test transport that records calls and returns configurable results.
    struct MockTransport {
        call_count: AtomicU32,
        /// If set, return this value for any lookup.
        value: Option<RemoteValue>,
    }

    impl MockTransport {
        fn new(value: Option<RemoteValue>) -> Self {
            Self {
                call_count: AtomicU32::new(0),
                value,
            }
        }

        fn calls(&self) -> u32 {
            self.call_count.load(Ordering::Relaxed)
        }
    }

    impl RemoteStateTransport for MockTransport {
        async fn lookup(
            &self,
            _target_node: NodeId,
            _target_address: &str,
            _partition_id: u32,
            _key: &[u8],
        ) -> Result<Option<RemoteValue>, RemoteStateError> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            Ok(self.value.clone())
        }
    }

    #[tokio::test]
    async fn test_local_partition_returns_none() {
        let transport = Arc::new(MockTransport::new(None));
        let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 100);

        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(1)); // local
        proxy.update_assignments(assignments);

        let result = proxy.get(0, b"key").await.unwrap();
        assert!(result.is_none());
        assert_eq!(transport.calls(), 0); // no RPC made
    }

    #[tokio::test]
    async fn test_remote_partition_calls_transport() {
        let value = RemoteValue {
            data: vec![42],
            partition_id: 0,
            source_node: NodeId(2),
        };
        let transport = Arc::new(MockTransport::new(Some(value)));
        let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 100);

        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(2)); // remote
        proxy.update_assignments(assignments);

        let mut addresses = HashMap::new();
        addresses.insert(NodeId(2), "127.0.0.1:9000".into());
        proxy.update_node_addresses(addresses);

        let result = proxy.get(0, b"key").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![42]);
        assert_eq!(transport.calls(), 1);
    }

    #[tokio::test]
    async fn test_cache_hit_avoids_rpc() {
        let value = RemoteValue {
            data: vec![42],
            partition_id: 0,
            source_node: NodeId(2),
        };
        let transport = Arc::new(MockTransport::new(Some(value)));
        let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 100);

        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(2));
        proxy.update_assignments(assignments);

        let mut addresses = HashMap::new();
        addresses.insert(NodeId(2), "127.0.0.1:9000".into());
        proxy.update_node_addresses(addresses);

        // First call — transport hit
        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 1);

        // Second call — cache hit, no RPC
        let result = proxy.get(0, b"key").await.unwrap();
        assert!(result.is_some());
        assert_eq!(transport.calls(), 1); // still 1
    }

    #[tokio::test]
    async fn test_unassigned_partition_error() {
        let transport = Arc::new(MockTransport::new(None));
        let proxy = RemoteStateProxy::new(transport, NodeId(1), 100);
        // No assignments set
        let result = proxy.get(0, b"key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let value = RemoteValue {
            data: vec![42],
            partition_id: 0,
            source_node: NodeId(2),
        };
        let transport = Arc::new(MockTransport::new(Some(value)));
        let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 100);

        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(2));
        proxy.update_assignments(assignments);

        let mut addresses = HashMap::new();
        addresses.insert(NodeId(2), "127.0.0.1:9000".into());
        proxy.update_node_addresses(addresses);

        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 1);

        // Invalidate and re-fetch
        proxy.invalidate_partition(0);
        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 2); // had to go remote again
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let value = RemoteValue {
            data: vec![1],
            partition_id: 0,
            source_node: NodeId(2),
        };
        let transport = Arc::new(MockTransport::new(Some(value)));
        let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 2); // capacity=2

        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(2));
        proxy.update_assignments(assignments);

        let mut addresses = HashMap::new();
        addresses.insert(NodeId(2), "127.0.0.1:9000".into());
        proxy.update_node_addresses(addresses);

        // Fill cache with 2 entries
        let _ = proxy.get(0, b"key1").await.unwrap();
        let _ = proxy.get(0, b"key2").await.unwrap();
        assert_eq!(transport.calls(), 2);

        // Adding a 3rd should evict the LRU (key1)
        let _ = proxy.get(0, b"key3").await.unwrap();
        assert_eq!(transport.calls(), 3);

        // key2 should still be cached, key1 should be evicted
        let _ = proxy.get(0, b"key2").await.unwrap();
        assert_eq!(transport.calls(), 3); // cached

        let _ = proxy.get(0, b"key1").await.unwrap();
        assert_eq!(transport.calls(), 4); // had to fetch again
    }

    #[tokio::test]
    async fn test_assignment_change_invalidates_cache() {
        let value = RemoteValue {
            data: vec![42],
            partition_id: 0,
            source_node: NodeId(2),
        };
        let transport = Arc::new(MockTransport::new(Some(value)));
        let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 100);

        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(2));
        assignments.insert(1, NodeId(3));
        proxy.update_assignments(assignments);

        let mut addresses = HashMap::new();
        addresses.insert(NodeId(2), "127.0.0.1:9000".into());
        addresses.insert(NodeId(3), "127.0.0.1:9001".into());
        proxy.update_node_addresses(addresses);

        // Populate cache for partition 0
        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 1);

        // Cache hit
        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 1);

        // Change owner of partition 0 from NodeId(2) to NodeId(3).
        // Partition 1 stays the same.
        let mut new_assignments = HashMap::new();
        new_assignments.insert(0, NodeId(3));
        new_assignments.insert(1, NodeId(3));
        proxy.update_assignments(new_assignments);

        // Partition 0 cache should be invalidated, requiring a new RPC.
        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 2);
    }

    #[tokio::test]
    async fn test_cache_key_includes_partition_id() {
        let value = RemoteValue {
            data: vec![42],
            partition_id: 0,
            source_node: NodeId(2),
        };
        let transport = Arc::new(MockTransport::new(Some(value)));
        let proxy = RemoteStateProxy::new(transport.clone(), NodeId(1), 100);

        let mut assignments = HashMap::new();
        assignments.insert(0, NodeId(2));
        assignments.insert(1, NodeId(2));
        proxy.update_assignments(assignments);

        let mut addresses = HashMap::new();
        addresses.insert(NodeId(2), "127.0.0.1:9000".into());
        proxy.update_node_addresses(addresses);

        // Same key bytes, different partitions — should be separate cache entries.
        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 1);

        let _ = proxy.get(1, b"key").await.unwrap();
        assert_eq!(transport.calls(), 2); // not a cache hit — different partition

        // Re-fetch both — should be cached now.
        let _ = proxy.get(0, b"key").await.unwrap();
        assert_eq!(transport.calls(), 2);

        let _ = proxy.get(1, b"key").await.unwrap();
        assert_eq!(transport.calls(), 2);
    }

    #[test]
    fn test_proxy_clone() {
        let transport = Arc::new(MockTransport::new(None));
        let proxy = RemoteStateProxy::new(transport, NodeId(1), 100);
        let _cloned = proxy.clone();
    }
}
