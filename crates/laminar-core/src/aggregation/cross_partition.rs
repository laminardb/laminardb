//! Cross-partition aggregate store backed by `papaya::HashMap` (F-XAGG-001).
//!
//! In a partition-parallel system, each partition computes partial aggregates
//! independently. The [`CrossPartitionAggregateStore`] provides a lock-free
//! concurrent hash map where partitions publish their partial aggregates
//! and readers can merge them on demand.
//!
//! ## Design
//!
//! - Each partition writes its partial aggregate under `(group_key, partition_id)`
//! - Readers iterate all partitions for a given group key and merge
//! - The underlying `papaya::HashMap` is lock-free and scales with readers
//!
//! ## Thread Safety
//!
//! All operations are `Send + Sync`. Writers use `pin()` + `insert()`;
//! readers use `pin()` + `get()`. No external locking required.

use bytes::Bytes;

/// A composite key combining a group key with a partition identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompositeKey {
    /// The aggregation group key (serialized).
    group_key: Bytes,
    /// The partition that produced this partial aggregate.
    partition_id: u32,
}

/// Lock-free concurrent store for cross-partition partial aggregates.
///
/// Each partition publishes serialized partial aggregates under its
/// `partition_id`. Readers merge partials for a given group key to
/// produce the final aggregate.
///
/// ## Performance
///
/// - Write (publish partial): single `papaya::HashMap::insert` — lock-free
/// - Read (get partial): single `papaya::HashMap::get` — lock-free
/// - Merge: iterate known partitions, collect partials, caller merges
pub struct CrossPartitionAggregateStore {
    /// Lock-free concurrent map: `(group_key, partition_id) -> partial_aggregate`.
    map: papaya::HashMap<CompositeKey, Bytes>,
    /// Total number of partitions (fixed at creation).
    num_partitions: u32,
}

impl CrossPartitionAggregateStore {
    /// Create a new store for the given number of partitions.
    #[must_use]
    pub fn new(num_partitions: u32) -> Self {
        Self {
            map: papaya::HashMap::new(),
            num_partitions,
        }
    }

    /// Publish a partial aggregate from a partition.
    ///
    /// Overwrites any previous partial for this `(group_key, partition_id)`.
    pub fn publish(&self, group_key: Bytes, partition_id: u32, partial: Bytes) {
        let key = CompositeKey {
            group_key,
            partition_id,
        };
        let guard = self.map.guard();
        self.map.insert(key, partial, &guard);
    }

    /// Get the partial aggregate for a specific partition.
    #[must_use]
    pub fn get_partial(&self, group_key: &[u8], partition_id: u32) -> Option<Bytes> {
        let key = CompositeKey {
            group_key: Bytes::copy_from_slice(group_key),
            partition_id,
        };
        let guard = self.map.guard();
        self.map.get(&key, &guard).cloned()
    }

    /// Collect all partial aggregates for a group key across all partitions.
    ///
    /// Returns a vector of `(partition_id, partial_bytes)` for all
    /// partitions that have published a partial for this key.
    #[must_use]
    pub fn collect_partials(&self, group_key: &[u8]) -> Vec<(u32, Bytes)> {
        let guard = self.map.guard();
        let mut result = Vec::new();
        for partition_id in 0..self.num_partitions {
            let key = CompositeKey {
                group_key: Bytes::copy_from_slice(group_key),
                partition_id,
            };
            if let Some(partial) = self.map.get(&key, &guard) {
                result.push((partition_id, partial.clone()));
            }
        }
        result
    }

    /// Remove all partials for a group key.
    pub fn remove_group(&self, group_key: &[u8]) {
        let guard = self.map.guard();
        for partition_id in 0..self.num_partitions {
            let key = CompositeKey {
                group_key: Bytes::copy_from_slice(group_key),
                partition_id,
            };
            self.map.remove(&key, &guard);
        }
    }

    /// Total number of partitions.
    #[must_use]
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Number of entries in the map (across all partitions and groups).
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the store has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// SAFETY: papaya::HashMap is Send + Sync, Bytes is Send + Sync.
// The CompositeKey contains only Bytes (Send+Sync) and u32 (Send+Sync).
unsafe impl Send for CrossPartitionAggregateStore {}
// SAFETY: same reasoning — all fields are Sync.
unsafe impl Sync for CrossPartitionAggregateStore {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_and_get() {
        let store = CrossPartitionAggregateStore::new(4);

        store.publish(
            Bytes::from_static(b"group1"),
            0,
            Bytes::from_static(b"partial_0"),
        );

        let result = store.get_partial(b"group1", 0);
        assert_eq!(result, Some(Bytes::from_static(b"partial_0")));

        // Missing partition
        assert!(store.get_partial(b"group1", 1).is_none());

        // Missing group
        assert!(store.get_partial(b"group2", 0).is_none());
    }

    #[test]
    fn test_overwrite_partial() {
        let store = CrossPartitionAggregateStore::new(2);

        store.publish(
            Bytes::from_static(b"key"),
            0,
            Bytes::from_static(b"v1"),
        );
        store.publish(
            Bytes::from_static(b"key"),
            0,
            Bytes::from_static(b"v2"),
        );

        assert_eq!(
            store.get_partial(b"key", 0),
            Some(Bytes::from_static(b"v2"))
        );
    }

    #[test]
    fn test_collect_partials() {
        let store = CrossPartitionAggregateStore::new(3);

        store.publish(Bytes::from_static(b"g"), 0, Bytes::from_static(b"p0"));
        store.publish(Bytes::from_static(b"g"), 2, Bytes::from_static(b"p2"));
        // partition 1 hasn't published yet

        let partials = store.collect_partials(b"g");
        assert_eq!(partials.len(), 2);

        let ids: Vec<u32> = partials.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&0));
        assert!(ids.contains(&2));
    }

    #[test]
    fn test_remove_group() {
        let store = CrossPartitionAggregateStore::new(2);

        store.publish(Bytes::from_static(b"g1"), 0, Bytes::from_static(b"a"));
        store.publish(Bytes::from_static(b"g1"), 1, Bytes::from_static(b"b"));
        store.publish(Bytes::from_static(b"g2"), 0, Bytes::from_static(b"c"));

        assert_eq!(store.len(), 3);

        store.remove_group(b"g1");

        assert!(store.get_partial(b"g1", 0).is_none());
        assert!(store.get_partial(b"g1", 1).is_none());
        // g2 still present
        assert_eq!(
            store.get_partial(b"g2", 0),
            Some(Bytes::from_static(b"c"))
        );
    }

    #[test]
    fn test_empty_store() {
        let store = CrossPartitionAggregateStore::new(4);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.num_partitions(), 4);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(CrossPartitionAggregateStore::new(4));
        let mut handles = vec![];

        // Multiple writers concurrently
        for partition in 0..4u32 {
            let store = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..100u32 {
                    let group = format!("group_{i}");
                    let value = format!("p{partition}_v{i}");
                    store.publish(
                        Bytes::from(group),
                        partition,
                        Bytes::from(value),
                    );
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All partitions should have published for group_50
        let partials = store.collect_partials(b"group_50");
        assert_eq!(partials.len(), 4);
    }
}
