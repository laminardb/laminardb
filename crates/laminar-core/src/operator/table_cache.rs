//! LRU cache and Xor filter for reference table partial-cache mode.
//!
//! `TableLruCache` provides O(1) slab-based LRU eviction for hot keys.
//! `TableXorFilter` wraps `xorf::Xor8` for negative-lookup short-circuiting.

use arrow::array::RecordBatch;
use fxhash::FxHashMap;

// ── LRU Cache ───────────────────────────────────────────────────────────

/// A slab node in the intrusive doubly-linked LRU list.
struct LruNode {
    key: String,
    value: RecordBatch,
    prev: usize,
    next: usize,
}

/// Sentinel value for null pointers in the linked list.
const SENTINEL: usize = usize::MAX;

/// O(1) slab-based LRU cache for reference table rows.
///
/// Uses an `FxHashMap<String, usize>` for key→slot lookup and a `Vec<LruNode>`
/// slab with intrusive doubly-linked list pointers for LRU ordering.
pub struct TableLruCache {
    /// Key → slab index lookup.
    index: FxHashMap<String, usize>,
    /// Slab of LRU nodes.
    slab: Vec<LruNode>,
    /// Free list of recycled slab indices.
    free: Vec<usize>,
    /// Head of the LRU list (most recently used).
    head: usize,
    /// Tail of the LRU list (least recently used).
    tail: usize,
    /// Maximum number of entries.
    max_entries: usize,
    /// Total number of `get` calls.
    gets: u64,
    /// Number of cache hits.
    hits: u64,
    /// Total number of evictions.
    evictions: u64,
}

impl TableLruCache {
    /// Create a new LRU cache with the given maximum capacity.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            index: FxHashMap::default(),
            slab: Vec::new(),
            free: Vec::new(),
            head: SENTINEL,
            tail: SENTINEL,
            max_entries,
            gets: 0,
            hits: 0,
            evictions: 0,
        }
    }

    /// Look up a key, promoting it to most-recently-used on hit.
    pub fn get(&mut self, key: &str) -> Option<&RecordBatch> {
        self.gets += 1;
        let &slot = self.index.get(key)?;
        self.hits += 1;
        self.detach(slot);
        self.push_front(slot);
        Some(&self.slab[slot].value)
    }

    /// Insert a key-value pair. Returns the evicted entry if the cache was full.
    pub fn insert(&mut self, key: String, value: RecordBatch) -> Option<(String, RecordBatch)> {
        // If key already present, update in place and promote.
        if let Some(&slot) = self.index.get(&key) {
            self.slab[slot].value = value;
            self.detach(slot);
            self.push_front(slot);
            return None;
        }

        // Evict if at capacity.
        let evicted = if self.index.len() >= self.max_entries {
            self.evict_tail()
        } else {
            None
        };

        // Allocate a slab slot.
        let slot = if let Some(free_slot) = self.free.pop() {
            self.slab[free_slot] = LruNode {
                key: key.clone(),
                value,
                prev: SENTINEL,
                next: SENTINEL,
            };
            free_slot
        } else {
            let slot = self.slab.len();
            self.slab.push(LruNode {
                key: key.clone(),
                value,
                prev: SENTINEL,
                next: SENTINEL,
            });
            slot
        };

        self.index.insert(key, slot);
        self.push_front(slot);

        evicted
    }

    /// Remove a key from the cache. Returns `true` if it was present.
    pub fn invalidate(&mut self, key: &str) -> bool {
        if let Some(slot) = self.index.remove(key) {
            self.detach(slot);
            self.free.push(slot);
            true
        } else {
            false
        }
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.index.clear();
        self.slab.clear();
        self.free.clear();
        self.head = SENTINEL;
        self.tail = SENTINEL;
    }

    /// Number of entries currently in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Maximum capacity.
    #[must_use]
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Cache hit rate as a fraction in `[0.0, 1.0]`. Returns 0.0 if no gets.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn hit_rate(&self) -> f64 {
        if self.gets == 0 {
            0.0
        } else {
            self.hits as f64 / self.gets as f64
        }
    }

    /// Total number of `get` calls.
    #[must_use]
    pub fn total_gets(&self) -> u64 {
        self.gets
    }

    /// Total cache hits.
    #[must_use]
    pub fn total_hits(&self) -> u64 {
        self.hits
    }

    /// Total evictions.
    #[must_use]
    pub fn total_evictions(&self) -> u64 {
        self.evictions
    }

    // ── Internal linked-list operations ──

    /// Detach a node from the doubly-linked list.
    fn detach(&mut self, slot: usize) {
        let prev = self.slab[slot].prev;
        let next = self.slab[slot].next;

        if prev == SENTINEL {
            self.head = next;
        } else {
            self.slab[prev].next = next;
        }

        if next == SENTINEL {
            self.tail = prev;
        } else {
            self.slab[next].prev = prev;
        }

        self.slab[slot].prev = SENTINEL;
        self.slab[slot].next = SENTINEL;
    }

    /// Push a node to the front (most recently used).
    fn push_front(&mut self, slot: usize) {
        self.slab[slot].prev = SENTINEL;
        self.slab[slot].next = self.head;

        if self.head != SENTINEL {
            self.slab[self.head].prev = slot;
        }
        self.head = slot;

        if self.tail == SENTINEL {
            self.tail = slot;
        }
    }

    /// Evict the tail (least recently used) entry.
    fn evict_tail(&mut self) -> Option<(String, RecordBatch)> {
        if self.tail == SENTINEL {
            return None;
        }
        let slot = self.tail;
        self.detach(slot);

        let node = &mut self.slab[slot];
        let key = std::mem::take(&mut node.key);
        let value = RecordBatch::new_empty(node.value.schema());
        let evicted_value = std::mem::replace(&mut node.value, value);

        self.index.remove(&key);
        self.free.push(slot);
        self.evictions += 1;

        Some((key, evicted_value))
    }
}

// ── Xor Filter ──────────────────────────────────────────────────────────

/// Wraps `xorf::Xor8` for negative-lookup short-circuiting.
///
/// If the filter says a key does *not* exist, it definitely doesn't (no
/// false negatives). If it says a key *does* exist, there's a ~0.4% chance
/// of a false positive — acceptable for avoiding unnecessary cache misses.
pub struct TableXorFilter {
    filter: Option<xorf::Xor8>,
    /// Mapping from string keys to u64 hashes used to build the filter.
    hash_fn: fn(&str) -> u64,
    /// Number of times `contains()` short-circuited (returned false).
    short_circuit_count: u64,
}

impl TableXorFilter {
    /// Create a new (empty) xor filter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            filter: None,
            hash_fn: hash_key,
            short_circuit_count: 0,
        }
    }

    /// Build the filter from a set of keys.
    #[must_use]
    pub fn build(keys: &[String]) -> Self {
        let mut f = Self::new();
        f.rebuild(keys);
        f
    }

    /// Rebuild the filter in place from a new set of keys.
    pub fn rebuild(&mut self, keys: &[String]) {
        if keys.is_empty() {
            self.filter = None;
            return;
        }
        let hashes: Vec<u64> = keys.iter().map(|k| (self.hash_fn)(k)).collect();
        self.filter = Some(xorf::Xor8::from(&hashes));
    }

    /// Check if a key *might* exist.
    ///
    /// Returns `true` if the filter is not built (permissive fallback).
    pub fn contains(&mut self, key: &str) -> bool {
        match &self.filter {
            Some(f) => {
                let h = (self.hash_fn)(key);
                let result = xorf::Filter::contains(f, &h);
                if !result {
                    self.short_circuit_count += 1;
                }
                result
            }
            None => true, // permissive: no filter means "might exist"
        }
    }

    /// Number of times `contains()` definitively returned `false`.
    #[must_use]
    pub fn short_circuits(&self) -> u64 {
        self.short_circuit_count
    }

    /// Whether the filter has been built.
    #[must_use]
    pub fn is_built(&self) -> bool {
        self.filter.is_some()
    }

    /// Clear the filter.
    pub fn clear(&mut self) {
        self.filter = None;
        self.short_circuit_count = 0;
    }
}

impl Default for TableXorFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Hash a string key to u64 using fxhash.
fn hash_key(key: &str) -> u64 {
    fxhash::hash64(key)
}

// ── Metrics ─────────────────────────────────────────────────────────────

/// Aggregated cache metrics for a single table.
#[derive(Debug, Clone)]
pub struct TableCacheMetrics {
    /// Total `get` calls on the LRU cache.
    pub cache_gets: u64,
    /// Total LRU cache hits.
    pub cache_hits: u64,
    /// Cache hit rate (0.0–1.0).
    pub cache_hit_rate: f64,
    /// Total LRU evictions.
    pub cache_evictions: u64,
    /// Number of entries currently in the LRU cache.
    pub cache_entries: usize,
    /// Maximum LRU cache capacity.
    pub cache_max_entries: usize,
    /// Whether the xor filter is built.
    pub xor_filter_built: bool,
    /// Number of xor filter short-circuits.
    pub xor_short_circuits: u64,
}

// ── Lookup Decision ─────────────────────────────────────────────────────

/// Decision from a cache-aware lookup.
#[derive(Debug)]
pub enum LookupDecision {
    /// Key definitely does not exist (xor filter negative).
    NotFound,
    /// Key found in cache (LRU hit or backing store).
    Found(RecordBatch),
    /// Key not in cache and may exist in backing store.
    CacheMiss,
}

// ── Helper to collect metrics from LRU + xor filter ─────────────────────

/// Collect metrics from an LRU cache and xor filter pair.
#[must_use]
pub fn collect_cache_metrics(lru: &TableLruCache, xor: &TableXorFilter) -> TableCacheMetrics {
    TableCacheMetrics {
        cache_gets: lru.total_gets(),
        cache_hits: lru.total_hits(),
        cache_hit_rate: lru.hit_rate(),
        cache_evictions: lru.total_evictions(),
        cache_entries: lru.len(),
        cache_max_entries: lru.max_entries(),
        xor_filter_built: xor.is_built(),
        xor_short_circuits: xor.short_circuits(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_row(id: i32, name: &str) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
            ],
        )
        .unwrap()
    }

    // ── LRU Cache tests ──

    #[test]
    fn test_lru_insert_and_get() {
        let mut cache = TableLruCache::new(10);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        let row = cache.get("a").unwrap();
        assert_eq!(row.num_rows(), 1);
    }

    #[test]
    fn test_lru_miss() {
        let mut cache = TableLruCache::new(10);
        assert!(cache.get("missing").is_none());
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = TableLruCache::new(2);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        cache.insert("b".to_string(), make_row(2, "Bob"));

        // This should evict "a" (tail)
        let evicted = cache.insert("c".to_string(), make_row(3, "Charlie"));
        assert!(evicted.is_some());
        let (key, _) = evicted.unwrap();
        assert_eq!(key, "a");

        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
    }

    #[test]
    fn test_lru_promotes_on_get() {
        let mut cache = TableLruCache::new(2);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        cache.insert("b".to_string(), make_row(2, "Bob"));

        // Access "a" so it becomes MRU; "b" is now LRU
        cache.get("a");

        // Insert "c" should evict "b" (the LRU), not "a"
        let evicted = cache.insert("c".to_string(), make_row(3, "Charlie"));
        assert_eq!(evicted.unwrap().0, "b");
        assert!(cache.get("a").is_some());
    }

    #[test]
    fn test_lru_update_existing_key() {
        let mut cache = TableLruCache::new(10);
        cache.insert("a".to_string(), make_row(1, "Old"));
        cache.insert("a".to_string(), make_row(1, "New"));

        assert_eq!(cache.len(), 1);
        let row = cache.get("a").unwrap();
        let names = row
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "New");
    }

    #[test]
    fn test_lru_invalidate() {
        let mut cache = TableLruCache::new(10);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        assert!(cache.invalidate("a"));
        assert!(cache.get("a").is_none());
        assert!(!cache.invalidate("a")); // already gone
    }

    #[test]
    fn test_lru_clear() {
        let mut cache = TableLruCache::new(10);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        cache.insert("b".to_string(), make_row(2, "Bob"));
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_lru_metrics() {
        let mut cache = TableLruCache::new(2);
        cache.insert("a".to_string(), make_row(1, "Alice"));

        cache.get("a"); // hit
        cache.get("b"); // miss
        cache.get("a"); // hit

        assert_eq!(cache.total_gets(), 3);
        assert_eq!(cache.total_hits(), 2);
        assert!((cache.hit_rate() - 2.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn test_lru_eviction_counter() {
        let mut cache = TableLruCache::new(1);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        cache.insert("b".to_string(), make_row(2, "Bob"));
        cache.insert("c".to_string(), make_row(3, "Charlie"));
        assert_eq!(cache.total_evictions(), 2);
    }

    #[test]
    fn test_lru_hit_rate_zero_gets() {
        let cache = TableLruCache::new(10);
        assert!((cache.hit_rate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_lru_slab_reuse() {
        let mut cache = TableLruCache::new(2);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        cache.insert("b".to_string(), make_row(2, "Bob"));
        // Evict "a"
        cache.insert("c".to_string(), make_row(3, "Charlie"));
        // Evict "b"
        cache.insert("d".to_string(), make_row(4, "Dave"));

        // Slab slots should be reused (slab.len() stays at 2)
        assert_eq!(cache.len(), 2);
        assert!(cache.get("c").is_some());
        assert!(cache.get("d").is_some());
    }

    #[test]
    fn test_lru_single_capacity() {
        let mut cache = TableLruCache::new(1);
        cache.insert("a".to_string(), make_row(1, "Alice"));
        let evicted = cache.insert("b".to_string(), make_row(2, "Bob"));
        assert_eq!(evicted.unwrap().0, "a");
        assert_eq!(cache.len(), 1);
        assert!(cache.get("b").is_some());
    }

    // ── Xor Filter tests ──

    #[test]
    fn test_xor_build_and_contains() {
        let keys = vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
        ];
        let mut filter = TableXorFilter::build(&keys);

        // These should be found (no false negatives)
        assert!(filter.contains("apple"));
        assert!(filter.contains("banana"));
        assert!(filter.contains("cherry"));
        assert!(filter.is_built());
    }

    #[test]
    fn test_xor_short_circuits_missing() {
        let keys: Vec<String> = (0..1000).map(|i| format!("key_{i}")).collect();
        let mut filter = TableXorFilter::build(&keys);

        // Test many missing keys - some should be short-circuited
        let mut short_circuited = 0;
        for i in 1000..2000 {
            if !filter.contains(&format!("key_{i}")) {
                short_circuited += 1;
            }
        }
        // With 0.4% FPR, most of the 1000 missing keys should short-circuit
        assert!(
            short_circuited > 900,
            "Expected >900 short-circuits, got {short_circuited}"
        );
        assert!(filter.short_circuits() > 900);
    }

    #[test]
    fn test_xor_empty_permissive() {
        let mut filter = TableXorFilter::new();
        // No filter built → permissive
        assert!(!filter.is_built());
        assert!(filter.contains("anything"));
        assert_eq!(filter.short_circuits(), 0);
    }

    #[test]
    fn test_xor_empty_keys_permissive() {
        let mut filter = TableXorFilter::build(&[]);
        assert!(!filter.is_built());
        assert!(filter.contains("anything"));
    }

    #[test]
    fn test_xor_rebuild() {
        let keys1 = vec!["a".to_string(), "b".to_string()];
        let mut filter = TableXorFilter::build(&keys1);
        assert!(filter.is_built());

        let keys2 = vec!["x".to_string(), "y".to_string()];
        filter.rebuild(&keys2);
        assert!(filter.is_built());

        // "x" and "y" should be found
        assert!(filter.contains("x"));
        assert!(filter.contains("y"));
    }

    #[test]
    fn test_xor_clear() {
        let keys = vec!["a".to_string(), "b".to_string()];
        let mut filter = TableXorFilter::build(&keys);
        assert!(filter.is_built());

        filter.clear();
        assert!(!filter.is_built());
        assert_eq!(filter.short_circuits(), 0);
    }

    #[test]
    fn test_xor_default() {
        let filter = TableXorFilter::default();
        assert!(!filter.is_built());
    }

    #[test]
    fn test_xor_short_circuit_counter() {
        let keys = vec!["only_key".to_string()];
        let mut filter = TableXorFilter::build(&keys);

        // The key itself should be found
        assert!(filter.contains("only_key"));
        assert_eq!(filter.short_circuits(), 0);
    }

    // ── Metrics collection test ──

    #[test]
    fn test_collect_cache_metrics() {
        let mut lru = TableLruCache::new(100);
        lru.insert("a".to_string(), make_row(1, "Alice"));
        lru.get("a");
        lru.get("missing");

        let keys = vec!["a".to_string()];
        let xor = TableXorFilter::build(&keys);

        let metrics = collect_cache_metrics(&lru, &xor);
        assert_eq!(metrics.cache_gets, 2);
        assert_eq!(metrics.cache_hits, 1);
        assert_eq!(metrics.cache_entries, 1);
        assert_eq!(metrics.cache_max_entries, 100);
        assert!(metrics.xor_filter_built);
        assert_eq!(metrics.xor_short_circuits, 0);
    }

    // ── LookupDecision test ──

    #[test]
    fn test_lookup_decision_variants() {
        let row = make_row(1, "Alice");
        let found = LookupDecision::Found(row);
        assert!(matches!(found, LookupDecision::Found(_)));
        assert!(matches!(LookupDecision::NotFound, LookupDecision::NotFound));
        assert!(matches!(
            LookupDecision::CacheMiss,
            LookupDecision::CacheMiss
        ));
    }
}
