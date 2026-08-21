use super::*;
use arrow_array::StringArray;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

fn test_batch(val: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![val]))]).unwrap()
}

fn small_cache(table_id: u32) -> LookupMemoryCache {
    LookupMemoryCache::new(
        table_id,
        LookupMemoryCacheConfig {
            capacity_bytes: 64 * 1024,
            ttl: None,
        },
    )
}

#[test]
fn test_lookup_cache_hit_miss() {
    let cache = small_cache(1);

    assert!(cache.get_cached(b"key1").is_not_found());

    cache.insert(b"key1", test_batch("value1"));
    let result = cache.get_cached(b"key1");
    assert!(result.is_hit());
    assert_eq!(result.into_batch().unwrap().num_rows(), 1);
}

#[test]
fn test_lookup_cache_eviction() {
    // Tiny byte budget: inserting many batches must evict or reject (the
    // bound is bytes, so the cache can't hold all 200 entries).
    let cache = LookupMemoryCache::new(
        1,
        LookupMemoryCacheConfig {
            capacity_bytes: 512,
            ttl: None,
        },
    );

    for i in 0..200u8 {
        cache.insert(&[i], test_batch(&format!("v{i}")));
    }

    assert!(
        cache.len() < 200,
        "byte bound did not evict: len {}",
        cache.len()
    );
}

#[test]
fn test_lookup_cache_invalidation() {
    let cache = small_cache(1);

    cache.insert(b"key1", test_batch("value1"));
    assert!(cache.get_cached(b"key1").is_hit());

    cache.invalidate(b"key1");
    assert!(cache.get_cached(b"key1").is_not_found());
}

#[test]
fn test_lookup_cache_table_id_isolation() {
    let cache_a = small_cache(1);
    let cache_b = small_cache(2);

    cache_a.insert(b"key1", test_batch("from_a"));
    cache_b.insert(b"key1", test_batch("from_b"));

    let batch_a = cache_a.get_cached(b"key1").into_batch().unwrap();
    let batch_b = cache_b.get_cached(b"key1").into_batch().unwrap();

    assert_eq!(batch_a.num_rows(), 1);
    assert_eq!(batch_b.num_rows(), 1);
    assert_ne!(batch_a, batch_b);
}

fn ttl_cache(ttl: Duration) -> LookupMemoryCache {
    LookupMemoryCache::new(
        1,
        LookupMemoryCacheConfig {
            capacity_bytes: 64 * 1024,
            ttl: Some(ttl),
        },
    )
}

#[test]
fn test_ttl_zero_expires_immediately() {
    // A zero TTL means every entry is already expired on the next read.
    let cache = ttl_cache(Duration::ZERO);
    cache.insert(b"k", test_batch("v"));
    assert!(cache.get_cached(b"k").is_not_found());
    // The expired entry was evicted, not just skipped.
    assert!(cache.is_empty());
}

#[test]
fn test_ttl_hit_then_expire() {
    let cache = ttl_cache(Duration::from_millis(20));
    cache.insert(b"k", test_batch("v"));
    // Fresh: still a hit.
    assert!(cache.get_cached(b"k").is_hit());
    std::thread::sleep(Duration::from_millis(40));
    // Past the TTL: lazy-expired to a miss.
    assert!(cache.get_cached(b"k").is_not_found());
    assert!(cache.is_empty());
}

#[test]
fn test_no_ttl_entry_survives() {
    // Without a TTL, an entry stays a hit regardless of age.
    let cache = small_cache(1);
    cache.insert(b"k", test_batch("v"));
    std::thread::sleep(Duration::from_millis(10));
    assert!(cache.get_cached(b"k").is_hit());
}
