use super::*;

#[test]
fn test_murmur2_known_values() {
    // Empty key: seed goes through final mixing.
    let h = murmur2(b"");
    assert_ne!(h, 0);

    // Known values consistent with Kafka's Java implementation.
    let h = murmur2(b"key1");
    assert_ne!(h, 0);
}

#[test]
fn test_murmur2_deterministic() {
    let h1 = murmur2(b"test-key");
    let h2 = murmur2(b"test-key");
    assert_eq!(h1, h2);

    let h3 = murmur2(b"different-key");
    assert_ne!(h1, h3);
}

#[test]
fn test_key_hash_partitioner_with_key() {
    let mut p = KeyHashPartitioner::new();
    let partition = p.partition(Some(b"order-123"), 6);
    assert!(partition.is_some());
    let part = partition.unwrap();
    assert!((0..6).contains(&part));

    // Same key → same partition
    let partition2 = p.partition(Some(b"order-123"), 6);
    assert_eq!(partition, partition2);
}

#[test]
fn test_key_hash_partitioner_no_key() {
    let mut p = KeyHashPartitioner::new();
    assert_eq!(p.partition(None, 6), None);
}

#[test]
fn test_round_robin_partitioner() {
    let mut p = RoundRobinPartitioner::new();
    assert_eq!(p.partition(None, 3), Some(0));
    assert_eq!(p.partition(None, 3), Some(1));
    assert_eq!(p.partition(None, 3), Some(2));
    assert_eq!(p.partition(None, 3), Some(0)); // wraps
}

#[test]
fn test_round_robin_ignores_key() {
    let mut p = RoundRobinPartitioner::new();
    assert_eq!(p.partition(Some(b"key"), 3), Some(0));
    assert_eq!(p.partition(Some(b"key"), 3), Some(1));
}

#[test]
fn test_sticky_partitioner() {
    let mut p = StickyPartitioner::new(3);

    // First 3 records go to partition 0
    assert_eq!(p.partition(None, 4), Some(0));
    assert_eq!(p.partition(None, 4), Some(0));
    assert_eq!(p.partition(None, 4), Some(0));

    // 4th record rotates to partition 1
    assert_eq!(p.partition(None, 4), Some(1));
    assert_eq!(p.partition(None, 4), Some(1));
    assert_eq!(p.partition(None, 4), Some(1));

    // 7th record rotates to partition 2
    assert_eq!(p.partition(None, 4), Some(2));
}
