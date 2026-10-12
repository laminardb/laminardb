use super::*;

/// Java `Utils.murmur2` reference values from librdkafka's `unittest_murmur2`, including the
/// unaligned-tail cases.
#[test]
fn murmur2_matches_java_reference_vectors() {
    let unaligned = b"PreAmbleWillBeRemoved,ThePrePartThatIs";
    let cases: [(&[u8], u32); 11] = [
        (b"kafka", 0xd067_cf64),
        (b"giberish123456789", 0x8f55_2b0c),
        (b"1234", 0x9fc9_7b14),
        (b"234", 0xe7c0_09ca),
        (b"34", 0x8739_30da),
        (b"4", 0x5a4b_5ca1),
        (unaligned, 0x7842_4f1c),
        (&unaligned[1..], 0x4a62_b377),
        (&unaligned[2..], 0xe0e4_e09e),
        (&unaligned[3..], 0x62b8_b43f),
        (b"", 0x106e_08d9),
    ];
    for (key, expected) in cases {
        assert_eq!(murmur2(key), expected, "{}", String::from_utf8_lossy(key));
    }
}

/// Java's `BuiltInPartitioner.partitionForKey`: `toPositive(murmur2(key)) % partitions`.
#[test]
fn key_hash_partition_matches_java_for_keyed_records() {
    let mut partitioner = KeyHashPartitioner::new();
    for partitions in [1, 3, 6, 12, 100] {
        let expected = i32::try_from((0xd067_cf64_u32 & 0x7fff_ffff) % partitions).unwrap();
        let partitions = i32::try_from(partitions).unwrap();
        assert_eq!(
            partitioner.partition(Some(b"kafka"), partitions),
            Some(expected)
        );
    }
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
