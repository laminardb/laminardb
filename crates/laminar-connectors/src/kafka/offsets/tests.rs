use super::*;

#[test]
fn test_update_and_get() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);
    tracker.update("events", 1, 200);

    assert_eq!(tracker.get("events", 0), Some(100));
    assert_eq!(tracker.get("events", 1), Some(200));
    assert_eq!(tracker.get("events", 2), None);
    assert_eq!(tracker.partition_count(), 2);
}

#[test]
fn test_update_advances_forward() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);
    tracker.update("events", 0, 200);
    assert_eq!(tracker.get("events", 0), Some(200));
}

#[test]
fn test_update_rejects_regression() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 200);
    tracker.update("events", 0, 100); // should be ignored
    assert_eq!(tracker.get("events", 0), Some(200));
}

#[test]
fn test_update_rejects_equal() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);
    tracker.update("events", 0, 100); // same offset, no change
    assert_eq!(tracker.get("events", 0), Some(100));
}

#[test]
fn test_update_force_overwrites() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 200);
    tracker.update_force("events", 0, 50); // force allows regression
    assert_eq!(tracker.get("events", 0), Some(50));
}

#[test]
fn test_checkpoint_roundtrip() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);
    tracker.update("events", 1, 200);
    tracker.update("orders", 0, 50);

    let cp = tracker.to_checkpoint_for_partitions([("events", 0), ("events", 1), ("orders", 0)]);
    let restored = OffsetTracker::try_from_checkpoint(&cp).unwrap();

    assert_eq!(restored.get("events", 0), Some(100));
    assert_eq!(restored.get("events", 1), Some(200));
    assert_eq!(restored.get("orders", 0), Some(50));
    assert_eq!(restored.partition_count(), 3);
}

#[test]
fn from_offset_map_supports_hyphenated_topics() {
    let map = HashMap::from([
        ("events:5".to_string(), "100".to_string()),
        ("my-topic:2".to_string(), "7".to_string()),
        ("trailing-hyphen-:3".to_string(), "9".to_string()),
    ]);
    let tracker = OffsetTracker::try_from_offset_map(&map).unwrap();
    assert_eq!(tracker.get("events", 5), Some(100));
    assert_eq!(tracker.get("my-topic", 2), Some(7));
    assert_eq!(tracker.get("trailing-hyphen-", 3), Some(9));
    assert_eq!(tracker.partition_count(), 3);
}

#[test]
fn from_offset_map_ignores_reserved_partition_baselines() {
    let map = HashMap::from([
        ("events:0".to_string(), "100".to_string()),
        (
            format!("{KAFKA_PARTITION_BASELINE_PREFIX}events:0"),
            "7".to_string(),
        ),
        (
            format!("{KAFKA_PARTITION_BASELINE_PREFIX}events:1"),
            "9".to_string(),
        ),
    ]);

    let tracker = OffsetTracker::try_from_offset_map(&map).unwrap();
    assert_eq!(tracker.get("events", 0), Some(100));
    assert_eq!(tracker.get("events", 1), None);
    assert_eq!(tracker.partition_count(), 1);
}

#[test]
fn checkpoint_roundtrips_topic_ending_in_hyphen() {
    let mut tracker = OffsetTracker::new();
    tracker.update("trailing-hyphen-", 3, 9);

    let checkpoint = tracker.to_checkpoint_for_partitions([("trailing-hyphen-", 3)]);
    assert_eq!(checkpoint.get_offset("trailing-hyphen-:3"), Some("9"));

    let restored = OffsetTracker::try_from_checkpoint(&checkpoint).unwrap();
    assert_eq!(restored.get("trailing-hyphen-", 3), Some(9));
}

#[test]
fn try_from_offset_map_rejects_malformed_entries() {
    for (key, value) in [
        ("bad-partition:x", "10"),
        ("noseparator", "10"),
        ("events-0", "10"),
        ("offset:0", "notanumber"),
        ("negative-partition:-1", "10"),
        ("negative-offset:0", "-1"),
        ("noncanonical-partition:00", "10"),
        ("noncanonical-offset:0", "010"),
        ("invalid:topic:0", "10"),
    ] {
        let map = HashMap::from([(key.to_string(), value.to_string())]);
        assert!(
            OffsetTracker::try_from_offset_map(&map).is_err(),
            "malformed offset entry {key}={value} was accepted"
        );
    }
}

#[test]
fn from_offset_map_empty_is_empty() {
    let tracker = OffsetTracker::try_from_offset_map(&HashMap::new()).unwrap();
    assert_eq!(tracker.partition_count(), 0);
}

#[test]
fn test_empty_tracker() {
    let tracker = OffsetTracker::new();
    assert_eq!(tracker.partition_count(), 0);
    assert!(tracker
        .to_checkpoint_for_partitions(std::iter::empty())
        .is_empty());
}

#[test]
fn test_topic_partition_list() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 99);
    tracker.update("events", 1, 199);

    let tpl = tracker.to_topic_partition_list();
    let elements = tpl.elements();
    assert_eq!(elements.len(), 2);

    for elem in &elements {
        match elem.partition() {
            0 => assert_eq!(elem.offset(), Offset::Offset(100)),
            1 => assert_eq!(elem.offset(), Offset::Offset(200)),
            _ => panic!("unexpected partition"),
        }
    }
}

#[test]
fn test_multi_topic_checkpoint() {
    let mut tracker = OffsetTracker::new();
    tracker.update("topic-a", 0, 10);
    tracker.update("topic-b", 0, 20);

    let cp = tracker.to_checkpoint_for_partitions([("topic-a", 0), ("topic-b", 0)]);
    let restored = OffsetTracker::try_from_checkpoint(&cp).unwrap();

    assert_eq!(restored.get("topic-a", 0), Some(10));
    assert_eq!(restored.get("topic-b", 0), Some(20));
}

#[test]
fn test_checkpoint_metadata() {
    let tracker = OffsetTracker::new();
    let cp = tracker.to_checkpoint_for_partitions(std::iter::empty());
    assert_eq!(cp.get_metadata("connector"), Some("kafka"));
    assert_eq!(
        cp.get_metadata(KAFKA_CHECKPOINT_VERSION_KEY),
        Some(KAFKA_CHECKPOINT_VERSION)
    );

    let mut previous = cp;
    previous.set_metadata(KAFKA_CHECKPOINT_VERSION_KEY, "1");
    let error = OffsetTracker::try_from_checkpoint(&previous).unwrap_err();
    assert!(error.to_string().contains("checkpoint.version=2"));
}

#[test]
fn test_retain_assigned() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);
    tracker.update("events", 1, 200);
    tracker.update("events", 2, 300);
    tracker.update("orders", 0, 50);

    let mut assigned = HashSet::new();
    assigned.insert(("events".to_string(), 0));
    assigned.insert(("events".to_string(), 2));
    // orders partition 0 and events partition 1 are NOT assigned (revoked)

    tracker.retain_assigned(&assigned);

    assert_eq!(tracker.get("events", 0), Some(100));
    assert_eq!(tracker.get("events", 1), None); // removed
    assert_eq!(tracker.get("events", 2), Some(300));
    assert_eq!(tracker.get("orders", 0), None); // removed
    assert_eq!(tracker.partition_count(), 2);
}

#[test]
fn test_retain_assigned_empty_set_clears_all() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);

    tracker.retain_assigned(&HashSet::new());

    assert_eq!(tracker.partition_count(), 0);
}

#[test]
fn test_to_checkpoint_for_partitions() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);
    tracker.update("events", 1, 200);
    tracker.update("orders", 0, 50);

    let cp = tracker.to_checkpoint_for_partitions([
        ("events", 0),
        ("orders", 0),
        ("events", 0),
        ("unknown", 9),
    ]);

    assert_eq!(cp.get_offset("events:0"), Some("100"));
    assert_eq!(cp.get_offset("events:1"), None); // filtered out
    assert_eq!(cp.get_offset("orders:0"), Some("50"));
    assert_eq!(cp.get_metadata("connector"), Some("kafka"));
    assert_eq!(tracker.partition_count(), 3);
}

#[test]
fn test_to_checkpoint_for_partitions_empty_returns_empty() {
    let mut tracker = OffsetTracker::new();
    tracker.update("events", 0, 100);
    tracker.update("events", 1, 200);

    // Empty assigned set → no owned partitions → empty checkpoint
    let cp = tracker.to_checkpoint_for_partitions(std::iter::empty());

    assert!(cp.is_empty());
    assert_eq!(cp.get_metadata("connector"), Some("kafka"));
}
