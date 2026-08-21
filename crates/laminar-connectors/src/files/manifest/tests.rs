use super::*;

#[test]
fn insert_and_exact_membership() {
    let mut manifest = FileIngestionManifest::new();
    assert!(!manifest.contains("a.csv"));
    manifest.insert("a.csv".into());
    assert!(manifest.contains("a.csv"));
    assert!(!manifest.contains("b.csv"));
}

#[test]
fn inventory_never_evicts_or_false_positives() {
    let mut manifest = FileIngestionManifest::new();
    for i in 0..10_000 {
        manifest.insert(format!("file_{i}.csv"));
    }
    assert_eq!(manifest.processed_count(), 10_000);
    for i in 10_000..20_000 {
        assert!(!manifest.contains(&format!("file_{i}.csv")));
    }
}

#[test]
fn checkpoint_snapshot_is_immutable_and_roundtrips() {
    let mut manifest = FileIngestionManifest::new();
    manifest.insert("a.csv".into());
    let mut old_checkpoint = SourceCheckpoint::new();
    manifest.to_checkpoint(&mut old_checkpoint);

    manifest.insert("b.csv".into());
    let mut new_checkpoint = SourceCheckpoint::new();
    manifest.to_checkpoint(&mut new_checkpoint);

    let old = FileIngestionManifest::from_checkpoint(&old_checkpoint).unwrap();
    assert!(old.contains("a.csv"));
    assert!(!old.contains("b.csv"));
    let new = FileIngestionManifest::from_checkpoint(&new_checkpoint).unwrap();
    assert!(new.contains("a.csv"));
    assert!(new.contains("b.csv"));
}

#[test]
fn discovery_snapshot_is_constant_time_live_exact_view() {
    let mut manifest = FileIngestionManifest::new();
    manifest.insert("a.csv".into());
    let snapshot = manifest.snapshot_for_dedup();
    manifest.insert("b.csv".into());

    assert!(snapshot.contains("a.csv"));
    assert!(snapshot.contains("b.csv"));
    assert!(!snapshot.contains("unknown.csv"));
}

#[test]
fn duplicate_insert_does_not_grow_serialized_inventory() {
    let mut manifest = FileIngestionManifest::new();
    manifest.insert("a.csv".into());
    manifest.insert("a.csv".into());
    assert_eq!(manifest.processed_count(), 1);
    assert_eq!(manifest.serialized_paths.fragment_count(), 1);
}

#[test]
fn empty_manifest_checkpoint_roundtrip() {
    let manifest = FileIngestionManifest::new();
    let mut checkpoint = SourceCheckpoint::new();
    manifest.to_checkpoint(&mut checkpoint);
    let restored = FileIngestionManifest::from_checkpoint(&checkpoint).unwrap();
    assert_eq!(restored.processed_count(), 0);
}
