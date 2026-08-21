use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::checkpoint::{
    CheckpointParticipant, LeaderProof, LeaderProofOwner, MAX_CHECKPOINT_PARTICIPANTS,
};
use crate::cluster::discovery::NodeId;
use crate::state::PARTITIONING_ABI_VERSION;
use object_store::local::LocalFileSystem;
use tempfile::tempdir;
use uuid::Uuid;

fn participant(node_id: u64, boot: u128) -> CheckpointParticipant {
    CheckpointParticipant {
        node_id,
        boot_incarnation: Uuid::from_u128(boot),
    }
}

fn leader(node_id: u64, boot: u128, token: u64) -> LeaderProof {
    LeaderProof {
        owner: LeaderProofOwner {
            node_id,
            boot_id: Uuid::from_u128(boot),
            process_term: 1,
        },
        fencing_token: token,
    }
}

fn participants_for(vnodes: &BTreeMap<u32, NodeId>) -> Vec<CheckpointParticipant> {
    vnodes
        .values()
        .map(|owner| owner.0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|node| participant(node, u128::from(node)))
        .collect()
}

fn snapshot(vnodes: BTreeMap<u32, NodeId>) -> AssignmentSnapshot {
    let participants = participants_for(&vnodes);
    AssignmentSnapshot::empty()
        .next_for_participants(vnodes, participants)
        .unwrap()
}

fn next_snapshot(
    current: &AssignmentSnapshot,
    vnodes: BTreeMap<u32, NodeId>,
) -> AssignmentSnapshot {
    let participants = participants_for(&vnodes);
    current.next_for_participants(vnodes, participants).unwrap()
}

fn store_in(dir: &std::path::Path) -> AssignmentSnapshotStore {
    let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    AssignmentSnapshotStore::new(fs)
}

async fn put_raw(store: &AssignmentSnapshotStore, path: OsPath, snapshot: &AssignmentSnapshot) {
    let bytes = serde_json::to_vec(snapshot).unwrap();
    store
        .store
        .put(&path, PutPayload::from(Bytes::from(bytes)))
        .await
        .unwrap();
}

#[tokio::test]
async fn load_missing_returns_none() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());
    assert!(s.load().await.unwrap().is_none());
}

#[test]
fn assignment_generation_persists_exact_process_roster() {
    let vnodes = BTreeMap::from([(0, NodeId(1)), (1, NodeId(2))]);
    let first = AssignmentSnapshot::empty()
        .next_for_participants(vnodes.clone(), vec![participant(1, 11), participant(2, 22)])
        .unwrap();
    assert!(first.has_canonical_participants());

    let restarted = first
        .next_for_participants(vnodes, vec![participant(1, 11), participant(2, 222)])
        .unwrap();
    assert_eq!(restarted.version, first.version + 1);
    assert_eq!(restarted.vnodes, first.vnodes);
    assert_ne!(restarted.participants, first.participants);
    assert!(restarted.has_canonical_participants());
}

#[test]
fn assignment_generation_rejects_zero_vnode_participants() {
    let error = AssignmentSnapshot::empty()
        .next_for_participants(
            BTreeMap::from([(0, NodeId(1))]),
            vec![participant(1, 11), participant(2, 22)],
        )
        .unwrap_err();

    assert!(matches!(error, SnapshotError::Invalid(message) if message.contains("canonical")));
}

#[test]
fn assignment_snapshot_requires_partitioning_abi() {
    let snapshot = snapshot(BTreeMap::from([(0, NodeId(1))]));
    assert_eq!(snapshot.partitioning_abi_version, PARTITIONING_ABI_VERSION);

    let mut value = serde_json::to_value(snapshot).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .remove("partitioning_abi_version");
    assert!(serde_json::from_value::<AssignmentSnapshot>(value).is_err());
}

#[test]
fn assignment_snapshot_requires_the_current_wire_shape() {
    let value = serde_json::to_value(snapshot(BTreeMap::from([(0, NodeId(1))]))).unwrap();

    for field in ["draining", "drain_transition"] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<AssignmentSnapshot>(missing).is_err());
    }

    let mut nested_unknown = value.clone();
    nested_unknown["participants"][0]["retired_field"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<AssignmentSnapshot>(nested_unknown).is_err());

    let draining = snapshot(BTreeMap::from([(0, NodeId(1))]))
        .next_draining(
            BTreeMap::from([(0, NodeId(1))]),
            vec![participant(1, 1)],
            leader(1, 1, 1),
        )
        .unwrap();
    let mut nested_unknown = serde_json::to_value(draining).unwrap();
    nested_unknown["drain_transition"]["leader"]["owner"]["retired_field"] =
        serde_json::Value::Null;
    assert!(serde_json::from_value::<AssignmentSnapshot>(nested_unknown).is_err());

    let mut unknown = value;
    unknown["retired_field"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<AssignmentSnapshot>(unknown).is_err());
}

#[test]
fn assignment_snapshot_rejects_more_than_the_partitioning_abi_limit() {
    let vnodes = (0..=u32::from(u16::MAX))
        .map(|key_group| (key_group, NodeId(1)))
        .collect();

    assert!(matches!(
        AssignmentSnapshot::empty()
            .next_for_participants(vnodes, vec![participant(1, 11)]),
        Err(SnapshotError::Invalid(message)) if message.contains("key-group count")
    ));
}

#[tokio::test]
async fn durable_assignment_rejects_wrong_partitioning_abi() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let mut snapshot = snapshot(BTreeMap::from([(0, NodeId(1))]));
    snapshot.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;

    assert!(matches!(
        store.save_if_absent(&snapshot).await,
        Err(SnapshotError::Invalid(message)) if message.contains("partitioning ABI")
    ));

    put_raw(&store, snapshot_path(1), &snapshot).await;
    assert!(matches!(
        store.load().await,
        Err(SnapshotError::Invalid(message)) if message.contains("partitioning ABI")
    ));
}

#[tokio::test]
async fn save_if_absent_then_load_roundtrip() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    let mut vnodes = BTreeMap::new();
    vnodes.insert(0, NodeId(1));
    vnodes.insert(1, NodeId(2));
    let snap = snapshot(vnodes);

    assert_eq!(s.save_if_absent(&snap).await.unwrap().as_ref(), Some(&snap),);
    let loaded = s.load().await.unwrap().unwrap();
    assert_eq!(loaded, snap);
}

#[test]
fn snapshot_path_is_canonical_across_the_u64_range() {
    assert_eq!(
        snapshot_path(1).as_ref(),
        "control/assignment-snapshots/v00000000000000000001.json"
    );
    assert_eq!(
        snapshot_path(u64::MAX).as_ref(),
        "control/assignment-snapshots/v18446744073709551615.json"
    );
}

#[test]
fn next_rejects_generation_overflow() {
    let mut current = snapshot(BTreeMap::from([(0, NodeId(1))]));
    current.version = u64::MAX;
    assert!(matches!(
        current.next(current.vnodes.clone()),
        Err(SnapshotError::Invalid(message)) if message.contains("overflow")
    ));
}

#[tokio::test]
async fn seed_write_rejects_non_seed_generation() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
    let second = first.next(first.vnodes.clone()).unwrap();

    assert!(matches!(
        store.save_if_absent(&second).await,
        Err(SnapshotError::Invalid(message)) if message.contains("version-one seed")
    ));
    assert!(store.load().await.unwrap().is_none());
}

#[tokio::test]
async fn seed_write_rejects_retained_nonempty_history() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
    store.save_if_absent(&first).await.unwrap();
    let second = first.next(first.vnodes.clone()).unwrap();
    store.save_if_version(&second, first.version).await.unwrap();
    let third = second.next(second.vnodes.clone()).unwrap();
    store.save_if_version(&third, second.version).await.unwrap();
    store.prune_before(3).await.unwrap();

    assert!(matches!(
        store.save_if_absent(&first).await,
        Err(SnapshotError::Invalid(message)) if message.contains("durable head 3")
    ));
    assert_eq!(store.list_versions().await.unwrap(), vec![3]);
}

#[tokio::test]
async fn save_rejects_noncanonical_owner_map_and_roster() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let canonical = snapshot(BTreeMap::from([(0, NodeId(1))]));

    let mut sparse = canonical.clone();
    sparse.vnodes = BTreeMap::from([(1, NodeId(1))]);
    assert!(matches!(
        store.save_if_absent(&sparse).await,
        Err(SnapshotError::Invalid(_))
    ));

    let mut uncovered = canonical;
    uncovered.participants.clear();
    assert!(matches!(
        store.save_if_absent(&uncovered).await,
        Err(SnapshotError::Invalid(_))
    ));
}

#[tokio::test]
async fn durable_assignment_rejects_oversized_participant_roster() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let maximum = u64::try_from(MAX_CHECKPOINT_PARTICIPANTS).unwrap();
    let participants = (1..=maximum + 1)
        .map(|node_id| participant(node_id, u128::from(node_id)))
        .collect();
    let oversized = AssignmentSnapshot {
        version: 1,
        partitioning_abi_version: PARTITIONING_ABI_VERSION,
        vnodes: BTreeMap::from([(0, NodeId(1))]),
        participants,
        updated_at_ms: 1,
        draining: false,
        drain_transition: None,
    };

    assert!(matches!(
        store.save_if_absent(&oversized).await,
        Err(SnapshotError::Invalid(message)) if message.contains("maximum is 129")
    ));

    put_raw(&store, snapshot_path(1), &oversized).await;
    assert!(matches!(
        store.load().await,
        Err(SnapshotError::Invalid(message)) if message.contains("maximum is 129")
    ));
}

#[tokio::test]
async fn load_rejects_path_payload_version_mismatch() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
    put_raw(&store, snapshot_path(2), &first).await;

    assert!(matches!(
        store.load_version(2).await,
        Err(SnapshotError::Invalid(message)) if message.contains("payload version")
    ));
}

#[tokio::test]
async fn load_rejects_generation_gap() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
    let second = first.next(first.vnodes.clone()).unwrap();
    let third = second.next(second.vnodes.clone()).unwrap();
    put_raw(&store, snapshot_path(1), &first).await;
    put_raw(&store, snapshot_path(3), &third).await;

    assert!(matches!(
        store.load().await,
        Err(SnapshotError::Invalid(message)) if message.contains("not contiguous")
    ));
}

#[tokio::test]
async fn load_rejects_noncanonical_snapshot_filename() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
    put_raw(
        &store,
        OsPath::from("control/assignment-snapshots/v1.json"),
        &first,
    )
    .await;

    assert!(matches!(
        store.load().await,
        Err(SnapshotError::Invalid(message)) if message.contains("filename")
    ));
}

#[tokio::test]
async fn load_returns_highest_version() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    let mut v1_map = BTreeMap::new();
    v1_map.insert(0, NodeId(1));
    let v1 = snapshot(v1_map);
    s.save_if_absent(&v1).await.unwrap();

    let mut v2_map = BTreeMap::new();
    v2_map.insert(0, NodeId(2));
    let v2 = next_snapshot(&v1, v2_map);
    // Rotate via save_if_version — the canonical post-boot path.
    assert!(matches!(
        s.save_if_version(&v2, v1.version).await.unwrap(),
        RotateOutcome::Rotated,
    ));

    let loaded = s.load().await.unwrap().unwrap();
    assert_eq!(loaded.version, 2);
    assert_eq!(loaded.vnodes.get(&0), Some(&NodeId(2)));

    // Older version is still readable directly until pruned.
    let v1_loaded = s.load_version(1).await.unwrap().unwrap();
    assert_eq!(v1_loaded, v1);
}

#[tokio::test]
async fn save_if_absent_first_writer_wins() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    let mut first_map = BTreeMap::new();
    first_map.insert(0, NodeId(1));
    first_map.insert(1, NodeId(2));
    let first = snapshot(first_map);

    let winner = s.save_if_absent(&first).await.unwrap();
    assert_eq!(winner.as_ref(), Some(&first), "first writer must win");

    // Second writer attempts a different assignment; should be
    // rejected without mutating the store.
    let mut second_map = BTreeMap::new();
    second_map.insert(0, NodeId(99));
    let second = snapshot(second_map);
    let rejected = s.save_if_absent(&second).await.unwrap();
    assert!(rejected.is_none(), "second writer must lose the CAS");

    let loaded = s.load().await.unwrap().unwrap();
    assert_eq!(loaded, first, "stored snapshot is the first writer's");
}

#[tokio::test]
async fn save_if_version_rejects_non_monotonic_bump() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    let mut m = BTreeMap::new();
    m.insert(0, NodeId(1));
    let v1 = snapshot(m);
    s.save_if_absent(&v1).await.unwrap();

    // Caller builds v3 but claims prior=1 — enforcing monotonic +1
    // catches accidental gap-skipping bugs before they land on
    // durable storage.
    let mut m2 = BTreeMap::new();
    m2.insert(0, NodeId(2));
    let v2 = next_snapshot(&v1, m2);
    let mut m3 = BTreeMap::new();
    m3.insert(0, NodeId(3));
    let v3 = next_snapshot(&v2, m3);
    let err = s.save_if_version(&v3, 1).await.unwrap_err();
    assert!(
        matches!(err, SnapshotError::Invalid(msg) if msg.contains("monotonic")),
        "non-monotonic bump must surface a clear error",
    );
}

#[tokio::test]
async fn save_if_version_rejects_future_prior_without_punching_a_gap() {
    let dir = tempdir().unwrap();
    let store = store_in(dir.path());
    let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
    store.save_if_absent(&first).await.unwrap();
    let second = first.next(first.vnodes.clone()).unwrap();
    store.save_if_version(&second, first.version).await.unwrap();
    let third = second.next(second.vnodes.clone()).unwrap();
    store.save_if_version(&third, second.version).await.unwrap();
    let fourth = third.next(third.vnodes.clone()).unwrap();
    let fifth = fourth.next(fourth.vnodes.clone()).unwrap();
    let sixth = fifth.next(fifth.vnodes.clone()).unwrap();

    assert!(matches!(
        store.save_if_version(&sixth, fifth.version).await,
        Err(SnapshotError::Invalid(message)) if message.contains("durable head 5")
    ));
    assert_eq!(store.list_versions().await.unwrap(), vec![1, 2, 3]);
    assert_eq!(store.load().await.unwrap().unwrap(), third);
}

#[tokio::test]
async fn save_if_version_succeeds_on_match() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    let mut v1_map = BTreeMap::new();
    v1_map.insert(0, NodeId(1));
    let first = snapshot(v1_map);
    s.save_if_absent(&first).await.unwrap();

    let mut v2_map = BTreeMap::new();
    v2_map.insert(0, NodeId(2));
    let second = next_snapshot(&first, v2_map);
    let outcome = s.save_if_version(&second, first.version).await.unwrap();
    assert!(matches!(outcome, RotateOutcome::Rotated));

    let loaded = s.load().await.unwrap().unwrap();
    assert_eq!(loaded, second);
}

#[tokio::test]
async fn save_if_version_conflict_surfaces_winner() {
    // Two racing rotations both propose v2 from v1. CAS at
    // `v{2}.json` picks one; the loser reloads and finds the
    // winner's canonical snapshot.
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    let mut seed = BTreeMap::new();
    seed.insert(0, NodeId(1));
    let v1 = snapshot(seed);
    s.save_if_absent(&v1).await.unwrap();

    let mut winner_map = BTreeMap::new();
    winner_map.insert(0, NodeId(10));
    let winner = next_snapshot(&v1, winner_map);
    assert!(matches!(
        s.save_if_version(&winner, v1.version).await.unwrap(),
        RotateOutcome::Rotated,
    ));

    let mut loser_map = BTreeMap::new();
    loser_map.insert(0, NodeId(20));
    let loser = next_snapshot(&v1, loser_map);
    match s.save_if_version(&loser, v1.version).await.unwrap() {
        RotateOutcome::Conflict(current) => {
            assert_eq!(
                *current, winner,
                "conflict must surface the winner's snapshot",
            );
        }
        RotateOutcome::Rotated => {
            panic!("stale-token update must not win the CAS");
        }
    }

    let loaded = s.load().await.unwrap().unwrap();
    assert_eq!(loaded, winner, "stored snapshot is the CAS winner's");
}

#[tokio::test]
async fn prune_before_drops_old_versions() {
    let dir = tempdir().unwrap();
    let s = store_in(dir.path());

    // Seed v1..=v4 by repeatedly rotating.
    let mut m = BTreeMap::new();
    m.insert(0, NodeId(1));
    let mut current = snapshot(m);
    s.save_if_absent(&current).await.unwrap();
    for _ in 0..3 {
        let next = current.next(current.vnodes.clone()).unwrap();
        s.save_if_version(&next, current.version).await.unwrap();
        current = next;
    }

    s.prune_before(3).await.unwrap();

    assert!(s.load_version(1).await.unwrap().is_none());
    assert!(s.load_version(2).await.unwrap().is_none());
    assert!(s.load_version(3).await.unwrap().is_some());
    assert!(s.load_version(4).await.unwrap().is_some());
    // `load()` still returns the most recent surviving snapshot.
    assert_eq!(s.load().await.unwrap().unwrap().version, 4);
}

#[tokio::test]
async fn prune_stops_at_first_delete_failure_without_punching_a_gap() {
    use crate::cluster::testing::{FaultyObjectStore, ObjectStoreFault};
    use object_store::memory::InMemory;

    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let faulty = Arc::new(FaultyObjectStore::new(inner));
    let wrapped: Arc<dyn ObjectStore> = faulty.clone();
    let store = AssignmentSnapshotStore::new(wrapped);
    let mut current = snapshot(BTreeMap::from([(0, NodeId(1))]));
    store.save_if_absent(&current).await.unwrap();
    for _ in 0..2 {
        let next = current.next(current.vnodes.clone()).unwrap();
        store.save_if_version(&next, current.version).await.unwrap();
        current = next;
    }

    faulty.set_fault(ObjectStoreFault::FailWrites);
    assert!(matches!(
        store.prune_before(3).await,
        Err(SnapshotError::Io(_))
    ));
    faulty.set_fault(ObjectStoreFault::None);
    assert_eq!(store.list_versions().await.unwrap(), vec![1, 2, 3]);
}

#[test]
fn empty_starts_at_version_zero() {
    let s = AssignmentSnapshot::empty();
    assert_eq!(s.version, 0);
    assert!(s.vnodes.is_empty());
}

#[test]
fn next_bumps_version() {
    let mut vnodes = BTreeMap::new();
    vnodes.insert(0, NodeId(1));
    let s = snapshot(vnodes);
    assert_eq!(s.version, 1);
}

#[test]
fn roundtrip_vec_conversions() {
    let assignment = vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)];
    let map = AssignmentSnapshot::vnodes_from_vec(&assignment);
    let snap = snapshot(map);
    let back = snap
        .to_vnode_vec(u32::try_from(assignment.len()).expect("test len fits u32"))
        .unwrap();
    assert_eq!(back, assignment);
}

#[test]
fn dense_conversion_rejects_smaller_and_larger_runtime_cardinality() {
    let snap = snapshot(BTreeMap::from([(0, NodeId(1)), (1, NodeId(1))]));
    for count in [1, 3] {
        assert!(matches!(
            snap.to_vnode_vec(count),
            Err(SnapshotError::Invalid(message)) if message.contains("vnode cardinality")
        ));
    }
}

#[tokio::test]
async fn recovery_proposal_stage_and_materialization_are_idempotent() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = AssignmentSnapshotStore::new(backing);
    let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
    store.save_if_absent(&predecessor).await.unwrap();
    let proposal = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));

    let first_reference = store.stage_recovery_proposal(&proposal).await.unwrap();
    let retry_reference = store.stage_recovery_proposal(&proposal).await.unwrap();
    assert_eq!(retry_reference, first_reference);
    assert_eq!(
        store
            .load_recovery_proposal(&first_reference)
            .await
            .unwrap(),
        proposal
    );
    assert!(matches!(
        store.materialize_recovery(&first_reference).await.unwrap(),
        RotateOutcome::Rotated
    ));
    assert!(matches!(
        store.materialize_recovery(&first_reference).await.unwrap(),
        RotateOutcome::Conflict(existing) if *existing == proposal
    ));
}

#[tokio::test]
async fn recovery_materialization_surfaces_a_different_same_version_winner() {
    let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = AssignmentSnapshotStore::new(backing);
    let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
    store.save_if_absent(&predecessor).await.unwrap();
    let winner = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));
    let loser = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(3))]));
    let winner_reference = store.stage_recovery_proposal(&winner).await.unwrap();
    let loser_reference = store.stage_recovery_proposal(&loser).await.unwrap();

    assert!(matches!(
        store.materialize_recovery(&winner_reference).await.unwrap(),
        RotateOutcome::Rotated
    ));
    assert!(matches!(
        store.materialize_recovery(&loser_reference).await.unwrap(),
        RotateOutcome::Conflict(existing) if *existing == winner
    ));
}

#[tokio::test]
async fn recovery_retention_removes_winning_and_losing_staged_bodies() {
    let backing = Arc::new(object_store::memory::InMemory::new());
    let store = AssignmentSnapshotStore::new(backing.clone());
    let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
    store.save_if_absent(&predecessor).await.unwrap();
    let winner = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));
    let loser = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(3))]));
    let winner_reference = store.stage_recovery_proposal(&winner).await.unwrap();
    let loser_reference = store.stage_recovery_proposal(&loser).await.unwrap();
    assert!(matches!(
        store.materialize_recovery(&winner_reference).await.unwrap(),
        RotateOutcome::Rotated
    ));
    let successor = next_snapshot(&winner, winner.vnodes.clone());
    assert!(matches!(
        store
            .save_if_version(&successor, winner.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));

    store.prune_before(successor.version).await.unwrap();

    for reference in [&winner_reference, &loser_reference] {
        assert!(matches!(
            backing.get(&recovery_proposal_path(reference)).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
    assert!(matches!(
        backing
            .get(&recovery_materialization_path(winner.version))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert!(store.load_version(winner.version).await.unwrap().is_none());
    assert_eq!(store.load().await.unwrap(), Some(successor));
}

#[tokio::test]
async fn recovery_materialization_rejects_a_tampered_staged_body() {
    let backing = Arc::new(object_store::memory::InMemory::new());
    let store = AssignmentSnapshotStore::new(backing.clone());
    let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
    store.save_if_absent(&predecessor).await.unwrap();
    let proposal = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));
    let reference = store.stage_recovery_proposal(&proposal).await.unwrap();
    let (mut tampered, encoded_reference) = proposal.encode_recovery_proposal().unwrap();
    assert_eq!(encoded_reference, reference);
    let marker = b"\"updated_at_ms\":";
    let value_start = tampered
        .windows(marker.len())
        .position(|window| window == marker)
        .unwrap()
        + marker.len();
    let digit = tampered[value_start..]
        .iter()
        .position(u8::is_ascii_digit)
        .map(|offset| value_start + offset)
        .unwrap();
    tampered[digit] = if tampered[digit] == b'9' { b'8' } else { b'9' };
    backing
        .put(
            &recovery_proposal_path(&reference),
            PutPayload::from(Bytes::from(tampered)),
        )
        .await
        .unwrap();

    assert!(matches!(
        store.load_recovery_proposal(&reference).await,
        Err(SnapshotError::Invalid(message)) if message.contains("content-addressed reference")
    ));
    assert!(store.materialize_recovery(&reference).await.is_err());
    assert_eq!(store.load().await.unwrap(), Some(predecessor));
}

#[test]
fn draining_survives_roundtrip() {
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant(1, 1)])
        .unwrap();
    assert!(!committed.draining);

    let drain = committed
        .next_draining(
            BTreeMap::from([(0, NodeId(2))]),
            vec![participant(2, 2)],
            leader(1, 1, 7),
        )
        .unwrap();
    let json = serde_json::to_vec(&drain).unwrap();
    let back: AssignmentSnapshot = serde_json::from_slice(&json).unwrap();
    back.validate().unwrap();
    assert!(back.draining);
    assert_eq!(back.drain_transition, drain.drain_transition);
    assert_eq!(back.version, drain.version);
}

#[tokio::test]
async fn drain_finalization_commits_the_certified_target_version() {
    let directory = tempdir().unwrap();
    let store = store_in(directory.path());
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant(1, 1)])
        .unwrap();
    store.save_if_absent(&committed).await.unwrap();
    let drain = committed
        .next_draining(
            BTreeMap::from([(0, NodeId(2))]),
            vec![participant(2, 2)],
            leader(1, 1, 7),
        )
        .unwrap();
    store
        .save_if_version(&drain, committed.version)
        .await
        .unwrap();
    let transition = drain.drain_transition.as_ref().unwrap().clone();
    let target = drain.committed_target().unwrap();

    assert!(matches!(
        store.finalize_drain(&drain, &target).await.unwrap(),
        RotateOutcome::Rotated
    ));
    let loaded = store.load().await.unwrap().unwrap();
    assert_eq!(loaded, target);
    assert_eq!(loaded.version, drain.version);
    assert_eq!(
        loaded.assignment_fence().unwrap(),
        transition.target.clone()
    );
    assert_eq!(
        store.load_drain_transition(drain.version).await.unwrap(),
        Some(transition)
    );
}

#[tokio::test]
async fn concurrent_drain_commit_and_abort_have_one_append_only_winner() {
    let memory: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let left = AssignmentSnapshotStore::new(Arc::clone(&memory));
    let right = AssignmentSnapshotStore::new(memory);
    let predecessor = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant(1, 1)])
        .unwrap();
    left.save_if_absent(&predecessor).await.unwrap();
    let drain = predecessor
        .next_draining(
            BTreeMap::from([(0, NodeId(2))]),
            vec![participant(2, 2)],
            leader(1, 1, 9),
        )
        .unwrap();
    left.save_if_version(&drain, predecessor.version)
        .await
        .unwrap();
    let commit = drain.committed_target().unwrap();
    let abort = drain.aborted_target(&predecessor).unwrap();

    let (commit_result, abort_result) = tokio::join!(
        left.finalize_drain(&drain, &commit),
        right.finalize_drain(&drain, &abort)
    );
    let outcomes = [commit_result.unwrap(), abort_result.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RotateOutcome::Rotated))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RotateOutcome::Conflict(_)))
            .count(),
        1
    );
    let loaded = left.load().await.unwrap().unwrap();
    assert!(loaded == commit || loaded == abort);
}
