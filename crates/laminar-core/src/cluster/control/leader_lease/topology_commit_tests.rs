//! Exact Commit boundaries use the existing root fixture and conditional-write fault store.

use super::*;
use crate::cluster::control::{
    LocalProcessAuthorityIdentity, TopologyRestoreInput, TOPOLOGY_COMMIT_PROTOCOL_VERSION,
};

#[path = "topology_activation_tests.rs"]
mod activation;

async fn prepared(authority: &LeaderLeaseStore) -> (Fixture, TopologyRestoreInput) {
    let fixture = fixture(authority).await;
    prepared_fixture(authority, fixture).await
}

pub(super) async fn prepared_fixture(
    authority: &LeaderLeaseStore,
    fixture: Fixture,
) -> (Fixture, TopologyRestoreInput) {
    fixture.stage(authority).await.unwrap();
    for index in 0..2 {
        let input = target_preparation::input(authority, &fixture, index).await;
        authority
            .certify_topology_target_preparation(
                &fixture.assignments,
                &topology_preparation::processes(authority),
                &input,
                TOPOLOGY_COMMIT_PROTOCOL_VERSION,
            )
            .await
            .unwrap();
    }
    let input = target_preparation::input(authority, &fixture, 0).await;
    (fixture, input)
}

pub(super) async fn commit(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    input: &TopologyRestoreInput,
) -> Result<TopologyAdmissionStatus, TopologyError> {
    authority
        .commit_topology_target(
            &fixture.lease.proof(),
            &fixture.assignments,
            &topology_preparation::processes(authority),
            input,
        )
        .await
}

pub(super) async fn reconstruct(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    process: LocalProcessAuthorityIdentity,
) -> Result<TopologyRestoreInput, TopologyError> {
    authority
        .committed_topology_restore_input(
            &fixture.assignments,
            &topology_preparation::processes(authority),
            fixture.operation.operation_id,
            process,
        )
        .await
}

#[tokio::test]
async fn topology_commit_atomically_publishes_target_and_exact_recoverable_root() {
    let authority = store(30_000);
    let (fixture, input) = prepared(&authority).await;
    let before = authority.load_record().await.unwrap().unwrap();
    let parent_admission = authority.recovery_admission_snapshot().await.unwrap();
    let committed = commit(&authority, &fixture, &input).await.unwrap();
    let head = authority.load_record().await.unwrap().unwrap();
    assert_eq!(head.version, TOPOLOGY_COMMIT_RECORD_VERSION);
    assert_eq!(committed.phase, TopologyAdmissionPhase::Committed);
    let decision = committed.commit.as_ref().unwrap();
    assert_eq!(decision.authority_sequence, before.lease.seq + 1);
    assert_eq!(decision.topology_version.get(), 2);
    assert_eq!(
        head.lease.catalog_manifest.as_ref(),
        Some(&fixture.descriptor.target_manifest)
    );
    assert_eq!(head.topology_baseline, before.topology_baseline);
    assert_eq!(head.commit_head, before.commit_head);
    assert_eq!(head.outcome_head, before.outcome_head);
    assert_eq!(committed.cut, input.operation().cut);
    assert_eq!(committed.migration_root, input.operation().migration_root);
    assert_eq!(
        committed.target_preparations,
        input.operation().target_preparations
    );
    assert!(authority
        .topology_restore_input(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            committed.operation_id,
            input.process()
        )
        .await
        .is_err());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let recovery = reconstruct(&reopened, &fixture, input.process())
        .await
        .unwrap();
    assert!(recovery.is_committed());
    assert!(recovery.is_committed_successor_of(&input));
    assert_eq!(recovery.checkpoint(), &fixture.index);
    assert_eq!(
        recovery.checkpoint().pipeline_identity,
        fixture.descriptor.parent_pipeline
    );
    assert_ne!(
        recovery.checkpoint().pipeline_identity,
        fixture.descriptor.target_pipeline
    );
    recovery
        .root()
        .validate_restore_cut(
            recovery.operation(),
            recovery.descriptor(),
            recovery.checkpoint(),
            &fixture.manifests,
        )
        .unwrap();
    assert_eq!(
        commit(&reopened, &fixture, &input).await.unwrap(),
        committed
    );
    assert_eq!(reopened.load().await.unwrap().unwrap().seq, head.lease.seq);
    let (catalog, status) = reopened.catalog_with_topology().await.unwrap().unwrap();
    assert_eq!(catalog.reference().unwrap(), decision.manifest);
    assert_eq!(status.committed_version().unwrap().get(), 2);
    assert!(
        matches!(status, crate::cluster::control::TopologyCatalogState::Versioned { committed: Some(ref stored), .. } if stored == decision)
    );
    let target_admission = authority.recovery_admission_snapshot().await.unwrap();
    assert_eq!(target_admission.topology_commit(), Some(decision));
    assert!(!authority
        .recovery_admission_is_current(&target_admission, &fixture.lease.proof())
        .await
        .unwrap());
    assert!(!authority
        .recovery_admission_is_current(&parent_admission, &fixture.lease.proof())
        .await
        .unwrap());
}

#[tokio::test]
async fn topology_commit_requires_every_protocol_four_owner_and_evidence_process() {
    for protocols in [vec![], vec![4], vec![4, 3], vec![3, 3]] {
        let authority = store(30_000);
        let fixture = fixture(&authority).await;
        fixture.stage(&authority).await.unwrap();
        for (index, protocol) in protocols.into_iter().enumerate() {
            let input = target_preparation::input(&authority, &fixture, index).await;
            authority
                .certify_topology_target_preparation(
                    &fixture.assignments,
                    &topology_preparation::processes(&authority),
                    &input,
                    protocol,
                )
                .await
                .unwrap();
        }
        let input = target_preparation::input(&authority, &fixture, 0).await;
        let before = authority.load_record().await.unwrap();
        assert!(matches!(
            commit(&authority, &fixture, &input).await,
            Err(TopologyError::Protocol(_))
        ));
        assert_eq!(authority.load_record().await.unwrap(), before);
    }
}

#[tokio::test]
async fn topology_commit_rejects_changed_image_and_assignment_without_append() {
    let authority = store(30_000);
    let (fixture, input) = prepared(&authority).await;
    let before = authority.load_record().await.unwrap();
    for case in 0..8 {
        let mut changed = target_preparation::input(&authority, &fixture, 0).await;
        match case {
            0 => changed.root.subscriptions.clear(),
            1 => changed.descriptor.target_pipeline.sha256 = digest(99),
            2 => changed.owned_vnodes.clear(),
            3 => changed.checkpoint.epoch += 1,
            4 => changed.process.process_term += 1,
            5 => {
                changed.target.entries.pop().unwrap();
            }
            6 => changed.operation.plan.sha256 = digest(99),
            _ => changed.restore_assignment.assignment_version += 1,
        }
        assert!(
            commit(&authority, &fixture, &changed).await.is_err(),
            "case {case}"
        );
        assert_eq!(authority.load_record().await.unwrap(), before);
    }
    let assignment = fixture.assignments.load().await.unwrap().unwrap();
    let next = assignment
        .next_for_participants(assignment.vnodes.clone(), assignment.participants.clone())
        .unwrap();
    fixture
        .assignments
        .save_if_version(&next, assignment.version)
        .await
        .unwrap();
    assert!(commit(&authority, &fixture, &input).await.is_err());
    assert_eq!(authority.load_record().await.unwrap(), before);
}

#[tokio::test]
async fn topology_commit_concurrent_identical_writers_publish_once() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(14));
    let (fixture, input) = prepared(&authority).await;
    let task_authority = Arc::clone(&authority);
    let task_fixture = Fixture {
        lease: fixture.lease.clone(),
        assignments: AssignmentSnapshotStore::new(authority.store.clone()),
        store: ObjectStoreCheckpointStore::new(authority.store.clone(), ""),
        operation: fixture.operation.clone(),
        descriptor: fixture.descriptor.clone(),
        index: fixture.index.clone(),
        manifests: fixture.manifests.clone(),
    };
    let pending = tokio::spawn(async move { commit(&task_authority, &task_fixture, &input).await });
    raw.entered.acquire().await.unwrap().forget();
    let winner_input = target_preparation::input(&authority, &fixture, 0).await;
    let winner = commit(&authority, &fixture, &winner_input).await.unwrap();
    raw.release.add_permits(1);
    assert_eq!(pending.await.unwrap().unwrap(), winner);
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 14);
}

#[tokio::test]
async fn topology_commit_lost_response_resolves_original_catalog_decision() {
    let (raw, authority) = ambiguous_once_at(30_000, lease_path(14));
    let (fixture, input) = prepared(&authority).await;
    let committed = commit(&authority, &fixture, &input).await.unwrap();
    assert_eq!(raw.put_count(&lease_path(14), "create"), 1);
    assert_eq!(
        commit(&authority, &fixture, &input).await.unwrap(),
        committed
    );
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 14);
}

#[tokio::test]
async fn topology_commit_cancelled_successful_write_reconstructs_without_target_checkpoint() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(14));
    let (fixture, input) = prepared(&authority).await;
    let process = input.process();
    let task_authority = Arc::clone(&authority);
    let proof = fixture.lease.proof();
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let pending = tokio::spawn(async move {
        task_authority
            .commit_topology_target(
                &proof,
                &task_assignments,
                &topology_preparation::processes(&task_authority),
                &input,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let recovery = reconstruct(&reopened, &fixture, process).await.unwrap();
    assert_eq!(
        recovery.operation().phase,
        TopologyAdmissionPhase::Committed
    );
    assert_eq!(recovery.checkpoint(), &fixture.index);
    assert_eq!(
        reopened
            .load_record()
            .await
            .unwrap()
            .unwrap()
            .outcome_head
            .unwrap()
            .epoch,
        1
    );
}

#[tokio::test(start_paused = true)]
async fn topology_commit_deadline_before_create_keeps_prepared_parent_authority() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(14));
    let (fixture, input) = prepared(&authority).await;
    let before = authority.load_record().await.unwrap();
    let task_authority = Arc::clone(&authority);
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let proof = fixture.lease.proof();
    let pending = tokio::spawn(async move {
        task_authority
            .commit_topology_target(
                &proof,
                &task_assignments,
                &topology_preparation::processes(&task_authority),
                &input,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(matches!(
        pending.await.unwrap(),
        Err(TopologyError::Contended)
    ));
    assert_eq!(authority.load_record().await.unwrap(), before);
}

#[tokio::test]
async fn topology_commit_leader_change_wins_before_create_and_aborts_only_precommit() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(14));
    let (fixture, input) = prepared(&authority).await;
    let task_authority = Arc::clone(&authority);
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let proof = fixture.lease.proof();
    let pending = tokio::spawn(async move {
        task_authority
            .commit_topology_target(
                &proof,
                &task_assignments,
                &topology_preparation::processes(&task_authority),
                &input,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    authority
        .begin_new_term(&fixture.lease.owner, 1)
        .await
        .unwrap();
    raw.release.add_permits(1);
    assert!(matches!(pending.await.unwrap(), Err(TopologyError::Fenced)));
    let aborted = authority
        .topology_operation_status(fixture.operation.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted { .. }
    ));
    assert!(aborted.commit.is_none());
    assert_eq!(
        authority
            .load()
            .await
            .unwrap()
            .unwrap()
            .catalog_manifest
            .as_ref(),
        Some(&fixture.descriptor.parent_manifest)
    );
}

#[tokio::test]
async fn topology_commit_leader_change_after_decision_preserves_target_and_rejects_abort() {
    let authority = store(30_000);
    let (fixture, input) = prepared(&authority).await;
    let committed = commit(&authority, &fixture, &input).await.unwrap();
    let LeaseOutcome::Acquired(replacement) = authority
        .begin_new_term(&fixture.lease.owner, 1)
        .await
        .unwrap()
    else {
        panic!("replacement leader");
    };
    assert_eq!(
        authority
            .topology_operation_status(committed.operation_id)
            .await
            .unwrap(),
        Some(committed.clone())
    );
    assert!(matches!(
        authority
            .abort_topology_plan(
                &replacement.proof(),
                committed.operation_id,
                &committed.plan
            )
            .await,
        Err(TopologyError::Conflict(_))
    ));
    assert!(commit(&authority, &fixture, &input).await.is_err());
    let recovered = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    assert_eq!(recovered.checkpoint(), input.checkpoint());
    assert_eq!(
        recovered.committed_leader.as_ref(),
        Some(&replacement.proof())
    );
    let head = authority.load_record().await.unwrap().unwrap();
    assert!(head.reject_topology_preparation().is_err());
    assert!(head.reject_pending_topology_commit().is_err());
    let mut newer = input.root().cut.checkpoint.clone();
    newer.epoch += 1;
    assert!(head.topology_cut_blocks_cleanup(&newer));
}

#[tokio::test]
async fn topology_commit_recovery_fault_preserves_target_and_rejects_parent_release() {
    let authority = store(30_000);
    let (fixture, input) = prepared(&authority).await;
    let committed = commit(&authority, &fixture, &input).await.unwrap();
    authority
        .record_recovery_fault(owner_recovery_fault_publisher(&fixture.lease.owner), 1)
        .await
        .unwrap();
    let head = authority.load_record().await.unwrap().unwrap();
    assert!(head.recovery_fault_slots.iter().any(|slot| slot.active));
    assert_eq!(head.topology_operations[0], committed);
    assert_eq!(
        head.lease.catalog_manifest.as_ref(),
        Some(&fixture.descriptor.target_manifest)
    );
    assert!(head.reject_pending_topology_commit().is_err());
    assert!(reconstruct(&authority, &fixture, input.process())
        .await
        .is_ok());
    assert!(authority
        .abort_topology_plan(
            &fixture.lease.proof(),
            committed.operation_id,
            &committed.plan
        )
        .await
        .is_err());
}

#[tokio::test]
async fn topology_commit_reconstruction_uses_current_boots_but_retains_historical_ownership() {
    let authority = store(30_000);
    let (fixture, input) = prepared(&authority).await;
    commit(&authority, &fixture, &input).await.unwrap();
    let process_store =
        crate::cluster::control::ProcessLeaseStore::new(authority.store.clone(), NodeId(2), 30_000);
    let prior = process_store.load().await.unwrap().unwrap();
    let observation = process_store.observe_rival(&prior).unwrap();
    tokio::time::sleep(Duration::from_millis(30_001)).await;
    let crate::cluster::control::ProcessLeaseOutcome::Acquired(replacement) = process_store
        .try_takeover(Uuid::from_u128(222), &observation, 30_001)
        .await
        .unwrap()
    else {
        panic!("full monotonic takeover");
    };
    let before = fixture.assignments.load().await.unwrap().unwrap();
    let mut participants = before.participants.clone();
    participants
        .iter_mut()
        .find(|participant| participant.node_id == 2)
        .unwrap()
        .boot_incarnation = replacement.owner;
    let assignment = before
        .next_for_participants(before.vnodes.clone(), participants)
        .unwrap();
    fixture
        .assignments
        .save_if_version(&assignment, before.version)
        .await
        .unwrap();
    let process = LocalProcessAuthorityIdentity {
        participant: crate::checkpoint::CheckpointParticipant {
            node_id: 2,
            boot_incarnation: replacement.owner,
        },
        process_term: replacement.term,
    };
    let recovered = reconstruct(&authority, &fixture, process).await.unwrap();
    assert_eq!(
        recovered.assignment().assignment_version,
        assignment.version
    );
    assert_eq!(
        recovered.plan().assignment.assignment_version,
        before.version
    );
    assert_eq!(recovered.owned_vnodes(), &[1]);
    assert_eq!(recovered.checkpoint(), &fixture.index);
    assert!(reconstruct(
        &authority,
        &fixture,
        LocalProcessAuthorityIdentity {
            participant: crate::checkpoint::CheckpointParticipant {
                node_id: 2,
                boot_incarnation: prior.owner
            },
            process_term: prior.term
        }
    )
    .await
    .is_err());
    let changed = assignment
        .next_for_participants(
            BTreeMap::from([(0, NodeId(2)), (1, NodeId(1))]),
            assignment.participants.clone(),
        )
        .unwrap();
    fixture
        .assignments
        .save_if_version(&changed, assignment.version)
        .await
        .unwrap();
    assert!(reconstruct(&authority, &fixture, process).await.is_err());
}

#[tokio::test]
async fn topology_commit_pruning_retains_decision_and_corrupt_anchor_fails_catalog_reads() {
    let authority = store(30_000);
    let (fixture, input) = prepared(&authority).await;
    let committed = commit(&authority, &fixture, &input).await.unwrap();
    let lease = authority.load().await.unwrap().unwrap();
    for now in 1..5 {
        authority
            .renew_exact(&lease.owner, lease.token, now)
            .await
            .unwrap();
    }
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert_eq!(
        authority
            .topology_operation_status(committed.operation_id)
            .await
            .unwrap(),
        Some(committed.clone())
    );
    assert!(reconstruct(&authority, &fixture, input.process())
        .await
        .is_ok());
    authority
        .store
        .delete(&lease_path(committed.commit.unwrap().authority_sequence))
        .await
        .unwrap();
    assert!(authority
        .topology_operation_status(fixture.operation.operation_id)
        .await
        .is_err());
    assert!(authority.topology_catalog_state().await.is_err());
}

#[tokio::test]
async fn topology_commit_malformed_decisions_catalog_rewrites_and_rollback_fail_closed() {
    let authority = store(30_000);
    let (fixture, input) = prepared(&authority).await;
    let before = authority.load_record().await.unwrap().unwrap();
    commit(&authority, &fixture, &input).await.unwrap();
    let record = authority.load_record().await.unwrap().unwrap();
    for case in 0..7 {
        let mut changed = record.clone();
        match case {
            0 => changed.lease.catalog_manifest = Some(fixture.descriptor.parent_manifest.clone()),
            1 => {
                changed.topology_operations[0].phase = TopologyAdmissionPhase::Aborted {
                    reason: crate::cluster::control::TopologyAbortReason::Requested,
                }
            }
            2 => changed.topology_operations[0].commit = None,
            3 => {
                changed.topology_operations[0]
                    .commit
                    .as_mut()
                    .unwrap()
                    .authority_sequence -= 1;
            }
            4 => {
                changed.topology_operations[0]
                    .commit
                    .as_mut()
                    .unwrap()
                    .manifest
                    .sha256 = digest(99);
            }
            5 => {
                changed.topology_operations[0]
                    .target_preparations
                    .pop()
                    .unwrap();
            }
            _ => changed.version = TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION,
        }
        assert!(changed.validate().is_err(), "case {case}");
    }
    let mut rewrite = before.clone();
    rewrite.lease.seq += 1;
    rewrite.lease.catalog_manifest = Some(fixture.descriptor.target_manifest.clone());
    assert!(before.validate_topology_successor(&rewrite).is_err());
    let mut rollback = record.clone();
    rollback.lease.seq += 1;
    rollback.topology_operations[0] = before.topology_operations[0].clone();
    rollback.lease.catalog_manifest = before.lease.catalog_manifest;
    assert!(record.validate_topology_successor(&rollback).is_err());
}
