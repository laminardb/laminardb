//! Authority fault injection uses the existing exact-cut fixture and conditional-write store.

use super::*;
use crate::cluster::control::{TopologyRestoreInput, TOPOLOGY_TARGET_PREPARATION_PROTOCOL_VERSION};

pub(super) async fn input(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    participant: usize,
) -> TopologyRestoreInput {
    let certificate = &fixture.operation.preparation.as_ref().unwrap().certificates[participant];
    authority
        .topology_restore_input(
            &fixture.assignments,
            &topology_preparation::processes(authority),
            fixture.operation.operation_id,
            crate::cluster::control::LocalProcessAuthorityIdentity {
                participant: certificate.participant,
                process_term: certificate.process_term,
            },
        )
        .await
        .unwrap()
}

async fn certify(
    authority: &LeaderLeaseStore,
    assignments: &AssignmentSnapshotStore,
    input: &TopologyRestoreInput,
) -> Result<TopologyAdmissionStatus, TopologyError> {
    authority
        .certify_topology_target_preparation(
            assignments,
            &topology_preparation::processes(authority),
            input,
            TOPOLOGY_TARGET_PREPARATION_PROTOCOL_VERSION,
        )
        .await
}

#[tokio::test]
async fn topology_target_preparation_requires_every_frozen_process_and_preserves_the_cut() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let staged = fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    let second = input(&authority, &fixture, 1).await;
    let lease = authority.load().await.unwrap().unwrap();
    let partial = certify(&authority, &fixture.assignments, &second)
        .await
        .unwrap();
    assert_eq!(partial.phase, TopologyAdmissionPhase::CutPrepared);
    assert_eq!(partial.target_preparations.len(), 1);
    assert!(!partial.target_preparation_complete());
    assert_eq!(
        partial.target_preparations[0].participant,
        second.process().participant
    );
    assert_eq!(
        partial.target_preparations[0].process_term,
        second.process().process_term
    );
    let fresh = input(&authority, &fixture, 0).await;
    assert_ne!(fresh, first);
    assert!(fresh.same_restore_requirements(&first));
    let complete = certify(&authority, &fixture.assignments, &first)
        .await
        .unwrap();
    assert!(complete.target_preparation_complete());
    assert_eq!(complete.target_preparations.len(), 2);
    assert_eq!(complete.cut, staged.cut);
    assert_eq!(complete.preparation, staged.preparation);
    assert_eq!(complete.migration_root, staged.migration_root);
    let head = authority.load_record().await.unwrap().unwrap();
    assert_eq!(head.version, TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION);
    assert_eq!(head.lease.catalog_manifest, lease.catalog_manifest);
    assert_eq!(head.lease.seq, lease.seq + 2);
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    assert_eq!(
        certify(&reopened, &fixture.assignments, &second)
            .await
            .unwrap(),
        complete
    );
    assert_eq!(
        certify(&reopened, &fixture.assignments, &first)
            .await
            .unwrap(),
        complete
    );
    assert_eq!(reopened.load().await.unwrap().unwrap().seq, head.lease.seq);
    assert!(matches!(reopened.topology_catalog_state().await.unwrap(),
        crate::cluster::control::TopologyCatalogState::Versioned { baseline, committed: None }
            if baseline.topology_version == TopologyVersion::LEGACY_BASELINE));
    assert_eq!(
        input(&reopened, &fixture, 0).await.checkpoint(),
        &fixture.index
    );
}

#[tokio::test]
async fn topology_target_preparation_concurrent_receipts_append_once_per_process() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(12));
    let fixture = fixture(&authority).await;
    fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    let second = input(&authority, &fixture, 1).await;
    let task_authority = Arc::clone(&authority);
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let pending =
        tokio::spawn(async move { certify(&task_authority, &task_assignments, &first).await });
    raw.entered.acquire().await.unwrap().forget();
    let partial = certify(&authority, &fixture.assignments, &second)
        .await
        .unwrap();
    assert!(!partial.target_preparation_complete());
    raw.release.add_permits(1);
    let complete = pending.await.unwrap().unwrap();
    assert!(complete.target_preparation_complete());
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 13);
    assert_eq!(
        complete
            .target_preparations
            .iter()
            .map(|receipt| receipt.authority_sequence)
            .collect::<Vec<_>>(),
        vec![13, 12]
    );
}

#[tokio::test]
async fn topology_target_preparation_rejects_divergent_inputs_and_old_protocol_without_append() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    fixture.stage(&authority).await.unwrap();
    let original = input(&authority, &fixture, 0).await;
    let before = authority.load_record().await.unwrap();
    for protocol in [0, 1, 2, 5] {
        assert!(matches!(
            authority
                .certify_topology_target_preparation(
                    &fixture.assignments,
                    &topology_preparation::processes(&authority),
                    &original,
                    protocol,
                )
                .await,
            Err(TopologyError::Protocol(_))
        ));
    }
    for case in 0..9 {
        let mut changed = input(&authority, &fixture, 0).await;
        match case {
            0 => changed.process.process_term += 1,
            1 => changed.process.participant.boot_incarnation = Uuid::from_u128(999),
            2 => changed.operation.plan.sha256 = digest(99),
            3 => changed.root.subscriptions.clear(),
            4 => changed.descriptor.target_pipeline.sha256 = digest(99),
            5 => changed.owned_vnodes.clear(),
            6 => changed.checkpoint.epoch += 1,
            7 => {
                assert!(changed.target.entries.pop().is_some());
            }
            _ => changed.plan.assignment.assignment_version += 1,
        }
        assert!(!changed.same_restore_requirements(&original));
        assert!(
            certify(&authority, &fixture.assignments, &changed)
                .await
                .is_err(),
            "case {case}"
        );
    }
    assert_eq!(authority.load_record().await.unwrap(), before);
}

#[tokio::test]
async fn topology_target_preparation_rechecks_all_processes_and_current_assignment_on_retry() {
    for change_assignment in [true, false] {
        let authority = store(30_000);
        let fixture = fixture(&authority).await;
        fixture.stage(&authority).await.unwrap();
        let first = input(&authority, &fixture, 0).await;
        let partial = certify(&authority, &fixture.assignments, &first)
            .await
            .unwrap();
        let before = authority.load().await.unwrap();
        if change_assignment {
            let prior = fixture.assignments.load().await.unwrap().unwrap();
            let next = prior
                .next_for_participants(prior.vnodes.clone(), prior.participants.clone())
                .unwrap();
            fixture
                .assignments
                .save_if_version(&next, prior.version)
                .await
                .unwrap();
        } else {
            let process = crate::cluster::control::ProcessLeaseStore::new(
                authority.store.clone(),
                NodeId(2),
                30_000,
            );
            let current = process.load().await.unwrap().unwrap();
            let observation = process.observe_rival(&current).unwrap();
            // Takeover requires a full candidate-local monotonic observation, not a future
            // diagnostic timestamp. There is no renewal manager in this authority fixture.
            tokio::time::sleep(Duration::from_millis(30_001)).await;
            assert!(matches!(
                process
                    .try_takeover(Uuid::from_u128(222), &observation, 30_001)
                    .await
                    .unwrap(),
                crate::cluster::control::ProcessLeaseOutcome::Acquired(_)
            ));
        }
        // Even an identical local retry cannot hide a different participant's lost process.
        assert!(certify(&authority, &fixture.assignments, &first)
            .await
            .is_err());
        assert_eq!(authority.load().await.unwrap(), before);
        assert_eq!(
            authority
                .topology_operation_status(first.operation().operation_id)
                .await
                .unwrap(),
            Some(partial)
        );
    }
}

#[tokio::test]
async fn topology_target_preparation_lost_response_reconciles_one_original_append() {
    let (raw, authority) = ambiguous_once_at(30_000, lease_path(12));
    let fixture = fixture(&authority).await;
    fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    let partial = certify(&authority, &fixture.assignments, &first)
        .await
        .unwrap();
    assert_eq!(partial.target_preparations.len(), 1);
    assert_eq!(raw.put_count(&lease_path(12), "create"), 1);
    assert_eq!(
        certify(&authority, &fixture.assignments, &first)
            .await
            .unwrap(),
        partial
    );
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 12);
}

#[tokio::test]
async fn topology_target_preparation_cancelled_successful_write_survives_reopen() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(12));
    let fixture = fixture(&authority).await;
    let staged = fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    let task_authority = Arc::clone(&authority);
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let pending =
        tokio::spawn(async move { certify(&task_authority, &task_assignments, &first).await });
    raw.entered.acquire().await.unwrap().forget();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let recovered = reopened
        .topology_operation_status(staged.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.target_preparations.len(), 1);
    assert!(!recovered.target_preparation_complete());
    assert_eq!(recovered.cut, staged.cut);
    let retained = input(&reopened, &fixture, 0).await;
    assert_eq!(
        certify(&reopened, &fixture.assignments, &retained)
            .await
            .unwrap(),
        recovered
    );
    assert_eq!(reopened.load().await.unwrap().unwrap().seq, 12);
}

#[tokio::test(start_paused = true)]
async fn topology_target_preparation_deadline_before_append_retains_the_unreleased_root() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(12));
    let fixture = fixture(&authority).await;
    let staged = fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    let task_authority = Arc::clone(&authority);
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let pending =
        tokio::spawn(async move { certify(&task_authority, &task_assignments, &first).await });
    raw.entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(matches!(
        pending.await.unwrap(),
        Err(TopologyError::Contended)
    ));
    assert_eq!(
        authority
            .topology_operation_status(staged.operation_id)
            .await
            .unwrap(),
        Some(staged)
    );
}

#[tokio::test]
async fn topology_target_preparation_leader_change_wins_the_blocked_append() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(12));
    let fixture = fixture(&authority).await;
    let staged = fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    let task_authority = Arc::clone(&authority);
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let pending =
        tokio::spawn(async move { certify(&task_authority, &task_assignments, &first).await });
    raw.entered.acquire().await.unwrap().forget();
    assert!(matches!(
        authority
            .begin_new_term(&fixture.lease.owner, 1)
            .await
            .unwrap(),
        LeaseOutcome::Acquired(_)
    ));
    raw.release.add_permits(1);
    assert!(matches!(pending.await.unwrap(), Err(TopologyError::Fenced)));
    let aborted = authority
        .topology_operation_status(staged.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted { .. }
    ));
    assert!(aborted.target_preparations.is_empty());
    assert_eq!(aborted.migration_root, staged.migration_root);
    assert_eq!(aborted.cut, staged.cut);
}

#[tokio::test]
async fn topology_target_preparation_abort_retains_receipts_and_pruning_pins_every_anchor() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    certify(&authority, &fixture.assignments, &first)
        .await
        .unwrap();
    let second = input(&authority, &fixture, 1).await;
    let complete = certify(&authority, &fixture.assignments, &second)
        .await
        .unwrap();
    assert!(complete.target_preparation_complete());
    authority
        .begin_new_term(&fixture.lease.owner, 1)
        .await
        .unwrap();
    let lease = authority.load().await.unwrap().unwrap();
    for step in 0..8 {
        authority
            .renew_exact(&lease.owner, lease.token, step)
            .await
            .unwrap();
    }
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    let aborted = authority
        .topology_operation_status(first.operation().operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted { .. }
    ));
    assert!(!aborted.target_preparation_complete());
    assert_eq!(aborted.target_preparations, complete.target_preparations);
    assert!(certify(&authority, &fixture.assignments, &first)
        .await
        .is_err());
    let sequence = complete.target_preparations[0].authority_sequence;
    authority.store.delete(&lease_path(sequence)).await.unwrap();
    assert!(authority
        .topology_operation_status(first.operation().operation_id)
        .await
        .is_err());
}

#[tokio::test]
async fn topology_target_preparation_malformed_receipts_and_rewrites_fail_closed() {
    let authority = store(30_000);
    let fixture = fixture(&authority).await;
    let staged = fixture.stage(&authority).await.unwrap();
    let first = input(&authority, &fixture, 0).await;
    let partial = certify(&authority, &fixture.assignments, &first)
        .await
        .unwrap();
    let head = authority.load_record().await.unwrap().unwrap();
    let mut legacy = head.clone();
    legacy.version = TOPOLOGY_SOURCE_ROOT_RECORD_VERSION;
    assert!(legacy.validate().is_err());
    for case in 0..9 {
        let mut changed = partial.clone();
        match case {
            0 => changed
                .target_preparations
                .push(changed.target_preparations[0].clone()),
            1 => changed.target_preparations[0].process_term += 1,
            2 => changed.target_preparations[0].participant.boot_incarnation = Uuid::from_u128(999),
            3 => changed.target_preparations[0].protocol_version = 2,
            4 => changed.target_preparations[0].authority_sequence = staged.status_sequence,
            5 => changed.target_preparations[0].authority_sequence += 1,
            6 => changed.migration_root = None,
            7 => changed.phase = TopologyAdmissionPhase::Quiescing,
            _ => {
                changed.target_preparations =
                    vec![changed.target_preparations[0].clone(); MAX_CHECKPOINT_PARTICIPANTS + 1];
            }
        }
        assert!(changed.validate(head.lease.seq).is_err(), "case {case}");
    }
    for case in 0..5 {
        let mut changed = partial.clone();
        changed.status_sequence += 1;
        match case {
            0 => changed.target_preparations.clear(),
            1 => changed.target_preparations[0].authority_sequence += 1,
            2 => changed.target_preparations[0].process_term += 1,
            3 => changed
                .target_preparations
                .push(changed.target_preparations[0].clone()),
            _ => changed.migration_root = None,
        }
        assert!(
            partial
                .validate_successor(&changed, changed.status_sequence)
                .is_err(),
            "rewrite {case}"
        );
    }
    // Historical statuses omit the new optional field and retain their canonical bytes.
    let encoded = serde_json::to_vec(&staged).unwrap();
    assert!(!String::from_utf8_lossy(&encoded).contains("target_preparations"));
    assert_eq!(
        serde_json::from_slice::<TopologyAdmissionStatus>(&encoded).unwrap(),
        staged
    );
    let LeaseOutcome::Acquired(renewed) = authority
        .renew_exact(&fixture.lease.owner, fixture.lease.token, 1)
        .await
        .unwrap()
    else {
        panic!("fixture leader must renew");
    };
    let mut forged = partial;
    forged.status_sequence = renewed.seq;
    forged.target_preparations[0].authority_sequence = renewed.seq;
    forged.validate(renewed.seq).unwrap();
    // A structurally valid receipt cannot borrow a later renewal as its original append.
    assert!(authority
        .audit_topology_target_preparations(&forged)
        .await
        .is_err());
}
