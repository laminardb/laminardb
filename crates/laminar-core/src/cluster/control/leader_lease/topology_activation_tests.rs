//! Current installation/Release fault boundaries, using the existing exact-root authority fixture.

use super::*;
use crate::cluster::control::TOPOLOGY_INSTALLATION_PROTOCOL_VERSION;

#[path = "topology_recovery_tests.rs"]
mod recovery;

async fn committed(authority: &LeaderLeaseStore) -> (Fixture, TopologyRestoreInput) {
    let (fixture, input) = prepared(authority).await;
    commit(authority, &fixture, &input).await.unwrap();
    let input = reconstruct(authority, &fixture, input.process())
        .await
        .unwrap();
    (fixture, input)
}

async fn certify(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    input: &TopologyRestoreInput,
    runtime: u128,
) -> Result<TopologyAdmissionStatus, TopologyError> {
    authority
        .certify_topology_installation(
            &fixture.assignments,
            &topology_preparation::processes(authority),
            input,
            Uuid::from_u128(runtime),
            TOPOLOGY_INSTALLATION_PROTOCOL_VERSION,
        )
        .await
}

async fn install_all(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    input: &TopologyRestoreInput,
) -> TopologyRestoreInput {
    for (index, process) in input.processes().iter().enumerate() {
        let input = reconstruct(authority, fixture, *process).await.unwrap();
        certify(authority, fixture, &input, (index + 1) as u128)
            .await
            .unwrap();
    }
    reconstruct(authority, fixture, input.process())
        .await
        .unwrap()
}

async fn release(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    input: &TopologyRestoreInput,
) -> Result<TopologyAdmissionStatus, TopologyError> {
    authority
        .release_topology_target(
            &input.current_leader().unwrap(),
            &fixture.assignments,
            &topology_preparation::processes(authority),
            input,
        )
        .await
}

#[tokio::test]
async fn topology_activation_requires_complete_current_roster_then_releases_exact_target() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    assert!(release(&authority, &fixture, &input).await.is_err());
    let first = certify(&authority, &fixture, &input, 1).await.unwrap();
    assert_eq!(first.phase, TopologyAdmissionPhase::Activating);
    assert!(!first.activation.as_ref().unwrap().installation_complete());
    let before = authority.load_record().await.unwrap();
    assert!(release(&authority, &fixture, &input).await.is_err());
    assert_eq!(authority.load_record().await.unwrap(), before);
    let input = install_all(&authority, &fixture, &input).await;
    let head = authority.load_record().await.unwrap().unwrap();
    let active = release(&authority, &fixture, &input).await.unwrap();
    assert_eq!(active.phase, TopologyAdmissionPhase::Active);
    assert!(!active.blocks_admission());
    assert_eq!(active.commit, input.operation().commit);
    assert_eq!(active.migration_root, input.operation().migration_root);
    assert_eq!(
        active
            .activation
            .as_ref()
            .unwrap()
            .release
            .as_ref()
            .unwrap()
            .authority_sequence,
        head.lease.seq + 1
    );
    let after = authority.load_record().await.unwrap().unwrap();
    assert_eq!(after.version, TOPOLOGY_INSTALLATION_RECORD_VERSION);
    let mut later_checkpoint = input.root().cut.checkpoint.clone();
    later_checkpoint.epoch += 1;
    assert!(!after.topology_cut_blocks_cleanup(&later_checkpoint));
    assert_eq!(
        LeaderLeaseStore::cleanup_stop_before(&after),
        Some(input.root().cut.checkpoint.clone())
    );
    assert_eq!(after.commit_head, head.commit_head);
    assert_eq!(after.outcome_head, head.outcome_head);
    assert_eq!(release(&authority, &fixture, &input).await.unwrap(), active);
    assert_eq!(
        authority.load_record().await.unwrap().unwrap().lease.seq,
        after.lease.seq
    );
    assert_eq!(
        authority
            .topology_operation_status(active.operation_id)
            .await
            .unwrap(),
        Some(active)
    );
    assert!(authority
        .authorize_topology_release(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &input,
            Uuid::from_u128(1)
        )
        .await
        .unwrap());
    assert!(!authority
        .authorize_topology_release(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &input,
            Uuid::from_u128(9)
        )
        .await
        .unwrap());
    // Release cannot make the historical parent a target recovery checkpoint.
    let admission = authority.recovery_admission_snapshot().await.unwrap();
    assert_eq!(
        admission.topology_commit(),
        input.operation().commit.as_ref()
    );
    assert!(!authority
        .recovery_admission_is_current(&admission, &fixture.lease.proof())
        .await
        .unwrap());
}

#[tokio::test]
async fn topology_activation_rejects_old_protocol_runtime_replacement_and_changed_input() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    for (runtime, protocol) in [(0, 5), (1, 4), (1, 99)] {
        assert!(authority
            .certify_topology_installation(
                &fixture.assignments,
                &topology_preparation::processes(&authority),
                &input,
                Uuid::from_u128(runtime),
                protocol
            )
            .await
            .is_err());
    }
    certify(&authority, &fixture, &input, 1).await.unwrap();
    let before = authority.load_record().await.unwrap();
    assert!(certify(&authority, &fixture, &input, 2).await.is_err());
    let mut changed = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    changed.owned_vnodes.clear();
    assert!(certify(&authority, &fixture, &changed, 1).await.is_err());
    assert_eq!(authority.load_record().await.unwrap(), before);
}

#[tokio::test]
async fn topology_activation_replacement_leader_collects_every_runtime_again() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let input = install_all(&authority, &fixture, &input).await;
    let old_round = input.operation().activation.clone().unwrap();
    let LeaseOutcome::Acquired(_) = authority
        .begin_new_term(&fixture.lease.owner, 1)
        .await
        .unwrap()
    else {
        panic!("new term");
    };
    assert!(release(&authority, &fixture, &input).await.is_err());
    let fresh = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    assert!(fresh.same_installed_generation(&input));
    let next = certify(&authority, &fixture, &fresh, 10).await.unwrap();
    let round = next.activation.as_ref().unwrap();
    assert_ne!(round.leader, old_round.leader);
    assert!(round.authority_sequence > old_round.authority_sequence);
    assert_eq!(round.installations.len(), 1);
    assert!(release(&authority, &fixture, &fresh).await.is_err());
    let peer = reconstruct(&authority, &fixture, fresh.processes()[1])
        .await
        .unwrap();
    certify(&authority, &fixture, &peer, 20).await.unwrap();
    let fresh = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    assert_eq!(
        release(&authority, &fixture, &fresh).await.unwrap().phase,
        TopologyAdmissionPhase::Active
    );
}

#[tokio::test]
async fn topology_activation_published_release_survives_harmless_leader_change() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let input = install_all(&authority, &fixture, &input).await;
    let active = release(&authority, &fixture, &input).await.unwrap();
    let LeaseOutcome::Acquired(_) = authority
        .begin_new_term(&fixture.lease.owner, 1)
        .await
        .unwrap()
    else {
        panic!("new term");
    };
    let fresh = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    assert_eq!(release(&authority, &fixture, &fresh).await.unwrap(), active);
    assert!(authority
        .authorize_topology_release(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &fresh,
            Uuid::from_u128(1)
        )
        .await
        .unwrap());
    assert_eq!(
        authority
            .topology_operation_status(active.operation_id)
            .await
            .unwrap(),
        Some(active)
    );
}

#[tokio::test]
async fn topology_activation_lost_installation_response_preserves_original_runtime_receipt() {
    let (raw, authority) = ambiguous_once_at(30_000, lease_path(15));
    let (fixture, input) = committed(&authority).await;
    let first = certify(&authority, &fixture, &input, 1).await.unwrap();
    assert_eq!(raw.put_count(&lease_path(15), "create"), 1);
    assert_eq!(
        certify(&authority, &fixture, &input, 1).await.unwrap(),
        first
    );
}

#[tokio::test]
async fn topology_activation_release_allows_only_target_checkpoint_during_assignment_refresh() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let input = install_all(&authority, &fixture, &input).await;
    release(&authority, &fixture, &input).await.unwrap();
    let input = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    let mut inventory = input.operation().cut.as_ref().unwrap().inventory.clone();
    inventory.attempt =
        crate::checkpoint::CheckpointAttempt::canonical(inventory.attempt.epoch + 1);
    let before = authority.load_record().await.unwrap();
    assert!(authority
        .begin_cluster_checkpoint_artifacts(&input.current_leader().unwrap(), inventory.clone())
        .await
        .is_err());
    assert_eq!(authority.load_record().await.unwrap(), before);
    inventory.pipeline_identity = input.descriptor().target_pipeline.clone();
    authority
        .begin_cluster_checkpoint_artifacts(&input.current_leader().unwrap(), inventory)
        .await
        .unwrap();
    assert!(authority
        .authorize_topology_release(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &input,
            Uuid::from_u128(1)
        )
        .await
        .unwrap());
    // Existing generic recovery cannot relabel the parent root even with target work in flight.
    let admission = authority.recovery_admission_snapshot().await.unwrap();
    assert_eq!(
        admission.topology_commit(),
        input.operation().commit.as_ref()
    );
    assert!(!authority
        .recovery_admission_is_current(&admission, &fixture.lease.proof())
        .await
        .unwrap());
}

#[tokio::test]
async fn topology_activation_rejects_owner_map_changes_before_drain_reservation() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let input = install_all(&authority, &fixture, &input).await;
    release(&authority, &fixture, &input).await.unwrap();
    let current = fixture.assignments.load().await.unwrap().unwrap();
    let proof = input.current_leader().unwrap();
    for remove_owner in [true, false] {
        let mut owners = current.vnodes.clone();
        for owner in owners.values_mut() {
            *owner = if remove_owner || owner.0 == 2 {
                NodeId(1)
            } else {
                NodeId(2)
            };
        }
        let mut roster = current.participants.clone();
        if remove_owner {
            roster.retain(|participant| participant.node_id != 2);
        }
        let proposal = current
            .next_draining(owners, roster, proof.clone())
            .unwrap();
        let before = authority.load_record().await.unwrap();
        assert!(authority
            .validate_topology_assignment_proposal(
                &proposal.drain_transition.as_ref().unwrap().target,
            )
            .await
            .is_err());
        assert!(authority
            .publish_assignment_drain(&proof, &fixture.assignments, &proposal)
            .await
            .is_err());
        assert_eq!(authority.load_record().await.unwrap(), before);
        assert_eq!(
            fixture.assignments.load().await.unwrap(),
            Some(current.clone())
        );
        assert!(fixture
            .assignments
            .load_version(current.version + 1)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn topology_activation_cancelled_successful_release_is_authoritative() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(17));
    let (fixture, input) = committed(&authority).await;
    let input = install_all(&authority, &fixture, &input).await;
    let process = input.process();
    let task_authority = Arc::clone(&authority);
    let assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let proof = input.current_leader().unwrap();
    let pending = tokio::spawn(async move {
        task_authority
            .release_topology_target(
                &proof,
                &assignments,
                &topology_preparation::processes(&task_authority),
                &input,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let input = reconstruct(&reopened, &fixture, process).await.unwrap();
    assert_eq!(input.operation().phase, TopologyAdmissionPhase::Active);
    assert_eq!(
        input
            .operation()
            .activation
            .as_ref()
            .unwrap()
            .release
            .as_ref()
            .unwrap()
            .authority_sequence,
        17
    );
    assert_eq!(
        release(&reopened, &fixture, &input).await.unwrap(),
        *input.operation()
    );
}

#[tokio::test]
async fn topology_activation_pruning_retains_installation_and_release_anchors() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let input = install_all(&authority, &fixture, &input).await;
    let active = release(&authority, &fixture, &input).await.unwrap();
    for now in 1..5 {
        authority
            .renew_exact(&fixture.lease.owner, fixture.lease.token, now)
            .await
            .unwrap();
    }
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert_eq!(
        authority
            .topology_operation_status(active.operation_id)
            .await
            .unwrap(),
        Some(active.clone())
    );
    authority
        .store
        .delete(&lease_path(
            active.activation.unwrap().installations[0].authority_sequence,
        ))
        .await
        .unwrap();
    assert!(authority.topology_catalog_state().await.is_err());
    assert!(authority
        .topology_operation_status(input.operation().operation_id)
        .await
        .is_err());
}

#[tokio::test]
async fn topology_activation_malformed_rosters_and_release_rewrites_fail_closed() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let input = install_all(&authority, &fixture, &input).await;
    release(&authority, &fixture, &input).await.unwrap();
    let head = authority.load_record().await.unwrap().unwrap();
    for case in 0..10 {
        let mut changed = head.clone();
        let operation = &mut changed.topology_operations[0];
        match case {
            0 => {
                operation.activation.as_mut().unwrap().installations.pop();
            }
            1 => operation.activation.as_mut().unwrap().installations[0].runtime_id = Uuid::nil(),
            2 => operation.activation.as_mut().unwrap().installations[0].protocol_version = 4,
            3 => operation.activation.as_mut().unwrap().processes[0].process_term = 0,
            4 => {
                operation
                    .activation
                    .as_mut()
                    .unwrap()
                    .release
                    .as_mut()
                    .unwrap()
                    .authority_sequence = head.lease.seq + 1;
            }
            5 => operation.activation = None,
            6 => operation.phase = TopologyAdmissionPhase::Committed,
            7 => changed.version = TOPOLOGY_COMMIT_RECORD_VERSION,
            8 => {
                operation.activation.as_mut().unwrap().installations[1].authority_sequence =
                    operation.activation.as_ref().unwrap().installations[0].authority_sequence;
            }
            _ => {
                changed.lease.seq += 1;
                operation.status_sequence = changed.lease.seq;
            }
        }
        assert!(changed.validate().is_err(), "case {case}");
    }
    let mut rewrite = head.clone();
    rewrite.lease.seq += 1;
    rewrite.topology_operations[0]
        .activation
        .as_mut()
        .unwrap()
        .installations[0]
        .runtime_id = Uuid::from_u128(99);
    rewrite.topology_operations[0].status_sequence = rewrite.lease.seq;
    assert!(head
        .validate_topology_admission_successor(&rewrite)
        .is_err());
}
