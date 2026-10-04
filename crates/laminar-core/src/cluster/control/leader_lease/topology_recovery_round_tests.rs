//! Authority boundaries only; runtime installation/readiness belongs to DB integration tests.

use super::*;
use crate::cluster::control::{
    ProcessLeaseAuthority, RecoverPhase, RecoveryAnnouncement, RecoveryRound,
};

async fn recovery_round(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    input: &TopologyRestoreInput,
    generation: u64,
) -> RecoveryRound {
    authority
        .record_recovery_fault(
            owner_recovery_fault_publisher(&fixture.lease.owner),
            generation,
        )
        .await
        .unwrap();
    let faults = authority.recovery_fault_inventory().await.unwrap();
    let round = RecoveryRound::new(
        generation,
        input.current_leader().unwrap(),
        input.assignment().clone(),
        Vec::new(),
        faults.revision(),
        faults.faults().to_vec(),
    )
    .unwrap();
    let processes = topology_preparation::processes(authority);
    let binding = authority
        .recovery_topology_binding(&round, Some((&fixture.assignments, &processes)))
        .await
        .unwrap();
    round.bind_topology(binding).unwrap()
}

async fn certify_recovered(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    round: &RecoveryRound,
    process: LocalProcessAuthorityIdentity,
    runtime: u128,
    epoch: u64,
) -> Result<TopologyAdmissionStatus, TopologyError> {
    let input = reconstruct(authority, fixture, process).await?;
    authority
        .certify_topology_recovery_installation(
            &fixture.assignments,
            &topology_preparation::processes(authority),
            &input,
            Uuid::from_u128(runtime),
            round,
            epoch,
        )
        .await
}

fn terminal(round: &RecoveryRound, epoch: u64) -> RecoveryAnnouncement {
    RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::ReleaseCommitted { epoch },
    }
}

async fn publish(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    terminal: &RecoveryAnnouncement,
) -> Result<RecordRecoveryReleaseCommitResult, ClusterCheckpointAuthorityError> {
    let reference = authority.stage_recovery_release_terminal(terminal).await?;
    authority
        .record_recovery_release_commit_with_topology(
            &terminal.round.leader_proof,
            reference,
            Some((
                &fixture.assignments,
                &topology_preparation::processes(authority),
            )),
        )
        .await
}

#[tokio::test]
async fn topology_recovery_round_first_release_requires_every_replacement_runtime() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let round = recovery_round(&authority, &fixture, &input, 1).await;
    let released = terminal(&round, input.checkpoint().epoch);
    let original = authority.load_record().await.unwrap().unwrap();
    assert!(publish(&authority, &fixture, &released).await.is_err());
    assert_eq!(authority.load_record().await.unwrap().unwrap(), original);
    let first = certify_recovered(&authority, &fixture, &round, input.processes()[0], 100, 1)
        .await
        .unwrap();
    assert_eq!(
        first.activation.as_ref().unwrap().recovery_round,
        Some(round.id)
    );
    assert!(publish(&authority, &fixture, &released).await.is_err());
    for (index, process) in input.processes().iter().enumerate().skip(1) {
        certify_recovered(
            &authority,
            &fixture,
            &round,
            *process,
            100 + index as u128,
            1,
        )
        .await
        .unwrap();
    }
    assert!(matches!(
        publish(&authority, &fixture, &released).await.unwrap(),
        RecordRecoveryReleaseCommitResult::Created(_)
    ));
    let head = authority.load_record().await.unwrap().unwrap();
    assert_eq!(head.version, TOPOLOGY_RECOVERY_RECORD_VERSION);
    assert_eq!(
        head.topology_operations[0].phase,
        TopologyAdmissionPhase::Active
    );
    assert_eq!(
        head.topology_operations[0].commit,
        original.topology_operations[0].commit
    );
    assert_eq!(head.commit_head, original.commit_head);
    assert_eq!(
        head.topology_operations[0]
            .activation
            .as_ref()
            .unwrap()
            .release
            .as_ref()
            .unwrap()
            .authority_sequence,
        head.recovery_release_head.as_ref().unwrap().sequence
    );
    assert!(authority
        .authorize_recovery_release_with_topology(
            owner_recovery_fault_publisher(&fixture.lease.owner),
            &released,
            Some((
                &fixture.assignments,
                &topology_preparation::processes(&authority)
            ))
        )
        .await
        .unwrap());
    let fresh = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    assert!(!authority
        .authorize_topology_release(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &fresh,
            Uuid::from_u128(100)
        )
        .await
        .unwrap());
    let snapshot = authority.recovery_admission_snapshot().await.unwrap();
    assert!(authority
        .recovery_admission_is_current(&snapshot, &round.leader_proof)
        .await
        .unwrap());
    let mut downgraded = head;
    downgraded.version = TOPOLOGY_INSTALLATION_RECORD_VERSION;
    assert!(downgraded.validate().is_err());
}

#[tokio::test]
async fn topology_recovery_round_active_root_keeps_original_release_and_fences_old_runtime() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let original = input.operation().clone();
    let round = recovery_round(&authority, &fixture, &input, 1).await;
    let released = terminal(&round, 1);
    for (index, process) in input.processes().iter().enumerate() {
        certify_recovered(
            &authority,
            &fixture,
            &round,
            *process,
            500 + index as u128,
            1,
        )
        .await
        .unwrap();
    }
    publish(&authority, &fixture, &released).await.unwrap();
    assert_eq!(
        authority
            .topology_operation_status(original.operation_id)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    for runtime in [1, 500] {
        assert!(!authority
            .authorize_topology_release(
                &fixture.assignments,
                &topology_preparation::processes(&authority),
                &input,
                Uuid::from_u128(runtime)
            )
            .await
            .unwrap());
    }
    assert!(authority
        .authorize_recovery_release_with_topology(
            owner_recovery_fault_publisher(&fixture.lease.owner),
            &released,
            Some((
                &fixture.assignments,
                &topology_preparation::processes(&authority)
            ))
        )
        .await
        .unwrap());
    // A lost-response retry remains idempotent after ordinary target progress advances.
    target_checkpoint(&authority, &fixture, &input, 2).await;
    assert!(matches!(
        publish(&authority, &fixture, &released).await.unwrap(),
        RecordRecoveryReleaseCommitResult::Unchanged(_)
    ));
    assert!(authority
        .authorize_recovery_release_with_topology(
            owner_recovery_fault_publisher(&fixture.lease.owner),
            &released,
            Some((
                &fixture.assignments,
                &topology_preparation::processes(&authority)
            ))
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn topology_recovery_round_supersedes_unreleased_installation_in_same_leader_term() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    certify(&authority, &fixture, &input, 1).await.unwrap();
    let first = recovery_round(&authority, &fixture, &input, 1).await;
    certify_recovered(&authority, &fixture, &first, input.processes()[0], 100, 1)
        .await
        .unwrap();
    let second = recovery_round(&authority, &fixture, &input, 2).await;
    assert!(
        certify_recovered(&authority, &fixture, &first, input.processes()[1], 101, 1)
            .await
            .is_err()
    );
    for (index, process) in input.processes().iter().enumerate() {
        certify_recovered(
            &authority,
            &fixture,
            &second,
            *process,
            200 + index as u128,
            1,
        )
        .await
        .unwrap();
    }
    assert!(matches!(
        publish(&authority, &fixture, &terminal(&first, 1))
            .await
            .unwrap(),
        RecordRecoveryReleaseCommitResult::FaultsChanged
    ));
    publish(&authority, &fixture, &terminal(&second, 1))
        .await
        .unwrap();
    let status = authority
        .topology_operation_status(input.operation().operation_id)
        .await
        .unwrap()
        .unwrap();
    let activation = status.activation.unwrap();
    assert_eq!(activation.recovery_round, Some(second.id));
    assert!(activation
        .installations
        .iter()
        .all(|receipt| receipt.runtime_id.as_u128() >= 200));
}

#[tokio::test]
async fn topology_recovery_round_target_checkpoint_rejects_parent_cut_and_unbound_release() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    target_checkpoint(&authority, &fixture, &input, 2).await;
    let round = recovery_round(&authority, &fixture, &input, 1).await;
    let before = authority.load_record().await.unwrap();
    assert!(publish(&authority, &fixture, &terminal(&round, 1))
        .await
        .is_err());
    let unbound = round.clone().bind_topology(None).unwrap();
    assert!(publish(&authority, &fixture, &terminal(&unbound, 2))
        .await
        .is_err());
    assert_eq!(authority.load_record().await.unwrap(), before);
    publish(&authority, &fixture, &terminal(&round, 2))
        .await
        .unwrap();
    assert_eq!(
        select(&authority, &fixture, input.process())
            .await
            .unwrap()
            .checkpoint()
            .epoch,
        2
    );
}

#[tokio::test]
async fn topology_recovery_round_never_infers_assignment_or_process_namespaces() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let round = recovery_round(&authority, &fixture, &input, 1).await;
    let other_assignments =
        AssignmentSnapshotStore::new(Arc::new(object_store::memory::InMemory::new()));
    other_assignments
        .save_if_absent(&fixture.assignments.load().await.unwrap().unwrap())
        .await
        .unwrap();
    let other_processes = ProcessLeaseAuthority::new(
        Arc::new(object_store::memory::InMemory::new()),
        Duration::from_secs(30),
    )
    .unwrap();
    for process in input.processes() {
        assert!(matches!(
            other_processes
                .store_for(NodeId(process.participant.node_id))
                .try_acquire(process.participant.boot_incarnation, 0)
                .await
                .unwrap(),
            crate::cluster::control::ProcessLeaseOutcome::Acquired(_)
        ));
    }
    // Distinct configured stores can prove the same exact frozen identities. Absent/wrong
    // configured stores fail rather than falling back to the checkpoint authority's own storage.
    authority
        .audit_recovery_topology(
            &round,
            Some(1),
            Some((&other_assignments, &other_processes)),
        )
        .await
        .unwrap();
    assert!(authority
        .audit_recovery_topology(&round, Some(1), None)
        .await
        .is_err());
    let empty = AssignmentSnapshotStore::new(Arc::new(object_store::memory::InMemory::new()));
    assert!(authority
        .audit_recovery_topology(&round, Some(1), Some((&empty, &other_processes)))
        .await
        .is_err());
    let empty_processes = ProcessLeaseAuthority::new(
        Arc::new(object_store::memory::InMemory::new()),
        Duration::from_secs(30),
    )
    .unwrap();
    assert!(authority
        .audit_recovery_topology(
            &round,
            Some(1),
            Some((&other_assignments, &empty_processes))
        )
        .await
        .is_err());
}

#[tokio::test]
async fn topology_recovery_round_malformed_binding_cannot_stage_a_terminal() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let round = recovery_round(&authority, &fixture, &input, 1).await;
    let before = authority.load_record().await.unwrap();
    for case in 0..4 {
        let mut value = serde_json::to_value(terminal(&round, 1)).unwrap();
        match case {
            0 => value["round"]["topology"]["protocol_version"] = serde_json::json!(5),
            1 => value["round"]["topology"]["processes"][0]["process_term"] = serde_json::json!(0),
            2 => value["round"]["topology"]["processes"] = serde_json::json!([]),
            3 => value["round"]["topology"]["commit"]["authority_sequence"] = serde_json::json!(0),
            _ => unreachable!(),
        }
        let changed: RecoveryAnnouncement = serde_json::from_value(value).unwrap();
        assert!(authority
            .stage_recovery_release_terminal(&changed)
            .await
            .is_err());
    }
    assert_eq!(authority.load_record().await.unwrap(), before);
}

#[tokio::test]
async fn topology_recovery_round_preserves_exact_handoff_pin_until_new_target_checkpoint() {
    use crate::cluster::control::{ProcessLeaseFence, ProcessLeaseOutcome, ProcessLeaseStore};

    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let original = input.operation().clone();
    let (outcome, mut index) = target_checkpoint(&authority, &fixture, &input, 2).await;
    let checkpoint = outcome.committed_checkpoint.unwrap();
    let processes = ProcessLeaseStore::new(authority.store.clone(), NodeId(2), 30_000);
    let prior = processes.load().await.unwrap().unwrap();
    let observed = processes.observe_rival(&prior).unwrap();
    tokio::time::sleep(Duration::from_millis(30_001)).await;
    let ProcessLeaseOutcome::Acquired(replacement) = processes
        .try_takeover(Uuid::from_u128(222), &observed, 30_001)
        .await
        .unwrap()
    else {
        panic!("replacement process");
    };
    let before = fixture.assignments.load().await.unwrap().unwrap();
    let process_fence = ProcessLeaseFence::new(prior, replacement.clone()).unwrap();
    // A failed owner must be replaced in its original slot. Even with a valid takeover,
    // admission must not strand the committed topology on a reduced or redistributed map.
    for remove_owner in [true, false] {
        let mut owners = before.vnodes.clone();
        for owner in owners.values_mut() {
            *owner = if remove_owner || owner.0 == 2 {
                NodeId(1)
            } else {
                NodeId(2)
            };
        }
        let mut roster = before.participants.clone();
        if remove_owner {
            roster.retain(|participant| participant.node_id != 2);
        } else {
            roster[1].boot_incarnation = replacement.owner;
        }
        let unsupported = before.next_for_participants(owners, roster).unwrap();
        let proposal = fixture
            .assignments
            .stage_recovery_proposal(&unsupported)
            .await
            .unwrap();
        let proof = input.current_leader().unwrap();
        let decision = AssignmentRecoveryDecision::new(
            before.assignment_fence().unwrap(),
            unsupported.assignment_fence().unwrap(),
            proposal,
            vec![process_fence.clone()],
            checkpoint.clone(),
            proof.clone(),
        )
        .unwrap();
        let authority_before = authority.load_record().await.unwrap();
        assert!(
            authority
                .record_assignment_recovery_decision(&proof, decision)
                .await
                .is_err(),
            "committed topology must reject survivor rescaling before authority admission"
        );
        assert_eq!(authority.load_record().await.unwrap(), authority_before);
        assert_eq!(
            fixture.assignments.load().await.unwrap(),
            Some(before.clone())
        );
        assert!(fixture
            .assignments
            .load_version(before.version + 1)
            .await
            .unwrap()
            .is_none());
    }
    let mut roster = before.participants.clone();
    roster[1].boot_incarnation = replacement.owner;
    let assignment = before
        .next_for_participants(before.vnodes.clone(), roster)
        .unwrap();
    let target = assignment.assignment_fence().unwrap();
    let authority_before = authority.load_record().await.unwrap();
    authority
        .validate_topology_assignment_proposal(&target)
        .await
        .unwrap();
    assert_eq!(authority.load_record().await.unwrap(), authority_before);
    let proposal = fixture
        .assignments
        .stage_recovery_proposal(&assignment)
        .await
        .unwrap();
    let proof = input.current_leader().unwrap();
    let decision = AssignmentRecoveryDecision::new(
        before.assignment_fence().unwrap(),
        target.clone(),
        proposal,
        vec![process_fence],
        checkpoint.clone(),
        proof.clone(),
    )
    .unwrap();
    authority
        .record_assignment_recovery_decision(&proof, decision)
        .await
        .unwrap();
    authority
        .materialize_assignment_recovery(target.assignment_version)
        .await
        .unwrap();
    let input = reconstruct(&authority, &fixture, input.process())
        .await
        .unwrap();
    let round = recovery_round(&authority, &fixture, &input, 1).await;
    let process_authority = topology_preparation::processes(&authority);
    authority
        .audit_recovery_topology(
            &round,
            Some(2),
            Some((&fixture.assignments, &process_authority)),
        )
        .await
        .unwrap();

    // A pin protects restore state; it cannot authorize a different assignment or payload.
    let head = authority.load_record().await.unwrap().unwrap();
    for case in 0..2 {
        let mut changed = head.clone();
        let pin = changed.assignment_handoff_pin.as_mut().unwrap();
        if case == 0 {
            pin.target.assignment_version += 1;
        } else {
            pin.checkpoint.sha256 = "f".repeat(64);
        }
        assert!(authority
            .audit_recovery_topology_from(
                &changed,
                &round,
                Some(2),
                Some((&fixture.assignments, &process_authority)),
            )
            .await
            .is_err());
    }
    for (index, process) in input.processes().iter().enumerate() {
        certify_recovered(
            &authority,
            &fixture,
            &round,
            *process,
            800 + index as u128,
            2,
        )
        .await
        .unwrap();
    }
    publish(&authority, &fixture, &terminal(&round, 2))
        .await
        .unwrap();
    assert_eq!(
        authority
            .topology_operation_status(original.operation_id)
            .await
            .unwrap(),
        Some(original)
    );
    assert_eq!(
        authority
            .assignment_handoff_checkpoint(&target)
            .await
            .unwrap(),
        Some(checkpoint.clone())
    );

    let mut inventory = input.operation().cut.as_ref().unwrap().inventory.clone();
    inventory.attempt = crate::checkpoint::CheckpointAttempt::canonical(3);
    inventory.pipeline_identity = input.descriptor().target_pipeline.clone();
    inventory.assignment_fence = Some(target.clone());
    authority
        .begin_cluster_checkpoint_artifacts(&proof, inventory)
        .await
        .unwrap();
    index.epoch = 3;
    index.checkpoint_id = 3;
    index.assignment_fence = Some(target.clone());
    for source in index.source_offsets.values_mut() {
        source.source_assignment_version = std::num::NonZeroU64::new(target.assignment_version);
    }
    index.predecessor = Some(checkpoint);
    let reference = authority.create_committed_checkpoint(&index).await.unwrap();
    authority
        .record_cluster_outcome(
            &proof,
            3,
            3,
            target.clone(),
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap();
    assert_eq!(
        authority
            .assignment_handoff_checkpoint(&target)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn topology_recovery_round_replaces_boot_before_first_target_checkpoint() {
    use crate::cluster::control::{ProcessLeaseFence, ProcessLeaseOutcome, ProcessLeaseStore};

    let preparing = store(30_000);
    let (fixture, _) = prepared(&preparing).await;
    let current = fixture.assignments.load().await.unwrap().unwrap();
    let before = preparing.load_record().await.unwrap();
    assert!(preparing
        .validate_topology_assignment_proposal(&current.assignment_fence().unwrap())
        .await
        .is_err());
    assert_eq!(preparing.load_record().await.unwrap(), before);

    for partial_installation in [false, true] {
        let authority = store(30_000);
        let (fixture, input) = committed(&authority).await;
        let input = if partial_installation {
            certify(&authority, &fixture, &input, 88).await.unwrap();
            reconstruct(&authority, &fixture, input.process())
                .await
                .unwrap()
        } else {
            input
        };
        assert_eq!(
            input.operation().phase,
            if partial_installation {
                TopologyAdmissionPhase::Activating
            } else {
                TopologyAdmissionPhase::Committed
            }
        );
        let original_commit = input.operation().commit.clone();
        let current = fixture.assignments.load().await.unwrap().unwrap();
        let proof = input.current_leader().unwrap();
        let drain = current
            .next_draining(
                current.vnodes.clone(),
                current.participants.clone(),
                proof.clone(),
            )
            .unwrap();
        let authority_before = authority.load_record().await.unwrap();
        assert!(authority
            .publish_assignment_drain(&proof, &fixture.assignments, &drain)
            .await
            .is_err());
        assert_eq!(authority.load_record().await.unwrap(), authority_before);
        assert_eq!(fixture.assignments.load().await.unwrap(), Some(current));
        let checkpoint = input.root().cut.checkpoint.clone();
        let process_store = ProcessLeaseStore::new(authority.store.clone(), NodeId(2), 30_000);
        let prior = process_store.load().await.unwrap().unwrap();
        let observed = process_store.observe_rival(&prior).unwrap();
        tokio::time::sleep(Duration::from_millis(30_001)).await;
        let ProcessLeaseOutcome::Acquired(replacement) = process_store
            .try_takeover(Uuid::from_u128(222), &observed, 30_001)
            .await
            .unwrap()
        else {
            panic!("replacement process");
        };
        let before = fixture.assignments.load().await.unwrap().unwrap();
        let mut roster = before.participants.clone();
        roster[1].boot_incarnation = replacement.owner;
        let assignment = before
            .next_for_participants(before.vnodes.clone(), roster)
            .unwrap();
        let target = assignment.assignment_fence().unwrap();
        let proof = input.current_leader().unwrap();
        let authority_before = authority.load_record().await.unwrap();
        authority
            .validate_topology_assignment_proposal(&target)
            .await
            .unwrap();
        assert_eq!(authority.load_record().await.unwrap(), authority_before);
        let proposal = fixture
            .assignments
            .stage_recovery_proposal(&assignment)
            .await
            .unwrap();
        let decision = AssignmentRecoveryDecision::new(
            before.assignment_fence().unwrap(),
            target.clone(),
            proposal,
            vec![ProcessLeaseFence::new(prior.clone(), replacement).unwrap()],
            checkpoint.clone(),
            proof.clone(),
        )
        .unwrap();
        authority
            .record_assignment_recovery_decision(&proof, decision)
            .await
            .unwrap();
        authority
            .materialize_assignment_recovery(target.assignment_version)
            .await
            .unwrap();
        let head = authority.load_record().await.unwrap().unwrap();
        for invalid in 0..2 {
            let mut changed = head.clone();
            let pin = changed.assignment_handoff_pin.as_mut().unwrap();
            if invalid == 0 {
                pin.checkpoint.sha256 = "f".repeat(64);
            } else {
                pin.target.assignment_digest[0] ^= 1;
            }
            assert!(changed.validate().is_err());
        }
        let input = reconstruct(&authority, &fixture, input.process())
            .await
            .unwrap();
        assert_eq!(input.operation().commit, original_commit);
        assert_eq!(input.checkpoint(), &fixture.index);
        let selection = select(&authority, &fixture, input.process()).await.unwrap();
        assert_eq!(selection.cut(), TopologyRecoveryCut::MigrationRoot);
        assert_eq!(selection.checkpoint(), &fixture.index);
        assert_eq!(
            authority
                .assignment_handoff_checkpoint(&target)
                .await
                .unwrap(),
            Some(checkpoint.clone())
        );
        let round = recovery_round(&authority, &fixture, &input, 1).await;
        assert!(certify_recovered(
            &authority,
            &fixture,
            &round,
            LocalProcessAuthorityIdentity {
                participant: crate::checkpoint::CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: prior.owner
                },
                process_term: prior.term,
            },
            700,
            1
        )
        .await
        .is_err());
        assert!(publish(&authority, &fixture, &terminal(&round, 1))
            .await
            .is_err());
        for (index, process) in input.processes().iter().enumerate() {
            certify_recovered(
                &authority,
                &fixture,
                &round,
                *process,
                800 + index as u128,
                1,
            )
            .await
            .unwrap();
        }
        publish(&authority, &fixture, &terminal(&round, 1))
            .await
            .unwrap();
        let head = authority.load_record().await.unwrap().unwrap();
        assert_eq!(
            head.topology_operations[0].phase,
            TopologyAdmissionPhase::Active
        );
        assert_eq!(head.topology_operations[0].commit, original_commit);
        assert_eq!(
            authority
                .assignment_handoff_checkpoint(&target)
                .await
                .unwrap(),
            Some(checkpoint)
        );
        assert!(authority
            .authorize_recovery_release_with_topology(
                owner_recovery_fault_publisher(&fixture.lease.owner),
                &terminal(&round, 1),
                Some((
                    &fixture.assignments,
                    &topology_preparation::processes(&authority)
                )),
            )
            .await
            .unwrap());
    }
}
