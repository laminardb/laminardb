use super::*;
use crate::cluster::control::topology::{
    TopologyAbortReason, TopologyAdmissionPhase, TopologyAdmissionPlan, TopologyError,
    TopologyOperationId, TopologyVersion, MAX_TOPOLOGY_OPERATIONS, TOPOLOGY_PROTOCOL_VERSION,
};

fn operation(value: u128) -> TopologyOperationId {
    Uuid::from_u128(value).try_into().unwrap()
}

pub(super) async fn fixture(
    authority: &LeaderLeaseStore,
) -> (
    LeaderLease,
    AssignmentSnapshotStore,
    TopologyAdmissionPlan,
    CatalogManifest,
) {
    let (lease, parent_manifest, deployment) = topology::topology_adoption_fixture(authority).await;
    authority
        .adopt_legacy_topology(&lease.proof(), operation(40), &parent_manifest, &deployment)
        .await
        .unwrap();
    let assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let fence = assignment_fence(&lease.owner);
    let seed = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[lease.owner.node]),
            fence.participants.clone(),
        )
        .unwrap();
    assignments.save_if_absent(&seed).await.unwrap();
    let mut target = catalog("events");
    target.entries.extend(catalog("additional_events").entries);
    let (_, target_manifest) = target.encode_and_reference().unwrap();
    let plan = TopologyAdmissionPlan {
        protocol_version: TOPOLOGY_PROTOCOL_VERSION,
        operation_id: operation(41),
        expected_parent: TopologyVersion::LEGACY_BASELINE,
        parent_manifest,
        target_manifest,
        assignment: fence,
        compatibility: None,
    };
    (lease, assignments, plan, target)
}

async fn drain(assignments: &AssignmentSnapshotStore, lease: &LeaderLease) -> AssignmentSnapshot {
    let prior = assignments.load().await.unwrap().unwrap();
    prior
        .next_draining(
            prior.vnodes.clone(),
            prior.participants.clone(),
            lease.proof(),
        )
        .unwrap()
}

#[tokio::test]
async fn topology_driver_hint_tracks_progress_but_damaged_plan_never_grants_authority() {
    let authority = store(30_000);
    assert!(authority
        .latest_topology_operation_hint()
        .await
        .unwrap()
        .is_none());
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    assert_eq!(
        authority.latest_topology_operation_hint().await.unwrap(),
        Some((plan.operation_id, admitted.status_sequence))
    );
    let aborted = authority
        .abort_topology_plan(&lease.proof(), plan.operation_id, &admitted.plan)
        .await
        .unwrap();
    assert_eq!(
        authority.latest_topology_operation_hint().await.unwrap(),
        Some((plan.operation_id, aborted.status_sequence))
    );
    authority
        .store
        .delete(&OsPath::from(format!(
            "control/topology-plans/v1/{}.json",
            admitted.plan.sha256
        )))
        .await
        .unwrap();
    assert_eq!(
        authority.latest_topology_operation_hint().await.unwrap(),
        Some((plan.operation_id, aborted.status_sequence))
    );
    assert!(authority
        .topology_operation_status(plan.operation_id)
        .await
        .is_err());
}

#[tokio::test]
async fn admission_retry_and_abort_keep_the_original_payload_and_catalog() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let before = authority.load().await.unwrap().unwrap();
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    assert_eq!(admitted.phase, TopologyAdmissionPhase::Planned);
    assert_eq!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await
            .unwrap(),
        admitted
    );
    let mut changed = plan.clone();
    changed.expected_parent = TopologyVersion::new(2).unwrap();
    assert!(matches!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &changed, &target)
            .await,
        Err(TopologyError::Conflict(_))
    ));
    let mut reused_adoption = plan.clone();
    reused_adoption.operation_id = operation(40);
    assert!(matches!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &reused_adoption, &target)
            .await,
        Err(TopologyError::Conflict(_))
    ));
    assert_eq!(
        authority.load().await.unwrap().unwrap().catalog_manifest,
        before.catalog_manifest
    );
    let aborted = authority
        .abort_topology_plan(&lease.proof(), plan.operation_id, &admitted.plan)
        .await
        .unwrap();
    assert_eq!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::Requested
        }
    );
    assert_eq!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await
            .unwrap(),
        aborted
    );
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    assert_eq!(
        reopened
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(aborted)
    );
    assert_eq!(
        reopened.load().await.unwrap().unwrap().catalog_manifest,
        before.catalog_manifest
    );
}

#[tokio::test]
async fn concurrent_parent_submissions_have_one_reservation() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let mut other = plan.clone();
    other.operation_id = operation(42);
    let proof = lease.proof();
    let (first, second) = tokio::join!(
        authority.admit_topology_plan(&proof, &assignments, &plan, &target),
        authority.admit_topology_plan(&proof, &assignments, &other, &target),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let head = authority.load_record().await.unwrap().unwrap();
    assert_eq!(head.topology_operations.len(), 1);
    assert_eq!(
        head.topology_operations[0].admitted_sequence,
        head.lease.seq
    );
    assert_eq!(
        head.lease.catalog_manifest.as_ref(),
        Some(&plan.parent_manifest)
    );
}

#[tokio::test]
async fn checkpoint_winning_the_admission_race_keeps_the_old_graph_authoritative() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(4));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let task_authority = authority.clone();
    let task_proof = lease.proof();
    let task_plan = plan.clone();
    let submit = tokio::spawn(async move {
        task_authority
            .admit_topology_plan(&task_proof, &assignments, &task_plan, &target)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    authority
        .begin_cluster_checkpoint_artifacts(&lease.proof(), inventory.clone())
        .await
        .unwrap();
    raw.release.add_permits(1);
    assert!(matches!(
        submit.await.unwrap(),
        Err(TopologyError::Contended)
    ));
    let head = authority.load_record().await.unwrap().unwrap();
    assert!(head.topology_operations.is_empty());
    assert_eq!(head.active_checkpoint_artifacts, Some(inventory));
    assert_eq!(
        head.lease.catalog_manifest.as_ref(),
        Some(&plan.parent_manifest)
    );
}

#[tokio::test]
async fn topology_winning_the_admission_race_blocks_checkpoint_artifacts() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(4));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    let task_authority = authority.clone();
    let proof = lease.proof();
    let checkpoint = tokio::spawn(async move {
        task_authority
            .begin_cluster_checkpoint_artifacts(&proof, inventory)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    raw.release.add_permits(1);
    assert!(matches!(
        checkpoint.await.unwrap(),
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    assert!(authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .active_checkpoint_artifacts
        .is_none());
    authority
        .abort_topology_plan(&lease.proof(), plan.operation_id, &admitted.plan)
        .await
        .unwrap();
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    authority
        .begin_cluster_checkpoint_artifacts(&lease.proof(), inventory)
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelled_drain_before_snapshot_publication_remains_reserved_and_recoverable() {
    let (raw, authority) = blocking_once_at(
        30_000,
        OsPath::from("control/assignment-snapshots/v00000000000000000002.json"),
    );
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let proposal = drain(&assignments, &lease).await;
    let task_authority = authority.clone();
    let task_proof = lease.proof();
    let task_proposal = proposal.clone();
    let publish = tokio::spawn(async move {
        task_authority
            .publish_assignment_drain(&task_proof, &assignments, &task_proposal)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    let assignments = AssignmentSnapshotStore::new(authority.store.clone());
    assert_eq!(assignments.load().await.unwrap().unwrap().version, 1);
    assert!(matches!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await,
        Err(TopologyError::Conflict(_))
    ));
    publish.abort();
    assert!(publish.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    reopened
        .materialize_reserved_assignment_drain(&assignments)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(assignments.load().await.unwrap(), Some(proposal));
    assert!(reopened
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_drain_reservation
        .is_some());
}

#[tokio::test]
async fn topology_winning_the_assignment_race_prevents_snapshot_publication() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(4));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let proposal = drain(&assignments, &lease).await;
    let task_authority = authority.clone();
    let task_proof = lease.proof();
    let task_proposal = proposal.clone();
    let publish = tokio::spawn(async move {
        task_authority
            .publish_assignment_drain(&task_proof, &assignments, &task_proposal)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    let assignments = AssignmentSnapshotStore::new(authority.store.clone());
    authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    raw.release.add_permits(1);
    assert!(matches!(
        publish.await.unwrap(),
        Err(ClusterCheckpointAuthorityError::Decision(
            DecisionError::Conflict(_)
        ))
    ));
    assert_eq!(assignments.load().await.unwrap().unwrap().version, 1);
    assert!(authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_drain_reservation
        .is_none());
}

#[tokio::test]
async fn renewal_preserves_preparation_and_term_change_durably_aborts_it() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    authority
        .renew_exact(&lease.owner, lease.token, 1)
        .await
        .unwrap();
    assert_eq!(
        authority
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(admitted.clone())
    );
    let LeaseOutcome::Acquired(replacement) =
        authority.begin_new_term(&lease.owner, 2).await.unwrap()
    else {
        panic!("new term");
    };
    let aborted = authority
        .topology_operation_status(plan.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::LeaderChanged
        }
    );
    assert_eq!(aborted.status_sequence, replacement.seq);
    assert!(matches!(
        authority
            .abort_topology_plan(&lease.proof(), plan.operation_id, &admitted.plan)
            .await,
        Err(TopologyError::Fenced)
    ));
    assert_eq!(
        authority
            .admit_topology_plan(&replacement.proof(), &assignments, &plan, &target)
            .await
            .unwrap(),
        aborted
    );
}

#[tokio::test]
async fn replacement_process_retry_returns_original_disposition_without_readmitting() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let replacement_owner = owner(1, 2, 2);
    let current = authority.load().await.unwrap().unwrap();
    let mut observation = authority
        .observe_rival(&replacement_owner, &current)
        .unwrap();
    observation.started = Instant::now()
        .checked_sub(Duration::from_millis(30_001))
        .unwrap();
    let LeaseOutcome::Acquired(replacement) = authority
        .try_takeover(&replacement_owner, &observation, 30_001)
        .await
        .unwrap()
    else {
        panic!("replacement must win after observing an unchanged leader for its full TTL");
    };
    let result = authority
        .admit_topology_plan(&replacement.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    assert_eq!(result.admitted_by, admitted.admitted_by);
    assert_eq!(result.admitted_sequence, admitted.admitted_sequence);
    assert_eq!(result.status_sequence, replacement.seq);
    assert_eq!(
        result.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::LeaderChanged
        }
    );
    assert_eq!(authority.load().await.unwrap().unwrap(), replacement);
    let mut reused = plan.clone();
    reused.expected_parent = TopologyVersion::new(2).unwrap();
    assert!(matches!(
        authority
            .admit_topology_plan(&replacement.proof(), &assignments, &reused, &target)
            .await,
        Err(TopologyError::Conflict(_))
    ));
    let mut fresh = plan;
    fresh.operation_id = operation(42);
    assert!(matches!(
        authority
            .admit_topology_plan(&replacement.proof(), &assignments, &fresh, &target)
            .await,
        Err(TopologyError::Fenced)
    ));
    assert_eq!(authority.load().await.unwrap().unwrap(), replacement);
}

#[tokio::test]
async fn recovery_fault_atomically_aborts_preparation_without_clearing_the_fault() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    authority
        .record_recovery_fault(owner_recovery_fault_publisher(&lease.owner), 1)
        .await
        .unwrap();
    let status = authority
        .topology_operation_status(plan.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        status.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::Recovery
        }
    );
    let head = authority.load_record().await.unwrap().unwrap();
    assert_eq!(status.status_sequence, head.recovery_fault_revision);
    assert!(head.recovery_fault_slots[0].active);
    let mut retry = plan;
    retry.operation_id = operation(42);
    assert!(matches!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &retry, &target)
            .await,
        Err(TopologyError::Conflict(_))
    ));
}

#[tokio::test]
async fn lost_admission_response_and_cancelled_client_preserve_original_operation() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(4));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let task_authority = authority.clone();
    let proof = lease.proof();
    let task_plan = plan.clone();
    let submit = tokio::spawn(async move {
        task_authority
            .admit_topology_plan(&proof, &assignments, &task_plan, &target)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    submit.abort();
    assert!(submit.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let status = reopened
        .topology_operation_status(plan.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.admitted_sequence, 4);
    let assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let mut target = catalog("events");
    target.entries.extend(catalog("additional_events").entries);
    assert_eq!(
        reopened
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await
            .unwrap(),
        status
    );
    assert_eq!(reopened.load().await.unwrap().unwrap().seq, 4);
}

#[tokio::test]
async fn exact_assignment_decision_releases_reservation_after_full_materialization() {
    let authority = store(30_000);
    let (lease, assignments, _, _) = fixture(&authority).await;
    let proposal = drain(&assignments, &lease).await;
    authority
        .publish_assignment_drain(&lease.proof(), &assignments, &proposal)
        .await
        .unwrap();
    let transition = proposal.drain_transition.as_ref().unwrap();
    let wrong = assignment_drain_transition_at(&lease.owner, lease.proof(), 3);
    let wrong_decision = AssignmentDrainDecision::abort(&wrong, lease.proof()).unwrap();
    assert!(authority
        .record_assignment_drain_decision(&lease.proof(), wrong_decision)
        .await
        .is_err());
    let decision = AssignmentDrainDecision::abort(transition, lease.proof()).unwrap();
    authority
        .record_assignment_drain_decision(&lease.proof(), decision)
        .await
        .unwrap();
    assert!(authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_drain_reservation
        .is_none());
    let prior = assignments.load_version(1).await.unwrap().unwrap();
    let rollback = prior.next(prior.vnodes.clone()).unwrap();
    assignments
        .finalize_drain(&proposal, &rollback)
        .await
        .unwrap();
    assert!(!assignments.load().await.unwrap().unwrap().draining);
}

#[tokio::test]
async fn authority_pruning_retains_request_and_abort_anchors_and_fails_on_corruption() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let aborted = authority
        .abort_topology_plan(&lease.proof(), plan.operation_id, &admitted.plan)
        .await
        .unwrap();
    for timestamp in 1..80 {
        authority
            .renew_exact(&lease.owner, lease.token, timestamp)
            .await
            .unwrap();
    }
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert!(
        read_authority_record(authority.store.as_ref(), admitted.admitted_sequence)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        read_authority_record(authority.store.as_ref(), aborted.status_sequence)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        authority
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(aborted)
    );
    authority
        .store
        .delete(&OsPath::from(format!(
            "control/topology-plans/v1/{}.json",
            admitted.plan.sha256
        )))
        .await
        .unwrap();
    assert!(authority
        .topology_operation_status(plan.operation_id)
        .await
        .is_err());
    assert!(LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .is_err());
}

#[tokio::test]
async fn journal_exhaustion_fails_before_staging_another_request() {
    let authority = store(30_000);
    let (lease, assignments, mut plan, target) = fixture(&authority).await;
    for value in 0..MAX_TOPOLOGY_OPERATIONS {
        plan.operation_id = operation(100 + value as u128);
        let admitted = authority
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await
            .unwrap();
        authority
            .abort_topology_plan(&lease.proof(), plan.operation_id, &admitted.plan)
            .await
            .unwrap();
    }
    plan.operation_id = operation(1000);
    let before = authority.load().await.unwrap().unwrap();
    assert!(matches!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await,
        Err(TopologyError::Conflict(_))
    ));
    assert_eq!(authority.load().await.unwrap().unwrap(), before);
    assert!(authority
        .topology_operation_status(plan.operation_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn terminal_drain_decision_materializes_cancelled_intent_before_releasing_reservation() {
    let (raw, authority) = blocking_once_at(
        30_000,
        OsPath::from("control/assignment-snapshots/v00000000000000000002.json"),
    );
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let proposal = drain(&assignments, &lease).await;
    let transition = proposal.drain_transition.clone().unwrap();
    let task_authority = authority.clone();
    let proof = lease.proof();
    let task_proposal = proposal.clone();
    let task = tokio::spawn(async move {
        task_authority
            .publish_assignment_drain(&proof, &assignments, &task_proposal)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let assignments = AssignmentSnapshotStore::new(authority.store.clone());
    assert_eq!(assignments.load().await.unwrap().unwrap().version, 1);
    let abort = AssignmentDrainDecision::abort(&transition, lease.proof()).unwrap();
    authority
        .record_assignment_drain_decision(&lease.proof(), abort)
        .await
        .unwrap();
    assert_eq!(assignments.load().await.unwrap(), Some(proposal));
    assert!(authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_drain_reservation
        .is_none());
    // The terminal decision has not yet materialized the rollback. The draining snapshot itself
    // still prevents admission; a definitive decision alone never freezes the predecessor map.
    assert!(matches!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await,
        Err(TopologyError::Conflict(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn admission_deadline_before_create_leaves_no_authorized_operation() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(4));
    let (lease, assignments, plan, target) = fixture(&authority).await;
    let proof = lease.proof();
    let task_authority = authority.clone();
    let task_plan = plan.clone();
    let task = tokio::spawn(async move {
        task_authority
            .admit_topology_plan(&proof, &assignments, &task_plan, &target)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Contended)));
    assert_eq!(authority.load().await.unwrap().unwrap().seq, 3);
    assert!(authority
        .topology_operation_status(plan.operation_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn inventory_replacement_is_rejected_before_live_authority_changes() {
    let authority = store(30_000);
    let (lease, assignments, mut plan, mut target) = fixture(&authority).await;
    target.entries[0].catalog_generation += 1;
    plan.target_manifest = target.encode_and_reference().unwrap().1;
    let before = authority.load().await.unwrap().unwrap();
    assert!(matches!(
        authority
            .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
            .await,
        Err(TopologyError::Invalid(_))
    ));
    assert_eq!(authority.load().await.unwrap().unwrap(), before);
}

#[tokio::test]
async fn assignment_proposal_cleanup_respects_pending_and_terminal_retention() {
    let authority = store(30_000);
    let (lease, assignments, _, _) = fixture(&authority).await;
    let proposal = drain(&assignments, &lease).await;
    authority
        .publish_assignment_drain(&lease.proof(), &assignments, &proposal)
        .await
        .unwrap();
    let reservation = authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .assignment_drain_reservation
        .unwrap();
    let path = OsPath::from(format!(
        "control/assignment-drain-proposals/v1/v{:020}/{}.json",
        reservation.proposal.version, reservation.proposal.sha256
    ));
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert!(authority.store.get(&path).await.is_ok());
    let abort =
        AssignmentDrainDecision::abort(proposal.drain_transition.as_ref().unwrap(), lease.proof())
            .unwrap();
    authority
        .record_assignment_drain_decision(&lease.proof(), abort)
        .await
        .unwrap();
    let prior = assignments.load_version(1).await.unwrap().unwrap();
    assignments
        .finalize_drain(&proposal, &prior.next(prior.vnodes.clone()).unwrap())
        .await
        .unwrap();
    let committed = assignments.load().await.unwrap().unwrap();
    let successor = committed
        .next_draining(
            committed.vnodes.clone(),
            committed.participants.clone(),
            lease.proof(),
        )
        .unwrap();
    authority
        .publish_assignment_drain(&lease.proof(), &assignments, &successor)
        .await
        .unwrap();
    let abort =
        AssignmentDrainDecision::abort(successor.drain_transition.as_ref().unwrap(), lease.proof())
            .unwrap();
    authority
        .record_assignment_drain_decision(&lease.proof(), abort)
        .await
        .unwrap();
    assignments
        .finalize_drain(
            &successor,
            &committed.next(committed.vnodes.clone()).unwrap(),
        )
        .await
        .unwrap();
    assignments.prune_before(3).await.unwrap();
    authority
        .prune_assignment_drain_decisions_before(&lease.proof(), 3)
        .await
        .unwrap();
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    assert!(matches!(
        authority.store.get(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}
