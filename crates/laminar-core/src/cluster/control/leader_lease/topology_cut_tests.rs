use super::*;
#[cfg(feature = "cluster")]
use crate::checkpoint::flags;
use crate::checkpoint::{CheckpointAttempt, CheckpointParticipant};
use crate::cluster::control::topology::{
    TopologyAbortReason, TopologyAdmissionPhase, TopologyAdmissionStatus, TopologyError,
};

async fn bound_fixture(
    authority: &LeaderLeaseStore,
    two_processes: bool,
) -> (
    LeaderLease,
    AssignmentSnapshotStore,
    TopologyAdmissionStatus,
    CheckpointArtifactInventory,
) {
    let (lease, assignments, mut plan, target) = topology_preparation::fixture(authority).await;
    if two_processes {
        let prior = assignments.load().await.unwrap().unwrap();
        let mut participants = prior.participants.clone();
        participants.push(CheckpointParticipant {
            node_id: 2,
            boot_incarnation: Uuid::from_u128(22),
        });
        let next = prior
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&[lease.owner.node, NodeId(2)]),
                participants,
            )
            .unwrap();
        assignments
            .save_if_version(&next, prior.version)
            .await
            .unwrap();
        plan.assignment = next.assignment_fence().unwrap();
    }
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    topology_preparation::prepare_all(authority, &assignments, &plan, &admitted).await;
    let inventory = checkpoint_artifact_inventory(authority, &plan.assignment, 1).await;
    let bound = authority
        .begin_topology_checkpoint_cut(
            &lease.proof(),
            &assignments,
            &topology_preparation::processes(authority),
            plan.operation_id,
            &admitted.plan,
            inventory.clone(),
        )
        .await
        .unwrap();
    (
        authority.load().await.unwrap().unwrap(),
        assignments,
        bound,
        inventory,
    )
}

async fn commit_cut(
    authority: &LeaderLeaseStore,
    proof: &LeaderProof,
    inventory: &CheckpointArtifactInventory,
) -> CommittedCheckpointRef {
    let fence = inventory.assignment_fence.as_ref().unwrap();
    let checkpoint = committed_checkpoint(
        authority,
        fence,
        inventory.attempt.epoch,
        inventory.attempt.checkpoint_id,
    )
    .await;
    authority
        .record_cluster_outcome(
            proof,
            inventory.attempt.epoch,
            inventory.attempt.checkpoint_id,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(checkpoint.clone()),
        )
        .await
        .unwrap();
    checkpoint
}

#[tokio::test]
async fn exact_cut_binding_precedes_barriers_and_is_payload_bound() {
    let authority = store(30_000);
    let (lease, assignments, bound, inventory) = bound_fixture(&authority, false).await;
    assert_eq!(bound.phase, TopologyAdmissionPhase::Quiescing);
    assert_eq!(
        bound.cut.as_ref().unwrap().bound_sequence,
        bound.status_sequence
    );
    let head = authority.load_record().await.unwrap().unwrap();
    #[cfg(feature = "cluster")]
    assert!(authority
        .validate_topology_checkpoint_barrier(
            &lease.proof(),
            inventory.attempt,
            None,
            flags::TOPOLOGY_CUT
        )
        .await
        .is_err());
    assert_eq!(head.version, TOPOLOGY_PREPARATION_RECORD_VERSION);
    assert_eq!(head.active_checkpoint_artifacts, Some(inventory.clone()));
    assert_eq!(
        authority
            .begin_topology_checkpoint_cut(
                &lease.proof(),
                &assignments,
                &topology_preparation::processes(&authority),
                bound.operation_id,
                &bound.plan,
                inventory.clone()
            )
            .await
            .unwrap(),
        bound
    );
    assert_eq!(
        authority
            .begin_cluster_checkpoint_artifacts(&lease.proof(), inventory.clone())
            .await
            .unwrap(),
        inventory
    );
    #[cfg(feature = "cluster")]
    {
        let fence = inventory.assignment_fence.as_ref().unwrap();
        authority
            .validate_topology_checkpoint_barrier(
                &lease.proof(),
                inventory.attempt,
                Some(fence),
                flags::TOPOLOGY_CUT,
            )
            .await
            .unwrap();
        for barrier_flags in [
            flags::NONE,
            flags::HANDOFF,
            flags::HANDOFF | flags::TOPOLOGY_CUT,
        ] {
            assert!(authority
                .validate_topology_checkpoint_barrier(
                    &lease.proof(),
                    inventory.attempt,
                    Some(fence),
                    barrier_flags
                )
                .await
                .is_err());
        }
    }
    let mut changed = inventory.clone();
    changed.attempt = CheckpointAttempt::canonical(2);
    assert!(authority
        .begin_topology_checkpoint_cut(
            &lease.proof(),
            &assignments,
            &topology_preparation::processes(&authority),
            bound.operation_id,
            &bound.plan,
            changed.clone()
        )
        .await
        .is_err());
    assert!(authority
        .begin_cluster_checkpoint_artifacts(&lease.proof(), changed)
        .await
        .is_err());
    assert!(authority
        .abort_topology_plan(&lease.proof(), bound.operation_id, &bound.plan)
        .await
        .is_err());
    let prior = assignments.load().await.unwrap().unwrap();
    let proposal = prior
        .next_draining(
            prior.vnodes.clone(),
            prior.participants.clone(),
            lease.proof(),
        )
        .unwrap();
    assert!(authority
        .publish_assignment_drain(&lease.proof(), &assignments, &proposal)
        .await
        .is_err());
    assert_eq!(
        authority.load().await.unwrap().unwrap().catalog_manifest,
        lease.catalog_manifest
    );
}

#[tokio::test]
async fn commit_requires_every_exact_process_completion_before_cut_prepared() {
    let authority = store(30_000);
    let (lease, _, bound, inventory) = bound_fixture(&authority, true).await;
    let participants = &inventory.assignment_fence.as_ref().unwrap().participants;
    assert!(authority
        .complete_topology_checkpoint_cut(&lease.proof(), inventory.attempt, participants[0])
        .await
        .is_err());
    let checkpoint = commit_cut(&authority, &lease.proof(), &inventory).await;
    let committed = authority
        .topology_operation_status(bound.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.phase, TopologyAdmissionPhase::Quiescing);
    assert_eq!(
        committed
            .cut
            .as_ref()
            .unwrap()
            .committed
            .as_ref()
            .unwrap()
            .checkpoint,
        checkpoint
    );
    assert!(committed
        .cut
        .as_ref()
        .unwrap()
        .completed_participants
        .is_empty());
    let first = authority
        .complete_topology_checkpoint_cut(&lease.proof(), inventory.attempt, participants[0])
        .await
        .unwrap();
    assert_eq!(first.phase, TopologyAdmissionPhase::Quiescing);
    let mut stale = participants[1];
    stale.boot_incarnation = Uuid::from_u128(23);
    assert!(matches!(
        authority
            .complete_topology_checkpoint_cut(&lease.proof(), inventory.attempt, stale)
            .await,
        Err(TopologyError::Fenced)
    ));
    assert_eq!(
        authority
            .complete_topology_checkpoint_cut(&lease.proof(), inventory.attempt, participants[0])
            .await
            .unwrap(),
        first
    );
    let prepared = authority
        .complete_topology_checkpoint_cut(&lease.proof(), inventory.attempt, participants[1])
        .await
        .unwrap();
    assert_eq!(prepared.phase, TopologyAdmissionPhase::CutPrepared);
    assert_eq!(
        &prepared.cut.as_ref().unwrap().completed_participants,
        participants
    );
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    assert_eq!(
        reopened
            .topology_operation_status(bound.operation_id)
            .await
            .unwrap(),
        Some(prepared.clone())
    );
    assert_eq!(
        reopened
            .complete_topology_checkpoint_cut(&lease.proof(), inventory.attempt, participants[0])
            .await
            .unwrap(),
        prepared
    );
    assert_eq!(
        reopened.load().await.unwrap().unwrap().catalog_manifest,
        lease.catalog_manifest
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn lost_binding_response_and_client_cancellation_leave_the_exact_admission() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(6));
    let (lease, assignments, plan, target) = topology_preparation::fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    topology_preparation::prepare_all(&authority, &assignments, &plan, &admitted).await;
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    let task_authority = authority.clone();
    let task_assignments = AssignmentSnapshotStore::new(authority.store.clone());
    let task_inventory = inventory.clone();
    let proof = lease.proof();
    let task_plan = admitted.plan.clone();
    let operation_id = admitted.operation_id;
    let task = tokio::spawn(async move {
        task_authority
            .begin_topology_checkpoint_cut(
                &proof,
                &task_assignments,
                &topology_preparation::processes(&task_authority),
                operation_id,
                &task_plan,
                task_inventory,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let recovered = reopened
        .topology_operation_status(operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.phase, TopologyAdmissionPhase::Quiescing);
    assert_eq!(recovered.cut.as_ref().unwrap().bound_sequence, 6);
    assert_eq!(
        reopened
            .begin_topology_checkpoint_cut(
                &lease.proof(),
                &assignments,
                &topology_preparation::processes(&authority),
                operation_id,
                &admitted.plan,
                inventory.clone()
            )
            .await
            .unwrap(),
        recovered
    );
    assert_eq!(
        reopened
            .load_record()
            .await
            .unwrap()
            .unwrap()
            .active_checkpoint_artifacts,
        Some(inventory)
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn lost_commit_response_keeps_the_cut_quiescing_until_sink_completion() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(7));
    let (lease, _, bound, inventory) = bound_fixture(&authority, false).await;
    let checkpoint = committed_checkpoint(
        &authority,
        inventory.assignment_fence.as_ref().unwrap(),
        1,
        1,
    )
    .await;
    let task_authority = authority.clone();
    let proof = lease.proof();
    let fence = inventory.assignment_fence.clone().unwrap();
    let task_checkpoint = checkpoint.clone();
    let task = tokio::spawn(async move {
        task_authority
            .record_cluster_outcome(
                &proof,
                1,
                1,
                fence,
                CheckpointVerdict::Commit,
                Some(task_checkpoint),
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let status = reopened
        .topology_operation_status(bound.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.phase, TopologyAdmissionPhase::Quiescing);
    assert_eq!(
        status
            .cut
            .as_ref()
            .unwrap()
            .committed
            .as_ref()
            .unwrap()
            .checkpoint,
        checkpoint
    );
    assert!(status
        .cut
        .as_ref()
        .unwrap()
        .completed_participants
        .is_empty());
    assert!(reopened
        .abort_topology_plan(&lease.proof(), bound.operation_id, &bound.plan)
        .await
        .is_err());
    assert_eq!(
        reopened
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .unwrap()
            .checkpoint_id,
        1
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cancelled_final_receipt_is_resolved_by_status_and_exact_retry() {
    let (raw, authority) = delayed_ambiguous_response_once_at(30_000, lease_path(8));
    let (lease, _, bound, inventory) = bound_fixture(&authority, false).await;
    commit_cut(&authority, &lease.proof(), &inventory).await;
    let participant = inventory.assignment_fence.as_ref().unwrap().participants[0];
    let task_authority = authority.clone();
    let proof = lease.proof();
    let attempt = inventory.attempt;
    let task = tokio::spawn(async move {
        task_authority
            .complete_topology_checkpoint_cut(&proof, attempt, participant)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let prepared = reopened
        .topology_operation_status(bound.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.phase, TopologyAdmissionPhase::CutPrepared);
    assert_eq!(prepared.status_sequence, 8);
    assert_eq!(
        reopened
            .complete_topology_checkpoint_cut(&lease.proof(), attempt, participant)
            .await
            .unwrap(),
        prepared
    );
}

#[tokio::test]
async fn definitive_abort_preserves_artifact_ownership_until_exact_cleanup() {
    let authority = store(30_000);
    let (lease, assignments, bound, inventory) = bound_fixture(&authority, false).await;
    authority
        .record_cluster_outcome(
            &lease.proof(),
            1,
            1,
            inventory.assignment_fence.clone().unwrap(),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    let aborted = authority
        .topology_operation_status(bound.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::CheckpointAborted
        }
    );
    assert_eq!(
        authority
            .load_record()
            .await
            .unwrap()
            .unwrap()
            .active_checkpoint_artifacts,
        Some(inventory.clone())
    );
    let mut plan = authority.load_topology_plan(&bound.plan).await.unwrap();
    let target = authority
        .load_catalog_manifest(&plan.target_manifest)
        .await
        .unwrap();
    plan.operation_id = Uuid::from_u128(44).try_into().unwrap();
    plan.parent_manifest = lease.catalog_manifest.clone().unwrap();
    plan.assignment = inventory.assignment_fence.clone().unwrap();
    assert!(authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .is_err());
    authority
        .finish_cluster_checkpoint_artifact_cleanup(&lease.proof(), &inventory)
        .await
        .unwrap();
    assert!(authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .active_checkpoint_artifacts
        .is_none());
    assert_eq!(
        authority
            .topology_operation_status(bound.operation_id)
            .await
            .unwrap(),
        Some(aborted)
    );
}

#[tokio::test]
async fn term_change_after_commit_aborts_only_the_candidate_and_preserves_the_parent_cut() {
    let authority = store(30_000);
    let (lease, assignments, bound, inventory) = bound_fixture(&authority, false).await;
    let checkpoint = commit_cut(&authority, &lease.proof(), &inventory).await;
    let LeaseOutcome::Acquired(new_lease) =
        authority.begin_new_term(&lease.owner, 1).await.unwrap()
    else {
        panic!("new term must be acquired");
    };
    let aborted = authority
        .topology_operation_status(bound.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::LeaderChanged
        }
    );
    assert_eq!(
        aborted
            .cut
            .as_ref()
            .unwrap()
            .committed
            .as_ref()
            .unwrap()
            .checkpoint,
        checkpoint
    );
    assert_eq!(
        authority
            .highest_cluster_committed_outcome()
            .await
            .unwrap()
            .unwrap()
            .committed_checkpoint,
        Some(checkpoint)
    );
    let participant = inventory.assignment_fence.as_ref().unwrap().participants[0];
    assert!(authority
        .complete_topology_checkpoint_cut(&lease.proof(), inventory.attempt, participant)
        .await
        .is_err());
    assert!(authority
        .complete_topology_checkpoint_cut(&new_lease.proof(), inventory.attempt, participant)
        .await
        .is_err());
    assert_eq!(new_lease.catalog_manifest, lease.catalog_manifest);
    let prior = assignments.load().await.unwrap().unwrap();
    let drain = prior
        .next_draining(
            prior.vnodes.clone(),
            prior.participants.clone(),
            new_lease.proof(),
        )
        .unwrap();
    authority
        .publish_assignment_drain(&new_lease.proof(), &assignments, &drain)
        .await
        .unwrap();
    assert_eq!(
        authority.load_record().await.unwrap().unwrap().version,
        TOPOLOGY_PREPARATION_RECORD_VERSION
    );
}

#[tokio::test]
async fn recovery_fault_before_all_completions_retains_the_irreversible_checkpoint() {
    let authority = store(30_000);
    let (lease, _, bound, inventory) = bound_fixture(&authority, true).await;
    let checkpoint = commit_cut(&authority, &lease.proof(), &inventory).await;
    authority
        .record_recovery_fault(owner_recovery_fault_publisher(&lease.owner), 1)
        .await
        .unwrap();
    let status = authority
        .topology_operation_status(bound.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        status.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::Recovery
        }
    );
    assert_eq!(
        status
            .cut
            .as_ref()
            .unwrap()
            .committed
            .as_ref()
            .unwrap()
            .checkpoint,
        checkpoint
    );
    assert!(authority
        .complete_topology_checkpoint_cut(
            &lease.proof(),
            inventory.attempt,
            inventory.assignment_fence.as_ref().unwrap().participants[1]
        )
        .await
        .is_err());
    assert!(
        authority
            .load_record()
            .await
            .unwrap()
            .unwrap()
            .recovery_fault_slots[0]
            .active
    );
}

#[tokio::test]
async fn cut_roots_and_admission_anchors_survive_bounded_pruning() {
    let authority = store(30_000);
    let (lease, _, bound, inventory) = bound_fixture(&authority, false).await;
    let checkpoint = commit_cut(&authority, &lease.proof(), &inventory).await;
    let prepared = authority
        .complete_topology_checkpoint_cut(
            &lease.proof(),
            inventory.attempt,
            inventory.assignment_fence.as_ref().unwrap().participants[0],
        )
        .await
        .unwrap();
    let proposed_newer = committed_checkpoint_with_predecessor(
        &authority,
        inventory.assignment_fence.as_ref().unwrap(),
        2,
        9,
        Some(checkpoint.clone()),
    )
    .await;
    assert!(authority
        .begin_cluster_artifact_cleanup(&lease.proof(), proposed_newer, accept_recovery_artifacts)
        .await
        .unwrap()
        .is_none());
    for timestamp in 1..80 {
        authority
            .renew_exact(&lease.owner, lease.token, timestamp)
            .await
            .unwrap();
    }
    LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .unwrap();
    for sequence in [
        prepared.admitted_sequence,
        prepared.status_sequence,
        prepared.cut.as_ref().unwrap().bound_sequence,
        prepared
            .cut
            .as_ref()
            .unwrap()
            .committed
            .as_ref()
            .unwrap()
            .authority_sequence,
    ] {
        assert!(read_authority_record(authority.store.as_ref(), sequence)
            .await
            .unwrap()
            .is_some());
    }
    assert_eq!(
        authority
            .topology_operation_status(bound.operation_id)
            .await
            .unwrap(),
        Some(prepared)
    );
    authority
        .store
        .delete(&committed_checkpoint_path(&checkpoint))
        .await
        .unwrap();
    assert!(authority
        .topology_operation_status(bound.operation_id)
        .await
        .is_err());
    assert!(LeaderLeaseStore::prune_history(&authority.store, 0)
        .await
        .is_err());
}

#[tokio::test]
async fn malformed_or_rewound_cut_evidence_is_rejected_without_an_append() {
    let authority = store(30_000);
    let (lease, _, bound, inventory) = bound_fixture(&authority, false).await;
    let head = authority.load_record().await.unwrap().unwrap();
    let mut malformed = head.clone();
    malformed.topology_operations[0].phase = TopologyAdmissionPhase::CutPrepared;
    assert!(malformed.validate().is_err());
    malformed = head.clone();
    malformed.version = TOPOLOGY_ADMISSION_RECORD_VERSION;
    assert!(malformed.validate().is_err());
    malformed = head.clone();
    malformed.topology_operations[0]
        .cut
        .as_mut()
        .unwrap()
        .bound_sequence = bound.admitted_sequence;
    assert!(malformed.validate().is_err());
    commit_cut(&authority, &lease.proof(), &inventory).await;
    let committed = authority.load_record().await.unwrap().unwrap();
    let mut fabricated = head.clone();
    fabricated.lease.seq = committed.lease.seq;
    fabricated
        .topology_operations
        .clone_from(&committed.topology_operations);
    assert!(
        head.validate_topology_admission_successor(&fabricated)
            .is_err(),
        "a cut Commit cannot be attached to an append without its terminal checkpoint outcome"
    );
    let mut rewind = committed.clone();
    rewind.topology_operations[0]
        .cut
        .as_mut()
        .unwrap()
        .committed = None;
    assert!(committed
        .validate_topology_admission_successor(&rewind)
        .is_err());
}

#[tokio::test]
async fn cut_binding_is_fenced_when_a_new_term_wins_before_its_append() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(6));
    let (lease, assignments, plan, target) = topology_preparation::fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    topology_preparation::prepare_all(&authority, &assignments, &plan, &admitted).await;
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    let task_authority = Arc::clone(&authority);
    let task_proof = lease.proof();
    let task = tokio::spawn(async move {
        task_authority
            .begin_topology_checkpoint_cut(
                &task_proof,
                &assignments,
                &topology_preparation::processes(&task_authority),
                plan.operation_id,
                &admitted.plan,
                inventory,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    authority.begin_new_term(&lease.owner, 1).await.unwrap();
    raw.release.add_permits(1);
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Fenced)));
    let status = authority
        .topology_operation_status(plan.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        status.phase,
        TopologyAdmissionPhase::Aborted {
            reason: TopologyAbortReason::LeaderChanged
        }
    );
    assert!(status.cut.is_none());
    assert!(authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .active_checkpoint_artifacts
        .is_none());
}

#[tokio::test(start_paused = true)]
async fn cut_binding_deadline_before_append_retains_only_the_original_plan() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(6));
    let (lease, assignments, plan, target) = topology_preparation::fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let prepared =
        topology_preparation::prepare_all(&authority, &assignments, &plan, &admitted).await;
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    let task_authority = Arc::clone(&authority);
    let task_proof = lease.proof();
    let task_plan = admitted.plan.clone();
    let task = tokio::spawn(async move {
        task_authority
            .begin_topology_checkpoint_cut(
                &task_proof,
                &assignments,
                &topology_preparation::processes(&task_authority),
                plan.operation_id,
                &task_plan,
                inventory,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(16)).await;
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Contended)));
    assert_eq!(
        authority
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(prepared)
    );
    assert!(authority
        .load_record()
        .await
        .unwrap()
        .unwrap()
        .active_checkpoint_artifacts
        .is_none());
}

#[tokio::test]
async fn unused_reservation_abort_retries_exactly_and_cannot_reclassify_admitted_abort() {
    let authority = store(30_000);
    let (lease, assignments, plan, target) = topology_preparation::fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    let unused = CheckpointAttempt::canonical(1);
    assert!(matches!(
        authority
            .abort_unadmitted_cluster_checkpoint(&lease.proof(), unused, plan.assignment.clone())
            .await
            .unwrap(),
        RecordOutcomeResult::Created(_)
    ));
    assert!(matches!(
        authority
            .abort_unadmitted_cluster_checkpoint(&lease.proof(), unused, plan.assignment.clone())
            .await
            .unwrap(),
        RecordOutcomeResult::Unchanged(_)
    ));
    assert_eq!(
        authority
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(admitted.clone())
    );
    topology_preparation::prepare_all(&authority, &assignments, &plan, &admitted).await;
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 2).await;
    authority
        .begin_topology_checkpoint_cut(
            &lease.proof(),
            &assignments,
            &topology_preparation::processes(&authority),
            plan.operation_id,
            &admitted.plan,
            inventory.clone(),
        )
        .await
        .unwrap();
    assert!(authority
        .abort_unadmitted_cluster_checkpoint(
            &lease.proof(),
            inventory.attempt,
            plan.assignment.clone()
        )
        .await
        .is_err());
    authority
        .record_cluster_outcome(
            &lease.proof(),
            inventory.attempt.epoch,
            inventory.attempt.checkpoint_id,
            plan.assignment.clone(),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    assert!(authority
        .abort_unadmitted_cluster_checkpoint(
            &lease.proof(),
            inventory.attempt,
            plan.assignment.clone()
        )
        .await
        .is_err());
    authority
        .finish_cluster_checkpoint_artifact_cleanup(&lease.proof(), &inventory)
        .await
        .unwrap();
    assert!(authority
        .abort_unadmitted_cluster_checkpoint(&lease.proof(), inventory.attempt, plan.assignment)
        .await
        .is_err());
}

#[tokio::test]
async fn cut_admission_winning_unused_abort_append_requires_normal_recovery() {
    let (raw, authority) = blocking_once_at(30_000, lease_path(6));
    let (lease, assignments, plan, target) = topology_preparation::fixture(&authority).await;
    let admitted = authority
        .admit_topology_plan(&lease.proof(), &assignments, &plan, &target)
        .await
        .unwrap();
    topology_preparation::prepare_all(&authority, &assignments, &plan, &admitted).await;
    let inventory = checkpoint_artifact_inventory(&authority, &plan.assignment, 1).await;
    let task_authority = Arc::clone(&authority);
    let proof = lease.proof();
    let fence = plan.assignment.clone();
    let attempt = inventory.attempt;
    let task = tokio::spawn(async move {
        task_authority
            .abort_unadmitted_cluster_checkpoint(&proof, attempt, fence)
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    let bound = authority
        .begin_topology_checkpoint_cut(
            &lease.proof(),
            &assignments,
            &topology_preparation::processes(&authority),
            plan.operation_id,
            &admitted.plan,
            inventory,
        )
        .await
        .unwrap();
    raw.release.add_permits(1);
    assert!(task.await.unwrap().is_err());
    assert!(authority
        .cluster_outcome(attempt.epoch)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        authority
            .topology_operation_status(plan.operation_id)
            .await
            .unwrap(),
        Some(bound)
    );
}
