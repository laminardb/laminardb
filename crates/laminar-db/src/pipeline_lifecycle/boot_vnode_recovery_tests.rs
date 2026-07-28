use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use laminar_core::checkpoint::{
    CheckpointAssignmentFence, CheckpointParticipant, CheckpointWatermark, ClusterRecoveryCapsule,
    CommittedSourceHandoff, ParticipantRecoveryRef, PipelineIdentity,
    CLUSTER_RECOVERY_CAPSULE_VERSION, PIPELINE_IDENTITY_VERSION,
};
use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, ClusterController, ClusterKv, InMemoryKv,
    LeaseDeadline,
};
use laminar_core::cluster::discovery::{NodeId as ClusterNodeId, NodeInfo};
use laminar_core::state::{CheckpointAttempt, InProcessBackend, NodeId, VnodeRegistry};

use crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut;
use crate::db::LaminarDB;
use crate::recovery_manager::vnode_chains::LoadedVnodeChains;
use crate::vnode_transition_staging::VnodeTransitionOrigin;

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn committed_source_handoff(
    attempt: CheckpointAttempt,
    sealed_assignment_version: u64,
) -> Arc<CommittedSourceHandoff> {
    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(1),
    };
    let capsule = ClusterRecoveryCapsule {
        version: CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt,
        deployment_id: uuid::Uuid::from_u128(7).to_string(),
        pipeline_identity: PipelineIdentity {
            canonical_version: PIPELINE_IDENTITY_VERSION,
            sha256: digest(1),
        },
        assignment_fence: CheckpointAssignmentFence::from_owner_map(
            sealed_assignment_version,
            &[1, 1],
            vec![participant],
        )
        .unwrap(),
        seal_inventory_sha256: digest(2),
        participants: vec![ParticipantRecoveryRef {
            participant_id: 1,
            readiness_sha256: digest(3),
            manifest_sha256: digest(4),
            portable_state_sha256: digest(5),
        }],
        source_offsets: BTreeMap::new(),
        source_metadata: BTreeMap::new(),
        source_assignment_versions: BTreeMap::new(),
        source_watermarks: BTreeMap::new(),
        cluster_watermark: CheckpointWatermark::Uninitialized,
        recovery_watermark_frontier: None,
        portable_state_sha256: digest(5),
    };
    Arc::new(CommittedSourceHandoff::try_from(&capsule).unwrap())
}

async fn boot_test_db(registry: Arc<VnodeRegistry>) -> Arc<LaminarDB> {
    let node = ClusterNodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    controller.publish_recovery_incarnation().await.unwrap();

    LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(2)))
        .vnode_registry(registry)
        .build()
        .await
        .unwrap()
}

fn boot_restore_authority(
    target: &laminar_core::state::VnodeAssignmentSnapshot,
    attempt: CheckpointAttempt,
    required_vnodes: &[u32],
) -> (
    CheckpointAssignmentFence,
    CheckpointParticipant,
    ValidatedClusterVnodeRestoreCut,
) {
    let owner_ids: Vec<u64> = target.owners().iter().map(|owner| owner.0).collect();
    let mut participant_ids = owner_ids.clone();
    participant_ids.sort_unstable();
    participant_ids.dedup();
    let participants: Vec<CheckpointParticipant> = participant_ids
        .into_iter()
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        })
        .collect();
    let participant = participants
        .iter()
        .copied()
        .find(|participant| participant.node_id == 1)
        .expect("boot test target must retain the local participant");
    let fence =
        CheckpointAssignmentFence::from_owner_map(target.version(), &owner_ids, participants)
            .unwrap();
    let identity = PipelineIdentity {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        sha256: digest(1),
    };
    let cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity,
        fence.clone(),
        &owner_ids,
        required_vnodes,
    )
    .unwrap();
    (fence, participant, cut)
}

fn loaded_vnode_chains(
    attempt: CheckpointAttempt,
    chains: HashMap<u32, Vec<Bytes>>,
) -> LoadedVnodeChains {
    LoadedVnodeChains::from_chains_for_test(Some(attempt), chains)
}

#[tokio::test]
async fn boot_report_marks_and_stages_the_exact_owned_roster() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let attempt = CheckpointAttempt::canonical(7);
    let report = loaded_vnode_chains(
        attempt,
        HashMap::from([
            (1, vec![Bytes::from_static(b"vnode-1")]),
            (0, vec![Bytes::from_static(b"vnode-0")]),
        ]),
    );
    let target_assignment = registry.versioned_snapshot();
    let (target_fence, participant, restore_cut) =
        boot_restore_authority(&target_assignment, attempt, &[0, 1]);
    let controller = db.cluster_controller.lock().clone().unwrap();
    db.publish_local_vnode_state_report(&controller, &target_assignment, true)
        .await
        .unwrap();

    db.publish_boot_vnode_restore_transition(
        &registry,
        &target_assignment,
        target_fence.clone(),
        participant,
        restore_cut,
        report,
    )
    .await
    .unwrap();

    let readiness = controller.read_adopted_assignments().await.unwrap();
    let local_report = readiness
        .iter()
        .find_map(|(node, report)| (*node == ClusterNodeId(1)).then_some(report))
        .expect("boot staging must retain the local readiness slot");
    assert_eq!(local_report.assignment_version, target_assignment.version());
    assert!(!local_report.vnode_state_ready);
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    let transition = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("boot transition must be published");
    assert_eq!(transition.origin(), &VnodeTransitionOrigin::BootRecovery);
    assert_eq!(transition.target(), &target_fence);
    assert_eq!(transition.acquired_vnodes(), [0, 1]);
    assert!(transition.revoked_vnodes().is_empty());
    assert_eq!(
        transition
            .restore_cut()
            .expect("boot transition must retain its cut")
            .attempt(),
        attempt
    );
    assert_eq!(transition.restores().len(), 2);
    assert_eq!(transition.restores()[0].vnode(), 0);
    assert_eq!(
        transition.restores()[0].chain(),
        [Bytes::from_static(b"vnode-0")]
    );
    assert_eq!(transition.restores()[1].vnode(), 1);
    assert_eq!(
        transition.restores()[1].chain(),
        [Bytes::from_static(b"vnode-1")]
    );
}

#[tokio::test]
async fn invalid_boot_report_changes_neither_staging_nor_lifecycle() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let attempt = CheckpointAttempt::canonical(7);
    let report = loaded_vnode_chains(
        attempt,
        HashMap::from([(0, vec![Bytes::from_static(b"vnode-0")])]),
    );
    let target_assignment = registry.versioned_snapshot();
    let (target_fence, participant, restore_cut) =
        boot_restore_authority(&target_assignment, attempt, &[0, 1]);

    let error = db
        .publish_boot_vnode_restore_transition(
            &registry,
            &target_assignment,
            target_fence,
            participant,
            restore_cut,
            report,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("does not match acquired roster"));
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(!registry.any_restoring());
}

#[tokio::test]
async fn changed_boot_assignment_rejects_report_without_mutation() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let target_assignment = registry.versioned_snapshot();
    let attempt = CheckpointAttempt::canonical(7);
    let (target_fence, participant, restore_cut) =
        boot_restore_authority(&target_assignment, attempt, &[0, 1]);
    registry.set_assignment_and_version(vec![NodeId(2), NodeId(2)].into(), 2);
    let report = loaded_vnode_chains(
        attempt,
        HashMap::from([
            (0, vec![Bytes::from_static(b"vnode-0")]),
            (1, vec![Bytes::from_static(b"vnode-1")]),
        ]),
    );

    let error = db
        .publish_boot_vnode_restore_transition(
            &registry,
            &target_assignment,
            target_fence,
            participant,
            restore_cut,
            report,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("changed"), "{error}");
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(!registry.any_restoring());
}

#[tokio::test]
async fn wrong_boot_attempt_rejects_report_without_mutation() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let target_assignment = registry.versioned_snapshot();
    let attempt = CheckpointAttempt::canonical(7);
    let (target_fence, participant, restore_cut) =
        boot_restore_authority(&target_assignment, attempt, &[0, 1]);
    let report = loaded_vnode_chains(
        CheckpointAttempt::canonical(8),
        HashMap::from([
            (0, vec![Bytes::from_static(b"vnode-0")]),
            (1, vec![Bytes::from_static(b"vnode-1")]),
        ]),
    );

    let error = db
        .publish_boot_vnode_restore_transition(
            &registry,
            &target_assignment,
            target_fence,
            participant,
            restore_cut,
            report,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("does not match"), "{error}");
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(!registry.any_restoring());
}

#[tokio::test]
async fn failed_boot_preparation_retains_prior_exact_transition_and_lifecycle() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let target = registry.versioned_snapshot();
    let prior_attempt = CheckpointAttempt::canonical(7);
    let (prior_fence, participant, prior_cut) =
        boot_restore_authority(&target, prior_attempt, &[0, 1]);
    db.publish_boot_vnode_restore_transition(
        &registry,
        &target,
        prior_fence,
        participant,
        prior_cut,
        loaded_vnode_chains(
            prior_attempt,
            HashMap::from([
                (0, vec![Bytes::from_static(b"prior-0")]),
                (1, vec![Bytes::from_static(b"prior-1")]),
            ]),
        ),
    )
    .await
    .unwrap();
    let prior = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("prior boot transition must be pending");

    let replacement_attempt = CheckpointAttempt::canonical(8);
    let (_replacement_fence, _participant, replacement_cut) =
        boot_restore_authority(&target, replacement_attempt, &[0, 1]);
    let error = db
        .prepare_boot_vnode_restore_transition(&replacement_cut)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("no durable assignment history"));
    let retained = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("failed replacement must retain prior transition");
    assert!(Arc::ptr_eq(&prior, &retained));
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    assert_eq!(retained.restore_cut().unwrap().attempt(), prior_attempt);
    assert_eq!(
        retained.restores()[0].chain(),
        [Bytes::from_static(b"prior-0")]
    );
}

#[tokio::test]
async fn successful_boot_replacement_publishes_exact_arc_and_target_lifecycle() {
    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    registry.set_assignment_and_version(vec![NodeId(1), NodeId(2)].into(), 2);
    let db = boot_test_db(Arc::clone(&registry)).await;
    let target = registry.versioned_snapshot();
    let prior_attempt = CheckpointAttempt::canonical(7);
    let (prior_fence, participant, prior_cut) =
        boot_restore_authority(&target, prior_attempt, &[0]);
    db.publish_boot_vnode_restore_transition(
        &registry,
        &target,
        prior_fence,
        participant,
        prior_cut,
        loaded_vnode_chains(
            prior_attempt,
            HashMap::from([(0, vec![Bytes::from_static(b"prior")])]),
        ),
    )
    .await
    .unwrap();
    let prior = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("prior boot transition must be pending");
    registry.mark_restoring(&[1]);
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);

    let replacement_attempt = CheckpointAttempt::canonical(8);
    let (replacement_fence, participant, replacement_cut) =
        boot_restore_authority(&target, replacement_attempt, &[0]);
    db.publish_boot_vnode_restore_transition(
        &registry,
        &target,
        replacement_fence.clone(),
        participant,
        replacement_cut,
        loaded_vnode_chains(
            replacement_attempt,
            HashMap::from([(0, vec![Bytes::from_static(b"replacement")])]),
        ),
    )
    .await
    .unwrap();

    let replacement = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("replacement boot transition must be pending");
    assert!(!Arc::ptr_eq(&prior, &replacement));
    assert_eq!(replacement.origin(), &VnodeTransitionOrigin::BootRecovery);
    assert_eq!(replacement.target(), &replacement_fence);
    assert_eq!(replacement.acquired_vnodes(), [0]);
    assert_eq!(
        replacement.restore_cut().unwrap().attempt(),
        replacement_attempt
    );
    assert_eq!(replacement.restores().len(), 1);
    assert_eq!(replacement.restores()[0].vnode(), 0);
    assert_eq!(
        replacement.restores()[0].chain(),
        [Bytes::from_static(b"replacement")]
    );
    assert_eq!(registry.restoring_vnodes(), vec![0]);
    assert!(!registry.is_restoring(1));
}

#[tokio::test]
async fn zero_owned_boot_target_retires_prior_transition_only_after_authority_audit() {
    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    registry.set_assignment_and_version(vec![peer_id, peer_id].into(), 1);

    let objects: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects)));
    let target = AssignmentSnapshot::empty()
        .next_for_participants(
            BTreeMap::from([(0, peer_id), (1, peer_id)]),
            vec![CheckpointParticipant {
                node_id: peer_id.0,
                boot_incarnation: uuid::Uuid::from_u128(2),
            }],
        )
        .unwrap();
    assignments.save_if_absent(&target).await.unwrap();

    let node = ClusterNodeId(self_id.0);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new(
        node,
        kv,
        Some(Arc::clone(&assignments)),
        members_rx,
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::clone(&objects))
        .state_backend(Arc::new(InProcessBackend::new(2)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(assignments)
        .build()
        .await
        .unwrap();

    let attempt = CheckpointAttempt::canonical(7);
    let identity = PipelineIdentity {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        sha256: digest(1),
    };
    let prior_participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: uuid::Uuid::from_u128(1),
    };
    let prior_fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[self_id.0, self_id.0],
        vec![prior_participant],
    )
    .unwrap();
    let prior_cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity.clone(),
        prior_fence.clone(),
        &[self_id.0, self_id.0],
        &[0, 1],
    )
    .unwrap();
    let prior = Arc::new(
        crate::vnode_transition_staging::PendingVnodeTransition::boot_recovery(
            prior_fence,
            &[self_id, self_id],
            prior_participant,
            identity.clone(),
            prior_cut,
            loaded_vnode_chains(
                attempt,
                HashMap::from([
                    (0, vec![Bytes::from_static(b"prior-0")]),
                    (1, vec![Bytes::from_static(b"prior-1")]),
                ]),
            ),
        )
        .unwrap(),
    );
    *db.pending_vnode_transition.lock() = Some(Arc::clone(&prior));
    registry.mark_restoring(&[0, 1]);

    let target_fence = target.assignment_fence().unwrap();
    let target_cut = ValidatedClusterVnodeRestoreCut::synthetic_for_transition_test(
        attempt,
        identity,
        target_fence,
        &[peer_id.0, peer_id.0],
        &[],
    )
    .unwrap();
    db.prepare_boot_vnode_restore_transition(&target_cut)
        .await
        .unwrap();

    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(!registry.any_restoring());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn unassigned_replacement_boot_defers_restore_without_publishing_state() {
    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let donor = VnodeRegistry::single_owner(2, NodeId(1)).versioned_snapshot();
    let (_, _, restore_cut) =
        boot_restore_authority(&donor, CheckpointAttempt::canonical(7), &[0, 1]);

    db.prepare_boot_vnode_restore_transition(&restore_cut)
        .await
        .expect("an unassigned replacement process must restore through later adoption");

    assert_eq!(registry.assignment_version(), 0);
    assert!(!registry.any_restoring());
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn unassigned_replacement_boot_rejects_retained_lifecycle_state() {
    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    registry.mark_restoring(&[0]);
    let db = boot_test_db(Arc::clone(&registry)).await;
    let donor = VnodeRegistry::single_owner(2, NodeId(1)).versioned_snapshot();
    let (_, _, restore_cut) =
        boot_restore_authority(&donor, CheckpointAttempt::canonical(7), &[0, 1]);

    let error = db
        .prepare_boot_vnode_restore_transition(&restore_cut)
        .await
        .expect_err("assignment zero with retained lifecycle state must fail closed");

    assert!(error.to_string().contains("[LDB-6031]"), "{error}");
    assert_eq!(registry.assignment_version(), 0);
    assert!(registry.is_restoring(0));
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn fresh_cluster_start_rejects_staged_vnode_state_without_clearing_it() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let attempt = CheckpointAttempt::canonical(7);
    let target = registry.versioned_snapshot();
    let (fence, participant, cut) = boot_restore_authority(&target, attempt, &[0, 1]);
    let loaded = loaded_vnode_chains(
        attempt,
        HashMap::from([
            (0, vec![Bytes::from_static(b"must-not-be-discarded-0")]),
            (1, vec![Bytes::from_static(b"must-not-be-discarded-1")]),
        ]),
    );
    db.publish_boot_vnode_restore_transition(&registry, &target, fence, participant, cut, loaded)
        .await
        .unwrap();
    let pending = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("valid boot transition must be pending");

    let error = db.validate_fresh_cluster_vnode_start().unwrap_err();

    assert!(error.to_string().contains("refusing a fresh graph"));
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    let retained = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("fresh-start rejection must retain pending work");
    assert!(Arc::ptr_eq(&pending, &retained));
    assert_eq!(retained.restores()[1].vnode(), 1);
    assert_eq!(
        retained.restores()[1].chain(),
        [Bytes::from_static(b"must-not-be-discarded-1")]
    );
}

async fn assert_boot_recovery_target_mismatch_is_non_mutating(
    recovered_attempt: CheckpointAttempt,
    recovered_assignment_version: u64,
) {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let committed_attempt = CheckpointAttempt::canonical(7);
    let sealed_assignment_version = 11;
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1), NodeId(1)].into(),
        2,
        committed_source_handoff(committed_attempt, sealed_assignment_version),
    );
    let target = registry.versioned_snapshot();
    let (fence, participant, cut) = boot_restore_authority(&target, committed_attempt, &[0, 1]);
    db.publish_boot_vnode_restore_transition(
        &registry,
        &target,
        fence,
        participant,
        cut,
        loaded_vnode_chains(
            committed_attempt,
            HashMap::from([
                (0, vec![Bytes::from_static(b"existing-stage-0")]),
                (1, vec![Bytes::from_static(b"existing-stage-1")]),
            ]),
        ),
    )
    .await
    .unwrap();
    let pending = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("pre-existing boot transition must be pending");

    let error = db
        .validate_boot_vnode_recovery_target(recovered_attempt, recovered_assignment_version)
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("does not match installed source handoff"));
    let assignment = registry.versioned_snapshot();
    assert_eq!(assignment.version(), 2);
    assert_eq!(assignment.owners(), [NodeId(1), NodeId(1)]);
    assert_eq!(assignment.source_handoff_installed_version(), Some(2));
    assert_eq!(assignment.source_handoff_attempt(), Some(committed_attempt));
    assert_eq!(
        assignment.source_handoff_assignment_version(),
        Some(sealed_assignment_version)
    );
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    let retained = db
        .pending_vnode_transition
        .lock()
        .clone()
        .expect("target mismatch must retain the pending transition");
    assert!(Arc::ptr_eq(&pending, &retained));
    assert_eq!(
        retained.restores()[0].chain(),
        [Bytes::from_static(b"existing-stage-0")]
    );
    assert!(registry.is_restoring(1));
}

#[tokio::test]
async fn boot_recovery_accepts_newer_committed_cut_under_the_installed_assignment() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let assignment_handoff_attempt = CheckpointAttempt::canonical(2);
    registry.set_assignment_and_version_with_source_handoff(
        vec![NodeId(1), NodeId(1)].into(),
        2,
        committed_source_handoff(assignment_handoff_attempt, 1),
    );

    let reconciled_handoff_version = db
        .validate_boot_vnode_recovery_target(CheckpointAttempt::canonical(17), 2)
        .expect(
            "a newer committed cut sealed under the current assignment must supersede its older acquisition handoff",
        );

    assert_eq!(reconciled_handoff_version, Some(2));
    let assignment = registry.versioned_snapshot();
    assert_eq!(assignment.version(), 2);
    assert_eq!(assignment.source_handoff_installed_version(), Some(2));
    assert_eq!(
        assignment.source_handoff_attempt(),
        Some(assignment_handoff_attempt)
    );
    assert_eq!(assignment.source_handoff_assignment_version(), Some(1));
}

#[tokio::test]
async fn boot_recovery_accepts_newer_cut_before_a_roster_only_successor() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let assignment_handoff_attempt = CheckpointAttempt::canonical(2);
    let owners: Arc<[NodeId]> = vec![NodeId(1), NodeId(1)].into();
    registry.set_assignment_and_version_with_source_handoff(
        Arc::clone(&owners),
        2,
        committed_source_handoff(assignment_handoff_attempt, 1),
    );
    registry.set_assignment_and_version_carrying_source_handoff(owners, 3);

    let reconciled_handoff_version = db
        .validate_boot_vnode_recovery_target(CheckpointAttempt::canonical(17), 2)
        .expect("a newer cut may precede a successor that carried the acquisition handoff");

    assert_eq!(reconciled_handoff_version, Some(2));
    let assignment = registry.versioned_snapshot();
    assert_eq!(assignment.version(), 3);
    assert_eq!(assignment.source_handoff_installed_version(), Some(2));
    assert_eq!(
        assignment.source_handoff_attempt(),
        Some(assignment_handoff_attempt)
    );
    assert_eq!(assignment.source_handoff_assignment_version(), Some(1));
}

#[tokio::test]
async fn boot_recovery_rejects_committed_handoff_attempt_mismatch_without_mutation() {
    assert_boot_recovery_target_mismatch_is_non_mutating(CheckpointAttempt::canonical(8), 11).await;
}

#[tokio::test]
async fn boot_recovery_rejects_newer_cut_before_handoff_installation() {
    assert_boot_recovery_target_mismatch_is_non_mutating(CheckpointAttempt::canonical(8), 1).await;
}

#[tokio::test]
async fn boot_recovery_rejects_sealed_assignment_mismatch_without_mutation() {
    assert_boot_recovery_target_mismatch_is_non_mutating(CheckpointAttempt::canonical(7), 12).await;
}
