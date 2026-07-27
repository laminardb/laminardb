use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use laminar_core::checkpoint::{
    CheckpointAssignmentFence, CheckpointParticipant, CheckpointWatermark, ClusterRecoveryCapsule,
    CommittedSourceHandoff, ParticipantRecoveryRef, PipelineIdentity,
    CLUSTER_RECOVERY_CAPSULE_VERSION, PIPELINE_IDENTITY_VERSION,
};
use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv, LeaseDeadline};
use laminar_core::cluster::discovery::{NodeId as ClusterNodeId, NodeInfo};
use laminar_core::state::{CheckpointAttempt, InProcessBackend, NodeId, VnodeRegistry};

use crate::db::LaminarDB;
use crate::recovery_manager::VnodeRehydration;

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

    LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(2)))
        .vnode_registry(registry)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn boot_report_marks_and_stages_the_exact_owned_roster() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let attempt = CheckpointAttempt::canonical(7);
    let report = VnodeRehydration {
        attempt: Some(attempt),
        restored: HashMap::from([
            (1, vec![Bytes::from_static(b"vnode-1")]),
            (0, vec![Bytes::from_static(b"vnode-0")]),
        ]),
    };
    let target_assignment = registry.versioned_snapshot();

    let staged = db
        .publish_boot_vnode_rehydration(&registry, &target_assignment, &[0, 1], attempt, report)
        .unwrap();

    assert_eq!(staged, 2);
    assert_eq!(registry.restoring_vnodes(), vec![0, 1]);
    let staged = db.rehydrated_vnode_state.lock();
    assert_eq!(staged.len(), 2);
    assert_eq!(staged[&0].epoch, attempt.epoch);
    assert_eq!(staged[&0].chain, vec![Bytes::from_static(b"vnode-0")]);
    assert_eq!(staged[&1].epoch, attempt.epoch);
    assert_eq!(staged[&1].chain, vec![Bytes::from_static(b"vnode-1")]);
}

#[tokio::test]
async fn invalid_boot_report_changes_neither_staging_nor_lifecycle() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let attempt = CheckpointAttempt::canonical(7);
    let report = VnodeRehydration {
        attempt: Some(attempt),
        restored: HashMap::from([(0, vec![Bytes::from_static(b"vnode-0")])]),
    };
    let target_assignment = registry.versioned_snapshot();

    let error = db
        .publish_boot_vnode_rehydration(&registry, &target_assignment, &[0, 1], attempt, report)
        .unwrap_err();

    assert!(error.to_string().contains("does not match owned roster"));
    assert!(db.rehydrated_vnode_state.lock().is_empty());
    assert!(!registry.any_restoring());
}

#[tokio::test]
async fn changed_boot_assignment_rejects_report_without_mutation() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let target_assignment = registry.versioned_snapshot();
    registry.set_assignment_and_version(vec![NodeId(2), NodeId(2)].into(), 2);
    let attempt = CheckpointAttempt::canonical(7);
    let report = VnodeRehydration {
        attempt: Some(attempt),
        restored: HashMap::from([
            (0, vec![Bytes::from_static(b"vnode-0")]),
            (1, vec![Bytes::from_static(b"vnode-1")]),
        ]),
    };

    let error = db
        .publish_boot_vnode_rehydration(&registry, &target_assignment, &[0, 1], attempt, report)
        .unwrap_err();

    assert!(error.to_string().contains("changed"), "{error}");
    assert!(db.rehydrated_vnode_state.lock().is_empty());
    assert!(!registry.any_restoring());
}

#[tokio::test]
async fn wrong_boot_attempt_rejects_report_without_mutation() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    let target_assignment = registry.versioned_snapshot();
    let attempt = CheckpointAttempt::canonical(7);
    let report = VnodeRehydration {
        attempt: Some(CheckpointAttempt::canonical(8)),
        restored: HashMap::from([
            (0, vec![Bytes::from_static(b"vnode-0")]),
            (1, vec![Bytes::from_static(b"vnode-1")]),
        ]),
    };

    let error = db
        .publish_boot_vnode_rehydration(&registry, &target_assignment, &[0, 1], attempt, report)
        .unwrap_err();

    assert!(error.to_string().contains("does not match"), "{error}");
    assert!(db.rehydrated_vnode_state.lock().is_empty());
    assert!(!registry.any_restoring());
}

#[tokio::test]
async fn startup_reset_clears_stale_staging_and_lifecycle() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    registry.mark_restoring(&[0, 1]);
    db.pending_revoke_vnodes.lock().insert(0);
    db.rehydrated_vnode_state.lock().insert(
        1,
        crate::db::RehydratedVnode {
            epoch: 7,
            chain: vec![Bytes::from_static(b"stale")],
        },
    );

    db.reset_staged_vnode_transition_for_startup();

    assert!(db.pending_revoke_vnodes.lock().is_empty());
    assert!(db.rehydrated_vnode_state.lock().is_empty());
    assert!(!registry.any_restoring());
    assert!(registry.restoring_vnodes().is_empty());
}

#[tokio::test]
async fn fresh_cluster_start_rejects_staged_vnode_state_without_clearing_it() {
    let registry = Arc::new(VnodeRegistry::single_owner(2, NodeId(1)));
    let db = boot_test_db(Arc::clone(&registry)).await;
    registry.mark_restoring(&[1]);
    db.rehydrated_vnode_state.lock().insert(
        1,
        crate::db::RehydratedVnode {
            epoch: 7,
            chain: vec![Bytes::from_static(b"must-not-be-discarded")],
        },
    );

    let error = db.validate_fresh_cluster_vnode_start().unwrap_err();

    assert!(error.to_string().contains("refusing a fresh graph"));
    assert!(registry.is_restoring(1));
    assert_eq!(
        db.rehydrated_vnode_state.lock()[&1].chain,
        vec![Bytes::from_static(b"must-not-be-discarded")]
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
    registry.mark_restoring(&[0]);
    db.rehydrated_vnode_state.lock().insert(
        0,
        crate::db::RehydratedVnode {
            epoch: committed_attempt.epoch,
            chain: vec![Bytes::from_static(b"existing-stage")],
        },
    );

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
    assert_eq!(registry.restoring_vnodes(), vec![0]);
    assert_eq!(
        db.rehydrated_vnode_state.lock()[&0].chain,
        vec![Bytes::from_static(b"existing-stage")]
    );
    assert!(!registry.is_restoring(1));
}

#[tokio::test]
async fn boot_recovery_rejects_committed_handoff_attempt_mismatch_without_mutation() {
    assert_boot_recovery_target_mismatch_is_non_mutating(CheckpointAttempt::canonical(8), 11).await;
}

#[tokio::test]
async fn boot_recovery_rejects_sealed_assignment_mismatch_without_mutation() {
    assert_boot_recovery_target_mismatch_is_non_mutating(CheckpointAttempt::canonical(7), 12).await;
}
