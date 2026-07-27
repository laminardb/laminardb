use super::*;
use laminar_core::storage::checkpoint_manifest::OperatorCheckpoint;
use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;
#[cfg(feature = "cluster")]
use laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore;
#[cfg(feature = "cluster")]
use sha2::{Digest, Sha256};

fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
    FileSystemCheckpointStore::new(dir)
}

fn finalized_manifest(checkpoint_id: u64) -> CheckpointManifest {
    let mut manifest = CheckpointManifest::new(checkpoint_id, checkpoint_id);
    manifest.durable_phase = DurableCheckpointPhase::Finalized;
    manifest
}

fn write_raw_filesystem_manifest(
    root: &std::path::Path,
    storage_id: u64,
    manifest: &CheckpointManifest,
) {
    let checkpoint_dir = root
        .join("checkpoints")
        .join(format!("checkpoint_{storage_id:06}"));
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    std::fs::write(
        checkpoint_dir.join("manifest.json"),
        serde_json::to_vec_pretty(manifest).unwrap(),
    )
    .unwrap();
}

fn pipeline_identity(byte: u8) -> PipelineIdentity {
    PipelineIdentity {
        canonical_version: laminar_core::storage::checkpoint_manifest::PIPELINE_IDENTITY_VERSION,
        sha256: format!("{byte:02x}").repeat(32),
    }
}

#[cfg(feature = "cluster")]
fn assignment_fence(
    version: u64,
    participants: &[u64],
) -> laminar_core::checkpoint::CheckpointAssignmentFence {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let participants = participants
        .iter()
        .map(|node_id| CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                .parse()
                .unwrap(),
        })
        .collect::<Vec<_>>();
    let owners = participants
        .iter()
        .map(|participant| participant.node_id)
        .collect::<Vec<_>>();
    CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
}

#[cfg(feature = "cluster")]
struct ClusterDecisions {
    capsules: laminar_core::checkpoint_decision::CheckpointDecisionStore,
    authority: laminar_core::cluster::control::LeaderLeaseStore,
    proof: laminar_core::checkpoint::LeaderProof,
}

#[cfg(feature = "cluster")]
impl std::ops::Deref for ClusterDecisions {
    type Target = laminar_core::checkpoint_decision::CheckpointDecisionStore;

    fn deref(&self) -> &Self::Target {
        &self.capsules
    }
}

#[cfg(feature = "cluster")]
async fn cluster_decisions(
    backing: std::sync::Arc<dyn object_store::ObjectStore>,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_id: u64,
) -> ClusterDecisions {
    use laminar_core::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

    let capsules = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::clone(&backing),
    );
    capsules.load_or_create_deployment_id().await.unwrap();
    let authority = laminar_core::cluster::control::LeaderLeaseStore::new(backing, 60_000);
    let owner = LeaderLeaseOwner {
        node: laminar_core::cluster::discovery::NodeId(leader_id),
        boot: fence
            .participant_incarnation(leader_id)
            .expect("test leader belongs to the assignment certificate"),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        unreachable!("fresh cluster authority must grant its first lease")
    };
    ClusterDecisions {
        capsules,
        authority,
        proof: lease.proof(),
    }
}

async fn record_local_commit(
    store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    checkpoint_id: u64,
) {
    store
        .record_outcome(
            checkpoint_id,
            checkpoint_id,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap();
}

async fn record_local_abort(
    store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    checkpoint_id: u64,
) {
    store
        .record_outcome(
            checkpoint_id,
            checkpoint_id,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
async fn record_cluster_capsule_commit(
    store: &ClusterDecisions,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    capsule: &laminar_core::checkpoint::ClusterRecoveryCapsule,
) {
    let capsule_ref = store.create_recovery_capsule(capsule).await.unwrap();
    store
        .authority
        .record_cluster_outcome(
            &store.proof,
            capsule.attempt.epoch,
            capsule.attempt.checkpoint_id,
            fence.clone(),
            CheckpointVerdict::Commit,
            Some(capsule_ref),
        )
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
async fn record_cluster_commit(
    store: &ClusterDecisions,
    checkpoint_id: u64,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    manifest_participant_id: u64,
    manifest: Option<&CheckpointManifest>,
) {
    let deployment_id = store.load_or_create_deployment_id().await.unwrap();
    let mut synthetic_manifest = CheckpointManifest::new(checkpoint_id, checkpoint_id);
    synthetic_manifest.participant_id = manifest_participant_id;
    synthetic_manifest.deployment_id.clone_from(&deployment_id);
    let manifest = manifest.unwrap_or(&synthetic_manifest);
    #[cfg(feature = "cluster")]
    let (manifest_sha256, portable_state_sha256) =
        crate::cluster_recovery_capsule::manifest_digests(manifest).unwrap();
    #[cfg(not(feature = "cluster"))]
    let (manifest_sha256, portable_state_sha256) = ("11".repeat(32), "22".repeat(32));
    let source_offsets = manifest
        .source_offsets
        .iter()
        .map(|(source, checkpoint)| {
            (
                source.clone(),
                checkpoint.offsets.clone().into_iter().collect(),
            )
        })
        .collect();
    let source_metadata = manifest
        .source_offsets
        .iter()
        .map(|(source, checkpoint)| {
            (
                source.clone(),
                checkpoint.metadata.clone().into_iter().collect(),
            )
        })
        .collect();
    let source_assignment_versions = manifest
        .source_offsets
        .iter()
        .filter_map(|(source, checkpoint)| {
            checkpoint
                .source_assignment_version
                .map(|version| (source.clone(), version))
        })
        .collect();
    let source_watermarks = manifest
        .source_watermarks
        .iter()
        .filter(|(source, _)| manifest.source_offsets.contains_key(*source))
        .map(|(source, watermark)| (source.clone(), *watermark))
        .collect();
    let participants = fence
        .participant_ids()
        .into_iter()
        .map(
            |participant_id| laminar_core::checkpoint::ParticipantRecoveryRef {
                participant_id,
                readiness_sha256: format!("{:x}", Sha256::digest(participant_id.to_le_bytes())),
                manifest_sha256: manifest_sha256.clone(),
                portable_state_sha256: portable_state_sha256.clone(),
            },
        )
        .collect();
    let capsule = laminar_core::checkpoint::ClusterRecoveryCapsule {
        version: laminar_core::checkpoint::CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt: CheckpointAttempt::canonical(checkpoint_id),
        deployment_id,
        pipeline_identity: manifest.pipeline_identity.clone(),
        assignment_fence: fence.clone(),
        seal_inventory_sha256: "33".repeat(32),
        participants,
        source_offsets,
        source_metadata,
        source_assignment_versions,
        source_watermarks,
        cluster_watermark: manifest.watermark.map_or(
            laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
            laminar_core::checkpoint::CheckpointWatermark::Active,
        ),
        recovery_watermark_frontier: manifest.watermark,
        portable_state_sha256,
    };
    record_cluster_capsule_commit(store, fence, &capsule).await;
}

#[cfg(feature = "cluster")]
async fn record_cluster_abort(
    store: &ClusterDecisions,
    checkpoint_id: u64,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
) {
    store
        .authority
        .record_cluster_outcome(
            &store.proof,
            checkpoint_id,
            checkpoint_id,
            fence.clone(),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
async fn record_cluster_commit_for_manifests(
    store: &ClusterDecisions,
    checkpoint_id: u64,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    manifests: &[(u64, &CheckpointManifest)],
) {
    assert_eq!(
        manifests.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        fence.participant_ids()
    );
    let source_manifest = manifests.first().unwrap().1;
    let mut portable_state_sha256 = None;
    let participants = manifests
        .iter()
        .map(|(participant_id, manifest)| {
            assert_eq!(manifest.participant_id, *participant_id);
            assert_eq!(manifest.epoch, checkpoint_id);
            assert_eq!(manifest.checkpoint_id, checkpoint_id);
            let (manifest_sha256, portable) =
                crate::cluster_recovery_capsule::manifest_digests(manifest).unwrap();
            if let Some(expected) = portable_state_sha256.as_ref() {
                assert_eq!(expected, &portable);
            } else {
                portable_state_sha256 = Some(portable.clone());
            }
            laminar_core::checkpoint::ParticipantRecoveryRef {
                participant_id: *participant_id,
                readiness_sha256: format!("{:x}", Sha256::digest(participant_id.to_le_bytes())),
                manifest_sha256,
                portable_state_sha256: portable,
            }
        })
        .collect();
    let source_offsets = source_manifest
        .source_offsets
        .iter()
        .map(|(source, checkpoint)| {
            (
                source.clone(),
                checkpoint.offsets.clone().into_iter().collect(),
            )
        })
        .collect();
    let source_metadata = source_manifest
        .source_offsets
        .iter()
        .map(|(source, checkpoint)| {
            (
                source.clone(),
                checkpoint.metadata.clone().into_iter().collect(),
            )
        })
        .collect();
    let source_assignment_versions = source_manifest
        .source_offsets
        .iter()
        .filter_map(|(source, checkpoint)| {
            checkpoint
                .source_assignment_version
                .map(|version| (source.clone(), version))
        })
        .collect();
    let source_watermarks = source_manifest
        .source_watermarks
        .iter()
        .filter(|(source, _)| source_manifest.source_offsets.contains_key(*source))
        .map(|(source, watermark)| (source.clone(), *watermark))
        .collect();
    let capsule = laminar_core::checkpoint::ClusterRecoveryCapsule {
        version: laminar_core::checkpoint::CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt: CheckpointAttempt::canonical(checkpoint_id),
        deployment_id: source_manifest.deployment_id.clone(),
        pipeline_identity: source_manifest.pipeline_identity.clone(),
        assignment_fence: fence.clone(),
        seal_inventory_sha256: "33".repeat(32),
        participants,
        source_offsets,
        source_metadata,
        source_assignment_versions,
        source_watermarks,
        cluster_watermark: source_manifest.watermark.map_or(
            laminar_core::checkpoint::CheckpointWatermark::Uninitialized,
            laminar_core::checkpoint::CheckpointWatermark::Active,
        ),
        recovery_watermark_frontier: source_manifest.watermark,
        portable_state_sha256: portable_state_sha256.unwrap(),
    };
    record_cluster_capsule_commit(store, fence, &capsule).await;
}

#[tokio::test]
async fn test_recover_no_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);

    let result = mgr.recover(None).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_recover_empty_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Save a basic checkpoint
    let manifest = finalized_manifest(5);
    store.save_with_state(&manifest, None).await.unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap().unwrap();

    assert_eq!(result.epoch(), 5);
}

#[tokio::test]
async fn recover_to_epoch_picks_newest_at_or_below_target() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    for checkpoint_id in [3_u64, 5, 7] {
        store
            .save(&finalized_manifest(checkpoint_id))
            .await
            .unwrap();
    }
    let mgr = RecoveryManager::new(&store);

    assert_eq!(
        mgr.recover_to_epoch(7, None)
            .await
            .unwrap()
            .unwrap()
            .epoch(),
        7
    );
    // A newer local epoch is rewound to the cluster-agreed target.
    assert_eq!(
        mgr.recover_to_epoch(6, None)
            .await
            .unwrap()
            .unwrap()
            .epoch(),
        5
    );
    assert_eq!(
        mgr.recover_to_epoch(5, None)
            .await
            .unwrap()
            .unwrap()
            .epoch(),
        5
    );
    // Only an explicit genesis rewind may start without a checkpoint.
    assert!(mgr.recover_to_epoch(0, None).await.unwrap().is_none());
    assert!(mgr.recover_to_epoch(2, None).await.is_err());
}

#[tokio::test]
async fn recover_to_epoch_rejects_target_older_than_highest_commit_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    record_local_commit(&decisions, 5).await;
    record_local_commit(&decisions, 7).await;

    let error = RecoveryManager::new(&store)
        .recover_to_epoch(5, Some(&decisions))
        .await
        .expect_err("an older target must not rewind the durable Commit frontier");

    assert!(
        error.to_string().contains(
            "recovery target epoch 5 is not the highest durable Commit outcome: epoch 7 checkpoint 7 is authoritative"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn recover_to_genesis_rejects_finalized_inventory_without_latest_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&finalized_manifest(1)).await.unwrap();
    std::fs::remove_file(dir.path().join("checkpoints/latest.txt")).unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let error = RecoveryManager::new(&store)
        .recover_to_epoch(0, Some(&decisions))
        .await
        .expect_err("finalized inventory cannot be discarded as genesis");

    assert!(
        error
            .to_string()
            .contains("finalized checkpoint 1 epoch 1 exists in recovery inventory"),
        "{error}"
    );
}

#[tokio::test]
async fn recover_to_genesis_rejects_dangling_latest_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
    std::fs::write(
        dir.path().join("checkpoints/latest.txt"),
        "checkpoint_000099",
    )
    .unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let error = RecoveryManager::new(&store)
        .recover_to_epoch(0, Some(&decisions))
        .await
        .expect_err("a dangling recovery pointer cannot be treated as genesis");

    assert!(
        error.to_string().contains(
            "checkpoint recovery pointer is invalid while no durable Commit outcome exists"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn recover_to_genesis_allows_prepared_only_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let recovered = RecoveryManager::new(&store)
        .recover_to_epoch(0, Some(&decisions))
        .await
        .unwrap();

    assert!(recovered.is_none());
    assert_eq!(
        store.load_by_id(1).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Prepared
    );
}

#[tokio::test]
async fn test_recover_with_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let mut manifest = finalized_manifest(3);
    manifest.watermark = Some(42_000);
    store.save(&manifest).await.unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap().unwrap();

    assert_eq!(result.manifest.watermark, Some(42_000));
}

#[tokio::test]
async fn test_recover_with_operator_states() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let mut manifest = finalized_manifest(7);
    manifest
        .operator_states
        .insert("0".to_string(), OperatorCheckpoint::inline(b"window-state"));
    manifest
        .operator_states
        .insert("3".to_string(), OperatorCheckpoint::inline(b"filter-state"));
    store.save_with_state(&manifest, None).await.unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap().unwrap();

    assert_eq!(result.manifest.operator_states.len(), 2);
    let op0 = result.manifest.operator_states.get("0").unwrap();
    assert_eq!(op0.decode_inline().unwrap(), b"window-state");
}

#[tokio::test]
async fn test_recover_table_store_path() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let mut manifest = finalized_manifest(1);
    manifest.table_store_checkpoint_path = Some("/data/table_store_cp_001".into());
    store.save(&manifest).await.unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap().unwrap();

    assert_eq!(
        result.manifest.table_store_checkpoint_path.as_deref(),
        Some("/data/table_store_cp_001")
    );
}

#[tokio::test]
async fn test_recover_fallback_to_previous_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    // Save two valid checkpoints
    let mut m1 = finalized_manifest(10);
    m1.watermark = Some(1000);
    store.save(&m1).await.unwrap();

    let mut m2 = finalized_manifest(20);
    m2.watermark = Some(2000);
    store.save(&m2).await.unwrap();

    // Corrupt the latest checkpoint by writing invalid JSON
    let latest_manifest_path = dir
        .path()
        .join("checkpoints")
        .join("checkpoint_000020")
        .join("manifest.json");
    std::fs::write(&latest_manifest_path, "not valid json!!!").unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap();

    // Should fall back to checkpoint 10.
    let recovered = result.expect("should recover from fallback checkpoint");
    assert_eq!(recovered.manifest.checkpoint_id, 10);
    assert_eq!(recovered.epoch(), 10);
    assert_eq!(recovered.manifest.watermark, Some(1000));
}

#[tokio::test]
async fn irrevocable_highest_commit_never_falls_back_to_older_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut older = finalized_manifest(10);
    older.deployment_id.clone_from(&deployment_id);
    store.save(&older).await.unwrap();
    let mut committed = finalized_manifest(20);
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    record_local_commit(&decisions, 20).await;
    let committed_manifest = dir
        .path()
        .join("checkpoints")
        .join("checkpoint_000020")
        .join("manifest.json");
    std::fs::write(committed_manifest, "corrupt").unwrap();

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .expect_err("a committed checkpoint cannot rewind to checkpoint 10");
    assert!(
        error.to_string().contains(
            "[LDB-6041] committed epoch 20 checkpoint 20 participant 0 artifacts are unreadable"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn test_recover_all_checkpoints_corrupt_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    // Save a checkpoint then corrupt it
    store.save(&finalized_manifest(5)).await.unwrap();

    let manifest_path = dir
        .path()
        .join("checkpoints")
        .join("checkpoint_000005")
        .join("manifest.json");
    std::fs::write(&manifest_path, "corrupt").unwrap();

    let mgr = RecoveryManager::new(&store);
    let error = mgr.recover(None).await.unwrap_err();
    assert!(error.to_string().contains("checkpoint history exists"));
}

#[tokio::test]
async fn test_recover_latest_ok_no_fallback_needed() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());

    store.save(&finalized_manifest(10)).await.unwrap();
    store.save(&finalized_manifest(20)).await.unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap().unwrap();

    // Should use the latest (no fallback needed)
    assert_eq!(result.manifest.checkpoint_id, 20);
    assert_eq!(result.epoch(), 20);
}

#[tokio::test]
async fn test_recover_with_sidecar_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let mut manifest = finalized_manifest(5);
    let large_data = vec![0xAB; 2048];
    manifest
        .operator_states
        .insert("big-op".into(), OperatorCheckpoint::external(0, 2048));

    store
        .save_with_state(
            &manifest,
            Some(&[bytes::Bytes::copy_from_slice(&large_data)]),
        )
        .await
        .unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap().unwrap();

    // External state should have been resolved to inline
    let op = result.manifest.operator_states.get("big-op").unwrap();
    assert!(!op.external, "external state should be resolved to inline");
    assert_eq!(op.decode_inline().unwrap(), large_data);
}

#[tokio::test]
async fn test_recover_mixed_inline_and_external() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let mut manifest = finalized_manifest(3);
    // Small inline state
    manifest
        .operator_states
        .insert("small-op".into(), OperatorCheckpoint::inline(b"tiny"));
    // Large external state
    let large_data = vec![0xCD; 4096];
    manifest
        .operator_states
        .insert("big-op".into(), OperatorCheckpoint::external(0, 4096));

    store
        .save_with_state(
            &manifest,
            Some(&[bytes::Bytes::copy_from_slice(&large_data)]),
        )
        .await
        .unwrap();

    let mgr = RecoveryManager::new(&store);
    let result = mgr.recover(None).await.unwrap().unwrap();

    let small = result.manifest.operator_states.get("small-op").unwrap();
    assert_eq!(small.decode_inline().unwrap(), b"tiny");

    let big = result.manifest.operator_states.get("big-op").unwrap();
    assert_eq!(big.decode_inline().unwrap(), large_data);
}

#[tokio::test]
async fn test_recover_missing_sidecar_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Manifest references external state but sidecar is missing
    let mut manifest = finalized_manifest(1);
    manifest
        .operator_states
        .insert("orphan".into(), OperatorCheckpoint::external(0, 100));
    store
        .save_with_state(&manifest, Some(&[bytes::Bytes::from(vec![0; 100])]))
        .await
        .unwrap();
    std::fs::remove_file(dir.path().join("checkpoints/checkpoint_000001/state.bin")).unwrap();

    let mgr = RecoveryManager::new(&store);
    let error = mgr.recover(None).await.unwrap_err();
    assert!(error.to_string().contains("checkpoint history exists"));
}

#[tokio::test]
async fn prepared_manifest_without_decision_is_not_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&CheckpointManifest::new(1, 1)).await.unwrap();

    let mgr = RecoveryManager::new(&store);
    let error = mgr.recover(None).await.unwrap_err();
    assert!(error.to_string().contains("checkpoint history exists"));
}

#[tokio::test]
async fn prepared_manifest_without_outcomes_recovers_genesis() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let recovered = RecoveryManager::new(&store)
        .recover(Some(&decisions))
        .await
        .unwrap();

    assert!(recovered.is_none());
    assert_eq!(
        store.load_by_id(1).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Prepared
    );
    let settled = decisions.outcome(1).await.unwrap().unwrap();
    assert_eq!(settled.checkpoint_id, 1);
    assert_eq!(settled.verdict, CheckpointVerdict::Abort);
}

#[tokio::test]
async fn recovery_settles_newer_prepared_attempt_before_restoring_older_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut committed = CheckpointManifest::new(5, 5);
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    let mut unresolved = CheckpointManifest::new(7, 7);
    unresolved.deployment_id.clone_from(&deployment_id);
    store.save(&unresolved).await.unwrap();
    record_local_commit(&decisions, 5).await;

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        (recovered.epoch(), recovered.manifest.checkpoint_id),
        (5, 5)
    );
    let settled = decisions.outcome(7).await.unwrap().unwrap();
    assert_eq!(settled.checkpoint_id, 7);
    assert_eq!(settled.verdict, CheckpointVerdict::Abort);
}

#[tokio::test]
async fn recovery_settles_outcome_less_prepared_below_a_later_closed_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut committed = CheckpointManifest::new(5, 5);
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    let mut unresolved = CheckpointManifest::new(6, 6);
    unresolved.deployment_id.clone_from(&deployment_id);
    store.save(&unresolved).await.unwrap();
    let mut later_aborted = CheckpointManifest::new(7, 7);
    later_aborted.deployment_id.clone_from(&deployment_id);
    store.save(&later_aborted).await.unwrap();
    record_local_commit(&decisions, 5).await;
    record_local_abort(&decisions, 7).await;

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.manifest.checkpoint_id, 5);
    let settled = decisions.outcome(6).await.unwrap().unwrap();
    assert_eq!(settled.checkpoint_id, 6);
    assert_eq!(settled.verdict, CheckpointVerdict::Abort);
}

#[tokio::test]
async fn exact_commit_winner_is_accepted_when_recovery_abort_loses() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut prepared = CheckpointManifest::new(7, 7);
    prepared.deployment_id.clone_from(&deployment_id);
    store.save(&prepared).await.unwrap();
    record_local_commit(&decisions, 7).await;

    let manager = RecoveryManager::new(&store).with_deployment_id(&deployment_id);
    manager
        .settle_local_prepared_attempts(&decisions, &[])
        .await
        .expect("the exact Commit winner must defeat the stale Abort attempt");
    let recovered = manager.recover(Some(&decisions)).await.unwrap().unwrap();

    assert_eq!(recovered.manifest.checkpoint_id, 7);
    assert_eq!(
        recovered.manifest.durable_phase,
        DurableCheckpointPhase::Finalized
    );
    assert!(decisions.outcome(7).await.unwrap().unwrap().is_commit());
}

#[tokio::test]
async fn recovery_does_not_mint_abort_from_invalid_prepared_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut invalid = CheckpointManifest::new(7, 7);
    invalid.deployment_id.clone_from(&deployment_id);
    invalid.timestamp_ms = 0;
    write_raw_filesystem_manifest(dir.path(), 7, &invalid);

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .expect_err("invalid Prepared inventory cannot authorize an Abort");

    let message = error.to_string();
    assert!(
        message.contains("recovery inventory is unreadable while settling Prepared witnesses"),
        "{error}"
    );
    assert!(message.contains("timestamp_ms is 0"), "{error}");
    assert!(decisions.outcome(7).await.unwrap().is_none());
}

#[tokio::test]
async fn recovery_does_not_mint_abort_from_foreign_prepared_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let foreign_authority = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let mut foreign = CheckpointManifest::new(7, 7);
    foreign.deployment_id = foreign_authority
        .load_or_create_deployment_id()
        .await
        .unwrap();
    store.save(&foreign).await.unwrap();

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .expect_err("foreign Prepared inventory cannot authorize an Abort");

    assert!(error
        .to_string()
        .contains("active deployment/pipeline identity"));
    assert!(decisions.outcome(7).await.unwrap().is_none());
}

#[tokio::test]
async fn recovery_rejects_newer_finalized_attempt_without_exact_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut committed = CheckpointManifest::new(5, 5);
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    let mut newer = finalized_manifest(7);
    newer.deployment_id.clone_from(&deployment_id);
    store.save(&newer).await.unwrap();
    record_local_commit(&decisions, 5).await;

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .expect_err("newer Finalized state cannot be bypassed by an older Commit");

    assert!(
        error
            .to_string()
            .contains("newer Finalized checkpoint 7 epoch 7 is not settled"),
        "{error}"
    );
}

#[tokio::test]
async fn commit_followed_by_abort_restores_the_commit_cut() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let outcomes = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = outcomes.load_or_create_deployment_id().await.unwrap();

    let mut committed = CheckpointManifest::new(5, 5);
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    let mut aborted = CheckpointManifest::new(7, 7);
    aborted.deployment_id.clone_from(&deployment_id);
    store.save(&aborted).await.unwrap();
    record_local_commit(&outcomes, 5).await;
    record_local_abort(&outcomes, 7).await;

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&outcomes))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.epoch(), 5);
    assert!(matches!(
        recovered.outcome.as_ref().unwrap().verdict,
        CheckpointVerdict::Commit
    ));
}

#[tokio::test]
async fn abort_only_history_has_no_recovery_cut() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let outcomes = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = outcomes.load_or_create_deployment_id().await.unwrap();
    let mut aborted = CheckpointManifest::new(5, 5);
    aborted.deployment_id.clone_from(&deployment_id);
    store.save(&aborted).await.unwrap();
    record_local_abort(&outcomes, 5).await;

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&outcomes))
        .await
        .unwrap();

    assert!(recovered.is_none());
    assert_eq!(
        store.load_by_id(5).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Prepared
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_recovery_accepts_historical_leader_proof_structurally() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let fence = assignment_fence(4, &[1]);
    let outcomes = cluster_decisions(backing, &fence, 1).await;
    let deployment_id = outcomes.load_or_create_deployment_id().await.unwrap();
    let mut manifest = CheckpointManifest::new(7, 7);
    manifest.participant_id = 1;
    manifest.deployment_id.clone_from(&deployment_id);
    store.save(&manifest).await.unwrap();
    record_cluster_commit(&outcomes, 7, &fence, 1, Some(&manifest)).await;

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&outcomes.authority, &outcomes.capsules)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.epoch(), 7);
    assert_eq!(
        recovered
            .outcome()
            .unwrap()
            .leader_proof
            .as_ref()
            .unwrap()
            .fencing_token,
        1
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn outcome_floor_does_not_dominate_a_newer_prepared_attempt() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let offline =
        ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
            .with_participant_id(1);
    let donor = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[2]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 2).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    record_cluster_commit(&decisions, 5, &fence, 2, None).await;
    let mut retained = CheckpointManifest::new(7, 7);
    retained.participant_id = 2;
    retained.deployment_id.clone_from(&deployment_id);
    donor.save(&retained).await.unwrap();
    record_cluster_commit(&decisions, 7, &fence, 2, Some(&retained)).await;
    decisions
        .authority
        .prune_cluster_outcomes_before(&decisions.proof, 7, |_| async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(
        decisions
            .authority
            .cluster_outcome_retention_boundary()
            .await
            .unwrap()
            .artifact_before_epoch,
        7
    );
    assert!(decisions
        .authority
        .cluster_outcome(5)
        .await
        .unwrap()
        .is_none());

    let mut stale = CheckpointManifest::new(8, 8);
    stale.participant_id = 1;
    stale.deployment_id.clone_from(&deployment_id);
    offline.save(&stale).await.unwrap();

    let error = RecoveryManager::new(&offline)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .expect_err("an outcome floor cannot settle a newer Prepared attempt");
    assert!(
        error
            .to_string()
            .contains("checkpoint 8 epoch 8 is not settled by an exact terminal outcome"),
        "{error}"
    );
    assert_eq!(
        offline.load_by_id(8).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Prepared
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn later_terminal_strictly_dominates_missing_prepared_attempt() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let fence = assignment_fence(4, &[1]);
    let decisions = cluster_decisions(backing, &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut committed = CheckpointManifest::new(5, 5);
    committed.participant_id = 1;
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    record_cluster_commit(&decisions, 5, &fence, 1, Some(&committed)).await;
    let mut prepared = CheckpointManifest::new(6, 6);
    prepared.participant_id = 1;
    prepared.deployment_id.clone_from(&deployment_id);
    store.save(&prepared).await.unwrap();
    record_cluster_abort(&decisions, 7, &fence).await;

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        (recovered.epoch(), recovered.manifest.checkpoint_id),
        (5, 5)
    );
    assert_eq!(
        store.load_by_id(6).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Prepared
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn newer_missing_prepared_attempt_remains_fatal() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let fence = assignment_fence(4, &[1]);
    let decisions = cluster_decisions(backing, &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut committed = CheckpointManifest::new(5, 5);
    committed.participant_id = 1;
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    record_cluster_commit(&decisions, 5, &fence, 1, Some(&committed)).await;
    let mut newer = CheckpointManifest::new(8, 8);
    newer.participant_id = 1;
    newer.deployment_id.clone_from(&deployment_id);
    store.save(&newer).await.unwrap();
    record_cluster_abort(&decisions, 7, &fence).await;

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .expect_err("a newer Prepared attempt cannot be inferred closed");

    assert!(
        error
            .to_string()
            .contains("checkpoint 8 epoch 8 is not settled by an exact terminal outcome"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn dominated_prepared_attempt_with_foreign_provenance_remains_fatal() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let fence = assignment_fence(4, &[1]);
    let decisions = cluster_decisions(backing, &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut committed = CheckpointManifest::new(5, 5);
    committed.participant_id = 1;
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    record_cluster_commit(&decisions, 5, &fence, 1, Some(&committed)).await;
    let mut foreign = CheckpointManifest::new(6, 6);
    foreign.participant_id = 1;
    foreign.deployment_id = uuid::Uuid::new_v4().to_string();
    store.save(&foreign).await.unwrap();
    record_cluster_abort(&decisions, 7, &fence).await;

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .expect_err("dominance cannot hide foreign recovery inventory");

    assert!(
        error.to_string().contains(
            "does not belong to storage participant 1 and the active deployment/pipeline identity"
        ),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_recovery_ignores_forged_standalone_outcome_keys() {
    use object_store::{ObjectStoreExt, PutPayload};

    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let store = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let fence = assignment_fence(4, &[1]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut manifest = CheckpointManifest::new(7, 7);
    manifest.participant_id = 1;
    manifest.deployment_id.clone_from(&deployment_id);
    store.save(&manifest).await.unwrap();
    record_cluster_commit(&decisions, 7, &fence, 1, Some(&manifest)).await;

    let mut forged = decisions
        .authority
        .cluster_outcome(7)
        .await
        .unwrap()
        .unwrap();
    forged.epoch = 9;
    forged.checkpoint_id = 99;
    backing
        .put(
            &object_store::path::Path::from("checkpoint-outcomes/epoch=9/outcome"),
            PutPayload::from_bytes(bytes::Bytes::from(serde_json::to_vec(&forged).unwrap())),
        )
        .await
        .unwrap();

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (recovered.epoch(), recovered.manifest.checkpoint_id),
        (7, 7)
    );
}

#[tokio::test]
async fn published_finalized_manifest_fails_without_commit_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&finalized_manifest(1)).await.unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let error = RecoveryManager::new(&store)
        .recover(Some(&decisions))
        .await
        .unwrap_err();

    assert!(
        error.to_string().contains(
            "published finalized checkpoint 1 epoch 1 exists but no durable Commit outcome exists"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn finalized_manifest_without_latest_pointer_fails_without_commit_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&finalized_manifest(1)).await.unwrap();
    std::fs::remove_file(dir.path().join("checkpoints/latest.txt")).unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let error = RecoveryManager::new(&store)
        .recover(Some(&decisions))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("finalized checkpoint 1 epoch 1 exists in recovery inventory"),
        "{error}"
    );
}

#[tokio::test]
async fn dangling_latest_pointer_fails_without_commit_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
    std::fs::write(
        dir.path().join("checkpoints/latest.txt"),
        "checkpoint_000099",
    )
    .unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let error = RecoveryManager::new(&store)
        .recover(Some(&decisions))
        .await
        .unwrap_err();

    assert!(
        error.to_string().contains(
            "checkpoint recovery pointer is invalid while no durable Commit outcome exists"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn unreadable_manifest_fails_without_commit_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
    std::fs::write(
        dir.path()
            .join("checkpoints/checkpoint_000001/manifest.json"),
        b"not-json",
    )
    .unwrap();
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );

    let error = RecoveryManager::new(&store)
        .recover(Some(&decisions))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("checkpoint 1 recovery inventory is unreadable"),
        "{error}"
    );
}

#[tokio::test]
async fn committed_prepared_manifest_is_finalized_and_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut prepared = CheckpointManifest::new(7, 7);
    prepared.deployment_id.clone_from(&deployment_id);
    store.save(&prepared).await.unwrap();
    record_local_commit(&decisions, 7).await;

    let recovered = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recovered.manifest.durable_phase,
        DurableCheckpointPhase::Finalized
    );
    assert_eq!(recovered.epoch(), 7);
    assert_eq!(
        store.load_by_id(7).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Finalized
    );
}

#[tokio::test]
async fn finalized_manifest_requires_exact_commit_outcome_when_store_is_configured() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut manifest = finalized_manifest(1);
    manifest.deployment_id.clone_from(&deployment_id);
    store.save(&manifest).await.unwrap();

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .recover(Some(&decisions))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("published finalized checkpoint 1 epoch 1 exists"));
}

#[tokio::test]
async fn exact_outcome_must_match_manifest_deployment() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let decision_deployment = decisions.load_or_create_deployment_id().await.unwrap();
    let other_namespace = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let manifest_deployment = other_namespace
        .load_or_create_deployment_id()
        .await
        .unwrap();
    assert_ne!(decision_deployment, manifest_deployment);

    let mut manifest = finalized_manifest(1);
    manifest.deployment_id.clone_from(&manifest_deployment);
    store.save(&manifest).await.unwrap();
    record_local_commit(&decisions, 1).await;

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&manifest_deployment)
        .recover(Some(&decisions))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("local decision authority deployment"),
        "{error}"
    );
}

#[tokio::test]
async fn pipeline_identity_mismatch_is_fatal_without_older_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let expected = pipeline_identity(0x11);

    let mut older = finalized_manifest(1);
    older.pipeline_identity = expected.clone();
    store.save(&older).await.unwrap();

    let mut latest = finalized_manifest(2);
    latest.pipeline_identity = pipeline_identity(0x22);
    store.save(&latest).await.unwrap();

    let error = RecoveryManager::new(&store)
        .with_pipeline_identity(&expected)
        .recover(None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("[LDB-6043]"));
    assert!(error.to_string().contains("checkpoint 2"));
}

#[tokio::test]
async fn identity_mismatch_does_not_finalize_committed_prepared_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut prepared = CheckpointManifest::new(7, 7);
    prepared.deployment_id.clone_from(&deployment_id);
    prepared.pipeline_identity = pipeline_identity(0x33);
    store.save(&prepared).await.unwrap();

    record_local_commit(&decisions, 7).await;

    let error = RecoveryManager::new(&store)
        .with_deployment_id(&deployment_id)
        .with_pipeline_identity(&pipeline_identity(0x44))
        .recover(Some(&decisions))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("[LDB-6043]"));
    let persisted = store.load_by_id(7).await.unwrap().unwrap();
    assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
}

#[tokio::test]
async fn outcome_scope_mismatch_does_not_finalize_prepared_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let prepared = CheckpointManifest::new(7, 7);
    store.save(&prepared).await.unwrap();

    let decisions = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
    );
    record_local_commit(&decisions, 7).await;

    let error = RecoveryManager::new(&store)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover(Some(&decisions))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("recovery authority scope Local"));
    let persisted = store.load_by_id(7).await.unwrap().unwrap();
    assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn local_recovery_rejects_cluster_outcome_before_manifest_access() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let fence = assignment_fence(4, &[1]);
    let decisions = cluster_decisions(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
        &fence,
        1,
    )
    .await;
    record_cluster_commit(&decisions, 7, &fence, 1, None).await;

    let error = RecoveryManager::new(&store)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("recovery authority scope Cluster"));
    assert!(error.to_string().contains("active runtime scope Local"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn excluded_participant_recovers_exact_portable_peer_manifest() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let donor = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 3]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut manifest = CheckpointManifest::new(7, 7);
    manifest.participant_id = 1;
    manifest.deployment_id.clone_from(&deployment_id);
    manifest.source_offsets.insert(
        "events".into(),
        ConnectorCheckpoint {
            offsets: HashMap::from([("partition:0".into(), "41".into())]),
            metadata: HashMap::from([("topic".into(), "events".into())]),
            source_assignment_version: std::num::NonZeroU64::new(4),
        },
    );
    manifest.source_names.push("events".into());
    manifest.source_watermarks.insert("events".into(), 42_000);
    manifest.watermark = Some(40_000);
    manifest
        .operator_states
        .insert("global".into(), OperatorCheckpoint::external(0, 5));
    let manifest = donor
        .save_with_state(&manifest, Some(&[Bytes::from_static(b"state")]))
        .await
        .unwrap();
    record_cluster_commit(&decisions, 7, &fence, 1, Some(&manifest)).await;

    let manager = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster);
    let recovered = manager
        .recover_cluster_to_epoch(7, &decisions.authority, &decisions.capsules)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.manifest.participant_id, 1);
    assert_eq!(
        recovered.manifest.source_offsets["events"].offsets["partition:0"],
        "41"
    );
    assert_eq!(
        recovered.manifest.source_offsets["events"].metadata["topic"],
        "events"
    );
    assert_eq!(recovered.manifest.source_watermarks["events"], 42_000);
    assert_eq!(recovered.manifest.watermark, Some(40_000));
    assert_eq!(
        recovered.manifest.operator_states["global"]
            .decode_inline()
            .unwrap(),
        b"state"
    );
    assert_eq!(
        recovered
            .cluster_capsule()
            .unwrap()
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(
        donor.load_by_id(7).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Prepared,
        "peer recovery must not rewrite another participant's recovery pointer"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_recovery_prefers_the_local_replica_over_a_valid_lower_id_peer() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let peer = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 2]);
    let decisions = cluster_decisions(backing, &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut participant_1 = CheckpointManifest::new(7, 7);
    participant_1.participant_id = 1;
    participant_1.deployment_id.clone_from(&deployment_id);
    let mut participant_2 = participant_1.clone();
    participant_2.participant_id = 2;
    peer.save(&participant_1).await.unwrap();
    local.save(&participant_2).await.unwrap();

    record_cluster_commit_for_manifests(
        &decisions,
        7,
        &fence,
        &[(1, &participant_1), (2, &participant_2)],
    )
    .await;

    let recovered = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.manifest.participant_id, 2);
    assert_eq!(
        local.load_by_id(7).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Finalized
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_recovery_uses_a_peer_when_the_local_sidecar_is_corrupt() {
    use object_store::{PutOptions, PutPayload};

    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let peer = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 2]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut participant_1 = CheckpointManifest::new(7, 7);
    participant_1.participant_id = 1;
    participant_1.deployment_id.clone_from(&deployment_id);
    participant_1
        .operator_states
        .insert("global".into(), OperatorCheckpoint::external(0, 5));
    let mut participant_2 = participant_1.clone();
    participant_2.participant_id = 2;
    let participant_1 = peer
        .save_with_state(&participant_1, Some(&[Bytes::from_static(b"state")]))
        .await
        .unwrap();
    let participant_2 = local
        .save_with_state(&participant_2, Some(&[Bytes::from_static(b"state")]))
        .await
        .unwrap();

    record_cluster_commit_for_manifests(
        &decisions,
        7,
        &fence,
        &[(1, &participant_1), (2, &participant_2)],
    )
    .await;
    backing
        .put_opts(
            &object_store::path::Path::from("nodes/2/checkpoints/state-000007.bin"),
            PutPayload::from_bytes(Bytes::from_static(b"other")),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let recovered = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.manifest.participant_id, 1);
    assert_eq!(
        recovered.manifest.operator_states["global"]
            .decode_inline()
            .unwrap(),
        b"state"
    );
    assert_eq!(
        peer.load_by_id(7).await.unwrap().unwrap().durable_phase,
        DurableCheckpointPhase::Prepared,
        "peer recovery must not publish the peer's prepared manifest"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn retention_preflight_uses_a_complete_peer_and_rejects_all_missing_sidecars() {
    use object_store::ObjectStoreExt;

    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let peer = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 2]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut participant_1 = CheckpointManifest::new(7, 7);
    participant_1.participant_id = 1;
    participant_1.deployment_id.clone_from(&deployment_id);
    participant_1
        .operator_states
        .insert("global".into(), OperatorCheckpoint::external(0, 5));
    let mut participant_2 = participant_1.clone();
    participant_2.participant_id = 2;
    let participant_1 = peer
        .save_with_state(&participant_1, Some(&[Bytes::from_static(b"state")]))
        .await
        .unwrap();
    let participant_2 = local
        .save_with_state(&participant_2, Some(&[Bytes::from_static(b"state")]))
        .await
        .unwrap();
    record_cluster_commit_for_manifests(
        &decisions,
        7,
        &fence,
        &[(1, &participant_1), (2, &participant_2)],
    )
    .await;
    let outcome = decisions
        .authority
        .cluster_outcome(7)
        .await
        .unwrap()
        .expect("cluster Commit outcome");
    let capsule = decisions
        .load_recovery_capsule(outcome.recovery_capsule.as_ref().unwrap())
        .await
        .unwrap();
    let manager = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster);

    backing
        .delete(&object_store::path::Path::from(
            "nodes/2/checkpoints/state-000007.bin",
        ))
        .await
        .unwrap();
    manager
        .preflight_cluster_committed_metadata(&outcome, &capsule)
        .await
        .expect("a complete peer sidecar must preserve the recovery cut");

    backing
        .delete(&object_store::path::Path::from(
            "nodes/1/checkpoints/state-000007.bin",
        ))
        .await
        .unwrap();
    let error = manager
        .preflight_cluster_committed_metadata(&outcome, &capsule)
        .await
        .expect_err("retention must fail when every participant sidecar is missing");
    assert!(
        error
            .to_string()
            .contains("no usable participant manifest metadata; 2 candidate(s) rejected"),
        "{error}"
    );
    assert!(error.to_string().contains("sidecar is absent"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_recovery_never_combines_a_manifest_and_sidecar_from_different_participants() {
    use object_store::ObjectStoreExt;

    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let peer = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 2]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();

    let mut participant_1 = CheckpointManifest::new(7, 7);
    participant_1.participant_id = 1;
    participant_1.deployment_id.clone_from(&deployment_id);
    participant_1
        .operator_states
        .insert("global".into(), OperatorCheckpoint::external(0, 5));
    let mut participant_2 = participant_1.clone();
    participant_2.participant_id = 2;
    let participant_1 = peer
        .save_with_state(&participant_1, Some(&[Bytes::from_static(b"state")]))
        .await
        .unwrap();
    let participant_2 = local
        .save_with_state(&participant_2, Some(&[Bytes::from_static(b"state")]))
        .await
        .unwrap();

    record_cluster_commit_for_manifests(
        &decisions,
        7,
        &fence,
        &[(1, &participant_1), (2, &participant_2)],
    )
    .await;
    backing
        .delete(&object_store::path::Path::from(
            "nodes/1/checkpoints/state-000007.bin",
        ))
        .await
        .unwrap();
    backing
        .delete(&object_store::path::Path::from(
            "nodes/2/manifests/manifest-000007.json",
        ))
        .await
        .unwrap();

    let error = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .expect_err("recovery must keep each participant's artifacts paired");

    assert!(
        error
            .to_string()
            .contains("no usable participant artifact replica; 2 candidate(s) rejected"),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("first failure: participant 2 manifest is absent"),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("last failure: participant 1 artifact integrity failed"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_recovery_rejects_changed_only_artifact_replica() {
    use object_store::{PutOptions, PutPayload};

    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let donor = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/1/".into())
        .with_participant_id(1);
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 3]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut manifest = CheckpointManifest::new(7, 7);
    manifest.participant_id = 1;
    manifest.deployment_id.clone_from(&deployment_id);
    donor.save(&manifest).await.unwrap();
    record_cluster_commit(&decisions, 7, &fence, 1, Some(&manifest)).await;
    manifest.source_watermarks.insert("events".into(), 42_000);
    backing
        .put_opts(
            &object_store::path::Path::from("nodes/1/manifests/manifest-000007.json"),
            PutPayload::from_bytes(bytes::Bytes::from(
                serde_json::to_vec_pretty(&manifest).unwrap(),
            )),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let error = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .expect_err("the capsule must reject a changed participant manifest");

    assert!(
        error
            .to_string()
            .contains("participant 1 manifest digest does not match the recovery capsule"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn committed_peer_manifest_must_match_the_exact_attempt() {
    use object_store::{ObjectStoreExt, PutPayload};

    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 3]);
    let decisions = cluster_decisions(std::sync::Arc::clone(&backing), &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let mut wrong_attempt = CheckpointManifest::new(8, 8);
    wrong_attempt.participant_id = 1;
    wrong_attempt.deployment_id.clone_from(&deployment_id);
    backing
        .put(
            &object_store::path::Path::from("nodes/1/manifests/manifest-000007.json"),
            PutPayload::from_bytes(bytes::Bytes::from(
                serde_json::to_vec_pretty(&wrong_attempt).unwrap(),
            )),
        )
        .await
        .unwrap();
    record_cluster_commit(&decisions, 7, &fence, 1, Some(&wrong_attempt)).await;

    let error = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster_to_epoch(7, &decisions.authority, &decisions.capsules)
        .await
        .expect_err("outcome and donor manifest must identify one exact attempt");

    assert!(
        error
            .to_string()
            .contains("storage checkpoint 7 contains manifest checkpoint 8"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn all_capsule_participant_artifacts_missing_fails_closed() {
    let backing: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let local = ObjectStoreCheckpointStore::new(std::sync::Arc::clone(&backing), "nodes/2/".into())
        .with_participant_id(2);
    let fence = assignment_fence(4, &[1, 2]);
    let decisions = cluster_decisions(backing, &fence, 1).await;
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    record_cluster_commit(&decisions, 7, &fence, 1, None).await;

    let error = RecoveryManager::new(&local)
        .with_deployment_id(&deployment_id)
        .with_outcome_scope(CheckpointScope::Cluster)
        .recover_cluster(&decisions.authority, &decisions.capsules)
        .await
        .expect_err("a Commit cannot fall back to genesis when every replica is absent");

    assert!(
        error
            .to_string()
            .contains("no usable participant artifact replica; 2 candidate(s) rejected"),
        "{error}"
    );
}
