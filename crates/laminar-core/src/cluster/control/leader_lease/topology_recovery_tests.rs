//! Recovery selection uses the actual authority Commit and immutable checkpoint indexes.

use super::*;
use crate::cluster::control::{TopologyRecoveryCut, TopologyRecoveryInput};

#[path = "topology_recovery_round_tests.rs"]
mod rounds;

async fn select(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    process: LocalProcessAuthorityIdentity,
) -> Result<TopologyRecoveryInput, TopologyError> {
    authority
        .committed_topology_recovery_input(
            &fixture.assignments,
            &topology_preparation::processes(authority),
            fixture.operation.operation_id,
            process,
        )
        .await
}

async fn active(authority: &LeaderLeaseStore) -> (Fixture, TopologyRestoreInput) {
    let (fixture, input) = committed(authority).await;
    let input = install_all(authority, &fixture, &input).await;
    release(authority, &fixture, &input).await.unwrap();
    let input = reconstruct(authority, &fixture, input.process())
        .await
        .unwrap();
    (fixture, input)
}

async fn target_checkpoint(
    authority: &LeaderLeaseStore,
    fixture: &Fixture,
    input: &TopologyRestoreInput,
    epoch: u64,
) -> (CheckpointOutcome, CommittedCheckpointIndex) {
    let mut inventory = input.operation().cut.as_ref().unwrap().inventory.clone();
    inventory.attempt = crate::checkpoint::CheckpointAttempt::canonical(epoch);
    inventory.pipeline_identity = input.descriptor().target_pipeline.clone();
    authority
        .begin_cluster_checkpoint_artifacts(&input.current_leader().unwrap(), inventory)
        .await
        .unwrap();
    let previous = authority
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .unwrap();
    let mut index = fixture.index.clone();
    index.epoch = epoch;
    index.checkpoint_id = epoch;
    index.pipeline_identity = input.descriptor().target_pipeline.clone();
    index.predecessor = previous.committed_checkpoint;
    for checkpoint in index.source_offsets.values_mut() {
        for offset in checkpoint.offsets.values_mut() {
            *offset = (100 + epoch).to_string();
        }
    }
    index.source_watermarks.insert("events".into(), 100);
    index.checkpoint_watermark = Some(100);
    for channel in &mut index.channel_progress {
        channel.watermark = Some(100);
    }
    let reference = authority.create_committed_checkpoint(&index).await.unwrap();
    authority
        .record_cluster_outcome(
            &input.current_leader().unwrap(),
            epoch,
            epoch,
            index.assignment_fence.clone().unwrap(),
            CheckpointVerdict::Commit,
            Some(reference),
        )
        .await
        .unwrap();
    let (outcome, selected) = authority
        .cluster_outcome_with_committed_checkpoint(epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.as_ref(), Some(&index));
    (outcome, index)
}

#[tokio::test]
async fn topology_recovery_selection_uses_exact_root_before_first_target_checkpoint() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let before = authority.load_record().await.unwrap();
    let selection = select(&authority, &fixture, input.process()).await.unwrap();
    assert_eq!(selection.cut(), TopologyRecoveryCut::MigrationRoot);
    assert_eq!(selection.checkpoint(), &fixture.index);
    assert_eq!(selection.outcome(), input.outcome());
    assert_eq!(selection.migration(), &input);
    assert_eq!(authority.load_record().await.unwrap(), before);
    // Selection cannot authorize either the historical pipeline or a replacement runtime.
    let admission = authority.recovery_admission_snapshot().await.unwrap();
    assert_eq!(
        admission.topology_commit(),
        input.operation().commit.as_ref()
    );
    assert!(!authority
        .recovery_admission_is_current(&admission, &fixture.lease.proof())
        .await
        .unwrap());
    assert!(!authority
        .authorize_topology_release(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            &input,
            Uuid::from_u128(901),
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn topology_recovery_selection_prefers_greatest_target_checkpoint_and_progress() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let root = select(&authority, &fixture, input.process()).await.unwrap();
    assert_eq!(root.cut(), TopologyRecoveryCut::MigrationRoot);
    target_checkpoint(&authority, &fixture, &input, 2).await;
    let earlier = select(&authority, &fixture, input.process()).await.unwrap();
    let (outcome, index) = target_checkpoint(&authority, &fixture, &input, 3).await;
    let before = authority.load_record().await.unwrap();
    let selection = select(&authority, &fixture, input.process()).await.unwrap();
    assert_eq!(selection.cut(), TopologyRecoveryCut::TargetCheckpoint);
    assert_eq!(selection.checkpoint(), &index);
    assert_eq!(selection.outcome(), &outcome);
    assert_eq!(
        selection.checkpoint().pipeline_identity,
        input.descriptor().target_pipeline
    );
    assert_eq!(selection.migration().checkpoint(), &fixture.index);
    assert_eq!(
        selection.checkpoint().source_offsets["events"].offsets["partition-0"],
        "103"
    );
    assert_eq!(selection.checkpoint().checkpoint_watermark, Some(100));
    assert!(!selection.same_restore_requirements(&root));
    assert!(!selection.same_restore_requirements(&earlier));
    assert!(selection
        .same_restore_requirements(&select(&authority, &fixture, input.process()).await.unwrap()));
    assert_eq!(authority.load_record().await.unwrap(), before);
}

#[tokio::test]
async fn topology_recovery_selection_damaged_target_index_never_falls_back_to_root() {
    for corrupt in [false, true] {
        let authority = store(30_000);
        let (fixture, input) = active(&authority).await;
        let (outcome, _) = target_checkpoint(&authority, &fixture, &input, 2).await;
        let reference = outcome.committed_checkpoint.unwrap();
        let path = committed_checkpoint_path(&reference);
        if corrupt {
            authority
                .store
                .put(&path, Bytes::from_static(b"corrupt target index").into())
                .await
                .unwrap();
        } else {
            authority.store.delete(&path).await.unwrap();
        }
        let before = authority.load_record().await.unwrap();
        assert!(select(&authority, &fixture, input.process()).await.is_err());
        assert_eq!(authority.load_record().await.unwrap(), before);
        assert!(reconstruct(&authority, &fixture, input.process())
            .await
            .is_ok());
    }
}

#[tokio::test]
async fn topology_recovery_selection_does_not_rewind_for_later_abort() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let (_, index) = target_checkpoint(&authority, &fixture, &input, 2).await;
    let mut inventory = input.operation().cut.as_ref().unwrap().inventory.clone();
    inventory.attempt = crate::checkpoint::CheckpointAttempt::canonical(3);
    inventory.pipeline_identity = input.descriptor().target_pipeline.clone();
    authority
        .begin_cluster_checkpoint_artifacts(&input.current_leader().unwrap(), inventory)
        .await
        .unwrap();
    authority
        .record_cluster_outcome(
            &input.current_leader().unwrap(),
            3,
            3,
            input.assignment().clone(),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    let selection = select(&authority, &fixture, input.process()).await.unwrap();
    assert_eq!(selection.cut(), TopologyRecoveryCut::TargetCheckpoint);
    assert_eq!(selection.checkpoint(), &index);
}

#[tokio::test]
async fn topology_recovery_selection_new_boot_retains_target_cut_and_rejects_original_release() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let (_, index) = target_checkpoint(&authority, &fixture, &input, 2).await;
    let processes =
        crate::cluster::control::ProcessLeaseStore::new(authority.store.clone(), NodeId(2), 30_000);
    let prior = processes.load().await.unwrap().unwrap();
    let observed = processes.observe_rival(&prior).unwrap();
    tokio::time::sleep(Duration::from_millis(30_001)).await;
    let crate::cluster::control::ProcessLeaseOutcome::Acquired(replacement) = processes
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
    fixture
        .assignments
        .save_if_version(&assignment, before.version)
        .await
        .unwrap();
    let process = LocalProcessAuthorityIdentity {
        participant: assignment.participants[1],
        process_term: replacement.term,
    };
    let selection = select(&authority, &fixture, process).await.unwrap();
    assert_eq!(selection.checkpoint(), &index);
    assert_eq!(selection.migration().process(), process);
    assert_eq!(
        selection.migration().assignment().assignment_version,
        assignment.version
    );
    assert_eq!(
        selection.checkpoint().assignment_fence.as_ref(),
        Some(&before.assignment_fence().unwrap())
    );
    assert!(!authority
        .authorize_topology_release(
            &fixture.assignments,
            &topology_preparation::processes(&authority),
            selection.migration(),
            Uuid::from_u128(2),
        )
        .await
        .unwrap());
    assert!(select(
        &authority,
        &fixture,
        LocalProcessAuthorityIdentity {
            participant: before.participants[1],
            process_term: prior.term,
        }
    )
    .await
    .is_err());
}

#[tokio::test]
async fn topology_recovery_selection_new_commit_during_read_requires_fresh_selection() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let (outcome, _) = target_checkpoint(&authority, &fixture, &input, 2).await;
    let (raw, wrapped) = blocking_get_once_with_inner(
        30_000,
        authority.store.clone(),
        committed_checkpoint_path(outcome.committed_checkpoint.as_ref().unwrap()),
    );
    let reader = Arc::clone(&wrapped);
    let operation = input.operation().operation_id;
    let process = input.process();
    let pending = tokio::spawn(async move {
        reader
            .committed_topology_recovery_input(
                &AssignmentSnapshotStore::new(reader.store.clone()),
                &topology_preparation::processes(&reader),
                operation,
                process,
            )
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    let (_, latest) = target_checkpoint(&authority, &fixture, &input, 3).await;
    raw.release.add_permits(1);
    assert!(matches!(pending.await.unwrap(), Err(TopologyError::Fenced)));
    assert_eq!(
        select(&wrapped, &fixture, process)
            .await
            .unwrap()
            .checkpoint(),
        &latest
    );
}

#[tokio::test(start_paused = true)]
async fn topology_recovery_selection_deadline_and_cancellation_never_mutate_authority() {
    for cancel in [false, true] {
        let authority = store(30_000);
        let (fixture, input) = active(&authority).await;
        let (outcome, index) = target_checkpoint(&authority, &fixture, &input, 2).await;
        let (raw, wrapped) = blocking_get_once_with_inner(
            30_000,
            authority.store.clone(),
            committed_checkpoint_path(outcome.committed_checkpoint.as_ref().unwrap()),
        );
        let before = authority.load_record().await.unwrap();
        let reader = Arc::clone(&wrapped);
        let operation = input.operation().operation_id;
        let process = input.process();
        let pending = tokio::spawn(async move {
            reader
                .committed_topology_recovery_input(
                    &AssignmentSnapshotStore::new(reader.store.clone()),
                    &topology_preparation::processes(&reader),
                    operation,
                    process,
                )
                .await
        });
        raw.entered.acquire().await.unwrap().forget();
        if cancel {
            pending.abort();
            assert!(pending.await.unwrap_err().is_cancelled());
        } else {
            tokio::time::advance(Duration::from_secs(16)).await;
            assert!(matches!(
                pending.await.unwrap(),
                Err(TopologyError::ReadTimedOut)
            ));
        }
        assert_eq!(authority.load_record().await.unwrap(), before);
        assert_eq!(
            select(&wrapped, &fixture, process)
                .await
                .unwrap()
                .checkpoint(),
            &index
        );
    }
}

#[tokio::test]
async fn topology_recovery_selection_checkpoint_transition_requires_exact_released_mapping() {
    let authority = store(30_000);
    let (fixture, input) = committed(&authority).await;
    let mut target = fixture.index.clone();
    target.epoch = 2;
    target.checkpoint_id = 2;
    target.pipeline_identity = input.descriptor().target_pipeline.clone();
    target.predecessor = input.outcome().committed_checkpoint.clone();
    assert!(target.validate_predecessor_index(&fixture.index).is_err());
    assert!(authority
        .validate_cluster_checkpoint_predecessor(&target, &fixture.index)
        .await
        .is_err());
    let input = install_all(&authority, &fixture, &input).await;
    release(&authority, &fixture, &input).await.unwrap();
    let before = authority.load_record().await.unwrap();
    assert_eq!(
        authority
            .validate_cluster_checkpoint_predecessor(&target, &fixture.index)
            .await
            .unwrap()
            .as_ref(),
        Some(input.root())
    );
    for case in 0..6 {
        let mut changed = target.clone();
        match case {
            0 => changed.pipeline_identity.sha256 = digest(77),
            1 => changed.source_names.clear(),
            2 => changed.predecessor.as_mut().unwrap().sha256 = digest(88),
            3 => changed.assignment_fence.as_mut().unwrap().assignment_digest = [99; 32],
            4 => {
                changed.source_watermarks.insert("events".into(), 9);
            }
            _ => changed.deployment_id = Uuid::from_u128(999).to_string(),
        }
        assert!(
            authority
                .validate_cluster_checkpoint_predecessor(&changed, &fixture.index)
                .await
                .is_err(),
            "case {case}"
        );
    }
    assert_eq!(authority.load_record().await.unwrap(), before);
    assert!(target.validate_predecessor_index(&fixture.index).is_err());
}

#[tokio::test]
async fn topology_cleanup_reclaims_target_checkpoints_but_retains_exact_root() {
    let authority = store(30_000);
    let (fixture, input) = active(&authority).await;
    let root = input.root().cut.checkpoint.clone();
    let (first, _) = target_checkpoint(&authority, &fixture, &input, 2).await;
    let proof = input.current_leader().unwrap();
    assert!(authority
        .begin_cluster_artifact_cleanup(
            &proof,
            first.committed_checkpoint.clone().unwrap(),
            |_| async { Ok(()) }
        )
        .await
        .unwrap()
        .is_none());
    let (protected, _) = target_checkpoint(&authority, &fixture, &input, 3).await;
    target_checkpoint(&authority, &fixture, &input, 4).await;
    let protected = protected.committed_checkpoint.unwrap();
    let cursor = authority
        .begin_cluster_artifact_cleanup(&proof, protected.clone(), |_| async { Ok(()) })
        .await
        .unwrap()
        .expect("obsolete target checkpoint must be reclaimable");
    assert_eq!(cursor.current, first.committed_checkpoint.unwrap());
    assert_eq!(cursor.next, Some(root.clone()));
    assert_eq!(cursor.stop_before, Some(root.clone()));
    let cursor = authority
        .mark_cluster_artifact_data_deleted(&proof, &cursor)
        .await
        .unwrap();
    CheckpointDecisionStore::new(authority.store.clone())
        .delete_committed_checkpoint(&cursor.current)
        .await
        .unwrap();
    assert!(authority
        .mark_cluster_artifact_metadata_deleted(&proof, &cursor)
        .await
        .unwrap()
        .is_none());
    assert!(authority
        .cluster_outcome_with_committed_checkpoint(root.epoch)
        .await
        .unwrap()
        .is_none());
    let selection = select(&authority, &fixture, input.process()).await.unwrap();
    assert_eq!(selection.cut(), TopologyRecoveryCut::TargetCheckpoint);
    assert_eq!(selection.checkpoint().epoch, 4);
    assert_eq!(selection.migration().checkpoint(), &fixture.index);
    assert_eq!(
        CheckpointDecisionStore::new(authority.store.clone())
            .load_committed_checkpoint(&root)
            .await
            .unwrap(),
        fixture.index
    );
}
