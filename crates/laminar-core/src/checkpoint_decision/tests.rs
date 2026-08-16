use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::memory::InMemory;

use super::*;
use crate::checkpoint::{
    CommittedCheckpointIndex, CommittedParticipantRef, PipelineIdentity,
    COMMITTED_CHECKPOINT_INDEX_VERSION,
};
use crate::state::LOCAL_NODE_ID;

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

async fn local_index(
    store: &CheckpointDecisionStore,
    checkpoint_id: u64,
    predecessor: Option<CommittedCheckpointRef>,
) -> CommittedCheckpointIndex {
    CommittedCheckpointIndex {
        version: COMMITTED_CHECKPOINT_INDEX_VERSION,
        deployment_id: store.load_or_create_deployment_id().await.unwrap(),
        pipeline_identity: PipelineIdentity::empty(),
        epoch: checkpoint_id,
        checkpoint_id,
        scope: CheckpointScope::Local,
        vnode_count: 4,
        assignment_fence: None,
        reassignment_portable: false,
        predecessor,
        participants: vec![CommittedParticipantRef {
            participant_id: LOCAL_NODE_ID.0,
            manifest_len: 12,
            manifest_sha256: digest(1),
            node_data_len: 0,
            node_data_sha256: digest(2),
        }],
        source_names: Vec::new(),
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        source_watermarks: BTreeMap::new(),
        checkpoint_watermark: None,
    }
}

async fn local_artifact_inventory(
    store: &CheckpointDecisionStore,
    checkpoint_id: u64,
) -> CheckpointArtifactInventory {
    CheckpointArtifactInventory {
        deployment_id: store.load_or_create_deployment_id().await.unwrap(),
        pipeline_identity: PipelineIdentity::empty(),
        attempt: CheckpointAttempt::canonical(checkpoint_id),
        assignment_fence: None,
    }
}

async fn begin_local_artifacts(store: &CheckpointDecisionStore, checkpoint_id: u64) {
    assert_eq!(
        store
            .begin_checkpoint_artifact_inventory(
                local_artifact_inventory(store, checkpoint_id).await,
            )
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Applied
    );
}

async fn publish_local_commit(
    store: &CheckpointDecisionStore,
    checkpoint_id: u64,
    predecessor: Option<CommittedCheckpointRef>,
) -> CommittedCheckpointRef {
    begin_local_artifacts(store, checkpoint_id).await;
    let index = local_index(store, checkpoint_id, predecessor).await;
    let reference = store.create_committed_checkpoint(&index).await.unwrap();
    store
        .record_outcome(
            checkpoint_id,
            checkpoint_id,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(reference.clone()),
        )
        .await
        .unwrap();
    reference
}

fn retention_state(result: CheckpointRetentionUpdateResult) -> CheckpointRetentionState {
    match result {
        CheckpointRetentionUpdateResult::Applied(state)
        | CheckpointRetentionUpdateResult::Unchanged(state) => state,
        CheckpointRetentionUpdateResult::Conflict { current } => {
            panic!("unexpected retention conflict: {current:?}")
        }
    }
}

#[tokio::test]
async fn committed_index_create_is_idempotent_and_exactly_verified() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(raw);
    begin_local_artifacts(&store, 1).await;
    let index = local_index(&store, 1, None).await;

    let reference = store.create_committed_checkpoint(&index).await.unwrap();
    assert_eq!(
        store.create_committed_checkpoint(&index).await.unwrap(),
        reference
    );
    assert_eq!(
        store.load_committed_checkpoint(&reference).await.unwrap(),
        index
    );

    let mut wrong = reference;
    wrong.sha256 = digest(9);
    assert!(store.load_committed_checkpoint(&wrong).await.is_err());
}

#[tokio::test]
async fn aborted_candidate_seal_blocks_create_and_replaces_an_existing_candidate() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(Arc::clone(&raw));

    let missing = local_index(&store, 1, None).await;
    let (_, missing_ref) = missing.encode_and_reference().unwrap();
    store
        .seal_aborted_committed_checkpoint_candidate(&missing)
        .await
        .unwrap();
    store
        .seal_aborted_committed_checkpoint_candidate(&missing)
        .await
        .unwrap();
    assert!(store.create_committed_checkpoint(&missing).await.is_err());
    assert!(store.load_committed_checkpoint(&missing_ref).await.is_err());

    let existing = local_index(&store, 2, None).await;
    let existing_ref = store.create_committed_checkpoint(&existing).await.unwrap();
    store
        .seal_aborted_committed_checkpoint_candidate(&existing)
        .await
        .unwrap();
    store
        .seal_aborted_committed_checkpoint_candidate(&existing)
        .await
        .unwrap();
    assert!(store
        .load_committed_checkpoint(&existing_ref)
        .await
        .is_err());
}

#[tokio::test]
async fn aborted_candidate_seal_rejects_foreign_or_unrecognized_content() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(Arc::clone(&raw));
    let index = local_index(&store, 1, None).await;
    let mut foreign = index.clone();
    foreign.deployment_id = "00000000-0000-0000-0000-000000000001".into();
    assert!(matches!(
        store
            .seal_aborted_committed_checkpoint_candidate(&foreign)
            .await,
        Err(DecisionError::Conflict(_))
    ));

    let (_, reference) = index.encode_and_reference().unwrap();
    let path = CheckpointDecisionStore::committed_checkpoint_path(&reference);
    raw.put(&path, PutPayload::from_static(b"not-a-checkpoint"))
        .await
        .unwrap();

    assert!(matches!(
        store
            .seal_aborted_committed_checkpoint_candidate(&index)
            .await,
        Err(DecisionError::Conflict(_))
    ));
    assert_eq!(
        raw.get(&path).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"not-a-checkpoint")
    );
}

#[tokio::test]
async fn local_commit_requires_the_content_addressed_index() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(raw);
    begin_local_artifacts(&store, 1).await;
    let index = local_index(&store, 1, None).await;
    let reference = store.create_committed_checkpoint(&index).await.unwrap();

    let result = store
        .record_outcome(
            1,
            1,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(reference.clone()),
        )
        .await
        .unwrap();
    assert!(matches!(result, RecordOutcomeResult::Created(_)));
    assert_eq!(
        store
            .latest_committed_outcome()
            .await
            .unwrap()
            .unwrap()
            .committed_checkpoint,
        Some(reference)
    );
}

#[tokio::test]
async fn abort_forbids_a_committed_index_reference() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(raw);
    let index = local_index(&store, 1, None).await;
    let reference = store.create_committed_checkpoint(&index).await.unwrap();

    assert!(store
        .record_outcome(
            1,
            1,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            Some(reference),
        )
        .await
        .is_err());
}

#[tokio::test]
async fn decision_head_keeps_latest_commit_across_a_later_abort() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(raw);
    begin_local_artifacts(&store, 1).await;
    let first = local_index(&store, 1, None).await;
    let first_ref = store.create_committed_checkpoint(&first).await.unwrap();
    store
        .record_outcome(
            1,
            1,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(first_ref),
        )
        .await
        .unwrap();
    store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .latest_committed_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
    assert_eq!(
        store
            .latest_terminal_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        2
    );
}

#[tokio::test]
async fn local_commit_must_extend_the_authoritative_commit() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(raw);
    begin_local_artifacts(&store, 1).await;
    let first = local_index(&store, 1, None).await;
    let first_ref = store.create_committed_checkpoint(&first).await.unwrap();
    store
        .record_outcome(
            1,
            1,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(first_ref.clone()),
        )
        .await
        .unwrap();

    begin_local_artifacts(&store, 2).await;
    let fork = local_index(&store, 2, None).await;
    let fork_ref = store.create_committed_checkpoint(&fork).await.unwrap();
    assert!(store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(fork_ref),
        )
        .await
        .is_err());

    let second = local_index(&store, 2, Some(first_ref)).await;
    let second_ref = store.create_committed_checkpoint(&second).await.unwrap();
    store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Commit,
            Some(second_ref),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .latest_committed_outcome()
            .await
            .unwrap()
            .unwrap()
            .epoch,
        2
    );
}

#[tokio::test]
async fn artifact_inventory_admits_one_exact_first_attempt() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let first_store = CheckpointDecisionStore::new(Arc::clone(&raw));
    let second_store = CheckpointDecisionStore::new(raw);
    let first = local_artifact_inventory(&first_store, 1).await;

    assert_eq!(
        first_store
            .begin_checkpoint_artifact_inventory(first.clone())
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Applied
    );
    let head = first_store
        .checkpoint_decision_head()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.latest_terminal, None);
    assert_eq!(head.active_artifacts, Some(first.clone()));
    assert_eq!(
        second_store
            .begin_checkpoint_artifact_inventory(first.clone())
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Unchanged
    );

    let second = local_artifact_inventory(&first_store, 2).await;
    assert_eq!(
        second_store
            .begin_checkpoint_artifact_inventory(second)
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Conflict {
            current: Some(first)
        }
    );
}

#[tokio::test]
async fn local_commit_requires_and_clears_the_exact_inventory() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(raw);
    let inventory = local_artifact_inventory(&store, 1).await;
    store
        .begin_checkpoint_artifact_inventory(inventory.clone())
        .await
        .unwrap();

    assert!(store
        .record_outcome(
            2,
            2,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .is_err());
    assert_eq!(
        store
            .checkpoint_decision_head()
            .await
            .unwrap()
            .and_then(|head| head.active_artifacts),
        Some(inventory)
    );

    let index = local_index(&store, 1, None).await;
    let reference = store.create_committed_checkpoint(&index).await.unwrap();
    assert!(matches!(
        store
            .record_outcome(
                1,
                1,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                Some(reference),
            )
            .await
            .unwrap(),
        RecordOutcomeResult::Created(_)
    ));
    assert_eq!(
        store
            .checkpoint_decision_head()
            .await
            .unwrap()
            .and_then(|head| head.active_artifacts),
        None
    );
}

#[tokio::test]
async fn local_abort_retains_inventory_until_exact_cleanup() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(raw);
    let first = local_artifact_inventory(&store, 1).await;
    let second = local_artifact_inventory(&store, 2).await;
    store
        .begin_checkpoint_artifact_inventory(first.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .complete_checkpoint_artifact_cleanup(&first)
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Conflict {
            current: Some(first.clone())
        }
    );
    store
        .record_outcome(
            1,
            1,
            CheckpointScope::Local,
            None,
            None,
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .complete_checkpoint_artifact_cleanup(&second)
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Conflict {
            current: Some(first.clone())
        }
    );
    assert_eq!(
        store
            .begin_checkpoint_artifact_inventory(first.clone())
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Conflict {
            current: Some(first.clone())
        }
    );
    assert_eq!(
        store
            .begin_checkpoint_artifact_inventory(second.clone())
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Conflict {
            current: Some(first.clone())
        }
    );
    assert_eq!(
        store
            .complete_checkpoint_artifact_cleanup(&first)
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Applied
    );
    assert_eq!(
        store
            .complete_checkpoint_artifact_cleanup(&first)
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Unchanged
    );
    assert_eq!(
        store
            .begin_checkpoint_artifact_inventory(second)
            .await
            .unwrap(),
        CheckpointArtifactInventoryUpdateResult::Applied
    );
}

#[tokio::test]
async fn retention_journal_resumes_two_phases_and_stops_before_retired_history() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::new(raw);
    let first = publish_local_commit(&store, 1, None).await;

    assert_eq!(
        retention_state(store.begin_checkpoint_retention(&first).await.unwrap()),
        CheckpointRetentionState::Idle {
            protected: first.clone()
        }
    );

    let second = publish_local_commit(&store, 2, Some(first.clone())).await;
    let delete_first = retention_state(store.begin_checkpoint_retention(&second).await.unwrap());
    let expected_cursor = CheckpointRetentionCursor {
        protected: second.clone(),
        current: first.clone(),
        next: None,
        stop_before: None,
    };
    assert_eq!(
        delete_first,
        CheckpointRetentionState::DeleteData {
            cursor: expected_cursor.clone()
        }
    );
    let delete_first_metadata = retention_state(
        store
            .advance_checkpoint_retention(&delete_first)
            .await
            .unwrap(),
    );
    assert_eq!(
        delete_first_metadata,
        CheckpointRetentionState::DeleteMetadata {
            cursor: expected_cursor
        }
    );
    store.delete_committed_checkpoint(&first).await.unwrap();
    store.delete_committed_checkpoint(&first).await.unwrap();
    assert!(store.load_committed_checkpoint(&first).await.is_err());
    assert_eq!(
        retention_state(
            store
                .advance_checkpoint_retention(&delete_first_metadata)
                .await
                .unwrap()
        ),
        CheckpointRetentionState::Idle {
            protected: second.clone()
        }
    );

    let third = publish_local_commit(&store, 3, Some(second.clone())).await;
    let delete_second = retention_state(store.begin_checkpoint_retention(&third).await.unwrap());
    assert_eq!(
        delete_second,
        CheckpointRetentionState::DeleteData {
            cursor: CheckpointRetentionCursor {
                protected: third.clone(),
                current: second,
                next: Some(first.clone()),
                stop_before: Some(first),
            }
        }
    );
    let delete_second_metadata = retention_state(
        store
            .advance_checkpoint_retention(&delete_second)
            .await
            .unwrap(),
    );
    assert_eq!(
        retention_state(
            store
                .advance_checkpoint_retention(&delete_second_metadata)
                .await
                .unwrap()
        ),
        CheckpointRetentionState::Idle { protected: third }
    );
}

#[tokio::test]
async fn retention_rejects_stale_cut_and_stale_phase_transition() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let first_store = CheckpointDecisionStore::new(Arc::clone(&raw));
    let second_store = CheckpointDecisionStore::new(raw);
    let first = publish_local_commit(&first_store, 1, None).await;
    first_store
        .begin_checkpoint_retention(&first)
        .await
        .unwrap();
    let second = publish_local_commit(&first_store, 2, Some(first.clone())).await;
    assert!(second_store
        .begin_checkpoint_retention(&first)
        .await
        .is_err());

    let delete_data = retention_state(
        first_store
            .begin_checkpoint_retention(&second)
            .await
            .unwrap(),
    );
    let delete_metadata = retention_state(
        first_store
            .advance_checkpoint_retention(&delete_data)
            .await
            .unwrap(),
    );
    assert!(matches!(
        second_store
            .advance_checkpoint_retention(&delete_data)
            .await
            .unwrap(),
        CheckpointRetentionUpdateResult::Conflict {
            current: Some(current)
        } if current == delete_metadata
    ));
    let idle = retention_state(
        first_store
            .advance_checkpoint_retention(&delete_metadata)
            .await
            .unwrap(),
    );
    assert_eq!(idle, CheckpointRetentionState::Idle { protected: second });
    assert!(first_store
        .advance_checkpoint_retention(&idle)
        .await
        .is_err());
}

#[tokio::test]
async fn local_allocator_burns_the_reserved_suffix_on_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let store = CheckpointDecisionStore::local_filesystem(directory.path()).unwrap();
    assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 1);
    assert_eq!(store.allocate_checkpoint_id().await.unwrap(), 2);
    let reserved_through = store
        .checkpoint_id_reservation_high_watermark()
        .await
        .unwrap();
    assert_eq!(reserved_through, LOCAL_RESERVATION_SIZE);
    drop(store);

    let reopened = CheckpointDecisionStore::local_filesystem(directory.path()).unwrap();
    assert_eq!(
        reopened
            .checkpoint_id_reservation_high_watermark()
            .await
            .unwrap(),
        reserved_through
    );
    assert!(reopened.allocate_checkpoint_id().await.unwrap() > reserved_through);
}

#[test]
fn abort_cannot_share_an_epoch_with_latest_commit() {
    let deployment_id = uuid::Uuid::now_v7().to_string();
    let commit = CheckpointOutcome {
        version: CHECKPOINT_OUTCOME_VERSION,
        scope: CheckpointScope::Local,
        epoch: 1,
        checkpoint_id: 1,
        deployment_id: deployment_id.clone(),
        assignment_fence: None,
        leader_proof: None,
        committed_checkpoint: Some(CommittedCheckpointRef {
            epoch: 1,
            checkpoint_id: 1,
            len: 1,
            sha256: digest(1),
        }),
        verdict: CheckpointVerdict::Commit,
    };
    let terminal = CheckpointOutcome {
        committed_checkpoint: None,
        verdict: CheckpointVerdict::Abort,
        ..commit.clone()
    };
    let head = DurableCheckpointDecisionHead {
        version: CHECKPOINT_DECISION_HEAD_VERSION,
        deployment_id: deployment_id.clone(),
        latest_terminal: Some(terminal),
        latest_commit: Some(commit),
        active_artifacts: None,
    };

    assert!(CheckpointDecisionStore::validate_decision_head_shape(&head, &deployment_id).is_err());
}
