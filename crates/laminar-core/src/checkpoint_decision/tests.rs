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
        predecessor: None,
        participants: vec![CommittedParticipantRef {
            participant_id: LOCAL_NODE_ID.0,
            manifest_len: 12,
            manifest_sha256: digest(1),
            node_data_len: 0,
            node_data_sha256: digest(2),
        }],
        source_offsets: BTreeMap::new(),
        channel_progress: Vec::new(),
        checkpoint_watermark: None,
    }
}

#[tokio::test]
async fn committed_index_create_is_idempotent_and_exactly_verified() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(raw);
    let index = local_index(&store, 1).await;

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
async fn local_commit_requires_the_content_addressed_index() {
    let raw: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = CheckpointDecisionStore::local_single_writer(raw);
    let index = local_index(&store, 1).await;
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
            .outcome(1)
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
    let index = local_index(&store, 1).await;
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
