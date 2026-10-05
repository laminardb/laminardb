//! Actual committed-output replay and retention across a released topology boundary.

use super::*;
use crate::checkpoint_coordinator::subscription_output::{
    cleanup_subscription_orphans, cluster_subscription_retention_horizon,
};
use crate::subscription::cluster::{
    ClusterReaderFrame, ClusterReaderRead, ClusterSubscriptionReader,
};
use crate::subscription::{ClusterSubscriptionError, SubscribeStart};
use laminar_core::checkpoint::{
    CheckpointStore, OutputDistributionCertificate, StreamGeneration, SubscriptionDigest,
};

fn store(fixture: &Fixture) -> Arc<dyn CheckpointStore> {
    Arc::new(
        ObjectStoreCheckpointStore::new(Arc::clone(&fixture.authority.checkpoint_store), "")
            .with_key_group_count(fixture.db.checkpoint_key_groups()),
    )
}

fn certificate(manifest: &CheckpointManifest) -> OutputDistributionCertificate {
    manifest
        .subscription_output
        .as_ref()
        .unwrap()
        .streams
        .iter()
        .find(|stream| stream.distribution_certificate.stream_id == "totals")
        .unwrap()
        .distribution_certificate
        .clone()
}

async fn observe(
    reader: &mut ClusterSubscriptionReader,
    manifest: &CheckpointManifest,
) -> Vec<(u32, u64)> {
    let expected = certificate(manifest);
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut identities = Vec::new();
        let mut batches = Vec::new();
        for _ in 0..64 {
            match reader.next().await {
                ClusterReaderRead::Frame(ClusterReaderFrame::Batch {
                    batch,
                    stream_generation,
                    partition,
                    partition_sequence,
                    committed_epoch,
                    ..
                }) => {
                    assert_eq!(stream_generation, expected.stream_generation);
                    assert_eq!(committed_epoch, manifest.epoch);
                    identities.push((u32::from(partition.get()), partition_sequence.get()));
                    batches.push(batch);
                }
                ClusterReaderRead::Frame(ClusterReaderFrame::Progress {
                    stream_generation,
                    epoch,
                    checkpoint_id,
                    ..
                }) => {
                    assert_eq!(stream_generation, expected.stream_generation);
                    assert_eq!(
                        (epoch, checkpoint_id),
                        (manifest.epoch, manifest.checkpoint_id)
                    );
                    assert_eq!(total(&batches), 45);
                    let stream = manifest
                        .subscription_output
                        .as_ref()
                        .unwrap()
                        .streams
                        .iter()
                        .find(|stream| stream.distribution_certificate.stream_id == "totals")
                        .unwrap();
                    let ranges = stream
                        .ranges
                        .iter()
                        .flat_map(|range| {
                            (range.first_sequence.get()..range.through_sequence.get())
                                .map(move |sequence| (u32::from(range.partition.get()), sequence))
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(identities, ranges);
                    assert!(!identities.is_empty());
                    return identities;
                }
                ClusterReaderRead::Terminal(error) => panic!("committed gateway failed: {error}"),
            }
        }
        panic!("gateway exceeded its bounded frame roster");
    })
    .await
    .expect("gateway did not reach target checkpoint")
}

#[tokio::test]
async fn topology_subscription_old_reader_and_target_reconnect_preserve_real_output_ids() {
    let (fixture, operation, manifest, reader) = Box::pin(checkpointed_with_reader(true)).await;
    let mut old_reader = reader.unwrap();
    let before = fixture.authority.lease_store.load().await.unwrap();
    let target = certificate(&manifest);
    let mut new_reader = ClusterSubscriptionReader::open(
        Arc::clone(&fixture.authority.lease_store),
        store(&fixture),
        Arc::new(target.clone()),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .unwrap();
    let old_ids = observe(&mut old_reader, &manifest).await;
    assert_eq!(observe(&mut new_reader, &manifest).await, old_ids);
    let selected = Box::pin(
        fixture
            .authority
            .controller
            .committed_topology_recovery_input(operation),
    )
    .await
    .unwrap();
    let mapping = selected
        .migration()
        .root()
        .subscriptions
        .iter()
        .find(|mapping| mapping.parent_certificate.stream_id == "totals")
        .unwrap();
    assert_eq!(
        mapping.parent_certificate.stream_generation,
        target.stream_generation
    );
    assert_ne!(
        mapping.parent_certificate.pipeline_identity,
        target.pipeline_identity
    );
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
}

#[tokio::test]
async fn topology_subscription_reconnect_rejects_changed_incarnation_schema_and_contract() {
    let (fixture, _, manifest) = Box::pin(checkpointed()).await;
    let target = certificate(&manifest);
    for case in 0..4 {
        let mut changed = target.clone();
        match case {
            0 => {
                changed.stream_generation =
                    StreamGeneration::from_digest(SubscriptionDigest::from_bytes([7; 32]));
            }
            1 => changed.schema_fingerprint = SubscriptionDigest::from_bytes([8; 32]),
            2 => changed.query_fingerprint = SubscriptionDigest::from_bytes([9; 32]),
            _ => changed.history_retention_bytes += 1,
        }
        let error = ClusterSubscriptionReader::open(
            Arc::clone(&fixture.authority.lease_store),
            store(&fixture),
            Arc::new(changed),
            SubscribeStart::AsOfEpoch(1),
            None,
        )
        .await
        .unwrap_err();
        match case {
            0 => assert!(matches!(
                error,
                DbError::Subscription(ClusterSubscriptionError::GenerationMismatch)
            )),
            1 => assert!(matches!(
                error,
                DbError::Subscription(ClusterSubscriptionError::SchemaMismatch)
            )),
            _ => assert!(matches!(
                error,
                DbError::Subscription(ClusterSubscriptionError::ManifestCorrupt { .. })
            )),
        }
    }
}

async fn segment_bytes(fixture: &Fixture, manifest: &CheckpointManifest) -> Vec<bytes::Bytes> {
    let mut bytes = Vec::new();
    for segment in manifest
        .subscription_output
        .as_ref()
        .unwrap()
        .streams
        .iter()
        .flat_map(|stream| &stream.segments)
    {
        bytes.push(
            fixture
                .authority
                .checkpoint_store
                .get(&object_store::path::Path::from(segment.object_key.as_str()))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        );
    }
    assert!(!bytes.is_empty());
    bytes
}

#[tokio::test]
async fn topology_subscription_retention_crosses_exact_root_and_rejects_changed_horizon() {
    let (fixture, operation, manifest) = Box::pin(checkpointed()).await;
    let selected = Box::pin(
        fixture
            .authority
            .controller
            .committed_topology_recovery_input(operation),
    )
    .await
    .unwrap();
    let before = segment_bytes(&fixture, &manifest).await;
    let store = store(&fixture);
    let decisions = CheckpointDecisionStore::new(Arc::clone(&fixture.authority.checkpoint_store));
    let horizon = Box::pin(cluster_subscription_retention_horizon(
        store.as_ref(),
        &decisions,
        &fixture.authority.lease_store,
        selected.checkpoint(),
        0,
    ))
    .await
    .unwrap();
    assert_eq!(&horizon, selected.migration().checkpoint());
    let mut reference = horizon.encode_and_reference().unwrap().1;
    let cleanup = Box::pin(cleanup_subscription_orphans(
        store.as_ref(),
        &decisions,
        &fixture.authority.lease_store,
        selected.checkpoint(),
        &reference,
        i64::MAX,
    ))
    .await
    .unwrap();
    assert_eq!(
        cleanup.retained_bytes,
        before.iter().map(|bytes| bytes.len() as u64).sum::<u64>()
    );
    assert_eq!(cleanup.orphan.objects_deleted, 0);
    reference.sha256 = "7".repeat(64);
    assert!(Box::pin(cleanup_subscription_orphans(
        store.as_ref(),
        &decisions,
        &fixture.authority.lease_store,
        selected.checkpoint(),
        &reference,
        i64::MAX,
    ))
    .await
    .is_err());
    assert_eq!(segment_bytes(&fixture, &manifest).await, before);
}

#[tokio::test]
async fn topology_subscription_missing_root_fails_replay_and_cleanup_without_deleting_output() {
    let (fixture, operation, manifest) = Box::pin(checkpointed()).await;
    let selected = Box::pin(
        fixture
            .authority
            .controller
            .committed_topology_recovery_input(operation),
    )
    .await
    .unwrap();
    let before = segment_bytes(&fixture, &manifest).await;
    let root = &selected
        .migration()
        .operation()
        .migration_root
        .as_ref()
        .unwrap()
        .root;
    fixture
        .authority
        .checkpoint_store
        .delete(&object_store::path::Path::from(format!(
            "control/topology-migration-roots/v1/{}.json",
            root.sha256,
        )))
        .await
        .unwrap();
    assert!(ClusterSubscriptionReader::open(
        Arc::clone(&fixture.authority.lease_store),
        store(&fixture),
        Arc::new(certificate(&manifest)),
        SubscribeStart::AsOfEpoch(1),
        None,
    )
    .await
    .is_err());
    let decisions = CheckpointDecisionStore::new(Arc::clone(&fixture.authority.checkpoint_store));
    assert!(Box::pin(cluster_subscription_retention_horizon(
        store(&fixture).as_ref(),
        &decisions,
        &fixture.authority.lease_store,
        selected.checkpoint(),
        0,
    ))
    .await
    .is_err());
    assert!(Box::pin(cleanup_subscription_orphans(
        store(&fixture).as_ref(),
        &decisions,
        &fixture.authority.lease_store,
        selected.checkpoint(),
        &selected
            .migration()
            .checkpoint()
            .encode_and_reference()
            .unwrap()
            .1,
        i64::MAX,
    ))
    .await
    .is_err());
    assert_eq!(segment_bytes(&fixture, &manifest).await, before);
}
