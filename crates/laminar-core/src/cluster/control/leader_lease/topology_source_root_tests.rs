//! Initialization at the real authority/create-only boundaries, using the existing cut fixture.

use super::*;
use crate::cluster::control::TopologySourceInitialization;
use std::sync::atomic::AtomicUsize;

impl Fixture {
    async fn initialized<F, Fut>(
        &self,
        authority: &LeaderLeaseStore,
        initialize: F,
    ) -> Result<TopologyAdmissionStatus, TopologyError>
    where
        F: FnOnce(crate::cluster::control::CatalogManifest, ClusterTopologyValidation) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<TopologySourceInitialization>, TopologyError>>,
    {
        authority
            .stage_topology_migration_root_with_initialization(
                &self.lease.proof(),
                &self.assignments,
                &topology_preparation::processes(authority),
                &self.store,
                self.operation.operation_id,
                &self.operation.plan,
                initialize,
            )
            .await
    }
}

pub(super) fn position(
    descriptor: &ClusterTopologyValidation,
    next: u64,
) -> TopologySourceInitialization {
    let source = descriptor
        .objects
        .iter()
        .find(|o| {
            o.kind == CatalogObjectKind::Source
                && o.transition == ClusterTopologyObjectTransition::AddFutureOnly
        })
        .unwrap();
    TopologySourceInitialization {
        name: source.name.clone(),
        catalog_generation: source.catalog_generation,
        compatibility_sha256: source.compatibility_sha256.clone(),
        checkpoint: ConnectorCheckpoint {
            offsets: HashMap::from([
                ("@laminar.kafka.next.v1:new:0".into(), next.to_string()),
                ("@laminar.kafka.next.v1:new:1".into(), "0".into()),
            ]),
            metadata: HashMap::from([
                ("connector".into(), "kafka".into()),
                ("checkpoint.version".into(), "2".into()),
            ]),
            input_channels: Some(vec![vec![1], vec![2]]),
            source_assignment_version: None,
        },
    }
}

fn slot(operation: &TopologyAdmissionStatus) -> OsPath {
    OsPath::from(format!(
        "control/topology-source-root-staging/v1/{}/{}.json",
        operation.operation_id.get(),
        operation.plan.sha256
    ))
}

#[tokio::test]
async fn topology_initialization_root_seals_complete_new_source_once_without_changing_the_parent() {
    let authority = store(30_000);
    let fixture = fixture_with_sources(&authority, true).await;
    let expected = position(&fixture.descriptor, 91);
    let calls = AtomicUsize::new(0);
    let before = authority.load().await.unwrap().unwrap();
    let status = fixture
        .initialized(&authority, |_, _| async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![expected.clone()])
        })
        .await
        .unwrap();
    let root = authority
        .topology_migration_root(status.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.format_version, 2);
    assert_eq!(root.source_initializations, [expected]);
    assert_eq!(
        root.future_only_objects,
        ["added_sink", "added_source", "added_stream", "later"]
    );
    assert_eq!(root.preserved_objects.len(), 3);
    assert_eq!(root.subscriptions[0].frontiers.len(), 2);
    assert_eq!(status.cut, fixture.operation.cut);
    assert_eq!(status.preparation, fixture.operation.preparation);
    assert_eq!(status.phase, TopologyAdmissionPhase::CutPrepared);
    assert_eq!(
        authority.load().await.unwrap().unwrap().catalog_manifest,
        before.catalog_manifest
    );
    assert_eq!(
        authority.load_record().await.unwrap().unwrap().version,
        TOPOLOGY_SOURCE_ROOT_RECORD_VERSION
    );
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    assert_eq!(
        fixture
            .initialized(&reopened, |_, _| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![position(&fixture.descriptor, 123)])
            })
            .await
            .unwrap(),
        status
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // After publication the authoritative content body is independent of the staging slot.
    authority
        .store
        .delete(&slot(&fixture.operation))
        .await
        .unwrap();
    let aborted = authority
        .abort_topology_plan(&fixture.lease.proof(), status.operation_id, &status.plan)
        .await
        .unwrap();
    assert_eq!(aborted.migration_root, status.migration_root);
    assert_eq!(
        reopened
            .topology_migration_root(status.operation_id)
            .await
            .unwrap(),
        Some(root)
    );
}

#[tokio::test]
async fn topology_initialization_cancel_after_source_seal_retries_without_resolving_latest_again() {
    let backing = store(30_000);
    let fixture = fixture_with_sources(&backing, true).await;
    let expected = position(&fixture.descriptor, 91);
    let expected_operation = fixture.operation.clone();
    let (raw, authority) = delayed_response_with_inner(
        30_000,
        backing.store.clone(),
        slot(&fixture.operation),
        true,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = calls.clone();
    let task_authority = authority.clone();
    let task_position = expected.clone();
    let task = tokio::spawn(async move {
        fixture
            .initialized(&task_authority, |_, _| async move {
                task_calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![task_position])
            })
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        authority
            .topology_operation_status(expected_operation.operation_id)
            .await
            .unwrap(),
        Some(expected_operation.clone())
    );
    assert!(expected_operation.migration_root.is_none());
    let reopened = LeaderLeaseStore::new(authority.store.clone(), 30_000);
    let retry = reopened
        .stage_topology_migration_root_with_initialization(
            &authority.load().await.unwrap().unwrap().proof(),
            &AssignmentSnapshotStore::new(authority.store.clone()),
            &topology_preparation::processes(&authority),
            &checkpoint_store(&authority, 2),
            expected_operation.operation_id,
            &expected_operation.plan,
            |_, _| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(TopologyError::Unsupported("latest moved".into()))
            },
        )
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(raw.put_count(&slot(&expected_operation), "create"), 1);
    assert_eq!(
        retry.status_sequence,
        expected_operation.status_sequence + 1
    );
    assert_eq!(
        reopened
            .topology_migration_root(retry.operation_id)
            .await
            .unwrap()
            .unwrap()
            .source_initializations,
        [expected]
    );
}

#[tokio::test]
async fn topology_initialization_lost_source_seal_response_and_concurrent_cursors_use_the_winner() {
    let backing = store(30_000);
    let fixture = fixture_with_sources(&backing, true).await;
    let (raw, authority) = delayed_response_with_inner(
        30_000,
        backing.store.clone(),
        slot(&fixture.operation),
        true,
    );
    let expected = position(&fixture.descriptor, 91);
    let second = position(&fixture.descriptor, 123);
    let first = fixture.initialized(&authority, |_, _| async { Ok(vec![expected.clone()]) });
    let second_call = async {
        raw.entered.acquire().await.unwrap().forget();
        // The first body is already durable but its response has not arrived. This concurrent
        // caller must read it and never invoke its later broker-position resolver.
        let status = fixture
            .initialized(&authority, |_, _| async { Ok(vec![second.clone()]) })
            .await
            .unwrap();
        raw.release.add_permits(1);
        status
    };
    let (first, second_status) = tokio::join!(first, second_call);
    assert_eq!(first.unwrap(), second_status);
    assert_eq!(raw.put_count(&slot(&fixture.operation), "create"), 1);
    assert_eq!(
        authority
            .topology_migration_root(second_status.operation_id)
            .await
            .unwrap()
            .unwrap()
            .source_initializations,
        [expected]
    );
}

#[tokio::test]
async fn topology_initialization_concurrent_unsealed_reads_choose_one_vector_and_one_append() {
    let authority = store(30_000);
    let fixture = fixture_with_sources(&authority, true).await;
    let barrier = tokio::sync::Barrier::new(2);
    let first = position(&fixture.descriptor, 91);
    let second = position(&fixture.descriptor, 123);
    let (a, b) = tokio::join!(
        fixture.initialized(&authority, |_, _| async {
            barrier.wait().await;
            Ok(vec![first.clone()])
        }),
        fixture.initialized(&authority, |_, _| async {
            barrier.wait().await;
            Ok(vec![second.clone()])
        }),
    );
    let a = a.unwrap();
    assert_eq!(a, b.unwrap());
    assert_eq!(a.status_sequence, fixture.operation.status_sequence + 1);
    let actual = authority
        .topology_migration_root(a.operation_id)
        .await
        .unwrap()
        .unwrap()
        .source_initializations;
    assert!(actual == [first] || actual == [second]);
}

#[tokio::test]
async fn topology_initialization_missing_incomplete_owned_or_mismatched_positions_never_append() {
    let authority = store(30_000);
    let fixture = fixture_with_sources(&authority, true).await;
    let before = authority.load().await.unwrap();
    assert!(fixture.stage(&authority).await.is_err());
    for kind in 0..7 {
        let mut invalid = position(&fixture.descriptor, 91);
        match kind {
            0 => invalid.checkpoint.input_channels = None,
            1 => invalid.checkpoint.input_channels = Some(vec![vec![2], vec![1]]),
            2 => invalid.checkpoint.source_assignment_version = std::num::NonZeroU64::new(1),
            3 => invalid.catalog_generation += 1,
            4 => invalid.name = "events".into(),
            5 => invalid.checkpoint.offsets.clear(),
            _ => invalid.checkpoint.input_channels = Some(vec![vec![1]; 4097]),
        }
        assert!(fixture
            .initialized(&authority, |_, _| async { Ok(vec![invalid]) })
            .await
            .is_err());
        assert_eq!(authority.load().await.unwrap(), before);
        assert!(matches!(
            authority.store.get(&slot(&fixture.operation)).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
    assert!(fixture
        .initialized(&authority, |_, _| async {
            Err(TopologyError::Unsupported(
                "connector cannot resolve".into(),
            ))
        })
        .await
        .is_err());
    assert_eq!(authority.load().await.unwrap(), before);
}

#[tokio::test]
async fn topology_initialization_corrupt_or_oversized_slot_fails_before_connector_resolution() {
    let authority = store(30_000);
    let fixture = fixture_with_sources(&authority, true).await;
    let before = authority.load().await.unwrap();
    let calls = AtomicUsize::new(0);
    for bytes in [
        b"{\"not_a_root\":true}".to_vec(),
        vec![b' '; 1024 * 1024 + 1],
    ] {
        authority
            .store
            .put(&slot(&fixture.operation), PutPayload::from(bytes))
            .await
            .unwrap();
        assert!(fixture
            .initialized(&authority, |_, _| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![position(&fixture.descriptor, 91)])
            })
            .await
            .is_err());
        assert_eq!(authority.load().await.unwrap(), before);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn topology_initialization_leader_loss_after_seal_cannot_publish_or_reuse_the_old_operation()
{
    let (raw, authority) = blocking_once_at(30_000, lease_path(11));
    let fixture = fixture_with_sources(&authority, true).await;
    let expected = fixture.operation.clone();
    let owner = fixture.lease.owner.clone();
    let task_authority = authority.clone();
    let task = tokio::spawn(async move {
        let cursor = position(&fixture.descriptor, 91);
        fixture
            .initialized(&task_authority, |_, _| async { Ok(vec![cursor]) })
            .await
    });
    raw.entered.acquire().await.unwrap().forget();
    let LeaseOutcome::Acquired(new_lease) = authority.begin_new_term(&owner, 1).await.unwrap()
    else {
        panic!("new leader")
    };
    raw.release.add_permits(1);
    assert!(matches!(task.await.unwrap(), Err(TopologyError::Fenced)));
    let aborted = authority
        .topology_operation_status(expected.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        aborted.phase,
        TopologyAdmissionPhase::Aborted { .. }
    ));
    assert!(aborted.migration_root.is_none());
    assert_eq!(aborted.cut, expected.cut);
    assert!(authority
        .stage_topology_migration_root_with_initialization(
            &new_lease.proof(),
            &AssignmentSnapshotStore::new(authority.store.clone()),
            &topology_preparation::processes(&authority),
            &checkpoint_store(&authority, 2),
            expected.operation_id,
            &expected.plan,
            |_, _| async { panic!("replacement leader must not resolve old latest") }
        )
        .await
        .is_err());
}

#[test]
fn topology_initialization_preserves_previous_format_one_root_bytes() {
    let evidence: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/topology-migration-root.json")).unwrap();
    let root: TopologyMigrationRoot = serde_json::from_value(evidence["root"].clone()).unwrap();
    assert!(root.source_initializations.is_empty());
    let (_, reference) = root.encode_and_reference().unwrap();
    assert_eq!(
        reference.sha256,
        "1b7e162e94f225017df72848d4e7fe1e7bd50be500abc77e86e888af37cf0f81"
    );
    assert_eq!(reference.encoded_len, 50_338);
}

#[tokio::test]
async fn topology_reset_seals_new_source_generation_and_excludes_retired_state_and_watermarks() {
    let authority = store(30_000);
    let fixture = fixture_with_changes(&authority, FixtureChange::ResetPipeline).await;
    let cursor = position(&fixture.descriptor, 91);
    let status = fixture
        .initialized(&authority, |_, _| async { Ok(vec![cursor.clone()]) })
        .await
        .unwrap();
    let root = authority
        .topology_migration_root(status.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(root.preserved_objects.is_empty());
    assert!(root.subscriptions.is_empty());
    assert_eq!(
        root.future_only_objects,
        ["events", "totals", "totals_sink"]
    );
    assert_eq!(root.source_initializations[0].catalog_generation, 2);
    root.validate_restore_cut(
        &status,
        &fixture.descriptor,
        &fixture.index,
        &fixture.manifests,
    )
    .unwrap();
    let mut target = fixture.index.clone();
    target.pipeline_identity = fixture.descriptor.target_pipeline.clone();
    target.epoch += 1;
    target.checkpoint_id += 1;
    target.predecessor = Some(root.cut.checkpoint.clone());
    for channel in &mut target.channel_progress {
        channel.watermark = Some(0);
    }
    target.source_watermarks.insert("events".into(), 0);
    target.checkpoint_watermark = Some(0);
    root.validate_target_checkpoint_predecessor(&fixture.descriptor, &target, &fixture.index)
        .unwrap();
    let mut forged = root.clone();
    forged.source_initializations[0].catalog_generation = 1;
    assert!(forged
        .validate_restore_cut(
            &status,
            &fixture.descriptor,
            &fixture.index,
            &fixture.manifests
        )
        .is_err());
    let plan = authority.load_topology_plan(&status.plan).await.unwrap();
    let parent = authority
        .load_catalog_manifest(&plan.parent_manifest)
        .await
        .unwrap();
    let mut catalog = authority
        .load_catalog_manifest(&plan.target_manifest)
        .await
        .unwrap();
    catalog.entries[1].catalog_generation = 7;
    let mut descriptor = fixture.descriptor.clone();
    descriptor.target_manifest = catalog.reference().unwrap();
    descriptor.compatibility_sha256 = descriptor.descriptor_digest().unwrap();
    assert!(descriptor.validate_catalogs(&parent, &catalog).is_err());
}

#[tokio::test]
async fn topology_initialization_audit_rejects_a_root_anchored_before_authority_format_eighteen() {
    let authority = store(30_000);
    let fixture = fixture_with_sources(&authority, true).await;
    let cursor = position(&fixture.descriptor, 91);
    let status = fixture
        .initialized(&authority, |_, _| async { Ok(vec![cursor]) })
        .await
        .unwrap();
    let plan = authority.load_topology_plan(&status.plan).await.unwrap();
    let mut anchor = authority.load_record().await.unwrap().unwrap();
    assert_eq!(anchor.version, TOPOLOGY_SOURCE_ROOT_RECORD_VERSION);
    anchor.version = TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION;
    // Corrupt only the immutable first append in this in-memory fixture. Its reference still
    // names a valid format-2 body, so the body audit must enforce the authority upgrade gate.
    authority
        .store
        .put(
            &lease_path(anchor.lease.seq),
            PutPayload::from(serde_json::to_vec(&anchor).unwrap()),
        )
        .await
        .unwrap();
    assert!(matches!(
        authority
            .audit_topology_migration_root(&status, &plan, Some(&fixture.descriptor))
            .await,
        Err(TopologyError::Protocol(_))
    ));
}
