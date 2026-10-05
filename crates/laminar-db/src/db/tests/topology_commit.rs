//! Stateful Commit/reconstruction through the DB API; target actors remain uninstalled.

use super::*;
use futures::FutureExt;
use laminar_core::cluster::control::TopologyAdmissionPhase;

#[path = "topology_transport.rs"]
mod topology_transport;

#[path = "topology_recovery.rs"]
mod recovery;

async fn committed_fixture() -> (
    Fixture,
    laminar_core::cluster::control::TopologyAdmissionStatus,
) {
    committed_fixture_with_additions(Vec::new()).await
}

async fn committed_fixture_with_additions(
    additions: Vec<laminar_core::cluster::control::CatalogManifestEntry>,
) -> (
    Fixture,
    laminar_core::cluster::control::TopologyAdmissionStatus,
) {
    let (fixture, staged) = restorable_fixture_with_additions(additions).await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let status = fixture
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .unwrap();
    assert!(image.is_committed());
    drop(image);
    (fixture, status)
}

#[tokio::test]
async fn topology_removal_cold_bootstrap_asserts_original_config_without_recreating_objects() {
    for drop_count in 1..=3 {
        assert_removal_cold_bootstrap(drop_count).await;
    }
}

#[tokio::test]
async fn topology_reset_cold_bootstrap_uses_new_incarnation_before_the_first_target_checkpoint() {
    let (fixture, staged) = restorable_fixture_with_statements(vec![
        "DROP SINK existing_sink".into(), "DROP STREAM totals".into(),
        "CREATE STREAM totals AS SELECT id, COUNT(*) AS total FROM trades GROUP BY id EMIT CHANGES WITH ('retain_history' = '4mb')".into(),
        "CREATE SINK existing_sink FROM totals INTO \"planning-sink\" ('topic' = 'changed-output')".into(),
    ]).await;
    let original = fixture
        .db
        .catalog_manifest_inventory()
        .unwrap()
        .iter()
        .map(|entry| entry.ddl.clone())
        .collect::<Vec<_>>();
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    assert_eq!(image.restored_frame_count(), 0);
    assert!(image.root().subscriptions.is_empty());
    let committed = fixture
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .unwrap();
    drop(image);
    let old_definition = fixture
        .db
        .connector_manager
        .lock()
        .get_ddl("totals")
        .unwrap()
        .to_owned();
    let uncertified_definition = old_definition.replace("SUM(value)", "MAX(value)");
    fixture
        .db
        .connector_manager
        .lock()
        .store_ddl("totals", &uncertified_definition);
    assert!(matches!(
        fixture.db.restore_catalog_from_manifest().await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    assert_eq!(
        fixture.db.connector_manager.lock().get_ddl("totals"),
        Some(uncertified_definition.as_str())
    );
    fixture
        .db
        .connector_manager
        .lock()
        .store_ddl("totals", &old_definition);
    let fresh = Fixture::with_authority(TestCatalogAuthority {
        checkpoint_store: Arc::clone(&fixture.authority.checkpoint_store),
        manifest_store: Arc::clone(&fixture.authority.manifest_store),
        lease_store: Arc::clone(&fixture.authority.lease_store),
        controller: Arc::clone(&fixture.authority.controller),
        lease_tx: fixture.authority.lease_tx.clone(),
        lease: fixture.authority.lease.clone(),
    })
    .await;
    let results = fresh
        .db
        .execute_cluster_bootstrap_batch(&original)
        .await
        .unwrap();
    assert!(results
        .iter()
        .all(|result| matches!(result, ExecuteResult::Ddl(info) if !info.applied)));
    let inventory = fresh.db.catalog_manifest_inventory().unwrap();
    assert_eq!(inventory[1].catalog_generation, 8);
    assert!(inventory[1].ddl.contains("COUNT(*)"));
    let authorization = fresh
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    fresh
        .db
        .install_shuffle_assignment_fence(authorization.assignment())
        .unwrap();
    let mut recovered = fresh
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(recovered.restored_frame_count(), 0);
    recovered.graph.set_query_budget_ns(u64::MAX);
    let output = recovered
        .graph
        .execute_cycle(&input(5), i64::MIN, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 3);
    assert_eq!(fresh.effects.load(Ordering::Acquire), 0);
    assert_eq!(fresh.resolutions.load(Ordering::Acquire), 0);
    fresh.db.shutdown().await.unwrap();
    fixture.db.shutdown().await.unwrap();
}

async fn assert_removal_cold_bootstrap(drop_count: usize) {
    let statements = [
        "DROP SINK existing_sink",
        "DROP STREAM totals",
        "DROP SOURCE trades",
    ];
    let (fixture, staged) = restorable_fixture_with_statements(
        statements[..drop_count]
            .iter()
            .map(|sql| (*sql).into())
            .collect(),
    )
    .await;
    let expected_frames = if drop_count == 1 { 9 } else { 0 };
    let parent = fixture
        .authority
        .manifest_store
        .load()
        .await
        .unwrap()
        .unwrap();
    let original = parent
        .entries
        .iter()
        .map(|entry| entry.ddl.clone())
        .collect::<Vec<_>>();
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    assert_eq!(image.restored_frame_count(), expected_frames);
    let committed = fixture
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .unwrap();
    assert_eq!(committed.phase, TopologyAdmissionPhase::Committed);
    assert!(image.root().future_only_objects.is_empty());
    assert_eq!(image.source_positions().len(), usize::from(drop_count < 3));
    drop(image);
    let authority = TestCatalogAuthority {
        checkpoint_store: Arc::clone(&fixture.authority.checkpoint_store),
        manifest_store: Arc::clone(&fixture.authority.manifest_store),
        lease_store: Arc::clone(&fixture.authority.lease_store),
        controller: Arc::clone(&fixture.authority.controller),
        lease_tx: fixture.authority.lease_tx.clone(),
        lease: fixture.authority.lease.clone(),
    };
    let fresh = Fixture::with_authority(authority).await;
    assert_eq!(
        fresh.db.catalog_manifest_inventory().unwrap(),
        parent.entries[..3 - drop_count]
    );
    assert!(fresh.db.connector_manager.lock().sinks().is_empty());
    let results = fresh
        .db
        .execute_cluster_bootstrap_batch(&original)
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert!(results
        .iter()
        .all(|result| matches!(result, ExecuteResult::Ddl(info) if !info.applied)));
    let mut changed = original.clone();
    changed[2] = changed[2].replace("old-output", "different-output");
    assert!(fresh
        .db
        .execute_cluster_bootstrap_batch(&changed)
        .await
        .is_err());
    let source_assertion = fresh
        .db
        .execute_cluster_bootstrap_batch(&original[..1])
        .await;
    // Removing the sink and stream leaves this as the complete current configuration.
    assert_eq!(source_assertion.is_ok(), drop_count == 2);
    assert!(fresh.db.connector_manager.lock().sinks().is_empty());
    let authorization = fresh
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(authorization.parent(), &parent);
    fresh
        .db
        .install_shuffle_assignment_fence(authorization.assignment())
        .unwrap();
    let mut recovered = fresh
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(recovered.restored_frame_count(), expected_frames);
    // Private complete-cycle assertions are independent of the production wall-clock budget.
    recovered.graph.set_query_budget_ns(u64::MAX);
    let batches = if drop_count < 3 {
        input(5)
    } else {
        rustc_hash::FxHashMap::default()
    };
    let output = recovered
        .graph
        .execute_cycle(&batches, i64::MIN, None)
        .await
        .unwrap();
    if drop_count == 1 {
        assert_eq!(total(&output["totals"]), 45);
    } else {
        assert!(output.is_empty());
        assert!(recovered.root().subscriptions.is_empty());
    }
    assert_eq!(fresh.effects.load(Ordering::SeqCst), 0);
    assert_eq!(fresh.resolutions.load(Ordering::SeqCst), 0);
    fresh.db.shutdown().await.unwrap();
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_preserves_state_and_reconstructs_after_image_loss() {
    let (fixture, staged) = restorable_fixture().await;
    let file = namespace_lock(&fixture.db);
    let local = fixture.db.catalog_manifest_inventory().unwrap();
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let root = image.root().clone();
    let parent = image.parent_checkpoint().clone();
    let bytes = image.managed_state_bytes();
    let db = Arc::clone(&fixture.db);
    let (committed, mut image) = tokio::spawn(async move {
        let committed = db.commit_cluster_topology_target(&mut image).await;
        (committed, image)
    })
    .await
    .unwrap();
    let committed = committed.unwrap();
    assert_eq!(committed.phase, TopologyAdmissionPhase::Committed);
    assert!(image.is_committed());
    assert!(image.parent_retirement_observed());
    assert_eq!(image.root(), &root);
    assert_eq!(image.parent_checkpoint(), &parent);
    assert_eq!(image.managed_state_bytes(), bytes);
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), local);
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    assert_eq!(DbState::load(&image.candidate.state), DbState::Created);
    let status = fixture.db.cluster_topology_status().await.unwrap();
    assert_eq!(status.committed_version.unwrap().get(), 2);
    assert!(status.locally_active_version.is_none());
    let before = fixture.authority.lease_store.load().await.unwrap();
    assert_eq!(
        fixture
            .db
            .commit_cluster_topology_target(&mut image)
            .await
            .unwrap(),
        committed
    );
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    drop(image);
    let mut recovered = fixture
        .db
        .recover_committed_cluster_topology(staged.operation_id)
        .await
        .unwrap();
    assert!(recovered.is_committed());
    assert_eq!(recovered.restored_frame_count(), 9);
    assert_eq!(recovered.root(), &root);
    assert_eq!(recovered.parent_checkpoint(), &parent);
    assert_eq!(
        recovered.root().subscriptions[0]
            .target_certificate
            .catalog_generation,
        7
    );
    assert!(
        matches!(&recovered.source_positions()["trades"], PreparedTopologySourcePosition::Preserved { attempt, checkpoint } if *attempt == CheckpointAttempt::canonical(1) && checkpoint.offsets()["old.cursor"] == "3")
    );
    assert!(
        matches!(&recovered.source_positions()["added_source"], PreparedTopologySourcePosition::Initialized { checkpoint } if checkpoint.offsets()["partition-0-next"] == "91" && checkpoint.assignment_version().is_none())
    );
    let output = recovered
        .graph
        .execute_cycle(&input(5), i64::MIN, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 45);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    require_namespace_owned(&fixture.db, &file);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.start().await.is_err());
    assert!(fixture.db.stop_pipeline().await.is_err());
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_created_replay_accepts_original_bootstrap_and_gates_ordinary_start() {
    let (fixture, committed) = committed_fixture().await;
    let authority = TestCatalogAuthority {
        checkpoint_store: Arc::clone(&fixture.authority.checkpoint_store),
        manifest_store: Arc::clone(&fixture.authority.manifest_store),
        lease_store: Arc::clone(&fixture.authority.lease_store),
        controller: Arc::clone(&fixture.authority.controller),
        lease_tx: fixture.authority.lease_tx.clone(),
        lease: fixture.authority.lease.clone(),
    };
    // A separate Created DB replays the committed inventory. This fixture keeps the configured
    // control process; the core test separately performs a real fenced process takeover.
    let fresh = Fixture::with_authority(authority).await;
    assert_eq!(fresh.db.catalog_manifest_inventory().unwrap().len(), 6);
    assert_eq!(
        fresh.db.catalog_manifest_inventory().unwrap()[1].catalog_generation,
        7
    );
    assert_eq!(
        *fresh.db.replayed_topology_version.lock(),
        Some(TopologyVersion::new(2).unwrap())
    );
    assert!(matches!(
        fresh.db.start().await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    assert_eq!(DbState::load(&fresh.db.state), DbState::Created);
    assert!(fresh.db.runtime_handle.lock().await.is_none());
    let authorization = fresh
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    let owners = vec![1; 8];
    fresh
        .db
        .shuffle_sender
        .lock()
        .as_ref()
        .unwrap()
        .install_assignment_fence(authorization.assignment(), &owners)
        .unwrap();
    fresh
        .db
        .shuffle_receiver
        .lock()
        .as_ref()
        .unwrap()
        .install_assignment_fence(authorization.assignment(), &owners)
        .unwrap();
    let image = fresh
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(image.restored_frame_count(), 9);
    assert!(image.is_committed());
    assert!(fresh
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
    assert_eq!(fresh.effects.load(Ordering::SeqCst), 0);
    assert_eq!(fresh.resolutions.load(Ordering::SeqCst), 0);
    let inventory = fresh.db.catalog_manifest_inventory().unwrap();
    let subset = inventory[..2]
        .iter()
        .map(|entry| entry.ddl.clone())
        .collect::<Vec<_>>();
    assert!(fresh
        .db
        .execute_cluster_bootstrap_batch(&subset)
        .await
        .is_err());
    assert_eq!(fresh.db.catalog_manifest_inventory().unwrap(), inventory);
    fresh.db.shutdown().await.unwrap();
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_resolves_successful_decision_before_image_authority_handoff() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    fixture
        .db
        .certify_cluster_topology_target_preparation(&mut image)
        .await
        .unwrap();
    let input = fixture
        .authority
        .controller
        .topology_restore_input(staged.operation_id)
        .await
        .unwrap();
    let committed = fixture
        .authority
        .controller
        .commit_topology_target(&input)
        .await
        .unwrap();
    assert!(!image.is_committed());
    let before = fixture.authority.lease_store.load().await.unwrap();
    assert_eq!(
        fixture
            .db
            .commit_cluster_topology_target(&mut image)
            .await
            .unwrap(),
        committed
    );
    assert!(image.is_committed());
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_rejects_uncommitted_recovery_foreign_image_and_local_fault() {
    let (fixture, staged) = restorable_fixture().await;
    assert!(fixture
        .db
        .recover_committed_cluster_topology(staged.operation_id)
        .await
        .is_err());
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let other = Fixture::new().await;
    let before = fixture.authority.lease_store.load().await.unwrap();
    assert!(other
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .is_err());
    fixture
        .db
        .pending_recovery_fault
        .store(1, Ordering::Release);
    assert!(fixture
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .is_err());
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert!(!image.is_committed());
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture
        .db
        .pending_recovery_fault
        .store(0, Ordering::Release);
    fixture.db.shutdown().await.unwrap();
    other.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_cancelled_reconstruction_retains_target_root_hold_and_compiler_owner() {
    let (fixture, committed) = committed_fixture().await;
    let file = namespace_lock(&fixture.db);
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    let _ = fixture.restore_validation.entered.notified().now_or_never();
    let db = Arc::clone(&fixture.db);
    let operation_id = committed.operation_id;
    let task =
        tokio::spawn(async move { db.recover_committed_cluster_topology(operation_id).await });
    fixture.restore_validation.entered.notified().await;
    assert!(fixture.db.topology_validation_lock.try_lock().is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(operation_id)
            .await
            .unwrap(),
        Some(committed)
    );
    require_namespace_owned(&fixture.db, &file);
    fixture
        .restore_validation
        .block
        .store(false, Ordering::Release);
    let image = fixture
        .db
        .recover_committed_cluster_topology(operation_id)
        .await
        .unwrap();
    assert_eq!(image.restored_frame_count(), 9);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_reconstruction_deadline_keeps_commit_and_releases_partial_image() {
    let (fixture, committed) = committed_fixture().await;
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    let _ = fixture.restore_validation.entered.notified().now_or_never();
    tokio::time::pause();
    let db = Arc::clone(&fixture.db);
    let operation_id = committed.operation_id;
    let task =
        tokio::spawn(async move { db.recover_committed_cluster_topology(operation_id).await });
    fixture.restore_validation.entered.notified().await;
    assert!(fixture.db.topology_validation_lock.try_lock().is_err());
    tokio::time::advance(Duration::from_secs(46)).await;
    assert!(matches!(
        task.await.unwrap(),
        Err(DbError::Topology(TopologyError::Contended))
    ));
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(operation_id)
            .await
            .unwrap(),
        Some(committed)
    );
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture
        .restore_validation
        .block
        .store(false, Ordering::Release);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_revalidates_sealed_cursors_within_total_budget_before_decision() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    let _ = fixture.restore_validation.entered.notified().now_or_never();
    tokio::time::pause();
    let db = Arc::clone(&fixture.db);
    let task = tokio::spawn(async move {
        let result = db.commit_cluster_topology_target(&mut image).await;
        (result, image)
    });
    fixture.restore_validation.entered.notified().await;
    let status = fixture
        .db
        .cluster_topology_operation_status(staged.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert!(status.target_preparation_complete());
    assert_eq!(status.phase, TopologyAdmissionPhase::CutPrepared);
    assert!(status.commit.is_none());
    tokio::time::advance(Duration::from_secs(46)).await;
    let (result, mut image) = task.await.unwrap();
    assert!(matches!(
        result,
        Err(DbError::Topology(TopologyError::Contended))
    ));
    assert!(!image.is_committed());
    assert!(image.parent_retirement_observed());
    assert_eq!(
        fixture
            .db
            .cluster_topology_status()
            .await
            .unwrap()
            .committed_version
            .unwrap()
            .get(),
        1
    );
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    fixture
        .restore_validation
        .block
        .store(false, Ordering::Release);
    fixture
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .unwrap();
    assert!(image.is_committed());
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_commit_db_missing_or_corrupt_parent_payload_keeps_commit_visible_and_identity_strict(
) {
    let (fixture, committed) = committed_fixture().await;
    let authorization = fixture
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    let store =
        ObjectStoreCheckpointStore::new(Arc::clone(&fixture.authority.checkpoint_store), "")
            .with_key_group_count(fixture.db.checkpoint_key_groups())
            .with_participant_id(1);
    let reader = crate::RecoveryManager::new(
        &store,
        &authorization.descriptor().target_pipeline,
        &authorization.descriptor().deployment_id,
        laminar_core::checkpoint::CheckpointScope::Cluster,
    );
    assert!(reader
        .recover_committed(authorization.outcome(), authorization.checkpoint())
        .await
        .is_err());
    assert!(reader
        .recover_topology_root(&authorization, 1024 * 1024)
        .await
        .is_err());
    let objects = &fixture.authority.checkpoint_store;
    let paths = object_paths(objects.as_ref()).await;
    let path = object_store::path::Path::from(
        paths
            .iter()
            .find(|path| path.ends_with("/node-data.bin"))
            .unwrap()
            .as_str(),
    );
    let original = objects.get(&path).await.unwrap().bytes().await.unwrap();
    objects.delete(&path).await.unwrap();
    for corrupt in [false, true] {
        if corrupt {
            objects
                .put(
                    &path,
                    object_store::PutPayload::from_bytes(bytes::Bytes::from(vec![
                        0;
                        original.len()
                    ])),
                )
                .await
                .unwrap();
        }
        assert!(fixture
            .db
            .recover_committed_cluster_topology(committed.operation_id)
            .await
            .is_err());
        assert_eq!(
            fixture
                .db
                .cluster_topology_operation_status(committed.operation_id)
                .await
                .unwrap(),
            Some(committed.clone())
        );
        assert_eq!(
            fixture
                .db
                .cluster_topology_status()
                .await
                .unwrap()
                .committed_version
                .unwrap()
                .get(),
            2
        );
        assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
        assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
        assert!(fixture.db.source_gate.load(Ordering::Acquire));
    }
    objects
        .put(&path, object_store::PutPayload::from_bytes(original))
        .await
        .unwrap();
    assert!(fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .is_ok());
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture.db.shutdown().await.unwrap();
}
