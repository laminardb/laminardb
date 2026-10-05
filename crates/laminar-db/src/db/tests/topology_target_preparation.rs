//! The DB receipt API must observe existing runtime owners before any authority append.

use super::*;

#[tokio::test]
async fn topology_target_preparation_certifies_a_retained_image_without_committing_or_releasing() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let catalog = fixture.db.catalog_manifest_inventory().unwrap();
    let root = image.root().clone();
    let bytes = image.managed_state_bytes();
    let db = Arc::clone(&fixture.db);
    let (result, mut image) = tokio::spawn(async move {
        let result = db
            .certify_cluster_topology_target_preparation(&mut image)
            .await;
        (result, image)
    })
    .await
    .unwrap();
    let prepared = result.unwrap();
    assert!(prepared.target_preparation_complete());
    assert_eq!(prepared.target_preparations.len(), 1);
    assert_eq!(
        prepared.phase,
        laminar_core::cluster::control::TopologyAdmissionPhase::CutPrepared
    );
    assert_eq!(prepared.target_preparations[0].protocol_version, 4);
    assert_eq!(
        prepared.target_preparations[0].participant,
        fixture
            .authority
            .controller
            .try_live_local_process_authority_identity()
            .unwrap()
            .participant
    );
    assert!(image.parent_retirement_observed());
    assert_eq!(image.root(), &root);
    assert_eq!(image.managed_state_bytes(), bytes);
    assert_eq!(DbState::load(&image.candidate.state), DbState::Created);
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), catalog);
    require_namespace_owned(&fixture.db, &file);
    let status = fixture.db.cluster_topology_status().await.unwrap();
    assert_eq!(status.committed_version.unwrap().get(), 1);
    assert!(status.locally_active_version.is_none());
    let sequence = fixture
        .authority
        .lease_store
        .load()
        .await
        .unwrap()
        .unwrap()
        .seq;
    assert_eq!(
        fixture
            .db
            .certify_cluster_topology_target_preparation(&mut image)
            .await
            .unwrap(),
        prepared
    );
    assert_eq!(
        fixture
            .authority
            .lease_store
            .load()
            .await
            .unwrap()
            .unwrap()
            .seq,
        sequence
    );
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    drop(image);
    // Dropping the image does not revoke the historical observation or authorize intake.
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(staged.operation_id)
            .await
            .unwrap(),
        Some(prepared)
    );
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert!(fixture.db.start().await.is_err());
    assert!(fixture.db.stop_pipeline().await.is_err());
    require_namespace_owned(&fixture.db, &file);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_target_preparation_cancellation_before_retirement_writes_no_receipt_and_can_retry(
) {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    let (owner, tracker) = ConnectorTaskOwner::new();
    let child = owner.track().unwrap();
    fixture.db.owned_connector_task_fences.lock().push(
        crate::connector_task_fence::ConnectorTaskFence::new("target-preparation-child", tracker),
    );
    drop(owner);
    let before = fixture.authority.lease_store.load().await.unwrap();
    let mut certification = Box::pin(
        fixture
            .db
            .certify_cluster_topology_target_preparation(&mut image),
    );
    tokio::select! {
        () = wait_for(&entered) => {},
        result = &mut certification => panic!("certification skipped the watcher: {result:?}"),
    }
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    drop(certification);
    assert!(!image.parent_retirement_observed());
    assert!(fixture.db.runtime_handle.lock().await.is_some());
    require_namespace_owned(&fixture.db, &file);
    release.notify_one();
    let db = Arc::clone(&fixture.db);
    let certification = tokio::spawn(async move {
        let result = db
            .certify_cluster_topology_target_preparation(&mut image)
            .await;
        (result, image)
    });
    // The connector's child remains owned after compute finishes; no receipt is allowed yet.
    let runtime = fixture.db.runtime_handle.lock().await;
    drop(runtime);
    assert!(!certification.is_finished());
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    drop(child);
    let (result, image) = tokio::time::timeout(Duration::from_secs(5), certification)
        .await
        .unwrap()
        .unwrap();
    assert!(result.unwrap().target_preparation_complete());
    assert!(fixture.db.owned_connector_task_fences.lock().is_empty());
    require_namespace_owned(&fixture.db, &file);
    drop(image);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_target_preparation_foreign_image_and_local_fault_cannot_publish() {
    let (fixture, staged) = restorable_fixture().await;
    let (foreign, _) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let before = fixture.authority.lease_store.load().await.unwrap();
    assert!(foreign
        .db
        .certify_cluster_topology_target_preparation(&mut image)
        .await
        .is_err());
    assert!(!foreign.db.runtime_shutdown.read().is_cancelled());
    assert!(!fixture.db.runtime_shutdown.read().is_cancelled());
    fixture
        .db
        .pending_recovery_fault
        .store(1, Ordering::Release);
    assert!(fixture
        .db
        .certify_cluster_topology_target_preparation(&mut image)
        .await
        .is_err());
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert!(!fixture.db.runtime_shutdown.read().is_cancelled());
    fixture
        .db
        .pending_recovery_fault
        .store(0, Ordering::Release);
    drop(image);
    fixture.db.shutdown().await.unwrap();
    foreign.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_target_preparation_process_loss_during_retirement_writes_no_receipt() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    let before = fixture.authority.lease_store.load().await.unwrap();
    let db = Arc::clone(&fixture.db);
    let certification = tokio::spawn(async move {
        let result = db
            .certify_cluster_topology_target_preparation(&mut image)
            .await;
        (result, image)
    });
    wait_for(&entered).await;
    fixture.authority.controller.fence_process_lease();
    release.notify_one();
    let (result, image) = certification.await.unwrap();
    assert!(result.is_err());
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    require_namespace_owned(&fixture.db, &file);
    drop(image);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_target_preparation_total_deadline_preserves_runtime_and_namespace_ownership() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    let before = fixture.authority.lease_store.load().await.unwrap();
    tokio::time::pause();
    let mut certification = Box::pin(
        fixture
            .db
            .certify_cluster_topology_target_preparation(&mut image),
    );
    tokio::select! {
        () = wait_for(&entered) => {},
        result = &mut certification => panic!("certification skipped the watcher: {result:?}"),
    }
    tokio::time::advance(Duration::from_secs(45)).await;
    assert!(certification.await.is_err());
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert!(fixture.db.runtime_handle.lock().await.is_some());
    assert!(!image.parent_retirement_observed());
    require_namespace_owned(&fixture.db, &file);
    tokio::time::resume();
    release.notify_one();
    assert!(fixture
        .db
        .certify_cluster_topology_target_preparation(&mut image)
        .await
        .unwrap()
        .target_preparation_complete());
    drop(image);
    fixture.db.shutdown().await.unwrap();
}
