//! Transport preparation is actorless and does not advance Committed to Active.

use super::*;
use laminar_core::shuffle::ShuffleTopologyFence;

#[path = "topology_installation.rs"]
mod installation;

#[tokio::test]
async fn topology_start_db_exact_committed_root_produces_distinct_preserved_and_initialized_requests(
) {
    use laminar_connectors::connector::{DeliveryGuarantee, SourcePosition, SourceStart};
    let (fixture, committed) = committed_fixture().await;
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let before = fixture.authority.lease_store.load().await.unwrap();
    for name in ["trades", "added_source"] {
        let registration = image.candidate.connector_manager.lock().sources()[name].clone();
        let start = SourceStart::new(
            image
                .candidate
                .build_registered_source_config(name, &registration)
                .unwrap(),
            image.source_positions()[name].startup_position(),
            DeliveryGuarantee::AtLeastOnce,
        )
        .unwrap();
        match start.into_parts().1 {
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => {
                assert_eq!(name, "trades");
                assert_eq!(attempt, CheckpointAttempt::canonical(1));
                assert_eq!(checkpoint.get_offset("old.cursor"), Some("3"));
                assert_eq!(checkpoint.assignment_version().unwrap().get(), 1);
            }
            SourcePosition::Initialized { checkpoint } => {
                assert_eq!(name, "added_source");
                assert_eq!(checkpoint.get_offset("partition-0-next"), Some("91"));
                assert!(checkpoint.assignment_version().is_none());
            }
            SourcePosition::Initial => {
                panic!("migration root cannot borrow mutable configured startup")
            }
        }
    }
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_transport_db_binds_exact_commit_preserves_state_and_keeps_runtime_held() {
    let (fixture, committed) = committed_fixture().await;
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let root = image.root().clone();
    let checkpoint = image.parent_checkpoint().clone();
    let bytes = image.managed_state_bytes();
    let catalog = fixture.db.catalog_manifest_inventory().unwrap();
    let before = fixture.authority.lease_store.load().await.unwrap();
    let legacy = image.graph.cluster_shuffle_config().unwrap().clone();
    // An independently retained stateless old graph must reject before admitting any batch.
    let mut old = crate::operator_graph::OperatorGraph::new(laminar_sql::create_session_context());
    old.set_key_group_count(fixture.db.checkpoint_key_groups());
    old.set_cluster_shuffle(legacy.clone());
    old.register_source_schema(
        "trades".into(),
        input(5)["trades"].first().unwrap().schema(),
    );
    old.add_query(
        "old_stream".into(),
        "SELECT * FROM trades".into(),
        None,
        None,
        None,
        None,
        false,
    );
    let db = Arc::clone(&fixture.db);
    let (prepared, retained) = tokio::spawn(async move {
        let prepared = db.prepare_cluster_topology_transport(&mut image).await;
        (prepared, image)
    })
    .await
    .unwrap();
    let mut image = retained;
    prepared.unwrap();
    let target = ShuffleTopologyFence::from_manifest(
        image.target_version(),
        &committed.commit.as_ref().unwrap().manifest,
    )
    .unwrap();
    assert_eq!(legacy.sender.topology_fence(), Some(target));
    assert_eq!(legacy.receiver.topology_fence(), Some(target));
    assert_eq!(
        image.graph.cluster_shuffle_config().unwrap().topology,
        Some(target)
    );
    fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .unwrap();
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), catalog);
    assert_eq!(image.root(), &root);
    assert_eq!(image.parent_checkpoint(), &checkpoint);
    assert_eq!(image.managed_state_bytes(), bytes);
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    require_namespace_owned(&fixture.db, &file);
    assert!(matches!(
        old.execute_cycle(&input(5), i64::MIN, None).await,
        Err(DbError::ShuffleNotReady(_))
    ));
    // Actual aggregate codecs continue privately from 30 to 45. No runtime actors or output sink
    // are installed; this direct codec probe cannot certify participant readiness or activation.
    let output = image
        .graph
        .execute_cycle(&input(5), i64::MIN, None)
        .await
        .unwrap();
    assert_eq!(total(&output["totals"]), 45);
    // Decoding alone permits binding; an executed image can never be rebound for installation.
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_transport(&mut image)
            .await,
        Err(DbError::Checkpoint(_))
    ));
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert_eq!(legacy.sender.topology_fence(), Some(target));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
    assert!(fixture.db.start().await.is_err());
    drop(image);
    let mut reconstructed = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(reconstructed.restored_frame_count(), 9);
    fixture
        .db
        .prepare_cluster_topology_transport(&mut reconstructed)
        .await
        .unwrap();
    assert_eq!(reconstructed.parent_checkpoint(), &checkpoint);
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_transport_db_rejects_uncommitted_foreign_and_faulted_images() {
    let (fixture, staged) = restorable_fixture().await;
    let mut image = fixture
        .db
        .prepare_cluster_topology_restore(staged.operation_id)
        .await
        .unwrap();
    assert!(fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .is_err());
    assert_eq!(
        image
            .graph
            .cluster_shuffle_config()
            .unwrap()
            .sender
            .topology_fence(),
        None
    );
    let other = Fixture::new().await;
    fixture
        .db
        .commit_cluster_topology_target(&mut image)
        .await
        .unwrap();
    assert!(other
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .is_err());
    fixture
        .db
        .terminal_pipeline_halt
        .store(true, Ordering::Release);
    assert!(fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .is_err());
    assert_eq!(
        image
            .graph
            .cluster_shuffle_config()
            .unwrap()
            .sender
            .topology_fence(),
        None
    );
    fixture
        .db
        .terminal_pipeline_halt
        .store(false, Ordering::Release);
    fixture.db.shutdown().await.unwrap();
    other.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_transport_db_cancellation_during_cursor_audit_keeps_commit_and_cut() {
    let (fixture, committed) = committed_fixture().await;
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let _ = fixture.restore_validation.entered.notified().now_or_never();
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    {
        let work = fixture.db.prepare_cluster_topology_transport(&mut image);
        tokio::pin!(work);
        tokio::select! { result = &mut work => panic!("cursor audit did not block: {result:?}"), () = fixture.restore_validation.entered.notified() => {} }
    }
    fixture
        .restore_validation
        .block
        .store(false, Ordering::Release);
    assert_eq!(
        fixture
            .db
            .shuffle_sender
            .lock()
            .as_ref()
            .unwrap()
            .topology_fence(),
        None
    );
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .unwrap();
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn topology_transport_db_cursor_deadline_retains_commit_and_retryable_image() {
    let (fixture, committed) = committed_fixture().await;
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let _ = fixture.restore_validation.entered.notified().now_or_never();
    fixture
        .restore_validation
        .block
        .store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .db
            .prepare_cluster_topology_transport(&mut image)
            .await,
        Err(DbError::Topology(TopologyError::Contended))
    ));
    assert_eq!(
        fixture
            .db
            .shuffle_receiver
            .lock()
            .as_ref()
            .unwrap()
            .topology_fence(),
        None
    );
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    fixture
        .restore_validation
        .block
        .store(false, Ordering::Release);
    fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .unwrap();
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_transport_db_created_reconstruction_remains_unstarted_and_gated() {
    let (fixture, committed) = committed_fixture().await;
    let authority = TestCatalogAuthority {
        checkpoint_store: Arc::clone(&fixture.authority.checkpoint_store),
        manifest_store: Arc::clone(&fixture.authority.manifest_store),
        lease_store: Arc::clone(&fixture.authority.lease_store),
        controller: Arc::clone(&fixture.authority.controller),
        lease_tx: fixture.authority.lease_tx.clone(),
        lease: fixture.authority.lease.clone(),
    };
    let fresh = Fixture::with_authority(authority).await;
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
    let mut image = fresh
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    fresh
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .unwrap();
    assert_eq!(DbState::load(&fresh.db.state), DbState::Created);
    assert!(fresh.db.runtime_handle.lock().await.is_none());
    assert!(fresh.db.source_gate.load(Ordering::Acquire));
    assert!(fresh.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fresh
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
    assert!(fresh.db.start().await.is_err());
    assert_eq!(fresh.effects.load(Ordering::SeqCst), 0);
    assert_eq!(fresh.resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(image.restored_frame_count(), 9);
    fresh.db.shutdown().await.unwrap();
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_transport_db_reobserves_actual_runtime_exit_before_binding() {
    let (fixture, committed) = committed_fixture().await;
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let file = namespace_lock(&fixture.db);
    let (entered, release) = blocked_runtime(&fixture.db).await;
    tokio::time::pause();
    let mut work = Box::pin(fixture.db.prepare_cluster_topology_transport(&mut image));
    tokio::select! {
        () = wait_for(&entered) => {},
        result = &mut work => panic!("transport preparation skipped runtime termination: {result:?}"),
    }
    tokio::time::advance(Duration::from_secs(45)).await;
    assert!(work.await.is_err());
    assert!(fixture.db.runtime_handle.lock().await.is_some());
    assert_eq!(
        fixture
            .db
            .shuffle_sender
            .lock()
            .as_ref()
            .unwrap()
            .topology_fence(),
        None
    );
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    require_namespace_owned(&fixture.db, &file);
    release.notify_one();
    tokio::time::resume();
    fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_transport_db_rejects_a_divergent_retained_fabric_and_preserves_commit() {
    let (fixture, committed) = committed_fixture().await;
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let scope = image.graph.cluster_shuffle_config().unwrap().clone();
    let divergent = ShuffleTopologyFence::new(2, [99; 32]).unwrap();
    // Trusted low-level integration can be misconfigured; the DB never accepts that digest as
    // its committed catalog, readiness proof, or authority to rewrite the local conflict floor.
    scope
        .sender
        .install_topology_fence_pair(&scope.receiver, None, divergent)
        .unwrap();
    assert!(fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .is_err());
    drop(image);
    assert!(matches!(
        fixture
            .db
            .recover_committed_cluster_topology(committed.operation_id)
            .await,
        Err(DbError::Topology(TopologyError::Fenced))
    ));
    assert_eq!(scope.sender.topology_fence(), Some(divergent));
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    fixture.db.shutdown().await.unwrap();
}
