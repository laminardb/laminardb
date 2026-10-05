//! Exact-root held runtime installation, distinct from durable participant-complete Release.

use super::*;
use laminar_connectors::connector::SourcePosition;

#[path = "topology_activation.rs"]
mod activation;

#[path = "topology_driver.rs"]
mod driver;

#[path = "topology_coordinated_recovery.rs"]
mod recovery;

#[path = "topology_submission.rs"]
mod submission;

fn enable_runtime(fixture: &Fixture) -> Arc<runtime_probe::InstallationProbe> {
    let probe = Arc::new(runtime_probe::InstallationProbe::default());
    *fixture.restore_validation.runtime.lock() = Some(Arc::clone(&probe));
    *fixture.db.assignment_snapshot_store.lock() = Some(Arc::new(
        laminar_core::cluster::control::AssignmentSnapshotStore::new(Arc::clone(
            &fixture.authority.checkpoint_store,
        )),
    ));
    probe
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned installation boundary was not reached");
}

#[tokio::test]
async fn topology_install_runtime_installs_exact_catalog_coordinator_and_held_actors() {
    let (fixture, committed) = committed_fixture().await;
    let file = namespace_lock(&fixture.db);
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let input = fixture
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    let parent = image.parent_checkpoint().clone();
    let old_source = fixture.db.catalog.get_source("trades").unwrap();
    let old_stream = fixture.db.catalog.get_stream_entry("totals").unwrap();
    let probe = enable_runtime(&fixture);
    let db = Arc::clone(&fixture.db);
    tokio::spawn(async move { db.install_committed_cluster_topology(image).await })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(DbState::load(&fixture.db.state), DbState::Running);
    assert_eq!(
        fixture.db.catalog_manifest_inventory().unwrap(),
        input.target().entries
    );
    assert!(Arc::ptr_eq(
        &old_source,
        &fixture.db.catalog.get_source("trades").unwrap()
    ));
    assert!(Arc::ptr_eq(
        &old_stream,
        &fixture.db.catalog.get_stream_entry("totals").unwrap()
    ));
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["totals"].catalog_generation,
        7
    );
    let starts = probe.starts.lock().clone();
    assert_eq!(starts.len(), 2);
    assert!(starts.iter().any(|(name, position)| name == "trades" && matches!(position,
        SourcePosition::Resume { attempt, checkpoint } if *attempt == CheckpointAttempt::canonical(1) && checkpoint.offsets()["old.cursor"] == "3")));
    assert!(starts.iter().any(|(name, position)| name == "added_source" && matches!(position,
        SourcePosition::Initialized { checkpoint } if checkpoint.offsets()["partition-0-next"] == "91" && checkpoint.assignment_version().is_none())));
    wait_until(|| probe.controls.load(Ordering::Acquire) >= 4).await;
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert!(probe.acknowledgements.lock().is_empty());
    assert!(probe.output.lock().is_empty());
    assert_eq!(probe.sink_opens.load(Ordering::Acquire), 2);
    assert_eq!(probe.sink_epochs.load(Ordering::Acquire), 0);
    {
        let coordinator = fixture.db.coordinator.lock().await;
        let coordinator = coordinator.as_ref().unwrap();
        assert_eq!(
            coordinator.bound_pipeline_identity().unwrap(),
            input.descriptor().target_pipeline
        );
        assert_eq!(
            coordinator.last_committed_ref(),
            input.outcome().committed_checkpoint.as_ref()
        );
        assert_eq!(
            coordinator
                .last_committed_manifest()
                .unwrap()
                .pipeline_identity,
            parent.pipeline_identity
        );
        assert!(coordinator.committed_manifest_needs_vnode_rebase(CheckpointAttempt::canonical(1)));
        assert_eq!(coordinator.epoch(), 2);
    }
    fixture.db.set_source_gate(false);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.start().await.is_err());
    assert!(fixture
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    require_namespace_owned(&fixture.db, &file);
    fixture.db.shutdown().await.unwrap();
    assert!(fixture.db.runtime_handle.lock().await.is_none());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert_eq!(probe.source_closes.load(Ordering::Acquire), 2);
    assert_eq!(probe.sink_closes.load(Ordering::Acquire), 2);
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn topology_install_runtime_uses_restored_aggregate_and_future_only_new_pipeline() {
    let (fixture, committed) = committed_fixture().await;
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let probe = enable_runtime(&fixture);
    fixture
        .db
        .install_committed_cluster_topology(image)
        .await
        .unwrap();
    assert!(probe.output.lock().is_empty());
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            3,
        )]),
    );
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(19)["trades"][0].clone(),
            91,
        )]),
    );
    // A fixture-only local gate opening probes the actual installed codecs/callback/sink actors.
    // This is deliberately not a production Release, which remains absent from shared authority.
    fixture.db.source_gate.store(false, Ordering::Release);
    wait_until(|| {
        let output = probe.output.lock();
        (output.iter().any(|(topic, _)| topic == "old-output")
            && output.iter().any(|(topic, _)| topic == "new-output"))
            || fixture.db.last_fault().is_some()
    })
    .await;
    fixture.db.source_gate.store(true, Ordering::Release);
    assert!(
        fixture.db.last_fault().is_none(),
        "{:?}",
        fixture.db.last_fault()
    );
    let output = probe.output.lock().clone();
    let existing = output
        .iter()
        .filter(|(topic, _)| topic == "old-output")
        .map(|(_, batch)| batch.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        total(&existing),
        45,
        "30 preserved plus three new records of value 5"
    );
    let added = output
        .iter()
        .filter(|(topic, _)| topic == "new-output")
        .map(|(_, batch)| batch)
        .collect::<Vec<_>>();
    assert_eq!(added.iter().map(|batch| batch.num_rows()).sum::<usize>(), 3);
    assert!(added.iter().all(|batch| batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .iter()
        .all(|value| *value == 19)));
    assert!(probe.acknowledgements.lock().is_empty());
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    assert!(fixture
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_install_runtime_caller_disconnect_keeps_the_existing_startup_owner() {
    let (fixture, committed) = committed_fixture().await;
    let file = namespace_lock(&fixture.db);
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let probe = enable_runtime(&fixture);
    probe.block_start.store(true, Ordering::Release);
    let db = Arc::clone(&fixture.db);
    let caller = tokio::spawn(async move { db.install_committed_cluster_topology(image).await });
    wait_for(&probe.start_entered).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(!fixture
        .db
        .startup_attempt
        .lock()
        .as_ref()
        .unwrap()
        .is_complete());
    assert_eq!(DbState::load(&fixture.db.state), DbState::Starting);
    assert!(fixture.db.start().await.is_err());
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    require_namespace_owned(&fixture.db, &file);
    probe.block_start.store(false, Ordering::Release);
    probe.start_release.notify_one();
    wait_until(|| {
        fixture
            .db
            .startup_attempt
            .lock()
            .as_ref()
            .unwrap()
            .is_complete()
    })
    .await;
    assert_eq!(DbState::load(&fixture.db.state), DbState::Running);
    assert_eq!(probe.starts.lock().len(), 2);
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(probe.output.lock().is_empty());
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_install_runtime_failed_atomic_start_retains_commit_namespace_and_retries_root() {
    let (fixture, committed) = committed_fixture().await;
    let file = namespace_lock(&fixture.db);
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let probe = enable_runtime(&fixture);
    probe.fail_start.store(true, Ordering::Release);
    assert!(fixture
        .db
        .install_committed_cluster_topology(image)
        .await
        .is_err());
    assert_eq!(DbState::load(&fixture.db.state), DbState::Created);
    assert!(fixture.db.runtime_handle.lock().await.is_none());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert!(fixture.db.owned_connector_task_fences.lock().is_empty());
    assert_eq!(probe.sink_opens.load(Ordering::Acquire), 2);
    assert_eq!(probe.sink_closes.load(Ordering::Acquire), 2);
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert!(probe.output.lock().is_empty());
    assert!(probe.acknowledgements.lock().is_empty());
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    require_namespace_owned(&fixture.db, &file);
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    probe.fail_start.store(false, Ordering::Release);
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(image.restored_frame_count(), 9);
    fixture
        .db
        .install_committed_cluster_topology(image)
        .await
        .unwrap();
    assert_eq!(DbState::load(&fixture.db.state), DbState::Running);
    {
        let starts = probe.starts.lock();
        assert!(starts
            .iter()
            .filter_map(|(_, position)| match position {
                SourcePosition::Initialized { checkpoint } => Some(checkpoint),
                _ => None,
            })
            .all(|checkpoint| checkpoint.offsets()["partition-0-next"] == "91"));
    }
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    require_namespace_owned(&fixture.db, &file);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_install_runtime_unsupported_sealed_source_rejects_before_sink_io() {
    let (fixture, committed) = committed_fixture().await;
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let probe = enable_runtime(&fixture);
    probe.reject_initialized.store(true, Ordering::Release);
    let error = fixture
        .db
        .install_committed_cluster_topology(image)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("atomic sealed-position startup"),
        "{error}"
    );
    assert_eq!(probe.sink_opens.load(Ordering::Acquire), 0);
    assert!(probe.starts.lock().is_empty());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
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
async fn topology_install_runtime_owned_deadline_cleans_up_without_losing_commit() {
    let (fixture, committed) = committed_fixture().await;
    let file = namespace_lock(&fixture.db);
    let mut image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    fixture
        .db
        .prepare_cluster_topology_transport(&mut image)
        .await
        .unwrap();
    let probe = enable_runtime(&fixture);
    probe.block_start.store(true, Ordering::Release);
    let db = Arc::clone(&fixture.db);
    // Use a short budget through the same owned driver as the public 45-second installation.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let work = tokio::spawn(async move { db.start_with_topology_image(image, deadline).await });
    wait_for(&probe.start_entered).await;
    let error = work.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("[LDB-6064]"), "{error}");
    assert_eq!(DbState::load(&fixture.db.state), DbState::Created);
    assert!(fixture
        .db
        .startup_attempt
        .lock()
        .as_ref()
        .unwrap()
        .is_complete());
    assert!(fixture.db.runtime_handle.lock().await.is_none());
    assert!(fixture.db.installed_vnode_state.lock().is_none());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert!(fixture.db.owned_connector_task_fences.lock().is_empty());
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert!(probe.output.lock().is_empty());
    assert!(probe.acknowledgements.lock().is_empty());
    assert_eq!(
        probe.sink_opens.load(Ordering::Acquire),
        probe.sink_closes.load(Ordering::Acquire)
    );
    require_namespace_owned(&fixture.db, &file);
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
async fn topology_install_runtime_process_loss_during_start_never_publishes_readiness() {
    let (fixture, committed) = committed_fixture().await;
    let file = namespace_lock(&fixture.db);
    let image = fixture
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let probe = enable_runtime(&fixture);
    probe.block_start.store(true, Ordering::Release);
    let db = Arc::clone(&fixture.db);
    let work = tokio::spawn(async move { db.install_committed_cluster_topology(image).await });
    wait_for(&probe.start_entered).await;
    fixture.authority.controller.fence_process_lease();
    probe.block_start.store(false, Ordering::Release);
    probe.start_release.notify_one();
    assert!(work.await.unwrap().is_err());
    assert_ne!(DbState::load(&fixture.db.state), DbState::Running);
    assert!(fixture.db.runtime_handle.lock().await.is_none());
    assert!(fixture.db.installed_vnode_state.lock().is_none());
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert!(probe.output.lock().is_empty());
    assert!(probe.acknowledgements.lock().is_empty());
    require_namespace_owned(&fixture.db, &file);
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
async fn topology_install_runtime_created_db_uses_root_before_any_target_checkpoint() {
    let (fixture, committed) = committed_fixture().await;
    fixture.db.shutdown().await.unwrap();
    let authority = TestCatalogAuthority {
        checkpoint_store: Arc::clone(&fixture.authority.checkpoint_store),
        manifest_store: Arc::clone(&fixture.authority.manifest_store),
        lease_store: Arc::clone(&fixture.authority.lease_store),
        controller: Arc::clone(&fixture.authority.controller),
        lease_tx: fixture.authority.lease_tx.clone(),
        lease: fixture.authority.lease.clone(),
    };
    let fresh = Fixture::with_authority(authority).await;
    let input = fresh
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    fresh
        .db
        .install_shuffle_assignment_fence(input.assignment())
        .unwrap();
    let image = fresh
        .db
        .recover_committed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(image.restored_frame_count(), 9);
    let probe = enable_runtime(&fresh);
    fresh
        .db
        .install_committed_cluster_topology(image)
        .await
        .unwrap();
    assert_eq!(DbState::load(&fresh.db.state), DbState::Running);
    assert_eq!(
        fresh.db.catalog_manifest_inventory().unwrap(),
        input.target().entries
    );
    assert_eq!(probe.starts.lock().len(), 2);
    assert_eq!(fresh.resolutions.load(Ordering::Acquire), 0);
    assert!(fresh.db.source_gate.load(Ordering::Acquire));
    assert!(fresh
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
    fresh.db.shutdown().await.unwrap();
}
