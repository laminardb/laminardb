//! The database-owned monitor advances actual held runtime phases from durable authority.

use super::*;
use crate::coordinated_recovery::TopologyDriver;
use laminar_core::cluster::control::TopologyAdmissionPhase;

#[tokio::test(start_paused = true)]
async fn topology_driver_prepares_candidate_and_stalled_cut_requests_existing_recovery() {
    let (fixture, assignments) = preparation_fixture().await;
    let admitted = admit_preparation_candidate(&fixture, &assignments).await;
    let mut driver = TopologyDriver::default();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    let prepared = fixture
        .db
        .cluster_topology_operation_status(admitted.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.phase, TopologyAdmissionPhase::Preparing);
    assert!(prepared
        .preparation
        .as_ref()
        .unwrap()
        .complete_sequence
        .is_some());
    // This fixture has no live checkpoint route. The worker must use the existing route and
    // cannot fabricate a cut from the retained coordinator or a closed intake flag.
    assert!(matches!(
        driver
            .poll(&fixture.db, &fixture.authority.controller)
            .await,
        Err(DbError::Checkpoint(_))
    ));
    assert!(fixture
        .db
        .cluster_topology_operation_status(admitted.operation_id)
        .await
        .unwrap()
        .unwrap()
        .cut
        .is_none());
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
    tokio::time::advance(Duration::from_secs(181)).await;
    assert!(
        matches!(driver.poll(&fixture.db, &fixture.authority.controller).await,
        Err(DbError::Topology(TopologyError::Conflict(message))) if message.contains("180 seconds"))
    );
    assert_ne!(fixture.db.pending_recovery_fault.load(Ordering::Acquire), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
}

#[tokio::test]
async fn topology_driver_advances_exact_root_to_release_with_one_private_image() {
    let (fixture, staged) = restorable_fixture().await;
    let probe = enable_runtime(&fixture);
    let file = namespace_lock(&fixture.db);
    let mut driver = TopologyDriver::default();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    assert!(fixture.db.topology_validation_lock.try_lock().is_err());
    assert!(probe.starts.lock().is_empty());
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
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
    for _ in 0..8 {
        driver
            .poll(&fixture.db, &fixture.authority.controller)
            .await
            .unwrap();
        let status = fixture
            .db
            .cluster_topology_operation_status(staged.operation_id)
            .await
            .unwrap()
            .unwrap();
        if status.phase != TopologyAdmissionPhase::Active {
            assert_eq!(probe.polls.load(Ordering::Acquire), 0);
            assert_eq!(probe.sink_epochs.load(Ordering::Acquire), 0);
            assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
        }
    }
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(staged.operation_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TopologyAdmissionPhase::Active
    );
    assert_eq!(
        fixture
            .db
            .cluster_topology_status()
            .await
            .unwrap()
            .locally_active_version
            .unwrap()
            .get(),
        2
    );
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    wait_until(|| {
        probe
            .output
            .lock()
            .iter()
            .any(|(topic, _)| topic == "old-output")
    })
    .await;
    let existing = probe
        .output
        .lock()
        .iter()
        .filter(|(topic, _)| topic == "old-output")
        .map(|(_, batch)| batch.clone())
        .collect::<Vec<_>>();
    assert_eq!(total(&existing), 45);
    assert_eq!(probe.starts.lock().len(), 2);
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    let sequence = fixture
        .authority
        .lease_store
        .load()
        .await
        .unwrap()
        .unwrap()
        .seq;
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
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
    require_namespace_owned(&fixture.db, &file);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_driver_reconstructs_private_image_after_retirement_without_parent_resume() {
    let (fixture, staged) = restorable_fixture().await;
    let probe = enable_runtime(&fixture);
    let mut driver = TopologyDriver::default();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    assert_eq!(DbState::load(&fixture.db.state), DbState::ShuttingDown);
    assert!(fixture.db.topology_validation_lock.try_lock().is_err());
    drop(driver);
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    let mut restarted = TopologyDriver::default();
    for _ in 0..8 {
        restarted
            .poll(&fixture.db, &fixture.authority.controller)
            .await
            .unwrap();
    }
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(staged.operation_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TopologyAdmissionPhase::Active
    );
    assert_eq!(probe.starts.lock().len(), 2);
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_driver_abort_drops_compiler_and_requests_coordinated_parent_recovery() {
    let (fixture, staged) = restorable_fixture().await;
    let mut driver = TopologyDriver::default();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    let aborted = fixture
        .authority
        .lease_store
        .abort_topology_plan(
            &fixture.authority.lease.proof(),
            staged.operation_id,
            &staged.plan,
        )
        .await
        .unwrap();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert_ne!(fixture.db.pending_recovery_fault.load(Ordering::Acquire), 0);
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(staged.operation_id)
            .await
            .unwrap()
            .unwrap(),
        aborted
    );
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn topology_driver_fault_releases_private_slot_and_cannot_publish_commit() {
    let (fixture, staged) = restorable_fixture().await;
    let mut driver = TopologyDriver::default();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    assert!(fixture.db.topology_validation_lock.try_lock().is_err());
    crate::coordinated_recovery::queue_local_fault(
        &fixture.authority.controller,
        &fixture.db.pending_recovery_fault,
    )
    .unwrap();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    assert!(fixture.db.topology_validation_lock.try_lock().is_ok());
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(fixture
        .db
        .cluster_topology_operation_status(staged.operation_id)
        .await
        .unwrap()
        .unwrap()
        .commit
        .is_none());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
}

#[tokio::test]
async fn topology_driver_retries_failed_atomic_installation_from_immutable_committed_root() {
    let (fixture, committed) = committed_fixture().await;
    let probe = enable_runtime(&fixture);
    let file = namespace_lock(&fixture.db);
    let mut driver = TopologyDriver::default();
    driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .unwrap();
    probe.fail_start.store(true, Ordering::Release);
    assert!(driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .is_err());
    assert_eq!(DbState::load(&fixture.db.state), DbState::Created);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
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
    probe.fail_start.store(false, Ordering::Release);
    for _ in 0..6 {
        driver
            .poll(&fixture.db, &fixture.authority.controller)
            .await
            .unwrap();
    }
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TopologyAdmissionPhase::Active
    );
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    assert!(probe.starts.lock().iter().all(|(_, position)| matches!(
        position,
        SourcePosition::Resume { .. } | SourcePosition::Initialized { .. }
    )));
    require_namespace_owned(&fixture.db, &file);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_driver_active_release_never_reconstructs_a_missing_runtime_from_parent() {
    let (fixture, committed, probe) = activation::installed().await;
    fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    // Losing the process-owned binding cannot reuse another runtime's original Release. An
    // automatic replacement needs an exact target recovery round, implemented separately.
    fixture.db.installed_topology_runtime.lock().take();
    let starts = probe.starts.lock().len();
    let mut driver = TopologyDriver::default();
    assert!(driver
        .poll(&fixture.db, &fixture.authority.controller)
        .await
        .is_err());
    assert_eq!(probe.starts.lock().len(), starts);
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TopologyAdmissionPhase::Active
    );
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_driver_owned_monitor_survives_observer_disconnect_during_installation() {
    let (fixture, committed) = committed_fixture().await;
    let probe = enable_runtime(&fixture);
    probe.block_start.store(true, Ordering::Release);
    fixture.db.enable_coordinated_recovery().unwrap();
    fixture.db.enable_coordinated_recovery().unwrap();
    let observer_db = Arc::clone(&fixture.db);
    let observer = tokio::spawn(async move {
        loop {
            if observer_db
                .cluster_topology_operation_status(committed.operation_id)
                .await
                .unwrap()
                .unwrap()
                .phase
                == TopologyAdmissionPhase::Active
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });
    tokio::time::timeout(Duration::from_secs(15), probe.start_entered.notified())
        .await
        .unwrap();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert_eq!(probe.sink_epochs.load(Ordering::Acquire), 0);
    probe.block_start.store(false, Ordering::Release);
    probe.start_release.notify_one();
    tokio::time::timeout(Duration::from_secs(15), async {
        while fixture.db.topology_cut_hold.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        fixture
            .db
            .cluster_topology_status()
            .await
            .unwrap()
            .locally_active_version
            .unwrap()
            .get(),
        2
    );
    assert_eq!(probe.starts.lock().len(), 2);
    assert!(fixture
        .db
        .recovery_monitor
        .lock()
        .as_ref()
        .is_some_and(|monitor| !monitor.is_finished()));
    fixture.db.shutdown().await.unwrap();
    assert!(fixture.db.recovery_monitor.lock().is_none());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
}
