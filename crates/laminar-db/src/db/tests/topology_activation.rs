//! Real held actors and restored state applying the exact durable Release.

use super::*;
use laminar_core::cluster::control::TopologyAdmissionPhase;

pub(super) async fn installed() -> (
    Fixture,
    laminar_core::cluster::control::TopologyAdmissionStatus,
    Arc<runtime_probe::InstallationProbe>,
) {
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
    (fixture, committed, probe)
}

#[tokio::test]
async fn topology_activation_runtime_certification_keeps_intake_held_and_release_preserves_aggregate(
) {
    let (fixture, committed, probe) = installed().await;
    let receipt = fixture
        .db
        .certify_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(receipt.phase, TopologyAdmissionPhase::Activating);
    assert!(receipt.activation.as_ref().unwrap().installation_complete());
    assert!(receipt.activation.as_ref().unwrap().release.is_none());
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert_eq!(probe.sink_epochs.load(Ordering::Acquire), 0);
    assert!(fixture
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
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
    let active = fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(active.phase, TopologyAdmissionPhase::Active);
    assert_eq!(active.commit, committed.commit);
    assert_eq!(active.migration_root, committed.migration_root);
    assert!(!fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert!(!fixture.db.source_gate.load(Ordering::Acquire));
    let status = fixture.db.cluster_topology_status().await.unwrap();
    assert_eq!(status.committed_version.unwrap().get(), 2);
    assert_eq!(status.locally_active_version.unwrap().get(), 2);
    wait_until(|| {
        let output = probe.output.lock();
        output.iter().any(|(topic, _)| topic == "old-output")
            && output.iter().any(|(topic, _)| topic == "new-output")
    })
    .await;
    let output = probe.output.lock().clone();
    let existing = output
        .iter()
        .filter(|(topic, _)| topic == "old-output")
        .map(|(_, batch)| batch.clone())
        .collect::<Vec<_>>();
    assert_eq!(total(&existing), 45);
    assert_eq!(
        output
            .iter()
            .filter(|(topic, _)| topic == "new-output")
            .map(|(_, batch)| batch.num_rows())
            .sum::<usize>(),
        3
    );
    assert!(probe.acknowledgements.lock().is_empty());
    assert_eq!(
        fixture
            .db
            .apply_cluster_topology_release(committed.operation_id)
            .await
            .unwrap(),
        active
    );
    let input = fixture
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    let revision = fixture
        .db
        .assignment_authority_revision
        .load(Ordering::Acquire);
    let refreshed = fixture
        .db
        .activate_assignment_authority(
            input.assignment(),
            None,
            revision,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(
        refreshed.installed && refreshed.intake_open,
        "Release remains the admission proof before the first target checkpoint"
    );
    // Controlled installation has no running migration driver. Public admission fails before
    // changing the live inventory or durable authority.
    let inventory = fixture.db.catalog_manifest_inventory().unwrap();
    let sequence = fixture
        .authority
        .lease_store
        .load()
        .await
        .unwrap()
        .unwrap()
        .seq;
    assert!(matches!(
        fixture
            .db
            .execute("CREATE STREAM still_guarded AS SELECT * FROM trades")
            .await,
        Err(DbError::Topology(TopologyError::Conflict(message)))
            if message.contains("live checkpoint/recovery coordinator")
    ));
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), inventory);
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
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_activation_runtime_held_assignment_refresh_keeps_exact_transport_certificate() {
    let (fixture, committed, probe) = installed().await;
    let input = fixture
        .authority
        .controller
        .committed_topology_restore_input(committed.operation_id)
        .await
        .unwrap();
    let revision = fixture
        .db
        .assignment_authority_revision
        .load(Ordering::Acquire);
    let refreshed = fixture
        .db
        .activate_assignment_authority(
            input.assignment(),
            None,
            revision,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(refreshed.installed);
    assert!(!refreshed.intake_open);
    assert_eq!(
        fixture
            .authority
            .controller
            .checkpoint_assignment_fence(input.assignment().assignment_version)
            .as_ref(),
        Some(input.assignment())
    );
    assert_eq!(
        fixture
            .db
            .shuffle_receiver
            .lock()
            .as_ref()
            .unwrap()
            .active_assignment_digest(),
        Some(input.assignment().digest())
    );
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    fixture
        .db
        .certify_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_activation_runtime_dead_source_with_retained_child_cannot_certify() {
    let (fixture, committed, probe) = installed().await;
    let (owner, tracker) = laminar_connectors::connector::ConnectorTaskOwner::new();
    let child = owner.track().unwrap();
    let source = crate::pipeline::streaming_coordinator::SourceTaskLease::spawn_for_test(
        "trades",
        async {},
        Some(tracker),
    );
    wait_until(|| !source.is_running()).await;
    assert!(!source.is_finished());
    let original = std::mem::replace(&mut fixture.db.owned_source_tasks.lock()[0], source);
    assert!(fixture
        .db
        .certify_installed_cluster_topology(committed.operation_id)
        .await
        .is_err());
    fixture.db.owned_source_tasks.lock()[0] = original;
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        committed
    );
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    drop(child);
    drop(owner);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_activation_runtime_dead_source_after_release_is_not_locally_active() {
    let (fixture, committed, _probe) = installed().await;
    let active = fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let source = crate::pipeline::streaming_coordinator::SourceTaskLease::spawn_for_test(
        "trades",
        async {},
        None,
    );
    wait_until(|| !source.is_running()).await;
    let original = std::mem::replace(&mut fixture.db.owned_source_tasks.lock()[0], source);
    assert!(fixture
        .db
        .apply_cluster_topology_release(committed.operation_id)
        .await
        .is_err());
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
            .unwrap(),
        Some(active)
    );
    fixture.db.owned_source_tasks.lock()[0] = original;
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_activation_runtime_caller_disconnect_keeps_the_bounded_db_owner() {
    let (fixture, committed, probe) = installed().await;
    let lifecycle = fixture.db.lifecycle_lock.lock().await;
    let db = Arc::clone(&fixture.db);
    let operation = committed.operation_id;
    let caller =
        tokio::spawn(async move { db.release_installed_cluster_topology(operation).await });
    wait_until(|| fixture.db.topology_validation_lock.try_lock().is_err()).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    drop(lifecycle);
    wait_until(|| {
        fixture
            .db
            .installed_topology_runtime
            .lock()
            .as_ref()
            .is_some_and(|runtime| runtime.released_sequence.is_some())
    })
    .await;
    assert!(!fixture.db.source_gate.load(Ordering::Acquire));
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(operation)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TopologyAdmissionPhase::Active
    );
    assert!(probe.output.lock().is_empty());
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_activation_runtime_process_loss_after_readiness_never_opens_intake() {
    let (fixture, committed, probe) = installed().await;
    fixture
        .db
        .certify_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    fixture.authority.controller.fence_process_lease();
    assert!(fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .is_err());
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap()
            .phase,
        TopologyAdmissionPhase::Activating
    );
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    if let Err(error) = fixture.db.shutdown().await {
        assert!(matches!(error, DbError::Pipeline(_)), "{error}");
        assert!(
            error.to_string().contains("faulted while shutting down"),
            "{error}"
        );
    }
    assert!(fixture.db.runtime_handle.lock().await.is_none());
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    assert!(fixture.db.owned_sink_handles.lock().is_empty());
}
