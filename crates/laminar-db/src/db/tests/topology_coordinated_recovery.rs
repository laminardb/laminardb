//! Real target actors restored and released by the existing database-owned recovery monitor.

use super::*;
use laminar_connectors::connector::SourcePosition;
use laminar_core::cluster::control::{RecoverPhase, TopologyAdmissionPhase, TopologyRecoveryCut};

fn diagnostics() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
}

#[tokio::test]
async fn topology_stateful_runtime_waits_for_release_and_cold_recovers_the_target_checkpoint() {
    diagnostics();
    let mut additions = stateful_additions();
    additions.push(laminar_core::cluster::control::CatalogManifestEntry {
        canonical_name: "new_result_sink".into(),
        kind: laminar_core::cluster::control::CatalogObjectKind::Sink,
        catalog_generation: 1,
        ddl: "CREATE SINK new_result_sink FROM new_total INTO \"planning-sink\" ('topic' = 'state-output')".into(),
    });
    let (fixture, committed) = committed_fixture_with_additions(additions).await;
    let _namespace = namespace_lock(&fixture.db);
    fixture
        .authority
        .controller
        .start_leased_barrier_server(
            "127.0.0.1:0".parse().unwrap(),
            None,
            fixture.process_lease.as_ref().unwrap(),
        )
        .await
        .unwrap();
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
    let generation = fixture.db.connector_manager.lock().streams()["new_total"]
        .subscription_certificate
        .as_ref()
        .unwrap()
        .stream_generation;
    enqueue(&probe, 3, 91, &[1]);
    assert!(fixture.db.cluster_intake_fenced());
    assert!(probe.output.lock().is_empty());
    fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    outputs(&probe, 45).await;
    wait_until(|| {
        probe
            .output
            .lock()
            .iter()
            .any(|(topic, _)| topic == "state-output")
    })
    .await;
    let state_output = || {
        total(
            &probe
                .output
                .lock()
                .iter()
                .filter(|(topic, _)| topic == "state-output")
                .map(|(_, batch)| batch.clone())
                .collect::<Vec<_>>(),
        )
    };
    assert_eq!(state_output(), 15);
    fixture.db.checkpoint().await.unwrap();
    let selected = fixture
        .authority
        .controller
        .committed_topology_recovery_input(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(selected.cut(), TopologyRecoveryCut::TargetCheckpoint);
    fixture.db.fence_coordinated_recovery_lifecycle();
    fixture.authority.controller.set_recovering(true);
    fixture
        .db
        .stop_pipeline_for_coordinated_recovery()
        .await
        .unwrap();
    assert!(fixture
        .db
        .prepare_committed_cluster_topology_startup()
        .await
        .unwrap());
    probe.output.lock().clear();
    probe.block_start.store(true, Ordering::Release);
    fixture.db.enable_coordinated_recovery().unwrap();
    held_start(&fixture, &probe, 2).await;
    enqueue(&probe, 6, 94, &[1]);
    released(&fixture).await;
    outputs(&probe, 60).await;
    wait_until(|| {
        probe
            .output
            .lock()
            .iter()
            .any(|(topic, _)| topic == "state-output")
    })
    .await;
    assert_eq!(state_output(), 30);
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["new_total"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        generation
    );
    let binding = fixture
        .db
        .installed_topology_runtime
        .lock()
        .clone()
        .unwrap();
    assert_eq!(
        binding.recovery.as_ref().unwrap().selection.checkpoint(),
        selected.checkpoint()
    );
    fixture.db.shutdown().await.unwrap();
}

fn request_recovery(fixture: &Fixture) {
    diagnostics();
    fixture.db.set_source_gate(true);
    fixture.authority.controller.set_recovering(true);
    crate::coordinated_recovery::queue_local_fault(
        &fixture.authority.controller,
        &fixture.db.pending_recovery_fault,
    )
    .unwrap();
    fixture.db.enable_coordinated_recovery().unwrap();
}

pub(super) async fn released(fixture: &Fixture) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if fixture
                .db
                .cluster_topology_status()
                .await
                .unwrap()
                .locally_active_version
                .is_some()
            {
                break;
            }
            assert!(
                !fixture.db.terminal_pipeline_halt.load(Ordering::Acquire),
                "{:?}",
                fixture.db.last_fault()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the actual target runtime did not reach coordinated Release");
}

fn future_batch(value: i64) -> RecordBatch {
    // Future input must be later than the cold target fixture's restored 100 ms watermark.
    let batch = input(value)["trades"][0].clone();
    let mut columns = batch.columns().to_vec();
    columns[1] = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
        200_000, 201_000, 202_000,
    ]));
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn enqueue(
    probe: &runtime_probe::InstallationProbe,
    old_cursor: u64,
    new_cursor: u64,
    new_channel: &[u8],
) {
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(future_batch(5), old_cursor)]),
    );
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned_in_channel(
            future_batch(19),
            new_cursor,
            new_channel,
        )]),
    );
}

async fn outputs(probe: &runtime_probe::InstallationProbe, expected: i64) {
    wait_until(|| {
        probe
            .output
            .lock()
            .iter()
            .any(|(topic, _)| topic == "old-output")
            && probe
                .output
                .lock()
                .iter()
                .any(|(topic, _)| topic == "new-output")
    })
    .await;
    assert_eq!(
        total(
            &probe
                .output
                .lock()
                .iter()
                .filter(|(topic, _)| topic == "old-output")
                .map(|(_, batch)| batch.clone())
                .collect::<Vec<_>>()
        ),
        expected
    );
}

pub(super) async fn held_start(
    fixture: &Fixture,
    probe: &runtime_probe::InstallationProbe,
    prior_starts: usize,
) {
    wait_until(|| probe.starts.lock().len() > prior_starts).await;
    assert!(fixture.db.cluster_intake_fenced());
    assert!(fixture.db.topology_cut_hold.load(Ordering::Acquire));
    let start = fixture
        .authority
        .controller
        .observe_recover_control()
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(start.phase, RecoverPhase::Start { .. }));
    assert!(start.round.topology_binding().is_some());
    assert!(fixture
        .db
        .cluster_topology_status()
        .await
        .unwrap()
        .locally_active_version
        .is_none());
    probe.block_start.store(false, Ordering::Release);
    probe.start_release.notify_one();
}

#[tokio::test]
async fn topology_coordinated_recovery_keeps_loss_unrepaired_until_release() {
    let (fixture, committed, probe) = activation::installed().await;
    let original = fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let receiver = fixture.db.shuffle_receiver.lock().clone().unwrap();
    let losses = receiver.delivery_loss_incidents();
    let repaired = receiver.recovered_delivery_loss_incidents();
    assert_eq!(repaired.load(Ordering::Acquire), 0);
    losses.fetch_add(1, Ordering::AcqRel);
    probe.block_start.store(true, Ordering::Release);
    request_recovery(&fixture);
    wait_until(|| probe.starts.lock().len() > 2).await;
    assert!(fixture.db.cluster_intake_fenced());
    assert!(receiver.has_unrecovered_delivery_loss());
    assert_eq!(repaired.load(Ordering::Acquire), 0);
    assert!(probe.output.lock().is_empty());
    assert_eq!(probe.sink_epochs.load(Ordering::Acquire), 0);
    let start = fixture
        .authority
        .controller
        .observe_recover_control()
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(start.phase, RecoverPhase::Start { .. }));
    assert_eq!(receiver.recovery_gen(), start.round.id.generation);
    probe.block_start.store(false, Ordering::Release);
    probe.start_release.notify_one();
    released(&fixture).await;
    assert_eq!(
        repaired.load(Ordering::Acquire),
        losses.load(Ordering::Acquire)
    );
    assert!(!receiver.has_unrecovered_delivery_loss());
    assert_eq!(
        fixture
            .db
            .cluster_topology_operation_status(committed.operation_id)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    enqueue(&probe, 3, 91, &[1]);
    outputs(&probe, 45).await;
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_coordinated_recovery_completes_first_release_with_real_held_actors() {
    let (fixture, committed, probe) = activation::installed().await;
    probe.block_start.store(true, Ordering::Release);
    request_recovery(&fixture);
    held_start(&fixture, &probe, 2).await;
    assert_eq!(probe.source_closes.load(Ordering::Acquire), 2);
    assert_eq!(probe.sink_closes.load(Ordering::Acquire), 2);
    // The original owners may tail-poll during ordinary shutdown. Their closes are observed;
    // replacement startup is blocked and has neither consumed input nor published output.
    assert!(probe.output.lock().is_empty());
    assert_eq!(probe.sink_epochs.load(Ordering::Acquire), 0);
    enqueue(&probe, 3, 91, &[1]);
    released(&fixture).await;
    outputs(&probe, 45).await;
    assert!(probe.acknowledgements.lock().is_empty());
    let binding = fixture
        .db
        .installed_topology_runtime
        .lock()
        .clone()
        .unwrap();
    let recovery = binding.recovery.as_ref().unwrap();
    assert!(recovery.released);
    assert_eq!(recovery.selection.cut(), TopologyRecoveryCut::MigrationRoot);
    assert!(binding.released_sequence.is_none());
    let status = fixture
        .db
        .cluster_topology_operation_status(committed.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.phase, TopologyAdmissionPhase::Active);
    assert_eq!(status.commit, committed.commit);
    assert_eq!(status.migration_root, committed.migration_root);
    assert_eq!(
        status.activation.as_ref().unwrap().recovery_round,
        Some(recovery.start.round.id)
    );
    assert_eq!(
        status.activation.as_ref().unwrap().installations[0].runtime_id,
        binding.runtime_id
    );
    let terminal = fixture
        .authority
        .controller
        .latest_committed_recover_release()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.round, recovery.start.round);
    assert!(!fixture
        .authority
        .controller
        .authorize_topology_release(&binding.input, binding.runtime_id)
        .await
        .unwrap());
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_coordinated_recovery_replaces_active_runtime_without_reusing_original_release() {
    let (fixture, committed, probe) = activation::installed().await;
    let original = fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    let old_binding = fixture
        .db
        .installed_topology_runtime
        .lock()
        .clone()
        .unwrap();
    probe.block_start.store(true, Ordering::Release);
    request_recovery(&fixture);
    held_start(&fixture, &probe, 2).await;
    assert_eq!(probe.source_closes.load(Ordering::Acquire), 2);
    assert_eq!(probe.sink_closes.load(Ordering::Acquire), 2);
    enqueue(&probe, 3, 91, &[1]);
    released(&fixture).await;
    outputs(&probe, 45).await;
    let new_binding = fixture
        .db
        .installed_topology_runtime
        .lock()
        .clone()
        .unwrap();
    assert_ne!(old_binding.runtime_id, new_binding.runtime_id);
    assert!(old_binding.shutdown.is_cancelled());
    let status = fixture
        .db
        .cluster_topology_operation_status(committed.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        status, original,
        "original activation evidence remains immutable"
    );
    assert!(!fixture
        .authority
        .controller
        .authorize_topology_release(&old_binding.input, old_binding.runtime_id)
        .await
        .unwrap());
    assert!(!fixture
        .authority
        .controller
        .authorize_topology_release(&new_binding.input, new_binding.runtime_id)
        .await
        .unwrap());
    let revision = fixture
        .db
        .assignment_authority_revision
        .load(Ordering::Acquire);
    let refreshed = fixture
        .db
        .activate_assignment_authority(
            new_binding.input.assignment(),
            None,
            revision,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(refreshed.installed && refreshed.intake_open);
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_coordinated_recovery_uses_real_target_checkpoint_and_continues_state() {
    let (fixture, committed, probe) = activation::installed().await;
    fixture
        .authority
        .controller
        .start_leased_barrier_server(
            "127.0.0.1:0".parse().unwrap(),
            None,
            fixture.process_lease.as_ref().unwrap(),
        )
        .await
        .unwrap();
    enqueue(&probe, 3, 91, &[1]);
    fixture
        .db
        .release_installed_cluster_topology(committed.operation_id)
        .await
        .unwrap();
    outputs(&probe, 45).await;
    fixture.db.checkpoint().await.unwrap();
    let selected = fixture
        .authority
        .controller
        .committed_topology_recovery_input(committed.operation_id)
        .await
        .unwrap();
    assert_eq!(selected.cut(), TopologyRecoveryCut::TargetCheckpoint);
    assert_eq!(
        selected.checkpoint().source_offsets["trades"].offsets["old.cursor"],
        "6"
    );
    assert_eq!(
        selected.checkpoint().source_offsets["added_source"].offsets["partition-0-next"],
        "94"
    );
    probe.output.lock().clear();
    probe.block_start.store(true, Ordering::Release);
    request_recovery(&fixture);
    held_start(&fixture, &probe, 2).await;
    released(&fixture).await;
    assert!(probe
        .acknowledgements
        .lock()
        .iter()
        .all(|(_, epoch)| *epoch <= selected.outcome().epoch));
    let binding = fixture
        .db
        .installed_topology_runtime
        .lock()
        .clone()
        .unwrap();
    let recovery = binding.recovery.as_ref().unwrap();
    assert_eq!(
        recovery.selection.outcome().committed_checkpoint,
        selected.outcome().committed_checkpoint
    );
    assert_eq!(recovery.selection.checkpoint(), selected.checkpoint());
    assert_eq!(
        fixture.db.last_recovery_epoch.lock().as_ref(),
        Some(&selected.outcome().epoch)
    );
    for (name, key, cursor) in [
        ("trades", "old.cursor", "6"),
        ("added_source", "partition-0-next", "94"),
    ] {
        assert!(probe.starts.lock().iter().skip(2).any(|(source, position)| source == name && matches!(position,
            SourcePosition::Resume { attempt, checkpoint } if attempt.epoch == selected.outcome().epoch && checkpoint.offsets()[key] == cursor)));
    }
    enqueue(&probe, 6, 94, &[1]);
    outputs(&probe, 60).await;
    fixture.db.checkpoint().await.unwrap();
    let next = fixture
        .authority
        .controller
        .committed_topology_recovery_input(committed.operation_id)
        .await
        .unwrap();
    assert!(next.outcome().epoch > selected.outcome().epoch);
    wait_until(|| {
        probe
            .acknowledgements
            .lock()
            .iter()
            .any(|(_, epoch)| *epoch == next.outcome().epoch)
    })
    .await;
    assert_eq!(
        next.checkpoint().predecessor.as_ref(),
        selected.outcome().committed_checkpoint.as_ref()
    );
    assert_eq!(
        next.checkpoint().source_offsets["trades"].offsets["old.cursor"],
        "9"
    );
    assert_eq!(
        next.checkpoint().source_offsets["added_source"].offsets["partition-0-next"],
        "97"
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
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_coordinated_recovery_cold_created_root_starts_only_through_monitor() {
    diagnostics();
    let (fixture, committed) = committed_fixture().await;
    let probe = enable_runtime(&fixture);
    fixture.db.fence_coordinated_recovery_lifecycle();
    fixture.authority.controller.set_recovering(true);
    fixture
        .db
        .stop_pipeline_for_coordinated_recovery()
        .await
        .unwrap();
    assert!(fixture
        .db
        .prepare_committed_cluster_topology_startup()
        .await
        .unwrap());
    assert_eq!(DbState::load(&fixture.db.state), DbState::Created);
    assert!(probe.starts.lock().is_empty());
    assert!(probe.output.lock().is_empty());
    assert!(fixture.db.source_gate.load(Ordering::Acquire));
    assert!(fixture.db.start().await.is_err());
    probe.block_start.store(true, Ordering::Release);
    fixture.db.enable_coordinated_recovery().unwrap();
    held_start(&fixture, &probe, 0).await;
    enqueue(&probe, 3, 91, &[1]);
    released(&fixture).await;
    outputs(&probe, 45).await;
    assert!(probe.acknowledgements.lock().is_empty());
    assert_eq!(
        fixture
            .db
            .cluster_topology_status()
            .await
            .unwrap()
            .locally_active_version,
        Some(committed.commit.unwrap().topology_version)
    );
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_coordinated_recovery_cold_target_checkpoint_keeps_exact_newer_cursor() {
    diagnostics();
    // Initial Release/checkpoint authority is a fixture here; reconstruction, actors, source
    // starts, restore readiness and the replacement Release are all driven by the real monitor.
    let (fixture, operation, manifest) =
        Box::pin(super::super::super::recovery::checkpointed()).await;
    let probe = enable_runtime(&fixture);
    fixture.db.fence_coordinated_recovery_lifecycle();
    fixture.authority.controller.set_recovering(true);
    fixture
        .db
        .stop_pipeline_for_coordinated_recovery()
        .await
        .unwrap();
    assert!(fixture
        .db
        .prepare_committed_cluster_topology_startup()
        .await
        .unwrap());
    probe.block_start.store(true, Ordering::Release);
    fixture.db.enable_coordinated_recovery().unwrap();
    held_start(&fixture, &probe, 0).await;
    enqueue(&probe, 6, 94, &[2]);
    released(&fixture).await;
    outputs(&probe, 60).await;
    let binding = fixture
        .db
        .installed_topology_runtime
        .lock()
        .clone()
        .unwrap();
    assert_eq!(binding.input.operation().operation_id, operation);
    let selected = &binding.recovery.as_ref().unwrap().selection;
    assert_eq!(selected.checkpoint().epoch, manifest.epoch);
    assert_eq!(selected.cut(), TopologyRecoveryCut::TargetCheckpoint);
    assert_eq!(
        selected.checkpoint().source_offsets,
        manifest
            .source_offsets
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>()
    );
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_coordinated_recovery_older_assignment_installs_exact_root_and_target_cut() {
    diagnostics();
    for target_checkpoint in [false, true] {
        let (fixture, operation) = if target_checkpoint {
            let (fixture, operation, _) =
                Box::pin(super::super::super::recovery::checkpointed()).await;
            (fixture, operation)
        } else {
            let (fixture, committed) = committed_fixture().await;
            (fixture, committed.operation_id)
        };
        super::super::super::recovery::advance_decode_assignment(&fixture, operation).await;
        let selected = fixture
            .authority
            .controller
            .committed_topology_recovery_input(operation)
            .await
            .unwrap();
        assert_eq!(
            selected
                .checkpoint()
                .assignment_fence
                .as_ref()
                .unwrap()
                .assignment_version,
            1
        );
        assert_eq!(selected.migration().assignment().assignment_version, 2);
        fixture
            .authority
            .controller
            .start_leased_barrier_server(
                "127.0.0.1:0".parse().unwrap(),
                None,
                fixture.process_lease.as_ref().unwrap(),
            )
            .await
            .unwrap();
        let probe = enable_runtime(&fixture);
        fixture.db.fence_coordinated_recovery_lifecycle();
        fixture.authority.controller.set_recovering(true);
        fixture
            .db
            .stop_pipeline_for_coordinated_recovery()
            .await
            .unwrap();
        assert!(fixture
            .db
            .prepare_committed_cluster_topology_startup()
            .await
            .unwrap());
        assert!(probe.starts.lock().is_empty());
        assert!(probe.output.lock().is_empty());
        probe.block_start.store(true, Ordering::Release);
        fixture.db.enable_coordinated_recovery().unwrap();
        held_start(&fixture, &probe, 0).await;
        enqueue(
            &probe,
            if target_checkpoint { 6 } else { 3 },
            if target_checkpoint { 94 } else { 91 },
            if target_checkpoint { &[2] } else { &[1] },
        );
        released(&fixture).await;
        outputs(&probe, if target_checkpoint { 60 } else { 45 }).await;
        let binding = fixture
            .db
            .installed_topology_runtime
            .lock()
            .clone()
            .unwrap();
        let recovery = binding.recovery.as_ref().unwrap();
        assert_eq!(recovery.selection.checkpoint(), selected.checkpoint());
        assert_eq!(recovery.selection.outcome(), selected.outcome());
        assert_eq!(binding.input.assignment().assignment_version, 2);
        fixture
            .db
            .ensure_topology_runtime_live(&binding.input, Some(&recovery.selection))
            .await
            .unwrap();
        if target_checkpoint {
            assert!(fixture
                .db
                .ensure_topology_runtime_live(&binding.input, None)
                .await
                .is_err());
        }
        {
            let coordinator = fixture.db.coordinator.lock().await;
            let coordinator = coordinator.as_ref().unwrap();
            assert_eq!(
                coordinator.last_committed_ref(),
                selected.outcome().committed_checkpoint.as_ref()
            );
            assert!(
                coordinator.last_committed_manifest().is_none(),
                "historical participant manifests must not be relabelled as the new assignment"
            );
        }
        assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
        fixture.db.checkpoint().await.unwrap();
        let next = fixture
            .authority
            .controller
            .committed_topology_recovery_input(operation)
            .await
            .unwrap();
        assert!(next.checkpoint().epoch > selected.checkpoint().epoch);
        assert_eq!(
            next.checkpoint().predecessor.as_ref(),
            selected.outcome().committed_checkpoint.as_ref()
        );
        assert_eq!(
            next.checkpoint()
                .assignment_fence
                .as_ref()
                .unwrap()
                .assignment_version,
            2
        );
        fixture
            .db
            .ensure_topology_runtime_live(&binding.input, Some(&recovery.selection))
            .await
            .unwrap();
        fixture.db.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn topology_coordinated_recovery_cold_missing_deployment_never_recreates_identity() {
    let (fixture, committed) = committed_fixture().await;
    let probe = enable_runtime(&fixture);
    fixture.db.fence_coordinated_recovery_lifecycle();
    fixture.authority.controller.set_recovering(true);
    fixture
        .db
        .stop_pipeline_for_coordinated_recovery()
        .await
        .unwrap();
    fixture
        .authority
        .checkpoint_store
        .delete(&object_store::path::Path::from(
            "checkpoint-deployment/identity.json",
        ))
        .await
        .unwrap();
    let before = fixture.authority.lease_store.load().await.unwrap();
    assert!(fixture
        .db
        .prepare_committed_cluster_topology_startup()
        .await
        .is_err());
    assert!(
        CheckpointDecisionStore::new(Arc::clone(&fixture.authority.checkpoint_store))
            .load_deployment_id()
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.authority.lease_store.load().await.unwrap(), before);
    assert_eq!(DbState::load(&fixture.db.state), DbState::Created);
    assert!(probe.starts.lock().is_empty());
    assert!(probe.output.lock().is_empty());
    assert!(fixture
        .db
        .cluster_topology_operation_status(committed.operation_id)
        .await
        .is_err());
    assert!(
        CheckpointDecisionStore::new(Arc::clone(&fixture.authority.checkpoint_store))
            .load_deployment_id()
            .await
            .unwrap()
            .is_none()
    );
    fixture.db.shutdown().await.unwrap();
}
