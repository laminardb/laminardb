//! Public submission over actual parent/target actors and metadata admission races.

use super::*;
use crate::ClusterTopologyRequest;
use laminar_core::cluster::control::{
    TopologyAdmissionPhase, TOPOLOGY_SUBMISSION_PROTOCOL_VERSION,
};

fn request(value: u128, statements: Vec<String>) -> ClusterTopologyRequest {
    ClusterTopologyRequest {
        operation_id: uuid::Uuid::from_u128(value).try_into().unwrap(),
        expected_parent_version: TopologyVersion::LEGACY_BASELINE,
        statements,
    }
}

async fn active(fixture: &Fixture, operation: laminar_core::cluster::control::TopologyOperationId) {
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let status = fixture
                .db
                .cluster_topology_operation_status(operation)
                .await
                .unwrap()
                .unwrap();
            if status.phase == TopologyAdmissionPhase::Active
                && !fixture.db.topology_cut_hold.load(Ordering::Acquire)
            {
                return;
            }
            assert!(
                !matches!(status.phase, TopologyAdmissionPhase::Aborted { .. }),
                "unexpected abort: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("owned migration did not reach exact target Release");
}

fn sink_total(probe: &runtime_probe::InstallationProbe) -> i64 {
    total(
        &probe
            .output
            .lock()
            .iter()
            .filter(|(topic, _)| topic == "old-output")
            .map(|(_, batch)| batch.clone())
            .collect::<Vec<_>>(),
    )
}

async fn latest_index(fixture: &Fixture) -> laminar_core::checkpoint::CommittedCheckpointIndex {
    let outcome = fixture
        .authority
        .lease_store
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .unwrap();
    fixture
        .authority
        .lease_store
        .load_committed_checkpoint(outcome.committed_checkpoint.as_ref().unwrap())
        .await
        .unwrap()
}

async fn running_parent() -> (Fixture, Arc<runtime_probe::InstallationProbe>) {
    let (fixture, _) = Box::pin(preparation_fixture()).await;
    let probe = enable_runtime(&fixture);
    probe.allow_parent_initial.store(true, Ordering::Release);
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
    DbState::Created.store(&fixture.db.state);
    fixture.db.enable_coordinated_recovery().unwrap();
    Box::pin(fixture.db.start()).await.unwrap();
    Box::pin(
        fixture
            .db
            .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(10)["trades"][0].clone(),
            0,
        )]),
    );
    wait_until(|| sink_total(&probe) == 30).await;
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    (fixture, probe)
}

#[tokio::test]
async fn topology_sink_removal_preserves_actual_state_retries_and_cold_recovery() {
    let (fixture, probe) = running_parent().await;
    let certificate = fixture.db.connector_manager.lock().streams()["totals"]
        .subscription_certificate
        .clone()
        .unwrap();
    probe.output.lock().clear();
    let request = request(
        1401,
        vec![
            "DROP SINK IF EXISTS existing_sink".into(),
            "CREATE SINK kept_sink FROM totals INTO \"planning-sink\" ('topic' = 'kept-output')"
                .into(),
        ],
    );
    let admitted = Box::pin(fixture.db.submit_cluster_topology_change(&request))
        .await
        .unwrap();
    assert!(admitted.commit.is_none());
    assert!(fixture
        .db
        .connector_manager
        .lock()
        .sinks()
        .contains_key("existing_sink"));
    active(&fixture, request.operation_id).await;
    assert!(!fixture
        .db
        .connector_manager
        .lock()
        .sinks()
        .contains_key("existing_sink"));
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["totals"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        certificate.stream_generation
    );
    assert_eq!(probe.sink_closes.load(Ordering::Acquire), 1);
    assert!(probe.output.lock().is_empty());
    let compiler = fixture.db.topology_validation_lock.lock().await;
    assert_eq!(
        Box::pin(fixture.db.submit_cluster_topology_change(&request))
            .await
            .unwrap()
            .phase,
        TopologyAdmissionPhase::Active
    );
    let mut changed = request.clone();
    changed.statements[0] = "DROP SINK existing_sink".into();
    assert!(matches!(
        Box::pin(fixture.db.submit_cluster_topology_change(&changed)).await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    drop(compiler);
    let kept_total = || {
        total(
            &probe
                .output
                .lock()
                .iter()
                .filter(|(topic, _)| topic == "kept-output")
                .map(|(_, batch)| batch.clone())
                .collect::<Vec<_>>(),
        )
    };
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            3,
        )]),
    );
    wait_until(|| kept_total() == 45).await;
    assert!(probe
        .output
        .lock()
        .iter()
        .all(|(topic, _)| topic != "old-output"));
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    assert_eq!(
        latest_index(&fixture).await.source_offsets["trades"].offsets["old.cursor"],
        "6"
    );
    let selected = fixture
        .authority
        .controller
        .committed_topology_recovery_input(request.operation_id)
        .await
        .unwrap();
    assert_eq!(
        selected.cut(),
        laminar_core::cluster::control::TopologyRecoveryCut::TargetCheckpoint
    );
    probe.output.lock().clear();
    super::recovery::restart_settled_target(&fixture, &probe, 2).await;
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            6,
        )]),
    );
    super::recovery::released(&fixture).await;
    wait_until(|| kept_total() == 60).await;
    assert!(probe
        .output
        .lock()
        .iter()
        .all(|(topic, _)| topic != "old-output"));
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["totals"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        certificate.stream_generation
    );
    let recreate = fixture.db.validate_cluster_topology_change(TopologyVersion::new(2).unwrap(), &["CREATE SINK existing_sink FROM totals INTO \"planning-sink\" ('topic' = 'old-output')".into()]).await.unwrap();
    let reincarnated = recreate
        .objects
        .iter()
        .find(|object| object.name == "existing_sink")
        .unwrap();
    assert_eq!(reincarnated.catalog_generation, 2);
    assert_eq!(
        reincarnated.transition,
        crate::ClusterTopologyObjectTransition::AddFutureOnly
    );
    let result = Box::pin(fixture.db.execute("DROP SINK kept_sink"))
        .await
        .unwrap();
    let ExecuteResult::Ddl(info) = result else {
        panic!("SQL must return a durable removal receipt")
    };
    assert!(!info.applied);
    active(&fixture, info.topology_operation.unwrap().operation_id).await;
    assert!(fixture.db.connector_manager.lock().sinks().is_empty());
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    assert_eq!(
        fixture
            .authority
            .lease_store
            .retired_topology_names()
            .await
            .unwrap(),
        std::collections::BTreeSet::from(["existing_sink".into(), "kept_sink".into()])
    );
    Box::pin(fixture.db.shutdown()).await.unwrap();
}

#[tokio::test]
async fn topology_public_atomic_then_sql_migration_preserves_actual_aggregate_and_progress() {
    let (fixture, _) = Box::pin(preparation_fixture()).await;
    let probe = enable_runtime(&fixture);
    probe.allow_parent_initial.store(true, Ordering::Release);
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
    DbState::Created.store(&fixture.db.state);
    fixture.db.enable_coordinated_recovery().unwrap();
    Box::pin(fixture.db.start()).await.unwrap();
    assert_eq!(
        Box::pin(
            fixture
                .db
                .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(5),)
        )
        .await
        .unwrap(),
        crate::ClusterStartupDisposition::Serving
    );
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(10)["trades"][0].clone(),
            0,
        )]),
    );
    wait_until(|| sink_total(&probe) == 30).await;
    let first = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(first.success, "{first:?}");
    let parent_inventory = fixture.db.catalog_manifest_inventory().unwrap();
    let parent_identity = fixture
        .db
        .topology_definition_identities()
        .await
        .unwrap()
        .pipeline;
    probe.output.lock().clear();
    let request = request(1301, independent_pipeline());
    let admitted = Box::pin(fixture.db.submit_cluster_topology_change(&request))
        .await
        .unwrap();
    assert_eq!(admitted.operation_id, request.operation_id);
    assert!(admitted.commit.is_none());
    assert_eq!(
        fixture.db.catalog_manifest_inventory().unwrap(),
        parent_inventory
    );
    active(&fixture, request.operation_id).await;
    let (_, plan, _, _) = fixture
        .authority
        .lease_store
        .topology_preparation_input(request.operation_id)
        .await
        .unwrap();
    assert_eq!(plan.protocol_version, TOPOLOGY_SUBMISSION_PROTOCOL_VERSION);
    assert!(probe.output.lock().is_empty());
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
    wait_until(|| {
        sink_total(&probe) == 45
            && probe
                .output
                .lock()
                .iter()
                .any(|(topic, _)| topic == "new-output")
    })
    .await;
    let target = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(target.success, "{target:?}");
    assert!(target.epoch > first.epoch);
    let index = latest_index(&fixture).await;
    assert_ne!(index.pipeline_identity, parent_identity);
    assert_eq!(index.source_offsets["trades"].offsets["old.cursor"], "6");
    assert_eq!(
        index.source_offsets["added_source"].offsets["partition-0-next"],
        "94"
    );
    let compiler = fixture.db.topology_validation_lock.lock().await;
    let retry = Box::pin(fixture.db.submit_cluster_topology_change(&request))
        .await
        .unwrap();
    assert_eq!(retry.phase, TopologyAdmissionPhase::Active);
    let mut conflict = request.clone();
    conflict
        .statements
        .push("CREATE STREAM unexpected AS SELECT * FROM totals".into());
    assert!(matches!(
        Box::pin(fixture.db.submit_cluster_topology_change(&conflict)).await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    drop(compiler);
    probe.output.lock().clear();
    // The recovery monitor can still own the compiler after publishing Active.
    let sql = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Box::pin(
                fixture
                    .db
                    .execute("CREATE STREAM downstream AS SELECT * FROM totals"),
            )
            .await
            {
                Err(DbError::Topology(TopologyError::PlanningBusy)) => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                result => break result,
            }
        }
    })
    .await
    .expect("topology compiler did not become available for the SQL migration")
    .unwrap();
    let ExecuteResult::Ddl(info) = sql else {
        panic!("SQL must return its durable receipt");
    };
    assert_eq!(info.statement_type, "TOPOLOGY MIGRATION");
    assert!(!info.applied);
    let operation = info.topology_operation.unwrap().operation_id;
    active(&fixture, operation).await;
    assert_eq!(
        fixture
            .db
            .cluster_topology_status()
            .await
            .unwrap()
            .locally_active_version
            .unwrap()
            .get(),
        3
    );
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            6,
        )]),
    );
    wait_until(|| sink_total(&probe) == 60).await;
    let successor = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(successor.success, "{successor:?}");
    assert!(successor.epoch > target.epoch);
    let index = latest_index(&fixture).await;
    assert_eq!(index.source_offsets["trades"].offsets["old.cursor"], "9");
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 1);
    Box::pin(fixture.db.shutdown()).await.unwrap();
}

/// This fixture certifies only public admission/retry; it deliberately has no runnable driver.
async fn metadata_coordinator(fixture: &Fixture) -> crate::db::ForceCheckpointRx {
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        test_checkpoint_store(),
    )
    .unwrap();
    coordinator
        .bind_pipeline_identity(
            fixture
                .db
                .topology_definition_identities()
                .await
                .unwrap()
                .pipeline,
        )
        .unwrap();
    *fixture.db.coordinator.try_lock().unwrap() = Some(coordinator);
    DbState::Running.store(&fixture.db.state);
    fixture.db.source_gate.store(false, Ordering::Release);
    let (sender, receiver) = crossfire::mpsc::bounded_async(8);
    *fixture.db.force_ckpt_tx.lock() = Some(sender);
    *fixture.db.recovery_monitor.lock() = Some(tokio::spawn(std::future::pending()));
    receiver
}

#[tokio::test]
async fn topology_public_admission_is_atomic_and_retries_terminal_payload_before_compiling() {
    let (fixture, _) = Box::pin(preparation_fixture()).await;
    let request = request(1302, independent_pipeline());
    assert!(
        Box::pin(fixture.db.submit_cluster_topology_change(&request))
            .await
            .is_err()
    );
    assert!(fixture
        .db
        .cluster_topology_operation_status(request.operation_id)
        .await
        .unwrap()
        .is_none());
    let _receiver = metadata_coordinator(&fixture).await;
    let inventory = fixture.db.catalog_manifest_inventory().unwrap();
    let admitted = Box::pin(fixture.db.submit_cluster_topology_change(&request))
        .await
        .unwrap();
    assert_eq!(admitted.phase, TopologyAdmissionPhase::Planned);
    let aborted = fixture
        .authority
        .lease_store
        .abort_topology_plan(
            &fixture.authority.lease.proof(),
            request.operation_id,
            &admitted.plan,
        )
        .await
        .unwrap();
    let _compiler = fixture.db.topology_validation_lock.lock().await;
    fixture.db.shutdown.store(true, Ordering::Release);
    assert_eq!(
        Box::pin(fixture.db.submit_cluster_topology_change(&request))
            .await
            .unwrap(),
        aborted
    );
    let mut conflict = request.clone();
    conflict.statements[0].push(' ');
    assert!(matches!(
        Box::pin(fixture.db.submit_cluster_topology_change(&conflict)).await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    assert_eq!(fixture.db.catalog_manifest_inventory().unwrap(), inventory);
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
    assert!(fixture.db.owned_source_tasks.lock().is_empty());
    fixture.db.recovery_monitor.lock().take().unwrap().abort();
}

#[tokio::test]
async fn topology_public_submission_waits_for_definitive_checkpoint_cleanup() {
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::checkpoint_decision::{CheckpointArtifactInventory, CheckpointVerdict};

    let (fixture, assignments) = Box::pin(preparation_fixture()).await;
    let _receiver = metadata_coordinator(&fixture).await;
    let inventory = CheckpointArtifactInventory {
        deployment_id: CheckpointDecisionStore::new(Arc::clone(
            &fixture.authority.checkpoint_store,
        ))
        .load_deployment_id()
        .await
        .unwrap()
        .unwrap(),
        pipeline_identity: fixture
            .db
            .topology_definition_identities()
            .await
            .unwrap()
            .pipeline,
        attempt: CheckpointAttempt::canonical(1),
        assignment_fence: Some(
            assignments
                .load()
                .await
                .unwrap()
                .unwrap()
                .assignment_fence()
                .unwrap(),
        ),
        sink_artifact_intent_protocol: true,
    };
    let proof = fixture.authority.lease.proof();
    fixture
        .authority
        .lease_store
        .begin_cluster_checkpoint_artifacts(&proof, inventory.clone())
        .await
        .unwrap();
    let request = request(1305, independent_pipeline());
    let mut submission = Box::pin(
        fixture
            .db
            .submit_cluster_topology_forwarded(&request, Duration::from_secs(5)),
    );
    tokio::select! {
        result = &mut submission => panic!("checkpoint reservation was bypassed or reported as a parent conflict: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(150)) => {}
    }
    assert!(fixture
        .authority
        .lease_store
        .topology_operation_status(request.operation_id)
        .await
        .unwrap()
        .is_none());
    fixture
        .authority
        .lease_store
        .record_cluster_outcome(
            &proof,
            1,
            1,
            inventory.assignment_fence.clone().unwrap(),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    // A durable Abort alone is insufficient: exact admitted artifacts remain protected.
    tokio::select! {
        result = &mut submission => panic!("unreconciled checkpoint artifacts were bypassed: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    fixture
        .authority
        .lease_store
        .finish_cluster_checkpoint_artifact_cleanup(&proof, &inventory)
        .await
        .unwrap();
    let admitted = submission.await.unwrap();
    assert_eq!(admitted.operation_id, request.operation_id);
    assert_eq!(admitted.phase, TopologyAdmissionPhase::Planned);
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
    fixture.db.recovery_monitor.lock().take().unwrap().abort();
    fixture.db.shutdown.store(true, Ordering::Release);
}

#[tokio::test]
async fn topology_public_competing_requests_and_expired_forwarding_never_admit_two_parents() {
    let (fixture, _) = Box::pin(preparation_fixture()).await;
    let _receiver = metadata_coordinator(&fixture).await;
    let left = request(
        1303,
        vec!["CREATE STREAM left_future AS SELECT * FROM totals".into()],
    );
    let right = request(
        1304,
        vec!["CREATE STREAM right_future AS SELECT * FROM totals".into()],
    );
    assert!(Box::pin(
        fixture
            .db
            .submit_cluster_topology_forwarded(&left, Duration::ZERO)
    )
    .await
    .is_err());
    let (left_result, right_result) = tokio::join!(
        Box::pin(fixture.db.submit_cluster_topology_change(&left)),
        Box::pin(fixture.db.submit_cluster_topology_change(&right)),
    );
    assert_eq!(
        usize::from(left_result.is_ok()) + usize::from(right_result.is_ok()),
        1
    );
    let loser = if left_result.is_ok() { &right } else { &left };
    assert!(matches!(
        Box::pin(fixture.db.submit_cluster_topology_change(loser)).await,
        Err(DbError::Topology(TopologyError::Conflict(_)))
    ));
    assert!(fixture
        .db
        .cluster_topology_operation_status(loser.operation_id)
        .await
        .unwrap()
        .is_none());
    fixture.db.recovery_monitor.lock().take().unwrap().abort();
    fixture.db.shutdown.store(true, Ordering::Release);
}

#[tokio::test]
async fn topology_source_and_managed_stream_removal_preserves_survivors_through_cold_recovery() {
    let (fixture, probe) = running_parent().await;
    let mut additions = independent_pipeline();
    additions.extend([
        "CREATE STREAM surviving_total AS SELECT id, SUM(value) AS total FROM added_source GROUP BY id EMIT CHANGES WITH ('retain_history' = '4mb')".into(),
        "CREATE SINK surviving_sink FROM surviving_total INTO \"planning-sink\" ('topic' = 'surviving-output')".into(),
    ]);
    let addition = request(1501, additions);
    Box::pin(fixture.db.submit_cluster_topology_change(&addition))
        .await
        .unwrap();
    active(&fixture, addition.operation_id).await;
    let surviving_total = || {
        total(
            &probe
                .output
                .lock()
                .iter()
                .filter(|(topic, _)| topic == "surviving-output")
                .map(|(_, batch)| batch.clone())
                .collect::<Vec<_>>(),
        )
    };
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            91,
        )]),
    );
    wait_until(|| surviving_total() == 15).await;
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    let certificate = fixture.db.connector_manager.lock().streams()["surviving_total"]
        .subscription_certificate
        .clone()
        .unwrap();
    let mut removal = request(
        1502,
        vec![
            "DROP SINK existing_sink".into(),
            "DROP STREAM totals".into(),
            "DROP SOURCE trades".into(),
        ],
    );
    removal.expected_parent_version = TopologyVersion::new(2).unwrap();
    // The recovery monitor can still own the compiler after publishing Active.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Box::pin(fixture.db.submit_cluster_topology_change(&removal)).await {
                Err(DbError::Topology(TopologyError::PlanningBusy)) => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                result => break result,
            }
        }
    })
    .await
    .expect("topology compiler did not become available for the removal")
    .unwrap();
    active(&fixture, removal.operation_id).await;
    assert_eq!(fixture.db.catalog.list_sources(), ["added_source"]);
    assert!(!fixture
        .db
        .connector_manager
        .lock()
        .streams()
        .contains_key("totals"));
    assert!(!fixture.db.subscription_registry.contains_name("totals"));
    assert_eq!(probe.source_closes.load(Ordering::Acquire), 3);
    assert_eq!(
        probe
            .starts
            .lock()
            .iter()
            .filter(|(name, _)| name == "trades")
            .count(),
        2
    );
    let selected = fixture
        .authority
        .controller
        .committed_topology_restore_input(removal.operation_id)
        .await
        .unwrap();
    assert_eq!(
        selected.checkpoint().source_names,
        ["added_source", "trades"]
    );
    assert_eq!(
        selected.checkpoint().source_offsets["trades"].offsets["old.cursor"],
        "3"
    );
    assert!(selected
        .root()
        .subscriptions
        .iter()
        .all(|mapping| mapping.parent_certificate.stream_id != "totals"));
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["surviving_total"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        certificate.stream_generation
    );
    probe.output.lock().clear();
    // Input queued for a retired connector is never polled by the target generation.
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(100)["trades"][0].clone(),
            3,
        )]),
    );
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(7)["trades"][0].clone(),
            94,
        )]),
    );
    wait_until(|| surviving_total() == 36).await;
    assert_eq!(probe.input.lock()["trades"].len(), 1);
    assert!(probe
        .output
        .lock()
        .iter()
        .all(|(topic, _)| topic != "old-output"));
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    let target = latest_index(&fixture).await;
    assert_eq!(target.source_names, ["added_source"]);
    assert_eq!(
        target.source_offsets["added_source"].offsets["partition-0-next"],
        "97"
    );
    assert!(target
        .channel_progress
        .iter()
        .all(|channel| channel.source_name == "added_source"));
    assert!(!target.source_watermarks.contains_key("trades"));
    let original = selected
        .parent()
        .entries
        .iter()
        .map(|entry| entry.ddl.clone())
        .collect::<Vec<_>>();
    probe.output.lock().clear();
    let prior_starts = probe.starts.lock().len();
    super::recovery::restart_settled_target(&fixture, &probe, prior_starts).await;
    // The parent baseline never matches the committed target catalog during recovery.
    let baseline = original[..3].to_vec();
    assert!(fixture
        .db
        .execute_cluster_bootstrap_batch(&baseline)
        .await
        .is_err());
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(9)["trades"][0].clone(),
            97,
        )]),
    );
    super::recovery::released(&fixture).await;
    wait_until(|| surviving_total() == 63).await;
    assert_eq!(
        probe
            .starts
            .lock()
            .iter()
            .filter(|(name, _)| name == "trades")
            .count(),
        2
    );
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["surviving_total"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        certificate.stream_generation
    );
    let recreated_source = fixture
        .db
        .validate_cluster_topology_change(TopologyVersion::new(3).unwrap(), &baseline[..1])
        .await
        .unwrap();
    assert_eq!(
        recreated_source
            .objects
            .iter()
            .find(|object| object.name == "trades")
            .unwrap()
            .catalog_generation,
        2
    );
    assert!(fixture
        .db
        .validate_cluster_topology_change(TopologyVersion::new(3).unwrap(), &baseline[1..2])
        .await
        .is_err());
    assert!(Box::pin(fixture.db.execute("DROP SOURCE added_source"))
        .await
        .is_err());
    let mut retire_dependents = request(
        1503,
        vec![
            "DROP SINK added_sink".into(),
            "DROP STREAM added_stream".into(),
            "DROP SINK surviving_sink".into(),
            "DROP STREAM surviving_total".into(),
        ],
    );
    retire_dependents.expected_parent_version = TopologyVersion::new(3).unwrap();
    Box::pin(
        fixture
            .db
            .submit_cluster_topology_change(&retire_dependents),
    )
    .await
    .unwrap();
    active(&fixture, retire_dependents.operation_id).await;
    let ExecuteResult::Ddl(info) = Box::pin(fixture.db.execute("DROP SOURCE added_source"))
        .await
        .unwrap()
    else {
        panic!("SQL must return a durable source removal receipt");
    };
    assert!(!info.applied);
    active(&fixture, info.topology_operation.unwrap().operation_id).await;
    assert!(fixture.db.catalog_manifest_inventory().unwrap().is_empty());
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    assert!(latest_index(&fixture).await.source_names.is_empty());
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 1);
    probe.input.lock().remove("trades");
    probe.output.lock().clear();
    let recreation = ClusterTopologyRequest {
        operation_id: uuid::Uuid::from_u128(1504).try_into().unwrap(),
        expected_parent_version: TopologyVersion::new(5).unwrap(),
        statements: vec![
            source_ddl("trades", "'topic' = 'new', 'start' = 'latest'"),
            baseline[1].clone(),
            baseline[2].clone(),
        ],
    };
    Box::pin(fixture.db.submit_cluster_topology_change(&recreation))
        .await
        .unwrap();
    active(&fixture, recreation.operation_id).await;
    assert!(fixture
        .db
        .catalog_manifest_inventory()
        .unwrap()
        .iter()
        .all(|entry| entry.catalog_generation == 2));
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            92,
        )]),
    );
    wait_until(|| sink_total(&probe) == 15).await;
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    let recreated_checkpoint = latest_index(&fixture).await;
    assert_eq!(
        recreated_checkpoint.source_offsets["trades"].offsets["partition-0-next"],
        "95"
    );
    assert!(!recreated_checkpoint.source_offsets["trades"]
        .offsets
        .contains_key("old.cursor"));
    assert_eq!(fixture.resolutions.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.effects.load(Ordering::SeqCst), 0);
    Box::pin(fixture.db.shutdown()).await.unwrap();
}

#[tokio::test]
async fn topology_compatible_stateless_replacement_preserves_managed_state_and_source_progress() {
    let (fixture, probe) = running_parent().await;
    let addition = request(
        1601,
        vec!["CREATE STREAM projected AS SELECT id, value AS total FROM trades".into()],
    );
    Box::pin(fixture.db.submit_cluster_topology_change(&addition))
        .await
        .unwrap();
    active(&fixture, addition.operation_id).await;
    let before = fixture.db.connector_manager.lock().streams()["totals"]
        .subscription_certificate
        .clone()
        .unwrap();
    let replacement = ClusterTopologyRequest { operation_id: uuid::Uuid::from_u128(1602).try_into().unwrap(), expected_parent_version: TopologyVersion::new(2).unwrap(), statements: vec![
        "CREATE OR REPLACE STREAM projected AS SELECT id, value * 2 AS total FROM trades".into(),
        "CREATE SINK projected_sink FROM projected INTO \"planning-sink\" ('topic' = 'projected-output')".into(),
    ] };
    let report = fixture
        .db
        .validate_cluster_topology_change(
            replacement.expected_parent_version,
            &replacement.statements,
        )
        .await
        .unwrap();
    let projected = report
        .objects
        .iter()
        .find(|object| object.name == "projected")
        .unwrap();
    assert_eq!(
        projected.transition,
        crate::ClusterTopologyObjectTransition::Preserve
    );
    assert_eq!(projected.catalog_generation, 1);
    probe.output.lock().clear();
    Box::pin(fixture.db.submit_cluster_topology_change(&replacement))
        .await
        .unwrap();
    active(&fixture, replacement.operation_id).await;
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            3,
        )]),
    );
    wait_until(|| sink_total(&probe) == 45).await;
    wait_until(|| {
        total(
            &probe
                .output
                .lock()
                .iter()
                .filter(|(topic, _)| topic == "projected-output")
                .map(|(_, batch)| batch.clone())
                .collect::<Vec<_>>(),
        ) == 30
    })
    .await;
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    assert_eq!(
        latest_index(&fixture).await.source_offsets["trades"].offsets["old.cursor"],
        "6"
    );
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["totals"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        before.stream_generation
    );
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 0);
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
    Box::pin(fixture.db.shutdown()).await.unwrap();
}

#[tokio::test]
async fn topology_pipeline_reset_separates_incarnations_and_recovers_future_input_without_resetting_survivors(
) {
    let (fixture, probe) = running_parent().await;
    let mut additions = independent_pipeline();
    additions.extend([
        "CREATE STREAM surviving_total AS SELECT id, SUM(value) AS total FROM added_source GROUP BY id EMIT CHANGES WITH ('retain_history' = '4mb')".into(),
        "CREATE SINK surviving_sink FROM surviving_total INTO \"planning-sink\" ('topic' = 'surviving-output')".into(),
    ]);
    let addition = request(1611, additions);
    Box::pin(fixture.db.submit_cluster_topology_change(&addition))
        .await
        .unwrap();
    active(&fixture, addition.operation_id).await;
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(5)["trades"][0].clone(),
            91,
        )]),
    );
    let surviving_total = || {
        total(
            &probe
                .output
                .lock()
                .iter()
                .filter(|(topic, _)| topic == "surviving-output")
                .map(|(_, batch)| batch.clone())
                .collect::<Vec<_>>(),
        )
    };
    wait_until(|| surviving_total() == 15).await;
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    let old_certificate = fixture.db.connector_manager.lock().streams()["totals"]
        .subscription_certificate
        .clone()
        .unwrap();
    let survivor = fixture.db.connector_manager.lock().streams()["surviving_total"]
        .subscription_certificate
        .clone()
        .unwrap();
    let reset = ClusterTopologyRequest { operation_id: uuid::Uuid::from_u128(1612).try_into().unwrap(), expected_parent_version: TopologyVersion::new(2).unwrap(), statements: vec![
        "DROP SINK existing_sink".into(), "DROP STREAM totals".into(), "DROP SOURCE trades".into(),
        source_ddl("trades", "'topic' = 'new', 'start' = 'latest'"),
        "CREATE STREAM totals AS SELECT value, SUM(id) AS total FROM trades GROUP BY value EMIT CHANGES WITH ('retain_history' = '4mb')".into(),
        "CREATE SINK existing_sink FROM totals INTO \"planning-sink\" ('topic' = 'reset-output')".into(),
    ] };
    probe.output.lock().clear();
    Box::pin(fixture.db.submit_cluster_topology_change(&reset))
        .await
        .unwrap();
    active(&fixture, reset.operation_id).await;
    let input = fixture
        .authority
        .controller
        .committed_topology_restore_input(reset.operation_id)
        .await
        .unwrap();
    assert!(input
        .root()
        .subscriptions
        .iter()
        .all(|mapping| mapping.parent_certificate.stream_id != "totals"));
    assert_eq!(
        input.checkpoint().source_offsets["trades"].offsets["old.cursor"],
        "3"
    );
    assert_eq!(
        input.root().source_initializations[0].checkpoint.offsets["partition-0-next"],
        "92"
    );
    let reset_certificate = fixture.db.connector_manager.lock().streams()["totals"]
        .subscription_certificate
        .clone()
        .unwrap();
    assert_eq!(reset_certificate.catalog_generation, 2);
    assert_ne!(
        reset_certificate.stream_generation,
        old_certificate.stream_generation
    );
    for name in ["trades", "totals", "existing_sink"] {
        assert_eq!(
            fixture
                .db
                .catalog_manifest_inventory()
                .unwrap()
                .iter()
                .find(|entry| entry.canonical_name == name)
                .unwrap()
                .catalog_generation,
            2
        );
    }
    assert!(probe.starts.lock().iter().rev().find(|(name, _)| name == "trades").is_some_and(|(_, position)| matches!(position, laminar_connectors::connector::SourcePosition::Initialized { checkpoint } if checkpoint.get_offset("old.cursor").is_none() && checkpoint.get_offset("partition-0-next") == Some("92"))));
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            super::input(5)["trades"][0].clone(),
            92,
        )]),
    );
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            super::input(7)["trades"][0].clone(),
            94,
        )]),
    );
    let reset_total = || {
        total(
            &probe
                .output
                .lock()
                .iter()
                .filter(|(topic, _)| topic == "reset-output")
                .map(|(_, batch)| batch.clone())
                .collect::<Vec<_>>(),
        )
    };
    wait_until(|| reset_total() == 3 && surviving_total() == 36).await;
    assert!(probe
        .output
        .lock()
        .iter()
        .all(|(topic, _)| topic != "old-output"));
    assert!(Box::pin(fixture.db.checkpoint()).await.unwrap().success);
    let selected = latest_index(&fixture).await;
    assert_eq!(
        selected.source_offsets["trades"].offsets["partition-0-next"],
        "95"
    );
    assert!(!selected.source_offsets["trades"]
        .offsets
        .contains_key("old.cursor"));
    probe.output.lock().clear();
    let prior_starts = probe.starts.lock().len();
    super::recovery::restart_settled_target(&fixture, &probe, prior_starts).await;
    probe.input.lock().insert(
        "trades".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            super::input(5)["trades"][0].clone(),
            95,
        )]),
    );
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            super::input(9)["trades"][0].clone(),
            97,
        )]),
    );
    super::recovery::released(&fixture).await;
    wait_until(|| reset_total() == 6 && surviving_total() == 63).await;
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["totals"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        reset_certificate.stream_generation
    );
    assert_eq!(
        fixture.db.connector_manager.lock().streams()["surviving_total"]
            .subscription_certificate
            .as_ref()
            .unwrap()
            .stream_generation,
        survivor.stream_generation
    );
    assert_eq!(fixture.resolutions.load(Ordering::Acquire), 2);
    assert_eq!(fixture.effects.load(Ordering::Acquire), 0);
    Box::pin(fixture.db.shutdown()).await.unwrap();
}

async fn committed_counts(db: &LaminarDB, epoch: u64) -> std::collections::BTreeMap<i64, i64> {
    let read = Box::pin(db.query_committed(
        "SELECT id, n FROM counts",
        crate::CommittedRead::Exact(epoch),
    ))
    .await
    .unwrap();
    let mut counts = std::collections::BTreeMap::new();
    for batch in &read.batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .clone()
        };
        counts.extend(
            column("id")
                .values()
                .iter()
                .copied()
                .zip(column("n").values().iter().copied()),
        );
    }
    counts
}

#[tokio::test]
async fn topology_sql_migration_keeps_a_materialized_view_and_checkpoints_its_rows() {
    let (fixture, _) = Box::pin(preparation_fixture_with_bootstrap(
        1,
        vec![
            "CREATE MATERIALIZED VIEW counts AS SELECT id, COUNT(*) AS n FROM trades GROUP BY id"
                .into(),
        ],
    ))
    .await;
    let probe = enable_runtime(&fixture);
    probe.allow_parent_initial.store(true, Ordering::Release);
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
    DbState::Created.store(&fixture.db.state);
    fixture.db.enable_coordinated_recovery().unwrap();
    Box::pin(fixture.db.start()).await.unwrap();
    assert_eq!(
        Box::pin(
            fixture
                .db
                .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(5))
        )
        .await
        .unwrap(),
        crate::ClusterStartupDisposition::Serving
    );
    let deliver = |ids: [i64; 3], first: u64| {
        let batch = RecordBatch::try_new(
            crate::temporal_test_source::schema(),
            vec![
                Arc::new(arrow::array::Int64Array::from(ids.to_vec())),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    1000, 2000, 3000,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![1; 3])),
            ],
        )
        .unwrap();
        probe.output.lock().clear();
        probe.input.lock().insert(
            "trades".into(),
            std::collections::VecDeque::from([runtime_probe::positioned(batch, first)]),
        );
    };
    let old_output = || {
        probe
            .output
            .lock()
            .iter()
            .any(|(topic, _)| topic == "old-output")
    };

    deliver([1, 2, 2], 0);
    wait_until(old_output).await;
    let parent = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(parent.success, "{parent:?}");
    assert_eq!(
        committed_counts(&fixture.db, parent.epoch).await,
        [(1, 1), (2, 2)].into()
    );
    let parent_identity = latest_index(&fixture).await.pipeline_identity;

    let request = request(1302, independent_pipeline());
    Box::pin(fixture.db.submit_cluster_topology_change(&request))
        .await
        .unwrap();
    active(&fixture, request.operation_id).await;
    deliver([1, 1, 1], 3);
    probe.input.lock().insert(
        "added_source".into(),
        std::collections::VecDeque::from([runtime_probe::positioned(
            input(19)["trades"][0].clone(),
            91,
        )]),
    );
    wait_until(|| {
        old_output()
            && probe
                .output
                .lock()
                .iter()
                .any(|(topic, _)| topic == "new-output")
    })
    .await;
    let target = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(target.success, "{target:?}");
    assert_ne!(
        latest_index(&fixture).await.pipeline_identity,
        parent_identity
    );
    // Key 2 is not touched after the migration, so only the migrated slice can hold it.
    assert_eq!(
        committed_counts(&fixture.db, target.epoch).await,
        [(1, 4), (2, 2)].into()
    );
    fixture.db.shutdown().await.unwrap();
}

async fn committed_pairs(db: &LaminarDB, sql: &str, epoch: u64) -> Vec<(i64, i64)> {
    let read = Box::pin(db.query_committed(sql, crate::CommittedRead::Exact(epoch)))
        .await
        .unwrap();
    let mut pairs = Vec::new();
    for batch in &read.batches {
        let column = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .clone()
        };
        pairs.extend(
            column(0)
                .values()
                .iter()
                .copied()
                .zip(column(1).values().iter().copied()),
        );
    }
    pairs.sort_unstable();
    pairs
}

#[tokio::test]
async fn topology_sql_migration_adds_and_drops_materialized_views() {
    let (fixture, _) = Box::pin(preparation_fixture_with_bootstrap(
        1,
        vec![
            "CREATE MATERIALIZED VIEW counts AS SELECT id, COUNT(*) AS n FROM trades GROUP BY id"
                .into(),
            "CREATE MATERIALIZED VIEW doomed AS SELECT id, COUNT(*) AS n FROM trades GROUP BY id"
                .into(),
            "CREATE MATERIALIZED VIEW seen AS SELECT id, value FROM trades WITH (retention = '100 rows')"
                .into(),
            "CREATE MATERIALIZED VIEW doomed_seen AS SELECT id, value FROM trades WITH (retention = '100 rows')"
                .into(),
        ],
    ))
    .await;
    let probe = enable_runtime(&fixture);
    probe.allow_parent_initial.store(true, Ordering::Release);
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
    DbState::Created.store(&fixture.db.state);
    fixture.db.enable_coordinated_recovery().unwrap();
    Box::pin(fixture.db.start()).await.unwrap();
    assert_eq!(
        Box::pin(
            fixture
                .db
                .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(5))
        )
        .await
        .unwrap(),
        crate::ClusterStartupDisposition::Serving
    );
    let deliver = |ids: [i64; 3], first: u64| {
        let batch = RecordBatch::try_new(
            crate::temporal_test_source::schema(),
            vec![
                Arc::new(arrow::array::Int64Array::from(ids.to_vec())),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    1000, 2000, 3000,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![1; 3])),
            ],
        )
        .unwrap();
        probe.output.lock().clear();
        probe.input.lock().insert(
            "trades".into(),
            std::collections::VecDeque::from([runtime_probe::positioned(batch, first)]),
        );
    };
    let old_output = || {
        probe
            .output
            .lock()
            .iter()
            .any(|(topic, _)| topic == "old-output")
    };

    deliver([1, 2, 2], 0);
    wait_until(old_output).await;
    let parent = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(parent.success, "{parent:?}");

    let recreate = fixture
        .db
        .validate_cluster_topology_change(
            TopologyVersion::LEGACY_BASELINE,
            &[
                "DROP MATERIALIZED VIEW counts".into(),
                "CREATE MATERIALIZED VIEW counts AS SELECT id, COUNT(*) AS n FROM trades GROUP BY id"
                    .into(),
            ],
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        recreate.contains("dropped and recreated in one migration"),
        "{recreate}"
    );

    let request = request(
        1303,
        vec![
            "DROP MATERIALIZED VIEW doomed".into(),
            "DROP MATERIALIZED VIEW doomed_seen".into(),
            "CREATE MATERIALIZED VIEW added_counts AS SELECT id, COUNT(*) AS n FROM trades GROUP BY id"
                .into(),
            "CREATE MATERIALIZED VIEW added_seen AS SELECT id, value FROM trades WITH (retention = '100 rows')"
                .into(),
        ],
    );
    Box::pin(fixture.db.submit_cluster_topology_change(&request))
        .await
        .unwrap();
    active(&fixture, request.operation_id).await;
    deliver([1, 1, 1], 3);
    wait_until(old_output).await;
    let target = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(target.success, "{target:?}");

    // Preserved views keep the rows from before the migration; added views start at the cut.
    let read = |sql: &'static str| committed_pairs(&fixture.db, sql, target.epoch);
    assert_eq!(read("SELECT id, n FROM counts").await, [(1, 4), (2, 2)]);
    assert_eq!(read("SELECT id, n FROM added_counts").await, [(1, 3)]);
    assert_eq!(
        read("SELECT id, value FROM seen").await,
        [(1, 1), (1, 1), (1, 1), (1, 1), (2, 1), (2, 1)]
    );
    assert_eq!(
        read("SELECT id, value FROM added_seen").await,
        [(1, 1), (1, 1), (1, 1)]
    );
    for dropped in ["doomed", "doomed_seen"] {
        assert!(
            Box::pin(fixture.db.query_committed(
                &format!("SELECT * FROM {dropped}"),
                crate::CommittedRead::Exact(target.epoch),
            ))
            .await
            .is_err(),
            "{dropped} is still readable"
        );
    }
    let index = latest_index(&fixture).await;
    assert!(index
        .view_frontiers
        .keys()
        .all(|view| view != "doomed_seen"));
    fixture.db.shutdown().await.unwrap();
}

#[tokio::test]
async fn topology_sql_migration_seeds_a_new_view_from_its_upstream_at_the_cut() {
    seeded_views_after_migration(false).await;
}

#[tokio::test]
async fn topology_root_recovery_seeds_a_new_view_once() {
    seeded_views_after_migration(true).await;
}

/// Views a migration adds over an existing view and aggregate hold the rows from before the cut,
/// also when the target recovers from the migration root before its first checkpoint.
async fn seeded_views_after_migration(recover_from_root: bool) {
    let (fixture, _) = Box::pin(preparation_fixture_with_bootstrap(
        1,
        vec![
            "CREATE MATERIALIZED VIEW counts AS SELECT id, COUNT(*) AS n FROM trades GROUP BY id"
                .into(),
            "CREATE STREAM relay AS SELECT id, total FROM totals".into(),
        ],
    ))
    .await;
    let probe = enable_runtime(&fixture);
    probe.allow_parent_initial.store(true, Ordering::Release);
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
    DbState::Created.store(&fixture.db.state);
    fixture.db.enable_coordinated_recovery().unwrap();
    Box::pin(fixture.db.start()).await.unwrap();
    assert_eq!(
        Box::pin(
            fixture
                .db
                .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(5))
        )
        .await
        .unwrap(),
        crate::ClusterStartupDisposition::Serving
    );
    let deliver = |ids: [i64; 3], first: u64| {
        let batch = RecordBatch::try_new(
            crate::temporal_test_source::schema(),
            vec![
                Arc::new(arrow::array::Int64Array::from(ids.to_vec())),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                    1000, 2000, 3000,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![1; 3])),
            ],
        )
        .unwrap();
        probe.output.lock().clear();
        probe.input.lock().insert(
            "trades".into(),
            std::collections::VecDeque::from([runtime_probe::positioned(batch, first)]),
        );
    };
    let old_output = || {
        probe
            .output
            .lock()
            .iter()
            .any(|(topic, _)| topic == "old-output")
    };

    deliver([1, 2, 2], 0);
    wait_until(old_output).await;
    let parent = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(parent.success, "{parent:?}");

    let unseedable = fixture
        .db
        .validate_cluster_topology_change(
            TopologyVersion::LEGACY_BASELINE,
            &["CREATE MATERIALIZED VIEW over_relay AS SELECT id, total FROM relay".into()],
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        unseedable.contains("keeps no rows to seed it"),
        "{unseedable}"
    );

    let request = request(
        1304,
        vec![
            "CREATE MATERIALIZED VIEW busy AS SELECT id, n FROM counts WHERE n > 1".into(),
            "CREATE MATERIALIZED VIEW by_n AS SELECT n, COUNT(*) AS ids FROM counts GROUP BY n"
                .into(),
            "CREATE MATERIALIZED VIEW running AS SELECT id, total FROM totals".into(),
        ],
    );
    Box::pin(fixture.db.submit_cluster_topology_change(&request))
        .await
        .unwrap();
    active(&fixture, request.operation_id).await;
    if recover_from_root {
        let prior_starts = probe.starts.lock().len();
        probe.block_start.store(true, Ordering::Release);
        super::recovery::request_recovery(&fixture);
        super::recovery::held_start(&fixture, &probe, prior_starts).await;
        super::recovery::released(&fixture).await;
        let runtime = fixture
            .db
            .installed_topology_runtime
            .lock()
            .clone()
            .unwrap();
        assert_eq!(
            runtime.recovery.as_ref().unwrap().selection.cut(),
            laminar_core::cluster::control::TopologyRecoveryCut::MigrationRoot
        );
    }
    deliver([1, 1, 1], 3);
    wait_until(old_output).await;
    let target = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(target.success, "{target:?}");

    // Key 2 is not touched after the cut, so only the seed can hold it.
    let read = |sql: &'static str| committed_pairs(&fixture.db, sql, target.epoch);
    assert_eq!(read("SELECT id, n FROM counts").await, [(1, 4), (2, 2)]);
    assert_eq!(read("SELECT id, n FROM busy").await, [(1, 4), (2, 2)]);
    assert_eq!(read("SELECT n, ids FROM by_n").await, [(2, 1), (4, 1)]);
    assert_eq!(
        read("SELECT id, total FROM running").await,
        [(1, 4), (2, 2)]
    );

    // The target checkpoint holds the seeded rows, so recovering from it seeds nothing again.
    let prior_starts = probe.starts.lock().len();
    super::recovery::restart_settled_target(&fixture, &probe, prior_starts).await;
    super::recovery::released(&fixture).await;
    deliver([3, 3, 3], 6);
    wait_until(old_output).await;
    let after = Box::pin(fixture.db.checkpoint()).await.unwrap();
    assert!(after.success, "{after:?}");
    let read = |sql: &'static str| committed_pairs(&fixture.db, sql, after.epoch);
    assert_eq!(
        read("SELECT id, n FROM busy").await,
        [(1, 4), (2, 2), (3, 3)]
    );
    assert_eq!(
        read("SELECT n, ids FROM by_n").await,
        [(2, 1), (3, 1), (4, 1)]
    );
    fixture.db.shutdown().await.unwrap();
}
