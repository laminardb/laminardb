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
    let sql = Box::pin(
        fixture
            .db
            .execute("CREATE STREAM downstream AS SELECT * FROM totals"),
    )
    .await
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
fn metadata_coordinator(fixture: &Fixture) -> crate::db::ForceCheckpointRx {
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
    let _receiver = metadata_coordinator(&fixture);
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
    let _receiver = metadata_coordinator(&fixture);
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
    let _receiver = metadata_coordinator(&fixture);
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
